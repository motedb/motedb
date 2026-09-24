//! Round 12b regressions: edge-intelligence hardening.
//!
//! 1. Vector-cache budget must honor the preset (embodied/robotics/edge cap
//!    it at 64/32MB) — a fixed 256MB default let a vector top-k balloon RSS
//!    past edge devices' memory ceilings.
//! 2. The oversized-column streaming path (vector column > cache budget)
//!    must read in bounded chunks: the old one-shot whole-column read
//!    allocated a data-size buffer per query (153MB on 100K×384) — OOM-class
//!    on small devices. Correctness is unchanged (same top-k as the cached
//!    path); only the read granularity changed.
//! 3. `max_row_id` now reads one 8-byte key per segment instead of loading
//!    every segment's full keys array — reopen must still allocate FRESH
//!    row_ids after restart (no key collisions with existing rows).

use motedb::types::Value;
use motedb::{DBConfig, Database};
use tempfile::TempDir;

fn db_with(preset: DBConfig) -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), preset).unwrap();
    (dir, db)
}

fn seed_vectors(db: &Database, n: i64, dim: usize) {
    db.execute(&format!(
        "CREATE TABLE v (id INT PRIMARY KEY, cat INT, emb VECTOR({dim}))"
    ))
    .unwrap();
    for b in (0..n).step_by(500) {
        let hi = (b + 500).min(n);
        let mut batch = Vec::new();
        for i in b..hi {
            let emb: Vec<f32> = (0..dim).map(|k| ((i % 17) as f32) * 0.25 + k as f32).collect();
            batch.push(vec![
                Value::Integer(i),
                Value::Integer(i % 3),
                Value::Tensor(Box::new(motedb::types::Tensor::new(emb))),
            ]);
        }
        db.execute_prepared_many("INSERT INTO v (id, cat, emb) VALUES (?, ?, ?)", batch)
            .unwrap();
    }
    db.checkpoint().unwrap();
}

fn topk(db: &Database, k: usize) -> Vec<i64> {
    // Query vector close to row id%17==4's embedding (cat values irrelevant).
    let q: Vec<String> = (0..8).map(|k| format!("{}", 4.0 * 0.25 + k as f64)).collect();
    db.query(&format!(
        "SELECT id FROM v ORDER BY emb <-> [{}] ASC LIMIT {k}",
        q.join(", ")
    ))
    .unwrap()
    .iter()
    .map(|r| match &r[0] {
        Value::Integer(i) => *i,
        v => panic!("{v:?}"),
    })
    .collect()
}

/// Streaming (over-budget) and cached paths must return the SAME top-k.
/// Edge preset caps the vector cache at 32MB; the seeded column (~6K×8 f32
/// = tiny) still fits, so additionally force streaming by shrinking the
/// budget to the minimum.
#[test]
fn knn_streaming_chunks_match_cached_results() {
    let (_d, db) = db_with(DBConfig::for_edge());
    seed_vectors(&db, 1500, 8);

    let cached = topk(&db, 5);
    assert_eq!(cached.len(), 5);
    // All top rows come from the id%17==4 family (distance 0 to the query).
    for id in &cached {
        assert_eq!(id % 17, 4, "top-k must be the exact-match family");
    }

    // Shrink the vector cache to the 1MB floor → the next query MUST stream
    // through the chunked path; results must be identical.
    db.set_vector_cache_budget("v", 1024 * 1024).unwrap();
    let streamed = topk(&db, 5);
    assert_eq!(cached, streamed, "streamed top-k must equal cached top-k");
}

/// UPDATE + DELETE visibility through the chunked streaming path (a
/// superseded row must not come back when its segment streams).
#[test]
fn knn_streaming_respects_update_delete() {
    let (_d, db) = db_with(DBConfig::for_edge());
    seed_vectors(&db, 300, 8);
    let before = topk(&db, 3);
    // Move the current best match far away, delete the runner-up.
    db.execute("UPDATE v SET emb = [500, 500, 500, 500, 500, 500, 500, 500] WHERE id = 4")
        .unwrap();
    db.execute("DELETE FROM v WHERE id = 21").unwrap(); // 21 % 17 == 4
    db.set_vector_cache_budget("v", 1024 * 1024).unwrap();
    let after = topk(&db, 3);
    assert!(!after.contains(&4), "UPDATED row must not match anymore");
    assert!(!after.contains(&21), "DELETED row must not ghost back");
    assert_eq!(after.len(), 3);
    let _ = before;
}

/// Reopen must keep allocating FRESH row_ids (max_row_id via last-key hint).
/// Insert → close → reopen → insert more → no PK conflicts, all rows visible.
#[test]
fn reopen_allocates_fresh_row_ids_multi_segment() {
    let dir = TempDir::new().unwrap();
    {
        let db = Database::create_with_config(dir.path(), DBConfig::for_edge()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v FLOAT)").unwrap();
        let mut batch = Vec::new();
        for i in 0..300i64 {
            batch.push(vec![Value::Float(i as f64 * 0.1)]);
        }
        db.execute_prepared_many("INSERT INTO t (v) VALUES (?)", batch).unwrap();
        // A second flush → multiple segments, exercising per-segment hints.
        let mut batch2 = Vec::new();
        for i in 0..300i64 {
            batch2.push(vec![Value::Float(i as f64)]);
        }
        db.execute_prepared_many("INSERT INTO t (v) VALUES (?)", batch2).unwrap();
        db.checkpoint().unwrap();
    }
    let db = Database::open_with_config(dir.path(), DBConfig::for_edge()).unwrap();
    let rows = db.query("SELECT COUNT(*), MAX(id) FROM t").unwrap();
    let cnt = match &rows[0][0] {
        Value::Integer(c) => *c,
        v => panic!("{v:?}"),
    };
    assert_eq!(cnt, 600);
    // Continue inserting — ids must continue past the old max.
    let mut batch3 = Vec::new();
    for i in 0..100i64 {
        batch3.push(vec![Value::Float(i as f64 * 2.0)]);
    }
    db.execute_prepared_many("INSERT INTO t (v) VALUES (?)", batch3).unwrap();
    let rows = db.query("SELECT COUNT(*) FROM t").unwrap();
    let total = match &rows[0][0] {
        Value::Integer(c) => *c,
        v => panic!("{v:?}"),
    };
    assert_eq!(total, 700, "all rows visible after reopen+insert");
    let rows = db.query("SELECT COUNT(DISTINCT id) FROM t").unwrap();
    let distinct = match &rows[0][0] {
        Value::Integer(c) => *c,
        v => panic!("{v:?}"),
    };
    assert_eq!(distinct, 700, "no row_id collisions after reopen");
}

/// The vector cache budget must be preset-wired (embodied = 64MB, robotics
/// = 32MB). 🔑 默认 256→64MB (2026-09 资源画像收口): 256MB 时 100K×384 表
/// 首查 knn 缓存解码副本峰值 +183~295MB RSS, 超出查询期 ≤100MB 档位;
/// 超限段走 pread 流式 (零 RSS 增量)。需要大表暖查延迟的场景按表调回
/// set_vector_cache_budget (基准取舍见 bench/README)。
#[test]
fn vector_cache_budget_honors_config() {
    let (_d, db) = db_with(DBConfig::for_embodied());
    seed_vectors(&db, 10, 8);
    assert_eq!(db.vector_cache_budget_bytes("v").unwrap(), 64 * 1024 * 1024);
    let (_d2, db2) = db_with(DBConfig::for_robotics());
    seed_vectors(&db2, 10, 8);
    assert_eq!(db2.vector_cache_budget_bytes("v").unwrap(), 32 * 1024 * 1024);
    let (_d3, db3) = db_with(DBConfig::for_testing());
    seed_vectors(&db3, 10, 8);
    assert_eq!(
        db3.vector_cache_budget_bytes("v").unwrap(),
        64 * 1024 * 1024,
        "default stays 64MB (resource-profile first; see bench/README)"
    );
}
