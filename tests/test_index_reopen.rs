//! Secondary-index durability across reopen.
//!
//! Regression for a whole bug family found by inspection + empirical testing:
//! under the async index pipeline, close() skipped EVERY index flush (a
//! stale one-shot guard flag), and the reopen loaders then failed three
//! different ways —
//!   - column: the "{table}.{column}" alias was never restored, and the
//!     on-disk index was stale/empty (mem_buffer content never flushed);
//!   - text:   TextFTSIndex::new was handed the ".fts.d" dir so
//!     with_extension produced "…fts.fts.d" — a fresh EMPTY index at a wrong
//!     path under a name nothing could look up;
//!   - vector: the SQ8 sidecar/header stayed count=0 (appends durable,
//!     index bookkeeping not) so the loaded index looked empty.
//!
//! These tests pin: clean close + reopen, crash-sim reopen (leaked handle,
//! lock removed), and post-reopen incremental inserts — for all three kinds.

use std::path::{Path, PathBuf};

use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        motedb::sql::QueryResult::Select { rows, .. } => rows,
        other => panic!("expected Select, got {:?} for {}", other, sql),
    }
}

/// Crash-sim: leak the handle (no close), drop the stale flock as the OS
/// would after SIGKILL, reopen.
fn reopen_after_crash(db_path: &Path) -> Database {
    std::fs::remove_file(db_path.join(".lock")).ok();
    Database::open(db_path).unwrap()
}

#[test]
fn test_column_index_survives_clean_reopen() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().join("t.mote");
    {
        let db = Database::create(&p).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v FLOAT)")
            .unwrap();
        // Custom index name (exercises the alias restore).
        db.execute("CREATE INDEX tv ON t (v)").unwrap();
        for i in 1..=20 {
            db.execute(&format!("INSERT INTO t VALUES ({0}, {0}.5)", i))
                .unwrap();
        }
        let _ = db.close();
    }
    let db = Database::open(&p).unwrap();
    let r = db
        .query_by_column("t", "v", &Value::Float(3.5))
        .unwrap_or_else(|e| panic!("query_by_column after reopen: {e}"));
    assert_eq!(r.len(), 1);
    assert_eq!(rows(&db, "SELECT id FROM t WHERE v = 3.5").len(), 1);

    // Incremental insert after reopen is indexed.
    db.execute("INSERT INTO t VALUES (99, 99.5)").unwrap();
    let r = db.query_by_column("t", "v", &Value::Float(99.5)).unwrap();
    assert_eq!(r.len(), 1);
}

#[test]
fn test_column_index_survives_crash_reopen() {
    let dir = TempDir::new().unwrap();
    let p: PathBuf = dir.path().join("t.mote");
    {
        let db = Database::create(&p).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();
        db.execute("CREATE INDEX tv ON t (v)").unwrap();
        for i in 1..=20 {
            db.execute(&format!("INSERT INTO t VALUES ({0}, {0})", i))
                .unwrap();
        }
        std::mem::forget(db);
    }
    let db = reopen_after_crash(&p);
    assert_eq!(rows(&db, "SELECT COUNT(*) FROM t").len(), 1);
    let r = db.query_by_column("t", "v", &Value::Integer(7)).unwrap();
    assert_eq!(r.len(), 1, "column index rebuilt after crash must find v=7");
}

#[test]
fn test_text_index_survives_clean_reopen() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().join("t.mote");
    {
        let db = Database::create(&p).unwrap();
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        db.execute("CREATE TEXT INDEX docs_body ON docs (body)")
            .unwrap();
        for i in 1..=10 {
            db.execute(&format!(
                "INSERT INTO docs VALUES ({0}, 'alpha bravo note {0}')",
                i
            ))
            .unwrap();
        }
        let _ = db.close();
    }
    let db = Database::open(&p).unwrap();
    let r = db
        .text_search_ranked("docs_body", "alpha", 10)
        .unwrap_or_else(|e| panic!("text search after reopen: {e}"));
    assert_eq!(r.len(), 10);
    assert_eq!(
        rows(&db, "SELECT id FROM docs WHERE MATCH(body, 'alpha')").len(),
        10
    );

    db.execute("INSERT INTO docs VALUES (99, 'alpha fresh')")
        .unwrap();
    let r = db.text_search_ranked("docs_body", "fresh", 10).unwrap();
    assert_eq!(r.len(), 1);
}

#[test]
fn test_text_index_survives_crash_reopen() {
    let dir = TempDir::new().unwrap();
    let p: PathBuf = dir.path().join("t.mote");
    {
        let db = Database::create(&p).unwrap();
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        db.execute("CREATE TEXT INDEX docs_body ON docs (body)")
            .unwrap();
        for i in 1..=10 {
            db.execute(&format!(
                "INSERT INTO docs VALUES ({0}, 'alpha bravo {0}')",
                i
            ))
            .unwrap();
        }
        std::mem::forget(db);
    }
    let db = reopen_after_crash(&p);
    let r = db
        .text_search_ranked("docs_body", "alpha", 10)
        .unwrap_or_else(|e| panic!("text search after crash reopen: {e}"));
    assert_eq!(r.len(), 10, "text index rebuilt from WAL-recovered rows");
}

#[test]
fn test_vector_index_survives_clean_reopen() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().join("t.mote");
    {
        let db = Database::create(&p).unwrap();
        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, emb VECTOR(4))")
            .unwrap();
        db.execute("CREATE VECTOR INDEX items_emb ON items (emb)")
            .unwrap();
        for i in 0..10 {
            let v: Vec<String> = (0..4)
                .map(|k| format!("{}", (i + k) as f32 / 10.0))
                .collect();
            db.execute(&format!(
                "INSERT INTO items VALUES ({0}, [{1}])",
                i,
                v.join(", ")
            ))
            .unwrap();
        }
        let _ = db.close();
    }
    let db = Database::open(&p).unwrap();
    let q: Vec<f32> = vec![0.0, 0.1, 0.2, 0.3];
    let r = db
        .vector_search("items_emb", &q, 3)
        .unwrap_or_else(|e| panic!("vector search after reopen: {e}"));
    assert_eq!(r.len(), 3);
    // Nearest to the query must be row 0 (the query IS row 0's vector).
    assert_eq!(r[0].0, 0, "nearest neighbor wrong after reopen: {:?}", r);
}

#[test]
fn test_vector_index_crash_self_heal() {
    // Crash-sim: appended SQ8 entries are durable but the sidecar/header
    // bookkeeping was not flushed. The loader must recover the true entry
    // count from the physical file.
    let dir = TempDir::new().unwrap();
    let p: PathBuf = dir.path().join("t.mote");
    {
        let db = Database::create(&p).unwrap();
        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, emb VECTOR(4))")
            .unwrap();
        db.execute("CREATE VECTOR INDEX items_emb ON items (emb)")
            .unwrap();
        for i in 0..10 {
            let v: Vec<String> = (0..4)
                .map(|k| format!("{}", (i + k) as f32 / 10.0))
                .collect();
            db.execute(&format!(
                "INSERT INTO items VALUES ({0}, [{1}])",
                i,
                v.join(", ")
            ))
            .unwrap();
        }
        std::mem::forget(db);
    }
    let db = reopen_after_crash(&p);
    let q: Vec<f32> = vec![0.0, 0.1, 0.2, 0.3];
    let r = db
        .vector_search("items_emb", &q, 3)
        .unwrap_or_else(|e| panic!("vector search after crash reopen: {e}"));
    // The query vector itself must at minimum be found (exact match).
    assert!(
        r.iter().any(|(id, _)| *id == 0),
        "self-healed index must find the exact query vector, got {:?}",
        r
    );
}
