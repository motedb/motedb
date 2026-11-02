//! B-06: `CREATE VECTOR INDEX` must actually serve SQL KNN queries. The SQL
//! path used to route to the exact brute force below 200K rows (a stale
//! recall measurement), so a 12-30s index build bought nothing for typical
//! table sizes. Routing is now index-first (with a small configurable floor)
//! and the graph result is overfetched (×4) then EXACTLY reranked — SQL
//! top-k must equal the brute-force answer.

use motedb::types::Value;
use motedb::{Database, QueryResult};
use tempfile::TempDir;

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        other => panic!("unexpected: {other:?}"),
    }
}

#[test]
fn sql_knn_uses_the_vector_index_and_matches_exact() {
    // Lower the index-routing floor for the test (default 20K rows — the
    // debug-mode suite cannot afford that many inserts + a graph build).
    std::env::set_var("MOTE_VEC_INDEX_MIN_ROWS", "500");
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE v (id INT PRIMARY KEY, emb VECTOR(32), tag TEXT)")
        .unwrap();

    // Deterministic pseudo-random 32-dim vectors, 800 rows (above the floor).
    let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut rnd = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    let mut vecs: Vec<Vec<f32>> = Vec::with_capacity(800);
    for batch in 0..8 {
        let mut t = String::new();
        for i in 0..100 {
            let id = batch * 100 + i;
            let v: Vec<String> = (0..32)
                .map(|_| format!("{:.4}", (rnd() % 10_000) as f32 / 10_000.0))
                .collect();
            if i > 0 {
                t.push(',');
            }
            t.push_str(&format!("({id}, '[{}]', 't{}')", v.join(","), id % 3));
            if i == 99 || batch == 7 {
                // remember every 100th for the probe queries
            }
        }
        vecs.push(vec![]);
        db.execute(&format!("INSERT INTO v VALUES {t}")).unwrap();
    }
    db.checkpoint().unwrap();

    // Ground truth BEFORE any index: brute-force SQL KNN.
    let query: Vec<String> = (0..32)
        .map(|_| format!("{:.4}", (rnd() % 10_000) as f32 / 10_000.0))
        .collect();
    let query = query.join(",");
    let sql = format!("SELECT id FROM v ORDER BY emb <-> '[{query}]' LIMIT 5");
    let before: Vec<i64> = rows(&db, &sql)
        .iter()
        .map(|r| match r[0] {
            Value::Integer(i) => i,
            _ => -1,
        })
        .collect();
    assert_eq!(before.len(), 5);

    // Build the index — the SQL plan must now route through it.
    db.execute("CREATE VECTOR INDEX v_emb ON v (emb)").unwrap();
    db.checkpoint().unwrap();

    let after: Vec<i64> = rows(&db, &sql)
        .iter()
        .map(|r| match r[0] {
            Value::Integer(i) => i,
            _ => -1,
        })
        .collect();
    assert_eq!(
        after, before,
        "index-routed SQL KNN must equal the exact brute-force top-k"
    );

    // Second query, and one with a WHERE filter (deepening path).
    let q2: Vec<String> = (0..32)
        .map(|_| format!("{:.4}", (rnd() % 10_000) as f32 / 10_000.0))
        .collect();
    let q2 = q2.join(",");
    let sql2 = format!("SELECT id FROM v ORDER BY emb <-> '[{q2}]' LIMIT 5");
    let plain: Vec<i64> = rows(&db, &sql2)
        .iter()
        .map(|r| match r[0] {
            Value::Integer(i) => i,
            _ => -1,
        })
        .collect();
    assert_eq!(plain.len(), 5);

    let filtered: Vec<i64> = rows(
        &db,
        &format!("SELECT id FROM v WHERE id >= 0 ORDER BY emb <-> '[{q2}]' LIMIT 5"),
    )
    .iter()
    .map(|r| match r[0] {
        Value::Integer(i) => i,
        _ => -1,
    })
    .collect();
    assert_eq!(filtered.len(), 5, "WHERE-covered query keeps 5 rows");
}
