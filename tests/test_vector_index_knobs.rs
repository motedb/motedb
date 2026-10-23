//! `CREATE VECTOR INDEX … WITH (max_degree=…, build_search_list=…,
//! search_list=…)` — build/query knob plumbing (Phase 2 §9).
//!
//! Also regression-guards the WITH clause reachability fix: WITH is a
//! KEYWORD token — the old `Identifier("WITH")` match never fired, so the
//! entire WITH clause (including `metric`) was unreachable dead code.

use motedb::Database;

#[test]
fn vector_index_with_options_parse_and_apply() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path().join("k.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, emb VECTOR(4))")
        .unwrap();
    for i in 0..500i64 {
        db.execute(&format!(
            "INSERT INTO t VALUES ({i}, [{i}.0, 1.0, -1.0, 0.5])"
        ))
        .unwrap();
    }
    // All knobs + the previously-dead metric option in one statement.
    db.execute(
        "CREATE VECTOR INDEX vi ON t(emb) WITH (\
         metric = 'l2', max_degree = 32, build_search_list = 40, search_list = 300)",
    )
    .unwrap();
    // Index is queryable and correct with the tuned build — in ALL THREE
    // query spellings. The quoted-literal form used to fall through the KNN
    // fast path into a sorter that silently skips expression keys,
    // returning arbitrary order (regression).
    let top1 = motedb::types::Value::Integer(9);
    let rows = db
        .query("SELECT id FROM t ORDER BY emb <-> [9, 1, -1, 0.5] LIMIT 1")
        .unwrap();
    assert_eq!(rows[0][0], top1, "unquoted vector literal");
    let rows = db
        .query("SELECT id FROM t ORDER BY emb <-> '[9, 1, -1, 0.5]' LIMIT 1")
        .unwrap();
    assert_eq!(rows[0][0], top1, "quoted bracketed string");
    let rows = db
        .execute_prepared(
            "SELECT id FROM t ORDER BY emb <-> ? LIMIT 1",
            vec![motedb::types::Value::Vector(motedb::types::ArcVec::new(
                vec![9.0, 1.0, -1.0, 0.5],
            ))],
        )
        .unwrap();
    match rows.materialize().unwrap() {
        motedb::QueryResult::Select { rows, .. } => assert_eq!(rows[0][0], top1, "param"),
        other => panic!("{other:?}"),
    }
    let _ = db.close();
}

#[test]
fn vector_index_with_options_validation_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path().join("k2.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, emb VECTOR(4))")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, [1, 2, 3, 4])")
        .unwrap();

    for (sql, needle) in [
        (
            "CREATE VECTOR INDEX b1 ON t(emb) WITH (max_degree = 4)",
            "max_degree must be in 8..=256",
        ),
        (
            "CREATE VECTOR INDEX b2 ON t(emb) WITH (build_search_list = 2)",
            "build_search_list must be >= 8",
        ),
        (
            "CREATE VECTOR INDEX b3 ON t(emb) WITH (nonsense = 1)",
            "Unknown WITH option",
        ),
    ] {
        let err = match db.execute(sql) {
            Err(e) => e,
            Ok(_) => panic!("expected error from: {sql}"),
        };
        assert!(err.to_string().contains(needle), "{sql} -> {err}");
    }
    let _ = db.close();
}
