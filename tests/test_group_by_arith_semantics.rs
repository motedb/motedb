//! Integer `%` and `/` in GROUP BY keys must mean the same thing on the
//! vectorized (streaming / prepared, batch hash-join) path as on the row
//! evaluator: truncated remainder and truncated integer division.
//!
//! v0.16.0's vectorized key evaluator used `rem_euclid` (-7 % 5 = 3, so
//! negative keys collided into the wrong bucket) and returned a Float for
//! inexact `/` (so `GROUP BY v / 10, w` partitioned differently from
//! `GROUP BY v / 10`).

use motedb::types::Value;
use motedb::{Database, QueryResult};
use tempfile::TempDir;

fn rows_exec(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        other => panic!("unexpected: {other:?}"),
    }
}

fn rows_prepared(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db
        .execute_prepared(sql, vec![])
        .unwrap()
        .materialize()
        .unwrap()
    {
        QueryResult::Select { rows, .. } => rows,
        other => panic!("unexpected: {other:?}"),
    }
}

fn setup() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, w TEXT)")
        .unwrap();
    for (i, (v, w)) in [
        (-7, "a"),
        (3, "a"),
        (13, "b"),
        (-2, "b"),
        (8, "a"),
        (15, "a"),
        (19, "a"),
        (25, "a"),
    ]
    .iter()
    .enumerate()
    {
        db.execute(&format!("INSERT INTO t VALUES ({i}, {v}, '{w}')"))
            .unwrap();
    }
    db.checkpoint().unwrap();
    (dir, db)
}

#[test]
fn group_by_modulo_with_negative_keys_matches_row_semantics() {
    let (_d, db) = setup();
    let sql = "SELECT v % 5 AS k, COUNT(*) AS c FROM t GROUP BY v % 5 ORDER BY k";
    // -7 → -2, -2 → -2, 3 → 3, 13 → 3, 8 → 3, 15 → 0, 19 → 4, 25 → 0
    let expected = vec![
        vec![Value::Integer(-2), Value::Integer(2)],
        vec![Value::Integer(0), Value::Integer(2)],
        vec![Value::Integer(3), Value::Integer(3)],
        vec![Value::Integer(4), Value::Integer(1)],
    ];
    assert_eq!(rows_exec(&db, sql), expected, "execute()");
    assert_eq!(rows_prepared(&db, sql), expected, "execute_prepared()");
}

#[test]
fn group_by_integer_division_multi_key_matches_row_semantics() {
    let (_d, db) = setup();
    let sql = "SELECT v / 10 AS k, w, COUNT(*) AS c FROM t GROUP BY v / 10, w ORDER BY k, w";
    // -7/10 → 0 (trunc), -2/10 → 0, 3/10 → 0, 8/10 → 0, 13/10 → 1, 15/10 → 1,
    // 19/10 → 1, 25/10 → 2
    let expected = vec![
        vec![
            Value::Integer(0),
            Value::Text("a".into()),
            Value::Integer(3),
        ],
        vec![
            Value::Integer(0),
            Value::Text("b".into()),
            Value::Integer(1),
        ],
        vec![
            Value::Integer(1),
            Value::Text("a".into()),
            Value::Integer(2),
        ],
        vec![
            Value::Integer(1),
            Value::Text("b".into()),
            Value::Integer(1),
        ],
        vec![
            Value::Integer(2),
            Value::Text("a".into()),
            Value::Integer(1),
        ],
    ];
    assert_eq!(rows_exec(&db, sql), expected, "execute()");
    assert_eq!(rows_prepared(&db, sql), expected, "execute_prepared()");
}

#[test]
fn join_group_by_modulo_fast_path_matches_row_semantics() {
    let (_d, db) = setup();
    db.execute("CREATE TABLE d (id INT PRIMARY KEY, tag TEXT)")
        .unwrap();
    for i in 0..8 {
        db.execute(&format!("INSERT INTO d VALUES ({i}, 'x')"))
            .unwrap();
    }
    db.checkpoint().unwrap();
    let sql = "SELECT t.v % 5 AS k, COUNT(*) AS c FROM t JOIN d ON t.id = d.id GROUP BY t.v % 5 ORDER BY k";
    let expected = vec![
        vec![Value::Integer(-2), Value::Integer(2)],
        vec![Value::Integer(0), Value::Integer(2)],
        vec![Value::Integer(3), Value::Integer(3)],
        vec![Value::Integer(4), Value::Integer(1)],
    ];
    assert_eq!(rows_exec(&db, sql), expected, "execute()");
    assert_eq!(rows_prepared(&db, sql), expected, "execute_prepared()");
}
