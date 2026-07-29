//! Bug Hunt v39 — UPDATE/DELETE with IN (SELECT ...) subquery in WHERE.
//!
//! `UPDATE t SET ... WHERE id IN (SELECT ...)` silently matched 0 rows
//! because execute_update called eval_expr_on_row directly on the raw WHERE
//! clause (which contained an unresolved Subquery node → NULL → false for
//! every row). Fixed by materializing subqueries in the WHERE clause before
//! the per-row scan.

use motedb::sql::QueryResult;
use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    (db, dir)
}

fn rows(r: QueryResult) -> Vec<Vec<Value>> {
    match r {
        QueryResult::Select { rows, .. } => rows,
        _ => vec![],
    }
}

fn q(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    rows(db.execute(sql).unwrap().materialize().unwrap())
}

// =========================================================================
// UPDATE with IN (SELECT ...) subquery
// =========================================================================

#[test]
fn test_update_in_subquery() {
    // Previously affected 0 rows (subquery not materialized in WHERE).
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();
    db.execute("UPDATE t SET v = v * 2 WHERE id IN (SELECT id FROM t WHERE v > 15)").unwrap();
    let r = q(&db, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(r, vec![
        vec![Value::Integer(1), Value::Integer(10)],
        vec![Value::Integer(2), Value::Integer(40)],
        vec![Value::Integer(3), Value::Integer(60)],
    ]);
}

#[test]
fn test_update_in_subquery_different_table() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER)").unwrap();
    db.execute("INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)").unwrap();
    db.execute("INSERT INTO b VALUES (1, 1), (2, 3)").unwrap();
    // Update a where a.id appears in b.a_id
    db.execute("UPDATE a SET v = 999 WHERE id IN (SELECT a_id FROM b)").unwrap();
    let r = q(&db, "SELECT id, v FROM a ORDER BY id");
    assert_eq!(r, vec![
        vec![Value::Integer(1), Value::Integer(999)],
        vec![Value::Integer(2), Value::Integer(20)],
        vec![Value::Integer(3), Value::Integer(999)],
    ]);
}

#[test]
fn test_update_in_subquery_empty_result() {
    // IN (empty subquery) should affect 0 rows.
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();
    db.execute("UPDATE t SET v = 999 WHERE id IN (SELECT id FROM t WHERE v > 100)").unwrap();
    let r = q(&db, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(r, vec![
        vec![Value::Integer(1), Value::Integer(10)],
        vec![Value::Integer(2), Value::Integer(20)],
    ]);
}

// =========================================================================
// UPDATE with scalar subquery comparison
// =========================================================================

#[test]
fn test_update_scalar_subquery_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();
    db.execute("UPDATE t SET v = 0 WHERE v > (SELECT AVG(v) FROM t)").unwrap();
    let r = q(&db, "SELECT id, v FROM t ORDER BY id");
    // AVG = 20; rows with v > 20 (id=3) → v=0
    assert_eq!(r, vec![
        vec![Value::Integer(1), Value::Integer(10)],
        vec![Value::Integer(2), Value::Integer(20)],
        vec![Value::Integer(3), Value::Integer(0)],
    ]);
}

// =========================================================================
// Regression: plain UPDATE (no subquery) still works
// =========================================================================

#[test]
fn test_update_literal_in() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();
    db.execute("UPDATE t SET v = v * 2 WHERE id IN (2, 3)").unwrap();
    let r = q(&db, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(r, vec![
        vec![Value::Integer(1), Value::Integer(10)],
        vec![Value::Integer(2), Value::Integer(40)],
        vec![Value::Integer(3), Value::Integer(60)],
    ]);
}

#[test]
fn test_update_simple_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();
    db.execute("UPDATE t SET v = 99 WHERE id = 1").unwrap();
    let r = q(&db, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(r[0], vec![Value::Integer(1), Value::Integer(99)]);
    assert_eq!(r[1], vec![Value::Integer(2), Value::Integer(20)]);
}
