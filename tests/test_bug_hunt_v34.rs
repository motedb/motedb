//! Bug Hunt v34 — Bool/Int type coercion + scalar SELECT WHERE.
//!
//! Two bugs fixed:
//!  1. `1 = TRUE` returned false; `flag = 1` (BOOLEAN column vs INT literal)
//!     matched nothing. SQL treats TRUE as 1 and FALSE as 0 — comparisons and
//!     arithmetic between Bool and Integer/Float now coerce correctly.
//!  2. `SELECT 1 WHERE 0` ignored the WHERE clause and always returned a row.
//!     Scalar SELECT (no FROM) now evaluates WHERE and returns 0 rows when
//!     the condition is false.

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
// A: Bool/Int equality coercion (scalar context)
// =========================================================================

#[test]
fn test_int_equals_true() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 1 = TRUE");
    assert_eq!(r[0][0], Value::Bool(true));
}

#[test]
fn test_int_equals_false() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 0 = FALSE");
    assert_eq!(r[0][0], Value::Bool(true));
}

#[test]
fn test_int_not_equals_false() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 1 != FALSE");
    assert_eq!(r[0][0], Value::Bool(true));
}

#[test]
fn test_int_not_equals_true() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 2 != TRUE");
    assert_eq!(r[0][0], Value::Bool(true));
}

#[test]
fn test_bool_arithmetic() {
    let (db, _d) = db();
    let r = q(&db, "SELECT TRUE + 0");
    assert_eq!(r[0][0], Value::Integer(1));
    let r = q(&db, "SELECT FALSE + 0");
    assert_eq!(r[0][0], Value::Integer(0));
    let r = q(&db, "SELECT TRUE + 1");
    assert_eq!(r[0][0], Value::Integer(2));
}

// =========================================================================
// B: Bool/Int coercion in table WHERE (compiled + positional paths)
// =========================================================================

#[test]
fn test_where_bool_column_eq_int() {
    // BOOLEAN column compared with Integer literal.
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, flag BOOLEAN)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, TRUE), (2, FALSE), (3, TRUE)")
        .unwrap();
    let r = q(&db, "SELECT id FROM t WHERE flag = 1 ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

#[test]
fn test_where_bool_column_eq_zero() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, flag BOOLEAN)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, TRUE), (2, FALSE), (3, TRUE)")
        .unwrap();
    let r = q(&db, "SELECT id FROM t WHERE flag = 0");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_where_int_column_eq_true() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 1), (2, 0), (3, 1)")
        .unwrap();
    let r = q(&db, "SELECT id FROM t WHERE v = TRUE ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

#[test]
fn test_where_bool_column_neq_int() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, flag BOOLEAN)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, TRUE), (2, FALSE), (3, TRUE)")
        .unwrap();
    let r = q(&db, "SELECT id FROM t WHERE flag != 1 ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_where_pk_eq_true() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE id = TRUE");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

// =========================================================================
// C: Scalar SELECT WHERE clause
// =========================================================================

#[test]
fn test_scalar_where_false_returns_empty() {
    // Previously returned [[1]] — WHERE was ignored for scalar SELECT.
    let (db, _d) = db();
    let r = q(&db, "SELECT 1 WHERE 0");
    assert!(r.is_empty(), "SELECT 1 WHERE 0 should return 0 rows");
}

#[test]
fn test_scalar_where_true_returns_row() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 1 WHERE 1");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_scalar_where_bool_false() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 42 WHERE FALSE");
    assert!(r.is_empty());
}

#[test]
fn test_scalar_where_bool_true() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 42 WHERE TRUE");
    assert_eq!(r, vec![vec![Value::Integer(42)]]);
}

#[test]
fn test_scalar_where_expression_true() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 42 WHERE 1 = 1");
    assert_eq!(r, vec![vec![Value::Integer(42)]]);
}

#[test]
fn test_scalar_where_expression_false() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 42 WHERE 1 = 2");
    assert!(r.is_empty());
}

#[test]
fn test_scalar_where_no_clause_unchanged() {
    // Regression: scalar SELECT without WHERE still works.
    let (db, _d) = db();
    let r = q(&db, "SELECT 1 + 1");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}
