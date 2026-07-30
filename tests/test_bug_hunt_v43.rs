//! Bug Hunt v43 — Integer overflow in UPDATE silently kept old value.
//!
//! `UPDATE t SET v = v + 1` where v = i64::MAX silently kept the old value
//! instead of erroring or promoting correctly. Root cause:
//!  1. positional_fast_add returned None on overflow → try_fast_update
//!     fell through, but the Float→Integer coercion in update_row_with_schema_ref
//!     used `*f as i64` which SATURATES (9.22e18 → i64::MAX), silently
//!     truncating back to the original value.
//!  2. The range check `*f <= i64::MAX as f64` was unreliable because
//!     `i64::MAX as f64` rounds UP to 2^63 (next representable f64),
//!     so the overflow value 2^63 passed the check.
//!
//! Fixed by:
//!  - positional_fast_add promotes to Float on overflow (matches eval_expr_on_row)
//!  - Range check uses strict bounds (< 2^63, > -(2^63+1))
//!  - validate_row rejects floats outside i64 range → UPDATE errors cleanly

use motedb::sql::QueryResult;
use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    (db, dir)
}

fn q_err(db: &Database, sql: &str) -> String {
    match db.execute(sql).and_then(|r| r.materialize()) {
        Ok(_) => "OK".to_string(),
        Err(e) => format!("{}", e),
    }
}

fn q(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).and_then(|r| r.materialize()) {
        Ok(QueryResult::Select { rows, .. }) => rows,
        _ => vec![],
    }
}

#[test]
fn test_update_overflow_errors() {
    // UPDATE v = v + 1 where v = i64::MAX should error (not silently keep old value).
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 9223372036854775807)").unwrap();
    let err = q_err(&db, "UPDATE t SET v = v + 1 WHERE id = 1");
    assert!(err.contains("Type mismatch") || err.contains("Integer"),
           "expected type error, got: {}", err);
    // Value unchanged.
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r[0][0], Value::Integer(9223372036854775807));
}

#[test]
fn test_update_overflow_mul_errors() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 9223372036854775807)").unwrap();
    let err = q_err(&db, "UPDATE t SET v = v * 2 WHERE id = 1");
    assert!(err.contains("Type mismatch") || err.contains("Integer"));
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r[0][0], Value::Integer(9223372036854775807));
}

#[test]
fn test_update_normal_add_works() {
    // Regression: normal UPDATE v = v + N still works.
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 100)").unwrap();
    db.execute("UPDATE t SET v = v + 50 WHERE id = 1").unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r[0][0], Value::Integer(150));
}

#[test]
fn test_update_near_max_add() {
    // v = i64::MAX - 10, add 5 → should work (no overflow).
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 9223372036854775797)").unwrap();
    db.execute("UPDATE t SET v = v + 5 WHERE id = 1").unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r[0][0], Value::Integer(9223372036854775802));
}

#[test]
fn test_select_overflow_promotes_to_float() {
    // SELECT v + 1 should still promote to Float (read path unaffected).
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 9223372036854775807)").unwrap();
    let r = q(&db, "SELECT v + 1 FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Float(_) => {} // correct: promoted
        other => panic!("expected Float, got {:?}", other),
    }
}
