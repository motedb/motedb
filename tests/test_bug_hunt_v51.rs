//! Bug Hunt v51 — Float→Integer INSERT data corruption.
//!
//! **Bug:** `INSERT INTO t(v) VALUES (3.0)` where `v` is an INTEGER column
//! stored `4613937818241073152` instead of `3`. The validate_row function
//! ALLOWED whole-number Floats for Integer columns (to support overflow
//! promotion from arithmetic), but no code path coerced the Float back to
//! Integer before storage. The f64 bit pattern was then reinterpreted as i64
//! on read → silent data corruption (3.0 → 4613937818241073152).
//!
//! **Fix:** Added Float→Integer coercion in all INSERT paths:
//!   - crud.rs insert_row_to_table (single-row INSERT)
//!   - crud.rs batch_insert_rows_to_table (batch INSERT < 100 rows)
//!   - crud.rs fast_batch_insert (AUTO_INCREMENT batch INSERT ≥ 100 rows)
//!   - row_converter.rs sql_row_to_row + values_to_row_by_columns
//! Fractional floats (3.14) are still correctly rejected by validate_row.

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

// ─────────────────────────────────────────────────────────────────────────
// Single-row INSERT: float-to-int coercion
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_insert_float_whole_into_int() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 3.0)").unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

#[test]
fn test_insert_float_large_whole_into_int() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v BIGINT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 1000000.0)").unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(1000000)]]);
}

#[test]
fn test_insert_float_negative_whole_into_int() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, -42.0)").unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(-42)]]);
}

#[test]
fn test_insert_float_fractional_into_int_rejected() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    // 3.14 is fractional — must be rejected, not truncated to 3.
    let r = db.execute("INSERT INTO t VALUES (1, 3.14)");
    assert!(r.is_err(), "fractional float into INT should error");
}

// ─────────────────────────────────────────────────────────────────────────
// Float column accepts both int and float (no corruption)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_insert_int_into_float() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 42)").unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Float(42.0)]]);
}

#[test]
#[allow(clippy::approx_constant)] // 3.14 是普通样例浮点，非 π
fn test_insert_float_into_float() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 3.14)").unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Float(3.14)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Batch INSERT path (< 100 rows): float-to-int coercion
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_batch_insert_float_into_int() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 1.0), (2, 2.0), (3, 3.0)")
        .unwrap();
    let r = q(&db, "SELECT v FROM t ORDER BY id");
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(3)],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Fast batch INSERT path (≥ 100 rows, AUTO_INCREMENT): float-to-int coercion
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_fast_batch_insert_float_into_int() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY AUTO_INCREMENT, v INT)")
        .unwrap();
    // 150 rows triggers fast_batch_insert path
    let mut sql = String::from("INSERT INTO t(v) VALUES ");
    for i in 0..150 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({}.0)", i));
    }
    db.execute(&sql).unwrap();
    let r = q(&db, "SELECT v FROM t ORDER BY id LIMIT 5");
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(0)],
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(3)],
            vec![Value::Integer(4)],
        ]
    );
    // Verify total count and that all are correct
    let cnt = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(cnt, vec![vec![Value::Integer(150)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// UPDATE path: float-to-int coercion (was already fixed in round 43/49,
// regression guard here)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_update_float_to_int() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    // v + 0.0 promotes to float, should coerce back to int 10
    db.execute("UPDATE t SET v = v + 0.0 WHERE id = 1").unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(10)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Survival across checkpoint/reopen
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_float_to_int_survives_checkpoint() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 42.0)").unwrap();
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(42)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// NULL still works after the coercion loop (must not crash on NULL values)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_insert_null_after_coercion_fix() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Null]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Negative literals / constant expressions in INSERT VALUES
//
// Bug: the parser represents `-1e15`, `-(5.0)` as UnaryOp(Minus, Literal),
// not as negative Literals. INSERT VALUES only accepted Literal/Parameter,
// so any value with a leading minus sign (or any constant expression) was
// rejected with "INSERT VALUES must be literals or parameters".
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_insert_negative_float_scientific() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, -1e15)").unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Float(-1e15)]]);
}

#[test]
fn test_insert_negative_parenthesized() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, -(5.0))").unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Float(-5.0)]]);
}

#[test]
fn test_insert_negative_int() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, -42)").unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(-42)]]);
}

#[test]
fn test_insert_constant_arithmetic() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    // Constant expression in VALUES — should be evaluated.
    db.execute("INSERT INTO t VALUES (1, 2 * 3)").unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(6)]]);
}

#[test]
fn test_insert_negative_scientific_small() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, -2.5e-3)").unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Float(-0.0025)]]);
}
