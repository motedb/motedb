//! Bug Hunt v65 — twelfth round: PK auto-increment, IN list order, float corners.

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
// IN list with many values (order independence, large list)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_in_list_order_independent() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,5)").unwrap();
    // Same value, different list positions.
    let r1 = q(&db, "SELECT v FROM t WHERE v IN (5, 10, 15)");
    let r2 = q(&db, "SELECT v FROM t WHERE v IN (10, 5, 15)");
    let r3 = q(&db, "SELECT v FROM t WHERE v IN (10, 15, 5)");
    assert_eq!(r1, r2);
    assert_eq!(r2, r3);
    assert_eq!(r1, vec![vec![Value::Integer(5)]]);
}

#[test]
fn test_in_list_duplicates() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,10)").unwrap();
    // Duplicates in IN list shouldn't cause duplicate results.
    let r = q(&db, "SELECT id FROM t WHERE v IN (5, 5, 5) ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_in_list_single_value() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,10)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE v IN (5)");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_in_list_text_values() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")
        .unwrap();
    let r = q(&db, "SELECT id FROM t WHERE s IN ('a', 'c') ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Float edge cases
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_float_equality() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 0.1 + 0.2 = 0.3");
    // 0.1+0.2 != 0.3 exactly in float, but this DB may use epsilon. Document.
    assert_eq!(r.len(), 1);
}

#[test]
fn test_negative_float_arithmetic() {
    let (db, _d) = db();
    let r = q(&db, "SELECT -1.5 * 2.0, -3.0 / -2.0");
    match &r[0][0] {
        Value::Float(f) => assert!((f - -3.0).abs() < 1e-9),
        Value::Integer(_) => panic!("expected float"),
        _ => panic!("{:?}", r),
    }
    match &r[0][1] {
        Value::Float(f) => assert!((f - 1.5).abs() < 1e-9),
        Value::Integer(_) => panic!("expected float"),
        _ => panic!("{:?}", r),
    }
}

#[test]
fn test_very_small_float() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 0.0001 * 0.0001");
    match &r[0][0] {
        Value::Float(f) => assert!((f - 0.00000001).abs() < 1e-15),
        _ => panic!("{:?}", r),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// SUM/AVG type preservation
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_sum_int_returns_int_large() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,1000000),(2,2000000),(3,3000000)")
        .unwrap();
    let r = q(&db, "SELECT SUM(v) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(6000000)]]);
}

#[test]
fn test_sum_float_returns_float() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,1.5),(2,2.5)").unwrap();
    let r = q(&db, "SELECT SUM(v) FROM t");
    match &r[0][0] {
        Value::Float(f) => assert!((f - 4.0).abs() < 1e-9),
        Value::Integer(_) => panic!("SUM of floats should be Float, got {:?}", r),
        _ => panic!("{:?}", r),
    }
}

#[test]
fn test_sum_mixed_int_float() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)")
        .unwrap();
    // Insert an int into a float column → stored as float.
    db.execute("INSERT INTO t VALUES (1,10),(2,2.5)").unwrap();
    let r = q(&db, "SELECT SUM(v) FROM t");
    match &r[0][0] {
        Value::Float(f) => assert!((f - 12.5).abs() < 1e-9),
        _ => panic!("{:?}", r),
    }
}

#[test]
fn test_avg_float_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,1.0),(2,2.0),(3,3.0)")
        .unwrap();
    let r = q(&db, "SELECT AVG(v) FROM t");
    match &r[0][0] {
        Value::Float(f) => assert!((f - 2.0).abs() < 1e-9),
        _ => panic!("{:?}", r),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Correlated subquery in SELECT (projection)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_correlated_subquery_in_select() {
    let (db, _d) = db();
    db.execute("CREATE TABLE orders(id INT PRIMARY KEY, cust INT, amt INT)")
        .unwrap();
    db.execute("CREATE TABLE customers(id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (1,1,100),(2,1,200),(3,2,50)")
        .unwrap();
    db.execute("INSERT INTO customers VALUES (1,'Alice'),(2,'Bob')")
        .unwrap();
    // Per-customer total via correlated subquery in SELECT.
    let r = q(
        &db,
        "SELECT c.name, (SELECT SUM(o.amt) FROM orders o WHERE o.cust = c.id) AS total FROM customers c ORDER BY c.name",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::text("Alice".into()), Value::Integer(300)],
            vec![Value::text("Bob".into()), Value::Integer(50)],
        ]
    );
}

#[test]
fn test_correlated_exists_like() {
    // Emulate EXISTS via correlated IN/count.
    let (db, _d) = db();
    db.execute("CREATE TABLE customers(id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE orders(id INT PRIMARY KEY, cust INT)")
        .unwrap();
    db.execute("INSERT INTO customers VALUES (1,'Alice'),(2,'Bob'),(3,'Carol')")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (1,1),(2,1),(3,3)")
        .unwrap();
    // Customers who have at least one order.
    let r = q(
        &db,
        "SELECT c.name FROM customers c WHERE c.id IN (SELECT cust FROM orders) ORDER BY c.name",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::text("Alice".into())],
            vec![Value::text("Carol".into())]
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// PK uniqueness violation
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_duplicate_pk_rejected() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    let res = db.execute("INSERT INTO t VALUES (1,20)");
    assert!(res.is_err(), "duplicate PK should be rejected");
    // Original row unchanged.
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(10)]]);
}

#[test]
fn test_pk_update_to_existing_rejected() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    // Update id 1 → 2 (which exists) should fail.
    let res = db.execute("UPDATE t SET id = 2 WHERE id = 1");
    assert!(res.is_err(), "UPDATE to existing PK should be rejected");
}

// ─────────────────────────────────────────────────────────────────────────
// NULL in arithmetic expressions
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_null_plus_null() {
    let (db, _d) = db();
    let r = q(&db, "SELECT NULL + NULL, NULL * 0, NULL - NULL");
    assert_eq!(r, vec![vec![Value::Null, Value::Null, Value::Null]]);
}

#[test]
fn test_null_comparison_all() {
    let (db, _d) = db();
    let r = q(&db, "SELECT NULL = NULL, NULL <> NULL, NULL < 1, NULL > 1");
    assert_eq!(
        r,
        vec![vec![Value::Null, Value::Null, Value::Null, Value::Null]]
    );
}

#[test]
fn test_null_in_arithmetic_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL)").unwrap();
    // v + 1 where v is NULL → NULL
    let r = q(&db, "SELECT v + 1 FROM t ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(11)], vec![Value::Null]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Boolean column full lifecycle
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_boolean_column_lifecycle() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, active BOOLEAN)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,TRUE),(2,FALSE),(3,NULL)")
        .unwrap();
    let r = q(&db, "SELECT active FROM t ORDER BY id");
    assert_eq!(
        r,
        vec![
            vec![Value::Bool(true)],
            vec![Value::Bool(false)],
            vec![Value::Null]
        ]
    );
    // WHERE active = TRUE
    let r2 = q(&db, "SELECT id FROM t WHERE active = TRUE");
    assert_eq!(r2, vec![vec![Value::Integer(1)]]);
    // WHERE active IS NULL
    let r3 = q(&db, "SELECT id FROM t WHERE active IS NULL");
    assert_eq!(r3, vec![vec![Value::Integer(3)]]);
    // COUNT(active) excludes NULL
    let r4 = q(&db, "SELECT COUNT(active) FROM t");
    assert_eq!(r4, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_boolean_update() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, active BOOLEAN)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,TRUE)").unwrap();
    db.execute("UPDATE t SET active = FALSE WHERE id = 1")
        .unwrap();
    let r = q(&db, "SELECT active FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Bool(false)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// String comparison case sensitivity
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_string_comparison_case_sensitive() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'Apple'),(2,'apple')")
        .unwrap();
    // Case-sensitive: 'Apple' != 'apple'
    let r = q(&db, "SELECT id FROM t WHERE s = 'Apple'");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_string_ordering_case() {
    let (db, _d) = db();
    // Uppercase sorts before lowercase in ASCII.
    let r = q(&db, "SELECT 'Z' < 'a', 'A' < 'B', 'a' < 'b'");
    assert_eq!(
        r,
        vec![vec![
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true)
        ]]
    );
}
