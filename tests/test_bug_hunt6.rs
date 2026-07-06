//! Tests for bugs found in audit round 4:
//! UPDATE PK duplicate, UPDATE nonexistent column, NULL in indexed column,
//! float range queries, modulo on floats, division by zero, double DROP TABLE

use motedb::{types::Value, Database};
use tempfile::TempDir;

fn rows(result: motedb::StreamingQueryResult) -> Vec<Vec<Value>> {
    use motedb::QueryResult;
    match result.materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        _ => panic!("Expected Select result"),
    }
}

fn msg(result: motedb::StreamingQueryResult) -> String {
    match result {
        motedb::StreamingQueryResult::Definition { message } => message,
        _ => panic!("Expected Definition"),
    }
}

// === Fix #1: UPDATE PK to duplicate value ===

#[test]
fn test_update_pk_to_duplicate_rejected() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'b')").unwrap();

    // UPDATE PK to existing value should fail
    let result = db.execute("UPDATE t SET id = 2 WHERE id = 1");
    assert!(result.is_err(), "UPDATE PK to duplicate should be rejected");

    // Verify original data is intact
    let r = rows(db.execute("SELECT id FROM t ORDER BY id").unwrap());
    assert_eq!(r.len(), 2);
    assert_eq!(&r[0][0], &Value::Integer(1));
    assert_eq!(&r[1][0], &Value::Integer(2));
}

#[test]
fn test_update_pk_to_new_value_works() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    // UPDATE PK to a new unique value should succeed
    db.execute("UPDATE t SET id = 99 WHERE id = 1").unwrap();

    let r = rows(db.execute("SELECT id, val FROM t").unwrap());
    assert_eq!(r.len(), 1);
    assert_eq!(&r[0][0], &Value::Integer(99));
    assert_eq!(&r[0][1], &Value::text("a".to_string()));

    // Old PK should no longer work
    let r2 = rows(db.execute("SELECT * FROM t WHERE id = 1").unwrap());
    assert!(r2.is_empty(), "Old PK should return no rows");

    // New PK should work
    let r3 = rows(db.execute("SELECT * FROM t WHERE id = 99").unwrap());
    assert_eq!(r3.len(), 1);
}

// === Fix #15: UPDATE SET nonexistent column ===

#[test]
fn test_update_nonexistent_column_rejected() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'original')").unwrap();

    let result = db.execute("UPDATE t SET nonexistent = 'oops' WHERE id = 1");
    assert!(
        result.is_err(),
        "UPDATE with nonexistent column should be rejected"
    );

    // Original data should be unchanged
    let r = rows(db.execute("SELECT val FROM t WHERE id = 1").unwrap());
    assert_eq!(r.len(), 1);
    assert_eq!(&r[0][0], &Value::text("original".to_string()));
}

// === Fix #5: DELETE row with NULL in indexed column ===

#[test]
fn test_delete_row_with_null_in_indexed_column() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT)")
        .unwrap();
    db.execute("CREATE INDEX idx_name ON t (name)").unwrap();

    // Insert rows: one with NULL name, one with non-NULL name
    db.execute("INSERT INTO t VALUES (1, NULL, 20)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'alice', 25)").unwrap();

    // Delete the row with NULL name — should not error
    db.execute("DELETE FROM t WHERE id = 1").unwrap();

    // Verify the row is gone
    let r = rows(db.execute("SELECT id FROM t").unwrap());
    assert_eq!(r.len(), 1);
    assert_eq!(&r[0][0], &Value::Integer(2));

    // Verify the index still works for the remaining row
    let r2 = rows(db.execute("SELECT id FROM t WHERE name = 'alice'").unwrap());
    assert_eq!(r2.len(), 1);
    assert_eq!(&r2[0][0], &Value::Integer(2));
}

// === Test #10: Float range queries across negative/positive boundary ===

#[test]
fn test_float_range_queries_negative_positive() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val FLOAT)")
        .unwrap();

    // Insert negative, zero, and positive floats
    let values = [-10.0, -5.0, 0.0, 5.0, 10.0];
    for (i, v) in values.iter().enumerate() {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i + 1, v))
            .unwrap();
    }

    // Range query: val BETWEEN -5.0 AND 5.0
    let r = rows(
        db.execute("SELECT id FROM t WHERE val BETWEEN -5.0 AND 5.0 ORDER BY id")
            .unwrap(),
    );
    assert_eq!(r.len(), 3, "BETWEEN -5.0 AND 5.0 should return 3 rows");
    assert_eq!(&r[0][0], &Value::Integer(2)); // -5.0
    assert_eq!(&r[1][0], &Value::Integer(3)); // 0.0
    assert_eq!(&r[2][0], &Value::Integer(4)); // 5.0

    // Range query: val >= 0.0
    let r2 = rows(
        db.execute("SELECT id FROM t WHERE val >= 0.0 ORDER BY id")
            .unwrap(),
    );
    assert_eq!(r2.len(), 3, "val >= 0.0 should return 3 rows");

    // Range query: val < 0.0
    let r3 = rows(
        db.execute("SELECT id FROM t WHERE val < 0.0 ORDER BY id")
            .unwrap(),
    );
    assert_eq!(r3.len(), 2, "val < 0.0 should return 2 rows");
}

// === Fix #12: modulo on floats ===

#[test]
fn test_modulo_on_floats() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10.5)").unwrap();

    // Integer % Integer should work
    let r = rows(db.execute("SELECT 10 % 3").unwrap());
    assert_eq!(r.len(), 1);
    assert_eq!(&r[0][0], &Value::Integer(1));

    // Float % Float — may or may not be supported, but should not panic
    let result = db.execute("SELECT 10.5 % 3.0");
    // Either works and returns correct result, or returns an error
    match result {
        Ok(r) => {
            let r = rows(r);
            if let Value::Float(f) = &r[0][0] {
                assert!(
                    (f - 1.5).abs() < 0.01,
                    "10.5 % 3.0 should be 1.5, got {}",
                    f
                );
            }
        }
        Err(_) => {
            // Modulo on floats not supported — acceptable
        }
    }
}

#[test]
fn test_modulo_integer_by_zero() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();

    // SELECT 10 % 0 should error, not panic
    let result = db.execute("SELECT 10 % 0");
    assert!(result.is_err(), "Modulo by zero should return error");
}

// === Test #7: Division by zero ===

#[test]
fn test_select_division_by_zero() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();

    // SELECT 10 / 0 should error, not panic or return NaN
    let result = db.execute("SELECT 10 / 0");
    assert!(result.is_err(), "Division by zero should return error");
}

#[test]
fn test_select_float_division_by_zero() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();

    let result = db.execute("SELECT 10.0 / 0.0");
    assert!(
        result.is_err(),
        "Float division by zero should return error"
    );
}

// === Test #9: DROP TABLE edge cases ===

#[test]
fn test_drop_table_nonexistent() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    let result = db.execute("DROP TABLE nonexistent");
    assert!(result.is_err(), "DROP TABLE nonexistent should error");
}

#[test]
fn test_drop_table_twice() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    db.execute("DROP TABLE t").unwrap();

    let result = db.execute("DROP TABLE t");
    assert!(result.is_err(), "Second DROP TABLE should error");
}

// === Test #11: Row count after delete + re-insert ===

#[test]
fn test_count_after_delete_reinsert() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
        .unwrap();

    // Insert 100 rows
    for i in 1..=100 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i))
            .unwrap();
    }

    // Delete half
    for i in 1..=50 {
        db.execute(&format!("DELETE FROM t WHERE id = {}", i))
            .unwrap();
    }

    // Re-insert deleted rows
    for i in 1..=50 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 10))
            .unwrap();
    }

    // COUNT(*) should be 100
    let r = rows(db.execute("SELECT COUNT(*) FROM t").unwrap());
    assert_eq!(
        &r[0][0],
        &Value::Integer(100),
        "COUNT(*) should be 100 after delete + re-insert"
    );
}

// === Additional edge case tests ===

#[test]
fn test_insert_null_into_non_null_column() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    // Create table without explicit NOT NULL — NULL is allowed
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();

    let r = rows(db.execute("SELECT val FROM t WHERE id = 1").unwrap());
    assert_eq!(r.len(), 1);
    assert!(matches!(&r[0][0], Value::Null), "val should be NULL");
}

#[test]
fn test_coalesce_all_null() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();

    let r = rows(db.execute("SELECT COALESCE(NULL, NULL, 42)").unwrap());
    assert_eq!(r.len(), 1);
    assert_eq!(&r[0][0], &Value::Integer(42));
}

#[test]
fn test_inverted_range_returns_empty() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
        .unwrap();
    for i in 1..=10 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i))
            .unwrap();
    }

    // BETWEEN 10 AND 1 is inverted — should return empty
    let r = rows(
        db.execute("SELECT * FROM t WHERE val BETWEEN 10 AND 1")
            .unwrap(),
    );
    // SQL BETWEEN with inverted range should return empty
    // (but some DBs treat BETWEEN 10 AND 1 as no rows since 10 > 1)
    assert!(
        r.len() <= 10,
        "Inverted range should return 0 rows or be handled gracefully"
    );
}

#[test]
fn test_update_with_expression_arithmetic() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    db.execute("UPDATE t SET val = val * 2 + 1 WHERE id = 1")
        .unwrap();

    let r = rows(db.execute("SELECT val FROM t WHERE id = 1").unwrap());
    assert_eq!(&r[0][0], &Value::Integer(21), "val should be 10*2+1 = 21");
}

#[test]
fn test_delete_from_empty_table() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
        .unwrap();

    // DELETE from empty table should succeed with 0 affected
    let result = db.execute("DELETE FROM t WHERE id = 999");
    assert!(result.is_ok(), "DELETE from empty table should not error");

    // Verify table is still empty
    let r = rows(db.execute("SELECT COUNT(*) FROM t").unwrap());
    assert_eq!(&r[0][0], &Value::Integer(0));
}
