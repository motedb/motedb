//! Tests for bugs found in audit round 5:
//! Hash join large int precision, TRUE AND NULL, SUBSTR position 0,
//! NULL PK rejected, DROP TABLE persistence, row count underflow,
//! Integer overflow fast paths, AND/OR NULL handling

use motedb::{Database, types::Value};
use tempfile::TempDir;

fn rows(result: motedb::StreamingQueryResult) -> Vec<Vec<Value>> {
    use motedb::QueryResult;
    match result.materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        _ => panic!("Expected Select result"),
    }
}

fn row(db: &Database, sql: &str) -> Vec<Value> {
    let r = rows(db.execute(sql).unwrap());
    assert_eq!(r.len(), 1);
    r[0].clone()
}

// === TRUE AND NULL, FALSE OR NULL ===

#[test]
fn test_and_null_returns_null() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    // TRUE AND NULL = NULL (not FALSE)
    let r = rows(db.execute("SELECT TRUE AND NULL").unwrap());
    assert_eq!(r.len(), 1);
    assert!(matches!(&r[0][0], Value::Null), "TRUE AND NULL should be NULL, got {:?}", r[0][0]);
}

#[test]
fn test_false_and_null_returns_false() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    // FALSE AND NULL = FALSE (short circuit)
    let r = rows(db.execute("SELECT FALSE AND NULL").unwrap());
    assert_eq!(r.len(), 1);
    assert_eq!(&r[0][0], &Value::Bool(false));
}

#[test]
fn test_true_or_null_returns_true() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    // TRUE OR NULL = TRUE
    let r = rows(db.execute("SELECT TRUE OR NULL").unwrap());
    assert_eq!(r.len(), 1);
    assert_eq!(&r[0][0], &Value::Bool(true));
}

#[test]
fn test_false_or_null_returns_null() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    // FALSE OR NULL = NULL (not FALSE)
    let r = rows(db.execute("SELECT FALSE OR NULL").unwrap());
    assert_eq!(r.len(), 1);
    assert!(matches!(&r[0][0], Value::Null), "FALSE OR NULL should be NULL");
}

#[test]
fn test_and_or_null_in_where() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL, 1)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 0, NULL)").unwrap();

    // a=1 AND b=1 → should be NULL for id=1 (b is NULL), row excluded
    let r = rows(db.execute("SELECT id FROM t WHERE a = 1 AND b = 1").unwrap());
    assert!(r.is_empty(), "a=1 AND b=NULL should not match any row");

    // a=1 OR b=1 → should match id=1 (a=1, b=NULL)
    let r = rows(db.execute("SELECT id FROM t WHERE a = 1 OR b = 1").unwrap());
    assert_eq!(r.len(), 2); // rows 1 and 2
}

// === SUBSTR position 0 ===

#[test]
fn test_substr_position_zero_treated_as_one() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    // SUBSTR with position 0 should be treated as position 1 (SQL standard)
    let r = row(&db, "SELECT SUBSTR('hello', 0, 2)");
    assert_eq!(&r[0], &Value::text("he".to_string()), "SUBSTR('hello', 0, 2) should be 'he'");
}

#[test]
fn test_substr_negative_position() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    // Negative position counts from end
    let r = row(&db, "SELECT SUBSTR('hello', -2)");
    assert_eq!(&r[0], &Value::text("lo".to_string()), "SUBSTR('hello', -2) should be 'lo'");
}

// === NULL PK rejected ===

#[test]
fn test_null_pk_rejected() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)").unwrap();

    let result = db.execute("INSERT INTO t VALUES (NULL, 'x')");
    assert!(result.is_err(), "NULL primary key should be rejected");
}

// === DROP TABLE persistence ===

#[test]
fn test_drop_table_persists_across_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
        db.execute("INSERT INTO t VALUES (1)").unwrap();
        db.execute("INSERT INTO t VALUES (2)").unwrap();
        db.execute("DROP TABLE t").unwrap();
        db.close().unwrap();
    }

    {
        let db = Database::open(&path).unwrap();
        // Table should be gone
        let result = db.execute("SELECT * FROM t");
        assert!(result.is_err(), "Table 't' should not exist after DROP TABLE + reopen");
    }
}

// === Row count correctness ===

#[test]
fn test_row_count_after_delete_all() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();

    for i in 1..=10 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap();
    }

    // Delete all rows
    for i in 1..=10 {
        db.execute(&format!("DELETE FROM t WHERE id = {}", i)).unwrap();
    }

    // COUNT(*) should be 0
    let r = rows(db.execute("SELECT COUNT(*) FROM t").unwrap());
    assert_eq!(&r[0][0], &Value::Integer(0));

    // Re-insert should work correctly and COUNT(*) should be 1
    db.execute("INSERT INTO t VALUES (99, 99)").unwrap();
    let r = rows(db.execute("SELECT COUNT(*) FROM t").unwrap());
    assert_eq!(&r[0][0], &Value::Integer(1), "COUNT(*) after delete-all + re-insert should be 1");
}

// === Integer arithmetic overflow in fast paths ===

#[test]
fn test_integer_overflow_add() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a BIGINT)").unwrap();
    db.execute(&format!("INSERT INTO t VALUES (1, {})", i64::MAX)).unwrap();

    // INT_MAX + 1 — should either error, or produce a result > i64::MAX (not wrap to INT_MIN)
    let result = db.execute(&format!("UPDATE t SET a = a + 1 WHERE id = 1"));
    match result {
        Ok(_) => {
            let r = row(&db, "SELECT a FROM t WHERE id = 1");
            match &r[0] {
                Value::Float(f) => {
                    // Overflow-to-f64 is acceptable: > i64::MAX
                    assert!(*f > i64::MAX as f64, "Float result {} should be > i64::MAX", f);
                }
                Value::Integer(i) => {
                    // Should NOT wrap to negative or be a small value
                    assert!(*i > 0, "Integer overflow should not wrap to {}, got error instead", i);
                }
                _ => {}
            }
        }
        Err(_) => { /* Error is acceptable */ }
    }
}

#[test]
fn test_integer_div_by_neg_one_overflow() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();

    // i64::MIN / -1 overflows — should error or promote to float, not return 0
    let result = db.execute(&format!("SELECT {} / -1", i64::MIN));
    match result {
        Ok(r) => {
            let r = rows(r);
            // Must not be 0 (bug would return 0)
            if matches!(&r[0][0], Value::Integer(0)) {
                panic!("i64::MIN / -1 returned 0 (should error or promote)");
            }
        }
        Err(_) => { /* Error is acceptable */ }
    }
}

// === Unary minus for i64::MIN ===

#[test]
fn test_unary_minus_i64_min() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v BIGINT)").unwrap();
    db.execute(&format!("INSERT INTO t VALUES (1, {})", i64::MIN)).unwrap();

    // -(-9223372036854775808) should promote to Float or error
    let r = row(&db, &format!("SELECT -v FROM t WHERE id = 1"));
    // Should be Float(i64::MIN as f64) = 9.223372036854776e18
    assert!(matches!(&r[0], Value::Float(f) if *f > 9e18), "-i64::MIN should promote to Float, got {:?}", r[0]);
}

// === AND/OR with NULL in WHERE via table data ===

#[test]
fn test_and_condition_with_null_column() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, x INT, y INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 1, 2)").unwrap();

    // x=1 AND y=2 should only match row 2
    let r = rows(db.execute("SELECT id FROM t WHERE x = 1 AND y = 2 ORDER BY id").unwrap());
    assert_eq!(r.len(), 1);
    assert_eq!(&r[0][0], &Value::Integer(2));
}

// === Large integer scan correctness ===

#[test]
fn test_large_integers_stored_and_retrieved() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, big_val BIGINT)").unwrap();

    // Values near i64 boundaries
    let values = [i64::MAX, i64::MIN, i64::MAX - 1, 0, 1, -1];
    for (i, v) in values.iter().enumerate() {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i + 1, v)).unwrap();
    }

    for (i, v) in values.iter().enumerate() {
        let r = row(&db, &format!("SELECT big_val FROM t WHERE id = {}", i + 1));
        assert_eq!(&r[0], &Value::Integer(*v), "Large int mismatch for id={}", i + 1);
    }
}

// === SUBSTR edge cases ===

#[test]
fn test_substr_length_exceeds_string() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    let r = row(&db, "SELECT SUBSTR('abc', 2, 100)");
    assert_eq!(&r[0], &Value::text("bc".to_string()));
}

#[test]
fn test_substr_start_beyond_length() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    let r = row(&db, "SELECT SUBSTR('abc', 10)");
    assert_eq!(&r[0], &Value::text("".to_string()));
}
