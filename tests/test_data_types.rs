//! Tests for data types: INT, FLOAT, TEXT, BOOL, TIMESTAMP, VECTOR, BLOB,
//! NULL handling, type coercion, edge values

use motedb::types::Tensor;
use motedb::{types::Value, Database};
use tempfile::TempDir;

fn rows(result: motedb::StreamingQueryResult) -> Vec<Vec<Value>> {
    use motedb::QueryResult;
    match result.materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        _ => panic!("Expected Select result"),
    }
}

fn row(result: motedb::StreamingQueryResult) -> Vec<Value> {
    let r = rows(result);
    assert_eq!(r.len(), 1);
    r.into_iter().next().unwrap()
}

// === Integer edge values ===

#[test]
fn test_large_integers() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 9999999999999)")
        .unwrap();

    let r = row(db.execute("SELECT val FROM t WHERE id = 1").unwrap());
    assert_eq!(&r[0], &Value::Integer(9999999999999i64));
}

#[test]
fn test_zero_and_negative() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 0)").unwrap();
    db.execute("INSERT INTO t VALUES (2, -1)").unwrap();
    db.execute("INSERT INTO t VALUES (3, -999)").unwrap();

    let r = rows(db.execute("SELECT val FROM t ORDER BY val").unwrap());
    assert_eq!(&r[0][0], &Value::Integer(-999));
    assert_eq!(&r[1][0], &Value::Integer(-1));
    assert_eq!(&r[2][0], &Value::Integer(0));
}

// === Float edge values ===

#[test]
fn test_float_precision() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 3.14159265358979)")
        .unwrap();

    let r = row(db.execute("SELECT val FROM t WHERE id = 1").unwrap());
    match &r[0] {
        Value::Float(f) => assert!((f - 3.14159265358979).abs() < 1e-10),
        other => panic!("Expected Float, got {:?}", other),
    }
}

#[test]
fn test_float_zero() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 0.0)").unwrap();

    let r = row(db.execute("SELECT val FROM t WHERE id = 1").unwrap());
    match &r[0] {
        Value::Float(f) => assert!(*f == 0.0),
        other => panic!("Expected Float, got {:?}", other),
    }
}

// === Text edge values ===

#[test]
fn test_unicode_text() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, '你好世界')").unwrap();
    db.execute("INSERT INTO t VALUES (2, '🎉🚀💻')").unwrap();

    let r = row(db.execute("SELECT val FROM t WHERE id = 1").unwrap());
    assert_eq!(&r[0], &Value::text("你好世界".to_string()));
}

#[test]
fn test_long_text() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
        .unwrap();
    let long_str = "x".repeat(10000);
    db.execute(&format!("INSERT INTO t VALUES (1, '{}')", long_str))
        .unwrap();

    let r = row(db.execute("SELECT val FROM t WHERE id = 1").unwrap());
    match &r[0] {
        Value::Text(s) => assert_eq!(s.len(), 10000),
        other => panic!("Expected Text, got {:?}", other),
    }
}

#[test]
fn test_text_with_special_chars() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
        .unwrap();
    // Test with escaped single quotes
    db.execute("INSERT INTO t VALUES (1, 'it''s a test')")
        .unwrap();

    let r = row(db.execute("SELECT val FROM t WHERE id = 1").unwrap());
    match &r[0] {
        Value::Text(s) => assert!(s.contains("it") && s.contains("test")),
        other => panic!("Expected Text, got {:?}", other),
    }
}

// === Boolean type ===

#[test]
fn test_boolean_operations() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE flags (id INT PRIMARY KEY, active BOOLEAN)")
        .unwrap();
    db.execute("INSERT INTO flags VALUES (1, TRUE)").unwrap();
    db.execute("INSERT INTO flags VALUES (2, FALSE)").unwrap();
    db.execute("INSERT INTO flags VALUES (3, NULL)").unwrap();

    let r = rows(
        db.execute("SELECT id FROM flags WHERE active = TRUE ORDER BY id")
            .unwrap(),
    );
    assert_eq!(r.len(), 1);
    assert_eq!(&r[0][0], &Value::Integer(1));

    let r = rows(
        db.execute("SELECT id FROM flags WHERE active IS NULL")
            .unwrap(),
    );
    assert_eq!(r.len(), 1);
    assert_eq!(&r[0][0], &Value::Integer(3));
}

// === NULL comprehensive ===

#[test]
fn test_null_in_all_columns() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b TEXT, c FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL, NULL, NULL)")
        .unwrap();

    let r = row(db.execute("SELECT a, b, c FROM t WHERE id = 1").unwrap());
    assert!(matches!(&r[0], Value::Null));
    assert!(matches!(&r[1], Value::Null));
    assert!(matches!(&r[2], Value::Null));
}

#[test]
fn test_null_arithmetic() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();

    let r = row(db.execute("SELECT val + 10 FROM t WHERE id = 1").unwrap());
    assert!(matches!(&r[0], Value::Null), "NULL + anything = NULL");
}

#[test]
fn test_null_comparison() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 42)").unwrap();

    // val = NULL should match nothing
    let r = rows(db.execute("SELECT id FROM t WHERE val = NULL").unwrap());
    assert_eq!(r.len(), 0);

    // val IS NULL should match row 1
    let r = rows(db.execute("SELECT id FROM t WHERE val IS NULL").unwrap());
    assert_eq!(r.len(), 1);

    // val IS NOT NULL should match row 2
    let r = rows(
        db.execute("SELECT id FROM t WHERE val IS NOT NULL")
            .unwrap(),
    );
    assert_eq!(r.len(), 1);
    assert_eq!(&r[0][0], &Value::Integer(2));
}

// === Vector/Tensor type ===

#[test]
fn test_vector_insert_retrieve() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE vecs (id INT PRIMARY KEY, emb VECTOR(3))")
        .unwrap();

    let row = vec![
        Value::Integer(1),
        Value::tensor(Tensor::new(vec![1.0, 2.0, 3.0])),
    ];
    let row_id = db.insert_row("vecs", row).unwrap();

    let got = db.get_row("vecs", row_id).unwrap();
    assert!(got.is_some());
    match &got.unwrap()[1] {
        Value::Tensor(t) => {
            let data = t.as_f32();
            assert_eq!(data.len(), 3);
            assert!((data[0] - 1.0).abs() < 0.01);
        }
        other => panic!("Expected Tensor, got {:?}", other),
    }
}

#[test]
fn test_vector_null() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE vecs (id INT PRIMARY KEY, emb VECTOR(3))")
        .unwrap();

    let vals = vec![Value::Integer(1), Value::Null];
    db.insert_row("vecs", vals).unwrap();

    let r = row(db.execute("SELECT emb FROM vecs WHERE id = 1").unwrap());
    assert!(matches!(&r[0], Value::Null), "NULL vector should be NULL");
}

// === Timestamp type ===

#[test]
fn test_timestamp_insert_retrieve() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE events (id INT PRIMARY KEY, ts TIMESTAMP)")
        .unwrap();

    let row = vec![
        Value::Integer(1),
        Value::Timestamp(motedb::types::Timestamp::from_micros(1700000000000)),
    ];
    let row_id = db.insert_row("events", row).unwrap();

    let got = db.get_row("events", row_id).unwrap();
    assert!(got.is_some());
    match &got.unwrap()[1] {
        Value::Timestamp(ts) => {
            assert_eq!(ts.as_micros(), 1700000000000);
        }
        other => panic!("Expected Timestamp, got {:?}", other),
    }
}

// === Mixed types in one table ===

#[test]
fn test_all_types_in_one_table() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE mixed (id INT PRIMARY KEY, i INT, f FLOAT, t TEXT, b BOOLEAN)")
        .unwrap();
    db.execute("INSERT INTO mixed VALUES (1, 42, 3.14, 'hello', TRUE)")
        .unwrap();

    let r = row(db
        .execute("SELECT i, f, t, b FROM mixed WHERE id = 1")
        .unwrap());
    assert_eq!(&r[0], &Value::Integer(42));
    match &r[1] {
        Value::Float(f) => assert!((f - 3.14).abs() < 0.01),
        other => panic!("Expected Float, got {:?}", other),
    }
    assert_eq!(&r[2], &Value::text("hello".to_string()));
    match &r[3] {
        Value::Bool(b) => assert!(*b),
        other => panic!("Expected Bool, got {:?}", other),
    }
}

// === Type coercion in comparisons ===

#[test]
fn test_int_float_comparison() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10.0)").unwrap();

    // Compare float column with integer literal
    let r = rows(db.execute("SELECT id FROM t WHERE val = 10").unwrap());
    assert!(r.len() >= 1, "INT literal should compare with FLOAT column");

    let r = rows(db.execute("SELECT id FROM t WHERE val > 9").unwrap());
    assert!(r.len() >= 1);
}

// === Multiple tables with different schemas ===

#[test]
fn test_multiple_tables_isolation() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE a (id INT PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("CREATE TABLE b (id INT PRIMARY KEY, val INT)")
        .unwrap();
    db.execute("INSERT INTO a VALUES (1, 'text')").unwrap();
    db.execute("INSERT INTO b VALUES (1, 42)").unwrap();

    let ra = row(db.execute("SELECT val FROM a WHERE id = 1").unwrap());
    assert_eq!(&ra[0], &Value::text("text".to_string()));

    let rb = row(db.execute("SELECT val FROM b WHERE id = 1").unwrap());
    assert_eq!(&rb[0], &Value::Integer(42));
}

// === Table with many columns ===

#[test]
fn test_wide_table() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE wide (id INT PRIMARY KEY, c1 INT, c2 INT, c3 INT, c4 INT, c5 INT, c6 INT, c7 INT, c8 INT, c9 INT, c10 INT)").unwrap();
    db.execute("INSERT INTO wide VALUES (1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)")
        .unwrap();

    let r = row(db
        .execute("SELECT c1, c5, c10 FROM wide WHERE id = 1")
        .unwrap());
    assert_eq!(&r[0], &Value::Integer(1));
    assert_eq!(&r[1], &Value::Integer(5));
    assert_eq!(&r[2], &Value::Integer(10));
}
