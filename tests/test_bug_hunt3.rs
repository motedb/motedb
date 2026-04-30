//! Regression tests for bugs found in audit round 3

use motedb::{Database, types::Value, sql::QueryResult};
use tempfile::TempDir;

fn create_db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("test.mote")).unwrap();
    (db, dir)
}

fn exec(db: &Database, sql: &str) -> QueryResult {
    db.execute(sql).unwrap().materialize().unwrap()
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match exec(db, sql) {
        QueryResult::Select { rows, .. } => rows,
        _ => vec![],
    }
}

// === Bug: IS NULL AND ... breaks parsing ===

#[test]
fn test_is_null_and_condition() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT, name TEXT, age INT)").unwrap();
    db.execute("INSERT INTO t (id, name, age) VALUES (1, 'Alice', 30)").unwrap();
    db.execute("INSERT INTO t (id, name, age) VALUES (2, NULL, 25)").unwrap();
    db.execute("INSERT INTO t (id, name, age) VALUES (3, 'Bob', NULL)").unwrap();

    let r = rows(&db, "SELECT id FROM t WHERE name IS NULL AND age = 25");
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Integer(2));
}

#[test]
fn test_not_in_and_condition() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT, v INT)").unwrap();
    db.execute("INSERT INTO t (id, v) VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t (id, v) VALUES (2, 20)").unwrap();
    db.execute("INSERT INTO t (id, v) VALUES (3, 30)").unwrap();

    let r = rows(&db, "SELECT id FROM t WHERE v NOT IN (10, 30) AND id > 0");
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Integer(2));
}

#[test]
fn test_like_and_order() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT, name TEXT)").unwrap();
    db.execute("INSERT INTO t (id, name) VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO t (id, name) VALUES (2, 'Bob')").unwrap();
    db.execute("INSERT INTO t (id, name) VALUES (3, 'alex')").unwrap();

    let r = rows(&db, "SELECT id FROM t WHERE name LIKE 'A%' ORDER BY id");
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Integer(1));
}

#[test]
fn test_between_and_condition() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT, v INT)").unwrap();
    db.execute("INSERT INTO t (id, v) VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t (id, v) VALUES (2, 20)").unwrap();
    db.execute("INSERT INTO t (id, v) VALUES (3, 30)").unwrap();

    let r = rows(&db, "SELECT id FROM t WHERE v BETWEEN 15 AND 25 AND id > 0");
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Integer(2));
}

#[test]
fn test_is_not_null_or_condition() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT, name TEXT)").unwrap();
    db.execute("INSERT INTO t (id, name) VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t (id, name) VALUES (2, 'Bob')").unwrap();

    let r = rows(&db, "SELECT id FROM t WHERE name IS NOT NULL OR id = 1");
    assert_eq!(r.len(), 2);
}

// === Bug: Right-associativity gives wrong arithmetic ===

#[test]
fn test_left_associative_subtraction() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    db.execute("INSERT INTO t (id) VALUES (1)").unwrap();

    let r = rows(&db, "SELECT 10 - 5 - 3 AS v FROM t");
    assert_eq!(r.len(), 1);
    // (10 - 5) - 3 = 2, NOT 10 - (5 - 3) = 8
    assert_eq!(r[0][0], Value::Integer(2));
}

#[test]
fn test_left_associative_division() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    db.execute("INSERT INTO t (id) VALUES (1)").unwrap();

    let r = rows(&db, "SELECT 100 / 10 / 2 AS v FROM t");
    // (100 / 10) / 2 = 5, NOT 100 / (10 / 2) = 20
    assert_eq!(r[0][0], Value::Integer(5));
}

// === Bug: log() maps to ln instead of log10 ===

#[test]
fn test_log_is_log10() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (v FLOAT)").unwrap();
    db.execute("INSERT INTO t (v) VALUES (100.0)").unwrap();

    let r = rows(&db, "SELECT log(v) AS res FROM t");
    if let Value::Float(v) = r[0][0] {
        let diff = (v - 2.0).abs();
        assert!(diff < 0.001, "log(100) should be 2.0, got {}", v);
    } else {
        panic!("Expected Float result, got {:?}", r[0][0]);
    }
}

#[test]
fn test_ln_is_natural_log() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (v FLOAT)").unwrap();
    db.execute("INSERT INTO t (v) VALUES (2.718281828)").unwrap();

    let r = rows(&db, "SELECT ln(v) AS res FROM t");
    if let Value::Float(v) = r[0][0] {
        let diff = (v - 1.0).abs();
        assert!(diff < 0.001, "ln(e) should be 1.0, got {}", v);
    } else {
        panic!("Expected Float result, got {:?}", r[0][0]);
    }
}

// === Bug: UTF-8 strings ===

#[test]
fn test_utf8_string_insert_select() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT, name TEXT)").unwrap();
    db.execute("INSERT INTO t (id, name) VALUES (1, '日本語')").unwrap();
    db.execute("INSERT INTO t (id, name) VALUES (2, '中文测试')").unwrap();

    let r = rows(&db, "SELECT name FROM t ORDER BY id");
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Text("日本語".to_string()));
    assert_eq!(r[1][0], Value::Text("中文测试".to_string()));
}

// === Bug: SQL standard doubled-quote escaping ===

#[test]
fn test_sql_standard_quote_escape() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT, name TEXT)").unwrap();
    db.execute("INSERT INTO t (id, name) VALUES (1, 'it''s')").unwrap();

    let r = rows(&db, "SELECT name FROM t WHERE id = 1");
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("it's".to_string()));
}

// === Complex combined expression parsing ===

#[test]
fn test_complex_where_with_postfix_and_infix() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT, a INT, b INT, c TEXT)").unwrap();
    db.execute("INSERT INTO t (id, a, b, c) VALUES (1, 10, 20, 'hello')").unwrap();
    db.execute("INSERT INTO t (id, a, b, c) VALUES (2, 30, 40, 'world')").unwrap();
    db.execute("INSERT INTO t (id, a, b, c) VALUES (3, 10, 40, NULL)").unwrap();

    // IS NULL + AND + comparison
    let r = rows(&db, "SELECT id FROM t WHERE c IS NULL AND a = 10");
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Integer(3));

    // NOT IN + OR
    let r = rows(&db, "SELECT id FROM t WHERE a NOT IN (10) OR b = 20 ORDER BY id");
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Integer(1));
    assert_eq!(r[1][0], Value::Integer(2));

    // IS NOT NULL + BETWEEN + AND
    let r = rows(&db, "SELECT id FROM t WHERE c IS NOT NULL AND b BETWEEN 15 AND 45 ORDER BY id");
    assert_eq!(r.len(), 2);
}

// === YEAR/MONTH/DAY accuracy (tested via unit tests) ===
// Note: TIMESTAMP() constructor is not yet supported in SQL, so we test
// days_to_date via unit tests in evaluator.rs instead.
