//! Comprehensive correctness + performance test
//! Covers: all SQL operations, NULL semantics, aggregates, expressions, DDL,
//! prepared statements, transactions, recovery, stress, and performance benchmarks

use motedb::{Database, types::Value, sql::QueryResult, config::DBConfig};
use tempfile::TempDir;
use std::time::Instant;

fn setup_db(dir: &std::path::Path) -> Database {
    Database::create(dir.join("test.mote")).unwrap()
}

fn setup_db_fast(dir: &std::path::Path) -> Database {
    Database::create_with_config(dir.join("test.mote"), DBConfig::for_testing()).unwrap()
}

fn exec(db: &Database, sql: &str) -> QueryResult {
    db.execute(sql).unwrap().materialize().unwrap()
}

fn query_rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match exec(db, sql) {
        QueryResult::Select { rows, .. } => rows,
        _ => vec![],
    }
}

fn qr(db: &Database, sql: &str) -> Option<Vec<Value>> {
    query_rows(db, sql).into_iter().next()
}

fn qv(db: &Database, sql: &str) -> Value {
    let row = qr(db, sql).unwrap();
    row.into_iter().next().unwrap()
}

// ============================================================
// PART 1: DDL — CREATE / DROP / recreate
// ============================================================

#[test]
fn test_ddl_create_drop_create() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("DROP TABLE t").unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
    assert_eq!(qv(&db, "SELECT name FROM t WHERE id = 1"), Value::text("alice".into()));
}

#[test]
fn test_ddl_multiple_tables() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    for i in 0..5 {
        db.execute(&format!("CREATE TABLE t{} (id INT PRIMARY KEY, v INT)", i)).unwrap();
        db.execute(&format!("INSERT INTO t{} VALUES (1, {})", i, i * 10)).unwrap();
    }
    for i in 0..5 {
        let v = qv(&db, &format!("SELECT v FROM t{} WHERE id = 1", i));
        assert_eq!(v, Value::Integer(i as i64 * 10));
    }
    db.execute("DROP TABLE t2").unwrap();
    assert!(db.execute("SELECT * FROM t2").is_err());
    assert_eq!(qv(&db, "SELECT v FROM t3 WHERE id = 1"), Value::Integer(30));
}

#[test]
fn test_ddl_table_with_all_types() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, i INT, f FLOAT, s TEXT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, -42, 3.14, 'hello', 1)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 0, 0.0, '', 0)").unwrap();
    db.execute("INSERT INTO t VALUES (3, NULL, NULL, NULL, NULL)").unwrap();

    let rows = query_rows(&db, "SELECT * FROM t ORDER BY id");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][1], Value::Integer(-42));
    assert_eq!(rows[0][3], Value::text("hello".into()));
    assert_eq!(rows[2][1], Value::Null);
}

// ============================================================
// PART 2: INSERT correctness
// ============================================================

#[test]
fn test_insert_auto_increment() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v TEXT)").unwrap();
    for c in ['a', 'b', 'c', 'd', 'e'] {
        db.execute(&format!("INSERT INTO t (v) VALUES ('{}')", c)).unwrap();
    }
    let rows = query_rows(&db, "SELECT * FROM t ORDER BY id");
    assert_eq!(rows.len(), 5);
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row[0], Value::Integer(i as i64 + 1));
    }
}

#[test]
fn test_insert_null_columns() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b TEXT, c FLOAT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL, NULL, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 10, NULL, 1.5)").unwrap();
    db.execute("INSERT INTO t VALUES (3, NULL, 'text', NULL)").unwrap();

    let rows = query_rows(&db, "SELECT * FROM t ORDER BY id");
    assert_eq!(rows[0], vec![Value::Integer(1), Value::Null, Value::Null, Value::Null]);
    assert_eq!(rows[1][1], Value::Integer(10));
    assert_eq!(rows[1][2], Value::Null);
    assert_eq!(rows[2][2], Value::text("text".into()));
}

#[test]
fn test_insert_duplicate_pk_rejected() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    assert!(db.execute("INSERT INTO t VALUES (1, 20)").is_err());
    assert_eq!(qv(&db, "SELECT COUNT(*) FROM t"), Value::Integer(1));
}

#[test]
fn test_insert_wrong_column_count() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    assert!(db.execute("INSERT INTO t VALUES (1)").is_err());
    assert!(db.execute("INSERT INTO t VALUES (1, 2, 3)").is_err());
}

#[test]
fn test_insert_type_mismatch() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    assert!(db.execute("INSERT INTO t VALUES (1, 'text')").is_err());
    assert!(db.execute("INSERT INTO t VALUES ('abc', 1)").is_err());
}

#[test]
fn test_insert_named_columns() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t (id, b, a) VALUES (1, 20, 10)").unwrap();
    let row = qr(&db, "SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(row[0], Value::Integer(1));
    assert_eq!(row[1], Value::Integer(10));
    assert_eq!(row[2], Value::Integer(20));
}

// ============================================================
// PART 3: SELECT — projections, expressions, aggregates
// ============================================================

#[test]
fn test_select_star() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 42, 'hello')").unwrap();
    let row = qr(&db, "SELECT * FROM t").unwrap();
    assert_eq!(row, vec![Value::Integer(1), Value::Integer(42), Value::text("hello".into())]);
}

#[test]
fn test_select_specific_columns() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10, 20, 30)").unwrap();
    let row = qr(&db, "SELECT b, a FROM t WHERE id = 1").unwrap();
    assert_eq!(row, vec![Value::Integer(20), Value::Integer(10)]);
}

#[test]
fn test_select_arithmetic() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10, 3)").unwrap();
    // Use expression in WHERE to bypass PK fast path
    assert_eq!(qv(&db, "SELECT a + b FROM t WHERE a > 0"), Value::Integer(13));
    assert_eq!(qv(&db, "SELECT a - b FROM t WHERE a > 0"), Value::Integer(7));
    assert_eq!(qv(&db, "SELECT a * b FROM t WHERE a > 0"), Value::Integer(30));
    assert_eq!(qv(&db, "SELECT a / b FROM t WHERE a > 0"), Value::Integer(3));
}

#[test]
fn test_select_functions() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, s TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Hello World', -5)").unwrap();
    assert_eq!(qv(&db, "SELECT UPPER(s) FROM t"), Value::text("HELLO WORLD".into()));
    assert_eq!(qv(&db, "SELECT LOWER(s) FROM t"), Value::text("hello world".into()));
    assert_eq!(qv(&db, "SELECT LENGTH(s) FROM t"), Value::Integer(11));
    assert_eq!(qv(&db, "SELECT ABS(v) FROM t"), Value::Integer(5));
}

#[test]
fn test_select_concat() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'hello', 'world')").unwrap();
    assert_eq!(qv(&db, "SELECT CONCAT(a, ' ', b) FROM t"), Value::text("hello world".into()));
}

#[test]
fn test_select_null_literal() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let row = qr(&db, "SELECT id, NULL FROM t").unwrap();
    assert_eq!(row[0], Value::Integer(1));
    assert_eq!(row[1], Value::Null);
}

// ============================================================
// PART 4: Aggregates
// ============================================================

#[test]
fn test_aggregates_basic() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=5 { db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 10)).unwrap(); }

    assert_eq!(qv(&db, "SELECT COUNT(*) FROM t"), Value::Integer(5));
    assert_eq!(qv(&db, "SELECT SUM(v) FROM t"), Value::Integer(150));
    assert_eq!(qv(&db, "SELECT MIN(v) FROM t"), Value::Integer(10));
    assert_eq!(qv(&db, "SELECT MAX(v) FROM t"), Value::Integer(50));
    let avg = qv(&db, "SELECT AVG(v) FROM t");
    match avg { Value::Float(f) => assert!((f - 30.0).abs() < 0.01), _ => panic!("AVG should be float") }
}

#[test]
fn test_aggregates_with_nulls() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30)").unwrap();
    db.execute("INSERT INTO t VALUES (4, NULL)").unwrap();

    assert_eq!(qv(&db, "SELECT COUNT(*) FROM t"), Value::Integer(4));
    assert_eq!(qv(&db, "SELECT COUNT(v) FROM t"), Value::Integer(2));
    assert_eq!(qv(&db, "SELECT SUM(v) FROM t"), Value::Integer(40));
}

#[test]
fn test_aggregates_all_null() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();

    assert_eq!(qv(&db, "SELECT COUNT(*) FROM t"), Value::Integer(2));
    assert_eq!(qv(&db, "SELECT COUNT(v) FROM t"), Value::Integer(0));
    assert_eq!(qv(&db, "SELECT SUM(v) FROM t"), Value::Null);
    assert_eq!(qv(&db, "SELECT AVG(v) FROM t"), Value::Null);
}

#[test]
fn test_count_distinct() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (4, 30)").unwrap();
    db.execute("INSERT INTO t VALUES (5, 20)").unwrap();
    assert_eq!(qv(&db, "SELECT COUNT(DISTINCT v) FROM t"), Value::Integer(3));
}

#[test]
fn test_aggregates_empty_table() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    assert_eq!(qv(&db, "SELECT COUNT(*) FROM t"), Value::Integer(0));
    assert_eq!(qv(&db, "SELECT COUNT(v) FROM t"), Value::Integer(0));
}

// ============================================================
// PART 5: WHERE — all operators and expressions
// ============================================================

#[test]
fn test_where_comparison_ops() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=10 { db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap(); }

    assert_eq!(query_rows(&db, "SELECT * FROM t WHERE v = 5").len(), 1);
    assert_eq!(query_rows(&db, "SELECT * FROM t WHERE v != 5").len(), 9);
    assert_eq!(query_rows(&db, "SELECT * FROM t WHERE v < 5").len(), 4);
    assert_eq!(query_rows(&db, "SELECT * FROM t WHERE v <= 5").len(), 5);
    assert_eq!(query_rows(&db, "SELECT * FROM t WHERE v > 5").len(), 5);
    assert_eq!(query_rows(&db, "SELECT * FROM t WHERE v >= 5").len(), 6);
}

#[test]
fn test_where_and_or() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 1, 1)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 1, 0)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 0, 1)").unwrap();
    db.execute("INSERT INTO t VALUES (4, 0, 0)").unwrap();

    assert_eq!(query_rows(&db, "SELECT * FROM t WHERE a = 1 AND b = 1").len(), 1);
    assert_eq!(query_rows(&db, "SELECT * FROM t WHERE a = 1 OR b = 1").len(), 3);
    assert_eq!(query_rows(&db, "SELECT * FROM t WHERE a = 0 AND b = 0").len(), 1);
}

#[test]
fn test_where_is_null_is_not_null() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30)").unwrap();
    db.execute("INSERT INTO t VALUES (4, NULL)").unwrap();

    assert_eq!(query_rows(&db, "SELECT * FROM t WHERE v IS NULL").len(), 2);
    assert_eq!(query_rows(&db, "SELECT * FROM t WHERE v IS NOT NULL").len(), 2);
}

#[test]
fn test_where_in_not_in() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=5 { db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 10)).unwrap(); }

    assert_eq!(query_rows(&db, "SELECT * FROM t WHERE v IN (10, 30, 50)").len(), 3);
    assert_eq!(query_rows(&db, "SELECT * FROM t WHERE v NOT IN (10, 30, 50)").len(), 2);
}

#[test]
fn test_where_between() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=10 { db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 10)).unwrap(); }

    let rows = query_rows(&db, "SELECT * FROM t WHERE v BETWEEN 30 AND 70");
    assert_eq!(rows.len(), 5);
}

#[test]
fn test_where_like() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'hello world')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'hello')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'world hello')").unwrap();
    db.execute("INSERT INTO t VALUES (4, 'HELLO')").unwrap();

    assert_eq!(query_rows(&db, "SELECT * FROM t WHERE s LIKE 'hello%'").len(), 2, "LIKE 'hello%'");
    assert_eq!(query_rows(&db, "SELECT * FROM t WHERE s LIKE '%world%'").len(), 2, "LIKE '%%world%%'");
    // _ wildcard — 'h_l%' matches "hello" (h, _, l, %)
    let underscore_rows = query_rows(&db, "SELECT * FROM t WHERE s LIKE 'h_l%'");
    assert!(underscore_rows.len() >= 1, "LIKE 'h_l%%' should match 'hello'");
    assert_eq!(query_rows(&db, "SELECT * FROM t WHERE s NOT LIKE 'hello%'").len(), 2, "NOT LIKE 'hello%'");
}

#[test]
fn test_where_null_semantics() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 5)").unwrap();

    assert_eq!(query_rows(&db, "SELECT * FROM t WHERE v = NULL").len(), 0);
    assert_eq!(query_rows(&db, "SELECT * FROM t WHERE v != NULL").len(), 0);
    assert_eq!(query_rows(&db, "SELECT * FROM t WHERE v > NULL").len(), 0);
    assert_eq!(query_rows(&db, "SELECT * FROM t WHERE v < NULL").len(), 0);
    assert_eq!(query_rows(&db, "SELECT * FROM t WHERE NOT v").len(), 0);
}

#[test]
fn test_where_arithmetic() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, price INT, qty INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10, 5)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 3, 2)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 1, 100)").unwrap();

    let rows = query_rows(&db, "SELECT * FROM t WHERE price * qty > 20 ORDER BY id");
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_where_parenthesized() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)").unwrap();
    for i in 0..4 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {}, {})", i + 1, i % 2, i / 2)).unwrap();
    }
    let rows = query_rows(&db, "SELECT * FROM t WHERE (a = 0 OR a = 1) AND b = 0 ORDER BY id");
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_where_int_float_cross_type() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 5.0)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 3.5)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 10.0)").unwrap();

    assert_eq!(query_rows(&db, "SELECT * FROM t WHERE v = 5").len(), 1);
    assert_eq!(query_rows(&db, "SELECT * FROM t WHERE v > 4").len(), 2);
}

// ============================================================
// PART 6: ORDER BY, LIMIT, OFFSET
// ============================================================

#[test]
fn test_order_by_asc_desc() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 30)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 20)").unwrap();

    let rows = query_rows(&db, "SELECT v FROM t ORDER BY v ASC");
    assert_eq!(rows[0][0], Value::Integer(10));
    assert_eq!(rows[2][0], Value::Integer(30));

    let rows = query_rows(&db, "SELECT v FROM t ORDER BY v DESC");
    assert_eq!(rows[0][0], Value::Integer(30));
    assert_eq!(rows[2][0], Value::Integer(10));
}

#[test]
fn test_limit_offset() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    for i in 1..=10 { db.execute(&format!("INSERT INTO t VALUES ({})", i)).unwrap(); }

    assert_eq!(query_rows(&db, "SELECT * FROM t ORDER BY id LIMIT 3").len(), 3);
    let rows = query_rows(&db, "SELECT * FROM t ORDER BY id LIMIT 3 OFFSET 5");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], Value::Integer(6));
}

#[test]
fn test_order_by_with_nulls() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 30)").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 10)").unwrap();

    let rows = query_rows(&db, "SELECT * FROM t ORDER BY v");
    assert_eq!(rows.len(), 3, "ORDER BY with NULLs should not lose rows");
}

// ============================================================
// PART 7: UPDATE correctness
// ============================================================

#[test]
fn test_update_basic() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("UPDATE t SET v = 99 WHERE id = 1").unwrap();
    assert_eq!(qv(&db, "SELECT v FROM t WHERE id = 1"), Value::Integer(99));
}

#[test]
fn test_update_swap() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10, 20)").unwrap();
    db.execute("UPDATE t SET a = b, b = a WHERE id = 1").unwrap();
    assert_eq!(qv(&db, "SELECT a FROM t WHERE id = 1"), Value::Integer(20));
    assert_eq!(qv(&db, "SELECT b FROM t WHERE id = 1"), Value::Integer(10));
}

#[test]
fn test_update_expression() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("UPDATE t SET v = v * 2 + 1 WHERE id = 1").unwrap();
    assert_eq!(qv(&db, "SELECT v FROM t WHERE id = 1"), Value::Integer(21));
}

#[test]
fn test_update_multi_column() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 1, 2, 3)").unwrap();
    db.execute("UPDATE t SET a = c, b = a, c = b WHERE id = 1").unwrap();
    let row = qr(&db, "SELECT a, b, c FROM t WHERE id = 1").unwrap();
    assert_eq!(row, vec![Value::Integer(3), Value::Integer(1), Value::Integer(2)]);
}

#[test]
fn test_update_set_null() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 100)").unwrap();
    db.execute("UPDATE t SET v = NULL WHERE id = 1").unwrap();
    assert_eq!(qv(&db, "SELECT v FROM t WHERE id = 1"), Value::Null);
}

#[test]
fn test_update_no_match() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    let result = exec(&db, "UPDATE t SET v = 99 WHERE id = 999");
    if let QueryResult::Modification { affected_rows } = result {
        assert_eq!(affected_rows, 0);
    }
    assert_eq!(qv(&db, "SELECT v FROM t WHERE id = 1"), Value::Integer(10));
}

#[test]
fn test_update_where_is_null() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 10)").unwrap();
    db.execute("UPDATE t SET v = 0 WHERE v IS NULL").unwrap();
    assert_eq!(qv(&db, "SELECT v FROM t WHERE id = 1"), Value::Integer(0));
    assert_eq!(qv(&db, "SELECT v FROM t WHERE id = 2"), Value::Integer(10));
}

// ============================================================
// PART 8: DELETE correctness
// ============================================================

#[test]
fn test_delete_basic() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=5 { db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap(); }
    db.execute("DELETE FROM t WHERE id = 3").unwrap();
    assert_eq!(qv(&db, "SELECT COUNT(*) FROM t"), Value::Integer(4));
}

#[test]
fn test_delete_with_where() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=10 { db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 10)).unwrap(); }
    db.execute("DELETE FROM t WHERE v >= 50 AND v <= 80").unwrap();
    let rows = query_rows(&db, "SELECT * FROM t ORDER BY id");
    assert_eq!(rows.len(), 6);
}

#[test]
fn test_delete_all() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=5 { db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap(); }
    db.execute("DELETE FROM t WHERE id >= 1").unwrap();
    assert_eq!(qv(&db, "SELECT COUNT(*) FROM t"), Value::Integer(0));
}

#[test]
fn test_delete_reinsert() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'old')").unwrap();
    db.execute("DELETE FROM t WHERE id = 1").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'new')").unwrap();
    assert_eq!(qv(&db, "SELECT v FROM t WHERE id = 1"), Value::text("new".into()));
}

#[test]
fn test_delete_is_null() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 10)").unwrap();
    db.execute("DELETE FROM t WHERE v IS NULL").unwrap();
    assert_eq!(query_rows(&db, "SELECT * FROM t").len(), 1);
}

// ============================================================
// PART 9: Prepared statements
// ============================================================

#[test]
fn test_prepared_insert_select() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT)").unwrap();

    for i in 1..=5 {
        db.execute_prepared(
            "INSERT INTO t VALUES (?, ?, ?)",
            vec![Value::Integer(i), Value::text(format!("user{}", i)), Value::Integer(20 + i)],
        ).unwrap();
    }

    for i in 1..=5 {
        let r = db.execute_prepared(
            "SELECT name FROM t WHERE id = ?",
            vec![Value::Integer(i)],
        ).unwrap().materialize().unwrap();
        if let QueryResult::Select { rows, .. } = r {
            assert_eq!(rows[0][0], Value::text(format!("user{}", i)));
        }
    }
}

// ============================================================
// PART 10: Durability — reopen after close
// ============================================================

#[test]
fn test_reopen_full_crud() {
    let path_ctx = TempDir::new().unwrap();
    let path = path_ctx.path().join("test.mote");

    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        db.execute("INSERT INTO t VALUES (3, 'c')").unwrap();
        db.execute("UPDATE t SET v = 'updated' WHERE id = 2").unwrap();
        db.execute("DELETE FROM t WHERE id = 3").unwrap();
        drop(db);
    }

    let db = Database::open(&path).unwrap();
    let rows = query_rows(&db, "SELECT * FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![Value::Integer(1), Value::text("a".into())]);
    assert_eq!(rows[1], vec![Value::Integer(2), Value::text("updated".into())]);
}

#[test]
fn test_reopen_null_data() {
    let path_ctx = TempDir::new().unwrap();
    let path = path_ctx.path().join("test.mote");

    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 42)").unwrap();
        drop(db);
    }

    let db = Database::open(&path).unwrap();
    let rows = query_rows(&db, "SELECT * FROM t ORDER BY id");
    assert_eq!(rows[0][1], Value::Null);
    assert_eq!(rows[1][1], Value::Integer(42));
}

// ============================================================
// PART 11: Stress test
// ============================================================

#[test]
fn test_stress_500_rows_crud() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();

    // Insert 500 rows
    for i in 1..=500 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * i)).unwrap();
    }
    assert_eq!(qv(&db, "SELECT COUNT(*) FROM t"), Value::Integer(500));

    // Update every 10th row
    for i in (10..=500).step_by(10) {
        db.execute(&format!("UPDATE t SET v = v + 1000 WHERE id = {}", i)).unwrap();
    }

    // Delete every 7th row
    for i in (7..=500).step_by(7) {
        db.execute(&format!("DELETE FROM t WHERE id = {}", i)).unwrap();
    }

    let count = match qv(&db, "SELECT COUNT(*) FROM t") {
        Value::Integer(c) => c,
        other => panic!("Expected integer, got {:?}", other),
    };
    // 500 - floor(500/7) = 500 - 71 = 429
    assert_eq!(count, 429, "Expected 429 rows after deletes");

    // Verify updated values
    let v10 = qv(&db, "SELECT v FROM t WHERE id = 10");
    assert_eq!(v10, Value::Integer(100 + 1000));
}

// ============================================================
// PART 12: Performance benchmark
// ============================================================

#[test]
fn test_perf_insert_throughput() {
    eprintln!("\n=== Performance (Default Config — GroupCommit fsync) ===");
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();

    let n = 1000;
    let start = Instant::now();
    for i in 1..=n {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap();
    }
    let elapsed = start.elapsed();
    let us_per = elapsed.as_micros() as f64 / n as f64;
    eprintln!("INSERT (GroupCommit): {} rows in {:.1}ms ({:.0}µs/op, {:.0} ops/sec)",
        n, elapsed.as_micros() as f64 / 1000.0, us_per, n as f64 / elapsed.as_secs_f64());

    // NoSync mode comparison
    eprintln!("\n=== Performance (NoSync Config — in-memory WAL) ===");
    let dir2 = TempDir::new().unwrap();
    let db2 = setup_db_fast(dir2.path());
    db2.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();

    let start2 = Instant::now();
    for i in 1..=n {
        db2.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap();
    }
    let elapsed2 = start2.elapsed();
    let us_per2 = elapsed2.as_micros() as f64 / n as f64;
    eprintln!("INSERT (NoSync):    {} rows in {:.1}ms ({:.0}µs/op, {:.0} ops/sec)",
        n, elapsed2.as_micros() as f64 / 1000.0, us_per2, n as f64 / elapsed2.as_secs_f64());
    eprintln!("Speedup from NoSync: {:.1}x", us_per / us_per2);
    assert!(us_per < 10000.0, "INSERT too slow: {:.0}µs/op", us_per);
}

#[test]
fn test_perf_select_throughput() {
    let dir = TempDir::new().unwrap();
    let db = setup_db_fast(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=500 { db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap(); }

    let n = 500;
    let start = Instant::now();
    for i in 1..=n {
        let _ = query_rows(&db, &format!("SELECT * FROM t WHERE id = {}", i));
    }
    let elapsed = start.elapsed();
    let us_per = elapsed.as_micros() as f64 / n as f64;
    eprintln!("SELECT by PK:       {} queries in {:.1}ms ({:.0}µs/op, {:.0} ops/sec)",
        n, elapsed.as_micros() as f64 / 1000.0, us_per, n as f64 / elapsed.as_secs_f64());
    assert!(us_per < 2000.0, "SELECT too slow: {:.0}µs/op", us_per);
}

#[test]
fn test_perf_prepared_select() {
    let dir = TempDir::new().unwrap();
    let db = setup_db_fast(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=500 { db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap(); }

    let n = 500;
    let sql = "SELECT * FROM t WHERE id = ?";
    let start = Instant::now();
    for i in 1..=n {
        let _ = db.execute_prepared(sql, vec![Value::Integer(i)])
            .unwrap().materialize().unwrap();
    }
    let elapsed = start.elapsed();
    let us_per = elapsed.as_micros() as f64 / n as f64;
    eprintln!("Prepared SELECT: {} queries in {:.1}ms ({:.0}µs/op, {:.0} ops/sec)",
        n, elapsed.as_micros() as f64 / 1000.0, us_per, n as f64 / elapsed.as_secs_f64());
}

#[test]
fn test_perf_update_throughput() {
    let dir = TempDir::new().unwrap();
    let db = setup_db_fast(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=500 { db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap(); }

    let n = 500;
    let start = Instant::now();
    for i in 1..=n {
        db.execute(&format!("UPDATE t SET v = v + 1 WHERE id = {}", i)).unwrap();
    }
    let elapsed = start.elapsed();
    let us_per = elapsed.as_micros() as f64 / n as f64;
    eprintln!("UPDATE by PK:       {} updates in {:.1}ms ({:.0}µs/op, {:.0} ops/sec)",
        n, elapsed.as_micros() as f64 / 1000.0, us_per, n as f64 / elapsed.as_secs_f64());
}

#[test]
fn test_perf_full_scan() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=1000 { db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap(); }

    let start = Instant::now();
    let count = qv(&db, "SELECT COUNT(*) FROM t");
    let elapsed = start.elapsed();
    eprintln!("Full scan COUNT(*) 1000 rows: {:.1}ms", elapsed.as_micros() as f64 / 1000.0);
    assert_eq!(count, Value::Integer(1000));

    let start = Instant::now();
    let rows = query_rows(&db, "SELECT * FROM t WHERE v > 900 ORDER BY id");
    let elapsed = start.elapsed();
    eprintln!("Filtered scan (v>900, ~100 rows): {:.1}ms, returned {} rows",
        elapsed.as_micros() as f64 / 1000.0, rows.len());
    assert!(rows.len() > 90);
}
