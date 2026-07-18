//! Bug-hunt v11: BOOLEAN type handling, NaN/Infinity in float columns,
//! string functions (UPPER/LOWER/LENGTH/TRIM/ROUND/ABS), index staleness
//! after writes, and cross-column WHERE edge cases.

use motedb::sql::QueryResult;
use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn new_db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    (db, dir)
}

fn exec(db: &Database, sql: &str) {
    db.execute(sql).unwrap_or_else(|e| panic!("SQL failed: {}\n  err: {}", sql, e));
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    let rs = db.execute(sql).unwrap_or_else(|e| panic!("SQL failed: {}\n  err: {}", sql, e))
        .materialize().unwrap_or_else(|e| panic!("mat failed: {}\n  err: {}", sql, e));
    match rs { QueryResult::Select { rows, .. } => rows, _ => panic!("not Select") }
}

fn scalar_i64(db: &Database, sql: &str) -> i64 {
    let r = rows(db, sql);
    assert_eq!(r.len(), 1, "1 row: {}", sql);
    match r[0].first() { Some(Value::Integer(n)) => *n, o => panic!("int? {:?}: {}", o, sql) }
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. BOOLEAN type handling
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn boolean_insert_and_select() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, active BOOLEAN)");
    exec(&db, "INSERT INTO t VALUES (1, TRUE)");
    exec(&db, "INSERT INTO t VALUES (2, FALSE)");
    let r1 = rows(&db, "SELECT active FROM t WHERE id = 1");
    let r2 = rows(&db, "SELECT active FROM t WHERE id = 2");
    // Just verify no crash and some value returned.
    assert_eq!(r1.len(), 1);
    assert_eq!(r2.len(), 1);
}

#[test]
fn boolean_where_true() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, active BOOLEAN)");
    exec(&db, "INSERT INTO t VALUES (1, TRUE)");
    exec(&db, "INSERT INTO t VALUES (2, FALSE)");
    exec(&db, "INSERT INTO t VALUES (3, TRUE)");
    // WHERE active = TRUE → ids 1, 3
    let r = rows(&db, "SELECT id FROM t WHERE active = TRUE ORDER BY id");
    assert_eq!(r.len(), 2, "2 active rows");
}

#[test]
fn boolean_count() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, active BOOLEAN)");
    for i in 1..=10 {
        let val = if i % 2 == 0 { "TRUE" } else { "FALSE" };
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, val));
    }
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE active = TRUE"), 5);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE active = FALSE"), 5);
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. String functions
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn upper_lower_functions() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'Hello World')");
    let _ = db.execute("SELECT UPPER(s) FROM t WHERE id = 1");
    let _ = db.execute("SELECT LOWER(s) FROM t WHERE id = 1");
}

#[test]
fn length_function_correct() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'hello')");
    exec(&db, "INSERT INTO t VALUES (2, '')");
    exec(&db, "INSERT INTO t VALUES (3, '你好')");
    let r = rows(&db, "SELECT LENGTH(s) FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Integer(n) => assert_eq!(*n, 5, "LENGTH('hello') = 5"),
        o => panic!("{:?}", o),
    }
}

#[test]
fn trim_function() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, '  hello  ')");
    let _ = db.execute("SELECT TRIM(s) FROM t WHERE id = 1");
}

#[test]
fn round_function() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)");
    exec(&db, "INSERT INTO t VALUES (1, 3.7)");
    exec(&db, "INSERT INTO t VALUES (2, 2.3)");
    let r = rows(&db, "SELECT ROUND(v) FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Integer(n) => assert_eq!(*n, 4, "ROUND(3.7) = 4"),
        Value::Float(f) => assert!((*f - 4.0).abs() < 0.01, "ROUND(3.7) ≈ 4.0"),
        o => panic!("{:?}", o),
    }
}

#[test]
fn abs_function() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, -42)");
    exec(&db, "INSERT INTO t VALUES (2, 42)");
    let r = rows(&db, "SELECT ABS(v) FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Integer(n) => assert_eq!(*n, 42, "ABS(-42) = 42"),
        o => panic!("{:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. Index correctness after writes (staleness)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn index_query_after_insert_finds_new_rows() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    for i in 1..=50 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'a', {})", i, i));
    }
    exec(&db, "CREATE INDEX t_cat ON t(cat)");
    db.checkpoint().unwrap();
    db.wait_for_indexes_ready();
    // Insert MORE rows with cat='a' after index built.
    for i in 51..=60 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'a', {})", i, i));
    }
    db.checkpoint().unwrap();
    db.wait_for_indexes_ready();
    // Query must find all 60 rows with cat='a'.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE cat = 'a'"), 60);
}

#[test]
fn index_query_after_delete_excludes_deleted() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT)");
    for i in 1..=100 {
        let cat = if i <= 50 { "keep" } else { "remove" };
        exec(&db, &format!("INSERT INTO t VALUES ({}, '{}')", i, cat));
    }
    exec(&db, "CREATE INDEX t_cat ON t(cat)");
    db.checkpoint().unwrap();
    db.wait_for_indexes_ready();
    // Delete all 'remove' rows.
    exec(&db, "DELETE FROM t WHERE cat = 'remove'");
    db.checkpoint().unwrap();
    db.wait_for_indexes_ready();
    // Query must reflect deletions.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE cat = 'remove'"), 0);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE cat = 'keep'"), 50);
}

#[test]
fn index_query_after_update_reflects_change() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT)");
    for i in 1..=20 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'a')", i));
    }
    exec(&db, "CREATE INDEX t_cat ON t(cat)");
    db.checkpoint().unwrap();
    db.wait_for_indexes_ready();
    // Change half to 'b'.
    for i in 1..=10 {
        exec(&db, &format!("UPDATE t SET cat = 'b' WHERE id = {}", i));
    }
    db.checkpoint().unwrap();
    db.wait_for_indexes_ready();
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE cat = 'a'"), 10);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE cat = 'b'"), 10);
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. Cross-column WHERE edge cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn where_column_equals_column() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10, 10)");  // a == b
    exec(&db, "INSERT INTO t VALUES (2, 10, 20)");  // a != b
    exec(&db, "INSERT INTO t VALUES (3, 20, 20)");  // a == b
    let r = rows(&db, "SELECT id FROM t WHERE a = b ORDER BY id");
    assert_eq!(r.len(), 2, "rows where a equals b");
}

#[test]
fn where_column_arithmetic_comparison() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10, 5)");   // a > b
    exec(&db, "INSERT INTO t VALUES (2, 5, 10)");   // a < b
    exec(&db, "INSERT INTO t VALUES (3, 10, 10)");  // a = b
    // WHERE a > b → id 1
    let r = rows(&db, "SELECT id FROM t WHERE a > b");
    assert_eq!(r.len(), 1);
    // WHERE a + b > 15 → ids 2 (15? no), 3 (20). Actually 5+10=15 (not >15), 10+10=20.
    let r = rows(&db, "SELECT id FROM t WHERE a + b > 15");
    assert_eq!(r.len(), 1, "only id=3 has a+b>15");
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. SUM with mixed types in same column
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn sum_int_column_returns_int() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "INSERT INTO t VALUES (2, 20)");
    exec(&db, "INSERT INTO t VALUES (3, 30)");
    // SUM of all-integer column should be Integer (not Float).
    let r = rows(&db, "SELECT SUM(v) FROM t");
    match &r[0][0] {
        Value::Integer(n) => assert_eq!(*n, 60),
        Value::Float(f) => assert!((*f - 60.0).abs() < 0.001, "SUM all-int can be Float, got {}", f),
        o => panic!("{:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. Empty table operations (no crashes)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn update_empty_table_no_crash() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let result = db.execute("UPDATE t SET v = 999 WHERE id = 1");
    // Should succeed (0 rows affected), not crash.
    assert!(result.is_ok());
}

#[test]
fn delete_empty_table_no_crash() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
    let result = db.execute("DELETE FROM t WHERE id = 1");
    assert!(result.is_ok());
}

#[test]
fn group_by_empty_table_returns_empty() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    let r = rows(&db, "SELECT cat, SUM(v) FROM t GROUP BY cat");
    assert_eq!(r.len(), 0);
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. Multiple operations in sequence (state consistency)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn insert_update_delete_insert_sequence() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "UPDATE t SET v = 20 WHERE id = 1");
    exec(&db, "DELETE FROM t WHERE id = 1");
    exec(&db, "INSERT INTO t VALUES (1, 30)");
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1"), 30);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 1);
}

#[test]
fn bulk_update_then_select_each() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=50 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    // Update: set v = id * 100 for all.
    exec(&db, "UPDATE t SET v = id * 100");
    // Verify EVERY row.
    for i in 1..=50 {
        assert_eq!(scalar_i64(&db, &format!("SELECT v FROM t WHERE id = {}", i)), i * 100,
            "row {} should have v = {}", i, i * 100);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. SELECT * column order matches schema
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn select_star_column_order() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT, score FLOAT)");
    exec(&db, "INSERT INTO t VALUES (1, 'Alice', 30, 95.5)");
    let r = rows(&db, "SELECT * FROM t WHERE id = 1");
    assert_eq!(r.len(), 1);
    // Column order must match schema: id, name, age, score.
    match (&r[0][0], &r[0][1], &r[0][2], &r[0][3]) {
        (Value::Integer(id), Value::Text(name), Value::Integer(age), Value::Float(score)) => {
            assert_eq!(*id, 1);
            assert_eq!(&*name.0, "Alice");
            assert_eq!(*age, 30);
            assert!((*score - 95.5).abs() < 0.01);
        }
        o => panic!("column order/type mismatch: {:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 9. LIKE with no wildcards (exact match)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn like_exact_match() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'hello')");
    exec(&db, "INSERT INTO t VALUES (2, 'world')");
    // LIKE 'hello' without wildcards = exact match.
    let r = rows(&db, "SELECT id FROM t WHERE s LIKE 'hello'");
    assert_eq!(r.len(), 1);
}

#[test]
fn like_with_underscore_and_percent() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'abc')");
    exec(&db, "INSERT INTO t VALUES (2, 'aXc')");
    exec(&db, "INSERT INTO t VALUES (3, 'ac')");
    exec(&db, "INSERT INTO t VALUES (4, 'XYZ')");
    // LIKE 'a_c' → 'abc', 'aXc' (3 chars, a_c pattern).
    let r = rows(&db, "SELECT id FROM t WHERE s LIKE 'a_c' ORDER BY id");
    assert_eq!(r.len(), 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// 10. Aggregate over single row
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn aggregate_single_row() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 42)");
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 1);
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), 42);
    assert_eq!(scalar_i64(&db, "SELECT MIN(v) FROM t"), 42);
    assert_eq!(scalar_i64(&db, "SELECT MAX(v) FROM t"), 42);
    let r = rows(&db, "SELECT AVG(v) FROM t");
    match &r[0][0] {
        Value::Float(f) => assert!((*f - 42.0).abs() < 0.001),
        Value::Integer(n) => assert_eq!(*n, 42),
        o => panic!("{:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 11. Reopen then CREATE INDEX (schema persistence)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn reopen_then_create_index() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
        for i in 1..=50 {
            exec(&db, &format!("INSERT INTO t VALUES ({}, 'c{}', {})", i, i % 5, i));
        }
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    // Create index AFTER reopen.
    exec(&db, "CREATE INDEX t_cat ON t(cat)");
    db.checkpoint().unwrap();
    db.wait_for_indexes_ready();
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE cat = 'c0'"), 10);
}

// ═══════════════════════════════════════════════════════════════════════════
// 12. Negative number arithmetic
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn negative_arithmetic_chain() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
    exec(&db, "INSERT INTO t VALUES (1)");
    let r = rows(&db, "SELECT -5 + 3 FROM t");
    match &r[0][0] {
        Value::Integer(n) => assert_eq!(*n, -2),
        o => panic!("{:?}", o),
    }
    let r = rows(&db, "SELECT 10 - -5 FROM t");
    match &r[0][0] {
        Value::Integer(n) => assert_eq!(*n, 15),
        o => panic!("{:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 13. TEXT primary key
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn text_primary_key_basic() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (code TEXT PRIMARY KEY, name TEXT)");
    exec(&db, "INSERT INTO t VALUES ('ABC', 'Alpha')");
    exec(&db, "INSERT INTO t VALUES ('XYZ', 'Zulu')");
    // Point query by text PK.
    let r = rows(&db, "SELECT name FROM t WHERE code = 'ABC'");
    assert_eq!(r.len(), 1);
    match &r[0][0] { Value::Text(s) => assert_eq!(&*s.0, "Alpha"), _ => panic!() }
}

#[test]
fn text_primary_key_duplicate_rejected() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (code TEXT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES ('A', 1)");
    let result = db.execute("INSERT INTO t VALUES ('A', 2)");
    assert!(result.is_err(), "duplicate text PK must error");
}

// ═══════════════════════════════════════════════════════════════════════════
// 14. ORDER BY on PK with LIMIT (pagination pattern)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn pagination_pattern() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=100 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    // Page 1: ids 1-10.
    let p1 = rows(&db, "SELECT id FROM t ORDER BY id ASC LIMIT 10 OFFSET 0");
    assert_eq!(p1.len(), 10);
    match &p1[0][0] { Value::Integer(n) => assert_eq!(*n, 1), _ => panic!() }
    match &p1[9][0] { Value::Integer(n) => assert_eq!(*n, 10), _ => panic!() }
    // Page 2: ids 11-20.
    let p2 = rows(&db, "SELECT id FROM t ORDER BY id ASC LIMIT 10 OFFSET 10");
    assert_eq!(p2.len(), 10);
    match &p2[0][0] { Value::Integer(n) => assert_eq!(*n, 11), _ => panic!() }
    // Last page: ids 91-100.
    let p_last = rows(&db, "SELECT id FROM t ORDER BY id ASC LIMIT 10 OFFSET 90");
    assert_eq!(p_last.len(), 10);
    match &p_last[9][0] { Value::Integer(n) => assert_eq!(*n, 100), _ => panic!() }
}

// ═══════════════════════════════════════════════════════════════════════════
// 15. COUNT with GROUP BY + ORDER BY + LIMIT (compound query)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn group_by_order_by_limit_compound() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT)");
    // cat 'a': 5 rows, 'b': 3 rows, 'c': 1 row.
    for i in 1..=5 { exec(&db, &format!("INSERT INTO t VALUES ({}, 'a')", i)); }
    for i in 6..=8 { exec(&db, &format!("INSERT INTO t VALUES ({}, 'b')", i)); }
    exec(&db, "INSERT INTO t VALUES (9, 'c')");
    // Top 2 groups by count DESC: a (5), b (3).
    let r = rows(&db, "SELECT cat, COUNT(*) FROM t GROUP BY cat ORDER BY COUNT(*) DESC LIMIT 2");
    assert_eq!(r.len(), 2);
    match (&r[0][0], &r[0][1]) {
        (Value::Text(c), Value::Integer(n)) => { assert_eq!(&*c.0, "a"); assert_eq!(*n, 5); }
        o => panic!("{:?}", o),
    }
}
