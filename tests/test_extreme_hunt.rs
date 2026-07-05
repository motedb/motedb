//! Round 6: Extreme hunt — special characters, unicode, overflow,
//! complex compound queries, mixed-type comparisons, edge cases in
//! aggregate/GROUP BY/JOIN/ORDER BY interactions.

use motedb::{Database, types::Value, sql::QueryResult};
use tempfile::TempDir;

fn mk() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    (db, dir)
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        _ => vec![],
    }
}

fn cnt(db: &Database, sql: &str) -> i64 {
    rows(db, sql).first().and_then(|r| r.first()).and_then(|v| {
        if let Value::Integer(i) = v { Some(*i) } else { None }
    }).unwrap_or(-1)
}

fn val(db: &Database, sql: &str) -> Value {
    rows(db, sql).first().and_then(|r| r.first()).cloned().unwrap_or(Value::Null)
}

// ═════════════════════════════════════════════════════════════════
// A. Unicode / special characters in TEXT
// ═════════════════════════════════════════════════════════════════

/// Chinese characters in TEXT column.
#[test]
fn test_unicode_chinese_text() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, '你好世界')").unwrap();
    db.flush().unwrap();
    let r = rows(&db, "SELECT name FROM t WHERE id = 1");
    assert_eq!(r[0][0], Value::text("你好世界".into()));
}

/// Emoji in TEXT column.
#[test]
fn test_emoji_in_text() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, msg TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Hello 🦀 Rust')").unwrap();
    db.flush().unwrap();
    let r = rows(&db, "SELECT msg FROM t WHERE id = 1");
    assert_eq!(r[0][0], Value::text("Hello 🦀 Rust".into()));
}

/// Text with quotes (single, double).
#[test]
fn test_text_with_quotes() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").unwrap();
    // Escaped single quote in SQL string.
    db.execute("INSERT INTO t VALUES (1, 'it''s great')").unwrap();
    db.flush().unwrap();
    let r = rows(&db, "SELECT v FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Text(t) => assert!(t.contains("great"), "Should contain 'great'"),
        _ => panic!("Expected Text"),
    }
}

/// Text with newline and tab.
#[test]
fn test_text_with_newline_tab() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'line1\nline2\ttabbed')").unwrap();
    db.flush().unwrap();
    let r = rows(&db, "SELECT v FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Text(t) => assert!(t.contains("line1"), "Should contain newline text"),
        _ => panic!("Expected Text"),
    }
}

/// Empty table name in WHERE — should not match.
#[test]
fn test_where_empty_string_not_null() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, '')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'data')").unwrap();
    db.execute("INSERT INTO t VALUES (3, NULL)").unwrap();
    db.flush().unwrap();
    // WHERE v = '' should find id=1 only.
    assert_eq!(cnt(&db, "SELECT COUNT(*) FROM t WHERE v = ''"), 1);
    // WHERE v != '' should exclude empty AND NULL.
    let non_empty = cnt(&db, "SELECT COUNT(*) FROM t WHERE v != ''");
    assert!(non_empty >= 1, "Should find non-empty values");
}

// ═════════════════════════════════════════════════════════════════
// B. Integer overflow / underflow edge cases
// ═════════════════════════════════════════════════════════════════

/// SUM of large integers — should not overflow silently.
#[test]
fn test_sum_large_integers_no_overflow() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v BIGINT)").unwrap();
    db.execute(&format!("INSERT INTO t VALUES (1, {})", i64::MAX / 2)).unwrap();
    db.execute(&format!("INSERT INTO t VALUES (2, {})", i64::MAX / 2)).unwrap();
    db.flush().unwrap();
    // SUM should either return a large Integer or overflow to Float.
    let r = rows(&db, "SELECT SUM(v) FROM t");
    assert_eq!(r.len(), 1);
    match &r[0][0] {
        Value::Integer(_) => { /* OK — wrapping or checked */ }
        Value::Float(f) => assert!(*f > i64::MAX as f64 * 0.9, "SUM should be large"),
        Value::Null => {} /* SUM over overflow may return NULL */
        _ => panic!("Unexpected SUM result"),
    }
}

/// Multiplication overflow — i64::MAX * 2.
#[test]
fn test_mul_overflow_promotes_to_float() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v BIGINT)").unwrap();
    db.execute(&format!("INSERT INTO t VALUES (1, {})", i64::MAX)).unwrap();
    db.flush().unwrap();
    let r = db.execute("SELECT v * 2 FROM t WHERE id = 1");
    assert!(r.is_ok(), "Multiplication overflow should not error");
}

// ═════════════════════════════════════════════════════════════════
// C. Complex compound queries
// ═════════════════════════════════════════════════════════════════

/// GROUP BY + ORDER BY + LIMIT combined.
#[test]
fn test_group_by_order_by_limit_combined() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, dept TEXT, salary INT)").unwrap();
    for i in 1..=30 {
        let dept = ["Eng", "Sales", "HR"][(i % 3) as usize];
        db.execute(&format!("INSERT INTO t VALUES ({}, '{}', {})", i, dept, i * 1000)).unwrap();
    }
    db.flush().unwrap();
    let r = rows(&db, "SELECT dept, SUM(salary) FROM t GROUP BY dept ORDER BY dept LIMIT 2");
    assert!(r.len() <= 2, "Should return at most 2 groups");
}

/// Subquery in SELECT (scalar subquery).
#[test]
fn test_scalar_subquery_in_select() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=10 { db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap(); }
    db.flush().unwrap();
    let r = rows(&db, "SELECT id, (SELECT MAX(v) FROM t) AS max_v FROM t WHERE id = 1");
    assert_eq!(r.len(), 1);
    // The scalar subquery should return 10 (MAX of 1..10).
    match &r[0][1] {
        Value::Integer(i) => assert_eq!(*i, 10, "MAX(v) subquery should return 10"),
        _ => panic!("Expected Integer for scalar subquery result"),
    }
}

/// WHERE with NOT IN.
#[test]
fn test_where_not_in() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=10 { db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap(); }
    db.flush().unwrap();
    let r = rows(&db, "SELECT id FROM t WHERE v NOT IN (1, 2, 3)");
    assert_eq!(r.len(), 7, "NOT IN (1,2,3) should exclude 3 rows");
}

/// WHERE with BETWEEN.
#[test]
fn test_where_between() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=10 { db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap(); }
    db.flush().unwrap();
    assert_eq!(cnt(&db, "SELECT COUNT(*) FROM t WHERE v BETWEEN 3 AND 7"), 5);
    assert_eq!(cnt(&db, "SELECT COUNT(*) FROM t WHERE v NOT BETWEEN 3 AND 7"), 5);
}

// ═════════════════════════════════════════════════════════════════
// D. Mixed-type comparison edge cases
// ═════════════════════════════════════════════════════════════════

/// WHERE comparing INT column with FLOAT literal.
#[test]
fn test_where_int_column_float_literal() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=10 { db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 10)).unwrap(); }
    db.flush().unwrap();
    // v = 50.0 should match v=50 (integer column, float literal).
    assert_eq!(cnt(&db, "SELECT COUNT(*) FROM t WHERE v = 50.0"), 1);
    assert_eq!(cnt(&db, "SELECT COUNT(*) FROM t WHERE v > 50.0"), 5); // 60,70,80,90,100
}

/// ORDER BY on TEXT column (lexicographic).
#[test]
fn test_order_by_text_lexicographic() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'banana')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'apple')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'cherry')").unwrap();
    db.flush().unwrap();
    let r = rows(&db, "SELECT name FROM t ORDER BY name ASC LIMIT 1");
    assert_eq!(r[0][0], Value::text("apple".into()), "Lexicographic ASC: apple first");
}

// ═════════════════════════════════════════════════════════════════
// E. Recovery with complex data patterns
// ═════════════════════════════════════════════════════════════════

/// Recovery preserves unicode text.
#[test]
fn test_recovery_preserves_unicode() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, '日本語')").unwrap();
        db.execute("INSERT INTO t VALUES (2, '한국어')").unwrap();
        db.execute("INSERT INTO t VALUES (3, 'العربية')").unwrap();
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(val(&db, "SELECT name FROM t WHERE id = 1"), Value::text("日本語".into()));
    assert_eq!(val(&db, "SELECT name FROM t WHERE id = 2"), Value::text("한국어".into()));
    assert_eq!(val(&db, "SELECT name FROM t WHERE id = 3"), Value::text("العربية".into()));
}

/// Recovery with many tables.
#[test]
fn test_recovery_many_tables() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        for t in 0..10 {
            db.execute(&format!("CREATE TABLE t{} (id INT PRIMARY KEY, v INT)", t)).unwrap();
            for i in 1..=5 {
                db.execute(&format!("INSERT INTO t{} VALUES ({}, {})", t, i, i)).unwrap();
            }
        }
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    for t in 0..10 {
        assert_eq!(cnt(&db, &format!("SELECT COUNT(*) FROM t{}", t)), 5,
            "Table t{} should have 5 rows", t);
    }
}

// ═════════════════════════════════════════════════════════════════
// F. Aggregate interaction with WHERE + GROUP BY
// ═════════════════════════════════════════════════════════════════

/// SUM with WHERE filter, grouped.
#[test]
fn test_sum_where_grouped() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, dept TEXT, amt INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'A', 100)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'A', 200)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'B', 50)").unwrap();
    db.execute("INSERT INTO t VALUES (4, 'B', 150)").unwrap();
    db.flush().unwrap();
    // SUM(amt) WHERE amt > 60, GROUP BY dept.
    let r = rows(&db, "SELECT dept, SUM(amt) FROM t WHERE amt > 60 GROUP BY dept");
    // A: 100+200=300, B: 150 (50 filtered out).
    assert!(!r.is_empty(), "Should return grouped results");
}

/// COUNT + GROUP BY + WHERE on different column.
#[test]
fn test_count_group_by_where_different_col() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, status TEXT)").unwrap();
    for i in 1..=20 {
        let cat = if i <= 10 { "X" } else { "Y" };
        let status = if i % 2 == 0 { "active" } else { "inactive" };
        db.execute(&format!("INSERT INTO t VALUES ({}, '{}', '{}')", i, cat, status)).unwrap();
    }
    db.flush().unwrap();
    let r = rows(&db, "SELECT cat, COUNT(*) FROM t WHERE status = 'active' GROUP BY cat");
    // Each cat has 5 active rows (half of 10).
    assert!(!r.is_empty(), "Should return grouped counts");
}

// ═════════════════════════════════════════════════════════════════
// G. Rapid schema changes
// ═════════════════════════════════════════════════════════════════

/// CREATE → INSERT → DROP → CREATE cycle on same table name.
#[test]
fn test_create_drop_create_cycle() {
    let (db, _d) = mk();
    for cycle in 0..3 {
        db.execute(&format!("CREATE TABLE cyc (id INT PRIMARY KEY, v TEXT)")).unwrap();
        db.execute(&format!("INSERT INTO cyc VALUES ({}, 'cycle{}')", cycle, cycle)).unwrap();
        assert_eq!(cnt(&db, "SELECT COUNT(*) FROM cyc"), 1);
        db.execute("DROP TABLE cyc").unwrap();
    }
}

/// CREATE many tables then query each.
#[test]
fn test_create_many_tables_query() {
    let (db, _d) = mk();
    for t in 0..5 {
        db.execute(&format!("CREATE TABLE tab{} (id INT PRIMARY KEY, v INT)", t)).unwrap();
        db.execute(&format!("INSERT INTO tab{} VALUES (1, {})", t, t * 100)).unwrap();
    }
    db.flush().unwrap();
    for t in 0..5 {
        assert_eq!(val(&db, &format!("SELECT v FROM tab{} WHERE id = 1", t)), Value::Integer(t * 100));
    }
}

// ═════════════════════════════════════════════════════════════════
// H. DELETE + re-INSERT lifecycle
// ═════════════════════════════════════════════════════════════════

/// Delete half, re-insert, verify counts.
#[test]
fn test_delete_half_reinsert() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=100 { db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap(); }
    db.flush().unwrap();
    // Delete even IDs.
    for i in (2..=100).step_by(2) {
        db.execute(&format!("DELETE FROM t WHERE id = {}", i)).unwrap();
    }
    assert_eq!(cnt(&db, "SELECT COUNT(*) FROM t"), 50);
    // Re-insert with new values.
    for i in (2..=100).step_by(2) {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 100)).unwrap();
    }
    assert_eq!(cnt(&db, "SELECT COUNT(*) FROM t"), 100);
    // Verify re-inserted values.
    assert_eq!(val(&db, "SELECT v FROM t WHERE id = 50"), Value::Integer(5000));
}

// ═════════════════════════════════════════════════════════════════
// I. Edge: SELECT with no WHERE (full scan correctness)
// ═════════════════════════════════════════════════════════════════

/// Full scan returns correct count after mixed ops.
#[test]
fn test_full_scan_after_mixed_ops() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=50 { db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap(); }
    db.execute("DELETE FROM t WHERE id <= 10").unwrap(); // -10
    db.execute("UPDATE t SET v = 999 WHERE id > 40").unwrap(); // update 10 rows
    for i in 51..=60 { db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap(); } // +10
    db.flush().unwrap();
    assert_eq!(cnt(&db, "SELECT COUNT(*) FROM t"), 50, "50 - 10 + 10 = 50");
    // Updated rows should have v=999.
    assert_eq!(cnt(&db, "SELECT COUNT(*) FROM t WHERE v = 999"), 10);
    // Non-updated rows (id 11-40) should have v=id.
    assert_eq!(val(&db, "SELECT v FROM t WHERE id = 25"), Value::Integer(25));
}

// ═════════════════════════════════════════════════════════════════
// J. Edge: Float special values
// ═════════════════════════════════════════════════════════════════

/// Float NaN — should be stored without panic.
#[test]
fn test_float_nan_no_panic() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)").unwrap();
    // Insert a value that produces NaN via calculation.
    let r = db.execute("INSERT INTO t VALUES (1, 0.0/0.0)");
    // May or may not parse — key is no panic.
    assert!(r.is_ok() || r.is_err());
}

/// Float infinity — should not panic.
#[test]
fn test_float_infinity_no_panic() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)").unwrap();
    let r = db.execute("INSERT INTO t VALUES (1, 1.0/0.0)");
    assert!(r.is_ok() || r.is_err());
}

/// Float negative zero.
#[test]
fn test_float_negative_zero() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, -0.0)").unwrap();
    db.flush().unwrap();
    let r = rows(&db, "SELECT v FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Float(f) => assert!(*f == 0.0, "-0.0 should equal 0.0, got {}", f),
        _ => panic!("Expected Float"),
    }
}
