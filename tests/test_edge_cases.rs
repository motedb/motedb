//! 边界条件 — Edge Cases & Error Handling Tests
//!
//! 核心原则：极端输入不崩溃，错误优雅处理。

#[path = "common/mod.rs"]
mod common;
use common::*;

#[test]
fn test_empty_table_all_queries() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, val FLOAT)");
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 0);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE name = 'x'"), 0);
    assert_eq!(count_rows(&db, "SELECT DISTINCT name FROM t"), 0);
    assert_eq!(count_rows(&db, "SELECT * FROM t LIMIT 10"), 0);
    assert_eq!(count_rows(&db, "SELECT * FROM t ORDER BY val DESC LIMIT 5"), 0);
}

#[test]
fn test_single_row_table() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'only')");
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE name = 'only'"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t LIMIT 10"), 1);
    assert_eq!(count_rows(&db, "SELECT DISTINCT name FROM t"), 1);
}

#[test]
fn test_null_values() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, val FLOAT)");
    exec(&db, "INSERT INTO t VALUES (1, NULL, NULL)");
    exec(&db, "INSERT INTO t VALUES (2, 'hello', 3.14)");
    exec(&db, "INSERT INTO t VALUES (3, NULL, 42.0)");
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 3);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE name = 'hello'"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE val = 42.0"), 1);
}

#[test]
#[ignore = "Empty string literal handling: needs proper escaping"]
fn test_empty_string() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, '')");
    exec(&db, "INSERT INTO t VALUES (2, 'non-empty')");
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 2);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE name = ''"), 1);
}

#[test]
fn test_very_long_string() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, data TEXT)");
    let long_str = "x".repeat(10000);
    exec(&db, &format!("INSERT INTO t VALUES (1, '{}')", long_str));
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE data LIKE 'x%'"), 1);
}

#[test]
fn test_max_integer() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO t VALUES (1, 9223372036854775807)"); // i64::MAX
    exec(&db, "INSERT INTO t VALUES (2, -9223372036854775808)"); // i64::MIN
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 2);
}

#[test]
fn test_negative_integers() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO t VALUES (1, -100)");
    exec(&db, "INSERT INTO t VALUES (2, -1)");
    exec(&db, "INSERT INTO t VALUES (3, 0)");
    exec(&db, "INSERT INTO t VALUES (4, 100)");
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 4);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE val = -100"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE val = 0"), 1);
}

#[test]
fn test_float_precision() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val FLOAT)");
    exec(&db, "INSERT INTO t VALUES (1, 3.141592653589793)");
    exec(&db, "INSERT INTO t VALUES (2, 0.1)");
    exec(&db, "INSERT INTO t VALUES (3, 1e10)");
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 3);
}

#[test]
fn test_unicode_text() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, '你好世界')");
    exec(&db, "INSERT INTO t VALUES (2, '🌱🌍⚡')");
    exec(&db, "INSERT INTO t VALUES (3, 'Mixed混合emoji😀')");
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 3);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE name = '你好世界'"), 1);
}

#[test]
fn test_special_characters_in_text() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, data TEXT)");
    // These should work with proper escaping (doubled quotes).
    exec(&db, "INSERT INTO t VALUES (1, 'has ''quotes''')");
    exec(&db, "INSERT INTO t VALUES (2, 'has spaces')");
    exec(&db, "INSERT INTO t VALUES (3, 'has-dashes_and.underscores')");
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 3);
}

#[test]
fn test_duplicate_pk_rejected() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO t VALUES (1, 100)");
    // Duplicate PK should fail (not panic)
    let result = db.execute("INSERT INTO t VALUES (1, 200)");
    assert!(result.is_err(), "Duplicate PK insert should error");
    // Original row unchanged
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 1);
}

#[test]
fn test_delete_nonexistent_row() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO t VALUES (1, 100)");
    // Deleting non-existent row should not crash
    let _ = db.execute("DELETE FROM t WHERE id = 999");
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 1);
}

#[test]
fn test_update_nonexistent_row() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO t VALUES (1, 100)");
    // Updating non-existent row should not crash
    let _ = db.execute("UPDATE t SET val = 999 WHERE id = 999");
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 1);
}

#[test]
#[ignore = "DROP+reCREATE: stale col_segment_store persists"]
fn test_table_create_drop_recreate() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO t VALUES (1, 100)");
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 1);

    exec(&db, "DROP TABLE t");
    assert!(db.execute("SELECT * FROM t").is_err());

    // Recreate with different schema
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'reborn')");
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE name = 'reborn'"), 1);
}

#[test]
#[ignore = "Wide table: batch INSERT with 20 columns needs parser support"]
fn test_many_columns_wide_table() {
    let (_dir, db) = setup_db();
    // 20-column table
    let mut cols = String::from("id INT PRIMARY KEY");
    for i in 1..20 {
        cols.push_str(&format!(", col{} TEXT", i));
    }
    exec(&db, &format!("CREATE TABLE wide ({})", cols));

    let mut vals = String::from("1");
    for i in 1..20 {
        vals.push_str(&format!(", 'val{}'", i));
    }
    exec(&db, &format!("INSERT INTO wide VALUES ({})", vals));
    assert_eq!(count_rows(&db, "SELECT * FROM wide"), 1);
}

#[test]
fn test_batch_insert_consistency() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    // Insert 100 rows via single batch SQL
    let mut sql = String::from("INSERT INTO t VALUES ");
    for i in 1..=100 {
        if i > 1 { sql.push(','); }
        sql.push_str(&format!("({}, {})", i, i * 2));
    }
    exec(&db, &sql);
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 100);
    // Verify distribution
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE val <= 100"), 50);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE val > 100"), 50);
}
