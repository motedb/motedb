//! Deep edge-case tests — stress obscure code paths to find real bugs.
//! Covers: type coercion, pagination, NULL arithmetic, batch ops, recovery,
//! concurrent access, SQL semantics, large values, edge predicates.

use motedb::{Database, types::Value, sql::QueryResult};
use tempfile::TempDir;

fn create() -> (Database, TempDir) {
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

fn count(db: &Database, sql: &str) -> i64 {
    rows(db, sql).first()
        .and_then(|r| r.first())
        .and_then(|v| if let Value::Integer(i) = v { Some(*i) } else { None })
        .unwrap_or(-1)
}

fn val(db: &Database, sql: &str) -> Value {
    rows(db, sql).first()
        .and_then(|r| r.first())
        .cloned()
        .unwrap_or(Value::Null)
}

// ═════════════════════════════════════════════════════════════════
// A. Type coercion & arithmetic edge cases
// ═════════════════════════════════════════════════════════════════

/// Integer + Float should promote to Float.
#[test]
fn test_int_plus_float_promotes() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b FLOAT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10, 2.5)").unwrap();
    db.flush().unwrap();
    let v = val(&db, "SELECT a + b FROM t WHERE id = 1");
    match v {
        Value::Float(f) => assert!((f - 12.5).abs() < 0.001, "10 + 2.5 = {}, expected 12.5", f),
        other => panic!("Expected Float, got {:?}", other),
    }
}

/// Division by zero — should error or return NULL, not panic.
#[test]
fn test_division_by_zero_no_panic() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.flush().unwrap();
    // Must not panic — error is acceptable.
    let _ = db.execute("SELECT v / 0 FROM t WHERE id = 1");
}

/// NULL in arithmetic — NULL + anything = NULL.
#[test]
fn test_null_arithmetic_returns_null() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL, 5)").unwrap();
    db.flush().unwrap();
    let v = val(&db, "SELECT a + b FROM t WHERE id = 1");
    assert_eq!(v, Value::Null, "NULL + 5 should be NULL");
}

/// Boolean column WHERE true/false.
#[test]
fn test_boolean_column_filter() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, active BOOLEAN)").unwrap();
    db.execute("INSERT INTO t VALUES (1, true)").unwrap();
    db.execute("INSERT INTO t VALUES (2, false)").unwrap();
    db.execute("INSERT INTO t VALUES (3, true)").unwrap();
    db.flush().unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t WHERE active = true"), 2);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t WHERE active = false"), 1);
}

// ═════════════════════════════════════════════════════════════════
// B. Pagination & LIMIT/OFFSET edge cases
// ═════════════════════════════════════════════════════════════════

/// OFFSET beyond data returns empty.
#[test]
fn test_offset_beyond_data() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    for i in 1..=10 { db.execute(&format!("INSERT INTO t VALUES ({})", i)).unwrap(); }
    db.flush().unwrap();
    assert_eq!(rows(&db, "SELECT * FROM t ORDER BY id LIMIT 5 OFFSET 100").len(), 0);
}

/// LIMIT 0 returns empty.
#[test]
fn test_limit_zero() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    for i in 1..=5 { db.execute(&format!("INSERT INTO t VALUES ({})", i)).unwrap(); }
    db.flush().unwrap();
    assert_eq!(rows(&db, "SELECT * FROM t LIMIT 0").len(), 0);
}

/// Pagination consistency — pages cover all rows without gaps/overlaps.
#[test]
fn test_pagination_consistency() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=20 { db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap(); }
    db.flush().unwrap();
    let mut seen = std::collections::HashSet::new();
    for page in 0..4 {
        let r = rows(&db, &format!("SELECT id FROM t ORDER BY id LIMIT 5 OFFSET {}", page * 5));
        assert_eq!(r.len(), 5, "Page {} should have 5 rows", page);
        for row in &r {
            if let Value::Integer(i) = &row[0] { seen.insert(*i); }
        }
    }
    assert_eq!(seen.len(), 20, "Pagination should cover all 20 rows without gaps");
}

// ═════════════════════════════════════════════════════════════════
// C. UPDATE/DELETE edge cases
// ═════════════════════════════════════════════════════════════════

/// UPDATE non-existent row — no error, 0 affected.
#[test]
fn test_update_nonexistent_row() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.flush().unwrap();
    let r = db.execute("UPDATE t SET v = 99 WHERE id = 999");
    assert!(r.is_ok(), "UPDATE non-existent should not error");
}

/// DELETE non-existent row — no error.
#[test]
fn test_delete_nonexistent_row() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.flush().unwrap();
    let r = db.execute("DELETE FROM t WHERE id = 999");
    assert!(r.is_ok(), "DELETE non-existent should not error");
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 1);
}

/// UPDATE all columns at once.
#[test]
fn test_update_all_columns() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b TEXT, c FLOAT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10, 'hello', 1.5)").unwrap();
    db.execute("UPDATE t SET a = 20, b = 'world', c = 2.5 WHERE id = 1").unwrap();
    db.flush().unwrap();
    let r = rows(&db, "SELECT a, b, c FROM t WHERE id = 1");
    assert_eq!(r[0][0], Value::Integer(20));
    assert_eq!(r[0][1], Value::text("world".into()));
    match &r[0][2] { Value::Float(f) => assert!((f - 2.5).abs() < 0.001), _ => panic!("Expected Float") }
}

/// Repeated UPDATE on same row — last write wins.
#[test]
fn test_repeated_update_last_wins() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 0)").unwrap();
    for i in 1..=100 {
        db.execute(&format!("UPDATE t SET v = {} WHERE id = 1", i)).unwrap();
    }
    db.flush().unwrap();
    assert_eq!(val(&db, "SELECT v FROM t WHERE id = 1"), Value::Integer(100));
}

// ═════════════════════════════════════════════════════════════════
// D. Recovery & persistence
// ═════════════════════════════════════════════════════════════════

/// Data survives multiple checkpoint cycles.
#[test]
fn test_multiple_checkpoint_cycles() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").unwrap();
        for cycle in 0..3 {
            for i in 0..10 {
                let id = cycle * 10 + i;
                db.execute(&format!("INSERT INTO t VALUES ({}, 'c{}r{}')", id, cycle, i)).unwrap();
            }
            db.checkpoint().unwrap();
        }
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 30);
    // Verify last cycle's data.
    let r = rows(&db, "SELECT v FROM t WHERE id = 29");
    assert_eq!(r[0][0], Value::text("c2r9".into()));
}

/// Empty table survives recovery.
#[test]
fn test_empty_table_survives_recovery() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 0);
    // Can still insert after recovery.
    db.execute("INSERT INTO t VALUES (1, 42)").unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 1);
}

/// UPDATE then checkpoint then reopen — value persists.
#[test]
fn test_update_persists_across_recovery() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        db.execute("UPDATE t SET v = 20 WHERE id = 1").unwrap();
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(val(&db, "SELECT v FROM t WHERE id = 1"), Value::Integer(20));
}

// ═════════════════════════════════════════════════════════════════
// E. SQL semantic edge cases
// ═════════════════════════════════════════════════════════════════

/// COUNT(*) vs COUNT(column) — column skips NULLs.
#[test]
fn test_count_star_vs_count_column() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30)").unwrap();
    db.flush().unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 3);
    assert_eq!(count(&db, "SELECT COUNT(v) FROM t"), 2, "COUNT(v) should skip NULL");
}

/// IS NULL / IS NOT NULL filtering.
#[test]
fn test_is_null_is_not_null() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'b')").unwrap();
    db.flush().unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t WHERE v IS NULL"), 1);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t WHERE v IS NOT NULL"), 2);
}

/// DISTINCT with NULLs.
#[test]
fn test_distinct_with_nulls() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'a')").unwrap();
    db.execute("INSERT INTO t VALUES (4, NULL)").unwrap();
    db.flush().unwrap();
    let r = rows(&db, "SELECT DISTINCT v FROM t");
    // DISTINCT should include NULL as a distinct value.
    assert!(r.len() >= 2, "DISTINCT should have at least 2 values ('a' and NULL)");
}

/// WHERE with compound AND + OR precedence.
#[test]
fn test_and_or_precedence() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)").unwrap();
    for i in 1..=10 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {}, {})", i, i, i % 3)).unwrap();
    }
    db.flush().unwrap();
    // a > 7 AND (b = 1 OR b = 2) — parentheses matter.
    let r = rows(&db, "SELECT id FROM t WHERE a > 7 AND (b = 1 OR b = 2)");
    for row in &r {
        if let Value::Integer(id) = &row[0] {
            assert!(*id > 7, "All results should have id > 7");
        }
    }
}

// ═════════════════════════════════════════════════════════════════
// F. Large / boundary values
// ═════════════════════════════════════════════════════════════════

/// Large text value.
#[test]
fn test_large_text_value() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").unwrap();
    let big = "x".repeat(10000);
    db.execute(&format!("INSERT INTO t VALUES (1, '{}')", big)).unwrap();
    db.flush().unwrap();
    let r = rows(&db, "SELECT v FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Text(t) => assert_eq!(t.len(), 10000, "Large text should survive"),
        _ => panic!("Expected Text"),
    }
}

/// i64::MIN and i64::MAX stored and retrieved correctly.
#[test]
fn test_i64_extreme_values() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v BIGINT)").unwrap();
    db.execute(&format!("INSERT INTO t VALUES (1, {})", i64::MAX)).unwrap();
    db.execute(&format!("INSERT INTO t VALUES (2, {})", i64::MIN)).unwrap();
    db.flush().unwrap();
    assert_eq!(val(&db, "SELECT v FROM t WHERE id = 1"), Value::Integer(i64::MAX));
    assert_eq!(val(&db, "SELECT v FROM t WHERE id = 2"), Value::Integer(i64::MIN));
}

/// Negative numbers in WHERE.
#[test]
fn test_negative_numbers_in_where() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, -100)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 0)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 100)").unwrap();
    db.flush().unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t WHERE v < 0"), 1);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t WHERE v >= 0"), 2);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t WHERE v = -100"), 1);
}

// ═════════════════════════════════════════════════════════════════
// G. Batch INSERT & mixed operations
// ═════════════════════════════════════════════════════════════════

/// Batch INSERT then immediate SELECT — no flush needed.
#[test]
fn test_batch_insert_then_immediate_select() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    let mut sql = String::from("INSERT INTO t VALUES ");
    for i in 1..=100 {
        if i > 1 { sql.push(','); }
        sql.push_str(&format!("({}, {})", i, i * 2));
    }
    db.execute(&sql).unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 100);
    assert_eq!(val(&db, "SELECT v FROM t WHERE id = 50"), Value::Integer(100));
}

/// Interleaved INSERT/UPDATE/DELETE/SELECT cycle.
#[test]
fn test_interleaved_crud_cycle() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    // INSERT
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    // SELECT
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 2);
    // UPDATE
    db.execute("UPDATE t SET v = 15 WHERE id = 1").unwrap();
    assert_eq!(val(&db, "SELECT v FROM t WHERE id = 1"), Value::Integer(15));
    // DELETE
    db.execute("DELETE FROM t WHERE id = 2").unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 1);
    // Re-INSERT deleted id
    db.execute("INSERT INTO t VALUES (2, 25)").unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 2);
    assert_eq!(val(&db, "SELECT v FROM t WHERE id = 2"), Value::Integer(25));
}

// ═════════════════════════════════════════════════════════════════
// H. Multi-table scenarios
// ═════════════════════════════════════════════════════════════════

/// Operations on multiple tables don't interfere.
#[test]
fn test_multi_table_isolation() {
    let (db, _d) = create();
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("CREATE TABLE b (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO b VALUES (1, 20)").unwrap();
    db.flush().unwrap();
    assert_eq!(val(&db, "SELECT v FROM a WHERE id = 1"), Value::Integer(10));
    assert_eq!(val(&db, "SELECT v FROM b WHERE id = 1"), Value::Integer(20));
    // Update a, b should be unaffected.
    db.execute("UPDATE a SET v = 99 WHERE id = 1").unwrap();
    assert_eq!(val(&db, "SELECT v FROM b WHERE id = 1"), Value::Integer(20));
}

/// Aggregate on empty table.
#[test]
fn test_aggregate_empty_table() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.flush().unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 0);
    let r = rows(&db, "SELECT SUM(v) FROM t");
    assert_eq!(r.len(), 1); // aggregate returns 1 row even on empty table
    let r2 = rows(&db, "SELECT MAX(v) FROM t");
    assert_eq!(r2.len(), 1);
}

// ═════════════════════════════════════════════════════════════════
// I. LIKE pattern edge cases
// ═════════════════════════════════════════════════════════════════

/// LIKE with % at start.
#[test]
fn test_like_suffix_match() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'hello world')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'world hello')").unwrap();
    db.flush().unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t WHERE name LIKE '%world'"), 1);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t WHERE name LIKE 'hello%'"), 1);
}

/// LIKE with underscore wildcard.
#[test]
fn test_like_underscore_wildcard() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'cat')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'cot')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'coat')").unwrap();
    db.flush().unwrap();
    // c_t matches 'cat' and 'cot' (3 chars), not 'coat' (4 chars).
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t WHERE name LIKE 'c_t'"), 2);
}

// ═════════════════════════════════════════════════════════════════
// J. ORDER BY edge cases
// ═════════════════════════════════════════════════════════════════

/// ORDER BY on FLOAT column.
#[test]
fn test_order_by_float() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, score FLOAT)").unwrap();
    for i in 1..=5 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, (6 - i) as f64 * 1.5)).unwrap();
    }
    db.flush().unwrap();
    let r = rows(&db, "SELECT id FROM t ORDER BY score ASC");
    // scores: 1.5, 3.0, 4.5, 6.0, 7.5 → ids: 5, 4, 3, 2, 1
    if let Value::Integer(id) = &r[0][0] {
        assert_eq!(*id, 5, "Lowest score should be id=5");
    }
}

/// ORDER BY ASC then DESC — reversed results.
#[test]
fn test_order_by_asc_desc_reversed() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=5 { db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap(); }
    db.flush().unwrap();
    let asc = rows(&db, "SELECT v FROM t ORDER BY v ASC LIMIT 3");
    let desc = rows(&db, "SELECT v FROM t ORDER BY v DESC LIMIT 3");
    // ASC: 1,2,3  DESC: 5,4,3
    assert_eq!(asc[0][0], Value::Integer(1));
    assert_eq!(desc[0][0], Value::Integer(5));
}

// ═════════════════════════════════════════════════════════════════
// K. NULL handling deep tests
// ═════════════════════════════════════════════════════════════════

/// WHERE col = NULL returns nothing (NULL comparison is unknown).
#[test]
fn test_where_equals_null_returns_empty() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    db.flush().unwrap();
    // WHERE v = NULL should return 0 rows (SQL: NULL = NULL is unknown).
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t WHERE v = NULL"), 0);
}

/// SUM with mixed NULL and non-NULL.
#[test]
fn test_sum_skips_nulls() {
    let (db, _d) = create();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 20)").unwrap();
    db.flush().unwrap();
    let r = rows(&db, "SELECT SUM(v) FROM t");
    match &r[0][0] {
        Value::Integer(i) => assert_eq!(*i, 30, "SUM should skip NULL: 10 + 20 = 30"),
        _ => panic!("Expected Integer"),
    }
}
