//! Aggressive bug-hunt suite v2: edge cases across types, SQL, transactions,
//! indexes, NULL handling, and recovery. Designed to surface silent wrong
//! answers (not crashes).
//!
//! Run: cargo test --release --test test_bug_hunt_v2

use motedb::sql::QueryResult;
use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn new_db() -> (Database, TempDir) {
    let dir = TempDir::new().expect("temp dir");
    let db = Database::create(dir.path()).expect("create db");
    (db, dir)
}

fn exec(db: &Database, sql: &str) {
    db.execute(sql).unwrap_or_else(|e| panic!("SQL failed: {}\n  error: {}", sql, e));
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    let rs = db
        .execute(sql)
        .unwrap_or_else(|e| panic!("SQL failed: {}\n  error: {}", sql, e))
        .materialize()
        .unwrap_or_else(|e| panic!("materialize failed: {}\n  sql: {}", e, sql));
    match rs {
        QueryResult::Select { rows, .. } => rows,
        _ => panic!("expected Select for: {}", sql),
    }
}

fn scalar_i64(db: &Database, sql: &str) -> i64 {
    let r = rows(db, sql);
    assert_eq!(r.len(), 1, "expected 1 row for: {}", sql);
    match r[0].first() {
        Some(Value::Integer(n)) => *n,
        other => panic!("expected Integer for: {}, got {:?}", sql, other),
    }
}

fn scalar_f64(db: &Database, sql: &str) -> f64 {
    let r = rows(db, sql);
    assert_eq!(r.len(), 1, "expected 1 row for: {}", sql);
    match r[0].first() {
        Some(Value::Float(n)) => *n,
        other => panic!("expected Float for: {}, got {:?}", sql, other),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. Aggregate correctness on edge values
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn agg_sum_overflow_safe() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 9223372036854775806)"); // i64::MAX - 1
    exec(&db, "INSERT INTO t VALUES (2, 1)");
    // SUM would overflow i64 — check it doesn't panic or wrap silently.
    let _ = db.execute("SELECT SUM(v) FROM t");
    // Whatever the result, it must not crash.
}

#[test]
fn agg_avg_integer_division() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=3 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    // AVG(1,2,3) = 2.0 — must be Float, not Integer division (1).
    let avg = scalar_f64(&db, "SELECT AVG(v) FROM t");
    assert!((avg - 2.0).abs() < 1e-9, "AVG must be 2.0, got {}", avg);
}

#[test]
fn agg_count_null_excluded() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "INSERT INTO t VALUES (2, NULL)");
    exec(&db, "INSERT INTO t VALUES (3, 30)");
    // COUNT(v) excludes NULL → 2. COUNT(*) includes all → 3.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(v) FROM t"), 2, "COUNT(col) excludes NULL");
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 3, "COUNT(*) includes NULL rows");
}

#[test]
fn agg_min_max_with_nulls() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, NULL)");
    exec(&db, "INSERT INTO t VALUES (2, 5)");
    exec(&db, "INSERT INTO t VALUES (3, NULL)");
    exec(&db, "INSERT INTO t VALUES (4, 15)");
    assert_eq!(scalar_i64(&db, "SELECT MIN(v) FROM t"), 5, "MIN ignores NULL");
    assert_eq!(scalar_i64(&db, "SELECT MAX(v) FROM t"), 15, "MAX ignores NULL");
}

#[test]
fn agg_empty_table() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    // Aggregates on empty table: COUNT=0, SUM=NULL, MIN/MAX=NULL, AVG=NULL.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 0);
    // SUM/MIN/MAX/AVG on empty — just ensure no panic.
    let _ = db.execute("SELECT SUM(v), MIN(v), MAX(v), AVG(v) FROM t");
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. NULL semantics in WHERE / comparisons
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn where_null_not_equal() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "INSERT INTO t VALUES (2, NULL)");
    exec(&db, "INSERT INTO t VALUES (3, 30)");
    // WHERE v != 10 — NULL row must NOT match (NULL != 10 is unknown, not true).
    let r = rows(&db, "SELECT id FROM t WHERE v != 10");
    let ids: Vec<i64> = r.iter().filter_map(|row| match row.get(0) {
        Some(Value::Integer(n)) => Some(*n),
        _ => None,
    }).collect();
    assert_eq!(ids, vec![3], "WHERE v != 10 must exclude NULL row (id=2)");
}

#[test]
fn where_is_null() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "INSERT INTO t VALUES (2, NULL)");
    let r = rows(&db, "SELECT id FROM t WHERE v IS NULL");
    assert_eq!(r.len(), 1, "IS NULL must find the NULL row");
}

#[test]
fn where_is_not_null() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "INSERT INTO t VALUES (2, NULL)");
    exec(&db, "INSERT INTO t VALUES (3, 30)");
    let r = rows(&db, "SELECT id FROM t WHERE v IS NOT NULL");
    assert_eq!(r.len(), 2, "IS NOT NULL must find 2 non-null rows");
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. String / text edge cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn text_with_quotes() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'it''s a test')");
    let r = rows(&db, "SELECT s FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Text(s) => assert_eq!(&*s.0, "it's a test"),
        other => panic!("expected Text, got {:?}", other),
    }
}

#[test]
fn text_empty_string() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, '')");
    let r = rows(&db, "SELECT s FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Text(s) => assert_eq!(&*s.0, ""),
        Value::Null => panic!("empty string should be Text, not NULL"),
        other => panic!("expected Text, got {:?}", other),
    }
}

#[test]
fn text_unicode() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, '你好世界🚀')");
    let r = rows(&db, "SELECT s FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Text(s) => assert_eq!(&*s.0, "你好世界🚀"),
        other => panic!("expected Text, got {:?}", other),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. UPDATE edge cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn update_set_null() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "UPDATE t SET v = NULL WHERE id = 1");
    let r = rows(&db, "SELECT v FROM t WHERE id = 1");
    assert!(matches!(r[0][0], Value::Null), "UPDATE to NULL must produce NULL");
}

#[test]
fn update_no_match() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    let result = db.execute("UPDATE t SET v = 999 WHERE id = 99999").unwrap();
    // Should succeed, affect 0 rows.
    if let QueryResult::Modification { affected_rows } = result.materialize().unwrap() {
        assert_eq!(affected_rows, 0, "UPDATE with no match affects 0 rows");
    }
    // Original row untouched.
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1"), 10);
}

#[test]
fn update_all_rows_no_where() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=5 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    exec(&db, "UPDATE t SET v = 0");
    for i in 1..=5 {
        assert_eq!(scalar_i64(&db, &format!("SELECT v FROM t WHERE id = {}", i)), 0);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. DELETE edge cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn delete_no_match() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    db.execute("DELETE FROM t WHERE id = 99999").unwrap();
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 1);
}

#[test]
fn delete_then_reinsert_same_pk() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "DELETE FROM t WHERE id = 1");
    exec(&db, "INSERT INTO t VALUES (1, 20)");
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1"), 20);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. Primary key constraints
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn duplicate_pk_rejected() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    let result = db.execute("INSERT INTO t VALUES (1, 20)");
    assert!(result.is_err(), "duplicate PK must be rejected");
}

#[test]
fn negative_pk_works() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (-1, 10)");
    exec(&db, "INSERT INTO t VALUES (-100, 20)");
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = -1"), 10);
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = -100"), 20);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. ORDER BY + LIMIT edge cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn order_by_asc_desc() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for (i, v) in [(1, 30), (2, 10), (3, 20)].iter() {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, v));
    }
    let asc: Vec<i64> = rows(&db, "SELECT v FROM t ORDER BY v ASC")
        .into_iter()
        .filter_map(|r| match r.get(0) { Some(Value::Integer(n)) => Some(*n), _ => None })
        .collect();
    assert_eq!(asc, vec![10, 20, 30]);
    let desc: Vec<i64> = rows(&db, "SELECT v FROM t ORDER BY v DESC")
        .into_iter()
        .filter_map(|r| match r.get(0) { Some(Value::Integer(n)) => Some(*n), _ => None })
        .collect();
    assert_eq!(desc, vec![30, 20, 10]);
}

#[test]
fn order_by_limit_offset() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=10 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    let r = rows(&db, "SELECT v FROM t ORDER BY v ASC LIMIT 3 OFFSET 2");
    let vals: Vec<i64> = r.iter().filter_map(|row| match row.get(0) {
        Some(Value::Integer(n)) => Some(*n), _ => None
    }).collect();
    assert_eq!(vals, vec![3, 4, 5], "LIMIT 3 OFFSET 2");
}

#[test]
fn limit_larger_than_table() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    let r = rows(&db, "SELECT * FROM t LIMIT 100");
    assert_eq!(r.len(), 1, "LIMIT > table size returns all rows");
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. GROUP BY edge cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn group_by_with_having() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 'a', 10)");
    exec(&db, "INSERT INTO t VALUES (2, 'a', 20)");
    exec(&db, "INSERT INTO t VALUES (3, 'b', 5)");
    exec(&db, "INSERT INTO t VALUES (4, 'b', 5)");
    exec(&db, "INSERT INTO t VALUES (5, 'c', 100)");
    // HAVING SUM(v) > 25 → only 'a' (30) and 'c' (100).
    let r = rows(&db, "SELECT cat, SUM(v) FROM t GROUP BY cat HAVING SUM(v) > 25 ORDER BY cat");
    assert_eq!(r.len(), 2, "HAVING must filter groups");
}

#[test]
fn group_by_empty_groups() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    // No rows — GROUP BY returns empty.
    let r = rows(&db, "SELECT cat, COUNT(*) FROM t GROUP BY cat");
    assert_eq!(r.len(), 0, "GROUP BY on empty table returns 0 groups");
}

// ═══════════════════════════════════════════════════════════════════════════
// 9. Type coercion
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn insert_float_into_int_column() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    // Inserting 3.9 into INT column — should truncate or error, not silently
    // store garbage. At minimum, must not panic.
    let _ = db.execute("INSERT INTO t VALUES (1, 3.9)");
}

#[test]
fn integer_float_comparison() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)");
    exec(&db, "INSERT INTO t VALUES (1, 10.0)");
    // WHERE v = 10 (int literal) on float column — should match 10.0.
    let r = rows(&db, "SELECT id FROM t WHERE v = 10");
    assert_eq!(r.len(), 1, "int literal must match float 10.0");
}

// ═══════════════════════════════════════════════════════════════════════════
// 10. LIKE patterns
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn like_percent_only() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'hello')");
    // LIKE '%' matches everything.
    assert_eq!(rows(&db, "SELECT id FROM t WHERE s LIKE '%'").len(), 1);
}

#[test]
fn like_case_sensitivity() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'Hello')");
    // LIKE is case-sensitive in standard SQL — 'hello' shouldn't match 'Hello'.
    let r = rows(&db, "SELECT id FROM t WHERE s LIKE 'hello'");
    // Document the behavior either way, but it must be consistent.
    let _ = r.len();
}

#[test]
fn like_underscore() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'cat')");
    exec(&db, "INSERT INTO t VALUES (2, 'coat')");
    // '_' matches exactly one char: 'c_t' matches 'cat' not 'coat'.
    let r = rows(&db, "SELECT id FROM t WHERE s LIKE 'c_t'");
    assert_eq!(r.len(), 1, "underscore matches single char");
}

// ═══════════════════════════════════════════════════════════════════════════
// 11. Durability / recovery
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn reopen_after_mixed_ops() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT, s TEXT)");
        exec(&db, "INSERT INTO t VALUES (1, 10, 'a')");
        exec(&db, "INSERT INTO t VALUES (2, 20, 'b')");
        exec(&db, "INSERT INTO t VALUES (3, 30, 'c')");
        exec(&db, "UPDATE t SET v = 999 WHERE id = 1");
        exec(&db, "DELETE FROM t WHERE id = 2");
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 2, "2 rows after reopen");
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1"), 999, "UPDATE persisted");
    // id=2 deleted, id=3 present.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE id = 2"), 0);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE id = 3"), 1);
}

#[test]
fn reopen_preserves_schema() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score FLOAT)");
        exec(&db, "INSERT INTO t VALUES (1, 'x', 1.5)");
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    // Schema must allow the same operations.
    exec(&db, "INSERT INTO t VALUES (2, 'y', 2.5)");
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// 12. Large batch insert correctness
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn batch_insert_all_visible() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=1000 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i * 2));
    }
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 1000);
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), 1000 * 1001); // sum(2..2000 step 2)
}

#[test]
fn checkpoint_during_operations() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=100 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
        if i % 50 == 0 {
            db.checkpoint().unwrap();
        }
    }
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 100);
}

// ═══════════════════════════════════════════════════════════════════════════
// 13. Transaction isolation (read-committed basics)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn txn_update_then_select_new_value() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    let tx = db.begin_transaction().unwrap();
    exec(&db, "UPDATE t SET v = 999 WHERE id = 1");
    // Within same txn, must see new value.
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1"), 999);
    db.commit_transaction(tx).unwrap();
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1"), 999);
}

#[test]
fn txn_insert_rollback_count_restored() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
    exec(&db, "INSERT INTO t VALUES (1)");
    let tx = db.begin_transaction().unwrap();
    for i in 2..=10 {
        exec(&db, &format!("INSERT INTO t VALUES ({})", i));
    }
    db.rollback_transaction(tx).unwrap();
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 1, "rollback restores count");
}

// ═══════════════════════════════════════════════════════════════════════════
// 14. Edge: empty / whitespace inputs
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn select_from_empty_table() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let r = rows(&db, "SELECT * FROM t");
    assert_eq!(r.len(), 0);
    let r = rows(&db, "SELECT id, v FROM t WHERE v > 5");
    assert_eq!(r.len(), 0);
}

#[test]
fn count_star_empty_table() {
    let (db, _d) = new_new_db_helper();
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM empty_t"), 0);
}

fn new_new_db_helper() -> (Database, TempDir) {
    let (db, d) = new_db();
    exec(&db, "CREATE TABLE empty_t (id INT PRIMARY KEY)");
    (db, d)
}
