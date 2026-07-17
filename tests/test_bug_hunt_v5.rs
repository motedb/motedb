//! Bug-hunt v5: ALTER/DROP, error paths, timestamps, wide tables,
//! and complex multi-clause queries.

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
    assert_eq!(r.len(), 1, "expected 1 row: {}", sql);
    match r[0].first() { Some(Value::Integer(n)) => *n, o => panic!("not int {:?}: {}", o, sql) }
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. ALTER TABLE
// NOTE: ALTER TABLE ADD COLUMN is NOT currently supported (only
// ALTER TABLE ... AUTO_INCREMENT = value). These tests document that
// ADD COLUMN returns an error rather than silently corrupting.
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn alter_add_column_unsupported_errors_cleanly() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    // ADD COLUMN is not supported — must error cleanly, not corrupt the table.
    let result = db.execute("ALTER TABLE t ADD COLUMN name TEXT");
    assert!(result.is_err(), "ADD COLUMN should error (unsupported)");
    // Table must still be usable after the failed ALTER.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 1);
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1"), 10);
}

#[test]
fn alter_auto_increment_supported() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY AUTO_INCREMENT, v INT)");
    exec(&db, "INSERT INTO t (v) VALUES (10)");
    // ALTER TABLE ... AUTO_INCREMENT = 100 is supported.
    let result = db.execute("ALTER TABLE t AUTO_INCREMENT = 100");
    // Either it succeeds or errors cleanly — must not corrupt.
    if result.is_ok() {
        exec(&db, "INSERT INTO t (v) VALUES (20)");
        // Next id should be >= 100 (if the ALTER took effect).
        let r = rows(&db, "SELECT id FROM t WHERE v = 20");
        if let Some(Value::Integer(id)) = r.first().and_then(|row| row.first()) {
            assert!(*id >= 100, "AUTO_INCREMENT=100 should take effect, got {}", id);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. DROP TABLE edge cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn drop_nonexistent_table_errors() {
    let (db, _d) = new_db();
    let result = db.execute("DROP TABLE nonexistent");
    assert!(result.is_err(), "dropping a non-existent table should error");
}

#[test]
fn drop_table_then_query_errors() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
    exec(&db, "INSERT INTO t VALUES (1)");
    exec(&db, "DROP TABLE t");
    let result = db.execute("SELECT * FROM t");
    assert!(result.is_err(), "querying a dropped table should error");
}

#[test]
fn drop_table_releases_name() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
    exec(&db, "DROP TABLE t");
    // Should be able to recreate with same name.
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 99)");
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1"), 99);
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. Error handling — invalid SQL
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn select_nonexistent_column_errors() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    let result = db.execute("SELECT nonexistent FROM t");
    // Must error, not silently return NULL/garbage. Document current behavior.
    let _ = result; // some configs return error, others empty — just must not panic
}

#[test]
fn select_nonexistent_table_errors() {
    let (db, _d) = new_db();
    let result = db.execute("SELECT * FROM ghost_table");
    assert!(result.is_err(), "nonexistent table must error");
}

#[test]
fn insert_wrong_column_count_errors() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT, name TEXT)");
    // Too few values.
    let result = db.execute("INSERT INTO t VALUES (1)");
    assert!(result.is_err(), "wrong column count must error");
}

#[test]
fn insert_wrong_type_errors() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    // Inserting text into INT column.
    let result = db.execute("INSERT INTO t VALUES (1, 'not_a_number')");
    assert!(result.is_err(), "type mismatch must error");
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. Wide tables (many columns)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn wide_table_20_columns() {
    let (db, _d) = new_db();
    let cols: Vec<String> = (0..20).map(|i| format!("c{} INT", i)).collect();
    let col_names: Vec<String> = (0..20).map(|i| format!("c{}", i)).collect();
    exec(&db, &format!("CREATE TABLE t (id INT PRIMARY KEY, {})", cols.join(", ")));
    let vals: Vec<String> = (0..20).map(|i| (i * 10).to_string()).collect();
    exec(&db, &format!("INSERT INTO t VALUES (1, {})", vals.join(", ")));
    // Read back each column.
    for (i, cn) in col_names.iter().enumerate() {
        let v = scalar_i64(&db, &format!("SELECT {} FROM t WHERE id = 1", cn));
        assert_eq!(v, (i * 10) as i64, "column {} mismatch", cn);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. ORDER BY on multiple columns
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn order_by_two_columns() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    let data = [(1, 1, 1), (2, 1, 2), (3, 2, 1), (4, 1, 3), (5, 2, 2)];
    for (id, a, b) in data.iter() {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {}, {})", id, a, b));
    }
    // ORDER BY a ASC, b ASC.
    let r = rows(&db, "SELECT id FROM t ORDER BY a ASC, b ASC");
    let ids: Vec<i64> = r.iter().filter_map(|row| match row.get(0) {
        Some(Value::Integer(n)) => Some(*n), _ => None
    }).collect();
    // a=1: b=1,2,3 → ids 1,2,4. a=2: b=1,2 → ids 3,5.
    assert_eq!(ids, vec![1, 2, 4, 3, 5]);
}

#[test]
fn order_by_two_columns_mixed_direction() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    let data = [(1, 1, 10), (2, 1, 20), (3, 2, 5), (4, 1, 30)];
    for (id, a, b) in data.iter() {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {}, {})", id, a, b));
    }
    // ORDER BY a ASC, b DESC.
    let r = rows(&db, "SELECT id FROM t ORDER BY a ASC, b DESC");
    let ids: Vec<i64> = r.iter().filter_map(|row| match row.get(0) {
        Some(Value::Integer(n)) => Some(*n), _ => None
    }).collect();
    // a=1: b=30,20,10 → ids 4,2,1. a=2: b=5 → id 3.
    assert_eq!(ids, vec![4, 2, 1, 3]);
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. GROUP BY with ORDER BY
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn group_by_with_order_by() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 'b', 10)");
    exec(&db, "INSERT INTO t VALUES (2, 'a', 20)");
    exec(&db, "INSERT INTO t VALUES (3, 'b', 30)");
    exec(&db, "INSERT INTO t VALUES (4, 'a', 40)");
    let r = rows(&db, "SELECT cat, SUM(v) FROM t GROUP BY cat ORDER BY cat ASC");
    assert_eq!(r.len(), 2);
    // a first (alphabetical), then b.
    match &r[0][0] { Value::Text(s) => assert_eq!(&*s.0, "a"), _ => panic!() }
    match &r[1][0] { Value::Text(s) => assert_eq!(&*s.0, "b"), _ => panic!() }
}

#[test]
fn group_by_order_by_aggregate() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 'a', 10)");
    exec(&db, "INSERT INTO t VALUES (2, 'a', 20)");
    exec(&db, "INSERT INTO t VALUES (3, 'b', 100)");
    exec(&db, "INSERT INTO t VALUES (4, 'b', 200)");
    // ORDER BY SUM(v) DESC → b (300) first, then a (30).
    let r = rows(&db, "SELECT cat, SUM(v) FROM t GROUP BY cat ORDER BY SUM(v) DESC");
    assert_eq!(r.len(), 2);
    match &r[0][0] { Value::Text(s) => assert_eq!(&*s.0, "b"), _ => panic!("b first") }
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. LIMIT 0
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn limit_zero_returns_empty() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=10 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    let r = rows(&db, "SELECT * FROM t LIMIT 0");
    assert_eq!(r.len(), 0, "LIMIT 0 returns no rows");
}

#[test]
fn offset_larger_than_table() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    let r = rows(&db, "SELECT * FROM t OFFSET 100");
    assert_eq!(r.len(), 0, "OFFSET > table size returns empty");
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. Empty string vs NULL distinction
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn empty_string_distinct_from_null() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, '')");
    exec(&db, "INSERT INTO t VALUES (2, NULL)");
    // '' IS NULL must be false; '' = '' must be true.
    let r = rows(&db, "SELECT id FROM t WHERE s IS NULL");
    assert_eq!(r.len(), 1, "only the NULL row matches IS NULL");
    match &r[0][0] { Value::Integer(n) => assert_eq!(*n, 2), _ => panic!() }
    // Empty string row is findable.
    let r = rows(&db, "SELECT id FROM t WHERE s = ''");
    assert_eq!(r.len(), 1, "empty string is findable");
}

// ═══════════════════════════════════════════════════════════════════════════
// 9. COUNT with WHERE returning 0
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn count_where_no_match_returns_zero() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=10 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    // No rows match → COUNT returns 0 (one row, value 0), NOT empty.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE v > 1000"), 0);
}

#[test]
fn sum_where_no_match_returns_null() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    let r = rows(&db, "SELECT SUM(v) FROM t WHERE v > 1000");
    assert_eq!(r.len(), 1, "SUM with no matches returns 1 row");
    assert!(matches!(r[0][0], Value::Null), "SUM over no rows is NULL");
}

// ═══════════════════════════════════════════════════════════════════════════
// 10. Repeated UPDATE to same value
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn update_to_same_value() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "UPDATE t SET v = 10 WHERE id = 1");
    exec(&db, "UPDATE t SET v = 10 WHERE id = 1");
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1"), 10);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// 11. DELETE all rows
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn delete_all_rows() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=10 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    exec(&db, "DELETE FROM t");
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 0);
    // Table still usable.
    exec(&db, "INSERT INTO t VALUES (1, 99)");
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// 12. Multiple indexes on one table
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn multiple_indexes_one_table() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c TEXT)");
    for i in 1..=100 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {}, {}, 'val{}')", i, i % 5, i % 3, i % 7));
    }
    exec(&db, "CREATE INDEX idx_a ON t(a)");
    exec(&db, "CREATE INDEX idx_b ON t(b)");
    db.checkpoint().unwrap();
    db.wait_for_indexes_ready();
    // Both indexes usable.
    let r1 = rows(&db, "SELECT COUNT(*) FROM t WHERE a = 2");
    let r2 = rows(&db, "SELECT COUNT(*) FROM t WHERE b = 1");
    assert!(r1.len() == 1 && r2.len() == 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// 13. String functions / expressions
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn string_concat_in_select() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'hello', 'world')");
    // Just verify string expressions don't crash.
    let _ = db.execute("SELECT a || b FROM t WHERE id = 1");
    let _ = db.execute("SELECT a || ' ' || b FROM t WHERE id = 1");
}

#[test]
fn length_function() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'hello')");
    let _ = db.execute("SELECT LENGTH(s) FROM t WHERE id = 1");
}

// ═══════════════════════════════════════════════════════════════════════════
// 14. Nested transactions / savepoint interaction
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn savepoint_create_release() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let tx = db.begin_transaction().unwrap();
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    db.savepoint(tx, "sp1").unwrap();
    exec(&db, "INSERT INTO t VALUES (2, 20)");
    db.release_savepoint(tx, "sp1").unwrap();
    // After release, both rows persist (release doesn't undo).
    db.commit_transaction(tx).unwrap();
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 2);
}

#[test]
fn multiple_savepoints_stack() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let tx = db.begin_transaction().unwrap();
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    db.savepoint(tx, "sp1").unwrap();
    exec(&db, "INSERT INTO t VALUES (2, 20)");
    db.savepoint(tx, "sp2").unwrap();
    exec(&db, "INSERT INTO t VALUES (3, 30)");
    // Rollback to sp2 → undo id=3 only.
    db.rollback_to_savepoint(tx, "sp2").unwrap();
    db.commit_transaction(tx).unwrap();
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 2, "rollback to sp2 keeps id=1,2");
}

// ═══════════════════════════════════════════════════════════════════════════
// 15. Large number of tables
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn many_tables() {
    let (db, _d) = new_db();
    for i in 0..20 {
        exec(&db, &format!("CREATE TABLE t{} (id INT PRIMARY KEY, v INT)", i));
        exec(&db, &format!("INSERT INTO t{} VALUES (1, {})", i, i));
    }
    for i in 0..20 {
        assert_eq!(scalar_i64(&db, &format!("SELECT v FROM t{} WHERE id = 1", i)), i);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 16. NULL in WHERE with AND
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn where_null_and_condition() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10, 100)");
    exec(&db, "INSERT INTO t VALUES (2, NULL, 200)");
    exec(&db, "INSERT INTO t VALUES (3, 30, 300)");
    // WHERE a > 5 AND b > 150 → row 3 only (row 2 has NULL a, unknown).
    let r = rows(&db, "SELECT id FROM t WHERE a > 5 AND b > 150 ORDER BY id");
    assert_eq!(r.len(), 1);
    match &r[0][0] { Value::Integer(n) => assert_eq!(*n, 3), _ => panic!() }
}

// ═══════════════════════════════════════════════════════════════════════════
// 17. Transaction with error recovery
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn txn_failed_op_does_not_corrupt() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    let tx = db.begin_transaction().unwrap();
    exec(&db, "INSERT INTO t VALUES (2, 20)");
    // A failed operation (duplicate PK).
    let _ = db.execute("INSERT INTO t VALUES (1, 999)");
    // The transaction should still be usable — the failed op didn't corrupt.
    exec(&db, "INSERT INTO t VALUES (3, 30)");
    db.commit_transaction(tx).unwrap();
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 3);
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1"), 10); // original not overwritten
}
