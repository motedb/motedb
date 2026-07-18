//! Bug-hunt v15: deep data integrity — many segment flushes, concurrent
//! checkpoint + query, WHERE with LIKE + IN combo, multi-row UPDATE batch,
//! and aggregate over partially-deleted data.

use motedb::sql::QueryResult;
use motedb::types::Value;
use motedb::Database;
use std::sync::Arc;
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
// 1. Many segment flushes — data integrity across 10+ checkpoints
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn ten_checkpoints_data_integrity() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let mut expected_sum: i64 = 0;
    let mut expected_count: i64 = 0;
    for round in 0..10 {
        for i in 1..=50 {
            let id = round * 50 + i;
            exec(&db, &format!("INSERT INTO t VALUES ({}, {})", id, id));
            expected_sum += id;
            expected_count += 1;
        }
        db.checkpoint().unwrap();
        // Verify after each checkpoint.
        assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), expected_count,
            "after checkpoint {}, count mismatch", round + 1);
        assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), expected_sum,
            "after checkpoint {}, sum mismatch", round + 1);
    }
    // Final verification: spot check random rows.
    for id in [1, 50, 51, 100, 250, 499, 500].iter() {
        assert_eq!(scalar_i64(&db, &format!("SELECT v FROM t WHERE id = {}", id)), *id as i64);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. Delete every other row — tombstone-heavy scan correctness
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn delete_every_other_row_scan_correct() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=200 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i * 2));
    }
    // Delete all odd rows.
    for i in (1..=200).step_by(2) {
        exec(&db, &format!("DELETE FROM t WHERE id = {}", i));
    }
    // Should have 100 even rows.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 100);
    // Full scan must return exactly 100 rows.
    let r = rows(&db, "SELECT id FROM t ORDER BY id");
    assert_eq!(r.len(), 100, "full scan after tombstones");
    // All IDs should be even.
    for row in &r {
        match &row[0] {
            Value::Integer(n) => assert_eq!(*n % 2, 0, "all remaining IDs must be even"),
            _ => panic!(),
        }
    }
    // SUM should be 2*(2+4+...+200) = 2 * 2*(1+2+...+100) = 2*2*5050 = 20200.
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), 20200);
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. WHERE with LIKE + IN combo
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn where_like_and_in_combo() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, cat INT)");
    exec(&db, "INSERT INTO t VALUES (1, 'apple_1', 1)");
    exec(&db, "INSERT INTO t VALUES (2, 'apple_2', 2)");
    exec(&db, "INSERT INTO t VALUES (3, 'banana_1', 1)");
    exec(&db, "INSERT INTO t VALUES (4, 'apple_3', 3)");
    exec(&db, "INSERT INTO t VALUES (5, 'cherry_1', 1)");
    // name LIKE 'apple%' AND cat IN (1, 2) → ids 1, 2.
    let r = rows(&db, "SELECT id FROM t WHERE name LIKE 'apple%' AND cat IN (1, 2) ORDER BY id");
    assert_eq!(r.len(), 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. Multi-row UPDATE batch (all at once via different WHERE)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn update_different_values_per_row() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=10 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 0)", i));
    }
    // Update each row to a different value.
    for i in 1..=10 {
        exec(&db, &format!("UPDATE t SET v = {} WHERE id = {}", i * 100, i));
    }
    // Verify each row has the correct value.
    for i in 1..=10 {
        assert_eq!(scalar_i64(&db, &format!("SELECT v FROM t WHERE id = {}", i)), i * 100);
    }
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), 5500); // 100+200+...+1000
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. Aggregate over partially-deleted data
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn aggregate_after_partial_delete() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    for i in 1..=100 {
        let cat = if i <= 50 { "keep" } else { "delete" };
        exec(&db, &format!("INSERT INTO t VALUES ({}, '{}', {})", i, cat, i));
    }
    // Delete 'delete' rows.
    for i in 51..=100 {
        exec(&db, &format!("DELETE FROM t WHERE id = {}", i));
    }
    // SUM of remaining: 1+2+...+50 = 1275.
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), 1275);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 50);
    assert_eq!(scalar_i64(&db, "SELECT MIN(v) FROM t"), 1);
    assert_eq!(scalar_i64(&db, "SELECT MAX(v) FROM t"), 50);
    // GROUP BY on remaining data.
    let r = rows(&db, "SELECT cat, COUNT(*) FROM t GROUP BY cat");
    assert_eq!(r.len(), 1, "only 'keep' group remains");
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. Concurrent reads during checkpoint (no corruption)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn concurrent_reads_during_checkpoint() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=200 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    let db = Arc::new(db);
    let mut handles = Vec::new();
    // Reader threads.
    for _ in 0..3 {
        let db = Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            let mut ok = 0;
            for _ in 0..30 {
                if db.execute("SELECT COUNT(*) FROM t").and_then(|r| r.materialize()).is_ok() {
                    ok += 1;
                }
            }
            ok
        }));
    }
    // Checkpoint thread.
    let db2 = Arc::clone(&db);
    handles.push(std::thread::spawn(move || {
        db2.checkpoint().unwrap();
        0u64
    }));
    for h in handles { let _ = h.join(); }
    // After all threads, data must be intact.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 200);
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. WHERE with IS NULL + IS NOT NULL combo
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn where_is_null_and_is_not_null() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10, 20)");
    exec(&db, "INSERT INTO t VALUES (2, NULL, 30)");
    exec(&db, "INSERT INTO t VALUES (3, 40, NULL)");
    exec(&db, "INSERT INTO t VALUES (4, NULL, NULL)");
    // a IS NULL AND b IS NOT NULL → id 2.
    let r = rows(&db, "SELECT id FROM t WHERE a IS NULL AND b IS NOT NULL");
    assert_eq!(r.len(), 1);
    // a IS NOT NULL AND b IS NULL → id 3.
    let r = rows(&db, "SELECT id FROM t WHERE a IS NOT NULL AND b IS NULL");
    assert_eq!(r.len(), 1);
    // a IS NULL OR b IS NULL → ids 2, 3, 4.
    let r = rows(&db, "SELECT id FROM t WHERE a IS NULL OR b IS NULL ORDER BY id");
    assert_eq!(r.len(), 3);
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. Nested OR/AND with parentheses
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn nested_or_and_with_parens() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10, 100)");
    exec(&db, "INSERT INTO t VALUES (2, 20, 200)");
    exec(&db, "INSERT INTO t VALUES (3, 10, 200)");
    exec(&db, "INSERT INTO t VALUES (4, 20, 100)");
    exec(&db, "INSERT INTO t VALUES (5, 30, 300)");
    // (a = 10 OR a = 30) AND (b = 100 OR b = 300)
    // → id 1 (a=10,b=100 ✓), id 5 (a=30,b=300 ✓).
    let r = rows(&db, "SELECT id FROM t WHERE (a = 10 OR a = 30) AND (b = 100 OR b = 300) ORDER BY id");
    assert_eq!(r.len(), 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// 9. SUM after UPDATE that makes some values NULL
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn sum_after_update_to_null() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "INSERT INTO t VALUES (2, 20)");
    exec(&db, "INSERT INTO t VALUES (3, 30)");
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), 60);
    // Update id=2 to NULL.
    exec(&db, "UPDATE t SET v = NULL WHERE id = 2");
    // SUM should skip NULL: 10 + 30 = 40.
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), 40);
}

// ═══════════════════════════════════════════════════════════════════════════
// 10. GROUP BY with text — ensure groups sorted by key
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn group_by_text_sorted_output() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 'charlie', 1)");
    exec(&db, "INSERT INTO t VALUES (2, 'alpha', 2)");
    exec(&db, "INSERT INTO t VALUES (3, 'bravo', 3)");
    let r = rows(&db, "SELECT cat FROM t GROUP BY cat ORDER BY cat ASC");
    let names: Vec<String> = r.iter().filter_map(|row| match &row[0] {
        Value::Text(s) => Some(s.0.to_string()), _ => None
    }).collect();
    assert_eq!(names, vec!["alpha", "bravo", "charlie"]);
}

// ═══════════════════════════════════════════════════════════════════════════
// 11. COUNT(*) after UPDATE (no count change)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn count_stable_after_many_updates() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=100 { exec(&db, &format!("INSERT INTO t VALUES ({}, 0)", i)); }
    for cycle in 0..5 {
        exec(&db, &format!("UPDATE t SET v = {}", cycle));
        assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 100,
            "count stable after UPDATE cycle {}", cycle);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 12. SELECT with expression + WHERE + ORDER BY + LIMIT (everything combined)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn everything_combined_query() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, a INT, b INT)");
    for i in 1..=50 {
        let cat = if i % 2 == 0 { "even" } else { "odd" };
        exec(&db, &format!("INSERT INTO t VALUES ({}, '{}', {}, {})", i, cat, i, i * 2));
    }
    // SELECT a + b, cat FROM t WHERE a > 10 ORDER BY a + b DESC LIMIT 5.
    let r = rows(&db, "SELECT a + b, cat FROM t WHERE a > 10 ORDER BY a + b DESC LIMIT 5");
    assert_eq!(r.len(), 5);
    // Verify descending order of a+b.
    let vals: Vec<i64> = r.iter().filter_map(|row| match row.get(0) {
        Some(Value::Integer(n)) => Some(*n), _ => None
    }).collect();
    for i in 1..vals.len() {
        assert!(vals[i-1] >= vals[i], "must be descending: {} vs {}", vals[i-1], vals[i]);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 13. Text column with newlines
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn text_with_newline() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    // Just verify it doesn't crash — newlines in text.
    let _ = db.execute("INSERT INTO t VALUES (1, 'line1\nline2')");
    let r = rows(&db, "SELECT s FROM t WHERE id = 1");
    assert_eq!(r.len(), 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// 14. Zero and negative in aggregate
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn zero_and_negative_aggregate() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 0)");
    exec(&db, "INSERT INTO t VALUES (2, -5)");
    exec(&db, "INSERT INTO t VALUES (3, 10)");
    exec(&db, "INSERT INTO t VALUES (4, -3)");
    exec(&db, "INSERT INTO t VALUES (5, 0)");
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), 2); // 0-5+10-3+0 = 2
    assert_eq!(scalar_i64(&db, "SELECT MIN(v) FROM t"), -5);
    assert_eq!(scalar_i64(&db, "SELECT MAX(v) FROM t"), 10);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 5);
}

// ═══════════════════════════════════════════════════════════════════════════
// 15. Reopen after DELETE ALL + re-insert
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn reopen_after_delete_all_reinsert() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
        for i in 1..=50 { exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i)); }
        db.checkpoint().unwrap();
        exec(&db, "DELETE FROM t");
        db.checkpoint().unwrap();
        exec(&db, "INSERT INTO t VALUES (1, 999)");
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 1);
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1"), 999);
}

// ═══════════════════════════════════════════════════════════════════════════
// 16. Multiple tables — cross-table COUNT correctness
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn multi_table_count_independent() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE c (id INT PRIMARY KEY)");
    for i in 1..=30 { exec(&db, &format!("INSERT INTO a VALUES ({})", i)); }
    for i in 1..=20 { exec(&db, &format!("INSERT INTO b VALUES ({})", i)); }
    for i in 1..=10 { exec(&db, &format!("INSERT INTO c VALUES ({})", i)); }
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM a"), 30);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM b"), 20);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM c"), 10);
    // Delete from b should not affect a or c.
    exec(&db, "DELETE FROM b WHERE id <= 10");
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM a"), 30);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM b"), 10);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM c"), 10);
}
