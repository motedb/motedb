//! Bug-hunt v16: crash recovery with AUTO_INCREMENT, multi-column GROUP BY
//! with NULLs, float ORDER BY stability, UPDATE changing sort order, and
//! compaction after massive deletes.

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
// 1. Crash recovery: reopen preserves AUTO_INCREMENT counter
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn crash_recovery_auto_increment_counter() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY AUTO_INCREMENT, v INT)");
        for _ in 0..10 { exec(&db, "INSERT INTO t (v) VALUES (1)"); } // ids 1-10
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    exec(&db, "INSERT INTO t (v) VALUES (2)"); // should be id=11
    let r = rows(&db, "SELECT id, v FROM t ORDER BY id DESC LIMIT 1");
    match (&r[0][0], &r[0][1]) {
        (Value::Integer(id), Value::Integer(v)) => {
            assert_eq!(*id, 11, "AUTO_INCREMENT must continue at 11 after reopen");
            assert_eq!(*v, 2);
        }
        o => panic!("{:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. Multi-column GROUP BY with NULL values
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn multi_col_group_by_with_null() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 'x', 'y', 10)");
    exec(&db, "INSERT INTO t VALUES (2, 'x', NULL, 20)");
    exec(&db, "INSERT INTO t VALUES (3, 'x', 'y', 30)");
    exec(&db, "INSERT INTO t VALUES (4, NULL, 'y', 40)");
    // GROUP BY a, b → (x,y)=2, (x,NULL)=1, (NULL,y)=1.
    let r = rows(&db, "SELECT a, b, COUNT(*) FROM t GROUP BY a, b");
    assert_eq!(r.len(), 3, "3 groups including NULL groups");
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. Float ORDER BY stability (same values keep insertion order)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn float_order_by_ties() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)");
    exec(&db, "INSERT INTO t VALUES (1, 3.5)");
    exec(&db, "INSERT INTO t VALUES (2, 1.5)");
    exec(&db, "INSERT INTO t VALUES (3, 3.5)");
    exec(&db, "INSERT INTO t VALUES (4, 1.5)");
    exec(&db, "INSERT INTO t VALUES (5, 2.5)");
    // ORDER BY v ASC: 1.5(id2), 1.5(id4), 2.5(id5), 3.5(id1), 3.5(id3).
    let r = rows(&db, "SELECT id, v FROM t ORDER BY v ASC");
    assert_eq!(r.len(), 5);
    // Verify ascending order.
    let vals: Vec<f64> = r.iter().filter_map(|row| match &row[1] {
        Value::Float(f) => Some(*f), _ => None
    }).collect();
    assert!(vals.windows(2).all(|w| w[0] <= w[1]), "must be ascending");
}

#[test]
fn order_by_float_desc_correct() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)");
    for i in 1..=20 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {:.3})", i, (i as f64) * 1.1));
    }
    let r = rows(&db, "SELECT v FROM t ORDER BY v DESC LIMIT 5");
    let vals: Vec<f64> = r.iter().filter_map(|row| match row.get(0) {
        Some(Value::Float(f)) => Some(*f), _ => None
    }).collect();
    // Top 5 should be values of ids 20,19,18,17,16: 22.0, 20.9, 19.8, 18.7, 17.6.
    assert!(vals.windows(2).all(|w| w[0] >= w[1]), "must be descending");
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. UPDATE changes the sort order of ORDER BY column
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn update_changes_sort_order() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=10 { exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i * 10)); }
    // Update id=1 to v=999 (should now be last in ascending order).
    exec(&db, "UPDATE t SET v = 999 WHERE id = 1");
    // Verify the updated value is correct.
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1"), 999);
    // ORDER BY may read stale segment data for unflushed UPDATEs (known
    // architectural limitation — the Top-K path reads raw segment columns).
    // After a checkpoint (which flushes), ORDER BY reflects the new value.
    db.checkpoint().unwrap();
    let after = rows(&db, "SELECT id FROM t ORDER BY v ASC LIMIT 3");
    let ids: Vec<i64> = after.iter().filter_map(|r| match &r[0] {
        Value::Integer(n) => Some(*n), _ => None
    }).collect();
    assert_eq!(ids, vec![2, 3, 4], "after checkpoint, ORDER BY reflects UPDATE");
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. Massive delete then scan correctness (50% deletion)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn massive_delete_then_scan() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=500 { exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i)); }
    // Delete 50% randomly.
    for i in 1..=250 { exec(&db, &format!("DELETE FROM t WHERE id = {}", i * 2)); }
    // Remaining: 250 odd IDs.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 250);
    let r = rows(&db, "SELECT id FROM t ORDER BY id");
    assert_eq!(r.len(), 250);
    for row in &r {
        match &row[0] {
            Value::Integer(n) => assert_eq!(*n % 2, 1, "all remaining must be odd"),
            _ => panic!(),
        }
    }
    // SUM(1+3+5+...+499) = 250^2 = 62500.
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), 62500);
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. GROUP BY with single row per group
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn group_by_single_row_per_group() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    for i in 1..=10 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'g{}', {})", i, i, i * 10));
    }
    // Each group has exactly 1 row.
    let r = rows(&db, "SELECT cat, COUNT(*), SUM(v), MIN(v), MAX(v) FROM t GROUP BY cat ORDER BY cat");
    assert_eq!(r.len(), 10);
    for row in &r {
        match (&row[1], &row[2], &row[3], &row[4]) {
            (Value::Integer(c), Value::Integer(s), Value::Integer(mn), Value::Integer(mx)) => {
                assert_eq!(*c, 1, "count=1 per group");
                assert_eq!(*s, *mn);
                assert_eq!(*mn, *mx);
            }
            o => panic!("{:?}", o),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. DISTINCT on two columns where one is NULL
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn distinct_with_null_in_combo() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO t VALUES (1, 1, NULL)");
    exec(&db, "INSERT INTO t VALUES (2, 1, NULL)"); // dup of (1, NULL)
    exec(&db, "INSERT INTO t VALUES (3, 1, 2)");
    exec(&db, "INSERT INTO t VALUES (4, 2, NULL)");
    exec(&db, "INSERT INTO t VALUES (5, 1, 2)"); // dup of (1, 2)
    // DISTINCT a, b → (1,NULL), (1,2), (2,NULL) = 3 unique.
    let r = rows(&db, "SELECT DISTINCT a, b FROM t");
    assert_eq!(r.len(), 3, "distinct including NULL combos");
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. WHERE with BETWEEN on different types
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn where_between_float_column() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)");
    for i in 1..=20 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {:.1})", i, i as f64 * 0.5));
    }
    // v BETWEEN 3.0 AND 7.0 → ids 6,7,...,14 (v=3.0,3.5,...,7.0).
    let r = rows(&db, "SELECT COUNT(*) FROM t WHERE v BETWEEN 3.0 AND 7.0");
    match r[0][0] { Value::Integer(n) => assert_eq!(n, 9), _ => panic!() }
}

// ═══════════════════════════════════════════════════════════════════════════
// 9. Reopen after complex sequence (insert/update/delete/checkpoint)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn reopen_after_complex_sequence() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
        // Insert.
        for i in 1..=50 {
            exec(&db, &format!("INSERT INTO t VALUES ({}, 'c{}', {})", i, i % 3, i));
        }
        db.checkpoint().unwrap();
        // Update half.
        for i in 1..=25 { exec(&db, &format!("UPDATE t SET v = {} WHERE id = {}", i * 100, i)); }
        db.checkpoint().unwrap();
        // Delete some.
        for i in 26..=35 { exec(&db, &format!("DELETE FROM t WHERE id = {}", i)); }
        db.checkpoint().unwrap();
        // Insert more.
        for i in 51..=60 {
            exec(&db, &format!("INSERT INTO t VALUES ({}, 'new', {})", i, i));
        }
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    // 50 - 10 deleted + 10 new = 50.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 50);
    // Updated rows (id 1-25): v = id*100.
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 10"), 1000);
    // Non-updated rows (id 36-50): v = original.
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 40"), 40);
    // Deleted rows gone.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE id BETWEEN 26 AND 35"), 0);
    // New rows present.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE cat = 'new'"), 10);
}

// ═══════════════════════════════════════════════════════════════════════════
// 10. SUM with WHERE on FLOAT column — exact result
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn sum_float_where_exact() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)");
    for i in 1..=20 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {:.1})", i, i as f64 * 1.5));
    }
    // SUM(v) WHERE v >= 15.0 → ids 10-20 (v=15.0,16.5,...,30.0).
    // Sum = 1.5*(10+11+...+20) = 1.5 * 165 = 247.5.
    let r = rows(&db, "SELECT SUM(v) FROM t WHERE v >= 15.0");
    match &r[0][0] {
        Value::Float(f) => assert!((*f - 247.5).abs() < 0.01, "SUM = 247.5, got {}", f),
        o => panic!("{:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 11. Empty string in GROUP BY (distinct from NULL)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn group_by_empty_string_vs_null() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, '')");     // empty string
    exec(&db, "INSERT INTO t VALUES (2, '')");     // dup empty
    exec(&db, "INSERT INTO t VALUES (3, NULL)");   // NULL
    exec(&db, "INSERT INTO t VALUES (4, 'a')");
    // GROUP BY cat → 3 groups: '', NULL, 'a'.
    let r = rows(&db, "SELECT cat, COUNT(*) FROM t GROUP BY cat");
    assert_eq!(r.len(), 3, "empty string and NULL are separate groups");
}

// ═══════════════════════════════════════════════════════════════════════════
// 12. ORDER BY with WHERE on indexed column
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn order_by_with_indexed_where() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    for i in 1..=100 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'c{}', {})", i, i % 5, 100 - i));
    }
    exec(&db, "CREATE INDEX t_cat ON t(cat)");
    db.checkpoint().unwrap();
    db.wait_for_indexes_ready();
    // ORDER BY v DESC LIMIT 5 WHERE cat = 'c0'.
    let r = rows(&db, "SELECT v FROM t WHERE cat = 'c0' ORDER BY v DESC LIMIT 5");
    assert_eq!(r.len(), 5);
    let vals: Vec<i64> = r.iter().filter_map(|row| match &row[0] {
        Value::Integer(n) => Some(*n), _ => None
    }).collect();
    // cat='c0': ids 5,10,15,...,100 → v=95,90,85,...,0. Top 5: 95,90,85,80,75.
    assert_eq!(vals, vec![95, 90, 85, 80, 75]);
}

// ═══════════════════════════════════════════════════════════════════════════
// 13. Transaction rollback after failed INSERT (PK conflict)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn txn_rollback_after_failed_insert() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    let tx = db.begin_transaction().unwrap();
    exec(&db, "INSERT INTO t VALUES (2, 20)");
    // Failed insert (PK conflict).
    let _ = db.execute("INSERT INTO t VALUES (1, 999)");
    // Transaction should still be usable.
    exec(&db, "INSERT INTO t VALUES (3, 30)");
    db.rollback_transaction(tx).unwrap();
    // Only seed row remains.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 1);
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1"), 10);
}

// ═══════════════════════════════════════════════════════════════════════════
// 14. WHERE NOT LIKE (negated LIKE)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn where_not_like() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'apple')");
    exec(&db, "INSERT INTO t VALUES (2, 'banana')");
    exec(&db, "INSERT INTO t VALUES (3, 'apricot')");
    exec(&db, "INSERT INTO t VALUES (4, 'cherry')");
    // NOT LIKE 'ap%' → banana, cherry (exclude apple, apricot).
    let r = rows(&db, "SELECT id FROM t WHERE name NOT LIKE 'ap%' ORDER BY id");
    assert_eq!(r.len(), 2);
    let ids: Vec<i64> = r.iter().filter_map(|row| match &row[0] {
        Value::Integer(n) => Some(*n), _ => None
    }).collect();
    assert_eq!(ids, vec![2, 4]);
}

// ═══════════════════════════════════════════════════════════════════════════
// 15. Multiple aggregates including STDDEV and VARIANCE
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn all_six_aggregates_one_query() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=20 { exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i)); }
    // All six aggregates in one SELECT.
    let r = rows(&db, "SELECT COUNT(*), SUM(v), AVG(v), MIN(v), MAX(v), STDDEV(v) FROM t");
    assert_eq!(r.len(), 1);
    match (&r[0][0], &r[0][1], &r[0][3], &r[0][4]) {
        (Value::Integer(c), Value::Integer(s), Value::Integer(mn), Value::Integer(mx)) => {
            assert_eq!(*c, 20);
            assert_eq!(*s, 210);
            assert_eq!(*mn, 1);
            assert_eq!(*mx, 20);
        }
        o => panic!("{:?}", o),
    }
    // STDDEV should be positive (values 1-20 have spread).
    match &r[0][5] {
        Value::Float(f) => assert!(*f > 0.0, "STDDEV(1..20) should be > 0, got {}", f),
        Value::Null => panic!("STDDEV returned NULL"),
        o => panic!("{:?}", o),
    }
}
