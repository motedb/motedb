//! Bug-hunt v9: concurrent multi-thread stress, compaction correctness,
//! and recovery integrity under heavy mixed workloads.

use motedb::sql::QueryResult;
use motedb::types::Value;
use motedb::Database;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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
// 1. Concurrent reads + single writer (no data corruption)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn concurrent_reads_during_writes() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=200 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    db.checkpoint().unwrap();

    let db = Arc::new(db);
    let write_done = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();

    // Reader threads.
    for _ in 0..3 {
        let db = Arc::clone(&db);
        let wd = Arc::clone(&write_done);
        handles.push(std::thread::spawn(move || {
            let mut reads = 0u64;
            while wd.load(Ordering::Relaxed) == 0 {
                let r = db.execute("SELECT COUNT(*) FROM t").and_then(|r| r.materialize());
                if r.is_ok() { reads += 1; }
                if reads > 50 { break; }
            }
            reads
        }));
    }
    // Writer thread: add 50 more rows.
    let db2 = Arc::clone(&db);
    let wd2 = Arc::clone(&write_done);
    handles.push(std::thread::spawn(move || {
        for i in 201..=250 {
            let _ = db2.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i));
        }
        wd2.store(1, Ordering::Relaxed);
        0u64
    }));

    for h in handles { let _ = h.join(); }
    // After all threads done, count must be consistent (>= 200).
    let final_count = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    assert!(final_count >= 200, "count must be at least 200, got {}", final_count);
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. Compaction preserves all data
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn compaction_preserves_data() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    // Insert in chunks to create multiple segments.
    for chunk in 0..5 {
        for i in 1..=100 {
            let id = chunk * 100 + i;
            exec(&db, &format!("INSERT INTO t VALUES ({}, {})", id, id * 2));
        }
        db.checkpoint().unwrap();
    }
    // After multiple checkpoints (may trigger compaction), all 500 rows intact.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 500);
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), 500 * 501);
    // Spot-check rows from different chunks.
    for id in [1, 100, 101, 250, 350, 500].iter() {
        assert_eq!(scalar_i64(&db, &format!("SELECT v FROM t WHERE id = {}", id)), id * 2);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. Recovery after many updates + deletes
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn recovery_after_mixed_ops() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
        for i in 1..=100 {
            exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
        }
        // Update half.
        for i in 1..=50 {
            exec(&db, &format!("UPDATE t SET v = {} WHERE id = {}", i * 100, i));
        }
        // Delete a quarter.
        for i in (1..=25).step_by(1) {
            exec(&db, &format!("DELETE FROM t WHERE id = {}", i));
        }
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    // 100 - 25 deleted = 75 rows.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 75);
    // id=1..25 deleted, id=26..50 updated (v=id*100), id=51..100 original (v=id).
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE id <= 25"), 0);
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 30"), 3000); // updated
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 60"), 60);   // original
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. Reopen multiple times preserves cumulative state
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn reopen_preserves_cumulative_state() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    for cycle in 1..=5 {
        let db = Database::open(&path).unwrap_or_else(|_| {
            // First cycle: create.
            Database::create(&path).unwrap()
        });
        if cycle == 1 {
            exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
        }
        // Each cycle adds 20 rows starting at (cycle-1)*20+1.
        for i in 1..=20 {
            let id = (cycle - 1) * 20 + i;
            exec(&db, &format!("INSERT INTO t VALUES ({}, {})", id, cycle));
        }
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 100, "5 cycles × 20 rows");
    // Each cycle's rows have v=cycle.
    for cycle in 1..=5 {
        let cnt = scalar_i64(&db, &format!("SELECT COUNT(*) FROM t WHERE v = {}", cycle));
        assert_eq!(cnt, 20, "cycle {} should have 20 rows", cycle);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. Index consistency after heavy writes
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn index_consistent_after_writes() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    for i in 1..=200 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'c{}', {})", i, i % 5, i));
    }
    exec(&db, "CREATE INDEX t_cat ON t(cat)");
    db.checkpoint().unwrap();
    db.wait_for_indexes_ready();
    // Delete some rows.
    for i in 1..=50 {
        exec(&db, &format!("DELETE FROM t WHERE id = {}", i));
    }
    // Index query must reflect deletes.
    let r = rows(&db, "SELECT COUNT(*) FROM t WHERE cat = 'c0'");
    match r[0][0] {
        Value::Integer(n) => {
            // Originally cat='c0' had ids 5,10,...,200 = 40 rows. Deleted ids 5,10,...,50 = 10.
            // So 40 - 10 = 30.
            assert_eq!(n, 30, "index must reflect deletes: {}", n);
        }
        _ => panic!(),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. SUM correctness after updates (no stale cache)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn sum_correct_after_repeated_updates() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=10 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, 10));
    }
    // Initial SUM = 100.
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), 100);
    // Update id=1 five times, incrementing by 1 each.
    for v in 11..=15 {
        exec(&db, &format!("UPDATE t SET v = {} WHERE id = 1", v));
        // SUM must reflect the latest value, not a stale one.
        let expected = 90 + v; // 9 rows × 10 + current v.
        assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), expected, "after v={}", v);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. GROUP BY after checkpoint (multi-segment)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn group_by_after_checkpoint_correct() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    // Insert in 3 batches with checkpoints → multiple segments.
    for batch in 0..3 {
        for i in 1..=50 {
            let id = batch * 50 + i;
            exec(&db, &format!("INSERT INTO t VALUES ({}, 'g{}', {})", id, id % 3, id));
        }
        db.checkpoint().unwrap();
    }
    // GROUP BY cat across all segments.
    let r = rows(&db, "SELECT cat, COUNT(*) FROM t GROUP BY cat ORDER BY cat");
    // 3 groups (g0, g1, g2), each with 50 rows.
    assert_eq!(r.len(), 3);
    for row in &r {
        match &row[1] {
            Value::Integer(cnt) => assert_eq!(*cnt, 50, "each group has 50 rows"),
            _ => panic!(),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. ORDER BY across segments
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn order_by_across_segments() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    // Insert in reverse order across batches.
    for batch in 0..3 {
        for i in (1..=50).rev() {
            let id = batch * 50 + i;
            exec(&db, &format!("INSERT INTO t VALUES ({}, {})", id, 200 - id));
        }
        db.checkpoint().unwrap();
    }
    // ORDER BY v ASC — v goes from 200-150=50 down to 200-1=199 reversed → 50,51,...,199.
    let r = rows(&db, "SELECT v FROM t ORDER BY v ASC LIMIT 5");
    let vals: Vec<i64> = r.iter().filter_map(|row| match row.get(0) {
        Some(Value::Integer(n)) => Some(*n), _ => None
    }).collect();
    assert_eq!(vals, vec![50, 51, 52, 53, 54]);
}

// ═══════════════════════════════════════════════════════════════════════════
// 9. DISTINCT across segments
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn distinct_across_segments() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT)");
    for batch in 0..3 {
        for i in 1..=50 {
            let id = batch * 50 + i;
            exec(&db, &format!("INSERT INTO t VALUES ({}, 'cat{}')", id, id % 5));
        }
        db.checkpoint().unwrap();
    }
    // DISTINCT cat across all segments → 5 unique values.
    let r = rows(&db, "SELECT DISTINCT cat FROM t ORDER BY cat");
    assert_eq!(r.len(), 5);
}

// ═══════════════════════════════════════════════════════════════════════════
// 10. Transaction across checkpoint boundary
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn txn_survives_checkpoint() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    let tx = db.begin_transaction().unwrap();
    exec(&db, "INSERT INTO t VALUES (2, 20)");
    // Checkpoint during transaction (shouldn't commit the uncommitted row).
    db.checkpoint().unwrap();
    exec(&db, "INSERT INTO t VALUES (3, 30)");
    db.commit_transaction(tx).unwrap();
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 3);
}

// ═══════════════════════════════════════════════════════════════════════════
// 11. Large table full scan performance sanity (correctness over speed)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn full_scan_large_table_all_rows_present() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=2000 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    db.checkpoint().unwrap();
    // Full scan must return ALL 2000 rows (not a subset).
    let r = rows(&db, "SELECT id FROM t ORDER BY id");
    assert_eq!(r.len(), 2000, "full scan must return all rows");
    // First and last must be 1 and 2000.
    match (&r[0][0], &r[1999][0]) {
        (Value::Integer(first), Value::Integer(last)) => {
            assert_eq!(*first, 1);
            assert_eq!(*last, 2000);
        }
        o => panic!("{:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 12. Empty GROUP BY result after filtering all out
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn group_by_empty_after_filter() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 'a', 10)");
    exec(&db, "INSERT INTO t VALUES (2, 'b', 20)");
    // WHERE filters all → GROUP BY returns 0 groups.
    let r = rows(&db, "SELECT cat, COUNT(*) FROM t WHERE v > 1000 GROUP BY cat");
    assert_eq!(r.len(), 0, "no matching rows → no groups");
}

// ═══════════════════════════════════════════════════════════════════════════
// 13. Multiple tables JOIN then aggregate
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn join_count_correct() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY, val INT)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY, a_id INT)");
    for i in 1..=50 {
        exec(&db, &format!("INSERT INTO a VALUES ({}, {})", i, i * 10));
    }
    for i in 1..=50 {
        // Each 'a' row has 2 matching 'b' rows.
        exec(&db, &format!("INSERT INTO b VALUES ({}, {})", i * 2 - 1, i));
        exec(&db, &format!("INSERT INTO b VALUES ({}, {})", i * 2, i));
    }
    // INNER JOIN a-b → 100 rows (50 a × 2 b each).
    let r = rows(&db, "SELECT COUNT(*) FROM a INNER JOIN b ON a.id = b.a_id");
    match r[0][0] {
        Value::Integer(n) => assert_eq!(n, 100, "join should produce 100 rows"),
        _ => panic!(),
    }
}
