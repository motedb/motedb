//! Bug-hunt v6: float aggregation precision, large dataset verification,
//! checkpoint/recovery stress, and data integrity after compaction.

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

fn scalar_f64(db: &Database, sql: &str) -> f64 {
    let r = rows(db, sql);
    assert_eq!(r.len(), 1);
    match r[0].first() { Some(Value::Float(n)) => *n, Some(Value::Integer(n)) => *n as f64, o => panic!("float? {:?}: {}", o, sql) }
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. Float aggregation precision
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn sum_float_precision() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)");
    for i in 1..=100 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 0.1)", i));
    }
    // SUM(0.1 × 100) should be close to 10.0 (float error acceptable).
    let s = scalar_f64(&db, "SELECT SUM(v) FROM t");
    assert!((s - 10.0).abs() < 0.01, "SUM(0.1*100) ≈ 10.0, got {}", s);
}

#[test]
fn avg_float_precision() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)");
    for i in 1..=10 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {:.3})", i, i as f64 * 1.5));
    }
    // AVG(1.5, 3.0, ..., 15.0) = 8.25
    let a = scalar_f64(&db, "SELECT AVG(v) FROM t");
    assert!((a - 8.25).abs() < 0.001, "AVG = 8.25, got {}", a);
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. Large dataset integrity (every row verifiable)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn large_dataset_each_row_correct() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=500 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i * 7));
    }
    // Verify EVERY row's value (not just count/sum).
    let mut mismatches = 0;
    for i in 1..=500 {
        let v = scalar_i64(&db, &format!("SELECT v FROM t WHERE id = {}", i));
        if v != i * 7 {
            mismatches += 1;
        }
    }
    assert_eq!(mismatches, 0, "{} rows had wrong values", mismatches);
}

#[test]
fn large_dataset_sum_matches_naive() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let mut expected_sum: i64 = 0;
    for i in 1..=1000 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
        expected_sum += i;
    }
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), expected_sum);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 1000);
    assert_eq!(scalar_i64(&db, "SELECT MIN(v) FROM t"), 1);
    assert_eq!(scalar_i64(&db, "SELECT MAX(v) FROM t"), 1000);
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. Checkpoint + reopen preserves everything
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn checkpoint_reopen_all_rows_intact() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT, s TEXT)");
        for i in 1..=200 {
            exec(&db, &format!("INSERT INTO t VALUES ({}, {}, 'row{}')", i, i * 3, i));
        }
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 200);
    // Spot-check several rows.
    for i in [1, 50, 100, 150, 200].iter() {
        let v = scalar_i64(&db, &format!("SELECT v FROM t WHERE id = {}", i));
        assert_eq!(v, i * 3, "row {} value mismatch after reopen", i);
    }
}

#[test]
fn multiple_checkpoints_no_data_loss() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for batch in 0..5 {
        for i in 1..=50 {
            let id = batch * 50 + i;
            exec(&db, &format!("INSERT INTO t VALUES ({}, {})", id, id));
        }
        db.checkpoint().unwrap();
    }
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 250);
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), 250 * 251 / 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. UPDATE then verify aggregate reflects new values
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn update_then_aggregate_reflects_change() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=10 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i * 10));
    }
    // Initial SUM = 10+20+...+100 = 550.
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), 550);
    // Update id=5 from 50 to 500 (delta +450).
    exec(&db, "UPDATE t SET v = 500 WHERE id = 5");
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), 1000, "SUM must reflect UPDATE");
    assert_eq!(scalar_i64(&db, "SELECT MAX(v) FROM t"), 500);
}

#[test]
fn delete_then_aggregate_reflects_removal() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=10 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), 55);
    // Delete id=5 (value 5).
    exec(&db, "DELETE FROM t WHERE id = 5");
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), 50, "SUM must reflect DELETE");
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 9);
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. GROUP BY correctness with many groups
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn group_by_many_groups_correct() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat INT, v INT)");
    for i in 1..=100 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {}, {})", i, i % 10, i));
    }
    // 10 groups (cat 0-9), each with 10 rows.
    let r = rows(&db, "SELECT cat, COUNT(*), SUM(v) FROM t GROUP BY cat ORDER BY cat");
    assert_eq!(r.len(), 10, "10 groups expected");
    // Each group has 10 rows.
    for row in &r {
        match (&row[0], &row[1]) {
            (Value::Integer(_cat), Value::Integer(cnt)) => assert_eq!(*cnt, 10),
            _ => panic!("wrong types"),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. Mixed type columns in one table
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn mixed_types_all_correct() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, i INT, f FLOAT, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 42, 3.14, 'hello')");
    exec(&db, "INSERT INTO t VALUES (2, -7, -2.5, 'world')");
    let r = rows(&db, "SELECT i, f, s FROM t WHERE id = 1");
    assert_eq!(r.len(), 1);
    match (&r[0][0], &r[0][1], &r[0][2]) {
        (Value::Integer(i), Value::Float(f), Value::Text(s)) => {
            assert_eq!(*i, 42);
            assert!((f - 3.14).abs() < 0.001);
            assert_eq!(&*s.0, "hello");
        }
        o => panic!("wrong types: {:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. WHERE with range on different types
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn where_range_on_float() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)");
    for i in 1..=10 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {:.1})", i, i as f64 * 1.5));
    }
    // v BETWEEN 3.0 AND 6.0 → values 3.0, 4.5, 6.0 → ids 2, 3, 4.
    let r = rows(&db, "SELECT id FROM t WHERE v BETWEEN 3.0 AND 6.0 ORDER BY id");
    assert_eq!(r.len(), 3);
}

#[test]
fn where_range_on_text() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    for (i, s) in [(1, "apple"), (2, "banana"), (3, "cherry"), (4, "date")].iter() {
        exec(&db, &format!("INSERT INTO t VALUES ({}, '{}')", i, s));
    }
    // Text BETWEEN is lexicographic. 'b' to 'd': 'banana' and 'cherry' match
    // ('b' ≤ s ≤ 'd'). 'date' does NOT match because 'date' > 'd' (longer
    // string sharing the prefix 'd' compares greater). 'apple' < 'b' excluded.
    let r = rows(&db, "SELECT id FROM t WHERE s BETWEEN 'b' AND 'd' ORDER BY id");
    assert_eq!(r.len(), 2, "banana + cherry match; date > 'd'");
    let r = rows(&db, "SELECT id FROM t WHERE s BETWEEN 'b' AND 'e' ORDER BY id");
    assert_eq!(r.len(), 3, "banana + cherry + date all ≤ 'e'");
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. Sequential PKs with gaps (after deletes)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn pk_with_gaps_correct() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=20 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    // Delete every other row.
    for i in (1..=20).step_by(2) {
        exec(&db, &format!("DELETE FROM t WHERE id = {}", i));
    }
    // Remaining: 2, 4, 6, ..., 20 = 10 rows.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 10);
    // Each even row still has correct value.
    for i in (2..=20).step_by(2) {
        assert_eq!(scalar_i64(&db, &format!("SELECT v FROM t WHERE id = {}", i)), i);
    }
    // Odd rows gone.
    for i in (1..=20).step_by(2) {
        assert_eq!(scalar_i64(&db, &format!("SELECT COUNT(*) FROM t WHERE id = {}", i)), 0);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 9. Concurrent reads during writes (interleaved on single thread)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn interleave_write_read_100_cycles() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for cycle in 0..100 {
        let id = cycle + 1;
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", id, id * 2));
        // Read back immediately.
        assert_eq!(scalar_i64(&db, &format!("SELECT v FROM t WHERE id = {}", id)), id * 2);
        assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), id);
    }
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 100);
}

// ═══════════════════════════════════════════════════════════════════════════
// 10. ORDER BY LIMIT picks correct rows
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn order_by_limit_top_5_correct() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    // Insert in random order of v.
    for (id, v) in [(1, 50), (2, 10), (3, 90), (4, 30), (5, 70), (6, 20), (7, 80), (8, 40), (9, 60), (10, 100)].iter() {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", id, v));
    }
    // Top 3 by v DESC → 100, 90, 80.
    let r = rows(&db, "SELECT v FROM t ORDER BY v DESC LIMIT 3");
    let vals: Vec<i64> = r.iter().filter_map(|row| match row.get(0) {
        Some(Value::Integer(n)) => Some(*n), _ => None
    }).collect();
    assert_eq!(vals, vec![100, 90, 80]);
}

// ═══════════════════════════════════════════════════════════════════════════
// 11. NULL sorting position
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn count_distinct_with_nulls() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "INSERT INTO t VALUES (2, 10)"); // dup
    exec(&db, "INSERT INTO t VALUES (3, NULL)");
    exec(&db, "INSERT INTO t VALUES (4, 20)");
    exec(&db, "INSERT INTO t VALUES (5, NULL)");
    // COUNT(DISTINCT v): distinct non-null values = {10, 20} = 2. NULLs excluded.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(DISTINCT v) FROM t"), 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// 12. Reopen with indexes preserved
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn reopen_indexed_queries_correct() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
        for i in 1..=100 {
            exec(&db, &format!("INSERT INTO t VALUES ({}, 'c{}', {})", i, i % 5, i));
        }
        exec(&db, "CREATE INDEX t_cat ON t(cat)");
        db.checkpoint().unwrap();
        db.wait_for_indexes_ready();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    db.wait_for_indexes_ready();
    // Indexed query must return correct count after reopen.
    let r = rows(&db, "SELECT COUNT(*) FROM t WHERE cat = 'c1'");
    match r[0][0] { Value::Integer(n) => assert_eq!(n, 20), _ => panic!() }
}
