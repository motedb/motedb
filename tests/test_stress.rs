//! 压力测试 — Stress & Scale Tests
//!
//! 核心原则：大规模数据下系统稳定运行。

#[path = "common/mod.rs"]
mod common;
use common::*;

#[test]
fn test_10k_insert_consistency() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE bench (id INT PRIMARY KEY, val FLOAT, tag TEXT)");
    insert_test_rows(&db, 10_000);

    // Exact count verification
    assert_eq!(count_rows(&db, "SELECT * FROM bench"), 10_000);
    // Distribution: every 3rd row is 'US' (0, 3, 6, ...)
    // 10_000 / 3 = 3333 + 1 (id 0) = 3334
    let us_count = count_rows(&db, "SELECT * FROM bench WHERE tag = 'US'");
    assert!(us_count > 3000 && us_count < 4000, "US count {} unexpected", us_count);
    let eu_count = count_rows(&db, "SELECT * FROM bench WHERE tag = 'EU'");
    assert_eq!(us_count + eu_count, 10_000);
}

#[test]
#[ignore = "Known bug: rapid DELETE+INSERT creates segment bloat"]
fn test_rapid_insert_delete_cycle() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO t VALUES (1, 100)");

    // Rapidly insert and delete the same row 500 times
    for _ in 0..500 {
        exec(&db, "INSERT INTO t VALUES (2, 200)");
        exec(&db, "DELETE FROM t WHERE id = 2");
    }

    // Should have exactly 1 row (original)
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE val = 100"), 1);
}

#[test]
#[ignore = "Known bug: rapid UPDATE creates duplicate rows"]
fn test_rapid_update_same_row() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO t VALUES (1, 0)");

    // Update same row 500 times
    for i in 1..=500 {
        exec(&db, &format!("UPDATE t SET val = {} WHERE id = 1", i));
    }

    // Row should exist with latest value
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 1);
}

#[test]
fn test_many_small_batches() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");

    // Insert 1000 rows one at a time (worst case for segment creation)
    for i in 1..=1000 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }

    assert_eq!(count_rows(&db, "SELECT * FROM t"), 1000);
    // Trigger query (compaction)
    let _ = fast_count(&db, "SELECT * FROM t");
    // Data must survive compaction
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 1000);
}

#[test]
fn test_large_result_set() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE bench (id INT PRIMARY KEY, val FLOAT, tag TEXT)");
    insert_test_rows(&db, 50_000);

    // Full table scan returns 50K rows
    let count = fast_count(&db, "SELECT * FROM bench");
    assert_eq!(count, 50_000);

    // WHERE returns ~1/3
    let filtered = fast_count(&db, "SELECT * FROM bench WHERE tag = 'US'");
    assert!(filtered > 15000 && filtered < 20000);
}

#[test]
fn test_mixed_read_write_stability() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE bench (id INT PRIMARY KEY, val FLOAT, tag TEXT)");
    insert_test_rows(&db, 5_000);

    // Interleave reads and writes
    for round in 0..10 {
        // Write
        exec(&db, &format!("INSERT INTO bench VALUES ({}, 0.0, 'NEW')", 900000 + round));
        // Read
        let _ = fast_count(&db, "SELECT * FROM bench WHERE tag = 'US'");
        // Read again
        let _ = fast_count(&db, "SELECT * FROM bench");
    }

    // Final count: original 5000 + 10 new
    assert_eq!(count_rows(&db, "SELECT * FROM bench"), 5_010);
}

#[test]
#[ignore = "CREATE INDEX after bulk insert: tag distribution issue"]
fn test_index_after_bulk_insert() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE bench (id INT PRIMARY KEY, val FLOAT, tag TEXT)");
    insert_test_rows(&db, 5_000);

    // Create index after data is inserted
    exec(&db, "CREATE INDEX idx_tag ON bench (tag) USING COLUMN");

    // Queries should work with index
    assert!(count_rows(&db, "SELECT * FROM bench WHERE tag = 'US'") > 0);
    assert_eq!(count_rows(&db, "SELECT DISTINCT tag FROM bench"), 2);
}

#[test]
#[ignore = "Long-running stress test — run with --ignored"]
fn test_50k_full_workload() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE bench (id INT PRIMARY KEY, val FLOAT, tag TEXT)");
    insert_test_rows(&db, 50_000);

    // Run all query types
    let _ = fast_count(&db, "SELECT * FROM bench");
    let _ = fast_count(&db, "SELECT * FROM bench WHERE tag = 'US'");
    let _ = fast_count(&db, "SELECT * FROM bench WHERE tag = 'US' LIMIT 10");
    let _ = db.execute("SELECT COUNT(*) FROM bench WHERE tag = 'US'");
    let _ = fast_count(&db, "SELECT DISTINCT tag FROM bench");
    let _ = db.execute("SELECT * FROM bench ORDER BY val DESC LIMIT 10");

    // Verify no data loss
    assert_eq!(count_rows(&db, "SELECT * FROM bench"), 50_000);
}
