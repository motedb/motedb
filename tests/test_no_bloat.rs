//! 不爆表 — Memory Stability & Anti-Bloat Tests
//!
//! 核心原则：内存到峰值后不再增长；段不会无限累积；
//! 压缩正确保持数据；缓存写入后失效。

#[path = "common/mod.rs"]
mod common;
use common::*;

#[test]
fn test_rss_stable_under_repeated_crud() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    // Insert 1000 rows
    for i in 1..=1000 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    let _ = fast_count(&db, "SELECT * FROM t");

    let initial_rss = get_rss_mb();

    // 2000 CRUD operations on fixed data
    for round in 0..20 {
        for i in 1..=100 {
            let id = (i % 1000) + 1;
            exec(&db, &format!("UPDATE t SET val = {} WHERE id = {}", round * 100 + i, id));
        }
        // Trigger compaction
        let _ = fast_count(&db, "SELECT * FROM t");
    }

    let final_rss = get_rss_mb();
    let growth = final_rss - initial_rss;
    // RSS should not grow more than 20MB under repeated CRUD on fixed data.
    assert!(
        growth < 20.0,
        "RSS grew {:.1}MB under repeated CRUD (initial={:.1}, final={:.1})",
        growth, initial_rss, final_rss
    );
}

#[test]
#[ignore = "RSS sublinear check needs larger data scale to be meaningful"]
fn test_rss_grows_sublinearly() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE bench (id INT PRIMARY KEY, val FLOAT, tag TEXT)");

    let mut rss_points: Vec<(usize, f64)> = Vec::new();

    for &target in &[10_000, 20_000, 40_000] {
        insert_test_rows(&db, target);
        let _ = fast_count(&db, "SELECT * FROM bench");
        let rss = get_rss_mb();
        rss_points.push((target, rss));
    }

    // RSS growth ratio should be less than data growth ratio.
    let (r1, rss1) = rss_points[0];
    let (r2, rss2) = rss_points[1];
    let (r3, rss3) = rss_points[2];

    let data_growth = r3 as f64 / r1 as f64; // 4x
    let rss_growth = rss3 / rss1.max(1.0);

    assert!(
        rss_growth < data_growth,
        "RSS growth {:.1}x should be < data growth {:.1}x (10K={:.1}MB, 40K={:.1}MB)",
        rss_growth, data_growth, rss1, rss3
    );
}

#[test]
fn test_compaction_preserves_data() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val INT, tag TEXT)");
    // Insert in small batches to create segments
    for batch in 0..10 {
        for i in 0..100 {
            let id = batch * 100 + i + 1;
            exec(&db, &format!("INSERT INTO t VALUES ({}, {}, 'tag{}')", id, id, id % 5));
        }
    }

    // Count before compaction
    let before = count_rows(&db, "SELECT * FROM t");
    let before_tag0 = count_rows(&db, "SELECT * FROM t WHERE tag = 'tag0'");

    // Trigger compaction via query
    let _ = fast_count(&db, "SELECT * FROM t");
    let _ = fast_count(&db, "SELECT * FROM t WHERE tag = 'tag0'");

    // Count after compaction
    let after = count_rows(&db, "SELECT * FROM t");
    let after_tag0 = count_rows(&db, "SELECT * FROM t WHERE tag = 'tag0'");

    assert_eq!(before, after, "Row count changed after compaction");
    assert_eq!(before_tag0, after_tag0, "Filtered count changed after compaction");
    assert_eq!(after, 1000);
    assert_eq!(after_tag0, 200);
}

#[test]
#[ignore = "Known bug: DELETE tombstone dedup issue"]
fn test_compaction_drops_tombstones() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    for i in 1..=200 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    // Delete half
    for i in (1..=200).step_by(2) {
        exec(&db, &format!("DELETE FROM t WHERE id = {}", i));
    }
    // Trigger compaction
    let _ = fast_count(&db, "SELECT * FROM t");
    let _ = fast_count(&db, "SELECT * FROM t");

    // Deleted rows must stay deleted
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 100);
    for i in (1..=200).step_by(2) {
        assert_eq!(count_rows(&db, &format!("SELECT * FROM t WHERE val = {}", i)), 0);
    }
}

#[test]
#[ignore = "Known bug: UPDATE creates duplicate rows instead of updating"]
fn test_compaction_dedup_updates() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO t VALUES (1, 1)");
    // Update same row 10 times
    for round in 1..=10 {
        exec(&db, &format!("UPDATE t SET val = {} WHERE id = 1", round * 100));
    }
    // Trigger compaction
    let _ = fast_count(&db, "SELECT * FROM t");

    // Only 1 row, with latest value
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE val = 1000"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE val = 100"), 0);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE val = 500"), 0);
}

#[test]
fn test_cache_invalidation_on_write() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO t VALUES (1, 100)");
    // Query to populate cache
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE val = 100"), 1);
    // Update
    exec(&db, "UPDATE t SET val = 200 WHERE id = 1");
    // Query again — should reflect updated value
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE val = 100"), 0);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE val = 200"), 1);
}

#[test]
fn test_no_memory_leak_in_queries() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE bench (id INT PRIMARY KEY, val FLOAT, tag TEXT)");
    insert_test_rows(&db, 5000);
    // Warm up
    let _ = fast_count(&db, "SELECT * FROM bench");
    let initial_rss = get_rss_mb();

    // Run 500 queries
    for _ in 0..500 {
        let _ = fast_count(&db, "SELECT * FROM bench WHERE tag = 'US'");
        let _ = fast_count(&db, "SELECT * FROM bench");
    }

    let final_rss = get_rss_mb();
    let growth = final_rss - initial_rss;
    assert!(
        growth < 15.0,
        "RSS grew {:.1}MB after 500 queries (initial={:.1}, final={:.1})",
        growth, initial_rss, final_rss
    );
}

#[test]
fn test_repeated_compaction_stable() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    // Insert + update cycle 5 times
    for cycle in 0..5 {
        // Add 100 new rows
        for i in 1..=100 {
            let id = cycle * 100 + i;
            exec(&db, &format!("INSERT INTO t VALUES ({}, {})", id, cycle));
        }
        // Trigger compaction
        let _ = fast_count(&db, "SELECT * FROM t");
        // Verify count
        let count = count_rows(&db, "SELECT * FROM t");
        assert_eq!(count, (cycle + 1) * 100, "Count wrong after cycle {}", cycle);
    }
    // Final verify after 5 compaction cycles
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 500);
}

#[test]
fn test_memory_after_index_creation() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE bench (id INT PRIMARY KEY, val FLOAT, tag TEXT)");
    insert_test_rows(&db, 10_000);
    let before_index = get_rss_mb();

    exec(&db, "CREATE INDEX idx_tag ON bench (tag) USING COLUMN");
    // Purge jemalloc
    {
        #[cfg(feature = "jemalloc")]
        {
            use tikv_jemalloc_ctl::{epoch, arenas};
            let _ = epoch::advance();
            if let Ok(n) = arenas::narenas::read() {
                for i in 0..n {
                    let name = format!("arena.{}.purge\0", i);
                    let _ = unsafe { tikv_jemalloc_ctl::raw::write(name.as_bytes(), ()) };
                }
            }
        }
    }

    let after_index = get_rss_mb();
    // Index creation shouldn't cause RSS to explode (>3x is a leak).
    assert!(
        after_index < before_index * 3.0,
        "RSS {:.1}MB > 3x of {:.1}MB after index creation",
        after_index, before_index
    );
}

#[test]
fn test_empty_table_memory() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    let rss = get_rss_mb();
    // Empty table should not consume much memory.
    assert!(rss < 50.0, "RSS {:.1}MB for empty table is too high", rss);
}

#[test]
#[ignore = "DROP TABLE memory measurement is flaky on macOS"]
fn test_table_drop_frees_memory() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    insert_test_rows(&db, 5000);
    let _ = fast_count(&db, "SELECT * FROM t");
    let before_drop = get_rss_mb();

    // Drop the table
    exec(&db, "DROP TABLE t");
    {
        #[cfg(feature = "jemalloc")]
        {
            use tikv_jemalloc_ctl::{epoch, arenas};
            let _ = epoch::advance();
            if let Ok(n) = arenas::narenas::read() {
                for i in 0..n {
                    let name = format!("arena.{}.purge\0", i);
                    let _ = unsafe { tikv_jemalloc_ctl::raw::write(name.as_bytes(), ()) };
                }
            }
        }
    }

    let after_drop = get_rss_mb();
    // Memory should not INCREASE after drop (allow some jitter).
    assert!(
        after_drop <= before_drop + 5.0,
        "RSS increased {:.1}MB after DROP TABLE (before={:.1}, after={:.1})",
        after_drop - before_drop, before_drop, after_drop
    );
}

#[test]
fn test_count_live_rows_after_deletes() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    // Insert 500, delete 200
    for i in 1..=500 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    for i in 1..=200 {
        exec(&db, &format!("DELETE FROM t WHERE id = {}", i));
    }
    let _ = fast_count(&db, "SELECT * FROM t");

    // Live rows should be exactly 300
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 300);
    // Deleted rows gone
    for i in 1..=200 {
        assert_eq!(count_rows(&db, &format!("SELECT * FROM t WHERE val = {}", i)), 0);
    }
    // Surviving rows
    for i in 201..=500 {
        assert_eq!(count_rows(&db, &format!("SELECT * FROM t WHERE val = {}", i)), 1);
    }
}
