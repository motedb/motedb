//! 不丢数据 — Durability & Crash Recovery Tests
//!
//! 核心原则：任何写操作后重启数据库，已提交的数据必须完整无损。
//! 覆盖 INSERT/UPDATE/DELETE/事务/批量写入/段恢复等场景。
//!
//! 注意：所有 crash-recovery 测试在 drop(db) 前调用 db.flush()，
//! 确保缓冲区的写入和 tombstone 持久化到磁盘。

#[path = "common/mod.rs"]
mod common;
use common::*;
use motedb::Database;

/// Flush + drop, simulating a clean checkpoint before crash.
fn flush_and_drop(db: Database) {
    let _ = db.flush();
    drop(db);
}

// ─── INSERT 持久性 ──────────────────────────────────────────────────────

#[test]
fn test_insert_survives_crash() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = create_db_at(&path);
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)");
        exec(&db, "INSERT INTO t VALUES (1, 'Alice')");
        exec(&db, "INSERT INTO t VALUES (2, 'Bob')");
        exec(&db, "INSERT INTO t VALUES (3, 'Charlie')");
        flush_and_drop(db);
    }
    let db = open_db_at(&path);
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 3);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE name = 'Bob'"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE name = 'Alice'"), 1);
}

#[test]
fn test_batch_insert_survives_crash() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = create_db_at(&path);
        exec(&db, "CREATE TABLE bench (id INT PRIMARY KEY, val FLOAT, tag TEXT)");
        insert_test_rows(&db, 1000);
        flush_and_drop(db);
    }
    let db = open_db_at(&path);
    assert_eq!(count_rows(&db, "SELECT * FROM bench"), 1000);
    assert_eq!(count_rows(&db, "SELECT * FROM bench WHERE tag = 'US'"), 334);
}

#[test]
#[ignore = "Flaky under parallel test load — passes in isolation"]
fn test_update_survives_crash() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = create_db_at(&path);
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val FLOAT)");
        exec(&db, "INSERT INTO t VALUES (1, 10.0)");
        exec(&db, "INSERT INTO t VALUES (2, 20.0)");
        exec(&db, "UPDATE t SET val = 999.0 WHERE id = 1");
        flush_and_drop(db);
    }
    let db = open_db_at(&path);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE val = 999.0"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE val = 10.0"), 0);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE val = 20.0"), 1);
}

#[test]
fn test_delete_survives_crash() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = create_db_at(&path);
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)");
        exec(&db, "INSERT INTO t VALUES (1, 'A')");
        exec(&db, "INSERT INTO t VALUES (2, 'B')");
        exec(&db, "INSERT INTO t VALUES (3, 'C')");
        exec(&db, "DELETE FROM t WHERE id = 2");
        // Trigger query so tombstone is flushed to a segment
        let _ = fast_count(&db, "SELECT * FROM t");
        flush_and_drop(db);
    }
    let db = open_db_at(&path);
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 2);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE name = 'A'"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE name = 'C'"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE name = 'B'"), 0);
}

#[test]
fn test_mixed_crud_survives_crash() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = create_db_at(&path);
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
        for i in 1..=10 {
            exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i * 10));
        }
        exec(&db, "UPDATE t SET val = 999 WHERE id = 5");
        exec(&db, "UPDATE t SET val = 888 WHERE id = 7");
        exec(&db, "DELETE FROM t WHERE id = 3");
        exec(&db, "DELETE FROM t WHERE id = 8");
        exec(&db, "INSERT INTO t VALUES (11, 110)");
        exec(&db, "INSERT INTO t VALUES (12, 120)");
        // Trigger flush of pending writes
        let _ = fast_count(&db, "SELECT * FROM t");
        flush_and_drop(db);
    }
    let db = open_db_at(&path);
    // 10 - 2 deleted + 2 inserted = 10
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 10);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE val = 999"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE val = 888"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE val = 110"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE val = 120"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE val = 30"), 0);
}

// ─── 事务持久性 ─────────────────────────────────────────────────────────

#[test]
fn test_transaction_commit_survives_crash() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = create_db_at(&path);
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)");
        exec(&db, "INSERT INTO t VALUES (1, 'old')");
        exec(&db, "BEGIN");
        exec(&db, "INSERT INTO t VALUES (2, 'txn_data')");
        exec(&db, "UPDATE t SET val = 'updated' WHERE id = 1");
        exec(&db, "COMMIT");
        // Trigger flush
        let _ = fast_count(&db, "SELECT * FROM t");
        flush_and_drop(db);
    }
    let db = open_db_at(&path);
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 2);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE val = 'txn_data'"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE val = 'updated'"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE val = 'old'"), 0);
}

#[test]
#[ignore = "Known bug: uncommitted txn data visible after restart"]
fn test_transaction_rollback_lost_on_crash() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = create_db_at(&path);
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)");
        exec(&db, "INSERT INTO t VALUES (1, 'committed')");
        let _ = fast_count(&db, "SELECT * FROM t");
        flush_and_drop(db);
    }
    // Phase 2: start uncommitted txn, then crash (drop without flush)
    {
        let db = open_db_at(&path);
        exec(&db, "BEGIN");
        exec(&db, "INSERT INTO t VALUES (2, 'uncommitted')");
        // No flush — simulate crash
        drop(db);
    }
    let db = open_db_at(&path);
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE val = 'committed'"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE val = 'uncommitted'"), 0);
}

// ─── 压缩 & 段恢复 ──────────────────────────────────────────────────────

#[test]
fn test_tombstone_survives_compaction() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    for i in 1..=100 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    for i in (1..=100).step_by(2) {
        exec(&db, &format!("DELETE FROM t WHERE id = {}", i));
    }
    // Trigger compaction via query
    let _ = fast_count(&db, "SELECT * FROM t");
    // Verify deletions honored
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 50);
    for i in (1..=100).step_by(2) {
        assert_eq!(count_rows(&db, &format!("SELECT * FROM t WHERE val = {}", i)), 0);
    }
    for i in (2..=100).step_by(2) {
        assert_eq!(count_rows(&db, &format!("SELECT * FROM t WHERE val = {}", i)), 1);
    }
}

#[test]
fn test_update_survives_compaction() {
    let (_dir, db) = setup_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    for i in 1..=50 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i * 10));
    }
    // Update all rows multiple times
    for round in 0..5 {
        for i in 1..=50 {
            exec(&db, &format!("UPDATE t SET val = {} WHERE id = {}", round * 1000 + i, i));
        }
    }
    // Trigger compaction via query
    let _ = fast_count(&db, "SELECT * FROM t");
    // Latest values must survive (round 4: 4000+id)
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 50);
    for i in 1..=50 {
        let expected = 4000 + i;
        assert_eq!(
            count_rows(&db, &format!("SELECT * FROM t WHERE val = {}", expected)),
            1,
            "id={} should have val={}", i, expected
        );
    }
}

#[test]
fn test_repeated_open_close() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    // Cycle 1: create + insert
    {
        let db = create_db_at(&path);
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
        exec(&db, "INSERT INTO t VALUES (1, 10)");
        flush_and_drop(db);
    }
    // Cycle 2
    {
        let db = open_db_at(&path);
        exec(&db, "INSERT INTO t VALUES (2, 20)");
        assert_eq!(count_rows(&db, "SELECT * FROM t"), 2);
        flush_and_drop(db);
    }
    // Cycle 3
    {
        let db = open_db_at(&path);
        exec(&db, "INSERT INTO t VALUES (3, 30)");
        assert_eq!(count_rows(&db, "SELECT * FROM t"), 3);
        flush_and_drop(db);
    }
    // Final verify
    let db = open_db_at(&path);
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 3);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE v = 10"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE v = 20"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE v = 30"), 1);
}

#[test]
#[ignore = "WAL recovery gap: large-batch data half-lost after reopen in normal test order"]
fn test_large_batch_durability() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = create_db_at(&path);
        exec(&db, "CREATE TABLE bench (id INT PRIMARY KEY, val FLOAT, tag TEXT)");
        insert_test_rows(&db, 10_000);
        flush_and_drop(db);
    }
    let db = open_db_at(&path);
    assert_eq!(count_rows(&db, "SELECT * FROM bench"), 10_000);
    assert_eq!(count_rows(&db, "SELECT * FROM bench WHERE tag = 'US'"), 3_334);
}

#[test]
fn test_auto_increment_persistence() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = create_db_at(&path);
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT)");
        exec(&db, "INSERT INTO t (name) VALUES ('A')");
        exec(&db, "INSERT INTO t (name) VALUES ('B')");
        exec(&db, "INSERT INTO t (name) VALUES ('C')");
        flush_and_drop(db);
    }
    let db = open_db_at(&path);
    exec(&db, "INSERT INTO t (name) VALUES ('D')");
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 4);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE name = 'D'"), 1);
}

#[test]
#[ignore = "Flaky in parallel test runs — passes when run alone"]
fn test_checkpoint_durability() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = create_db_at(&path);
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
        for i in 1..=100 {
            exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
        }
        let _ = db.checkpoint();
        for i in 101..=200 {
            exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
        }
        flush_and_drop(db);
    }
    let db = open_db_at(&path);
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 200);
}

#[test]
#[ignore = "Known bug: concurrent write durability across restart"]
fn test_concurrent_writes_durability() {
    use std::sync::Arc;
    use std::thread;

    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = Arc::new(create_db_at(&path));
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, thread_id INT)");
        let handles: Vec<_> = (0..4)
            .map(|tid| {
                let db = db.clone();
                thread::spawn(move || {
                    for i in 0..250 {
                        let id = tid * 250 + i + 1;
                        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", id, tid));
                    }
                })
            })
            .collect();
        for h in handles { h.join().unwrap(); }
        // Flush before drop
        let _ = db.flush();
        drop(db);
    }
    let db = open_db_at(&path);
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 1000);
    for tid in 0..4 {
        assert_eq!(count_rows(&db, &format!("SELECT * FROM t WHERE thread_id = {}", tid)), 250);
    }
}

#[test]
fn test_null_value_durability() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = create_db_at(&path);
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, val FLOAT)");
        exec(&db, "INSERT INTO t VALUES (1, NULL, NULL)");
        exec(&db, "INSERT INTO t VALUES (2, 'hello', 3.14)");
        exec(&db, "INSERT INTO t VALUES (3, NULL, 42.0)");
        flush_and_drop(db);
    }
    let db = open_db_at(&path);
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 3);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE name = 'hello'"), 1);
}

#[test]
fn test_multi_segment_recovery() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = create_db_at(&path);
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
        // Write in batches to create multiple segments
        for batch in 0..20 {
            for i in 0..50 {
                let id = batch * 50 + i + 1;
                exec(&db, &format!("INSERT INTO t VALUES ({}, {})", id, id * 2));
            }
        }
        flush_and_drop(db);
    }
    let db = open_db_at(&path);
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 1000);
}
