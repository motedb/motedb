//! DELETE 持久性测试 —— 验证优化 #2（DELETE 延迟 flush）的正确性。
//!
//! 优化前：每行 DELETE 都 flush_buffer（tombstone 立即落盘到 columnar_ms segment）。
//! 优化后：tombstone 留 write_buf，靠查询路径 ensure_query_visibility 延迟 flush，
//!         重启时由 WAL recovery 回放 tombstone 到 ColSegmentStore。
//!
//! 本测试验证：DELETE 后不 flush（模拟崩溃/重启），已删除的行不得"复活"。
//! 关键：用 GroupCommit（默认）保证 WAL delete 记录已落盘，但 ColSegmentStore
//! 的 tombstone 可能仍在内存 write_buf（未 flush）。

#[path = "common/mod.rs"]
mod common;
use common::*;
use motedb::Database;

/// INSERT 若干行 → DELETE 一部分 → 不 flush 直接重启 → 验证删除持久。
#[test]
fn test_delete_survives_restart_without_flush() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    // Phase 1: 插入 100 行
    {
        let db = Database::create(&path).unwrap();
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
        for i in 1..=100 {
            exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i * 10));
        }
        assert_eq!(count_rows(&db, "SELECT * FROM t"), 100);

        // Phase 2: DELETE 一半（id > 50），不 flush
        exec(&db, "DELETE FROM t WHERE id > 50");
        assert_eq!(count_rows(&db, "SELECT * FROM t"), 50);
        assert_eq!(count_rows(&db, "SELECT * FROM t WHERE id <= 50"), 50);
        assert_eq!(count_rows(&db, "SELECT * FROM t WHERE id > 50"), 0);

        // 🔑 不调用 flush/close，直接 drop —— 模拟崩溃。
        // WAL 在 GroupCommit 下已落盘（log_delete_raw 已 fsync），
        // 但 ColSegmentStore 的 tombstone 可能还在内存 write_buf。
        drop(db);
    }

    // Phase 3: 重启，验证删除持久（行不复活）
    let db = Database::open(&path).unwrap();
    assert_eq!(
        count_rows(&db, "SELECT * FROM t"),
        50,
        "DELETE 未持久：重启后已删除的行复活了"
    );
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE id > 50"), 0);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE id = 50"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE id = 51"), 0);
}

/// DELETE 后重新 INSERT 同 PK，重启后应是新值（tombstone 不应误删新行）。
#[test]
fn test_delete_then_reinsert_survives_restart() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = Database::create(&path).unwrap();
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
        exec(&db, "INSERT INTO t VALUES (1, 'old')");
        exec(&db, "INSERT INTO t VALUES (2, 'old2')");
        exec(&db, "DELETE FROM t WHERE id = 1");
        // 重新插入 id=1（新值）—— tombstone 的 timestamp 应小于新行
        exec(&db, "INSERT INTO t VALUES (1, 'new')");
        assert_eq!(count_rows(&db, "SELECT * FROM t"), 2);
        db.close().unwrap();
        drop(db);
    }

    let db = Database::open(&path).unwrap();
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 2);
    // id=1 应存在（新值），id=2 存在（未删）
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE id = 1"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE id = 2"), 1);
}
