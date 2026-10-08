//! Online backup API (backup_to): consistent snapshot while the database is
//! open, under concurrent write load, with independent restore.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        motedb::sql::QueryResult::Select { rows, .. } => rows,
        other => panic!("expected Select, got {:?} for {}", other, sql),
    }
}

fn count(db: &Database, table: &str) -> i64 {
    let r = rows(db, &format!("SELECT COUNT(*) FROM {table}"));
    match r[0][0] {
        Value::Integer(n) => n,
        _ => panic!("count not int"),
    }
}

#[test]
fn test_backup_and_restore_roundtrip() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    for i in 0..100 {
        db.execute(&format!("INSERT INTO t VALUES ({i}, 'name-{i}')"))
            .unwrap();
    }

    let backup_dir = TempDir::new().unwrap();
    let dest = backup_dir.path().join("snapshot");
    db.backup_to(&dest).unwrap();

    // Restore: open the copy as an independent database.
    let restored = Database::open(&dest).unwrap();
    assert_eq!(count(&restored, "t"), 100);
    let r = rows(&restored, "SELECT name FROM t WHERE id = 42");
    assert_eq!(r[0][0], Value::text("name-42".to_string()));
    // Original unaffected.
    assert_eq!(count(&db, "t"), 100);
}

#[test]
fn test_backup_is_independent_of_future_writes() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let backup_dir = TempDir::new().unwrap();
    let dest = backup_dir.path().join("snapshot");
    db.backup_to(&dest).unwrap();

    // Post-backup writes must NOT appear in the snapshot.
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    db.execute("UPDATE t SET v = 999 WHERE id = 1").unwrap();

    let restored = Database::open(&dest).unwrap();
    assert_eq!(count(&restored, "t"), 1);
    let r = rows(&restored, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r[0][0], Value::Integer(10));
}

#[test]
fn test_backup_under_concurrent_writes_is_consistent() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    for i in 0..50 {
        db.execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .unwrap();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let writer_db = Arc::clone(&db);
    let stop_w = Arc::clone(&stop);
    let writer = std::thread::spawn(move || {
        let mut i = 1000;
        while !stop_w.load(Ordering::Relaxed) {
            writer_db
                .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .unwrap();
            i += 1;
        }
    });

    let backup_dir = TempDir::new().unwrap();
    let dest = backup_dir.path().join("snapshot");
    db.backup_to(&dest).unwrap();
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();

    // The snapshot must be a valid database containing the pre-backup state
    // (plus whatever concurrent writes had committed when the copy started).
    let restored = Database::open(&dest).unwrap();
    let n = count(&restored, "t");
    assert!(n >= 50, "backup lost pre-existing rows: {n} < 50");
    // No holes among ids 0..50.
    for i in 0..50 {
        let r = rows(&restored, &format!("SELECT v FROM t WHERE id = {i}"));
        assert_eq!(r[0][0], Value::Integer(i), "row {i} corrupted in backup");
    }
    // The live DB has everything.
    let live = count(&db, "t");
    assert!(
        live > n,
        "concurrent writes after backup missing: {live} <= {n}"
    );
}

#[test]
fn test_backup_destination_must_not_exist() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();

    let backup_dir = TempDir::new().unwrap();
    let dest = backup_dir.path().join("snap");
    db.backup_to(&dest).unwrap();
    // Second backup to the same destination fails cleanly.
    assert!(db.backup_to(&dest).is_err());
}

#[test]
fn test_backup_of_database_with_upserts_and_ts() {
    // Exercise a mix of table kinds: standard with upserts + TimeSeries.
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE s (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();
    for round in 0..10 {
        db.execute(&format!(
            "INSERT INTO s VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET c = c + excluded.c + {round}"
        )).unwrap();
    }
    db.execute("CREATE TABLE m (ts TIMESTAMP, v FLOAT) TIMESERIES(ts)")
        .unwrap();
    for i in 0..20 {
        db.execute(&format!("INSERT INTO m VALUES ({}, 1.5)", i * 1000))
            .unwrap();
    }

    let backup_dir = TempDir::new().unwrap();
    let dest = backup_dir.path().join("snapshot");
    db.backup_to(&dest).unwrap();

    let restored = Database::open(&dest).unwrap();
    // Compare against the live DB rather than a hand-computed constant.
    let live_c = rows(&db, "SELECT c FROM s WHERE id = 1")[0][0].clone();
    let r = rows(&restored, "SELECT c FROM s WHERE id = 1");
    assert_eq!(r[0][0], live_c);
    assert_eq!(count(&restored, "m"), 20);
}

// 🔁 外部测评后回归: backup_to 快照必须包含派生索引的当前状态。
// 修复前: backup 只跑 flush_impl(有意不碰 text/vector 索引), FTS 的内存
// pending posting lists 与 index-builder 未落盘批次都不进快照 ——
// 500 行快照的 MATCH 只命中 396。flush_all_indexes_for_backup 先排空
// builder 队列再无条件 flush(锁安全论证见 persistence.rs)。
#[test]
fn backup_snapshot_contains_text_index_state() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("CREATE TEXT INDEX ti ON t(s)").unwrap();
    for i in 0..500 {
        db.execute(&format!("INSERT INTO t VALUES ({}, 'doc {} common')", i, i))
            .unwrap();
    }
    db.checkpoint().unwrap();

    let snap = TempDir::new().unwrap();
    db.backup_to(snap.path()).unwrap();

    // 源库继续写不受影响
    db.execute("INSERT INTO t VALUES (9999, 'after backup common')")
        .unwrap();
    assert_eq!(count(&db, "t"), 501);

    // 快照 = 备份时刻状态: 行数与全文检索都完整
    let restored = Database::open(snap.path()).unwrap();
    assert_eq!(count(&restored, "t"), 500);
    let n = rows(&restored, "SELECT COUNT(*) FROM t WHERE MATCH(s, 'common')");
    match &n[0][0] {
        Value::Integer(c) => assert_eq!(*c, 500, "快照 FTS 缺数据"),
        other => panic!("expected INTEGER, got {:?}", other),
    }
    // 源库重开同样完整
    drop(db);
    let db2 = Database::open(dir.path()).unwrap();
    let n = rows(&db2, "SELECT COUNT(*) FROM t WHERE MATCH(s, 'common')");
    match &n[0][0] {
        Value::Integer(c) => assert_eq!(*c, 501),
        other => panic!("expected INTEGER, got {:?}", other),
    }
}

// 🔁 复审#4/fuzz 实抓: backup 目标位于库目录内部时, 递归复制自吞噬
// (路径逐层加深 → ENAMETOOLONG)。现应明确拒绝。
#[test]
fn backup_destination_inside_db_dir_rejected() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("inner.mote")).unwrap();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    let dest = dir.path().join("inner.mote/nested/snap.mote");
    std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
    let e = match db.backup_to(&dest) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("库内目标应报错"),
    };
    assert!(e.contains("outside"), "{}", e);
    // 平级/外部目标仍正常
    let ok_dest = dir.path().join("snap.mote");
    db.backup_to(&ok_dest).unwrap();
}
