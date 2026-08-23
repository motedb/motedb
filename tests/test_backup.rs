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
