//! Striped autocommit write-lock regression tests.
//!
//! The api layer used to take ONE global write_lock for every autocommit
//! INSERT/UPDATE/DELETE — concurrent autocommit writes were fully
//! serialized (4-thread write throughput ≈ single-thread), and WAL
//! group-commit could never batch concurrent writers. Writers now hold a
//! per-table stripe lock; flush/backup take all stripes + the global lock.
//!
//! These tests pin the semantics the lock exists to protect:
//! - same-table read-modify-write (v = v + 1) must never lose updates
//! - cross-table writers proceed concurrently
//! - backup_to during concurrent writes yields a consistent snapshot

use motedb::types::Value;
use motedb::Database;
use std::sync::Arc;
use tempfile::TempDir;

fn rows(r: motedb::StreamingQueryResult) -> Vec<Vec<Value>> {
    use motedb::QueryResult;
    match r.materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        _ => panic!("expected select"),
    }
}

#[test]
fn concurrent_rmw_no_lost_updates() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.execute("CREATE TABLE cnt (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO cnt VALUES (1, 0)").unwrap();

    let mut handles = Vec::new();
    for _ in 0..4 {
        let db = db.clone();
        handles.push(std::thread::spawn(move || {
            for _ in 0..250 {
                db.execute("UPDATE cnt SET v = v + 1 WHERE id = 1")
                    .expect("increment must succeed");
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let got = rows(db.execute("SELECT v FROM cnt WHERE id = 1").unwrap());
    assert_eq!(
        got[0][0],
        Value::Integer(1000),
        "lost update under striped autocommit lock"
    );
}

#[test]
fn concurrent_same_pk_inserts_rejected() {
    // Same-table writers still serialize → the PK dup check can't race
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();

    let mut handles = Vec::new();
    for _ in 0..4 {
        let db = db.clone();
        handles.push(std::thread::spawn(move || {
            let mut accepted = 0usize;
            for i in 0..200i64 {
                let id = i % 50; // heavy PK collision pressure
                if db
                    .execute(&format!("INSERT INTO t VALUES ({id}, {i})"))
                    .is_ok()
                {
                    accepted += 1;
                }
            }
            accepted
        }));
    }
    let mut total = 0usize;
    for h in handles {
        total += h.join().unwrap();
    }
    let got = rows(db.execute("SELECT COUNT(*) FROM t").unwrap());
    let count = match &got[0][0] {
        Value::Integer(n) => *n,
        other => panic!("expected int, got {other:?}"),
    };
    assert_eq!(
        count as usize, total,
        "accepted inserts must equal stored rows (no dup, no loss)"
    );
    assert_eq!(count, 50, "each PK stored at most once");
}

#[test]
fn backup_during_concurrent_writes_is_consistent() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("CREATE TABLE b (id INT PRIMARY KEY, v INT)")
        .unwrap();

    let mut handles = Vec::new();
    for t in ["a", "b"] {
        let db = db.clone();
        let t = t.to_string();
        handles.push(std::thread::spawn(move || {
            for i in 0..300i64 {
                db.execute(&format!("INSERT INTO {t} VALUES ({i}, {i})"))
                    .unwrap();
            }
        }));
    }
    // Backup while writers run: the all-stripes + global barrier must make
    // the snapshot a consistent point-in-time (no torn WAL/segment state).
    let db2 = db.clone();
    let backup_dir = TempDir::new().unwrap();
    let dest = backup_dir.path().join("snap");
    let dest2 = dest.clone();
    let hb = std::thread::spawn(move || db2.backup_to(&dest2));
    for h in handles {
        h.join().unwrap();
    }
    hb.join().unwrap().expect("backup during concurrent writes");

    let snap = Database::open(&dest).unwrap();
    for t in ["a", "b"] {
        let got = rows(snap.execute(&format!("SELECT COUNT(*) FROM {t}")).unwrap());
        let n = match &got[0][0] {
            Value::Integer(n) => *n,
            other => panic!("expected int, got {other:?}"),
        };
        assert!(
            (0..=300).contains(&n),
            "snapshot {t} count {n} within [0, 300]"
        );
        // every row present must be complete
        let rows_got = rows(
            snap.execute(&format!("SELECT v FROM {t} ORDER BY v"))
                .unwrap(),
        );
        for (i, r) in rows_got.iter().enumerate() {
            assert_eq!(r[0], Value::Integer(i as i64), "{t} row {i} intact");
        }
    }
}
