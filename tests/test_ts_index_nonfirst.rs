//! §10c: the timestamp index covers Timestamp columns in ANY position.
//!
//! v1 only indexed rows whose FIRST value was a Timestamp
//! (`index_row_timestamp` looked at `row.first()`), so tables like
//! `(id INT, name TEXT, ts TIMESTAMP)` were served by the index only after a
//! checkpoint/reopen rebuild — `query_timestamp_range` returned nothing for
//! freshly inserted rows. The write path now picks the first
//! Value::Timestamp anywhere in the row, matching
//! `rebuild_timestamp_index`'s first-Timestamp-column choice.
use motedb::Database;
use tempfile::TempDir;

#[test]
fn ts_not_first_column_live_visibility() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("nf.mote")).unwrap();
    db.execute("CREATE TABLE m (id INT PRIMARY KEY, name TEXT, ts TIMESTAMP)")
        .unwrap();
    for i in 1..=10i64 {
        db.execute(&format!(
            "INSERT INTO m (id, name, ts) VALUES ({i}, 'n{i}', {})",
            i * 1_000_000
        ))
        .unwrap();
    }
    let ids = db.query_timestamp_range(0, 20_000_000).unwrap();
    assert_eq!(
        ids.len(),
        10,
        "write-path index must cover ts in a non-first column before any checkpoint"
    );
}

#[test]
fn ts_not_first_column_after_reopen() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("nf.mote");
    {
        let db = Database::create(&db_path).unwrap();
        // Two timestamp columns: the index keys on the FIRST ts column
        // (`ts` = i×100M); the second (`ts2` = i×1M) must not leak in.
        db.execute("CREATE TABLE m (id INT PRIMARY KEY, ts TIMESTAMP, ts2 TIMESTAMP)")
            .unwrap();
        for i in 1..=10i64 {
            db.execute(&format!(
                "INSERT INTO m (id, ts, ts2) VALUES ({i}, {}, {})",
                i * 100_000_000,
                i * 1_000_000
            ))
            .unwrap();
        }
    }
    let db = Database::open(&db_path).unwrap();
    let full = db.query_timestamp_range(0, 1_000_000_000).unwrap();
    assert_eq!(
        full.len(),
        10,
        "reopen rebuild must index non-first-position ts (col 2 of 3)"
    );
    // ts values are i×100M: only i=1 falls in (0, 150M).
    let mid = db.query_timestamp_range(0, 150_000_000).unwrap();
    assert_eq!(
        mid.len(),
        1,
        "window selects by the first ts column's values"
    );
    // ts2 has 2 values in (0, 2M) — none may surface: entries are keyed by
    // ts, proving the second timestamp column does not leak into the index.
    let narrow = db.query_timestamp_range(0, 2_000_000).unwrap();
    assert!(
        narrow.is_empty(),
        "second ts column must not leak into the index"
    );
}

#[test]
fn ts_last_column_crash_recovery() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("nf.mote");
    {
        let db = Database::create(&db_path).unwrap();
        db.execute("CREATE TABLE m (id INT PRIMARY KEY, payload FLOAT, note TEXT, ts TIMESTAMP)")
            .unwrap();
        for i in 1..=8i64 {
            db.execute(&format!(
                "INSERT INTO m (id, payload, note, ts) VALUES ({i}, {}, 'x', {})",
                i as f64 * 0.5,
                i * 1_000_000
            ))
            .unwrap();
        }
        std::mem::forget(db);
    }
    let _ = std::fs::remove_file(db_path.join(".lock"));
    let db = Database::open(&db_path).unwrap();
    let ids = db.query_timestamp_range(0, 20_000_000).unwrap();
    assert_eq!(
        ids.len(),
        8,
        "WAL recovery + rebuild must index last-column ts"
    );
}
