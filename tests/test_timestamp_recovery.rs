//! Timestamp column visibility across crash recovery.
//!
//! Regression for the decode_any type-blindness bug: raw-format WAL records
//! decoded without schema turned Timestamp values into Integers, so BOTH the
//! recovery-time timestamp index fill AND the memtable range-scan fallback
//! missed every row — `query_timestamp_range` returned nothing after a crash
//! for standard tables with a TIMESTAMP column.

use motedb::Database;
use tempfile::TempDir;

#[test]
fn test_timestamp_range_live() {
    // Control: without any crash, the runtime index covers the rows.
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("ts.mote")).unwrap();
    db.execute("CREATE TABLE events (ts TIMESTAMP, v INTEGER)")
        .unwrap();
    for i in 1..=10i64 {
        db.execute(&format!(
            "INSERT INTO events VALUES ({}, {})",
            i * 1_000_000,
            i
        ))
        .unwrap();
    }
    let ids = db.query_timestamp_range(0, 20_000_000).unwrap();
    assert_eq!(ids.len(), 10, "live range query must see all rows");
}

#[test]
fn test_timestamp_range_after_crash_recovery() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("ts.mote");
    {
        let db = Database::create(&db_path).unwrap();
        db.execute("CREATE TABLE events (ts TIMESTAMP, v INTEGER)")
            .unwrap();
        for i in 1..=10i64 {
            db.execute(&format!(
                "INSERT INTO events VALUES ({}, {})",
                i * 1_000_000,
                i
            ))
            .unwrap();
        }
        // Crash-sim: leak the handle (no close/flush/checkpoint). WAL records
        // for all 10 rows are already in the kernel page cache.
        std::mem::forget(db);
    }
    // Release the stale lock as the OS would after SIGKILL.
    let _ = std::fs::remove_file(db_path.join(".lock"));

    let db = Database::open(&db_path).unwrap();
    // All rows survived and are visible to the timestamp range path.
    let ids = db.query_timestamp_range(0, 20_000_000).unwrap();
    assert_eq!(
        ids.len(),
        10,
        "timestamp range after crash recovery lost rows (index + memtable fallback both empty)"
    );
    // And the data itself is queryable + correct.
    let n = db.query("SELECT COUNT(*) FROM events").unwrap();
    assert_eq!(n[0][0], motedb::types::Value::Integer(10));
}
