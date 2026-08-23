//! TimeSeries query/DML semantics — the complete matrix that round-5 fixed.
//!
//! TimeSeries tables live in the ColumnarStore. Before these fixes:
//!   - COUNT(*) with any WHERE returned 0 (count fast paths read LSM);
//!   - GROUP BY returned 0 rows (routed to materialize/LSM);
//!   - DELETE reported rows deleted and counted them down while every row
//!     stayed fully visible (tombstones written to a store TS reads never
//!     consult);
//!   - UPDATE errored with an internal ColSegmentStore gate message.

use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        motedb::sql::QueryResult::Select { rows, .. } => rows,
        other => panic!("expected Select, got {:?} for {}", other, sql),
    }
}

fn scalar(db: &Database, sql: &str) -> Value {
    rows(db, sql)[0][0].clone()
}

fn setup() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE m (ts TIMESTAMP, v FLOAT) TIMESERIES(ts)")
        .unwrap();
    // v cycles 0,1,2 — 7×0, 7×1, 6×2 for 20 rows.
    for i in 0..20i64 {
        db.execute(&format!(
            "INSERT INTO m VALUES ({}, {})",
            i * 1000,
            (i % 3) as f64
        ))
        .unwrap();
    }
    (db, dir)
}

#[test]
fn test_ts_count_with_where() {
    let (db, _d) = setup();
    assert_eq!(
        scalar(&db, "SELECT COUNT(*) FROM m WHERE v = 1.0"),
        Value::Integer(7)
    );
    assert_eq!(
        scalar(&db, "SELECT COUNT(*) FROM m WHERE ts = 5000"),
        Value::Integer(1)
    );
    assert_eq!(
        scalar(&db, "SELECT COUNT(*) FROM m WHERE ts < 5000"),
        Value::Integer(5)
    );
    assert_eq!(
        scalar(
            &db,
            "SELECT COUNT(*) FROM m WHERE ts >= 5000 AND ts <= 9000"
        ),
        Value::Integer(5)
    );
    assert_eq!(
        scalar(&db, "SELECT COUNT(v) FROM m WHERE v = 2.0"),
        Value::Integer(6)
    );
}

#[test]
fn test_ts_aggregates() {
    let (db, _d) = setup();
    assert_eq!(scalar(&db, "SELECT SUM(v) FROM m"), Value::Float(19.0));
    assert_eq!(
        scalar(&db, "SELECT AVG(v) FROM m"),
        Value::Float(19.0 / 20.0)
    );
    assert_eq!(scalar(&db, "SELECT MIN(v) FROM m"), Value::Float(0.0));
    assert_eq!(scalar(&db, "SELECT MAX(v) FROM m"), Value::Float(2.0));
    assert_eq!(
        scalar(&db, "SELECT SUM(v) FROM m WHERE ts <= 5000"),
        Value::Float(6.0)
    ); // 0+1+2+0+1+2
}

#[test]
fn test_ts_group_by() {
    let (db, _d) = setup();
    let r = rows(&db, "SELECT v, COUNT(*) FROM m GROUP BY v ORDER BY v");
    assert_eq!(
        r,
        vec![
            vec![Value::Float(0.0), Value::Integer(7)],
            vec![Value::Float(1.0), Value::Integer(7)],
            vec![Value::Float(2.0), Value::Integer(6)],
        ]
    );
    let r = rows(
        &db,
        "SELECT v, SUM(v) FROM m WHERE ts >= 6000 GROUP BY v ORDER BY v",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::Float(0.0), Value::Float(0.0)],
            vec![Value::Float(1.0), Value::Float(5.0)],
            vec![Value::Float(2.0), Value::Float(8.0)],
        ]
    );
}

#[test]
fn test_ts_select_shapes() {
    let (db, _d) = setup();
    assert_eq!(rows(&db, "SELECT ts FROM m").len(), 20);
    assert_eq!(rows(&db, "SELECT ts FROM m LIMIT 3").len(), 3);
    assert_eq!(rows(&db, "SELECT ts FROM m ORDER BY ts LIMIT 3").len(), 3);
    // DESC ordering actually returns descending values.
    let r = rows(&db, "SELECT ts FROM m ORDER BY ts DESC LIMIT 2");
    assert_eq!(
        r[0][0],
        Value::Timestamp(motedb::types::Timestamp::from_micros(19000))
    );
    assert_eq!(
        r[1][0],
        Value::Timestamp(motedb::types::Timestamp::from_micros(18000))
    );
}

#[test]
fn test_ts_update_rejected_clearly() {
    let (db, _d) = setup();
    let err = match db.execute("UPDATE m SET v = 9.9 WHERE ts = 5000") {
        Err(e) => e.to_string(),
        Ok(_) => panic!("UPDATE on TimeSeries must be rejected"),
    };
    assert!(
        err.contains("immutable"),
        "UPDATE on TimeSeries must give a clear immutability error: {err}"
    );
}

#[test]
fn test_ts_delete_requires_time_range() {
    let (db, _d) = setup();
    let err = match db.execute("DELETE FROM m WHERE v = 1.0") {
        Err(e) => e.to_string(),
        Ok(_) => panic!("non-range DELETE on TimeSeries must be rejected"),
    };
    assert!(
        err.contains("time-range"),
        "non-range DELETE on TimeSeries must be rejected: {err}"
    );
}

#[test]
fn test_ts_delete_all_purges_everything() {
    // DELETE FROM (no WHERE) maps to cutoff = i64::MAX — everything expires.
    let (db, _d) = setup();
    let n = db.execute("DELETE FROM m").unwrap().affected_rows();
    assert!(n >= 20, "purge-all must remove all rows, got {n}");
    assert_eq!(scalar(&db, "SELECT COUNT(*) FROM m"), Value::Integer(0));
    assert_eq!(rows(&db, "SELECT ts FROM m").len(), 0);
}

#[test]
fn test_ts_semantics_survive_crash_reopen() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().join("t.mote");
    {
        let db = Database::create(&p).unwrap();
        db.execute("CREATE TABLE m (ts TIMESTAMP, v FLOAT) TIMESERIES(ts)")
            .unwrap();
        for i in 0..20i64 {
            db.execute(&format!(
                "INSERT INTO m VALUES ({}, {})",
                i * 1000,
                (i % 3) as f64
            ))
            .unwrap();
        }
        std::mem::forget(db);
    }
    std::fs::remove_file(p.join(".lock")).ok();
    let db = Database::open(&p).unwrap();
    assert_eq!(
        scalar(&db, "SELECT COUNT(*) FROM m WHERE v = 1.0"),
        Value::Integer(7)
    );
    let r = rows(&db, "SELECT v, COUNT(*) FROM m GROUP BY v ORDER BY v");
    assert_eq!(r.len(), 3);
}

#[test]
fn test_ts_vacuum_then_crash_no_doubling() {
    // Regression: VACUUM flushed data but never truncated the WAL — a crash
    // after VACUUM replayed the WAL on top of the flushed segments and
    // DOUBLED every TimeSeries row (10 → 20).
    let dir = TempDir::new().unwrap();
    let p = dir.path().join("t.mote");
    {
        let db = Database::create(&p).unwrap();
        db.execute("CREATE TABLE m (ts TIMESTAMP, v FLOAT) TIMESERIES(ts)")
            .unwrap();
        for i in 0..10i64 {
            db.execute(&format!(
                "INSERT INTO m VALUES ({}, {})",
                i * 1000,
                i as f64
            ))
            .unwrap();
        }
        assert_eq!(rows(&db, "SELECT ts FROM m").len(), 10);
        db.execute("VACUUM").unwrap();
        std::mem::forget(db);
    }
    std::fs::remove_file(p.join(".lock")).ok();
    let db = Database::open(&p).unwrap();
    assert_eq!(
        rows(&db, "SELECT ts FROM m").len(),
        10,
        "rows doubled after VACUUM + crash"
    );
    assert_eq!(scalar(&db, "SELECT COUNT(*) FROM m"), Value::Integer(10));
    assert_eq!(rows(&db, "SELECT * FROM m LATEST BY ts").len(), 10);
}
