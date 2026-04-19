//! End-to-end integration tests for the Columnar Segment Store.

use motedb::Database;
use motedb::config::DBConfig;
use motedb::types::{Value, Timestamp};
use tempfile::TempDir;

fn create_ts_table(db: &Database, name: &str) {
    db.execute(&format!(
        "CREATE TABLE {} (ts TIMESTAMP, temperature FLOAT, humidity FLOAT, label TEXT) TIMESERIES(ts)",
        name
    )).unwrap();
}

#[test]
fn test_columnar_sql_insert_and_query() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::default()).unwrap();

    create_ts_table(&db, "sensors");

    // Ingest 100 rows via columnar API (SQL TIMESTAMP parsing is complex)
    let mut rows = Vec::new();
    for i in 0..100 {
        rows.push(vec![
            Value::Timestamp(Timestamp::from_micros(1_000_000 + i as i64 * 10_000)),
            Value::Float(25.0 + i as f64 * 0.1),
            Value::Float(60.0 + i as f64 * 0.05),
            Value::Text(format!("sensor_{}", i)),
        ]);
    }
    let result = db.columnar_store().ingest("sensors", rows).unwrap();
    assert_eq!(result.row_ids.len(), 100);

    db.flush().unwrap();
}

#[test]
fn test_columnar_api_ingest_and_query() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::default()).unwrap();

    create_ts_table(&db, "metrics");

    // Batch insert 200 rows via columnar store API
    let mut rows = Vec::new();
    for i in 0..200 {
        rows.push(vec![
            Value::Timestamp(Timestamp::from_micros(1_000_000 + i as i64 * 5_000)),
            Value::Float(20.0 + (i as f64) * 0.05),
            Value::Float(50.0 + (i as f64) * 0.02),
            Value::Text(format!("point_{}", i)),
        ]);
    }

    let result = db.columnar_store().ingest("metrics", rows).unwrap();
    assert_eq!(result.row_ids.len(), 200);

    db.flush().unwrap();
    assert!(db.columnar_store().segment_count("metrics") > 0);

    // Query time range
    let results = db.columnar_store().query_time_range(
        "metrics", 1_500_000, 1_600_000,
        &["ts".to_string(), "temperature".to_string()],
    ).unwrap();

    assert!(!results.is_empty(), "Should return results for overlapping time range");

    for (_row_id, sql_row) in &results {
        if let Some(Value::Timestamp(ts)) = sql_row.get("ts") {
            let micros = ts.as_micros();
            assert!(micros >= 1_500_000 && micros <= 1_600_000,
                "Timestamp {} should be in [1500000, 1600000]", micros);
        }
    }
}

#[test]
fn test_columnar_ttl_gc() {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::default();
    config.columnar_config.buffer_row_capacity = 50;
    let db = Database::create_with_config(dir.path(), config).unwrap();

    create_ts_table(&db, "old_data");

    // Old data segment
    let mut old_rows = Vec::new();
    for i in 0..50 {
        old_rows.push(vec![
            Value::Timestamp(Timestamp::from_micros(100_000 + i as i64 * 2_000)),
            Value::Float(25.0),
            Value::Float(60.0),
            Value::Text("old".to_string()),
        ]);
    }
    db.columnar_store().ingest("old_data", old_rows).unwrap();
    db.flush().unwrap();

    // New data segment
    let mut new_rows = Vec::new();
    for i in 0..50 {
        new_rows.push(vec![
            Value::Timestamp(Timestamp::from_micros(1_000_000 + i as i64 * 2_000)),
            Value::Float(30.0),
            Value::Float(55.0),
            Value::Text("new".to_string()),
        ]);
    }
    db.columnar_store().ingest("old_data", new_rows).unwrap();
    db.flush().unwrap();

    assert_eq!(db.columnar_store().segment_count("old_data"), 2);

    // GC: remove old segments
    let deleted = db.columnar_store().gc_expired("old_data", 500_000).unwrap();
    assert_eq!(deleted, 1);
    assert_eq!(db.columnar_store().segment_count("old_data"), 1);

    // Verify new data still accessible
    let results = db.columnar_store().query_time_range(
        "old_data", 1_000_000, 1_100_000, &[],
    ).unwrap();
    assert!(!results.is_empty());
}

#[test]
fn test_columnar_gorilla_compression_roundtrip() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::default()).unwrap();

    db.execute("CREATE TABLE mixed (ts TIMESTAMP, value FLOAT, count INT, active BOOL, name TEXT) TIMESERIES(ts)").unwrap();

    let mut rows = Vec::new();
    for i in 0..500 {
        rows.push(vec![
            Value::Timestamp(Timestamp::from_micros(1_000_000 + i as i64 * 1_000)),
            Value::Float(25.0 + (i as f64) * 0.01),
            Value::Integer(i),
            Value::Bool(i % 2 == 0),
            Value::Text(format!("entry_{}", i % 10)),
        ]);
    }

    let result = db.columnar_store().ingest("mixed", rows).unwrap();
    assert_eq!(result.row_ids.len(), 500);
    db.flush().unwrap();

    let results = db.columnar_store().query_time_range(
        "mixed", 1_000_000, 1_500_000, &[],
    ).unwrap();
    assert_eq!(results.len(), 500);

    // Verify round-trip
    for (i, (_row_id, sql_row)) in results.iter().enumerate() {
        if let Some(Value::Timestamp(ts)) = sql_row.get("ts") {
            assert_eq!(ts.as_micros(), 1_000_000 + i as i64 * 1_000,
                "Timestamp mismatch at row {}", i);
        }
        if let Some(Value::Integer(count)) = sql_row.get("count") {
            assert_eq!(*count, i as i64, "Count mismatch at row {}", i);
        }
        if let Some(Value::Bool(active)) = sql_row.get("active") {
            assert_eq!(*active, i % 2 == 0, "Bool mismatch at row {}", i);
        }
        if let Some(Value::Text(name)) = sql_row.get("name") {
            assert_eq!(name, &format!("entry_{}", i % 10), "Name mismatch at row {}", i);
        }
    }
}

#[test]
fn test_dual_engine_standard_and_timeseries() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::default()).unwrap();

    // Standard table
    db.execute("CREATE TABLE users (id INT, name TEXT)").unwrap();
    db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')").unwrap();

    // TimeSeries table
    create_ts_table(&db, "sensor_log");
    let rows = vec![vec![
        Value::Timestamp(Timestamp::from_micros(1_000_000)),
        Value::Float(25.5),
        Value::Float(60.0),
        Value::Text("ok".to_string()),
    ]];
    db.columnar_store().ingest("sensor_log", rows).unwrap();
    db.flush().unwrap();

    // Query Standard table via LSM
    let result = db.execute("SELECT * FROM users").unwrap();
    if let motedb::QueryResult::Select { rows, .. } = result.materialize().unwrap() {
        assert_eq!(rows.len(), 2);
    } else {
        panic!("Expected Select result");
    }

    // Query TimeSeries table via columnar store
    let results = db.columnar_store().query_time_range(
        "sensor_log", 0, 2_000_000, &[],
    ).unwrap();
    assert_eq!(results.len(), 1);
}
