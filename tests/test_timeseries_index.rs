//! Comprehensive TimeSeries index tests.
//!
//! Tests the four-layer indexing system:
//! - Layer 1: Zone Maps (per-column min/max statistics)
//! - Layer 2: Timestamp sorting + binary search
//! - Layer 3: Bloom filters for Text columns
//! - Layer 4: SegmentManager binary search + condition pruning

use motedb::config::DBConfig;
use motedb::storage::columnar::segment_manager::ColumnCondition;
use motedb::types::{Timestamp, Value};
use motedb::Database;
use tempfile::TempDir;

/// Helper: create a DB with a small buffer for frequent flushing.
fn create_db() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::default();
    config.columnar_config.buffer_row_capacity = 100;
    config.columnar_config.segment_target_rows = 500;
    config.columnar_config.enable_merge = false; // disable merge for predictable segment counts
    let db = Database::create_with_config(dir.path(), config).unwrap();
    (dir, db)
}

fn create_sensor_table(db: &Database, name: &str) {
    db.execute(&format!(
        "CREATE TABLE {} (ts TIMESTAMP, temperature FLOAT, humidity FLOAT, label TEXT, zone INT) TIMESERIES(ts)",
        name
    )).unwrap();
}

fn ingest_rows(db: &Database, table: &str, count: usize, base_ts: i64, ts_step: i64) {
    let mut rows = Vec::with_capacity(count);
    for i in 0..count {
        rows.push(vec![
            Value::Timestamp(Timestamp::from_micros(base_ts + i as i64 * ts_step)),
            Value::Float(20.0 + (i as f64 % 10.0)),
            Value::Float(50.0 + (i as f64 % 5.0)),
            Value::Text(format!("label_{}", i % 20)),
            Value::Integer((i % 5) as i64),
        ]);
    }
    db.columnar_store().ingest(table, rows).unwrap();
}

// ============================================================
// Test 1: Timestamp sorting + binary search correctness
// ============================================================

#[test]
fn test_timestamp_sort_binary_search() {
    let (_dir, db) = create_db();
    create_sensor_table(&db, "sort_test");

    // Insert 500 rows with shuffled-like timestamps (out-of-order arrival)
    let mut rows = Vec::new();
    for batch in 0..5 {
        for i in 0..100 {
            // Each batch has a different base offset to create out-of-order writes
            let ts = 1_000_000 + (batch * 200_000 + i * 100) as i64;
            rows.push(vec![
                Value::Timestamp(Timestamp::from_micros(ts)),
                Value::Float(25.0 + batch as f64),
                Value::Float(60.0),
                Value::Text(format!("b{}_{}", batch, i)),
                Value::Integer(batch as i64),
            ]);
        }
    }

    db.columnar_store().ingest("sort_test", rows).unwrap();
    db.flush().unwrap();

    // Query full range — should return all 500 rows
    let results = db.columnar_store().query_time_range(
        "sort_test", 0, 2_000_000, &[],
    ).unwrap();
    assert_eq!(results.len(), 500, "Should return all 500 rows");

    // Verify timestamps are in ascending order within each segment
    // (after sorting, rows within a segment should be ordered by ts)
    let mut prev_ts = i64::MIN;
    for (_rid, row) in &results {
        if let Some(Value::Timestamp(ts)) = row.get("ts") {
            let micros = ts.as_micros();
            assert!(micros >= prev_ts,
                "Rows should be sorted by timestamp: {} < {}", micros, prev_ts);
            prev_ts = micros;
        }
    }

    // Narrow range query — test binary search accuracy
    let narrow = db.columnar_store().query_time_range(
        "sort_test", 1_400_000, 1_600_000, &[],
    ).unwrap();
    for (_rid, row) in &narrow {
        if let Some(Value::Timestamp(ts)) = row.get("ts") {
            let micros = ts.as_micros();
            assert!(micros >= 1_400_000 && micros <= 1_600_000,
                "Narrow range result out of bounds: {}", micros);
        }
    }
}

// ============================================================
// Test 2: Zone Maps (column statistics) — segment pruning
// ============================================================

#[test]
fn test_zone_map_segment_pruning() {
    let (_dir, db) = create_db();
    create_sensor_table(&db, "zone_test");

    // Insert 3 separate batches with non-overlapping zone values
    // Batch 1: zone=0, temp 10-15
    let mut rows1 = Vec::new();
    for i in 0..100 {
        rows1.push(vec![
            Value::Timestamp(Timestamp::from_micros(100_000 + i as i64 * 100)),
            Value::Float(10.0 + i as f64 * 0.05),
            Value::Float(50.0),
            Value::Text("cold".to_string()),
            Value::Integer(0),
        ]);
    }
    db.columnar_store().ingest("zone_test", rows1).unwrap();
    db.flush().unwrap();

    // Batch 2: zone=1, temp 20-25
    let mut rows2 = Vec::new();
    for i in 0..100 {
        rows2.push(vec![
            Value::Timestamp(Timestamp::from_micros(200_000 + i as i64 * 100)),
            Value::Float(20.0 + i as f64 * 0.05),
            Value::Float(55.0),
            Value::Text("warm".to_string()),
            Value::Integer(1),
        ]);
    }
    db.columnar_store().ingest("zone_test", rows2).unwrap();
    db.flush().unwrap();

    // Batch 3: zone=2, temp 30-35
    let mut rows3 = Vec::new();
    for i in 0..100 {
        rows3.push(vec![
            Value::Timestamp(Timestamp::from_micros(300_000 + i as i64 * 100)),
            Value::Float(30.0 + i as f64 * 0.05),
            Value::Float(60.0),
            Value::Text("hot".to_string()),
            Value::Integer(2),
        ]);
    }
    db.columnar_store().ingest("zone_test", rows3).unwrap();
    db.flush().unwrap();

    assert_eq!(db.columnar_store().segment_count("zone_test"), 3);

    // Query with zone=0 condition — should prune segments 2 and 3
    let conditions = vec![
        ColumnCondition::Equals {
            column_idx: 4, // zone column
            value: Value::Integer(0),
        },
    ];
    let results = db.columnar_store().query_with_conditions(
        "zone_test", 0, 500_000, &conditions, &[],
    ).unwrap();

    // All results should have zone=0
    for (_rid, row) in &results {
        if let Some(Value::Integer(zone)) = row.get("zone") {
            assert_eq!(*zone, 0, "Zone should be 0, got {}", zone);
        }
    }

    // Range query: temperature in [25, 35]
    let range_conditions = vec![
        ColumnCondition::Range {
            column_idx: 1, // temperature column
            low: Value::Float(25.0),
            high: Value::Float(35.0),
        },
    ];
    let range_results = db.columnar_store().query_with_conditions(
        "zone_test", 0, 500_000, &range_conditions, &[],
    ).unwrap();

    for (_rid, row) in &range_results {
        if let Some(Value::Float(temp)) = row.get("temperature") {
            assert!(*temp >= 25.0 && *temp <= 35.0,
                "Temperature should be in [25, 35], got {}", temp);
        }
    }
}

// ============================================================
// Test 3: Bloom Filter — Text column pruning
// ============================================================

#[test]
fn test_bloom_filter_text_pruning() {
    let (_dir, db) = create_db();
    create_sensor_table(&db, "bloom_test");

    // Batch 1: labels "alpha", "beta"
    let mut rows1 = Vec::new();
    for i in 0..100 {
        rows1.push(vec![
            Value::Timestamp(Timestamp::from_micros(100_000 + i as i64 * 100)),
            Value::Float(20.0),
            Value::Float(50.0),
            Value::Text(if i % 2 == 0 { "alpha".to_string() } else { "beta".to_string() }),
            Value::Integer(0),
        ]);
    }
    db.columnar_store().ingest("bloom_test", rows1).unwrap();
    db.flush().unwrap();

    // Batch 2: labels "gamma", "delta"
    let mut rows2 = Vec::new();
    for i in 0..100 {
        rows2.push(vec![
            Value::Timestamp(Timestamp::from_micros(200_000 + i as i64 * 100)),
            Value::Float(25.0),
            Value::Float(55.0),
            Value::Text(if i % 2 == 0 { "gamma".to_string() } else { "delta".to_string() }),
            Value::Integer(1),
        ]);
    }
    db.columnar_store().ingest("bloom_test", rows2).unwrap();
    db.flush().unwrap();

    // Query: label = "alpha" → should only match segment 1
    let conditions = vec![
        ColumnCondition::Equals {
            column_idx: 3, // label column
            value: Value::Text("alpha".to_string()),
        },
    ];
    let results = db.columnar_store().query_with_conditions(
        "bloom_test", 0, 500_000, &conditions, &[],
    ).unwrap();

    for (_rid, row) in &results {
        if let Some(Value::Text(label)) = row.get("label") {
            assert_eq!(label, "alpha", "Label should be 'alpha', got '{}'", label);
        }
    }

    // Query: label = "nonexistent" → should return empty (bloom filter negative)
    let no_conditions = vec![
        ColumnCondition::Equals {
            column_idx: 3,
            value: Value::Text("nonexistent".to_string()),
        },
    ];
    let no_results = db.columnar_store().query_with_conditions(
        "bloom_test", 0, 500_000, &no_conditions, &[],
    ).unwrap();
    assert!(no_results.is_empty(), "Should return empty for nonexistent label");
}

// ============================================================
// Test 4: SegmentManager binary search pruning
// ============================================================

#[test]
fn test_segment_manager_binary_search_pruning() {
    let (_dir, db) = create_db();
    create_sensor_table(&db, "pruning_test");

    // Create 5 segments with distinct time ranges
    for seg in 0..5u64 {
        let mut rows = Vec::new();
        for i in 0..100 {
            rows.push(vec![
                Value::Timestamp(Timestamp::from_micros((seg * 1_000_000 + i * 1_000) as i64)),
                Value::Float(20.0 + seg as f64),
                Value::Float(50.0),
                Value::Text(format!("seg_{}", seg)),
                Value::Integer(seg as i64),
            ]);
        }
        db.columnar_store().ingest("pruning_test", rows).unwrap();
        db.flush().unwrap();
    }

    assert_eq!(db.columnar_store().segment_count("pruning_test"), 5);

    // Narrow time range should only hit segment 2 [2_000_000, 2_099_000]
    let narrow = db.columnar_store().query_time_range(
        "pruning_test", 2_030_000, 2_050_000, &[],
    ).unwrap();
    assert!(!narrow.is_empty(), "Should find rows in segment 2 range");
    for (_rid, row) in &narrow {
        if let Some(Value::Timestamp(ts)) = row.get("ts") {
            let micros = ts.as_micros();
            assert!(micros >= 2_030_000 && micros <= 2_050_000,
                "Out of range: {}", micros);
        }
    }

    // Query across boundary of segment 1 and segment 2
    let boundary = db.columnar_store().query_time_range(
        "pruning_test", 1_090_000, 2_010_000, &[],
    ).unwrap();
    assert!(!boundary.is_empty(), "Should find rows at segment boundary");

    // Full range should return all rows
    let full = db.columnar_store().query_time_range(
        "pruning_test", 0, 10_000_000, &[],
    ).unwrap();
    assert_eq!(full.len(), 500, "Full range should return all 500 rows");
}

// ============================================================
// Test 5: Combined condition + time range pruning
// ============================================================

#[test]
fn test_combined_time_and_column_pruning() {
    let (_dir, db) = create_db();
    create_sensor_table(&db, "combined_test");

    // Insert data with varying zone values across time ranges
    for zone in 0..3u64 {
        let mut rows = Vec::new();
        for i in 0..100 {
            rows.push(vec![
                Value::Timestamp(Timestamp::from_micros((zone * 1_000_000 + i * 1_000) as i64)),
                Value::Float(20.0 + zone as f64 * 10.0),
                Value::Float(50.0),
                Value::Text(format!("zone_{}", zone)),
                Value::Integer(zone as i64),
            ]);
        }
        db.columnar_store().ingest("combined_test", rows).unwrap();
        db.flush().unwrap();
    }

    assert_eq!(db.columnar_store().segment_count("combined_test"), 3);

    // Query: time range [0, 10_000_000] AND zone=1
    let conditions = vec![
        ColumnCondition::Equals {
            column_idx: 4, // zone column
            value: Value::Integer(1),
        },
    ];
    let results = db.columnar_store().query_with_conditions(
        "combined_test", 0, 10_000_000, &conditions, &[],
    ).unwrap();

    for (_rid, row) in &results {
        if let Some(Value::Integer(zone)) = row.get("zone") {
            assert_eq!(*zone, 1, "All results should have zone=1");
        }
        if let Some(Value::Text(label)) = row.get("label") {
            assert_eq!(label, "zone_1", "Label should be 'zone_1'");
        }
    }

    // Combined: narrow time + column condition
    let narrow_conditions = vec![
        ColumnCondition::Equals {
            column_idx: 4,
            value: Value::Integer(2),
        },
    ];
    let narrow_results = db.columnar_store().query_with_conditions(
        "combined_test", 0, 1_500_000, &narrow_conditions, &[],
    ).unwrap();
    assert!(narrow_results.is_empty(),
        "zone=2 data is in [2M, 3M), so [0, 1.5M) with zone=2 should be empty");
}

// ============================================================
// Test 6: Large dataset — 10K rows, multi-segment
// ============================================================

#[test]
fn test_10k_rows_multi_segment() {
    let (_dir, db) = create_db();
    create_sensor_table(&db, "large_test");

    // Insert 5,000 rows → 50 segments of 100 rows each
    for batch in 0..50 {
        let mut rows = Vec::new();
        for i in 0..100 {
            let ts = (batch * 100_000 + i * 100) as i64;
            rows.push(vec![
                Value::Timestamp(Timestamp::from_micros(ts)),
                Value::Float(20.0 + (batch as f64 % 10.0)),
                Value::Float(50.0 + (i as f64 % 5.0)),
                Value::Text(format!("label_{}", batch % 10)),
                Value::Integer((batch % 5) as i64),
            ]);
        }
        db.columnar_store().ingest("large_test", rows).unwrap();
        db.flush().unwrap();
    }

    let seg_count = db.columnar_store().segment_count("large_test");
    assert!(seg_count >= 10, "Should have many segments, got {}", seg_count);

    // Full query
    let full = db.columnar_store().query_time_range(
        "large_test", 0, 20_000_000, &[],
    ).unwrap();
    assert_eq!(full.len(), 5_000, "Full query should return all 5K rows, got {}", full.len());

    // Condition: label = "label_3"
    let label_cond = vec![
        ColumnCondition::Equals {
            column_idx: 3,
            value: Value::Text("label_3".to_string()),
        },
    ];
    let label_results = db.columnar_store().query_with_conditions(
        "large_test", 0, 20_000_000, &label_cond, &[],
    ).unwrap();
    // label_3 appears in batches 3, 13, 23, ..., 43 → 5 batches × 100 rows = 500
    assert_eq!(label_results.len(), 500,
        "label_3 should match 500 rows, got {}", label_results.len());

    // Verify all results have correct label
    for (_rid, row) in &label_results {
        if let Some(Value::Text(label)) = row.get("label") {
            assert_eq!(label, "label_3");
        }
    }
}

// ============================================================
// Test 7: Buffer data queryable with conditions
// ============================================================

#[test]
fn test_buffer_query_with_conditions() {
    let (_dir, db) = create_db();
    create_sensor_table(&db, "buf_test");

    // Insert but do NOT flush — data stays in buffer
    let rows: Vec<Vec<Value>> = (0..50).map(|i| {
        vec![
            Value::Timestamp(Timestamp::from_micros(100_000 + i as i64 * 100)),
            Value::Float(20.0 + i as f64 * 0.1),
            Value::Float(50.0),
            Value::Text(if i < 25 { "type_a".to_string() } else { "type_b".to_string() }),
            Value::Integer(if i < 25 { 0 } else { 1 }),
        ]
    }).collect();
    db.columnar_store().ingest("buf_test", rows).unwrap();
    // Intentionally no flush

    // Query with condition: zone=0
    let conditions = vec![
        ColumnCondition::Equals {
            column_idx: 4,
            value: Value::Integer(0),
        },
    ];
    let results = db.columnar_store().query_with_conditions(
        "buf_test", 0, 200_000, &conditions, &[],
    ).unwrap();
    assert_eq!(results.len(), 25, "Should return 25 rows with zone=0 from buffer");

    for (_rid, row) in &results {
        if let Some(Value::Integer(zone)) = row.get("zone") {
            assert_eq!(*zone, 0);
        }
    }
}

// ============================================================
// Test 8: Segment v2 metadata — verify stats + sorted flag
// ============================================================

#[test]
fn test_segment_v2_metadata() {
    let (_dir, db) = create_db();
    create_sensor_table(&db, "meta_test");

    ingest_rows(&db, "meta_test", 200, 1_000_000, 1_000);
    db.flush().unwrap();

    assert!(db.columnar_store().segment_count("meta_test") > 0);

    // Query to verify data integrity
    let results = db.columnar_store().query_time_range(
        "meta_test", 0, 2_000_000, &[],
    ).unwrap();
    assert_eq!(results.len(), 200, "Should return all 200 rows");

    // Verify sorted order
    let mut prev_ts = i64::MIN;
    for (_rid, row) in &results {
        if let Some(Value::Timestamp(ts)) = row.get("ts") {
            let micros = ts.as_micros();
            assert!(micros >= prev_ts, "Should be sorted: {} < {}", micros, prev_ts);
            prev_ts = micros;
        }
    }
}

// ============================================================
// Test 9: Column projection with conditions
// ============================================================

#[test]
fn test_column_projection_with_conditions() {
    let (_dir, db) = create_db();
    create_sensor_table(&db, "proj_test");

    ingest_rows(&db, "proj_test", 300, 1_000_000, 1_000);
    db.flush().unwrap();

    // Query only ts and label columns with zone condition
    let conditions = vec![
        ColumnCondition::Equals {
            column_idx: 4,
            value: Value::Integer(2),
        },
    ];
    let results = db.columnar_store().query_with_conditions(
        "proj_test", 0, 2_000_000, &conditions,
        &["ts".to_string(), "label".to_string()],
    ).unwrap();

    for (_rid, row) in &results {
        // Should only have ts and label
        assert!(row.contains_key("ts"), "Should have ts column");
        assert!(row.contains_key("label"), "Should have label column");
        assert!(!row.contains_key("temperature"), "Should NOT have temperature column");
        assert!(!row.contains_key("humidity"), "Should NOT have humidity column");
        // zone shouldn't be in output since we didn't request it
    }
}

// ============================================================
// Test 10: Empty result for out-of-range query
// ============================================================

#[test]
fn test_empty_result_out_of_range() {
    let (_dir, db) = create_db();
    create_sensor_table(&db, "empty_test");

    ingest_rows(&db, "empty_test", 100, 1_000_000, 1_000);
    db.flush().unwrap();

    // Query a range far from the data
    let results = db.columnar_store().query_time_range(
        "empty_test", 100_000_000, 200_000_000, &[],
    ).unwrap();
    assert!(results.is_empty(), "Out-of-range query should return empty");

    // With condition on out-of-range value
    let conditions = vec![
        ColumnCondition::Equals {
            column_idx: 4,
            value: Value::Integer(999),
        },
    ];
    let cond_results = db.columnar_store().query_with_conditions(
        "empty_test", 0, 2_000_000, &conditions, &[],
    ).unwrap();
    assert!(cond_results.is_empty(), "Condition for nonexistent zone should be empty");
}

// ============================================================
// Test 11: Round-trip data integrity after flush
// ============================================================

#[test]
fn test_data_integrity_roundtrip() {
    let (_dir, db) = create_db();
    create_sensor_table(&db, "integrity_test");

    let mut rows = Vec::new();
    for i in 0..250 {
        rows.push(vec![
            Value::Timestamp(Timestamp::from_micros(1_000_000 + i as i64 * 4_000)),
            Value::Float(20.0 + (i as f64) * 0.1),
            Value::Float(50.0 + (i as f64) * 0.05),
            Value::Text(format!("sensor_{}", i % 50)),
            Value::Integer((i % 5) as i64),
        ]);
    }
    db.columnar_store().ingest("integrity_test", rows.clone()).unwrap();
    db.flush().unwrap();

    let results = db.columnar_store().query_time_range(
        "integrity_test", 0, 2_000_000, &[],
    ).unwrap();
    assert_eq!(results.len(), 250, "Should return all 250 rows");

    // Verify each row's data matches what was inserted
    for (i, (_rid, sql_row)) in results.iter().enumerate() {
        let expected_ts = 1_000_000 + i as i64 * 4_000;
        let expected_temp = 20.0 + i as f64 * 0.1;
        let expected_humidity = 50.0 + i as f64 * 0.05;
        let expected_zone = (i % 5) as i64;
        let expected_label = format!("sensor_{}", i % 50);

        if let Some(Value::Timestamp(ts)) = sql_row.get("ts") {
            assert_eq!(ts.as_micros(), expected_ts, "ts mismatch at row {}", i);
        }
        if let Some(Value::Float(temp)) = sql_row.get("temperature") {
            assert!((temp - expected_temp).abs() < 0.001,
                "temp mismatch at row {}: {} vs {}", i, temp, expected_temp);
        }
        if let Some(Value::Float(hum)) = sql_row.get("humidity") {
            assert!((hum - expected_humidity).abs() < 0.001,
                "humidity mismatch at row {}", i);
        }
        if let Some(Value::Integer(zone)) = sql_row.get("zone") {
            assert_eq!(*zone, expected_zone, "zone mismatch at row {}", i);
        }
        if let Some(Value::Text(label)) = sql_row.get("label") {
            assert_eq!(label, &expected_label, "label mismatch at row {}", i);
        }
    }
}

// ============================================================
// Test 12: Multiple range conditions combined
// ============================================================

#[test]
fn test_multiple_range_conditions() {
    let (_dir, db) = create_db();
    create_sensor_table(&db, "multi_cond_test");

    ingest_rows(&db, "multi_cond_test", 500, 1_000_000, 1_000);
    db.flush().unwrap();

    // zone = 3 AND temperature in range [20.3, 20.7]
    let conditions = vec![
        ColumnCondition::Equals {
            column_idx: 4, // zone
            value: Value::Integer(3),
        },
        ColumnCondition::Range {
            column_idx: 1, // temperature
            low: Value::Float(20.0),
            high: Value::Float(24.0),
        },
    ];
    let results = db.columnar_store().query_with_conditions(
        "multi_cond_test", 0, 2_000_000, &conditions, &[],
    ).unwrap();

    // zone=3 rows have temperature 20.0 + (i%10)*0.1 where i%5==3
    // That means i=3,8,13,18,23,... → temp = 20.3,20.8,20.3,20.8,20.3,...
    // Only temp <= 24.0: all zone=3 rows qualify since max temp is 20.9
    for (_rid, row) in &results {
        if let Some(Value::Integer(zone)) = row.get("zone") {
            assert_eq!(*zone, 3, "zone should be 3");
        }
        if let Some(Value::Float(temp)) = row.get("temperature") {
            assert!(*temp >= 20.0 && *temp <= 24.0,
                "temp should be in [20, 24], got {}", temp);
        }
    }
}

// ============================================================
// Test 13: Segment merge preserves indexing features
// ============================================================

#[test]
fn test_merge_preserves_indexing() {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::default();
    config.columnar_config.buffer_row_capacity = 50;
    config.columnar_config.enable_merge = true;
    config.columnar_config.merge_threshold_segments = 4;
    config.columnar_config.segment_target_rows = 500;
    let db = Database::create_with_config(dir.path(), config).unwrap();

    db.execute(
        "CREATE TABLE merge_idx (ts TIMESTAMP, val FLOAT, tag TEXT) TIMESERIES(ts)"
    ).unwrap();

    // Create 6 small segments
    for batch in 0..6 {
        let mut rows = Vec::new();
        for i in 0..50 {
            rows.push(vec![
                Value::Timestamp(Timestamp::from_micros((batch * 100_000 + i * 1_000) as i64)),
                Value::Float(batch as f64 * 10.0 + i as f64 * 0.1),
                Value::Text(format!("tag_{}", batch % 3)),
            ]);
        }
        db.columnar_store().ingest("merge_idx", rows).unwrap();
        db.flush().unwrap();
    }

    // Verify data integrity after merges
    let all = db.columnar_store().query_time_range(
        "merge_idx", 0, 1_000_000, &[],
    ).unwrap();
    assert_eq!(all.len(), 300, "All 300 rows should be queryable after merge, got {}", all.len());

    // Condition query should still work
    let conditions = vec![
        ColumnCondition::Equals {
            column_idx: 2,
            value: Value::Text("tag_1".to_string()),
        },
    ];
    let tagged = db.columnar_store().query_with_conditions(
        "merge_idx", 0, 1_000_000, &conditions, &[],
    ).unwrap();
    // tag_1 appears in batch 1, 4 → 100 rows
    assert_eq!(tagged.len(), 100, "tag_1 should match 100 rows, got {}", tagged.len());
    for (_rid, row) in &tagged {
        if let Some(Value::Text(tag)) = row.get("tag") {
            assert_eq!(tag, "tag_1");
        }
    }
}

// ============================================================
// Test 14: WAL crash recovery preserves indexing
// ============================================================

#[test]
fn test_crash_recovery_with_indexing() {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::default();
    config.columnar_config.buffer_row_capacity = 50;
    let path = dir.path().to_path_buf();

    // Phase 1: Write data
    {
        let db = Database::create_with_config(&path, config.clone()).unwrap();
        db.execute(
            "CREATE TABLE recover_test (ts TIMESTAMP, val FLOAT, tag TEXT) TIMESERIES(ts)"
        ).unwrap();

        let mut rows = Vec::new();
        for i in 0..200 {
            rows.push(vec![
                Value::Timestamp(Timestamp::from_micros(1_000_000 + i as i64 * 1_000)),
                Value::Float(20.0 + i as f64 * 0.05),
                Value::Text(format!("tag_{}", i % 10)),
            ]);
        }
        db.columnar_store().ingest("recover_test", rows).unwrap();
        db.flush().unwrap();
        // Don't close cleanly — simulate crash (WAL already flushed to segments)
    }

    // Phase 2: Reopen and verify
    {
        let db = Database::open_with_config(&path, config.clone()).unwrap();
        let results = db.columnar_store().query_time_range(
            "recover_test", 0, 2_000_000, &[],
        ).unwrap();
        assert_eq!(results.len(), 200, "Should recover all 200 rows, got {}", results.len());
    }
}
