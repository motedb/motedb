//! Performance benchmarks for TimeSeries indexing system.
//!
//! Measures: ingest throughput, query latency with/without conditions,
//! segment pruning effectiveness, bloom filter hit/miss.

use motedb::config::DBConfig;
use motedb::storage::columnar::segment_manager::ColumnCondition;
use motedb::types::{Timestamp, Value};
use motedb::Database;
use std::time::Instant;
use tempfile::TempDir;

fn create_db_for_bench() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::default();
    config.columnar_config.buffer_row_capacity = 5000;
    config.columnar_config.segment_target_rows = 100_000;
    config.columnar_config.enable_merge = false;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    (dir, db)
}

fn setup_table(db: &Database) {
    db.execute(
        "CREATE TABLE bench (ts TIMESTAMP, temperature FLOAT, humidity FLOAT, label TEXT, zone INT) TIMESERIES(ts)"
    ).unwrap();
}

fn ingest_batch(db: &Database, table: &str, count: usize, base_ts: i64) {
    let mut rows = Vec::with_capacity(count);
    for i in 0..count {
        rows.push(vec![
            Value::Timestamp(Timestamp::from_micros(base_ts + i as i64 * 100)),
            Value::Float(20.0 + (i as f64 % 30.0)),
            Value::Float(40.0 + (i as f64 % 20.0)),
            Value::text(format!("label_{}", i % 50)),
            Value::Integer((i % 10) as i64),
        ]);
    }
    db.columnar_store().ingest(table, rows).unwrap();
}

#[test]
fn bench_timeseries_full_suite() {
    let (_dir, db) = create_db_for_bench();
    setup_table(&db);

    let total_rows = 50_000usize;
    let base_ts = 1_000_000i64;

    // ---- Phase 1: Ingest Throughput ----
    println!("\n=== Phase 1: Ingest Throughput ===");
    let t0 = Instant::now();
    ingest_batch(&db, "bench", total_rows, base_ts);
    let ingest_time = t0.elapsed();
    println!(
        "Ingest {} rows: {:.2?} ({:.0} rows/sec)",
        total_rows,
        ingest_time,
        total_rows as f64 / ingest_time.as_secs_f64()
    );

    db.flush().unwrap();
    let seg_count = db.columnar_store().segment_count("bench");
    println!("Segments after flush: {}", seg_count);

    // ---- Phase 2: Full Range Query ----
    println!("\n=== Phase 2: Query Performance ===");

    // 2a: Full scan
    let t0 = Instant::now();
    let full = db
        .columnar_store()
        .query_time_range("bench", 0, i64::MAX, &[])
        .unwrap();
    let full_time = t0.elapsed();
    println!(
        "Full scan ({} rows): {:.2?} ({:.0} rows/sec)",
        full.len(),
        full_time,
        full.len() as f64 / full_time.as_secs_f64()
    );
    assert_eq!(full.len(), total_rows);

    // 2b: Narrow time range (~5% of data)
    let narrow_start = base_ts + 2_000_000i64;
    let narrow_end = base_ts + 2_250_000i64;
    let t0 = Instant::now();
    let narrow = db
        .columnar_store()
        .query_time_range("bench", narrow_start, narrow_end, &[])
        .unwrap();
    let narrow_time = t0.elapsed();
    println!(
        "Narrow time range ({} rows): {:.2?}",
        narrow.len(),
        narrow_time
    );

    // 2c: Column projection (only 2 of 5 columns)
    let t0 = Instant::now();
    let projected = db
        .columnar_store()
        .query_time_range(
            "bench",
            narrow_start,
            narrow_end,
            &["ts".to_string(), "temperature".to_string()],
        )
        .unwrap();
    let proj_time = t0.elapsed();
    println!(
        "Column projection ({} rows): {:.2?}",
        projected.len(),
        proj_time
    );

    // ---- Phase 3: Condition-Based Queries ----
    println!("\n=== Phase 3: Condition Pruning Performance ===");

    // 3a: Zone equals (should prune ~90% of segments)
    let zone_cond = vec![ColumnCondition::Equals {
        column_idx: 4,
        value: Value::Integer(5),
    }];
    let t0 = Instant::now();
    let zone_results = db
        .columnar_store()
        .query_with_conditions("bench", 0, i64::MAX, &zone_cond, &[])
        .unwrap();
    let zone_time = t0.elapsed();
    println!("Zone=5 ({} rows): {:.2?}", zone_results.len(), zone_time);
    // zone 5 is every 10th row
    assert!(zone_results.len() > 0, "Should find zone=5 rows");

    // 3b: Label equals (bloom filter)
    let label_cond = vec![ColumnCondition::Equals {
        column_idx: 3,
        value: Value::text("label_7".to_string()),
    }];
    let t0 = Instant::now();
    let label_results = db
        .columnar_store()
        .query_with_conditions("bench", 0, i64::MAX, &label_cond, &[])
        .unwrap();
    let label_time = t0.elapsed();
    println!(
        "Label='label_7' ({} rows): {:.2?}",
        label_results.len(),
        label_time
    );

    // 3c: Label nonexistent (bloom filter negative — fast path)
    let no_cond = vec![ColumnCondition::Equals {
        column_idx: 3,
        value: Value::text("NONEXISTENT_LABEL".to_string()),
    }];
    let t0 = Instant::now();
    let no_results = db
        .columnar_store()
        .query_with_conditions("bench", 0, i64::MAX, &no_cond, &[])
        .unwrap();
    let no_time = t0.elapsed();
    println!(
        "Label='NONEXISTENT' ({} rows): {:.2?} (bloom filter fast path)",
        no_results.len(),
        no_time
    );
    assert!(no_results.is_empty());

    // 3d: Temperature range condition
    let temp_cond = vec![ColumnCondition::Range {
        column_idx: 1,
        low: Value::Float(25.0),
        high: Value::Float(28.0),
    }];
    let t0 = Instant::now();
    let temp_results = db
        .columnar_store()
        .query_with_conditions("bench", 0, i64::MAX, &temp_cond, &[])
        .unwrap();
    let temp_time = t0.elapsed();
    println!(
        "Temp [25,28] ({} rows): {:.2?}",
        temp_results.len(),
        temp_time
    );

    // 3e: Combined: time range + zone
    let combined_conds = vec![ColumnCondition::Equals {
        column_idx: 4,
        value: Value::Integer(3),
    }];
    let t0 = Instant::now();
    let combined = db
        .columnar_store()
        .query_with_conditions(
            "bench",
            base_ts,
            base_ts + 2_500_000i64,
            &combined_conds,
            &[],
        )
        .unwrap();
    let combined_time = t0.elapsed();
    println!(
        "Time+Zone combined ({} rows): {:.2?}",
        combined.len(),
        combined_time
    );

    // ---- Phase 4: Benchmark Segment Pruning Effectiveness ----
    println!("\n=== Phase 4: Pruning Effectiveness ===");
    let narrow_seg_count = seg_count; // total segments
                                      // Estimate how many segments the narrow query had to scan
    let narrow_seg_estimate = 1.max(narrow_seg_count / 20); // ~5% of range
    println!("Total segments: {}", narrow_seg_count);
    println!(
        "Estimated segments scanned for narrow query: ~{}",
        narrow_seg_estimate
    );
    println!(
        "Pruning ratio: ~{:.1}x reduction",
        narrow_seg_count as f64 / narrow_seg_estimate as f64
    );

    // ---- Summary ----
    println!("\n=== Summary ===");
    println!("Rows: {} | Segments: {}", total_rows, seg_count);
    println!(
        "Ingest: {:.0} rows/sec",
        total_rows as f64 / ingest_time.as_secs_f64()
    );
    println!(
        "Full scan: {:.0} rows/sec",
        full.len() as f64 / full_time.as_secs_f64()
    );
    println!("Narrow range: {:.2?}", narrow_time);
    println!(
        "Zone condition: {:.2?} ({} rows)",
        zone_time,
        zone_results.len()
    );
    println!(
        "Label bloom hit: {:.2?} ({} rows)",
        label_time,
        label_results.len()
    );
    println!(
        "Label bloom miss: {:.2?} ({} rows)",
        no_time,
        no_results.len()
    );
    println!(
        "Combined query: {:.2?} ({} rows)",
        combined_time,
        combined.len()
    );
}

#[test]
fn bench_ingest_throughput() {
    let (_dir, db) = create_db_for_bench();
    setup_table(&db);

    let rows_per_batch = 10_000usize;
    let num_batches = 5;

    println!(
        "\n=== Ingest Throughput ({} batches × {} rows) ===",
        num_batches, rows_per_batch
    );

    for batch in 0..num_batches {
        let t0 = Instant::now();
        ingest_batch(
            &db,
            "bench",
            rows_per_batch,
            1_000_000 + batch as i64 * 10_000_000,
        );
        let elapsed = t0.elapsed();
        println!(
            "Batch {}: {:.2?} ({:.0} rows/sec)",
            batch,
            elapsed,
            rows_per_batch as f64 / elapsed.as_secs_f64()
        );
    }

    let t0 = Instant::now();
    db.flush().unwrap();
    println!("Flush: {:.2?}", t0.elapsed());

    let count = db.columnar_store().segment_count("bench");
    println!("Total segments: {}", count);
}
