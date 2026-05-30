//! Benchmark: Parallel scan with rayon vs sequential scan.
//! Measures speedup for CPU-intensive full table scans with WHERE filtering.

use motedb::{Database, DBConfig};
use tempfile::TempDir;
use std::time::Instant;

fn main() {
    let dir = TempDir::new().expect("temp dir");
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).expect("create db");

    db.execute("CREATE TABLE sales (
        id INT PRIMARY KEY AUTO_INCREMENT,
        customer TEXT,
        amount FLOAT,
        region TEXT,
        category TEXT,
        status TEXT
    )").unwrap();

    // Insert rows in batches
    let n = 100_000;
    println!("Inserting {} rows...", n);
    let mut batch = String::with_capacity(n * 80);
    for i in 0..n {
        let region = match i % 4 { 0 => "'US'", 1 => "'EU'", 2 => "'APAC'", _ => "'LATAM'" };
        let customer = format!("'cust_{}'", i % 500);
        let amount = (i as f64 * 1.7 + 42.0) % 1000.0;
        let category = match i % 10 { 0 => "'A'", 1 => "'B'", 2 => "'C'", 3 => "'D'", 4 => "'E'", _ => "'F'" };
        let status = if i % 3 == 0 { "'active'" } else { "'inactive'" };
        batch.push_str(&format!("({},{:.2},{},{},{}),",
            customer, amount, region, category, status));
        if batch.len() > 500_000 || i == n - 1 {
            batch.truncate(batch.len() - 1);
            db.execute(&format!(
                "INSERT INTO sales (customer, amount, region, category, status) VALUES {}", batch
            )).unwrap();
            batch.clear();
        }
    }
    db.flush().ok();
    println!("Done inserting.\n");

    let iters = 20;

    // ── Benchmark 1: Selective WHERE filter (25% selectivity) ──
    println!("═══ Benchmark 1: WHERE region='US' (25% selectivity, ~25K rows) ═══");
    let sql1 = "SELECT * FROM sales WHERE region = 'US'";

    // Warmup
    let _ = db.execute(sql1).unwrap().materialize().unwrap();

    let mut times: Vec<u128> = Vec::new();
    for i in 0..iters {
        let start = Instant::now();
        let result = db.execute(sql1).unwrap().materialize().unwrap();
        let elapsed = start.elapsed().as_micros();
        times.push(elapsed);
        if i < 3 {
            let rows = match &result {
                motedb::QueryResult::Select { rows, .. } => rows.len(),
                _ => 0,
            };
            println!("  Run {}: {} μs ({} rows)", i + 1, elapsed, rows);
        }
        drop(result);
    }
    times.sort_unstable();
    let median = times[iters / 2];
    let min = times[0];
    let max = times[iters - 1];
    println!("  Median: {} μs (min: {}, max: {})", median, min, max);
    println!("  Throughput: {:.0} rows/sec", 25_000.0 / (median as f64 / 1_000_000.0));

    // ── Benchmark 2: Even more selective (10% selectivity) ──
    println!("\n═══ Benchmark 2: WHERE category='A' (10% selectivity, ~10K rows) ═══");
    let sql2 = "SELECT * FROM sales WHERE category = 'A'";

    let _ = db.execute(sql2).unwrap().materialize().unwrap();

    let mut times2: Vec<u128> = Vec::new();
    for i in 0..iters {
        let start = Instant::now();
        let result = db.execute(sql2).unwrap().materialize().unwrap();
        let elapsed = start.elapsed().as_micros();
        times2.push(elapsed);
        if i < 3 {
            let rows = match &result {
                motedb::QueryResult::Select { rows, .. } => rows.len(),
                _ => 0,
            };
            println!("  Run {}: {} μs ({} rows)", i + 1, elapsed, rows);
        }
        drop(result);
    }
    times2.sort_unstable();
    let median2 = times2[iters / 2];
    println!("  Median: {} μs", median2);

    // ── Benchmark 3: Complex AND condition ──
    println!("\n═══ Benchmark 3: WHERE region='US' AND status='active' (~8.3K rows) ═══");
    let sql3 = "SELECT * FROM sales WHERE region = 'US' AND status = 'active'";

    let _ = db.execute(sql3).unwrap().materialize().unwrap();

    let mut times3: Vec<u128> = Vec::new();
    for i in 0..iters {
        let start = Instant::now();
        let result = db.execute(sql3).unwrap().materialize().unwrap();
        let elapsed = start.elapsed().as_micros();
        times3.push(elapsed);
        if i < 3 {
            let rows = match &result {
                motedb::QueryResult::Select { rows, .. } => rows.len(),
                _ => 0,
            };
            println!("  Run {}: {} μs ({} rows)", i + 1, elapsed, rows);
        }
        drop(result);
    }
    times3.sort_unstable();
    let median3 = times3[iters / 2];
    println!("  Median: {} μs", median3);

    // ── Benchmark 4: Full scan (no WHERE — tests baseline) ──
    println!("\n═══ Benchmark 4: SELECT * (full scan, no WHERE, 100K rows) ═══");
    let sql4 = "SELECT * FROM sales";

    let _ = db.execute(sql4).unwrap().materialize().unwrap();

    let mut times4: Vec<u128> = Vec::new();
    for _ in 0..5 {
        let start = Instant::now();
        let result = db.execute(sql4).unwrap().materialize().unwrap();
        let elapsed = start.elapsed().as_micros();
        times4.push(elapsed);
        drop(result);
    }
    times4.sort_unstable();
    let median4 = times4[2];
    println!("  Median: {} μs", median4);

    // ── Summary ──
    println!("\n═══ Summary ═══");
    println!("  Filter 25%:  {} μs median", median);
    println!("  Filter 10%:  {} μs median", median2);
    println!("  Filter AND:  {} μs median", median3);
    println!("  Full scan:   {} μs median", median4);

    #[cfg(feature = "rayon")]
    println!("\n  ✅ Rayon parallel scan enabled (par_chunks for WHERE queries)");
    #[cfg(not(feature = "rayon"))]
    println!("\n  ⚠️  Rayon disabled — sequential scan only");
}
