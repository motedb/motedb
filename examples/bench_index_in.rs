//! Benchmark: IN subquery with and without column index acceleration.
//! Tests both large IN (subquery) and small IN (literal list) patterns.

use motedb::{Database, DBConfig};
use tempfile::TempDir;
use std::time::Instant;

fn main() {
    let dir = TempDir::new().expect("temp dir");
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).expect("create db");

    db.execute("CREATE TABLE sales (id INT PRIMARY KEY AUTO_INCREMENT, customer TEXT, amount FLOAT, region TEXT)").unwrap();

    // Insert 100K rows
    let n = 100_000;
    let mut batch = String::with_capacity(n * 60);
    for i in 0..n {
        let region = match i % 3 { 0 => "'US'", 1 => "'EU'", _ => "'APAC'" };
        let customer = format!("'cust_{}'", i % 100);
        let amount = (i as f64 * 1.7 + 42.0) % 1000.0;
        batch.push_str(&format!("({},{:.2},{}),", customer, amount, region));
        if batch.len() > 500_000 || i == n - 1 {
            batch.truncate(batch.len() - 1);
            db.execute(&format!("INSERT INTO sales (customer, amount, region) VALUES {}", batch)).unwrap();
            batch.clear();
        }
    }
    db.flush().ok();

    let iters = 10;

    // ── Test 1: Small IN literal list (5 values, ~5K matching rows = 5%) ──
    // This is the sweet spot for index acceleration.
    let sql_small = "SELECT * FROM sales WHERE customer IN ('cust_1','cust_5','cust_10','cust_20','cust_50')";

    println!("\n  IN Subquery Index Benchmark (100K rows)");
    println!("  {}", "=".repeat(60));

    // Without index
    let warmup = db.execute(sql_small).unwrap().materialize().unwrap();
    drop(warmup);
    let mut total_no = 0u128;
    for _ in 0..iters {
        let start = Instant::now();
        let result = db.execute(sql_small).unwrap().materialize().unwrap();
        total_no += start.elapsed().as_micros();
        let rows = match &result { motedb::QueryResult::Select { rows, .. } => rows.len(), _ => 0 };
        println!("  No index:   {} us ({} rows)", start.elapsed().as_micros(), rows);
        drop(result);
    }
    let avg_no = total_no / iters as u128;

    // Create index
    println!("\n  Creating column index on sales.customer...");
    let start = Instant::now();
    db.execute("CREATE INDEX idx_customer ON sales (customer) USING COLUMN").unwrap();
    println!("  Index built in {} ms", start.elapsed().as_millis());

    // With index
    let warmup = db.execute(sql_small).unwrap().materialize().unwrap();
    drop(warmup);
    let mut total_idx = 0u128;
    for _ in 0..iters {
        let start = Instant::now();
        let result = db.execute(sql_small).unwrap().materialize().unwrap();
        total_idx += start.elapsed().as_micros();
        let rows = match &result { motedb::QueryResult::Select { rows, .. } => rows.len(), _ => 0 };
        println!("  With index: {} us ({} rows)", start.elapsed().as_micros(), rows);
        drop(result);
    }
    let avg_idx = total_idx / iters as u128;

    let speedup = avg_no as f64 / avg_idx as f64;
    println!("\n  {}", "-".repeat(60));
    println!("  Small IN (5 values, ~5K rows):");
    println!("    No index:   {} us (avg of {})", avg_no, iters);
    println!("    With index: {} us (avg of {})", avg_idx, iters);
    println!("    Speedup:    {:.1}x", speedup);

    if speedup > 1.3 {
        println!("    ✅ Index acceleration effective!");
    } else if speedup > 0.9 {
        println!("    ⚠️  Marginal (index overhead ≈ scan cost at this scale)");
    } else {
        println!("    ❌ Index path slower");
    }

    // ── Test 2: Large IN subquery (33+ values, falls through to full scan) ──
    let sql_large = "SELECT * FROM sales WHERE customer IN (SELECT customer FROM sales WHERE region = 'US')";

    println!("\n  Large IN subquery (33+ unique values, ~33K rows):");
    let warmup = db.execute(sql_large).unwrap().materialize().unwrap();
    drop(warmup);

    let mut total_large = 0u128;
    for _ in 0..5 {
        let start = Instant::now();
        let result = db.execute(sql_large).unwrap().materialize().unwrap();
        total_large += start.elapsed().as_micros();
        let rows = match &result { motedb::QueryResult::Select { rows, .. } => rows.len(), _ => 0 };
        println!("    {} us ({} rows)", start.elapsed().as_micros(), rows);
        drop(result);
    }
    println!("    Avg: {} us (correctly falls through to full scan with HashSet)", total_large / 5);

    // ── Test 3: Medium IN list (10 values, ~10K rows = 10%) ──
    let sql_med = "SELECT * FROM sales WHERE customer IN ('cust_1','cust_2','cust_3','cust_4','cust_5','cust_6','cust_7','cust_8','cust_9','cust_10')";

    println!("\n  Medium IN (10 values, ~10K rows):");
    let mut total_med = 0u128;
    for _ in 0..iters {
        let start = Instant::now();
        let result = db.execute(sql_med).unwrap().materialize().unwrap();
        total_med += start.elapsed().as_micros();
        drop(result);
    }
    println!("    With index: {} us (avg)", total_med / iters as u128);
}
