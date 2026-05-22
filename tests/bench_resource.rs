//! Resource & Latency Diagnostic Benchmark
//!
//! Measures: memory footprint, query latency distribution (p50/p95/p99), CPU overhead
//!
//! Run: cargo test --test bench_resource --release -- --nocapture --test-threads=1

use motedb::{Database, DBConfig};
use tempfile::TempDir;
use std::time::Instant;

fn is_ci() -> bool { std::env::var("CI").is_ok() }

fn edge_config() -> DBConfig {
    DBConfig::for_edge()
}

fn create_db() -> (Database, TempDir) {
    let dir = TempDir::new().expect("temp dir");
    let db = Database::create_with_config(dir.path(), edge_config()).expect("create db");
    (db, dir)
}

fn exec(db: &Database, sql: &str) -> motedb::sql::QueryResult {
    db.execute(sql).expect("execute SQL").materialize().expect("materialize")
}

// ============================================================================
// Memory measurement utilities
// ============================================================================

fn get_process_memory_kb() -> (usize, usize) {
    // Returns (RSS kb, VMS kb)
    #[cfg(target_os = "macos")]
    {
        let pid = std::process::id();
        let output = std::process::Command::new("ps")
            .args(["-o", "rss,vsz", "-p", &pid.to_string()])
            .output()
            .ok();
        if let Some(out) = output {
            let stdout = String::from_utf8_lossy(&out.stdout);
            for line in stdout.lines().skip(1) {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    let rss: usize = parts[0].parse().unwrap_or(0);
                    let vms: usize = parts[1].parse().unwrap_or(0);
                    return (rss, vms);
                }
            }
        }
        (0, 0)
    }
    #[cfg(not(target_os = "macos"))]
    {
        (0, 0)
    }
}

fn print_memory(label: &str) -> (usize, usize) {
    let (rss, vms) = get_process_memory_kb();
    println!("  {:<50} | RSS: {:>8} KB ({:>5.1} MB) | VMS: {:>8} KB ({:>5.1} MB)",
        label, rss, rss as f64 / 1024.0, vms, vms as f64 / 1024.0);
    (rss, vms)
}

// ============================================================================
// Latency distribution
// ============================================================================

fn print_latency_distribution(label: &str, latencies_us: &[u64]) {
    if latencies_us.is_empty() {
        println!("  {:<50} | No data", label);
        return;
    }

    let mut sorted = latencies_us.to_vec();
    sorted.sort_unstable();

    let n = sorted.len();
    let p50 = sorted[n * 50 / 100];
    let p75 = sorted[n * 75 / 100];
    let p90 = sorted[n * 90 / 100];
    let p95 = sorted[n * 95 / 100];
    let p99 = sorted[n * 99 / 100];
    let min = sorted[0];
    let max = sorted[n - 1];
    let avg: u64 = sorted.iter().sum::<u64>() / n as u64;

    println!("  {:<50} | min={:>6}µs  p50={:>6}µs  p75={:>6}µs  p90={:>6}µs  p95={:>6}µs  p99={:>6}µs  max={:>6}µs  avg={:>6}µs",
        label, min, p50, p75, p90, p95, p99, max, avg);
}

fn print_separator() {
    println!("  {}", "─".repeat(130));
}

// ============================================================================
// Test 1: Memory Footprint at Different Scales
// ============================================================================

#[test]
fn bench_memory_footprint() {
    println!("\n{}", "=".repeat(130));
    println!("  Memory Footprint at Different Scales");
    println!("{}", "=".repeat(130));

    let ci = is_ci();
    let rows_small: i64  = if ci {  2_000 } else { 10_000 };
    let rows_medium: i64 = if ci {  5_000 } else { 50_000 };
    let rows_large: i64  = if ci { 10_000 } else { 100_000 };

    let (rss_baseline, _) = print_memory("Baseline (empty DB)");

    // Small batch
    {
        let (db, _dir) = create_db();
        exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT, tag TEXT, value INTEGER)");
        for i in 1..=rows_small {
            exec(&db, &format!("INSERT INTO t VALUES ({}, 'name_{}', {:.1}, 'tag_{}', {})",
                i, i, i as f64 * 1.5, i % 10, i * 10));
        }
        let (rss_small, _) = print_memory(&format!("After INSERT {} rows (5 cols, MemTable)", rows_small));
        println!("  → ΔRSS: {} KB ({:.1} MB) for {} rows = {:.1} bytes/row",
            rss_small - rss_baseline, (rss_small - rss_baseline) as f64 / 1024.0,
            rows_small,
            (rss_small - rss_baseline) as f64 * 1024.0 / rows_small as f64);
    }

    // Medium batch
    {
        let (db, _dir) = create_db();
        exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT, tag TEXT, value INTEGER)");
        for i in 1..=rows_medium {
            exec(&db, &format!("INSERT INTO t VALUES ({}, 'name_{}', {:.1}, 'tag_{}', {})",
                i, i, i as f64 * 1.5, i % 10, i * 10));
        }
        let (rss_medium, _) = print_memory(&format!("After INSERT {} rows (5 cols, MemTable)", rows_medium));

        // Flush to SSTable
        db.flush().expect("flush");
        db.wait_for_indexes_ready();
        let (rss_medium_sst, _) = print_memory(&format!("After flush {} → SSTable", rows_medium));
        println!("  → ΔRSS MemTable: {} KB ({:.1} MB) = {:.1} bytes/row",
            rss_medium - rss_baseline, (rss_medium - rss_baseline) as f64 / 1024.0,
            (rss_medium - rss_baseline) as f64 * 1024.0 / rows_medium as f64);
        println!("  → ΔRSS SSTable:  {} KB ({:.1} MB) = {:.1} bytes/row",
            rss_medium_sst - rss_baseline, (rss_medium_sst - rss_baseline) as f64 / 1024.0,
            (rss_medium_sst - rss_baseline) as f64 * 1024.0 / rows_medium as f64);

        // Checkpoint + drop
        db.checkpoint().expect("checkpoint");
        drop(db);
        let (_rss_after_drop, _) = print_memory("After checkpoint + drop DB");
    }

    // Large batch
    {
        let (db, _dir) = create_db();
        exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT, tag TEXT, value INTEGER)");
        let start = Instant::now();
        for i in 1..=rows_large {
            exec(&db, &format!("INSERT INTO t VALUES ({}, 'name_{}', {:.1}, 'tag_{}', {})",
                i, i, i as f64 * 1.5, i % 10, i * 10));
        }
        let insert_ms = start.elapsed().as_millis();
        let (rss_large, _) = print_memory(&format!("After INSERT {} rows (MemTable)", rows_large));
        println!("  → Insert: {}ms, {:.0} ops/s", insert_ms, rows_large as f64 / (insert_ms as f64 / 1000.0));
        println!("  → ΔRSS: {} KB ({:.1} MB) = {:.1} bytes/row",
            rss_large - rss_baseline, (rss_large - rss_baseline) as f64 / 1024.0,
            (rss_large - rss_baseline) as f64 * 1024.0 / rows_large as f64);
    }
}

// ============================================================================
// Test 2: Query Latency Distribution (PK, Range, Full Scan)
// ============================================================================

#[test]
fn bench_query_latency() {
    println!("\n{}", "=".repeat(130));
    println!("  Query Latency Distribution (p50/p95/p99)");
    println!("{}", "=".repeat(130));

    let ci = is_ci();
    let seed_rows: i64   = if ci {  5_000 } else { 30_000 };
    let pk_queries: i64  = if ci {  1_000 } else {  5_000 };
    let idx_queries: usize = if ci {    100 } else {    500 };
    let scan_queries: usize = if ci {     10 } else {     50 };

    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT, tag TEXT, value INTEGER)");

    // Seed rows
    for i in 1..=seed_rows {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'name_{}', {:.1}, 'tag_{}', {})",
            i, i, i as f64 * 1.5, i % 10, i * 10));
    }

    println!("\n  --- Phase 1: PK Point Query (MemTable, {} rows) ---", seed_rows);
    print_separator();

    // PK queries — latency per operation
    let mut pk_latencies: Vec<u64> = Vec::with_capacity(pk_queries as usize);
    for i in 1..=pk_queries {
        let start = Instant::now();
        exec(&db, &format!("SELECT * FROM t WHERE id = {}", i));
        pk_latencies.push(start.elapsed().as_micros() as u64);
    }
    print_latency_distribution(&format!("PK SELECT * (MemTable, {} queries)", pk_queries), &pk_latencies);

    // PK queries with specific columns
    let mut pk_proj_latencies: Vec<u64> = Vec::with_capacity(pk_queries as usize);
    for i in 1..=pk_queries {
        let start = Instant::now();
        exec(&db, &format!("SELECT name, score FROM t WHERE id = {}", i));
        pk_proj_latencies.push(start.elapsed().as_micros() as u64);
    }
    print_latency_distribution(&format!("PK SELECT 2/5 cols (MemTable, {} queries)", pk_queries), &pk_proj_latencies);

    // Flush to SSTable
    db.flush().expect("flush");
    db.wait_for_indexes_ready();

    println!("\n  --- Phase 2: PK Point Query (SSTable, {} rows) ---", seed_rows);
    print_separator();

    let mut pk_sst_latencies: Vec<u64> = Vec::with_capacity(pk_queries as usize);
    for i in 1..=pk_queries {
        let start = Instant::now();
        exec(&db, &format!("SELECT * FROM t WHERE id = {}", i));
        pk_sst_latencies.push(start.elapsed().as_micros() as u64);
    }
    print_latency_distribution(&format!("PK SELECT * (SSTable cold, {} queries)", pk_queries), &pk_sst_latencies);

    // Warm cache pass
    let mut pk_warm_latencies: Vec<u64> = Vec::with_capacity(pk_queries as usize);
    for i in 1..=pk_queries {
        let start = Instant::now();
        exec(&db, &format!("SELECT * FROM t WHERE id = {}", i));
        pk_warm_latencies.push(start.elapsed().as_micros() as u64);
    }
    print_latency_distribution(&format!("PK SELECT * (SSTable warm, {} queries)", pk_queries), &pk_warm_latencies);

    println!("\n  --- Phase 3: Column Index Scan ---");
    print_separator();

    exec(&db, "CREATE INDEX idx_tag ON t (tag)");
    exec(&db, "CREATE INDEX idx_score ON t (score)");

    let mut idx_eq_latencies: Vec<u64> = Vec::with_capacity(idx_queries);
    for _ in 0..idx_queries {
        let start = Instant::now();
        exec(&db, "SELECT * FROM t WHERE tag = 'tag_3'");
        idx_eq_latencies.push(start.elapsed().as_micros() as u64);
    }
    print_latency_distribution(&format!("Column eq (tag='tag_3', ~{} rows, {} queries)", seed_rows / 10, idx_queries), &idx_eq_latencies);

    let mut idx_range_latencies: Vec<u64> = Vec::with_capacity(idx_queries);
    for _ in 0..idx_queries {
        let start = Instant::now();
        exec(&db, "SELECT * FROM t WHERE score > 20000.0 AND score < 30000.0");
        idx_range_latencies.push(start.elapsed().as_micros() as u64);
    }
    print_latency_distribution(&format!("Column range (score 20K-30K, {} queries)", idx_queries), &idx_range_latencies);

    println!("\n  --- Phase 4: Full Table Scan ---");
    print_separator();

    let mut scan_latencies: Vec<u64> = Vec::with_capacity(scan_queries);
    for _ in 0..scan_queries {
        let start = Instant::now();
        exec(&db, "SELECT * FROM t");
        scan_latencies.push(start.elapsed().as_micros() as u64);
    }
    print_latency_distribution(&format!("SELECT * {} rows ({} queries)", seed_rows, scan_queries), &scan_latencies);

    let mut count_latencies: Vec<u64> = Vec::with_capacity(scan_queries);
    for _ in 0..scan_queries {
        let start = Instant::now();
        exec(&db, "SELECT COUNT(*) AS cnt FROM t");
        count_latencies.push(start.elapsed().as_micros() as u64);
    }
    print_latency_distribution(&format!("COUNT(*) ({} queries)", scan_queries), &count_latencies);
}

// ============================================================================
// Test 3: INSERT/UPDATE/DELETE Latency + CPU Throughput
// ============================================================================

#[test]
fn bench_write_latency_cpu() {
    println!("\n{}", "=".repeat(130));
    println!("  Write Latency Distribution + CPU Throughput");
    println!("{}", "=".repeat(130));

    let ci = is_ci();
    let insert_latency_rows: i64 = if ci {  2_000 } else { 10_000 };
    let insert_throughput_rows: i64 = if ci { 5_000 } else { 50_000 };
    let update_rows: i64 = if ci { 1_000 } else { 5_000 };
    let delete_rows: i64 = if ci {   500 } else { 3_000 };

    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT, status TEXT)");

    // INSERT latency
    println!("\n  --- INSERT Latency ---");
    print_separator();

    let mut insert_latencies: Vec<u64> = Vec::with_capacity(insert_latency_rows as usize);
    for i in 1..=insert_latency_rows {
        let start = Instant::now();
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'user_{}', {:.1}, 'active')",
            i, i, i as f64 * 2.0));
        insert_latencies.push(start.elapsed().as_micros() as u64);
    }
    print_latency_distribution(&format!("INSERT ({} rows, 4 cols)", insert_latency_rows), &insert_latencies);

    // INSERT throughput (no latency measurement overhead)
    let throughput_start = Instant::now();
    let throughput_base = insert_latency_rows + 1;
    for i in 0..insert_throughput_rows {
        let id = throughput_base + i;
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'user_{}', {:.1}, 'active')",
            id, id, id as f64 * 2.0));
    }
    let throughput_ms = throughput_start.elapsed().as_millis();
    println!("  INSERT throughput ({} rows): {:.0} ops/s ({:.1} µs/op)",
        insert_throughput_rows,
        insert_throughput_rows as f64 / (throughput_ms as f64 / 1000.0),
        throughput_ms as f64 * 1000.0 / insert_throughput_rows as f64);

    // UPDATE latency
    println!("\n  --- UPDATE Latency ---");
    print_separator();

    let mut update_latencies: Vec<u64> = Vec::with_capacity(update_rows as usize);
    for i in 1..=update_rows {
        let start = Instant::now();
        exec(&db, &format!("UPDATE t SET score = score + 100, status = 'updated' WHERE id = {}", i));
        update_latencies.push(start.elapsed().as_micros() as u64);
    }
    print_latency_distribution(&format!("UPDATE by PK ({} rows)", update_rows), &update_latencies);

    // DELETE latency
    println!("\n  --- DELETE Latency ---");
    print_separator();

    let mut delete_latencies: Vec<u64> = Vec::with_capacity(delete_rows as usize);
    for i in 1..=delete_rows {
        let start = Instant::now();
        exec(&db, &format!("DELETE FROM t WHERE id = {}", i));
        delete_latencies.push(start.elapsed().as_micros() as u64);
    }
    print_latency_distribution(&format!("DELETE by PK ({} rows)", delete_rows), &delete_latencies);

    // Memory after writes
    println!("\n  --- Memory After Writes ---");
    print_separator();
    let total_inserts = insert_latency_rows + insert_throughput_rows;
    print_memory(&format!("After {} INSERT + {} UPDATE + {} DELETE", total_inserts, update_rows, delete_rows));
}

// ============================================================================
// Test 4: Concurrent CPU Throughput
// ============================================================================

#[test]
fn bench_concurrent_cpu() {
    use std::sync::Arc;
    use std::thread;

    println!("\n{}", "=".repeat(130));
    println!("  Concurrent CPU Throughput (Read + Write Mixed)");
    println!("{}", "=".repeat(130));

    let ci = is_ci();
    let seed_rows: i64      = if ci {  2_000 } else { 10_000 };
    let read_threads: usize  = if ci {      4 } else {      8 };
    let read_ops: usize      = if ci {  2_000 } else { 10_000 };
    let write_threads: usize = if ci {      2 } else {      4 };
    let write_ops: usize     = if ci {  1_000 } else {  5_000 };
    let mixed_read_threads: usize  = if ci { 1 } else { 2 };
    let mixed_write_threads: usize = if ci { 1 } else { 2 };
    let mixed_ops: usize          = if ci { 1_000 } else { 5_000 };

    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT, value INTEGER)");

    // Seed
    for i in 1..=seed_rows {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'data_{}', {})", i, i, i * 10));
    }

    let db = Arc::new(db);

    // Test 1: Read-heavy
    println!("\n  --- {} Read Threads ({} ops each) ---", read_threads, read_ops);
    let read_start = Instant::now();
    let mut handles = vec![];
    for t in 0..read_threads {
        let db_clone = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            let mut count = 0;
            for i in 0..read_ops {
                let id = (t * read_ops + i + 1) as i64 % seed_rows + 1;
                let _ = db_clone.execute(&format!("SELECT * FROM t WHERE id = {}", id));
                count += 1;
            }
            count
        }));
    }
    let read_total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
    let read_elapsed = read_start.elapsed();
    println!("  {} reads in {:.0}ms → {:.0} ops/s ({:.1} µs/op)",
        read_total, read_elapsed.as_millis(),
        read_total as f64 / read_elapsed.as_secs_f64(),
        read_elapsed.as_micros() as f64 / read_total as f64);

    // Test 2: Write-heavy
    println!("\n  --- {} Write Threads ({} INSERT each) ---", write_threads, write_ops);
    let write_start = Instant::now();
    let mut handles = vec![];
    for t in 0..write_threads {
        let db_clone = Arc::clone(&db);
        let base_id = seed_rows + 1 + (t * write_ops) as i64;
        handles.push(thread::spawn(move || {
            let mut count = 0;
            for i in 0..write_ops {
                let id = base_id + i as i64;
                let _ = db_clone.execute(&format!(
                    "INSERT INTO t VALUES ({}, 'thread_{}_{}', {})",
                    id, t, i, id * 10));
                count += 1;
            }
            count
        }));
    }
    let write_total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
    let write_elapsed = write_start.elapsed();
    println!("  {} writes in {:.0}ms → {:.0} ops/s ({:.1} µs/op)",
        write_total, write_elapsed.as_millis(),
        write_total as f64 / write_elapsed.as_secs_f64(),
        write_elapsed.as_micros() as f64 / write_total as f64);

    // Test 3: Mixed
    println!("\n  --- Mixed ({} read + {} write threads) ---", mixed_read_threads, mixed_write_threads);
    let mixed_start = Instant::now();
    let mut handles = vec![];

    let current_max_id = seed_rows + (write_threads * write_ops) as i64;

    // Read threads
    for _t in 0..mixed_read_threads {
        let db_clone = Arc::clone(&db);
        let ops = mixed_ops;
        let seed = seed_rows;
        handles.push(thread::spawn(move || {
            let mut count = 0;
            for i in 0..ops {
                let id = (i % seed as usize) as i64 + 1;
                let _ = db_clone.execute(&format!("SELECT * FROM t WHERE id = {}", id));
                count += 1;
            }
            count
        }));
    }

    // Write threads
    for t in 0..mixed_write_threads {
        let db_clone = Arc::clone(&db);
        let base = current_max_id + 1 + (t * mixed_ops) as i64;
        let ops = mixed_ops;
        handles.push(thread::spawn(move || {
            let mut count = 0;
            for i in 0..ops {
                let id = base + i as i64;
                let _ = db_clone.execute(&format!(
                    "INSERT INTO t VALUES ({}, 'mixed_{}_{}', {})",
                    id, t, i, id * 10));
                count += 1;
            }
            count
        }));
    }

    let results: Vec<usize> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let mixed_total: usize = results.iter().sum();
    let mixed_elapsed = mixed_start.elapsed();
    println!("  {} ops in {:.0}ms → {:.0} ops/s ({:.1} µs/op)",
        mixed_total, mixed_elapsed.as_millis(),
        mixed_total as f64 / mixed_elapsed.as_secs_f64(),
        mixed_elapsed.as_micros() as f64 / mixed_total as f64);

    // Memory check
    print_memory("After concurrent test");
}
