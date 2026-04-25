//! Resource & Latency Diagnostic Benchmark
//!
//! Measures: memory footprint, query latency distribution (p50/p95/p99), CPU overhead
//!
//! Run: cargo test --test bench_resource --release -- --nocapture --test-threads=1

use motedb::{Database, DBConfig};
use tempfile::TempDir;
use std::time::{Instant, Duration};

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

    let (rss_baseline, _) = print_memory("Baseline (empty DB)");

    // 10K rows
    {
        let (db, _dir) = create_db();
        exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT, tag TEXT, value INTEGER)");
        for i in 1..=10_000i64 {
            exec(&db, &format!("INSERT INTO t VALUES ({}, 'name_{}', {:.1}, 'tag_{}', {})",
                i, i, i as f64 * 1.5, i % 10, i * 10));
        }
        let (rss_10k, _) = print_memory("After INSERT 10K rows (5 cols, MemTable)");
        println!("  → ΔRSS: {} KB ({:.1} MB) for 10K rows = {:.1} bytes/row",
            rss_10k - rss_baseline, (rss_10k - rss_baseline) as f64 / 1024.0,
            (rss_10k - rss_baseline) as f64 * 1024.0 / 10_000.0);
    }

    // 50K rows
    {
        let (db, _dir) = create_db();
        exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT, tag TEXT, value INTEGER)");
        for i in 1..=50_000i64 {
            exec(&db, &format!("INSERT INTO t VALUES ({}, 'name_{}', {:.1}, 'tag_{}', {})",
                i, i, i as f64 * 1.5, i % 10, i * 10));
        }
        let (rss_50k, _) = print_memory("After INSERT 50K rows (5 cols, MemTable)");

        // Flush to SSTable
        db.flush().expect("flush");
        std::thread::sleep(Duration::from_millis(500));
        let (rss_50k_sst, _) = print_memory("After flush 50K → SSTable");
        println!("  → ΔRSS MemTable: {} KB ({:.1} MB) = {:.1} bytes/row",
            rss_50k - rss_baseline, (rss_50k - rss_baseline) as f64 / 1024.0,
            (rss_50k - rss_baseline) as f64 * 1024.0 / 50_000.0);
        println!("  → ΔRSS SSTable:  {} KB ({:.1} MB) = {:.1} bytes/row",
            rss_50k_sst - rss_baseline, (rss_50k_sst - rss_baseline) as f64 / 1024.0,
            (rss_50k_sst - rss_baseline) as f64 * 1024.0 / 50_000.0);

        // Checkpoint + drop
        db.checkpoint().expect("checkpoint");
        drop(db);
        let (_rss_after_drop, _) = print_memory("After checkpoint + drop DB");
    }

    // 100K rows
    {
        let (db, _dir) = create_db();
        exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT, tag TEXT, value INTEGER)");
        let start = Instant::now();
        for i in 1..=100_000i64 {
            exec(&db, &format!("INSERT INTO t VALUES ({}, 'name_{}', {:.1}, 'tag_{}', {})",
                i, i, i as f64 * 1.5, i % 10, i * 10));
        }
        let insert_ms = start.elapsed().as_millis();
        let (rss_100k, _) = print_memory("After INSERT 100K rows (MemTable)");
        println!("  → Insert: {}ms, {:.0} ops/s", insert_ms, 100_000.0 / (insert_ms as f64 / 1000.0));
        println!("  → ΔRSS: {} KB ({:.1} MB) = {:.1} bytes/row",
            rss_100k - rss_baseline, (rss_100k - rss_baseline) as f64 / 1024.0,
            (rss_100k - rss_baseline) as f64 * 1024.0 / 100_000.0);
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

    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT, tag TEXT, value INTEGER)");

    // Seed 30K rows
    for i in 1..=30_000i64 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'name_{}', {:.1}, 'tag_{}', {})",
            i, i, i as f64 * 1.5, i % 10, i * 10));
    }

    println!("\n  --- Phase 1: PK Point Query (MemTable, 30K rows) ---");
    print_separator();

    // PK queries — latency per operation
    let mut pk_latencies: Vec<u64> = Vec::with_capacity(5000);
    for i in 1..=5000i64 {
        let start = Instant::now();
        exec(&db, &format!("SELECT * FROM t WHERE id = {}", i));
        pk_latencies.push(start.elapsed().as_micros() as u64);
    }
    print_latency_distribution("PK SELECT * (MemTable, 5K queries)", &pk_latencies);

    // PK queries with specific columns
    let mut pk_proj_latencies: Vec<u64> = Vec::with_capacity(5000);
    for i in 1..=5000i64 {
        let start = Instant::now();
        exec(&db, &format!("SELECT name, score FROM t WHERE id = {}", i));
        pk_proj_latencies.push(start.elapsed().as_micros() as u64);
    }
    print_latency_distribution("PK SELECT 2/5 cols (MemTable, 5K queries)", &pk_proj_latencies);

    // Flush to SSTable
    db.flush().expect("flush");
    std::thread::sleep(Duration::from_millis(500));

    println!("\n  --- Phase 2: PK Point Query (SSTable, 30K rows) ---");
    print_separator();

    let mut pk_sst_latencies: Vec<u64> = Vec::with_capacity(5000);
    for i in 1..=5000i64 {
        let start = Instant::now();
        exec(&db, &format!("SELECT * FROM t WHERE id = {}", i));
        pk_sst_latencies.push(start.elapsed().as_micros() as u64);
    }
    print_latency_distribution("PK SELECT * (SSTable cold, 5K queries)", &pk_sst_latencies);

    // Warm cache pass
    let mut pk_warm_latencies: Vec<u64> = Vec::with_capacity(5000);
    for i in 1..=5000i64 {
        let start = Instant::now();
        exec(&db, &format!("SELECT * FROM t WHERE id = {}", i));
        pk_warm_latencies.push(start.elapsed().as_micros() as u64);
    }
    print_latency_distribution("PK SELECT * (SSTable warm, 5K queries)", &pk_warm_latencies);

    println!("\n  --- Phase 3: Column Index Scan ---");
    print_separator();

    exec(&db, "CREATE INDEX idx_tag ON t (tag)");
    exec(&db, "CREATE INDEX idx_score ON t (score)");

    let mut idx_eq_latencies: Vec<u64> = Vec::with_capacity(500);
    for _ in 0..500 {
        let start = Instant::now();
        exec(&db, "SELECT * FROM t WHERE tag = 'tag_3'");
        idx_eq_latencies.push(start.elapsed().as_micros() as u64);
    }
    print_latency_distribution("Column eq (tag='tag_3', ~3K rows, 500 queries)", &idx_eq_latencies);

    let mut idx_range_latencies: Vec<u64> = Vec::with_capacity(500);
    for _ in 0..500 {
        let start = Instant::now();
        exec(&db, "SELECT * FROM t WHERE score > 20000.0 AND score < 30000.0");
        idx_range_latencies.push(start.elapsed().as_micros() as u64);
    }
    print_latency_distribution("Column range (score 20K-30K, 500 queries)", &idx_range_latencies);

    println!("\n  --- Phase 4: Full Table Scan ---");
    print_separator();

    let mut scan_latencies: Vec<u64> = Vec::with_capacity(50);
    for _ in 0..50 {
        let start = Instant::now();
        exec(&db, "SELECT * FROM t");
        scan_latencies.push(start.elapsed().as_micros() as u64);
    }
    print_latency_distribution("SELECT * 30K rows (50 queries)", &scan_latencies);

    let mut count_latencies: Vec<u64> = Vec::with_capacity(50);
    for _ in 0..50 {
        let start = Instant::now();
        exec(&db, "SELECT COUNT(*) AS cnt FROM t");
        count_latencies.push(start.elapsed().as_micros() as u64);
    }
    print_latency_distribution("COUNT(*) (50 queries)", &count_latencies);
}

// ============================================================================
// Test 3: INSERT/UPDATE/DELETE Latency + CPU Throughput
// ============================================================================

#[test]
fn bench_write_latency_cpu() {
    println!("\n{}", "=".repeat(130));
    println!("  Write Latency Distribution + CPU Throughput");
    println!("{}", "=".repeat(130));

    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT, status TEXT)");

    // INSERT latency
    println!("\n  --- INSERT Latency ---");
    print_separator();

    let mut insert_latencies: Vec<u64> = Vec::with_capacity(10_000);
    for i in 1..=10_000i64 {
        let start = Instant::now();
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'user_{}', {:.1}, 'active')",
            i, i, i as f64 * 2.0));
        insert_latencies.push(start.elapsed().as_micros() as u64);
    }
    print_latency_distribution("INSERT (10K rows, 4 cols)", &insert_latencies);

    // INSERT throughput (no latency measurement overhead)
    let throughput_start = Instant::now();
    for i in 10_001..=60_000i64 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'user_{}', {:.1}, 'active')",
            i, i, i as f64 * 2.0));
    }
    let throughput_ms = throughput_start.elapsed().as_millis();
    println!("  INSERT throughput (50K rows): {:.0} ops/s ({:.1} µs/op)",
        50_000.0 / (throughput_ms as f64 / 1000.0),
        throughput_ms as f64 * 1000.0 / 50_000.0);

    // UPDATE latency
    println!("\n  --- UPDATE Latency ---");
    print_separator();

    let mut update_latencies: Vec<u64> = Vec::with_capacity(5000);
    for i in 1..=5000i64 {
        let start = Instant::now();
        exec(&db, &format!("UPDATE t SET score = score + 100, status = 'updated' WHERE id = {}", i));
        update_latencies.push(start.elapsed().as_micros() as u64);
    }
    print_latency_distribution("UPDATE by PK (5K rows)", &update_latencies);

    // DELETE latency
    println!("\n  --- DELETE Latency ---");
    print_separator();

    let mut delete_latencies: Vec<u64> = Vec::with_capacity(3000);
    for i in 1..=3000i64 {
        let start = Instant::now();
        exec(&db, &format!("DELETE FROM t WHERE id = {}", i));
        delete_latencies.push(start.elapsed().as_micros() as u64);
    }
    print_latency_distribution("DELETE by PK (3K rows)", &delete_latencies);

    // Memory after writes
    println!("\n  --- Memory After Writes ---");
    print_separator();
    print_memory("After 60K INSERT + 5K UPDATE + 3K DELETE");
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

    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT, value INTEGER)");

    // Seed
    for i in 1..=10_000i64 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'data_{}', {})", i, i, i * 10));
    }

    let db = Arc::new(db);

    // Test 1: Read-heavy (8 read threads)
    println!("\n  --- 8 Read Threads (10K ops each) ---");
    let read_start = Instant::now();
    let mut handles = vec![];
    for t in 0..8 {
        let db_clone = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            let mut count = 0;
            for i in 0..10_000 {
                let id = (t * 10000 + i + 1) as i64 % 10_000 + 1;
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

    // Test 2: Write-heavy (4 write threads)
    println!("\n  --- 4 Write Threads (5K INSERT each) ---");
    let write_start = Instant::now();
    let mut handles = vec![];
    for t in 0..4 {
        let db_clone = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            let base = 10001 + t * 5000;
            let mut count = 0;
            for i in 0..5000 {
                let id = (base + i) as i64;
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

    // Test 3: Mixed (2 read + 2 write)
    println!("\n  --- Mixed (2 read + 2 write threads) ---");
    let mixed_start = Instant::now();
    let mut handles = vec![];

    // Read threads
    for _t in 0..2 {
        let db_clone = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            let mut count = 0;
            for i in 0..5000 {
                let id = (i % 10_000) as i64 + 1;
                let _ = db_clone.execute(&format!("SELECT * FROM t WHERE id = {}", id));
                count += 1;
            }
            count
        }));
    }

    // Write threads
    for t in 0..2 {
        let db_clone = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            let base = 30001 + t * 5000;
            let mut count = 0;
            for i in 0..5000 {
                let id = (base + i) as i64;
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
