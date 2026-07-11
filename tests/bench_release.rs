//! MoteDB v0.5.2 — Comprehensive Performance Report
//!
//! Covers: INSERT, SELECT, COUNT, WHERE, LIKE, GROUP BY, ORDER BY, Aggregate,
//! Point Query, UPDATE, DELETE — at 100K / 500K / 1M / 2M row scales.
//! Tracks: throughput, latency (p50/p95/p99), RSS memory, disk footprint.
//!
//! Run: cargo test --release --test bench_release -- --nocapture --ignored --test-threads=1

use motedb::{DBConfig, Database};
use std::time::Instant;
use tempfile::TempDir;

// ── Helpers ──────────────────────────────────────────────────────────────

fn rss_kb() -> usize {
    #[cfg(target_os = "macos")]
    {
        let pid = std::process::id();
        let out = std::process::Command::new("ps")
            .args(["-o", "rss", "-p", &pid.to_string()])
            .output()
            .ok();
        if let Some(o) = out {
            let s = String::from_utf8_lossy(&o.stdout);
            for line in s.lines().skip(1) {
                if let Ok(v) = line.trim().parse::<usize>() {
                    return v;
                }
            }
        }
        0
    }
    #[cfg(not(target_os = "macos"))]
    {
        0
    }
}

fn mb(kb: usize) -> f64 {
    kb as f64 / 1024.0
}

fn fmt_us(us: u64) -> String {
    if us < 1_000 {
        format!("{}µs", us)
    } else if us < 1_000_000 {
        format!("{:.2}ms", us as f64 / 1000.0)
    } else {
        format!("{:.2}s", us as f64 / 1_000_000.0)
    }
}

#[allow(dead_code)]
fn fmt_dur(d: std::time::Duration) -> String {
    fmt_us(d.as_micros() as u64)
}

fn dir_size(path: &std::path::Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                total += dir_size(&p);
            } else if let Ok(m) = e.metadata() {
                total += m.len();
            }
        }
    }
    total
}

fn mote_dir(dir: &TempDir) -> std::path::PathBuf {
    let p = dir.path();
    let parent = p.parent().unwrap_or(std::path::Path::new("/tmp"));
    let name = p
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    parent.join(format!("{}.mote", name))
}

fn latency_stats(us: &[u64]) -> (u64, u64, u64, u64) {
    if us.is_empty() {
        return (0, 0, 0, 0);
    }
    let mut s = us.to_vec();
    s.sort_unstable();
    let n = s.len();
    (
        s[n / 2],        // p50
        s[n * 95 / 100], // p95
        s[n * 99 / 100], // p99
        s[n - 1],        // max
    )
}

// ── Benchmark ────────────────────────────────────────────────────────────

struct Results {
    rows: usize,
    insert_ms: u128,
    insert_rss_mb: f64,
    insert_throughput: f64,
    disk_mb: f64,
    bytes_per_row: f64,
    // Query latencies in µs (p50)
    count_all_us: u64,
    count_where_us: u64,
    like_prefix_us: u64,
    agg_multi_us: u64,
    group_by_us: u64,
    order_by_us: u64,
    point_query_p50_us: u64,
    point_query_p95_us: u64,
    point_query_p99_us: u64,
    update_pk_us: u64,
    delete_pk_us: u64,
    // Memory
    steady_rss_mb: f64,
    peak_rss_mb: f64,
}

impl Results {
    fn print_table_row(&self) {
        println!(
            "  {:>8} | {:>7.1} | {:>10.0} | {:>6.1} | {:>6.1} | {:>6.1} | {:>6.1} | {:>6.1} | {:>7.1} | {:>7.1}",
            self.rows as f64 / 1000.0,
            self.insert_ms as f64 / 1000.0,
            self.insert_throughput,
            self.disk_mb,
            self.bytes_per_row,
            self.count_where_us as f64 / 1000.0,
            self.like_prefix_us as f64 / 1000.0,
            self.agg_multi_us as f64 / 1000.0,
            self.group_by_us as f64 / 1000.0,
            self.order_by_us as f64 / 1000.0,
        );
    }
}

fn bench_scale(n: usize) -> Results {
    let dir = TempDir::new().unwrap();
    let mote = mote_dir(&dir);
    let db = Database::create_with_config(dir.path(), DBConfig::for_edge()).unwrap();
    let _rss_base = rss_kb();

    // Schema: realistic 5-column table
    db.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, region TEXT, score FLOAT, qty INT)",
    )
    .unwrap();

    // ── INSERT benchmark ──
    let t0 = Instant::now();
    for i in 1..=n as i64 {
        let region = ["north", "south", "east", "west", "central"][(i % 5) as usize];
        db.execute(&format!(
            "INSERT INTO t VALUES ({}, 'item_{}', '{}', {:.2}, {})",
            i,
            i,
            region,
            (i as f64) * 1.7,
            i % 1000,
        ))
        .unwrap();
    }
    let insert_ms = t0.elapsed().as_millis();
    let insert_rss = rss_kb();
    let insert_throughput = n as f64 / (insert_ms as f64 / 1000.0);

    // ── Checkpoint to flush + compact ──
    db.checkpoint().ok();
    std::thread::sleep(std::time::Duration::from_millis(200));

    let disk = dir_size(&mote);
    let steady_rss = rss_kb();

    // ── Query warmup ──
    let _ = db.execute("SELECT COUNT(*) FROM t");
    let _ = db.execute("SELECT COUNT(*) FROM t WHERE region = 'north'");

    // ── COUNT(*) ──
    let iters = 20;
    let _ = db.execute("SELECT COUNT(*) FROM t");
    let t = Instant::now();
    for _ in 0..iters {
        let _ = db.execute("SELECT COUNT(*) FROM t");
    }
    let count_all_us = t.elapsed().as_micros() as u64 / iters;

    // ── COUNT(*) WHERE eq ──
    let _ = db.execute("SELECT COUNT(*) FROM t WHERE region = 'north'");
    let t = Instant::now();
    for _ in 0..iters {
        let _ = db.execute("SELECT COUNT(*) FROM t WHERE region = 'north'");
    }
    let count_where_us = t.elapsed().as_micros() as u64 / iters;

    // ── LIKE prefix ──
    let _ = db.execute("SELECT COUNT(*) FROM t WHERE name LIKE 'item_1%'");
    let t = Instant::now();
    for _ in 0..iters {
        let _ = db.execute("SELECT COUNT(*) FROM t WHERE name LIKE 'item_1%'");
    }
    let like_prefix_us = t.elapsed().as_micros() as u64 / iters;

    // ── Multi-aggregate ──
    let sql_agg = "SELECT SUM(qty), AVG(score), MIN(score), MAX(score), COUNT(*) FROM t";
    let _ = db.execute(sql_agg);
    let t = Instant::now();
    for _ in 0..10 {
        let _ = db.execute(sql_agg);
    }
    let agg_multi_us = t.elapsed().as_micros() as u64 / 10;

    // ── GROUP BY ──
    let sql_gb = "SELECT region, COUNT(*), SUM(qty), AVG(score) FROM t GROUP BY region";
    let _ = db.execute(sql_gb);
    let t = Instant::now();
    for _ in 0..10 {
        let _ = db.execute(sql_gb);
    }
    let group_by_us = t.elapsed().as_micros() as u64 / 10;

    // ── ORDER BY LIMIT (top-K) ──
    let sql_ob = "SELECT id FROM t ORDER BY score DESC LIMIT 10";
    let _ = db.execute(sql_ob);
    let t = Instant::now();
    for _ in 0..iters {
        let _ = db.execute(sql_ob);
    }
    let order_by_us = t.elapsed().as_micros() as u64 / iters;

    // ── Point query (PK lookup) latency distribution ──
    let mut latencies = Vec::with_capacity(200);
    for i in 0..200 {
        let pid = (n as i64 / 2 + i) % n as i64;
        let t = Instant::now();
        let _ = db.execute(&format!("SELECT * FROM t WHERE id = {}", pid));
        latencies.push(t.elapsed().as_micros() as u64);
    }
    let (pq_p50, pq_p95, pq_p99, _) = latency_stats(&latencies);

    let peak_rss = rss_kb();

    // ── UPDATE by PK ──
    let update_target = n as i64 / 2;
    let _ = db.execute(&format!(
        "UPDATE t SET qty = 999 WHERE id = {}",
        update_target
    ));
    let t = Instant::now();
    for i in 0..50 {
        let _ = db.execute(&format!(
            "UPDATE t SET qty = {} WHERE id = {}",
            i,
            update_target + i
        ));
    }
    let update_pk_us = t.elapsed().as_micros() as u64 / 50;

    // ── DELETE + re-INSERT by PK ──
    let del_target = n as i64 / 3;
    let t = Instant::now();
    let _ = db.execute(&format!("DELETE FROM t WHERE id = {}", del_target));
    let delete_pk_us = t.elapsed().as_micros() as u64;
    // Re-insert to keep data consistent
    let _ = db.execute(&format!(
        "INSERT INTO t VALUES ({}, 'item_{}', 'north', 42.0, 100)",
        del_target, del_target
    ));

    let final_rss = rss_kb();

    Results {
        rows: n,
        insert_ms,
        insert_rss_mb: mb(insert_rss),
        insert_throughput,
        disk_mb: disk as f64 / 1_048_576.0,
        bytes_per_row: disk as f64 / n as f64,
        count_all_us,
        count_where_us,
        like_prefix_us,
        agg_multi_us,
        group_by_us,
        order_by_us,
        point_query_p50_us: pq_p50,
        point_query_p95_us: pq_p95,
        point_query_p99_us: pq_p99,
        update_pk_us,
        delete_pk_us,
        steady_rss_mb: mb(steady_rss),
        peak_rss_mb: mb(peak_rss.max(final_rss)),
    }
}

#[test]
#[ignore = "release benchmark: run with --ignored"]
fn bench_release_report() {
    println!(
        "\n{}",
        "╔══════════════════════════════════════════════════════════════════════════╗"
    );
    println!("║         MoteDB v0.5.2 — Comprehensive Performance Report               ║");
    println!("║         Release Build · for_edge() config · macOS arm64                 ║");
    println!("╚══════════════════════════════════════════════════════════════════════════╝");

    let scales = [100_000, 500_000, 1_000_000, 2_000_000];
    let mut all_results = Vec::new();

    for &n in &scales {
        println!("\n{}", "─".repeat(80));
        println!("  Scale: {}K rows", n / 1000);
        println!("{}", "─".repeat(80));
        let r = bench_scale(n);
        println!(
            "  INSERT:           {:>8.2}s  ({:>8.0} rows/sec)",
            r.insert_ms as f64 / 1000.0,
            r.insert_throughput
        );
        println!("  RSS after INSERT: {:>8.1} MB", r.insert_rss_mb);
        println!(
            "  Disk:             {:>8.1} MB  ({:.1} bytes/row)",
            r.disk_mb, r.bytes_per_row
        );
        println!("  Steady RSS:       {:>8.1} MB", r.steady_rss_mb);
        println!("  Peak RSS:         {:>8.1} MB", r.peak_rss_mb);
        println!();
        println!("  ── Query Latency (per query) ──");
        println!("  COUNT(*):             {}", fmt_us(r.count_all_us));
        println!("  COUNT(*) WHERE eq:    {}", fmt_us(r.count_where_us));
        println!("  LIKE prefix:          {}", fmt_us(r.like_prefix_us));
        println!("  SUM/AVG/MIN/MAX:      {}", fmt_us(r.agg_multi_us));
        println!("  GROUP BY (5 groups):  {}", fmt_us(r.group_by_us));
        println!("  ORDER BY LIMIT 10:    {}", fmt_us(r.order_by_us));
        println!();
        println!("  ── Point Query (PK lookup) latency ──");
        println!(
            "  p50: {}   p95: {}   p99: {}",
            fmt_us(r.point_query_p50_us),
            fmt_us(r.point_query_p95_us),
            fmt_us(r.point_query_p99_us)
        );
        println!();
        println!("  ── Write latency ──");
        println!("  UPDATE by PK:  {}", fmt_us(r.update_pk_us));
        println!("  DELETE by PK:  {}", fmt_us(r.delete_pk_us));

        all_results.push(r);
    }

    // ── Summary Table ──
    println!("\n{}", "═".repeat(80));
    println!("  SUMMARY TABLE");
    println!("{}", "═".repeat(80));
    println!();
    println!("  Scale (K) | INSERT  | rows/sec  | Disk   | B/row | WHERE  | LIKE   | AGG    | GROUP  | ORDER  ");
    println!("  ---------|---------|-----------|--------|-------|--------|--------|--------|--------|--------");
    for r in &all_results {
        r.print_table_row();
    }

    // ── Memory scaling table ──
    println!();
    println!("  ── Memory Scaling (does RSS grow with data?) ──");
    println!("  Scale   | Insert RSS | Steady RSS | Peak RSS | Disk");
    println!("  --------|------------|------------|----------|--------");
    for r in &all_results {
        println!(
            "  {:>5}K  | {:>8.1} MB | {:>8.1} MB | {:>6.1} MB | {:>6.1} MB",
            r.rows / 1000,
            r.insert_rss_mb,
            r.steady_rss_mb,
            r.peak_rss_mb,
            r.disk_mb,
        );
    }

    // ── Point query scaling ──
    println!();
    println!("  ── Point Query (PK) Latency Distribution ──");
    println!("  Scale   |   p50    |   p95    |   p99");
    println!("  --------|----------|----------|----------");
    for r in &all_results {
        println!(
            "  {:>5}K  | {:>8} | {:>8} | {:>8}",
            r.rows / 1000,
            fmt_us(r.point_query_p50_us),
            fmt_us(r.point_query_p95_us),
            fmt_us(r.point_query_p99_us),
        );
    }

    println!("\n{}", "═".repeat(80));
    println!();
}
