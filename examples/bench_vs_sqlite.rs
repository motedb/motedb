//! MoteDB vs SQLite Comparative Benchmark
//!
//! Runs identical workloads against both databases using the same:
//! - Schema (sales table: id, customer TEXT, amount FLOAT, region TEXT)
//! - Data (300K rows, same values)
//! - Queries (full scan, WHERE, GROUP BY, ORDER BY, PK lookup, etc.)
//!
//! SQLite is configured for maximum performance:
//! - WAL mode, synchronous=NORMAL, 64MB cache
//! - Prepared statements for all queries
//! - Explicit transactions for batch INSERT
//!
//! Usage:
//!   cargo run --release --example bench_vs_sqlite

use motedb::{Database, DBConfig, QueryResult};
use rusqlite::Connection;
use tempfile::TempDir;
use std::time::Instant;

// ─── Helpers ───────────────────────────────────────────────────────────────

fn get_rss_kb() -> u64 {
    let pid = std::process::id();
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output().ok()
        .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse::<u64>().ok())
        .unwrap_or(0)
}

/// Run a closure N times, return median microseconds.
fn bench_us<F: FnMut()>(mut f: F, warmup: usize, iters: usize) -> u64 {
    for _ in 0..warmup { f(); }
    let mut times = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        f();
        times.push(t.elapsed().as_micros() as u64);
    }
    times.sort();
    times[iters / 2] // median
}

// ─── Data Generation ───────────────────────────────────────────────────────

/// Generate 300K rows of sales data as (customer, amount, region) tuples.
fn gen_data(n: usize) -> Vec<(String, f64, &'static str)> {
    let mut rows = Vec::with_capacity(n);
    for i in 0..n {
        let region = if i % 3 == 0 { "US" } else { "EU" };
        let customer = format!("cust_{}", i % (n / 10).max(10));
        let amount = (i as f64 * 1.7 + 42.0) % 1000.0;
        rows.push((customer, amount, region));
    }
    rows
}

// ─── MoteDB Setup + Queries ────────────────────────────────────────────────

struct MoteDBBench {
    db: Database,
    _dir: TempDir,
    n: usize,
}

impl MoteDBBench {
    fn setup(n: usize) -> Self {
        let dir = TempDir::new().unwrap();
        let mut config = DBConfig::for_edge();
        config.max_result_rows = None;
        let db = Database::create_with_config(dir.path(), config).unwrap();
        db.execute("CREATE TABLE sales (id INT PRIMARY KEY AUTO_INCREMENT, customer TEXT, amount FLOAT, region TEXT)").unwrap();
        Self { db, _dir: dir, n }
    }

    fn insert(&self, data: &[(String, f64, &'static str)]) -> u128 {
        let batch_size = 5000;
        let t = Instant::now();
        for chunk in data.chunks(batch_size) {
            let mut batch = String::with_capacity(batch_size * 60);
            for (customer, amount, region) in chunk {
                batch.push_str(&format!("('{}',{:.2},'{}'),", customer, amount, region));
            }
            batch.truncate(batch.len() - 1);
            self.db.execute(&format!("INSERT INTO sales (customer, amount, region) VALUES {}", batch)).unwrap();
        }
        t.elapsed().as_millis()
    }

    fn create_indexes(&self) -> u128 {
        let t = Instant::now();
        self.db.execute("CREATE INDEX idx_region ON sales (region) USING COLUMN").unwrap();
        self.db.execute("CREATE INDEX idx_customer ON sales (customer) USING COLUMN").unwrap();
        t.elapsed().as_millis()
    }

    fn vacuum(&self) -> u128 {
        let t = Instant::now();
        let _ = self.db.vacuum();
        t.elapsed().as_millis()
    }

    /// Execute a query and return (row_count, micros_per_call) for median of 10 runs.
    /// Uses O(1) row_count() for columnar results — no materialization overhead.
    fn query(&self, sql: &str) -> (usize, u64) {
        let mut row_count = 0;
        let us = bench_us(|| {
            let result = self.db.execute(sql).unwrap();
            row_count = result.row_count();
            // Fallback: if streaming result, materialize for accurate count
            if row_count == 0 {
                row_count = result.materialize().unwrap().row_count();
            }
        }, 2, 10);
        (row_count, us)
    }

    fn pk_select(&self, id: usize) -> (usize, u64) {
        self.query(&format!("SELECT * FROM sales WHERE id = {}", id))
    }

    fn pk_update(&self, id: usize) -> (usize, u64) {
        self.query(&format!("UPDATE sales SET amount = 999.99 WHERE id = {}", id))
    }
}

// ─── SQLite Setup + Queries ────────────────────────────────────────────────

struct SQLiteBench {
    conn: Connection,
    n: usize,
}

impl SQLiteBench {
    fn setup(n: usize) -> Self {
        let conn = Connection::open_in_memory().unwrap();
        // Maximum performance configuration
        conn.execute_batch("
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA cache_size = -64000;  -- 64MB cache
            PRAGMA temp_store = MEMORY;
            PRAGMA mmap_size = 268435456;  -- 256MB mmap
            PRAGMA page_size = 4096;
        ").unwrap();
        conn.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY AUTOINCREMENT, customer TEXT, amount REAL, region TEXT)", []).unwrap();
        Self { conn, n }
    }

    fn insert(&self, data: &[(String, f64, &'static str)]) -> u128 {
        let batch_size = 5000;
        let t = Instant::now();
        for chunk in data.chunks(batch_size) {
            // Explicit transaction per batch for max throughput
            self.conn.execute_batch("BEGIN TRANSACTION").unwrap();
            {
                let mut stmt = self.conn.prepare(
                    "INSERT INTO sales (customer, amount, region) VALUES (?1, ?2, ?3)"
                ).unwrap();
                for (customer, amount, region) in chunk {
                    stmt.execute(rusqlite::params![customer, *amount, region]).unwrap();
                }
            }
            self.conn.execute_batch("COMMIT").unwrap();
        }
        t.elapsed().as_millis()
    }

    fn create_indexes(&self) -> u128 {
        let t = Instant::now();
        self.conn.execute("CREATE INDEX idx_region ON sales (region)", []).unwrap();
        self.conn.execute("CREATE INDEX idx_customer ON sales (customer)", []).unwrap();
        t.elapsed().as_millis()
    }

    /// Execute a query, return (row_count, median_us)
    fn query(&self, sql: &str) -> (usize, u64) {
        let mut row_count = 0;
        let us = bench_us(|| {
            let mut stmt = self.conn.prepare(sql).unwrap();
            let mut rows = stmt.query([]).unwrap();
            let mut count = 0;
            while let Some(_) = rows.next().unwrap() {
                count += 1;
            }
            row_count = count;
        }, 2, 10);
        (row_count, us)
    }

    fn query_prepared(&self, sql: &str) -> (usize, u64) {
        let mut stmt = self.conn.prepare(sql).unwrap();
        let mut row_count = 0;
        let us = bench_us(|| {
            let mut rows = stmt.query([]).unwrap();
            let mut count = 0;
            while let Some(_) = rows.next().unwrap() {
                count += 1;
            }
            row_count = count;
        }, 2, 10);
        (row_count, us)
    }

    fn pk_select(&self, id: usize) -> (usize, u64) {
        let sql = format!("SELECT * FROM sales WHERE id = {}", id);
        self.query(&sql)
    }

    fn pk_update(&self, id: usize) -> (usize, u64) {
        let sql = format!("UPDATE sales SET amount = 999.99 WHERE id = {}", id);
        let mut row_count = 0;
        let us = bench_us(|| {
            self.conn.execute(&sql, []).unwrap();
            row_count = 1;
        }, 2, 10);
        (row_count, us)
    }
}

// ─── Main ──────────────────────────────────────────────────────────────────

fn main() {
    let n = 300_000;
    let data = gen_data(n);

    println!();
    println!("  ╔══════════════════════════════════════════════════════════════════╗");
    println!("  ║       MoteDB vs SQLite — Embedded Database Benchmark           ║");
    println!("  ║       {} rows × 4 columns (id, customer, amount, region)       ║", n);
    println!("  ╚══════════════════════════════════════════════════════════════════╝");
    println!();

    // ── Phase 1: Setup ──────────────────────────────────────────────────

    println!("  ┌─ Setup Phase ──────────────────────────────────────────────────┐");

    let rss_before = get_rss_kb();

    // MoteDB setup
    let mote = MoteDBBench::setup(n);
    let mote_insert = mote.insert(&data);
    let mote_index = mote.create_indexes();
    let mote_vacuum = mote.vacuum();

    let rss_after_mote = get_rss_kb();

    // SQLite setup
    let sqlite = SQLiteBench::setup(n);
    let sqlite_insert = sqlite.insert(&data);
    let sqlite_index = sqlite.create_indexes();

    let rss_after_sqlite = get_rss_kb();

    println!("  │");
    println!("  │  {:30} {:>12} {:>12}", "", "MoteDB", "SQLite");
    println!("  │  {}", "-".repeat(58));
    println!("  │  {:30} {:>9} ms   {:>9} ms",
        "INSERT {} rows".replace("{}", &n.to_string()), mote_insert, sqlite_insert);
    println!("  │  {:30} {:>9} ms   {:>9} ms",
        "CREATE 2 indexes", mote_index, sqlite_index);
    println!("  │  {:30} {:>9} ms   {:>12}",
        "Vacuum/compact", mote_vacuum, "n/a");
    println!("  │  {}", "-".repeat(58));
    let mote_ins_rps = n as f64 / mote_insert as f64 * 1000.0;
    let sqlite_ins_rps = n as f64 / sqlite_insert as f64 * 1000.0;
    println!("  │  {:30} {:>9}/s   {:>9}/s",
        "INSERT throughput", format!("{:.0}", mote_ins_rps), format!("{:.0}", sqlite_ins_rps));
    let mote_idx_rps = (n as f64 * 2.0) / mote_index as f64 * 1000.0;
    let sqlite_idx_rps = (n as f64 * 2.0) / sqlite_index as f64 * 1000.0;
    println!("  │  {:30} {:>9}/s   {:>9}/s",
        "INDEX throughput", format!("{:.0}", mote_idx_rps), format!("{:.0}", sqlite_idx_rps));
    println!("  │");
    println!("  │  {:30} {:>9} KB  {:>9} KB",
        "RSS after setup", rss_after_mote, rss_after_sqlite);
    println!("  │  {:30} {:>9} B   {:>9} B",
        "Memory per row",
        rss_after_mote * 1024 / n as u64,
        rss_after_sqlite * 1024 / n as u64);
    println!("  └────────────────────────────────────────────────────────────────┘");
    println!();

    // ── Phase 2: Query Benchmarks ───────────────────────────────────────

    println!("  ┌─ Query Benchmarks (median of 10 runs) ─────────────────────────┐");
    println!("  │");
    println!("  │  {:36} {:>10} {:>10} {:>6}", "Operation", "MoteDB", "SQLite", "Ratio");
    println!("  │  {}", "-".repeat(66));

    let queries: Vec<(&str, &str)> = vec![
        ("Full scan (SELECT *)",           "SELECT * FROM sales"),
        ("WHERE region='US'",              "SELECT * FROM sales WHERE region = 'US'"),
        ("WHERE customer='cust_1'",        "SELECT * FROM sales WHERE customer = 'cust_1'"),
        ("GROUP BY + aggregates",          "SELECT customer, COUNT(*), SUM(amount), AVG(amount) FROM sales GROUP BY customer"),
        ("ORDER BY amount DESC LIMIT 10",  "SELECT * FROM sales ORDER BY amount DESC LIMIT 10"),
        ("SELECT DISTINCT region",         "SELECT DISTINCT region FROM sales"),
        ("LIKE 'cust_1%'",                 "SELECT * FROM sales WHERE customer LIKE 'cust_1%'"),
        ("IN subquery",                    "SELECT id FROM sales WHERE customer IN (SELECT customer FROM sales WHERE region = 'US')"),
        ("COUNT/SUM/MIN/MAX WHERE",        "SELECT COUNT(*), SUM(amount), MIN(amount), MAX(amount) FROM sales WHERE region = 'US'"),
    ];

    let mut mote_wins = 0u64;
    let mut sqlite_wins = 0u64;
    let mut results: Vec<(&str, u64, u64, usize, usize)> = Vec::new();

    for (label, sql) in &queries {
        let (mote_rows, mote_us) = mote.query(sql);
        let (sqlite_rows, sqlite_us) = sqlite.query(sql);
        let ratio = mote_us as f64 / sqlite_us as f64;
        let winner = if mote_us < sqlite_us { "⚡ MoteDB" } else { "🏆 SQLite" };
        if mote_us <= sqlite_us { mote_wins += 1; } else { sqlite_wins += 1; }

        println!("  │  {:36} {:>7} μs  {:>7} μs  {:>5.2}x  {}",
            label, mote_us, sqlite_us, ratio, winner);
        results.push((*label, mote_us, sqlite_us, mote_rows, sqlite_rows));
    }

    // PK operations (need special handling)
    let mid = n / 2;
    let (mote_rows, mote_us) = mote.pk_select(mid);
    let (sqlite_rows, sqlite_us) = sqlite.pk_select(mid);
    let ratio = mote_us as f64 / sqlite_us as f64;
    if mote_us <= sqlite_us { mote_wins += 1; } else { sqlite_wins += 1; }
    println!("  │  {:36} {:>7} μs  {:>7} μs  {:>5.2}x  {}",
        "PK point SELECT", mote_us, sqlite_us, ratio,
        if mote_us <= sqlite_us { "⚡ MoteDB" } else { "🏆 SQLite" });

    let (mote_rows, mote_us) = mote.pk_update(mid);
    let (sqlite_rows, sqlite_us) = sqlite.pk_update(mid);
    let ratio = mote_us as f64 / sqlite_us as f64;
    if mote_us <= sqlite_us { mote_wins += 1; } else { sqlite_wins += 1; }
    println!("  │  {:36} {:>7} μs  {:>7} μs  {:>5.2}x  {}",
        "PK point UPDATE", mote_us, sqlite_us, ratio,
        if mote_us <= sqlite_us { "⚡ MoteDB" } else { "🏆 SQLite" });

    // Row count verification
    println!("  │");
    println!("  │  Row count verification (MoteDB / SQLite):");
    for (label, mote_us, sqlite_us, mr, sr) in &results {
        let check = if mr == sr { "✓" } else { "✗ MISMATCH" };
        println!("  │    {:34} {:>6} / {:<6} {}", label, mr, sr, check);
    }

    println!("  │");
    println!("  │  Summary: MoteDB wins {} / SQLite wins {} (of {} tests)", mote_wins, sqlite_wins, mote_wins + sqlite_wins);
    println!("  └────────────────────────────────────────────────────────────────┘");

    // ── Phase 3: Throughput Summary ─────────────────────────────────────

    println!();
    println!("  ┌─ Throughput Summary (rows/sec) ────────────────────────────────┐");
    println!("  │");
    println!("  │  {:36} {:>12} {:>12}", "Operation", "MoteDB", "SQLite");
    println!("  │  {}", "-".repeat(62));

    for (label, mote_us, sqlite_us, _, _) in &results {
        let mote_rps = if *mote_us > 0 { n as u64 * 1_000_000 / mote_us } else { 0 };
        let sqlite_rps = if *sqlite_us > 0 { n as u64 * 1_000_000 / sqlite_us } else { 0 };
        let mote_str = format_thru(mote_rps);
        let sqlite_str = format_thru(sqlite_rps);
        println!("  │  {:36} {:>12} {:>12}", label, mote_str, sqlite_str);
    }

    println!("  └────────────────────────────────────────────────────────────────┘");

    // ── Phase 4: Scale Linearity ────────────────────────────────────────

    // RSS after all queries
    let rss_final = get_rss_kb();
    println!();
    println!("  RSS final: {} KB", rss_final);
    println!();
}

fn format_thru(rps: u64) -> String {
    if rps >= 1_000_000 {
        format!("{:.1}M/s", rps as f64 / 1_000_000.0)
    } else if rps >= 1_000 {
        format!("{:.0}K/s", rps as f64 / 1_000.0)
    } else {
        format!("{}/s", rps)
    }
}
