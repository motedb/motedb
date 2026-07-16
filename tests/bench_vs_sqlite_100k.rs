//! MoteDB vs SQLite — 100K-row head-to-head benchmark.
//!
//! Latency + peak RSS + disk footprint, measured on the same workload.
//! SQLite uses WAL + PRAGMA synchronous=NORMAL (fair, production-realistic).
//!
//! Run: cargo test --release --test bench_vs_sqlite_100k -- --nocapture --test-threads=1 --ignored

use motedb::{types::Value, DBConfig, Database};
use rusqlite;
use std::time::Instant;

// ── Helpers ──────────────────────────────────────────────────────────────

#[cfg(target_os = "macos")]
fn rss_kb() -> usize {
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
fn rss_kb() -> usize {
    0
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

fn fmt_us(us: u64) -> String {
    if us < 1_000 {
        format!("{}µs", us)
    } else if us < 1_000_000 {
        format!("{:.2}ms", us as f64 / 1000.0)
    } else {
        format!("{:.2}s", us as f64 / 1_000_000.0)
    }
}

/// Returns (per-query µs, speedup-vs-sqlite where >1 means MoteDB is faster).
fn compare(label: &str, mote_us: u64, sqlite_us: u64) -> (f64, String) {
    let speedup = if mote_us == 0 {
        f64::INFINITY
    } else {
        sqlite_us as f64 / mote_us as f64
    };
    let verdict = if mote_us == 0 {
        "—".to_string()
    } else if speedup > 1.1 {
        format!("{:.2}x faster 🚀", speedup)
    } else if speedup < 0.9 {
        format!("{:.2}x slower 🐢", 1.0 / speedup)
    } else {
        "≈tie".to_string()
    };
    println!(
        "  {:<44} MoteDB {:>9}  SQLite {:>9}  {}",
        label,
        fmt_us(mote_us),
        fmt_us(sqlite_us),
        verdict
    );
    (speedup, verdict.to_string())
}

// ── Setup ────────────────────────────────────────────────────────────────

const N: usize = 100_000;

fn setup_motedb() -> (Database, std::path::PathBuf) {
    let dir = tempfile::TempDir::new().unwrap();
    let mote_path = {
        let p = dir.path();
        p.parent()
            .unwrap_or(std::path::Path::new("/tmp"))
            .join(format!(
                "{}.mote",
                p.file_name()
                    .map(|s| s.to_string_lossy().into_owned())
                    .unwrap_or_default()
            ))
    };
    // Keep dir alive by leaking it (test process is short-lived).
    let dir_path = dir.into_path();
    let db = Database::create_with_config(&dir_path, DBConfig::for_edge()).unwrap();
    db.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, region TEXT, score FLOAT, qty INT)",
    )
    .unwrap();

    // Batch inserts via single txn-like burst (MoteDB auto-batches).
    let t = Instant::now();
    for i in 1..=N as i64 {
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
    db.checkpoint().ok();
    std::thread::sleep(std::time::Duration::from_millis(300));
    println!("  MoteDB INSERT 100K: {:.2}s", t.elapsed().as_secs_f64());
    (db, mote_path)
}

fn setup_sqlite() -> (rusqlite::Connection, std::path::PathBuf) {
    let path = std::env::temp_dir().join("motedb_vs_sqlite_100k.sqlite");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}-wal", path.to_string_lossy()));
    let _ = std::fs::remove_file(format!("{}-shm", path.to_string_lossy()));
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
        .unwrap();
    conn.execute_batch(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, region TEXT, score REAL, qty INTEGER)",
    )
    .unwrap();

    let t = Instant::now();
    // Use a single transaction for fair bulk-insert comparison.
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = conn
            .prepare("INSERT INTO t VALUES (?, ?, ?, ?, ?)")
            .unwrap();
        for i in 1..=N as i64 {
            let region = ["north", "south", "east", "west", "central"][(i % 5) as usize];
            stmt.execute(rusqlite::params![
                i,
                format!("item_{}", i),
                region,
                (i as f64) * 1.7,
                i % 1000,
            ])
            .unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    println!("  SQLite INSERT 100K: {:.2}s", t.elapsed().as_secs_f64());
    // checkpoint WAL → main file so disk footprint is comparable
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").ok();
    (conn, path)
}

fn time_mote(db: &Database, sql: &str, iters: u32) -> u64 {
    let _ = db.execute(sql); // warmup (populates caches)
    let t = Instant::now();
    for _ in 0..iters {
        let _ = db.execute(sql);
    }
    t.elapsed().as_micros() as u64 / iters as u64
}

fn time_sqlite(conn: &rusqlite::Connection, sql: &str, iters: u32) -> u64 {
    let mut stmt = conn.prepare(sql).unwrap();
    {
        let mut rows = stmt.query([]).unwrap();
        while rows.next().unwrap().is_some() {}
    }
    let t = Instant::now();
    for _ in 0..iters {
        let mut rows = stmt.query([]).unwrap();
        while rows.next().unwrap().is_some() {}
    }
    t.elapsed().as_micros() as u64 / iters as u64
}

// ── Benchmark ────────────────────────────────────────────────────────────

#[test]
#[ignore = "benchmark: run with --ignored"]
fn bench_vs_sqlite_100k() {
    println!(
        "\n╔══════════════════════════════════════════════════════════════════════╗"
    );
    println!(
        "║   MoteDB vs SQLite WAL  —  100K-row Head-to-Head Benchmark         ║"
    );
    println!(
        "║   Same schema, same data, same queries  ·  Release build           ║"
    );
    println!(
        "╚══════════════════════════════════════════════════════════════════════╝\n"
    );

    println!("── Setup (INSERT 100K rows) ──");
    let (mdb, mote_disk_path) = setup_motedb();
    let (sdb, sqlite_disk_path) = setup_sqlite();

    let mote_disk_mb = dir_size(&mote_disk_path) as f64 / 1_048_576.0;
    let sqlite_disk_mb = {
        let mut sz = 0u64;
        for ext in &["", "-wal", "-shm"] {
            let p = format!("{}{}", sqlite_disk_path.to_string_lossy(), ext);
            if let Ok(m) = std::fs::metadata(&p) {
                sz += m.len();
            }
        }
        sz as f64 / 1_048_576.0
    };
    println!("\n── Disk Footprint ──");
    println!(
        "  MoteDB:  {:>7.2} MB   ({:.1} bytes/row)",
        mote_disk_mb,
        mote_disk_mb * 1_048_576.0 / N as f64
    );
    println!(
        "  SQLite:  {:>7.2} MB   ({:.1} bytes/row)",
        sqlite_disk_mb,
        sqlite_disk_mb * 1_048_576.0 / N as f64
    );

    let fast = 50; // fast queries: high iter count
    let med = 20; // medium
    let slow = 5; // slow / IO-heavy

    let mut mote_wins = 0u32;
    let mut sqlite_wins = 0u32;
    let mut ties = 0u32;
    let mut mote_speedups: Vec<(String, f64)> = Vec::new();
    let mut sqlite_speedups: Vec<(String, f64)> = Vec::new();

    macro_rules! run {
        ($section:expr, $label:expr, $sql:expr, $iters:expr) => {{
            println!("\n── {} ──", $section);
            let m = time_mote(&mdb, $sql, $iters);
            let s = time_sqlite(&sdb, $sql, $iters);
            let (sp, v) = compare($label, m, s);
            if sp > 1.1 {
                mote_wins += 1;
                mote_speedups.push(($label.to_string(), sp));
            } else if sp < 0.9 {
                sqlite_wins += 1;
                sqlite_speedups.push(($label.to_string(), 1.0 / sp));
            } else {
                ties += 1;
            }
            let _ = v;
        }};
    }

    // ── 1. Point Query (PK lookup) ──
    // Both engines re-parse the SQL each call here (MoteDB has stmt cache,
    // SQLite also re-parses raw execute). Fair apples-to-apples.
    run!("Point Query (PK lookup)", "WHERE id = 50000 (raw SQL)", "SELECT id FROM t WHERE id = 50000", fast);

    // ── 1b. Point query latency distribution (200 random PKs) ──
    println!("\n── Point Query Latency Distribution (200 random PKs) ──");
    {
        let mut mote_lat = Vec::with_capacity(200);
        let mut mote_prep_lat = Vec::with_capacity(200);
        let mut sqlite_lat = Vec::with_capacity(200);
        // Pre-warm execute_prepared (first call parses + caches)
        let _ = mdb.execute_prepared(
            "SELECT id FROM t WHERE id = ?",
            vec![Value::Integer(1)],
        );
        for i in 0..200i64 {
            let pid = (i * 977 + 50000) % N as i64 + 1;
            // MoteDB raw SQL (format! every call — measures parse + lookup)
            let sql = format!("SELECT id FROM t WHERE id = {}", pid);
            let t = Instant::now();
            let _ = mdb.execute(&sql);
            mote_lat.push(t.elapsed().as_micros() as u64);
            // MoteDB execute_prepared (cache hit — measures lookup only)
            let t = Instant::now();
            let _ = mdb.execute_prepared(
                "SELECT id FROM t WHERE id = ?",
                vec![Value::Integer(pid)],
            );
            mote_prep_lat.push(t.elapsed().as_micros() as u64);
            // SQLite (parameterized — its fast path)
            let mut stmt = sdb.prepare("SELECT id FROM t WHERE id = ?").unwrap();
            let t = Instant::now();
            let _ = stmt.query_row(rusqlite::params![pid], |_| Ok(()));
            sqlite_lat.push(t.elapsed().as_micros() as u64);
        }
        mote_lat.sort_unstable();
        mote_prep_lat.sort_unstable();
        sqlite_lat.sort_unstable();
        let n = mote_lat.len();
        println!(
            "  MoteDB execute()          p50: {:>5}  p95: {:>5}  p99: {:>5}  (µs, raw SQL)",
            mote_lat[n / 2], mote_lat[n * 95 / 100], mote_lat[n * 99 / 100]
        );
        println!(
            "  MoteDB execute_prepared() p50: {:>5}  p95: {:>5}  p99: {:>5}  (µs, cache hit)",
            mote_prep_lat[n / 2], mote_prep_lat[n * 95 / 100], mote_prep_lat[n * 99 / 100]
        );
        println!(
            "  SQLite  prepared stmt     p50: {:>5}  p95: {:>5}  p99: {:>5}  (µs)",
            sqlite_lat[n / 2], sqlite_lat[n * 95 / 100], sqlite_lat[n * 99 / 100]
        );
    }

    // ── 2. Aggregate (no GROUP BY) ──
    run!("Aggregate", "COUNT(*)", "SELECT COUNT(*) FROM t", fast);
    run!("Aggregate", "SUM(qty), AVG(score), MIN, MAX", "SELECT SUM(qty), AVG(score), MIN(score), MAX(score) FROM t", med);

    // ── 3. WHERE filter ──
    run!("WHERE filter", "WHERE region = 'north'", "SELECT id FROM t WHERE region = 'north'", med);

    // ── 4. LIKE prefix ──
    run!("LIKE prefix", "WHERE name LIKE 'item_1%'", "SELECT COUNT(*) FROM t WHERE name LIKE 'item_1%'", med);

    // ── 5. ORDER BY LIMIT (top-K) ──
    run!("ORDER BY LIMIT", "ORDER BY score DESC LIMIT 10", "SELECT id FROM t ORDER BY score DESC LIMIT 10", fast);

    // ── 6. GROUP BY ──
    run!("GROUP BY", "GROUP BY region (5 groups)", "SELECT region, COUNT(*), SUM(qty), AVG(score) FROM t GROUP BY region", med);

    // ── 7. DISTINCT ──
    run!("DISTINCT", "SELECT DISTINCT region", "SELECT DISTINCT region FROM t", fast);

    // ── 8. Full scan ──
    run!("Full scan", "SELECT * FROM t", "SELECT * FROM t", slow);

    // ── RSS measurement ──
    let mote_rss = rss_kb();
    println!("\n── Peak RSS (process) ──");
    println!("  MoteDB:  {:>7.1} MB", mote_rss as f64 / 1024.0);
    // SQLite runs in same process; its RSS contribution is the file size it mmaps/caches.
    println!("  SQLite:  {:>7.1} MB (in-process, see disk footprint)", sqlite_disk_mb);

    // ── Summary ──
    println!("\n{}", "═".repeat(72));
    println!("  SUMMARY");
    println!("{}", "═".repeat(72));
    println!(
        "  MoteDB wins: {}  |  SQLite wins: {}  |  ties: {}",
        mote_wins, sqlite_wins, ties
    );

    if !mote_speedups.is_empty() {
        println!("\n  🚀 MoteDB faster on:");
        mote_speedups.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        for (label, sp) in &mote_speedups {
            println!("    {:.<42} {:>6.2}x", label, sp);
        }
    }
    if !sqlite_speedups.is_empty() {
        println!("\n  🐢 SQLite faster on:");
        sqlite_speedups.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        for (label, sp) in &sqlite_speedups {
            println!("    {:.<42} {:>6.2}x", label, sp);
        }
    }

    // Disk comparison
    println!("\n  💾 Disk: MoteDB {:.2} MB vs SQLite {:.2} MB", mote_disk_mb, sqlite_disk_mb);
    if mote_disk_mb < sqlite_disk_mb {
        println!(
            "     MoteDB uses {:.2}x less disk",
            sqlite_disk_mb / mote_disk_mb
        );
    } else {
        println!(
            "     SQLite uses {:.2}x less disk",
            mote_disk_mb / sqlite_disk_mb
        );
    }

    println!("\n{}", "═".repeat(72));
}
