//! MoteDB v0.5.0 vs SQLite — head-to-head on the query paths we optimized.
//!
//! Run: cargo test --release --test bench_vs_sqlite_v05 -- --nocapture --test-threads=1

use motedb::{Database, types::Value, sql::QueryResult};
use rusqlite;
use std::time::Instant;

fn setup_motedb(n: usize) -> Database {
    let dir = "/tmp/motedb_vs_sqlite";
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(format!("{}.mote", &dir));
    let db = Database::create(&dir).unwrap();
    db.execute("CREATE TABLE sales (id INT PRIMARY KEY, region TEXT, product TEXT, qty INT, price FLOAT)").unwrap();
    for i in 1..=n as i64 {
        let r = ["North","South","East","West"][(i%4) as usize];
        let p = ["Laptop","Phone","Tablet","Watch"][(i%4) as usize];
        db.execute(&format!("INSERT INTO sales VALUES ({}, '{}', '{}', {}, {:.2})", i, r, p, i%100, 10.0+(i as f64%500.0))).unwrap();
    }
    // Second table for JOIN
    db.execute("CREATE TABLE prices (id INT PRIMARY KEY, p FLOAT)").unwrap();
    for i in 1..=n as i64 {
        db.execute(&format!("INSERT INTO prices VALUES ({}, {:.2})", i, i as f64 * 1.5)).unwrap();
    }
    db
}

fn setup_sqlite(n: usize) -> rusqlite::Connection {
    let path = "/tmp/motedb_vs_sqlite.sqlite";
    let _ = std::fs::remove_file(&path);
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap();
    conn.execute_batch("CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, product TEXT, qty INTEGER, price REAL)").unwrap();
    for i in 1..=n as i64 {
        let r = ["North","South","East","West"][(i%4) as usize];
        let p = ["Laptop","Phone","Tablet","Watch"][(i%4) as usize];
        conn.execute(
            "INSERT INTO sales VALUES (?, ?, ?, ?, ?)",
            rusqlite::params![i, r, p, i%100, 10.0+(i as f64%500.0)],
        ).unwrap();
    }
    conn.execute_batch("CREATE TABLE prices (id INTEGER PRIMARY KEY, p REAL)").unwrap();
    for i in 1..=n as i64 {
        conn.execute("INSERT INTO prices VALUES (?, ?)", rusqlite::params![i, i as f64 * 1.5]).unwrap();
    }
    conn
}

fn fmt(us: u128) -> String {
    if us < 1_000 { format!("{}µs", us) }
    else if us < 1_000_000 { format!("{:.2}ms", us as f64 / 1000.0) }
    else { format!("{:.2}s", us as f64 / 1_000_000.0) }
}

fn ratio(mote: u128, sqlite: u128) -> String {
    if sqlite == 0 || mote == 0 { return "—".into(); }
    let r = mote as f64 / sqlite as f64;
    if r < 0.95 { format!("{:.2}x 🚀", 1.0/r) }
    else if r > 1.05 { format!("{:.2}x 🐢", r) }
    else { "≈tie".into() }
}

fn bench_line(label: &str, mote_us: u128, sqlite_us: u128, iters: usize) {
    let m = fmt(mote_us / iters as u128);
    let s = fmt(sqlite_us / iters as u128);
    let r = ratio(mote_us, sqlite_us);
    println!("  {:<48} MoteDB {:>8}  SQLite {:>8}  {}", label, m, s, r);
}

fn time_mote(db: &Database, sql: &str, iters: usize) -> u128 {
    let _ = db.execute(sql); // warmup
    let t = Instant::now();
    for _ in 0..iters { let _ = db.execute(sql); }
    t.elapsed().as_micros()
}

fn time_sqlite(conn: &rusqlite::Connection, sql: &str, iters: usize) -> u128 {
    // Prepare once, execute many times (fair comparison — MoteDB also caches parse).
    let mut stmt = conn.prepare(sql).unwrap();
    // warmup
    { let mut rows = stmt.query([]).unwrap(); while rows.next().unwrap().is_some() {} }
    let t = Instant::now();
    for _ in 0..iters {
        let mut rows = stmt.query([]).unwrap();
        while rows.next().unwrap().is_some() {}
    }
    t.elapsed().as_micros()
}

#[test]
fn bench_v050_vs_sqlite() {
    let n: usize = if std::env::var("CI").is_ok() { 2_000 } else { 5_000 };

    println!("\n╔══════════════════════════════════════════════════════════════════╗");
    println!("║   MoteDB v0.5.0 vs SQLite WAL  —  Head-to-Head Benchmark       ║");
    println!("║   Dataset: {} rows  ·  Release build                            ║", n);
    println!("╚══════════════════════════════════════════════════════════════════╝\n");

    let mdb = setup_motedb(n);
    let sdb = setup_sqlite(n);
    let iters_fast = 200;  // fast queries
    let iters_med = 50;    // medium queries
    let iters_slow = 20;   // slow queries

    // ── 1. Point query ──
    println!("── Point Query ──");
    bench_line("WHERE id = 1",
        time_mote(&mdb, "SELECT id FROM sales WHERE id = 1", iters_fast),
        time_sqlite(&sdb, "SELECT id FROM sales WHERE id = 1", iters_fast),
        iters_fast);

    // ── 2. COUNT(*) ──
    println!("\n── Aggregate (no GROUP BY) ──");
    bench_line("COUNT(*)",
        time_mote(&mdb, "SELECT COUNT(*) FROM sales", iters_fast),
        time_sqlite(&sdb, "SELECT COUNT(*) FROM sales", iters_fast),
        iters_fast);
    bench_line("SUM(qty), AVG(price), MIN(qty), MAX(qty)",
        time_mote(&mdb, "SELECT SUM(qty), AVG(price), MIN(qty), MAX(qty) FROM sales", iters_med),
        time_sqlite(&sdb, "SELECT SUM(qty), AVG(price), MIN(qty), MAX(qty) FROM sales", iters_med),
        iters_med);

    // ── 3. ORDER BY LIMIT (top-K) ──
    println!("\n── ORDER BY LIMIT (top-K) ──");
    bench_line("ORDER BY price DESC LIMIT 10",
        time_mote(&mdb, "SELECT id FROM sales ORDER BY price DESC LIMIT 10", iters_fast),
        time_sqlite(&sdb, "SELECT id FROM sales ORDER BY price DESC LIMIT 10", iters_fast),
        iters_fast);
    bench_line("ORDER BY region ASC, price DESC LIMIT 50",
        time_mote(&mdb, "SELECT id FROM sales ORDER BY region ASC, price DESC LIMIT 50", iters_med),
        time_sqlite(&sdb, "SELECT id FROM sales ORDER BY region ASC, price DESC LIMIT 50", iters_med),
        iters_med);

    // ── 4. GROUP BY ──
    println!("\n── GROUP BY ──");
    bench_line("GROUP BY region, COUNT(*)",
        time_mote(&mdb, "SELECT region, COUNT(*) FROM sales GROUP BY region", iters_fast),
        time_sqlite(&sdb, "SELECT region, COUNT(*) FROM sales GROUP BY region", iters_fast),
        iters_fast);
    bench_line("GROUP BY region, product (multi-col)",
        time_mote(&mdb, "SELECT region, product, COUNT(*) FROM sales GROUP BY region, product", iters_med),
        time_sqlite(&sdb, "SELECT region, product, COUNT(*) FROM sales GROUP BY region, product", iters_med),
        iters_med);
    bench_line("GROUP BY region HAVING COUNT(*) > 1",
        time_mote(&mdb, "SELECT region, COUNT(*) FROM sales GROUP BY region HAVING COUNT(*) > 1", iters_med),
        time_sqlite(&sdb, "SELECT region, COUNT(*) FROM sales GROUP BY region HAVING COUNT(*) > 1", iters_med),
        iters_med);
    bench_line("COUNT(DISTINCT region)",
        time_mote(&mdb, "SELECT COUNT(DISTINCT region) FROM sales", iters_med),
        time_sqlite(&sdb, "SELECT COUNT(DISTINCT region) FROM sales", iters_med),
        iters_med);

    // ── 5. JOIN ──
    println!("\n── JOIN ──");
    bench_line("INNER JOIN sales-prices LIMIT 100",
        time_mote(&mdb, "SELECT s.id, p.p FROM sales s INNER JOIN prices p ON s.id = p.id LIMIT 100", iters_slow),
        time_sqlite(&sdb, "SELECT s.id, p.p FROM sales s INNER JOIN prices p ON s.id = p.id LIMIT 100", iters_slow),
        iters_slow);

    // ── 6. Subquery ──
    println!("\n── Subquery ──");
    bench_line("WHERE id IN (SELECT id FROM prices LIMIT 50)",
        time_mote(&mdb, "SELECT id FROM sales WHERE id IN (SELECT id FROM prices LIMIT 50)", iters_slow),
        time_sqlite(&sdb, "SELECT id FROM sales WHERE id IN (SELECT id FROM prices LIMIT 50)", iters_slow),
        iters_slow);
    bench_line("WHERE qty > (SELECT AVG(qty) FROM sales)",
        time_mote(&mdb, "SELECT id FROM sales WHERE qty > (SELECT AVG(qty) FROM sales)", iters_slow),
        time_sqlite(&sdb, "SELECT id FROM sales WHERE qty > (SELECT AVG(qty) FROM sales)", iters_slow),
        iters_slow);

    // ── 7. DISTINCT / LIMIT ──
    println!("\n── DISTINCT / LIMIT ──");
    bench_line("SELECT DISTINCT region",
        time_mote(&mdb, "SELECT DISTINCT region FROM sales", iters_fast),
        time_sqlite(&sdb, "SELECT DISTINCT region FROM sales", iters_fast),
        iters_fast);
    bench_line("LIMIT 50",
        time_mote(&mdb, "SELECT id FROM sales LIMIT 50", iters_fast),
        time_sqlite(&sdb, "SELECT id FROM sales LIMIT 50", iters_fast),
        iters_fast);

    // ── 8. WHERE filter (text equality) ──
    println!("\n── WHERE filter ──");
    bench_line("WHERE region = 'North'",
        time_mote(&mdb, "SELECT id FROM sales WHERE region = 'North'", iters_med),
        time_sqlite(&sdb, "SELECT id FROM sales WHERE region = 'North'", iters_med),
        iters_med);

    // ── 9. SELECT * full scan ──
    println!("\n── Full scan ──");
    bench_line("SELECT * FROM sales",
        time_mote(&mdb, "SELECT * FROM sales", iters_slow),
        time_sqlite(&sdb, "SELECT * FROM sales", iters_slow),
        iters_slow);

    println!("\n╔══════════════════════════════════════════════════════════════════╗");
    println!("║  🚀 = MoteDB faster   🐢 = MoteDB slower   ≈tie = within 5%       ║");
    println!("╚══════════════════════════════════════════════════════════════════╝\n");
}
