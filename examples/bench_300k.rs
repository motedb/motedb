use motedb::{Database, DBConfig};
use tempfile::TempDir;
use std::time::Instant;

fn get_rss_kb() -> u64 {
    let pid = std::process::id();
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output().ok()
        .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse::<u64>().ok())
        .unwrap_or(0)
}

fn setup_db(n: usize) -> (Database, TempDir, u128, u128) {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute("CREATE TABLE sales (id INT PRIMARY KEY AUTO_INCREMENT, customer TEXT, amount FLOAT, region TEXT)").unwrap();
    let batch_size = 5000;
    let t_insert = Instant::now();
    for batch_start in (0..n).step_by(batch_size) {
        let end = (batch_start + batch_size).min(n);
        let mut batch = String::with_capacity(batch_size * 60);
        for i in batch_start..end {
            let region = if i % 3 == 0 { "'US'" } else { "'EU'" };
            let customer = format!("'cust_{}'", i % (n / 10).max(10));
            let amount = (i as f64 * 1.7 + 42.0) % 1000.0;
            batch.push_str(&format!("({},{:.2},{}),", customer, amount, region));
        }
        batch.truncate(batch.len() - 1);
        db.execute(&format!("INSERT INTO sales (customer, amount, region) VALUES {}", batch)).unwrap();
    }
    let insert_ms = t_insert.elapsed().as_millis();
    // Create column indexes for benchmarked WHERE columns.
    // The optimizer uses these to generate PointQuery plans for
    // low-selectivity queries (estimated_rows < 5% of total).
    let t_index = Instant::now();
    db.execute("CREATE INDEX idx_region ON sales (region) USING COLUMN").unwrap();
    db.execute("CREATE INDEX idx_customer ON sales (customer) USING COLUMN").unwrap();
    let index_ms = t_index.elapsed().as_millis();
    (db, dir, insert_ms, index_ms)
}

fn bench<F: FnMut()>(_label: &str, mut f: F) -> u64 {
    f(); // warmup
    let start = Instant::now();
    let iters = 10;
    for _ in 0..iters { f(); }
    start.elapsed().as_micros() as u64 / iters as u64
}

fn rows(db: &Database, sql: &str) -> usize {
    db.execute(sql).unwrap().materialize().unwrap().row_count()
}

fn main() {
    let n = 300_000;
    println!("\n  MoteDB 300K Row Benchmark");
    println!("  {}", "=".repeat(70));
    
    let (db, _dir, insert_ms, index_ms) = setup_db(n);
    let rss = get_rss_kb();
    let total_ms = insert_ms + index_ms;
    println!("  Insert {} rows: {}ms ({} rows/sec)", n, insert_ms, (n as u64 * 1000) / (insert_ms as u64).max(1));
    println!("  Create 2 indexes: {}ms", index_ms);
    println!("  Total setup: {}ms", total_ms);
    println!("  RSS after insert: {} KB ({} B/row)", rss, rss * 1024 / n as u64);
    println!();

    let pk_select_sql = format!("SELECT * FROM sales WHERE id = {}", n / 2);
    let pk_update_sql = format!("UPDATE sales SET amount = 999.99 WHERE id = {}", n / 2);
    let queries: Vec<(&str, &str)> = vec![
        ("SELECT * (full scan)", "SELECT * FROM sales"),
        ("WHERE region='US'", "SELECT * FROM sales WHERE region = 'US'"),
        ("WHERE customer='cust_1'", "SELECT * FROM sales WHERE customer = 'cust_1'"),
        ("GROUP BY + COUNT(*)", "SELECT customer, COUNT(*), SUM(amount), AVG(amount) FROM sales GROUP BY customer"),
        ("ORDER BY + LIMIT 10", "SELECT * FROM sales ORDER BY amount DESC LIMIT 10"),
        ("SELECT DISTINCT", "SELECT DISTINCT region FROM sales"),
        ("IN subquery", "SELECT id FROM sales WHERE customer IN (SELECT customer FROM sales WHERE region = 'US')"),
        ("LIKE 'cust_1%'", "SELECT * FROM sales WHERE customer LIKE 'cust_1%'"),
        ("COUNT(*) WHERE", "SELECT COUNT(*), SUM(amount), MIN(amount), MAX(amount) FROM sales WHERE region = 'US'"),
        ("PK SELECT", &pk_select_sql),
        ("PK UPDATE", &pk_update_sql),
        ("PK DELETE + re-insert", "DELETE FROM sales WHERE id = 1"),
    ];

    println!("  {:45} | {:>8} us | {:>10} rows/ms", "Operation", "us/op", "throughput");
    println!("  {}", "-".repeat(70));
    
    let mut results = Vec::new();
    for (label, sql) in &queries {
        let us = bench(label, &mut || { rows(&db, sql); });
        let row_count = rows(&db, sql);
        let throughput = if us > 0 { n as u64 * 1000 / us } else { 999999999 };
        println!("  {:45} | {:>8} us | {:>10} rows/ms | {} rows", label, us, throughput, row_count);
        results.push((label, us));
    }

    // Memory after all queries
    let rss_after = get_rss_kb();
    println!("\n  RSS after queries: {} KB (Δ {} KB)", rss_after, rss_after as i64 - rss as i64);

    // Linearity check: compare 100K vs 300K
    println!("\n  Linearity (100K → 300K, expect ~3x):");
    let baseline_100k: Vec<(&str, u64)> = vec![
        ("scan", 23016), ("WHERE", 24021), ("GROUP BY", 22446),
        ("ORDER BY", 22632), ("DISTINCT", 20320), ("IN subquery", 55803), ("LIKE", 22961),
    ];
    for (label, t100) in &baseline_100k {
        if let Some((_, t300)) = results.iter().find(|(l, _)| l.contains(label)) {
            let ratio = *t300 as f64 / *t100 as f64;
            let verdict = if ratio <= 3.5 { "OK (linear)" } else if ratio <= 5.0 { "WARN" } else { "BAD" };
            println!("    {:20} {:.1}x  ({} us → {} us)  {}", label, ratio, t100, t300, verdict);
        }
    }
}
