//! Measure P99 latency of all query types after INSERT.
//! First query includes sync compaction (cold), subsequent are warm.
use motedb::{Database, DBConfig};
use tempfile::TempDir;
use std::time::{Duration, Instant};

fn main() {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute("CREATE TABLE sales (id INT PRIMARY KEY AUTO_INCREMENT, customer TEXT, amount FLOAT, region TEXT)").unwrap();

    let n = 300_000usize;
    let bs = 5000;
    for start in (0..n).step_by(bs) {
        let end = (start + bs).min(n);
        let mut batch = String::new();
        for i in start..end {
            let region = if i % 3 == 0 { "US" } else { "EU" };
            if !batch.is_empty() { batch.push(','); }
            batch.push_str(&format!("('cust_{}',{:.2},'{}')", i % 30000, (i as f64 * 1.7) % 1000.0, region));
        }
        db.execute(&format!("INSERT INTO sales (customer, amount, region) VALUES {}", batch)).unwrap();
    }

    // Warmup: trigger sync compaction (cold query)
    let t = Instant::now();
    let _ = db.execute("SELECT COUNT(*) FROM sales").unwrap();
    eprintln!("[p99] first query (sync compaction): {} ms", t.elapsed().as_millis());

    // Measure each query type 20 times, compute P99
    let queries = [
        ("PK point", "SELECT * FROM sales WHERE id = 150000"),
        ("WHERE region", "SELECT * FROM sales WHERE region = 'US'"),
        ("Full scan", "SELECT * FROM sales"),
        ("GROUP BY", "SELECT customer, COUNT(*) FROM sales GROUP BY customer"),
        ("COUNT WHERE", "SELECT COUNT(*), SUM(amount) FROM sales WHERE region = 'US'"),
    ];

    for (name, sql) in &queries {
        let mut times: Vec<Duration> = Vec::with_capacity(20);
        for _ in 0..20 {
            let t = Instant::now();
            let r = db.execute(sql).unwrap();
            let _ = r.materialize().unwrap();
            times.push(t.elapsed());
        }
        times.sort();
        let p50 = times[10];
        let p99 = times[19]; // max of 20 samples ≈ P95-P99
        eprintln!("[p99] {:20} P50={:.2}ms P99={:.2}ms", name, p50.as_secs_f64()*1000.0, p99.as_secs_f64()*1000.0);
    }
    println!("DONE");
}
