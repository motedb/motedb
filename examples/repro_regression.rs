//! Regression repro + perf probe. Temporary diagnostic.
use motedb::{Database, DBConfig};
use tempfile::TempDir;
use std::time::Instant;

fn main() {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();

    db.execute("CREATE TABLE sales (id INT PRIMARY KEY AUTO_INCREMENT, customer TEXT, amount FLOAT, region TEXT)").unwrap();

    let n = 60_000usize;
    let batch_size = 5000;
    let t = Instant::now();
    for start in (0..n).step_by(batch_size) {
        let end = (start + batch_size).min(n);
        let mut batch = String::with_capacity(batch_size * 60);
        for i in start..end {
            let region = if i % 3 == 0 { "US" } else { "EU" };
            let customer = format!("cust_{}", i % (n / 10).max(10));
            let amount = (i as f64 * 1.7 + 42.0) % 1000.0;
            batch.push_str(&format!("('{}',{:.2},'{}'),", customer, amount, region));
        }
        batch.truncate(batch.len() - 1);
        db.execute(&format!("INSERT INTO sales (customer, amount, region) VALUES {}", batch)).unwrap();
    }
    eprintln!("[repro] INSERT {} rows: {} ms", n, t.elapsed().as_millis());

    let t = Instant::now();
    db.execute("CREATE INDEX idx_region ON sales (region) USING COLUMN").unwrap();
    let idx1 = t.elapsed().as_millis();
    let t = Instant::now();
    db.execute("CREATE INDEX idx_customer ON sales (customer) USING COLUMN").unwrap();
    let idx2 = t.elapsed().as_millis();
    eprintln!("[repro] CREATE INDEX region:   {} ms", idx1);
    eprintln!("[repro] CREATE INDEX customer: {} ms", idx2);
    eprintln!("[repro] CREATE INDEX total:    {} ms", idx1 + idx2);

    let t = Instant::now();
    let r = db.execute("SELECT id FROM sales WHERE customer IN (SELECT customer FROM sales WHERE region = 'US')").unwrap();
    let cnt = r.materialize().unwrap().row_count();
    eprintln!("[repro] IN subquery: count={} ({} ms) [expect ~{}]", cnt, t.elapsed().as_millis(), n);

    let r = db.execute("SELECT * FROM sales WHERE region = 'US'").unwrap();
    eprintln!("[repro] WHERE region='US': {} (expect ~{})", r.materialize().unwrap().row_count(), n / 3);

    let r = db.execute("SELECT * FROM sales").unwrap();
    eprintln!("[repro] SELECT *: {} (expect {})", r.materialize().unwrap().row_count(), n);

    // Aggregate correctness tests (critical for embedded analytics)
    let r = db.execute("SELECT COUNT(*), SUM(amount), MIN(amount), MAX(amount) FROM sales WHERE region = 'US'").unwrap();
    eprintln!("[repro] COUNT/SUM/MIN/MAX WHERE: {:?}", r.materialize().unwrap().row_count());

    let r = db.execute("SELECT customer, COUNT(*), SUM(amount) FROM sales GROUP BY customer").unwrap();
    eprintln!("[repro] GROUP BY customer: {} groups (expect ~{})", r.materialize().unwrap().row_count(), n / 10);

    let r = db.execute("SELECT * FROM sales ORDER BY amount DESC LIMIT 10").unwrap();
    eprintln!("[repro] ORDER BY LIMIT: {} rows (expect 10)", r.materialize().unwrap().row_count());

    let t = std::time::Instant::now();
    let r = db.execute("SELECT * FROM sales WHERE id = 30000").unwrap();
    let pk_rows = r.materialize().unwrap().row_count();
    eprintln!("[repro] PK point SELECT id=30000: {} rows ({} μs)", pk_rows, t.elapsed().as_micros());
    println!("DONE");
}
