/// Columnar vs Row-based scan benchmark
use motedb::{Database, DBConfig, QueryResult};
use tempfile::TempDir;
use std::time::Instant;

fn main() {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT, val FLOAT, region TEXT)").unwrap();
    db.execute("CREATE INDEX idx_region ON t (region) USING COLUMN").unwrap();

    let n = 300_000;
    println!("Inserting {} rows...", n);
    let t0 = Instant::now();
    for batch_start in (0..n).step_by(5000) {
        let end = (batch_start + 5000).min(n);
        let mut sql = String::from("INSERT INTO t (name, val, region) VALUES ");
        for i in batch_start..end {
            let region = if i % 3 == 0 { "'US'" } else { "'EU'" };
            sql.push_str(&format!("('user_{}',{:.2},{}),", i % 5000, i as f64 * 1.7 + 42.0, region));
        }
        sql.pop();
        db.execute(&sql).unwrap();
    }
    println!("Insert: {}ms", t0.elapsed().as_millis());

    // Row-based scan
    let t0 = Instant::now();
    let r1 = db.execute("SELECT * FROM t").unwrap().materialize().unwrap();
    let row_ms = t0.elapsed().as_millis();
    let row_count = match &r1 { QueryResult::Select { rows, .. } => rows.len(), _ => 0 };
    println!("Row scan: {}ms ({} rows)", row_ms, row_count);

    // Vacuum to build columnar SSTable
    println!("Vacuuming...");
    let t0 = Instant::now();
    db.vacuum().unwrap();
    println!("Vacuum: {}ms", t0.elapsed().as_millis());

    // Columnar scan
    let t0 = Instant::now();
    let r2 = db.execute("SELECT * FROM t").unwrap().materialize().unwrap();
    let col_ms = t0.elapsed().as_millis();
    let col_count = match &r2 { QueryResult::Select { rows, .. } => rows.len(), _ => 0 };
    println!("Columnar scan: {}ms ({} rows)", col_ms, col_count);

    println!("\n=== RESULT ===");
    println!("Row-based:  {}ms", row_ms);
    println!("Columnar:   {}ms", col_ms);
    println!("Speedup:    {:.1}x", row_ms as f64 / col_ms.max(1) as f64);
    println!("Row count:  {} / {}", row_count, col_count);
}
