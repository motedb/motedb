use motedb::{Database, DBConfig};
use tempfile::TempDir;

fn rss_kb() -> usize {
    let pid = std::process::id();
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output().ok()
        .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse::<usize>().ok())
        .unwrap_or(0)
}

fn main() {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute("CREATE TABLE sensors (id INT PRIMARY KEY AUTO_INCREMENT, device TEXT, value FLOAT, region TEXT)").unwrap();

    // INSERT 300K rows (realistic: sensor data ingestion)
    let n = 300_000usize;
    let bs = 5000;
    for start in (0..n).step_by(bs) {
        let end = (start + bs).min(n);
        let mut batch = String::new();
        for i in start..end {
            let region = if i % 3 == 0 { "US" } else { "EU" };
            if !batch.is_empty() { batch.push(','); }
            batch.push_str(&format!("('dev_{}',{:.2},'{}')", i % 100, (i as f64 * 1.7) % 1000.0, region));
        }
        db.execute(&format!("INSERT INTO sensors (device, value, region) VALUES {}", batch)).unwrap();
    }
    eprintln!("[real] after {} INSERT: {} KB", n, rss_kb());

    // Realistic queries: PK point, WHERE filter, COUNT (not SELECT *)
    let _ = db.execute("SELECT * FROM sensors WHERE id = 150000").unwrap();
    eprintln!("[real] after PK query: {} KB", rss_kb());

    let _ = db.execute("SELECT COUNT(*) FROM sensors WHERE region = 'US'").unwrap();
    eprintln!("[real] after COUNT: {} KB", rss_kb());

    let _ = db.execute("SELECT * FROM sensors WHERE region = 'US' LIMIT 10").unwrap();
    eprintln!("[real] after WHERE LIMIT 10: {} KB", rss_kb());

    println!("DONE");
}
