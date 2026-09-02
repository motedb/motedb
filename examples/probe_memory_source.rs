//! Probe: identify what grows RSS linearly with data.
//!
/// Inserts rows in batches (queries only COUNT(*) which is O(1) metadata),
/// and at each milestone reports RSS vs on-disk size of the col-segment
/// directory. If RSS-delta tracks disk-delta, the growth is segment
/// file_data/row_map residency (heap), not query caches.
use motedb::{DBConfig, Database};
use std::time::Instant;
use tempfile::TempDir;

fn get_rss_kb() -> u64 {
    let pid = std::process::id();
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()
        .and_then(|o| {
            String::from_utf8_lossy(&o.stdout)
                .trim()
                .parse::<u64>()
                .ok()
        })
        .unwrap_or(0)
}

fn dir_size(p: &std::path::Path) -> u64 {
    let mut total = 0;
    if let Ok(rd) = std::fs::read_dir(p) {
        for e in rd.flatten() {
            if let Ok(md) = e.metadata() {
                if md.is_file() {
                    total += md.len();
                } else if md.is_dir() {
                    total += dir_size(&e.path());
                }
            }
        }
    }
    total
}

fn count_files(p: &std::path::Path) -> usize {
    let mut n = 0;
    if let Ok(rd) = std::fs::read_dir(p) {
        for e in rd.flatten() {
            if let Ok(md) = e.metadata() {
                if md.is_file() {
                    n += 1;
                } else if md.is_dir() {
                    n += count_files(&e.path());
                }
            }
        }
    }
    n
}

fn main() {
    println!("\n  Memory-source probe (insert-only, no scans)");
    let dir = TempDir::new().unwrap();
    // Database lives in the SIBLING directory {stem}.mote (core.rs with_extension).
    let db_dir = dir.path().with_extension("mote");
    let mut config = DBConfig::for_general();
    config.auto_checkpoint = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute(
        "CREATE TABLE t (
        id INT PRIMARY KEY AUTO_INCREMENT,
        name TEXT,
        val FLOAT,
        code TEXT,
        ts BIGINT
    )",
    )
    .unwrap();
    db.execute("CREATE INDEX idx_code ON t (code) USING COLUMN")
        .unwrap();

    let milestones = [100_000usize, 400_000, 800_000, 1_600_000];
    let mut total = 0usize;
    let batch = 5000;
    let mut prev_rss = 0.0f64;
    let mut prev_disk = 0.0f64;

    println!(
        "  {:>9} | {:>8} | {:>9} | {:>10} | {:>11} | {:>12} | {:>8}",
        "rows", "RSS_MB", "disk_MB", "seg_files", "dRSS_MB", "dDisk_MB", "dRSS/dDisk"
    );
    println!("  {}", "-".repeat(80));
    for m in milestones {
        let t = Instant::now();
        while total < m {
            let end = (total + batch).min(m);
            let mut sql = String::with_capacity(batch * 70);
            sql.push_str("INSERT INTO t (name, val, code, ts) VALUES ");
            for i in total..end {
                sql.push_str(&format!(
                    "('user_{}', {:.2}, 'CD_{:04}', {})",
                    i % 1000,
                    (i as f64) * 0.5,
                    i % 1000,
                    1_700_000_000_i64 + i as i64
                ));
                if i + 1 < end {
                    sql.push(',');
                }
            }
            db.execute(&sql).unwrap();
            total = end;
        }
        let el = t.elapsed().as_millis();
        // Force flush + segment materialization (same as any real query would).
        let _ = db.execute("SELECT COUNT(*) FROM t WHERE val > 500");
        std::thread::sleep(std::time::Duration::from_millis(200));
        let rss = get_rss_kb() as f64 / 1024.0;
        let disk = dir_size(&db_dir) as f64 / 1024.0 / 1024.0;
        let files = count_files(&db_dir);
        let (dr, dd) = (rss - prev_rss, disk - prev_disk);
        let ratio = if dd > 0.5 { dr / dd } else { f64::NAN };
        println!(
            "  {:>9} | {:>8.1} | {:>9.1} | {:>10} | {:>11.1} | {:>12.1} | {:>8.2}   [insert {}ms]",
            total, rss, disk, files, dr, dd, ratio, el
        );
        prev_rss = rss;
        prev_disk = disk;
    }
    println!();
}
