//! Distinguish live vs retained allocator memory under a small budget.
use motedb::{DBConfig, Database};
use std::time::Instant;
use tempfile::TempDir;

fn jstats() -> String {
    #[cfg(feature = "jemalloc")]
    {
        use tikv_jemalloc_ctl::{epoch, stats};
        let _ = epoch::advance();
        format!(
            "allocated={:.1}MB active={:.1}MB resident={:.1}MB retained={:.1}MB",
            stats::allocated::read()
                .map(|v| v as f64 / 1048576.0)
                .unwrap_or(0.0),
            stats::active::read()
                .map(|v| v as f64 / 1048576.0)
                .unwrap_or(0.0),
            stats::resident::read()
                .map(|v| v as f64 / 1048576.0)
                .unwrap_or(0.0),
            stats::retained::read()
                .map(|v| v as f64 / 1048576.0)
                .unwrap_or(0.0),
        )
    }
    #[cfg(not(feature = "jemalloc"))]
    "jemalloc off".to_string()
}

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

fn main() {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_general();
    config.auto_checkpoint = None;
    config.col_cache_budget_mb = Some(32);
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT, val FLOAT, code TEXT, ts BIGINT)").unwrap();
    let mut total = 0usize;
    let batch = 5000;
    while total < 3_200_000 {
        let end = (total + batch).min(3_200_000);
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
    println!(
        "after insert:      RSS={:.0}MB {}",
        get_rss_kb() as f64 / 1024.0,
        jstats()
    );
    let t = Instant::now();
    let _ = db.execute("SELECT COUNT(*) FROM t WHERE val > 500");
    println!(
        "filter 1: {:?} RSS={:.0}MB {}",
        t.elapsed(),
        get_rss_kb() as f64 / 1024.0,
        jstats()
    );
    let t = Instant::now();
    let _ = db.execute("SELECT COUNT(*) FROM t WHERE val > 500");
    println!(
        "filter 2: {:?} RSS={:.0}MB {}",
        t.elapsed(),
        get_rss_kb() as f64 / 1024.0,
        jstats()
    );
    let t = Instant::now();
    let _ = db.execute("SELECT code, COUNT(*), AVG(val) FROM t GROUP BY code");
    println!(
        "group:   {:?} RSS={:.0}MB {}",
        t.elapsed(),
        get_rss_kb() as f64 / 1024.0,
        jstats()
    );
    std::thread::sleep(std::time::Duration::from_millis(600));
    println!(
        "settled 600ms:     RSS={:.0}MB {}",
        get_rss_kb() as f64 / 1024.0,
        jstats()
    );
    println!("{}", db.debug_memory_report());
}
