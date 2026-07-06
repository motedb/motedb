//! 大规模性能与内存稳定性基准测试
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

fn purge_memory() {
    #[cfg(feature = "jemalloc")]
    {
        use tikv_jemalloc_ctl::{arenas, epoch};
        let _ = epoch::advance();
        if let Ok(n) = arenas::narenas::read() {
            for i in 0..n {
                let name = format!("arena.{}.purge\0", i);
                let _ = unsafe { tikv_jemalloc_ctl::raw::write(name.as_bytes(), ()) };
            }
        }
    }
}

fn main() {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute(
        "CREATE TABLE bench (id INT PRIMARY KEY AUTO_INCREMENT, val FLOAT, tag TEXT, region TEXT)",
    )
    .unwrap();

    println!("\n  MoteDB 大规模性能与内存基准");
    println!("  {}", "=".repeat(90));
    println!(
        "  {:>8} | {:>8} | {:>12} | {:>12} | {:>12} | {:>12}",
        "行数", "RSS(MB)", "全表扫描", "WHERE过滤", "GROUP BY", "COUNT+SUM"
    );
    println!("  {}", "-".repeat(90));

    let batch_size = 5_000;
    let checkpoints = [50_000usize, 100_000, 200_000, 300_000, 500_000, 1_000_000];
    let mut total_rows = 0usize;
    let mut rss_samples: Vec<(usize, f64)> = Vec::new();
    let mut query_p99s: Vec<(usize, u64, u64, u64, u64)> = Vec::new();

    for &target in &checkpoints {
        while total_rows < target {
            let end = (total_rows + batch_size).min(target);
            let mut batch = String::with_capacity(batch_size * 60);
            for i in total_rows..end {
                let tag = if i % 3 == 0 { "US" } else { "EU" };
                batch.push_str(&format!("({:.1},'{}','{}'),", i as f64, i % 1000, tag));
            }
            batch.truncate(batch.len() - 1);
            db.execute(&format!(
                "INSERT INTO bench (val, tag, region) VALUES {}",
                batch
            ))
            .unwrap();
            total_rows = end;
        }

        purge_memory();
        let rss_before = get_rss_kb() as f64 / 1024.0;

        let measure = |sql: &str| -> (u64, u64) {
            let _ = db.execute(sql).unwrap().row_count();
            let mut times = Vec::with_capacity(10);
            for _ in 0..10 {
                let t = Instant::now();
                let _ = db.execute(sql).unwrap().row_count();
                times.push(t.elapsed().as_micros() as u64);
            }
            times.sort();
            (times[5], times[9])
        };

        let (s50, s99) = measure("SELECT * FROM bench");
        let (w50, w99) = measure("SELECT * FROM bench WHERE tag = 'US'");
        let (g50, g99) = measure("SELECT region, COUNT(*), AVG(val) FROM bench GROUP BY region");
        let (c50, c99) = measure("SELECT COUNT(*), SUM(val) FROM bench WHERE tag = 'US'");

        purge_memory();
        let rss_after = get_rss_kb() as f64 / 1024.0;
        rss_samples.push((total_rows, rss_after));
        query_p99s.push((total_rows, s99, w99, g99, c99));

        println!("  {:>8} | {:>5.1}→{:.1} | {:>5}μs(P:{}) | {:>5}μs(P:{}) | {:>5}μs(P:{}) | {:>5}μs(P:{})",
            total_rows, rss_before, rss_after,
            s99, s50, w99, w50, g99, g50, c99, c50);
    }

    println!("  {}", "-".repeat(90));
    println!("\n  📊 达标分析：");
    let all_p99_ok = query_p99s
        .iter()
        .all(|&(_, s, w, g, c)| s <= 100_000 && w <= 100_000 && g <= 100_000 && c <= 100_000);
    let peak_rss = rss_samples.iter().map(|&(_, r)| r).fold(0.0f64, f64::max);
    let steady_rss = rss_samples.last().map(|&(_, r)| r).unwrap_or(0.0);
    println!(
        "    P99 < 100ms:     {}",
        if all_p99_ok { "✅" } else { "❌" }
    );
    println!(
        "    内存 < 100M:     {} (峰值 {:.1}MB)",
        if peak_rss <= 100.0 { "✅" } else { "❌" },
        peak_rss
    );
    println!(
        "    80% 时间 < 50M:  {} (稳态 {:.1}MB)",
        if steady_rss <= 50.0 { "✅" } else { "❌" },
        steady_rss
    );

    println!("\n  内存曲线：");
    for &(rows, rss) in &rss_samples {
        let bar = "#".repeat((rss / 5.0) as usize);
        println!("    {:>8} rows: {:>6.1} MB {}", rows, rss, bar);
    }
}
