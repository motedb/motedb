/// 内存线性增长测试 v2：关注增量 B/row（排除冷启动开销）
use motedb::config::DBConfig;
use motedb::Database;
use std::time::Instant;
use tempfile::TempDir;

fn is_ci() -> bool {
    std::env::var("CI").is_ok()
}

fn exec(db: &Database, sql: &str) {
    let _ = db
        .execute(sql)
        .expect("execute SQL")
        .materialize()
        .expect("materialize");
}

fn get_rss_mb() -> f64 {
    let pid = std::process::id();
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .expect("ps command");
    let rss_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
    rss_str.parse::<f64>().unwrap_or(0.0) / 1024.0
}

#[test]
fn test_memory_linear_growth() {
    println!("\n{}", "=".repeat(100));
    println!("  Memory Linear Growth Test v2 — incremental ΔRSS per batch");
    println!("{}", "=".repeat(100));

    let dir = TempDir::new().expect("temp dir");
    let db = Database::create_with_config(dir.path(), DBConfig::for_edge()).expect("create db");
    exec(&db, "CREATE TABLE mem_test (id INTEGER PRIMARY KEY, name TEXT, score FLOAT, tag TEXT, data TEXT)");

    let warmup_rows = if is_ci() { 2_000 } else { 10_000 };
    let batch_size = if is_ci() { 2_000 } else { 10_000 };
    let num_batches = if is_ci() { 3 } else { 10 };

    // ── Phase 1: Warm up (absorb cold-start allocations) ──
    println!("\n  Phase 1: Warm-up ({} rows)", warmup_rows);
    for i in 1..=warmup_rows as i64 {
        exec(
            &db,
            &format!(
                "INSERT INTO mem_test VALUES ({}, 'name_{}', {:.3}, 'tag_{}', 'data_{}')",
                i,
                i,
                i as f64 * 0.123,
                i % 20,
                i
            ),
        );
    }
    db.flush().expect("flush");
    db.wait_for_indexes_ready();
    let rss_warm = get_rss_mb();
    let warm_rows = warmup_rows;
    println!(
        "  Warm-up done: {} rows, RSS: {:.1} MB",
        warm_rows, rss_warm
    );

    // ── Phase 2: Incremental inserts — measure ΔRSS per batch ──
    println!("\n{}", "-".repeat(100));
    println!(
        "  {:>10} {:>12} {:>10} {:>10} {:>10} {:>10} {:>8}",
        "TotalRows", "BatchRows", "RSS(MB)", "ΔRSS(MB)", "ΔB/row", "CumB/row", "ops/s"
    );
    println!("{}", "-".repeat(100));

    let mut total_rows = warm_rows;
    let mut prev_rss = rss_warm;
    let mut deltas: Vec<(usize, f64)> = vec![]; // (batch_size, delta_mb)

    for _b in 0..num_batches {
        let start = Instant::now();
        for i in 1..=batch_size as i64 {
            let id = total_rows as i64 + i;
            exec(
                &db,
                &format!(
                    "INSERT INTO mem_test VALUES ({}, 'name_{}', {:.3}, 'tag_{}', 'data_{}')",
                    id,
                    id,
                    id as f64 * 0.123,
                    id % 20,
                    id
                ),
            );
        }
        let elapsed_ms = start.elapsed().as_millis();
        total_rows += batch_size;

        db.flush().expect("flush");
        db.wait_for_indexes_ready();

        let rss = get_rss_mb();
        let delta = rss - prev_rss;
        let delta_bytes_per_row = (delta * 1024.0 * 1024.0) / batch_size as f64;
        let cum_bytes_per_row =
            ((rss - rss_warm) * 1024.0 * 1024.0) / (total_rows - warm_rows) as f64;
        let ops_s = batch_size as f64 / (elapsed_ms as f64 / 1000.0);

        println!(
            "  {:>10} {:>12} {:>10.1} {:>10.1} {:>10.0} {:>10.0} {:>8.0}",
            total_rows, batch_size, rss, delta, delta_bytes_per_row, cum_bytes_per_row, ops_s
        );

        deltas.push((batch_size, delta));
        prev_rss = rss;
    }

    // ── Analysis ──
    let final_rows = total_rows;
    let final_rss = prev_rss;
    let total_delta = final_rss - rss_warm;
    let total_delta_rows = final_rows - warm_rows;

    println!("\n{}", "-".repeat(100));
    println!("  Analysis: {} to {} rows", warm_rows, final_rows);
    println!("{}", "-".repeat(100));

    // Incremental bytes/row for each batch
    let inc_bpr: Vec<f64> = deltas
        .iter()
        .map(|&(n, d)| (d * 1024.0 * 1024.0) / n as f64)
        .collect();
    let avg_bpr = inc_bpr.iter().sum::<f64>() / inc_bpr.len() as f64;
    let min_bpr = inc_bpr.iter().cloned().fold(f64::INFINITY, f64::min);
    let max_bpr = inc_bpr.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let variance: f64 =
        inc_bpr.iter().map(|&x| (x - avg_bpr).powi(2)).sum::<f64>() / inc_bpr.len() as f64;
    let stddev = variance.sqrt();
    let cv = stddev / avg_bpr * 100.0; // coefficient of variation

    println!("  Incremental ΔB/row per 10K batch:");
    for (i, &bpr) in inc_bpr.iter().enumerate() {
        println!("    Batch {}: {:.0} B/row", i + 1, bpr);
    }
    println!("\n  Average incremental: {:.0} B/row", avg_bpr);
    println!(
        "  Std deviation:       {:.0} B/row (CV: {:.1}%)",
        stddev, cv
    );
    println!(
        "  Range:               {:.0} - {:.0} B/row",
        min_bpr, max_bpr
    );
    println!(
        "  Overall:             {:.0} B/row ({} rows, {:.1} MB)",
        total_delta * 1024.0 * 1024.0 / total_delta_rows as f64,
        total_delta_rows,
        total_delta
    );

    // Linear regression on incremental batches
    let n = deltas.len() as f64;
    let mut cum_rows = 0usize;
    let mut points: Vec<(f64, f64)> = vec![];
    for (i, &(batch, _delta)) in deltas.iter().enumerate() {
        cum_rows += batch;
        points.push((
            cum_rows as f64,
            rss_warm + deltas[..=i].iter().map(|&(_, d)| d).sum::<f64>(),
        ));
    }
    let sum_x: f64 = points.iter().map(|(x, _)| *x).sum();
    let sum_y: f64 = points.iter().map(|(_, y)| *y).sum();
    let sum_xy: f64 = points.iter().map(|(x, y)| x * y).sum();
    let sum_x2: f64 = points.iter().map(|(x, _)| x * x).sum();
    let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x.powi(2));
    let mean_y = sum_y / n;
    let ss_tot: f64 = points.iter().map(|(_, y)| (y - mean_y).powi(2)).sum();
    let ss_res: f64 = points
        .iter()
        .map(|(x, y)| (y - (slope * x + (sum_y - slope * sum_x) / n)).powi(2))
        .sum();
    let r_squared = 1.0 - ss_res / ss_tot;
    let slope_bytes = slope * 1024.0 * 1024.0;

    println!(
        "\n  Linear regression: RSS = {:.0} B/row × N + offset",
        slope_bytes
    );
    println!("  R² = {:.6}", r_squared);

    // Verdict
    println!("\n{}", "=".repeat(100));
    if r_squared > 0.98 {
        println!(
            "  ✓ Memory growth is LINEAR (R² = {:.4}, slope = {:.0} B/row)",
            r_squared, slope_bytes
        );
    } else {
        println!(
            "  ✗ Memory growth may NOT be linear (R² = {:.4})",
            r_squared
        );
    }
    if cv < 30.0 {
        println!(
            "  ✓ Per-batch ΔB/row is STABLE (CV = {:.1}%, avg = {:.0} B/row)",
            cv, avg_bpr
        );
    } else {
        println!(
            "  ~ Per-batch ΔB/row varies (CV = {:.1}%, range {:.0}-{:.0} B/row)",
            cv, min_bpr, max_bpr
        );
    }

    // File size check
    db.checkpoint().expect("checkpoint");
    let mote_dir = dir.path().with_extension("mote");
    let dir_size = get_directory_size_mb(&mote_dir);
    let disk_bpr = if dir_size > 0.01 && final_rows > 0 {
        dir_size * 1024.0 * 1024.0 / final_rows as f64
    } else {
        0.0
    };
    let sst_size = get_directory_size_mb(&mote_dir.join("lsm"));
    let wal_size = get_directory_size_mb(&mote_dir.join("wal"));
    println!(
        "  Disk: {:.1} MB total (SST: {:.1} MB, WAL: {:.1} MB) = {:.0} B/row on disk",
        dir_size, sst_size, wal_size, disk_bpr
    );

    db.close().expect("close");
}

fn get_directory_size_mb(path: &std::path::Path) -> f64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_file() {
                if let Ok(m) = p.metadata() {
                    total += m.len();
                }
            } else if p.is_dir() {
                total += get_directory_size_bytes(&p);
            }
        }
    }
    total as f64 / (1024.0 * 1024.0)
}

fn get_directory_size_bytes(path: &std::path::Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_file() {
                if let Ok(m) = p.metadata() {
                    total += m.len();
                }
            } else if p.is_dir() {
                total += get_directory_size_bytes(&p);
            }
        }
    }
    total
}
