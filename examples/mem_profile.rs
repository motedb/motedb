use motedb::Database;
use tempfile::TempDir;

fn main() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT)").unwrap().materialize().unwrap();

    let baseline = get_rss_kb();
    println!("=== Memory Stabilization Test (Insert-Flush Cycles) ===");
    println!("MemTable: 4MB, pk_lookup: 50K entries, row_cache: 10K entries");
    println!("Baseline: {:.1} MB\n", baseline as f64 / 1024.0);
    println!("{:<8} | {:>8} | {:>10} | {:>12} | {:>12}", "Cycle", "Total", "RSS MB", "Delta MB", "Cycle Δ MB");
    println!("{}", "─".repeat(70));

    // 20K rows per cycle ≈ 4MB MemTable → triggers auto-flush
    let rows_per_cycle = 20_000;
    let total_cycles = 25;
    let mut prev_rss = baseline;
    let mut total_rows = 0i64;

    for cycle in 1..=total_cycles {
        let start = total_rows + 1;
        let end = total_rows + rows_per_cycle as i64;
        for i in start..=end {
            db.execute(&format!(
                "INSERT INTO t VALUES ({}, 'name_{}', {:.1})", i, i, i as f64 * 1.5
            )).unwrap().materialize().unwrap();
        }
        total_rows = end;

        // Flush every cycle
        db.flush().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100));

        let rss = get_rss_kb();
        let delta = rss.saturating_sub(baseline);
        let cycle_delta = rss.saturating_sub(prev_rss);

        if cycle <= 5 || cycle % 5 == 0 {
            println!("{:<8} | {:>6}K | {:>8.1} | {:>10.1} | {:>10.1}",
                cycle, total_rows / 1000, rss as f64 / 1024.0,
                delta as f64 / 1024.0, cycle_delta as f64 / 1024.0);
        }
        prev_rss = rss;
    }

    // Final: check if memory is stable
    let rss_end = get_rss_kb();
    let rss_mid = get_rss_kb(); // measure again to see if it's stable
    println!("\n=== Final Check ===");
    println!("Final RSS: {:.1} MB (variance: {} KB)", rss_end as f64 / 1024.0, (rss_end as i64 - rss_mid as i64).unsigned_abs());

    // Test: do queries to warm up caches, then check RSS
    println!("\n--- Query phase (100 PK lookups) ---");
    for i in 1..=100 {
        db.execute(&format!("SELECT * FROM t WHERE id = {}", i * 100)).unwrap().materialize().unwrap();
    }
    let rss_after_query = get_rss_kb();
    println!("After queries: {:.1} MB (Δ = {} KB)", rss_after_query as f64 / 1024.0,
        (rss_after_query as i64 - rss_end as i64).unsigned_abs());

    println!("\n=== Verdict ===");
    let rss_final = rss_after_query as f64 / 1024.0;
    let rows_k = total_rows / 1000;
    println!("Total: {}K rows, RSS: {:.1} MB, {:.1} B/row", rows_k, rss_final, rss_final * 1024.0 * 1024.0 / total_rows as f64);
    println!("\nKey bounded structures:");
    println!("  pk_lookup: 50K entries × ~80B = ~4MB (LRU, evicts on overflow)");
    println!("  row_cache: 10K entries × ~1KB = ~10MB (LRU)");
    println!("  sstable_cache: 128 entries × ~100KB = ~12.8MB (LRU)");
    println!("  memtable: 4MB (flushes to SSTable on overflow)");
    println!("  Total bounded: ~31MB + OS/allocator overhead");

    drop(db);
}

fn get_rss_kb() -> usize {
    let pid = std::process::id();
    let output = std::process::Command::new("ps")
        .args(["-o", "rss", "-p", &pid.to_string()])
        .output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines().skip(1) {
        if let Ok(rss) = line.trim().parse::<usize>() {
            return rss;
        }
    }
    0
}
