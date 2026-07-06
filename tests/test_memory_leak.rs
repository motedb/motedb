//! Memory leak detection test: verify RSS doesn't grow under sustained query workload

use motedb::{DBConfig, Database, QueryResult};
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

fn setup_db(n: usize) -> (Database, TempDir) {
    let dir = TempDir::new().expect("temp dir");
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).expect("create db");
    db.execute(
        "CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT, val FLOAT, tag TEXT)",
    )
    .unwrap();

    let mut batch = String::with_capacity(n * 60);
    for i in 0..n {
        let tag = if i % 3 == 0 { "'A'" } else { "'B'" };
        let name = format!("'item_{}'", i % 100);
        let val = (i as f64 * 1.7 + 42.0) % 1000.0;
        batch.push_str(&format!("({},{:.2},{}),", name, val, tag));
        if batch.len() > 500_000 || i == n - 1 {
            batch.truncate(batch.len() - 1);
            db.execute(&format!("INSERT INTO t (name, val, tag) VALUES {}", batch))
                .unwrap();
            batch.clear();
        }
    }
    // Flush to measure steady-state RSS (not peak write RSS)
    db.flush().ok();
    (db, dir)
}

/// Check that repeated queries don't cause RSS to grow unboundedly.
/// If RSS grows >50% after the warmup phase, it suggests a leak.
#[test]
fn test_no_memory_leak_under_load() {
    let (db, _dir) = setup_db(50_000);

    let queries = vec![
        "SELECT * FROM t",
        "SELECT * FROM t WHERE tag = 'A'",
        "SELECT name, COUNT(*), SUM(val), AVG(val) FROM t GROUP BY name",
        "SELECT * FROM t ORDER BY val DESC LIMIT 10",
        "SELECT DISTINCT tag FROM t",
        "SELECT id FROM t WHERE name IN (SELECT name FROM t WHERE tag = 'A')",
        "SELECT * FROM t WHERE name LIKE 'item_1%'",
        "SELECT COUNT(*), SUM(val), MIN(val), MAX(val), AVG(val) FROM t WHERE tag = 'A'",
    ];

    // Warmup: run all queries once to populate any caches
    for sql in &queries {
        let r = db.execute(sql).unwrap().materialize().unwrap();
        drop(r);
    }

    // Measure baseline RSS after warmup
    let rss_baseline = get_rss_kb();
    println!(
        "\n  MEMORY LEAK TEST (50K rows, {} queries/round)",
        queries.len()
    );
    println!("  {}", "=".repeat(60));
    println!("  Baseline RSS: {} KB", rss_baseline);

    let rounds = 50;
    let mut rss_samples: Vec<u64> = Vec::new();
    let mut latencies: Vec<u64> = Vec::new();

    for round in 0..rounds {
        let start = Instant::now();
        for sql in &queries {
            let r = db.execute(sql).unwrap().materialize().unwrap();
            drop(r);
        }
        let elapsed_us = start.elapsed().as_micros() as u64;

        let rss = get_rss_kb();
        rss_samples.push(rss);
        latencies.push(elapsed_us);

        if round % 10 == 9 {
            let delta_kb = rss as i64 - rss_baseline as i64;
            let pct = if rss_baseline > 0 {
                delta_kb as f64 / rss_baseline as f64 * 100.0
            } else {
                0.0
            };
            println!(
                "  Round {:>3}/{} | RSS {:>8} KB | Δ {:+>6} KB ({:+.1}%) | Latency {:>6} us",
                round + 1,
                rounds,
                rss,
                delta_kb,
                pct,
                elapsed_us
            );
        }
    }

    // Analysis
    let rss_first = rss_samples[0];
    let rss_last = *rss_samples.last().unwrap();
    let rss_max = *rss_samples.iter().max().unwrap();
    let rss_growth_pct = (rss_last as f64 - rss_first as f64) / rss_first as f64 * 100.0;

    let lat_first = latencies[0];
    let lat_last = *latencies.last().unwrap();
    let lat_avg_first5: u64 = latencies[..5].iter().sum::<u64>() / 5;
    let lat_avg_last5: u64 = latencies[latencies.len() - 5..].iter().sum::<u64>() / 5;
    let lat_growth_pct =
        (lat_avg_last5 as f64 - lat_avg_first5 as f64) / lat_avg_first5 as f64 * 100.0;

    println!("\n  {}", "-".repeat(60));
    println!(
        "  RSS:  first {} KB → last {} KB → max {} KB | growth {:+.1}%",
        rss_first, rss_last, rss_max, rss_growth_pct
    );
    println!(
        "  Lat:  avg(first 5) {} us → avg(last 5) {} us | growth {:+.1}%",
        lat_avg_first5, lat_avg_last5, lat_growth_pct
    );

    // Verdict
    let rss_ok = rss_growth_pct < 30.0; // < 30% growth over 50 rounds = no leak
    let lat_ok = lat_growth_pct < 50.0; // < 50% latency growth = no degradation

    println!();
    if rss_ok && lat_ok {
        println!("  ✅ PASS: No memory leak or latency degradation detected");
    } else {
        if !rss_ok {
            println!(
                "  ❌ FAIL: RSS grew {:+.1}% — possible memory leak",
                rss_growth_pct
            );
        }
        if !lat_ok {
            println!(
                "  ❌ FAIL: Latency grew {:+.1}% — possible degradation",
                lat_growth_pct
            );
        }
    }

    assert!(
        rss_ok,
        "RSS grew {:+.1}% over {} rounds — possible memory leak",
        rss_growth_pct, rounds
    );
    assert!(
        lat_ok,
        "Latency grew {:+.1}% over {} rounds — possible degradation",
        lat_growth_pct, rounds
    );
}

/// Test that streaming (for_each) uses O(1) memory vs materialize which uses O(N).
/// Compare RSS growth between materialize() and for_each() on a large result set.
#[test]
fn test_streaming_vs_materialize_memory() {
    use motedb::{ForEachResult, StreamingControl};

    let (db, _dir) = setup_db(100_000);

    // Materialize: loads all rows into memory
    let rss_before = get_rss_kb();
    let mat_result = db
        .execute("SELECT * FROM t")
        .unwrap()
        .materialize()
        .unwrap();
    let rss_after_mat = get_rss_kb();
    let mat_rows = match &mat_result {
        QueryResult::Select { rows, .. } => rows.len(),
        _ => 0,
    };
    let mat_rss_delta = rss_after_mat as i64 - rss_before as i64;
    drop(mat_result);

    println!("\n  STREAMING vs MATERIALIZE (100K rows)");
    println!("  {}", "=".repeat(50));
    println!(
        "  Materialize: {} rows, RSS Δ = {:+} KB",
        mat_rows, mat_rss_delta
    );

    // for_each: processes rows one at a time
    let rss_before = get_rss_kb();
    let mut count = 0usize;
    let for_each_result: ForEachResult = db
        .execute("SELECT * FROM t")
        .unwrap()
        .for_each(
            |_cols, _row| {
                count += 1;
                Ok(StreamingControl::Continue)
            },
            None,
        )
        .unwrap();
    let rss_after_stream = get_rss_kb();
    let stream_rss_delta = rss_after_stream as i64 - rss_before as i64;

    println!(
        "  for_each:    {} rows, RSS Δ = {:+} KB",
        count, stream_rss_delta
    );
    println!(
        "  Ratio:       materialize uses {:.1}x more RSS than streaming",
        if stream_rss_delta > 0 {
            mat_rss_delta as f64 / stream_rss_delta as f64
        } else {
            f64::INFINITY
        }
    );

    // Streaming should use significantly less memory
    assert!(
        stream_rss_delta < mat_rss_delta,
        "Streaming RSS delta ({}) should be less than materialize ({})",
        stream_rss_delta,
        mat_rss_delta
    );
    assert_eq!(count, 100_000, "for_each should process all 100K rows");
}
