//! 2M-row benchmark: memory must NOT grow with data size.
//! Run: cargo test --release --test bench_2m -- --nocapture --ignored --test-threads=1

use motedb::{DBConfig, Database};
use std::time::Instant;
use tempfile::TempDir;

fn rss_kb() -> usize {
    #[cfg(target_os = "macos")]
    {
        let pid = std::process::id();
        let out = std::process::Command::new("ps")
            .args(["-o", "rss", "-p", &pid.to_string()])
            .output()
            .ok();
        if let Some(o) = out {
            let s = String::from_utf8_lossy(&o.stdout);
            for line in s.lines().skip(1) {
                if let Ok(v) = line.trim().parse::<usize>() {
                    return v;
                }
            }
        }
        0
    }
    #[cfg(not(target_os = "macos"))]
    {
        0
    }
}

fn mb(kb: usize) -> f64 {
    kb as f64 / 1024.0
}

fn dir_size(path: &std::path::Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                total += dir_size(&p);
            } else if let Ok(m) = e.metadata() {
                total += m.len();
            }
        }
    }
    total
}

#[test]
#[ignore = "2M-row bench: run with --ignored"]
fn bench_2m_memory_and_perf() {
    let n: usize = 2_000_000;
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_edge()).unwrap();

    println!("\n{}", "═".repeat(80));
    println!("  2M-Row Benchmark  |  Target: ≤30MB daily, ≤60MB peak, zero data-dependent growth");
    println!("{}", "═".repeat(80));

    let _rss0 = rss_kb();
    println!("  Baseline RSS (empty DB):         {:>8.1} MB", mb(_rss0));

    // ── Schema ──
    db.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, cat TEXT, score FLOAT, qty INT)",
    )
    .unwrap();

    // ── Bulk INSERT using batch ──
    let t0 = Instant::now();
    let batch = 50_000;
    let mut id = 1i64;
    while id <= n as i64 {
        let end = (id + batch - 1).min(n as i64);
        // Use multi-row INSERT via API batch for speed
        for i in id..=end {
            let cat = ["alpha", "beta", "gamma", "delta", "epsilon"][(i as usize) % 5];
            db.execute(&format!(
                "INSERT INTO t VALUES ({}, 'name_{}', '{}', {:.1}, {})",
                i,
                i,
                cat,
                i as f64 * 0.1,
                i % 1000
            ))
            .unwrap();
        }
        id = end + 1;
    }
    let insert_ms = t0.elapsed().as_millis();
    let rss_after_insert = rss_kb();
    println!("  INSERT 2M rows:                  {:>8} ms", insert_ms);
    println!(
        "  RSS after INSERT:                {:>8.1} MB  (Δ {:>+5.1} MB)",
        mb(rss_after_insert),
        mb(rss_after_insert) - mb(rss0)
    );

    // ── Force checkpoint / flush to measure steady-state disk ──
    db.checkpoint().ok();
    std::thread::sleep(std::time::Duration::from_millis(500));

    // MoteDB stores files under {path}.mote/ (sibling of the path passed to
    // create_with_config). The temp dir path has no extension, so .mote is
    // appended: /tmp/xxx → /tmp/xxx.mote
    let ds_mote = {
        let p = dir.path();
        // path.with_extension("mote") for a path without extension
        let parent = p.parent().unwrap_or(std::path::Path::new("/tmp"));
        let name = p
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        parent.join(format!("{}.mote", name))
    };

    // Detailed disk breakdown
    fn print_dir_breakdown(path: &std::path::Path, label: &str) -> u64 {
        if !path.exists() {
            return 0;
        }
        let mut total = 0u64;
        let mut entries: Vec<_> = std::fs::read_dir(path).unwrap().flatten().collect();
        entries.sort_by_key(|e| e.path());
        for e in &entries {
            let p = e.path();
            if p.is_dir() {
                let sub = print_dir_breakdown(&p, "");
                total += sub;
                let name = p
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default();
                let count = std::fs::read_dir(&p).map(|d| d.count()).unwrap_or(0);
                println!(
                    "    {:<25} {:>10.1} MB ({} entries)",
                    name,
                    sub as f64 / 1_048_576.0,
                    count
                );
            } else if let Ok(m) = e.metadata() {
                total += m.len();
            }
        }
        total
    }

    let ds = if ds_mote.exists() {
        println!("\n  Disk breakdown ({:?}):", ds_mote.file_name());
        print_dir_breakdown(&ds_mote, "")
    } else {
        dir_size(dir.path())
    };
    println!(
        "  Disk after INSERT:               {:>8.1} MB  ({:.1} bytes/row)",
        ds as f64 / 1_048_576.0,
        ds as f64 / n as f64
    );

    // ── Query benchmarks ──
    println!("\n  ── Query Performance ──");

    // COUNT(*)
    let iters = 10;
    let _ = db.execute("SELECT COUNT(*) FROM t");
    let t = Instant::now();
    for _ in 0..iters {
        let _ = db.execute("SELECT COUNT(*) FROM t");
    }
    println!(
        "  COUNT(*):                        {:>8.2} ms/iter",
        t.elapsed().as_secs_f64() * 1000.0 / iters as f64
    );

    // COUNT(*) WHERE cat = 'alpha'
    let _ = db.execute("SELECT COUNT(*) FROM t WHERE cat = 'alpha'");
    let t = Instant::now();
    for _ in 0..iters {
        let _ = db.execute("SELECT COUNT(*) FROM t WHERE cat = 'alpha'");
    }
    println!(
        "  COUNT(*) WHERE eq:               {:>8.2} ms/iter",
        t.elapsed().as_secs_f64() * 1000.0 / iters as f64
    );

    // LIKE
    let _ = db.execute("SELECT COUNT(*) FROM t WHERE name LIKE 'name_1%'");
    let t = Instant::now();
    for _ in 0..iters {
        let _ = db.execute("SELECT COUNT(*) FROM t WHERE name LIKE 'name_1%'");
    }
    println!(
        "  COUNT(*) WHERE LIKE prefix:      {:>8.2} ms/iter",
        t.elapsed().as_secs_f64() * 1000.0 / iters as f64
    );

    // Aggregate SUM/AVG
    let _ = db.execute("SELECT SUM(qty), AVG(score), MIN(score), MAX(score) FROM t");
    let t = Instant::now();
    for _ in 0..iters {
        let _ = db.execute("SELECT SUM(qty), AVG(score), MIN(score), MAX(score) FROM t");
    }
    println!(
        "  SUM/AVG/MIN/MAX (2 cols):        {:>8.2} ms/iter",
        t.elapsed().as_secs_f64() * 1000.0 / iters as f64
    );

    // GROUP BY
    let _ = db.execute("SELECT cat, COUNT(*) FROM t GROUP BY cat");
    let t = Instant::now();
    for _ in 0..(iters / 2).max(1) {
        let _ = db.execute("SELECT cat, COUNT(*) FROM t GROUP BY cat");
    }
    let gb_iters = (iters / 2).max(1);
    println!(
        "  GROUP BY cat COUNT(*):           {:>8.2} ms/iter",
        t.elapsed().as_secs_f64() * 1000.0 / gb_iters as f64
    );

    // ORDER BY LIMIT
    let _ = db.execute("SELECT id FROM t ORDER BY score DESC LIMIT 10");
    let t = Instant::now();
    for _ in 0..iters {
        let _ = db.execute("SELECT id FROM t ORDER BY score DESC LIMIT 10");
    }
    println!(
        "  ORDER BY score DESC LIMIT 10:    {:>8.2} ms/iter",
        t.elapsed().as_secs_f64() * 1000.0 / iters as f64
    );

    // Point query (PK)
    let pid = n / 2;
    let _ = db.execute(&format!("SELECT * FROM t WHERE id = {}", pid));
    let t = Instant::now();
    for i in 0..1000 {
        let _ = db.execute(&format!("SELECT * FROM t WHERE id = {}", (pid + i) % n));
    }
    println!(
        "  Point PK lookup:                 {:>8.2} µs/iter",
        t.elapsed().as_micros() as f64 / 1000.0
    );

    // ── Peak memory during heaviest query ──
    let rss_before_peak = rss_kb();
    let _ = db.execute("SELECT cat, COUNT(*), SUM(qty) FROM t GROUP BY cat");
    let rss_peak = rss_kb();
    println!(
        "\n  RSS before heavy query:          {:>8.1} MB",
        mb(rss_before_peak)
    );
    println!(
        "  RSS peak during GROUP BY:        {:>8.1} MB",
        mb(rss_peak)
    );
    println!(
        "  RSS after (steady):              {:>8.1} MB",
        mb(rss_kb())
    );

    println!("\n  ── Summary ──");
    println!(
        "  Steady-state RSS:                {:>8.1} MB  (target ≤30 MB) {}",
        mb(rss_kb()),
        if mb(rss_kb()) <= 30.0 { "✅" } else { "❌" }
    );
    println!(
        "  Peak RSS:                        {:>8.1} MB  (target ≤60 MB) {}",
        mb(rss_peak.max(rss_after_insert)),
        if mb(rss_peak.max(rss_after_insert) as usize) <= 60.0 {
            "✅"
        } else {
            "❌"
        }
    );
    println!(
        "  Disk:                            {:>8.1} MB  ({:.1} bytes/row)",
        ds as f64 / 1_048_576.0,
        ds as f64 / n as f64
    );
    println!("{}", "═".repeat(80));

    drop(db);
    let _ = dir; // keep temp dir alive
}
