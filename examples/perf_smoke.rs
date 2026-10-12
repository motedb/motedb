//! Performance regression smoke gate.
//!
//! Measures five core query shapes and asserts each against the full-scan
//! baseline AS A RATIO — machine-independent, so it holds on dev laptops and
//! CI runners alike. Ratios carry ~2× headroom over current measurements;
//! they exist to catch complexity-class regressions (the O(N²) text-eq bug
//! that shipped as 568s vs 8ms would blow past these by 1000×), not to track
//! few-percent noise.
//!
//! Run: cargo run --release --example perf_smoke

use motedb::{DBConfig, Database};
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn median_of(v: &mut [Duration]) -> Duration {
    v.sort();
    v[v.len() / 2]
}

fn bench(db: &Database, sql: &str, rounds: usize) -> Duration {
    // One warmup (page/col caches) + timed rounds, median.
    let _ = db.execute(sql);
    let mut times = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let t = Instant::now();
        let r = db.execute(sql).unwrap();
        let _ = r.row_count();
        times.push(t.elapsed());
    }
    median_of(&mut times)
}

fn main() {
    let n = 200_000;
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute("CREATE TABLE sales (id INT PRIMARY KEY, customer TEXT, amount FLOAT, region TEXT)")
        .unwrap();
    for chunk in 0..(n / 5000) {
        let mut sql = String::from("INSERT INTO sales VALUES ");
        for i in 0..5000 {
            let id = chunk * 5000 + i + 1;
            sql.push_str(&format!(
                "({}, 'cust_{}', {:.2}, '{}')",
                id,
                id % 100,
                (id as f64) * 1.5,
                if id % 3 == 0 { "US" } else { "EU" }
            ));
            if i + 1 < 5000 {
                sql.push(',');
            }
        }
        db.execute(&sql).unwrap();
    }
    println!("setup: {} rows", n);

    // v0.14 shapes: BLOB scan, RRF fusion, traj_sim (episode DTW). A second
    // table carries the multimodal payloads; sizes kept modest so the gate
    // stays fast while still catching complexity-class regressions.
    db.execute("CREATE TABLE payloads (id INT PRIMARY KEY, body TEXT, data BLOB)")
        .unwrap();
    for i in 0..20_000i64 {
        db.execute(&format!(
            "INSERT INTO payloads VALUES ({}, 'doc {} about grasp {}', X'{}')",
            i,
            i,
            i % 50,
            "ab".repeat(16)
        ))
        .unwrap();
    }
    db.execute("CREATE TEXT INDEX payloads_body ON payloads(body)")
        .unwrap();
    db.execute(
        "CREATE EPISODE TABLE teleop (session TEXT, ts TIMESTAMP, joint VECTOR(2)) EPISODE BY (session, ts) GAP 1000000",
    )
    .unwrap();
    for s in 0..2i64 {
        for e in 0..200i64 {
            let base = 1_700_000_000_000_000i64 + s * 1_000_000_000_000 + e * 2_000_000_000;
            for k in 0..64i64 {
                let x = ((e + k) as f64) * 0.05;
                db.execute(&format!(
                    "INSERT INTO teleop VALUES ('s{}', {}, [{}, 0.5])",
                    s,
                    base + k * 100_000,
                    x
                ))
                .unwrap();
            }
        }
    }
    let demo: String = (0..64)
        .map(|i| format!("{}", i as f64 * 0.05))
        .collect::<Vec<_>>()
        .join(", ");
    let blob_hex = "ab".repeat(16);
    let blob_scan_sql = format!("SELECT COUNT(*) FROM payloads WHERE data = X'{blob_hex}'");
    let rrf_sql = "SELECT id FROM RRF((SELECT id FROM payloads WHERE MATCH(body, 'grasp') ORDER BY bm25(body) LIMIT 50), (SELECT id FROM payloads ORDER BY id LIMIT 50), k = 10)";
    let traj_sql = format!(
        "SELECT session, t0 FROM EPISODES(teleop) ORDER BY traj_sim(traj_joint, [{}]) LIMIT 10",
        demo
    );
    println!("setup: {} rows (+20k payloads, 400 episodes)", n);

    let queries: &[(&str, &str, f64)] = &[
        ("full_scan (baseline)", "SELECT * FROM sales LIMIT 200000", f64::INFINITY),
        ("text_eq_filter", "SELECT id FROM sales WHERE region = 'US' LIMIT 100000", 8.0),
        ("group_by_agg", "SELECT region, COUNT(*), AVG(amount) FROM sales GROUP BY region", 8.0),
        ("count_sum_filter", "SELECT COUNT(*), SUM(amount) FROM sales WHERE region = 'US'", 6.0),
        ("order_topk", "SELECT id, amount FROM sales WHERE region = 'US' ORDER BY amount DESC LIMIT 10", 8.0),
        ("in_subquery", "SELECT id FROM sales WHERE customer IN (SELECT customer FROM sales WHERE region = 'US') LIMIT 100000", 14.0),
        ("blob_scan", blob_scan_sql.as_str(), 10.0),
        ("rrf_fusion", rrf_sql, 20.0),
        ("traj_sim_topk", traj_sql.as_str(), 30.0),
    ];

    let mut baseline = Duration::ZERO;
    let mut failures = 0usize;
    for (idx, (name, sql, budget)) in queries.iter().enumerate() {
        let t = bench(&db, sql, 5);
        if idx == 0 {
            baseline = t;
            println!("{:24} {:>10?}  (baseline)", name, t);
            continue;
        }
        let ratio = t.as_secs_f64() / baseline.as_secs_f64();
        let ok = ratio < *budget && t < Duration::from_secs(5);
        if !ok {
            failures += 1;
        }
        println!(
            "{:24} {:>10?}  ratio {:>5.2}x budget {:.0}x  {}",
            name,
            t,
            ratio,
            budget,
            if ok { "OK" } else { "REGRESSION" }
        );
    }
    if failures > 0 {
        eprintln!(
            "\n{} query shape(s) exceeded their ratio/absolute budget",
            failures
        );
        std::process::exit(1);
    }
    println!("\nperf smoke: all budgets hold");
}
