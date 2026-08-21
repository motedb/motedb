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

fn median_of(v: &mut Vec<Duration>) -> Duration {
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
    db.execute(
        "CREATE TABLE sales (id INT PRIMARY KEY, customer TEXT, amount FLOAT, region TEXT)",
    )
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

    let queries: &[(&str, &str, f64)] = &[
        ("full_scan (baseline)", "SELECT * FROM sales LIMIT 200000", f64::INFINITY),
        ("text_eq_filter", "SELECT id FROM sales WHERE region = 'US' LIMIT 100000", 8.0),
        ("group_by_agg", "SELECT region, COUNT(*), AVG(amount) FROM sales GROUP BY region", 8.0),
        ("count_sum_filter", "SELECT COUNT(*), SUM(amount) FROM sales WHERE region = 'US'", 6.0),
        ("order_topk", "SELECT id, amount FROM sales WHERE region = 'US' ORDER BY amount DESC LIMIT 10", 8.0),
        ("in_subquery", "SELECT id FROM sales WHERE customer IN (SELECT customer FROM sales WHERE region = 'US') LIMIT 100000", 14.0),
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
        eprintln!("\n{} query shape(s) exceeded their ratio/absolute budget", failures);
        std::process::exit(1);
    }
    println!("\nperf smoke: all budgets hold");
}
