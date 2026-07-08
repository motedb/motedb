//! Scalability test: latency & memory vs data size

use motedb::{DBConfig, Database};
use std::time::Instant;
use tempfile::TempDir;

fn setup_db(n: usize) -> (Database, TempDir) {
    let dir = TempDir::new().expect("temp dir");
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None; // No limit for scalability testing
    let db = Database::create_with_config(dir.path(), config).expect("create db");
    db.execute("CREATE TABLE sales (id INT PRIMARY KEY AUTO_INCREMENT, customer TEXT, amount FLOAT, region TEXT)").unwrap();

    let mut batch = String::with_capacity(n * 60);
    for i in 0..n {
        let region = if i % 3 == 0 { "'US'" } else { "'EU'" };
        let customer = format!("'cust_{}'", i % (n / 10).max(10));
        let amount = (i as f64 * 1.1 + 50.0) % 1000.0;
        batch.push_str(&format!("({},{:.2},{}),", customer, amount, region));
        if batch.len() > 1_000_000 || i == n - 1 {
            batch.truncate(batch.len() - 1);
            db.execute(&format!(
                "INSERT INTO sales (customer, amount, region) VALUES {}",
                batch
            ))
            .unwrap();
            batch.clear();
        }
    }
    (db, dir)
}

fn row_count(db: &Database, sql: &str) -> usize {
    let r = db
        .execute(sql)
        .expect("execute")
        .materialize()
        .expect("materialize");
    r.select_rows()
        .map(|(_, rows)| rows.len())
        .unwrap_or(r.affected_rows())
}

fn bench<F: FnMut()>(label: &str, n: usize, mut f: F) -> u64 {
    f(); // warmup
    let iters = if n < 10_000 {
        200
    } else if n < 50_000 {
        100
    } else {
        30
    };
    let start = Instant::now();
    for _ in 0..iters {
        f();
    }
    let us = start.elapsed().as_micros() as u64 / iters;
    println!(
        "  {:45} | {:>6} rows | {:>8} us/op | {:>6} rows/ms",
        label,
        n,
        us,
        n as u64 * 1000 / us.max(1)
    );
    us
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

#[test]
#[ignore = "slow: runs 7 query types × 5 data sizes (up to 100K rows), ~2min due to LIKE/GROUP BY cost; run with --ignored"]
fn test_scalability_latency() {
    let sizes = [1_000, 5_000, 10_000, 30_000, 100_000];

    println!("\n  LATENCY vs DATA SIZE");
    println!("  {}", "=".repeat(80));
    println!(
        "  {:45} | {:>6}       | {:>8}     | {:>8}",
        "Operation", "Rows", "us/op", "rows/ms"
    );
    println!("  {}", "-".repeat(80));

    let mut prev: Option<(usize, Vec<u64>)> = None; // (prev_n, prev_times)

    for &n in &sizes {
        let (db, _dir) = setup_db(n);

        let mut times = Vec::new();

        times.push(bench("SELECT * (full scan)", n, || {
            row_count(&db, "SELECT * FROM sales");
        }));
        times.push(bench("WHERE region='US' (filter)", n, || {
            row_count(&db, "SELECT * FROM sales WHERE region = 'US'");
        }));
        times.push(bench("GROUP BY customer + COUNT(*)", n, || {
            row_count(
                &db,
                "SELECT customer, COUNT(*) FROM sales GROUP BY customer",
            );
        }));
        times.push(bench("ORDER BY amount DESC LIMIT 10", n, || {
            row_count(&db, "SELECT * FROM sales ORDER BY amount DESC LIMIT 10");
        }));
        times.push(bench("SELECT DISTINCT region", n, || {
            row_count(&db, "SELECT DISTINCT region FROM sales");
        }));
        times.push(bench("WHERE customer IN (SELECT ...)", n, || { row_count(&db, "SELECT id FROM sales WHERE customer IN (SELECT customer FROM sales WHERE region = 'US')"); }));
        times.push(bench("WHERE customer LIKE 'cust_1%'", n, || {
            row_count(&db, "SELECT * FROM sales WHERE customer LIKE 'cust_1%'");
        }));
        times.push(bench("PK SELECT by id", n, || {
            row_count(&db, &format!("SELECT * FROM sales WHERE id = {}", n / 2));
        }));

        // Linearity check
        if let Some((prev_n, prev_times)) = &prev {
            let data_ratio = n as f64 / *prev_n as f64;
            println!(
                "  >>> Scaling from {}K to {}K rows (data {:.1}x):",
                prev_n / 1000,
                n / 1000,
                data_ratio
            );
            let labels = [
                "scan",
                "WHERE",
                "GROUP BY",
                "ORDER BY",
                "DISTINCT",
                "IN subquery",
                "LIKE",
                "PK SELECT",
            ];
            for (i, label) in labels.iter().enumerate() {
                let time_ratio = times[i] as f64 / prev_times[i] as f64;
                let verdict = if time_ratio <= data_ratio * 1.3 {
                    "OK (linear)"
                } else if time_ratio <= data_ratio * 2.5 {
                    "WARN"
                } else {
                    "BAD (super-linear)"
                };
                println!(
                    "      {:20} time {:.1}x / data {:.1}x  =>  {}  ({} us -> {} us)",
                    label, time_ratio, data_ratio, verdict, prev_times[i], times[i]
                );
            }
        }

        prev = Some((n, times));
        println!();
        drop(db);
    }
}

#[test]
#[ignore = "hardware-threshold: passes in release, flaky in debug"]
fn test_scalability_memory() {
    let sizes = [1_000, 10_000, 30_000, 100_000];

    println!("\n  MEMORY vs DATA SIZE");
    println!("  {}", "=".repeat(60));
    println!(
        "  {:>8} | {:>10} | {:>10} | {:>10} | {:>10}",
        "Rows", "RSS KB", "B/row", "Disk KB", "B/row"
    );
    println!("  {}", "-".repeat(60));

    let mut prev: Option<(usize, u64, u64)> = None;

    for &n in &sizes {
        let (db, dir) = setup_db(n);
        db.flush().ok();

        // Read all rows to populate caches
        let _ = db.execute("SELECT * FROM sales");

        let rss_kb = get_rss_kb();
        let disk_kb = {
            let output = std::process::Command::new("du")
                .args(["-sk", &dir.path().to_string_lossy()])
                .output()
                .ok();
            output
                .and_then(|o| {
                    String::from_utf8_lossy(&o.stdout)
                        .split_whitespace()
                        .next()?
                        .parse::<u64>()
                        .ok()
                })
                .unwrap_or(0)
        };

        println!(
            "  {:>8} | {:>8} KB | {:>8} B | {:>8} KB | {:>8} B",
            n,
            rss_kb,
            rss_kb * 1024 / n as u64,
            disk_kb,
            disk_kb * 1024 / n as u64
        );

        if let Some((prev_n, prev_rss, prev_disk)) = &prev {
            let data_ratio = n as f64 / *prev_n as f64;
            let rss_ratio = (rss_kb - 5000).max(1) as f64 / (prev_rss - 5000).max(1) as f64; // subtract ~5MB base
            let disk_ratio = disk_kb as f64 / (*prev_disk).max(1) as f64;
            let verdict_rss = if rss_ratio <= data_ratio * 1.5 {
                "linear"
            } else {
                "super-linear"
            };
            let verdict_disk = if disk_ratio <= data_ratio * 1.5 {
                "linear"
            } else {
                "super-linear"
            };
            println!(
                "    >>> RSS {:.1}x, Disk {:.1}x vs {:.1}x data growth => RSS {}, Disk {}",
                rss_ratio, disk_ratio, data_ratio, verdict_rss, verdict_disk
            );
        }

        prev = Some((n, rss_kb, disk_kb));
        drop(db);
    }

    println!("\n  Note: RSS includes ~5-10 MB process overhead. B/row = total RSS / rows.");
}
