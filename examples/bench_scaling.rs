//! Query scaling benchmark: grow one table 100K → 1.6M rows and measure
//! per-stage latency of point lookup / indexed equality / full scan /
//! filter / GROUP BY, plus RSS memory.
//!
//! Answers: (1) does memory have a ceiling? (2) which queries scale
//! linearly with data size and which stay flat?
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

/// Tiny LCG so we don't need the rand crate.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 16
    }
}

fn main() {
    println!("\n  MoteDB Query Scaling Benchmark (100K -> 1.6M rows)");
    println!("  config: for_general (desktop defaults)\n");

    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_general();
    config.max_result_rows = None; // allow full materialization measurements
                                   // Measure query paths cleanly: the background auto-checkpoint thread
                                   // occasionally fires mid-query and its lock hold time shows up as huge
                                   // single-shot outliers (COUNT 910ms vs 27µs). Production still benefits
                                   // from it; benchmarks of query scaling should not sample that contention.
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

    let milestones = [100_000usize, 200_000, 400_000, 800_000, 1_600_000];
    let mut total = 0usize;
    let batch_size = 5000;

    println!(
        "  {:>9} | {:>8} | {:>7} | {:>9} | {:>10} | {:>11} | {:>10} | {:>10} | {:>10}",
        "rows",
        "RSS_MB",
        "B/row",
        "point_us",
        "idx_eq_ms",
        "idx_lim_us",
        "count_ms",
        "filter_ms",
        "group_ms"
    );
    println!("  {}", "-".repeat(115));

    for &milestone in &milestones {
        let to_insert = milestone - total;
        if to_insert > 0 {
            let t0 = Instant::now();
            for batch_start in (0..to_insert).step_by(batch_size) {
                let end = (batch_start + batch_size).min(to_insert);
                let mut batch = String::with_capacity(batch_size * 60);
                for i in batch_start..end {
                    let idx = total + i;
                    let name = format!("'user_{}'", idx % 5000);
                    let val = (idx as f64 * 1.7 + 42.0) % 1000.0;
                    let code = format!("'CD_{}'", idx % 1000);
                    let ts = 1700000000000000u64 + idx as u64 * 1000;
                    batch.push_str(&format!("({},{:.2},{},{}),", name, val, code, ts));
                }
                batch.truncate(batch.len() - 1);
                db.execute(&format!(
                    "INSERT INTO t (name, val, code, ts) VALUES {}",
                    batch
                ))
                .unwrap();
            }
            let ins_ms = t0.elapsed().as_millis();
            total = milestone;
            println!(
                "  [inserted to {} in {}ms — {} rows/s]",
                total,
                ins_ms,
                (to_insert as u64 * 1000) / ins_ms.max(1) as u64
            );
        }

        // --- Query suite ---------------------------------------------
        // Scan-class queries: min of 3 runs (steady state — the FIRST
        // query after bulk insert may trigger query-time segment compaction,
        // an amortized one-time maintenance cost, not query-path latency).
        let best_of_3 = |sql: &str| -> f64 {
            (0..3)
                .map(|_| {
                    let t0 = Instant::now();
                    db.execute(sql).unwrap().materialize().unwrap();
                    t0.elapsed().as_secs_f64() * 1000.0
                })
                .fold(f64::INFINITY, f64::min)
        };

        // 1. PK point lookup: 3000 random ids, avg per-op latency.
        let mut lcg = Lcg(0xdeadbeef);
        let t0 = Instant::now();
        let iters = 3000u64;
        for _ in 0..iters {
            let id = (lcg.next() % total as u64) + 1;
            let sql = format!("SELECT id, name, val, code, ts FROM t WHERE id = {}", id);
            let n = db.execute(&sql).unwrap().materialize().unwrap().row_count();
            assert_eq!(n, 1, "point query must return exactly 1 row");
        }
        let point_us = t0.elapsed().as_micros() as f64 / iters as f64;

        // 2. Indexed equality (1000 distinct codes -> matches grow with N).
        // min of 3: first call after insert pays one-time page-in/compaction.
        let idx_eq_ms = best_of_3("SELECT id, name, val FROM t WHERE code = 'CD_777'");

        // 3. Indexed equality + LIMIT (seek cost only, output bounded).
        let t0 = Instant::now();
        for _ in 0..200 {
            let n = db
                .execute("SELECT id FROM t WHERE code = 'CD_777' LIMIT 100")
                .unwrap()
                .materialize()
                .unwrap()
                .row_count();
            assert_eq!(n, 100);
        }
        let idx_lim_us = t0.elapsed().as_micros() as f64 / 200.0;

        let count_ms = best_of_3("SELECT COUNT(*) FROM t");
        let filter_ms = best_of_3("SELECT COUNT(*) FROM t WHERE val > 500");
        let group_ms = best_of_3("SELECT code, COUNT(*), AVG(val) FROM t GROUP BY code");

        let rss = get_rss_kb();
        let b_per_row = rss * 1024 / total.max(1) as u64;
        println!(
            "  {:>9} | {:>8.1} | {:>7} | {:>9.1} | {:>10.2} | {:>11.1} | {:>10.1} | {:>10.1} | {:>10.1}",
            total,
            rss as f64 / 1024.0,
            b_per_row,
            point_us,
            idx_eq_ms,
            idx_lim_us,
            count_ms,
            filter_ms,
            group_ms
        );
    }

    println!("\n  Legend:");
    println!("    point_us   = PK point query avg (SELECT ... WHERE id=?, 3000 iters)");
    println!("    idx_eq_ms  = indexed equality WHERE code='CD_777' (matches = N/1000)");
    println!("    idx_lim_us = indexed equality + LIMIT 100 (seek-only, 200 iters)");
    println!("    count_ms   = SELECT COUNT(*) full scan");
    println!("    filter_ms  = COUNT(*) WHERE val>500 (non-indexed, ~50% match)");
    println!("    group_ms   = GROUP BY code (1000 groups)");
    println!();
}
