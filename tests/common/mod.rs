//! Shared test infrastructure for MoteDB test suites.
//!
//! Provides common helpers used across all test files:
//! - Database setup (edge config, with data pre-loaded)
//! - SQL execution helpers
//! - RSS memory measurement
//! - Crash-recovery verification
//! - Timing/latency measurement

use motedb::{Database, DBConfig, QueryResult};
use std::time::{Duration, Instant};
use tempfile::TempDir;

// ─── Database Setup ─────────────────────────────────────────────────────

/// Create a temporary database with edge config (jemalloc, no result limit).
/// Returns (TempDir, Database). The TempDir must be kept alive for the DB's lifetime.
pub fn setup_db() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    (dir, db)
}

/// Create a database at a specific path (for restart-recovery tests).
/// Caller manages the directory lifecycle.
pub fn create_db_at(path: &std::path::Path) -> Database {
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    Database::create_with_config(path, config).unwrap()
}

/// Open an existing database (for restart-recovery tests).
pub fn open_db_at(path: &std::path::Path) -> Database {
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    Database::open_with_config(path, config).unwrap()
}

/// Execute SQL, panicking on error. Convenience for test readability.
pub fn exec(db: &Database, sql: &str) {
    db.execute(sql).unwrap();
}

/// Execute SQL and return the number of result rows.
pub fn count_rows(db: &Database, sql: &str) -> usize {
    match db.execute(sql).unwrap().materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows.len(),
        _ => 0,
    }
}

/// Execute SQL and return row_count() without materialization (fast).
pub fn fast_count(db: &Database, sql: &str) -> usize {
    db.execute(sql).unwrap().row_count()
}

/// Insert N rows into a standard test table with batch INSERT.
/// Table: (id INT PRIMARY KEY, val FLOAT, tag TEXT)
/// Rows are inserted with IDs start_id..start_id+n (no overlap on repeated calls).
pub fn insert_test_rows(db: &Database, n: usize) {
    insert_test_rows_from(db, n, 0);
}

/// Insert N rows starting from start_id. Avoids duplicate PK on repeated calls.
pub fn insert_test_rows_from(db: &Database, n: usize, start_id: usize) {
    let batch_size = 5000;
    for start in (0..n).step_by(batch_size) {
        let end = (start + batch_size).min(n);
        let mut sql = String::with_capacity(batch_size * 50);
        for i in start..end {
            let id = start_id + i + 1;
            let tag = if id % 3 == 0 { "US" } else { "EU" };
            sql.push_str(&format!("({}, {:.1}, '{}'),", id, id as f64, tag));
        }
        sql.truncate(sql.len() - 1);
        exec(db, &format!("INSERT INTO bench (id, val, tag) VALUES {}", sql));
    }
}

// ─── Memory Measurement ─────────────────────────────────────────────────

/// Get current process RSS in MB.
pub fn get_rss_mb() -> f64 {
    get_rss_kb() as f64 / 1024.0
}

/// Get current process RSS in KB.
pub fn get_rss_kb() -> u64 {
    let pid = std::process::id();
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()
        .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse::<u64>().ok())
        .unwrap_or(0)
}

// ─── Timing ─────────────────────────────────────────────────────────────

/// Run a closure N times and return (p50, p99) in microseconds.
pub fn measure_p99_us<F: FnMut()>(mut f: F, iterations: usize) -> (u64, u64) {
    if iterations == 0 {
        return (0, 0);
    }
    let mut times = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let t = Instant::now();
        f();
        times.push(t.elapsed().as_micros() as u64);
    }
    times.sort();
    let p50 = times[times.len() / 2];
    let p99_idx = ((times.len() as f64 * 0.99) as usize).min(times.len() - 1);
    let p99 = times[p99_idx];
    (p50, p99)
}

/// Assert that a value is within a percentage of expected.
pub fn assert_within_pct(actual: f64, expected: f64, pct: f64, label: &str) {
    let tolerance = expected * pct / 100.0;
    assert!(
        (actual - expected).abs() <= tolerance.max(1.0),
        "{}: {:.1} not within {:.1}% of {:.1}",
        label, actual, pct, expected
    );
}
