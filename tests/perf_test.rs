//! Quick performance micro-benchmarks for MoteDB
//!
//! Tests: INSERT, SELECT by PK, SELECT * (full scan), UPDATE, DELETE, durability

use motedb::Database;
use tempfile::TempDir;
use std::time::Instant;

fn create_db() -> (Database, TempDir) {
    let dir = TempDir::new().expect("temp dir");
    let db = Database::create(dir.path()).expect("create db");
    (db, dir)
}

fn exec(db: &Database, sql: &str) -> motedb::sql::QueryResult {
    db.execute(sql).expect("execute SQL").materialize().expect("materialize")
}

fn count_rows(db: &Database, table: &str) -> i64 {
    let result = exec(db, &format!("SELECT COUNT(*) AS cnt FROM {}", table));
    match result {
        motedb::sql::QueryResult::Select { rows, .. } => {
            if let Some(row) = rows.first() {
                if let Some(motedb::types::Value::Integer(cnt)) = row.first() {
                    return *cnt;
                }
            }
            0
        }
        _ => 0,
    }
}

struct BenchResult {
    name: String,
    rows: usize,
    total_ms: u64,
    per_row_us: f64,
    throughput: f64,
}

impl std::fmt::Display for BenchResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{:<50} | {:>6} ops | {:>8.1} ms | {:>8.1} µs/op | {:>10.0} ops/s",
            self.name, self.rows, self.total_ms as f64, self.per_row_us, self.throughput
        )
    }
}

fn bench<F>(name: &str, ops: usize, f: F) -> BenchResult
where
    F: FnOnce() -> u64,
{
    let elapsed_ms = f();
    let per_row_us = if ops > 0 { (elapsed_ms as f64 * 1000.0) / ops as f64 } else { 0.0 };
    let throughput = if elapsed_ms > 0 { ops as f64 / (elapsed_ms as f64 / 1000.0) } else { f64::INFINITY };
    BenchResult {
        name: name.to_string(),
        rows: ops,
        total_ms: elapsed_ms,
        per_row_us,
        throughput,
    }
}

// ── Tests ──

#[test]
fn test_perf_insert_10k() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT)");

    const N: usize = 10_000;

    let r = bench("INSERT (10K rows, single-stmt)", N, || {
        let start = Instant::now();
        for i in 1..=N as i64 {
            exec(&db, &format!(
                "INSERT INTO t VALUES ({}, 'user_{}', {})",
                i, i, i as f64 * 1.5
            ));
        }
        start.elapsed().as_millis() as u64
    });
    println!("{}", r);

    // Flush and verify (retry to handle async SSTable registration)
    db.flush().expect("flush");
    let mut cnt = 0i64;
    for attempt in 0..20 {
        cnt = count_rows(&db, "t");
        if cnt == N as i64 {
            break;
        }
        if attempt >= 10 {
            eprintln!("Warning: row count after flush: {} (expected {}), attempt {}", cnt, N, attempt);
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    assert_eq!(cnt, N as i64, "Expected {} rows, got {}", N, cnt);
}

#[test]
fn test_perf_select_pk() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT, score FLOAT)");

    const N: usize = 5_000;

    // Seed
    for i in 1..=N as i64 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'v{}', {})", i, i, i as f64));
    }

    let r = bench(&format!("SELECT by PK ({} queries, MemTable)", N), N, || {
        let start = Instant::now();
        for i in 1..=N as i64 {
            exec(&db, &format!("SELECT * FROM t WHERE id = {}", i));
        }
        start.elapsed().as_millis() as u64
    });
    println!("{}", r);

    // After flush
    db.flush().expect("flush");

    let r2 = bench(&format!("SELECT by PK ({} queries, SSTable)", N), N, || {
        let start = Instant::now();
        for i in 1..=N as i64 {
            exec(&db, &format!("SELECT * FROM t WHERE id = {}", i));
        }
        start.elapsed().as_millis() as u64
    });
    println!("{}", r2);
}

#[test]
fn test_perf_full_scan() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)");

    const N: usize = 5_000;

    for i in 1..=N as i64 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'row_{}', {})", i, i, i * 10));
    }

    // Scan from MemTable
    let r = bench(&format!("SELECT * full scan ({} rows, MemTable)", N), N, || {
        let start = Instant::now();
        exec(&db, "SELECT * FROM t");
        start.elapsed().as_millis() as u64
    });
    println!("{}", r);

    // Scan from SSTable
    db.flush().expect("flush");

    let r2 = bench(&format!("SELECT * full scan ({} rows, SSTable)", N), N, || {
        let start = Instant::now();
        exec(&db, "SELECT * FROM t");
        start.elapsed().as_millis() as u64
    });
    println!("{}", r2);
}

#[test]
fn test_perf_update() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT, counter INTEGER)");

    const N: usize = 5_000;

    for i in 1..=N as i64 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'original', 0)", i));
    }

    let r = bench(&format!("UPDATE counter ({} rows)", N), N, || {
        let start = Instant::now();
        for i in 1..=N as i64 {
            exec(&db, &format!("UPDATE t SET counter = {} WHERE id = {}", i, i));
        }
        start.elapsed().as_millis() as u64
    });
    println!("{}", r);

    // Verify a value
    let result = exec(&db, "SELECT * FROM t WHERE id = 42");
    if let motedb::sql::QueryResult::Select { rows, .. } = result {
        assert!(!rows.is_empty(), "Row 42 should exist");
    }
}

#[test]
fn test_perf_delete() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");

    const N: usize = 5_000;

    for i in 1..=N as i64 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'row_{}')", i, i));
    }

    let delete_count = N / 2;
    let r = bench(&format!("DELETE ({} rows from {})", delete_count, N), delete_count, || {
        let start = Instant::now();
        for i in (1..=N as i64).step_by(2) {
            exec(&db, &format!("DELETE FROM t WHERE id = {}", i));
        }
        start.elapsed().as_millis() as u64
    });
    println!("{}", r);

    // Verify odd rows deleted
    let result = exec(&db, "SELECT * FROM t WHERE id = 1");
    if let motedb::sql::QueryResult::Select { rows, .. } = result {
        assert!(rows.is_empty(), "Deleted row 1 should not exist");
    }
    let result2 = exec(&db, "SELECT * FROM t WHERE id = 2");
    if let motedb::sql::QueryResult::Select { rows, .. } = result2 {
        assert!(!rows.is_empty(), "Row 2 should still exist");
    }
}

#[test]
fn test_perf_durability() {
    let dir = TempDir::new().expect("temp dir");
    let db_path = dir.path().to_path_buf();

    const N: usize = 5_000;

    // Phase 1: Write
    let write_ms = {
        let db = Database::create(&db_path).expect("create db");
        exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT, num INTEGER)");

        let start = Instant::now();
        for i in 1..=N as i64 {
            exec(&db, &format!("INSERT INTO t VALUES ({}, 'v{}', {})", i, i, i * 10));
        }
        let write_ms = start.elapsed().as_millis() as u64;

        let r = BenchResult {
            name: format!("INSERT {} rows (before restart)", N),
            rows: N,
            total_ms: write_ms,
            per_row_us: (write_ms as f64 * 1000.0) / N as f64,
            throughput: N as f64 / (write_ms as f64 / 1000.0),
        };
        println!("{}", r);

        db.checkpoint().expect("checkpoint");
        db.close().expect("close");
        write_ms
    };

    // Phase 2: Read after restart
    {
        let db = Database::open(&db_path).expect("open db");

        // Point query
        let start = Instant::now();
        let result = exec(&db, "SELECT * FROM t WHERE id = 42");
        let read_ms = start.elapsed().as_millis() as u64;
        if let motedb::sql::QueryResult::Select { rows, .. } = result {
            assert!(!rows.is_empty(), "Row should exist after restart");
        }

        let r = BenchResult {
            name: "SELECT by PK after restart".to_string(),
            rows: 1,
            total_ms: read_ms,
            per_row_us: read_ms as f64 * 1000.0,
            throughput: if read_ms > 0 { 1000.0 / read_ms as f64 } else { f64::INFINITY },
        };
        println!("{}", r);

        // Full scan
        let start = Instant::now();
        let result = exec(&db, "SELECT * FROM t");
        let scan_ms = start.elapsed().as_millis() as u64;
        if let motedb::sql::QueryResult::Select { rows, .. } = result {
            assert_eq!(rows.len(), N, "Expected {} rows after restart, got {}", N, rows.len());
        }

        let r = BenchResult {
            name: format!("SELECT * after restart ({} rows)", N),
            rows: N,
            total_ms: scan_ms,
            per_row_us: (scan_ms as f64 * 1000.0) / N as f64,
            throughput: N as f64 / (scan_ms as f64 / 1000.0),
        };
        println!("{}", r);
    }
}

#[test]
fn test_perf_mixed_crud() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT, score INTEGER)");

    const N: usize = 5_000;

    let total_ms = {
        let start = Instant::now();

        // Insert
        for i in 1..=N as i64 {
            exec(&db, &format!("INSERT INTO t VALUES ({}, 'v{}', {})", i, i, i));
        }

        // Update every 3rd
        let updates = N / 3;
        for i in (1..=N as i64).step_by(3) {
            exec(&db, &format!("UPDATE t SET score = -1 WHERE id = {}", i));
        }

        // Delete every 5th
        let deletes = N / 5;
        for i in (1..=N as i64).step_by(5) {
            exec(&db, &format!("DELETE FROM t WHERE id = {}", i));
        }

        // Point selects
        let selects = 100;
        for i in 1..=selects as i64 {
            exec(&db, &format!("SELECT * FROM t WHERE id = {}", i));
        }

        start.elapsed().as_millis() as u64
    };

    let total_ops = N + N / 3 + N / 5 + 100;
    let r = BenchResult {
        name: format!("Mixed CRUD ({:.0} ops total, {} rows)", total_ops, N),
        rows: total_ops,
        total_ms,
        per_row_us: (total_ms as f64 * 1000.0) / total_ops as f64,
        throughput: total_ops as f64 / (total_ms as f64 / 1000.0),
    };
    println!("{}", r);
}
