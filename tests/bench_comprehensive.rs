//! Comprehensive Benchmark — P0/P1/P2 Optimization Verification
//!
//! Tests all critical paths with detailed latency/throughput reporting:
//!
//! 1. INSERT throughput (single + batch)
//! 2. Point query (PK lookup) — MemTable + SSTable + Cache
//! 3. UPDATE / DELETE latency
//! 4. Column index range scan
//! 5. Full table scan
//! 6. Checkpoint performance (fast vs full)
//! 7. AUTO_INCREMENT recovery (O(1) vs scan)
//! 8. Concurrent mixed workload
//! 9. WAL recovery after crash
//! 10. PreparedStatement cache hit rate
//!
//! Run: cargo test --test bench_comprehensive --release -- --nocapture --test-threads=1

use motedb::{DBConfig, Database};
use std::time::Instant;
use tempfile::TempDir;

/// CI mode: smaller data sizes for reliable parallel execution
fn is_ci() -> bool {
    std::env::var("CI").is_ok()
}

fn edge_config() -> DBConfig {
    DBConfig::for_edge()
}

fn create_db() -> (Database, TempDir) {
    let dir = TempDir::new().expect("temp dir");
    let db = Database::create_with_config(dir.path(), edge_config()).expect("create db");
    (db, dir)
}

fn exec(db: &Database, sql: &str) -> motedb::sql::QueryResult {
    db.execute(sql)
        .expect("execute SQL")
        .materialize()
        .expect("materialize")
}

#[allow(dead_code)]
fn print_header(title: &str) {
    println!("\n{}", "=".repeat(90));
    println!("  {}", title);
    println!("{}", "=".repeat(90));
}

fn print_result(name: &str, ops: usize, elapsed_ms: u64) {
    let per_op_us = if ops > 0 {
        (elapsed_ms as f64 * 1000.0) / ops as f64
    } else {
        0.0
    };
    let throughput = if elapsed_ms > 0 {
        ops as f64 / (elapsed_ms as f64 / 1000.0)
    } else {
        f64::INFINITY
    };
    println!(
        "  {:<60} | {:>7} ops | {:>8.1} ms | {:>8.1} µs/op | {:>10.0} ops/s",
        name, ops, elapsed_ms as f64, per_op_us, throughput
    );
}

fn print_separator() {
    println!("  {}", "-".repeat(100));
}

// ═══════════════════════════════════════════════════════════════
// Test 1: INSERT Throughput
// ═══════════════════════════════════════════════════════════════

#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn bench_insert_throughput() {
    let (db, _dir) = create_db();
    exec(
        &db,
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT, email TEXT, score FLOAT, age INTEGER)",
    );

    let n: usize = if is_ci() { 5_000 } else { 50_000 };
    let ms = {
        let start = Instant::now();
        for i in 1..=n as i64 {
            exec(
                &db,
                &format!(
                    "INSERT INTO t1 VALUES ({}, 'user_{}', 'user_{}@test.com', {}, {})",
                    i,
                    i,
                    i,
                    i as f64 * 1.5,
                    20 + (i % 50)
                ),
            );
        }
        start.elapsed().as_millis() as u64
    };
    print_result(
        &format!("INSERT {} rows (5 cols, PK auto-increment)", n),
        n,
        ms,
    );
    let insert_ops_per_s = if ms > 0 {
        n as f64 / (ms as f64 / 1000.0)
    } else {
        0.0
    };
    println!("  -> Throughput: {:.0} inserts/s", insert_ops_per_s);
    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 2: Point Query (PK Lookup) — 3 phases
// ═══════════════════════════════════════════════════════════════

#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn bench_point_query() {
    let (db, _dir) = create_db();
    exec(
        &db,
        "CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT, score FLOAT, tag TEXT)",
    );

    let n: usize = if is_ci() { 5_000 } else { 30_000 };
    let q: usize = if is_ci() { 2_000 } else { 10_000 };

    // Seed
    let seed_start = Instant::now();
    for i in 1..=n as i64 {
        exec(
            &db,
            &format!(
                "INSERT INTO t2 VALUES ({}, 'val_{}', {}, 'tag_{}')",
                i,
                i,
                i as f64,
                i % 10
            ),
        );
    }
    let seed_ms = seed_start.elapsed().as_millis() as u64;
    print_result(&format!("Seed: INSERT {} rows", n), n, seed_ms);

    print_separator();

    // Phase 1: PK lookup — MemTable (all in memory)
    let mem_ms = {
        let start = Instant::now();
        for i in 1..=q as i64 {
            exec(&db, &format!("SELECT * FROM t2 WHERE id = {}", i));
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("PK SELECT {} (MemTable)", q), q, mem_ms);

    // Phase 2: Flush to SSTable
    db.flush().expect("flush");
    db.wait_for_indexes_ready();

    let sst_ms = {
        let start = Instant::now();
        for i in 1..=q as i64 {
            exec(&db, &format!("SELECT * FROM t2 WHERE id = {}", i));
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("PK SELECT {} (SSTable + RowCache)", q), q, sst_ms);

    // Phase 3: Cache-warm repeated queries
    let repeats = if is_ci() { 20 } else { 100 };
    let warm_ms = {
        let start = Instant::now();
        for _ in 0..repeats {
            for i in 1..=100i64 {
                exec(&db, &format!("SELECT * FROM t2 WHERE id = {}", i));
            }
        }
        start.elapsed().as_millis() as u64
    };
    let warm_total = repeats * 100;
    print_result(
        &format!(
            "PK SELECT {} (100 unique × {}, fully cached)",
            warm_total, repeats
        ),
        warm_total,
        warm_ms,
    );

    let mem_per_op = mem_ms as f64 * 1000.0 / q as f64;
    let sst_per_op = sst_ms as f64 * 1000.0 / q as f64;
    let warm_per_op = warm_ms as f64 * 1000.0 / warm_total as f64;
    println!(
        "  -> MemTable: {:.1}µs, SSTable: {:.1}µs, Cached: {:.1}µs",
        mem_per_op, sst_per_op, warm_per_op
    );
    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 3: UPDATE / DELETE Latency (P0: AtomicU64 + batch WAL)
// ═══════════════════════════════════════════════════════════════

#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn bench_update_delete() {
    let (db, _dir) = create_db();
    exec(
        &db,
        "CREATE TABLE t3 (id INTEGER PRIMARY KEY, name TEXT, score FLOAT, status TEXT)",
    );

    let n: usize = if is_ci() { 5_000 } else { 30_000 };

    // Seed
    for i in 1..=n as i64 {
        exec(
            &db,
            &format!(
                "INSERT INTO t3 VALUES ({}, 'name_{}', {}, 'active')",
                i, i, i as f64
            ),
        );
    }

    print_separator();

    // UPDATE latency (sequential)
    let upd_count = n / 3;
    let upd_ms = {
        let start = Instant::now();
        for i in (1..=n as i64).step_by(3) {
            exec(
                &db,
                &format!(
                    "UPDATE t3 SET score = score + 10, status = 'updated' WHERE id = {}",
                    i
                ),
            );
        }
        start.elapsed().as_millis() as u64
    };
    print_result(
        &format!("UPDATE {} rows (1/3 of table)", upd_count),
        upd_count,
        upd_ms,
    );

    // DELETE latency (sequential)
    let del_count = n / 5;
    let del_ms = {
        let start = Instant::now();
        for i in (1..=n as i64).step_by(5) {
            exec(&db, &format!("DELETE FROM t3 WHERE id = {}", i));
        }
        start.elapsed().as_millis() as u64
    };
    print_result(
        &format!("DELETE {} rows (1/5 of table)", del_count),
        del_count,
        del_ms,
    );

    // Post-delete SELECT
    let sel_count = if is_ci() { 1_000 } else { 5_000 };
    let sel_ms = {
        let start = Instant::now();
        for i in 1..=sel_count as i64 {
            exec(&db, &format!("SELECT * FROM t3 WHERE id = {}", i));
        }
        start.elapsed().as_millis() as u64
    };
    print_result(
        &format!("SELECT {} after UPDATE+DELETE", sel_count),
        sel_count,
        sel_ms,
    );

    let upd_per_op = upd_ms as f64 * 1000.0 / upd_count as f64;
    let del_per_op = del_ms as f64 * 1000.0 / del_count as f64;
    println!(
        "  -> UPDATE: {:.1}µs/op, DELETE: {:.1}µs/op",
        upd_per_op, del_per_op
    );
    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 4: Checkpoint Performance (P1: fast vs full)
// ═══════════════════════════════════════════════════════════════

#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn bench_checkpoint() {
    let (db, _dir) = create_db();
    exec(
        &db,
        "CREATE TABLE t4 (id INTEGER PRIMARY KEY, data TEXT, value FLOAT)",
    );

    let n: usize = if is_ci() { 5_000 } else { 30_000 };

    // Seed data
    for i in 1..=n as i64 {
        exec(
            &db,
            &format!(
                "INSERT INTO t4 VALUES ({}, 'data_{}', {})",
                i,
                i,
                i as f64 * 2.0
            ),
        );
    }

    print_separator();

    // Fast checkpoint (no index rebuild)
    let fast_ms = {
        let start = Instant::now();
        db.checkpoint().expect("fast checkpoint");
        start.elapsed().as_millis() as u64
    };
    print_result(
        &format!("Fast checkpoint ({} rows, skip rebuild)", n),
        1,
        fast_ms,
    );
    println!("  -> Fast checkpoint: {}ms", fast_ms);

    // Insert more data
    for i in (n + 1) as i64..=(n * 2) as i64 {
        exec(
            &db,
            &format!(
                "INSERT INTO t4 VALUES ({}, 'data_{}', {})",
                i,
                i,
                i as f64 * 2.0
            ),
        );
    }

    // Full checkpoint (with index rebuild)
    let full_ms = {
        let start = Instant::now();
        db.checkpoint_full().expect("full checkpoint");
        start.elapsed().as_millis() as u64
    };
    print_result(
        &format!("Full checkpoint ({} rows, with rebuild)", n * 2),
        1,
        full_ms,
    );
    println!("  -> Full checkpoint: {}ms", full_ms);

    // Second fast checkpoint (should be near-instant, nothing pending)
    let second_fast_ms = {
        let start = Instant::now();
        db.checkpoint().expect("second fast checkpoint");
        start.elapsed().as_millis() as u64
    };
    print_result("Fast checkpoint (no pending updates)", 1, second_fast_ms);
    println!("  -> Second fast: {}ms (should be <1ms)", second_fast_ms);

    println!(
        "  -> Fast/Full speedup: {:.1}x",
        if full_ms > 0 {
            full_ms as f64 / fast_ms.max(1) as f64
        } else {
            0.0
        }
    );
    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 5: AUTO_INCREMENT Recovery (P2: O(1) vs scan)
// ═══════════════════════════════════════════════════════════════

#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn bench_auto_increment_recovery() {
    let dir = TempDir::new().expect("temp dir");
    let db_path = dir.path().to_path_buf();

    let n: usize = if is_ci() { 5_000 } else { 50_000 };

    // Phase 1: Create, insert, checkpoint (persist counter)
    {
        let db = Database::create_with_config(&db_path, edge_config()).expect("create db");
        exec(&db, "CREATE TABLE t5 (id INTEGER PRIMARY KEY, data TEXT)");

        for i in 1..=n as i64 {
            exec(&db, &format!("INSERT INTO t5 VALUES ({}, 'data_{}')", i, i));
        }
        db.checkpoint().expect("checkpoint");
        db.close().expect("close");
    }

    print_separator();

    // Phase 2: Reopen — should use O(1) catalog recovery
    let reopen_ms = {
        let start = Instant::now();
        let db = Database::open_with_config(&db_path, edge_config()).expect("open db");
        let elapsed = start.elapsed().as_millis() as u64;

        exec(
            &db,
            &format!("INSERT INTO t5 VALUES ({}, 'after_recovery')", n as i64 + 1),
        );
        db.close().expect("close");
        elapsed
    };
    print_result(
        &format!("Reopen DB ({} rows, O(1) counter recovery)", n),
        1,
        reopen_ms,
    );
    println!("  -> Recovery: {}ms", reopen_ms);
}

// ═══════════════════════════════════════════════════════════════
// Test 6: Column Index Scan
// ═══════════════════════════════════════════════════════════════

#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn bench_column_index() {
    let (db, _dir) = create_db();
    exec(
        &db,
        "CREATE TABLE t6 (id INTEGER PRIMARY KEY, category TEXT, price FLOAT, stock INTEGER)",
    );

    let n: usize = if is_ci() { 5_000 } else { 30_000 };

    // Seed
    for i in 1..=n as i64 {
        let cat = match i % 5 {
            0 => "electronics",
            1 => "books",
            2 => "clothing",
            3 => "food",
            _ => "toys",
        };
        exec(
            &db,
            &format!(
                "INSERT INTO t6 VALUES ({}, '{}', {:.1}, {})",
                i,
                cat,
                10.0 + (i as f64 % 990.0),
                i % 100
            ),
        );
    }

    // Create indexes
    exec(&db, "CREATE INDEX idx_cat ON t6 (category)");
    exec(&db, "CREATE INDEX idx_price ON t6 (price)");

    db.flush().expect("flush");
    db.wait_for_indexes_ready();

    print_separator();

    let q = if is_ci() { 50 } else { 200 };

    // Exact match (category = X)
    let eq_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT * FROM t6 WHERE category = 'electronics'");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(
        &format!("Column eq scan × {} (category='electronics')", q),
        q,
        eq_ms,
    );

    // Range scan (price between)
    let range_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(
                &db,
                "SELECT * FROM t6 WHERE price > 500.0 AND price < 600.0",
            );
        }
        start.elapsed().as_millis() as u64
    };
    print_result(
        &format!("Column range scan × {} (500 < price < 600)", q),
        q,
        range_ms,
    );

    // No-index scan (full filter)
    let no_idx_ms = {
        let start = Instant::now();
        for _ in 0..q / 4 {
            exec(&db, "SELECT * FROM t6 WHERE stock > 80");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(
        &format!("Full scan + filter × {} (stock > 80, no index)", q / 4),
        q / 4,
        no_idx_ms,
    );
    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 7: Full Table Scan
// ═══════════════════════════════════════════════════════════════

#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn bench_full_scan() {
    let (db, _dir) = create_db();
    exec(
        &db,
        "CREATE TABLE t7 (id INTEGER PRIMARY KEY, event_type TEXT, payload TEXT, ts INTEGER)",
    );

    let n: usize = if is_ci() { 5_000 } else { 50_000 };

    for i in 1..=n as i64 {
        exec(
            &db,
            &format!(
                "INSERT INTO t7 VALUES ({}, 'type_{}', 'payload_{}', {})",
                i,
                i % 20,
                i,
                1700000000 + i
            ),
        );
    }

    print_separator();

    // MemTable scan
    let mem_scan_ms = {
        let start = Instant::now();
        exec(&db, "SELECT * FROM t7");
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("SELECT * {} rows (MemTable)", n), n, mem_scan_ms);

    db.flush().expect("flush");
    db.wait_for_indexes_ready();

    // SSTable scan
    let sst_scan_ms = {
        let start = Instant::now();
        exec(&db, "SELECT * FROM t7");
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("SELECT * {} rows (SSTable)", n), n, sst_scan_ms);

    // COUNT(*) fast path
    let count_ms = {
        let start = Instant::now();
        for _ in 0..50 {
            exec(&db, "SELECT COUNT(*) AS cnt FROM t7");
        }
        start.elapsed().as_millis() as u64
    };
    print_result("COUNT(*) × 50", 50, count_ms);
    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 8: Mixed CRUD (total wall time)
// ═══════════════════════════════════════════════════════════════

#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn bench_mixed_crud() {
    let (db, _dir) = create_db();
    exec(
        &db,
        "CREATE TABLE t8 (id INTEGER PRIMARY KEY, customer TEXT, amount FLOAT, status TEXT)",
    );

    let n: usize = if is_ci() { 5_000 } else { 30_000 };
    let n_updates = n / 3;
    let n_deletes = n / 5;
    let n_selects = if is_ci() { 1_000 } else { 5_000 };
    let total_ops = n + n_updates + n_deletes + n_selects;

    let total_ms = {
        let start = Instant::now();

        // INSERT
        for i in 1..=n as i64 {
            exec(
                &db,
                &format!(
                    "INSERT INTO t8 VALUES ({}, 'customer_{}', {:.1}, 'pending')",
                    i,
                    i % 1000,
                    10.0 + (i as f64 % 990.0)
                ),
            );
        }
        let insert_elapsed = start.elapsed().as_millis() as u64;

        // UPDATE
        let upd_start = Instant::now();
        for i in (1..=n as i64).step_by(3) {
            exec(
                &db,
                &format!(
                    "UPDATE t8 SET status = 'shipped', amount = amount + 10 WHERE id = {}",
                    i
                ),
            );
        }
        let upd_elapsed = upd_start.elapsed().as_millis() as u64;

        // DELETE
        let del_start = Instant::now();
        for i in (1..=n as i64).step_by(5) {
            exec(&db, &format!("DELETE FROM t8 WHERE id = {}", i));
        }
        let del_elapsed = del_start.elapsed().as_millis() as u64;

        // SELECT
        let sel_start = Instant::now();
        for i in (1..=n_selects as i64).rev() {
            exec(&db, &format!("SELECT * FROM t8 WHERE id = {}", i));
        }
        let sel_elapsed = sel_start.elapsed().as_millis() as u64;

        println!(
            "  -> INSERT: {}ms, UPDATE: {}ms, DELETE: {}ms, SELECT: {}ms",
            insert_elapsed, upd_elapsed, del_elapsed, sel_elapsed
        );

        start.elapsed().as_millis() as u64
    };

    print_result(
        &format!("Mixed CRUD total ({} ops)", total_ops),
        total_ops,
        total_ms,
    );
    let per_op = total_ms as f64 * 1000.0 / total_ops as f64;
    println!("  -> Average: {:.1}µs/op overall", per_op);
    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 9: WAL Recovery (Crash Simulation)
// ═══════════════════════════════════════════════════════════════

#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn bench_wal_recovery() {
    let dir = TempDir::new().expect("temp dir");
    let db_path = dir.path().to_path_buf();

    let n: usize = if is_ci() { 5_000 } else { 30_000 };

    // Phase 1: Create, insert, flush (but don't checkpoint WAL)
    {
        let db = Database::create_with_config(&db_path, edge_config()).expect("create db");
        exec(
            &db,
            "CREATE TABLE t9 (id INTEGER PRIMARY KEY, data TEXT, value INTEGER)",
        );
        for i in 1..=n as i64 {
            exec(
                &db,
                &format!("INSERT INTO t9 VALUES ({}, 'data_{}', {})", i, i, i * 10),
            );
        }
        db.flush().expect("flush");
        drop(db);
    }

    print_separator();

    // Phase 2: Reopen — WAL recovery
    let reopen_ms = {
        let start = Instant::now();
        let db = Database::open_with_config(&db_path, edge_config()).expect("open db");

        let result = exec(&db, "SELECT COUNT(*) AS cnt FROM t9");
        let count = match result {
            motedb::sql::QueryResult::Select { rows, .. } => rows
                .first()
                .and_then(|r| r.first())
                .map(|v| {
                    if let motedb::types::Value::Integer(c) = v {
                        *c
                    } else {
                        0
                    }
                })
                .unwrap_or(0),
            _ => 0,
        };
        let elapsed = start.elapsed().as_millis() as u64;
        db.close().expect("close");
        println!("  -> Recovered {} rows", count);
        assert!(count > 0, "Should recover rows from WAL");
        elapsed
    };
    print_result(&format!("WAL recovery + open ({} rows)", n), 1, reopen_ms);
    println!("  -> Recovery time: {}ms", reopen_ms);
}

// ═══════════════════════════════════════════════════════════════
// Test 10: PreparedStatement Cache + Concurrent-like pattern
// ═══════════════════════════════════════════════════════════════

#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn bench_prepared_statement_cache() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t10 (id INTEGER PRIMARY KEY, data TEXT)");

    let n: usize = if is_ci() { 2_000 } else { 5_000 };
    for i in 1..=n as i64 {
        exec(
            &db,
            &format!("INSERT INTO t10 VALUES ({}, 'data_{}')", i, i),
        );
    }

    print_separator();

    // Cold cache
    let cold_count = if is_ci() { 500 } else { 1000 };
    let cold_ms = {
        let start = Instant::now();
        for i in 1..=cold_count as i64 {
            exec(&db, &format!("SELECT * FROM t10 WHERE id = {}", i));
        }
        start.elapsed().as_millis() as u64
    };
    print_result(
        &format!("PK SELECT {} (cold stmt cache)", cold_count),
        cold_count,
        cold_ms,
    );

    // Hot cache
    let repeats = if is_ci() { 20 } else { 100 };
    let hot_ms = {
        let start = Instant::now();
        for _ in 0..repeats {
            for i in 1..=100i64 {
                exec(&db, &format!("SELECT * FROM t10 WHERE id = {}", i));
            }
        }
        start.elapsed().as_millis() as u64
    };
    let hot_total = repeats * 100;
    print_result(
        &format!(
            "PK SELECT {} (100 unique × {}, stmt cache hit)",
            hot_total, repeats
        ),
        hot_total,
        hot_ms,
    );

    let cold_per_op = cold_ms as f64 * 1000.0 / cold_count as f64;
    let hot_per_op = hot_ms as f64 * 1000.0 / hot_total as f64;
    let speedup = if hot_per_op > 0.0 {
        cold_per_op / hot_per_op
    } else {
        0.0
    };
    println!(
        "  -> Cold: {:.1}µs/op, Hot: {:.1}µs/op, Speedup: {:.1}x",
        cold_per_op, hot_per_op, speedup
    );
    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 11: End-to-End Throughput (INSERT + checkpoint + reopen)
// ═══════════════════════════════════════════════════════════════

#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn bench_e2e_lifecycle() {
    let dir = TempDir::new().expect("temp dir");
    let db_path = dir.path().to_path_buf();

    let n: usize = if is_ci() { 5_000 } else { 50_000 };

    // Phase 1: Insert
    {
        let db = Database::create_with_config(&db_path, edge_config()).expect("create db");
        exec(&db, "CREATE TABLE lifecycle (id INTEGER PRIMARY KEY, name TEXT, score FLOAT, tag TEXT, ts INTEGER)");

        let start = Instant::now();
        for i in 1..=n as i64 {
            exec(
                &db,
                &format!(
                    "INSERT INTO lifecycle VALUES ({}, 'name_{}', {:.1}, 'tag_{}', {})",
                    i,
                    i,
                    i as f64 * 1.5,
                    i % 10,
                    1700000000 + i
                ),
            );
        }
        let elapsed = start.elapsed().as_millis() as u64;
        print_result(&format!("Phase 1: INSERT {}", n), n, elapsed);

        // Phase 2: Flush
        let flush_start = Instant::now();
        db.flush().expect("flush");
        db.wait_for_indexes_ready();
        let flush_elapsed = flush_start.elapsed().as_millis() as u64;
        print_result("Phase 2: Flush", 1, flush_elapsed);

        // Phase 3: Checkpoint
        let cp_start = Instant::now();
        db.checkpoint().expect("checkpoint");
        let cp_elapsed = cp_start.elapsed().as_millis() as u64;
        print_result("Phase 3: Checkpoint (fast)", 1, cp_elapsed);

        db.close().expect("close");
    };

    // Phase 4: Reopen + query
    let reopen_start = Instant::now();
    let db = Database::open_with_config(&db_path, edge_config()).expect("open db");
    let reopen_ms = reopen_start.elapsed().as_millis() as u64;
    print_result("Phase 4: Reopen", 1, reopen_ms);

    // Phase 5: Post-reopen queries
    let q = if is_ci() { 1_000 } else { 5_000 };
    let query_start = Instant::now();
    for i in 1..=q as i64 {
        exec(&db, &format!("SELECT * FROM lifecycle WHERE id = {}", i));
    }
    let query_ms = query_start.elapsed().as_millis() as u64;
    print_result(
        &format!("Phase 5: PK SELECT {} after reopen", q),
        q,
        query_ms,
    );

    // Full scan
    let scan_start = Instant::now();
    exec(&db, "SELECT * FROM lifecycle");
    let scan_ms = scan_start.elapsed().as_millis() as u64;
    print_result(
        &format!("Phase 6: SELECT * {} rows after reopen", n),
        n,
        scan_ms,
    );

    let query_per_op = query_ms as f64 * 1000.0 / q as f64;
    let scan_per_row = scan_ms as f64 * 1000.0 / n as f64;
    println!(
        "  -> PK query: {:.1}µs/op, Full scan: {:.2}µs/row",
        query_per_op, scan_per_row
    );

    db.close().expect("close");
}

// ═══════════════════════════════════════════════════════════════
// Test 12: Concurrent Mixed Workload (multi-thread stress)
// ═══════════════════════════════════════════════════════════════

#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn bench_concurrent_mixed() {
    use std::sync::Arc;
    use std::thread;

    let (db, _dir) = create_db();
    exec(
        &db,
        "CREATE TABLE t12 (id INTEGER PRIMARY KEY, data TEXT, value INTEGER)",
    );
    let db = Arc::new(db);

    // Seed
    let seed: usize = if is_ci() { 2_000 } else { 10_000 };
    for i in 1..=seed as i64 {
        exec(
            &db,
            &format!("INSERT INTO t12 VALUES ({}, 'seed_{}', {})", i, i, i * 10),
        );
    }

    print_separator();
    let (n_threads, ops_per_thread) = if is_ci() { (2, 500) } else { (4, 2500) };
    let total_concurrent = n_threads * ops_per_thread;
    println!(
        "  Starting {} threads × {} ops each ({} total)",
        n_threads, ops_per_thread, total_concurrent
    );

    let total_ms = {
        let start = Instant::now();
        let mut handles = vec![];

        for t in 0..n_threads {
            let db_clone = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                let base = t * ops_per_thread;
                let mut ops = 0;
                for i in 0..ops_per_thread {
                    let id = (base + i + 1) as i64 + seed as i64;
                    let sql = format!(
                        "INSERT INTO t12 VALUES ({}, 'thread_{}_{}', {})",
                        id,
                        t,
                        i,
                        id * 10
                    );
                    db_clone
                        .execute(&sql)
                        .expect("insert")
                        .materialize()
                        .expect("mat");
                    ops += 1;
                }
                ops
            }));
        }

        let total_ops: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        let elapsed = start.elapsed().as_millis() as u64;
        print_result(
            &format!("Concurrent INSERT {} ({} threads)", total_ops, n_threads),
            total_ops,
            elapsed,
        );
        elapsed
    };

    let ops_per_s = total_concurrent as f64 / (total_ms as f64 / 1000.0);
    println!("  -> Concurrent throughput: {:.0} ops/s", ops_per_s);
    if let Ok(db) = Arc::try_unwrap(db) {
        db.close().ok();
    }
}

// ═══════════════════════════════════════════════════════════════
// Summary helper
// ═══════════════════════════════════════════════════════════════

#[allow(dead_code)]
fn exec_count(db: &Database, sql: &str) -> i64 {
    let result = exec(db, sql);
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
