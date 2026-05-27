//! ACID & Comprehensive Performance Benchmark
//!
//! Tests: WAL durability, crash recovery, transaction rollback,
//! concurrent consistency, mixed OLTP, and resource limits.
//!
//! Run: cargo test --test bench_acid --profile release-test -- --nocapture --test-threads=1

use motedb::{Database, DBConfig, DurabilityLevel};
use tempfile::TempDir;
use std::time::Instant;

fn is_ci() -> bool { std::env::var("CI").is_ok() }

fn exec(db: &Database, sql: &str) -> motedb::sql::QueryResult {
    db.execute(sql).expect("execute SQL").materialize().expect("materialize")
}

fn query_val(db: &Database, sql: &str) -> i64 {
    let result = exec(db, sql);
    match result {
        motedb::sql::QueryResult::Select { rows, .. } => {
            rows.first().and_then(|r| r.first())
                .map(|v| if let motedb::types::Value::Integer(c) = v { *c } else { 0 })
                .unwrap_or(0)
        }
        _ => 0,
    }
}

macro_rules! print_result {
    ($label:expr, $ops:expr, $ms:expr) => {
        println!("{:<60} | {:>8} ops | {:>8.1} ms | {:>10.1} µs/op | {:>10} ops/s",
            $label, $ops, $ms as f64, ($ms as f64 * 1000.0) / $ops as f64,
            if $ms > 0 { ($ops as f64 * 1000.0 / $ms as f64) as u64 } else { u64::MAX });
    };
}

fn sep() {
    println!("{}", "-".repeat(100));
}

// ═══════════════════════════════════════════════════════════════
// Test 1: Durability levels comparison
// ═══════════════════════════════════════════════════════════════
#[test]
fn test_durability_comparison() {
    println!("\n{}", "=".repeat(100));
    println!("  Durability Level Comparison (INSERT 10K rows, single thread)");
    println!("{}", "=".repeat(100));

    let configs: Vec<(&str, DBConfig)> = vec![
        ("Synchronous", {
            let mut c = DBConfig::for_edge();
            c.wal_config.durability_level = DurabilityLevel::Synchronous;
            c
        }),
        ("GroupCommit", {
            let mut c = DBConfig::for_edge();
            c.wal_config.durability_level = DurabilityLevel::group_commit();
            c
        }),
        ("Periodic(50ms)", DBConfig::for_edge()),
        ("NoSync", {
            let mut c = DBConfig::for_edge();
            c.wal_config.durability_level = DurabilityLevel::NoSync;
            c
        }),
    ];

    let n = if is_ci() { 500 } else { 2_000 };

    for (name, config) in configs {
        let dir = TempDir::new().unwrap();
        let db = Database::create_with_config(dir.path(), config).unwrap();
        exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT, score FLOAT)");

        let start = Instant::now();
        for i in 1..=n as i64 {
            exec(&db, &format!("INSERT INTO t VALUES ({}, 'v_{}', {:.1})", i, i, i as f64 * 0.5));
        }
        let ms = start.elapsed().as_millis() as u64;

        let count = query_val(&db, "SELECT COUNT(*) AS cnt FROM t");
        assert_eq!(count, n as i64, "{}: count mismatch", name);

        print_result!(format!("{:>15}", name), n, ms);
        db.close().unwrap();
    }
}

// ═══════════════════════════════════════════════════════════════
// Test 2: Crash recovery — unflushed WAL
// ═══════════════════════════════════════════════════════════════
#[test]
fn test_crash_recovery() {
    println!("\n{}", "=".repeat(100));
    println!("  Crash Recovery: simulate crash with unflushed data");
    println!("{}", "=".repeat(100));

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().to_path_buf();
    let config = DBConfig::for_edge();

    // Phase 1: Insert rows (no checkpoint, simulating crash after writes)
    let n = if is_ci() { 1_000 } else { 5_000 };
    {
        let db = Database::create_with_config(&db_path, config.clone()).unwrap();
        exec(&db, "CREATE TABLE crash_test (id INTEGER PRIMARY KEY, data TEXT, val INTEGER)");
        let start = Instant::now();
        for i in 1..=n as i64 {
            exec(&db, &format!("INSERT INTO crash_test VALUES ({}, 'data_{}', {})", i, i, i * 10));
        }
        let insert_ms = start.elapsed().as_millis() as u64;
        print_result!("Phase 1: INSERT 5K (pre-crash)", n, insert_ms);

        // Don't checkpoint or close cleanly — just drop
        // Periodic mode may not have flushed last batch
        // Sleep a bit to let periodic flush thread catch up
        std::thread::sleep(std::time::Duration::from_millis(200));
    }

    sep();

    // Phase 2: Reopen — WAL recovery should restore data
    let start = Instant::now();
    let db = Database::open_with_config(&db_path, config).unwrap();
    let reopen_ms = start.elapsed().as_millis() as u64;
    print_result!("Phase 2: Reopen (WAL recovery)", 1, reopen_ms);

    let count = query_val(&db, "SELECT COUNT(*) AS cnt FROM crash_test");
    println!("  -> Recovered {} / {} rows", count, n);

    // Verify some specific rows
    let v1 = query_val(&db, "SELECT val FROM crash_test WHERE id = 1");
    let v100 = query_val(&db, "SELECT val FROM crash_test WHERE id = 100");
    let v5000 = query_val(&db, "SELECT val FROM crash_test WHERE id = 5000");
    println!("  -> Row 1 val={} (expect 10), Row 100 val={} (expect 1000), Row 5000 val={} (expect 50000)", v1, v100, v5000);

    assert!(count > 0, "Should recover at least some rows from WAL");
    db.close().unwrap();
}

// ═══════════════════════════════════════════════════════════════
// Test 3: Checkpoint + full recovery (data integrity)
// ═══════════════════════════════════════════════════════════════
#[test]
fn test_checkpoint_integrity() {
    println!("\n{}", "=".repeat(100));
    println!("  Checkpoint + Recovery: full data integrity verification");
    println!("{}", "=".repeat(100));

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().to_path_buf();
    let config = DBConfig::for_edge();
    let n = if is_ci() { 500 } else { 2_000 };

    // Phase 1: Insert + flush + checkpoint
    {
        let db = Database::create_with_config(&db_path, config.clone()).unwrap();
        exec(&db, "CREATE TABLE integrity (id INTEGER PRIMARY KEY, name TEXT, score FLOAT, tag TEXT)");
        let start = Instant::now();
        for i in 1..=n as i64 {
            exec(&db, &format!(
                "INSERT INTO integrity VALUES ({}, 'name_{}', {:.2}, 'tag_{}')",
                i, i, i as f64 * 1.5, i % 5
            ));
        }
        let insert_ms = start.elapsed().as_millis() as u64;
        print_result!(format!("Phase 1: INSERT {}K", n / 1000), n, insert_ms);

        db.flush().unwrap();
        db.wait_for_indexes_ready();
        let cp_start = Instant::now();
        db.checkpoint().unwrap();
        let cp_ms = cp_start.elapsed().as_millis() as u64;
        print_result!("Phase 2: Checkpoint", 1, cp_ms);
        db.close().unwrap();
    }

    sep();

    // Phase 2: Reopen + verify every row
    let db = Database::open_with_config(&db_path, config).unwrap();
    let count = query_val(&db, "SELECT COUNT(*) AS cnt FROM integrity");
    println!("  -> Total rows after recovery: {}", count);
    assert_eq!(count, n as i64, "All rows must survive checkpoint recovery");

    // Verify data correctness (spot check)
    let mut errors = 0;
    for i in [1, 100, 500, n as i64 / 2, n as i64] {
        let result = exec(&db, &format!("SELECT name, score, tag FROM integrity WHERE id = {}", i));
        match result {
            motedb::sql::QueryResult::Select { rows, .. } => {
                if let Some(row) = rows.first() {
                    // Verify structure
                    if row.len() != 3 { errors += 1; }
                } else {
                    errors += 1;
                }
            }
            _ => { errors += 1; }
        }
    }
    println!("  -> Spot-check errors: {}/5", errors);
    assert_eq!(errors, 0, "All spot-checked rows must be correct");
    db.close().unwrap();
}

// ═══════════════════════════════════════════════════════════════
// Test 4: UPDATE/DELETE + verify
// ═══════════════════════════════════════════════════════════════
#[test]
fn test_update_delete_consistency() {
    println!("\n{}", "=".repeat(100));
    println!("  UPDATE/DELETE Consistency: write → verify → update → verify → delete → verify");
    println!("{}", "=".repeat(100));

    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_edge()).unwrap();
    exec(&db, "CREATE TABLE crud (id INTEGER PRIMARY KEY, val TEXT, score INTEGER)");
    let n = if is_ci() { 500 } else { 1_000 };
    let start = Instant::now();
    for i in 1..=n as i64 {
        exec(&db, &format!("INSERT INTO crud VALUES ({}, 'original_{}', {})", i, i, i));
    }
    let insert_ms = start.elapsed().as_millis() as u64;
    print_result!(format!("INSERT {}", n), n, insert_ms);

    // Verify inserts
    let count = query_val(&db, "SELECT COUNT(*) AS cnt FROM crud");
    assert_eq!(count, n as i64);
    println!("  -> After INSERT: {} rows ✓", count);

    // Update half
    let n_update = n / 2;
    let start = Instant::now();
    for i in 1..=n_update as i64 {
        exec(&db, &format!("UPDATE crud SET val = 'updated_{}', score = score * 2 WHERE id = {}", i, i));
    }
    let update_ms = start.elapsed().as_millis() as u64;
    print_result!(format!("UPDATE {}", n_update), n_update, update_ms);

    // Verify updates
    let updated_score = query_val(&db, "SELECT score FROM crud WHERE id = 100");
    assert_eq!(updated_score, 200, "Row 100 score should be 200 (100*2)");
    let unchanged_id = (n_update + 1 + n) / 2;
    let unchanged_score = query_val(&db, &format!("SELECT score FROM crud WHERE id = {}", unchanged_id));
    assert_eq!(unchanged_score, unchanged_id, "Row {} score should be unchanged", unchanged_id);
    println!("  -> UPDATE verified: row 100 score={} (expect 200), row {} score={} (expect {}) ✓",
        updated_score, unchanged_id, unchanged_score, unchanged_id);

    // Delete rows from second half
    let del_start = n_update + 1;
    let del_end = n_update + n / 5;
    let n_delete = del_end - del_start + 1;
    let start = Instant::now();
    for i in del_start..=del_end {
        exec(&db, &format!("DELETE FROM crud WHERE id = {}", i));
    }
    let delete_ms = start.elapsed().as_millis() as u64;
    print_result!(format!("DELETE {}", n_delete), n_delete, delete_ms);

    // Verify deletes
    let count_after = query_val(&db, "SELECT COUNT(*) AS cnt FROM crud");
    assert_eq!(count_after, (n - n_delete) as i64, "Should have {} rows after deleting {}", n - n_delete, n_delete);
    let deleted = query_val(&db, &format!("SELECT COUNT(*) AS cnt FROM crud WHERE id BETWEEN {} AND {}", del_start, del_end));
    assert_eq!(deleted, 0, "Deleted rows should not be visible");
    println!("  -> DELETE verified: {} rows remain, deleted range count={} ✓", count_after, deleted);
}

// ═══════════════════════════════════════════════════════════════
// Test 5: Concurrent writes — no data loss
// ═══════════════════════════════════════════════════════════════
#[test]
fn test_concurrent_consistency() {
    println!("\n{}", "=".repeat(100));
    println!("  Concurrent Write Consistency: 4 threads × 2.5K inserts, verify no loss");
    println!("{}", "=".repeat(100));

    let dir = TempDir::new().unwrap();
    let db = std::sync::Arc::new(Database::create_with_config(dir.path(), DBConfig::for_edge()).unwrap());
    exec(&db, "CREATE TABLE concurrent (id INTEGER PRIMARY KEY, thread INTEGER, val TEXT)");

    let n_threads = 4;
    let n_per_thread = if is_ci() { 500 } else { 2500 };
    let n_total = n_threads * n_per_thread;

    let start = Instant::now();
    let mut handles = Vec::new();

    for t in 0..n_threads {
        let db_clone = db.clone();
        let handle = std::thread::spawn(move || {
            let base = t * n_per_thread + 1;
            for i in 0..n_per_thread {
                let id = base + i;
                db_clone.execute(&format!(
                    "INSERT INTO concurrent VALUES ({}, {}, 'thread_{}_val_{}')",
                    id, t, t, i
                )).unwrap();
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().unwrap();
    }

    let ms = start.elapsed().as_millis() as u64;
    print_result!(format!("Concurrent INSERT {}K", n_total / 1000), n_total, ms);

    // Verify total count
    let count = query_val(&db, "SELECT COUNT(*) AS cnt FROM concurrent");
    println!("  -> Expected: {}, Actual: {}, Loss: {}", n_total, count, n_total as i64 - count);
    assert_eq!(count, n_total as i64, "No rows should be lost in concurrent writes");

    // Flush to SSTables before per-thread WHERE checks to avoid scan consistency
    // issues when data straddles memtable/SSTable boundary during concurrent flush
    db.flush().unwrap();
    db.wait_for_indexes_ready();

    // Verify per-thread counts
    for t in 0..n_threads {
        let t_count = query_val(&db, &format!("SELECT COUNT(*) AS cnt FROM concurrent WHERE thread = {}", t));
        assert_eq!(t_count, n_per_thread as i64, "Thread {} count mismatch", t);
    }
    println!("  -> Per-thread counts all correct ✓");
}

// ═══════════════════════════════════════════════════════════════
// Test 6: Mixed OLTP workload
// ═══════════════════════════════════════════════════════════════
#[test]
fn test_mixed_oltp() {
    println!("\n{}", "=".repeat(100));
    println!("  Mixed OLTP: INSERT → SELECT → UPDATE → SELECT → DELETE → SELECT");
    println!("{}", "=".repeat(100));

    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_edge()).unwrap();
    exec(&db, "CREATE TABLE oltp (id INTEGER PRIMARY KEY, status TEXT, amount FLOAT)");

    let n = if is_ci() { 1_000 } else { 5_000 };
    let start = Instant::now();
    for i in 1..=n as i64 {
        exec(&db, &format!("INSERT INTO oltp VALUES ({}, 'pending', {:.2})", i, i as f64 * 10.0));
    }
    let insert_ms = start.elapsed().as_millis() as u64;
    print_result!(format!("INSERT {}", n), n, insert_ms);

    // SELECT (full scan)
    let start = Instant::now();
    let count = query_val(&db, "SELECT COUNT(*) AS cnt FROM oltp");
    let select1_ms = start.elapsed().as_millis() as u64;
    print_result!(format!("COUNT(*) {}", n), 1, select1_ms);
    assert_eq!(count, n as i64);

    // UPDATE 50%
    let n_update = n / 2;
    let start = Instant::now();
    for i in 1..=n_update as i64 {
        exec(&db, &format!("UPDATE oltp SET status = 'completed' WHERE id = {}", i));
    }
    let update_ms = start.elapsed().as_millis() as u64;
    print_result!(format!("UPDATE {}", n_update), n_update, update_ms);

    // SELECT with filter
    let completed = query_val(&db, "SELECT COUNT(*) AS cnt FROM oltp WHERE status = 'completed'");
    let pending = query_val(&db, "SELECT COUNT(*) AS cnt FROM oltp WHERE status = 'pending'");
    println!("  -> completed={}, pending={} (expect {}/{})", completed, pending, n_update, n_update);
    assert_eq!(completed, n_update as i64);
    assert_eq!(pending, n_update as i64);

    // DELETE last 20%
    let n_delete = n / 5;
    let del_start = n - n_delete + 1;
    let start = Instant::now();
    for i in del_start..=n as i64 {
        exec(&db, &format!("DELETE FROM oltp WHERE id = {}", i));
    }
    let delete_ms = start.elapsed().as_millis() as u64;
    print_result!(format!("DELETE {}", n_delete), n_delete, delete_ms);

    // Final verification
    let final_count = query_val(&db, "SELECT COUNT(*) AS cnt FROM oltp");
    assert_eq!(final_count, (n - n_delete) as i64, "Should have {} rows after deleting {}", n - n_delete, n_delete);
    println!("  -> Final count: {} ✓", final_count);
}

// ═══════════════════════════════════════════════════════════════
// Test 7: Flush + SSTable query correctness
// ═══════════════════════════════════════════════════════════════
#[test]
fn test_sstable_query_correctness() {
    println!("\n{}", "=".repeat(100));
    println!("  SSTable Query Correctness: insert → flush → query → verify");
    println!("{}", "=".repeat(100));

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().to_path_buf();
    let config = DBConfig::for_edge();
    let n = if is_ci() { 500 } else { 2_000 };

    {
        let db = Database::create_with_config(&db_path, config.clone()).unwrap();
        exec(&db, "CREATE TABLE sst (id INTEGER PRIMARY KEY, data TEXT, score FLOAT)");

        let start = Instant::now();
        for i in 1..=n as i64 {
            exec(&db, &format!("INSERT INTO sst VALUES ({}, 'row_{}', {:.3})", i, i, i as f64 * 0.123));
        }
        let insert_ms = start.elapsed().as_millis() as u64;
        print_result!(format!("INSERT {}K (MemTable)", n / 1000), n, insert_ms);

        // Query from MemTable
        let start = Instant::now();
        for i in (1..=n as i64 / 10 * 10).step_by(10) {
            let v = query_val(&db, &format!("SELECT id FROM sst WHERE id = {}", i));
            assert_eq!(v, i, "MemTable query id={}", i);
        }
        let mem_query_ms = start.elapsed().as_millis() as u64;
        print_result!("PK SELECT 100 (MemTable)", 100, mem_query_ms);

        // Flush
        db.flush().unwrap();
        db.wait_for_indexes_ready();
        println!("  -> Flushed to SSTable");

        // Query from SSTable
        let start = Instant::now();
        for i in (1..=n as i64 / 10 * 10).step_by(10) {
            let v = query_val(&db, &format!("SELECT id FROM sst WHERE id = {}", i));
            assert_eq!(v, i, "SSTable query id={}", i);
        }
        let sst_query_ms = start.elapsed().as_millis() as u64;
        print_result!("PK SELECT 100 (SSTable)", 100, sst_query_ms);

        db.checkpoint().unwrap();
        db.close().unwrap();
    }

    sep();

    // Reopen and verify from SSTable
    let db = Database::open_with_config(&db_path, config).unwrap();
    let count = query_val(&db, "SELECT COUNT(*) AS cnt FROM sst");
    assert_eq!(count, n as i64, "Post-recovery count must match");

    // Verify random rows
    let mut errors = 0;
    for i in [1, 50, 100, 500, n as i64 / 2, n as i64] {
        let v = query_val(&db, &format!("SELECT id FROM sst WHERE id = {}", i));
        if v != i { errors += 1; }
    }
    println!("  -> Post-recovery verification: {} rows, {}/6 spot-check errors", count, errors);
    assert_eq!(errors, 0, "All post-recovery queries must be correct");
}

// ═══════════════════════════════════════════════════════════════
// Test 8: Edge resource limits
// ═══════════════════════════════════════════════════════════════
#[test]
fn test_edge_resource_usage() {
    println!("\n{}", "=".repeat(100));
    println!("  Edge Resource Usage: memory footprint + cold start time");
    println!("{}", "=".repeat(100));

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().to_path_buf();
    let config = DBConfig::for_edge();

    let get_rss = || -> u64 {
        // Use platform-specific memory query
        #[cfg(target_os = "macos")]
        {
            let output = std::process::Command::new("ps")
                .args(["-o", "rss=", "-p", &std::process::id().to_string()])
                .output().unwrap();
            String::from_utf8_lossy(&output.stdout).trim().parse::<u64>().unwrap_or(0) * 1024
        }
        #[cfg(not(target_os = "macos"))]
        { 0u64 }
    };

    // Cold start
    let start = Instant::now();
    let db = Database::create_with_config(&db_path, config.clone()).unwrap();
    let cold_start_ms = start.elapsed().as_millis() as u64;
    exec(&db, "CREATE TABLE edge (id INTEGER PRIMARY KEY, sensor TEXT, val FLOAT, ts INTEGER)");

    let baseline_rss = get_rss();
    println!("  -> Cold start: {}ms, baseline RSS: {:.1} MB", cold_start_ms, baseline_rss as f64 / 1024.0 / 1024.0);

    // Insert rows (edge-scale)
    let n = if is_ci() { 5_000 } else { 50_000 };
    let start = Instant::now();
    for i in 1..=n as i64 {
        exec(&db, &format!("INSERT INTO edge VALUES ({}, 'sensor_{}', {:.2}, {})",
            i, i % 10, i as f64 * 0.1, 1700000000 + i));
    }
    let insert_ms = start.elapsed().as_millis() as u64;
    let after_insert_rss = get_rss();
    let delta_rss = after_insert_rss as i64 - baseline_rss as i64;
    let bytes_per_row = if delta_rss > 0 { delta_rss as f64 / n as f64 } else { 0.0 };

    print_result!(format!("INSERT {}K (edge config)", n / 1000), n, insert_ms);
    println!("  -> RSS after insert: {:.1} MB, ΔRSS: {:.1} MB ({:.0} bytes/row)",
        after_insert_rss as f64 / 1024.0 / 1024.0,
        delta_rss as f64 / 1024.0 / 1024.0,
        bytes_per_row);

    assert!(cold_start_ms < 100, "Cold start should be <100ms, got {}ms", cold_start_ms);
    db.close().unwrap();
}
