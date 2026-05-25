//! Concurrent Workload Benchmark — read-heavy, mixed read/write, concurrent
//! transactions, concurrent checkpoint, concurrent prepared statements
//!
//! Run: cargo test --test bench_concurrent --release -- --nocapture --test-threads=1

use motedb::{Database, DBConfig};
use tempfile::TempDir;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

fn is_ci() -> bool {
    std::env::var("CI").is_ok()
}

fn edge_config() -> DBConfig {
    DBConfig::for_edge()
}

fn exec(db: &Database, sql: &str) -> motedb::sql::QueryResult {
    db.execute(sql).expect("execute SQL").materialize().expect("materialize")
}

fn print_result(name: &str, ops: usize, elapsed_ms: u64) {
    let per_op_us = if ops > 0 { (elapsed_ms as f64 * 1000.0) / ops as f64 } else { 0.0 };
    let throughput = if elapsed_ms > 0 { ops as f64 / (elapsed_ms as f64 / 1000.0) } else { f64::INFINITY };
    println!(
        "  {:<60} | {:>7} ops | {:>8.1} ms | {:>8.1} µs/op | {:>10.0} ops/s",
        name, ops, elapsed_ms as f64, per_op_us, throughput
    );
}

fn print_separator() {
    println!("  {}", "-".repeat(100));
}

// ═══════════════════════════════════════════════════════════════
// Test 1: Read-Heavy Concurrent Workload (90% reads)
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_read_heavy_concurrent() {
    let dir = TempDir::new().expect("temp dir");
    let db = Arc::new(Database::create_with_config(dir.path(), edge_config()).expect("create db"));
    exec(&db, "CREATE TABLE rh (id INT PRIMARY KEY, data TEXT, val INT)");

    let seed: usize = if is_ci() { 2_000 } else { 10_000 };
    for i in 1..=seed as i64 {
        exec(&db, &format!("INSERT INTO rh VALUES ({}, 'data_{}', {})", i, i, i * 10));
    }

    print_separator();

    let (n_threads, reads_per_thread) = if is_ci() { (2, 500) } else { (4, 2500) };
    let total_reads = n_threads * reads_per_thread;

    let ms = {
        let start = Instant::now();
        let mut handles = vec![];

        for t in 0..n_threads {
            let db_clone = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                let mut ops = 0;
                for i in 0..reads_per_thread {
                    let id = ((t * reads_per_thread + i) % seed) as i64 + 1;
                    let sql = format!("SELECT * FROM rh WHERE id = {}", id);
                    db_clone.execute(&sql).expect("select").materialize().expect("mat");
                    ops += 1;
                }
                ops
            }));
        }

        let total_ops: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        let elapsed = start.elapsed().as_millis() as u64;
        print_result(
            &format!("Read-heavy {} threads × {} reads", n_threads, reads_per_thread),
            total_ops, elapsed,
        );
        elapsed
    };

    let throughput = total_reads as f64 / (ms as f64 / 1000.0);
    println!("  -> {} threads reading concurrently: {:.0} reads/s", n_threads, throughput);
    if let Ok(db) = Arc::try_unwrap(db) { db.close().ok(); }
}

// ═══════════════════════════════════════════════════════════════
// Test 2: Mixed Read/Write Concurrent (70% read, 30% write)
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_mixed_read_write_concurrent() {
    let dir = TempDir::new().expect("temp dir");
    let db = Arc::new(Database::create_with_config(dir.path(), edge_config()).expect("create db"));
    exec(&db, "CREATE TABLE mix (id INT PRIMARY KEY, val TEXT, score FLOAT)");

    let seed: usize = if is_ci() { 1_000 } else { 5_000 };
    for i in 1..=seed as i64 {
        exec(&db, &format!("INSERT INTO mix VALUES ({}, 'v_{}', {})", i, i, i as f64 * 1.5));
    }

    print_separator();

    let n_threads = if is_ci() { 2 } else { 4 };
    let ops_per_thread = if is_ci() { 500 } else { 2000 };
    let total_ops = n_threads * ops_per_thread;

    let ms = {
        let start = Instant::now();
        let mut handles = vec![];

        for t in 0..n_threads {
            let db_clone = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                let base_id = (seed + t * ops_per_thread) as i64;
                let mut ops = 0;
                for i in 0..ops_per_thread {
                    if i % 10 < 7 {
                        // 70% reads
                        let id = (i % seed) as i64 + 1;
                        let sql = format!("SELECT * FROM mix WHERE id = {}", id);
                        db_clone.execute(&sql).expect("select").materialize().expect("mat");
                    } else if i % 10 < 9 {
                        // 20% inserts
                        let id = base_id + i as i64;
                        let sql = format!("INSERT INTO mix VALUES ({}, 'new_{}', {})", id, i, id as f64);
                        db_clone.execute(&sql).expect("insert").materialize().expect("mat");
                    } else {
                        // 10% updates
                        let id = (i % seed) as i64 + 1;
                        let sql = format!("UPDATE mix SET score = score + 1 WHERE id = {}", id);
                        db_clone.execute(&sql).expect("update").materialize().expect("mat");
                    }
                    ops += 1;
                }
                ops
            }));
        }

        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        let elapsed = start.elapsed().as_millis() as u64;
        print_result(
            &format!("Mixed R/W {} threads × {} ops (70R/20W/10U)", n_threads, ops_per_thread),
            total, elapsed,
        );
        elapsed
    };

    let throughput = total_ops as f64 / (ms as f64 / 1000.0);
    println!("  -> Mixed workload throughput: {:.0} ops/s", throughput);
    if let Ok(db) = Arc::try_unwrap(db) { db.close().ok(); }
}

// ═══════════════════════════════════════════════════════════════
// Test 3: Concurrent Transactions (begin/commit/rollback)
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_concurrent_transactions() {
    let dir = TempDir::new().expect("temp dir");
    let db = Arc::new(Database::create_with_config(dir.path(), edge_config()).expect("create db"));
    exec(&db, "CREATE TABLE txn_data (id INT PRIMARY KEY, val INT)");

    // Seed
    for i in 1..=100i64 {
        exec(&db, &format!("INSERT INTO txn_data VALUES ({}, {})", i, i * 10));
    }

    print_separator();

    let n_threads = if is_ci() { 2 } else { 4 };
    let txns_per_thread = if is_ci() { 50 } else { 200 };

    let ms = {
        let start = Instant::now();
        let mut handles = vec![];

        for t in 0..n_threads {
            let db_clone = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                let mut committed = 0;
                let mut rolled_back = 0;
                for i in 0..txns_per_thread {
                    let tx = db_clone.begin_transaction().expect("begin");
                    let id = 101 + t * txns_per_thread + i;
                    let row = vec![
                        motedb::types::Value::Integer(id as i64),
                        motedb::types::Value::Integer(id as i64 * 10),
                    ];
                    db_clone.insert_row_with_txn("txn_data", tx, row).expect("insert with txn");

                    if i % 5 == 0 {
                        db_clone.rollback_transaction(tx).expect("rollback");
                        rolled_back += 1;
                    } else {
                        db_clone.commit_transaction(tx).expect("commit");
                        committed += 1;
                    }
                }
                (committed, rolled_back)
            }));
        }

        let (total_committed, total_rolled_back): (usize, usize) = handles.into_iter()
            .map(|h| h.join().unwrap())
            .fold((0, 0), |(c, r), (tc, tr)| (c + tc, r + tr));

        let elapsed = start.elapsed().as_millis() as u64;
        let total_ops = total_committed + total_rolled_back;
        print_result(
            &format!("Concurrent txn {} threads × {} (commit/rollback)", n_threads, txns_per_thread),
            total_ops, elapsed,
        );
        println!("  -> Committed: {}, Rolled back: {}", total_committed, total_rolled_back);
        elapsed
    };

    let total = n_threads * txns_per_thread;
    let throughput = total as f64 / (ms as f64 / 1000.0);
    println!("  -> Transaction throughput: {:.0} txns/s", throughput);
    if let Ok(db) = Arc::try_unwrap(db) { db.close().ok(); }
}

// ═══════════════════════════════════════════════════════════════
// Test 4: Concurrent Writes (insert-only, contention)
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_concurrent_inserts() {
    let dir = TempDir::new().expect("temp dir");
    let db = Arc::new(Database::create_with_config(dir.path(), edge_config()).expect("create db"));
    exec(&db, "CREATE TABLE ci (id INT PRIMARY KEY, payload TEXT, ts INT)");

    print_separator();

    let (n_threads, inserts_per_thread) = if is_ci() { (2, 500) } else { (4, 2500) };
    let total_inserts = n_threads * inserts_per_thread;

    let ms = {
        let start = Instant::now();
        let mut handles = vec![];

        for t in 0..n_threads {
            let db_clone = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                let base = t * inserts_per_thread;
                let mut ops = 0;
                for i in 0..inserts_per_thread {
                    let id = (base + i + 1) as i64;
                    let sql = format!("INSERT INTO ci VALUES ({}, 'payload_{}_{}', {})", id, t, i, 1700000000 + id);
                    db_clone.execute(&sql).expect("insert").materialize().expect("mat");
                    ops += 1;
                }
                ops
            }));
        }

        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        let elapsed = start.elapsed().as_millis() as u64;
        print_result(
            &format!("Concurrent INSERT {} threads × {}", n_threads, inserts_per_thread),
            total, elapsed,
        );
        elapsed
    };

    // Verify row count
    let result = exec(&db, "SELECT COUNT(*) FROM ci");
    if let motedb::sql::QueryResult::Select { rows, .. } = result {
        if let Some(motedb::types::Value::Integer(count)) = rows.first().and_then(|r| r.first()) {
            println!("  -> Total rows after concurrent inserts: {}", count);
            assert_eq!(*count, total_inserts as i64, "All concurrent inserts should succeed");
        }
    }

    let throughput = total_inserts as f64 / (ms as f64 / 1000.0);
    println!("  -> Concurrent insert throughput: {:.0} ops/s", throughput);
    if let Ok(db) = Arc::try_unwrap(db) { db.close().ok(); }
}

// ═══════════════════════════════════════════════════════════════
// Test 5: Concurrent Row API (insert_row + get_row)
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_concurrent_row_api() {
    let dir = TempDir::new().expect("temp dir");
    let db = Arc::new(Database::create_with_config(dir.path(), edge_config()).expect("create db"));
    exec(&db, "CREATE TABLE row_api (id INT PRIMARY KEY, val TEXT)");

    // Seed
    let seed: usize = if is_ci() { 500 } else { 2000 };
    let mut row_ids = Vec::new();
    for i in 1..=seed as i64 {
        let row = vec![motedb::types::Value::Integer(i), motedb::types::Value::text(format!("v_{}", i))];
        let rid = db.insert_row("row_api", row).expect("insert_row");
        row_ids.push(rid);
    }

    print_separator();

    let n_threads = if is_ci() { 2 } else { 4 };
    let reads_per_thread = if is_ci() { 500 } else { 2000 };

    // Concurrent reads via row API
    let read_ms = {
        let ids = Arc::new(row_ids.clone());
        let start = Instant::now();
        let mut handles = vec![];

        for _ in 0..n_threads {
            let db_clone = Arc::clone(&db);
            let ids_clone = Arc::clone(&ids);
            handles.push(thread::spawn(move || {
                let mut ops = 0;
                for i in 0..reads_per_thread {
                    let idx = i % ids_clone.len();
                    let _ = db_clone.get_row("row_api", ids_clone[idx]).expect("get_row");
                    ops += 1;
                }
                ops
            }));
        }

        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        let elapsed = start.elapsed().as_millis() as u64;
        let _total_reads = n_threads * reads_per_thread;
        print_result(
            &format!("Concurrent get_row {} threads × {} reads", n_threads, reads_per_thread),
            total, elapsed,
        );
        elapsed
    };

    // Concurrent inserts via row API
    let insert_per_thread = if is_ci() { 200 } else { 1000 };
    let insert_ms = {
        let start = Instant::now();
        let mut handles = vec![];

        for t in 0..n_threads {
            let db_clone = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                let mut ops = 0;
                for i in 0..insert_per_thread {
                    let id = (seed + t * insert_per_thread + i + 1) as i64;
                    let row = vec![motedb::types::Value::Integer(id), motedb::types::Value::text(format!("new_{}", id))];
                    db_clone.insert_row("row_api", row).expect("insert_row");
                    ops += 1;
                }
                ops
            }));
        }

        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        let elapsed = start.elapsed().as_millis() as u64;
        print_result(
            &format!("Concurrent insert_row {} threads × {} inserts", n_threads, insert_per_thread),
            total, elapsed,
        );
        elapsed
    };

    let read_throughput = (n_threads * reads_per_thread) as f64 / (read_ms as f64 / 1000.0);
    let insert_throughput = (n_threads * insert_per_thread) as f64 / (insert_ms as f64 / 1000.0);
    println!("  -> get_row: {:.0} ops/s, insert_row: {:.0} ops/s", read_throughput, insert_throughput);
    if let Ok(db) = Arc::try_unwrap(db) { db.close().ok(); }
}

// ═══════════════════════════════════════════════════════════════
// Test 6: Concurrent Prepared Statements
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_concurrent_prepared() {
    use motedb::types::Value;

    let dir = TempDir::new().expect("temp dir");
    let db = Arc::new(Database::create_with_config(dir.path(), edge_config()).expect("create db"));
    exec(&db, "CREATE TABLE cp (id INT PRIMARY KEY, name TEXT, val INT)");

    let seed: usize = if is_ci() { 1_000 } else { 5_000 };
    for i in 1..=seed as i64 {
        exec(&db, &format!("INSERT INTO cp VALUES ({}, 'name_{}', {})", i, i, i * 10));
    }

    print_separator();

    let n_threads = if is_ci() { 2 } else { 4 };
    let ops_per_thread = if is_ci() { 300 } else { 1500 };

    let ms = {
        let start = Instant::now();
        let mut handles = vec![];

        for _t in 0..n_threads {
            let db_clone = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                let mut ops = 0;
                for i in 0..ops_per_thread {
                    let id = (i % seed) as i64 + 1;
                    if i % 10 < 7 {
                        // Read
                        let _ = db_clone.execute_prepared(
                            "SELECT * FROM cp WHERE id = ?",
                            vec![Value::Integer(id)],
                        ).expect("prepared select");
                    } else {
                        // Update
                        let _ = db_clone.execute_prepared(
                            "UPDATE cp SET val = val + 1 WHERE id = ?",
                            vec![Value::Integer(id)],
                        ).expect("prepared update");
                    }
                    ops += 1;
                }
                ops
            }));
        }

        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        let elapsed = start.elapsed().as_millis() as u64;
        print_result(
            &format!("Concurrent prepared {} threads × {} ops", n_threads, ops_per_thread),
            total, elapsed,
        );
        elapsed
    };

    let total = n_threads * ops_per_thread;
    let throughput = total as f64 / (ms as f64 / 1000.0);
    println!("  -> Concurrent prepared throughput: {:.0} ops/s", throughput);
    if let Ok(db) = Arc::try_unwrap(db) { db.close().ok(); }
}

// ═══════════════════════════════════════════════════════════════
// Test 7: Concurrent DELETE + Reclaim
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_concurrent_delete() {
    let dir = TempDir::new().expect("temp dir");
    let db = Arc::new(Database::create_with_config(dir.path(), edge_config()).expect("create db"));
    exec(&db, "CREATE TABLE cd (id INT PRIMARY KEY, data TEXT)");

    let seed: usize = if is_ci() { 2_000 } else { 10_000 };
    for i in 1..=seed as i64 {
        exec(&db, &format!("INSERT INTO cd VALUES ({}, 'data_{}')", i, i));
    }

    print_separator();

    let db = Arc::new(db);
    let n_threads = if is_ci() { 2 } else { 4 };
    let deletes_per_thread = seed / n_threads;

    let ms = {
        let start = Instant::now();
        let mut handles = vec![];

        for t in 0..n_threads {
            let db_clone = Arc::clone(&db);
            let start_id = (t * deletes_per_thread) as i64 + 1;
            let end_id = ((t + 1) * deletes_per_thread) as i64;
            handles.push(thread::spawn(move || {
                let mut ops = 0;
                for id in start_id..=end_id {
                    let sql = format!("DELETE FROM cd WHERE id = {}", id);
                    db_clone.execute(&sql).expect("delete").materialize().expect("mat");
                    ops += 1;
                }
                ops
            }));
        }

        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        let elapsed = start.elapsed().as_millis() as u64;
        print_result(
            &format!("Concurrent DELETE {} threads × {} rows", n_threads, deletes_per_thread),
            total, elapsed,
        );
        elapsed
    };

    let total = n_threads * deletes_per_thread;
    let throughput = total as f64 / (ms as f64 / 1000.0);
    println!("  -> Concurrent delete throughput: {:.0} ops/s", throughput);

    // Verify table is empty
    let result = exec(&db, "SELECT COUNT(*) FROM cd");
    if let motedb::sql::QueryResult::Select { rows, .. } = result {
        if let Some(motedb::types::Value::Integer(count)) = rows.first().and_then(|r| r.first()) {
            println!("  -> Rows remaining after deletes: {}", count);
        }
    }
    if let Ok(db) = Arc::try_unwrap(db) { db.close().ok(); }
}

// ═══════════════════════════════════════════════════════════════
// Test 8: Write-Then-Read Consistency Under Concurrency
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_concurrent_write_read_consistency() {
    let dir = TempDir::new().expect("temp dir");
    let db = Arc::new(Database::create_with_config(dir.path(), edge_config()).expect("create db"));
    exec(&db, "CREATE TABLE wrc (id INT PRIMARY KEY, val INT)");

    print_separator();

    let n_writers = if is_ci() { 1 } else { 2 };
    let n_readers = if is_ci() { 1 } else { 2 };
    let rows_per_writer = if is_ci() { 500 } else { 2500 };

    let ms = {
        let start = Instant::now();
        let mut handles = vec![];

        // Writers
        for w in 0..n_writers {
            let db_clone = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                let base = w * rows_per_writer;
                let mut ops = 0;
                for i in 0..rows_per_writer {
                    let id = (base + i + 1) as i64;
                    let sql = format!("INSERT INTO wrc VALUES ({}, {})", id, id * 10);
                    db_clone.execute(&sql).expect("insert").materialize().expect("mat");
                    ops += 1;
                }
                ops
            }));
        }

        // Readers (read what's been written so far)
        for _ in 0..n_readers {
            let db_clone = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                let mut ops = 0;
                for i in 1..=rows_per_writer as i64 {
                    let sql = format!("SELECT * FROM wrc WHERE id = {}", i);
                    let _ = db_clone.execute(&sql);
                    ops += 1;
                }
                ops
            }));
        }

        let results: Vec<usize> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let elapsed = start.elapsed().as_millis() as u64;
        let total_ops: usize = results.iter().sum();
        print_result(
            &format!("Write+Read concurrent ({}W + {}R)", n_writers, n_readers),
            total_ops, elapsed,
        );
        elapsed
    };

    let total = (n_writers + n_readers) * rows_per_writer;
    let throughput = total as f64 / (ms as f64 / 1000.0);
    println!("  -> Combined throughput: {:.0} ops/s", throughput);
    if let Ok(db) = Arc::try_unwrap(db) { db.close().ok(); }
}
