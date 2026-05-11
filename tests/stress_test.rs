//! Stress Test — 验证 P0 查询优化 + Edge 优化的效果
//!
//! 覆盖场景：
//! 1. 大规模 INSERT (50K)
//! 2. PK 点查 (10K queries, MemTable + SSTable)
//! 3. Column Index 范围查询
//! 4. 全表扫描 (50K rows)
//! 5. 混合 CRUD (INSERT + UPDATE + DELETE + SELECT)
//! 6. PreparedStatement 缓存命中率
//! 7. RowCache 命中率

use motedb::Database;
use tempfile::TempDir;
use std::time::Instant;

fn is_ci() -> bool { std::env::var("CI").is_ok() }

fn create_db() -> (Database, TempDir) {
    let dir = TempDir::new().expect("temp dir");
    let db = Database::create(dir.path()).expect("create db");
    (db, dir)
}

fn exec(db: &Database, sql: &str) -> motedb::sql::QueryResult {
    db.execute(sql).expect("execute SQL").materialize().expect("materialize")
}

fn print_result(name: &str, ops: usize, elapsed_ms: u64) {
    let per_op_us = if ops > 0 { (elapsed_ms as f64 * 1000.0) / ops as f64 } else { 0.0 };
    let throughput = if elapsed_ms > 0 { ops as f64 / (elapsed_ms as f64 / 1000.0) } else { f64::INFINITY };
    println!(
        "{:<55} | {:>6} ops | {:>8.1} ms | {:>8.1} µs/op | {:>10.0} ops/s",
        name, ops, elapsed_ms as f64, per_op_us, throughput
    );
}

// ── Test 1: 大规模 INSERT ──

#[test]
fn stress_insert_50k() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, score FLOAT, age INTEGER)");

    let n: usize = if is_ci() { 10_000 } else { 50_000 };

    let ms = {
        let start = Instant::now();
        for i in 1..=n as i64 {
            exec(&db, &format!(
                "INSERT INTO users VALUES ({}, 'user_{}', 'user_{}@test.com', {}, {})",
                i, i, i, i as f64 * 1.5, 20 + (i % 50)
            ));
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("INSERT {} rows (5 cols, PK auto)", n), n, ms);

    db.flush().expect("flush");
    db.wait_for_indexes_ready();

    let cnt = exec_count(&db, "SELECT COUNT(*) AS cnt FROM users");
    println!("  -> Row count after flush: {}", cnt);
    assert!(cnt > 0, "Should have rows after flush");
}

// ── Test 2: PK 点查 (MemTable + SSTable) ──

#[test]
fn stress_pk_lookup() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT, score FLOAT, tag TEXT)");

    let n: usize = if is_ci() { 5_000 } else { 20_000 };
    let q: usize = if is_ci() { 2_000 } else { 10_000 };

    // Seed
    let seed_ms = {
        let start = Instant::now();
        for i in 1..=n as i64 {
            exec(&db, &format!(
                "INSERT INTO t VALUES ({}, 'val_{}', {}, 'tag_{}')",
                i, i, i as f64, i % 10
            ));
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("Seed INSERT {} rows", n), n, seed_ms);

    // PK lookup — MemTable (all in memory)
    let mem_ms = {
        let start = Instant::now();
        for i in 1..=q as i64 {
            exec(&db, &format!("SELECT * FROM t WHERE id = {}", i));
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("PK SELECT {} queries (MemTable)", q), q, mem_ms);

    // Flush to SSTable
    db.flush().expect("flush");
    db.wait_for_indexes_ready();

    // PK lookup — SSTable (tests RowCache + PreparedStatement cache)
    let sst_ms = {
        let start = Instant::now();
        for i in 1..=q as i64 {
            exec(&db, &format!("SELECT * FROM t WHERE id = {}", i));
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("PK SELECT {} queries (SSTable + Cache)", q), q, sst_ms);

    // Repeated PK queries (tests PreparedStatement cache hit)
    let cached_reps = if is_ci() { 20 } else { 100 };
    let cached_total = 100 * cached_reps;
    let cached_ms = {
        let start = Instant::now();
        // Repeat same 100 queries N times
        for _ in 0..cached_reps {
            for i in 1..=100i64 {
                exec(&db, &format!("SELECT * FROM t WHERE id = {}", i));
            }
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("PK SELECT {} (repeated 100 queries × {}, stmt cache)", cached_total, cached_reps), cached_total, cached_ms);
}

// ── Test 3: Column Index 查询 ──

#[test]
fn stress_column_index() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price FLOAT, stock INTEGER)");

    let n: usize = if is_ci() { 5_000 } else { 30_000 };
    let queries: usize = if is_ci() { 20 } else { 100 };

    // Seed
    let seed_ms = {
        let start = Instant::now();
        for i in 1..=n as i64 {
            let cat = match i % 5 {
                0 => "electronics",
                1 => "books",
                2 => "clothing",
                3 => "food",
                _ => "toys",
            };
            exec(&db, &format!(
                "INSERT INTO products VALUES ({}, 'product_{}', '{}', {}, {})",
                i, i, cat, 10.0 + (i as f64 % 990.0), i % 100
            ));
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("Seed INSERT {} rows (5 categories)", n), n, seed_ms);

    // Create column index
    exec(&db, "CREATE INDEX idx_category ON products (category)");
    exec(&db, "CREATE INDEX idx_price ON products (price)");

    db.flush().expect("flush");
    db.wait_for_indexes_ready();

    // Exact match on category
    let eq_ms = {
        let start = Instant::now();
        for _ in 0..queries {
            exec(&db, "SELECT * FROM products WHERE category = 'electronics'");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("Column eq scan × {} (category='electronics')", queries), queries, eq_ms);

    // Range query on price
    let range_ms = {
        let start = Instant::now();
        for _ in 0..queries {
            exec(&db, "SELECT * FROM products WHERE price > 500.0 AND price < 600.0");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("Column range scan × {} (500 < price < 600)", queries), queries, range_ms);
}

// ── Test 4: 全表扫描 ──

#[test]
fn stress_full_scan() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE events (id INTEGER PRIMARY KEY, event_type TEXT, payload TEXT, ts INTEGER)");

    let n: usize = if is_ci() { 10_000 } else { 50_000 };
    let count_rounds: usize = if is_ci() { 10 } else { 50 };

    let seed_ms = {
        let start = Instant::now();
        for i in 1..=n as i64 {
            exec(&db, &format!(
                "INSERT INTO events VALUES ({}, 'type_{}', 'payload_data_{}', {})",
                i, i % 20, i, 1700000000 + i
            ));
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("Seed INSERT {} rows", n), n, seed_ms);

    // Scan from MemTable
    let mem_scan_ms = {
        let start = Instant::now();
        exec(&db, "SELECT * FROM events");
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("SELECT * {} rows (MemTable)", n), n, mem_scan_ms);

    // Flush
    db.flush().expect("flush");
    db.wait_for_indexes_ready();

    // Scan from SSTable
    let sst_scan_ms = {
        let start = Instant::now();
        exec(&db, "SELECT * FROM events");
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("SELECT * {} rows (SSTable)", n), n, sst_scan_ms);

    // Scan with filter
    let filter_ms = {
        let start = Instant::now();
        exec(&db, "SELECT * FROM events WHERE event_type = 'type_5'");
        start.elapsed().as_millis() as u64
    };
    print_result("SELECT * with WHERE filter (SSTable, 1/20 match)", n, filter_ms);

    // COUNT(*) fast path
    let count_ms = {
        let start = Instant::now();
        for _ in 0..count_rounds {
            exec(&db, "SELECT COUNT(*) AS cnt FROM events");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("COUNT(*) × {} (SSTable)", count_rounds), count_rounds, count_ms);
}

// ── Test 5: 混合 CRUD (INSERT + UPDATE + DELETE + SELECT) ──

#[test]
fn stress_mixed_crud() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT, amount FLOAT, status TEXT)");

    let n: usize = if is_ci() { 5_000 } else { 30_000 };
    let selects: usize = if is_ci() { 1_000 } else { 5_000 };

    let total_ms = {
        let start = Instant::now();

        // Phase 1: INSERT
        for i in 1..=n as i64 {
            exec(&db, &format!(
                "INSERT INTO orders VALUES ({}, 'customer_{}', {}, 'pending')",
                i, i % 1000, 10.0 + (i as f64 % 990.0)
            ));
        }

        // Phase 2: UPDATE (1/3)
        let _updates = n / 3;
        for i in (1..=n as i64).step_by(3) {
            exec(&db, &format!(
                "UPDATE orders SET status = 'shipped', amount = amount + 10 WHERE id = {}", i
            ));
        }

        // Phase 3: DELETE (1/5)
        let _deletes = n / 5;
        for i in (1..=n as i64).step_by(5) {
            exec(&db, &format!("DELETE FROM orders WHERE id = {}", i));
        }

        // Phase 4: Point SELECT (random)
        for i in (1..=selects as i64).rev() {
            exec(&db, &format!("SELECT * FROM orders WHERE id = {}", i));
        }

        start.elapsed().as_millis() as u64
    };

    let total_ops = n + n / 3 + n / 5 + selects;
    print_result(
        &format!("Mixed CRUD ({} ops: {}ins + {}upd + {}del + {}sel)",
            total_ops, n, n/3, n/5, selects),
        total_ops, total_ms
    );
}

// ── Test 6: Batch INSERT 性能 ──

#[test]
fn stress_batch_insert() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE metrics (id INTEGER PRIMARY KEY, host TEXT, cpu FLOAT, mem FLOAT, ts INTEGER)");

    let n: usize = if is_ci() { 10_000 } else { 50_000 };
    let batch: usize = if is_ci() { 100 } else { 500 };

    let ms = {
        let start = Instant::now();
        let mut id = 1i64;
        while id <= n as i64 {
            let end = (id + batch as i64 - 1).min(n as i64);
            // Simulate batch by inserting a chunk
            for i in id..=end {
                exec(&db, &format!(
                    "INSERT INTO metrics VALUES ({}, 'host_{}', {}, {}, {})",
                    i, i % 10, (i as f64 % 100.0), (i as f64 % 50.0), 1700000000 + i
                ));
            }
            id = end + 1;
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("INSERT {} rows (batch {} chunks)", n, n / batch), n, ms);
}

// ── Test 7: PreparedStatement 缓存效果 ──

#[test]
fn stress_prepared_statement_cache() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE cache_test (id INTEGER PRIMARY KEY, data TEXT)");

    let n: usize = if is_ci() { 2_000 } else { 5_000 };

    // Seed
    for i in 1..=n as i64 {
        exec(&db, &format!("INSERT INTO cache_test VALUES ({}, 'data_{}')", i, i));
    }

    // Phase 1: Cold cache — 1000 different queries
    let cold_ms = {
        let start = Instant::now();
        for i in 1..=1000i64 {
            exec(&db, &format!("SELECT * FROM cache_test WHERE id = {}", i));
        }
        start.elapsed().as_millis() as u64
    };
    print_result("PK SELECT 1K queries (cold stmt cache -> warm)", 1000, cold_ms);

    // Phase 2: Hot cache — same 100 queries repeated 100 times
    let hot_ms = {
        let start = Instant::now();
        for _ in 0..100 {
            for i in 1..=100i64 {
                exec(&db, &format!("SELECT * FROM cache_test WHERE id = {}", i));
            }
        }
        start.elapsed().as_millis() as u64
    };
    print_result("PK SELECT 10K (100 unique x 100 repeat, stmt cache hit)", 10_000, hot_ms);

    let hot_per_op = hot_ms as f64 * 1000.0 / 10_000.0;
    let cold_per_op = cold_ms as f64 * 1000.0 / 1_000.0;
    let speedup = if hot_per_op > 0.0 { cold_per_op / hot_per_op } else { 0.0 };
    println!("  -> Cold: {:.1} us/op, Hot: {:.1} us/op, Speedup: {:.1}x",
        cold_per_op, hot_per_op, speedup);
}

// ── Helper ──

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
