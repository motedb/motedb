//! Index Operations Benchmark — index creation time, column index query performance,
//! multiple indexes, index drop+recreate, vector index build time, text index build time
//!
//! Run: cargo test --test bench_index_ops --release -- --nocapture --test-threads=1

use motedb::{Database, DBConfig, types::Value};
use motedb::types::Tensor;
use tempfile::TempDir;
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
// Test 1: Column Index Creation Time
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_column_index_creation() {
    let dir = TempDir::new().expect("temp dir");
    let db = Database::create_with_config(dir.path(), edge_config()).expect("create db");
    exec(&db, "CREATE TABLE products (id INT PRIMARY KEY, name TEXT, category TEXT, price FLOAT, stock INT)");

    let n: usize = if is_ci() { 5_000 } else { 30_000 };

    // Seed
    let categories = ["electronics", "books", "clothing", "food", "toys"];
    for i in 1..=n as i64 {
        let cat = categories[(i as usize) % categories.len()];
        exec(&db, &format!(
            "INSERT INTO products VALUES ({}, 'prod_{}', '{}', {:.1}, {})",
            i, i, cat, 10.0 + (i as f64 % 990.0), i % 100
        ));
    }

    print_separator();

    // Create index on TEXT column
    let cat_idx_ms = {
        let start = Instant::now();
        exec(&db, "CREATE INDEX idx_cat ON products (category) USING COLUMN");
        db.wait_for_indexes_ready();
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("CREATE COLUMN INDEX on category ({} rows)", n), 1, cat_idx_ms);

    // Create index on FLOAT column
    let price_idx_ms = {
        let start = Instant::now();
        exec(&db, "CREATE INDEX idx_price ON products (price) USING COLUMN");
        db.wait_for_indexes_ready();
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("CREATE COLUMN INDEX on price ({} rows)", n), 1, price_idx_ms);

    // Create index on INT column
    let stock_idx_ms = {
        let start = Instant::now();
        exec(&db, "CREATE INDEX idx_stock ON products (stock) USING COLUMN");
        db.wait_for_indexes_ready();
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("CREATE COLUMN INDEX on stock ({} rows)", n), 1, stock_idx_ms);

    println!("  -> Total index build time: {}ms", cat_idx_ms + price_idx_ms + stock_idx_ms);
    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 2: Column Index Query Performance
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_column_index_queries() {
    let dir = TempDir::new().expect("temp dir");
    let db = Database::create_with_config(dir.path(), edge_config()).expect("create db");
    exec(&db, "CREATE TABLE items (id INT PRIMARY KEY, tag TEXT, score FLOAT, qty INT)");

    let n: usize = if is_ci() { 5_000 } else { 30_000 };
    let tags = ["alpha", "beta", "gamma", "delta", "epsilon"];

    for i in 1..=n as i64 {
        let tag = tags[(i as usize) % tags.len()];
        exec(&db, &format!(
            "INSERT INTO items VALUES ({}, '{}', {:.1}, {})",
            i, tag, 10.0 + (i as f64 % 990.0), i % 50
        ));
    }

    // Create indexes
    exec(&db, "CREATE INDEX idx_tag ON items (tag) USING COLUMN");
    db.wait_for_indexes_ready();
    // Only create second index for CI (smaller dataset)
    if n <= 5000 {
        exec(&db, "CREATE INDEX idx_score ON items (score) USING COLUMN");
        db.wait_for_indexes_ready();
    }

    print_separator();

    let q = if is_ci() { 50 } else { 200 };

    // Exact match on indexed text column
    let eq_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT * FROM items WHERE tag = 'alpha'");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("Indexed tag='alpha' × {}", q), q, eq_ms);

    // Range query on indexed float column
    let range_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT * FROM items WHERE score > 500 AND score < 600");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("Indexed score range × {}", q), q, range_ms);

    // No-index full scan comparison
    let no_idx_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT * FROM items WHERE qty > 40");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("No-index qty > 40 × {}", q), q, no_idx_ms);

    // Multi-condition with one indexed
    let multi_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT * FROM items WHERE tag = 'beta' AND qty > 10");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("Indexed tag + unindexed qty × {}", q), q, multi_ms);

    let eq_per = eq_ms as f64 * 1000.0 / q as f64;
    let no_idx_per = no_idx_ms as f64 * 1000.0 / q as f64;
    let speedup = if no_idx_per > 0.0 { no_idx_per / eq_per.max(0.01) } else { 0.0 };
    println!("  -> Indexed: {:.1}µs, No-index: {:.1}µs, Speedup: {:.1}x", eq_per, no_idx_per, speedup);
    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 3: Index Performance at Varying Scales
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_index_scaling() {
    let dir = TempDir::new().expect("temp dir");
    let db = Database::create_with_config(dir.path(), edge_config()).expect("create db");
    exec(&db, "CREATE TABLE scale (id INT PRIMARY KEY, category TEXT, val INT)");

    print_separator();

    let sizes: Vec<usize> = if is_ci() {
        vec![1_000, 5_000, 10_000]
    } else {
        vec![5_000, 20_000, 50_000]
    };

    let categories = ["A", "B", "C", "D", "E"];

    for &size in &sizes {
        // Seed
        for i in 1..=size as i64 {
            let cat = categories[(i as usize) % categories.len()];
            exec(&db, &format!("INSERT INTO scale VALUES ({}, '{}', {})", i, cat, i % 100));
        }

        // Create index
        exec(&db, &format!("CREATE INDEX idx_sc_{} ON scale (category) USING COLUMN", size));
        db.flush().expect("flush");
        db.wait_for_indexes_ready();

        // Query
        let q = 50;
        let ms = {
            let start = Instant::now();
            for _ in 0..q {
                exec(&db, "SELECT * FROM scale WHERE category = 'A'");
            }
            start.elapsed().as_millis() as u64
        };

        let per_op = ms as f64 * 1000.0 / q as f64;
        print_result(&format!("Indexed query on {} rows × {}", size, q), q, ms);
        println!("    -> Per query: {:.1}µs at {} rows", per_op, size);

        // Drop for next iteration
        let _ = db.execute(&format!("DROP INDEX idx_sc_{}", size));
        let _ = db.execute("DELETE FROM scale");
    }

    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 4: Vector Index Build Time
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_vector_index_build() {
    let dir = TempDir::new().expect("temp dir");
    let db = Database::create_with_config(dir.path(), edge_config()).expect("create db");
    exec(&db, "CREATE TABLE vecs (id INT PRIMARY KEY, embedding VECTOR(8))");

    let n: usize = if is_ci() { 500 } else { 5_000 };

    // Seed vectors
    let seed_ms = {
        let start = Instant::now();
        for i in 0..n {
            let row = vec![
                Value::Integer(i as i64),
                Value::tensor(Tensor::new(vec![
                    i as f32, (i + 1) as f32, (i + 2) as f32, (i + 3) as f32,
                    (i + 4) as f32, (i + 5) as f32, (i + 6) as f32, (i + 7) as f32,
                ])),
            ];
            db.insert_row("vecs", row).expect("insert vector");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("Seed {} vectors (8-dim)", n), n, seed_ms);

    print_separator();

    // Build vector index
    let build_ms = {
        let start = Instant::now();
        exec(&db, "CREATE VECTOR INDEX idx_vec ON vecs (embedding)");
        db.wait_for_indexes_ready();
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("CREATE VECTOR INDEX on {} vectors", n), 1, build_ms);

    // KNN search
    let q = if is_ci() { 10 } else { 50 };
    let query = vec![5.0_f32, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0];
    let knn_ms = {
        let start = Instant::now();
        for _ in 0..q {
            let _ = db.vector_search("idx_vec", &query, 10);
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("Vector KNN search (k=10) × {}", q), q, knn_ms);

    let knn_per = knn_ms as f64 * 1000.0 / q as f64;
    println!("  -> Index build: {}ms, KNN: {:.1}µs/query", build_ms, knn_per);
    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 5: Text Index Build Time
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_text_index_build() {
    let dir = TempDir::new().expect("temp dir");
    let db = Database::create_with_config(dir.path(), edge_config()).expect("create db");
    exec(&db, "CREATE TABLE docs (id INT PRIMARY KEY, content TEXT)");

    let n: usize = if is_ci() { 500 } else { 5_000 };
    let words = [
        "database management system relational query optimization index",
        "machine learning neural network deep learning artificial intelligence",
        "distributed computing consensus algorithm fault tolerance replication",
        "operating system kernel scheduler memory management virtualization",
        "networking protocol tcp ip routing switching firewall security",
    ];

    // Seed documents
    let seed_ms = {
        let start = Instant::now();
        for i in 0..n {
            let text = words[i % words.len()];
            exec(&db, &format!("INSERT INTO docs VALUES ({}, '{}')", i, text));
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("Seed {} documents", n), n, seed_ms);

    print_separator();

    // Build text index
    let build_ms = {
        let start = Instant::now();
        exec(&db, "CREATE TEXT INDEX idx_text ON docs (content)");
        db.wait_for_indexes_ready();
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("CREATE TEXT INDEX on {} docs", n), 1, build_ms);

    // Text search
    let q = if is_ci() { 10 } else { 50 };
    let search_ms = {
        let start = Instant::now();
        for _ in 0..q {
            let _ = db.text_search_ranked("idx_text", "database", 10);
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("Text search 'database' top-10 × {}", q), q, search_ms);

    let search_per = search_ms as f64 * 1000.0 / q as f64;
    println!("  -> Index build: {}ms, Search: {:.1}µs/query", build_ms, search_per);
    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 6: Multiple Indexes on Same Table
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_multiple_indexes() {
    let dir = TempDir::new().expect("temp dir");
    let db = Database::create_with_config(dir.path(), edge_config()).expect("create db");
    exec(&db, "CREATE TABLE multi (id INT PRIMARY KEY, a TEXT, b FLOAT, c INT, d TEXT)");

    let n: usize = if is_ci() { 3_000 } else { 15_000 };

    for i in 1..=n as i64 {
        exec(&db, &format!(
            "INSERT INTO multi VALUES ({}, 'a_{}', {:.1}, {}, 'd_{}')",
            i, i % 10, 10.0 + (i as f64 % 990.0), i % 50, i % 20
        ));
    }

    print_separator();

    // Create indexes one by one, measuring each
    let idx1_ms = {
        let start = Instant::now();
        exec(&db, "CREATE INDEX idx_a ON multi (a) USING COLUMN");
        db.wait_for_indexes_ready();
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("Index #1 (a TEXT) on {} rows", n), 1, idx1_ms);

    let idx2_ms = {
        let start = Instant::now();
        exec(&db, "CREATE INDEX idx_b ON multi (b) USING COLUMN");
        db.wait_for_indexes_ready();
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("Index #2 (b FLOAT) on {} rows", n), 1, idx2_ms);

    let idx3_ms = {
        let start = Instant::now();
        exec(&db, "CREATE INDEX idx_c ON multi (c) USING COLUMN");
        db.wait_for_indexes_ready();
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("Index #3 (c INT) on {} rows", n), 1, idx3_ms);

    db.flush().expect("flush");
    db.wait_for_indexes_ready();

    // Query with multiple indexes available
    let q = if is_ci() { 50 } else { 200 };

    let q1_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT * FROM multi WHERE a = 'a_5'");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("Query idx_a × {}", q), q, q1_ms);

    let q2_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT * FROM multi WHERE b > 500 AND b < 600");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("Query idx_b range × {}", q), q, q2_ms);

    let q3_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT * FROM multi WHERE c = 25");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("Query idx_c × {}", q), q, q3_ms);

    println!("  -> Total build: {}ms, Per-query avg: {:.1}µs",
        idx1_ms + idx2_ms + idx3_ms,
        (q1_ms + q2_ms + q3_ms) as f64 * 1000.0 / (q * 3) as f64
    );
    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 7: Index Write Amplification
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_index_write_amplification() {
    let dir = TempDir::new().expect("temp dir");
    let db = Database::create_with_config(dir.path(), edge_config()).expect("create db");
    exec(&db, "CREATE TABLE wa (id INT PRIMARY KEY, category TEXT, val INT)");

    let n: usize = if is_ci() { 3_000 } else { 15_000 };

    print_separator();

    // Phase 1: Insert without index
    let no_idx_insert_ms = {
        let start = Instant::now();
        for i in 1..=n as i64 {
            exec(&db, &format!("INSERT INTO wa VALUES ({}, 'cat_{}', {})", i, i % 10, i % 100));
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("INSERT {} rows (no index)", n), n, no_idx_insert_ms);

    // Create index
    exec(&db, "CREATE INDEX idx_wa ON wa (category) USING COLUMN");
    db.wait_for_indexes_ready();

    // Phase 2: Insert with existing index
    let with_idx_insert_ms = {
        let start = Instant::now();
        for i in (n + 1) as i64..=(n * 2) as i64 {
            exec(&db, &format!("INSERT INTO wa VALUES ({}, 'cat_{}', {})", i, i % 10, i % 100));
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("INSERT {} rows (with index)", n), n, with_idx_insert_ms);

    // Phase 3: Update with index
    let upd_count = n / 3;
    let with_idx_update_ms = {
        let start = Instant::now();
        for i in (1..=n as i64).step_by(3) {
            exec(&db, &format!("UPDATE wa SET category = 'updated' WHERE id = {}", i));
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("UPDATE {} rows (with index)", upd_count), upd_count, with_idx_update_ms);

    let no_idx_per = no_idx_insert_ms as f64 * 1000.0 / n as f64;
    let with_idx_per = with_idx_insert_ms as f64 * 1000.0 / n as f64;
    let amplification = if no_idx_per > 0.0 { with_idx_per / no_idx_per } else { 0.0 };
    println!("  -> No index: {:.1}µs/insert, With index: {:.1}µs/insert, Amplification: {:.2}x",
        no_idx_per, with_idx_per, amplification);
    db.close().ok();
}
