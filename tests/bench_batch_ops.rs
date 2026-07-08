//! Batch Operations Benchmark — batch insert at varying sizes, batch_insert_map,
//! batch insert with vectors, batch update patterns, row API vs SQL throughput
//!
//! Run: cargo test --test bench_batch_ops --release -- --nocapture --test-threads=1

use motedb::types::Tensor;
use motedb::{types::Value, DBConfig, Database};
use std::time::Instant;
use tempfile::TempDir;

fn is_ci() -> bool {
    std::env::var("CI").is_ok()
}

fn edge_config() -> DBConfig {
    DBConfig::for_edge()
}

fn exec(db: &Database, sql: &str) -> motedb::sql::QueryResult {
    db.execute(sql)
        .expect("execute SQL")
        .materialize()
        .expect("materialize")
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
// Test 1: Batch Insert at Varying Sizes
// ═══════════════════════════════════════════════════════════════

#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn bench_batch_insert_sizes() {
    let dir = TempDir::new().expect("temp dir");
    let db = Database::create_with_config(dir.path(), edge_config()).expect("create db");
    exec(
        &db,
        "CREATE TABLE batch_sizes (id INT PRIMARY KEY, name TEXT, val INT)",
    );

    print_separator();

    let sizes: Vec<usize> = if is_ci() {
        vec![10, 50, 100, 500]
    } else {
        vec![10, 100, 1000, 5000, 10000]
    };

    let mut next_id = 1i64;

    for &batch_size in &sizes {
        let mut total_ms = 0u64;
        let mut total_rows = 0usize;
        let num_batches = if batch_size >= 5000 { 5 } else { 10 };

        for _ in 0..num_batches {
            let mut batch = Vec::with_capacity(batch_size);
            for _i in 0..batch_size {
                let id = next_id;
                next_id += 1;
                batch.push(vec![
                    Value::Integer(id),
                    Value::text(format!("name_{}", id)),
                    Value::Integer((id % 1000) as i64),
                ]);
            }

            let start = Instant::now();
            let row_ids = db.batch_insert("batch_sizes", batch).expect("batch insert");
            let elapsed = start.elapsed().as_millis() as u64;
            total_ms += elapsed;
            total_rows += row_ids.len();
        }

        let per_batch = total_ms as f64 / num_batches as f64;
        let per_row = total_ms as f64 * 1000.0 / total_rows as f64;
        print_result(
            &format!("batch_insert({}) × {} batches", batch_size, num_batches),
            total_rows,
            total_ms,
        );
        println!(
            "    -> Per batch: {:.1}ms, Per row: {:.1}µs",
            per_batch, per_row
        );
    }

    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 2: batch_insert_map vs batch_insert Performance
// ═══════════════════════════════════════════════════════════════

#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn bench_batch_insert_vs_map() {
    let dir = TempDir::new().expect("temp dir");

    let batch_size = if is_ci() { 200 } else { 1000 };
    let num_batches = if is_ci() { 5 } else { 10 };

    print_separator();

    // batch_insert (Vec<Value>)
    let vec_ms = {
        let db = Database::create_with_config(dir.path(), edge_config()).expect("create db");
        exec(
            &db,
            "CREATE TABLE vec_table (id INT PRIMARY KEY, name TEXT, score FLOAT)",
        );

        let start = Instant::now();
        let mut total_rows = 0;
        for b in 0..num_batches {
            let mut batch = Vec::with_capacity(batch_size);
            for i in 0..batch_size {
                let id = (b * batch_size + i + 1) as i64;
                batch.push(vec![
                    Value::Integer(id),
                    Value::text(format!("user_{}", id)),
                    Value::Float(id as f64 * 1.5),
                ]);
            }
            let ids = db.batch_insert("vec_table", batch).expect("batch insert");
            total_rows += ids.len();
        }
        let elapsed = start.elapsed().as_millis() as u64;
        print_result(
            &format!("batch_insert (Vec) {} rows", total_rows),
            total_rows,
            elapsed,
        );
        db.close().ok();
        elapsed
    };

    // batch_insert_map (HashMap)
    let map_ms = {
        let dir2 = TempDir::new().expect("temp dir");
        let db = Database::create_with_config(dir2.path(), edge_config()).expect("create db");
        exec(
            &db,
            "CREATE TABLE map_table (id INT PRIMARY KEY, name TEXT, score FLOAT)",
        );

        let start = Instant::now();
        let mut total_rows = 0;
        for b in 0..num_batches {
            let mut batch = Vec::with_capacity(batch_size);
            for i in 0..batch_size {
                let id = (b * batch_size + i + 1) as i64;
                let mut map = std::collections::HashMap::new();
                map.insert("id".to_string(), Value::Integer(id));
                map.insert("name".to_string(), Value::text(format!("user_{}", id)));
                map.insert("score".to_string(), Value::Float(id as f64 * 1.5));
                batch.push(map);
            }
            let ids = db
                .batch_insert_map("map_table", batch)
                .expect("batch insert map");
            total_rows += ids.len();
        }
        let elapsed = start.elapsed().as_millis() as u64;
        print_result(
            &format!("batch_insert_map (HashMap) {} rows", total_rows),
            total_rows,
            elapsed,
        );
        db.close().ok();
        elapsed
    };

    let vec_per = vec_ms as f64 * 1000.0 / (batch_size * num_batches) as f64;
    let map_per = map_ms as f64 * 1000.0 / (batch_size * num_batches) as f64;
    println!(
        "  -> Vec: {:.1}µs/row, HashMap: {:.1}µs/row, Ratio: {:.2}x",
        vec_per,
        map_per,
        if vec_per > 0.0 {
            map_per / vec_per
        } else {
            0.0
        }
    );
}

// ═══════════════════════════════════════════════════════════════
// Test 3: Batch Insert with Vectors (Tensors)
// ═══════════════════════════════════════════════════════════════

#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn bench_batch_insert_vectors() {
    let dir = TempDir::new().expect("temp dir");
    let db = Database::create_with_config(dir.path(), edge_config()).expect("create db");
    exec(
        &db,
        "CREATE TABLE vec_batch (id INT PRIMARY KEY, embedding VECTOR(16), metadata TEXT)",
    );

    print_separator();

    let batch_size = if is_ci() { 50 } else { 200 };
    let num_batches = if is_ci() { 5 } else { 20 };
    let dim = 16usize;

    let (total_ms, total_rows) = {
        let start = Instant::now();
        let mut total = 0;
        for b in 0..num_batches {
            let mut batch = Vec::with_capacity(batch_size);
            for i in 0..batch_size {
                let id = (b * batch_size + i + 1) as i64;
                let vec_data: Vec<f32> = (0..dim).map(|d| id as f32 + d as f32 * 0.1).collect();
                batch.push(vec![
                    Value::Integer(id),
                    Value::tensor(Tensor::new(vec_data)),
                    Value::text(format!("meta_{}", id)),
                ]);
            }
            let ids = db
                .batch_insert("vec_batch", batch)
                .expect("batch insert vectors");
            total += ids.len();
        }
        (start.elapsed().as_millis() as u64, total)
    };

    print_result(
        &format!("batch_insert vectors ({}-dim) {} rows", dim, total_rows),
        total_rows,
        total_ms,
    );
    let per_row = total_ms as f64 * 1000.0 / total_rows as f64;
    println!(
        "  -> Per row: {:.1}µs (including {}-dim tensor)",
        per_row, dim
    );

    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 4: Row API vs SQL INSERT Throughput
// ═══════════════════════════════════════════════════════════════

#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn bench_row_api_vs_sql_insert() {
    let dir = TempDir::new().expect("temp dir");

    let n: usize = if is_ci() { 2_000 } else { 10_000 };

    print_separator();

    // SQL INSERT
    let sql_ms = {
        let db = Database::create_with_config(dir.path(), edge_config()).expect("create db");
        exec(
            &db,
            "CREATE TABLE sql_ins (id INT PRIMARY KEY, name TEXT, val INT)",
        );

        let start = Instant::now();
        for i in 1..=n as i64 {
            exec(
                &db,
                &format!(
                    "INSERT INTO sql_ins VALUES ({}, 'name_{}', {})",
                    i,
                    i,
                    i * 10
                ),
            );
        }
        let elapsed = start.elapsed().as_millis() as u64;
        print_result(&format!("SQL INSERT {} rows", n), n, elapsed);
        db.close().ok();
        elapsed
    };

    // Row API insert_row
    let row_api_ms = {
        let dir2 = TempDir::new().expect("temp dir");
        let db = Database::create_with_config(dir2.path(), edge_config()).expect("create db");
        exec(
            &db,
            "CREATE TABLE row_ins (id INT PRIMARY KEY, name TEXT, val INT)",
        );

        let start = Instant::now();
        for i in 1..=n as i64 {
            let row = vec![
                Value::Integer(i),
                Value::text(format!("name_{}", i)),
                Value::Integer(i * 10),
            ];
            db.insert_row("row_ins", row).expect("insert_row");
        }
        let elapsed = start.elapsed().as_millis() as u64;
        print_result(&format!("insert_row {} rows", n), n, elapsed);
        db.close().ok();
        elapsed
    };

    // Batch insert
    let batch_ms = {
        let dir3 = TempDir::new().expect("temp dir");
        let db = Database::create_with_config(dir3.path(), edge_config()).expect("create db");
        exec(
            &db,
            "CREATE TABLE batch_ins (id INT PRIMARY KEY, name TEXT, val INT)",
        );

        let chunk_size = 100;
        let start = Instant::now();
        for chunk_start in (1..=n as i64).step_by(chunk_size) {
            let mut batch = Vec::with_capacity(chunk_size);
            for i in chunk_start..(chunk_start + chunk_size as i64).min(n as i64 + 1) {
                batch.push(vec![
                    Value::Integer(i),
                    Value::text(format!("name_{}", i)),
                    Value::Integer(i * 10),
                ]);
            }
            db.batch_insert("batch_ins", batch).expect("batch insert");
        }
        let elapsed = start.elapsed().as_millis() as u64;
        print_result(&format!("batch_insert(100) {} rows total", n), n, elapsed);
        db.close().ok();
        elapsed
    };

    let sql_per = sql_ms as f64 * 1000.0 / n as f64;
    let row_per = row_api_ms as f64 * 1000.0 / n as f64;
    let batch_per = batch_ms as f64 * 1000.0 / n as f64;
    println!(
        "  -> SQL: {:.1}µs/row, Row API: {:.1}µs/row, Batch: {:.1}µs/row",
        sql_per, row_per, batch_per
    );
    println!(
        "  -> Row API vs SQL: {:.2}x, Batch vs SQL: {:.2}x",
        if sql_per > 0.0 {
            sql_per / row_per
        } else {
            0.0
        },
        if sql_per > 0.0 {
            sql_per / batch_per
        } else {
            0.0
        }
    );
}

// ═══════════════════════════════════════════════════════════════
// Test 5: Batch Read (get_row sequential)
// ═══════════════════════════════════════════════════════════════

#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn batch_read_sequential() {
    let dir = TempDir::new().expect("temp dir");
    let db = Database::create_with_config(dir.path(), edge_config()).expect("create db");
    exec(
        &db,
        "CREATE TABLE seq_read (id INT PRIMARY KEY, name TEXT, score FLOAT)",
    );

    let n: usize = if is_ci() { 2_000 } else { 10_000 };

    // Seed via batch
    let batch_size = 500;
    let mut row_ids = Vec::with_capacity(n);
    for chunk_start in (1..=n as i64).step_by(batch_size) {
        let mut batch = Vec::with_capacity(batch_size);
        for i in chunk_start..(chunk_start + batch_size as i64).min(n as i64 + 1) {
            batch.push(vec![
                Value::Integer(i),
                Value::text(format!("name_{}", i)),
                Value::Float(i as f64 * 1.5),
            ]);
        }
        let ids = db.batch_insert("seq_read", batch).expect("batch insert");
        row_ids.extend(ids);
    }

    print_separator();

    // Sequential get_row
    let q = if is_ci() { 2_000 } else { 10_000 };
    let get_ms = {
        let start = Instant::now();
        for i in 0..q {
            let _ = db
                .get_row("seq_read", row_ids[i % row_ids.len()])
                .expect("get_row");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("get_row sequential × {}", q), q, get_ms);

    // Random get_row
    let rand_ms = {
        let start = Instant::now();
        for i in 0..q {
            // Simple pseudo-random index
            let idx = ((i * 7 + 13) % row_ids.len()) as usize;
            let _ = db.get_row("seq_read", row_ids[idx]).expect("get_row");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("get_row pseudo-random × {}", q), q, rand_ms);

    // get_row_map
    let map_ms = {
        let start = Instant::now();
        for i in 0..q {
            let _ = db
                .get_row_map("seq_read", row_ids[i % row_ids.len()])
                .expect("get_row_map");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("get_row_map × {}", q), q, map_ms);

    let get_per = get_ms as f64 * 1000.0 / q as f64;
    let map_per = map_ms as f64 * 1000.0 / q as f64;
    println!(
        "  -> get_row: {:.1}µs/op, get_row_map: {:.1}µs/op",
        get_per, map_per
    );
    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 6: Batch Update/Delete via Row API
// ═══════════════════════════════════════════════════════════════

#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn bench_batch_update_delete() {
    let dir = TempDir::new().expect("temp dir");
    let db = Database::create_with_config(dir.path(), edge_config()).expect("create db");
    exec(
        &db,
        "CREATE TABLE bud (id INT PRIMARY KEY, name TEXT, val INT)",
    );

    let n: usize = if is_ci() { 1_000 } else { 5_000 };

    // Seed
    let mut row_ids = Vec::with_capacity(n);
    for i in 1..=n as i64 {
        let row = vec![
            Value::Integer(i),
            Value::text(format!("v_{}", i)),
            Value::Integer(i * 10),
        ];
        let rid = db.insert_row("bud", row).expect("insert_row");
        row_ids.push(rid);
    }

    print_separator();

    // Batch update via update_row
    let upd_count = n / 2;
    let upd_ms = {
        let start = Instant::now();
        for i in 0..upd_count {
            let id = (i + 1) as i64;
            let new_row = vec![
                Value::Integer(id),
                Value::text(format!("updated_{}", id)),
                Value::Integer(id * 20),
            ];
            db.update_row("bud", row_ids[i], new_row)
                .expect("update_row");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("update_row × {}", upd_count), upd_count, upd_ms);

    // Batch delete via delete_row
    let del_count = n / 4;
    let del_ms = {
        let start = Instant::now();
        for i in 0..del_count {
            db.delete_row("bud", row_ids[upd_count + i])
                .expect("delete_row");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("delete_row × {}", del_count), del_count, del_ms);

    // Verify remaining count
    let result = exec(&db, "SELECT COUNT(*) FROM bud");
    if let motedb::sql::QueryResult::Select { rows, .. } = result {
        if let Some(Value::Integer(count)) = rows.first().and_then(|r| r.first()) {
            println!("  -> Remaining rows: {}", count);
        }
    }

    let upd_per = upd_ms as f64 * 1000.0 / upd_count as f64;
    let del_per = del_ms as f64 * 1000.0 / del_count as f64;
    println!(
        "  -> update_row: {:.1}µs/op, delete_row: {:.1}µs/op",
        upd_per, del_per
    );
    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 7: Large Text Handling Performance
// ═══════════════════════════════════════════════════════════════

#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn bench_large_text() {
    let dir = TempDir::new().expect("temp dir");
    let db = Database::create_with_config(dir.path(), edge_config()).expect("create db");
    exec(
        &db,
        "CREATE TABLE big_text (id INT PRIMARY KEY, content TEXT)",
    );

    print_separator();

    let text_sizes: Vec<usize> = if is_ci() {
        vec![100, 1000, 10000]
    } else {
        vec![100, 1000, 10000, 100000]
    };

    for &text_size in &text_sizes {
        let text = "x".repeat(text_size);
        let n = if is_ci() { 50 } else { 100 };

        let insert_ms = {
            let start = Instant::now();
            for i in 0..n {
                // Use row API to avoid SQL escaping issues
                let row = vec![Value::Integer(i as i64), Value::text(text.clone())];
                db.insert_row("big_text", row).expect("insert");
            }
            start.elapsed().as_millis() as u64
        };
        print_result(
            &format!("Insert {} rows with {}-byte text", n, text_size),
            n,
            insert_ms,
        );

        // Read back
        let read_ms = {
            let start = Instant::now();
            for i in 0..n {
                let _ = db.get_row("big_text", i as u64);
            }
            start.elapsed().as_millis() as u64
        };
        print_result(
            &format!("Read {} rows with {}-byte text", n, text_size),
            n,
            read_ms,
        );

        let _ = db.execute("DELETE FROM big_text");
        let ins_per = insert_ms as f64 * 1000.0 / n as f64;
        let read_per = read_ms as f64 * 1000.0 / n as f64;
        println!(
            "    -> Insert: {:.1}µs/row, Read: {:.1}µs/row at {} bytes",
            ins_per, read_per, text_size
        );
    }

    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 8: Wide Table (Many Columns) Performance
// ═══════════════════════════════════════════════════════════════

#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn bench_wide_table() {
    let dir = TempDir::new().expect("temp dir");
    let db = Database::create_with_config(dir.path(), edge_config()).expect("create db");

    let col_counts: Vec<usize> = if is_ci() {
        vec![5, 10, 20]
    } else {
        vec![5, 10, 20, 50]
    };

    print_separator();

    for &n_cols in &col_counts {
        // Create table with n_cols + 1 (id) columns
        let table_name = format!("wide_{}", n_cols);
        let mut col_defs = vec!["id INT PRIMARY KEY".to_string()];
        for c in 0..n_cols {
            col_defs.push(format!("c{} INT", c));
        }
        exec(
            &db,
            &format!("CREATE TABLE {} ({})", table_name, col_defs.join(", ")),
        );

        let n_rows: usize = if is_ci() { 500 } else { 2000 };

        // Insert via row API
        let insert_ms = {
            let start = Instant::now();
            for i in 1..=n_rows as i64 {
                let mut row = vec![Value::Integer(i)];
                for c in 0..n_cols {
                    row.push(Value::Integer(i + c as i64));
                }
                db.insert_row(&table_name, row).expect("insert");
            }
            start.elapsed().as_millis() as u64
        };
        print_result(
            &format!("Insert {} rows × {} cols", n_rows, n_cols + 1),
            n_rows,
            insert_ms,
        );

        // SELECT all columns
        let select_ms = {
            let start = Instant::now();
            for _ in 0..50 {
                exec(&db, &format!("SELECT * FROM {} WHERE id = 1", table_name));
            }
            start.elapsed().as_millis() as u64
        };
        print_result(
            &format!("SELECT * ({} cols) × 50", n_cols + 1),
            50,
            select_ms,
        );

        let ins_per = insert_ms as f64 * 1000.0 / n_rows as f64;
        let sel_per = select_ms as f64 * 1000.0 / 50.0;
        println!(
            "    -> Insert: {:.1}µs/row, Select: {:.1}µs/query at {} cols",
            ins_per,
            sel_per,
            n_cols + 1
        );

        let _ = db.execute(&format!("DROP TABLE {}", table_name));
    }

    db.close().ok();
}
