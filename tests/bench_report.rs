//! MoteDB v0.2.0 Benchmark Suite — Technical Report
//!
//! Run: cargo test --test bench_report -- --nocapture --test-threads=1

use motedb::{sql::QueryResult, types::Value, Database};
use std::time::Instant;

fn setup_db(name: &str) -> Database {
    let dir = format!("/tmp/motedb_bench_{}", name);
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(format!("{}.mote", &dir));
    Database::create(&dir).unwrap()
}

fn exec(db: &Database, sql: &str) -> QueryResult {
    db.execute(sql).unwrap().materialize().unwrap()
}

fn format_duration(us: u128) -> String {
    if us < 1000 {
        format!("{}µs", us)
    } else if us < 1_000_000 {
        format!("{:.1}ms", us as f64 / 1000.0)
    } else {
        format!("{:.2}s", us as f64 / 1_000_000.0)
    }
}

fn format_throughput(count: usize, us: u128) -> String {
    if us == 0 {
        return "N/A".to_string();
    }
    let ops_per_sec = count as f64 / (us as f64 / 1_000_000.0);
    if ops_per_sec >= 1_000_000.0 {
        format!("{:.1}M ops/s", ops_per_sec / 1_000_000.0)
    } else if ops_per_sec >= 1000.0 {
        format!("{:.1}K ops/s", ops_per_sec / 1000.0)
    } else {
        format!("{:.0} ops/s", ops_per_sec)
    }
}

fn format_latency(us: u128) -> String {
    if us < 1000 {
        format!("{}µs", us)
    } else {
        format!("{:.1}µs", us as f64)
    }
}

fn count_rows(result: &QueryResult) -> usize {
    match result {
        QueryResult::Select { rows, .. } => rows.len(),
        _ => 0,
    }
}

#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn bench_report_v020() {
    let n: usize = if std::env::var("CI").is_ok() {
        5_000
    } else {
        10_000
    };

    println!("\n╔══════════════════════════════════════════════════════════════════╗");
    println!("║           MoteDB v0.2.0  Performance Benchmark Report           ║");
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!(
        "║  Dataset: {} rows                                               ║",
        n
    );
    println!("║  Engine:  Rust · LSM-Tree · B+Tree · mmap                       ║");
    println!("╚══════════════════════════════════════════════════════════════════╝\n");

    // ─── 1. INSERT THROUGHPUT ───
    {
        let db = setup_db("insert");
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT, score FLOAT)")
            .unwrap();
        db.execute("CREATE INDEX idx_age ON t (age)").unwrap();

        let t = Instant::now();
        for i in 0..n {
            db.execute_prepared(
                "INSERT INTO t (id, name, age, score) VALUES (?, ?, ?, ?)",
                vec![
                    Value::Integer(i as i64),
                    Value::text(format!("user_{}", i)),
                    Value::Integer(20 + (i as i64 % 50)),
                    Value::Float(50.0 + (i as f64 % 100.0)),
                ],
            )
            .unwrap();
        }
        let elapsed = t.elapsed().as_micros();
        println!("┌─────────────────────────────────────────────────────────────────┐");
        println!("│  1. INSERT THROUGHPUT (with column index build)                 │");
        println!("├─────────────────────┬───────────────────────────────────────────┤");
        println!(
            "│  Rows inserted      │  {:>12}                            │",
            n
        );
        println!(
            "│  Total time         │  {:>12}                           │",
            format_duration(elapsed)
        );
        println!(
            "│  Throughput         │  {:>12}                         │",
            format_throughput(n, elapsed)
        );
        println!(
            "│  Avg latency        │  {:>12}                           │",
            format_latency(elapsed / n as u128)
        );
        println!("└─────────────────────┴───────────────────────────────────────────┘\n");
        db.close().unwrap();
    }

    // ─── 2. POINT QUERY (PK) ───
    {
        let db = setup_db("pk_query");
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT)")
            .unwrap();
        for i in 0..n {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'u{}', 25)", i, i))
                .unwrap();
        }

        let queries: usize = 2_000.min(n);
        let t = Instant::now();
        for i in 0..queries {
            let _ = exec(&db, &format!("SELECT * FROM t WHERE id = {}", i * 3 % n));
        }
        let elapsed = t.elapsed().as_micros();
        println!("┌─────────────────────────────────────────────────────────────────┐");
        println!("│  2. POINT QUERY — PRIMARY KEY (id = ?)                         │");
        println!("├─────────────────────┬───────────────────────────────────────────┤");
        println!(
            "│  Queries            │  {:>12}                            │",
            queries
        );
        println!(
            "│  Total time         │  {:>12}                           │",
            format_duration(elapsed)
        );
        println!(
            "│  QPS                │  {:>12}                         │",
            format_throughput(queries, elapsed)
        );
        println!(
            "│  Avg latency        │  {:>12}                           │",
            format_latency(elapsed / queries as u128)
        );
        println!("└─────────────────────┴───────────────────────────────────────────┘\n");
        db.close().unwrap();
    }

    // ─── 3. COLUMN INDEX QUERY ───
    {
        let db = setup_db("col_query");
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT)")
            .unwrap();
        db.execute("CREATE INDEX idx_age ON t (age)").unwrap();
        for i in 0..n {
            db.execute(&format!(
                "INSERT INTO t VALUES ({}, 'u{}', {})",
                i,
                i,
                20 + (i as i64 % 50)
            ))
            .unwrap();
        }
        db.wait_for_indexes_ready();

        let queries: usize = 2_000.min(n);
        let t = Instant::now();
        for i in 0..queries {
            let _ = exec(
                &db,
                &format!("SELECT * FROM t WHERE age = {}", 30 + (i as i64 % 20)),
            );
        }
        let elapsed = t.elapsed().as_micros();
        println!("┌─────────────────────────────────────────────────────────────────┐");
        println!("│  3. COLUMN INDEX QUERY — WHERE age = ?                         │");
        println!("├─────────────────────┬───────────────────────────────────────────┤");
        println!(
            "│  Queries            │  {:>12}                            │",
            queries
        );
        println!(
            "│  Total time         │  {:>12}                           │",
            format_duration(elapsed)
        );
        println!(
            "│  QPS                │  {:>12}                         │",
            format_throughput(queries, elapsed)
        );
        println!(
            "│  Avg latency        │  {:>12}                           │",
            format_latency(elapsed / queries as u128)
        );
        println!("└─────────────────────┴───────────────────────────────────────────┘\n");
        db.close().unwrap();
    }

    // ─── 4. COLUMN INDEX RANGE QUERY ───
    {
        let db = setup_db("range_query");
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
            .unwrap();
        db.execute("CREATE INDEX idx_val ON t (val)").unwrap();
        for i in 0..n {
            db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i % 100))
                .unwrap();
        }
        db.wait_for_indexes_ready(); // Ensure column index is fully built before range queries

        let queries: usize = 1_000.min(n);
        let t = Instant::now();
        for i in 0..queries {
            let v = i as i64 % 40;
            let _ = exec(
                &db,
                &format!("SELECT * FROM t WHERE val >= {} AND val <= {}", v, v + 10),
            );
        }
        let elapsed = t.elapsed().as_micros();
        println!("┌─────────────────────────────────────────────────────────────────┐");
        println!("│  4. RANGE QUERY — WHERE val >= ? AND val <= ?                  │");
        println!("├─────────────────────┬───────────────────────────────────────────┤");
        println!(
            "│  Queries            │  {:>12}                            │",
            queries
        );
        println!(
            "│  Total time         │  {:>12}                           │",
            format_duration(elapsed)
        );
        println!(
            "│  QPS                │  {:>12}                         │",
            format_throughput(queries, elapsed)
        );
        println!(
            "│  Avg latency        │  {:>12}                           │",
            format_latency(elapsed / queries as u128)
        );
        println!("└─────────────────────┴───────────────────────────────────────────┘\n");
        db.close().unwrap();
    }

    // ─── 5. FULL TABLE SCAN ───
    {
        let db = setup_db("full_scan");
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, data TEXT)")
            .unwrap();
        for i in 0..n {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'data_{}')", i, i))
                .unwrap();
        }

        let t = Instant::now();
        let result = exec(&db, "SELECT * FROM t");
        let elapsed = t.elapsed().as_micros();
        let rows = count_rows(&result);
        println!("┌─────────────────────────────────────────────────────────────────┐");
        println!("│  5. FULL TABLE SCAN — SELECT * FROM t                          │");
        println!("├─────────────────────┬───────────────────────────────────────────┤");
        println!(
            "│  Rows scanned       │  {:>12}                            │",
            rows
        );
        println!(
            "│  Total time         │  {:>12}                           │",
            format_duration(elapsed)
        );
        println!(
            "│  Throughput         │  {:>12} rows/s                    │",
            format_throughput(rows, elapsed)
        );
        println!("└─────────────────────┴───────────────────────────────────────────┘\n");
        db.close().unwrap();
    }

    // ─── 6. MIXED CRUD ───
    {
        let db = setup_db("crud");
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
            .unwrap();
        db.execute("CREATE INDEX idx_val ON t (val)").unwrap();
        for i in 0..n {
            db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 10))
                .unwrap();
        }

        let ops: usize = 1_000.min(n);
        let t = Instant::now();
        for i in 0..ops {
            match i % 10 {
                0..=4 => {
                    let _ = exec(&db, &format!("SELECT * FROM t WHERE id = {}", i));
                }
                5..=6 => {
                    let _ = exec(
                        &db,
                        &format!("UPDATE t SET val = {} WHERE id = {}", i * 100, i),
                    );
                }
                7..=8 => {
                    let _ = exec(&db, &format!("SELECT * FROM t WHERE val = {}", i * 100));
                }
                9 => {
                    let rid = n + i;
                    let _ = exec(&db, &format!("INSERT INTO t VALUES ({}, {})", rid, rid));
                    let _ = exec(&db, &format!("DELETE FROM t WHERE id = {}", rid));
                }
                _ => {}
            }
        }
        let elapsed = t.elapsed().as_micros();
        println!("┌─────────────────────────────────────────────────────────────────┐");
        println!("│  6. MIXED CRUD (50% SELECT, 20% UPDATE, 20% IDX, 10% INS/DEL) │");
        println!("├─────────────────────┬───────────────────────────────────────────┤");
        println!(
            "│  Operations         │  {:>12}                            │",
            ops
        );
        println!(
            "│  Total time         │  {:>12}                           │",
            format_duration(elapsed)
        );
        println!(
            "│  Throughput         │  {:>12} ops/s                    │",
            format_throughput(ops, elapsed)
        );
        println!("└─────────────────────┴───────────────────────────────────────────┘\n");
        db.close().unwrap();
    }

    // ─── 7. VECTOR SEARCH ───
    {
        let db = setup_db("vector");
        let dim: usize = 64;
        let vec_n: usize = n / 5;
        db.execute(&format!("CREATE TABLE items (id INT, emb VECTOR({}))", dim))
            .unwrap();

        for i in 0..vec_n {
            let v: Vec<String> = (0..dim)
                .map(|j| format!("{:.4}", ((i * dim + j) as f32 * 0.01).sin()))
                .collect();
            db.execute(&format!(
                "INSERT INTO items (id, emb) VALUES ({}, [{}])",
                i,
                v.join(", ")
            ))
            .unwrap();
        }

        let queries: usize = 100.min(vec_n);
        let t = Instant::now();
        for i in 0..queries {
            let offset = (i * 7) % vec_n;
            let v: Vec<String> = (0..dim)
                .map(|j| format!("{:.4}", ((offset * dim + j) as f32 * 0.01).sin()))
                .collect();
            let _ = exec(
                &db,
                &format!(
                    "SELECT id FROM items ORDER BY emb <-> [{}] LIMIT 10",
                    v.join(", ")
                ),
            );
        }
        let elapsed = t.elapsed().as_micros();
        println!("┌─────────────────────────────────────────────────────────────────┐");
        println!(
            "│  7. VECTOR SEARCH — ORDER BY emb <-> [q] LIMIT 10 (dim={})    │",
            dim
        );
        println!("├─────────────────────┬───────────────────────────────────────────┤");
        println!(
            "│  Vectors indexed    │  {:>12}                            │",
            vec_n
        );
        println!(
            "│  Queries            │  {:>12}                            │",
            queries
        );
        println!(
            "│  Total time         │  {:>12}                           │",
            format_duration(elapsed)
        );
        println!(
            "│  QPS                │  {:>12}                         │",
            format_throughput(queries, elapsed)
        );
        println!(
            "│  Avg latency        │  {:>12}                           │",
            format_latency(elapsed / queries.max(1) as u128)
        );
        println!("└─────────────────────┴───────────────────────────────────────────┘\n");
        db.close().unwrap();
    }

    // ─── 8. FULL-TEXT SEARCH ───
    {
        let db = setup_db("fts");
        db.execute("CREATE TABLE docs (id INT, content TEXT)")
            .unwrap();

        let fts_n: usize = n / 2;
        let words = [
            "database",
            "vector",
            "search",
            "index",
            "query",
            "performance",
            "rust",
            "embedded",
            "columnar",
            "spatial",
        ];
        for i in 0..fts_n {
            let w1 = words[i % words.len()];
            let w2 = words[(i + 3) % words.len()];
            let content = format!(
                "This is document {} about {} and {} technology systems.",
                i, w1, w2
            );
            db.execute(&format!(
                "INSERT INTO docs (id, content) VALUES ({}, '{}')",
                i, content
            ))
            .unwrap();
        }
        db.execute("CREATE TEXT INDEX idx_content ON docs(content)")
            .unwrap();

        let queries: usize = 500.min(fts_n);
        let t = Instant::now();
        for i in 0..queries {
            let term = words[i % words.len()];
            let _ = exec(
                &db,
                &format!(
                    "SELECT id FROM docs WHERE MATCH(content, '{}') ORDER BY id",
                    term
                ),
            );
        }
        let elapsed = t.elapsed().as_micros();
        println!("┌─────────────────────────────────────────────────────────────────┐");
        println!("│  8. FULL-TEXT SEARCH — MATCH(content, 'term')                  │");
        println!("├─────────────────────┬───────────────────────────────────────────┤");
        println!(
            "│  Documents          │  {:>12}                            │",
            fts_n
        );
        println!(
            "│  Queries            │  {:>12}                            │",
            queries
        );
        println!(
            "│  Total time         │  {:>12}                           │",
            format_duration(elapsed)
        );
        println!(
            "│  QPS                │  {:>12}                         │",
            format_throughput(queries, elapsed)
        );
        println!(
            "│  Avg latency        │  {:>12}                           │",
            format_latency(elapsed / queries as u128)
        );
        println!("└─────────────────────┴───────────────────────────────────────────┘\n");
        db.close().unwrap();
    }

    // ─── 9. SPATIAL QUERY ───
    {
        let db = setup_db("spatial");
        db.execute("CREATE TABLE points (id INT, location GEOMETRY)")
            .unwrap();

        let sp_n: usize = n / 2;
        for i in 0..sp_n {
            let x = (i as f64 * 0.001) % 100.0;
            let y = (i as f64 * 0.0017) % 100.0;
            db.execute(&format!(
                "INSERT INTO points (id, location) VALUES ({}, POINT({:.4}, {:.4}))",
                i, x, y
            ))
            .unwrap();
        }

        let queries: usize = 500.min(sp_n);
        let t = Instant::now();
        for i in 0..queries {
            let cx = (i as f64 * 0.1) % 100.0;
            let cy = 50.0;
            let _ = exec(&db, &format!(
                "SELECT id FROM points WHERE WITHIN_RADIUS(location, POINT({:.4}, {:.4}), 10.0) ORDER BY id",
                cx, cy
            ));
        }
        let elapsed = t.elapsed().as_micros();
        println!("┌─────────────────────────────────────────────────────────────────┐");
        println!("│  9. SPATIAL QUERY — WITHIN_RADIUS(loc, pt, r=10)               │");
        println!("├─────────────────────┬───────────────────────────────────────────┤");
        println!(
            "│  Points             │  {:>12}                            │",
            sp_n
        );
        println!(
            "│  Queries            │  {:>12}                            │",
            queries
        );
        println!(
            "│  Total time         │  {:>12}                           │",
            format_duration(elapsed)
        );
        println!(
            "│  QPS                │  {:>12}                         │",
            format_throughput(queries, elapsed)
        );
        println!(
            "│  Avg latency        │  {:>12}                           │",
            format_latency(elapsed / queries as u128)
        );
        println!("└─────────────────────┴───────────────────────────────────────────┘\n");
        db.close().unwrap();
    }

    // ─── 10. WAL RECOVERY ───
    {
        let dir = "/tmp/motedb_bench_recovery";
        let _ = std::fs::remove_dir_all(dir);
        let _ = std::fs::remove_dir_all(format!("{}.mote", dir));
        {
            let db = Database::create(dir).unwrap();
            db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
                .unwrap();
            for i in 0..n {
                db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i))
                    .unwrap();
            }
            db.flush().unwrap();
            db.close().unwrap();
        }

        let t = Instant::now();
        let db = Database::open(dir).unwrap();
        let elapsed = t.elapsed().as_micros();
        let result = exec(&db, "SELECT COUNT(*) FROM t");
        let count = count_rows(&result);
        println!("┌─────────────────────────────────────────────────────────────────┐");
        println!(
            "│  10. WAL RECOVERY — REOPEN AFTER {} ROW INSERT             │",
            n
        );
        println!("├─────────────────────┬───────────────────────────────────────────┤");
        println!(
            "│  Rows verified      │  {:>12}                            │",
            count
        );
        println!(
            "│  Recovery time      │  {:>12}                           │",
            format_duration(elapsed)
        );
        println!(
            "│  Throughput         │  {:>12} rows/s                    │",
            format_throughput(n, elapsed)
        );
        println!("└─────────────────────┴───────────────────────────────────────────┘\n");
        db.close().unwrap();
    }

    // ─── 11. PREPARED STATEMENT vs RAW SQL ───
    {
        let db = setup_db("prepared");
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
            .unwrap();
        for i in 0..n {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'val_{}')", i, i))
                .unwrap();
        }

        let queries: usize = 2_000.min(n);

        // Raw SQL (with format!)
        let t = Instant::now();
        for i in 0..queries {
            let _ = exec(&db, &format!("SELECT * FROM t WHERE id = {}", i % n));
        }
        let elapsed_raw = t.elapsed().as_micros();

        // Prepared statement (cache hit after warm-up)
        let sql = "SELECT * FROM t WHERE id = ?";
        for i in 0..100 {
            let _ = db
                .execute_prepared(sql, vec![Value::Integer(i as i64)])
                .unwrap()
                .materialize()
                .unwrap();
        }

        let t = Instant::now();
        for i in 0..queries {
            let _ = db
                .execute_prepared(sql, vec![Value::Integer((i % n) as i64)])
                .unwrap()
                .materialize()
                .unwrap();
        }
        let elapsed_prep = t.elapsed().as_micros();
        let speedup = elapsed_raw as f64 / elapsed_prep.max(1) as f64;
        println!("┌─────────────────────────────────────────────────────────────────┐");
        println!("│  11. PREPARED STATEMENT vs RAW SQL                              │");
        println!("├─────────────────────┬───────────────────────────────────────────┤");
        println!(
            "│  Queries            │  {:>12}                            │",
            queries
        );
        println!(
            "│  Raw SQL            │  {:>12}  ({:>10})           │",
            format_duration(elapsed_raw),
            format_throughput(queries, elapsed_raw)
        );
        println!(
            "│  Prepared stmt      │  {:>12}  ({:>10})           │",
            format_duration(elapsed_prep),
            format_throughput(queries, elapsed_prep)
        );
        println!(
            "│  Speedup            │  {:>12.1}x                            │",
            speedup
        );
        println!("└─────────────────────┴───────────────────────────────────────────┘\n");
        db.close().unwrap();
    }

    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║                    Benchmark Complete                            ║");
    println!("╚══════════════════════════════════════════════════════════════════╝\n");
}
