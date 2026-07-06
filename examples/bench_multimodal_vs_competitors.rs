//! 多模态读写性能基准测试
//!
//! 对比 MoteDB 多模态能力与：
//! - 纯标量（SQLite）读写性能
//! - 暴力向量搜索（ground truth KNN）
//! - SQLite FTS5 全文搜索
//!
//! 运行：cargo run --release --example bench_multimodal_vs_competitors

use motedb::{DBConfig, Database};
use rusqlite::{params, Connection};
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn get_rss_mb() -> f64 {
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
        .map(|kb| kb as f64 / 1024.0)
        .unwrap_or(0.0)
}

fn measure_us<F: FnMut()>(mut f: F, iterations: usize) -> (u64, u64, u64) {
    let mut times = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let t = Instant::now();
        f();
        times.push(t.elapsed().as_micros() as u64);
    }
    times.sort();
    let p50 = times[times.len() / 2];
    let p99 = times[(times.len() as f64 * 0.99) as usize];
    let avg = times.iter().sum::<u64>() / times.len() as u64;
    (p50, p99, avg)
}

// ─── Vector Search: MoteDB vs Brute Force ───────────────────────────────

fn generate_vectors(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut rng = seed;
    (0..n)
        .map(|_| {
            (0..dim)
                .map(|_| {
                    rng = rng
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    ((rng >> 33) as f64 / (1u64 << 31) as f64 - 1.0) as f32
                })
                .collect()
        })
        .collect()
}

fn brute_force_knn(vectors: &[Vec<f32>], query: &[f32], k: usize) -> Vec<(usize, f32)> {
    let mut dists: Vec<(usize, f32)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let d: f32 = v
                .iter()
                .zip(query.iter())
                .map(|(a, b)| (a - b).powi(2))
                .sum::<f32>()
                .sqrt();
            (i, d)
        })
        .collect();
    dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    dists.into_iter().take(k).collect()
}

fn bench_vector_search() {
    println!("\n  ═══ 向量搜索：MoteDB DiskANN vs 暴力搜索 ═══");
    println!("  {}", "─".repeat(70));

    let n = 10_000;
    let dim = 128;
    let k = 10;
    let vectors = generate_vectors(n, dim, 42);
    let query = &vectors[0]; // search for similar to first vector

    // ── MoteDB Vector KNN ──
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute("CREATE TABLE vecs (id INT PRIMARY KEY AUTO_INCREMENT, cat TEXT, emb VECTOR(128))")
        .unwrap();

    // INSERT vectors
    let t = Instant::now();
    for chunk in vectors.chunks(1000) {
        let mut sql = String::from("INSERT INTO vecs (cat, emb) VALUES ");
        for (i, v) in chunk.iter().enumerate() {
            if i > 0 {
                sql.push(',');
            }
            let vstr: Vec<String> = v.iter().map(|f| format!("{}", f)).collect();
            sql.push_str(&format!("('cat_{}', [{}])", i % 10, vstr.join(",")));
        }
        db.execute(&sql).unwrap();
    }
    let mote_insert_ms = t.elapsed().as_millis();
    println!(
        "  INSERT {} vectors ({}-dim): {} ms ({:.0} vecs/s)",
        n,
        dim,
        mote_insert_ms,
        n as f64 / mote_insert_ms as f64 * 1000.0
    );

    // CREATE VECTOR INDEX
    let t = Instant::now();
    db.execute("CREATE VECTOR INDEX idx_emb ON vecs (emb)")
        .unwrap();
    let mote_index_ms = t.elapsed().as_millis();
    println!("  CREATE VECTOR INDEX: {} ms", mote_index_ms);

    // Warm up
    let qstr = query
        .iter()
        .map(|f| format!("{}", f))
        .collect::<Vec<_>>()
        .join(",");
    let _ = db.execute(&format!(
        "SELECT id FROM vecs ORDER BY emb <-> [{}] LIMIT {}",
        qstr, k
    ));

    // Measure MoteDB KNN
    let (mote_p50, mote_p99, mote_avg) = measure_us(
        || {
            let _ = db.execute(&format!(
                "SELECT id FROM vecs ORDER BY emb <-> [{}] LIMIT {}",
                qstr, k
            ));
        },
        20,
    );

    println!(
        "  KNN search (k={}): P50={}μs P99={}μs avg={}μs",
        k, mote_p50, mote_p99, mote_avg
    );

    // ── Brute Force KNN ──
    let (bf_p50, bf_p99, bf_avg) = measure_us(
        || {
            let _ = brute_force_knn(&vectors, query, k);
        },
        20,
    );

    println!(
        "  暴力搜索 (k={}): P50={}μs P99={}μs avg={}μs",
        k, bf_p50, bf_p99, bf_avg
    );
    println!("  加速比: {:.1}x faster", bf_avg as f64 / mote_avg as f64);

    // Memory
    let mote_rss = get_rss_mb();
    println!("  MoteDB RSS: {:.1} MB", mote_rss);

    println!("  {}", "─".repeat(70));
}

// ─── Text Search: MoteDB FTS vs SQLite FTS5 ─────────────────────────────

fn generate_docs(n: usize) -> Vec<(String, String)> {
    let words = [
        "database",
        "machine",
        "learning",
        "robot",
        "sensor",
        "edge",
        "compute",
        "ai",
        "embedded",
        "realtime",
        "spatial",
        "vector",
        "search",
        "index",
        "query",
        "performance",
        "memory",
        "storage",
        "crash",
        "recovery",
    ];
    (0..n)
        .map(|i| {
            let title = format!("doc_{}_{}", i, words[i % words.len()]);
            let body = (0..20)
                .map(|j| words[(i + j) % words.len()])
                .collect::<Vec<_>>()
                .join(" ");
            (title, body)
        })
        .collect()
}

fn bench_text_search() {
    println!("\n  ═══ 全文搜索：MoteDB FTS vs SQLite FTS5 ═══");
    println!("  {}", "─".repeat(70));

    let n = 10_000;
    let docs = generate_docs(n);
    let query = "database";

    // ── MoteDB FTS ──
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute(
        "CREATE TABLE docs (id INT PRIMARY KEY AUTO_INCREMENT, title TEXT, body TEXT, cat TEXT)",
    )
    .unwrap();

    // INSERT
    let t = Instant::now();
    for chunk in docs.chunks(1000) {
        let mut sql = String::from("INSERT INTO docs (title, body, cat) VALUES ");
        for (i, (title, body)) in chunk.iter().enumerate() {
            if i > 0 {
                sql.push(',');
            }
            sql.push_str(&format!(
                "('{}', '{}', 'cat_{}')",
                title.replace('\'', "''"),
                body.replace('\'', "''"),
                i % 5
            ));
        }
        db.execute(&sql).unwrap();
    }
    let mote_insert_ms = t.elapsed().as_millis();
    println!(
        "  MoteDB INSERT {} docs: {} ms ({:.0} docs/s)",
        n,
        mote_insert_ms,
        n as f64 / mote_insert_ms as f64 * 1000.0
    );

    // CREATE TEXT INDEX
    let t = Instant::now();
    db.execute("CREATE TEXT INDEX idx_body ON docs (body)")
        .unwrap();
    let mote_index_ms = t.elapsed().as_millis();
    println!("  MoteDB CREATE TEXT INDEX: {} ms", mote_index_ms);

    // Warm up
    let _ = db.execute(&format!(
        "SELECT id FROM docs WHERE MATCH(body, '{}') LIMIT 10",
        query
    ));

    // Measure MoteDB FTS
    let (mote_p50, mote_p99, mote_avg) = measure_us(
        || {
            let _ = db.execute(&format!(
                "SELECT id FROM docs WHERE MATCH(body, '{}') LIMIT 10",
                query
            ));
        },
        50,
    );

    println!(
        "  MoteDB FTS top-10: P50={}μs P99={}μs avg={}μs",
        mote_p50, mote_p99, mote_avg
    );

    // ── SQLite FTS5 ──
    let sqlite_dir = TempDir::new().unwrap();
    let conn = Connection::open(sqlite_dir.path().join("test.db")).unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-32000;",
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, body TEXT, cat TEXT)",
        [],
    )
    .unwrap();
    conn.execute_batch(
        "CREATE VIRTUAL TABLE docs_fts USING fts5(body, content='docs', content_rowid='id');",
    )
    .unwrap();

    // INSERT
    let t = Instant::now();
    conn.execute_batch("BEGIN TRANSACTION").unwrap();
    {
        let mut stmt = conn
            .prepare("INSERT INTO docs (title, body, cat) VALUES (?, ?, ?)")
            .unwrap();
        for (title, body) in &docs {
            let cat = format!("cat_{}", title.len() % 5);
            stmt.execute(params![title, body, cat]).unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    // Build FTS index
    conn.execute_batch("INSERT INTO docs_fts(docs_fts) VALUES('rebuild');")
        .unwrap();
    let sqlite_insert_ms = t.elapsed().as_millis();
    println!(
        "  SQLite  INSERT {} docs + FTS5: {} ms ({:.0} docs/s)",
        n,
        sqlite_insert_ms,
        n as f64 / sqlite_insert_ms as f64 * 1000.0
    );

    // Warm up
    let _ = conn.execute(
        "SELECT id FROM docs_fts WHERE docs_fts MATCH ? LIMIT 10",
        params![query],
    );

    // Measure SQLite FTS5
    let (sql_p50, sql_p99, sql_avg) = measure_us(
        || {
            let _ = conn.execute(
                "SELECT id FROM docs_fts WHERE docs_fts MATCH ? LIMIT 10",
                params![query],
            );
        },
        50,
    );

    println!(
        "  SQLite  FTS5 top-10: P50={}μs P99={}μs avg={}μs",
        sql_p50, sql_p99, sql_avg
    );

    if mote_avg > 0 && sql_avg > 0 {
        println!(
            "  搜索加速比: {:.1}x ({})",
            mote_avg as f64 / sql_avg as f64,
            if mote_avg < sql_avg {
                "MoteDB 更快"
            } else {
                "SQLite 更快"
            }
        );
    }

    // LIKE prefix comparison
    let (mote_like_p50, _, mote_like_avg) = measure_us(
        || {
            let _ = db.execute("SELECT id FROM docs WHERE title LIKE 'doc_1%' LIMIT 10");
        },
        50,
    );
    let (sql_like_p50, _, sql_like_avg) = measure_us(
        || {
            let _ = conn.execute("SELECT id FROM docs WHERE title LIKE 'doc_1%' LIMIT 10", []);
        },
        50,
    );
    println!(
        "  LIKE prefix P50: MoteDB={}μs SQLite={}μs",
        mote_like_p50, sql_like_p50
    );

    println!("  {}", "─".repeat(70));
}

// ─── Spatial Search: MoteDB vs SQLite RTree ─────────────────────────────

fn bench_spatial_search() {
    println!("\n  ═══ 空间查询：MoteDB i-Octree vs SQLite RTree ═══");
    println!("  {}", "─".repeat(70));

    let n = 10_000;

    // ── MoteDB Spatial ──
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute("CREATE TABLE points (id INT PRIMARY KEY AUTO_INCREMENT, loc GEOMETRY, val FLOAT, region TEXT)").unwrap();

    // INSERT
    let t = Instant::now();
    let mut sql = String::from("INSERT INTO points (loc, val, region) VALUES ");
    for i in 0..n {
        if i > 0 {
            sql.push(',');
        }
        let x = (i as f64 * 0.37) % 1000.0;
        let y = (i as f64 * 0.59) % 1000.0;
        sql.push_str(&format!(
            "(POINT({:.1},{:.1}), {:.1}, '{}')",
            x,
            y,
            i as f64,
            if i % 3 == 0 { "US" } else { "EU" }
        ));
    }
    db.execute(&sql).unwrap();
    let mote_insert_ms = t.elapsed().as_millis();
    println!(
        "  MoteDB INSERT {} points: {} ms ({:.0} pts/s)",
        n,
        mote_insert_ms,
        n as f64 / mote_insert_ms as f64 * 1000.0
    );

    // CREATE SPATIAL INDEX
    let t = Instant::now();
    db.execute("CREATE SPATIAL INDEX idx_loc ON points (loc)")
        .unwrap();
    let mote_index_ms = t.elapsed().as_millis();
    println!("  MoteDB CREATE SPATIAL INDEX: {} ms", mote_index_ms);

    // Warm up + measure KNN
    let _ = db.execute("SELECT id FROM points ORDER BY ST_DISTANCE(loc, 500.0, 500.0) LIMIT 10");
    let (mote_p50, mote_p99, mote_avg) = measure_us(
        || {
            let _ = db
                .execute("SELECT id FROM points ORDER BY ST_DISTANCE(loc, 500.0, 500.0) LIMIT 10");
        },
        50,
    );

    println!(
        "  MoteDB spatial KNN top-10: P50={}μs P99={}μs avg={}μs",
        mote_p50, mote_p99, mote_avg
    );

    // ── SQLite (brute force — no RTree extension available) ──
    let sqlite_dir = TempDir::new().unwrap();
    let conn = Connection::open(sqlite_dir.path().join("test.db")).unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-32000;",
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE points (id INTEGER PRIMARY KEY, x REAL, y REAL, val REAL, region TEXT)",
        [],
    )
    .unwrap();
    conn.execute("CREATE INDEX idx_xy ON points(x, y)", [])
        .unwrap();

    // INSERT
    let t = Instant::now();
    conn.execute_batch("BEGIN TRANSACTION").unwrap();
    {
        let mut stmt = conn
            .prepare("INSERT INTO points (x, y, val, region) VALUES (?, ?, ?, ?)")
            .unwrap();
        for i in 0..n {
            let x = (i as f64 * 0.37) % 1000.0;
            let y = (i as f64 * 0.59) % 1000.0;
            stmt.execute(params![
                x,
                y,
                i as f64,
                if i % 3 == 0 { "US" } else { "EU" }
            ])
            .unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    let sqlite_insert_ms = t.elapsed().as_millis();
    println!(
        "  SQLite  INSERT {} points + INDEX: {} ms ({:.0} pts/s)",
        n,
        sqlite_insert_ms,
        n as f64 / sqlite_insert_ms as f64 * 1000.0
    );

    // Brute force spatial KNN
    let _ = conn.execute(
        "SELECT id FROM points ORDER BY ((x-500.0)*(x-500.0) + (y-500.0)*(y-500.0)) LIMIT 10",
        [],
    );
    let (sql_p50, sql_p99, sql_avg) = measure_us(
        || {
            let _ = conn.execute("SELECT id FROM points ORDER BY ((x-500.0)*(x-500.0) + (y-500.0)*(y-500.0)) LIMIT 10", []);
        },
        50,
    );

    println!(
        "  SQLite  spatial KNN top-10: P50={}μs P99={}μs avg={}μs",
        sql_p50, sql_p99, sql_avg
    );
    println!("  加速比: {:.1}x faster", sql_avg as f64 / mote_avg as f64);

    println!("  {}", "─".repeat(70));
}

// ─── Mixed Multimodal Write Performance ─────────────────────────────────

fn bench_multimodal_write() {
    println!("\n  ═══ 多模态写入：INSERT + CREATE INDEX ═══");
    println!("  {}", "─".repeat(70));

    let n = 10_000;
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute("CREATE TABLE items (id INT PRIMARY KEY AUTO_INCREMENT, emb VECTOR(64), loc GEOMETRY, body TEXT, price FLOAT, region TEXT)").unwrap();

    // Generate multimodal data
    let vectors = generate_vectors(n, 64, 99);
    let words = [
        "database", "robot", "sensor", "edge", "ai", "embedded", "realtime", "query",
    ];

    // INSERT one by one (realistic edge device pattern)
    let t = Instant::now();
    for i in 0..n {
        let x = (i as f64 * 0.37) % 1000.0;
        let y = (i as f64 * 0.59) % 1000.0;
        let body = words[i % words.len()];
        let vstr: Vec<String> = vectors[i].iter().map(|f| format!("{}", f)).collect();
        let sql = format!(
            "INSERT INTO items (emb, loc, body, price, region) VALUES ([{}], POINT({:.1},{:.1}), '{}', {:.1}, '{}')",
            vstr.join(","), x, y, body, i as f64, if i%3==0{"US"}else{"EU"}
        );
        db.execute(&sql).unwrap();
    }
    let insert_ms = t.elapsed().as_millis();
    let rss = get_rss_mb();
    println!(
        "  INSERT {} multimodal rows (VEC+GEO+TEXT): {} ms ({:.0} rows/s, RSS {:.0}MB)",
        n,
        insert_ms,
        n as f64 / insert_ms as f64 * 1000.0,
        rss
    );

    // CREATE all indexes
    let t = Instant::now();
    db.execute("CREATE VECTOR INDEX idx_emb ON items (emb)")
        .unwrap();
    let vec_idx = t.elapsed().as_millis();
    println!("  CREATE VECTOR INDEX: {} ms", vec_idx);

    let t = Instant::now();
    db.execute("CREATE SPATIAL INDEX idx_loc ON items (loc)")
        .unwrap();
    let spa_idx = t.elapsed().as_millis();
    println!("  CREATE SPATIAL INDEX: {} ms", spa_idx);

    let t = Instant::now();
    db.execute("CREATE TEXT INDEX idx_body ON items (body)")
        .unwrap();
    let txt_idx = t.elapsed().as_millis();
    println!("  CREATE TEXT INDEX: {} ms", txt_idx);

    let t = Instant::now();
    db.execute("CREATE INDEX idx_region ON items (region) USING COLUMN")
        .unwrap();
    let col_idx = t.elapsed().as_millis();
    println!("  CREATE COLUMN INDEX: {} ms", col_idx);

    let total_idx = vec_idx + spa_idx + txt_idx + col_idx;
    println!("  Total CREATE INDEX: {} ms", total_idx);

    // Mixed queries
    let qstr = vectors[0]
        .iter()
        .map(|f| format!("{}", f))
        .collect::<Vec<_>>()
        .join(",");
    let _ = db.execute(&format!(
        "SELECT id FROM items ORDER BY emb <-> [{}] LIMIT 10",
        qstr
    ));
    let _ = db.execute("SELECT id FROM items ORDER BY ST_DISTANCE(loc, 500.0, 500.0) LIMIT 10");
    let _ = db.execute("SELECT id FROM items WHERE MATCH(body, 'robot') LIMIT 10");

    let (knn_p50, knn_p99, _) = measure_us(
        || {
            let _ = db.execute(&format!(
                "SELECT id FROM items ORDER BY emb <-> [{}] LIMIT 10",
                qstr
            ));
        },
        20,
    );
    let (spatial_p50, spatial_p99, _) = measure_us(
        || {
            let _ =
                db.execute("SELECT id FROM items ORDER BY ST_DISTANCE(loc, 500.0, 500.0) LIMIT 10");
        },
        20,
    );
    let (fts_p50, fts_p99, _) = measure_us(
        || {
            let _ = db.execute("SELECT id FROM items WHERE MATCH(body, 'robot') LIMIT 10");
        },
        20,
    );

    println!("\n  Mixed query latency (10K rows):");
    println!("    Vector KNN:      P50={}μs  P99={}μs", knn_p50, knn_p99);
    println!(
        "    Spatial KNN:     P50={}μs  P99={}μs",
        spatial_p50, spatial_p99
    );
    println!("    FTS search:      P50={}μs  P99={}μs", fts_p50, fts_p99);

    let final_rss = get_rss_mb();
    println!("\n  Final RSS: {:.0} MB", final_rss);
    println!("  {}", "─".repeat(70));
}

fn main() {
    println!("\n  ╔══════════════════════════════════════════════════════════════╗");
    println!("  ║       MoteDB 多模态读写性能基准 — vs 竞品                     ║");
    println!("  ╚══════════════════════════════════════════════════════════════╝");

    bench_vector_search();
    bench_text_search();
    bench_spatial_search();
    bench_multimodal_write();

    println!("\n  ════════════════════════════════════════════════════════════════");
    println!("  基准测试完成。");
}
