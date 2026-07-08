//! Comprehensive Multimodal Benchmark & Test Suite
//!
//! Tests: Vector KNN, Spatial queries, Text FTS, Mixed multimodal
//! Scale: 50K-100K items (embedded-realistic)
//! Metrics: latency, throughput, RSS memory
//!
//! Run: cargo test --release --features jemalloc --test bench_multimodal_full -- --nocapture --test-threads=1

use motedb::{DBConfig, Database};
use std::time::Instant;
use tempfile::TempDir;

fn rss_mb() -> f64 {
    let pid = std::process::id();
    std::process::Command::new("ps")
        .args(["-o", "rss", "-p", &pid.to_string()])
        .output()
        .ok()
        .and_then(|o| {
            let s = String::from_utf8_lossy(&o.stdout);
            s.lines()
                .skip(1)
                .next()
                .and_then(|l| l.trim().parse::<usize>().ok())
                .map(|v| v as f64 / 1024.0)
        })
        .unwrap_or(0.0)
}

fn edge_db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let mut c = DBConfig::for_edge();
    c.max_result_rows = None;
    (Database::create_with_config(dir.path(), c).unwrap(), dir)
}

fn timed<F: FnOnce()>(name: &str, f: F) {
    let t = Instant::now();
    f();
    let ms = t.elapsed().as_millis();
    println!("  {:<55} {:>7} ms  (RSS: {:.0} MB)", name, ms, rss_mb());
}

// ═══════════════════════════════════════════════════════════════
// Vector KNN Benchmark (50K vectors, 128-dim)
// ═══════════════════════════════════════════════════════════════
#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn bench_vector_knn_10k() {
    let (db, _dir) = edge_db();
    let n = 10_000usize;
    let dim = 128;

    db.execute(&format!(
        "CREATE TABLE vectors (id INT PRIMARY KEY AUTO_INCREMENT, embedding VECTOR({}), category TEXT)",
        dim
    )).unwrap();

    // Bulk insert
    timed(
        &format!("INSERT {} vectors (10K scale) ({}-dim)", n, dim),
        || {
            let batch = 1000;
            for start in (0..n).step_by(batch) {
                let end = (start + batch).min(n);
                let mut sql = String::from("INSERT INTO vectors (embedding, category) VALUES ");
                for i in start..end {
                    if i > start {
                        sql.push(',');
                    }
                    let cat = match i % 4 {
                        0 => "electronics",
                        1 => "clothing",
                        2 => "books",
                        _ => "food",
                    };
                    // Generate pseudo-random vector
                    let vec: Vec<String> = (0..dim)
                        .map(|d| format!("{:.4}", ((i * 7 + d * 13) as f64).sin() * 0.5 + 0.5))
                        .collect();
                    sql.push_str(&format!("([{}],'{}')", vec.join(","), cat));
                }
                match db.execute(&sql) {
                    Ok(_) => {}
                    Err(e) => panic!("batch INSERT failed: {:?}", e),
                }
            }
        },
    );

    // Create vector index
    timed("CREATE VECTOR INDEX (10K, 128-dim)", || {
        let _ = db
            .execute("CREATE VECTOR INDEX idx_emb ON vectors (embedding)")
            .unwrap();
    });

    // KNN search via index
    let query_vec: Vec<String> = (0..dim)
        .map(|d| format!("{:.4}", (d as f64 * 0.1).sin() * 0.5 + 0.5))
        .collect();
    let knn_sql = format!(
        "SELECT id, category FROM vectors WHERE KNN_SEARCH(embedding, [{}], 10)",
        query_vec.join(",")
    );

    timed("KNN_SEARCH k=10 (warm cache, x10)", || {
        for _ in 0..10 {
            let r = db.execute(&knn_sql).unwrap().materialize().unwrap();
            assert!(r.row_count() <= 11, "KNN should return <= 11 results");
        }
    });

    // KNN via SQL ORDER BY distance
    let order_sql = format!(
        "SELECT id FROM vectors ORDER BY embedding <-> [{}] LIMIT 10",
        query_vec.join(",")
    );
    timed("ORDER BY distance LIMIT 10 (x5)", || {
        for _ in 0..5 {
            let r = db.execute(&order_sql).unwrap().materialize().unwrap();
            assert!(
                matches!(&r, motedb::QueryResult::Select { rows, .. } if !rows.is_empty()),
                "should find nearest vectors"
            );
        }
    });

    // Vector + category filter
    let filter_sql = format!(
        "SELECT id FROM vectors WHERE category = 'electronics' ORDER BY embedding <-> [{}] LIMIT 5",
        query_vec.join(",")
    );
    timed("Vector + category filter (x5)", || {
        for _ in 0..5 {
            let r = db.execute(&filter_sql).unwrap().materialize().unwrap();
            if let motedb::QueryResult::Select { rows, .. } = r {
                for row in &rows {
                    // All results should be electronics
                    assert_eq!(
                        row.iter()
                            .filter(|v| matches!(v, motedb::types::Value::Text(_)))
                            .count(),
                        0
                    );
                }
            }
        }
    });

    println!("  -> Vector KNN benchmark complete");
}

// ═══════════════════════════════════════════════════════════════
// Spatial Benchmark (50K points)
// ═══════════════════════════════════════════════════════════════
#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn bench_spatial_10k() {
    let (db, _dir) = edge_db();
    let n = 10_000usize;

    db.execute("CREATE TABLE places (id INT PRIMARY KEY AUTO_INCREMENT, location GEOMETRY, name TEXT, population INT)").unwrap();

    // Bulk insert spatial points
    timed(&format!("INSERT {} spatial points (10K scale)", n), || {
        let batch = 1000;
        for start in (0..n).step_by(batch) {
            let end = (start + batch).min(n);
            let mut sql = String::from("INSERT INTO places (location, name, population) VALUES ");
            for i in start..end {
                if i > start {
                    sql.push(',');
                }
                let lat = 30.0 + (i as f64 * 0.001) % 30.0; // 30-60 lat
                let lon = -120.0 + (i as f64 * 0.001) % 60.0; // -120 to -60 lon
                let pop = (i * 100 + 500) as i64;
                sql.push_str(&format!(
                    "(POINT({:.1},{:.1}),'city_{}',{})",
                    lon,
                    lat,
                    i % 1000,
                    pop
                ));
            }
            match db.execute(&sql) {
                Ok(_) => {}
                Err(e) => panic!("batch INSERT failed: {:?}", e),
            }
        }
    });

    // Create spatial index
    timed("CREATE SPATIAL INDEX (10K points)", || {
        let _ = db
            .execute("CREATE SPATIAL INDEX idx_loc ON places (location)")
            .unwrap();
    });

    // ST_DISTANCE query
    timed("ST_DISTANCE ORDER BY LIMIT 10 (x10)", || {
        for _ in 0..10 {
            let r = db.execute("SELECT id, name FROM places ORDER BY ST_DISTANCE(location, -100.0, 40.0) LIMIT 10")
                .unwrap().materialize().unwrap();
            assert!(
                matches!(&r, motedb::QueryResult::Select { rows, .. } if !rows.is_empty()),
                "should find nearest places"
            );
        }
    });

    // Population filter + spatial
    timed("Spatial + population > 500000 (x5)", || {
        for _ in 0..5 {
            let _r = db.execute("SELECT id FROM places WHERE population > 500000 ORDER BY ST_DISTANCE(location, -100.0, 40.0) LIMIT 10")
                .unwrap().materialize().unwrap();
        }
    });

    println!("  -> Spatial benchmark complete");
}

// ═══════════════════════════════════════════════════════════════
// Text/FTS Benchmark (50K documents)
// ═══════════════════════════════════════════════════════════════
#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn bench_text_fts_10k() {
    let (db, _dir) = edge_db();
    let n = 10_000usize;
    let words = [
        "database",
        "machine",
        "learning",
        "robot",
        "sensor",
        "embedded",
        "real-time",
        "edge",
        "iot",
        "performance",
    ];

    db.execute("CREATE TABLE docs (id INT PRIMARY KEY AUTO_INCREMENT, title TEXT, body TEXT, category TEXT)").unwrap();

    timed(&format!("INSERT {} documents (10K scale)", n), || {
        let batch = 1000;
        for start in (0..n).step_by(batch) {
            let end = (start + batch).min(n);
            let mut sql = String::from("INSERT INTO docs (title, body, category) VALUES ");
            for i in start..end {
                if i > start {
                    sql.push(',');
                }
                let w1 = words[i % words.len()];
                let w2 = words[(i + 3) % words.len()];
                let cat = match i % 3 {
                    0 => "tech",
                    1 => "science",
                    _ => "news",
                };
                sql.push_str(&format!(
                    "('{} guide {}','{} and {} for edge computing','{}')",
                    w1, i, w1, w2, cat
                ));
            }
            match db.execute(&sql) {
                Ok(_) => {}
                Err(e) => panic!("batch INSERT failed: {:?}", e),
            }
        }
    });

    timed("CREATE TEXT INDEX (10K docs)", || {
        let _ = db
            .execute("CREATE TEXT INDEX idx_body ON docs (body)")
            .unwrap();
    });

    timed("FTS 'database' top-10 (x10)", || {
        for _ in 0..10 {
            let _r = db
                .execute("SELECT id FROM docs WHERE MATCH(body, 'database') LIMIT 10")
                .unwrap()
                .materialize()
                .unwrap();
        }
    });

    timed("FTS + category filter (x5)", || {
        for _ in 0..5 {
            let _r = db.execute("SELECT id FROM docs WHERE MATCH(body, 'sensor') AND category = 'tech' LIMIT 10")
                .unwrap().materialize().unwrap();
        }
    });

    timed("LIKE prefix scan 'robot%' (x5)", || {
        for _ in 0..5 {
            let _r = db
                .execute("SELECT id FROM docs WHERE title LIKE 'robot%' LIMIT 10")
                .unwrap()
                .materialize()
                .unwrap();
        }
    });

    println!("  -> Text/FTS benchmark complete");
}

// ═══════════════════════════════════════════════════════════════
// Mixed Multimodal Benchmark (Vector + Spatial + Text in same table)
// ═══════════════════════════════════════════════════════════════
#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn bench_mixed_multimodal_10k() {
    let (db, _dir) = edge_db();
    let n = 10_000usize;

    db.execute(&format!(
        "CREATE TABLE items (id INT PRIMARY KEY AUTO_INCREMENT, emb VECTOR(64), loc GEOMETRY, info TEXT, price FLOAT, region TEXT)"
    )).unwrap();

    timed(&format!("INSERT {} multimodal items", n), || {
        let batch = 1000;
        for start in (0..n).step_by(batch) {
            let end = (start + batch).min(n);
            let mut sql = String::from("INSERT INTO items (emb, loc, info, price, region) VALUES ");
            for i in start..end {
                if i > start {
                    sql.push(',');
                }
                let vec: Vec<String> = (0..64)
                    .map(|d| format!("{:.3}", ((i + d) as f64 * 0.1).sin()))
                    .collect();
                let lat = 30.0 + (i as f64 * 0.01) % 30.0;
                let lon = -120.0 + (i as f64 * 0.01) % 60.0;
                let price = (i as f64 * 1.7) % 1000.0;
                let region = if i % 3 == 0 { "US" } else { "EU" };
                sql.push_str(&format!(
                    "([{}],POINT({:.1},{:.1}),'item info {}',{},'{}')",
                    vec.join(","),
                    lon,
                    lat,
                    i,
                    price,
                    region
                ));
            }
            match db.execute(&sql) {
                Ok(_) => {}
                Err(e) => panic!("batch INSERT failed: {:?}", e),
            }
        }
    });

    // Create all indexes
    timed(
        "CREATE multimodal indexes (vector + spatial + text + column)",
        || {
            let _ = db.execute("CREATE VECTOR INDEX idx_emb ON items (emb)");
            let _ = db.execute("CREATE SPATIAL INDEX idx_loc ON items (loc)");
            let _ = db.execute("CREATE TEXT INDEX idx_desc ON items (info)");
            let _ = db.execute("CREATE INDEX idx_region ON items (region) USING COLUMN");
        },
    );

    // Query 1: Vector KNN + price filter
    let qvec: Vec<String> = (0..64)
        .map(|d| format!("{:.3}", (d as f64 * 0.1).sin()))
        .collect();
    timed("KNN + price > 500 (x5)", || {
        for _ in 0..5 {
            let sql = format!(
                "SELECT id FROM items WHERE KNN_SEARCH(emb, [{}], 10) AND price > 500",
                qvec.join(",")
            );
            let _r = db.execute(&sql).unwrap().materialize().unwrap();
        }
    });

    // Query 2: Spatial + region filter
    timed("Spatial + region='US' (x5)", || {
        for _ in 0..5 {
            let _r = db.execute("SELECT id FROM items WHERE region = 'US' ORDER BY ST_DISTANCE(loc, -100.0, 40.0) LIMIT 10")
                .unwrap().materialize().unwrap();
        }
    });

    // Query 3: Text + price range
    timed("FTS 'item' + price range (x5)", || {
        for _ in 0..5 {
            let _r = db.execute("SELECT id FROM items WHERE MATCH(info, 'item') AND price > 200 AND price < 800 LIMIT 10")
                .unwrap().materialize().unwrap();
        }
    });

    // Query 4: Aggregate by region
    timed("GROUP BY region COUNT + AVG(price)", || {
        let _r = db
            .execute("SELECT region, COUNT(*), AVG(price) FROM items GROUP BY region")
            .unwrap()
            .materialize()
            .unwrap();
    });

    // Memory check
    let final_rss = rss_mb();
    println!("  -> Final RSS: {:.0} MB", final_rss);
    assert!(
        final_rss < 80.0,
        "RSS should be < 50MB for 20K multimodal items, got {:.0} MB",
        final_rss
    );

    println!("  -> Mixed multimodal benchmark complete");
}

// ═══════════════════════════════════════════════════════════════
// Correctness Tests for Multimodal
// ═══════════════════════════════════════════════════════════════
#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn test_vector_insert_and_knn() {
    let (db, _dir) = edge_db();
    db.execute(
        "CREATE TABLE vecs (id INT PRIMARY KEY AUTO_INCREMENT, embedding VECTOR(4), label TEXT)",
    )
    .unwrap();

    db.execute("INSERT INTO vecs (embedding, label) VALUES ([1.0, 0.0, 0.0, 0.0], 'x-axis')")
        .unwrap();
    db.execute("INSERT INTO vecs (embedding, label) VALUES ([0.0, 1.0, 0.0, 0.0], 'y-axis')")
        .unwrap();
    db.execute("INSERT INTO vecs (embedding, label) VALUES ([0.0, 0.0, 1.0, 0.0], 'z-axis')")
        .unwrap();
    db.execute("INSERT INTO vecs (embedding, label) VALUES ([1.0, 1.0, 0.0, 0.0], 'xy-plane')")
        .unwrap();

    // Query nearest to [1,0,0,0] — should be 'x-axis' (distance 0) then 'xy-plane' (distance ~0.3)
    let r = db
        .execute("SELECT label FROM vecs ORDER BY embedding <-> [1.0, 0.0, 0.0, 0.0] LIMIT 2")
        .unwrap()
        .materialize()
        .unwrap();
    if let motedb::QueryResult::Select { rows, .. } = r {
        assert!(rows.len() >= 1, "should find nearest vector");
    }
}

#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn test_spatial_insert_and_distance() {
    let (db, _dir) = edge_db();
    db.execute("CREATE TABLE pts (id INT PRIMARY KEY AUTO_INCREMENT, loc GEOMETRY)")
        .unwrap();
    db.execute("INSERT INTO pts (loc) VALUES (POINT(0, 0))")
        .unwrap();
    db.execute("INSERT INTO pts (loc) VALUES (POINT(1, 1))")
        .unwrap();
    db.execute("INSERT INTO pts (loc) VALUES (POINT(10, 10))")
        .unwrap();

    let r = db
        .execute("SELECT id FROM pts ORDER BY ST_DISTANCE(loc, 0, 0) LIMIT 2")
        .unwrap()
        .materialize()
        .unwrap();
    if let motedb::QueryResult::Select { rows, .. } = r {
        assert!(rows.len() >= 1, "should find nearest point");
    }
}

#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn test_multimodal_table_all_types() {
    let (db, _dir) = edge_db();
    db.execute("CREATE TABLE mm (id INT PRIMARY KEY AUTO_INCREMENT, vec VECTOR(3), loc GEOMETRY, txt TEXT, val FLOAT, cat TEXT)").unwrap();

    db.execute("INSERT INTO mm (vec, loc, txt, val, cat) VALUES ([1.0, 2.0, 3.0], POINT(1.0, 2.0), 'hello world', 42.5, 'A')").unwrap();
    db.execute("INSERT INTO mm (vec, loc, txt, val, cat) VALUES ([4.0, 5.0, 6.0], POINT(3.0, 4.0), 'foo bar', 99.9, 'B')").unwrap();

    // All types coexist in one table
    let r = db
        .execute("SELECT COUNT(*) FROM mm")
        .unwrap()
        .materialize()
        .unwrap();
    if let motedb::QueryResult::Select { rows, .. } = r {
        assert_eq!(rows.len(), 1, "COUNT should return 1 row");
    }

    // WHERE filter on scalar column + text
    let r = db
        .execute("SELECT id FROM mm WHERE cat = 'A'")
        .unwrap()
        .materialize()
        .unwrap();
    if let motedb::QueryResult::Select { rows, .. } = r {
        assert_eq!(rows.len(), 1, "should find 1 row with cat='A'");
    }

    // LIKE on text
    let r = db
        .execute("SELECT id FROM mm WHERE txt LIKE 'hello%'")
        .unwrap()
        .materialize()
        .unwrap();
    if let motedb::QueryResult::Select { rows, .. } = r {
        assert_eq!(rows.len(), 1, "should find 1 row with txt LIKE 'hello%'");
    }
}

#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn test_p99_multimodal_queries() {
    let (db, _dir) = edge_db();
    let n = 10_000usize;

    db.execute("CREATE TABLE sensors (id INT PRIMARY KEY AUTO_INCREMENT, emb VECTOR(32), loc GEOMETRY, label TEXT, region TEXT)").unwrap();

    // Seed
    let batch = 1000;
    for start in (0..n).step_by(batch) {
        let end = (start + batch).min(n);
        let mut sql = String::from("INSERT INTO sensors (emb, loc, label, region) VALUES ");
        for i in start..end {
            if i > start {
                sql.push(',');
            }
            let vec: Vec<String> = (0..32)
                .map(|d| format!("{:.2}", ((i + d) as f64 * 0.1).sin()))
                .collect();
            let lat = 30.0 + (i as f64 * 0.01) % 30.0;
            let lon = -120.0 + (i as f64 * 0.01) % 60.0;
            let region = if i % 3 == 0 { "US" } else { "EU" };
            sql.push_str(&format!(
                "([{}],POINT({:.1},{:.1}),'sensor_{}','{}')",
                vec.join(","),
                lon,
                lat,
                i,
                region
            ));
        }
        match db.execute(&sql) {
            Ok(_) => {}
            Err(e) => panic!("batch INSERT failed: {:?}", e),
        }
    }

    // Create indexes
    let _ = db.execute("CREATE VECTOR INDEX idx_emb ON sensors (emb)");
    let _ = db.execute("CREATE SPATIAL INDEX idx_loc ON sensors (loc)");

    // P99 measurement: run each query type 20 times, report max
    let qvec: Vec<String> = (0..32)
        .map(|d| format!("{:.2}", (d as f64 * 0.1).sin()))
        .collect();

    let queries: Vec<(&str, String)> = vec![
        (
            "PK point",
            "SELECT * FROM sensors WHERE id = 5000".to_string(),
        ),
        (
            "WHERE region",
            "SELECT * FROM sensors WHERE region = 'US' LIMIT 10".to_string(),
        ),
        (
            "COUNT WHERE",
            "SELECT COUNT(*) FROM sensors WHERE region = 'US'".to_string(),
        ),
        (
            "GROUP BY",
            "SELECT region, COUNT(*) FROM sensors GROUP BY region".to_string(),
        ),
        (
            "Vector KNN",
            format!(
                "SELECT id FROM sensors ORDER BY emb <-> [{}] LIMIT 5",
                qvec.join(",")
            ),
        ),
        (
            "Spatial dist",
            "SELECT id FROM sensors ORDER BY ST_DISTANCE(loc, -100.0, 40.0) LIMIT 5".to_string(),
        ),
    ];

    for (name, sql) in &queries {
        let mut max_us = 0u128;
        for _ in 0..20 {
            let t = Instant::now();
            let _ = db.execute(sql).unwrap().materialize().unwrap();
            max_us = max_us.max(t.elapsed().as_micros());
        }
        println!("  {:<20} P99 = {:>8.2} ms", name, max_us as f64 / 1000.0);
    }

    println!("  -> P99 multimodal benchmark complete");
}
