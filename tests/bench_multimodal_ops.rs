//! Multimodal Benchmark — Text FTS, Vector KNN, Spatial Queries
//!
//! Run: cargo test --release --test bench_multimodal_ops -- --nocapture --test-threads=1

use motedb::{Database, DBConfig};
use tempfile::TempDir;
use std::time::Instant;

fn is_ci() -> bool { std::env::var("CI").is_ok() }

fn edge_config() -> DBConfig { DBConfig::for_edge() }

fn exec(db: &Database, sql: &str) -> motedb::sql::QueryResult {
    db.execute(sql).unwrap().materialize().unwrap()
}

fn print_result(name: &str, ops: usize, elapsed_ms: u64) {
    let per_op_us = if ops > 0 { (elapsed_ms as f64 * 1000.0) / ops as f64 } else { 0.0 };
    let throughput = if elapsed_ms > 0 { ops as f64 / (elapsed_ms as f64 / 1000.0) } else { f64::INFINITY };
    println!("  {:<60} | {:>7} ops | {:>8.1} ms | {:>8.1} µs/op | {:>10.0} ops/s",
        name, ops, elapsed_ms as f64, per_op_us, throughput);
}

fn print_separator() {
    println!("  {}", "-".repeat(100));
}

// ═══════════════════════════════════════════════════════════════
// Test 1: Text FTS Performance
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_text_fts() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), edge_config()).unwrap();
    let n = if is_ci() { 500 } else { 5000 };

    exec(&db, "CREATE TABLE docs (id INT PRIMARY KEY, title TEXT, body TEXT)");

    println!("\n  === Text FTS Benchmark ({} documents) ===", n);
    print_separator();

    // Seed documents with varied text
    let words = ["database", "management", "system", "relational", "query",
                 "optimization", "index", "machine", "learning", "neural",
                 "network", "deep", "artificial", "intelligence", "distributed",
                 "computing", "consensus", "algorithm", "fault", "tolerance"];
    let seed_start = Instant::now();
    for i in 0..n {
        let mut parts = Vec::new();
        for j in 0..5 {
            parts.push(words[(i + j) % words.len()]);
        }
        let title = parts[..2].join(" ");
        let body = parts.join(" ");
        exec(&db, &format!("INSERT INTO docs VALUES ({}, '{}', '{}')", i, title, body));
    }
    let seed_ms = seed_start.elapsed().as_millis() as u64;
    print_result(&format!("Seed {} documents", n), n, seed_ms);

    // Create text indexes
    let idx_start = Instant::now();
    exec(&db, "CREATE TEXT INDEX idx_title ON docs (title)");
    exec(&db, "CREATE TEXT INDEX idx_body ON docs (body)");
    db.wait_for_indexes_ready();
    db.flush().unwrap();
    let idx_ms = idx_start.elapsed().as_millis() as u64;
    print_result("CREATE TEXT INDEX x2", 2, idx_ms);

    print_separator();

    let q = if is_ci() { 10 } else { 50 };

    // FTS keyword search
    let fts_ms = {
        let start = Instant::now();
        for _ in 0..q {
            let _ = db.text_search_ranked("idx_body", "database", 10);
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("FTS 'database' top-10 x {}", q), q, fts_ms);

    // MATCH ... AGAINST query
    let match_ms = {
        let start = Instant::now();
        for _ in 0..q {
            let _ = exec(&db, "SELECT id FROM docs WHERE MATCH(title, 'machine')");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("MATCH(title, 'machine') x {}", q), q, match_ms);

    // Multi-word FTS
    let multi_ms = {
        let start = Instant::now();
        for _ in 0..q {
            let _ = db.text_search_ranked("idx_body", "machine learning", 10);
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("FTS 'machine learning' top-10 x {}", q), q, multi_ms);

    let fts_per = fts_ms as f64 * 1000.0 / q as f64;
    let match_per = match_ms as f64 * 1000.0 / q as f64;
    println!("  -> FTS: {:.1}µs/op, MATCH: {:.1}µs/op", fts_per, match_per);

    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 2: Vector KNN Performance
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_vector_knn() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), edge_config()).unwrap();
    let dim = 128;
    let n = if is_ci() { 200 } else { 2000 };

    exec(&db, &format!("CREATE TABLE vecs (id INT PRIMARY KEY, embedding VECTOR({}))", dim));

    println!("\n  === Vector KNN Benchmark ({} vectors, dim={}) ===", n, dim);
    print_separator();

    // Seed vectors
    let seed_start = Instant::now();
    for i in 0..n {
        let values: Vec<f32> = (0..dim).map(|j| {
            ((i as f32).sin() + j as f32 * 0.01).sin()
        }).collect();
        let tensor_str = format!("[{}]", values.iter().map(|v| format!("{:.6}", v)).collect::<Vec<_>>().join(", "));
        exec(&db, &format!("INSERT INTO vecs VALUES ({}, {})", i, tensor_str));
    }
    let seed_ms = seed_start.elapsed().as_millis() as u64;
    print_result(&format!("Seed {} vectors ({}-dim)", n, dim), n, seed_ms);

    // Create vector index
    let idx_start = Instant::now();
    exec(&db, "CREATE VECTOR INDEX idx_vec ON vecs (embedding)");
    db.wait_for_indexes_ready();
    let idx_ms = idx_start.elapsed().as_millis() as u64;
    print_result("CREATE VECTOR INDEX", 1, idx_ms);

    print_separator();

    let q = if is_ci() { 5 } else { 20 };
    let query_vec: Vec<f32> = (0..dim).map(|j| (j as f32 * 0.02).sin()).collect();

    // Vector KNN search
    let knn_ms = {
        let start = Instant::now();
        for _ in 0..q {
            let _ = db.vector_search("idx_vec", &query_vec, 10);
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("Vector KNN (k=10) x {}", q), q, knn_ms);

    // SQL ORDER BY vector distance
    let query_str = format!("[{}]", query_vec.iter().map(|v| format!("{:.6}", v)).collect::<Vec<_>>().join(", "));
    let sql_knn_ms = {
        let start = Instant::now();
        for _ in 0..q {
            let _ = exec(&db, &format!(
                "SELECT id FROM vecs ORDER BY embedding <-> {} LIMIT 10", query_str
            ));
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("SQL ORDER BY emb <-> q LIMIT 10 x {}", q), q, sql_knn_ms);

    let knn_per = knn_ms as f64 * 1000.0 / q as f64;
    let sql_per = sql_knn_ms as f64 * 1000.0 / q as f64;
    println!("  -> Raw KNN: {:.1}µs/op, SQL KNN: {:.1}µs/op", knn_per, sql_per);

    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 3: Spatial Query Performance
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_spatial_queries() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), edge_config()).unwrap();
    let n = if is_ci() { 500 } else { 5000 };

    exec(&db, "CREATE TABLE points (id INT PRIMARY KEY, name TEXT, location GEOMETRY)");

    println!("\n  === Spatial Query Benchmark ({} points) ===", n);
    print_separator();

    // Seed spatial points (clustered around a few locations)
    let seed_start = Instant::now();
    let centers = [(116.40, 39.90), (121.47, 31.23), (113.26, 23.13), (120.16, 30.25)];
    for i in 0..n {
        let center = centers[i % centers.len()];
        let lat = center.0 + (i as f64 * 0.001).sin() * 2.0;
        let lon = center.1 + (i as f64 * 0.001).cos() * 2.0;
        exec(&db, &format!("INSERT INTO points VALUES ({}, 'point_{}', POINT({:.6}, {:.6}))",
            i, i, lat, lon));
    }
    let seed_ms = seed_start.elapsed().as_millis() as u64;
    print_result(&format!("Seed {} spatial points", n), n, seed_ms);

    // Create spatial index
    let idx_start = Instant::now();
    exec(&db, "CREATE INDEX idx_loc ON points (location) USING OCTREE");
    db.wait_for_indexes_ready();
    db.flush().unwrap();
    let idx_ms = idx_start.elapsed().as_millis() as u64;
    print_result("CREATE SPATIAL INDEX (IOctree)", 1, idx_ms);

    print_separator();

    let q = if is_ci() { 10 } else { 50 };

    // ST_DISTANCE query
    let dist_ms = {
        let start = Instant::now();
        for _ in 0..q {
            let _ = exec(&db, "SELECT id FROM points ORDER BY ST_DISTANCE(location, 116.5, 40.0) LIMIT 10");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("ST_DISTANCE ORDER BY LIMIT 10 x {}", q), q, dist_ms);

    // WITHIN_RADIUS query
    let radius_ms = {
        let start = Instant::now();
        for _ in 0..q {
            let _ = exec(&db, "SELECT COUNT(*) FROM points WHERE WITHIN_RADIUS(location, POINT(116.5, 40.0), 100.0)");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("WITHIN_RADIUS 100km x {}", q), q, radius_ms);

    // ST_WITHIN_3D query (bounding box)
    let bbox_ms = {
        let start = Instant::now();
        for _ in 0..q {
            let _ = exec(&db, "SELECT COUNT(*) FROM points WHERE ST_WITHIN_3D(location, 113.0, 23.0, 0, 117.0, 40.0, 0)");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("ST_WITHIN_3D bbox x {}", q), q, bbox_ms);

    let dist_per = dist_ms as f64 * 1000.0 / q as f64;
    let radius_per = radius_ms as f64 * 1000.0 / q as f64;
    println!("  -> ST_DISTANCE: {:.1}µs/op, WITHIN_RADIUS: {:.1}µs/op", dist_per, radius_per);

    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 4: Mixed Multimodal Workload
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_multimodal_mixed() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), edge_config()).unwrap();
    let n = if is_ci() { 200 } else { 1000 };

    // Dual table setup: one with vectors + text, one with spatial
    exec(&db, "CREATE TABLE items (id INT PRIMARY KEY, title TEXT, embedding VECTOR(64))");
    exec(&db, "CREATE TABLE locations (id INT PRIMARY KEY, name TEXT, coords GEOMETRY)");

    println!("\n  === Mixed Multimodal Benchmark ({} items) ===", n);
    print_separator();

    // Seed
    let words = ["laptop", "phone", "tablet", "watch", "camera", "speaker", "monitor", "keyboard"];
    for i in 0..n {
        let title = words[i % words.len()];
        let vec: Vec<f32> = (0..64).map(|j| (i as f32 * 0.1 + j as f32).sin()).collect();
        let tensor_str = format!("[{}]", vec.iter().map(|v| format!("{:.6}", v)).collect::<Vec<_>>().join(", "));
        exec(&db, &format!("INSERT INTO items VALUES ({}, '{}', {})", i, title, tensor_str));
        exec(&db, &format!("INSERT INTO locations VALUES ({}, 'loc_{}', POINT({:.4}, {:.4}))",
            i, i, 116.0 + i as f64 * 0.01, 40.0 + i as f64 * 0.01));
    }

    // Create all index types
    exec(&db, "CREATE TEXT INDEX idx_title ON items (title)");
    exec(&db, "CREATE VECTOR INDEX idx_emb ON items (embedding)");
    exec(&db, "CREATE INDEX idx_coords ON locations (coords) USING OCTREE");
    db.wait_for_indexes_ready();
    db.flush().unwrap();

    print_separator();

    let q = if is_ci() { 5 } else { 20 };

    // Text search
    let text_ms = {
        let start = Instant::now();
        for _ in 0..q {
            let _ = db.text_search_ranked("idx_title", "laptop", 5);
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("FTS title 'laptop' top-5 x {}", q), q, text_ms);

    // Vector search
    let qvec: Vec<f32> = (0..64).map(|j| (j as f32 * 0.02).sin()).collect();
    let vec_ms = {
        let start = Instant::now();
        for _ in 0..q {
            let _ = db.vector_search("idx_emb", &qvec, 5);
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("Vector KNN (k=5) x {}", q), q, vec_ms);

    // Spatial search
    let spatial_ms = {
        let start = Instant::now();
        for _ in 0..q {
            let _ = exec(&db, "SELECT id FROM locations ORDER BY ST_DISTANCE(coords, 116.5, 40.0) LIMIT 5");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("ST_DISTANCE LIMIT 5 x {}", q), q, spatial_ms);

    println!("  -> FTS: {:.0}µs/op, KNN: {:.0}µs/op, Spatial: {:.0}µs/op",
        text_ms as f64 * 1000.0 / q as f64,
        vec_ms as f64 * 1000.0 / q as f64,
        spatial_ms as f64 * 1000.0 / q as f64);

    db.close().ok();
}
