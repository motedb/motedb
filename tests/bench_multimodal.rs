//! Multimodal Benchmark: Vector, Spatial, Text/FTS
//!
//! Run: cargo test --test bench_multimodal --release -- --nocapture --test-threads=1

use motedb::Database;
use tempfile::TempDir;
use std::time::Instant;

fn create_db() -> (Database, TempDir) {
    let dir = TempDir::new().expect("temp dir");
    let db = Database::create(dir.path()).expect("create db");
    (db, dir)
}

fn exec(db: &Database, sql: &str) -> motedb::sql::QueryResult {
    db.execute(sql).unwrap_or_else(|_| panic!("SQL: {}", sql)).materialize().expect("materialize")
}

fn get_rss_mb() -> f64 {
    let pid = std::process::id();
    let output = std::process::Command::new("ps")
        .args(["-o", "rss", "-p", &pid.to_string()])
        .output().ok();
    if let Some(out) = output {
        let stdout = String::from_utf8_lossy(&out.stdout);
        for line in stdout.lines().skip(1) {
            if let Ok(rss) = line.trim().parse::<usize>() {
                return rss as f64 / 1024.0;
            }
        }
    }
    0.0
}

fn print_latency(label: &str, latencies_us: &[u64]) {
    if latencies_us.is_empty() { return; }
    let mut s = latencies_us.to_vec();
    s.sort_unstable();
    let n = s.len();
    let p50 = s[n * 50 / 100];
    let p95 = s[n * 95 / 100];
    let p99 = s[n * 99 / 100];
    let avg: u64 = s.iter().sum::<u64>() / n as u64;
    println!("  {:<60} | p50={:>7}µs  p95={:>7}µs  p99={:>7}µs  avg={:>7}µs", label, p50, p95, p99, avg);
}

fn sep() { println!("  {}", "─".repeat(105)); }

// Simple deterministic random
static mut RNG: u64 = 42;
fn rand_f32() -> f32 {
    unsafe {
        RNG = RNG.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        ((RNG >> 33) as f32) / (u32::MAX as f32) - 0.5
    }
}
fn rand_f64() -> f64 {
    unsafe {
        RNG = RNG.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (RNG >> 33) as f64 / (1u64 << 31) as f64
    }
}

// ============================================================================
// Test 1: Vector Index — ANN Search (small dataset for speed)
// ============================================================================

#[test]
fn bench_vector() {
    println!("\n{}", "=".repeat(105));
    println!("  Vector Index Benchmark (DiskANN, 128-dim)");
    println!("{}", "=".repeat(105));

    let (db, _dir) = create_db();
    let rss0 = get_rss_mb();

    exec(&db, "CREATE TABLE items (id INTEGER PRIMARY KEY, cat TEXT, emb VECTOR(128))");
    exec(&db, "CREATE VECTOR INDEX items_emb ON items(emb)");

    // Insert 2000 rows (fast enough for benchmark)
    let n = 2_000;
    println!("\n  --- INSERT {} rows × 128-dim ---", n);
    sep();
    let t0 = Instant::now();
    for i in 1..=n as i64 {
        let mut v = String::from('[');
        for d in 0..128 {
            if d > 0 { v.push_str(", "); }
            v.push_str(&format!("{:.3}", rand_f32()));
        }
        v.push(']');
        exec(&db, &format!("INSERT INTO items VALUES ({}, 'c{}', {})", i, i % 5, v));
    }
    let insert_ms = t0.elapsed().as_millis();
    println!("  INSERT: {}ms ({:.0} ops/s, {:.0} µs/op)", insert_ms,
        n as f64 / (insert_ms as f64 / 1000.0), insert_ms as f64 * 1000.0 / n as f64);

    // Flush to build DiskANN index
    println!("  Flushing + checkpoint...");
    let t0 = Instant::now();
    db.flush().expect("flush");
    db.checkpoint().expect("checkpoint");
    std::thread::sleep(std::time::Duration::from_millis(500));
    println!("  Flush: {}ms", t0.elapsed().as_millis());
    println!("  Memory: {:.1} MB (Δ = {:.1} MB)", get_rss_mb(), get_rss_mb() - rss0);

    // ANN search via API
    println!("\n  --- ANN Search (top-10, API) ---");
    sep();
    let mut ann_lat: Vec<u64> = Vec::with_capacity(500);
    for _ in 0..500 {
        let q: Vec<f32> = (0..128).map(|_| rand_f32()).collect();
        let t = Instant::now();
        let res = db.vector_search("items_emb", &q, 10).expect("vector search");
        ann_lat.push(t.elapsed().as_micros() as u64);
        if ann_lat.len() == 1 {
            println!("  Sample: {} results", res.len());
        }
    }
    print_latency(&format!("ANN search ({} vecs, 128-dim, top-10)", n), &ann_lat);

    // SQL vector search
    println!("\n  --- SQL ORDER BY embedding <-> query ---");
    sep();
    let mut sql_lat: Vec<u64> = Vec::with_capacity(100);
    for _ in 0..100 {
        let mut q = String::from('[');
        for d in 0..128 {
            if d > 0 { q.push_str(", "); }
            q.push_str(&format!("{:.3}", rand_f32()));
        }
        q.push(']');
        let sql = format!("SELECT id, cat FROM items ORDER BY emb <-> {} LIMIT 10", q);
        let t = Instant::now();
        let _ = exec(&db, &sql);
        sql_lat.push(t.elapsed().as_micros() as u64);
    }
    print_latency("SQL ORDER BY emb <-> query LIMIT 10", &sql_lat);

    println!("\n  Memory after vector benchmark: {:.1} MB", get_rss_mb());
}

// ============================================================================
// Test 2: Spatial Index — Range + KNN
// ============================================================================

#[test]
fn bench_spatial() {
    println!("\n{}", "=".repeat(105));
    println!("  Spatial Index Benchmark (Grid+RTree Hybrid)");
    println!("{}", "=".repeat(105));

    let (db, _dir) = create_db();
    let rss0 = get_rss_mb();

    exec(&db, "CREATE TABLE locs (id INTEGER PRIMARY KEY, name TEXT, coords GEOMETRY)");
    exec(&db, "CREATE SPATIAL INDEX loc_coords ON locs(coords)");

    let n = 10_000;
    println!("\n  --- INSERT {} spatial points ---", n);
    sep();
    let t0 = Instant::now();
    for i in 1..=n as i64 {
        let x = 116.0 + (i as f64 % 10000.0) / 10000.0;
        let y = 39.5 + (i as f64 % 10000.0) / 20000.0 + 0.5;
        exec(&db, &format!("INSERT INTO locs VALUES ({}, 'p{}', POINT({}, {}))", i, i, x, y));
    }
    let insert_ms = t0.elapsed().as_millis();
    println!("  INSERT: {}ms ({:.0} ops/s)", insert_ms, n as f64 / (insert_ms as f64 / 1000.0));

    db.flush().expect("flush");
    db.checkpoint().expect("checkpoint");
    std::thread::sleep(std::time::Duration::from_millis(500));
    println!("  Memory: {:.1} MB (Δ = {:.1} MB)", get_rss_mb(), get_rss_mb() - rss0);

    // ST_WITHIN range query
    println!("\n  --- ST_WITHIN Range Query ---");
    sep();
    let mut range_lat: Vec<u64> = Vec::with_capacity(500);
    for _ in 0..500 {
        let cx = 116.0 + rand_f64() * 0.9;
        let cy = 39.5 + rand_f64() * 0.5 + 0.5;
        let sql = format!(
            "SELECT * FROM locs WHERE ST_WITHIN(coords, {:.4}, {:.4}, {:.4}, {:.4})",
            cx - 0.02, cy - 0.02, cx + 0.02, cy + 0.02
        );
        let t = Instant::now();
        let _ = exec(&db, &sql);
        range_lat.push(t.elapsed().as_micros() as u64);
    }
    print_latency("ST_WITHIN (bbox ~0.04° × 0.04°, 500 queries)", &range_lat);

    // ST_DISTANCE + ORDER BY
    println!("\n  --- ST_DISTANCE + ORDER BY LIMIT 10 ---");
    sep();
    let mut dist_lat: Vec<u64> = Vec::with_capacity(200);
    for _ in 0..200 {
        let sql = "SELECT id, name, ST_DISTANCE(coords, 116.5, 40.0) AS dist FROM locs ORDER BY dist LIMIT 10";
        let t = Instant::now();
        let _ = exec(&db, sql);
        dist_lat.push(t.elapsed().as_micros() as u64);
    }
    print_latency("ST_DISTANCE ORDER BY LIMIT 10 (200 queries)", &dist_lat);

    // ST_KNN
    println!("\n  --- ST_KNN Nearest Neighbor ---");
    sep();
    let mut knn_lat: Vec<u64> = Vec::with_capacity(200);
    for _ in 0..200 {
        let cx = 116.0 + rand_f64() * 0.9;
        let cy = 39.5 + rand_f64() * 0.5 + 0.5;
        let sql = format!("SELECT * FROM locs WHERE ST_KNN(coords, {:.4}, {:.4}, 10)", cx, cy);
        let t = Instant::now();
        let _ = exec(&db, &sql);
        knn_lat.push(t.elapsed().as_micros() as u64);
    }
    print_latency("ST_KNN top-10 (200 queries)", &knn_lat);

    println!("\n  Memory after spatial benchmark: {:.1} MB", get_rss_mb());
}

// ============================================================================
// Test 3: Text / Full-Text Search
// ============================================================================

#[test]
fn bench_text_search() {
    println!("\n{}", "=".repeat(105));
    println!("  Text / Full-Text Search Benchmark (BM25)");
    println!("{}", "=".repeat(105));

    let (db, _dir) = create_db();
    let rss0 = get_rss_mb();

    exec(&db, "CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, body TEXT)");
    exec(&db, "CREATE TEXT INDEX docs_body ON docs(body)");

    let words = [
        "database", "vector", "search", "index", "query", "performance",
        "embedding", "model", "neural", "network", "machine", "learning",
        "spatial", "geometry", "point", "distance", "algorithm", "graph",
        "rust", "memory", "thread", "concurrent", "benchmark", "latency",
    ];

    let n = 10_000;
    println!("\n  --- INSERT {} docs ---", n);
    sep();
    let t0 = Instant::now();
    for i in 1..=n as i64 {
        let wc = 5 + (i % 11) as usize;
        let body: Vec<&str> = (0..wc).map(|w| words[(i as usize + w * 7) % words.len()]).collect();
        let body_s = body.join(" ").replace("'", "''");
        exec(&db, &format!("INSERT INTO docs VALUES ({}, 'Doc {}', '{}')", i, i, body_s));
    }
    let insert_ms = t0.elapsed().as_millis();
    println!("  INSERT: {}ms ({:.0} ops/s)", insert_ms, n as f64 / (insert_ms as f64 / 1000.0));

    db.flush().expect("flush");
    db.checkpoint().expect("checkpoint");
    std::thread::sleep(std::time::Duration::from_millis(500));
    println!("  Memory: {:.1} MB (Δ = {:.1} MB)", get_rss_mb(), get_rss_mb() - rss0);

    // MATCH AGAINST
    println!("\n  --- MATCH AGAINST (BM25) ---");
    sep();

    let queries = [
        ("database index", "2 common terms"),
        ("vector embedding neural", "3 keywords"),
        ("rust memory concurrent", "3 keywords"),
        ("spatial geometry point", "3 keywords"),
        ("nonexistent_xyz", "no match"),
    ];

    for (q, desc) in &queries {
        let mut lat: Vec<u64> = Vec::with_capacity(200);
        for _ in 0..200 {
            let sql = format!(
                "SELECT id, title, MATCH(body) AGAINST('{}') AS score \
                 FROM docs WHERE MATCH(body) AGAINST('{}') ORDER BY score DESC LIMIT 10",
                q, q
            );
            let t = Instant::now();
            let _ = exec(&db, &sql);
            lat.push(t.elapsed().as_micros() as u64);
        }
        print_latency(&format!("MATCH AGAINST '{}' [{}]", q, desc), &lat);
    }

    // Direct API
    println!("\n  --- Direct API ---");
    sep();
    let mut api_lat: Vec<u64> = Vec::with_capacity(200);
    for _ in 0..200 {
        let t = Instant::now();
        let _ = db.text_search_ranked("docs_body", "database index", 10);
        api_lat.push(t.elapsed().as_micros() as u64);
    }
    print_latency("text_search_ranked() top-10 (200 queries)", &api_lat);

    println!("\n  Memory after text benchmark: {:.1} MB", get_rss_mb());
}

// ============================================================================
// Test 4: Multimodal Combined Memory
// ============================================================================

#[test]
fn bench_multimodal_memory() {
    println!("\n{}", "=".repeat(105));
    println!("  Multimodal Memory Footprint (Vector + Spatial + Text Combined)");
    println!("{}", "=".repeat(105));

    let (db, _dir) = create_db();
    let rss0 = get_rss_mb();
    println!("  Baseline: {:.1} MB", rss0);

    // Create tables + indexes
    exec(&db, "CREATE TABLE vecs (id INTEGER PRIMARY KEY, emb VECTOR(64))");
    exec(&db, "CREATE TABLE pts (id INTEGER PRIMARY KEY, loc GEOMETRY)");
    exec(&db, "CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)");

    exec(&db, "CREATE VECTOR INDEX vecs_emb ON vecs(emb)");
    exec(&db, "CREATE SPATIAL INDEX pts_loc ON pts(loc)");
    exec(&db, "CREATE TEXT INDEX docs_body ON docs(body)");

    println!("  After CREATE: {:.1} MB", get_rss_mb());

    let words = ["database", "search", "vector", "index", "query", "spatial"];

    // Insert 5 rounds × 2000 rows × 3 tables
    for round in 1..=5 {
        let start = (round - 1) * 2000 + 1;
        let end = round * 2000;

        for i in start..=end {
            // Vector (64-dim)
            let mut v = String::from('[');
            for d in 0..64 {
                if d > 0 { v.push_str(", "); }
                v.push_str(&format!("{:.3}", ((i * 17 + d * 31) as f64).sin()));
            }
            v.push(']');
            exec(&db, &format!("INSERT INTO vecs VALUES ({}, {})", i, v));

            // Spatial point
            let x = 116.0 + (i as f64 % 10000.0) / 10000.0;
            let y = 39.0 + (i as f64 % 10000.0) / 20000.0 + 0.5;
            exec(&db, &format!("INSERT INTO pts VALUES ({}, POINT({}, {}))", i, x, y));

            // Text
            let body: Vec<&str> = (0..8).map(|w| words[(i as usize + w) % words.len()]).collect();
            exec(&db, &format!("INSERT INTO docs VALUES ({}, '{}')", i, body.join(" ")));
        }

        db.flush().expect("flush");
        let rss = get_rss_mb();
        let total = round * 2000;
        println!("  Round {} ({}K rows × 3 tables): {:.1} MB (Δ = {:.1} MB)",
            round, total / 1000, rss, rss - rss0);
    }

    // Final checkpoint
    db.checkpoint().expect("checkpoint");
    std::thread::sleep(std::time::Duration::from_millis(500));
    println!("\n  After final checkpoint: {:.1} MB", get_rss_mb());

    // Query memory impact
    println!("\n  --- Query Phase (memory impact) ---");
    sep();
    let rss_q0 = get_rss_mb();

    // Vector searches
    for _ in 0..50 {
        let q: Vec<f32> = (0..64).map(|i| (i as f32 * 0.1).sin()).collect();
        let _ = db.vector_search("vecs_emb", &q, 10);
    }
    println!("  After 50 vector searches: {:.1} MB (Δ = {:.1} MB)", get_rss_mb(), get_rss_mb() - rss_q0);

    // Spatial queries (reduced count: each query scans ~10K rows from SSTable)
    let spatial_start = std::time::Instant::now();
    for i in 0..5 {
        let t = std::time::Instant::now();
        let _ = exec(&db, "SELECT * FROM pts WHERE ST_WITHIN(loc, 116.0, 39.5, 117.0, 40.5)");
        println!("  Spatial query {}: {:?}", i + 1, t.elapsed());
    }
    println!("  After 5 spatial queries: {:.1} MB ({:?} total)", get_rss_mb(), spatial_start.elapsed());

    // Text searches
    for _ in 0..50 {
        let _ = exec(&db, "SELECT * FROM docs WHERE MATCH(body) AGAINST('vector search') LIMIT 10");
    }
    println!("  After 50 text searches: {:.1} MB", get_rss_mb());

    println!("\n  Final: {:.1} MB (total Δ = {:.1} MB for 10K rows × 3 modalities)", get_rss_mb(), get_rss_mb() - rss0);
}
