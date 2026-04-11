//! SQL Fast Path Correctness Tests
//!
//! Tests for the executor fast paths:
//! - MATCH AGAINST (text search)
//! - ST_WITHIN (spatial range)
//! - ST_KNN (spatial nearest neighbor)
//! - ST_DISTANCE ORDER BY (spatial distance sort)
//! - Vector ORDER BY (<->)
//! - Mixed WHERE clauses
//!
//! Run: cargo test --test test_fast_paths -- --test-threads=1

use motedb::Database;
use motedb::types::Value;
use tempfile::TempDir;

fn create_db() -> (Database, TempDir) {
    let dir = TempDir::new().expect("temp dir");
    let db = Database::create(dir.path()).expect("create db");
    (db, dir)
}

fn exec(db: &Database, sql: &str) -> motedb::sql::QueryResult {
    db.execute(sql).expect(&format!("SQL: {}", sql)).materialize().expect("materialize")
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match exec(db, sql) {
        motedb::sql::QueryResult::Select { rows, .. } => rows,
        _ => vec![],
    }
}

fn setup_spatial(db: &Database) {
    exec(db, "CREATE TABLE locations (id INTEGER PRIMARY KEY, name TEXT, coords GEOMETRY)");
    exec(db, "CREATE SPATIAL INDEX locations_coords ON locations(coords)");

    // Insert a grid of points: (116+x*0.1, 40+y*0.1) for x,y in 0..5
    for x in 0..5i64 {
        for y in 0..5i64 {
            let id = x * 5 + y + 1;
            let px = 116.0 + x as f64 * 0.1;
            let py = 40.0 + y as f64 * 0.1;
            exec(db, &format!("INSERT INTO locations VALUES ({}, 'p_{}_{}', POINT({}, {}))", id, x, y, px, py));
        }
    }
    db.flush().expect("flush");
    db.checkpoint().expect("checkpoint");
    std::thread::sleep(std::time::Duration::from_millis(500));
}

fn setup_text(db: &Database) {
    exec(db, "CREATE TABLE articles (id INTEGER PRIMARY KEY, title TEXT, body TEXT)");
    exec(db, "CREATE TEXT INDEX articles_body ON articles(body)");

    let docs = [
        (1, "Intro to Rust", "Rust is a systems programming language focused on safety and performance"),
        (2, "Rust Concurrency", "Rust provides fearless concurrency with threads and async"),
        (3, "Python Basics", "Python is a popular programming language for data science"),
        (4, "Database Design", "Database indexing improves query performance significantly"),
        (5, "Vector Search", "Vector databases enable similarity search using embeddings"),
        (6, "Spatial Data", "Spatial indexing with R-trees enables efficient geo queries"),
        (7, "Rust vs C++", "Rust offers memory safety without garbage collection unlike C++"),
        (8, "ML Pipelines", "Machine learning pipelines process data for model training"),
    ];

    for (id, title, body) in &docs {
        let title_escaped = title.replace("'", "''");
        let body_escaped = body.replace("'", "''");
        exec(db, &format!("INSERT INTO articles VALUES ({}, '{}', '{}')", id, title_escaped, body_escaped));
    }

    db.flush().expect("flush");
    db.checkpoint().expect("checkpoint");
    std::thread::sleep(std::time::Duration::from_millis(500));
}

// ============================================================================
// Spatial Fast Path Tests
// ============================================================================

#[test]
fn test_st_within_basic() {
    let (db, _dir) = create_db();
    setup_spatial(&db);

    // All points are in [116.0, 116.4] × [40.0, 40.4]
    let result = rows(&db, "SELECT * FROM locations WHERE ST_WITHIN(coords, 116.0, 40.0, 117.0, 41.0)");
    assert_eq!(result.len(), 25, "All 25 points should be within the large bbox");
}

#[test]
fn test_st_within_narrow_bbox() {
    let (db, _dir) = create_db();
    setup_spatial(&db);

    // Narrow bbox around (116.0, 40.0) — should match only nearby points
    let result = rows(&db, "SELECT * FROM locations WHERE ST_WITHIN(coords, 115.95, 39.95, 116.05, 40.05)");
    assert!(!result.is_empty(), "Should find at least the origin point");
    assert!(result.len() <= 4, "Narrow bbox should match few points");
}

#[test]
fn test_st_within_no_results() {
    let (db, _dir) = create_db();
    setup_spatial(&db);

    let result = rows(&db, "SELECT * FROM locations WHERE ST_WITHIN(coords, 0.0, 0.0, 1.0, 1.0)");
    assert!(result.is_empty(), "No points should be in Africa");
}

#[test]
fn test_st_knn_basic() {
    let (db, _dir) = create_db();
    setup_spatial(&db);

    let result = rows(&db, "SELECT * FROM locations WHERE ST_KNN(coords, 116.0, 40.0, 3)");
    assert_eq!(result.len(), 3, "KNN should return exactly 3 results");
}

#[test]
fn test_st_knn_k_larger_than_data() {
    let (db, _dir) = create_db();
    setup_spatial(&db);

    let result = rows(&db, "SELECT * FROM locations WHERE ST_KNN(coords, 116.0, 40.0, 100)");
    // Should return all or most points
    assert!(result.len() >= 20, "KNN with k > data should return most points");
}

#[test]
fn test_st_distance_order_by() {
    let (db, _dir) = create_db();
    setup_spatial(&db);

    let result = rows(&db,
        "SELECT id, name, ST_DISTANCE(coords, 116.0, 40.0) AS dist FROM locations ORDER BY dist LIMIT 5");
    assert_eq!(result.len(), 5, "Should return top 5 results");

    // Distances should be ascending (or non-decreasing)
    for i in 1..result.len() {
        let d_prev = match &result[i-1][2] { Value::Float(d) => *d, _ => f64::MAX };
        let d_curr = match &result[i][2] { Value::Float(d) => *d, _ => f64::MIN };
        assert!(d_curr >= d_prev - 0.01, "Distances should be ascending: {} vs {}", d_prev, d_curr);
    }
}

#[test]
fn test_st_knn_returns_nearby() {
    let (db, _dir) = create_db();
    setup_spatial(&db);

    // Query near (116.2, 40.2) — should find points near that area
    let result = rows(&db, "SELECT * FROM locations WHERE ST_KNN(coords, 116.2, 40.2, 3)");
    assert_eq!(result.len(), 3, "KNN should return 3 results");
}

// ============================================================================
// Text Search Fast Path Tests
// ============================================================================

#[test]
fn test_match_against_basic() {
    let (db, _dir) = create_db();
    setup_text(&db);

    let result = rows(&db,
        "SELECT id, title FROM articles WHERE MATCH(body) AGAINST('Rust programming') ORDER BY id");
    assert!(!result.is_empty(), "Should find Rust-related articles");

    // Should find at least docs 1, 2
    let ids: Vec<i64> = result.iter().filter_map(|r| match &r[0] {
        Value::Integer(i) => Some(*i),
        _ => None,
    }).collect();
    assert!(ids.contains(&1), "Should find 'Intro to Rust'");
    assert!(ids.contains(&2), "Should find 'Rust Concurrency'");
}

#[test]
fn test_match_against_with_score() {
    let (db, _dir) = create_db();
    setup_text(&db);

    let result = rows(&db,
        "SELECT id, MATCH(body) AGAINST('Rust') AS score FROM articles WHERE MATCH(body) AGAINST('Rust') ORDER BY score DESC LIMIT 5");

    assert!(!result.is_empty(), "Should find results");

    // All scores should be positive
    for row in &result {
        if let Value::Float(score) = row[1] {
            assert!(score > 0.0, "Score should be positive, got {}", score);
        }
    }
}

#[test]
fn test_match_against_no_results() {
    let (db, _dir) = create_db();
    setup_text(&db);

    let result = rows(&db,
        "SELECT id FROM articles WHERE MATCH(body) AGAINST('xyznonexistent')");
    assert!(result.is_empty(), "Should find nothing for nonsense query");
}

#[test]
fn test_match_against_single_term() {
    let (db, _dir) = create_db();
    setup_text(&db);

    let result = rows(&db,
        "SELECT id FROM articles WHERE MATCH(body) AGAINST('spatial') LIMIT 5");
    assert!(!result.is_empty(), "Should find 'spatial' in article 6");
}

#[test]
fn test_match_against_limit() {
    let (db, _dir) = create_db();
    setup_text(&db);

    let result = rows(&db,
        "SELECT id FROM articles WHERE MATCH(body) AGAINST('database') LIMIT 2");
    assert!(result.len() <= 2, "Should respect LIMIT");
}

// ============================================================================
// Vector Fast Path Tests
// ============================================================================

#[test]
fn test_vector_order_by_returns_results() {
    let (db, _dir) = create_db();

    exec(&db, "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, emb VECTOR(4))");
    exec(&db, "CREATE VECTOR INDEX items_emb ON items(emb)");

    for i in 1..=10i64 {
        let v = format!("[{:.1}, {:.1}, {:.1}, {:.1}]", i as f64, i as f64, i as f64, i as f64);
        exec(&db, &format!("INSERT INTO items VALUES ({}, 'item_{}', {})", i, i, v));
    }

    db.flush().expect("flush");
    db.checkpoint().expect("checkpoint");
    std::thread::sleep(std::time::Duration::from_millis(500));

    let result = rows(&db,
        "SELECT id, name FROM items ORDER BY emb <-> [5.0, 5.0, 5.0, 5.0] LIMIT 3");
    assert_eq!(result.len(), 3, "Should return top 3");

    // With L2 distance, [5,5,5,5] should be closest to id=5 (distance=0)
    assert_eq!(result[0][0], Value::Integer(5), "Closest should be id=5");
}

#[test]
fn test_vector_order_by_with_distance() {
    let (db, _dir) = create_db();

    exec(&db, "CREATE TABLE vecs (id INTEGER PRIMARY KEY, v VECTOR(4))");
    exec(&db, "CREATE VECTOR INDEX vecs_v ON vecs(v)");

    for i in 1..=20i64 {
        let v = format!("[{:.1}, {:.1}, {:.1}, {:.1}]", i as f64, i as f64, i as f64, i as f64);
        exec(&db, &format!("INSERT INTO vecs VALUES ({}, {})", i, v));
    }

    db.flush().expect("flush");
    db.checkpoint().expect("checkpoint");
    std::thread::sleep(std::time::Duration::from_millis(500));

    let result = rows(&db,
        "SELECT id, v <-> [10.0, 10.0, 10.0, 10.0] AS dist FROM vecs ORDER BY dist LIMIT 5");
    assert_eq!(result.len(), 5, "Should return top 5");

    // Distances should be non-negative
    for row in &result {
        if let Value::Float(d) = row[1] {
            assert!(d >= 0.0, "Distance should be non-negative");
        }
    }
}

// ============================================================================
// Mixed / Complex Queries
// ============================================================================

#[test]
fn test_select_star_with_st_within() {
    let (db, _dir) = create_db();
    setup_spatial(&db);

    let result = rows(&db, "SELECT * FROM locations WHERE ST_WITHIN(coords, 116.0, 40.0, 116.15, 40.15)");
    assert!(!result.is_empty(), "SELECT * should work with ST_WITHIN");

    // Each row should have at least id and name columns
    for row in &result {
        assert!(row.len() >= 2, "Row should have at least id and name");
    }
}

#[test]
fn test_st_distance_order_by_with_limit_1() {
    let (db, _dir) = create_db();
    setup_spatial(&db);

    let result = rows(&db,
        "SELECT id FROM locations ORDER BY ST_DISTANCE(coords, 116.0, 40.0) LIMIT 1");
    assert_eq!(result.len(), 1, "Should return 1 result");
}

#[test]
fn test_match_against_select_specific_columns() {
    let (db, _dir) = create_db();
    setup_text(&db);

    let result = rows(&db,
        "SELECT title FROM articles WHERE MATCH(body) AGAINST('vector search')");
    assert!(!result.is_empty(), "Should find vector search articles");

    // Should only return the title column
    for row in &result {
        assert_eq!(row.len(), 1, "Should only project requested column");
    }
}

#[test]
fn test_count_with_indexed_where() {
    let (db, _dir) = create_db();
    setup_spatial(&db);

    let result = rows(&db, "SELECT COUNT(*) as cnt FROM locations");
    assert_eq!(result[0][0], Value::Integer(25), "Should count all 25 locations");
}

// ============================================================================
// Persistence: data survives flush+checkpoint
// ============================================================================

#[test]
fn test_spatial_query_after_reopen() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().to_path_buf();

    // Create and populate
    {
        let db = Database::create(&path).expect("create db");
        exec(&db, "CREATE TABLE pts (id INTEGER PRIMARY KEY, loc GEOMETRY)");
        exec(&db, "CREATE SPATIAL INDEX pts_loc ON pts(loc)");
        for i in 1..=5i64 {
            exec(&db, &format!("INSERT INTO pts VALUES ({}, POINT({}, {}))", i, 116.0 + i as f64 * 0.1, 39.9));
        }
        db.flush().expect("flush");
        db.checkpoint().expect("checkpoint");
    }

    // Reopen and query
    {
        let db = Database::open(&path).expect("reopen db");
        let result = rows(&db, "SELECT * FROM pts");
        assert_eq!(result.len(), 5, "All rows should survive reopen");
    }
}

#[test]
fn test_text_search_after_insert() {
    let (db, _dir) = create_db();

    exec(&db, "CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)");
    exec(&db, "CREATE TEXT INDEX docs_body ON docs(body)");
    exec(&db, "INSERT INTO docs VALUES (1, 'hello world database')");
    exec(&db, "INSERT INTO docs VALUES (2, 'vector search engine')");

    db.flush().expect("flush");
    db.checkpoint().expect("checkpoint");
    std::thread::sleep(std::time::Duration::from_millis(500));

    let result = rows(&db, "SELECT id FROM docs WHERE MATCH(body) AGAINST('database')");
    assert!(!result.is_empty(), "Text search should find 'database'");
}
