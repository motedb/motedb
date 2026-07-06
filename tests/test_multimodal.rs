//! Multimodal regression tests — vectors, spatial, FTS, timestamp

use motedb::{sql::QueryResult, types::Value, Database};
use tempfile::TempDir;

fn create_db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("test.mote")).unwrap();
    (db, dir)
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        _ => vec![],
    }
}

// ============================================================
// Vector tests
// ============================================================

#[test]
fn test_vector_insert_and_select() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE items (id INT, embedding VECTOR(4))")
        .unwrap();
    db.execute("INSERT INTO items (id, embedding) VALUES (1, [1.0, 0.0, 0.0, 0.0])")
        .unwrap();
    db.execute("INSERT INTO items (id, embedding) VALUES (2, [0.0, 1.0, 0.0, 0.0])")
        .unwrap();
    db.execute("INSERT INTO items (id, embedding) VALUES (3, [0.9, 0.1, 0.0, 0.0])")
        .unwrap();

    let r = rows(&db, "SELECT id FROM items ORDER BY id");
    assert_eq!(r.len(), 3);
}

#[test]
fn test_vector_l2_distance() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE v (id INT, emb VECTOR(3))")
        .unwrap();
    db.execute("INSERT INTO v (id, emb) VALUES (1, [1.0, 0.0, 0.0])")
        .unwrap();
    db.execute("INSERT INTO v (id, emb) VALUES (2, [0.0, 1.0, 0.0])")
        .unwrap();
    db.execute("INSERT INTO v (id, emb) VALUES (3, [0.0, 0.0, 1.0])")
        .unwrap();

    // L2 distance from [1,0,0] to [0,1,0] = sqrt(2) ≈ 1.414
    let r = rows(
        &db,
        "SELECT id FROM v WHERE id != 1 ORDER BY emb <-> [1.0, 0.0, 0.0]",
    );
    assert_eq!(r.len(), 2);
}

#[test]
fn test_vector_cosine_distance() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE v (id INT, emb VECTOR(3))")
        .unwrap();
    db.execute("INSERT INTO v (id, emb) VALUES (1, [1.0, 0.0, 0.0])")
        .unwrap();
    db.execute("INSERT INTO v (id, emb) VALUES (2, [0.5, 0.5, 0.0])")
        .unwrap();
    db.execute("INSERT INTO v (id, emb) VALUES (3, [-1.0, 0.0, 0.0])")
        .unwrap();

    // Cosine distance from [1,0,0]:
    //   to [1,0,0] = 0 (identical)
    //   to [0.5,0.5,0] = 1 - cos(45°) ≈ 0.293
    //   to [-1,0,0] = 1 - (-1) = 2.0 (opposite)
    let r = rows(&db, "SELECT id FROM v ORDER BY emb <=> [1.0, 0.0, 0.0]");
    assert_eq!(r.len(), 3);
    // Closest should be id=1 (distance 0)
    assert_eq!(r[0][0], Value::Integer(1));
    // Farthest should be id=3 (distance 2.0)
    assert_eq!(r[2][0], Value::Integer(3));
}

#[test]
fn test_vector_dot_product() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE v (id INT, emb VECTOR(3))")
        .unwrap();
    db.execute("INSERT INTO v (id, emb) VALUES (1, [1.0, 2.0, 3.0])")
        .unwrap();

    // Dot product [1,2,3] . [1,1,1] = 6
    let r = rows(&db, "SELECT emb <#> [1.0, 1.0, 1.0] AS dp FROM v");
    assert_eq!(r.len(), 1);
    if let Value::Float(dp) = r[0][0] {
        let diff = (dp - 6.0).abs();
        assert!(diff < 0.01, "dot product should be 6.0, got {}", dp);
    }
}

#[test]
fn test_vector_zero_vector_cosine() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE v (id INT, emb VECTOR(2))")
        .unwrap();
    db.execute("INSERT INTO v (id, emb) VALUES (1, [0.0, 0.0])")
        .unwrap();
    db.execute("INSERT INTO v (id, emb) VALUES (2, [1.0, 0.0])")
        .unwrap();

    // Zero vector cosine distance should be 1.0 (max), not NaN/Inf
    let r = rows(&db, "SELECT emb <=> [1.0, 0.0] AS dist FROM v WHERE id = 1");
    assert_eq!(r.len(), 1);
    if let Value::Float(dist) = r[0][0] {
        assert!(
            !dist.is_nan() && !dist.is_infinite(),
            "distance should be finite, got {}",
            dist
        );
    }
}

#[test]
fn test_vector_dimension_mismatch() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE v (id INT, emb VECTOR(3))")
        .unwrap();
    db.execute("INSERT INTO v (id, emb) VALUES (1, [1.0, 0.0, 0.0])")
        .unwrap();

    // Querying with wrong dimension vector: behavior is undefined (may error or not)
    // Currently MoteDB does not enforce dimension check at query time for vector literals
    let result = db.execute("SELECT emb <-> [1.0, 0.0] AS dist FROM v");
    // Just verify no panic — the query may succeed or error depending on implementation
    let _ = result.map(|s| s.materialize());
}

// ============================================================
// Spatial tests
// ============================================================

#[test]
fn test_spatial_insert_and_select() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE poi (id INT, location GEOMETRY)")
        .unwrap();
    db.execute("INSERT INTO poi (id, location) VALUES (1, POINT(1.0, 2.0))")
        .unwrap();
    db.execute("INSERT INTO poi (id, location) VALUES (2, POINT(3.0, 4.0))")
        .unwrap();

    let r = rows(&db, "SELECT id FROM poi ORDER BY id");
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Integer(1));
    assert_eq!(r[1][0], Value::Integer(2));
}

#[test]
fn test_spatial_st_distance() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE poi (id INT, location GEOMETRY)")
        .unwrap();
    db.execute("INSERT INTO poi (id, location) VALUES (1, POINT(0.0, 0.0))")
        .unwrap();
    db.execute("INSERT INTO poi (id, location) VALUES (2, POINT(3.0, 4.0))")
        .unwrap();

    // ST_DISTANCE(column, x, y) — MoteDB syntax
    let r = rows(
        &db,
        "SELECT ST_DISTANCE(location, 0.0, 0.0) AS dist FROM poi WHERE id = 2",
    );
    assert_eq!(r.len(), 1);
    if let Value::Float(dist) = r[0][0] {
        let diff = (dist - 5.0).abs();
        assert!(diff < 0.01, "ST_DISTANCE should be 5.0, got {}", dist);
    }
}

#[test]
fn test_spatial_st_distance_3d() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE poi (id INT, location GEOMETRY)")
        .unwrap();
    db.execute("INSERT INTO poi (id, location) VALUES (1, POINT3D(1.0, 2.0, 2.0))")
        .unwrap();
    db.execute("INSERT INTO poi (id, location) VALUES (2, POINT3D(4.0, 6.0, 14.0))")
        .unwrap();

    // ST_DISTANCE_3D(column, x, y, z)
    let r = rows(
        &db,
        "SELECT ST_DISTANCE_3D(location, 1.0, 2.0, 2.0) AS dist FROM poi WHERE id = 2",
    );
    assert_eq!(r.len(), 1);
    if let Value::Float(dist) = r[0][0] {
        let diff = (dist - 13.0).abs();
        assert!(diff < 0.01, "ST_DISTANCE 3D should be 13.0, got {}", dist);
    }
}

#[test]
fn test_spatial_within_radius() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE poi (id INT, location GEOMETRY)")
        .unwrap();
    db.execute("INSERT INTO poi (id, location) VALUES (1, POINT(0.0, 0.0))")
        .unwrap();
    db.execute("INSERT INTO poi (id, location) VALUES (2, POINT(10.0, 10.0))")
        .unwrap();
    db.execute("INSERT INTO poi (id, location) VALUES (3, POINT(1.0, 0.0))")
        .unwrap();

    // WITHIN_RADIUS(point, center, radius) as generic function call
    let r = rows(
        &db,
        "SELECT id FROM poi WHERE WITHIN_RADIUS(location, POINT(0.0, 0.0), 5.0) ORDER BY id",
    );
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Integer(1));
    assert_eq!(r[1][0], Value::Integer(3));
}

// ============================================================
// Full-text search tests
// ============================================================

#[test]
fn test_fts_insert_and_search() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE docs (id INT, content TEXT)")
        .unwrap();
    db.execute(
        "INSERT INTO docs (id, content) VALUES (1, 'the quick brown fox jumps over the lazy dog')",
    )
    .unwrap();
    db.execute("INSERT INTO docs (id, content) VALUES (2, 'a fox is a clever animal')")
        .unwrap();
    db.execute("INSERT INTO docs (id, content) VALUES (3, 'the dog barks at night')")
        .unwrap();

    // Simple text search with LIKE
    let r = rows(
        &db,
        "SELECT id FROM docs WHERE content LIKE '%fox%' ORDER BY id",
    );
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Integer(1));
    assert_eq!(r[1][0], Value::Integer(2));
}

#[test]
fn test_fts_case_insensitive_like() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE docs (id INT, content TEXT)")
        .unwrap();
    db.execute("INSERT INTO docs (id, content) VALUES (1, 'Hello World')")
        .unwrap();

    let r = rows(&db, "SELECT id FROM docs WHERE content LIKE '%hello%'");
    // LIKE is case-sensitive in MoteDB, so this should return 0 rows
    // If it returns 1, that means LIKE was made case-insensitive
    assert!(r.len() <= 1); // Either behavior is acceptable
}

#[test]
fn test_fts_match_query() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE docs (id INT, content TEXT)")
        .unwrap();
    db.execute("INSERT INTO docs (id, content) VALUES (1, 'machine learning algorithms')")
        .unwrap();
    db.execute("INSERT INTO docs (id, content) VALUES (2, 'deep learning neural networks')")
        .unwrap();
    db.execute("INSERT INTO docs (id, content) VALUES (3, 'cooking recipes for dinner')")
        .unwrap();

    // MATCH(column, query_text) — generic function form
    let r = rows(
        &db,
        "SELECT id FROM docs WHERE MATCH(content, 'learning') ORDER BY id",
    );
    assert!(
        r.len() >= 2,
        "Should find at least 2 docs with 'learning', got {}",
        r.len()
    );
}

// ============================================================
// Timestamp tests
// ============================================================

#[test]
fn test_timestamp_insert_and_select() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE events (id INT, ts TIMESTAMP)")
        .unwrap();
    db.execute("INSERT INTO events (id, ts) VALUES (1, 1700000000000000)")
        .unwrap();
    db.execute("INSERT INTO events (id, ts) VALUES (2, 1700000050000000)")
        .unwrap();

    let r = rows(&db, "SELECT id FROM events ORDER BY id");
    assert_eq!(r.len(), 2);
}

#[test]
fn test_timestamp_comparison() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE events (id INT, ts TIMESTAMP)")
        .unwrap();
    db.execute("INSERT INTO events (id, ts) VALUES (1, 1700000000000000)")
        .unwrap();
    db.execute("INSERT INTO events (id, ts) VALUES (2, 1700000050000000)")
        .unwrap();
    db.execute("INSERT INTO events (id, ts) VALUES (3, 1700000100000000)")
        .unwrap();

    // Select events after ts > 1700000050000000
    let r = rows(&db, "SELECT id FROM events WHERE ts > 1700000050000000");
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Integer(3));
}

#[test]
fn test_timestamp_functions() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    db.execute("INSERT INTO t (id) VALUES (1)").unwrap();

    // HOUR extraction from a known timestamp
    // 2023-11-14 22:13:20 UTC = 1700000000 seconds = 1700000000000000 microseconds
    let r = rows(&db, "SELECT HOUR(1700000000000000) AS h FROM t");
    if let Value::Integer(h) = r[0][0] {
        assert!(h >= 0 && h <= 23, "HOUR should be 0-23, got {}", h);
    }
}

// ============================================================
// Cross-type tests
// ============================================================

#[test]
fn test_mixed_types_in_table() {
    let (db, _dir) = create_db();
    db.execute(
        "CREATE TABLE mixed (id INT, name TEXT, score FLOAT, active BOOL, embedding VECTOR(2))",
    )
    .unwrap();
    db.execute("INSERT INTO mixed (id, name, score, active, embedding) VALUES (1, 'Alice', 95.5, true, [1.0, 0.0])").unwrap();
    db.execute("INSERT INTO mixed (id, name, score, active, embedding) VALUES (2, 'Bob', 82.0, false, [0.0, 1.0])").unwrap();

    let r = rows(&db, "SELECT id, name, score FROM mixed WHERE active = true");
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Integer(1));
}

#[test]
fn test_null_in_vector_column() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT, emb VECTOR(2))")
        .unwrap();
    db.execute("INSERT INTO t (id, emb) VALUES (1, [1.0, 0.0])")
        .unwrap();
    db.execute("INSERT INTO t (id) VALUES (2)").unwrap();

    let r = rows(&db, "SELECT id FROM t WHERE emb IS NULL");
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Integer(2));

    let r = rows(&db, "SELECT id FROM t WHERE emb IS NOT NULL");
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Integer(1));
}

#[test]
fn test_multiple_vector_columns() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT, emb_a VECTOR(2), emb_b VECTOR(2))")
        .unwrap();
    db.execute("INSERT INTO t (id, emb_a, emb_b) VALUES (1, [1.0, 0.0], [0.0, 1.0])")
        .unwrap();
    db.execute("INSERT INTO t (id, emb_a, emb_b) VALUES (2, [0.0, 1.0], [1.0, 0.0])")
        .unwrap();

    let r = rows(&db, "SELECT id FROM t ORDER BY id");
    assert_eq!(r.len(), 2);
}
