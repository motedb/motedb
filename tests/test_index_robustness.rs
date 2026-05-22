//! Index Robustness Tests
//!
//! Tests for index operations with:
//! - Table/column names containing underscores
//! - Custom index names
//! - DROP INDEX cleanup
//! - Stale index marking
//!
//! Run: cargo test --test test_index_robustness -- --test-threads=1

use motedb::Database;
use motedb::types::Value;
use tempfile::TempDir;

fn create_db() -> (Database, TempDir) {
    let dir = TempDir::new().expect("temp dir");
    let db = Database::create(dir.path()).expect("create db");
    (db, dir)
}

fn exec(db: &Database, sql: &str) -> motedb::sql::QueryResult {
    db.execute(sql).unwrap_or_else(|_| panic!("SQL: {}", sql)).materialize().expect("materialize")
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match exec(db, sql) {
        motedb::sql::QueryResult::Select { rows, .. } => rows,
        _ => vec![],
    }
}

// ============================================================================
// 1. Underscore table/column names
// ============================================================================

#[test]
fn test_vector_index_underscore_table_name() {
    let (db, _dir) = create_db();

    exec(&db, "CREATE TABLE user_profiles (id INTEGER PRIMARY KEY, emb VECTOR(4))");
    exec(&db, "CREATE VECTOR INDEX user_profiles_emb ON user_profiles(emb)");

    // Insert vectors
    for i in 1..=10i64 {
        let v = format!("[{:.1}, {:.1}, {:.1}, {:.1}]", i as f64, (i+1) as f64, (i+2) as f64, (i+3) as f64);
        exec(&db, &format!("INSERT INTO user_profiles VALUES ({}, {})", i, v));
    }

    db.flush().expect("flush");
    db.checkpoint().expect("checkpoint");
    db.wait_for_indexes_ready();

    // Vector search should work
    let query = vec![1.0, 2.0, 3.0, 4.0];
    let results = db.vector_search("user_profiles_emb", &query, 3).expect("vector search");
    assert!(!results.is_empty(), "Should find vectors via index");
    assert!(results.len() <= 3, "Should return at most 3 results");
}

#[test]
fn test_spatial_index_underscore_column_name() {
    let (db, _dir) = create_db();

    exec(&db, "CREATE TABLE store_locations (id INTEGER PRIMARY KEY, geo_coords GEOMETRY)");
    exec(&db, "CREATE SPATIAL INDEX store_geo_coords ON store_locations(geo_coords)");

    for i in 1..=20i64 {
        let x = 116.0 + (i as f64) * 0.01;
        let y = 39.9 + (i as f64) * 0.01;
        exec(&db, &format!("INSERT INTO store_locations VALUES ({}, POINT({}, {}))", i, x, y));
    }

    db.flush().expect("flush");
    db.checkpoint().expect("checkpoint");
    db.wait_for_indexes_ready();

    // Spatial queries should work
    let result = rows(&db, "SELECT * FROM store_locations WHERE ST_WITHIN(geo_coords, 116.0, 39.9, 116.3, 40.3)");
    assert!(!result.is_empty(), "ST_WITHIN should find points");

    let knn = rows(&db, "SELECT * FROM store_locations WHERE ST_KNN(geo_coords, 116.1, 39.9, 3)");
    assert!(!knn.is_empty(), "ST_KNN should find neighbors");
}

#[test]
fn test_text_index_underscore_names() {
    let (db, _dir) = create_db();

    exec(&db, "CREATE TABLE doc_archive (id INTEGER PRIMARY KEY, full_body TEXT)");
    exec(&db, "CREATE TEXT INDEX doc_full_body ON doc_archive(full_body)");

    for i in 1..=10i64 {
        let body = format!("document number {} about database and search", i);
        exec(&db, &format!("INSERT INTO doc_archive VALUES ({}, '{}')", i, body));
    }

    db.flush().expect("flush");
    db.checkpoint().expect("checkpoint");
    db.wait_for_indexes_ready();

    let result = rows(&db,
        "SELECT id FROM doc_archive WHERE MATCH(full_body) AGAINST('database search') LIMIT 5");
    assert!(!result.is_empty(), "MATCH AGAINST should find docs");
}

#[test]
fn test_double_underscore_in_name() {
    let (db, _dir) = create_db();

    exec(&db, "CREATE TABLE my__special__table (id INTEGER PRIMARY KEY, my__col TEXT)");
    exec(&db, "INSERT INTO my__special__table VALUES (1, 'hello')");

    let result = rows(&db, "SELECT * FROM my__special__table WHERE id = 1");
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][1], Value::text("hello".to_string()));
}

// ============================================================================
// 2. Custom index names (user-specified via SQL)
// ============================================================================

#[test]
fn test_custom_column_index_name() {
    let (db, _dir) = create_db();

    exec(&db, "CREATE TABLE products (id INTEGER PRIMARY KEY, category TEXT, price FLOAT)");
    exec(&db, "CREATE INDEX my_cat_idx ON products(category)");

    exec(&db, "INSERT INTO products VALUES (1, 'electronics', 99.9)");
    exec(&db, "INSERT INTO products VALUES (2, 'books', 15.0)");
    exec(&db, "INSERT INTO products VALUES (3, 'electronics', 199.0)");

    db.flush().expect("flush");
    db.checkpoint().expect("checkpoint");
    db.wait_for_indexes_ready();

    // Point query should use column index
    let result = rows(&db, "SELECT * FROM products WHERE category = 'electronics'");
    assert_eq!(result.len(), 2, "Should find 2 electronics via column index");
}

// ============================================================================
// 3. DROP INDEX cleanup
// ============================================================================

#[test]
fn test_drop_column_index_removes_alias() {
    let (db, _dir) = create_db();

    exec(&db, "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)");
    exec(&db, "CREATE INDEX my_email_idx ON users(email)");

    exec(&db, "INSERT INTO users VALUES (1, 'a@b.com')");
    exec(&db, "INSERT INTO users VALUES (2, 'c@d.com')");

    db.flush().expect("flush");
    db.checkpoint().expect("checkpoint");
    db.wait_for_indexes_ready();

    // Query works with index
    let before = rows(&db, "SELECT * FROM users WHERE email = 'a@b.com'");
    assert_eq!(before.len(), 1);

    // Drop the index
    exec(&db, "DROP INDEX my_email_idx ON users");

    // Table should still be queryable (full scan fallback)
    let after = rows(&db, "SELECT * FROM users");
    assert_eq!(after.len(), 2, "Table data should still be accessible after DROP INDEX");
}

#[test]
fn test_drop_vector_index() {
    let (db, _dir) = create_db();

    exec(&db, "CREATE TABLE vecs (id INTEGER PRIMARY KEY, v VECTOR(4))");
    exec(&db, "CREATE VECTOR INDEX vecs_v ON vecs(v)");

    for i in 1..=5i64 {
        exec(&db, &format!("INSERT INTO vecs VALUES ({}, [{}, {}, {}, {}])", i, i, i+1, i+2, i+3));
    }

    db.flush().expect("flush");
    db.checkpoint().expect("checkpoint");
    db.wait_for_indexes_ready();

    // Search works before drop
    let before = db.vector_search("vecs_v", &[1.0, 2.0, 3.0, 4.0], 3);
    assert!(before.is_ok(), "Vector search should work before drop");

    // Drop index
    exec(&db, "DROP INDEX vecs_v ON vecs");

    // Search should fail after drop
    let after = db.vector_search("vecs_v", &[1.0, 2.0, 3.0, 4.0], 3);
    assert!(after.is_err(), "Vector search should fail after DROP INDEX");
}

#[test]
fn test_drop_text_index() {
    let (db, _dir) = create_db();

    exec(&db, "CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)");
    exec(&db, "CREATE TEXT INDEX docs_body ON docs(body)");

    for i in 1..=5i64 {
        exec(&db, &format!("INSERT INTO docs VALUES ({}, 'test document number {}')", i, i));
    }

    db.flush().expect("flush");
    db.checkpoint().expect("checkpoint");
    db.wait_for_indexes_ready();

    // Drop the text index
    exec(&db, "DROP INDEX docs_body ON docs");

    // Table data should still be accessible
    let result = rows(&db, "SELECT * FROM docs");
    assert_eq!(result.len(), 5, "Table data should survive DROP INDEX");
}

#[test]
fn test_drop_spatial_index() {
    let (db, _dir) = create_db();

    exec(&db, "CREATE TABLE pts (id INTEGER PRIMARY KEY, loc GEOMETRY)");
    exec(&db, "CREATE SPATIAL INDEX pts_loc ON pts(loc)");

    for i in 1..=5i64 {
        exec(&db, &format!("INSERT INTO pts VALUES ({}, POINT({}, {}))", i, 116.0 + i as f64 * 0.01, 39.9));
    }

    db.flush().expect("flush");
    db.checkpoint().expect("checkpoint");
    db.wait_for_indexes_ready();

    // Drop
    exec(&db, "DROP INDEX pts_loc ON pts");

    // Data still exists
    let result = rows(&db, "SELECT * FROM pts");
    assert_eq!(result.len(), 5, "Table data should survive DROP INDEX");
}

// ============================================================================
// 4. Multiple indexes on same table
// ============================================================================

#[test]
fn test_multiple_indexes_on_one_table() {
    let (db, _dir) = create_db();

    exec(&db, "CREATE TABLE records (id INTEGER PRIMARY KEY, category TEXT, priority INTEGER, embedding VECTOR(4))");
    exec(&db, "CREATE INDEX records_category ON records(category)");
    exec(&db, "CREATE INDEX records_priority ON records(priority)");
    exec(&db, "CREATE VECTOR INDEX records_embedding ON records(embedding)");

    for i in 1..=20i64 {
        let cat = if i % 2 == 0 { "A" } else { "B" };
        let pri = i % 3;
        let emb = format!("[{:.1}, {:.1}, {:.1}, {:.1}]", i as f64, i as f64 * 0.5, i as f64 * 0.3, i as f64 * 0.1);
        exec(&db, &format!("INSERT INTO records VALUES ({}, '{}', {}, {})", i, cat, pri, emb));
    }

    db.flush().expect("flush");
    db.checkpoint().expect("checkpoint");
    db.wait_for_indexes_ready();

    // Column index query
    let cat_result = rows(&db, "SELECT * FROM records WHERE category = 'A'");
    assert_eq!(cat_result.len(), 10, "Category A should have 10 records");

    // Priority query
    let pri_result = rows(&db, "SELECT * FROM records WHERE priority = 0");
    assert!(!pri_result.is_empty(), "Should find records with priority 0");

    // Vector search
    let vec_result = db.vector_search("records_embedding", &[1.0, 0.5, 0.3, 0.1], 5).expect("vector search");
    assert!(!vec_result.is_empty(), "Vector search should return results");
}

// ============================================================================
// 5. Index on empty table
// ============================================================================

#[test]
fn test_index_on_empty_table() {
    let (db, _dir) = create_db();

    exec(&db, "CREATE TABLE empty_t (id INTEGER PRIMARY KEY, val TEXT, emb VECTOR(4))");
    exec(&db, "CREATE INDEX empty_t_val ON empty_t(val)");
    exec(&db, "CREATE VECTOR INDEX empty_t_emb ON empty_t(emb)");

    db.flush().expect("flush");
    db.checkpoint().expect("checkpoint");
    db.wait_for_indexes_ready();

    // Queries on empty indexed table should work
    let result = rows(&db, "SELECT * FROM empty_t WHERE val = 'x'");
    assert!(result.is_empty());

    let vec_result = db.vector_search("empty_t_emb", &[1.0, 2.0, 3.0, 4.0], 5);
    assert!(vec_result.is_ok());
    assert!(vec_result.unwrap().is_empty());
}

// ============================================================================
// 6. DROP non-existent index
// ============================================================================

#[test]
fn test_drop_nonexistent_index_errors() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");

    let result = db.execute("DROP INDEX nonexistent_idx ON t");
    assert!(result.is_err() || result.unwrap().materialize().is_err(),
        "Dropping non-existent index should error");
}
