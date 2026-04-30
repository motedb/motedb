//! Regression tests for bug-hunt round 4 (Critical + High fixes)

use motedb::{Database, types::Value, sql::QueryResult};
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
// C1/C2: Column index was empty due to DefaultHasher vs table_id
// ============================================================

#[test]
fn test_column_index_returns_results() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE users (id INT PRIMARY KEY, age INT)").unwrap();
    db.execute("INSERT INTO users (id, age) VALUES (1, 25)").unwrap();
    db.execute("INSERT INTO users (id, age) VALUES (2, 30)").unwrap();
    db.execute("INSERT INTO users (id, age) VALUES (3, 25)").unwrap();

    // Create column index — previously returned 0 rows due to wrong hash
    db.execute("CREATE INDEX idx_age ON users (age)").unwrap();

    // Give index builder time
    std::thread::sleep(std::time::Duration::from_millis(500));

    let r = rows(&db, "SELECT id FROM users WHERE age = 25 ORDER BY id");
    assert!(r.len() >= 2, "Column index should find at least 2 rows with age=25, got {}", r.len());
}

// ============================================================
// H1: can_eval_positional validates function names
// ============================================================

#[test]
fn test_year_function_in_where() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE events (id INT PRIMARY KEY, ts TIMESTAMP)").unwrap();
    db.execute("INSERT INTO events (id, ts) VALUES (1, 1700000000000000)").unwrap();
    db.execute("INSERT INTO events (id, ts) VALUES (2, 1600000000000000)").unwrap();

    // YEAR() must work in SELECT — the function is handled by evaluator's HashMap path
    let r = rows(&db, "SELECT YEAR(ts) AS y FROM events ORDER BY id");
    assert_eq!(r.len(), 2);
    if let Value::Integer(y) = r[0][0] {
        assert!(y > 0, "YEAR should be positive, got {}", y);
    }
}

#[test]
fn test_coalesce_in_where() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();
    db.execute("INSERT INTO t (id, val) VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t (id) VALUES (2)").unwrap();

    let r = rows(&db, "SELECT id FROM t WHERE COALESCE(val, 0) > 0");
    assert_eq!(r.len(), 1, "COALESCE in WHERE should work");
    assert_eq!(r[0][0], Value::Integer(1));
}

// ============================================================
// H2: Vector distance operators in positional path
// ============================================================

#[test]
fn test_vector_distance_in_where_positional() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3))").unwrap();
    db.execute("INSERT INTO items (id, emb) VALUES (1, [1.0, 0.0, 0.0])").unwrap();
    db.execute("INSERT INTO items (id, emb) VALUES (2, [0.0, 1.0, 0.0])").unwrap();
    db.execute("INSERT INTO items (id, emb) VALUES (3, [0.0, 0.0, 1.0])").unwrap();

    // L2 distance filter in WHERE — previously returned 0 rows silently
    let r = rows(&db, "SELECT id FROM items WHERE (emb <-> [1.0, 0.0, 0.0]) < 0.5");
    assert_eq!(r.len(), 1, "Vector L2 distance in WHERE should find 1 row");
    assert_eq!(r[0][0], Value::Integer(1));
}

// ============================================================
// H7: NULL comparison returns false (not crash)
// ============================================================

#[test]
fn test_null_comparison_returns_false() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();
    db.execute("INSERT INTO t (id, val) VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t (id) VALUES (2)").unwrap();

    // NULL < 5 should return false, not crash
    let r = rows(&db, "SELECT id FROM t WHERE val < 5");
    assert_eq!(r.len(), 0, "NULL < 5 should be false");

    let r = rows(&db, "SELECT id FROM t WHERE val > 5");
    assert_eq!(r.len(), 1, "val > 5 should match id=1");
}

// ============================================================
// H8: length() returns char count not byte count
// ============================================================

#[test]
fn test_length_unicode() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO t (id, name) VALUES (1, '你好世界')").unwrap();

    let r = rows(&db, "SELECT LENGTH(name) AS len FROM t WHERE id = 1");
    assert_eq!(r.len(), 1);
    if let Value::Integer(len) = r[0][0] {
        assert_eq!(len, 4, "LENGTH should count characters, not bytes (你好世界 = 4 chars)");
    }
}

// ============================================================
// M1: concat NULL propagates
// ============================================================

#[test]
fn test_concat_null_propagates() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT)").unwrap();
    db.execute("INSERT INTO t (id, a, b) VALUES (1, 'hello', 'world')").unwrap();
    db.execute("INSERT INTO t (id, a) VALUES (2, 'hello')").unwrap();

    let r = rows(&db, "SELECT CONCAT(a, b) AS result FROM t WHERE id = 2");
    assert_eq!(r.len(), 1);
    assert!(matches!(r[0][0], Value::Null), "CONCAT with NULL should return NULL");
}

// ============================================================
// M3: Constraints in any order
// ============================================================

#[test]
fn test_constraints_any_order() {
    let (db, _dir) = create_db();
    // PRIMARY KEY before NOT NULL — previously failed
    db.execute("CREATE TABLE t (id INT PRIMARY KEY NOT NULL, val INT)").unwrap();
    db.execute("INSERT INTO t (id, val) VALUES (1, 42)").unwrap();

    let r = rows(&db, "SELECT val FROM t WHERE id = 1");
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Integer(42));
}

// ============================================================
// M3: Explicit NULL keyword
// ============================================================

#[test]
fn test_explicit_null_column() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT, name TEXT NULL)").unwrap();
    db.execute("INSERT INTO t (id, name) VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO t (id) VALUES (2)").unwrap();

    let r = rows(&db, "SELECT id FROM t WHERE name IS NULL");
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Integer(2));
}

// ============================================================
// H5: Batch insert duplicate PK
// ============================================================

#[test]
fn test_batch_insert_duplicate_pk() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();

    // Batch insert with duplicate PK should fail
    let result = db.execute("INSERT INTO t (id, val) VALUES (1, 10), (1, 20)");
    assert!(result.is_err() || result.unwrap().materialize().is_err(),
            "Batch insert with duplicate PK should fail");
}

// ============================================================
// L5: PK range uses actual primary key name
// ============================================================

#[test]
fn test_pk_equality_non_id_column() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE items (sku INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO items (sku, name) VALUES (100, 'a')").unwrap();
    db.execute("INSERT INTO items (sku, name) VALUES (200, 'b')").unwrap();

    // PK lookup on non-'id' column should work via fast path or full scan
    let r = rows(&db, "SELECT name FROM items WHERE sku = 200");
    assert_eq!(r.len(), 1, "PK lookup on 'sku' should find 1 row, got {}", r.len());
    assert_eq!(r[0][0], Value::Text("b".to_string()));
}
