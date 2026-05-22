//! Comprehensive test: logic correctness, performance, resource consumption
//! Covers P0-P3 optimizations and parameterized query edge cases.

use motedb::{Database, types::Value, config::DBConfig};
use std::time::Instant;
use std::path::Path;
use tempfile::TempDir;

fn setup_db(dir: &Path) -> Database {
    let mut config = DBConfig::for_testing();
    config.auto_checkpoint = None;  // Disable auto-checkpoint to avoid CI hangs
    let db = Database::create_with_config(dir.join("audit.mote"), config).unwrap();
    db.execute("CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT, age INT, score FLOAT)").unwrap();
    db
}

fn setup_db_fast(dir: &Path) -> Database {
    let mut config = DBConfig::for_testing();
    // Disable auto-checkpoint in tests to avoid background thread contention on CI
    config.auto_checkpoint = None;
    let db = Database::create_with_config(dir.join("audit.mote"), config).unwrap();
    db.execute("CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT, age INT, score FLOAT)").unwrap();
    db
}

fn insert_n_users(db: &Database, n: i64) {
    for i in 0..n {
        db.execute(&format!(
            "INSERT INTO users VALUES (null, 'user{}', {}, {})",
            i, 20 + (i % 50), 50.0 + i as f64
        )).unwrap();
    }
}

fn query_rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    let result = db.execute(sql).unwrap().materialize().unwrap();
    match result {
        motedb::QueryResult::Select { rows, .. } => rows,
        _ => vec![],
    }
}

fn query_rows_prepared(db: &Database, sql: &str, params: Vec<Value>) -> Vec<Vec<Value>> {
    let result = db.execute_prepared(sql, params).unwrap().materialize().unwrap();
    match result {
        motedb::QueryResult::Select { rows, .. } => rows,
        _ => vec![],
    }
}

// ============================================================================
// 1. Parameterized query correctness
// ============================================================================

#[test]
fn test_parameterized_select_basic() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    insert_n_users(&db, 100);

    // Find a known user
    let all = query_rows(&db, "SELECT * FROM users WHERE name = 'user42'");
    assert_eq!(all.len(), 1);
    let target_id = match all[0][0] { Value::Integer(id) => id, _ => panic!("no id") };

    let rows = query_rows_prepared(&db, "SELECT * FROM users WHERE id = ?", vec![Value::Integer(target_id)]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::text("user42".to_string()));
}

#[test]
fn test_parameterized_select_multi_params() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    insert_n_users(&db, 50);

    // age = 20+i%50, so age 31..39 exists for i where (20+i%50) in 31..39
    let rows = query_rows_prepared(&db,
        "SELECT * FROM users WHERE age > ? AND age < ?",
        vec![Value::Integer(30), Value::Integer(40)]
    );
    assert!(rows.len() > 0, "Should find rows with age between 30 and 40");
    for row in &rows {
        let age = match &row[2] { Value::Integer(a) => *a, _ => -1 };
        assert!(age > 30 && age < 40, "Age should be in (30, 40), got {}", age);
    }
}

#[test]
fn test_parameterized_insert() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());

    db.execute_prepared(
        "INSERT INTO users VALUES (null, ?, ?, ?)",
        vec![Value::text("Alice".to_string()), Value::Integer(30), Value::Float(95.5)]
    ).unwrap();

    let rows = query_rows(&db, "SELECT * FROM users WHERE name = 'Alice'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][2], Value::Integer(30));
    assert_eq!(rows[0][3], Value::Float(95.5));
}

#[test]
fn test_parameterized_numbered_params() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    insert_n_users(&db, 10);

    let rows = query_rows_prepared(&db,
        "SELECT * FROM users WHERE age > ?1 AND score < ?2",
        vec![Value::Integer(15), Value::Float(999.0)]
    );
    assert!(rows.len() > 0, "Should find rows with age>15 and score<999");
}

#[test]
fn test_parameterized_unbound_error() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    insert_n_users(&db, 5);

    // execute_prepared with empty params but query has ? → should error
    let result = db.execute_prepared("SELECT * FROM users WHERE id = ?", vec![]);
    assert!(result.is_err(), "execute_prepared with unbound ? should error");

    // execute() with ? is non-prepared — may silently return 0 rows (acceptable)
    let result = db.execute("SELECT * FROM users WHERE id = ?");
    match result {
        Ok(r) => {
            let rows = match r.materialize() {
                Ok(motedb::QueryResult::Select { rows, .. }) => rows,
                _ => vec![],
            };
            assert_eq!(rows.len(), 0, "Unbound ? via execute() should return 0 rows");
        }
        Err(_) => {} // Also acceptable
    }
}

#[test]
fn test_parameterized_cache_hit() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    insert_n_users(&db, 10);

    let sql = "SELECT * FROM users WHERE name = ?";

    let rows1 = query_rows_prepared(&db, sql, vec![Value::text("user1".to_string())]);
    assert_eq!(rows1.len(), 1);

    // Second call: cache hit — different param, different result
    let rows2 = query_rows_prepared(&db, sql, vec![Value::text("user5".to_string())]);
    assert_eq!(rows2.len(), 1);
    assert_eq!(rows2[0][1], Value::text("user5".to_string()));
}

#[test]
fn test_execute_with_unbound_parameter() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    insert_n_users(&db, 5);

    // execute() with ? should not panic — either error or 0 rows
    let result = db.execute("SELECT * FROM users WHERE id = ?");
    match result {
        Ok(r) => {
            let rows = match r.materialize() {
                Ok(motedb::QueryResult::Select { rows, .. }) => rows,
                _ => vec![],
            };
            assert_eq!(rows.len(), 0, "Unbound parameter should return 0 rows");
        }
        Err(_) => {} // OK
    }
}

// ============================================================================
// 2. Table-qualified column names
// ============================================================================

#[test]
fn test_table_qualified_column_where() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    insert_n_users(&db, 30);

    let rows = query_rows(&db, "SELECT * FROM users WHERE users.age > 30");
    assert!(rows.len() > 0);
    for row in &rows {
        let age = match &row[2] { Value::Integer(a) => *a, _ => 0 };
        assert!(age > 30, "Age should be > 30, got {}", age);
    }
}

#[test]
fn test_table_qualified_column_select() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    insert_n_users(&db, 1);

    let row0 = query_rows(&db, "SELECT * FROM users").into_iter().next().unwrap();
    let id = match row0[0] { Value::Integer(i) => i, _ => panic!() };

    let rows = query_rows(&db, &format!("SELECT users.name, users.age FROM users WHERE id = {}", id));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::text("user0".to_string()));
}

// ============================================================================
// 3. Full scan WHERE
// ============================================================================

#[test]
fn test_full_scan_where_filter() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    insert_n_users(&db, 200);

    let rows = query_rows(&db, "SELECT * FROM users WHERE age = 35");
    assert!(rows.len() > 0);
    for row in &rows {
        let age = match &row[2] { Value::Integer(a) => *a, _ => -1 };
        assert_eq!(age, 35);
    }
}

#[test]
fn test_full_scan_where_and() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    insert_n_users(&db, 100);

    let rows = query_rows(&db, "SELECT * FROM users WHERE age > 25 AND age < 30");
    for row in &rows {
        let age = match &row[2] { Value::Integer(a) => *a, _ => -1 };
        assert!(age > 25 && age < 30);
    }
}

// ============================================================================
// 4. Column index scan
// ============================================================================

#[test]
fn test_column_index_scan_correctness() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());

    db.execute("CREATE TABLE items (id INT PRIMARY KEY AUTO_INCREMENT, category TEXT, price FLOAT)").unwrap();
    db.execute("CREATE INDEX items_category ON items(category)").unwrap();

    for i in 0..50 {
        let cat = if i % 3 == 0 { "A" } else if i % 3 == 1 { "B" } else { "C" };
        db.execute(&format!("INSERT INTO items VALUES (null, '{}', {})", cat, 10.0 + i as f64)).unwrap();
    }

    let rows = query_rows(&db, "SELECT * FROM items WHERE category = 'A'");
    assert_eq!(rows.len(), 17);
}

// ============================================================================
// 5. Edge cases
// ============================================================================

#[test]
fn test_edge_null_where() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    insert_n_users(&db, 5);

    let rows = query_rows(&db, "SELECT * FROM users WHERE age = NULL");
    assert_eq!(rows.len(), 0);
}

#[test]
fn test_edge_empty_table() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    let rows = query_rows(&db, "SELECT * FROM users WHERE age > 10");
    assert_eq!(rows.len(), 0);
}

#[test]
fn test_edge_update_reselect() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    insert_n_users(&db, 1);

    let row0 = query_rows(&db, "SELECT * FROM users").into_iter().next().unwrap();
    let id = match row0[0] { Value::Integer(i) => i, _ => panic!() };

    db.execute(&format!("UPDATE users SET age = 30, score = 95.0 WHERE id = {}", id)).unwrap();

    let rows = query_rows(&db, &format!("SELECT * FROM users WHERE id = {}", id));
    assert_eq!(rows[0][2], Value::Integer(30));
    assert_eq!(rows[0][3], Value::Float(95.0));
}

#[test]
fn test_edge_delete_verify() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    insert_n_users(&db, 10);

    let row5 = query_rows(&db, "SELECT * FROM users WHERE name = 'user5'");
    let id5 = match row5[0][0] { Value::Integer(i) => i, _ => panic!() };

    db.execute(&format!("DELETE FROM users WHERE id = {}", id5)).unwrap();

    let rows = query_rows(&db, "SELECT * FROM users");
    assert_eq!(rows.len(), 9);

    let rows2 = query_rows(&db, &format!("SELECT * FROM users WHERE id = {}", id5));
    assert_eq!(rows2.len(), 0);
}

#[test]
fn test_edge_string_with_keywords() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("INSERT INTO users VALUES (null, 'from where select', 25, 90.0)").unwrap();

    let rows = query_rows(&db, "SELECT * FROM users WHERE name = 'from where select'");
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_edge_checkpoint_recovery() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("audit.mote");

    {
        let mut config = DBConfig::for_testing();
        config.auto_checkpoint = None;
        let db = Database::create_with_config(&path, config).unwrap();
        db.execute("CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT, age INT)").unwrap();
        insert_n_users_simple(&db, 100);
        db.checkpoint().unwrap();
    }

    let db = Database::open(&path).unwrap();  // Reopen uses existing data
    let rows = query_rows(&db, "SELECT * FROM users");
    assert_eq!(rows.len(), 100);

    let rows2 = query_rows_prepared(&db, "SELECT * FROM users WHERE name = ?", vec![Value::text("user50".to_string())]);
    assert_eq!(rows2.len(), 1);
}

fn insert_n_users_simple(db: &Database, n: i64) {
    for i in 0..n {
        db.execute(&format!("INSERT INTO users VALUES (null, 'user{}', {})", i, 20 + i)).unwrap();
    }
}

// ============================================================================
// 6. PERFORMANCE + RESOURCE
// ============================================================================

fn get_rss_kb() -> u64 {
    #[cfg(target_os = "macos")]
    {
        let pid = std::process::id();
        let output = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &pid.to_string()])
            .output().ok();
        output.and_then(|o| String::from_utf8(o.stdout).ok())
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0)
    }
    #[cfg(not(target_os = "macos"))]
    { 0 }
}

#[test]
fn test_comprehensive_summary() {
    let dir = TempDir::new().unwrap();
    let db = setup_db_fast(dir.path());

    println!("\n====== MoteDB Comprehensive Audit ======");

    // INSERT
    let n = if std::env::var("CI").is_ok() { 500 } else { 2000 };
    let start = Instant::now();
    insert_n_users(&db, n);
    let insert_time = start.elapsed();
    let insert_us = insert_time.as_micros() as f64 / n as f64;
    println!("  INSERT:   {:.0} µs/op, {:.0} ops/s", insert_us, 1_000_000.0 / insert_us);

    // PK SELECT (raw)
    let pk_iters = if std::env::var("CI").is_ok() { 200 } else { 1000 };
    let start = Instant::now();
    for i in 0..pk_iters {
        let _ = query_rows(&db, &format!("SELECT * FROM users WHERE id = {}", i % n + 1));
    }
    let pk_us = start.elapsed().as_micros() as f64 / pk_iters as f64;
    println!("  PK SELECT (raw):  {:.0} µs/op", pk_us);

    // PK SELECT (prepared)
    let start = Instant::now();
    for i in 0..pk_iters {
        let _ = query_rows_prepared(&db,
            "SELECT * FROM users WHERE id = ?",
            vec![Value::Integer((i % n + 1) as i64)]
        );
    }
    let pk_prep_us = start.elapsed().as_micros() as f64 / pk_iters as f64;
    println!("  PK SELECT (prep): {:.0} µs/op  ({:.2}x vs raw)", pk_prep_us, pk_us / pk_prep_us);

    // Full scan
    let start = Instant::now();
    let _rows = query_rows(&db, "SELECT * FROM users WHERE age > 40");
    let scan_time = start.elapsed();
    println!("  Full scan {} rows (WHERE age>40): {:.1} ms ({:.1} µs/row)",
        n, scan_time.as_secs_f64() * 1000.0, scan_time.as_micros() as f64 / n as f64);

    // UPDATE
    let update_n = if std::env::var("CI").is_ok() { 100 } else { 500 };
    let start = Instant::now();
    for i in 1..=update_n {
        db.execute(&format!("UPDATE users SET score = {} WHERE id = {}", 100.0 + i as f64, i)).unwrap();
    }
    println!("  UPDATE:   {:.0} µs/op", start.elapsed().as_micros() as f64 / update_n as f64);

    // DELETE
    let delete_n = if std::env::var("CI").is_ok() { 50 } else { 200 };
    let start = Instant::now();
    for i in 1..=delete_n {
        db.execute(&format!("DELETE FROM users WHERE id = {}", i)).unwrap();
    }
    println!("  DELETE:   {:.0} µs/op", start.elapsed().as_micros() as f64 / delete_n as f64);

    // Checkpoint
    let start = Instant::now();
    db.checkpoint().unwrap();
    println!("  Checkpoint: {:.0} ms", start.elapsed().as_secs_f64() * 1000.0);

    // Memory
    println!("  RSS: {:.1} MB", get_rss_kb() as f64 / 1024.0);

    // Correctness
    let rows = query_rows(&db, "SELECT * FROM users WHERE age > 45");
    assert!(rows.len() > 0);
    for row in &rows {
        let age = match &row[2] { Value::Integer(a) => *a, _ => 0 };
        assert!(age > 45);
    }

    // Parameterized correctness after CRUD
    let rows_p = query_rows_prepared(&db,
        "SELECT * FROM users WHERE age > ?",
        vec![Value::Integer(45)]
    );
    assert_eq!(rows_p.len(), rows.len());

    println!("==========================================\n");
}
