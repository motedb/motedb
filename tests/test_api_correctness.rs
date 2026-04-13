//! API Correctness Tests
//!
//! Tests that public API methods route to the correct table,
//! handle edge cases, and return expected results.
//!
//! Run: cargo test --test test_api_correctness -- --test-threads=1

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

fn query_rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match exec(db, sql) {
        motedb::sql::QueryResult::Select { rows, .. } => rows,
        _ => vec![],
    }
}

// ============================================================================
// 1. batch_insert routes to correct table (not _default)
// ============================================================================

#[test]
fn test_batch_insert_routes_to_named_table() {
    let (db, _dir) = create_db();

    exec(&db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");

    let input_rows = vec![
        vec![Value::Integer(1), Value::Text("Alice".to_string()), Value::Integer(30)],
        vec![Value::Integer(2), Value::Text("Bob".to_string()), Value::Integer(25)],
        vec![Value::Integer(3), Value::Text("Charlie".to_string()), Value::Integer(35)],
    ];

    let row_ids = db.batch_insert("users", input_rows).expect("batch insert");
    assert_eq!(row_ids.len(), 3, "Should return 3 row IDs");

    // Verify data is in the correct table via SQL
    let result = query_rows(&db, "SELECT * FROM users ORDER BY id");
    assert_eq!(result.len(), 3);
    assert_eq!(result[0][1], Value::Text("Alice".to_string()));
    assert_eq!(result[1][1], Value::Text("Bob".to_string()));
    assert_eq!(result[2][1], Value::Text("Charlie".to_string()));
}

#[test]
fn test_batch_insert_multiple_tables_isolation() {
    let (db, _dir) = create_db();

    exec(&db, "CREATE TABLE table_a (id INTEGER PRIMARY KEY, val TEXT)");
    exec(&db, "CREATE TABLE table_b (id INTEGER PRIMARY KEY, val TEXT)");

    let rows_a = vec![
        vec![Value::Integer(1), Value::Text("A1".to_string())],
        vec![Value::Integer(2), Value::Text("A2".to_string())],
    ];
    let rows_b = vec![
        vec![Value::Integer(1), Value::Text("B1".to_string())],
        vec![Value::Integer(2), Value::Text("B2".to_string())],
    ];

    db.batch_insert("table_a", rows_a).expect("batch insert A");
    db.batch_insert("table_b", rows_b).expect("batch insert B");

    let result_a = query_rows(&db, "SELECT * FROM table_a ORDER BY id");
    let result_b = query_rows(&db, "SELECT * FROM table_b ORDER BY id");

    assert_eq!(result_a.len(), 2);
    assert_eq!(result_b.len(), 2);
    assert_eq!(result_a[0][1], Value::Text("A1".to_string()));
    assert_eq!(result_b[0][1], Value::Text("B1".to_string()));
}

// ============================================================================
// 2. get_row reads from correct table (using API-inserted row_ids)
// ============================================================================

#[test]
fn test_get_row_named_table() {
    let (db, _dir) = create_db();

    exec(&db, "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)");

    // Insert via API to get the internal row_id
    let row = vec![Value::Integer(1), Value::Text("widget".to_string())];
    let row_id = db.insert_row("items", row).expect("insert_row");

    // get_row should find the row using internal row_id
    let read_back = db.get_row("items", row_id).expect("get_row");
    assert!(read_back.is_some(), "Should find row in 'items' table via row_id");
    let read_back = read_back.unwrap();
    assert_eq!(read_back[1], Value::Text("widget".to_string()));

    // get_row_map should also work
    let map = db.get_row_map("items", row_id).expect("get_row_map");
    assert!(map.is_some(), "Should find row via get_row_map");
    let map = map.unwrap();
    assert_eq!(map.get("name"), Some(&Value::Text("widget".to_string())));
}

#[test]
fn test_get_row_map_wrong_table_returns_none() {
    let (db, _dir) = create_db();

    exec(&db, "CREATE TABLE table_x (id INTEGER PRIMARY KEY, val TEXT)");
    exec(&db, "CREATE TABLE table_y (id INTEGER PRIMARY KEY, val TEXT)");

    // Insert into table_x via API to get internal row_id
    let row = vec![Value::Integer(1), Value::Text("X".to_string())];
    let row_id = db.insert_row("table_x", row).expect("insert_row");

    // Row exists in table_x but not table_y
    let result_x = db.get_row_map("table_x", row_id).expect("get_row_map x");
    let result_y = db.get_row_map("table_y", row_id).expect("get_row_map y");

    assert!(result_x.is_some(), "Row should exist in table_x");
    assert!(result_y.is_none(), "Row should NOT exist in table_y");
}

#[test]
fn test_get_row_with_table_name() {
    let (db, _dir) = create_db();

    exec(&db, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)");

    // Insert via API to capture internal row_id
    let row = vec![Value::Integer(42), Value::Text("gadget".to_string())];
    let row_id = db.insert_row("products", row).expect("insert_row");

    let found = db.get_row("products", row_id).expect("get_row");
    assert!(found.is_some(), "Should find row in products table");
    let found = found.unwrap();
    assert_eq!(found[1], Value::Text("gadget".to_string()));

    // Non-existent row_id
    let not_found = db.get_row("products", 999999).expect("get_row");
    assert!(not_found.is_none(), "Should not find non-existent row");
}

// ============================================================================
// 3. insert_row / update_row / delete_row consistency
// ============================================================================

#[test]
fn test_insert_get_update_delete_cycle() {
    let (db, _dir) = create_db();

    exec(&db, "CREATE TABLE sensors (id INTEGER PRIMARY KEY, temp FLOAT, label TEXT)");

    // Insert via API
    let row = vec![Value::Integer(1), Value::Float(23.5), Value::Text("indoor".to_string())];
    let row_id = db.insert_row("sensors", row).expect("insert_row");

    // Read back
    let read_back = db.get_row("sensors", row_id).expect("get_row").expect("row exists");
    assert_eq!(read_back[0], Value::Integer(1));
    assert_eq!(read_back[2], Value::Text("indoor".to_string()));

    // Update via API
    let new_row = vec![Value::Integer(1), Value::Float(26.0), Value::Text("outdoor".to_string())];
    db.update_row("sensors", row_id, new_row).expect("update_row");

    let updated = db.get_row("sensors", row_id).expect("get_row").expect("row exists");
    assert_eq!(updated[1], Value::Float(26.0));
    assert_eq!(updated[2], Value::Text("outdoor".to_string()));

    // Delete via API
    db.delete_row("sensors", row_id).expect("delete_row");
    let deleted = db.get_row("sensors", row_id).expect("get_row");
    assert!(deleted.is_none(), "Row should be deleted");
}

// ============================================================================
// 4. insert_row_map / batch_insert_map consistency
// ============================================================================

#[test]
fn test_insert_row_map() {
    let (db, _dir) = create_db();

    exec(&db, "CREATE TABLE logs (id INTEGER PRIMARY KEY, level TEXT, msg TEXT)");

    let mut row = std::collections::HashMap::new();
    row.insert("id".to_string(), Value::Integer(1));
    row.insert("level".to_string(), Value::Text("INFO".to_string()));
    row.insert("msg".to_string(), Value::Text("started".to_string()));

    let _row_id = db.insert_row_map("logs", row).expect("insert_row_map");

    let result = query_rows(&db, "SELECT * FROM logs WHERE id = 1");
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][1], Value::Text("INFO".to_string()));
}

#[test]
fn test_batch_insert_map() {
    let (db, _dir) = create_db();

    exec(&db, "CREATE TABLE events (id INTEGER PRIMARY KEY, type TEXT)");

    let mut input_rows = Vec::new();
    for i in 1..=5 {
        let mut row = std::collections::HashMap::new();
        row.insert("id".to_string(), Value::Integer(i));
        row.insert("type".to_string(), Value::Text(format!("event_{}", i)));
        input_rows.push(row);
    }

    let row_ids = db.batch_insert_map("events", input_rows).expect("batch_insert_map");
    assert_eq!(row_ids.len(), 5);

    let result = query_rows(&db, "SELECT COUNT(*) as cnt FROM events");
    assert_eq!(result[0][0], Value::Integer(5));
}

// ============================================================================
// 5. Edge cases
// ============================================================================

#[test]
fn test_empty_batch_insert() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");

    let row_ids = db.batch_insert("t", vec![]).expect("empty batch");
    assert!(row_ids.is_empty());
}

#[test]
fn test_get_row_nonexistent_table() {
    let (db, _dir) = create_db();

    let result = db.get_row("nonexistent_table", 1);
    assert!(result.is_err(), "Should error for nonexistent table");
}

#[test]
fn test_batch_insert_wrong_schema() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");

    // Wrong number of columns
    let input_rows = vec![
        vec![Value::Integer(1), Value::Text("ok".to_string()), Value::Integer(999)],
    ];
    let result = db.batch_insert("t", input_rows);
    assert!(result.is_err(), "Should fail validation for wrong schema");
}
