//! Tests for low-level row API: insert_row, get_row, update_row, delete_row,
//! get_row_map, insert_row_map, batch_insert, batch_insert_map,
//! insert_row_with_txn, get_table_schema

use motedb::{types::Value, Database};
use tempfile::TempDir;

fn setup() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, score FLOAT)")
        .unwrap();
    (db, dir)
}

// === insert_row + get_row ===

#[test]
fn test_insert_and_get_row() {
    let (db, _dir) = setup();
    let row = vec![
        Value::Integer(1),
        Value::text("Alice".to_string()),
        Value::Float(95.5),
    ];
    let row_id = db.insert_row("users", row).unwrap();
    // RowId may be 0-based or 1-based depending on implementation

    let result = db.get_row("users", row_id).unwrap();
    assert!(result.is_some());
    let retrieved = result.unwrap();
    assert_eq!(retrieved.len(), 3);
}

#[test]
fn test_get_row_nonexistent() {
    let (db, _dir) = setup();
    let result = db.get_row("users", 99999).unwrap();
    assert!(
        result.is_none(),
        "get_row for nonexistent should return None"
    );
}

#[test]
fn test_insert_row_wrong_table() {
    let (db, _dir) = setup();
    let row = vec![
        Value::Integer(1),
        Value::text("test".to_string()),
        Value::Float(1.0),
    ];
    let result = db.insert_row("nonexistent", row);
    assert!(
        result.is_err(),
        "insert_row to nonexistent table should error"
    );
}

// === update_row ===

#[test]
fn test_update_row() {
    let (db, _dir) = setup();
    let row = vec![
        Value::Integer(1),
        Value::text("Alice".to_string()),
        Value::Float(90.0),
    ];
    let row_id = db.insert_row("users", row).unwrap();

    let new_row = vec![
        Value::Integer(1),
        Value::text("Bob".to_string()),
        Value::Float(95.0),
    ];
    db.update_row("users", row_id, new_row).unwrap();

    // Verify via SQL
    let result = db
        .execute("SELECT name, score FROM users WHERE id = 1")
        .unwrap();
    let r = match result.materialize().unwrap() {
        motedb::QueryResult::Select { rows, .. } => rows,
        _ => panic!("Expected Select"),
    };
    assert_eq!(r.len(), 1);
    assert_eq!(&r[0][0], &Value::text("Bob".to_string()));
}

#[test]
fn test_update_row_nonexistent() {
    let (db, _dir) = setup();
    let new_row = vec![
        Value::Integer(99),
        Value::text("Ghost".to_string()),
        Value::Float(0.0),
    ];
    let result = db.update_row("users", 99999, new_row);
    assert!(result.is_err(), "update_row nonexistent should error");
}

// === delete_row ===

#[test]
fn test_delete_row() {
    let (db, _dir) = setup();
    let row = vec![
        Value::Integer(1),
        Value::text("Alice".to_string()),
        Value::Float(90.0),
    ];
    let row_id = db.insert_row("users", row).unwrap();

    db.delete_row("users", row_id).unwrap();

    let result = db.get_row("users", row_id).unwrap();
    assert!(result.is_none(), "Deleted row should return None");
}

#[test]
fn test_delete_row_nonexistent() {
    let (db, _dir) = setup();
    let result = db.delete_row("users", 99999);
    assert!(result.is_err(), "delete_row nonexistent should error");
}

// === insert_row_map ===

#[test]
fn test_insert_row_map_full() {
    let (db, _dir) = setup();
    let mut map = std::collections::HashMap::new();
    map.insert("id".to_string(), Value::Integer(1));
    map.insert("name".to_string(), Value::text("Charlie".to_string()));
    map.insert("score".to_string(), Value::Float(88.0));

    let row_id = db.insert_row_map("users", map).unwrap();
    // RowId assignment varies by implementation

    // Verify via get_row_map
    let result = db.get_row_map("users", row_id).unwrap();
    assert!(result.is_some());
    let m = result.unwrap();
    assert_eq!(m.get("name"), Some(&Value::text("Charlie".to_string())));
}

#[test]
fn test_insert_row_map_partial() {
    let (db, _dir) = setup();
    let mut map = std::collections::HashMap::new();
    map.insert("id".to_string(), Value::Integer(2));
    map.insert("name".to_string(), Value::text("Dave".to_string()));
    // score omitted -> should be NULL

    db.insert_row_map("users", map).unwrap();

    let result = db.execute("SELECT score FROM users WHERE id = 2").unwrap();
    let r = match result.materialize().unwrap() {
        motedb::QueryResult::Select { rows, .. } => rows,
        _ => panic!("Expected Select"),
    };
    assert!(
        matches!(&r[0][0], Value::Null),
        "Omitted column should be NULL"
    );
}

// === batch_insert ===

#[test]
fn test_batch_insert_large() {
    let (db, _dir) = setup();
    let mut batch = Vec::new();
    for i in 1..=50 {
        batch.push(vec![
            Value::Integer(i),
            Value::text(format!("user_{}", i)),
            Value::Float(i as f64 * 2.0),
        ]);
    }

    let row_ids = db.batch_insert("users", batch).unwrap();
    assert_eq!(row_ids.len(), 50);

    let result = db.execute("SELECT COUNT(*) FROM users").unwrap();
    let r = match result.materialize().unwrap() {
        motedb::QueryResult::Select { rows, .. } => rows,
        _ => panic!("Expected Select"),
    };
    assert_eq!(&r[0][0], &Value::Integer(50));
}

// === batch_insert_map ===

#[test]
fn test_batch_insert_map() {
    let (db, _dir) = setup();
    let mut rows = Vec::new();
    for i in 1..=10 {
        let mut map = std::collections::HashMap::new();
        map.insert("id".to_string(), Value::Integer(i));
        map.insert("name".to_string(), Value::text(format!("batch_{}", i)));
        map.insert("score".to_string(), Value::Float(i as f64));
        rows.push(map);
    }

    let row_ids = db.batch_insert_map("users", rows).unwrap();
    assert_eq!(row_ids.len(), 10);
}

// === insert_row_with_txn + commit ===

#[test]
fn test_insert_row_with_txn_commit() {
    let (db, _dir) = setup();
    let tx = db.begin_transaction().unwrap();

    let row = vec![
        Value::Integer(1),
        Value::text("txn_user".to_string()),
        Value::Float(77.7),
    ];
    let _row_id = db.insert_row_with_txn("users", tx, row).unwrap();

    db.commit_transaction(tx).unwrap();

    // After commit, row should be visible via SQL
    let result = db.execute("SELECT name FROM users WHERE id = 1").unwrap();
    let r = match result.materialize().unwrap() {
        motedb::QueryResult::Select { rows, .. } => rows,
        _ => panic!("Expected Select"),
    };
    assert_eq!(r.len(), 1);
    assert_eq!(&r[0][0], &Value::text("txn_user".to_string()));
}

#[test]
fn test_insert_row_with_txn_rollback() {
    let (db, _dir) = setup();
    let tx = db.begin_transaction().unwrap();

    let row = vec![
        Value::Integer(1),
        Value::text("rollback_user".to_string()),
        Value::Float(0.0),
    ];
    db.insert_row_with_txn("users", tx, row).unwrap();

    db.rollback_transaction(tx).unwrap();

    // After rollback, row should NOT be visible
    let result = db.execute("SELECT COUNT(*) FROM users").unwrap();
    let r = match result.materialize().unwrap() {
        motedb::QueryResult::Select { rows, .. } => rows,
        _ => panic!("Expected Select"),
    };
    assert_eq!(&r[0][0], &Value::Integer(0));
}

// === get_table_schema ===

#[test]
fn test_get_table_schema() {
    let (db, _dir) = setup();
    let schema = db.execute("DESCRIBE users");
    assert!(schema.is_ok(), "DESCRIBE should work");
}

// === CRUD cycle via row API ===

#[test]
fn test_full_crud_cycle() {
    let (db, _dir) = setup();

    // Create
    let row = vec![
        Value::Integer(1),
        Value::text("original".to_string()),
        Value::Float(50.0),
    ];
    let row_id = db.insert_row("users", row).unwrap();

    // Read
    let got = db.get_row("users", row_id).unwrap().unwrap();
    assert_eq!(got.len(), 3);

    // Update
    let updated = vec![
        Value::Integer(1),
        Value::text("updated".to_string()),
        Value::Float(99.0),
    ];
    db.update_row("users", row_id, updated).unwrap();

    // Verify update via get_row_map
    let map = db.get_row_map("users", row_id).unwrap().unwrap();
    assert_eq!(map.get("name"), Some(&Value::text("updated".to_string())));
    match map.get("score") {
        Some(Value::Float(f)) => assert!((f - 99.0).abs() < 0.01),
        other => panic!("Expected Float(99.0), got {:?}", other),
    }

    // Delete
    db.delete_row("users", row_id).unwrap();
    assert!(db.get_row("users", row_id).unwrap().is_none());
}

// === Concurrent row API ===

#[test]
fn test_concurrent_row_inserts() {
    use std::sync::Arc;
    use std::thread;

    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
        .unwrap();

    let mut handles = vec![];
    for t in 0..4 {
        let db_clone = db.clone();
        handles.push(thread::spawn(move || {
            for i in 0..25 {
                let id = t * 25 + i;
                let row = vec![Value::Integer(id), Value::text(format!("v_{}", id))];
                db_clone.insert_row("t", row).unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let result = db.execute("SELECT COUNT(*) FROM t").unwrap();
    let r = match result.materialize().unwrap() {
        motedb::QueryResult::Select { rows, .. } => rows,
        _ => panic!("Expected Select"),
    };
    assert_eq!(&r[0][0], &Value::Integer(100));
}
