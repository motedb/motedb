//! ACID Integration Tests for MoteDB
//!
//! Validates Atomicity, Consistency, Isolation, Durability across CRUD operations.

use motedb::Database;
use motedb::types::Value;
use tempfile::TempDir;

// ── Helpers ──────────────────────────────────────────────────────────────

fn create_test_db() -> (Database, TempDir) {
    let dir = TempDir::new().expect("create temp dir");
    let db = Database::create(dir.path()).expect("create db");
    (db, dir)
}

fn open_test_db(path: &std::path::Path) -> Database {
    Database::open(path).expect("open db")
}

/// Execute SQL and get the materialized QueryResult
fn exec(db: &Database, sql: &str) -> motedb::sql::QueryResult {
    db.execute(sql).expect("execute SQL").materialize().expect("materialize")
}

/// Execute a SELECT and return rows as Vec<HashMap<col, val>>
fn select_maps(db: &Database, sql: &str) -> Vec<std::collections::HashMap<String, Value>> {
    let result = exec(db, sql);
    match result {
        motedb::sql::QueryResult::Select { columns, rows } => {
            rows.into_iter()
                .map(|row| {
                    columns.iter()
                        .zip(row)
                        .map(|(col, val)| (col.clone(), val))
                        .collect()
                })
                .collect()
        }
        _ => panic!("Expected SELECT result, got {:?}", result),
    }
}

/// Count rows in a table via SELECT COUNT(*)
fn count_rows(db: &Database, table: &str) -> usize {
    let rows = select_maps(db, &format!("SELECT COUNT(*) AS cnt FROM {}", table));
    // The executor fast-path may return "COUNT(*)" instead of alias "cnt"
    let row = rows.first().unwrap_or_else(|| panic!("COUNT returned no rows for table {}", table));
    let val = row.get("cnt")
        .or_else(|| row.get("COUNT(*)"));
    match val {
        Some(Value::Integer(n)) => *n as usize,
        other => panic!("COUNT returned unexpected value for table {}: {:?}, row keys: {:?}", table, other, row.keys().collect::<Vec<_>>()),
    }
}

// ════════════════════════════════════════════════════════════════════════
// 1. CRUD — Basic Create / Read / Update / Delete
// ════════════════════════════════════════════════════════════════════════

#[test]
fn test_crud_insert_and_select() {
    let (db, _dir) = create_test_db();

    exec(&db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");

    let result = exec(&db, "INSERT INTO users VALUES (1, 'Alice', 30)");
    assert_eq!(result.affected_rows(), 1);

    let rows = select_maps(&db, "SELECT * FROM users");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("name"), Some(&Value::Text("Alice".into())));
    assert_eq!(rows[0].get("age"), Some(&Value::Integer(30)));
}

#[test]
fn test_crud_update() {
    let (db, _dir) = create_test_db();
    exec(&db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
    exec(&db, "INSERT INTO users VALUES (1, 'Alice', 30)");

    let result = exec(&db, "UPDATE users SET name = 'Bob', age = 25 WHERE id = 1");
    assert_eq!(result.affected_rows(), 1);

    let rows = select_maps(&db, "SELECT * FROM users");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("name"), Some(&Value::Text("Bob".into())));
    assert_eq!(rows[0].get("age"), Some(&Value::Integer(25)));
}

#[test]
fn test_crud_delete() {
    let (db, _dir) = create_test_db();
    exec(&db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
    exec(&db, "INSERT INTO users VALUES (1, 'Alice')");
    exec(&db, "INSERT INTO users VALUES (2, 'Bob')");

    let result = exec(&db, "DELETE FROM users WHERE id = 1");
    assert_eq!(result.affected_rows(), 1);

    let rows = select_maps(&db, "SELECT * FROM users");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("name"), Some(&Value::Text("Bob".into())));
}

#[test]
fn test_crud_delete_all() {
    let (db, _dir) = create_test_db();
    exec(&db, "CREATE TABLE items (id INTEGER PRIMARY KEY, v TEXT)");
    exec(&db, "INSERT INTO items VALUES (1, 'a')");
    exec(&db, "INSERT INTO items VALUES (2, 'b')");
    exec(&db, "INSERT INTO items VALUES (3, 'c')");

    exec(&db, "DELETE FROM items WHERE id = 1");
    exec(&db, "DELETE FROM items WHERE id = 2");
    exec(&db, "DELETE FROM items WHERE id = 3");

    let rows = select_maps(&db, "SELECT * FROM items");
    assert_eq!(rows.len(), 0);
}

#[test]
fn test_crud_multiple_rows() {
    let (db, _dir) = create_test_db();
    exec(&db, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price FLOAT)");

    for i in 0..50i64 {
        exec(&db, &format!("INSERT INTO products VALUES ({}, 'Product{}', {})", i, i, i as f64 * 1.5));
    }

    assert_eq!(count_rows(&db, "products"), 50);

    // Update
    exec(&db, "UPDATE products SET price = 99.9 WHERE id = 0");

    let rows = select_maps(&db, "SELECT * FROM products WHERE id = 0");
    assert_eq!(rows.len(), 1);

    // Delete
    for i in 0..25 {
        exec(&db, &format!("DELETE FROM products WHERE id = {}", i));
    }

    assert_eq!(count_rows(&db, "products"), 25);
}

// ════════════════════════════════════════════════════════════════════════
// 2. CONSISTENCY — Data stays valid through operations
// ════════════════════════════════════════════════════════════════════════

#[test]
fn test_consistency_update_preserves_other_rows() {
    let (db, _dir) = create_test_db();
    exec(&db, "CREATE TABLE items (id INTEGER PRIMARY KEY, val INTEGER)");

    for i in 1..=5i64 {
        exec(&db, &format!("INSERT INTO items VALUES ({}, {})", i, i * 10));
    }

    exec(&db, "UPDATE items SET val = 999 WHERE id = 3");

    let rows = select_maps(&db, "SELECT * FROM items ORDER BY id");
    assert_eq!(rows.len(), 5);

    assert_eq!(rows[2].get("val"), Some(&Value::Integer(999)));
    assert_eq!(rows[0].get("val"), Some(&Value::Integer(10)));
    assert_eq!(rows[1].get("val"), Some(&Value::Integer(20)));
    assert_eq!(rows[3].get("val"), Some(&Value::Integer(40)));
    assert_eq!(rows[4].get("val"), Some(&Value::Integer(50)));
}

#[test]
fn test_consistency_delete_does_not_corrupt_neighbors() {
    let (db, _dir) = create_test_db();
    exec(&db, "CREATE TABLE seq (id INTEGER PRIMARY KEY, data TEXT)");

    for i in 1..=10i64 {
        exec(&db, &format!("INSERT INTO seq VALUES ({}, 'row{}')", i, i));
    }

    for i in (2..=10).step_by(2) {
        exec(&db, &format!("DELETE FROM seq WHERE id = {}", i));
    }

    let rows = select_maps(&db, "SELECT * FROM seq ORDER BY id");
    assert_eq!(rows.len(), 5);

    let ids: Vec<i64> = rows.iter()
        .filter_map(|r| if let Some(Value::Integer(id)) = r.get("id") { Some(*id) } else { None })
        .collect();
    assert_eq!(ids, vec![1, 3, 5, 7, 9]);
}

#[test]
fn test_consistency_reinsert_after_delete() {
    let (db, _dir) = create_test_db();
    exec(&db, "CREATE TABLE kv (id INTEGER PRIMARY KEY, val TEXT)");

    exec(&db, "INSERT INTO kv VALUES (1, 'original')");
    exec(&db, "DELETE FROM kv WHERE id = 1");

    let rows = select_maps(&db, "SELECT * FROM kv");
    assert_eq!(rows.len(), 0);

    exec(&db, "INSERT INTO kv VALUES (1, 'restored')");

    let rows = select_maps(&db, "SELECT * FROM kv");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("val"), Some(&Value::Text("restored".into())));
}

#[test]
fn test_consistency_idempotent_update() {
    let (db, _dir) = create_test_db();
    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    exec(&db, "INSERT INTO t VALUES (1, 42)");

    for _ in 0..5 {
        exec(&db, "UPDATE t SET v = 42 WHERE id = 1");
    }

    let rows = select_maps(&db, "SELECT * FROM t");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("v"), Some(&Value::Integer(42)));
}

#[test]
fn test_consistency_sequential_updates() {
    let (db, _dir) = create_test_db();
    exec(&db, "CREATE TABLE counter (id INTEGER PRIMARY KEY, v INTEGER)");
    exec(&db, "INSERT INTO counter VALUES (1, 0)");

    for i in 1..=10i64 {
        exec(&db, &format!("UPDATE counter SET v = {} WHERE id = 1", i));
    }

    let rows = select_maps(&db, "SELECT * FROM counter");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("v"), Some(&Value::Integer(10)));
}

// ════════════════════════════════════════════════════════════════════════
// 3. ISOLATION — Table-level isolation
// ════════════════════════════════════════════════════════════════════════

#[test]
fn test_isolation_separate_tables_independent() {
    let (db, _dir) = create_test_db();

    exec(&db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)");
    exec(&db, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT)");

    exec(&db, "INSERT INTO t1 VALUES (1, 'table1')");
    exec(&db, "INSERT INTO t2 VALUES (1, 'table2')");

    exec(&db, "DELETE FROM t1 WHERE id = 1");

    let t1_rows = select_maps(&db, "SELECT * FROM t1");
    let t2_rows = select_maps(&db, "SELECT * FROM t2");

    assert_eq!(t1_rows.len(), 0, "t1 should be empty after delete");
    assert_eq!(t2_rows.len(), 1, "t2 should still have its row");
    assert_eq!(t2_rows[0].get("val"), Some(&Value::Text("table2".into())));
}

#[test]
fn test_isolation_same_key_different_tables() {
    let (db, _dir) = create_test_db();
    exec(&db, "CREATE TABLE alpha (id INTEGER PRIMARY KEY, data TEXT)");
    exec(&db, "CREATE TABLE beta (id INTEGER PRIMARY KEY, data TEXT)");

    exec(&db, "INSERT INTO alpha VALUES (42, 'alpha_data')");
    exec(&db, "INSERT INTO beta VALUES (42, 'beta_data')");

    let alpha = select_maps(&db, "SELECT * FROM alpha WHERE id = 42");
    let beta = select_maps(&db, "SELECT * FROM beta WHERE id = 42");

    assert_eq!(alpha[0].get("data"), Some(&Value::Text("alpha_data".into())));
    assert_eq!(beta[0].get("data"), Some(&Value::Text("beta_data".into())));
}

#[test]
fn test_isolation_update_one_table_does_not_affect_another() {
    let (db, _dir) = create_test_db();
    exec(&db, "CREATE TABLE source (id INTEGER PRIMARY KEY, v INTEGER)");
    exec(&db, "CREATE TABLE target (id INTEGER PRIMARY KEY, v INTEGER)");

    exec(&db, "INSERT INTO source VALUES (1, 100)");
    exec(&db, "INSERT INTO target VALUES (1, 200)");

    exec(&db, "UPDATE source SET v = 999 WHERE id = 1");

    let source = select_maps(&db, "SELECT * FROM source WHERE id = 1");
    let target = select_maps(&db, "SELECT * FROM target WHERE id = 1");

    assert_eq!(source[0].get("v"), Some(&Value::Integer(999)));
    assert_eq!(target[0].get("v"), Some(&Value::Integer(200)));
}

#[test]
fn test_isolation_three_tables_full_crud() {
    let (db, _dir) = create_test_db();
    exec(&db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
    exec(&db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount FLOAT)");
    exec(&db, "CREATE TABLE products (id INTEGER PRIMARY KEY, title TEXT, price FLOAT)");

    exec(&db, "INSERT INTO users VALUES (1, 'Alice')");
    exec(&db, "INSERT INTO orders VALUES (10, 1, 99.9)");
    exec(&db, "INSERT INTO products VALUES (100, 'Widget', 49.9)");

    exec(&db, "UPDATE users SET name = 'Bob' WHERE id = 1");
    exec(&db, "DELETE FROM orders WHERE id = 10");

    let users = select_maps(&db, "SELECT * FROM users");
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].get("name"), Some(&Value::Text("Bob".into())));

    let orders = select_maps(&db, "SELECT * FROM orders");
    assert_eq!(orders.len(), 0);

    let products = select_maps(&db, "SELECT * FROM products");
    assert_eq!(products.len(), 1);
    assert_eq!(products[0].get("title"), Some(&Value::Text("Widget".into())));
}

// ════════════════════════════════════════════════════════════════════════
// 4. DURABILITY — Data survives flush/restart
// ════════════════════════════════════════════════════════════════════════

#[test]
fn test_durability_flush_then_read() {
    let (db, _dir) = create_test_db();
    exec(&db, "CREATE TABLE durable (id INTEGER PRIMARY KEY, data TEXT)");

    for i in 1..=10i64 {
        exec(&db, &format!("INSERT INTO durable VALUES ({}, 'data_{}')", i, i));
    }

    db.flush().unwrap();

    let rows = select_maps(&db, "SELECT * FROM durable ORDER BY id");
    assert_eq!(rows.len(), 10);
    assert_eq!(rows[0].get("data"), Some(&Value::Text("data_1".into())));
    assert_eq!(rows[9].get("data"), Some(&Value::Text("data_10".into())));
}

#[test]
fn test_durability_data_persists_across_close() {
    let dir = TempDir::new().expect("create temp dir");
    let db_path = dir.path().to_path_buf();

    // Phase 1: Create, insert, checkpoint, close
    {
        let db = Database::create(&db_path).expect("create db");
        exec(&db, "CREATE TABLE persist_test (id INTEGER PRIMARY KEY, val TEXT)");
        exec(&db, "INSERT INTO persist_test VALUES (1, 'hello')");
        exec(&db, "INSERT INTO persist_test VALUES (2, 'world')");
        db.checkpoint().unwrap();
        db.close().unwrap();
    }

    // Phase 2: Reopen — data must survive
    {
        let db = open_test_db(&db_path);

        let rows = select_maps(&db, "SELECT * FROM persist_test ORDER BY id");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("val"), Some(&Value::Text("hello".into())));
        assert_eq!(rows[1].get("val"), Some(&Value::Text("world".into())));
    }
}

#[test]
fn test_durability_wal_recovery_after_unclean_shutdown() {
    let dir = TempDir::new().expect("create temp dir");
    let db_path = dir.path().to_path_buf();

    // Phase 1: Create and insert, flush but drop without close (simulate crash)
    {
        let db = Database::create(&db_path).expect("create db");
        exec(&db, "CREATE TABLE crash_test (id INTEGER PRIMARY KEY, payload TEXT)");
        exec(&db, "INSERT INTO crash_test VALUES (1, 'before_crash')");
        exec(&db, "INSERT INTO crash_test VALUES (2, 'also_before_crash')");
        db.flush().unwrap();
        drop(db);
    }

    // Phase 2: Reopen — WAL recovery should restore data
    {
        let db = open_test_db(&db_path);
        let rows = select_maps(&db, "SELECT * FROM crash_test ORDER BY id");
        assert!(rows.len() >= 2, "Expected at least 2 rows after WAL recovery, got {}", rows.len());

        let found_ids: Vec<i64> = rows.iter()
            .filter_map(|r| if let Some(Value::Integer(id)) = r.get("id") { Some(*id) } else { None })
            .collect();
        assert!(found_ids.contains(&1), "Row id=1 should exist after WAL recovery");
        assert!(found_ids.contains(&2), "Row id=2 should exist after WAL recovery");
    }
}

#[test]
fn test_durability_update_survives_restart() {
    let dir = TempDir::new().expect("create temp dir");
    let db_path = dir.path().to_path_buf();

    {
        let db = Database::create(&db_path).expect("create db");
        exec(&db, "CREATE TABLE up_test (id INTEGER PRIMARY KEY, v INTEGER)");
        exec(&db, "INSERT INTO up_test VALUES (1, 100)");
        exec(&db, "UPDATE up_test SET v = 200 WHERE id = 1");
        db.checkpoint().unwrap();
        db.close().unwrap();
    }

    {
        let db = open_test_db(&db_path);
        let rows = select_maps(&db, "SELECT * FROM up_test");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("v"), Some(&Value::Integer(200)));
    }
}

#[test]
fn test_durability_delete_survives_restart() {
    let dir = TempDir::new().expect("create temp dir");
    let db_path = dir.path().to_path_buf();

    {
        let db = Database::create(&db_path).expect("create db");
        exec(&db, "CREATE TABLE del_test (id INTEGER PRIMARY KEY, v TEXT)");
        exec(&db, "INSERT INTO del_test VALUES (1, 'a')");
        exec(&db, "INSERT INTO del_test VALUES (2, 'b')");
        exec(&db, "INSERT INTO del_test VALUES (3, 'c')");
        exec(&db, "DELETE FROM del_test WHERE id = 2");
        db.checkpoint().unwrap();
        db.close().unwrap();
    }

    {
        let db = open_test_db(&db_path);
        let rows = select_maps(&db, "SELECT * FROM del_test ORDER BY id");
        assert_eq!(rows.len(), 2);
        let ids: Vec<i64> = rows.iter()
            .filter_map(|r| if let Some(Value::Integer(id)) = r.get("id") { Some(*id) } else { None })
            .collect();
        assert_eq!(ids, vec![1, 3]);
    }
}

#[test]
fn test_durability_full_crud_cycle_across_restart() {
    let dir = TempDir::new().expect("create temp dir");
    let db_path = dir.path().to_path_buf();

    // Phase 1: Full CRUD cycle
    {
        let db = Database::create(&db_path).expect("create db");
        exec(&db, "CREATE TABLE cycle (id INTEGER PRIMARY KEY, v TEXT)");

        for i in 1..=5i64 {
            exec(&db, &format!("INSERT INTO cycle VALUES ({}, 'v{}')", i, i));
        }
        exec(&db, "UPDATE cycle SET v = 'updated' WHERE id = 3");
        exec(&db, "DELETE FROM cycle WHERE id = 5");

        db.checkpoint().unwrap();
        db.close().unwrap();
    }

    // Phase 2: Verify all operations survived
    {
        let db = open_test_db(&db_path);
        let rows = select_maps(&db, "SELECT * FROM cycle ORDER BY id");
        assert_eq!(rows.len(), 4, "Should have 4 rows (5 inserted - 1 deleted)");

        assert_eq!(rows[2].get("v"), Some(&Value::Text("updated".into())));
        let ids: Vec<i64> = rows.iter()
            .filter_map(|r| if let Some(Value::Integer(id)) = r.get("id") { Some(*id) } else { None })
            .collect();
        assert_eq!(ids, vec![1, 2, 3, 4]);
    }
}

// ════════════════════════════════════════════════════════════════════════
// 5. MULTI-TABLE DURABILITY — Multi-table data survives restart
// ════════════════════════════════════════════════════════════════════════

#[test]
fn test_multi_table_same_key_persists_across_restart() {
    let dir = TempDir::new().expect("create temp dir");
    let db_path = dir.path().to_path_buf();

    {
        let db = Database::create(&db_path).expect("create db");
        exec(&db, "CREATE TABLE alpha (id INTEGER PRIMARY KEY, data TEXT)");
        exec(&db, "CREATE TABLE beta (id INTEGER PRIMARY KEY, data TEXT)");

        exec(&db, "INSERT INTO alpha VALUES (42, 'alpha_data')");
        exec(&db, "INSERT INTO beta VALUES (42, 'beta_data')");

        db.checkpoint().unwrap();
        db.close().unwrap();
    }

    {
        let db = open_test_db(&db_path);
        let alpha = select_maps(&db, "SELECT * FROM alpha WHERE id = 42");
        let beta = select_maps(&db, "SELECT * FROM beta WHERE id = 42");

        assert_eq!(alpha[0].get("data"), Some(&Value::Text("alpha_data".into())));
        assert_eq!(beta[0].get("data"), Some(&Value::Text("beta_data".into())));
    }
}

// ════════════════════════════════════════════════════════════════════════
// 6. STRESS — Large dataset CRUD
// ════════════════════════════════════════════════════════════════════════

#[test]
fn test_stress_large_dataset_crud() {
    let (db, _dir) = create_test_db();
    exec(&db, "CREATE TABLE big (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)");

    const N: i64 = 200;

    for i in 1..=N {
        exec(&db, &format!("INSERT INTO big VALUES ({}, 'user_{}', {})", i, i, i * 10));
    }

    assert_eq!(count_rows(&db, "big"), N as usize);

    // Update every 10th row
    for i in (10..=N).step_by(10) {
        exec(&db, &format!("UPDATE big SET score = -1 WHERE id = {}", i));
    }

    // Delete rows with id % 10 == 5
    for i in (5..=N).step_by(10) {
        exec(&db, &format!("DELETE FROM big WHERE id = {}", i));
    }

    let remaining = count_rows(&db, "big");
    let expected = (N as usize) - (N as usize / 10);
    assert_eq!(remaining, expected, "row count mismatch after mixed CRUD");

    // Verify updated row
    let rows = select_maps(&db, "SELECT * FROM big WHERE id = 10");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("score"), Some(&Value::Integer(-1)));

    // Verify deleted row
    let rows = select_maps(&db, "SELECT * FROM big WHERE id = 5");
    assert_eq!(rows.len(), 0);

    // Verify normal row
    let rows = select_maps(&db, "SELECT * FROM big WHERE id = 1");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("score"), Some(&Value::Integer(10)));
}

#[test]
fn test_stress_restart_with_large_dataset() {
    let dir = TempDir::new().expect("create temp dir");
    let db_path = dir.path().to_path_buf();
    const N: i64 = 100;

    {
        let db = Database::create(&db_path).expect("create db");
        exec(&db, "CREATE TABLE restart_test (id INTEGER PRIMARY KEY, v INTEGER)");

        for i in 1..=N {
            exec(&db, &format!("INSERT INTO restart_test VALUES ({}, {})", i, i * 100));
        }

        for i in (10..=N).step_by(10) {
            exec(&db, &format!("UPDATE restart_test SET v = -{} WHERE id = {}", i, i));
        }

        for i in (5..=N).step_by(10) {
            exec(&db, &format!("DELETE FROM restart_test WHERE id = {}", i));
        }

        db.checkpoint().unwrap();
        db.close().unwrap();
    }

    {
        let db = open_test_db(&db_path);
        let remaining = count_rows(&db, "restart_test");
        let expected = (N as usize) - (N as usize / 10);
        assert_eq!(remaining, expected);

        let rows = select_maps(&db, "SELECT * FROM restart_test WHERE id = 10");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("v"), Some(&Value::Integer(-10)));

        let rows = select_maps(&db, "SELECT * FROM restart_test WHERE id = 5");
        assert_eq!(rows.len(), 0);

        let rows = select_maps(&db, "SELECT * FROM restart_test WHERE id = 1");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("v"), Some(&Value::Integer(100)));
    }
}

