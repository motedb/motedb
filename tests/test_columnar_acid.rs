/// Comprehensive ACID and columnar architecture tests
use motedb::{Database, DBConfig};
use motedb::types::Value;
use tempfile::TempDir;

fn create_db() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT, val FLOAT, region TEXT)").unwrap();
    (dir, db)
}

fn count_rows(db: &Database, sql: &str) -> usize {
    match db.execute(sql).unwrap().materialize().unwrap() {
        motedb::QueryResult::Select { rows, .. } => rows.len(),
        _ => 0,
    }
}

// ── Basic CRUD ───────────────────────────────────────────────────

#[test]
fn test_insert_and_select() {
    let (_dir, db) = create_db();
    db.execute("INSERT INTO t (name, val, region) VALUES ('Alice', 1.0, 'US')").unwrap();
    db.execute("INSERT INTO t (name, val, region) VALUES ('Bob', 2.0, 'EU')").unwrap();
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 2);
}

#[test]
fn test_batch_insert() {
    let (_dir, db) = create_db();
    let rows: Vec<Vec<Value>> = (0..20).map(|i| vec![
        Value::Integer(i as i64),
        Value::Text(motedb::types::ArcString(std::sync::Arc::from(format!("user_{}", i)))),
        Value::Float(i as f64 * 1.5),
        Value::Text(motedb::types::ArcString(std::sync::Arc::from(if i % 2 == 0 { "US" } else { "EU" }))),
    ]).collect();
    db.batch_insert("t", rows).unwrap();
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 20);
}

#[test]
fn test_update() {
    let (_dir, db) = create_db();
    db.execute("INSERT INTO t (name, val, region) VALUES ('Alice', 1.0, 'US')").unwrap();
    db.execute("UPDATE t SET val = 99.0 WHERE name = 'Alice'").unwrap();
    let r = db.execute("SELECT val FROM t WHERE name = 'Alice'").unwrap().materialize().unwrap();
    match r {
        motedb::QueryResult::Select { rows, .. } => assert_eq!(rows[0][0], Value::Float(99.0)),
        _ => panic!(),
    }
}

#[test]
fn test_delete() {
    let (_dir, db) = create_db();
    db.execute("INSERT INTO t (name, val, region) VALUES ('Alice', 1.0, 'US')").unwrap();
    db.execute("INSERT INTO t (name, val, region) VALUES ('Bob', 2.0, 'EU')").unwrap();
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 2);
    db.execute("DELETE FROM t WHERE name = 'Alice'").unwrap();
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 1);
    assert_eq!(count_rows(&db, "SELECT * FROM t WHERE name = 'Bob'"), 1);
}

// ── Restart recovery ─────────────────────────────────────────────

#[test]
fn test_restart_recovery() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut config = DBConfig::for_edge();
        config.max_result_rows = None;
        let db = Database::create_with_config(&path, config).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT, val FLOAT, region TEXT)").unwrap();
        for i in 0..50 {
            db.execute(&format!("INSERT INTO t (name, val, region) VALUES ('user_{}', {}, '{}')",
                i, i as f64, if i % 2 == 0 { "US" } else { "EU" })).unwrap();
        }
    }
    // Reopen
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::open_with_config(&path, config).unwrap();
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 50);
}

// ── ACID: Transactions ───────────────────────────────────────────

#[test]
fn test_transaction_commit() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT, val FLOAT, region TEXT)").unwrap();
    db.execute("INSERT INTO t (name, val, region) VALUES ('Alice', 1.0, 'US')").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t (name, val, region) VALUES ('Bob', 2.0, 'EU')").unwrap();
    db.execute("COMMIT").unwrap();
    eprintln!("COMMIT done, before SELECT");
    let count = count_rows(&db, "SELECT * FROM t");
    eprintln!("count: {}", count);
    assert_eq!(count, 2);
    eprintln!("test passed, dropping db");
    drop(db);
    eprintln!("db dropped");
    drop(dir);
    eprintln!("dir dropped");
}

#[test]
fn test_transaction_rollback() {
    let (_dir, db) = create_db();
    db.execute("INSERT INTO t (name, val, region) VALUES ('Alice', 1.0, 'US')").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t (name, val, region) VALUES ('Bob', 2.0, 'EU')").unwrap();
    // Inside txn, both visible
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 2);
    db.execute("ROLLBACK").unwrap();
    // After rollback, only Alice
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 1);
}

// ── WHERE / LIKE filters ─────────────────────────────────────────

#[test]
fn test_where_filter() {
    let (_dir, db) = create_db();
    for i in 0..500 {
        db.execute(&format!("INSERT INTO t (name, val, region) VALUES ('user_{}', {}, '{}')",
            i, i as f64, if i % 3 == 0 { "US" } else { "EU" })).unwrap();
    }
    let n = count_rows(&db, "SELECT * FROM t WHERE region = 'US'");
    assert!(n > 150 && n < 180, "expected ~167 US rows, got {}", n);
}

#[test]
fn test_like_filter() {
    let (_dir, db) = create_db();
    for i in 0..500 {
        db.execute(&format!("INSERT INTO t (name, val, region) VALUES ('item_{}', {}, '{}')",
            i, i as f64, "X")).unwrap();
    }
    let n = count_rows(&db, "SELECT * FROM t WHERE name LIKE 'item_1%'");
    assert!(n >= 11, "expected >=11 item_1%, got {}", n);
}

// ── Aggregates ───────────────────────────────────────────────────

#[test]
fn test_aggregate() {
    let (_dir, db) = create_db();
    for i in 0..20 {
        db.execute(&format!("INSERT INTO t (name, val, region) VALUES ('u{}', {}, 'X')", i % 10, i as f64)).unwrap();
    }
    let r = db.execute("SELECT COUNT(*), SUM(val), AVG(val) FROM t").unwrap().materialize().unwrap();
    match r {
        motedb::QueryResult::Select { rows, .. } => {
            assert_eq!(rows[0][0], Value::Integer(20));
            assert_eq!(rows[0][1], Value::Float(190.0)); // 0+1+2+...+19
            assert_eq!(rows[0][2], Value::Float(9.5));
        }
        _ => panic!(),
    }
}

#[test]
fn test_group_by() {
    let (_dir, db) = create_db();
    for i in 0..30 {
        let r = if i % 3 == 0 { "US" } else if i % 3 == 1 { "EU" } else { "ASIA" };
        db.execute(&format!("INSERT INTO t (name, val, region) VALUES ('u{}', {}, '{}')", i % 10, i as f64, r)).unwrap();
    }
    let r = db.execute("SELECT region, COUNT(*), SUM(val) FROM t GROUP BY region").unwrap().materialize().unwrap();
    match r {
        motedb::QueryResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 3);
            for row in &rows {
                assert_eq!(row[1], Value::Integer(10));
            }
        }
        _ => panic!(),
    }
}

// ── Constraints ──────────────────────────────────────────────────

#[test]
fn test_pk_uniqueness() {
    let (_dir, db) = create_db();
    db.execute("CREATE TABLE t2 (id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO t2 (id, name) VALUES (1, 'Alice')").unwrap();
    assert!(db.execute("INSERT INTO t2 (id, name) VALUES (1, 'Bob')").is_err());
}

#[test]
fn test_not_null() {
    let (_dir, db) = create_db();
    db.execute("CREATE TABLE t3 (id INT PRIMARY KEY, name TEXT NOT NULL)").unwrap();
    assert!(db.execute("INSERT INTO t3 (id) VALUES (1)").is_err());
}

// ── Mixed INSERT paths ───────────────────────────────────────────

#[test]
fn test_mixed_insert() {
    let (_dir, db) = create_db();
    db.execute("INSERT INTO t (name, val, region) VALUES ('Alice', 1.0, 'US')").unwrap();
    let batch: Vec<Vec<Value>> = (0..10).map(|i| vec![
        Value::Integer(100 + i as i64),
        Value::Text(motedb::types::ArcString(std::sync::Arc::from(format!("b{}", i)))),
        Value::Float(i as f64),
        Value::Text(motedb::types::ArcString(std::sync::Arc::from("EU"))),
    ]).collect();
    db.batch_insert("t", batch).unwrap();
    db.execute("INSERT INTO t (name, val, region) VALUES ('Bob', 99.0, 'US')").unwrap();
    assert_eq!(count_rows(&db, "SELECT * FROM t"), 12);
}

// ── ORDER BY + LIMIT ─────────────────────────────────────────────

#[test]
fn test_order_by_limit() {
    let (_dir, db) = create_db();
    for i in 0..30 {
        db.execute(&format!("INSERT INTO t (name, val, region) VALUES ('u{}', {}, 'X')", i, (i as f64 * 1.7) % 1000.0)).unwrap();
    }
    let r = db.execute("SELECT * FROM t ORDER BY val DESC LIMIT 10").unwrap().materialize().unwrap();
    match r {
        motedb::QueryResult::Select { rows, .. } => assert_eq!(rows.len(), 10),
        _ => panic!(),
    }
}

// ── DISTINCT ─────────────────────────────────────────────────────

#[test]
fn test_distinct() {
    let (_dir, db) = create_db();
    for i in 0..300 {
        db.execute(&format!("INSERT INTO t (name, val, region) VALUES ('u{}', {}, '{}')",
            i % 10, i as f64, if i % 2 == 0 { "US" } else { "EU" })).unwrap();
    }
    let r = db.execute("SELECT DISTINCT region FROM t").unwrap().materialize().unwrap();
    match r {
        motedb::QueryResult::Select { rows, .. } => assert_eq!(rows.len(), 2),
        _ => panic!(),
    }
}
