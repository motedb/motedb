//! Tests for concurrent multi-threaded database operations

use motedb::{Database, types::Value};
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

fn rows(result: motedb::StreamingQueryResult) -> Vec<Vec<Value>> {
    use motedb::QueryResult;
    match result.materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        _ => panic!("Expected Select result"),
    }
}

#[test]
fn test_concurrent_inserts() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();

    let n_threads = 4;
    let rows_per_thread = 100;
    let mut handles = vec![];

    for t in 0..n_threads {
        let db_clone = db.clone();
        let handle = thread::spawn(move || {
            let start = t * rows_per_thread;
            for i in start..start + rows_per_thread {
                db_clone.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 10)).unwrap();
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().unwrap();
    }

    let result = db.execute("SELECT COUNT(*) FROM t").unwrap();
    let r = rows(result);
    assert_eq!(&r[0][0], &Value::Integer((n_threads * rows_per_thread) as i64));
}

#[test]
fn test_concurrent_reads_writes() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();
    for i in 0..50 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap();
    }

    let db_writer = db.clone();
    let db_reader = db.clone();

    // Writer thread inserts more rows
    let writer = thread::spawn(move || {
        for i in 50..150 {
            db_writer.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap();
        }
    });

    // Reader thread counts rows
    let reader = thread::spawn(move || {
        for _ in 0..50 {
            let result = db_reader.execute("SELECT COUNT(*) FROM t").unwrap();
            let r = rows(result);
            // Count should be >= 50 (initial) and <= 150
            if let Value::Integer(count) = &r[0][0] {
                assert!(*count >= 50, "Count should be >= 50, got {}", count);
                assert!(*count <= 150, "Count should be <= 150, got {}", count);
            }
        }
    });

    writer.join().unwrap();
    reader.join().unwrap();

    let result = db.execute("SELECT COUNT(*) FROM t").unwrap();
    let r = rows(result);
    assert_eq!(&r[0][0], &Value::Integer(150));
}

#[test]
fn test_concurrent_transactions() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());

    db.execute("CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)").unwrap();
    for i in 0..10 {
        db.execute(&format!("INSERT INTO accounts VALUES ({}, 100)", i)).unwrap();
    }

    let n_threads = 4;
    let mut handles = vec![];

    for t in 0..n_threads {
        let db_clone = db.clone();
        let handle = thread::spawn(move || {
            // Each thread inserts new rows (no conflict)
            for j in 0..10 {
                let id = 100 + t * 10 + j;
                let tx = db_clone.begin_transaction().unwrap();
                db_clone.execute(&format!("INSERT INTO accounts VALUES ({}, 50)", id)).unwrap();
                let _ = db_clone.commit_transaction(tx);
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().unwrap();
    }

    let result = db.execute("SELECT COUNT(*) FROM accounts").unwrap();
    let r = rows(result);
    assert_eq!(&r[0][0], &Value::Integer(50)); // 10 initial + 4*10 = 50
}

#[test]
fn test_concurrent_select_different_tables() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());

    db.execute("CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)").unwrap();
    db.execute("CREATE TABLE t2 (id INT PRIMARY KEY, val TEXT)").unwrap();

    for i in 0..100 {
        db.execute(&format!("INSERT INTO t1 VALUES ({}, 'a{}')", i, i)).unwrap();
        db.execute(&format!("INSERT INTO t2 VALUES ({}, 'b{}')", i, i)).unwrap();
    }

    let db1 = db.clone();
    let db2 = db.clone();

    let h1 = thread::spawn(move || {
        for _ in 0..50 {
            let r = db1.execute("SELECT COUNT(*) FROM t1").unwrap();
            let rows = rows(r);
            assert_eq!(&rows[0][0], &Value::Integer(100));
        }
    });

    let h2 = thread::spawn(move || {
        for _ in 0..50 {
            let r = db2.execute("SELECT COUNT(*) FROM t2").unwrap();
            let rows = rows(r);
            assert_eq!(&rows[0][0], &Value::Integer(100));
        }
    });

    h1.join().unwrap();
    h2.join().unwrap();
}

#[test]
fn test_concurrent_insert_and_checkpoint() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();

    let db_writer = db.clone();
    let db_checkpointer = db.clone();

    let writer = thread::spawn(move || {
        for i in 0..200 {
            db_writer.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap();
        }
    });

    let checkpointer = thread::spawn(move || {
        for _ in 0..5 {
            let _ = db_checkpointer.checkpoint();
        }
    });

    writer.join().unwrap();
    checkpointer.join().unwrap();

    let result = db.execute("SELECT COUNT(*) FROM t").unwrap();
    let r = rows(result);
    assert_eq!(&r[0][0], &Value::Integer(200));
}
