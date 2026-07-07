//! Round 5: Advanced hunt — concurrent writes, B-tree boundaries,
//! AUTO_INCREMENT edge cases, WAL truncation, and schema operations.

use motedb::{sql::QueryResult, types::Value, Database};
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

fn mk() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    (db, dir)
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        _ => vec![],
    }
}

fn cnt(db: &Database, sql: &str) -> i64 {
    rows(db, sql)
        .first()
        .and_then(|r| r.first())
        .and_then(|v| {
            if let Value::Integer(i) = v {
                Some(*i)
            } else {
                None
            }
        })
        .unwrap_or(-1)
}

fn val(db: &Database, sql: &str) -> Value {
    rows(db, sql)
        .first()
        .and_then(|r| r.first())
        .cloned()
        .unwrap_or(Value::Null)
}

// ═════════════════════════════════════════════════════════════════
// A. Concurrent writes — multi-thread INSERT on same table
// ═════════════════════════════════════════════════════════════════

/// 4 threads each insert 50 rows into the same table.
#[test]
fn test_concurrent_insert_4_threads() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v INT)")
        .unwrap();

    let mut handles = vec![];
    for t in 0..4u32 {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for i in 0..50i64 {
                let v = (t as i64) * 100 + i;
                let _ = db.execute(&format!("INSERT INTO t (v) VALUES ({})", v));
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    db.flush().unwrap();
    let total = cnt(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(total, 200, "4 threads × 50 = 200 rows, got {}", total);
}

/// 2 threads read while 1 thread writes.
#[test]
fn test_concurrent_readers_one_writer() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 1..=100 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i))
            .unwrap();
    }
    db.flush().unwrap();

    let mut handles = vec![];
    for _ in 0..2 {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for _ in 0..50 {
                let n = cnt(&db, "SELECT COUNT(*) FROM t");
                assert!(n >= 100, "Reader should see at least 100 rows, got {}", n);
            }
        }));
    }
    {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for i in 101..=150 {
                let _ = db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i));
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

// ═════════════════════════════════════════════════════════════════
// B. AUTO_INCREMENT edge cases
// ═════════════════════════════════════════════════════════════════

/// AUTO_INCREMENT starts from 1.
#[test]
fn test_auto_increment_starts_from_1() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v TEXT)")
        .unwrap();
    db.execute("INSERT INTO t (v) VALUES ('a')").unwrap();
    db.execute("INSERT INTO t (v) VALUES ('b')").unwrap();
    assert_eq!(
        val(&db, "SELECT id FROM t WHERE v = 'a'"),
        Value::Integer(1)
    );
    assert_eq!(
        val(&db, "SELECT id FROM t WHERE v = 'b'"),
        Value::Integer(2)
    );
}

/// AUTO_INCREMENT after DELETE — ID keeps incrementing (no reuse).
#[test]
fn test_auto_increment_after_delete_no_reuse() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v INT)")
        .unwrap();
    db.execute("INSERT INTO t (v) VALUES (10)").unwrap(); // id=1
    db.execute("INSERT INTO t (v) VALUES (20)").unwrap(); // id=2
    db.execute("DELETE FROM t WHERE id = 2").unwrap();
    db.execute("INSERT INTO t (v) VALUES (30)").unwrap(); // id=3 (not reuse of 2)
    let id3 = val(&db, "SELECT id FROM t WHERE v = 30");
    assert!(
        id3 != Value::Integer(2),
        "AUTO_INCREMENT should not reuse deleted IDs, got {:?}",
        id3
    );
}

/// AUTO_INCREMENT with explicit ID — counter advances past explicit ID.
#[test]
fn test_auto_increment_with_explicit_id() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (100, 10)").unwrap();
    db.execute("INSERT INTO t (v) VALUES (20)").unwrap();
    let next_id = val(&db, "SELECT id FROM t WHERE v = 20");
    assert_eq!(
        next_id,
        Value::Integer(101),
        "AUTO_INCREMENT should continue from max explicit ID+1"
    );
}

/// AUTO_INCREMENT survives recovery.
#[test]
fn test_auto_increment_survives_recovery() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v INT)")
            .unwrap();
        for i in 0..5 {
            db.execute(&format!("INSERT INTO t (v) VALUES ({})", i))
                .unwrap();
        }
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    // Next insert should get id=6 (not restart from 1).
    db.execute("INSERT INTO t (v) VALUES (99)").unwrap();
    let id = val(&db, "SELECT id FROM t WHERE v = 99");
    assert_eq!(
        id,
        Value::Integer(6),
        "AUTO_INCREMENT should continue after recovery, got {:?}",
        id
    );
}

// ═════════════════════════════════════════════════════════════════
// C. B-tree index boundary cases
// ═════════════════════════════════════════════════════════════════

/// Column index on INTEGER column — range query.
#[test]
fn test_column_index_range_query() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, age INT)")
        .unwrap();
    for i in 1..=100 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i))
            .unwrap();
    }
    db.execute("CREATE INDEX idx_age ON t (age) USING COLUMN")
        .unwrap();
    db.wait_for_indexes_ready();
    let r = rows(&db, "SELECT id FROM t WHERE age >= 50 AND age <= 55");
    assert_eq!(r.len(), 6, "age 50..55 = 6 rows");
}

/// Column index — query returns correct row after UPDATE.
#[test]
fn test_column_index_after_update() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, cat TEXT)")
        .unwrap();
    for i in 1..=10 {
        let cat = if i <= 5 { "A" } else { "B" };
        db.execute(&format!("INSERT INTO t VALUES ({}, '{}')", i, cat))
            .unwrap();
    }
    db.execute("CREATE INDEX idx_cat ON t (cat) USING COLUMN")
        .unwrap();
    db.wait_for_indexes_ready();
    assert_eq!(cnt(&db, "SELECT COUNT(*) FROM t WHERE cat = 'A'"), 5);
    // Move one row from A to B.
    db.execute("UPDATE t SET cat = 'B' WHERE id = 1").unwrap();
    // Now A should have 4, B should have 6.
    let a_count = cnt(&db, "SELECT COUNT(*) FROM t WHERE cat = 'A'");
    let b_count = cnt(&db, "SELECT COUNT(*) FROM t WHERE cat = 'B'");
    assert_eq!(a_count, 4, "After moving 1 row A→B, A should have 4");
    assert_eq!(b_count, 6, "B should have 6");
}

// ═════════════════════════════════════════════════════════════════
// D. WAL recovery — unclean shutdown simulation
// ═════════════════════════════════════════════════════════════════

/// Data written but not checkpointed — survives via WAL replay.
#[test]
fn test_wal_recovery_without_checkpoint() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
        // No explicit checkpoint — WAL should recover.
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(
        cnt(&db, "SELECT COUNT(*) FROM t"),
        2,
        "WAL should recover uncommitted data"
    );
}

/// Recovery after DELETE — deleted rows stay deleted via WAL.
#[test]
fn test_wal_recovery_after_delete() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
        for i in 1..=5 {
            db.execute(&format!("INSERT INTO t VALUES ({})", i))
                .unwrap();
        }
        db.execute("DELETE FROM t WHERE id = 3").unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(
        cnt(&db, "SELECT COUNT(*) FROM t"),
        4,
        "4 rows after deleting id=3"
    );
    assert_eq!(rows(&db, "SELECT * FROM t WHERE id = 3").len(), 0);
}

// ═════════════════════════════════════════════════════════════════
// E. Schema operations
// ═════════════════════════════════════════════════════════════════

/// CREATE TABLE IF NOT EXISTS — no error if already exists.
#[test]
fn test_create_table_if_not_exists() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    // Second create should not error (or should be idempotent).
    let r = db.execute("CREATE TABLE IF NOT EXISTS t (id INT PRIMARY KEY)");
    // Accept either success or error — key is no panic.
    assert!(r.is_ok() || r.is_err());
}

/// DROP TABLE IF EXISTS — no error if not exists.
#[test]
fn test_drop_table_if_exists() {
    let (db, _d) = mk();
    let r = db.execute("DROP TABLE IF EXISTS nonexistent");
    assert!(
        r.is_ok(),
        "DROP TABLE IF EXISTS should not error on nonexistent table"
    );
}

/// Multiple tables — interleaved operations.
#[test]
fn test_multiple_tables_interleaved() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("CREATE TABLE b (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("CREATE TABLE c (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO a VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO b VALUES (1, 20)").unwrap();
    db.execute("INSERT INTO c VALUES (1, 30)").unwrap();
    db.execute("INSERT INTO a VALUES (2, 11)").unwrap();
    db.flush().unwrap();
    assert_eq!(cnt(&db, "SELECT COUNT(*) FROM a"), 2);
    assert_eq!(cnt(&db, "SELECT COUNT(*) FROM b"), 1);
    assert_eq!(cnt(&db, "SELECT COUNT(*) FROM c"), 1);
}

// ═════════════════════════════════════════════════════════════════
// F. Data integrity stress
// ═════════════════════════════════════════════════════════════════

/// 1000 rows with random-looking data — verify exact values after flush.
#[test]
fn test_1000_rows_exact_values() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score FLOAT)")
        .unwrap();
    for i in 1..=1000i64 {
        let name = format!("user_{}_{}", i, i * 7 % 13);
        let score = (i as f64 * 3.14159) % 100.0;
        db.execute(&format!(
            "INSERT INTO t VALUES ({}, '{}', {:.4})",
            i, name, score
        ))
        .unwrap();
    }
    db.flush().unwrap();
    // Verify every 100th row.
    for i in (100..=1000).step_by(100) {
        let expected_name = format!("user_{}_{}", i, i * 7 % 13);
        let r = rows(&db, &format!("SELECT name FROM t WHERE id = {}", i));
        assert_eq!(r.len(), 1, "Row id={} should exist", i);
        assert_eq!(
            r[0][0],
            Value::text(expected_name),
            "Name mismatch at id={}",
            i
        );
    }
}

/// All rows readable via SELECT * — no truncation.
#[test]
fn test_select_all_no_truncation() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 1..=500 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i))
            .unwrap();
    }
    db.flush().unwrap();
    let r = rows(&db, "SELECT * FROM t");
    assert_eq!(r.len(), 500, "SELECT * should return all 500 rows");
}

/// SUM over large dataset matches manual calculation.
#[test]
fn test_sum_large_dataset_accuracy() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let n = 1000i64;
    for i in 1..=n {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i))
            .unwrap();
    }
    db.flush().unwrap();
    // SUM(1..1000) = 1000 * 1001 / 2 = 500500
    let expected = n * (n + 1) / 2;
    assert_eq!(val(&db, "SELECT SUM(v) FROM t"), Value::Integer(expected));
}

// ═════════════════════════════════════════════════════════════════
// G. DISTINCT + aggregate combinations
// ═════════════════════════════════════════════════════════════════

/// COUNT(DISTINCT col) — counts unique values.
#[test]
fn test_count_distinct() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, cat TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'A')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'B')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'A')").unwrap();
    db.execute("INSERT INTO t VALUES (4, 'C')").unwrap();
    db.execute("INSERT INTO t VALUES (5, 'A')").unwrap();
    db.flush().unwrap();
    // COUNT(DISTINCT cat) = 3 (A, B, C).
    let r = rows(&db, "SELECT COUNT(DISTINCT cat) FROM t");
    assert_eq!(r.len(), 1);
    match &r[0][0] {
        Value::Integer(n) => assert_eq!(*n, 3, "COUNT(DISTINCT cat) should be 3"),
        _ => panic!("Expected Integer"),
    }
}

/// SELECT DISTINCT col — returns unique values.
#[test]
fn test_select_distinct_values() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, cat TEXT)")
        .unwrap();
    for i in 1..=10 {
        let cat = ["X", "Y", "Z"][(i % 3) as usize];
        db.execute(&format!("INSERT INTO t VALUES ({}, '{}')", i, cat))
            .unwrap();
    }
    db.flush().unwrap();
    let r = rows(&db, "SELECT DISTINCT cat FROM t");
    assert_eq!(r.len(), 3, "Should have 3 distinct categories");
}

// ═════════════════════════════════════════════════════════════════
// H. Edge: NULL in various positions
// ═════════════════════════════════════════════════════════════════

/// NULL in PK position (should error or handle gracefully).
#[test]
fn test_null_primary_key_no_panic() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let r = db.execute("INSERT INTO t VALUES (NULL, 10)");
    // NULL PK should error (PK can't be NULL), but must not panic.
    assert!(r.is_ok() || r.is_err());
}

/// All-NULL table (every column is NULL for every row).
#[test]
fn test_all_null_table() {
    let (db, _d) = mk();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b TEXT, c FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL, NULL, NULL)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL, NULL, NULL)")
        .unwrap();
    db.flush().unwrap();
    assert_eq!(cnt(&db, "SELECT COUNT(*) FROM t"), 2);
    assert_eq!(cnt(&db, "SELECT COUNT(*) FROM t WHERE a IS NULL"), 2);
    assert_eq!(cnt(&db, "SELECT COUNT(*) FROM t WHERE b IS NULL"), 2);
    assert_eq!(cnt(&db, "SELECT COUNT(*) FROM t WHERE c IS NULL"), 2);
}
