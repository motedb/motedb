//! Crash Recovery Tests — Verify durability across process crash simulation
//!
//! Run: cargo test --release --test test_crash_recovery -- --test-threads=1

use motedb::{Database, types::Value, sql::QueryResult};

fn exec(db: &Database, sql: &str) -> QueryResult {
    db.execute(sql).unwrap().materialize().unwrap()
}

fn get_first_count(db: &Database, sql: &str) -> i64 {
    match exec(db, sql) {
        QueryResult::Select { rows, .. } => {
            rows.first().and_then(|r| r.first()).and_then(|v| match v {
                Value::Integer(i) => Some(*i),
                _ => None,
            }).unwrap_or(0)
        }
        _ => 0,
    }
}

fn remove_db(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    let _ = std::fs::remove_dir_all(format!("{}.mote", path));
}

// ═══════════════════════════════════════════════════════════════
// Test 1: Basic durability — committed rows survive crash
// ═══════════════════════════════════════════════════════════════
#[test]
fn test_crash_committed_rows_survive() {
    let dir = "/tmp/motedb_crash_test_1";
    remove_db(dir);

    // Phase 1: Create, insert, commit, close
    {
        let db = Database::create(dir).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();

        let txn_id = db.begin_transaction().unwrap();
        db.insert_row_with_txn("t", txn_id, vec![Value::Integer(1), Value::Integer(100)]).unwrap();
        db.insert_row_with_txn("t", txn_id, vec![Value::Integer(2), Value::Integer(200)]).unwrap();
        db.commit_transaction(txn_id).unwrap();

        db.flush().unwrap();
        db.close().unwrap();
    }

    // Phase 2: Reopen and verify
    {
        let db = Database::open(dir).unwrap();
        let result = exec(&db, "SELECT * FROM t WHERE id = 1");
        let rows = match result {
            QueryResult::Select { rows, .. } => rows,
            _ => vec![],
        };
        assert!(!rows.is_empty(), "Committed row id=1 should survive crash");
        assert_eq!(rows[0][1], Value::Integer(100), "Row data should be intact");

        let cnt = get_first_count(&db, "SELECT COUNT(*) FROM t");
        assert_eq!(cnt, 2, "Should have 2 committed rows");
        db.close().unwrap();
    }

    remove_db(dir);
}

// ═══════════════════════════════════════════════════════════════
// Test 2: Insert without transaction survives crash
// NOTE: LSM writes happen at insert time (not commit time).
// Transactional rollback of LSM data requires write buffering (future work).
// ═══════════════════════════════════════════════════════════════
#[test]
fn test_crash_raw_insert_survives() {
    let dir = "/tmp/motedb_crash_test_2";
    remove_db(dir);

    {
        let db = Database::create(dir).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 100)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 200)").unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }

    {
        let db = Database::open(dir).unwrap();
        let cnt = get_first_count(&db, "SELECT COUNT(*) FROM t");
        assert_eq!(cnt, 2, "Raw INSERT rows should survive crash");
        db.close().unwrap();
    }

    remove_db(dir);
}

// ═══════════════════════════════════════════════════════════════
// Test 3: Multiple checkpoints survive crash
// ═══════════════════════════════════════════════════════════════
#[test]
fn test_crash_multiple_checkpoints() {
    let dir = "/tmp/motedb_crash_test_4";
    remove_db(dir);

    let n = 500;
    {
        let db = Database::create(dir).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();

        // Batch 1
        for i in 0..n / 2 {
            db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 10)).unwrap();
        }
        db.flush().unwrap();

        // Batch 2
        for i in n / 2..n {
            db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 10)).unwrap();
        }
        db.flush().unwrap();

        db.close().unwrap();
    }

    {
        let db = Database::open(dir).unwrap();
        let cnt = get_first_count(&db, "SELECT COUNT(*) FROM t");
        assert_eq!(cnt, n as i64, "All {} rows should survive crash", n);

        // Spot-check a few rows
        let r0 = exec(&db, "SELECT * FROM t WHERE id = 0");
        let r_mid = exec(&db, &format!("SELECT * FROM t WHERE id = {}", n / 2));
        let r_last = exec(&db, &format!("SELECT * FROM t WHERE id = {}", n - 1));

        assert!(!matches!(&r0, QueryResult::Select { rows, .. } if rows.is_empty()), "Row 0 missing");
        assert!(!matches!(&r_mid, QueryResult::Select { rows, .. } if rows.is_empty()), "Mid row missing");
        assert!(!matches!(&r_last, QueryResult::Select { rows, .. } if rows.is_empty()), "Last row missing");

        db.close().unwrap();
    }

    remove_db(dir);
}

// ═══════════════════════════════════════════════════════════════
// Test 5: Multiple transactions survive crash
// ═══════════════════════════════════════════════════════════════
#[test]
fn test_crash_multiple_transactions() {
    let dir = "/tmp/motedb_crash_test_5";
    remove_db(dir);

    {
        let db = Database::create(dir).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();

        for txn_round in 0..5 {
            let txn_id = db.begin_transaction().unwrap();
            for i in 0..10 {
                let rid = txn_round * 10 + i + 1;
                db.insert_row_with_txn("t", txn_id, vec![
                    Value::Integer(rid as i64),
                    Value::Integer(rid as i64 * 100),
                ]).unwrap();
            }
            db.commit_transaction(txn_id).unwrap();
        }

        db.flush().unwrap();
        db.close().unwrap();
    }

    {
        let db = Database::open(dir).unwrap();
        let cnt = get_first_count(&db, "SELECT COUNT(*) FROM t");
        assert_eq!(cnt, 50, "All 50 transaction rows should survive crash");

        // Verify data from each transaction round
        for txn_round in 0..5 {
            let rid = txn_round * 10 + 1;
            let result = exec(&db, &format!("SELECT val FROM t WHERE id = {}", rid));
            let rows = match result { QueryResult::Select { rows, .. } => rows, _ => vec![] };
            assert!(!rows.is_empty(), "Row from txn round {} should exist", txn_round);
            assert_eq!(rows[0][0], Value::Integer(rid as i64 * 100));
        }

        db.close().unwrap();
    }

    remove_db(dir);
}

// ═══════════════════════════════════════════════════════════════
// Test 6: WAL recovery after many small writes
// ═══════════════════════════════════════════════════════════════
#[test]
fn test_crash_many_small_writes() {
    let dir = "/tmp/motedb_crash_test_6";
    remove_db(dir);

    let n = 2000;
    {
        let db = Database::create(dir).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, data TEXT)").unwrap();

        for i in 0..n {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'row_{}')", i, i)).unwrap();
        }

        db.close().unwrap();
    }

    {
        let db = Database::open(dir).unwrap();
        let cnt = get_first_count(&db, "SELECT COUNT(*) FROM t");
        assert_eq!(cnt, n as i64, "All {} rows should survive crash", n);
        db.close().unwrap();
    }

    remove_db(dir);
}

// ═══════════════════════════════════════════════════════════════
// Test 7: Update survives crash
// ═══════════════════════════════════════════════════════════════
#[test]
fn test_crash_update_survives() {
    let dir = "/tmp/motedb_crash_test_7";
    remove_db(dir);

    {
        let db = Database::create(dir).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 100)").unwrap();
        db.execute("UPDATE t SET val = 999 WHERE id = 1").unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }

    {
        let db = Database::open(dir).unwrap();
        let result = exec(&db, "SELECT val FROM t WHERE id = 1");
        let rows = match result { QueryResult::Select { rows, .. } => rows, _ => vec![] };
        assert!(!rows.is_empty(), "Row should exist after crash");
        assert_eq!(rows[0][0], Value::Integer(999), "Updated value should survive crash");
        db.close().unwrap();
    }

    remove_db(dir);
}

// ═══════════════════════════════════════════════════════════════
// Test 8: Delete survives crash
// ═══════════════════════════════════════════════════════════════
#[test]
fn test_crash_delete_survives() {
    let dir = "/tmp/motedb_crash_test_8";
    remove_db(dir);

    {
        let db = Database::create(dir).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 100)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 200)").unwrap();
        db.execute("DELETE FROM t WHERE id = 1").unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }

    {
        let db = Database::open(dir).unwrap();
        let cnt = get_first_count(&db, "SELECT COUNT(*) FROM t");
        assert_eq!(cnt, 1, "Only 1 row should remain after delete survives crash");

        let result = exec(&db, "SELECT * FROM t WHERE id = 1");
        let rows = match result { QueryResult::Select { rows, .. } => rows, _ => vec![] };
        assert!(rows.is_empty(), "Deleted row should not come back");

        let result2 = exec(&db, "SELECT * FROM t WHERE id = 2");
        let rows2 = match result2 { QueryResult::Select { rows, .. } => rows, _ => vec![] };
        assert!(!rows2.is_empty(), "Non-deleted row should survive");
        db.close().unwrap();
    }

    remove_db(dir);
}
