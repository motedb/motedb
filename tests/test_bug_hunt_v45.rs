//! Bug Hunt v45 — Crash recovery: uncommitted transaction rollback on close.
//!
//! When `close()` was called with an active (uncommitted) transaction, the
//! uncommitted UPDATE changes persisted to disk (via ColSegmentStore buffer
//! flush). On reopen, the database showed the uncommitted values instead of
//! the last committed state — a critical data integrity bug.
//!
//! Fixed by rolling back any active transaction in `close()` before flushing:
//!   1. Replay the undo log (restores pre-UPDATE/DELETE values in storage)
//!   2. The write_set (uncommitted INSERTs) is discarded with the context

use motedb::sql::QueryResult;
use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn q(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).and_then(|r| r.materialize()) {
        Ok(QueryResult::Select { rows, .. }) => rows,
        _ => vec![],
    }
}

#[test]
fn test_crash_recovery_update_rollback() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    // Setup committed data
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        db.flush().unwrap();
        db.checkpoint().unwrap();
    }

    // Start txn, UPDATE, close WITHOUT commit
    {
        let db = Database::open(&path).unwrap();
        db.execute("BEGIN").unwrap();
        db.execute("UPDATE t SET v = 999 WHERE id = 1").unwrap();
        let r = q(&db, "SELECT v FROM t WHERE id = 1");
        assert_eq!(r[0][0], Value::Integer(999)); // read-your-writes
        db.close().unwrap(); // crash simulation
    }

    // Reopen — UPDATE should be rolled back
    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r[0][0], Value::Integer(10), "uncommitted UPDATE must rollback on close");
}

#[test]
fn test_crash_recovery_insert_rollback() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        db.flush().unwrap();
        db.checkpoint().unwrap();
    }

    // Uncommitted INSERT
    {
        let db = Database::open(&path).unwrap();
        db.execute("BEGIN").unwrap();
        db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
        let r = q(&db, "SELECT COUNT(*) FROM t");
        assert_eq!(r[0][0], Value::Integer(2));
        db.close().unwrap();
    }

    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r[0][0], Value::Integer(1), "uncommitted INSERT must rollback on close");
}

#[test]
fn test_crash_recovery_delete_rollback() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();
        db.flush().unwrap();
        db.checkpoint().unwrap();
    }

    // Uncommitted DELETE
    {
        let db = Database::open(&path).unwrap();
        db.execute("BEGIN").unwrap();
        db.execute("DELETE FROM t WHERE id = 2").unwrap();
        let r = q(&db, "SELECT COUNT(*) FROM t");
        assert_eq!(r[0][0], Value::Integer(1));
        db.close().unwrap();
    }

    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r[0][0], Value::Integer(2), "uncommitted DELETE must rollback on close");
}

#[test]
fn test_committed_transaction_survives_close() {
    // Regression: committed data must still survive close.
    // NOTE: UPDATE-in-transaction persistence has a known MVCC limitation
    // (the UPDATE value may not persist across reopen). This test verifies
    // that committed INSERTs survive, which is the critical case.
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
        db.execute("BEGIN").unwrap();
        db.execute("INSERT INTO t VALUES (1, 99)").unwrap();
        db.execute("COMMIT").unwrap();
        db.close().unwrap();
    }

    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r[0][0], Value::Integer(99), "committed INSERT must persist");
}

#[test]
fn test_no_active_transaction_close_normal() {
    // Regression: close without any transaction should work normally.
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 42)").unwrap();
        db.close().unwrap();
    }

    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r[0][0], Value::Integer(42));
}

// =========================================================================
// UPDATE in transaction: INSERT + UPDATE + COMMIT persistence
// =========================================================================

#[test]
fn test_txn_insert_then_update_persists() {
    // INSERT then UPDATE in same transaction, COMMIT, reopen.
    // Previously: COMMIT flushed the stale INSERT value (overwriting UPDATE).
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
        db.execute("BEGIN").unwrap();
        db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        db.execute("UPDATE t SET v = 99 WHERE id = 1").unwrap();
        // Read-your-writes: should see 99
        let r = q(&db, "SELECT v FROM t WHERE id = 1");
        assert_eq!(r[0][0], Value::Integer(99), "read-your-writes after UPDATE in txn");
        db.execute("COMMIT").unwrap();
        db.close().unwrap();
    }

    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r[0][0], Value::Integer(99), "committed INSERT+UPDATE must persist");
}

#[test]
fn test_txn_insert_then_update_multiple() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
        db.execute("BEGIN").unwrap();
        db.execute("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();
        db.execute("UPDATE t SET v = v + 100 WHERE id > 0").unwrap();
        db.execute("COMMIT").unwrap();
        db.close().unwrap();
    }

    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(r, vec![
        vec![Value::Integer(1), Value::Integer(110)],
        vec![Value::Integer(2), Value::Integer(120)],
    ]);
}

#[test]
fn test_txn_update_precommitted_row() {
    // UPDATE a row that was committed BEFORE the transaction.
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        db.flush().unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Database::open(&path).unwrap();
        db.execute("BEGIN").unwrap();
        db.execute("UPDATE t SET v = 50 WHERE id = 1").unwrap();
        db.execute("COMMIT").unwrap();
        db.close().unwrap();
    }

    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r[0][0], Value::Integer(50), "committed UPDATE on pre-existing row");
}

// =========================================================================
// DELETE in transaction: write_set merge
// =========================================================================

#[test]
fn test_txn_delete_uncommitted_insert() {
    // INSERT then DELETE in same txn, COMMIT — row should be gone.
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
        db.execute("BEGIN").unwrap();
        db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        db.execute("DELETE FROM t WHERE id = 1").unwrap();
        let r = q(&db, "SELECT COUNT(*) FROM t");
        assert_eq!(r[0][0], Value::Integer(0), "row should be deleted in-txn");
        db.execute("COMMIT").unwrap();
        db.close().unwrap();
    }

    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r[0][0], Value::Integer(0), "deleted INSERT must not persist");
}

#[test]
fn test_txn_delete_some_inserts() {
    // INSERT 3 rows, DELETE 1, COMMIT — 2 should remain.
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
        db.execute("BEGIN").unwrap();
        db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();
        db.execute("DELETE FROM t WHERE id = 2").unwrap();
        db.execute("COMMIT").unwrap();
        db.close().unwrap();
    }

    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT id FROM t ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

#[test]
fn test_txn_delete_precommitted_row() {
    // DELETE a row committed before the transaction.
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();
        db.flush().unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Database::open(&path).unwrap();
        db.execute("BEGIN").unwrap();
        db.execute("DELETE FROM t WHERE id = 1").unwrap();
        db.execute("COMMIT").unwrap();
        db.close().unwrap();
    }

    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT id FROM t");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_txn_mixed_insert_update_delete_commit() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();
        db.flush().unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Database::open(&path).unwrap();
        db.execute("BEGIN").unwrap();
        db.execute("INSERT INTO t VALUES (3, 30)").unwrap();
        db.execute("UPDATE t SET v = 99 WHERE id = 1").unwrap();
        db.execute("DELETE FROM t WHERE id = 2").unwrap();
        db.execute("COMMIT").unwrap();
        db.close().unwrap();
    }

    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(r, vec![
        vec![Value::Integer(1), Value::Integer(99)],
        vec![Value::Integer(3), Value::Integer(30)],
    ]);
}
