//! ACID & Transaction correctness audit tests.
//!
//! These tests verify the documented transaction semantics actually hold:
//! - rollback undoes writes
//! - savepoint rollback undoes only post-savepoint writes
//! - commit durability
//! - execute vs execute_prepared consistency under transactions
//! - closed-database guards
//!
//! Run: cargo test --release --test test_acid_audit

use motedb::Database;
use tempfile::TempDir;

fn setup(table_sql: &str) -> (Database, TempDir) {
    let dir = TempDir::new().expect("temp dir");
    let db = Database::create(dir.path()).expect("create db");
    db.execute(table_sql).expect("create table");
    (db, dir)
}

fn count(db: &Database, table: &str) -> i64 {
    let sql = format!("SELECT COUNT(*) FROM {}", table);
    let rs = db.execute(&sql).expect("count").materialize().expect("mat");
    use motedb::sql::QueryResult;
    if let QueryResult::Select { rows, .. } = rs {
        if let Some(motedb::types::Value::Integer(n)) = rows.first().and_then(|r| r.first()) {
            return *n;
        }
    }
    panic!("no count result");
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. ROLLBACK must undo INSERTs
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn rollback_undoes_insert() {
    let (db, _dir) = setup("CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    db.execute("INSERT INTO t VALUES (1, 10)").expect("seed");

    let tx = db.begin_transaction().expect("begin");
    db.execute("INSERT INTO t VALUES (2, 20)").expect("insert in txn");

    db.rollback_transaction(tx).expect("rollback");
    // After rollback, only the seed row should remain
    assert_eq!(count(&db, "t"), 1, "rollback must undo the insert");
}

#[test]
fn rollback_undoes_update() {
    let (db, _dir) = setup("CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    db.execute("INSERT INTO t VALUES (1, 10)").expect("seed");

    let tx = db.begin_transaction().expect("begin");
    db.execute("UPDATE t SET v = 999 WHERE id = 1").expect("update in txn");
    db.rollback_transaction(tx).expect("rollback");

    // Original value must be restored
    let rs = db.execute("SELECT v FROM t WHERE id = 1")
        .expect("select").materialize().expect("mat");
    use motedb::sql::QueryResult;
    if let QueryResult::Select { rows, .. } = rs {
        assert_eq!(rows.len(), 1);
        match &rows[0][0] {
            motedb::types::Value::Integer(n) => assert_eq!(*n, 10, "rollback must restore v=10"),
            other => panic!("expected Integer, got {:?}", other),
        }
    }
}

#[test]
fn rollback_undoes_delete() {
    let (db, _dir) = setup("CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    db.execute("INSERT INTO t VALUES (1, 10)").expect("seed");
    db.execute("INSERT INTO t VALUES (2, 20)").expect("seed2");

    let tx = db.begin_transaction().expect("begin");
    db.execute("DELETE FROM t WHERE id = 1").expect("delete in txn");
    db.rollback_transaction(tx).expect("rollback");
    assert_eq!(count(&db, "t"), 2, "rollback must restore deleted row");
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. COMMIT must persist writes
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn commit_persists_writes() {
    let (db, _dir) = setup("CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let tx = db.begin_transaction().expect("begin");
    db.execute("INSERT INTO t VALUES (1, 10)").expect("insert");
    db.commit_transaction(tx).expect("commit");
    assert_eq!(count(&db, "t"), 1, "commit must persist the row");
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. SAVEPOINT rollback
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn savepoint_rollback_undoes_post_savepoint_only() {
    let (db, _dir) = setup("CREATE TABLE t (id INT PRIMARY KEY, v INT)");

    let tx = db.begin_transaction().expect("begin");
    db.execute("INSERT INTO t VALUES (1, 10)").expect("pre-savepoint insert");
    db.savepoint(tx, "sp1").expect("savepoint");
    db.execute("INSERT INTO t VALUES (2, 20)").expect("post-savepoint insert");

    db.rollback_to_savepoint(tx, "sp1").expect("rollback to savepoint");
    // After savepoint rollback + commit, only the pre-savepoint row should persist.
    db.commit_transaction(tx).expect("commit");
    assert_eq!(count(&db, "t"), 1, "only pre-savepoint row should remain after commit");
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. execute() vs execute_prepared() consistency under transactions
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn execute_prepared_respects_transaction() {
    let (db, _dir) = setup("CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let tx = db.begin_transaction().expect("begin");
    // Use execute_prepared with a parameterized INSERT
    db.execute_prepared(
        "INSERT INTO t VALUES (?, ?)",
        vec![
            motedb::types::Value::Integer(1),
            motedb::types::Value::Integer(10),
        ],
    )
    .expect("prepared insert in txn");
    db.rollback_transaction(tx).expect("rollback");
    assert_eq!(
        count(&db, "t"),
        0,
        "rollback must undo prepared insert too"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. Closed-database guards
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn execute_after_close_returns_error() {
    let (db, _dir) = setup("CREATE TABLE t (id INT PRIMARY KEY)");
    db.close().expect("close");
    let r = db.execute("SELECT * FROM t");
    assert!(r.is_err(), "execute after close must error");
}

#[test]
fn execute_prepared_after_close_returns_error() {
    let (db, _dir) = setup("CREATE TABLE t (id INT PRIMARY KEY)");
    // Warm the cache so it doesn't fail on parse
    let _ = db.execute_prepared("SELECT * FROM t WHERE id = ?", vec![motedb::types::Value::Integer(1)]);
    db.close().expect("close");
    let r = db.execute_prepared(
        "SELECT * FROM t WHERE id = ?",
        vec![motedb::types::Value::Integer(1)],
    );
    assert!(
        r.is_err(),
        "execute_prepared after close must error (currently missing guard)"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. Crash/restart durability of committed data
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn committed_data_survives_reopen() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).expect("create");
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").expect("ddl");
        for i in 1..=100 {
            db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 2)).expect("insert");
        }
        db.checkpoint().expect("checkpoint");
        db.close().expect("close");
    }
    // Reopen — all 100 rows must be present
    let db = Database::open(&path).expect("reopen");
    assert_eq!(count(&db, "t"), 100, "all committed rows must survive reopen");
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. Transaction isolation — uncommitted writes not visible after rollback
//    from a fresh connection perspective (reopen mid-transaction is not
//    possible, but verify rollback truly clears buffered writes).
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn rollback_then_commit_only_sees_committed() {
    let (db, _dir) = setup("CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    // Tx1: insert + rollback
    let tx1 = db.begin_transaction().expect("begin1");
    db.execute("INSERT INTO t VALUES (1, 10)").expect("insert tx1");
    db.rollback_transaction(tx1).expect("rollback tx1");

    // Tx2: insert + commit
    let tx2 = db.begin_transaction().expect("begin2");
    db.execute("INSERT INTO t VALUES (2, 20)").expect("insert tx2");
    db.commit_transaction(tx2).expect("commit tx2");

    assert_eq!(count(&db, "t"), 1, "only tx2's row should exist");
    // Verify it's row id=2, not id=1
    let rs = db.execute("SELECT id FROM t").expect("select").materialize().expect("mat");
    use motedb::sql::QueryResult;
    if let QueryResult::Select { rows, .. } = rs {
        assert_eq!(rows.len(), 1);
        match &rows[0][0] {
            motedb::types::Value::Integer(n) => assert_eq!(*n, 2, "should be the committed row id=2"),
            other => panic!("expected Integer, got {:?}", other),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. Empty transaction commit/rollback are no-ops
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn empty_transaction_commit_is_noop() {
    let (db, _dir) = setup("CREATE TABLE t (id INT PRIMARY KEY)");
    db.execute("INSERT INTO t VALUES (1)").expect("seed");
    let tx = db.begin_transaction().expect("begin");
    db.commit_transaction(tx).expect("commit");
    assert_eq!(count(&db, "t"), 1);
}

#[test]
fn empty_transaction_rollback_is_noop() {
    let (db, _dir) = setup("CREATE TABLE t (id INT PRIMARY KEY)");
    db.execute("INSERT INTO t VALUES (1)").expect("seed");
    let tx = db.begin_transaction().expect("begin");
    db.rollback_transaction(tx).expect("rollback");
    assert_eq!(count(&db, "t"), 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// 9. Read-your-writes: SELECT inside a transaction must see uncommitted writes
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn ryw_insert_visible_in_full_scan() {
    let (db, _dir) = setup("CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    db.execute("INSERT INTO t VALUES (1, 10)").expect("seed");
    let tx = db.begin_transaction().expect("begin");
    db.execute("INSERT INTO t VALUES (2, 20)").expect("insert in txn");
    // SELECT * must see 2 rows (1 committed + 1 uncommitted)
    assert_eq!(count(&db, "t"), 2, "full scan must see uncommitted insert");
    db.rollback_transaction(tx).expect("rollback");
}

#[test]
fn ryw_insert_visible_in_pk_point_query() {
    let (db, _dir) = setup("CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let tx = db.begin_transaction().expect("begin");
    db.execute("INSERT INTO t VALUES (42, 99)").expect("insert in txn");
    // PK point query must find the uncommitted row
    let rs = db.execute("SELECT v FROM t WHERE id = 42").expect("select").materialize().expect("mat");
    use motedb::sql::QueryResult;
    if let QueryResult::Select { rows, .. } = rs {
        assert_eq!(rows.len(), 1, "pk point query must see uncommitted insert");
        match &rows[0][0] {
            motedb::types::Value::Integer(n) => assert_eq!(*n, 99),
            other => panic!("expected Integer 99, got {:?}", other),
        }
    } else {
        panic!("expected Select result");
    }
    db.rollback_transaction(tx).expect("rollback");
}

#[test]
fn ryw_count_reflects_uncommitted_writes() {
    let (db, _dir) = setup("CREATE TABLE t (id INT PRIMARY KEY)");
    db.execute("INSERT INTO t VALUES (1)").expect("seed");
    let tx = db.begin_transaction().expect("begin");
    db.execute("INSERT INTO t VALUES (2)").expect("insert in txn");
    db.execute("INSERT INTO t VALUES (3)").expect("insert in txn");
    assert_eq!(count(&db, "t"), 3, "COUNT(*) must reflect uncommitted inserts");
    db.rollback_transaction(tx).expect("rollback");
    assert_eq!(count(&db, "t"), 1, "after rollback only seed remains");
}

#[test]
fn ryw_delete_invisible_in_scan() {
    let (db, _dir) = setup("CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    db.execute("INSERT INTO t VALUES (1, 10)").expect("seed1");
    db.execute("INSERT INTO t VALUES (2, 20)").expect("seed2");
    let tx = db.begin_transaction().expect("begin");
    db.execute("DELETE FROM t WHERE id = 1").expect("delete in txn");
    // SELECT * must see only 1 row (the undeleted one)
    assert_eq!(count(&db, "t"), 1, "scan must not see deleted row");
    db.rollback_transaction(tx).expect("rollback");
    assert_eq!(count(&db, "t"), 2, "after rollback both rows restored");
}

#[test]
fn ryw_delete_invisible_in_pk_point_query() {
    let (db, _dir) = setup("CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    db.execute("INSERT INTO t VALUES (1, 10)").expect("seed");
    let tx = db.begin_transaction().expect("begin");
    db.execute("DELETE FROM t WHERE id = 1").expect("delete in txn");
    // PK point query must NOT find the deleted row
    let rs = db.execute("SELECT v FROM t WHERE id = 1").expect("select").materialize().expect("mat");
    use motedb::sql::QueryResult;
    if let QueryResult::Select { rows, .. } = rs {
        assert_eq!(rows.len(), 0, "pk point query must not see deleted row");
    }
    db.rollback_transaction(tx).expect("rollback");
}

#[test]
fn ryw_update_visible_in_scan() {
    let (db, _dir) = setup("CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    db.execute("INSERT INTO t VALUES (1, 10)").expect("seed");
    let tx = db.begin_transaction().expect("begin");
    db.execute("UPDATE t SET v = 999 WHERE id = 1").expect("update in txn");
    // SELECT must see the updated value
    let rs = db.execute("SELECT v FROM t WHERE id = 1").expect("select").materialize().expect("mat");
    use motedb::sql::QueryResult;
    if let QueryResult::Select { rows, .. } = rs {
        assert_eq!(rows.len(), 1);
        match &rows[0][0] {
            motedb::types::Value::Integer(n) => assert_eq!(*n, 999, "must see updated value"),
            other => panic!("expected Integer 999, got {:?}", other),
        }
    }
    db.rollback_transaction(tx).expect("rollback");
}

#[test]
fn ryw_mixed_insert_delete_update() {
    let (db, _dir) = setup("CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    db.execute("INSERT INTO t VALUES (1, 10)").expect("seed1");
    db.execute("INSERT INTO t VALUES (2, 20)").expect("seed2");
    let tx = db.begin_transaction().expect("begin");
    db.execute("INSERT INTO t VALUES (3, 30)").expect("insert in txn");
    db.execute("DELETE FROM t WHERE id = 1").expect("delete in txn");
    db.execute("UPDATE t SET v = 222 WHERE id = 2").expect("update in txn");
    // After: id=1 deleted, id=2 v=222, id=3 v=30 → 2 rows
    assert_eq!(count(&db, "t"), 2, "must reflect all uncommitted changes");
    // Check id=2 has updated value
    let rs = db.execute("SELECT v FROM t WHERE id = 2").expect("select").materialize().expect("mat");
    use motedb::sql::QueryResult;
    if let QueryResult::Select { rows, .. } = rs {
        match &rows[0][0] {
            motedb::types::Value::Integer(n) => assert_eq!(*n, 222),
            other => panic!("expected 222, got {:?}", other),
        }
    }
    db.rollback_transaction(tx).expect("rollback");
    assert_eq!(count(&db, "t"), 2, "after rollback original 2 rows restored");
}
