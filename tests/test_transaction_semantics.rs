//! Transaction semantics regression tests (BUG #29/#30/#31 family).
//!
//! - BUG #29: a second INSERT with the same PK inside ONE transaction
//!   silently overwrote the buffered write_set row (no error, one row left).
//! - BUG #30: UPDATE changing the PK of a row INSERTed in the same txn left
//!   the row under the OLD row_id while its content claimed the NEW pk —
//!   `WHERE pk = <new>` never found it, `WHERE pk = <old>` returned it.
//! - BUG #31: savepoint ROLLBACK ignored updates to write_set rows (the new
//!   value survived) and to relocated rows (the row vanished entirely).

use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn rows(r: motedb::StreamingQueryResult) -> Vec<Vec<Value>> {
    use motedb::QueryResult;
    match r.materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        _ => panic!("expected select"),
    }
}

fn ints(v: &[Vec<Value>]) -> Vec<i64> {
    v.iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            other => panic!("expected integer, got {other:?}"),
        })
        .collect()
}

#[test]
fn dup_pk_inside_txn_rejected() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    let tx = db.begin_transaction().unwrap();
    db.insert_row_with_txn("t", tx, vec![Value::Integer(1), Value::Text("a".into())])
        .unwrap();
    let second = db.insert_row_with_txn("t", tx, vec![Value::Integer(1), Value::Text("b".into())]);
    assert!(
        second.is_err(),
        "second INSERT with same PK in one txn must fail, got {:?}",
        second
    );
    db.commit_transaction(tx).unwrap();
    assert_eq!(
        ints(&rows(db.execute("SELECT id FROM t").unwrap())),
        vec![1]
    );
}

#[test]
fn dup_pk_inside_txn_via_sql_rejected() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    let second = db.execute("INSERT INTO t VALUES (1, 'b')");
    assert!(second.is_err(), "SQL INSERT dup PK in txn must fail");
    db.execute("COMMIT").unwrap();
    assert_eq!(
        ints(&rows(db.execute("SELECT id FROM t").unwrap())),
        vec![1]
    );
}

#[test]
fn cold_cache_txn_dup_pk_rejected() {
    // BUG #32: after reopen the pk_lookup cache is cold and ColSegmentStore
    // tables have no column index — the old storage-level check silently
    // no-op'd and the duplicate INSERT was accepted.
    let dir = TempDir::new().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (3, 'committed')").unwrap();
    }
    let db = Database::open(dir.path()).unwrap();
    let tx = db.begin_transaction().unwrap();
    let r = db.insert_row_with_txn("t", tx, vec![Value::Integer(3), Value::Text("dup".into())]);
    assert!(
        r.is_err(),
        "dup INSERT with cold pk cache must fail, got {r:?}"
    );
    db.rollback_transaction(tx).unwrap();
}

#[test]
fn concurrent_txn_dup_pk_one_wins() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    let tx1 = db.begin_transaction().unwrap();
    let tx2 = db.begin_transaction().unwrap();
    db.insert_row_with_txn("t", tx1, vec![Value::Integer(1), Value::Text("a".into())])
        .unwrap();
    db.insert_row_with_txn("t", tx2, vec![Value::Integer(1), Value::Text("b".into())])
        .unwrap();
    let c1 = db.commit_transaction(tx1);
    let c2 = db.commit_transaction(tx2);
    assert!(c1.is_ok());
    assert!(c2.is_err(), "conflicting concurrent commit must fail");
    let got = rows(db.execute("SELECT id FROM t").unwrap());
    assert_eq!(got.len(), 1, "exactly one row survives, got {got:?}");
}

#[test]
fn update_then_full_rollback_restores() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    let tx = db.begin_transaction().unwrap();
    db.execute("UPDATE t SET v = 999 WHERE id = 1").unwrap();
    assert_eq!(
        ints(&rows(db.execute("SELECT v FROM t WHERE id = 1").unwrap())),
        vec![999],
        "read-your-writes during txn"
    );
    db.rollback_transaction(tx).unwrap();
    assert_eq!(
        ints(&rows(db.execute("SELECT v FROM t WHERE id = 1").unwrap())),
        vec![10],
        "ROLLBACK must restore the pre-txn value"
    );
}

#[test]
fn delete_then_full_rollback_restores() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    let tx = db.begin_transaction().unwrap();
    db.execute("DELETE FROM t WHERE id = 1").unwrap();
    db.rollback_transaction(tx).unwrap();
    assert_eq!(
        ints(&rows(db.execute("SELECT id FROM t ORDER BY id").unwrap())),
        vec![1, 2]
    );
}

#[test]
fn update_pk_to_existing_value_rejected() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    let r = db.execute("UPDATE t SET id = 2 WHERE id = 1");
    assert!(r.is_err(), "PK change to an existing PK must fail");
    assert_eq!(
        ints(&rows(db.execute("SELECT id FROM t ORDER BY id").unwrap())),
        vec![1, 2]
    );
}

#[test]
fn buffered_row_pk_change_visible_after_commit() {
    let dir = TempDir::new().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
            .unwrap();
        let tx = db.begin_transaction().unwrap();
        db.insert_row_with_txn("t", tx, vec![Value::Integer(5), Value::Text("a".into())])
            .unwrap();
        db.execute("UPDATE t SET id = 7 WHERE id = 5").unwrap();
        db.commit_transaction(tx).unwrap();
        assert_eq!(
            ints(&rows(db.execute("SELECT id FROM t").unwrap())),
            vec![7]
        );
        // PK point queries must resolve via the NEW pk
        let q7 = rows(db.execute("SELECT v FROM t WHERE id = 7").unwrap());
        assert_eq!(q7.len(), 1, "WHERE id=7 must find the row, got {q7:?}");
        let q5 = rows(db.execute("SELECT v FROM t WHERE id = 5").unwrap());
        assert!(q5.is_empty(), "WHERE id=5 must not find it, got {q5:?}");
    }
    // and survive reopen
    let db = Database::open(dir.path()).unwrap();
    let q7 = rows(db.execute("SELECT v FROM t WHERE id = 7").unwrap());
    assert_eq!(q7.len(), 1, "PK query must work after reopen");
}

#[test]
fn buffered_row_pk_change_to_taken_pk_rejected() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (3, 'committed')").unwrap();
    let tx = db.begin_transaction().unwrap();
    db.insert_row_with_txn("t", tx, vec![Value::Integer(5), Value::Text("a".into())])
        .unwrap();
    // to a COMMITTED pk
    let r1 = db.execute("UPDATE t SET id = 3 WHERE id = 5");
    assert!(r1.is_err(), "relocation onto committed PK must fail");
    // to another BUFFERED pk
    db.insert_row_with_txn("t", tx, vec![Value::Integer(9), Value::Text("b".into())])
        .unwrap();
    let r2 = db.execute("UPDATE t SET id = 9 WHERE id = 5");
    assert!(r2.is_err(), "relocation onto buffered PK must fail");
    db.rollback_transaction(tx).unwrap();
}

#[test]
fn insert_then_delete_in_txn() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    let tx = db.begin_transaction().unwrap();
    db.insert_row_with_txn("t", tx, vec![Value::Integer(1), Value::Text("a".into())])
        .unwrap();
    db.insert_row_with_txn("t", tx, vec![Value::Integer(2), Value::Text("b".into())])
        .unwrap();
    db.execute("DELETE FROM t WHERE id = 1").unwrap();
    db.commit_transaction(tx).unwrap();
    assert_eq!(
        ints(&rows(db.execute("SELECT id FROM t ORDER BY id").unwrap())),
        vec![2]
    );
}

#[test]
fn savepoint_rollback_restores_ws_update() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    let tx = db.begin_transaction().unwrap();
    db.insert_row_with_txn("t", tx, vec![Value::Integer(5), Value::Text("a".into())])
        .unwrap();
    db.savepoint(tx, "s1".into()).unwrap();
    db.execute("UPDATE t SET v = 'changed' WHERE id = 5")
        .unwrap();
    db.rollback_to_savepoint(tx, "s1").unwrap();
    db.commit_transaction(tx).unwrap();
    let got = rows(db.execute("SELECT v FROM t").unwrap());
    assert_eq!(
        got.len(),
        1,
        "row must survive savepoint rollback of an UPDATE"
    );
    match &got[0][0] {
        Value::Text(t) => assert_eq!(&**t, "a", "savepoint rollback must restore old value"),
        other => panic!("expected text, got {other:?}"),
    }
}

#[test]
fn savepoint_rollback_after_pk_relocation() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    let tx = db.begin_transaction().unwrap();
    db.insert_row_with_txn("t", tx, vec![Value::Integer(5), Value::Text("a".into())])
        .unwrap();
    db.savepoint(tx, "s1".into()).unwrap();
    db.execute("UPDATE t SET id = 7 WHERE id = 5").unwrap();
    db.rollback_to_savepoint(tx, "s1").unwrap();
    db.commit_transaction(tx).unwrap();
    // the row must still exist, under its ORIGINAL pk
    assert_eq!(
        ints(&rows(db.execute("SELECT id FROM t").unwrap())),
        vec![5],
        "savepoint rollback must undo the PK relocation"
    );
    let q = rows(db.execute("SELECT v FROM t WHERE id = 5").unwrap());
    assert_eq!(q.len(), 1);
}

#[test]
fn full_rollback_no_phantom_rows() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    let tx = db.begin_transaction().unwrap();
    db.insert_row_with_txn("t", tx, vec![Value::Integer(5), Value::Text("a".into())])
        .unwrap();
    db.execute("UPDATE t SET v = 'changed' WHERE id = 5")
        .unwrap();
    db.execute("UPDATE t SET id = 7 WHERE id = 5").unwrap();
    db.rollback_transaction(tx).unwrap();
    // buffered-row deltas must never materialize storage rows on ROLLBACK
    assert_eq!(
        ints(&rows(db.execute("SELECT id FROM t").unwrap())),
        Vec::<i64>::new()
    );
}
