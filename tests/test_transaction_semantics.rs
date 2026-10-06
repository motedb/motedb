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
    db.savepoint(tx, "s1").unwrap();
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
    db.savepoint(tx, "s1").unwrap();
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

#[test]
fn in_txn_pk_point_update_fast_path_semantics() {
    // The PK fast path used to be blanket-disabled inside transactions, so
    // every `UPDATE … WHERE id = N` did a full-table scan (~70ms at 100K
    // rows). Re-enabled, it must stay read-your-writes correct: storage
    // rows, rows updated twice, buffered INSERTs, PK relocations, and
    // txn-DELETEd rows must all behave exactly like the old scan path.
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for s in (0..300).step_by(50) {
        let vals: Vec<String> = (s..s + 50).map(|i| format!("({i}, {i})")).collect();
        db.execute(&format!("INSERT INTO t VALUES {}", vals.join(",")))
            .unwrap();
    }

    let tx = db.begin_transaction().unwrap();
    // 1. Storage rows: repeated point updates in one txn (the benchmark shape).
    for round in 1..=2 {
        for i in 0..300 {
            db.execute(&format!(
                "UPDATE t SET v = {} WHERE id = {}",
                i * 10 + round,
                i
            ))
            .unwrap();
        }
    }
    // 2. Buffered INSERT visible to a later point UPDATE in the same txn.
    db.execute("INSERT INTO t VALUES (1000, 1000)").unwrap();
    db.execute("UPDATE t SET v = -1 WHERE id = 1000").unwrap();
    // 3. PK change on a buffered row via the point path.
    db.execute("INSERT INTO t VALUES (2000, 2000)").unwrap();
    db.execute("UPDATE t SET id = 2001, v = -2 WHERE id = 2000")
        .unwrap();
    // 4. DELETE then UPDATE of the deleted PK: must affect 0 rows.
    db.execute("DELETE FROM t WHERE id = 299").unwrap();
    db.execute("UPDATE t SET v = -3 WHERE id = 299").unwrap();
    db.commit_transaction(tx).unwrap();

    let q = ints(&rows(db.execute("SELECT v FROM t WHERE id = 0").unwrap()));
    assert_eq!(q, vec![2], "second round of point updates must win");
    let q = ints(&rows(db.execute("SELECT v FROM t WHERE id = 298").unwrap()));
    assert_eq!(q, vec![2982]);
    // deleted PK: gone; its neighbor untouched
    assert_eq!(
        ints(&rows(
            db.execute("SELECT id FROM t WHERE id = 299").unwrap()
        )),
        Vec::<i64>::new(),
        "UPDATE after in-txn DELETE must not resurrect the row"
    );
    // buffered insert + update
    let q = ints(&rows(
        db.execute("SELECT v FROM t WHERE id = 1000").unwrap(),
    ));
    assert_eq!(q, vec![-1], "point UPDATE must reach the buffered INSERT");
    // relocated buffered row
    let q = ints(&rows(
        db.execute("SELECT v FROM t WHERE id = 2001").unwrap(),
    ));
    assert_eq!(
        q,
        vec![-2],
        "relocated buffered row must live under its new PK"
    );
    assert_eq!(
        ints(&rows(
            db.execute("SELECT id FROM t WHERE id = 2000").unwrap()
        )),
        Vec::<i64>::new(),
        "old PK must not match after relocation"
    );
    // total row count: 300 - 1 deleted + 2 buffered inserts (2000 relocated, not added)
    let n = ints(&rows(db.execute("SELECT COUNT(*) FROM t").unwrap()));
    assert_eq!(n, vec![301]);
}

#[test]
fn sql_savepoint_errors_propagate_and_survive_rollback_to() {
    // Two bugs found by the external-integration E2E:
    // 1. The streaming entry folded SAVEPOINT/ROLLBACK TO errors into a
    //    success message — a SAVEPOINT without an active transaction was a
    //    silent no-op, and the later ROLLBACK TO "succeeded" while the
    //    UPDATE stayed committed.
    // 2. ROLLBACK TO destroyed the target savepoint; SQL keeps it alive
    //    (RELEASE / a second ROLLBACK TO must work).
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE s (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO s VALUES (1, 10)").unwrap();

    // 1. SAVEPOINT without a transaction must be a LOUD error.
    assert!(db.execute("SAVEPOINT orphan").is_err());

    // 2. Full cycle inside a transaction.
    db.execute("BEGIN").unwrap();
    db.execute("SAVEPOINT sp1").unwrap();
    db.execute("UPDATE s SET v = 99 WHERE id = 1").unwrap();
    db.execute("ROLLBACK TO sp1").unwrap();
    let q = ints(&rows(db.execute("SELECT v FROM s WHERE id = 1").unwrap()));
    assert_eq!(q, vec![10], "rollback to savepoint restores the value");

    // 3. The target savepoint SURVIVES: a second ROLLBACK TO undoes new
    //    writes only, and RELEASE succeeds.
    db.execute("UPDATE s SET v = 42 WHERE id = 1").unwrap();
    db.execute("ROLLBACK TO sp1").unwrap();
    let q = ints(&rows(db.execute("SELECT v FROM s WHERE id = 1").unwrap()));
    assert_eq!(q, vec![10], "second rollback to the same savepoint");
    db.execute("UPDATE s SET v = 42 WHERE id = 1").unwrap();
    db.execute("RELEASE sp1").unwrap();
    db.execute("COMMIT").unwrap();
    let q = ints(&rows(db.execute("SELECT v FROM s WHERE id = 1").unwrap()));
    assert_eq!(q, vec![42], "release keeps the changes");

    // 4. Nested savepoints: rolling back to the outer one undoes both
    //    (restoring the value AS OF the outer savepoint — 42 here).
    db.execute("BEGIN").unwrap();
    db.execute("SAVEPOINT a").unwrap();
    db.execute("UPDATE s SET v = 1 WHERE id = 1").unwrap();
    db.execute("SAVEPOINT b").unwrap();
    db.execute("UPDATE s SET v = 2 WHERE id = 1").unwrap();
    db.execute("ROLLBACK TO a").unwrap();
    db.execute("COMMIT").unwrap();
    let q = ints(&rows(db.execute("SELECT v FROM s WHERE id = 1").unwrap()));
    assert_eq!(q, vec![42], "outer savepoint rollback undoes nested writes");
}

/// 🔒 In-process multi-connection: the second open used to fail on flock
/// ("already open by another process"). Connections now attach to the SAME
/// engine via the api-layer registry; the LAST close performs shutdown.
#[test]
fn in_process_multi_connection_shared_engine() {
    let dir = TempDir::new().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let c1 = Database::open(dir.path()).unwrap();
    let c2 = Database::open(dir.path()).unwrap();
    let c3 = Database::open_with_config(dir.path(), motedb::DBConfig::for_testing()).unwrap();

    // Cross-connection visibility (shared engine).
    c1.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    let q = ints(&rows(c2.execute("SELECT COUNT(*) FROM t").unwrap()));
    assert_eq!(q, vec![2]);

    // Rollback on one connection is visible to the others.
    let tx = c1.begin_transaction().unwrap();
    c1.execute("UPDATE t SET v = 99 WHERE id = 1").unwrap();
    c1.rollback_transaction(tx).unwrap();
    let q = ints(&rows(c3.execute("SELECT v FROM t WHERE id = 1").unwrap()));
    assert_eq!(q, vec![10]);

    // Non-last closes detach only.
    c1.close().unwrap();
    c2.close().unwrap();
    let q = ints(&rows(c3.execute("SELECT COUNT(*) FROM t").unwrap()));
    assert_eq!(q, vec![2]);

    // Last close fully shuts down; reopen starts fresh.
    c3.close().unwrap();
    let c4 = Database::open(dir.path()).unwrap();
    let q = ints(&rows(c4.execute("SELECT COUNT(*) FROM t").unwrap()));
    assert_eq!(q, vec![2]);
    c4.close().unwrap();
}

/// 🔒 M1: transactional UPDATEs of storage rows are BUFFERED (pending_updates)
/// and applied at COMMIT — a concurrent committer's value can no longer be
/// clobbered by another txn's ROLLBACK (storage is untouched until commit).
/// This was the known-limitation test (in-place writes + undo replay).
#[test]
fn rollback_after_concurrent_commit_keeps_winner() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), motedb::DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE x (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO x VALUES (1, 100)").unwrap();

    let t1 = db.begin_transaction().unwrap();
    db.execute("UPDATE x SET v = 1 WHERE id = 1").unwrap();
    let t2 = db.begin_transaction().unwrap();
    db.execute("UPDATE x SET v = 2 WHERE id = 1").unwrap();
    db.commit_transaction(t2).unwrap();
    db.rollback_transaction(t1).unwrap();

    let q = ints(&rows(db.execute("SELECT v FROM x WHERE id = 1").unwrap()));
    assert_eq!(
        q,
        vec![2],
        "committed concurrent value must survive t1 rollback"
    );

    // Clean rollback still restores (no concurrent committer).
    db.execute("UPDATE x SET v = 7 WHERE id = 1").unwrap();
    let t3 = db.begin_transaction().unwrap();
    db.execute("UPDATE x SET v = 9 WHERE id = 1").unwrap();
    db.rollback_transaction(t3).unwrap();
    let q = ints(&rows(db.execute("SELECT v FROM x WHERE id = 1").unwrap()));
    assert_eq!(q, vec![7], "no-conflict rollback restores the old value");
}

// 🔒 Regression: the COMMIT apply of buffered pending deletes removed the row
// (tombstone + indexes + row cache) but NOT the pk_lookup cache entry — a
// re-INSERT of the same PK then failed with a bogus "Duplicate primary key".
// The autocommit delete path (delete_row_impl step 7.2) has always cleaned
// the cache; the buffered path must too. All DELETE forms (literal SQL,
// parameterized fast-PK deferral, executemany) share the commit apply, so
// one test with all three forms covers it.
#[test]
fn txn_delete_commit_allows_same_pk_reinsert() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();

    for id in [10, 20, 30] {
        db.execute(format!("INSERT INTO t VALUES ({id}, {id})").as_str())
            .unwrap();
    }

    // literal form
    db.execute("BEGIN").unwrap();
    db.execute("DELETE FROM t WHERE id = 10").unwrap();
    db.execute("COMMIT").unwrap();
    // parameterized form (rides the api fast-PK deferral into the executor)
    db.execute("BEGIN").unwrap();
    let _ = db
        .execute_prepared("DELETE FROM t WHERE id = ?", vec![Value::Integer(20)])
        .unwrap()
        .materialize()
        .unwrap();
    db.execute("COMMIT").unwrap();
    // executemany form
    db.execute("BEGIN").unwrap();
    let _ = db
        .execute_prepared_many("DELETE FROM t WHERE id = ?", vec![vec![Value::Integer(30)]])
        .unwrap();
    db.execute("COMMIT").unwrap();

    assert_eq!(
        ints(&rows(db.execute("SELECT COUNT(*) FROM t").unwrap())),
        [0]
    );

    // Re-insert every deleted PK — all must succeed and read back.
    for (id, v) in [(10, 100), (20, 200), (30, 300)] {
        db.execute(format!("INSERT INTO t VALUES ({id}, {v})").as_str())
            .unwrap_or_else(|e| panic!("re-insert of deleted PK {id} failed: {e}"));
    }
    assert_eq!(
        ints(&rows(db.execute("SELECT id FROM t ORDER BY id").unwrap())),
        [10, 20, 30]
    );
    assert_eq!(
        ints(&rows(db.execute("SELECT v FROM t WHERE id = 20").unwrap())),
        [200]
    );

    // Rollback variant: an UNCOMMITTED delete restores the live row — the
    // original row reads back with its value, and a duplicate INSERT of the
    // same PK is (correctly) still rejected.
    db.execute("BEGIN").unwrap();
    db.execute("DELETE FROM t WHERE id = 10").unwrap();
    db.execute("ROLLBACK").unwrap();
    assert_eq!(
        ints(&rows(db.execute("SELECT v FROM t WHERE id = 10").unwrap())),
        [100]
    );
    let dup = db.execute("INSERT INTO t VALUES (10, 999)");
    assert!(dup.is_err(), "true duplicate must still be rejected");
}

// 🔒 W4b regression: transactional MATCH read-your-writes. The FTS fast
// path answers from the index (committed text only) — inside a transaction
// it must overlay buffered writes so the result is exactly what COMMIT
// would make the index return:
//   - uncommitted INSERT (write_set) → VISIBLE to MATCH
//   - uncommitted DELETE (tombstone)  → HIDDEN from MATCH
//   - uncommitted text UPDATE         → old terms gone, new terms searchable
//   - projection reads buffered values; ROLLBACK restores exactly.
#[test]
fn match_ryw_inside_transaction() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    for i in 1..=50 {
        db.execute(format!("INSERT INTO docs VALUES ({i}, 'alpha doc{i} number{i}')").as_str())
            .unwrap();
    }
    db.execute("CREATE TEXT INDEX docs_body ON docs (body)")
        .unwrap();

    let count_alpha = |db: &Database| -> usize {
        let rows = db
            .execute("SELECT id FROM docs WHERE MATCH(body, 'alpha')")
            .unwrap()
            .materialize()
            .unwrap();
        match rows {
            motedb::QueryResult::Select { rows, .. } => rows.len(),
            other => panic!("{other:?}"),
        }
    };

    // Baseline outside txn.
    assert_eq!(count_alpha(&db), 50);

    // 1. Uncommitted INSERT is visible; COUNT via MATCH too.
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO docs VALUES (500, 'alpha bravo fresh')")
        .unwrap();
    assert_eq!(
        count_alpha(&db),
        51,
        "uncommitted INSERT must be visible to MATCH"
    );
    assert_eq!(
        ints(&rows(
            db.execute("SELECT COUNT(*) FROM docs WHERE MATCH(body, 'alpha')")
                .unwrap()
        )),
        [51]
    );
    // 2. Uncommitted DELETE hides the row.
    db.execute("DELETE FROM docs WHERE id = 7").unwrap();
    assert_eq!(
        count_alpha(&db),
        50,
        "uncommitted DELETE must hide from MATCH"
    );
    // 3. Uncommitted text UPDATE: old term gone, new term searchable, and
    //    the projection reads the BUFFERED text.
    db.execute("UPDATE docs SET body = 'gamma replaced' WHERE id = 10")
        .unwrap();
    assert_eq!(count_alpha(&db), 49);
    assert_eq!(
        ints(&rows(
            db.execute("SELECT COUNT(*) FROM docs WHERE MATCH(body, 'gamma')")
                .unwrap()
        )),
        [1]
    );
    let text = rows(db.execute("SELECT body FROM docs WHERE id = 10").unwrap());
    assert_eq!(text[0][0], Value::Text("gamma replaced".into()));
    // 4. LIMIT shape inside the txn (overlay + early exit).
    let limited = rows(
        db.execute("SELECT id FROM docs WHERE MATCH(body, 'alpha') LIMIT 3")
            .unwrap(),
    );
    assert_eq!(limited.len(), 3);
    // 5. ROLLBACK restores everything exactly.
    db.execute("ROLLBACK").unwrap();
    assert_eq!(count_alpha(&db), 50);
    let text = rows(db.execute("SELECT body FROM docs WHERE id = 10").unwrap());
    assert_eq!(text[0][0], Value::Text("alpha doc10 number10".into()));

    // 6. COMMIT makes the overlaid view durable: insert visible, deleted
    //    gone, updated text reindexed.
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO docs VALUES (600, 'alpha delta committed')")
        .unwrap();
    db.execute("DELETE FROM docs WHERE id = 8").unwrap();
    db.execute("UPDATE docs SET body = 'epsilon committed' WHERE id = 9")
        .unwrap();
    db.execute("COMMIT").unwrap();
    assert_eq!(count_alpha(&db), 49); // 50 - deleted 8 - updated-away 9 + insert 600
    assert_eq!(
        ints(&rows(
            db.execute("SELECT COUNT(*) FROM docs WHERE MATCH(body, 'delta')")
                .unwrap()
        )),
        [1]
    );
    assert_eq!(
        ints(&rows(
            db.execute("SELECT COUNT(*) FROM docs WHERE MATCH(body, 'epsilon')")
                .unwrap()
        )),
        [1]
    );
}

// 🔒 Regression (E1 differential catch): txn_aggregate_overlaid ignored the
// WHERE clause entirely — inside a transaction WITH buffered writes,
// `COUNT(*) WHERE device = '..'` returned the unfiltered total (and SUM/
// AVG/MIN/MAX aggregated every row). The overlay path must filter by the
// same predicate the non-transactional aggregate paths apply.
#[test]
fn txn_aggregate_where_is_applied() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, device TEXT, v INT)")
        .unwrap();
    for i in 0..500 {
        db.execute(
            format!(
                "INSERT INTO t VALUES ({}, 'dev-{:02}', {})",
                i,
                i % 8,
                i % 11
            )
            .as_str(),
        )
        .unwrap();
    }
    let auto = |sql: &str| -> i64 {
        match &rows(db.execute(sql).unwrap())[0][0] {
            Value::Integer(n) => *n,
            v => panic!("{v:?}"),
        }
    };

    // Reference (autocommit): COUNT / SUM / AVG with WHERE.
    let c_ref = auto("SELECT COUNT(*) FROM t WHERE device = 'dev-03'");
    let s_ref = auto("SELECT SUM(v) FROM t WHERE v >= 7");
    assert!(c_ref > 0);

    // Same queries inside a transaction WITH a buffered write (the routing
    // gate sends these to txn_aggregate_overlaid).
    db.execute("BEGIN").unwrap();
    db.execute("UPDATE t SET v = v WHERE id = 0").unwrap(); // buffered pending
    let c_txn = auto("SELECT COUNT(*) FROM t WHERE device = 'dev-03'");
    assert_eq!(c_txn, c_ref, "in-txn COUNT WHERE must equal autocommit");
    let s_txn = auto("SELECT SUM(v) FROM t WHERE v >= 7");
    assert_eq!(s_txn, s_ref, "in-txn SUM WHERE must equal autocommit");
    // Unfiltered total unchanged (WHERE-none still counts everything).
    let total = auto("SELECT COUNT(*) FROM t");
    assert_eq!(total, 500);
    // write_set rows are filtered by the predicate too.
    db.execute("INSERT INTO t VALUES (900, 'dev-03', 99)")
        .unwrap();
    let c2 = auto("SELECT COUNT(*) FROM t WHERE device = 'dev-03'");
    assert_eq!(
        c2,
        c_ref + 1,
        "uncommitted INSERT matching the WHERE counts"
    );
    let c3 = auto("SELECT COUNT(*) FROM t WHERE device = 'dev-05'");
    assert_eq!(c3, auto("SELECT COUNT(*) FROM t WHERE device = 'dev-05'"));
    db.execute("ROLLBACK").unwrap();
    assert_eq!(auto("SELECT COUNT(*) FROM t"), 500);
}

// 🔒 J5 regression (acceptance smoke catch): with ONLY buffered INSERTs in
// the transaction (no pending updates/deletes), the aggregate routing gate
// let `col_segment_aggregate` answer from RAW STORAGE — uncommitted rows
// vanished from COUNT/SUM/AVG with a WHERE clause (plain COUNT(*) had its
// own ws handling and was correct).
#[test]
fn txn_inserts_visible_in_filtered_aggregate() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT, v INT)")
        .unwrap();
    for i in 0..500 {
        db.execute(format!("INSERT INTO docs VALUES ({i}, 'doc {i} alpha', {i})").as_str())
            .unwrap();
    }
    let count_alpha = |db: &Database| -> i64 {
        match &rows(
            db.execute("SELECT COUNT(*) FROM docs WHERE body LIKE '%alpha%'")
                .unwrap(),
        )[0][0]
        {
            Value::Integer(n) => *n,
            v => panic!("{v:?}"),
        }
    };
    assert_eq!(count_alpha(&db), 500);

    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO docs VALUES (900, 'txx alpha', 900)")
        .unwrap();
    // Plain COUNT(*) sees the ws row (pre-existing path).
    assert_eq!(
        ints(&rows(db.execute("SELECT COUNT(*) FROM docs").unwrap())),
        [501]
    );
    // WHERE-filtered aggregate MUST see it too (was 500).
    assert_eq!(count_alpha(&db), 501, "ws row missing from filtered COUNT");
    assert_eq!(
        ints(&rows(
            db.execute("SELECT COUNT(*) FROM docs WHERE body LIKE '%txx%'")
                .unwrap()
        )),
        [1]
    );
    assert_eq!(
        ints(&rows(
            db.execute("SELECT SUM(v) FROM docs WHERE id = 900")
                .unwrap()
        )),
        [900]
    );
    db.execute("ROLLBACK").unwrap();
    assert_eq!(count_alpha(&db), 500);

    // COMMIT makes it durable.
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO docs VALUES (901, 'kept alpha', 901)")
        .unwrap();
    db.execute("COMMIT").unwrap();
    assert_eq!(count_alpha(&db), 501);
}
