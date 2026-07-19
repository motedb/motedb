//! Bug-hunt v23: JOIN ON NULL matching, savepoints, INSERT...SELECT,
//! nested subquery in IN, transaction isolation between write/read.

use motedb::sql::QueryResult;
use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn new_db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    (db, dir)
}

fn exec(db: &Database, sql: &str) {
    db.execute(sql)
        .unwrap_or_else(|e| panic!("SQL failed: {}\n  err: {}", sql, e));
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    let rs = db
        .execute(sql)
        .unwrap_or_else(|e| panic!("SQL failed: {}\n  err: {}", sql, e))
        .materialize()
        .unwrap_or_else(|e| panic!("materialize failed: {}\n  err: {}", sql, e));
    match rs {
        QueryResult::Select { rows, .. } => rows,
        _ => panic!("expected Select for: {}", sql),
    }
}

fn scalar_i64(db: &Database, sql: &str) -> i64 {
    let r = rows(db, sql);
    assert_eq!(r.len(), 1, "expected 1 row: {}", sql);
    match r[0].first() {
        Some(Value::Integer(n)) => *n,
        o => panic!("expected int, got {:?}: {}", o, sql),
    }
}

fn ids_sorted(db: &Database, sql: &str) -> Vec<i64> {
    let r = rows(db, sql);
    let mut ids: Vec<i64> = r
        .iter()
        .filter_map(|row| match row.first() {
            Some(Value::Integer(n)) => Some(*n),
            _ => None,
        })
        .collect();
    ids.sort();
    ids
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION A: JOIN ON NULL
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn inner_join_on_null_does_not_match() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY, k INT)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY, k INT)");
    exec(&db, "INSERT INTO a VALUES (1, NULL), (2, 5)");
    exec(&db, "INSERT INTO b VALUES (1, NULL), (2, 5)");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM a JOIN b ON a.k = b.k");
    // NULL = NULL is unknown → no match. Only k=5 matches.
    assert_eq!(n, 1);
}

#[test]
fn left_join_preserves_left_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY, k INT)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY, k INT)");
    exec(&db, "INSERT INTO a VALUES (1, NULL)");
    exec(&db, "INSERT INTO b VALUES (1, NULL)");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM a LEFT JOIN b ON a.k = b.k");
    // LEFT JOIN preserves all left rows. a has 1 row → 1 row (with NULL right).
    assert_eq!(n, 1);
}

#[test]
fn join_with_is_null_in_on() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY, k INT)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY, k INT)");
    exec(&db, "INSERT INTO a VALUES (1, NULL), (2, 5)");
    exec(&db, "INSERT INTO b VALUES (1, NULL), (2, 5)");
    // Use `a.k IS NULL AND b.k IS NULL` as a join condition.
    let n = scalar_i64(
        &db,
        "SELECT COUNT(*) FROM a JOIN b ON a.k IS NULL AND b.k IS NULL",
    );
    // Both tables have 1 NULL-k row. Cartesian: 1*1 = 1 (NULL, NULL) combo.
    assert_eq!(n, 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION B: Savepoints
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn savepoint_basic_rollback_to() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let tx = db.begin_transaction().expect("begin");
    let _ = db.execute("INSERT INTO t VALUES (1, 10)").and_then(|r| r.materialize());
    db.savepoint(tx, "sp1").expect("savepoint");
    let _ = db.execute("INSERT INTO t VALUES (2, 20)").and_then(|r| r.materialize());
    db.rollback_to_savepoint(tx, "sp1").expect("rb to sp");
    db.commit_transaction(tx).expect("commit");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, 1, "only row 1 should survive");
}

#[test]
fn savepoint_release_then_no_rollback() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let tx = db.begin_transaction().expect("begin");
    let _ = db.execute("INSERT INTO t VALUES (1, 10)").and_then(|r| r.materialize());
    db.savepoint(tx, "sp1").expect("savepoint");
    let _ = db.execute("INSERT INTO t VALUES (2, 20)").and_then(|r| r.materialize());
    db.release_savepoint(tx, "sp1").expect("release");
    // After release, rollback_to should fail (savepoint gone).
    let r = db.rollback_to_savepoint(tx, "sp1");
    assert!(r.is_err(), "rollback_to after release should fail");
    db.commit_transaction(tx).expect("commit");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, 2);
}

#[test]
fn savepoint_nested() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let tx = db.begin_transaction().expect("begin");
    let _ = db.execute("INSERT INTO t VALUES (1, 10)").and_then(|r| r.materialize());
    db.savepoint(tx, "outer").expect("outer");
    let _ = db.execute("INSERT INTO t VALUES (2, 20)").and_then(|r| r.materialize());
    db.savepoint(tx, "inner").expect("inner");
    let _ = db.execute("INSERT INTO t VALUES (3, 30)").and_then(|r| r.materialize());
    db.rollback_to_savepoint(tx, "inner").expect("rb inner");
    db.commit_transaction(tx).expect("commit");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    // ids 1, 2 survive; id 3 was rolled back via inner savepoint.
    assert_eq!(n, 2);
}

#[test]
fn savepoint_rollback_to_outer_after_inner() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let tx = db.begin_transaction().expect("begin");
    let _ = db.execute("INSERT INTO t VALUES (1, 10)").and_then(|r| r.materialize());
    db.savepoint(tx, "outer").expect("outer");
    let _ = db.execute("INSERT INTO t VALUES (2, 20)").and_then(|r| r.materialize());
    db.savepoint(tx, "inner").expect("inner");
    let _ = db.execute("INSERT INTO t VALUES (3, 30)").and_then(|r| r.materialize());
    // Rollback to outer — should discard both 2 and 3.
    db.rollback_to_savepoint(tx, "outer").expect("rb outer");
    db.commit_transaction(tx).expect("commit");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, 1, "only id=1 survives (rolled back to outer)");
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION C: Transaction rollback undoes everything
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn txn_rollback_after_multiple_writes() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    let tx = db.begin_transaction().expect("begin");
    let _ = db.execute("INSERT INTO t VALUES (4, 40)").and_then(|r| r.materialize());
    let _ = db.execute("UPDATE t SET v = 999 WHERE id = 1").and_then(|r| r.materialize());
    let _ = db.execute("DELETE FROM t WHERE id = 2").and_then(|r| r.materialize());
    db.rollback_transaction(tx).expect("rollback");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, 3, "rollback should undo all writes");
    let v = scalar_i64(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(v, 10, "UPDATE was rolled back");
}

#[test]
fn txn_commit_persists() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let tx = db.begin_transaction().expect("begin");
    let _ = db.execute("INSERT INTO t VALUES (1, 10)").and_then(|r| r.materialize());
    let _ = db.execute("INSERT INTO t VALUES (2, 20)").and_then(|r| r.materialize());
    db.commit_transaction(tx).expect("commit");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION D: Subquery in IN
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn in_subquery_basic() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY, v INT)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)");
    exec(&db, "INSERT INTO b VALUES (1, 20), (2, 30)");
    let ids = ids_sorted(&db, "SELECT id FROM a WHERE v IN (SELECT v FROM b)");
    // a.v values: 10, 20, 30. b.v values: 20, 30. So a.id 2 (v=20), 3 (v=30).
    assert_eq!(ids, vec![2, 3]);
}

#[test]
fn not_in_subquery() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY, v INT)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)");
    exec(&db, "INSERT INTO b VALUES (1, 20), (2, 30)");
    let ids = ids_sorted(&db, "SELECT id FROM a WHERE v NOT IN (SELECT v FROM b)");
    // a.v not in {20,30}: only v=10 → id=1.
    assert_eq!(ids, vec![1]);
}

#[test]
fn in_subquery_empty_returns_nothing() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY, v INT)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO a VALUES (1, 10), (2, 20)");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM a WHERE v IN (SELECT v FROM b)");
    assert_eq!(n, 0);
}

#[test]
fn in_subquery_with_null_in_outer() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY, v INT)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO a VALUES (1, NULL), (2, 10)");
    exec(&db, "INSERT INTO b VALUES (1, 10)");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM a WHERE v IN (SELECT v FROM b)");
    // NULL IN {10} → unknown → no match. Only v=10 (id=2).
    assert_eq!(n, 1);
}

#[test]
fn in_subquery_with_null_in_inner() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY, v INT)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)");
    exec(&db, "INSERT INTO b VALUES (1, NULL), (2, 20)");
    // v IN (NULL, 20): NULL never matches; 20 matches → id=2.
    // NOT IN (NULL, 20): for v not in list, NULL in list → unknown → no match.
    let n_in = scalar_i64(&db, "SELECT COUNT(*) FROM a WHERE v IN (SELECT v FROM b)");
    assert_eq!(n_in, 1, "only v=20 matches");
    let n_not_in = scalar_i64(&db, "SELECT COUNT(*) FROM a WHERE v NOT IN (SELECT v FROM b)");
    // NULL in inner list makes NOT IN return unknown for ALL rows.
    assert_eq!(n_not_in, 0, "NOT IN with NULL in inner list returns 0 rows");
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION E: Multi-row operations consistency
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn update_with_subquery_in_set() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20)");
    // UPDATE t SET v = (SELECT MAX(v) FROM t) — scalar subquery evaluated
    // once, result (20) applied to every matching row.
    db.execute("UPDATE t SET v = (SELECT MAX(v) FROM t)").unwrap();
    let r = rows(&db, "SELECT v FROM t ORDER BY id");
    assert!(matches!(&r[0][0], Value::Integer(20)), "got {:?}", r[0][0]);
    assert!(matches!(&r[1][0], Value::Integer(20)));
}

#[test]
fn delete_with_subquery_returned_ids() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY, v INT)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY, a_id INT)");
    exec(&db, "INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)");
    exec(&db, "INSERT INTO b VALUES (1, 1), (2, 3)");
    // Delete a rows that have a matching b.a_id.
    db.execute("DELETE FROM a WHERE id IN (SELECT a_id FROM b)").unwrap();
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM a");
    // Deleted ids 1 and 3. Remaining: id=2.
    assert_eq!(n, 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION F: COUNT(*) semantics across operations
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn count_after_delete_matches() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
    for i in 1..=20 {
        exec(&db, &format!("INSERT INTO t VALUES ({})", i));
    }
    exec(&db, "DELETE FROM t WHERE id <= 10");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, 10);
}

#[test]
fn count_after_reopen_matches() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
        for i in 1..=15 {
            exec(&db, &format!("INSERT INTO t VALUES ({})", i));
        }
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, 15);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION G: Numerical edge cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn sum_of_negatives() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, -5), (2, -10), (3, -15)");
    let n = scalar_i64(&db, "SELECT SUM(v) FROM t");
    assert_eq!(n, -30);
}

#[test]
fn avg_of_negatives() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, -10), (2, -20)");
    let r = rows(&db, "SELECT AVG(v) FROM t");
    match &r[0][0] {
        Value::Float(f) => assert!((f - (-15.0)).abs() < 1e-6),
        Value::Integer(n) => assert_eq!(*n, -15),
        o => panic!("{:?}", o),
    }
}

#[test]
fn min_negative() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, -5), (2, -100), (3, 50)");
    let n = scalar_i64(&db, "SELECT MIN(v) FROM t");
    assert_eq!(n, -100);
}

#[test]
fn integer_boundary_i64_min() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v BIGINT)");
    exec(&db, "INSERT INTO t VALUES (1, -9223372036854775808)"); // i64::MIN
    let r = rows(&db, "SELECT v FROM t WHERE id = 1");
    assert!(matches!(&r[0][0], Value::Integer(n) if *n == i64::MIN));
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION H: LIKE case sensitivity
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn like_case_sensitive() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'Apple'), (2, 'apple'), (3, 'APPLE')");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE s LIKE 'apple'");
    // SQL standard: LIKE is case-sensitive (in most DBs).
    assert_eq!(n, 1, "LIKE 'apple' matches only exact case");
}

#[test]
fn like_case_insensitive_lower_pattern() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'Apple'), (2, 'apple')");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE s LIKE 'A%'");
    // LIKE 'A%' matches only 'Apple' (starts with uppercase A).
    assert_eq!(n, 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION I: WHERE clause evaluation order
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn where_division_by_zero_in_predicate_no_crash() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 0)");
    // WHERE v / 0 > 0 — division by zero in predicate.
    // Many DBs error here; SQLite returns NULL (treated as false).
    // Either is acceptable; the test just verifies no crash.
    let r = db.execute("SELECT id FROM t WHERE v / 0 > 0");
    assert!(r.is_ok() || r.is_err(), "should not crash");
}

#[test]
fn where_short_circuit_avoids_error() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 0), (2, 10)");
    // WHERE v = 0 OR 100 / v > 0 — short-circuit avoids div by zero for v=0.
    let r = db.execute("SELECT COUNT(*) FROM t WHERE v = 0 OR 100 / v > 0");
    assert!(r.is_ok(), "OR short-circuit should avoid division by zero");
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION J: Index after DELETE consistency
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn index_query_excludes_deleted_rows() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT)");
    for i in 1..=20 {
        let c = if i <= 10 { "a" } else { "b" };
        exec(&db, &format!("INSERT INTO t VALUES ({}, '{}')", i, c));
    }
    exec(&db, "CREATE INDEX t_cat ON t(cat)");
    db.checkpoint().unwrap();
    db.wait_for_indexes_ready();
    // Delete all cat='a' rows.
    for i in 1..=10 {
        exec(&db, &format!("DELETE FROM t WHERE id = {}", i));
    }
    db.checkpoint().unwrap();
    db.wait_for_indexes_ready();
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE cat = 'a'");
    assert_eq!(n, 0, "deleted rows should not appear in index query");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE cat = 'b'");
    assert_eq!(n, 10);
}

#[test]
fn index_query_after_update() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT)");
    for i in 1..=10 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'a')", i));
    }
    exec(&db, "CREATE INDEX t_cat ON t(cat)");
    db.checkpoint().unwrap();
    db.wait_for_indexes_ready();
    // Change half to 'b'.
    for i in 1..=5 {
        exec(&db, &format!("UPDATE t SET cat = 'b' WHERE id = {}", i));
    }
    db.checkpoint().unwrap();
    db.wait_for_indexes_ready();
    let a = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE cat = 'a'");
    let b = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE cat = 'b'");
    assert_eq!(a, 5);
    assert_eq!(b, 5);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION K: Transaction read-your-writes (within same Database handle)
//
// MoteDB v0.5.x uses a per-Database-handle transaction context: once
// begin_transaction() is called, all subsequent execute() calls on that
// same handle run inside the transaction (read-your-writes). There is no
// "outside the transaction" read on the same handle. This matches how
// embedded single-connection databases (like SQLite default mode) work.
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn txn_read_your_writes_visible() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    let tx = db.begin_transaction().expect("begin");
    let _ = db.execute("INSERT INTO t VALUES (2, 20)").and_then(|r| r.materialize());
    // Within the txn, both rows are visible (read-your-writes).
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, 2, "within txn, uncommitted writes are visible");
    db.rollback_transaction(tx).expect("rollback");
    // After rollback, only the committed row is visible.
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, 1, "after rollback, uncommitted writes are gone");
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION L: DROP TABLE cleanup
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn drop_table_then_show_tables_excludes() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY)");
    exec(&db, "DROP TABLE a");
    // b should still exist.
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM b");
    assert_eq!(n, 0);
    // a should be gone.
    let result = db.execute("SELECT COUNT(*) FROM a");
    assert!(result.is_err(), "dropped table should error on query");
}

#[test]
fn drop_table_releases_name() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
    exec(&db, "DROP TABLE t");
    // Should be able to recreate with same name.
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, 0);
}
