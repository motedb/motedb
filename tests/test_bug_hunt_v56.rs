//! Bug Hunt v56 — third round: UPDATE/DELETE, JOIN, dates, NULL/subquery.

use motedb::sql::QueryResult;
use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    (db, dir)
}

fn rows(r: QueryResult) -> Vec<Vec<Value>> {
    match r {
        QueryResult::Select { rows, .. } => rows,
        _ => vec![],
    }
}

fn q(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    rows(db.execute(sql).unwrap().materialize().unwrap())
}

// ─────────────────────────────────────────────────────────────────────────
// Bug 1: UPDATE changing an Integer non-auto PK makes the row stale/invisible
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_update_change_pk_then_lookup() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (5, 10)").unwrap();
    db.execute("UPDATE t SET id = 6 WHERE id = 5").unwrap();
    let r = q(&db, "SELECT * FROM t WHERE id = 6");
    assert_eq!(
        r,
        vec![vec![Value::Integer(6), Value::Integer(10)]],
        "row should be findable under new PK after UPDATE, got {:?}",
        r
    );
}

#[test]
fn test_update_change_pk_old_pk_gone() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (5, 10)").unwrap();
    db.execute("UPDATE t SET id = 6 WHERE id = 5").unwrap();
    let r = q(&db, "SELECT * FROM t WHERE id = 5");
    assert!(r.is_empty(), "old PK should no longer match");
}

#[test]
fn test_update_change_pk_scan_all() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (5, 10)").unwrap();
    db.execute("UPDATE t SET id = 6 WHERE id = 5").unwrap();
    // Full scan should still see exactly one row with the new PK.
    let r = q(&db, "SELECT * FROM t ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(6), Value::Integer(10)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Bug 2: NULL IN (SELECT ...) returns FALSE instead of NULL
// (See the table-based repros below — a FROM-less SELECT can't route
//  subquery materialization, so these tests use a real table.)
// ─────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────
// Bug 3: HOUR/MINUTE/SECOND wrong for pre-epoch timestamps
// ─────────────────────────────────────────────────────────────────────────

fn ts_db() -> (Database, TempDir) {
    let (db, dir) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, ts TIMESTAMP)").unwrap();
    db.execute(
        "INSERT INTO t VALUES (1, '1969-12-31T23:59:59'), (2, '2024-01-15T10:30:45')",
    )
    .unwrap();
    (db, dir)
}

#[test]
fn test_second_pre_epoch() {
    let (db, _d) = ts_db();
    // 1969-12-31 23:59:59 is one second before epoch.
    let r = q(&db, "SELECT SECOND(ts) FROM t WHERE id = 1");
    assert_eq!(
        r,
        vec![vec![Value::Integer(59)]],
        "SECOND of 23:59:59 should be 59, got {:?}",
        r
    );
}

#[test]
fn test_minute_pre_epoch() {
    let (db, _d) = ts_db();
    let r = q(&db, "SELECT MINUTE(ts) FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(59)]]);
}

#[test]
fn test_hour_pre_epoch() {
    let (db, _d) = ts_db();
    let r = q(&db, "SELECT HOUR(ts) FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(23)]]);
}

#[test]
fn test_time_funcs_post_epoch_still_correct() {
    let (db, _d) = ts_db();
    let r = q(&db, "SELECT HOUR(ts), MINUTE(ts), SECOND(ts) FROM t WHERE id = 2");
    assert_eq!(r, vec![vec![Value::Integer(10), Value::Integer(30), Value::Integer(45)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Bug 2: NULL IN (SELECT ...) returns FALSE instead of NULL (via a real table)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_null_in_subquery_returns_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE main(id INT)").unwrap();
    db.execute("CREATE TABLE s(v INT)").unwrap();
    db.execute("INSERT INTO main VALUES (1)").unwrap();
    db.execute("INSERT INTO s VALUES (1),(2)").unwrap();
    // NULL literal compared via IN (subquery), in a projection over main.
    let r = q(&db, "SELECT id, NULL IN (SELECT v FROM s) FROM main");
    assert_eq!(
        r,
        vec![vec![Value::Integer(1), Value::Null]],
        "NULL IN (subquery) should be NULL (UNKNOWN), got {:?}",
        r
    );
}

#[test]
fn test_value_in_subquery_with_null_no_match_returns_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE main(id INT)").unwrap();
    db.execute("CREATE TABLE s(v INT)").unwrap();
    db.execute("INSERT INTO main VALUES (5)").unwrap();
    db.execute("INSERT INTO s VALUES (1),(2),(NULL)").unwrap();
    // 5 not in (1,2,NULL) → UNKNOWN → NULL
    let r = q(&db, "SELECT id, 5 IN (SELECT v FROM s) FROM main");
    assert_eq!(r, vec![vec![Value::Integer(5), Value::Null]]);
}

#[test]
fn test_value_in_subquery_match_returns_true() {
    let (db, _d) = db();
    db.execute("CREATE TABLE main(id INT)").unwrap();
    db.execute("CREATE TABLE s(v INT)").unwrap();
    db.execute("INSERT INTO main VALUES (2)").unwrap();
    db.execute("INSERT INTO s VALUES (1),(2),(NULL)").unwrap();
    let r = q(&db, "SELECT id, 2 IN (SELECT v FROM s) FROM main");
    assert_eq!(r, vec![vec![Value::Integer(2), Value::Bool(true)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Tier 2 probe: NULL IN (literal) already returns NULL (v55 fix) — consistency
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_null_in_literal_vs_subquery_consistent() {
    let (db, _d) = db();
    db.execute("CREATE TABLE main(id INT)").unwrap();
    db.execute("CREATE TABLE s(v INT)").unwrap();
    db.execute("INSERT INTO main VALUES (1)").unwrap();
    db.execute("INSERT INTO s VALUES (1),(2)").unwrap();
    let lit = q(&db, "SELECT NULL IN (1, 2) FROM main");
    let sub = q(&db, "SELECT NULL IN (SELECT v FROM s) FROM main");
    assert_eq!(lit, sub, "NULL IN literal vs subquery must agree");
    assert_eq!(lit, vec![vec![Value::Null]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Bonus: DELETE + re-INSERT same PK; affected counts
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_delete_then_reinsert_same_pk() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 100)").unwrap();
    db.execute("DELETE FROM t WHERE id = 1").unwrap();
    db.execute("INSERT INTO t VALUES (1, 200)").unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(200)]]);
}

#[test]
fn test_update_self_reference_multi_col() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 5, 0)").unwrap();
    // SET a=a+1, b=a*2 → a=6, b=10 (both use original a=5).
    db.execute("UPDATE t SET a = a + 1, b = a * 2 WHERE id = 1").unwrap();
    let r = q(&db, "SELECT a, b FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(6), Value::Integer(10)]]);
}

#[test]
fn test_update_set_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 100)").unwrap();
    db.execute("UPDATE t SET v = NULL WHERE id = 1").unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Null]]);
}
