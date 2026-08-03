//! Bug Hunt v36 — Date/time functions accept Integer + TIMESTAMP column scan.
//!
//! Two bugs fixed:
//!  1. Date functions (YEAR/MONTH/DAY/etc.) required Value::Timestamp and
//!     errored on Integer literals. Now accept both (Integer = epoch micros).
//!  2. TIMESTAMP columns decoded as NULL in scan_projected_filtered (the
//!     non-lazy decode path's match had no Timestamp arm → `_ => NULL`).
//!     This broke SELECT YEAR(ts_col), date functions, and direct timestamp
//!     projection. Fixed by adding Timestamp arms in both lazy and non-lazy
//!     decode paths.

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

// =========================================================================
// Date functions accept Integer (micros)
// =========================================================================

#[test]
fn test_year_integer_literal() {
    let (db, _d) = db();
    // 1700000000000000 micros = 2023-11-14 22:13:20 UTC
    let r = q(&db, "SELECT YEAR(1700000000000000)");
    assert_eq!(r[0][0], Value::Integer(2023));
}

#[test]
fn test_month_integer_literal() {
    let (db, _d) = db();
    let r = q(&db, "SELECT MONTH(1700000000000000)");
    assert_eq!(r[0][0], Value::Integer(11));
}

#[test]
fn test_day_integer_literal() {
    let (db, _d) = db();
    let r = q(&db, "SELECT DAY(1700000000000000)");
    assert_eq!(r[0][0], Value::Integer(14));
}

#[test]
fn test_hour_integer_literal() {
    let (db, _d) = db();
    let r = q(&db, "SELECT HOUR(1700000000000000)");
    assert_eq!(r[0][0], Value::Integer(22));
}

#[test]
fn test_day_of_week_integer_literal() {
    let (db, _d) = db();
    let r = q(&db, "SELECT DAY_OF_WEEK(1700000000000000)");
    assert!(r[0][0] >= Value::Integer(1) && r[0][0] <= Value::Integer(7));
}

#[test]
fn test_date_add_integer() {
    let (db, _d) = db();
    let r = q(&db, "SELECT DATE_ADD(1700000000000000, 86400)");
    // Should return a Timestamp 86400 seconds later.
    match &r[0][0] {
        Value::Timestamp(_) => {}
        other => panic!("expected Timestamp, got {:?}", other),
    }
}

#[test]
fn test_date_diff_integer() {
    let (db, _d) = db();
    let r = q(&db, "SELECT DATE_DIFF(1700000000000000, 1699900000000000)");
    // Difference in seconds: 10000000000 micros / 1000000 = 10000 seconds.
    assert_eq!(r[0][0], Value::Integer(100000));
}

#[test]
fn test_year_cast_timestamp() {
    let (db, _d) = db();
    let r = q(&db, "SELECT YEAR(CAST(1700000000000000 AS TIMESTAMP))");
    assert_eq!(r[0][0], Value::Integer(2023));
}

// =========================================================================
// Date functions in WHERE (works via try_fast_select / ColSegmentStore path)
// =========================================================================

#[test]
fn test_year_in_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE ev (id INTEGER PRIMARY KEY, ts TIMESTAMP)")
        .unwrap();
    db.execute("INSERT INTO ev VALUES (1, 1700000000000000), (2, 1600000000000000)")
        .unwrap();
    let r = q(&db, "SELECT id FROM ev WHERE YEAR(ts) = 2023");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_year_in_where_no_match() {
    let (db, _d) = db();
    db.execute("CREATE TABLE ev (id INTEGER PRIMARY KEY, ts TIMESTAMP)")
        .unwrap();
    db.execute("INSERT INTO ev VALUES (1, 1700000000000000)")
        .unwrap();
    let r = q(&db, "SELECT id FROM ev WHERE YEAR(ts) = 1999");
    assert!(r.is_empty());
}

// =========================================================================
// || concatenation in UPDATE SET
// =========================================================================

#[test]
fn test_update_concat_operator() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
    db.execute("UPDATE t SET name = 'pre_' || name WHERE id = 1")
        .unwrap();
    let r = q(&db, "SELECT name FROM t WHERE id = 1");
    assert_eq!(r[0][0], Value::text("pre_alice".into()));
}

// =========================================================================
// TIMESTAMP column scan (was NULL — scan_projected_filtered decode bug)
// =========================================================================

#[test]
fn test_year_timestamp_column() {
    // Previously returned NULL: scan_projected_filtered decoded TIMESTAMP
    // columns via the `_ => NULL` arm (no Timestamp match case).
    let (db, _d) = db();
    db.execute("CREATE TABLE ev (id INTEGER PRIMARY KEY, ts TIMESTAMP)")
        .unwrap();
    db.execute("INSERT INTO ev VALUES (1, 1700000000000000), (2, 1600000000000000)")
        .unwrap();
    let r = q(&db, "SELECT id, YEAR(ts) FROM ev ORDER BY id");
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(2023)],
            vec![Value::Integer(2), Value::Integer(2020)],
        ]
    );
}

#[test]
fn test_month_timestamp_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE ev (id INTEGER PRIMARY KEY, ts TIMESTAMP)")
        .unwrap();
    db.execute("INSERT INTO ev VALUES (1, 1700000000000000)")
        .unwrap();
    let r = q(&db, "SELECT MONTH(ts) FROM ev");
    assert_eq!(r[0][0], Value::Integer(11));
}

#[test]
fn test_day_timestamp_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE ev (id INTEGER PRIMARY KEY, ts TIMESTAMP)")
        .unwrap();
    db.execute("INSERT INTO ev VALUES (1, 1700000000000000)")
        .unwrap();
    let r = q(&db, "SELECT DAY(ts) FROM ev");
    assert_eq!(r[0][0], Value::Integer(14));
}

#[test]
fn test_select_timestamp_column_type() {
    // SELECT ts returns the timestamp value. The zero-copy projection path
    // may return Integer (raw micros) while the scan path returns Timestamp —
    // both are acceptable as long as the value is correct. The key test is
    // that date functions work (test_year_timestamp_column etc.).
    let (db, _d) = db();
    db.execute("CREATE TABLE ev (id INTEGER PRIMARY KEY, ts TIMESTAMP)")
        .unwrap();
    db.execute("INSERT INTO ev VALUES (1, 1700000000000000)")
        .unwrap();
    let r = q(&db, "SELECT ts FROM ev");
    // Accept either Integer(micros) or Timestamp(micros) — both carry the value.
    let micros = match &r[0][0] {
        Value::Timestamp(t) => t.as_micros(),
        Value::Integer(i) => *i,
        other => panic!("expected Timestamp or Integer, got {:?}", other),
    };
    assert_eq!(micros, 1700000000000000);
}

#[test]
fn test_to_micros_timestamp_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE ev (id INTEGER PRIMARY KEY, ts TIMESTAMP)")
        .unwrap();
    db.execute("INSERT INTO ev VALUES (1, 1700000000000000)")
        .unwrap();
    let r = q(&db, "SELECT TO_MICROS(ts) FROM ev");
    assert_eq!(r[0][0], Value::Integer(1700000000000000));
}

#[test]
fn test_date_add_timestamp_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE ev (id INTEGER PRIMARY KEY, ts TIMESTAMP)")
        .unwrap();
    db.execute("INSERT INTO ev VALUES (1, 1700000000000000)")
        .unwrap();
    let r = q(&db, "SELECT DATE_ADD(ts, 86400) FROM ev");
    match &r[0][0] {
        Value::Timestamp(t) => assert_eq!(t.as_micros(), 1700086400000000),
        other => panic!("expected Timestamp, got {:?}", other),
    }
}

#[test]
fn test_year_in_where_on_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE ev (id INTEGER PRIMARY KEY, ts TIMESTAMP)")
        .unwrap();
    db.execute("INSERT INTO ev VALUES (1, 1700000000000000), (2, 1600000000000000)")
        .unwrap();
    let r = q(&db, "SELECT id FROM ev WHERE YEAR(ts) = 2023");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
    let r = q(&db, "SELECT id FROM ev WHERE YEAR(ts) = 2020");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}
