//! Bug Hunt v36 — Date/time functions accept Integer (micros) + IN () parse.
//!
//! Date functions (YEAR/MONTH/DAY/HOUR/MINUTE/SECOND/DAY_OF_WEEK/DATE_ADD/
//! DATE_DIFF) previously required Value::Timestamp and errored on Integer
//! literals. Now they accept both Timestamp and Integer (treating Integer as
//! epoch microseconds), matching SQLite behavior.
//!
//! NOTE: `SELECT YEAR(ts_col)` on a TIMESTAMP column still returns NULL due to
//! a deeper scan_table_rows_streamable bug (the columnar_sstables sync doesn't
//! preserve Timestamp values). This is tracked as a known storage limitation.
//! `WHERE YEAR(ts_col) = 2023` works (different code path).

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
    db.execute("CREATE TABLE ev (id INTEGER PRIMARY KEY, ts TIMESTAMP)").unwrap();
    db.execute("INSERT INTO ev VALUES (1, 1700000000000000), (2, 1600000000000000)").unwrap();
    let r = q(&db, "SELECT id FROM ev WHERE YEAR(ts) = 2023");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_year_in_where_no_match() {
    let (db, _d) = db();
    db.execute("CREATE TABLE ev (id INTEGER PRIMARY KEY, ts TIMESTAMP)").unwrap();
    db.execute("INSERT INTO ev VALUES (1, 1700000000000000)").unwrap();
    let r = q(&db, "SELECT id FROM ev WHERE YEAR(ts) = 1999");
    assert!(r.is_empty());
}

// =========================================================================
// || concatenation in UPDATE SET
// =========================================================================

#[test]
fn test_update_concat_operator() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
    db.execute("UPDATE t SET name = 'pre_' || name WHERE id = 1").unwrap();
    let r = q(&db, "SELECT name FROM t WHERE id = 1");
    assert_eq!(r[0][0], Value::text("pre_alice".into()));
}
