//! Bug Hunt v31 — GROUP BY column-ordering, SUM/MIN/MAX/AVG/COUNT(col)
//! NULL semantics in the columnar GROUP BY fast path.
//!
//! Root cause: `col_segment_group_by` (executor.rs) hard-coded the GROUP BY
//! column as the first SELECT output slot, breaking `SELECT COUNT(*), g ...`.
//! It also:
//!   - Used `group_mins == INFINITY` as the "all-NULL" sentinel for SUM, but
//!     only computed min/max when the query had MIN/MAX — so every SUM wrongly
//!     returned NULL.
//!   - Leaked the INFINITY / NEG_INFINITY initial sentinel as i64::MAX/MIN
//!     for MIN/MAX over an all-NULL group.
//!   - Divided AVG by total row count (COUNT(*)) instead of non-NULL count.
//!   - Returned COUNT(*) for COUNT(col) instead of non-NULL count.
//!
//! Each test corresponds to a confirmed wrong-result case, now fixed.

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

/// Compare two row sets regardless of row order (GROUP BY results are not
/// guaranteed to be in any particular order).
fn assert_rows_eq(actual: Vec<Vec<Value>>, expected: Vec<Vec<Value>>) {
    let mut a: Vec<String> = actual.iter().map(|r| format!("{:?}", r)).collect();
    let mut b: Vec<String> = expected.iter().map(|r| format!("{:?}", r)).collect();
    a.sort();
    b.sort();
    assert_eq!(a, b, "rows differ (compared as sets)");
}

// =========================================================================
// A: GROUP BY column ordering
// =========================================================================

#[test]
fn test_groupby_agg_first_then_key() {
    // Previously returned [[key, NULL]] — the COUNT(*) slot was NULL because
    // the group key was hard-coded into position 0.
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, g TEXT, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30)").unwrap();
    let r = q(&db, "SELECT COUNT(*), g FROM t GROUP BY g");
    assert_rows_eq(r, vec![
        vec![Value::Integer(2), Value::text("a".into())],
        vec![Value::Integer(1), Value::text("b".into())],
    ]);
}

#[test]
fn test_groupby_two_aggs_key_last() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, g TEXT, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 5)").unwrap();
    let r = q(&db, "SELECT COUNT(*), SUM(v), g FROM t GROUP BY g");
    assert_rows_eq(r, vec![
        vec![Value::Integer(2), Value::Integer(30), Value::text("a".into())],
        vec![Value::Integer(1), Value::Integer(5), Value::text("b".into())],
    ]);
}

#[test]
fn test_groupby_agg_middle() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, g TEXT, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 5)").unwrap();
    let r = q(&db, "SELECT g, COUNT(*), MAX(v) FROM t GROUP BY g");
    assert_rows_eq(r, vec![
        vec![Value::text("a".into()), Value::Integer(2), Value::Integer(20)],
        vec![Value::text("b".into()), Value::Integer(1), Value::Integer(5)],
    ]);
}

#[test]
fn test_groupby_all_aggs() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, g TEXT, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 5), (4, 'b', 7)").unwrap();
    let r = q(&db, "SELECT g, COUNT(*), SUM(v), MIN(v), MAX(v), AVG(v) FROM t GROUP BY g");
    assert_rows_eq(r, vec![
        vec![
            Value::text("a".into()), Value::Integer(2), Value::Integer(30),
            Value::Integer(10), Value::Integer(20), Value::Float(15.0),
        ],
        vec![
            Value::text("b".into()), Value::Integer(2), Value::Integer(12),
            Value::Integer(5), Value::Integer(7), Value::Float(6.0),
        ],
    ]);
}

// =========================================================================
// B: NULL semantics in aggregates
// =========================================================================

#[test]
fn test_groupby_sum_all_null_is_null() {
    // Group 'c' has only NULL values → SUM should be NULL (not 0).
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, g TEXT, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a', 10), (2, 'c', NULL)").unwrap();
    let r = q(&db, "SELECT g, SUM(v) FROM t GROUP BY g");
    assert_rows_eq(r, vec![
        vec![Value::text("a".into()), Value::Integer(10)],
        vec![Value::text("c".into()), Value::Null],
    ]);
}

#[test]
fn test_groupby_min_max_all_null_is_null() {
    // Previously leaked i64::MAX / i64::MIN (the INFINITY/NEG_INFINITY
    // initial sentinels cast to i64).
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, g TEXT, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'c', NULL)").unwrap();
    let r = q(&db, "SELECT MIN(v), MAX(v) FROM t GROUP BY g");
    assert_eq!(r, vec![vec![Value::Null, Value::Null]]);
}

#[test]
fn test_groupby_avg_ignores_null() {
    // AVG should divide SUM by non-NULL count, not total row count.
    // Group 'a': values [10, NULL, 30] → AVG = 40/2 = 20, not 40/3.
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, g TEXT, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a', 10), (2, 'a', NULL), (3, 'a', 30)").unwrap();
    let r = q(&db, "SELECT g, AVG(v) FROM t GROUP BY g");
    assert_eq!(r, vec![vec![Value::text("a".into()), Value::Float(20.0)]]);
}

#[test]
fn test_groupby_count_col_ignores_null() {
    // COUNT(v) counts non-NULL values only; COUNT(*) counts all rows.
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, g TEXT, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a', 10), (2, 'a', NULL), (3, 'a', 30)").unwrap();
    let r = q(&db, "SELECT g, COUNT(*), COUNT(v) FROM t GROUP BY g");
    assert_eq!(r, vec![vec![
        Value::text("a".into()),
        Value::Integer(3),  // COUNT(*)
        Value::Integer(2),  // COUNT(v) — non-NULL
    ]]);
}

#[test]
fn test_groupby_count_col_with_sum_on_same_col() {
    // COUNT(v) reuses the SUM agg's nn_count tracker.
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, g TEXT, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a', 10), (2, 'a', NULL), (3, 'a', 30), (4, 'b', NULL)").unwrap();
    let r = q(&db, "SELECT g, COUNT(v), SUM(v) FROM t GROUP BY g");
    assert_rows_eq(r, vec![
        vec![Value::text("a".into()), Value::Integer(2), Value::Integer(40)],
        vec![Value::text("b".into()), Value::Integer(0), Value::Null],
    ]);
}

#[test]
fn test_groupby_avg_all_null_is_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, g TEXT, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'c', NULL), (2, 'c', NULL)").unwrap();
    let r = q(&db, "SELECT g, AVG(v) FROM t GROUP BY g");
    assert_eq!(r, vec![vec![Value::text("c".into()), Value::Null]]);
}

#[test]
fn test_groupby_null_group_row() {
    // Rows with NULL in the GROUP BY column form their own group.
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, g TEXT, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a', 10), (2, NULL, 100), (3, NULL, 200)").unwrap();
    let r = q(&db, "SELECT g, COUNT(*), SUM(v) FROM t GROUP BY g");
    assert_rows_eq(r, vec![
        vec![Value::Null, Value::Integer(2), Value::Integer(300)],
        vec![Value::text("a".into()), Value::Integer(1), Value::Integer(10)],
    ]);
}

// =========================================================================
// C: IN (SELECT ...) subquery — was treated as scalar and errored
// =========================================================================

#[test]
fn test_in_select_subquery_text() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a (id INT, name TEXT)").unwrap();
    db.execute("CREATE TABLE b (bname TEXT)").unwrap();
    db.execute("INSERT INTO a VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')").unwrap();
    db.execute("INSERT INTO b VALUES ('alice'), ('carol')").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM a WHERE name IN (SELECT bname FROM b)");
    assert_eq!(r[0][0], Value::Integer(2));
}

#[test]
fn test_not_in_select_subquery() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a (id INT, name TEXT)").unwrap();
    db.execute("CREATE TABLE b (bname TEXT)").unwrap();
    db.execute("INSERT INTO a VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')").unwrap();
    db.execute("INSERT INTO b VALUES ('alice'), ('carol')").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM a WHERE name NOT IN (SELECT bname FROM b)");
    assert_eq!(r[0][0], Value::Integer(1)); // bob
}

#[test]
fn test_not_in_select_subquery_with_null() {
    // NOT IN with NULL in subquery → no rows (SQL three-valued logic).
    let (db, _d) = db();
    db.execute("CREATE TABLE a (id INT, name TEXT)").unwrap();
    db.execute("CREATE TABLE b (bname TEXT)").unwrap();
    db.execute("INSERT INTO a VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')").unwrap();
    db.execute("INSERT INTO b VALUES ('alice'), (NULL)").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM a WHERE name NOT IN (SELECT bname FROM b)");
    assert_eq!(r[0][0], Value::Integer(0));
}

// =========================================================================
// D: HAVING without GROUP BY — COUNT(*) O(1) fast path ignored HAVING
// =========================================================================

#[test]
fn test_having_no_groupby_false_condition() {
    // SELECT COUNT(*) FROM t HAVING COUNT(*) > 5 where count is 3 → 0 rows.
    // Previously the O(1) row-counter fast path returned [[3]] ignoring HAVING.
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t HAVING COUNT(*) > 5");
    assert!(r.is_empty(), "HAVING COUNT(*) > 5 with count=3 should return 0 rows, got {:?}", r);
}

#[test]
fn test_having_no_groupby_true_condition() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t HAVING COUNT(*) > 2");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

#[test]
fn test_having_no_groupby_equality() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t HAVING COUNT(*) = 3");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
    let r = q(&db, "SELECT COUNT(*) FROM t HAVING COUNT(*) = 5");
    assert!(r.is_empty());
}

#[test]
fn test_having_no_groupby_with_other_aggregate() {
    // HAVING on SUM (not COUNT) without GROUP BY.
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();
    let r = q(&db, "SELECT SUM(v) FROM t HAVING SUM(v) > 50");
    assert_eq!(r, vec![vec![Value::Integer(60)]]);
    let r = q(&db, "SELECT SUM(v) FROM t HAVING SUM(v) > 100");
    assert!(r.is_empty(), "HAVING SUM(v) > 100 with sum=60 should return 0 rows");
}
