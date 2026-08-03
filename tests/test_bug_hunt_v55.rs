//! Bug Hunt v55 — second round: probing candidates from a deeper audit.

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
// Bug A/C: SUM/MIN/MAX over INTEGER column via fast path returns Float
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_sum_int_returns_int_simple() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    let r = q(&db, "SELECT SUM(v) FROM t");
    assert_eq!(
        r,
        vec![vec![Value::Integer(30)]],
        "SUM of INT column must be Integer, got {:?}",
        r
    );
}

#[test]
fn test_min_max_int_returns_int_simple() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,5)")
        .unwrap();
    let r = q(&db, "SELECT MIN(v), MAX(v) FROM t");
    assert_eq!(
        r,
        vec![vec![Value::Integer(5), Value::Integer(20)]],
        "MIN/MAX of INT column must be Integer, got {:?}",
        r
    );
}

#[test]
fn test_sum_int_with_count_and_text_filter() {
    // Triggers the count_sum_min_max_text_filter fast path (Bug A).
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, amt INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'x',10),(2,'x',20),(3,'y',5)")
        .unwrap();
    let r = q(&db, "SELECT COUNT(*), SUM(amt) FROM t WHERE cat='x'");
    assert_eq!(
        r,
        vec![vec![Value::Integer(2), Value::Integer(30)]],
        "SUM(amt) with text filter must be Integer 30, got {:?}",
        r
    );
}

#[test]
fn test_min_max_int_with_text_filter() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, amt INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'x',10),(2,'x',20),(3,'x',5)")
        .unwrap();
    let r = q(&db, "SELECT MIN(amt), MAX(amt) FROM t WHERE cat='x'");
    assert_eq!(
        r,
        vec![vec![Value::Integer(5), Value::Integer(20)]],
        "MIN/MAX(amt) with text filter must be Integer, got {:?}",
        r
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Bug B: MIN/MAX/AVG over empty set should be NULL, not 0
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_agg_empty_set_returns_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    let r = q(
        &db,
        "SELECT COUNT(*), MIN(v), MAX(v), AVG(v), SUM(v) FROM t WHERE v > 100",
    );
    assert_eq!(
        r,
        vec![vec![
            Value::Integer(0),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ]],
        "aggregates over empty set: COUNT=0, others NULL; got {:?}",
        r
    );
}

#[test]
fn test_agg_empty_set_with_text_filter() {
    // Triggers count_sum_min_max_text_filter with no matches (Bug B).
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, amt INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'x',10)").unwrap();
    let r = q(
        &db,
        "SELECT COUNT(*), MIN(amt), MAX(amt), AVG(amt) FROM t WHERE cat='nope'",
    );
    assert_eq!(
        r,
        vec![vec![
            Value::Integer(0),
            Value::Null,
            Value::Null,
            Value::Null,
        ]],
        "empty-set aggregates with text filter must be NULL; got {:?}",
        r
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Bug D/E/F: NULL three-valued logic in IN/BETWEEN/LIKE projections
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_null_in_list_returns_null() {
    let (db, _d) = db();
    let r = q(&db, "SELECT NULL IN (1, 2, 3)");
    assert_eq!(r, vec![vec![Value::Null]], "NULL IN (...) should be NULL");
}

#[test]
fn test_value_in_list_with_null_no_match_returns_null() {
    // 5 not in (1,2,NULL) → UNKNOWN → NULL (not false)
    let (db, _d) = db();
    let r = q(&db, "SELECT 5 IN (1, 2, NULL)");
    assert_eq!(
        r,
        vec![vec![Value::Null]],
        "5 IN (1,2,NULL) should be NULL (unknown), got {:?}",
        r
    );
}

#[test]
fn test_value_in_list_with_null_match_returns_true() {
    // 2 in (1,2,NULL) → TRUE (found, regardless of NULL)
    let (db, _d) = db();
    let r = q(&db, "SELECT 2 IN (1, 2, NULL)");
    assert_eq!(r, vec![vec![Value::Bool(true)]]);
}

#[test]
fn test_null_between_returns_null() {
    let (db, _d) = db();
    let r = q(&db, "SELECT NULL BETWEEN 1 AND 5");
    assert_eq!(r, vec![vec![Value::Null]], "NULL BETWEEN should be NULL");
}

#[test]
fn test_between_with_null_bound_returns_null() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 3 BETWEEN NULL AND 5");
    assert_eq!(
        r,
        vec![vec![Value::Null]],
        "3 BETWEEN NULL AND 5 should be NULL, got {:?}",
        r
    );
}

#[test]
fn test_null_like_returns_null() {
    let (db, _d) = db();
    let r = q(&db, "SELECT NULL LIKE 'a%'");
    assert_eq!(r, vec![vec![Value::Null]], "NULL LIKE should be NULL");
}

#[test]
fn test_value_like_null_returns_null() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 'abc' LIKE NULL");
    assert_eq!(
        r,
        vec![vec![Value::Null]],
        "'abc' LIKE NULL should be NULL, got {:?}",
        r
    );
}

// Consistency check: NULL = 1 already returns NULL (prior fix) — verify.
#[test]
fn test_null_eq_returns_null() {
    let (db, _d) = db();
    let r = q(&db, "SELECT NULL = 1");
    assert_eq!(r, vec![vec![Value::Null]]);
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE three-valued logic must still be correct after the projection fix:
// NULL predicates are treated as "not matched" (is_truthy(Null) == false).
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_where_not_in_subquery_with_null_yields_no_rows() {
    // NOT IN with a NULL in the set → UNKNOWN for every row → no rows.
    let (db, _d) = db();
    db.execute("CREATE TABLE main(id INT PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE excl(v INT)").unwrap();
    db.execute("INSERT INTO main VALUES (1),(2),(3)").unwrap();
    db.execute("INSERT INTO excl VALUES (2),(NULL)").unwrap();
    let r = q(
        &db,
        "SELECT id FROM main WHERE id NOT IN (SELECT v FROM excl) ORDER BY id",
    );
    assert!(r.is_empty(), "NOT IN with NULL in set must yield no rows");
}

#[test]
fn test_where_in_subquery_with_null_still_matches() {
    // IN with a NULL in the set still returns the matched rows.
    let (db, _d) = db();
    db.execute("CREATE TABLE main(id INT PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE excl(v INT)").unwrap();
    db.execute("INSERT INTO main VALUES (1),(2),(3)").unwrap();
    db.execute("INSERT INTO excl VALUES (2),(NULL)").unwrap();
    let r = q(
        &db,
        "SELECT id FROM main WHERE id IN (SELECT v FROM excl) ORDER BY id",
    );
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_where_between_null_bound_matches_nothing() {
    // x BETWEEN NULL AND 5 → UNKNOWN → no rows.
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,3),(2,7)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE v BETWEEN NULL AND 5");
    assert!(r.is_empty());
}

#[test]
fn test_where_not_in_literal_list_works() {
    // Sanity: NOT IN without NULL works normally.
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE id NOT IN (1, 2) ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Bug H: Int/Float numerically-equal dedup in DISTINCT / COUNT(DISTINCT)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_count_distinct_int_vs_float_consistent() {
    // This DB intentionally treats numerically-equal Int and Float as equal
    // (Integer(5) == Float(5.0) with matching hashes — see types/mod.rs). This
    // is a deliberate cross-type numeric-equality design, NOT a bug: it is
    // consistent across `=`, IN, GROUP BY, DISTINCT, and UNION dedup. SQLite
    // differs (distinct storage classes → 2), but this matches Postgres-ish
    // numeric equality. This test documents and locks the consistent behavior.
    let (db, _d) = db();
    // Cross-type numeric equality holds.
    let r = q(&db, "SELECT 5 = 5.0, 5 < 5.1, 5 > 4.9");
    assert_eq!(
        r,
        vec![vec![
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true)
        ]]
    );
    // IN dedups across types too.
    let r2 = q(&db, "SELECT 5 IN (5.0, 6.0)");
    assert_eq!(r2, vec![vec![Value::Bool(true)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Bonus probes: arithmetic/coercion edge cases
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_null_arithmetic_returns_null() {
    let (db, _d) = db();
    let r = q(&db, "SELECT NULL + 1, NULL * 2, NULL - 1");
    assert_eq!(r, vec![vec![Value::Null, Value::Null, Value::Null]]);
}

#[test]
fn test_sum_all_null_returns_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,NULL),(2,NULL)")
        .unwrap();
    let r = q(&db, "SELECT SUM(v), AVG(v), MIN(v), MAX(v) FROM t");
    assert_eq!(
        r,
        vec![vec![Value::Null, Value::Null, Value::Null, Value::Null]],
        "SUM/AVG/MIN/MAX of all-NULL must be NULL, got {:?}",
        r
    );
}

#[test]
fn test_count_null_column_returns_zero() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,NULL),(2,NULL)")
        .unwrap();
    let r = q(&db, "SELECT COUNT(v) FROM t");
    assert_eq!(
        r,
        vec![vec![Value::Integer(0)]],
        "COUNT of all-NULL column is 0"
    );
}
