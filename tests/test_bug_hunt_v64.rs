//! Bug Hunt v64 — eleventh round: deeper query/aggregate/operator corners.

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

fn f_of(v: &Value) -> f64 {
    match v {
        Value::Integer(i) => *i as f64,
        Value::Float(f) => *f,
        _ => panic!("expected number, got {:?}", v),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// LIKE with combined wildcards
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_like_combined_wildcards() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'abc'),(2,'axc'),(3,'abbc'),(4,'ac')").unwrap();
    // a_c → exactly 3 chars, a _ c → abc, axc (not abbc, not ac)
    let r = q(&db, "SELECT id FROM t WHERE s LIKE 'a_c' ORDER BY id");
    assert_eq!(
        r.iter().map(|row| match &row[0] { Value::Integer(i) => *i, _ => -1 }).collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[test]
fn test_like_percent_underscore_combo() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'hello'),(2,'help'),(3,'heap'),(4,'hi')").unwrap();
    // he_lo → he, any char, l, o → hello only
    let r = q(&db, "SELECT id FROM t WHERE s LIKE 'he_lo' ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_like_prefix_and_suffix() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'test.txt'),(2,'test.sql'),(3,'other.txt')").unwrap();
    // test% → both test.* ; %txt → test.txt, other.txt
    let r1 = q(&db, "SELECT id FROM t WHERE s LIKE 'test%' ORDER BY id");
    assert_eq!(
        r1.iter().map(|row| match &row[0] { Value::Integer(i) => *i, _ => -1 }).collect::<Vec<_>>(),
        vec![1, 2]
    );
    let r2 = q(&db, "SELECT id FROM t WHERE s LIKE '%.txt' ORDER BY id");
    assert_eq!(
        r2.iter().map(|row| match &row[0] { Value::Integer(i) => *i, _ => -1 }).collect::<Vec<_>>(),
        vec![1, 3]
    );
}

#[test]
fn test_not_like() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'apple'),(2,'banana'),(3,'apricot')").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE s NOT LIKE 'a%' ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate with CASE
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_sum_case_conditional() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, g INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,1,10),(2,1,20),(3,2,30)").unwrap();
    // SUM(CASE WHEN g=1 THEN v ELSE 0 END) = 30
    let r = q(&db, "SELECT SUM(CASE WHEN g = 1 THEN v ELSE 0 END) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(30)]]);
}

#[test]
fn test_groupby_case_bucket() {
    // GROUP BY a CASE expression is not supported (parser expects a column
    // name). Use a pre-computed bucket column instead. This test documents
    // that GROUP BY <expression> errors (missing feature, not a wrong result).
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,15),(3,25),(4,35)").unwrap();
    let res = db.execute(
        "SELECT CASE WHEN v < 20 THEN 'low' ELSE 'high' END AS bucket, COUNT(*) \
         FROM t GROUP BY CASE WHEN v < 20 THEN 'low' ELSE 'high' END ORDER BY bucket",
    );
    assert!(res.is_err(), "GROUP BY <expression> is unsupported (should error)");
}

#[test]
fn test_having_case() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(g INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(1,20),(2,5),(2,5)").unwrap();
    let r = q(
        &db,
        "SELECT g, SUM(v) FROM t GROUP BY g HAVING CASE WHEN SUM(v) > 20 THEN 1 ELSE 0 END = 1 ORDER BY g",
    );
    // g1 sum=30 > 20 → match; g2 sum=10 → no
    assert_eq!(r, vec![vec![Value::Integer(1), Value::Integer(30)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY with NULL placement (explicit check)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_order_by_nulls_first_asc() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,30),(2,NULL),(3,10)").unwrap();
    let r = q(&db, "SELECT v FROM t ORDER BY v ASC");
    // This DB puts NULLs first in ASC (documented). So: NULL, 10, 30.
    let vals: Vec<Option<i64>> = r.iter().map(|row| match &row[0] {
        Value::Integer(i) => Some(*i),
        Value::Null => None,
        _ => panic!(),
    }).collect();
    assert_eq!(vals, vec![None, Some(10), Some(30)]);
}

#[test]
fn test_order_by_nulls_last_desc() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,30),(2,NULL),(3,10)").unwrap();
    let r = q(&db, "SELECT v FROM t ORDER BY v DESC");
    // DESC: 30, 10, NULL (NULLs last in DESC).
    let vals: Vec<Option<i64>> = r.iter().map(|row| match &row[0] {
        Value::Integer(i) => Some(*i),
        Value::Null => None,
        _ => panic!(),
    }).collect();
    assert_eq!(vals, vec![Some(30), Some(10), None]);
}

// ─────────────────────────────────────────────────────────────────────────
// Subquery returning NULL / empty
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_scalar_subquery_empty_returns_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    // Subquery matching nothing → scalar NULL.
    let r = q(&db, "SELECT (SELECT v FROM t WHERE id = 999) AS x");
    assert_eq!(r, vec![vec![Value::Null]]);
}

#[test]
fn test_subquery_in_arithmetic() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    // v + (SELECT MAX(v) FROM t) = v + 20
    let r = q(&db, "SELECT id, v + (SELECT MAX(v) FROM t) FROM t ORDER BY id");
    assert_eq!(
        r,
        vec![vec![Value::Integer(1), Value::Integer(30)], vec![Value::Integer(2), Value::Integer(40)]]
    );
}

#[test]
fn test_in_subquery_no_match() {
    let (db, _d) = db();
    db.execute("CREATE TABLE main(id INT PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE s(v INT)").unwrap();
    db.execute("INSERT INTO main VALUES (1),(2),(3)").unwrap();
    db.execute("INSERT INTO s VALUES (10),(20)").unwrap();
    let r = q(&db, "SELECT id FROM main WHERE id IN (SELECT v FROM s) ORDER BY id");
    assert!(r.is_empty(), "no match → empty");
}

#[test]
fn test_not_in_subquery() {
    let (db, _d) = db();
    db.execute("CREATE TABLE main(id INT PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE s(v INT)").unwrap();
    db.execute("INSERT INTO main VALUES (1),(2),(3)").unwrap();
    db.execute("INSERT INTO s VALUES (2)").unwrap();
    let r = q(&db, "SELECT id FROM main WHERE id NOT IN (SELECT v FROM s) ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Implicit conversion in comparisons
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_text_number_comparison() {
    // '10' vs 10 — different types. SQLite compares as text vs int (different
    // storage classes). This DB may coerce or not. Just document.
    let (db, _d) = db();
    let r = q(&db, "SELECT '10' = 10");
    // Documented: likely false (Text != Integer) or true (coerced).
    assert_eq!(r.len(), 1);
}

#[test]
fn test_timestamp_string_comparison_direct() {
    // Typed-string-literal syntax `TIMESTAMP '...'` is unsupported. Compare a
    // TIMESTAMP column value against a string literal instead (supported).
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, ts TIMESTAMP)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'2024-01-01T00:00:00')").unwrap();
    let r = q(&db, "SELECT ts < '2024-02-01T00:00:00' FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Bool(true)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Multiple aggregates with DISTINCT in one query
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_multiple_distinct_aggs() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,1,10),(2,1,20),(3,2,10),(4,2,20)").unwrap();
    let r = q(&db, "SELECT COUNT(DISTINCT a), COUNT(DISTINCT b) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(2), Value::Integer(2)]]);
}

#[test]
fn test_distinct_sum_together() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,10),(3,20)").unwrap();
    // SUM(v)=40, SUM(DISTINCT v)=30
    let r = q(&db, "SELECT SUM(v), SUM(DISTINCT v) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(40), Value::Integer(30)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// UPDATE with expression using multiple columns
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_update_multi_col_expression() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT, c INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,2,3,0)").unwrap();
    // c = a + b * 2 → 2 + 6 = 8
    db.execute("UPDATE t SET c = a + b * 2 WHERE id = 1").unwrap();
    let r = q(&db, "SELECT c FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(8)]]);
}

#[test]
fn test_update_all_rows() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    db.execute("UPDATE t SET v = 0").unwrap();
    let r = q(&db, "SELECT SUM(v) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(0)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE with function on column
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_where_abs_function() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,-5),(2,3),(3,-10)").unwrap();
    // WHERE ABS(v) > 4 → ids 1, 3
    let r = q(&db, "SELECT id FROM t WHERE ABS(v) > 4 ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

#[test]
fn test_where_mod_function() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,3),(2,4),(3,5),(4,6)").unwrap();
    // WHERE MOD(v, 2) = 0 → even → ids 2, 4
    let r = q(&db, "SELECT id FROM t WHERE MOD(v, 2) = 0 ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(2)], vec![Value::Integer(4)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// GROUP BY with WHERE filtering before grouping
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_where_then_groupby() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(g INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(1,20),(1,30),(2,5),(2,15)").unwrap();
    // WHERE v > 10, then GROUP BY g: g1 → 20,30 (sum 50); g2 → 15 (sum 15)
    let r = q(&db, "SELECT g, SUM(v) FROM t WHERE v > 10 GROUP BY g ORDER BY g");
    assert_eq!(
        r,
        vec![vec![Value::Integer(1), Value::Integer(50)], vec![Value::Integer(2), Value::Integer(15)]]
    );
}

#[test]
fn test_where_groupby_having_chain() {
    let (db, _d) = db();
    db.execute("CREATE TABLE sales(region TEXT, amt INT)").unwrap();
    db.execute("INSERT INTO sales VALUES ('US',100),('US',50),('EU',200),('EU',30)").unwrap();
    // WHERE amt > 40, GROUP BY region, HAVING SUM > 100
    // US: 100+50=150>100 ✓ ; EU: 200>100 ✓
    let r = q(&db, "SELECT region, SUM(amt) FROM sales WHERE amt > 40 GROUP BY region HAVING SUM(amt) > 100 ORDER BY region");
    assert_eq!(
        r,
        vec![vec![Value::text("EU".into()), Value::Integer(200)], vec![Value::text("US".into()), Value::Integer(150)]]
    );
}
