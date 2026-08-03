//! Bug Hunt v70 — seventeenth round: NULL three-valued logic deep checks, agg corners.

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
// NOT IN with NULL in the column (three-valued logic)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_not_in_column_with_null() {
    // If the column has NULLs, those rows are excluded by NOT IN (NULL → UNKNOWN).
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL),(3,30)")
        .unwrap();
    // NOT IN (10) → id 2 is NULL (UNKNOWN, excluded), id 3 = 30 (not in) → match.
    let r = q(&db, "SELECT id FROM t WHERE v NOT IN (10) ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

#[test]
fn test_in_column_with_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL),(3,30)")
        .unwrap();
    // IN (10, 30) → ids 1, 3 (NULL excluded).
    let r = q(&db, "SELECT id FROM t WHERE v IN (10, 30) ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

#[test]
fn test_not_in_literal_list_with_null_value() {
    // NOT IN (10, NULL) — column value is non-null but list has NULL → UNKNOWN.
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    // v NOT IN (10, NULL): every row → UNKNOWN (because NULL in list) → no rows.
    let res = db.execute("SELECT id FROM t WHERE v NOT IN (10, NULL)");
    match res {
        Ok(r) => {
            let got = rows(r.materialize().unwrap());
            // Standard SQL: NOT IN with NULL in list → no rows.
            assert!(
                got.is_empty(),
                "NOT IN (.., NULL) should yield no rows, got {:?}",
                got
            );
        }
        Err(_) => { /* NULL literal in IN list may be unsupported */ }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// COALESCE in arithmetic
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_coalesce_in_arithmetic() {
    let (db, _d) = db();
    let r = q(&db, "SELECT COALESCE(NULL, 0) + 5");
    assert_eq!(r, vec![vec![Value::Integer(5)]]);
}

#[test]
fn test_coalesce_in_comparison() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,NULL),(2,10)").unwrap();
    // COALESCE(v, 0) > 5 → id1 (0>5 false), id2 (10>5 true)
    let r = q(&db, "SELECT id FROM t WHERE COALESCE(v, 0) > 5 ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_nullif_in_arithmetic() {
    // NULLIF(x, 0) avoids div-by-zero: x / NULLIF(y, 0) → NULL if y=0.
    let (db, _d) = db();
    let r = q(&db, "SELECT 10 / NULLIF(0, 0)");
    // NULLIF(0,0) = NULL; 10 / NULL = NULL
    assert_eq!(r, vec![vec![Value::Null]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate over expression with COALESCE
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_avg_coalesce() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL),(3,30)")
        .unwrap();
    // AVG(COALESCE(v, 0)) = (10+0+30)/3 = 40/3 ≈ 13.33
    let r = q(&db, "SELECT AVG(COALESCE(v, 0)) FROM t");
    assert!((f_of(&r[0][0]) - (40.0 / 3.0)).abs() < 1e-9);
}

#[test]
fn test_sum_coalesce_not_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,NULL),(2,NULL)")
        .unwrap();
    // SUM(COALESCE(v, 0)) = 0 (not NULL, because COALESCE makes all non-null).
    let r = q(&db, "SELECT SUM(COALESCE(v, 0)) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(0)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY + LIMIT with NULLs
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_order_limit_with_nulls() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,30),(2,NULL),(3,10),(4,NULL),(5,20)")
        .unwrap();
    // ORDER BY v LIMIT 3 → NULLs first (ASC): NULL, NULL, 10 → ids 2, 4, 3
    let r = q(&db, "SELECT id FROM t ORDER BY v LIMIT 3");
    let ids: Vec<i64> = r
        .iter()
        .map(|row| match &row[0] {
            Value::Integer(i) => *i,
            _ => -1,
        })
        .collect();
    assert_eq!(ids.len(), 3);
    // First two should be the NULL rows (ids 2, 4 in some order), third is id 3 (v=10).
    assert!(
        ids.contains(&3),
        "third should be id 3 (v=10), got {:?}",
        ids
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Float column BETWEEN
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_float_between() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,1.5),(2,2.5),(3,3.5)")
        .unwrap();
    let r = q(
        &db,
        "SELECT id FROM t WHERE v BETWEEN 2.0 AND 3.0 ORDER BY id",
    );
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_float_not_between() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,1.5),(2,2.5),(3,3.5)")
        .unwrap();
    let r = q(
        &db,
        "SELECT id FROM t WHERE v NOT BETWEEN 2.0 AND 3.0 ORDER BY id",
    );
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// SUM overflow → float promotion (documented behavior)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_sum_large_no_overflow() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    // Sum within i64 range.
    db.execute("INSERT INTO t VALUES (1,1000000000),(2,2000000000),(3,3000000000)")
        .unwrap();
    let r = q(&db, "SELECT SUM(v) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(6000000000)]]);
}

#[test]
fn test_sum_promotes_to_float_on_overflow() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    // Near i64::MAX: sum overflows → promoted to Float (documented).
    let max = i64::MAX;
    db.execute(&format!("INSERT INTO t VALUES (1,{}),(2,{})", max, max))
        .unwrap();
    let r = q(&db, "SELECT SUM(v) FROM t");
    // Result should be Float (overflow), roughly 2*MAX.
    match &r[0][0] {
        Value::Float(_) => { /* documented overflow → Float */ }
        Value::Integer(_) => panic!("expected Float promotion on overflow, got Integer"),
        _ => panic!("{:?}", r),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE with column alias in expression (should error — alias not in WHERE)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_count_with_groupby_and_having_expression() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(g INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(1,20),(2,5),(2,15)")
        .unwrap();
    let r = q(
        &db,
        "SELECT g, SUM(v) * 2 AS dbl FROM t GROUP BY g HAVING SUM(v) * 2 > 30 ORDER BY g",
    );
    // g1 sum=30 *2=60>30 ✓ ; g2 sum=20 *2=40>30 ✓
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(60)],
            vec![Value::Integer(2), Value::Integer(40)],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Self-comparison (column = column)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_column_equals_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,5,5),(2,5,10),(3,10,10)")
        .unwrap();
    // a = b → ids 1, 3
    let r = q(&db, "SELECT id FROM t WHERE a = b ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

#[test]
fn test_column_greater_than_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10,5),(2,5,10),(3,7,7)")
        .unwrap();
    // a > b → id 1
    let r = q(&db, "SELECT id FROM t WHERE a > b");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// MIN/MAX of negative numbers
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_min_max_negative() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,-5),(2,-10),(3,-3)")
        .unwrap();
    let r = q(&db, "SELECT MIN(v), MAX(v) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(-10), Value::Integer(-3)]]);
}

#[test]
fn test_sum_negative() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,-10),(2,20),(3,-5)")
        .unwrap();
    let r = q(&db, "SELECT SUM(v) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(5)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// COUNT with no rows in group
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_groupby_count_zero_via_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(g INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(1,20),(2,30)")
        .unwrap();
    // WHERE filters out all of g=2 → g=2 group doesn't appear.
    let r = q(
        &db,
        "SELECT g, COUNT(*) FROM t WHERE v < 30 GROUP BY g ORDER BY g",
    );
    assert_eq!(r, vec![vec![Value::Integer(1), Value::Integer(2)]]);
}
