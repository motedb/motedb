//! Bug Hunt v48 — HAVING with aggregate expression arguments.
//!
//! `HAVING SUM(q * p) > 100` returned empty results because the aggregate
//! key construction used `format!("{:?}", arg)` (Debug format) in both the
//! executor (aggregate_expr_key) and the evaluator, producing keys like
//! `SUM(BinaryOp { ... })` that didn't match the SELECT column name
//! `SUM(q Mul p)` (from expr_to_column_name). The HAVING lookup failed
//! → every group was filtered out.
//!
//! Fixed by making both aggregate_expr_key and the evaluator's aggregate
//! key builder use the same expression-to-name format as expr_to_column_name.

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

fn assert_rows(actual: Vec<Vec<Value>>, expected: Vec<Vec<Value>>) {
    let mut a: Vec<String> = actual.iter().map(|r| format!("{:?}", r)).collect();
    let mut b: Vec<String> = expected.iter().map(|r| format!("{:?}", r)).collect();
    a.sort();
    b.sort();
    assert_eq!(a, b);
}

#[test]
fn test_having_sum_expression() {
    let (db, _d) = db();
    db.execute("CREATE TABLE s (id INTEGER PRIMARY KEY, r TEXT, q INTEGER, p INTEGER)")
        .unwrap();
    db.execute("INSERT INTO s VALUES (1, 'US', 10, 5), (2, 'US', 20, 3), (3, 'EU', 15, 5), (4, 'US', 5, 5), (5, 'EU', 10, 3)").unwrap();
    // US: SUM(q*p) = 50+60+25 = 135; EU: SUM(q*p) = 75+30 = 105
    let r = q(
        &db,
        "SELECT r, SUM(q * p) FROM s GROUP BY r HAVING SUM(q * p) > 100",
    );
    assert_rows(
        r,
        vec![
            vec![Value::text("EU".into()), Value::Integer(105)],
            vec![Value::text("US".into()), Value::Integer(135)],
        ],
    );
}

#[test]
fn test_having_sum_expression_false() {
    let (db, _d) = db();
    db.execute("CREATE TABLE s (id INTEGER PRIMARY KEY, r TEXT, q INTEGER, p INTEGER)")
        .unwrap();
    db.execute("INSERT INTO s VALUES (1, 'US', 10, 5), (2, 'US', 20, 3)")
        .unwrap();
    // US: SUM(q*p) = 50+60 = 110
    let r = q(
        &db,
        "SELECT r, SUM(q * p) FROM s GROUP BY r HAVING SUM(q * p) > 200",
    );
    assert!(r.is_empty());
}

#[test]
fn test_having_count_expression() {
    let (db, _d) = db();
    db.execute("CREATE TABLE s (id INTEGER PRIMARY KEY, r TEXT, q INTEGER, p INTEGER)")
        .unwrap();
    db.execute("INSERT INTO s VALUES (1, 'US', 10, 5), (2, 'US', 20, 3), (3, 'EU', 15, 5)")
        .unwrap();
    let r = q(
        &db,
        "SELECT r, COUNT(q * p) FROM s GROUP BY r HAVING COUNT(q * p) >= 2",
    );
    assert_rows(r, vec![vec![Value::text("US".into()), Value::Integer(2)]]);
}

#[test]
fn test_having_avg_expression() {
    let (db, _d) = db();
    db.execute("CREATE TABLE s (id INTEGER PRIMARY KEY, r TEXT, q INTEGER, p INTEGER)")
        .unwrap();
    db.execute("INSERT INTO s VALUES (1, 'A', 10, 2), (2, 'A', 20, 4), (3, 'B', 5, 1)")
        .unwrap();
    // A: AVG(q*p) = (20+80)/2 = 50; B: AVG(q*p) = 5
    let r = q(
        &db,
        "SELECT r, AVG(q * p) FROM s GROUP BY r HAVING AVG(q * p) > 10",
    );
    assert_rows(r, vec![vec![Value::text("A".into()), Value::Float(50.0)]]);
}

#[test]
fn test_having_simple_regression() {
    // Regression: simple HAVING (no expression args) still works.
    let (db, _d) = db();
    db.execute("CREATE TABLE s (id INTEGER PRIMARY KEY, r TEXT, q INTEGER)")
        .unwrap();
    db.execute("INSERT INTO s VALUES (1, 'US', 10), (2, 'US', 20), (3, 'EU', 5)")
        .unwrap();
    let r = q(&db, "SELECT r, SUM(q) FROM s GROUP BY r HAVING SUM(q) > 15");
    assert_rows(r, vec![vec![Value::text("US".into()), Value::Integer(30)]]);
}
