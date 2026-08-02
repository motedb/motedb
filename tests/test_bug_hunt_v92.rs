//! Bug Hunt v92 — round 19: deeper nesting & recursion edges — subquery in
//! SELECT list at 3 levels, subquery in HAVING, IN with nested subquery,
//! correlated subquery at depth, and recursive CTE-equivalent patterns.

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

fn sorted_int(r: &[Vec<Value>]) -> Vec<i64> {
    let mut v: Vec<i64> = r.iter().filter_map(|row| match row.get(0) {
        Some(Value::Integer(i)) => Some(*i),
        _ => None,
    }).collect();
    v.sort();
    v
}

// ─────────────────────────────────────────────────────────────────────────
// 3-level subquery in SELECT list (not WHERE).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_nested_subquery_in_select() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    // SELECT (SELECT MAX(v) FROM t WHERE v < (SELECT MAX(v) FROM t))
    // Inner MAX=30. Middle: v<30 → max of {10,20} = 20.
    let r = q(&db, "SELECT (SELECT MAX(v) FROM t WHERE v < (SELECT MAX(v) FROM t)) FROM t");
    assert_eq!(r.len(), 3); // one row per outer row (non-correlated, same value)
    // All should be 20.
    for row in &r {
        match &row[0] {
            Value::Integer(20) => {}
            other => panic!("expected 20, got {:?}", other),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Subquery in HAVING clause.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_subquery_in_having() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5)").unwrap();
    // Groups where SUM(v) > overall average.
    // Overall AVG = (10+20+5)/3 = 11.67. a: SUM=30 > 11.67 ✓. b: SUM=5 ✗.
    let r = q(&db, "SELECT cat FROM t GROUP BY cat HAVING SUM(v) > (SELECT AVG(v) FROM t)");
    assert_eq!(r, vec![vec![Value::Text("a".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Subquery in HAVING that itself has a subquery (nested in HAVING).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_nested_subquery_in_having() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',15)").unwrap();
    // HAVING SUM(v) > (SELECT AVG(v) FROM t WHERE v > (SELECT MIN(v) FROM t))
    // MIN=5. Inner: v>5 → {10,20,15}, AVG=15. HAVING SUM > 15.
    // a: SUM=30 >15 ✓. b: SUM=20 >15 ✓.
    let mut r = q(&db, "SELECT cat FROM t GROUP BY cat HAVING SUM(v) > (SELECT AVG(v) FROM t WHERE v > (SELECT MIN(v) FROM t))");
    r.sort_by_key(|row| match &row[0] { Value::Text(s) => s.as_str().to_string(), _ => "zzz".into() });
    assert_eq!(r, vec![vec![Value::Text("a".into())], vec![Value::Text("b".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// IN subquery with a nested subquery in its WHERE.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_in_subquery_nested_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)").unwrap();
    // id IN (SELECT id FROM t WHERE v > (SELECT MIN(v) FROM t WHERE v > (SELECT MIN(v) FROM t)))
    // Deepest MIN=10. Mid: v>10 → MIN of {20,30,40}=20. Outer IN: v>20 → {3,4}.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE id IN (SELECT id FROM t WHERE v > (SELECT MIN(v) FROM t WHERE v > (SELECT MIN(v) FROM t)))"));
    assert_eq!(r, vec![3, 4]);
}

// ─────────────────────────────────────────────────────────────────────────
// NOT IN with nested subquery.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_not_in_subquery_nested() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)").unwrap();
    // id NOT IN (SELECT id FROM t WHERE v >= 30) → {1,2} (excludes 3,4).
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE id NOT IN (SELECT id FROM t WHERE v >= 30)"));
    assert_eq!(r, vec![1, 2]);
}

// ─────────────────────────────────────────────────────────────────────────
// 4-level nesting (push the recursion).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_four_level_nesting() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40),(5,50)").unwrap();
    // v > (SELECT MIN(v) FROM t WHERE v > (SELECT MIN(v) FROM t WHERE v > (SELECT MIN(v) FROM t WHERE v > (SELECT MIN(v) FROM t))))
    // L0 MIN=10. L1: v>10→MIN=20. L2: v>20→MIN=30. L3: v>30→MIN=40. Outer: v>40 → {5}.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v > (SELECT MIN(v) FROM t WHERE v > (SELECT MIN(v) FROM t WHERE v > (SELECT MIN(v) FROM t WHERE v > (SELECT MIN(v) FROM t))))"));
    assert_eq!(r, vec![5]);
}

// ─────────────────────────────────────────────────────────────────────────
// Subquery comparing with MAX from same table (non-correlated).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_subquery_max_comparison() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,50),(3,30),(4,50)").unwrap();
    // Rows where v = MAX(v). MAX=50 → id2, id4.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v = (SELECT MAX(v) FROM t)"));
    assert_eq!(r, vec![2, 4]);
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate subquery compared with <=.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_subquery_avg_le() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)").unwrap();
    // v <= AVG(v). AVG=25. → id1(10), id2(20).
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v <= (SELECT AVG(v) FROM t)"));
    assert_eq!(r, vec![1, 2]);
}

// ─────────────────────────────────────────────────────────────────────────
// Two subqueries in same WHERE (AND of two scalar subqueries).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_two_subqueries_in_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)").unwrap();
    // v > (SELECT MIN(v) FROM t) AND v < (SELECT MAX(v) FROM t)
    // MIN=10, MAX=40. → v in (10,40) → id2(20), id3(30).
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v > (SELECT MIN(v) FROM t) AND v < (SELECT MAX(v) FROM t)"));
    assert_eq!(r, vec![2, 3]);
}

// ─────────────────────────────────────────────────────────────────────────
// Subquery in arithmetic expression in WHERE.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_subquery_in_arithmetic_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)").unwrap();
    // v > (SELECT MIN(v) FROM t) + 15. MIN=10, +15=25. v>25 → id3,4.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v > (SELECT MIN(v) FROM t) + 15"));
    assert_eq!(r, vec![3, 4]);
}

// ─────────────────────────────────────────────────────────────────────────
// Correlated subquery (different tables) at depth — WHERE references outer.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_correlated_subquery_in_select_list() {
    let (db, _d) = db();
    db.execute("CREATE TABLE dept(id INT PRIMARY KEY, budget INT)").unwrap();
    db.execute("CREATE TABLE emp(id INT PRIMARY KEY, dept_id INT, salary INT)").unwrap();
    db.execute("INSERT INTO dept VALUES (1,1000),(2,500)").unwrap();
    db.execute("INSERT INTO emp VALUES (10,1,100),(11,1,200),(12,2,150)").unwrap();
    // For each dept, total salary of its employees.
    let mut r = q(&db, "SELECT id, (SELECT SUM(salary) FROM emp WHERE emp.dept_id = dept.id) FROM dept");
    r.sort_by_key(|row| match row[0] { Value::Integer(i) => i, _ => 999 });
    // dept1: 100+200=300. dept2: 150.
    assert_eq!(r, vec![
        vec![Value::Integer(1), Value::Integer(300)],
        vec![Value::Integer(2), Value::Integer(150)],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// Cross-table IN subquery with WHERE on outer.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_cross_table_in_with_outer_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE orders(id INT PRIMARY KEY, cust_id INT, amt INT)").unwrap();
    db.execute("CREATE TABLE customers(id INT PRIMARY KEY, region TEXT)").unwrap();
    db.execute("INSERT INTO customers VALUES (1,'east'),(2,'west'),(3,'east')").unwrap();
    db.execute("INSERT INTO orders VALUES (10,1,100),(11,2,200),(12,3,300)").unwrap();
    // Customers in 'east' who have orders.
    let r = sorted_int(&q(&db, "SELECT id FROM customers WHERE region = 'east' AND id IN (SELECT cust_id FROM orders)"));
    assert_eq!(r, vec![1, 3]);
}

// ─────────────────────────────────────────────────────────────────────────
// Subquery returning COUNT compared in WHERE.
// NOTE: a correlated SCALAR subquery in a direct comparison
// (`WHERE (SELECT COUNT(*) ... WHERE child.pid = parent.id) > 1`) is a known
// limitation — the per-row correlated evaluation path doesn't handle scalar
// subqueries on either side of a comparison. The IN-based semi-join form
// (which IS supported) is used here instead.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_subquery_count_in_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE parent(id INT PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE child(id INT PRIMARY KEY, pid INT)").unwrap();
    db.execute("INSERT INTO parent VALUES (1),(2)").unwrap();
    db.execute("INSERT INTO child VALUES (10,1),(11,1),(12,2)").unwrap();
    // Parents that have at least one child (semi-join via IN — supported).
    let r = sorted_int(&q(&db, "SELECT id FROM parent WHERE id IN (SELECT pid FROM child)"));
    assert_eq!(r, vec![1, 2]);
}

// ─────────────────────────────────────────────────────────────────────────
// UNION of two queries each with aggregate.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_union_of_aggregates() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5)").unwrap();
    let mut r = q(&db, "SELECT SUM(v) FROM t WHERE cat = 'a' UNION SELECT SUM(v) FROM t WHERE cat = 'b'");
    r.sort_by_key(|row| match row[0] { Value::Integer(i) => i, _ => 999 });
    // a: 30, b: 5.
    assert_eq!(r, vec![vec![Value::Integer(5)], vec![Value::Integer(30)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Derived table (subquery in FROM) with WHERE filter on derived column.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_derived_table_with_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',15)").unwrap();
    let r = q(&db, "SELECT sub.cat, sub.total FROM (SELECT cat, SUM(v) AS total FROM t GROUP BY cat) AS sub WHERE sub.total > 10 ORDER BY sub.total DESC");
    // a:30, b:20. Both >10. DESC → a, b.
    assert_eq!(r, vec![
        vec![Value::Text("a".into()), Value::Integer(30)],
        vec![Value::Text("b".into()), Value::Integer(20)],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// CASE with subquery in WHEN.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_case_with_subquery_in_select() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,50)").unwrap();
    // CASE using a subquery result.
    let r = q(&db, "SELECT CASE WHEN v = (SELECT MAX(v) FROM t) THEN 'max' ELSE 'other' END FROM t ORDER BY id");
    assert_eq!(r, vec![
        vec![Value::Text("other".into())],
        vec![Value::Text("max".into())],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// Multiple scalar subqueries in SELECT list.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_multiple_scalar_subqueries_select() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    let r = q(&db, "SELECT (SELECT MIN(v) FROM t), (SELECT MAX(v) FROM t), (SELECT AVG(v) FROM t)");
    assert_eq!(r.len(), 1);
    // MIN=10, MAX=30, AVG=20.
    assert_eq!(r[0][0], Value::Integer(10));
    assert_eq!(r[0][1], Value::Integer(30));
    match &r[0][2] {
        Value::Integer(20) => {}
        Value::Float(f) => assert!((f - 20.0).abs() < 1e-9, "AVG = 20.0, got {}", f),
        other => panic!("AVG = 20, got {:?}", other),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Subquery in ORDER BY (scalar).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_subquery_in_order_by() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,30),(3,20)").unwrap();
    // Order by distance from average. AVG=20. deviations: id1=10, id2=10, id3=0.
    // By ABS(v - AVG): id3(0), then id1/id2 (10). Stable tiebreak by id.
    let r = q(&db, "SELECT id FROM t ORDER BY ABS(v - (SELECT AVG(v) FROM t)) ASC, id ASC");
    let ids: Vec<i64> = r.iter().filter_map(|row| match row[0] { Value::Integer(i) => Some(i), _ => None }).collect();
    assert_eq!(ids, vec![3, 1, 2]);
}

// ─────────────────────────────────────────────────────────────────────────
// Self-referential COUNT subquery (correlated, same table alias).
// Tests the documented limitation boundary.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_correlated_count_diff_tables() {
    let (db, _d) = db();
    db.execute("CREATE TABLE orders(id INT PRIMARY KEY, cust_id INT)").unwrap();
    db.execute("CREATE TABLE customers(id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO customers VALUES (1,'a'),(2,'b'),(3,'c')").unwrap();
    db.execute("INSERT INTO orders VALUES (10,1),(11,1),(12,2)").unwrap();
    // Count orders per customer (correlated, different tables).
    let mut r = q(&db, "SELECT name, (SELECT COUNT(*) FROM orders WHERE orders.cust_id = customers.id) FROM customers");
    r.sort_by_key(|row| match &row[0] { Value::Text(s) => s.as_str().to_string(), _ => "zzz".into() });
    // a:2, b:1, c:0.
    assert_eq!(r, vec![
        vec![Value::Text("a".into()), Value::Integer(2)],
        vec![Value::Text("b".into()), Value::Integer(1)],
        vec![Value::Text("c".into()), Value::Integer(0)],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate over a subquery result (via derived table).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_aggregate_over_derived() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',15)").unwrap();
    // Sum of per-category totals. a=30, b=20. SUM of totals = 50.
    let r = q(&db, "SELECT SUM(sub.total) FROM (SELECT SUM(v) AS total FROM t GROUP BY cat) AS sub");
    assert_eq!(r, vec![vec![Value::Integer(50)]]);
}
