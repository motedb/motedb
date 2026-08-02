//! Bug Hunt v93 — round 20: more expression-container subquery recursion,
//! nested CASE, COALESCE-with-subquery, deeply nested IN, and consistency
//! between subquery positions.

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
// COALESCE with subquery as one of its args.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_coalesce_with_subquery() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL)").unwrap();
    // COALESCE(v, (SELECT MAX(v) FROM t)) — for NULL v, use MAX.
    // MAX=10. id1: v=10 → 10. id2: v=NULL → MAX=10.
    let r = q(&db, "SELECT COALESCE(v, (SELECT MAX(v) FROM t)) FROM t ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(10)], vec![Value::Integer(10)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Nested CASE (CASE inside CASE) both with subqueries.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_nested_case_with_subquery() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,30)").unwrap();
    // Outer CASE: if v > (SELECT MIN(v)+5) then inner CASE using MAX.
    // MIN+5 = 15. id1(10): not >15 → 'low'. id2(30): >15 → inner: v=(SELECT MAX)=30? → 'max'.
    let r = q(&db, "SELECT CASE WHEN v > (SELECT MIN(v) FROM t) + 5 THEN (CASE WHEN v = (SELECT MAX(v) FROM t) THEN 'max' ELSE 'high' END) ELSE 'low' END FROM t ORDER BY id");
    assert_eq!(r, vec![
        vec![Value::Text("low".into())],
        vec![Value::Text("max".into())],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// Subquery in arithmetic with CASE.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_subquery_plus_case() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    // (SELECT MAX(v) FROM t) + (CASE WHEN 1=1 THEN 5 ELSE 0 END) = 10+5=15.
    let r = q(&db, "SELECT (SELECT MAX(v) FROM t) + (CASE WHEN 1 = 1 THEN 5 ELSE 0 END) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(15)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// IN subquery referencing column in outer WHERE (semi-join consistency).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_in_subquery_consistency() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    // v IN (SELECT v FROM t WHERE v > 15) → {20,30}.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v IN (SELECT v FROM t WHERE v > 15)"));
    assert_eq!(r, vec![2, 3]);
}

// ─────────────────────────────────────────────────────────────────────────
// Two IN subqueries combined with AND.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_two_in_subqueries_and() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("CREATE TABLE s1(x INT)").unwrap();
    db.execute("CREATE TABLE s2(y INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    db.execute("INSERT INTO s1 VALUES (10),(30)").unwrap();
    db.execute("INSERT INTO s2 VALUES (10),(20)").unwrap();
    // v IN (s1) AND v IN (s2) → v in {10,30} ∩ {10,20} = {10} → id1.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v IN (SELECT x FROM s1) AND v IN (SELECT y FROM s2)"));
    assert_eq!(r, vec![1]);
}

// ─────────────────────────────────────────────────────────────────────────
// IN subquery combined with regular WHERE condition.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_in_subquery_plus_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT, cat TEXT)").unwrap();
    db.execute("CREATE TABLE s(x INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10,'a'),(2,20,'a'),(3,30,'b')").unwrap();
    db.execute("INSERT INTO s VALUES (10),(20)").unwrap();
    // v IN (s) AND cat = 'a' → id1 (10,a), id2 (20,a). id3 (30,b) excluded by both.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v IN (SELECT x FROM s) AND cat = 'a'"));
    assert_eq!(r, vec![1, 2]);
}

// ─────────────────────────────────────────────────────────────────────────
// Subquery in WHERE compared with <= and the column.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_subquery_le_in_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)").unwrap();
    // v <= (SELECT AVG(v) FROM t). AVG=25 → id1(10), id2(20).
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v <= (SELECT AVG(v) FROM t)"));
    assert_eq!(r, vec![1, 2]);
}

// ─────────────────────────────────────────────────────────────────────────
// Subquery returning NULL compared (NULL > anything = unknown).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_subquery_null_comparison() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("CREATE TABLE empty(x INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    // (SELECT MAX(x) FROM empty) is NULL. v > NULL → unknown → no rows.
    let r = q(&db, "SELECT id FROM t WHERE v > (SELECT MAX(x) FROM empty)");
    assert_eq!(r, Vec::<Vec<Value>>::new());
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate subquery in SELECT, non-correlated, repeated per row.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_aggregate_subquery_per_row() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    // Each row gets the same MAX.
    let r = q(&db, "SELECT id, (SELECT MAX(v) FROM t) FROM t ORDER BY id");
    assert_eq!(r, vec![
        vec![Value::Integer(1), Value::Integer(30)],
        vec![Value::Integer(2), Value::Integer(30)],
        vec![Value::Integer(3), Value::Integer(30)],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// Subquery with arithmetic on aggregate in WHERE.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_arithmetic_on_subquery_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)").unwrap();
    // v > (SELECT SUM(v) FROM t) / 4. SUM=100, /4=25. v>25 → id3,4.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v > (SELECT SUM(v) FROM t) / 4"));
    assert_eq!(r, vec![3, 4]);
}

// ─────────────────────────────────────────────────────────────────────────
// NOT IN subquery then verify excluded.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_not_in_subquery_verify() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("CREATE TABLE s(x INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    db.execute("INSERT INTO s VALUES (20)").unwrap();
    // v NOT IN (s) → id1(10), id3(30). Excludes id2(20).
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v NOT IN (SELECT x FROM s)"));
    assert_eq!(r, vec![1, 3]);
}

// ─────────────────────────────────────────────────────────────────────────
// Subquery in HAVING with COUNT.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_having_subquery_count() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',15),(5,'b',25)").unwrap();
    // Groups where COUNT(*) > (SELECT COUNT(*) FROM t) / 2.
    // Total=5, /2=2.5. a:count=2 (not >2.5). b:count=3 (>2.5).
    let r = q(&db, "SELECT cat FROM t GROUP BY cat HAVING COUNT(*) > (SELECT COUNT(*) FROM t) / 2");
    assert_eq!(r, vec![vec![Value::Text("b".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Subquery with aggregate in ORDER BY DESC.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_subquery_order_by_desc() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,30),(3,20)").unwrap();
    // Order by (MAX - v) DESC. MAX=30. id1:20, id2:0, id3:10. DESC → id1(20), id3(10), id2(0).
    let r = q(&db, "SELECT id FROM t ORDER BY (SELECT MAX(v) FROM t) - v DESC");
    let ids: Vec<i64> = r.iter().filter_map(|row| match row[0] { Value::Integer(i) => Some(i), _ => None }).collect();
    assert_eq!(ids, vec![1, 3, 2]);
}

// ─────────────────────────────────────────────────────────────────────────
// Derived table with aggregate then ORDER BY the aggregate.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_derived_table_order_by_agg() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',15),(5,'c',100)").unwrap();
    let r = q(&db, "SELECT sub.cat, sub.total FROM (SELECT cat, SUM(v) AS total FROM t GROUP BY cat) AS sub ORDER BY sub.total ASC");
    // a:30, b:20, c:100. ASC → b(20), a(30), c(100).
    let cats: Vec<String> = r.iter().filter_map(|row| match &row[0] { Value::Text(s) => Some(s.as_str().to_string()), _ => None }).collect();
    assert_eq!(cats, vec!["b".to_string(), "a".to_string(), "c".to_string()]);
}

// ─────────────────────────────────────────────────────────────────────────
// Subquery in SELECT compared via outer arithmetic.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_subquery_select_arithmetic() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    // v - (SELECT MIN(v) FROM t) per row. MIN=10. id1:0, id2:10, id3:20.
    let r = q(&db, "SELECT v - (SELECT MIN(v) FROM t) FROM t ORDER BY id");
    assert_eq!(r, vec![
        vec![Value::Integer(0)],
        vec![Value::Integer(10)],
        vec![Value::Integer(20)],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// 3-level subquery in HAVING.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_three_level_subquery_having() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'a',30),(4,'b',5)").unwrap();
    // HAVING SUM(v) > (SELECT AVG(v) FROM t WHERE v > (SELECT MIN(v) FROM t))
    // MIN=5. Inner: v>5 → {10,20,30} AVG=20. HAVING SUM > 20.
    // a: SUM=60 >20 ✓. b: SUM=5 ✗.
    let r = q(&db, "SELECT cat FROM t GROUP BY cat HAVING SUM(v) > (SELECT AVG(v) FROM t WHERE v > (SELECT MIN(v) FROM t))");
    assert_eq!(r, vec![vec![Value::Text("a".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Subquery returning 0 rows used in IN (empty set → no matches).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_in_empty_subquery() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("CREATE TABLE s(x INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    // v IN (empty set) → no matches.
    let r = q(&db, "SELECT id FROM t WHERE v IN (SELECT x FROM s WHERE x > 100)");
    assert_eq!(r, Vec::<Vec<Value>>::new());
}

// ─────────────────────────────────────────────────────────────────────────
// NOT IN empty subquery (all rows match).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_not_in_empty_subquery() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("CREATE TABLE s(x INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v NOT IN (SELECT x FROM s WHERE x > 100)"));
    assert_eq!(r, vec![1, 2]);
}

// ─────────────────────────────────────────────────────────────────────────
// Subquery in CASE in ORDER BY.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_case_subquery_in_order_by() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,30)").unwrap();
    // Order by CASE: if v=MAX then 0 else 1. MAX=30. id2(30)→0, id1(10)→1. ASC → id2, id1.
    let r = q(&db, "SELECT id FROM t ORDER BY CASE WHEN v = (SELECT MAX(v) FROM t) THEN 0 ELSE 1 END ASC");
    let ids: Vec<i64> = r.iter().filter_map(|row| match row[0] { Value::Integer(i) => Some(i), _ => None }).collect();
    assert_eq!(ids, vec![2, 1]);
}
