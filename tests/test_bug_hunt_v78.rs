//! Bug Hunt v78 — round 5: type coercion boundaries, COALESCE, CASE edges,
//! Float↔Int, ORDER BY ordinal, self-referential subquery, edge WHERE.

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
// Float ↔ Integer comparison and arithmetic.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_float_int_comparison() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10.5),(2,20.0),(3,30.5)").unwrap();
    // 20.0 == 20 (int literal) should match id=2.
    let r = q(&db, "SELECT id FROM t WHERE v = 20");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_float_int_arithmetic() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = q(&db, "SELECT 10 / 4 FROM t");
    // Integer division → 2 (truncated) or 2.5 (float). Accept either.
    match &r[0][0] {
        Value::Integer(2) => {}
        Value::Float(f) => assert!((f - 2.5).abs() < 1e-9, "10/4 = 2.5, got {}", f),
        other => panic!("10/4 unexpected {:?}", other),
    }
}

#[test]
fn test_float_multiplication() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = q(&db, "SELECT 2.5 * 4 FROM t");
    match &r[0][0] {
        Value::Float(f) => assert!((f - 10.0).abs() < 1e-9, "2.5*4 = 10.0, got {}", f),
        Value::Integer(10) => {}
        other => panic!("2.5*4 unexpected {:?}", other),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// COALESCE.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_coalesce_first_non_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,NULL),(2,10),(3,NULL)").unwrap();
    let r = q(&db, "SELECT COALESCE(v, 0) FROM t ORDER BY id");
    assert_eq!(r, vec![
        vec![Value::Integer(0)],
        vec![Value::Integer(10)],
        vec![Value::Integer(0)],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// CASE expression (searched form).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_searched_case() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,15),(3,25)").unwrap();
    let r = q(&db, "SELECT CASE WHEN v < 10 THEN 'low' WHEN v < 20 THEN 'mid' ELSE 'high' END FROM t ORDER BY id");
    assert_eq!(r, vec![
        vec![Value::Text("low".into())],
        vec![Value::Text("mid".into())],
        vec![Value::Text("high".into())],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY ordinal (1-based column position).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_order_by_ordinal() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,30),(2,10),(3,20)").unwrap();
    // ORDER BY 2 means order by the 2nd column (v).
    let r = q(&db, "SELECT id, v FROM t ORDER BY 2");
    let ids: Vec<i64> = r.iter().filter_map(|row| match row[0] { Value::Integer(i) => Some(i), _ => None }).collect();
    assert_eq!(ids, vec![2, 3, 1]); // v ascending: 10,20,30 → ids 2,3,1
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY alias.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_order_by_alias() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,30),(2,10),(3,20)").unwrap();
    let r = q(&db, "SELECT v AS val FROM t ORDER BY val");
    let vals: Vec<i64> = r.iter().filter_map(|row| match row[0] { Value::Integer(i) => Some(i), _ => None }).collect();
    assert_eq!(vals, vec![10, 20, 30]);
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate MIN/MAX on TEXT (lexicographic).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_min_max_text() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'banana'),(2,'apple'),(3,'cherry')").unwrap();
    let r1 = q(&db, "SELECT MIN(s) FROM t");
    assert_eq!(r1, vec![vec![Value::Text("apple".into())]]);
    let r2 = q(&db, "SELECT MAX(s) FROM t");
    assert_eq!(r2, vec![vec![Value::Text("cherry".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Subquery in WHERE returning NULL handling (NOT IN empty set = all rows).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_not_in_empty_subquery() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    // NOT IN (empty set) → all rows.
    let mut r = q(&db, "SELECT id FROM t WHERE v NOT IN (SELECT v FROM t WHERE v > 100)");
    r.sort_by_key(|row| match row[0] { Value::Integer(i) => i, _ => 999 });
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(2)], vec![Value::Integer(3)]]);
}

#[test]
fn test_in_empty_subquery() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE v IN (SELECT v FROM t WHERE v > 100)");
    assert_eq!(r, Vec::<Vec<Value>>::new());
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE with arithmetic on columns.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_where_arithmetic() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    // 3+7=10, 5+5=10, 2+8=10 (all sum to 10), 1+1=2.
    db.execute("INSERT INTO t VALUES (1,3,7),(2,5,5),(3,2,8),(4,1,1)").unwrap();
    let mut r = q(&db, "SELECT id FROM t WHERE a + b = 10");
    r.sort_by_key(|row| match row[0] { Value::Integer(i) => i, _ => 999 });
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(2)], vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// NULL in arithmetic → NULL (propagation).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_null_arithmetic_propagation() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,NULL,5)").unwrap();
    let r = q(&db, "SELECT a + b FROM t");
    assert_eq!(r, vec![vec![Value::Null]]);
}

// ─────────────────────────────────────────────────────────────────────────
// SELECT with no FROM (constant query).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_select_no_from() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 1 + 1");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_select_string_literal_no_from() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 'hello'");
    assert_eq!(r, vec![vec![Value::Text("hello".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Negative LIMIT (should error or be treated as no limit — not crash).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_limit_zero() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    let r = q(&db, "SELECT id FROM t LIMIT 0");
    assert_eq!(r, Vec::<Vec<Value>>::new());
}

// ─────────────────────────────────────────────────────────────────────────
// GROUP BY with WHERE filtering before aggregation.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_by_with_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',5)").unwrap();
    // WHERE v > 5 excludes rows 3,4 (v=5). Group 'a' sum=30.
    let mut r = q(&db, "SELECT cat, SUM(v) FROM t WHERE v > 5 GROUP BY cat");
    r.sort_by_key(|row| match &row[0] { Value::Text(s) => s.as_str().to_string(), _ => "zzz".into() });
    assert_eq!(r, vec![vec![Value::Text("a".into()), Value::Integer(30)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// INSERT then SELECT preserves exact integer values (no float drift).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_integer_precision_roundtrip() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 1000000),(2, 999999),(3, 123456789)").unwrap();
    let mut r = q(&db, "SELECT v FROM t ORDER BY v");
    r.sort_by_key(|row| match row[0] { Value::Integer(i) => i, _ => 999 });
    assert_eq!(r, vec![
        vec![Value::Integer(999999)],
        vec![Value::Integer(1000000)],
        vec![Value::Integer(123456789)],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// Re-insert after DELETE reuses PK (if not reserved).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_reinsert_after_delete() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    db.execute("DELETE FROM t WHERE id = 1").unwrap();
    // Re-insert id=1 should work (PK freed).
    db.execute("INSERT INTO t VALUES (1,99)").unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(99)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Comparison: TEXT vs TEXT equality.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_text_equality() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'foo'),(2,'bar'),(3,'foo')").unwrap();
    let mut r = q(&db, "SELECT id FROM t WHERE s = 'foo'");
    r.sort_by_key(|row| match row[0] { Value::Integer(i) => i, _ => 999 });
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// LIKE with no wildcard (exact match).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_like_exact_no_wildcard() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'foo'),(2,'foobar'),(3,'bar')").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE s LIKE 'foo'");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Multiple aggregates with GROUP BY.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_multiple_aggregates_group_by() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'a',30),(4,'b',5)").unwrap();
    let mut r = q(&db, "SELECT cat, COUNT(*), SUM(v), MIN(v), MAX(v), AVG(v) FROM t GROUP BY cat");
    r.sort_by_key(|row| match &row[0] { Value::Text(s) => s.as_str().to_string(), _ => "zzz".into() });
    assert_eq!(r.len(), 2);
    // Group 'a': count=3, sum=60, min=10, max=30, avg=20.
    let a_row = &r[0];
    assert_eq!(a_row[1], Value::Integer(3));
    assert_eq!(a_row[2], Value::Integer(60));
    assert_eq!(a_row[3], Value::Integer(10));
    assert_eq!(a_row[4], Value::Integer(30));
    // avg could be int 20 or float 20.0.
    match &a_row[5] {
        Value::Integer(20) => {}
        Value::Float(f) => assert!((f - 20.0).abs() < 1e-9, "avg for group a = 20.0, got {}", f),
        other => panic!("avg for group a should be 20, got {:?}", other),
    }
}
