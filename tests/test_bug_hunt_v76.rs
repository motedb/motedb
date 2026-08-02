//! Bug Hunt v76 — round 3: aggregates with NULL, string functions,
//! self-join, HAVING with expression, nested subqueries, edge numerics.

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
// MIN/MAX with NULLs: NULLs are ignored.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_min_ignores_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,NULL),(3,10)").unwrap();
    let r = q(&db, "SELECT MIN(v) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(5)]]);
}

#[test]
fn test_max_ignores_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,NULL),(3,10)").unwrap();
    let r = q(&db, "SELECT MAX(v) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(10)]]);
}

#[test]
fn test_avg_with_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL),(3,20)").unwrap();
    let r = q(&db, "SELECT AVG(v) FROM t");
    // AVG = (10+20)/2 = 15.0 (NULL ignored, divisor is non-null count = 2).
    assert_eq!(r.len(), 1);
    match &r[0][0] {
        Value::Float(f) => assert!((f - 15.0).abs() < 1e-9, "AVG = 15.0, got {}", f),
        Value::Integer(i) => assert_eq!(*i, 15, "AVG = 15, got {}", i),
        other => panic!("AVG expected 15, got {:?}", other),
    }
}

#[test]
fn test_avg_all_null_is_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,NULL),(2,NULL)").unwrap();
    let r = q(&db, "SELECT AVG(v) FROM t");
    assert_eq!(r, vec![vec![Value::Null]]);
}

// ─────────────────────────────────────────────────────────────────────────
// String functions.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_length_function() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'hello')").unwrap();
    let r = q(&db, "SELECT LENGTH(s) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(5)]]);
}

#[test]
fn test_lower_upper() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'HeLLo')").unwrap();
    let r1 = q(&db, "SELECT LOWER(s) FROM t");
    assert_eq!(r1, vec![vec![Value::Text("hello".into())]]);
    let r2 = q(&db, "SELECT UPPER(s) FROM t");
    assert_eq!(r2, vec![vec![Value::Text("HELLO".into())]]);
}

#[test]
fn test_trim_function() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'  hi  ')").unwrap();
    let r = q(&db, "SELECT TRIM(s) FROM t");
    assert_eq!(r, vec![vec![Value::Text("hi".into())]]);
}

#[test]
fn test_concat_function() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = q(&db, "SELECT 'foo' || 'bar' FROM t");
    assert_eq!(r, vec![vec![Value::Text("foobar".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Self-join.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_self_join() {
    let (db, _d) = db();
    db.execute("CREATE TABLE emp(id INT PRIMARY KEY, name TEXT, mgr INT)").unwrap();
    // mgr references another emp.id.
    db.execute("INSERT INTO emp VALUES (1,'alice',NULL),(2,'bob',1),(3,'carol',1)").unwrap();
    let r = q(&db, "SELECT e.name, m.name FROM emp e JOIN emp m ON e.mgr = m.id");
    let mut got: Vec<(String, String)> = r.iter().filter_map(|row| {
        match (&row[0], &row[1]) {
            (Value::Text(a), Value::Text(b)) => Some((a.as_str().to_string(), b.as_str().to_string())),
            _ => None,
        }
    }).collect();
    got.sort();
    assert_eq!(got, vec![
        ("bob".to_string(), "alice".to_string()),
        ("carol".to_string(), "alice".to_string()),
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// HAVING with aggregate expression.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_having_count_gt() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'a'),(3,'b'),(4,'c'),(5,'c'),(6,'c')").unwrap();
    let mut r = q(&db, "SELECT cat, COUNT(*) FROM t GROUP BY cat HAVING COUNT(*) > 2");
    r.sort_by_key(|row| match &row[0] { Value::Text(s) => s.as_str().to_string(), _ => "zzz".into() });
    // Only 'c' has count 3.
    assert_eq!(r, vec![vec![Value::Text("c".into()), Value::Integer(3)]]);
}

#[test]
fn test_having_sum_filter() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, amt INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',5)").unwrap();
    let mut r = q(&db, "SELECT cat, SUM(amt) FROM t GROUP BY cat HAVING SUM(amt) > 10");
    r.sort_by_key(|row| match &row[0] { Value::Text(s) => s.as_str().to_string(), _ => "zzz".into() });
    // 'a' sums to 30 (>10), 'b' sums to 10 (not > 10).
    assert_eq!(r, vec![vec![Value::Text("a".into()), Value::Integer(30)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Nested subquery (subquery inside subquery).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_nested_subquery() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    // id IN (SELECT id FROM t WHERE v > (SELECT MIN(v) FROM t))
    // MIN(v)=10, so inner set = {2,3}. Result ids in {2,3}.
    let mut r = q(&db, "SELECT id FROM t WHERE id IN (SELECT id FROM t WHERE v > (SELECT MIN(v) FROM t))");
    r.sort_by_key(|row| match row[0] { Value::Integer(i) => i, _ => 999 });
    assert_eq!(r, vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// EXISTS subquery. NOTE: the engine does not yet parse EXISTS — it raises a
// clear ParseError rather than returning wrong results, so this documents a
// feature gap. We instead verify the equivalent semi-join via IN (subquery),
// which IS supported.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_semi_join_via_in_subquery() {
    let (db, _d) = db();
    db.execute("CREATE TABLE orders(id INT PRIMARY KEY, cust_id INT)").unwrap();
    db.execute("CREATE TABLE customers(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO customers VALUES (1),(2)").unwrap();
    db.execute("INSERT INTO orders VALUES (10,1)").unwrap();
    // Customer 1 has an order, customer 2 does not.
    let mut r = q(&db, "SELECT id FROM customers WHERE id IN (SELECT cust_id FROM orders)");
    r.sort_by_key(|row| match row[0] { Value::Integer(i) => i, _ => 999 });
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Numeric edge: large integers, overflow guard.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_large_integer() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (9223372036854775807)").unwrap(); // i64::MAX
    let r = q(&db, "SELECT id FROM t");
    assert_eq!(r, vec![vec![Value::Integer(9223372036854775807)]]);
}

#[test]
fn test_integer_addition() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = q(&db, "SELECT 2 + 3 FROM t");
    assert_eq!(r, vec![vec![Value::Integer(5)]]);
}

#[test]
fn test_modulo_operator() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = q(&db, "SELECT 17 % 5 FROM t");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// UPDATE all rows (no WHERE).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_update_all_rows() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    db.execute("UPDATE t SET v = 0").unwrap();
    let mut r = q(&db, "SELECT v FROM t");
    r.sort_by_key(|row| match row[0] { Value::Integer(i) => i, _ => 999 });
    assert_eq!(r, vec![vec![Value::Integer(0)], vec![Value::Integer(0)], vec![Value::Integer(0)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// DELETE all rows.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_delete_all_rows() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    db.execute("DELETE FROM t").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(0)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY multiple columns with mixed ASC/DESC.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_order_by_multi_key_mixed() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,1,1),(2,1,2),(3,2,1),(4,2,2)").unwrap();
    // ORDER BY a ASC, b DESC.
    let r = q(&db, "SELECT id FROM t ORDER BY a ASC, b DESC");
    let ids: Vec<i64> = r.iter().filter_map(|row| match row[0] { Value::Integer(i) => Some(i), _ => None }).collect();
    // a=1: b DESC → id=2 (b=2), id=1 (b=1). a=2: b DESC → id=4, id=3.
    assert_eq!(ids, vec![2, 1, 4, 3]);
}

// ─────────────────────────────────────────────────────────────────────────
// LIMIT with OFFSET.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_limit_offset() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3),(4),(5)").unwrap();
    let r = q(&db, "SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 1");
    let ids: Vec<i64> = r.iter().filter_map(|row| match row[0] { Value::Integer(i) => Some(i), _ => None }).collect();
    assert_eq!(ids, vec![2, 3]);
}

#[test]
fn test_offset_beyond_rows() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2)").unwrap();
    let r = q(&db, "SELECT id FROM t ORDER BY id LIMIT 10 OFFSET 100");
    assert_eq!(r, Vec::<Vec<Value>>::new(), "OFFSET beyond row count must return empty");
}

// ─────────────────────────────────────────────────────────────────────────
// Boolean expression in WHERE with AND/OR precedence.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_and_or_precedence() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    // a=1 OR a=2 AND b=3  ==  a=1 OR (a=2 AND b=3)
    db.execute("INSERT INTO t VALUES (1,1,1),(2,2,3),(3,2,1),(4,1,3)").unwrap();
    let mut r = q(&db, "SELECT id FROM t WHERE a = 1 OR a = 2 AND b = 3");
    r.sort_by_key(|row| match row[0] { Value::Integer(i) => i, _ => 999 });
    // a=1 → id 1,4. (a=2 AND b=3) → id 2. So {1,2,4}.
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(2)], vec![Value::Integer(4)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// COUNT(DISTINCT col).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_count_distinct() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,10),(3,20),(4,20),(5,30)").unwrap();
    let r = q(&db, "SELECT COUNT(DISTINCT v) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// GROUP BY with aggregate and ORDER BY aggregate.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_by_order_by_aggregate() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',15)").unwrap();
    let r = q(&db, "SELECT cat, SUM(v) FROM t GROUP BY cat ORDER BY SUM(v) DESC");
    // 'a' sum=30, 'b' sum=20. DESC → a, b.
    assert_eq!(r, vec![
        vec![Value::Text("a".into()), Value::Integer(30)],
        vec![Value::Text("b".into()), Value::Integer(20)],
    ]);
}
