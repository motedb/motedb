//! Bug Hunt v80 — round 7: cross-path aggregate consistency, ORDER BY edges,
//! LIMIT/OFFSET interactions, type coercion in JOIN ON, subquery scalar
//! edges, GROUP_CONCAT, nested aggregates.

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
// ORDER BY with LIMIT — must sort BEFORE limiting.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_order_by_then_limit() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,30),(2,10),(3,50),(4,20),(5,40)").unwrap();
    // ORDER BY v DESC LIMIT 2 → 50, 40.
    let r = q(&db, "SELECT v FROM t ORDER BY v DESC LIMIT 2");
    let vals: Vec<i64> = r.iter().filter_map(|row| match row[0] { Value::Integer(i) => Some(i), _ => None }).collect();
    assert_eq!(vals, vec![50, 40]);
}

#[test]
fn test_order_by_then_offset_limit() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,30),(2,10),(3,50),(4,20),(5,40)").unwrap();
    // ORDER BY v ASC OFFSET 1 LIMIT 2 → skip 10, take 20,30.
    let r = q(&db, "SELECT v FROM t ORDER BY v ASC LIMIT 2 OFFSET 1");
    let vals: Vec<i64> = r.iter().filter_map(|row| match row[0] { Value::Integer(i) => Some(i), _ => None }).collect();
    assert_eq!(vals, vec![20, 30]);
}

// ─────────────────────────────────────────────────────────────────────────
// DISTINCT then LIMIT — dedup before limiting.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_distinct_then_limit() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,10),(3,20),(4,20),(5,30)").unwrap();
    let r = q(&db, "SELECT DISTINCT v FROM t ORDER BY v LIMIT 2");
    let vals: Vec<i64> = r.iter().filter_map(|row| match row[0] { Value::Integer(i) => Some(i), _ => None }).collect();
    assert_eq!(vals, vec![10, 20]);
}

// ─────────────────────────────────────────────────────────────────────────
// GROUP BY then LIMIT (limit applies to groups).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_by_then_limit() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')").unwrap();
    let r = q(&db, "SELECT cat FROM t GROUP BY cat ORDER BY cat LIMIT 2");
    let cats: Vec<String> = r.iter().filter_map(|row| match &row[0] { Value::Text(s) => Some(s.as_str().to_string()), _ => None }).collect();
    assert_eq!(cats, vec!["a".to_string(), "b".to_string()]);
}

// ─────────────────────────────────────────────────────────────────────────
// Scalar subquery returning no rows → NULL.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_scalar_subquery_empty() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE empty(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = q(&db, "SELECT (SELECT MAX(v) FROM empty) FROM t");
    assert_eq!(r, vec![vec![Value::Null]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate of expression.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_sum_of_expression() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10,1),(2,20,2),(3,30,3)").unwrap();
    // SUM(a + b): 11 + 22 + 33 = 66.
    let r = q(&db, "SELECT SUM(a + b) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(66)]]);
}

#[test]
fn test_count_of_expression() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL),(3,30)").unwrap();
    // COUNT(a + 1) ignores NULL a.
    let r = q(&db, "SELECT COUNT(a + 1) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// JOIN with WHERE filtering both tables.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_join_with_where_both() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(id INT PRIMARY KEY, x INT)").unwrap();
    db.execute("CREATE TABLE b(id INT PRIMARY KEY, y INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1,10),(2,20)").unwrap();
    db.execute("INSERT INTO b VALUES (1,100),(2,200)").unwrap();
    let r = q(&db, "SELECT a.x, b.y FROM a JOIN b ON a.id = b.id WHERE a.x > 15");
    assert_eq!(r, vec![vec![Value::Integer(20), Value::Integer(200)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE clause referencing columns not in SELECT.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_where_col_not_in_select() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10,100),(2,20,200)").unwrap();
    // SELECT a, but filter on b.
    let r = q(&db, "SELECT a FROM t WHERE b > 150");
    assert_eq!(r, vec![vec![Value::Integer(20)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY column not in SELECT.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_order_by_col_not_in_select() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10,3),(2,20,1),(3,30,2)").unwrap();
    // SELECT a, ORDER BY b.
    let r = q(&db, "SELECT a FROM t ORDER BY b ASC");
    let vals: Vec<i64> = r.iter().filter_map(|row| match row[0] { Value::Integer(i) => Some(i), _ => None }).collect();
    // b order: 1(id2),2(id3),3(id1) → a: 20,30,10.
    assert_eq!(vals, vec![20, 30, 10]);
}

// ─────────────────────────────────────────────────────────────────────────
// HAVING without column in SELECT (aggregate in HAVING only).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_having_aggregate_not_selected() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5)").unwrap();
    // SELECT cat, but HAVING on SUM(v).
    let r = q(&db, "SELECT cat FROM t GROUP BY cat HAVING SUM(v) > 10");
    // 'a' sum=30 > 10, 'b' sum=5 not.
    assert_eq!(r, vec![vec![Value::Text("a".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Empty table aggregate.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_empty_table_aggregates() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    assert_eq!(q(&db, "SELECT COUNT(*) FROM t"), vec![vec![Value::Integer(0)]]);
    assert_eq!(q(&db, "SELECT COUNT(v) FROM t"), vec![vec![Value::Integer(0)]]);
    assert_eq!(q(&db, "SELECT SUM(v) FROM t"), vec![vec![Value::Null]]);
    assert_eq!(q(&db, "SELECT MIN(v) FROM t"), vec![vec![Value::Null]]);
    assert_eq!(q(&db, "SELECT MAX(v) FROM t"), vec![vec![Value::Null]]);
}

// ─────────────────────────────────────────────────────────────────────────
// GROUP BY producing empty result when WHERE filters all.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_by_all_filtered() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10)").unwrap();
    let r = q(&db, "SELECT cat, COUNT(*) FROM t WHERE v > 100 GROUP BY cat");
    assert_eq!(r, Vec::<Vec<Value>>::new());
}

// ─────────────────────────────────────────────────────────────────────────
// Float aggregate precision.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_float_sum_precision() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,0.1),(2,0.2)").unwrap();
    let r = q(&db, "SELECT SUM(v) FROM t");
    match &r[0][0] {
        Value::Float(f) => assert!((f - 0.3).abs() < 1e-9, "0.1+0.2 = 0.3, got {}", f),
        other => panic!("SUM float unexpected {:?}", other),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Comparison operators: <= and >= boundaries.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_le_ge_boundaries() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    let mut r1 = q(&db, "SELECT id FROM t WHERE v <= 20");
    r1.sort_by_key(|row| match row[0] { Value::Integer(i) => i, _ => 999 });
    assert_eq!(r1, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
    let mut r2 = q(&db, "SELECT id FROM t WHERE v >= 20");
    r2.sort_by_key(|row| match row[0] { Value::Integer(i) => i, _ => 999 });
    assert_eq!(r2, vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Text comparison: < > ordering.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_text_less_than() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'apple'),(2,'banana'),(3,'apricot')").unwrap();
    let mut r = q(&db, "SELECT s FROM t WHERE s < 'banana'");
    r.sort_by_key(|row| match &row[0] { Value::Text(s) => s.as_str().to_string(), _ => "zzz".into() });
    assert_eq!(r, vec![vec![Value::Text("apple".into())], vec![Value::Text("apricot".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// SELECT with duplicate column names.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_select_duplicate_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    let r = q(&db, "SELECT v, v FROM t");
    assert_eq!(r, vec![vec![Value::Integer(10), Value::Integer(10)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE with column on both sides (cross-column comparison).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_where_cross_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10,20),(2,30,30),(3,40,10)").unwrap();
    let mut r = q(&db, "SELECT id FROM t WHERE a < b");
    r.sort_by_key(|row| match row[0] { Value::Integer(i) => i, _ => 999 });
    // id1: 10<20 ✓, id2: 30<30 ✗, id3: 40<10 ✗.
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// INSERT with explicit NULL value.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_insert_explicit_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    let r = q(&db, "SELECT v FROM t");
    assert_eq!(r, vec![vec![Value::Null]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Multiple INSERTs accumulate.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_multiple_inserts() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    db.execute("INSERT INTO t VALUES (3)").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// UPDATE setting column to NULL.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_update_set_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    db.execute("UPDATE t SET v = NULL WHERE id = 1").unwrap();
    let r = q(&db, "SELECT v FROM t");
    assert_eq!(r, vec![vec![Value::Null]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Subquery in WHERE comparing to outer column (correlated).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_correlated_subquery_gt() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',15)").unwrap();
    // Find rows where v > average v in same cat.
    // 'a' avg=15 → id2 (20>15). 'b' avg=10 → id4 (15>10).
    let mut r = q(&db, "SELECT id FROM t t1 WHERE v > (SELECT AVG(v) FROM t t2 WHERE t2.cat = t1.cat)");
    r.sort_by_key(|row| match row[0] { Value::Integer(i) => i, _ => 999 });
    assert_eq!(r, vec![vec![Value::Integer(2)], vec![Value::Integer(4)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// IN with subquery matching PK.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_in_subquery_pk() {
    let (db, _d) = db();
    db.execute("CREATE TABLE parent(id INT PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE child(id INT PRIMARY KEY, pid INT)").unwrap();
    db.execute("INSERT INTO parent VALUES (1),(2),(3)").unwrap();
    db.execute("INSERT INTO child VALUES (10,1),(20,2),(30,99)").unwrap();
    // Children whose pid exists in parent.
    let mut r = q(&db, "SELECT id FROM child WHERE pid IN (SELECT id FROM parent)");
    r.sort_by_key(|row| match row[0] { Value::Integer(i) => i, _ => 999 });
    assert_eq!(r, vec![vec![Value::Integer(10)], vec![Value::Integer(20)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Nested arithmetic with precedence.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_arithmetic_precedence() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    // 2 + 3 * 4 = 14 (mult first).
    let r = q(&db, "SELECT 2 + 3 * 4 FROM t");
    assert_eq!(r, vec![vec![Value::Integer(14)]]);
}

#[test]
fn test_arithmetic_parens() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    // (2 + 3) * 4 = 20.
    let r = q(&db, "SELECT (2 + 3) * 4 FROM t");
    assert_eq!(r, vec![vec![Value::Integer(20)]]);
}
