//! Bug Hunt v77 — round 4: ORDER BY expression, NULL comparisons,
//! INSERT default/missing columns, multi-table JOIN, type coercion in JOIN,
//! COUNT(*) with GROUP BY, string escaping.

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
// ORDER BY an expression (e.g. ORDER BY a + b).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_order_by_expression() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,1,9),(2,5,5),(3,3,3)").unwrap();
    // a+b: id1=10, id2=10, id3=6. ORDER BY a+b ASC.
    let r = q(&db, "SELECT id FROM t ORDER BY a + b ASC, id ASC");
    let ids: Vec<i64> = r.iter().filter_map(|row| match row[0] { Value::Integer(i) => Some(i), _ => None }).collect();
    // id3 (6) first, then id1, id2 (both 10, tie broken by id ASC).
    assert_eq!(ids, vec![3, 1, 2]);
}

// ─────────────────────────────────────────────────────────────────────────
// NULL comparisons: NULL = NULL is NULL (not TRUE).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_null_equals_null_is_unknown() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,NULL),(2,10)").unwrap();
    // WHERE v = NULL must match NOTHING (use IS NULL instead).
    let r = q(&db, "SELECT id FROM t WHERE v = NULL");
    assert_eq!(r, Vec::<Vec<Value>>::new());
}

#[test]
fn test_is_null_matches() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,NULL),(2,10),(3,NULL)").unwrap();
    let mut r = q(&db, "SELECT id FROM t WHERE v IS NULL");
    r.sort_by_key(|row| match row[0] { Value::Integer(i) => i, _ => 999 });
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

#[test]
fn test_is_not_null_matches() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,NULL),(2,10),(3,NULL)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE v IS NOT NULL");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// INSERT with fewer columns (others default to NULL).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_insert_partial_columns() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t (id, a) VALUES (1, 10)").unwrap();
    let r = q(&db, "SELECT id, a, b FROM t");
    assert_eq!(r, vec![vec![Value::Integer(1), Value::Integer(10), Value::Null]]);
}

#[test]
fn test_insert_named_columns_reordered() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    // Insert b before a.
    db.execute("INSERT INTO t (id, b, a) VALUES (1, 20, 10)").unwrap();
    let r = q(&db, "SELECT a, b FROM t");
    assert_eq!(r, vec![vec![Value::Integer(10), Value::Integer(20)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Three-table JOIN.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_three_table_join() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(id INT PRIMARY KEY, bid INT)").unwrap();
    db.execute("CREATE TABLE b(id INT PRIMARY KEY, cid INT)").unwrap();
    db.execute("CREATE TABLE c(id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO a VALUES (1,10)").unwrap();
    db.execute("INSERT INTO b VALUES (10,100)").unwrap();
    db.execute("INSERT INTO c VALUES (100,'leaf')").unwrap();
    let r = q(&db, "SELECT c.name FROM a JOIN b ON a.bid = b.id JOIN c ON b.cid = c.id");
    assert_eq!(r, vec![vec![Value::Text("leaf".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Type coercion in JOIN ON (INT vs TEXT numeric).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_join_same_type() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(id INT PRIMARY KEY, k INT)").unwrap();
    db.execute("CREATE TABLE b(id INT PRIMARY KEY, k INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1,5)").unwrap();
    db.execute("INSERT INTO b VALUES (1,5)").unwrap();
    let r = q(&db, "SELECT a.id FROM a JOIN b ON a.k = b.k");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// LEFT JOIN (if supported) — rows with no match get NULLs.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_left_join_null_fill() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(id INT PRIMARY KEY, bid INT)").unwrap();
    db.execute("CREATE TABLE b(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1,10),(2,20)").unwrap();
    db.execute("INSERT INTO b VALUES (10,99)").unwrap();
    // a.id=2 has bid=20 which doesn't match any b → b.v should be NULL.
    let r = q(&db, "SELECT a.id, b.v FROM a LEFT JOIN b ON a.bid = b.id ORDER BY a.id");
    assert_eq!(r, vec![
        vec![Value::Integer(1), Value::Integer(99)],
        vec![Value::Integer(2), Value::Null],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// COUNT(*) with GROUP BY.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_count_star_group_by() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'a'),(3,'a'),(4,'b'),(5,'b')").unwrap();
    let mut r = q(&db, "SELECT cat, COUNT(*) FROM t GROUP BY cat");
    r.sort_by_key(|row| match &row[0] { Value::Text(s) => s.as_str().to_string(), _ => "zzz".into() });
    assert_eq!(r, vec![
        vec![Value::Text("a".into()), Value::Integer(3)],
        vec![Value::Text("b".into()), Value::Integer(2)],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// String with quotes (escaping via doubled quote).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_string_with_quote() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'it''s')").unwrap();
    let r = q(&db, "SELECT s FROM t");
    assert_eq!(r, vec![vec![Value::Text("it's".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Empty string vs NULL.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_empty_string_is_not_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'')").unwrap();
    let r1 = q(&db, "SELECT s FROM t");
    assert_eq!(r1, vec![vec![Value::Text("".into())]]);
    // Empty string is NOT NULL.
    let r2 = q(&db, "SELECT id FROM t WHERE s IS NULL");
    assert_eq!(r2, Vec::<Vec<Value>>::new());
    let r3 = q(&db, "SELECT id FROM t WHERE s IS NOT NULL");
    assert_eq!(r3, vec![vec![Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// UPDATE SET multiple columns.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_update_multiple_columns() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10,20)").unwrap();
    db.execute("UPDATE t SET a = 100, b = 200 WHERE id = 1").unwrap();
    let r = q(&db, "SELECT a, b FROM t");
    assert_eq!(r, vec![vec![Value::Integer(100), Value::Integer(200)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// DELETE with WHERE.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_delete_with_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    db.execute("DELETE FROM t WHERE v > 15").unwrap();
    let mut r = q(&db, "SELECT v FROM t");
    r.sort_by_key(|row| match row[0] { Value::Integer(i) => i, _ => 999 });
    assert_eq!(r, vec![vec![Value::Integer(10)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// DISTINCT on multiple columns.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_distinct_multi_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,1,1),(2,1,1),(3,1,2),(4,2,1)").unwrap();
    let mut r = q(&db, "SELECT DISTINCT a, b FROM t");
    r.sort_by_key(|row| (match row[0] { Value::Integer(i) => i, _ => 999 }, match row[1] { Value::Integer(i) => i, _ => 999 }));
    assert_eq!(r, vec![
        vec![Value::Integer(1), Value::Integer(1)],
        vec![Value::Integer(1), Value::Integer(2)],
        vec![Value::Integer(2), Value::Integer(1)],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate without GROUP BY over filtered set.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_aggregate_with_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)").unwrap();
    let r = q(&db, "SELECT SUM(v), COUNT(*), MIN(v), MAX(v) FROM t WHERE v > 15");
    // Rows 2,3,4: sum=90, count=3, min=20, max=40.
    assert_eq!(r, vec![vec![Value::Integer(90), Value::Integer(3), Value::Integer(20), Value::Integer(40)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Comparison operators chain via AND (range check equivalent).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_chained_comparison_via_and() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,10),(3,15),(4,20)").unwrap();
    let mut r = q(&db, "SELECT id FROM t WHERE v >= 10 AND v <= 20");
    r.sort_by_key(|row| match row[0] { Value::Integer(i) => i, _ => 999 });
    assert_eq!(r, vec![vec![Value::Integer(2)], vec![Value::Integer(3)], vec![Value::Integer(4)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// SELECT with table alias and qualified columns.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_table_alias_qualified() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    let r = q(&db, "SELECT x.id, x.v FROM t x");
    assert_eq!(r, vec![vec![Value::Integer(1), Value::Integer(10)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Whitespace / newline tolerance in SQL.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_multiline_sql() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    let r = q(&db, "SELECT id\n  FROM t\n  WHERE v = 10");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Boolean literal TRUE/FALSE.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_boolean_literal() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = q(&db, "SELECT TRUE FROM t");
    // TRUE should evaluate to truthy (Bool(true) or Integer(1)).
    assert_eq!(r.len(), 1);
    match &r[0][0] {
        Value::Bool(true) => {}
        Value::Integer(1) => {}
        other => panic!("TRUE expected, got {:?}", other),
    }
}
