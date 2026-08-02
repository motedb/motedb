//! Bug Hunt v94 — round 21: CASE-with-aggregate detection, more expression
//! recursion edges, and consistency probing for the fixed paths.

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
// ORDER BY CASE WHEN referencing a column (verify v93 fix holds).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_order_by_case_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,30),(2,10),(3,20)").unwrap();
    // ORDER BY CASE WHEN v > 15 THEN 0 ELSE 1 END — v>15: id1(30)→0, id3(20)→0; id2(10)→1.
    // ASC: 0s first (id1,id3), then id2.
    let r = q(&db, "SELECT id FROM t ORDER BY CASE WHEN v > 15 THEN 0 ELSE 1 END ASC, id ASC");
    let ids: Vec<i64> = r.iter().filter_map(|row| match row[0] { Value::Integer(i) => Some(i), _ => None }).collect();
    assert_eq!(ids, vec![1, 3, 2]);
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY CASE with multiple WHEN referencing different columns.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_order_by_case_multi_col() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,5,0),(2,0,5),(3,5,5)").unwrap();
    // ORDER BY CASE WHEN a > b THEN a WHEN b > a THEN b ELSE 0 END.
    // id1: a=5>b=0 → a=5. id2: b=5>a=0 → b=5. id3: a=b → 0. ASC: id3(0),id1(5),id2(5).
    let r = q(&db, "SELECT id FROM t ORDER BY CASE WHEN a > b THEN a WHEN b > a THEN b ELSE 0 END ASC, id ASC");
    let ids: Vec<i64> = r.iter().filter_map(|row| match row[0] { Value::Integer(i) => Some(i), _ => None }).collect();
    assert_eq!(ids, vec![3, 1, 2]);
}

// ─────────────────────────────────────────────────────────────────────────
// CASE in WHERE referencing column (verify eval).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_case_in_where_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,15),(3,25)").unwrap();
    // WHERE CASE WHEN v > 20 THEN 1 ELSE 0 END = 1 → id3.
    let r = q(&db, "SELECT id FROM t WHERE CASE WHEN v > 20 THEN 1 ELSE 0 END = 1");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// CASE returning column value (not just literal).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_case_returns_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10,20),(2,30,40)").unwrap();
    // CASE WHEN a > b THEN a ELSE b → returns the larger.
    let r = q(&db, "SELECT CASE WHEN a > b THEN a ELSE b END FROM t ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(20)], vec![Value::Integer(40)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Nested CASE in SELECT.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_nested_case_select() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,15),(3,25)").unwrap();
    let r = q(&db, "SELECT CASE WHEN v < 10 THEN 'a' ELSE (CASE WHEN v < 20 THEN 'b' ELSE 'c' END) END FROM t ORDER BY id");
    assert_eq!(r, vec![
        vec![Value::Text("a".into())],
        vec![Value::Text("b".into())],
        vec![Value::Text("c".into())],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// Simple CASE form with column (CASE col WHEN val).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_simple_case_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')").unwrap();
    let r = q(&db, "SELECT CASE cat WHEN 'a' THEN 1 WHEN 'b' THEN 2 ELSE 0 END FROM t ORDER BY id");
    assert_eq!(r, vec![
        vec![Value::Integer(1)],
        vec![Value::Integer(2)],
        vec![Value::Integer(0)],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate over CASE (SUM of CASE) — common pattern, verify.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_sum_of_case_grouped() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',15)").unwrap();
    // SUM(CASE WHEN v > 10 THEN v ELSE 0 END) per cat.
    // a: v=10→0, v=20→20 → 20. b: v=5→0, v=15→15 → 15.
    let mut r = q(&db, "SELECT cat, SUM(CASE WHEN v > 10 THEN v ELSE 0 END) FROM t GROUP BY cat");
    r.sort_by_key(|row| match &row[0] { Value::Text(s) => s.as_str().to_string(), _ => "zzz".into() });
    assert_eq!(r, vec![
        vec![Value::Text("a".into()), Value::Integer(20)],
        vec![Value::Text("b".into()), Value::Integer(15)],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// CASE with NULL in WHEN/THEN.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_case_with_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,NULL),(2,10)").unwrap();
    // CASE WHEN v IS NULL THEN 'null' ELSE 'val' END.
    let r = q(&db, "SELECT CASE WHEN v IS NULL THEN 'null' ELSE 'val' END FROM t ORDER BY id");
    assert_eq!(r, vec![
        vec![Value::Text("null".into())],
        vec![Value::Text("val".into())],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// COALESCE with column + column.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_coalesce_two_columns() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,NULL,5),(2,10,NULL),(3,NULL,NULL)").unwrap();
    let r = q(&db, "SELECT COALESCE(a, b, -1) FROM t ORDER BY id");
    assert_eq!(r, vec![
        vec![Value::Integer(5)],
        vec![Value::Integer(10)],
        vec![Value::Integer(-1)],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY CASE DESC.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_order_by_case_desc() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,25),(3,15)").unwrap();
    // CASE WHEN v > 20 THEN 0 ELSE 1 DESC.
    // v>20: id2(25)→0; id1,id3→1. DESC: 1s first (id1,id3), then id2(0).
    let r = q(&db, "SELECT id FROM t ORDER BY CASE WHEN v > 20 THEN 0 ELSE 1 END DESC, id ASC");
    let ids: Vec<i64> = r.iter().filter_map(|row| match row[0] { Value::Integer(i) => Some(i), _ => None }).collect();
    assert_eq!(ids, vec![1, 3, 2]);
}

// ─────────────────────────────────────────────────────────────────────────
// CASE in arithmetic.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_case_in_arithmetic() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,15)").unwrap();
    // v + CASE WHEN v > 10 THEN 100 ELSE 0 END.
    // id1: 5+0=5. id2: 15+100=115.
    let r = q(&db, "SELECT v + CASE WHEN v > 10 THEN 100 ELSE 0 END FROM t ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(5)], vec![Value::Integer(115)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Multiple CASE in SELECT.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_multiple_case_select() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,5,10)").unwrap();
    let r = q(&db, "SELECT CASE WHEN a > 0 THEN 'pos_a' ELSE 'neg_a' END, CASE WHEN b > 5 THEN 'big_b' ELSE 'small_b' END FROM t");
    assert_eq!(r, vec![vec![Value::Text("pos_a".into()), Value::Text("big_b".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// CASE with all conditions false and no ELSE → NULL.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_case_all_false_no_else() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,5)").unwrap();
    let r = q(&db, "SELECT CASE WHEN v > 100 THEN 'a' WHEN v > 200 THEN 'b' END FROM t");
    assert_eq!(r, vec![vec![Value::Null]]);
}

// ─────────────────────────────────────────────────────────────────────────
// CASE equality on TEXT.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_case_text_equality() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'foo'),(2,'bar')").unwrap();
    let r = q(&db, "SELECT CASE WHEN s = 'foo' THEN 'F' ELSE 'O' END FROM t ORDER BY id");
    assert_eq!(r, vec![vec![Value::Text("F".into())], vec![Value::Text("O".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// CASE with arithmetic in WHEN condition.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_case_arithmetic_in_when() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,3,7),(2,10,5)").unwrap();
    // CASE WHEN a + b > 10 THEN 'big' ELSE 'small'.
    // id1: 10 not>10 → small. id2: 15>10 → big.
    let r = q(&db, "SELECT CASE WHEN a + b > 10 THEN 'big' ELSE 'small' END FROM t ORDER BY id");
    assert_eq!(r, vec![vec![Value::Text("small".into())], vec![Value::Text("big".into())]]);
}
