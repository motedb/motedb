//! Bug Hunt v82 — round 9: DISTINCT aggregate combos, NULL + string funcs,
//! cross-type comparison edges, COALESCE chains, nested arithmetic,
//! IN with subquery + NULL, GROUP BY with expression in SELECT.

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
// SUM(DISTINCT col) — dedup before summing.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_sum_distinct() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,10),(3,20),(4,20),(5,30)").unwrap();
    // Distinct values: 10, 20, 30. Sum = 60.
    let r = q(&db, "SELECT SUM(DISTINCT v) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(60)]]);
}

#[test]
fn test_min_max_distinct() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,10),(3,30),(4,30)").unwrap();
    // MIN/MAX DISTINCT same as MIN/MAX (dedup doesn't change extremes).
    assert_eq!(q(&db, "SELECT MIN(DISTINCT v) FROM t"), vec![vec![Value::Integer(10)]]);
    assert_eq!(q(&db, "SELECT MAX(DISTINCT v) FROM t"), vec![vec![Value::Integer(30)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// COUNT(DISTINCT) with all duplicates.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_count_distinct_all_dup() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,5),(3,5)").unwrap();
    let r = q(&db, "SELECT COUNT(DISTINCT v) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// String functions on NULL input → NULL.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_length_of_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,NULL)").unwrap();
    let r = q(&db, "SELECT LENGTH(s) FROM t");
    assert_eq!(r, vec![vec![Value::Null]]);
}

#[test]
fn test_upper_of_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,NULL)").unwrap();
    let r = q(&db, "SELECT UPPER(s) FROM t");
    assert_eq!(r, vec![vec![Value::Null]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Cross-type comparison: INT column vs FLOAT literal.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_int_col_float_literal() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    // v = 10.0 should match id=1 (int 10 == float 10.0).
    let r = q(&db, "SELECT id FROM t WHERE v = 10.0");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_int_col_float_inequality() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    // v > 15.5 → id 2,3.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v > 15.5"));
    assert_eq!(r, vec![2, 3]);
}

// ─────────────────────────────────────────────────────────────────────────
// COALESCE returning first non-NULL from columns.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_coalesce_columns() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,NULL,20),(2,10,NULL),(3,NULL,NULL)").unwrap();
    let r = q(&db, "SELECT COALESCE(a, b) FROM t ORDER BY id");
    assert_eq!(r, vec![
        vec![Value::Integer(20)],
        vec![Value::Integer(10)],
        vec![Value::Null],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// Nested arithmetic with division and modulo.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_nested_arith_div_mod() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    // (10 + 5) / 3 = 5 (int div) ; 17 % 5 = 2.
    let r1 = q(&db, "SELECT (10 + 5) / 3 FROM t");
    match &r1[0][0] {
        Value::Integer(5) => {}
        Value::Float(f) => assert!((f - 5.0).abs() < 1e-9),
        other => panic!("(10+5)/3 expected 5, got {:?}", other),
    }
    let r2 = q(&db, "SELECT 17 % 5 FROM t");
    assert_eq!(r2, vec![vec![Value::Integer(2)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// IN with subquery that includes NULL (NULL in the set).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_in_subquery_with_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    db.execute("CREATE TABLE s(id INT PRIMARY KEY, x INT)").unwrap();
    db.execute("INSERT INTO s VALUES (1,10),(2,NULL)").unwrap();
    // v IN (SELECT x FROM s) where set = {10, NULL}.
    // v=10 matches 10 → TRUE. v=20,30 don't match 10, and NULL makes it UNKNOWN.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v IN (SELECT x FROM s)"));
    assert_eq!(r, vec![1], "only v=10 matches the non-NULL member");
}

// ─────────────────────────────────────────────────────────────────────────
// GROUP BY with computed SELECT expression referencing grouped column.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_by_with_computed_select() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5)").unwrap();
    // SELECT cat, SUM(v) * 2 FROM t GROUP BY cat.
    let mut r = q(&db, "SELECT cat, SUM(v) * 2 FROM t GROUP BY cat");
    r.sort_by_key(|row| match &row[0] { Value::Text(s) => s.as_str().to_string(), _ => "zzz".into() });
    // 'a' sum=30 (*2=60), 'b' sum=5 (*2=10).
    assert_eq!(r, vec![
        vec![Value::Text("a".into()), Value::Integer(60)],
        vec![Value::Text("b".into()), Value::Integer(10)],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY on aggregate result via alias.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_order_by_aggregate_alias() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',50),(4,'b',5)").unwrap();
    let r = q(&db, "SELECT cat, SUM(v) AS total FROM t GROUP BY cat ORDER BY total ASC");
    // 'a' sum=30, 'b' sum=55. ASC → a, b.
    assert_eq!(r, vec![
        vec![Value::Text("a".into()), Value::Integer(30)],
        vec![Value::Text("b".into()), Value::Integer(55)],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// Multiple aggregates with GROUP BY including COUNT.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_by_multi_agg_with_count() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',NULL),(3,'a',30),(4,'b',5)").unwrap();
    let mut r = q(&db, "SELECT cat, COUNT(*), COUNT(v), SUM(v) FROM t GROUP BY cat");
    r.sort_by_key(|row| match &row[0] { Value::Text(s) => s.as_str().to_string(), _ => "zzz".into() });
    // 'a': COUNT(*)=3, COUNT(v)=2 (NULL excluded), SUM(v)=40.
    // 'b': COUNT(*)=1, COUNT(v)=1, SUM(v)=5.
    assert_eq!(r, vec![
        vec![Value::Text("a".into()), Value::Integer(3), Value::Integer(2), Value::Integer(40)],
        vec![Value::Text("b".into()), Value::Integer(1), Value::Integer(1), Value::Integer(5)],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE with OR across different columns.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_where_or_different_cols() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10,0),(2,0,20),(3,30,30),(4,0,0)").unwrap();
    // a > 5 OR b > 5 → id1 (a=10), id2 (b=20), id3 (both). id4 neither.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE a > 5 OR b > 5"));
    assert_eq!(r, vec![1, 2, 3]);
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE with NOT.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_where_not() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    // NOT (v = 20) → id 1,3 (v=20 excluded; NULL semantics don't apply, no NULLs).
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE NOT (v = 20)"));
    assert_eq!(r, vec![1, 3]);
}

#[test]
fn test_where_not_with_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL),(3,30)").unwrap();
    // NOT (v = 10): v=10→FALSE→NOT→TRUE; v=NULL→NULL→NOT→NULL(excluded); v=30→TRUE.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE NOT (v = 10)"));
    assert_eq!(r, vec![3], "NULL row excluded (NOT NULL = NULL)");
}

// ─────────────────────────────────────────────────────────────────────────
// LIKE with % at start only.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_like_suffix() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'report.pdf'),(2,'data.csv'),(3,'image.pdf')").unwrap();
    let mut r = q(&db, "SELECT s FROM t WHERE s LIKE '%.pdf'");
    r.sort_by_key(|row| match &row[0] { Value::Text(s) => s.as_str().to_string(), _ => "zzz".into() });
    assert_eq!(r, vec![
        vec![Value::Text("image.pdf".into())],
        vec![Value::Text("report.pdf".into())],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// INSERT with DEFAULT-less partial then SELECT.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_partial_insert_then_select_all() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT, c INT)").unwrap();
    db.execute("INSERT INTO t (id, c) VALUES (1, 30)").unwrap();
    let r = q(&db, "SELECT id, a, b, c FROM t");
    assert_eq!(r, vec![vec![Value::Integer(1), Value::Null, Value::Null, Value::Integer(30)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// UPDATE WHERE with AND on multiple columns.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_update_where_and() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10,100),(2,10,200),(3,20,100)").unwrap();
    db.execute("UPDATE t SET a = 99 WHERE a = 10 AND b = 200").unwrap();
    let r = q(&db, "SELECT id, a FROM t ORDER BY id");
    assert_eq!(r, vec![
        vec![Value::Integer(1), Value::Integer(10)],
        vec![Value::Integer(2), Value::Integer(99)],
        vec![Value::Integer(3), Value::Integer(20)],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// DELETE with complex WHERE.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_delete_complex_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10,5),(2,20,15),(3,30,25)").unwrap();
    // Delete where a > 15 AND b < 30 → id2 (20,15), id3 (30,25).
    db.execute("DELETE FROM t WHERE a > 15 AND b < 30").unwrap();
    let r = q(&db, "SELECT id FROM t");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Self-comparison via != (not equal) with NULL.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_not_equal_self_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL),(3,30)").unwrap();
    // v != v: non-NULL → FALSE; NULL → NULL. So nothing matches.
    let r = q(&db, "SELECT id FROM t WHERE v != v");
    assert_eq!(r, Vec::<Vec<Value>>::new());
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate over single-group with WHERE filtering all → one NULL row.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_aggregate_all_filtered_one_row() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    // WHERE filters all → aggregate over empty set returns one row (SUM=NULL).
    let sum = q(&db, "SELECT SUM(v) FROM t WHERE v > 100");
    assert_eq!(sum, vec![vec![Value::Null]]);
    let cnt = q(&db, "SELECT COUNT(*) FROM t WHERE v > 100");
    assert_eq!(cnt, vec![vec![Value::Integer(0)]]);
}
