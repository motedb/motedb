//! Bug Hunt v97 — round 24: nested transactions (rejection), string REPLACE/
//! padding if supported, multi-level derived tables, COUNT(DISTINCT) on TEXT,
//! and aggregate edge cases.

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
// COUNT(DISTINCT) on TEXT column.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_count_distinct_text() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'a'),(4,'c'),(5,'b')").unwrap();
    assert_eq!(q(&db, "SELECT COUNT(DISTINCT s) FROM t"), vec![vec![Value::Integer(3)]]);
}

#[test]
fn test_count_distinct_text_grouped() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'x','a'),(2,'x','b'),(3,'x','a'),(4,'y','c')").unwrap();
    let mut r = q(&db, "SELECT cat, COUNT(DISTINCT s) FROM t GROUP BY cat");
    r.sort_by_key(|row| match &row[0] { Value::Text(s) => s.as_str().to_string(), _ => "zzz".into() });
    // x: distinct s = {a,b} = 2. y: {c} = 1.
    assert_eq!(r, vec![
        vec![Value::Text("x".into()), Value::Integer(2)],
        vec![Value::Text("y".into()), Value::Integer(1)],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// SUM(DISTINCT) and AVG(DISTINCT).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_sum_distinct_grouped() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'a',10),(4,'b',5)").unwrap();
    let mut r = q(&db, "SELECT cat, SUM(DISTINCT v) FROM t GROUP BY cat");
    r.sort_by_key(|row| match &row[0] { Value::Text(s) => s.as_str().to_string(), _ => "zzz".into() });
    // a: distinct v = {10,20} sum=30. b: {5} sum=5.
    assert_eq!(r, vec![
        vec![Value::Text("a".into()), Value::Integer(30)],
        vec![Value::Text("b".into()), Value::Integer(5)],
    ]);
}

#[test]
fn test_avg_distinct() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,10),(3,20),(4,20),(5,30)").unwrap();
    // AVG(DISTINCT v): distinct = {10,20,30}, avg = 20.
    let r = q(&db, "SELECT AVG(DISTINCT v) FROM t");
    match &r[0][0] {
        Value::Float(f) => assert!((f - 20.0).abs() < 1e-9, "AVG(DISTINCT) = 20, got {}", f),
        Value::Integer(20) => {}
        other => panic!("AVG(DISTINCT) = 20, got {:?}", other),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// MIN(DISTINCT)/MAX(DISTINCT) (same as MIN/MAX).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_min_max_distinct_grouped() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'a',10),(4,'b',5),(5,'b',15)").unwrap();
    let mut r = q(&db, "SELECT cat, MIN(DISTINCT v), MAX(DISTINCT v) FROM t GROUP BY cat");
    r.sort_by_key(|row| match &row[0] { Value::Text(s) => s.as_str().to_string(), _ => "zzz".into() });
    assert_eq!(r, vec![
        vec![Value::Text("a".into()), Value::Integer(10), Value::Integer(20)],
        vec![Value::Text("b".into()), Value::Integer(5), Value::Integer(15)],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// Multiple DISTINCT aggregates in one query.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_multiple_distinct_aggregates() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT, w INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10,100),(2,10,200),(3,20,100),(4,20,200)").unwrap();
    // COUNT(DISTINCT v) = 2 ({10,20}). COUNT(DISTINCT w) = 2 ({100,200}).
    let r = q(&db, "SELECT COUNT(DISTINCT v), COUNT(DISTINCT w) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(2), Value::Integer(2)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Nested derived tables (subquery in FROM of subquery).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_nested_derived_tables() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',15)").unwrap();
    // Inner: per-cat sum. Outer: filter sum > 10.
    let r = q(&db, "SELECT outer_q.cat FROM (SELECT cat FROM (SELECT cat, SUM(v) AS s FROM t GROUP BY cat) AS inner_q WHERE inner_q.s > 10) AS outer_q ORDER BY outer_q.cat");
    let cats: Vec<String> = r.iter().filter_map(|row| match &row[0] { Value::Text(s) => Some(s.as_str().to_string()), _ => None }).collect();
    assert_eq!(cats, vec!["a".to_string(), "b".to_string()]);
}

// ─────────────────────────────────────────────────────────────────────────
// COMMIT without BEGIN (should error or no-op).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_commit_without_begin() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    let res = db.execute("COMMIT");
    // Should error (no active transaction) or no-op.
    // Either is acceptable; just verify no crash and table still usable.
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(0)]]);
}

#[test]
fn test_rollback_without_begin() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    let _ = db.execute("ROLLBACK");
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(0)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// BEGIN ... BEGIN (nested, should reject per v52c).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_begin_begin_reject() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("BEGIN TRANSACTION").unwrap();
    let res = db.execute("BEGIN TRANSACTION");
    assert!(res.is_err(), "nested BEGIN must be rejected");
    db.execute("ROLLBACK").unwrap();
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate of constant in GROUP BY.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_constant_aggregate() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'a'),(3,'b')").unwrap();
    // SUM(1) per group = count of rows.
    let mut r = q(&db, "SELECT cat, SUM(1) FROM t GROUP BY cat");
    r.sort_by_key(|row| match &row[0] { Value::Text(s) => s.as_str().to_string(), _ => "zzz".into() });
    assert_eq!(r, vec![
        vec![Value::Text("a".into()), Value::Integer(2)],
        vec![Value::Text("b".into()), Value::Integer(1)],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE with arithmetic producing same as column compare.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_where_arith_vs_col() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    // v * 2 = 40 → id2 (20*2=40).
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v * 2 = 40"));
    assert_eq!(r, vec![2]);
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY with expression then LIMIT.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_order_by_expr_limit() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,30),(2,10),(3,50),(4,20),(5,40)").unwrap();
    // ORDER BY v ASC LIMIT 2 → 10, 20.
    let r = q(&db, "SELECT id FROM t ORDER BY v ASC LIMIT 2");
    let ids: Vec<i64> = r.iter().filter_map(|row| match row[0] { Value::Integer(i) => Some(i), _ => None }).collect();
    assert_eq!(ids, vec![2, 4]);
}

// ─────────────────────────────────────────────────────────────────────────
// GROUP BY with HAVING using a non-aggregate column.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_having_non_agg_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5)").unwrap();
    // HAVING cat != 'b' (filter on group key).
    let r = q(&db, "SELECT cat, SUM(v) FROM t GROUP BY cat HAVING cat != 'b'");
    assert_eq!(r, vec![vec![Value::Text("a".into()), Value::Integer(30)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// SELECT with WHERE referencing aliased column (shouldn't work — alias not in WHERE).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_select_alias_in_order_by() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,30),(2,10),(3,20)").unwrap();
    let r = q(&db, "SELECT v AS val FROM t ORDER BY val ASC");
    let vals: Vec<i64> = r.iter().filter_map(|row| match row[0] { Value::Integer(i) => Some(i), _ => None }).collect();
    assert_eq!(vals, vec![10, 20, 30]);
}

// ─────────────────────────────────────────────────────────────────────────
// Empty GROUP BY result with HAVING (no groups pass).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_having_all_excluded() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20)").unwrap();
    // HAVING SUM(v) > 100 → no group passes.
    let r = q(&db, "SELECT cat FROM t GROUP BY cat HAVING SUM(v) > 100");
    assert_eq!(r, Vec::<Vec<Value>>::new());
}

// ─────────────────────────────────────────────────────────────────────────
// Multiple rows with same PK in batch INSERT (should fail entire batch or skip).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_batch_insert_duplicate_pk() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    // Batch with duplicate PK (id=1 twice).
    let res = db.execute("INSERT INTO t VALUES (1,10),(1,20),(2,30)");
    // Either: whole batch fails (0 rows), or partial (id=1 first wins, id=2 inserted).
    // Verify no corruption: querying shouldn't panic.
    let r = q(&db, "SELECT COUNT(*) FROM t");
    // Accept either 0 (all failed) or 2 (id=1 first + id=2).
    match &r[0][0] {
        Value::Integer(n) => assert!(*n == 0 || *n == 2, "expected 0 or 2 rows, got {}", n),
        other => panic!("count unexpected {:?}", other),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate over column with all same values.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_aggregate_uniform_values() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,5),(3,5)").unwrap();
    assert_eq!(q(&db, "SELECT SUM(v), MIN(v), MAX(v), AVG(v), COUNT(DISTINCT v) FROM t"),
        vec![vec![Value::Integer(15), Value::Integer(5), Value::Integer(5), Value::Integer(5), Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// SELECT with no WHERE, no ORDER BY, just project (basic).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_basic_project() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10,20)").unwrap();
    let r = q(&db, "SELECT a, b FROM t");
    assert_eq!(r, vec![vec![Value::Integer(10), Value::Integer(20)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY DESC with LIMIT on PK.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_order_desc_limit_pk() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=5 {
        db.execute(&format!("INSERT INTO t VALUES ({},{})", i, i * 10)).unwrap();
    }
    let r = q(&db, "SELECT id FROM t ORDER BY id DESC LIMIT 2");
    let ids: Vec<i64> = r.iter().filter_map(|row| match row[0] { Value::Integer(i) => Some(i), _ => None }).collect();
    assert_eq!(ids, vec![5, 4]);
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE with column IN (subquery) where subquery has WHERE on different col.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_in_subquery_filtered() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40)").unwrap();
    // id IN (SELECT id FROM t WHERE cat = 'a') → {1,2}.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE id IN (SELECT id FROM t WHERE cat = 'a')"));
    assert_eq!(r, vec![1, 2]);
}

// ─────────────────────────────────────────────────────────────────────────
// SUM over expression with GROUP BY.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_sum_expr_grouped() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, qty INT, price INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',2,5),(2,'a',3,10),(3,'b',1,20)").unwrap();
    let mut r = q(&db, "SELECT cat, SUM(qty * price) FROM t GROUP BY cat");
    r.sort_by_key(|row| match &row[0] { Value::Text(s) => s.as_str().to_string(), _ => "zzz".into() });
    // a: 2*5 + 3*10 = 40. b: 1*20 = 20.
    assert_eq!(r, vec![
        vec![Value::Text("a".into()), Value::Integer(40)],
        vec![Value::Text("b".into()), Value::Integer(20)],
    ]);
}
