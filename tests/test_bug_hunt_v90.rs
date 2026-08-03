//! Bug Hunt v90 — round 17: final targeted probe — numeric coercion across
//! all paths, BOOL↔INT in WHERE/JOIN, mixed-type aggregate, and the trickiest
//! multi-path scenarios (same semantic query, different evaluation routes).

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
    let mut v: Vec<i64> = r
        .iter()
        .filter_map(|row| match row.get(0) {
            Some(Value::Integer(i)) => Some(*i),
            _ => None,
        })
        .collect();
    v.sort();
    v
}

// ─────────────────────────────────────────────────────────────────────────
// BOOL column compared with INT literal in WHERE.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_bool_where_eq_int() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, flag BOOLEAN)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,TRUE),(2,FALSE),(3,TRUE)")
        .unwrap();
    // flag = 1 (TRUE) → id1, id3.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE flag = 1"));
    assert_eq!(r, vec![1, 3]);
    // flag = 0 (FALSE) → id2.
    let r2 = sorted_int(&q(&db, "SELECT id FROM t WHERE flag = 0"));
    assert_eq!(r2, vec![2]);
}

#[test]
fn test_bool_where_ne_int() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, flag BOOLEAN)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,TRUE),(2,FALSE),(3,TRUE)")
        .unwrap();
    // flag != 1 → only FALSE matches (id2).
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE flag != 1"));
    assert_eq!(r, vec![2]);
}

// ─────────────────────────────────────────────────────────────────────────
// INT column compared with BOOL literal.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_int_where_eq_bool() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,0),(2,1),(3,5)")
        .unwrap();
    // v = TRUE (1) → id2.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v = TRUE"));
    assert_eq!(r, vec![2]);
    // v = FALSE (0) → id1.
    let r2 = sorted_int(&q(&db, "SELECT id FROM t WHERE v = FALSE"));
    assert_eq!(r2, vec![1]);
}

// ─────────────────────────────────────────────────────────────────────────
// Mixed-type aggregate (INT + FLOAT in same column shouldn't happen, but
// INT column summed with FLOAT literal expression).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_int_col_plus_float_literal() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    let r = q(&db, "SELECT SUM(v + 0.5) FROM t");
    match &r[0][0] {
        Value::Float(f) => assert!((f - 31.0).abs() < 1e-9, "(10.5 + 20.5) = 31.0, got {}", f),
        other => panic!("SUM(v+0.5) = 31.0, got {:?}", other),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// JOIN ON with BOOL column.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_join_on_bool() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(id INT PRIMARY KEY, flag BOOLEAN)")
        .unwrap();
    db.execute("CREATE TABLE b(id INT PRIMARY KEY, flag BOOLEAN)")
        .unwrap();
    db.execute("INSERT INTO a VALUES (1,TRUE)").unwrap();
    db.execute("INSERT INTO b VALUES (1,TRUE),(2,FALSE)")
        .unwrap();
    let r = q(&db, "SELECT a.id FROM a JOIN b ON a.flag = b.flag");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// GROUP BY BOOL column.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_by_bool() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, flag BOOLEAN, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,TRUE,10),(2,FALSE,20),(3,TRUE,30)")
        .unwrap();
    let mut r = q(&db, "SELECT flag, COUNT(*) FROM t GROUP BY flag");
    r.sort_by_key(|row| match row[0] {
        Value::Bool(b) => b,
        _ => false,
    });
    // FALSE group: count 1. TRUE group: count 2.
    assert_eq!(r.len(), 2);
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY BOOL column.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_order_by_bool_asc() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, flag BOOLEAN)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,TRUE),(2,FALSE),(3,TRUE)")
        .unwrap();
    let r = q(&db, "SELECT id FROM t ORDER BY flag ASC, id ASC");
    let ids: Vec<i64> = r
        .iter()
        .filter_map(|row| match row[0] {
            Value::Integer(i) => Some(i),
            _ => None,
        })
        .collect();
    // FALSE (0) before TRUE (1): id2 first, then id1, id3.
    assert_eq!(ids, vec![2, 1, 3]);
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE flag (bare boolean, no comparison).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_where_bare_bool_true() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, flag BOOLEAN)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,TRUE),(2,FALSE),(3,TRUE)")
        .unwrap();
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE flag"));
    assert_eq!(r, vec![1, 3]);
}

#[test]
fn test_where_bare_bool_false() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, flag BOOLEAN)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,TRUE),(2,FALSE),(3,TRUE)")
        .unwrap();
    // WHERE NOT flag → id2.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE NOT flag"));
    assert_eq!(r, vec![2]);
}

// ─────────────────────────────────────────────────────────────────────────
// Cross-path: WHERE flag = TRUE vs WHERE flag vs WHERE flag = 1 — all agree.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_bool_where_three_forms_agree() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, flag BOOLEAN)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,TRUE),(2,FALSE),(3,TRUE)")
        .unwrap();
    let via_eq_true = sorted_int(&q(&db, "SELECT id FROM t WHERE flag = TRUE"));
    let via_bare = sorted_int(&q(&db, "SELECT id FROM t WHERE flag"));
    let via_eq_one = sorted_int(&q(&db, "SELECT id FROM t WHERE flag = 1"));
    assert_eq!(via_eq_true, vec![1, 3]);
    assert_eq!(via_bare, via_eq_true);
    assert_eq!(via_eq_one, via_eq_true);
}

// ─────────────────────────────────────────────────────────────────────────
// Float column WHERE with INT literal (coercion).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_float_col_int_literal_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10.0),(2,20.5),(3,30.0)")
        .unwrap();
    // v = 30 (int) should match v=30.0.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v = 30"));
    assert_eq!(r, vec![3]);
    // v > 10 → id2, id3.
    let r2 = sorted_int(&q(&db, "SELECT id FROM t WHERE v > 10"));
    assert_eq!(r2, vec![2, 3]);
}

// ─────────────────────────────────────────────────────────────────────────
// Integer division vs float division consistency.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_division_consistency() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    // 10 / 4 — record behavior.
    let r1 = q(&db, "SELECT 10 / 4 FROM t");
    let _ = r1;
    // 10.0 / 4 — should be float 2.5.
    let r2 = q(&db, "SELECT 10.0 / 4 FROM t");
    match &r2[0][0] {
        Value::Float(f) => assert!((f - 2.5).abs() < 1e-9),
        other => panic!("10.0/4 = 2.5, got {:?}", other),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// NULL in arithmetic with various operators.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_null_arithmetic_all_ops() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,NULL,5)").unwrap();
    // Any arithmetic with NULL → NULL.
    assert_eq!(q(&db, "SELECT a + b FROM t"), vec![vec![Value::Null]]);
    assert_eq!(q(&db, "SELECT a - b FROM t"), vec![vec![Value::Null]]);
    assert_eq!(q(&db, "SELECT a * b FROM t"), vec![vec![Value::Null]]);
    assert_eq!(q(&db, "SELECT a / b FROM t"), vec![vec![Value::Null]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Comparison result used in arithmetic (TRUE/FALSE as 1/0).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_comparison_in_arithmetic() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    // (v > 5) is TRUE → if treated as 1, (v > 5) + (v > 100) = 1 + 0 = 1.
    // May not be supported; just verify no crash.
    let _ = db
        .execute("SELECT (v > 5) FROM t")
        .and_then(|s| s.materialize());
}

// ─────────────────────────────────────────────────────────────────────────
// String comparison case sensitivity.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_string_case_sensitive_comparison() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'Hello'),(2,'hello')")
        .unwrap();
    // 'Hello' != 'hello' (case-sensitive).
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE s = 'Hello'"));
    assert_eq!(r, vec![1]);
    let r2 = sorted_int(&q(&db, "SELECT id FROM t WHERE s = 'hello'"));
    assert_eq!(r2, vec![2]);
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY STRING case-sensitive ordering.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_order_by_string_case() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'apple'),(2,'Banana'),(3,'cherry')")
        .unwrap();
    let r = q(&db, "SELECT s FROM t ORDER BY s ASC");
    // Uppercase 'B' (66) < lowercase 'a' (97) in ASCII.
    let vals: Vec<String> = r
        .iter()
        .filter_map(|row| match &row[0] {
            Value::Text(s) => Some(s.as_str().to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(
        vals,
        vec![
            "Banana".to_string(),
            "apple".to_string(),
            "cherry".to_string()
        ]
    );
}
