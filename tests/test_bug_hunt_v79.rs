//! Bug Hunt v79 — round 6: implicit coercion boundaries, string functions
//! on edge inputs, ABS/ROUND/CEIL/FLOOR, IN with mixed types, timestamp
//! comparisons, cross-path consistency for the same logical query.

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
// Numeric functions: ABS, ROUND, CEIL/FLOOR.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_abs_positive() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = q(&db, "SELECT ABS(5) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(5)]]);
}

#[test]
fn test_abs_negative() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = q(&db, "SELECT ABS(-7) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(7)]]);
}

#[test]
fn test_abs_zero() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = q(&db, "SELECT ABS(0) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(0)]]);
}

#[test]
fn test_round_positive_decimals() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = q(&db, "SELECT ROUND(3.14159, 2) FROM t");
    match &r[0][0] {
        Value::Float(f) => assert!((f - 3.14).abs() < 1e-9, "ROUND(3.14159,2)=3.14, got {}", f),
        Value::Integer(i) => assert_eq!(*i, 3),
        other => panic!("ROUND unexpected {:?}", other),
    }
}

#[test]
fn test_round_zero_decimals() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = q(&db, "SELECT ROUND(2.5) FROM t");
    // ROUND(2.5) — banker's or half-up; both acceptable, just record.
    assert_eq!(r.len(), 1);
    match &r[0][0] {
        Value::Float(f) => assert!(*f == 3.0 || *f == 2.0, "ROUND(2.5) got {}", f),
        Value::Integer(i) => assert!(*i == 3 || *i == 2),
        other => panic!("ROUND unexpected {:?}", other),
    }
}

#[test]
fn test_ceil_floor() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r1 = q(&db, "SELECT CEIL(2.3) FROM t");
    match &r1[0][0] {
        Value::Float(f) => assert!((f - 3.0).abs() < 1e-9),
        Value::Integer(3) => {}
        other => panic!("CEIL(2.3)=3, got {:?}", other),
    }
    let r2 = q(&db, "SELECT FLOOR(2.9) FROM t");
    match &r2[0][0] {
        Value::Float(f) => assert!((f - 2.0).abs() < 1e-9),
        Value::Integer(2) => {}
        other => panic!("FLOOR(2.9)=2, got {:?}", other),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// String functions on edge inputs.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_length_empty_string() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = q(&db, "SELECT LENGTH('') FROM t");
    assert_eq!(r, vec![vec![Value::Integer(0)]]);
}

#[test]
fn test_upper_lower_empty() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = q(&db, "SELECT UPPER('') FROM t");
    assert_eq!(r, vec![vec![Value::Text("".into())]]);
}

#[test]
fn test_concat_with_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,NULL)").unwrap();
    // 'a' || NULL — SQL standard: NULL concatenation → NULL.
    let r = q(&db, "SELECT 'a' || s FROM t");
    assert_eq!(r, vec![vec![Value::Null]]);
}

// ─────────────────────────────────────────────────────────────────────────
// IN with mixed types (int column, int literals).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_in_int_literals() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    let mut r = q(&db, "SELECT id FROM t WHERE v IN (10, 30)");
    r.sort_by_key(|row| match row[0] { Value::Integer(i) => i, _ => 999 });
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

#[test]
fn test_not_in_int_literals() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    let mut r = q(&db, "SELECT id FROM t WHERE v NOT IN (10, 30)");
    r.sort_by_key(|row| match row[0] { Value::Integer(i) => i, _ => 999 });
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Cross-path consistency: same query via different routes must agree.
// Filtered aggregate vs full scan aggregate.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_sum_via_where_matches_unfiltered() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)").unwrap();
    let total = q(&db, "SELECT SUM(v) FROM t");
    let filtered = q(&db, "SELECT SUM(v) FROM t WHERE v > 15");
    // total = 100, filtered (20+30+40) = 90.
    assert_eq!(total, vec![vec![Value::Integer(100)]]);
    assert_eq!(filtered, vec![vec![Value::Integer(90)]]);
}

#[test]
fn test_count_via_indexed_vs_scan() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'a'),(3,'b'),(4,'b'),(5,'b')").unwrap();
    // GROUP BY cat — count must be consistent regardless of internal path.
    let mut r = q(&db, "SELECT cat, COUNT(*) FROM t GROUP BY cat");
    r.sort_by_key(|row| match &row[0] { Value::Text(s) => s.as_str().to_string(), _ => "zzz".into() });
    assert_eq!(r, vec![
        vec![Value::Text("a".into()), Value::Integer(2)],
        vec![Value::Text("b".into()), Value::Integer(3)],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// UPDATE then immediate SELECT consistency (read-your-writes).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_update_then_aggregate() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    db.execute("UPDATE t SET v = 100 WHERE id = 1").unwrap();
    let r = q(&db, "SELECT SUM(v) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(120)]]);
}

#[test]
fn test_delete_then_count() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    db.execute("DELETE FROM t WHERE v >= 20").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Negative number arithmetic and comparison.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_negative_arithmetic() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = q(&db, "SELECT -5 + 3 FROM t");
    assert_eq!(r, vec![vec![Value::Integer(-2)]]);
}

#[test]
fn test_subtraction_to_negative() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = q(&db, "SELECT 3 - 10 FROM t");
    assert_eq!(r, vec![vec![Value::Integer(-7)]]);
}

#[test]
fn test_negative_in_order_by() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,-5),(2,3),(3,-10),(4,0)").unwrap();
    let r = q(&db, "SELECT v FROM t ORDER BY v ASC");
    let vals: Vec<i64> = r.iter().filter_map(|row| match row[0] { Value::Integer(i) => Some(i), _ => None }).collect();
    assert_eq!(vals, vec![-10, -5, 0, 3]);
}

// ─────────────────────────────────────────────────────────────────────────
// Boolean column values.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_boolean_column_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, flag BOOLEAN)").unwrap();
    db.execute("INSERT INTO t VALUES (1,TRUE),(2,FALSE),(3,TRUE)").unwrap();
    let mut r = q(&db, "SELECT id FROM t WHERE flag = TRUE");
    r.sort_by_key(|row| match row[0] { Value::Integer(i) => i, _ => 999 });
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

#[test]
fn test_boolean_column_count() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, flag BOOLEAN)").unwrap();
    db.execute("INSERT INTO t VALUES (1,TRUE),(2,FALSE),(3,TRUE)").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t WHERE flag");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// DISTINCT count consistency.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_distinct_count_consistency() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,10),(3,20),(4,20),(5,20),(6,30)").unwrap();
    let distinct = q(&db, "SELECT DISTINCT v FROM t");
    assert_eq!(distinct.len(), 3, "3 distinct values; got {:?}", distinct);
}

// ─────────────────────────────────────────────────────────────────────────
// NULL in GROUP BY (NULL forms its own group).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_by_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,NULL,20),(3,'a',30),(4,NULL,40)").unwrap();
    let r = q(&db, "SELECT cat, COUNT(*) FROM t GROUP BY cat");
    // 2 groups: 'a' (count 2), NULL (count 2).
    assert_eq!(r.len(), 2, "NULL should form its own group; got {:?}", r);
}

// ─────────────────────────────────────────────────────────────────────────
// LIKE case sensitivity (standard SQL LIKE is case-sensitive).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_like_case_sensitive() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'Apple'),(2,'apple')").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE s LIKE 'Apple'");
    assert_eq!(r, vec![vec![Value::Integer(1)]], "LIKE is case-sensitive");
}

// ─────────────────────────────────────────────────────────────────────────
// LIKE with percent in middle.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_like_percent_middle() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'hello world'),(2,'hello'),(3,'world hello')").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE s LIKE 'hello%world'");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Multiple WHERE conditions with AND/OR mix.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_complex_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT, c INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,1,2,3),(2,1,2,4),(3,1,5,3),(4,2,2,3)").unwrap();
    // (a=1 AND b=2) AND (c=3 OR c=4)
    let mut r = q(&db, "SELECT id FROM t WHERE (a = 1 AND b = 2) AND (c = 3 OR c = 4)");
    r.sort_by_key(|row| match row[0] { Value::Integer(i) => i, _ => 999 });
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate over single row.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_aggregate_single_row() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,42)").unwrap();
    assert_eq!(q(&db, "SELECT SUM(v) FROM t"), vec![vec![Value::Integer(42)]]);
    assert_eq!(q(&db, "SELECT MIN(v) FROM t"), vec![vec![Value::Integer(42)]]);
    assert_eq!(q(&db, "SELECT MAX(v) FROM t"), vec![vec![Value::Integer(42)]]);
    assert_eq!(q(&db, "SELECT AVG(v) FROM t"), vec![vec![Value::Integer(42)]]);
    assert_eq!(q(&db, "SELECT COUNT(v) FROM t"), vec![vec![Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Self-comparison: WHERE v = v (always true for non-NULL).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_self_equality_non_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL),(3,30)").unwrap();
    // v = v is TRUE for non-NULL, NULL (unknown) for NULL → NULL row excluded.
    let mut r = q(&db, "SELECT id FROM t WHERE v = v");
    r.sort_by_key(|row| match row[0] { Value::Integer(i) => i, _ => 999 });
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Comparison operators: <> (not equal).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_not_equal_operator() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    let mut r = q(&db, "SELECT id FROM t WHERE v <> 20");
    r.sort_by_key(|row| match row[0] { Value::Integer(i) => i, _ => 999 });
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

#[test]
fn test_not_equal_with_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL)").unwrap();
    // v <> 10: id=1 is FALSE, id=2 (NULL) is UNKNOWN → neither matches.
    let r = q(&db, "SELECT id FROM t WHERE v <> 10");
    assert_eq!(r, Vec::<Vec<Value>>::new());
}
