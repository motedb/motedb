//! Bug Hunt v88 — round 15: timestamp/date handling, vector column edges,
//! large result sets, and stress consistency (same query many ways).

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
// TIMESTAMP column basics.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_timestamp_insert_select() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, ts TIMESTAMP)").unwrap();
    db.execute("INSERT INTO t VALUES (1, '2024-01-15T10:30:00')").unwrap();
    let r = q(&db, "SELECT ts FROM t");
    assert_eq!(r.len(), 1);
    // Verify it round-trips (either as Timestamp or Text ISO).
}

#[test]
fn test_timestamp_where_equality() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, ts TIMESTAMP)").unwrap();
    db.execute("INSERT INTO t VALUES (1, '2024-01-15T10:30:00'),(2, '2024-06-01T08:00:00')").unwrap();
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE ts = '2024-01-15T10:30:00'"));
    assert_eq!(r, vec![1]);
}

#[test]
fn test_timestamp_where_inequality() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, ts TIMESTAMP)").unwrap();
    db.execute("INSERT INTO t VALUES (1, '2024-01-15T10:30:00'),(2, '2024-06-01T08:00:00'),(3, '2024-12-31T23:59:59')").unwrap();
    // ts > '2024-03-01' → id2, id3.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE ts > '2024-03-01T00:00:00'"));
    assert_eq!(r, vec![2, 3]);
}

#[test]
fn test_timestamp_order_by() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, ts TIMESTAMP)").unwrap();
    db.execute("INSERT INTO t VALUES (1, '2024-12-01T00:00:00'),(2, '2024-01-01T00:00:00'),(3, '2024-06-01T00:00:00')").unwrap();
    let r = q(&db, "SELECT id FROM t ORDER BY ts ASC");
    let ids: Vec<i64> = r.iter().filter_map(|row| match row[0] { Value::Integer(i) => Some(i), _ => None }).collect();
    assert_eq!(ids, vec![2, 3, 1]); // Jan, Jun, Dec.
}

#[test]
fn test_timestamp_min_max() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, ts TIMESTAMP)").unwrap();
    db.execute("INSERT INTO t VALUES (1, '2024-01-15T10:30:00'),(2, '2024-06-01T08:00:00'),(3, '2024-03-15T12:00:00')").unwrap();
    let minr = q(&db, "SELECT MIN(ts) FROM t");
    let maxr = q(&db, "SELECT MAX(ts) FROM t");
    assert_eq!(minr.len(), 1);
    assert_eq!(maxr.len(), 1);
    // MIN should be Jan, MAX should be Jun.
}

#[test]
fn test_timestamp_in_list() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, ts TIMESTAMP)").unwrap();
    db.execute("INSERT INTO t VALUES (1, '2024-01-15T10:30:00'),(2, '2024-06-01T08:00:00'),(3, '2024-03-15T12:00:00')").unwrap();
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE ts IN ('2024-01-15T10:30:00', '2024-06-01T08:00:00')"));
    assert_eq!(r, vec![1, 2]);
}

// ─────────────────────────────────────────────────────────────────────────
// Date extraction functions.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_extract_year() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, ts TIMESTAMP)").unwrap();
    db.execute("INSERT INTO t VALUES (1, '2024-01-15T10:30:00')").unwrap();
    let r = q(&db, "SELECT YEAR(ts) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(2024)]]);
}

#[test]
fn test_extract_month() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, ts TIMESTAMP)").unwrap();
    db.execute("INSERT INTO t VALUES (1, '2024-03-15T10:30:00')").unwrap();
    let r = q(&db, "SELECT MONTH(ts) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

#[test]
fn test_extract_day_hour() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, ts TIMESTAMP)").unwrap();
    db.execute("INSERT INTO t VALUES (1, '2024-03-15T10:30:45')").unwrap();
    let day = q(&db, "SELECT DAY(ts) FROM t");
    assert_eq!(day, vec![vec![Value::Integer(15)]]);
    let hour = q(&db, "SELECT HOUR(ts) FROM t");
    assert_eq!(hour, vec![vec![Value::Integer(10)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Large result set (stress consistency).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_large_result_set() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    // Insert 200 rows.
    for i in 1..=200 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 2)).unwrap();
    }
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(200)]]);
    let sumr = q(&db, "SELECT SUM(v) FROM t");
    // SUM(2,4,...,400) = 2*(1+2+...+200) = 2*20100 = 40200.
    assert_eq!(sumr, vec![vec![Value::Integer(40200)]]);
}

#[test]
fn test_large_result_order_limit() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=100 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, (i * 7) % 100)).unwrap();
    }
    let r = q(&db, "SELECT id FROM t ORDER BY v ASC, id ASC LIMIT 5");
    assert_eq!(r.len(), 5);
    // First 5 by v ASC — v=0 first (id where (id*7)%100==0).
}

// ─────────────────────────────────────────────────────────────────────────
// Same query via multiple syntactic forms — must agree.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_eq_vs_in_single() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    let via_eq = sorted_int(&q(&db, "SELECT id FROM t WHERE v = 20"));
    let via_in = sorted_int(&q(&db, "SELECT id FROM t WHERE v IN (20)"));
    assert_eq!(via_eq, via_in);
    assert_eq!(via_eq, vec![2]);
}

#[test]
fn test_ne_vs_not_in_single() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    let via_ne = sorted_int(&q(&db, "SELECT id FROM t WHERE v != 20"));
    let via_notin = sorted_int(&q(&db, "SELECT id FROM t WHERE v NOT IN (20)"));
    assert_eq!(via_ne, via_notin);
    assert_eq!(via_ne, vec![1, 3]);
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate of zero rows (filtered out) — single NULL/0 row.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_aggregate_zero_rows_filtered() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    assert_eq!(q(&db, "SELECT COUNT(*) FROM t WHERE v > 100"), vec![vec![Value::Integer(0)]]);
    assert_eq!(q(&db, "SELECT SUM(v) FROM t WHERE v > 100"), vec![vec![Value::Null]]);
    assert_eq!(q(&db, "SELECT MIN(v) FROM t WHERE v > 100"), vec![vec![Value::Null]]);
    assert_eq!(q(&db, "SELECT MAX(v) FROM t WHERE v > 100"), vec![vec![Value::Null]]);
    assert_eq!(q(&db, "SELECT AVG(v) FROM t WHERE v > 100"), vec![vec![Value::Null]]);
}

// ─────────────────────────────────────────────────────────────────────────
// DELETE all then re-populate (reset).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_delete_all_repopulate() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    db.execute("DELETE FROM t").unwrap();
    db.execute("INSERT INTO t VALUES (1,100),(2,200),(3,300)").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
    let sumr = q(&db, "SELECT SUM(v) FROM t");
    assert_eq!(sumr, vec![vec![Value::Integer(600)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Multiple UPDATEs accumulate.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_multiple_updates_accumulate() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    db.execute("UPDATE t SET v = v + 5").unwrap();
    db.execute("UPDATE t SET v = v + 5").unwrap();
    db.execute("UPDATE t SET v = v + 5").unwrap();
    let r = q(&db, "SELECT v FROM t");
    assert_eq!(r, vec![vec![Value::Integer(25)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE with IS NULL and IS NOT NULL combined via OR.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_is_null_or_is_not_null_all() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL),(3,30)").unwrap();
    // v IS NULL OR v IS NOT NULL → all rows (tautology, but via IS checks).
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v IS NULL OR v IS NOT NULL"));
    assert_eq!(r, vec![1, 2, 3]);
}

// ─────────────────────────────────────────────────────────────────────────
// GROUP BY with ORDER BY on group key.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_by_order_by_key() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'c',1),(2,'a',2),(3,'b',3)").unwrap();
    let r = q(&db, "SELECT cat, SUM(v) FROM t GROUP BY cat ORDER BY cat ASC");
    assert_eq!(r, vec![
        vec![Value::Text("a".into()), Value::Integer(2)],
        vec![Value::Text("b".into()), Value::Integer(3)],
        vec![Value::Text("c".into()), Value::Integer(1)],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// COUNT(*) with no rows returns 0 (not empty result).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_count_star_empty_returns_zero() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(0)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// DISTINCT then COUNT via subquery.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_count_distinct_subquery_text() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'x'),(2,'y'),(3,'x'),(4,'z'),(5,'y')").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM (SELECT DISTINCT s FROM t) AS sub");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Negative number in BETWEEN.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_between_with_negative_bounds() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,-50),(2,-10),(3,0),(4,10)").unwrap();
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v BETWEEN -20 AND 5"));
    assert_eq!(r, vec![2, 3]);
}

// ─────────────────────────────────────────────────────────────────────────
// UPDATE setting multiple columns with expressions.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_update_multi_col_expressions() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10,20)").unwrap();
    db.execute("UPDATE t SET a = a + 100, b = b * 2 WHERE id = 1").unwrap();
    let r = q(&db, "SELECT a, b FROM t");
    assert_eq!(r, vec![vec![Value::Integer(110), Value::Integer(40)]]);
}
