//! Bug Hunt v73 — twentieth round: index consistency, agg consistency, numeric edges.

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

fn f_of(v: &Value) -> f64 {
    match v {
        Value::Integer(i) => *i as f64,
        Value::Float(f) => *f,
        _ => panic!("expected number, got {:?}", v),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Index consistency across checkpoint
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_index_consistent_after_checkpoint_update() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1,'a'),(2,'b')").unwrap();
        db.execute("CREATE INDEX idx_cat ON t(cat)").unwrap();
        db.execute("UPDATE t SET cat = 'b' WHERE id = 1").unwrap();
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    // After reopen, index should reflect the UPDATE.
    let r = q(&db, "SELECT id FROM t WHERE cat = 'b' ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
    let r2 = q(&db, "SELECT id FROM t WHERE cat = 'a'");
    assert!(
        r2.is_empty(),
        "old value 'a' should have no rows after UPDATE"
    );
}

#[test]
fn test_index_consistent_after_checkpoint_delete() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'a')")
            .unwrap();
        db.execute("CREATE INDEX idx_cat ON t(cat)").unwrap();
        db.execute("DELETE FROM t WHERE id = 1").unwrap();
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT id FROM t WHERE cat = 'a' ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

#[test]
fn test_index_point_query_after_many_ops() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("CREATE INDEX idx_v ON t(v)").unwrap();
    // Insert, update, delete in a mix.
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    db.execute("UPDATE t SET v = 10 WHERE id = 3").unwrap();
    db.execute("DELETE FROM t WHERE id = 2").unwrap();
    // v=10 → ids 1, 3.
    let r = q(&db, "SELECT id FROM t WHERE v = 10 ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
    let r2 = q(&db, "SELECT id FROM t WHERE v = 20");
    assert!(
        r2.is_empty(),
        "v=20 (id2 deleted, id3 updated) should be empty"
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate consistency: GROUP BY vs filtered equivalent
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_groupby_sum_matches_filtered_sum() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(g INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(1,20),(2,5),(2,15),(3,100)")
        .unwrap();
    // SUM for g=1 via GROUP BY.
    let r1 = q(&db, "SELECT SUM(v) FROM t WHERE g = 1");
    // SUM for g=1 via GROUP BY + HAVING.
    let r2 = q(&db, "SELECT g, SUM(v) FROM t GROUP BY g HAVING g = 1");
    assert_eq!(r1[0][0], r2[0][1], "filtered SUM should match GROUP BY SUM");
}

#[test]
fn test_count_star_equals_count_rows() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL),(3,30)")
        .unwrap();
    let cnt = q(&db, "SELECT COUNT(*) FROM t")[0][0].clone();
    let nrows = q(&db, "SELECT id FROM t").len() as i64;
    assert_eq!(cnt, Value::Integer(nrows));
}

#[test]
fn test_avg_consistency() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (10),(20),(30),(40)")
        .unwrap();
    // AVG = SUM/COUNT = 100/4 = 25.
    let avg = f_of(&q(&db, "SELECT AVG(v) FROM t")[0][0]);
    let sum = f_of(&q(&db, "SELECT SUM(v) FROM t")[0][0]);
    let cnt = f_of(&q(&db, "SELECT COUNT(v) FROM t")[0][0]);
    assert!((avg - sum / cnt).abs() < 1e-9, "AVG should equal SUM/COUNT");
}

// ─────────────────────────────────────────────────────────────────────────
// Numeric edge cases
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_i64_max_value() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute(&format!("INSERT INTO t VALUES (1,{})", i64::MAX))
        .unwrap();
    let r = q(&db, "SELECT v FROM t");
    assert_eq!(r, vec![vec![Value::Integer(i64::MAX)]]);
}

#[test]
fn test_i64_min_value() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute(&format!("INSERT INTO t VALUES (1,{})", i64::MIN))
        .unwrap();
    let r = q(&db, "SELECT v FROM t");
    assert_eq!(r, vec![vec![Value::Integer(i64::MIN)]]);
}

#[test]
fn test_modulo_negative_dividend() {
    // -7 % 3 — sign of result follows dividend (truncated division, C-like).
    let (db, _d) = db();
    let r = q(&db, "SELECT -7 % 3");
    assert_eq!(r, vec![vec![Value::Integer(-1)]]);
}

#[test]
fn test_modulo_negative_divisor() {
    // 7 % -3 → 1 (truncated: sign follows dividend).
    let (db, _d) = db();
    let r = q(&db, "SELECT 7 % -3");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_division_truncates_toward_zero() {
    // -7 / 2 → -3 (truncated toward zero, not floored -4).
    let (db, _d) = db();
    let r = q(&db, "SELECT -7 / 2");
    assert_eq!(r, vec![vec![Value::Integer(-3)]]);
}

#[test]
fn test_abs_of_negative() {
    let (db, _d) = db();
    let r = q(&db, "SELECT ABS(-100)");
    assert_eq!(r, vec![vec![Value::Integer(100)]]);
}

#[test]
fn test_arithmetic_chain_negatives() {
    let (db, _d) = db();
    let r = q(&db, "SELECT -5 - -3 - -2");
    // -5 + 3 + 2 = 0
    assert_eq!(r, vec![vec![Value::Integer(0)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// String functions return correct type
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_upper_returns_text() {
    let (db, _d) = db();
    let r = q(&db, "SELECT UPPER('abc')");
    assert!(matches!(r[0][0], Value::Text(_)));
}

#[test]
fn test_substr_returns_text() {
    let (db, _d) = db();
    let r = q(&db, "SELECT SUBSTR('hello', 1, 3)");
    assert!(matches!(r[0][0], Value::Text(_)));
}

#[test]
fn test_length_returns_int() {
    let (db, _d) = db();
    let r = q(&db, "SELECT LENGTH('hello')");
    assert!(matches!(r[0][0], Value::Integer(_)));
}

// ─────────────────────────────────────────────────────────────────────────
// IS NULL / IS NOT NULL with text and float
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_is_null_text_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'x'),(2,NULL)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE s IS NULL");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_is_not_null_float_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,1.5),(2,NULL)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE v IS NOT NULL");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE with OR across different columns
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_where_or_different_columns() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10,0),(2,0,20),(3,10,20)")
        .unwrap();
    // a = 10 OR b = 20 → all three
    let r = q(&db, "SELECT id FROM t WHERE a = 10 OR b = 20 ORDER BY id");
    assert_eq!(
        r.iter()
            .map(|row| match &row[0] {
                Value::Integer(i) => *i,
                _ => -1,
            })
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// COUNT(*) with WHERE that filters all
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_count_star_all_filtered() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t WHERE id > 100");
    assert_eq!(r, vec![vec![Value::Integer(0)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// DISTINCT + LIMIT
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_distinct_with_limit() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,10),(3,20),(4,20),(5,30)")
        .unwrap();
    let r = q(&db, "SELECT DISTINCT v FROM t ORDER BY v LIMIT 2");
    assert_eq!(r, vec![vec![Value::Integer(10)], vec![Value::Integer(20)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Nested subquery in IN with aggregate
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_in_subquery_with_avg() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)")
        .unwrap();
    // rows where v > AVG(v) (avg=25) → ids 3, 4
    let r = q(
        &db,
        "SELECT id FROM t WHERE v > (SELECT AVG(v) FROM t) ORDER BY id",
    );
    assert_eq!(r, vec![vec![Value::Integer(3)], vec![Value::Integer(4)]]);
}

#[test]
fn test_not_in_subquery_with_max() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    // ids where v != MAX(v)=30 → ids 1, 2
    let r = q(
        &db,
        "SELECT id FROM t WHERE v NOT IN (SELECT MAX(v) FROM t) ORDER BY id",
    );
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY then LIMIT then the result is the first K
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_order_limit_returns_smallest() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,50),(2,10),(3,30),(4,20)")
        .unwrap();
    // ORDER BY v ASC LIMIT 2 → two smallest: 10, 20
    let r = q(&db, "SELECT v FROM t ORDER BY v ASC LIMIT 2");
    assert_eq!(r, vec![vec![Value::Integer(10)], vec![Value::Integer(20)]]);
}

#[test]
fn test_order_desc_limit_returns_largest() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,50),(2,10),(3,30),(4,20)")
        .unwrap();
    let r = q(&db, "SELECT v FROM t ORDER BY v DESC LIMIT 2");
    assert_eq!(r, vec![vec![Value::Integer(50)], vec![Value::Integer(30)]]);
}
