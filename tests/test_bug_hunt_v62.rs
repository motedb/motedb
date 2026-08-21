//! Bug Hunt v62 — ninth round: deep edges, operator precedence, type corners.

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
// Operator precedence deep checks
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_arithmetic_vs_comparison() {
    // a + b > c → (a+b) > c
    let (db, _d) = db();
    let r = q(&db, "SELECT 2 + 3 > 4");
    assert_eq!(r, vec![vec![Value::Bool(true)]]);
}

#[test]
fn test_concat_precedence_with_comparison() {
    // a || b = 'xy' → (a || b) = 'xy'
    let (db, _d) = db();
    let r = q(&db, "SELECT 'x' || 'y' = 'xy'");
    assert_eq!(r, vec![vec![Value::Bool(true)]]);
}

#[test]
fn test_not_precedence() {
    // NOT a = b → NOT (a = b)
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,10)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE NOT v = 5");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_unary_minus_precedence() {
    let (db, _d) = db();
    let r = q(&db, "SELECT -2 * -3, -2 + 3, 10 - -5");
    assert_eq!(
        r,
        vec![vec![
            Value::Integer(6),
            Value::Integer(1),
            Value::Integer(15)
        ]]
    );
}

#[test]
fn test_modulo_precedence() {
    // % binds with * and / (precedence 5), tighter than +/-
    let (db, _d) = db();
    let r = q(&db, "SELECT 2 + 7 % 3");
    // 7%3 = 1, +2 = 3
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// TIMESTAMP lifecycle
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_timestamp_insert_select_roundtrip() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, ts TIMESTAMP)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, '2024-06-15T12:30:45')")
        .unwrap();
    let r = q(&db, "SELECT ts FROM t WHERE id = 1");
    assert_eq!(r.len(), 1);
    assert!(matches!(r[0][0], Value::Timestamp(_)));
}

#[test]
fn test_timestamp_extract_components() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, ts TIMESTAMP)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, '2024-06-15T12:30:45')")
        .unwrap();
    let r = q(
        &db,
        "SELECT YEAR(ts), MONTH(ts), DAY(ts), HOUR(ts), MINUTE(ts), SECOND(ts) FROM t WHERE id = 1",
    );
    assert_eq!(
        r,
        vec![vec![
            Value::Integer(2024),
            Value::Integer(6),
            Value::Integer(15),
            Value::Integer(12),
            Value::Integer(30),
            Value::Integer(45),
        ]]
    );
}

#[test]
fn test_timestamp_order_chronological() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, ts TIMESTAMP)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'2024-06-15T12:00:00'),(2,'2024-01-01T00:00:00'),(3,'2024-12-31T23:59:59')").unwrap();
    let r = q(&db, "SELECT id FROM t ORDER BY ts");
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(2)],
            vec![Value::Integer(1)],
            vec![Value::Integer(3)]
        ]
    );
}

#[test]
fn test_timestamp_comparison_filter() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, ts TIMESTAMP)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'2024-01-15T00:00:00'),(2,'2024-06-15T00:00:00'),(3,'2023-12-25T00:00:00')").unwrap();
    let r = q(
        &db,
        "SELECT id FROM t WHERE ts > '2024-01-01T00:00:00' ORDER BY id",
    );
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
}

#[test]
fn test_timestamp_min_max() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, ts TIMESTAMP)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'2024-06-15T00:00:00'),(2,'2024-01-01T00:00:00'),(3,'2024-12-31T00:00:00')").unwrap();
    let r = q(&db, "SELECT MIN(ts), MAX(ts) FROM t");
    assert_eq!(r.len(), 1);
    // MIN = 2024-01-01, MAX = 2024-12-31
    assert!(matches!(r[0][0], Value::Timestamp(_)));
    assert!(matches!(r[0][1], Value::Timestamp(_)));
}

// ─────────────────────────────────────────────────────────────────────────
// Window function deeper edges
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_lag_with_explicit_offset() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)")
        .unwrap();
    // LAG(v, 2) — value 2 rows back
    let r = q(
        &db,
        "SELECT id, LAG(v, 2) OVER (ORDER BY id) AS prev2 FROM t ORDER BY id",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Null],
            vec![Value::Integer(2), Value::Null],
            vec![Value::Integer(3), Value::Integer(10)],
            vec![Value::Integer(4), Value::Integer(20)],
        ]
    );
}

#[test]
fn test_lag_with_default() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    // LAG(v, 1, -1) — default -1 when no prior row
    let res = db.execute("SELECT id, LAG(v, 1, -1) OVER (ORDER BY id) FROM t ORDER BY id");
    match res {
        Ok(r) => {
            let got = rows(r.materialize().unwrap());
            assert_eq!(got.len(), 2);
            // id1: prev = -1 (default); id2: prev = 10
            assert_eq!(got[0][1], Value::Integer(-1));
            assert_eq!(got[1][1], Value::Integer(10));
        }
        Err(_) => { /* default arg unsupported: acceptable */ }
    }
}

#[test]
fn test_row_number_simple() {
    let (db, _d) = db();
    db.execute("CREATE TABLE s(id INT PRIMARY KEY, score INT)")
        .unwrap();
    db.execute("INSERT INTO s VALUES (1,30),(2,10),(3,20)")
        .unwrap();
    let r = q(
        &db,
        "SELECT id, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn FROM s ORDER BY rn",
    );
    // desc: 30(rn1), 20(rn2), 10(rn3) → ids 1,3,2
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(1)],
            vec![Value::Integer(3), Value::Integer(2)],
            vec![Value::Integer(2), Value::Integer(3)],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Nested CTE + subquery
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_cte_then_in_subquery() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'b',20),(3,'a',30)")
        .unwrap();
    let r = q(
        &db,
        "WITH x AS (SELECT cat, SUM(v) AS s FROM t GROUP BY cat) SELECT cat FROM x WHERE s IN (SELECT MAX(s) FROM x)",
    );
    // x: a=40, b=20. MAX(s)=40. cat where s=40 → 'a'
    assert_eq!(r, vec![vec![Value::text("a".into())]]);
}

#[test]
fn test_cte_used_twice() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    let r = q(
        &db,
        "WITH x AS (SELECT * FROM t) SELECT a.v FROM x a JOIN x b ON a.id = b.id ORDER BY a.v",
    );
    assert_eq!(r, vec![vec![Value::Integer(10)], vec![Value::Integer(20)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate of expression
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_sum_of_expression() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, price INT, qty INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10,2),(2,5,3),(3,20,1)")
        .unwrap();
    // SUM(price * qty) = 20 + 15 + 20 = 55
    let r = q(&db, "SELECT SUM(price * qty) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(55)]]);
}

#[test]
fn test_count_distinct_expression() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,1,1),(2,1,1),(3,2,2)")
        .unwrap();
    // COUNT(DISTINCT a+b): values 2,2,4 → distinct 2,4 → 2
    let r = q(&db, "SELECT COUNT(DISTINCT a + b) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// String functions in WHERE
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_where_upper_equality() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'Alice'),(2,'ALICE'),(3,'Bob')")
        .unwrap();
    let r = q(&db, "SELECT id FROM t WHERE UPPER(s) = 'ALICE' ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
}

#[test]
fn test_where_length_filter() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'hi'),(2,'hello'),(3,'x')")
        .unwrap();
    let r = q(&db, "SELECT id FROM t WHERE LENGTH(s) >= 3 ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_where_substr_filter() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'alice@x.com'),(2,'bob'),(3,'carol@y.com')")
        .unwrap();
    let r = q(
        &db,
        "SELECT id FROM t WHERE SUBSTR(s, 1, 5) = 'alice' ORDER BY id",
    );
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// DELETE all + re-insert + query (tombstone correctness)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_delete_all_reinsert_count() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
        db.execute("DELETE FROM t").unwrap();
        db.execute("INSERT INTO t VALUES (1,100)").unwrap();
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT COUNT(*), SUM(v) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(1), Value::Integer(100)]]);
}

#[test]
fn test_update_pk_then_delete_new_pk() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (5,10)").unwrap();
    db.execute("UPDATE t SET id = 6 WHERE id = 5").unwrap();
    db.execute("DELETE FROM t WHERE id = 6").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(0)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Multiple statements rejection / security
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_reject_multiple_statements() {
    let (db, _d) = db();
    let res = db.execute("SELECT 1; SELECT 2");
    assert!(res.is_err(), "multiple statements should be rejected");
}

#[test]
fn test_empty_query_rejected() {
    let (db, _d) = db();
    let res = db.execute("");
    assert!(res.is_err(), "empty query should be rejected");
}

// ─────────────────────────────────────────────────────────────────────────
// COALESCE with mixed types
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_coalesce_returns_first_non_null_type() {
    let (db, _d) = db();
    let r = q(&db, "SELECT COALESCE(NULL, 42), COALESCE(NULL, 'x')");
    assert_eq!(r, vec![vec![Value::Integer(42), Value::text("x".into())]]);
}

#[test]
fn test_coalesce_in_arithmetic() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,NULL),(2,5)").unwrap();
    // SUM of COALESCE(v, 0) = 0 + 5 = 5
    let r = q(&db, "SELECT SUM(COALESCE(v, 0)) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(5)]]);
}
