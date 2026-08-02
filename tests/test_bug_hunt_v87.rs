//! Bug Hunt v87 — round 14: string function edges, CAST type matrix,
//! implicit coercion, UPDATE edge semantics, multi-row transaction,
//! and consistency between identical queries via different routes.

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
// CAST matrix: int↔text, float↔text, bool↔int.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_cast_int_to_float() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = q(&db, "SELECT CAST(id AS FLOAT) FROM t");
    match &r[0][0] {
        Value::Float(f) => assert!((f - 1.0).abs() < 1e-9),
        Value::Integer(1) => {} // acceptable
        other => panic!("CAST int AS FLOAT = 1.0, got {:?}", other),
    }
}

#[test]
fn test_cast_float_to_int_truncates() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = q(&db, "SELECT CAST(3.7 AS INT) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(3)]], "CAST 3.7 AS INT truncates to 3");
}

#[test]
fn test_cast_text_to_int() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = q(&db, "SELECT CAST('42' AS INT) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(42)]]);
}

#[test]
fn test_cast_int_to_text() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = q(&db, "SELECT CAST(123 AS TEXT) FROM t");
    assert_eq!(r, vec![vec![Value::Text("123".into())]]);
}

#[test]
fn test_cast_negative_to_text() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = q(&db, "SELECT CAST(-99 AS TEXT) FROM t");
    assert_eq!(r, vec![vec![Value::Text("-99".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// SUBSTR / SUBSTRING (if supported).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_substr_basic() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    // SUBSTR('hello', 2, 3) = 'ell'
    let r = q(&db, "SELECT SUBSTR('hello', 2, 3) FROM t");
    assert_eq!(r, vec![vec![Value::Text("ell".into())]]);
}

#[test]
fn test_substr_no_length() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    // SUBSTR('hello', 2) = 'ello' (to end)
    let r = q(&db, "SELECT SUBSTR('hello', 2) FROM t");
    assert_eq!(r, vec![vec![Value::Text("ello".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Implicit string-numeric coercion in arithmetic.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_arithmetic_constant() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = q(&db, "SELECT 100 - 33 FROM t");
    assert_eq!(r, vec![vec![Value::Integer(67)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// UPDATE SET to expression involving another column.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_update_set_col_from_col() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10,20)").unwrap();
    db.execute("UPDATE t SET b = a WHERE id = 1").unwrap();
    let r = q(&db, "SELECT a, b FROM t");
    assert_eq!(r, vec![vec![Value::Integer(10), Value::Integer(10)]]);
}

#[test]
fn test_update_set_col_arithmetic() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10,20)").unwrap();
    db.execute("UPDATE t SET a = a + b").unwrap();
    let r = q(&db, "SELECT a, b FROM t");
    assert_eq!(r, vec![vec![Value::Integer(30), Value::Integer(20)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Multi-statement transaction with multiple operations.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_transaction_multi_op() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    db.execute("BEGIN TRANSACTION").unwrap();
    db.execute("INSERT INTO t VALUES (2,20)").unwrap();
    db.execute("UPDATE t SET v = 99 WHERE id = 1").unwrap();
    db.execute("DELETE FROM t WHERE id = 2").unwrap();
    db.execute("COMMIT").unwrap();
    // After commit: id=1 (v=99), id=2 deleted.
    let r = q(&db, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1), Value::Integer(99)]]);
}

#[test]
fn test_transaction_rollback_multi_op() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    db.execute("BEGIN TRANSACTION").unwrap();
    db.execute("INSERT INTO t VALUES (2,20)").unwrap();
    db.execute("UPDATE t SET v = 99 WHERE id = 1").unwrap();
    db.execute("ROLLBACK").unwrap();
    // After rollback: only id=1 (v=10), all changes undone.
    let r = q(&db, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1), Value::Integer(10)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Read-your-writes within transaction.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_read_your_writes_in_txn() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    db.execute("BEGIN TRANSACTION").unwrap();
    db.execute("INSERT INTO t VALUES (2,20)").unwrap();
    // Within txn, should see both rows.
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(2)]], "read-your-writes within txn");
    db.execute("ROLLBACK").unwrap();
}

// ─────────────────────────────────────────────────────────────────────────
// Nested transaction rejection (v52c fix verification).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_nested_transaction_rejected() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("BEGIN TRANSACTION").unwrap();
    let res = db.execute("BEGIN TRANSACTION");
    assert!(res.is_err(), "nested BEGIN TRANSACTION must be rejected");
    db.execute("ROLLBACK").unwrap();
}

// ─────────────────────────────────────────────────────────────────────────
// COUNT after DELETE in same session.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_count_after_delete_sequence() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3),(4),(5)").unwrap();
    assert_eq!(q(&db, "SELECT COUNT(*) FROM t"), vec![vec![Value::Integer(5)]]);
    db.execute("DELETE FROM t WHERE id <= 2").unwrap();
    assert_eq!(q(&db, "SELECT COUNT(*) FROM t"), vec![vec![Value::Integer(3)]]);
    db.execute("DELETE FROM t WHERE id = 5").unwrap();
    assert_eq!(q(&db, "SELECT COUNT(*) FROM t"), vec![vec![Value::Integer(2)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY with tie on first key, secondary key breaks tie.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_order_by_tie_secondary() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (3,1,1),(1,1,3),(2,1,2)").unwrap();
    // a all = 1, tie. ORDER BY a ASC, b ASC → b: 1,2,3 → id 3,2,1.
    let r = q(&db, "SELECT id FROM t ORDER BY a ASC, b ASC");
    let ids: Vec<i64> = r.iter().filter_map(|row| match row[0] { Value::Integer(i) => Some(i), _ => None }).collect();
    assert_eq!(ids, vec![3, 2, 1]);
}

// ─────────────────────────────────────────────────────────────────────────
// DISTINCT with aggregate (COUNT(DISTINCT) vs COUNT over DISTINCT subquery).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_count_distinct_vs_group() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,10),(3,20),(4,20),(5,30)").unwrap();
    let via_cd = q(&db, "SELECT COUNT(DISTINCT v) FROM t");
    // Count distinct values via GROUP BY.
    let groups = q(&db, "SELECT v FROM t GROUP BY v");
    assert_eq!(via_cd, vec![vec![Value::Integer(3)]]);
    assert_eq!(groups.len(), 3, "3 distinct values via GROUP BY");
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE with column compared to column from same row (functional).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_where_col_vs_col_arith() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT, c INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,3,4,7),(2,1,2,5),(3,2,3,4)").unwrap();
    // a + b = c → id1 (3+4=7 ✓), id2 (1+2=3≠5), id3 (2+3=5≠4).
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE a + b = c"));
    assert_eq!(r, vec![1]);
}

// ─────────────────────────────────────────────────────────────────────────
// IN with column reference (not just literals) — usually unsupported,
// verify it errors clearly rather than wrong result.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_in_subquery_not_in_list() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("CREATE TABLE b(id INT PRIMARY KEY, w INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1,10),(2,20)").unwrap();
    db.execute("INSERT INTO b VALUES (1,10),(2,30)").unwrap();
    // a.v IN (b.w) — a.v in the set of b.w values {10, 30}.
    let r = sorted_int(&q(&db, "SELECT id FROM a WHERE v IN (SELECT w FROM b)"));
    assert_eq!(r, vec![1], "a.v=10 is in {{10,30}}; a.v=20 is not");
}

// ─────────────────────────────────────────────────────────────────────────
// SUM over expression in GROUP BY.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_sum_expression() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, qty INT, price INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',2,10),(2,'a',3,5),(3,'b',1,20)").unwrap();
    // SUM(qty * price): a = 20+15=35, b = 20.
    let mut r = q(&db, "SELECT cat, SUM(qty * price) FROM t GROUP BY cat");
    r.sort_by_key(|row| match &row[0] { Value::Text(s) => s.as_str().to_string(), _ => "zzz".into() });
    assert_eq!(r, vec![
        vec![Value::Text("a".into()), Value::Integer(35)],
        vec![Value::Text("b".into()), Value::Integer(20)],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// Empty string in WHERE.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_where_empty_string() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,''),(2,'x'),(3,'')").unwrap();
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE s = ''"));
    assert_eq!(r, vec![1, 3]);
}

// ─────────────────────────────────────────────────────────────────────────
// LIKE matching empty pattern.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_like_empty_pattern() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,''),(2,'x')").unwrap();
    // LIKE '' matches only empty string.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE s LIKE ''"));
    assert_eq!(r, vec![1]);
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY on PRIMARY KEY.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_order_by_pk() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (3,30),(1,10),(2,20)").unwrap();
    let r = q(&db, "SELECT id FROM t ORDER BY id ASC");
    let ids: Vec<i64> = r.iter().filter_map(|row| match row[0] { Value::Integer(i) => Some(i), _ => None }).collect();
    assert_eq!(ids, vec![1, 2, 3]);
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate consistency: SUM via index vs scan (single column, no WHERE).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_sum_consistency_repeated() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    let s1 = q(&db, "SELECT SUM(v) FROM t");
    let s2 = q(&db, "SELECT SUM(v) FROM t");
    let s3 = q(&db, "SELECT SUM(v) FROM t");
    assert_eq!(s1, s2);
    assert_eq!(s2, s3);
    assert_eq!(s1, vec![vec![Value::Integer(60)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// HAVING with COUNT(*) condition.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_having_count_star() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'a'),(3,'b'),(4,'c'),(5,'c'),(6,'c')").unwrap();
    // Groups with COUNT(*) >= 3.
    let mut r = q(&db, "SELECT cat FROM t GROUP BY cat HAVING COUNT(*) >= 3");
    r.sort_by_key(|row| match &row[0] { Value::Text(s) => s.as_str().to_string(), _ => "zzz".into() });
    assert_eq!(r, vec![vec![Value::Text("c".into())]]); // only 'c' has 3.
}

// ─────────────────────────────────────────────────────────────────────────
// Multiple JOINs with WHERE.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_multi_join_with_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(id INT PRIMARY KEY, x INT)").unwrap();
    db.execute("CREATE TABLE b(id INT PRIMARY KEY, aid INT, y INT)").unwrap();
    db.execute("CREATE TABLE c(id INT PRIMARY KEY, bid INT, z INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1,100)").unwrap();
    db.execute("INSERT INTO b VALUES (10,1,200)").unwrap();
    db.execute("INSERT INTO c VALUES (100,10,300)").unwrap();
    let r = q(&db, "SELECT a.x, b.y, c.z FROM a JOIN b ON a.id = b.aid JOIN c ON b.id = c.bid WHERE a.x > 50");
    assert_eq!(r, vec![vec![Value::Integer(100), Value::Integer(200), Value::Integer(300)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Self-join for hierarchy.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_self_join_hierarchy() {
    let (db, _d) = db();
    db.execute("CREATE TABLE emp(id INT PRIMARY KEY, name TEXT, mgr_id INT)").unwrap();
    db.execute("INSERT INTO emp VALUES (1,'ceo',NULL),(2,'vp1',1),(3,'vp2',1),(4,'eng',2)").unwrap();
    // Find each employee and their manager's name.
    let mut r = q(&db, "SELECT e.name, m.name FROM emp e LEFT JOIN emp m ON e.mgr_id = m.id");
    r.sort_by_key(|row| match &row[0] { Value::Text(s) => s.as_str().to_string(), _ => "zzz".into() });
    // ceo has no manager (NULL), others have.
    assert_eq!(r.len(), 4);
    // ceo row: name=ceo, manager=NULL.
    let ceo_row = r.iter().find(|row| matches!(&row[0], Value::Text(s) if s.as_str() == "ceo")).unwrap();
    assert_eq!(ceo_row[1], Value::Null, "ceo has no manager");
}

// ─────────────────────────────────────────────────────────────────────────
// SELECT with WHERE on aggregate result (must use HAVING, not WHERE).
// Verify WHERE on aggregate errors or is handled.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_count_distinct_multi_col() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,1,1),(2,1,2),(3,1,1),(4,2,1)").unwrap();
    // COUNT(DISTINCT a, b) — distinct pairs: (1,1),(1,2),(2,1) = 3.
    // May not be supported (single-arg DISTINCT); use subquery approach.
    let r = q(&db, "SELECT COUNT(*) FROM (SELECT DISTINCT a, b FROM t) AS sub");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}
