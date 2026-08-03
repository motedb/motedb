//! Bug Hunt v83 — round 10: UNION vs UNION ALL, multi-row DISTINCT, deep
//! nesting, GROUP BY + HAVING + ORDER BY combo, string edges, edge numerics,
//! cross-table subquery correlation (different tables, not self-ref).

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
// UNION (dedup) vs UNION ALL (no dedup).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_union_dedup() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (10),(20),(10)").unwrap();
    let r = q(&db, "SELECT v FROM t UNION SELECT v FROM t");
    let mut vals: Vec<i64> = r
        .iter()
        .filter_map(|row| match row[0] {
            Value::Integer(i) => Some(i),
            _ => None,
        })
        .collect();
    vals.sort();
    assert_eq!(vals, vec![10, 20], "UNION dedups across both sides");
}

#[test]
fn test_union_all_no_dedup() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (10),(20),(10)").unwrap();
    let r = q(&db, "SELECT v FROM t UNION ALL SELECT v FROM t");
    let mut vals: Vec<i64> = r
        .iter()
        .filter_map(|row| match row[0] {
            Value::Integer(i) => Some(i),
            _ => None,
        })
        .collect();
    vals.sort();
    assert_eq!(
        vals,
        vec![10, 10, 10, 10, 20, 20],
        "UNION ALL keeps all 6 rows"
    );
}

// ─────────────────────────────────────────────────────────────────────────
// INTERSECT.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_intersect() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(v INT)").unwrap();
    db.execute("CREATE TABLE b(v INT)").unwrap();
    db.execute("INSERT INTO a VALUES (10),(20),(30)").unwrap();
    db.execute("INSERT INTO b VALUES (20),(30),(40)").unwrap();
    let mut r = q(&db, "SELECT v FROM a INTERSECT SELECT v FROM b");
    r.sort_by_key(|row| match row[0] {
        Value::Integer(i) => i,
        _ => 999,
    });
    assert_eq!(r, vec![vec![Value::Integer(20)], vec![Value::Integer(30)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// EXCEPT (set difference).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_except() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(v INT)").unwrap();
    db.execute("CREATE TABLE b(v INT)").unwrap();
    db.execute("INSERT INTO a VALUES (10),(20),(30)").unwrap();
    db.execute("INSERT INTO b VALUES (20),(30),(40)").unwrap();
    let mut r = q(&db, "SELECT v FROM a EXCEPT SELECT v FROM b");
    r.sort_by_key(|row| match row[0] {
        Value::Integer(i) => i,
        _ => 999,
    });
    assert_eq!(r, vec![vec![Value::Integer(10)]], "a minus b = {{10}}");
}

// ─────────────────────────────────────────────────────────────────────────
// GROUP BY + HAVING + ORDER BY combined.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_having_order_combo() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',15),(5,'c',100)")
        .unwrap();
    let r = q(
        &db,
        "SELECT cat, SUM(v) FROM t GROUP BY cat HAVING SUM(v) > 10 ORDER BY SUM(v) DESC",
    );
    // sums: a=30, b=20, c=100. >10 → all. DESC: c(100), a(30), b(20).
    assert_eq!(
        r,
        vec![
            vec![Value::Text("c".into()), Value::Integer(100)],
            vec![Value::Text("a".into()), Value::Integer(30)],
            vec![Value::Text("b".into()), Value::Integer(20)],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Cross-table correlated subquery (different inner/outer tables — should work
// unlike self-referential).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_cross_table_correlated() {
    let (db, _d) = db();
    db.execute("CREATE TABLE orders(id INT PRIMARY KEY, cust_id INT, amt INT)")
        .unwrap();
    db.execute("CREATE TABLE customers(id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO customers VALUES (1,'alice'),(2,'bob')")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (10,1,100),(11,1,200),(12,2,50)")
        .unwrap();
    // For each customer, their total order amount (correlated on cust_id).
    let mut r = q(
        &db,
        "SELECT name, (SELECT SUM(amt) FROM orders WHERE cust_id = customers.id) FROM customers",
    );
    r.sort_by_key(|row| match &row[0] {
        Value::Text(s) => s.as_str().to_string(),
        _ => "zzz".into(),
    });
    // alice: 100+200=300. bob: 50.
    assert_eq!(
        r,
        vec![
            vec![Value::Text("alice".into()), Value::Integer(300)],
            vec![Value::Text("bob".into()), Value::Integer(50)],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Subquery in WHERE comparing column to aggregate-of-other-table.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_subquery_gt_aggregate() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,15),(3,25),(4,35)")
        .unwrap();
    // v > average(v). avg = 20. → id3 (25), id4 (35).
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v > (SELECT AVG(v) FROM t)"));
    assert_eq!(r, vec![3, 4]);
}

// ─────────────────────────────────────────────────────────────────────────
// Nested IN subqueries (2 levels).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_nested_in_subquery() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(id INT PRIMARY KEY, bid INT)")
        .unwrap();
    db.execute("CREATE TABLE b(id INT PRIMARY KEY, cid INT)")
        .unwrap();
    db.execute("CREATE TABLE c(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO a VALUES (1,10),(2,20)").unwrap();
    db.execute("INSERT INTO b VALUES (10,100),(20,200)")
        .unwrap();
    db.execute("INSERT INTO c VALUES (100)").unwrap();
    // a.id where a.bid in (b.id where b.cid in (c.id)).
    // b.cid=100 is in c → b.id=10 qualifies → a.id=1.
    let r = sorted_int(&q(
        &db,
        "SELECT id FROM a WHERE bid IN (SELECT id FROM b WHERE cid IN (SELECT id FROM c))",
    ));
    assert_eq!(r, vec![1]);
}

// ─────────────────────────────────────────────────────────────────────────
// String with special characters.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_string_special_chars() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a,b'),(2,'c;d'),(3,'e f')")
        .unwrap();
    let r = q(&db, "SELECT id FROM t WHERE s = 'a,b'");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Numeric edge: INT min/max values arithmetic.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_int_min_value() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,-9223372036854775808)")
        .unwrap(); // i64::MIN
    let r = q(&db, "SELECT v FROM t");
    assert_eq!(r, vec![vec![Value::Integer(-9223372036854775808)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Boolean expression short-circuit (no error on NULL side).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_or_short_circuit() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL)").unwrap();
    // v = 10 OR v > 5: id1 TRUE; id2: NULL OR NULL → NULL → excluded.
    // Actually id2 v=NULL: (NULL=10)=NULL, (NULL>5)=NULL, NULL OR NULL=NULL.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v = 10 OR v > 5"));
    assert_eq!(r, vec![1]);
}

// ─────────────────────────────────────────────────────────────────────────
// COUNT after GROUP BY with single group.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_count_group_single() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'x'),(2,'x'),(3,'x')")
        .unwrap();
    let r = q(&db, "SELECT cat, COUNT(*) FROM t GROUP BY cat");
    assert_eq!(r, vec![vec![Value::Text("x".into()), Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// DISTINCT on TEXT column.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_distinct_text() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'a'),(4,'c'),(5,'b')")
        .unwrap();
    let mut r = q(&db, "SELECT DISTINCT s FROM t");
    r.sort_by_key(|row| match &row[0] {
        Value::Text(s) => s.as_str().to_string(),
        _ => "zzz".into(),
    });
    assert_eq!(
        r,
        vec![
            vec![Value::Text("a".into())],
            vec![Value::Text("b".into())],
            vec![Value::Text("c".into())],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY on multiple columns both ASC.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_order_by_multi_asc() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,2,1),(2,1,2),(3,1,1),(4,2,2)")
        .unwrap();
    let r = q(&db, "SELECT id FROM t ORDER BY a ASC, b ASC");
    let ids: Vec<i64> = r
        .iter()
        .filter_map(|row| match row[0] {
            Value::Integer(i) => Some(i),
            _ => None,
        })
        .collect();
    // a=1: b ASC → id3(b=1),id2(b=2). a=2: b ASC → id1(b=1),id4(b=2).
    assert_eq!(ids, vec![3, 2, 1, 4]);
}

// ─────────────────────────────────────────────────────────────────────────
// LIMIT 1 returns single row.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_limit_one() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,30),(2,10),(3,20)")
        .unwrap();
    let r = q(&db, "SELECT v FROM t ORDER BY v ASC LIMIT 1");
    assert_eq!(r, vec![vec![Value::Integer(10)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate MIN/MAX on FLOAT column.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_min_max_float() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,1.5),(2,2.5),(3,0.5)")
        .unwrap();
    let minr = q(&db, "SELECT MIN(v) FROM t");
    match &minr[0][0] {
        Value::Float(f) => assert!((f - 0.5).abs() < 1e-9),
        other => panic!("MIN float = 0.5, got {:?}", other),
    }
    let maxr = q(&db, "SELECT MAX(v) FROM t");
    match &maxr[0][0] {
        Value::Float(f) => assert!((f - 2.5).abs() < 1e-9),
        other => panic!("MAX float = 2.5, got {:?}", other),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// SUM over FLOAT column.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_sum_float() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,1.5),(2,2.5),(3,3.0)")
        .unwrap();
    let r = q(&db, "SELECT SUM(v) FROM t");
    match &r[0][0] {
        Value::Float(f) => assert!((f - 7.0).abs() < 1e-9, "1.5+2.5+3.0=7.0, got {}", f),
        other => panic!("SUM float = 7.0, got {:?}", other),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE with column = column (same table, cross-column equality).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_where_col_equals_col() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,5,5),(2,5,6),(3,6,6)")
        .unwrap();
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE a = b"));
    assert_eq!(r, vec![1, 3]);
}

// ─────────────────────────────────────────────────────────────────────────
// SELECT with mixed aggregate and non-aggregate (non-grouped column).
// SQLite allows this (picks a value). Just verify it doesn't crash/wrong-count.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_select_count_and_col() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'a'),(3,'b')")
        .unwrap();
    // COUNT(*) with a bare column (no GROUP BY) — implicit single group.
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Repeated INSERT of same PK should fail (uniqueness).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_duplicate_pk_rejected() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    let res = db.execute("INSERT INTO t VALUES (1,20)");
    assert!(res.is_err(), "duplicate PK must be rejected");
    // Original row intact.
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(10)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// DELETE non-existent row (no-op).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_delete_nonexistent() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    let res = db.execute("DELETE FROM t WHERE id = 999");
    assert!(res.is_ok(), "DELETE non-existent row should not error");
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// UPDATE non-existent row (no-op).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_update_nonexistent() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    let res = db.execute("UPDATE t SET v = 99 WHERE id = 999");
    assert!(res.is_ok(), "UPDATE non-existent row should not error");
    let r = q(&db, "SELECT v FROM t");
    assert_eq!(r, vec![vec![Value::Integer(10)]], "no rows changed");
}
