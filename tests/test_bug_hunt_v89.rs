//! Bug Hunt v89 — round 16: index consistency, boundary numerics, multi-key
//! interactions, floating-point edges, large-value round-trip, and
//! write-then-read consistency under various patterns.

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
// Index consistency after UPDATE on indexed column.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_indexed_update_moves_entry() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b')").unwrap();
    db.execute("CREATE INDEX idx_cat ON t(cat)").unwrap();
    // Wait for index build.
    db.wait_for_indexes_ready();
    // Update id=2's cat from 'b' to 'a'.
    db.execute("UPDATE t SET cat = 'a' WHERE id = 2").unwrap();
    // Now both rows have cat='a'.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE cat = 'a'"));
    assert_eq!(r, vec![1, 2], "both rows should match after UPDATE");
    // Old value 'b' should have no rows.
    let r2 = q(&db, "SELECT id FROM t WHERE cat = 'b'");
    assert_eq!(r2, vec![] as Vec<Vec<Value>>, "old value gone after UPDATE");
}

#[test]
fn test_indexed_delete_removes_entry() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'a'),(3,'b')").unwrap();
    db.execute("CREATE INDEX idx_cat ON t(cat)").unwrap();
    db.wait_for_indexes_ready();
    db.execute("DELETE FROM t WHERE id = 1").unwrap();
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE cat = 'a'"));
    assert_eq!(r, vec![2], "deleted row gone from index");
}

#[test]
fn test_indexed_insert_visible() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a')").unwrap();
    db.execute("CREATE INDEX idx_cat ON t(cat)").unwrap();
    db.wait_for_indexes_ready();
    db.execute("INSERT INTO t VALUES (2,'a')").unwrap();
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE cat = 'a'"));
    assert_eq!(r, vec![1, 2], "new row visible via index");
}

// ─────────────────────────────────────────────────────────────────────────
// Boundary integer comparisons.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_boundary_int_max_comparison() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 9223372036854775807)").unwrap(); // i64::MAX
    let r = q(&db, "SELECT id FROM t WHERE v >= 9223372036854775807");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_boundary_int_min_comparison() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, -9223372036854775808)").unwrap(); // i64::MIN
    let r = q(&db, "SELECT id FROM t WHERE v <= -9223372036854775808");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_int_overflow_wraps_safely() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    // i64::MAX + 1 — should not silently wrap (error or promote to float).
    let res = db.execute("SELECT 9223372036854775807 + 1 FROM t").and_then(|s| s.materialize());
    match res {
        Ok(r) => {
            let r = rows(r);
            // Accept either Float (promoted) or error already handled. Just verify no silent wrap to negative.
            if let Some(Value::Integer(i)) = r.get(0).and_then(|row| row.get(0)) {
                assert!(*i > 0, "must not silently wrap to negative; got {}", i);
            }
        }
        Err(_) => {} // error is acceptable
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Floating-point edges.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_float_very_small() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 0.0001)").unwrap();
    let r = q(&db, "SELECT v FROM t WHERE v > 0.00005");
    assert_eq!(r.len(), 1);
}

#[test]
fn test_float_very_large() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 1e15)").unwrap();
    let r = q(&db, "SELECT v FROM t");
    match &r[0][0] {
        Value::Float(f) => assert!((f - 1e15).abs() < 1e6, "1e15 round-trip, got {}", f),
        other => panic!("1e15 expected Float, got {:?}", other),
    }
}

#[test]
fn test_float_negative_zero() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, -0.0)").unwrap();
    let r = q(&db, "SELECT v FROM t");
    assert_eq!(r.len(), 1);
    // -0.0 == 0.0 in float comparison.
    let r2 = q(&db, "SELECT id FROM t WHERE v = 0.0");
    assert_eq!(r2, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_sum_float_precision_many() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)").unwrap();
    for i in 1..=10 {
        db.execute(&format!("INSERT INTO t VALUES ({}, 0.1)", i)).unwrap();
    }
    let r = q(&db, "SELECT SUM(v) FROM t");
    match &r[0][0] {
        Value::Float(f) => assert!((f - 1.0).abs() < 1e-6, "10×0.1 ≈ 1.0, got {}", f),
        other => panic!("SUM 10×0.1 = 1.0, got {:?}", other),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Large TEXT value round-trip.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_large_text_roundtrip() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    let big = "x".repeat(1000);
    db.execute(&format!("INSERT INTO t VALUES (1, '{}')", big)).unwrap();
    let r = q(&db, "SELECT s FROM t");
    assert_eq!(r, vec![vec![Value::Text(big.into())]]);
}

#[test]
fn test_text_with_newlines() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    // Insert text containing newlines (SQL string literal can span lines).
    db.execute("INSERT INTO t VALUES (1, 'line1\nline2')").unwrap();
    let r = q(&db, "SELECT LENGTH(s) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(11)]]); // 'line1' + \n + 'line2' = 11
}

// ─────────────────────────────────────────────────────────────────────────
// GROUP BY + ORDER BY + LIMIT combo.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_order_limit_combo() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',15),(5,'c',30)").unwrap();
    // Top-2 groups by SUM(v) DESC.
    let r = q(&db, "SELECT cat, SUM(v) FROM t GROUP BY cat ORDER BY SUM(v) DESC LIMIT 2");
    // sums: a=30, b=20, c=30. DESC: a/c (30), then b (20). LIMIT 2.
    assert_eq!(r.len(), 2);
    // Both top groups should have sum 30.
    let sums: Vec<i64> = r.iter().filter_map(|row| match row.get(1) { Some(Value::Integer(i)) => Some(*i), _ => None }).collect();
    assert!(sums.iter().all(|&s| s == 30), "top-2 should both be sum=30; got {:?}", sums);
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE with compound AND of 3+ conditions.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_where_three_and() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT, c INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,1,2,3),(2,1,2,4),(3,1,5,3),(4,2,2,3)").unwrap();
    // a=1 AND b=2 AND c=3 → id1 only.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE a = 1 AND b = 2 AND c = 3"));
    assert_eq!(r, vec![1]);
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE with compound OR of 3+ conditions.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_where_three_or() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)").unwrap();
    // v=10 OR v=30 OR v=50 → id1, id3.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v = 10 OR v = 30 OR v = 50"));
    assert_eq!(r, vec![1, 3]);
}

// ─────────────────────────────────────────────────────────────────────────
// Mixed AND/OR with parens.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_mixed_and_or_parens() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,1,10),(2,1,20),(3,2,10),(4,2,20)").unwrap();
    // (a=1 AND b=10) OR (a=2 AND b=20) → id1, id4.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE (a = 1 AND b = 10) OR (a = 2 AND b = 20)"));
    assert_eq!(r, vec![1, 4]);
}

// ─────────────────────────────────────────────────────────────────────────
// COUNT with GROUP BY and HAVING and ORDER BY and LIMIT (full combo).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_full_group_combo() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'a'),(3,'b'),(4,'b'),(5,'b'),(6,'c'),(7,'c'),(8,'c'),(9,'c')").unwrap();
    // Groups with COUNT >= 2, ordered by COUNT DESC, LIMIT 2.
    let r = q(&db, "SELECT cat, COUNT(*) FROM t GROUP BY cat HAVING COUNT(*) >= 2 ORDER BY COUNT(*) DESC LIMIT 2");
    // counts: a=2, b=3, c=4. DESC: c(4), b(3). LIMIT 2 → c, b.
    let cats: Vec<String> = r.iter().filter_map(|row| match &row[0] { Value::Text(s) => Some(s.as_str().to_string()), _ => None }).collect();
    assert_eq!(cats, vec!["c".to_string(), "b".to_string()]);
}

// ─────────────────────────────────────────────────────────────────────────
// Self-join with aggregate.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_self_join_aggregate() {
    let (db, _d) = db();
    db.execute("CREATE TABLE emp(id INT PRIMARY KEY, dept TEXT, salary INT)").unwrap();
    db.execute("INSERT INTO emp VALUES (1,'eng',100),(2,'eng',200),(3,'sales',150)").unwrap();
    // Find employees earning more than dept average.
    // eng avg=150 → id2 (200>150). sales avg=150 → none.
    let r = sorted_int(&q(&db, "SELECT e.id FROM emp e WHERE e.salary > (SELECT AVG(e2.salary) FROM emp e2 WHERE e2.dept = e.dept)"));
    assert_eq!(r, vec![2]);
}

// ─────────────────────────────────────────────────────────────────────────
// INSERT many rows in one statement.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_insert_many_rows_one_stmt() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40),(5,50)").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(5)]]);
    let sumr = q(&db, "SELECT SUM(v) FROM t");
    assert_eq!(sumr, vec![vec![Value::Integer(150)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// SELECT with WHERE on PK equality (point query).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_pk_point_query() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 2");
    assert_eq!(r, vec![vec![Value::Integer(20)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// UPDATE WHERE on PK.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_update_where_pk() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    db.execute("UPDATE t SET v = 99 WHERE id = 2").unwrap();
    let r = q(&db, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(r, vec![
        vec![Value::Integer(1), Value::Integer(10)],
        vec![Value::Integer(2), Value::Integer(99)],
        vec![Value::Integer(3), Value::Integer(30)],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// DELETE WHERE on PK.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_delete_where_pk() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    db.execute("DELETE FROM t WHERE id = 2").unwrap();
    let r = q(&db, "SELECT id FROM t ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// NULL in ORDER BY ASC and DESC (consistency).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_null_order_asc_desc_consistency() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL),(3,30),(4,NULL)").unwrap();
    let asc = q(&db, "SELECT id FROM t ORDER BY v ASC, id ASC");
    let desc = q(&db, "SELECT id FROM t ORDER BY v DESC, id DESC");
    // Both should return all 4 rows.
    assert_eq!(asc.len(), 4);
    assert_eq!(desc.len(), 4);
    // Non-NULL rows must be in correct relative order in each.
    let asc_ids: Vec<i64> = asc.iter().filter_map(|r| match r[0] { Value::Integer(i) => Some(i), _ => None }).collect();
    let desc_ids: Vec<i64> = desc.iter().filter_map(|r| match r[0] { Value::Integer(i) => Some(i), _ => None }).collect();
    // ASC non-NULL: v=10(id1) before v=30(id3).
    assert!(asc_ids.iter().position(|&i| i == 1).unwrap() < asc_ids.iter().position(|&i| i == 3).unwrap());
    // DESC non-NULL: v=30(id3) before v=10(id1).
    assert!(desc_ids.iter().position(|&i| i == 3).unwrap() < desc_ids.iter().position(|&i| i == 1).unwrap());
}

// ─────────────────────────────────────────────────────────────────────────
// Repeated LIKE with different patterns.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_like_various_patterns() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'hello world'),(2,'world hello'),(3,'hello'),(4,'world')").unwrap();
    // Prefix.
    assert_eq!(sorted_int(&q(&db, "SELECT id FROM t WHERE s LIKE 'hello%'")), vec![1, 3]);
    // Suffix.
    assert_eq!(sorted_int(&q(&db, "SELECT id FROM t WHERE s LIKE '%world'")), vec![1, 4]);
    // Contains.
    assert_eq!(sorted_int(&q(&db, "SELECT id FROM t WHERE s LIKE '%hello%'")), vec![1, 2, 3]);
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate over filtered set with index.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_aggregate_filtered_indexed() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',30)").unwrap();
    db.execute("CREATE INDEX idx_cat ON t(cat)").unwrap();
    db.wait_for_indexes_ready();
    let r = q(&db, "SELECT SUM(v) FROM t WHERE cat = 'a'");
    assert_eq!(r, vec![vec![Value::Integer(30)]]);
}
