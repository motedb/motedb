//! Bug Hunt v68 — fifteenth round: multi-segment agg, txn+index, type/cast corners.

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

fn affected(db: &Database, sql: &str) -> usize {
    match db.execute(sql).unwrap().materialize().unwrap() {
        QueryResult::Modification { affected_rows } => affected_rows,
        _ => 0,
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Multi-segment aggregation (force many flushes → multiple segments)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_multi_segment_count_sum() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    // Insert in batches with checkpoint between to create multiple segments.
    for batch in 0..5 {
        let start = batch * 20 + 1;
        let vals: Vec<String> = (0..20).map(|i| format!("({},{})", start + i, start + i)).collect();
        db.execute(&format!("INSERT INTO t VALUES {}", vals.join(","))).unwrap();
        db.checkpoint().unwrap();
    }
    let r = q(&db, "SELECT COUNT(*), SUM(v), MIN(v), MAX(v) FROM t");
    // 100 rows, v from 1..=100, sum = 5050
    assert_eq!(r, vec![vec![Value::Integer(100), Value::Integer(5050), Value::Integer(1), Value::Integer(100)]]);
}

#[test]
fn test_multi_segment_filtered_agg() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    for batch in 0..3 {
        let start = batch * 10 + 1;
        let cat = if batch % 2 == 0 { "x" } else { "y" };
        let vals: Vec<String> = (0..10).map(|i| format!("({},'{}',{})", start + i, cat, start + i)).collect();
        db.execute(&format!("INSERT INTO t VALUES {}", vals.join(","))).unwrap();
        db.checkpoint().unwrap();
    }
    // cat 'x' = batches 0, 2 = 20 rows (ids 1-10, 21-30). Sum of v = sum(1..10)+sum(21..30) = 55 + 255 = 310
    let r = q(&db, "SELECT COUNT(*), SUM(v) FROM t WHERE cat = 'x'");
    assert_eq!(r, vec![vec![Value::Integer(20), Value::Integer(310)]]);
}

#[test]
fn test_multi_segment_groupby() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(g INT, v INT)").unwrap();
    for batch in 0..4 {
        let g = (batch % 2) + 1;
        let vals: Vec<String> = (0..10).map(|i| format!("({},{})", g, g * 10 + i)).collect();
        db.execute(&format!("INSERT INTO t VALUES {}", vals.join(","))).unwrap();
        db.checkpoint().unwrap();
    }
    // g1 = batches 0,2 = 20 rows; g2 = batches 1,3 = 20 rows
    let r = q(&db, "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY g");
    assert_eq!(r, vec![vec![Value::Integer(1), Value::Integer(20)], vec![Value::Integer(2), Value::Integer(20)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Transaction + index interaction
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_txn_insert_then_indexed_query() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)").unwrap();
    db.execute("CREATE INDEX idx_cat ON t(cat)").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b')").unwrap();
    // Within txn, indexed query should see uncommitted inserts.
    let r = q(&db, "SELECT id FROM t WHERE cat = 'a'");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
    db.execute("ROLLBACK").unwrap();
    let r2 = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r2, vec![vec![Value::Integer(0)]]);
}

#[test]
fn test_txn_update_indexed_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b')").unwrap();
    db.execute("CREATE INDEX idx_cat ON t(cat)").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("UPDATE t SET cat = 'a' WHERE id = 2").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE cat = 'a' ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
    db.execute("ROLLBACK").unwrap();
    let r2 = q(&db, "SELECT id FROM t WHERE cat = 'a'");
    assert_eq!(r2, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_txn_delete_then_count() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("DELETE FROM t WHERE v >= 20").unwrap();
    let r = q(&db, "SELECT COUNT(*), SUM(v) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(1), Value::Integer(10)]]);
    db.execute("ROLLBACK").unwrap();
    let r2 = q(&db, "SELECT COUNT(*), SUM(v) FROM t");
    assert_eq!(r2, vec![vec![Value::Integer(3), Value::Integer(60)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Type-specific WHERE
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_where_float_comparison() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,1.5),(2,2.5),(3,3.5)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE v > 2.0 ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]);
}

#[test]
fn test_where_float_exact_equality() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,2.5),(2,3.5)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE v = 2.5");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_where_bool_equality() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, active BOOLEAN)").unwrap();
    db.execute("INSERT INTO t VALUES (1,TRUE),(2,FALSE),(3,TRUE)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE active = TRUE ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

#[test]
fn test_where_timestamp_range() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, ts TIMESTAMP)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'2024-01-15T00:00:00'),(2,'2024-06-15T00:00:00'),(3,'2024-12-15T00:00:00')").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE ts BETWEEN '2024-03-01' AND '2024-09-01' ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_where_timestamp_in_list() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, ts TIMESTAMP)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'2024-01-15T00:00:00'),(2,'2024-06-15T00:00:00'),(3,'2024-12-15T00:00:00')").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE ts IN ('2024-01-15T00:00:00', '2024-12-15T00:00:00') ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// CAST correctness
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_cast_negative_float_to_int() {
    let (db, _d) = db();
    let r = q(&db, "SELECT CAST(-2.9 AS INTEGER)");
    assert_eq!(r, vec![vec![Value::Integer(-2)]]);
}

#[test]
fn test_cast_text_to_int_negative() {
    let (db, _d) = db();
    let r = q(&db, "SELECT CAST('-42' AS INTEGER)");
    assert_eq!(r, vec![vec![Value::Integer(-42)]]);
}

#[test]
fn test_cast_preserves_in_arithmetic() {
    let (db, _d) = db();
    // CAST(3.7 AS INTEGER) + 1 = 3 + 1 = 4
    let r = q(&db, "SELECT CAST(3.7 AS INTEGER) + 1");
    assert_eq!(r, vec![vec![Value::Integer(4)]]);
}

#[test]
fn test_cast_in_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'10'),(2,'20'),(3,'30')").unwrap();
    // WHERE CAST(s AS INTEGER) > 15 → ids 2, 3
    let r = q(&db, "SELECT id FROM t WHERE CAST(s AS INTEGER) > 15 ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// DELETE affected count with subquery + index
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_delete_affected_with_subquery() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, g INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,1),(2,1),(3,2),(4,2),(5,3)").unwrap();
    db.execute("CREATE TABLE keep(g INT)").unwrap();
    db.execute("INSERT INTO keep VALUES (1),(3)").unwrap();
    let n = affected(&db, "DELETE FROM t WHERE g IN (SELECT g FROM keep)");
    // delete g=1 (2 rows), g=3 (1 row) = 3
    assert_eq!(n, 3);
    let r = q(&db, "SELECT id FROM t ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(3)], vec![Value::Integer(4)]]);
}

#[test]
fn test_update_affected_with_subquery() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    db.execute("CREATE TABLE mark(id INT)").unwrap();
    db.execute("INSERT INTO mark VALUES (1),(3)").unwrap();
    let n = affected(&db, "UPDATE t SET v = 0 WHERE id IN (SELECT id FROM mark)");
    assert_eq!(n, 2);
    let r = q(&db, "SELECT v FROM t ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(0)], vec![Value::Integer(20)], vec![Value::Integer(0)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// GROUP BY with NULL key
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_groupby_null_key() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(g INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(NULL,20),(NULL,30),(1,40)").unwrap();
    let r = q(&db, "SELECT g, COUNT(*), SUM(v) FROM t GROUP BY g ORDER BY g");
    // g=1: 2 rows sum 50; g=NULL: 2 rows sum 50 (NULL is its own group)
    assert_eq!(
        r,
        vec![
            vec![Value::Null, Value::Integer(2), Value::Integer(50)],
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(50)],
        ]
    );
}

#[test]
fn test_groupby_text_key() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES ('a',10),('b',20),('a',30)").unwrap();
    let r = q(&db, "SELECT cat, SUM(v) FROM t GROUP BY cat ORDER BY cat");
    assert_eq!(
        r,
        vec![vec![Value::text("a".into()), Value::Integer(40)], vec![Value::text("b".into()), Value::Integer(20)]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY on multiple types
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_order_by_desc_text() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'apple'),(2,'banana'),(3,'cherry')").unwrap();
    let r = q(&db, "SELECT s FROM t ORDER BY s DESC");
    assert_eq!(
        r,
        vec![
            vec![Value::text("cherry".into())],
            vec![Value::text("banana".into())],
            vec![Value::text("apple".into())],
        ]
    );
}

#[test]
fn test_order_by_timestamp() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, ts TIMESTAMP)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'2024-06-15T00:00:00'),(2,'2024-01-01T00:00:00'),(3,'2024-12-31T00:00:00')").unwrap();
    let r = q(&db, "SELECT id FROM t ORDER BY ts DESC");
    assert_eq!(
        r.iter().map(|row| match &row[0] { Value::Integer(i) => *i, _ => -1 }).collect::<Vec<_>>(),
        vec![3, 1, 2]
    );
}
