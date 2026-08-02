//! Bug Hunt v66 — thirteenth round: complex WHERE, ORDER BY with NULL, persist stress.

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
// Complex WHERE (AND/OR + functions + comparisons)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_where_complex_and_or() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT, c TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10,5,'x'),(2,20,5,'y'),(3,10,15,'x'),(4,30,5,'z')").unwrap();
    // (a > 5 AND b = 5) OR c = 'x'
    // id1: a10>5,b5 ✓ ; id2: a20>5,b5 ✓ ; id3: c='x' ✓ ; id4: a30>5,b5 ✓
    let r = q(&db, "SELECT id FROM t WHERE (a > 5 AND b = 5) OR c = 'x' ORDER BY id");
    assert_eq!(
        r.iter().map(|row| match &row[0] { Value::Integer(i) => *i, _ => -1 }).collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
}

#[test]
fn test_where_function_and_comparison() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'alice',30),(2,'bob',15),(3,'carol',40)").unwrap();
    // LENGTH(s) > 3 AND v > 20
    let r = q(&db, "SELECT id FROM t WHERE LENGTH(s) > 3 AND v > 20 ORDER BY id");
    // alice(5>3,30>20)✓ ; bob(3 not >3)✗ ; carol(5>3,40>20)✓
    assert_eq!(
        r.iter().map(|row| match &row[0] { Value::Integer(i) => *i, _ => -1 }).collect::<Vec<_>>(),
        vec![1, 3]
    );
}

#[test]
fn test_where_nested_parens() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,1,1),(2,1,2),(3,2,1),(4,2,2)").unwrap();
    // ((a = 1 AND b = 1) OR (a = 2 AND b = 2))
    let r = q(&db, "SELECT id FROM t WHERE ((a = 1 AND b = 1) OR (a = 2 AND b = 2)) ORDER BY id");
    assert_eq!(
        r.iter().map(|row| match &row[0] { Value::Integer(i) => *i, _ => -1 }).collect::<Vec<_>>(),
        vec![1, 4]
    );
}

#[test]
fn test_where_not_with_parens() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,1,1),(2,1,2),(3,2,1)").unwrap();
    // NOT (a = 1 AND b = 1) → ids 2, 3
    let r = q(&db, "SELECT id FROM t WHERE NOT (a = 1 AND b = 1) ORDER BY id");
    assert_eq!(
        r.iter().map(|row| match &row[0] { Value::Integer(i) => *i, _ => -1 }).collect::<Vec<_>>(),
        vec![2, 3]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY with NULLs + multiple columns
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_order_by_null_then_value() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, g INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,1,NULL),(2,1,10),(3,2,NULL),(4,2,5)").unwrap();
    // ORDER BY g, v → NULLs first within each g
    let r = q(&db, "SELECT id FROM t ORDER BY g, v");
    // g1: NULL(id1), 10(id2); g2: NULL(id3), 5(id4)
    assert_eq!(
        r.iter().map(|row| match &row[0] { Value::Integer(i) => *i, _ => -1 }).collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
}

#[test]
fn test_order_by_mixed_types_consistency() {
    // ORDER BY should be stable for a text column.
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'banana'),(2,'apple'),(3,'cherry'),(4,'apple')").unwrap();
    let r = q(&db, "SELECT s FROM t ORDER BY s");
    assert_eq!(
        r,
        vec![
            vec![Value::text("apple".into())],
            vec![Value::text("apple".into())],
            vec![Value::text("banana".into())],
            vec![Value::text("cherry".into())],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Persistence stress: many operations, reopen, verify
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_persist_mixed_ops_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT, s TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')").unwrap();
        db.execute("UPDATE t SET v = v + 100 WHERE id <= 2").unwrap();
        db.execute("DELETE FROM t WHERE s = 'c'").unwrap();
        db.execute("INSERT INTO t VALUES (4,40,'d')").unwrap();
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT id, v, s FROM t ORDER BY id");
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(110), Value::text("a".into())],
            vec![Value::Integer(2), Value::Integer(120), Value::text("b".into())],
            vec![Value::Integer(4), Value::Integer(40), Value::text("d".into())],
        ]
    );
}

#[test]
fn test_persist_multiple_checkpoints() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1,10)").unwrap();
        db.checkpoint().unwrap();
        db.execute("INSERT INTO t VALUES (2,20)").unwrap();
        db.checkpoint().unwrap();
        db.execute("UPDATE t SET v = 99 WHERE id = 1").unwrap();
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(
        r,
        vec![vec![Value::Integer(1), Value::Integer(99)], vec![Value::Integer(2), Value::Integer(20)]]
    );
}

#[test]
fn test_index_survives_reopen_with_updates() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1,'a'),(2,'b')").unwrap();
        db.execute("CREATE INDEX idx_cat ON t(cat)").unwrap();
        db.execute("INSERT INTO t VALUES (3,'a')").unwrap();
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    // Index should still find both 'a' rows.
    let r = q(&db, "SELECT id FROM t WHERE cat = 'a' ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate over filtered JOIN
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_join_filter_aggregate() {
    let (db, _d) = db();
    db.execute("CREATE TABLE orders(id INT PRIMARY KEY, cust INT, amt INT, status TEXT)").unwrap();
    db.execute("CREATE TABLE cust(id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO orders VALUES (1,1,100,'ok'),(2,1,200,'pending'),(3,2,50,'ok')").unwrap();
    db.execute("INSERT INTO cust VALUES (1,'Alice'),(2,'Bob')").unwrap();
    // Total of 'ok' orders per customer.
    let r = q(
        &db,
        "SELECT c.name, SUM(o.amt) FROM cust c JOIN orders o ON c.id = o.cust WHERE o.status = 'ok' GROUP BY c.name ORDER BY c.name",
    );
    // Alice: 100 (ok only); Bob: 50
    assert_eq!(
        r,
        vec![vec![Value::text("Alice".into()), Value::Integer(100)], vec![Value::text("Bob".into()), Value::Integer(50)]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// COUNT(*) with GROUP BY and ORDER BY count
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_groupby_count_orderby_count() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(g INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(1),(1),(2),(2),(3)").unwrap();
    let r = q(&db, "SELECT g, COUNT(*) AS c FROM t GROUP BY g ORDER BY c DESC, g ASC");
    // g1:3, g2:2, g3:1 → desc by count: 3,2,1
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(3)],
            vec![Value::Integer(2), Value::Integer(2)],
            vec![Value::Integer(3), Value::Integer(1)],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// String functions chained
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_chained_string_funcs() {
    let (db, _d) = db();
    let r = q(&db, "SELECT UPPER(SUBSTR('hello world', 1, 5))");
    assert_eq!(r, vec![vec![Value::text("HELLO".into())]]);
}

#[test]
fn test_length_of_function_result() {
    let (db, _d) = db();
    let r = q(&db, "SELECT LENGTH(UPPER('abc'))");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

#[test]
fn test_replace_then_upper() {
    let (db, _d) = db();
    let r = q(&db, "SELECT UPPER(REPLACE('a-b-c', '-', ' '))");
    assert_eq!(r, vec![vec![Value::text("A B C".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Nested arithmetic in SELECT
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_nested_arithmetic() {
    let (db, _d) = db();
    let r = q(&db, "SELECT ((2 + 3) * 4 - 1) / 2");
    // (5*4-1)/2 = 19/2 = 9 (integer division)
    assert_eq!(r, vec![vec![Value::Integer(9)]]);
}

#[test]
fn test_arithmetic_with_columns_complex() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT, c INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10,5,2)").unwrap();
    // (a + b) * c - a = 15*2 - 10 = 20
    let r = q(&db, "SELECT (a + b) * c - a FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(20)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// DISTINCT with expression
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_distinct_expression() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,1),(2,1),(3,2),(4,2)").unwrap();
    // DISTINCT v * 10 → 10, 20
    let r = q(&db, "SELECT DISTINCT v * 10 FROM t ORDER BY v * 10");
    assert_eq!(
        r,
        vec![vec![Value::Integer(10)], vec![Value::Integer(20)]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Empty result aggregate
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_aggregate_empty_where_result() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    let r = q(&db, "SELECT COUNT(*), SUM(v), AVG(v), MIN(v), MAX(v) FROM t WHERE v > 100");
    assert_eq!(
        r,
        vec![vec![Value::Integer(0), Value::Null, Value::Null, Value::Null, Value::Null]]
    );
}
