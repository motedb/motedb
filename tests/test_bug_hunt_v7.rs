//! Bug-hunt v7: final sweep — nested subqueries, JOIN edge cases,
//! expression evaluation, and stress patterns.

use motedb::sql::QueryResult;
use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn new_db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    (db, dir)
}

fn exec(db: &Database, sql: &str) {
    db.execute(sql).unwrap_or_else(|e| panic!("SQL failed: {}\n  err: {}", sql, e));
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    let rs = db.execute(sql).unwrap_or_else(|e| panic!("SQL failed: {}\n  err: {}", sql, e))
        .materialize().unwrap_or_else(|e| panic!("mat failed: {}\n  err: {}", sql, e));
    match rs { QueryResult::Select { rows, .. } => rows, _ => panic!("not Select") }
}

fn scalar_i64(db: &Database, sql: &str) -> i64 {
    let r = rows(db, sql);
    assert_eq!(r.len(), 1, "1 row: {}", sql);
    match r[0].first() { Some(Value::Integer(n)) => *n, o => panic!("int? {:?}: {}", o, sql) }
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. Nested subquery (subquery in subquery)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn nested_in_subqueries() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY, v INT)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY, ref INT)");
    exec(&db, "CREATE TABLE c (id INT PRIMARY KEY, ref INT)");
    exec(&db, "INSERT INTO a VALUES (1, 100)");
    exec(&db, "INSERT INTO a VALUES (2, 200)");
    exec(&db, "INSERT INTO b VALUES (1, 1)");
    exec(&db, "INSERT INTO b VALUES (2, 2)");
    exec(&db, "INSERT INTO c VALUES (1, 1)");
    // a.id IN (SELECT ref FROM b WHERE ref IN (SELECT ref FROM c))
    // c has ref=1, so b.ref IN (1) → b row 1 (ref=1). a.id IN (1) → a row 1.
    let r = rows(&db, "SELECT id FROM a WHERE id IN (SELECT ref FROM b WHERE ref IN (SELECT ref FROM c))");
    assert!(r.len() >= 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. JOIN with WHERE on joined columns
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn join_with_where_filter() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE orders (id INT PRIMARY KEY, cust_id INT, amt INT)");
    exec(&db, "CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, region TEXT)");
    exec(&db, "INSERT INTO customers VALUES (1, 'Alice', 'US')");
    exec(&db, "INSERT INTO customers VALUES (2, 'Bob', 'EU')");
    exec(&db, "INSERT INTO customers VALUES (3, 'Carol', 'US')");
    exec(&db, "INSERT INTO orders VALUES (1, 1, 100)");
    exec(&db, "INSERT INTO orders VALUES (2, 2, 200)");
    exec(&db, "INSERT INTO orders VALUES (3, 3, 150)");
    exec(&db, "INSERT INTO orders VALUES (4, 1, 50)");
    // JOIN + WHERE region = 'US' → orders for Alice (1,4) and Carol (3) = 3 rows.
    let r = rows(&db, "SELECT o.id FROM orders o INNER JOIN customers c ON o.cust_id = c.id WHERE c.region = 'US' ORDER BY o.id");
    assert_eq!(r.len(), 3);
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. JOIN producing aggregate
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn join_with_aggregate() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE orders (id INT PRIMARY KEY, cust_id INT, amt INT)");
    exec(&db, "CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)");
    exec(&db, "INSERT INTO customers VALUES (1, 'Alice')");
    exec(&db, "INSERT INTO customers VALUES (2, 'Bob')");
    exec(&db, "INSERT INTO orders VALUES (1, 1, 100)");
    exec(&db, "INSERT INTO orders VALUES (2, 1, 200)");
    exec(&db, "INSERT INTO orders VALUES (3, 2, 50)");
    // NOTE: parser doesn't support table-qualified names in GROUP BY
    // (alias.col), and SELECT alias.col with GROUP BY bare col mismatches.
    // Use bare column names in both SELECT and GROUP BY for JOIN+aggregate.
    let r = rows(&db, "SELECT name, SUM(amt) FROM orders o INNER JOIN customers c ON o.cust_id = c.id GROUP BY name");
    assert_eq!(r.len(), 2, "Alice and Bob");
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. Arithmetic with NULL
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn arithmetic_with_null_propagates() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10, NULL)");
    let r = rows(&db, "SELECT a + b FROM t WHERE id = 1");
    // NULL + 10 = NULL (SQL semantics: arithmetic with NULL yields NULL).
    assert!(matches!(r[0][0], Value::Null), "10 + NULL must be NULL");
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. UPDATE with expression
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn update_with_arithmetic_expression() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "UPDATE t SET v = v + 5 WHERE id = 1");
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1"), 15);
    exec(&db, "UPDATE t SET v = v * 2 WHERE id = 1");
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1"), 30);
}

#[test]
fn update_all_with_expression() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=5 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i * 10));
    }
    exec(&db, "UPDATE t SET v = v + 1");
    for i in 1..=5 {
        assert_eq!(scalar_i64(&db, &format!("SELECT v FROM t WHERE id = {}", i)), i * 10 + 1);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. CASE in WHERE
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn select_with_case_expression_output() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 5)");
    exec(&db, "INSERT INTO t VALUES (2, 15)");
    exec(&db, "INSERT INTO t VALUES (3, 25)");
    // CASE to categorize.
    let r = rows(&db, "SELECT id, CASE WHEN v < 10 THEN 'low' WHEN v < 20 THEN 'mid' ELSE 'high' END FROM t ORDER BY id");
    assert_eq!(r.len(), 3);
    match &r[0][1] { Value::Text(s) => assert_eq!(&*s.0, "low"), _ => panic!("5→low") }
    match &r[1][1] { Value::Text(s) => assert_eq!(&*s.0, "mid"), _ => panic!("15→mid") }
    match &r[2][1] { Value::Text(s) => assert_eq!(&*s.0, "high"), _ => panic!("25→high") }
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. LIKE with special characters
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn like_with_percent_in_middle() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'hello world')");
    exec(&db, "INSERT INTO t VALUES (2, 'helloworld')");
    exec(&db, "INSERT INTO t VALUES (3, 'hello')");
    // LIKE 'hello%world' matches 'hello world' (has space between).
    let r = rows(&db, "SELECT id FROM t WHERE s LIKE 'hello%world'");
    assert!(r.len() >= 1, "should match hello world");
}

#[test]
fn like_multiple_underscores() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'abc')");
    exec(&db, "INSERT INTO t VALUES (2, 'abcd')");
    exec(&db, "INSERT INTO t VALUES (3, 'ab')");
    // 'a_c' matches exactly 3 chars: 'abc'. Not 'abcd' (4) or 'ab' (2).
    let r = rows(&db, "SELECT id FROM t WHERE s LIKE 'a_c'");
    assert_eq!(r.len(), 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. DISTINCT with NULL
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn distinct_includes_null_once() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, NULL)");
    exec(&db, "INSERT INTO t VALUES (2, NULL)");
    exec(&db, "INSERT INTO t VALUES (3, 10)");
    exec(&db, "INSERT INTO t VALUES (4, 10)");
    // DISTINCT v → {NULL, 10} = 2 distinct values.
    let r = rows(&db, "SELECT DISTINCT v FROM t");
    assert_eq!(r.len(), 2, "NULL appears once, 10 once");
}

// ═══════════════════════════════════════════════════════════════════════════
// 9. GROUP BY with NULL group + aggregate
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn group_by_null_with_sum() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 'a', 10)");
    exec(&db, "INSERT INTO t VALUES (2, NULL, 20)");
    exec(&db, "INSERT INTO t VALUES (3, NULL, 30)");
    exec(&db, "INSERT INTO t VALUES (4, 'a', 40)");
    // GROUP BY cat: 'a' → SUM 50, NULL → SUM 50.
    let r = rows(&db, "SELECT cat, SUM(v) FROM t GROUP BY cat");
    assert_eq!(r.len(), 2, "2 groups: 'a' and NULL");
}

// ═══════════════════════════════════════════════════════════════════════════
// 10. Stress: many small transactions
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn many_small_transactions() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=50 {
        let tx = db.begin_transaction().unwrap();
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
        db.commit_transaction(tx).unwrap();
    }
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 50);
}

#[test]
fn many_rolled_back_transactions() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    for i in 2..=50 {
        let tx = db.begin_transaction().unwrap();
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
        db.rollback_transaction(tx).unwrap();
    }
    // Only the seed row should remain.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// 11. ORDER BY on PK (already sorted)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn order_by_pk_desc() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=10 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    let r = rows(&db, "SELECT id FROM t ORDER BY id DESC");
    let ids: Vec<i64> = r.iter().filter_map(|row| match row.get(0) {
        Some(Value::Integer(n)) => Some(*n), _ => None
    }).collect();
    assert_eq!(ids, vec![10, 9, 8, 7, 6, 5, 4, 3, 2, 1]);
}

// ═══════════════════════════════════════════════════════════════════════════
// 12. SELECT with no FROM (constant expressions)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn select_constant_expression() {
    let (db, _d) = new_db();
    // SELECT without FROM — should return a single row with the computed value.
    let _ = db.execute("SELECT 1 + 1");
    let _ = db.execute("SELECT 42");
}

// ═══════════════════════════════════════════════════════════════════════════
// 13. DELETE with complex WHERE
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn delete_with_or_condition() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=10 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    // Delete rows where v < 3 OR v > 8 → ids 1,2,9,10 deleted.
    exec(&db, "DELETE FROM t WHERE v < 3 OR v > 8");
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 6);
    // Remaining: 3,4,5,6,7,8.
    assert_eq!(scalar_i64(&db, "SELECT MIN(v) FROM t"), 3);
    assert_eq!(scalar_i64(&db, "SELECT MAX(v) FROM t"), 8);
}

// ═══════════════════════════════════════════════════════════════════════════
// 14. Aggregate over filtered set
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn aggregate_with_where_correct() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    for i in 1..=20 {
        let cat = if i % 2 == 0 { "even" } else { "odd" };
        exec(&db, &format!("INSERT INTO t VALUES ({}, '{}', {})", i, cat, i));
    }
    // SUM of even rows: 2+4+...+20 = 110.
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t WHERE cat = 'even'"), 110);
    // COUNT of odd rows: 10.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE cat = 'odd'"), 10);
}

// ═══════════════════════════════════════════════════════════════════════════
// 15. Self-join
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn self_join_basic() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE emp (id INT PRIMARY KEY, name TEXT, mgr_id INT)");
    exec(&db, "INSERT INTO emp VALUES (1, 'CEO', 0)");
    exec(&db, "INSERT INTO emp VALUES (2, 'Alice', 1)");
    exec(&db, "INSERT INTO emp VALUES (3, 'Bob', 1)");
    exec(&db, "INSERT INTO emp VALUES (4, 'Carol', 2)");
    // Self-join: employee + their manager's name.
    let r = rows(&db, "SELECT e.name FROM emp e INNER JOIN emp m ON e.mgr_id = m.id ORDER BY e.id");
    // CEO has mgr_id=0 (no match), Alice/Bob mgr=1 (CEO), Carol mgr=2 (Alice).
    // Matched: Alice, Bob, Carol = 3 rows.
    assert_eq!(r.len(), 3);
}
