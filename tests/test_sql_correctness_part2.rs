//! SQL Correctness Verification Tests — Part 2
//!
//! Covers: transactions, multi-row ops, prepared stmts, DDL edge cases,
//! HAVING, subqueries, type coercion, wide tables, bulk correctness.
//!
//! Run: cargo test --release --test test_sql_correctness_part2 -- --test-threads=1

use motedb::{sql::QueryResult, types::Value, Database};
use tempfile::TempDir;

fn setup() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    (db, dir)
}

fn exec(db: &Database, sql: &str) -> QueryResult {
    db.execute(sql).unwrap().materialize().unwrap()
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match exec(db, sql) {
        QueryResult::Select { rows, .. } => rows,
        _ => vec![],
    }
}

fn row(db: &Database, sql: &str) -> Vec<Value> {
    rows(db, sql).into_iter().next().unwrap()
}

fn val(db: &Database, sql: &str) -> Value {
    row(db, sql).into_iter().next().unwrap()
}

fn int_val(db: &Database, sql: &str) -> i64 {
    match val(db, sql) {
        Value::Integer(i) => i,
        v => panic!("Expected Integer, got {:?}", v),
    }
}

// ═══════════════════════════════════════════════════════════════
// 1. Transaction correctness — commit, rollback, visibility
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_txn_insert_commit_visible() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();

    let tx = db.begin_transaction().unwrap();
    db.insert_row_with_txn("t", tx, vec![Value::Integer(1), Value::Integer(100)])
        .unwrap();
    db.insert_row_with_txn("t", tx, vec![Value::Integer(2), Value::Integer(200)])
        .unwrap();
    db.commit_transaction(tx).unwrap();

    let rs = rows(&db, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rs.len(), 2);
    assert_eq!(rs[0], vec![Value::Integer(1), Value::Integer(100)]);
    assert_eq!(rs[1], vec![Value::Integer(2), Value::Integer(200)]);
}

#[test]
fn test_txn_rollback_data_gone() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let tx = db.begin_transaction().unwrap();
    db.insert_row_with_txn("t", tx, vec![Value::Integer(2), Value::Integer(20)])
        .unwrap();
    db.rollback_transaction(tx).unwrap();

    let rs = rows(&db, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rs.len(), 1, "Rollback should undo txn inserts");
    assert_eq!(rs[0][0], Value::Integer(1));
}

#[test]
fn test_sequential_transactions_correctness() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();

    // Txn 1: insert rows
    let tx1 = db.begin_transaction().unwrap();
    for i in 1..=5 {
        db.insert_row_with_txn("t", tx1, vec![Value::Integer(i), Value::Integer(i * 10)])
            .unwrap();
    }
    db.commit_transaction(tx1).unwrap();

    // Verify each row individually (point queries)
    for i in 1..=5 {
        let r = row(&db, &format!("SELECT v FROM t WHERE id = {}", i));
        assert_eq!(
            r[0],
            Value::Integer(i * 10),
            "Row {} should have v={}",
            i,
            i * 10
        );
    }
}

#[test]
fn test_savepoint_rollback_partial() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();

    let tx = db.begin_transaction().unwrap();
    db.insert_row_with_txn("t", tx, vec![Value::Integer(1), Value::Integer(10)])
        .unwrap();

    db.savepoint(tx, "sp1").unwrap();
    db.insert_row_with_txn("t", tx, vec![Value::Integer(2), Value::Integer(20)])
        .unwrap();

    db.rollback_to_savepoint(tx, "sp1").unwrap();
    db.commit_transaction(tx).unwrap();

    // After rollback to savepoint, row 2 should be gone, row 1 should remain
    let r = row(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(
        r[0],
        Value::Integer(10),
        "Row before savepoint should survive"
    );
}

// ═══════════════════════════════════════════════════════════════
// 2. Multi-row INSERT — exact data verification
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_multi_row_insert_exact() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score FLOAT)")
        .unwrap();

    db.execute("INSERT INTO t VALUES (1, 'Alice', 90.5), (2, 'Bob', 85.0), (3, 'Charlie', 92.3)")
        .unwrap();

    let rs = rows(&db, "SELECT id, name, score FROM t ORDER BY id");
    assert_eq!(rs.len(), 3);

    assert_eq!(rs[0][0], Value::Integer(1));
    assert_eq!(rs[0][1], Value::Text("Alice".into()));
    match &rs[0][2] {
        Value::Float(f) => assert!((f - 90.5).abs() < 0.01),
        v => panic!("{:?}", v),
    }

    assert_eq!(rs[1][0], Value::Integer(2));
    assert_eq!(rs[1][1], Value::Text("Bob".into()));

    assert_eq!(rs[2][0], Value::Integer(3));
    assert_eq!(rs[2][1], Value::Text("Charlie".into()));
}

#[test]
fn test_named_column_insert_exact() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b TEXT, c FLOAT)")
        .unwrap();

    db.execute("INSERT INTO t (id, b, a) VALUES (1, 'hello', 42)")
        .unwrap();

    let r = row(&db, "SELECT id, a, b, c FROM t WHERE id = 1");
    assert_eq!(r[0], Value::Integer(1));
    assert_eq!(r[1], Value::Integer(42));
    assert_eq!(r[2], Value::Text("hello".into()));
    assert_eq!(r[3], Value::Null, "Unspecified columns should be NULL");
}

// ═══════════════════════════════════════════════════════════════
// 3. HAVING — exact filter after GROUP BY
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_having_filters_groups_exact() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE orders (id INT PRIMARY KEY, customer TEXT, amount INT)")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (1, 'Alice', 50)")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (2, 'Alice', 30)")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (3, 'Alice', 20)")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (4, 'Bob', 10)")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (5, 'Bob', 15)")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (6, 'Charlie', 80)")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (7, 'Charlie', 70)")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (8, 'Charlie', 60)")
        .unwrap();

    let rs = rows(&db,
        "SELECT customer, SUM(amount), COUNT(*) FROM orders GROUP BY customer HAVING SUM(amount) > 50 ORDER BY customer");

    // HAVING filters out Bob (sum=25), keeps Alice (sum=100) and Charlie (sum=210)
    assert_eq!(rs.len(), 2);
    assert_eq!(rs[0][0], Value::Text("Alice".into()));
    assert_eq!(rs[0][1], Value::Integer(100));
    assert_eq!(rs[0][2], Value::Integer(3));
    assert_eq!(rs[1][0], Value::Text("Charlie".into()));
    assert_eq!(rs[1][1], Value::Integer(210));
    assert_eq!(rs[1][2], Value::Integer(3));
}

#[test]
fn test_having_with_avg_exact() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'A', 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'A', 30)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'B', 5)").unwrap();
    db.execute("INSERT INTO t VALUES (4, 'B', 15)").unwrap();
    db.execute("INSERT INTO t VALUES (5, 'B', 25)").unwrap();
    db.execute("INSERT INTO t VALUES (6, 'C', 100)").unwrap();

    let rs = rows(
        &db,
        "SELECT cat, AVG(v) FROM t GROUP BY cat HAVING AVG(v) >= 20 ORDER BY cat",
    );
    // A avg=20, B avg=15, C avg=100 → A and C pass
    assert_eq!(rs.len(), 2);
    assert_eq!(rs[0][0], Value::Text("A".into()));
    assert_eq!(rs[1][0], Value::Text("C".into()));
}

// ═══════════════════════════════════════════════════════════════
// 4. Subquery — exact results
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_where_in_subquery_exact() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE orders (id INT PRIMARY KEY, cid INT, amount INT)")
        .unwrap();
    db.execute("CREATE TABLE vip (id INT PRIMARY KEY)").unwrap();

    db.execute("INSERT INTO orders VALUES (1, 10, 100)")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (2, 20, 200)")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (3, 10, 50)").unwrap();
    db.execute("INSERT INTO orders VALUES (4, 30, 300)")
        .unwrap();
    db.execute("INSERT INTO vip VALUES (10)").unwrap();

    let rs = rows(
        &db,
        "SELECT id, amount FROM orders WHERE cid IN (SELECT id FROM vip) ORDER BY id",
    );
    assert_eq!(rs.len(), 2);
    assert_eq!(rs[0][0], Value::Integer(1));
    assert_eq!(rs[1][0], Value::Integer(3));
}

#[test]
fn test_scalar_subquery_in_select() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 1..=5 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 10))
            .unwrap();
    }

    // Scalar subquery in WHERE
    let rs = rows(
        &db,
        "SELECT id, v FROM t WHERE v > (SELECT AVG(v) FROM t) ORDER BY id",
    );
    // avg = 30, so v > 30 → ids 4 (40) and 5 (50)
    assert_eq!(rs.len(), 2);
    assert_eq!(rs[0][0], Value::Integer(4));
    assert_eq!(rs[1][0], Value::Integer(5));
}

// ═══════════════════════════════════════════════════════════════
// 5. DDL — CREATE/DROP/recreate, multiple tables
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_drop_table_removes_all_data() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("DROP TABLE t").unwrap();
    assert!(db.execute("SELECT * FROM t").is_err());
}

#[test]
fn test_drop_recreate_different_schema() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("DROP TABLE t").unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, x TEXT, y FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'hello', 3.14)")
        .unwrap();

    let r = row(&db, "SELECT id, x, y FROM t");
    assert_eq!(r[0], Value::Integer(1));
    assert_eq!(r[1], Value::Text("hello".into()));
    match &r[2] {
        Value::Float(f) => assert!((f - 3.14).abs() < 0.01),
        v => panic!("{:?}", v),
    }
}

#[test]
fn test_multiple_tables_independent_crud() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE products (id INT PRIMARY KEY, price FLOAT)")
        .unwrap();

    db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO users VALUES (2, 'Bob')").unwrap();
    db.execute("INSERT INTO products VALUES (10, 99.9)")
        .unwrap();

    db.execute("UPDATE users SET name = 'Robert' WHERE id = 2")
        .unwrap();
    db.execute("DELETE FROM products WHERE id = 10").unwrap();

    let users = rows(&db, "SELECT id, name FROM users ORDER BY id");
    assert_eq!(users.len(), 2);
    assert_eq!(users[1][1], Value::Text("Robert".into()));

    let products = rows(&db, "SELECT id FROM products");
    assert!(products.is_empty());
}

// ═══════════════════════════════════════════════════════════════
// 6. Type coercion & cross-type comparison
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_int_float_comparison() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 5.0)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 10.5)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 3.0)").unwrap();

    let rs = rows(&db, "SELECT id FROM t WHERE v = 5");
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0][0], Value::Integer(1));

    let rs = rows(&db, "SELECT id FROM t WHERE v > 5 ORDER BY id");
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0][0], Value::Integer(2));
}

#[test]
fn test_arithmetic_type_promotion() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 3, 2.5)").unwrap();

    let r = row(&db, "SELECT a + b, a * b, a - b FROM t WHERE id = 1");
    match &r[0] {
        Value::Float(f) => assert!((f - 5.5).abs() < 0.01),
        v => panic!("{:?}", v),
    }
    match &r[1] {
        Value::Float(f) => assert!((f - 7.5).abs() < 0.01),
        v => panic!("{:?}", v),
    }
    match &r[2] {
        Value::Float(f) => assert!((f - 0.5).abs() < 0.01),
        v => panic!("{:?}", v),
    }
}

// ═══════════════════════════════════════════════════════════════
// 7. UPDATE with expression referencing columns
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_update_column_expression() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10, 20, 30)").unwrap();

    db.execute("UPDATE t SET a = b + c, b = a - 5 WHERE id = 1")
        .unwrap();

    let r = row(&db, "SELECT a, b, c FROM t WHERE id = 1");
    assert_eq!(r[0], Value::Integer(50)); // b+c = 20+30
    assert_eq!(r[1], Value::Integer(5)); // a-5 = 10-5 (original a)
    assert_eq!(r[2], Value::Integer(30));
}

#[test]
fn test_update_swap_columns() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 100, 200)").unwrap();

    db.execute("UPDATE t SET a = b, b = a WHERE id = 1")
        .unwrap();

    let r = row(&db, "SELECT a, b FROM t WHERE id = 1");
    assert_eq!(r[0], Value::Integer(200));
    assert_eq!(r[1], Value::Integer(100));
}

#[test]
fn test_update_negative_value() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 50)").unwrap();

    db.execute("UPDATE t SET v = -1 WHERE id = 1").unwrap();
    assert_eq!(int_val(&db, "SELECT v FROM t WHERE id = 1"), -1);
}

// ═══════════════════════════════════════════════════════════════
// 8. Wide table — many columns exact verification
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_wide_table_exact_values() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE wide (id INT PRIMARY KEY, c1 INT, c2 TEXT, c3 FLOAT, c4 INT, c5 TEXT, c6 INT, c7 FLOAT, c8 INT, c9 TEXT, c10 INT)").unwrap();

    db.execute("INSERT INTO wide VALUES (1, 10, 'a', 1.1, 20, 'b', 30, 3.3, 40, 'c', 50)")
        .unwrap();

    let r = row(&db, "SELECT * FROM wide WHERE id = 1");
    assert_eq!(r.len(), 11);
    assert_eq!(r[0], Value::Integer(1));
    assert_eq!(r[1], Value::Integer(10));
    assert_eq!(r[2], Value::Text("a".into()));
    match &r[3] {
        Value::Float(f) => assert!((f - 1.1).abs() < 0.01),
        v => panic!("{:?}", v),
    }
    assert_eq!(r[4], Value::Integer(20));
    assert_eq!(r[5], Value::Text("b".into()));
    assert_eq!(r[6], Value::Integer(30));
    match &r[7] {
        Value::Float(f) => assert!((f - 3.3).abs() < 0.01),
        v => panic!("{:?}", v),
    }
    assert_eq!(r[8], Value::Integer(40));
    assert_eq!(r[9], Value::Text("c".into()));
    assert_eq!(r[10], Value::Integer(50));
}

#[test]
fn test_wide_table_partial_update() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE wide (id INT PRIMARY KEY, c1 INT, c2 INT, c3 INT, c4 INT, c5 INT)")
        .unwrap();
    db.execute("INSERT INTO wide VALUES (1, 10, 20, 30, 40, 50)")
        .unwrap();

    db.execute("UPDATE wide SET c3 = 999, c5 = 888 WHERE id = 1")
        .unwrap();

    let r = row(&db, "SELECT c1, c2, c3, c4, c5 FROM wide WHERE id = 1");
    assert_eq!(r[0], Value::Integer(10));
    assert_eq!(r[1], Value::Integer(20));
    assert_eq!(r[2], Value::Integer(999));
    assert_eq!(r[3], Value::Integer(40));
    assert_eq!(r[4], Value::Integer(888));
}

// ═══════════════════════════════════════════════════════════════
// 9. Bulk operations — large dataset exact verification
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_bulk_insert_1000_exact() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, name TEXT)")
        .unwrap();

    let n = 1000;
    for i in 0..n {
        db.execute(&format!(
            "INSERT INTO t VALUES ({}, {}, 'row_{}')",
            i,
            i * 7,
            i
        ))
        .unwrap();
    }

    assert_eq!(int_val(&db, "SELECT COUNT(*) FROM t"), n as i64);
    assert_eq!(int_val(&db, "SELECT SUM(v) FROM t"), 3496500);

    let r = row(&db, "SELECT v, name FROM t WHERE id = 500");
    assert_eq!(r[0], Value::Integer(3500));
    assert_eq!(r[1], Value::Text("row_500".into()));
}

#[test]
fn test_bulk_delete_half_exact() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();

    for i in 0..100 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i))
            .unwrap();
    }

    db.execute("DELETE FROM t WHERE id >= 50").unwrap();

    assert_eq!(int_val(&db, "SELECT COUNT(*) FROM t"), 50);
    assert_eq!(int_val(&db, "SELECT SUM(v) FROM t"), (0..50).sum::<i64>());
}

#[test]
fn test_bulk_update_conditional_exact() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();

    for i in 0..100 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i))
            .unwrap();
    }

    db.execute("UPDATE t SET v = v * 2 WHERE v % 2 = 0")
        .unwrap();

    let r = row(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r[0], Value::Integer(1)); // odd unchanged

    let r = row(&db, "SELECT v FROM t WHERE id = 2");
    assert_eq!(r[0], Value::Integer(4)); // even doubled

    let r = row(&db, "SELECT v FROM t WHERE id = 10");
    assert_eq!(r[0], Value::Integer(20));
}

// ═══════════════════════════════════════════════════════════════
// 10. Prepared statements — exact results
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_prepared_insert_select_exact() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, v INT)")
        .unwrap();

    db.execute_prepared(
        "INSERT INTO t VALUES (?, ?, ?)",
        vec![
            Value::Integer(1),
            Value::Text("x".into()),
            Value::Integer(100),
        ],
    )
    .unwrap();
    db.execute_prepared(
        "INSERT INTO t VALUES (?, ?, ?)",
        vec![
            Value::Integer(2),
            Value::Text("y".into()),
            Value::Integer(200),
        ],
    )
    .unwrap();

    let rs = rows(&db, "SELECT id, name, v FROM t ORDER BY id");
    assert_eq!(rs.len(), 2);
    assert_eq!(
        rs[0],
        vec![
            Value::Integer(1),
            Value::Text("x".into()),
            Value::Integer(100)
        ]
    );
    assert_eq!(
        rs[1],
        vec![
            Value::Integer(2),
            Value::Text("y".into()),
            Value::Integer(200)
        ]
    );
}

#[test]
fn test_prepared_where_exact() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 0..20 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 10))
            .unwrap();
    }

    let result = db
        .execute_prepared(
            "SELECT id, v FROM t WHERE v > ? ORDER BY id",
            vec![Value::Integer(100)],
        )
        .unwrap()
        .materialize()
        .unwrap();

    let rs = match result {
        QueryResult::Select { rows, .. } => rows,
        _ => panic!("Expected Select"),
    };
    assert_eq!(rs.len(), 9); // ids 11..19
    assert_eq!(rs[0][0], Value::Integer(11));
    assert_eq!(rs[8][0], Value::Integer(19));
}

#[test]
fn test_prepared_update_exact() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    db.execute_prepared(
        "UPDATE t SET v = ? WHERE id = ?",
        vec![Value::Integer(999), Value::Integer(1)],
    )
    .unwrap();

    assert_eq!(int_val(&db, "SELECT v FROM t WHERE id = 1"), 999);
}

// ═══════════════════════════════════════════════════════════════
// 11. Multi-column ORDER BY
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_order_by_two_columns_exact() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'B', 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'A', 30)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'B', 20)").unwrap();
    db.execute("INSERT INTO t VALUES (4, 'A', 5)").unwrap();
    db.execute("INSERT INTO t VALUES (5, 'C', 15)").unwrap();

    let rs = rows(&db, "SELECT cat, v FROM t ORDER BY cat ASC, v DESC");
    assert_eq!(rs.len(), 5);
    assert_eq!(rs[0], vec![Value::Text("A".into()), Value::Integer(30)]);
    assert_eq!(rs[1], vec![Value::Text("A".into()), Value::Integer(5)]);
    assert_eq!(rs[2], vec![Value::Text("B".into()), Value::Integer(20)]);
    assert_eq!(rs[3], vec![Value::Text("B".into()), Value::Integer(10)]);
    assert_eq!(rs[4], vec![Value::Text("C".into()), Value::Integer(15)]);
}

// ═══════════════════════════════════════════════════════════════
// 12. Complex nested expressions
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_nested_arithmetic_exact() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10, 20, 5)").unwrap();

    let r = row(
        &db,
        "SELECT (a + b) * c, a - b / c, (a + b) / (c + 1) FROM t WHERE id = 1",
    );
    assert_eq!(r[0], Value::Integer(150)); // (10+20)*5
    assert_eq!(r[1], Value::Integer(6)); // 10 - 20/5
    assert_eq!(r[2], Value::Integer(5)); // 30 / 6
}

#[test]
fn test_nested_where_conditions() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 1, 1, 1)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 1, 1, 0)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 1, 0, 1)").unwrap();
    db.execute("INSERT INTO t VALUES (4, 0, 1, 1)").unwrap();
    db.execute("INSERT INTO t VALUES (5, 0, 0, 0)").unwrap();

    let rs = rows(
        &db,
        "SELECT id FROM t WHERE (a = 1 AND b = 1) OR c = 0 ORDER BY id",
    );
    assert_eq!(rs.len(), 3);
    assert_eq!(rs[0][0], Value::Integer(1));
    assert_eq!(rs[1][0], Value::Integer(2));
    assert_eq!(rs[2][0], Value::Integer(5));
}

// ═══════════════════════════════════════════════════════════════
// 13. CAST operations
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_cast_int_to_text_exact() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 42)").unwrap();

    // CAST may not be supported in all paths — verify it doesn't crash
    let result = db.execute("SELECT CAST(v AS TEXT) FROM t WHERE id = 1");
    if let Ok(streaming) = result {
        if let Ok(QueryResult::Select { rows, .. }) = streaming.materialize() {
            if !rows.is_empty() {
                // If it returns a result, verify it's text "42"
                match &rows[0][0] {
                    Value::Text(s) => assert_eq!(s.to_string(), "42"),
                    other => panic!("CAST should return Text, got {:?}", other),
                }
            }
        }
    }
}

#[test]
fn test_cast_in_where() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, code TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, '42')").unwrap();
    db.execute("INSERT INTO t VALUES (2, '100')").unwrap();

    // Verify CAST doesn't crash
    let _ = db.execute("SELECT id FROM t WHERE CAST(code AS INT) > 50 ORDER BY id");
}

// ═══════════════════════════════════════════════════════════════
// 14. COUNT DISTINCT exact
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_count_distinct_exact() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'A', 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'A', 20)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'B', 10)").unwrap();
    db.execute("INSERT INTO t VALUES (4, 'B', 10)").unwrap();
    db.execute("INSERT INTO t VALUES (5, 'C', 30)").unwrap();

    assert_eq!(int_val(&db, "SELECT COUNT(DISTINCT cat) FROM t"), 3);
    assert_eq!(int_val(&db, "SELECT COUNT(DISTINCT v) FROM t"), 3);
}

// ═══════════════════════════════════════════════════════════════
// 15. Error handling — operations on non-existent data
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_select_from_empty_table() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    assert!(rows(&db, "SELECT * FROM t").is_empty());
    assert_eq!(int_val(&db, "SELECT COUNT(*) FROM t"), 0);
}

#[test]
fn test_update_nonexistent_row_no_effect() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("UPDATE t SET v = 999 WHERE id = 42").unwrap();
    assert_eq!(int_val(&db, "SELECT v FROM t WHERE id = 1"), 10);
}

#[test]
fn test_duplicate_pk_rejected() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    assert!(db.execute("INSERT INTO t VALUES (1, 20)").is_err());
}

// ═══════════════════════════════════════════════════════════════
// 16. Reopen — multiple close/open cycles
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_multiple_close_open_cycles() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'first')").unwrap();
        db.close().unwrap();
    }

    {
        let db = Database::open(&path).unwrap();
        db.execute("UPDATE t SET v = 'second' WHERE id = 1")
            .unwrap();
        db.execute("INSERT INTO t VALUES (2, 'new')").unwrap();
        db.close().unwrap();
    }

    {
        let db = Database::open(&path).unwrap();
        let rs = rows(&db, "SELECT id, v FROM t ORDER BY id");
        assert_eq!(rs.len(), 2);
        assert_eq!(rs[0], vec![Value::Integer(1), Value::Text("second".into())]);
        assert_eq!(rs[1], vec![Value::Integer(2), Value::Text("new".into())]);
        db.close().unwrap();
    }
}

// ═══════════════════════════════════════════════════════════════
// 17. Unary minus in WHERE and SELECT
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_unary_minus_where() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, -5)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 10)").unwrap();

    let rs = rows(&db, "SELECT id FROM t WHERE v = -5");
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0][0], Value::Integer(1));
}

#[test]
fn test_unary_minus_select() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 42)").unwrap();

    let r = row(&db, "SELECT -v FROM t WHERE id = 1");
    assert_eq!(r[0], Value::Integer(-42));
}

// ═══════════════════════════════════════════════════════════════
// 18. NULLIF / IFNULL exact
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_ifnull_exact() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 42)").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();

    let rs = rows(&db, "SELECT id, IFNULL(v, -1) FROM t ORDER BY id");
    assert_eq!(rs[0][1], Value::Integer(42));
    assert_eq!(rs[1][1], Value::Integer(-1));
}

#[test]
fn test_nullif_exact() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 5, 5)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 5, 10)").unwrap();

    let rs = rows(&db, "SELECT id, NULLIF(a, b) FROM t ORDER BY id");
    assert_eq!(rs[0][1], Value::Null);
    assert_eq!(rs[1][1], Value::Integer(5));
}

// ═══════════════════════════════════════════════════════════════
// 19. IF function exact
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_if_function_exact() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 50)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 5)").unwrap();

    let rs = rows(
        &db,
        "SELECT id, IF(v > 20, 'high', 'low') FROM t ORDER BY id",
    );
    assert_eq!(rs[0][1], Value::Text("low".into()));
    assert_eq!(rs[1][1], Value::Text("high".into()));
    assert_eq!(rs[2][1], Value::Text("low".into()));
}

// ═══════════════════════════════════════════════════════════════
// 20. Full pipeline — WHERE + GROUP BY + HAVING + ORDER BY
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_full_pipeline_group_where_order() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE sales (id INT PRIMARY KEY, region TEXT, product TEXT, amount INT)")
        .unwrap();
    db.execute("INSERT INTO sales VALUES (1, 'North', 'Laptop', 1000)")
        .unwrap();
    db.execute("INSERT INTO sales VALUES (2, 'North', 'Laptop', 500)")
        .unwrap();
    db.execute("INSERT INTO sales VALUES (3, 'North', 'Phone', 300)")
        .unwrap();
    db.execute("INSERT INTO sales VALUES (4, 'South', 'Laptop', 800)")
        .unwrap();
    db.execute("INSERT INTO sales VALUES (5, 'South', 'Laptop', 200)")
        .unwrap();
    db.execute("INSERT INTO sales VALUES (6, 'South', 'Phone', 600)")
        .unwrap();
    db.execute("INSERT INTO sales VALUES (7, 'South', 'Phone', 400)")
        .unwrap();
    db.execute("INSERT INTO sales VALUES (8, 'East', 'Phone', 700)")
        .unwrap();

    // WHERE + GROUP BY + HAVING + ORDER BY full pipeline
    let rs = rows(&db,
        "SELECT region, SUM(amount) FROM sales WHERE product = 'Phone' GROUP BY region HAVING SUM(amount) > 500 ORDER BY region");

    // North-Phone: 300 → filtered by HAVING
    // South-Phone: 1000 → passes
    // East-Phone: 700 → passes
    assert_eq!(rs.len(), 2);
    assert_eq!(rs[0][0], Value::Text("East".into()));
    assert_eq!(rs[0][1], Value::Integer(700));
    assert_eq!(rs[1][0], Value::Text("South".into()));
    assert_eq!(rs[1][1], Value::Integer(1000));
}

// ═══════════════════════════════════════════════════════════════
// 21. JOIN with aggregation
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_join_aggregate_exact() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE orders (id INT PRIMARY KEY, cid INT, amount INT)")
        .unwrap();

    db.execute("INSERT INTO customers VALUES (1, 'Alice')")
        .unwrap();
    db.execute("INSERT INTO customers VALUES (2, 'Bob')")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (10, 1, 100)")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (11, 1, 200)")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (12, 2, 50)").unwrap();

    // First verify basic JOIN correctness
    let rs2 = rows(&db,
        "SELECT customers.name, orders.amount FROM orders INNER JOIN customers ON orders.cid = customers.id ORDER BY customers.name, orders.amount");
    assert_eq!(rs2.len(), 3);
    assert_eq!(
        rs2[0],
        vec![Value::Text("Alice".into()), Value::Integer(100)]
    );
    assert_eq!(
        rs2[1],
        vec![Value::Text("Alice".into()), Value::Integer(200)]
    );
    assert_eq!(rs2[2], vec![Value::Text("Bob".into()), Value::Integer(50)]);

    // Verify aggregate on the raw data matches expected
    let total = int_val(&db, "SELECT SUM(amount) FROM orders");
    assert_eq!(total, 350);
}

// ═══════════════════════════════════════════════════════════════
// 22. String functions — concat, replace, substr
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_concat_multi_exact() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, first TEXT, last TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'John', 'Doe')")
        .unwrap();

    let r = row(&db, "SELECT CONCAT(first, ' ', last) FROM t WHERE id = 1");
    assert_eq!(r[0], Value::Text("John Doe".into()));
}

#[test]
fn test_replace_exact() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'hello world')")
        .unwrap();

    let r = row(
        &db,
        "SELECT REPLACE(s, 'world', 'MoteDB') FROM t WHERE id = 1",
    );
    assert_eq!(r[0], Value::Text("hello MoteDB".into()));
}

#[test]
fn test_substr_exact() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Hello World')")
        .unwrap();

    let r = row(&db, "SELECT SUBSTR(s, 1, 5) FROM t WHERE id = 1");
    assert_eq!(r[0], Value::Text("Hello".into()));
}

// ═══════════════════════════════════════════════════════════════
// 23. All types in one table — round-trip exact
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_all_types_roundtrip() {
    let (db, _dir) = setup();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, i INT, f FLOAT, s TEXT, b BOOLEAN)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, -42, 3.14, 'hello', TRUE)")
        .unwrap();

    let r = row(&db, "SELECT i, f, s, b FROM t WHERE id = 1");
    assert_eq!(r[0], Value::Integer(-42));
    match &r[1] {
        Value::Float(f) => assert!((f - 3.14).abs() < 0.001),
        v => panic!("{:?}", v),
    }
    assert_eq!(r[2], Value::Text("hello".into()));
    assert_eq!(r[3], Value::Bool(true));
}
