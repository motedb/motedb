//! Bug-hunt v3: deeper edge cases. Targets areas most likely to have silent
//! bugs: NULL in aggregates with GROUP BY, float precision, ORDER BY stability,
//! negative numbers, zero, multi-column PKs, and transaction re-entrancy.

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
    match rs { QueryResult::Select { rows, .. } => rows, _ => panic!("not Select: {}", sql) }
}

fn scalar_i64(db: &Database, sql: &str) -> i64 {
    let r = rows(db, sql);
    assert_eq!(r.len(), 1, "1 row expected: {}", sql);
    match r[0].first() { Some(Value::Integer(n)) => *n, o => panic!("not int {:?}: {}", o, sql) }
}

fn scalar_f64(db: &Database, sql: &str) -> f64 {
    let r = rows(db, sql);
    assert_eq!(r.len(), 1);
    match r[0].first() { Some(Value::Float(n)) => *n, o => panic!("not float {:?}: {}", o, sql) }
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. GROUP BY with NULL keys
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn group_by_null_key() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 'a', 10)");
    exec(&db, "INSERT INTO t VALUES (2, NULL, 20)");
    exec(&db, "INSERT INTO t VALUES (3, NULL, 30)");
    exec(&db, "INSERT INTO t VALUES (4, 'a', 40)");
    let r = rows(&db, "SELECT cat, COUNT(*) FROM t GROUP BY cat ORDER BY cat");
    // NULL is its own group: NULL→2, 'a'→2. Must not crash or merge.
    // Just verify we get 2 groups.
    assert_eq!(r.len(), 2, "NULL should form its own group");
}

#[test]
fn group_by_count_distinct() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    for (id, c, v) in [(1, "a", 1), (2, "a", 1), (3, "a", 2), (4, "b", 1)].iter() {
        exec(&db, &format!("INSERT INTO t VALUES ({}, '{}', {})", id, c, v));
    }
    // COUNT(DISTINCT v) per group.
    let r = rows(&db, "SELECT cat, COUNT(DISTINCT v) FROM t GROUP BY cat ORDER BY cat");
    assert_eq!(r.len(), 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. Float precision & special values
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn float_precision_roundtrip() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)");
    let precise = 3.141592653589793_f64;
    exec(&db, &format!("INSERT INTO t VALUES (1, {:.17})", precise));
    let r = rows(&db, "SELECT v FROM t WHERE id = 1");
    match r[0][0] {
        Value::Float(n) => assert!((n - precise).abs() < 1e-15, "precision lost: {} vs {}", n, precise),
        _ => panic!("not float"),
    }
}

#[test]
fn float_zero_and_negative() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)");
    exec(&db, "INSERT INTO t VALUES (1, 0.0)");
    exec(&db, "INSERT INTO t VALUES (2, -0.0)");
    exec(&db, "INSERT INTO t VALUES (3, -3.14)");
    assert_eq!(scalar_f64(&db, "SELECT v FROM t WHERE id = 1"), 0.0);
    assert_eq!(scalar_f64(&db, "SELECT v FROM t WHERE id = 3"), -3.14);
    // 0.0 and -0.0 should both be findable.
    assert_eq!(rows(&db, "SELECT id FROM t WHERE v = 0").len(), 2);
}

#[test]
fn float_very_small_large() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)");
    exec(&db, "INSERT INTO t VALUES (1, 1e-300)");
    exec(&db, "INSERT INTO t VALUES (2, 1e300)");
    assert_eq!(scalar_f64(&db, "SELECT v FROM t WHERE id = 1"), 1e-300);
    assert_eq!(scalar_f64(&db, "SELECT v FROM t WHERE id = 2"), 1e300);
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. ORDER BY on mixed/edge values
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn order_by_with_nulls() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 30)");
    exec(&db, "INSERT INTO t VALUES (2, NULL)");
    exec(&db, "INSERT INTO t VALUES (3, 10)");
    exec(&db, "INSERT INTO t VALUES (4, NULL)");
    exec(&db, "INSERT INTO t VALUES (5, 20)");
    // ORDER BY v — NULLs must not crash. Verify it returns all 5 rows.
    let r = rows(&db, "SELECT id FROM t ORDER BY v");
    assert_eq!(r.len(), 5, "ORDER BY with NULLs must return all rows");
}

#[test]
fn order_by_desc_with_nulls() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "INSERT INTO t VALUES (2, NULL)");
    exec(&db, "INSERT INTO t VALUES (3, 20)");
    let r = rows(&db, "SELECT id FROM t ORDER BY v DESC");
    assert_eq!(r.len(), 3);
}

#[test]
fn order_by_text() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    for (i, s) in [(1, "banana"), (2, "apple"), (3, "cherry")].iter() {
        exec(&db, &format!("INSERT INTO t VALUES ({}, '{}')", i, s));
    }
    let r = rows(&db, "SELECT s FROM t ORDER BY s ASC");
    let names: Vec<String> = r.iter().filter_map(|row| match &row[0] {
        Value::Text(s) => Some(s.0.to_string()),
        _ => None,
    }).collect();
    assert_eq!(names, vec!["apple", "banana", "cherry"]);
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. Integer boundaries
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn i64_max_min_roundtrip() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 9223372036854775807)"); // i64::MAX
    exec(&db, "INSERT INTO t VALUES (2, -9223372036854775808)"); // i64::MIN
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1"), i64::MAX);
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 2"), i64::MIN);
}

#[test]
fn pk_zero() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (0, 99)");
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 0"), 99);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. WHERE with AND / OR / NOT precedence
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn where_and_or_precedence() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10, 10)");
    exec(&db, "INSERT INTO t VALUES (2, 10, 20)");
    exec(&db, "INSERT INTO t VALUES (3, 20, 10)");
    exec(&db, "INSERT INTO t VALUES (4, 20, 20)");
    // a = 10 AND (b = 10 OR b = 20) → rows 1, 2
    let r = rows(&db, "SELECT id FROM t WHERE a = 10 AND (b = 10 OR b = 20)");
    assert_eq!(r.len(), 2);
    // a = 10 OR b = 10 → rows 1, 2, 3
    let r = rows(&db, "SELECT id FROM t WHERE a = 10 OR b = 10");
    assert_eq!(r.len(), 3);
}

#[test]
fn where_not_equal_combined() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=5 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i * 10));
    }
    // NOT (v = 10 OR v = 50) → rows 2, 3, 4
    let r = rows(&db, "SELECT id FROM t WHERE NOT (v = 10 OR v = 50)");
    assert_eq!(r.len(), 3);
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. IN clause
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn where_in_integers() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=10 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    let r = rows(&db, "SELECT id FROM t WHERE v IN (3, 5, 7) ORDER BY id");
    assert_eq!(r.len(), 3);
}

#[test]
fn where_in_empty_list() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    // IN () — some DBs error, some return empty. Just ensure no panic.
    let _ = db.execute("SELECT * FROM t WHERE v IN ()");
}

#[test]
fn where_not_in() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=5 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    let r = rows(&db, "SELECT id FROM t WHERE v NOT IN (1, 3, 5) ORDER BY id");
    assert_eq!(r.len(), 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. BETWEEN
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn where_between_inclusive() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=10 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    let r = rows(&db, "SELECT id FROM t WHERE v BETWEEN 3 AND 7 ORDER BY id");
    assert_eq!(r.len(), 5, "BETWEEN is inclusive");
}

#[test]
fn where_not_between() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=10 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    let r = rows(&db, "SELECT id FROM t WHERE v NOT BETWEEN 3 AND 7 ORDER BY id");
    assert_eq!(r.len(), 5);
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. Transaction commit visibility across operations
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn txn_commit_then_new_txn_sees_data() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let tx1 = db.begin_transaction().unwrap();
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    db.commit_transaction(tx1).unwrap();
    // New transaction must see committed data.
    let tx2 = db.begin_transaction().unwrap();
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 1);
    exec(&db, "INSERT INTO t VALUES (2, 20)");
    db.commit_transaction(tx2).unwrap();
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 2);
}

#[test]
fn txn_rollback_does_not_block_later_ops() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let tx1 = db.begin_transaction().unwrap();
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    db.rollback_transaction(tx1).unwrap();
    // After rollback, normal ops must work.
    exec(&db, "INSERT INTO t VALUES (2, 20)");
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 1);
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 2"), 20);
}

// ═══════════════════════════════════════════════════════════════════════════
// 9. Multiple tables interaction
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn multi_table_independent() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY, v INT)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO a VALUES (1, 100)");
    exec(&db, "INSERT INTO b VALUES (1, 200)");
    assert_eq!(scalar_i64(&db, "SELECT v FROM a WHERE id = 1"), 100);
    assert_eq!(scalar_i64(&db, "SELECT v FROM b WHERE id = 1"), 200);
    exec(&db, "UPDATE a SET v = 999 WHERE id = 1");
    // b must be unaffected.
    assert_eq!(scalar_i64(&db, "SELECT v FROM b WHERE id = 1"), 200);
    assert_eq!(scalar_i64(&db, "SELECT v FROM a WHERE id = 1"), 999);
}

#[test]
fn drop_table_then_recreate() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    // Just ensure DROP+recreate doesn't leave stale state.
    let _ = db.execute("DROP TABLE t");
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 20)");
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 1);
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1"), 20);
}

// ═══════════════════════════════════════════════════════════════════════════
// 10. SELECT projection edge cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn select_column_order_matches_schema() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10, 20, 30)");
    // Select in different order than schema.
    let r = rows(&db, "SELECT c, a, b FROM t WHERE id = 1");
    assert_eq!(r.len(), 1);
    // c=30, a=10, b=20
    match (&r[0][0], &r[0][1], &r[0][2]) {
        (Value::Integer(c), Value::Integer(a), Value::Integer(b)) => {
            assert_eq!(*c, 30);
            assert_eq!(*a, 10);
            assert_eq!(*b, 20);
        }
        o => panic!("wrong types: {:?}", o),
    }
}

#[test]
fn select_same_column_twice() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 42)");
    let r = rows(&db, "SELECT v, v FROM t WHERE id = 1");
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].len(), 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// 11. Arithmetic in SELECT
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn select_arithmetic() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10, 3)");
    let r = rows(&db, "SELECT a + b, a - b, a * b FROM t WHERE id = 1");
    assert_eq!(r.len(), 1);
    match (&r[0][0], &r[0][1], &r[0][2]) {
        (Value::Integer(sum), Value::Integer(diff), Value::Integer(prod)) => {
            assert_eq!(*sum, 13);
            assert_eq!(*diff, 7);
            assert_eq!(*prod, 30);
        }
        o => panic!("expected ints: {:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 12. Repeated queries (cache correctness)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn repeated_select_after_update() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    // Run same query multiple times, then update, then again — caches must
    // not return stale results.
    for _ in 0..5 {
        assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1"), 10);
    }
    exec(&db, "UPDATE t SET v = 999 WHERE id = 1");
    for _ in 0..5 {
        assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1"), 999, "stale cache after UPDATE");
    }
}

#[test]
fn repeated_count_after_insert_delete() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
    exec(&db, "INSERT INTO t VALUES (1)");
    for _ in 0..3 {
        assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 1);
    }
    exec(&db, "INSERT INTO t VALUES (2)");
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 2, "COUNT must update after INSERT");
    exec(&db, "DELETE FROM t WHERE id = 1");
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 1, "COUNT must update after DELETE");
}

// ═══════════════════════════════════════════════════════════════════════════
// 13. Large text values
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn large_text_10kb() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    let big = "x".repeat(10240);
    exec(&db, &format!("INSERT INTO t VALUES (1, '{}')", big));
    let r = rows(&db, "SELECT s FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Text(s) => assert_eq!(s.0.len(), 10240),
        _ => panic!("not text"),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 14. Close and reopen with checkpoint in between
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn multiple_reopen_cycles() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
        exec(&db, "INSERT INTO t VALUES (1, 10)");
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    // Reopen #1
    {
        let db = Database::open(&path).unwrap();
        assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1"), 10);
        exec(&db, "INSERT INTO t VALUES (2, 20)");
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    // Reopen #2
    {
        let db = Database::open(&path).unwrap();
        assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 2);
        db.close().unwrap();
    }
}
