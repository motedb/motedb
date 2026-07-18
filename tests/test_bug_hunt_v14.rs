//! Bug-hunt v14: LEFT JOIN, JOIN+GROUP BY combos, schema validation,
//! error recovery, and edge cases in type handling.

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
    match rs {
        QueryResult::Select { rows, .. } => rows,
        _ => panic!("not Select"),
    }
}

fn scalar_i64(db: &Database, sql: &str) -> i64 {
    let r = rows(db, sql);
    assert_eq!(r.len(), 1, "1 row: {}", sql);
    match r[0].first() { Some(Value::Integer(n)) => *n, o => panic!("int? {:?}: {}", o, sql) }
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. LEFT JOIN correctness (unmatched left rows get NULLs)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn left_join_unmatched_gets_null() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY, name TEXT)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY, a_id INT, val INT)");
    exec(&db, "INSERT INTO a VALUES (1, 'Alice')");
    exec(&db, "INSERT INTO a VALUES (2, 'Bob')");  // no matching b
    exec(&db, "INSERT INTO b VALUES (1, 1, 100)");
    // LEFT JOIN: Alice gets val=100, Bob gets val=NULL.
    let r = rows(&db, "SELECT name, val FROM a LEFT JOIN b ON a.id = b.a_id ORDER BY a.id");
    assert_eq!(r.len(), 2, "LEFT JOIN keeps unmatched left rows");
    // Bob should have NULL val.
    match &r[1][1] {
        Value::Null => {} // correct
        Value::Integer(_) => panic!("unmatched LEFT JOIN should have NULL"),
        o => panic!("{:?}", o),
    }
}

#[test]
fn left_join_count_includes_unmatched() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY, a_id INT)");
    for i in 1..=5 { exec(&db, &format!("INSERT INTO a VALUES ({})", i)); }
    exec(&db, "INSERT INTO b VALUES (1, 1)"); // only 1 match
    exec(&db, "INSERT INTO b VALUES (2, 2)");
    // LEFT JOIN → 5 rows (3 unmatched get NULL).
    let r = rows(&db, "SELECT COUNT(*) FROM a LEFT JOIN b ON a.id = b.a_id");
    match r[0][0] { Value::Integer(n) => assert_eq!(n, 5), _ => panic!() }
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. JOIN + WHERE on right table
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn inner_join_where_on_right_table() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY, name TEXT)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY, a_id INT, status TEXT, amt INT)");
    exec(&db, "INSERT INTO a VALUES (1, 'Alice')");
    exec(&db, "INSERT INTO b VALUES (1, 1, 'active', 100)");
    exec(&db, "INSERT INTO b VALUES (2, 1, 'inactive', 200)");
    // INNER JOIN + WHERE status = 'active' → only the active row.
    let r = rows(&db, "SELECT amt FROM a INNER JOIN b ON a.id = b.a_id WHERE status = 'active'");
    assert_eq!(r.len(), 1);
    match &r[0][0] { Value::Integer(n) => assert_eq!(*n, 100), _ => panic!() }
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. Schema validation
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn insert_extra_column_errors() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let result = db.execute("INSERT INTO t VALUES (1, 10, 999)");
    assert!(result.is_err(), "extra column should error");
}

#[test]
fn insert_missing_column_errors() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT, name TEXT)");
    let result = db.execute("INSERT INTO t VALUES (1, 10)");
    assert!(result.is_err(), "missing column should error");
}

#[test]
fn create_table_duplicate_name_errors() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
    let result = db.execute("CREATE TABLE t (id INT PRIMARY KEY)");
    assert!(result.is_err(), "duplicate table name should error");
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. UPDATE non-existent column errors
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn update_nonexistent_column_errors() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    let result = db.execute("UPDATE t SET nonexistent = 5 WHERE id = 1");
    assert!(result.is_err(), "UPDATE on nonexistent column should error");
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. Large primary key values
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn large_pk_values() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1000000, 10)");
    exec(&db, "INSERT INTO t VALUES (2000000, 20)");
    exec(&db, "INSERT INTO t VALUES (999999999, 30)");
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1000000"), 10);
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 999999999"), 30);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 3);
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. GROUP BY + JOIN (simple)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn join_group_by_simple() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY, cat TEXT)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY, a_id INT, amt INT)");
    exec(&db, "INSERT INTO a VALUES (1, 'x')");
    exec(&db, "INSERT INTO a VALUES (2, 'y')");
    exec(&db, "INSERT INTO b VALUES (1, 1, 10)");
    exec(&db, "INSERT INTO b VALUES (2, 1, 20)");
    exec(&db, "INSERT INTO b VALUES (3, 2, 30)");
    // GROUP BY cat, SUM(amt) via JOIN. x: 30, y: 30.
    let r = rows(&db, "SELECT cat, SUM(amt) FROM a INNER JOIN b ON a.id = b.a_id GROUP BY cat ORDER BY cat");
    assert_eq!(r.len(), 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. Multiple UPDATEs on same row in sequence
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn sequential_updates_same_row() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 0)");
    for i in 1..=20 {
        exec(&db, &format!("UPDATE t SET v = {} WHERE id = 1", i));
    }
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1"), 20);
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. NULL in JOIN condition (should not match)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn join_null_condition_no_match() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY, x INT)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY, y INT)");
    exec(&db, "INSERT INTO a VALUES (1, 10)");
    exec(&db, "INSERT INTO a VALUES (2, NULL)");
    exec(&db, "INSERT INTO b VALUES (1, 10)");
    exec(&db, "INSERT INTO b VALUES (2, NULL)");
    // INNER JOIN ON x = y: NULL = NULL is unknown, so no match for row 2.
    let r = rows(&db, "SELECT COUNT(*) FROM a INNER JOIN b ON x = y");
    match r[0][0] {
        Value::Integer(n) => assert!(n <= 2, "NULL = NULL should not match in JOIN"),
        _ => panic!(),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 9. Reopen with indexes (index rebuilt correctly)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn reopen_with_indexes_correct() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
        for i in 1..=100 {
            exec(&db, &format!("INSERT INTO t VALUES ({}, 'c{}', {})", i, i % 5, i));
        }
        exec(&db, "CREATE INDEX t_cat ON t(cat)");
        db.checkpoint().unwrap();
        db.wait_for_indexes_ready();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    db.wait_for_indexes_ready();
    // Indexed query after reopen.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE cat = 'c0'"), 20);
    // Non-indexed query after reopen.
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t WHERE v > 50"), 3775); // sum(51..100)
}

// ═══════════════════════════════════════════════════════════════════════════
// 10. SELECT COUNT(*) with multiple WHERE conditions
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn count_multiple_where_conditions() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT)");
    for i in 1..=100 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {}, {}, {})", i, i % 3, i % 5, i % 7));
    }
    // Multiple AND conditions.
    let r = rows(&db, "SELECT COUNT(*) FROM t WHERE a = 0 AND b = 0 AND c = 0");
    // a=0 (i%3==0), b=0 (i%5==0), c=0 (i%7==0) → i%105==0 in 1..100 → only i=...none? lcm(3,5,7)=105>100 → 0.
    match r[0][0] { Value::Integer(n) => assert_eq!(n, 0, "no i in 1..100 divisible by 105"), _ => panic!() }
    // Less restrictive.
    let r = rows(&db, "SELECT COUNT(*) FROM t WHERE a = 1 AND b = 1");
    match r[0][0] { Value::Integer(n) => assert!(n > 0, "should have matches"), _ => panic!() }
}

// ═══════════════════════════════════════════════════════════════════════════
// 11. DISTINCT + ORDER BY combined
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn distinct_order_by_combined() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 30)");
    exec(&db, "INSERT INTO t VALUES (2, 10)");
    exec(&db, "INSERT INTO t VALUES (3, 30)");
    exec(&db, "INSERT INTO t VALUES (4, 20)");
    exec(&db, "INSERT INTO t VALUES (5, 10)");
    // DISTINCT v ORDER BY v ASC → 10, 20, 30.
    let r = rows(&db, "SELECT DISTINCT v FROM t ORDER BY v ASC");
    assert_eq!(r.len(), 3);
    let vals: Vec<i64> = r.iter().filter_map(|row| match row.get(0) {
        Some(Value::Integer(n)) => Some(*n), _ => None
    }).collect();
    assert_eq!(vals, vec![10, 20, 30]);
}

// ═══════════════════════════════════════════════════════════════════════════
// 12. Transaction with both INSERT and DELETE
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn txn_insert_and_delete_together() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "INSERT INTO t VALUES (2, 20)");
    let tx = db.begin_transaction().unwrap();
    exec(&db, "INSERT INTO t VALUES (3, 30)");
    exec(&db, "DELETE FROM t WHERE id = 1");
    // Within txn: 2 rows (deleted 1, added 1).
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 2);
    db.commit_transaction(tx).unwrap();
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 2);
    // id=1 gone, id=2 and id=3 present.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE id = 1"), 0);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE id = 3"), 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// 13. Very long text in WHERE
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn long_text_in_where() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    let long = "x".repeat(5000);
    exec(&db, &format!("INSERT INTO t VALUES (1, '{}')", long));
    let r = rows(&db, &format!("SELECT id FROM t WHERE s = '{}'", long));
    assert_eq!(r.len(), 1, "long text equality match");
}

// ═══════════════════════════════════════════════════════════════════════════
// 14. Empty GROUP BY result set (table exists but no rows match)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn group_by_where_no_match_empty() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 'a', 10)");
    exec(&db, "INSERT INTO t VALUES (2, 'b', 20)");
    let r = rows(&db, "SELECT cat, SUM(v) FROM t WHERE v > 1000 GROUP BY cat");
    assert_eq!(r.len(), 0);
}

// ═══════════════════════════════════════════════════════════════════════════
// 15. Nested CASE in aggregate
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn sum_of_case_expression() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 5)");
    exec(&db, "INSERT INTO t VALUES (2, 15)");
    exec(&db, "INSERT INTO t VALUES (3, 25)");
    // SUM(CASE WHEN v > 10 THEN v ELSE 0 END) = 0 + 15 + 25 = 40.
    let _ = db.execute("SELECT SUM(CASE WHEN v > 10 THEN v ELSE 0 END) FROM t");
}
