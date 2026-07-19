//! Bug-hunt v20: floats in aggregates, negative numbers, NULL in batch
//! INSERT, mixed AND/OR precedence, subquery in SELECT column, and
//! schema/DDL edge cases.

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
    db.execute(sql)
        .unwrap_or_else(|e| panic!("SQL failed: {}\n  err: {}", sql, e));
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    let rs = db
        .execute(sql)
        .unwrap_or_else(|e| panic!("SQL failed: {}\n  err: {}", sql, e))
        .materialize()
        .unwrap_or_else(|e| panic!("materialize failed: {}\n  err: {}", sql, e));
    match rs {
        QueryResult::Select { rows, .. } => rows,
        _ => panic!("expected Select for: {}", sql),
    }
}

fn scalar_i64(db: &Database, sql: &str) -> i64 {
    let r = rows(db, sql);
    assert_eq!(r.len(), 1, "expected 1 row: {}", sql);
    match r[0].first() {
        Some(Value::Integer(n)) => *n,
        o => panic!("expected int, got {:?}: {}", o, sql),
    }
}

fn scalar_f64(db: &Database, sql: &str) -> f64 {
    let r = rows(db, sql);
    assert_eq!(r.len(), 1, "expected 1 row: {}", sql);
    match r[0].first() {
        Some(Value::Float(n)) => *n,
        Some(Value::Integer(n)) => *n as f64,
        o => panic!("expected float, got {:?}: {}", o, sql),
    }
}

fn ids_sorted(db: &Database, sql: &str) -> Vec<i64> {
    let r = rows(db, sql);
    let mut ids: Vec<i64> = r
        .iter()
        .filter_map(|row| match row.first() {
            Some(Value::Integer(n)) => Some(*n),
            _ => None,
        })
        .collect();
    ids.sort();
    ids
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION A: Float precision in aggregates
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn sum_floats_exact() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, f FLOAT)");
    exec(&db, "INSERT INTO t VALUES (1, 0.1), (2, 0.2), (3, 0.3)");
    let f = scalar_f64(&db, "SELECT SUM(f) FROM t");
    // 0.1+0.2+0.3 = 0.6 but floating point has tiny error.
    assert!((f - 0.6).abs() < 1e-6, "got {}", f);
}

#[test]
fn avg_float_exact() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, f FLOAT)");
    exec(&db, "INSERT INTO t VALUES (1, 1.0), (2, 2.0), (3, 3.0), (4, 4.0)");
    let f = scalar_f64(&db, "SELECT AVG(f) FROM t");
    assert!((f - 2.5).abs() < 1e-6);
}

#[test]
fn sum_mixed_int_float_promotes_to_float() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, i INT, f FLOAT)");
    exec(&db, "INSERT INTO t VALUES (1, 10, 0.5)");
    // SUM(i) = 10 (int), SUM(f) = 0.5 (float). SUM(i + f) = 10.5.
    let f = scalar_f64(&db, "SELECT i + f FROM t WHERE id = 1");
    assert!((f - 10.5).abs() < 1e-6);
}

#[test]
fn min_max_on_float_column() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, f FLOAT)");
    exec(&db, "INSERT INTO t VALUES (1, -1.5), (2, 2.5), (3, 0.0)");
    let min = scalar_f64(&db, "SELECT MIN(f) FROM t");
    let max = scalar_f64(&db, "SELECT MAX(f) FROM t");
    assert!((min - (-1.5)).abs() < 1e-6);
    assert!((max - 2.5).abs() < 1e-6);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION B: Negative numbers / unary minus
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn negative_literal_in_where() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, -10), (2, -20), (3, 30)");
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE v < -5");
    assert_eq!(ids, vec![1, 2]);
}

#[test]
fn negative_literal_in_insert() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, -100)");
    let n = scalar_i64(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(n, -100);
}

#[test]
fn unary_minus_on_column() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 5)");
    let n = scalar_i64(&db, "SELECT -v FROM t WHERE id = 1");
    assert_eq!(n, -5);
}

#[test]
fn double_negation() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 42)");
    let n = scalar_i64(&db, "SELECT - -v FROM t WHERE id = 1");
    assert_eq!(n, 42);
}

#[test]
fn negative_in_arithmetic() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    let n = scalar_i64(&db, "SELECT v - -5 FROM t WHERE id = 1");
    // 10 - (-5) = 15.
    assert_eq!(n, 15);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION C: NULL in batch INSERT
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn batch_insert_with_nulls() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b TEXT)");
    exec(
        &db,
        "INSERT INTO t VALUES (1, NULL, 'x'), (2, 5, NULL), (3, NULL, NULL)",
    );
    let r = rows(&db, "SELECT a, b FROM t ORDER BY id");
    assert!(matches!(&r[0][0], Value::Null));
    assert!(matches!(&r[0][1], Value::Text(t) if t.as_str() == "x"));
    assert!(matches!(&r[1][0], Value::Integer(5)));
    assert!(matches!(&r[1][1], Value::Null));
    assert!(matches!(&r[2][0], Value::Null));
    assert!(matches!(&r[2][1], Value::Null));
}

#[test]
fn count_after_batch_insert_with_nulls() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30), (4, NULL)");
    let star = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    let col = scalar_i64(&db, "SELECT COUNT(v) FROM t");
    assert_eq!(star, 4);
    assert_eq!(col, 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION D: AND/OR precedence
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn and_binds_tighter_than_or() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO t VALUES (1, 1, 1), (2, 1, 0), (3, 0, 1), (4, 0, 0)");
    // a = 1 OR a = 0 AND b = 0  →  a = 1 OR (a = 0 AND b = 0)
    // id=1 (a=1) ✓; id=2 (a=1) ✓; id=3 (a=0,b=1) ✗; id=4 (a=0,b=0) ✓.
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE a = 1 OR a = 0 AND b = 0");
    assert_eq!(ids, vec![1, 2, 4]);
}

#[test]
fn or_with_parens() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO t VALUES (1, 1, 1), (2, 1, 0), (3, 0, 1), (4, 0, 0)");
    // (a = 1 OR a = 0) AND b = 0  → all rows where b=0.
    // id=2 (b=0) ✓; id=4 (b=0) ✓.
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE (a = 1 OR a = 0) AND b = 0");
    assert_eq!(ids, vec![2, 4]);
}

#[test]
fn nested_and_or() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT)");
    exec(&db, "INSERT INTO t VALUES \
        (1, 1, 1, 1), \
        (2, 1, 1, 0), \
        (3, 0, 1, 1), \
        (4, 0, 0, 1)");
    // (a = 1 AND b = 1) OR (c = 1 AND a = 0)
    // id=1 (a=1,b=1) ✓; id=2 (a=1,b=1) ✓; id=3 (c=1,a=0) ✓; id=4 (c=1,a=0) ✓.
    let ids = ids_sorted(
        &db,
        "SELECT id FROM t WHERE (a = 1 AND b = 1) OR (c = 1 AND a = 0)",
    );
    assert_eq!(ids, vec![1, 2, 3, 4]);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION E: Subquery in SELECT column (uncorrelated)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn uncorrelated_subquery_in_select_column() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    let r = rows(
        &db,
        "SELECT id, (SELECT MAX(v) FROM t) AS mx FROM t ORDER BY id",
    );
    assert_eq!(r.len(), 3);
    // mx = 30 for all rows.
    for row in &r {
        assert!(matches!(&row[1], Value::Integer(30)));
    }
}

#[test]
fn subquery_in_select_with_literal() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    let n = scalar_i64(&db, "SELECT (SELECT 42) FROM t WHERE id = 1");
    assert_eq!(n, 42);
}

#[test]
fn scalar_subquery_empty_returns_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    let r = rows(
        &db,
        "SELECT id, (SELECT v FROM t WHERE id = 999) AS x FROM t WHERE id = 1",
    );
    // Inner returns no rows → NULL.
    assert!(matches!(&r[0][1], Value::Null));
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION F: DDL edge cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn create_table_if_not_exists_idempotent() {
    let (db, _dir) = new_db();
    let r1 = db.execute("CREATE TABLE t (id INT PRIMARY KEY)");
    let r2 = db.execute("CREATE TABLE IF NOT EXISTS t (id INT PRIMARY KEY)");
    // First succeeds; second with IF NOT EXISTS should also succeed (no-op).
    assert!(r1.is_ok());
    assert!(r2.is_ok(), "CREATE TABLE IF NOT EXISTS should be idempotent");
}

#[test]
fn create_table_duplicate_errors() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
    let r = db.execute("CREATE TABLE t (id INT PRIMARY KEY)");
    assert!(r.is_err(), "duplicate CREATE TABLE must error");
}

#[test]
fn drop_table_if_exists() {
    let (db, _dir) = new_db();
    let r = db.execute("DROP TABLE IF EXISTS nonexistent");
    assert!(r.is_ok(), "DROP TABLE IF EXISTS should be no-op for missing table");
}

#[test]
fn drop_nonexistent_errors() {
    let (db, _dir) = new_db();
    let r = db.execute("DROP TABLE nonexistent");
    assert!(r.is_err(), "DROP TABLE on missing must error");
}

#[test]
fn recreate_table_after_drop() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "DROP TABLE t");
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, 0, "recreated table should be empty");
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION G: Type-coercion in arithmetic results
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn int_plus_int_stays_int() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO t VALUES (1, 5, 3)");
    let r = rows(&db, "SELECT a + b FROM t WHERE id = 1");
    assert!(matches!(&r[0][0], Value::Integer(8)));
}

#[test]
fn int_minus_negative() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT)");
    exec(&db, "INSERT INTO t VALUES (1, 5)");
    let r = rows(&db, "SELECT a - 10 FROM t WHERE id = 1");
    assert!(matches!(&r[0][0], Value::Integer(-5)));
}

#[test]
fn multiplication_overflow_does_not_crash() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 2000000000)"); // ~2 billion
    // 2e9 * 2e9 overflows i64? Actually i64 max is ~9.2e18, 2e9*2e9 = 4e18, fits.
    let r = db.execute("SELECT v * v FROM t WHERE id = 1");
    // Should not panic. May wrap if implementation uses i32 internally.
    assert!(r.is_ok(), "large multiplication must not crash");
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION H: GROUP BY with multiple aggregates
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn group_by_with_multiple_aggregates() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, g INT, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, 40)");
    let r = rows(
        &db,
        "SELECT g, COUNT(*), SUM(v), AVG(v), MIN(v), MAX(v) FROM t GROUP BY g ORDER BY g",
    );
    assert_eq!(r.len(), 2);
    // Group 1: count=2, sum=30, avg=15, min=10, max=20.
    assert!(matches!(&r[0][0], Value::Integer(1)));
    assert!(matches!(&r[0][1], Value::Integer(2)));
    assert!(matches!(&r[0][2], Value::Integer(30)));
    match &r[0][3] {
        Value::Float(f) => assert!((f - 15.0).abs() < 1e-6),
        Value::Integer(n) => assert_eq!(*n, 15),
        o => panic!("{:?}", o),
    }
    assert!(matches!(&r[0][4], Value::Integer(10)));
    assert!(matches!(&r[0][5], Value::Integer(20)));
}

#[test]
fn group_by_with_having_and_multiple_aggregates() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, g INT, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, 40), (5, 3, 5)");
    let r = rows(
        &db,
        "SELECT g, SUM(v) FROM t GROUP BY g HAVING SUM(v) > 25 ORDER BY g",
    );
    // sums: g1=30, g2=70, g3=5. HAVING > 25 → g1, g2.
    assert_eq!(r.len(), 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION I: ORDER BY edge cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn order_by_with_limit_equal_to_count() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    let r = rows(&db, "SELECT id FROM t ORDER BY v DESC LIMIT 3");
    assert_eq!(r.len(), 3);
}

#[test]
fn order_by_limit_larger_than_count() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20)");
    let r = rows(&db, "SELECT id FROM t ORDER BY v LIMIT 100");
    assert_eq!(r.len(), 2, "LIMIT > row count returns all rows");
}

#[test]
fn order_by_with_offset() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
    let r = rows(&db, "SELECT id FROM t ORDER BY v OFFSET 2");
    // Skip first 2 (v=10,20), return v=30,40 → ids 3,4.
    assert_eq!(r.len(), 2);
    let ids: Vec<i64> = r
        .iter()
        .filter_map(|r| match r.first() {
            Some(Value::Integer(n)) => Some(*n),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec![3, 4]);
}

#[test]
fn order_by_with_limit_and_offset() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
    let r = rows(&db, "SELECT id FROM t ORDER BY v LIMIT 2 OFFSET 1");
    // Skip 1 (v=10), take 2 (v=20,30) → ids 2,3.
    assert_eq!(r.len(), 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION J: Empty table behavior
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn select_from_empty_table() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let r = rows(&db, "SELECT * FROM t");
    assert_eq!(r.len(), 0);
}

#[test]
fn count_empty_table() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, 0);
}

#[test]
fn sum_empty_table_returns_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let r = rows(&db, "SELECT SUM(v) FROM t");
    // SQL standard: SUM over empty set is NULL.
    assert!(
        matches!(&r[0][0], Value::Null),
        "SUM of empty should be NULL, got {:?}",
        r[0][0]
    );
}

#[test]
fn max_empty_table_returns_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let r = rows(&db, "SELECT MAX(v) FROM t");
    assert!(matches!(&r[0][0], Value::Null));
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION K: UPDATE returning correct values
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn update_with_expression() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    db.execute("UPDATE t SET v = v * 2 + 1 WHERE id = 1").unwrap();
    let n = scalar_i64(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(n, 21);
}

#[test]
fn update_all_rows_no_where() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    db.execute("UPDATE t SET v = 0").unwrap();
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE v = 0");
    assert_eq!(n, 3);
}

#[test]
fn update_multiple_columns() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10, 20)");
    db.execute("UPDATE t SET a = 100, b = 200 WHERE id = 1").unwrap();
    let r = rows(&db, "SELECT a, b FROM t WHERE id = 1");
    assert!(matches!(&r[0][0], Value::Integer(100)));
    assert!(matches!(&r[0][1], Value::Integer(200)));
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION L: IS NULL / IS NOT NULL
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn is_null_finds_nulls() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30)");
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE v IS NULL");
    assert_eq!(ids, vec![2]);
}

#[test]
fn is_not_null_excludes_nulls() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30)");
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE v IS NOT NULL");
    assert_eq!(ids, vec![1, 3]);
}

#[test]
fn count_is_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, NULL), (2, NULL), (3, 5)");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE v IS NULL");
    assert_eq!(n, 2);
}
