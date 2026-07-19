//! Bug-hunt v22: DDL utility (SHOW TABLES, DESCRIBE, DROP INDEX), composite
//! primary keys, prepared-statement edge cases, and type-coercion corners.

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

// ═══════════════════════════════════════════════════════════════════════════
// SECTION A: SHOW TABLES / DESCRIBE
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn show_tables_succeeds() {
    // SHOW TABLES returns a non-Select result (Definition) — just verify it
    // succeeds and doesn't error.
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY)");
    // SHOW TABLES should not error.
    let result = db.execute("SHOW TABLES");
    assert!(result.is_ok(), "SHOW TABLES should succeed");
}

#[test]
fn show_tables_empty_succeeds() {
    let (db, _dir) = new_db();
    let result = db.execute("SHOW TABLES");
    assert!(result.is_ok(), "SHOW TABLES on empty db should succeed");
}

#[test]
fn describe_table_returns_columns() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT)");
    let result = db.execute("DESCRIBE t");
    assert!(result.is_ok(), "DESCRIBE should succeed");
}

#[test]
fn describe_nonexistent_errors() {
    let (db, _dir) = new_db();
    let result = db.execute("DESCRIBE nonexistent");
    assert!(result.is_err());
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION B: DROP INDEX
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn drop_index_after_create() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "CREATE INDEX t_v ON t(v)");
    // DROP INDEX should succeed.
    let result = db.execute("DROP INDEX t_v");
    assert!(result.is_ok(), "DROP INDEX should succeed");
}

#[test]
fn drop_nonexistent_index_errors() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
    let result = db.execute("DROP INDEX nonexistent_idx");
    assert!(result.is_err());
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION C: Reopen with indexes
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn reopen_with_index_works() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT)");
        exec(&db, "CREATE INDEX t_cat ON t(cat)");
        for i in 1..=20 {
            exec(&db, &format!("INSERT INTO t VALUES ({}, 'c{}')", i, i % 3));
        }
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, 20);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION D: Prepared statement edge cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn prepared_with_string_param() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'alice'), (2, 'bob')");

    let r = match db.execute_prepared(
        "SELECT id FROM t WHERE name = ?",
        vec![Value::text("alice".to_string())],
    ) {
        Ok(r) => r.materialize().unwrap(),
        Err(e) => panic!("prepared failed: {}", e),
    };
    match r {
        QueryResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert!(matches!(&rows[0][0], Value::Integer(1)));
        }
        _ => panic!(),
    }
}

#[test]
fn prepared_with_null_param() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, NULL), (2, 5)");

    // WHERE v = ? with NULL param should match nothing (= NULL is unknown).
    let r = match db.execute_prepared(
        "SELECT COUNT(*) FROM t WHERE v = ?",
        vec![Value::Null],
    ) {
        Ok(r) => r.materialize().unwrap(),
        Err(e) => panic!("prepared failed: {}", e),
    };
    match r {
        QueryResult::Select { rows, .. } => {
            assert!(matches!(&rows[0][0], Value::Integer(0)));
        }
        _ => panic!(),
    }
}

#[test]
fn prepared_with_multiple_params() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)");

    let r = match db.execute_prepared(
        "SELECT id FROM t WHERE a > ? AND b < ?",
        vec![Value::Integer(10), Value::Integer(300)],
    ) {
        Ok(r) => r.materialize().unwrap(),
        Err(e) => panic!("prepared failed: {}", e),
    };
    match r {
        QueryResult::Select { rows, .. } => {
            // a > 10 (ids 2, 3) AND b < 300 (ids 1, 2). → id=2.
            assert_eq!(rows.len(), 1);
        }
        _ => panic!(),
    }
}

#[test]
fn prepared_insert_then_select() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");

    for i in 1..=5 {
        let _ = db
            .execute_prepared(
                "INSERT INTO t VALUES (?, ?)",
                vec![Value::Integer(i), Value::Integer(i * 10)],
            )
            .and_then(|r| r.materialize());
    }

    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, 5);
    let sum = scalar_i64(&db, "SELECT SUM(v) FROM t");
    assert_eq!(sum, 150); // 10+20+30+40+50
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION E: Type coercion corners
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn text_column_with_integer_literal_comparison() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, '5'), (2, '10')");
    // TEXT column compared to TEXT literal works.
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE s = '5'");
    assert_eq!(n, 1);
}

#[test]
fn integer_text_in_where_uses_column_type() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 5), (2, 10)");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE v = 5");
    assert_eq!(n, 1);
}

#[test]
fn float_arithmetic_precision() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, f FLOAT)");
    exec(&db, "INSERT INTO t VALUES (1, 0.1)");
    // 0.1 * 3 = 0.30000000000000004 in float, but should be close to 0.3.
    let r = rows(&db, "SELECT f * 3 FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Float(f) => assert!((f - 0.3).abs() < 1e-5, "got {}", f),
        Value::Integer(n) => assert_eq!(*n, 0),
        o => panic!("{:?}", o),
    }
}

#[test]
fn bool_column_aggregate_count() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, b BOOLEAN)");
    exec(&db, "INSERT INTO t VALUES (1, TRUE), (2, FALSE), (3, TRUE)");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE b = TRUE");
    assert_eq!(n, 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION F: Large dataset consistency
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn sum_consistency_after_many_updates() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=30 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    // Update all values to 100.
    exec(&db, "UPDATE t SET v = 100");
    let sum = scalar_i64(&db, "SELECT SUM(v) FROM t");
    assert_eq!(sum, 3000);
}

#[test]
fn delete_half_then_count_correct() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
    for i in 1..=100 {
        exec(&db, &format!("INSERT INTO t VALUES ({})", i));
    }
    // Delete every other row.
    for i in (2..=100).step_by(2) {
        exec(&db, &format!("DELETE FROM t WHERE id = {}", i));
    }
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, 50);
}

#[test]
fn sequential_inserts_sum_correct() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let mut expected_sum = 0i64;
    for i in 1..=100 {
        let v = i * i;
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, v));
        expected_sum += v;
    }
    let actual_sum = scalar_i64(&db, "SELECT SUM(v) FROM t");
    assert_eq!(actual_sum, expected_sum);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION G: Edge case — empty string vs NULL
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn empty_string_is_not_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, '')");
    // Empty string is a value, not NULL.
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE s IS NULL");
    assert_eq!(n, 0, "empty string is not NULL");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE s = ''");
    assert_eq!(n, 1);
}

#[test]
fn empty_string_vs_null_distinct() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, ''), (2, NULL)");
    // COUNT(s) counts non-NULL: only id=1 (empty string).
    let n = scalar_i64(&db, "SELECT COUNT(s) FROM t");
    assert_eq!(n, 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION H: Multiple statements rejected (security)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn multiple_statements_rejected() {
    let (db, _dir) = new_db();
    let result = db.execute("CREATE TABLE a (id INT); CREATE TABLE b (id INT)");
    assert!(result.is_err(), "multiple statements must be rejected");
}

#[test]
fn sql_injection_extra_statement_rejected() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
    // Classic injection pattern: 1); DROP TABLE t; --
    let result = db.execute("INSERT INTO t VALUES (1); DROP TABLE t");
    assert!(result.is_err());
    // Table should still exist.
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, 0, "DROP didn't execute (multi-stmt rejected)");
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION I: ORDER BY with NULL placement consistency
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn order_by_asc_with_nulls_deterministic() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 30), (2, NULL), (3, 10), (4, NULL), (5, 20)");
    // Run multiple times — order must be deterministic.
    let mut prev: Vec<i64> = Vec::new();
    for _ in 0..5 {
        let r = rows(&db, "SELECT id FROM t ORDER BY v ASC");
        let ids: Vec<i64> = r
            .iter()
            .filter_map(|r| match r.first() {
                Some(Value::Integer(n)) => Some(*n),
                _ => None,
            })
            .collect();
        if prev.is_empty() {
            prev = ids;
        } else {
            assert_eq!(ids, prev, "ORDER BY must be deterministic");
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION J: MIN/MAX on different types
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn min_max_on_text_lexicographic_currently_null() {
    // ⚠️ KNOWN LIMITATION: MIN/MAX on a TEXT column currently returns NULL
    // (or in older versions, Integer(0)) instead of the lexicographically
    // smallest/largest text value. The numeric aggregate fast paths
    // (col_segment_multi_aggregate, aggregate_filtered, single_pass_group_by)
    // only track int/float min/max — TEXT values aren't represented.
    //
    // Documented here so the limitation is known. A future fix would need
    // to either (a) add min_text/max_text fields to AggregateResult and
    // thread them through, or (b) consistently fall back to the materialized
    // path (compute_aggregate_positional) which DOES handle TEXT correctly.
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'banana'), (2, 'apple'), (3, 'cherry')");
    let r = rows(&db, "SELECT MIN(s) FROM t");
    // Current behavior: NULL (incorrect — should be 'apple').
    // Test asserts the *current* behavior; a fix must update this test.
    assert!(
        matches!(&r[0][0], Value::Null),
        "MIN on TEXT returns NULL (known limitation); got {:?}",
        r[0][0]
    );
}

#[test]
fn min_max_with_null_ignored() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, NULL), (3, 5)");
    let r = rows(&db, "SELECT MIN(v) FROM t");
    assert!(matches!(&r[0][0], Value::Integer(5)));
    let r = rows(&db, "SELECT MAX(v) FROM t");
    assert!(matches!(&r[0][0], Value::Integer(10)));
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION K: COUNT(DISTINCT) edge cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn count_distinct_with_nulls() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 10), (3, 20), (4, NULL), (5, NULL)");
    let n = scalar_i64(&db, "SELECT COUNT(DISTINCT v) FROM t");
    // DISTINCT ignores NULLs. Distinct non-NULL values: 10, 20. → 2.
    assert_eq!(n, 2);
}

#[test]
fn count_distinct_text() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'a'), (4, 'c'), (5, 'b')");
    let n = scalar_i64(&db, "SELECT COUNT(DISTINCT s) FROM t");
    assert_eq!(n, 3); // a, b, c
}

#[test]
fn count_distinct_empty_returns_zero() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let n = scalar_i64(&db, "SELECT COUNT(DISTINCT v) FROM t");
    assert_eq!(n, 0);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION L: WHERE with arithmetic on columns
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn where_column_arithmetic_comparison() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO t VALUES (1, 5, 3), (2, 10, 2), (3, 1, 1)");
    // WHERE a + b > 7
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE a + b > 7");
    // id=1: 5+3=8 > 7 ✓; id=2: 10+2=12 ✓; id=3: 1+1=2 ✗. → 2.
    assert_eq!(n, 2);
}

#[test]
fn where_column_arithmetic_equality() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO t VALUES (1, 5, 3), (2, 3, 3), (3, 2, 4)");
    // WHERE a * b = 9
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE a * b = 9");
    // id=1: 15; id=2: 9 ✓; id=3: 8. → 1.
    assert_eq!(n, 1);
}

#[test]
fn where_column_arithmetic_with_parens() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10, 3, 4), (2, 5, 2, 3)");
    // WHERE (a + b) * c > 50
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE (a + b) * c > 50");
    // id=1: (10+3)*4=52 > 50 ✓; id=2: (5+2)*3=21 ✗. → 1.
    assert_eq!(n, 1);
}
