//! Bug-hunt v8: operator precedence, comparison across types,
//! integer/float edge values, string comparison, and SELECT projection
//! correctness with computed columns.

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

fn scalar_opt_i64(db: &Database, sql: &str) -> Option<i64> {
    let r = rows(db, sql);
    assert_eq!(r.len(), 1);
    match r[0].first() {
        Some(Value::Integer(n)) => Some(*n),
        Some(Value::Null) => None,
        o => panic!("int/null? {:?}: {}", o, sql),
    }
}

fn scalar_f64(db: &Database, sql: &str) -> f64 {
    let r = rows(db, sql);
    assert_eq!(r.len(), 1);
    match r[0].first() {
        Some(Value::Float(n)) => *n,
        Some(Value::Integer(n)) => *n as f64,
        o => panic!("float? {:?}: {}", o, sql),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. Arithmetic operator precedence
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn precedence_mul_before_add() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
    exec(&db, "INSERT INTO t VALUES (1)");
    // 2 + 3 * 4 = 14 (not 20).
    let r = rows(&db, "SELECT 2 + 3 * 4 FROM t");
    match &r[0][0] {
        Value::Integer(n) => assert_eq!(*n, 14, "2 + 3*4 must be 14"),
        o => panic!("expected int, got {:?}", o),
    }
}

#[test]
fn precedence_parentheses_override() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
    exec(&db, "INSERT INTO t VALUES (1)");
    // (2 + 3) * 4 = 20.
    let r = rows(&db, "SELECT (2 + 3) * 4 FROM t");
    match &r[0][0] {
        Value::Integer(n) => assert_eq!(*n, 20),
        o => panic!("{:?}", o),
    }
}

#[test]
fn precedence_div_and_mod() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
    exec(&db, "INSERT INTO t VALUES (1)");
    // 17 / 5 = 3 (integer division), 17 % 5 = 2.
    let _ = db.execute("SELECT 17 / 5 FROM t");
    let _ = db.execute("SELECT 17 % 5 FROM t");
}

#[test]
fn precedence_unary_minus() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 5)");
    let r = rows(&db, "SELECT -v FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Integer(n) => assert_eq!(*n, -5),
        o => panic!("{:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. Comparison across int/float boundary
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn compare_int_col_float_literal() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "INSERT INTO t VALUES (2, 20)");
    exec(&db, "INSERT INTO t VALUES (3, 30)");
    // WHERE v > 15.5 → rows 2, 3 (int col compared with float literal).
    let r = rows(&db, "SELECT id FROM t WHERE v > 15.5 ORDER BY id");
    assert_eq!(r.len(), 2);
}

#[test]
fn compare_float_col_int_literal() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)");
    for i in 1..=5 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {:.1})", i, i as f64 * 1.5));
    }
    // v = 3.0 (int literal matching float col) → id=2 (3.0).
    let r = rows(&db, "SELECT id FROM t WHERE v = 3");
    assert_eq!(r.len(), 1);
}

#[test]
fn compare_inequality_float() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)");
    exec(&db, "INSERT INTO t VALUES (1, 1.5)");
    exec(&db, "INSERT INTO t VALUES (2, 2.5)");
    exec(&db, "INSERT INTO t VALUES (3, 3.5)");
    let r = rows(&db, "SELECT id FROM t WHERE v >= 2.5 ORDER BY id");
    assert_eq!(r.len(), 2);
    let r = rows(&db, "SELECT id FROM t WHERE v < 2.5 ORDER BY id");
    assert_eq!(r.len(), 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. SUM with mixed int/float values
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn sum_mixed_int_float_column() {
    // Insert ints into a FLOAT column — SUM should treat them as floats.
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "INSERT INTO t VALUES (2, 20.5)");
    exec(&db, "INSERT INTO t VALUES (3, 30)");
    // SUM = 10 + 20.5 + 30 = 60.5 (must be Float).
    let s = scalar_f64(&db, "SELECT SUM(v) FROM t");
    assert!((s - 60.5).abs() < 0.001, "SUM mixed = 60.5, got {}", s);
}

#[test]
fn avg_all_integers_is_float() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "INSERT INTO t VALUES (2, 20)");
    exec(&db, "INSERT INTO t VALUES (3, 25)");
    // AVG(10, 20, 25) = 55/3 ≈ 18.333 (Float, not integer division).
    let a = scalar_f64(&db, "SELECT AVG(v) FROM t");
    assert!((a - 18.333).abs() < 0.01, "AVG must be float 18.33, got {}", a);
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. NULL in expressions and aggregates (thorough)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn sum_ignores_nulls() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "INSERT INTO t VALUES (2, NULL)");
    exec(&db, "INSERT INTO t VALUES (3, 20)");
    exec(&db, "INSERT INTO t VALUES (4, NULL)");
    exec(&db, "INSERT INTO t VALUES (5, 30)");
    // SUM(v) = 10 + 20 + 30 = 60 (NULLs skipped, not 0).
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), 60);
}

#[test]
fn avg_ignores_nulls_in_count() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "INSERT INTO t VALUES (2, NULL)");
    exec(&db, "INSERT INTO t VALUES (3, 30)");
    // AVG(v) = (10 + 30) / 2 = 20 (count=2, not 3).
    let a = scalar_f64(&db, "SELECT AVG(v) FROM t");
    assert!((a - 20.0).abs() < 0.001, "AVG ignoring NULL = 20.0, got {}", a);
}

#[test]
fn all_null_sum_returns_null() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, NULL)");
    exec(&db, "INSERT INTO t VALUES (2, NULL)");
    // SUM of all-NULL column = NULL (not 0).
    let r = rows(&db, "SELECT SUM(v) FROM t");
    assert_eq!(r.len(), 1);
    assert!(matches!(r[0][0], Value::Null), "SUM(all NULL) must be NULL");
}

#[test]
fn count_col_vs_count_star_with_nulls() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "INSERT INTO t VALUES (2, NULL)");
    exec(&db, "INSERT INTO t VALUES (3, 30)");
    // COUNT(*) = 3 (all rows), COUNT(v) = 2 (non-NULL only).
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 3);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(v) FROM t"), 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. String comparison semantics
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn string_equality_exact() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'Hello')");
    exec(&db, "INSERT INTO t VALUES (2, 'hello')");
    exec(&db, "INSERT INTO t VALUES (3, 'Hello')");
    // Case-sensitive: 'Hello' matches 2 rows, 'hello' matches 1.
    assert_eq!(rows(&db, "SELECT id FROM t WHERE s = 'Hello'").len(), 2);
    assert_eq!(rows(&db, "SELECT id FROM t WHERE s = 'hello'").len(), 1);
}

#[test]
fn string_ordering_lexicographic() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    for (i, s) in [(1, "a"), (2, "ab"), (3, "abc"), (4, "b"), (5, "ba")].iter() {
        exec(&db, &format!("INSERT INTO t VALUES ({}, '{}')", i, s));
    }
    // WHERE s < 'b' → a, ab, abc.
    let r = rows(&db, "SELECT id FROM t WHERE s < 'b' ORDER BY id");
    assert_eq!(r.len(), 3);
    // WHERE s >= 'b' → b, ba.
    let r = rows(&db, "SELECT id FROM t WHERE s >= 'b' ORDER BY id");
    assert_eq!(r.len(), 2);
}

#[test]
fn string_comparison_with_numbers() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, '100')");
    exec(&db, "INSERT INTO t VALUES (2, '20')");
    exec(&db, "INSERT INTO t VALUES (3, '9')");
    // String comparison: '20' < '9' (lexicographic: '2' < '9').
    // WHERE s < '9' → '100', '20' (both start with chars < '9').
    let r = rows(&db, "SELECT id FROM t WHERE s < '9' ORDER BY id");
    assert_eq!(r.len(), 2, "lexicographic: '100' and '20' < '9'");
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. UPDATE changing PK (should be rejected or handled safely)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn update_primary_key_value() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    // UPDATE the PK column itself.
    let result = db.execute("UPDATE t SET id = 999 WHERE id = 1");
    // Either it works (row moves to id=999) or errors. Either way, no crash.
    if result.is_ok() {
        // If it worked, old id=1 should be gone (0 rows), new id=999 present.
        let r1 = rows(&db, "SELECT v FROM t WHERE id = 1");
        assert_eq!(r1.len(), 0, "old id=1 should be gone after PK update");
        let r2 = rows(&db, "SELECT v FROM t WHERE id = 999");
        assert_eq!(r2.len(), 1, "row moved to id=999");
        match &r2[0][0] {
            Value::Integer(n) => assert_eq!(*n, 10, "value preserved"),
            o => panic!("expected 10, got {:?}", o),
        }
    }
    // Table must be consistent (exactly 1 row).
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 1);
}

#[test]
fn update_pk_to_existing_pk_errors() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "INSERT INTO t VALUES (2, 20)");
    // UPDATE id=1 to id=2 (which already exists) — must error (PK conflict).
    let result = db.execute("UPDATE t SET id = 2 WHERE id = 1");
    assert!(result.is_err(), "updating PK to existing value must error");
    // Original rows intact.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. GROUP BY with aggregate in HAVING using different column
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn group_by_having_count() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT)");
    // cat 'a': 3 rows, 'b': 2 rows, 'c': 1 row.
    exec(&db, "INSERT INTO t VALUES (1, 'a')");
    exec(&db, "INSERT INTO t VALUES (2, 'a')");
    exec(&db, "INSERT INTO t VALUES (3, 'a')");
    exec(&db, "INSERT INTO t VALUES (4, 'b')");
    exec(&db, "INSERT INTO t VALUES (5, 'b')");
    exec(&db, "INSERT INTO t VALUES (6, 'c')");
    // HAVING COUNT(*) >= 2 → 'a' (3), 'b' (2). 'c' (1) excluded.
    let r = rows(&db, "SELECT cat, COUNT(*) FROM t GROUP BY cat HAVING COUNT(*) >= 2 ORDER BY cat");
    assert_eq!(r.len(), 2);
    match &r[0][0] { Value::Text(s) => assert_eq!(&*s.0, "a"), _ => panic!() }
    match &r[1][0] { Value::Text(s) => assert_eq!(&*s.0, "b"), _ => panic!() }
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. Multiple aggregates in one query
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn five_aggregates_one_query() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=100 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    let r = rows(&db, "SELECT COUNT(*), SUM(v), AVG(v), MIN(v), MAX(v) FROM t");
    assert_eq!(r.len(), 1);
    match (&r[0][0], &r[0][1], &r[0][3], &r[0][4]) {
        (Value::Integer(c), Value::Integer(s), Value::Integer(mn), Value::Integer(mx)) => {
            assert_eq!(*c, 100);
            assert_eq!(*s, 5050);
            assert_eq!(*mn, 1);
            assert_eq!(*mx, 100);
        }
        o => panic!("wrong types: {:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 9. DELETE then INSERT preserving AUTO_INCREMENT counter
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn auto_increment_after_delete() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY AUTO_INCREMENT, v INT)");
    exec(&db, "INSERT INTO t (v) VALUES (10)");  // id=1
    exec(&db, "INSERT INTO t (v) VALUES (20)");  // id=2
    exec(&db, "INSERT INTO t (v) VALUES (30)");  // id=3
    exec(&db, "DELETE FROM t WHERE id = 3");
    exec(&db, "INSERT INTO t (v) VALUES (40)");  // id should be 4, not reuse 3.
    let r = rows(&db, "SELECT id, v FROM t ORDER BY id");
    let last = r.last().unwrap();
    match (&last[0], &last[1]) {
        (Value::Integer(id), Value::Integer(v)) => {
            assert_eq!(*id, 4, "AUTO_INCREMENT must not reuse deleted id=3");
            assert_eq!(*v, 40);
        }
        o => panic!("{:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 10. Boolean expression evaluation
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn where_complex_boolean() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10, 20, 30)");
    exec(&db, "INSERT INTO t VALUES (2, 10, 20, 40)");
    exec(&db, "INSERT INTO t VALUES (3, 10, 30, 30)");
    exec(&db, "INSERT INTO t VALUES (4, 20, 20, 30)");
    // (a = 10 AND b = 20) OR c = 40 → rows 1, 2 (a=10,b=20), row 2 again (c=40).
    // Unique: 1, 2.
    let r = rows(&db, "SELECT id FROM t WHERE (a = 10 AND b = 20) OR c = 40 ORDER BY id");
    assert_eq!(r.len(), 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// 11. Large batch then selective query
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn large_batch_selective_query_correct() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, mod5 INT, v INT)");
    for i in 1..=500 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {}, {})", i, i % 5, i * 2));
    }
    // Query WHERE mod5 = 0 → ids 5, 10, ..., 500 = 100 rows.
    let r = rows(&db, "SELECT COUNT(*) FROM t WHERE mod5 = 0");
    match r[0][0] { Value::Integer(n) => assert_eq!(n, 100), _ => panic!() }
    // SUM of v for mod5=0: 2*(5+10+...+500) = 2 * (5+10+...+500).
    // 5+10+...+500 = 5*(1+2+...+100) = 5*5050 = 25250. ×2 = 50500.
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t WHERE mod5 = 0"), 50500);
}

// ═══════════════════════════════════════════════════════════════════════════
// 12. Negative numbers in WHERE and arithmetic
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn negative_in_arithmetic() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, -10)");
    exec(&db, "INSERT INTO t VALUES (2, 20)");
    let r = rows(&db, "SELECT v + (-5) FROM t WHERE id = 1");
    match &r[0][0] { Value::Integer(n) => assert_eq!(*n, -15), o => panic!("{:?}", o) }
    // WHERE v < 0 → row 1.
    assert_eq!(rows(&db, "SELECT id FROM t WHERE v < 0").len(), 1);
}

#[test]
fn negative_aggregate() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, -10)");
    exec(&db, "INSERT INTO t VALUES (2, -20)");
    exec(&db, "INSERT INTO t VALUES (3, 30)");
    // SUM(-10, -20, 30) = 0.
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), 0);
    // MIN = -20, MAX = 30.
    assert_eq!(scalar_i64(&db, "SELECT MIN(v) FROM t"), -20);
    assert_eq!(scalar_i64(&db, "SELECT MAX(v) FROM t"), 30);
}

// ═══════════════════════════════════════════════════════════════════════════
// 13. Empty result set then aggregate
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn aggregate_after_filtering_all_out() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "INSERT INTO t VALUES (2, 20)");
    // WHERE filters out all rows.
    let r = rows(&db, "SELECT SUM(v) FROM t WHERE v > 1000");
    assert_eq!(r.len(), 1, "aggregate over empty set returns 1 row");
    assert!(matches!(r[0][0], Value::Null), "SUM over empty set = NULL");
    let r = rows(&db, "SELECT COUNT(*) FROM t WHERE v > 1000");
    assert_eq!(r.len(), 1);
    match &r[0][0] { Value::Integer(0) => {}, o => panic!("COUNT empty = 0, got {:?}", o) }
}

// ═══════════════════════════════════════════════════════════════════════════
// 14. Repeated DELETE + INSERT cycle (tombstone accumulation)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn delete_reinsert_cycle_correct() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for cycle in 0..20 {
        exec(&db, &format!("INSERT INTO t VALUES (1, {})", cycle));
        exec(&db, "DELETE FROM t WHERE id = 1");
    }
    // After 20 cycles of insert+delete, table should be empty.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 0);
    // Final insert sticks.
    exec(&db, "INSERT INTO t VALUES (1, 999)");
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1"), 999);
}

// ═══════════════════════════════════════════════════════════════════════════
// 15. GROUP BY with multiple aggregates on different columns
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn group_by_multi_agg_diff_cols() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, a INT, b INT)");
    exec(&db, "INSERT INTO t VALUES (1, 'x', 10, 100)");
    exec(&db, "INSERT INTO t VALUES (2, 'x', 20, 200)");
    exec(&db, "INSERT INTO t VALUES (3, 'y', 30, 300)");
    let r = rows(&db, "SELECT cat, SUM(a), SUM(b), COUNT(*) FROM t GROUP BY cat ORDER BY cat");
    assert_eq!(r.len(), 2);
    // x: SUM(a)=30, SUM(b)=300, COUNT=2.
    match (&r[0][0], &r[0][1], &r[0][2], &r[0][3]) {
        (Value::Text(c), Value::Integer(sa), Value::Integer(sb), Value::Integer(cnt)) => {
            assert_eq!(&*c.0, "x");
            assert_eq!(*sa, 30);
            assert_eq!(*sb, 300);
            assert_eq!(*cnt, 2);
        }
        o => panic!("{:?}", o),
    }
}
