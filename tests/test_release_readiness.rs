//! Release Readiness — Edge Case Tests
//!
//! Production-grade edge case coverage: NULL semantics, empty-table behavior,
//! type coercion, large/boundary values, special-character handling,
//! interleaved CRUD cycles, CASE/UNION/STDDEV correctness, and restart
//! recovery. Every test asserts exact values — not just "no error".
//!
//! Run: cargo test --release --test test_release_readiness

use motedb::types::Value;
use motedb::{DBConfig, Database, QueryResult};
use tempfile::TempDir;

// ─────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────

fn edge_db() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    (dir, db)
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

fn float_val(db: &Database, sql: &str) -> f64 {
    match val(db, sql) {
        Value::Float(f) => f,
        v => panic!("Expected Float, got {:?}", v),
    }
}

fn text_val(db: &Database, sql: &str) -> String {
    match val(db, sql) {
        Value::Text(s) => s.to_string(),
        v => panic!("Expected Text, got {:?}", v),
    }
}

// ═══════════════════════════════════════════════════════════════════
// 1. NULL handling
// ═══════════════════════════════════════════════════════════════════

#[test]
fn null_insert_and_select_roundtrip() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 42, 'hi')").unwrap();

    let r1 = row(&db, "SELECT a, b FROM t WHERE id = 1");
    assert_eq!(r1[0], Value::Null);
    assert_eq!(r1[1], Value::Null);

    let r2 = row(&db, "SELECT a, b FROM t WHERE id = 2");
    assert_eq!(r2[0], Value::Integer(42));
    assert_eq!(r2[1], Value::Text("hi".into()));
}

#[test]
fn null_in_where_is_null_and_is_not_null() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30)").unwrap();
    db.execute("INSERT INTO t VALUES (4, NULL)").unwrap();

    let nulls = rows(&db, "SELECT id FROM t WHERE v IS NULL ORDER BY id");
    assert_eq!(nulls.len(), 2);
    assert_eq!(nulls[0][0], Value::Integer(2));
    assert_eq!(nulls[1][0], Value::Integer(4));

    let not_nulls = rows(&db, "SELECT id FROM t WHERE v IS NOT NULL ORDER BY id");
    assert_eq!(not_nulls.len(), 2);
    assert_eq!(not_nulls[0][0], Value::Integer(1));
    assert_eq!(not_nulls[1][0], Value::Integer(3));
}

#[test]
fn count_ignores_null_count_star_does_not() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30)").unwrap();
    db.execute("INSERT INTO t VALUES (4, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (5, 50)").unwrap();

    assert_eq!(int_val(&db, "SELECT COUNT(*) FROM t"), 5);
    assert_eq!(int_val(&db, "SELECT COUNT(v) FROM t"), 3); // NULLs skipped
}

#[test]
fn sum_and_avg_ignore_null() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 20)").unwrap();
    db.execute("INSERT INTO t VALUES (4, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (5, 30)").unwrap();

    // SUM: 10 + 20 + 30 = 60
    assert_eq!(int_val(&db, "SELECT SUM(v) FROM t"), 60);
    // AVG: 60 / 3 = 20.0
    assert!((float_val(&db, "SELECT AVG(v) FROM t") - 20.0).abs() < 0.001);
}

#[test]
fn null_comparisons_are_unknown() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();

    // NULL = NULL is UNKNOWN → not returned
    assert!(rows(&db, "SELECT id FROM t WHERE v = NULL").is_empty());
    // NULL <> 1 is UNKNOWN
    assert!(rows(&db, "SELECT id FROM t WHERE v <> 1").is_empty());
    // NULL > 0 is UNKNOWN
    assert!(rows(&db, "SELECT id FROM t WHERE v > 0").is_empty());
    // NULL < 0 is UNKNOWN
    assert!(rows(&db, "SELECT id FROM t WHERE v < 0").is_empty());
}

#[test]
fn null_arithmetic_propagates_null() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 10, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (3, NULL, 20)").unwrap();

    assert_eq!(row(&db, "SELECT a + b FROM t WHERE id = 1")[0], Value::Null);
    assert_eq!(row(&db, "SELECT a + b FROM t WHERE id = 2")[0], Value::Null);
    assert_eq!(row(&db, "SELECT a + b FROM t WHERE id = 3")[0], Value::Null);
}

// ═══════════════════════════════════════════════════════════════════
// 2. Empty table operations
// ═══════════════════════════════════════════════════════════════════

#[test]
fn select_from_empty_table_returns_no_rows() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();

    let rs = rows(&db, "SELECT * FROM t");
    assert!(rs.is_empty(), "SELECT on empty table must return no rows");
}

#[test]
fn count_star_on_empty_table_is_zero() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    assert_eq!(int_val(&db, "SELECT COUNT(*) FROM t"), 0);
}

#[test]
fn aggregates_on_empty_table_return_null() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();

    assert_eq!(val(&db, "SELECT SUM(v) FROM t"), Value::Null);
    assert_eq!(val(&db, "SELECT AVG(v) FROM t"), Value::Null);
    assert_eq!(val(&db, "SELECT MIN(v) FROM t"), Value::Null);
    assert_eq!(val(&db, "SELECT MAX(v) FROM t"), Value::Null);
}

#[test]
fn group_by_on_empty_table_returns_no_groups() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)")
        .unwrap();

    let rs = rows(&db, "SELECT cat, COUNT(*) FROM t GROUP BY cat");
    assert!(rs.is_empty(), "GROUP BY on empty table must yield no rows");
}

// ═══════════════════════════════════════════════════════════════════
// 3. Type coercion edge cases
// ═══════════════════════════════════════════════════════════════════

#[test]
fn integer_overflow_promotes_to_float() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute(&format!("INSERT INTO t VALUES (1, {})", i64::MAX))
        .unwrap();

    // i64::MAX + 1 overflows i64 → result must promote to Float.
    let r = row(&db, "SELECT v + 1 FROM t WHERE id = 1");
    match &r[0] {
        Value::Float(f) => {
            let expected = i64::MAX as f64 + 1.0;
            assert!(
                (f - expected).abs() < 1.0,
                "expected ~{}, got {}",
                expected,
                f
            );
        }
        v => panic!("Expected Float after overflow, got {:?}", v),
    }
}

#[test]
fn float_to_integer_comparison_in_where() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 9.5)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 10.5)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 11.5)").unwrap();

    // Compare FLOAT column against INTEGER literal — mixed-type comparison.
    let rs = rows(&db, "SELECT id FROM t WHERE v > 10 ORDER BY id");
    assert_eq!(rs.len(), 2);
    assert_eq!(rs[0][0], Value::Integer(2));
    assert_eq!(rs[1][0], Value::Integer(3));

    let rs = rows(&db, "SELECT id FROM t WHERE v <= 10 ORDER BY id");
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0][0], Value::Integer(1));
}

#[test]
fn text_column_ordered_lexicographically() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, '123')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'abc')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'xyz')").unwrap();

    // TEXT columns compare lexicographically by character code point.
    // '123' ('1'=0x31) and 'abc' ('a'=0x61) are both < 'b' (0x62);
    // 'xyz' ('x'=0x78) is > 'b'.
    let rs = rows(&db, "SELECT s FROM t WHERE s < 'b' ORDER BY s");
    assert_eq!(rs.len(), 2);
    assert_eq!(rs[0][0], Value::Text("123".into()));
    assert_eq!(rs[1][0], Value::Text("abc".into()));

    // Equality match on a text literal is exact.
    let rs = rows(&db, "SELECT id FROM t WHERE s = 'abc'");
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0][0], Value::Integer(2));
}

// ═══════════════════════════════════════════════════════════════════
// 4. Large values
// ═══════════════════════════════════════════════════════════════════

#[test]
fn very_long_text_string_10kb_roundtrip() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, s TEXT)")
        .unwrap();

    // 10 KB payload: 10240 chars
    let big = "A".repeat(10_240);
    db.execute(&format!(
        "INSERT INTO t VALUES (1, '{}')",
        big.escape_default()
    ))
    .unwrap();

    let got = text_val(&db, "SELECT s FROM t WHERE id = 1");
    assert_eq!(got.len(), 10_240, "10KB text must round-trip intact");
    assert_eq!(got, big);
}

#[test]
fn large_integers_i64_max_and_min() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute(&format!("INSERT INTO t VALUES (1, {})", i64::MAX))
        .unwrap();
    db.execute(&format!("INSERT INTO t VALUES (2, {})", i64::MIN))
        .unwrap();

    assert_eq!(int_val(&db, "SELECT v FROM t WHERE id = 1"), i64::MAX);
    assert_eq!(int_val(&db, "SELECT v FROM t WHERE id = 2"), i64::MIN);

    // Boundary ordering: i64::MIN < i64::MAX
    let rs = rows(&db, "SELECT v FROM t ORDER BY v ASC");
    assert_eq!(rs[0][0], Value::Integer(i64::MIN));
    assert_eq!(rs[1][0], Value::Integer(i64::MAX));
}

#[test]
fn negative_primary_key_values() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, label TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (-5, 'neg_five')").unwrap();
    db.execute("INSERT INTO t VALUES (-1, 'neg_one')").unwrap();
    db.execute("INSERT INTO t VALUES (0, 'zero')").unwrap();

    assert_eq!(
        text_val(&db, "SELECT label FROM t WHERE id = -5"),
        "neg_five"
    );
    assert_eq!(
        text_val(&db, "SELECT label FROM t WHERE id = -1"),
        "neg_one"
    );

    // Lookup by negative PK works
    let rs = rows(&db, "SELECT id FROM t WHERE id < 0 ORDER BY id");
    assert_eq!(rs.len(), 2);
    assert_eq!(rs[0][0], Value::Integer(-5));
    assert_eq!(rs[1][0], Value::Integer(-1));
}

// ═══════════════════════════════════════════════════════════════════
// 5. SQL injection / special characters (parameterized via literals only)
// ═══════════════════════════════════════════════════════════════════

#[test]
fn text_with_single_quotes_obrien() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    // O'Brien — escape the single quote by doubling it in the SQL literal.
    db.execute("INSERT INTO t VALUES (1, 'O''Brien')").unwrap();

    assert_eq!(text_val(&db, "SELECT name FROM t WHERE id = 1"), "O'Brien");
}

#[test]
fn text_with_semicolons() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, body TEXT)")
        .unwrap();
    // A semicolon inside a string literal must not be treated as statement end.
    db.execute("INSERT INTO t VALUES (1, 'a; b; c; DROP TABLE t;')")
        .unwrap();

    assert_eq!(
        text_val(&db, "SELECT body FROM t WHERE id = 1"),
        "a; b; c; DROP TABLE t;"
    );
    // The DROP inside the string must NOT have executed
    assert_eq!(int_val(&db, "SELECT COUNT(*) FROM t"), 1);
}

#[test]
fn text_with_backslashes() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, path TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'C:\\\\Program Files\\\\app')")
        .unwrap();

    let got = text_val(&db, "SELECT path FROM t WHERE id = 1");
    assert_eq!(got, "C:\\Program Files\\app");
}

#[test]
fn empty_string_is_distinct_from_null() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, '')").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();

    assert_eq!(
        row(&db, "SELECT s FROM t WHERE id = 1")[0],
        Value::Text("".into()),
        "empty string must round-trip as Text, not Null"
    );
    assert_eq!(
        row(&db, "SELECT s FROM t WHERE id = 2")[0],
        Value::Null,
        "NULL must round-trip as Null"
    );

    // IS NULL matches only row 2, not the empty string.
    let nulls = rows(&db, "SELECT id FROM t WHERE s IS NULL");
    assert_eq!(nulls.len(), 1);
    assert_eq!(nulls[0][0], Value::Integer(2));

    // Empty string is selectable by equality.
    let empties = rows(&db, "SELECT id FROM t WHERE s = ''");
    assert_eq!(empties.len(), 1);
    assert_eq!(empties[0][0], Value::Integer(1));
}

// ═══════════════════════════════════════════════════════════════════
// 6. Single-threaded interleaved operations
// ═══════════════════════════════════════════════════════════════════

#[test]
fn insert_update_delete_select_cycle() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();

    // INSERT
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    assert_eq!(int_val(&db, "SELECT v FROM t WHERE id = 1"), 10);

    // UPDATE
    db.execute("UPDATE t SET v = 20 WHERE id = 1").unwrap();
    assert_eq!(int_val(&db, "SELECT v FROM t WHERE id = 1"), 20);

    // DELETE
    db.execute("DELETE FROM t WHERE id = 1").unwrap();
    assert_eq!(int_val(&db, "SELECT COUNT(*) FROM t"), 0);

    // Re-INSERT (PK recycled)
    db.execute("INSERT INTO t VALUES (1, 30)").unwrap();
    assert_eq!(int_val(&db, "SELECT v FROM t WHERE id = 1"), 30);
}

#[test]
fn multiple_updates_on_same_row() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 0)").unwrap();

    for i in 1..=10 {
        db.execute(&format!("UPDATE t SET v = {} WHERE id = 1", i))
            .unwrap();
    }
    assert_eq!(int_val(&db, "SELECT v FROM t WHERE id = 1"), 10);

    // Only one live row for id=1 after all updates (no duplicate versions leak)
    assert_eq!(int_val(&db, "SELECT COUNT(*) FROM t WHERE id = 1"), 1);
    assert_eq!(int_val(&db, "SELECT COUNT(*) FROM t"), 1);
}

#[test]
fn delete_then_reinsert_same_pk() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (7, 100)").unwrap();

    db.execute("DELETE FROM t WHERE id = 7").unwrap();
    assert_eq!(int_val(&db, "SELECT COUNT(*) FROM t"), 0);

    // Same PK must be reusable after delete.
    db.execute("INSERT INTO t VALUES (7, 200)").unwrap();
    assert_eq!(int_val(&db, "SELECT v FROM t WHERE id = 7"), 200);
    assert_eq!(int_val(&db, "SELECT COUNT(*) FROM t"), 1);

    // A second insert with the same PK must still be rejected.
    assert!(
        db.execute("INSERT INTO t VALUES (7, 300)").is_err(),
        "duplicate PK after reinsert must be rejected"
    );
}

// ═══════════════════════════════════════════════════════════════════
// 7. CASE WHEN — conditional expressions
// ═══════════════════════════════════════════════════════════════════

#[test]
fn case_when_multiple_branches() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 5)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 15)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 25)").unwrap();

    let r = rows(&db,
        "SELECT v, CASE WHEN v < 10 THEN 'small' WHEN v < 20 THEN 'mid' ELSE 'big' END FROM t ORDER BY v");
    assert_eq!(r.len(), 3);
    assert_eq!(r[0][1], Value::text("small".to_string()));
    assert_eq!(r[1][1], Value::text("mid".to_string()));
    assert_eq!(r[2][1], Value::text("big".to_string()));
}

#[test]
fn case_when_without_else_returns_null() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 5)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 50)").unwrap();

    let r = rows(
        &db,
        "SELECT v, CASE WHEN v < 10 THEN 'low' END FROM t ORDER BY v",
    );
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][1], Value::text("low".to_string())); // v=5 matches
    assert_eq!(r[1][1], Value::Null); // v=50 doesn't match, no ELSE
}

#[test]
fn nested_case_when() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, -5)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 5)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 50)").unwrap();

    let r = rows(
        &db,
        "SELECT v, CASE \
         WHEN v < 0 THEN 'negative' \
         WHEN v < 10 THEN CASE WHEN v = 5 THEN 'five' ELSE 'other' END \
         ELSE 'large' END FROM t ORDER BY v",
    );
    assert_eq!(r.len(), 3);
    assert_eq!(r[0][1], Value::text("negative".to_string())); // v=-5
    assert_eq!(r[1][1], Value::text("five".to_string())); // v=5
    assert_eq!(r[2][1], Value::text("large".to_string())); // v=50
}

// ═══════════════════════════════════════════════════════════════════
// 8. UNION / UNION ALL
// ═══════════════════════════════════════════════════════════════════

#[test]
fn union_all_preserves_duplicates() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 20)").unwrap();

    let rs = rows(
        &db,
        "SELECT v FROM t WHERE id = 1 UNION ALL SELECT v FROM t WHERE v >= 10",
    );
    // Left: {10}. Right: {10,10,20}. UNION ALL keeps every row → 4 rows.
    assert_eq!(rs.len(), 4);
}

#[test]
fn union_deduplicates_rows() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 20)").unwrap();

    let rs = rows(
        &db,
        "SELECT v FROM t WHERE id = 1 UNION SELECT v FROM t WHERE v >= 10",
    );
    // Left: {10}. Right: {10,10,20}. UNION dedups → {10, 20} = 2 distinct.
    assert_eq!(rs.len(), 2);

    let mut values: Vec<i64> = rs
        .iter()
        .map(|r| match r[0] {
            Value::Integer(i) => i,
            _ => panic!("expected Integer"),
        })
        .collect();
    values.sort();
    assert_eq!(values, vec![10, 20]);
}

#[test]
fn union_combines_disjoint_sets() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 1)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 2)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 3)").unwrap();
    db.execute("INSERT INTO t VALUES (4, 4)").unwrap();

    let rs = rows(
        &db,
        "SELECT v FROM t WHERE v <= 2 UNION SELECT v FROM t WHERE v >= 3 ORDER BY v",
    );
    assert_eq!(rs.len(), 4);
    assert_eq!(rs[0][0], Value::Integer(1));
    assert_eq!(rs[3][0], Value::Integer(4));
}

// ═══════════════════════════════════════════════════════════════════
// 9. STDDEV / VARIANCE
//
// NOTE: As of this release, STDDEV/VARIANCE are NOT recognized as aggregates
// by the executor's projection planner. They evaluate per-row (returning
// NULL because each "group" has one row) instead of producing a single
// aggregate row. The single-row result of an aggregate query over N rows
// therefore has N rows of NULL rather than one statistical value.
//
// Reference values when fixed (sample formula, n-1 divisor):
//   STDDEV(2,4,6)   = 2.0   (variance = 4.0)
//   STDDEV(10,20,30)= 10.0  (variance = 100.0)
//   n < 2           → NULL
//   empty set       → NULL  (this case happens to "work" because the
//                            zero-row input yields a single NULL aggregate).
// ═══════════════════════════════════════════════════════════════════

#[test]
fn stddev_on_empty_set() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();

    // STDDEV/VARIANCE over an empty set must yield a single row containing
    // NULL (matching SUM/AVG, which return [[Null]]). Now that STDDEV/
    // VARIANCE are registered as aggregates, this works correctly.
    let sd_rows = rows(&db, "SELECT STDDEV(v) FROM t");
    assert_eq!(sd_rows.len(), 1, "STDDEV on empty set yields 1 NULL row");
    assert_eq!(sd_rows[0][0], Value::Null);

    let var_rows = rows(&db, "SELECT VARIANCE(v) FROM t");
    assert_eq!(var_rows.len(), 1);
    assert_eq!(var_rows[0][0], Value::Null);

    // Sanity check: SUM is a proper aggregate and does return one NULL row.
    assert_eq!(val(&db, "SELECT SUM(v) FROM t"), Value::Null);
}

#[test]
fn stddev_on_single_value() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 42)").unwrap();

    // Sample stddev with n<2 is undefined → NULL, returned as a single
    // aggregate row (count < 2 guard in compute_aggregate_positional).
    let rs = rows(&db, "SELECT STDDEV(v) FROM t");
    assert_eq!(
        rs.len(),
        1,
        "aggregate without GROUP BY must return exactly one row"
    );
    assert_eq!(rs[0][0], Value::Null);

    let rs = rows(&db, "SELECT VARIANCE(v) FROM t");
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0][0], Value::Null);
}

#[test]
fn stddev_over_three_values() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    // Values 2, 4, 6: mean=4, deviations -2,0,2 → sample var = 8/(3-1) = 4
    // → STDDEV should be 2.0, VARIANCE should be 4.0.
    db.execute("INSERT INTO t VALUES (1, 2)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 4)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 6)").unwrap();

    // STDDEV/VARIANCE now collapse to a single aggregate row with the correct
    // sample-statistic values (n-1 denominator).
    let rs = rows(&db, "SELECT STDDEV(v) FROM t");
    assert_eq!(rs.len(), 1, "STDDEV collapses to 1 aggregate row");
    match &rs[0][0] {
        Value::Float(f) => assert!((f - 2.0).abs() < 1e-9, "STDDEV(2,4,6) = 2.0, got {}", f),
        Value::Null => panic!("STDDEV returned NULL"),
        other => panic!("expected Float, got {:?}", other),
    }

    let rs = rows(&db, "SELECT VARIANCE(v) FROM t");
    assert_eq!(rs.len(), 1);
    match &rs[0][0] {
        Value::Float(f) => assert!((f - 4.0).abs() < 1e-9, "VARIANCE(2,4,6) = 4.0, got {}", f),
        Value::Null => panic!("VARIANCE returned NULL"),
        other => panic!("expected Float, got {:?}", other),
    }
}

#[test]
fn stddev_with_group_by() {
    let (_dir, db) = edge_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, cat TEXT)")
        .unwrap();
    // Group A: 10, 20, 30 → sample STDDEV = 10.0.
    db.execute("INSERT INTO t VALUES (1, 10, 'A')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20, 'A')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30, 'A')").unwrap();

    // GROUP BY cat yields 1 group with the correct STDDEV.
    let rs = rows(&db, "SELECT cat, STDDEV(v) FROM t GROUP BY cat");
    assert_eq!(rs.len(), 1, "GROUP BY cat yields 1 group");
    match &rs[0][1] {
        Value::Float(f) => assert!((f - 10.0).abs() < 1e-9, "STDDEV(10,20,30) = 10.0, got {}", f),
        Value::Null => panic!("group STDDEV returned NULL"),
        other => panic!("expected Float, got {:?}", other),
    }
}

// ═══════════════════════════════════════════════════════════════════
// 10. Restart recovery
// ═══════════════════════════════════════════════════════════════════

#[test]
fn restart_recovery_preserves_all_inserted_data() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;

    // Phase 1: create, insert many rows, drop DB handle.
    {
        let db = Database::create_with_config(&path, config.clone()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score FLOAT, region TEXT)")
            .unwrap();
        for i in 0..100 {
            db.execute(&format!(
                "INSERT INTO t VALUES ({}, 'user_{}', {}, '{}')",
                i,
                i,
                i as f64 * 1.5,
                if i % 2 == 0 { "US" } else { "EU" }
            ))
            .unwrap();
        }
        db.flush().unwrap();
        db.close().unwrap();
    }

    // Phase 2: reopen and verify every row is present with exact values.
    let db = Database::open_with_config(&path, config).unwrap();
    assert_eq!(int_val(&db, "SELECT COUNT(*) FROM t"), 100);

    let rs = rows(&db, "SELECT id, name, score, region FROM t ORDER BY id");
    assert_eq!(rs.len(), 100);
    for (i, r) in rs.iter().enumerate() {
        assert_eq!(r[0], Value::Integer(i as i64), "id mismatch at {}", i);
        assert_eq!(r[1], Value::Text(format!("user_{}", i).into()));
        match &r[2] {
            Value::Float(f) => assert!(
                (f - (i as f64 * 1.5)).abs() < 0.001,
                "score mismatch at {}",
                i
            ),
            v => panic!("expected Float score, got {:?}", v),
        }
        assert_eq!(
            r[3],
            Value::Text(if i % 2 == 0 { "US" } else { "EU" }.into())
        );
    }
    db.close().unwrap();
}

#[test]
fn restart_recovery_preserves_updates_and_deletes() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .unwrap();
        for i in 0..30 {
            db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 10))
                .unwrap();
        }
        // Delete the tail and mutate the head before closing.
        db.execute("DELETE FROM t WHERE id >= 25").unwrap();
        db.execute("UPDATE t SET v = -1 WHERE id < 5").unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }

    {
        let db = Database::open(&path).unwrap();
        let rs = rows(&db, "SELECT id, v FROM t ORDER BY id");
        assert_eq!(rs.len(), 25, "5 rows deleted, 25 remain");

        for r in &rs[0..5] {
            assert_eq!(r[1], Value::Integer(-1), "head should be v=-1, got {:?}", r);
        }
        for r in &rs[5..25] {
            let id = match r[0] {
                Value::Integer(i) => i,
                _ => panic!(),
            };
            assert_eq!(r[1], Value::Integer(id * 10));
        }
        db.close().unwrap();
    }
}
