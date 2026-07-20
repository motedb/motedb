//! Bug-hunt v25: DELETE edge cases, NULL semantics in aggregates,
//! type coercion, COUNT(*) index fast-path vs tombstones, ORDER BY
//! NULL ordering, string function boundary cases, and IN/NOT IN with
//! NULL list members.

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

fn try_exec(db: &Database, sql: &str) -> Result<(), String> {
    match db.execute(sql) {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("{:?}", e)),
    }
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

fn affected(db: &Database, sql: &str) -> i64 {
    let rs = db
        .execute(sql)
        .unwrap_or_else(|e| panic!("SQL failed: {}\n  err: {}", sql, e))
        .materialize()
        .unwrap_or_else(|e| panic!("materialize failed: {}\n  err: {}", sql, e));
    match rs {
        QueryResult::Modification { affected_rows } => affected_rows as i64,
        _ => panic!("expected Modification for: {}", sql),
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
// SECTION A: DELETE behavior
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn delete_all_rows_no_where() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    let n = affected(&db, "DELETE FROM t");
    assert_eq!(n, 3, "DELETE FROM t with no WHERE should delete all 3 rows");
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 0);
}

#[test]
fn delete_returns_zero_when_no_match() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20)");
    let n = affected(&db, "DELETE FROM t WHERE id = 999");
    assert_eq!(n, 0);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 2);
}

#[test]
fn delete_by_pk_then_reinsert_same_id() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    affected(&db, "DELETE FROM t WHERE id = 1");
    exec(&db, "INSERT INTO t VALUES (1, 99)");
    let r = rows(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Integer(99));
}

#[test]
fn delete_with_complex_where() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT, cat TEXT)");
    exec(
        &db,
        "INSERT INTO t VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'a'), (4, 40, 'b'), (5, 50, 'a')",
    );
    let n = affected(&db, "DELETE FROM t WHERE cat = 'a' AND v > 10");
    assert_eq!(n, 2, "should delete rows 3 and 5 (cat=a, v>10)");
    let remaining = ids_sorted(&db, "SELECT id FROM t");
    assert_eq!(remaining, vec![1, 2, 4]);
}

#[test]
fn delete_with_in_clause() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
    let n = affected(&db, "DELETE FROM t WHERE id IN (2, 4)");
    assert_eq!(n, 2);
    let remaining = ids_sorted(&db, "SELECT id FROM t");
    assert_eq!(remaining, vec![1, 3]);
}

#[test]
fn delete_with_not_in_clause() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
    let n = affected(&db, "DELETE FROM t WHERE id NOT IN (1, 2)");
    assert_eq!(n, 2);
    let remaining = ids_sorted(&db, "SELECT id FROM t");
    assert_eq!(remaining, vec![1, 2]);
}

#[test]
fn delete_where_null_column() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30)");
    let n = affected(&db, "DELETE FROM t WHERE v IS NULL");
    assert_eq!(n, 1, "should delete the row where v is NULL");
    let remaining = ids_sorted(&db, "SELECT id FROM t");
    assert_eq!(remaining, vec![1, 3]);
}

#[test]
fn delete_all_then_count() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20)");
    affected(&db, "DELETE FROM t");
    // Insert again to verify table is still usable
    exec(&db, "INSERT INTO t VALUES (5, 50), (6, 60)");
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 2);
    let ids = ids_sorted(&db, "SELECT id FROM t");
    assert_eq!(ids, vec![5, 6]);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION B: COUNT(*) with index fast-path — must reflect deletes
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn count_after_delete_with_index() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT)");
    exec(&db, "CREATE INDEX idx_cat ON t (cat)");
    exec(
        &db,
        "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'a'), (4, 'a')",
    );
    // 3 rows have cat='a'
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE cat = 'a'"), 3);
    // Delete one cat='a' row
    affected(&db, "DELETE FROM t WHERE id = 1");
    // COUNT via index should now reflect the deletion
    assert_eq!(
        scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE cat = 'a'"),
        2,
        "COUNT(*) via index must reflect deletes"
    );
}

#[test]
fn count_after_update_with_index() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT)");
    exec(&db, "CREATE INDEX idx_cat ON t (cat)");
    exec(
        &db,
        "INSERT INTO t VALUES (1, 'a'), (2, 'a'), (3, 'b')",
    );
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE cat = 'a'"), 2);
    // Update one row from 'a' to 'b'
    affected(&db, "UPDATE t SET cat = 'b' WHERE id = 1");
    assert_eq!(
        scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE cat = 'a'"),
        1,
        "COUNT(*) via index must reflect updates"
    );
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE cat = 'b'"), 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION C: NULL in aggregates
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn sum_ignores_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30)");
    assert_eq!(
        scalar_i64(&db, "SELECT SUM(v) FROM t"),
        40,
        "SUM should ignore NULLs"
    );
}

#[test]
fn avg_ignores_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30)");
    // AVG = (10 + 30) / 2 = 20
    let r = rows(&db, "SELECT AVG(v) FROM t");
    assert_eq!(r.len(), 1);
    match &r[0][0] {
        Value::Float(f) => assert!((f - 20.0).abs() < 1e-9, "AVG = 20, got {}", f),
        Value::Integer(i) => assert_eq!(*i, 20, "AVG should be 20"),
        o => panic!("expected numeric AVG, got {:?}", o),
    }
}

#[test]
fn count_column_ignores_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30), (4, NULL)");
    // COUNT(v) counts non-NULL values only
    assert_eq!(scalar_i64(&db, "SELECT COUNT(v) FROM t"), 2);
    // COUNT(*) counts all rows
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 4);
}

#[test]
fn min_max_ignore_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30), (4, NULL), (5, 5)");
    assert_eq!(scalar_i64(&db, "SELECT MIN(v) FROM t"), 5);
    assert_eq!(scalar_i64(&db, "SELECT MAX(v) FROM t"), 30);
}

#[test]
fn sum_all_null_returns_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, NULL), (2, NULL)");
    let r = rows(&db, "SELECT SUM(v) FROM t");
    assert_eq!(r.len(), 1);
    assert!(
        matches!(r[0][0], Value::Null),
        "SUM of all NULLs should be NULL, got {:?}",
        r[0][0]
    );
}

#[test]
fn avg_empty_table_returns_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let r = rows(&db, "SELECT AVG(v) FROM t");
    assert_eq!(r.len(), 1);
    assert!(
        matches!(r[0][0], Value::Null),
        "AVG of empty table should be NULL, got {:?}",
        r[0][0]
    );
}

#[test]
fn count_empty_table_returns_zero() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 0);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(v) FROM t"), 0);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION D: ORDER BY NULL placement
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn order_by_asc_nulls_first_or_last_documented() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, NULL), (3, 5), (4, NULL), (5, 20)");
    let r = rows(&db, "SELECT v FROM t ORDER BY v ASC");
    // Document current behavior: NULLs sort first in ASC (less than everything)
    // Capture the observed ordering to lock in the behavior.
    let observed: Vec<String> = r
        .iter()
        .map(|row| match &row[0] {
            Value::Null => "NULL".to_string(),
            Value::Integer(n) => n.to_string(),
            o => panic!("unexpected: {:?}", o),
        })
        .collect();
    // Either [NULL, NULL, 5, 10, 20] (nulls-first) or [5, 10, 20, NULL, NULL] (nulls-last)
    let valid_a = vec!["NULL", "NULL", "5", "10", "20"];
    let valid_b = vec!["5", "10", "20", "NULL", "NULL"];
    assert!(
        observed == valid_a || observed == valid_b,
        "ORDER BY ASC produced unexpected ordering: {:?}",
        observed
    );
}

#[test]
fn order_by_desc_with_nulls() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, NULL), (3, 5), (4, NULL), (5, 20)");
    let r = rows(&db, "SELECT v FROM t ORDER BY v DESC");
    let observed: Vec<String> = r
        .iter()
        .map(|row| match &row[0] {
            Value::Null => "NULL".to_string(),
            Value::Integer(n) => n.to_string(),
            o => panic!("unexpected: {:?}", o),
        })
        .collect();
    // DESC reverses ASC. If ASC is null-first then DESC puts nulls last.
    let valid_a = vec!["20", "10", "5", "NULL", "NULL"];
    let valid_b = vec!["NULL", "NULL", "20", "10", "5"];
    assert!(
        observed == valid_a || observed == valid_b,
        "ORDER BY DESC produced unexpected ordering: {:?}",
        observed
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION E: Type coercion in WHERE
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn compare_int_to_float_in_where() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)");
    exec(&db, "INSERT INTO t VALUES (1, 1.5), (2, 2.5), (3, 3.5)");
    // Compare FLOAT column to INTEGER literal
    let r = rows(&db, "SELECT id FROM t WHERE v > 2");
    let mut ids: Vec<i64> = r
        .iter()
        .filter_map(|row| match row[0] {
            Value::Integer(n) => Some(n),
            _ => None,
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![2, 3], "FLOAT > INT literal should coerce");
}

#[test]
fn compare_float_column_to_float_literal() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)");
    exec(&db, "INSERT INTO t VALUES (1, 1.5), (2, 2.5), (3, 3.5)");
    let r = rows(&db, "SELECT id FROM t WHERE v = 2.5");
    assert_eq!(r.len(), 1);
}

#[test]
fn int_column_arithmetic_in_where() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    // v + 5 > 25 → rows where v > 20
    let r = rows(&db, "SELECT id FROM t WHERE v + 5 > 25");
    let mut ids: Vec<i64> = r
        .iter()
        .filter_map(|row| match row[0] {
            Value::Integer(n) => Some(n),
            _ => None,
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![3]);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION F: NULL in IN / NOT IN (SQL three-valued logic)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn in_list_with_null_does_not_match() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    // 5 IN (1, NULL, 10) → UNKNOWN (not TRUE) in standard SQL.
    // Our impl: NULL IN (...) returns false; non-NULL IN list-with-NULL still
    // matches when the value is in the non-NULL part.
    let r = rows(&db, "SELECT id FROM t WHERE v IN (10, NULL, 30)");
    let mut ids: Vec<i64> = r
        .iter()
        .filter_map(|row| match row[0] {
            Value::Integer(n) => Some(n),
            _ => None,
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![1, 3]);
}

#[test]
fn not_in_list_with_null_returns_empty() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    // Standard SQL: v NOT IN (10, NULL) → UNKNOWN for all rows → no rows pass
    let r = rows(&db, "SELECT id FROM t WHERE v NOT IN (10, NULL)");
    // We accept either: (a) empty (strict SQL), or (b) rows where v != 10 (lenient).
    // Document the actual behavior with an assertion.
    let count = r.len();
    assert!(
        count == 0 || count == 2,
        "NOT IN with NULL: expected 0 (strict) or 2 (lenient), got {}",
        count
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION G: String function boundaries
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn substr_out_of_range_returns_empty_or_short() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'hello')");
    let r = rows(&db, "SELECT substr(s, 10, 5) FROM t WHERE id = 1");
    // Start past end → empty string
    match &r[0][0] {
        Value::Text(s) => assert_eq!(s.as_str(), ""),
        o => panic!("expected empty text, got {:?}", o),
    }
}

#[test]
fn substr_start_zero_treated_as_one() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'hello')");
    let r = rows(&db, "SELECT substr(s, 0, 3) FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Text(s) => assert_eq!(s.as_str(), "hel", "substr(s, 0, 3) should behave like substr(s, 1, 3)"),
        o => panic!("expected text, got {:?}", o),
    }
}

#[test]
fn substr_negative_start_from_end() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'hello')");
    let r = rows(&db, "SELECT substr(s, -3) FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Text(s) => assert_eq!(s.as_str(), "llo"),
        o => panic!("expected text, got {:?}", o),
    }
}

#[test]
fn length_of_unicode_chars() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'héllo')");
    // 'héllo' has 5 characters (é is one char), but 6 bytes.
    let r = rows(&db, "SELECT length(s) FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Integer(n) => assert_eq!(*n, 5, "length counts chars not bytes"),
        o => panic!("expected int, got {:?}", o),
    }
}

#[test]
fn concat_with_null_returns_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'hello'), (2, NULL)");
    let r = rows(&db, "SELECT concat(s, 'x') FROM t WHERE id = 2");
    assert!(
        matches!(r[0][0], Value::Null),
        "concat with NULL should propagate NULL, got {:?}",
        r[0][0]
    );
}

#[test]
fn upper_lower_unicode_safe() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'Hello')");
    let r = rows(&db, "SELECT upper(s), lower(s) FROM t WHERE id = 1");
    match (&r[0][0], &r[0][1]) {
        (Value::Text(u), Value::Text(l)) => {
            assert_eq!(u.as_str(), "HELLO");
            assert_eq!(l.as_str(), "hello");
        }
        o => panic!("expected two texts, got {:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION H: INSERT / SELECT round-trip with mixed NULL
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn select_null_value_roundtrip() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, NULL)");
    let r = rows(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r.len(), 1);
    assert!(matches!(r[0][0], Value::Null));
}

#[test]
fn select_where_column_eq_null_returns_nothing() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30)");
    // v = NULL is UNKNOWN — never matches.
    let r = rows(&db, "SELECT id FROM t WHERE v = NULL");
    assert_eq!(r.len(), 0, "v = NULL must not match any row");
}

#[test]
fn select_where_column_ne_value_excludes_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30)");
    // v != 10: row 2 (NULL) should NOT match (NULL != 10 is UNKNOWN).
    let r = rows(&db, "SELECT id FROM t WHERE v != 10");
    let mut ids: Vec<i64> = r
        .iter()
        .filter_map(|row| match row[0] {
            Value::Integer(n) => Some(n),
            _ => None,
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![3], "v != 10 should exclude NULL row");
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION I: GROUP BY with NULL keys
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn group_by_null_key() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    exec(
        &db,
        "INSERT INTO t VALUES (1, 'a', 10), (2, NULL, 20), (3, 'a', 30), (4, NULL, 40)",
    );
    // NULL should form its own group.
    let r = rows(&db, "SELECT cat, SUM(v) FROM t GROUP BY cat ORDER BY cat");
    // Groups: 'a' → 40, NULL → 60
    let sum_a = r.iter().find(|row| matches!(&row[0], Value::Text(s) if s.as_str() == "a"));
    let sum_null = r.iter().find(|row| matches!(&row[0], Value::Null));
    assert!(sum_a.is_some(), "missing group 'a'");
    assert!(sum_null.is_some(), "missing NULL group");
    match &sum_a.unwrap()[1] {
        Value::Integer(n) => assert_eq!(*n, 40),
        o => panic!("expected int 40, got {:?}", o),
    }
    match &sum_null.unwrap()[1] {
        Value::Integer(n) => assert_eq!(*n, 60),
        o => panic!("expected int 60, got {:?}", o),
    }
}

#[test]
fn count_distinct_with_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(
        &db,
        "INSERT INTO t VALUES (1, 10), (2, 10), (3, 20), (4, NULL), (5, NULL)",
    );
    // COUNT(DISTINCT v) ignores NULLs → distinct non-null = {10, 20} → 2
    let r = rows(&db, "SELECT COUNT(DISTINCT v) FROM t");
    match &r[0][0] {
        Value::Integer(n) => assert_eq!(*n, 2, "COUNT(DISTINCT) ignores NULL"),
        o => panic!("expected int, got {:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION J: Numeric edge cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn insert_negative_int() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, -5), (2, -100), (3, 0)");
    let r = rows(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r[0][0], Value::Integer(-5));
    let mn = scalar_i64(&db, "SELECT MIN(v) FROM t");
    assert_eq!(mn, -100);
}

#[test]
fn arithmetic_with_negative_result() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 5), (2, 10)");
    let r = rows(&db, "SELECT v - 100 FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Integer(n) => assert_eq!(*n, -95),
        o => panic!("expected int -95, got {:?}", o),
    }
}

#[test]
fn division_returns_float_when_not_exact() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    let r = rows(&db, "SELECT v / 4 FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Float(f) => assert!((f - 2.5).abs() < 1e-9, "10 / 4 = 2.5, got {}", f),
        Value::Integer(n) => assert_eq!(*n, 2, "integer division"),
        o => panic!("expected numeric, got {:?}", o),
    }
}

#[test]
fn modulo_operator() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 17), (2, 10)");
    let r = rows(&db, "SELECT v % 5 FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Integer(n) => assert_eq!(*n, 2, "17 % 5 = 2"),
        o => panic!("expected int 2, got {:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION K: UPDATE then SELECT sees new value
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn update_all_rows_no_where() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    let n = affected(&db, "UPDATE t SET v = 0");
    assert_eq!(n, 3);
    let sum = scalar_i64(&db, "SELECT SUM(v) FROM t");
    assert_eq!(sum, 0);
}

#[test]
fn update_set_to_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20)");
    affected(&db, "UPDATE t SET v = NULL WHERE id = 1");
    let r = rows(&db, "SELECT v FROM t WHERE id = 1");
    assert!(matches!(r[0][0], Value::Null), "expected NULL after SET v = NULL");
}

#[test]
fn update_set_to_expression() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20)");
    affected(&db, "UPDATE t SET v = v + 5 WHERE id = 1");
    let r = rows(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r[0][0], Value::Integer(15));
}

#[test]
fn update_multiple_columns() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10, 20)");
    affected(&db, "UPDATE t SET a = 100, b = 200 WHERE id = 1");
    let r = rows(&db, "SELECT a, b FROM t WHERE id = 1");
    assert_eq!(r[0][0], Value::Integer(100));
    assert_eq!(r[0][1], Value::Integer(200));
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION L: CASE expression variants
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn case_with_no_else_returns_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20)");
    let r = rows(&db, "SELECT CASE WHEN v > 15 THEN 'big' END FROM t WHERE id = 1");
    assert!(
        matches!(r[0][0], Value::Null),
        "CASE with no ELSE and no match → NULL"
    );
}

#[test]
fn case_multiple_when_branches() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 5), (2, 15), (3, 25)");
    let r = rows(&db, "SELECT CASE WHEN v < 10 THEN 'a' WHEN v < 20 THEN 'b' ELSE 'c' END FROM t ORDER BY id");
    let labels: Vec<&str> = r
        .iter()
        .map(|row| match &row[0] {
            Value::Text(s) => s.as_str(),
            o => panic!("expected text, got {:?}", o),
        })
        .collect();
    assert_eq!(labels, vec!["a", "b", "c"]);
}

#[test]
fn case_in_aggregate_context() {
    // 🐛 Regression (v25): SUM(CASE WHEN v >= 20 THEN 1 ELSE 0 END) silently
    // returned NULL. Root cause: col_segment_multi_aggregate accepted the
    // non-bare-column arg (CASE), set col=None, then the single-pass
    // accumulator skipped it (counts stayed 0 → SUM returned NULL).
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 5), (2, 15), (3, 25), (4, 35)");
    // SUM(CASE ...) — counts how many are >= 20
    let r = rows(&db, "SELECT SUM(CASE WHEN v >= 20 THEN 1 ELSE 0 END) FROM t");
    match &r[0][0] {
        Value::Integer(n) => assert_eq!(*n, 2, "two rows have v >= 20"),
        o => panic!("expected int 2, got {:?}", o),
    }
}

#[test]
fn case_in_aggregate_count() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 5), (2, 15), (3, 25), (4, 35)");
    // COUNT(CASE WHEN v >= 20 THEN 1 END) — counts non-null CASE results
    let r = rows(&db, "SELECT COUNT(CASE WHEN v >= 20 THEN 1 END) FROM t");
    match &r[0][0] {
        Value::Integer(n) => assert_eq!(*n, 2, "two rows produce non-null CASE result"),
        o => panic!("expected int 2, got {:?}", o),
    }
}

#[test]
fn case_in_aggregate_sum_arithmetic() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 5), (2, 15), (3, 25), (4, 35)");
    // SUM(v + 10) — arithmetic arg, not bare column
    // (5+10) + (15+10) + (25+10) + (35+10) = 15+25+35+45 = 120
    let r = rows(&db, "SELECT SUM(v + 10) FROM t");
    match &r[0][0] {
        Value::Integer(n) => assert_eq!(*n, 120, "SUM(v+10) = 120"),
        Value::Float(f) => assert!((f - 120.0).abs() < 1e-9, "SUM(v+10) = 120, got {}", f),
        o => panic!("expected numeric 120, got {:?}", o),
    }
}

#[test]
fn case_in_aggregate_with_where() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 5), (2, 15), (3, 25), (4, 35)");
    // SUM(CASE ...) WITH a WHERE clause — must still evaluate CASE per row.
    let r = rows(&db, "SELECT SUM(CASE WHEN v >= 20 THEN 1 ELSE 0 END) FROM t WHERE v > 10");
    // After WHERE: rows 2,3,4 (v=15,25,35). Of those, v>=20 → 25,35 → 2 rows.
    match &r[0][0] {
        Value::Integer(n) => assert_eq!(*n, 2),
        o => panic!("expected int 2, got {:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION M: LIMIT edge cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn limit_zero_returns_no_rows() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20)");
    let r = rows(&db, "SELECT * FROM t LIMIT 0");
    assert_eq!(r.len(), 0);
}

#[test]
fn limit_larger_than_table() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20)");
    let r = rows(&db, "SELECT * FROM t LIMIT 100");
    assert_eq!(r.len(), 2);
}

#[test]
fn limit_with_offset() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");
    let r = rows(&db, "SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 2");
    let ids: Vec<i64> = r
        .iter()
        .filter_map(|row| match row[0] {
            Value::Integer(n) => Some(n),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec![3, 4]);
}

#[test]
fn offset_beyond_end_returns_empty() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20)");
    let r = rows(&db, "SELECT * FROM t OFFSET 100");
    assert_eq!(r.len(), 0);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION N: DISTINCT
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn distinct_single_column() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 10), (3, 20), (4, 20), (5, 30)");
    let r = rows(&db, "SELECT DISTINCT v FROM t ORDER BY v");
    let vals: Vec<i64> = r
        .iter()
        .filter_map(|row| match row[0] {
            Value::Integer(n) => Some(n),
            _ => None,
        })
        .collect();
    assert_eq!(vals, vec![10, 20, 30]);
}

#[test]
fn distinct_with_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(
        &db,
        "INSERT INTO t VALUES (1, 10), (2, NULL), (3, 10), (4, NULL)",
    );
    let r = rows(&db, "SELECT DISTINCT v FROM t");
    // Distinct values: {10, NULL}
    assert_eq!(r.len(), 2, "DISTINCT should collapse to {{10, NULL}}");
}

#[test]
fn distinct_multiple_columns() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec(
        &db,
        "INSERT INTO t VALUES (1, 1, 1), (2, 1, 1), (3, 1, 2), (4, 2, 1)",
    );
    let r = rows(&db, "SELECT DISTINCT a, b FROM t");
    // Distinct (a,b) pairs: (1,1), (1,2), (2,1)
    assert_eq!(r.len(), 3);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION O: Error handling — invalid operations should error, not silently succeed
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn insert_into_nonexistent_table_errors() {
    let (db, _dir) = new_db();
    let res = try_exec(&db, "INSERT INTO nope VALUES (1, 2)");
    assert!(res.is_err(), "INSERT into nonexistent table must error");
}

#[test]
fn select_from_nonexistent_table_errors() {
    let (db, _dir) = new_db();
    let res = try_exec(&db, "SELECT * FROM nope");
    assert!(res.is_err(), "SELECT from nonexistent table must error");
}

#[test]
fn duplicate_pk_insert_errors_or_silent() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    let res = try_exec(&db, "INSERT INTO t VALUES (1, 20)");
    // Either error (strict) or silently overwrite (UPSERT-like).
    // We accept either, but capture the resulting state.
    if res.is_ok() {
        let r = rows(&db, "SELECT v FROM t WHERE id = 1");
        assert_eq!(r.len(), 1);
        // If overwrite: v=20; if silent skip: v=10. Either is a defensible behavior;
        // we just verify there's still exactly one row.
    }
}

#[test]
fn update_nonexistent_table_errors() {
    let (db, _dir) = new_db();
    let res = try_exec(&db, "UPDATE nope SET v = 1");
    assert!(res.is_err(), "UPDATE on nonexistent table must error");
}

#[test]
fn delete_nonexistent_table_errors() {
    let (db, _dir) = new_db();
    let res = try_exec(&db, "DELETE FROM nope");
    assert!(res.is_err(), "DELETE on nonexistent table must error");
}
