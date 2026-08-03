//! Bug Hunt v35 — GROUP_CONCAT aggregate (was completely broken).
//!
//! GROUP_CONCAT was recognized as a function name but NOT registered as an
//! aggregate in is_aggregate_expr, so it was evaluated per-row (returning
//! NULL for every input row). This made `SELECT GROUP_CONCAT(v) FROM t`
//! return N rows of NULL instead of one concatenated string.
//!
//! Fixed by:
//!   - Adding GROUP_CONCAT to is_aggregate_expr / collect_aggregate_calls
//!   - Implementing the aggregate in eval_aggregate + compute_aggregate_positional
//!   - Adding it to try_parse_aggregate (with separator support via extra field)
//!   - Forcing fallback from single_pass_group_by (which can't handle it)
//!   - Adding it to the evaluator's aggregate lookup (for HAVING)

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

// =========================================================================
// GROUP_CONCAT basic
// =========================================================================

#[test]
fn test_group_concat_default_separator() {
    // Default separator is comma.
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 2), (2, 4), (3, 6)")
        .unwrap();
    let r = q(&db, "SELECT GROUP_CONCAT(v) FROM t");
    assert_eq!(r, vec![vec![Value::text("2,4,6".into())]]);
}

#[test]
fn test_group_concat_custom_separator() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 2), (2, 4), (3, 6)")
        .unwrap();
    let r = q(&db, "SELECT GROUP_CONCAT(v, '-') FROM t");
    assert_eq!(r, vec![vec![Value::text("2-4-6".into())]]);
}

#[test]
fn test_group_concat_text() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();
    let r = q(&db, "SELECT GROUP_CONCAT(name) FROM t");
    assert_eq!(r, vec![vec![Value::text("alice,bob".into())]]);
}

#[test]
fn test_group_concat_text_custom_sep() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();
    let r = q(&db, "SELECT GROUP_CONCAT(name, ', ') FROM t");
    assert_eq!(r, vec![vec![Value::text("alice, bob".into())]]);
}

// =========================================================================
// GROUP_CONCAT with NULL / empty
// =========================================================================

#[test]
fn test_group_concat_skips_null() {
    // NULL values are skipped (not included as empty strings).
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 1), (2, NULL), (3, 3)")
        .unwrap();
    let r = q(&db, "SELECT GROUP_CONCAT(v) FROM t");
    assert_eq!(r, vec![vec![Value::text("1,3".into())]]);
}

#[test]
fn test_group_concat_all_null_returns_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL), (2, NULL)")
        .unwrap();
    let r = q(&db, "SELECT GROUP_CONCAT(v) FROM t");
    assert_eq!(r, vec![vec![Value::Null]]);
}

#[test]
fn test_group_concat_empty_table_returns_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    let r = q(&db, "SELECT GROUP_CONCAT(v) FROM t");
    assert_eq!(r, vec![vec![Value::Null]]);
}

#[test]
fn test_group_concat_single_value() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 42)").unwrap();
    let r = q(&db, "SELECT GROUP_CONCAT(v) FROM t");
    assert_eq!(r, vec![vec![Value::text("42".into())]]);
}

// =========================================================================
// GROUP_CONCAT with GROUP BY
// =========================================================================

#[test]
fn test_group_concat_with_group_by() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT, v INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a', 1), (2, 'a', 2), (3, 'b', 3)")
        .unwrap();
    let r = q(
        &db,
        "SELECT k, GROUP_CONCAT(v) FROM t GROUP BY k ORDER BY k",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::text("a".into()), Value::text("1,2".into())],
            vec![Value::text("b".into()), Value::text("3".into())],
        ]
    );
}

#[test]
fn test_group_concat_mixed_types() {
    // Integers and text in same column.
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'x'), (2, 'y'), (3, 'z')")
        .unwrap();
    let r = q(&db, "SELECT GROUP_CONCAT(v, '|') FROM t");
    assert_eq!(r, vec![vec![Value::text("x|y|z".into())]]);
}

// =========================================================================
// || string concatenation operator (was not parsed at all)
// =========================================================================

#[test]
fn test_concat_operator_literals() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 'a' || 'b'");
    assert_eq!(r, vec![vec![Value::text("ab".into())]]);
}

#[test]
fn test_concat_operator_chained() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 'a' || 'b' || 'c'");
    assert_eq!(r, vec![vec![Value::text("abc".into())]]);
}

#[test]
fn test_concat_operator_mixed_types() {
    // Non-text values are stringified.
    let (db, _d) = db();
    let r = q(&db, "SELECT 'v=' || 42");
    assert_eq!(r, vec![vec![Value::text("v=42".into())]]);
}

#[test]
fn test_concat_operator_null_propagates() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 'a' || NULL");
    assert_eq!(r, vec![vec![Value::Null]]);
    let r = q(&db, "SELECT NULL || 'b'");
    assert_eq!(r, vec![vec![Value::Null]]);
}

#[test]
fn test_concat_operator_with_columns() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();
    let r = q(&db, "SELECT name || '!' FROM t ORDER BY id");
    assert_eq!(
        r,
        vec![
            vec![Value::text("alice!".into())],
            vec![Value::text("bob!".into())],
        ]
    );
}

#[test]
fn test_concat_operator_multi_columns() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();
    let r = q(&db, "SELECT id || '-' || name FROM t ORDER BY id");
    assert_eq!(
        r,
        vec![
            vec![Value::text("1-alice".into())],
            vec![Value::text("2-bob".into())],
        ]
    );
}
