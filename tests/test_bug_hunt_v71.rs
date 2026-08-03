//! Bug Hunt v71 — eighteenth round: error handling, parser edges, odd queries.

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

// ─────────────────────────────────────────────────────────────────────────
// Error cases (should error, not crash or silently succeed)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_select_nonexistent_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    let res = db.execute("SELECT nonexistent FROM t");
    assert!(res.is_err(), "nonexistent column should error");
}

#[test]
fn test_select_nonexistent_table() {
    let (db, _d) = db();
    let res = db.execute("SELECT * FROM nonexistent");
    assert!(res.is_err(), "nonexistent table should error");
}

#[test]
fn test_insert_column_count_mismatch() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(a INT, b INT, c INT)").unwrap();
    let res = db.execute("INSERT INTO t VALUES (1, 2)");
    assert!(res.is_err(), "column count mismatch should error");
}

#[test]
fn test_insert_nonexistent_table() {
    let (db, _d) = db();
    let res = db.execute("INSERT INTO nope VALUES (1)");
    assert!(res.is_err());
}

#[test]
fn test_update_nonexistent_table() {
    let (db, _d) = db();
    let res = db.execute("UPDATE nope SET v = 1");
    assert!(res.is_err());
}

#[test]
fn test_drop_nonexistent_table() {
    let (db, _d) = db();
    let res = db.execute("DROP TABLE nope");
    assert!(res.is_err());
}

#[test]
fn test_create_duplicate_table() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    let res = db.execute("CREATE TABLE t(id INT PRIMARY KEY)");
    assert!(res.is_err(), "duplicate table name should error");
}

#[test]
fn test_division_by_zero_errors() {
    let (db, _d) = db();
    let res = db.execute("SELECT 1 / 0");
    assert!(res.is_err(), "integer division by zero should error");
}

#[test]
fn test_unknown_function() {
    let (db, _d) = db();
    let res = db.execute("SELECT UNKNOWN_FUNC(1)");
    // Should error (unknown function), not silently return NULL/wrong value.
    assert!(res.is_err(), "unknown function should error");
}

// ─────────────────────────────────────────────────────────────────────────
// Type mismatch on INSERT (strict)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_insert_text_into_int_errors() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    let res = db.execute("INSERT INTO t VALUES (1, 'not a number')");
    // Non-numeric text into INT should error.
    assert!(res.is_err(), "text into INT column should error");
}

#[test]
fn test_insert_int_into_bool_strict() {
    // INSERT is strict about Bool vs Int (both directions rejected at insert),
    // even though comparison-level coercion (`b = 1`) works. This documents
    // that strictness — consistent with `INSERT INTO int_col VALUES (TRUE)`
    // also being rejected.
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, b BOOLEAN)")
        .unwrap();
    let res = db.execute("INSERT INTO t VALUES (1, 1)");
    assert!(res.is_err(), "INSERT int into BOOLEAN is strict (rejected)");
    // But TRUE works.
    db.execute("INSERT INTO t VALUES (1, TRUE)").unwrap();
    let r = q(&db, "SELECT b FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Bool(true)]]);
    // And comparison-level coercion works (b = 1 matches TRUE).
    let r2 = q(&db, "SELECT id FROM t WHERE b = 1");
    assert_eq!(r2, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_failed_insert_does_not_block_same_pk() {
    // 🔑 Regression: a rejected INSERT (validation failure) previously left a
    // phantom entry in the PK cache, so a subsequent valid INSERT with the same
    // PK failed with "Duplicate primary key" even though no row was stored.
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, b BOOLEAN)")
        .unwrap();
    // First insert: rejected (type mismatch).
    let bad = db.execute("INSERT INTO t VALUES (1, 1)");
    assert!(bad.is_err());
    // No row should exist.
    let r0 = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r0, vec![vec![Value::Integer(0)]]);
    // Second insert with same PK: must succeed (phantom PK was cleaned up).
    db.execute("INSERT INTO t VALUES (1, TRUE)")
        .expect("valid INSERT with same PK as a prior FAILED insert must succeed");
    let r1 = q(&db, "SELECT id, b FROM t");
    assert_eq!(r1, vec![vec![Value::Integer(1), Value::Bool(true)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Parser edges
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_trailing_semicolon() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t;");
    assert_eq!(r, vec![vec![Value::Integer(0)]]);
}

#[test]
fn test_extra_whitespace() {
    let (db, _d) = db();
    let r = q(&db, "SELECT    1   +   2");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

#[test]
fn test_newlines_in_query() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 1\n+\n2");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

#[test]
fn test_case_insensitive_keywords() {
    let (db, _d) = db();
    db.execute("create table t(id int primary key)").unwrap();
    let r = q(&db, "SeLeCt COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(0)]]);
}

#[test]
fn test_mixed_case_function() {
    let (db, _d) = db();
    let r = q(&db, "SELECT Abs(-5), UPPER('a')");
    assert_eq!(r, vec![vec![Value::Integer(5), Value::text("A".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Quoted identifiers (if supported)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_double_quoted_identifier() {
    let (db, _d) = db();
    let res = db.execute("CREATE TABLE t(\"id\" INT PRIMARY KEY)");
    match res {
        Ok(_) => {
            let r = q(&db, "SELECT \"id\" FROM t");
            assert_eq!(r.len(), 0); // empty table, just no error
        }
        Err(_) => { /* quoted identifiers may be unsupported */ }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// NULL keyword as value
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_select_null_literal() {
    let (db, _d) = db();
    let r = q(&db, "SELECT NULL");
    assert_eq!(r, vec![vec![Value::Null]]);
}

#[test]
fn test_select_true_false_literals() {
    let (db, _d) = db();
    let r = q(&db, "SELECT TRUE, FALSE");
    assert_eq!(r, vec![vec![Value::Bool(true), Value::Bool(false)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Complex nested expression
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_deeply_nested_expression() {
    let (db, _d) = db();
    let r = q(&db, "SELECT ((((1 + 2) * 3) - 4) / 5) + 6");
    // ((3*3)-4)/5 + 6 = 5/5 + 6 = 1 + 6 = 7
    assert_eq!(r, vec![vec![Value::Integer(7)]]);
}

#[test]
fn test_expression_with_functions_nested() {
    let (db, _d) = db();
    let r = q(&db, "SELECT ABS(ROUND(-2.6, 0))");
    // ROUND(-2.6) = -3, ABS(-3) = 3
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Empty table + aggregate (returns NULL/0 correctly)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_empty_table_aggregates_all() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(v INT)").unwrap();
    let r = q(
        &db,
        "SELECT COUNT(*), COUNT(v), SUM(v), AVG(v), MIN(v), MAX(v) FROM t",
    );
    assert_eq!(
        r,
        vec![vec![
            Value::Integer(0),
            Value::Integer(0),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null
        ]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// DESC keyword on CREATE INDEX / table (if supported)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_index_simple_equality() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    db.execute("CREATE INDEX idx_v ON t(v)").unwrap();
    // Equality lookup via index.
    let r = q(&db, "SELECT id FROM t WHERE v = 20");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE 1=1 and WHERE 1=0 (constant conditions)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_where_always_true() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE 1 = 1 ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
}

#[test]
fn test_where_always_false() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE 1 = 0");
    assert!(r.is_empty());
}

#[test]
fn test_where_true_literal() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2)").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t WHERE TRUE");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}
