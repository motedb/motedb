//! Bug Hunt v52 — IS NULL / postfix operator precedence.
//!
//! **Bug:** `a + b IS NULL` parsed as `a + (b IS NULL)` instead of
//! `(a + b) IS NULL`. The parser's postfix operators (IS NULL, IN, LIKE,
//! BETWEEN) had no precedence check — they were always consumed regardless
//! of the surrounding `min_precedence`. So when parsing the right operand
//! of `+` (min_precedence=5), the IS NULL postfix was consumed inside it,
//! binding IS NULL tighter than arithmetic.
//!
//! Result: `WHERE a + b IS NULL` matched rows where `a + (b IS NULL)` was
//! truthy (e.g. row [1,10,NULL] → 10 + TRUE = 11 → truthy → wrongly matched),
//! instead of rows where `a + b` is actually NULL.
//!
//! **Fix:** Added a `POSTFIX_PRECEDENCE` constant (= 3, same as comparison
//! operators) and only consume postfix operators when
//! `POSTFIX_PRECEDENCE >= min_precedence`.

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

fn nulls_db() -> (Database, TempDir) {
    let (db, dir) = db();
    db.execute("CREATE TABLE nulls(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute(
        "INSERT INTO nulls VALUES (1, 10, NULL), (2, NULL, 20), (3, NULL, NULL), (4, 5, 5)",
    )
    .unwrap();
    (db, dir)
}

// ─────────────────────────────────────────────────────────────────────────
// IS NULL with arithmetic expression
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_arith_expr_is_null() {
    let (db, _d) = nulls_db();
    // a + b IS NULL: rows 1 (10+NULL=NULL), 2 (NULL+20=NULL), 3 (NULL+NULL=NULL).
    // Row 4: 5+5=10 (not NULL).
    let r = q(&db, "SELECT id FROM nulls WHERE a + b IS NULL ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(2)], vec![Value::Integer(3)]]);
}

#[test]
fn test_arith_expr_is_not_null() {
    let (db, _d) = nulls_db();
    // a + b IS NOT NULL: only row 4 (5+5=10).
    let r = q(&db, "SELECT id FROM nulls WHERE a + b IS NOT NULL ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(4)]]);
}

#[test]
fn test_mul_expr_is_null() {
    let (db, _d) = nulls_db();
    let r = q(&db, "SELECT id FROM nulls WHERE a * b IS NULL ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(2)], vec![Value::Integer(3)]]);
}

#[test]
fn test_sub_expr_is_null() {
    let (db, _d) = nulls_db();
    let r = q(&db, "SELECT id FROM nulls WHERE a - b IS NULL ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(2)], vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// IS NULL with simple column (regression — was already correct)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_col_is_null() {
    let (db, _d) = nulls_db();
    let r = q(&db, "SELECT id FROM nulls WHERE a IS NULL ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]);
}

#[test]
fn test_col_is_not_null() {
    let (db, _d) = nulls_db();
    let r = q(&db, "SELECT id FROM nulls WHERE b IS NOT NULL ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(2)], vec![Value::Integer(4)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// IS NULL with concat (|| binds tighter than IS NULL)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_concat_expr_is_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a TEXT, b TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'x', 'y'), (2, 'x', NULL), (3, NULL, NULL)").unwrap();
    // a || b IS NULL: row 2 (x||NULL=NULL), row 3 (NULL||NULL=NULL).
    // Row 1: 'x'||'y'='xy' (not NULL).
    let r = q(&db, "SELECT id FROM t WHERE a || b IS NULL ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Combined with AND/OR — precedence must still be right
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_arith_is_null_and_condition() {
    let (db, _d) = nulls_db();
    // a + b IS NULL AND id > 1: rows 2, 3 (id>1 among the NULL ones)
    let r = q(&db, "SELECT id FROM nulls WHERE a + b IS NULL AND id > 1 ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]);
}

#[test]
fn test_arith_is_null_or_condition() {
    let (db, _d) = nulls_db();
    // a + b IS NULL OR id = 4: rows 1,2,3 (NULL) + row 4 (id=4)
    let r = q(&db, "SELECT id FROM nulls WHERE a + b IS NULL OR id = 4 ORDER BY id");
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(3)],
            vec![Value::Integer(4)],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// After checkpoint (col-segment path) — same parser, regression guard
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_arith_is_null_after_checkpoint() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE nulls(id INT PRIMARY KEY, a INT, b INT)").unwrap();
        db.execute(
            "INSERT INTO nulls VALUES (1, 10, NULL), (2, NULL, 20), (3, NULL, NULL), (4, 5, 5)",
        )
        .unwrap();
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT id FROM nulls WHERE a + b IS NULL ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(2)], vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// IN / LIKE / BETWEEN with arithmetic (precedence consistency)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_arith_in_list() {
    let (db, _d) = nulls_db();
    // a + b IN (10, 30): row 4 (5+5=10). Rows 1,2,3 have NULL a+b.
    let r = q(&db, "SELECT id FROM nulls WHERE a + b IN (10) ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(4)]]);
}

#[test]
fn test_arith_between() {
    let (db, _d) = nulls_db();
    // a + b BETWEEN 1 AND 100: row 4 (5+5=10). Others NULL.
    let r = q(&db, "SELECT id FROM nulls WHERE a + b BETWEEN 1 AND 100 ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(4)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Simple CASE (CASE expr WHEN val THEN ...) — was a parse error.
// The parser only supported the searched form (CASE WHEN cond THEN ...).
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_simple_case_with_else() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 5)").unwrap();
    let r = q(
        &db,
        "SELECT CASE a WHEN 10 THEN 'ten' ELSE 'other' END FROM t ORDER BY id",
    );
    assert_eq!(
        r,
        vec![vec![Value::text("ten".into())], vec![Value::text("other".into())]]
    );
}

#[test]
fn test_simple_case_no_else() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 5)").unwrap();
    // No ELSE: row 2 (a=5) doesn't match → NULL.
    let r = q(&db, "SELECT CASE a WHEN 10 THEN 'ten' END FROM t ORDER BY id");
    assert_eq!(
        r,
        vec![vec![Value::text("ten".into())], vec![Value::Null]]
    );
}

#[test]
fn test_simple_case_multi_when() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();
    let r = q(
        &db,
        "SELECT CASE a WHEN 10 THEN 'A' WHEN 20 THEN 'B' ELSE 'C' END FROM t ORDER BY id",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::text("A".into())],
            vec![Value::text("B".into())],
            vec![Value::text("C".into())],
        ]
    );
}

#[test]
fn test_simple_case_with_text() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'cat'), (2, 'dog')").unwrap();
    let r = q(
        &db,
        "SELECT CASE s WHEN 'cat' THEN 1 WHEN 'dog' THEN 2 ELSE 0 END FROM t ORDER BY id",
    );
    assert_eq!(
        r,
        vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
    );
}

#[test]
fn test_searched_case_still_works() {
    // Regression: the searched form (CASE WHEN cond ...) must still work.
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 5)").unwrap();
    let r = q(
        &db,
        "SELECT CASE WHEN a = 10 THEN 'ten' ELSE 'other' END FROM t ORDER BY id",
    );
    assert_eq!(
        r,
        vec![vec![Value::text("ten".into())], vec![Value::text("other".into())]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Nested BEGIN TRANSACTION — must error (was silently starting a new txn,
// discarding the outer txn's buffered writes → data loss).
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_nested_begin_errors() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("BEGIN TRANSACTION").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    // Nested BEGIN must error, not silently reset the transaction.
    let r = db.execute("BEGIN TRANSACTION");
    assert!(r.is_err(), "nested BEGIN should error");
    // Outer transaction is still active — rollback undoes both inserts.
    db.execute("ROLLBACK").unwrap();
    let r = q(&db, "SELECT * FROM t ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1), Value::Integer(10)]]);
}

#[test]
fn test_begin_after_commit_ok() {
    // After COMMIT, a new BEGIN should work (not treated as nested).
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("BEGIN TRANSACTION").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("COMMIT").unwrap();
    // New transaction after commit — must succeed.
    let r = db.execute("BEGIN TRANSACTION");
    assert!(r.is_ok(), "BEGIN after COMMIT should succeed");
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    db.execute("COMMIT").unwrap();
    let r = q(&db, "SELECT * FROM t ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
}
