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
