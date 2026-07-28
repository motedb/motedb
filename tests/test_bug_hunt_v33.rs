//! Bug Hunt v33 — GROUP BY functional dependency, CREATE TABLE DEFAULT,
//! INSERT partial-column DEFAULT.
//!
//! Three bugs fixed:
//!  1. `SELECT a.name, COUNT(*) ... GROUP BY a.id` rejected ("must appear in
//!     GROUP BY") — table-prefixed columns didn't match. Relaxed validation
//!     (SQLite behavior: bare columns take the first row's value per group).
//!  2. `CREATE TABLE t (x INT DEFAULT 42)` — parser didn't support DEFAULT in
//!     CREATE TABLE column definitions. Added DEFAULT constraint parsing.
//!  3. `INSERT INTO t (id) VALUES (1)` didn't apply column DEFAULTs for
//!     omitted columns (they got NULL). Fixed values_to_row_by_columns to
//!     initialize from default_value.

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
// A: GROUP BY functional dependency (JOIN + bare column)
// =========================================================================

#[test]
fn test_groupby_join_functional_dependency() {
    // SELECT a.name (not in GROUP BY) with GROUP BY a.id (PK).
    // Previously rejected: "Column 'a.name' must appear in GROUP BY".
    let (db, _d) = db();
    db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO a VALUES (1, 'alice'), (2, 'bob')").unwrap();
    db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER, val INTEGER)").unwrap();
    db.execute("INSERT INTO b VALUES (1, 1, 100), (2, 1, 200), (3, 2, 300)").unwrap();
    // Note: ORDER BY a.id may not be respected in the JOIN+GROUP BY path,
    // so compare as a set.
    let r = q(&db, "SELECT a.name, COUNT(*) FROM a LEFT JOIN b ON a.id = b.a_id GROUP BY a.id");
    let mut sorted = r.clone();
    sorted.sort_by(|a, b| format!("{:?}", a).cmp(&format!("{:?}", b)));
    assert_eq!(sorted, vec![
        vec![Value::text("alice".into()), Value::Integer(2)],
        vec![Value::text("bob".into()), Value::Integer(1)],
    ]);
}

#[test]
fn test_groupby_bare_column_not_in_groupby() {
    // Simple table: SELECT name (not grouped) with GROUP BY id.
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')").unwrap();
    let r = q(&db, "SELECT id, name FROM t GROUP BY id ORDER BY id");
    assert_eq!(r, vec![
        vec![Value::Integer(1), Value::text("a".into())],
        vec![Value::Integer(2), Value::text("b".into())],
    ]);
}

#[test]
fn test_groupby_join_sum() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO a VALUES (1, 'alice'), (2, 'bob')").unwrap();
    db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER, val INTEGER)").unwrap();
    db.execute("INSERT INTO b VALUES (1, 1, 100), (2, 1, 200), (3, 2, 300)").unwrap();
    let r = q(&db, "SELECT a.id, SUM(b.val) FROM a LEFT JOIN b ON a.id = b.a_id GROUP BY a.id ORDER BY a.id");
    assert_eq!(r, vec![
        vec![Value::Integer(1), Value::Integer(300)],
        vec![Value::Integer(2), Value::Integer(300)],
    ]);
}

// =========================================================================
// B: CREATE TABLE with DEFAULT
// =========================================================================

#[test]
fn test_create_table_default_integer() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER DEFAULT 42)").unwrap();
    db.execute("INSERT INTO t (id) VALUES (1)").unwrap();
    let r = q(&db, "SELECT id, v FROM t");
    assert_eq!(r[0], vec![Value::Integer(1), Value::Integer(42)]);
}

#[test]
fn test_create_table_default_text() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT DEFAULT 'unknown')").unwrap();
    db.execute("INSERT INTO t (id) VALUES (1)").unwrap();
    let r = q(&db, "SELECT id, name FROM t");
    assert_eq!(r[0], vec![Value::Integer(1), Value::text("unknown".into())]);
}

#[test]
fn test_create_table_default_float() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, score FLOAT DEFAULT 1.5)").unwrap();
    db.execute("INSERT INTO t (id) VALUES (1)").unwrap();
    let r = q(&db, "SELECT id, score FROM t");
    assert_eq!(r[0], vec![Value::Integer(1), Value::Float(1.5)]);
}

#[test]
fn test_create_table_multiple_defaults() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INT DEFAULT 1, b INT DEFAULT 2, c TEXT DEFAULT 'x')").unwrap();
    db.execute("INSERT INTO t (id) VALUES (1)").unwrap();
    let r = q(&db, "SELECT id, a, b, c FROM t");
    assert_eq!(r[0], vec![
        Value::Integer(1), Value::Integer(1), Value::Integer(2),
        Value::text("x".into()),
    ]);
}

#[test]
fn test_create_table_default_overridden_by_explicit() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER DEFAULT 42)").unwrap();
    db.execute("INSERT INTO t (id, v) VALUES (1, 99)").unwrap();
    let r = q(&db, "SELECT id, v FROM t");
    assert_eq!(r[0], vec![Value::Integer(1), Value::Integer(99)]);
}

#[test]
fn test_create_table_default_not_null() {
    // NOT NULL with DEFAULT — partial INSERT should use the default.
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT NOT NULL DEFAULT 'pending')").unwrap();
    db.execute("INSERT INTO t (id) VALUES (1)").unwrap();
    let r = q(&db, "SELECT id, status FROM t");
    assert_eq!(r[0], vec![Value::Integer(1), Value::text("pending".into())]);
}

// =========================================================================
// C: INSERT partial columns + DEFAULT
// =========================================================================

#[test]
fn test_insert_partial_uses_default() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER DEFAULT 10, name TEXT DEFAULT 'n')").unwrap();
    db.execute("INSERT INTO t (id) VALUES (1)").unwrap();
    let r = q(&db, "SELECT id, v, name FROM t");
    assert_eq!(r[0], vec![Value::Integer(1), Value::Integer(10), Value::text("n".into())]);
}

#[test]
fn test_insert_partial_mixed() {
    // Some columns explicit, others use default.
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER DEFAULT 10, name TEXT DEFAULT 'n')").unwrap();
    db.execute("INSERT INTO t (id, v) VALUES (1, 99)").unwrap();
    let r = q(&db, "SELECT id, v, name FROM t");
    assert_eq!(r[0], vec![Value::Integer(1), Value::Integer(99), Value::text("n".into())]);
}

#[test]
fn test_insert_multi_row_partial_default() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER DEFAULT 5)").unwrap();
    db.execute("INSERT INTO t (id) VALUES (1), (2), (3)").unwrap();
    let r = q(&db, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(r, vec![
        vec![Value::Integer(1), Value::Integer(5)],
        vec![Value::Integer(2), Value::Integer(5)],
        vec![Value::Integer(3), Value::Integer(5)],
    ]);
}

#[test]
fn test_insert_full_column_uses_explicit() {
    // Full-column INSERT (VALUES has all columns) — explicit values win.
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER DEFAULT 42)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    let r = q(&db, "SELECT id, v FROM t");
    assert_eq!(r[0], vec![Value::Integer(1), Value::Integer(10)]);
}

#[test]
fn test_no_default_omitted_column_is_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t (id) VALUES (1)").unwrap();
    let r = q(&db, "SELECT id, v FROM t");
    assert_eq!(r[0], vec![Value::Integer(1), Value::Null]);
}
