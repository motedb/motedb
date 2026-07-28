//! Bug Hunt v32 — NOT operator precedence, ALTER TABLE DEFAULT backfill,
//! ALTER TABLE NOT NULL parsing, AUTO_INCREMENT PureInteger.
//!
//! Each test corresponds to a confirmed wrong-result/parse-error bug.

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
// A: NOT operator precedence
// =========================================================================

#[test]
fn test_not_greater_than() {
    // NOT v > 20 should be (NOT (v > 20)), not ((NOT v) > 20).
    // Previously parsed as (NOT v) > 20 which matched nothing.
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 30), (2, 10), (3, 20)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE NOT v > 20 ORDER BY id");
    // v=30 → NOT(30>20)=NOT(true)=false (excluded)
    // v=10 → NOT(10>20)=NOT(false)=true (included)
    // v=20 → NOT(20>20)=NOT(false)=true (included)
    assert_eq!(r, vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]);
}

#[test]
fn test_not_less_than() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 30), (2, 10), (3, 20)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE NOT v < 20 ORDER BY id");
    // v=30 → NOT(true)=false... wait: 30<20=false, NOT(false)=true (included)
    // v=10 → 10<20=true, NOT(true)=false (excluded)
    // v=20 → 20<20=false, NOT(false)=true (included)
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

#[test]
fn test_not_equals() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 30), (2, 10), (3, 20)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE NOT v = 20 ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
}

#[test]
fn test_not_with_and() {
    // NOT binds tighter than AND: NOT v > 20 AND v > 5
    // = (NOT (v > 20)) AND (v > 5)
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 30), (2, 10), (3, 3)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE NOT v > 20 AND v > 5 ORDER BY id");
    // v=30: NOT(30>20)=false AND ... → false
    // v=10: NOT(10>20)=true AND 10>5=true → true
    // v=3:  NOT(3>20)=true AND 3>5=false → false
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_not_null_semistics() {
    // NOT (NULL > 20) = NOT UNKNOWN = UNKNOWN → row excluded.
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 30), (2, NULL)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE NOT v > 20 ORDER BY id");
    // Only id=1 (v=30): NOT(30>20)=false → excluded!
    // id=2 (v=NULL): NOT(unknown)=unknown → excluded.
    assert!(r.is_empty());
}

// =========================================================================
// B: ALTER TABLE ADD COLUMN ... DEFAULT backfill
// =========================================================================

#[test]
fn test_alter_add_column_default_integer() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    db.execute("ALTER TABLE t ADD COLUMN cnt INTEGER DEFAULT 42").unwrap();
    let r = q(&db, "SELECT id, cnt FROM t ORDER BY id");
    assert_eq!(r, vec![
        vec![Value::Integer(1), Value::Integer(42)],
        vec![Value::Integer(2), Value::Integer(42)],
        vec![Value::Integer(3), Value::Integer(42)],
    ]);
}

#[test]
fn test_alter_add_column_default_text() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2)").unwrap();
    db.execute("ALTER TABLE t ADD COLUMN name TEXT DEFAULT 'unknown'").unwrap();
    let r = q(&db, "SELECT id, name FROM t ORDER BY id");
    assert_eq!(r[0], vec![Value::Integer(1), Value::text("unknown".into())]);
    assert_eq!(r[1], vec![Value::Integer(2), Value::text("unknown".into())]);
}

#[test]
fn test_alter_add_column_default_float() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2)").unwrap();
    db.execute("ALTER TABLE t ADD COLUMN score FLOAT DEFAULT 1.5").unwrap();
    let r = q(&db, "SELECT id, score FROM t ORDER BY id");
    assert_eq!(r[0], vec![Value::Integer(1), Value::Float(1.5)]);
    assert_eq!(r[1], vec![Value::Integer(2), Value::Float(1.5)]);
}

#[test]
fn test_alter_add_column_no_default_is_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("ALTER TABLE t ADD COLUMN note TEXT").unwrap();
    let r = q(&db, "SELECT id, note FROM t");
    assert_eq!(r[0], vec![Value::Integer(1), Value::Null]);
}

#[test]
fn test_alter_add_column_default_persists_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
        db.execute("INSERT INTO t VALUES (1), (2)").unwrap();
        db.execute("ALTER TABLE t ADD COLUMN cnt INTEGER DEFAULT 99").unwrap();
    }
    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT id, cnt FROM t ORDER BY id");
    assert_eq!(r[0], vec![Value::Integer(1), Value::Integer(99)]);
    assert_eq!(r[1], vec![Value::Integer(2), Value::Integer(99)]);
}

// =========================================================================
// C: ALTER TABLE ADD COLUMN ... NOT NULL [DEFAULT]
// =========================================================================

#[test]
fn test_alter_add_column_not_null_default() {
    // `NOT NULL DEFAULT 0` — previously failed to parse ("Multiple statements").
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2)").unwrap();
    db.execute("ALTER TABLE t ADD COLUMN flag INTEGER NOT NULL DEFAULT 0").unwrap();
    let r = q(&db, "SELECT id, flag FROM t ORDER BY id");
    assert_eq!(r[0], vec![Value::Integer(1), Value::Integer(0)]);
    assert_eq!(r[1], vec![Value::Integer(2), Value::Integer(0)]);
}

#[test]
fn test_alter_add_column_default_then_not_null() {
    // Reverse order: `DEFAULT 0 NOT NULL`.
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("ALTER TABLE t ADD COLUMN flag INTEGER DEFAULT 1 NOT NULL").unwrap();
    let r = q(&db, "SELECT id, flag FROM t");
    assert_eq!(r[0], vec![Value::Integer(1), Value::Integer(1)]);
}

#[test]
fn test_alter_add_column_explicit_null() {
    // `NULL` constraint (explicit nullable).
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("ALTER TABLE t ADD COLUMN note TEXT NULL").unwrap();
    let r = q(&db, "SELECT id, note FROM t");
    assert_eq!(r[0], vec![Value::Integer(1), Value::Null]);
}

// =========================================================================
// D: ALTER TABLE AUTO_INCREMENT with PureInteger token
// =========================================================================

#[test]
fn test_alter_auto_increment_pure_integer() {
    // After the PureInteger lexer change, `AUTO_INCREMENT = 100` emits
    // PureInteger(100), which the old parser didn't handle.
    let (db, _d) = db();
    db.execute("CREATE TABLE c (id INTEGER PRIMARY KEY AUTO_INCREMENT, val TEXT)").unwrap();
    db.execute("INSERT INTO c (val) VALUES ('a')").unwrap();
    db.execute("INSERT INTO c (val) VALUES ('b')").unwrap();
    db.execute("ALTER TABLE c AUTO_INCREMENT = 100").unwrap();
    // Next insert should use 100.
    db.execute("INSERT INTO c (val) VALUES ('c')").unwrap();
    let r = q(&db, "SELECT id FROM c ORDER BY id");
    assert_eq!(r[2], vec![Value::Integer(100)]);
}
