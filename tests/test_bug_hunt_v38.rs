//! Bug Hunt v38 — LAG/LEAD window functions + regression coverage.
//!
//! LAG(expr [, offset [, default]]) and LEAD(...) were defined in the AST
//! but rejected by the parser ("Unsupported window function") and the
//! executor returned NULL for them. Now fully implemented:
//!   - Parser accepts LAG/LEAD with 1-3 arguments
//!   - Executor computes the value from the offset row (default offset=1,
//!     default value=NULL)
//!   - Works with PARTITION BY (resets at partition boundaries)

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
// LAG
// =========================================================================

#[test]
fn test_lag_basic() {
    let (db, _d) = db();
    db.execute("CREATE TABLE s (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    db.execute("INSERT INTO s VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    let r = q(
        &db,
        "SELECT id, LAG(v, 1) OVER (ORDER BY id) FROM s ORDER BY id",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Null],
            vec![Value::Integer(2), Value::Integer(10)],
            vec![Value::Integer(3), Value::Integer(20)],
        ]
    );
}

#[test]
fn test_lag_default_value() {
    let (db, _d) = db();
    db.execute("CREATE TABLE s (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    db.execute("INSERT INTO s VALUES (1, 10), (2, 20)").unwrap();
    let r = q(
        &db,
        "SELECT id, LAG(v, 1, -1) OVER (ORDER BY id) FROM s ORDER BY id",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(-1)],
            vec![Value::Integer(2), Value::Integer(10)],
        ]
    );
}

#[test]
fn test_lag_offset_2() {
    let (db, _d) = db();
    db.execute("CREATE TABLE s (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    db.execute("INSERT INTO s VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
        .unwrap();
    let r = q(
        &db,
        "SELECT id, LAG(v, 2) OVER (ORDER BY id) FROM s ORDER BY id",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Null],
            vec![Value::Integer(2), Value::Null],
            vec![Value::Integer(3), Value::Integer(10)],
            vec![Value::Integer(4), Value::Integer(20)],
        ]
    );
}

#[test]
fn test_lag_partition() {
    // LAG resets at partition boundaries.
    let (db, _d) = db();
    db.execute("CREATE TABLE s (id INTEGER PRIMARY KEY, k TEXT, v INTEGER)")
        .unwrap();
    db.execute("INSERT INTO s VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30)")
        .unwrap();
    let r = q(
        &db,
        "SELECT id, LAG(v, 1) OVER (PARTITION BY k ORDER BY id) FROM s ORDER BY id",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Null],
            vec![Value::Integer(2), Value::Integer(10)],
            vec![Value::Integer(3), Value::Null], // partition 'b' resets
        ]
    );
}

// =========================================================================
// LEAD
// =========================================================================

#[test]
fn test_lead_basic() {
    let (db, _d) = db();
    db.execute("CREATE TABLE s (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    db.execute("INSERT INTO s VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    let r = q(
        &db,
        "SELECT id, LEAD(v, 1) OVER (ORDER BY id) FROM s ORDER BY id",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(20)],
            vec![Value::Integer(2), Value::Integer(30)],
            vec![Value::Integer(3), Value::Null],
        ]
    );
}

#[test]
fn test_lead_default_value() {
    let (db, _d) = db();
    db.execute("CREATE TABLE s (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    db.execute("INSERT INTO s VALUES (1, 10), (2, 20)").unwrap();
    let r = q(
        &db,
        "SELECT id, LEAD(v, 1, 0) OVER (ORDER BY id) FROM s ORDER BY id",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(20)],
            vec![Value::Integer(2), Value::Integer(0)],
        ]
    );
}

#[test]
fn test_lead_offset_2() {
    let (db, _d) = db();
    db.execute("CREATE TABLE s (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    db.execute("INSERT INTO s VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
        .unwrap();
    let r = q(
        &db,
        "SELECT id, LEAD(v, 2) OVER (ORDER BY id) FROM s ORDER BY id",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(30)],
            vec![Value::Integer(2), Value::Integer(40)],
            vec![Value::Integer(3), Value::Null],
            vec![Value::Integer(4), Value::Null],
        ]
    );
}

// =========================================================================
// LAG/LEAD with existing window functions (no regression)
// =========================================================================

#[test]
fn test_mixed_window_functions() {
    let (db, _d) = db();
    db.execute("CREATE TABLE s (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    db.execute("INSERT INTO s VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    let r = q(
        &db,
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id), RANK() OVER (ORDER BY v) FROM s ORDER BY id",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(1), Value::Integer(1)],
            vec![Value::Integer(2), Value::Integer(2), Value::Integer(2)],
            vec![Value::Integer(3), Value::Integer(3), Value::Integer(3)],
        ]
    );
}
