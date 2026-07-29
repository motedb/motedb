//! Bug Hunt v40 — COUNT(*) alias in derived tables / CTEs.
//!
//! `SELECT t.c FROM (SELECT COUNT(*) as c FROM base) t` returned NULL
//! because the COUNT(*) O(1) fast paths hardcoded the column name as
//! "COUNT(*)" instead of using the user-provided alias "c". The outer
//! query then couldn't find column "c" in the derived table.
//!
//! Fixed both fast paths (with-WHERE and without-WHERE) to use the alias
//! from SelectColumn::Expr(_, Some(alias)) when present.

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
// COUNT(*) alias in derived table
// =========================================================================

#[test]
fn test_count_alias_derived_table() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();
    let r = q(&db, "SELECT t.c FROM (SELECT COUNT(*) as c FROM t) t");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

#[test]
fn test_count_alias_derived_table_with_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();
    let r = q(&db, "SELECT t.c FROM (SELECT COUNT(*) as c FROM t WHERE v > 10) t");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

// =========================================================================
// COUNT(*) alias in CTE
// =========================================================================

#[test]
fn test_count_alias_cte() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();
    let r = q(&db, "WITH x AS (SELECT COUNT(*) as c FROM t) SELECT c FROM x");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

#[test]
fn test_count_alias_cte_with_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();
    let r = q(&db, "WITH x AS (SELECT COUNT(*) as c FROM t WHERE v > 10) SELECT c FROM x");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

// =========================================================================
// Other aggregate aliases in derived table (regression)
// =========================================================================

#[test]
fn test_sum_alias_derived_table() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();
    let r = q(&db, "SELECT t.s FROM (SELECT SUM(v) as s FROM t) t");
    assert_eq!(r, vec![vec![Value::Integer(60)]]);
}

#[test]
fn test_max_alias_derived_table() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();
    let r = q(&db, "SELECT t.m FROM (SELECT MAX(v) as m FROM t) t");
    assert_eq!(r, vec![vec![Value::Integer(30)]]);
}

// =========================================================================
// COUNT(*) without alias (default name) — regression
// =========================================================================

#[test]
fn test_count_no_alias_direct() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2)").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r[0][0], Value::Integer(2));
}

#[test]
fn test_count_alias_direct() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    let r = q(&db, "SELECT COUNT(*) as total FROM t");
    assert_eq!(r[0][0], Value::Integer(3));
}
