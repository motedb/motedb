//! INSERT naming a column that does not exist must fail with
//! ColumnNotFound instead of silently storing NULL for the typo'd column
//! (v0.16.0 accepted it and reported `affected_rows: 1`).
//!
//! `values_to_row_by_columns` (sql/row_converter.rs) looks each listed
//! column up with `if let Some(col_def) = schema.get_column(name)` and has
//! no else branch; nothing upstream validates the INSERT column list
//! (the v0.16 `validate_where_columns` fix covers WHERE only).

use motedb::types::Value;
use motedb::{Database, QueryResult};
use tempfile::TempDir;

#[test]
fn insert_unknown_column_is_rejected() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    let r = db.execute("INSERT INTO t (id, nmae) VALUES (1, 'Alice')");
    let err = r.as_ref().err().map(|e| e.to_string());
    eprintln!(
        "INSERT with typo column -> is_err={} err={err:?}",
        r.is_err()
    );

    let rows = match db
        .execute("SELECT * FROM t")
        .unwrap()
        .materialize()
        .unwrap()
    {
        QueryResult::Select { rows, .. } => rows,
        other => panic!("unexpected: {other:?}"),
    };
    eprintln!("stored: {rows:?}");

    assert!(r.is_err(), "expected ColumnNotFound, got Ok");
    assert!(
        err.as_deref().unwrap_or("").contains("nmae"),
        "error should name the bad column: {err:?}"
    );
    assert!(rows.is_empty(), "nothing must be stored: {rows:?}");
}

#[test]
fn insert_select_unknown_target_column_is_rejected() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO src VALUES (1, 'Ada')").unwrap();

    let r = db.execute("INSERT INTO dst (id, nmae) SELECT id, name FROM src");
    assert!(
        r.is_err(),
        "INSERT ... SELECT with unknown target column must fail"
    );

    let rows = match db
        .execute("SELECT * FROM dst")
        .unwrap()
        .materialize()
        .unwrap()
    {
        QueryResult::Select { rows, .. } => rows,
        other => panic!("unexpected: {other:?}"),
    };
    assert!(rows.is_empty(), "nothing must be stored: {rows:?}");
}

/// A correct column list (any order, subset) must keep working.
#[test]
fn insert_with_valid_column_list_still_works() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INT)")
        .unwrap();
    db.execute("INSERT INTO t (name, id) VALUES ('Ada', 1)")
        .unwrap();
    db.execute("INSERT INTO t (id) VALUES (2)").unwrap();
    let rows = match db
        .execute("SELECT id, name, age FROM t ORDER BY id")
        .unwrap()
        .materialize()
        .unwrap()
    {
        QueryResult::Select { rows, .. } => rows,
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], Value::Text("Ada".into()));
    assert!(matches!(rows[1][1], Value::Null));
}
