//! Prepared-statement DML regression tests (BUG #45).
//!
//! `execute_prepared` on `UPDATE t SET v = <literal> WHERE <pk> = ?` used to
//! take the fast-PK path, which only applied SET assignments whose value was
//! a **Parameter** — literal SET values were silently dropped, the old row
//! was cloned back as-is, and the statement reported `affected_rows: 1`
//! without changing anything. The same family: WHERE parameters on
//! UPDATE/DELETE were never substituted into the executor path (eval hit
//! `Parameter` → Err → swallowed as "no match").
//!
//! These tests pin: literal SET, parameter SET, mixed, DELETE with params,
//! read-your-update consistency, and the non-PK WHERE shape.

use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn rows(r: motedb::StreamingQueryResult) -> Vec<Vec<Value>> {
    use motedb::QueryResult;
    match r.materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        _ => panic!("expected select"),
    }
}

fn ints(v: &[Vec<Value>]) -> Vec<i64> {
    v.iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            o => panic!("expected int, got {o:?}"),
        })
        .collect()
}

fn setup() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, s TEXT)")
        .unwrap();
    for i in 0..10i64 {
        db.execute(&format!(
            "INSERT INTO t VALUES ({i}, {}, 's{}')",
            i * 3,
            i % 3
        ))
        .unwrap();
    }
    (db, dir)
}

#[test]
fn prepared_update_literal_set_applies() {
    let (db, _d) = setup();
    let r = db
        .execute_prepared("UPDATE t SET v = 50 WHERE id = ?", vec![Value::Integer(5)])
        .unwrap();
    let affected = match r {
        motedb::StreamingQueryResult::Modification { affected_rows } => affected_rows,
        _ => panic!("expected modification"),
    };
    assert_eq!(affected, 1, "exactly one row must be affected");
    // read-your-update through THREE different read paths
    assert_eq!(db.get_row("t", 5).unwrap().unwrap()[1], Value::Integer(50));
    assert_eq!(
        ints(&rows(db.execute("SELECT id FROM t WHERE v = 50").unwrap())),
        vec![5]
    );
    assert_eq!(
        rows(db.execute("SELECT v FROM t WHERE id = 5").unwrap())[0][0],
        Value::Integer(50)
    );
}

#[test]
fn prepared_update_param_set_applies() {
    let (db, _d) = setup();
    db.execute_prepared(
        "UPDATE t SET v = ? WHERE id = ?",
        vec![Value::Integer(77), Value::Integer(3)],
    )
    .unwrap();
    assert_eq!(db.get_row("t", 3).unwrap().unwrap()[1], Value::Integer(77));
}

#[test]
fn prepared_update_mixed_set_applies() {
    let (db, _d) = setup();
    // one literal + one parameter in the same SET clause
    db.execute_prepared(
        "UPDATE t SET v = 1, s = ? WHERE id = ?",
        vec![Value::Text("mixed".into()), Value::Integer(2)],
    )
    .unwrap();
    let row = db.get_row("t", 2).unwrap().unwrap();
    assert_eq!(row[1], Value::Integer(1), "literal SET must apply");
    assert_eq!(row[2], Value::Text("mixed".into()), "param SET must apply");
}

#[test]
fn prepared_negative_literal_set_applies() {
    let (db, _d) = setup();
    // SET with a negative literal (parser folds UnaryOp(Minus, Literal))
    db.execute_prepared("UPDATE t SET v = -5 WHERE id = ?", vec![Value::Integer(7)])
        .unwrap();
    assert_eq!(db.get_row("t", 7).unwrap().unwrap()[1], Value::Integer(-5));
}

#[test]
fn prepared_delete_with_params_removes_row() {
    let (db, _d) = setup();
    // two-parameter conjunction
    db.execute_prepared(
        "DELETE FROM t WHERE v = ? AND id > ?",
        vec![Value::Integer(15), Value::Integer(4)], // row 5 has v=15
    )
    .unwrap();
    assert!(
        db.get_row("t", 5).unwrap().is_none(),
        "targeted row must be deleted"
    );
    assert_eq!(
        ints(&rows(db.execute("SELECT COUNT(*) FROM t").unwrap())),
        vec![9]
    );
}

#[test]
fn prepared_update_non_pk_where_applies() {
    let (db, _d) = setup();
    // v = ? shape: exercises the scan path rather than the PK fast path
    db.execute_prepared(
        "UPDATE t SET v = 99 WHERE v = ?",
        vec![Value::Integer(6)], // rows 2 (v=6) and … v=6 only for id=2
    )
    .unwrap();
    assert_eq!(
        ints(&rows(db.execute("SELECT id FROM t WHERE v = 99").unwrap())),
        vec![2]
    );
}

#[test]
fn prepared_select_still_works_after_dml() {
    let (db, _d) = setup();
    // interleave SELECT and UPDATE on the same prepared cache
    let before = db
        .execute_prepared("SELECT v FROM t WHERE id = ?", vec![Value::Integer(4)])
        .unwrap();
    assert_eq!(rows(before)[0][0], Value::Integer(12));
    db.execute_prepared("UPDATE t SET v = 12 WHERE id = ?", vec![Value::Integer(4)])
        .unwrap();
    let after = db
        .execute_prepared("SELECT v FROM t WHERE id = ?", vec![Value::Integer(4)])
        .unwrap();
    assert_eq!(rows(after)[0][0], Value::Integer(12));
}

#[test]
fn prepared_dml_persists_across_checkpoint() {
    let dir = TempDir::new().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .unwrap();
        for i in 0..10i64 {
            db.execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .unwrap();
        }
        db.execute_prepared("UPDATE t SET v = 42 WHERE id = ?", vec![Value::Integer(6)])
            .unwrap();
        db.execute_prepared("DELETE FROM t WHERE id = ?", vec![Value::Integer(7)])
            .unwrap();
        db.checkpoint().unwrap();
    }
    let db = Database::open(dir.path()).unwrap();
    assert_eq!(db.get_row("t", 6).unwrap().unwrap()[1], Value::Integer(42));
    assert!(db.get_row("t", 7).unwrap().is_none());
}
