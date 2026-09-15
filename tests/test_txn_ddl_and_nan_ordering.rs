//! Round 10 regressions:
//! 1. NaN sort keys — NaN must sort AFTER all real values (ASC), never beat
//!    an exact 0.0 distance in top-k (Postgres semantics).
//! 2. Destructive DDL (DROP TABLE/INDEX, ALTER) inside a transaction must be
//!    rejected — it executed immediately and ROLLBACK silently lost the table.
//! 3. COMMIT/ROLLBACK without an active transaction must error (SQLite-style),
//!    not silently succeed.

use motedb::types::{ArcVec, Value};
use motedb::{DBConfig, Database};
use tempfile::TempDir;

fn db() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    (dir, db)
}

fn rows_to_f64(rows: &[Vec<Value>], col: usize) -> Vec<f64> {
    rows.iter()
        .map(|r| match &r[col] {
            Value::Float(f) => *f,
            Value::Integer(i) => *i as f64,
            other => panic!("unexpected {other:?}"),
        })
        .collect()
}

#[test]
fn order_by_float_nan_sorts_last_asc_first_desc() {
    let (_d, db) = db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, f FLOAT)")
        .unwrap();
    for (id, f) in [
        (1, 1.0f64),
        (2, f64::NAN),
        (3, -5.0),
        (4, f64::INFINITY),
        (5, 0.0),
    ] {
        db.execute_prepared(
            "INSERT INTO t (id, f) VALUES (?, ?)",
            vec![Value::Integer(id), Value::Float(f)],
        )
        .unwrap();
    }

    let rows = db.query("SELECT f FROM t ORDER BY f ASC").unwrap();
    let vals = rows_to_f64(&rows, 0);
    let nan_pos = vals.iter().position(|v| v.is_nan()).unwrap();
    assert_eq!(
        nan_pos,
        vals.len() - 1,
        "NaN must sort after +inf in ASC: {vals:?}"
    );
    assert!(vals[..nan_pos].windows(2).all(|w| w[0] <= w[1]));

    let rows = db.query("SELECT f FROM t ORDER BY f DESC").unwrap();
    let vals = rows_to_f64(&rows, 0);
    assert!(vals[0].is_nan(), "NaN must sort first in DESC: {vals:?}");
}

#[test]
fn order_by_distance_nan_never_beats_exact_match() {
    let (_d, db) = db();
    db.execute("CREATE TABLE v (id INT PRIMARY KEY, emb VECTOR(3))")
        .unwrap();
    let embs: &[(i64, Vec<f32>)] = &[
        (1, vec![f32::NAN, 0.0, 1.0]),
        (2, vec![1.0, 0.0, 0.0]),
        (3, vec![0.0, f32::INFINITY, 0.0]),
        (4, vec![0.0, 1.0, 0.0]),
    ];
    for (id, e) in embs {
        db.execute_prepared(
            "INSERT INTO v (id, emb) VALUES (?, ?)",
            vec![Value::Integer(*id), Value::Vector(ArcVec::new(e.clone()))],
        )
        .unwrap();
    }

    let rows = db
        .query("SELECT id FROM v WHERE emb IS NOT NULL ORDER BY emb <-> [1, 0, 0] ASC LIMIT 4")
        .unwrap();
    let ids: Vec<i64> = rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(ids[0], 2, "exact match must be top-1, got {ids:?}");
    assert!(
        !ids[..ids.len() - 1].contains(&1),
        "NaN-distance row must sort last, got {ids:?}"
    );

    let rows = db
        .query("SELECT id FROM v ORDER BY emb <-> [1, 0, 0] ASC LIMIT 1")
        .unwrap();
    assert!(matches!(&rows[0][0], Value::Integer(i) if *i == 2));
}

#[test]
fn drop_table_inside_txn_rejected_table_survives_rollback() {
    let (_d, db) = db();
    db.execute("CREATE TABLE keep (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO keep VALUES (1, 42)").unwrap();

    db.execute("BEGIN").unwrap();
    match db.execute("DROP TABLE keep") {
        Ok(_) => panic!("DROP TABLE inside txn must error"),
        Err(e) => assert!(
            e.to_string().contains("cannot run inside a transaction"),
            "got: {e}"
        ),
    }
    db.execute("ROLLBACK").unwrap();

    let rows = db.query("SELECT v FROM keep").unwrap();
    assert!(matches!(&rows[0][0], Value::Integer(42)));
}

#[test]
fn drop_index_and_alter_inside_txn_rejected() {
    let (_d, db) = db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, emb VECTOR(3))")
        .unwrap();
    db.execute("INSERT INTO t (id, emb) VALUES (1, [1, 0, 0])")
        .unwrap();
    db.execute("CREATE VECTOR INDEX ti ON t(emb)").unwrap();

    db.execute("BEGIN").unwrap();
    for sql in ["DROP INDEX ti", "ALTER TABLE t ADD COLUMN extra INT"] {
        match db.execute(sql) {
            Ok(_) => panic!("{sql} inside txn must error"),
            Err(e) => assert!(
                e.to_string().contains("cannot run inside a transaction"),
                "{sql}: got {e}"
            ),
        }
    }
    db.execute("ROLLBACK").unwrap();

    // index still usable after the rejected DDL + rollback
    let q: Vec<f32> = vec![1.0, 0.0, 0.0];
    let got = db.vector_search("ti", &q, 1).unwrap();
    assert!(got.iter().all(|(rid, _)| *rid == 1), "got {got:?}");
}

#[test]
fn commit_rollback_without_txn_errors() {
    let (_d, db) = db();
    for sql in ["COMMIT", "ROLLBACK"] {
        match db.execute(sql) {
            Ok(_) => panic!("{sql} without txn must error"),
            Err(e) => assert!(
                e.to_string().contains("no transaction is active"),
                "{sql}: got {e}"
            ),
        }
    }

    // normal flow still works
    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("COMMIT").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    db.execute("ROLLBACK").unwrap();
    let rows = db.query("SELECT COUNT(*) FROM t").unwrap();
    assert!(matches!(&rows[0][0], Value::Integer(1)));
}
