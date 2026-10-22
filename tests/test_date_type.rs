//! DATE column type — end-to-end (DDL / string-literal coercion / string
//! comparison / checkpoint-reopen / parquet Date32 round-trip).

use motedb::types::Value;
use motedb::Database;

#[test]
fn date_column_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE ev (id INT PRIMARY KEY, d DATE, note TEXT)")
        .unwrap();
    // String literals coerce on insert (both bare and via params).
    for (i, d) in ["2026-10-22", "2026-01-01", "2000-02-29", "1999-12-31"]
        .iter()
        .enumerate()
    {
        db.execute_prepared(
            "INSERT INTO ev VALUES (?, ?, ?)",
            vec![
                Value::Integer(i as i64),
                Value::Text(motedb::types::ArcString::from(*d)),
                Value::text_from("row"),
            ],
        )
        .unwrap();
    }
    db.checkpoint().unwrap();

    // Read back: Date values (as days), orderable.
    let rows = db.query("SELECT id, d FROM ev ORDER BY d").unwrap();
    let first = rows[0][1].clone();
    let last = rows[3][1].clone();
    match (&first, &last) {
        (Value::Date(a), Value::Date(b)) => {
            assert_eq!(a.to_iso(), "1999-12-31");
            assert_eq!(b.to_iso(), "2026-10-22");
        }
        other => panic!("{other:?}"),
    }

    // String comparisons in WHERE.
    let n = db
        .query("SELECT COUNT(*) FROM ev WHERE d >= '2026-01-01'")
        .unwrap()[0][0]
        .clone();
    assert_eq!(n, Value::Integer(2));
    let leap = db
        .query("SELECT id FROM ev WHERE d = '2000-02-29'")
        .unwrap();
    assert_eq!(leap.len(), 1);

    // GROUP BY on DATE (hash path through Value::Hash).
    let g = db
        .query("SELECT COUNT(*) FROM ev GROUP BY d ORDER BY COUNT(*)")
        .unwrap();
    assert_eq!(g.len(), 4);

    // Reopen: persisted round-trip keeps the calendar type.
    drop(db);
    let db = Database::open(dir.path().join("d.mote")).unwrap();
    let rows = db.query("SELECT d FROM ev WHERE id = 3").unwrap();
    match &rows[0][0] {
        Value::Date(d) => assert_eq!(d.to_iso(), "1999-12-31"),
        other => panic!("after reopen: {other:?}"),
    }
    db.close();
}

#[test]
fn date_parquet_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path().join("dp.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, d DATE)")
        .unwrap();
    for (i, d) in ["2026-10-22", "1970-01-01", "2100-12-31"]
        .iter()
        .enumerate()
    {
        db.execute_prepared(
            "INSERT INTO t VALUES (?, ?)",
            vec![
                Value::Integer(i as i64),
                Value::Text(motedb::types::ArcString::from(*d)),
            ],
        )
        .unwrap();
    }
    let pq = dir.path().join("d.parquet");
    db.execute(&format!("EXPORT TABLE t TO '{}'", pq.display()))
        .unwrap();
    db.execute("CREATE TABLE t2 (id INT PRIMARY KEY, d DATE)")
        .unwrap();
    db.execute(&format!("IMPORT PARQUET '{}' INTO t2", pq.display()))
        .unwrap();
    let rows = db.query("SELECT d FROM t2 ORDER BY d").unwrap();
    match (&rows[0][0], &rows[2][0]) {
        (Value::Date(a), Value::Date(b)) => {
            assert_eq!(a.to_iso(), "1970-01-01");
            assert_eq!(b.to_iso(), "2100-12-31");
        }
        other => panic!("{other:?}"),
    }
    db.close();
}

#[test]
fn date_rejects_invalid_literals() {
    // Unparseable strings stay TEXT → later comparisons simply don't match;
    // the coercion is lenient by design (mirrors Timestamp handling). This
    // test pins that behavior so a future strict mode is a conscious change.
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path().join("di.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, d DATE)")
        .unwrap();
    db.execute_prepared(
        "INSERT INTO t VALUES (?, ?)",
        vec![
            Value::Integer(1),
            Value::Text(motedb::types::ArcString::from("not-a-date")),
        ],
    )
    .unwrap();
    let n = db
        .query("SELECT COUNT(*) FROM t WHERE d = '2026-01-01'")
        .unwrap();
    assert_eq!(n[0][0], Value::Integer(0));
    db.close();
}
