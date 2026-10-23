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
    let _ = db.close();
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
    let _ = db.close();
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
    let _ = db.close();
}

/// 🔒 Regression (2026-10-24 test campaign): `SELECT *` with no ORDER BY
/// takes the VEC M4 star path, whose DATE materializer read the raw I64
/// batch cell without the `valid` bitmap — every NULL date came back as
/// 1970-01-01 (epoch 0). Parameterized middle-of-batch NULLs, verified
/// across executemany's batched-INSERT shape.
#[test]
fn null_date_survives_star_scan() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Database::create(dir.path().join("nd.mote")).unwrap();
    db.execute("CREATE TABLE ev (id INT PRIMARY KEY AUTO_INCREMENT, d DATE, v FLOAT, i INT)")
        .unwrap();
    db.execute_prepared_many(
        "INSERT INTO ev (d, v, i) VALUES (?, ?, ?)",
        vec![
            vec![
                motedb::types::Value::text("2024-01-15".to_string()),
                motedb::types::Value::Float(1.5),
                motedb::types::Value::Integer(1),
            ],
            vec![
                motedb::types::Value::Null,
                motedb::types::Value::Null,
                motedb::types::Value::Null,
            ],
            vec![
                motedb::types::Value::text("2024-03-01".to_string()),
                motedb::types::Value::Float(3.5),
                motedb::types::Value::Integer(3),
            ],
        ],
    )
    .unwrap();
    let r = db
        .execute("SELECT * FROM ev")
        .unwrap()
        .materialize()
        .unwrap();
    let (_c, rows) = r.select_rows().unwrap();
    assert_eq!(
        rows[1][1],
        motedb::types::Value::Null,
        "NULL date must stay NULL on the star path"
    );
    assert_eq!(
        rows[0][1],
        motedb::types::Value::Date(motedb::types::Date::parse("2024-01-15").unwrap())
    );
    // Parquet export must not turn the NULL into epoch-day either.
    let p = dir.path().join("nd.parquet");
    db.execute(&format!("EXPORT TABLE ev TO '{}'", p.display()))
        .unwrap();
    db.execute("CREATE TABLE ev2 (id INT PRIMARY KEY, d DATE, v FLOAT, i INT)")
        .unwrap();
    db.execute(&format!("IMPORT PARQUET '{}' INTO ev2", p.display()))
        .unwrap();
    let r3 = db
        .execute("SELECT d FROM ev2 WHERE id = 2")
        .unwrap()
        .materialize()
        .unwrap();
    assert_eq!(
        r3.select_rows().unwrap().1[0][0],
        motedb::types::Value::Null
    );
}
