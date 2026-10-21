//! Parquet import/export round-trip — `EXPORT TABLE t TO 'x.parquet'` /
//! `IMPORT PARQUET 'x.parquet' INTO t` (src/export/parquet.rs).
//!
//! Covers: every storable column type, NULL cells in nullable columns,
//! name-based column mapping (reordered + subset + extra column), and
//! durability of imported rows across reopen.

use motedb::types::{ArcBytes, ArcString, ArcVec, Timestamp, Value};
use motedb::Database;

const SCHEMA_SRC: &str = "CREATE TABLE src (\
    id INT PRIMARY KEY, ts TIMESTAMP, name TEXT, score FLOAT, \
    active BOOLEAN, emb VECTOR(4), payload BLOB)";

/// One source row exercising every type plus NULLs: every 7th row NULLs the
/// text, every 5th the vector, every 3rd the float, every 11th the blob.
fn insert_row(db: &Database, i: i64) {
    let name = if i % 7 == 0 {
        Value::Null
    } else {
        Value::Text(ArcString::from(format!("name-{i}")))
    };
    let score = if i % 3 == 0 {
        Value::Null
    } else {
        Value::Float(i as f64 * 0.5)
    };
    let active = Value::Bool(i % 2 == 0);
    let emb = if i % 5 == 0 {
        Value::Null
    } else {
        Value::Vector(ArcVec::new(vec![i as f32, 0.25, -1.5, 42.0]))
    };
    let payload = if i % 11 == 0 {
        Value::Null
    } else {
        Value::Blob(ArcBytes::new(vec![0u8, 0xaf, i as u8, 0xff]))
    };
    db.execute_prepared(
        "INSERT INTO src VALUES (?, ?, ?, ?, ?, ?, ?)",
        vec![
            Value::Integer(i),
            Value::Timestamp(Timestamp::from_micros(i * 1_000_000)),
            name,
            score,
            active,
            emb,
            payload,
        ],
    )
    .unwrap();
}

fn one_row(db: &Database, sql: &str) -> Vec<Value> {
    db.query(sql).unwrap().pop().expect("expected one row")
}

#[test]
fn parquet_roundtrip_all_types() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("rt.mote");
    let db = Database::create(&db_path).unwrap();

    db.execute(SCHEMA_SRC).unwrap();
    for i in 0..300i64 {
        insert_row(&db, i);
    }
    db.checkpoint().unwrap();

    let pq = dir.path().join("src.parquet");
    let exported = db
        .execute(&format!("EXPORT TABLE src TO '{}'", pq.display()))
        .unwrap()
        .materialize()
        .unwrap();
    match exported {
        motedb::QueryResult::Definition { message } => {
            assert!(message.contains("300 rows"), "{message}");
        }
        other => panic!("expected Definition, got {other:?}"),
    }
    assert!(pq.exists());
    assert!(pq.metadata().unwrap().len() > 0);

    // Same-schema table: full-width round-trip.
    db.execute(&SCHEMA_SRC.replace("src", "dst")).unwrap();
    let imported = db
        .execute(&format!("IMPORT PARQUET '{}' INTO dst", pq.display()))
        .unwrap()
        .materialize()
        .unwrap();
    match imported {
        motedb::QueryResult::Select { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Integer(300)]], "imported row count");
        }
        other => panic!("expected Select, got {other:?}"),
    }

    assert_eq!(
        db.query("SELECT COUNT(*) FROM dst").unwrap()[0][0],
        Value::Integer(300)
    );

    // Spot-check a fully-populated row (41 is coprime to every NULL cycle).
    let r = one_row(&db, "SELECT * FROM dst WHERE id = 41");
    assert_eq!(r[0], Value::Integer(41));
    assert_eq!(r[1], Value::Timestamp(Timestamp::from_micros(41_000_000)));
    assert_eq!(r[2], Value::Text(ArcString::from("name-41")));
    assert_eq!(r[3], Value::Float(20.5));
    assert_eq!(r[4], Value::Bool(false));
    match &r[5] {
        Value::Vector(v) => assert_eq!(v.0.as_ref(), &[41.0f32, 0.25, -1.5, 42.0]),
        other => panic!("emb: {other:?}"),
    }
    match &r[6] {
        Value::Blob(b) => assert_eq!(b.0.as_ref(), &[0x00u8, 0xaf, 41, 0xff]),
        other => panic!("payload: {other:?}"),
    }

    // NULL cells survive the round-trip as NULLs (not defaults, not zeros).
    for (id, col) in [(7i64, "name"), (15, "emb"), (9, "score"), (11, "payload")] {
        let r = one_row(&db, &format!("SELECT {col} FROM dst WHERE id = {id}"));
        assert_eq!(r[0], Value::Null, "id={id} col={col}");
    }

    // Imported rows are durable: reopen and re-count.
    let db = {
        drop(db);
        Database::open(&db_path).unwrap()
    };
    assert_eq!(
        db.query("SELECT COUNT(*) FROM dst").unwrap()[0][0],
        Value::Integer(300)
    );
    drop(db);
}

#[test]
fn parquet_import_maps_columns_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path().join("map.mote")).unwrap();
    db.execute(SCHEMA_SRC).unwrap();
    for i in 0..50i64 {
        insert_row(&db, i);
    }
    let pq = dir.path().join("map.parquet");
    db.execute(&format!("EXPORT TABLE src TO '{}'", pq.display()))
        .unwrap();

    // Reordered columns, a subset (ts/score/active dropped), and an extra
    // table column the file does not carry → NULL.
    db.execute(
        "CREATE TABLE shuffled (payload BLOB, id INT PRIMARY KEY, extra TEXT, \
         emb VECTOR(4), name TEXT)",
    )
    .unwrap();
    db.execute(&format!("IMPORT PARQUET '{}' INTO shuffled", pq.display()))
        .unwrap()
        .materialize()
        .unwrap();
    assert_eq!(
        db.query("SELECT COUNT(*) FROM shuffled").unwrap()[0][0],
        Value::Integer(50)
    );

    let r = one_row(&db, "SELECT * FROM shuffled WHERE id = 3");
    assert_eq!(r[0], Value::Blob(ArcBytes::new(vec![0, 0xaf, 3, 0xff])));
    assert_eq!(r[1], Value::Integer(3));
    assert_eq!(r[2], Value::Null, "extra column not in the file");
    match &r[3] {
        Value::Vector(v) => assert_eq!(v.0.as_ref(), &[3.0f32, 0.25, -1.5, 42.0]),
        other => panic!("emb: {other:?}"),
    }
    assert_eq!(r[4], Value::Text(ArcString::from("name-3")));
}

#[test]
fn parquet_export_rejects_spatial_columns() {
    // The mapping error path: a SPATIAL column must produce a clear error,
    // not a corrupt file.
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path().join("sp.mote")).unwrap();
    db.execute("CREATE TABLE pts (id INT PRIMARY KEY, loc GEOMETRY)")
        .unwrap();
    db.execute("INSERT INTO pts VALUES (1, POINT(1.0, 2.0))")
        .unwrap();
    let pq = dir.path().join("pts.parquet");
    let err = match db.execute(&format!("EXPORT TABLE pts TO '{}'", pq.display())) {
        Err(e) => e,
        Ok(_) => panic!("expected SPATIAL export to fail"),
    };
    assert!(err.to_string().to_lowercase().contains("spatial"), "{err}");
}

#[test]
fn parquet_import_missing_file_is_a_clear_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path().join("mf.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    let err = match db.execute(&format!(
        "IMPORT PARQUET '{}' INTO t",
        dir.path().join("nope.parquet").display()
    )) {
        Err(e) => e,
        Ok(_) => panic!("expected missing-file import to fail"),
    };
    assert!(!err.to_string().is_empty(), "{err}");
}
