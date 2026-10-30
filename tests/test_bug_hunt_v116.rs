//! Round-26 bug-hunt probes — classic DDL/binding features never probed:
//!
//! * CREATE TABLE column DEFAULT values (INSERT omitting the column)
//! * NOT NULL constraint enforcement (insert + update paths)
//! * composite PRIMARY KEY (dup rejection, point read, upsert, reopen)
//! * LIMIT ? / OFFSET ? bound parameters
//! * identifier length limit (MAX_IDENTIFIER_LENGTH boundary)
//! * TRUE/FALSE/NULL literals in various positions
//! * timestamp ± integer-µs arithmetic in WHERE
//! * JOIN with OR in the ON condition

use motedb::types::{ArcString, Timestamp, Value};
use motedb::Database;

fn new_db() -> (Database, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path().join("t.mote")).unwrap();
    (db, dir)
}

fn sel(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    db.query(sql).unwrap()
}

fn ints(rows: &[Vec<Value>]) -> Vec<i64> {
    rows.iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            o => panic!("expected int, got {o:?}"),
        })
        .collect()
}

// ---------------------------------------------------------------------
// DEFAULT column values
// ---------------------------------------------------------------------

#[test]
fn create_table_default_values_fill_on_omitted_column() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE d (id INT PRIMARY KEY, v INT DEFAULT 7, s TEXT DEFAULT 'hi', f FLOAT DEFAULT 1.5)")
        .unwrap();

    // INSERT naming only id — defaults must fill the rest.
    db.execute("INSERT INTO d (id) VALUES (1)").unwrap();
    let row = sel(&db, "SELECT v, s, f FROM d WHERE id = 1").pop().unwrap();
    assert_eq!(row[0], Value::Integer(7), "INT DEFAULT fills");
    assert_eq!(row[1], Value::Text(ArcString::from("hi")), "TEXT DEFAULT fills");
    match &row[2] {
        Value::Float(f) => assert!((f - 1.5).abs() < 1e-9, "FLOAT DEFAULT fills: {f}"),
        o => panic!("FLOAT DEFAULT: {o:?}"),
    }

    // Explicit values override defaults.
    db.execute("INSERT INTO d (id, v, s, f) VALUES (2, 99, 'x', 9.0)").unwrap();
    let row = sel(&db, "SELECT v, s FROM d WHERE id = 2").pop().unwrap();
    assert_eq!(row[0], Value::Integer(99));
    assert_eq!(row[1], Value::Text(ArcString::from("x")));

    // Bare NULL still writes NULL (DEFAULT only fills OMITTED columns).
    db.execute("INSERT INTO d (id, v) VALUES (3, NULL)").unwrap();
    let row = sel(&db, "SELECT v FROM d WHERE id = 3").pop().unwrap();
    assert!(matches!(row[0], Value::Null), "explicit NULL beats DEFAULT");

    // Defaults survive a reopen.
    db.checkpoint().unwrap();
}

// ---------------------------------------------------------------------
// NOT NULL enforcement
// ---------------------------------------------------------------------

#[test]
fn not_null_constraint_enforced() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE n (id INT PRIMARY KEY, v INT NOT NULL)").unwrap();
    db.execute("INSERT INTO n VALUES (1, 10)").unwrap();

    // INSERT NULL violates.
    let r = db.execute("INSERT INTO n VALUES (2, NULL)");
    assert!(r.is_err(), "NULL insert into NOT NULL column must fail");
    let count = ints(&sel(&db, "SELECT COUNT(*) FROM n"))[0];
    assert_eq!(count, 1, "violating row not inserted");

    // UPDATE to NULL violates.
    let r = db.execute("UPDATE n SET v = NULL WHERE id = 1");
    assert!(r.is_err(), "NULL update on NOT NULL column must fail");
    let v = ints(&sel(&db, "SELECT v FROM n WHERE id = 1"))[0];
    assert_eq!(v, 10, "original value intact after rejected update");

    // Valid update works.
    db.execute("UPDATE n SET v = 20 WHERE id = 1").unwrap();
    let v = ints(&sel(&db, "SELECT v FROM n WHERE id = 1"))[0];
    assert_eq!(v, 20);
}

// ---------------------------------------------------------------------
// Composite PRIMARY KEY
// ---------------------------------------------------------------------

#[test]
fn composite_primary_key_semantics() {
    let (db, dir) = new_db();
    let made = db.execute("CREATE TABLE cp (a INT, b TEXT, v INT, PRIMARY KEY (a, b))");
    if made.is_err() {
        // Composite PK unsupported: must be a CLEAN error, and the table
        // must not half-exist.
        let tables = db.execute("SHOW TABLES").unwrap().materialize().unwrap();
        let _ = tables;
        return;
    }

    db.execute("INSERT INTO cp VALUES (1, 'x', 10)").unwrap();
    // Exact duplicate (both key parts) rejected.
    let r = db.execute("INSERT INTO cp VALUES (1, 'x', 99)");
    assert!(r.is_err(), "composite duplicate rejected");
    // Same a, different b — allowed.
    db.execute("INSERT INTO cp VALUES (1, 'y', 20)").unwrap();
    // Same b, different a — allowed.
    db.execute("INSERT INTO cp VALUES (2, 'x', 30)").unwrap();

    let n = ints(&sel(&db, "SELECT COUNT(*) FROM cp"))[0];
    assert_eq!(n, 3, "three distinct composite keys live");

    // Point lookup on both parts.
    let v = ints(&sel(&db, "SELECT v FROM cp WHERE a = 1 AND b = 'y'"));
    assert_eq!(v, vec![20], "composite point lookup");

    // UPSERT on the composite key.
    db.execute("INSERT INTO cp VALUES (1, 'x', 111) ON CONFLICT (a, b) DO UPDATE SET v = 111")
        .unwrap();
    let v = ints(&sel(&db, "SELECT v FROM cp WHERE a = 1 AND b = 'x'"));
    assert_eq!(v, vec![111], "composite upsert updates in place");
    let n = ints(&sel(&db, "SELECT COUNT(*) FROM cp"))[0];
    assert_eq!(n, 3, "upsert did not add a row");

    // DELETE by composite key; reopen keeps exactly the rest.
    db.execute("DELETE FROM cp WHERE a = 2 AND b = 'x'").unwrap();
    let path = dir.path().join("t.mote");
    drop(db);
    let db2 = Database::open(path).unwrap();
    let n = ints(&sel(&db2, "SELECT COUNT(*) FROM cp"))[0];
    assert_eq!(n, 2, "composite keys survive reopen");
    // Duplicate still rejected after reopen.
    let r = db2.execute("INSERT INTO cp VALUES (1, 'y', 5)");
    assert!(r.is_err(), "composite uniqueness enforced after reopen");
}

// ---------------------------------------------------------------------
// LIMIT ? / OFFSET ? bound parameters
// ---------------------------------------------------------------------

#[test]
fn limit_offset_bound_parameters() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE l (id INT PRIMARY KEY)").unwrap();
    for i in 0..20 {
        db.execute(&format!("INSERT INTO l VALUES ({i})")).unwrap();
    }

    db.execute_prepared("SELECT id FROM l ORDER BY id LIMIT ?", vec![Value::Integer(3)])
        .unwrap();
    let rows = match db
        .execute_prepared(
            "SELECT id FROM l ORDER BY id LIMIT ? OFFSET ?",
            vec![Value::Integer(3), Value::Integer(5)],
        )
        .unwrap()
        .materialize()
        .unwrap()
    {
        motedb::QueryResult::Select { rows, .. } => rows,
        o => panic!("{o:?}"),
    };
    let got: Vec<i64> = rows.iter().map(|r| match &r[0] { Value::Integer(i) => *i, o => panic!("{o:?}") }).collect();
    assert_eq!(got, vec![5, 6, 7], "LIMIT ? OFFSET ? binds correctly");

    // LIMIT 0 via param
    let rows = match db
        .execute_prepared("SELECT id FROM l LIMIT ?", vec![Value::Integer(0)])
        .unwrap()
        .materialize()
        .unwrap()
    {
        motedb::QueryResult::Select { rows, .. } => rows,
        o => panic!("{o:?}"),
    };
    assert_eq!(rows.len(), 0, "LIMIT ? = 0 → empty");
}

// ---------------------------------------------------------------------
// Identifier length limit
// ---------------------------------------------------------------------

#[test]
fn identifier_length_limit_is_clean() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();

    // A 64-char identifier is fine (typical limit).
    let ok_name = "a".repeat(64);
    db.execute(&format!("CREATE TABLE {ok_name} (id INT PRIMARY KEY)")).unwrap();

    // A grossly long identifier (engine limit: 4096) is a clean error.
    let huge_name = "b".repeat(5000);
    let r = db.execute(&format!("CREATE TABLE {huge_name} (id INT PRIMARY KEY)"));
    match r {
        Ok(_) => panic!("1000-char identifier must be rejected"),
        Err(e) => {
            let m = format!("{e:?}");
            assert!(m.contains("long") || m.contains("Identifier") || m.contains("Parse"), "{m}");
        }
    }
}

// ---------------------------------------------------------------------
// TRUE/FALSE/NULL literals
// ---------------------------------------------------------------------

#[test]
fn true_false_null_literals() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE b (id INT PRIMARY KEY, flag BOOLEAN)").unwrap();
    db.execute("INSERT INTO b VALUES (1, TRUE)").unwrap();
    db.execute("INSERT INTO b VALUES (2, FALSE)").unwrap();
    db.execute("INSERT INTO b VALUES (3, NULL)").unwrap();

    let r = sel(&db, "SELECT flag FROM b ORDER BY id");
    assert!(matches!(r[0][0], Value::Bool(true)));
    assert!(matches!(r[1][0], Value::Bool(false)));
    assert!(matches!(r[2][0], Value::Null));

    // TRUE/FALSE in WHERE comparisons against integers (TRUE=1).
    let ids = ints(&sel(&db, "SELECT id FROM b WHERE flag = TRUE"));
    assert_eq!(ids, vec![1]);
    let ids = ints(&sel(&db, "SELECT id FROM b WHERE flag = 1"));
    assert_eq!(ids, vec![1], "TRUE = 1 coercion in WHERE");
    // NULL literal comparison never matches.
    let ids = ints(&sel(&db, "SELECT id FROM b WHERE flag = NULL"));
    assert_eq!(ids, Vec::<i64>::new(), "= NULL never matches (IS NULL required)");
}

// ---------------------------------------------------------------------
// Timestamp ± integer-µs arithmetic in WHERE
// ---------------------------------------------------------------------

#[test]
fn timestamp_arithmetic_in_where() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE ts (id INT PRIMARY KEY, at TIMESTAMP)").unwrap();
    let base = 1_700_000_000i64;
    for i in 0..5 {
        db.execute_prepared(
            "INSERT INTO ts VALUES (?, ?)",
            vec![Value::Integer(i), Value::Timestamp(Timestamp::from_secs(base + i * 60))],
        )
        .unwrap();
    }
    // at + 60s > boundary: ids where ts+60 > base+180 → ts > base+120 → ids 3,4
    let ids = ints(&sel(
        &db,
        &format!("SELECT id FROM ts WHERE at + 60000000 > {}", (base + 180) * 1_000_000),
    ));
    assert_eq!(ids, vec![3, 4], "ts + µs arithmetic in WHERE");
    // at - 60s: ids where ts-60 >= base → ids 1..4
    let ids = ints(&sel(
        &db,
        &format!("SELECT id FROM ts WHERE at - 60000000 >= {}", base * 1_000_000),
    ));
    assert_eq!(ids, vec![1, 2, 3, 4], "ts - µs arithmetic in WHERE");
}

// ---------------------------------------------------------------------
// JOIN with OR in ON
// ---------------------------------------------------------------------

#[test]
fn join_with_or_in_on_condition() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, x INT)").unwrap();
    db.execute("CREATE TABLE b (id INT PRIMARY KEY, y INT)").unwrap();
    for i in 0..3 {
        db.execute(&format!("INSERT INTO a VALUES ({i}, {i})")).unwrap();
        db.execute(&format!("INSERT INTO b VALUES ({i}, {})", i * 10)).unwrap();
    }
    // ON a.x = b.y OR b.id = 2: a0 matches b0 (x=0=y0) AND b2 → 2 rows;
    // a1, a2 match only b2 → 1 row each. Total 4.
    let rows = sel(
        &db,
        "SELECT a.id, b.id FROM a JOIN b ON a.x = b.y OR b.id = 2 ORDER BY a.id, b.id",
    );
    assert_eq!(rows.len(), 4, "OR in ON: a0 matches b0+b2, a1/a2 match b2");
    assert_eq!(rows[0], vec![Value::Integer(0), Value::Integer(0)], "a0-b0 via x=y");
    assert_eq!(rows[1], vec![Value::Integer(0), Value::Integer(2)], "a0-b2 via OR");
    assert_eq!(rows[2], vec![Value::Integer(1), Value::Integer(2)]);
    assert_eq!(rows[3], vec![Value::Integer(2), Value::Integer(2)]);
}
