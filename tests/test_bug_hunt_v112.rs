//! Round-22 bug-hunt probes.
//!
//! Focus areas:
//! * GROUP BY expression (non-column) — does it group correctly or
//!   silently mis-group / error?
//! * COUNT/SUM/AVG(DISTINCT x) semantics
//! * NULLS FIRST / NULLS LAST explicit ordering
//! * identifier case-insensitivity (SELECT ID, FROM T)
//! * string equality vs LIKE case sensitivity
//! * DDL inside a transaction (CREATE TABLE + ROLLBACK → reopen consistency)
//! * LATEST BY on a time-series-ish table
//! * AUTO_INCREMENT behavior after explicit-id insert and delete-max
//! * BETWEEN on TIMESTAMP columns

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

fn setup(db: &Database) {
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, g TEXT, v INT)")
        .unwrap();
    let rows: [(i64, &str, i64); 8] = [
        (0, "a", 5),
        (1, "a", 15),
        (2, "a", 25),
        (3, "b", 3),
        (4, "b", 13),
        (5, "c", 7),
        (6, "c", 7),
        (7, "c", 21),
    ];
    for (id, g, v) in rows {
        db.execute_prepared(
            "INSERT INTO t VALUES (?, ?, ?)",
            vec![
                Value::Integer(id),
                Value::Text(ArcString::from(g)),
                Value::Integer(v),
            ],
        )
        .unwrap();
    }
}

// ---------------------------------------------------------------------
// GROUP BY expression
// ---------------------------------------------------------------------

#[test]
fn group_by_expression_buckets() {
    let (db, _d) = new_db();
    setup(&db);

    // Group by v/10: buckets 0 (5,3,7,7), 1 (15,13), 2 (25,21)
    let r = sel(
        &db,
        "SELECT v / 10, COUNT(*) FROM t GROUP BY v / 10 ORDER BY v / 10",
    );
    assert_eq!(r.len(), 3, "three buckets: {r:?}");
    assert_eq!(r[0][0], Value::Integer(0));
    assert_eq!(r[0][1], Value::Integer(4));
    assert_eq!(r[1][0], Value::Integer(1));
    assert_eq!(r[1][1], Value::Integer(2));
    assert_eq!(r[2][0], Value::Integer(2));
    assert_eq!(r[2][1], Value::Integer(2));

    // Expression grouping must agree with grouping by the projected alias
    let r2 = sel(
        &db,
        "SELECT v / 10 bucket, COUNT(*) c FROM t GROUP BY bucket ORDER BY bucket",
    );
    let r1 = sel(
        &db,
        "SELECT v / 10, COUNT(*) FROM t GROUP BY v / 10 ORDER BY 1",
    );
    for (a, b) in r1.iter().zip(r2.iter()) {
        assert_eq!(a[0], b[0]);
        assert_eq!(a[1], b[1]);
    }

    // CASE expression grouping: low (v<10): ids 0,3,5,6 → 4; high: 4
    let r = sel(
        &db,
        "SELECT CASE WHEN v < 10 THEN 'low' ELSE 'high' END, COUNT(*) FROM t GROUP BY CASE WHEN v < 10 THEN 'low' ELSE 'high' END ORDER BY 1",
    );
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Text(ArcString::from("high")));
    assert_eq!(r[0][1], Value::Integer(4));
    assert_eq!(r[1][0], Value::Text(ArcString::from("low")));
    assert_eq!(r[1][1], Value::Integer(4));
}

// ---------------------------------------------------------------------
// Aggregates over DISTINCT values
// ---------------------------------------------------------------------

#[test]
fn distinct_aggregates() {
    let (db, _d) = new_db();
    setup(&db);

    // v values: 5,15,25,3,13,7,7,21 — distinct: 5,15,25,3,13,7,21 (7 dup once)
    let r = ints(&sel(&db, "SELECT COUNT(DISTINCT v) FROM t"));
    assert_eq!(r[0], 7, "7 distinct values (7 appears twice)");
    let r = ints(&sel(&db, "SELECT SUM(DISTINCT v) FROM t"));
    assert_eq!(r[0], 5 + 15 + 25 + 3 + 13 + 7 + 21, "91");
    let r = sel(&db, "SELECT AVG(DISTINCT v) FROM t");
    match &r[0][0] {
        // 89 = 5+15+25+3+13+7+21 (7 counted once)
        Value::Float(f) => assert!((f - 89.0 / 7.0).abs() < 1e-9, "avg = {f}"),
        o => panic!("AVG(DISTINCT) expected float, got {o:?}"),
    }
    // Non-distinct for contrast (one row, two cells)
    let r = sel(&db, "SELECT COUNT(v), SUM(v) FROM t");
    assert_eq!(r[0][0], Value::Integer(8));
    assert_eq!(r[0][1], Value::Integer(96));

    // DISTINCT aggregate + GROUP BY
    let r = sel(
        &db,
        "SELECT g, COUNT(DISTINCT v) FROM t GROUP BY g ORDER BY g",
    );
    // a: {5,15,25} = 3 distinct; b: {3,13} = 2; c: {7,21} = 2 (7 dup)
    assert_eq!(
        r[0],
        vec![Value::Text(ArcString::from("a")), Value::Integer(3)]
    );
    assert_eq!(
        r[1],
        vec![Value::Text(ArcString::from("b")), Value::Integer(2)]
    );
    assert_eq!(
        r[2],
        vec![Value::Text(ArcString::from("c")), Value::Integer(2)]
    );
}

// ---------------------------------------------------------------------
// NULLS FIRST / NULLS LAST
// ---------------------------------------------------------------------

#[test]
fn nulls_first_last_explicit() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE n (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for (id, v) in [(0, 3), (1, 1), (2, 0), (3, 2)] {
        let val = if v == 0 {
            Value::Null
        } else {
            Value::Integer(v * 10)
        };
        db.execute_prepared("INSERT INTO n VALUES (?, ?)", vec![Value::Integer(id), val])
            .unwrap();
    }
    // ids: 0→30, 1→10, 2→NULL, 3→20

    let asc = ints(&sel(&db, "SELECT id FROM n ORDER BY v ASC"));
    assert_eq!(asc, vec![2, 1, 3, 0], "default ASC: NULL first");
    let desc = ints(&sel(&db, "SELECT id FROM n ORDER BY v DESC"));
    assert_eq!(desc, vec![0, 3, 1, 2], "default DESC: NULL last");

    let nf = ints(&sel(&db, "SELECT id FROM n ORDER BY v ASC NULLS FIRST"));
    assert_eq!(nf, vec![2, 1, 3, 0], "explicit NULLS FIRST");
    let nl = ints(&sel(&db, "SELECT id FROM n ORDER BY v ASC NULLS LAST"));
    assert_eq!(nl, vec![1, 3, 0, 2], "ASC NULLS LAST: 10,20,30,NULL");
    let dnf = ints(&sel(&db, "SELECT id FROM n ORDER BY v DESC NULLS FIRST"));
    assert_eq!(dnf, vec![2, 0, 3, 1], "DESC NULLS FIRST: NULL,30,20,10");
    let dnl = ints(&sel(&db, "SELECT id FROM n ORDER BY v DESC NULLS LAST"));
    assert_eq!(dnl, vec![0, 3, 1, 2], "explicit NULLS LAST");
}

// ---------------------------------------------------------------------
// Identifier case-insensitivity
// ---------------------------------------------------------------------

#[test]
fn identifier_case_insensitivity() {
    let (db, _d) = new_db();
    setup(&db);

    // Column names are case-INSENSITIVE (unquoted identifiers fold)
    let r = ints(&sel(&db, "SELECT ID FROM t WHERE V = 5"));
    assert_eq!(r, vec![0], "column names match case-insensitively");
    // Table names are case-SENSITIVE (storage keys are exact; folding is a
    // breaking change deferred — pinned so a future change is deliberate).
    let err = db.query("SELECT id FROM T WHERE id = 3").unwrap_err();
    assert!(
        format!("{err:?}").contains("not found"),
        "FROM T with table t: expect TableNotFound, got {err:?}",
    );
    // Keyword case
    let r = ints(&sel(&db, "select id from t where id = 1"));
    assert_eq!(r, vec![1]);
    // Mixed alias case
    let r = sel(&db, "SELECT V AS BIG_V FROM t WHERE ID = 0");
    assert_eq!(r[0][0], Value::Integer(5));
    // Aggregate case
    let r = ints(&sel(&db, "select count(*) from t"));
    assert_eq!(r[0], 8);
    // Function case
    let r = sel(&db, "SELECT UPPER(g) FROM t WHERE id = 0");
    assert_eq!(r[0][0], Value::Text(ArcString::from("A")));
}

// ---------------------------------------------------------------------
// Case sensitivity: string equality vs LIKE
// ---------------------------------------------------------------------

#[test]
fn string_equality_vs_like_case_sensitivity() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE cs (id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    for (i, s) in ["Apple", "apple", "APPLE", "banana"].iter().enumerate() {
        db.execute_prepared(
            "INSERT INTO cs VALUES (?, ?)",
            vec![Value::Integer(i as i64), Value::Text(ArcString::from(*s))],
        )
        .unwrap();
    }

    // = is case-SENSITIVE (Postgres semantics)
    let r = ints(&sel(&db, "SELECT id FROM cs WHERE s = 'apple'"));
    assert_eq!(r, vec![1], "equality is case-sensitive");

    // LIKE is case-SENSITIVE too (PostgreSQL semantics — pinned round-22;
    // SQLite/MySQL fold ASCII case in LIKE, MoteDB does not)
    let r = ints(&sel(&db, "SELECT id FROM cs WHERE s LIKE 'apple'"));
    assert_eq!(r, vec![1], "LIKE is case-sensitive");
    let r = ints(&sel(&db, "SELECT id FROM cs WHERE s LIKE 'A%'"));
    assert_eq!(
        r,
        vec![0, 2],
        "LIKE 'A%' matches Apple and APPLE (capital A)"
    );
    let r = ints(&sel(&db, "SELECT id FROM cs WHERE s LIKE 'appl%'"));
    assert_eq!(r, vec![1], "LIKE 'appl%' matches only lowercase 'apple'");
    // Wildcards stay case-sensitive too: only APPLE contains capital P
    let r = ints(&sel(&db, "SELECT id FROM cs WHERE s LIKE '%P%'"));
    assert_eq!(r, vec![2], "%P% matches only APPLE");
}

// ---------------------------------------------------------------------
// DDL inside a transaction
// ---------------------------------------------------------------------

#[test]
fn ddl_inside_transaction_rollback() {
    let (db, dir) = new_db();
    db.execute("CREATE TABLE base (id INT PRIMARY KEY)")
        .unwrap();
    db.execute("INSERT INTO base VALUES (1)").unwrap();

    let tx = db.begin_transaction().unwrap();
    db.execute("CREATE TABLE rolled (id INT PRIMARY KEY)")
        .unwrap();
    db.execute("INSERT INTO rolled VALUES (99)").unwrap();
    db.execute("INSERT INTO base VALUES (2)").unwrap();
    db.rollback_transaction(tx).unwrap();

    // base row 2 must be gone (normal txn rollback)
    let r = ints(&sel(&db, "SELECT id FROM base ORDER BY id"));
    assert_eq!(r, vec![1], "txn INSERT rolled back");

    // The table created inside the rolled-back txn: either fully gone
    // (transactional DDL) or fully present-but-empty (non-transactional DDL)
    // — anything else is corruption. Probe and pin.
    let table_state = db.query("SELECT COUNT(*) FROM rolled");
    match table_state {
        Ok(rows) => {
            let n = match &rows[0][0] {
                Value::Integer(n) => *n,
                o => panic!("{o:?}"),
            };
            assert_eq!(n, 0, "table survives but its txn-inserted row must not");
            let tables = db.query("SHOW TABLES").unwrap();
            assert!(
                tables
                    .iter()
                    .flatten()
                    .any(|v| matches!(v, Value::Text(s) if s.as_str() == "rolled")),
                "surviving table is listed in SHOW TABLES",
            );
        }
        Err(_) => {
            // Transactional DDL: table is gone entirely
            let tables = db.query("SHOW TABLES").unwrap();
            assert!(
                !tables
                    .iter()
                    .flatten()
                    .any(|v| matches!(v, Value::Text(s) if s.as_str() == "rolled")),
                "rolled-back table must not appear in SHOW TABLES",
            );
        }
    }

    // Reopen: same visible state (no half-created table files crashing open)
    let path = dir.path().join("t.mote");
    drop(db);
    let db2 = Database::open(path).unwrap();
    let r = ints(&sel(&db2, "SELECT id FROM base ORDER BY id"));
    assert_eq!(r, vec![1], "base survives reopen");
    // rolled's reopen state matches its pre-reopen state
    match db2.query("SELECT COUNT(*) FROM rolled") {
        Ok(rows) => {
            let n = match &rows[0][0] {
                Value::Integer(n) => *n,
                o => panic!("{o:?}"),
            };
            assert_eq!(n, 0);
        }
        Err(e) => assert!(
            matches!(e, motedb::MoteDBError::TableNotFound(_))
                || format!("{e:?}").contains("not found"),
            "reopen: rolled cleanly absent, got {e:?}",
        ),
    }
}

// ---------------------------------------------------------------------
// AUTO_INCREMENT gaps and explicit ids
// ---------------------------------------------------------------------

#[test]
fn auto_increment_after_explicit_and_delete() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE ai (id INT PRIMARY KEY AUTO_INCREMENT, v INT)")
        .unwrap();
    db.execute("INSERT INTO ai (v) VALUES (10)").unwrap();
    db.execute("INSERT INTO ai (v) VALUES (20)").unwrap();
    // Explicit id far above the counter
    db.execute("INSERT INTO ai VALUES (100, 30)").unwrap();
    db.execute("INSERT INTO ai (v) VALUES (40)").unwrap();
    let ids = ints(&sel(&db, "SELECT id FROM ai ORDER BY id"));
    // Next auto id must exceed the explicit 100 (MySQL behavior: counter
    // jumps past explicit max)
    assert_eq!(ids.len(), 4);
    assert_eq!(ids[0], 1);
    assert_eq!(ids[1], 2);
    assert_eq!(ids[2], 100);
    assert!(
        ids[3] > 100,
        "counter advanced past explicit id 100: {ids:?}"
    );

    // Delete the max row — counter must NOT regress
    db.execute("DELETE FROM ai WHERE id > 99").unwrap();
    db.execute("INSERT INTO ai (v) VALUES (50)").unwrap();
    let ids = ints(&sel(&db, "SELECT id FROM ai ORDER BY id"));
    let last = *ids.last().unwrap();
    let prev = ids[ids.len() - 2];
    assert!(last > prev, "counter never regresses after delete: {ids:?}");
    assert!(
        last > 100,
        "counter stays past 100 after deleting the max row: {ids:?}"
    );
}

// ---------------------------------------------------------------------
// BETWEEN on TIMESTAMP
// ---------------------------------------------------------------------

#[test]
fn between_on_timestamps() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE ts (id INT PRIMARY KEY, at TIMESTAMP)")
        .unwrap();
    let base = 1_700_000_000i64; // seconds
    for i in 0..5 {
        db.execute_prepared(
            "INSERT INTO ts VALUES (?, ?)",
            vec![
                Value::Integer(i),
                Value::Timestamp(Timestamp::from_secs(base + i * 3600)),
            ],
        )
        .unwrap();
    }
    // base = 2023-11-14 22:13:20 UTC, +1h per id. Strict window
    // 22:20:00..23:00:00 contains NO row (id0 is 22:13:20, id1 is 23:13:20)
    // — an empty result proves both bounds coerce correctly rather than
    // something silently matching.
    let r = ints(&sel(
        &db,
        "SELECT id FROM ts WHERE at BETWEEN '2023-11-14 22:20:00' AND '2023-11-14 23:00:00'",
    ));
    assert_eq!(
        r,
        Vec::<i64>::new(),
        "window between two stored rows is empty"
    );

    // Inclusive both ends: subquery bounds spanning all rows
    let r2 = ints(&sel(
        &db,
        "SELECT id FROM ts WHERE at BETWEEN (SELECT at FROM ts WHERE id = 0) AND (SELECT at FROM ts WHERE id = 4)",
    ));
    assert_eq!(
        r2,
        vec![0, 1, 2, 3, 4],
        "BETWEEN bounds via subquery are inclusive"
    );

    // A one-row window around id 1's exact value
    let r3 = ints(&sel(
        &db,
        "SELECT id FROM ts WHERE at BETWEEN '2023-11-14 23:13:19' AND '2023-11-14 23:13:21'",
    ));
    assert_eq!(r3, vec![1], "tight literal window hits exactly id 1");
}
