//! Round-21 bug-hunt probes.
//!
//! Focus areas:
//! * secondary-index consistency under mutation (shadow-map oracle; the
//!   classic silent-wrong-results class: index drift after UPDATE/DELETE/
//!   INSERT/UPSERT/NULL writes)
//! * NULL keys in an indexed column (WHERE cat IS NULL through the index)
//! * date/time function edges on DATE and TIMESTAMP columns
//! * string function edges (REPLACE/REVERSE/REPEAT/INSTR/CONCAT NULL)
//! * ALTER TABLE ADD COLUMN metadata + post-ALTER writes + reopen
//! * window frame edges (ROWS BETWEEN, LAG/LEAD defaults)

use std::collections::BTreeMap;

use motedb::types::{ArcString, Value};
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
// Secondary-index consistency under mutation (shadow-map oracle)
// ---------------------------------------------------------------------

/// The test mirrors the table in a BTreeMap and checks, after every
/// mutation phase, that indexed equality / range queries agree with the
/// mirror. Any drift between index and table is a silent wrong result.
#[test]
fn index_consistency_under_mutation() {
    let (db, dir) = new_db();
    db.execute("CREATE TABLE items (id INT PRIMARY KEY, cat TEXT, val INT)").unwrap();

    let cats = ["a", "b", "c", "d"];
    let mut oracle: BTreeMap<i64, (Option<String>, Option<i64>)> = BTreeMap::new();
    for i in 0..60i64 {
        let cat = cats[i as usize % 4];
        let val = i * 7 % 100;
        db.execute_prepared(
            "INSERT INTO items VALUES (?, ?, ?)",
            vec![
                Value::Integer(i),
                Value::Text(ArcString::from(cat)),
                Value::Integer(val),
            ],
        )
        .unwrap();
        oracle.insert(i, (Some(cat.to_string()), Some(val)));
    }
    // Indexes AFTER data exists (build path).
    db.execute("CREATE INDEX cat_idx ON items(cat)").unwrap();
    db.execute("CREATE INDEX val_idx ON items(val)").unwrap();

    let check = |db: &Database, oracle: &BTreeMap<i64, (Option<String>, Option<i64>)>, phase: &str| {
        // Per-cat equality through cat_idx
        let mut cats_present: Vec<Option<&str>> =
            oracle.values().map(|(c, _)| c.as_deref()).collect();
        cats_present.sort_unstable();
        cats_present.dedup();
        for cat in cats_present {
            let expected: Vec<i64> = oracle
                .iter()
                .filter(|(_, (c, _))| c.as_deref() == cat)
                .map(|(id, _)| *id)
                .collect();
            let got = match cat {
                Some(c) => ints(&sel(
                    db,
                    &format!("SELECT id FROM items WHERE cat = '{}' ORDER BY id", c),
                )),
                None => ints(&sel(db, "SELECT id FROM items WHERE cat IS NULL ORDER BY id")),
            };
            assert_eq!(got, expected, "{phase}: cat={cat:?} equality drifted");
        }
        // Range query through val_idx, several windows
        for (lo, hi) in [(0, 25), (25, 60), (60, 101)] {
            let expected: Vec<i64> = oracle
                .iter()
                .filter(|(_, (_, v))| matches!(v, Some(x) if *x >= lo && *x < hi))
                .map(|(id, _)| *id)
                .collect();
            let got = ints(&sel(
                db,
                &format!("SELECT id FROM items WHERE val >= {} AND val < {} ORDER BY id", lo, hi),
            ));
            assert_eq!(got, expected, "{phase}: val range [{lo},{hi}) drifted");
        }
        // COUNT matches the mirror exactly
        let count = ints(&sel(db, "SELECT COUNT(*) FROM items"))[0];
        assert_eq!(count, oracle.len() as i64, "{phase}: COUNT drifted");
    };

    check(&db, &oracle, "initial");

    // P1: UPDATE 12 rows to new cats + new vals
    for i in (0..60).step_by(5) {
        let new_cat = if i % 10 == 0 { "e" } else { "a" };
        let new_val = i + 200;
        db.execute_prepared(
            "UPDATE items SET cat = ?, val = ? WHERE id = ?",
            vec![
                Value::Text(ArcString::from(new_cat)),
                Value::Integer(new_val),
                Value::Integer(i),
            ],
        )
        .unwrap();
        oracle.insert(i, (Some(new_cat.to_string()), Some(new_val)));
    }
    check(&db, &oracle, "after P1 update");

    // P2: DELETE 10 rows
    for i in (1..60).step_by(6) {
        db.execute(&format!("DELETE FROM items WHERE id = {i}")).unwrap();
        oracle.remove(&i);
    }
    check(&db, &oracle, "after P2 delete");

    // P3: INSERT 15 new rows, some reusing deleted ids
    for k in 0..15i64 {
        let id = if k < 5 { 1 + 6 * k } else { 1000 + k };
        let cat = cats[k as usize % 4];
        let val = k * 13;
        db.execute_prepared(
            "INSERT INTO items VALUES (?, ?, ?)",
            vec![
                Value::Integer(id),
                Value::Text(ArcString::from(cat)),
                Value::Integer(val),
            ],
        )
        .unwrap();
        oracle.insert(id, (Some(cat.to_string()), Some(val)));
    }
    check(&db, &oracle, "after P3 insert");

    // P4: UPSERT changes cat of existing ids (conflict-update path)
    for k in 0..4i64 {
        let id = 1000 + k;
        let cat = "e";
        let val = 500 + k;
        db.execute_prepared(
            "INSERT INTO items VALUES (?, ?, ?) ON CONFLICT (id) DO UPDATE SET cat = ?, val = ?",
            vec![
                Value::Integer(id),
                Value::Text(ArcString::from(cat)),
                Value::Integer(val),
                Value::Text(ArcString::from(cat)),
                Value::Integer(val),
            ],
        )
        .unwrap();
        oracle.insert(id, (Some(cat.to_string()), Some(val)));
    }
    check(&db, &oracle, "after P4 upsert");

    // P5: NULL out cat on some rows — NULL keys in the index
    for id in [3, 7, 11, 1005] {
        db.execute(&format!("UPDATE items SET cat = NULL WHERE id = {id}")).unwrap();
        let val = oracle.get(&id).map(|(_, v)| *v).unwrap_or(None);
        oracle.insert(id, (None, val));
    }
    check(&db, &oracle, "after P5 null-cat");

    // Reopen: indexes must reflect the same state
    let path = dir.path().join("t.mote");
    drop(db);
    let db2 = Database::open(path).unwrap();
    check(&db2, &oracle, "after reopen");
}

// ---------------------------------------------------------------------
// Date/time function edges
// ---------------------------------------------------------------------

#[test]
fn date_function_edges() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE ev (id INT PRIMARY KEY, d DATE, ts TIMESTAMP)").unwrap();
    for (i, (d, ts)) in [
        ("2024-01-31", "2024-01-31 10:20:30"),
        ("2024-02-29", "2024-02-29 23:59:59"),
        ("2023-12-01", "2023-12-01 00:00:00"),
    ]
    .iter()
    .enumerate()
    {
        db.execute_prepared(
            "INSERT INTO ev VALUES (?, ?, ?)",
            vec![
                Value::Integer(i as i64),
                Value::Text(ArcString::from(*d)),
                Value::Text(ArcString::from(*ts)),
            ],
        )
        .unwrap();
    }

    let one = |sql: &str| -> Value { sel(&db, sql).pop().unwrap()[0].clone() };

    // Field extraction on DATE
    assert_eq!(one("SELECT YEAR(d) FROM ev WHERE id = 0"), Value::Integer(2024));
    assert_eq!(one("SELECT MONTH(d) FROM ev WHERE id = 0"), Value::Integer(1));
    assert_eq!(one("SELECT DAY_OF_MONTH(d) FROM ev WHERE id = 0"), Value::Integer(31));
    // Leap day
    assert_eq!(one("SELECT DAY_OF_MONTH(d) FROM ev WHERE id = 1"), Value::Integer(29));
    // Field extraction on TIMESTAMP
    assert_eq!(one("SELECT HOUR(ts) FROM ev WHERE id = 0"), Value::Integer(10));
    assert_eq!(one("SELECT MINUTE(ts) FROM ev WHERE id = 0"), Value::Integer(20));
    assert_eq!(one("SELECT SECOND(ts) FROM ev WHERE id = 0"), Value::Integer(30));

    // DATEDIFF: signed difference in SECONDS (engine convention, now
    // documented). Same-day = 0; Jan 31 vs Dec 1 of the prior year = 61
    // days = 5,270,400 s. 🔴 round-21: DATE-column args used to TypeError
    // → NULL; literal ISO strings are accepted too.
    let diff = match one("SELECT DATEDIFF(d, d) FROM ev WHERE id = 0") {
        Value::Integer(n) => n,
        o => panic!("DATEDIFF same-day expected 0, got {o:?}"),
    };
    assert_eq!(diff, 0);
    match one("SELECT DATEDIFF('2024-01-31', '2023-12-01')") {
        Value::Integer(n) => assert!(n == 5_270_400 || n == -5_270_400, "DATEDIFF ±61 days in seconds, got {n}"),
        o => panic!("DATEDIFF literal args expected int, got {o:?}"),
    }
    // DATE columns accepted directly: Feb 29 2024 − Jan 31 2024 = 29 days
    match one("SELECT DATEDIFF(d, d) FROM ev WHERE id = 1") {
        Value::Integer(n) => assert_eq!(n, 0),
        o => panic!("DATEDIFF on DATE columns, got {o:?}"),
    }
    let cross = sel(&db, "SELECT DATEDIFF((SELECT d FROM ev WHERE id = 1), (SELECT d FROM ev WHERE id = 0))");
    match cross[0][0].clone() {
        Value::Integer(n) => assert!(n == 29 * 86_400 || n == -29 * 86_400, "Feb29−Jan31 = ±29 days in seconds, got {n}"),
        o => panic!("DATEDIFF cross-row DATE, got {o:?}"),
    }

    // Ordering + range on DATE (string literal coercion)
    let ordered = ints(&sel(&db, "SELECT id FROM ev ORDER BY d"));
    assert_eq!(ordered, vec![2, 0, 1], "chronological: Dec 2023, Jan 31, Feb 29");
    let in_2024 = ints(&sel(&db, "SELECT id FROM ev WHERE d >= '2024-01-01' ORDER BY id"));
    assert_eq!(in_2024, vec![0, 1]);

    // DATE round-trip: stored value survives a read
    let stored = sel(&db, "SELECT d FROM ev WHERE id = 0").pop().unwrap()[0].clone();
    assert!(
        matches!(stored, Value::Date(_)) || matches!(stored, Value::Text(_)),
        "DATE column returns {stored:?}"
    );

    // 🔴 round-21 regressions: CAST-to-TEXT used to leak Rust Debug output
    // ("Date(Date { days: 19753 })"); YEAR/MONTH/DAY on DATE returned NULL.
    assert_eq!(one("SELECT CAST(d AS TEXT) FROM ev WHERE id = 0"), Value::Text(ArcString::from("2024-01-31")));
    assert_eq!(
        one("SELECT CAST(ts AS TEXT) FROM ev WHERE id = 0"),
        Value::Text(ArcString::from("2024-01-31 10:20:30")),
    );
    // Fractional seconds survive parse → render round-trip (parse_iso used
    // to drop the ".5" entirely).
    db.execute("INSERT INTO ev VALUES (9, '2024-06-01', '2024-06-01 12:00:00.5')").unwrap();
    assert_eq!(
        one("SELECT CAST(ts AS TEXT) FROM ev WHERE id = 9"),
        Value::Text(ArcString::from("2024-06-01 12:00:00.500000")),
    );
    let us = match one("SELECT ts FROM ev WHERE id = 9") {
        Value::Timestamp(t) => t.as_micros(),
        o => panic!("expected timestamp, got {o:?}"),
    };
    assert_eq!(us % 1_000_000, 500_000, "fractional seconds stored, not truncated");
}

// ---------------------------------------------------------------------
// String function edges
// ---------------------------------------------------------------------

#[test]
fn string_function_edges() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE s (id INT PRIMARY KEY, t TEXT)").unwrap();
    db.execute("INSERT INTO s VALUES (1, 'hello world')").unwrap();

    let one = |sql: &str| -> Value { sel(&db, sql).pop().unwrap()[0].clone() };

    assert_eq!(
        one("SELECT REPLACE(t, 'o', '0') FROM s"),
        Value::Text(ArcString::from("hell0 w0rld")),
    );
    assert_eq!(
        one("SELECT REPLACE(t, 'world', 'W') FROM s"),
        Value::Text(ArcString::from("hello W")),
    );
    assert_eq!(
        one("SELECT REPLACE(t, 'zzz', 'x') FROM s"),
        Value::Text(ArcString::from("hello world")),
        "no-op replace leaves text intact",
    );
    assert_eq!(one("SELECT REVERSE(t) FROM s"), Value::Text(ArcString::from("dlrow olleh")));
    assert_eq!(
        one("SELECT REPEAT('ab', 3) FROM s"),
        Value::Text(ArcString::from("ababab")),
    );
    let pos = match one("SELECT INSTR(t, 'world') FROM s") {
        Value::Integer(p) => p,
        o => panic!("INSTR expected int, got {o:?}"),
    };
    assert_eq!(pos, 7, "'world' starts at 1-based position 7");
    let pos = match one("SELECT INSTR(t, 'zzz') FROM s") {
        Value::Integer(p) => p,
        o => panic!("INSTR miss expected int, got {o:?}"),
    };
    assert_eq!(pos, 0, "INSTR miss is 0");

    // CONCAT ignores NULL operands (PostgreSQL/SQLite>=3.44 semantics —
    // pinned; MySQL propagates NULL instead). The `||` operator is the
    // SQL-standard form and DOES propagate NULL.
    let c = one("SELECT CONCAT(t, NULL) FROM s");
    assert_eq!(c, Value::Text(ArcString::from("hello world")));
    let c = one("SELECT CONCAT('a', 'b', 'c') FROM s");
    assert_eq!(c, Value::Text(ArcString::from("abc")));
    let c = one("SELECT t || NULL FROM s");
    assert!(matches!(c, Value::Null), "|| with NULL → NULL (standard), got {c:?}");
    let c = one("SELECT 'a' || 'b' FROM s");
    assert_eq!(c, Value::Text(ArcString::from("ab")));

    // STR_LEFT / STR_RIGHT bounds
    assert_eq!(one("SELECT STR_LEFT(t, 5) FROM s"), Value::Text(ArcString::from("hello")));
    assert_eq!(one("SELECT STR_RIGHT(t, 5) FROM s"), Value::Text(ArcString::from("world")));
    assert_eq!(
        one("SELECT STR_LEFT(t, 99) FROM s"),
        Value::Text(ArcString::from("hello world")),
        "past-end clamps",
    );
}

// ---------------------------------------------------------------------
// ALTER TABLE ADD COLUMN — metadata + post-ALTER writes + reopen
// ---------------------------------------------------------------------

#[test]
fn alter_add_column_metadata_and_writes() {
    let (db, dir) = new_db();
    db.execute("CREATE TABLE al (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO al VALUES (1, 10)").unwrap();

    db.execute("ALTER TABLE al ADD COLUMN note TEXT").unwrap();

    // New column visible in SELECT * with NULL fill
    let row = sel(&db, "SELECT id, v, note FROM al WHERE id = 1").pop().unwrap();
    assert_eq!(row[0], Value::Integer(1));
    assert_eq!(row[1], Value::Integer(10));
    assert!(matches!(row[2], Value::Null), "fresh column reads NULL");

    // Write through the new column (both update + insert shapes)
    db.execute("UPDATE al SET note = 'first' WHERE id = 1").unwrap();
    db.execute("INSERT INTO al VALUES (2, 20, 'second')").unwrap();
    let notes: Vec<Value> = sel(&db, "SELECT note FROM al ORDER BY id").into_iter().map(|r| r[0].clone()).collect();
    assert_eq!(notes[0], Value::Text(ArcString::from("first")));
    assert_eq!(notes[1], Value::Text(ArcString::from("second")));

    // Aggregate over the new column
    let n = ints(&sel(&db, "SELECT COUNT(note) FROM al"))[0];
    assert_eq!(n, 2, "COUNT over new column");

    // Reopen: schema + data survive
    let path = dir.path().join("t.mote");
    drop(db);
    let db2 = Database::open(path).unwrap();
    let notes: Vec<Value> = sel(&db2, "SELECT note FROM al ORDER BY id").into_iter().map(|r| r[0].clone()).collect();
    assert_eq!(notes.len(), 2);
    assert_eq!(notes[1], Value::Text(ArcString::from("second")));
}

// ---------------------------------------------------------------------
// Window frame edges
// ---------------------------------------------------------------------

#[test]
fn window_frame_rows_between() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE w (id INT PRIMARY KEY, g INT, v INT)").unwrap();
    for i in 0..6 {
        db.execute_prepared(
            "INSERT INTO w VALUES (?, ?, ?)",
            vec![Value::Integer(i), Value::Integer(i / 3), Value::Integer(i + 1)],
        )
        .unwrap();
    }
    // v by id: [1,2,3,4,5,6] — g0 = [1,2,3], g1 = [4,5,6]

    // ROWS 1 PRECEDING .. CURRENT ROW, per partition
    let sums = ints(&sel(
        &db,
        "SELECT SUM(v) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM w ORDER BY id",
    ));
    assert_eq!(sums, vec![1, 3, 5, 4, 9, 11], "prev+current running sums");

    // ROWS CURRENT ROW .. 1 FOLLOWING
    let sums = ints(&sel(
        &db,
        "SELECT SUM(v) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM w ORDER BY id",
    ));
    assert_eq!(sums, vec![3, 5, 3, 9, 11, 6], "current+next sums");

    // LAG with explicit default
    let lags = ints(&sel(
        &db,
        "SELECT LAG(v, 1, 0) OVER (ORDER BY id) FROM w",
    ));
    assert_eq!(lags, vec![0, 1, 2, 3, 4, 5], "LAG default fills the first row");

    // LEAD without default → NULL at the tail
    let leads = sel(&db, "SELECT LEAD(v, 1) OVER (ORDER BY id) FROM w");
    for (i, r) in leads.iter().enumerate() {
        if i == 5 {
            assert!(matches!(r[0], Value::Null), "LEAD tail is NULL");
        } else {
            assert!(matches!(&r[0], Value::Integer(x) if *x == i as i64 + 2));
        }
    }
}

// ---------------------------------------------------------------------
// LEFT/RIGHT as function names — keyword collision probe
// ---------------------------------------------------------------------

#[test]
fn left_right_function_keyword_collision() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE s (id INT PRIMARY KEY, t TEXT)").unwrap();
    db.execute("INSERT INTO s VALUES (1, 'hello')").unwrap();

    // Docs expose STR_LEFT/STR_RIGHT; MySQL/SQLite spellings LEFT()/RIGHT()
    // collide with join keywords. Accept either working or a clean parse
    // error — but never a panic.
    for sql in ["SELECT LEFT(t, 2) FROM s", "SELECT RIGHT(t, 2) FROM s"] {
        match db.query(sql) {
            Ok(rows) => {
                assert_eq!(rows.len(), 1, "{sql}: one row");
            }
            Err(e) => {
                let msg = format!("{e:?}");
                assert!(msg.contains("Parse"), "{sql}: expected parse error, got {msg}");
            }
        }
    }
}
