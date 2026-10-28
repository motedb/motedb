//! Round-20 bug-hunt probes — SQL edge semantics across less-trodden paths.
//!
//! Focus areas:
//! * LIMIT / OFFSET edges (zero, beyond-end, huge)
//! * ORDER BY NULL placement consistency across query shapes (plain,
//!   DISTINCT, GROUP BY, UNION, top-K LIMIT pushdown)
//! * scalar-function NULL propagation (LENGTH/UPPER/LOWER/TRIM/SUBSTRING)
//! * SUBSTRING out-of-range / non-positive start
//! * LIKE ESCAPE edges (escaped %, escaped _, trailing escape char)
//! * UPDATE multi-assignment old-value semantics (SET a = a + 1, b = a)
//! * integer overflow demotes to FLOAT instead of panicking
//! * NOT IN with NULL in the set (three-valued logic)
//! * Parquet DATE round-trip (the one column type the existing
//!   test_parquet_roundtrip.rs schema omits)

use motedb::types::{ArcString, Date, Value};
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
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, s TEXT)").unwrap();
    for i in 0..10 {
        let v = if i % 4 == 0 { Value::Null } else { Value::Integer(i * 10) };
        db.execute_prepared(
            "INSERT INTO t VALUES (?, ?, ?)",
            vec![Value::Integer(i), v, Value::Text(ArcString::from(format!("s{i}")))],
        )
        .unwrap();
    }
}

// ---------------------------------------------------------------------
// Implicit (no-AS) select-list aliases — round-20 fix
// ---------------------------------------------------------------------

/// Before round-20, any alias without the AS keyword died with the
/// misleading ParseError "Multiple statements are not supported"
/// (`SELECT v x`, `SELECT COUNT(*) c`, `SELECT v + 1 total` ...).
#[test]
fn implicit_select_alias_without_as() {
    let (db, _d) = new_db();
    setup(&db);

    // Column reference with implicit alias
    let rows = sel(&db, "SELECT id n FROM t ORDER BY n LIMIT 1");
    assert_eq!(rows[0][0], Value::Integer(0));

    // Aggregate with implicit alias, used by HAVING/ORDER BY
    let rows = sel(&db, "SELECT v grp, COUNT(*) c FROM t GROUP BY v ORDER BY c DESC, grp ASC");
    let first = &rows[0];
    let c = match &first[1] {
        Value::Integer(n) => *n,
        o => panic!("expected int count, got {o:?}"),
    };
    assert_eq!(c, 3, "NULL group has 3 members (ids 0, 4, 8)");
    assert!(matches!(first[0], Value::Null));

    // Arithmetic expression with implicit alias
    let rows = sel(&db, "SELECT v + 1 shifted FROM t WHERE id = 1");
    assert_eq!(rows[0][0], Value::Integer(11));

    // Function call with implicit alias
    let rows = sel(&db, "SELECT LENGTH(s) l FROM t WHERE id = 1");
    assert_eq!(rows[0][0], Value::Integer(2));

    // Mixed AS and implicit in one list
    let rows = sel(&db, "SELECT id AS i, v val FROM t WHERE id = 1");
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[0][1], Value::Integer(10));

    // The alias must not swallow clause keywords (they are keyword tokens,
    // not identifiers): these still parse as clauses, not aliases.
    let rows = sel(&db, "SELECT id FROM t WHERE id < 2 ORDER BY id DESC");
    assert_eq!(ints(&rows), vec![1, 0]);
    let rows = sel(&db, "SELECT 1 x UNION SELECT 2 ORDER BY 1");
    assert_eq!(rows.len(), 2);
}

// ---------------------------------------------------------------------
// Unknown column in WHERE must ERROR — round-20 fix
// ---------------------------------------------------------------------

/// Per-row filter paths swallow evaluation errors by design (dynamic
/// fallback), which turned column typos into silent empty results:
/// `WHERE usr_id = 5` returned 0 rows with no error on SELECT / COUNT /
/// UPDATE / DELETE alike. Round-20 pins the schema binding upfront.
#[test]
fn where_unknown_column_errors_across_statement_types() {
    let (db, _d) = new_db();
    setup(&db);

    for sql in [
        "SELECT id FROM t WHERE zzz > 2",
        "SELECT COUNT(*) FROM t WHERE zzz > 2",
        "SELECT id FROM t WHERE id = 1 AND zzz = 2",
        "SELECT id FROM t WHERE zzz IN (1, 2)",
        "SELECT id FROM t WHERE zzz LIKE 'x%'",
    ] {
        let err = db.query(sql).expect_err(&format!("must error: {sql}"));
        assert!(
            matches!(err, motedb::MoteDBError::ColumnNotFound(ref c) if c == "zzz"),
            "{sql}: expected ColumnNotFound(zzz), got {err:?}",
        );
    }
    for sql in [
        "UPDATE t SET v = 9 WHERE zzz = 1",
        "DELETE FROM t WHERE zzz = 1",
    ] {
        let r = db.execute(sql);
        let err = r.err().unwrap_or_else(|| panic!("must error: {sql}"));
        assert!(
            matches!(err, motedb::MoteDBError::ColumnNotFound(ref c) if c == "zzz"),
            "{sql}: expected ColumnNotFound(zzz), got {err:?}",
        );
    }

    // Sanity after the failed statements: data untouched, valid WHERE works.
    assert_eq!(sel(&db, "SELECT id FROM t WHERE v > 20").len(), 5);
    assert_eq!(sel(&db, "SELECT COUNT(*) FROM t").len(), 1);
}

/// WHERE referencing a SELECT-list alias errors (aliases bind AFTER WHERE
/// in standard SQL) — pins the boundary of the round-20 validator.
#[test]
fn where_alias_reference_errors_like_standard_sql() {
    let (db, _d) = new_db();
    setup(&db);
    let err = db
        .query("SELECT v AS x FROM t WHERE x > 2")
        .expect_err("alias not visible in WHERE");
    assert!(matches!(err, motedb::MoteDBError::ColumnNotFound(_)), "{err:?}");
}

/// The validator must not break subqueries: IN-subqueries and correlated
/// EXISTS legally reference columns the outer walk knows nothing about.
#[test]
fn where_subquery_and_correlated_exists_unaffected() {
    let (db, _d) = new_db();
    setup(&db);
    assert_eq!(
        ints(&sel(&db, "SELECT id FROM t WHERE v IN (SELECT v FROM t WHERE v < 30)")),
        vec![1, 2],
    );
    // Correlated: subquery's WHERE references the OUTER table t. NULL-v
    // rows (ids 0, 4, 8) never match (NULL = x is never TRUE) → 7 rows.
    assert_eq!(
        sel(&db, "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM t t2 WHERE t2.v = t.v)").len(),
        7,
    );
}

// ---------------------------------------------------------------------
// LIMIT / OFFSET edges
// ---------------------------------------------------------------------

#[test]
fn limit_zero_offset_beyond_and_slice() {
    let (db, _d) = new_db();
    setup(&db);

    // LIMIT 0 → empty, not "no limit"
    assert_eq!(sel(&db, "SELECT id FROM t LIMIT 0").len(), 0);

    // OFFSET beyond the end → empty
    assert_eq!(sel(&db, "SELECT id FROM t OFFSET 100").len(), 0);

    // LIMIT + OFFSET slice: rows 2..7
    assert_eq!(
        ints(&sel(&db, "SELECT id FROM t ORDER BY id LIMIT 5 OFFSET 2")),
        vec![2, 3, 4, 5, 6],
    );

    // huge limit capped at table size
    assert_eq!(sel(&db, "SELECT id FROM t LIMIT 1000000").len(), 10);

    // OFFSET exactly at the boundary → empty
    assert_eq!(sel(&db, "SELECT id FROM t OFFSET 10").len(), 0);
}

// ---------------------------------------------------------------------
// ORDER BY NULL placement consistency
// ---------------------------------------------------------------------

/// NULL v values sit at ids 0, 4, 8. ASC → NULLs first; DESC → NULLs last.
/// Every query shape must agree with the plain shape.
#[test]
fn order_by_null_placement_consistent_across_shapes() {
    let (db, _d) = new_db();
    setup(&db);

    let plain_asc = ints(&sel(&db, "SELECT id FROM t ORDER BY v ASC, id ASC"));
    assert_eq!(plain_asc, vec![0, 4, 8, 1, 2, 3, 5, 6, 7, 9], "ASC puts NULL first");

    let plain_desc = ints(&sel(&db, "SELECT id FROM t ORDER BY v DESC, id DESC"));
    assert_eq!(plain_desc, vec![9, 7, 6, 5, 3, 2, 1, 8, 4, 0], "DESC puts NULL last");

    // DISTINCT over v: NULL collapses to one row and still sorts first ASC
    let d = sel(&db, "SELECT DISTINCT v FROM t ORDER BY v ASC");
    assert!(matches!(d[0][0], Value::Null), "DISTINCT+ORDER BY: NULL first");

    // GROUP BY + ORDER BY on the group key
    let g = sel(&db, "SELECT v, COUNT(*) c FROM t GROUP BY v ORDER BY v ASC");
    assert!(matches!(g[0][0], Value::Null), "GROUP BY+ORDER BY: NULL first");

    // UNION + ORDER BY (the merged path)
    let u = sel(
        &db,
        "SELECT v FROM t WHERE id < 5 UNION SELECT v FROM t WHERE id >= 5 ORDER BY v ASC",
    );
    assert!(matches!(u[0][0], Value::Null), "UNION+ORDER BY: NULL first");

    // top-K: LIMIT 3 with NULLs-first ASC must return the three NULL rows
    let topk = ints(&sel(&db, "SELECT id FROM t ORDER BY v ASC, id ASC LIMIT 3"));
    assert_eq!(topk, vec![0, 4, 8], "top-K keeps NULL-first order");
}

// ---------------------------------------------------------------------
// Scalar function NULL propagation + SUBSTRING edges
// ---------------------------------------------------------------------

#[test]
fn scalar_functions_propagate_null() {
    let (db, _d) = new_db();
    setup(&db);

    // v is NULL at id=0: NULL in → NULL out for every string function.
    for f in ["LENGTH(v)", "UPPER(v)", "LOWER(v)", "TRIM(v)", "SUBSTRING(v, 1, 2)"] {
        let sql = format!("SELECT {f} FROM t WHERE id = 0");
        let rows = sel(&db, &sql);
        assert_eq!(rows.len(), 1, "{f}: one row");
        assert!(matches!(rows[0][0], Value::Null), "{f}: NULL in → NULL out");
    }
}

#[test]
fn substring_out_of_range_and_edges() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE w (id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO w VALUES (1, 'abcdef')").unwrap();

    let one = |sql: &str| -> Value { sel(&db, sql).pop().unwrap()[0].clone() };

    // start beyond length → empty string (not NULL, not error)
    assert_eq!(one("SELECT SUBSTRING(s, 10, 2) FROM w"), Value::Text(ArcString::from("")));
    // length runs past the end → clamp
    assert_eq!(one("SELECT SUBSTRING(s, 4, 99) FROM w"), Value::Text(ArcString::from("def")));
    // zero length → empty
    assert_eq!(one("SELECT SUBSTRING(s, 2, 0) FROM w"), Value::Text(ArcString::from("")));
    // two-arg form to the end
    assert_eq!(one("SELECT SUBSTRING(s, 4) FROM w"), Value::Text(ArcString::from("def")));
    // SQL-standard FROM/FOR spelling parses (round-12 fix) and agrees
    assert_eq!(
        one("SELECT SUBSTRING(s FROM 2 FOR 3) FROM w"),
        Value::Text(ArcString::from("bcd")),
    );
}

// ---------------------------------------------------------------------
// LIKE ESCAPE edges
// ---------------------------------------------------------------------

#[test]
fn like_escape_edges() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE lk (id INT PRIMARY KEY, s TEXT)").unwrap();
    for (i, s) in ["100%", "a_b", "z_y", "x!y"].iter().enumerate() {
        db.execute_prepared(
            "INSERT INTO lk VALUES (?, ?)",
            vec![Value::Integer(i as i64), Value::Text(ArcString::from(*s))],
        )
        .unwrap();
    }

    // escaped % matches a literal % and nothing else
    let hits = sel(&db, "SELECT id FROM lk WHERE s LIKE '%!%' ESCAPE '!'");
    assert_eq!(ints(&hits), vec![0]);

    // escaped _ matches a literal underscore, not any char
    let hits = sel(&db, "SELECT id FROM lk WHERE s LIKE 'a!_b' ESCAPE '!'");
    assert_eq!(ints(&hits), vec![1]);

    // plain _ still means any single char alongside an escaped literal:
    // `_!_y` = <any char><literal _>y → matches 'z_y' but not 'x!y'
    let hits = sel(&db, "SELECT id FROM lk WHERE s LIKE '_!_y' ESCAPE '!'");
    assert_eq!(ints(&hits), vec![2]);

    // pattern that is only the escape char → matches a literal '!'
    let hits = sel(&db, "SELECT id FROM lk WHERE s LIKE '!' ESCAPE '!'");
    assert_eq!(ints(&hits), Vec::<i64>::new(), "'!' alone must not match 'x!y' (2 chars)");
}

// ---------------------------------------------------------------------
// UPDATE multi-assignment old-value semantics
// ---------------------------------------------------------------------

#[test]
fn update_multi_set_uses_old_values() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE u (a INT PRIMARY KEY, b INT)").unwrap();
    db.execute("INSERT INTO u VALUES (10, 100)").unwrap();

    // SQL standard: every SET expression sees the OLD row. b must get 10,
    // not 11 (which would mean a's NEW value leaked into b's expression).
    db.execute("UPDATE u SET a = a + 1, b = a").unwrap();
    let row = sel(&db, "SELECT a, b FROM u").pop().unwrap();
    assert_eq!(row[0], Value::Integer(11));
    assert_eq!(row[1], Value::Integer(10), "b = a must read the OLD a");

    // Both sides read OLD values (no sequential application):
    // old a = 11, old b = 10 → new a = 10, new b = 11.
    db.execute("UPDATE u SET a = b, b = a").unwrap();
    let row = sel(&db, "SELECT a, b FROM u").pop().unwrap();
    assert_eq!(row[0], Value::Integer(10), "a = b (old b = 10)");
    assert_eq!(row[1], Value::Integer(11), "b = a (old a = 11) — both read OLD values");
}

#[test]
fn integer_overflow_demotes_to_float() {
    let (db, _d) = new_db();
    let one = |sql: &str| -> Value { sel(&db, sql).pop().unwrap()[0].clone() };
    let max = i64::MAX as f64;
    assert_eq!(
        one("SELECT 9223372036854775807 + 1"),
        Value::Float(max + 1.0),
        "overflow demotes to float, not a panic",
    );
    assert_eq!(
        one("SELECT -9223372036854775808 - 1"),
        Value::Float(-max - 2.0),
    );
    assert_eq!(
        one("SELECT 9223372036854775807 * 2"),
        Value::Float(max * 2.0),
    );
}

// ---------------------------------------------------------------------
// NOT IN with NULL in the set — three-valued logic
// ---------------------------------------------------------------------

#[test]
fn not_in_with_null_yields_no_rows() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE a (id INT PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE b (v INT)").unwrap();
    for i in 0..3 {
        db.execute_prepared("INSERT INTO a VALUES (?)", vec![Value::Integer(i)])
            .unwrap();
    }
    db.execute("INSERT INTO b VALUES (1)").unwrap();
    db.execute("INSERT INTO b VALUES (NULL)").unwrap();

    // id NOT IN (1, NULL) is never TRUE → zero rows
    let rows = sel(&db, "SELECT id FROM a WHERE id NOT IN (SELECT v FROM b)");
    assert_eq!(
        ints(&rows),
        Vec::<i64>::new(),
        "NOT IN over a set containing NULL matches nothing",
    );

    // Positive direction: id IN (1, NULL) returns the 1
    let rows = sel(&db, "SELECT id FROM a WHERE id IN (SELECT v FROM b)");
    assert_eq!(ints(&rows), vec![1]);

    // Without the NULL, NOT IN behaves normally
    db.execute("DELETE FROM b WHERE v IS NULL").unwrap();
    let rows = sel(&db, "SELECT id FROM a WHERE id NOT IN (SELECT v FROM b)");
    assert_eq!(ints(&rows), vec![0, 2]);
}

// ---------------------------------------------------------------------
// Parquet DATE round-trip (gap in existing coverage)
// ---------------------------------------------------------------------

#[test]
fn parquet_date_roundtrip_and_missing_column_nulls() {
    let (db, d) = new_db();
    db.execute("CREATE TABLE src (id INT PRIMARY KEY, d DATE, label TEXT)").unwrap();
    db.execute_prepared(
        "INSERT INTO src VALUES (?, ?, ?)",
        vec![
            Value::Integer(1),
            Value::Date(Date::from_days(20000)),
            Value::Text(ArcString::from("row1")),
        ],
    )
    .unwrap();
    db.execute_prepared(
        "INSERT INTO src VALUES (?, ?, ?)",
        vec![Value::Integer(2), Value::Null, Value::Null],
    )
    .unwrap();

    let path = d.path().join("dates.parquet");
    db.execute(&format!("EXPORT TABLE src TO '{}'", path.display())).unwrap();

    // Same-shape table: values survive, NULLs survive
    db.execute("CREATE TABLE dst (id INT PRIMARY KEY, d DATE, label TEXT)").unwrap();
    db.execute(&format!("IMPORT PARQUET '{}' INTO dst", path.display())).unwrap();

    let rows = sel(&db, "SELECT id, d, label FROM dst ORDER BY id");
    assert_eq!(rows[0][1], Value::Date(Date::from_days(20000)));
    assert_eq!(rows[0][2], Value::Text(ArcString::from("row1")));
    assert!(matches!(rows[1][1], Value::Null));
    assert!(matches!(rows[1][2], Value::Null));

    // Target table with an EXTRA column not present in the file → NULL
    db.execute("CREATE TABLE wide (id INT PRIMARY KEY, d DATE, label TEXT, extra INT)").unwrap();
    db.execute(&format!("IMPORT PARQUET '{}' INTO wide", path.display())).unwrap();
    let rows = sel(&db, "SELECT extra FROM wide ORDER BY id");
    for r in &rows {
        assert!(matches!(r[0], Value::Null), "missing file column fills NULL");
    }
}
