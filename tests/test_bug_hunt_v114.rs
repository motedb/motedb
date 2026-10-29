//! Round-24 bug-hunt probes.
//!
//! Focus areas:
//! * spatial boundary exactness (radius ≤, box edges inclusive) and octree
//!   view after UPDATEs move points in/out of range — vs brute-force oracle
//! * TIME_BUCKET alignment (timestamps exactly on interval multiples)
//! * division-by-zero family (int/int, float/float, float by zero literal)
//!   — consistent error or NULL, never a panic
//! * ROUND with negative digits
//! * `||` coercion (text || integer)
//! * UPDATE SET from scalar subquery over the same table
//! * INSERT ... SELECT support

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
// Spatial boundary exactness + movement under index
// ---------------------------------------------------------------------

#[test]
fn spatial_boundaries_and_movement() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE p (id INT PRIMARY KEY, c GEOMETRY)").unwrap();
    // Points around origin at exact distances 1, 2, 3 along axes.
    for (id, x, y, z) in [(1i64, 1.0f64, 0.0, 0.0), (2, 0.0, 2.0, 0.0), (3, 0.0, 0.0, 3.0), (4, 2.0, 2.0, 0.0)] {
        db.execute(&format!(
            "INSERT INTO p VALUES ({id}, POINT3D({x}, {y}, {z}))"
        ))
        .unwrap();
    }

    // Radius boundary: distance exactly 2.0 must be INCLUDED (≤ semantics).
    let r = ints(&sel(&db, "SELECT id FROM p WHERE ST_RADIUS_3D(c, 0, 0, 0, 2.0) ORDER BY id"));
    assert_eq!(r, vec![1, 2], "radius 2.0 includes the point at distance exactly 2");

    // Box boundary: points exactly ON the min/max faces are inside.
    let r = ints(&sel(
        &db,
        "SELECT id FROM p WHERE ST_WITHIN_3D(c, 0, 0, 0, 2, 2, 2) ORDER BY id",
    ));
    assert_eq!(r, vec![1, 2, 4], "box [0..2]³ includes face-boundary points 1, 2, 4");

    // Build the octree index — same answers through the index.
    // (Radius truth: ids 1, 2 — id4 (2,2,0) is at distance 2.83 > 2.0.)
    db.execute("CREATE SPATIAL INDEX p_c ON p(c)").unwrap();
    let r2 = ints(&sel(&db, "SELECT id FROM p WHERE ST_RADIUS_3D(c, 0, 0, 0, 2.0) ORDER BY id"));
    assert_eq!(r2, vec![1, 2], "indexed radius agrees with unindexed");
    let r2 = ints(&sel(&db, "SELECT id FROM p WHERE ST_WITHIN_3D(c, 0, 0, 0, 2, 2, 2) ORDER BY id"));
    assert_eq!(r2, vec![1, 2, 4], "indexed box agrees with unindexed");

    // Movement: pull id 3 (dist 3) INTO the radius and push id 1 OUT.
    db.execute("UPDATE p SET c = POINT3D(1.5, 0, 0) WHERE id = 3").unwrap();
    db.execute("UPDATE p SET c = POINT3D(9, 9, 9) WHERE id = 1").unwrap();
    let r = ints(&sel(&db, "SELECT id FROM p WHERE ST_RADIUS_3D(c, 0, 0, 0, 2.0) ORDER BY id"));
    assert_eq!(r, vec![2, 3], "after movement: id3 in, id1 out");

    // New point inserted after index build is visible.
    db.execute("INSERT INTO p VALUES (9, POINT3D(0.5, 0, 0))").unwrap();
    let r = ints(&sel(&db, "SELECT id FROM p WHERE ST_RADIUS_3D(c, 0, 0, 0, 2.0) ORDER BY id"));
    assert_eq!(r, vec![2, 3, 9], "post-index insert visible");

    // ST_KNN_3D top-3 exactness vs hand-computed truth (id9 0.5, id3 1.5, id2 2.0)
    let r = ints(&sel(&db, "SELECT id FROM p WHERE ST_KNN_3D(c, 0, 0, 0, 3) ORDER BY id"));
    assert!(r.contains(&9) && r.contains(&3) && r.contains(&2), "knn top-3 = {{9,3,2}}, got {r:?}");
}

// ---------------------------------------------------------------------
// TIME_BUCKET alignment
// ---------------------------------------------------------------------

#[test]
fn time_bucket_alignment() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE ev (id INT PRIMARY KEY, ts TIMESTAMP)").unwrap();
    // Exactly on 5-minute boundaries (epoch-based), plus one second after.
    // 1700000000 = 2023-11-14 22:13:20; use clean multiples instead:
    // 00:00:00, 00:05:00, 00:10:00 of a chosen day + :05:01.
    let base = 1700006400i64; // 2023-11-14 23:20:00? — computed below via dates
    let _ = base;
    let day = 1700000000i64 - 1700000000 % 86400; // start of that UTC day
    for (i, off) in [0i64, 300, 600, 301, 299].iter().enumerate() {
        db.execute_prepared(
            "INSERT INTO ev VALUES (?, ?)",
            vec![
                Value::Integer(i as i64),
                Value::Timestamp(motedb::types::Timestamp::from_secs(day + off)),
            ],
        )
        .unwrap();
    }

    let one = |sql: &str| -> Value { sel(&db, sql).pop().unwrap()[0].clone() };
    // Every on-boundary ts buckets to itself (floor is identity on exact
    // multiples); 00:05:01 and 00:04:59 both bucket to 00:05:00? No:
    // 00:04:59 floors DOWN to 00:00:00.
    let b0 = match one("SELECT TIME_BUCKET('5m', ts) FROM ev WHERE id = 0") {
        Value::Timestamp(t) => t.as_secs(),
        o => panic!("{o:?}"),
    };
    assert_eq!(b0, day, "ts exactly on a bucket boundary floors to itself");
    let b4 = match one("SELECT TIME_BUCKET('5m', ts) FROM ev WHERE id = 4") {
        Value::Timestamp(t) => t.as_secs(),
        o => panic!("{o:?}"),
    };
    assert_eq!(b4, day, "00:04:59 floors down into the first bucket");
    let b3 = match one("SELECT TIME_BUCKET('5m', ts) FROM ev WHERE id = 3") {
        Value::Timestamp(t) => t.as_secs(),
        o => panic!("{o:?}"),
    };
    assert_eq!(b3, day + 300, "00:05:01 floors to 00:05:00");

    // GROUP BY bucket counts: bucket(day)=3 (ids 0,1,4→day? no: id4=299→day,
    // id1=300→day+300, id2=600→day+600, id3=301→day+300)
    let rows = sel(
        &db,
        "SELECT TIME_BUCKET('5m', ts) b, COUNT(*) FROM ev GROUP BY b ORDER BY b",
    );
    let counts: Vec<i64> = rows.iter().map(|r| match &r[1] { Value::Integer(n) => *n, o => panic!("{o:?}") }).collect();
    assert_eq!(counts, vec![2, 2, 1], "buckets: day=ids 0,4; +300=ids 1,3; +600=id 2");
}

// ---------------------------------------------------------------------
// Division-by-zero family
// ---------------------------------------------------------------------

#[test]
fn division_by_zero_family() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE d (id INT PRIMARY KEY, a INT, b FLOAT)").unwrap();
    db.execute("INSERT INTO d VALUES (1, 10, 2.5)").unwrap();
    db.execute("INSERT INTO d VALUES (2, 0, 0.0)").unwrap();

    // 🔴 round-24 pinned: division by zero ERRORS for BOTH integer and
    // float divisors (PostgreSQL semantics — PG errors on 1.0/0.0 too;
    // SQLite's NULL and IEEE's ±inf are NOT used). Consistent across
    // column/literal × int/float forms; never a panic.
    for sql in [
        "SELECT a / 0 FROM d WHERE id = 1",
        "SELECT b / 0 FROM d WHERE id = 1",
        "SELECT b / 0.0 FROM d WHERE id = 1",
        "SELECT 2.5 / 0 FROM d",
        "SELECT 2.5 / 0.0 FROM d",
    ] {
        let err = db.query(sql).expect_err(&format!("must error: {sql}"));
        assert!(
            format!("{err:?}").contains("DivisionByZero"),
            "{sql}: DivisionByZero, got {err:?}",
        );
    }
    // 🔴 round-24 pinned ASYMMETRY (documented limitation): a division by
    // zero in a WHERE filter is swallowed by the filter fallback path as
    // "no match" (0 rows, no error) while the projection form errors. The
    // filter swallow is load-bearing for dynamic fallbacks; changing it
    // needs the filter-Error-propagation rework noted in docs/03.
    let n = ints(&sel(&db, "SELECT id FROM d WHERE b / 0 > 0"));
    assert_eq!(n, Vec::<i64>::new(), "WHERE div-by-zero → no match (pinned)");
    // Modulo by zero errors (existing semantics).
    assert!(db.query("SELECT a % 0 FROM d WHERE id = 1").is_err());
}

// ---------------------------------------------------------------------
// ROUND negative digits, || coercion
// ---------------------------------------------------------------------

#[test]
fn round_negative_digits_and_concat_coercion() {
    let (db, _d) = new_db();
    let one = |sql: &str| -> Value { sel(&db, sql).pop().unwrap()[0].clone() };

    // ROUND(x, -1): round to tens.
    let r = match one("SELECT ROUND(123.456, -1)") {
        Value::Float(f) => f,
        o => panic!("{o:?}"),
    };
    assert!((r - 120.0).abs() < 1e-9, "ROUND(123.456, -1) = 120, got {r}");
    let r = match one("SELECT ROUND(125.0, -1)") {
        Value::Float(f) => f,
        o => panic!("{o:?}"),
    };
    assert!((r - 130.0).abs() < 1e-9, "ROUND(125, -1) half-away = 130, got {r}");
    let r = match one("SELECT ROUND(178, -2)") {
        Value::Integer(n) => n,
        Value::Float(f) => f as i64,
        o => panic!("{o:?}"),
    };
    assert_eq!(r, 200, "ROUND(178, -2) = 200");

    // || coercion: text || integer → text.
    let c = one("SELECT 'n=' || 42");
    assert_eq!(c, Value::Text(ArcString::from("n=42")), "|| coerces int to text");
    // Integer || integer is NOT text concat in some engines (bit-or);
    // MoteDB's || is concat — pin whichever it is consistently.
    let c = one("SELECT 1 || 2");
    match c {
        Value::Text(t) => assert_eq!(t.as_str(), "12", "1||2 text concat"),
        Value::Integer(n) => assert_eq!(n, 3, "1||2 bit-or (documented alternative)"),
        o => panic!("1||2 unexpected: {o:?}"),
    }
}

// ---------------------------------------------------------------------
// UPDATE SET from scalar subquery over the same table
// ---------------------------------------------------------------------

#[test]
fn update_set_from_scalar_subquery_same_table() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, v INT)").unwrap();
    for (id, v) in [(1i64, 10), (2, 20), (3, 30)] {
        db.execute(&format!("INSERT INTO u VALUES ({id}, {v})")).unwrap();
    }

    // Every row gets the table's max (evaluated BEFORE the update applies).
    db.execute("UPDATE u SET v = (SELECT MAX(v) FROM u)").unwrap();
    let vs = ints(&sel(&db, "SELECT v FROM u ORDER BY id"));
    assert_eq!(vs, vec![30, 30, 30], "scalar subquery saw pre-update values");

    // Self-referencing arithmetic: v = v + avg (avg of {30,30,30} = 30 → 60).
    db.execute("UPDATE u SET v = v + (SELECT AVG(v) FROM u)").unwrap();
    let vs = ints(&sel(&db, "SELECT v FROM u ORDER BY id"));
    assert_eq!(vs, vec![60, 60, 60], "avg read pre-update state");
}

// ---------------------------------------------------------------------
// INSERT ... SELECT
// ---------------------------------------------------------------------

#[test]
fn insert_select_support() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE src (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("CREATE TABLE dst (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 0..5 {
        db.execute(&format!("INSERT INTO src VALUES ({i}, {})", i * 10)).unwrap();
    }

    // Accept clean support OR a clean parse error — never a panic or a
    // partial silent import.
    match db.execute("INSERT INTO dst SELECT * FROM src WHERE v >= 20") {
        Ok(_) => {
            let r = ints(&sel(&db, "SELECT id FROM dst ORDER BY id"));
            assert_eq!(r, vec![2, 3, 4], "insert-select copies the filtered rows");
        }
        Err(e) => {
            let m = format!("{e:?}");
            assert!(m.contains("Parse") || m.contains("upport") || m.contains("nexpected"), "{e:?}");
        }
    }
}
