//! §10b explicit window frames: ROWS/RANGE BETWEEN ... AND ... semantics.
//!
//! Expected values are hand-derived against PostgreSQL/DuckDB frame rules:
//! - ROWS bounds count physical rows in partition order;
//! - RANGE UNBOUNDED/CURRENT ROW follow peer groups;
//! - RANGE with numeric offsets is a value interval on the single ORDER BY
//!   key (first_ge/first_gt binary searches over the sorted key);
//! - ranking/offset functions (ROW_NUMBER/RANK/LAG/…) ignore frames.
use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn setup() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("frame.db")).unwrap();
    db.execute("CREATE TABLE m (id INT PRIMARY KEY, g TEXT, v INT)")
        .unwrap();
    for id in 1..=10i64 {
        let g = if id <= 5 { "a" } else { "b" };
        db.execute(&format!(
            "INSERT INTO m (id, g, v) VALUES ({id}, '{g}', {})",
            id * 10
        ))
        .unwrap();
    }
    (dir, db)
}

/// Run `SELECT id, <expr> AS w FROM m WHERE g='a' ORDER BY id` and return the
/// w column rendered as strings (NULL → "NULL", floats trimmed of trailing
/// zeros where exact).
fn wcol(db: &Database, expr: &str) -> Vec<String> {
    let sql = format!("SELECT id, {expr} AS w FROM m WHERE g = 'a' ORDER BY id");
    let r = db.execute(&sql).unwrap().materialize().unwrap();
    let (_cols, rows) = r.select_rows().unwrap();
    rows.iter()
        .map(|row| match &row[1] {
            Value::Integer(i) => i.to_string(),
            Value::Float(f) => {
                if f.fract() == 0.0 {
                    format!("{}", *f as i64)
                } else {
                    format!("{:.4}", f)
                }
            }
            Value::Null => "NULL".into(),
            other => format!("{:?}", other),
        })
        .collect()
}

#[test]
fn rows_sliding_aggregates() {
    let (_dir, db) = setup();
    // v = 10 20 30 40 50 (partition 'a')
    assert_eq!(
        wcol(
            &db,
            "SUM(v) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"
        ),
        vec!["30", "60", "90", "120", "90"],
        "centered rolling sum"
    );
    assert_eq!(
        wcol(
            &db,
            "SUM(v) OVER (ORDER BY v ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)"
        ),
        vec!["150", "140", "120", "90", "50"],
        "reverse cumulative (moving frame start — exact recompute path)"
    );
    assert_eq!(
        wcol(
            &db,
            "SUM(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
        ),
        vec!["10", "30", "60", "100", "150"],
        "cumulative (running-accumulator fast path)"
    );
    assert_eq!(
        wcol(&db, "SUM(v) OVER (ORDER BY v ROWS UNBOUNDED PRECEDING)"),
        vec!["10", "30", "60", "100", "150"],
        "single-bound shorthand (end defaults to CURRENT ROW)"
    );
    assert_eq!(
        wcol(
            &db,
            "COUNT(*) OVER (ORDER BY v ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING)"
        ),
        vec!["2", "2", "1", "0", "0"],
        "lookahead-only frame; empty frame ⇒ COUNT 0"
    );
    assert_eq!(
        wcol(
            &db,
            "SUM(v) OVER (ORDER BY v ROWS BETWEEN 4 FOLLOWING AND UNBOUNDED FOLLOWING)"
        ),
        vec!["50", "NULL", "NULL", "NULL", "NULL"],
        "legal frame that empties past the partition end ⇒ SUM NULL"
    );
}

#[test]
fn rows_avg_min_max() {
    let (_dir, db) = setup();
    assert_eq!(
        wcol(
            &db,
            "AVG(v) OVER (ORDER BY v ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)"
        ),
        vec!["10", "15", "20", "30", "40"],
        "AVG over trailing 3 rows"
    );
    assert_eq!(
        wcol(
            &db,
            "MIN(v) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"
        ),
        vec!["10", "10", "20", "30", "40"],
        "MIN over centered frame"
    );
    assert_eq!(
        wcol(
            &db,
            "MAX(v) OVER (ORDER BY v ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING)"
        ),
        vec!["20", "30", "40", "50", "50"],
        "MAX over current+next"
    );
}

#[test]
fn rows_first_last_value() {
    let (_dir, db) = setup();
    assert_eq!(
        wcol(
            &db,
            "LAST_VALUE(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING)"
        ),
        vec!["20", "30", "40", "50", "50"],
        "LAST_VALUE = next row in frame"
    );
    assert_eq!(
        wcol(
            &db,
            "FIRST_VALUE(v) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 2 PRECEDING)"
        ),
        vec!["NULL", "NULL", "10", "20", "30"],
        "FIRST_VALUE two rows back; empty frame near partition start ⇒ NULL"
    );
    assert_eq!(
        wcol(
            &db,
            "LAST_VALUE(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
        ),
        vec!["50"; 5],
        "full-frame LAST_VALUE"
    );
}

#[test]
fn range_peer_group_semantics() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("peers.db")).unwrap();
    db.execute("CREATE TABLE d (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for (id, v) in [(1, 10), (2, 10), (3, 20)] {
        db.execute(&format!("INSERT INTO d (id, v) VALUES ({id}, {v})"))
            .unwrap();
    }
    let r = db
        .execute(
            "SELECT id, SUM(v) OVER (ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS w \
             FROM d ORDER BY id",
        )
        .unwrap()
        .materialize()
        .unwrap();
    let (_cols, rows) = r.select_rows().unwrap();
    let got: Vec<i64> = rows
        .iter()
        .map(|r| match &r[1] {
            Value::Integer(i) => *i,
            other => panic!("expected int, got {:?}", other),
        })
        .collect();
    // both 10s are peers: the running frame closes at the peer-group END,
    // so the first 10 already sees both.
    assert_eq!(got, vec![20, 20, 40]);
}

#[test]
fn range_value_intervals() {
    let (_dir, db) = setup();
    // v spaced 10 apart: ±5 covers only self; ±10 also covers both neighbors.
    assert_eq!(
        wcol(
            &db,
            "SUM(v) OVER (ORDER BY v RANGE BETWEEN 5 PRECEDING AND 5 FOLLOWING)"
        ),
        vec!["10", "20", "30", "40", "50"],
        "offset < spacing ⇒ singleton frames"
    );
    assert_eq!(
        wcol(
            &db,
            "SUM(v) OVER (ORDER BY v RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING)"
        ),
        vec!["30", "60", "90", "120", "90"],
        "offset = spacing ⇒ self ± 1 neighbor"
    );
    assert_eq!(
        wcol(
            &db,
            "SUM(v) OVER (ORDER BY v RANGE BETWEEN 20 PRECEDING AND 10 PRECEDING)"
        ),
        vec!["NULL", "10", "30", "50", "70"],
        "strictly-before window; empty at partition start ⇒ NULL"
    );
    assert_eq!(
        wcol(
            &db,
            "COUNT(*) OVER (ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND 10 PRECEDING)"
        ),
        vec!["0", "1", "2", "3", "4"],
        "offset end bound via upper-bound search"
    );
}

#[test]
fn range_offset_desc_order() {
    let (_dir, db) = setup();
    // ORDER BY v DESC: PRECEDING means LARGER values. Window = v ∈ [v+10, v+20]
    // (end bound 10 PRECEDING is inclusive of the peer group at v+10).
    // Rows by id ascending (v = 10..50): v=10 → {20,30}=50, v=20 → {30,40}=70,
    // v=30 → {40,50}=90, v=40 → {50}, v=50 → empty ⇒ NULL.
    assert_eq!(
        wcol(
            &db,
            "SUM(v) OVER (ORDER BY v DESC RANGE BETWEEN 20 PRECEDING AND 10 PRECEDING)"
        ),
        vec!["50", "70", "90", "50", "NULL"],
        "asymmetric DESC interval"
    );
}

#[test]
fn ranking_and_offset_funcs_ignore_frame() {
    let (_dir, db) = setup();
    assert_eq!(
        wcol(
            &db,
            "ROW_NUMBER() OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"
        ),
        vec!["1", "2", "3", "4", "5"]
    );
    assert_eq!(
        wcol(
            &db,
            "LAG(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
        ),
        vec!["NULL", "10", "20", "30", "40"]
    );
}

#[test]
fn frame_parse_errors() {
    let (_dir, db) = setup();
    let cases = [
        // start may not be UNBOUNDED FOLLOWING
        "SELECT SUM(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW) FROM m",
        // start after end
        "SELECT SUM(v) OVER (ORDER BY v ROWS BETWEEN 2 FOLLOWING AND 1 FOLLOWING) FROM m",
        // RANGE offset requires exactly one ORDER BY key
        "SELECT SUM(v) OVER (RANGE BETWEEN 5 PRECEDING AND 5 FOLLOWING) FROM m",
        "SELECT SUM(v) OVER (ORDER BY v, id RANGE BETWEEN 5 PRECEDING AND 5 FOLLOWING) FROM m",
        // GROUPS not supported
        "SELECT SUM(v) OVER (ORDER BY v GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM m",
    ];
    for sql in cases {
        let err = db
            .execute(sql)
            .err()
            .unwrap_or_else(|| panic!("expected error: {sql}"));
        assert!(
            !err.to_string().is_empty(),
            "error must carry a message: {sql}"
        );
    }
    // non-numeric ORDER BY key with RANGE offset fails at execution
    let err = db
        .execute("SELECT SUM(v) OVER (ORDER BY g RANGE BETWEEN 5 PRECEDING AND 5 FOLLOWING) FROM m")
        .err()
        .expect("text ORDER BY key with RANGE offset must fail");
    assert!(err.to_string().to_lowercase().contains("range"));
}
