//! Round 12c regressions: bugs found by the wheel-level E2E pass.

use motedb::types::Value;
use motedb::{DBConfig, Database};
use tempfile::TempDir;

fn db() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    (dir, db)
}

fn ids(db: &Database, sql: &str) -> Vec<i64> {
    db.query(sql)
        .unwrap()
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            v => panic!("{v:?}"),
        })
        .collect()
}

/// `ORDER BY x LIMIT k` must agree with the unlimited ORDER BY on NULL
/// placement (NULLs first ASC / last DESC — the engine-wide order_by_cmp
/// semantics). The top-k heap path coerced NULL floats to NaN (total-order
/// MAXIMUM): ASC top-k dropped null rows while DESC ranked them FIRST.
#[test]
fn topk_limit_agrees_with_full_sort_on_nulls() {
    let (_d, db) = db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, score FLOAT)").unwrap();
    let mut batch = Vec::new();
    for i in 0..10i64 {
        batch.push(vec![Value::Integer(i), Value::Float(i as f64 * 1.5)]);
    }
    batch.push(vec![Value::Integer(100), Value::Null]); // NULL row
    batch.push(vec![Value::Integer(101), Value::Float(f64::NAN)]); // NaN row
    db.execute_prepared_many("INSERT INTO t (id, score) VALUES (?, ?)", batch)
        .unwrap();
    db.checkpoint().unwrap();

    let full_asc = ids(&db, "SELECT id FROM t ORDER BY score ASC");
    assert_eq!(full_asc[0], 100, "NULL first on ASC (full sort)");
    assert_eq!(*full_asc.last().unwrap(), 101, "NaN last on ASC (full sort)");

    let asc3 = ids(&db, "SELECT id FROM t ORDER BY score ASC LIMIT 3");
    assert_eq!(asc3, vec![100, 0, 1], "ASC LIMIT must include the NULL row: {asc3:?}");

    let desc3 = ids(&db, "SELECT id FROM t ORDER BY score DESC LIMIT 3");
    assert_eq!(desc3, vec![101, 9, 8], "DESC LIMIT: NaN first, NULL NOT in top: {desc3:?}");

    // DESC with k covering every non-null row: the NULL row comes LAST.
    let desc_all = ids(&db, "SELECT id FROM t ORDER BY score DESC LIMIT 12");
    assert_eq!(desc_all.len(), 12);
    assert_eq!(*desc_all.last().unwrap(), 100, "NULL last on DESC when k covers it");

    // Integer sort column (same semantics through the i64 heap path).
    db.execute("CREATE TABLE ti (id INT PRIMARY KEY, n INT)").unwrap();
    let mut b2 = Vec::new();
    for i in 0..8i64 {
        b2.push(vec![Value::Integer(i), Value::Integer(i * 3)]);
    }
    b2.push(vec![Value::Integer(90), Value::Null]);
    db.execute_prepared_many("INSERT INTO ti (id, n) VALUES (?, ?)", b2)
        .unwrap();
    db.checkpoint().unwrap();
    let asc = ids(&db, "SELECT id FROM ti ORDER BY n ASC LIMIT 2");
    assert_eq!(asc, vec![90, 0], "int NULL first ASC: {asc:?}");
    let desc = ids(&db, "SELECT id FROM ti ORDER BY n DESC LIMIT 1");
    assert_eq!(desc, vec![7], "int NULL not in DESC top: {desc:?}");
}

/// Zero-argument `BM25_SCORE()` (paired with `WHERE MATCH(col, 'q')`) must
/// project the driving search's score. It required a matching first ARGUMENT,
/// so the zero-arg form returned NULL for every row.
#[test]
fn bm25_score_zero_arg_returns_scores() {
    let (_d, db) = db();
    db.execute("CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)").unwrap();
    let mut batch = Vec::new();
    for i in 0..30i64 {
        let body = if i % 2 == 0 {
            format!("alpha beta common text {i}")
        } else {
            format!("gamma delta other text {i}")
        };
        batch.push(vec![
            Value::Integer(i),
            Value::Text(motedb::types::ArcString::from(body.as_str())),
        ]);
    }
    db.execute_prepared_many("INSERT INTO docs (id, body) VALUES (?, ?)", batch)
        .unwrap();
    db.checkpoint().unwrap();
    db.execute("CREATE TEXT INDEX docs_body ON docs(body)").unwrap();

    let rows = db
        .query("SELECT id, BM25_SCORE() FROM docs WHERE MATCH(body, 'alpha') LIMIT 5")
        .unwrap();
    assert_eq!(rows.len(), 5);
    for r in &rows {
        match &r[1] {
            Value::Float(s) => {
                assert!(*s > 0.0, "BM25 score must be positive for a hit, got {s}");
            }
            v => panic!("BM25_SCORE() returned {v:?} for a matched row"),
        }
    }
    // Ranked ordering: scores non-increasing.
    let scores: Vec<f64> = rows
        .iter()
        .map(|r| match &r[1] {
            Value::Float(s) => *s,
            _ => 0.0,
        })
        .collect();
    assert!(
        scores.windows(2).all(|w| w[0] >= w[1]),
        "scores must be non-increasing: {scores:?}"
    );
}

/// `LATEST BY` must never be silently dropped: the TimeSeries columnar
/// pushdown (try_columnar_select) had no latest-per-group fold but intercepted
/// the query anyway, returning EVERY row for `… WHERE … LATEST BY ts` (with
/// or without WHERE). It now declines, letting the materialized path's
/// apply_latest_by run.
#[test]
fn latest_by_not_dropped_by_columnar_pushdown() {
    let (_d, db) = db();
    db.execute(
        "CREATE TABLE m (sensor TEXT, ts TIMESTAMP, temp FLOAT) TIMESERIES(ts)",
    )
    .unwrap();
    let mut batch = Vec::new();
    for k in 0..20i64 {
        batch.push(vec![
            Value::Text(motedb::types::ArcString::from(
                if k % 2 == 0 { "s0" } else { "s1" },
            )),
            Value::Timestamp(motedb::types::Timestamp::from_micros(
                1_700_000_000i64 * 1_000_000 + k * 1000,
            )),
            Value::Float(20.0 + k as f64),
        ]);
    }
    db.execute_prepared_many("INSERT INTO m (sensor, ts, temp) VALUES (?, ?, ?)", batch)
        .unwrap();
    db.checkpoint().unwrap();

    // Canonical semantics: LATEST BY <series column> groups by that column
    // and keeps each group's max-ts row (LATEST BY ts groups by ts itself —
    // all rows — by design, see test_timeseries_semantics).
    // No WHERE: latest row per sensor (s0 → k=18, s1 → k=19).
    let rows = db
        .query("SELECT sensor, temp FROM m LATEST BY sensor")
        .unwrap();
    assert_eq!(rows.len(), 2, "one row per sensor: {rows:?}");
    let got: std::collections::HashMap<String, f64> = rows
        .iter()
        .map(|r| match (&r[0], &r[1]) {
            (Value::Text(t), Value::Float(f)) => (t.as_str().to_string(), *f),
            v => panic!("{v:?}"),
        })
        .collect();
    assert_eq!(got.get("s0"), Some(&38.0), "s0 latest");
    assert_eq!(got.get("s1"), Some(&39.0), "s1 latest");

    // With WHERE: the columnar pushdown used to intercept this shape and
    // silently drop the clause (every row back); it now defers to
    // apply_latest_by.
    let rows = db
        .query("SELECT sensor, temp FROM m WHERE sensor = 's1' LATEST BY sensor")
        .unwrap();
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert!(matches!((&rows[0][0], &rows[0][1]),
        (Value::Text(t), Value::Float(f)) if t.as_str() == "s1" && *f == 39.0));
}
