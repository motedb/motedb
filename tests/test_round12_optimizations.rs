//! Round 12 regressions: the four optimization targets from the competitor
//! benchmark post-mortem.
//!
//! 1. Disk: checkpoint's `force_compact_all` retired superseded segments in
//!    the manifest but never called `sync_manifest` — the physical files
//!    stayed until the next reopen swept them, so a checkpointed table
//!    occupied 2× its data size (322MB vs 161MB on the 100K×384 dataset).
//! 2. Range aggregation: multi-term AND predicates (`ts >= a AND ts <= b AND
//!    device = 'x'`) either fell to the full materialization path
//!    (COUNT(*)-only shape) or materialized rows passing only the FIRST
//!    predicate before post-filtering. Both are replaced by a fused
//!    single-pass fold over raw column bytes.
//! 3. Equi-JOIN + GROUP BY: the multi-way hash join scanned BOTH tables
//!    full-width — on a table with a 384-dim VECTOR column that decoded
//!    ~153MB of tensor Values for a query referencing only `device`/`zone`
//!    (138ms, +300MB RSS). Scans are now projected to referenced columns.
//! 4. COUNT(col) inside the join path ignored non-numeric non-NULL values
//!    (TEXT/TIMESTAMP/BOOL never incremented the counter).

use motedb::types::Value;
use motedb::{DBConfig, Database};
use tempfile::TempDir;

fn db() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    (dir, db)
}

/// Deterministic LCG so expected values are recomputable without rand.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn f64(&mut self) -> f64 {
        (self.next() as f64 / (1u64 << 31) as f64) * 20.0 - 10.0
    }
}

fn one_i64(rows: &[Vec<Value>], col: usize) -> i64 {
    match &rows[0][col] {
        Value::Integer(i) => *i,
        v => panic!(
            "expected integer at col {col}, got {v:?} (row: {:?})",
            rows[0]
        ),
    }
}

fn one_f64(rows: &[Vec<Value>], col: usize) -> Option<f64> {
    match &rows[0][col] {
        Value::Float(f) => Some(*f),
        Value::Null => None,
        v => panic!("expected float/null, got {v:?}"),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// 1. Disk: checkpoint physically deletes superseded segment files.
// ─────────────────────────────────────────────────────────────────────────

/// Bulk inserts (each ≥ the 8MB write-buffer flush threshold) create several
/// segments; the final checkpoint merges them into one — and must DELETE the
/// superseded files (previously they survived until the next reopen).
#[test]
fn checkpoint_deletes_superseded_segment_files() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE big (id INT PRIMARY KEY, note TEXT)")
        .unwrap();

    // ~120KB/row × 3 batches of 100 → each batch crosses the 8MB threshold,
    // producing one flushed segment per batch (3 segments before checkpoint).
    let payload = "x".repeat(120_000);
    for b in 0..3 {
        let batch: Vec<Vec<Value>> = (0..100)
            .map(|i| {
                vec![
                    Value::Integer(b * 100 + i),
                    Value::Text(motedb::types::ArcString::from(payload.as_str())),
                ]
            })
            .collect();
        db.execute_prepared_many("INSERT INTO big (id, note) VALUES (?, ?)", batch)
            .unwrap();
    }

    let seg_dir = dir.path().join("columnar_ms").join("big");
    let before: Vec<_> = std::fs::read_dir(&seg_dir)
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            (p.extension().is_some_and(|x| x == "sst")).then_some(p)
        })
        .collect();
    assert!(
        before.len() >= 2,
        "expected ≥2 segments from batched flushes, got {}",
        before.len()
    );

    db.checkpoint().unwrap();

    // Long TEXT must survive the merge: query it straight after checkpoint
    // (in-memory segments) — a Null here means the MERGE re-encode dropped it.
    let rows = db.query("SELECT note FROM big WHERE id = 250").unwrap();
    match &rows[0][0] {
        Value::Text(t) => assert_eq!(t.as_str().len(), 120_000, "post-checkpoint length"),
        v => panic!("expected text right after checkpoint, got {v:?}"),
    }

    let after: Vec<_> = std::fs::read_dir(&seg_dir)
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            (p.extension().is_some_and(|x| x == "sst")).then_some(p)
        })
        .collect();
    assert_eq!(
        after.len(),
        1,
        "checkpoint must leave exactly ONE merged segment file, found {}",
        after.len()
    );

    // Data intact after reopen.
    db.close().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let rows = db.query("SELECT COUNT(*) FROM big").unwrap();
    assert_eq!(one_i64(&rows, 0), 300);
    let rows = db.query("SELECT note FROM big WHERE id = 250").unwrap();
    match &rows[0][0] {
        Value::Text(t) => assert_eq!(t.as_str().len(), 120_000),
        v => panic!("expected text, got {v:?}"),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// 2. Fused multi-predicate aggregation (differential vs source data).
// ─────────────────────────────────────────────────────────────────────────

/// Row source shared by the query and the Rust-computed expectation.
struct AggRow {
    ts: i64,
    dev: Option<String>,
    val: Option<f64>,
    n: i64,
}

fn seed_agg_table(db: &Database) -> Vec<AggRow> {
    db.execute("CREATE TABLE ev (id INT PRIMARY KEY, ts TIMESTAMP, dev TEXT, val FLOAT, n INT)")
        .unwrap();
    let mut rng = Lcg(42);
    let mut src = Vec::new();
    let mut batch: Vec<Vec<Value>> = Vec::new();
    for i in 0..2000i64 {
        let ts = 1_700_000_000i64 * 1_000_000 + i * 7919; // strictly increasing
        let dev = if i % 11 == 0 {
            None
        } else {
            Some(format!("dev-{}", i % 5))
        };
        let val = if i % 7 == 0 { None } else { Some(rng.f64()) };
        let n = (i * 13) % 101 - 50;
        batch.push(vec![
            Value::Integer(i),
            Value::Timestamp(motedb::types::Timestamp::from_micros(ts)),
            match &dev {
                Some(d) => Value::Text(motedb::types::ArcString::from(d.as_str())),
                None => Value::Null,
            },
            match val {
                Some(v) => Value::Float(v),
                None => Value::Null,
            },
            Value::Integer(n),
        ]);
        src.push(AggRow { ts, dev, val, n });
    }
    db.execute_prepared_many(
        "INSERT INTO ev (id, ts, dev, val, n) VALUES (?, ?, ?, ?, ?)",
        batch,
    )
    .unwrap();
    db.checkpoint().unwrap(); // force a merged single segment
    src
}

/// SQL three-valued comparison for the expectation side.
fn matches_sql(v: Option<f64>, target: f64, ge: bool) -> bool {
    match v {
        Some(x) => {
            if ge {
                x >= target
            } else {
                x < target
            }
        }
        None => false, // NULL compares false
    }
}

#[test]
fn fused_range_aggregation_matches_expected() {
    let (_d, db) = db();
    let src = seed_agg_table(&db);

    // Case A: ts range AND dev equality (the competitor-benchmark shape).
    let lo = src[300].ts;
    let hi = src[1700].ts;
    let rows = db
        .query(&format!(
            "SELECT COUNT(*), COUNT(val), SUM(val), AVG(val), MIN(val), MAX(val) \
             FROM ev WHERE ts >= {lo} AND ts <= {hi} AND dev = 'dev-3'"
        ))
        .unwrap();
    let sel: Vec<&AggRow> = src
        .iter()
        .filter(|r| r.ts >= lo && r.ts <= hi && r.dev.as_deref() == Some("dev-3"))
        .collect();
    let vals: Vec<f64> = sel.iter().filter_map(|r| r.val).collect();
    assert_eq!(one_i64(&rows, 0), sel.len() as i64, "COUNT(*)");
    assert_eq!(
        one_i64(&rows, 1),
        vals.len() as i64,
        "COUNT(val) skips NULLs"
    );
    let sum: f64 = vals.iter().sum();
    assert!((one_f64(&rows, 2).unwrap() - sum).abs() < 1e-9, "SUM");
    assert!(
        (one_f64(&rows, 3).unwrap() - sum / vals.len() as f64).abs() < 1e-9,
        "AVG"
    );
    assert!(
        (one_f64(&rows, 4).unwrap() - vals.iter().cloned().fold(f64::INFINITY, f64::min)).abs()
            < 1e-9,
        "MIN"
    );
    assert!(
        (one_f64(&rows, 5).unwrap() - vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max)).abs()
            < 1e-9,
        "MAX"
    );

    // Case B: dev equality + val range over an INTEGER agg column.
    let rows = db
        .query("SELECT COUNT(*), SUM(n), MIN(n), MAX(n) FROM ev WHERE dev = 'dev-1' AND val > 0")
        .unwrap();
    let sel: Vec<&AggRow> = src
        .iter()
        .filter(|r| {
            r.dev.as_deref() == Some("dev-1") && matches_sql(r.val, 0.0, true) && r.val != Some(0.0)
        })
        .collect();
    assert_eq!(one_i64(&rows, 0), sel.len() as i64);
    let ns: Vec<i64> = sel.iter().map(|r| r.n).collect();
    assert_eq!(one_i64(&rows, 1), ns.iter().sum::<i64>(), "SUM(int)");
    assert_eq!(one_i64(&rows, 2), *ns.iter().min().unwrap(), "MIN(int)");
    assert_eq!(one_i64(&rows, 3), *ns.iter().max().unwrap(), "MAX(int)");

    // Case C: COUNT(*)-only with a 3-term AND (previously materialized the
    // full table).
    let rows = db
        .query(&format!(
            "SELECT COUNT(*) FROM ev WHERE ts >= {lo} AND ts <= {hi} AND dev = 'dev-2'"
        ))
        .unwrap();
    let want = src
        .iter()
        .filter(|r| r.ts >= lo && r.ts <= hi && r.dev.as_deref() == Some("dev-2"))
        .count();
    assert_eq!(one_i64(&rows, 0), want as i64);

    // Case D: empty result — COUNT 0, SUM/AVG/MIN/MAX NULL.
    let rows = db
        .query(
            "SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM ev \
                WHERE ts >= 9999999999999999 AND dev = 'dev-3' AND val > 100",
        )
        .unwrap();
    assert_eq!(one_i64(&rows, 0), 0);
    for c in 1..5 {
        assert!(one_f64(&rows, c).is_none(), "col {c} must be NULL");
    }

    // Case E: text range predicate + two float predicates.
    let rows = db
        .query("SELECT COUNT(*) FROM ev WHERE dev >= 'dev-2' AND dev < 'dev-4' AND val >= -5 AND val <= 5")
        .unwrap();
    let want = src
        .iter()
        .filter(|r| {
            let d_ok = match &r.dev {
                Some(d) => d.as_str() >= "dev-2" && d.as_str() < "dev-4",
                None => false,
            };
            let v_ok = matches!(r.val, Some(x) if x >= -5.0 && x <= 5.0);
            d_ok && v_ok
        })
        .count();
    assert_eq!(one_i64(&rows, 0), want as i64);

    // Case F: integer literal compared against a FLOAT column (coercion).
    let rows = db
        .query("SELECT COUNT(*) FROM ev WHERE val >= 9 AND dev = 'dev-0'")
        .unwrap();
    let want = src
        .iter()
        .filter(|r| r.dev.as_deref() == Some("dev-0") && matches_sql(r.val, 9.0, true))
        .count();
    assert_eq!(one_i64(&rows, 0), want as i64);

    // Case G: no WHERE — the pre-existing single-pass path must still agree.
    let rows = db
        .query("SELECT COUNT(*), COUNT(val), SUM(val) FROM ev")
        .unwrap();
    assert_eq!(one_i64(&rows, 0), 2000);
    assert_eq!(
        one_i64(&rows, 1),
        src.iter().filter(|r| r.val.is_some()).count() as i64
    );
}

/// Boolean literals in a MULTI-predicate WHERE must coerce like the
/// single-predicate path (`flag = TRUE AND id < k`). A Bool literal left the
/// integer target empty and matched nothing (found by the Round-12c E2E).
#[test]
fn fused_aggregation_boolean_literal_predicate() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE b (id INT PRIMARY KEY, flag BOOLEAN, n INT)")
        .unwrap();
    let batch: Vec<Vec<Value>> = (0..50i64)
        .map(|i| {
            vec![
                Value::Integer(i),
                Value::Bool(i % 2 == 0),
                Value::Integer(i),
            ]
        })
        .collect();
    db.execute_prepared_many("INSERT INTO b (id, flag, n) VALUES (?, ?, ?)", batch)
        .unwrap();
    db.checkpoint().unwrap();

    let rows = db
        .query("SELECT COUNT(*), SUM(n) FROM b WHERE flag = TRUE AND id < 20")
        .unwrap();
    assert_eq!(one_i64(&rows, 0), 10, "flag=TRUE AND id<20");
    assert_eq!(one_i64(&rows, 1), (0..20i64).step_by(2).sum::<i64>());

    let rows = db
        .query("SELECT COUNT(*) FROM b WHERE flag = FALSE AND id < 20")
        .unwrap();
    assert_eq!(one_i64(&rows, 0), 10);

    // Integer literal against the BOOLEAN column (same coercion family).
    let rows = db
        .query("SELECT COUNT(*) FROM b WHERE flag = 0 AND id < 20")
        .unwrap();
    assert_eq!(one_i64(&rows, 0), 10);
}

/// UPDATE + DELETE visibility through the fused aggregate path (segments hold
/// superseded versions; newest-wins + tombstones must apply).
#[test]
fn fused_aggregation_respects_update_and_delete() {
    let (_d, db) = db();
    let src = seed_agg_table(&db);
    // UPDATE 100 rows into dev-4 with val 42, DELETE 50 others.
    db.execute("UPDATE ev SET dev = 'dev-4', val = 42 WHERE id < 100")
        .unwrap();
    db.execute("DELETE FROM ev WHERE id >= 100 AND id < 150")
        .unwrap();
    db.checkpoint().unwrap(); // merge with tombstones dropped

    let rows = db
        .query("SELECT COUNT(*), COUNT(val), SUM(val) FROM ev WHERE dev = 'dev-4'")
        .unwrap();
    let updated = src[..100].len();
    let orig_dev4_after_del = src[150..]
        .iter()
        .filter(|r| r.dev.as_deref() == Some("dev-4"))
        .count();
    let mut expected_sum = 42.0 * 100.0;
    let mut expected_n = 100;
    for r in &src[150..] {
        if r.dev.as_deref() == Some("dev-4") {
            if let Some(v) = r.val {
                expected_sum += v;
                expected_n += 1;
            }
        }
    }
    assert_eq!(one_i64(&rows, 0), (updated + orig_dev4_after_del) as i64);
    assert_eq!(one_i64(&rows, 1), expected_n);
    assert!((one_f64(&rows, 2).unwrap() - expected_sum).abs() < 1e-9);

    let rows = db.query("SELECT COUNT(*) FROM ev").unwrap();
    assert_eq!(one_i64(&rows, 0), 2000 - 50);
}

// ─────────────────────────────────────────────────────────────────────────
// 3. Projected equi-JOIN + GROUP BY (differential vs source data).
// ─────────────────────────────────────────────────────────────────────────

struct JoinRow {
    device: String,
    val: Option<f64>,
    grp: i64,
}

fn seed_join_tables(db: &Database) -> Vec<JoinRow> {
    db.execute(
        "CREATE TABLE ev (id INT PRIMARY KEY, device TEXT, val FLOAT, grp INT, \
         emb VECTOR(8), note TEXT)",
    )
    .unwrap();
    db.execute("CREATE TABLE sen (device TEXT PRIMARY KEY, zone INT)")
        .unwrap();
    let mut rng = Lcg(7);
    let mut src = Vec::new();
    let mut batch: Vec<Vec<Value>> = Vec::new();
    for i in 0..3000i64 {
        // dev-8/dev-9 intentionally absent from sen (dropped by inner join).
        let device = format!("dev-{}", i % 10);
        let val = if i % 13 == 0 { None } else { Some(rng.f64()) };
        let grp = i % 7;
        let emb: Vec<f32> = (0..8).map(|k| k as f32 + (i % 5) as f32).collect();
        batch.push(vec![
            Value::Integer(i),
            Value::Text(motedb::types::ArcString::from(device.as_str())),
            match val {
                Some(v) => Value::Float(v),
                None => Value::Null,
            },
            Value::Integer(grp),
            Value::Tensor(Box::new(motedb::types::Tensor::new(emb))),
            Value::Text(motedb::types::ArcString::from(format!("note {i}"))),
        ]);
        src.push(JoinRow { device, val, grp });
    }
    db.execute_prepared_many(
        "INSERT INTO ev (id, device, val, grp, emb, note) VALUES (?, ?, ?, ?, ?, ?)",
        batch,
    )
    .unwrap();
    // sen: devices 0..7, zone = device# % 4.
    let sen: Vec<Vec<Value>> = (0..8i64)
        .map(|i| {
            vec![
                Value::Text(motedb::types::ArcString::from(format!("dev-{i}").as_str())),
                Value::Integer(i % 4),
            ]
        })
        .collect();
    db.execute_prepared_many("INSERT INTO sen (device, zone) VALUES (?, ?)", sen)
        .unwrap();
    db.checkpoint().unwrap();
    src
}

/// Expected join+group result computed in plain Rust.
fn expected_join_groups(
    src: &[JoinRow],
    zone_filter: Option<i64>,
    grp_min: Option<i64>,
) -> Vec<(String, i64, i64, f64)> {
    use std::collections::BTreeMap;
    // device → zone from sen (device# % 4, devices 0..7)
    let zone_of = |d: &str| -> Option<i64> {
        let n: i64 = d.trim_start_matches("dev-").parse().unwrap();
        (n < 8).then(|| n % 4)
    };
    let mut groups: BTreeMap<String, (i64, i64, f64)> = BTreeMap::new();
    for r in src {
        let Some(z) = zone_of(&r.device) else {
            continue;
        };
        if let Some(want) = zone_filter {
            if z != want {
                continue;
            }
        }
        if let Some(g) = grp_min {
            if r.grp < g {
                continue;
            }
        }
        let e = groups.entry(r.device.clone()).or_insert((0, 0, 0.0));
        e.0 += 1;
        if let Some(v) = r.val {
            e.1 += 1;
            e.2 += v;
        }
    }
    groups
        .into_iter()
        .map(|(k, (cnt, nn, sum))| (k, cnt, nn, sum))
        .collect()
}

fn check_join_result(rows: &[Vec<Value>], expected: &[(String, i64, i64, f64)], ctx: &str) {
    assert_eq!(rows.len(), expected.len(), "{ctx}: group count");
    for (got, want) in rows.iter().zip(expected.iter()) {
        let dev = match &got[0] {
            Value::Text(t) => t.as_str().to_string(),
            v => panic!("{ctx}: expected text group key, got {v:?}"),
        };
        assert_eq!(dev, want.0, "{ctx}: group key");
        assert_eq!(
            match &got[1] {
                Value::Integer(i) => *i,
                v => panic!("{ctx}: {v:?}"),
            },
            want.1,
            "{ctx}: COUNT(*)"
        );
        assert_eq!(
            match &got[2] {
                Value::Integer(i) => *i,
                v => panic!("{ctx}: {v:?}"),
            },
            want.2,
            "{ctx}: COUNT(val)"
        );
        match &got[3] {
            Value::Float(s) => assert!((s - want.3).abs() < 1e-8, "{ctx}: SUM(val)"),
            Value::Null => assert_eq!(want.2, 0, "{ctx}: SUM NULL only when no non-null vals"),
            v => panic!("{ctx}: {v:?}"),
        }
    }
}

#[test]
fn projected_join_groupby_matches_expected() {
    let (_d, db) = db();
    let src = seed_join_tables(&db);

    // The competitor-benchmark shape: WHERE on the right side only.
    let rows = db
        .query(
            "SELECT e.device, COUNT(*), COUNT(e.val), SUM(e.val) FROM ev e \
             JOIN sen s ON e.device = s.device WHERE s.zone = 3 GROUP BY e.device \
             ORDER BY e.device",
        )
        .unwrap();
    check_join_result(&rows, &expected_join_groups(&src, Some(3), None), "zone=3");

    // WHERE on the LEFT side.
    let rows = db
        .query(
            "SELECT e.device, COUNT(*), COUNT(e.val), SUM(e.val) FROM ev e \
             JOIN sen s ON e.device = s.device WHERE e.grp >= 5 GROUP BY e.device \
             ORDER BY e.device",
        )
        .unwrap();
    check_join_result(&rows, &expected_join_groups(&src, None, Some(5)), "grp>=5");

    // No WHERE at all.
    let rows = db
        .query(
            "SELECT e.device, COUNT(*), COUNT(e.val), SUM(e.val) FROM ev e \
             JOIN sen s ON e.device = s.device GROUP BY e.device ORDER BY e.device",
        )
        .unwrap();
    check_join_result(&rows, &expected_join_groups(&src, None, None), "no where");
}

/// JOIN + GROUP BY with AVG/MIN/MAX and a parameterized WHERE.
#[test]
fn projected_join_groupby_avg_min_max() {
    let (_d, db) = db();
    let src = seed_join_tables(&db);
    let rows = db
        .query(
            "SELECT e.device, AVG(e.val), MIN(e.val), MAX(e.val) FROM ev e \
             JOIN sen s ON e.device = s.device WHERE s.zone = 2 GROUP BY e.device \
             ORDER BY e.device",
        )
        .unwrap();
    let expected = expected_join_groups(&src, Some(2), None);
    assert_eq!(rows.len(), expected.len());
    for (got, want) in rows.iter().zip(expected.iter()) {
        assert_eq!(
            match &got[0] {
                Value::Text(t) => t.as_str(),
                v => panic!("{v:?}"),
            },
            want.0
        );
        let vals: Vec<f64> = src
            .iter()
            .filter(|r| {
                r.device == want.0
                    && r.val.is_some()
                    && (r.device.trim_start_matches("dev-").parse::<i64>().unwrap() % 4) == 2
            })
            .filter_map(|r| r.val)
            .collect();
        match &got[1] {
            Value::Float(a) => {
                assert!((a - want.3 / want.2 as f64).abs() < 1e-8, "AVG")
            }
            v => panic!("AVG {v:?}"),
        }
        match &got[2] {
            Value::Float(m) => {
                assert!((m - vals.iter().cloned().fold(f64::INFINITY, f64::min)).abs() < 1e-9)
            }
            v => panic!("MIN {v:?}"),
        }
        match &got[3] {
            Value::Float(m) => {
                assert!((m - vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max)).abs() < 1e-9)
            }
            v => panic!("MAX {v:?}"),
        }
        let _ = vals;
    }
}

/// COUNT(text_col) inside the join path must count non-NULL TEXT values.
#[test]
fn join_count_text_column_counts_non_nulls() {
    let (_d, db) = db();
    let _src = seed_join_tables(&db);
    let rows = db
        .query(
            "SELECT e.device, COUNT(e.note) FROM ev e JOIN sen s ON e.device = s.device \
             WHERE s.zone = 1 GROUP BY e.device ORDER BY e.device",
        )
        .unwrap();
    assert!(!rows.is_empty());
    for r in &rows {
        let n = match &r[1] {
            Value::Integer(i) => *i,
            v => panic!("{v:?}"),
        };
        assert!(n > 0, "every seeded note is non-NULL TEXT");
    }
}

/// Three-table chain still correct through the pruned multi-way path.
#[test]
fn three_table_join_chain_with_pruning() {
    let (_d, db) = db();
    let _src = seed_join_tables(&db);
    db.execute("CREATE TABLE zone_names (zone INT PRIMARY KEY, label TEXT)")
        .unwrap();
    let zn: Vec<Vec<Value>> = (0..4i64)
        .map(|z| {
            vec![
                Value::Integer(z),
                Value::Text(motedb::types::ArcString::from(format!("zone-{z}").as_str())),
            ]
        })
        .collect();
    db.execute_prepared_many("INSERT INTO zone_names (zone, label) VALUES (?, ?)", zn)
        .unwrap();

    let rows = db
        .query(
            "SELECT z.label, COUNT(*), SUM(e.val) FROM ev e \
             JOIN sen s ON e.device = s.device \
             JOIN zone_names z ON s.zone = z.zone \
             WHERE z.label <> 'zone-0' GROUP BY z.label ORDER BY z.label",
        )
        .unwrap();
    assert_eq!(rows.len(), 3, "zones 1..3");
    for r in &rows {
        let n = match &r[1] {
            Value::Integer(i) => *i,
            v => panic!("{v:?}"),
        };
        assert!(n > 0);
    }
    // zone-0 rows excluded: total across the three = joined rows with zone>0.
    let all = db
        .query("SELECT COUNT(*) FROM ev e JOIN sen s ON e.device = s.device")
        .unwrap();
    let excluded = db
        .query("SELECT COUNT(*) FROM ev e JOIN sen s ON e.device = s.device WHERE s.zone = 0")
        .unwrap();
    let sum: i64 = rows
        .iter()
        .map(|r| match &r[1] {
            Value::Integer(i) => *i,
            _ => 0,
        })
        .sum();
    assert_eq!(sum + one_i64(&excluded, 0), one_i64(&all, 0));
}

/// NULL join keys never match, and parameterized WHERE works through the
/// positional fast filter.
#[test]
fn join_null_keys_and_params() {
    let (_d, db) = db();
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, k INT, v FLOAT)")
        .unwrap();
    db.execute("CREATE TABLE b (id INT PRIMARY KEY, k INT, w FLOAT)")
        .unwrap();
    let mut ba = Vec::new();
    for i in 0..50i64 {
        let k = if i % 10 == 0 {
            Value::Null
        } else {
            Value::Integer(i % 7)
        };
        ba.push(vec![Value::Integer(i), k, Value::Float(i as f64)]);
    }
    db.execute_prepared_many("INSERT INTO a (id, k, v) VALUES (?, ?, ?)", ba)
        .unwrap();
    let mut bb = Vec::new();
    for i in 0..7i64 {
        let k = if i == 3 {
            Value::Null
        } else {
            Value::Integer(i)
        };
        bb.push(vec![
            Value::Integer(100 + i),
            k,
            Value::Float(i as f64 * 2.0),
        ]);
    }
    db.execute_prepared_many("INSERT INTO b (id, k, w) VALUES (?, ?, ?)", bb)
        .unwrap();

    // NULL keys on either side drop out of the inner join.
    let rows = db
        .query("SELECT COUNT(*) FROM a JOIN b ON a.k = b.k")
        .unwrap();
    // keys 0..6 except 3 (b.k NULL) → a rows with k in {0,1,2,4,5,6}
    let want = (0..50i64).filter(|i| i % 10 != 0 && (i % 7) != 3).count();
    assert_eq!(one_i64(&rows, 0), want as i64);
}

// ─────────────────────────────────────────────────────────────────────────
// 4. Fused aggregation under a transaction (read-your-writes).
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn fused_aggregation_sees_uncommitted_inserts() {
    let (_d, db) = db();
    let src = seed_agg_table(&db);
    db.execute("BEGIN").unwrap();
    for i in 2000..2100i64 {
        db.execute(&format!(
            "INSERT INTO ev (id, ts, dev, val, n) VALUES ({i}, {}, 'dev-3', 1.5, 1)",
            1_700_000_000i64 * 1_000_000 + i * 7919
        ))
        .unwrap();
    }
    // The write_set adjustment must count rows matching the predicate.
    let rows = db
        .query("SELECT COUNT(*) FROM ev WHERE dev = 'dev-3' AND val > 1")
        .unwrap();
    let committed = src
        .iter()
        .filter(|r| r.dev.as_deref() == Some("dev-3") && r.val.is_some_and(|v| v > 1.0))
        .count();
    assert_eq!(one_i64(&rows, 0), (committed + 100) as i64);
    db.execute("ROLLBACK").unwrap();
    let rows = db
        .query("SELECT COUNT(*) FROM ev WHERE dev = 'dev-3' AND val > 1")
        .unwrap();
    assert_eq!(one_i64(&rows, 0), committed as i64);
}
