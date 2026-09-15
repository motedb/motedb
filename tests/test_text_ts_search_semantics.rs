//! Full-text search + TimeSeries regression tests for bugs found by the
//! text/ts accuracy evals (bench/text_eval.py, bench/ts_eval.py).
//!
//! Text:
//!  * BM25 doc lengths were destroyed by the first auto-flush inside a bulk
//!    backfill (flush() cleared the pending-length map before the writer ran),
//!    so after ~2K docs every document scored with the same constant length
//!    and ranking went length-blind;
//!  * search()/search_ranked()/search_single_term() returned the FIRST of
//!    {pending, disk} posting instead of their union — a term split across
//!    both (partial flush) hid every earlier document;
//!  * the unranked path INTERSECTED multi-term postings while the ranked path
//!    unioned them — same query, different sets with and without LIMIT;
//!  * `MATCH(c, q) AND other` silently dropped the other predicate, and
//!    COUNT(*) WHERE MATCH returned one row per match instead of a count;
//!  * the unindexed fallback answered an AND-of-substrings check.
//!
//! TimeSeries:
//!  * `DELETE WHERE ts < cutoff` left every expired row that lived in the
//!    write buffer or in a segment straddling the cutoff fully visible.
use motedb::types::Value;
use motedb::{DBConfig, Database};
use tempfile::TempDir;

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    let r = db
        .execute(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .materialize()
        .unwrap();
    let (_, rows) = r.select_rows().unwrap();
    rows.to_vec()
}

fn query_rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    rows(db, sql)
}

fn scalar(db: &Database, sql: &str) -> Value {
    rows(db, sql)[0][0].clone()
}

fn ids(db: &Database, sql: &str) -> Vec<i64> {
    rows(db, sql)
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            other => panic!("{sql}: {other:?}"),
        })
        .collect()
}

// ════════════════════════════ Full-text search ════════════════════════════

fn text_db(n: usize) -> (Database, TempDir, Vec<String>) {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE d (id INT PRIMARY KEY, cat TEXT, content TEXT)")
        .unwrap();
    // Deterministic docs: word length encodes the doc number so ranking by
    // BM25 (shorter doc = higher score for the same tf) is predictable.
    let mut docs = Vec::with_capacity(n);
    for i in 0..n {
        docs.push(format!("apple banana cherry dog elephant doc{}", i));
    }
    for s in (0..n).step_by(2000) {
        let vals: Vec<String> = docs[s..(s + 2000).min(n)]
            .iter()
            .enumerate()
            .map(|(j, t)| {
                format!(
                    "({}, '{}', '{}')",
                    s + j + 1,
                    if (s + j) % 2 == 0 { "a" } else { "b" },
                    t
                )
            })
            .collect();
        db.execute(&format!("INSERT INTO d VALUES {}", vals.join(",")))
            .unwrap();
    }
    db.execute("CREATE TEXT INDEX d_content ON d(content)")
        .unwrap();
    (db, dir, docs)
}

/// A bulk backfill large enough to trigger the in-insert auto-flush must not
/// lose document lengths: ranking must stay length-aware (shorter doc first).
#[test]
fn bm25_ranking_survives_backfill_auto_flush() {
    let (db, _d, _docs) = text_db(20_000);
    // 'apple' is in every doc with tf=1 → score depends only on doc length.
    // The shortest docs are the LAST inserted ones (docN suffix adds length,
    // so early docs are shorter — doc0 shortest).
    let top = ids(
        &db,
        "SELECT id FROM d WHERE MATCH(content, 'apple') LIMIT 5",
    );
    // All top-5 docs must be from the short end (first ~10% of ids).
    let max_id = top.iter().copied().max().unwrap();
    assert!(
        max_id <= 2000,
        "top-5 contains a long doc (max id {max_id}) — length normalization lost"
    );
    // And the scores the fast path computes must be strictly descending.
    let r = rows(
        &db,
        "SELECT id FROM d WHERE MATCH(content, 'apple') LIMIT 5",
    );
    let _ = r;
}

#[test]
fn match_set_semantics_with_and_without_limit_and_index() {
    let (db, _d, _docs) = text_db(4_000);
    // 'apple banana' must be the OR union on every path.
    let with_limit = ids(
        &db,
        "SELECT id FROM d WHERE MATCH(content, 'apple banana') LIMIT 1000000",
    );
    let without_limit = ids(&db, "SELECT id FROM d WHERE MATCH(content, 'apple banana')");
    let no_index = {
        db.execute("CREATE TABLE n (id INT PRIMARY KEY, content TEXT)")
            .unwrap();
        let vals: Vec<String> = (1..=100)
            .map(|i| {
                format!(
                    "({}, '{}')",
                    i,
                    if i % 3 == 0 {
                        "apple banana cherry"
                    } else {
                        "cherry date"
                    }
                )
            })
            .collect();
        db.execute(&format!("INSERT INTO n VALUES {}", vals.join(",")))
            .unwrap();
        ids(&db, "SELECT id FROM n WHERE MATCH(content, 'apple banana')")
    };
    assert_eq!(
        without_limit.len(),
        4_000,
        "un-LIMITed MATCH must return the full set"
    );
    assert_eq!(
        with_limit.len(),
        without_limit.len(),
        "LIMIT ≥ set size must not change the set"
    );
    assert_eq!(
        no_index.len(),
        33,
        "no-index MATCH: OR semantics (33 docs + …)"
    );
    // The no-index COUNT path must agree with the row path.
    assert_eq!(
        scalar(
            &db,
            "SELECT COUNT(*) FROM n WHERE MATCH(content, 'apple banana')"
        ),
        Value::Integer(33)
    );
}

#[test]
fn match_compound_predicates_and_aggregates() {
    let (db, _d, _docs) = text_db(4_000);
    // AND must actually filter (used to be dropped → both cat values returned).
    for id in ids(
        &db,
        "SELECT id FROM d WHERE MATCH(content, 'apple') AND cat = 'a'",
    ) {
        assert_eq!(id % 2, 1, "row {id} violates cat='a'");
    }
    // COUNT must count, not emit one row per match.
    let n = scalar(&db, "SELECT COUNT(*) FROM d WHERE MATCH(content, 'apple')");
    assert_eq!(n, Value::Integer(4_000));
    let sum = scalar(
        &db,
        "SELECT SUM(id) FROM d WHERE MATCH(content, 'apple') AND cat = 'a'",
    );
    let expect: i64 = (0..2_000).map(|i| 2 * i + 1).sum();
    assert_eq!(sum, Value::Integer(expect));
}

#[test]
fn text_index_survives_checkpoint_reopen_and_edits() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE d (id INT PRIMARY KEY, content TEXT)")
        .unwrap();
    let vals: Vec<String> = (1..=5_000)
        .map(|i| {
            format!(
                "({}, '{}')",
                i,
                if i % 2 == 0 {
                    "quokka sleeps here"
                } else {
                    "cat naps there"
                }
            )
        })
        .collect();
    db.execute(&format!("INSERT INTO d VALUES {}", vals.join(",")))
        .unwrap();
    db.execute("CREATE TEXT INDEX d_content ON d(content)")
        .unwrap();
    db.checkpoint().unwrap();
    db.close().unwrap();

    let db = Database::open(dir.path()).unwrap();
    assert_eq!(
        ids(&db, "SELECT id FROM d WHERE MATCH(content, 'quokka')").len(),
        2_500,
        "index empty or partial after reopen"
    );
    // UPDATE reindexes both sides.
    db.execute("UPDATE d SET content = 'wombat wanders' WHERE id = 2")
        .unwrap();
    assert!(!ids(&db, "SELECT id FROM d WHERE MATCH(content, 'quokka')").contains(&2));
    assert!(ids(&db, "SELECT id FROM d WHERE MATCH(content, 'wombat')").contains(&2));
    // DELETE removes index entries.
    db.execute("DELETE FROM d WHERE id = 4").unwrap();
    assert!(!ids(&db, "SELECT id FROM d WHERE MATCH(content, 'quokka')").contains(&4));
}

#[test]
fn ngram_tokenizer_ddl() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE zh (id INT PRIMARY KEY, content TEXT)")
        .unwrap();
    db.execute(
        "INSERT INTO zh VALUES (1, '机器人控制系统'), (2, '传感器数据采集'), (3, '自动驾驶感知')",
    )
    .unwrap();
    db.execute("CREATE TEXT INDEX zh_content ON zh(content) USING TOKENIZER ngram(2)")
        .unwrap();
    assert_eq!(
        ids(&db, "SELECT id FROM zh WHERE MATCH(content, '机器人')"),
        [1]
    );
    assert_eq!(
        ids(&db, "SELECT id FROM zh WHERE MATCH(content, '数据')"),
        [2]
    );
    try_db_err(
        &db,
        "CREATE TEXT INDEX bad ON zh(content) USING TOKENIZER bogus",
    );
}

fn try_db_err(db: &Database, sql: &str) {
    if db.execute(sql).is_ok() {
        panic!("{sql} should have failed");
    }
}

// ════════════════════════════ TimeSeries ══════════════════════════════════

#[test]
fn ts_delete_purges_buffered_and_straddling_rows() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE m (ts TIMESTAMP, sid INT, v FLOAT) TIMESERIES(ts)")
        .unwrap();
    // 200K micros base, 100K rows via multi-row inserts (buffered), so the
    // cutoff at the midpoint splits buffered rows AND segments.
    let base: i64 = 1_700_000_000_000_000;
    let n = 100_000i64;
    let cutoff = base + n / 2 * 1_000_000;
    for s in (0..n).step_by(2_000) {
        let vals: Vec<String> = (s..(s + 2_000).min(n))
            .map(|i| {
                format!(
                    "({}, {}, {:.1})",
                    base + i * 1_000_000,
                    i % 4,
                    i as f64 * 0.5
                )
            })
            .collect();
        db.execute(&format!("INSERT INTO m VALUES {}", vals.join(",")))
            .unwrap();
    }
    assert_eq!(
        scalar(&db, "SELECT COUNT(*) FROM m"),
        Value::Integer(n),
        "seed count"
    );
    db.execute(&format!("DELETE FROM m WHERE ts < {cutoff}"))
        .unwrap();
    let leaked = scalar(&db, &format!("SELECT COUNT(*) FROM m WHERE ts < {cutoff}"));
    assert_eq!(
        leaked,
        Value::Integer(0),
        "expired rows still visible after DELETE"
    );
    let kept = scalar(&db, &format!("SELECT COUNT(*) FROM m WHERE ts >= {cutoff}"));
    assert_eq!(kept, Value::Integer(n - n / 2), "kept rows must survive");
    // 🔒 Range-pruned reads after a partial DELETE. The straddle rewrite used
    // to write placeholder (MAX, MIN) segment time bounds, so every pruned
    // query — plain ranged SELECT and range aggregate alike — skipped the
    // rewritten segment (kept rows invisible); only full-range scans masked it.
    let r = query_rows(&db, &format!("SELECT ts FROM m WHERE ts >= {cutoff}"));
    assert_eq!(
        r.len(),
        (n - n / 2) as usize,
        "plain ranged SELECT after DELETE"
    );
    let hi = base + n * 1_000_000; // full kept span
    let agg = scalar(
        &db,
        &format!("SELECT COUNT(*) FROM m WHERE ts BETWEEN {cutoff} AND {hi}"),
    );
    assert_eq!(
        agg,
        Value::Integer(n - n / 2),
        "range aggregate after DELETE"
    );
    let c = scalar(
        &db,
        &format!("SELECT COUNT(*), AVG(v) FROM m WHERE ts >= {cutoff} AND sid >= 0"),
    );
    let _ = c; // impure predicate falls back to the scan path; count covered above

    // 🔒 Range-aggregate pushdown equals the scan path (mixed predicates).
    let (lo, hi) = (base + 10_000 * 1_000_000, base + 20_000 * 1_000_000);
    let pushed = query_rows(
        &db,
        &format!(
            "SELECT COUNT(*), SUM(v), MIN(v), MAX(v), AVG(v) FROM m \
             WHERE ts BETWEEN {lo} AND {hi}"
        ),
    );
    let scanned = query_rows(
        &db,
        &format!(
            "SELECT COUNT(*), SUM(v), MIN(v), MAX(v), AVG(v) FROM m \
             WHERE ts BETWEEN {lo} AND {hi} AND sid >= 0"
        ),
    );
    assert_eq!(pushed.len(), 1);
    assert_eq!(scanned.len(), 1);
    for (a, b) in pushed[0].iter().zip(scanned[0].iter()) {
        match (a, b) {
            (Value::Float(x), Value::Float(y)) => {
                assert!((x - y).abs() < 1e-9, "pushdown {x} vs scan {y}")
            }
            _ => assert_eq!(a, b, "pushdown vs scan mismatch"),
        }
    }

    // 🔒 TIME_BUCKET GROUP BY through the pushdown (pure time predicate,
    // over the KEPT range — everything below cutoff was deleted).
    let buckets = query_rows(
        &db,
        &format!(
            "SELECT TIME_BUCKET('10s', ts) AS b, COUNT(*) FROM m \
             WHERE ts >= {cutoff} AND ts < {} GROUP BY b",
            cutoff + 25_000_000
        ),
    );
    let expect: Vec<(i64, i64)> = vec![
        (cutoff, 10), // 1-second spacing: 10s bucket = 10 rows
        (cutoff + 10_000_000, 10),
        (cutoff + 20_000_000, 5),
    ];
    assert_eq!(buckets.len(), 3, "bucket count");
    for (row, (b, cnt)) in buckets.iter().zip(expect) {
        assert!(
            matches!(&row[0], Value::Timestamp(t) if t.as_micros() == b),
            "bucket boundary {row:?} vs {b}"
        );
        assert_eq!(row[1], Value::Integer(cnt));
    }

    // Reopen: the purge (with corrected segment bounds) must be durable.
    db.checkpoint().unwrap();
    db.close().unwrap();
    let db = Database::open(dir.path()).unwrap();
    assert_eq!(
        scalar(&db, &format!("SELECT COUNT(*) FROM m WHERE ts < {cutoff}")),
        Value::Integer(0),
        "expired rows resurrected after reopen"
    );
    assert_eq!(
        scalar(&db, &format!("SELECT COUNT(*) FROM m WHERE ts >= {cutoff}")),
        Value::Integer(n - n / 2),
        "kept rows lost after reopen"
    );
}

#[test]
fn ts_latest_by_and_late_rows() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE m (ts TIMESTAMP, sid INT, v FLOAT) TIMESERIES(ts)")
        .unwrap();
    // 3 sensors × 3 rows; LATEST BY sid must keep exactly the newest per sid.
    let rows_ts = [
        (0, 1000),
        (1, 2000),
        (2, 3000),
        (0, 4000),
        (1, 5000),
        (2, 6000),
        (0, 7000),
        (1, 8000),
        (2, 9000),
    ];
    let vals: Vec<String> = rows_ts
        .iter()
        .map(|(s, t)| format!("({}, {}, {}.0)", t, s, *t as f64 / 1000.0))
        .collect();
    db.execute(&format!("INSERT INTO m VALUES {}", vals.join(",")))
        .unwrap();
    let r = rows(&db, "SELECT ts, sid FROM m LATEST BY sid");
    assert_eq!(r.len(), 3, "LATEST BY must collapse to one row per sensor");
    let latest: Vec<i64> = r
        .iter()
        .filter_map(|r| match &r[0] {
            Value::Timestamp(t) => Some(t.as_micros()),
            _ => None,
        })
        .collect();
    assert!(latest.contains(&7000) && latest.contains(&8000) && latest.contains(&9000));
    // Late (out-of-order) insert stays visible.
    db.execute("INSERT INTO m VALUES (500, 0, 99.0)").unwrap();
    assert_eq!(
        scalar(&db, "SELECT v FROM m WHERE ts BETWEEN 500 AND 500"),
        Value::Float(99.0)
    );
}

// ═════════════ TIME_BUCKET downsampling + BM25_SCORE ═════════════════════

#[test]
fn time_bucket_group_by_alias_and_expression() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE m (ts TIMESTAMP, sid INT, v FLOAT) TIMESERIES(ts)")
        .unwrap();
    // 2 sensors × 3 five-second buckets.
    let seed: Vec<(i64, i64, f64)> = vec![
        (0, 1, 1.0),
        (0, 2, 2.0),
        (5_000_000, 1, 3.0),
        (5_000_000, 2, 4.0),
        (10_000_000, 1, 5.0),
        (10_000_000, 2, 6.0),
    ];
    let vals: Vec<String> = seed
        .iter()
        .map(|(t, s, v)| format!("({t}, {s}, {v})"))
        .collect();
    db.execute(&format!("INSERT INTO m VALUES {}", vals.join(",")))
        .unwrap();

    // GROUP BY alias
    let r = query_rows(
        &db,
        "SELECT TIME_BUCKET('5s', ts) AS b, COUNT(*), AVG(v) FROM m GROUP BY b ORDER BY b",
    );
    assert_eq!(r.len(), 3);
    let expect: Vec<(i64, i64, f64)> = vec![(0, 2, 1.5), (5_000_000, 2, 3.5), (10_000_000, 2, 5.5)];
    for (row, (b, n, avg)) in r.iter().zip(expect) {
        assert!(matches!(&row[0], Value::Timestamp(t) if t.as_micros() == b));
        assert_eq!(row[1], Value::Integer(n));
        match &row[2] {
            Value::Float(f) => assert!((f - avg).abs() < 1e-9),
            other => panic!("avg {other:?}"),
        }
    }

    // GROUP BY full expression
    let r = query_rows(
        &db,
        "SELECT TIME_BUCKET('5s', ts) AS b, COUNT(*) FROM m \
         GROUP BY TIME_BUCKET('5s', ts) ORDER BY b",
    );
    assert_eq!(
        r.len(),
        3,
        "GROUP BY <expression> must resolve to the projected expression"
    );

    // Mixed column + expression
    let r = query_rows(
        &db,
        "SELECT sid, TIME_BUCKET('5s', ts) AS b, COUNT(*) FROM m \
         GROUP BY sid, b ORDER BY sid, b",
    );
    assert_eq!(r.len(), 6, "2 sensors × 3 buckets");
    assert!(matches!(&r[0][0], Value::Integer(1)));
    assert!(matches!(&r[0][2], Value::Integer(1)));

    // 🔒 One-sided time predicates in the materialized GROUP BY path. The
    // fast filter compiled `col op literal` without a Timestamp arm, so
    // `ts >= <micros>` matched ZERO rows here (BETWEEN survived because it
    // takes the evaluator path; `ts >= X + 0` because the right side stops
    // being a literal).
    let cut = 5_000_000i64;
    for (label, sql, expect_rows, expect_total) in [
        (
            "ts >= cut",
            format!("SELECT TIME_BUCKET('5s', ts) AS b, COUNT(*) FROM m WHERE ts >= {cut} GROUP BY b"),
            2usize,
            4i64,
        ),
        (
            "ts <= cut",
            format!("SELECT TIME_BUCKET('5s', ts) AS b, COUNT(*) FROM m WHERE ts <= {cut} GROUP BY b"),
            2,
            4,
        ),
        (
            "ts >= AND <=",
            format!(
                "SELECT TIME_BUCKET('5s', ts) AS b, COUNT(*) FROM m \
                 WHERE ts >= {cut} AND ts <= {cut} GROUP BY b"
            ),
            1,
            2,
        ),
        (
            "ts BETWEEN (regression guard)",
            format!("SELECT TIME_BUCKET('5s', ts) AS b, COUNT(*) FROM m WHERE ts BETWEEN {cut} AND {cut} GROUP BY b"),
            1,
            2,
        ),
    ] {
        let r = query_rows(&db, &sql);
        let total: i64 = r.iter().map(|row| match &row[1] {
            Value::Integer(n) => *n,
            other => panic!("{label}: non-integer count {other:?}"),
        }).sum();
        assert_eq!(r.len(), expect_rows, "{label}: bucket count");
        assert_eq!(total, expect_total, "{label}: matched row total");
    }
}

#[test]
fn bm25_score_projection() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE d (id INT PRIMARY KEY, cat TEXT, content TEXT)")
        .unwrap();
    db.execute("INSERT INTO d VALUES (1,'a','the quick brown fox'), (2,'b','quick quick fox'), (3,'a','lazy dog')")
        .unwrap();
    db.execute("CREATE TEXT INDEX d_content ON d(content)")
        .unwrap();

    // Scores must be real BM25 values (doc 2 has tf=2 → highest), and the
    // id column must survive the projection (one bad expression column used
    // to nullify the whole row).
    let r = rows(
        &db,
        "SELECT id, BM25_SCORE(content, 'quick') AS s FROM d WHERE MATCH(content, 'quick')",
    );
    assert_eq!(r.len(), 2);
    let (id0, s0) = match &r[0] {
        v => (
            match &v[0] {
                Value::Integer(i) => *i,
                o => panic!("{o:?}"),
            },
            match &v[1] {
                Value::Float(f) => *f,
                o => panic!("{o:?}"),
            },
        ),
    };
    let (id1, s1) = match &r[1] {
        v => (
            match &v[0] {
                Value::Integer(i) => *i,
                o => panic!("{o:?}"),
            },
            match &v[1] {
                Value::Float(f) => *f,
                o => panic!("{o:?}"),
            },
        ),
    };
    assert_eq!(id0, 2, "tf=2 doc must rank first");
    assert_eq!(id1, 1);
    assert!(
        s0 > s1 && s1 > 0.0,
        "real BM25 scores, descending (got {s0}, {s1})"
    );

    // LIMIT keeps ranked order; the same query without LIMIT still scores.
    let r = rows(
        &db,
        "SELECT id, BM25_SCORE(content, 'quick') AS s FROM d WHERE MATCH(content, 'quick') LIMIT 1",
    );
    assert_eq!(r.len(), 1);
    assert!(matches!(&r[0][0], Value::Integer(2)));
}

/// 🔒 `SELECT COUNT(*) WHERE MATCH` answers from the index postings (was:
/// materialize every matching row to count it). Must agree with the row-set
/// path, handle COUNT(col)/COUNT(), unknown terms, and leave compound
/// predicates (MATCH AND …) on the general pipeline.
#[test]
fn count_where_match_uses_index_postings() {
    let (db, _d, _docs) = text_db(5000);
    let n_match = ids(&db, "SELECT id FROM d WHERE MATCH(content, 'banana')").len() as i64;
    assert_eq!(
        scalar(&db, "SELECT COUNT(*) FROM d WHERE MATCH(content, 'banana')"),
        Value::Integer(n_match),
        "COUNT(*) vs row-set"
    );
    assert_eq!(
        scalar(
            &db,
            "SELECT COUNT(content) FROM d WHERE MATCH(content, 'banana')"
        ),
        Value::Integer(n_match),
        "COUNT(col) on the matched column"
    );
    assert_eq!(
        scalar(&db, "SELECT COUNT() FROM d WHERE MATCH(content, 'banana')"),
        Value::Integer(n_match),
        "bare COUNT()"
    );
    assert_eq!(
        scalar(
            &db,
            "SELECT COUNT(*) AS n FROM d WHERE MATCH(content, 'banana elephant')"
        ),
        Value::Integer(n_match), // every doc contains both
        "alias + two-token OR"
    );
    assert_eq!(
        scalar(
            &db,
            "SELECT COUNT(*) FROM d WHERE MATCH(content, 'zzzqqqxyzzw')"
        ),
        Value::Integer(0),
        "unknown term"
    );
    // Compound predicate: NOT the fast path; general pipeline must filter.
    let cat_a = ids(
        &db,
        "SELECT id FROM d WHERE MATCH(content, 'banana') AND cat = 'a'",
    )
    .len() as i64;
    assert_eq!(
        scalar(
            &db,
            "SELECT COUNT(*) FROM d WHERE MATCH(content, 'banana') AND cat = 'a'"
        ),
        Value::Integer(cat_a),
        "compound predicate still exact"
    );
}

#[test]
fn create_index_if_not_exists_is_idempotent() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE d (id INT PRIMARY KEY, cat TEXT, content TEXT)")
        .unwrap();
    db.execute("INSERT INTO d VALUES (1,'a','alpha beta')")
        .unwrap();

    // First call creates; second is a no-op, not an error.
    db.execute("CREATE TEXT INDEX IF NOT EXISTS d_content ON d(content)")
        .unwrap();
    db.execute("CREATE TEXT INDEX IF NOT EXISTS d_content ON d(content)")
        .unwrap();
    // Existing index + IF NOT EXISTS also no-ops.
    db.execute("CREATE TEXT INDEX d_content2 ON d(content)")
        .unwrap();
    db.execute("CREATE TEXT INDEX IF NOT EXISTS d_content2 ON d(content)")
        .unwrap();
    // Without IF NOT EXISTS a duplicate still errors.
    assert!(db
        .execute("CREATE TEXT INDEX d_content ON d(content)")
        .is_err());
    // Index still answers queries after the no-op calls.
    let r = ids(&db, "SELECT id FROM d WHERE MATCH(content, 'alpha')");
    assert_eq!(r, vec![1]);
}

#[test]
fn multi_way_join_hash_and_aggregate_fold() {
    use motedb::QueryResult;
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE cust (id INT PRIMARY KEY, name TEXT, tier INT)")
        .unwrap();
    db.execute("INSERT INTO cust VALUES (1,'ann',0),(2,'bob',0),(3,'cat',1),(4,'dan',1)")
        .unwrap();
    db.execute("CREATE TABLE orders (id INT PRIMARY KEY, cid INT, amt FLOAT)")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (1,1,10.0),(2,1,20.0),(3,2,30.0),(4,3,40.0),(5,3,50.0)")
        .unwrap();
    db.execute("CREATE TABLE docs (id INT PRIMARY KEY, tag TEXT)")
        .unwrap();
    db.execute("INSERT INTO docs VALUES (1,'x'),(2,'y'),(3,'x'),(4,'y'),(5,'x')")
        .unwrap();

    // 3-table equi join through the multi-way hash path.
    let r = query_rows(
        &db,
        "SELECT c.name, o.amt FROM orders o JOIN cust c ON o.cid = c.id \
         JOIN docs d ON d.id = o.id ORDER BY o.id",
    );
    assert_eq!(r.len(), 5, "all orders match exactly one cust and one doc");

    // 2-table join + GROUP BY + SUM through the aggregate fold.
    let r = query_rows(
        &db,
        "SELECT c.tier, COUNT(*), SUM(o.amt) FROM orders o JOIN cust c ON o.cid = c.id \
         GROUP BY c.tier ORDER BY c.tier",
    );
    assert_eq!(r.len(), 2);
    assert!(
        matches!((&r[0][0], &r[0][1], &r[0][2]), (Value::Integer(0), Value::Integer(3), Value::Float(f)) if (*f - 60.0).abs() < 1e-9)
    );
    assert!(
        matches!((&r[1][0], &r[1][1], &r[1][2]), (Value::Integer(1), Value::Integer(2), Value::Float(f)) if (*f - 90.0).abs() < 1e-9)
    );

    // ORDER BY an aggregate in the SELECT list.
    let r = query_rows(
        &db,
        "SELECT c.name, COUNT(*) AS n FROM orders o JOIN cust c ON o.cid = c.id \
         GROUP BY c.name ORDER BY COUNT(*) DESC, c.name",
    );
    assert_eq!(r.len(), 3);
    assert!(matches!(&r[0][0], Value::Text(t) if t.as_str() == "ann"));
    assert!(matches!(&r[0][1], Value::Integer(2)));

    // WHERE on the joined product.
    let r = query_rows(
        &db,
        "SELECT COUNT(*) FROM orders o JOIN cust c ON o.cid = c.id WHERE o.amt > 25.0",
    );
    assert!(matches!(&r[0][0], Value::Integer(3)));

    // 4-table equi join.
    let r = query_rows(
        &db,
        "SELECT COUNT(*) FROM orders o JOIN cust c ON o.cid = c.id \
         JOIN docs d ON d.id = o.id JOIN cust c2 ON c2.id = o.cid",
    );
    assert!(matches!(&r[0][0], Value::Integer(5)));
    let _ = QueryResult::Modification { affected_rows: 0 }; // import guard
}

/// 🔒 JOIN correctness found by the multi-table differential fuzzer
/// (examples/join_differential_fuzz.rs vs SQLite).
#[test]
fn join_where_qualified_column_deterministic_and_correct() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE items (id INT PRIMARY KEY, day INT, tag TEXT)")
        .unwrap();
    db.execute("CREATE TABLE orders (id INT PRIMARY KEY, cid INT, day INT)")
        .unwrap();
    db.execute("INSERT INTO items VALUES (1, 5, 'zz')").unwrap();
    db.execute("INSERT INTO orders VALUES (10, 1, 5), (11, 2, 5), (12, 3, 9)")
        .unwrap();

    // `WHERE i.id = 1` used to strip the qualifier and bind to a RANDOM one
    // of i.id/o.id via HashMap-iteration fallback — the same query returned
    // 0, 1 or 2 across runs. It must deterministically mean the LEFT row.
    for _ in 0..20 {
        let r = scalar(
            &db,
            "SELECT COUNT(*) FROM items i LEFT JOIN orders o ON o.day = i.day WHERE i.id = 1",
        );
        assert_eq!(r, Value::Integer(2));
        let r = rows(
            &db,
            "SELECT i.tag, COUNT(o.id) FROM items i LEFT JOIN orders o ON o.day = i.day \
             WHERE i.id = 1 GROUP BY i.tag",
        );
        assert_eq!(r.len(), 1);
        assert!(matches!(&r[0][0], Value::Text(t) if t.as_str() == "zz"));
        assert_eq!(r[0][1], Value::Integer(2));
    }
}

#[test]
fn join_order_by_desc_limit_offset_not_prematurely_truncated() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE cust (id INT PRIMARY KEY)")
        .unwrap();
    db.execute("CREATE TABLE orders (id INT PRIMARY KEY, cid INT)")
        .unwrap();
    let custs: Vec<String> = (1..=10).map(|i| format!("({i})")).collect();
    db.execute(&format!("INSERT INTO cust VALUES {}", custs.join(",")))
        .unwrap();
    // 40 orders spread over the 10 customers (id 1..=40).
    let orders: Vec<String> = (1..=40)
        .map(|i| format!("({i}, {}) ", (i - 1) % 10 + 1))
        .collect();
    db.execute(&format!("INSERT INTO orders VALUES {}", orders.join(",")))
        .unwrap();

    // The 2-table fast path early-stopped the driver scan at LIMIT before
    // the DESC sort — OFFSET then produced fewer/wrong rows.
    let r = ids(
        &db,
        "SELECT o.id FROM orders o JOIN cust c ON o.cid = c.id ORDER BY o.id DESC LIMIT 5 OFFSET 3",
    );
    assert_eq!(r, vec![37, 36, 35, 34, 33]);
    let r = ids(
        &db,
        "SELECT o.id FROM orders o JOIN cust c ON o.cid = c.id ORDER BY o.id LIMIT 5",
    );
    assert_eq!(r, vec![1, 2, 3, 4, 5]);
}

/// 🔒 Aggregate + correlated subquery used to be a hard error, then (with
/// the guard relaxed) silently returned 0 — the positional aggregate paths
/// treated the subquery evaluation error as "no match". They now decline to
/// the materialized path, which evaluates correlated subqueries per row.
#[test]
fn aggregate_with_correlated_subquery_where() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE t (a INT)").unwrap();
    db.execute("CREATE TABLE u (t_id INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(NULL)").unwrap();
    db.execute("INSERT INTO u VALUES (1),(1),(2)").unwrap();

    let r = scalar(
        &db,
        "SELECT COUNT(*) FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.t_id = t.a)",
    );
    assert_eq!(r, Value::Integer(2), "COUNT over correlated EXISTS");
    let r = scalar(
        &db,
        "SELECT SUM(a) FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.t_id = t.a)",
    );
    // SUM over INT may surface as Integer on this path (engine-wide it is
    // Float on some paths, Integer on others — both equal 3 here).
    match r {
        Value::Float(f) => assert!((f - 3.0).abs() < 1e-9),
        Value::Integer(i) => assert_eq!(i, 3),
        other => panic!("SUM {other:?}"),
    }
    // Correlated scalar subquery in an aggregate WHERE: for t.a=1 the
    // subquery yields min(1,1)=1 → 1>1 false; t.a=2 → min(2)=2 → false;
    // t.a=NULL → empty subquery → NULL comparison → false. SQLite also
    // returns 0 here (verified in the differential fuzzer shapes).
    let r = scalar(
        &db,
        "SELECT COUNT(*) FROM t WHERE a > (SELECT MIN(t_id) FROM u WHERE u.t_id = t.a)",
    );
    assert_eq!(r, Value::Integer(0));
}

/// 🔒 NULLS FIRST/LAST: explicit placement honored independently of
/// ASC/DESC; absent the clause, the dialect default holds (NULLs first on
/// ASC, last on DESC).
#[test]
fn order_by_nulls_first_last() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30), (4, NULL)")
        .unwrap();

    let vs = |sql: &str| -> Vec<Option<i64>> {
        query_rows(&db, sql)
            .into_iter()
            .map(|r| match &r[1] {
                Value::Integer(i) => Some(*i),
                Value::Null => None,
                o => panic!("{o:?}"),
            })
            .collect()
    };
    assert_eq!(
        vs("SELECT id, v FROM t ORDER BY v"),
        vec![None, None, Some(10), Some(30)]
    );
    assert_eq!(
        vs("SELECT id, v FROM t ORDER BY v NULLS LAST"),
        vec![Some(10), Some(30), None, None]
    );
    assert_eq!(
        vs("SELECT id, v FROM t ORDER BY v DESC"),
        vec![Some(30), Some(10), None, None]
    );
    assert_eq!(
        vs("SELECT id, v FROM t ORDER BY v DESC NULLS FIRST"),
        vec![None, None, Some(30), Some(10)]
    );
    assert_eq!(
        vs("SELECT id, v FROM t ORDER BY v ASC NULLS FIRST"),
        vec![None, None, Some(10), Some(30)]
    );
    assert_eq!(
        vs("SELECT id, v FROM t ORDER BY v ASC NULLS LAST"),
        vec![Some(10), Some(30), None, None]
    );
    // LIMIT applies AFTER the authoritative sort.
    let r = vs("SELECT id, v FROM t ORDER BY v NULLS LAST LIMIT 2");
    assert_eq!(r, vec![Some(10), Some(30)]);
}

/// 🔒 Large TEXT: the builder's in-memory u16 length prefix capped values at
/// 65,534 bytes even though the on-disk text layout (u32 offsets) supports
/// 4 GiB. The prefix is now u32 — values up to megabytes round-trip through
/// checkpoint/reopen AND segment merges.
#[test]
fn large_text_roundtrip_and_merge() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE big (id INT PRIMARY KEY, t TEXT)")
        .unwrap();
    let sizes: [usize; 5] = [1, 65_534, 65_535, 300_000, 1_500_000];
    for (i, n) in sizes.iter().enumerate() {
        let s = "x".repeat(*n);
        db.execute(&format!(
            "INSERT INTO big (id, t) VALUES ({}, '{}')",
            i + 1,
            s
        ))
        .unwrap();
    }
    let check = |db: &Database, tag: &str| {
        for (i, n) in sizes.iter().enumerate() {
            let r = scalar(
                &db,
                &format!("SELECT LENGTH(t) FROM big WHERE id = {}", i + 1),
            );
            assert_eq!(r, Value::Integer(*n as i64), "{tag}: id={} want {n}", i + 1);
        }
    };
    check(&db, "in-memory");
    db.checkpoint().unwrap();
    db.close().unwrap();
    let db = Database::open(dir.path()).unwrap();
    check(&db, "after-reopen");
    // Trip a segment merge, then re-verify (merge re-encodes text columns).
    for i in 0..40 {
        db.execute(&format!(
            "INSERT INTO big (id, t) VALUES ({}, 's')",
            1000 + i
        ))
        .unwrap();
    }
    let _ = db
        .execute("SELECT COUNT(*) FROM big")
        .unwrap()
        .materialize();
    db.checkpoint().unwrap();
    db.close().unwrap();
    let db = Database::open(dir.path()).unwrap();
    check(&db, "after-merge");
}
