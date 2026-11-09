//! 🔴 Path-matrix correctness sweep: the same logical query must return the
//! same *correct* result no matter which physical fast path the planner
//! picks. #17 (MATCH + ORDER BY DESC silently ascending) and #18
//! (eq-WHERE + ORDER BY projecting TIMESTAMP as NULL) were both found by a
//! user demo on paths no test had ever combined.
//!
//! This sweep is NOT SQL-vs-SQL equivalence (two spellings can agree on a
//! wrong answer) — it checks every shape against a brute-force oracle
//! computed in plain Rust from the fixture rows:
//!
//!   expected = fixture.filter(WHERE).order(ORDER BY, NULLs-smallest)
//!              .offset/limit(PAGING).project(PROJECTION)
//!
//! Matrix (single-segment table):
//!   18 WHERE forms × 13 ORDER BY forms × 5 paging forms × 5 projections
//! Plus: aggregate oracles, an FTS/MATCH matrix (#17 family), and a
//! deliberately multi-segment table (flush → insert → flush) for the
//! multi-segment decode branches.

use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

type Pred = fn(&Row) -> bool;
type OrderKeys = fn(&Row) -> Vec<(OrdV, bool)>;
type Project = fn(&Row) -> Vec<String>;

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

const N: usize = 40;
const TS_BASE: i64 = 1_794_300_000_000_000; // µs, arbitrary epoch
const TS_STEP: i64 = 37 * 60 * 1_000_000; // 37 min — unique per row

struct Row {
    id: i64,
    ival: Option<i64>,
    fval: Option<f64>,
    sval: Option<&'static str>,
    flag: Option<bool>,
    ts: i64,   // µs, never NULL
    dday: i32, // days since epoch, never NULL
}

fn ts_of(i: usize) -> i64 {
    TS_BASE + i as i64 * TS_STEP
}

fn fixture() -> Vec<Row> {
    let d0 = date0();
    (0..N)
        .map(|i| Row {
            id: i as i64,
            ival: if i % 11 == 5 {
                None
            } else {
                Some(((i * 7) % 23) as i64)
            },
            fval: if i % 13 == 7 {
                None
            } else {
                Some(i as f64 * 0.25 - 5.0)
            },
            sval: if i % 17 == 3 {
                None
            } else {
                Some(["alpha", "beta", "gamma", "delta"][i % 4])
            },
            flag: if i % 19 == 9 { None } else { Some(i % 2 == 0) },
            ts: ts_of(i),
            dday: d0 + i as i32, // unique — Jan 1 .. Feb 9
        })
        .collect()
}

fn iso_date(days: i32) -> String {
    motedb::types::Date::from_days(days).to_iso()
}

/// Canonical rendering of a Value — same variant+value ⇒ same string,
/// different variant ⇒ different string (catches Timestamp-vs-Integer etc.).
fn canon(v: &Value) -> String {
    match v {
        Value::Null => "∅".into(),
        Value::Integer(i) => format!("I:{i}"),
        Value::Float(f) => format!("F:{f:?}"),
        Value::Text(s) => format!("S:{}", s.as_str()),
        Value::Bool(b) => format!("B:{}", if *b { 1 } else { 0 }),
        Value::Timestamp(t) => format!("T:{}", t.as_micros()),
        Value::Date(d) => format!("D:{}", d.days()),
        other => format!("X:{other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Oracle: ORDER keys as plain-Rust closures. NULLs smallest, like order_by_cmp.
// ---------------------------------------------------------------------------

/// Orderable key mirroring order_by_cmp: Null < anything.
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy)]
enum OrdV {
    Null,
    I(i64),
    F(u64), // monotone-encoded finite f64 (unsigned compare!)
    S(&'static str),
    B(bool),
}

/// f64 total order (no NaNs in fixture): IEEE-754 total-order monotone map,
/// compared as UNSIGNED — sign-flip for negatives, then ^sign-bit for
/// positives (a signed compare inverts the two halves).
fn fkey(f: f64) -> OrdV {
    let b = f.to_bits();
    let m = if (b as i64) < 0 {
        !b
    } else {
        b ^ 0x8000_0000_0000_0000
    };
    OrdV::F(m)
}

// ---------------------------------------------------------------------------
// Matrix definition
// ---------------------------------------------------------------------------

const WHERE_SHAPES: &[(&str, Pred)] = &[
    ("", |_| true),
    ("WHERE ival = 7", |r| r.ival == Some(7)),
    ("WHERE ival >= 15", |r| r.ival.is_some_and(|v| v >= 15)),
    ("WHERE ival < 8", |r| r.ival.is_some_and(|v| v < 8)),
    ("WHERE ival BETWEEN 10 AND 20", |r| {
        r.ival.is_some_and(|v| (10..=20).contains(&v))
    }),
    ("WHERE ival IN (5, 15, 25)", |r| {
        r.ival.is_some_and(|v| v == 5 || v == 15 || v == 25)
    }),
    ("WHERE ival IS NULL", |r| r.ival.is_none()),
    ("WHERE ival IS NOT NULL", |r| r.ival.is_some()),
    ("WHERE sval = 'beta'", |r| r.sval == Some("beta")),
    ("WHERE sval LIKE 'a%'", |r| {
        r.sval.is_some_and(|s| s.starts_with('a'))
    }),
    ("WHERE flag = TRUE", |r| r.flag == Some(true)),
    ("WHERE flag = FALSE", |r| r.flag == Some(false)),
    ("WHERE fval >= 0.25", |r| r.fval.is_some_and(|v| v >= 0.25)),
    ("WHERE fval IS NULL", |r| r.fval.is_none()),
    ("WHERE ts >= 1794300000000000", |r| r.ts >= TS_BASE),
    ("WHERE ts = 1794322200000000", |r| r.ts == ts_of(10)),
    (
        "WHERE ts BETWEEN 1794300000000000 AND 1794326640000000",
        |r| (TS_BASE..=ts_of(12)).contains(&r.ts),
    ),
    // day 20 = 2026-01-21; day 7 = 2026-01-08 (placeholders patched at sweep
    // time — the DATE literal form is probed once in setup_table's caller)
    ("WHERE d >= {D20}", |r| r.dday >= date0() + 20),
    ("WHERE d = {D7}", |r| r.dday == date0() + 7),
];

/// Epoch days of 2026-01-01 — the fixture's DATE origin.
fn date0() -> i32 {
    motedb::types::Date::parse("2026-01-01").unwrap().days()
}

const ORDER_SHAPES: &[(&str, OrderKeys)] = &[
    ("", |_| vec![]),
    ("ORDER BY id", |r| vec![(OrdV::I(r.id), true)]),
    ("ORDER BY id DESC", |r| vec![(OrdV::I(r.id), false)]),
    ("ORDER BY ts", |r| vec![(OrdV::I(r.ts), true)]),
    ("ORDER BY ts DESC", |r| vec![(OrdV::I(r.ts), false)]),
    ("ORDER BY d", |r| vec![(OrdV::I(r.dday as i64), true)]),
    ("ORDER BY d DESC", |r| vec![(OrdV::I(r.dday as i64), false)]),
    ("ORDER BY fval", |r| {
        vec![(r.fval.map_or(OrdV::Null, |f| fkey(f)), true)]
    }),
    ("ORDER BY fval DESC", |r| {
        vec![(r.fval.map_or(OrdV::Null, |f| fkey(f)), false)]
    }),
    // tie-prone keys: explicit id tiebreak in SQL, mirrored in oracle
    ("ORDER BY sval, id", |r| {
        vec![
            (r.sval.map_or(OrdV::Null, OrdV::S), true),
            (OrdV::I(r.id), true),
        ]
    }),
    ("ORDER BY ival, id", |r| {
        vec![
            (r.ival.map_or(OrdV::Null, OrdV::I), true),
            (OrdV::I(r.id), true),
        ]
    }),
    ("ORDER BY ival DESC, id DESC", |r| {
        vec![
            (r.ival.map_or(OrdV::Null, OrdV::I), false),
            (OrdV::I(r.id), false),
        ]
    }),
    ("ORDER BY flag DESC, id", |r| {
        vec![
            (r.flag.map_or(OrdV::Null, OrdV::B), false),
            (OrdV::I(r.id), true),
        ]
    }),
];

const PAGING_SHAPES: &[(&str, usize, usize)] = &[
    ("", 0, usize::MAX),
    ("LIMIT 7", 0, 7),
    ("LIMIT 7 OFFSET 3", 3, 7),
    ("LIMIT 100000", 0, 100_000),
    ("OFFSET 6", 6, usize::MAX),
];

const PROJ_SHAPES: &[(&str, Project)] = &[
    ("SELECT ts", |r| vec![format!("T:{}", r.ts)]),
    ("SELECT d, sval", |r| {
        vec![
            format!("D:{}", r.dday),
            r.sval.map(|s| format!("S:{s}")).unwrap_or("∅".into()),
        ]
    }),
    ("SELECT fval, ival", |r| {
        vec![
            r.fval.map(|f| format!("F:{f:?}")).unwrap_or("∅".into()),
            r.ival.map(|i| format!("I:{i}")).unwrap_or("∅".into()),
        ]
    }),
    ("SELECT id, ts, d, flag", |r| {
        vec![
            format!("I:{}", r.id),
            format!("T:{}", r.ts),
            format!("D:{}", r.dday),
            r.flag
                .map(|b| format!("B:{}", if b { 1 } else { 0 }))
                .unwrap_or("∅".into()),
        ]
    }),
    ("SELECT sval", |r| {
        vec![r.sval.map(|s| format!("S:{s}")).unwrap_or("∅".into())]
    }),
];

fn setup_table(db: &Database) {
    db.execute(
        "CREATE TABLE t (id INT PRIMARY KEY, ival INT, fval REAL, sval TEXT, \
         flag BOOLEAN, ts TIMESTAMP, d DATE)",
    )
    .unwrap();
    for r in fixture() {
        let iv = r.ival.map(|v| v.to_string()).unwrap_or("NULL".into());
        let fv = r.fval.map(|v| format!("{v:?}")).unwrap_or("NULL".into());
        let sv = r.sval.map(|v| format!("'{v}'")).unwrap_or("NULL".into());
        let fl = r.flag.map(|v| v.to_string()).unwrap_or("NULL".into());
        db.execute(&format!(
            "INSERT INTO t VALUES ({}, {}, {}, {}, {}, {}, '{}')",
            r.id,
            iv,
            fv,
            sv,
            fl,
            r.ts,
            iso_date(r.dday)
        ))
        .unwrap();
    }
}

// ---------------------------------------------------------------------------
// The sweep
// ---------------------------------------------------------------------------

struct Failures {
    mismatches: Vec<String>,
    errors: Vec<String>,
}

impl Failures {
    fn note_mismatch(&mut self, sql: &str, got: &[Vec<Value>], want: &[Vec<String>]) {
        if self.mismatches.len() < 12 {
            let g: Vec<String> = got
                .iter()
                .map(|r| r.iter().map(canon).collect::<Vec<_>>().join(","))
                .collect();
            self.mismatches
                .push(format!("SQL:   {sql}\n  got:  {g:?}\n  want: {want:?}"));
        }
        self.mismatches.push(format!("__count__ {sql}"));
    }
}

/// Tie-tolerant comparison: rows within a maximal run of EQUAL sort keys may
/// come back in any order (SQL leaves tie order unspecified; heap/topk paths
/// legitimately differ from stable insertion order). Rows in different tie
/// groups must not interleave. `tie_runs` = length of each equal-key run.
fn tie_tolerant_eq(got: &[Vec<String>], want: &[Vec<String>], tie_runs: &[usize]) -> bool {
    if got.len() != want.len() {
        return false;
    }
    let mut g = got.iter();
    let mut w = want.iter();
    for &run in tie_runs {
        let mut gr: Vec<&Vec<String>> = (&mut g).take(run).collect();
        let mut wr: Vec<&Vec<String>> = (&mut w).take(run).collect();
        gr.sort();
        wr.sort();
        if gr != wr {
            return false;
        }
    }
    true
}

fn sweep(db: &Database, table: &str, rows: &[Row], date_lit: &str, fails: &mut Failures) {
    for (wsql, keep) in WHERE_SHAPES {
        let wsql = wsql
            .replace(
                "{D20}",
                &date_lit.replacen("{}", &iso_date(date0() + 20), 1),
            )
            .replace("{D7}", &date_lit.replacen("{}", &iso_date(date0() + 7), 1));
        for (osql, keys) in ORDER_SHAPES {
            for (psql, offset, limit) in PAGING_SHAPES {
                for (proj_sql, proj) in PROJ_SHAPES {
                    let sql = format!("{proj_sql} FROM {table} {wsql} {osql} {psql}");
                    let (want, tie_runs): (Vec<Vec<String>>, Vec<usize>) = {
                        let mut kept: Vec<&Row> = rows.iter().filter(|r| keep(r)).collect();
                        kept.sort_by(|a, b| {
                            let ka = keys(a);
                            let kb = keys(b);
                            for (i, (v, asc)) in ka.iter().enumerate() {
                                let cmp = v.cmp(kb.get(i).map(|(v2, _)| v2).unwrap_or(&OrdV::Null));
                                let c = if *asc { cmp } else { cmp.reverse() };
                                if c != std::cmp::Ordering::Equal {
                                    return c;
                                }
                            }
                            std::cmp::Ordering::Equal
                        });
                        let paged: Vec<&Row> =
                            kept.iter().skip(*offset).take(*limit).copied().collect();
                        // Maximal runs of equal consecutive sort keys → ties.
                        let key_of =
                            |r: &Row| -> Vec<OrdV> { keys(r).iter().map(|(v, _)| *v).collect() };
                        let mut runs: Vec<usize> = Vec::new();
                        let mut i = 0;
                        while i < paged.len() {
                            let mut j = i + 1;
                            while j < paged.len() && key_of(paged[j]) == key_of(paged[i]) {
                                j += 1;
                            }
                            runs.push(j - i);
                            i = j;
                        }
                        (paged.iter().map(|r| proj(r)).collect(), runs)
                    };
                    match db.query(&sql) {
                        Ok(got) => {
                            let gc: Vec<Vec<String>> = got
                                .iter()
                                .map(|r| r.iter().map(canon).collect::<Vec<_>>())
                                .collect();
                            if !tie_tolerant_eq(&gc, &want, &tie_runs) {
                                fails.note_mismatch(&sql, &got, &want);
                            }
                        }
                        Err(e) => {
                            if fails.errors.len() < 12 {
                                fails.errors.push(format!("{sql}  →  {e}"));
                            }
                            fails.errors.push(format!("__count__ {sql}"));
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn path_matrix_single_segment() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    setup_table(&db);

    // Probe the DATE comparison literal form: prefer explicit DATE '...'
    // (typed); fall back to bare '...' if the parser wants the short form.
    let probe_typed = format!("SELECT id FROM t WHERE d = '{}'", iso_date(date0() + 7));
    let date_lit = if db.query(&probe_typed).is_ok() {
        "'{}'"
    } else {
        "'{}'"
    };

    let mut fails = Failures {
        mismatches: vec![],
        errors: vec![],
    };
    sweep(&db, "t", &fixture(), date_lit, &mut fails);

    let m = fails
        .mismatches
        .iter()
        .filter(|s| !s.starts_with("__count__"))
        .count();
    let mc = fails.mismatches.len() - m;
    let e = fails
        .errors
        .iter()
        .filter(|s| !s.starts_with("__count__"))
        .count();
    let ec = fails.errors.len() - e;
    assert!(
        fails.mismatches.is_empty() && fails.errors.is_empty(),
        "\npath-matrix sweep: {mc} mismatches, {ec} errors (detailed {m}/{e})\n--- mismatches ---\n{}\n--- errors ---\n{}",
        fails.mismatches.iter().filter(|s| !s.starts_with("__count__")).cloned().collect::<Vec<_>>().join("\n"),
        fails.errors.iter().filter(|s| !s.starts_with("__count__")).cloned().collect::<Vec<_>>().join("\n"),
    );
}

// ---------------------------------------------------------------------------
// Aggregates vs oracle
// ---------------------------------------------------------------------------

#[test]
fn path_matrix_aggregates() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    setup_table(&db);
    let rows = fixture();

    let wheres: &[(&str, Pred)] = &[
        ("", |_| true),
        ("WHERE ival >= 10", |r| r.ival.is_some_and(|v| v >= 10)),
        ("WHERE sval = 'beta'", |r| r.sval == Some("beta")),
        ("WHERE flag = TRUE", |r| r.flag == Some(true)),
    ];

    for (wsql, keep) in wheres {
        let sel: Vec<&Row> = rows.iter().filter(|r| keep(r)).collect();

        // MIN/MAX ts → canonical T:µs of the min/max over selection
        let (min_ts, max_ts): (Option<i64>, Option<i64>) = if sel.is_empty() {
            (None, None)
        } else {
            (
                Some(sel.iter().map(|r| r.ts).min().unwrap()),
                Some(sel.iter().map(|r| r.ts).max().unwrap()),
            )
        };
        let (min_d, max_d): (Option<i32>, Option<i32>) = if sel.is_empty() {
            (None, None)
        } else {
            (
                Some(sel.iter().map(|r| r.dday).min().unwrap()),
                Some(sel.iter().map(|r| r.dday).max().unwrap()),
            )
        };
        let count = sel.len();

        let cases: Vec<(&str, Option<String>)> = vec![
            ("MIN(ts)", min_ts.map(|v| format!("T:{v}"))),
            ("MAX(ts)", max_ts.map(|v| format!("T:{v}"))),
            ("MIN(d)", min_d.map(|v| format!("D:{v}"))),
            ("MAX(d)", max_d.map(|v| format!("D:{v}"))),
            ("COUNT(*)", Some(format!("I:{count}"))),
        ];
        for (agg, want) in cases {
            let sql = format!("SELECT {agg} FROM t {wsql}");
            let got = db.query(&sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
            let g = got
                .first()
                .map(|r| r.iter().map(canon).collect::<Vec<_>>().join(","))
                .unwrap_or_default();
            let w = want.unwrap_or_else(|| "∅".into());
            assert_eq!(g, w, "aggregate oracle mismatch: {sql}");
        }
    }

    // Empty selection: MIN → NULL, COUNT → 0
    let got = db
        .query("SELECT MIN(ts), MAX(d), COUNT(*) FROM t WHERE ival >= 9999")
        .unwrap();
    let g = got[0].iter().map(canon).collect::<Vec<_>>();
    assert_eq!(
        g,
        vec!["∅", "∅", "I:0"],
        "empty-selection aggregate must be NULL/NULL/0"
    );
}

// ---------------------------------------------------------------------------
// FTS matrix (#17 family)
// ---------------------------------------------------------------------------

#[test]
fn path_matrix_fts_order_paging() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE f (id INT PRIMARY KEY, ts TIMESTAMP, note TEXT)")
        .unwrap();
    db.execute("CREATE TEXT INDEX f_note ON f(note)").unwrap();
    for i in 0..30i64 {
        db.execute(&format!(
            "INSERT INTO f VALUES ({}, {}, 'tok{} record {} unique{}')",
            i,
            ts_of(i as usize),
            i % 3,
            i,
            i
        ))
        .unwrap();
    }
    std::thread::sleep(std::time::Duration::from_millis(1500)); // async index build

    // rows containing token "tok1" → i%3==1 → ids 1,4,7,...,28
    let matched: Vec<i64> = (0..30).filter(|i| i % 3 == 1).collect();

    for osql in ["", "ORDER BY ts", "ORDER BY ts DESC", "ORDER BY id DESC"] {
        for psql in ["", "LIMIT 5", "LIMIT 5 OFFSET 2", "LIMIT 100000"] {
            let sql = format!("SELECT id FROM f WHERE MATCH(note) AGAINST('tok1') {osql} {psql}");
            let got = db.query(&sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
            let mut ids: Vec<i64> = got
                .iter()
                .map(|r| match &r[0] {
                    Value::Integer(i) => *i,
                    other => panic!("{sql}: id came back as {other:?}"),
                })
                .collect();

            let mut want = matched.clone();
            match osql {
                "ORDER BY ts" => {} // insertion order == ts order (unique)
                "ORDER BY ts DESC" => want.reverse(),
                "ORDER BY id DESC" => want.reverse(),
                _ => {} // no ORDER: document order == insertion order
            }
            let (off, lim) = match psql {
                "LIMIT 5" => (0, 5),
                "LIMIT 5 OFFSET 2" => (2, 5),
                "LIMIT 100000" => (0, usize::MAX),
                _ => (0, usize::MAX),
            };
            let want: Vec<i64> = want.into_iter().skip(off).take(lim).collect();
            // No-ORDER BY result order is unspecified in principle — compare
            // as sets for the "" case, strictly otherwise (direction bugs like
            // #17 only manifest with ORDER BY).
            if osql.is_empty() {
                ids.sort_unstable();
                let mut w = want.clone();
                w.sort_unstable();
                assert_eq!(ids, w, "FTS shape {psql:?}: row set must match oracle");
            } else {
                assert_eq!(
                    ids, want,
                    "FTS {osql} {psql}: ordered result wrong (#17 family)"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Multi-segment table (two flush boundaries) — multi-seg decode branches
// ---------------------------------------------------------------------------

#[test]
fn path_matrix_multi_segment() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute(
        "CREATE TABLE m (id INT PRIMARY KEY, ival INT, fval REAL, sval TEXT, \
         flag BOOLEAN, ts TIMESTAMP, d DATE)",
    )
    .unwrap();

    let all = fixture();
    // Batch 1: ids 0..20, then a TEXT-eq probe (its fast path flushes the
    // buffer → segment 1). Batch 2: ids 20..40, probe again → segment 2.
    for r in &all[..20] {
        db.execute(&format!(
            "INSERT INTO m VALUES ({}, {}, {}, '{}', {}, {}, '{}')",
            r.id,
            r.ival.unwrap_or(-999),
            r.fval.map(|v| format!("{v:?}")).unwrap_or("NULL".into()),
            r.sval.unwrap_or("x"),
            r.flag.unwrap_or(true),
            r.ts,
            iso_date(r.dday)
        ))
        .unwrap();
    }
    let _ = db.query("SELECT id FROM m WHERE sval = 'alpha'").unwrap();
    for r in &all[20..] {
        db.execute(&format!(
            "INSERT INTO m VALUES ({}, {}, {}, '{}', {}, {}, '{}')",
            r.id,
            r.ival.unwrap_or(-999),
            r.fval.map(|v| format!("{v:?}")).unwrap_or("NULL".into()),
            r.sval.unwrap_or("x"),
            r.flag.unwrap_or(true),
            r.ts,
            iso_date(r.dday)
        ))
        .unwrap();
    }
    let _ = db.query("SELECT id FROM m WHERE sval = 'alpha'").unwrap();

    // NOTE: multi-seg fixture rows differ slightly (NULLs replaced) — rebuild
    // the oracle fixture to mirror exactly what we inserted.
    let mrows: Vec<Row> = all
        .iter()
        .map(|r| {
            let mut r2 = Row {
                id: r.id,
                ival: r.ival,
                fval: r.fval,
                sval: r.sval,
                flag: r.flag,
                ts: r.ts,
                dday: r.dday,
            };
            if r.ival.is_none() {
                r2.ival = Some(-999);
            }
            if r.sval.is_none() {
                r2.sval = Some("x");
            }
            if r.flag.is_none() {
                r2.flag = Some(true);
            }
            r2
        })
        .collect();

    let shapes: Vec<(&str, fn(&Row) -> bool)> = vec![
        ("WHERE ival >= 10", |r| r.ival.is_some_and(|v| v >= 10)),
        ("WHERE sval = 'beta'", |r| r.sval == Some("beta")),
        ("WHERE fval >= 0.25", |r| r.fval.is_some_and(|v| v >= 0.25)),
        ("WHERE flag = TRUE", |r| r.flag == Some(true)),
        ("WHERE ts >= 1794300000000000", |r| r.ts >= TS_BASE),
        ("WHERE d >= {D10}", |r| r.dday >= date0() + 10),
    ];
    let date_lit = {
        let probe = format!("SELECT id FROM m WHERE d = '{}'", iso_date(date0() + 7));
        if db.query(&probe).is_ok() {
            "'{}'"
        } else {
            "'{}'"
        }
    };

    let mut fails = Failures {
        mismatches: vec![],
        errors: vec![],
    };
    let projs: &[(&str, fn(&Row) -> Vec<String>)] = &[
        ("SELECT fval", |r: &Row| {
            vec![r.fval.map(|f| format!("F:{f:?}")).unwrap_or("∅".into())]
        }),
        ("SELECT ts", |r: &Row| vec![format!("T:{}", r.ts)]),
        ("SELECT d, sval", |r: &Row| {
            vec![
                format!("D:{}", r.dday),
                r.sval.map(|s| format!("S:{s}")).unwrap_or("∅".into()),
            ]
        }),
        ("SELECT id, ts, d, flag", |r: &Row| {
            vec![
                format!("I:{}", r.id),
                format!("T:{}", r.ts),
                format!("D:{}", r.dday),
                r.flag
                    .map(|b| format!("B:{}", if b { 1 } else { 0 }))
                    .unwrap_or("∅".into()),
            ]
        }),
    ];
    for (wsql, keep) in &shapes {
        let wsql = wsql.replace(
            "{D10}",
            &date_lit.replacen("{}", &iso_date(date0() + 10), 1),
        );
        for osql in ["", "ORDER BY ts DESC", "ORDER BY id"] {
            for psql in ["", "LIMIT 7"] {
                for (proj_sql, proj) in projs {
                    let sql = format!("{proj_sql} FROM m {wsql} {osql} {psql}");
                    let keys: fn(&Row) -> Vec<(OrdV, bool)> = match osql {
                        "ORDER BY ts DESC" => |r| vec![(OrdV::I(r.ts), false)],
                        "ORDER BY id" => |r| vec![(OrdV::I(r.id), true)],
                        _ => |_| vec![],
                    };
                    let (off, lim) = if psql == "LIMIT 7" {
                        (0, 7)
                    } else {
                        (0, usize::MAX)
                    };
                    let mut kept: Vec<&Row> = mrows.iter().filter(|r| keep(r)).collect();
                    kept.sort_by(|a, b| {
                        let ka = keys(a);
                        let kb = keys(b);
                        for (i, (v, asc)) in ka.iter().enumerate() {
                            let cmp = v.cmp(kb.get(i).map(|(v2, _)| v2).unwrap_or(&OrdV::Null));
                            let c = if *asc { cmp } else { cmp.reverse() };
                            if c != std::cmp::Ordering::Equal {
                                return c;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                    let want: Vec<Vec<String>> =
                        kept.iter().skip(off).take(lim).map(|r| proj(r)).collect();
                    match db.query(&sql) {
                        Ok(got) => {
                            let gc: Vec<String> = got
                                .iter()
                                .map(|r| r.iter().map(canon).collect::<Vec<_>>().join(","))
                                .collect();
                            let wc: Vec<String> = want.iter().map(|r| r.join(",")).collect();
                            if gc != wc {
                                fails.note_mismatch(&sql, &got, &want);
                            }
                        }
                        Err(e) => {
                            if fails.errors.len() < 12 {
                                fails.errors.push(format!("{sql}  →  {e}"));
                            }
                            fails.errors.push(format!("__count__ {sql}"));
                        }
                    }
                }
            }
        }
    }
    let m = fails
        .mismatches
        .iter()
        .filter(|s| !s.starts_with("__count__"))
        .count();
    let mc = fails.mismatches.len() - m;
    let e = fails
        .errors
        .iter()
        .filter(|s| !s.starts_with("__count__"))
        .count();
    let ec = fails.errors.len() - e;
    assert!(
        fails.mismatches.is_empty() && fails.errors.is_empty(),
        "\nmulti-segment sweep: {mc} mismatches, {ec} errors\n--- mismatches ---\n{}\n--- errors ---\n{}",
        fails.mismatches.iter().filter(|s| !s.starts_with("__count__")).cloned().collect::<Vec<_>>().join("\n"),
        fails.errors.iter().filter(|s| !s.starts_with("__count__")).cloned().collect::<Vec<_>>().join("\n"),
    );
}
