//! 🔴 聚合全矩阵 × 向量 KNN × 空间谓词 —— 第三块 oracle 扫描。
//!
//! 前两轮（路径矩阵、重启×DML）覆盖了标量查询与持久化; 多模态核心
//! （SUM/AVG/GROUP BY/HAVING、`ORDER BY emb <-> ?`、ST_WITHIN/距离序）
//! 一直是 demo 主力路径却没被暴力 oracle 校验过。全部对照 Rust 侧
//! filter/aggregate/brute-force-KNN/几何计算。

use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

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

fn rows_canon(rows: &[Vec<Value>]) -> Vec<String> {
    rows.iter()
        .map(|r| r.iter().map(canon).collect::<Vec<_>>().join(","))
        .collect()
}

// ===========================================================================
// A. 聚合全矩阵: SUM/AVG × WHERE × GROUP BY（单/双键）× HAVING × ORDER BY 聚合
// ===========================================================================

struct ARow {
    id: i64,
    ival: Option<i64>,
    fval: Option<f64>,
    sval: &'static str,
    flag: bool,
    ts: i64,
}

fn agg_fixture() -> Vec<ARow> {
    (0..36)
        .map(|i| ARow {
            id: i,
            ival: if i % 9 == 4 {
                None
            } else {
                Some(((i * 13) % 21) as i64 - 10)
            },
            fval: if i % 12 == 7 {
                None
            } else {
                Some(i as f64 * 0.25 - 4.5) // exact binary steps
            },
            sval: ["aa", "bb", "cc"][(i % 3) as usize],
            flag: i % 2 == 0,
            ts: 1_794_300_000_000_000 + i as i64 * 97_000_000,
        })
        .collect()
}

fn setup_agg(db: &Database) {
    db.execute(
        "CREATE TABLE a (id INT PRIMARY KEY, ival INT, fval REAL, sval TEXT, \
         flag BOOLEAN, ts TIMESTAMP)",
    )
    .unwrap();
    for r in agg_fixture() {
        let ival = r
            .ival
            .map(|v| v.to_string())
            .unwrap_or_else(|| "NULL".into());
        let fval = r
            .fval
            .map(|v| format!("{v:?}"))
            .unwrap_or_else(|| "NULL".into());
        db.execute(&format!(
            "INSERT INTO a VALUES ({}, {ival}, {fval}, '{}', {}, {})",
            r.id, r.sval, r.flag, r.ts
        ))
        .unwrap();
    }
}

type Keep = fn(&ARow) -> bool;

fn oracle_scalar(rows: &[ARow], keep: Keep, pick: fn(&ARow) -> Option<f64>) -> Option<f64> {
    let vals: Vec<f64> = rows.iter().filter(|r| keep(r)).filter_map(pick).collect();
    if vals.is_empty() {
        None
    } else {
        Some(vals.iter().sum())
    }
}

#[test]
fn aggregate_sum_avg_matrix() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    setup_agg(&db);
    let rows = agg_fixture();

    let wheres: &[(&str, Keep)] = &[
        ("", |_| true),
        ("WHERE ival >= 0", |r| r.ival.is_some_and(|v| v >= 0)),
        ("WHERE sval = 'bb'", |r| r.sval == "bb"),
        ("WHERE flag = TRUE", |r| r.flag),
        ("WHERE fval IS NOT NULL", |r| r.fval.is_some()),
        ("WHERE id < 18", |r| r.id < 18),
    ];

    for (wsql, keep) in wheres {
        // SUM/AVG over the integer column (NULLs skipped by SQL semantics).
        let want_sum_i = oracle_scalar(&rows, *keep, |r| r.ival.map(|v| v as f64));
        let want_avg_i = oracle_scalar(&rows, *keep, |r| r.ival.map(|v| v as f64)).map(|s| {
            let n = rows
                .iter()
                .filter(|r| (keep)(r) && r.ival.is_some())
                .count();
            s / n as f64
        });
        let want_sum_f = oracle_scalar(&rows, *keep, |r| r.fval);
        let want_avg_f = oracle_scalar(&rows, *keep, |r| r.fval).map(|s| {
            let n = rows
                .iter()
                .filter(|r| (keep)(r) && r.fval.is_some())
                .count();
            s / n as f64
        });

        let cases: Vec<(&str, Option<f64>, bool)> = vec![
            ("SUM(ival)", want_sum_i, true),
            ("AVG(ival)", want_avg_i, false),
            ("SUM(fval)", want_sum_f, true),
            ("AVG(fval)", want_avg_f, false),
        ];
        for (agg, want, int_col) in cases {
            let sql = format!("SELECT {agg} FROM a {wsql}");
            let got = &db.query(&sql).unwrap()[0][0];
            let w = want.clone();
            match (got, w, int_col) {
                (Value::Null, None, _) => {}
                (Value::Integer(g), Some(wv), true) => {
                    assert_eq!(*g, wv as i64, "{sql}: SUM(ival) integer oracle")
                }
                (Value::Float(g), Some(wv), false) => {
                    assert!((g - wv).abs() < 1e-9, "{sql}: AVG oracle got {g} want {wv}")
                }
                (Value::Float(g), Some(wv), true) => {
                    // int SUM promoted to float only on overflow — none here.
                    assert!((g - wv).abs() < 1e-9, "{sql}: SUM oracle got {g} want {wv}")
                }
                other => panic!("{sql}: variant mismatch {other:?}"),
            }
        }
    }

    // Empty selection: SUM → NULL, AVG → NULL, COUNT → 0.
    let got = db
        .query("SELECT SUM(ival), AVG(fval), COUNT(*) FROM a WHERE id > 999")
        .unwrap();
    let g: Vec<String> = got[0].iter().map(canon).collect();
    assert_eq!(g, vec!["∅", "∅", "I:0"], "empty-set aggregate semantics");
}

#[test]
fn group_by_having_matrix() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    setup_agg(&db);
    let rows = agg_fixture();

    // Single-key GROUP BY + HAVING + ORDER BY agg alias.
    let mut groups: Vec<(String, i64, f64)> = Vec::new(); // (sval, count, sum_fval)
    for key in ["aa", "bb", "cc"] {
        let sel: Vec<&ARow> = rows.iter().filter(|r| r.sval == key).collect();
        let cnt = sel.len() as i64;
        let sum_f: f64 = sel.iter().filter_map(|r| r.fval).sum();
        groups.push((format!("S:{key}"), cnt, sum_f));
    }
    groups.sort_by(|a, b| (b.1, a.0.clone()).cmp(&(a.1, b.0.clone()))); // n DESC, key ASC

    let got = db
        .query(
            "SELECT sval, COUNT(*) AS n, SUM(fval) AS s FROM a \
                GROUP BY sval HAVING COUNT(*) > 0 ORDER BY n DESC, sval ASC",
        )
        .unwrap();
    let g: Vec<String> = rows_canon(&got);
    let want: Vec<String> = groups
        .iter()
        .map(|(k, n, s)| format!("{k},I:{n},F:{s:?}"))
        .collect();
    assert_eq!(g, want, "GROUP BY + HAVING + ORDER BY agg alias");

    // HAVING that filters groups out entirely.
    let got = db
        .query("SELECT sval, COUNT(*) FROM a GROUP BY sval HAVING COUNT(*) > 100")
        .unwrap();
    assert!(got.is_empty(), "over-tight HAVING must return zero groups");

    // Two-key GROUP BY vs oracle.
    let mut want2: Vec<String> = Vec::new();
    for key in ["aa", "bb", "cc"] {
        for f in [true, false] {
            let sel: Vec<&ARow> = rows
                .iter()
                .filter(|r| r.sval == key && r.flag == f)
                .collect();
            if sel.is_empty() {
                continue;
            }
            let sum_i: i64 = sel.iter().filter_map(|r| r.ival).sum();
            want2.push(format!("S:{key},B:{},I:{sum_i}", if f { 1 } else { 0 }));
        }
    }
    want2.sort();
    let got = db
        .query("SELECT sval, flag, SUM(ival) FROM a GROUP BY sval, flag ORDER BY sval, flag")
        .unwrap();
    let mut g = rows_canon(&got);
    g.sort();
    assert_eq!(g, want2, "two-key GROUP BY + SUM with NULL ival rows");
}

// ===========================================================================
// B. 向量 KNN 暴力 oracle（含过滤 KNN）
// ===========================================================================

const VD: usize = 4;
const VN: usize = 40;

fn vec_of(i: usize) -> Vec<f64> {
    // Tie-free by construction: strictly increasing dominant component.
    vec![
        i as f64 * 1.0,
        (i as f64 * 7.0) % 13.0 - 6.0,
        ((i as f64 * 5.0) % 11.0 - 5.0) * 0.5,
        (i as f64 * 0.25 - 5.0),
    ]
}

fn l2(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b).map(|(x, y)| (x - y).powi(2)).sum()
}

#[test]
fn vector_knn_matches_bruteforce() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE v (id INT PRIMARY KEY, tag TEXT, emb VECTOR(4))")
        .unwrap();
    for i in 0..VN {
        let e = vec_of(i);
        db.execute(&format!(
            "INSERT INTO v VALUES ({}, '{}', '[{},{},{},{}]')",
            i,
            ["x", "y", "z"][i % 3],
            e[0],
            e[1],
            e[2],
            e[3]
        ))
        .unwrap();
    }

    for q in [
        [0.0_f64, 0.0, 0.0, 0.0],
        [19.0, 4.0, -2.0, 3.5],
        [-3.0, 6.0, 1.0, -4.0],
    ] {
        for k in [1usize, 5, 17, 100] {
            // Unfiltered KNN vs brute force.
            let sql = format!(
                "SELECT id FROM v ORDER BY emb <-> '[{},{},{},{}]' LIMIT {k}",
                q[0], q[1], q[2], q[3]
            );
            let got: Vec<i64> = db
                .query(&sql)
                .unwrap()
                .iter()
                .map(|r| match &r[0] {
                    Value::Integer(i) => *i,
                    other => panic!("{sql}: id as {other:?}"),
                })
                .collect();
            let mut ranked: Vec<(usize, f64)> = (0..VN).map(|i| (i, l2(&vec_of(i), &q))).collect();
            ranked.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let want: Vec<i64> = ranked.iter().take(k).map(|(i, _)| *i as i64).collect();
            assert_eq!(got, want, "KNN q={q:?} k={k} diverges from brute force");
        }

        // Filtered KNN (top-k with WHERE) vs brute force over the subset.
        let keep = |i: usize| (i as i64) % 2 == 0;
        let sql = format!(
            "SELECT id FROM v WHERE id % 2 = 0 ORDER BY emb <-> '[{},{},{},{}]' LIMIT 7",
            q[0], q[1], q[2], q[3]
        );
        let got: Vec<i64> = db
            .query(&sql)
            .unwrap()
            .iter()
            .map(|r| match &r[0] {
                Value::Integer(i) => *i,
                other => panic!("id as {other:?}"),
            })
            .collect();
        let mut ranked: Vec<(usize, f64)> = (0..VN)
            .filter(|i| keep(*i))
            .map(|i| (i, l2(&vec_of(i), &q)))
            .collect();
        ranked.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        let want: Vec<i64> = ranked.iter().take(7).map(|(i, _)| *i as i64).collect();
        assert_eq!(got, want, "filtered KNN q={q:?} diverges");
    }
}

// ===========================================================================
// C. 空间谓词 + 距离序（robot-memory 的主力路径）
// ===========================================================================

const SN: usize = 30;

fn pt_of(i: usize) -> (f64, f64, f64) {
    (
        (i as f64 * 1.3) % 12.0 - 6.0,
        (i as f64 * 2.1) % 12.0 - 6.0,
        ((i as f64 * 0.7) % 5.0) - 2.5,
    )
}

fn d3(a: (f64, f64, f64), b: (f64, f64, f64)) -> f64 {
    (a.0 - b.0).powi(2) + (a.1 - b.1).powi(2) + (a.2 - b.2).powi(2)
}

#[test]
fn spatial_within_and_order_match_bruteforce() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE s (id INT PRIMARY KEY, loc GEOMETRY)")
        .unwrap();
    for i in 0..SN {
        let (x, y, z) = pt_of(i);
        db.execute(&format!(
            "INSERT INTO s VALUES ({i}, POINT3D({x:.4}, {y:.4}, {z:.4}))"
        ))
        .unwrap();
    }

    // ST_WITHIN (2D box, inclusive) vs brute force — box chosen off-lattice
    // so no point sits on a boundary (convention-independent).
    for (x0, y0, x1, y1) in [(-5.13, -5.77, 0.31, 0.47), (0.11, -6.03, 5.99, 5.51)] {
        let sql =
            format!("SELECT id FROM s WHERE ST_WITHIN(loc, {x0}, {y0}, {x1}, {y1}) ORDER BY id");
        let got: Vec<i64> = db
            .query(&sql)
            .unwrap()
            .iter()
            .map(|r| match &r[0] {
                Value::Integer(i) => *i,
                other => panic!("{sql}: id as {other:?}"),
            })
            .collect();
        let want: Vec<i64> = (0..SN as i64)
            .filter(|&i| {
                let (x, y, _) = pt_of(i as usize);
                x > x0 && x < x1 && y > y0 && y < y1
            })
            .collect();
        assert_eq!(got, want, "ST_WITHIN box ({x0},{y0})-({x1},{y1}) diverges");
    }

    // ORDER BY loc <-> ST_POINT — pinned contract (verified against the
    // engine, and the convention worth writing down): `<->` computes the
    // FULL 3D distance and ST_POINT(x, y) lifts to z = 0. A row with
    // z ≠ 0 therefore pays its z offset. NOTE: this differs from PostGIS,
    // where `<->` is 2D and 3D needs an explicit operator — and from the
    // engine's own ST_WITHIN (2D unless the _3D suffix). Rows here have
    // z ≠ 0 on purpose so the z-participating semantics stays pinned.
    for q in [(0.0_f64, 0.0), (-5.5, 4.25), (6.1, -6.2)] {
        for k in [1usize, 6, 30] {
            let sql = format!(
                "SELECT id FROM s ORDER BY loc <-> ST_POINT({}, {}) LIMIT {k}",
                q.0, q.1
            );
            let got: Vec<i64> = db
                .query(&sql)
                .unwrap()
                .iter()
                .map(|r| match &r[0] {
                    Value::Integer(i) => *i,
                    other => panic!("{sql}: id as {other:?}"),
                })
                .collect();
            let mut ranked: Vec<(usize, f64)> = (0..SN)
                .map(|i| (i, d3(pt_of(i), (q.0, q.1, 0.0))))
                .collect();
            ranked.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let want: Vec<i64> = ranked.iter().take(k).map(|(i, _)| *i as i64).collect();
            assert_eq!(
                got, want,
                "spatial KNN q={q:?} k={k} diverges from brute force"
            );
        }
    }

    // ORDER BY ST_DISTANCE_3D vs 3D brute force.
    let q3 = (-1.2, 0.4, 1.1);
    let got: Vec<i64> = db
        .query("SELECT id FROM s ORDER BY ST_DISTANCE_3D(loc, -1.2, 0.4, 1.1) LIMIT 8")
        .unwrap()
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            other => panic!("id as {other:?}"),
        })
        .collect();
    let mut ranked: Vec<(usize, f64)> = (0..SN).map(|i| (i, d3(pt_of(i), q3))).collect();
    ranked.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    let want: Vec<i64> = ranked.iter().take(8).map(|(i, _)| *i as i64).collect();
    assert_eq!(
        got, want,
        "ST_DISTANCE_3D ordering diverges from brute force"
    );

    // Projected distance value sanity (exact tie-free construction not
    // guaranteed → fuzzy compare on the projected scalar for a fixed row).
    let got = &db
        .query("SELECT ST_DISTANCE_3D(loc, -1.2, 0.4, 1.1) AS d FROM s WHERE id = 7")
        .unwrap()[0][0];
    match got {
        Value::Float(g) => {
            let want = d3(pt_of(7), q3).sqrt();
            assert!(
                (g - want).abs() < 1e-6,
                "ST_DISTANCE_3D value {g} vs {want}"
            );
        }
        other => panic!("distance projected as {other:?}"),
    }
}
