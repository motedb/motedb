//! 🔴 多段表 × 跨段版本 × 查询形状 —— 对 Rust 镜像状态的 oracle 扫描。
//!
//! 前几轮矩阵的结构性缺口: 多段矩阵只有纯插入数据，DML 矩阵只有单次
//! flush 的表。"表有多个段，且某些行在更新的段里有新版本或墓碑"——
//! 机器人和长驻进程的常态——从未被扫过。首跑即抓到: `WHERE text IN
//! ('a','b')` 返回被 UPDATE 行的旧版本并复活已删行（IN-list 扫描按段
//! 从旧到新遍历、却用首见即胜出去重）。
//!
//! oracle = Rust 侧镜像的最终逻辑状态。三种布局各跑一遍、结果必须相同:
//!   L1 多段新建  L2 关闭重开之后  L3 CHECKPOINT 之后（可能已合并为单段）

use motedb::types::Value;
use motedb::Database;
use std::collections::BTreeMap;
use tempfile::TempDir;

const BASE: i64 = 1_794_300_000_000_000;
const D0: i32 = 20_454; // 2026-01-01
const S: [&str; 3] = ["us", "eu", "ap"];

#[derive(Clone, Debug)]
struct R {
    id: i64,
    ival: Option<i64>,
    sval: Option<&'static str>,
    fval: Option<f64>,
    flag: Option<bool>,
    ts: i64,
    dd: i32,
}

type Mirror = BTreeMap<i64, R>;

fn fresh(id: i64) -> R {
    R {
        id,
        ival: if id % 7 == 3 {
            None
        } else {
            Some((id * 13) % 50)
        },
        sval: if id % 11 == 5 {
            None
        } else {
            Some(S[(id % 3) as usize])
        },
        fval: if id % 9 == 4 {
            None
        } else {
            Some(id as f64 * 0.5 - 20.0)
        },
        flag: if id % 13 == 6 {
            None
        } else {
            Some(id % 2 == 0)
        },
        ts: BASE + id * 1_000_000,
        dd: D0 + id as i32,
    }
}

fn opt(r: &R, c: usize) -> String {
    match c {
        0 => format!("I:{}", r.id),
        1 => r
            .ival
            .map(|v| format!("I:{v}"))
            .unwrap_or_else(|| "∅".into()),
        2 => r
            .sval
            .map(|v| format!("S:{v}"))
            .unwrap_or_else(|| "∅".into()),
        3 => r
            .fval
            .map(|v| format!("F:{v:?}"))
            .unwrap_or_else(|| "∅".into()),
        4 => r
            .flag
            .map(|v| format!("B:{}", if v { 1 } else { 0 }))
            .unwrap_or_else(|| "∅".into()),
        5 => format!("T:{}", r.ts),
        _ => format!("D:{}", r.dd),
    }
}

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

fn q(db: &Database, sql: &str) -> Vec<Vec<String>> {
    db.query(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| r.iter().map(canon).collect())
        .collect()
}

fn flush(db: &Database) {
    // 文本等值查询会 flush 写缓冲 → 段边界
    let _ = db.query("SELECT id FROM m WHERE sval = 'zzz'");
}

fn build(db: &Database) -> Mirror {
    db.execute(
        "CREATE TABLE m (id INT PRIMARY KEY, ival INT, sval TEXT, fval REAL, \
         flag BOOLEAN, ts TIMESTAMP, d DATE)",
    )
    .unwrap();
    let mut m = Mirror::new();
    let ins = |db: &Database, m: &mut Mirror, r: R| {
        let (iv, sv, fv, fl) = (
            r.ival
                .map(|v| v.to_string())
                .unwrap_or_else(|| "NULL".into()),
            r.sval
                .map(|v| format!("'{v}'"))
                .unwrap_or_else(|| "NULL".into()),
            r.fval
                .map(|v| format!("{v:?}"))
                .unwrap_or_else(|| "NULL".into()),
            r.flag
                .map(|v| v.to_string())
                .unwrap_or_else(|| "NULL".into()),
        );
        db.execute(&format!(
            "INSERT INTO m VALUES ({}, {iv}, {sv}, {fv}, {fl}, {}, '{}')",
            r.id,
            r.ts,
            motedb::types::Date::from_days(r.dd).to_iso()
        ))
        .unwrap();
        m.insert(r.id, r);
    };
    for id in 0..60 {
        ins(db, &mut m, fresh(id));
    }
    flush(db);
    // 第一轮: 改值（移出/移入各个 IN 列表与范围）、置 NULL、删除
    for id in 0..60i64 {
        if id % 5 == 0 {
            db.execute(&format!(
                "UPDATE m SET ival = {}, sval = 'zz' WHERE id = {id}",
                999 - id
            ))
            .unwrap();
            let r = m.get_mut(&id).unwrap();
            r.ival = Some(999 - id);
            r.sval = Some("zz");
        }
        if id % 17 == 0 {
            db.execute(&format!(
                "UPDATE m SET fval = NULL, flag = TRUE WHERE id = {id}"
            ))
            .unwrap();
            let r = m.get_mut(&id).unwrap();
            r.fval = None;
            r.flag = Some(true);
        }
    }
    for id in 0..60i64 {
        if id % 23 == 0 {
            db.execute(&format!("DELETE FROM m WHERE id = {id}"))
                .unwrap();
            m.remove(&id);
        }
    }
    flush(db);
    for id in 60..120 {
        ins(db, &mut m, fresh(id));
    }
    flush(db);
    // 第二轮: 两批都动; 删后重插; 对已删行的 UPDATE（0 行）
    for id in 0..120i64 {
        if id % 4 == 0 {
            // 已在第一轮删除的 id: SQL 影响 0 行，镜像同样跳过
            db.execute(&format!(
                "UPDATE m SET ival = ival + 1, sval = 'ap' WHERE id = {id}"
            ))
            .unwrap();
            if let Some(r) = m.get_mut(&id) {
                r.ival = r.ival.map(|v| v + 1);
                r.sval = Some("ap");
            }
        }
    }
    for id in 60..120i64 {
        if id % 19 == 0 {
            db.execute(&format!("DELETE FROM m WHERE id = {id}"))
                .unwrap();
            m.remove(&id);
        }
    }
    db.execute("UPDATE m SET sval = 'qq' WHERE id = 46")
        .unwrap();
    let mut again = fresh(23);
    again.ival = Some(7);
    again.sval = Some("eu");
    again.fval = Some(1.5);
    again.flag = Some(false);
    ins(db, &mut m, again);
    flush(db);
    for id in 120..150 {
        ins(db, &mut m, fresh(id));
    }
    flush(db);
    m
}

// ---------------------------------------------------------------------------
// 形状定义
// ---------------------------------------------------------------------------

type Pred = fn(&R) -> bool;
type Proj = fn(&R) -> Vec<String>;

const PROJS: &[(&str, Proj)] = &[
    ("*", |r| (0..7).map(|c| opt(r, c)).collect()),
    ("id", |r| vec![opt(r, 0)]),
    ("id, ival, sval", |r| vec![opt(r, 0), opt(r, 1), opt(r, 2)]),
    ("id, flag, ts, d", |r| {
        vec![opt(r, 0), opt(r, 4), opt(r, 5), opt(r, 6)]
    }),
];

fn preds() -> Vec<(String, Pred)> {
    // 时间/日期字面量从常量算出 —— 不手写数字（手抄已经错过两次）。
    let ts_gt = format!("WHERE ts > {}", BASE + 50_000_000);
    let d_ge = format!(
        "WHERE d >= '{}'",
        motedb::types::Date::from_days(D0 + 100).to_iso()
    );
    vec![
        (String::new(), |_| true),
        ("WHERE ival = 13".to_string(), |r| r.ival == Some(13)),
        ("WHERE ival > 20".to_string(), |r| {
            r.ival.is_some_and(|v| v > 20)
        }),
        ("WHERE ival <> 999".to_string(), |r| {
            r.ival.is_some_and(|v| v != 999)
        }),
        ("WHERE ival IS NULL".to_string(), |r| r.ival.is_none()),
        ("WHERE ival IS NOT NULL".to_string(), |r| r.ival.is_some()),
        ("WHERE sval = 'us'".to_string(), |r| r.sval == Some("us")),
        ("WHERE sval = 'zz'".to_string(), |r| r.sval == Some("zz")),
        ("WHERE sval <> 'us'".to_string(), |r| {
            r.sval.is_some_and(|v| v != "us")
        }),
        ("WHERE sval IS NULL".to_string(), |r| r.sval.is_none()),
        ("WHERE sval LIKE 'u%'".to_string(), |r| {
            r.sval.is_some_and(|v| v.starts_with('u'))
        }),
        ("WHERE sval LIKE '%p%'".to_string(), |r| {
            r.sval.is_some_and(|v| v.contains('p'))
        }),
        ("WHERE sval IN ('us')".to_string(), |r| r.sval == Some("us")),
        ("WHERE sval IN ('us', 'eu')".to_string(), |r| {
            matches!(r.sval, Some("us" | "eu"))
        }),
        ("WHERE sval IN ('us', 'eu', 'ap', 'zz')".to_string(), |r| {
            matches!(r.sval, Some("us" | "eu" | "ap" | "zz"))
        }),
        ("WHERE sval IN ('zz', 'ap')".to_string(), |r| {
            matches!(r.sval, Some("zz" | "ap"))
        }),
        ("WHERE sval IN ('nope', 'nada')".to_string(), |_| false),
        ("WHERE ival IN (13, 26)".to_string(), |r| {
            matches!(r.ival, Some(13 | 26))
        }),
        ("WHERE ival IN (13, 26, 39, 999)".to_string(), |r| {
            matches!(r.ival, Some(13 | 26 | 39 | 999))
        }),
        ("WHERE id IN (0, 23, 46, 149)".to_string(), |r| {
            matches!(r.id, 0 | 23 | 46 | 149)
        }),
        ("WHERE id BETWEEN 10 AND 40".to_string(), |r| {
            (10..=40).contains(&r.id)
        }),
        ("WHERE fval > 10.5".to_string(), |r| {
            r.fval.is_some_and(|v| v > 10.5)
        }),
        ("WHERE fval = 1.5".to_string(), |r| r.fval == Some(1.5)),
        ("WHERE flag = TRUE".to_string(), |r| r.flag == Some(true)),
        ("WHERE flag = FALSE".to_string(), |r| r.flag == Some(false)),
        ("WHERE ival > 10 AND flag = TRUE".to_string(), |r| {
            r.ival.is_some_and(|v| v > 10) && r.flag == Some(true)
        }),
        ("WHERE sval = 'us' OR ival > 40".to_string(), |r| {
            r.sval == Some("us") || r.ival.is_some_and(|v| v > 40)
        }),
        ("WHERE NOT (ival > 20)".to_string(), |r| {
            r.ival.is_some_and(|v| v <= 20)
        }),
        ("WHERE ival + 1 > 30".to_string(), |r| {
            r.ival.is_some_and(|v| v + 1 > 30)
        }),
        (ts_gt, |r| r.ts > BASE + 50_000_000),
        // ISO 文本字面量: BASE+50s = 2026-11-10 08:40:50（由 Python 核算）
        ("WHERE ts >= '2026-11-10 08:40:50'".to_string(), |r| {
            r.ts >= BASE + 50_000_000
        }),
        // 无法解析的日期字面量: 比较为 UNKNOWN → 0 行
        ("WHERE d >= 'not-a-date'".to_string(), |_| false),
        (d_ge, |r| r.dd >= D0 + 100),
        (
            "WHERE sval IN ('zz', 'ap') AND ival IS NOT NULL".to_string(),
            |r| matches!(r.sval, Some("zz" | "ap")) && r.ival.is_some(),
        ),
    ]
}

fn matched(m: &Mirror, keep: Pred) -> Vec<&R> {
    m.values().filter(|r| keep(r)).collect()
}

fn check_layout(db: &Database, m: &Mirror, label: &str) {
    let preds = preds();
    // 子查询 IN 的 oracle: sval ∈ {zz, ap} 的 id 集合
    let sub_ids: std::collections::HashSet<i64> = m
        .values()
        .filter(|r| matches!(r.sval, Some("zz" | "ap")))
        .map(|r| r.id)
        .collect();

    for (psql, proj) in PROJS.iter() {
        for (wsql, keep) in &preds {
            let sql_base = format!("SELECT {psql} FROM m {wsql}");
            // A1. 无 ORDER BY 的全量结果 = key 序（本表 key 即主键）。
            //     镜像是按 id 升序的 BTreeMap，所以这里同时钉住了行集与
            //     行序: 任何过期行/复活行/重复行，任何"段拼接"或"最新段
            //     优先"的顺序，都会在这里现形。
            let got = q(db, &sql_base);
            let want_ms: Vec<Vec<String>> = matched(m, *keep).iter().map(|r| proj(r)).collect();
            assert_eq!(
                got, want_ms,
                "[{label}] unordered full result (key order)\nsql: {sql_base}"
            );

            // A2. ORDER BY id: 逐行精确
            let got = q(db, &format!("{sql_base} ORDER BY id"));
            let want_ord: Vec<Vec<String>> = matched(m, *keep).iter().map(|r| proj(r)).collect();
            assert_eq!(
                got, want_ord,
                "[{label}] ordered rows\nsql: {sql_base} ORDER BY id"
            );

            // B. ORDER BY id DESC + LIMIT
            let want_desc: Vec<Vec<String>> = matched(m, *keep)
                .iter()
                .rev()
                .take(7)
                .map(|r| proj(r))
                .collect();
            let got = q(db, &format!("{sql_base} ORDER BY id DESC LIMIT 7"));
            assert_eq!(got, want_desc, "[{label}] desc+limit\nsql: {sql_base}");

            // C. 深分页
            let want_page: Vec<Vec<String>> = matched(m, *keep)
                .iter()
                .skip(5)
                .take(9)
                .map(|r| proj(r))
                .collect();
            let got = q(db, &format!("{sql_base} ORDER BY id ASC LIMIT 9 OFFSET 5"));
            assert_eq!(got, want_page, "[{label}] paging\nsql: {sql_base}");

            // D. 无 ORDER BY 的 LIMIT/OFFSET = key 序的前 limit 行（跳过前
            //    offset 行）。行集与行序都精确断言; 超出命中数的 OFFSET 得
            //    空页。
            for (lim, off) in [(7usize, 0usize), (1500, 3), (5, 2000)] {
                let got = q(db, &format!("{sql_base} LIMIT {lim} OFFSET {off}"));
                let want_page: Vec<Vec<String>> = matched(m, *keep)
                    .iter()
                    .skip(off)
                    .take(lim)
                    .map(|r| proj(r))
                    .collect();
                assert_eq!(
                    got, want_page,
                    "[{label}] LIMIT page (key order)\nsql: {sql_base} LIMIT {lim} OFFSET {off}"
                );
            }
        }
    }

    // 子查询 IN（半连接）: 单独处理（oracle 需要那把 id 集合）
    {
        let sql = "SELECT id, sval FROM m WHERE id IN (SELECT id FROM m WHERE sval IN ('zz', 'ap')) ORDER BY id";
        let got = q(db, sql);
        let want: Vec<Vec<String>> = m
            .values()
            .filter(|r| sub_ids.contains(&r.id))
            .map(|r| vec![opt(r, 0), opt(r, 2)])
            .collect();
        assert_eq!(got, want, "[{label}] IN-subquery semi-join");
    }

    // E. 聚合
    for (wsql, keep) in &preds {
        let sel = matched(m, *keep);
        let cnt = sel.len();
        let ivals: Vec<i64> = sel.iter().filter_map(|r| r.ival).collect();
        let fvals: Vec<f64> = sel.iter().filter_map(|r| r.fval).collect();
        let (fmin, fmax) = if fvals.is_empty() {
            ("∅".to_string(), "∅".to_string())
        } else {
            (
                format!(
                    "F:{:?}",
                    fvals.iter().cloned().fold(f64::INFINITY, f64::min)
                ),
                format!(
                    "F:{:?}",
                    fvals.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
                ),
            )
        };
        let one = |db: &Database, agg: &str| -> String {
            q(db, &format!("SELECT {agg} FROM m {wsql}"))[0][0].clone()
        };
        assert_eq!(
            one(db, "COUNT(*)"),
            format!("I:{cnt}"),
            "[{label}] COUNT(*) {wsql}"
        );
        assert_eq!(
            one(db, "COUNT(ival)"),
            format!("I:{}", ivals.len()),
            "[{label}] COUNT(ival) {wsql}"
        );
        assert_eq!(
            one(db, "SUM(ival)"),
            if ivals.is_empty() {
                "∅".into()
            } else {
                format!("I:{}", ivals.iter().sum::<i64>())
            },
            "[{label}] SUM(ival) {wsql}"
        );
        assert_eq!(
            one(db, "MIN(ts)"),
            sel.iter()
                .map(|r| r.ts)
                .min()
                .map(|v| format!("T:{v}"))
                .unwrap_or_else(|| "∅".into()),
            "[{label}] MIN(ts) {wsql}"
        );
        assert_eq!(
            one(db, "MAX(d)"),
            sel.iter()
                .map(|r| r.dd)
                .max()
                .map(|v| format!("D:{v}"))
                .unwrap_or_else(|| "∅".into()),
            "[{label}] MAX(d) {wsql}"
        );
        // 多聚合组合（融合/下推路径）: 类型变体必须逐列保持
        let combo = q(
            db,
            &format!(
                "SELECT COUNT(*), MIN(ival), MAX(ival), MIN(ts), MAX(ts), MIN(d), MAX(d), \
                 COUNT(fval), MIN(fval), MAX(fval) FROM m {wsql}"
            ),
        );
        let want_combo = [
            format!("I:{cnt}"),
            ivals
                .iter()
                .min()
                .map(|v| format!("I:{v}"))
                .unwrap_or_else(|| "∅".into()),
            ivals
                .iter()
                .max()
                .map(|v| format!("I:{v}"))
                .unwrap_or_else(|| "∅".into()),
            sel.iter()
                .map(|r| r.ts)
                .min()
                .map(|v| format!("T:{v}"))
                .unwrap_or_else(|| "∅".into()),
            sel.iter()
                .map(|r| r.ts)
                .max()
                .map(|v| format!("T:{v}"))
                .unwrap_or_else(|| "∅".into()),
            sel.iter()
                .map(|r| r.dd)
                .min()
                .map(|v| format!("D:{v}"))
                .unwrap_or_else(|| "∅".into()),
            sel.iter()
                .map(|r| r.dd)
                .max()
                .map(|v| format!("D:{v}"))
                .unwrap_or_else(|| "∅".into()),
            format!("I:{}", fvals.len()),
            fmin,
            fmax,
        ];
        assert_eq!(
            combo[0].join(","),
            want_combo.join(","),
            "[{label}] aggregate combo {wsql}"
        );
        let got_avg = one(db, "AVG(fval)");
        if fvals.is_empty() {
            assert_eq!(got_avg, "∅", "[{label}] AVG(fval) empty {wsql}");
        } else {
            let want = fvals.iter().sum::<f64>() / fvals.len() as f64;
            let g: f64 = got_avg[2..].parse().expect("AVG is a float");
            assert!(
                (g - want).abs() < 1e-9,
                "[{label}] AVG(fval) {wsql}: {g} vs {want}"
            );
        }
    }

    // F. GROUP BY
    for wsql in [
        "",
        "WHERE ival IS NOT NULL",
        "WHERE sval IN ('us', 'eu', 'ap', 'zz')",
    ] {
        let keep: Pred = match wsql {
            "" => |_| true,
            "WHERE ival IS NOT NULL" => |r| r.ival.is_some(),
            _ => |r| matches!(r.sval, Some("us" | "eu" | "ap" | "zz")),
        };
        let mut groups: Vec<(Option<&'static str>, i64, Option<i64>)> = Vec::new();
        for r in matched(m, keep) {
            match groups.iter_mut().find(|(k, _, _)| *k == r.sval) {
                Some((_, n, s)) => {
                    *n += 1;
                    if let Some(v) = r.ival {
                        *s = Some(s.unwrap_or(0) + v);
                    }
                }
                None => groups.push((r.sval, 1, r.ival)),
            }
        }
        // ORDER BY sval: NULL 组最先（引擎约定: NULL 最小）
        groups.sort_by(|a, b| match (&a.0, &b.0) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(x), Some(y)) => x.cmp(y),
        });
        let want: Vec<String> = groups
            .iter()
            .map(|(k, n, s)| {
                format!(
                    "{},I:{n},{}",
                    k.map(|v| format!("S:{v}")).unwrap_or_else(|| "∅".into()),
                    s.map(|v| format!("I:{v}")).unwrap_or_else(|| "∅".into())
                )
            })
            .collect();
        let got: Vec<String> = q(
            db,
            &format!("SELECT sval, COUNT(*), SUM(ival) FROM m {wsql} GROUP BY sval ORDER BY sval"),
        )
        .iter()
        .map(|r| r.join(","))
        .collect();
        assert_eq!(got, want, "[{label}] GROUP BY {wsql}");

        // GROUP BY 里的 MIN/MAX: 时间戳/日期列保持各自的类型变体
        let want_mm: Vec<String> = groups
            .iter()
            .map(|(k, _, _)| {
                let g: Vec<&R> = matched(m, keep)
                    .into_iter()
                    .filter(|r| r.sval == *k)
                    .collect();
                let or_none = |on: bool, v: String| if on { v } else { "∅".into() };
                format!(
                    "{},{},{},{},{},{}",
                    k.map(|v| format!("S:{v}")).unwrap_or_else(|| "∅".into()),
                    or_none(
                        !g.is_empty(),
                        format!("T:{}", g.iter().map(|r| r.ts).min().unwrap())
                    ),
                    or_none(
                        !g.is_empty(),
                        format!("D:{}", g.iter().map(|r| r.dd).max().unwrap())
                    ),
                    or_none(
                        g.iter().any(|r| r.ival.is_some()),
                        format!("I:{}", g.iter().filter_map(|r| r.ival).min().unwrap())
                    ),
                    or_none(
                        g.iter().any(|r| r.fval.is_some()),
                        format!(
                            "F:{:?}",
                            g.iter()
                                .filter_map(|r| r.fval)
                                .fold(f64::NEG_INFINITY, f64::max)
                        )
                    ),
                    or_none(
                        g.iter().any(|r| r.flag.is_some()),
                        if g.iter().filter_map(|r| r.flag).max().unwrap() {
                            "B:1"
                        } else {
                            "B:0"
                        }
                        .to_string()
                    ),
                )
            })
            .collect();
        let got_mm: Vec<String> = q(
            db,
            &format!(
                "SELECT sval, MIN(ts), MAX(d), MIN(ival), MAX(fval), MAX(flag) \
                 FROM m {wsql} GROUP BY sval ORDER BY sval"
            ),
        )
        .iter()
        .map(|r| r.join(","))
        .collect();
        assert_eq!(got_mm, want_mm, "[{label}] GROUP BY MIN/MAX {wsql}");
    }
}

#[test]
fn multisegment_with_cross_segment_versions_all_layouts() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("m.mote");
    let db = Database::create(&path).unwrap();
    let m = build(&db);
    check_layout(&db, &m, "L1 multi-segment");

    std::mem::drop(db);
    let db = Database::open(&path).unwrap();
    check_layout(&db, &m, "L2 reopened");

    let _ = db.execute("CHECKPOINT");
    check_layout(&db, &m, "L3 checkpoint");
}

/// 回归: IN 列表（≥2 个元素）曾返回被 UPDATE 行的旧版本并复活已删行。
#[test]
fn in_list_sees_newest_versions_and_no_deleted_rows() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)")
        .unwrap();
    for i in 0..10 {
        db.execute(&format!("INSERT INTO t VALUES ({i}, 'a', {})", i * 10))
            .unwrap();
    }
    let _ = db.query("SELECT id FROM t WHERE cat = 'zzz'");
    db.execute("UPDATE t SET cat = 'b', v = 999 WHERE id = 3")
        .unwrap();
    db.execute("UPDATE t SET cat = 'b', v = 999 WHERE id = 4")
        .unwrap();
    db.execute("UPDATE t SET cat = 'c', v = 999 WHERE id = 5")
        .unwrap();
    let _ = db.query("SELECT id FROM t WHERE cat = 'zzz'");
    db.execute("DELETE FROM t WHERE id = 7").unwrap();
    let _ = db.query("SELECT id FROM t WHERE cat = 'zzz'");

    let run = |sql: &str| -> Vec<Vec<String>> {
        db.query(sql)
            .unwrap()
            .iter()
            .map(|r| r.iter().map(canon).collect())
            .collect()
    };
    let rows = run("SELECT id, v FROM t WHERE cat IN ('a', 'b') ORDER BY id");
    let flat: Vec<String> = rows.iter().map(|r| r.join(":")).collect();
    assert_eq!(
        flat,
        [
            "I:0:I:0",
            "I:1:I:10",
            "I:2:I:20",
            "I:3:I:999",
            "I:4:I:999",
            "I:6:I:60",
            "I:8:I:80",
            "I:9:I:90"
        ],
        "IN-list must return the newest version of each row and no deleted rows"
    );
    assert_eq!(
        run("SELECT id FROM t WHERE cat IN ('a', 'b', 'c') ORDER BY id").len(),
        9
    );
}
