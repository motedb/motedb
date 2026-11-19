//! 🔴 重启一致性 × DML 矩阵 —— 对 Rust 暴力 oracle 的两块新扫描。
//!
//! 路径矩阵（test_path_matrix_correctness.rs）验证了"同一进程内查询形状
//! 正确"; 嵌入式数据库还有两条等重要的用户路径此前没有 oracle:
//!
//!   A. **重启一致性**: 同一批查询 close→reopen 之后必须逐行一致
//!      （含 WAL 未 checkpoint 的路径——段/清单/WAL 恢复的合成结果）。
//!   B. **DML 后读回**: UPDATE 部分列不得丢失未更新列、DELETE 后
//!      LATEST BY/聚合正确、UPSERT 语义、整链对 Rust 侧镜像状态校验。

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

fn date0() -> i32 {
    motedb::types::Date::parse("2026-01-01").unwrap().days()
}

const N: usize = 30;

fn setup(db: &Database) {
    db.execute(
        "CREATE TABLE t (id INT PRIMARY KEY, ival INT, fval REAL, sval TEXT, \
         flag BOOLEAN, ts TIMESTAMP, d DATE)",
    )
    .unwrap();
    for i in 0..N as i64 {
        let ival = if i % 9 == 4 {
            "NULL"
        } else {
            &((i * 7) % 19).to_string()
        };
        let fval = if i % 11 == 3 {
            "NULL".to_string()
        } else {
            format!("{:?}", i as f64 * 0.5 - 7.0)
        };
        let sval = if i % 13 == 5 {
            "NULL".to_string()
        } else {
            format!("'{}'", ["alpha", "beta", "gamma"][i as usize % 3])
        };
        let flag = if i % 17 == 2 {
            "NULL".to_string()
        } else {
            (i % 2 == 0).to_string()
        };
        db.execute(&format!(
            "INSERT INTO t VALUES ({i}, {ival}, {fval}, {sval}, {flag}, {}, '{}')",
            1_794_300_000_000_000 + i * 133_000_000,
            motedb::types::Date::from_days(date0() + i as i32).to_iso(),
        ))
        .unwrap();
    }
}

/// 覆盖面代表性查询子集（过滤/排序/分页/投影/FTS/聚合/LATEST BY 混合）。
const QUERIES: &[&str] = &[
    "SELECT id, ts, d, flag FROM t ORDER BY id LIMIT 12",
    "SELECT ts FROM t WHERE ival >= 7 ORDER BY ts DESC",
    "SELECT d, sval FROM t WHERE sval = 'beta' ORDER BY d ASC",
    "SELECT fval, ival FROM t WHERE fval >= -1.0 ORDER BY fval, id",
    "SELECT id, ts FROM t WHERE flag = TRUE ORDER BY id DESC LIMIT 9",
    "SELECT * FROM t ORDER BY id LIMIT 30",
    "SELECT MIN(ts), MAX(ts), COUNT(*) FROM t",
    "SELECT sval, COUNT(*) AS n FROM t GROUP BY sval ORDER BY n DESC, sval ASC",
    "SELECT id, ts FROM t WHERE ts >= 1794300000000000 ORDER BY ts ASC",
];

// ---------------------------------------------------------------------------
// A. 重启一致性（两条关闭路径: 显式 CHECKPOINT 后关闭 / 脏 WAL 直接关）
// ---------------------------------------------------------------------------

fn snapshot(db: &Database) -> Vec<Vec<String>> {
    QUERIES
        .iter()
        .map(|q| {
            let rows = db.query(q).unwrap_or_else(|e| panic!("{q}: {e}"));
            rows_canon(&rows)
        })
        .collect()
}

fn reopen_and_compare(dir_path: &std::path::Path, before: &[Vec<String>], label: &str) {
    let db2 = Database::open(dir_path).expect("reopen");
    let after = snapshot(&db2);
    for (qi, (b, a)) in before.iter().zip(after.iter()).enumerate() {
        assert_eq!(
            b, a,
            "{label}: query {qi} ({}) differs after reopen\nbefore: {b:?}\nafter:  {a:?}",
            QUERIES[qi]
        );
    }
    drop(db2);
}

#[test]
fn path_matrix_survives_reopen_clean() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    setup(&db);
    let before = snapshot(&db);
    db.execute("CHECKPOINT").unwrap();
    drop(db); // clean close after checkpoint
    reopen_and_compare(dir.path(), &before, "checkpointed close");
}

#[test]
fn path_matrix_survives_reopen_dirty_wal() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    setup(&db);
    // No CHECKPOINT: force the WAL-replay path on reopen.
    let before = snapshot(&db);
    drop(db);
    reopen_and_compare(dir.path(), &before, "dirty WAL close");
}

#[test]
fn path_matrix_survives_reopen_after_dml() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    setup(&db);
    // Mutate then reopen: UPDATE partial columns + DELETE a slice + re-INSERT.
    db.execute("UPDATE t SET sval = 'renamed', flag = FALSE WHERE id >= 20")
        .unwrap();
    db.execute("DELETE FROM t WHERE id % 7 = 3").unwrap();
    db.execute("INSERT INTO t VALUES (100, 5, 1.5, 'zeta', TRUE, 1794300400000000, '2026-02-15')")
        .unwrap();
    let before = snapshot(&db);
    drop(db);
    reopen_and_compare(dir.path(), &before, "post-DML close");
}

// ---------------------------------------------------------------------------
// B. DML 后读回（Rust 侧镜像状态做 oracle）
// ---------------------------------------------------------------------------

/// Mirror of a row kept in Rust; SQL mutations are applied to both sides.
#[derive(Clone, Debug)]
struct MRow {
    id: i64,
    ival: Option<i64>,
    fval: Option<f64>,
    sval: Option<String>,
    flag: Option<bool>,
    ts: i64,
    dday: i32,
}

fn mirror_fixture() -> Vec<MRow> {
    (0..24i64)
        .map(|i| MRow {
            id: i,
            ival: if i % 8 == 3 { None } else { Some((i * 5) % 17) },
            fval: if i % 10 == 6 {
                None
            } else {
                Some(i as f64 * 0.25 - 3.0)
            },
            sval: if i % 12 == 7 {
                None
            } else {
                Some(["aa", "bb", "cc", "dd"][(i % 4) as usize].to_string())
            },
            flag: if i % 14 == 5 { None } else { Some(i % 3 == 0) },
            ts: 1_794_300_000_000_000 + i * 111_000_000,
            dday: date0() + i as i32,
        })
        .collect()
}

fn insert_mirror(db: &Database, rows: &[MRow]) {
    db.execute(
        "CREATE TABLE m (id INT PRIMARY KEY, ival INT, fval REAL, sval TEXT, \
         flag BOOLEAN, ts TIMESTAMP, d DATE)",
    )
    .unwrap();
    for r in rows {
        let ival = r
            .ival
            .map(|v| v.to_string())
            .unwrap_or_else(|| "NULL".into());
        let fval = r
            .fval
            .map(|v| format!("{v:?}"))
            .unwrap_or_else(|| "NULL".into());
        let sval = r
            .sval
            .as_ref()
            .map(|v| format!("'{v}'"))
            .unwrap_or_else(|| "NULL".into());
        let flag = r
            .flag
            .map(|v| v.to_string())
            .unwrap_or_else(|| "NULL".into());
        db.execute(&format!(
            "INSERT INTO m VALUES ({}, {ival}, {fval}, {sval}, {flag}, {}, '{}')",
            r.id,
            r.ts,
            motedb::types::Date::from_days(r.dday).to_iso(),
        ))
        .unwrap();
    }
}

fn mirror_expected(rows: &[MRow], keep: &dyn Fn(&MRow) -> bool) -> Vec<String> {
    let mut kept: Vec<&MRow> = rows.iter().filter(|r| keep(r)).collect();
    kept.sort_by_key(|r| r.id);
    kept.iter()
        .map(|r| {
            vec![
                format!("I:{}", r.id),
                r.ival
                    .map(|v| format!("I:{v}"))
                    .unwrap_or_else(|| "∅".into()),
                r.fval
                    .map(|v| format!("F:{v:?}"))
                    .unwrap_or_else(|| "∅".into()),
                r.sval
                    .as_ref()
                    .map(|v| format!("S:{v}"))
                    .unwrap_or_else(|| "∅".into()),
                r.flag
                    .map(|v| format!("B:{}", if v { 1 } else { 0 }))
                    .unwrap_or_else(|| "∅".into()),
                format!("T:{}", r.ts),
                format!("D:{}", r.dday),
            ]
            .join(",")
        })
        .collect()
}

fn check_readback(db: &Database, rows: &[MRow], stage: &str) {
    let got = rows_canon(&db.query("SELECT * FROM m ORDER BY id").unwrap());
    let want = mirror_expected(rows, &|_| true);
    assert_eq!(got, want, "{stage}: full readback diverges from mirror");
}

#[test]
fn update_partial_columns_keep_others() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let mut rows = mirror_fixture();
    insert_mirror(&db, &rows);

    // 1. Update ONE column on a text-filtered subset — every other column of
    //    matched rows must survive byte-identically.
    db.execute("UPDATE m SET flag = TRUE WHERE sval = 'bb'")
        .unwrap();
    for r in rows.iter_mut() {
        if r.sval.as_deref() == Some("bb") {
            r.flag = Some(true);
        }
    }
    check_readback(&db, &rows, "after single-column UPDATE");

    // 2. Update two columns via range on the PK.
    db.execute("UPDATE m SET ival = 99, fval = -0.5 WHERE id >= 10 AND id < 18")
        .unwrap();
    for r in rows.iter_mut() {
        if (10..18).contains(&r.id) {
            r.ival = Some(99);
            r.fval = Some(-0.5);
        }
    }
    check_readback(&db, &rows, "after two-column range UPDATE");

    // 3. Update to NULL explicitly (was non-NULL), then away from NULL.
    db.execute("UPDATE m SET sval = NULL WHERE id = 0").unwrap();
    db.execute("UPDATE m SET sval = 'fresh' WHERE id = 0")
        .unwrap();
    for r in rows.iter_mut() {
        if r.id == 0 {
            r.sval = Some("fresh".to_string());
        }
    }
    check_readback(&db, &rows, "after NULL round-trip UPDATE");
}

#[test]
fn delete_then_latest_and_aggregates() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let mut rows = mirror_fixture();
    insert_mirror(&db, &rows);

    db.execute("DELETE FROM m WHERE ival IS NULL").unwrap();
    rows.retain(|r| r.ival.is_some());
    check_readback(&db, &rows, "after DELETE IS NULL");

    // Aggregates over the mutated table vs mirror.
    let cnt = &db.query("SELECT COUNT(*) FROM m").unwrap()[0][0];
    assert_eq!(canon(cnt), format!("I:{}", rows.len()));

    let max_ts = rows.iter().map(|r| r.ts).max().unwrap();
    let got = &db.query("SELECT MAX(ts) FROM m").unwrap()[0][0];
    assert_eq!(canon(got), format!("T:{max_ts}"));

    // LATEST BY after deletes: per sval latest id (non-NULL sval only).
    let got = db
        .query("SELECT sval, id FROM m WHERE sval IS NOT NULL LATEST BY sval ORDER BY id")
        .unwrap();
    let mut want: Vec<(String, i64)> = Vec::new();
    for r in &rows {
        if let Some(s) = &r.sval {
            if !want.iter().any(|(ws, _)| ws == s) {
                want.push((format!("S:{s}"), r.id));
            }
        }
    }
    // LATEST BY keeps the row with max ts per key; ts is monotonic in id.
    want.clear();
    for r in &rows {
        if let Some(s) = &r.sval {
            match want
                .iter_mut()
                .find(|(ws, _): &&mut (String, i64)| ws == &format!("S:{s}"))
            {
                Some(e) => e.1 = e.1.max(r.id),
                None => want.push((format!("S:{s}"), r.id)),
            }
        }
    }
    want.sort_by_key(|(_, id)| *id);
    let got_canon: Vec<String> = got
        .iter()
        .map(|r| format!("{},{}", canon(&r[0]), canon(&r[1])))
        .collect();
    let want_canon: Vec<String> = want.iter().map(|(s, id)| format!("{s},I:{id}")).collect();
    assert_eq!(got_canon, want_canon, "LATEST BY after DELETE diverges");
}

#[test]
fn upsert_semantics_via_pk_update() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let mut rows = mirror_fixture();
    insert_mirror(&db, &rows);

    // Upsert an existing PK: full-row replace (INSERT OR REPLACE semantics
    // if supported; otherwise the engine must error rather than duplicate).
    let outcome = db.execute(
        "INSERT OR REPLACE INTO m VALUES (7, 1, 2.25, 'upserted', FALSE, 1794300009999000, '2026-03-01')",
    );
    match outcome {
        Ok(_) => {
            for r in rows.iter_mut() {
                if r.id == 7 {
                    *r = MRow {
                        id: 7,
                        ival: Some(1),
                        fval: Some(2.25),
                        sval: Some("upserted".into()),
                        flag: Some(false),
                        ts: 1_794_300_009_999_000,
                        dday: motedb::types::Date::parse("2026-03-01").unwrap().days(),
                    };
                }
            }
        }
        Err(e) => {
            // Rejected upsert must not mutate anything.
            assert!(
                db.query("SELECT COUNT(*) FROM m").unwrap()[0][0]
                    == Value::Integer(rows.len() as i64),
                "failed INSERT OR REPLACE still mutated: {e}"
            );
        }
    }
    check_readback(&db, &rows, "after INSERT OR REPLACE");
}
