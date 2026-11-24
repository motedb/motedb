//! 🔴 DISTINCT 语义回归 —— 对 Rust 侧集合的 oracle。
//!
//! 物化路径与聚合下推路径里各有一份 DISTINCT 快路径（同一份代码的两份
//! 拷贝），曾经:
//! * 不检查 WHERE: `SELECT DISTINCT c FROM t WHERE …` 返回未过滤列的去重值
//!   （`DISTINCT id WHERE v > 10` = 全表行数）
//! * 只对 `out_pos[0]` 去重: `SELECT DISTINCT a, b` 返回 a 的值、每行只有
//!   一列（声明的列数与行宽不符）
//! * 文本列走 `distinct_text_values`（启发式）:
//!   - 连续 4096 行无新值即提前退出 → 值成簇存放的数据（按主机/地区分堆
//!     的日志与时间序表）只返回第一簇的值
//!   - 最多 10000 个值
//!   - NULL 渲染成 ""，表里一旦有墓碑又被整个丢掉
//!
//! oracle: Rust 侧按同样谓词过滤后的集合。DISTINCT 无 ORDER BY 时行序
//! 不作保证 → 比较多重集合。

use motedb::types::Value;
use motedb::Database;
use std::collections::BTreeSet;
use tempfile::TempDir;

type Tup = (i64, Option<&'static str>, i64);

fn canon(v: &Value) -> String {
    match v {
        Value::Null => "∅".into(),
        Value::Integer(i) => format!("I:{i}"),
        Value::Text(s) => format!("S:{}", s.as_str()),
        other => format!("{other:?}"),
    }
}

/// 结果作为多重集合（每行规范化后排序）。
fn set(db: &Database, sql: &str) -> BTreeSet<String> {
    db.query(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| r.iter().map(canon).collect::<Vec<_>>().join(","))
        .collect()
}

fn flush(db: &Database) {
    let _ = db.query("SELECT id FROM t WHERE region = 'zzz'");
}

fn small(db: &Database) {
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, region TEXT, v INT)")
        .unwrap();
    let mut vals = String::new();
    for i in 0..30 {
        if i > 0 {
            vals.push(',');
        }
        let region = if i % 11 == 5 {
            "NULL"
        } else {
            ["'a'", "'b'", "'c'"][i % 3]
        };
        vals.push_str(&format!("({i}, {region}, {})", (i * 7) % 20));
    }
    db.execute(&format!("INSERT INTO t VALUES {vals}")).unwrap();
    flush(db);
}

fn mirror_rows(with_null_region: bool) -> Vec<Tup> {
    (0..30i64)
        .map(|i| {
            let region = if with_null_region && i % 11 == 5 {
                None
            } else {
                Some(["a", "b", "c"][(i % 3) as usize])
            };
            (i, region, (i * 7) % 20)
        })
        .collect()
}

#[test]
fn distinct_applies_where() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    small(&db);
    let rows = mirror_rows(true);

    let want = |keep: &dyn Fn(&Tup) -> bool| -> BTreeSet<String> {
        rows.iter()
            .filter(|r| keep(r))
            .map(|r| format!("I:{}", r.0))
            .collect()
    };
    assert_eq!(
        set(&db, "SELECT DISTINCT region FROM t WHERE region = 'a'"),
        BTreeSet::from(["S:a".to_string()]),
        "WHERE must be applied before dedup"
    );
    assert_eq!(
        set(&db, "SELECT DISTINCT id FROM t WHERE region = 'a'"),
        want(&|r| r.1 == Some("a")),
        "DISTINCT id WHERE text = …"
    );
    assert_eq!(
        set(&db, "SELECT DISTINCT id FROM t WHERE v > 10"),
        want(&|r| r.2 > 10),
        "DISTINCT id WHERE int > …"
    );
    assert_eq!(
        set(&db, "SELECT DISTINCT id FROM t WHERE region IN ('a', 'b')"),
        want(&|r| matches!(r.1, Some("a" | "b"))),
        "DISTINCT id WHERE text IN (…)"
    );
    assert_eq!(
        set(
            &db,
            "SELECT COUNT(DISTINCT region) FROM t WHERE region = 'a'"
        ),
        BTreeSet::from(["I:1".to_string()])
    );
}

#[test]
fn distinct_multi_column_emits_every_column() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    small(&db);
    let rows = mirror_rows(true);

    let all: BTreeSet<String> = rows
        .iter()
        .map(|r| {
            format!(
                "{},I:{}",
                r.1.map(|v| format!("S:{v}")).unwrap_or_else(|| "∅".into()),
                r.2
            )
        })
        .collect();
    let got = set(&db, "SELECT DISTINCT region, v FROM t");
    assert_eq!(
        got, all,
        "DISTINCT a, b dedups on the full row and returns both columns"
    );

    let filtered: BTreeSet<String> = rows
        .iter()
        .filter(|r| r.0 < 9)
        .map(|r| {
            format!(
                "{},I:{}",
                r.1.map(|v| format!("S:{v}")).unwrap_or_else(|| "∅".into()),
                r.2
            )
        })
        .collect();
    assert_eq!(
        set(&db, "SELECT DISTINCT region, v FROM t WHERE id < 9"),
        filtered
    );

    // 行宽必须等于声明的列数（绑定层按列名解包，短行会出错）。
    let cols_rows = db
        .query("SELECT DISTINCT region, v FROM t WHERE id < 9")
        .unwrap();
    assert!(
        cols_rows.iter().all(|r| r.len() == 2),
        "every row has 2 values"
    );
}

/// 值成簇存放: 3 个地区各 `per` 行、按地区顺序写入。
fn clustered(db: &Database, per: usize, batches: usize) {
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, region TEXT)")
        .unwrap();
    let total = per * 3;
    let per_batch = total / batches;
    let mut id = 0usize;
    for b in 0..batches {
        let mut chunk = String::new();
        for _ in 0..per_batch {
            if !chunk.is_empty() {
                chunk.push(',');
            }
            chunk.push_str(&format!("({}, '{}')", id, ["us", "eu", "ap"][id / per]));
            id += 1;
        }
        db.execute(&format!("INSERT INTO t VALUES {chunk}"))
            .unwrap();
        flush(db);
        let _ = b;
    }
}

#[test]
fn distinct_text_is_exact_on_value_clustered_data() {
    // 4096 < 每簇行数: 旧的"稳定窗口"提前退出在第一簇中途就停了，
    // 后两个地区整体消失。
    for batches in [1usize, 3, 7] {
        let dir = TempDir::new().unwrap();
        let db = Database::create(dir.path()).unwrap();
        clustered(&db, 5000, batches);
        let want = BTreeSet::from(["S:ap".to_string(), "S:eu".to_string(), "S:us".to_string()]);
        assert_eq!(
            set(&db, "SELECT DISTINCT region FROM t"),
            want,
            "{batches} segment(s)"
        );
        // 与聚合口径一致
        assert_eq!(
            set(&db, "SELECT COUNT(DISTINCT region) FROM t"),
            BTreeSet::from(["I:3".to_string()])
        );
    }
}

#[test]
fn distinct_text_has_no_value_cap() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, tag TEXT)")
        .unwrap();
    let n = 25_000;
    let mut chunk = String::new();
    for i in 0..n {
        if i % 1000 != 0 {
            chunk.push(',');
        }
        chunk.push_str(&format!("({i}, 'tag{i}')"));
        if i % 1000 == 999 {
            db.execute(&format!("INSERT INTO t VALUES {chunk}"))
                .unwrap();
            chunk.clear();
        }
    }
    flush(&db);
    let got = db.query("SELECT DISTINCT tag FROM t").unwrap();
    assert_eq!(
        got.len(),
        n,
        "25,000 distinct values — not capped at 10,000"
    );
    let uniq: BTreeSet<String> = got.iter().map(|r| canon(&r[0])).collect();
    assert_eq!(uniq.len(), n, "and they are all distinct");
}

#[test]
fn distinct_text_null_is_a_value_of_its_own() {
    // 有无墓碑两种状态: NULL 都必须作为恰好一个去重值出现（既不是 ""，
    // 也不被丢掉）。
    for with_tombstone in [false, true] {
        let dir = TempDir::new().unwrap();
        let db = Database::create(dir.path()).unwrap();
        small(&db);
        if with_tombstone {
            db.execute("DELETE FROM t WHERE id = 7").unwrap();
            flush(&db);
        }
        let got = set(&db, "SELECT DISTINCT region FROM t");
        let want = BTreeSet::from([
            "∅".to_string(),
            "S:a".to_string(),
            "S:b".to_string(),
            "S:c".to_string(),
        ]);
        assert_eq!(got, want, "tombstone present = {with_tombstone}");
    }
}
