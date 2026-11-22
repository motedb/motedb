//! 🔴 FTS × DML 新鲜度 + 混合 RRF + TIME_BUCKET oracle —— 第五块扫描。
//!
//! A. 全文索引的 DML 跟随: 建索引后 INSERT 的行必须可搜、UPDATE 文本列
//!    后搜新不搜旧、DELETE 后不再命中（向量索引的对应坑已验干净，
//!    FTS 走不同的索引结构——分开验）。
//! B. 混合检索（RRF 融合 FTS + 向量）vs Rust 手算 RRF。
//! C. TIME_BUCKET 分桶边界 vs Rust 按秒切桶。

use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn ids(db: &Database, sql: &str) -> Vec<i64> {
    db.query(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            other => panic!("{sql}: id as {other:?}"),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// A. FTS × DML
// ---------------------------------------------------------------------------

#[test]
fn fts_index_tracks_insert_update_delete() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, body TEXT)")
        .unwrap();
    for i in 0..30 {
        db.execute(&format!(
            "INSERT INTO t VALUES ({i}, 'stock note {i} alpha beta')",
        ))
        .unwrap();
    }
    db.execute("CREATE TEXT INDEX t_body ON t(body)").unwrap();
    std::thread::sleep(std::time::Duration::from_millis(1500));

    // 基线: 全部命中
    let got = ids(
        &db,
        "SELECT id FROM t WHERE MATCH(body) AGAINST('alpha') ORDER BY id",
    );
    assert_eq!(got, (0..30).collect::<Vec<_>>(), "baseline MATCH");

    // A1. 建索引后 INSERT 的行必须可搜
    db.execute("INSERT INTO t VALUES (100, 'fresh alpha shipment')")
        .unwrap();
    db.execute("INSERT INTO t VALUES (101, 'no keyword here')")
        .unwrap();
    let got = ids(
        &db,
        "SELECT id FROM t WHERE MATCH(body) AGAINST('alpha') ORDER BY id",
    );
    assert_eq!(
        got.last(),
        Some(&100),
        "row inserted AFTER index build must be searchable (got {got:?})"
    );
    assert!(!got.contains(&101), "non-matching insert must stay out");

    // A2. UPDATE 文本列: 搜新词命中、搜旧词不再命中该行
    db.execute("UPDATE t SET body = 'zeta only text' WHERE id = 5")
        .unwrap();
    let alpha = ids(
        &db,
        "SELECT id FROM t WHERE MATCH(body) AGAINST('alpha') ORDER BY id",
    );
    assert!(
        !alpha.contains(&5),
        "old text must leave the index after UPDATE"
    );
    let zeta = ids(&db, "SELECT id FROM t WHERE MATCH(body) AGAINST('zeta')");
    assert_eq!(zeta, vec![5], "new text must be searchable after UPDATE");

    // A3. DELETE 后不再命中
    db.execute("DELETE FROM t WHERE id = 7").unwrap();
    let alpha = ids(
        &db,
        "SELECT id FROM t WHERE MATCH(body) AGAINST('alpha') ORDER BY id",
    );
    assert!(!alpha.contains(&7), "deleted row must leave the FTS index");
}

// ---------------------------------------------------------------------------
// B. 混合检索 RRF vs 手算
// ---------------------------------------------------------------------------

#[test]
fn hybrid_rrf_matches_manual_fusion() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE h (id INT PRIMARY KEY, body TEXT, emb VECTOR(4))")
        .unwrap();
    // 设计: 词项命中与向量近邻刻意错开，让 RRF 融合有信息量。
    //  id: 0..12; body 含 'target' 当 id<6; 向量与查询的距离随 id 递增。
    for i in 0..12 {
        let body = if i < 6 {
            "target keyword"
        } else {
            "other filler"
        };
        let e: Vec<String> = (0..4)
            .map(|d| format!("{:.3}", i as f64 + d as f64 * 0.1))
            .collect();
        db.execute(&format!(
            "INSERT INTO h VALUES ({i}, '{body}', '[{}]')",
            e.join(",")
        ))
        .unwrap();
    }
    db.execute("CREATE TEXT INDEX h_body ON h(body)").unwrap();
    db.execute("CREATE VECTOR INDEX h_emb ON h(emb)").unwrap();
    std::thread::sleep(std::time::Duration::from_millis(1500));

    let q = "[0.0, 0.1, 0.2, 0.3]";
    let sql = format!(
        "SELECT id FROM RRF(\
         (SELECT id FROM h WHERE MATCH(body) AGAINST('target') LIMIT 6), \
         (SELECT id FROM h ORDER BY emb <-> '{q}' LIMIT 6), \
         k = 5)"
    );
    let got = ids(&db, &sql);

    // 手算: FTS 命中 = [0..6)（文档序），向量序 = id 升序 [0..6)。
    // 两个子查询结果在此构造下完全重叠（id 0..6）——RRF 融合应恰为它们。
    let want: Vec<i64> = vec![0, 1, 2, 3, 4];
    assert_eq!(
        got, want,
        "RRF of identical sub-rankings = the ranking itself"
    );

    // 错开构造: FTS 用 'other'（id 6..12），向量仍近 id 0..6 → 融合集 = 两个集合的 RRF 加权。
    let sql = format!(
        "SELECT id FROM RRF(\
         (SELECT id FROM h WHERE MATCH(body) AGAINST('other') LIMIT 6), \
         (SELECT id FROM h ORDER BY emb <-> '{q}' LIMIT 6), \
         k = 6)"
    );
    let got = ids(&db, &sql);
    // 手算 RRF (rrk=60, 等权): id 0..5 各 1/60·1? —— 用公式: score = Σ 1/(rrk + rank_i)
    // FTS 排名: id6→1, 7→2, ... 11→6;  向量排名: id0→1, ... id5→6.
    let mut score: Vec<(i64, f64)> = Vec::new();
    for i in 0..6 {
        score.push((i as i64, 1.0 / (60.0 + (i + 1) as f64))); // vector rank i+1
        score.push(((i + 6) as i64, 1.0 / (60.0 + (i + 1) as f64))); // fts rank i+1
    }
    // 合并同 id（此构造无重叠 id，各自独立）→ 排序取前 6: 全部并列同分，
    // 并列序未定义 → 比较集合而不是顺序。
    let mut sorted = score.clone();
    sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    let want_set: Vec<i64> = {
        let mut s: Vec<i64> = sorted.iter().take(6).map(|(i, _)| *i).collect();
        s.sort();
        s
    };
    let mut got_set = got.clone();
    got_set.sort();
    assert_eq!(got_set, want_set, "RRF fused set must match manual fusion");
}

// ---------------------------------------------------------------------------
// C. TIME_BUCKET 分桶 vs 手算
// ---------------------------------------------------------------------------

#[test]
fn time_bucket_boundaries_match_manual() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE s (ts TIMESTAMP, v FLOAT)")
        .unwrap();
    // 10 分钟桶: 故意放跨桶边界样本（桶起点本身属下一桶）。
    let base = 1_794_300_000_000_000i64; // µs
    let min10 = 600_000_000i64;
    // ts = base + {0, 1, 599, 600, 601, 1199, 1200} 秒（相对值跨 3 个桶）
    let offs = [0i64, 1, 599, 600, 601, 1199, 1200, 1201];
    for (i, o) in offs.iter().enumerate() {
        db.execute(&format!(
            "INSERT INTO s VALUES ({}, {})",
            base + o * 1_000_000,
            i as f64
        ))
        .unwrap();
    }
    let rows = db
        .query(
            "SELECT TIME_BUCKET('10m', ts) AS b, COUNT(*) AS n, AVG(v) AS a \
                FROM s GROUP BY b ORDER BY b",
        )
        .unwrap();
    // 手算: 桶起点 = base + k*600s（对齐到 epoch? —— 以 base 为锚验证相对分桶）
    let fmt = |rows: &Vec<Vec<Value>>| -> Vec<String> {
        rows.iter()
            .map(|r| {
                let b = match &r[0] {
                    Value::Timestamp(t) => t.as_micros().to_string(),
                    other => format!("{other:?}"),
                };
                let n = match &r[1] {
                    Value::Integer(i) => i.to_string(),
                    o => format!("{o:?}"),
                };
                let a = match &r[2] {
                    Value::Float(f) => format!("{f:.3}"),
                    o => format!("{o:?}"),
                };
                format!("{b}/{n}/{a}")
            })
            .collect()
    };
    let got = fmt(&rows);
    // 期望: 手工分组 offs: [0,1,599]→3行(v 0,1,2 avg 1) ; [600,601,1199]→3行(3,4,5 avg 4) ; [1200,1201]→2行(6,7 avg 6.5)
    // 桶标签的时间值取决于锚点（epoch 对齐 vs 首行对齐）——断言分组结构 +
    // AVG 序列，标签单独按 epoch 对齐验证。
    assert_eq!(rows.len(), 3, "10m buckets: {offs:?} must span 3 buckets");
    let counts: Vec<String> = rows
        .iter()
        .map(|r| match &r[1] {
            Value::Integer(i) => i.to_string(),
            o => format!("{o:?}"),
        })
        .collect();
    assert_eq!(counts, vec!["3", "3", "2"], "bucket populations");
    let avgs: Vec<f64> = rows
        .iter()
        .map(|r| match &r[2] {
            Value::Float(f) => *f,
            o => 0.0,
        })
        .collect();
    let want_avg = [1.0f64, 4.0, 6.5];
    for (g, w) in avgs.iter().zip(want_avg.iter()) {
        assert!((g - w).abs() < 1e-6, "bucket avg {g} vs {w}");
    }
    // 桶标签: epoch 对齐时 base=1794300000000000000µs 是否恰为 10m 边界?
    // base = 2026-11-11T...，不保证整刻 —— 检查相邻桶间距恰为 600s。
    let b0 = match &rows[0][0] {
        Value::Timestamp(t) => t.as_micros(),
        o => 0,
    };
    let b1 = match &rows[1][0] {
        Value::Timestamp(t) => t.as_micros(),
        o => 0,
    };
    let b2 = match &rows[2][0] {
        Value::Timestamp(t) => t.as_micros(),
        o => 0,
    };
    assert_eq!(b1 - b0, min10, "adjacent bucket labels 600s apart");
    assert_eq!(b2 - b1, min10, "adjacent bucket labels 600s apart");
    // 首桶必须包含 ts=base（off 0）→ 标签 ≤ base 且 > base-600s（同一桶窗内）
    assert!(
        b0 <= base && base - b0 < min10,
        "first bucket label anchors its window"
    );
}
