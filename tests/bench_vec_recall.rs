//! B3 向量召回测量（0.18 设计文档 §B3 第一步: 先测量）。
//!
//! 场景: 建索引 → 增量插入 N 批（含 UPDATE/DELETE churn）→ 每阶段
//! top-k 索引路径 vs 暴力精确扫对拍，输出 recall@{1,10,100}。
//! 已知的 stranded-node 增量场景（use_index 路由注释实测 0.03–0.09）
//! 在此固化; 修复后阈值 0.98@10 起步。
//!
//! Run: make vec-recall（或 cargo test --release --test bench_vec_recall -- --ignored --nocapture）

use motedb::sql::QueryResult;
use motedb::types::Value;
use motedb::Database;

const DIM: usize = 64;
const N0: usize = 20_000; // ≥ 默认 MOTE_VEC_INDEX_MIN_ROWS，建索引
const NQ: usize = 30; // 测量查询数
const BATCH: usize = 500; // 增量批大小

fn rng_next(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *state = x;
    x.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

/// 第 id 个向量的确定性生成（簇状: 8 个簇中心 + 抖动，贴近真实嵌入）。
fn emb_of(id: i64) -> String {
    let cluster = (id as u64 % 8) as f32;
    let jitter = |i: i64, k: usize| -> f32 {
        let mut s = (id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15 ^ (k as u64));
        s ^= s >> 33;
        (s % 1000) as f32 / 1000.0 - 0.5
    };
    let comps: Vec<String> = (0..DIM)
        .map(|k| {
            let v = if k % 8 == (cluster as usize) {
                1.0 + jitter(id, k) * 0.2
            } else {
                jitter(id, k) * 0.2
            };
            format!("{v:.4}")
        })
        .collect();
    format!("'[{ }]'", comps.join(", "))
}

/// 查询锚点三分布: 旧向量（建索引期）/ 新向量（增量期，stranded 嫌疑
/// 人群）/ 随机。stranded 场景的受害者是"真近邻在索引外"的查询。
fn query_vec(seed: u64, max_id: i64) -> String {
    match seed % 3 {
        0 => emb_of((seed % 10_000) as i64), // 旧行
        1 => emb_of(20_000 + (seed % (max_id - 20_000).max(1) as u64) as i64), // 新行
        _ => {
            let mut s = seed;
            s ^= s >> 33;
            emb_of((s % max_id as u64) as i64)
        }
    }
}

fn top_k_ids(db: &Database, sql: &str) -> Vec<i64> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows
            .iter()
            .map(|r| match &r[0] {
                Value::Integer(i) => *i,
                o => panic!("{o:?}"),
            })
            .collect(),
        o => panic!("{o:?}"),
    }
}

fn recall_at<'a>(truth: &'a [i64], got: &'a [i64], k: usize) -> f64 {
    let t: std::collections::HashSet<&i64> = truth.iter().take(k).collect();
    if t.is_empty() {
        return 1.0;
    }
    let hits = got.iter().take(k).filter(|g| t.contains(g)).count();
    hits as f64 / t.len() as f64
}

fn percentiles(mut v: Vec<f64>, p: f64) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((v.len() as f64 - 1.0) * p).round() as usize;
    v[idx.min(v.len() - 1)]
}

/// 一轮测量: 索引路径 vs 暴力（MOTE_VEC_INDEX_MIN_ROWS 顶到天上 → 必走
/// brute）——同一查询同一 ORDER BY 语句。
fn measure(db: &Database, phase: &str, seed: &mut u64) -> (Vec<f64>, Vec<f64>) {
    let saved = std::env::var("MOTE_VEC_INDEX_MIN_ROWS").ok();
    let mut r10_idx = Vec::new();
    let mut r10_brute = Vec::new();
    let max_id = 20_000 + 6 * BATCH as i64;
    let mut t_index = std::time::Duration::ZERO;
    let mut t_brute = std::time::Duration::ZERO;
    for q in 0..NQ {
        let qv = query_vec(rng_next(seed) ^ (q as u64 + 1), max_id);
        // 索引路径: 阈值 0 → 表 ≥0 行就走索引。
        std::env::set_var("MOTE_VEC_INDEX_MIN_ROWS", "0");
        let t0 = std::time::Instant::now();
        let via_index = top_k_ids(
            db,
            &format!("SELECT id FROM v ORDER BY emb <-> {qv} LIMIT 100"),
        );
        t_index += t0.elapsed();
        std::env::remove_var("MOTE_VEC_INDEX_MIN_ROWS");
        // 暴力路径: 阈值顶到天上 → 必走全表精确扫。
        std::env::set_var("MOTE_VEC_INDEX_MIN_ROWS", "999999999");
        let t1 = std::time::Instant::now();
        let brute = top_k_ids(
            db,
            &format!("SELECT id FROM v ORDER BY emb <-> {qv} LIMIT 100"),
        );
        t_brute += t1.elapsed();
        std::env::remove_var("MOTE_VEC_INDEX_MIN_ROWS");

        r10_idx.push(recall_at(&brute, &via_index, 10));
        r10_brute.push(1.0); // 暴力 vs 暴力自参照
    }
    if let Some(s) = saved {
        std::env::set_var("MOTE_VEC_INDEX_MIN_ROWS", s);
    }
    let p50 = percentiles(r10_idx.clone(), 0.50);
    let p99 = percentiles(r10_idx.clone(), 0.99);
    println!(
        "[vec-recall] {phase:24} recall@10 p50={p50:.4} p99={p99:.4} min={:.4} | t_index={:.0}µs/q t_brute={:.0}µs/q",
        r10_idx.iter().cloned().fold(f64::INFINITY, f64::min),
        t_index.as_micros() as f64 / NQ as f64,
        t_brute.as_micros() as f64 / NQ as f64,
    );
    (r10_idx, r10_brute)
}

#[test]
#[ignore = "bench: B3 召回测量，make vec-recall 运行"]
fn vec_recall_incremental_churn() {
    // 阈值: 修复后启用（起步 0.98@10）。测量先行——首次运行如实报告。
    let threshold = std::env::var("MOTE_VEC_RECALL_MIN")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.85);

    let dir = tempfile::TempDir::new().unwrap();
    // 🔴 stranded 场景复现关键: BatchOnly——图只在 checkpoint 重建，
    // 增量批悬空（Hybrid 默认下新向量即时入图，stranded 不复现）。
    let cfg = motedb::DBConfig {
        index_update_strategy: motedb::config::IndexUpdateStrategy::BatchOnly,
        ..Default::default()
    };
    let db = Database::create_with_config(dir.path(), cfg).unwrap();
    db.execute(&format!(
        "CREATE TABLE v (id INT PRIMARY KEY, emb VECTOR({DIM}))"
    ))
    .unwrap();
    let mut seed = 0xB3_2026_1110u64;

    // ── 初始批量: 建索引基础 ──
    for i in 0..N0 {
        db.execute(&format!("INSERT INTO v VALUES ({i}, {})", emb_of(i as i64)))
            .unwrap();
    }
    db.checkpoint().unwrap();
    // 🔴 显式建向量索引（use_index 的前提; B-06 后索引优先为默认路由）。
    let t0 = std::time::Instant::now();
    db.execute("CREATE VECTOR INDEX vidx ON v(emb)").unwrap();
    println!(
        "[vec-recall] CREATE VECTOR INDEX: {:.1}s",
        t0.elapsed().as_secs_f64()
    );
    let mut s = seed;
    let (r, _) = measure(&db, "after-bulk-build", &mut s);
    seed = s;

    // ── 增量批 + churn ──
    for b in 0..6 {
        let base = (N0 + b * BATCH) as i64;
        for i in base..base + BATCH as i64 {
            db.execute(&format!("INSERT INTO v VALUES ({i}, {})", emb_of(i)))
                .unwrap();
        }
        // churn: 更新 10% + 删除 5%（老行与新行各半）
        for j in 0..(BATCH / 10) as i64 {
            let victim = if j % 2 == 0 {
                (rng_next(&mut seed) % N0 as u64) as i64
            } else {
                base + (rng_next(&mut seed) % BATCH as u64) as i64
            };
            let _ = db.execute(&format!(
                "UPDATE v SET emb = {} WHERE id = {victim}",
                emb_of(victim + 1_000_000)
            ));
        }
        for _ in 0..(BATCH / 20) {
            let victim = (rng_next(&mut seed) % N0 as u64) as i64;
            let _ = db.execute(&format!("DELETE FROM v WHERE id = {victim}"));
        }
        if b == 2 {
            db.checkpoint().unwrap();
        }
        let phase = format!("after-incr-{batch}", batch = b + 1);
        let (r, _) = measure(&db, &phase, &mut seed);
        let p50 = percentiles(r.clone(), 0.50);
        let worst = r.iter().cloned().fold(f64::INFINITY, f64::min);
        // 断言只在显式给阈值时执行（测量模式默认打印）。
        // 断言模式: MOTE_VEC_RECALL_ASSERT=1。默认阈值按 B3 实测基线
        // （8-簇对抗数据: p50≥0.85 p99≥0.99 worst≥0.5; 均匀/真实嵌入数据
        // 实测 p50=1.0）。MOTE_VEC_RECALL_MIN 可收紧。
        if std::env::var("MOTE_VEC_RECALL_ASSERT").is_ok() {
            let p99 = percentiles(r.clone(), 0.99);
            assert!(
                p50 >= threshold && p99 >= 0.99 && worst >= 0.5,
                "{phase}: p50={p50:.4} p99={p99:.4} worst={worst:.4} 阈值 {threshold} — \
                 向量召回退化（stranded/图质量）"
            );
        }
    }
    db.close().unwrap();
}
