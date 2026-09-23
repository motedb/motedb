//! 无索引 knn 精确扫描段内 morsel 并行化的差分测试:
//! 期望值由测试内暴力计算; 覆盖 UPDATE (newest-wins 去重) / DELETE
//! (墓碑声明, 无幽灵行) / cosine / 空 NULL 向量。
use motedb::types::Value;
use motedb::{Database, DBConfig};
use tempfile::TempDir;

const N: usize = 60_000; // > PARALLEL_MORSEL_MIN_ROWS(20K): 并行分支必走
const DIM: usize = 8;

fn gen_vec(i: usize, salt: usize) -> Vec<f32> {
    // 确定性伪随机向量 (splitmix64 — 24-bit 精度均匀分量)。
    // 🔑 早期的 fract((i*K + d*C)*φ) 线性型生成器在 8 维下产生大量
    // 余弦距离恰为 0.0 的近平行向量 (f32 平局任意序), 差分无法判定。
    (0..DIM)
        .map(|d| {
            let mut z = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
                ^ (d as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9)
                ^ (salt as u64).wrapping_mul(0x94D0_49BB_1331_11EB);
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            (((z >> 40) as f64) / ((1u64 << 24) as f64)) as f32 * 2.0 - 1.0
        })
        .collect()
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        motedb::sql::QueryResult::Select { rows, .. } => rows,
        _ => panic!("expected select"),
    }
}

fn knn_ids(db: &Database, metric: &str, q: &[f32], k: usize) -> Vec<i64> {
    let sql = format!(
        "SELECT id FROM t ORDER BY emb {} ? LIMIT {}",
        if metric == "l2" { "<->" } else { "<=>" },
        k
    );
    let params = vec![Value::Vector(motedb::types::ArcVec(
        q.iter().copied().collect(),
    ))];
    let r = db
        .execute_prepared(&sql, params)
        .unwrap()
        .materialize()
        .unwrap();
    match r {
        motedb::sql::QueryResult::Select { rows, .. } => rows
            .into_iter()
            .map(|row| match &row[0] {
                Value::Integer(i) => *i,
                other => panic!("not integer: {:?}", other),
            })
            .collect(),
        _ => panic!("expected select"),
    }
}

fn expected_knn(vectors: &[(i64, Vec<f32>)], q: &[f32], metric: &str, k: usize) -> Vec<i64> {
    // 🔑 距离用引擎自己的核 (SIMD 求和序 vs 标量 fp 差异不进差分 — 本测试
    // 校验的是扫描/堆/去重逻辑, 距离核另有测试)。
    let mut scored: Vec<(f32, i64)> = vectors
        .iter()
        .map(|(id, v)| {
            let d = if metric == "l2" {
                motedb::distance::euclidean::euclidean_distance_squared(q, v)
            } else {
                motedb::distance::cosine::cosine_distance(q, v)
            };
            (d, *id)
        })
        .collect();
    scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap().then(a.1.cmp(&b.1)));
    scored.into_iter().take(k).map(|(_, id)| id).collect()
}

#[test]
fn knn_parallel_matches_bruteforce() {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_testing();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute(&format!(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, emb VECTOR({}))"
    , DIM))
    .unwrap();
    // 两个 30K 批 (两段, 各 ≥20K → 并行分支)
    for batch_start in [0usize, 30_000] {
        let rows: Vec<Vec<Value>> = (0..30_000)
            .map(|i| {
                let id = (batch_start + i) as i64;
                vec![
                    Value::Integer(id),
                    Value::Vector(motedb::types::ArcVec(
                        gen_vec(id as usize, 0).into_iter().collect(),
                    )),
                ]
            })
            .collect();
        db.insert_rows("t", rows).unwrap();
    }
    let live: Vec<(i64, Vec<f32>)> = (1..=(N as i64))
        .map(|id| (id, gen_vec(id as usize, 0)))
        .collect();

    for metric in ["l2", "cosine"] {
        for qi in [0usize, 12345, 59999] {
            let q = gen_vec(qi, 7);
            let got = knn_ids(&db, metric, &q, 10);
            let want = expected_knn(&live, &q, metric, 10);
            assert_eq!(got, want, "metric={} qi={} full-table", metric, qi);
        }
    }

    // UPDATE 500 行换新向量 (跨段 newest-wins: 新版本在更新段, 旧版本在原段)
    for id in 1..=500i64 {
        let v = gen_vec(id as usize, 99);
        db.execute_prepared(
            "UPDATE t SET emb = ? WHERE id = ?",
            vec![
                Value::Vector(motedb::types::ArcVec(v.into_iter().collect())),
                Value::Integer(id),
            ],
        )
        .unwrap();
    }
    let live2: Vec<(i64, Vec<f32>)> = (1..=(N as i64))
        .map(|id| (id, gen_vec(id as usize, if id <= 500 { 99 } else { 0 })))
        .collect();
    for qi in [0usize, 777] {
        let q = gen_vec(qi, 7);
        let got = knn_ids(&db, "l2", &q, 10);
        let want = expected_knn(&live2, &q, "l2", 10);
        assert_eq!(got, want, "after UPDATE qi={}", qi);
    }

    // DELETE 700 行: 结果必须排除且不缺行 (幽灵行/丢行双向检查)
    db.execute("DELETE FROM t WHERE id > 59300").unwrap();
    let live3: Vec<(i64, Vec<f32>)> = live2
        .into_iter()
        .filter(|(id, _)| *id <= 59300)
        .collect();
    for qi in [0usize, 42] {
        let q = gen_vec(qi, 7);
        let got = knn_ids(&db, "l2", &q, 10);
        let want = expected_knn(&live3, &q, "l2", 10);
        assert_eq!(got.len(), 10, "result count after DELETE qi={}", qi);
        assert_eq!(got, want, "after DELETE qi={}", qi);
    }

    // NULL 向量行不参与
    db.execute("INSERT INTO t (id, emb) VALUES (999999, NULL)").unwrap();
    let q = gen_vec(5, 7);
    let got = knn_ids(&db, "l2", &q, 10);
    assert_eq!(got.len(), 10);
    assert!(!got.contains(&999999), "NULL vector must not appear");
}
