//! A1 §A1.5 差分 harness — 同一谓词在九条执行路径上必须同答案。
//!
//! 覆盖路径: ① SELECT WHERE 快路径 ② DELETE ③ UPDATE ④ INNER JOIN
//! （t1 起）⑤ INNER JOIN（表序翻转）⑥ 逗号 join ⑦ 子查询 IN ⑧ 聚合
//! WHERE（COUNT）⑨ GROUP BY + HAVING。DML 路径在副本库上执行，比对
//! 受影响行集与 ① 的结果集。
//!
//! 种子可复现: `MOTEDB_DIFF_SEED=<n>`（默认 20261104）。失败时打印
//! 种子 + 谓词 + 各路结果，重跑即复现。
//!
//! Run: cargo test --release --test test_cmp_differential

use motedb::sql::QueryResult;
use motedb::types::Value;
use motedb::Database;

// ── 确定性 RNG（xorshift64*，与 stability harness 同款风格） ────────────

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

// ── 基础数据 ─────────────────────────────────────────────────────────────

const N_ROWS: i64 = 180;

/// 建库并灌入基础数据（NULL 有比例地散布; emb 用 [i, 2i, 3i, 1] 模式，
/// 部分行显式 NULL）。
fn build_base(dir: &std::path::Path, rng: &mut Rng) -> Database {
    let db = Database::create(dir).unwrap();
    db.execute(
        "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, c TEXT, flag BOOLEAN, \
         emb VECTOR(4))",
    )
    .unwrap();
    db.execute("CREATE TABLE t2 (id INT PRIMARY KEY, t1_id INT, v FLOAT)")
        .unwrap();
    for i in 0..N_ROWS {
        let a = if rng.below(6) == 0 {
            "NULL"
        } else {
            &(i * 7 % 23).to_string()
        };
        let c = if rng.below(6) == 0 {
            "NULL".to_string()
        } else {
            format!("'w{}'", i % 9)
        };
        let flag: String = match rng.below(8) {
            0 => "NULL".into(),
            1 => "TRUE".into(),
            _ => "FALSE".into(),
        };
        let emb = if rng.below(7) == 0 {
            "NULL".to_string()
        } else {
            format!("'[{},{},{},1]'", i, i * 2, i * 3)
        };
        db.execute(&format!(
            "INSERT INTO t1 VALUES ({i}, {a}, {c}, {flag}, {emb})"
        ))
        .unwrap();
    }
    for j in 0..N_ROWS / 2 {
        // t1_id 覆盖部分 t1（奇偶混合），有 NULL。
        let t1_id = if j % 9 == 0 {
            "NULL".into()
        } else {
            (j * 2 % N_ROWS).to_string()
        };
        let v = if j % 5 == 0 {
            "NULL".into()
        } else {
            format!("{:.2}", j as f64 * 1.5)
        };
        db.execute(&format!("INSERT INTO t2 VALUES ({j}, {t1_id}, {v})"))
            .unwrap();
    }
    db.checkpoint().unwrap();
    db
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        other => panic!("expected select: {other:?}"),
    }
}

fn id_set(db: &Database, sql: &str) -> Vec<i64> {
    let mut ids: Vec<i64> = rows(db, sql)
        .into_iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            o => panic!("non-int id: {o:?}"),
        })
        .collect();
    ids.sort_unstable();
    ids
}

/// 在新目录重放基础数据（DML 路径的副本库）。复用同一 RNG 序列会得到
/// 同样的 NULL 布局——由调用方传入 fresh seed 重放相同序列。
fn build_copy(seed: u64) -> (Database, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let mut rng = Rng(seed);
    let db = build_base(dir.path(), &mut rng);
    (db, dir)
}

// ── 谓词生成（类型感知 + 边界字面量） ────────────────────────────────────

#[derive(Clone, Copy, PartialEq)]
enum Col {
    A,    // INT
    C,    // TEXT
    Flag, // BOOLEAN
    Emb,  // VECTOR(4)
}

impl Col {
    fn name(self) -> &'static str {
        match self {
            Col::A => "a",
            Col::C => "c",
            Col::Flag => "flag",
            Col::Emb => "emb",
        }
    }
    fn literals(self, rng: &mut Rng) -> Vec<String> {
        match self {
            Col::A => vec![
                "7".into(),
                "0".into(),
                "22".into(),
                "-1".into(),
                "NULL".into(),
                // 跨型拼写: float / 引号文本
                "7.0".into(),
                "'7'".into(),
            ],
            Col::C => vec![
                "'w3'".into(),
                "'w0'".into(),
                "'zzz'".into(),
                "''".into(),
                "NULL".into(),
                // 跨型拼写
                "3".into(),
            ],
            Col::Flag => vec![
                "TRUE".into(),
                "FALSE".into(),
                "1".into(),
                "0".into(),
                "2".into(),
                "NULL".into(),
                "1.0".into(),
            ],
            Col::Emb => vec![
                // 命中某行（id 可变） / 未命中 / 非向量文本 / NULL
                format!(
                    "'[{},{},{},1]'",
                    rng.below(180),
                    rng.below(360),
                    rng.below(540)
                ),
                "'[9, 9, 9, 9]'".into(),
                "'not-a-vec'".into(),
                "NULL".into(),
                "'[1, 2, 3]'".into(), // 维度不符
            ],
        }
    }
}

/// 原子谓词: col op literal。
fn atom(rng: &mut Rng) -> String {
    let col = [(Col::A, 4u64), (Col::C, 3), (Col::Flag, 2), (Col::Emb, 3)];
    let total: u64 = col.iter().map(|(_, w)| w).sum();
    let mut pick = rng.below(total);
    let (c, _) = col
        .iter()
        .find(|(_, w)| {
            if pick < *w {
                true
            } else {
                pick -= w;
                false
            }
        })
        .expect("pick < total");
    let ops = ["=", "<>", "<", "<=", ">", ">="];
    let op = ops[rng.below(ops.len() as u64) as usize];
    let lits = c.literals(rng);
    let lit = lits[rng.below(lits.len() as u64) as usize].clone();
    // IN 集合形态（含 NULL 成员的经典陷阱）
    if rng.below(5) == 0 {
        let l2 = lits[rng.below(lits.len() as u64) as usize].clone();
        let neg = if rng.below(2) == 0 { " NOT" } else { "" };
        return format!("{col} {neg} IN ({lit}, {l2})", col = c.name());
    }
    // IS NULL 形态
    if rng.below(7) == 0 {
        return format!("{} IS NULL", c.name());
    }
    format!("{} {} {}", c.name(), op, lit)
}

/// 组合谓词: 原子 × AND/OR/NOT，深度 ≤ 2。
fn predicate(rng: &mut Rng, depth: u32) -> String {
    if depth == 0 || rng.below(3) == 0 {
        return atom(rng);
    }
    match rng.below(5) {
        0 | 1 => format!(
            "({}) AND ({})",
            predicate(rng, depth - 1),
            predicate(rng, depth - 1)
        ),
        2 | 3 => format!(
            "({}) OR ({})",
            predicate(rng, depth - 1),
            predicate(rng, depth - 1)
        ),
        _ => format!("NOT ({})", predicate(rng, depth - 1)),
    }
}

// ── 九路对拍 ─────────────────────────────────────────────────────────────

fn scalar(db: &Database, sql: &str) -> i64 {
    match &rows(db, sql)[0][0] {
        Value::Integer(i) => *i,
        o => panic!("non-int scalar: {o:?}"),
    }
}

#[test]
fn nine_paths_agree_on_random_predicates() {
    let seed: u64 = std::env::var("MOTEDB_DIFF_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2026_1104);
    let mut rng = Rng(seed ^ 0x9E37_79B9_7F4A_7C15);

    let (db, _dir) = build_copy(seed);
    // CI 友好: 默认 80 轮（~3min）; `MOTEDB_DIFF_ROUNDS=200` 跑深验
    //（本地三种子 200 轮全绿, 2026-11-04）。
    const ROUNDS_ENV: &str = "MOTEDB_DIFF_ROUNDS";
    let rounds: usize = std::env::var(ROUNDS_ENV)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(80);

    for round in 0..rounds {
        let p = predicate(&mut rng, 2);
        // 路径 ①: SELECT WHERE（基准）
        let base = id_set(&db, &format!("SELECT id FROM t1 WHERE {p}"));
        let base_set: std::collections::BTreeSet<i64> = base.iter().copied().collect();

        // 路径 ⑧: 聚合 WHERE（COUNT 必须等于 |base|）
        let cnt = scalar(&db, &format!("SELECT COUNT(*) FROM t1 WHERE {p}"));
        assert_eq!(
            cnt as usize,
            base.len(),
            "seed={seed} round={round} 谓词 {p:?}: COUNT 路径 {} vs SELECT 路径 {}",
            cnt,
            base.len()
        );

        // 路径 ⑨: GROUP BY + HAVING 的分组总数一致
        let grouped = rows(
            &db,
            &format!(
                "SELECT flag, COUNT(*) FROM t1 WHERE {p} GROUP BY flag \
                 HAVING COUNT(*) >= 0"
            ),
        );
        let grouped_sum: i64 = grouped
            .iter()
            .map(|r| match &r[1] {
                Value::Integer(i) => *i,
                o => panic!("non-int count: {o:?}"),
            })
            .sum();
        assert_eq!(
            grouped_sum as usize,
            base.len(),
            "seed={seed} round={round} 谓词 {p:?}: GROUP BY 总数 {grouped_sum} vs {}",
            base.len()
        );

        // 路径 ④⑤⑥: JOIN 三形态（t1 侧谓词）——结果应等于
        // base ∩ {t1.id : 存在 t2.t1_id = t1.id}
        let joinable: std::collections::BTreeSet<i64> = id_set(
            &db,
            "SELECT DISTINCT t1.id FROM t1 JOIN t2 ON t2.t1_id = t1.id",
        )
        .into_iter()
        .collect();
        let expected_join: Vec<i64> = base_set.intersection(&joinable).copied().collect();
        for (name, sql) in [
            (
                "INNER JOIN t1-first",
                format!("SELECT t1.id FROM t1 JOIN t2 ON t2.t1_id = t1.id WHERE {p}"),
            ),
            (
                "INNER JOIN flipped",
                format!("SELECT t1.id FROM t2 JOIN t1 ON t1.id = t2.t1_id WHERE {p}"),
            ),
            (
                "comma join",
                format!("SELECT t1.id FROM t1, t2 WHERE t2.t1_id = t1.id AND ({p})"),
            ),
        ] {
            let got = id_set(&db, &sql);
            assert_eq!(
                got, expected_join,
                "seed={seed} round={round} 谓词 {p:?}: {name} 分歧（期望 {:?} 得到 {:?}）",
                expected_join, got
            );
        }

        // 路径 ⑦: 子查询 IN 复合
        let subq = id_set(
            &db,
            &format!(
                "SELECT id FROM t1 WHERE ({p}) AND id IN \
                 (SELECT t1_id FROM t2 WHERE t1_id IS NOT NULL)"
            ),
        );
        assert_eq!(
            subq, expected_join,
            "seed={seed} round={round} 谓词 {p:?}: 子查询 IN 分歧"
        );

        // DML 路径 ②③: 副本库上执行，受影响行集必须等于 base。
        // 探针用整行元组对比（UPDATE 的 a+1000 在 a=NULL 时得 NULL，
        // 不能用值域探针）。
        let snapshot = |cdb: &Database| -> std::collections::BTreeMap<i64, String> {
            rows(cdb, "SELECT id, a, c, flag, emb FROM t1 ORDER BY id")
                .into_iter()
                .map(|r| match &r[0] {
                    Value::Integer(i) => (*i, format!("{:?}", &r[1..])),
                    o => panic!("non-int id: {o:?}"),
                })
                .collect()
        };
        for (name, stmt) in [
            ("DELETE", format!("DELETE FROM t1 WHERE {p}")),
            // 哨兵常量（非数据域值）: 匹配行的元组必然可见变化——
            // `a = a + 1000` 在 a=NULL 时得 NULL，行匹配但快照无差异。
            (
                "UPDATE",
                format!("UPDATE t1 SET c = '__diff_mark__' WHERE {p}"),
            ),
        ] {
            let (cdb, _cdir) = build_copy(seed);
            let before = snapshot(&cdb);
            let n = match cdb.execute(&stmt).unwrap().materialize().unwrap() {
                QueryResult::Modification { affected_rows } => affected_rows,
                o => panic!("{o:?}"),
            };
            assert_eq!(
                n as usize,
                base.len(),
                "seed={seed} round={round} 谓词 {p:?}: {name} affected {n} vs SELECT {}",
                base.len()
            );
            let after = snapshot(&cdb);
            let touched: std::collections::BTreeSet<i64> = if name == "DELETE" {
                before
                    .keys()
                    .filter(|k| !after.contains_key(k))
                    .copied()
                    .collect()
            } else {
                before
                    .iter()
                    .filter(|(k, v)| after.get(k).is_some_and(|nv| nv != *v))
                    .map(|(k, _)| *k)
                    .collect()
            };
            assert_eq!(
                touched, base_set,
                "seed={seed} round={round} 谓词 {p:?}: {name} 行集分歧"
            );
            cdb.close().unwrap();
        }
    }
    db.close().unwrap();
    eprintln!("nine_paths: {rounds} 轮 × 九路径一致 (seed={seed})");
}
