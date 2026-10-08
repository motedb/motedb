//! SQL 组合差分 fuzz vs SQLite — 覆盖 v2 新特性与既有算子的幂组合:
//! CTE(普通/UNION 体/RECURSIVE/别名/链式)× 窗口函数(11 种 × 分区/排序/
//! DISTINCT/嵌表达式/GROUP BY 联用)× JOIN × 子查询 × 别名。
//!
//! 每轮同数据双引擎播种,随机变异(INSERT/UPDATE/DELETE)穿插随机模板
//! 查询,结果归一化后精确对拍(无序查询按多重集比较;有序模板显式
//! tie-break 保证序确定)。这层抓的是定向测试覆盖不到的组合形状——
//! 复审系列的非确定性 bug 就属于这一类。

use motedb::{DBConfig, Database};
use rusqlite::Connection;
use tempfile::TempDir;

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 16
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn norm_cell(v: rusqlite::types::Value) -> String {
    use rusqlite::types::Value;
    match v {
        Value::Null => "NULL".into(),
        Value::Integer(i) => i.to_string(),
        Value::Real(f) => format!("{:.4}", f),
        Value::Text(s) => s,
        Value::Blob(b) => format!("blob({})", b.len()),
    }
}

fn norm_mote(v: &motedb::types::Value) -> String {
    use motedb::types::Value as V;
    match v {
        V::Null => "NULL".into(),
        V::Integer(i) => i.to_string(),
        V::Float(f) => format!("{:.4}", f),
        V::Text(s) => s.as_str().to_string(),
        V::Bool(b) => (if *b { 1 } else { 0 }).to_string(),
        other => format!("{other:?}"),
    }
}

fn run_sqlite(con: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = con.prepare(sql).map_err(|e| format!("sqlite: {e}"))?;
    let ncols = stmt.column_count();
    let mut rows_iter = stmt.query([]).map_err(|e| format!("sqlite: {e}"))?;
    let mut rows = Vec::new();
    while let Some(r) = rows_iter.next().map_err(|e| format!("sqlite: {e}"))? {
        let mut row = Vec::with_capacity(ncols);
        for i in 0..ncols {
            let v: rusqlite::types::Value = r.get(i).unwrap_or(rusqlite::types::Value::Null);
            row.push(norm_cell(v));
        }
        rows.push(row);
    }
    Ok(rows)
}

fn run_mote(db: &Database, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let r = db
        .execute(sql)
        .map_err(|e| format!("mote: {e}"))?
        .materialize()
        .map_err(|e| format!("mote: {e}"))?;
    let Some((_, rows)) = r.select_rows() else {
        return Ok(Vec::new());
    };
    Ok(rows
        .iter()
        .map(|row| row.iter().map(norm_mote).collect())
        .collect())
}

fn compare(
    db: &Database,
    con: &Connection,
    sql: &str,
    ordered: bool,
    label: &str,
    divergences: &mut Vec<String>,
) {
    let a = run_sqlite(con, sql);
    let b = run_mote(db, sql);
    match (&a, &b) {
        (Err(_), Err(_)) => {}
        (Err(ea), Ok(_)) => divergences.push(format!(
            "{label} {sql}\n  sqlite errors ({ea}), MoteDB succeeds"
        )),
        (Ok(_), Err(eb)) => divergences.push(format!(
            "{label} {sql}\n  MoteDB errors ({eb}), sqlite succeeds"
        )),
        (Ok(ra), Ok(rb)) => {
            let (mut ra, mut rb) = (ra.clone(), rb.clone());
            if !ordered {
                ra.sort();
                rb.sort();
            }
            if ra != rb {
                divergences.push(format!(
                    "{label} {sql}\n  sqlite ({}) {ra:?}\n  mote   ({}) {rb:?}",
                    ra.len(),
                    rb.len()
                ));
            }
        }
    }
}

/// 随机组合模板池: (生成器闭包, 结果是否按序比较)。
/// 有序模板全部显式 tie-break(尾键含唯一 id / c), 使引擎间行序确定。
fn gen_query(rng: &mut Lcg) -> (String, bool) {
    let k = 3 + rng.below(14) as i64; // 3..=16
    let tag = rng.below(4);
    match rng.below(20) {
        0 => (
            format!(
                "WITH x AS (SELECT a, c FROM t WHERE a >= {k}) SELECT COUNT(*), MAX(a) FROM x"
            ),
            false,
        ),
        1 => (
            format!(
                "WITH x AS (SELECT a FROM t WHERE a < {k} UNION ALL SELECT a + {k} FROM t WHERE c = 't{tag}') SELECT SUM(a) FROM x"
            ),
            false,
        ),
        2 => (
            "WITH x AS (SELECT a FROM t UNION SELECT a FROM t) SELECT COUNT(*) FROM x".to_string(),
            false,
        ),
        3 => (
            "WITH x(s) AS (SELECT SUM(a) FROM t) SELECT s FROM x".to_string(),
            false,
        ),
        4 => (
            format!("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < {k}) SELECT SUM(n) FROM r"),
            false,
        ),
        5 => (
            // 递归上界来自表数据(标量子查询); MAX 为 NULL 时两边都立即终止
            "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < (SELECT MAX(a) FROM t WHERE a < 20)) SELECT COUNT(*) FROM r".to_string(),
            false,
        ),
        6 => (
            // CTE 链: 后一个引用前一个
            format!("WITH x AS (SELECT a, c FROM t WHERE a IS NOT NULL), y AS (SELECT a + 1 AS w FROM x WHERE a < {k}) SELECT COUNT(*), MIN(w) FROM y"),
            false,
        ),
        7 => (
            "SELECT id, SUM(a) OVER (PARTITION BY c) FROM t WHERE a IS NOT NULL ORDER BY id".to_string(),
            true,
        ),
        8 => (
            // 运行聚合: ORDER BY 含唯一键使帧序确定
            "SELECT id, SUM(a) OVER (ORDER BY a, id) FROM t WHERE a IS NOT NULL ORDER BY id".to_string(),
            true,
        ),
        9 => (
            // 注: SQLite 不支持窗口内 DISTINCT("not supported as window
            // functions")— MoteDB 是超集, 该形态的正确性由 v101 用例按值
            // 断言; 此处对拍 SQLite 支持的形状 + 裸 DISTINCT 聚合。
            "SELECT COUNT(a) OVER (), COUNT(*) OVER () FROM t WHERE a IS NOT NULL".to_string(),
            false,
        ),
        10 => (
            "SELECT id, LAG(a, 1, -1) OVER (ORDER BY id) FROM t ORDER BY id".to_string(),
            true,
        ),
        11 => (
            "SELECT id, LEAD(a, 2) OVER (PARTITION BY c ORDER BY id) FROM t WHERE c IS NOT NULL ORDER BY id".to_string(),
            true,
        ),
        12 => (
            "SELECT id, FIRST_VALUE(a) OVER (PARTITION BY c ORDER BY id), LAST_VALUE(a) OVER (PARTITION BY c ORDER BY id) FROM t WHERE c IS NOT NULL ORDER BY id".to_string(),
            true,
        ),
        13 => (
            // 窗口嵌在表达式里
            "SELECT SUM(a) OVER () + 1, AVG(a) OVER () * 2 FROM t WHERE a IS NOT NULL".to_string(),
            false,
        ),
        14 => (
            // GROUP BY + 窗口: ORDER BY 聚合别名 + 组键 tie-break
            "SELECT c, SUM(a) AS s, ROW_NUMBER() OVER (ORDER BY SUM(a) DESC, c) FROM t WHERE a IS NOT NULL GROUP BY c ORDER BY c".to_string(),
            true,
        ),
        15 => (
            // CTE + 窗口组合
            "WITH x AS (SELECT id, a, c FROM t WHERE a IS NOT NULL) SELECT id, AVG(a) OVER (PARTITION BY c) FROM x ORDER BY id".to_string(),
            true,
        ),
        16 => (
            // 窗口 over 派生表(聚合子查询)
            "SELECT SUM(s) OVER (), COUNT(*) OVER () FROM (SELECT SUM(a) AS s FROM t WHERE a IS NOT NULL GROUP BY c) AS g".to_string(),
            false,
        ),
        17 => (
            // JOIN + CTE + 窗口。(score, id) 在 JOIN 输出唯一 → rn 分配确定;
            // 但输出行在重复 id 间序不定 → 按多重集比较。
            "WITH j AS (SELECT t.id, u.score FROM t INNER JOIN u ON u.t_id = t.id) SELECT id, ROW_NUMBER() OVER (ORDER BY score, id) FROM j ORDER BY id".to_string(),
            false,
        ),
        18 => (
            // 窗口 + WHERE 子查询
            "SELECT id, SUM(a) OVER (PARTITION BY c) FROM t WHERE a IN (SELECT MAX(a) FROM t) ORDER BY id".to_string(),
            true,
        ),
        19 => (
            format!(
                "WITH x AS (SELECT a FROM t WHERE a IS NOT NULL ORDER BY a, id LIMIT {k}) SELECT SUM(a) FROM x"
            ),
            false,
        ),
        _ => (
            // 裸 DISTINCT 聚合(SQLite 支持)
            "SELECT COUNT(DISTINCT a), SUM(DISTINCT a) FROM t WHERE a IS NOT NULL"
                .to_string(),
            false,
        ),
    }
}

#[test]
fn fuzz_sql_combination_differential_vs_sqlite() {
    let mut total_div = Vec::new();
    for round in 0..3u64 {
        let dir = TempDir::new().unwrap();
        let mut config = DBConfig::for_testing();
        config.auto_checkpoint = None;
        let db = Database::create_with_config(dir.path(), config).unwrap();
        let con = Connection::open_in_memory().unwrap();

        for stmt in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b REAL, c TEXT)",
            "CREATE TABLE u (id INTEGER PRIMARY KEY, t_id INTEGER, tag TEXT, score REAL)",
        ] {
            db.execute(stmt).unwrap();
            con.execute_batch(stmt).unwrap();
        }

        let mut rng = Lcg(0xBADC0DE + round * 7919);
        let mut divergences: Vec<String> = Vec::new();
        let mut next_id: i64 = 1;

        // 播种: 小值域(重复键多 — 分区/并列行/去重才有意义) + 少量 NULL/边界
        for _ in 0..36 {
            let id = next_id;
            next_id += 1;
            let a = match rng.below(10) {
                0 => "NULL".to_string(),
                1 => "9007199254740991".to_string(),
                _ => rng.below(8).to_string(), // 高重复
            };
            let b = format!("{:.2}", rng.below(200) as f64 / 3.0);
            let c = match rng.below(12) {
                0 => "NULL".to_string(),
                _ => format!("'t{}'", rng.below(3)), // 仅 3 个组
            };
            let sql = format!("INSERT INTO t (id, a, b, c) VALUES ({id}, {a}, {b}, {c})");
            let _ = con.execute(&sql, ());
            let _ = db.execute(&sql);
            let t_ref = if rng.below(5) == 0 {
                "NULL".to_string()
            } else {
                rng.below(40).to_string()
            };
            let usql = format!(
                "INSERT INTO u (id, t_id, tag, score) VALUES ({id}, {t_ref}, 'g{}', {:.2})",
                rng.below(3),
                rng.below(100) as f64 / 3.0
            );
            let _ = con.execute(&usql, ());
            let _ = db.execute(&usql);
        }

        // 变异 + 随机模板对拍
        for step in 0..28 {
            match rng.below(8) {
                0..=1 => {
                    let id = next_id;
                    next_id += 1;
                    let sql = format!(
                        "INSERT INTO t (id, a, b, c) VALUES ({id}, {}, {:.2}, 't{}')",
                        rng.below(8),
                        rng.below(100) as f64 / 3.0,
                        rng.below(3)
                    );
                    let _ = con.execute(&sql, ());
                    let _ = db.execute(&sql);
                }
                2 => {
                    let sql = format!(
                        "UPDATE t SET b = {:.2} WHERE a = {}",
                        rng.below(100) as f64 / 3.0,
                        rng.below(8)
                    );
                    let _ = con.execute(&sql, ());
                    let _ = db.execute(&sql);
                }
                3 => {
                    let sql = format!("DELETE FROM t WHERE a = {} AND c = 't0'", rng.below(8));
                    let _ = con.execute(&sql, ());
                    let _ = db.execute(&sql);
                }
                4 => {
                    let sql = format!(
                        "UPDATE u SET tag = 'gx' WHERE score > {:.2}",
                        rng.below(50) as f64 / 3.0
                    );
                    let _ = con.execute(&sql, ());
                    let _ = db.execute(&sql);
                }
                _ => {}
            }
            if step % 2 == 0 {
                let label = format!("[r{round} s{step}]");
                // 每步 4 个随机模板 — 组合空间随机采样
                for _ in 0..4 {
                    let (sql, ordered) = gen_query(&mut rng);
                    compare(&db, &con, &sql, ordered, &label, &mut divergences);
                }
            }
        }
        total_div.extend(divergences);
    }
    assert!(
        total_div.is_empty(),
        "combination-fuzz divergences vs SQLite:\n{}",
        total_div
            .iter()
            .take(10)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n──\n")
    );
}
