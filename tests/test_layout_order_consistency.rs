//! 🔴 布局无关的行序: 同一批数据、同一条查询，表是一个段、六个段、key
//! 交错的六个段、重开之后、CHECKPOINT 之后 —— 返回的行必须逐行相同。
//!
//! 无 ORDER BY 的行序不由 SQL 定义，但它在同一个数据库的两种状态之间
//! 翻转是实实在在的 bug: 机器人/长驻进程随时在"一个段"和"多个段"之间
//! 切换（写缓冲 flush、后台合并），重启更是常态。约定: **key 序**（升序
//! 内部 key; 整数主键表即主键序）—— 段落盘时已按它排序、归并游标按它
//! 输出、合并后的表就是它，是唯一不随布局移动的顺序。
//!
//! 历史（本文件若早存在即可拦住）:
//!  * 0.18.3 之前: 多段表上多条过滤扫描走"最新段优先"，
//!    `LIMIT 7` 返回最新的 7 行。
//!  * 0.18.3: 四条路径改成"全量物化再排序" —— `LIMIT` 退化成全扫描
//!    （30 万行 17×）; `scan_projected_filtered` 与 TEXT IN 列表路径没改，
//!    向量化执行器开/关时分页不同。
//!  * 0.18.6 之前: 统一成"段位置序" —— key 区间交错的段被首尾相接
//!    （`id > 3 LIMIT 2` 进程内返回 {5,7}、重开后返回 {4,5}），被 UPDATE
//!    的行跳到末尾、下次合并又跳回来。
//!  * 现在: 统一经 `select_doc_order_positions`（跨段按 key 归并 +
//!    newest-wins + 取满即停）。

use motedb::Database;
use std::path::Path;
use tempfile::TempDir;

const BATCHES: usize = 6;
const PER_BATCH: usize = 1000;

fn row_sql(id: usize) -> String {
    format!(
        "({}, {}, '{}', {}, {})",
        id,
        id % 1000,
        ["us", "eu", "ap"][id % 3],
        id as f64 * 0.5,
        id.is_multiple_of(2)
    )
}

fn insert(db: &Database, ids: impl Iterator<Item = usize>) {
    let mut chunk = String::new();
    for (k, id) in ids.enumerate() {
        if !k.is_multiple_of(1000) {
            chunk.push(',');
        }
        chunk.push_str(&row_sql(id));
        if k % 1000 == 999 {
            db.execute(&format!("INSERT INTO t VALUES {chunk}"))
                .unwrap();
            chunk.clear();
        }
    }
    if !chunk.is_empty() {
        db.execute(&format!("INSERT INTO t VALUES {chunk}"))
            .unwrap();
    }
}

fn flush(db: &Database) {
    // 文本等值查询会 flush 写缓冲 → 段边界
    let _ = db.query("SELECT id FROM t WHERE sval = 'zzz'");
}

/// `single`: 全部行一个段。`multi`: 六段，id 连续分区（key 区间不重叠）。
/// `interleaved`: 六段，id 按批次取模分配（各段 key 区间交错）。
fn build(dir: &Path, layout: &str) -> Database {
    let db = Database::create(dir).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, ival INT, sval TEXT, fval REAL, flag BOOLEAN)")
        .unwrap();
    let total = BATCHES * PER_BATCH;
    match layout {
        "single" => insert(&db, 0..total),
        "multi" => {
            for b in 0..BATCHES {
                insert(&db, b * PER_BATCH..(b + 1) * PER_BATCH);
                flush(&db);
            }
        }
        _ => {
            for b in 0..BATCHES {
                insert(&db, (b..total).step_by(BATCHES));
                flush(&db);
            }
        }
    }
    db
}

fn q(db: &Database, sql: &str) -> Vec<Vec<String>> {
    db.query(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| r.iter().map(|v| format!("{v:?}")).collect())
        .collect()
}

const SHAPES: &[&str] = &[
    "SELECT id FROM t LIMIT 7",
    "SELECT id FROM t LIMIT 7 OFFSET 2003",
    "SELECT * FROM t LIMIT 7",
    "SELECT id FROM t WHERE ival > 900 LIMIT 7",
    "SELECT id FROM t WHERE id % 2 = 0 LIMIT 7",
    "SELECT id FROM t WHERE ival + 1 > 995 LIMIT 7",
    "SELECT id FROM t WHERE fval > 2990.0 LIMIT 7",
    "SELECT id FROM t WHERE sval = 'eu' LIMIT 7",
    "SELECT id FROM t WHERE sval LIKE 'e%' LIMIT 7",
    "SELECT id FROM t WHERE sval LIKE '%s%' LIMIT 7",
    "SELECT id FROM t WHERE sval IN ('eu', 'ap') LIMIT 7",
    "SELECT id FROM t WHERE ival IN (5, 1005, 2005) LIMIT 7",
    "SELECT id FROM t WHERE ival BETWEEN 990 AND 1000 LIMIT 7",
    "SELECT id FROM t WHERE fval IS NOT NULL LIMIT 7",
    "SELECT id FROM t WHERE flag = TRUE LIMIT 7",
    "SELECT id FROM t WHERE sval <> 'us' LIMIT 7",
    "SELECT id FROM t WHERE ival = 3 OR ival = 4 LIMIT 7",
    "SELECT id FROM t WHERE ival > 100 AND flag = TRUE LIMIT 7",
    // 大页（越过选择器的"小页走键查找"阈值）与无 LIMIT
    "SELECT id FROM t WHERE flag = TRUE LIMIT 1500",
    "SELECT id FROM t",
    "SELECT id FROM t WHERE ival > 5990",
    "SELECT id FROM t WHERE sval IN ('eu', 'ap')",
];

fn check_against_single(single: &Database, others: &[(&str, &Database)], label: &str) {
    for sql in SHAPES {
        let want = q(single, sql);
        for (name, db) in others {
            assert_eq!(
                q(db, sql),
                want,
                "[{label}] {name}: rows differ from the 1-segment table\nsql: {sql}"
            );
        }
    }
}

#[test]
fn same_rows_across_segment_layouts_and_restarts() {
    let dir = TempDir::new().unwrap();
    let single = build(&dir.path().join("single"), "single");
    let multi = build(&dir.path().join("multi"), "multi");
    let interleaved = build(&dir.path().join("inter"), "interleaved");

    check_against_single(
        &single,
        &[
            ("6 segments", &multi),
            ("6 interleaved segments", &interleaved),
        ],
        "in-process",
    );

    std::mem::drop(multi);
    std::mem::drop(interleaved);
    let multi = Database::open(dir.path().join("multi")).unwrap();
    let interleaved = Database::open(dir.path().join("inter")).unwrap();
    check_against_single(
        &single,
        &[
            ("6 segments", &multi),
            ("6 interleaved segments", &interleaved),
        ],
        "after reopen",
    );

    let _ = multi.execute("CHECKPOINT");
    let _ = interleaved.execute("CHECKPOINT");
    check_against_single(
        &single,
        &[
            ("6 segments", &multi),
            ("6 interleaved segments", &interleaved),
        ],
        "after CHECKPOINT",
    );
}

/// 文本 IN 列表路径投影 BOOLEAN 列: 布尔列是 1 字节布局，旧的解码臂走
/// get_i64 会越界 panic。
#[test]
fn boolean_projection_through_text_in_list() {
    let dir = TempDir::new().unwrap();
    let db = build(&dir.path().join("b"), "multi");
    let rows = q(
        &db,
        "SELECT id, flag FROM t WHERE sval IN ('us', 'eu') ORDER BY id LIMIT 6",
    );
    assert_eq!(rows.len(), 6);
    // sval IN ('us', 'eu') ⇔ id % 3 ∈ {0, 1}
    for (r, id) in rows.iter().zip([0usize, 1, 3, 4, 6, 7]) {
        assert_eq!(r[0], format!("Integer({id})"));
        assert_eq!(
            r[1],
            if id % 2 == 0 {
                "Bool(true)"
            } else {
                "Bool(false)"
            }
        );
    }
}
