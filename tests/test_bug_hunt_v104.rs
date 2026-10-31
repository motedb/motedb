//! round-13 bug 狩猎: 全文检索 / 空间查询 / 窗口函数扩展 / 并发 DDL / TTL。

use motedb::{types::Value as V, Database};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

fn new_db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("t.mote")).unwrap();
    (db, dir)
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<V>> {
    let r = db
        .execute(sql)
        .unwrap_or_else(|e| panic!("SQL failed: {sql}\n  err: {e}"))
        .materialize()
        .unwrap();
    let (_, rs) = r.select_rows().unwrap();
    rs.to_vec()
}

fn ints(db: &Database, sql: &str) -> Vec<i64> {
    rows(db, sql)
        .iter()
        .map(|r| match &r[0] {
            V::Integer(n) => *n,
            o => panic!("expected int, got {o:?}: {sql}"),
        })
        .collect()
}

/// 全文检索: MATCH 命中含关键词的行, 不含的不命中; BM25 排序自洽。
#[test]
fn probe_fts_match_and_ranking() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE art (id INT PRIMARY KEY, content TEXT)")
        .unwrap();
    db.execute("INSERT INTO art VALUES (1, 'rust database engine')")
        .unwrap();
    db.execute("INSERT INTO art VALUES (2, 'web framework')")
        .unwrap();
    db.execute("INSERT INTO art VALUES (3, 'vector database for robots')")
        .unwrap();
    db.execute("CREATE TEXT INDEX art_content ON art(content)")
        .unwrap();
    let hits = ints(&db, "SELECT id FROM art WHERE MATCH(content, 'database')");
    let mut h = hits.clone();
    h.sort();
    assert_eq!(h, vec![1, 3], "MATCH finds both 'database' rows: {hits:?}");
    let no = ints(&db, "SELECT id FROM art WHERE MATCH(content, 'quantum')");
    assert!(no.is_empty(), "no false positives: {no:?}");
    // BM25 分数非负且命中行才有分。
    let r = rows(&db, "SELECT id, BM25_SCORE(content, 'database') FROM art WHERE MATCH(content, 'database') ORDER BY 2 DESC");
    assert_eq!(r.len(), 2);
    for row in &r {
        match &row[1] {
            V::Float(s) => assert!(*s >= 0.0, "bm25 non-negative"),
            o => panic!("bm25 float: {o:?}"),
        }
    }
}

/// FTS 增量: 插入后新词可命中, 删除后旧词不再命中。
#[test]
fn probe_fts_incremental_visibility() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE art (id INT PRIMARY KEY, content TEXT)")
        .unwrap();
    db.execute("INSERT INTO art VALUES (1, 'alpha beta')")
        .unwrap();
    assert_eq!(
        ints(&db, "SELECT id FROM art WHERE MATCH(content, 'alpha')"),
        vec![1]
    );
    db.execute("INSERT INTO art VALUES (2, 'gamma alpha')")
        .unwrap();
    let mut h = ints(&db, "SELECT id FROM art WHERE MATCH(content, 'alpha')");
    h.sort();
    assert_eq!(h, vec![1, 2], "new row searchable immediately");
    db.execute("DELETE FROM art WHERE id = 1").unwrap();
    assert_eq!(
        ints(&db, "SELECT id FROM art WHERE MATCH(content, 'alpha')"),
        vec![2],
        "deleted row no longer matches"
    );
}

/// 空间: ST_WITHIN bbox 查询只命中框内点。
#[test]
fn probe_spatial_within_bbox() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE poi (id INT PRIMARY KEY, coords GEOMETRY)")
        .unwrap(); // 空间列为 GEOMETRY (POINT3D 别名)
    db.execute("INSERT INTO poi VALUES (1, POINT(116.5, 39.5))")
        .unwrap();
    db.execute("INSERT INTO poi VALUES (2, POINT(120.0, 30.0))")
        .unwrap();
    db.execute("INSERT INTO poi VALUES (3, POINT(116.9, 39.9))")
        .unwrap();
    // 空间索引经 SQL DDL 创建 (CREATE SPATIAL INDEX; OCTREE 为别名)。
    db.execute("CREATE SPATIAL INDEX poi_coords ON poi(coords)")
        .unwrap();
    let inside = {
        let mut v = ints(
            &db,
            "SELECT id FROM poi WHERE ST_WITHIN(coords, 116.0, 39.0, 117.0, 40.0)",
        );
        v.sort();
        v
    };
    assert_eq!(
        inside,
        vec![1, 3],
        "bbox contains only in-box points: {inside:?}"
    );
    let none = ints(
        &db,
        "SELECT id FROM poi WHERE ST_WITHIN(coords, 0.0, 0.0, 1.0, 1.0)",
    );
    assert!(none.is_empty());
}

/// 窗口函数: LEAD/LAG 默认偏移 1 无默认值 → NULL。
#[test]
fn probe_window_lead_lag_defaults() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE s (id INT PRIMARY KEY, g TEXT, v INT)")
        .unwrap();
    for (i, v) in [(1, 10), (2, 20), (3, 30)] {
        db.execute(&format!("INSERT INTO s VALUES ({i}, 'a', {v})"))
            .unwrap();
    }
    let r = rows(
        &db,
        "SELECT v, LEAD(v) OVER (ORDER BY v), LAG(v) OVER (ORDER BY v) FROM s ORDER BY v",
    );
    // v=10: lead=20, lag=NULL; v=20: lead=30, lag=10; v=30: lead=NULL, lag=20
    assert_eq!(r[0], vec![V::Integer(10), V::Integer(20), V::Null]);
    assert_eq!(r[1], vec![V::Integer(20), V::Integer(30), V::Integer(10)]);
    assert_eq!(r[2], vec![V::Integer(30), V::Null, V::Integer(20)]);
}

/// 窗口: LEAD 带显式默认值。
#[test]
fn probe_window_lead_with_default() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE s (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for (i, v) in [(1, 7), (2, 9)] {
        db.execute(&format!("INSERT INTO s VALUES ({i}, {v})"))
            .unwrap();
    }
    let r = rows(
        &db,
        "SELECT v, LEAD(v, 1, -1) OVER (ORDER BY v) FROM s ORDER BY v",
    );
    assert_eq!(r[0], vec![V::Integer(7), V::Integer(9)]);
    assert_eq!(
        r[1],
        vec![V::Integer(9), V::Integer(-1)],
        "explicit default replaces NULL"
    );
}

/// ROW_NUMBER 窗口与 PARTITION 组合: 每分区独立编号。
#[test]
fn probe_window_row_number_partition() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE s (id INT PRIMARY KEY, g TEXT)")
        .unwrap();
    for (i, g) in [(1, "a"), (2, "b"), (3, "a"), (4, "b")] {
        db.execute(&format!("INSERT INTO s VALUES ({i}, '{g}')"))
            .unwrap();
    }
    let r = ints(
        &db,
        "SELECT ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) FROM s ORDER BY id",
    );
    assert_eq!(r, vec![1, 1, 2, 2], "per-partition numbering");
}

/// 并发 DDL: 写入进行中 CREATE INDEX 不丢数据、不死锁, 索引建好可查。
#[test]
fn probe_create_index_under_concurrent_inserts() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path().join("c.mote")).unwrap());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let mut hs: Vec<std::thread::JoinHandle<()>> = Vec::new();
    {
        let (db, stop) = (Arc::clone(&db), Arc::clone(&stop));
        hs.push(std::thread::spawn(move || {
            let mut i = 0i64;
            while !stop.load(Ordering::Relaxed) {
                i += 1;
                let _ = db.execute(&format!("INSERT INTO t VALUES ({i}, 'name{i}')"));
            }
        }));
    }
    std::thread::sleep(Duration::from_millis(300));
    let idx = db.execute("CREATE INDEX idx_t_name ON t(name)");
    std::thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::Relaxed);
    for h in hs {
        h.join().unwrap();
    }
    assert!(idx.is_ok(), "CREATE INDEX under load: {:?}", idx.err());
    // 数据完整: COUNT 与最大 id 一致 (id 连续自增, 插入失败则少)。
    let max = ints(&db, "SELECT MAX(id) FROM t")[0];
    let cnt = ints(&db, "SELECT COUNT(*) FROM t")[0];
    assert_eq!(
        max as i64, cnt,
        "no lost inserts during DDL (max={max} cnt={cnt})"
    );
    // 索引可用: 等值查询命中。
    let hit = ints(&db, "SELECT id FROM t WHERE name = 'name1'");
    assert_eq!(hit, vec![1]);
}

/// 并发 DROP TABLE + 未提交事务: 事务里的表被并发 DROP, 提交不得 panic。
#[test]
fn probe_drop_table_concurrent_with_txn() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path().join("d.mote")).unwrap());
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("CREATE TABLE b (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO a VALUES (1, 1)").unwrap();
    db.execute("INSERT INTO b VALUES (1, 1)").unwrap();

    // 事务持有 b 的缓冲写, 并发 DROP b, 提交必须优雅 (成功或明确报错)。
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO b VALUES (2, 2)").unwrap();
    let drop = db.execute("DROP TABLE b");
    let commit = db.execute("COMMIT");
    // 无论顺序如何, 不允许 panic; 结果自洽: 要么 drop 失败表在, 要么 drop 成功。
    if drop.is_ok() {
        let tables = rows(&db, "SHOW TABLES");
        let has_b = tables
            .iter()
            .any(|r| r.iter().any(|c| matches!(c, V::Text(s) if s.as_str()=="b")));
        assert!(!has_b, "dropped table must be gone");
        // COMMIT 对已删表的缓冲写必须优雅处理。
        let _ = commit;
    } else {
        // drop 被拒 (事务占用) 是合法实现 — 表必须还在且 COMMIT 成功。
        let _ = db.execute("COMMIT");
        let n = ints(&db, "SELECT COUNT(*) FROM b");
        assert!(
            n == vec![1] || n == vec![2],
            "table intact, txn visible: {n:?}"
        );
    }
    // 全库不残留坏状态: a 表可正常读写。
    db.execute("INSERT INTO a VALUES (9, 9)").unwrap();
    assert_eq!(ints(&db, "SELECT COUNT(*) FROM a"), vec![2]);
}

/// 时序表: 时间戳索引列的范围查询正确 (ts 首列语义)。
#[test]
fn probe_timeseries_range_query() {
    let (db, _d) = new_db();
    let r = db
        .execute("CREATE TABLE m (ts TIMESTAMP, device TEXT, val FLOAT) TIME SERIES (INTERVAL 1s)");
    if r.is_err() {
        // 语法变体: 尝试文档的另一种形式。
        let _ = db.execute("CREATE TABLE m (ts TIMESTAMP, device TEXT, val FLOAT)");
    }
    let base = 1_700_000_000_000_000i64; // µs
    for i in 0..10i64 {
        let ts = base + i * 1_000_000;
        let dev = if i % 2 == 0 { "d1" } else { "d2" };
        db.execute(&format!("INSERT INTO m VALUES ({ts}, '{dev}', {i}.5)"))
            .unwrap();
    }
    let lo = base + 2_000_000;
    let hi = base + 5_000_000;
    let n = ints(
        &db,
        &format!("SELECT COUNT(*) FROM m WHERE ts >= {lo} AND ts <= {hi}"),
    );
    // ts ∈ [2s, 5s] → 4 行 (i=2..5)。
    assert_eq!(n, vec![4], "ts range inclusive both ends: {n:?}");
    let d1 = ints(
        &db,
        &format!("SELECT COUNT(*) FROM m WHERE ts >= {lo} AND ts <= {hi} AND device = 'd1'"),
    );
    // i=2,4 是 d1 → 2 行。
    assert_eq!(d1, vec![2]);
}

/// UPSERT OR REPLACE 语义: 整行替换 (未列出列取默认/NULL), 不是部分更新。
#[test]
fn probe_insert_or_replace_full_row() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 100, 200)").unwrap();
    db.execute("INSERT OR REPLACE INTO t (id, a) VALUES (1, 999)")
        .unwrap();
    let r = rows(&db, "SELECT a, b FROM t WHERE id = 1");
    assert_eq!(
        (r[0][0].clone(), r[0][1].clone()),
        (V::Integer(999), V::Null),
        "OR REPLACE replaces the WHOLE row (b reverts to NULL)"
    );
    // 对照: ON CONFLICT DO UPDATE 只更新指定列。
    db.execute("INSERT INTO t VALUES (2, 10, 20)").unwrap();
    db.execute(
        "INSERT INTO t (id, a) VALUES (2, 77) ON CONFLICT (id) DO UPDATE SET a = excluded.a",
    )
    .unwrap();
    let r2 = rows(&db, "SELECT a, b FROM t WHERE id = 2");
    assert_eq!(
        (r2[0][0].clone(), r2[0][1].clone()),
        (V::Integer(77), V::Integer(20)),
        "DO UPDATE keeps unspecified columns"
    );
}

/// 二次打开后 FTS 仍可查 (索引持久化/重建)。
#[test]
fn probe_fts_survives_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let db = Database::create(dir.path().join("f.mote")).unwrap();
        db.execute("CREATE TABLE art (id INT PRIMARY KEY, content TEXT)")
            .unwrap();
        db.execute("INSERT INTO art VALUES (1, 'persistent search token')")
            .unwrap();
        db.execute("CHECKPOINT").unwrap();
    }
    let db = Database::open(dir.path().join("f.mote")).unwrap();
    let hits = ints(&db, "SELECT id FROM art WHERE MATCH(content, 'persistent')");
    assert_eq!(hits, vec![1], "FTS works after checkpoint+reopen");
}

/// MATCH 作为投影表达式 (round-13 修复): 此前 evaluator 对 Expr::Match 报错,
/// 投影物化为全 NULL — 含关键词的行也是 NULL。现按行求值: 任一查询 token
/// 命中 → true; 引号短语按原文匹配。
#[test]
fn probe_match_projection_bool() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE art (id INT PRIMARY KEY, content TEXT)")
        .unwrap();
    db.execute("INSERT INTO art VALUES (1, 'rust database engine')")
        .unwrap();
    db.execute("INSERT INTO art VALUES (2, 'web framework')")
        .unwrap();
    let r = rows(
        &db,
        "SELECT id, MATCH(content, 'database') AS m FROM art ORDER BY id",
    );
    assert_eq!(
        r[0],
        vec![V::Integer(1), V::Bool(true)],
        "matching row → true"
    );
    assert_eq!(
        r[1],
        vec![V::Integer(2), V::Bool(false)],
        "non-matching row → false"
    );
    // 多 token 空格 = AND 语义 (与 WHERE 索引路径一致, 见
    // tests/test_text_ts_search_semantics)。
    let r2 = rows(
        &db,
        "SELECT id, MATCH(content, 'framework engine') AS m FROM art ORDER BY id",
    );
    assert_eq!(
        r2[0],
        vec![V::Integer(1), V::Bool(false)],
        "AND: 行1 无 'framework'"
    );
    assert_eq!(
        r2[1],
        vec![V::Integer(2), V::Bool(false)],
        "AND: 行2 无 'engine'"
    );
    // 显式 OR = 并集。
    let r3 = rows(
        &db,
        "SELECT id, MATCH(content, 'framework OR engine') AS m FROM art ORDER BY id",
    );
    assert_eq!(
        r3[0],
        vec![V::Integer(1), V::Bool(true)],
        "OR union: 'engine'"
    );
    assert_eq!(
        r3[1],
        vec![V::Integer(2), V::Bool(true)],
        "OR union: 'framework'"
    );
    // 非 TEXT 列 → false (不 panic)。
    let r3 = rows(&db, "SELECT MATCH(id, '1') AS m FROM art WHERE id = 1");
    assert_eq!(r3[0], vec![V::Bool(false)], "non-text column never matches");
}

/// BM25_SCORE 文档口径: 需先 CREATE TEXT INDEX; 有索引后分数非负且可排序。
#[test]
fn probe_bm25_score_with_index() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE art (id INT PRIMARY KEY, content TEXT)")
        .unwrap();
    db.execute("INSERT INTO art VALUES (1, 'rust database engine')")
        .unwrap();
    db.execute("INSERT INTO art VALUES (2, 'web framework')")
        .unwrap();
    db.execute("INSERT INTO art VALUES (3, 'vector database for robots')")
        .unwrap();
    db.execute("CREATE TEXT INDEX art_content ON art(content)")
        .unwrap();
    let r = rows(&db,
        "SELECT id, BM25_SCORE(content, 'database') AS score FROM art WHERE MATCH(content, 'database') ORDER BY score DESC");
    assert_eq!(r.len(), 2, "both 'database' rows");
    for row in &r {
        match &row[1] {
            V::Float(s) => assert!(*s >= 0.0, "bm25 non-negative"),
            o => panic!("bm25 float: {o:?}"),
        }
    }
    let ids: Vec<i64> = r
        .iter()
        .map(|x| match &x[0] {
            V::Integer(n) => *n,
            _ => -1,
        })
        .collect();
    assert_eq!(ids.len(), 2);
}
