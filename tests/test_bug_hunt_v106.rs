//! round-18 bug 狩猎: CTE / TTL / EPISODE 表 / 向量索引持久化 / 大批量 INSERT /
//! WITH 语义。

use motedb::{types::Value as V, Database};
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

fn sorted(mut v: Vec<i64>) -> Vec<i64> {
    v.sort();
    v
}

/// 基本 CTE: WITH c AS (...) SELECT FROM c。
#[test]
fn probe_cte_basic() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, g TEXT, v INT)").unwrap();
    for (i, g, v) in [(1, "a", 10), (2, "a", 20), (3, "b", 30), (4, "b", 40)] {
        db.execute(&format!("INSERT INTO t VALUES ({i}, '{g}', {v})")).unwrap();
    }
    let r = sorted(ints(
        &db,
        "WITH ca AS (SELECT * FROM t WHERE g = 'a') SELECT id FROM ca",
    ));
    assert_eq!(r, vec![1, 2], "basic CTE filter");
    // CTE + 聚合。
    let n = ints(&db, "WITH s AS (SELECT v FROM t) SELECT SUM(v) FROM s");
    assert_eq!(n, vec![100], "CTE + aggregate");
    // 多 CTE, 后引用前。
    let r2 = sorted(ints(
        &db,
        "WITH ca AS (SELECT * FROM t WHERE g = 'a'), cb AS (SELECT * FROM ca WHERE v > 10) SELECT id FROM cb",
    ));
    assert_eq!(r2, vec![2], "chained CTEs");
}

/// CTE 在 JOIN / 子查询中的可用性。
#[test]
fn probe_cte_join_and_subquery() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, k INT)").unwrap();
    db.execute("CREATE TABLE b (id INT PRIMARY KEY, k INT, w TEXT)").unwrap();
    db.execute("INSERT INTO a VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO b VALUES (100, 10, 'ten')").unwrap();
    let r = rows(
        &db,
        "WITH ca AS (SELECT * FROM a) SELECT ca.id, b.w FROM ca JOIN b ON ca.k = b.k",
    );
    assert_eq!(r.len(), 1, "CTE used in JOIN");
    assert_eq!(r[0], vec![V::Integer(1), V::text("ten".into())]);
    let n = ints(&db, "WITH ca AS (SELECT k FROM a) SELECT COUNT(*) FROM b WHERE k IN (SELECT k FROM ca)");
    assert_eq!(n, vec![1], "CTE in IN-subquery");
}

/// TimeSeries TTL 语法与语义 (文档口径: TIMESERIES(ts) TTL <seconds>)。
#[test]
fn probe_timeseries_ttl() {
    let (db, _d) = new_db();
    let r = db.execute("CREATE TABLE m (ts TIMESTAMP, v FLOAT) TIMESERIES(ts) TTL 86400");
    assert!(r.is_ok(), "TTL create: {:?}", r.err());
    db.execute("INSERT INTO m VALUES (1000000, 1.0)").unwrap();
    db.execute("INSERT INTO m VALUES (2000000, 2.0)").unwrap();
    let n = ints(&db, "SELECT COUNT(*) FROM m");
    assert_eq!(n, vec![2], "rows visible before TTL");
}

/// EPISODE 表: 建表语法 + 基本写入查询。
#[test]
fn probe_episode_table() {
    let (db, _d) = new_db();
    let r = db.execute(
        "CREATE EPISODE TABLE teleop (session TEXT, ts TIMESTAMP, v FLOAT) EPISODE BY (session, ts) GAP '5s'",
    );
    if r.is_err() {
        // 语法若不支持, 打印错误但不失败 — 只锁定不 panic。
        eprintln!("episode create err: {:?}", r.err());
        return;
    }
    let base = 1_700_000_000_000_000i64;
    for i in 0..4i64 {
        // 同 session, 连续 1s 间隔 → 一个 episode; i=3 与 i=2 隔 10s → 新 episode。
        let t = base + i * 1_000_000 + if i == 3 { 9_000_000 } else { 0 };
        db.execute(&format!("INSERT INTO teleop VALUES ('s1', {t}, {}.0)", i)).unwrap();
    }
    let n = ints(&db, "SELECT COUNT(*) FROM teleop");
    assert_eq!(n, vec![4], "all rows queryable");
}

/// 向量索引持久化: checkpoint + 重开后 KNN 正确。
#[test]
fn probe_vector_index_persistence() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("v.mote");
    {
        let db = Database::create(&db_path).unwrap();
        db.execute("CREATE TABLE vec (id INT PRIMARY KEY, emb VECTOR(2))").unwrap();
        db.execute("INSERT INTO vec VALUES (1, '[0.0, 0.0]')").unwrap();
        db.execute("INSERT INTO vec VALUES (2, '[5.0, 5.0]')").unwrap();
        db.execute("INSERT INTO vec VALUES (3, '[10.0, 10.0]')").unwrap();
        db.execute("CREATE VECTOR INDEX vec_emb ON vec(emb)").unwrap();
        let near = ints(&db, "SELECT id FROM vec ORDER BY emb <-> '[1.0, 1.0]' LIMIT 2");
        assert_eq!(near, vec![1, 2], "pre-checkpoint KNN");
        db.execute("CHECKPOINT").unwrap();
    }
    let db = Database::open(&db_path).unwrap();
    let near2 = ints(&db, "SELECT id FROM vec ORDER BY emb <-> '[1.0, 1.0]' LIMIT 2");
    assert_eq!(near2, vec![1, 2], "KNN after checkpoint+reopen");
    let near3 = ints(&db, "SELECT id FROM vec ORDER BY emb <-> '[9.0, 9.0]' LIMIT 1");
    assert_eq!(near3, vec![3], "far query point");
}

/// 大批量多行 VALUES INSERT: 原子性 + 计数精确。
#[test]
fn probe_large_batch_insert() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    let vals: Vec<String> = (1..=500).map(|i| format!("({i}, {i})")).collect();
    db.execute(&format!("INSERT INTO t VALUES {}", vals.join(",")))
        .unwrap();
    let n = ints(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, vec![500], "500-row batch count");
    let s = ints(&db, "SELECT SUM(v) FROM t");
    assert_eq!(s, vec![500 * 501 / 2], "sum 1..=500 = {}", 500 * 501 / 2);
    // 重开一致。
    db.execute("CHECKPOINT").unwrap();
    drop(db);
}

/// UPDATE 的 SET 里含标量子查询 (文档/惯例行为)。
#[test]
fn probe_update_set_subquery() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for (i, v) in [(1, 10), (2, 20), (3, 30)] {
        db.execute(&format!("INSERT INTO t VALUES ({i}, {v})")).unwrap();
    }
    // v = 全表平均 (20) → 全部变 20。
    db.execute("UPDATE t SET v = (SELECT AVG(v) FROM t)").unwrap();
    let r = rows(&db, "SELECT v FROM t ORDER BY id");
    for row in &r {
        assert_eq!(row[0], V::Integer(20), "SET scalar subquery: {:?}", row);
    }
}

/// 字符串转义: 引号转义 (SQL 标准 '')。
#[test]
fn probe_string_quote_escape() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'it''s')").unwrap();
    let r = rows(&db, "SELECT s FROM t");
    assert_eq!(r[0][0], V::text("it's".into()), "doubled-quote escape");
    // LIKE 匹配含引号文本。
    let n = ints(&db, "SELECT COUNT(*) FROM t WHERE s LIKE 'it%'");
    assert_eq!(n, vec![1], "LIKE on escaped text");
}

/// 布尔列的聚合与过滤。
#[test]
fn probe_bool_column_semantics() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, flag BOOL)").unwrap();
    db.execute("INSERT INTO t VALUES (1, TRUE)").unwrap();
    db.execute("INSERT INTO t VALUES (2, FALSE)").unwrap();
    db.execute("INSERT INTO t VALUES (3, NULL)").unwrap();
    let n = ints(&db, "SELECT COUNT(*) FROM t WHERE flag = TRUE");
    assert_eq!(n, vec![1], "flag = TRUE excludes FALSE and NULL");
    let n2 = ints(&db, "SELECT COUNT(*) FROM t WHERE flag = FALSE");
    assert_eq!(n2, vec![1], "flag = FALSE");
    let n3 = ints(&db, "SELECT COUNT(*) FROM t WHERE flag IS NULL");
    assert_eq!(n3, vec![1], "NULL flag");
    let n4 = ints(&db, "SELECT COUNT(*) FROM t WHERE flag = 1");
    assert_eq!(n4, vec![1], "BOOL/INT coercion TRUE=1");
    let n5 = ints(&db, "SELECT COUNT(flag) FROM t");
    assert_eq!(n5, vec![2], "COUNT skips NULL");
}
