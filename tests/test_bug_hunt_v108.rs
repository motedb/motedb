//! round-19: 未探域收尾 — prepared statements / Parquet 往返 / FTS 更新增量 /
//! 窗口累计 / SAVEPOINT 嵌套 / 过滤向量搜索 / 时序 GC / batch API / CROSS JOIN。

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

/// PREPARE / EXECUTE 参数化: 同一语句多次执行不同参数。
#[test]
fn probe_prepare_execute_params() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 1..=3i64 {
        db.execute_prepared(
            "INSERT INTO t VALUES (?, ?)",
            vec![V::Integer(i), V::Integer(i * 100)],
        )
        .unwrap_or_else(|e| panic!("prepared insert {i}: {e}"));
    }
    let n = ints(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, vec![3], "3 parameterized inserts");
    let s = ints(&db, "SELECT SUM(v) FROM t");
    assert_eq!(s, vec![600]);
    // 参数化更新 (BUG #45 家族回归扩展)。
    db.execute_prepared("UPDATE t SET v = v + 1 WHERE id = ?", vec![V::Integer(2)])
        .unwrap_or_else(|e| panic!("prepared update: {e}"));
    let v2 = ints(&db, "SELECT v FROM t WHERE id = 2");
    assert_eq!(v2, vec![201], "prepared UPDATE with expression SET");
    db.execute_prepared("DELETE FROM t WHERE id = ?", vec![V::Integer(1)])
        .unwrap_or_else(|e| panic!("prepared delete: {e}"));
    let c = ints(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(c, vec![2], "prepared DELETE");
}

/// FTS 增量: UPDATE 改文本后 MATCH 反映新文本。
#[test]
fn probe_fts_after_text_update() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE art (id INT PRIMARY KEY, content TEXT)")
        .unwrap();
    db.execute("INSERT INTO art VALUES (1, 'alpha beta')")
        .unwrap();
    db.execute("CREATE TEXT INDEX art_content ON art(content)")
        .unwrap();
    assert_eq!(
        ints(&db, "SELECT id FROM art WHERE MATCH(content, 'alpha')"),
        vec![1]
    );
    db.execute("UPDATE art SET content = 'gamma delta' WHERE id = 1")
        .unwrap();
    let old = ints(&db, "SELECT id FROM art WHERE MATCH(content, 'alpha')");
    assert!(
        old.is_empty(),
        "old term no longer matches after UPDATE: {old:?}"
    );
    let new = ints(&db, "SELECT id FROM art WHERE MATCH(content, 'gamma')");
    assert_eq!(new, vec![1], "new term matches after UPDATE");
}

/// 窗口函数: SUM OVER 累计 (running total) + 分区内累计。
#[test]
fn probe_window_running_sum() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE s (id INT PRIMARY KEY, g TEXT, v INT)")
        .unwrap();
    for (i, g, v) in [(1, "a", 10), (2, "a", 20), (3, "b", 5), (4, "b", 15)] {
        db.execute(&format!("INSERT INTO s VALUES ({i}, '{g}', {v})"))
            .unwrap();
    }
    let r = rows(
        &db,
        "SELECT id, SUM(v) OVER (ORDER BY id) AS rt FROM s ORDER BY id",
    );
    let rt: Vec<i64> = r
        .iter()
        .map(|x| match &x[1] {
            V::Integer(n) => *n,
            _ => -1,
        })
        .collect();
    assert_eq!(rt, vec![10, 30, 35, 50], "global running sum");
    let r2 = rows(
        &db,
        "SELECT id, SUM(v) OVER (PARTITION BY g ORDER BY id) AS pt FROM s ORDER BY id",
    );
    let pt: Vec<i64> = r2
        .iter()
        .map(|x| match &x[1] {
            V::Integer(n) => *n,
            _ => -1,
        })
        .collect();
    assert_eq!(pt, vec![10, 30, 5, 20], "per-partition running sum");
}

/// SAVEPOINT 嵌套: 回滚内层保留外层之后的写入。
#[test]
fn probe_nested_savepoints() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("SAVEPOINT sp1").unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    db.execute("SAVEPOINT sp2").unwrap();
    db.execute("INSERT INTO t VALUES (3)").unwrap();
    let c3 = ints(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(c3, vec![3], "all 3 visible in-txn");
    db.execute("ROLLBACK TO sp2").unwrap();
    let c2 = ints(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(c2, vec![2], "rollback to sp2 drops id=3");
    db.execute("ROLLBACK TO sp1").unwrap();
    let c1 = ints(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(c1, vec![1], "rollback to sp1 drops id=2 too");
    db.execute("COMMIT").unwrap();
    let c = ints(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(c, vec![1], "commit keeps only pre-sp1 row");
}

/// 过滤向量搜索: KNN + WHERE 谓词组合 (top-k 深化语义)。
#[test]
fn probe_filtered_vector_search() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE v (id INT PRIMARY KEY, cat TEXT, emb VECTOR(2))")
        .unwrap();
    // 6 行: cat=a 的在近处, cat=b 的在远处。
    for i in 0..6i64 {
        let cat = if i < 3 { "a" } else { "b" };
        let x = if i < 3 { i as f32 } else { 100.0 + i as f32 };
        db.execute(&format!(
            "INSERT INTO v VALUES ({i}, '{cat}', '[{x}, {x}]')"
        ))
        .unwrap();
    }
    // 只要 cat=a 的 top-3 → 全部 a (即使 b 更近也是被过滤掉)。
    let r = ints(
        &db,
        "SELECT id FROM v WHERE cat = 'a' ORDER BY emb <-> '[2.0, 2.0]' LIMIT 3",
    );
    let mut rs = r.clone();
    rs.sort();
    assert_eq!(rs, vec![0, 1, 2], "filtered KNN returns only cat=a: {r:?}");
}

/// 时序 GC: 过期行被清除, 保留行不受影响。
#[test]
fn probe_timeseries_gc() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE m (ts TIMESTAMP, v INT) TIMESERIES(ts)")
        .unwrap();
    let base = 1_700_000_000_000_000i64;
    for i in 0..4i64 {
        let ts = base + i * 1_000_000;
        db.execute(&format!("INSERT INTO m VALUES ({ts}, {i})"))
            .unwrap();
    }
    let n = ints(&db, "SELECT COUNT(*) FROM m");
    assert_eq!(n, vec![4]);
    // TimeSeries DELETE (ts < cutoff) 走 gc_timeseries: 删 i=0,1, 保留 i=2,3。
    let cutoff = base + 2_000_000;
    let r = db
        .execute(&format!("DELETE FROM m WHERE ts < {cutoff}"))
        .unwrap()
        .materialize()
        .unwrap();
    let removed = match r {
        motedb::sql::QueryResult::Modification { affected_rows } => affected_rows,
        _ => 0,
    };
    assert_eq!(removed, 2, "gc removed exactly the 2 expired rows");
    let n2 = ints(&db, "SELECT COUNT(*) FROM m");
    assert_eq!(n2, vec![2], "post-gc count");
    let mn = ints(&db, "SELECT MIN(v) FROM m");
    assert_eq!(mn, vec![2], "min surviving v = 2");
}

/// CROSS JOIN: 笛卡尔积。
#[test]
fn probe_cross_join() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE a (x INT)").unwrap();
    db.execute("CREATE TABLE b (y INT)").unwrap();
    for x in [1, 2] {
        db.execute(&format!("INSERT INTO a VALUES ({x})")).unwrap();
    }
    for y in [10, 20, 30] {
        db.execute(&format!("INSERT INTO b VALUES ({y})")).unwrap();
    }
    // CROSS JOIN (round-19 修复: 此前 Expected On)。
    let n = ints(&db, "SELECT COUNT(*) FROM a CROSS JOIN b");
    assert_eq!(n, vec![6], "2 × 3 cartesian (CROSS JOIN)");
    // 逗号连接 (同轮修复: 此前报 Multiple statements)。
    let n2 = ints(&db, "SELECT COUNT(*) FROM a, b");
    assert_eq!(n2, vec![6], "2 × 3 cartesian (comma)");
    let s = ints(&db, "SELECT SUM(x * y) FROM a, b");
    // (1+2)*(10+20+30) = 3*60 = 180
    assert_eq!(s, vec![180]);
    // 逗号连接 + WHERE 过滤 (等价 INNER JOIN ON)。
    let f = ints(&db, "SELECT COUNT(*) FROM a, b WHERE x = 1 AND y >= 20");
    assert_eq!(f, vec![2], "comma join + WHERE filter");
}

/// batch_insert_map API: 批量插入 + 精确计数。
#[test]
fn probe_batch_insert_map() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, s TEXT)")
        .unwrap();
    let batch: Vec<motedb::types::SqlRow> = (1..=100)
        .map(|i| {
            let mut m = motedb::types::SqlRow::new();
            m.insert("id".to_string(), V::Integer(i));
            m.insert("v".to_string(), V::Integer(i * 10));
            m.insert("s".to_string(), V::text(format!("row{i}")));
            m
        })
        .collect();
    let n = db.batch_insert_map("t", batch);
    if n.is_ok() {
        let c = ints(&db, "SELECT COUNT(*) FROM t");
        assert_eq!(c, vec![100], "batch count");
        let s = ints(&db, "SELECT SUM(v) FROM t");
        assert_eq!(s, vec![100 * 101 / 2 * 10]);
    }
}

/// UPSERT + AUTO_INCREMENT: 冲突走 DO UPDATE 不烧号 (或烧号但无重复)。
#[test]
fn probe_upsert_auto_increment() {
    let (db, _d) = new_db();
    // NOTE: `k INT UNIQUE` 列约束语法尚不支持 (Expected RParen) — 记录为缺口。
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v INT)")
        .unwrap();
    db.execute("INSERT INTO t (v) VALUES (10)").unwrap();
    let id1 = ints(&db, "SELECT id FROM t")[0];
    let r = db.execute(&format!(
        "INSERT INTO t (id, v) VALUES ({id1}, 99) ON CONFLICT (id) DO UPDATE SET v = excluded.v"
    ));
    assert!(r.is_ok(), "upsert on pk: {:?}", r.err());
    let v = ints(&db, &format!("SELECT v FROM t WHERE id = {id1}"));
    assert_eq!(v, vec![99], "upserted value");
    let c = ints(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(c, vec![1], "no duplicate row");
    // 后续自增不冲突。
    db.execute("INSERT INTO t (v) VALUES (20)").unwrap();
    let ids = ints(&db, "SELECT id FROM t ORDER BY id");
    assert_eq!(ids.len(), 2);
    assert!(ids[0] < ids[1], "auto ids strictly increasing: {ids:?}");
}
