//! round-12 第二簇: 聚合空表语义 / 标量子查询 / EXISTS / 向量边界 / 自连接。

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

/// 空表聚合: SUM/AVG/MIN/MAX → NULL, COUNT → 0 (标准语义)。
#[test]
fn probe_aggregates_on_empty_table() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    assert_eq!(ints(&db, "SELECT COUNT(*) FROM t"), vec![0]);
    let r = rows(&db, "SELECT SUM(v), AVG(v), MIN(v), MAX(v) FROM t");
    assert_eq!(
        r[0],
        vec![V::Null, V::Null, V::Null, V::Null],
        "empty-table aggregates are NULL"
    );
    // 空表 + WHERE 过滤同样。
    let r2 = rows(&db, "SELECT SUM(v) FROM t WHERE v > 0");
    assert_eq!(r2[0], vec![V::Null]);
    // GROUP BY 空表 → 0 组。
    let r3 = rows(&db, "SELECT v, COUNT(*) FROM t GROUP BY v");
    assert!(r3.is_empty(), "no groups on empty table: {r3:?}");
}

/// MIN/MAX 忽略 NULL; 全 NULL 列 → NULL。
#[test]
fn probe_min_max_ignore_null() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (5)").unwrap();
    db.execute("INSERT INTO t VALUES (NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (3)").unwrap();
    let r = rows(&db, "SELECT MIN(v), MAX(v), COUNT(v) FROM t");
    assert_eq!(
        r[0],
        vec![V::Integer(3), V::Integer(5), V::Integer(2)],
        "MIN/MAX/COUNT ignore NULLs"
    );
    // 全 NULL 列。
    db.execute("CREATE TABLE n (v INT)").unwrap();
    db.execute("INSERT INTO n VALUES (NULL)").unwrap();
    db.execute("INSERT INTO n VALUES (NULL)").unwrap();
    let r2 = rows(
        &db,
        "SELECT MIN(v), MAX(v), SUM(v), COUNT(v), COUNT(*) FROM n",
    );
    assert_eq!(
        r2[0],
        vec![V::Null, V::Null, V::Null, V::Integer(0), V::Integer(2)]
    );
}

/// 标量子查询: SELECT 列表里的子查询对每行求值。
#[test]
fn probe_scalar_subquery_in_select_list() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE d (id INT PRIMARY KEY, n INT)")
        .unwrap();
    db.execute("INSERT INTO d VALUES (1, 100)").unwrap();
    db.execute("INSERT INTO d VALUES (2, 200)").unwrap();
    let r = ints(
        &db,
        "SELECT id + (SELECT n FROM d WHERE id = 1) FROM d ORDER BY id",
    );
    assert_eq!(r, vec![101, 102], "scalar subquery per row");
}

/// EXISTS / NOT EXISTS 语义。
#[test]
fn probe_exists_semantics() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE a (id INT PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE b (a_id INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1)").unwrap();
    db.execute("INSERT INTO a VALUES (2)").unwrap();
    db.execute("INSERT INTO b VALUES (1)").unwrap();
    let has = ints(
        &db,
        "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.a_id = a.id)",
    );
    assert_eq!(has, vec![1], "EXISTS correlated");
    let none = ints(
        &db,
        "SELECT id FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.a_id = a.id)",
    );
    assert_eq!(none, vec![2], "NOT EXISTS");
}

/// 自连接: 同表两别名必须互相独立。
#[test]
fn probe_self_join_independence() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE e (id INT PRIMARY KEY, boss INT)")
        .unwrap();
    db.execute("INSERT INTO e VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO e VALUES (2, 1)").unwrap();
    db.execute("INSERT INTO e VALUES (3, 1)").unwrap();
    let r = ints(
        &db,
        "SELECT w.id FROM e w JOIN e b ON w.boss = b.id ORDER BY w.id",
    );
    assert_eq!(
        r,
        vec![2, 3],
        "self-join resolves both aliases independently"
    );
}

/// 向量: k 超过行数返回全部; 维度不匹配的插入被拒绝 (不静默截断)。
#[test]
fn probe_vector_k_and_dim_boundaries() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE v (id INT PRIMARY KEY, emb VECTOR(2))")
        .unwrap();
    db.execute("INSERT INTO v VALUES (1, '[0.0, 0.0]')")
        .unwrap();
    db.execute("INSERT INTO v VALUES (2, '[3.0, 4.0]')")
        .unwrap();
    // k > 行数 → 返回全部 (不 panic)。
    let all = ints(
        &db,
        "SELECT id FROM v ORDER BY emb <-> '[0.0, 0.0]' LIMIT 10",
    );
    assert_eq!(all, vec![1, 2]);
    // 维度不匹配的向量必须被拒绝或报错 — 不许静默接受。
    let bad = db.execute("INSERT INTO v VALUES (3, '[1.0, 2.0, 3.0]')");
    assert!(bad.is_err(), "dim-mismatch insert must be rejected");
    // 修复后表内容不变。
    let n = ints(&db, "SELECT COUNT(*) FROM v");
    assert_eq!(n, vec![2]);
}

/// 向量距离算子族: <-> (L2) 与 <=> (cosine) 对同一数据给出一致的 top-1
/// (此处用正交向量, 两算子 top1 一致是可判定的)。
#[test]
fn probe_vector_operator_variants() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE v (id INT PRIMARY KEY, emb VECTOR(2))")
        .unwrap();
    db.execute("INSERT INTO v VALUES (1, '[1.0, 0.0]')")
        .unwrap();
    db.execute("INSERT INTO v VALUES (2, '[0.0, 1.0]')")
        .unwrap();
    let l2 = ints(
        &db,
        "SELECT id FROM v ORDER BY emb <-> '[0.9, 0.1]' LIMIT 1",
    );
    let cos = ints(
        &db,
        "SELECT id FROM v ORDER BY emb <=> '[0.9, 0.1]' LIMIT 1",
    );
    assert_eq!(l2, vec![1], "L2 nearest");
    assert_eq!(
        cos,
        vec![1],
        "cosine nearest (same winner for near-axis query)"
    );
}

/// 嵌套聚合引用: WHERE 里用标量聚合子查询。
#[test]
fn probe_aggregate_subquery_in_where() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for (i, v) in [(1, 10), (2, 20), (3, 30)] {
        db.execute(&format!("INSERT INTO t VALUES ({i}, {v})"))
            .unwrap();
    }
    let r = ints(
        &db,
        "SELECT id FROM t WHERE v > (SELECT AVG(v) FROM t) ORDER BY id",
    );
    assert_eq!(r, vec![3], "AVG=20; strict > keeps only v=30");
}

/// IN 子查询 (非静态列表)。
#[test]
fn probe_in_subquery() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE a (id INT PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE b (x INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1)").unwrap();
    db.execute("INSERT INTO a VALUES (2)").unwrap();
    db.execute("INSERT INTO a VALUES (3)").unwrap();
    db.execute("INSERT INTO b VALUES (2)").unwrap();
    db.execute("INSERT INTO b VALUES (3)").unwrap();
    let r = ints(
        &db,
        "SELECT id FROM a WHERE id IN (SELECT x FROM b) ORDER BY id",
    );
    assert_eq!(r, vec![2, 3]);
    let r2 = ints(
        &db,
        "SELECT id FROM a WHERE id NOT IN (SELECT x FROM b) ORDER BY id",
    );
    assert_eq!(r2, vec![1]);
}

/// 字符串比较序与 ORDER BY 一致 (二进制/字典序自洽)。
#[test]
fn probe_text_ordering_consistency() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    for (i, s) in [(1, "apple"), (2, "Banana"), (3, "apple"), (4, "cherry")] {
        db.execute(&format!("INSERT INTO t VALUES ({i}, '{s}')"))
            .unwrap();
    }
    let asc = rows(&db, "SELECT s FROM t ORDER BY s ASC, id ASC");
    let desc = rows(&db, "SELECT s FROM t ORDER BY s DESC, id DESC");
    let mut rev = desc.clone();
    rev.reverse();
    assert_eq!(asc, rev, "text ASC/DESC mirror consistency");
    // 相等比较大小写敏感 (标准默认)。
    let n = ints(&db, "SELECT COUNT(*) FROM t WHERE s = 'apple'");
    assert_eq!(n, vec![2], "case-sensitive equality");
}
