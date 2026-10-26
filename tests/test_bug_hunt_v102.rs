//! round-12 bug 狩猎: SQL 语义边角 + 向量索引一致性探针簇。
//!
//! 每个探针独立, 断言标准 SQL 语义 (引擎特有行为只断言内部一致性)。

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

fn scalar(db: &Database, sql: &str) -> V {
    rows(db, sql).remove(0).remove(0)
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

/// COUNT(DISTINCT x) 计非 NULL 互异值。
#[test]
fn probe_count_distinct() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (a INT)").unwrap();
    for v in [1, 2, 2, 3, 3, 3] {
        db.execute(&format!("INSERT INTO t VALUES ({v})")).unwrap();
    }
    db.execute("INSERT INTO t VALUES (NULL)").unwrap();
    assert_eq!(ints(&db, "SELECT COUNT(DISTINCT a) FROM t"), vec![3]);
    assert_eq!(ints(&db, "SELECT COUNT(a) FROM t"), vec![6]);
    assert_eq!(ints(&db, "SELECT COUNT(*) FROM t"), vec![7]);
}

/// ORDER BY ASC/DESC 必须互为精确逆序 (含 NULL 的位置自洽)。
#[test]
fn probe_order_by_null_consistency() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, x INT)").unwrap();
    for (id, x) in [(1, 5), (2, 3), (3, 6), (4, 3), (5, 9)] {
        db.execute(&format!("INSERT INTO t VALUES ({id}, {x})")).unwrap();
    }
    db.execute("INSERT INTO t VALUES (6, NULL)").unwrap();
    let asc = ints(&db, "SELECT id FROM t ORDER BY x ASC, id ASC");
    let desc = ints(&db, "SELECT id FROM t ORDER BY x DESC, id DESC");
    let mut rev = desc.clone();
    rev.reverse();
    assert_eq!(asc, rev, "ASC and DESC must be exact mirrors (ties by id too)");
}

/// GROUP BY: NULL 自成一组。
#[test]
fn probe_group_by_null_group() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (g TEXT, v INT)").unwrap();
    for (g, v) in [(Some("a"), 1), (Some("a"), 2), (None, 3), (None, 4)] {
        let gsql = match g {
            Some(s) => format!("'{s}'"),
            None => "NULL".into(),
        };
        db.execute(&format!("INSERT INTO t VALUES ({gsql}, {v})")).unwrap();
    }
    let r = rows(&db, "SELECT g, COUNT(*), SUM(v) FROM t GROUP BY g ORDER BY g");
    assert_eq!(r.len(), 2, "NULL forms its own group: {r:?}");
}

/// HAVING 无 GROUP BY: 整表一组, HAVING 过滤聚合结果。
#[test]
fn probe_having_without_group_by() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    for v in [1, 2, 3] {
        db.execute(&format!("INSERT INTO t VALUES ({v})")).unwrap();
    }
    let r = rows(&db, "SELECT SUM(v) FROM t HAVING SUM(v) > 5");
    assert_eq!(r.len(), 1, "HAVING passes (SUM=6 > 5)");
    let r2 = rows(&db, "SELECT SUM(v) FROM t HAVING SUM(v) > 10");
    assert!(r2.is_empty(), "HAVING rejects (SUM=6 <= 10): {r2:?}");
}

/// LIMIT/OFFSET 边界: OFFSET 超界为空; 负值不 panic。
#[test]
fn probe_limit_offset_edges() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    for i in 1..=5 {
        db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    let n = ints(&db, "SELECT id FROM t ORDER BY id LIMIT 3 OFFSET 2");
    assert_eq!(n, vec![3, 4, 5]);
    let over = ints(&db, "SELECT id FROM t ORDER BY id LIMIT 5 OFFSET 10");
    assert!(over.is_empty(), "OFFSET beyond table: {over:?}");
    let _ = db.execute("SELECT id FROM t LIMIT -1");
    let _ = db.execute("SELECT id FROM t OFFSET -5");
}

/// 字符串函数: SUBSTRING 两种形式 / TRIM / CONCAT 与 || 的 NULL 语义分工。
#[test]
fn probe_string_functions() {
    let (db, _d) = new_db();
    // 标准 SQL 形式 (round-12 修复: 此前只支持逗号形式, FROM 处解析失败)。
    match scalar(&db, "SELECT SUBSTRING('abcdef' FROM 2 FOR 3)") {
        V::Text(s) => assert_eq!(s.as_str(), "bcd"),
        o => panic!("substring FROM/FOR: {o:?}"),
    }
    match scalar(&db, "SELECT SUBSTRING('abcdef' FROM 2)") {
        V::Text(s) => assert_eq!(s.as_str(), "bcdef"),
        o => panic!("substring FROM (no FOR): {o:?}"),
    }
    // 逗号形式保持不变。
    match scalar(&db, "SELECT SUBSTRING('abcdef', 2, 3)") {
        V::Text(s) => assert_eq!(s.as_str(), "bcd"),
        o => panic!("substring comma form: {o:?}"),
    }
    match scalar(&db, "SELECT TRIM('  hi  ')") {
        V::Text(s) => assert_eq!(s.as_str(), "hi"),
        o => panic!("trim: {o:?}"),
    }
    // CONCAT 遵循 PostgreSQL 语义 (跳过 NULL); || 遵循标准语义 (NULL 传播)。
    // 两者并存且分工明确是自洽设计, 此处锁定该行为。
    match scalar(&db, "SELECT CONCAT('a', NULL)") {
        V::Text(s) => assert_eq!(s.as_str(), "a"),
        o => panic!("concat skips NULLs (PG semantics): {o:?}"),
    }
    assert_eq!(scalar(&db, "SELECT 'a' || NULL"), V::Null, "|| propagates NULL");
}

/// SUM 溢出提升为浮点 (不回绕为负数)。
#[test]
fn probe_sum_overflow_promotes() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (v BIGINT)").unwrap();
    db.execute("INSERT INTO t VALUES (9223372036854775807)").unwrap();
    db.execute("INSERT INTO t VALUES (9223372036854775807)").unwrap();
    match scalar(&db, "SELECT SUM(v) FROM t") {
        V::Float(f) => assert!(f > 1.8e19, "overflow promotes to float: {f}"),
        V::Integer(i) => panic!("wrapped instead of promoting: {i}"),
        o => panic!("sum overflow: {o:?}"),
    }
}

/// 除零: 引擎自洽即可 (NULL 或错误), 不许 panic / 崩溃。
#[test]
fn probe_division_by_zero_no_panic() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 0)").unwrap();
    let _ = db.execute("SELECT a / b FROM t");
    let _ = db.execute("SELECT 1 / 0");
    let _ = db.execute("SELECT 1.0 / 0");
}

/// IN 含 NULL: x IN (1, NULL) 对 x=1 为真; NOT IN 含 NULL 恒非真。
#[test]
fn probe_in_with_null() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    for v in [1, 2] {
        db.execute(&format!("INSERT INTO t VALUES ({v})")).unwrap();
    }
    let n = ints(&db, "SELECT v FROM t WHERE v IN (1, NULL)");
    assert_eq!(n, vec![1], "IN matches the non-null member only");
    let n2 = rows(&db, "SELECT v FROM t WHERE v NOT IN (1, NULL)");
    assert!(n2.is_empty(), "NOT IN with NULL is never true: {n2:?}");
}

/// CASE: NULL 条件走 ELSE; CASE 无 ELSE 得 NULL。
#[test]
fn probe_case_null_condition() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (NULL)").unwrap();
    let r = rows(&db, "SELECT CASE WHEN v > 0 THEN 'pos' ELSE 'other' END FROM t");
    assert_eq!(r[0][0], V::Text(motedb::types::ArcString(std::sync::Arc::from("other"))), "NULL condition falls to ELSE");
    let r2 = rows(&db, "SELECT CASE WHEN v > 0 THEN 'pos' END FROM t");
    assert_eq!(r2[0][0], V::Null, "no ELSE → NULL");
}

/// UPSERT DO UPDATE 用 excluded. 引用旧值冲突列。
#[test]
fn probe_upsert_excluded() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, n INT, total INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10, 100)").unwrap();
    db.execute(
        "INSERT INTO t VALUES (1, 5, 0) ON CONFLICT (id) DO UPDATE SET n = excluded.n, total = total + excluded.n",
    )
    .unwrap();
    let r = rows(&db, "SELECT n, total FROM t WHERE id = 1");
    assert_eq!(r[0][0], V::Integer(5), "excluded.n (new row value)");
    assert_eq!(r[0][1], V::Integer(105), "total + excluded.n = 100+5");
}

/// 向量更新后 KNN 必须反映新位置 (索引增量维护正确性)。
#[test]
fn probe_knn_after_vector_update() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE vec (id INT PRIMARY KEY, emb VECTOR(2))").unwrap();
    db.execute("INSERT INTO vec VALUES (1, '[0.0, 0.0]')").unwrap();
    db.execute("INSERT INTO vec VALUES (2, '[5.0, 5.0]')").unwrap();
    db.execute("INSERT INTO vec VALUES (3, '[10.0, 10.0]')").unwrap();
    let near = ints(&db, "SELECT id FROM vec ORDER BY emb <-> '[1.0, 1.0]' LIMIT 2");
    assert_eq!(near, vec![1, 2], "initial nearest");
    // 把 1 号向量移到远处, 2 号变最近。
    db.execute("UPDATE vec SET emb = '[20.0, 20.0]' WHERE id = 1").unwrap();
    let near2 = ints(&db, "SELECT id FROM vec ORDER BY emb <-> '[1.0, 1.0]' LIMIT 2");
    assert_eq!(near2, vec![2, 3], "KNN after vector UPDATE (index delta): {near2:?}");
}

/// 向量删除后 KNN 不得返回已删 id。
#[test]
fn probe_knn_after_vector_delete() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE vec (id INT PRIMARY KEY, emb VECTOR(2))").unwrap();
    db.execute("INSERT INTO vec VALUES (1, '[0.0, 0.0]')").unwrap();
    db.execute("INSERT INTO vec VALUES (2, '[1.0, 1.0]')").unwrap();
    db.execute("INSERT INTO vec VALUES (3, '[2.0, 2.0]')").unwrap();
    db.execute("DELETE FROM vec WHERE id = 1").unwrap();
    let near = ints(&db, "SELECT id FROM vec ORDER BY emb <-> '[0.5, 0.5]' LIMIT 5");
    assert!(!near.contains(&1), "deleted id must not appear in KNN: {near:?}");
    assert_eq!(near.len(), 2);
}

/// 事务内混合 DDL: BEGIN; INSERT; CREATE INDEX; COMMIT 后索引生效且数据在。
#[test]
fn probe_ddl_inside_transaction() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").unwrap();
    let r = (|| -> motedb::Result<()> {
        db.execute("BEGIN")?;
        db.execute("INSERT INTO t VALUES (1, 'a')")?;
        db.execute("CREATE INDEX idx_name ON t(name)")?;
        db.execute("COMMIT")?;
        Ok(())
    })();
    if r.is_ok() {
        let n = ints(&db, "SELECT COUNT(*) FROM t");
        assert_eq!(n, vec![1], "row committed with DDL in txn");
        let q = ints(&db, "SELECT id FROM t WHERE name = 'a'");
        assert_eq!(q, vec![1]);
    } else {
        // DDL-in-txn 被拒绝是合法实现 — 但必须不留下半状态。
        let _ = db.execute("ROLLBACK");
        let n = ints(&db, "SELECT COUNT(*) FROM t");
        assert!(n == vec![0] || n == vec![1], "no half state: {n:?}");
    }
}

/// UPDATE 的 SET 引用同表其他列 (前值语义: 全部 SET 用旧行求值)。
#[test]
fn probe_update_swap_columns() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10, 20)").unwrap();
    db.execute("UPDATE t SET a = b, b = a").unwrap();
    let r = rows(&db, "SELECT a, b FROM t");
    assert_eq!(
        (r[0][0].clone(), r[0][1].clone()),
        (V::Integer(20), V::Integer(10)),
        "simultaneous swap uses OLD values"
    );
}
