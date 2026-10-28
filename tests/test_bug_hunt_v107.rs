//! round-18 第二簇: DELETE 子查询 / UPSERT WHERE / 向量维度防御 / 时间戳字符串
//! 比较 / 表达式排序分组 / 整数除法 / ALTER+重开 / 嵌套子查询。

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

/// DELETE ... WHERE id IN (子查询)。
#[test]
fn probe_delete_with_subquery() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("CREATE TABLE b (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=5 {
        db.execute(&format!("INSERT INTO a VALUES ({i}, {i}00)")).unwrap();
    }
    for i in 3..=5 {
        db.execute(&format!("INSERT INTO b VALUES ({i}, 0)")).unwrap();
    }
    db.execute("DELETE FROM a WHERE id IN (SELECT id FROM b)").unwrap();
    let r = ints(&db, "SELECT id FROM a ORDER BY id");
    assert_eq!(r, vec![1, 2], "delete via IN-subquery");
}

/// UPSERT DO UPDATE ... WHERE 条件 (冲突时仅在条件满足才更新)。
#[test]
fn probe_upsert_do_update_where() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, n INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    let r = db.execute(
        "INSERT INTO t VALUES (1, 99) ON CONFLICT (id) DO UPDATE SET n = excluded.n WHERE t.n < 5",
    );
    if r.is_ok() {
        // 条件不满足 (10 >= 5) → 保留原值。
        let v = ints(&db, "SELECT n FROM t");
        assert_eq!(v, vec![10], "WHERE false keeps original");
    } else {
        // 语法不支持 → 当前值必须不变。
        let v = ints(&db, "SELECT n FROM t");
        assert_eq!(v, vec![10], "unsupported shape leaves row intact");
    }
    // 条件满足的形状。
    let r2 = db.execute(
        "INSERT INTO t VALUES (1, 99) ON CONFLICT (id) DO UPDATE SET n = excluded.n WHERE t.n > 5",
    );
    if r2.is_ok() {
        let v2 = ints(&db, "SELECT n FROM t");
        assert_eq!(v2, vec![99], "WHERE true applies update");
    }
}

/// 向量 KNN 查询点维度不匹配: 明确报错, 不 panic / 不静默错答。
#[test]
fn probe_vector_query_dim_mismatch() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE v (id INT PRIMARY KEY, emb VECTOR(2))").unwrap();
    db.execute("INSERT INTO v VALUES (1, '[0.0, 0.0]')").unwrap();
    let bad = db.execute("SELECT id FROM v ORDER BY emb <-> '[1.0, 2.0, 3.0]' LIMIT 1");
    assert!(bad.is_err(), "dim-mismatched query point must error");
    let _ = db.execute("SELECT id FROM v ORDER BY emb <-> '[1.0]' LIMIT 1");
    let n = ints(&db, "SELECT COUNT(*) FROM v");
    assert_eq!(n, vec![1], "table intact after rejected queries");
}

/// 时间戳列与 ISO 字符串比较。
#[test]
fn probe_timestamp_vs_string_comparison() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE m (ts TIMESTAMP, v INT)").unwrap();
    db.execute("INSERT INTO m VALUES ('2024-06-15T10:00:00', 1)").unwrap();
    db.execute("INSERT INTO m VALUES ('2024-06-15T12:00:00', 2)").unwrap();
    let n = ints(&db, "SELECT COUNT(*) FROM m WHERE ts >= '2024-06-15T11:00:00'");
    assert_eq!(n, vec![1], "string bound filters by time");
    let n2 = ints(&db, "SELECT COUNT(*) FROM m WHERE ts < '2024-06-15T11:00:00'");
    assert_eq!(n2, vec![1], "other side");
}

/// ORDER BY 表达式 + GROUP BY 表达式。
#[test]
fn probe_order_group_by_expression() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for (i, v) in [(1, 10), (2, 20), (3, 10), (4, 20)] {
        db.execute(&format!("INSERT INTO t VALUES ({i}, {v})")).unwrap();
    }
    let r = ints(&db, "SELECT id FROM t ORDER BY v DESC, id ASC");
    assert_eq!(r, vec![2, 4, 1, 3], "multi-key order");
    let g = rows(&db, "SELECT v / 10 AS bucket, COUNT(*) FROM t GROUP BY v / 10 ORDER BY bucket");
    assert_eq!(g.len(), 2, "GROUP BY expression buckets");
    // 表达式排序: -v 升序 = v 降序 (-20 < -10 → v=20 的行先出)。
    let r2 = ints(&db, "SELECT id FROM t ORDER BY -v, id");
    assert_eq!(r2, vec![2, 4, 1, 3], "ORDER BY -v (negation flips)");
}

/// 整数除法截断 + 负数运算。
#[test]
fn probe_integer_division_and_negatives() {
    let (db, _d) = new_db();
    let q = |s: &str| rows(&db, s).remove(0).remove(0);
    assert_eq!(q("SELECT 7 / 2"), V::Integer(3), "int division truncates");
    assert_eq!(q("SELECT -7 / 2"), V::Integer(-3), "truncation toward zero");
    assert_eq!(q("SELECT 7 % 2"), V::Integer(1));
    assert_eq!(q("SELECT -5 + 3"), V::Integer(-2));
    db.execute("CREATE TABLE t (v INT)").unwrap();
    for v in [-5, -1, 0, 1, 5] {
        db.execute(&format!("INSERT INTO t VALUES ({v})")).unwrap();
    }
    let s = ints(&db, "SELECT SUM(v) FROM t");
    assert_eq!(s, vec![0], "sum of symmetric values");
    let mn = ints(&db, "SELECT MIN(v) FROM t");
    assert_eq!(mn, vec![-5]);
    let desc = ints(&db, "SELECT v FROM t ORDER BY v DESC");
    assert_eq!(desc, vec![5, 1, 0, -1, -5], "negative ordering");
}

/// ALTER ADD COLUMN 后旧行读 NULL, 新行有值, 重开保持。
#[test]
fn probe_alter_add_column_and_reopen() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("a.mote");
    {
        let db = Database::create(&db_path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        db.execute("ALTER TABLE t ADD COLUMN b TEXT").unwrap();
        let r = rows(&db, "SELECT id, a, b FROM t");
        assert_eq!(r[0][2], V::Null, "old row: new column NULL");
        db.execute("INSERT INTO t (id, a, b) VALUES (2, 20, 'x')").unwrap();
        // 旧行 UPDATE 新列。
        db.execute("UPDATE t SET b = 'y' WHERE id = 1").unwrap();
        let r2 = rows(&db, "SELECT b FROM t ORDER BY id");
        assert_eq!(r2[0][0], V::text("y".into()));
        assert_eq!(r2[1][0], V::text("x".into()));
        db.execute("CHECKPOINT").unwrap();
    }
    let db = Database::open(&db_path).unwrap();
    let r3 = rows(&db, "SELECT id, a, b FROM t ORDER BY id");
    assert_eq!(r3.len(), 2);
    assert_eq!(r3[0][2], V::text("y".into()), "reopen: altered values persist");
    assert_eq!(r3[1][2], V::text("x".into()));
    let n = ints(&db, "SELECT COUNT(*) FROM t WHERE b IS NULL");
    assert_eq!(n, vec![0]);
}

/// 三层嵌套子查询。
#[test]
fn probe_nested_subqueries_three_deep() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE a (id INT PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE b (id INT PRIMARY KEY, aid INT)").unwrap();
    db.execute("CREATE TABLE c (id INT PRIMARY KEY, bid INT, v INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1)").unwrap();
    db.execute("INSERT INTO b VALUES (10, 1)").unwrap();
    db.execute("INSERT INTO c VALUES (100, 10, 42)").unwrap();
    db.execute("INSERT INTO c VALUES (101, 99, 7)").unwrap();
    let r = ints(
        &db,
        "SELECT v FROM c WHERE bid IN (SELECT id FROM b WHERE aid IN (SELECT id FROM a))",
    );
    assert_eq!(r, vec![42], "three-level nesting");
}

/// 跨类型比较: INT 列 vs TEXT 字面量不匹配 (不 panic)。
#[test]
fn probe_cross_type_comparison_safety() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    let _ = db.execute("SELECT * FROM t WHERE v = 'abc'");
    let _ = db.execute("SELECT * FROM t WHERE v = '10'");
    let _ = db.execute("SELECT * FROM t WHERE id = 1.5");
    let n = ints(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, vec![1], "table intact after cross-type probes");
}

/// UPDATE 影响行数 + 不匹配 WHERE 的 UPDATE 返回 0。
#[test]
fn probe_update_affected_rows() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for (i, v) in [(1, 1), (2, 2), (3, 3)] {
        db.execute(&format!("INSERT INTO t VALUES ({i}, {v})")).unwrap();
    }
    let r = db
        .execute("UPDATE t SET v = 99 WHERE v > 1")
        .unwrap()
        .materialize()
        .unwrap();
    if let motedb::sql::QueryResult::Modification { affected_rows } = r {
        assert_eq!(affected_rows, 2, "affected rows for v > 1");
    } else {
        panic!("not Modification");
    }
    let r0 = db
        .execute("UPDATE t SET v = 0 WHERE id > 100")
        .unwrap()
        .materialize()
        .unwrap();
    if let motedb::sql::QueryResult::Modification { affected_rows } = r0 {
        assert_eq!(affected_rows, 0, "no match → 0 affected");
    }
}
