//! 相关子查询外引用绑定回归 (第六轮 bug 猎杀)。
//!
//! 两个数据正确性 bug:
//! ① bind_outer_columns 把子查询 WHERE 里**两侧**的表限定列都绑到外行
//!   (自 join 同表时代的 设计), 内外表不同但裸列同名时 `b.k = a.k`
//!   被改写成 `a.k = a.k` 恒真 — 相关 EXISTS 匹配所有行、相关标量子
//!   查询计数恒为内表全量。
//! ② try_positional_where 用未检相关的 materialize_subqueries eager 执行
//!   相关标量子查询 (外引用被剥前缀解析到内表同名列, 同样恒真)。
use motedb::Database;
use tempfile::TempDir;

fn setup() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("cs.mote")).unwrap();
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, k INT)")
        .unwrap();
    db.execute("CREATE TABLE b (id INT PRIMARY KEY, k INT)")
        .unwrap();
    for i in 1..=3i64 {
        db.execute(&format!("INSERT INTO a VALUES ({i}, {})", i * 10))
            .unwrap();
    }
    db.execute("INSERT INTO b VALUES (1, 10)").unwrap(); // 只与 a.id=1 同 k
    db.execute("INSERT INTO b VALUES (2, 999)").unwrap();
    (dir, db)
}

fn ids(db: &Database, sql: &str) -> Vec<i64> {
    let r = db.execute(sql).unwrap().materialize().unwrap();
    r.select_rows()
        .unwrap()
        .1
        .iter()
        .map(|r| match &r[0] {
            motedb::types::Value::Integer(i) => *i,
            o => panic!("{o:?}"),
        })
        .collect()
}

#[test]
fn correlated_exists_respects_inner_refs() {
    let (_dir, db) = setup();
    // 只有 a.id=1 的 k(10) 在 b 中存在。
    assert_eq!(
        ids(
            &db,
            "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k)"
        ),
        vec![1],
        "correlated EXISTS must match only the correlated row"
    );
    assert_eq!(
        ids(
            &db,
            "SELECT id FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k = a.k)"
        ),
        vec![2, 3],
        "NOT EXISTS is the exact complement"
    );
    // 非相关 EXISTS 不回归。
    assert_eq!(
        ids(&db, "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b)"),
        vec![1, 2, 3]
    );
}

#[test]
fn correlated_scalar_counts_inner_matches() {
    let (_dir, db) = setup();
    let r = db
        .execute(
            "SELECT id, (SELECT COUNT(*) FROM b WHERE b.k = a.k) AS c \
             FROM a ORDER BY id",
        )
        .unwrap()
        .materialize()
        .unwrap();
    let (_c, rows) = r.select_rows().unwrap();
    let got: Vec<(i64, i64)> = rows
        .iter()
        .map(|r| match (&r[0], &r[1]) {
            (motedb::types::Value::Integer(a), motedb::types::Value::Integer(b)) => (*a, *b),
            o => panic!("{o:?}"),
        })
        .collect();
    assert_eq!(
        got,
        vec![(1, 1), (2, 0), (3, 0)],
        "per-row correlated count"
    );

    // WHERE 里的相关标量子查询 (try_positional_where 路径)。
    assert_eq!(
        ids(
            &db,
            "SELECT id FROM a WHERE (SELECT COUNT(*) FROM b WHERE b.k = a.k) > 0"
        ),
        vec![1],
        "correlated scalar in WHERE"
    );
}

#[test]
fn innermost_scope_wins_for_bare_names() {
    // 无限定名列在子查询内取最内层作用域 (SQL 标准): `b.k = k` 中 k 即
    // b.k → 恒真 → 所有外行匹配。钉住该语义防止"修复"引入歧义。
    let (_dir, db) = setup();
    assert_eq!(
        ids(
            &db,
            "SELECT id FROM a WHERE (SELECT COUNT(*) FROM b WHERE b.k = k) > 0"
        ),
        vec![1, 2, 3],
        "bare name resolves to the innermost scope"
    );
}

#[test]
fn correlated_subquery_with_join_shape() {
    // joins 形状 (users/depts, 外引用不同裸名) — 曾因剥前缀解析到内表
    // 缺列而恒 false (EXISTS 空结果) 的另一面。
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("cs2.mote")).unwrap();
    db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, dept INT)")
        .unwrap();
    db.execute("CREATE TABLE depts (id INT PRIMARY KEY, dname TEXT)")
        .unwrap();
    for (i, n, d) in [(1, "alice", 10), (2, "bob", 99), (3, "carol", 10)] {
        db.execute(&format!("INSERT INTO users VALUES ({i}, '{n}', {d})"))
            .unwrap();
    }
    db.execute("INSERT INTO depts VALUES (10, 'eng')").unwrap();
    let got = ids(
        &db,
        "SELECT id FROM users WHERE EXISTS \
         (SELECT 1 FROM depts WHERE depts.id = users.dept) ORDER BY name",
    );
    assert_eq!(
        got,
        vec![1, 3],
        "only alice(1)/carol(3) have an existing dept"
    );
}
