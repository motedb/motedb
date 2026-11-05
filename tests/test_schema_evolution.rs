//! B2 schema 演进验收（0.18 设计文档 §B2）。
//!
//! DROP COLUMN = 逻辑删除（物理宽度不变，dropped 槽位 NULL 占位）;
//! RENAME COLUMN = 纯 schema 层。覆盖: 读写往返、checkpoint 后、reopen、
//! WAL 回放、守卫（PK/NOT NULL/索引）、SELECT * 跳过、INSERT 形态。

use motedb::sql::QueryResult;
use motedb::types::Value;
use motedb::Database;

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        o => panic!("{o:?}"),
    }
}

fn cols(db: &Database, sql: &str) -> Vec<String> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        QueryResult::Select { columns, .. } => columns,
        o => panic!("{o:?}"),
    }
}

#[test]
fn drop_column_logical_delete_roundtrip() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b TEXT, score FLOAT)")
        .unwrap();
    for i in 0..10 {
        db.execute(&format!(
            "INSERT INTO t VALUES ({i}, {i}0, 'w{i}', {}.5)",
            i as f64
        ))
        .unwrap();
    }
    db.checkpoint().unwrap();

    // DROP 中间列
    db.execute("ALTER TABLE t DROP COLUMN a").unwrap();

    // SELECT * 跳过 dropped; 引用 dropped 报错
    let names = cols(&db, "SELECT * FROM t LIMIT 1");
    assert_eq!(names, vec!["id", "b", "score"], "SELECT * 跳过 dropped");
    assert!(db.execute("SELECT a FROM t").is_err(), "dropped 引用应报错");

    // 剩余列数据完好
    let r = rows(&db, "SELECT id, b, score FROM t WHERE id = 3");
    assert_eq!(
        r[0],
        vec![Value::Integer(3), Value::text_from("w3"), Value::Float(3.5)]
    );

    // 位置式 INSERT（值对应可见列）; dropped 槽位 NULL 占位
    db.execute("INSERT INTO t VALUES (100, 'new', 9.5)")
        .unwrap();
    let r = rows(&db, "SELECT id, b, score FROM t WHERE id = 100");
    assert_eq!(r[0][1], Value::text_from("new"));

    // checkpoint + 增量后再读（物理宽度不变的混合段往返）
    for i in 200..205 {
        db.execute(&format!("INSERT INTO t VALUES ({i}, 'late{i}', 1.5)"))
            .unwrap();
    }
    db.checkpoint().unwrap();
    let n = rows(&db, "SELECT id FROM t").len();
    assert_eq!(n, 16, "drop 后全部行可见");

    // reopen 持久化
    let path = dir.path().to_path_buf();
    drop(db);
    let db = Database::open(&path).unwrap();
    let names = cols(&db, "SELECT * FROM t LIMIT 1");
    assert_eq!(names, vec!["id", "b", "score"], "reopen 后 dropped 仍隐藏");
    assert!(db.execute("SELECT a FROM t").is_err());
    let r = rows(&db, "SELECT b FROM t WHERE id = 203");
    assert_eq!(r[0][0], Value::text_from("late203"));
    db.close().unwrap();
}

#[test]
fn drop_column_guards() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE g (id INT PRIMARY KEY, req INT NOT NULL, plain INT, tagged TEXT)")
        .unwrap();
    db.execute("INSERT INTO g VALUES (1, 5, 6, 'x')").unwrap();

    // PK 拒绝
    assert!(db
        .execute("ALTER TABLE g DROP COLUMN id")
        .err()
        .is_some_and(|e| e.to_string().contains("primary key")));
    // NOT NULL 无 DEFAULT 拒绝
    assert!(db
        .execute("ALTER TABLE g DROP COLUMN req")
        .err()
        .is_some_and(|e| e.to_string().contains("NOT NULL")));
    // 有索引拒绝（v1 不级联）
    db.execute("CREATE INDEX gi ON g(plain)").unwrap();
    assert!(db
        .execute("ALTER TABLE g DROP COLUMN plain")
        .err()
        .is_some_and(|e| e.to_string().contains("index")));
    // 重复 DROP 拒绝
    db.execute("ALTER TABLE g DROP COLUMN tagged").unwrap();
    assert!(db
        .execute("ALTER TABLE g DROP COLUMN tagged")
        .err()
        .is_some_and(|e| e.to_string().contains("already dropped")));
    // 其余列不受影响
    let r = rows(&db, "SELECT id, req, plain FROM g");
    assert_eq!(
        r[0],
        vec![Value::Integer(1), Value::Integer(5), Value::Integer(6)]
    );
    db.close().unwrap();
}

#[test]
fn rename_column_roundtrip() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE r (id INT PRIMARY KEY, old_name INT)")
        .unwrap();
    db.execute("INSERT INTO r VALUES (1, 42)").unwrap();
    db.execute("CREATE INDEX ri ON r(old_name)").unwrap();

    // 有索引拒绝
    assert!(db
        .execute("ALTER TABLE r RENAME COLUMN old_name TO new_name")
        .err()
        .is_some_and(|e| e.to_string().contains("index")));

    db.execute("DROP INDEX ri").unwrap();
    db.execute("ALTER TABLE r RENAME COLUMN old_name TO new_name")
        .unwrap();

    // 旧名报错、新名可查
    assert!(db.execute("SELECT old_name FROM r").is_err());
    assert_eq!(
        rows(&db, "SELECT new_name FROM r WHERE id = 1")[0][0],
        Value::Integer(42)
    );
    // 以新名写
    db.execute("INSERT INTO r VALUES (2, 99)").unwrap();
    db.execute("UPDATE r SET new_name = 100 WHERE id = 1")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT new_name FROM r WHERE id = 1")[0][0],
        Value::Integer(100)
    );
    // 重名冲突拒绝
    db.execute("ALTER TABLE r ADD COLUMN other INT").unwrap();
    assert!(db
        .execute("ALTER TABLE r RENAME COLUMN other TO new_name")
        .err()
        .is_some_and(|e| e.to_string().contains("already exists")));

    // reopen 持久化
    db.checkpoint().unwrap();
    let path = dir.path().to_path_buf();
    drop(db);
    let db = Database::open(&path).unwrap();
    assert_eq!(
        rows(&db, "SELECT new_name FROM r WHERE id = 2")[0][0],
        Value::Integer(99),
        "rename 在 reopen 后保持"
    );
    db.close().unwrap();
}

/// DROP 后的 WAL 回放兼容: 崩溃窗口（不 close）后 reopen，dropped 语义保持。
#[test]
fn drop_column_wal_replay_survives_unclean_close() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("w.mote");
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE w (id INT PRIMARY KEY, a INT, b INT)")
            .unwrap();
        db.execute("INSERT INTO w VALUES (1, 10, 20)").unwrap();
        db.checkpoint().unwrap();
        db.execute("ALTER TABLE w DROP COLUMN a").unwrap();
        // dropped 后写入未 checkpoint 的行 → WAL 回放路径走 coerce
        db.execute("INSERT INTO w VALUES (2, 30)").unwrap();
        // 不 close（Drop 兜底不保证 checkpoint）——模拟崩溃窗口。
    }
    let db = Database::open(&path).unwrap();
    let names = cols(&db, "SELECT * FROM w LIMIT 1");
    assert_eq!(names, vec!["id", "b"]);
    let n = rows(&db, "SELECT id FROM w").len();
    assert_eq!(n, 2, "崩溃窗口后两行都在");
    assert!(db.execute("SELECT a FROM w").is_err(), "dropped 引用仍报错");
    db.close().unwrap();
}
