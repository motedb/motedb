//! 显式整型 PK 大批快路径 (fast_batch_insert_explicit) 差分测试:
//! row_id=PK 语义 / 批内与存量唯一性 + 回滚 / auto-inc 表 counter 越位 /
//! 负值 PK 回退慢路径 / 重开一致 / 后续 UPDATE/DELETE 的 pk_cache 精确性。
use motedb::types::Value;
use motedb::{Database, DBConfig};
use tempfile::TempDir;

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        motedb::sql::QueryResult::Select { rows, .. } => rows,
        _ => panic!("expected select"),
    }
}

fn count(db: &Database, sql: &str) -> i64 {
    match rows(db, sql).pop().unwrap().pop().unwrap() {
        Value::Integer(v) => v,
        other => panic!("{:?}", other),
    }
}

fn mk_rows(start: i64, n: usize, v_offset: f64) -> Vec<Vec<Value>> {
    (0..n)
        .map(|i| {
            vec![
                Value::Integer(start + i as i64),
                Value::Text(format!("row-{}", start + i as i64).into()),
                Value::Float(v_offset + i as f64),
            ]
        })
        .collect()
}

/// row_id = PK: 点查/范围查/ORDER BY 都按 PK 语义工作。
#[test]
fn row_id_equals_pk() {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_testing();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, s TEXT, v FLOAT)").unwrap();
    db.insert_rows("t", mk_rows(1000, 500, 1.0)).unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 500);
    // 点查 (row_id 二分路径)
    let r = rows(&db, "SELECT s, v FROM t WHERE id = 1234");
    assert_eq!(r, vec![vec![Value::Text("row-1234".into()), Value::Float(235.0)]]);
    // 范围 + ORDER BY id
    let r = rows(&db, "SELECT id FROM t WHERE id >= 1495 ORDER BY id DESC LIMIT 3");
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1499)],
            vec![Value::Integer(1498)],
            vec![Value::Integer(1497)]
        ]
    );
}

/// 批内重复 → 报错且不留占位 (错误后同批重插成功)。
#[test]
fn intra_batch_duplicate_errors_cleanly() {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_testing();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, s TEXT, v FLOAT)").unwrap();
    let mut batch = mk_rows(10, 150, 1.0);
    batch[149][0] = Value::Integer(10); // 与首行重复
    let err = db.insert_rows("t", batch);
    assert!(err.is_err(), "intra-batch duplicate must error");
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 0, "no partial rows");
    // 重插 (无重复) 成功 — 上一失败的保留键已回滚
    db.insert_rows("t", mk_rows(10, 150, 1.0)).unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 150);
    // 存量重复同样报错
    let err = db.insert_rows("t", mk_rows(10, 150, 1.0));
    assert!(err.is_err(), "existing duplicate must error");
}

/// auto-inc 表带显式 PK 大批: id 保留 + counter 越位 (后续 auto 不撞)。
#[test]
fn autoinc_table_explicit_ids_counter_bumps() {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_testing();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY AUTO_INCREMENT, s TEXT, v FLOAT)").unwrap();
    db.insert_rows("t", mk_rows(5000, 150, 1.0)).unwrap();
    let r = rows(&db, "SELECT s FROM t WHERE id = 5149");
    assert_eq!(r, vec![vec![Value::Text("row-5149".into())]]);
    // 后续 auto 分配必须 > 5149
    db.execute("INSERT INTO t (s, v) VALUES ('auto', 9.0)").unwrap();
    let r = rows(&db, "SELECT id FROM t WHERE s = 'auto'");
    match &r[0][0] {
        Value::Integer(id) => assert!(*id > 5149, "auto id {} must exceed explicit max", id),
        other => panic!("{:?}", other),
    }
}

/// 负值 / 超界 PK → 慢路径 (语义仍正确)。
#[test]
fn negative_pk_falls_back_correctly() {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_testing();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, s TEXT, v FLOAT)").unwrap();
    let batch: Vec<Vec<Value>> = (0..150)
        .map(|i| {
            vec![
                Value::Integer(-5000 + i as i64),
                Value::Text(format!("neg-{}", i).into()),
                Value::Float(i as f64),
            ]
        })
        .collect();
    db.insert_rows("t", batch).unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 150);
    let r = rows(&db, "SELECT s FROM t WHERE id = -5000");
    assert_eq!(r, vec![vec![Value::Text("neg-0".into())]]);
}

/// 重开一致 + 快路径插入后的 UPDATE/DELETE (pk_cache 存真实 row_id)。
#[test]
fn reopen_and_crud_after_fastpath() {
    let dir = TempDir::new().unwrap();
    {
        let mut config = DBConfig::for_testing();
        config.max_result_rows = None;
        let db = Database::create_with_config(dir.path(), config).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, s TEXT, v FLOAT)").unwrap();
        db.insert_rows("t", mk_rows(1, 300, 1.0)).unwrap();
        db.execute("UPDATE t SET v = 99.5 WHERE id = 150").unwrap();
        db.execute("DELETE FROM t WHERE id = 151").unwrap();
        db.close().unwrap();
    }
    let mut config = DBConfig::for_testing();
    config.max_result_rows = None;
    let db = Database::open_with_config(dir.path(), config).unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 299);
    let r = rows(&db, "SELECT v FROM t WHERE id = 150");
    assert_eq!(r, vec![vec![Value::Float(99.5)]]);
    let r = rows(&db, "SELECT id FROM t WHERE id = 151");
    assert!(r.is_empty());
}
