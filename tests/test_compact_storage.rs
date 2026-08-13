//! compact_storage 模式正确性测试。
//! 验证 compact_storage=true（for_edge/robotics/embodied 默认）下
//! PK 点查返回正确数据（BUG-1+2 回归测试）。

use motedb::types::Value;
use motedb::{Database, QueryResult};

fn fval(db: &Database, sql: &str) -> i64 {
    match db.execute(sql).unwrap().materialize().unwrap() {
        QueryResult::Select { rows, .. } => match rows.get(0).and_then(|r| r.get(0)) {
            Some(Value::Integer(i)) => *i,
            other => panic!("expected int, got {:?}", other),
        },
        _ => panic!("expected select"),
    }
}

#[test]
fn test_compact_storage_pk_point_query() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut config = motedb::DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, name TEXT)")
        .unwrap();
    for i in 1..=100 {
        db.execute(&format!(
            "INSERT INTO t VALUES ({}, {}, 'item{}')",
            i,
            i * 10,
            i
        ))
        .unwrap();
    }
    db.checkpoint().unwrap();

    // PK point queries must return correct values
    for i in 1..=100 {
        let v = fval(&db, &format!("SELECT v FROM t WHERE id = {}", i));
        assert_eq!(v, i * 10, "PK id={} should have v={}, got {}", i, i * 10, v);
    }
}

#[test]
fn test_compact_storage_where_int_filter() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut config = motedb::DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 1..=50 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i))
            .unwrap();
    }
    db.checkpoint().unwrap();

    let count = fval(&db, "SELECT COUNT(*) FROM t WHERE v > 25");
    assert_eq!(count, 25, "WHERE v > 25 should return 25 rows");

    let count = fval(&db, "SELECT COUNT(*) FROM t WHERE v <= 10");
    assert_eq!(count, 10, "WHERE v <= 10 should return 10 rows");
}

#[test]
fn test_compact_storage_reopen_correctness() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let mut config = motedb::DBConfig::for_edge();
        config.max_result_rows = None;
        let db = Database::create_with_config(&path, config).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .unwrap();
        for i in 1..=20 {
            db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 100))
                .unwrap();
        }
        db.checkpoint().unwrap();
        db.close().unwrap();
    }

    let mut config = motedb::DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::open_with_config(&path, config).unwrap();

    // After reopen, data must be correct
    for i in 1..=20 {
        let v = fval(&db, &format!("SELECT v FROM t WHERE id = {}", i));
        assert_eq!(
            v,
            i * 100,
            "After reopen: PK id={} should have v={}",
            i,
            i * 100
        );
    }

    let count = fval(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(count, 20, "After reopen: should have 20 rows");
}
