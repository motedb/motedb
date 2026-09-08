//! execute_prepared_many: one INSERT template, N parameter sets, executed as
//! a single multi-row INSERT (one WAL fsync + batched index updates).
use motedb::types::Value;
use motedb::{DBConfig, Database};
use tempfile::TempDir;

fn count(db: &Database, sql: &str) -> i64 {
    let r = db.execute(sql).unwrap().materialize().unwrap();
    let (_, rows) = r.select_rows().unwrap();
    match &rows[0][0] {
        Value::Integer(i) => *i,
        v => panic!("expected int, got {:?}", v),
    }
}

#[test]
fn executemany_basic_insert_and_readback() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score FLOAT)")
        .unwrap();

    let batch: Vec<Vec<Value>> = (1..=100i64)
        .map(|i| {
            vec![
                Value::Integer(i),
                Value::Text(format!("user{}", i).into()),
                Value::Float(i as f64 / 2.0),
            ]
        })
        .collect();
    let affected = db
        .execute_prepared_many("INSERT INTO t (id, name, score) VALUES (?, ?, ?)", batch)
        .unwrap();
    assert_eq!(affected, 100);

    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 100);
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM t WHERE id > 50"),
        50,
        "params must land in the right columns"
    );
    let sum = count(&db, "SELECT SUM(id) AS s FROM t");
    assert_eq!(sum, 5050);
}

#[test]
fn executemany_auto_increment_pk() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v INT)")
        .unwrap();

    let batch: Vec<Vec<Value>> = (0..50).map(|i| vec![Value::Integer(i)]).collect();
    let affected = db
        .execute_prepared_many("INSERT INTO t (v) VALUES (?)", batch)
        .unwrap();
    assert_eq!(affected, 50);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 50);
    // DISTINCT ids must all be assigned (no NULL/duplicate PKs)
    assert_eq!(count(&db, "SELECT COUNT(DISTINCT id) FROM t"), 50);
}

#[test]
fn executemany_empty_batch_is_noop() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    let affected = db
        .execute_prepared_many("INSERT INTO t (id) VALUES (?)", vec![])
        .unwrap();
    assert_eq!(affected, 0);
}

#[test]
fn executemany_rejects_non_insert() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    let err = db.execute_prepared_many("SELECT 1", vec![vec![]]);
    assert!(err.is_err(), "non-INSERT must be rejected");
}

#[test]
fn executemany_partial_literal_template() {
    // Template mixing literals and parameters.
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, kind TEXT, v INT)")
        .unwrap();
    let batch: Vec<Vec<Value>> = (1..=10i64).map(|i| vec![Value::Integer(i)]).collect();
    db.execute_prepared_many(
        "INSERT INTO t (id, kind, v) VALUES (?, 'sensor', 42)",
        batch,
    )
    .unwrap();
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM t WHERE kind='sensor' AND v=42"),
        10
    );
}

#[test]
fn executemany_survives_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .unwrap();
        let batch: Vec<Vec<Value>> = (1..=30i64)
            .map(|i| vec![Value::Integer(i), Value::Integer(i * 10)])
            .collect();
        db.execute_prepared_many("INSERT INTO t (id, v) VALUES (?, ?)", batch)
            .unwrap();
        db.execute("CHECKPOINT").unwrap();
        db.close().unwrap();
    }
    let db = Database::open(dir.path()).unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 30);
    assert_eq!(count(&db, "SELECT SUM(v) FROM t"), 4650);
}
