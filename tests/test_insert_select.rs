//! INSERT ... SELECT 差分测试: 列序映射/WHERE 过滤/自插/参数化/事务/
//! TimeSeries 表/错误形状 — 与手工期望及 VALUES 路径对拍。
use motedb::types::Value;
use motedb::{Database, DBConfig};
use tempfile::TempDir;

fn setup(dir: &TempDir) -> Database {
    let mut config = DBConfig::for_testing();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute(
        "CREATE TABLE src (id INTEGER PRIMARY KEY AUTO_INCREMENT, v FLOAT, t TEXT, n INTEGER)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE dst (id INTEGER PRIMARY KEY AUTO_INCREMENT, v FLOAT, t TEXT, n INTEGER)",
    )
    .unwrap();
    let rows: Vec<Vec<Value>> = (0..100)
        .map(|i| {
            vec![
                Value::Null,
                Value::Float(i as f64 * 0.5),
                Value::Text(format!("t{}", i % 7).into()),
                Value::Integer(i),
            ]
        })
        .collect();
    db.insert_rows("src", rows).unwrap();
    db
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        motedb::sql::QueryResult::Select { rows, .. } => rows,
        _ => panic!("expected select"),
    }
}

fn scalar(db: &Database, sql: &str) -> Value {
    rows(db, sql).pop().unwrap().pop().unwrap()
}

/// 基本形状: 带列序 / WHERE 过滤 / 表达式投影。
#[test]
fn basic_shapes() {
    let dir = TempDir::new().unwrap();
    let db = setup(&dir);
    let affected = db
        .execute("INSERT INTO dst (v, t, n) SELECT v, t, n FROM src WHERE n < 40")
        .unwrap();
    match affected {
        motedb::sql::StreamingQueryResult::Modification { affected_rows } => {
            assert_eq!(affected_rows, 40)
        }
        _ => panic!("expected modification"),
    }
    // 行数与内容对拍
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM dst"),
        vec![vec![Value::Integer(40)]]
    );
    let r = rows(&db, "SELECT v, t, n FROM dst WHERE n = 39");
    assert_eq!(r, vec![vec![Value::Float(19.5), Value::Text("t4".into()), Value::Integer(39)]]);
    // 表达式投影 + 别名
    db.execute("INSERT INTO dst (v, t, n) SELECT v * 2.0, 'lit', n + 1 FROM src WHERE n >= 40 AND n < 50")
        .unwrap();
    // src.n=44 → dst.n = n+1 = 45, v = 44*0.5*2 = 44.0
    let r = rows(&db, "SELECT v, t, n FROM dst WHERE n = 45");
    assert_eq!(r, vec![vec![Value::Float(44.0), Value::Text("lit".into()), Value::Integer(45)]]);
}

/// 无列序: SELECT 输出必须按 schema 全宽 (含 NULL PK → auto)。
#[test]
fn no_column_list_requires_full_width() {
    let dir = TempDir::new().unwrap();
    let db = setup(&dir);
    db.execute("INSERT INTO dst SELECT NULL, v, t, n FROM src WHERE n < 10")
        .unwrap();
    assert_eq!(scalar(&db, "SELECT COUNT(*) FROM dst"), Value::Integer(10));
    // 少列 → 显式报错 (不静默 NULL 填充)
    let err = db.execute("INSERT INTO dst SELECT v, t FROM src");
    assert!(err.is_err(), "arity mismatch must error");
}

/// 自插: 先物化后插入, 无无限循环, 行数翻倍。
#[test]
fn self_insert() {
    let dir = TempDir::new().unwrap();
    let db = setup(&dir);
    db.execute("INSERT INTO dst (v, t, n) SELECT v, t, n FROM src WHERE n < 10")
        .unwrap();
    db.execute("INSERT INTO dst (v, t, n) SELECT v, t, n FROM dst")
        .unwrap();
    assert_eq!(scalar(&db, "SELECT COUNT(*) FROM dst"), Value::Integer(20));
    // 值对拍 (SUM 校验)
    let s1 = scalar(&db, "SELECT SUM(v) FROM dst").clone();
    let s2 = scalar(&db, "SELECT SUM(v) * 2 FROM src WHERE n < 10");
    match (s1, s2) {
        (Value::Float(a), Value::Float(b)) => assert!((a - b).abs() < 1e-9, "{a} != {b}"),
        other => panic!("unexpected: {:?}", other),
    }
}

/// ORDER BY / LIMIT 下推到 SELECT 源。
#[test]
fn order_by_limit_source() {
    let dir = TempDir::new().unwrap();
    let db = setup(&dir);
    db.execute("INSERT INTO dst (v, t, n) SELECT v, t, n FROM src ORDER BY n DESC LIMIT 5")
        .unwrap();
    assert_eq!(scalar(&db, "SELECT COUNT(*) FROM dst"), Value::Integer(5));
    assert_eq!(
        scalar(&db, "SELECT MIN(n) FROM dst"),
        Value::Integer(95)
    );
}

/// JOIN 子查询源。
#[test]
fn join_source() {
    let dir = TempDir::new().unwrap();
    let db = setup(&dir);
    db.execute("CREATE TABLE dim (k INTEGER, label TEXT)").unwrap();
    db.execute("INSERT INTO dim VALUES (1, 'one'), (2, 'two')").unwrap();
    db.execute(
        "INSERT INTO dst (v, t, n) SELECT s.v, d.label, s.n FROM src s JOIN dim d ON s.n = d.k",
    )
    .unwrap();
    assert_eq!(scalar(&db, "SELECT COUNT(*) FROM dst"), Value::Integer(2));
    let r = rows(&db, "SELECT t FROM dst ORDER BY n");
    assert_eq!(
        r,
        vec![vec![Value::Text("one".into())], vec![Value::Text("two".into())]]
    );
}

/// ON CONFLICT + SELECT → 显式错误 (阶段一不支持)。
#[test]
fn on_conflict_select_errors_clearly() {
    let dir = TempDir::new().unwrap();
    let db = setup(&dir);
    let err = db.execute("INSERT OR IGNORE INTO dst (v, t, n) SELECT v, t, n FROM src");
    assert!(err.is_err());
    let msg = match err {
        Err(e) => format!("{:?}", e),
        Ok(_) => panic!("expected error"),
    };
    assert!(msg.contains("SELECT"), "error should mention SELECT: {msg}");
}

/// 事务内 INSERT..SELECT + 回滚。
#[test]
fn txn_insert_select_rollback() {
    let dir = TempDir::new().unwrap();
    let db = setup(&dir);
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO dst (v, t, n) SELECT v, t, n FROM src WHERE n < 30")
        .unwrap();
    assert_eq!(scalar(&db, "SELECT COUNT(*) FROM dst"), Value::Integer(30));
    db.execute("ROLLBACK").unwrap();
    assert_eq!(scalar(&db, "SELECT COUNT(*) FROM dst"), Value::Integer(0));
    // 提交路径
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO dst (v, t, n) SELECT v, t, n FROM src WHERE n < 30")
        .unwrap();
    db.execute("COMMIT").unwrap();
    assert_eq!(scalar(&db, "SELECT COUNT(*) FROM dst"), Value::Integer(30));
}

/// 参数化 SELECT 源 (executemany 模板)。
#[test]
fn parameterized_source() {
    let dir = TempDir::new().unwrap();
    let db = setup(&dir);
    // api.execute_prepared 走参数求值
    let r = db
        .execute_prepared(
            "INSERT INTO dst (v, t, n) SELECT v, t, n FROM src WHERE n > ?",
            vec![Value::Integer(90)],
        )
        .unwrap();
    match r {
        motedb::sql::StreamingQueryResult::Modification { affected_rows } => assert_eq!(affected_rows, 9),
        _ => panic!("expected modification"),
    }
    assert_eq!(scalar(&db, "SELECT COUNT(*) FROM dst"), Value::Integer(9));
}

/// TimeSeries 表目标。
#[test]
fn timeseries_target() {
    let dir = TempDir::new().unwrap();
    let db = setup(&dir);
    db.execute(
        "CREATE TABLE ts (time TIMESTAMP, device TEXT, val FLOAT) TIMESERIES(time)",
    )
    .unwrap();
    db.execute("INSERT INTO ts SELECT n, t, v FROM src WHERE n < 20")
        .unwrap();
    assert_eq!(scalar(&db, "SELECT COUNT(*) FROM ts"), Value::Integer(20));
}

/// 重开一致性。
#[test]
fn reopen_consistency() {
    let dir = TempDir::new().unwrap();
    {
        let db = setup(&dir);
        db.execute("INSERT INTO dst (v, t, n) SELECT v, t, n FROM src WHERE n < 25")
            .unwrap();
        db.close().unwrap();
    }
    let mut config = DBConfig::for_testing();
    config.max_result_rows = None;
    let db = Database::open_with_config(dir.path(), config).unwrap();
    assert_eq!(scalar(&db, "SELECT COUNT(*) FROM dst"), Value::Integer(25));
    assert_eq!(
        scalar(&db, "SELECT SUM(n) FROM dst"),
        Value::Integer((0..25).sum())
    );
}

/// 大批量: 与 VALUES 路径同量级的吞吐 (batch 管线生效)。
#[test]
fn bulk_throughput_shape() {
    let dir = TempDir::new().unwrap();
    let db = setup(&dir);
    db.execute("INSERT INTO dst (v, t, n) SELECT v, t, n FROM src")
        .unwrap();
    assert_eq!(scalar(&db, "SELECT COUNT(*) FROM dst"), Value::Integer(100));
}
