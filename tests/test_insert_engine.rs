//! 核心插入引擎专项差分测试:
//! 1. insert_rows 按位/省略自增 PK — 快慢两条路径行为一致
//! 2. 显式自增 PK 在大批 (≥100) 不被 auto id 静默改写
//! 3. 字典序无关性由绑定层保证 (见 test_insert_arrays.py)

use motedb::types::Value;
use motedb::{Database, DBConfig};
use tempfile::TempDir;

fn setup(dir: &TempDir) -> Database {
    let mut config = DBConfig::for_testing();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY AUTO_INCREMENT, c TEXT, v FLOAT)",
    )
    .unwrap();
    db
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        motedb::sql::QueryResult::Select { rows, .. } => rows,
        _ => panic!("expected select"),
    }
}

/// 省略 PK: 小批 (<100 慢路径) 与大批 (≥100 快路径) 都必须补 auto id 且
/// 其余列值不错位 (旧 bug: 字典序转置 + row[0]=id 覆盖首列 → 数据全毁)。
#[test]
fn omitted_pk_small_and_large_batches_align() {
    let dir = TempDir::new().unwrap();
    let db = setup(&dir);

    let small: Vec<Vec<Value>> = (0..50)
        .map(|i| {
            vec![
                Value::Null,
                Value::Text(format!("small-{}", i).into()),
                Value::Float(i as f64),
            ]
        })
        .collect();
    db.insert_rows("t", small).unwrap();

    let large: Vec<Vec<Value>> = (0..150)
        .map(|i| {
            vec![
                Value::Null,
                Value::Text(format!("large-{}", i).into()),
                Value::Float(1000.0 + i as f64),
            ]
        })
        .collect();
    db.insert_rows("t", large).unwrap();

    let r = rows(&db, "SELECT id, c, v FROM t ORDER BY id");
    assert_eq!(r.len(), 200, "both batches inserted");
    for (i, row) in r.iter().enumerate() {
        let id = match &row[0] {
            Value::Integer(v) => *v,
            other => panic!("row {}: id is not Integer: {:?}", i, other),
        };
        assert_eq!(id, (i + 1) as i64, "auto ids are sequential from 1");
        // 小批先插 (small-0..49), 大批后插 (large-0..149) — 首列必须匹配
        let expect_c = if id <= 50 {
            format!("small-{}", id - 1)
        } else {
            format!("large-{}", id - 51)
        };
        match (&row[1], &row[2]) {
            (Value::Text(c), Value::Float(v)) => {
                assert_eq!(c.as_str(), expect_c, "column c aligned at id {}", id);
                assert_eq!(
                    *v,
                    if id <= 50 {
                        (id - 1) as f64
                    } else {
                        1000.0 + (id - 51) as f64
                    },
                    "column v aligned at id {}",
                    id
                );
            }
            other => panic!("row {}: wrong types: {:?}", i, other),
        }
    }
}

/// 显式自增 PK 在大批中必须保留 (旧行为: fast path 用 counter id 覆盖)。
#[test]
fn explicit_pk_in_large_batch_is_honored() {
    let dir = TempDir::new().unwrap();
    let db = setup(&dir);

    let batch: Vec<Vec<Value>> = (0..150)
        .map(|i| {
            vec![
                Value::Integer(1000 + i as i64),
                Value::Text(format!("ex-{}", i).into()),
                Value::Float(i as f64),
            ]
        })
        .collect();
    db.insert_rows("t", batch).unwrap();

    let r = rows(&db, "SELECT id, c FROM t WHERE id >= 1000 ORDER BY id");
    assert_eq!(r.len(), 150, "all explicit ids present");
    for (i, row) in r.iter().enumerate() {
        match (&row[0], &row[1]) {
            (Value::Integer(id), Value::Text(c)) => {
                assert_eq!(*id, 1000 + i as i64, "explicit id preserved");
                assert_eq!(c.as_str(), format!("ex-{}", i));
            }
            other => panic!("wrong types: {:?}", other),
        }
    }
    // 重复显式 PK 必须报错 (唯一性)
    let dup_row = || {
        vec![
            Value::Integer(1000),
            Value::Text("dup".into()),
            Value::Float(0.0),
        ]
    };
    let dup: Vec<Vec<Value>> = (0..150).map(|_| dup_row()).collect();
    assert!(
        db.insert_rows("t", dup).is_err(),
        "duplicate explicit PK must fail"
    );
    // 后续 auto 分配不得与显式 id 撞 (counter 应越过 1149)
    let after: Vec<Vec<Value>> = (0..5)
        .map(|i| {
            vec![
                Value::Null,
                Value::Text(format!("auto-{}", i).into()),
                Value::Float(0.0),
            ]
        })
        .collect();
    db.insert_rows("t", after).unwrap();
    let r = rows(&db, "SELECT id FROM t WHERE c LIKE 'auto-%' ORDER BY id");
    for row in &r {
        if let Value::Integer(id) = &row[0] {
            assert!(*id > 1149, "auto id {} must not collide with explicit ids", id);
        }
    }
}

/// 大批 + 混合 (部分显式 PK 部分 NULL) 走全路径, NULL 由 counter 补。
#[test]
fn mixed_pk_large_batch_uses_full_path() {
    let dir = TempDir::new().unwrap();
    let db = setup(&dir);

    let mut batch: Vec<Vec<Value>> = Vec::new();
    batch.push(vec![
        Value::Integer(500),
        Value::Text("explicit".into()),
        Value::Float(1.0),
    ]);
    for i in 0..149 {
        batch.push(vec![
            Value::Null,
            Value::Text(format!("n-{}", i).into()),
            Value::Float(2.0),
        ]);
    }
    db.insert_rows("t", batch).unwrap();

    let r = rows(&db, "SELECT c FROM t WHERE id = 500");
    assert_eq!(r.len(), 1, "explicit id 500 present");
    match &r[0][0] {
        Value::Text(c) => assert_eq!(c.as_str(), "explicit"),
        other => panic!("wrong type: {:?}", other),
    }
    // NULL PK 行得到 counter id (从 start=1 起, 500 已被显式占用不会给)
    let r = rows(&db, "SELECT id, c FROM t WHERE c LIKE 'n-%'");
    assert_eq!(r.len(), 149);
    for row in &r {
        if let Value::Integer(id) = &row[0] {
            assert!(*id != 500, "auto id must not collide with explicit 500");
        }
    }
}
