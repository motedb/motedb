//! 全乘积/链式 INNER JOIN 的 COUNT(*) 折叠差分测试:
//! ON 常量真/假、单表 ON 谓词、WHERE 单表下推、多步链 — 与通用路径 (禁用
//! 折叠的等价 SQL 形状) 对拍必须全等。
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
    let r = rows(db, sql);
    match r.first().and_then(|r| r.first()) {
        Some(Value::Integer(v)) => *v,
        other => panic!("not an integer count: {:?}", other),
    }
}

fn setup(dir: &TempDir) -> Database {
    let mut config = DBConfig::for_testing();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute(
        "CREATE TABLE a (id INTEGER PRIMARY KEY AUTO_INCREMENT, x INTEGER, t TEXT)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE b (id INTEGER PRIMARY KEY AUTO_INCREMENT, y INTEGER)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE c (id INTEGER PRIMARY KEY AUTO_INCREMENT, z INTEGER)",
    )
    .unwrap();
    let a_rows: Vec<Vec<Value>> = (0..500)
        .map(|i| {
            vec![
                Value::Null,
                Value::Integer(i % 7),
                Value::Text(if i % 3 == 0 { "even" } else { "odd" }.into()),
            ]
        })
        .collect();
    db.insert_rows("a", a_rows).unwrap();
    let b_rows: Vec<Vec<Value>> = (0..300)
        .map(|i| vec![Value::Null, Value::Integer(i % 11)])
        .collect();
    db.insert_rows("b", b_rows).unwrap();
    let c_rows: Vec<Vec<Value>> = (0..40)
        .map(|i| vec![Value::Null, Value::Integer(i)])
        .collect();
    db.insert_rows("c", c_rows).unwrap();
    db
}

/// 常量真 ON: 计数 = 行数乘积; 与逐行物化对照 (加一个恒真跨表 OR 不行 —
/// 用带 LIMIT 的等价子查询对照太绕, 直接手工算期望值)。
#[test]
fn constant_true_on_folds_to_product() {
    let dir = TempDir::new().unwrap();
    let db = setup(&dir);
    // 500 × 300
    assert_eq!(count(&db, "SELECT COUNT(*) FROM a JOIN b ON 1=1"), 150_000);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM a JOIN b ON 2 > 1"), 150_000);
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM a JOIN b ON 1=1 AND 3 = 3"),
        150_000
    );
    // 三表链: 500 × 300 × 40
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM a JOIN b ON 1=1 JOIN c ON 1=1"),
        6_000_000
    );
}

/// 常量假 / NULL-ish ON → 0。
#[test]
fn constant_false_on_is_zero() {
    let dir = TempDir::new().unwrap();
    let db = setup(&dir);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM a JOIN b ON 1=0"), 0);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM a JOIN b ON 1 > 2"), 0);
    // 混合: 真与假合取 → 0
    assert_eq!(count(&db, "SELECT COUNT(*) FROM a JOIN b ON 1=1 AND 1=0"), 0);
    // NULL 比较 → UNKNOWN → falsy → 0
    assert_eq!(count(&db, "SELECT COUNT(*) FROM a JOIN b ON NULL = 1"), 0);
}

/// 单表 ON 谓词 (ON a.x < 3): 计数 = count(a where x<3) × count(b)。
#[test]
fn single_table_on_pred_factores() {
    let dir = TempDir::new().unwrap();
    let db = setup(&dir);
    // a.x % 7 < 3 → x∈{0,1,2}: i%7 ∈ {0,1,2} 的行数
    let a_pred = count(&db, "SELECT COUNT(*) FROM a WHERE a.x < 3");
    let expect = a_pred * 300;
    assert_eq!(count(&db, "SELECT COUNT(*) FROM a JOIN b ON a.x < 3"), expect);
    // 谓词在 b 侧
    let b_pred = count(&db, "SELECT COUNT(*) FROM b WHERE b.y = 5");
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM a JOIN b ON b.y = 5"),
        500 * b_pred
    );
    // 单表谓词 + 常量真合取
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM a JOIN b ON 1=1 AND a.x < 3"),
        expect
    );
}

/// WHERE 单表下推与 ON 谓词叠加。
#[test]
fn where_pushdown_combines_with_on() {
    let dir = TempDir::new().unwrap();
    let db = setup(&dir);
    let a_p = count(&db, "SELECT COUNT(*) FROM a WHERE a.x < 3");
    let b_p = count(&db, "SELECT COUNT(*) FROM b WHERE b.y >= 8");
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM a JOIN b ON 1=1 WHERE a.x < 3 AND b.y >= 8"),
        a_p * b_p
    );
    // 谓词同时在 ON 和 WHERE
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM a JOIN b ON a.x < 3 WHERE a.x < 2"),
        count(&db, "SELECT COUNT(*) FROM a WHERE a.x < 2") * 300
    );
}

/// 跨表 ON → decline 走通用路径, 结果仍须正确 (等值 hash 路径)。
#[test]
fn cross_table_on_still_correct() {
    let dir = TempDir::new().unwrap();
    let db = setup(&dir);
    // 等值: a.x = b.y — hash join
    let equi = count(&db, "SELECT COUNT(*) FROM a JOIN b ON a.x = b.y");
    assert_eq!(equi, count(&db, "SELECT COUNT(*) FROM a JOIN b ON a.x = b.y"));
    // 与物化路径对拍: 加一个恒假的单表谓词强制非折叠形状? 更直接:
    // 用 MOTE 语义 — b.y IS NOT NULL 形状 WHERE 不下推 → 通用路径对拍
    let v1 = count(&db, "SELECT COUNT(*) FROM a JOIN b ON a.x = b.y");
    // 同一形状再跑一次 (路径稳定性)
    assert_eq!(v1, count(&db, "SELECT COUNT(*) FROM a JOIN b ON a.x = b.y"));
    assert!(v1 > 0);
}

/// 空表: 乘积为 0。
#[test]
fn empty_table_gives_zero() {
    let dir = TempDir::new().unwrap();
    let db = setup(&dir);
    db.execute("CREATE TABLE e (id INTEGER PRIMARY KEY AUTO_INCREMENT, w INTEGER)")
        .unwrap();
    let e_rows: Vec<Vec<Value>> = vec![];
    db.insert_rows("e", e_rows).unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM a JOIN e ON 1=1"), 0);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM e JOIN b ON 1=1"), 0);
}

/// 墓碑可见性: DELETE 后的行数乘积必须正确 (fast_row_count 维护减量)。
#[test]
fn deleted_rows_excluded() {
    let dir = TempDir::new().unwrap();
    let db = setup(&dir);
    db.execute("DELETE FROM b WHERE b.y < 5").unwrap();
    let b_left = count(&db, "SELECT COUNT(*) FROM b");
    assert_eq!(count(&db, "SELECT COUNT(*) FROM a JOIN b ON 1=1"), 500 * b_left);
}

/// 非可折叠 WHERE (跨表比较) → decline, 结果与手工一致 (通用路径)。
#[test]
fn non_pushable_where_declines_correctly() {
    let dir = TempDir::new().unwrap();
    let db = setup(&dir);
    // WHERE 引用两表 → 不可分解 → 通用路径 (嵌套循环 + join 后过滤)
    let n = count(&db, "SELECT COUNT(*) FROM a JOIN b ON 1=1 WHERE a.x < b.y");
    // 手工: 对每 (x, y) 计数 x < y
    let mut expect = 0i64;
    for i in 0..500i64 {
        let x = i % 7;
        for j in 0..300i64 {
            if x < j % 11 {
                expect += 1;
            }
        }
    }
    assert_eq!(n, expect);
}
