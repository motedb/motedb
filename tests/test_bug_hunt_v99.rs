//! Bug Hunt v99 — 外部生产测评反馈 (P0): 参数化主键点查 `SELECT v FROM t
//! WHERE id = ?` 走 execute_prepared 快路径时，返回的 columns 是**全表列名**
//! (`['id','v','s']`) 而数据只有投影值 (`[7]`) —— 列名与数据静默错位，
//! Python 层 zip 后变成 `{'id': 7}`（应为 `{'v': 7}`）。
//!
//! 根因: FastPkMeta 只缓存了 select_col_positions（投影下标），execute_
//! fast_pk_with_meta 三个 SELECT 返回点都回填了全表 column_names。
//! 修复: 缓存投影输出列名 select_col_names，非 `SELECT *` 时返回它；
//! 同时 detect_fast_pk_pattern 严格校验 —— 表达式/聚合/混入 `*`/未知列/
//! GROUP BY/HAVING/OFFSET>0/LIMIT 0/LIMIT ? 一律拒绝快路径回退全路径
//! （旧 filter_map 会静默丢列，`SELECT COUNT(*) ... WHERE id = ?` 曾返回
//! 整行错标数据）。
//!
//! 本文件覆盖: execute / query / fetch_arrays / query_arrow 四个消费面
//! 共用的 StreamingQueryResult —— Rust 层校验 columns+rows 的对应关系，
//! 并与字面量查询（走 try_col_segment_pk_point_query 另一条路径）对拍。

use motedb::sql::QueryResult;
use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    (db, dir)
}

/// 与测评报告完全一致的最小复现: 表 (id, v, s), 查 v。
#[test]
fn test_pk_param_partial_projection_labels() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 7, 'hello')").unwrap();

    // 首次调用（detect + 立即执行）
    let r = db
        .execute_prepared("SELECT v FROM t WHERE id = ?", vec![Value::Integer(1)])
        .unwrap()
        .materialize()
        .unwrap();
    match &r {
        QueryResult::Select { columns, rows } => {
            assert_eq!(columns, &vec!["v".to_string()], "首次调用: 列名应为投影列");
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0],
                vec![Value::Integer(7)],
                "首次调用: 数据应为 v 的值"
            );
        }
        other => panic!("expected Select, got {:?}", other),
    }

    // 二次调用（stmt_cache 命中 cached fast_pk —— 修复前该路径同样错标）
    let r2 = db
        .execute_prepared("SELECT v FROM t WHERE id = ?", vec![Value::Integer(1)])
        .unwrap()
        .materialize()
        .unwrap();
    match &r2 {
        QueryResult::Select { columns, rows } => {
            assert_eq!(columns, &vec!["v".to_string()], "缓存命中: 列名应为投影列");
            assert_eq!(rows[0], vec![Value::Integer(7)]);
        }
        other => panic!("expected Select, got {:?}", other),
    }
}

/// 多列投影、乱序投影、别名、限定名 —— 列名必须与 SELECT 列表一一对应。
#[test]
fn test_pk_param_projection_variants() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT, s TEXT, w FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (2, 7, 'hello', 1.5)")
        .unwrap();

    // 多列 + 乱序
    let r = db
        .execute_prepared("SELECT s, v FROM t WHERE id = ?", vec![Value::Integer(2)])
        .unwrap()
        .materialize()
        .unwrap();
    match &r {
        QueryResult::Select { columns, rows } => {
            assert_eq!(columns, &vec!["s".to_string(), "v".to_string()]);
            assert_eq!(
                rows[0],
                vec![Value::Text("hello".into()), Value::Integer(7)]
            );
        }
        other => panic!("expected Select, got {:?}", other),
    }

    // 别名: 输出名是 alias（与通用路径 build_select_columns 一致）
    let r = db
        .execute_prepared(
            "SELECT v AS val FROM t WHERE id = ?",
            vec![Value::Integer(2)],
        )
        .unwrap()
        .materialize()
        .unwrap();
    match &r {
        QueryResult::Select { columns, rows } => {
            assert_eq!(columns, &vec!["val".to_string()]);
            assert_eq!(rows[0], vec![Value::Integer(7)]);
        }
        other => panic!("expected Select, got {:?}", other),
    }

    // 限定名 t.v
    let r = db
        .execute_prepared("SELECT t.v FROM t WHERE id = ?", vec![Value::Integer(2)])
        .unwrap()
        .materialize()
        .unwrap();
    match &r {
        QueryResult::Select { columns, rows } => {
            assert_eq!(columns, &vec!["t.v".to_string()]);
            assert_eq!(rows[0], vec![Value::Integer(7)]);
        }
        other => panic!("expected Select, got {:?}", other),
    }

    // SELECT * 不受影响: 列名仍是全表
    let r = db
        .execute_prepared("SELECT * FROM t WHERE id = ?", vec![Value::Integer(2)])
        .unwrap()
        .materialize()
        .unwrap();
    match &r {
        QueryResult::Select { columns, rows } => {
            assert_eq!(
                columns,
                &vec![
                    "id".to_string(),
                    "v".to_string(),
                    "s".to_string(),
                    "w".to_string()
                ]
            );
            assert_eq!(rows[0].len(), 4);
        }
        other => panic!("expected Select, got {:?}", other),
    }
}

/// 参数化 vs 字面量对拍 —— 两条不同快路径必须给出一致的 (columns, rows)。
#[test]
fn test_pk_param_matches_literal() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 7, 'a'), (2, 8, 'b'), (3, 9, 'c')")
        .unwrap();

    for id in [1i64, 2, 3] {
        let param = db
            .execute_prepared("SELECT v, s FROM t WHERE id = ?", vec![Value::Integer(id)])
            .unwrap()
            .materialize()
            .unwrap();
        let literal = db
            .execute(&format!("SELECT v, s FROM t WHERE id = {}", id))
            .unwrap()
            .materialize()
            .unwrap();
        match (&param, &literal) {
            (
                QueryResult::Select {
                    columns: pc,
                    rows: pr,
                },
                QueryResult::Select {
                    columns: lc,
                    rows: lr,
                },
            ) => {
                assert_eq!(pc, lc, "id={}: 参数化与字面量列名不一致", id);
                assert_eq!(pr, lr, "id={}: 参数化与字面量数据不一致", id);
            }
            _ => panic!("both should be Select"),
        }
    }
}

/// 不存在的行: 空 rows，但 columns 仍必须是投影列名（fetch_arrays 依赖
/// columns 建键）。
#[test]
fn test_pk_param_absent_row_columns() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 7, 'a')").unwrap();

    let r = db
        .execute_prepared("SELECT v FROM t WHERE id = ?", vec![Value::Integer(999)])
        .unwrap()
        .materialize()
        .unwrap();
    match &r {
        QueryResult::Select { columns, rows } => {
            assert_eq!(columns, &vec!["v".to_string()]);
            assert!(rows.is_empty());
        }
        other => panic!("expected Select, got {:?}", other),
    }

    // 🔑 AUTO_INCREMENT 表以 NULL 参数点查: Absent 分支必须返回空 SELECT
    // （修复前返回 Modification, Python query() 直接抛
    // "expects a SELECT statement"）。SQL 三值逻辑 id = NULL → 无行。
    db.execute("CREATE TABLE auto_t(id INTEGER PRIMARY KEY AUTO_INCREMENT, v INT)")
        .unwrap();
    db.execute("INSERT INTO auto_t(v) VALUES (42)").unwrap();
    let r = db
        .execute_prepared("SELECT v FROM auto_t WHERE id = ?", vec![Value::Null])
        .unwrap()
        .materialize()
        .unwrap();
    match &r {
        QueryResult::Select { columns, rows } => {
            assert_eq!(columns, &vec!["v".to_string()]);
            assert!(rows.is_empty(), "id = NULL 应返回 0 行");
        }
        other => panic!("expected Select, got {:?}", other),
    }
}

/// 事务内读己之写（txn_lookup_row 路径）同样不能错标。
#[test]
fn test_pk_param_in_txn_projection() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT, s TEXT)")
        .unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (5, 123, 'x')").unwrap();
    let r = db
        .execute_prepared("SELECT v FROM t WHERE id = ?", vec![Value::Integer(5)])
        .unwrap()
        .materialize()
        .unwrap();
    match &r {
        QueryResult::Select { columns, rows } => {
            assert_eq!(columns, &vec!["v".to_string()]);
            assert_eq!(rows, &vec![vec![Value::Integer(123)]]);
        }
        other => panic!("expected Select, got {:?}", other),
    }
    db.execute("COMMIT").unwrap();
}

/// 快路径曾静默丢列的形状 —— 现在必须回退全路径并给出正确结果:
/// 聚合、表达式、混入 *、GROUP BY、OFFSET、LIMIT 0。
#[test]
fn test_pk_param_shapes_defer_to_full_path() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 7, 'a'), (2, 8, 'b')")
        .unwrap();

    // 聚合: 单行单列 count（修复前返回整行 + 全表列名）
    let r = db
        .execute_prepared(
            "SELECT COUNT(*) FROM t WHERE id = ?",
            vec![Value::Integer(1)],
        )
        .unwrap()
        .materialize()
        .unwrap();
    match &r {
        QueryResult::Select { columns, rows } => {
            assert_eq!(rows.len(), 1, "COUNT(*) 应返回一行");
            assert_eq!(rows[0].len(), 1, "COUNT(*) 应返回一列, got {:?}", rows[0]);
            assert_eq!(rows[0][0], Value::Integer(1));
            assert_eq!(columns.len(), 1, "COUNT(*) 列名数应为 1");
        }
        other => panic!("expected Select, got {:?}", other),
    }

    // 表达式
    let r = db
        .execute_prepared("SELECT v + 1 FROM t WHERE id = ?", vec![Value::Integer(1)])
        .unwrap()
        .materialize()
        .unwrap();
    match &r {
        QueryResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].len(), 1);
            assert_eq!(rows[0][0], Value::Integer(8));
        }
        other => panic!("expected Select, got {:?}", other),
    }

    // OFFSET 单行结果应整体跳过 → 0 行
    let r = db
        .execute_prepared(
            "SELECT v FROM t WHERE id = ? OFFSET 1",
            vec![Value::Integer(1)],
        )
        .unwrap()
        .materialize()
        .unwrap();
    match &r {
        QueryResult::Select { rows, .. } => assert!(rows.is_empty(), "OFFSET 1 应跳过唯一行"),
        other => panic!("expected Select, got {:?}", other),
    }

    // LIMIT 0 → 0 行
    let r = db
        .execute_prepared(
            "SELECT v FROM t WHERE id = ? LIMIT 0",
            vec![Value::Integer(1)],
        )
        .unwrap()
        .materialize()
        .unwrap();
    match &r {
        QueryResult::Select { rows, .. } => assert!(rows.is_empty(), "LIMIT 0 应返回 0 行"),
        other => panic!("expected Select, got {:?}", other),
    }

    // 混入 * : SELECT *, v — 回退全路径（不再走快路径丢列），并与字面量
    // 路径对拍确认形状一致（通用路径对 * 展开后重复列有去重，以对拍为准）。
    let param = db
        .execute_prepared("SELECT *, v FROM t WHERE id = ?", vec![Value::Integer(1)])
        .unwrap()
        .materialize()
        .unwrap();
    let literal = db
        .execute("SELECT *, v FROM t WHERE id = 1")
        .unwrap()
        .materialize()
        .unwrap();
    match (&param, &literal) {
        (
            QueryResult::Select {
                columns: pc,
                rows: pr,
            },
            QueryResult::Select {
                columns: lc,
                rows: lr,
            },
        ) => {
            assert_eq!(pr.len(), 1, "点查应命中一行");
            assert!(!pr[0].is_empty(), "SELECT *, v 不应丢成空投影");
            assert_eq!(pc, lc, "参数化与字面量列名应一致");
            assert_eq!(pr, lr, "参数化与字面量数据应一致");
        }
        _ => panic!("both should be Select"),
    }
}

/// 非 INT 主键（走 pk_lookup 缓存）同样校验投影列名。
#[test]
fn test_pk_param_text_pk_projection() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(name TEXT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES ('alice', 7)").unwrap();
    // 先让 pk_lookup 缓存命中（首查走全路径建立缓存）
    db.execute("SELECT v FROM t WHERE name = 'alice'").unwrap();

    let r = db
        .execute_prepared(
            "SELECT v FROM t WHERE name = ?",
            vec![Value::Text("alice".into())],
        )
        .unwrap()
        .materialize()
        .unwrap();
    match &r {
        QueryResult::Select { columns, rows } => {
            assert_eq!(columns, &vec!["v".to_string()]);
            assert_eq!(rows, &vec![vec![Value::Integer(7)]]);
        }
        other => panic!("expected Select, got {:?}", other),
    }
}

/// checkpoint + 重开后快路径（缓存重建）仍正确。
#[test]
fn test_pk_param_projection_after_reopen() {
    let (db, dir) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 7, 'hello')").unwrap();
    db.checkpoint().unwrap();
    drop(db);

    let db = Database::open(dir.path()).unwrap();
    let r = db
        .execute_prepared("SELECT v FROM t WHERE id = ?", vec![Value::Integer(1)])
        .unwrap()
        .materialize()
        .unwrap();
    match &r {
        QueryResult::Select { columns, rows } => {
            assert_eq!(columns, &vec!["v".to_string()]);
            assert_eq!(rows, &vec![vec![Value::Integer(7)]]);
        }
        other => panic!("expected Select, got {:?}", other),
    }
}

/// 字面量列索引快路径(BUG #46 同类): WHERE 非主键索引列 = 字面量 时,
/// SELECT 列表里的未知列 / 限定名 / 别名曾被 filter_map 静默丢弃,
/// 返回列名与数据错位的结果。现在必须回退全路径:
/// 未知列 → 报错;限定名/别名 → 全路径正确处理。
#[test]
fn test_literal_index_filter_select_list_strict() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)")
        .unwrap();
    db.execute("CREATE INDEX idx_cat ON t(cat)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30)")
        .unwrap();

    // 未知列: 全路径应报错,而不是返回空投影的错位行
    let r = db.execute("SELECT nosuch FROM t WHERE cat = 'a'");
    assert!(r.is_err(), "未知列必须报错");

    // 限定名 t.v: 全路径正确解析
    let r = db
        .execute("SELECT t.v FROM t WHERE cat = 'a'")
        .unwrap()
        .materialize()
        .unwrap();
    match &r {
        QueryResult::Select { columns, rows } => {
            let mut vals: Vec<i64> = rows
                .iter()
                .filter_map(|row| match row.first() {
                    Some(Value::Integer(i)) => Some(*i),
                    _ => None,
                })
                .collect();
            vals.sort();
            assert_eq!(vals, vec![10, 20], "限定名查询数据: {:?}", rows);
            assert_eq!(columns.len(), 1, "限定名查询列数: {:?}", columns);
        }
        other => panic!("expected Select, got {:?}", other),
    }

    // 别名: 全路径正确解析
    let r = db
        .execute("SELECT v AS val FROM t WHERE cat = 'a'")
        .unwrap()
        .materialize()
        .unwrap();
    match &r {
        QueryResult::Select { columns, rows } => {
            assert_eq!(columns, &vec!["val".to_string()]);
            assert_eq!(rows.len(), 2);
        }
        other => panic!("expected Select, got {:?}", other),
    }

    // 正常裸列仍走快路径且正确
    let r = db
        .execute("SELECT v FROM t WHERE cat = 'a'")
        .unwrap()
        .materialize()
        .unwrap();
    match &r {
        QueryResult::Select { columns, rows } => {
            assert_eq!(columns, &vec!["v".to_string()]);
            let mut vals: Vec<i64> = rows
                .iter()
                .filter_map(|row| match row.first() {
                    Some(Value::Integer(i)) => Some(*i),
                    _ => None,
                })
                .collect();
            vals.sort();
            assert_eq!(vals, vec![10, 20]);
        }
        other => panic!("expected Select, got {:?}", other),
    }

    // 同样校验 PK 字面量路径: 未知列报错
    let r = db.execute("SELECT nosuch FROM t WHERE id = 1");
    assert!(r.is_err(), "PK 路径未知列必须报错");
}
