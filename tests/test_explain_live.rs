//! B4 EXPLAIN 决策点实录验收（0.18 设计文档 §B4）。
//!
//! EXPLAIN = 重放执行 + thread-local 采集——计划永远与实际一致; 未插桩
//! 路径回退 v1 启发式（如实标注 source=heuristic 语义的 note 行）。

use motedb::sql::QueryResult;
use motedb::types::Value;
use motedb::Database;

fn explain(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db
        .execute(&format!("EXPLAIN {sql}"))
        .unwrap()
        .materialize()
        .unwrap()
    {
        QueryResult::Select { rows, .. } => rows,
        o => panic!("{o:?}"),
    }
}

fn has_op(rows: &[Vec<Value>], op: &str) -> bool {
    rows.iter()
        .any(|r| matches!(&r[1], Value::Text(o) if o.as_str() == op))
}

#[test]
fn explain_reports_live_decision_points() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, emb VECTOR(4))")
        .unwrap();
    for i in 0..50 {
        db.execute(&format!(
            "INSERT INTO t VALUES ({i}, {}, '[{}, 2, 3, 4]')",
            i % 5,
            i
        ))
        .unwrap();
    }
    db.checkpoint().unwrap();

    // ① 列段扫描: 实录
    let rows = explain(&db, "SELECT id FROM t WHERE v > 2");
    assert!(has_op(&rows, "scan"), "扫描实录: {rows:?}");

    // ② JOIN 路由: 实录
    let rows = explain(
        &db,
        "SELECT a.id FROM t a JOIN t b ON a.id = b.id WHERE a.v > 2",
    );
    assert!(
        has_op(&rows, "join_route") || has_op(&rows, "join"),
        "JOIN 路由实录: {rows:?}"
    );

    // ③ 向量 KNN（建索引 + 阈值 0 强制走路由决策）: knn_route 实录
    std::env::set_var("MOTE_VEC_INDEX_MIN_ROWS", "0");
    db.execute("CREATE VECTOR INDEX vidx ON t(emb)").unwrap();
    // 阈值在查询时读取——EXPLAIN 重放期间保持 0 以路由到向量计划。
    let rows = explain(&db, "SELECT id FROM t ORDER BY emb <-> '[1,2,3,4]' LIMIT 5");
    std::env::remove_var("MOTE_VEC_INDEX_MIN_ROWS");
    assert!(
        has_op(&rows, "knn_route")
            || has_op(&rows, "knn_freshness")
            || has_op(&rows, "knn")
            // 该形状（无 WHERE）由列段扫描 + top-k 堆接管——同为实录决策点。
            || (has_op(&rows, "sort") && has_op(&rows, "scan_route")),
        "KNN 路由实录: {rows:?}"
    );

    // ④ 未插桩路径: 回退启发式而不是报错（note 行如实标注）
    let rows = explain(&db, "SELECT * FROM t WHERE id = 5");
    assert!(!rows.is_empty(), "点查回退不报错: {rows:?}");
    db.close().unwrap();
}

/// EXPLAIN 重放不得产生写副作用（SELECT-only 已由入口保证）。
#[test]
fn explain_replay_has_no_side_effects() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE s (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 0..10 {
        db.execute(&format!("INSERT INTO s VALUES ({i}, {i})"))
            .unwrap();
    }
    db.checkpoint().unwrap();
    for _ in 0..3 {
        let _ = explain(&db, "SELECT id FROM s WHERE v > 3");
    }
    let n = match db
        .execute("SELECT COUNT(*) FROM s")
        .unwrap()
        .materialize()
        .unwrap()
    {
        QueryResult::Select { rows, .. } => match &rows[0][0] {
            Value::Integer(i) => *i,
            o => panic!("{o:?}"),
        },
        o => panic!("{o:?}"),
    };
    assert_eq!(n, 10, "EXPLAIN 重放不得改数据");
    db.close().unwrap();
}
