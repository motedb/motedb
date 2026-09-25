//! VEC M4 集成回归：批投影（try_vec_projection）与批过滤 top-k
//! （try_vec_filter_topk）直接调用（绕过 MOTE_VEC 环境门），与完整 SQL
//! 执行器输出（旧路径权威语义）逐行对拍，覆盖 NULL/分页/多段去重/三值。
use motedb::sql::ast::SelectStmt;
use motedb::sql::{Lexer, Parser, QueryExecutor};
use motedb::types::Value;
use motedb::MoteDB;
use std::sync::Arc;

fn parse(sql: &str) -> SelectStmt {
    match Parser::new(Lexer::new(sql).tokenize().unwrap())
        .parse()
        .unwrap()
    {
        motedb::sql::ast::Statement::Select { stmt, .. } => stmt,
        _ => panic!("expected select statement"),
    }
}

/// 进程内先于任何 vec_enabled() 调用置 MOTE_VEC=on（OnceLock 首调即锁存；
/// 本测试二进制只含 vec-M4 用例，所有测试都先经过此守卫）。
static ENABLE_VEC: std::sync::Once = std::sync::Once::new();
fn enable_vec() {
    ENABLE_VEC.call_once(|| {
        std::env::set_var("MOTE_VEC", "on");
    });
}

fn ex(db: &Arc<MoteDB>, sql: &str) -> motedb::QueryResult {
    let stmt = Parser::new(Lexer::new(sql).tokenize().unwrap())
        .parse()
        .unwrap();
    QueryExecutor::new(db.clone()).execute(stmt).unwrap()
}

fn sql_rows(db: &Arc<MoteDB>, sql: &str) -> Vec<Vec<Value>> {
    match ex(db, sql) {
        motedb::QueryResult::Select { rows, .. } => rows,
        o => panic!("expected select: {:?}", o),
    }
}

/// 建表：500 行，周期 NULL（val/qty/ts），8 个设备，checkpoint 成段。
fn seed(db: &Arc<MoteDB>) {
    ex(
        &db,
        "CREATE TABLE t (id INT PRIMARY KEY, ts TIMESTAMP, dev TEXT, val REAL, qty INT)",
    );
    for i in 1..=500i64 {
        let (v, q) = if i % 9 == 0 {
            ("NULL".into(), "NULL".into())
        } else {
            (
                format!("{:.3}", i as f64 * 1.37 - 350.0),
                (i % 13 - 6).to_string(),
            )
        };
        let ts = if i % 17 == 0 {
            "NULL".to_string()
        } else {
            (1_700_000_000_000_000 + i * 777).to_string()
        };
        ex(
            &db,
            &format!(
                "INSERT INTO t VALUES ({}, {}, 'dev-{:02}', {}, {})",
                i,
                ts,
                i % 8,
                v,
                q
            ),
        );
    }
    db.checkpoint().unwrap();
}

fn vec_projection(db: &Arc<MoteDB>, sql: &str) -> Vec<Vec<Value>> {
    enable_vec();
    let stmt = parse(sql);
    let store = db.get_col_segment_store("t").expect("store");
    let schema = db.get_table_schema("t").unwrap();
    motedb::sql::vector_exec::try_vec_projection(
        &store,
        &schema,
        &stmt.columns,
        stmt.where_clause.as_ref(),
        stmt.limit,
        stmt.offset.unwrap_or(0),
    )
    .unwrap()
    .expect("vec projection should handle this shape")
}

fn vec_filter_topk(db: &Arc<MoteDB>, sql: &str) -> Vec<Vec<Value>> {
    enable_vec();
    let stmt = parse(sql);
    let store = db.get_col_segment_store("t").expect("store");
    let schema = db.get_table_schema("t").unwrap();
    motedb::sql::vector_exec::try_vec_filter_topk(&store, &schema, &stmt)
        .unwrap()
        .expect("vec filter topk should handle this shape")
}

/// 批投影 == SQL（含 NULL/全列/过滤/LIMIT/OFFSET）。行序也一致
/// （分页语义依赖顺序 — 与 scan_projected_filtered 复刻）。
#[test]
fn vec_projection_matches_sql() {
    enable_vec();
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("m4a.mote")).unwrap());
    seed(&db);

    for q in [
        "SELECT id, ts, dev, val, qty FROM t",
        "SELECT id, val FROM t",
        "SELECT dev, qty FROM t WHERE qty > 0",
        "SELECT id, ts FROM t WHERE val BETWEEN -100 AND 100 AND dev = 'dev-03'",
        "SELECT id FROM t LIMIT 7",
        "SELECT id FROM t LIMIT 7 OFFSET 100",
        "SELECT id, dev FROM t WHERE dev = 'dev-05' OR qty IS NULL",
        "SELECT id FROM t WHERE NOT (val > 0)",
    ] {
        let expect = sql_rows(&db, q);
        let got = vec_projection(&db, q);
        assert_eq!(expect.len(), got.len(), "row count: {q}");
        for (i, (a, b)) in expect.iter().zip(got.iter()).enumerate() {
            assert_eq!(a, b, "{q} row {i}");
        }
    }
}

/// Timestamp 列必须还原 Value::Timestamp（非 Integer）。
#[test]
fn vec_projection_timestamp_type() {
    enable_vec();
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("m4ts.mote")).unwrap());
    seed(&db);
    let got = vec_projection(&db, "SELECT ts FROM t WHERE id = 1");
    assert!(matches!(got[0][0], Value::Timestamp(_)), "{:?}", got);
}

/// 多段 + UPDATE/DELETE 后的 newest-wins：批投影与 SQL 一致
/// （该场景批路径 decline 墓碑 → 返回 None；SQL 旧路径权威）。
#[test]
fn vec_projection_declines_on_tombstones() {
    enable_vec();
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("m4tomb.mote")).unwrap());
    seed(&db);
    // 🔑 不 checkpoint：UPDATE/DELETE 的墓碑进 write buffer，
    // try_vec_projection 内部 flush_buffer 刷出后段里带墓碑 → decline
    // （checkpoint 会合并清掉墓碑，decline 前提就不成立了）。
    ex(&db, "UPDATE t SET val = 999.0 WHERE id <= 50");
    ex(&db, "DELETE FROM t WHERE id > 450");
    let stmt = parse("SELECT id, val FROM t");
    let store = db.get_col_segment_store("t").expect("store");
    let schema = db.get_table_schema("t").unwrap();
    let out = motedb::sql::vector_exec::try_vec_projection(
        &store,
        &schema,
        &stmt.columns,
        stmt.where_clause.as_ref(),
        stmt.limit,
        stmt.offset.unwrap_or(0),
    )
    .unwrap();
    assert!(out.is_none(), "墓碑存在时必须 decline 走旧路径");
}

/// 批过滤 top-k == SQL（ORDER BY 数值/Timestamp 键 + LIMIT/OFFSET + WHERE）。
#[test]
fn vec_filter_topk_matches_sql() {
    enable_vec();
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("m4b.mote")).unwrap());
    seed(&db);

    for q in [
        "SELECT id, ts FROM t WHERE qty > 0 ORDER BY ts DESC LIMIT 10",
        "SELECT id, ts FROM t WHERE ts >= 1700000000000777 ORDER BY ts ASC LIMIT 25",
        "SELECT id, val FROM t WHERE val > -100.0 AND val < 100.0 ORDER BY val DESC LIMIT 40 OFFSET 10",
        "SELECT id, qty FROM t WHERE dev = 'dev-02' ORDER BY qty DESC LIMIT 15",
        // NULL 键排序：NULLs first ASC / last DESC（引擎级语义）
        "SELECT id, ts FROM t WHERE id <= 100 ORDER BY ts ASC LIMIT 12",
        "SELECT id, ts FROM t WHERE id <= 100 ORDER BY ts DESC LIMIT 12",
        "SELECT id, val FROM t WHERE id <= 200 ORDER BY val ASC LIMIT 20 OFFSET 5",
        // 深分页
        "SELECT id, ts, dev FROM t WHERE qty IS NOT NULL ORDER BY id ASC LIMIT 30 OFFSET 400",
    ] {
        let expect = sql_rows(&db, q);
        let got = vec_filter_topk(&db, q);
        assert_eq!(expect.len(), got.len(), "row count: {q}");
        for (i, (a, b)) in expect.iter().zip(got.iter()).enumerate() {
            assert_eq!(a, b, "{q} row {i}");
        }
    }
}
