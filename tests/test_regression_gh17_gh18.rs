//! 🔴 回归: issue #17 / #18 —— 过滤+排序组合路径上的两个静默错误结果。
//!
//! #17: MATCH AGAINST + ORDER BY ts DESC → 静默升序（方向被丢）。
//! #18: 等值 WHERE + ORDER BY（无 LIMIT）→ TIMESTAMP 列投影为 NULL
//!      （同查询加 LIMIT 正常; 非 WHERE 或非 ORDER 均正常）。
//!
//! 两例均为 0.18 差分 harness 九路径未覆盖的组合（第 ⑩ 路径候选）。

use motedb::Database;

fn setup_fts() -> (Database, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE s (ts TIMESTAMP, obj TEXT, note TEXT)")
        .unwrap();
    db.execute("CREATE TEXT INDEX s_note ON s(note)").unwrap();
    for i in 0..5i64 {
        db.execute(&format!(
            "INSERT INTO s VALUES ({}, 'cup', '红色 杯子 第{i}次')",
            (1794300000 + i * 60) * 1_000_000
        ))
        .unwrap();
    }
    (db, dir)
}

fn ts_of(v: &motedb::types::Value) -> i64 {
    match v {
        motedb::types::Value::Timestamp(t) => t.as_micros(),
        motedb::types::Value::Integer(i) => *i,
        _ => panic!("ts column returned {v:?}, not a timestamp value"),
    }
}

/// issue #17: MATCH 过滤后 ORDER BY ts DESC 必须仍是降序。
#[test]
fn fts_match_order_by_ts_desc_is_descending() {
    let (db, _d) = setup_fts();

    let plain = db
        .query("SELECT ts FROM s ORDER BY ts DESC LIMIT 1")
        .unwrap();
    let latest_expected = ts_of(&plain[0][0]);
    assert_eq!(latest_expected, (1794300000 + 4 * 60) * 1_000_000);

    let fts = db
        .query("SELECT ts FROM s WHERE MATCH(note) AGAINST('红色') ORDER BY ts DESC LIMIT 1")
        .unwrap();
    assert_eq!(fts.len(), 1, "MATCH must not drop rows");
    assert_eq!(
        ts_of(&fts[0][0]),
        latest_expected,
        "MATCH + ORDER BY ts DESC must return the LATEST row, not the oldest (#17)"
    );

    // 多行形态: DESC 整列必须单调递减
    let all = db
        .query("SELECT ts FROM s WHERE MATCH(note) AGAINST('红色') ORDER BY ts DESC")
        .unwrap();
    assert_eq!(all.len(), 5);
    let vals: Vec<i64> = all.iter().map(|r| ts_of(&r[0])).collect();
    let mut sorted = vals.clone();
    sorted.sort_unstable_by(|a, b| b.cmp(a));
    assert_eq!(vals, sorted, "full MATCH + DESC result must be descending");
}

/// issue #18: 等值 WHERE + ORDER BY（无 LIMIT）不得把 TIMESTAMP 列投成 NULL。
#[test]
fn eq_where_order_by_projects_timestamp_not_null() {
    let (db, _d) = setup_fts();

    // 对照组: 无 ORDER / 带 LIMIT —— 均正常
    let ok1 = db.query("SELECT ts FROM s WHERE obj = 'cup'").unwrap();
    assert_eq!(ts_of(&ok1[0][0]), 1794300000 * 1_000_000);
    let ok2 = db
        .query("SELECT ts FROM s WHERE obj = 'cup' ORDER BY ts ASC LIMIT 100000")
        .unwrap();
    assert_eq!(ts_of(&ok2[0][0]), 1794300000 * 1_000_000);

    // 病灶: 等值 WHERE + ORDER BY（无 LIMIT）
    let broken = db
        .query("SELECT ts FROM s WHERE obj = 'cup' ORDER BY ts ASC")
        .unwrap();
    assert_eq!(broken.len(), 5, "row count is correct — only ts is NULLed");
    for (i, r) in broken.iter().enumerate() {
        assert_eq!(
            ts_of(&r[0]),
            (1794300000 + i as i64 * 60) * 1_000_000,
            "#18: row {i} ts must be the real timestamp, ascending"
        );
    }
}

/// #18 的另一面（ lesser inconsistency，顺带钉住）: 同一 TIMESTAMP 列在不同
/// 路径应返回同构值（Timestamp 变体），不该一会儿 Integer 一会儿 Timestamp。
#[test]
fn timestamp_projection_variant_is_consistent_across_paths() {
    let (db, _d) = setup_fts();
    let paths = [
        "SELECT ts FROM s",
        "SELECT ts FROM s ORDER BY ts ASC",
        "SELECT ts FROM s ORDER BY ts ASC LIMIT 3",
        "SELECT ts FROM s WHERE obj = 'cup'",
        "SELECT ts FROM s WHERE obj = 'cup' ORDER BY ts ASC",
        "SELECT ts FROM s WHERE obj = 'cup' ORDER BY ts ASC LIMIT 3",
        "SELECT ts, obj FROM s WHERE obj = 'cup' ORDER BY ts ASC LIMIT 3",
    ];
    let mut variants = std::collections::HashSet::new();
    for q in paths {
        let rows = db.query(q).unwrap();
        if let Some(r) = rows.first() {
            variants.insert(std::mem::discriminant(&r[0]));
        }
    }
    assert_eq!(
        variants.len(),
        1,
        "same TIMESTAMP column must return one Value variant across paths, got {variants:?}"
    );
}
