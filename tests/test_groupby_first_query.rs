use motedb::types::Value;
use motedb::{Database, DBConfig};
use tempfile::TempDir;

// 回归: 多段表上 GROUP BY 是首个需要 keys 的查询时, col_segment_group_by
// 慢路径 dedup 曾用未加载的栅栏键 — 每 2048 行被当同 key, 100K 行只剩
// 50 行 32 组 (静默错果)。修复: need_dedup 时先 load_full_keys。

#[test]
fn groupby_first_query_multi_segment() {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_testing();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute(
        "CREATE TABLE s (id INTEGER PRIMARY KEY AUTO_INCREMENT, device TEXT, zone TEXT, val FLOAT, ts TIMESTAMP)",
    )
    .unwrap();
    for b in 0..2 {
        let rows: Vec<Vec<Value>> = (0..50_000)
            .map(|i| {
                vec![
                    Value::Null,
                    Value::Text(format!("dev-{:03}", (b * 50000 + i) % 64).into()),
                    Value::Text("north".into()),
                    Value::Float((i as f64) * 0.01),
                    Value::Integer(1700000000_000_000 + (b as i64) * 50_000 + i as i64),
                ]
            })
            .collect();
        db.insert_rows("s", rows).unwrap();
    }
    db.execute("CHECKPOINT").unwrap();
    // 第一个查询就是 GROUP BY (无前置 flush 查询)
    let r = db
        .execute("SELECT device, COUNT(*) FROM s GROUP BY device")
        .unwrap()
        .materialize()
        .unwrap();
    if let motedb::sql::QueryResult::Select { columns, rows } = r {
        let total: i64 = rows
            .iter()
            .map(|r| match &r[1] {
                Value::Integer(v) => *v,
                _ => 0,
            })
            .sum();
        println!("columns={:?} groups={} total={}", columns, rows.len(), total);
        for r in rows.iter().take(3) { println!("  row: {:?}", r); }
        let r2 = db.execute("SELECT id, device FROM s LIMIT 5").unwrap().materialize().unwrap();
        if let motedb::sql::QueryResult::Select { rows: rr, .. } = r2 {
            for r in rr.iter() { println!("  scan: {:?}", r); }
        }
        let r3 = db.execute("SELECT COUNT(*) FROM s WHERE device = 'dev-000'").unwrap().materialize().unwrap();
        println!("dev-000 count: {:?}", r3);
        assert_eq!(rows.len(), 64, "64 device groups");
        assert_eq!(total, 100_000, "all rows counted");
    }
}
