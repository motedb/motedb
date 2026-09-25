//! WAL-less 段阵的 auto-checkpoint 触发验证:
//! fast path / insert_arrays 加载不写 WAL (WAL 大小触发对它们盲) —
//! 段计数触发 (max_segment_count) 必须在持续加载下自动合并段阵。
use motedb::types::Value;
use motedb::{AutoCheckpointConfig, Database, DBConfig};
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn sst_count(dir: &TempDir, table: &str) -> usize {
    let p = dir.path().join("columnar_ms").join(table);
    match std::fs::read_dir(&p) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == "sst"))
            .count(),
        Err(_) => 0,
    }
}

#[test]
fn auto_checkpoint_merges_walless_segment_array() {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_testing();
    config.max_result_rows = None;
    // WAL 大小触发永不命中 (WAL-less); 段阈值 8; 检查间隔最小化。
    config.auto_checkpoint = Some(AutoCheckpointConfig {
        max_wal_size_bytes: u64::MAX,
        min_interval_secs: 1,
        max_segment_count: 8,
    });
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute("CREATE TABLE big (id INTEGER PRIMARY KEY AUTO_INCREMENT, tag TEXT, note TEXT)")
        .unwrap();
    let payload = "x".repeat(120_000);

    // 10 批 × 12MB = 段阵 > 8 阈值 (每批 4MB flush 阈值 → 一段/批)。
    let mut expect = 0i64;
    for b in 0..10 {
        let rows: Vec<Vec<Value>> = (0..100)
            .map(|i| {
                vec![
                    Value::Null,
                    Value::Text(format!("{}-{}", b, i).into()),
                    Value::Text(payload.clone().into()),
                ]
            })
            .collect();
        expect += 100;
        db.insert_rows("big", rows).unwrap();
    }
    // 不手动 checkpoint — 等后台线程 (检查周期 max(1,10)=10s) 自动触发。
    let deadline = Instant::now() + Duration::from_secs(35);
    let mut merged = false;
    while Instant::now() < deadline {
        let n = sst_count(&dir, "big");
        if n > 0 && n <= 8 {
            merged = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    // 数据完好
    let r = db
        .execute("SELECT COUNT(*) FROM big")
        .unwrap()
        .materialize()
        .unwrap();
    if let motedb::sql::QueryResult::Select { rows, .. } = r {
        assert_eq!(rows[0][0], Value::Integer(expect));
    }
    assert!(
        merged,
        "auto-checkpoint should merge the WAL-less segment array to <=8 segments (got {})",
        sst_count(&dir, "big")
    );
    // 合并后数据仍可查 (段合并正确性)
    let r = db
        .execute("SELECT note FROM big WHERE id = 500")
        .unwrap()
        .materialize()
        .unwrap();
    if let motedb::sql::QueryResult::Select { rows, .. } = r {
        assert_eq!(rows.len(), 1);
    }
    db.close().unwrap();
}
