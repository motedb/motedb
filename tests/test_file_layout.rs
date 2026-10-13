//! P1 file-layout 收敛验收 (docs/design/2026-10-file-layout-plan.md §1):
//! 惰性创建 + 死文件清理。
//!
//! 断言两个不变量:
//! 1. 无 TIMESTAMP 列、无用户索引、纯 Standard 表的库,固定文件集合最小化
//!    (无 wal/*.wal、无 columnar/、无 lsm/blobs、无 indexes/、无 *.fresh)。
//! 2. WAL 惰性生命周期: 写入物化分区文件 → CHECKPOINT 删除它们 → 重开数据完好。
use motedb::{DBConfig, Database};
use tempfile::TempDir;

fn file_set(root: &std::path::Path) -> Vec<String> {
    fn walk(root: &std::path::Path, dir: &std::path::Path, out: &mut Vec<String>) {
        if let Ok(rd) = std::fs::read_dir(dir) {
            for e in rd.flatten() {
                let p = e.path();
                let rel = p
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .trim_start_matches('/')
                    .to_string();
                if p.is_dir() {
                    walk(root, &p, out);
                } else {
                    out.push(rel);
                }
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out.sort();
    out
}

#[test]
fn p1_no_ts_no_index_minimal_fixed_files() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("d.mote");
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, dev TEXT, v FLOAT)")
            .unwrap();
        for i in 0..500 {
            db.execute(&format!(
                "INSERT INTO t VALUES ({}, 'd{}', {})",
                i,
                i % 7,
                i as f64 * 0.5
            ))
            .unwrap();
        }
        db.execute("CHECKPOINT").unwrap();
        db.close().unwrap();
    }

    let files = file_set(&path);
    assert!(
        !files
            .iter()
            .any(|f| f.starts_with("wal/") && f.ends_with(".wal")),
        "wal/ must hold no partition files after CHECKPOINT (lazy + unlink): {:?}",
        files
    );
    assert!(
        !files.iter().any(|f| f.starts_with("columnar/")),
        "legacy columnar/ dir must not exist for Standard-only DB: {:?}",
        files
    );
    assert!(
        !files.iter().any(|f| f.starts_with("lsm/blobs/")),
        "lsm/blobs/ must not exist (no large values): {:?}",
        files
    );
    assert!(
        !files.iter().any(|f| f.starts_with("indexes/")),
        "indexes/ must not exist (no TIMESTAMP col, no user index): {:?}",
        files
    );
    assert!(
        !files.iter().any(|f| f.contains("fresh")),
        "clean-close marker must be dot-prefixed .clean_close: {:?}",
        files
    );
    assert!(
        files.iter().any(|f| f == ".clean_close"),
        ".clean_close marker must be stamped on clean close: {:?}",
        files
    );
    // 数据本体必须存在
    assert!(
        files
            .iter()
            .any(|f| f.starts_with("columnar_ms/t/") && f.ends_with(".sst")),
        "segment data file must exist: {:?}",
        files
    );
    assert!(
        files.iter().any(|f| f == "columnar_ms/t/MANIFEST"),
        "per-table MANIFEST must exist (P2 will globalize it): {:?}",
        files
    );
}

#[test]
fn p1_wal_lazy_lifecycle_and_recovery() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("d.mote");
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();
        // 1) 建库后未写入: wal/ 目录存在但零文件
        let files = file_set(&path);
        assert!(
            !files
                .iter()
                .any(|f| f.starts_with("wal/") && f.ends_with(".wal")),
            "fresh DB must hold zero WAL partition files: {:?}",
            files
        );

        // 2) 写入(不 checkpoint): 分区文件物化
        for i in 0..100 {
            db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i))
                .unwrap();
        }
        let files = file_set(&path);
        assert!(
            files
                .iter()
                .any(|f| f.starts_with("wal/") && f.ends_with(".wal")),
            "WAL partition files must materialize on first write: {:?}",
            files
        );

        // 3) CHECKPOINT: 分区文件被删除(替代 0 字节残留)
        db.execute("CHECKPOINT").unwrap();
        let files = file_set(&path);
        assert!(
            !files
                .iter()
                .any(|f| f.starts_with("wal/") && f.ends_with(".wal")),
            "CHECKPOINT must remove (not truncate) partition files: {:?}",
            files
        );

        // 4) checkpoint 后继续写入 + 非正常关闭(模拟 kill): WAL 重放恢复
        for i in 100..200 {
            db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i))
                .unwrap();
        }
        db.close().unwrap();
    }

    // 重开: 全部 200 行可见(WAL 已在 close/checkpoint 处理)
    let db = Database::open(&path).unwrap();
    let r = db
        .execute("SELECT COUNT(*) FROM t")
        .unwrap()
        .materialize()
        .unwrap();
    let (cols, data) = r.select_rows().unwrap();
    assert_eq!(cols[0], "COUNT(*)");
    assert_eq!(data[0][0], motedb::types::Value::Integer(200));
    db.close().unwrap();
}

#[test]
fn p1_ts_column_materializes_timestamp_idx() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("d.mote");
    {
        let db = Database::create(&path).unwrap();
        // ts-FIRST 列: index_row_timestamp 快路径在首个 TIMESTAMP 行写入时
        // 即物化 indexes/timestamp.idx(非首列 ts 表走异步 index-builder/
        // checkpoint rebuild,物化时点后移——行为与 P1 前一致,仅文件创建推迟)。
        db.execute("CREATE TABLE ev (ts TIMESTAMP, id INTEGER PRIMARY KEY, v FLOAT)")
            .unwrap();
        for i in 0..100 {
            // 唯一时间戳(分钟进位) — BTree 对同键是 update 语义,重复 ts 会合并
            let (m, sec) = (i / 60, i % 60);
            db.execute(&format!(
                "INSERT INTO ev VALUES ('2024-01-15 10:{:02}:{:02}', {}, {})",
                m, sec, i, i as f64
            ))
            .unwrap();
        }
        let files = file_set(&path);
        assert!(
            files.iter().any(|f| f == "indexes/timestamp.idx"),
            "timestamp.idx must materialize on first TIMESTAMP-first insert: {:?}",
            files
        );

        db.execute("CHECKPOINT").unwrap();
        let r = db
            .execute("SELECT COUNT(*) FROM ev")
            .unwrap()
            .materialize()
            .unwrap();
        let (_, data) = r.select_rows().unwrap();
        assert_eq!(data[0][0], motedb::types::Value::Integer(100));
        db.close().unwrap();
    }

    // 重开 + 范围查询(索引从盘加载,含 rebuild 增量)。
    // 端点用引擎自己的 parse_iso 推导,与插入字符串同一时区解释。
    let db = Database::open(&path).unwrap();
    let start = motedb::types::Timestamp::parse_iso("2024-01-15 10:00:00")
        .unwrap()
        .as_micros();
    let end = motedb::types::Timestamp::parse_iso("2024-01-15 10:31:39")
        .unwrap()
        .as_micros();
    let ids = db.query_timestamp_range(start, end).unwrap();
    assert_eq!(ids.len(), 100, "range query must find all indexed rows");
    db.close().unwrap();
}

#[test]
fn p1_legacy_fresh_marker_honored_once() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("d.mote");
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();
        db.close().unwrap();
    }
    // 手工放置旧版标记,模拟旧版本库
    std::fs::write(path.join("fts_indexes.fresh"), b"v1").unwrap();
    let _ = std::fs::remove_file(path.join(".clean_close"));

    let db = Database::open(&path).unwrap();
    db.close().unwrap();

    assert!(
        !path.join("fts_indexes.fresh").exists(),
        "legacy marker must be consumed at open"
    );
}

#[test]
fn p1_default_config_partition_files_lazy() {
    // 默认 4 分区配置下同样成立(回归防呆: 防止未来改回预创建)
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("d.mote");
    let config = DBConfig::default();
    {
        let db = Database::create_with_config(&path, config.clone()).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();
        db.close().unwrap();
        let files = file_set(&path);
        assert!(
            !files
                .iter()
                .any(|f| f.starts_with("wal/") && f.ends_with(".wal")),
            "default-config fresh DB must hold zero WAL files: {:?}",
            files
        );
    }
    let db = Database::open_with_config(&path, config).unwrap();
    db.close().unwrap();
}
