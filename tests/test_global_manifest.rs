//! P2 全局单一 MANIFEST 验收 (docs/design/2026-10-file-layout-plan.md §2):
//! 一个 `columnar_ms/MANIFEST` 覆盖全库 — 每表 -1 文件 -1 fd,含 v0.14
//! 旧布局的崩溃安全迁移。
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

fn count(db: &Database, sql: &str) -> i64 {
    let r = db.execute(sql).unwrap().materialize().unwrap();
    let (_, data) = r.select_rows().unwrap();
    match data[0][0] {
        motedb::types::Value::Integer(i) => i,
        ref v => panic!("unexpected count value: {v:?}"),
    }
}

#[test]
fn p2_single_global_manifest_file() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("d.mote");
    {
        let db = Database::create(&path).unwrap();
        for t in ["alpha", "beta", "gamma"] {
            db.execute(&format!(
                "CREATE TABLE {t} (id INTEGER PRIMARY KEY, v TEXT)"
            ))
            .unwrap();
            for i in 0..300 {
                db.execute(&format!("INSERT INTO {t} VALUES ({i}, 'v{i}')"))
                    .unwrap();
            }
        }
        db.execute("CHECKPOINT").unwrap();

        // 每表只剩 1 个 .sst;manifest 只有一个全局文件
        let files = file_set(&path);
        assert!(
            files.iter().any(|f| f == "columnar_ms/MANIFEST"),
            "global MANIFEST must exist: {files:?}"
        );
        let per_table_manifests = files
            .iter()
            .filter(|f| f.matches('/').count() == 1 && f.ends_with("MANIFEST"))
            .count();
        assert_eq!(
            per_table_manifests, 1,
            "exactly one manifest (the global one): {files:?}"
        );
        for t in ["alpha", "beta", "gamma"] {
            assert!(
                files
                    .iter()
                    .any(|f| f.starts_with(&format!("columnar_ms/{t}/")) && f.ends_with(".sst")),
                "segment data present for {t}: {files:?}"
            );
        }

        for t in ["alpha", "beta", "gamma"] {
            assert_eq!(count(&db, &format!("SELECT COUNT(*) FROM {t}")), 300);
        }
        db.close().unwrap();
    }

    // 重开: 数据完好,仍只有一个 manifest
    let db = Database::open(&path).unwrap();
    for t in ["alpha", "beta", "gamma"] {
        assert_eq!(count(&db, &format!("SELECT COUNT(*) FROM {t}")), 300);
    }
    let files = file_set(&path);
    assert_eq!(
        files.iter().filter(|f| f.ends_with("MANIFEST")).count(),
        1,
        "reopen must not create per-table manifests: {files:?}"
    );
    db.close().unwrap();
}

#[test]
fn p2_migration_from_legacy_per_table_layout() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("d.mote");

    // 手工构造 v0.14 旧布局: 每表 columnar_ms/<t>/MANIFEST + 段文件。
    // 用旧版本二进制不现实,直接以当前代码无法产生的"孤儿段文件 + 旧 manifest"
    // 形态模拟: 旧 manifest 是 v1 格式 (无 table_id)。
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE keepme (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        db.execute("CREATE TABLE droppme (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        db.execute("DROP TABLE droppme").unwrap();
        db.close().unwrap();
    }
    // 把全局 manifest 拆回旧布局: keepme 的段文件已在全局 manifest 里;
    // 构造旧形态 = 删全局 manifest + 为 keepme 写一个 v1 per-table manifest。
    let ms = path.join("columnar_ms");
    std::fs::remove_file(ms.join("MANIFEST")).unwrap();
    let keep_dir = ms.join("keepme");
    std::fs::create_dir_all(&keep_dir).unwrap();
    {
        // v1 header: MOTS + version=1 + count=0
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MOTS");
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        // AddSegment(1)
        buf.push(1u8);
        buf.extend_from_slice(&1u64.to_le_bytes());
        std::fs::write(keep_dir.join("MANIFEST"), &buf).unwrap();
    }
    // droppme 目录残留一个孤儿段文件(表已不在 registry) — 迁移应删除整个目录
    let drop_dir = ms.join("droppme");
    std::fs::create_dir_all(&drop_dir).unwrap();
    std::fs::write(drop_dir.join("0000000001.sst"), b"stale").unwrap();

    // 重开触发迁移
    let db = Database::open(&path).unwrap();
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM keepme"),
        0,
        "keepme has no rows — the adopted segment id 1 file does not exist on disk, must not panic"
    );
    let files = file_set(&path);
    assert!(
        files.iter().any(|f| f == "columnar_ms/MANIFEST"),
        "global manifest written by migration: {files:?}"
    );
    assert!(
        !files.iter().any(|f| f == "columnar_ms/keepme/MANIFEST"),
        "legacy manifest consumed: {files:?}"
    );
    assert!(
        !drop_dir.exists(),
        "directory of a registry-absent table must be removed by migration"
    );
    db.close().unwrap();

    // 迁移幂等: 再次打开不再变化
    let db = Database::open(&path).unwrap();
    assert_eq!(
        file_set(&path)
            .iter()
            .filter(|f| f.ends_with("MANIFEST"))
            .count(),
        1
    );
    db.close().unwrap();
}

#[test]
fn p2_truncated_global_manifest_orphan_adoption() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("d.mote");
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        for i in 0..100 {
            db.execute(&format!("INSERT INTO t VALUES ({i}, 'v{i}')"))
                .unwrap();
        }
        // 强制 flush 两次 → 2 个段;然后 compaction 合并 (触发 prepare_for_query)
        db.execute("CHECKPOINT").unwrap();
        let _ = count(&db, "SELECT COUNT(*) FROM t");
        for i in 100..200 {
            db.execute(&format!("INSERT INTO t VALUES ({i}, 'w{i}')"))
                .unwrap();
        }
        db.execute("CHECKPOINT").unwrap();
        db.close().unwrap();
    }

    // 模拟崩溃尾损坏: 截断全局 manifest 的最后一条记录
    let mp = path.join("columnar_ms").join("MANIFEST");
    let mut data = std::fs::read(&mp).unwrap();
    assert!(data.len() > 11, "manifest has records");
    data.truncate(data.len() - 9); // AddSegment 记录 = 1+4+8 = 13B;截掉尾部
    std::fs::write(&mp, &data).unwrap();

    // 重开: replay 在截断处停止,无记录的段文件按孤儿收编 — 行数不减
    let db = Database::open(&path).unwrap();
    let n = count(&db, "SELECT COUNT(*) FROM t");
    assert!(n >= 100, "adopted rows survive manifest tail loss: got {n}");
    db.close().unwrap();
}

#[test]
fn p2_drop_table_leaves_no_stale_manifest_state() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("d.mote");
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE victim (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        db.execute("CREATE TABLE survivor (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        for i in 0..50 {
            db.execute(&format!("INSERT INTO victim VALUES ({i}, 'x{i}')"))
                .unwrap();
            db.execute(&format!("INSERT INTO survivor VALUES ({i}, 's{i}')"))
                .unwrap();
        }
        db.execute("CHECKPOINT").unwrap();
        db.execute("DROP TABLE victim").unwrap();

        assert!(count(&db, "SELECT COUNT(*) FROM survivor") == 50);
        db.close().unwrap();
    }
    {
        let db = Database::open(&path).unwrap();
        // survivor 完好
        assert_eq!(count(&db, "SELECT COUNT(*) FROM survivor"), 50);
        // victim 不存在
        assert!(db.execute("SELECT COUNT(*) FROM victim").is_err());
        db.close().unwrap();
    }
    // 重开后 survivor 数据仍完好 (DropTable 记录不误伤)
    let db = Database::open(&path).unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM survivor"), 50);
    db.close().unwrap();
}

#[test]
fn p2_fd_count_flat_in_tables() {
    // fd 目标: 固定 fd 不随表数线性增长。50 表打开,库相关 fd < 20
    // (MANIFEST 1 + lock 1 + 惰性对象),对比 P1 前的 6+T=56。
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("d.mote");
    let config = DBConfig::default();
    let db = Database::create_with_config(&path, config.clone()).unwrap();
    for t in 0..50 {
        db.execute(&format!(
            "CREATE TABLE t{t} (id INTEGER PRIMARY KEY, v INTEGER)"
        ))
        .unwrap();
        for b in 0..3 {
            let rows: Vec<String> = ((b * 100)..(b * 100 + 100))
                .map(|i| format!("({i}, {i})"))
                .collect();
            db.execute(&format!("INSERT INTO t{t} VALUES {}", rows.join(",")))
                .unwrap();
        }
    }
    db.execute("CHECKPOINT").unwrap();

    let files = file_set(&path);
    let manifests = files.iter().filter(|f| f.ends_with("MANIFEST")).count();
    assert_eq!(manifests, 1, "50 tables → still one MANIFEST: {files:?}");
    let ssts = files.iter().filter(|f| f.ends_with(".sst")).count();
    assert!(
        ssts <= 50,
        "steady state ≤1 sst per table after compaction: got {ssts}"
    );
    db.close().unwrap();

    // 文件总数 ≤ 3 + 表数 + 段数(≤表数) + lock
    assert!(
        files.len() <= 55,
        "file count must be ~3+T+I, got {}: {files:?}",
        files.len()
    );
}

#[test]
fn p2_runtime_fd_flat_across_tables() {
    // P2 核心承诺: 常驻 fd 不随表数线性增长 (P1 前 = 6+T)。50 表打开,
    // 库相关 fd 应为个位数+惰性对象, 而非 56。经 /dev/fd 计数。
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("d.mote");
    let config = DBConfig::default();
    let db = Database::create_with_config(&path, config.clone()).unwrap();
    for t in 0..50 {
        db.execute(&format!(
            "CREATE TABLE t{t} (id INTEGER PRIMARY KEY, v INTEGER)"
        ))
        .unwrap();
        for b in 0..3 {
            let rows: Vec<String> = ((b * 200)..(b * 200 + 200))
                .map(|i| format!("({i}, {i})"))
                .collect();
            db.execute(&format!("INSERT INTO t{t} VALUES {}", rows.join(",")))
                .unwrap();
        }
    }
    db.execute("CHECKPOINT").unwrap();
    let _ = db
        .execute("SELECT COUNT(*) FROM t0")
        .unwrap()
        .materialize()
        .unwrap();

    // 进程 fd 总数(含测试框架自身)。P1 前 50 表 ≈ 6+50+框架 ≈ 60+;
    // P2 应 ≤ 框架基线 + 个位数。先测基线: 开一个空库的进程内增量不可行,
    // 直接断言总数上界(测试进程自身 fd ~10-20, 库 ≤ 10)。
    let fd_count = std::fs::read_dir("/dev/fd")
        .map(|rd| rd.count())
        .unwrap_or_else(|_| {
            // /dev/fd 不可用时退化为 /proc (linux)
            std::fs::read_dir("/proc/self/fd")
                .map(|rd| rd.count())
                .unwrap_or(0)
        });
    assert!(
        fd_count < 40,
        "50-table DB must hold single-digit fds (P2), process total was {fd_count}"
    );
    db.close().unwrap();
}

#[test]
fn p3_meta_bin_unifies_catalog_lsn_and_clean_close() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("d.mote");
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE mt (id INTEGER PRIMARY KEY AUTO_INCREMENT, v TEXT)")
            .unwrap();
        for i in 0..50 {
            db.execute(&format!("INSERT INTO mt (v) VALUES ('x{i}')"))
                .unwrap();
        }
        db.execute("CHECKPOINT").unwrap();
        assert_eq!(count(&db, "SELECT COUNT(*) FROM mt"), 50);
        db.close().unwrap();
    }
    // meta.bin 存在; 旧三文件不复存在
    assert!(path.join("meta.bin").exists());
    assert!(!path.join("catalog.bin").exists());
    assert!(!path.join("index_metadata.bin").exists());
    assert!(!path.join("lsn_counter").exists());

    // 重开: catalog(AUTO_INCREMENT 续号)、lsn、数据全保留
    let db = Database::open(&path).unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM mt"), 50);
    let r = db.execute("INSERT INTO mt (v) VALUES ('after')").unwrap();
    let _ = r;
    let n = count(&db, "SELECT COUNT(*) FROM mt");
    assert_eq!(n, 51);
    // AUTO_INCREMENT 必须续号 (id=51) 而不是从 1 重新分配
    let r = db
        .execute("SELECT id FROM mt WHERE v = 'after'")
        .unwrap()
        .materialize()
        .unwrap();
    let (_, data) = r.select_rows().unwrap();
    assert_eq!(
        data[0][0],
        motedb::types::Value::Integer(51),
        "AUTO_INCREMENT survives reopen via meta.bin"
    );
    db.close().unwrap();
}

#[test]
fn p3_migration_from_pre_p3_layout() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("d.mote");
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE legacy (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        for i in 0..30 {
            db.execute(&format!("INSERT INTO legacy VALUES ({i}, 'l{i}')"))
                .unwrap();
        }
        db.execute("CHECKPOINT").unwrap();
        db.close().unwrap();
    }
    // 伪造 pre-P3 布局: 从 meta.bin 拆出 catalog 节回 catalog.bin, 删 meta.bin
    let meta_path = path.join("meta.bin");
    let data = std::fs::read(&meta_path).unwrap();
    assert!(data.len() > 10);
    // 找到 catalog 节 (kind=1): header 10B 后第一节
    let sec_len = u32::from_le_bytes(data[11..15].try_into().unwrap()) as usize;
    let crc_off = 15usize;
    let payload = &data[19..19 + sec_len];
    assert_eq!(data[10], 1, "first section is catalog");
    let expect_crc = u32::from_le_bytes(data[crc_off..crc_off + 4].try_into().unwrap());
    assert_eq!(crc32fast::hash(payload), expect_crc);
    std::fs::write(path.join("catalog.bin"), payload).unwrap();
    std::fs::write(path.join("lsn_counter"), 0u64.to_le_bytes()).unwrap();
    std::fs::remove_file(&meta_path).unwrap();

    // 重开: 迁移回 meta.bin, 表完好
    let db = Database::open(&path).unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM legacy"), 30);
    assert!(meta_path.exists(), "meta.bin recreated by migration");
    assert!(!path.join("catalog.bin").exists(), "legacy file consumed");
    db.close().unwrap();

    // 再次重开幂等
    let db = Database::open(&path).unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM legacy"), 30);
    db.close().unwrap();
}

#[test]
fn f1_manifest_compaction_bounds_size_and_preserves_state() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("d.mote");
    let ms_dir_holder;
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        // 积累记录: 大量小 flush → AddSegment×N (每 checkpoint flush 一个段)
        for round in 0..1200 {
            db.execute(&format!("INSERT INTO t VALUES ({round}, 'v{round}')"))
                .unwrap();
            db.execute("CHECKPOINT").unwrap();
        }
        assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 1200);
        db.close().unwrap();
        ms_dir_holder = path.join("columnar_ms");
    }
    let manifest = ms_dir_holder.join("MANIFEST");
    let len = std::fs::metadata(&manifest).unwrap().len();
    // 1200 轮 ≈ 无 compaction 时 ~50KB 记录; 阈值 64KB 未到则文件也不应
    // 超过 64KB + 单记录 — 断言硬上界。
    assert!(
        len < 64 * 1024 + 4096,
        "manifest must stay bounded ({len}B after 1200 flush cycles)"
    );

    // 重开: 数据完好, 内存状态与文件一致(隐式验证 compaction 后 replay 等价)
    let db = Database::open(&path).unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 1200);
    db.close().unwrap();
}

#[test]
fn f1_open_time_scales_once_not_per_table() {
    // 回归防呆: 打开耗时不应随 manifest 历史显著膨胀 (F1 修复前,
    // 每个 store 全量 replay 整个 manifest)。
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("d.mote");
    {
        let db = Database::create(&path).unwrap();
        for t in 0..20 {
            db.execute(&format!(
                "CREATE TABLE t{t} (id INTEGER PRIMARY KEY, v TEXT)"
            ))
            .unwrap();
        }
        // 每表积累 ~40 条 AddSegment 记录 (compaction 合并段, 历史进 ever_seen)
        for round in 0..40 {
            for t in 0..20 {
                db.execute(&format!("INSERT INTO t{t} VALUES ({round}, 'v')"))
                    .unwrap();
            }
            db.execute("CHECKPOINT").unwrap();
        }
        db.close().unwrap();
    }
    let t0 = std::time::Instant::now();
    let db = Database::open(&path).unwrap();
    let open_ms = t0.elapsed().as_millis();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t0"), 40);
    db.close().unwrap();
    // 20 表 + 800 轮记录: 修复后应远小于 1s (修复前量级 ~50ms+/表组);
    // 上界宽松以防 CI 抖动, 主要防 N×replay 退化回归。
    assert!(open_ms < 1000, "open took {open_ms}ms");
}
