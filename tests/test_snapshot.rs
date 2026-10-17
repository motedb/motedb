//! P4 EXPORT/IMPORT 单文件快照验收 (docs/design/2026-10-file-layout-plan.md §4):
//! MOTESNAP 容器 — 数据库的"运输形态"。
//!
//! 覆盖: SQL 面与 API 面的往返 (含 VECTOR/BLOB/FTS/dataset)、压缩开关、
//! 损坏检测、排除项、导入后即正常库 (P2/P3 布局)。
use motedb::{Database, MoteDB};
use tempfile::TempDir;

fn count(db: &Database, sql: &str) -> i64 {
    let r = db.execute(sql).unwrap().materialize().unwrap();
    let (_, data) = r.select_rows().unwrap();
    match data[0][0] {
        motedb::types::Value::Integer(i) => i,
        ref v => panic!("unexpected count: {v:?}"),
    }
}

fn build_source_db(path: &std::path::Path) {
    let db = Database::create(path).unwrap();
    db.execute(
        "CREATE TABLE sensors (id INTEGER PRIMARY KEY, dev TEXT, ts TIMESTAMP, \
         temp FLOAT, emb VECTOR(4), note TEXT)",
    )
    .unwrap();
    db.execute("CREATE TABLE files (id INTEGER PRIMARY KEY, name TEXT, payload BLOB)")
        .unwrap();
    db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();

    for i in 0..500 {
        db.execute(&format!(
            "INSERT INTO sensors VALUES ({i}, 'd{}', '2024-01-15 10:{:02}:{:02}', {}, \
             [{}, {}, {}, {}], 'note-{i}')",
            i % 7,
            i / 60,
            i % 60,
            i as f64 * 0.1,
            i as f64,
            (i + 1) as f64,
            (i + 2) as f64,
            (i + 3) as f64
        ))
        .unwrap();
    }
    for i in 0..100 {
        db.execute(&format!(
            "INSERT INTO files VALUES ({i}, 'f{i}', X'{}')",
            "ab".repeat(16)
        ))
        .unwrap();
    }
    for i in 0..80 {
        db.execute(&format!(
            "INSERT INTO docs VALUES ({i}, 'robot arm calibration note {i}')"
        ))
        .unwrap();
    }
    db.execute("CREATE TEXT INDEX ft_body ON docs(body)")
        .unwrap();
    // Give the FTS build a query to settle, then pin a dataset snapshot.
    let _ = count(&db, "SELECT COUNT(*) FROM docs");
    db.execute("COMMIT DATASET 'v1' NOTE 'pre-export'").unwrap();
    db.execute("CHECKPOINT").unwrap();
    db.close().unwrap();
}

#[test]
fn p4_sql_export_roundtrip_multimodal() {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("src.mote");
    build_source_db(&src);

    let snap = dir.path().join("snap.mote");
    {
        let db = Database::open(&src).unwrap();
        let r = db
            .execute(&format!("EXPORT DB TO '{}'", snap.display()))
            .unwrap()
            .materialize()
            .unwrap();
        let _ = r;
        db.close().unwrap();
    }
    assert!(snap.is_file(), "single snapshot file created");

    // Import into a fresh directory and open as a normal database.
    let dest = dir.path().join("dest.mote");
    MoteDB::import_snapshot(&snap, &dest).unwrap();
    {
        let db = Database::open(&dest).unwrap();
        assert_eq!(count(&db, "SELECT COUNT(*) FROM sensors"), 500);
        assert_eq!(count(&db, "SELECT COUNT(*) FROM files"), 100);
        assert_eq!(count(&db, "SELECT COUNT(*) FROM docs"), 80);

        // Vector search survives the roundtrip.
        let hits = db
            .execute(
                "SELECT COUNT(*) FROM (
                       SELECT id FROM sensors
                       ORDER BY emb <-> '[0.0, 1.0, 2.0, 3.0]' ASC LIMIT 10) AS t",
            )
            .unwrap()
            .materialize()
            .unwrap();
        let (_, data) = hits.select_rows().unwrap();
        assert_eq!(data[0][0], motedb::types::Value::Integer(10));

        // BLOB bytes intact.
        let blob = db
            .execute("SELECT payload FROM files WHERE id = 7")
            .unwrap()
            .materialize()
            .unwrap();
        let (_, data) = blob.select_rows().unwrap();
        assert_eq!(
            data[0][0],
            motedb::types::Value::Blob(motedb::types::ArcBytes(std::sync::Arc::from(
                vec![0xabu8; 16].as_slice()
            )))
        );

        // FTS survives (rebuilt or carried — MATCH must return rows).
        let fts = db
            .execute("SELECT COUNT(*) FROM docs WHERE MATCH(body, 'calibration')")
            .unwrap()
            .materialize()
            .unwrap();
        let (_, data) = fts.select_rows().unwrap();
        assert_eq!(data[0][0], motedb::types::Value::Integer(80));

        // Dataset pin metadata carried: _dataset_commits is a normal table.
        let ds = count(&db, "SELECT COUNT(*) FROM _dataset_commits");
        assert!(
            ds >= 3,
            "dataset v1 commit records present after import: {ds}"
        );
        db.close().unwrap();
    }
}

#[test]
fn p4_compressed_export_smaller_and_roundtrip() {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("src.mote");
    build_source_db(&src);

    let snap_plain = dir.path().join("plain.mote");
    let snap_z = dir.path().join("z.mote");
    {
        let db = Database::open(&src).unwrap();
        let _ = db
            .execute(&format!("EXPORT DB TO '{}'", snap_plain.display()))
            .unwrap();
        let _ = db
            .execute(&format!("EXPORT DB TO '{}' COMPRESSED", snap_z.display()))
            .unwrap();
        db.close().unwrap();
    }
    let plain_len = std::fs::metadata(&snap_plain).unwrap().len();
    let z_len = std::fs::metadata(&snap_z).unwrap().len();
    // The compressible TEXT/VECTOR payload must shrink.
    assert!(z_len < plain_len, "zstd {z_len} >= plain {plain_len}");

    let dest = dir.path().join("dest.mote");
    MoteDB::import_snapshot(&snap_z, &dest).unwrap();
    let db = Database::open(&dest).unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM sensors"), 500);
    db.close().unwrap();
}

#[test]
fn p4_corrupt_snapshot_rejected() {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("src.mote");
    build_source_db(&src);

    let snap = dir.path().join("s.mote");
    {
        let db = Database::open(&src).unwrap();
        let _ = db
            .execute(&format!("EXPORT DB TO '{}'", snap.display()))
            .unwrap();
        db.close().unwrap();
    }

    // Flip a byte in the middle of the archive.
    let mut data = std::fs::read(&snap).unwrap();
    let mid = data.len() / 2;
    data[mid] ^= 0xFF;
    std::fs::write(&snap, &data).unwrap();

    let dest = dir.path().join("dest.mote");
    let err = MoteDB::import_snapshot(&snap, &dest).expect_err("must fail");
    assert!(
        err.to_string().to_lowercase().contains("verif")
            || err.to_string().to_lowercase().contains("crc")
            || err.to_string().to_lowercase().contains("truncat")
            || err.to_string().to_lowercase().contains("zstd")
            || err.to_string().to_lowercase().contains("decode"),
        "corruption detected, got: {err}"
    );
}

#[test]
fn p4_export_excludes_live_state_and_import_layout_is_p3() {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("src.mote");
    build_source_db(&src);

    let snap = dir.path().join("s.mote");
    {
        let db = Database::open(&src).unwrap();
        let r = db
            .execute(&format!("EXPORT DB TO '{}' COMPRESSED", snap.display()))
            .unwrap()
            .materialize()
            .unwrap();
        let _ = r;
        db.close().unwrap();
    }

    let dest = dir.path().join("dest.mote");
    MoteDB::import_snapshot(&snap, &dest).unwrap();

    // P3 final layout: no wal/ remnant, no legacy files, meta.bin present.
    let files: Vec<String> = std::fs::read_dir(&dest)
        .unwrap()
        .flatten()
        .filter_map(|e| e.file_name().to_str().map(String::from))
        .collect();
    assert!(
        !files.iter().any(|f| f == "wal"),
        "wal/ excluded: {files:?}"
    );
    assert!(!files.iter().any(|f| f == "catalog.bin"));
    assert!(files.iter().any(|f| f == "meta.bin"));
    // And the imported DB reopens cleanly.
    let db = Database::open(&dest).unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM docs"), 80);
    db.close().unwrap();
}

#[test]
fn p4_export_rejects_dest_inside_db_and_existing() {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("src.mote");
    build_source_db(&src);

    let db = Database::open(&src).unwrap();
    let inside = src.join("inside.mote");
    assert!(db
        .execute(&format!("EXPORT DB TO '{}'", inside.display()))
        .is_err());
    let snap = dir.path().join("ok.mote");
    let _ = db
        .execute(&format!("EXPORT DB TO '{}'", snap.display()))
        .unwrap();
    // Second export to the same path must fail (exists).
    assert!(db
        .execute(&format!("EXPORT DB TO '{}'", snap.display()))
        .is_err());
    db.close().unwrap();

    // Import onto an existing directory must fail.
    assert!(MoteDB::import_snapshot(&snap, &src).is_err());
}

#[test]
fn container_readonly_mode_end_to_end() {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("src.mote");
    build_source_db(&src);
    let snap = dir.path().join("fleet.mote");
    {
        let db = Database::open(&src).unwrap();
        let _ = db
            .execute(&format!("EXPORT DB TO '{}'", snap.display()))
            .unwrap();
        db.close().unwrap();
    }
    let hash_before = {
        let mut h = crc32fast::Hasher::new();
        h.update(&std::fs::read(&snap).unwrap());
        h.finalize()
    };

    // 显式只读打开
    let ro = Database::open_readonly(&snap).unwrap();
    assert_eq!(count(&ro, "SELECT COUNT(*) FROM sensors"), 500);
    assert_eq!(count(&ro, "SELECT COUNT(*) FROM files"), 100);
    let blob = ro
        .execute("SELECT LENGTH(payload) FROM files WHERE id = 7")
        .unwrap()
        .materialize()
        .unwrap();
    let (_, rows) = blob.select_rows().unwrap();
    assert_eq!(rows[0][0], motedb::types::Value::Integer(16));
    // TS 表 (方案 E 统一后) 也在容器内可查
    let fts = count(
        &ro,
        "SELECT COUNT(*) FROM docs WHERE MATCH(body, 'calibration')",
    );
    assert_eq!(fts, 80);

    // 写全部拒绝
    for sql in [
        "INSERT INTO sensors VALUES (999, 'x', 1.0, 1, [1,2,3,4], 'n')",
        "UPDATE files SET name = 'x' WHERE id = 1",
        "DELETE FROM docs WHERE id = 1",
        "CREATE TABLE rogue (id INTEGER PRIMARY KEY)",
        "DROP TABLE sensors",
        "CHECKPOINT",
        "VACUUM",
        "BEGIN",
    ] {
        let err = ro.execute(sql).err().expect(sql);
        assert!(
            err.to_string().to_lowercase().contains("read-only"),
            "{sql} -> {err}"
        );
    }
    assert!(ro.insert_rows("sensors", vec![vec![]]).is_err());
    ro.close().unwrap();

    // 容器源文件字节不变
    let hash_after = {
        let mut h = crc32fast::Hasher::new();
        h.update(&std::fs::read(&snap).unwrap());
        h.finalize()
    };
    assert_eq!(hash_before, hash_after, "container file must be untouched");

    // open() 自动路由 (文件路径直接命中容器)
    let ro2 = Database::open(&snap).unwrap();
    assert_eq!(count(&ro2, "SELECT COUNT(*) FROM sensors"), 500);
    assert!(ro2.execute("INSERT INTO docs VALUES (1, 'x')").is_err());
    ro2.close().unwrap();

    // 并存: 两个只读句柄同时打开同一容器
    let a = Database::open_readonly(&snap).unwrap();
    let b = Database::open_readonly(&snap).unwrap();
    assert_eq!(count(&a, "SELECT COUNT(*) FROM files"), 100);
    assert_eq!(count(&b, "SELECT COUNT(*) FROM docs"), 80);
    a.close().unwrap();
    b.close().unwrap();

    // 非容器文件报清晰错误
    let not_container = dir.path().join("plain.txt");
    std::fs::write(&not_container, b"garbage").unwrap();
    let err = Database::open_readonly(&not_container).err().unwrap();
    assert!(err.to_string().contains("not a MOTESNAP container"));
}

#[test]
fn container_readonly_gates_prepared_and_direct_paths() {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("src.mote");
    build_source_db(&src);
    let snap = dir.path().join("c.mote");
    {
        let db = Database::open(&src).unwrap();
        let _ = db
            .execute(&format!("EXPORT DB TO '{}'", snap.display()))
            .unwrap();
        db.close().unwrap();
    }
    let ro = Database::open_readonly(&snap).unwrap();

    // prepared-statement entry (execute_prepared / execute_prepared_many)
    let err = ro
        .execute_prepared(
            "INSERT INTO docs VALUES (?, ?)",
            vec![
                motedb::types::Value::Integer(1),
                motedb::types::Value::Text("x".into()),
            ],
        )
        .err()
        .expect("prepared INSERT must be rejected");
    assert!(err.to_string().contains("read-only"), "{err}");
    let err = ro
        .execute_prepared_many(
            "INSERT INTO docs VALUES (?, ?)",
            vec![vec![
                motedb::types::Value::Integer(2),
                motedb::types::Value::Text("y".into()),
            ]],
        )
        .expect_err("prepared-many INSERT must be rejected");
    assert!(err.to_string().contains("read-only"), "{err}");
    // direct API
    assert!(ro.insert_rows("docs", vec![vec![]]).is_err());
    assert!(ro.checkpoint().is_err());
    ro.close().unwrap();

    // create + read_only 组合拒绝
    let bad = motedb::DBConfig {
        read_only: true,
        ..Default::default()
    };
    let err = Database::create_with_config(dir.path().join("bad.mote"), bad)
        .err()
        .expect("create+read_only must be rejected");
    assert!(err.to_string().contains("open_readonly"), "{err}");
}

#[test]
fn btree_small_file_reopens_after_v3_reservation() {
    // 审计回归: read_superblock 曾对 <64KB 文件 read_exact EOF — v2 升级
    // 路径与零页 v3 文件全炸。空表建索引 = 最小文件形态。
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("t.mote");
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
            .unwrap();
        // 空表上建列索引: btree 文件只有 superblock。
        db.execute("CREATE INDEX idx_v ON t(v)").unwrap();
        db.close().unwrap();
    }
    // 重开: 小文件 read_superblock 必须 EOF-容忍。
    let db = Database::open(&path).unwrap();
    db.execute("INSERT INTO t VALUES (1, 42)").unwrap();
    let r = db
        .execute("SELECT COUNT(*) FROM t WHERE v = 42")
        .unwrap()
        .materialize()
        .unwrap();
    let (_, rows) = r.select_rows().unwrap();
    assert_eq!(rows[0][0], motedb::types::Value::Integer(1));
    db.execute("CHECKPOINT").unwrap();
    db.close().unwrap();
    // 再重开: 写入页后 (≥64KB) 同样正常。
    let db = Database::open(&path).unwrap();
    let r = db
        .execute("SELECT COUNT(*) FROM t WHERE v = 42")
        .unwrap()
        .materialize()
        .unwrap();
    let (_, rows) = r.select_rows().unwrap();
    assert_eq!(rows[0][0], motedb::types::Value::Integer(1));
    db.close().unwrap();
}

#[test]
fn btree_v2_index_files_survive_upgrade() {
    // 审计轮2回归: v3 保留区变更后, version != 3 曾把存量 v2 索引静默重置为
    // 空(干净库无 WAL 重放 → 重建被跳过 → 索引驱动查询悄悄丢行)。现在 v2
    // 保持可读。模拟: 把新写索引文件的版本字节改成 2 再重开。
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("t.mote");
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
            .unwrap();
        for i in 0..200 {
            db.execute(&format!("INSERT INTO t VALUES ({i}, {})", i % 17))
                .unwrap();
        }
        db.execute("CREATE INDEX idx_v ON t(v)").unwrap();
        db.execute("CHECKPOINT").unwrap();
        db.close().unwrap();
    }
    // Patch every column index file's version field (u32 LE at offset 4) to 2.
    let idx_dir = path.join("indexes");
    let mut patched = 0;
    for e in std::fs::read_dir(&idx_dir).unwrap().flatten() {
        let p = e.path();
        if p.extension().and_then(|x| x.to_str()) == Some("idx")
            && p.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .starts_with("column_")
        {
            let mut data = std::fs::read(&p).unwrap();
            data[4..8].copy_from_slice(&2u32.to_le_bytes());
            std::fs::write(&p, &data).unwrap();
            patched += 1;
        }
    }
    assert!(patched >= 1, "expected at least one column index");

    let db = Database::open(&path).unwrap();
    let r = db
        .execute("SELECT COUNT(*) FROM t WHERE v = 5")
        .unwrap()
        .materialize()
        .unwrap();
    let (_, rows) = r.select_rows().unwrap();
    assert_eq!(
        rows[0][0],
        motedb::types::Value::Integer(12),
        "v2 index serves queries after upgrade"
    );
    db.close().unwrap();
}

#[test]
fn container_extraction_concurrent_opens() {
    // 审计轮2回归: 两进程同时首开同一容器, 解包竞争的落败方曾直接报错。
    // 线程级模拟: 并发 open_readonly, 双方都必须成功。
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("src.mote");
    build_source_db(&src);
    let snap = dir.path().join("c.mote");
    {
        let db = Database::open(&src).unwrap();
        let _ = db
            .execute(&format!("EXPORT DB TO '{}'", snap.display()))
            .unwrap();
        db.close().unwrap();
    }
    let s1 = snap.clone();
    let s2 = snap.clone();
    let h1 = std::thread::spawn(move || Database::open_readonly(&s1));
    let h2 = std::thread::spawn(move || Database::open_readonly(&s2));
    let a = h1.join().expect("t1 panicked").expect("t1 open failed");
    let b = h2.join().expect("t2 panicked").expect("t2 open failed");
    assert_eq!(count(&a, "SELECT COUNT(*) FROM sensors"), 500);
    assert_eq!(count(&b, "SELECT COUNT(*) FROM sensors"), 500);
    a.close().unwrap();
    b.close().unwrap();
}
