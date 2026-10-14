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
