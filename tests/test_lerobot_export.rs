//! `EXPORT LEROBOT` — LeRobot v3 dataset shape (src/export/lerobot.rs).
//!
//! The exact on-disk contract was extracted from a reference dataset
//! generated with the official writer (lerobot 0.6.1): see the module docs
//! in `src/export/lerobot.rs`. These tests verify the structure and values;
//! the official-loader interop run is a local script (needs torch).

use motedb::types::{Timestamp, Value};
use motedb::Database;

/// Base time 2026-01-01T00:00:00Z in micros + offset (params avoid the
/// unsupported `TIMESTAMP '...'` literal).
fn ts(micros: i64) -> Value {
    Value::Timestamp(Timestamp::from_micros(1_767_225_600_000_000 + micros))
}

fn setup_episode_table(db: &Database) {
    db.execute(
        "CREATE EPISODE TABLE teleop (\
         session TEXT, ts TIMESTAMP, joint VECTOR(4), gripper FLOAT, note TEXT) \
         EPISODE BY (session, ts) GAP '5s'",
    )
    .unwrap();
    let insert = |session: &str, t_us: i64, joint: [f32; 4], gripper: f64, note: &str| {
        db.execute_prepared(
            "INSERT INTO teleop VALUES (?, ?, ?, ?, ?)",
            vec![
                Value::Text(motedb::types::ArcString::from(session)),
                ts(t_us),
                Value::Vector(motedb::types::ArcVec::new(joint.to_vec())),
                Value::Float(gripper),
                Value::Text(motedb::types::ArcString::from(note)),
            ],
        )
        .unwrap();
    };
    // Episode 0: session a, 6 frames 100ms apart.
    for i in 0..6i64 {
        insert(
            "a",
            i * 100_000,
            [i as f32, 1.0, -1.0, 0.5],
            (i % 2) as f64,
            "ep-a",
        );
    }
    // Same session, 10s later → gap > 5s → episode 1.
    for i in 0..4i64 {
        insert(
            "a",
            10_000_000 + i * 100_000,
            [i as f32, 9.0, 9.0, 9.0],
            0.5,
            "ep-a-late",
        );
    }
    // Different session → episode 2 (even without a gap).
    for i in 0..3i64 {
        insert("b", i * 100_000, [i as f32, 2.0, 2.0, 2.0], 0.25, "ep-b");
    }
    db.checkpoint().unwrap();
}

#[test]
fn lerobot_export_directory_and_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path().join("lr.mote")).unwrap();
    setup_episode_table(&db);

    let out = dir.path().join("dataset");
    let res = db
        .execute(&format!(
            "EXPORT LEROBOT teleop TO '{}' FPS 20 TASKS note",
            out.display()
        ))
        .unwrap()
        .materialize()
        .unwrap();
    match res {
        motedb::QueryResult::Definition { message } => {
            assert!(message.contains("3 episodes"), "{message}");
            assert!(message.contains("13 frames"), "{message}");
            assert!(message.contains("3 tasks"), "{message}");
        }
        other => panic!("{other:?}"),
    }

    // Directory contract.
    for f in [
        "meta/info.json",
        "meta/stats.json",
        "meta/tasks.parquet",
        "meta/episodes/chunk-000/file-000.parquet",
        "data/chunk-000/file-000.parquet",
    ] {
        assert!(out.join(f).is_file(), "missing {f}");
    }

    let info: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(out.join("meta/info.json")).unwrap())
            .unwrap();
    assert_eq!(info["codebase_version"], "v3.0");
    assert_eq!(info["fps"], 20);
    assert_eq!(info["total_episodes"], 3);
    assert_eq!(info["total_frames"], 13);
    assert_eq!(info["total_tasks"], 3);
    assert_eq!(info["splits"]["train"], "0:3");
    assert_eq!(info["video_path"], serde_json::Value::Null);
    // Feature mapping: joint VECTOR(4) → float32 shape [4]; gripper FLOAT →
    // float32 [1]; session/ts/note consumed.
    assert_eq!(info["features"]["joint"]["dtype"], "float32");
    assert_eq!(info["features"]["joint"]["shape"], serde_json::json!([4]));
    assert_eq!(info["features"]["gripper"]["dtype"], "float32");
    assert_eq!(info["features"]["gripper"]["shape"], serde_json::json!([1]));
    assert!(info["features"].get("session").is_none());
    assert!(info["features"].get("ts").is_none());
    assert!(info["features"].get("note").is_none());
    for k in [
        "timestamp",
        "frame_index",
        "episode_index",
        "index",
        "task_index",
    ] {
        assert!(info["features"].get(k).is_some(), "default feature {k}");
    }

    // stats.json covers numeric features + defaults, skips strings.
    let stats: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(out.join("meta/stats.json")).unwrap())
            .unwrap();
    for k in ["joint", "gripper", "timestamp", "index", "task_index"] {
        assert!(stats.get(k).is_some(), "stats[{k}] missing");
        for s in ["min", "max", "mean", "std", "count", "q50"] {
            assert!(stats[k].get(s).is_some(), "stats[{k}][{s}] missing");
        }
    }
    assert!(stats.get("session").is_none());
    // joint is 4-dim → per-dim stat lists.
    assert_eq!(stats["joint"]["min"].as_array().unwrap().len(), 4);
    // gripper scalar → single-element lists.
    assert_eq!(stats["gripper"]["max"].as_array().unwrap().len(), 1);
}

#[test]
fn lerobot_export_data_reads_back_via_parquet_import() {
    // Structural read-back through our own parquet importer: the int64
    // default columns come back as INTEGER, row counts and key values match.
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path().join("lr2.mote")).unwrap();
    setup_episode_table(&db);

    let out = dir.path().join("dataset");
    db.execute(&format!(
        "EXPORT LEROBOT teleop TO '{}' FPS 20 TASKS note",
        out.display()
    ))
    .unwrap();

    // The data file's `index` column is simply not present in the target
    // table (name-based mapping drops it); everything else maps by name.
    db.execute(
        "CREATE TABLE frames2 (t_sec FLOAT, frame_index INT, episode_index INT, task_index INT)",
    )
    .unwrap();
    let imported = db
        .execute(&format!(
            "IMPORT PARQUET '{}' INTO frames2",
            out.join("data/chunk-000/file-000.parquet").display()
        ))
        .unwrap()
        .materialize()
        .unwrap();
    match imported {
        motedb::QueryResult::Select { rows, .. } => {
            assert_eq!(rows, vec![vec![motedb::types::Value::Integer(13)]]);
        }
        other => panic!("{other:?}"),
    }
    // Episode sizes: 6 + 4 + 3.
    let counts = db
        .query(
            "SELECT episode_index, COUNT(*) AS n FROM frames2 GROUP BY episode_index ORDER BY episode_index",
        )
        .unwrap();
    let pairs: Vec<String> = counts.iter().map(|r| format!("{:?}", r)).collect();
    assert_eq!(pairs.len(), 3, "{pairs:?}");
    // episode 0 has 6 rows, 1 has 4, 2 has 3 — check via aggregate values.
    let n = |ep: i64| -> i64 {
        match &db
            .query(&format!(
                "SELECT COUNT(*) FROM frames2 WHERE episode_index = {ep}"
            ))
            .unwrap()[0][0]
        {
            Value::Integer(i) => *i,
            other => panic!("{other:?}"),
        }
    };
    assert_eq!((n(0), n(1), n(2)), (6, 4, 3));
}

#[test]
fn lerobot_export_requires_episode_table_and_empty_dir() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path().join("lr3.mote")).unwrap();
    db.execute("CREATE TABLE plain (id INT PRIMARY KEY)")
        .unwrap();
    db.execute("INSERT INTO plain VALUES (1)").unwrap();
    let err = match db.execute(&format!(
        "EXPORT LEROBOT plain TO '{}'",
        dir.path().join("ds").display()
    )) {
        Err(e) => e,
        Ok(_) => panic!("expected EPISODE TABLE requirement error"),
    };
    assert!(err.to_string().contains("EPISODE"), "{err}");

    // Non-empty destination is refused.
    db.execute("CREATE EPISODE TABLE t2 (s TEXT, ts TIMESTAMP) EPISODE BY (s, ts) GAP '5s'")
        .unwrap();
    db.execute_prepared(
        "INSERT INTO t2 VALUES (?, ?)",
        vec![Value::Text(motedb::types::ArcString::from("x")), ts(0)],
    )
    .unwrap();
    let occupied = dir.path().join("occupied");
    std::fs::create_dir_all(occupied.join("junk")).unwrap();
    let err = match db.execute(&format!("EXPORT LEROBOT t2 TO '{}'", occupied.display())) {
        Err(e) => e,
        Ok(_) => panic!("expected non-empty destination error"),
    };
    assert!(err.to_string().contains("not empty"), "{err}");
}
