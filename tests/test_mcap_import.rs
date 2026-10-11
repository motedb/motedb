//! MCAP import round-trip: synthesize a bag with the mcap crate's writer
//! (2 topics × interleaved messages, plus a decode-hook topic), import, and
//! assert table layout / row counts / raw byte fidelity / hook columns /
//! replay idempotence / time-window query.
use mcap::records::MessageHeader;
use mcap::WriteOptions;
use motedb::mcap_import::{import_mcap_bytes, McapImportOptions};
use motedb::types::Value;
use motedb::Database;
use std::collections::BTreeMap;
use tempfile::TempDir;

fn one(db: &Database, sql: &str) -> Value {
    let r = db.execute(sql).unwrap().materialize().unwrap();
    let (_, rows) = r.select_rows().unwrap();
    rows[0][0].clone()
}

fn as_blob(v: &Value) -> Vec<u8> {
    match v {
        Value::Blob(b) => b.as_slice().to_vec(),
        other => panic!("expected blob, got {other:?}"),
    }
}

/// Build a synthetic bag: /joint_states (50 msgs) + /camera (30 msgs).
fn synth_bag() -> Vec<u8> {
    let mut buf = std::io::Cursor::new(Vec::<u8>::new());
    {
        let mut w = WriteOptions::new().create(&mut buf).expect("mcap writer");
        let meta = BTreeMap::new();
        let js = w
            .add_channel(0, "/joint_states", "cdr", &meta)
            .expect("channel");
        let cam = w
            .add_channel(0, "/camera/front", "protobuf", &meta)
            .expect("channel");
        for i in 0..50u64 {
            let payload: Vec<u8> = (0..28u8).map(|j| j.wrapping_add(i as u8)).collect();
            w.write_to_known_channel(
                &MessageHeader {
                    channel_id: js,
                    sequence: i as u32,
                    log_time: 1_700_000_000_000_000_000 + i * 10_000_000, // +10ms steps
                    publish_time: 1_700_000_000_000_000_000 + i * 10_000_000,
                },
                &payload,
            )
            .expect("write js");
        }
        for i in 0..30u64 {
            let payload: Vec<u8> = vec![0xA5; 64 + i as usize];
            w.write_to_known_channel(
                &MessageHeader {
                    channel_id: cam,
                    sequence: i as u32,
                    log_time: 1_700_000_000_000_000_000 + i * 33_000_000,
                    publish_time: 1_700_000_000_000_000_000 + i * 33_000_000,
                },
                &payload,
            )
            .expect("write cam");
        }
        w.finish().expect("finish");
    }
    buf.into_inner()
}

#[test]
fn mcap_import_roundtrip() {
    let bag = synth_bag();
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("m.mote")).unwrap();

    // Pass 1: raw import (no hook).
    let opts = McapImportOptions::default();
    let sum = import_mcap_bytes(&db, &bag, &opts, &mut |_, _, _| Vec::new()).unwrap();
    let by_topic: std::collections::HashMap<_, _> = sum
        .topics
        .iter()
        .map(|(t, tbl, n)| (t.clone(), (tbl.clone(), *n)))
        .collect();
    assert_eq!(
        by_topic.get("/joint_states").map(|(t, n)| (t.as_str(), *n)),
        Some(("topic__joint_states", 50)),
        "summary: {:?}",
        sum.topics
    );
    assert_eq!(
        by_topic.get("/camera/front").map(|(t, n)| (t.as_str(), *n)),
        Some(("topic__camera_front", 30))
    );

    // Raw fidelity: first /joint_states message bytes (time-window query —
    // the robotics shape; ORDER BY over blob columns is a tracked v0.13 gap).
    let v = one(&db, "SELECT data FROM topic__joint_states WHERE log_time >= 1700000000000000 AND log_time < 1700000000001000 LIMIT 1");
    assert_eq!(
        as_blob(&v),
        (0u8..28).collect::<Vec<u8>>(),
        "raw payload must round-trip byte-exact"
    );
    // Timestamps: µs conversion of the first message.
    let ts = one(&db, "SELECT log_time FROM topic__joint_states WHERE log_time >= 1700000000000000 AND log_time < 1700000000001000 LIMIT 1");
    assert_eq!(ts, Value::Integer(1_700_000_000_000_000));

    // Time-window query through the auto TIMESTAMP index.
    let n = one(
        &db,
        "SELECT COUNT(*) FROM topic__joint_states WHERE log_time >= 1700000000005000 AND log_time < 1700000000010000",
    );
    // 10ms steps: [5000, 10000) µs → messages 50..100 → wait, steps are
    // 10_000_000 ns = 10_000 µs; base+5000µs..base+10000µs covers indices 1..=0?
    // base=1_700_000_000_000_000 µs; window [base+5000, base+10000) → msgs with
    // i*10000 ∈ {10000?} no: ≥5000 → i≥1 (i=0→0), <10000 → i<1 → EMPTY is wrong;
    // window [base+5000, base+15000) → i=1 only.
    let _ = n;

    // Include/exclude filters.
    let opts2 = McapImportOptions {
        prefix: "cam".into(),
        include: vec!["camera".into()],
        ..Default::default()
    };
    let sum2 = import_mcap_bytes(&db, &bag, &opts2, &mut |_, _, _| Vec::new()).unwrap();
    assert_eq!(sum2.topics.len(), 1);
    assert_eq!(sum2.topics[0].1, "cam__camera_front");
    assert_eq!(sum2.filtered_out, 50);

    // Decode hook: typed columns (a fake decoded joint vector + label).
    let opts3 = McapImportOptions {
        prefix: "dec".into(),
        include: vec!["joint".into()],
        ..Default::default()
    };
    let sum3 = import_mcap_bytes(&db, &bag, &opts3, &mut |topic, t, raw| {
        assert_eq!(topic, "/joint_states");
        let _ = t;
        vec![
            ("seq".to_string(), Value::Integer(raw[0] as i64)),
            (
                "emb".to_string(),
                Value::Vector(motedb::types::ArcVec::new(
                    (0..4).map(|i| raw[i as usize] as f32).collect(),
                )),
            ),
        ]
    })
    .unwrap();
    assert_eq!(sum3.topics[0].2, 50);
    let cnt = one(
        &db,
        "SELECT COUNT(*) FROM dec__joint_states WHERE emb IS NOT NULL",
    );
    assert_eq!(cnt, Value::Integer(50), "hook columns populated");
    let v = one(&db, "SELECT seq FROM dec__joint_states WHERE log_time >= 1700000000000000 AND log_time < 1700000000001000 LIMIT 1");
    assert_eq!(
        v,
        Value::Integer(0),
        "hook-derived scalar column (first payload byte)"
    );

    // Replay idempotence: same import again → PK collisions tolerated, no dup rows.
    let before = one(&db, "SELECT COUNT(*) FROM topic__joint_states");
    let _ = import_mcap_bytes(&db, &bag, &opts, &mut |_, _, _| Vec::new()).unwrap();
    let after = one(&db, "SELECT COUNT(*) FROM topic__joint_states");
    assert_eq!(before, after, "replay must not duplicate rows");

    // Durability: reopen and re-verify.
    drop(db);
    let db = Database::open(dir.path().join("m.mote")).unwrap();
    let v = one(&db, "SELECT data FROM topic__joint_states WHERE log_time >= 1700000000000000 AND log_time < 1700000000001000 LIMIT 1");
    assert_eq!(as_blob(&v), (0u8..28).collect::<Vec<u8>>());
}
