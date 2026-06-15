//! Tests for the append-only multi-segment columnar store (col_segment).
//! Covers S1-S5: skeleton, multi-flush, merge, manifest, compaction.

use motedb::storage::col_segment::{ColSegmentStore, Manifest, ManifestState};
use motedb::types::{ColumnType, Value};
use tempfile::TempDir;

fn col_types() -> Vec<ColumnType> {
    vec![ColumnType::Integer, ColumnType::Text]
}

#[test]
fn s1_single_segment_get() {
    let dir = TempDir::new().unwrap();
    let store = ColSegmentStore::create(dir.path(), "t", col_types()).unwrap();
    store
        .append_rows(&[
            (1, 100, vec![Value::Integer(10), Value::Text("a".into())]),
            (2, 100, vec![Value::Integer(20), Value::Text("b".into())]),
        ])
        .unwrap();
    store.flush_buffer().unwrap();

    assert_eq!(store.segment_count(), 1);
    assert_eq!(
        store.get(1).unwrap(),
        vec![Value::Integer(10), Value::Text("a".into())]
    );
    assert_eq!(
        store.get(2).unwrap(),
        vec![Value::Integer(20), Value::Text("b".into())]
    );
    assert!(store.get(999).is_none());
}

#[test]
fn s2_multiple_flushes_create_multiple_segments() {
    let dir = TempDir::new().unwrap();
    let store = ColSegmentStore::create(dir.path(), "t", col_types()).unwrap();

    store
        .append_rows(&[
            (1, 100, vec![Value::Integer(10), Value::Text("a".into())]),
            (2, 100, vec![Value::Integer(20), Value::Text("b".into())]),
        ])
        .unwrap();
    store.flush_buffer().unwrap();
    assert_eq!(store.segment_count(), 1);

    store
        .append_rows(&[(
            3,
            200,
            vec![Value::Integer(30), Value::Text("c".into())],
        )])
        .unwrap();
    store.flush_buffer().unwrap();
    assert_eq!(store.segment_count(), 2);

    // All keys visible via get (checks newest-segment-first).
    assert_eq!(store.get(1).unwrap()[0], Value::Integer(10));
    assert_eq!(store.get(3).unwrap()[0], Value::Integer(30));
}

#[test]
fn s3_merge_all_distinct_keys_across_segments() {
    let dir = TempDir::new().unwrap();
    let store = ColSegmentStore::create(dir.path(), "t", col_types()).unwrap();
    store
        .append_rows(&[
            (1, 100, vec![Value::Integer(10), Value::Text("a".into())]),
            (2, 100, vec![Value::Integer(20), Value::Text("b".into())]),
        ])
        .unwrap();
    store.flush_buffer().unwrap();
    store
        .append_rows(&[
            (3, 100, vec![Value::Integer(30), Value::Text("c".into())]),
            (4, 100, vec![Value::Integer(40), Value::Text("d".into())]),
        ])
        .unwrap();
    store.flush_buffer().unwrap();

    let keys: Vec<u64> = store.scan().map(|(k, _, _)| k).collect();
    assert_eq!(keys, vec![1, 2, 3, 4]);
}

#[test]
fn s3_merge_newest_version_wins() {
    let dir = TempDir::new().unwrap();
    let store = ColSegmentStore::create(dir.path(), "t", col_types()).unwrap();
    // seg1: key=1 val=10 ts=100
    store
        .append_rows(&[(
            1,
            100,
            vec![Value::Integer(10), Value::Text("old".into())],
        )])
        .unwrap();
    store.flush_buffer().unwrap();
    // seg2: key=1 val=99 ts=200 (newer)
    store
        .append_rows(&[(
            1,
            200,
            vec![Value::Integer(99), Value::Text("new".into())],
        )])
        .unwrap();
    store.flush_buffer().unwrap();

    let rows: Vec<(u64, Vec<Value>)> = store.scan().map(|(k, _, r)| (k, r)).collect();
    assert_eq!(rows.len(), 1, "same key deduplicated");
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[0].1[0], Value::Integer(99), "newest version wins");
    assert_eq!(rows[0].1[1], Value::Text("new".into()));
}

#[test]
fn s4_manifest_records_and_recovers() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("MANIFEST");
    {
        let mut m = Manifest::create(&path).unwrap();
        m.add_segment(1).unwrap();
        m.add_segment(2).unwrap();
        m.record_compaction(3, &[1, 2]).unwrap();
    }
    // Reopen and replay.
    let m = Manifest::open(&path).unwrap();
    let state: ManifestState = m.replay();
    assert!(state.active_segments.contains(&3), "new segment 3 active");
    assert!(
        !state.active_segments.contains(&1),
        "old segment 1 superseded"
    );
    assert!(
        !state.active_segments.contains(&2),
        "old segment 2 superseded"
    );
    assert!(
        state.obsolete_files.contains(&1) && state.obsolete_files.contains(&2),
        "old files pending GC"
    );
}

#[test]
fn s5_compaction_merges_to_one_segment() {
    let dir = TempDir::new().unwrap();
    let store = ColSegmentStore::create(dir.path(), "t", col_types()).unwrap();
    for start in [0u64, 2, 4] {
        store
            .append_rows(&[
                (
                    start + 1,
                    100,
                    vec![Value::Integer((start + 1) as i64), Value::Text("x".into())],
                ),
                (
                    start + 2,
                    100,
                    vec![Value::Integer((start + 2) as i64), Value::Text("y".into())],
                ),
            ])
            .unwrap();
        store.flush_buffer().unwrap();
    }
    assert_eq!(store.segment_count(), 3);
    assert!(store.needs_compaction());

    store.compact_once().unwrap();
    assert_eq!(store.segment_count(), 1, "compaction reduces to 1 segment");

    let keys: Vec<u64> = store.scan().map(|(k, _, _)| k).collect();
    assert_eq!(keys, vec![1, 2, 3, 4, 5, 6], "all data still visible after compaction");
}

#[test]
fn s5_compaction_dedups_same_key() {
    let dir = TempDir::new().unwrap();
    let store = ColSegmentStore::create(dir.path(), "t", col_types()).unwrap();
    // 3 segments all with key=1, increasing ts
    for ts in [100u64, 200, 300] {
        store
            .append_rows(&[(
                1,
                ts,
                vec![Value::Integer(ts as i64), Value::Text("v".into())],
            )])
            .unwrap();
        store.flush_buffer().unwrap();
    }
    store.compact_once().unwrap();
    let rows: Vec<(u64, Vec<Value>)> = store.scan().map(|(k, _, r)| (k, r)).collect();
    assert_eq!(rows.len(), 1, "compaction dedups");
    assert_eq!(rows[0].1[0], Value::Integer(300), "keeps newest version");
}
