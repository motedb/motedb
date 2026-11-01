//! The global timestamp index (`query_timestamp_range`) must keep every
//! row that shares a microsecond and must forget rows that are deleted or
//! re-timestamped.
//!
//! v0.16.0: `BTreeConfig::unique_keys` was never read, so the leaf insert
//! overwrote the row id for an existing timestamp (one row per µs), and no
//! write path ever removed an entry (ghost row ids after DELETE/UPDATE).

use motedb::index::btree::{BTree, BTreeConfig};
use motedb::{Database, QueryResult};
use tempfile::TempDir;

/// Direct B-tree check: a run of equal keys longer than a leaf must survive
/// splits (the run straddles the separator) for range / contains / remove.
#[test]
fn btree_non_unique_run_spanning_leaves() {
    let dir = TempDir::new().unwrap();
    let mut tree = BTree::with_config(
        dir.path().join("dup.idx"),
        BTreeConfig {
            unique_keys: false,
            allow_updates: true,
            order: 32,
            ..Default::default()
        },
    )
    .unwrap();

    // surrounding keys so the run is not at the tree edge
    for k in [5u64, 7, 8] {
        tree.insert(k, 1000 + k).unwrap();
    }
    for v in 0..300u64 {
        tree.insert(10, v).unwrap();
    }
    for k in [11u64, 12, 20] {
        tree.insert(k, 2000 + k).unwrap();
    }
    assert_eq!(tree.len(), 306);

    // idempotent re-insert of an existing pair
    assert_eq!(tree.insert(10, 42).unwrap(), Some(42));
    assert_eq!(tree.len(), 306);

    let run = tree.range(&10, &10).unwrap();
    assert_eq!(run.len(), 300, "every duplicate must be returned");
    let mut vals: Vec<u64> = run.iter().map(|(_, v)| *v).collect();
    vals.sort_unstable();
    assert_eq!(vals, (0..300).collect::<Vec<_>>());

    assert_eq!(tree.range(&0, &9).unwrap().len(), 3);
    assert_eq!(tree.range(&11, &100).unwrap().len(), 3);
    assert_eq!(tree.range_with_limit(&10, &10, 7).unwrap().len(), 7);

    assert!(tree.contains_entry(10, 150).unwrap());
    assert!(!tree.contains_entry(10, 999).unwrap());

    // remove exactly one pair from the middle of the run
    assert!(tree.remove_entry(10, 150).unwrap());
    assert!(!tree.remove_entry(10, 150).unwrap());
    assert!(!tree.contains_entry(10, 150).unwrap());
    assert_eq!(tree.range(&10, &10).unwrap().len(), 299);
    assert_eq!(tree.len(), 305);

    // persisted
    tree.flush().unwrap();
    drop(tree);
    let tree = BTree::with_config(
        dir.path().join("dup.idx"),
        BTreeConfig {
            unique_keys: false,
            allow_updates: true,
            order: 32,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(tree.range(&10, &10).unwrap().len(), 299);
}

fn count_rows(db: &Database, sql: &str) -> usize {
    match db.execute(sql).unwrap().materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows.len(),
        other => panic!("unexpected: {other:?}"),
    }
}

#[test]
fn timestamp_index_keeps_duplicates_and_tracks_delete_update() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("d.mote");
    let t1 = 1_000_000i64;
    let t2 = 2_000_000i64;
    let t3 = 3_000_000i64;
    {
        let db = Database::create(&path).unwrap();
        // ts is NOT the first column — recovery used to index column 0 only.
        db.execute("CREATE TABLE ev (id INT PRIMARY KEY, v INT, ts TIMESTAMP)")
            .unwrap();
        for id in 1..=3 {
            db.execute(&format!("INSERT INTO ev VALUES ({id}, {id}0, {t1})"))
                .unwrap();
        }
        for id in 4..=5 {
            db.execute(&format!("INSERT INTO ev VALUES ({id}, {id}0, {t2})"))
                .unwrap();
        }
        assert_eq!(count_rows(&db, "SELECT id FROM ev"), 5);

        assert_eq!(
            db.query_timestamp_range(t1, t1).unwrap().len(),
            3,
            "three rows share t1"
        );
        assert_eq!(db.query_timestamp_range(t1, t2).unwrap().len(), 5);

        db.checkpoint().unwrap();
        assert_eq!(db.query_timestamp_range(t1, t1).unwrap().len(), 3);

        db.execute("DELETE FROM ev WHERE id = 2").unwrap();
        assert_eq!(
            db.query_timestamp_range(t1, t1).unwrap().len(),
            2,
            "deleted row must leave the index"
        );

        db.execute(&format!("UPDATE ev SET ts = {t3} WHERE id = 1"))
            .unwrap();
        assert_eq!(db.query_timestamp_range(t1, t1).unwrap().len(), 1);
        assert_eq!(db.query_timestamp_range(t3, t3).unwrap().len(), 1);
        assert_eq!(db.query_timestamp_range(t1, t3).unwrap().len(), 4);
        db.checkpoint().unwrap();
    }

    let db = Database::open(&path).unwrap();
    assert_eq!(db.query_timestamp_range(t1, t1).unwrap().len(), 1);
    assert_eq!(db.query_timestamp_range(t2, t2).unwrap().len(), 2);
    assert_eq!(db.query_timestamp_range(t3, t3).unwrap().len(), 1);
    assert_eq!(db.query_timestamp_range(t1, t3).unwrap().len(), 4);
    assert_eq!(
        count_rows(&db, &format!("SELECT id FROM ev WHERE ts = {t1}")),
        1
    );
}

/// `UPDATE … SET ts = <int µs | ISO string>` must store the timestamp, not
/// the epoch: the shared schema coercion lacked the TIMESTAMP arm that the
/// column-list INSERT path had privately.
#[test]
fn update_coerces_integer_and_iso_text_into_timestamp_column() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE ev (id INT PRIMARY KEY, v INT, ts TIMESTAMP)")
        .unwrap();
    db.execute("INSERT INTO ev VALUES (1, 10, 1000000)")
        .unwrap();

    let ts_of = |db: &Database| match db
        .execute("SELECT ts FROM ev WHERE id = 1")
        .unwrap()
        .materialize()
        .unwrap()
    {
        QueryResult::Select { rows, .. } => rows[0][0].clone(),
        other => panic!("unexpected: {other:?}"),
    };

    db.execute("UPDATE ev SET ts = 3000000 WHERE id = 1")
        .unwrap();
    assert_eq!(
        ts_of(&db),
        motedb::types::Value::Timestamp(motedb::types::Timestamp::from_micros(3_000_000))
    );

    db.execute("UPDATE ev SET ts = '1970-01-01 00:00:05' WHERE id = 1")
        .unwrap();
    assert_eq!(
        ts_of(&db),
        motedb::types::Value::Timestamp(motedb::types::Timestamp::from_micros(5_000_000))
    );
    assert_eq!(
        db.query_timestamp_range(5_000_000, 5_000_000)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        db.query_timestamp_range(1_000_000, 3_000_000)
            .unwrap()
            .len(),
        0
    );
}

/// P7: flush() used to rebuild the page table from the LRU cache alone and
/// truncate the file — every page evicted since the last flush was silently
/// destroyed (≈180K-entry lifetime ceiling on the timestamp index; a backup
/// flushed unconditionally). Now flush covers every allocated page and the
/// LRU writeback (`push`, not the silently-dropping `put`) keeps evicted
/// pages' disk images current.
#[test]
fn flush_persists_pages_beyond_cache_capacity() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("u.idx");
    {
        let mut t = BTree::with_config(
            path.clone(),
            BTreeConfig {
                unique_keys: false,
                allow_updates: true,
                order: 32,
                cache_size: 64,
                ..Default::default()
            },
        )
        .unwrap();
        for i in 0..2000u64 {
            t.insert(i, i * 7).unwrap();
        }
        t.flush().unwrap();
    }
    let t = BTree::with_config(
        path,
        BTreeConfig {
            unique_keys: false,
            allow_updates: true,
            order: 32,
            cache_size: 64,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(t.range(&0, &1999).unwrap().len(), 2000);
    assert_eq!(t.len(), 2000);
}

/// P1: a run of key 0 (every pre-1970 timestamp clamps to micros 0) must be
/// fully visible to range/remove — `first_leaf_for_scan` used to probe
/// key-1 = key (saturating) and descend right of the straddled run.
#[test]
fn key_zero_run_is_fully_visible() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("z.idx");
    {
        let mut t = BTree::with_config(
            path.clone(),
            BTreeConfig {
                unique_keys: false,
                allow_updates: true,
                order: 32,
                cache_size: 64,
                ..Default::default()
            },
        )
        .unwrap();
        for i in 0..300u64 {
            t.insert(0, i).unwrap();
        }
        t.flush().unwrap();
    }
    let mut t = BTree::with_config(
        path,
        BTreeConfig {
            unique_keys: false,
            allow_updates: true,
            order: 32,
            cache_size: 64,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(t.range(&0, &0).unwrap().len(), 300);
    assert!(t.remove_entry(0, 150).unwrap());
    assert_eq!(t.range(&0, &0).unwrap().len(), 299);
}
