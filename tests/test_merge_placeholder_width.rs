//! Repro: DATE column values are destroyed by col_segment compaction.
//!
//! `encode_column_raw` (storage/col_segment/store.rs) has no `Value::Date`
//! arm, so every date read back during merge falls into the stored-NULL
//! arm — and that arm emits a 2-byte placeholder for a column whose fixed
//! layout is 8 bytes/row.
//!
//! Regression test for the fix (encode_column_raw / null_placeholder).

use motedb::storage::col_segment::ColSegmentStore;
use motedb::types::{ColumnType, Date, Value};
use tempfile::TempDir;

#[test]
fn date_column_survives_compaction() {
    let dir = TempDir::new().unwrap();
    let store = ColSegmentStore::create(
        dir.path(),
        "t",
        vec![ColumnType::Integer, ColumnType::Date],
        None,
    )
    .unwrap();

    for start in [0u64, 2, 4] {
        store
            .append_rows(&[
                (
                    start + 1,
                    100,
                    vec![
                        Value::Integer((start + 1) as i64),
                        Value::Date(Date::from_days(19753 + start as i32)),
                    ],
                ),
                (
                    start + 2,
                    100,
                    vec![Value::Integer((start + 2) as i64), Value::Null],
                ),
            ])
            .unwrap();
        store.flush_buffer().unwrap();
    }
    assert_eq!(store.segment_count(), 3);

    let before: Vec<Value> = (1..=6).map(|k| store.get(k).unwrap()[1].clone()).collect();
    eprintln!("before compaction: {before:?}");

    store.compact_once().unwrap();
    assert_eq!(store.segment_count(), 1);

    let after: Vec<Value> = (1..=6).map(|k| store.get(k).unwrap()[1].clone()).collect();
    eprintln!("after  compaction: {after:?}");

    assert_eq!(before, after, "DATE values must survive compaction");
}

#[test]
fn sql_date_survives_checkpoint_and_merge() {
    let dir = TempDir::new().unwrap();
    let db = motedb::Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE ev (id INT PRIMARY KEY, d DATE)")
        .unwrap();

    for i in 1..=4 {
        db.execute(&format!("INSERT INTO ev VALUES ({i}, '2024-01-0{i}')"))
            .unwrap();
        db.checkpoint().unwrap();
    }

    let rows = match db
        .execute("SELECT id, d FROM ev ORDER BY id")
        .unwrap()
        .materialize()
        .unwrap()
    {
        motedb::QueryResult::Select { rows, .. } => rows,
        other => panic!("unexpected: {other:?}"),
    };
    eprintln!("rows after 4 checkpoints: {rows:?}");

    for (i, row) in rows.iter().enumerate() {
        assert!(
            !matches!(row[1], Value::Null),
            "row {} lost its DATE after checkpoint/merge: {row:?}",
            i + 1
        );
    }
    assert_eq!(rows.len(), 4);
}

/// Control: a single checkpoint (one segment, no merge) keeps the dates.
#[test]
fn sql_date_control_single_checkpoint() {
    let dir = TempDir::new().unwrap();
    let db = motedb::Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE ev (id INT PRIMARY KEY, d DATE)")
        .unwrap();
    db.execute("INSERT INTO ev VALUES (1, '2024-01-01')")
        .unwrap();
    db.execute("INSERT INTO ev VALUES (2, '2024-01-02')")
        .unwrap();
    db.checkpoint().unwrap();

    let rows = match db
        .execute("SELECT id, d FROM ev ORDER BY id")
        .unwrap()
        .materialize()
        .unwrap()
    {
        motedb::QueryResult::Select { rows, .. } => rows,
        other => panic!("unexpected: {other:?}"),
    };
    eprintln!("rows after 1 checkpoint: {rows:?}");
    for row in &rows {
        assert!(!matches!(row[1], Value::Null), "control failed: {row:?}");
    }
}

fn select_rows(db: &motedb::Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        motedb::QueryResult::Select { rows, .. } => rows,
        other => panic!("unexpected: {other:?}"),
    }
}

/// Tiered tail merge keeps tombstones and writes a NULL placeholder per
/// column for each one; a BLOB column's placeholder must be the u32 length
/// prefix the builder re-parses, or every later blob in the segment
/// desyncs. Base segment of 200 rows, then two small tail segments holding
/// inserts + deletes so `compact_small_tail` (tail*2 < base) fires.
#[test]
fn blob_column_survives_tiered_merge_with_tombstones() {
    let dir = TempDir::new().unwrap();
    let db = motedb::Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE b (id INT PRIMARY KEY, payload BLOB)")
        .unwrap();

    let blob_hex = |i: i64| -> String {
        // distinct, variable-length payloads so a misparse is visible
        (0..(8 + (i % 7) as usize))
            .map(|k| format!("{:02X}", (i as usize * 31 + k * 7) % 256))
            .collect()
    };

    for i in 0..200 {
        db.execute(&format!("INSERT INTO b VALUES ({i}, X'{}')", blob_hex(i)))
            .unwrap();
    }
    db.checkpoint().unwrap();

    for i in 200..205 {
        db.execute(&format!("INSERT INTO b VALUES ({i}, X'{}')", blob_hex(i)))
            .unwrap();
    }
    for i in [3, 50, 199] {
        db.execute(&format!("DELETE FROM b WHERE id = {i}"))
            .unwrap();
    }
    db.checkpoint().unwrap();

    for i in 205..210 {
        db.execute(&format!("INSERT INTO b VALUES ({i}, X'{}')", blob_hex(i)))
            .unwrap();
    }
    for i in [7, 201] {
        db.execute(&format!("DELETE FROM b WHERE id = {i}"))
            .unwrap();
    }
    db.checkpoint().unwrap();

    // Query entry triggers prepare_for_query → tiered merge (keep_tombstones).
    let rows = select_rows(&db, "SELECT id, payload FROM b ORDER BY id");
    let rows2 = select_rows(&db, "SELECT id, payload FROM b ORDER BY id");
    assert_eq!(rows, rows2, "results must be stable across the merge");

    let deleted = [3i64, 7, 50, 199, 201];
    assert_eq!(rows.len(), 210 - deleted.len());
    for row in &rows {
        let id = match row[0] {
            Value::Integer(i) => i,
            ref o => panic!("bad id {o:?}"),
        };
        assert!(!deleted.contains(&id), "deleted id {id} resurrected");
        let expect: Vec<u8> = (0..blob_hex(id).len() / 2)
            .map(|k| u8::from_str_radix(&blob_hex(id)[k * 2..k * 2 + 2], 16).unwrap())
            .collect();
        match &row[1] {
            Value::Blob(b) => assert_eq!(b.as_slice(), expect.as_slice(), "blob for id {id}"),
            o => panic!("id {id}: expected Blob, got {o:?}"),
        }
    }
}

/// `ALTER TABLE … ADD COLUMN d DATE DEFAULT '2024-01-01'` on a table that
/// already has segments rewrites them with the default backfilled. The
/// literal arrives as Text; it must be encoded in the DATE column's 8-byte
/// layout (parsed), never as TEXT `[len][bytes]`.
#[test]
fn alter_add_date_column_with_text_default_backfills_old_segments() {
    let dir = TempDir::new().unwrap();
    let db = motedb::Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 1..=3 {
        db.execute(&format!("INSERT INTO t VALUES ({i}, {})", i * 10))
            .unwrap();
    }
    db.checkpoint().unwrap();

    db.execute("ALTER TABLE t ADD COLUMN d DATE DEFAULT '2024-01-01'")
        .unwrap();
    db.execute("INSERT INTO t VALUES (4, 40, '2024-02-02')")
        .unwrap();
    db.checkpoint().unwrap();
    db.checkpoint().unwrap();

    let rows = select_rows(&db, "SELECT id, v, d FROM t ORDER BY id");
    eprintln!("after ALTER ADD COLUMN DATE DEFAULT: {rows:?}");
    assert_eq!(rows.len(), 4);
    let d0 = Date::parse("2024-01-01").unwrap();
    for row in &rows[..3] {
        assert_eq!(
            row[2],
            Value::Date(d0),
            "backfilled default for {:?}",
            row[0]
        );
    }
    assert_eq!(rows[3][2], Value::Date(Date::parse("2024-02-02").unwrap()));
    // v column must be untouched by the rewrite
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row[1], Value::Integer((i as i64 + 1) * 10));
    }
}

/// Same tiered-merge tombstone path, driven at the store level so the
/// `compact_small_tail` (keep_tombstones) branch is guaranteed to run.
#[test]
fn store_blob_tombstone_placeholder_keeps_column_aligned() {
    use motedb::types::ArcBytes;
    let dir = TempDir::new().unwrap();
    let store = ColSegmentStore::create(
        dir.path(),
        "b",
        vec![ColumnType::Integer, ColumnType::Blob],
        None,
    )
    .unwrap();
    let blob = |k: u64| -> Vec<u8> {
        (0..(8 + (k % 7) as usize))
            .map(|j| ((k * 31 + j as u64 * 7) % 256) as u8)
            .collect()
    };
    let row = |k: u64| {
        vec![
            Value::Integer(k as i64),
            Value::Blob(ArcBytes::new(blob(k))),
        ]
    };

    let base: Vec<(u64, u64, Vec<Value>)> = (0..200u64).map(|k| (k, 100, row(k))).collect();
    store.append_rows(&base).unwrap();
    store.flush_buffer().unwrap();

    store
        .append_rows(&(200..205u64).map(|k| (k, 200, row(k))).collect::<Vec<_>>())
        .unwrap();
    for k in [3u64, 50, 199] {
        store.append_tombstone(k, 201).unwrap();
    }
    store.flush_buffer().unwrap();

    store
        .append_rows(&(205..210u64).map(|k| (k, 300, row(k))).collect::<Vec<_>>())
        .unwrap();
    for k in [7u64, 201] {
        store.append_tombstone(k, 301).unwrap();
    }
    store.flush_buffer().unwrap();
    assert_eq!(store.segment_count(), 3);

    store.prepare_for_query().unwrap();
    assert_eq!(
        store.segment_count(),
        2,
        "tiered merge folded the two tail segments"
    );

    let deleted = [3u64, 7, 50, 199, 201];
    for k in 0..210u64 {
        let got = store.get(k);
        if deleted.contains(&k) {
            assert!(got.is_none(), "deleted key {k} resurrected: {got:?}");
        } else {
            let got = got.unwrap_or_else(|| panic!("key {k} missing after merge"));
            assert_eq!(
                got[1],
                Value::Blob(ArcBytes::new(blob(k))),
                "blob for key {k}"
            );
        }
    }
    let live: Vec<u64> = store.scan().map(|(k, _, _)| k).collect();
    assert_eq!(live.len(), 210 - deleted.len());
    assert!(live.iter().all(|k| !deleted.contains(k)));
}
