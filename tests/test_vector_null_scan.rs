//! Regression: NULL vector cells used to poison whole-column scans.
//!
//! `read_vectors`/`read_spatial` skip NULL (and deleted) rows; the callers
//! in store.rs / merge.rs / crud.rs re-attached values with a forward-only
//! linear merge keyed on row id. A NULL cell's key is absent from the
//! decoded list, so the search cursor ran to the end — every LATER row
//! decoded as None. `SELECT emb FROM t` with no WHERE returned NULL for the
//! entire column whenever any row had a NULL vector, and compaction
//! (merge.rs) rewrote that loss permanently. Fixed by
//! `ColumnarSSTable::scatter_by_row_id` (exact hash scatter).

use motedb::types::ArcVec;
use motedb::Database;

#[test]
fn null_vector_does_not_poison_full_column_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path().join("nv.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, emb VECTOR(3), note TEXT)")
        .unwrap();

    // Row 0 is NULL (the worst case for the old forward-merge), rows 1..3
    // carry vectors, row 2 is NULL again in the middle.
    db.execute("INSERT INTO t VALUES (0, NULL, 'null-first')")
        .unwrap();
    for i in 1..=3i64 {
        if i == 2 {
            db.execute("INSERT INTO t VALUES (2, NULL, 'mid-null')")
                .unwrap();
        } else {
            db.execute(&format!(
                "INSERT INTO t VALUES ({i}, [{i}.0, 0.5, -1.0], 'v{i}')"
            ))
            .unwrap();
        }
    }
    db.checkpoint().unwrap(); // rows land in columnar segments

    // The bug: unfiltered full scan (scan_projected_filtered / pvector path).
    let rows = db.query("SELECT emb FROM t").unwrap();
    let mut non_null = 0;
    for (i, r) in rows.iter().enumerate() {
        match &r[0] {
            motedb::types::Value::Vector(v) => {
                assert_eq!(v.0.as_ref()[0], i as f32, "row {i} vector value");
                non_null += 1;
            }
            motedb::types::Value::Null => assert!(i == 0 || i == 2, "unexpected NULL at {i}"),
            other => panic!("row {i}: {other:?}"),
        }
    }
    assert_eq!(non_null, 2, "rows 1 and 3 must survive row 0's NULL");
    drop(db);
}

#[test]
fn null_vector_survives_checkpoint_and_rescan() {
    // The compaction path (merge.rs) had the same merge; verify vectors
    // survive a checkpoint cycle when NULLs interleave.
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("cp.mote");
    let db = Database::create(&db_path).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, emb VECTOR(2))")
        .unwrap();
    for i in 0..200i64 {
        let sql = if i % 4 == 0 {
            format!("INSERT INTO t VALUES ({i}, NULL)")
        } else {
            format!("INSERT INTO t VALUES ({i}, [{i}.0, 1.0])")
        };
        db.execute(&sql).unwrap();
    }
    db.checkpoint().unwrap();

    let expect_non_null = 150; // 200 - 50 NULLs
    let count = &db
        .query("SELECT COUNT(*) FROM t WHERE emb IS NOT NULL")
        .unwrap()[0][0];
    assert_eq!(count, &motedb::types::Value::Integer(expect_non_null));

    // Reopen — the on-disk columnar segments must decode identically.
    drop(db);
    let db = Database::open(&db_path).unwrap();
    let count = &db
        .query("SELECT COUNT(*) FROM t WHERE emb IS NOT NULL")
        .unwrap()[0][0];
    assert_eq!(count, &motedb::types::Value::Integer(expect_non_null));
    // And the vectors are the right values, not just non-NULL.
    let r = db.query("SELECT emb FROM t WHERE id = 199").unwrap();
    match &r[0][0] {
        motedb::types::Value::Vector(v) => assert_eq!(v.0.as_ref(), &[199.0f32, 1.0]),
        other => panic!("row 199: {other:?}"),
    }
    let _ = ArcVec::new(vec![]); // keep the import honest for future edits
}
