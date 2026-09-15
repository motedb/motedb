//! Round 10 regressions: vector top-k visibility across UPDATE/DELETE.
//!
//! Three state-machine holes in the `ORDER BY emb <-> ?` columnar fast path
//! (all found by driving the store through real flush timings):
//! 1. Duplicate keys in the write buffer (INSERT's original + UPDATE's new
//!    version) resolved FIRST-wins — the scan kept the OLD vector.
//! 2. A buffered tombstone (DELETE not yet flushed) was skipped silently —
//!    older segments' live versions resurrected the deleted row (ghost),
//!    the row fetch then dropped it, and the query returned fewer rows
//!    than LIMIT.
//! 3. A tombstone-only flushed segment has a NULL-placeholder vector column
//!    (dim 0 ≠ query dim); the whole segment was skipped — its tombstones
//!    never claimed keys and the ghost came back after background flush.
//!
//! These tests force each state deterministically under for_testing (no
//! background threads): checkpoint() flushes rows into segments at chosen
//! moments, leaving tombstones/new versions in the buffer.

use motedb::{DBConfig, Database};
use tempfile::TempDir;

fn db() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    (dir, db)
}

fn topk(db: &Database, k: usize) -> Vec<i64> {
    db.query(&format!(
        "SELECT id FROM vc ORDER BY emb <-> [0.1, 0, 0] ASC LIMIT {k}"
    ))
    .unwrap()
    .iter()
    .map(|r| match &r[0] {
        motedb::types::Value::Integer(i) => *i,
        o => panic!("{o:?}"),
    })
    .collect()
}

fn seed(db: &Database) {
    db.execute("CREATE TABLE vc (id INT PRIMARY KEY, emb VECTOR(3))")
        .unwrap();
    for i in 1..=5i64 {
        db.execute(&format!(
            "INSERT INTO vc (id, emb) VALUES ({i}, [{i}, 0, 0])"
        ))
        .unwrap();
    }
}

/// UPDATE while the INSERT is still unflushed: the buffer holds two versions
/// of the key — the NEW one must win (was: first/old won).
#[test]
fn update_in_buffer_new_version_wins_immediately() {
    let (_d, db) = db();
    seed(&db);
    db.execute("UPDATE vc SET emb = [0.1, 0, 0] WHERE id = 5")
        .unwrap();
    let got = topk(&db, 2);
    assert_eq!(got, vec![5, 1], "updated vector must be visible at once");
}

/// INSERT flushed to a segment, UPDATE's new version still buffered: new
/// vector wins; after checkpoint the segment version must not resurrect.
#[test]
fn update_after_checkpoint_still_new_version() {
    let (_d, db) = db();
    seed(&db);
    db.checkpoint().unwrap();
    db.execute("UPDATE vc SET emb = [0.1, 0, 0] WHERE id = 5")
        .unwrap();
    assert_eq!(topk(&db, 2), vec![5, 1]);
    db.checkpoint().unwrap();
    assert_eq!(
        topk(&db, 2),
        vec![5, 1],
        "checkpoint must not resurrect the old vector"
    );
}

/// DELETE with the tombstone still in the write buffer: no ghost row, and
/// the query must return a FULL result set (the ghost used to consume a
/// top-k slot and get dropped by the row fetch).
#[test]
fn delete_with_buffered_tombstone_no_ghost_full_results() {
    let (_d, db) = db();
    seed(&db);
    db.execute("UPDATE vc SET emb = [0.1, 0, 0] WHERE id = 5")
        .unwrap();
    db.execute("DELETE FROM vc WHERE id = 5").unwrap();
    let got = topk(&db, 4);
    assert_eq!(
        got,
        vec![1, 2, 3, 4],
        "deleted row must vanish, LIMIT k = k rows"
    );
}

/// DELETE tombstone flushed into its own (NULL-placeholder) segment: the
/// dim-0 vector column must not cause the segment's tombstones to be
/// skipped — the deleted row stays gone.
#[test]
fn delete_tombstone_only_segment_still_gone() {
    let (_d, db) = db();
    seed(&db);
    db.checkpoint().unwrap(); // inserts live in a segment
    db.execute("DELETE FROM vc WHERE id = 5").unwrap();
    db.checkpoint().unwrap(); // tombstone flushed to its own segment
    let got = topk(&db, 4);
    assert_eq!(got, vec![1, 2, 3, 4]);
    // ...and again after reopen
    drop(db);
    let db2 = Database::open(_d.path()).unwrap();
    assert_eq!(topk(&db2, 4), vec![1, 2, 3, 4]);
}

/// UPDATE→DELETE→re-INSERT the same id: the re-inserted row must be visible
/// with its newest vector (exercises the version chain end-to-end).
#[test]
fn delete_then_reinsert_visible() {
    let (_d, db) = db();
    seed(&db);
    db.execute("UPDATE vc SET emb = [9, 9, 9] WHERE id = 3")
        .unwrap();
    db.execute("DELETE FROM vc WHERE id = 3").unwrap();
    db.execute("INSERT INTO vc (id, emb) VALUES (3, [0.1, 0, 0])")
        .unwrap();
    let got = topk(&db, 2);
    assert_eq!(
        got,
        vec![3, 1],
        "re-inserted row must rank with its new vector"
    );
}
