//! PK-change semantics regression tests (BUG #33/#34 family).
//!
//! UPDATE changing a primary key physically MOVES the row: tombstone at the
//! old composite key + append at the new PK-derived key. Everything keyed by
//! row_id must follow:
//! - BUG #33: row_cache kept the new row under the OLD row_id slot — one row
//!   visible under two ids via get_row/get_table_row.
//! - BUG #34: secondary indexes (column/FTS/vector/octree) kept their entries
//!   keyed to the old row_id and pk_cache mapped the new PK to the old
//!   row_id — after the ghost cache was fixed, the row silently vanished
//!   from every index-driven lookup.

use motedb::types::{Tensor, Value};
use motedb::Database;
use tempfile::TempDir;

fn rows(r: motedb::StreamingQueryResult) -> Vec<Vec<Value>> {
    use motedb::QueryResult;
    match r.materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        _ => panic!("expected select"),
    }
}

#[test]
fn no_ghost_row_cache_entry_after_pk_change() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (5, 'a')").unwrap();
    // warm the cache with the original row, then move the PK
    let _ = db.get_row("t", 5).unwrap();
    db.execute("UPDATE t SET id = 7 WHERE id = 5").unwrap();
    assert!(
        db.get_row("t", 5).unwrap().is_none(),
        "row must be GONE at the old row_id (no ghost cache entry)"
    );
    let live = db
        .get_row("t", 7)
        .unwrap()
        .expect("row must live at new id");
    assert_eq!(live[0], Value::Integer(7));
}

#[test]
fn secondary_indexes_follow_pk_change() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, body TEXT, cat INT)")
        .unwrap();
    for i in 0..5i64 {
        db.execute(&format!(
            "INSERT INTO t VALUES ({i}, 'doc{i} unique{i}', 100)"
        ))
        .unwrap();
    }
    db.execute("CREATE TEXT INDEX idx_body ON t(body)").unwrap();
    db.execute("CREATE INDEX idx_cat ON t(cat)").unwrap();
    db.wait_for_indexes_ready();

    // warm the row cache, then move the row
    let _ = db.get_row("t", 3).unwrap();
    db.execute("UPDATE t SET id = 50 WHERE id = 3").unwrap();

    let hits = db.text_search_ranked("idx_body", "unique3", 5).unwrap();
    assert_eq!(hits.len(), 1, "FTS must find the moved doc, got {hits:?}");
    assert_eq!(hits[0].0, 50, "FTS hit must be keyed to the NEW row_id");

    let col = rows(
        db.execute("SELECT id FROM t WHERE cat = 100 AND id = 50")
            .unwrap(),
    );
    assert_eq!(col.len(), 1, "column index must resolve the moved row");

    assert_eq!(
        rows(db.execute("SELECT body FROM t WHERE id = 50").unwrap()).len(),
        1
    );
    assert_eq!(
        rows(db.execute("SELECT body FROM t WHERE id = 3").unwrap()).len(),
        0
    );
}

#[test]
fn vector_index_follows_pk_change() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, emb VECTOR(8))")
        .unwrap();
    for i in 0..10i64 {
        let mut v = vec![0.0f32; 8];
        v[i as usize % 8] = 10.0;
        v[7] = i as f32 * 0.1;
        db.insert_row("t", vec![Value::Integer(i), Value::tensor(Tensor::new(v))])
            .unwrap();
    }
    db.execute("CREATE VECTOR INDEX idx_emb ON t(emb)").unwrap();
    db.wait_for_indexes_ready();

    let old = db.get_row("t", 3).unwrap().unwrap();
    let mut updated = old;
    updated[0] = Value::Integer(50);
    db.update_row("t", 3, updated).unwrap();
    db.checkpoint().unwrap();

    let mut q = vec![0.0f32; 8];
    q[3] = 10.0;
    q[7] = 0.3;
    let hits = db.vector_search("idx_emb", &q, 3).unwrap();
    assert!(
        !hits.is_empty(),
        "vector search must find results after PK change"
    );
    assert_eq!(hits[0].0, 50, "top-1 must be the moved row, got {hits:?}");
    assert!(
        hits.iter().all(|(id, _)| *id != 3),
        "old row_id must not leak, got {hits:?}"
    );
}

#[test]
fn pk_change_survives_checkpoint_and_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, body TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (3, 'needle')").unwrap();
        db.execute("CREATE TEXT INDEX idx_body ON t(body)").unwrap();
        db.wait_for_indexes_ready();
        db.execute("UPDATE t SET id = 9 WHERE id = 3").unwrap();
        db.checkpoint().unwrap();
    }
    let db = Database::open(dir.path()).unwrap();
    let q = rows(db.execute("SELECT body FROM t WHERE id = 9").unwrap());
    assert_eq!(q.len(), 1, "PK query must work after reopen");
    let hits = db.text_search_ranked("idx_body", "needle", 5).unwrap();
    assert_eq!(hits.len(), 1, "FTS must work after reopen");
    assert_eq!(hits[0].0, 9);
}
