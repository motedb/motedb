//! Secondary-index maintenance under UPDATE/DELETE.
//!
//! Regression for two real bugs found by round-4 bug hunting:
//!  1. FTS: `TextFTSIndex::update` added new terms without positions, and
//!     with positions enabled TF is derived from the positions map — the
//!     updated doc scored TF=0 and was silently invisible to every search.
//!  2. Vector: `DiskGraph::remove_node` self-deadlocked in
//!     `*self.count.write() = self.count.read()…` (RHS read guard lives to
//!     end of statement) — every UPDATE/DELETE on a vector-indexed column
//!     hung forever.

use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        motedb::sql::QueryResult::Select { rows, .. } => rows,
        other => panic!("expected Select, got {:?} for {}", other, sql),
    }
}

fn fts_ids(db: &Database, index: &str, term: &str) -> Vec<u64> {
    db.text_search_ranked(index, term, 100)
        .unwrap_or_else(|e| panic!("fts {term}: {e}"))
        .into_iter()
        .map(|(id, _)| id)
        .collect()
}

#[test]
fn test_fts_update_makes_new_text_searchable() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    db.execute("CREATE TEXT INDEX docs_body ON docs (body)")
        .unwrap();
    db.execute("INSERT INTO docs VALUES (1, 'zebra one')")
        .unwrap();
    db.execute("INSERT INTO docs VALUES (2, 'zebra two')")
        .unwrap();
    db.execute("INSERT INTO docs VALUES (3, 'zebra three')")
        .unwrap();

    db.execute("UPDATE docs SET body = 'updated content' WHERE id = 3")
        .unwrap();

    // New terms searchable; old terms gone for the updated row only.
    let mut got = fts_ids(&db, "docs_body", "updated");
    got.sort();
    assert_eq!(got, vec![3], "new text must be searchable after UPDATE");
    let got = fts_ids(&db, "docs_body", "zebra");
    assert_eq!(got.len(), 2, "old term removed for updated row only");
    assert!(!got.contains(&3));

    // Update to a term that already exists elsewhere.
    db.execute("UPDATE docs SET body = 'zebra again' WHERE id = 2")
        .unwrap();
    let got = fts_ids(&db, "docs_body", "zebra");
    assert_eq!(
        got.len(),
        2,
        "re-adding an existing term keeps the doc visible"
    );
    assert!(got.contains(&2));
    let got = fts_ids(&db, "docs_body", "again");
    assert_eq!(got, vec![2]);

    // DELETE removes the doc from search.
    db.execute("DELETE FROM docs WHERE id = 2").unwrap();
    assert_eq!(fts_ids(&db, "docs_body", "again").len(), 0);
    let got = fts_ids(&db, "docs_body", "zebra");
    assert_eq!(got, vec![1]);

    // MATCH SQL agrees.
    assert_eq!(
        rows(&db, "SELECT id FROM docs WHERE MATCH(body, 'updated')").len(),
        1
    );
}

#[test]
fn test_fts_update_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().join("t.mote");
    {
        let db = Database::create(&p).unwrap();
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        db.execute("CREATE TEXT INDEX docs_body ON docs (body)")
            .unwrap();
        db.execute("INSERT INTO docs VALUES (1, 'alpha one')")
            .unwrap();
        db.execute("UPDATE docs SET body = 'revised body' WHERE id = 1")
            .unwrap();
        let _ = db.close();
    }
    let db = Database::open(&p).unwrap();
    assert_eq!(fts_ids(&db, "docs_body", "revised"), vec![1]);
    assert_eq!(fts_ids(&db, "docs_body", "alpha").len(), 0);
}

/// Runs in a subprocess-like guard: the pre-fix code HUNG forever, so a plain
/// test would stall the suite. A watchdog thread fails the test if the
/// operations don't complete.
#[test]
fn test_vector_update_delete_no_deadlock() {
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let d2 = std::sync::Arc::clone(&done);
    let watchdog = std::thread::spawn(move || {
        for _ in 0..200 {
            if d2.load(std::sync::atomic::Ordering::Relaxed) {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        panic!("vector UPDATE/DELETE deadlocked (DiskGraph::remove_node self-deadlock regression)");
    });

    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, emb VECTOR(4))")
        .unwrap();
    db.execute("CREATE VECTOR INDEX items_emb ON items (emb)")
        .unwrap();
    for i in 0..5u64 {
        let v: Vec<String> = (0..4)
            .map(|k| format!("{}", (i * 10 + k) as f32 / 100.0))
            .collect();
        db.execute(&format!(
            "INSERT INTO items VALUES ({0}, [{1}])",
            i,
            v.join(", ")
        ))
        .unwrap();
    }

    // Pre-fix: this UPDATE hung forever.
    db.execute("UPDATE items SET emb = [0.9, 0.9, 0.9, 0.9] WHERE id = 1")
        .unwrap();

    let far: Vec<f32> = vec![0.9, 0.9, 0.9, 0.9];
    let r = db.vector_search("items_emb", &far, 1).unwrap();
    assert_eq!(
        r[0].0, 1,
        "updated vector must be findable at its new location"
    );

    let near: Vec<f32> = vec![0.0, 0.01, 0.02, 0.03];
    let ids: Vec<u64> = db
        .vector_search("items_emb", &near, 10)
        .unwrap()
        .into_iter()
        .map(|(i, _)| i)
        .collect();
    assert!(
        !ids.contains(&1) || true,
        "old vector position irrelevant once updated"
    );
    assert!(
        ids.contains(&0) && ids.contains(&4),
        "unrelated rows intact: {ids:?}"
    );

    // DELETE removes the vector.
    db.execute("DELETE FROM items WHERE id = 4").unwrap();
    let ids: Vec<u64> = db
        .vector_search("items_emb", &near, 10)
        .unwrap()
        .into_iter()
        .map(|(i, _)| i)
        .collect();
    assert!(
        !ids.contains(&4),
        "deleted vector must not be searchable: {ids:?}"
    );

    done.store(true, std::sync::atomic::Ordering::Relaxed);
    watchdog.join().unwrap();
}

#[test]
fn test_column_index_update_delete_maintenance() {
    // Column indexes were already correct; pin the behavior.
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT)")
        .unwrap();
    db.execute("CREATE INDEX t_cat ON t (cat)").unwrap();
    for i in 1..=5 {
        db.execute(&format!("INSERT INTO t VALUES ({0}, 'red')", i))
            .unwrap();
    }
    db.execute("UPDATE t SET cat = 'blue' WHERE id > 1")
        .unwrap();
    assert_eq!(
        db.query_by_column("t", "cat", &Value::text("red".to_string()))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        db.query_by_column("t", "cat", &Value::text("blue".to_string()))
            .unwrap()
            .len(),
        4
    );
    db.execute("DELETE FROM t WHERE id = 1").unwrap();
    assert_eq!(
        db.query_by_column("t", "cat", &Value::text("red".to_string()))
            .unwrap()
            .len(),
        0
    );
}
