//! FTS phrase positions must survive multiple index flushes.
//!
//! Positions live under one key per term (shard 0xFE). v0.16.0 rewrote
//! that key on every flush with only the current batch's docs, so a quoted
//! phrase query silently stopped matching every document flushed earlier
//! while plain term MATCH (append-only data shards) still found them.

use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        motedb::QueryResult::Select { rows, .. } => rows,
        other => panic!("unexpected: {other:?}"),
    }
}

fn sorted(mut v: Vec<u64>) -> Vec<u64> {
    v.sort_unstable();
    v
}

fn setup(dir: &TempDir) -> Database {
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE d (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    db.execute("CREATE TEXT INDEX d_body ON d (body)").unwrap();
    db
}

#[test]
fn phrase_search_sees_docs_from_every_flush() {
    let dir = TempDir::new().unwrap();
    let db = setup(&dir);

    // Six flush rounds: enough to cross the ≥5-shard consolidation
    // threshold for the two phrase terms as well.
    let mut expected: Vec<u64> = Vec::new();
    let mut next_id = 1u64;
    for round in 0..6 {
        for _ in 0..10 {
            db.execute(&format!(
                "INSERT INTO d VALUES ({next_id}, 'round {round} machine learning doc {next_id}')"
            ))
            .unwrap();
            expected.push(next_id);
            next_id += 1;
        }
        // decoy: both words, wrong order / not adjacent
        db.execute(&format!(
            "INSERT INTO d VALUES ({next_id}, 'learning about machine tools')"
        ))
        .unwrap();
        next_id += 1;
        db.checkpoint().unwrap();
    }
    // plus a few unflushed (pending) docs
    for _ in 0..3 {
        db.execute(&format!(
            "INSERT INTO d VALUES ({next_id}, 'pending machine learning {next_id}')"
        ))
        .unwrap();
        expected.push(next_id);
        next_id += 1;
    }

    let got = sorted(db.text_search_phrase("d_body", "machine learning").unwrap());
    assert_eq!(got, expected, "phrase hits must cover every flush round");

    // Same through SQL (quoted = phrase).
    let sql_ids: Vec<u64> = rows(
        &db,
        "SELECT id FROM d WHERE MATCH(body, '\"machine learning\"') ORDER BY id",
    )
    .into_iter()
    .map(|r| match r[0] {
        Value::Integer(i) => i as u64,
        ref o => panic!("bad id {o:?}"),
    })
    .collect();
    assert_eq!(sql_ids, expected);

    // Plain (non-phrase) MATCH also sees the decoys.
    let all_match = rows(
        &db,
        "SELECT id FROM d WHERE MATCH(body, 'machine learning')",
    );
    assert_eq!(all_match.len(), expected.len() + 6);

    // Survives close + reopen.
    drop(db);
    let db = Database::open(dir.path()).unwrap();
    let got = sorted(db.text_search_phrase("d_body", "machine learning").unwrap());
    assert_eq!(got, expected, "after reopen");
}

#[test]
fn phrase_positions_follow_updates_across_flushes() {
    let dir = TempDir::new().unwrap();
    let db = setup(&dir);

    db.execute("INSERT INTO d VALUES (1, 'alpha bravo charlie')")
        .unwrap();
    db.execute("INSERT INTO d VALUES (2, 'bravo alpha charlie')")
        .unwrap();
    db.checkpoint().unwrap();

    // doc 1 loses the phrase, doc 2 gains it — in a later flush.
    db.execute("UPDATE d SET body = 'alpha charlie bravo' WHERE id = 1")
        .unwrap();
    db.execute("UPDATE d SET body = 'charlie alpha bravo' WHERE id = 2")
        .unwrap();
    db.checkpoint().unwrap();

    assert_eq!(
        sorted(db.text_search_phrase("d_body", "alpha bravo").unwrap()),
        vec![2]
    );

    // A deleted doc must not come back through stale positions.
    db.execute("DELETE FROM d WHERE id = 2").unwrap();
    db.checkpoint().unwrap();
    assert!(db
        .text_search_phrase("d_body", "alpha bravo")
        .unwrap()
        .is_empty());
}

/// P8: after an UPDATE changes a term's frequency, BM25 must score with the
/// NEW tf — ascending shard order + first-wins merge let the oldest shard's
/// stale tf win permanently (and consolidation cemented it).
#[test]
fn bm25_uses_fresh_tf_after_update() {
    let dir = TempDir::new().unwrap();
    let db = setup(&dir);
    db.execute("INSERT INTO d VALUES (1, 'apple apple apple pie')")
        .unwrap();
    db.execute("INSERT INTO d VALUES (2, 'apple pie')").unwrap();
    db.checkpoint().unwrap();
    // doc1 now has IDENTICAL text to doc2 → identical tf=1, identical length.
    db.execute("UPDATE d SET body = 'apple pie' WHERE id = 1")
        .unwrap();
    db.checkpoint().unwrap();

    let rows = rows(
        &db,
        "SELECT id, bm25(body) AS s FROM d WHERE MATCH(body, 'apple')",
    );
    assert_eq!(rows.len(), 2);
    let s1 = match &rows[0][1] {
        Value::Float(f) => *f,
        o => panic!("bad score {o:?}"),
    };
    let s2 = match &rows[1][1] {
        Value::Float(f) => *f,
        o => panic!("bad score {o:?}"),
    };
    assert_eq!(
        s1, s2,
        "identical documents must score identically (stale tf: {rows:?})"
    );
}

/// Exact df: a doc re-posted by a later UPDATE shard must count ONCE toward
/// the term's document frequency. Two identical docs + one update that keeps
/// the term ⇒ every doc's BM25 score must be stable across the flush that
/// re-posts it (df inflation moved everyone's IDF before).
#[test]
fn df_counts_reposted_docs_once() {
    let dir = TempDir::new().unwrap();
    let db = setup(&dir);
    db.execute("INSERT INTO d VALUES (1, 'apple pie')").unwrap();
    db.execute("INSERT INTO d VALUES (2, 'apple tart')")
        .unwrap();
    db.checkpoint().unwrap();
    let before = rows(
        &db,
        "SELECT id, bm25(body) FROM d WHERE MATCH(body, 'apple')",
    );

    // Re-post doc 1 with the SAME apple occurrence (tf unchanged) — its
    // posting now lives in two shards.
    db.execute("UPDATE d SET body = 'apple pie' WHERE id = 1")
        .unwrap();
    db.checkpoint().unwrap();
    let after = rows(
        &db,
        "SELECT id, bm25(body) FROM d WHERE MATCH(body, 'apple')",
    );

    assert_eq!(before.len(), after.len());
    for (b, a) in before.iter().zip(after.iter()) {
        let (bb, aa) = (format!("{:?}", b[1]), format!("{:?}", a[1]));
        assert_eq!(
            bb, aa,
            "scores must not shift when a doc is re-posted: {before:?} vs {after:?}"
        );
    }
}
