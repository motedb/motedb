//! Hybrid-search primitives (v0.13.1): RRF table function, bm25() ranked
//! ORDER BY, snippet(). The RRF test computes the expected fusion IN THE
//! TEST from the two subqueries' raw outputs — a differential check of the
//! engine's fusion against the reference formula.
use motedb::{types::Value, Database};
use tempfile::TempDir;

fn one(db: &Database, sql: &str) -> Value {
    let r = db.execute(sql).unwrap().materialize().unwrap();
    let (_, rows) = r.select_rows().unwrap();
    rows[0][0].clone()
}

fn col(db: &Database, sql: &str) -> Vec<Value> {
    let r = db.execute(sql).unwrap().materialize().unwrap();
    let (_, rows) = r.select_rows().unwrap();
    rows.iter().map(|r| r[0].clone()).collect()
}

/// 12 docs: ids 1..=6 contain 'grasp' (lexical set), ids 7..=12 don't but
/// their embeddings sit near the probe vector (semantic set).
fn seed(db: &Database) {
    db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT, emb VECTOR(4))")
        .unwrap();
    db.execute("CREATE TEXT INDEX docs_body ON docs(body)")
        .unwrap();
    for i in 1..=6i64 {
        db.execute(&format!(
            "INSERT INTO docs VALUES ({i}, 'grasp failure case {i} with grip', [{i}.0, 0.0, 0.0, 0.0])"
        ))
        .unwrap();
    }
    for i in 7..=12i64 {
        // Bodies avoid the query terms; embeddings approach the probe.
        let d = (12 - i) as f64; // id 12 closest to probe [10,0,0,0]
        db.execute(&format!(
            "INSERT INTO docs VALUES ({i}, 'unrelated note {i} about transport', [{}., 0.0, 0.0, 0.0])",
            10.0 - d
        ))
        .unwrap();
    }
    db.checkpoint().unwrap();
}

#[test]
fn rrf_fusion_matches_reference_formula() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("r.mote")).unwrap();
    seed(&db);

    // The two candidate lists, fetched raw.
    let lex = col(
        &db,
        "SELECT id FROM docs WHERE MATCH(body, 'grasp') ORDER BY bm25(body) LIMIT 10",
    );
    let sem = col(
        &db,
        "SELECT id FROM docs ORDER BY emb <-> [10.0, 0.0, 0.0, 0.0] LIMIT 10",
    );
    assert!(!lex.is_empty() && !sem.is_empty());

    // Reference fusion (rrk=60, equal weights).
    use std::collections::HashMap;
    let mut fused: HashMap<i64, f64> = HashMap::new();
    for (list, w) in [(&lex, 1.0), (&sem, 1.0)] {
        for (rank0, v) in list.iter().enumerate() {
            let id = match v {
                Value::Integer(i) => *i,
                other => panic!("{other:?}"),
            };
            *fused.entry(id).or_insert(0.0) += w / (60.0 + rank0 as f64 + 1.0);
        }
    }
    let mut expected: Vec<(i64, f64)> = fused.into_iter().collect();
    expected.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap().then_with(|| a.0.cmp(&b.0)));
    let want_ids: Vec<i64> = expected.into_iter().take(10).map(|(id, _)| id).collect();

    // Engine RRF.
    let got = col(
        &db,
        "SELECT id FROM RRF((SELECT id FROM docs WHERE MATCH(body, 'grasp') ORDER BY bm25(body) LIMIT 10), (SELECT id FROM docs ORDER BY emb <-> [10.0, 0.0, 0.0, 0.0] LIMIT 10))",
    );
    let got_ids: Vec<i64> = got
        .iter()
        .map(|v| match v {
            Value::Integer(i) => *i,
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(
        got_ids, want_ids,
        "engine RRF must match the reference formula"
    );

    // Named args: k + rrk + weights change the fusion (w1=0 disables the
    // lexical list entirely → semantic order only).
    let sem_only = col(
        &db,
        "SELECT id FROM RRF((SELECT id FROM docs WHERE MATCH(body, 'grasp') ORDER BY bm25(body) LIMIT 10), (SELECT id FROM docs ORDER BY emb <-> [10.0, 0.0, 0.0, 0.0] LIMIT 10), k = 5, rrk = 60, w1 = 0.0)",
    );
    let want_sem: Vec<i64> = sem
        .iter()
        .take(5)
        .map(|v| match v {
            Value::Integer(i) => *i,
            other => panic!("{other:?}"),
        })
        .collect();
    let got_sem: Vec<i64> = sem_only
        .iter()
        .map(|v| match v {
            Value::Integer(i) => *i,
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(
        got_sem, want_sem,
        "w1=0 must reduce RRF to the second list (k=5)"
    );

    // Shape errors.
    assert!(db
        .execute("SELECT id FROM RRF((SELECT id FROM docs LIMIT 3))")
        .is_err());
    assert!(db.execute("SELECT id FROM RRF((SELECT id, body FROM docs LIMIT 3), (SELECT id FROM docs LIMIT 3))").is_err());
    assert!(db.execute("SELECT id FROM RRF((SELECT id FROM docs LIMIT 1), (SELECT id FROM docs LIMIT 1), bogus = 1)").is_err());
}

#[test]
fn bm25_order_returns_ranked_order() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("b.mote")).unwrap();
    // id 1 mentions 'grasp' twice (higher TF → higher BM25), id 2 once.
    db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    db.execute("CREATE TEXT INDEX docs_body ON docs(body)")
        .unwrap();
    db.execute("INSERT INTO docs VALUES (1, 'grasp grasp release cycle')")
        .unwrap();
    db.execute("INSERT INTO docs VALUES (2, 'plain grasp note')")
        .unwrap();
    db.execute("INSERT INTO docs VALUES (3, 'nothing relevant')")
        .unwrap();
    db.checkpoint().unwrap();

    let ids = col(
        &db,
        "SELECT id FROM docs WHERE MATCH(body, 'grasp') ORDER BY bm25(body) LIMIT 5",
    );
    assert_eq!(
        ids,
        vec![Value::Integer(1), Value::Integer(2)],
        "higher TF ranks first"
    );
    // Equivalence with the documented BM25_SCORE column form.
    let ids2 = col(
        &db,
        "SELECT id FROM docs WHERE MATCH(body, 'grasp') ORDER BY score DESC LIMIT 5",
    );
    assert_eq!(ids, ids2);
}

#[test]
fn snippet_windows_and_highlights() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("s.mote")).unwrap();
    db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    db.execute("INSERT INTO docs VALUES (1, 'the robot attempted to grasp the cylinder but the grip slipped; retry succeeded later')").unwrap();
    db.checkpoint().unwrap();

    let v = one(
        &db,
        "SELECT snippet(body, 'grasp', 40) FROM docs WHERE id = 1",
    );
    let Value::Text(s) = v else { panic!("text") };
    let s = s.as_str().to_string();
    assert!(s.contains("<b>grasp</b>"), "term highlighted: {s}");
    assert!(s.starts_with('…'), "leading ellipsis when hit is deep: {s}");
    assert!(s.len() < 90, "windowed, not the whole text: {s}");

    // No-match → head window, no highlight.
    let v = one(
        &db,
        "SELECT snippet(body, 'zzz', 20) FROM docs WHERE id = 1",
    );
    let Value::Text(s) = v else { panic!("text") };
    let s = s.as_str().to_string();
    assert!(!s.contains("<b>"), "no highlight without a hit: {s}");
    assert!(s.starts_with("the robot"), "head window: {s}");
}
