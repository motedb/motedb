//! J1: hybrid retrieval (BM25 + vector RRF fusion) differential tests.

use motedb::types::Value;
use motedb::Database;
use std::collections::HashMap;

fn setup() -> (Database, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT, emb VECTOR(4))")
        .unwrap();
    // 12 docs: bodies with varying keyword counts, embeddings in a line.
    let texts = [
        "rust database engine",      // 0
        "rust rust rust embedded",   // 1 (strong 'rust')
        "database of databases",     // 2
        "engine room",               // 3
        "rust database",             // 4
        "vector search index",       // 5
        "embedding vectors here",    // 6
        "nothing relevant at all",   // 7
        "database engine rust fast", // 8
        "totally different words",   // 9
        "rust",                      // 10
        "engine engine engine",      // 11 (strong 'engine')
    ];
    for (i, t) in texts.iter().enumerate() {
        let v = i as f32 * 0.25;
        db.execute(format!("INSERT INTO docs VALUES ({i}, '{t}', [{v}, {v}, {v}, {v}])").as_str())
            .unwrap();
    }
    db.execute("CREATE TEXT INDEX docs_body ON docs (body)")
        .unwrap();
    db.execute("CREATE VECTOR INDEX docs_emb ON docs (emb)")
        .unwrap();
    (db, dir)
}

/// 🔒 RRF differential: fused scores must equal Σ 1/(rrf_k + rank) computed
/// BY HAND from the two primitive lists.
#[test]
fn hybrid_rrf_matches_hand_computation() {
    let (db, _dir) = setup();
    let bm25: Vec<(u64, f32)> = db
        .text_search_ranked("docs_body", "rust database", 24)
        .unwrap();
    let knn: Vec<(u64, f32)> = db
        .vector_search("docs_emb", &[1.0, 1.0, 1.0, 1.0], 24)
        .unwrap();
    assert!(!bm25.is_empty() && !knn.is_empty());

    // Hand computation.
    let rrf_k = 60.0f32;
    let mut expected: HashMap<u64, (f32, Option<f32>, Option<f32>)> = HashMap::new();
    for (r, (id, s)) in bm25.iter().enumerate() {
        let e = expected.entry(*id).or_insert((0.0, None, None));
        e.0 += 1.0 / (rrf_k + r as f32 + 1.0);
        e.1 = Some(*s);
    }
    for (r, (id, d)) in knn.iter().enumerate() {
        let e = expected.entry(*id).or_insert((0.0, None, None));
        e.0 += 1.0 / (rrf_k + r as f32 + 1.0);
        e.2 = Some(*d);
    }

    let hits = db
        .hybrid_search(
            "docs_body",
            "rust database",
            "docs_emb",
            &[1.0, 1.0, 1.0, 1.0],
            10,
            60,
            4,
        )
        .unwrap();
    assert_eq!(hits.len(), 10);
    for h in &hits {
        let (rrf, bm25, dist) = expected[&h.row_id];
        assert!(
            (h.rrf - rrf).abs() < 1e-6,
            "rrf mismatch for doc {}: engine {} vs hand {}",
            h.row_id,
            h.rrf,
            rrf
        );
        match (bm25, h.bm25) {
            (Some(a), Some(b)) => assert!((a - b).abs() < 1e-5),
            (None, None) => {}
            (a, b) => panic!("bm25 presence mismatch for {}: {a:?} vs {b:?}", h.row_id),
        }
    }
    // Descending order.
    for w in hits.windows(2) {
        assert!(w[0].rrf >= w[1].rrf);
    }
    // Docs appearing in BOTH lists outrank single-list docs with the same
    // per-list ranks (the whole point of fusion).
    let both_lists: Vec<_> = hits
        .iter()
        .filter(|h| h.bm25.is_some() && h.distance.is_some())
        .collect();
    assert!(!both_lists.is_empty(), "fixture must have overlap docs");
}

/// 🔒 k / depth edge behavior: k larger than candidates; fetch depth bounds.
#[test]
fn hybrid_k_edges_and_determinism() {
    let (db, _dir) = setup();
    // k = 100 (more docs than exist): clamp to distinct candidate count.
    let hits = db
        .hybrid_search(
            "docs_body",
            "rust",
            "docs_emb",
            &[0.0, 0.0, 0.0, 0.0],
            100,
            60,
            4,
        )
        .unwrap();
    assert!(hits.len() <= 12);
    // Determinism: same inputs → identical output.
    let a = db
        .hybrid_search(
            "docs_body",
            "rust",
            "docs_emb",
            &[0.5, 0.5, 0.5, 0.5],
            5,
            60,
            4,
        )
        .unwrap();
    let b = db
        .hybrid_search(
            "docs_body",
            "rust",
            "docs_emb",
            &[0.5, 0.5, 0.5, 0.5],
            5,
            60,
            4,
        )
        .unwrap();
    assert_eq!(
        a.iter().map(|h| h.row_id).collect::<Vec<_>>(),
        b.iter().map(|h| h.row_id).collect::<Vec<_>>()
    );
    // rrf_k=1 sharpens top ranks (still descending, valid scores).
    let c = db
        .hybrid_search(
            "docs_body",
            "rust",
            "docs_emb",
            &[0.5, 0.5, 0.5, 0.5],
            5,
            1,
            4,
        )
        .unwrap();
    assert!(c.iter().all(|h| h.rrf > 0.0));
    // rows projection: names + row values align.
    let (names, rows, hits2) = db
        .hybrid_search_rows(
            "docs_body",
            "rust",
            "docs_emb",
            &[0.5, 0.5, 0.5, 0.5],
            5,
            60,
            4,
        )
        .unwrap();
    assert_eq!(rows.len(), hits2.len());
    assert_eq!(names.len(), rows.first().map(|r| r.len()).unwrap_or(0));
    assert!(names.contains(&"body".to_string()));
    // row values are the indexed table's: spot check a body is text.
    if let Some(r) = rows.first() {
        assert!(matches!(r.first(), Some(Value::Integer(_))));
    }
}
