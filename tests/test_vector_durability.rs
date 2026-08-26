//! Vector index durability round-trip tests (BUG #25 family).
//!
//! SQ8Vectors/DiskGraph treat their data files as append-only logs, but the
//! sidecar index rebuild only scanned the first `count` records without
//! last-wins dedup and without tombstones for deletes. Consequences:
//! - vector UPDATE silently reverted to the old value after flush+reload
//! - graph edge maintenance after the initial build was lost on flush+reload
//! - DELETE resurrected (delete-all resurrected everything)
//! - delete + re-insert of the same row_id hit "already exists" (the
//!   update_row vector path is exactly delete_vector + update_vector)

use motedb::index::vamana::disk_graph::DiskGraph;
use motedb::index::vamana::diskann_index::DiskANNIndex;
use motedb::index::vamana::sq8::SQ8Quantizer;
use motedb::index::vamana::sq8_vectors::SQ8Vectors;
use motedb::index::vamana::VamanaConfig;
use motedb::types::{Tensor, Value};
use motedb::Database;
use std::sync::Arc;
use tempfile::TempDir;

// === DiskGraph: edge updates must survive flush + reload ===

#[test]
fn graph_edge_update_survives_flush_reload() {
    let dir = TempDir::new().unwrap();
    {
        let g = DiskGraph::create(dir.path(), 32, 1000).unwrap();
        g.set_neighbors(1, vec![2]).unwrap();
        g.set_neighbors(2, vec![1]).unwrap();
        // "update" node 1: re-point its edges (appends a second record)
        g.set_neighbors(1, vec![3]).unwrap();
        g.set_neighbors(3, vec![1]).unwrap();
        g.flush().unwrap();
    }
    let g = DiskGraph::load(dir.path(), 1000).unwrap();
    assert_eq!(g.node_count(), 3, "all 3 nodes must survive reload");
    let n1 = g.neighbors(1);
    assert!(
        n1.contains(&3) && !n1.contains(&2),
        "node 1 must keep its UPDATED edges, got {:?}",
        &*n1
    );
    let n3 = g.neighbors(3);
    assert!(n3.contains(&1), "node 3 must be reachable, got {:?}", &*n3);
}

#[test]
fn graph_delete_survives_flush_reload() {
    let dir = TempDir::new().unwrap();
    {
        let g = DiskGraph::create(dir.path(), 32, 1000).unwrap();
        g.set_neighbors(1, vec![2, 3]).unwrap();
        g.set_neighbors(2, vec![1, 3]).unwrap();
        g.set_neighbors(3, vec![1, 2]).unwrap();
        g.remove_node(2);
        g.flush().unwrap();
    }
    let g = DiskGraph::load(dir.path(), 1000).unwrap();
    assert_eq!(g.node_count(), 2, "deleted node must not resurrect");
    assert_eq!(
        g.neighbors(2).len(),
        0,
        "deleted node must not resolve via sidecar"
    );
    // Remaining nodes must not have been dropped by the prefix-scan rebuild
    assert!(g.neighbors(1).len() > 0, "node 1 edges must survive");
    assert!(g.neighbors(3).len() > 0, "node 3 edges must survive");
}

#[test]
fn graph_append_after_reload_does_not_overwrite() {
    let dir = TempDir::new().unwrap();
    {
        let g = DiskGraph::create(dir.path(), 32, 1000).unwrap();
        g.set_neighbors(1, vec![2]).unwrap();
        g.set_neighbors(2, vec![1]).unwrap();
        // simulate an update round: append new versions for existing nodes
        g.set_neighbors(1, vec![2, 3]).unwrap();
        g.set_neighbors(2, vec![1, 3]).unwrap();
        g.set_neighbors(3, vec![1, 2]).unwrap();
        g.flush().unwrap();
    }
    {
        let g = DiskGraph::load(dir.path(), 1000).unwrap();
        // next_offset must be EOF, not "after count records" (mid-file)
        g.set_neighbors(4, vec![1]).unwrap();
        g.flush().unwrap();
    }
    let g = DiskGraph::load(dir.path(), 1000).unwrap();
    assert_eq!(g.node_count(), 4);
    let n1 = g.neighbors(1);
    assert!(
        n1.contains(&3),
        "node 1 must keep 3-neighbor edges: {:?}",
        &*n1
    );
    let n4 = g.neighbors(4);
    assert!(n4.contains(&1), "node 4 appended after reload must survive");
}

// === SQ8Vectors ===

#[test]
fn sq8_update_survives_flush_reload() {
    let dir = TempDir::new().unwrap();
    let q = Arc::new(SQ8Quantizer::new(4));
    {
        let s = SQ8Vectors::create(dir.path(), q.clone(), 100).unwrap();
        s.insert(1, vec![1.0, 0.0, 0.0, 0.0]).unwrap();
        s.insert(2, vec![0.0, 1.0, 0.0, 0.0]).unwrap();
        s.update(1, vec![0.0, 0.0, 0.0, 1.0]).unwrap();
        s.flush().unwrap();
    }
    let s = SQ8Vectors::load(dir.path(), q, 100).unwrap();
    assert_eq!(s.len(), 2);
    let v = s.get(1).unwrap();
    assert!(
        v[3] > 0.5 && v[0] < 0.5,
        "updated vector must be the NEW value, got {:?}",
        &*v
    );
}

#[test]
fn sq8_delete_then_reinsert_same_id() {
    let dir = TempDir::new().unwrap();
    let q = Arc::new(SQ8Quantizer::new(4));
    {
        let s = SQ8Vectors::create(dir.path(), q.clone(), 100).unwrap();
        s.insert(1, vec![1.0, 0.0, 0.0, 0.0]).unwrap();
        s.flush().unwrap(); // id 1 now in the sidecar
        let deleted = s.delete(1).unwrap();
        assert!(deleted, "delete must report removed=true");
        // re-insert the same id (the update_row vector path):
        // sidecar still physically lists id 1 → old code errored here
        s.insert(1, vec![0.0, 0.0, 0.0, 1.0])
            .expect("re-insert after delete must not hit 'already exists'");
        s.flush().unwrap();
    }
    let s = SQ8Vectors::load(dir.path(), q, 100).unwrap();
    assert_eq!(s.len(), 1, "exactly one live vector");
    let v = s.get(1).unwrap();
    assert!(
        v[3] > 0.5 && v[0] < 0.5,
        "must read the REINSERTED vector, got {:?}",
        &*v
    );
}

#[test]
fn sq8_delete_all_no_resurrection() {
    let dir = TempDir::new().unwrap();
    let q = Arc::new(SQ8Quantizer::new(4));
    {
        let s = SQ8Vectors::create(dir.path(), q.clone(), 100).unwrap();
        s.insert(1, vec![1.0, 0.0, 0.0, 0.0]).unwrap();
        s.insert(2, vec![0.0, 1.0, 0.0, 0.0]).unwrap();
        s.insert(3, vec![0.0, 0.0, 1.0, 0.0]).unwrap();
        s.flush().unwrap();
        s.delete(1).unwrap();
        s.delete(2).unwrap();
        s.delete(3).unwrap();
        s.flush().unwrap();
    }
    let s = SQ8Vectors::load(dir.path(), q, 100).unwrap();
    assert_eq!(s.len(), 0, "delete-all must not resurrect on reload");
    assert!(s.get(1).is_none(), "deleted id must not resolve");
    assert!(s.ids().is_empty(), "sidecar must be empty after delete-all");
}

// === DiskANNIndex end-to-end ===

fn clustered_vectors(n: usize, dim: usize) -> Vec<(u64, Vec<f32>)> {
    // 4 well-separated clusters so stale edges visibly destroy recall
    let mut out = Vec::new();
    for i in 0..n {
        let cluster = i % 4;
        let mut v = vec![0.0f32; dim];
        v[cluster] = 10.0;
        v[dim - 1] = (i / 4) as f32 * 0.01;
        out.push((i as u64, v));
    }
    out
}

#[test]
fn diskann_update_then_reload_returns_new_vector() {
    let dir = TempDir::new().unwrap();
    let config = VamanaConfig::embedded(8);
    {
        let index = DiskANNIndex::create(dir.path(), 8, config.clone()).unwrap();
        index.build(clustered_vectors(60, 8)).unwrap();
        // Move row 7 from cluster 3 to a unique spot inside cluster 0
        let mut moved = vec![0.0f32; 8];
        moved[0] = 10.0;
        moved[7] = 0.5;
        index.update(7, moved.clone()).unwrap();
        index.flush().unwrap();
    }
    let index = DiskANNIndex::load(dir.path(), config).unwrap();
    let mut query = vec![0.0f32; 8];
    query[0] = 10.0;
    query[7] = 0.5;
    let results = index.search(&query, 1).unwrap();
    assert!(!results.is_empty(), "search must return results");
    assert_eq!(
        results[0].0, 7,
        "top-1 for the moved vector's new location must be row 7, got {:?}",
        results
    );
}

#[test]
fn diskann_delete_then_reload_no_ghost_results() {
    let dir = TempDir::new().unwrap();
    let config = VamanaConfig::embedded(8);
    {
        let index = DiskANNIndex::create(dir.path(), 8, config.clone()).unwrap();
        index.build(clustered_vectors(40, 8)).unwrap();
        for id in 0..20u64 {
            index.delete(id).unwrap();
        }
        index.flush().unwrap();
    }
    let index = DiskANNIndex::load(dir.path(), config).unwrap();
    assert_eq!(index.len(), 20, "20 vectors must remain");
    for cluster in 0..4 {
        let mut query = vec![0.0f32; 8];
        query[cluster] = 10.0;
        let results = index.search(&query, 10).unwrap();
        for (id, _dist) in &results {
            assert!(
                *id >= 20,
                "deleted id {} must not appear in search results",
                id
            );
        }
    }
}

// === Recall quantification after updates (round-12 follow-up) ===

fn deterministic_vector(seed: usize, dim: usize) -> Vec<f32> {
    // cheap deterministic LCG — no external rng dependency
    let mut state = (seed as u64)
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    (0..dim)
        .map(|_| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((state >> 33) as f32 / (u32::MAX >> 1) as f32) - 1.0
        })
        .collect()
}

#[test]
fn diskann_recall_after_mass_updates_and_reload() {
    let dir = TempDir::new().unwrap();
    let dim = 16usize;
    let n = 300usize;
    let vectors: Vec<(u64, Vec<f32>)> = (0..n)
        .map(|i| (i as u64, deterministic_vector(i, dim)))
        .collect();

    let config = VamanaConfig::embedded(dim);
    let expected: Vec<(u64, Vec<f32>)> = {
        let index = DiskANNIndex::create(dir.path(), dim, config.clone()).unwrap();
        index.build(vectors).unwrap();

        // Rewrite half the dataset to NEW positions (quantized in-place)
        let mut updated: Vec<(u64, Vec<f32>)> = (0..n)
            .map(|i| (i as u64, deterministic_vector(i, dim)))
            .collect();
        for i in (0..n).step_by(2) {
            let v = deterministic_vector(10_000 + i, dim);
            index.update(i as u64, v.clone()).unwrap();
            updated[i].1 = v;
        }
        index.flush().unwrap();
        updated
    };

    let index = DiskANNIndex::load(dir.path(), config).unwrap();
    assert_eq!(index.len(), n);

    let l2 = |a: &[f32], b: &[f32]| {
        a.iter()
            .zip(b)
            .map(|(x, y)| (x - y).powi(2))
            .sum::<f32>()
            .sqrt()
    };

    let mut recalls = Vec::new();
    for q in 0..50usize {
        let query = deterministic_vector(10_000 + (q * 2), dim); // query at updated positions
                                                                 // ground truth: brute force over the EXPECTED (post-update) vectors
        let mut truth: Vec<(u64, f32)> = expected
            .iter()
            .map(|(id, v)| (*id, l2(&query, v)))
            .collect();
        truth.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        let truth_ids: std::collections::HashSet<u64> =
            truth.iter().take(10).map(|(id, _)| *id).collect();

        let results = index.search(&query, 10).unwrap();
        let hits = results
            .iter()
            .filter(|(id, _)| truth_ids.contains(id))
            .count();
        recalls.push(hits as f32 / 10.0);
    }
    let avg: f32 = recalls.iter().sum::<f32>() / recalls.len() as f32;
    assert!(
        avg >= 0.8,
        "recall@10 after updates+reload must be >= 0.8, got {avg:.3} ({:?})",
        recalls
    );
}

// === DB level: incremental inserts after reopen ===

#[test]
fn db_vector_insert_after_reopen_then_search() {
    let dir = TempDir::new().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(8))")
            .unwrap();
        for (id, v) in clustered_vectors(40, 8) {
            db.insert_row(
                "items",
                vec![Value::Integer(id as i64), Value::tensor(Tensor::new(v))],
            )
            .unwrap();
        }
        db.execute("CREATE VECTOR INDEX idx_emb ON items(emb)")
            .unwrap();
        db.wait_for_indexes_ready();
        db.checkpoint().unwrap();
    }
    {
        // Reopen and insert MORE rows with vectors (batch_insert path)
        let db = Database::open(dir.path()).unwrap();
        for (id, v) in clustered_vectors(20, 8).into_iter().map(|(i, mut v)| {
            (100 + i, {
                v[0] = 10.0;
                v[7] = 0.9;
                v
            })
        }) {
            db.insert_row(
                "items",
                vec![Value::Integer(id as i64), Value::tensor(Tensor::new(v))],
            )
            .unwrap();
        }
        db.checkpoint().unwrap();
    }
    let db = Database::open(dir.path()).unwrap();
    let mut query = vec![0.0f32; 8];
    query[0] = 10.0;
    query[7] = 0.9;
    let results = db.vector_search("idx_emb", &query, 5).unwrap();
    assert!(
        results.iter().any(|(id, _)| *id >= 100),
        "newly inserted vectors must be searchable after reopen, got {:?}",
        results
    );
}

#[test]
fn db_vector_update_survives_checkpoint_and_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(8))")
            .unwrap();
        for (i, (id, v)) in clustered_vectors(60, 8).into_iter().enumerate() {
            let _ = i;
            db.insert_row(
                "items",
                vec![Value::Integer(id as i64), Value::tensor(Tensor::new(v))],
            )
            .unwrap();
        }
        db.execute("CREATE VECTOR INDEX idx_emb ON items(emb)")
            .unwrap();
        db.wait_for_indexes_ready();
        db.checkpoint().unwrap();

        // Move row 7 to a unique spot inside cluster 0 via update_row — the
        // path that runs delete_vector + update_vector on the index
        let old = db.get_row("items", 7).unwrap().expect("row 7 must exist");
        let mut updated = old;
        updated[1] = Value::tensor(Tensor::new(vec![10.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5]));
        db.update_row("items", 7, updated).unwrap();
        db.checkpoint().unwrap();
    }
    let db = Database::open(dir.path()).unwrap();
    let results = db
        .vector_search("idx_emb", &[10.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5], 1)
        .unwrap();
    assert!(
        !results.is_empty(),
        "vector_search must return results after reopen"
    );
    assert_eq!(
        results[0].0, 7,
        "top-1 must be the UPDATED row 7, got {:?}",
        results
    );
}

#[test]
fn db_vector_delete_survives_checkpoint_and_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(8))")
            .unwrap();
        for (id, v) in clustered_vectors(40, 8) {
            db.insert_row(
                "items",
                vec![Value::Integer(id as i64), Value::tensor(Tensor::new(v))],
            )
            .unwrap();
        }
        db.execute("CREATE VECTOR INDEX idx_emb ON items(emb)")
            .unwrap();
        db.wait_for_indexes_ready();
        db.checkpoint().unwrap();

        db.execute("DELETE FROM items WHERE id < 20").unwrap();
        db.checkpoint().unwrap();
    }
    let db = Database::open(dir.path()).unwrap();
    for cluster in 0..4usize {
        let mut query = vec![0.0f32; 8];
        query[cluster] = 10.0;
        let results = db.vector_search("idx_emb", &query, 10).unwrap();
        for (id, _dist) in &results {
            assert!(
                *id >= 20,
                "deleted id {} must not appear in search after reopen",
                id
            );
        }
    }
}
