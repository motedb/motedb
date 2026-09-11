//! Index-level recall probe on REAL embeddings.
//!
//! The SQL layer answers `ORDER BY emb <-> ? LIMIT k` with an exact SIMD scan
//! below `EXACT_SCAN_MAX_ROWS` (200K rows), so the DiskANN graph never
//! influences results for smaller tables. This probe calls
//! `Database::vector_search` directly to measure what the graph index itself
//! delivers at a given scale, against brute-force ground truth.
//!
//! Inputs are raw little-endian f32 matrices (row-major, `dim` floats per
//! row), e.g. exported from numpy with `arr.astype("<f4").tofile(path)`:
//!
//! ```bash
//! cargo run --release --example vector_recall_real -- \
//!     ~/.cache/motedb_eval/nli-corpus.f32 ~/.cache/motedb_eval/nli-queries.f32 40000 384 200
//! ```
use motedb::types::{SqlRow, Value};
use motedb::{DBConfig, Database};
use std::collections::{HashMap, HashSet};
use std::time::Instant;

fn read_f32(path: &str, n: usize, dim: usize) -> Vec<Vec<f32>> {
    let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    assert!(
        bytes.len() >= n * dim * 4,
        "{path}: need {} bytes for {n}x{dim}, have {}",
        n * dim * 4,
        bytes.len()
    );
    (0..n)
        .map(|i| {
            let off = i * dim * 4;
            (0..dim)
                .map(|j| {
                    let s = off + j * 4;
                    f32::from_le_bytes([bytes[s], bytes[s + 1], bytes[s + 2], bytes[s + 3]])
                })
                .collect()
        })
        .collect()
}

fn l2sq(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

/// Exact top-k row indices (ascending L2), ties broken by index.
fn exact_topk(d: &[Vec<f32>], q: &[f32], k: usize) -> Vec<usize> {
    let mut ds: Vec<(f32, usize)> = d.iter().enumerate().map(|(i, v)| (l2sq(q, v), i)).collect();
    ds.select_nth_unstable_by(k - 1, |a, b| a.partial_cmp(b).unwrap());
    ds.truncate(k);
    ds.sort_by(|a, b| a.partial_cmp(b).unwrap());
    ds.into_iter().map(|(_, i)| i).collect()
}

fn recall(got: &[Vec<usize>], truth: &[Vec<usize>], k: usize) -> f64 {
    let mut s = 0.0;
    for (g, t) in got.iter().zip(truth) {
        let gs: HashSet<usize> = g.iter().take(k).copied().collect();
        s += t.iter().take(k).filter(|i| gs.contains(i)).count() as f64 / k as f64;
    }
    s / got.len() as f64
}

fn pct(lat: &mut [f64], p: f64) -> f64 {
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    lat[((lat.len() as f64 - 1.0) * p) as usize]
}

fn main() -> motedb::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 5 {
        eprintln!("usage: vector_recall_real <corpus.f32> <queries.f32> <n> <dim> [nq=200]");
        std::process::exit(2);
    }
    let n: usize = args[3].parse().unwrap();
    let dim: usize = args[4].parse().unwrap();
    let nq: usize = args.get(5).map(|s| s.parse().unwrap()).unwrap_or(200);
    let d = read_f32(&args[1], n, dim);
    let q = read_f32(&args[2], nq, dim);
    println!("corpus {n}x{dim}, {nq} held-out queries");

    let dir = tempfile::TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing())?;
    db.execute(&format!(
        "CREATE TABLE t (id INT PRIMARY KEY, emb VECTOR({dim}))"
    ))?;

    // Insert first (no index yet), remembering RowId → corpus index.
    let t0 = Instant::now();
    let mut rid_to_idx: HashMap<u64, usize> = HashMap::with_capacity(n);
    for start in (0..n).step_by(5000) {
        let end = (start + 5000).min(n);
        let rows: Vec<SqlRow> = (start..end)
            .map(|i| {
                let mut r = SqlRow::new();
                r.insert("id".into(), Value::Integer(i as i64 + 1));
                r.insert(
                    "emb".into(),
                    Value::Vector(motedb::types::ArcVec::new(d[i].clone())),
                );
                r
            })
            .collect();
        let rids = db.batch_insert_with_vectors_map("t", rows, &["emb"])?;
        for (j, rid) in rids.into_iter().enumerate() {
            rid_to_idx.insert(rid, start + j);
        }
    }
    db.execute("CHECKPOINT")?;
    println!("insert: {:.1}s", t0.elapsed().as_secs_f64());

    // Bulk build.
    let t0 = Instant::now();
    db.execute("CREATE VECTOR INDEX t_emb ON t(emb)")?;
    db.execute("CHECKPOINT")?;
    let build_s = t0.elapsed().as_secs_f64();
    println!(
        "CREATE VECTOR INDEX: {build_s:.1}s ({:.2} ms/row)",
        build_s * 1000.0 / n as f64
    );

    const K: usize = 100;
    let truth: Vec<Vec<usize>> = q.iter().map(|qq| exact_topk(&d, qq, K)).collect();

    let run = |label: &str, db: &Database| -> motedb::Result<()> {
        let mut got = Vec::with_capacity(nq);
        let mut lat = Vec::with_capacity(nq);
        let mut missing = 0usize;
        for qq in &q {
            let t = Instant::now();
            let res = db.vector_search("t_emb", qq, K)?;
            lat.push(t.elapsed().as_secs_f64() * 1e3);
            let ids: Vec<usize> = res
                .iter()
                .filter_map(|(rid, _)| {
                    let m = rid_to_idx.get(rid).copied();
                    if m.is_none() {
                        missing += 1;
                    }
                    m
                })
                .collect();
            got.push(ids);
        }
        let avg = lat.iter().sum::<f64>() / lat.len() as f64;
        let n_ret = got.iter().map(|g| g.len()).sum::<usize>() as f64 / got.len() as f64;
        println!(
            "{label:32} recall@1 {:.4}  recall@10 {:.4}  recall@100 {:.4}   [avg {avg:.2}ms  p50 {:.2}  p95 {:.2}]  returned {n_ret:.1}/{K}{}",
            recall(&got, &truth, 1),
            recall(&got, &truth, 10),
            recall(&got, &truth, K),
            pct(&mut lat.clone(), 0.5),
            pct(&mut lat.clone(), 0.95),
            if missing > 0 { format!("  unknown-rowid {missing}") } else { String::new() }
        );
        Ok(())
    };
    run("DiskANN vector_search", &db)?;

    // Self-queries: a stored vector must return itself first.
    let mut hit = 0usize;
    let step = (n / 200).max(1);
    let mut cnt = 0usize;
    for i in (0..n).step_by(step) {
        let res = db.vector_search("t_emb", &d[i], 1)?;
        if res.first().and_then(|(rid, _)| rid_to_idx.get(rid)) == Some(&i) {
            hit += 1;
        }
        cnt += 1;
    }
    println!(
        "self-query top-1 == self: {:.4} ({hit}/{cnt})",
        hit as f64 / cnt as f64
    );

    // Reopen from disk.
    db.close()?;
    let db = Database::open(dir.path())?;
    run("DiskANN after reopen", &db)?;
    db.close()?;
    Ok(())
}
