//! Trajectory-similarity index spike (W5 go/no-go, v0.14 roadmap).
//!
//! Pure in-memory prototype of the planned `traj_sim` index:
//!   z-norm → PAA(8) → SAX word (8 symbols, alphabet 8) → bucket;
//!   query = neighborhood enumeration (Hamming ≤ 2 → 1+56+1372 lookups)
//!   → DTW refine over candidates.
//! Two strategies measured against brute-force DTW ground truth:
//!   A) SAX neighborhood → DTW          (the planned index)
//!   B) PAA Euclidean top-500 → DTW     (the documented fallback)
//! Corpus: 90k z-normed random walks + 20k structured shapes (10 motion
//! families × 2k, phase/amplitude/noise jitter) = 110k × 64 points.
//! Queries: 50 structured (fresh noise/phase — the robotics shape) +
//! 50 random walks (ill-defined truth; reported for completeness).
//!
//! Gate (docs/design/2026-10-embodied-roadmap.md §5):
//!   recall@10 ≥ 0.90 (structured queries) · p99 ≤ 500 ms · index ≤ 8%
//!
//! Run: cargo run --release --example traj_spike

use std::collections::HashMap;
use std::time::Instant;

const L: usize = 64; // trajectory length (post-PAA of longer raw series in prod)
const SEGS: usize = 8; // SAX word length
const ALPHA: usize = 8; // alphabet size
const N_CORPUS: usize = 110_000;
const N_WALK: usize = 90_000; // random-walk portion
const N_QUERIES: usize = 100;
const TOPK: usize = 10;
const PAA_FALLBACK_N: usize = 500; // strategy B shortlist

/// Equal-area N(0,1) breakpoints for 8 symbols.
const BREAKS: [f32; ALPHA - 1] = [-1.1503, -0.7673, -0.4307, 0.0, 0.4307, 0.7673, 1.1503];

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 16
    }
    fn f(&mut self) -> f64 {
        (self.next() % 1_000_000) as f64 / 1_000_000.0
    }
}

fn znorm(v: &mut [f32]) {
    let n = v.len() as f32;
    let mean = v.iter().sum::<f32>() / n;
    let var = v.iter().map(|x| (x - mean) * (x - mean)).sum::<f32>() / n;
    let std = var.sqrt().max(1e-9);
    for x in v.iter_mut() {
        *x = (*x - mean) / std;
    }
}

/// Plain O(L²) DTW with optional early abandon (row-min > threshold).
fn dtw(a: &[f32], b: &[f32], abandon: Option<f32>) -> Option<f32> {
    let m = b.len();
    let mut prev = vec![f32::INFINITY; m + 1];
    let mut cur = vec![f32::INFINITY; m + 1];
    prev[0] = 0.0;
    for &ai in a {
        cur[0] = f32::INFINITY;
        let mut row_min = f32::INFINITY;
        for j in 0..m {
            let d = (ai - b[j]).abs();
            let v = d + prev[j].min(prev[j + 1]).min(cur[j]);
            cur[j + 1] = v;
            if v < row_min {
                row_min = v;
            }
        }
        if let Some(th) = abandon {
            if row_min > th {
                return None;
            }
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    Some(prev[m])
}

fn paa8(v: &[f32]) -> [f32; SEGS] {
    let step = v.len() / SEGS;
    let mut out = [0f32; SEGS];
    for s in 0..SEGS {
        let seg = &v[s * step..(s + 1) * step];
        out[s] = seg.iter().sum::<f32>() / seg.len() as f32;
    }
    znorm(&mut out);
    out
}

fn sax_word(paa: &[f32; SEGS]) -> [u8; SEGS] {
    let mut w = [0u8; SEGS];
    for (i, &x) in paa.iter().enumerate() {
        let mut s = 0u8;
        while s < (ALPHA - 1) as u8 && x > BREAKS[s as usize] {
            s += 1;
        }
        w[i] = s;
    }
    w
}

/// Hamming distance ≤ HAMMING neighborhood: 1 + C(8,1)·7 + C(8,2)·49 words.
fn neighbors(word: &[u8; SEGS]) -> Vec<[u8; SEGS]> {
    let mut out = Vec::with_capacity(1500);
    out.push(*word);
    for p1 in 0..SEGS {
        for s1 in 0..ALPHA as u8 {
            if s1 == word[p1] {
                continue;
            }
            let mut w1 = *word;
            w1[p1] = s1;
            out.push(w1);
            for p2 in (p1 + 1)..SEGS {
                for s2 in 0..ALPHA as u8 {
                    if s2 == w1[p2] {
                        continue;
                    }
                    let mut w2 = w1;
                    w2[p2] = s2;
                    out.push(w2);
                }
            }
        }
    }
    out
}

/// Motion family g ∈ 0..10: sin(2πf·t + φ)·A + noise; f varies per family,
/// φ/A/noise jitter per member — robotics "motion primitive" shape.
fn family_shape(g: usize, rng: &mut Lcg) -> Vec<f32> {
    let f = 0.5 + g as f64 * 0.35;
    let phase = rng.f() * std::f64::consts::TAU;
    let amp = 0.8 + rng.f() * 0.4;
    let noise = 0.05 + rng.f() * 0.10;
    let mut v: Vec<f32> = (0..L)
        .map(|t| {
            let x = (std::f64::consts::TAU * f * t as f64 / L as f64 + phase).sin();
            (x * amp + (rng.f() - 0.5) * 2.0 * noise) as f32
        })
        .collect();
    znorm(&mut v);
    v
}

fn random_walk(rng: &mut Lcg) -> Vec<f32> {
    let mut v: Vec<f32> = Vec::with_capacity(L);
    let mut acc = 0f64;
    for _ in 0..L {
        acc += rng.f() - 0.5;
        v.push(acc as f32);
    }
    znorm(&mut v);
    v
}

struct Outcome {
    recall_structured: f64,
    latency_us: u128,
    candidates: usize,
}

fn main() {
    let t0 = Instant::now();
    let mut rng = Lcg(0x7EA1);

    // ── Corpus ──
    let mut corpus: Vec<Vec<f32>> = Vec::with_capacity(N_CORPUS);
    let mut is_structured = vec![false; N_CORPUS];
    for (i, slot) in is_structured.iter_mut().enumerate() {
        if i < N_WALK {
            corpus.push(random_walk(&mut rng));
        } else {
            let g = i % 10;
            corpus.push(family_shape(g, &mut rng));
            *slot = true;
        }
    }

    // ── Build index: word → ids (strategy A) ──
    let tb = Instant::now();
    let mut index: HashMap<[u8; SEGS], Vec<u32>> = HashMap::with_capacity(N_CORPUS);
    let mut paas: Vec<[f32; SEGS]> = Vec::with_capacity(N_CORPUS);
    for (i, v) in corpus.iter().enumerate() {
        let paa = paa8(v);
        let w = sax_word(&paa);
        index.entry(w).or_default().push(i as u32);
        paas.push(paa);
    }
    let build_ms = tb.elapsed().as_millis();

    // Index overhead: word+id entries vs raw f32 payload.
    let entries: usize = index.values().map(|v| v.len()).sum();
    let index_bytes = entries * (SEGS + 4);
    let raw_bytes = N_CORPUS * L * 4;
    let overhead_pct = index_bytes as f64 / raw_bytes as f64 * 100.0;

    // ── Queries ──
    let mut queries: Vec<(Vec<f32>, bool)> = Vec::with_capacity(N_QUERIES);
    for g in 0..N_QUERIES / 2 {
        let mut q = family_shape(g, &mut rng);
        // fresh noise so the query isn't byte-identical to a member
        for x in q.iter_mut().step_by(7) {
            *x += (rng.f() - 0.5) as f32 * 0.08;
        }
        znorm(&mut q);
        queries.push((q, true));
    }
    for _ in 0..N_QUERIES / 2 {
        queries.push((random_walk(&mut rng), false));
    }

    // ── Ground truth: parallel brute DTW top-10 ──
    let tg = Instant::now();
    let truth: Vec<Vec<u32>> = {
        let corpus_ref = &corpus;
        std::thread::scope(|s| {
            let handles: Vec<_> = queries
                .iter()
                .map(|(q, _)| {
                    s.spawn(move || {
                        let mut heap: Vec<(f32, u32)> = Vec::with_capacity(TOPK + 1);
                        for (id, c) in corpus_ref.iter().enumerate() {
                            let d = dtw(q, c, None).unwrap_or(f32::INFINITY);
                            if heap.len() < TOPK {
                                heap.push((d, id as u32));
                                heap.sort_by(|a, b| b.partial_cmp(a).unwrap());
                            } else if d < heap[0].0 {
                                heap[0] = (d, id as u32);
                                heap.sort_by(|a, b| b.partial_cmp(a).unwrap());
                            }
                        }
                        heap.into_iter().map(|(_, id)| id).collect::<Vec<_>>()
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        })
    };
    let truth_s = tg.elapsed().as_secs_f64();

    // ── Strategy A: SAX neighborhood → DTW refine ──
    let mut visited = vec![0u32; N_CORPUS];
    let mut epoch = 0u32;
    let ta = Instant::now();
    let a: Vec<Outcome> = queries
        .iter()
        .enumerate()
        .map(|(qi, (q, _))| {
            epoch += 1;
            let t = Instant::now();
            let qpaa = paa8(q);
            let qw = sax_word(&qpaa);
            let mut cands: Vec<u32> = Vec::new();
            for w in neighbors(&qw) {
                if let Some(ids) = index.get(&w) {
                    for &id in ids {
                        if visited[id as usize] != epoch {
                            visited[id as usize] = epoch;
                            cands.push(id);
                        }
                    }
                }
            }
            let n_cands = cands.len();
            // Refine with DTW (abandon vs current 10th once the heap fills).
            let mut heap: Vec<(f32, u32)> = Vec::with_capacity(TOPK + 1);
            for id in cands {
                let th = if heap.len() >= TOPK {
                    Some(heap[0].0)
                } else {
                    None
                };
                if let Some(d) = dtw(q, &corpus[id as usize], th) {
                    if heap.len() < TOPK {
                        heap.push((d, id));
                        heap.sort_by(|a, b| b.partial_cmp(a).unwrap());
                    } else if d < heap[0].0 {
                        heap[0] = (d, id);
                        heap.sort_by(|a, b| b.partial_cmp(a).unwrap());
                    }
                }
            }
            let got: Vec<u32> = heap.into_iter().map(|(_, id)| id).collect();
            Outcome {
                recall_structured: Outcome::recall_for(&got, &truth[qi]),
                latency_us: t.elapsed().as_micros(),
                candidates: n_cands,
            }
        })
        .collect();
    let a_s = ta.elapsed().as_secs_f64();

    // ── Strategy B: PAA Euclidean top-500 → DTW refine ──
    let tb2 = Instant::now();
    let b: Vec<Outcome> = queries
        .iter()
        .enumerate()
        .map(|(qi, (q, _))| {
            let t = Instant::now();
            let qpaa = paa8(q);
            let mut scored: Vec<(f32, u32)> = paas
                .iter()
                .enumerate()
                .map(|(id, p)| {
                    let d: f32 = p.iter().zip(&qpaa).map(|(x, y)| (x - y) * (x - y)).sum();
                    (d, id as u32)
                })
                .collect();
            scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            scored.truncate(PAA_FALLBACK_N);
            let n_cands = scored.len();
            let mut heap: Vec<(f32, u32)> = Vec::with_capacity(TOPK + 1);
            for (_, id) in scored {
                let th = if heap.len() >= TOPK {
                    Some(heap[0].0)
                } else {
                    None
                };
                if let Some(d) = dtw(q, &corpus[id as usize], th) {
                    if heap.len() < TOPK {
                        heap.push((d, id));
                        heap.sort_by(|a, b| b.partial_cmp(a).unwrap());
                    } else if d < heap[0].0 {
                        heap[0] = (d, id);
                        heap.sort_by(|a, b| b.partial_cmp(a).unwrap());
                    }
                }
            }
            let got: Vec<u32> = heap.into_iter().map(|(_, id)| id).collect();
            Outcome {
                recall_structured: Outcome::recall_for(&got, &truth[qi]),
                latency_us: t.elapsed().as_micros(),
                candidates: n_cands,
            }
        })
        .collect();
    let b_s = tb2.elapsed().as_secs_f64();

    // ── Report ──
    println!(
        "corpus={}×{}  build={}ms  truth={}s  total={}s",
        N_CORPUS,
        L,
        build_ms,
        truth_s,
        t0.elapsed().as_secs_f64()
    );
    println!(
        "index: {} entries, {} B vs raw {} B → {:.1}% overhead (gate ≤ 8%)",
        entries, index_bytes, raw_bytes, overhead_pct
    );

    for (name, outs, total_s) in [("A SAX→DTW", &a, a_s), ("B PAA→DTW", &b, b_s)] {
        // Headline = structured-query recall (the robotics shape; the gate
        // metric). Random-walk queries have ill-defined DTW truth (all walks
        // sit at near-equal distance) — reported separately, NOT gated.
        let rec_by: Vec<f64> = outs
            .iter()
            .zip(queries.iter())
            .filter(|(_, (_, s))| *s)
            .map(|(o, _)| o.recall_structured)
            .collect();
        let mean_rec = rec_by.iter().sum::<f64>() / rec_by.len() as f64;
        let walk: Vec<f64> = outs
            .iter()
            .zip(queries.iter())
            .filter(|(_, (_, s))| !*s)
            .map(|(o, _)| o.recall_structured)
            .collect();
        let mut lat: Vec<u128> = outs.iter().map(|o| o.latency_us).collect();
        lat.sort_unstable();
        let p50 = lat[lat.len() / 2];
        let p99 = lat[lat.len() * 99 / 100];
        let cands: usize = outs.iter().map(|o| o.candidates).sum::<usize>() / outs.len();
        println!(
            "{name}: STRUCTURED recall@10 = {:.3} (gate ≥ 0.90) | p50 = {}µs p99 = {}µs (gate ≤ 500ms) | avg candidates = {} | wall = {:.1}s",
            mean_rec, p50, p99, cands, total_s
        );
        println!(
            "    walk queries = {:.3} (ill-defined truth, informational)",
            walk.iter().sum::<f64>() / walk.len() as f64
        );
    }

    // Gate verdict on strategy A (the planned index), with B as fallback data.
    let a_rec: f64 = a.iter().map(|o| o.recall_structured).sum::<f64>() / a.len() as f64;
    let mut a_lat: Vec<u128> = a.iter().map(|o| o.latency_us).collect();
    a_lat.sort_unstable();
    let a_p99 = a_lat[a_lat.len() * 99 / 100];
    let gate = a_rec >= 0.90 && a_p99 as f64 <= 500_000.0 && overhead_pct <= 8.0;
    println!(
        "GATE: {}",
        if gate {
            "GO ✅"
        } else {
            "NO-GO ❌ (evaluate strategy B / redesign)"
        }
    );
}

impl Outcome {
    fn recall_for(got: &[u32], truth: &[u32]) -> f64 {
        let hits = got.iter().filter(|id| truth.contains(id)).count();
        hits as f64 / TOPK as f64
    }
}
