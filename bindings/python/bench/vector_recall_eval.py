#!/usr/bin/env python3
"""
Vector-search accuracy evaluation with a real open-source embedding model.

Model: sentence-transformers/all-MiniLM-L6-v2 (22M params, 384-dim, CPU/MPS).

Two layers, because the executor routes by table size
(src/sql/executor.rs `EXACT_SCAN_MAX_ROWS = 200_000`):

  A. SciFact — 5,183 scientific abstracts, 300 test queries with qrels.
     Below the threshold, so `ORDER BY emb <-> ?` is the SIMD exact scan.
       * DB top-k vs numpy exact top-k        → recall@k (fidelity, expect ~1.0)
       * DB top-10 vs human qrels             → nDCG@10 / MRR@10 / Recall@10
         (end-to-end "does semantic search work", compared with the same
         model's numpy ceiling; MTEB reports nDCG@10 ≈ 0.645 for this model)

  B. all-nli — N real sentences (default 220,000 > threshold) so the same
     query is served by the DiskANN graph index.
       * no index (exact)  vs  DiskANN        → recall@1/10/100 + latency
       * self-queries (a stored vector as query must return itself)
       * reopen-from-disk recall, and recall after incremental inserts

Usage:
  python3 vector_recall_eval.py             # both layers
  python3 vector_recall_eval.py --skip-b    # SciFact only (fast)
  python3 vector_recall_eval.py --n-corpus 220000 --n-queries 200

All datasets/models are read from the local HuggingFace cache (offline).
Embeddings are cached under ~/.cache/motedb_eval so reruns skip encoding.
"""
import argparse
import hashlib
import json
import os
import shutil
import tempfile
import time

os.environ.setdefault("HF_HUB_OFFLINE", "1")
os.environ.setdefault("HF_DATASETS_OFFLINE", "1")
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")

import warnings  # noqa: E402

import numpy as np  # noqa: E402

# macOS Accelerate BLAS emits a spurious "divide by zero in matmul" on f32.
warnings.filterwarnings("ignore", message=".*matmul.*", category=RuntimeWarning)

MODEL = "sentence-transformers/all-MiniLM-L6-v2"
DIM = 384
CACHE = os.path.expanduser("~/.cache/motedb_eval")


def log(msg=""):
    print(msg, flush=True)


# ---------------------------------------------------------------- embedding
def load_model():
    import torch
    from sentence_transformers import SentenceTransformer

    device = "mps" if torch.backends.mps.is_available() else "cpu"
    m = SentenceTransformer(MODEL, device=device)
    log(f"model: {MODEL}  dim={m.get_sentence_embedding_dimension()}  "
        f"max_seq={m.max_seq_length}  device={device}")
    return m


def embed(model, texts, tag):
    """Normalized float32 embeddings, cached by content hash."""
    h = hashlib.sha1(("\n".join(texts) + MODEL).encode()).hexdigest()[:16]
    path = os.path.join(CACHE, f"{tag}-{len(texts)}-{h}.npy")
    if os.path.exists(path):
        embs = np.load(path)
        log(f"  {tag}: loaded {len(embs)} cached embeddings")
        return embs
    t0 = time.perf_counter()
    embs = model.encode(texts, batch_size=256, normalize_embeddings=True,
                        convert_to_numpy=True, show_progress_bar=False)
    embs = np.ascontiguousarray(embs, dtype=np.float32)
    dt = time.perf_counter() - t0
    log(f"  {tag}: embedded {len(texts)} texts in {dt:.1f}s ({len(texts) / dt:.0f}/s)")
    os.makedirs(CACHE, exist_ok=True)
    np.save(path, embs)
    return embs


# ---------------------------------------------------------------- ground truth
def exact_topk(D, Q, k, dn=None):
    """Exact L2 top-k (0-based row indices) in float32, matching the DB metric."""
    if dn is None:
        dn = (D * D).sum(1)
    out = np.empty((len(Q), k), dtype=np.int64)
    for i in range(0, len(Q), 64):
        q = Q[i:i + 64]
        d2 = dn[None, :] - 2.0 * (q @ D.T) + (q * q).sum(1)[:, None]
        idx = np.argpartition(d2, k, axis=1)[:, :k]
        part = np.take_along_axis(d2, idx, 1)
        order = np.argsort(part, axis=1, kind="stable")
        out[i:i + 64] = np.take_along_axis(idx, order, 1)
    return out


def recall_at(got, truth, k):
    """Mean |top-k(got) ∩ top-k(truth)| / k."""
    s = 0.0
    for g, t in zip(got, truth):
        s += len(set(g[:k]) & set(t[:k])) / k
    return s / len(got)


# ---------------------------------------------------------------- MoteDB helpers
def open_db(path, preset=None):
    import motedb
    return motedb.Database(path, preset=preset)


def create_table(db, table):
    db.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, emb VECTOR({DIM}))")


def insert_rows(db, table, D, id_offset=0, batch=2000):
    """Insert D[i] as id = id_offset + i + 1 (ids are 1-based, row index + 1)."""
    t0 = time.perf_counter()
    sql = f"INSERT INTO {table} (id, emb) VALUES (?, ?)"
    for s in range(0, len(D), batch):
        chunk = D[s:s + batch]
        db.executemany(sql, [[id_offset + s + j + 1, chunk[j].tolist()]
                             for j in range(len(chunk))])
    dt = time.perf_counter() - t0
    log(f"  inserted {len(D)} rows in {dt:.1f}s ({len(D) / dt:.0f} rows/s)")
    return dt


def db_topk(db, table, Q, k, op="<->"):
    """Run ORDER BY emb <op> ? LIMIT k per query; return 0-based indices + latencies."""
    sql = f"SELECT id FROM {table} ORDER BY emb {op} ? LIMIT {k}"
    out, lat = [], []
    for q in Q:
        t0 = time.perf_counter()
        _, rows = db.query(sql, params=[q.tolist()])
        lat.append(time.perf_counter() - t0)
        out.append([r[0] - 1 for r in rows])
    lat = np.array(lat) * 1e3
    return out, lat


def lat_str(lat):
    return f"avg {lat.mean():.2f}ms  p50 {np.percentile(lat, 50):.2f}  p95 {np.percentile(lat, 95):.2f}"


# ---------------------------------------------------------------- IR metrics
def ir_metrics(ranked, qrels, k=10):
    """ranked: {qid: [doc_id,...]}, qrels: {qid: set(doc_id)}."""
    ndcg, mrr, rec, hit1 = [], [], [], []
    for qid, rel in qrels.items():
        r = ranked[qid][:k]
        gains = [1.0 if d in rel else 0.0 for d in r]
        dcg = sum(g / np.log2(i + 2) for i, g in enumerate(gains))
        idcg = sum(1.0 / np.log2(i + 2) for i in range(min(len(rel), k)))
        ndcg.append(dcg / idcg)
        mrr.append(next((1.0 / (i + 1) for i, g in enumerate(gains) if g), 0.0))
        rec.append(sum(gains) / len(rel))
        hit1.append(gains[0] if gains else 0.0)
    return {"nDCG@10": float(np.mean(ndcg)), "MRR@10": float(np.mean(mrr)),
            "Recall@10": float(np.mean(rec)), "Hit@1": float(np.mean(hit1))}


def fmt_ir(m):
    return "  ".join(f"{k} {v:.4f}" for k, v in m.items())


# ================================================================ Layer A
def layer_a(model, results):
    from datasets import load_dataset

    log("\n=== A. SciFact (5,183 abstracts, 300 test queries, exact-scan path) ===")
    corpus = load_dataset("mteb/scifact", "corpus")["corpus"]
    queries = load_dataset("mteb/scifact", "queries")["queries"]
    qrels_ds = load_dataset("mteb/scifact", "default")["test"]

    doc_ids = [r["_id"] for r in corpus]
    doc_texts = [(r["title"] + " " + r["text"]).strip() for r in corpus]
    qrels = {}
    for r in qrels_ds:
        if r["score"] > 0:
            qrels.setdefault(r["query-id"], set()).add(r["corpus-id"])
    qmap = {r["_id"]: r["text"] for r in queries}
    qids = [q for q in qmap if q in qrels]
    q_texts = [qmap[q] for q in qids]
    log(f"  docs={len(doc_texts)}  queries={len(qids)}  "
        f"qrels={sum(len(v) for v in qrels.values())}")

    D = embed(model, doc_texts, "scifact-docs")
    Q = embed(model, q_texts, "scifact-queries")

    tmp = tempfile.mkdtemp(prefix="motedb_eval_a_")
    try:
        db = open_db(os.path.join(tmp, "scifact.mote"))
        create_table(db, "docs")
        insert_rows(db, "docs", D)
        t0 = time.perf_counter()
        db.execute("CREATE VECTOR INDEX docs_emb ON docs(emb)")
        db.checkpoint()
        log(f"  CREATE VECTOR INDEX + checkpoint: {time.perf_counter() - t0:.2f}s")

        K = 10
        truth = exact_topk(D, Q, K)
        res = {}
        for op, name in (("<->", "L2"), ("<=>", "cosine")):
            got, lat = db_topk(db, "docs", Q, K, op)
            r = {f"recall@{k}": recall_at(got, truth, k) for k in (1, 5, 10)}
            log(f"  DB {name:6} vs numpy exact:  " +
                "  ".join(f"{k} {v:.4f}" for k, v in r.items()) + f"   [{lat_str(lat)}]")
            ranked = {qid: [doc_ids[i] for i in g] for qid, g in zip(qids, got)}
            ir = ir_metrics(ranked, qrels)
            log(f"  DB {name:6} vs qrels:        {fmt_ir(ir)}")
            res[name] = {"fidelity": r, "ir": ir, "latency_ms_avg": float(lat.mean())}

        ranked_np = {qid: [doc_ids[i] for i in t] for qid, t in zip(qids, truth)}
        ir_np = ir_metrics(ranked_np, qrels)
        log(f"  numpy exact vs qrels:     {fmt_ir(ir_np)}   (model ceiling)")
        res["numpy_exact_ir"] = ir_np

        # A few concrete examples so the numbers are tangible.
        log("\n  sample queries (DB L2 top-1):")
        got, _ = db_topk(db, "docs", Q[:4], 1)
        for qid, qt, g in zip(qids[:4], q_texts[:4], got):
            hit = doc_ids[g[0]] in qrels[qid]
            title = corpus[g[0]]["title"][:70]
            log(f"    {'✓' if hit else '✗'} Q: {qt[:70]}")
            log(f"        → {title}")
        db.close()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    results["scifact"] = res


# ================================================================ Layer B
def build_nli_sentences(n_needed):
    from datasets import load_dataset

    ds = load_dataset("sentence-transformers/all-nli", "pair")["train"]
    sents = set()
    for col in ("anchor", "positive"):
        for s in ds[col]:
            s = " ".join(s.split())
            if 15 <= len(s) <= 300:
                sents.add(s)
    sents = sorted(sents)
    rng = np.random.default_rng(42)
    rng.shuffle(sents)
    if len(sents) < n_needed:
        raise SystemExit(f"all-nli has only {len(sents)} unique sentences, need {n_needed}")
    return sents


def layer_b(model, results, n_corpus, n_queries, n_extra, preset, reopen_preset):
    log(f"\n=== B. all-nli sentences: {n_corpus:,} rows (> 200K ⇒ DiskANN path) "
        f"preset={preset} reopen={reopen_preset} ===")
    sents = build_nli_sentences(n_corpus + n_queries + n_extra)
    corpus = sents[:n_corpus]
    held_out = sents[n_corpus:n_corpus + n_queries]
    extra = sents[n_corpus + n_queries:n_corpus + n_queries + n_extra]
    log(f"  unique sentences available: {len(sents):,}   "
        f"corpus={len(corpus):,}  queries={len(held_out)}  extra={len(extra)}")

    D = embed(model, corpus, "nli-corpus")
    Q = embed(model, held_out, "nli-queries")
    E = embed(model, extra, "nli-extra") if n_extra else np.zeros((0, DIM), np.float32)

    rng = np.random.default_rng(7)
    self_idx = rng.choice(len(D), size=min(200, len(D)), replace=False)
    QS = D[self_idx]

    dn = (D * D).sum(1)
    K = 100
    truth = exact_topk(D, Q, K, dn)

    res = {}
    tmp = tempfile.mkdtemp(prefix="motedb_eval_b_")
    path = os.path.join(tmp, "nli.mote")
    try:
        db = open_db(path, preset)
        create_table(db, "nli")
        res["insert_s"] = insert_rows(db, "nli", D)
        db.checkpoint()

        def measure(label, k_list=(1, 10, 100)):
            got, lat = db_topk(db, "nli", Q, K)
            r = {f"recall@{k}": recall_at(got, truth, k) for k in k_list}
            log(f"  {label:34} " + "  ".join(f"{k} {v:.4f}" for k, v in r.items())
                + f"   [{lat_str(lat)}]")
            r["latency_ms_avg"] = float(lat.mean())
            r["latency_ms_p95"] = float(np.percentile(lat, 95))
            return r

        # 1. Same table, no index → exact SIMD scan (control).
        res["no_index_exact"] = measure("no index (exact scan)")

        # 2. Build DiskANN (~5 ms/row measured at 20K-40K rows).
        log(f"  building vector index on {n_corpus:,}×{DIM} "
            f"(expect ~{n_corpus * 5.5 / 1000 / 60:.0f} min) ...")
        t0 = time.perf_counter()
        db.execute("CREATE VECTOR INDEX nli_emb ON nli(emb)")
        db.checkpoint()
        res["index_build_s"] = time.perf_counter() - t0
        log(f"  CREATE VECTOR INDEX on {n_corpus:,}×{DIM}: {res['index_build_s']:.1f}s")
        res["diskann"] = measure("DiskANN")

        # 3. Self-queries: stored vector as query must come back first.
        got, lat = db_topk(db, "nli", QS, 1)
        self_hit = float(np.mean([g[0] == i for g, i in zip(got, self_idx)]))
        log(f"  DiskANN self-query top-1 == self:  {self_hit:.4f}   [{lat_str(lat)}]")
        res["diskann_self_hit"] = self_hit

        # 4. Reopen from disk (optionally under a different preset, since the
        #    cache sizing differs between edge/general).
        db.close()
        db = open_db(path, reopen_preset)
        res["diskann_reopen"] = measure(f"DiskANN after reopen ({reopen_preset})")

        # 5. Incremental inserts through the live index, then re-measure
        #    against ground truth that now includes the new rows.
        if n_extra:
            t0 = time.perf_counter()
            insert_rows(db, "nli", E, id_offset=len(D))
            db.checkpoint()
            D2 = np.vstack([D, E])
            truth2 = exact_topk(D2, Q, K)
            got, lat = db_topk(db, "nli", Q, K)
            r = {f"recall@{k}": recall_at(got, truth2, k) for k in (1, 10, 100)}
            # How many of the exact top-10 are new rows — shows the new rows
            # actually matter for these queries.
            new_share = float(np.mean([(t[:10] >= len(D)).mean() for t in truth2]))
            log(f"  {'DiskANN +' + str(n_extra) + ' incremental rows':34} "
                + "  ".join(f"{k} {v:.4f}" for k, v in r.items())
                + f"   [{lat_str(lat)}]  (new rows in exact top-10: {new_share:.1%})")
            r["new_rows_share_in_top10"] = new_share
            res["diskann_after_incremental"] = r
        db.close()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    results["nli"] = res


# ================================================================ main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-a", action="store_true")
    ap.add_argument("--skip-b", action="store_true")
    ap.add_argument("--n-corpus", type=int, default=220_000)
    ap.add_argument("--n-queries", type=int, default=200)
    ap.add_argument("--n-extra", type=int, default=5_000,
                    help="rows inserted incrementally after the index exists")
    ap.add_argument("--preset", default="edge",
                    help="preset for layer B load/build (edge: periodic fsync, "
                         "~50K rows/s; general: group-commit, ~260 rows/s)")
    ap.add_argument("--reopen-preset", default="general",
                    help="preset used when reopening the layer-B store")
    ap.add_argument("--out", default=os.path.join(CACHE, "results.json"))
    args = ap.parse_args()

    import motedb
    log(f"motedb python binding: {motedb.__file__}")
    model = load_model()
    results = {"model": MODEL, "dim": DIM}
    t0 = time.perf_counter()
    if not args.skip_a:
        layer_a(model, results)
    if not args.skip_b:
        layer_b(model, results, args.n_corpus, args.n_queries, args.n_extra,
                args.preset, args.reopen_preset)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(results, f, indent=2)
    log(f"\nresults → {args.out}   (total {time.perf_counter() - t0:.0f}s)")


if __name__ == "__main__":
    main()
