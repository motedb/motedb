#!/usr/bin/env python3
"""向量检索行业对照：MoteDB DiskANN vs FAISS HNSW / IVFFlat / Flat（同一 220K×384 聚簇语料）。

复用 prof_ann.py 的语料与查询协议（corpus.npy + rng(42) 选 200 查询 + numpy 暴力
ground truth），保证 recall/延迟完全可比。FAISS 各索引同语料重建（nlist/nprobe、
efSearch 各档位）→ 同一批查询 → 同一判分。

输出: 每行一个 JSON: {engine_mode, recall@10, recall@1, p50_ms, p95_ms, p99_ms,
build_s, disk_mb, rss_mb}
"""
import json
import os
import resource
import shutil
import sys
import tempfile
import time

import numpy as np

BENCH = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BENCH)
sys.path.insert(0, os.path.join(BENCH, ".."))
import motedb  # noqa: E402

ROOT = os.path.expanduser("~/.cache/motedb_ann_bench")
CORPUS = os.path.join(ROOT, "idx-220000-384", "corpus.npy")
MOTE_DB = os.path.join(ROOT, "idx-220000-384", "bench.mote")
Q = 200
K = 10


def peak_rss_mb():
    v = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return v / (1024 * 1024) if v > 10_000_000 else v / 1024.0


def ground_truth(corpus, queries):
    qs = queries.astype(np.float32)
    qn = (qs * qs).sum(axis=1, keepdims=True)
    cn = (corpus * corpus).sum(axis=1)
    part = np.argpartition(qn + cn - 2.0 * (qs @ corpus.T), K, axis=1)[:, :K]
    gt = np.empty((len(queries), K), dtype=np.int64)
    for r in range(part.shape[0]):
        d = (qn[r] + cn - 2.0 * (qs[r] @ corpus.T))
        order = part[r][np.argsort(d[part[r]])]
        gt[r] = order
    return gt + 1  # mote 表 id 1 基


def bench_mote_diskann(corpus, queries, gt, out):
    db = motedb.Database(MOTE_DB)
    for i in range(5):
        db.query("SELECT id FROM t ORDER BY emb <-> ? LIMIT 10", params=[queries[i].tolist()])
    times, hits = [], 0
    for i in range(Q):
        t0 = time.perf_counter()
        _, rows = db.query("SELECT id FROM t ORDER BY emb <-> ? LIMIT 10",
                           params=[queries[i].tolist()])
        times.append(time.perf_counter() - t0)
        got = {r[0] for r in rows}
        hits += len(got & set(gt[i]))
    a = np.asarray(times) * 1e3
    disk = sum(os.path.getsize(os.path.join(MOTE_DB, f))
               for f in os.listdir(MOTE_DB)) / 1e6
    out.append({
        "engine_mode": "motedb_diskann",
        "recall@10": round(hits / (Q * K), 4),
        "recall@1": round(sum(1 for i in range(Q)
                              if gt[i][0] in {r[0] for r in db.query(
                                  "SELECT id FROM t ORDER BY emb <-> ? LIMIT 10",
                                  params=[queries[i].tolist()])[1]}) / Q, 4),
        "p50_ms": round(float(np.percentile(a, 50)), 3),
        "p95_ms": round(float(np.percentile(a, 95)), 3),
        "p99_ms": round(float(np.percentile(a, 99)), 3),
        "build_s": None,
        "disk_mb": round(disk, 1),
        "rss_mb": round(peak_rss_mb(), 1),
    })
    db.close()


def faiss_recall(idx, queries, gt, nprobe=None, ef=None):
    if nprobe is not None:
        faiss = sys.modules["faiss"]
        faiss.ParameterSpace().set_index_parameter(idx, "nprobe", nprobe)
    if ef is not None:
        faiss = sys.modules["faiss"]
        faiss.ParameterSpace().set_index_parameter(idx, "efSearch", ef)
    times, hits = [], 0
    I = np.empty((Q, K), dtype="int64")
    for i in range(Q):
        t0 = time.perf_counter()
        _, I[i] = idx.search(queries[i][None, :], K)
        times.append(time.perf_counter() - t0)
        hits += len(set(I[i].tolist()) & set(gt[i].tolist()))
    a = np.asarray(times) * 1e3
    return hits, a


def bench_faiss(corpus, queries, gt, out):
    import faiss
    d = corpus.shape[1]

    # Flat（精确基线）
    t0 = time.perf_counter()
    flat = faiss.IndexFlatL2(d)
    flat.add(np.ascontiguousarray(corpus))
    build_flat = time.perf_counter() - t0
    hits, a = faiss_recall(flat, queries, gt)
    out.append({"engine_mode": "faiss_flat_exact", "recall@10": round(hits / (Q * K), 4),
                "recall@1": round(sum(1 for i in range(Q) if gt[i][0] == I0(i, flat, queries)) / Q, 4),
                "p50_ms": round(float(np.percentile(a, 50)), 3),
                "p95_ms": round(float(np.percentile(a, 95)), 3),
                "p99_ms": round(float(np.percentile(a, 99)), 3),
                "build_s": round(build_flat, 3), "disk_mb": round(corpus.nbytes / 1e6, 1),
                "rss_mb": round(peak_rss_mb(), 1)})

    # IVFFlat
    nlist = 1024
    t0 = time.perf_counter()
    quant = faiss.IndexFlatL2(d)
    ivf = faiss.IndexIVFFlat(quant, d, nlist, faiss.METRIC_L2)
    ivf.train(np.ascontiguousarray(corpus[::2].copy()))
    ivf.add(np.ascontiguousarray(corpus))
    build_ivf = time.perf_counter() - t0
    for nprobe in (1, 8, 32):
        hits, a = faiss_recall(ivf, queries, gt, nprobe=nprobe)
        out.append({"engine_mode": f"faiss_ivfflat_nprobe{nprobe}",
                    "recall@10": round(hits / (Q * K), 4), "recall@1": None,
                    "p50_ms": round(float(np.percentile(a, 50)), 3),
                    "p95_ms": round(float(np.percentile(a, 95)), 3),
                    "p99_ms": round(float(np.percentile(a, 99)), 3),
                    "build_s": round(build_ivf, 3),
                    "disk_mb": None, "rss_mb": round(peak_rss_mb(), 1)})

    # HNSW
    t0 = time.perf_counter()
    hnsw = faiss.IndexHNSWFlat(d, 16, faiss.METRIC_L2)
    hnsw.add(np.ascontiguousarray(corpus))
    build_h = time.perf_counter() - t0
    for ef in (16, 32, 64):
        hits, a = faiss_recall(hnsw, queries, gt, ef=ef)
        out.append({"engine_mode": f"faiss_hnsw16_ef{ef}",
                    "recall@10": round(hits / (Q * K), 4), "recall@1": None,
                    "p50_ms": round(float(np.percentile(a, 50)), 3),
                    "p95_ms": round(float(np.percentile(a, 95)), 3),
                    "p99_ms": round(float(np.percentile(a, 99)), 3),
                    "build_s": round(build_h, 3),
                    "disk_mb": None, "rss_mb": round(peak_rss_mb(), 1)})


def I0(i, flat, queries):
    return int(flat.search(queries[i][None, :], 1)[1][0][0]) + 1


def main():
    corpus = np.load(CORPUS)
    rng = np.random.default_rng(42)
    queries = corpus[rng.integers(0, len(corpus), Q)].astype(np.float32)
    gt = ground_truth(corpus, queries)
    out = []
    bench_mote_diskann(corpus, queries, gt, out)
    bench_faiss(corpus, queries, gt, out)
    for row in out:
        print("JSON " + json.dumps(row))


if __name__ == "__main__":
    main()
