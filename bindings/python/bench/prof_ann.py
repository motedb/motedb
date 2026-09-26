#!/usr/bin/env python3
"""A 系列 ANN 基准 (行业差距补齐计划 A0): 220K×384 索引搜索 p50/p95 +
recall@10 vs numpy 暴力精确, 索引持久化在 ~/.cache/motedb_ann_bench/
跨里程碑复用 (A1/A2 只改搜索, 不改图格式, 同一索引可重测)。

用法: python3 prof_ann.py [--n 220000] [--q 200] [--rebuild]
"""
import argparse
import os
import shutil
import sys
import time

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import motedb

ROOT = os.path.expanduser("~/.cache/motedb_ann_bench")
DIM = 384


def splitmix_vec(i, d, salt=0):
    """确定性伪随机向量 (同 tests/test_knn_parallel.rs) — 可复现 ground truth。"""
    z = (i * 0x9E3779B97F4A7C15) ^ (d * 0xBF58476D1CE4E5B9) ^ (salt * 0x94D049BB133111EB)
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
    z ^= z >> 31
    return ((z >> 40) / (1 << 24)) * 2.0 - 1.0


def build_corpus(n, dim, clusters=1024, noise_scale=0.30):
    """高斯混合簇语料 (确定性): 1024 个 splitmix 簇心 (单位立方体内),
    每行 = 簇心 + N(0, noise)。真实嵌入分布近似 (sentence embeddings 呈
    簇结构) — 图索引的可导航性依赖簇结构。

    🔑 教训: 首版用 384 维均匀随机 — 图导航最坏情形 (无簇, 长程边被
    robust_prune 剪断), recall@10 实测 0.10 (自查询连距离 0 的自身都
    找不到); 同代码在真实 sentence 嵌入上 recall 0.99。ANN 基准必须用
    聚簇分布。"""
    M1 = np.uint64(0x9E3779B97F4A7C15)
    M2 = np.uint64(0xBF58476D1CE4E5B9)
    M3 = np.uint64(0x94D049BB133111EB)

    def splitmix_stream(seed, count):
        idx = np.arange(count, dtype=np.uint64) + np.uint64(seed)
        out = np.empty((count,), dtype=np.float64)
        for _ in range(1):  # 单轮混合足够
            z = (idx * M1)
            z = (z ^ (z >> np.uint64(30))) * M2
            z = (z ^ (z >> np.uint64(27))) * M3
            z = z ^ (z >> np.uint64(31))
            out = (z >> np.uint64(40)) / float(1 << 24)
        return out

    centers = np.empty((clusters, dim), dtype=np.float64)
    for d in range(dim):
        centers[:, d] = splitmix_stream(d * 1000 + 1, clusters)
    assign = np.arange(n, dtype=np.int64) % clusters
    noise = np.random.default_rng(7).standard_normal((n, dim)) * noise_scale
    corpus = (centers[assign] + noise).astype(np.float32)
    return corpus


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=220_000)
    ap.add_argument("--q", type=int, default=200)
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--rebuild", action="store_true")
    args = ap.parse_args()

    d = os.path.join(ROOT, f"idx-{args.n}-{DIM}")
    db_path = os.path.join(d, "bench.mote")
    corpus_path = os.path.join(d, "corpus.npy")
    marker = os.path.join(d, "BUILT")

    if args.rebuild and os.path.isdir(d):
        shutil.rmtree(d)
    os.makedirs(d, exist_ok=True)

    if not os.path.exists(marker):
        corpus = build_corpus(args.n, DIM)
        np.save(corpus_path, corpus)
        db = motedb.Database(db_path)
        db.execute(
            f"CREATE TABLE t (id INTEGER PRIMARY KEY AUTO_INCREMENT, emb VECTOR({DIM}))"
        )
        t0 = time.perf_counter()
        B = 50_000
        for i in range(0, args.n, B):
            j = min(i + B, args.n)
            db.insert_arrays("t", {"emb": corpus[i:j]})
        t_load = time.perf_counter() - t0
        print(f"load {args.n} rows: {t_load:.1f}s ({args.n/t_load/1000:.0f}K rows/s)")

        t0 = time.perf_counter()
        db.execute("CREATE VECTOR INDEX t_emb ON t(emb)")
        t_build = time.perf_counter() - t0
        print(f"CREATE VECTOR INDEX: {t_build:.1f}s  ({t_build/args.n*1e3:.2f} ms/row)")
        db.close()
        open(marker, "w").write(f"build_s={t_build:.1f}\n")
        print(f"index persisted at {d} (后续里程碑复用, 免重建)")
    else:
        print(f"reusing persisted index at {d}")

    corpus = np.load(corpus_path)
    n = len(corpus)

    # 查询向量: 语料前 args.q 行 (自助查询, 便于 recall@1 判定) + 随机混合
    rng = np.random.default_rng(42)
    qidx = rng.integers(0, n, args.q)
    queries = corpus[qidx].astype(np.float32)

    # numpy 暴力 ground truth (L2^2 与引擎 <-> 一致)
    qs = queries.astype(np.float32)
    qn = (qs * qs).sum(axis=1, keepdims=True)
    cn = (corpus * corpus).sum(axis=1)
    gt = np.empty((args.q, args.k), dtype=np.int64)
    CH = 50
    for s in range(0, args.q, CH):
        e = min(s + CH, args.q)
        d2 = qn[s:e] + cn - 2.0 * (qs[s:e] @ corpus.T)
        # 🔑 表 id 是 1 基自增 (row_id 1..n), numpy 下标 0 基 — +1 对齐。
        # 🔑 argpartition 的前 k 个无序 (position 0 未必最小) — recall@1
        # 统计错序的假阴性 (二版基准 recall@1=0.215 的根因, 引擎实际 0/30
        # 错序)。排序后再取。
        part = np.argpartition(d2, args.k, axis=1)[:, : args.k]
        for r in range(part.shape[0]):
            part[r] = part[r][np.argsort(d2[r][part[r]])]
        gt[s:e] = part + 1

    db = motedb.Database(db_path)
    # warmup 5
    for i in range(5):
        db.query("SELECT id FROM t ORDER BY emb <-> ? LIMIT 10",
                 params=[queries[i].tolist()])

    times = []
    hits10 = 0
    hits1 = 0
    for i in range(args.q):
        q = queries[i].tolist()
        t0 = time.perf_counter()
        _, rows = db.query("SELECT id FROM t ORDER BY emb <-> ? LIMIT 10", params=[q])
        times.append((time.perf_counter() - t0) * 1e3)
        got = {r[0] for r in rows}
        hits10 += len(got & set(gt[i].tolist()))
        hits1 += 1 if rows and rows[0][0] in set(gt[i, :1].tolist()) else 0

    ts = np.array(times)
    print(f"\nANN search p50={np.percentile(ts,50):.2f}ms  p95={np.percentile(ts,95):.2f}ms  "
          f"p99={np.percentile(ts,99):.2f}ms  avg={ts.mean():.2f}ms")
    print(f"recall@{args.k} = {hits10/(args.q*args.k):.4f}   recall@1 = {hits1/args.q:.4f}")
    db.close()


if __name__ == "__main__":
    main()
