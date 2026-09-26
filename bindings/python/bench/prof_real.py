#!/usr/bin/env python3
"""真实嵌入 (all-nli) ANN 查询循环 — 索引持久化, 供 sample 剖析查询期。"""
import glob
import os
import sys
import time

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import motedb

CACHE = os.path.expanduser("~/.cache/motedb_eval")
ROOT = os.path.expanduser("~/.cache/motedb_ann_bench/real-220k")
DIM = 384


def find_embeddings():
    for p in glob.glob(os.path.join(CACHE, "nli-corpus-220000-*.npy")):
        a = np.load(p)
        if a.shape == (220000, DIM):
            return a
    raise SystemExit("no cached 220000x384 corpus under " + CACHE)


def main():
    n = 220_000
    corpus = find_embeddings()
    db_path = os.path.join(ROOT, "bench.mote")
    marker = os.path.join(ROOT, "BUILT")
    if not os.path.exists(marker):
        os.makedirs(ROOT, exist_ok=True)
        db = motedb.Database(db_path)
        db.execute(f"CREATE TABLE t (id INTEGER PRIMARY KEY AUTO_INCREMENT, emb VECTOR({DIM}))")
        t0 = time.perf_counter()
        for i in range(0, n, 50_000):
            j = min(i + 50_000, n)
            db.insert_arrays("t", {"emb": corpus[i:j]})
        db.execute("CREATE VECTOR INDEX t_emb ON t(emb)")
        print(f"build {time.perf_counter()-t0:.0f}s")
        db.close()
        open(marker, "w").write("ok")
    corpus = corpus.astype(np.float32)

    db = motedb.Database(db_path)
    # 真实 held-out 查询 (比 self 查询难得多 — self 的 top-1 即自身,
    # 行走瞬间收敛; eval 口径 5.7ms vs self 循环 1.4ms 的差异来源)
    qfiles = sorted(glob.glob(os.path.join(CACHE, "nli-queries-*.npy")), reverse=True)
    qs = np.load(qfiles[0]).astype(np.float32) if qfiles else corpus[:1000]
    for i in range(5):
        db.query("SELECT id FROM t ORDER BY emb <-> ? LIMIT 10", params=[qs[i].tolist()])
    t0 = time.perf_counter()
    i = 0
    while time.perf_counter() - t0 < 8:
        db.query("SELECT id FROM t ORDER BY emb <-> ? LIMIT 10", params=[qs[i % len(qs)].tolist()])
        i += 1
    print(f"{i} queries in 8s")
    db.close()


if __name__ == "__main__":
    main()
