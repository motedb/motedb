#!/usr/bin/env python3
"""Scale probe (D1, vector side): 1M × 384 table — load + brute knn.
Per the D1 plan the vector table only exercises LOAD and KNN (index build
at 1M is extrapolated from the known 220K build time).

Usage: python3 prof_scale_vec.py [--n 1000000]
"""
import argparse
import os
import resource
import shutil
import sys
import tempfile
import time

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import motedb  # noqa: E402


def peak_rss_mb():
    v = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return v / (1024 * 1024) if v > 10_000_000 else v / 1024.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=1_000_000)
    ap.add_argument("--dim", type=int, default=384)
    args = ap.parse_args()
    N, D = args.n, args.dim

    tmp = tempfile.mkdtemp(prefix="motedb_scale_vec_")
    path = os.path.join(tmp, "vec.mote")
    print(f"D1 vector probe: {N:,} × {D}d at {tmp}")
    try:
        # Gaussian-mixture corpus (same family as prof_ann.py: navigable,
        # not uniform-random worst case), generated + inserted in batches so
        # the probe never holds the full 1.5GB corpus plus a second copy.
        rng = np.random.default_rng(11)
        centers = rng.standard_normal((64, D)).astype(np.float32)

        t0 = time.perf_counter()
        db = motedb.Database(path, preset="general")
        db.execute(f"CREATE TABLE ev (id INT PRIMARY KEY, emb VECTOR({D}))")
        B = 20_000
        data_parts = []
        for s in range(0, N, B):
            j = min(s + B, N)
            batch = (
                centers[rng.integers(0, 64, j - s)]
                + rng.standard_normal((j - s, D)).astype(np.float32) * np.float32(0.30)
            )
            data_parts.append(batch)
            db.insert_arrays("ev", {"id": range(s, j), "emb": batch})
        data = np.concatenate(data_parts)
        del data_parts
        load_s = time.perf_counter() - t0
        print(f"  load: {load_s:7.2f}s  {N / load_s:,.0f} rows/s"
              f"  peakRSS {peak_rss_mb():.0f}MB")

        t0 = time.perf_counter()
        db.checkpoint()
        db.close()
        ck_s = time.perf_counter() - t0
        print(f"  checkpoint+close: {ck_s:6.2f}s  peakRSS {peak_rss_mb():.0f}MB")
        db = motedb.Database(path, preset="general")

        # Brute knn (no index): the engine's parallel vector scan.
        qs = data[rng.integers(0, N, 30)].astype(np.float32)
        lat = []
        got = []
        for q in qs[:5]:  # warm
            db.query(
                "SELECT id FROM ev ORDER BY emb <-> ? LIMIT 10",
                params=[q.tolist()],
            )
        for q in qs:
            t0 = time.perf_counter()
            _, rows = db.query(
                "SELECT id FROM ev ORDER BY emb <-> ? LIMIT 10",
                params=[q.tolist()],
            )
            lat.append(time.perf_counter() - t0)
            # ids were inserted 0-based (range(s, j)) — SQL id == numpy row.
            got.append([r[0] for r in rows])
        lat.sort()
        # recall vs numpy brute force — via |x|² - 2x·q + |q|² (matmul).
        # A direct broadcast diff materializes 30×1M×384 floats ≈ 46GB in
        # one expression — it OOM'd the host the first time this ran.
        norm2 = np.einsum("ij,ij->i", data, data)
        cross = data @ qs.T  # (N, 30)
        q2 = np.einsum("ij,ij->i", qs, qs)
        truth = []
        for i in range(len(qs)):
            d2i = norm2 - 2 * cross[:, i] + q2[i]
            truth.append(set(np.argpartition(d2i, 10)[:10].tolist()))
        rec = np.mean(
            [len(g & t) / 10 for g, t in zip(map(set, got), truth)]
        )
        print(f"  brute knn (no index): p50 {lat[15]*1e3:.0f}ms  p95 {lat[28]*1e3:.0f}ms"
              f"  recall@10 {rec:.3f}  peakRSS {peak_rss_mb():.0f}MB")

        disk = sum(
            os.path.getsize(os.path.join(dp, f))
            for dp, _, fs in os.walk(tmp)
            for f in fs
        )
        print(f"  disk footprint: {disk/1e6:.0f}MB")
        return 0
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
