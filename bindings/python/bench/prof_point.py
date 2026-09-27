#!/usr/bin/env python3
"""Point-read profile: mote vs SQLite, compete_bench's `q_point` shape
(SELECT val FROM ev WHERE id = ?) on a 100K × (id,ts,device,val,note,emb)
table. The projected fast-PK path (C1) must decode ONLY `val` — the row
holds a 384-float embedding and text that must stay untouched.

Usage: python3 prof_point.py [--n 100000] [--q 2000]
"""
import argparse
import os
import shutil
import sqlite3
import struct
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import motedb  # noqa: E402


def percentile(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p / 100.0))]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=100_000)
    ap.add_argument("--q", type=int, default=2000)
    args = ap.parse_args()
    N, Q = args.n, args.q

    tmp = tempfile.mkdtemp(prefix="motedb_point_")
    try:
        import random
        rng = random.Random(7)
        emb_blob = struct.pack(f"{384}f", *([0.5] * 384))
        path = os.path.join(tmp, "p.mote")
        db = motedb.Database(path, preset="general")
        db.execute(
            "CREATE TABLE ev (id INT PRIMARY KEY, ts TIMESTAMP, device TEXT,"
            " val FLOAT, note TEXT, emb VECTOR(384))"
        )
        B = 5000
        for s in range(0, N, B):
            rows = []
            for i in range(s, min(s + B, N)):
                rows.append((i, 1_700_000_000_000_000 + i * 1000,
                             f"dev-{i % 16}", i * 0.25, f"note {i} " + "x" * 40, emb_blob))
            db.executemany("INSERT INTO ev VALUES (?, ?, ?, ?, ?, ?)", rows)
        db.checkpoint()
        db.close()
        db = motedb.Database(path, preset="general")

        # mote: distinct random ids, fresh param each call
        ids = [rng.randrange(N) for _ in range(Q)]
        for i in ids[:50]:  # warm caches
            db.query("SELECT val FROM ev WHERE id = ?", params=[i])
        ts = []
        for i in ids:
            t0 = time.perf_counter()
            db.query("SELECT val FROM ev WHERE id = ?", params=[i])
            ts.append(time.perf_counter() - t0)
        m50, m95 = percentile(ts, 50) * 1e6, percentile(ts, 95) * 1e6

        # multi-column projection (2 cols)
        ts2 = []
        for i in ids:
            t0 = time.perf_counter()
            db.query("SELECT device, val FROM ev WHERE id = ?", params=[i])
            ts2.append(time.perf_counter() - t0)
        d50, d95 = percentile(ts2, 50) * 1e6, percentile(ts2, 95) * 1e6

        # SELECT * (full row incl. 384-float emb) for contrast
        ts3 = []
        for i in ids[:500]:
            t0 = time.perf_counter()
            db.query("SELECT * FROM ev WHERE id = ?", params=[i])
            ts3.append(time.perf_counter() - t0)
        s50, s95 = percentile(ts3, 50) * 1e6, percentile(ts3, 95) * 1e6

        # sqlite reference
        con = sqlite3.connect(os.path.join(tmp, "p.sqlite"))
        con.execute("CREATE TABLE ev (id INTEGER PRIMARY KEY, ts INTEGER, device TEXT,"
                    " val REAL, note TEXT, emb BLOB)")
        con.executemany(
            "INSERT INTO ev VALUES (?, ?, ?, ?, ?, ?)",
            ((i, 1_700_000_000_000_000 + i * 1000, f"dev-{i % 16}", i * 0.25,
              f"note {i} " + "x" * 40, emb_blob) for i in range(N)),
        )
        con.commit()
        for i in ids[:50]:
            con.execute("SELECT val FROM ev WHERE id = ?", (i,)).fetchone()
        ts4 = []
        for i in ids:
            t0 = time.perf_counter()
            con.execute("SELECT val FROM ev WHERE id = ?", (i,)).fetchone()
            ts4.append(time.perf_counter() - t0)
        q50, q95 = percentile(ts4, 50) * 1e6, percentile(ts4, 95) * 1e6

        print(f"{'shape':<28} {'p50':>9} {'p95':>9}")
        print(f"{'mote SELECT val (1 col)':<28} {m50:7.1f}µs {m95:7.1f}µs")
        print(f"{'mote SELECT device,val':<28} {d50:7.1f}µs {d95:7.1f}µs")
        print(f"{'mote SELECT * (full row)':<28} {s50:7.1f}µs {s95:7.1f}µs")
        print(f"{'sqlite SELECT val':<28} {q50:7.1f}µs {q95:7.1f}µs")
        return 0
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
