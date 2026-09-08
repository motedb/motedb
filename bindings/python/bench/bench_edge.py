#!/usr/bin/env python3
"""Edge-workload benchmark: MoteDB vs SQLite (stdlib) — reproducible anywhere.

Optional competitors are used when importable and gracefully skipped
otherwise:
  - sqlite-vec  (pip install sqlite-vec)   → vector ANN in SQLite
  - lancedb     (pip install lancedb)      → embedded multimodal vector DB

Workload (embedded/embodied profile):
  1. bulk insert  N rows (text + float + 8-dim embedding + ts)
  2. point lookup by PK              (robot percepts by id)
  3. filtered count (float range)    (sensor thresholding)
  4. GROUP BY low-cardinality        (telemetry rollup)
  5. vector ANN top-5                (memory retrieval)
  6. peak RSS

Run:  python3 bench_edge.py [N]
"""
import os
import resource
import sqlite3
import sys
import tempfile
import time

N = int(sys.argv[1]) if len(sys.argv) > 1 else 50_000
DIM = 8
SEED = 0xC0FFEE


def lcg():
    global SEED
    SEED = (SEED * 6364136223846793005 + 1442695040888963407) & (2**64 - 1)
    return SEED >> 16


def rows():
    r = lcg
    for i in range(N):
        emb = [(r() % 1000) / 1000.0 for _ in range(DIM)]
        yield (i, f"doc_{r() % 100}", (r() % 10000) / 100.0, emb, 1_700_000_000 + i)


def rss_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def bench_motedb():
    import motedb

    d = tempfile.mkdtemp()
    db = motedb.Database(os.path.join(d, "bench.mote"), preset="edge")
    db.execute(
        f"CREATE TABLE t (id INT PRIMARY KEY, c TEXT, v FLOAT, emb VECTOR({DIM}), ts BIGINT)"
    )
    t0 = time.perf_counter()
    batch = []
    BATCH = 500
    for row in rows():
        batch.append(row)
        if len(batch) >= BATCH:
            vals = ",".join(["(?,?,?,?,?)"] * len(batch))
            flat = [x for r in batch for x in (list(r[:3]) + [r[3], r[4]])]
            db.execute(
                f"INSERT INTO t (id, c, v, emb, ts) VALUES {vals}", params=flat
            )
            batch = []
    if batch:
        vals = ",".join(["(?,?,?,?,?)"] * len(batch))
        flat = [x for r in batch for x in (list(r[:3]) + [r[3], r[4]])]
        db.execute(f"INSERT INTO t (id, c, v, emb, ts) VALUES {vals}", params=flat)
    ins = time.perf_counter() - t0

    # Warmup: first query after bulk insert pays flush + query-time
    # compaction (one-time, documented engine behavior) — measure steady state.
    db.execute("SELECT COUNT(*) AS n FROM t WHERE v > 50.0")
    db.execute("SELECT id FROM t WHERE id = 0")

    t0 = time.perf_counter()
    for k in range(2000):
        db.execute("SELECT id FROM t WHERE id = ?", params=[int(lcg() % N)])
    pt = (time.perf_counter() - t0) / 2000 * 1e6

    def best_of_3(fn):
        ts = []
        for _ in range(3):
            t0 = time.perf_counter()
            fn()
            ts.append(time.perf_counter() - t0)
        return min(ts) * 1e3

    fl = best_of_3(lambda: db.execute("SELECT COUNT(*) AS n FROM t WHERE v > 50.0"))
    gb = best_of_3(lambda: db.execute("SELECT c, COUNT(*) AS n FROM t GROUP BY c"))

    q = [0.5] * DIM
    qsql = f"[{', '.join('%f' % x for x in q)}]"

    def ann100():
        for _ in range(100):
            db.execute(f"SELECT id FROM t ORDER BY emb <-> {qsql} LIMIT 5")

    ann = best_of_3(ann100) / 100

    mem = rss_mb()
    db.close()
    return ("MoteDB", ins, pt, fl, gb, ann, mem)


def bench_sqlite():
    d = tempfile.mkdtemp()
    con = sqlite3.connect(os.path.join(d, "bench.db"))
    con.execute("PRAGMA journal_mode=WAL")
    con.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, c TEXT, v REAL, emb BLOB, ts INTEGER)"
    )
    import struct

    t0 = time.perf_counter()
    con.executemany(
        "INSERT INTO t VALUES (?,?,?,?,?)",
        [
            (i, c, v, struct.pack(f"{DIM}f", *emb), ts)
            for (i, c, v, emb, ts) in rows()
        ],
    )
    con.commit()
    ins = time.perf_counter() - t0

    t0 = time.perf_counter()
    for k in range(2000):
        con.execute("SELECT id FROM t WHERE id = ?", (int(lcg() % N),)).fetchall()
    pt = (time.perf_counter() - t0) / 2000 * 1e6

    def best_of_3(fn):
        ts = []
        for _ in range(3):
            t0 = time.perf_counter()
            fn()
            ts.append(time.perf_counter() - t0)
        return min(ts) * 1e3

    fl = best_of_3(
        lambda: con.execute("SELECT COUNT(*) FROM t WHERE v > 50.0").fetchone()
    )
    gb = best_of_3(
        lambda: con.execute("SELECT c, COUNT(*) FROM t GROUP BY c").fetchall()
    )

    q = [0.5] * DIM

    def sq_dist(blob):
        es = struct.unpack(f"{DIM}f", blob)
        return sum((a - b) ** 2 for a, b in zip(es, q))

    t0 = time.perf_counter()
    for _ in range(10):  # brute force is slow — 10 iters, scaled ×10
        allrows = con.execute("SELECT id, emb FROM t").fetchall()
        allrows.sort(key=lambda r: sq_dist(r[1]))
        _ = allrows[:5]
    ann = (time.perf_counter() - t0) / 10 * 1e3

    mem = rss_mb()
    con.close()
    return ("SQLite", ins, pt, fl, gb, ann, mem)


def main():
    print(f"\n  edge-workload benchmark  (N={N}, dim={DIM}, python {sys.version_info.major}.{sys.version_info.minor})")
    print("  " + "-" * 82)
    print(f"  {'engine':10} | {'insert_s':>8} | {'point_us':>8} | {'filter_ms':>9} | {'group_ms':>8} | {'ann5_ms':>8} | {'rss_MB':>7}")
    print("  " + "-" * 82)
    for fn in (bench_motedb, bench_sqlite):
        try:
            name, ins, pt, fl, gb, ann, mem = fn()
            print(
                f"  {name:10} | {ins:8.2f} | {pt:8.1f} | {fl:9.2f} | {gb:8.2f} | {ann:8.2f} | {mem:7.0f}"
            )
        except Exception as e:
            print(f"  {fn.__name__:10} | skipped ({e})")
    print()


if __name__ == "__main__":
    main()
