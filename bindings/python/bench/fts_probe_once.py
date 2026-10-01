#!/usr/bin/env python3
"""W4a probe: one fresh process, three query paths measured together.

Prints one line: unranked/ranked/point p50 (ms). Run many fresh processes to
see whether the ~100µs tax is process-wide (all three slow together) or
path-specific (unranked slow while ranked/point stay fast in the same process).
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import motedb  # noqa: E402
import numpy as np  # noqa: E402

DB = "/tmp/fts_probe_db/f.mote"
Q_UNRANKED = "SELECT id FROM ev WHERE MATCH(note, 'charlie delta') LIMIT 10"
Q_RANKED = ("SELECT id FROM ev WHERE MATCH(note, 'charlie delta') "
            "ORDER BY BM25_SCORE() DESC LIMIT 10")
Q_POINT = "SELECT id FROM ev WHERE id = 50000"


def p50(db, sql, iters=60):
    for _ in range(5):
        db.query(sql)
    ts = []
    for _ in range(iters):
        t0 = time.perf_counter()
        db.query(sql)
        ts.append((time.perf_counter() - t0) * 1e3)
    return float(np.percentile(ts, 50))


def main():
    db = motedb.Database(DB, preset="general")
    u = p50(db, Q_UNRANKED)
    r = p50(db, Q_RANKED)
    p = p50(db, Q_POINT)
    # interleave re-measure to rule out drift
    u2 = p50(db, Q_UNRANKED, 30)
    db.close()
    print(f"RESULT unranked={u:.4f}/{u2:.4f} ranked={r:.4f} point={p:.4f} pid={os.getpid()}")


if __name__ == "__main__":
    main()
