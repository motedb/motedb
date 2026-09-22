#!/usr/bin/env python3
"""M1/M4 路径并行化 + GROUP BY &str 路径基线/验证基准 (1M 行, MOTE_VEC=on)。

形状:
  A range COUNT+AVG (M1 try_vec_no_group_aggregate)
  B filter top-k (M4 try_vec_filter_topk)
  C 深分页 top-k (M4)
  D GROUP BY device 无 ORDER (&str 旧路径 / M2)
  E GROUP BY + ORDER BY (M2)
"""
import os
import sys
import time
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import motedb

try:
    import numpy as np
except ImportError:
    sys.exit("numpy required")

N = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
BATCH = 50_000

tmp = tempfile.mkdtemp()
path = os.path.join(tmp, "b.mote")
db = motedb.Database(path)
db.execute(
    "CREATE TABLE s (id INTEGER PRIMARY KEY AUTO_INCREMENT, device TEXT, zone TEXT, val FLOAT, ts TIMESTAMP)"
)

rng = np.random.default_rng(7)
devs = np.array([f"dev-{i:03d}" for i in range(64)])
zones = np.array(["north", "south", "east", "west"])
t0 = time.perf_counter()
inserted = 0
while inserted < N:
    m = min(BATCH, N - inserted)
    cols = {
        "device": list(devs[rng.integers(0, len(devs), m)]),
        "zone": list(zones[rng.integers(0, len(zones), m)]),
        "val": rng.random(m) * 1000.0,
        "ts": [
            (1700000000 + int(x)) * 1_000_000
            for x in np.sort(rng.integers(0, 30 * 24 * 3600, m))
        ],
    }
    db.insert_arrays("s", cols)
    inserted += m
db.execute("CHECKPOINT")
t_load = time.perf_counter() - t0
print(f"loaded+checkpoint {N} rows in {t_load:.1f}s")

QUERIES = [
    ("A range COUNT+AVG", "SELECT COUNT(*), AVG(val) FROM s WHERE val > 500.0"),
    ("B filter top-k", "SELECT id, device, ts FROM s WHERE device = 'dev-007' ORDER BY ts DESC LIMIT 10"),
    ("C deep page top-k", "SELECT id FROM s WHERE val < 100.0 ORDER BY id LIMIT 100 OFFSET 999000"),
    ("D groupby no-order", "SELECT device, COUNT(*) FROM s GROUP BY device"),
    ("E groupby order", "SELECT device, COUNT(*) AS c FROM s GROUP BY device ORDER BY c DESC LIMIT 10"),
]

R = int(os.environ.get("ROUNDS", "7"))
for name, sql in QUERIES:
    times = []
    last = None
    for _ in range(R):
        t = time.perf_counter()
        last = db.execute(sql)
        times.append((time.perf_counter() - t) * 1000)
    times.sort()
    p50 = times[len(times) // 2]
    print(f"  {name:<20} p50={p50:7.2f}ms  (min={times[0]:.2f})  rows={len(last)}")
    if os.environ.get("VERBOSE"):
        print("   ", str(last[:2])[:100])

db.close()
os.system(f"rm -rf {tmp}")
