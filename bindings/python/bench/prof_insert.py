#!/usr/bin/env python3
"""100K×384 导入吞吐正式基准：数据预生成（排除构建噪声），纯 API 吞吐。

用法: python3 prof_insert.py [n=100000] [batch=10000]
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
    sys.exit("numpy required for this bench")

N = int(sys.argv[1]) if len(sys.argv) > 1 else 100_000
BATCH = int(sys.argv[2]) if len(sys.argv) > 2 else 10_000
DIM = 384

# ── 预生成所有批次数据（不计时） ──
rng = np.random.default_rng(42)
batches = []
inserted = 0
while inserted < N:
    m = min(BATCH, N - inserted)
    batches.append((
        {
            "c": [f"cust-{(inserted + i) % 1000}" for i in range(m)],
            "v": [(inserted + i) * 0.5 for i in range(m)],
            "emb": rng.random((m, DIM), dtype=np.float32),
            "ts": [1700000000 + (inserted + i) for i in range(m)],
        },
        # executemany 对照行的构造数据
        [
            (
                f"cust-{(inserted + i) % 1000}",
                (inserted + i) * 0.5,
                rng.random(DIM).astype(np.float32).tolist(),
                1700000000 + (inserted + i),
            )
            for i in range(m)
        ],
    ))
    inserted += m

# ── 场景 1: insert_arrays（列式 numpy） ──
tmp = tempfile.mkdtemp()
db = motedb.Database(os.path.join(tmp, "a.mote"))
db.execute(
    f"CREATE TABLE t (id INTEGER PRIMARY KEY AUTO_INCREMENT, c TEXT, v FLOAT, emb VECTOR({DIM}), ts BIGINT)"
)
t0 = time.perf_counter()
for cols, _ in batches:
    db.insert_arrays("t", cols)
t_arr = time.perf_counter() - t0
cnt = db.execute("SELECT COUNT(*) FROM t")[0]["COUNT(*)"]
db.close()

# ── 场景 2: executemany（行式参数绑定, 含 .tolist 预转） ──
db = motedb.Database(os.path.join(tmp, "b.mote"))
db.execute(
    f"CREATE TABLE t (id INTEGER PRIMARY KEY AUTO_INCREMENT, c TEXT, v FLOAT, emb VECTOR({DIM}), ts BIGINT)"
)
t0 = time.perf_counter()
db.execute("BEGIN")
for _, rows in batches:
    db.executemany("INSERT INTO t (c, v, emb, ts) VALUES (?, ?, ?, ?)", rows)
db.execute("COMMIT")
t_em = time.perf_counter() - t0
cnt2 = db.execute("SELECT COUNT(*) FROM t")[0]["COUNT(*)"]
db.close()
os.system(f"rm -rf {tmp}")

print(
    f"import {N}x{DIM} batch={BATCH}:\n"
    f"  insert_arrays  {t_arr*1000:7.0f}ms  {N/t_arr/1000:6.0f}K rows/s  (count={cnt})\n"
    f"  executemany    {t_em*1000:7.0f}ms  {N/t_em/1000:6.0f}K rows/s  (count={cnt2})"
)
