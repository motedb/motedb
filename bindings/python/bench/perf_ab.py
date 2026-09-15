#!/usr/bin/env python3
"""Round 11 A/B 焦点基准：Round 10 改动触及的路径 + 机内 numpy 对照。
交替运行消除机器负载漂移。用法：python3 ab_r11.py
"""
import json, os, shutil, tempfile, time
import numpy as np

N, DIM = 100_000, 384

def gen():
    rng = np.random.default_rng(42)
    ts = (1_700_000_000 + np.sort(rng.integers(0, 30 * 24 * 3600, N))) * 1_000_000
    dev = np.array([f"dev-{i % 64:02d}" for i in range(N)])
    val = rng.standard_normal(N).astype(np.float32) * 10
    emb = rng.standard_normal((N, DIM)).astype(np.float32)
    return ts, dev, val, emb

def lat(f, iters):
    out = []
    for _ in range(iters):
        t0 = time.perf_counter(); f(); out.append(time.perf_counter() - t0)
    a = np.asarray(out) * 1e3
    return round(float(np.percentile(a, 50)), 3)

def run():
    import motedb
    ts, dev, val, emb = gen()
    tmp = tempfile.mkdtemp()
    db = motedb.Database(os.path.join(tmp, "a.mote"), preset="general")
    db.execute("CREATE TABLE ev (id INT PRIMARY KEY, ts TIMESTAMP, device TEXT, val FLOAT, emb VECTOR(384))")
    rows = [(i, int(ts[i]), str(dev[i]), float(val[i]), emb[i].tolist()) for i in range(N)]
    for i in range(0, N, 5000):
        db.executemany("INSERT INTO ev VALUES (?, ?, ?, ?, ?)", rows[i:i+5000][0:0] or
                       [(r[0], r[1], r[2], r[3], r[4]) for r in rows[i:i+5000]])
    db.checkpoint(); db.close()
    db = motedb.Database(os.path.join(tmp, "a.mote"))

    rng = np.random.default_rng(1)
    qs = emb[rng.integers(0, N, 30)]
    vi = [0]
    def q_vec():
        vi[0] = (vi[0] + 1) % 30
        db.query("SELECT id FROM ev ORDER BY emb <-> ? LIMIT 10", params=[qs[vi[0]].tolist()])
    def q_topk():
        db.query("SELECT id FROM ev ORDER BY val ASC LIMIT 10")
    def q_range():
        i = int(rng.integers(0, N - N // 10))
        db.query("SELECT COUNT(*) FROM ev WHERE ts >= ? AND ts <= ?", params=[int(ts[i]), int(ts[i + N // 10])])
    def q_point():
        db.query("SELECT val FROM ev WHERE id = ?", params=[int(rng.integers(0, N))])
    q_vec(); q_topk(); q_range(); q_point()  # warm

    # numpy control（与引擎无关，衡量当次机器状态）
    def np_brute():
        d = ((emb - qs[vi[0]]) ** 2).sum(1)
        np.argpartition(d, 10)[:10]

    r = {
        "vector_knn10_p50_ms": lat(q_vec, 30),
        "ordered_limit_p50_ms": lat(q_topk, 100),
        "ts_range_count_p50_ms": lat(q_range, 50),
        "pk_point_p50_ms": lat(q_point, 300),
        "numpy_control_p50_ms": lat(np_brute, 15),
    }
    db.close(); shutil.rmtree(tmp, ignore_errors=True)
    return r

if __name__ == "__main__":
    print("JSON " + json.dumps(run()))
