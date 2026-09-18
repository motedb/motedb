#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""资源消耗全面测评 (v0.10.0): 内存 / 磁盘 / 时间 / CPU × 全生命周期阶段.

维度:
  A 工件大小 (CLI/.so/wheel)          F 查询期内存 (逐查询类, 峰值Δ中位数)
  B 空库/常开足迹                     G steady-state (缓存填满后 RSS)
  C 加载: 吞吐/峰值RSS/盘(WAL→cp)     H edge preset 对照 (32MB 向量预算/zstd)
  D 索引构建: FTS/向量 (时/RSS/盘)    I 重开 + crash 恢复 (kill -9 → WAL 重放)
  E checkpoint: 时长/盘收敛           J SQLite 参照 (同形状加载/盘/查询RSS)

方法学: psutil 50ms 采样峰值; 查询内存取 3 个窗口峰值Δ的中位数 (jemalloc
保留使单次采样高方差 — R13 复核结论); 驱动侧源数组在引擎测量前 del+gc。
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time

import numpy as np
import psutil

R = {}


def du_mb(p):
    if not os.path.exists(p):
        return 0.0
    total = 0
    if os.path.isdir(p):
        for root, _, files in os.walk(p):
            for f in files:
                total += os.path.getsize(os.path.join(root, f))
    else:
        total = os.path.getsize(p)
    return round(total / 1e6, 1)


class RSS:
    def __init__(self):
        self.p = psutil.Process()
        self.peak = self.p.memory_info().rss
        self.stop = False
        self.t = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while not self.stop:
            self.peak = max(self.peak, self.p.memory_info().rss)
            time.sleep(0.05)

    @classmethod
    def now_mb(cls):
        return psutil.Process().memory_info().rss / 1e6

    def __enter__(self):
        self.t.start()
        return self

    def __exit__(self, *x):
        self.stop = True
        self.t.join(timeout=1)

    def mb(self):
        return round(self.peak / 1e6, 1)


def q_rss_med(db, sql, params=None, windows=3, per=20):
    """单查询类的峰值 RSS Δ 中位数: 每窗口先记录基线, 跑 per 次, 取窗口内峰值."""
    deltas = []
    for _ in range(windows):
        import gc as _gc
        _gc.collect()
        base = RSS.now_mb()
        with RSS() as rss:
            for _ in range(per):
                db.query(sql, params)
        deltas.append(rss.mb() - base)
    deltas.sort()
    return round(deltas[len(deltas) // 2], 1)


def lat_ms(db, sql, params=None, n=30):
    ts = []
    for _ in range(n):
        t0 = time.perf_counter()
        db.query(sql, params)
        ts.append((time.perf_counter() - t0) * 1000)
    ts.sort()
    return round(ts[len(ts) // 2], 2)


import motedb  # noqa: E402

N = 100_000
DIM = 384
RNG = np.random.default_rng(42)

# ── A 工件大小 ──
root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
cli = os.path.join(root, "target", "release", "motedb-cli")
so_candidates = []
for rel in ("target/release/libmotedb.dylib", "target/release/libmotedb.a"):
    p = os.path.join(root, rel)
    if os.path.exists(p):
        so_candidates.append(p)
R["artifact_mb"] = {
    "motedb_cli": du_mb(cli),
    "python_so": du_mb(so_candidates[0]) if so_candidates else None,
}
print("A 工件:", R["artifact_mb"])

tmp = tempfile.mkdtemp(prefix="res_bench_")

# ════════ MoteDB 默认配置 全生命周期 ════════
path = os.path.join(tmp, "res.mote")

# ── B 空库足迹 ──
base0 = RSS.now_mb()
db = motedb.Database(path)
R["open_fresh_rss_mb"] = round(RSS.now_mb() - base0, 1)
db.execute("CREATE TABLE ev (id INT PRIMARY KEY, ts TIMESTAMP, dev TEXT, val REAL, note TEXT, emb VECTOR(384))")

# ── C 加载 (驱动侧源数组与引擎侧分开测量) ──
ts = (1_700_000_000_000_000 + np.arange(N) * 1_000_000).astype(np.int64)
dev = np.array([f"dev-{i % 64:03d}" for i in range(N)])
val = RNG.uniform(0, 100, N)
notes = np.array([f"note {i % 1000} charlie delta {i}" for i in range(N)])
emb = RNG.standard_normal((N, DIM)).astype(np.float32)
driver_mb = round((ts.nbytes + dev.dtype.itemsize * 0 + dev.size * 24 + val.nbytes + notes.size * 30 + emb.nbytes) / 1e6, 1)

t0 = time.perf_counter()
py_rows = [
    [int(i), int(ts[i]), str(dev[i]), float(val[i]), str(notes[i]), emb[i].tolist()]
    for i in range(N)
]
conv_s = time.perf_counter() - t0
before_load = RSS.now_mb()
with RSS() as rss_load:
    t0 = time.perf_counter()
    CH = 5000
    for lo in range(0, N, CH):
        db.executemany("INSERT INTO ev VALUES (?, ?, ?, ?, ?, ?)", py_rows[lo:lo + CH])
    load_s = time.perf_counter() - t0
    wal_mb = du_mb(path if os.path.isdir(path) else path)
    t0 = time.perf_counter()
    db.checkpoint()
    cp_s = time.perf_counter() - t0
    db_mb = du_mb(path if os.path.isdir(path) else path)
R["load"] = {
    "rows_per_s": round(N / load_s),
    "python_conv_s": round(conv_s, 2),
    "load_s": round(load_s, 2),
    "driver_arrays_mb": driver_mb,
    "load_peak_rss_delta_mb": round(rss_load.mb() - before_load, 1),
    "disk_wal_pre_cp_mb": wal_mb,
    "checkpoint_s": round(cp_s, 2),
    "disk_after_cp_mb": db_mb,
}
print("C 加载:", R["load"])
del py_rows
import gc as gc_
gc_.collect()

# ── D 索引构建 (FTS + 向量) ──
b = RSS.now_mb()
t0 = time.perf_counter()
db.execute("CREATE TEXT INDEX ev_note ON ev(note)")
fts_s = time.perf_counter() - t0
with RSS() as r_fts:
    # 触发索引物化 (若惰性): 一条 MATCH
    db.query("SELECT id FROM ev WHERE MATCH(note, 'charlie') LIMIT 5")
R["index_fts"] = {"build_s": round(fts_s, 2), "rss_delta_mb": round(max(r_fts.mb() - b, 0), 1),
                  "disk_mb": du_mb(path)}
print("D FTS:", R["index_fts"])

qs = emb[RNG.integers(0, N, 8)]

# ── F/G 查询期内存 + steady-state ──
idle_after_load = RSS.now_mb()
db.query("SELECT COUNT(*) FROM ev")
R["query_rss_delta_mb"] = {
    "point": q_rss_med(db, "SELECT val FROM ev WHERE id = ?", params=[4242]),
    "range_agg_fused": q_rss_med(db, "SELECT COUNT(*), AVG(val) FROM ev WHERE ts >= 1700000000000000 AND ts <= 1700000800000000000 AND dev = ?", params=["dev-007"]),
    "range_agg_between_80pct": q_rss_med(db, "SELECT COUNT(*), AVG(val) FROM ev WHERE id BETWEEN 10000 AND 90000"),
    "groupby": q_rss_med(db, "SELECT dev, COUNT(*), AVG(val) FROM ev GROUP BY dev"),
    "topk": q_rss_med(db, "SELECT id FROM ev ORDER BY val DESC LIMIT 10"),
    "join": q_rss_med(db, "SELECT a.dev, COUNT(*) FROM ev a JOIN ev b ON a.dev = b.dev WHERE a.id <= 20000 AND b.id <= 2000 GROUP BY a.dev", windows=2, per=5),
    "fts_match": q_rss_med(db, "SELECT id FROM ev WHERE MATCH(note, 'charlie delta') LIMIT 10"),
    "vector_knn10": q_rss_med(db, "SELECT id FROM ev ORDER BY emb <-> ? LIMIT 10", params=[qs[0].tolist()]),
    "full_scan_project": q_rss_med(db, "SELECT id, dev FROM ev LIMIT 50000"),
}
# steady-state: 全查询类跑热后 RSS
for _ in range(3):
    db.query("SELECT dev, COUNT(*) FROM ev GROUP BY dev")
    db.query("SELECT id FROM ev ORDER BY emb <-> ? LIMIT 10", params=[qs[1].tolist()])
gc_.collect()
R["steady_state_rss_delta_mb"] = round(RSS.now_mb() - idle_after_load, 1)
R["latency_ms"] = {
    "point": lat_ms(db, "SELECT val FROM ev WHERE id = ?", params=[4242]),
    "range_agg_fused": lat_ms(db, "SELECT COUNT(*), AVG(val) FROM ev WHERE ts >= 1700000000000000 AND ts <= 1700000800000000000 AND dev = ?", params=["dev-007"]),
    "range_agg_between_80pct": lat_ms(db, "SELECT COUNT(*), AVG(val) FROM ev WHERE id BETWEEN 10000 AND 90000"),
    "groupby": lat_ms(db, "SELECT dev, COUNT(*), AVG(val) FROM ev GROUP BY dev"),
    "knn10": lat_ms(db, "SELECT id FROM ev ORDER BY emb <-> ? LIMIT 10", params=[qs[2].tolist()]),
}
print("F/G 查询内存:", R["query_rss_delta_mb"], "| steady:", R["steady_state_rss_delta_mb"])

# ── I 重开 + crash 恢复 ──
db.checkpoint()
db.close()
b = RSS.now_mb()
t0 = time.perf_counter()
db = motedb.Database(path)
reopen_s = time.perf_counter() - t0
R["reopen"] = {"open_s": round(reopen_s, 3), "rss_delta_mb": round(RSS.now_mb() - b, 1)}
n_before = db.query("SELECT COUNT(*) FROM ev")[1][0][0]

# crash: kill -9 写入进程 → WAL 重放
crash_path = os.path.join(tmp, "crash.mote")
cdb = motedb.Database(crash_path)
cdb.execute("CREATE TABLE c (id INT PRIMARY KEY, v INT)")
cdb.executemany("INSERT INTO c VALUES (?, ?)", [[i, i] for i in range(1000)])
cdb.checkpoint()
cdb.close()  # 🔑 父连接必须先关 — 子进程独占打开, kill -9 后测 WAL 重放
# 用子进程持有第二个连接再 kill -9: 直接对当前进程做不了 — 用脚本子进程
crash_child = os.path.join(tmp, "child.py")
open(crash_child, "w").write(
    "import motedb, time\n"
    f"db = motedb.Database({crash_path!r})\n"
    "db.executemany('INSERT INTO c VALUES (?, ?)', [[1000 + i, i] for i in range(300)])\n"
    "print('ready', flush=True)\n"
    "time.sleep(60)\n"
)
proc = subprocess.Popen([sys.executable, crash_child], stdout=subprocess.PIPE)
proc.stdout.readline()  # 等 ready (写入已在 WAL, 未 checkpoint)
proc.kill()
proc.wait()
t0 = time.perf_counter()
db2 = motedb.Database(crash_path)
cnt = db2.query("SELECT COUNT(*) FROM c")[1][0][0]
crash_recover_s = time.perf_counter() - t0
R["crash_recovery"] = {"reopen_s": round(crash_recover_s, 3),
                       "rows_visible": cnt, "expected": 1300}
db2.close()
print("I 重开/crash:", R["reopen"], R["crash_recovery"])
db.close()

# ════════ H edge preset 对照 ════════
epath = os.path.join(tmp, "edge.mote")
edb = motedb.Database(epath, preset="edge")
edb.execute("CREATE TABLE ev (id INT PRIMARY KEY, ts TIMESTAMP, dev TEXT, val REAL, note TEXT, emb VECTOR(384))")
t0 = time.perf_counter()
for lo in range(0, N, CH):
    edb.executemany("INSERT INTO ev VALUES (?, ?, ?, ?, ?, ?)",
                    [[int(i), int(ts[i]), str(dev[i]), float(val[i]), str(notes[i]), emb[i].tolist()]
                     for i in range(lo, min(lo + CH, N))])
edge_load_s = time.perf_counter() - t0
edge_pre_cp = du_mb(epath)
t0 = time.perf_counter()
edb.checkpoint()
edge_cp_s = time.perf_counter() - t0
edge_mb = du_mb(epath)
b = RSS.now_mb()
edb.query("SELECT id FROM ev ORDER BY emb <-> ? LIMIT 10", params=[qs[3].tolist()])
edge_knn_first_mb = round(RSS.now_mb() - b, 1)
edge_knn_ms = lat_ms(edb, "SELECT id FROM ev ORDER BY emb <-> ? LIMIT 10", params=[qs[4].tolist()])
R["edge_preset"] = {
    "load_s": round(edge_load_s, 2),
    "disk_pre_cp_mb": edge_pre_cp,
    "checkpoint_s": round(edge_cp_s, 2),
    "disk_mb": edge_mb,
    "knn_first_query_rss_mb": edge_knn_first_mb,
    "knn_p50_ms": edge_knn_ms,
}
print("H edge:", R["edge_preset"])
edb.close()

# ════════ J SQLite 参照 ════════
import sqlite3  # noqa: E402

sdb = sqlite3.connect(os.path.join(tmp, "ref.sqlite"))
sdb.execute("PRAGMA journal_mode=WAL")
sdb.execute("CREATE TABLE ev (id INTEGER PRIMARY KEY, ts INTEGER, dev TEXT, val REAL, note TEXT)")
srows = [(int(i), int(ts[i]), str(dev[i]), float(val[i]), str(notes[i])) for i in range(N)]
import struct
vecblob = emb.astype(np.float32).tobytes()
b = RSS.now_mb()
with RSS() as r_sql:
    t0 = time.perf_counter()
    sdb.executemany("INSERT INTO ev VALUES (?,?,?,?,?)", srows)
    sqlite_load_s = time.perf_counter() - t0
    sdb.commit()
    sqlite_rss_load = r_sql.mb() - b
sqlite_mb = du_mb(os.path.join(tmp, "ref.sqlite")) + du_mb(os.path.join(tmp, "ref.sqlite-wal"))
# sqlite 查询参照
def q_rss_sql(sql, params=None, windows=3, per=20):
    deltas = []
    for _ in range(windows):
        base = RSS.now_mb()
        with RSS() as rss:
            for _ in range(per):
                sdb.execute(sql, params or ()).fetchall()
        deltas.append(rss.mb() - base)
    deltas.sort()
    return round(deltas[len(deltas) // 2], 1)

R["sqlite_ref"] = {
    "load_s": round(sqlite_load_s, 2),
    "load_rss_delta_mb": round(sqlite_rss_load, 1),
    "disk_mb": sqlite_mb,
    "point_rss": q_rss_sql("SELECT val FROM ev WHERE id = 4242"),
    "range_agg_rss": q_rss_sql("SELECT COUNT(*), AVG(val) FROM ev WHERE id BETWEEN 10000 AND 90000"),
    "groupby_rss": q_rss_sql("SELECT dev, COUNT(*), AVG(val) FROM ev GROUP BY dev"),
}
sdb.close()
print("J sqlite:", R["sqlite_ref"])

shutil.rmtree(tmp, ignore_errors=True)
out = os.path.join(os.path.dirname(__file__) or ".", "resource_bench_v0.10.0.json")
open(out, "w").write(json.dumps(R, indent=1, ensure_ascii=False))
print("JSON " + json.dumps(R))
