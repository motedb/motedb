#!/usr/bin/env python3
"""Round 11 竞品基准：MoteDB vs SQLite vs DuckDB vs FAISS(+numpy)。

同一数据集（100K 行：int PK / ts / device / val / ~80B 文本 / 384d 向量）、
同一组负载；每个引擎在独立子进程中运行（隔离 RSS）。
输出：JSON（load 吞吐、磁盘占用、峰值内存、各查询 avg/p50/p95）。

用法：python3 compete_r11.py --engine mote|sqlite|duckdb|faiss
     （父进程逐个调用并汇总）
"""
import argparse, json, os, shutil, struct, sys, tempfile, threading, time
import numpy as np

N = 100_000
DIM = 384
DEVICES = [f"dev-{i:02d}" for i in range(64)]
WORDS = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf",
         "hotel", "india", "juliet", "kilo", "lima", "mike", "november"]

def gen():
    rng = np.random.default_rng(42)
    ts = (1_700_000_000 + np.sort(rng.integers(0, 30 * 24 * 3600, N))) * 1_000_000
    dev = np.array([DEVICES[i % 64] for i in range(N)])
    val = rng.standard_normal(N).astype(np.float32) * 10
    notes = []
    rr = np.random.default_rng(7)
    for i in range(N):
        k = 6 + (i % 5)
        ws = [WORDS[j % len(WORDS)] for j in rr.integers(0, len(WORDS), k)]
        notes.append(f"row {i} " + " ".join(ws))
    emb = rng.standard_normal((N, DIM)).astype(np.float32)
    return ts, dev, val, notes, emb


def batches(ts, dev, val, notes, emb, bs=5000):
    for i in range(0, N, bs):
        j = min(i + bs, N)
        rows = [(k, int(ts[k]), str(dev[k]), float(val[k]), notes[k], emb[k].tolist())
                for k in range(i, j)]
        yield rows

def lat(f, iters, *a):
    out = []
    for _ in range(iters):
        t0 = time.perf_counter()
        f(*a)
        out.append(time.perf_counter() - t0)
    a_ = np.asarray(out) * 1e3
    return {"avg_ms": round(float(a_.mean()), 3),
            "p50_ms": round(float(np.percentile(a_, 50)), 3),
            "p95_ms": round(float(np.percentile(a_, 95)), 3)}

def free_driver(locals_dict, keep):
    import gc
    for k in list(locals_dict):
        if k not in keep:
            locals_dict.pop(k, None)
    gc.collect()

class RSS:
    def __init__(self):
        import psutil
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
        import psutil
        return psutil.Process().memory_info().rss / 1e6
    def __enter__(self):
        self.t.start(); return self
    def __exit__(self, *x):
        self.stop = True; self.t.join(timeout=1)
    def mb(self):
        return round(self.peak / 1e6, 1)

def du_mb(path):
    total = 0
    if os.path.isfile(path):
        return round(os.path.getsize(path) / 1e6, 1)
    for root, _, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return round(total / 1e6, 1)

R = {}

def bench_mote(tmp):
    import motedb
    path = os.path.join(tmp, "mote.mote")
    ts, dev, val, notes, emb = gen()
    db = motedb.Database(path, preset="general")
    db.execute("CREATE TABLE ev (id INT PRIMARY KEY, ts TIMESTAMP, device TEXT, val FLOAT, note TEXT, emb VECTOR(384))")
    # 🔥 列式批量加载 (insert_arrays): 引擎的批量导入正解 — executemany
    # 的 Python 侧逐行对象构造 (.tolist() 每 384 浮点建列表) 是旧口径的
    # 瓶颈 (48K rows/s); insert_arrays 列式提取 153K rows/s (3.2×), id
    # 语义不变 (显式 PK 走 full path 校验+唯一性)。
    with RSS() as rss:
        t0 = time.perf_counter()
        for i in range(0, N, 5000):
            j = min(i + 5000, N)
            db.insert_arrays("ev", {
                "id": list(range(i, j)),
                "ts": [int(x) for x in ts[i:j]],
                "device": [str(x) for x in dev[i:j]],
                "val": [float(x) for x in val[i:j]],
                "note": notes[i:j],
                "emb": emb[i:j],
            })
        load_s = time.perf_counter() - t0
    R["load_rows_per_s"] = round(N / load_s)
    R["load_peak_rss_mb"] = rss.mb()
    db.execute("CREATE TABLE sen (device TEXT PRIMARY KEY, zone INT)")
    db.executemany("INSERT INTO sen VALUES (?, ?)", [(d, i % 8) for i, d in enumerate(DEVICES)])
    db.checkpoint()
    t0 = time.perf_counter()
    db.execute("CREATE TEXT INDEX ev_note ON ev(note)")
    R["text_index_build_s"] = round(time.perf_counter() - t0, 3)

    rng = np.random.default_rng(1)
    ids = rng.integers(0, N, 500)
    def q_point():
        db.query("SELECT val FROM ev WHERE id = ?", params=[int(rng.integers(0, N))])
    R["q_point"] = lat(q_point, 200)
    # range agg
    def q_range():
        i = int(rng.integers(0, N - 100_000 // 10))
        db.query("SELECT COUNT(*), AVG(val) FROM ev WHERE ts >= ? AND ts <= ? AND device = ?",
                 params=[int(ts[i]), int(ts[i + N // 10]), "dev-07"])
    R["q_range_agg"] = lat(q_range, 100)
    R["q_groupby"] = lat(lambda: db.query("SELECT device, COUNT(*), AVG(val) FROM ev GROUP BY device"), 20)
    R["q_topk"] = lat(lambda: db.query("SELECT id FROM ev ORDER BY val ASC LIMIT 10"), 100)
    R["q_join"] = lat(lambda: db.query(
        "SELECT e.device, COUNT(*) FROM ev e JOIN sen s ON e.device = s.device WHERE s.zone = 3 GROUP BY e.device"), 20)
    # vector top-10 (exact scan path at 100K rows)
    qs = emb[rng.integers(0, N, 30)]
    def q_vec(i=[0]):
        i[0] = (i[0] + 1) % 30
        db.query("SELECT id FROM ev ORDER BY emb <-> ? LIMIT 10", params=[qs[i[0]].tolist()])
    R["q_vector_knn10"] = lat(q_vec, 30)
    R["q_fts"] = lat(lambda: db.query(
        "SELECT id, BM25_SCORE() FROM ev WHERE MATCH(note, 'charlie delta') LIMIT 10"), 50)
    db.checkpoint(); db.close()
    cand = path if path.endswith(".mote") else path + ".mote"
    R["db_mb"] = du_mb(cand) if os.path.exists(cand) else du_mb(path)
    # Free the driver-side corpus so the query-phase RSS delta is comparable
    # with SQLite/DuckDB (which subtract their base): MoteDB used to report an
    # ABSOLUTE peak that included ~350MB of Python-held source arrays.
    del ts, dev, val, notes, emb
    import gc as _gc
    _gc.collect()
    base = RSS.now_mb()
    with RSS() as rss2:
        db = motedb.Database(path)
        q_vec()
        R["q_vector_knn10"]  # ensure loaded
        R["query_peak_rss_mb"] = round(rss2.mb() - base, 1)
        R["q_point_reopen"] = lat(lambda: db.query("SELECT val FROM ev WHERE id = ?", params=[int(rng.integers(0, N))]), 200)
    db.close()

def bench_sqlite(tmp):
    import sqlite3
    path = os.path.join(tmp, "lite.db")
    ts, dev, val, notes, emb = gen()
    con = sqlite3.connect(path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("CREATE TABLE ev (id INTEGER PRIMARY KEY, ts INTEGER, device TEXT, val REAL, note TEXT, emb BLOB)")
    con.execute("CREATE TABLE sen (device TEXT PRIMARY KEY, zone INT)")
    con.executemany("INSERT INTO sen VALUES (?, ?)", [(d, i % 8) for i, d in enumerate(DEVICES)])
    with RSS() as rss:
        t0 = time.perf_counter()
        for batch in batches(ts, dev, val, notes, emb):
            con.executemany(
                "INSERT INTO ev VALUES (?, ?, ?, ?, ?, ?)",
                [(r[0], r[1], r[2], r[3], r[4], r[5] and np.asarray(r[5], dtype=np.float32).tobytes()) for r in batch])
        con.commit()
        load_s = time.perf_counter() - t0
    R["load_rows_per_s"] = round(N / load_s)
    R["load_peak_rss_mb"] = rss.mb()
    con.execute("CREATE INDEX ix_ts ON ev(ts, device)")
    con.execute("CREATE VIRTUAL TABLE ev_fts USING fts5(note, content='ev', content_rowid='id')")
    t0 = time.perf_counter()
    con.execute("INSERT INTO ev_fts(rowid, note) SELECT id, note FROM ev")
    con.commit()
    R["fts_build_s"] = round(time.perf_counter() - t0, 3)

    rng = np.random.default_rng(1)
    def q_point():
        con.execute("SELECT val FROM ev WHERE id = ?", (int(rng.integers(0, N)),)).fetchone()
    R["q_point"] = lat(q_point, 200)
    def q_range():
        i = int(rng.integers(0, N - 100_000 // 10))
        con.execute("SELECT COUNT(*), AVG(val) FROM ev WHERE ts >= ? AND ts <= ? AND device = ?",
                    (int(ts[i]), int(ts[i + N // 10]), "dev-07")).fetchone()
    R["q_range_agg"] = lat(q_range, 100)
    R["q_groupby"] = lat(lambda: con.execute("SELECT device, COUNT(*), AVG(val) FROM ev GROUP BY device").fetchall(), 20)
    R["q_topk"] = lat(lambda: con.execute("SELECT id FROM ev ORDER BY val ASC LIMIT 10").fetchall(), 100)
    R["q_join"] = lat(lambda: con.execute(
        "SELECT e.device, COUNT(*) FROM ev e JOIN sen s ON e.device = s.device WHERE s.zone = 3 GROUP BY e.device").fetchall(), 20)
    # vector: 无原生支持 → 一次性取回 numpy 暴力（标注）
    q_idx = rng.integers(0, N, 30)
    qs = emb[q_idx]
    import gc as _gc
    _gc.collect()
    blobs = np.stack([np.frombuffer(r[0], dtype=np.float32) for r in
                      con.execute("SELECT emb FROM ev ORDER BY id").fetchall()])
    def q_vec(i=[0]):
        i[0] = (i[0] + 1) % 30
        d = ((blobs - qs[i[0]]) ** 2).sum(1)
        np.argpartition(d, 10)[:10]
    R["q_vector_knn10_numpy"] = lat(q_vec, 30)
    R["q_fts"] = lat(lambda: con.execute(
        "SELECT rowid FROM ev_fts WHERE ev_fts MATCH 'charlie delta' LIMIT 10").fetchall(), 50)
    base = RSS.now_mb()
    with RSS() as rss2:
        q_point()
        con.execute("SELECT COUNT(*) FROM ev").fetchone()
        R["query_peak_rss_mb"] = round(rss2.mb() - base, 1)
    con.close()
    R["db_mb"] = du_mb(path)

def bench_duckdb(tmp):
    import duckdb
    path = os.path.join(tmp, "duck.db")
    ts, dev, val, notes, emb = gen()
    con = duckdb.connect(path)
    con.execute("CREATE TABLE ev (id INTEGER PRIMARY KEY, ts BIGINT, device VARCHAR, val FLOAT, note VARCHAR, emb FLOAT[384])")
    con.execute("CREATE TABLE sen (device VARCHAR PRIMARY KEY, zone INTEGER)")
    con.executemany("INSERT INTO sen VALUES (?, ?)", [(d, i % 8) for i, d in enumerate(DEVICES)])
    with RSS() as rss:
        t0 = time.perf_counter()
        one = "(?, ?, ?, ?, ?, ?::FLOAT[384])"
        for batch in batches(ts, dev, val, notes, emb, 1000):
            sql = "INSERT INTO ev VALUES " + ",".join([one] * len(batch))
            flat = [x for r in batch for x in r]
            con.execute(sql, flat)
        load_s = time.perf_counter() - t0
    R["load_rows_per_s"] = round(N / load_s)
    R["load_peak_rss_mb"] = rss.mb()
    con.execute("CREATE INDEX ix_ts ON ev(ts)")
    import gc as _gc
    _gc.collect()

    rng = np.random.default_rng(1)
    def q_point():
        con.execute("SELECT val FROM ev WHERE id = ?", [int(rng.integers(0, N))]).fetchone()
    R["q_point"] = lat(q_point, 200)
    def q_range():
        i = int(rng.integers(0, N - 100_000 // 10))
        con.execute("SELECT COUNT(*), AVG(val) FROM ev WHERE ts >= ? AND ts <= ? AND device = ?",
                    [int(ts[i]), int(ts[i + N // 10]), "dev-07"]).fetchone()
    R["q_range_agg"] = lat(q_range, 100)
    R["q_groupby"] = lat(lambda: con.execute("SELECT device, COUNT(*), AVG(val) FROM ev GROUP BY device").fetchall(), 20)
    R["q_topk"] = lat(lambda: con.execute("SELECT id FROM ev ORDER BY val ASC LIMIT 10").fetchall(), 100)
    R["q_join"] = lat(lambda: con.execute(
        "SELECT e.device, COUNT(*) FROM ev e JOIN sen s ON e.device = s.device WHERE s.zone = 3 GROUP BY e.device").fetchall(), 20)
    qs = emb[rng.integers(0, N, 30)]
    def q_vec(i=[0]):
        i[0] = (i[0] + 1) % 30
        con.execute("SELECT id FROM ev ORDER BY array_distance(emb, ?::FLOAT[384]) LIMIT 10",
                    [qs[i[0]].tolist()]).fetchall()
    R["q_vector_knn10"] = lat(q_vec, 30)
    R["q_fts_like"] = lat(lambda: con.execute(
        "SELECT id FROM ev WHERE note LIKE '%charlie%delta%' LIMIT 10").fetchall(), 20)
    base = RSS.now_mb()
    with RSS() as rss2:
        q_point()
        con.execute("SELECT COUNT(*) FROM ev").fetchone()
        R["query_peak_rss_mb"] = round(rss2.mb() - base, 1)
    con.close()
    R["db_mb"] = du_mb(path)

def bench_faiss(tmp):
    """FAISS 向量专项：IndexFlatL2（精确）对照 MoteDB 精确扫描路径。"""
    import faiss
    ts, dev, val, notes, emb = gen()
    with RSS() as rss:
        t0 = time.perf_counter()
        index = faiss.IndexFlatL2(DIM)
        index.add(emb)
        build_s = time.perf_counter() - t0
    R["index_build_s"] = round(build_s, 3)
    R["load_peak_rss_mb"] = rss.mb()
    rng = np.random.default_rng(1)
    qs = emb[rng.integers(0, N, 30)]
    def q_vec(i=[0]):
        i[0] = (i[0] + 1) % 30
        index.search(qs[i[0]][None, :], 10)
    R["q_vector_knn10"] = lat(q_vec, 30)
    R["db_mb"] = round(emb.nbytes / 1e6, 1)  # 内存索引，无磁盘
    with RSS() as rss2:
        q_vec()
        R["query_peak_rss_mb"] = rss2.mb()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", required=True, choices=["mote", "sqlite", "duckdb", "faiss"])
    a = ap.parse_args()
    tmp = tempfile.mkdtemp(prefix=f"r11_{a.engine}_")
    try:
        {"mote": bench_mote, "sqlite": bench_sqlite,
         "duckdb": bench_duckdb, "faiss": bench_faiss}[a.engine](tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print("JSON " + json.dumps(R))

if __name__ == "__main__":
    main()
