#!/usr/bin/env python3
"""规模曲线跨引擎基准：MoteDB vs SQLite vs DuckDB × 10K/100K/1M 行 × 六形状。

同一 4 列表（id/ts/device/val），每档独立建库。形状: 点查 / 范围聚合(10%窗口) /
GROUP BY 64 组 / TopK / 向量 KNN10 精确 / FTS(LIKE 或 MATCH)。

用法: python3 compete_scale.py --engine mote|sqlite|duckdb
输出: 每档一行 JSON。
"""
import argparse, json, os, shutil, tempfile, time
import numpy as np

DEVICES = [f"dev-{i:02d}" for i in range(64)]
WORDS = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"]
DIMS = 64  # 向量维度取小值让 1M 档可行


def gen(n):
    rng = np.random.default_rng(42)
    ts = (1_700_000_000 + np.sort(rng.integers(0, 30 * 24 * 3600, n))) * 1_000
    dev = np.array([DEVICES[i % 64] for i in range(n)])
    val = rng.standard_normal(n).astype(np.float32) * 10
    rr = np.random.default_rng(7)
    notes = []
    for i in range(n):
        k = 4 + (i % 3)
        notes.append(" ".join(WORDS[j % len(WORDS)] for j in rr.integers(0, len(WORDS), k)))
    emb = rng.standard_normal((n, DIMS)).astype(np.float32)
    return ts, dev, val, notes, emb


def lat(f, iters):
    out = []
    for _ in range(iters):
        t0 = time.perf_counter()
        f()
        out.append(time.perf_counter() - t0)
    a = np.asarray(out) * 1e3
    return {"avg": round(float(a.mean()), 4), "p50": round(float(np.percentile(a, 50)), 4)}


def run(engine, n, tmp):
    R = {"n": n}
    ts, dev, val, notes, emb = gen(n)
    rng = np.random.default_rng(1)
    qs = emb[rng.integers(0, n, 30)]

    if engine == "mote":
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
        import motedb
        db = motedb.Database(os.path.join(tmp, "s.mote"), preset="general")
        db.execute(f"CREATE TABLE ev (id INT PRIMARY KEY, ts INT, device TEXT, val FLOAT, note TEXT, emb VECTOR({DIMS}))")
        B = 50_000
        t0 = time.perf_counter()
        for i in range(0, n, B):
            j = min(i + B, n)
            db.insert_arrays("ev", {"id": list(range(i, j)), "ts": [int(x) for x in ts[i:j]],
                                    "device": [str(x) for x in dev[i:j]], "val": [float(x) for x in val[i:j]],
                                    "note": notes[i:j], "emb": emb[i:j]})
        R["load_s"] = round(time.perf_counter() - t0, 2)
        db.checkpoint()
        t0 = time.perf_counter()
        db.execute("CREATE TEXT INDEX ev_note ON ev(note)")
        R["fts_build_s"] = round(time.perf_counter() - t0, 3)
        R["q_point"] = lat(lambda: db.query("SELECT val FROM ev WHERE id = ?", params=[int(rng.integers(0, n))]), 200)
        def qr():
            i = int(rng.integers(0, n - n // 10))
            db.query("SELECT COUNT(*), AVG(val) FROM ev WHERE ts >= ? AND ts <= ? AND device = ?",
                     params=[int(ts[i]), int(ts[i + n // 10]), "dev-07"])
        R["q_range_agg"] = lat(qr, 50)
        R["q_groupby"] = lat(lambda: db.query("SELECT device, COUNT(*), AVG(val) FROM ev GROUP BY device"), 20)
        R["q_topk"] = lat(lambda: db.query("SELECT id FROM ev ORDER BY val ASC LIMIT 10"), 50)
        def mote_knn(i=[0]):
            i[0] = (i[0] + 1) % 30
            db.query("SELECT id FROM ev ORDER BY emb <-> ? LIMIT 10",
                     params=[qs[i[0]].tolist()])
        R["q_knn10"] = lat(mote_knn, 30)
        R["q_fts"] = lat(lambda: db.query("SELECT id FROM ev WHERE MATCH(note, 'charlie delta') LIMIT 10"), 50)
        db.close()

    elif engine == "sqlite":
        import sqlite3
        con = sqlite3.connect(os.path.join(tmp, "s.db"), isolation_level=None)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute(f"CREATE TABLE ev (id INT PRIMARY KEY, ts INT, device TEXT, val REAL, note TEXT, emb BLOB)")
        t0 = time.perf_counter()
        con.execute("BEGIN")
        con.executemany("INSERT INTO ev VALUES (?,?,?,?,?,?)",
                        [(i, int(ts[i]), str(dev[i]), float(val[i]), notes[i], emb[i].tobytes())
                         for i in range(n)])
        con.execute("COMMIT")
        R["load_s"] = round(time.perf_counter() - t0, 2)
        t0 = time.perf_counter()
        con.execute("CREATE VIRTUAL TABLE ev_fts USING fts5(note, content='ev', content_rowid='id')")
        con.execute("INSERT INTO ev_fts(rowid, note) SELECT id, note FROM ev")
        R["fts_build_s"] = round(time.perf_counter() - t0, 3)
        R["q_point"] = lat(lambda: con.execute("SELECT val FROM ev WHERE id = ?", (int(rng.integers(0, n)),)).fetchone(), 200)
        def qr():
            i = int(rng.integers(0, n - n // 10))
            con.execute("SELECT COUNT(*), AVG(val) FROM ev WHERE ts >= ? AND ts <= ? AND device = ?",
                        (int(ts[i]), int(ts[i + n // 10]), "dev-07")).fetchone()
        R["q_range_agg"] = lat(qr, 50)
        R["q_groupby"] = lat(lambda: con.execute("SELECT device, COUNT(*), AVG(val) FROM ev GROUP BY device").fetchall(), 20)
        R["q_topk"] = lat(lambda: con.execute("SELECT id FROM ev ORDER BY val ASC LIMIT 10").fetchall(), 50)
        # sqlite 无向量 — numpy 全扫口径（与 compete_bench 一致）
        # |x|² 与查询平方在循环外预计算（1M 行每迭代重算是 O(n²) 级浪费）
        emb64 = emb.astype(np.float64)
        cn = (emb64 * emb64).sum(1)
        def knn(i=[0]):
            i[0] = (i[0] + 1) % 30
            q = qs[i[0]].astype(np.float64)
            D = cn - 2.0 * (emb64 @ q) + float(q @ q)
            np.argpartition(D, 10)[:10]
        R["q_knn10"] = lat(knn, 30)
        R["q_fts"] = lat(lambda: con.execute("SELECT rowid FROM ev_fts WHERE ev_fts MATCH 'charlie delta' LIMIT 10").fetchall(), 50)
        con.close()

    else:
        import duckdb
        con = duckdb.connect(os.path.join(tmp, "s.duck"))
        con.execute(f"CREATE TABLE ev (id INT PRIMARY KEY, ts BIGINT, device TEXT, val DOUBLE, note TEXT, emb FLOAT[{DIMS}])")
        t0 = time.perf_counter()
        try:
            import pyarrow as pa
            emb_col = pa.FixedSizeListArray.from_arrays(pa.array(emb.reshape(-1)), DIMS)
            tbl = pa.table({"id": np.arange(n, dtype="int32"), "ts": ts.astype("int64"),
                            "device": list(map(str, dev)), "val": val.astype("float64"),
                            "note": notes, "emb": emb_col})
            con.register("t_arrow", tbl)
            con.execute("INSERT INTO ev SELECT * FROM t_arrow")
            con.unregister("t_arrow")
        except ImportError:
            con.executemany("INSERT INTO ev VALUES (?,?,?,?,?,?)",
                            [(i, int(ts[i]), str(dev[i]), float(val[i]), notes[i], emb[i].tolist())
                             for i in range(n)])
        R["load_s"] = round(time.perf_counter() - t0, 2)
        R["fts_build_s"] = None
        R["q_point"] = lat(lambda: con.execute("SELECT val FROM ev WHERE id = ?", [int(rng.integers(0, n))]).fetchone(), 200)
        def qr():
            i = int(rng.integers(0, n - n // 10))
            con.execute("SELECT COUNT(*), AVG(val) FROM ev WHERE ts >= ? AND ts <= ? AND device = ?",
                        [int(ts[i]), int(ts[i + n // 10]), "dev-07"]).fetchone()
        R["q_range_agg"] = lat(qr, 50)
        R["q_groupby"] = lat(lambda: con.execute("SELECT device, COUNT(*), AVG(val) FROM ev GROUP BY device").fetchall(), 20)
        R["q_topk"] = lat(lambda: con.execute("SELECT id FROM ev ORDER BY val ASC LIMIT 10").fetchall(), 50)
        def knn(i=[0]):
            i[0] = (i[0] + 1) % 30
            con.execute(f"SELECT id FROM ev ORDER BY array_distance(emb, ?::FLOAT[{DIMS}]) LIMIT 10",
                        [qs[i[0]].tolist()]).fetchall()
        R["q_knn10"] = lat(knn, 30)
        R["q_fts"] = lat(lambda: con.execute("SELECT id FROM ev WHERE note LIKE '%charlie%delta%' LIMIT 10").fetchall(), 50)
        con.close()

    print("JSON " + json.dumps(R))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", required=True, choices=["mote", "sqlite", "duckdb"])
    a = ap.parse_args()
    for n in (10_000, 100_000, 1_000_000):
        tmp = tempfile.mkdtemp(prefix=f"cs_{a.engine}_{n}_")
        try:
            run(a.engine, n, tmp)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
