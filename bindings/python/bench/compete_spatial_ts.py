#!/usr/bin/env python3
"""空间 (i-Octree 3D) 与时序 (LATEST BY / 时间范围) 跨引擎 SOTA 对照。

时序 (1M 行, 64 传感器, 2% 乱序):
  ts_load / ts_range_agg (10% 窗口) / ts_latest_by (MoteDB LATEST BY vs
  SQLite(DESC 索引) vs DuckDB arg_max) / ts_order_limit / ts_delete_range
空间 (500K 3D 点, 室内 LiDAR 风格):
  sp_load / sp_bbox (0.1% 选择率 WITHIN) / sp_knn10 (i-Octree vs 列数学
  全扫 — 无 SpatiaLite 环境下该口径即行业嵌入式现实) / sp_radius_count

用法: python3 compete_spatial_ts.py --engine mote|sqlite|duckdb
输出: 单行 JSON。
"""
import argparse, json, os, shutil, tempfile, time
import numpy as np

N_TS = 1_000_000
N_SP = 500_000
R = {}


def lat(f, iters):
    out = []
    for _ in range(iters):
        t0 = time.perf_counter()
        f()
        out.append(time.perf_counter() - t0)
    a = np.asarray(out) * 1e3
    return {"avg_ms": round(float(a.mean()), 3), "p50_ms": round(float(np.percentile(a, 50)), 3)}


def gen_ts():
    rng = np.random.default_rng(42)
    base = rng.integers(0, 3600, 64)  # per-sensor start
    n_per = N_TS // 64
    sids = np.repeat(np.arange(64), n_per)
    t = np.concatenate([base[s] + np.arange(n_per) for s in range(64)]).astype(np.int64)
    # 2% late arrivals: swap some consecutive ts within a sensor
    jitter = rng.random(len(t)) < 0.02
    t[jitter] -= 2
    v = rng.standard_normal(len(t)).astype(np.float32) * 10
    order = np.argsort(t, kind="stable")
    return sids[order], t[order], v[order]


def gen_points():
    # indoor-ish: floor/ceiling slabs + box cluster
    rng = np.random.default_rng(7)
    floor = np.column_stack([
        rng.uniform(0, 10, int(N_SP * 0.4)),
        rng.uniform(0, 10, int(N_SP * 0.4)),
        rng.uniform(0, 0.05, int(N_SP * 0.4)),
    ])
    n_box = N_SP - int(N_SP * 0.4)
    box = np.column_stack([
        rng.uniform(3, 4, n_box), rng.uniform(3, 4, n_box), rng.uniform(0.5, 2, n_box)
    ])
    pts = np.vstack([floor, box]).astype(np.float64)
    return pts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", required=True, choices=["mote", "sqlite", "duckdb"])
    a = ap.parse_args()

    sids, ts, vals = gen_ts()
    pts = gen_points()
    tmp = tempfile.mkdtemp(prefix=f"cst_{a.engine}_")

    # ───────────────────────── 时序 ─────────────────────────
    if a.engine == "mote":
        import sys
        sys.path.insert(0, "..")
        import motedb
        db = motedb.Database(os.path.join(tmp, "ts.mote"), preset="general")
        db.execute("CREATE TABLE sen (ts INT, sid INT, v FLOAT) TIMESERIES(ts)")
        t0 = time.perf_counter()
        # TimeSeries 表由 ColumnarStore 服务 — SQL 多行 VALUES (ts_eval 同款)
        B = 2_000
        for i in range(0, len(ts), B):
            j = min(i + B, len(ts))
            vals_str = ",".join(
                "(%d, %d, %.4f)" % (int(ts[k]), int(sids[k]), vals[k]) for k in range(i, j))
            db.execute("INSERT INTO sen (ts, sid, v) VALUES " + vals_str)
        R["ts_load_s"] = round(time.perf_counter() - t0, 2)
        R["ts_load_rows_s"] = round(len(ts) / R["ts_load_s"])
        lo, hi = int(ts[len(ts)//10]), int(ts[len(ts)//10 + len(ts)//10])
        R["ts_range_agg"] = lat(lambda: db.query(
            "SELECT COUNT(*), AVG(v) FROM sen WHERE ts >= ? AND ts <= ?", params=[lo, hi]), 20)
        R["ts_latest_by"] = lat(lambda: db.query("SELECT ts, sid, v FROM sen LATEST BY sid"), 20)
        R["ts_order_limit"] = lat(lambda: db.query(
            "SELECT sid, ts, v FROM sen ORDER BY ts DESC LIMIT 10"), 50)
        cut = int(ts[len(ts) - 10_000])
        t0 = time.perf_counter()
        db.execute("DELETE FROM sen WHERE ts < ?", params=[cut])
        R["ts_delete_10k_s"] = round(time.perf_counter() - t0, 3)
        db.close()
    elif a.engine == "sqlite":
        import sqlite3
        con = sqlite3.connect(os.path.join(tmp, "ts.db"), isolation_level=None)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        con.execute("CREATE TABLE sen (sid INT, ts INT, v REAL)")
        con.execute("CREATE INDEX sen_ts ON sen(ts DESC)")
        con.execute("CREATE INDEX sen_sid_ts ON sen(sid, ts DESC)")
        t0 = time.perf_counter()
        con.execute("BEGIN")
        con.executemany("INSERT INTO sen VALUES (?,?,?)", list(zip(sids.tolist(), ts.tolist(), vals.tolist())))
        con.execute("COMMIT")
        R["ts_load_s"] = round(time.perf_counter() - t0, 2)
        R["ts_load_rows_s"] = round(len(ts) / R["ts_load_s"])
        lo, hi = int(ts[len(ts)//10]), int(ts[len(ts)//10 + len(ts)//10])
        R["ts_range_agg"] = lat(lambda: con.execute(
            "SELECT COUNT(*), AVG(v) FROM sen WHERE ts >= ? AND ts <= ?", (lo, hi)).fetchone(), 20)
        # LATEST BY 等价: 每传感器 DESC 首行 (arg-max)
        def latest():
            out = []
            for s in range(64):
                out.append(con.execute(
                    "SELECT ts, v FROM sen WHERE sid = ? ORDER BY ts DESC LIMIT 1", (s,)).fetchone())
            return out
        R["ts_latest_by"] = lat(latest, 5)
        R["ts_order_limit"] = lat(lambda: con.execute(
            "SELECT sid, ts, v FROM sen ORDER BY ts DESC LIMIT 10").fetchall(), 50)
        cut = int(ts[len(ts) - 10_000])
        t0 = time.perf_counter()
        con.execute("DELETE FROM sen WHERE ts < ?", (cut,))
        R["ts_delete_10k_s"] = round(time.perf_counter() - t0, 3)
        con.close()
    else:
        import duckdb
        con = duckdb.connect(os.path.join(tmp, "ts.duck"))
        con.execute("CREATE TABLE sen (sid INT, ts BIGINT, v DOUBLE)")
        import pyarrow as pa
        tbl = pa.table({"sid": sids.astype("int32"), "ts": ts, "v": vals.astype("float64")})
        t0 = time.perf_counter()
        con.register("t_a", tbl)
        con.execute("INSERT INTO sen SELECT * FROM t_a")
        con.unregister("t_a")
        R["ts_load_s"] = round(time.perf_counter() - t0, 2)
        R["ts_load_rows_s"] = round(len(ts) / R["ts_load_s"])
        lo, hi = int(ts[len(ts)//10]), int(ts[len(ts)//10 + len(ts)//10])
        R["ts_range_agg"] = lat(lambda: con.execute(
            "SELECT COUNT(*), AVG(v) FROM sen WHERE ts >= ? AND ts <= ?", [lo, hi]).fetchone(), 20)
        R["ts_latest_by"] = lat(lambda: con.execute(
            "SELECT sid, arg_max(ts, ts), arg_max(v, ts) FROM sen GROUP BY sid").fetchall(), 5)
        R["ts_order_limit"] = lat(lambda: con.execute(
            "SELECT sid, ts, v FROM sen ORDER BY ts DESC LIMIT 10").fetchall(), 50)
        cut = int(ts[len(ts) - 10_000])
        t0 = time.perf_counter()
        con.execute("DELETE FROM sen WHERE ts < ?", [cut])
        R["ts_delete_10k_s"] = round(time.perf_counter() - t0, 3)
        con.close()

    # ───────────────────────── 空间 3D ─────────────────────────
    if a.engine == "mote":
        import sys
        sys.path.insert(0, "..")
        import motedb
        db = motedb.Database(os.path.join(tmp, "sp.mote"), preset="general")
        db.execute("CREATE TABLE pts (id INTEGER PRIMARY KEY, p GEOMETRY)")
        t0 = time.perf_counter()
        B = 100_000
        for i in range(0, len(pts), B):
            j = min(i + B, len(pts))
            db.insert_arrays("pts", {"id": list(range(i, j)),
                                     "p": [f"POINT({x} {y} {z})" for x, y, z in pts[i:j]]})
        R["sp_load_s"] = round(time.perf_counter() - t0, 2)
        R["sp_load_rows_s"] = round(len(pts) / R["sp_load_s"])
        t0 = time.perf_counter()
        db.execute("CREATE SPATIAL INDEX pts_p ON pts (p)")
        R["sp_index_build_s"] = round(time.perf_counter() - t0, 2)
        # bbox ~0.1%: the box cluster is 1x1x1.5 of 100 area → 0.3% of floor;
        # choose 0.5..0.55 slab
        R["sp_bbox"] = lat(lambda: db.query(
            "SELECT id FROM pts WHERE ST_WITHIN_3D(p, 3.0, 3.0, 0.5, 3.05, 3.05, 0.55)"), 20)
        q = [float(x) for x in pts[len(pts)//2]]
        R["sp_knn10"] = lat(lambda: db.query(
            "SELECT id FROM pts WHERE ST_KNN_3D(p, %r, %r, %r, 10)" % (q[0], q[1], q[2])), 20)
        R["sp_radius_count"] = lat(lambda: db.query(
            "SELECT COUNT(*) FROM pts WHERE ST_RADIUS_3D(p, %r, %r, %r, 0.1)" % (q[0], q[1], q[2])), 10)
        db.close()
    elif a.engine == "sqlite":
        import sqlite3
        con = sqlite3.connect(os.path.join(tmp, "sp.db"), isolation_level=None)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("CREATE TABLE pts (id INTEGER PRIMARY KEY, x DOUBLE, y DOUBLE, z DOUBLE)")
        t0 = time.perf_counter()
        con.execute("BEGIN")
        con.executemany("INSERT INTO pts VALUES (?,?,?,?)",
                        [(i, float(p[0]), float(p[1]), float(p[2])) for i, p in enumerate(pts)])
        con.execute("COMMIT")
        R["sp_load_s"] = round(time.perf_counter() - t0, 2)
        R["sp_load_rows_s"] = round(len(pts) / R["sp_load_s"])
        t0 = time.perf_counter()
        con.execute("CREATE INDEX pts_x ON pts(x, y, z)")
        R["sp_index_build_s"] = round(time.perf_counter() - t0, 2)
        R["sp_bbox"] = lat(lambda: con.execute(
            "SELECT id FROM pts WHERE x BETWEEN 3.0 AND 3.05 AND y BETWEEN 3.0 AND 3.05 "
            "AND z BETWEEN 0.5 AND 0.55").fetchall(), 20)
        q = [float(x) for x in pts[len(pts)//2]]
        def knn():
            return con.execute(
                "SELECT id, (x-?)*(x-?)+(y-?)*(y-?)+(z-?)*(z-?) AS d FROM pts "
                "ORDER BY d LIMIT 10", (q[0], q[0], q[1], q[1], q[2], q[2])).fetchall()
        R["sp_knn10"] = lat(knn, 5)
        def rad():
            return con.execute(
                "SELECT COUNT(*) FROM pts WHERE (x-?)*(x-?)+(y-?)*(y-?)+(z-?)*(z-?) <= ?",
                (q[0], q[0], q[1], q[1], q[2], q[2], 0.01)).fetchone()
        R["sp_radius_count"] = lat(rad, 5)
        con.close()
    else:
        import duckdb
        con = duckdb.connect(os.path.join(tmp, "sp.duck"))
        con.execute("CREATE TABLE pts (id BIGINT, x DOUBLE, y DOUBLE, z DOUBLE)")
        import pyarrow as pa
        tbl = pa.table({"id": np.arange(len(pts), dtype="int64"),
                        "x": pts[:, 0], "y": pts[:, 1], "z": pts[:, 2]})
        t0 = time.perf_counter()
        con.register("p_a", tbl)
        con.execute("INSERT INTO pts SELECT * FROM p_a")
        con.unregister("p_a")
        R["sp_load_s"] = round(time.perf_counter() - t0, 2)
        R["sp_load_rows_s"] = round(len(pts) / R["sp_load_s"])
        t0 = time.perf_counter()
        con.execute("CREATE INDEX pts_x ON pts(x, y, z)")
        R["sp_index_build_s"] = round(time.perf_counter() - t0, 2)
        R["sp_bbox"] = lat(lambda: con.execute(
            "SELECT id FROM pts WHERE x BETWEEN 3.0 AND 3.05 AND y BETWEEN 3.0 AND 3.05 "
            "AND z BETWEEN 0.5 AND 0.55").fetchall(), 20)
        q = [float(x) for x in pts[len(pts)//2]]
        R["sp_knn10"] = lat(lambda: con.execute(
            "SELECT id FROM pts ORDER BY (x-?)*(x-?)+(y-?)*(y-?)+(z-?)*(z-?) LIMIT 10",
            [q[0], q[0], q[1], q[1], q[2], q[2]]).fetchall(), 5)
        R["sp_radius_count"] = lat(lambda: con.execute(
            "SELECT COUNT(*) FROM pts WHERE (x-?)*(x-?)+(y-?)*(y-?)+(z-?)*(z-?) <= ?",
            [q[0], q[0], q[1], q[1], q[2], q[2], 0.01]).fetchone(), 5)
        con.close()

    shutil.rmtree(tmp, ignore_errors=True)
    print("JSON " + json.dumps(R))


if __name__ == "__main__":
    main()
