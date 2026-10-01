#!/usr/bin/env python3
"""写路径 + 事务 + 并发 + 崩溃恢复跨引擎基准：MoteDB vs SQLite(WAL) vs DuckDB。

补 compete_bench.py（只测 load+读）之外的维度：
  A. 写路径   autocommit / 显式事务 × INSERT / UPDATE-PK / DELETE-PK + scan-UPDATE
  B. 事务     rollback 成本、executemany 批语义
  C. 并发     8 线程并行点读、读写并发（写事务期间读）
  D. 崩溃     子进程 kill -9（os._exit）后重开：提交持久性、未提交零可见、重开时间
  E. 冷启动   全新进程 open + 首查询
  F. FTS      同口径 MATCH LIMIT 10（无排序）+ bm25 排序 LIMIT 10

用法: python3 compete_writes.py --engine mote|sqlite|duckdb [--mode bench|crash-child|open-child]
     （父进程逐引擎调用并汇总，输出 JSON 行）
"""
import argparse, json, os, shutil, subprocess, sys, tempfile, time
from concurrent.futures import ThreadPoolExecutor

N = 50_000
CRASH_COMMIT = 500
CRASH_UNCOMMITTED = 300
WORDS = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"]

R = {}


def base_rows(n=N):
    return [(i, 1_700_000_000_000 + i * 1000, f"dev-{i % 64:02d}",
             float(i % 997) / 7.0,
             " ".join(WORDS[(i + j) % len(WORDS)] for j in range(5)))
            for i in range(n)]


def lat(f, iters):
    out = []
    for _ in range(iters):
        t0 = time.perf_counter()
        f()
        out.append(time.perf_counter() - t0)
    return round(sum(out) / len(out) * 1e3, 4)


def th(name, v):
    R[name] = v


# ─────────────────────────────────────────── mote ────────────────────────────

def make_mote(path):
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
    import motedb
    return motedb.Database(path, preset="general"), motedb


def load_mote(db, rows):
    db.execute("CREATE TABLE ev (id INT PRIMARY KEY, ts INT, device TEXT, val FLOAT, note TEXT)")
    db.insert_arrays("ev", {"id": [r[0] for r in rows], "ts": [r[1] for r in rows],
                            "device": [r[2] for r in rows], "val": [r[3] for r in rows],
                            "note": [r[4] for r in rows]})


def bench_mote(tmp):
    db, _ = make_mote(os.path.join(tmp, "w.mote"))
    rows = base_rows()
    load_mote(db, rows)
    db.checkpoint()

    # A. 写路径
    def w_ins_auto():
        for i in range(300):
            db.execute("INSERT INTO ev VALUES (?, ?, ?, ?, ?)",
                       params=[100_000 + i, 1, "dev-99", 1.0, "alpha bravo"])
    w_ins_auto(); db.execute("DELETE FROM ev WHERE id >= 100000")
    t0 = time.perf_counter(); w_ins_auto()
    th("w_ins_auto_rows_s", round(300 / (time.perf_counter() - t0), 1))
    db.execute("DELETE FROM ev WHERE id >= 100000")

    tx = db.begin()
    t0 = time.perf_counter()
    for i in range(300):
        db.execute("INSERT INTO ev VALUES (?, ?, ?, ?, ?)",
                   params=[100_000 + i, 1, "dev-99", 1.0, "alpha bravo"])
    db.commit(tx)
    th("w_ins_txn_rows_s", round(300 / (time.perf_counter() - t0), 1))
    db.execute("DELETE FROM ev WHERE id >= 100000")

    def w_upd_auto():
        for i in range(300):
            db.execute("UPDATE ev SET val = val + 1 WHERE id = ?", params=[i])
    t0 = time.perf_counter(); w_upd_auto()
    th("w_upd_pk_auto_rows_s", round(300 / (time.perf_counter() - t0), 1))

    tx = db.begin()
    t0 = time.perf_counter()
    for i in range(300):
        db.execute("UPDATE ev SET val = val + 1 WHERE id = ?", params=[i])
    db.commit(tx)
    th("w_upd_pk_txn_rows_s", round(300 / (time.perf_counter() - t0), 1))

    t0 = time.perf_counter()
    for _ in range(5):
        db.execute("UPDATE ev SET val = val + 1 WHERE device = 'dev-07'")
    per = (time.perf_counter() - t0) / 5
    th("w_upd_scan_rows_s", round((N // 64) / per, 1))

    def w_del_auto():
        for i in range(300):
            db.execute("DELETE FROM ev WHERE id = ?", params=[30_000 + i])
    t0 = time.perf_counter(); w_del_auto()
    th("w_del_pk_auto_rows_s", round(300 / (time.perf_counter() - t0), 1))
    db.execute("INSERT INTO ev SELECT * FROM ev WHERE false")  # noop keep shapes
    for i in range(300):
        db.execute("INSERT INTO ev VALUES (?, ?, ?, ?, ?)",
                   params=[30_000 + i, 1, "dev-30", 1.0, "alpha bravo"])

    tx = db.begin()
    t0 = time.perf_counter()
    for i in range(300):
        db.execute("DELETE FROM ev WHERE id = ?", params=[40_000 + i])
    db.commit(tx)
    th("w_del_pk_txn_rows_s", round(300 / (time.perf_counter() - t0), 1))
    for i in range(300):
        db.execute("INSERT INTO ev VALUES (?, ?, ?, ?, ?)",
                   params=[40_000 + i, 1, "dev-40", 1.0, "alpha bravo"])

    # B. 事务 rollback 成本（300 更新后回滚，重开时数据应为旧值）
    tx = db.begin()
    for i in range(300):
        db.execute("UPDATE ev SET val = 12345.0 WHERE id = ?", params=[i])
    t0 = time.perf_counter()
    db.rollback(tx)
    th("t_rollback_300_ms", round((time.perf_counter() - t0) * 1e3, 3))
    got = db.query("SELECT val FROM ev WHERE id = 1")[1][0][0]
    assert abs(got - (float(1 % 997) / 7.0 + 2.0)) < 1e-6, f"rollback data wrong: {got}"

    # executemany 批语义（in-txn）
    tx = db.begin()
    t0 = time.perf_counter()
    db.executemany("UPDATE ev SET val = val + 1 WHERE id = ?",
                   [[i] for i in range(1000)])
    db.commit(tx)
    th("t_executemany_upd_rows_s", round(1000 / (time.perf_counter() - t0), 1))

    # C. 并发：8 线程并行点读
    def pread(_i):
        db.query("SELECT val FROM ev WHERE id = ?", params=[int(_i % N)])
    ids = list(range(800))
    t0 = time.perf_counter()
    with ThreadPoolExecutor(8) as ex:
        list(ex.map(pread, ids))
    th("c_par_read8_ms", round((time.perf_counter() - t0) * 1e3, 1))  # 800 queries

    # 读写并发：写连接开事务更新 500 行期间，同连接点读 200 次
    db2, _ = make_mote(os.path.join(tmp, "w.mote"))
    tx = db.begin()
    for i in range(500):
        db.execute("UPDATE ev SET val = 7.0 WHERE id = ?", params=[i])
    t0 = time.perf_counter()
    for i in range(200):
        db2.query("SELECT val FROM ev WHERE id = ?", params=[int(ids[i] % N)])
    th("c_read_during_write_ms", round((time.perf_counter() - t0) * 1e3, 1))
    db.rollback(tx); db2.close()

    # F. FTS（mote）
    t0 = time.perf_counter()
    db.execute("CREATE TEXT INDEX ev_note ON ev(note)")
    th("f_build_s", round(time.perf_counter() - t0, 3))
    th("f_match_top10_ms", lat(lambda: db.query(
        "SELECT id FROM ev WHERE MATCH(note, 'charlie delta') LIMIT 10"), 50))
    try:
        th("f_match_rank10_ms", lat(lambda: db.query(
            "SELECT id, BM25_SCORE() FROM ev WHERE MATCH(note, 'charlie delta') "
            "ORDER BY BM25_SCORE() DESC LIMIT 10"), 50))
    except Exception as e:
        th("f_match_rank10_ms", f"unsupported: {e}")
    db.close()


# ────────────────────────────────────────── sqlite ───────────────────────────

def bench_sqlite(tmp):
    import sqlite3
    p = os.path.join(tmp, "w.db")
    con = sqlite3.connect(p, isolation_level=None, check_same_thread=False)  # autocommit; pool threads share the conn
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("CREATE TABLE ev (id INT PRIMARY KEY, ts INT, device TEXT, val REAL, note TEXT)")
    t0 = time.perf_counter()
    con.execute("BEGIN")
    con.executemany("INSERT INTO ev VALUES (?,?,?,?,?)", base_rows())
    con.execute("COMMIT")
    th("_load_s", round(time.perf_counter() - t0, 2))

    def w_ins_auto():
        for i in range(300):
            con.execute("INSERT INTO ev VALUES (?,?,?,?,?)", (100_000 + i, 1, "dev-99", 1.0, "alpha bravo"))
    w_ins_auto(); con.execute("DELETE FROM ev WHERE id >= 100000")
    t0 = time.perf_counter(); w_ins_auto()
    th("w_ins_auto_rows_s", round(300 / (time.perf_counter() - t0), 1))
    con.execute("DELETE FROM ev WHERE id >= 100000")

    con.execute("BEGIN")
    t0 = time.perf_counter()
    for i in range(300):
        con.execute("INSERT INTO ev VALUES (?,?,?,?,?)", (100_000 + i, 1, "dev-99", 1.0, "alpha bravo"))
    con.execute("COMMIT")
    th("w_ins_txn_rows_s", round(300 / (time.perf_counter() - t0), 1))
    con.execute("DELETE FROM ev WHERE id >= 100000")

    def w_upd_auto():
        for i in range(300):
            con.execute("UPDATE ev SET val = val + 1 WHERE id = ?", (i,))
    t0 = time.perf_counter(); w_upd_auto()
    th("w_upd_pk_auto_rows_s", round(300 / (time.perf_counter() - t0), 1))

    con.execute("BEGIN")
    t0 = time.perf_counter()
    for i in range(300):
        con.execute("UPDATE ev SET val = val + 1 WHERE id = ?", (i,))
    con.execute("COMMIT")
    th("w_upd_pk_txn_rows_s", round(300 / (time.perf_counter() - t0), 1))

    t0 = time.perf_counter()
    for _ in range(5):
        con.execute("UPDATE ev SET val = val + 1 WHERE device = 'dev-07'")
    per = (time.perf_counter() - t0) / 5
    th("w_upd_scan_rows_s", round((N // 64) / per, 1))

    def w_del_auto():
        for i in range(300):
            con.execute("DELETE FROM ev WHERE id = ?", (30_000 + i,))
    t0 = time.perf_counter(); w_del_auto()
    th("w_del_pk_auto_rows_s", round(300 / (time.perf_counter() - t0), 1))
    con.executemany("INSERT INTO ev VALUES (?,?,?,?,?)",
                    [(30_000 + i, 1, "dev-30", 1.0, "alpha bravo") for i in range(300)])

    con.execute("BEGIN")
    t0 = time.perf_counter()
    for i in range(300):
        con.execute("DELETE FROM ev WHERE id = ?", (40_000 + i,))
    con.execute("COMMIT")
    th("w_del_pk_txn_rows_s", round(300 / (time.perf_counter() - t0), 1))
    con.executemany("INSERT INTO ev VALUES (?,?,?,?,?)",
                    [(40_000 + i, 1, "dev-40", 1.0, "alpha bravo") for i in range(300)])

    con.execute("BEGIN")
    for i in range(300):
        con.execute("UPDATE ev SET val = 12345.0 WHERE id = ?", (i,))
    t0 = time.perf_counter()
    con.execute("ROLLBACK")
    th("t_rollback_300_ms", round((time.perf_counter() - t0) * 1e3, 3))
    got = con.execute("SELECT val FROM ev WHERE id = 1").fetchone()[0]
    assert abs(got - (float(1 % 997) / 7.0 + 2.0)) < 1e-6, f"rollback data wrong: {got}"

    con.execute("BEGIN")
    t0 = time.perf_counter()
    con.executemany("UPDATE ev SET val = val + 1 WHERE id = ?", [(i,) for i in range(1000)])
    con.execute("COMMIT")
    th("t_executemany_upd_rows_s", round(1000 / (time.perf_counter() - t0), 1))

    # C. 并行点读（sqlite 同连接多线程串行 — 用 check_same_thread=False 仍受 GIL+锁限制，
    #    这是 SQLite python 内嵌的公平口径）
    ids = list(range(800))
    # WAL 多读者: 每线程独立连接 (同一连接跨线程并发 execute 会段错误 —
    # python sqlite3 的连接不是线程安全的; 每线程一连接也是 sqlite 的
    # 标准最佳实践, 对它最公平)
    tls = __import__("threading").local()

    def tcon():
        c = getattr(tls, "con", None)
        if c is None:
            c = sqlite3.connect(p, isolation_level=None)
            c.execute("PRAGMA journal_mode=WAL")
            tls.con = c
        return c

    def pread(_i):
        tcon().execute("SELECT val FROM ev WHERE id = ?", (_i % N,)).fetchone()
    t0 = time.perf_counter()
    with ThreadPoolExecutor(8) as ex:
        list(ex.map(pread, ids))
    th("c_par_read8_ms", round((time.perf_counter() - t0) * 1e3, 1))

    con2 = sqlite3.connect(p, isolation_level=None)
    con2.execute("PRAGMA journal_mode=WAL")
    con.execute("BEGIN")
    for i in range(500):
        con.execute("UPDATE ev SET val = 7.0 WHERE id = ?", (i,))
    t0 = time.perf_counter()
    for i in range(200):
        con2.execute("SELECT val FROM ev WHERE id = ?", (ids[i] % N,)).fetchone()
    th("c_read_during_write_ms", round((time.perf_counter() - t0) * 1e3, 1))
    con.execute("ROLLBACK"); con2.close()

    # F. FTS5
    t0 = time.perf_counter()
    con.execute("CREATE VIRTUAL TABLE ev_fts USING fts5(note, content='ev', content_rowid='id')")
    con.execute("INSERT INTO ev_fts(rowid, note) SELECT id, note FROM ev")
    th("f_build_s", round(time.perf_counter() - t0, 3))
    th("f_match_top10_ms", lat(lambda: con.execute(
        "SELECT rowid FROM ev_fts WHERE ev_fts MATCH 'charlie delta' LIMIT 10").fetchall(), 50))
    th("f_match_rank10_ms", lat(lambda: con.execute(
        "SELECT rowid FROM ev_fts WHERE ev_fts MATCH 'charlie delta' "
        "ORDER BY bm25(ev_fts) LIMIT 10").fetchall(), 50))
    con.close()


# ────────────────────────────────────────── duckdb ───────────────────────────

def bench_duckdb(tmp):
    import duckdb
    p = os.path.join(tmp, "w.duck")
    con = duckdb.connect(p)
    con.execute("CREATE TABLE ev (id INT PRIMARY KEY, ts BIGINT, device TEXT, val DOUBLE, note TEXT)")
    rows = base_rows()
    cols = list(zip(*rows))
    import numpy as np
    arrow_tbl = None
    try:
        import pyarrow as pa
        arrow_tbl = pa.table({"id": np.array(cols[0], dtype="int32"),
                              "ts": np.array(cols[1], dtype="int64"),
                              "device": list(cols[2]),
                              "val": np.array(cols[3], dtype="float64"),
                              "note": list(cols[4])})
    except ImportError:
        pass
    t0 = time.perf_counter()
    if arrow_tbl is not None:
        con.register("base_arrow", arrow_tbl)
        con.execute("INSERT INTO ev SELECT * FROM base_arrow")
        con.unregister("base_arrow")
    else:
        con.executemany("INSERT INTO ev VALUES (?,?,?,?,?)", rows)
    th("_load_s", round(time.perf_counter() - t0, 2))

    # duckdb 事务/写路径
    def w_ins_auto():
        for i in range(300):
            con.execute("INSERT INTO ev VALUES (?,?,?,?,?)", [100_000 + i, 1, "dev-99", 1.0, "alpha bravo"])
    w_ins_auto(); con.execute("DELETE FROM ev WHERE id >= 100000")
    t0 = time.perf_counter(); w_ins_auto()
    th("w_ins_auto_rows_s", round(300 / (time.perf_counter() - t0), 1))
    con.execute("DELETE FROM ev WHERE id >= 100000")

    con.execute("BEGIN")
    t0 = time.perf_counter()
    for i in range(300):
        con.execute("INSERT INTO ev VALUES (?,?,?,?,?)", [100_000 + i, 1, "dev-99", 1.0, "alpha bravo"])
    con.execute("COMMIT")
    th("w_ins_txn_rows_s", round(300 / (time.perf_counter() - t0), 1))
    con.execute("DELETE FROM ev WHERE id >= 100000")

    def w_upd_auto():
        for i in range(300):
            con.execute("UPDATE ev SET val = val + 1 WHERE id = ?", [i])
    t0 = time.perf_counter(); w_upd_auto()
    th("w_upd_pk_auto_rows_s", round(300 / (time.perf_counter() - t0), 1))

    con.execute("BEGIN")
    t0 = time.perf_counter()
    for i in range(300):
        con.execute("UPDATE ev SET val = val + 1 WHERE id = ?", [i])
    con.execute("COMMIT")
    th("w_upd_pk_txn_rows_s", round(300 / (time.perf_counter() - t0), 1))

    t0 = time.perf_counter()
    for _ in range(5):
        con.execute("UPDATE ev SET val = val + 1 WHERE device = 'dev-07'")
    per = (time.perf_counter() - t0) / 5
    th("w_upd_scan_rows_s", round((N // 64) / per, 1))

    def w_del_auto():
        for i in range(300):
            con.execute("DELETE FROM ev WHERE id = ?", [30_000 + i])
    t0 = time.perf_counter(); w_del_auto()
    th("w_del_pk_auto_rows_s", round(300 / (time.perf_counter() - t0), 1))
    con.executemany("INSERT INTO ev VALUES (?,?,?,?,?)",
                    [(30_000 + i, 1, "dev-30", 1.0, "alpha bravo") for i in range(300)])

    con.execute("BEGIN")
    t0 = time.perf_counter()
    for i in range(300):
        con.execute("DELETE FROM ev WHERE id = ?", [40_000 + i])
    con.execute("COMMIT")
    th("w_del_pk_txn_rows_s", round(300 / (time.perf_counter() - t0), 1))
    con.executemany("INSERT INTO ev VALUES (?,?,?,?,?)",
                    [(40_000 + i, 1, "dev-40", 1.0, "alpha bravo") for i in range(300)])

    con.execute("BEGIN")
    for i in range(300):
        con.execute("UPDATE ev SET val = 12345.0 WHERE id = ?", [i])
    t0 = time.perf_counter()
    con.execute("ROLLBACK")
    th("t_rollback_300_ms", round((time.perf_counter() - t0) * 1e3, 3))
    got = con.execute("SELECT val FROM ev WHERE id = 1").fetchone()[0]
    assert abs(got - (float(1 % 997) / 7.0 + 2.0)) < 1e-6, f"rollback data wrong: {got}"

    con.execute("BEGIN")
    t0 = time.perf_counter()
    con.executemany("UPDATE ev SET val = val + 1 WHERE id = ?", [(i,) for i in range(1000)])
    con.execute("COMMIT")
    th("t_executemany_upd_rows_s", round(1000 / (time.perf_counter() - t0), 1))

    _tls = __import__("threading").local()

    def pread(_i):
        # duckdb 连接同样不能跨线程并发 execute — cursor() 复制同库连接
        # (duckdb 官方并发用法), 每线程缓存一个
        c = getattr(_tls, "cur", None)
        if c is None:
            c = con.cursor()
            _tls.cur = c
        c.execute("SELECT val FROM ev WHERE id = ?", [_i % N]).fetchone()
    ids = list(range(800))
    t0 = time.perf_counter()
    with ThreadPoolExecutor(8) as ex:
        list(ex.map(pread, ids))
    th("c_par_read8_ms", round((time.perf_counter() - t0) * 1e3, 1))

    # 读写并发：duckdb 同进程第二连接只读打开
    try:
        con2 = duckdb.connect(p, read_only=True)
        con.execute("BEGIN")
        for i in range(500):
            con.execute("UPDATE ev SET val = 7.0 WHERE id = ?", [i])
        t0 = time.perf_counter()
        for i in range(200):
            con2.execute("SELECT val FROM ev WHERE id = ?", [ids[i] % N]).fetchone()
        th("c_read_during_write_ms", round((time.perf_counter() - t0) * 1e3, 1))
        con.execute("ROLLBACK"); con2.close()
    except Exception as e:
        th("c_read_during_write_ms", f"unsupported: {type(e).__name__}")
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass

    # F: duckdb 无内嵌 FTS — 只给 LIKE 对照
    th("f_build_s", None)
    th("f_match_top10_ms", lat(lambda: con.execute(
        "SELECT id FROM ev WHERE note LIKE '%charlie%delta%' LIMIT 10").fetchall(), 50))
    con.close()


# ───────────────────────────────── 崩溃恢复 / 冷启动（子进程）─────────────────

def crash_child(engine, path):
    """打开、写 CRASH_COMMIT 行（已提交）、再开事务写 CRASH_UNCOMMITTED 行、os._exit 模拟断电。"""
    if engine == "mote":
        db, _ = make_mote(path)
        db.execute("CREATE TABLE ev (id INT PRIMARY KEY, v INT)")
        for i in range(CRASH_COMMIT):
            db.execute("INSERT INTO ev VALUES (?, ?)", params=[i, i])
        db.begin()
        for i in range(CRASH_UNCOMMITTED):
            db.execute("INSERT INTO ev VALUES (?, ?)", params=[CRASH_COMMIT + i, i])
        sys.stdout.flush()
        os._exit(1)
    elif engine == "sqlite":
        import sqlite3
        con = sqlite3.connect(path, isolation_level=None)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        con.execute("CREATE TABLE ev (id INT PRIMARY KEY, v INT)")
        con.execute("BEGIN")
        con.executemany("INSERT INTO ev VALUES (?,?)", [(i, i) for i in range(CRASH_COMMIT)])
        con.execute("COMMIT")
        con.execute("BEGIN")
        con.executemany("INSERT INTO ev VALUES (?,?)",
                        [(CRASH_COMMIT + i, i) for i in range(CRASH_UNCOMMITTED)])
        sys.stdout.flush()
        os._exit(1)
    else:
        import duckdb
        con = duckdb.connect(path)
        con.execute("CREATE TABLE ev (id INT PRIMARY KEY, v INT)")
        con.executemany("INSERT INTO ev VALUES (?,?)", [(i, i) for i in range(CRASH_COMMIT)])
        con.execute("BEGIN")
        con.executemany("INSERT INTO ev VALUES (?,?)",
                        [(CRASH_COMMIT + i, i) for i in range(CRASH_UNCOMMITTED)])
        sys.stdout.flush()
        os._exit(1)


def open_child(engine, path):
    """全新进程打开既有库 + 首点查，输出 JSON。"""
    t0 = time.perf_counter()
    if engine == "mote":
        db, _ = make_mote(path)
        db.query("SELECT v FROM ev WHERE id = ?", params=[7])
    elif engine == "sqlite":
        import sqlite3
        con = sqlite3.connect(path)
        con.execute("SELECT v FROM ev WHERE id = 7").fetchone()
    else:
        import duckdb
        con = duckdb.connect(path)
        con.execute("SELECT v FROM ev WHERE id = 7").fetchone()
    open_s = time.perf_counter() - t0
    print("JSON " + json.dumps({"open_first_query_s": round(open_s, 4)}))


def bench_crash_and_open(engine, tmp):
    """父进程：spawn crash-child → 重开验证 → open-child 测冷启动。"""
    ext = {"mote": "mote", "sqlite": "db", "duckdb": "duck"}[engine]
    path = os.path.join(tmp, f"crash.{ext}")

    t0 = time.perf_counter()
    subprocess.run([sys.executable, os.path.abspath(__file__), "--engine", engine,
                    "--mode", "crash-child", "--path", path], check=False)
    spawn_s = time.perf_counter() - t0

    # 重开验证
    t0 = time.perf_counter()
    if engine == "mote":
        db, _ = make_mote(path)
        n = db.query("SELECT COUNT(*) FROM ev")[1][0][0]
        ids = [r[0] for r in db.query("SELECT id FROM ev WHERE id >= ? ORDER BY id LIMIT 5",
                                      params=[CRASH_COMMIT])[1]]
        db.close()  # 释放文件锁 — open-child 子进程要独立打开
    elif engine == "sqlite":
        import sqlite3
        con = sqlite3.connect(path)
        n = con.execute("SELECT COUNT(*) FROM ev").fetchone()[0]
        ids = [r[0] for r in con.execute(
            "SELECT id FROM ev WHERE id >= ? ORDER BY id LIMIT 5", (CRASH_COMMIT,)).fetchall()]
        con.close()
    else:
        import duckdb
        con = duckdb.connect(path)
        n = con.execute("SELECT COUNT(*) FROM ev").fetchone()[0]
        ids = [r[0] for r in con.execute(
            "SELECT id FROM ev WHERE id >= ? ORDER BY id LIMIT 5", [CRASH_COMMIT]).fetchall()]
        con.close()  # duckdb 同样单进程独占
    reopen_s = time.perf_counter() - t0
    th("x_reopen_s", round(reopen_s, 4))
    th("x_rows_visible", int(n))
    th("x_uncommitted_leaked", bool(ids), )
    th("x_recover_ok", n == CRASH_COMMIT and not ids)
    # 冷启动（子进程全进口径）
    out = subprocess.run([sys.executable, os.path.abspath(__file__), "--engine", engine,
                          "--mode", "open-child", "--path", path],
                         capture_output=True, text=True, check=False)
    for line in out.stdout.splitlines():
        if line.startswith("JSON "):
            th("e_open_first_query_s", json.loads(line[5:])["open_first_query_s"])
    th("_crash_spawn_s", round(spawn_s, 3))


# ────────────────────────────────────────── main ─────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", required=True, choices=["mote", "sqlite", "duckdb"])
    ap.add_argument("--mode", default="bench", choices=["bench", "crash-child", "open-child"])
    ap.add_argument("--path", default=None)
    a = ap.parse_args()

    if a.mode == "crash-child":
        crash_child(a.engine, a.path)
        return
    if a.mode == "open-child":
        open_child(a.engine, a.path)
        return

    tmp = tempfile.mkdtemp(prefix=f"cw_{a.engine}_")
    try:
        {"mote": bench_mote, "sqlite": bench_sqlite, "duckdb": bench_duckdb}[a.engine](tmp)
        bench_crash_and_open(a.engine, tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print("JSON " + json.dumps(R))


if __name__ == "__main__":
    main()
