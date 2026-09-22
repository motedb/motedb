#!/usr/bin/env python3
"""insert_arrays 列式批量插入正确性: 与 executemany 逐行对拍 (值/NULL/向量/unicode/重开)。"""
import os, sys, tempfile, shutil
import numpy as np
import motedb

FAIL = 0

def check(name, got, want):
    global FAIL
    if got != want:
        FAIL += 1
        print(f"FAIL {name}: got {got!r} want {want!r}")
    else:
        print(f"ok   {name}")

tmp = tempfile.mkdtemp(prefix="ia_")
try:
    db = motedb.Database(os.path.join(tmp, "a.db"))
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, ts TIMESTAMP, dev TEXT, val FLOAT, note TEXT, emb VECTOR(4))")
    N = 200
    ids = np.arange(N, dtype=np.int64)
    ts = (1_700_000_000 + np.arange(N)) * 1_000_000
    dev = np.array([f"dev-{i % 5}" for i in range(N)])
    val = np.arange(N, dtype=np.float32) * 0.5
    note = [None if i % 7 == 0 else f"row {i} note" for i in range(N)]  # 含 NULL
    emb = np.arange(N * 4, dtype=np.float32).reshape(N, 4)

    n = db.insert_arrays("t", {
        "id": ids, "ts": ts, "dev": dev, "val": val, "note": note, "emb": emb,
    })
    check("row count", n, N)
    check("count(*)", db.execute("SELECT COUNT(*) AS c FROM t")[0]["c"], N)

    # NULL 语义
    check("null count", db.execute("SELECT COUNT(*) AS c FROM t WHERE note IS NULL")[0]["c"],
          sum(1 for x in note if x is None))
    # 抽行验证
    r = db.execute("SELECT id, ts, dev, val, note, emb FROM t WHERE id = 3")[0]
    check("row 3 scalars", (r["id"], r["ts"], r["dev"], r["val"]), (3, int(ts[3]), "dev-3", 1.5))
    check("row 3 emb", [round(x, 4) for x in r["emb"]], [round(float(x), 4) for x in emb[3]])
    r = db.execute("SELECT note FROM t WHERE id = 7")[0]["note"]
    check("row 7 note NULL", r, None)
    # unicode 数组列直传
    db2 = motedb.Database(os.path.join(tmp, "b.mote"))
    db2.execute("CREATE TABLE u (id INT PRIMARY KEY, s TEXT)")
    u = np.array(["héllo", "wörld", "中文", "x"])
    db2.insert_arrays("u", {"id": np.arange(4, dtype=np.int64), "s": u})
    got = [r["s"] for r in db2.execute("SELECT s FROM u ORDER BY id")]
    check("unicode roundtrip", got, ["héllo", "wörld", "中文", "x"])
    # executemany 对拍 (整表)
    em = []
    for i in range(N):
        em.append((int(i), int(ts[i]), str(dev[i]), float(val[i]), note[i], [float(x) for x in emb[i]]))
    db3 = motedb.Database(os.path.join(tmp, "c.db"))
    db3.execute("CREATE TABLE t (id INT PRIMARY KEY, ts TIMESTAMP, dev TEXT, val FLOAT, note TEXT, emb VECTOR(4))")
    db3.executemany("INSERT INTO t VALUES (?, ?, ?, ?, ?, ?)", em)
    a = db.execute("SELECT SUM(val) AS s, AVG(val) AS a, MIN(note) AS mn FROM t")[0]
    b = db3.execute("SELECT SUM(val) AS s, AVG(val) AS a, MIN(note) AS mn FROM t")[0]
    check("agg parity", (round(a["s"], 4), round(a["a"], 4), a["mn"]),
          (round(b["s"], 4), round(b["a"], 4), b["mn"]))
    # 重开一致性
    db2.close(); db2 = motedb.Database(os.path.join(tmp, "b.mote"))
    check("reopen", [r["s"] for r in db2.execute("SELECT s FROM u ORDER BY id")], ["héllo", "wörld", "中文", "x"])
finally:
    shutil.rmtree(tmp, ignore_errors=True)

# ── 按位放置回归 (2026-09 修复: 旧实现按字典序转置 → 错位损毁) ──
dbp = motedb.Database(os.path.join(tmp, "pos.mote"))
dbp.execute("CREATE TABLE t (id INTEGER PRIMARY KEY AUTO_INCREMENT, c TEXT, v FLOAT)")
# 1) 字典序 != schema 序
dbp.insert_arrays("t", {"v": [9.5, 8.5], "c": ["zz", "yy"]})
check("dict order != schema order", dbp.execute("SELECT c, v FROM t ORDER BY c"),
      [{"c": "yy", "v": 8.5}, {"c": "zz", "v": 9.5}])
# 2) 省略自增 PK: 小批 (慢路径) 与大批 (快路径) 数据均完整
dbp.insert_arrays("t", {"c": ["small"], "v": [0.5]})
dbp.insert_arrays("t", {"c": [f"L{i}" for i in range(150)], "v": [1.0] * 150})
got = dbp.execute("SELECT c, v FROM t WHERE c = 'small'")
check("omitted PK small batch", got, [{"c": "small", "v": 0.5}])
got = dbp.execute("SELECT c, v FROM t WHERE c = 'L149'")
check("omitted PK large batch", got, [{"c": "L149", "v": 1.0}])
check("auto id sequence", [r["id"] for r in dbp.execute("SELECT id FROM t ORDER BY id LIMIT 4")],
      [1, 2, 3, 4])
# 3) 省略非 PK 列 → NULL
dbp.execute("CREATE TABLE u (id INTEGER PRIMARY KEY AUTO_INCREMENT, a TEXT, b FLOAT)")
dbp.insert_arrays("u", {"a": ["only_a"]})
check("omitted non-PK col is NULL", dbp.execute("SELECT * FROM u"),
      [{"id": 1, "a": "only_a", "b": None}])
# 4) 未知列报错
try:
    dbp.insert_arrays("u", {"nope": [1]})
    check("unknown column errors", "no error", "error")
except (RuntimeError, ValueError):
    check("unknown column errors", "error", "error")
# 5) 显式 PK 大批不被改写 (Rust 门修复的对拍)
dbp.execute("CREATE TABLE w (id INTEGER PRIMARY KEY AUTO_INCREMENT, x TEXT)")
dbp.insert_arrays("w", {"x": ["a", "b"]})  # 先 2 行 auto id
dbp.insert_arrays("w", {"id": list(range(1000, 1150)), "x": [f"e{i}" for i in range(150)]})
check("explicit PK honored in large batch",
      dbp.execute("SELECT x FROM w WHERE id = 1000"), [{"x": "e0"}])
dbp.close()

print("ALL OK" if FAIL == 0 else f"{FAIL} FAILURES")
sys.exit(1 if FAIL else 0)
