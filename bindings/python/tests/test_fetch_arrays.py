#!/usr/bin/env python3
"""fetch_arrays 列式取回回归: 类型映射 / NULL 回退 / 空结果 / 参数化 /
向量列 / 错误形状 / 与 execute() 对拍。"""
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import motedb

try:
    import numpy as np
    HAS_NP = True
except ImportError:
    HAS_NP = False

FAIL = 0


def check(name, got, want):
    global FAIL
    if got == want:
        print(f"ok   {name}")
    else:
        FAIL += 1
        print(f"FAIL {name}\n  got:  {got!r}\n  want: {want!r}")


tmp = tempfile.mkdtemp()
try:
    db = motedb.Database(os.path.join(tmp, "a.mote"))
    db.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY AUTO_INCREMENT, v FLOAT, s TEXT, b BOOLEAN, ts TIMESTAMP, emb VECTOR(3))"
    )
    N = 2000
    db.insert_arrays("t", {
        "v": [i * 0.25 for i in range(N)],
        "s": [f"s{i % 40}" for i in range(N)],
        "b": [i % 3 == 0 for i in range(N)],
        "ts": [1700000000000000 + i * 1000 for i in range(N)],
        "emb": [[0.1 * (i % 5), 0.2, 0.3] for i in range(N)],
    })
    db.execute("CHECKPOINT")

    q = "SELECT id, v, s, b, ts, emb FROM t WHERE v >= 100.0"
    dict_rows = db.execute(q)
    cols, arrays = db.fetch_arrays(q)

    check("columns", cols, ["id", "v", "s", "b", "ts", "emb"])
    n = len(dict_rows)
    check("row count", len(arrays["id"]), n)
    if HAS_NP:
        check("id dtype", str(arrays["id"].dtype), "int64")
        check("v dtype", str(arrays["v"].dtype), "float64")
        check("b dtype", str(arrays["b"].dtype), "bool")
        check("ts dtype", str(arrays["ts"].dtype), "int64")
        check("id is ndarray", isinstance(arrays["id"], np.ndarray), True)

    # 全行对拍 (dict vs 列式)
    ok = True
    for i in range(n):
        if dict_rows[i]["id"] != int(arrays["id"][i]):
            ok = False
            break
        if abs(dict_rows[i]["v"] - float(arrays["v"][i])) > 1e-12:
            ok = False
            break
        if dict_rows[i]["s"] != arrays["s"][i]:
            ok = False
            break
        if dict_rows[i]["b"] != bool(arrays["b"][i]):
            ok = False
            break
        if dict_rows[i]["ts"] != int(arrays["ts"][i]):
            ok = False
            break
        if [round(x, 6) for x in dict_rows[i]["emb"]] != [round(x, 6) for x in arrays["emb"][i]]:
            ok = False
            break
    check("full differential vs execute()", ok, True)
    check("text col is list", type(arrays["s"]).__name__, "list")
    check("vector col is list", type(arrays["emb"]).__name__, "list")

    # 参数化
    c2, a2 = db.fetch_arrays("SELECT id FROM t WHERE v > ?", params=[500.0])
    check("params", len(a2["id"]), db.execute("SELECT COUNT(*) FROM t WHERE v > 500.0")[0]["COUNT(*)"])

    # NULL 列 → 逐值列表
    db.execute("CREATE TABLE n2 (id INTEGER PRIMARY KEY AUTO_INCREMENT, v FLOAT)")
    db.insert_arrays("n2", {"v": [1.0, None, 3.0]})
    _, a3 = db.fetch_arrays("SELECT v FROM n2")
    check("NULL col fallback", list(a3["v"]), [1.0, None, 3.0])

    # 空结果
    _, a4 = db.fetch_arrays("SELECT id, v FROM t WHERE v < 0")
    if HAS_NP:
        check("empty result lens", (len(a4["id"]), len(a4["v"])), (0, 0))

    # 非 SELECT → 报错
    try:
        db.fetch_arrays("INSERT INTO n2 (v) VALUES (9.0)")
        check("non-select errors", "no error", "error")
    except (RuntimeError, ValueError):
        check("non-select errors", "error", "error")

    # LIMIT/OFFSET 形状
    _, a5 = db.fetch_arrays("SELECT id FROM t ORDER BY id LIMIT 5 OFFSET 10")
    check("limit offset", [int(x) for x in a5["id"]], [11, 12, 13, 14, 15])

    # 重开一致性
    db.close()
    db = motedb.Database(os.path.join(tmp, "a.mote"))
    _, a6 = db.fetch_arrays("SELECT COUNT(*) FROM t")
    check("reopen count", int(arrays_count[0]) if (arrays_count := a6["COUNT(*)"]) else 0, N)
    db.close()
finally:
    shutil.rmtree(tmp, ignore_errors=True)

print("ALL OK" if FAIL == 0 else f"{FAIL} FAILURES")
sys.exit(1 if FAIL else 0)
