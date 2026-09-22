#!/usr/bin/env python3
"""M1/M4 并行 + M2 大表截胡的 A/B 对拍 (250K 行, ≥PARALLEL_MIN_ROWS=200K)。

MOTE_VEC=on (并行分支) vs MOTE_VEC=off (旧路径) 必须全等。
用法: python3 test_parallel_ab.py
"""
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
BENCH = "/tmp/mote_parallel_ab_data.mote"

SETUP = r'''
import os, sys, time
sys.path.insert(0, "/Users/luo/glm/motedb/bindings/python")
import motedb
import numpy as np
N = 250_000
path = sys.argv[1]
db = motedb.Database(path)
db.execute("CREATE TABLE s (id INTEGER PRIMARY KEY AUTO_INCREMENT, device TEXT, zone TEXT, val FLOAT, ts TIMESTAMP)")
rng = np.random.default_rng(11)
devs = np.array([f"dev-{i:03d}" for i in range(64)])
zones = np.array(["north", "south", "east", "west"])
ins = 0
while ins < N:
    m = min(50000, N - ins)
    db.insert_arrays("s", {
        "device": list(devs[rng.integers(0, 64, m)]),
        "zone": list(zones[rng.integers(0, 4, m)]),
        "val": rng.random(m) * 1000.0,
        "ts": [(1700000000 + int(x)) * 1_000_000 for x in np.sort(rng.integers(0, 2592000, m))],
    })
    ins += m
# 制造少量 UPDATE/DELETE (墓碑+多段) — 部分形状会 decline, 对拍仍须全等
db.execute("UPDATE s SET val = val + 1.0 WHERE id <= 500")
db.execute("DELETE FROM s WHERE id > 249000")
db.execute("CHECKPOINT")
db.close()
print("SETUP_DONE")
'''

QUERIES = [
    # M1 无谓词
    ("m1_nopred", "SELECT COUNT(*), SUM(val), MIN(val), MAX(val) FROM s"),
    # M1 AND 链
    ("m1_and", "SELECT COUNT(*), AVG(val) FROM s WHERE val > 500.0 AND zone = 'north'"),
    ("m1_and_ts", "SELECT COUNT(*), MIN(ts), MAX(ts) FROM s WHERE val < 300.0 AND ts > 1700040000000000"),
    # M1 混合 (OR)
    ("m1_or", "SELECT COUNT(*), SUM(val) FROM s WHERE val > 900.0 OR val < 50.0"),
    ("m1_or3", "SELECT COUNT(*), AVG(val), MIN(val), MAX(val) FROM s WHERE (val > 800.0 OR val < 100.0) AND zone != 'south'"),
    # M2 大表 GROUP BY 无 ORDER (新截胡)
    ("m2_groupby", "SELECT device, COUNT(*), SUM(val), MIN(val), MAX(val) FROM s GROUP BY device"),
    ("m2_groupby_where", "SELECT zone, COUNT(*), AVG(val) FROM s WHERE val > 200.0 GROUP BY zone"),
    # M4 filter top-k
    ("m4_topk", "SELECT id, device, ts FROM s WHERE device = 'dev-007' ORDER BY ts DESC LIMIT 10"),
    ("m4_topk_asc", "SELECT id, val FROM s WHERE zone = 'east' AND val > 100.0 ORDER BY val LIMIT 20"),
    # M4 深分页
    ("m4_deep", "SELECT id FROM s WHERE val < 900.0 ORDER BY id LIMIT 100 OFFSET 248500"),
    # 混合谓词 top-k (OR)
    ("m4_topk_or", "SELECT id, val FROM s WHERE val > 950.0 OR val < 20.0 ORDER BY val DESC LIMIT 15"),
]


def run(mode):
    env = dict(os.environ, MOTE_VEC=mode)
    out = subprocess.run(
        [sys.executable, "-", BENCH],
        input=RUNNER,
        capture_output=True,
        text=True,
        env=env,
    )
    if out.returncode != 0:
        print(out.stderr[-2000:])
        sys.exit(f"runner failed in {mode} mode")
    result = {}
    for line in out.stdout.splitlines():
        if line.startswith("R\t"):
            _, name, payload = line.split("\t", 2)
            result[name] = payload
    return result


RUNNER = r'''
import os, sys
sys.path.insert(0, "/Users/luo/glm/motedb/bindings/python")
import motedb
db = motedb.Database(sys.argv[1])
QS = %s
for name, sql in QS:
    try:
        rows = db.execute(sql)
        if name.startswith("m2_groupby"):
            rows = sorted(map(lambda r: tuple(sorted(r.items())), rows))
        print("R\t" + name + "\t" + repr(rows))
    except Exception as e:
        print("R\t" + name + "\tERROR:" + str(e))
db.close()
''' % repr(QUERIES)

# setup once (both modes read the same data file — read-only queries)
tmp = tempfile.mkdtemp()
d = subprocess.run([sys.executable, "-", os.path.join(tmp, "d.mote")],
                   input=SETUP, capture_output=True, text=True)
if "SETUP_DONE" not in d.stdout:
    print(d.stdout[-500:], d.stderr[-2000:])
    sys.exit("setup failed")

on = run("on")
off = run("off")
fails = 0
for name, _ in QUERIES:
    a, b = on.get(name), off.get(name)
    if a == b:
        print(f"ok   {name}")
    else:
        fails += 1
        print(f"FAIL {name}")
        print(f"  on : {str(a)[:200]}")
        print(f"  off: {str(b)[:200]}")

import shutil
shutil.rmtree(tmp, ignore_errors=True)
print("ALL OK" if fails == 0 else f"{fails} FAILURES")
sys.exit(1 if fails else 0)
