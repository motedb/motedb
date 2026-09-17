#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Round 13 特性自洽校验 (tier-2) — MoteDB 特有功能对照 Python 暴力计算.

覆盖: 向量 KNN (L2/cosine) × 变异可见性, LATEST BY (+ORDER BY),
MATCH/BM25 (需 TEXT INDEX — 无索引时 MATCH 过滤可用但 BM25_SCORE 为
NULL, 属文档化限制), 空间 ST_WITHIN / loc <-> ST_POINT top-k,
TIMESERIES 删插 (行不可变设计), 事务回滚/提交.

用法: python3 test_feature_selfcheck.py [seed]
"""
import math
import os
import random
import shutil
import sys
import tempfile

import motedb

SEED = int(sys.argv[1]) if len(sys.argv) > 1 else 1
rng = random.Random(SEED)
tmp = tempfile.mkdtemp(prefix="mote_feat_%d_" % SEED)
fails = []


def check(name, cond, detail=""):
    if not cond:
        fails.append(name)
        print("FAIL: %s %s" % (name, detail))


db = motedb.Database(os.path.join(tmp, "f.mote"))

# ───────────────────────── 向量 KNN ─────────────────────────
DIM = 8
db.execute("CREATE TABLE vec (id INT PRIMARY KEY, cat TEXT, emb VECTOR(%d))" % DIM)
N = 300
vecs = {}
rows = []
for i in range(1, N + 1):
    v = [rng.uniform(-3, 3) for _ in range(DIM)]
    vecs[i] = v
    rows.append([i, rng.choice(["a", "b", "c"]), v])
db.executemany("INSERT INTO vec VALUES (?, ?, ?)", rows)


def brute_knn(q, k, metric, cat=None):
    def dist(v):
        if metric == "l2":
            return sum((a - b) ** 2 for a, b in zip(q, v))
        dot = sum(a * b for a, b in zip(q, v))
        na = math.sqrt(sum(a * a for a in q)) * math.sqrt(sum(b * b for b in v))
        return 1.0 - dot / na if na else 1.0
    cand = [(dist(v), i) for i, v in vecs.items() if cat is None or CATS[i] == cat]
    cand.sort(key=lambda t: (t[0], t[1]))
    return [i for _, i in cand[:k]]


CATS = {r[0]: r[1] for r in rows}
for trial in range(6):
    q = [rng.uniform(-3, 3) for _ in range(DIM)]
    k = rng.randint(1, 10)
    metric = rng.choice(["l2", "cosine"])
    op = "<->" if metric == "l2" else "<=>" if False else None
    # 绑定名: L2 用 <->, cosine 用 <=>
    op = "<->" if metric == "l2" else "<=>"
    cols, got = db.query(
        "SELECT id, emb %s ? AS d FROM vec ORDER BY d ASC LIMIT %d" % (op, k), [q])
    want = brute_knn(q, k, metric)
    got_ids = [r[0] for r in got]
    # 浮点 tie 时允许相邻距离差 < 1e-9 的成员差异 — 否则必须精确
    check("knn_%s_k%d" % (metric, k), got_ids == want,
          "got %s want %s" % (got_ids[:5], want[:5]))

# KNN 变异可见性: DELETE + UPDATE 向量后立即查询
del_ids = rng.sample(list(vecs), 40)
for i in del_ids:
    db.execute("DELETE FROM vec WHERE id = ?", [i])
for i in del_ids:
    del vecs[i]
upd_ids = rng.sample(list(vecs), 30)
for i in upd_ids:
    v = [rng.uniform(-3, 3) for _ in range(DIM)]
    vecs[i] = v
    db.execute("UPDATE vec SET emb = ? WHERE id = ?", [v, i])
db.checkpoint()
for trial in range(4):
    q = [rng.uniform(-3, 3) for _ in range(DIM)]
    k = rng.randint(1, 10)
    cols, got = db.query(
        "SELECT id, emb <-> ? AS d FROM vec ORDER BY d ASC LIMIT %d" % k, [q])
    want = brute_knn(q, k, "l2")
    check("knn_after_mut_t%d" % trial, [r[0] for r in got] == want,
          "got %s want %s" % ([r[0] for r in got][:5], want[:5]))

# ───────────────────────── LATEST BY ─────────────────────────
db.execute("CREATE TABLE m (sensor TEXT, ts TIMESTAMP, v REAL) TIMESERIES(ts)")
SENSORS = ["s1", "s2", "s3"]
latest = {}
ts_of = {}
allrows = []
ts = 1000
for i in range(90):
    ts += rng.randint(1, 5)
    s = rng.choice(SENSORS)
    v = round(rng.uniform(0, 100), 3)
    allrows.append([s, ts, v])
db.executemany("INSERT INTO m VALUES (?, ?, ?)", allrows)
for s, t, v in allrows:
    latest[s] = (t, v)
cols, got = db.query("SELECT sensor, ts, v FROM m LATEST BY sensor ORDER BY sensor")
want = sorted([[s, latest[s][0], latest[s][1]] for s in SENSORS])
check("latest_by_sensor", [list(r) for r in got] == want,
      "got %r want %r" % (got, want))
# timeseries 行不可变 (设计): DELETE 到该时间点(保留更早?) 不行 — 只支持
# ts < v 形式。改用: 删掉 s2 全部 (< max_ts+1) 再重插一条最新记录
db.execute("DELETE FROM m WHERE ts < %d" % (latest["s2"][0] + 1))
db.execute("INSERT INTO m VALUES ('s2', %d, -1.0)" % (latest["s2"][0] + 100))
cols, got = db.query("SELECT sensor, v FROM m LATEST BY sensor ORDER BY sensor")
d = {r[0]: r[1] for r in got}
# DELETE ts < s2_latest+1 删除所有更早行; 各 sensor 幸存与否取决于其 latest ts
ok = d.get("s2") == -1.0
for s in SENSORS:
    if s == "s2":
        continue
    t, v = latest[s]
    if t >= latest["s2"][0] + 1:
        ok = ok and d.get(s) == v
    else:
        ok = ok and s not in d
check("latest_by_after_delete_reinsert", ok, repr(d))

# ───────────────────────── MATCH / BM25 ─────────────────────────
db.execute("CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)")
DOCS = {
    1: "the quick brown fox jumps over the lazy dog",
    2: "quick quick brown cats",
    3: "lazy dogs sleep all day",
    4: "the fox and the hound",
    5: "quicker than quick",
}
db.executemany("INSERT INTO docs VALUES (?, ?)", [[k, v] for k, v in DOCS.items()])
db.checkpoint()
db.execute("CREATE TEXT INDEX docs_body ON docs(body)")
cols, got = db.query(
    "SELECT id, BM25_SCORE() AS s FROM docs WHERE MATCH(body, 'quick') ORDER BY s DESC, id ASC")
# brute tf: docs containing "quick" (词干化 quick/quicker 视实现而定 — 只验证包含
# 精确词的 doc 全部命中且分数>0, 未命中 doc 不出现)
hits = sorted(k for k, t in DOCS.items() if "quick" in t.split())
got_ids = [r[0] for r in got]
check("match_quick_covers", all(h in got_ids for h in [1, 2, 5]),
      "got %r" % got_ids)
check("match_quick_only_matching", all(g in hits + [5] for g in got_ids), repr(got_ids))
check("match_scores_positive", all(r[1] is not None and r[1] > 0 for r in got), repr(got))
cols, got = db.query(
    "SELECT id FROM docs WHERE MATCH(body, 'zebra')")
check("match_nohit_empty", len(got) == 0, repr(got))
# 变异可见性: DELETE 命中文档后重查
db.execute("DELETE FROM docs WHERE id IN (1, 2)")
cols, got = db.query("SELECT id, BM25_SCORE() FROM docs WHERE MATCH(body, 'quick') ORDER BY id")
check("match_after_delete", [r[0] for r in got] == [5], repr(got))

# ───────────────────────── 空间 ─────────────────────────
db.execute("CREATE TABLE poi (id INT PRIMARY KEY, loc GEOMETRY)")
pts = {}
spatial_rows = []
for i in range(1, 121):
    x = rng.uniform(0, 100)
    y = rng.uniform(0, 100)
    pts[i] = (x, y)
    spatial_rows.append([i, {"type": "Point", "x": x, "y": y}])
db.executemany("INSERT INTO poi VALUES (?, ?)", spatial_rows)
x0, y0, x1, y1 = 20.0, 30.0, 60.0, 70.0
cols, got = db.query(
    "SELECT id FROM poi WHERE ST_WITHIN(loc, %.6f, %.6f, %.6f, %.6f) ORDER BY id ASC"
    % (x0, y0, x1, y1))
want = sorted(i for i, (x, y) in pts.items() if x0 <= x <= x1 and y0 <= y <= y1)
check("st_within_envelope", [r[0] for r in got] == want,
      "got %d want %d" % (len(got), len(want)))
qx, qy = 50.0, 50.0
cols, got = db.query(
    "SELECT id FROM poi ORDER BY loc <-> ST_POINT(?, ?) ASC LIMIT 5", [qx, qy])
dist = {i: math.hypot(x - qx, y - qy) for i, (x, y) in pts.items()}
want = [i for i, _ in sorted(dist.items(), key=lambda t: (t[1], t[0]))[:5]]
check("spatial_knn_top5", [r[0] for r in got] == want,
      "got %r want %r" % ([r[0] for r in got], want))

# ───────────────────────── 事务回滚 ─────────────────────────
db.execute("CREATE TABLE tx (id INT PRIMARY KEY, v INT)")
db.executemany("INSERT INTO tx VALUES (?, ?)", [[i, i] for i in range(1, 21)])
tx = db.begin()
db.execute("INSERT INTO tx VALUES (100, 100)", None)
db.execute("UPDATE tx SET v = -1 WHERE id <= 5", None)
db.rollback(tx)
cols, got = db.query("SELECT COUNT(*), SUM(v), MIN(v) FROM tx")
check("rollback_discards", got[0] == (20, 210, 1), repr(got))
tx2 = db.begin()
db.execute("INSERT INTO tx VALUES (101, 101)", None)
db.commit(tx2)
cols, got = db.query("SELECT COUNT(*) FROM tx")
check("commit_persists", got[0][0] == 21, repr(got))

db.close()
shutil.rmtree(tmp, ignore_errors=True)
print("tier2 seed=%d: %s" % (SEED, "ALL OK" if not fails else "FAILURES: %s" % fails))
sys.exit(1 if fails else 0)
