#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Round 13b: 大表差分回归 (默认 8K 行, 有界查询) + 重开一致性.

与 test_fuzz_differential.py 的差异:
- 全部查询要么服务端聚合 (GROUP BY / COUNT / AVG — 返回少量行),
  要么严格 LIMIT — 大表的 JOIN 绝不无界投影过 Python 边界。
- 数据分 3 批插入、每批 checkpoint — 强制 ColSegmentStore 多段
  (multi-segment) 合并路径, 弥补 400 行 harness 只打单段的缺口。
- 末尾 REOPEN 相位: close → 重新 open → 全查询复跑, 对照关闭前结果
  (持久化层一致性 oracle)。

用法: python3 test_fuzz_bigtable.py [seed] [n_items] [n_queries]
"""
import math
import os
import random
import shutil
import sqlite3
import sys
import tempfile

import motedb

SEED = int(sys.argv[1]) if len(sys.argv) > 1 else 1
NQUERIES = int(sys.argv[3]) if len(sys.argv) > 3 else 60
NITEMS = int(sys.argv[2]) if len(sys.argv) > 2 else 8000

rng = random.Random(SEED)
tmp = tempfile.mkdtemp(prefix="mote_big_%d_" % SEED)
findings = []


def note(kind, phase, sql, detail):
    findings.append((kind, phase, sql, detail))
    print("[%s|%s] %s\n    %s" % (kind, phase, sql.replace("\n", " "), detail))


CATS = ["alpha", "beta", "gamma", "delta", "", "哈哈", None]
TAGS = ["red", "green", "blue", "", None]

sdb = sqlite3.connect(":memory:")
mdb = motedb.Database(os.path.join(tmp, "big.mote"))
for d in ("CREATE TABLE items (id INT PRIMARY KEY, cat TEXT, grp INT, val REAL, qty INT, note TEXT)",
          "CREATE TABLE tags (id INT PRIMARY KEY, item_id INT, tag TEXT, score REAL)",
          "CREATE TABLE zones (id INT PRIMARY KEY, name TEXT, code INT)"):
    mdb.execute(d)
    sdb.execute(d.replace("INT PRIMARY KEY", "INTEGER PRIMARY KEY"))

# ── 分 3 批插入 + 每批 checkpoint → 多段列存 ──
BATCH = NITEMS // 3
tid = 1
for b in range(3):
    lo, hi = b * BATCH + 1, (b + 1) * BATCH if b < 2 else NITEMS
    items = []
    tags = []
    for i in range(lo, hi + 1):
        items.append((
            i, rng.choice(CATS), rng.randint(0, 6),
            None if rng.random() < 0.12 else round(rng.uniform(-50, 50), 4),
            None if rng.random() < 0.08 else rng.randint(-10, 100),
            None if rng.random() < 0.15 else "note-%d" % rng.randint(0, 20),
        ))
        for _ in range(rng.randint(0, 3)):
            tags.append((tid, None if rng.random() < 0.05 else i,
                         rng.choice(TAGS),
                         None if rng.random() < 0.10 else round(rng.uniform(0, 10), 3)))
            tid += 1
    mdb.executemany("INSERT INTO items VALUES (?, ?, ?, ?, ?, ?)", [list(r) for r in items])
    mdb.executemany("INSERT INTO tags VALUES (?, ?, ?, ?)", [list(r) for r in tags])
    sdb.executemany("INSERT INTO items VALUES (?, ?, ?, ?, ?, ?)", items)
    sdb.executemany("INSERT INTO tags VALUES (?, ?, ?, ?)", tags)
    sdb.commit()
    mdb.checkpoint()
print("loaded %d items, %d tags (3 batches + checkpoints)" % (NITEMS, tid - 1))
for z in range(8):
    mdb.execute("INSERT INTO zones VALUES (%d, 'zone%d', %d)" % (z, z, z))
    sdb.execute("INSERT INTO zones VALUES (%d, 'zone%d', %d)" % (z, z, z))
sdb.commit()


def norm(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, float):
        return "NaN" if math.isnan(v) else round(v, 6)
    return v


def norm_rows(rows):
    return [tuple(norm(v) for v in r) for r in rows]


def feq(a, b):
    if isinstance(a, float) and isinstance(b, float) and a != b:
        return abs(a - b) <= 2e-6 * max(1.0, abs(a), abs(b))
    return a == b


def cmp_rows(a, b):
    if len(a) != len(b):
        return "rowcount %d vs %d" % (len(a), len(b))
    for i, (ra, rb) in enumerate(zip(a, b)):
        if len(ra) != len(rb):
            return "row %d width" % i
        for va, vb in zip(ra, rb):
            if not feq(va, vb):
                return "row %d: %r vs %r" % (i, ra, rb)
    return None


INTS = [-5, 0, 1, 3, 7, 100]


# ── 有界查询生成器: 聚合(服务端) 或 LIMIT ≤ 50 ──
def g_agg_group():
    g = rng.choice([["grp"], ["cat"], ["grp", "cat"]])
    a = rng.sample(["COUNT(*)", "COUNT(val)", "SUM(qty)", "AVG(val)", "MIN(val)", "MAX(val)"],
                   rng.randint(1, 3))
    where = rng.choice(["", " WHERE qty > 50", " WHERE cat IS NOT NULL", " WHERE val < 0",
                        " WHERE qty NOT IN (7)", " WHERE grp <> 3"])
    return ("SELECT %s, %s FROM items%s GROUP BY %s ORDER BY %s"
            % (", ".join(g), ", ".join(a), where, ", ".join(g),
               ", ".join("%s ASC" % c for c in g))), True


def g_agg_flat():
    a = rng.sample(["COUNT(*)", "COUNT(val)", "SUM(val)", "AVG(val)", "MIN(cat)", "MAX(qty)"],
                   rng.randint(1, 3))
    where = rng.choice(["grp > 4", "qty BETWEEN 20 AND 80", "cat IS NULL", "val IS NOT NULL",
                        "id % 7 = 3", "note LIKE 'note-1%'"])
    return "SELECT %s FROM items WHERE %s" % (", ".join(a), where), False


def g_range_limit():
    cols = rng.sample(["id", "cat", "grp", "val", "qty", "note"], rng.randint(1, 4))
    k = rng.choice(["val", "qty", "grp", "cat", "note"])
    d = rng.choice(["ASC", "DESC"])
    return ("SELECT %s FROM items WHERE id %% 97 = %d ORDER BY %s %s, id %s LIMIT 40"
            % (", ".join(cols), rng.randint(0, 96), k, d, d)), True


def g_point_in():
    vals = ", ".join(str(rng.choice(INTS)) for _ in range(rng.randint(1, 3)))
    return ("SELECT id, val FROM items WHERE qty IN (%s) ORDER BY id ASC LIMIT 50" % vals), True


def g_in_sub():
    return ("SELECT id, cat FROM items WHERE id IN "
            "(SELECT item_id FROM tags WHERE tag = %s) ORDER BY id ASC LIMIT 50"
            % repr(rng.choice(["red", "green", "blue"]))), True


def g_join_group():  # 服务端聚合: 返回 ≤ 7 行
    return ("SELECT i.grp, COUNT(*), COUNT(t.score), AVG(t.score) "
            "FROM items i JOIN tags t ON i.id = t.item_id "
            "WHERE i.id <= %d AND i.qty %s GROUP BY i.grp ORDER BY i.grp ASC"
            ) % (rng.randint(2000, NITEMS), rng.choice(["> 0", "IS NOT NULL", "< 50"])), True


def g_three_group():  # 服务端聚合: 返回 ≤ 8 行
    return ("SELECT z.name, COUNT(i.id), AVG(i.val) FROM zones z "
            "JOIN items i ON i.grp = z.code AND i.id <= %d "
            "JOIN tags t ON t.item_id = i.id "
            "GROUP BY z.name ORDER BY z.name ASC" % rng.randint(2000, NITEMS)), True


def g_scalar_sub():
    return ("SELECT COUNT(*), MAX(val) FROM items WHERE val > "
            "(SELECT AVG(val) FROM items)"), False


def g_distinct():
    return ("SELECT DISTINCT grp, cat FROM items WHERE id %% 31 = %d ORDER BY grp ASC, cat ASC"
            % rng.randint(0, 30)), True


def g_left_join_limit():
    lo = rng.randint(1, NITEMS - 300)
    return ("SELECT i.id, t.tag FROM items i LEFT JOIN tags t ON i.id = t.item_id "
            "AND t.score > 5 WHERE i.id BETWEEN %d AND %d "
            "ORDER BY i.id ASC, t.id ASC LIMIT 50" % (lo, lo + 200)), True


GENS = [g_agg_group, g_agg_group, g_agg_flat, g_range_limit, g_point_in, g_in_sub,
        g_join_group, g_three_group, g_scalar_sub, g_distinct, g_left_join_limit]

QUERIES = []
for _ in range(NQUERIES):
    try:
        QUERIES.append(rng.choice(GENS)())
    except Exception:
        pass


def run_phase(phase, qset):
    bad = 0
    for sql, ordered in qset:
        try:
            rs = norm_rows(sdb.execute(sql).fetchall())
        except Exception as e:
            note("HARNESS", phase, sql, "sqlite errored: %s" % e)
            continue
        try:
            _, raw = mdb.query(sql)
            rm = norm_rows(raw)
        except Exception as e:
            note("MOTE_ERROR", phase, sql, repr(e))
            bad += 1
            continue
        d = cmp_rows(rs, rm)
        if d:
            note("WRONG", phase, sql, d + " | sqlite=%r mote=%r" % (rs[:2], rm[:2]))
            bad += 1
    return bad


# ── P1: 多段列存 (3 checkpoint 之后) ──
run_phase("P1_multiseg", QUERIES)

# ── P2: 变异 + 再 checkpoint ──
mut_sqls = [
    "UPDATE items SET val = val * 1.5 WHERE grp = %d AND id %% 297 = 3" % rng.randint(0, 6),
    "UPDATE tags SET item_id = %d WHERE id %% 977 = 5" % rng.randint(1, NITEMS),
    "DELETE FROM tags WHERE id % 13 = 4 AND tag = 'red'",
    "DELETE FROM items WHERE id % 457 = 11 AND grp = 5",
]
for m in mut_sqls:
    mdb.execute(m)
    sdb.execute(m)
sdb.commit()
mdb.checkpoint()
run_phase("P2_mut", QUERIES)

# ── P3: 追加写入 (memtable + 多段合并) ──
extra = []
extra_tags = []
nid = NITEMS
ntid = tid
for j in range(500):
    nid += 1
    extra.append([nid, rng.choice(CATS), rng.randint(0, 6),
                  None if rng.random() < 0.2 else round(rng.uniform(-9, 9), 3),
                  rng.randint(-3, 9), "late-%d" % j])
    if rng.random() < 0.5:
        extra_tags.append([ntid, nid, rng.choice(TAGS), round(rng.uniform(0, 9), 2)])
        ntid += 1
mdb.executemany("INSERT INTO items VALUES (?, ?, ?, ?, ?, ?)", extra)
mdb.executemany("INSERT INTO tags VALUES (?, ?, ?, ?)", extra_tags)
sdb.executemany("INSERT INTO items VALUES (?, ?, ?, ?, ?, ?)", [tuple(r) for r in extra])
sdb.executemany("INSERT INTO tags VALUES (?, ?, ?, ?)", [tuple(r) for r in extra_tags])
sdb.commit()
run_phase("P3_append", QUERIES)

# ── P4: 重开一致性 — close → reopen → 复跑, 对照 P3 与 SQLite ──
before = {}
for sql, _ in QUERIES:
    try:
        before[sql] = norm_rows(mdb.query(sql)[1])
    except Exception:
        before[sql] = None
path = os.path.join(tmp, "big.mote")
mdb.close()
mdb2 = motedb.Database(path)
diverge = 0
for sql, ordered in QUERIES:
    try:
        after = norm_rows(mdb2.query(sql)[1])
    except Exception as e:
        note("REOPEN_ERROR", "P4", sql, repr(e))
        diverge += 1
        continue
    if before[sql] is None:
        continue
    d = cmp_rows(before[sql], after)
    if d:
        note("REOPEN_DIVERGE", "P4", sql, d)
        diverge += 1
mdb2.close()

kinds = {}
for f in findings:
    kinds[f[0]] = kinds.get(f[0], 0) + 1
print("SUMMARY seed=%d items=%d queries=%d findings=%s reopen_diverge=%d"
      % (SEED, NITEMS, len(QUERIES), kinds, diverge))
shutil.rmtree(tmp, ignore_errors=True)
sys.exit(1 if findings else 0)
