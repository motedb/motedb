#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Round 13 差分对拍回归 (SQLite oracle × MoteDB) — 固定 seed 快速版.

完整 campaign harness 见 bench 目录的历史; 本文件是入库的回归门槛:
- 三路对拍: SQLite vs MoteDB-LSM(未 checkpoint) vs MoteDB-列存(checkpoint 后)
- 随机变异 (UPDATE/DELETE/INSERT) 后复跑 — 抓过期索引/快照类 bug
- 同状态跨相位自洽 (SELF_DIVERGE) — 抓快速路径与列存路径静默分歧

用法: python3 test_fuzz_differential.py [n_queries_per_seed]
"""
import math
import os
import random
import shutil
import sqlite3
import sys
import tempfile

import motedb

NQ = int(sys.argv[1]) if len(sys.argv) > 1 else 250
NROWS = 400
SEEDS = [1, 2, 3, 4]

failures = []


def run_seed(SEED):
    rng = random.Random(SEED)
    tmp = tempfile.mkdtemp(prefix="mote_fuzzreg_%d_" % SEED)
    findings = []

    def note(kind, phase, sql, detail):
        findings.append((kind, phase, sql, detail))

    CATS = ["alpha", "beta", "gamma", "delta", "", "哈哈", None]
    TAGS = ["red", "green", "blue", "", None]

    def gen_items(n):
        rows = []
        for i in range(1, n + 1):
            rows.append((
                i, rng.choice(CATS), rng.randint(0, 6),
                None if rng.random() < 0.12 else round(rng.uniform(-50, 50), 4),
                None if rng.random() < 0.08 else rng.randint(-10, 100),
                None if rng.random() < 0.15 else "note-%d" % rng.randint(0, 20),
            ))
        return rows

    def gen_tags(items):
        rows = []
        tid = 1
        for r in items:
            for _ in range(rng.randint(0, 3)):
                rows.append((tid, None if rng.random() < 0.05 else r[0],
                             rng.choice(TAGS),
                             None if rng.random() < 0.10 else round(rng.uniform(0, 10), 3)))
                tid += 1
        return rows

    sdb = sqlite3.connect(":memory:")
    mdb = motedb.Database(os.path.join(tmp, "fuzz.mote"))
    for d in ("CREATE TABLE items (id INT PRIMARY KEY, cat TEXT, grp INT, val REAL, qty INT, note TEXT)",
              "CREATE TABLE tags (id INT PRIMARY KEY, item_id INT, tag TEXT, score REAL)",
              "CREATE TABLE zones (id INT PRIMARY KEY, name TEXT, code INT)"):
        mdb.execute(d)
        sdb.execute(d.replace("INT PRIMARY KEY", "INTEGER PRIMARY KEY"))

    ITEMS = gen_items(NROWS)
    TAGS = gen_tags(ITEMS)
    for tbl, rows in (("items", ITEMS), ("tags", TAGS), ("zones", [(i, "zone%d" % i, i) for i in range(8)])):
        ph = ",".join("?" * len(rows[0]))
        mdb.executemany("INSERT INTO %s VALUES (%s)" % (tbl, ph), [list(r) for r in rows])
        sdb.executemany("INSERT INTO %s VALUES (%s)" % (tbl, ph), rows)
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

    def skey(rows):
        def k(r):
            return tuple((v is None, isinstance(v, str), "" if v is None else v) for v in r)
        try:
            return sorted(rows, key=k)
        except TypeError:
            return sorted(map(str, rows))

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

    TEXTS = ["alpha", "beta", "gamma", "delta", "", "哈哈"]
    NUM_COLS, FLT_COLS, TXT_COLS = ["grp", "qty", "id"], ["val", "qty", "id"], ["cat", "note"]
    INTS, FLOATS = [-5, 0, 1, 3, 7, 100], [-50.0, -0.5, 0.0, 3.14, 27.5, 49.9]

    def pred(depth=0):
        r = rng.random()
        if depth < 2 and r < 0.30:
            return "(%s)%s(%s)" % (pred(depth + 1), rng.choice([" AND ", " OR "]), pred(depth + 1))
        if depth < 2 and r < 0.34:
            return "NOT (%s)" % pred(depth + 1)
        kind = rng.randint(0, 7)
        if kind == 0:
            return "%s %s %d" % (rng.choice(NUM_COLS), rng.choice(["=", "<>", "!=", "<", ">", "<=", ">="]), rng.choice(INTS))
        if kind == 1:
            return "%s %s %s" % (rng.choice(FLT_COLS), rng.choice(["=", "<", ">", "<=", ">="]), rng.choice(FLOATS))
        if kind == 2:
            return "%s LIKE %s" % (rng.choice(TXT_COLS), rng.choice(["'a%'", "'%a%'", "'note-1%'", "'%哈哈%'", "''", "'z%'"]))
        if kind == 3:
            vals = ", ".join(str(rng.choice(INTS)) for _ in range(rng.randint(1, 3)))
            if rng.random() < 0.3:
                vals += ", NULL"
            return "%s %sIN (%s)" % (rng.choice(NUM_COLS), rng.choice(["", "NOT "]), vals)
        if kind == 4:
            a, b = rng.choice(INTS), rng.choice(INTS) + 2
            return "%s %sBETWEEN %d AND %d" % (rng.choice(NUM_COLS), rng.choice(["", "NOT "]), min(a, b), max(a, b))
        if kind == 5:
            return "%s IS %sNULL" % (rng.choice(["cat", "note", "val", "qty", "grp"]), rng.choice(["", "NOT "]))
        if kind == 6:
            return "%s %s NULL" % (rng.choice(NUM_COLS), rng.choice(["=", "<>", ">", "<"]))
        return "ABS(COALESCE(val, 0)) %s %s" % (rng.choice(["<", ">", "<=", ">="]), rng.choice(FLOATS))

    AGGS = ["COUNT(*)", "COUNT(val)", "COUNT(cat)", "SUM(qty)", "SUM(val)", "AVG(val)",
            "MIN(val)", "MAX(val)", "MIN(cat)", "MAX(qty)", "COUNT(DISTINCT cat)"]

    def q_filter_order():
        cols = rng.sample(["id", "cat", "grp", "val", "qty", "note"], rng.randint(1, 4))
        sql = "SELECT %s FROM items WHERE %s" % (", ".join(cols), pred())
        if rng.random() < 0.8:
            k, d = rng.choice(["val", "qty", "grp", "cat", "note"]), rng.choice(["ASC", "DESC"])
            sql += " ORDER BY %s %s, id %s" % (k, d, d)
            if rng.random() < 0.5:
                sql += " LIMIT %d" % rng.randint(0, 30)
                if rng.random() < 0.4:
                    sql += " OFFSET %d" % rng.randint(0, 10)
            return sql, True
        return sql, False

    def q_distinct():
        cols = rng.sample(["cat", "grp", "note", "qty"], rng.randint(1, 3))
        return ("SELECT DISTINCT %s FROM items WHERE qty %s %d ORDER BY %s"
                % (", ".join(cols), rng.choice(["<", ">", "="]), rng.choice(INTS),
                   ", ".join("%s ASC" % c for c in cols))), True

    def q_agg_flat():
        a = rng.sample(AGGS, rng.randint(1, 3))
        return "SELECT %s FROM items WHERE %s" % (", ".join(a), pred()), False

    def q_group():
        g = rng.choice([["grp"], ["cat"], ["grp", "cat"], ["qty"]])
        a = rng.sample(AGGS, rng.randint(1, 3))
        sql = "SELECT %s, %s FROM items" % (", ".join(g), ", ".join(a))
        if rng.random() < 0.6:
            sql += " WHERE %s" % pred()
        sql += " GROUP BY %s" % ", ".join(g)
        if rng.random() < 0.4:
            sql += " HAVING COUNT(*) %s %d" % (rng.choice([">", ">=", "<"]), rng.randint(0, 30))
        return sql + " ORDER BY %s" % ", ".join("%s ASC" % c for c in g), True

    def q_expr():
        e = rng.choice(["UPPER(cat)", "LOWER(cat)", "LENGTH(note)", "ABS(qty)",
                        "ROUND(val, 1)", "COALESCE(note, cat, 'none')", "COALESCE(qty, 0) + 1",
                        "qty * 2 - 1", "val / 4", "SUBSTR(cat, 1, 2)", "TRIM(cat)"])
        return "SELECT id, %s AS e FROM items WHERE id %% 3 = %d ORDER BY id ASC" % (e, rng.randint(0, 2)), True

    def q_join_inner():
        proj = rng.sample(["i.id", "i.cat", "i.val", "t.tag", "t.score"], rng.randint(2, 4))
        extra = " AND i.grp %s %d" % (rng.choice(["=", "<", ">"]), rng.choice(INTS)) if rng.random() < 0.5 else ""
        sql = "SELECT %s FROM items i JOIN tags t ON i.id = t.item_id%s" % (", ".join(proj), extra)
        if rng.random() < 0.4:
            sql += " WHERE t.score %s" % rng.choice(["IS NULL", "IS NOT NULL", "> 5", "< 5"])
        if rng.random() < 0.5:
            return sql + " ORDER BY i.id ASC, t.id ASC LIMIT %d" % rng.randint(0, 40), True
        return sql, False

    def q_join_group():
        return ("SELECT i.grp, COUNT(*), COUNT(t.score), SUM(t.score) "
                "FROM items i JOIN tags t ON i.id = t.item_id "
                "WHERE i.qty %s GROUP BY i.grp ORDER BY i.grp ASC"
                ) % rng.choice(["IS NOT NULL", "> 0", "< 50", "<> 10", "= 5"]), True

    def q_join_left():
        return ("SELECT i.id, t.tag, t.score FROM items i LEFT JOIN tags t "
                "ON i.id = t.item_id AND t.score > %s WHERE i.grp %% 2 = %d "
                "ORDER BY i.id ASC, t.id ASC LIMIT %d"
                ) % (rng.choice(FLOATS), rng.randint(0, 1), rng.randint(0, 50)), True

    def q_join_three():
        return ("SELECT z.name, COUNT(i.id), AVG(i.val) FROM zones z "
                "JOIN items i ON i.grp = z.code JOIN tags t ON t.item_id = i.id "
                "WHERE i.id <= %d GROUP BY z.name ORDER BY z.name ASC" % rng.randint(50, NROWS + 50)), True

    def q_in_sub():
        return ("SELECT id, cat FROM items WHERE id IN "
                "(SELECT item_id FROM tags WHERE tag = %s) ORDER BY id ASC"
                % repr(rng.choice(["red", "green", "blue"]))), True

    def q_scalar_sub():
        return ("SELECT id, val FROM items WHERE val > "
                "(SELECT AVG(val) FROM items) ORDER BY id ASC LIMIT 20"), True

    GENS = [q_filter_order, q_filter_order, q_filter_order, q_distinct, q_agg_flat,
            q_group, q_group, q_expr, q_join_inner, q_join_inner, q_join_group,
            q_join_left, q_join_three, q_in_sub, q_scalar_sub]

    QUERIES = []
    for _ in range(NQ):
        try:
            QUERIES.append(rng.choice(GENS)())
        except Exception:
            pass

    BASELINE = {}

    def run_phase(phase, qset, self_compare=False):
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
                continue
            a, b = (rs, rm) if ordered else (skey(rs), skey(rm))
            d = cmp_rows(a, b)
            if d:
                note("WRONG", phase, sql, d)
        if self_compare:
            for sql, ordered in qset:
                try:
                    now = norm_rows(mdb.query(sql)[1])
                except Exception:
                    BASELINE[sql] = None
                    continue
                old = BASELINE.get(sql)
                BASELINE[sql] = now
                if old is None:
                    continue
                d = cmp_rows(skey(old), skey(now))
                if d:
                    note("SELF_DIVERGE", phase, sql, d)

    run_phase("P1_lsm", QUERIES)
    BASELINE.clear()
    mdb.checkpoint()
    run_phase("P2_col", QUERIES, self_compare=True)

    def sqlstr(v):
        return "NULL" if v is None else (repr(v) if isinstance(v, str) else str(v))

    for step in range(2):
        grp = rng.randint(0, 6)
        muts = [
            "UPDATE items SET val = val * 1.5 WHERE grp = %d" % grp,
            "UPDATE items SET note = NULL WHERE id %% 9 = %d" % rng.randint(0, 8),
            "UPDATE tags SET item_id = %d WHERE id BETWEEN %d AND %d" % (rng.randint(1, NROWS), rng.randint(1, 50), rng.randint(51, 120)),
            "DELETE FROM tags WHERE tag = %s" % repr(rng.choice(["red", "green"])),
            "DELETE FROM items WHERE id %% 23 = %d AND grp = %d" % (rng.randint(0, 22), grp),
            "INSERT INTO items VALUES (%d, %s, %d, %s, %s, %s)" % (
                NROWS + step + 1, sqlstr(rng.choice(CATS)), rng.randint(0, 6),
                sqlstr(None if rng.random() < 0.3 else round(rng.uniform(-9, 9), 3)),
                sqlstr(rng.randint(-3, 9)), sqlstr("note-new%d" % step)),
        ]
        for m in rng.sample(muts, 3):
            mdb.execute(m)
            sdb.execute(m)
        sdb.commit()
        run_phase("P3_mut%d" % step, QUERIES[: len(QUERIES) // 2])
        BASELINE.clear()
        mdb.checkpoint()
        run_phase("P4_cp%d" % step, QUERIES[: len(QUERIES) // 2], self_compare=True)

    mdb.close()
    shutil.rmtree(tmp, ignore_errors=True)
    return findings


for seed in SEEDS:
    fs = run_seed(seed)
    for kind, phase, sql, detail in fs:
        print("FAIL seed=%d [%s|%s] %s\n  %s" % (seed, kind, phase, sql.replace("\n", " "), detail))
        failures.append((seed, kind))
    print("seed %d: %s" % (seed, "OK" if not fs else "%d findings" % len(fs)))

print("结果: %d 通过, %d 失败" % (len(SEEDS) - len({s for s, _ in failures}), len(failures)))
sys.exit(1 if failures else 0)
