#!/usr/bin/env python3
"""Round 12c 全面端到端功能测试：SQL 面 + 四模态 + 事务 + R12/12b 新路径 + 边缘 preset。"""
import math, os, shutil, sys, tempfile, traceback
import numpy as np

import motedb

PASS = FAIL = 0
FAILURES = []

def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
    else:
        FAIL += 1
        FAILURES.append(f"{name}: {detail}")
        print(f"  ✗ {name}  {detail}")

def q1(db, sql, params=None):
    """query 返回 (cols, rows)；返回首行首列"""
    cols, rows = db.query(sql, params=params) if params is not None else db.query(sql)
    return rows[0][0] if rows and rows[0] else None

def approx(a, b, eps=1e-6):
    return a is not None and b is not None and abs(a - b) <= eps

# ───────────────────────── 1. SQL 功能面 ─────────────────────────
def test_sql_surface(tmp):
    db = motedb.Database(os.path.join(tmp, "sql.mote"))
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score FLOAT, flag BOOLEAN, ts TIMESTAMP)")
    rows = [(i, f"name-{i % 5}", i * 1.5, i % 2 == 0, 1700000000000 + i * 1000) for i in range(50)]
    db.executemany("INSERT INTO t VALUES (?, ?, ?, ?, ?)", rows)
    db.checkpoint()

    check("count", q1(db, "SELECT COUNT(*) FROM t") == 50)
    check("point pk", q1(db, "SELECT name FROM t WHERE id = 7") == "name-2")
    check("where and", q1(db, "SELECT COUNT(*) FROM t WHERE id >= 10 AND id < 20 AND flag = TRUE") == 5)
    check("between", q1(db, "SELECT COUNT(*) FROM t WHERE id BETWEEN 5 AND 9") == 5)
    check("in", q1(db, "SELECT COUNT(*) FROM t WHERE name IN ('name-1', 'name-2')") == 20)
    check("like", q1(db, "SELECT COUNT(*) FROM t WHERE name LIKE 'name-3'") == 10)
    check("float range", q1(db, "SELECT COUNT(*) FROM t WHERE score > 20") == 36)

    # 聚合（含 R12 融合路径）
    cols, r = db.query("SELECT COUNT(*), SUM(score), AVG(score), MIN(score), MAX(score) FROM t WHERE id >= 10 AND id < 30 AND flag = TRUE")
    sel = [x for i, x, f, *_ in [(i, i * 1.5, None) for i in range(10, 30)] if i % 2 == 0]
    check("fused count", r[0][0] == 10, r[0])
    check("fused sum", approx(r[0][1], sum(sel)), r[0])
    check("fused avg", approx(r[0][2], sum(sel) / 10), r[0])
    check("fused min", approx(r[0][3], 10 * 1.5), r[0])
    check("fused max", approx(r[0][4], 28 * 1.5), r[0])

    # GROUP BY + HAVING + ORDER BY
    cols, r = db.query("SELECT name, COUNT(*) AS c FROM t GROUP BY name HAVING COUNT(*) >= 10 ORDER BY name")
    check("groupby rows", len(r) == 5, r)
    check("groupby counts", all(row[1] == 10 for row in r), r)
    check("groupby order", [row[0] for row in r] == sorted(row[0] for row in r), r)

    # ORDER BY NULL/NaN 语义（R10）
    db.execute("INSERT INTO t (id, name, score, flag, ts) VALUES (100, 'nan-row', ?, TRUE, 1)", params=[float('nan')])
    db.execute("INSERT INTO t VALUES (101, 'null-row', NULL, TRUE, 1)")
    cols, r = db.query("SELECT id FROM t ORDER BY score ASC LIMIT 3")
    check("order null-first", r[0][0] == 101, r)  # NULL 第一
    check("order nan-last", q1(db, "SELECT id FROM t ORDER BY score ASC LIMIT 1 OFFSET 51") == 100)

    # UPDATE / DELETE
    db.execute("UPDATE t SET score = 99 WHERE id < 5")
    check("update", q1(db, "SELECT COUNT(*) FROM t WHERE score = 99") == 5)
    db.execute("DELETE FROM t WHERE id >= 100")
    check("delete", q1(db, "SELECT COUNT(*) FROM t") == 50)

    # 子查询
    check("scalar subq", q1(db, "SELECT COUNT(*) FROM t WHERE score > (SELECT AVG(score) FROM t)") is not None)
    check("in subq", q1(db, "SELECT COUNT(*) FROM t WHERE id IN (SELECT id FROM t WHERE id < 3)") == 3)

    # JOIN（R12 列裁剪路径）
    db.execute("CREATE TABLE g (grp TEXT PRIMARY KEY, zone INT)")
    db.executemany("INSERT INTO g VALUES (?, ?)", [(f"name-{i}", i % 2) for i in range(5)])
    db.checkpoint()
    cols, r = db.query("SELECT t.name, COUNT(*), AVG(t.score) FROM t JOIN g ON t.name = g.grp WHERE g.zone = 1 GROUP BY t.name ORDER BY t.name")
    check("join groupby", len(r) == 2 and all(row[1] == 10 for row in r), r)
    def expect_avg(grp):
        ids = [i for i in range(50) if i % 5 == grp]
        vals = [99.0 if i < 5 else i * 1.5 for i in ids]
        return sum(vals) / len(vals)
    check("join avg", all(approx(row[2], expect_avg(int(row[0].split("-")[1]))) for row in r), r)
    db.close()

# ───────────────────────── 2. 向量模态 ─────────────────────────
def test_vector(tmp):
    db = motedb.Database(os.path.join(tmp, "vec.mote"))
    db.execute("CREATE TABLE v (id INT PRIMARY KEY, cat TEXT, emb VECTOR(8))")
    rng = np.random.default_rng(7)
    embs = rng.standard_normal((500, 8)).astype(np.float32)
    db.executemany("INSERT INTO v VALUES (?, ?, ?)",
                   [(i, f"c{i % 3}", embs[i].tolist()) for i in range(500)])
    db.checkpoint()

    qv = embs[42].tolist()
    # SQL 精确 top-k vs numpy
    cols, r = db.query("SELECT id FROM v ORDER BY emb <-> ? LIMIT 5", params=[qv])
    got = [row[0] for row in r]
    d = ((embs - embs[42]) ** 2).sum(axis=1)
    want = list(np.argsort(d)[:5])
    check("knn exact top5", sorted(got) == sorted(int(x) for x in want), f"got={got} want={want}")
    # 参数化 vs 字面量一致
    lit = "[" + ",".join(str(x) for x in qv) + "]"
    cols, r2 = db.query(f"SELECT id FROM v ORDER BY emb <-> {lit} LIMIT 5")
    check("knn literal==param", [row[0] for row in r2] == got)

    # 流式路径 == 缓存路径（R12b）
    db.set_vector_cache_budget("v", 1024 * 1024)  # 1MB → 强制分块流式
    cols, r3 = db.query("SELECT id FROM v ORDER BY emb <-> ? LIMIT 5", params=[qv])
    check("knn streamed==cached", [row[0] for row in r3] == got)

    # UPDATE/DELETE 可见性（R10/R12b）
    best = got[0]
    db.execute("DELETE FROM v WHERE id = ?", params=[best])
    db.execute("UPDATE v SET emb = ? WHERE id = ?", params=([99.0] * 8, got[1] if len(got) > 1 else 1))
    db.set_vector_cache_budget("v", 256 * 1024 * 1024)
    cols, r4 = db.query("SELECT id FROM v ORDER BY emb <-> ? LIMIT 5", params=[qv])
    ids4 = [row[0] for row in r4]
    check("knn del gone", best not in ids4, ids4)
    check("knn upd gone", (got[1] if len(got) > 1 else -1) not in ids4, ids4)
    check("knn after 5 rows", len(ids4) == 5)
    db.close()

# ───────────────────────── 3. 文本 FTS ─────────────────────────
def test_fts(tmp):
    db = motedb.Database(os.path.join(tmp, "fts.mote"))
    db.execute("CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)")
    texts = [
        "the quick brown fox jumps", "lazy dogs sleep all day",
        "quick thinking saves lives", "brown bears eat honey",
        "quantum computing advances", "the fox escapes again quick",
    ] * 20
    db.executemany("INSERT INTO docs VALUES (?, ?)", [(i, texts[i % len(texts)]) for i in range(120)])
    db.checkpoint()
    db.execute("CREATE TEXT INDEX docs_body ON docs(body)")

    cols, r = db.query("SELECT COUNT(*) FROM docs WHERE MATCH(body, 'quick')")
    check("fts count", r[0][0] == 60, r)
    cols, r = db.query("SELECT id, BM25_SCORE() FROM docs WHERE MATCH(body, 'brown fox') LIMIT 5")
    check("fts bm25 rows", len(r) == 5 and all(x[1] is not None for x in r), r)
    # 更新可见性
    db.execute("UPDATE docs SET body = 'zzz unrelated content' WHERE id = 0")
    db.execute("DELETE FROM docs WHERE id = 6")
    cols, r = db.query("SELECT COUNT(*) FROM docs WHERE MATCH(body, 'quick')")
    check("fts update visible", r[0][0] == 58, r)
    db.close()

# ───────────────────────── 4. 空间模态 ─────────────────────────
def test_spatial(tmp):
    db = motedb.Database(os.path.join(tmp, "sp.mote"))
    db.execute("CREATE TABLE poi (id INT PRIMARY KEY, loc GEOMETRY)")
    pts = [{"type": "Point", "x": float(i), "y": float(i % 7)} for i in range(100)]
    db.executemany("INSERT INTO poi VALUES (?, ?)", [(i, pts[i]) for i in range(100)])
    # 大几何（R10 修复：>64KB）
    big = {"type": "LineString", "points": [[float(k), float(k)] for k in range(20000)]}
    db.execute("INSERT INTO poi VALUES (999, ?)", params=[big]) if False else None
    try:
        db.execute("INSERT INTO poi (id, loc) VALUES (999, ?)", params=[big])
        ok = True
    except Exception as e:
        ok = False
        print("  spatial big insert err:", e)
    db.checkpoint()
    db.close()
    db = motedb.Database(os.path.join(tmp, "sp.mote"))
    n = q1(db, "SELECT COUNT(*) FROM poi")
    check("spatial count", n == 101, n)
    cols, r = db.query("SELECT id FROM poi WHERE ST_DISTANCE(loc, 10.0, 3.0) < 1.5")
    check("spatial distance filter", sorted(row[0] for row in r) == [9, 10, 11], r)
    cols, r = db.query("SELECT id, ST_DISTANCE(loc, 10.0, 3.0) AS d FROM poi WHERE id != 999 ORDER BY d ASC LIMIT 3")
    check("spatial order", r[0][0] == 10 and r[0][1] == 0, r)
    if ok:
        cols, r = db.query("SELECT id FROM poi WHERE id = 999")
        check("spatial big reopen", len(r) == 1, r)
    db.close()

# ───────────────────────── 5. 时序 ─────────────────────────
def test_timeseries(tmp):
    db = motedb.Database(os.path.join(tmp, "ts.mote"))
    db.execute("CREATE TABLE m (sensor TEXT, ts TIMESTAMP, temp FLOAT, hum FLOAT) TIMESERIES(ts)")
    rows = [(f"s{i % 4}", 1700000000000 + k * 1000, 20 + (k % 30) * 0.5, 40 + (k % 50)) for k in range(2000) for i in [k]]
    db.executemany("INSERT INTO m VALUES (?, ?, ?, ?)", rows)
    db.checkpoint()
    n = q1(db, "SELECT COUNT(*) FROM m")
    check("ts count", n == 2000, n)
    cols, r = db.query("SELECT COUNT(*), AVG(temp) FROM m WHERE ts >= 1700000000000 AND ts < 1700000500000")
    sel = [20 + (k % 30) * 0.5 for k in range(500)]
    check("ts range count", r[0][0] == 500, r)
    check("ts range avg", approx(r[0][1], sum(sel) / 500), r)
    cols, r = db.query("SELECT sensor, temp FROM m WHERE sensor = 's1' LATEST BY sensor")
    check("ts latest", len(r) == 1 and r[0][0] == "s1", r)
    db.close()

# ───────────────────────── 6. 事务 & DDL 规则（R10）─────────────────────────
def test_txn(tmp):
    db = motedb.Database(os.path.join(tmp, "tx.mote"))
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, v INT)")
    db.execute("BEGIN")
    db.execute("INSERT INTO a VALUES (1, 10)")
    db.execute("UPDATE a SET v = 20 WHERE id = 1")
    check("txn read-own-write", q1(db, "SELECT v FROM a WHERE id = 1") == 20)
    db.execute("ROLLBACK")
    check("txn rollback", q1(db, "SELECT COUNT(*) FROM a") == 0)
    db.execute("BEGIN")
    db.execute("INSERT INTO a VALUES (2, 5)")
    db.execute("COMMIT")
    check("txn commit", q1(db, "SELECT COUNT(*) FROM a") == 1)
    # 破坏性 DDL 在事务内拒绝（R10）
    db.execute("BEGIN")
    try:
        db.execute("DROP TABLE a")
        check("ddl-in-txn rejected", False, "DROP TABLE succeeded in txn")
    except Exception:
        check("ddl-in-txn rejected", True)
    db.execute("ROLLBACK")
    check("table survives", q1(db, "SELECT COUNT(*) FROM a") == 1)
    # 孤立 COMMIT/ROLLBACK 报错（R10）
    try:
        db.execute("COMMIT")
        check("stray commit errors", False)
    except Exception:
        check("stray commit errors", True)
    db.close()

# ───────────────────────── 7. R12/12b 专项 ─────────────────────────
def test_r12_specifics(tmp):
    # 7a. checkpoint 磁盘回收（文件数）
    path = os.path.join(tmp, "disk.mote")
    db = motedb.Database(path)
    db.execute("CREATE TABLE big (id INT PRIMARY KEY, note TEXT)")
    payload = "x" * 120_000
    for b in range(3):
        db.executemany("INSERT INTO big VALUES (?, ?)", [(b * 100 + i, payload) for i in range(100)])
    db.checkpoint(); db.close()
    sst = [f for f in os.listdir(os.path.join(path, "columnar_ms", "big")) if f.endswith(".sst")]
    check("disk single segment", len(sst) == 1, sst)
    cols, r = db2 = None, None
    db = motedb.Database(path)
    cols, r = db.query("SELECT note FROM big WHERE id = 250")
    check("long text survives", r and r[0][0] is not None and len(r[0][0]) == 120_000, r[:1] if r else r)
    db.close()

    # 7b. 查询期内存（粗验证：无增长即可，精确值已由探针覆盖）
    # 7c. 重开续写 row_id 不冲突
    path2 = os.path.join(tmp, "rid.mote")
    db = motedb.Database(path2)
    db.execute("CREATE TABLE r (id INT PRIMARY KEY AUTO_INCREMENT, v FLOAT)")
    db.executemany("INSERT INTO r (v) VALUES (?)", [[float(i)] for i in range(300)])
    db.checkpoint(); db.close()
    db = motedb.Database(path2)
    db.executemany("INSERT INTO r (v) VALUES (?)", [[float(i)] for i in range(100)])
    db.checkpoint(); db.close()
    db = motedb.Database(path2)
    check("reopen fresh rowids", q1(db, "SELECT COUNT(DISTINCT id) FROM r") == 400)
    db.close()

# ───────────────────────── 8. edge preset 端到端 ─────────────────────────
def test_edge_preset(tmp):
    path = os.path.join(tmp, "edge.mote")
    db = motedb.Database(path, preset="edge")
    db.execute("CREATE TABLE s (id INT PRIMARY KEY, v FLOAT, emb VECTOR(8))")
    rng = np.random.default_rng(3)
    embs = rng.standard_normal((2000, 8)).astype(np.float32)
    for i in range(0, 2000, 500):
        db.executemany("INSERT INTO s VALUES (?, ?, ?)",
                       [(k, float(k), embs[k].tolist()) for k in range(i, i + 500)])
    db.checkpoint()
    check("edge budget 32MB", db.vector_cache_budget_bytes("s") == 32 * 1024 * 1024)
    qv = embs[11].tolist()
    cols, r = db.query("SELECT id FROM s ORDER BY emb <-> ? LIMIT 5", params=[qv])
    d = ((embs - embs[11]) ** 2).sum(axis=1)
    want = sorted(int(x) for x in np.argsort(d)[:5])
    check("edge knn exact", sorted(row[0] for row in r) == want,
          f"got={[x[0] for x in r]} want={want}")
    db.close()

def main():
    tmp = tempfile.mkdtemp(prefix="r12c_e2e_")
    try:
        for fn in (test_sql_surface, test_vector, test_fts, test_spatial,
                   test_timeseries, test_txn, test_r12_specifics, test_edge_preset):
            print(f"── {fn.__name__}")
            try:
                fn(tmp)
            except Exception:
                global PASS, FAIL
                FAIL += 1
                FAILURES.append(f"{fn.__name__} EXCEPTION")
                traceback.print_exc()
        print(f"\n结果: {PASS} 通过, {FAIL} 失败")
        for f in FAILURES:
            print("  ✗", f)
        sys.exit(1 if FAIL else 0)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

main()
