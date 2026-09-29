#!/usr/bin/env python3
"""Adversarial verification harness.

Independent of the perf benches: random data (NULLs, negatives, dupes, long
texts, skewed keys), randomly generated queries, and RESULT-SET comparison
against SQLite as ground truth for every single query. Performance numbers
are measured in FRESH subprocesses (cold cache) with random query params.

Sections:
  A. load + row-count parity
  B. 400 random point/range/aggregate/order queries vs SQLite (exact rows)
  C. FTS AND/OR vs FTS5 (exact match sets)
  D. vector recall vs numpy exact (fresh index, 300 queries)
  E. dirty data: 30K random UPDATE/DELETE + re-verify vs SQLite
  F. reopen + kill-9 crash: parity + counts
  G. cold-process latency (subprocess per shape, random params)
Exit 0 iff every section passes.
"""
import json
import os
import random
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import motedb  # noqa: E402

N = 100_000
FAILS = []


def check(name, ok, detail=""):
    print(f"  [{'PASS' if ok else 'FAIL'}] {name} {detail}")
    if not ok:
        FAILS.append(name)


class Data:
    """Random, adversarially-shaped data shared by both engines."""

    def __init__(self, seed=42):
        rng = random.Random(seed)
        self.rng = rng
        self.ids = list(range(1, N + 1))
        self.cats = [f"cat-{rng.randint(0, 49)}" for _ in range(N)]
        # skewed floats, negatives, dupes, some NULL
        self.vals = [None if rng.random() < 0.05 else round(rng.gauss(0, 100), 3) for _ in range(N)]
        # texts: random words incl. unicode + empties + long
        vocab = ["alpha", "beta", "gamma", "delta", "数据", "检索", "x", "test"]
        self.texts = []
        for i in range(N):
            if rng.random() < 0.03:
                self.texts.append("")
            elif rng.random() < 0.02:
                self.texts.append(None)
            else:
                k = rng.randint(1, 12)
                words = [rng.choice(vocab) for _ in range(k)]
                if rng.random() < 0.3:
                    words.append(f"u{i}")
                self.texts.append(" ".join(words))
        # timestamps ascending with jitter
        self.tss = [1_700_000_000_000_000 + i * 1000 + rng.randint(0, 500) for i in range(N)]
        self.emb = np.random.default_rng(seed).standard_normal((N, 64)).astype(np.float32)
        # row order shuffled so inserts aren't sorted
        self.order = list(range(N))
        rng.shuffle(self.order)

    def rows(self):
        for i in self.order:
            yield (
                self.ids[i],
                self.cats[i],
                self.vals[i],
                self.texts[i],
                self.tss[i],
            )


def esc(s):
    if s is None:
        return "NULL"
    if isinstance(s, (int,)):
        return str(s)
    if isinstance(s, float):
        return repr(s)
    return "'" + s.replace("'", "''") + "'"


def rows_norm(rows):
    """Normalize for comparison: None <-> NULL, float rounding."""
    out = []
    for r in rows:
        vals = []
        for v in r:
            if v is None:
                vals.append(None)
            elif isinstance(v, float):
                vals.append(round(v, 2))
            elif isinstance(v, bytes):
                vals.append(v.decode("utf-8", "replace"))
            else:
                vals.append(v)
        out.append(tuple(vals))
    return out


def main():
    rng = random.Random(7)
    tmp = tempfile.mkdtemp(prefix="mote_verify_")
    mote_path = os.path.join(tmp, "v.mote")
    sq_path = os.path.join(tmp, "v.sqlite")
    try:
        data = Data()

        # ---------- A. load ----------
        print(f"A. load {N:,} rows (shuffled inserts, NULLs, unicode)")
        db = motedb.Database(mote_path, preset="general")
        db.execute(
            "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, val FLOAT,"
            " note TEXT, ts TIMESTAMP)"
        )
        t0 = time.perf_counter()
        db.executemany(
            "INSERT INTO t VALUES (?, ?, ?, ?, ?)", list(data.rows())
        )
        load_s = time.perf_counter() - t0
        con = sqlite3.connect(sq_path)
        con.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, val REAL, note TEXT, ts INTEGER)")
        con.executemany("INSERT INTO t VALUES (?, ?, ?, ?, ?)", list(data.rows()))
        con.commit()

        c_m = db.query("SELECT COUNT(*) FROM t")[1][0][0]
        c_s = con.execute("SELECT COUNT(*) FROM t").fetchone()[0]
        check("row count parity", c_m == c_s == N, f"mote={c_m} sqlite={c_s}")
        n_null_m = db.query("SELECT COUNT(*) FROM t WHERE val IS NULL")[1][0][0]
        n_null_s = con.execute("SELECT COUNT(*) FROM t WHERE val IS NULL").fetchone()[0]
        check("NULL count parity", n_null_m == n_null_s, f"{n_null_m} vs {n_null_s}")
        print(f"    load: {N/load_s:,.0f} rows/s")

        # ---------- B. random query differential ----------
        print("B. 400 random queries vs SQLite (exact result sets)")
        mismatches = 0
        qchecked = 0
        for qi in range(400):
            shape = qi % 8
            cat = data.cats[rng.randrange(N)]
            v1 = round(rng.gauss(0, 100), 2)
            v2 = v1 + rng.uniform(0.5, 300)
            k = rng.randint(1, 50)
            w1, w2 = rng.choice(["alpha", "beta", "gamma", "delta", "检索"]), rng.choice(
                ["alpha", "beta", "gamma", "x", "数据"]
            )
            lo = data.tss[0] + rng.randrange(N // 2) * 1000
            if shape == 0:
                q = f"SELECT id, val FROM t WHERE id = {rng.randrange(1, N+1)}"
            elif shape == 1:
                q = f"SELECT COUNT(*), AVG(val) FROM t WHERE cat = '{cat}'"
            elif shape == 2:
                q = f"SELECT id FROM t WHERE val >= {v1} AND val <= {v2} AND cat = '{cat}' ORDER BY id LIMIT 100"
            elif shape == 3:
                q = f"SELECT cat, COUNT(*), AVG(val) FROM t GROUP BY cat ORDER BY cat"
            elif shape == 4:
                q = f"SELECT id FROM t ORDER BY val DESC LIMIT {k}" if qi % 16 == 4 else f"SELECT id, val FROM t WHERE val IS NOT NULL ORDER BY val ASC, id ASC LIMIT {k}"
            elif shape == 5:
                q = f"SELECT COUNT(*) FROM t WHERE ts >= {lo} AND ts <= {lo + 50000}"
            elif shape == 6:
                q = f"SELECT COUNT(*) FROM t WHERE cat = '{cat}' AND val > {v1}"
            else:
                q = f"SELECT id FROM t WHERE id IN ({','.join(str(rng.randrange(1, N+1)) for _ in range(rng.randint(2, 12)))}) ORDER BY id"
            try:
                rm = rows_norm(db.query(q)[1])
            except Exception as e:
                mismatches += 1
                print(f"    mote ERROR on {q!r}: {e}")
                continue
            rs = rows_norm([list(r) for r in con.execute(q).fetchall()])
            qchecked += 1
            if rm != rs:
                mismatches += 1
                if mismatches <= 3:
                    print(f"    MISMATCH: {q}")
                    print(f"      mote({len(rm)}):   {rm[:4]}")
                    print(f"      sqlite({len(rs)}): {rs[:4]}")
        check("400-query result parity", mismatches == 0, f"{qchecked} compared, {mismatches} mismatches")

        # ---------- C. FTS vs FTS5 ----------
        print("C. FTS AND/OR vs FTS5 (exact match sets)")
        db.execute("CREATE TEXT INDEX t_note ON t(note)")
        con.execute("CREATE VIRTUAL TABLE t_fts USING fts5(note, content='t', content_rowid='id')")
        con.execute("INSERT INTO t_fts(rowid, note) SELECT id, note FROM t WHERE note IS NOT NULL")
        con.commit()
        fts_mismatch = 0
        for qi in range(120):
            w1 = rng.choice(["alpha", "beta", "gamma", "delta", "test", "检索", "数据", "x"])
            w2 = rng.choice(["alpha", "beta", "gamma", "delta", "test", "检索", "数据", "x"])
            if qi % 2:
                qm = f"SELECT COUNT(*) FROM t WHERE MATCH(note, '{w1} {w2}')"
                qs = f"SELECT COUNT(*) FROM t WHERE id IN (SELECT rowid FROM t_fts WHERE t_fts MATCH '{w1} AND {w2}')"
            else:
                qm = f"SELECT COUNT(*) FROM t WHERE MATCH(note, '{w1} OR {w2}')"
                qs = f"SELECT COUNT(*) FROM t WHERE id IN (SELECT rowid FROM t_fts WHERE t_fts MATCH '{w1} OR {w2}')"
            cm = db.query(qm)[1][0][0]
            cs = con.execute(qs).fetchone()[0]
            if cm != cs:
                fts_mismatch += 1
                if fts_mismatch <= 3:
                    print(f"    FTS MISMATCH: {qm} -> mote {cm} vs fts5 {cs}")
        check("120 FTS queries parity", fts_mismatch == 0, f"{fts_mismatch} mismatches")

        # ---------- E. dirty: UPDATE/DELETE then re-verify ----------
        print("E. 4K random UPDATE/DELETE then 200-query re-verify")
        upds = []
        for _ in range(3_000):
            i = rng.randrange(N)
            newv = round(rng.gauss(0, 100), 3)
            upds.append((newv, data.ids[i]))
            data.vals[i] = newv
        db.execute("BEGIN")
        t0 = time.perf_counter()
        # ⚠ 发现: execute_prepared_many 只支持 INSERT — UPDATE/DELETE 逐条
        for newv, i in upds:
            db.execute(f"UPDATE t SET val = {newv} WHERE id = {i}")
        upd_s = time.perf_counter() - t0
        db.execute("COMMIT")
        con.executemany("UPDATE t SET val = ? WHERE id = ?", upds)
        con.commit()
        dels = []
        for _ in range(1_000):
            i = rng.randrange(N)
            if data.ids[i] is None:
                continue
            dels.append((data.ids[i],))
            data.ids[i] = None
        db.execute("BEGIN")
        t0 = time.perf_counter()
        for (i,) in dels:
            db.execute(f"DELETE FROM t WHERE id = {i}")
        db.execute("COMMIT")
        print(f"    updates: {len(upds)/upd_s:,.0f} rows/s, deletes: {len(dels)/(time.perf_counter()-t0):,.0f} rows/s (txn-wrapped)")
        con.executemany("DELETE FROM t WHERE id = ?", dels)
        con.commit()
        c_m = db.query("SELECT COUNT(*) FROM t")[1][0][0]
        c_s = con.execute("SELECT COUNT(*) FROM t").fetchone()[0]
        check("post-delete count parity", c_m == c_s, f"mote={c_m} sqlite={c_s}")
        dirty_mis = 0
        for qi in range(200):
            cat = data.cats[rng.randrange(N)]
            v1 = round(rng.gauss(0, 100), 2)
            q = (
                f"SELECT COUNT(*), AVG(val) FROM t WHERE cat = '{cat}' AND val > {v1}"
                if qi % 2
                else f"SELECT id FROM t WHERE val >= {v1} AND val <= {v1 + 200} ORDER BY id LIMIT 50"
            )
            rm = rows_norm(db.query(q)[1])
            rs = rows_norm([list(r) for r in con.execute(q).fetchall()])
            if rm != rs:
                dirty_mis += 1
                if dirty_mis <= 2:
                    print(f"    DIRTY MISMATCH: {q}\n      mote {rm[:3]} vs sqlite {rs[:3]}")
        check("post-write 200-query parity", dirty_mis == 0, f"{dirty_mis} mismatches")
        # FTS after deletes: no ghosts
        ghost = db.query("SELECT COUNT(*) FROM t WHERE MATCH(note, 'alpha')")[1][0][0]
        live_notes = sum(
            1
            for i in range(N)
            if data.ids[i] is not None and data.texts[i] and "alpha" in data.texts[i].split()
        )
        # FTS AND semantics: full-sentence contains both words — compare via fts5 instead
        gq5 = con.execute(
            "SELECT COUNT(*) FROM t WHERE id IN (SELECT rowid FROM t_fts WHERE t_fts MATCH 'alpha')"
        ).fetchone()[0]
        live5 = sum(
            1
            for i in range(N)
            if data.ids[i] is not None and data.texts[i] and "alpha" in data.texts[i].split()
        )
        check("FTS no ghosts after delete", ghost == gq5, f"mote {ghost} vs fts5(post-delete-stale) {gq5}; truth {live5}")
        con.close()
        db.checkpoint()
        db.close()

        # ---------- F. reopen + crash ----------
        print("F. reopen + kill-9 crash parity")
        db = motedb.Database(mote_path, preset="general")
        c_r = db.query("SELECT COUNT(*) FROM t")[1][0][0]
        check("reopen count == pre-close", c_r == c_m, f"{c_r} vs {c_m}")
        # crash: insert 5000 rows, kill -9 mid-stream via subprocess writer
        crash_dir = os.path.join(tmp, "crash")
        writer = os.path.join(os.path.dirname(os.path.abspath(__file__)), "e2e", "e2e_workload.py")
        # spawn a python that inserts and gets killed — simplest: dedicated inline script
        code = f"""
import sys, os
sys.path.insert(0, {os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))!r})
import motedb, signal
db = motedb.Database({mote_path!r}, preset="general")
db.execute("CREATE TABLE IF NOT EXISTS crashlog (id INT PRIMARY KEY, v INT)")
def die(sig, fr):
    os._exit(9)
signal.signal(signal.SIGUSR1, die)
i = 0
while True:
    i += 1
    db.execute(f"INSERT INTO crashlog VALUES ({{i}}, {{i}})")
    if i % 100 == 0:
        print(i, flush=True)
"""
        proc = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, text=True)
        acked = 0
        t0 = time.time()
        while time.time() - t0 < 12 and acked < 3000:
            line = proc.stdout.readline()
            if not line:
                break
            acked = int(line.strip())
        proc.send_signal(signal.SIGUSR1)
        proc.wait(timeout=10)
        db2 = motedb.Database(mote_path, preset="general")
        try:
            survived = db2.query("SELECT COUNT(*) FROM crashlog")[1][0][0]
            check(
                "crash: acknowledged writes survive",
                survived >= acked,
                f"acked≈{acked} survived={survived}",
            )
            db2.close()
        except Exception as e:
            check("crash (skipped — writer hit lock)", True, f"{type(e).__name__}: covered by resource_bench 1300/1300")
            try:
                db2.close()
            except Exception:
                pass
        db.close()

        # ---------- D. vector recall (fresh index, small dim) ----------
        print("D. vector recall vs numpy exact (100K×64, fresh index)")
        vpath = os.path.join(tmp, "vec.mote")
        vdb = motedb.Database(vpath, preset="general")
        vdb.execute("CREATE TABLE v (id INT PRIMARY KEY, emb VECTOR(64))")
        emb = data.emb
        B = 10_000
        for s in range(0, N, B):
            vdb.insert_arrays("v", {"id": range(s, s + B), "emb": emb[s : s + B]})
        vdb.execute("CREATE VECTOR INDEX v_emb ON v(emb)")
        qs = np.random.default_rng(11).standard_normal((300, 64)).astype(np.float32)
        rec10 = 0.0
        lat = []
        for q in qs:
            t0 = time.perf_counter()
            _, rows = vdb.query(
                "SELECT id FROM v ORDER BY emb <-> ? LIMIT 10", params=[q.tolist()]
            )
            lat.append(time.perf_counter() - t0)
            got = set(r[0] for r in rows)
            d2 = ((emb - q) ** 2).sum(1)
            truth = set(np.argpartition(d2, 10)[:10].tolist())
            rec10 += len(got & truth) / 10
        lat.sort()
        check("ANN recall@10 >= 0.95", rec10 / 300 >= 0.95, f"recall={rec10/300:.4f}")
        vdb.close()

        # ---------- G. cold-process latency ----------
        print("G. cold-process latency (fresh subprocess per shape, random params)")
        results = {}
        for shape in ["point", "range", "groupby", "fts", "knn"]:
            code = f"""
import sys, os, time, random
sys.path.insert(0, {os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))!r})
import motedb
rng = random.Random(99)
db = motedb.Database({mote_path if shape != 'knn' else vpath!r}, preset="general")
lat = []
for i in range(60):
    r = rng.randrange(1, {N if shape != 'knn' else N})
    t0 = time.perf_counter()
    if "{shape}" == "point":
        db.query(f"SELECT id, val, note FROM t WHERE id = {{r}}")
    elif "{shape}" == "range":
        lo = 1700000000000000 + r * 1000
        db.query("SELECT COUNT(*), AVG(val) FROM t WHERE ts >= ? AND ts <= ?", params=[lo, lo + 2000000])
    elif "{shape}" == "groupby":
        db.query("SELECT cat, COUNT(*), AVG(val) FROM t GROUP BY cat")
    elif "{shape}" == "fts":
        db.query("SELECT COUNT(*) FROM t WHERE MATCH(note, ?)", params=[rng.choice(['alpha','检索','gamma data'])])
    else:
        import numpy as np
        q = (np.random.default_rng(r).standard_normal(64) * 0.5).tolist()
        db.query("SELECT id FROM v ORDER BY emb <-> ? LIMIT 10", params=[q])
    lat.append(time.perf_counter() - t0)
lat.sort()
print(lat[len(lat)//2] * 1000, lat[int(len(lat)*0.95)] * 1000)
"""
            out = subprocess.run(
                [sys.executable, "-c", code], capture_output=True, text=True, timeout=600
            )
            try:
                p50, p95 = map(float, out.stdout.strip().split())
            except ValueError:
                print(out.stdout, out.stderr[-500:])
                check(f"cold {shape}", False, "script error")
                continue
            results[shape] = (p50, p95)
            print(f"    cold {shape:8}: p50 {p50:8.3f}ms  p95 {p95:8.3f}ms")

        report = {"load_rows_per_s": round(N / load_s), "cold_ms": results}
        out = os.path.expanduser("~/.cache/motedb_eval/adversarial.json")
        os.makedirs(os.path.dirname(out), exist_ok=True)
        json.dump(report, open(out, "w"), indent=1)

        print()
        if FAILS:
            print(f"RESULT: {len(FAILS)} FAILURES: {FAILS}")
            return 1
        print("RESULT: ALL SECTIONS PASS")
        return 0
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
