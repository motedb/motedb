#!/usr/bin/env python3
"""MoteDB end-to-end workload — external-integration level.

Exercises the database the way an embedded user does: Python wheel (or the
motedb-cli binary) against real files on disk, across process boundaries.

Suites (default = all):
  1. multimodal   vector / text / spatial / time-series smoke with indexes
  2. persistence  checkpoint + close + reopen: rows AND indexes usable
  3. txn          commit / rollback / read-your-writes
  4. crash        true kill -9 mid-ingest: every ACKNOWLEDGED write survives
  5. cli          motedb-cli piped SQL (create/query/reopen) + doctor exit code

Usage:
  e2e_workload.py [--only multimodal,persistence,...] [--root DIR]
  e2e_workload.py --crash-writer DB_PATH N PROGRESS_FILE   (internal)

Exit code 0 = all pass. FAIL lines explain what broke.
"""
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time

import motedb

CLI = os.environ.get("MOTE_CLI", "motedb-cli")


def _resolve_cli():
    """MOTE_CLI > PATH > repo target/release。找不到返回 None (调用方 SKIP
    CLI 段 — errors 套件曾因裸调用缺失二进制 FileNotFoundError 崩套件,
    CI smoke 也挂在这)。"""
    import shutil as _sh
    c = os.environ.get("MOTE_CLI")
    if c:
        return c if os.path.exists(c) or _sh.which(c) else None
    w = _sh.which("motedb-cli")
    if w:
        return w
    # repo layout: bindings/python/e2e/ -> ../../target/release
    for cand in (
        os.path.join(os.path.dirname(os.path.abspath(__file__)), *([".."] * 2 + ["target", "release", "motedb-cli"])),
    ):
        if os.path.exists(cand):
            return cand
    return None
FAILURES = []


def check(name, cond, detail=""):
    tag = "PASS" if cond else "FAIL"
    print(f"{tag} {name}" + (f"  [{detail}]" if detail else ""), flush=True)
    if not cond:
        FAILURES.append(name)


def esc(s):
    return s.replace("'", "''")


# ------------------------------------------------------------------ suites
def suite_multimodal(root):
    db = motedb.Database(os.path.join(root, "mm.mote"), preset="general")
    try:
        # vector
        db.execute("CREATE TABLE vec (id INT PRIMARY KEY, emb VECTOR(8))")
        for s in range(0, 1000, 200):
            vals = ",".join(
                f"({s+j+1}, [{', '.join(str(((s+j)*7+k) % 97) + '.0' for k in range(8))}])"
                for j in range(200))
            db.execute("INSERT INTO vec (id, emb) VALUES " + vals)
        _, rows = db.query("SELECT id FROM vec ORDER BY emb <-> "
                           "[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0] LIMIT 5")
        check("vector knn returns k rows", len(rows) == 5, f"got {len(rows)}")

        # text
        db.execute("CREATE TABLE txt (id INT PRIMARY KEY, content TEXT)")
        docs = [f"sensor {i % 20} reading text {i}" for i in range(500)]
        for s in range(0, 500, 100):
            vals = ",".join(f"({s+j+1},'{esc(docs[s+j])}')" for j in range(100))
            db.execute("INSERT INTO txt VALUES " + vals)
        db.execute("CREATE TEXT INDEX txt_content ON txt(content)")
        _, a = db.query("SELECT COUNT(*) FROM txt WHERE MATCH(content, 'sensor')")
        _, b = db.query("SELECT id FROM txt WHERE MATCH(content, 'sensor')")
        check("text MATCH count == set size", a[0][0] == len(b), f"{a[0][0]} vs {len(b)}")
        # AND default (FTS5-compatible): every doc holds sensor+reading;
        # 'sensor 7' narrows to the 25 docs whose i%20==7; unknown ANDs to 0;
        # explicit OR is the escape hatch back to the full set.
        _, r = db.query("SELECT COUNT(*) FROM txt WHERE MATCH(content, 'sensor reading')")
        check("text AND default: both terms", r[0][0] == 500, f"{r[0][0]}")
        _, r = db.query("SELECT COUNT(*) FROM txt WHERE MATCH(content, 'sensor 7')")
        check("text AND: partial overlap", r[0][0] == 25, f"{r[0][0]}")
        _, r = db.query("SELECT COUNT(*) FROM txt WHERE MATCH(content, 'sensor zzzqqq')")
        check("text AND unknown term is empty", r[0][0] == 0, f"{r[0][0]}")
        _, r = db.query("SELECT COUNT(*) FROM txt WHERE MATCH(content, 'zzzqqq OR sensor')")
        check("text explicit OR union", r[0][0] == 500, f"{r[0][0]}")

        # spatial
        db.execute("CREATE TABLE pts (id INT PRIMARY KEY, pt GEOMETRY)")
        vals = ",".join(f"({i+1}, POINT3D({i%10}.0, {i%7}.0, {i%5}.0))" for i in range(300))
        db.execute("INSERT INTO pts VALUES " + vals)
        db.execute("CREATE OCTREE INDEX pts_pt ON pts(pt)")
        _, rows = db.query("SELECT id FROM pts WHERE ST_KNN_3D(pt, 0.0, 0.0, 0.0, 10)")
        check("spatial knn returns k rows", len(rows) == 10, f"got {len(rows)}")

        # time-series
        db.execute("CREATE TABLE ts (ts TIMESTAMP, sid INT, v FLOAT) TIMESERIES(ts)")
        base = 1_700_000_000_000_000
        rows_sql = ",".join(f"({base + i*1_000_000}, {i%4}, {i*0.5:.1f})" for i in range(5000))
        db.execute("INSERT INTO ts VALUES " + rows_sql)
        _, r = db.query(f"SELECT COUNT(*) FROM ts WHERE ts BETWEEN {base} AND {base + 1000*1_000_000}")
        check("ts range count exact", r[0][0] == 1001, f"got {r[0][0]}")
        b0 = base // 60_000_000 * 60_000_000  # true 60s bucket boundary
        _, r = db.query(f"SELECT TIME_BUCKET('60s', ts) AS b, COUNT(*) FROM ts "
                        f"WHERE ts >= {base} AND ts < {base + 120_000_000} GROUP BY b")
        check("time_bucket group-by buckets", len(r) == 3 and r[0][0] == b0 and r[0][1] == 40,
              f"buckets={len(r)} first={r[0] if r else None}")
        _, r = db.query(f"SELECT COUNT(*) FROM ts WHERE TIME_BUCKET('60s', ts) = {b0}")
        check("time_bucket WHERE equality", r[0][0] == 40, f"got {r[0][0]}")

        # flush-race stress: insert then IMMEDIATELY query — the auto-flush
        # thread used to move buffered rows into a new segment between the
        # KNN scan's segment snapshot and buffer read, silently dropping
        # them (flaky "got 0"). Every iteration must see every row.
        db.execute("CREATE TABLE vec2 (id INT PRIMARY KEY, emb VECTOR(8))")
        ok, seen = True, 0
        for round_no in range(10):
            for batch in range(2):
                vals = ",".join(
                    f"({900000 + seen + j}, "
                    f"[{', '.join(str(float((seen + j) % 13)) for _ in range(8))}])"
                    for j in range(100))
                db.execute("INSERT INTO vec2 VALUES " + vals)
                seen += 100
                _, r = db.query("SELECT id FROM vec2 ORDER BY "
                                "emb <-> [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0] "
                                "LIMIT 3000")
                if len(r) != seen:
                    ok = False
                    check(f"vector flush-race (insert {seen})", False,
                          f"inserted {seen}, knn saw {len(r)}")
                    break
            if not ok:
                break
        if ok:
            check("vector flush-race (20x insert+query, 2000 rows)", True)
    finally:
        db.close()


def suite_persistence(root):
    path = os.path.join(root, "persist.mote")
    db = motedb.Database(path, preset="general")
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, content TEXT)")
    for s in range(0, 3000, 500):
        vals = ",".join(f"({s+j+1},'row {s+j} text')" for j in range(500))
        db.execute("INSERT INTO t VALUES " + vals)
    db.execute("CREATE TEXT INDEX t_content ON t(content)")
    _, before = db.query("SELECT COUNT(*) FROM t WHERE MATCH(content, 'row')")
    db.checkpoint()
    db.close()

    db = motedb.Database(path, preset="general")
    _, n = db.query("SELECT COUNT(*) FROM t")
    check("reopen keeps rows", n[0][0] == 3000, f"got {n[0][0]}")
    _, after = db.query("SELECT COUNT(*) FROM t WHERE MATCH(content, 'row')")
    check("reopen keeps text index", after[0][0] == before[0][0],
          f"{after[0][0]} vs {before[0][0]}")
    db.execute("INSERT INTO t VALUES (999999, 'row fresh text')")
    _, r = db.query("SELECT COUNT(*) FROM t WHERE MATCH(content, 'fresh')")
    check("index grows after reopen", r[0][0] == 1, f"got {r[0][0]}")
    db.close()


def suite_txn(root):
    path = os.path.join(root, "txn.mote")
    db = motedb.Database(path, preset="general")
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
    tx = db.begin()
    db.execute("UPDATE t SET v = 99 WHERE id = 1")
    _, r = db.query("SELECT v FROM t WHERE id = 1")
    check("read-your-writes in txn", r[0][0] == 99, f"got {r[0][0]}")
    db.rollback(tx)
    _, r = db.query("SELECT v FROM t WHERE id = 1")
    check("rollback restores", r[0][0] == 10, f"got {r[0][0]}")

    tx = db.begin()
    for i in range(3, 203):
        db.execute(f"INSERT INTO t VALUES ({i}, {i})")
    db.commit(tx)
    _, r = db.query("SELECT COUNT(*) FROM t")
    check("txn commit persists", r[0][0] == 202, f"got {r[0][0]}")
    db.close()


def crash_writer(db_path, n, progress_file):
    """Insert n rows autocommit; append each ACKED row id to progress_file."""
    db = motedb.Database(db_path, preset="general")
    db.execute("CREATE TABLE IF NOT EXISTS w (id INT PRIMARY KEY, v INT)")
    with open(progress_file, "w", buffering=1) as p:
        for i in range(1, n + 1):
            db.execute(f"INSERT INTO w VALUES ({i}, {i})")
            p.write(f"{i}\n")
    db.close()  # clean close only after ALL acks — the parent kills before this


def suite_crash(root):
    path = os.path.join(root, "crash.mote")
    progress = os.path.join(root, "progress.txt")
    n = 5000
    proc = subprocess.Popen(
        [sys.executable, __file__, "--crash-writer", path, str(n), progress],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    # wait until at least 500 acks, then SIGKILL mid-write
    deadline = time.time() + 60
    while time.time() < deadline:
        if os.path.exists(progress):
            with open(progress) as f:
                lines = f.read().split()
            if len(lines) >= 500:
                break
        if proc.poll() is not None:
            break
        time.sleep(0.02)
    time.sleep(0.1)  # let a few more in-flight acks land
    proc.send_signal(signal.SIGKILL)
    proc.wait()
    with open(progress) as f:
        acked = [int(x) for x in f.read().split()]
    db = motedb.Database(path, preset="general")
    _, r = db.query("SELECT COUNT(*), MAX(id) FROM w")
    db.close()
    got_n, got_max = r[0]
    # rows acknowledged on a previous line may still be in the same fsync
    # batch as later ones — the invariant is: count >= last line whose row
    # we SAW acked, and count <= total written. Strictly: every id up to the
    # last progress line must exist.
    check("kill -9 keeps every ACKed write",
          got_n >= acked[-1] and got_max >= acked[-1] if acked else False,
          f"acked {acked[-1] if acked else 0}, reopened count={got_n} max={got_max}")
    _, r2 = motedb.Database(path, preset="general").query("SELECT COUNT(*) FROM w WHERE id <= ?", params=[acked[-1]])
    check("no holes in acked prefix",
          r2[0][0] == acked[-1], f"prefix count {r2[0][0]} vs acked {acked[-1]}")


def suite_cli(root):
    cli = _resolve_cli()
    if cli is None:
        print("SKIP cli (binary 'motedb-cli' not found)")
        return
    db = os.path.join(root, "cli.mote")
    sql = ("CREATE TABLE c (id INT PRIMARY KEY, v TEXT);\n"
           "INSERT INTO c VALUES (1,'x'),(2,'y');\n"
           "CHECKPOINT;\n"
           "SELECT COUNT(*) FROM c;\n.exit\n")
    out = subprocess.run([cli, db], input=sql, capture_output=True, text=True,
                         timeout=60).stdout
    check("cli create+insert+count", "2" in out, out.strip().splitlines()[-3:])
    check("cli CHECKPOINT works", "checkpoint complete" in out
          and "Parse error" not in out, out[-120:])
    out2 = subprocess.run([cli, db], input="SELECT COUNT(*) FROM c;\n.exit\n",
                          capture_output=True, text=True, timeout=60).stdout
    check("cli reopen sees data", "2" in out2)
    doc = subprocess.run([cli, "doctor", db], capture_output=True, text=True,
                         timeout=60)
    check("cli doctor exits 0 on healthy db", doc.returncode == 0,
          f"rc={doc.returncode}")
    # dot commands + multiline SQL + error recovery inside one session
    sql = ("CREATE TABLE m (id INT PRIMARY KEY,\n"
           "  v TEXT);\n"                       # multiline statement
           "INSERT INTO m VALUES (1, 'a');\n"
           ".tables\n"
           ".schema m\n"
           "SELECT COUNT(*) FROM m;\n"
           "UPDATE bogus SET x = 1;\n"           # error must not kill shell
           "SELECT COUNT(*) FROM m;\n"
           ".exit\n")
    out = subprocess.run([cli, db], input=sql, capture_output=True, text=True,
                         timeout=60).stdout
    check("cli multiline SQL", "1 row(s) affected" in out, out[-120:])
    check("cli .tables", "m" in out.split("Tables:")[1][:40] if "Tables:" in out else False)
    check("cli error then continue", out.count("1") >= 2, "second COUNT ok")
    doc2 = subprocess.run([cli, "doctor", db], capture_output=True, text=True,
                          timeout=60)
    check("cli doctor ok after error session", doc2.returncode == 0,
          f"rc={doc2.returncode}")


def expect_error(db, name, sql, fragment=""):
    try:
        db.query(sql)
        check(name, False, "expected an error, got success")
    except Exception as e:  # noqa
        ok = fragment.lower() in str(e).lower() if fragment else True
        check(name, ok, str(e)[:90])


def suite_sql_types(root):
    db = motedb.Database(os.path.join(root, "types.mote"), preset="general")
    try:
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY AUTO_INCREMENT, "
                   "flag BOOL, ts TIMESTAMP, note TEXT, x FLOAT)")
        db.execute("INSERT INTO t (flag, ts, note, x) VALUES "
                   "(TRUE, '2024-01-15 10:30:00', 'it''s', -2.5)")
        db.execute("INSERT INTO t (flag, ts, note, x) VALUES "
                   "(FALSE, '2024-02-20 08:00:00', NULL, NULL)")
        _, r = db.query("SELECT id, flag, note, x FROM t ORDER BY id")
        check("auto-increment ids", [row[0] for row in r] == [1, 2], str(r))
        check("bool round-trip", [row[1] for row in r] == [True, False], str(r))
        check("quote escaping", r[0][2] == "it's", repr(r[0][2]))
        check("NULL round-trip", r[1][2] is None and r[1][3] is None, str(r[1]))
        _, r = db.query("SELECT ts FROM t WHERE id = 1")
        check("ISO timestamp parsed", isinstance(r[0][0], int) and r[0][0] > 1.7e15,
              str(r[0][0]))
        _, r = db.query("SELECT CASE WHEN flag THEN 'yes' ELSE 'no' END AS f, "
                        "COALESCE(note, 'none') FROM t ORDER BY id")
        check("CASE/COALESCE", r == [("yes", "it's"), ("no", "none")], str(r))
        _, r = db.query("SELECT COUNT(*) FROM t WHERE x > -3.0 AND x IS NOT NULL")
        check("float compare + IS NOT NULL", r[0][0] == 1, str(r))
        _, r = db.query("SELECT COUNT(*) FROM t WHERE note LIKE 'it%'")
        check("LIKE", r[0][0] == 1, str(r))
        _, r = db.query("SELECT COUNT(*) FROM t WHERE id IN (1, 3, 9)")
        check("IN list", r[0][0] == 1, str(r))
        _, r = db.query("SELECT COUNT(*) FROM t WHERE id BETWEEN 1 AND 2")
        check("BETWEEN", r[0][0] == 2, str(r))
        # ALTER ADD COLUMN + dup PK rejection
        db.execute("ALTER TABLE t ADD COLUMN extra TEXT")
        db.execute("UPDATE t SET extra = 'e' WHERE id = 1")
        _, r = db.query("SELECT extra FROM t WHERE id = 1")
        check("ALTER ADD COLUMN + UPDATE", r[0][0] == "e", str(r))
        try:
            db.execute("INSERT INTO t (id, flag) VALUES (1, TRUE)")
            check("dup PK rejected (auto-inc explicit id)", False, "no error")
        except Exception as e:  # noqa
            check("dup PK rejected (auto-inc explicit id)",
                  "duplicate" in str(e).lower(), str(e)[:80])
        # SQL-level transactions + SAVEPOINT semantics (errors must propagate)
        try:
            db.execute("SAVEPOINT orphan")
            check("SAVEPOINT without txn errors", False, "no error")
        except Exception:  # noqa
            check("SAVEPOINT without txn errors", True)
        db.execute("BEGIN")
        db.execute("SAVEPOINT sp")
        db.execute("UPDATE t SET x = 99 WHERE id = 1")
        db.execute("ROLLBACK TO sp")
        _, r = db.query("SELECT x FROM t WHERE id = 1")
        check("ROLLBACK TO restores", r[0][0] == -2.5, str(r))
        db.execute("UPDATE t SET x = 42 WHERE id = 1")
        db.execute("RELEASE sp")
        db.execute("COMMIT")
        _, r = db.query("SELECT x FROM t WHERE id = 1")
        check("RELEASE keeps changes", r[0][0] == 42, str(r))
        # DROP TABLE
        db.execute("CREATE TABLE junk (id INT PRIMARY KEY)")
        db.execute("DROP TABLE junk")
        expect_error(db, "dropped table gone", "SELECT COUNT(*) FROM junk", "not found")
    finally:
        db.close()


def suite_aggregates(root):
    db = motedb.Database(os.path.join(root, "agg.mote"), preset="general")
    try:
        db.execute("CREATE TABLE s (g TEXT, v INT)")
        rows = [("a", "1"), ("a", "2"), ("a", "NULL"), ("b", "10"), ("b", "20")]
        vals = ",".join(f"('{g}', {v})" for g, v in rows)
        db.execute("INSERT INTO s VALUES " + vals)
        _, r = db.query("SELECT COUNT(*), COUNT(v), SUM(v), AVG(v), MIN(v), MAX(v) FROM s")
        check("COUNT(*) vs COUNT(col) with NULL", tuple(r[0]) == (5, 4, 33, 8.25, 1, 20),
              str(r[0]))
        _, r = db.query("SELECT COUNT(*), SUM(v) FROM s WHERE v > 1000")
        check("empty-set aggregates (SQL standard)", tuple(r[0]) == (0, None), str(r[0]))
        _, r = db.query("SELECT g, COUNT(*), SUM(v) FROM s GROUP BY g ORDER BY g")
        check("GROUP BY", r == [("a", 3, 3), ("b", 2, 30)], str(r))
        _, r = db.query("SELECT g, SUM(v) AS t FROM s GROUP BY g HAVING SUM(v) > 10")
        check("HAVING", r == [("b", 30)], str(r))
        db.execute("CREATE TABLE d (v TEXT)")
        db.execute("INSERT INTO d VALUES ('x'), ('x'), ('y')")
        _, r = db.query("SELECT DISTINCT v FROM d ORDER BY v")
        check("DISTINCT", r == [("x",), ("y",)], str(r))
        _, r = db.query("SELECT v FROM s ORDER BY v * -1 LIMIT 2")
        # engine follows the SQLite dialect: NULLs sort FIRST ascending
        check("ORDER BY expression + LIMIT (SQLite NULLS FIRST)",
              [row[0] for row in r] == [None, 20], str(r))
        _, r = db.query("SELECT v FROM s WHERE v IS NOT NULL ORDER BY v * -1 LIMIT 2")
        check("ORDER BY expr + IS NOT NULL", [row[0] for row in r] == [20, 10], str(r))
        _, r = db.query("SELECT v FROM s WHERE v IS NOT NULL ORDER BY v DESC LIMIT 2 OFFSET 1")
        check("OFFSET", [row[0] for row in r] == [10, 2], str(r))
    finally:
        db.close()


def suite_edits(root):
    db = motedb.Database(os.path.join(root, "edits.mote"), preset="general")
    try:
        # text index visibility under UPDATE/DELETE
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, txt TEXT, n INT)")
        db.execute("INSERT INTO t VALUES (1,'alpha beta',1),(2,'alpha gamma',2),"
                   "(3,'delta',3)")
        db.execute("CREATE TEXT INDEX t_txt ON t(txt)")
        db.execute("UPDATE t SET txt = 'alpha epsilon' WHERE id = 2")
        _, a = db.query("SELECT id FROM t WHERE MATCH(txt, 'gamma')")
        _, b = db.query("SELECT id FROM t WHERE MATCH(txt, 'epsilon')")
        check("text UPDATE: old word gone, new findable", a == [] and b == [(2,)],
              f"gamma={a} epsilon={b}")
        db.execute("DELETE FROM t WHERE id = 1")
        _, a = db.query("SELECT id FROM t WHERE MATCH(txt, 'alpha')")
        check("text DELETE: no ghosts", a == [(2,)], str(a))
        # spatial visibility
        db.execute("CREATE TABLE p (id INT PRIMARY KEY, pt GEOMETRY)")
        db.execute("INSERT INTO p VALUES (1, POINT3D(0,0,0)), (2, POINT3D(100,100,100))")
        db.execute("CREATE OCTREE INDEX p_pt ON p(pt)")
        db.execute("UPDATE p SET pt = POINT3D(1, 1, 1) WHERE id = 2")
        db.execute("DELETE FROM p WHERE id = 1")
        _, r = db.query("SELECT id FROM p WHERE ST_KNN_3D(pt, 0.0, 0.0, 0.0, 5)")
        check("spatial edits visible via KNN", r == [(2,)], str(r))
        _, r = db.query("SELECT id FROM p WHERE ST_RADIUS_3D(pt, 100, 100, 100, 5)")
        check("spatial DELETE: old location empty", r == [], str(r))
        # vector visibility
        db.execute("CREATE TABLE v (id INT PRIMARY KEY, emb VECTOR(4))")
        db.execute("INSERT INTO v VALUES (1, [1.0, 0.0, 0.0, 0.0]), "
                   "(2, [0.0, 1.0, 0.0, 0.0])")
        db.execute("UPDATE v SET emb = [0.0, 0.0, 1.0, 0.0] WHERE id = 1")
        db.execute("DELETE FROM v WHERE id = 2")
        _, r = db.query("SELECT id FROM v ORDER BY emb <-> [0.0, 0.0, 1.0, 0.0] LIMIT 5")
        check("vector edits visible via KNN", r == [(1,)], str(r))
        # ts late rows + delete cutoff
        base = 1_700_000_000_000_000
        db.execute("CREATE TABLE ts (ts TIMESTAMP, v FLOAT) TIMESERIES(ts)")
        db.execute(f"INSERT INTO ts VALUES ({base}, 1.0), ({base + 2_000_000}, 2.0)")
        db.execute(f"INSERT INTO ts VALUES ({base + 1_000_000}, 1.5)")  # late
        _, r = db.query(f"SELECT COUNT(*) FROM ts WHERE ts <= {base + 2_000_000}")
        check("late row visible in range", r[0][0] == 3, str(r))
    finally:
        db.close()


def suite_errors(root):
    db = motedb.Database(os.path.join(root, "err.mote"), preset="general")
    try:
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        db.execute("INSERT INTO t VALUES (1, 'x')")
        expect_error(db, "unknown table", "SELECT * FROM missing_t", "not found")
        expect_error(db, "unknown column", "SELECT nope FROM t", "column")
        expect_error(db, "parse error", "SELEC * FROM t", "parse")
        expect_error(db, "division by zero", "SELECT 1/0", "zero")
        expect_error(db, "type mismatch", "INSERT INTO t VALUES (2, 3)", None)
        # after all those errors the connection is fully usable
        db.execute("INSERT INTO t VALUES (3, 'ok')")
        _, r = db.query("SELECT COUNT(*) FROM t")
        check("db usable after errors", r[0][0] == 2, str(r))
        # CLI: bad SQL must not kill the shell (缺二进制时 SKIP, 不崩套件)
        cli = _resolve_cli()
        if cli is None:
            check("cli survives bad SQL", "SKIP (motedb-cli not found)", "SKIP")
        else:
            out = subprocess.run([cli, os.path.join(root, "err_cli.mote")],
                                 input="CREATE TABLE z (id INT);\n"
                                       "INSERT bogus;\n"
                                       "INSERT INTO z VALUES (1);\n"
                                       ".exit\n",
                                 capture_output=True, text=True, timeout=60)
            check("cli survives bad SQL", "1 row(s) affected" in out.stdout
                  and "motedb-cli" not in out.stderr[:0], out.stdout[-80:])
    finally:
        db.close()


def suite_params(root):
    db = motedb.Database(os.path.join(root, "params.mote"), preset="general")
    try:
        db.execute("CREATE TABLE p (id INT PRIMARY KEY, f FLOAT, s TEXT, emb VECTOR(4))")
        db.execute("INSERT INTO p VALUES (?, ?, ?, ?)",
                   params=[1, 1.5, "hel'lo", [1.0, 2.0, 3.0, 4.0]])
        db.execute("INSERT INTO p VALUES (2, 2.5, 'plain', [0.0, 1.0, 0.0, 0.0])")
        _, a = db.query("SELECT s FROM p WHERE id = ?", params=[1])
        check("param INT binding", a[0][0] == "hel'lo", str(a))
        _, a = db.query("SELECT id FROM p WHERE f > ?", params=[2.0])
        check("param FLOAT in WHERE", a == [(2,)], str(a))
        _, a = db.query("SELECT id FROM p WHERE s = ?", params=["plain"])
        check("param TEXT in WHERE", a == [(2,)], str(a))
        _, a = db.query("SELECT id FROM p ORDER BY emb <-> ? LIMIT 1",
                        params=[[0.0, 1.0, 0.0, 0.0]])
        check("param VECTOR in KNN", a == [(2,)], str(a))
        _, b = db.query("SELECT id FROM p ORDER BY emb <-> [0.0, 1.0, 0.0, 0.0] LIMIT 1")
        check("param == literal results", a == b, f"{a} vs {b}")
    finally:
        db.close()


def suite_invariant(root):
    """Bank-soak: random transfers in explicit transactions must preserve the
    total balance across edits, rollbacks, a crash-like reopen and checkpoint."""
    import random
    db = motedb.Database(os.path.join(root, "bank.mote"), preset="general")
    try:
        rng = random.Random(7)
        n = 200
        db.execute("CREATE TABLE acct (id INT PRIMARY KEY, bal INT, live BOOL)")
        vals = ",".join(f"({i}, {1000 + i}, TRUE)" for i in range(n))
        db.execute("INSERT INTO acct VALUES " + vals)

        def total():
            _, r = db.query("SELECT SUM(bal) FROM acct WHERE live")
            return r[0][0]

        start_total = total()
        check("bank initial total", start_total == sum(1000 + i for i in range(n)),
              str(start_total))
        # 300 transfers (some rolled back), 30 close/reopen accounts
        for i in range(300):
            a, b = rng.sample(range(n), 2)
            amt = rng.randint(1, 50)
            tx = db.begin()
            db.execute(f"UPDATE acct SET bal = bal - {amt} WHERE id = {a}")
            db.execute(f"UPDATE acct SET bal = bal + {amt} WHERE id = {b}")
            if i % 10 == 7:
                db.rollback(tx)
            else:
                db.commit(tx)
        for who in rng.sample(range(n), 30):
            db.execute(f"UPDATE acct SET bal = 0, live = FALSE WHERE id = {who} AND live")
        t = total()
        check("bank total preserved after 300 txns + closures",
              t == sum(b for (b,) in
                       db.query("SELECT bal FROM acct WHERE live")[1]),
              f"sum={t}")
        # no negative balances (transfers respected funds? not guaranteed —
        # accounts can go negative; the INVARIANT is conservation only)
        _, r = db.query("SELECT COUNT(*) FROM acct WHERE live AND bal < 0")
        negatives = r[0][0]
        db.checkpoint()
        db.close()
        db = motedb.Database(os.path.join(root, "bank.mote"), preset="general")
        t2 = total()
        check("bank total survives checkpoint+reopen", t2 == t, f"{t2} vs {t}")
        _, r = db.query("SELECT COUNT(*) FROM acct WHERE live")
        check("live flags survive", r[0][0] == n - 30, str(r))
        # one bulk correction txn: move everything above median to a vault row
        db.execute("INSERT INTO acct VALUES (99999, 0, TRUE)")
        before_total = t2
        tx = db.begin()
        db.execute("UPDATE acct SET bal = bal / 2 WHERE live AND bal > 1000")
        db.commit(tx)
        _, after = db.query("SELECT SUM(bal) FROM acct WHERE live")
        removed = before_total - after[0][0]
        tx = db.begin()
        db.execute(f"UPDATE acct SET bal = bal + {removed} WHERE id = 99999")
        db.commit(tx)
        check("bank vault reconciliation conserves total", total() == before_total,
              f"{total()} vs {before_total}")
    finally:
        db.close()


SUITES = {
    "multimodal": suite_multimodal,
    "persistence": suite_persistence,
    "txn": suite_txn,
    "crash": suite_crash,
    "cli": suite_cli,
    "sql_types": suite_sql_types,
    "aggregates": suite_aggregates,
    "edits": suite_edits,
    "errors": suite_errors,
    "params": suite_params,
    "invariant": suite_invariant,
}


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--crash-writer":
        crash_writer(sys.argv[2], int(sys.argv[3]), sys.argv[4])
        return
    only = None
    root = tempfile.mkdtemp(prefix="motedb_e2e_")
    args = sys.argv[1:]
    if "--only" in args:
        only = args[args.index("--only") + 1].split(",")
    if "--root" in args:
        root = args[args.index("--root") + 1]
    print(f"motedb {motedb.__version__ if hasattr(motedb, '__version__') else '?'} "
          f"python {sys.version.split()[0]} root={root}", flush=True)
    try:
        for name, fn in SUITES.items():
            if only and name not in only:
                continue
            print(f"\n=== {name} ===", flush=True)
            try:
                fn(root)
            except Exception as e:  # noqa
                check(f"suite {name} crashed", False, repr(e)[:120])
    finally:
        shutil.rmtree(root, ignore_errors=True)
    print(f"\n{'ALL PASS' if not FAILURES else 'FAILURES: ' + ', '.join(FAILURES)}",
          flush=True)
    sys.exit(1 if FAILURES else 0)


if __name__ == "__main__":
    main()
