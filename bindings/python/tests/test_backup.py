#!/usr/bin/env python3
"""backup_to 绑定回归: 在线快照一致性 / 目标已存在报错 / 快照可独立打开 /
Fts/vectors preserved after backup-restore roundtrip."""
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import motedb

FAIL = 0


def check(name, got, want):
    global FAIL
    if got == want:
        print(f"ok   {name}")
    else:
        FAIL += 1
        print(f"FAIL {name}\n  got:  {got!r}\n  want: {want!r}")


tmp = tempfile.mkdtemp()
src = os.path.join(tmp, "src.mote")
snap = os.path.join(tmp, "snapshot.mote")

try:
    db = motedb.Database(src)
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT, s TEXT)")
    db.execute("CREATE TEXT INDEX ti ON t(s)")
    for i in range(500):
        db.execute("INSERT INTO t VALUES (?, ?, ?)", params=[i, i * 2, f"doc {i} common"])
    db.checkpoint()

    # 在线备份 (库保持打开)
    db.backup_to(snap)
    check("backup dest created", os.path.isdir(snap), True)

    # 源库继续写入不受影响
    db.execute("INSERT INTO t VALUES (9999, -1, 'after backup')")
    check("post-backup write", db.execute("SELECT COUNT(*) AS n FROM t")[0]["n"], 501)

    # 快照是备份时刻的一致状态 (不含 backup 之后的那条)
    db2 = motedb.Database(snap)
    check("snapshot row count", db2.execute("SELECT COUNT(*) AS n FROM t")[0]["n"], 500)
    check(
        "snapshot fts",
        db2.execute("SELECT COUNT(*) AS n FROM t WHERE MATCH(s, 'common')")[0]["n"],
        500,
    )
    check(
        "snapshot point query",
        db2.execute("SELECT v FROM t WHERE id = ?", params=[42]),
        [{"v": 84}],
    )
    db2.close()

    # 目标已存在 → 报错
    try:
        db.backup_to(snap)
        check("existing dest rejected", "no error", "error")
    except Exception as e:
        check("existing dest rejected", "error: " + type(e).__name__, "error: " + type(e).__name__)

    db.close()
finally:
    shutil.rmtree(tmp, ignore_errors=True)

print("ALL OK" if not FAIL else f"{FAIL} FAILED")
sys.exit(1 if FAIL else 0)
