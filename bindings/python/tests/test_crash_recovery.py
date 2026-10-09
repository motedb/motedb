#!/usr/bin/env python3
"""kill -9 崩溃恢复回归: 表数据连续无缺口 + FTS 完整。
v0.12.8 修复前: 已 checkpoint 的表崩溃后无 WAL 回放 → 文本索引的内存
pending 丢失且无人重建 → MATCH 静默缺行(实测 198/300, doctor 仍 PASS)。
修复: open 时检测 index total_docs < 表行数 → 走既有重建通道。"""
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import motedb

WRITER = textwrap.dedent("""
    import motedb, sys
    db = motedb.Database(sys.argv[1])
    db.execute("CREATE TABLE IF NOT EXISTS c(id INT PRIMARY KEY, v TEXT)")
    db.execute("CREATE TEXT INDEX IF NOT EXISTS ci ON c(v)")
    for i in range(300):
        db.execute("INSERT INTO c VALUES (?, ?)", params=[i, f"row {i} common"])
    db.checkpoint(); print("FLUSHED", flush=True)
    for i in range(300, 500):
        db.execute("INSERT INTO c VALUES (?, ?)", params=[i, f"post {i}"])
        if i % 50 == 0:
            db.checkpoint(); print(f"CKPT{i}", flush=True)
""")


def main():
    tmp = tempfile.mkdtemp()
    path = os.path.join(tmp, "crash.mote")
    fails = 0
    trials = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    try:
        for t in range(trials):
            if os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=True)
            elif os.path.exists(path):
                os.remove(path)
            proc = subprocess.Popen(
                [sys.executable, "-c", WRITER, path],
                stdout=subprocess.PIPE, text=True)
            killed = None
            for line in proc.stdout:
                if line.strip().startswith("CKPT"):
                    proc.kill()
                    killed = line.strip()
                    break
            proc.wait()
            db = motedb.Database(path)
            n = db.execute("SELECT COUNT(*) AS n FROM c")[0]["n"]
            mx = db.execute("SELECT MAX(id) AS m FROM c")[0]["m"]
            cnt = db.execute("SELECT COUNT(DISTINCT id) AS d FROM c")[0]["d"]
            fts = db.execute(
                "SELECT COUNT(*) AS n FROM c WHERE MATCH(v, 'common')")[0]["n"]
            db.close()
            ok = cnt == mx + 1 and fts == 300
            print(f"{'ok  ' if ok else 'FAIL'} trial{t} kill@{killed} "
                  f"rows={n} contiguous={cnt == mx + 1} fts={fts}/300")
            if not ok:
                fails += 1
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print("ALL OK" if not fails else f"{fails} FAILED")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
