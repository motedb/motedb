#!/usr/bin/env python3
"""kill -9 崩溃恢复回归: 表数据连续无缺口 + FTS 完整(双向)。
v0.12.8 修复: 已 checkpoint 的表崩溃后无 WAL 回放 → 文本索引的内存
pending 丢失且无人重建 → MATCH 静默缺行(实测 198/300, doctor 仍 PASS)。
v0.12.9 修复两个残余缺口:
  (a) 重建在旧索引上追加而非重置 → total_docs/TF 膨胀(BM25 漂移);
  (b) 墓碑丢失方向: crash 前 DELETE 的内存 tombstone 未落盘 → 磁盘索引
      total_docs > 表行数, 旧 `<` 谓词不触发重建 → MATCH 幻影多计。
偶数轮 kill@CKPT 打插入丢失窗口, 奇数轮 kill@DELCKPT 打墓碑丢失窗口;
两方向都断言 MATCH 计数与活行精确一致。"""
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
    db.execute("DELETE FROM c WHERE id >= 400")
    db.checkpoint(); print("DELCKPT", flush=True)
""")


def main():
    tmp = tempfile.mkdtemp()
    path = os.path.join(tmp, "crash.mote")
    fails = 0
    trials = int(sys.argv[1]) if len(sys.argv) > 1 else 6
    try:
        for t in range(trials):
            if os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=True)
            elif os.path.exists(path):
                os.remove(path)
            proc = subprocess.Popen(
                [sys.executable, "-c", WRITER, path],
                stdout=subprocess.PIPE, text=True)
            # Even trials: kill in the post-checkpoint INSERT window
            # (index behind). Odd trials: kill right after the post-DELETE
            # checkpoint (tombstones still memory-only → index ahead).
            want = "DELCKPT" if t % 2 else "CKPT"
            killed = None
            for line in proc.stdout:
                s = line.strip()
                if s.startswith("CKPT") and want == "CKPT":
                    proc.kill(); killed = s; break
                if s == "DELCKPT" and want == "DELCKPT":
                    proc.kill(); killed = s; break
            proc.wait()
            db = motedb.Database(path)
            n = db.execute("SELECT COUNT(*) AS n FROM c")[0]["n"]
            mx = db.execute("SELECT MAX(id) AS m FROM c")[0]["m"]
            cnt = db.execute("SELECT COUNT(DISTINCT id) AS d FROM c")[0]["d"]
            fts = db.execute(
                "SELECT COUNT(*) AS n FROM c WHERE MATCH(v, 'common')")[0]["n"]
            post_live = db.execute(
                "SELECT COUNT(*) AS n FROM c WHERE id >= 300")[0]["n"]
            post_fts = db.execute(
                "SELECT COUNT(*) AS n FROM c WHERE MATCH(v, 'post')")[0]["n"]
            db.close()
            ok = (cnt == mx + 1 and fts == 300
                  and post_fts == post_live)  # exact: no missing, no phantoms
            print(f"{'ok  ' if ok else 'FAIL'} trial{t} kill@{killed} "
                  f"rows={n} contiguous={cnt == mx + 1} fts={fts}/300 "
                  f"post fts={post_fts}/live{post_live}")
            if not ok:
                fails += 1
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print("ALL OK" if not fails else f"{fails} FAILED")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
