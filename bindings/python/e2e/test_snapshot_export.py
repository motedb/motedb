"""P4 快照冒烟: db.export_to / motedb.import_snapshot / SQL EXPORT DB TO。

运行: python bindings/python/e2e/test_snapshot_export.py
"""
import os
import sys
import tempfile

import motedb


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        src = os.path.join(tmp, "src.mote")
        db = motedb.Database(src)
        db.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, emb VECTOR(3))"
        )
        rows = [(i, f"v{i}", [float(i), 1.0, 2.0]) for i in range(50)]
        db.executemany("INSERT INTO t VALUES (?, ?, ?)", rows)
        db.checkpoint()

        # API 面: 压缩导出
        snap = os.path.join(tmp, "fleet.mote")
        stats = db.export_to(snap, compressed=True)
        assert os.path.isfile(snap), "snapshot must be a single file"
        assert stats["files"] > 0, stats
        assert stats["bytes"] > 0, stats
        db.close()

        # SQL 面: 另一份不压缩
        snap2 = os.path.join(tmp, "plain.mote")
        db = motedb.Database(src)
        db.execute(f"EXPORT DB TO '{snap2}'")
        db.close()
        assert os.path.isfile(snap2)
        assert os.path.getsize(snap) < os.path.getsize(snap2), (
            "compressed should be smaller"
        )

        # 导入 → 打开 → 数据一致
        dest = os.path.join(tmp, "dest.mote")
        motedb.import_snapshot(snap, dest)
        db2 = motedb.Database(dest)
        cols, data = db2.query("SELECT COUNT(*) AS n FROM t")
        n = data[0][0]
        assert n == 50, f"rows={n}"
        _, top = db2.query(
            "SELECT id FROM t ORDER BY emb <-> [0.0, 1.0, 2.0] ASC LIMIT 3"
        )
        assert [r[0] for r in top] == [0, 1, 2], top
        db2.close()

        # 损坏检测
        with open(snap, "rb") as f:
            data = bytearray(f.read())
        data[len(data) // 2] ^= 0xFF
        bad = os.path.join(tmp, "bad.mote")
        with open(bad, "wb") as f:
            f.write(data)
        try:
            motedb.import_snapshot(bad, os.path.join(tmp, "bad_dest.mote"))
            print("FAIL: corruption not detected")
            return 1
        except Exception:
            pass

        # 导入产物为 P3 布局 (打开后的空 wal/ 目录是合法库判定标志, 允许)
        names = set(os.listdir(dest))
        assert "meta.bin" in names, names
        assert "catalog.bin" not in names, names
        wal_files = []
        if os.path.isdir(os.path.join(dest, "wal")):
            wal_files = os.listdir(os.path.join(dest, "wal"))
        assert not wal_files, wal_files

    print("PASS: python snapshot smoke (export/import/sql/corrupt/layout)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
