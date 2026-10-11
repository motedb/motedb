#!/usr/bin/env python3
"""MCAP 导入端到端: 固定测试 bag(200 joint + 100 camera)→ 表布局/raw 字节/
解码钩子/过滤/重放幂等。bag 由 examples/gen_test_bag.rs 生成入库。"""
import os
import sys
import tempfile
import shutil

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import motedb

BAG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "demo.mcap")


def main():
    assert os.path.exists(BAG), f"missing fixture {BAG}"
    tmp = tempfile.mkdtemp()
    path = os.path.join(tmp, "imp.mote")
    try:
        db = motedb.Database(path)
        r = db.import_mcap(BAG)
        topics = {t["topic"]: t for t in r["topics"]}
        assert topics["/joint_states"]["rows"] == 200, topics
        assert topics["/camera/front"]["rows"] == 100, topics
        table = topics["/joint_states"]["table"]
        assert table == "topic__joint_states", table

        # Raw bytes: first joint message = 0..27.
        row = db.execute(
            f"SELECT data FROM {table} WHERE log_time >= 1700000000000000 "
            f"AND log_time < 1700000000001000 LIMIT 1")[0]["data"]
        assert row == bytes(range(28)), row

        # Re-import → idempotent (no duplicates).
        db.import_mcap(BAG)
        n = db.execute(f"SELECT COUNT(*) AS n FROM {table}")[0]["n"]
        assert n == 200, n
        db.close()

        # Decode hook pass on a fresh prefix: typed columns from raw bytes.
        db = motedb.Database(path)
        r2 = db.import_mcap(
            BAG, include=["joint"], prefix="dec",
            decode={"/joint_states": lambda t, raw: {
                "seq": raw[0], "emb": [b / 255.0 for b in raw[:4]],
            }})
        assert r2["topics"][0]["rows"] == 200, r2
        n2 = db.execute("SELECT COUNT(*) AS n FROM dec__joint_states WHERE emb IS NOT NULL")[0]["n"]
        assert n2 == 200, n2
        seq = db.execute(
            "SELECT seq FROM dec__joint_states WHERE log_time >= 1700000000000000 "
            "AND log_time < 1700000000001000 LIMIT 1")[0]["seq"]
        assert seq == 0, seq
        db.close()

        # Durability: reopen and re-verify raw + hook tables.
        db = motedb.Database(path)
        row = db.execute(
            f"SELECT data FROM {table} WHERE log_time >= 1700000000000000 "
            f"AND log_time < 1700000000001000 LIMIT 1")[0]["data"]
        assert row == bytes(range(28)), row
        n2 = db.execute("SELECT COUNT(*) AS n FROM dec__joint_states")[0]["n"]
        assert n2 == 200, n2
        db.close()
        print("ALL OK")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
