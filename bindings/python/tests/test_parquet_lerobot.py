"""Parquet / LeRobot interop through the Python API (Phase 1 §5).

Covers: export_parquet + pyarrow read-back, import_parquet from a pyarrow-
written file, export_lerobot directory contract, and (skipped without torch)
the MoteDataset DataLoader.
"""

import os
import shutil

import pytest

motedb = pytest.importorskip("motedb")

pa = pytest.importorskip("pyarrow")
import pyarrow.parquet as pq  # noqa: E402


def make_db(tmp_path, table="t"):
    db = motedb.Database(str(tmp_path / "mote.mote"))
    db.execute(
        f"CREATE TABLE {table} (id INT PRIMARY KEY, score FLOAT, name TEXT, emb VECTOR(4))"
    )
    return db


def seed(db, n=100, table="t"):
    import numpy as np

    db.insert_arrays(
        table,
        {
            "id": np.arange(n, dtype=np.int64),
            "score": np.arange(n, dtype=np.float64) * 0.5,
            "name": [f"row-{i}" if i % 7 else None for i in range(n)],
            "emb": np.arange(n * 4, dtype=np.float32).reshape(n, 4),
        },
    )
    db.checkpoint()
    return n


def test_parquet_export_import_roundtrip(tmp_path):
    db = make_db(tmp_path)
    n = seed(db)
    out = str(tmp_path / "t.parquet")

    db.export_parquet("t", out)
    t = pq.read_table(out)
    assert t.num_rows == n
    assert t.column("name")[0].as_py() is None  # NULL survives
    assert t.column("emb")[9].as_py() == [36.0, 37.0, 38.0, 39.0]

    # Round-trip back into a second table.
    db.execute("CREATE TABLE t2 (id INT PRIMARY KEY, emb VECTOR(4))")
    rows = db.import_parquet(out, "t2")
    assert rows == n
    got = db.execute("SELECT id FROM t2 WHERE id = 9")[0]["id"]
    assert got == 9


def test_import_parquet_from_pyarrow(tmp_path):
    import numpy as np

    tbl = pa.table(
        {
            "id": pa.array(range(50)),
            "emb": pa.array(
                [[float(i)] * 4 for i in range(50)], type=pa.list_(pa.float32(), 4)
            ),
        }
    )
    src = str(tmp_path / "pa.parquet")
    pq.write_table(tbl, src, compression="snappy")

    db = make_db(tmp_path, table="staging")
    assert db.import_parquet(src, "staging") == 50
    v = db.execute("SELECT emb FROM staging WHERE id = 7")[0]["emb"]
    assert list(v) == [7.0, 7.0, 7.0, 7.0]


def test_export_lerobot_directory(tmp_path):
    db = motedb.Database(str(tmp_path / "lr.mote"))
    db.execute(
        "CREATE EPISODE TABLE teleop (session TEXT, ts TIMESTAMP, "
        "joint VECTOR(4), gripper FLOAT, note TEXT) "
        "EPISODE BY (session, ts) GAP '5s'"
    )
    import numpy as np

    base = 1_767_225_600_000_000
    db.insert_arrays(
        "teleop",
        {
            "session": ["a"] * 10 + ["b"] * 5,
            "ts": np.array(
                [base + i * 100_000 for i in range(10)]
                + [base + i * 100_000 for i in range(5)],
                dtype=np.int64,
            ),
            "joint": np.arange(15 * 4, dtype=np.float32).reshape(15, 4),
            "gripper": np.linspace(0, 1, 15),
            "note": ["task-a"] * 10 + ["task-b"] * 5,
        },
    )
    db.checkpoint()

    out = str(tmp_path / "dataset")
    db.export_lerobot("teleop", out, fps=20, tasks="note")

    for f in [
        "meta/info.json",
        "meta/stats.json",
        "meta/tasks.parquet",
        "meta/episodes/chunk-000/file-000.parquet",
        "data/chunk-000/file-000.parquet",
    ]:
        assert os.path.isfile(os.path.join(out, f)), f

    import json

    info = json.load(open(os.path.join(out, "meta/info.json")))
    assert info["codebase_version"] == "v3.0"
    assert info["fps"] == 20
    assert info["total_episodes"] == 2
    assert info["total_frames"] == 15
    assert info["total_tasks"] == 2

    tasks = pq.read_table(os.path.join(out, "meta/tasks.parquet"))
    assert tasks.num_rows == 2
    data = pq.read_table(os.path.join(out, "data/chunk-000/file-000.parquet"))
    assert data.num_rows == 15


def test_path_quoting(tmp_path):
    db = make_db(tmp_path)
    seed(db, n=3)
    weird = str(tmp_path / "it's a file.parquet")
    db.export_parquet("t", weird)
    assert os.path.isfile(weird)
    db.execute("CREATE TABLE t3 (id INT PRIMARY KEY, emb VECTOR(4))")
    assert db.import_parquet(weird, "t3") == 3


def test_torch_dataloader(tmp_path):
    torch = pytest.importorskip("torch")
    from motedb.torch import MoteDataset

    db = make_db(tmp_path)
    seed(db, n=64)
    ds = MoteDataset(db, "SELECT id, score, emb FROM t")
    assert len(ds) == 64
    row = ds[9]
    assert row["id"] == 9
    assert row["emb"].shape == torch.Size([4])
    dl = torch.utils.data.DataLoader(ds, batch_size=16)
    batch = next(iter(dl))
    assert batch["emb"].shape == torch.Size([16, 4])
    assert batch["id"].tolist() == list(range(16))
