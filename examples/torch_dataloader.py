"""Training-pipeline demo: build an EPISODE table, export to LeRobot v3,
load frames into a PyTorch DataLoader, and train a step.

Requires: torch + numpy (pip install torch numpy).
Run: python examples/torch_dataloader.py demo.mote
"""
import sys

import numpy as np

import motedb
from motedb.torch import MoteDataset


def main():
    db_path = sys.argv[1] if len(sys.argv) > 1 else "demo.mote"
    db = motedb.Database(db_path)
    db.execute(
        "CREATE EPISODE TABLE teleop (session TEXT, ts TIMESTAMP, "
        "joint VECTOR(4), gripper FLOAT, note TEXT) "
        "EPISODE BY (session, ts) GAP '5s'"
    )
    n = 200
    db.insert_arrays(
        "teleop",
        {
            "session": ["robot-1"] * n,
            "ts": np.arange(1_767_225_600_000_000, 1_767_225_600_000_000 + n * 50_000, 50_000, dtype=np.int64),
            "joint": np.random.default_rng(0).normal(size=(n, 4)).astype(np.float32),
            "gripper": np.linspace(0, 1, n),
            "note": ["pick cube"] * n,
        },
    )
    db.checkpoint()
    print(f"ingested {n} frames")

    # Export for the LeRobot ecosystem (optional leg of the demo).
    db.export_lerobot("teleop", "dataset/", fps=20, tasks="note")
    print("exported ./dataset/ — loadable by lerobot.datasets.LeRobotDataset")

    # Train directly on the SQL view.
    import torch

    ds = MoteDataset(db, "SELECT joint, gripper FROM teleop")
    dl = torch.utils.data.DataLoader(ds, batch_size=32, shuffle=True)
    head = torch.nn.Linear(4 + 1, 4)
    opt = torch.optim.SGD(head.parameters(), lr=1e-3)
    for step, batch in enumerate(dl):
        x = torch.cat([batch["joint"], batch["gripper"].unsqueeze(-1)], dim=1).float()
        loss = ((head(x) - batch["joint"]) ** 2).mean()
        opt.zero_grad()
        loss.backward()
        opt.step()
        if step == 0:
            print(f"first batch: joint {tuple(batch['joint'].shape)}, loss {loss.item():.4f}")
    print(f"trained {step + 1} steps over {len(ds)} frames")


if __name__ == "__main__":
    main()
