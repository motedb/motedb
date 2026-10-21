# Training DataLoader (PyTorch)

Serve MoteDB query results to a training loop as torch tensors — the last
mile of the `MCAP → SQL → LeRobot` pipeline (`docs/design/2026-10-industry-parity-plan.md` Phase 1 §6).

## MoteDataset

```python
from motedb import Database
from motedb.torch import MoteDataset
import torch

db = Database.open_readonly("fleet_snapshot.mote")   # read-only container: multi-reader safe
ds = MoteDataset(db, "SELECT joint, gripper, task_index FROM frames WHERE episode = 0")
dl = torch.utils.data.DataLoader(ds, batch_size=64, shuffle=True)
for batch in dl:
    ...  # batch["joint"]: (B, dim) float32 tensor — zero-copy row views
```

- The SELECT runs **once** through the engine's columnar fetch
  (`fetch_arrays`): numeric/timestamp columns arrive as numpy arrays with
  zero-copy where the engine already built them; VECTOR columns arrive as a
  2-D `(n, dim)` float32 array.
- `__getitem__` slices per-row views (`torch.from_numpy`) — no per-item
  copies, mirroring how `LeRobotDataset` serves frames from in-memory
  parquet.
- `num_workers > 0` is safe: no DB handle is touched after `__init__`, and
  the numpy buffers are shared copy-on-write under fork.
- TEXT columns come back as Python strings (collate handles them like any
  string batch); NULL comes back as `None`.

## LeRobot round-trip shape

For a LeRobot dataset exported with `db.export_lerobot(...)` and loaded back
(or any table with the v3 column layout):

```python
ds = MoteDataset(db, "SELECT timestamp, joint, gripper, task_index FROM frames")
```

trains on the same frame fields `LeRobotDataset` exposes.

## Performance notes

- Memory: the full result set is held as columnar numpy (same footprint as
  `LeRobotDataset`'s in-memory parquet). For out-of-core iteration, page
  through episodes with `WHERE episode_index = ?` per `MoteDataset`.
- Requires `numpy` (for vector columns) and `torch`. Neither is a hard
  dependency of the `motedb` package itself.
