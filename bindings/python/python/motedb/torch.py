"""PyTorch Dataset over MoteDB — `from motedb.torch import MoteDataset`.

Map-style dataset: the query runs ONCE through the engine's columnar fetch
(``fetch_arrays`` — numpy zero-copy where the engine already built arrays),
and ``__getitem__`` slices per-row views (``torch.from_numpy`` on 2-D vector
columns — no copies). This mirrors how LeRobotDataset serves frames from an
in-memory parquet, so training code transfers unchanged:

>>> import torch
>>> from motedb import Database
>>> from motedb.torch import MoteDataset
>>> db = Database("teleop.mote")
>>> ds = MoteDataset(db, "SELECT joint, gripper, task_index FROM frames")
>>> dl = torch.utils.data.DataLoader(ds, batch_size=64, shuffle=True)
>>> batch = next(iter(dl))
>>> batch["joint"].shape
torch.Size([64, 4])

Multiprocessing workers are safe: the underlying numpy arrays are shared
copy-on-write under fork, and no DB handle is touched after ``__init__``.
"""

from __future__ import annotations


def _require_torch():
    try:
        import torch
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            "MoteDataset requires torch (`pip install torch`) — "
            "see docs/19-training-dataloader.md"
        ) from e
    return torch


class MoteDataset:
    """Map-style torch Dataset over one MoteDB SELECT.

    Args:
        db: an open ``motedb.Database`` (read-only containers work — and are
            the recommended fleet/training form: ``open_readonly`` on an
            exported snapshot).
        sql: the SELECT defining the frame set.
        params: optional ``?`` parameters.
    """

    def __init__(self, db, sql, params=None):
        torch = _require_torch()
        import numpy as np

        self._torch = torch
        self._cols, data = db.fetch_arrays(sql, params=params)
        # fetch_arrays returns numpy for homogeneous scalar columns but plain
        # lists for VECTOR columns — lift those to one 2-D float32 array so
        # __getitem__ can hand out zero-copy row views.
        self._arrays = {}
        for c in self._cols:
            v = data[c]
            if not isinstance(v, np.ndarray) and isinstance(v, list) and v:
                first = v[0]
                if isinstance(first, (list, tuple)) and all(
                    len(x) == len(first) for x in v
                ):
                    v = np.ascontiguousarray(v, dtype=np.float32)
            self._arrays[c] = v
        first = next(iter(self._arrays.values()), None)
        self._len = len(first) if first is not None else 0

    def __len__(self):
        return self._len

    def __getitem__(self, i):
        torch = self._torch
        import numpy as np

        row = {}
        for c in self._cols:
            v = self._arrays[c]
            if isinstance(v, np.ndarray) and v.ndim == 2:
                # Vector column: per-row view, zero-copy.
                row[c] = torch.from_numpy(v[i])
            elif isinstance(v, np.ndarray):
                row[c] = v[i].item()
            else:
                row[c] = v[i]
        return row
