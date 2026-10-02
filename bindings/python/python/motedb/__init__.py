"""MoteDB — AI-native embedded multimodal database.

Rust-native engine (vectors / full-text / spatial / time-series in one ACID
store) with a Python-first surface. This wrapper adds Arrow/pandas interop
on top of the native extension.
"""

from ._native import *  # noqa: F401,F403
from ._native import Database as _Database

__version__ = __import__("_native_version_hack", fromlist=[""]) if False else None


def _native_version():
    import motedb._native as _n

    return getattr(_n, "__version__", "0.12.0")


__version__ = _native_version()


def query_arrow(self, sql, params=None):
    """Run a SELECT and return a pyarrow.Table.

    Columnar columns ride numpy (zero-copy where the engine already built
    arrays); VECTOR columns become FixedSizeListArray(float32). Requires
    pyarrow (`pip install pyarrow`).
    """
    import pyarrow as pa

    cols, data = self.fetch_arrays(sql, params=params)
    arrays = []
    for c in cols:
        v = data[c]
        # VECTOR columns: 2D numpy (fast path) or a list of equal-length
        # lists → FixedSizeListArray(float32), the canonical Arrow vector
        # representation.
        try:
            import numpy as _np

            if isinstance(v, _np.ndarray) and v.ndim == 2:
                flat = _np.ascontiguousarray(v, dtype=_np.float32).reshape(-1)
                arrays.append(
                    pa.FixedSizeListArray.from_arrays(pa.array(flat), v.shape[1])
                )
                continue
        except ImportError:
            pass
        if isinstance(v, list) and v and isinstance(v[0], (list, tuple)):
            dim = len(v[0])
            if all(len(x) == dim for x in v):
                flat = [f for row in v for f in row]
                arrays.append(
                    pa.FixedSizeListArray.from_arrays(pa.array(flat, type=pa.float32()), dim)
                )
                continue
        arrays.append(pa.array(v))
    return pa.Table.from_arrays(arrays, names=list(cols))


def query_pandas(self, sql, params=None):
    """Run a SELECT and return a pandas.DataFrame (via Arrow)."""
    try:
        import pandas as pd
    except ImportError as e:  # pragma: no cover
        raise ImportError("query_pandas requires pandas (`pip install pandas`)") from e
    return self.query_arrow(sql, params=params).to_pandas()


# Attach the interop methods to the pyo3 Database class (heap type — setattr
# on the class works; verified in test).
_Database.query_arrow = query_arrow
_Database.query_pandas = query_pandas

__all__ = [n for n in dir() if not n.startswith("_")]
