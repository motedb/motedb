# MoteDB Python Bindings

Embedded multimodal database for Python — SQL + vector search + full-text
search + spatial in one engine, with edge-presets for memory-constrained
devices.

## Install (from source)

```bash
pip install maturin
cd bindings/python
maturin develop --release
# no maturin / offline? scripts/build_wheel.sh builds a wheel with cargo+zip
```

## Quick start

```python
import motedb

db = motedb.Database("/data/store.mote", preset="edge")   # general|edge|robotics|embodied
db.execute("CREATE TABLE IF NOT EXISTS docs (id INT PRIMARY KEY AUTO_INCREMENT, text TEXT, emb VECTOR(384))")
db.execute("INSERT INTO docs (text, emb) VALUES (?, ?)", params=["hello world", [0.1] * 384])

rows = db.execute("SELECT id, text FROM docs WHERE text MATCH AGAINST('hello')")   # list[dict]
cols, tuples = db.query("SELECT id, text FROM docs")                               # (columns, list[tuple])

# Vector ANN — literal or parameter vectors both order correctly:
near = db.execute("SELECT id FROM docs ORDER BY emb <-> ? LIMIT 5", params=[[0.1] * 384])

tx = db.begin()
db.execute("UPDATE docs SET text = ? WHERE id = 1", params=["hi"])
db.rollback(tx)          # or db.commit(tx)

db.checkpoint()          # durability point
db.close()
```

## API

| Method | Notes |
|---|---|
| `Database(path, preset=None, create=True)` | create-or-open |
| `execute(sql, params=None)` | SELECT → `list[dict]`; DML → affected count; DDL → None |
| `query(sql, params=None)` | SELECT → `(columns, list[tuple])` |
| `executemany(sql, params)` | one INSERT per parameter set — single WAL fsync per batch |
| `begin()/commit(tx)/rollback(tx)` | transactions |
| `doctor()` | self-check → `{"verdict", "checks": [{name,status,detail}]}` |
| `checkpoint()/vacuum()/close()` | lifecycle |

Parameter types: `None/bool/int/float/str/list[float]` (→ embedding vector).
Result types: `None/bool/int/float/str/list[float]` (vector), timestamp as
int microseconds, tensor/geometry render as tags.

## Benchmark

`bench/bench_edge.py` — reproducible edge-workload comparison vs stdlib
SQLite (see `bench/README.md` for methodology and reference numbers: point
2.9×, filter 3.7×, GROUP BY 9.8×, ANN 3.2×, RSS −12%; honest loss on batch
insert).

## Publishing

Wheels for 4 targets are built by `.github/workflows/python-wheels.yml` on
tag push; PyPI publishing uses Trusted Publisher (register the repo at
pypi.org → no token secret needed).

## Known gaps (pre-1.0)

- Tensor/Spatial/TextDoc values pass through as tags, not Python objects.
