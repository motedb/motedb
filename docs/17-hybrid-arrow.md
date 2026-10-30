# Hybrid Retrieval & Arrow/pandas Interop

## Hybrid Search (BM25 + Vector RRF)

Fuses full-text (BM25) and vector (KNN) results from a single query,
ranked by **Reciprocal Rank Fusion** — no cross-engine score calibration
needed; documents hit by both routes naturally float to the top.

```python
import motedb

db = motedb.Database("app.mote", preset="general")
db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT, emb VECTOR(384))")
db.execute("CREATE TEXT INDEX docs_body ON docs (body)")
db.execute("CREATE VECTOR INDEX docs_emb ON docs (emb)")

hits = db.hybrid_search(
    "docs_body", "rust database",        # text index + query terms
    "docs_emb", [0.1, 0.2, 0.3, 0.4],    # vector index + query vector (384-dim)
    k=10,          # number of results
    rrf_k=60,      # RRF smoothing constant (default 60; higher = more rank-flat)
    fetch_mult=4,  # per-route candidate depth = k × fetch_mult
)
for h in hits:
    # row data + three extra keys:
    print(h["id"], h["__rrf__"], h["__bm25__"], h["__distance__"])
```

Semantics: `__bm25__` / `__distance__` are `None` when that route did not
match the document; documents hit by both routes get the highest fused
score. Rows are returned in descending fused-score order.

## Arrow / pandas Interop

```python
tbl = db.query_arrow("SELECT id, body, emb FROM docs")     # pyarrow.Table
df  = db.query_pandas("SELECT id, v FROM t WHERE v > 10")  # pandas.DataFrame
```

- Scalar columns pass through numpy directly (int64 / float64 / bool / str);
- `VECTOR(n)` columns export as Arrow `fixed_size_list<float32>[n]`;
- `query_pandas` converts via Arrow; pyarrow / pandas are optional
  dependencies (a pointer is printed when missing).

## Known Limitations

| Item | Detail |
|---|---|
| `geom` reserved | `geom` cannot be used as a column name (`GEOMETRY` type-alias collision) — pick another name |
| `LIMIT ?` with set ops | UNION/EXCEPT/INTERSECT results do not support parameterized LIMIT/OFFSET (plain SELECTs do) |
| In-txn MATCH visibility | Uncommitted INSERTs inside a transaction are invisible to `MATCH` full-text predicates until commit |
| TimeSeries `ts INT` | Supported, but `TIMESTAMP` is recommended (enables Gorilla encoding + zone-map pruning) |
