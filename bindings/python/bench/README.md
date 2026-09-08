# Edge-workload benchmark (reproducible)

`python3 bench_edge.py [N]` — stdlib-only baseline (SQLite via `sqlite3`),
MoteDB via the Python bindings. Runs anywhere Python ≥3.9 runs.

Methodology:
- batched inserts (500-row multi-VALUES / executemany) — the realistic
  embedded write pattern, not per-row FFI calls;
- one warmup pass first (bulk-insert → first-query flush/compaction is a
  documented one-time cost; steady state is what applications live in);
- best-of-3 for scan-shaped queries; 2000-iteration mean for point lookups;
- ANN: MoteDB vector ORDER BY vs SQLite brute-force scan (install
  `sqlite-vec` / `lancedb` and extend for their native paths).

Reference (Apple silicon, N=50K, dim=8):

| shape | MoteDB | SQLite | note |
|---|---|---|---|
| batch insert | 0.54s | 0.28s | SQLite's executemany C loop wins — honest loss |
| PK point | 2.8µs | 8.2µs | 2.9× |
| filter count | 0.84ms | 3.14ms | 3.7× |
| GROUP BY | 1.81ms | 17.8ms | 9.8× |
| ANN top-5 | 41ms | 135ms | 3.2× (SQLite is brute-force) |
| peak RSS | 56MB | 64MB | -12% |
