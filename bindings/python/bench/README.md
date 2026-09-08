# Edge-workload benchmark (reproducible)

`python3 bench_edge.py [N]` — stdlib-only baseline (SQLite via `sqlite3`),
MoteDB via the Python bindings. Runs anywhere Python ≥3.9 runs.

Methodology:
- batched inserts (500-row executemany batches both sides) — the realistic
  embedded write pattern, not per-row FFI calls;
- one warmup pass first (bulk-insert → first-query flush/compaction is a
  documented one-time cost; steady state is what applications live in);
- best-of-3 for scan-shaped queries; 2000-iteration mean for point lookups;
- ANN: MoteDB vector ORDER BY vs SQLite brute-force scan (install
  `sqlite-vec` / `lancedb` and extend for their native paths).

Reference (Apple silicon, N=50K, dim=8):

| shape | MoteDB | SQLite | note |
|---|---|---|---|
| batch insert | 0.29s | 0.16s | SQLite's C executemany loop still wins (1.8×) |
| PK point | 1.4µs | 3.2µs | 2.3× |
| filter count | 0.33ms | 1.34ms | 4.1× |
| GROUP BY | 0.77ms | 7.41ms | 9.6× |
| ANN top-5 | 1.20ms | 56.45ms | 47× (SQLite is brute-force) |
| peak RSS | 45MB | 46MB | parity |
