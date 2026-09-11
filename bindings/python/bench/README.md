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

# Vector-search accuracy with a real embedding model

`python3 vector_recall_eval.py [--skip-a] [--skip-b] [--n-corpus N]` —
`sentence-transformers/all-MiniLM-L6-v2` (22M params, 384-dim), everything
read from the local HuggingFace cache (offline). Embeddings are cached under
`~/.cache/motedb_eval` so reruns skip encoding.

Why two layers: the executor answers `ORDER BY emb <-> ? LIMIT k` with an
exact SIMD scan up to `EXACT_SCAN_MAX_ROWS` (200K rows); only larger tables
are served by the DiskANN graph. Ground truth is float32 brute-force L2 in
numpy (embeddings are normalized, so L2 and cosine rank identically).

**A. SciFact** (5,183 abstracts, 300 test queries + qrels, exact-scan path):

| | recall@1 | recall@5 | recall@10 | nDCG@10 | MRR@10 | Recall@10 | avg latency |
|---|---|---|---|---|---|---|---|
| MoteDB `<->` vs numpy exact | 1.0000 | 1.0000 | 1.0000 | | | | 0.18ms |
| MoteDB `<=>` vs numpy exact | 1.0000 | 1.0000 | 1.0000 | | | | 0.21ms |
| MoteDB top-10 vs human qrels | | | | 0.6451 | 0.6047 | 0.7833 | |
| numpy exact vs qrels (model ceiling) | | | | 0.6451 | 0.6047 | 0.7833 | |

nDCG@10 = 0.6451 matches the MTEB-published SciFact score for this model
(64.51), i.e. the DB adds zero retrieval loss on the exact path. Load: 5,183
rows in 0.3s, `CREATE VECTOR INDEX` 4.2s.

**B. all-nli** (220,000 unique real sentences, 200 held-out queries, k=100;
`preset=edge` for the load, reopened under `general`):

| path | recall@1 | recall@10 | recall@100 | avg / p95 latency |
|---|---|---|---|---|
| no index — exact scan (streamed, 338MB > cache budget) | 1.0000 | 1.0000 | 0.9999 | 142ms / 167ms |
| DiskANN (`CREATE VECTOR INDEX`) + exact re-rank | 1.0000 | 0.9910 | 0.9814 | 30ms / 41ms |
| DiskANN, self-query returns itself | 200/200 | | | 10.9ms |
| DiskANN after close + reopen | 1.0000 | 0.9910 | 0.9814 | 28.5ms / 36ms |
| DiskANN after +5,000 incremental rows | 1.0000 | 0.9905 | 0.9817 | 22ms / 30ms |

Costs on the same run (Apple silicon, single-threaded build): 220K-row load
6.2s, `CREATE VECTOR INDEX` on 220K×384 12.8 min (3.5 ms/row), incremental
inserts through the live index ~150 rows/s. Index-level probe at smaller
scale (bypasses the 200K threshold via `Database::vector_search`):
`cargo run --release --example vector_recall_real -- ~/.cache/motedb_eval/nli-corpus.f32
~/.cache/motedb_eval/nli-queries.f32 40000 384 200` → build 64s, recall@1
0.99 / @10 0.9675 / @100 0.9556 at 40K rows (23s and 0.97 / 0.9705 / 0.947 at
20K), ~5–7ms per raw `vector_search` (no re-rank at this layer).

## What the first run of this eval found (and what was fixed)

The numbers above are after fixing what the first run surfaced. Before:

| | before | after |
|---|---|---|
| autocommit `executemany` into a table with a VECTOR column, `general` preset | 263 rows/s (one group-commit fsync per row) | 69,585 rows/s |
| `SELECT emb … WHERE id = ?` / `… ORDER BY id LIMIT k` / `… WHERE id > 0` after CHECKPOINT | embedding column returned **NULL** (full scans were fine) | correct |
| `CREATE VECTOR INDEX`, 220K×384 | 72.7 min (seek+read per neighbor and per vector during the build) | 12.8 min |
| exact-scan kNN, 5K×384 | 1.4ms | 0.18ms (decoded column cached within the col_cache budget) |
| exact-scan kNN, 220K×384, no index | 811ms | 142ms (streamed; column exceeds the budget) |
| DiskANN recall@1 / @10 / @100 (SQL path) | 0.99 / 0.985 / 0.9745 | 1.0 / 0.991 / 0.981 (exact re-rank of 2k candidates) |
| exact-scan kNN under debug assertions | aborts (`&[f32]` built from an unaligned byte offset — UB) | alignment-safe |

Fixes, in the order they were applied:

1. Multi-row INSERT into vector tables took a per-row path (one WAL
   group-commit wait each) because the batch path's AUTO_INCREMENT fast lane
   skipped vector/text/spatial index maintenance; the fast lane now maintains
   them, the SQL layer uses the batch path, and transactional inserts index at
   COMMIT (previously multi-row ones were indexed *before* commit — a ROLLBACK
   left ghost vectors — and single-row ones never were).
2. The columnar point/top-K/filtered materializers only knew Fixed and Text
   columns; Vector and Spatial fell through to NULL once the write buffer had
   been checkpointed. They now do a bounded per-row read.
3. DiskANN's `graph.bin` dropped its mmap on every append and read one
   neighbor per syscall; appended records are now mirrored in a 256KB
   in-memory tail and flushed in bulk, records are one read/write each, and
   the SQ8 distance kernels run directly on the mmap'd bytes instead of
   copying each vector through an LRU.
4. Exact-scan kNN now decodes the vector column into an aligned, budgeted
   per-segment cache (`col_cache_budget_mb`), streaming through a scratch
   buffer when the column exceeds the budget; the budget is enforced on this
   path (it previously ran only when the store was created). Vector columns
   are stored raw in every preset — f32 embeddings compress ~8%, and
   whole-column compression made every point read decompress the column
   (p95 of indexed queries hit 460ms during one intermediate run).
5. Indexed queries over-fetch 2k candidates from the SQ8 graph and re-rank
   them with the table's full-precision vectors.

Regression tests: `tests/test_insert_paths_index_maintenance.rs`,
`tests/test_vector_column_read_paths.rs`.

# Spatial search accuracy (i-Octree)

`python3 spatial_eval.py [--n 200000] [--nq 100]` — a synthetic indoor
LiDAR-style scan (floor/ceiling/walls/boxes/pillars + 1 cm sensor noise +
100 exact duplicates, coordinates rounded to 0.1 mm so SQL and the numpy
ground truth see identical numbers). 2D functions (`ST_KNN`, `ST_WITHIN`,
`ST_DISTANCE`) desugar to the 3D forms with z = 0, so one index covers both.

Reference (Apple silicon, general preset, 200,088 points):

| check | accuracy | latency |
|---|---|---|
| ST_KNN_3D k=1 / k=10 / k=100 vs numpy | recall 1.0000, distance-ordered | 0.13 / 0.38 / 3.5 ms |
| ORDER BY ST_DISTANCE_3D LIMIT 10 (± index) | recall 1.0000, distances exact (max rel err 0) | 0.4 ms (index) / 43 ms (scan) |
| ST_RADIUS_3D r=0.05 / 0.3 / 1.0 (avg 1/64/963 pts) | precision = recall = 1.0000 | 0.02 / 2.0 / 32 ms |
| ST_WITHIN_3D (random boxes, avg 185 pts) | precision = recall = 1.0000 | 6.1 ms |
| COUNT(*) WHERE ST_RADIUS_3D / ST_KNN_3D | correct | 35 / 152 ms |
| DELETE 5% + move 5% to one coordinate | 0 ghosts, 10,004/10,004 moved rows found, recall 1.0 | ~4 ms per single-row stmt |
| +5,000 incremental multi-row inserts | recall 1.0, new rows searchable | 70K pts/s |
| close + reopen | recall unchanged | — |

Load: 200K points in 2.0s; `CREATE OCTREE INDEX` 0.4s. Single-row UPDATE by
PK used to cost ~95 ms at this table size: a PK-cache miss fell back to a
full table scan, and that scan force-compacted every segment (an O(table)
rewrite per statement). PK misses on integer-PK columnar tables are now an
O(log N) binary search (see `resolve_pk_with_cache`); the 2,008-statement
edit phase of the 20K eval went 25.9s → 8.1s.

## What the first run of this eval found (and what was fixed)

| | before | after |
|---|---|---|
| negative coordinates | `POINT3D(-1, …)`, `ST_KNN_3D(pt, -1, …)`, `ST_WITHIN(…, -180, -90, …)` all parse errors | accepted everywhere |
| `SELECT id, ST_DISTANCE_3D(pt, …)` | NULL (evaluator errored, projection swallowed it) | exact f64 distances |
| un-indexed `ORDER BY ST_DISTANCE_3D … LIMIT k` | sorted on NULL → arbitrary rows | correct order |
| indexed ORDER BY / KNN distance values | **squared** distances | Euclidean |
| `WHERE ST_KNN_3D` (projection) | extra `distance` column (squared) appended to any SELECT list | exactly the SELECT list |
| `COUNT/SUM WHERE ST_KNN_3D / ST_RADIUS_3D` | 0 / NULL | correct |
| `WHERE ST_KNN_3D` without an index | error / empty result | exact scan fallback (memoized per statement) |
| moving >32 rows to one coordinate | rows silently dropped from the index (min-extent leaf slot is 32) | overflow list, merged into every query/delete, persisted (format v3) |
| kNN k=100 @ 20K points | 46.6 ms (GEOMETRY point read decoded the whole column per row) | 0.37 ms (decoded spatial column cached in col_cache) |

Regression tests: `tests/test_spatial_sql_semantics.rs`.
