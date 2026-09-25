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

# Full-text search accuracy (BM25)

`python3 text_eval.py [--n 20000] [--nq 30]` — 20K real all-nli sentences,
reference = from-scratch BM25 in numpy with the engine's exact parameters
(whitespace tokenizer, k1=1.5, b=0.75, Lucene idf; a second reference mirrors
the 1-byte fieldnorm quantization the on-disk format applies to doc lengths).

| check | result |
|---|---|
| MATCH set vs reference OR-set (rare/mid/common terms + pairs) | precision 1.0000, recall 1.0000 |
| ranked top-10, best-score coverage | 1.0029 (≈1.0; >1 = a quantization-boundary doc outranks its exact score) |
| ranked order consistency / boundary | 28/30, 19/30 (1-byte fieldnorm ties — same design as Lucene) |
| case-insensitivity, unknown terms | correct |
| MATCH AND cat='a' | 0 violations |
| COUNT / SUM over MATCH | correct |
| 1000 deletes + 1000 updates | ghosts 0, 1000/1000 moved docs findable, precision/recall 1.0 |
| +5,000 multi-row inserts | immediately searchable |
| checkpoint + reopen | index intact |
| Chinese, `USING TOKENIZER ngram(2)` | precision/recall 1.0 |
| indexed vs no-index MATCH | 0/20 divergent |

## What the first run found (and what was fixed)

| | before | after |
|---|---|---|
| BM25 doc lengths after a bulk backfill | **destroyed** by the first auto-flush (flush() cleared the pending-length map before the writer ran) → every doc scored with the same constant length, ranking length-blind | lengths persisted; ranking matches reference |
| multi-term postings, partial flush | returned the FIRST of {pending, disk} — a term split across both hid every earlier doc (20K backfill matched only the second 10K) | pending ∪ disk merged (WAND cursors re-sorted) |
| unranked multi-term (no LIMIT) | INTERSECTED postings (≠ ranked path's union) | union + dedup |
| no LIMIT at all | silently capped at 1000 rows | full set |
| `MATCH(c, q) AND cat='a'` | AND side dropped → rows violating cat | compound predicates evaluated; fast path restricted to bare MATCH |
| `COUNT(*) WHERE MATCH` | one row per match | correct count |
| MATCH without an index | AND-of-substrings (different set than the index) | token-OR scan, same set |
| `USING TOKENIZER ngram(2)` | parse error | supported (whitespace/ngram(n); ngram is why CJK works) |

# Time-series search accuracy

`python3 ts_eval.py [--n 200000]` — 64 sensors × 3,125 s at 1 Hz + jitter,
2% late out-of-order rows, reference in numpy. 200,000 rows:

| check | result |
|---|---|
| range COUNT (6 windows, incl./excl. edges) | 6/6 exact |
| COUNT/SUM/AVG/MIN/MAX over a range | exact to 1e-6 |
| GROUP BY sid × 5 aggregates | 64/64 groups exact |
| ORDER BY ts DESC LIMIT 10 | exact |
| LATEST BY sid | exact (was silently ignored — returned every row) |
| 4,000 late out-of-order rows | all visible |
| `DELETE WHERE ts < cutoff` | kept exact, **leaked 0** (was 34,432: write-buffer rows never GC'd + straddling-segment prefixes; GC now flushes the buffer first and rewrites straddling segments row-level) |
| checkpoint + reopen | count intact |

Range-query latency ~80–140 ms at 200K rows (multi-segment scan); DELETE
0.2 s. `TIME_BUCKET('5s'|'1m'|'1h'|'1d', ts)` downsampling works in SELECT
and GROUP BY (by alias or full expression); `BM25_SCORE(col, query)` in the
SELECT list returns real BM25 scores on MATCH queries (with or without
LIMIT; no-LIMIT score queries rank over the full match set).

Regression tests: `tests/test_text_ts_search_semantics.rs`.

# Overall performance (all modalities, one run)

`python3 perf_overall.py [--ts-rows 1000000]` — loads a single database with
all four modalities and measures load throughput + query latency (avg/p50/p95
over warmed runs). Loads run under `preset="edge"`, queries after close +
reopen under `preset="general"`. Reference (Apple silicon, 2026-09, release
wheel): 1M time-series rows, 190K×384 vectors (under the 200K exact-scan
threshold), 100K text docs, 200K 3D points — 516 MB on disk.

| workload | before | after (this round) |
|---|---|---|
| load time-series (autocommit multi-row VALUES) | 201K rows/s | 214K rows/s |
| load vector 384-d (txn executemany, 20K-row commits) | 17.8K rows/s | 24.9K rows/s |
| load text / CREATE TEXT INDEX | 266K rows/s / 3.2 s | 355K rows/s / 2.1 s |
| load spatial / CREATE OCTREE INDEX | 70K rows/s / 1.0 s | 105K rows/s / 0.5 s |
| PK point lookup | 2 µs | 1–2 µs |
| full-scan aggregate (100K) | 0.14 ms | 0.11 ms |
| PK range COUNT 10K span | 18 ms | 13 ms |
| vector exact KNN-10, 190K×384 | 79 ms | 49 ms (numpy brute force: 29 ms) |
| spatial KNN-10 / radius-50 / box-100 (200K pts) | 0.38 / 0.43 / 0.81 ms | 0.37 / 0.40 / 0.70 ms |
| text MATCH ranked top-10 / two terms (100K docs) | 3.4 / 0.15 ms | 3.0 / 0.13 ms |
| text COUNT(*) WHERE MATCH | 93 ms | **0.05 ms** (index postings) |
| ts range COUNT over 1 h window (1M rows) | 445 ms | **20 ms** (segment-pruned fold) |
| ts range AVG/MIN/MAX 1 h | 444 ms | 28 ms |
| ts TIME_BUCKET('60s') 6 h | 1.45 s | **215 ms** |
| ts GROUP BY sid, 6 h window | 513 ms | 202 ms |
| in-txn UPDATE ×2000 (`WHERE id = N`) | 17 rows/s | **253 rows/s** (= autocommit parity) |
| DELETE ts < max−1h (purges 770K rows) | 79 ms | 100 ms, kept count exact |
| close + reopen | 0.2 / 0.4 s | 0.24 / 0.11 s; first octree query 8.5 ms |

What was fixed in this round (regression-tested in
`tests/test_text_ts_search_semantics.rs` + `test_transaction_semantics.rs`):

- **In-transaction UPDATE lost the PK fast path** — the whole path was
  blanket-disabled inside transactions (a buffered INSERT is invisible to
  storage-only resolution), so every `UPDATE … WHERE id = N` full-scanned
  (~72 ms/statement at 100K rows). `execute_update_pk` is now
  transaction-aware (txn tombstones skipped, buffered rows updated in the
  write_set, matching uncommitted INSERTs folded in) — 15×, with
  read-your-writes semantics covered by tests.
- **No time-range pushdown on TIMESERIES aggregates/GROUP BY** — every
  aggregate full-materialized all rows. `ts_simple_aggregate` now routes
  pure time-range predicates (comparisons / BETWEEN / AND, or no WHERE) to
  `ColumnarStore::aggregate_time_range`: segment time pruning + per-column
  fold, zero row materialization, COUNT/SUM/AVG/MIN/MAX ± GROUP BY column or
  TIME_BUCKET bucket. 1 h window at 1M rows: 445 → 20 ms.
- **Stale segment time bounds after a partial DELETE** (correctness, found
  by the pushdown): `purge_straddling_rows` wrote placeholder (MAX, MIN)
  timestamps into the rewritten segment's metadata, so any time-pruned read
  — plain ranged SELECT included — silently skipped it (kept rows
  invisible); only full-range scans masked it. Bounds are now recomputed
  from the surviving rows.
- **`COUNT(*) WHERE MATCH` materialized every matching row** just to count
  it (93 ms at 100K docs). A dedicated fast path counts the index postings
  directly (0.05 ms); `COUNT(col)` on the matched column and bare `COUNT()`
  included, compound predicates still take the general pipeline.
- (previous round) MVCC `VersionStore::evict_if_needed` was O(N) per insert
  past `max_entries` — quadratic bulk writes; now amortized and
  cooldown-gated. And `compile_simple_comparison` had no Timestamp arm, so
  `ts >= <micros>` in a materialized GROUP BY silently matched zero rows;
  comparisons now go through `Value::partial_cmp`.

Remaining known hot spots (measured, not yet fixed):

- **LATEST BY sid** full-materializes (~1.5 s at 1M rows) — could walk
  time-ordered segments newest-first instead.
- **ORDER BY ts DESC LIMIT k** (~366 ms at 1M) — no top-k over the
  columnar store yet; the B+Tree-style "read segments in reverse" trick
  applies.
- Autocommit UPDATE (~250 rows/s) pays one group-commit fsync per row;
  multi-row `UPDATE … WHERE id IN (…)` avoids it.
- **2-table JOIN with GROUP BY / measure filter** misses the equi-join
  fast path and materializes (~300–350 ms for 200K × 5K); plain
  `try_positional_inner_join` handles only simple projections.
- **3+ table JOINs** degrade to nested-loop per-row evaluation — a
  bounded 10K-row three-way join ran minutes; rewrite as chained 2-way
  joins (or wait for a multi-way hash join).
- Correlated subqueries inside aggregate WHERE are rejected with an
  explicit error (`rewrite as a join`), not silently wrong.
- One open `Database` per path per process (a second connection errors
  with "already open by another process"); in-process concurrency is
  threads over a single handle, not multiple connections.

Core-SQL reference (200K orders × 5K customers, release wheel): point
filter p50 0.02 ms; range filter 0.8 ms; ORDER BY … LIMIT 20 → 6 ms;
LIKE '%…%' 3.6 ms; scalar/IN subquery 24/13 ms; 2-way JOIN+aggregate
~340 ms; 5,000 point UPDATEs in one txn 257 rows/s (= autocommit parity);
20K-row INSERT txn 0.06 s; rollback restores UPDATE and DELETE.

# End-to-end (external integration, incl. Docker/Linux)

`bindings/python/e2e/e2e_workload.py` drives the database the way an
embedding application does — Python wheel + `motedb-cli` binary against real
files, across process boundaries:

| suite | checks |
|---|---|
| multimodal | vector KNN, text MATCH, spatial KNN, ts range/TIME_BUCKET (SELECT + WHERE), flush-race stress (20x insert+query) |
| persistence | checkpoint + reopen: rows, text index, index growth |
| txn | read-your-writes, rollback (UPDATE + buffered INSERT), commit durability |
| crash | real `kill -9` mid-ingest: every ACKed write survives, no holes |
| cli | piped SQL through `motedb-cli`, reopen, `doctor` exit code |

`bash scripts/docker_e2e.sh` builds the CLI + wheel from source inside a
clean Linux container (uses the local rust image + cargo cache — no network)
and runs the same workload on Debian/Python 3.13/x86_64.

Integration bugs found by the E2E runs (all fixed):

1. **Linux wheel build was broken twice over**: `bindings/python/build.rs`
   unconditionally passed macOS's `-undefined dynamic_lookup` to the linker
   (fatal on Linux with the new lld default), and the default `jemalloc`
   feature uses initial-exec TLS, which a dlopen()ed extension cannot place
   in glibc's static TLS block (`import motedb` died instantly). build.rs is
   now macOS-gated; the Python crate builds without jemalloc on non-macOS.
2. **Flush-race visibility bug (flaky, platform-independent)**: the
   brute-force vector KNN / spatial KNN fallback / no-index MATCH scans
   snapshotted segments and then read buffered rows WITHOUT a lock; the
   200 ms auto-flush thread could move buffered rows into a new segment
   between the two views — the row appeared in NEITHER (observed as
   `ORDER BY emb <-> ?` returning 0 rows ~1 run in 3 under emulation).
   All three scans now hold the store's flush lock across the snapshot +
   buffered read. NOTE: containers building from a mounted checkout MUST
   set `CARGO_TARGET_DIR` outside the checkout — a Linux build writing the
   host's `target/` corrupts macOS rlibs (mixed-arch archive members).

# Broader functional E2E (round 2)

The suite grew to 11 (sql_types / aggregates / edits / errors / params /
invariant bank-soak added). Round 2 found five more real bugs, all fixed
with regression tests (`test_transaction_semantics::sql_savepoint_*`,
engine unit coverage via the E2E assertions):

1. **SAVEPOINT/ROLLBACK TO/RELEASE errors were swallowed into success
   messages** in the streaming entry — a SAVEPOINT without an active
   transaction was a silent no-op and the later ROLLBACK TO "succeeded"
   while the UPDATE stayed committed. Errors now propagate.
2. **ROLLBACK TO destroyed the target savepoint** — SQL keeps it alive
   (RELEASE / a second ROLLBACK TO must work; its already-undone deltas are
   cleared so they don't replay twice).
3. **`UPDATE … SET float_col = 55` stored 0.0** — UPDATE coercion had
   Float→Integer but not Integer→Float; INSERT paths already coerced.
4. **AUTO_INCREMENT tables accepted explicit duplicate ids** — the PK
   uniqueness check was gated off entirely for auto-inc tables, and the
   explicit-id check only consulted the PK cache, which auto-assigned rows
   never populate. Explicit ids are now checked against cache AND storage
   (O(1) row fetch — auto-inc ids ARE row ids) and advance the counter.
5. **`INSERT INTO t (id, ts) VALUES (1, '2024-01-15 10:30:00')` stored
   ts=0** — the column-list row builder lacked the ISO-text→Timestamp
   coercion the schema-order path has.

Also verified (dialect, documented not changed): ORDER BY expression sorts
NULLs FIRST ascending — SQLite semantics, not PostgreSQL's NULLS LAST.

# P0 fixes (round 3)

Three product-level gaps from the assessment, closed:

1. **CLI `CHECKPOINT`/`VACUUM`** — the shell hand-parsed SQL and these are
   DB operations (intercepted in the api facade, never in the parser), so
   they were hard parse errors in the shell while working from every other
   client. The shell now intercepts them around the executor call (also
   fixed: the statement's trailing `;` survived the old trim order, which
   is why the first attempt at the interception didn't match).
2. **`CREATE INDEX IF NOT EXISTS`** — parsed (typed and untyped forms),
   short-circuits to a notice when the index exists; plain duplicates
   still error. The idempotent-migration staple.
3. **CI gaps** — `python-wheels.yml` now installs and imports the built
   wheel on native runners and runs the 11-suite E2E workload before
   `publish` (the two Linux-fatal packaging bugs would both have been
   caught by this); `ci.yml` gained a push-level `e2e` job (ubuntu +
   macos: maturin wheel + CLI binary + full workload) so the
   external-integration class of regression is caught per push, not per
   release.

# P1 round: JOIN execution + time-series read paths (round 4)

1. **Multi-way INNER equi-joins** (3+ tables, left-deep chains) now run as
   successive hash joins over concatenated positional rows
   (`try_multi_way_inner_join`) instead of the general path's per-row
   nested-loop evaluation — a bounded 10K-row three-way join used to run
   MINUTES; it is now ~70 ms, and a 4-way join ~98 ms (200K × 5K × 20K
   data). Falls back cleanly for non-equi ON conditions.
2. **JOIN + GROUP BY/aggregate shapes** (2+ tables, COUNT/SUM/AVG/MIN/MAX
   on plain columns, plain-column keys, ORDER BY on output or a SELECT-list
   aggregate) fold directly over the join product: 2-table join+aggregate
   344 → 48 ms; join+filter+top-N 332 → 45 ms.
3. **`LATEST BY`** on a TimeSeries table folds per-group max-ts directly in
   the ColumnarStore (`latest_by_group`, only the needed columns decoded):
   1.5 s → 205 ms at 1M rows, per-group latest-ts exact.
4. **`ORDER BY <ts_col> [DESC] LIMIT k`**: bounded top-k heap over the
   decoded ts column (`topk_by_ts`): 366 ms → 43 ms at 1M rows, both
   directions order-exact.

**P0 correctness find on the way (pre-existing, not introduced by this
round): the Gorilla timestamp codec silently corrupted data.** The 32-bit
delta-of-delta bucket's encoder prefix was one bit short of the decoder's
expectation, and delta-of-deltas beyond ±2^31 were truncated outright — any
flush/merge batch whose sorted timestamps contained a jump beyond ±2047
after irregular deltas (typical: out-of-order inserts) corrupted every
later row of the segment on disk. Reproduced as: 200K-row time-series
insert + 4K out-of-order rows + DELETE → 3,488 live rows silently lost
(2000-row batches lost 8192). Fixed (32-bit prefix aligned, 64-bit bucket
added), regression-tested at unit level (`test_timestamp_dod_bucket_
roundtrips`, `test_timestamps_out_of_order_flush_batch_roundtrip`), and
`ts_eval.py` is green again (DELETE leak 0, reopen exact). Any database
written by an older build whose workload had out-of-order timestamp
inserts should be re-validated (`motedb doctor`, row counts vs source).

# Integration round 5: multi-table differential fuzzing (vs SQLite)

New harness: `cargo run --release --example join_differential_fuzz -- [rounds]`
— randomized 3-table schemas (NULL-rich join keys) plus a fixed battery:
2/3/4-way equi-joins, LEFT/mixed/self/reversed-ON joins, JOIN+GROUP BY/
HAVING/ORDER-BY-aggregate/LIMIT+OFFSET, empty-set aggregates, non-equi ON
conditions, UPDATE/DELETE interleaved between comparisons. 100 rounds =
8,800 checks vs SQLite in-memory.

Found (both fixed, regression-tested in `test_text_ts_search_semantics.rs`):

1. **Nondeterministic column binding on JOIN WHERE** — `try_extract_point_query`
   stripped the table qualifier from `WHERE i.id = 1`, and the fallback
   matched the FIRST key ending in `.id` while iterating a HashMap — with
   `i.id`/`o.id` both present the query bound to a random one and returned
   0, 1 or 2 rows across identical runs (same process included: 9/24/17
   split over 50 runs). Now: the qualified name is preserved, resolution is
   exact-key → bare-key → UNIQUE suffix (ambiguous = no match).
2. **Premature LIMIT truncation on `ORDER BY … DESC … OFFSET` joins** — the
   2-table fast path early-stopped the driver scan at LIMIT before the DESC
   sort, so `ORDER BY o.id DESC LIMIT 5 OFFSET 3` returned wrong/short
   results. Early-stop now requires no WHERE, no ORDER BY and no OFFSET.

Known benign divergence (calibrated in the harness): AVG differs from
SQLite by ≤1 in the last printed decimal — SQLite's AVG is a running mean,
MoteDB's is sum/count (accumulation-order float semantics, both correct);
compared with a 1.1% numeric tolerance while integers/text stay exact.

Also confirmed still open (documented): `SELECT … WHERE EXISTS(correlated)`
with aggregates is an explicit unsupported error, not silent wrongness.

# Competitive-gap round 6: build throughput, rerank verification, multi-connection

Three items from the product-gap list, closed and measured:

1. **DiskANN build throughput: 72.7 min → 10.5 min (7x) at 220K×384.**
   Two fixes compound: (a) the SQ8 read path now serves the whole quantized
   set from a RAM "pin" during builds (`pin_all`/`unpin_all` around the
   graph construction — the bounded LRU thrashed on the build's
   O(search-list)-per-node access pattern; `get()` falls back to pure-compute
   dequantization from the pin, removing file reads AND LRU write-lock
   churn); pins are dropped on any mutation for staleness safety. The
   earlier mmap zero-copy reads had already taken 19.8 → 2.5 ms/row; the
   pin takes it to ~2.1-2.9 ms/row. Remaining cost is the sequential
   per-node incremental build loop (parallelizing it changes graph-quality
   semantics — tracked as the next lever).
2. **Full-precision rerank verified end-to-end at 220K through SQL**:
   `ORDER BY emb <-> ? LIMIT k` over-fetches 2k+16 candidates from the
   graph and re-ranks exactly against the table's f32 vectors — recall@1
   **1.0000** (was 0.99 index-level), recall@10 **0.9915** (was 0.9850),
   self-query 1.0000, stable across reopen and +5K incremental inserts.
3. **In-process multi-connection**: the second `Database::open()` on the
   same directory used to fail on flock ("already open by another
   process"). The api layer now keeps a canonical-path registry of live
   engines and attaches new connections to the SAME Arc<MoteDB> (flock is
   per open-file-description — same-process opens never conflict now);
   `close()` detaches, and only the LAST connection shuts down (a live-
   connection counter, not Arc::strong_count — the executor's own Arc made
   that unreliable). Verified: 5 concurrent Python connections (1 writer +
   4 readers, 62K+ aggregate queries) with zero errors; cross-connection
   read-your-writes and rollback visibility exact. Regression test in
   `test_transaction_semantics::in_process_multi_connection_shared_engine`.

# Round 7 sweep: multi-connection edges + isolation semantics

Found and fixed one bug in the new shared-handle registry:

- **Registry key instability create→open**: at `create()` time the target
  directory doesn't exist, so `canonicalize` fell back to the raw path while
  a later `open()` canonicalized successfully (including macOS `/tmp` →
  `/private/tmp` symlink resolution) — different keys, no attach, flock
  error. Keys now normalize the PARENT directory and re-join the leaf.
  Verified: create-then-open while open attaches; relative vs absolute
  path spellings attach to the same engine; cross-process flock still
  blocks (CLI vs a live Python handle) and releases on close.

Documented semantics (known limitation, not a regression): **transactions
are NOT isolated across connections.** The engine's transactions are
write-through (UPDATEs hit storage + undo log; buffered INSERTs surface via
shared bookkeeping), so a second connection sees another connection's
uncommitted writes and rollbacks propagate. Single-connection workflows
(all existing tests, the bank-soak invariant suite) are unaffected; for
multi-connection coordination treat writes as immediately visible, or
serialize writers on one connection. True cross-connection MVCC snapshot
visibility is a storage-layer project (tracked below).

Regression sweep after the fixes: E2E 11 suites ALL PASS; differential
fuzzers 7,129 + 1,320 checks, 0 divergences (the one phase2 hit is the
documented aggregate+correlated-subquery error).

# Round 7 sweep — additional findings

Edge sweep beyond the registry fix:

- **TEXT hard cap: 65,534 bytes** (columnar segment format uses a 16-bit
  length prefix). 65,534 round-trips intact; 65,535+ fails with a clear
  validation error. For long-document / agent-memory workloads this is a
  real product constraint — chunk at the application layer or wait for a
  large-object path (storage-format change, tracked).
- Verified clean (no action): 120-column wide tables; nested BEGIN is a
  loud error; DROP + re-CREATE of the same table name; quoted reserved
  words as table/column names (`SELECT "from" FROM "select"`).
- One test asserting the OLD single-connection semantics
  (`test_double_open_rejected_and_lock_released`) was updated: in-process
  reopens now attach by design; cross-process flock still blocks (verified
  CLI-vs-Python). Drop-without-close also works (the registry's Weak goes
  dead, next open starts fresh).

# Round 8: closing three known gaps

1. **Aggregate + correlated subquery — now WORKS** (was an explicit
   unsupported error): `SELECT COUNT(*) FROM t WHERE EXISTS (SELECT … WHERE
   u.x = t.y)` routes to the materialized path's per-row evaluation. The
   fix was guarding the two positional aggregate fast paths (they treated
   the subquery eval error as "no match" — silent 0). Differential fuzzer
   phase2: 21 checks, **0 divergences** (was the 1 known error-divergence);
   cross-checked against the equivalent IN form. Regression tests in
   `test_exists_subquery` (updated from "must error" to "must be correct")
   and `test_text_ts_search_semantics::aggregate_with_correlated_subquery_where`.
2. **Python GIL released for the whole Rust-side execution** (execute /
   query / executemany): the binding held the GIL for every DB call,
   serializing all threads — a concurrent workload (1 writer + 4 readers)
   ran the writer at 64 rows/s. Now 163 rows/s (2.5×; remaining gap vs the
   269 rows/s solo baseline is genuine CPU contention from the readers).
3. **`ORDER BY … NULLS FIRST/LAST`** supported (SQL-standard syntax;
   parser + AST + a NULL/value-independent comparator in apply_order_by).
   Non-default placements bypass every fast sorter via a boundary
   authoritative re-sort (with LIMIT/OFFSET stripped for the inner run so
   flag-ignoring top-k paths cannot pre-truncate). Default dialect
   unchanged: NULLs first on ASC, last on DESC.

# Round 9: TEXT 64KB cap lifted + parallel graph build

1. **Large TEXT: the 65,534-byte cap is gone.** The cap lived in the
   col-segment builder's IN-MEMORY format ([u16 len][bytes] per value,
   0xFFFF reserved for NULL) — even though the on-disk text layout
   ([null_bitmap][u32 offsets][strings]) always supported 4 GiB. The
   builder prefix is now u32 (NULL = len 0 + authoritative null_flags),
   converted at all 7 encode/decode sites (add_values, add_row, finish(),
   in-memory rebuild decode, merge re-encode ×2, raw-slice NULL
   placeholders ×2 — including the placeholder WIDTH for Text columns)
   plus the write-time schema validation. Verified end-to-end: 1B → 3MB
   values round-trip through checkpoint/reopen AND segment merges
   (regression `large_text_roundtrip_and_merge`); 2MB via the Python
   binding. Two tests asserting the old rejection were updated to assert
   round-trip instead. Spatial columns keep their u16 bincode prefix
   (geometries are small) — noted.
2. **DiskANN graph build: batch-parallel construction (race-fixed).** The
   per-node incremental loop (greedy search + prune + reverse edges) now
   runs under rayon `par_iter` per 5000-node batch. The first version
   had a lost-update race: the reverse-edge maintenance is a
   read-clone-modify-write of OTHER nodes' edge lists, so two concurrent
   inserts backlinking into the same node silently dropped one edge —
   `transactional_insert_indexed_at_commit_not_before` failed ~1 run in
   3 (a committed row's top-1 flipped). Fixed with per-node mutation
   stripes in DiskGraph (`with_node_lock`, 256 stripes, lock order
   node-stripe → flush_lock): every mutation site in
   `incremental_insert_into_graph` now holds the node's stripe for the
   whole read-modify-write (forward-edge set, per-neighbor backlink via
   the extracted `link_neighbor`, and the force-backlink tail). Stress:
   20/20 clean on the previously flaky test. Re-measured at 40K×384
   (general preset, SQL top-k): single-thread 1.51 ms/row → parallel
   0.95 ms/row (**1.6×**), recall@1 = recall@10 = 1.0000, reopen-stable.
   Note: a tiny `for_testing`-config harness showed no parallel gain
   even without locks — the mutation phase (globally serialized by
   flush_lock + inbound map inside `set_neighbors`) dominates there;
   the search-dominated general preset is the representative case.
   Combined with the RAM pin this takes the 220K build from 72.7 min
   (pre-campaign) to ~7.5 min extrapolated.

# Round 10: adversarial probes — NaN ordering, txn/DDL semantics, vector
# top-k visibility, spatial 64KB truncation

Adversarial probe batteries (extreme values / constraints / txns /
three-valued logic / spatial / TS / vector-column lifecycle) found six
real bugs; all fixed with regression tests
(`test_txn_ddl_and_nan_ordering.rs`, `test_vector_visibility.rs`,
`test_spatial_large_geometry.rs`).

1. **NaN sort keys beat real values.** Every ORDER BY comparator used
   `partial_cmp().unwrap_or(Equal)`, which makes NaN compare "equal" to
   everything — its sorted position was arbitrary (measured:
   `ORDER BY emb <-> ? LIMIT 1` returned the NaN-distance row over an
   exact 0.0 match). `OrderedF32`'s documented "NaN = +∞" contract was
   never actually implemented either. Fixed with a shared
   `order_by_cmp` (NULLs first, NaN after all reals — Postgres ASC
   semantics) across all 10+ sort sites plus the vector top-k heap and
   final ranking; WHERE filters keep three-valued logic (NaN compares
   false), verified NOT IN/IN/NOT LIKE against SQLite semantics.
2. **Stray COMMIT/ROLLBACK silently succeeded** ("No active
   transaction" as a SUCCESS result) — a double-commit looked like
   committed data. Now errors, SQLite-style ("cannot COMMIT - no
   transaction is active"), in both the QueryResult and streaming
   dispatch paths.
3. **DROP TABLE inside a transaction permanently lost the table on
   ROLLBACK** — DDL executed immediately and rollback silently didn't
   restore it (verified: table+data gone). DDL is not transactional, so
   destructive DDL (DROP TABLE/DROP INDEX/ALTER) now errors while a
   transaction is active instead of promising an undo it can't deliver.
   CREATE TABLE in a txn keeps working (empty schema escapes, data
   honors the txn — verified).
4. **Vector top-k visibility holes (three states, one fast path).**
   `ORDER BY emb <-> ?` columnar scans mishandled versioned rows:
   (a) duplicate keys in the write buffer resolved FIRST-wins, so an
   UPDATE's new vector lost to the INSERT's original until a flush
   happened; (b) a buffered DELETE tombstone was skipped silently, so
   older segments' live versions resurrected the deleted row (ghost at
   the top of top-k); (c) once the tombstone flushed into its own
   segment, the segment was skipped wholesale because its NULL
   placeholder vector column has dim 0 ≠ query dim — the ghost came
   back. In the SQL layer the ghost was then dropped by the row fetch,
   so users saw EITHER a deleted row or FEWER rows than LIMIT
   (nondeterministic on flush timing). Fixes:
   `buffered_column_values` now resolves last-occurrence-wins (same
   rule as `get`), and tombstones claim their keys in the fast path's
   `seen` set — from the buffer (new `buffered_tombstone_keys`) and
   from every segment branch (including dim-mismatch/undecodable
   segments). Verified across UPDATE→DELETE→checkpoint→reopen
   sequences under both for_testing and general (background threads).
5. **Spatial values > 64KB silently became NULL after reopen.** The
   spatial column format was [len:u16][bincode(Geometry)] and the
   writer TRUNCATED at 65,535 bytes; the mangled payload then failed
   deserialization and read back NULL (70K-point LineString vanished;
   300-point was fine). The prefix is now escape-encoded (u16 normally,
   0xFFFF + u32 for large payloads) at all four encode/decode sites —
   backward compatible, since every valid pre-escape row had
   len < 0xFFFF. 200K-point geometries round-trip through
   checkpoint+reopen.
6. **Python binding: LineString/Polygon inserts always failed.** The
   `points` dict value extracted as `Vec<(f64, f64)>`, which pyo3 only
   accepts for real tuples — the natural `[[x, y], ...]` list shape
   errored ("requires a non-empty 'points' list"). Now accepts both
   lists and tuples, with per-point arity validation.

Also probed clean: i64 min/max + overflow literals, INT+overflow
promotes to float, TEXT with NUL bytes, PK duplicate/NULL and NOT NULL
enforcement, executemany atomicity (mid-batch PK error → nothing
lands), nested BEGIN rejected, three-valued logic (NOT IN with NULL,
NOT (x=1), IS NULL, LIKE NULL/NOT LIKE), TS out-of-order inserts and
duplicate timestamps (kept, ordered correctly), vector
UPDATE→DELETE→re-INSERT consistency. Noted, not changed: CREATE TABLE
inside a txn survives ROLLBACK with data rolled back (MySQL-style
non-transactional DDL); TS duplicate (ts) rows are kept, not deduped;
MIN/MAX aggregates still use partial_cmp (NaN-in-MIN/MAX semantics
deferred).

# Round 11: performance regression + competitor benchmark

## Regression check (no regressions)

1. **Round-9 ↔ Round-10 A/B (interleaved, 3 rounds each, 100K×384
   table, p50):** vector knn 6.25 vs 6.25 ms, ORDER BY LIMIT 0.361 vs
   0.360 ms, TS range count 2.62 vs 2.64 ms, PK point 0.003 ms both —
   Round 10's correctness fixes (NaN-aware comparators, tombstone
   claiming, escape prefixes) cost nothing measurable. In-run numpy
   brute-force control confirmed machine stability.
2. **DiskANN build** (40K×384, general preset): 0.81 ms/row
   (vs 0.95 after the R9 stripe-lock fix, 2.48 sequential, ~19.8
   pre-campaign), recall@1 = recall@10 = 1.0000, reopen-stable.
3. **perf_overall.py full run** came back with some numbers worse than
   the recorded history (vector exact 48.7 → 76 ms, LATEST BY ~258 ms
   vs 205 ms) — but the run coincided with other load on the machine
   (its numpy brute-force control was +27% too). Under quiet load the
   A/B above shows parity; treat perf_overall absolute numbers as
   load-sensitive. Post-campaign highlights vs the pre-campaign
   baseline JSON: LATEST BY 1538 → 258 ms, top-k ORDER BY 366 → 442 ms
   at 1M rows under load (both dominated by ambient load; see A/B for
   like-for-like), UPDATE-in-txn 253 → 265 rows/s.

## Competitor benchmark (compete_bench.py)

Same dataset everywhere: 100K rows × (int PK, ts, device, val,
~80B text, 384-d f32 vector). Each engine in its own subprocess;
batched loads (driver never materializes the full row list);
DuckDB loaded via 1000-row multi-VALUES (executemany is its slowest
path, noted separately). p50 latencies:

| workload (p50)            | MoteDB  | SQLite 3.51 | DuckDB 1.4.5 | FAISS Flat |
|---------------------------|---------|-------------|--------------|------------|
| bulk load rows/s (w/ vec) | 42,106  | 59,556      | 7,407 (multi-VALUES; 2,877 via executemany) | n/a (in-RAM) |
| text/FTS index build      | 3.31 s  | 0.14 s (FTS5) | n/a (LIKE only) | n/a |
| PK point lookup           | 17 µs   | **6 µs**    | 73 µs        | n/a |
| range COUNT+AVG (10%)     | 5.2 ms  | 0.38 ms     | **0.29 ms**  | n/a |
| GROUP BY device           | 1.53 ms | 51.5 ms     | **0.60 ms** | n/a |
| top-k ORDER BY LIMIT 10   | **0.36 ms** | 37.9 ms | 0.74 ms      | n/a |
| equi-JOIN + GROUP BY      | 138 ms  | 10.5 ms     | **1.2 ms**   | n/a |
| exact vector knn@10       | 6.1 ms  | 16.0 ms (fetch-all + numpy) | 67.6 ms (array_distance) | **2.8 ms** |
| text two-term search      | 0.009 ms | 0.013 ms (FTS5 MATCH) | 0.22 ms (LIKE) | n/a |
| DB size on disk           | 351.5 MB | 212.6 MB | 489.7 MB | 153.6 MB (raw f32, no durability) |
| query-phase RSS delta     | 1.35 GB (incl. mmap'd segments + decoded col caches + jemalloc retention) | ~0 | ~0 | 670 MB (holds the index in RAM) |

Takeaways:
* **MoteDB's edge**: top-k ordered scans (2× DuckDB, 105× SQLite),
  exact vector search inside SQL (11× DuckDB's array_distance, 2.6×
  fetch-all+numpy), FTS on par with SQLite FTS5, GROUP BY 34× faster
  than SQLite — all in ONE durable embedded engine (the only one of
  the four doing SQL + FTS + vector + spatial + TS natively).
* **Honest gaps**: PK point lookup 3× behind SQLite's B-tree (17 µs —
  still sub-frame); un-indexed range aggregation 13-18× behind
  (SQLite uses its (ts, device) index; a regular-table secondary
  index path is the improvement item); equi-JOIN 138 ms vs DuckDB's
  1.2 ms — the multi-way join work targeted TS-shaped queries, this
  100K-probe × 64-build + GROUP BY shape hits a slow path (top
  optimization candidate); disk footprint 1.65× SQLite (f32 vectors +
  LSM segment duplication — SQ8 quantized storage only kicks in with
  a vector index); load throughput mid-pack.
* FAISS queries faster (2.8 ms) but is RAM-only with no SQL,
  durability, or multi-modal story; MoteDB is within 2.2× of a
  dedicated SIMD ANN library while providing the full database
  around it, and switches to DiskANN (sublinear) past 200K rows.

# Round 12: fix the four competitor gaps (disk / RSS / range agg / JOIN)

Round 11's table left four honest gaps. Round 12 root-caused each with
targeted probes (per-stage RSS via vmmap footprint, per-file disk listings,
query-shape bisection) and fixed all four, plus three latent correctness
bugs the differential tests exposed along the way.

## Root causes → fixes

1. **Disk 2× duplication (351.5 MB)**: `checkpoint()` ran
   `force_compact_all()` (merging 20 segments into one) but never called
   `sync_manifest()` — the 20 superseded segment files stayed on disk until
   the NEXT reopen happened to sweep them. Fix: checkpoint syncs the
   manifest + physically deletes retired files. Verified by a stage probe:
   post-checkpoint (zero queries) 322.4 MB → **161.2 MB, 1 segment**.
2. **Query-phase RSS 1.35 GB** had three contributors, all fixed:
   - `prepare_for_query` eagerly loaded the WHOLE merged segment into heap
     within the 256 MB col-cache budget (165 MB resident from ONE
     `SELECT COUNT(*)`). Fix: eager load capped at the same 8 MiB open-time
     threshold; bigger segments stay lazy (fence + seek+read).
   - Multi-term-AND aggregates fell back to full-table materialization
     (100K full-width rows incl. a 384-dim VECTOR column ≈ +180 MB).
     Fixed by (3) below.
   - The equi-JOIN fast path scanned BOTH tables full-width (+294 MB,
     138 ms). Fixed by (4) below.
   - Decoded-column cache split into two budgets: 64 MB general
     (text/fixed decodes re-stream cheaply) + 256 MB vector-only (a cached
     100K×384 column answers knn in ~6 ms vs ~80 ms streamed — the split
     keeps that headline number without letting scan caches track data
     size).
3. **Range COUNT+AVG 5.2 ms → 0.79 ms**: `WHERE ts>=? AND ts<=? AND device=?`
     either fell to the materialized path (COUNT(*)-only shape returned
     `None` outright) or materialized every row passing the FIRST predicate
     before post-filtering. New `ColSegmentStore::aggregate_multi_filtered`
     evaluates the full AND over raw column bytes (typed i64/f64/bool/text
     decoders, per-column pre-decode once per segment) and folds
     count/sum/min/max in the same pass — zero per-row Value allocation.
4. **equi-JOIN + GROUP BY 138 ms → 18.9 ms**: the multi-way hash join now
   projects each table to the columns actually referenced anywhere in the
   statement (SELECT/WHERE/ON/GROUP BY/ORDER BY via a strict expression
   walker; anything un-walkable or `SELECT *` disables pruning). The bench
   query touches 2 of 9 columns — the 153 MB VECTOR column is never
   decoded. Simple AND predicates also filter positionally instead of
   per-row expression interpretation.

## Latent bugs fixed en route (found by the differential tests)

- `COUNT(col)` in the text-equality aggregate fast path counted NULL rows
  (and used the row count as the AVG denominator).
- TEXT values > 64 KB read back as NULL on the point-query path: the
  page-cache "sanity cap" rejected `len > 65536` outright; now bounded by
  the column region size (a real corruption bound). 120 KB strings survive
  flush → merge → checkpoint → reopen.
- `COUNT(text_col)` inside the join path ignored non-numeric non-NULL
  values.
- `count_sum_min_max_text_filter` never flushed the write buffer (recent
  INSERTs were invisible to it).

## Rerun (same dataset as Round 11)

MoteDB and SQLite rerun together under identical (moderate) ambient load;
DuckDB/FAISS columns are Round 11 values (engine code unchanged).
Query-phase RSS now measured as a driver-freed delta, matching the
SQLite/DuckDB methodology.

| workload (p50)            | MoteDB R11 → R12 | SQLite (same run) | DuckDB (R11) |
|---------------------------|------------------|-------------------|--------------|
| range COUNT+AVG (10%)     | 5.2 → **0.79 ms** | 0.47 ms          | 0.29 ms |
| equi-JOIN + GROUP BY      | 138 → **18.9 ms** | 11.9 ms          | 1.2 ms |
| GROUP BY device           | 1.53 → 2.1 ms*   | 75.4 ms          | 0.60 ms |
| top-k ORDER BY LIMIT 10   | 0.36 → 0.53 ms*  | 53.6 ms          | 0.74 ms |
| exact vector knn@10       | 6.1 → 7.9 ms*    | 22.4 ms (numpy)  | 67.6 ms |
| PK point lookup           | 17 → 21 µs*      | 7 µs             | 73 µs |
| DB size on disk           | 351.5 → **186.5 MB** | 212.6 MB      | 489.7 MB |
| query-phase RSS delta     | 1.35 GB → **44 MB** | ~0             | ~0 |

\* slightly worse than R11's quiet-machine numbers; the whole batch ran
with a foreign benchmark pinning ~4 cores (SQLite's own numbers are ~2×
its R11 values in the same run — the relative picture is what holds).

Takeaways: disk now BEATS SQLite (186.5 vs 212.6 MB) with the same f32
vector payload; query-phase RSS is same order as SQLite's (44 MB vs ~0,
and 15× below FAISS's in-RAM index); the range-agg gap closed from 13× to
1.7× vs SQLite (DuckDB's zone maps keep it ahead); JOIN closed from 13×
to 1.6× vs SQLite but remains the top optimization candidate vs DuckDB
(the remaining cost is the projected scan materializing one Vec<Value>
per row — a raw-bytes group accumulation over the join column would take
it to low single-digit ms). The first-query compaction stall is also gone
(checkpoint leaves one segment; prepare_for_query has nothing to merge).

Regression coverage: `tests/test_round12_optimizations.rs` — 9
differential tests (checkpoint file reclamation + reopen integrity,
fused multi-predicate aggregates vs source-computed expectations across
NULLs/text-ranges/coercions/empty sets/UPDATE+DELETE visibility/
in-txn read-your-writes, projected join+group-by vs Rust-computed
expectations incl. NULL join keys, 3-table chains, COUNT(text_col)),
plus the full `cargo test --release -p motedb` suite (EXIT=0).

# Round 12b: edge-intelligence audit (edge / robotics / embodied presets)

Audited the Round-12 memory work against the embedded presets and found
three edge-specific issues, all fixed and regression-tested
(`tests/test_round12b_edge.rs`):

1. **Vector cache budget ignored the presets** — the new 256MB
   vector-column cache was a fixed constant, so `for_embodied` (which
   promises "~80MB peak" and makes vector KNN a PRIMARY workload) could
   balloon past edge memory ceilings. Now a `vector_cache_budget_mb`
   config: general 256MB (default), embodied 64MB, edge/robotics 32MB,
   runtime-tunable via `Database.set_vector_cache_budget(table, bytes)`.
2. **Oversized-column streaming read whole-column** — when the vector
   column exceeds the (now smaller) budget, the top-k path materialized
   the entire column per query: 153MB read buffer on a 100K×384 table,
   +292MB peak RSS with allocator retention — OOM-class on a 256MB
   device. Vector columns are always stored raw (flag=0), so the scan
   now streams ~8MB chunks; measured query-phase RSS delta went
   **+292MB → +4MB** with knn latency unchanged-to-better (25.6ms p50
   under ambient load). Streamed results are bit-identical to the cached
   path (differential test incl. UPDATE/DELETE visibility).
3. **Cold reopen loaded every segment's full key array** —
   `max_row_id()` (next_row_id recovery) was O(N) with a full-keys load
   per segment. Keys are stored sorted (the binary-search invariant), so
   it now reads ONE 8-byte key per segment: edge-preset reopen on a
   100K-row DB **378ms → ~45ms**.

Also verified empirically:
- **Crash recovery**: kill -9 mid-load (50K rows, edge preset) → reopen
  shows all checkpointed rows, appending works, no duplicates.
- **Edge disk**: edge preset's zstd compact storage lands the 100K×384
  dataset at **156.6MB** (vs 186.5MB general, 212.6MB SQLite).
- **Edge latencies** (100K rows): PK point 15µs, fused range agg 0.75ms.
- **Footprint**: CLI 7MB, Python wheel 5MB, loaded .so 8MB — no edge
  storage concern.

# Round 12c: full functional E2E (wheel + CLI) — 4 bugs found & fixed

Comprehensive functional pass over the current engine (Python wheel E2E 51
checks, CLI E2E 17 checks — both suites saved under `bindings/python/tests/`),
plus Rust differential regressions (`tests/test_round12c_e2e_bugs.rs`).
Coverage: SQL surface (types/expressions/order/group/having/join/subquery),
vector (exact-knn vs numpy, param-vs-literal, streamed-vs-cached, UPDATE/
DELETE visibility), FTS (match/BM25/update/delete), spatial (ST_WITHIN /
ST_DISTANCE / 20K-point geometry reopen), timeseries (range agg, LATEST BY),
transactions (read-own-write/rollback/DDL-in-txn/stray-COMMIT), the Round-12
paths (fused aggregates, join pruning, checkpoint disk reclaim, long TEXT,
vector budget API), edge preset E2E, crash recovery, CLI shell/doctor/error
handling/persistence.

Bugs found by the pass (all fixed + regression-tested):

1. **Boolean literals in multi-predicate WHERE matched nothing** —
   `flag = TRUE AND id < 20` returned 0 rows: the fused aggregate path
   left the integer target empty for `Value::Bool` literals (single-
   predicate paths coerced correctly).
2. **`ORDER BY x LIMIT k` disagreed with unlimited ORDER BY on NULLs** —
   the top-k heap path coerced NULL floats to NaN (total-order MAXIMUM):
   ASC top-k DROPPED null rows while DESC ranked them FIRST. NULL now
   orders as the smallest value engine-wide (NULLs first ASC / last DESC),
   in both `top_k_row_indices_typed` and `top_k_from_indices_typed` (which
   previously SKIPPED null rows outright).
3. **Zero-argument `BM25_SCORE()` returned NULL for every row** — the FTS
   projection only filled the score when the call had a matching first
   ARGUMENT; the documented `SELECT id, BM25_SCORE() … WHERE MATCH(col,
   'q')` shape (used by the Round-11 benchmark!) silently lost scores.
   Only latencies were ever asserted there, never the values.
4. **`LATEST BY` silently dropped by two WHERE fast paths** —
   `try_columnar_select` (TS columnar pushdown) and FAST PATH 1d
   (`try_positional_where`) served `… WHERE sensor='s1' LATEST BY sensor`
   without any latest-per-group fold and returned EVERY matching row.
   Both now decline; the streaming entry routes all LATEST BY shapes to
   the materialized path whose tail applies `apply_latest_by`.

Note: `LATEST BY <col>` groups BY that column and keeps each group's
max-timestamp row (`LATEST BY sensor` = latest per sensor). `LATEST BY ts`
groups by ts itself — all rows back — by design
(test_timeseries_semantics asserts this).

## Round 13 — Bug 清除计划: differential fuzz campaign

Round 12c 的 E2E 还能挖出 4 个正确性 bug, 说明手工用例已到边际收益。
本轮改为系统化差分测试, 两个 harness 入库为常驻回归门槛:

- **`tests/test_fuzz_differential.py`** (tier-1): SQLite(oracle) × MoteDB 三路
  对拍 — 同一批随机 SQL 分别在 ①SQLite ②MoteDB-LSM(未 checkpoint)
  ③MoteDB-列存(checkpoint 后) 执行, 外加随机变异 (UPDATE/DELETE/INSERT)
  后复跑 + 同状态跨相位自洽 (SELF_DIVERGE, 专抓快速路径/列存路径静默分歧)。
  查询语法覆盖: 多层 WHERE (AND/OR/NOT/IN/BETWEEN/LIKE/IS NULL/NULL 比较)、
  聚合 (COUNT/SUM/AVG/MIN/MAX/DISTINCT)、GROUP BY+HAVING、ORDER BY 多键
  (投影外列) + LIMIT/OFFSET、INNER/LEFT/三表 JOIN、IN 子查询、标量子查询、
  表达式投影 (UPPER/ROUND/COALESCE/算术等)。campaign 量: 20 seed × 400 查询
  × 5 相位全绿; 入库版固定 4 seed × 250 查询 (~1 分钟)。
  (Round 13b 已补大表专用有界 harness — 见下; 30K×3 seed 的完整 campaign 因
  SQLite oracle 侧的等值 JOIN 太慢未跑完, 多段+重开一致性以 6-8K 入库版为准,
  更大规模留待预物化期望结果的方案。)
- **`tests/test_feature_selfcheck.py`** (tier-2): MoteDB 特有功能对照 Python
  暴力计算 — 向量 KNN (L2/cosine, 含变异可见性)、LATEST BY (+ORDER BY)、
  MATCH/BM25 (需 TEXT INDEX)、空间 ST_WITHIN / `loc <-> ST_POINT` top-k、
  TIMESERIES 删插、事务回滚。8 seed 全绿。

### 挖出并修复的 12 个 bug

| # | 症状 (fuzz 发现) | 根因 | 修复 |
|---|---|---|---|
| 1 | `JOIN … WHERE a.v <> 10` 多返回恰好等于 NULL 行数的行 | `apply_op_value` 的 `Ne => v != target` 在 v=Null 时为 true, 丢了三值逻辑 (SQL: NULL <> x = UNKNOWN) | NULL 操作数一律不匹配 (executor.rs) |
| 2 | `JOIN … ORDER BY a.id, b.id DESC` 第二排序键失效 | 投影排序的 bare-name 回退把未投影的 `b.id` 误匹配到输出列 `a.id`; 三处同类 (join 投影排序 / join 聚合排序 / finalize_join_result) | 限定名精确匹配 + 裸名唯一命中, 否则回落通用路径; finalize 在投影前于 combined 全列行上解析 |
| 3 | `WHERE NOT (g < NULL OR …)` 返回全部行 | `eval_expr_on_row` 的 AND/OR 是二值逻辑 (`is_truthy(a) \|\| is_truthy(b)`), NULL 被压成 FALSE, NOT 一翻全过 | Kleene 三值 AND/OR (FALSE 主导/TRUE 主导/其余 NULL) |
| 4 | `ROUND(48.05, 1)` = 48.1 (SQLite/MySQL 均为 48.0) | `(f*10^d).round()/10^d` 被乘法浮点误差污染 (48.05 的 f64 真值是 48.0499…) | 精确十进制字符串展开 + half-away-from-zero 舍入 (`round_f64_half_away`) |
| 5 | 变异后 `IN (SELECT …)` 返回已删行 (SQLite 返回 0 行) | `build_in_hashset_from_columnar` 直读列存 SSTable 投影, 绕过 LSM 墓碑/写集合并层 — 冻结在旧快照 | 改用与 JOIN 相同的变异可见扫描 `scan_table_rows_fast_projected` |
| 6 | `… WHERE id = 100` (非 AUTO_INCREMENT PK, 无索引) 硬报 "Column index not found" | `try_optimize_primary_key_point_query` 假设 PK 自动带列索引 (对非 AUTO_INCREMENT 不成立); `execute_range_query_streaming` 同类 | 无索引 → decline/回落全扫, 语义不变 |
| 7 | `SELECT MIN(cat) WHERE grp > NULL` 返回 '' (应为 NULL) | `build_comparison_predicate` 用 Value::partial_cmp 的 NULL-最小全序做过滤 → `grp > NULL` 匹配全部行; `v < 100` 也会匹配 NULL 值行 | target 或行值为 NULL 一律不匹配 |
| 8 | JOIN `ORDER BY i.id, t.id LIMIT 27` 最后一行与无 LIMIT 版本不一致 | join hash 探针的 "LIMIT 提前终止" PERF 优化在 ORDER BY 存在时仍先截断后排序 | ORDER BY 存在时禁止早停 |
| 9 | `LATEST BY sensor ORDER BY sensor` 返回 2 行 s2 + 0 行 s1 | 物化路径先排序后 apply_latest_by, 后者按索引把 filtered_rows(原始序) 与投影行配对 — 排序置换后错位; 且 HashMap into_values 输出乱序 | 排序置换记录 + filtered_rows 同步重排; apply_latest_by 改两遍法按输入顺序稳定输出 |
| 10 | `ORDER BY loc <-> ST_POINT(x, y)` 原样返回插入序 | GEOMETRY 距离排序无下推 (VECTOR 有), 列存扫描的投影排序静默跳过求不出键的表达式 | 新 `order_by_needs_full_rows` 路由: 表达式键引用投影外列 → 物化全行排序 (VECTOR 距离键仍走列存 top-k) |
| 11 | 同上 — 物化路径报 "Unknown function: ST_POINT" | 通用求值器不实现 ST_POINT | evaluator 新增 ST_POINT(x, y[, z]) → Spatial Point/Point3D |
| 12 | 同上 — 报 "Left operand is not a vector" | `BinaryOperator::L2Distance` 求值只认 Tensor/Vector | Spatial 点对走欧氏距离分支 |

回归覆盖: `tests/test_round13_bug_hunt.rs` 10 例 (Rust) + 两个入库 harness。

### 顺带发现 (非 bug, 记录)

- Python 绑定参数计数宽松: 传 `[vec]` 以外的形状 (如裸 vec 被拆成 N 个标量)
  不报错, 静默绑定第一个值 — 用法 footgun, 文档已注明 params 必须是 list。
- 无 TEXT INDEX 时 MATCH 过滤可用但 BM25_SCORE() 为 NULL (分数图仅索引路径
  填充) — 文档化: BM25_SCORE 需先 `CREATE TEXT INDEX`。
- TIMESERIES 行不可变: UPDATE 报错提示用 "DELETE (时间范围) + 重插" 替代
  (by design); DELETE 仅支持 `ts < value` 形式谓词。
- AVG/SUM 浮点与 SQLite 有 ±1e-6 knife-edge 差异 (SQLite 用 Kahan 求和) —
  harness 以 2e-6 容差对齐, 属求和顺序噪声非正确性问题。

## Round 13b — 大表有界差分 + 形状扩展

Round 13 收口时留下的 30K 大表 campaign 缺口: 旧 harness 的无 LIMIT 三表 JOIN
把数百万行拉过 Python 边界跑不完。本轮重设计并继续扩展:

- **`tests/test_fuzz_bigtable.py`** (新入库): 全部查询要么服务端聚合
  (GROUP BY / COUNT / AVG — 返回少量行) 要么严格 LIMIT ≤ 50; 数据分 3 批
  插入、每批 checkpoint — 强制 ColSegmentStore **多段合并**路径 (400 行
  harness 只打单段); 末尾 **REOPEN 相位** (close → reopen → 全查询复跑,
  对照关闭前结果) 作为持久化层一致性 oracle。30K×3 seed campaign 跑通。
- **形状扩展** (test_fuzz_differential.py +7 类生成器): CASE WHEN、字符串
  函数 (|| / CONCAT / REPLACE / INSTR / UPPER||LOWER)、自 join、GROUP BY
  别名 (k)、GROUP BY 表达式 (id % 5)、ORDER BY 表达式、LIMIT 0。

### 挖出并修复的 4 个缺口 (SQLite/PG 语义对齐)

| # | 症状 | 根因 | 修复 |
|---|---|---|---|
| 1 | `GROUP BY b % 3` parse error ("Multiple statements") | parse_group_by_items 只收列名/函数调用, `%` 直接截断语句 | 项级回溯: 先试列名, 后跟运算符/字面量则回溯按完整表达式重解析 → canonical name |
| 2 | `SELECT a AS k … GROUP BY k` 报 "Aggregate function COUNT not yet implemented" | apply_group_by 的别名匹配只认 Expr 别名, 漏 ColumnWithAlias → 分组键解析失败 | 列别名先映射回底层列名再解析 |
| 3 | `SELECT b % 3 … GROUP BY b % 3` 报 "must be in GROUP BY" | 非聚合 FunctionCall 组键有"代表行求值"分支, BinaryOp 没有 | BinaryOp/UnaryOp/CASE 纯非聚合表达式同走代表行求值 (SQLite 语义) |
| 4 | `CONCAT(NULL, '!')` 返回 NULL (SQLite/PG 返回 '!'); INSTR 未实现 (列上静默 NULL) | CONCAT 按 \|\| 语义传播 NULL; INSTR 缺失 (两条快路径的名字列表里有 concat 各一处) | CONCAT 三处统一跳过 NULL 参数 (\|\| 保持传播); INSTR(hay, needle) 1-based、未命中 0、NULL→NULL |

回归: test_round13_bug_hunt.rs 新增 3 例 (13/13) + 入库 harness 含新形状。
`GROUP BY UPPER(a)` 等函数形式此前已支持 (TIME_BUCKET 同路径)。
多列 IN `(a,b) IN ((…))` 仍不支持 (明确报 parse error, 非静默错误) — 记录为
特性缺口。

### Round 13b 后性能复核 (无回退)

Round 13/13b 改动多在 executor 热路径, 用 compete_bench 同批跑 mote+sqlite
(sqlite 作负载控制) 对照 R12 表:

| workload (p50) | R12 | R13b | sqlite 同批 (R12→现在) |
|---|---|---|---|
| range COUNT+AVG | 0.79 ms | 0.758 ms | 0.47 → 0.39 |
| equi-JOIN + GROUP BY | 18.9 ms | 16.68 ms | 11.9 → 10.9 |
| GROUP BY device | 2.1 ms | 1.57 ms | 75.4 → 58.1 |
| top-k ORDER LIMIT 10 | 0.53 ms | 0.457 ms | 53.6 → 45.7 |
| vector knn@10 | 7.9 ms | 6.5 ms | 22.4 → 18.4 |
| PK 点查 | 21 µs | 18 µs | 7 → 7 |
| DB size | 186.5 MB | 186.5 MB | — |

全部持平或更好。`query_peak_rss_mb` 三次复跑 24.8 / 63.5 / 92.4 MB — 该
指标 (reopen 后首个 knn 的解码峰值, jemalloc 保留时序) 本身高方差, R12 的
44 在同一分布内, 不构成回退信号 (knn 延迟与磁盘大小完全稳定)。

改动路径专项延迟 (@100K 行): IN 子查询 (重写为变异可见扫描) 0.72 ms;
LATEST BY 100 组 @50K 13.3 ms (物化路径结构未变); 新能力 GROUP BY 表达式
`id % 100` 73.6 ms — 走物化 SqlRow 路径 (对照组普通列 GROUP BY 1.39 ms
走融合快路径), 与 SQLite 自身的 group-by 形状 (~58-60 ms) 同量级; 这是新
功能的首版成本而非回退, 位置化表达式求值记为后续优化项。

## Round 13c — 资源消耗全面测评 (v0.10.0) + 两个资源黑洞修复

`resource_bench.py` (新入库): 100K 行 × (ts/dev/val/text/384 维向量) 数据集,
psutil 50ms 峰值采样, 查询内存取 3 窗口中位数 (jemalloc 保留使单采样高方差)。

### 资源画像 (v0.10.0, M-series, 100K×384)

| 维度 | 数值 |
|---|---|
| 工件 | CLI 7.0 MB / Python .so 6.5 MB |
| 空库 open | +2.3 MB RSS |
| 加载 | 62K rows/s (1.6s); 引擎侧峰值 Δ 211 MB; WAL 332 MB → checkpoint 0.34s → **162 MB** (回收 51%) |
| FTS 索引构建 | 3.4s, +5.5 MB RSS, +14 MB 盘 |
| 查询内存 (峰值Δ中位) | 点查/范围聚合/GROUP BY/top-k/FTS/knn 全部 **≈0**; join 63 MB (62 万行物化); 5 万行投影扫描 1.1 MB |
| steady-state (缓存填满) | +160 MB (64MB 通用 + 向量解码预算) |
| 重开 | 19 ms; **crash 恢复** (kill -9 → WAL 重放) 12 ms, 1300/1300 行可见 |
| edge preset | 盘 157 MB (zstd); knn 流式 26 ms (默认 6 ms — 32MB 预算下内存换时间), 首查 RSS Δ ≈ 0 |

SQLite 参照 (同形状无向量列): 盘 12.8 MB / 全查询 RSS ≈ 0 — 磁盘对等比较见
compete_bench (186.5 vs 212.6 MB, 双方含向量)。

### 测评挖出并修复的两个资源黑洞

1. **join WHERE 无谓词下推** — `a JOIN b ON k WHERE a.id≤N AND b.id≤M` 先
   物化全表叉积再过滤: 5K×500 自 join 曾 **11.4 s + 5.2 GB RSS** (1.56 亿
   中间行)。修复: AND 链中 `alias.col op literal` 按表前缀下推进两侧投影
   扫描 (多路 join + 2 表 hash 路径; OR/NOT/LIKE/IS NULL 等留在 join 后
   过滤, NULL 三值语义由 apply_op_value 保证)。修复后同查询 **20.2 ms +
   7.3 MB** (567×/700×)。
2. **单 BETWEEN 聚合未进融合路径** — `WHERE id BETWEEN a AND b` 的
   COUNT+AVG 走物化扫描: 88 ms + 312 MB (80% 范围 @100K)。修复:
   parse_where_comparisons 把 BETWEEN 折叠为 `>= AND <=` 两个比较
   (NOT BETWEEN 是 OR 语义, 正确回落通用路径)。修复后 **0.78 ms + 0 MB**。

回归: test_round13_bug_hunt.rs 新增 join 下推选择性/NULL/UPDATE 可见性
回归 (14/14) + fuzz tier-1 6 seed 全绿 (BETWEEN/下推语义对拍 SQLite)。

## Round 13d — 规模热点扫描: 四个资源黑洞全部修复

100K 行 × (id/ts/dev/cat/val/note/32 维向量) 热点扫描 (38 形状), 时间
>100ms 或 RSS >100MB 标红。聚合/排序/分页/标量函数/LIKE/IN/子查询/投影/
knn/FTS 全部正常 (最高 count-distinct 50ms、全表投影 86ms)。四个黑洞:

| # | 形状 | 修复前 | 修复 | 修复后 |
|---|---|---|---|---|
| 1 | 三表 join 计数 (ON 带 `b.id<=N` 合取) | **20min+ 跑不完** | ON 合取拆分: 等值对走 hash, 新表单表残余预过滤扫描, 跨表残余 probe 循环对合并行求值 (多路 join 接受合取 ON, 不再 decline 到通用嵌套循环) | **201 ms** |
| 2 | LEFT JOIN 反连接 (`ON … AND b.id<=50 WHERE b.id IS NULL`) | **20min+ 跑不完** | inner/left join 同样拆合取: 右表单表残余预过滤后走 hash; 残余未全部消耗时保持嵌套循环逐候选 eval (残余为空才允许纯等值 hash — 曾丢残余条件, fuzz 抓出) | **376 ms** |
| 3 | UPDATE 多行 (10% 行 @100K) | **39.4 s** (每行一次组提交 fsync 等待 ~3.2ms) | 语句级批量: 逐行落缓冲/墓碑/缓存, WAL 全部 deferred 入队, 语句末一次 `wal_group_barrier` | **138 ms (285×)** |
| 4 | DELETE 多行 | 同上 4ms/行 | 同批量模式 | 301 行 1.2s→76ms (16×) |

语义与持久化验证:
- ON 残余跨表条件 (`t2.tag = t.tag` 自 join) 曾被 hash 快路径丢弃 — 修复 +
  Rust 回归 (test_round13_bug_hunt 15/15) + fuzz 8 seed 全绿 (自 join 生成器
  覆盖此形状)
- 批量变异 crash 恢复: 5000 行表批量 UPDATE×2 + DELETE + 子进程再变异后
  kill -9 → 重开 WAL 重放零丢失 (5/11 倍数残留 0、全部变异可见)
- UPDATE 语句语义变化: 校验/求值失败的行现在使整条语句写入前失败 (旧的
  "写一半再报错" 更接近原子, 但非事务内仍非原子 — 与 SQLite 相同)

## Round 13e — 扫描#2: GROUP BY 表达式丢键修复 + 快路径 + 计算键 hash join

DDL/大事务/恢复全部健康 (CREATE INDEX 22ms、ALTER 28ms、单事务 30K 插入
107ms、回滚 95ms、大 WAL 重开 38ms)。挖出并修复:

| # | 症状 | 根因 | 修复 |
|---|---|---|---|
| 1 | `GROUP BY id%3, id%5` (双 canonical 表达式) 返回 **3 组而非 15 组** — 正确性 bug | apply_group_by 的无别名 SELECT 表达式匹配任何组项 (`alias.is_none() \|\| …` 短路了 canonical 相等) → 两个组键都解析到第一个表达式 | 无别名表达式必须 canonical 名相等; 别名按字面匹配 |
| 2 | GROUP BY 表达式 20K 行 1.8s (有未合并写 35µs/行; 列 GROUP BY 0.26ms) | 表达式组键让位置化快路径 decline → SqlRow 物化路径 | 新 `try_expression_group_by` 快路径: 流式行 + eval_expr_on_row 求键 + 单遍累加 (COUNT/SUM/AVG/MIN/MAX, NULL 语义对齐, ≤2 键, 别名/输出 ORDER BY, LIMIT) |
| 3 | 非等值 ON `a.id = b.id - 1` 2K×20K **23.5 min** | 无等值对 → O(N×M) 嵌套循环逐候选建 SqlRow eval | 新 `try_expr_key_hash_join` 计算键 hash: 单表表达式侧逐行求值建 hash、列侧探测 (`a.id*2=b.id` 表达式在左同样归一) → **40.8 ms (34,500×)** |

已知限制 (记录待办): 双侧表达式 ON (`UPPER(a.dev)=UPPER(b.dev)`) 无法单侧
建 hash → 仍嵌套循环 (正确但慢); 无 WHERE 全乘积 join 的 COUNT 折叠
(20K×20K 9 亿结果行 144s — COUNT(*) 可不物化直接折叠); `INSERT … SELECT`、
`FROM (SELECT …)` 子查询形态不支持。

回归: round13 Rust 回归 17/17 (新增双表达式组键 + 计算键 join) + fuzz 8
seed 全绿 + GROUP BY 表达式差分 10/10 + 非等值 ON 差分 5/5 + 全量 229 bin
EXIT=0 + E2E 51/51。

## VEC M4 — 排序/输出边界: top-k 缺口五连修 (100K×384 ev 表)

`sample` 采样定位三条验收线的热点后发现: 全表投影 46.5ms 中 **Python 边界
(dict 构造+释放) 占 ~60-80%、存储扫描仅 ~19%**; `ORDER BY ts LIMIT k` 慢 22×
不是扫描慢, 是快路径的 `is_numeric` 白名单漏了 Timestamp; 深分页慢是整序后
丢弃。修复:

| # | 形状 (@100K) | 前 | 后 | 修复 |
|---|---|---|---|---|
| 1 | `ORDER BY ts DESC LIMIT 10` | 11.7ms | **0.49ms** (24×) | top-k 快路径 is_numeric 补 Timestamp (i64 micros 本就是定宽) |
| 2 | `ORDER BY id LIMIT 100 OFFSET 99800` | 21.7ms | **4.5ms** (4.8×, 目标"减半") | top-k 支持 OFFSET: k=offset+limit 有界选择, 只解码最终页 |
| 3 | `WHERE ts>=? ORDER BY ts LIMIT 100` | 22.5ms | **0.69ms** (33×) | `try_vec_filter_topk` (MOTE_VEC=on): 批谓词 + typed 键 select_nth, 只解码 K 行 |
| 4 | `SELECT 5 列` 全表 (execute dicts) | 46.5ms | **27ms** (1.7×) | `try_vec_projection` 批投影 (Rust 扫描 ~10→4ms) + Python 边界重写 |
| 5 | top-k val (基准线) | 0.52ms | 0.53ms | 持平 ✓ |

Python 边界重写 (bindings, 不受 MOTE_VEC 门控, 所有 execute()/query() 受益):
- 列名 PyString **每查询创建一次**并复用 — 旧行为每 (行,列) 一次
  PyUnicode_New + str hash, 100K×5 结果 = 500K 次冗余构造 (CPython 把 hash
  缓存在对象内, 复用 key 对象即免重复 hash)
- TEXT 值驻留缓存 (FxHash, 上限 8192 项): 低基数列 (device/enum) 千行映射
  到 handful 个 PyString; 唯一值列只付一次 hash 查找

`SELECT 5 列` 27ms 已近 dict-per-row API 地板 (纯 CPython 构造 100K×5 键
dict = 11.6ms + 400K 个值对象创建 ~15ms); `db.query()` 元组路径同 26.6ms —
差异已被值转换成本吞没。大结果如需更低延迟需列式返回 API (后续另议)。

验证: A/B 16 形状 (vec on/off) 行集+顺序全等 + test_vec_m4 4/4 (投影/top-k
对拍 SQL、Timestamp 类型、墓碑 decline) + fuzz 4 seed × (on/off) + bigtable
重开 reopen_diverge=0 (on) + E2E 51/51 + CLI 17/17 + ACID 22/22。

## VEC M5 — 并行里程碑: 三雷拆除 + morsel 并行聚合/join

1M 行 (10 核 M 系列) 的三个 decline 黑洞先修 (并行一条会 decline 的路径
毫无意义), 再并行:

| 形状 @1M | 旧路径 | M5 串行 | M5 并行 | 总加速 |
|---|---|---|---|---|
| GROUP BY 文本键 + ORDER BY | 146.8ms | 42ms (M2 接管普通列键+ORDER) | **4.8ms** | **30×** |
| GROUP BY + AVG + ORDER BY | 164.6ms | 43ms | **5.6ms** | 29× |
| JOIN + GROUP BY build 侧键 (s.zone) | 187ms | 42ms (BKey 扩展) | **8.3ms** | **22×** |
| JOIN + AVG + GROUP BY | 199ms | 78ms | **8.9ms** | 22× |
| bench 形状 JOIN (e.device 键) | 17.6ms | 17.6ms | **3.6ms** | 4.8× (验收 2-3×) |

三处修复:
1. **M2 接管普通列键 + ORDER BY/LIMIT 形状**: col_segment_group_by 一见
   ORDER BY/LIMIT 就整体 decline → 全物化+全排序 (排序 64 个组竟要 147ms)。
   M2 的组输出 ORDER BY 尾部只排组数行。无 ORDER 的纯列键仍走 &str 零分配
   路径 (不截胡)。
2. **M3 build 侧组键 (GROUP BY s.zone)**: build 表 HashMap 值扩为
   (匹配数, 组值), probe 命中折叠进组值分组; 同 join 键跨组值 → decline
   (PK 维度表不触发)。NULL 组值归 NULL 组 (SQL 语义)。AVG 乘法折叠本就
   正确 (for _ in 0..matches 折 sum+count)。
3. **雷#1 params 竞态拆除**: 绑定参数从 executor 共享 RwLock 改 thread-local
   (与 CURRENT_TXN_ID 同模型) — 多线程共用 Database 时 bind→execute→clear
   窗口互相踩踏 (A 拿到 B 的参数=静默错行, 或参数被清空报错)。竞态压测
   (8 线程 × 500 参数化点查) 修复前 FAIL / 修复后 PASS。

morsel 并行 (M2/M3, MOTE_VEC=on, ≥200K 行):
- 行按 chunk 分 rayon 线程 (chunk 数 = 线程数封顶 16), 各建 partial 组表,
  主线程 VecAcc::merge 合并 (CompSum::merge 保 Neumaier 精度)
- 🔑 教训 ×2: (a) `par_chunk_count` 曾把下界写成上界 → nchunks=n, 每 chunk
  1 行, rayon 被百万微型任务淹没 (并行比串行慢 25×, 全线程卡 join 调度 —
  sample 的 self-time 全在 rayon plumbing); (b) M2 通用路径每行一次
  Vec<Value> 键分配, 并行时 16 线程在分配器锁上踩踏 → 单键一律 Value 键
  (克隆 = Arc 计数/POD, 零堆分配)
- 雷#2/#3 (CURRENT_TXN_ID / memo TLS): vec 批路径不读 TLS 状态 + 事务内
  decline + 谓词纯字面量 — 工作线程不触碰雷区, 并发读压测无错果

验证: A/B 12 形状 250K 行 (并行参与) 全等 (ORDER 精确序 / 无 ORDER 多重集)
+ 竞态压测 2/2 + fuzz 4 seed × (on/off) + bigtable 重开 diverge=0 + E2E
51/51 + CLI 17/17 + edge (no-rayon) profile 编译通过。

## VEC M6 — 收编清理: 默认开启 + 六语义 bug + executor 拆模块

**MOTE_VEC 默认开启** (MOTE_VEC=off 一键回全旧路径)。M1-M5 默认关闭的唯一
阻塞 (事务回滚 undo 双写段发散) 已被三重保守门完整掩蔽, 转默认开后 232 bin
全量成为 vec 审计面, 暴露并修复 6 个语义 bug:

| # | bug | 抓出者 |
|---|---|---|
| 1 | SUM 整数溢出静默回绕 (wrapping_add) → checked_add 提升 Float | v27 |
| 2 | `WHERE ts = 'ISO串'` 恒 false (I64 vs Text 类型化比较无强转) → 编译期预解析 | v53 |
| 3 | MIN(ts)/MAX(ts) 返回 Integer → ts 标志还原 Timestamp | v62 |
| 4 | SUM(BOOLEAN) = 0 → true→1 数值累加 | v86 |
| 5 | 🔴 点查缓存解码对 CachedCol::Batch 全返 NULL → UPDATE 旧行全 NULL | no_bloat |
| 6 | 🔴 `GROUP BY departments.name` 错解析到探测表 employees.name (带前缀禁止 bare 回退) | sql_joins |

executor.rs (30,086 行) 拆模块 (纯代码移动, pub(super) + 子模块可见父私有项,
零可见性风暴, 一次编译通过):

| 文件 | 行数 | 内容 |
|---|---|---|
| executor/mod.rs | 22,327 | 核心分发 + DML/DDL/表达式/流式结果 |
| executor/scan.rs | 2,701 | 全表扫描/col-segment 扫描/事务合并/投影扫描 |
| executor/agg.rs | 2,378 | 聚合下推/多聚合/GROUP BY 下推 + WHERE 解析 |
| executor/join.rs | 2,696 | multi-way/positional/hash/expr-key join + 左右全外 |

fast path 收编说明: 静态扫描零死代码 (全部私有 fn 有引用); 旧 fast path
保留为 MOTE_VEC=off 灭火开关的回退路径, 不做破坏性删除 — 默认开后它们
仅在 decline 形状 (事务/墓碑/多段/复杂类型) 上服务。

验证: 拆分前后 232 bin 全绿 EXIT=0 + fuzz 4 seed × (默认开/off) + bigtable
重开 0 发散 + E2E 51/51 + CLI 17/17 + compete_bench 无回退 (join 1.70ms,
groupby 1.65ms)。

## 导入吞吐战役 — WAL 微压缩黑洞 + insert_arrays 列式 API

计划验收表的 ≥200K 行/s 目标: 采样定位到三层问题, 修掉两层, 第三层
(核心插入引擎) 是另一场战役, 如实记录:

1. **WAL 微压缩黑洞**: WAL_COMPRESS_THRESHOLD 曾是 128B — 1.6KB 的行级
   WAL 记录全部走压缩检查, 100K 行导入 = 20 万次微型 zstd (每次都建
   Huffman/FSE 表) ≈ 0.6s 纯开销。→ 阈值 4KB + 32KB×2 前缀采样预检 +
   值得压缩门槛 10%→25% (WAL 短命, 边际压缩比不配热写路径吃 CPU)。
   注: 行交错序列化让采样无法分辨 (文本恒可压), 门槛提高才是主效。
2. **`db.insert_arrays(table, {列: numpy 数组/列表})`** 列式批量插入:
   numpy (i64/f64/f32/2D-f32 → 向量列/<U unicode) 经 tobytes 一次 memcpy
   解码, str/int/float 列表走同构批量 C-API 提取; 跳过 SQL 解析与逐行
   Python 对象。executemany 的 Python 侧天花板实测 124K 行/s (每行
   .tolist() 建 384 个 float 对象 = 0.8s), 此 API 是列式数据正解。
3. **TIMESTAMP 强转 bug** (insert_arrays 对拍抓出): 直通批量路径的裸
   Integer micros 未包成 Value::Timestamp → 编码落 0 (读回全 0)。
   batch_insert 层补 Integer→Timestamp 强转 (SQL 路径在求值层已有)。

吞吐 (100K×384, 同机噪声 ±20%): executemany 33-42K; insert_arrays
numpy 列式 43-54K。**未达标**: 核心插入引擎天花板实测 ~250K 行/s
(0.4s/100K = WAL 序列化+行编码+段构建), 提取侧零成本也压不进 200K 墙内
—— 需 WAL 序列化/行编码/段构建的专项优化 (记录待办)。单发大调用反而更
慢 (35.8K vs 分块 53.9K), 非每批 fsync 瓶颈。

正确性: test_insert_arrays.py 10/10 (标量/NULL/向量/unicode 往返/
executemany 聚合对拍/重开一致) + fuzz 4 seed × (on/off) + E2E 51/51 +
全量 232 bin EXIT=0。

## 插入引擎专项 — 天花板解剖: 根因不在引擎, 在 pyo3 提取层 (3 bug)

上节记录的 "核心插入引擎 ~250K 行/s 天花板" 解剖后推翻: 纯 Rust 基准
(`examples/bench_insert_engine.rs`, 100K×384) 实测 **2.1-2.5M rows/s**
(含向量数据, 默认 group-commit 配置), close 落盘 0.13s。所谓引擎天花板
是 Python 侧测量的误归因。sample 剖析 Python 进程: insert_arrays 窗口
95% 时间在 Python→Rust 边界提取, 真正的 DB 写入只占 ~2%。

1. **tobytes→Vec<u8> 逐字节 PyLong (20× 黑洞)**: `extract::<Vec<u8>>()`
   把 bytes 对象当通用 Python 序列 — 每字节建一个 PyLong 再转回 u8,
   100K×384 = 1.5 亿次。修复: downcast `PyBytes` + `as_bytes()` 借切片
   (零拷贝零对象)。insert 窗口 1300ms → 70ms。
2. **insert_arrays 字典序转置 = 静默数据损毁**: 旧实现按字典键序转置成
   行, 与 schema 列序无关 — 字典序 ≠ schema 序时值落错列 (TEXT 列收到
   Float 被清成空串); 省略前导自增 PK 时 `row[pk_pos]=auto_id` 覆盖首列
   真实值, 100 行全毁且无报错。修复: 绑定层先取 `db.table_columns()`
   (新增 api), 值按 schema 位置放置, 缺失列 = NULL (自增 PK 的 NULL 由
   引擎填 auto id, 两条路径一致); 未知列显式报错。
3. **fast_batch_insert 静默丢弃显式自增 PK**: SQL 层与慢路径都保留
   `INSERT INTO t (id,…) VALUES (100,…)` 的显式 id (值即 row id + 唯一性
   检查), 唯独 ≥100 行快路径用 counter id 覆盖。修复: 批内任一行带非
   NULL PK → 走全路径 (SQL 批量导入省略 id → 仍走快路径, 无性能回归)。

吞吐 (100K×384, 数据预生成, 同机噪声 ±20%): **insert_arrays 311-385K
rows/s** (原 43-54K, 计划验收线 ≥200K 超 1.8 倍); executemany 119-132K
(.tolist() Python 侧天花板)。400K 行持续 296K rows/s。

正确性: test_insert_arrays.py 15/15 (原 10 + 按位放置/字典序/省略 PK
快慢两路径/省略非 PK 列 NULL/未知列报错/显式 PK 大批保留) +
tests/test_insert_engine.rs 3/3 (Rust 侧对拍) + fuzz 3 seed × (on/off) +
bigtable reopen_diverge=0 ×3 + E2E 51/51 + CLI 17/17。

## M1/M4 路径并行 + GROUP BY 首查询错果修复 (vec-parallel 收尾)

M5 只并行了 M2/M3; 本轮补 M1 (无组聚合) / M4 (过滤 top-k), 并在采样中
挖出一个**主分支现行静默错果 bug**:

1. **M1 morsel 并行** (`try_vec_no_group_aggregate`): 可见集/去重顺序算好
   后, 行折叠按 chunk 分 rayon 线程 (三种谓词形状: 无/AND 链/混合 OR —
   混合形状整段先算一次谓词集, chunk 内只做 contains), partial VecAcc
   主线程 merge (MIN/MAX 可交换, 和走 CompSum::merge 保 Neumaier)。
2. **M4 morsel 并行** (`try_vec_filter_topk`): 谓词过滤 + 有序键提取
   (提取为公共 `topk_ord_key`) 按 chunk 并行, entries 拼接后 select_nth
   仍顺序 (k 有界)。
3. **M2 大表截胡修正**: 无 ORDER 纯列键 GROUP BY 曾无条件让位 &str 零分
   配路径 — 1M 行实测 &str 串行 14.3ms vs M2 并行 4.8ms, "不截胡"在大表
   上是负优化。现在 ≥PARALLEL_MIN_ROWS(200K) 走 M2, 小表仍 &str。
4. **去重条件精确化** (M1/M4/M2): `segments.len() > 1` →
   `may_have_duplicate_keys()` — 纯插入多段 (row_id 唯一 ⇒ key 唯一)
   不再强制 keys 加载 + HashSet 去重 (~3ms/100K 行); overlap_possible
   (UPDATE/DELETE 置位, 重开 2+ 段保守置位, 全量合并清除) 才去重。
5. **🚨 col_segment_group_by 慢路径错果 (主分支现行 bug)**: 多段表上
   GROUP BY 是首个需要 keys 的查询时, dedup 的 `row_map.key(i)` 在 keys
   未加载时回退到**栅栏键** (每 fence_interval≈2048 行一个) — 每 2048 行
   被当成同 key, seen 集合只留首行: **100K 行 GROUP BY 只剩 50 行/32 组,
   静默错果**。全量套件没抓到是因为测试流程里总有前置查询先把 keys 载入
   缓存。修复: need_dedup 时先 `load_full_keys()`; 回归测试
   `test_groupby_first_query` (GROUP BY 作为首查询)。顺手修掉两阶段快路径
   `row_groups: Vec<u16>` 的 65535 组截断。

吞吐 @1M (基准 `bench/prof_vec_parallel.py`, p50):

| 形状 | 前 | 后 | 加速 |
|---|---|---|---|
| A range COUNT+AVG (M1) | 7.72ms | **1.66ms** | 4.7× |
| B filter top-k (M4) | 5.81ms | **2.12ms** | 2.7× |
| C 深分页 top-k (M4) | 3.85ms | **1.81ms** | 2.1× |
| D GROUP BY 无 ORDER | 14.34ms | **4.85ms** | 3.0× (M2 接管) |
| E GROUP BY+ORDER (对照) | 4.85ms | 4.81ms | 持平 |

@100K (低于并行门槛, 受益于去重精确化): A 3.39→**0.78** / B 3.37→**0.63**
/ C 3.20→**0.42ms**。D &str 路径 2.4-2.5ms (计划 <0.8ms 目标仍开放 — 需
并行门槛下调或 &str 路径并行化, 记录待办)。

验证: A/B 对拍 11/11 (250K 行, MOTE_VEC on/off 全等, 含墓碑+多段) +
test_groupby_first_query + fuzz 3 seed × (on/off) + bigtable
reopen_diverge=0 + E2E 51/51 + CLI 17/17 + insert_arrays 15/15 + 全量套件。

## 并行门槛拆分 — VEC 计划查询目标全部收口

上节的 100K 表 (批量导入后 2×50K 段, checkpoint 不合并小段) 暴露门槛
粒度问题: 并行判据是**每段**行数, 100K 门槛对多段小段永不触发。拆两级:

- `PARALLEL_MIN_ROWS = 100K` (路由级): 无 ORDER 纯列键 GROUP BY 让位 &str
  还是走 M2 的分界 (跨段累计行数)。
- `PARALLEL_MORSEL_MIN_ROWS = 20K` (段内折叠级): M1/M2/M3/M4 的 par_chunks
  判据。校准: 单次 par_chunks (≤16 chunk) 调度+merge ~0.1ms, 20K 行折叠
  工作 ≥0.5ms 仍有净收益; 更小查询不进并行分支零开销。实测 @20K 表
  GROUP BY 0.33ms / @50K 0.79ms — 全尺寸赢无反伤。

compete_bench (官方 100K×384 口径, p50):

| 指标 | 计划目标 | R13 基线 | 现在 | 状态 |
|---|---|---|---|---|
| GROUP BY device | <0.8ms | 1.57ms | **0.713ms** | ✅ |
| range COUNT+AVG | <0.4ms | 0.76ms | **0.185ms** | ✅ |
| equi-JOIN + GROUP BY | <4ms | 16.7ms | **0.441ms** | ✅ (超 37×) |
| top-k ORDER LIMIT | 持平 | 0.457ms | 0.452ms | ✅ |
| PK 点查 | — | 18µs | 17µs | ✅ |
| 批量导入 (insert_arrays, 384 维口径) | ≥200K/s | 62K | 311-385K/s | ✅ |

VEC 计划 (M0-M6 + 后续并行收尾) 的全部量化目标至此收口。注: 本表导入
行为 insert_arrays 列式口径; compete_bench 的 load_rows_per_s 仍走
executemany (Python 侧 .tolist() 天花板 ~124K), 口径不同未列入。

验证: A/B 对拍 11/11 + fuzz 3 seed × (on/off) + bigtable diverge=0 +
E2E 51/51 + CLI 17/17 + insert_arrays 15/15 + test_groupby_first_query
+ 全量套件 + edge (no-rayon) 编译。

## 全乘积 COUNT 折叠 — 6 亿对 144s → 0.5ms

Round 13e 记录的待办: 无跨表约束 join 的 `COUNT(*)` 走通用路径 — 双侧物化
SqlRow + 每对 combine_rows 建 HashMap + eval, 20K×30K (6 亿对) 物化 144s。
新 `try_join_count_fold` (挂在聚合分发块顶部): 当每步 ON 都是 (a) 常量表
达式 (true 继续 / falsy → 0, 含 NULL 比较 UNKNOWN→false 同通用路径) 或
(b) 只引用单表的 `prefix.col op literal` 谓词, 且 WHERE 严格全分解为单表
谓词 (extract_pushdown_preds 会静默丢弃不匹配叶 — 折叠路径自数 AND 叶,
叶数不符即 decline) 时, INNER join 计数因式分解:

    COUNT(*) = Π 各表 (谓词过滤后) 行数

无谓词表走 O(1) 原子计数器 (INSERT++/DELETE--), 有谓词表投影扫描过滤。
跨表/等值 ON decline — 等值已有 hash 路径 (对拍无回退); 事务内 decline
(read-your-writes 留给通用路径)。

| 形状 (20K×30K) | 前 | 后 |
|---|---|---|
| `JOIN b ON 1=1` COUNT(*) | 144s 级 | **0.5ms** (O(1)) |
| `JOIN b ON a.x >= 0` (单表谓词) | 分钟级 | **8.5ms** (O(N)) |
| `ON 1=1 WHERE a.x < 5` | 分钟级 | **1.9ms** |
| `ON a.x = b.y` (等值, decline 对照) | — | 21.8ms 持平 |

溢出语义: 乘积超 i64::MAX 饱和 (旧路径 u64 计数同界)。

验证: test_join_count_fold 8/8 (常量真/假、NULL、单表谓词双表侧、
WHERE×ON 叠加、三表链、空表、DELETE 墓碑、跨表 decline 手工对拍) +
fuzz 3 seed × (on/off) + bigtable diverge=0 + E2E 51 + CLI 17 +
insert_arrays 15 + parallel_ab 11 + 全量套件。

## INSERT ... SELECT — SQL 缺口补齐 (500K 行 1.59M rows/s)

Round 13e 记录的待办: `INSERT INTO t ... SELECT ...` 解析层就不支持
("Expected Values")。补齐全链路:

- **AST**: InsertStmt 增 `select: Option<Box<SelectStmt>>` (与 values 互斥)。
- **解析器**: 列清单后接受 SELECT 源 (ON CONFLICT 后缀语法照旧解析,
  但与 SELECT 组合在执行层显式报错 — 阶段一不支持)。
- **执行器** (execute_insert_ref 顶部): `execute_select_internal` 物化
  SELECT 行 (**内部路径不受 max_result_rows 截断** — 子查询同款; 避免
  大 INSERT..SELECT 被静默截断), 行已是求值好的 Values, 按 columns
  (或无列清单时的 schema 全宽 — 数量不符显式报错, 不静默 NULL 填充)
  经 values_to_row_by_columns 建行后走**同一插入管线** (batch WAL/事务
  缓冲/索引/last_insert_id)。TimeSeries 目标表同样支持 (columnar ingest);
  自插因先物化后插入无无限循环; 事务内 ROLLBACK 干净。
- max_parameter_index walker 补 SELECT 源参数扫描。

吞吐: 500K 行 (4 列) INSERT..SELECT 315ms = **1.59M rows/s**, 校验和
对拍一致 — 与原生 batch 插入同量级 (物化 + 批量写两趟)。

验证: test_insert_select 11/11 (基本形状/表达式投影/无列序全宽与报错/
自插翻倍对拍/ORDER LIMIT 源/JOIN 源/ON CONFLICT 显式错误/事务回滚与
提交/参数化/TS 表/重开一致/500K 批量) + fuzz 2seed×(on/off) + bigtable
diverge=0 + E2E 51 + CLI 17 + insert_arrays 15 + parallel_ab 11 + 全量套件。

## fetch_arrays — 列式 Python 返回 API (numpy 零拷贝)

VEC 计划表外的最后一项记录候选: 查询结果列式直接返回 numpy, 免逐行
dict 拼装。新 `db.fetch_arrays(sql, params=None)` → `(columns, {列名:
numpy 数组 | Python 列表})`:

- 同质无 NULL 列单遍分类后直接 `np.frombuffer` 零拷贝 (Rust 侧 lazy
  `import numpy` — sys.modules 命中 ~µs; 缺 numpy / 调用失败回退 bytes
  对象, 用户可自行 frombuffer):
  INTEGER / TIMESTAMP → `<i8` (Timestamp 为 micros, 同 execute() 语义)
  FLOAT → `<f8`; BOOLEAN → `bool`
- TEXT 列 → str 列表 (复用驻留缓存); 含 NULL / 混合类型 / VECTOR /
  SPATIAL 列 → 逐值 Python 对象列表 (None 表示 NULL) — 不做 NaN 假 NULL。
- 只读数组 (frombuffer 语义); 需要 writable 用户侧 `.copy()`。

吞吐 (100K×5 列, WHERE 过滤后 ~90K 行): execute() 逐行 dict 20.4ms /
query() 元组 17.6ms / **fetch_arrays 8.7ms** (2.3×)。分析负载
(df = pd.DataFrame(dict(arrays)) / 直接送 numpy kernel) 的正解。

验证: test_fetch_arrays 16/16 (五列类型映射/dtype、与 execute() 全行
对拍、Text/Vector 列表、NULL 回退、参数化、空结果、非 SELECT 报错、
LIMIT OFFSET、重开一致) + E2E 51 + CLI 17 + insert_arrays 15 +
parallel_ab 11 + fuzz 2seed×(on/off) + bigtable diverge=0 + 全量套件
(Rust 侧无改动, 套件确认)。

## knn 精确扫描并行 — 官方 bench 最慢查询 6.05 → 4.66ms (带宽地板)

瓶颈扫描定位: compete_bench 最慢查询是无索引 `ORDER BY emb <-> ? LIMIT 10`
精确扫描 (100K×384 = 153.6MB/查, 串行 SIMD)。两层并行化:

1. **段内 morsel 并行** (≥20K 行大段, checkpoint 合并后的单段形态):
   行 chunk 分 rayon 线程, 各建本地 top-k 堆 (MAX-heap peek=最差, 严格
   更近才替换 — 同串行语义), prior-seen 只读快照 + 段后合并键声明 —
   跨段 newest-wins 去重序逐位保持。缓存/流式两条读取路径都覆盖。
2. **跨段并行** (≥2 段的段阵 — 批量导入后 4MB flush 的常态, 段内 20K
   门槛对 ~5K 小段永不触发): 每段一个 rayon 任务。**跨段无重复键判据**
   用段键区间两两不相交 (段键升序存储, row_map.key(0) fence 边界精确 +
   last_key_hint 均 O(1)) — UPDATE 重写同 key 的新版本必然落进与旧段
   重叠的区间 → 判据失败 → 保守走顺序 newest-wins 路径。重开时保守置位
   的 overlap_possible 不再误伤纯插入段阵。缓存/预算决策与缓存填充保持
   串行 (RSS 语义不变), 流式段的 8MB 分块读取在各段任务内并行。

吞吐: knn10 p50 **6.05 → 4.66ms** (1.3×)。到此是**内存带宽地板**:
153.6MB 精确扫描 @ ~33GB/s — 再快需要近似索引 (CREATE VECTOR INDEX
已有 DiskANN/HNSW 路径) 而非更快的精确扫描。其余形状全部持平
(point 17µs / range 0.20 / groupby 0.80 / join 0.45 / topk 0.48 / fts 0.14ms)。

教训 (测试侧): 差分数据生成器用 fract((i*K+d*C)*φ) 线性型在 8 维下产生
大量 f32 余弦恰为 0.0 的近平行向量 (平局任意序, 双方都是合法答案) —
换 splitmix64 (24-bit 均匀分量) 才能做确定性差分。

验证: test_knn_parallel (60K×8 两段, L2+cosine 三查询点与测试内暴力
对拍 + UPDATE 500 行 newest-wins + DELETE 700 行无幽灵不缺行 + NULL
向量不参与) + E2E 51 + CLI 17 + insert_arrays 15 + parallel_ab 11 +
fetch_arrays 16 + fuzz 2seed×(on/off) + bigtable diverge=0 + 全量套件。

## compete_bench 加载口径切换 insert_arrays — 官方口径 62K → 152K rows/s

官方基准的加载路径从 executemany (SQL 参数绑定, Python 侧逐行对象构造 +
.tolist() 每 384 浮点建列表) 切到 `db.insert_arrays` 列式批量 API:

- **load_rows_per_s: ~62K → 151,674** (2.5×); load_peak_rss 830 → 710MB。
- id 语义不变 (0..N-1 显式 PK 走 full path 校验+唯一性, q_point 依赖)。
- 全部查询形状与切换前一致 (knn 4.77 / point 17µs / groupby 0.87 /
  join 0.47 / topk 0.50 / fts 0.14ms) — 数据正确性由基准自身查询隐式
  验证。
- 口径说明: 各引擎用各自的批量导入正解 (sqlite executemany / duckdb
  原生 / mote insert_arrays)。executemany 口径的历史数字 (48-62K) 留在
  本文件历史章节 — 那是 SQL 绑定路径的 Python 侧天花板, 不是引擎上限;
  无显式 PK 的自增表 (fast path) 为 311-385K rows/s (fb593a2 章节)。

## 查询内存画像收口 — 默认向量缓存预算 256→64MB (首查 knn RSS +2MB)

用户约束: 查询期 RSS 峰值 ≤100MB (嵌入式定位)。knn 并行后官方 bench 的
query_peak_rss 升到 74-116MB, 20ms 采样器实测首查真实峰值 **+288~307MB**:

组成 = 向量缓存解码堆副本 (预算 256MB 时 100K×384 表全量缓存 ≈154MB
保留) + 并行流式任务的整段缓冲 (16 并发 × 7.7MB ≈ 120MB 瞬时)。

三处修复:

1. **并行流式缓冲有界化**: 跨段/段内并行任务的列读取从整段一次性分配
   改为 ≤1MB 子分块循环 (并发 × 1MB 有界)。
2. **流式路径 f32 对齐直读**: 三处流式循环的 bytes 块 + 每行
   copy_le_f32 两趟拷贝改为 pread 直接落位 f32 对齐缓冲, 行切片即
   &[f32] (LE 主机盘上字节序即主机序; aarch64/x86_64 均如此)。
3. **默认预算 256→64MB** (`DEFAULT_VECTOR_COL_CACHE_BUDGET_BYTES`):
   超限段走 pread 流式 — 零 RSS 增量 (页缓存不计入进程 RSS); ≤64MB 的
   表仍全量缓存 (暖查最快)。需要大表暖查延迟的场景按表调回:
   `db.set_vector_cache_budget("ev", 256*1024*1024)` (edge/robotics
   preset 本就 32MB)。

取舍实测 (100K×384, 重开后首查 knn):

| 配置 | 首查 RSS 峰值增量 | 暖查 p50 |
|---|---|---|
| 默认 64MB (流式) | **+2MB** | 27.4ms |
| 调回 256MB (缓存) | +251~295MB | 5.4ms |

官方 bench (默认档): query_peak_rss **0.1MB**, knn10 27.1ms — 与
sqlite (18.4ms) 同量级; 调回 256MB 后 4.8ms (5.6×)。这是**画像优先
的默认**: 内存换延迟的旋钮留给部署方。其余形状持平 (load 144K /
point 18µs / range 0.24 / groupby 0.92 / join 0.55ms)。

验证: test_knn_parallel (L2+cosine 对拍 + UPDATE/DELETE/NULL) + E2E
51 + CLI 17 + insert_arrays 15 + parallel_ab 11 + fetch_arrays 16 +
fuzz 2seed×(on/off) + bigtable diverge=0 + 全量套件。

## DiskANN churn 稳定性专项 — 孤立 2-环根因 + 周期性领养 (flake 归零)

全量套件唯一的残留时序 flake (`churn_rebuild_covers_all_nodes`, 五轮全量
出现三次, 隔离复跑却过) 实为**进程级概率 bug**: `batch_build_graph` 用
`thread_rng()` 洗牌插入序 → 每进程拓扑不同, ~5-20% 概率挂 (循环复现
19/20 → 8/40 → 9/40)。

**根因** (库内诊断测试转储搁浅节点出入边): 两个节点**互指成孤立 2-环**
且无第三者指向 — 入度守卫 (`evictable`: 入度 >1 才可驱逐) 防的是"零入
边", 防不了"幸存入边来自同样孤立的环"。rebuild 窗口内的行级插入没有
全局可达性检查, 搁浅要等下一次 rebuild 的领养才恢复 — 测试断言的正是
这个窗口。

修复 (三件套):
1. **set_neighbors 连通性守卫截断**: 盲截断丢最高 id 改为尾部丢弃时跳过
   不可驱逐者, 不够丢则临时超限 (宽度自愈, 搁浅无法自愈)。
2. **强制回链无 victim 时不驱逐**: 此前兜底驱逐任意最远边 (可能是受害
   者唯一入边) 改为溢出追加 (set_neighbors_overflow_ok)。
3. **周期性增量领养**: 把 rebuild 内的孤点领养提取为 `adopt_orphans`,
   单行插入路径每 max(50, len/50) 次 churn 跑一次 flood-fill+领养 —
   O(V+E)/len/50 摊销 ≈ 每次插入 ~64 次边读; rebuild 窗口内搁浅最多
   积压 len/50 次插入。

验证: churn 测试循环 **40/40** + 诊断测试 60 trial 零失败 (修复前
5-20% 挂); vamana 模块 27/27; vector recall 220K×384 持平 (recall@1/10/
100 = 0.99/0.99/0.98, 增量 +5000 行 0.995/0.99/0.98); E2E 51 + CLI 17 +
insert_arrays 15 + fuzz + bigtable diverge=0 + 全量套件。诊断测试
(`churn_connectivity_diagnostic`, 默认 ignore) 留作工具。

## resource_bench 全套资源画像复扫 (64MB 默认档, 2026-09)

快照: `resource_bench_2026-09_profile.json` (基线 `resource_bench_v0.10.0.json`)。

| 指标 | v0.10.0 基线 | 现在 | 评 |
|---|---|---|---|
| 加载 rows/s (executemany 口径) | 62,181 | **88,279** | +42% (WAL/pyo3 优化红利) |
| 加载峰值 RSS Δ | 210.9 | **55.7** | −74% |
| 查询内存 (点查/范围/分组/topk/join/FTS/knn/全扫) | ≈0 (join 63.2) | **全 ≈0 (join 0.1)** | join 物化优化红利 |
| steady-state RSS Δ (全类跑热) | 159.5 | **18.8** | −88% (向量缓存 64MB 档) |
| groupby 延迟 | 1.71ms | 0.98ms | |
| knn10 延迟 | 6.14ms | 24.61ms | 已知取舍 (流式; 调回 256MB → 5.4ms) |
| edge preset knn p50 | 26.39ms | 18.39ms | 更快 |
| 磁盘 (checkpoint 后 / FTS) | 162.0 / 175.9 | 162.0 / 175.9 | 持平 |
| 重开 / crash 恢复 (kill -9) | ok | ok (1300/1300 行可见) | ✓ |

结论: 查询期与 steady-state 内存全部落在 ≤100MB 档位内 (实测最大
steady 18.8MB); 唯一延迟回退是 knn 流式 (上节记录的画像优先取舍, 部署
方可按表调回预算)。加载/查询吞吐、join 内存、edge preset 全面改善。

## 流式 knn 零拷贝 — 字节距离核, 24.6 → 18.0ms (页缓存带宽地板)

64MB 默认档的流式 knn 此前是三趟扫描: read_bytes_at (mmap 借出) → fbuf
对齐落位复制 → 距离核。新 `*_distance_bytes` 核 (euclidean/cosine) 直接
消费**任意对齐的 LE 字节切片** — read_bytes_at 借出的 mmap 切片直接喂,
零分配零复制, 三处流式循环 (顺序/跨段/段内并行) 全部切换:

- NEON vld1q / AVX2 loadu 非对齐加载原生支持 (裸指针);
- 标量回退 from_le_bytes (可移植, BE 主机正确);
- 与主核同语义 (cosine clamp / 零范数)。

实测 (100K×384, 64MB 默认档): 暖查 p50 **24.6 → 18.25ms** (−26%), RSS
仍 +0MB; 官方 bench knn10 27.1 → **17.97ms** — 与 sqlite 精确扫描
(18.4ms) 持平且零额外内存。调回 256MB 缓存档仍 4.4ms (35GB/s 堆带宽)。
18ms 是页缓存带宽地板 (~8.4GB/s, macOS page-cache 读取约为堆带宽 1/4);
再快只有缓存档 (换内存) 或近似索引两条路。其余形状持平
(load 173K / groupby 0.80 / join 0.44ms)。

验证: test_knn_parallel (L2+cosine 暴力对拍 + UPDATE/DELETE/NULL — 字节
核与主核等价性) + vamana 27/27 + E2E 51 + CLI 17 + insert_arrays 15 +
parallel_ab 11 + fetch_arrays 16 + fuzz 2seed×(on/off) + bigtable
diverge=0 + 全量套件。

## 加载路径双优化 — 显式 PK fast path + executemany numpy 参数 (官方 152K→254K)

两项加载优化 + 一个被暴露的先存 checkpoint 漏洞:

1. **显式整型 PK 大批快路径** (`fast_batch_insert_explicit`, 门: ≥100 行 +
   全整数非负 <2^31 PK + pk_lookup 在): row_id = PK 值 (慢路径同语义),
   批内+存量唯一性经 pk_lookup **存真实 row_id** (慢路径留 0 占位),
   auto-inc 表 counter 越过最大显式 PK; 复用 auto-inc 快路径的免 WAL/免
   逐行 validate 全部节省。负值/超界/TEXT PK → 慢路径。过程中抓到并修复
   快路径继承的 Integer→Timestamp 强转缺失 (insert_arrays ts 列读回 0 —
   fb593a2 在慢路径修过, 分流把同一形状转发了过来; 既有 ts 用例抓住)。
2. **executemany numpy 行视图参数**: py_to_mote 检测 numpy 1D 数组
   (f4/f8/i8) 经 tobytes 一次 memcpy 解码为向量 — 用户免每行 .tolist()
   (124K rows/s 的 Python 侧天花板), executemany 65K → **174K rows/s**。
3. **🚨 先存 checkpoint 漏洞 (被 1 暴露)**: checkpoint_impl 在
   pending_updates==0 且 WAL 空 → 整体早退 — fast path 不写 WAL 不置位,
   段永远不合并 (reopen 才补), auto-checkpoint 对 WAL-less 加载从不触发。
   e2e "disk single segment" 用例抓住。修复: fast path 批末
   increment_pending_updates (慢路径在 WAL append 前的同款信号)。

吞吐 (100K×5 列含 384 维向量, 同机): executemany .tolist() 65K (基线) /
executemany numpy 视图 **174K** (2.7×) / insert_arrays 显式 PK
**286-289K** (1.9×, 原 153K 慢路径) / 官方 compete_bench load
**253,938 rows/s** (原 152K)。其余形状持平 (knn 19.0 / rss −0.0)。

验证: test_insert_explicit_fastpath 5/5 (row_id=PK 点查/范围/ORDER、批内
重复报错+精确回滚+重插、auto-inc 表显式 id+counter 越位、负值 PK 慢路径
回退、重开+后续 UPDATE/DELETE 的 pk_cache 精确性) + insert_arrays 17/17
(新增 numpy 行参数 f32/f64 往返) + E2E 51 + CLI 17 + parallel_ab 11 +
fetch_arrays 16 + fuzz 3seed×(on/off) + bigtable diverge=0 + 全量套件。

## WAL-less 段阵的 auto-checkpoint — 段计数触发 (验证收口)

上节修复的 `pending_updates` 信号只救了**手动** checkpoint; 完整验证暴露
更深一层: **auto-checkpoint 线程只按 WAL 目录大小触发** — fast path /
insert_arrays 加载 (推荐的批量导入方式) 不写 WAL, WAL 恒 0 → 自动触发
永远不发生, 持续加载下段阵无界增长 (10GB ≈ 2500 段), 只等 reopen 或
手动 checkpoint 收口。

修复: AutoCheckpointConfig 新增 `max_segment_count` (默认 32; edge/
embodied/robotics preset 16), 后台线程在 WAL 大小检查之外加**所有
ColSegmentStore 表的段总计数**触发 — 段计数直接度量被泄漏的资源, 与
写粒度无关 (WAL 大小是间接代理)。

验证: test_auto_checkpoint_segments — 10 批 × 12MB WAL-less 加载
(WAL 触发恒不命中), 阈值 8: 后台线程 (~10s 检查周期) 自动合并段阵到
≤8, 行数与内容完好; 手动 checkpoint 对照。全量门槛 + E2E 51 + CLI 17 +
fuzz 2seed×(on/off) + bigtable diverge=0。

测试侧教训: 验证测试曾给 2 列表塞 3 值行 — fast path 跳过 validate_row,
builder 的越界保护**静默丢弃整列** (120KB 载荷列消失 → 缓冲 <4MB 永不
flush → 段数为 0), 掩盖了真实行为。宽行进快路径是既知语义 (SQL 层保证
行宽), 但值得记录: 测试数据形状必须与 schema 严格一致。

## resource_bench 快照刷新 — fast path 耐久性门 (crash 契约恢复)

快照刷新暴露最后一个 (也是最重要的) 回归: 显式 PK fast path 免 WAL 后,
resource bench 的 crash 段 (子进程 executemany 300 行返回成功 → kill -9
→ 重放) 只见 1000/1300 — **返回成功的自动提交写入必须扛住 kill -9** 是
GroupCommit/Synchronous 档的契约, fast path 破坏了它 (自增 fast path 同
款隐患, 只是此前的测试形状没踩到)。

修复: `fast_batch_insert_with_ids` (两个 fast path 的公共尾段) 加耐久性
门 — Synchronous/GroupCommit 下同样写 WAL (从 store_rows 建 WALRecord,
不额外 clone 一份); NoSync/Periodic (尾部丢失本就是契约) 保留全速免 WAL。

诚实代价 (GroupCommit 默认档): 官方 load 254K→**174K** (仍为本战役前
62K 的 2.8×), insert_arrays 显式 PK 286K→171K, executemany numpy
174K→125K; NoSync/Periodic preset (edge/robotics/testing) 保留 286K。
加载峰值 RSS 55.7→209MB (WAL 序列化的行副本 — 加载期而非查询期, ≤100MB
约束针对查询期)。

最终快照 `resource_bench_2026-09_profile.json` (覆盖前版):
load 97K (executemany .tolist() 口径) / 查询 RSS 全形状 ≈0 / steady
−1.0MB / knn 18.3ms / **crash 1300/1300 ✓** / 磁盘 162MB / 重开 RSS
−9.1MB。此前章节的 254K/286K 数字注明为 NoSync 等价口径。

验证: crash 重放手工复现 1300/1300 + test_insert_explicit_fastpath 5/5 +
test_insert_engine 3/3 + 全量门槛与套件 (下)。

## WAL 序列化零 clone — 假设证伪, 借用版保留

耐久性门后的假设: WALRecord::Insert 的整行 clone 是 GroupCommit 档加载
的主要差额, `batch_append` 借用化 (直接从 &Row 序列化 — bincode 本就
借用; 字节格式与 Insert/InsertRaw 逐位一致, 重放走既有解码器) 可把
174K 拉回 250K+。

**实测证伪**: clone 版 171-174K vs 借用版 167-169K (噪声内持平)。差额
的真正构成是 WAL 序列化 + 写盘本身 (100K×384 行 = 160MB 额外 bincode
编码与 IO) — 耐久性的固有价格, 不是 clone。借用版保留 (少一份整行
materialize, 分配更少, 代码更直白); crash 契约复验 1300/1300 ✓。

GroupCommit 档的加载吞吐地板因此定格 ~170K (战役前 62K 的 2.7×);
需要 286K 的批量导入场景用 NoSync/Periodic preset (尾部丢失是契约)。
更进一步的路径是 WAL 消除 (段文件直写 + manifest/段双 fsync 替代 WAL
重放), 属另线战役。

## WAL 消除 — fast path 段直写耐久, GroupCommit 档 171→232K

耐久性门修好 crash 契约后, GroupCommit 档的加载地板 ~170K (差额 = WAL
bincode 序列化 + 写盘, 100K×384 = 160MB 额外编码与 IO)。零 clone 假设
证伪后, 正解是**数据只写一遍**:

- 核实 flush 链路发现段文件写入**本就耐久** (temp 文件 + fsync + rename
  原子发布); manifest 追加不 fsync 但设计上由孤儿领养兜底 (段文件完整,
  manifest 尾丢失 → recovery 领养), 且有现成的 `manifest.sync()`。
- 新 `ColSegmentStore::flush_buffer_durable()`: flush (段原子发布) +
  manifest fsync — 返回成功后 kill -9 (页缓存可见) 与掉电 (段已 fsync,
  manifest 尾丢失走领养) 都恢复, **无需 WAL 重放**。
- fast path 耐久分支从借用式 WAL 切换为每批 durable flush (数据只写一遍);
  每批一段, 段阵由 auto-checkpoint 段计数触发合并 (与上节协同)。借用式
  WAL API (a1d7bb9) 被超越, 已移除。

吞吐 (GroupCommit 默认档, 100K×5 列含 384 维): insert_arrays 显式 PK
171→**232K** (向 NoSync 286K 收复 81%); 官方 load 174→**197K**;
executemany 持平 128K (瓶颈在 Python 侧行构造, 非引擎写路径)。crash
契约复验 **1300/1300** ✓。knn 18-21ms 波动 (每批一段 → 段数增加, 跨段
并行路径消化)。

验证: crash 重放 1300/1300 + test_insert_explicit_fastpath 5/5 + E2E
51 + CLI 17 + insert_arrays 17 + parallel_ab 11 + fetch_arrays 16 +
fuzz 3seed×(on/off) + bigtable diverge=0 + 全量套件。

## 四引擎同日对照 — v0.11.0 新鲜基线 (2026-09-26, load≈3.5)

替代上方 Round 11/12 时代的对照表 (彼时 MoteDB join 138ms)。同脚本同
数据 (100K×384), 负载 ~3.5 同日连跑, p50:

| workload | MoteDB 0.11.0 | SQLite 3.51 | DuckDB 1.4.5 | FAISS Flat |
|---|---|---|---|---|
| bulk load rows/s (w/ vec) | **194,829** | 54,243 | 6,997* | n/a |
| PK point | 17µs | **5µs** | 69µs | n/a |
| range COUNT+AVG | **0.197ms** | 0.33ms | 0.279ms | n/a |
| GROUP BY device | 0.79ms | 46.4ms | **0.558ms** | n/a |
| top-k ORDER LIMIT | **0.457ms** | 35.0ms | 0.71ms | n/a |
| equi-JOIN+GROUP BY | **0.472ms** | 10.5ms | 1.098ms | n/a |
| exact vector knn@10 | 18.4ms | n/a | 65.8ms | **3.6ms** |
| text two-term search | 0.141ms | **0.013ms** (FTS5) | n/a | n/a |
| query RSS delta | **0.0MB** | 0.0MB | 0.1MB | 669.8MB (in-RAM) |
| db size | **186.5MB** | 212.6MB | 489.7MB | 153.6MB (无持久化) |

口径注: *DuckDB 1000-row multi-VALUES 路径 (Appender/read_parquet 更快
但不在本脚本); MoteDB load 为 GroupCommit 耐久档 insert_arrays; knn 为
无索引精确扫描 (64MB 默认缓存档; 256MB 档 4.4ms 与 FAISS 同内存量级);
FAISS 无 SQL/持久化/FTS — 列不可直接比, 列出仅供 knn 参照。

解读 (差距定位): 分析形状 (range/topk/join) 在 100K-1M 嵌入式档位与
DuckDB 互有胜负 (join 快 2.3×, groupby 慢 1.4× — 均在双方固定开销量
级); 加载/存储/内存画像领先。落后项: 点查 vs SQLite 3.4×, FTS vs
FTS5 10×, ANN 索引路径 vs FAISS IVF 类 ~10×, >内存规模的 spill/外存
路径未建, SQL 广度 (窗口函数/递归 CTE/代价优化器) vs DuckDB 一代。
