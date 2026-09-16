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
