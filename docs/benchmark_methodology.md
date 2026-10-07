# Benchmark Methodology

> How MoteDB's published numbers are produced, and how to reproduce them on
> your own machine in one command. Every number we publish must be
> reproducible by this procedure — if you cannot reproduce a claim, that is a
> bug; please open an issue with your `benchmark_results/` output attached.

## One-command reproduction

```bash
make compete          # core set: ~15-30 min
make compete-quick    # smoke: ~3 min
make compete-full     # + writes/vector/FTS/scale suites: 30-60 min
```

Prerequisites: Rust 1.87+ (release toolchain), Python 3.9+, `pip install
motedb-python` (the suite checks that the installed version matches
`Cargo.toml` and warns on mismatch; `PIP_INSTALL=1 make compete` pins it
automatically).

> ⚠️ **Benchmarking local source changes**: the Python suites drive the
> *installed* `motedb-python`, not your working tree. After changing engine
> code, rebuild and install the local binding first: `make wheel`.
> (A version match does not imply a code match — this trap cost us a
> debugging session during the suite's own shakeout.)

Optional engines are used **when installed** and skipped gracefully
otherwise: `pip install duckdb faiss-cpu`. SQLite comparisons use the Python
standard library `sqlite3`; FTS comparisons additionally use SQLite FTS5
(bundled with most Python builds) and, when present, `tantivy`.

Everything lands in `benchmark_results/<UTC-timestamp>_<mode>/`: one log per
step plus `SUMMARY.txt` with extracted verdicts and every `JSON` result line.
The suite **exits non-zero if any step fails** — including the correctness
harness (below), which means a benchmark run that silently produces wrong
results is reported as a failure, not a number.

## What is measured

| Suite | What it covers |
|---|---|
| `bench_quick_baseline` (Rust) | single-row INSERT, PK point lookup, COUNT/aggregate/range/ORDER BY at 10K rows |
| `bench_comprehensive` (Rust) | 50K-row lifecycle: load, flush, checkpoint, reopen, scan, CRUD, WAL recovery, concurrency |
| `bench_timeseries_index` (Rust) | time-series ingest throughput + segment scans |
| `bench_vs_sqlite_100k` (Rust) | 100K-row head-to-head vs SQLite (same schema, same statements) |
| `compete_bench.py` (Python) | 100K mixed workload (int PK / ts / text / 384-d vector) vs SQLite, DuckDB, FAISS(+numpy): load throughput, disk footprint, peak RSS, per-query avg/p50/p95 |
| `compete_spatial_ts.py` | 1M-row time-series + 500K 3-D points vs SQLite/DuckDB: range aggregates, bbox/KNN, deletes |
| `compete_writes.py` | write-path durability ladder (autocommit / periodic / batch) incl. kill-9 crash matrix |
| `compete_vec_industry.py` | recall@10 + latency vs FAISS Flat / IVF / HNSW at matched recall |
| `compete_fts_industry.py` | FTS build time + match quality vs SQLite FTS5 / tantivy |
| `compete_scale.py` | 1M-row scan/aggregate/top-K scaling |
| `adversarial_verify.py` | **correctness gate**: 400+ randomized queries result-set-compared against SQLite as oracle, FTS match sets vs FTS5, vector recall vs numpy exact, dirty-data update/delete re-verify, kill-9 crash parity, cold-process latency |

## Dataset and workload rules

- **Deterministic generators**: every dataset is produced from fixed seeds
  (`default_rng(42)` for shapes, seed 7 for text) — same inputs on every
  machine and every run. No hand-picked data.
- **Identical workload per engine**: each compete script materializes one
  dataset and drives every engine with the same rows, the same statements,
  and the same query mix. Query parameters are randomized per run (the
  adversarial harness) so results cannot overfit a warm path.
- **Per-engine process isolation**: engines run in separate processes so RSS
  measurements are independent and a crashed engine cannot contaminate
  another's numbers.

## Fairness rules (the ones we hold ourselves to)

1. **Durability is matched before comparing write throughput.** MoteDB's
   default durability is GroupCommit (fsync per commit — *stronger* than
   SQLite's default). Same-durability comparisons against SQLite WAL+NORMAL
   use MoteDB's `periodic` level. We publish both and label which is which;
   claims of "faster than SQLite" are made **only** on the matched-durability
   configuration.
2. **Recall is matched before comparing ANN latency.** Vector-index
   comparisons report recall@10 alongside latency; a latency comparison is
   only made against a config at ≥ equal recall (e.g. our SQ8 vs FAISS Flat
   at recall ≈1.0, vs HNSW/IVF at their tuned recall, stated per row).
3. **Report the machine.** Absolute numbers vary across hardware. Every
   published table states the machine class (chip, RAM, OS, version under
   test). Cross-machine comparisons of *ratios* use the CI perf gate
   (`examples/perf_smoke.rs`), which asserts query-shape latencies as ratios
   against a full-scan baseline — machine-independent budgets that catch
   complexity regressions, not percentage noise.
4. **Warmup and iteration count are fixed in code**: query latencies report
   avg/p50/p95 over fixed iteration counts after one warmup pass; cold-start
   effects are measured only by suites explicitly designed for it
   (`adversarial_verify` section G).
5. **CI mode exists and is labeled**: `CI=1` shrinks dataset sizes for CI
   reliability. Numbers published externally always come from full-size runs.

## Known caveats (published with the numbers, not hidden)

- Python-binding per-row bridging (`executemany`) carries interpreter
  overhead; the high-throughput path is the batch API (`insert_arrays`).
  Both are reported.
- First-query latency after open includes page-cache effects; F1-style
  warmup (`madvise`) is default-on in current versions. Cold-start suites
  measure this explicitly rather than averaging it away.
- DuckDB/FAISS/tantivy are optional dependencies: a `benchmark_results/`
  directory records which engines were present. When comparing our published
  tables with your run, check the engine list matches.

## Provenance

- `BENCHMARK.md` — curated headline tables (manually assembled from suite
  outputs, machine stated per table).
- `benchmark_results/` — raw logs + JSON, git-ignored, one directory per
  run. When filing a performance issue or dispute, attach one.
- Historical snapshots used in the changelog (e.g.
  `resource_bench_v0.10.0.json`) are committed under `bindings/python/bench/`
  for reference.
