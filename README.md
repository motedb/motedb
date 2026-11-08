# MoteDB

**AI-native embedded multimodal database for embodied intelligence.**
Columnar storage engine with ACID transactions, vector search, full-text search, and spatial indexing — in a single embedded library.

[![Rust](https://img.shields.io/badge/rust-1.87+-orange.svg)](https://rust-lang.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![crates.io](https://img.shields.io/crates/v/motedb.svg)](https://crates.io/crates/motedb)
[![crates.io downloads](https://img.shields.io/crates/d/motedb.svg)](https://crates.io/crates/motedb)
[![docs.rs](https://docs.rs/motedb/badge.svg)](https://docs.rs/motedb)
[![PyPI](https://img.shields.io/pypi/v/motedb-python.svg)](https://pypi.org/project/motedb-python/)
[![npm](https://img.shields.io/npm/v/motedb.svg)](https://www.npmjs.com/package/motedb)
[![CI](https://github.com/motedb/motedb/actions/workflows/ci.yml/badge.svg)](https://github.com/motedb/motedb/actions/workflows/ci.yml)
[![Perf Gate](https://github.com/motedb/motedb/actions/workflows/perf-gate.yml/badge.svg)](https://github.com/motedb/motedb/actions/workflows/perf-gate.yml)
[![Website](https://img.shields.io/badge/website-motedb.github.io-ff8a2b.svg)](https://motedb.github.io/)

> **Status: pre-1.0.** The Rust embedding API and storage engine are stable and
> heavily tested; the SQL surface and multi-language FFI are still evolving.
> See [Supported SQL](#sql-support) for the current feature set.

## Quick Start

```bash
cargo add motedb
```

**Prefer a 10-minute tour of the core capabilities** (Python: continuous
views, hybrid search, Arrow, snapshot deployment)? Open
[`examples/motedb-demo.ipynb`](examples/motedb-demo.ipynb) — it runs
directly on Colab; `pip install motedb-python` is the only prerequisite.

A minimal, runnable example (`examples/hello_world.rs`):

```rust
use motedb::{Database, QueryResult};

fn main() -> motedb::Result<()> {
    // Create or open an embedded database (single file + WAL sidecars).
    let db = Database::create("hello.mote")?;

    // Standard SQL: CREATE / INSERT / SELECT
    db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")?;
    db.execute("INSERT INTO users VALUES (1, 'Ada', 36)")?;
    db.execute("INSERT INTO users VALUES (2, 'Linus', 54)")?;

    // Query — materialize results
    let result = db.execute("SELECT name, age FROM users WHERE age > 40")?;
    if let QueryResult::Select { rows, .. } = result.materialize()? {
        for row in &rows {
            println!("{:?}", row);
        }
    }

    Ok(())
}
```

Run it with:

```bash
cargo run --example hello_world
```

For the multimodal features (vector / full-text / spatial search), see
[`examples/crud.rs`](examples/crud.rs) and the [indexes overview](docs/06-indexes-overview.md).

Python bindings (`pip install motedb-python`, imported as `motedb`) — bulk load and columnar fetch are
first-class:

```python
import motedb, numpy as np

db = motedb.Database("app.mote")
db.execute("CREATE TABLE ev (id INT PRIMARY KEY, ts TIMESTAMP, emb VECTOR(384), tag TEXT)")

# Fast path: columnar bulk load (numpy passthrough, 197K rows/s durable):
db.insert_arrays("ev", {
    "id":    np.arange(N),
    "ts":    ts_micros_array,          # i64 micros
    "emb":   emb_2d_float32,           # (N, 384) f32
    "tag":   [f"t{i%16}" for i in range(N)],
})

# Fast path: columnar fetch (numeric columns → zero-copy numpy; TEXT/NULL → Python lists):
cols, arrays = db.fetch_arrays("SELECT id, ts, emb_norm... FROM ev WHERE ts > ?",
                               params=[t0])

# INSERT ... SELECT (materialized, then the same bulk pipeline — 1.6M rows/s):
db.execute("INSERT INTO archive SELECT * FROM ev WHERE ts < ?", params=[cutoff])
```

## Performance

Benchmark: 100K rows × 384-dim vectors (+TEXT/FLOAT/TIMESTAMP) on Apple
Silicon M-series, official harness `bindings/python/bench/compete_bench.py`
(SQLite baseline uses the same shape; full methodology and history in
`bindings/python/bench/README.md`).

| Operation | MoteDB | SQLite | Ratio |
|-----------|--------|--------|-------|
| Bulk load `insert_arrays` (GroupCommit durable) | 197K rows/s | 2.4M rows/s* | *SQLite has no vector column |
| Bulk load (NoSync/Periodic preset) | 286K rows/s | — | |
| PK point lookup | 17µs | 7µs | |
| Range COUNT+AVG (fused + parallel) | 0.19ms | 0.39ms | 2.1x |
| GROUP BY (morsel-parallel) | 0.71ms | 58ms | 82x |
| equi-JOIN + GROUP BY (batched hash join) | 0.44ms | 10.9ms | 25x |
| top-k ORDER LIMIT | 0.45ms | 45.7ms | 100x |
| Vector top-10 (exact scan, SIMD + parallel) | 18ms | 18.4ms** | **SQLite has no vector type; same data volume only |
| FTS BM25 | 0.13ms | — | |
| Cross-product COUNT(*) (600M pairs, folded) | 0.5ms | — | |

**Query memory**: RSS delta across all query shapes ≈ 0 (measured
steady-state < 20MB; default-profile budget ≤ 100MB). Full resource
profiles in `resource_bench_*.json`.

**Multi-engine comparison** (220K×384, same corpus and ground truth,
one-command repro via `make bench-industry`): MoteDB DiskANN recall@10
**1.0000** / p50 **0.57ms** — vs faiss-hnsw(ef256) 0.96/0.38ms, usearch
0.994/0.25ms, duckdb-hnsw 0.999/0.96ms, lancedb-ivfpq(default)
0.22-0.25/1.2-3.1ms. Full table and methodology — including the rows
unfavorable to MoteDB — in
[docs/benchmark_report_2026-10-21.md](docs/benchmark_report_2026-10-21.md).

**Execution kernel**: vectorized (VEC) + morsel parallelism on by default
(`MOTE_VEC=off` restores the old row-wise path); index-free vector scans
run a zero-copy byte-distance kernel (page-cache bandwidth floor).

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                     MoteDB                           │
├──────────┬──────────┬──────────┬───────────────────┤
│  SQL     │  Vector  │  Text    │  Spatial          │
│  Parser  │  DiskANN │  FTS     │  i-Octree         │
├──────────┴──────────┴──────────┴───────────────────┤
│              Columnar Storage Engine                 │
│  ┌─────────┐  ┌──────────┐  ┌────────────────────┐ │
│  │ WAL     │→ │ Columnar │→ │ Columnar SSTable    │ │
│  │ (fsync) │  │ Buffer   │  │ (mmap + zstd)       │ │
│  └─────────┘  └──────────┘  └────────────────────┘ │
├─────────────────────────────────────────────────────┤
│  MVCC Transaction │ Snapshot Isolation │ Conflict  │
└─────────────────────────────────────────────────────┘
```

- **Storage**: Columnar SSTable, mmap zero-copy access, optional page-level zstd compression (`compact_storage`, ~40-50% smaller disk; on by default in `for_edge`)
- **Write Path**: WAL (durability) → columnar buffer → auto-finalize → SSTable
- **Read Path**: SelectColumnar (zero-materialization), typed array access, predicate pushdown
- **Transactions**: VersionStore MVCC with snapshot isolation and conflict detection

## Logging

MoteDB emits lifecycle and durability events (WAL flush, checkpoint, background
thread errors) through the standard [`log`](https://docs.rs/log) facade. The
library installs **no logger itself** — your application chooses one (e.g.
`env_logger`, `tracing`, `slog`) and controls verbosity:

```rust
fn main() {
    // Install a logger (env_logger reads RUST_LOG).
    let _ = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("off")
    ).try_init();

    let db = motedb::Database::create("my_data").unwrap();
    // ...
}
```

```bash
RUST_LOG=motedb=info  ./your_app   # lifecycle events (open/close/checkpoint)
RUST_LOG=motedb=warn  ./your_app   # degraded conditions only
RUST_LOG=motedb=debug ./your_app   # verbose hot-path detail
```

With no logger installed, all logging compiles to a no-op (zero runtime cost).
See [`examples/logging.rs`](examples/logging.rs) for a runnable demo.

## Features

- 🔍 **Operational self-check** — `db.doctor()` / `motedb-cli doctor <path>`: per-item PASS/WARN report covering table layout, memory budgets, index coverage, and disk usage.

### Hybrid search (new in v0.13.1)

```sql
SELECT id FROM RRF(
  (SELECT id FROM docs WHERE MATCH(body, 'grasp failure') ORDER BY bm25(body) LIMIT 100),
  (SELECT id FROM docs ORDER BY emb <-> ? LIMIT 100)
);
```

One-SQL BM25 ⊕ vector fusion (Reciprocal Rank Fusion, `k`/`rrk`/`w1..w4`
tunable). `snippet(body, 'grasp failure', 80)` renders FTS5-style excerpts
with `<b>` highlights.

### Frame↔state alignment: ASOF JOIN (v0.14 work-in-progress)

```sql
-- For each frame, the latest joint-state at-or-before its timestamp (per session):
SELECT f.fname, j.q FROM frames f
ASOF JOIN joints j ON f.session = j.session AND j.ts <= f.ts;
-- LEFT ASOF JOIN keeps frames with no prior state (NULL right side).
```

### Episode model + trajectory search (v0.14 work-in-progress)

```sql
CREATE EPISODE TABLE teleop (
  session TEXT, ts TIMESTAMP, joint VECTOR(7), gripper FLOAT
) EPISODE BY (session, ts) GAP '5s';

-- One row per episode: bounds, row count, and a 64-sample resampled
-- trajectory per VECTOR column (flattened 64×dim).
SELECT session, t0, t1, n_rows FROM EPISODES(teleop);

-- Trajectory similarity (DTW over the resampled series; two-stage
-- PAA-shortlist design validated in docs/design/2026-10-traj-spike.md):
SELECT session, t0 FROM EPISODES(teleop)
ORDER BY traj_sim(traj_joint, [0.0, 0.1, …]) LIMIT 10;
```

Episodes are **derived data** — segmentation is a pure function of
`(key, ts, gap)`, so there is no registry to persist or invalidate and
reopen consistency is free.

### Dataset snapshots (v0.14 work-in-progress)

```sql
COMMIT DATASET 'fixpos-v3' NOTE 'after gripper fix';
-- … table keeps evolving (compaction GC will not touch pinned segments) …
EXPORT DATASET 'fixpos-v3' TO '/data/fixpos-v3.mote';  -- materialized copy
DROP DATASET 'fixpos-v3';                              -- release the pin
```

A snapshot is just the pinned segment list (segments are immutable files)
— near-free to take, persisted in `_dataset_commits`, and compaction
physically defers deletion of pinned files until the pin is dropped.
EXPORT materializes the pinned state into a fresh standalone database.

### Robotics data on-ramp (new in v0.13)

```bash
motedb import mcap robot.mote recording.mcap        # CLI
# or Python:
db.import_mcap("recording.mcap", decode={"/joint_states": my_decoder})
```

One table per topic (`_uid` PK, `log_time`/`publish_time` TIMESTAMP + auto
index, raw message `data BLOB`); a per-topic Python decode hook adds typed
columns (e.g. decoded `emb VECTOR`) at import time. Re-imports are no-ops
(`_uid` dedup) — crash-safe resumes.

### Multimodal

| Modality | Index | Query |
|----------|-------|-------|
| Tabular | Column Value (B-tree) | `WHERE`, `ORDER BY`, `GROUP BY` |
| Vector | DiskANN (Vamana graph) | `ORDER BY col <-> query LIMIT k` |
| Text | FTS (Inverted Index) | `WHERE MATCH(col) AGAINST('query')` |
| Spatial | i-Octree (3D) | `ST_DISTANCE`, KNN, radius search |

### Embedded Optimized

- **Low memory**: 222 B/row (vs SQLite's 335 B — 34% less); query-time RSS
  delta ≈ 0 (default vector-cache budget 64MB, per-table via
  `set_vector_cache_budget`)
- **Zero-copy reads**: mmap with on-demand page loading; index-free vector
  scans use a zero-copy byte-distance kernel (unaligned SIMD straight off
  the page cache)
- **Fast writes**: columnar bulk load via `insert_arrays` — 197K rows/s in
  the GroupCommit durable profile / 286K in NoSync (100K×384 basis). The
  fast path writes segments directly with durability (temp+fsync+rename
  atomic publish + manifest fsync); the WAL only serves the slow path
- **Dual-trigger auto-checkpoint**: WAL size + segment count
  (`max_segment_count`, default 32; edge preset 16) — keeps the segment
  array bounded for WAL-less bulk loads
- **Small disk**: zstd compression in compact mode (~40-50% smaller, 67 B/row on disk)
- **No daemon**: Single library, embedded directly

### ACID

- **Atomic**: WAL-based crash recovery
- **Consistent**: PK uniqueness, NOT NULL, type coercion
- **Isolated**: MVCC snapshot isolation
- **Durable**: WAL fsync + auto-finalize

### Quality Gates

Every push runs through three CI workflows:

| Workflow | What it guards |
|----------|----------------|
| **CI** | fmt (strict), clippy, cargo-deny supply chain, unit + integration suites on ubuntu/macos, **native arm64 test runner** (the edge/robotics target), aarch64 cross-compile, kill -9 crash-injection durability loop |
| **Perf Gate** | Query-shape latency *ratios* vs a full-scan baseline — machine-independent budgets that catch complexity-class regressions (e.g. an O(N²) path that slipped through once) |
| **Fuzz** (daily) | 5 min × 2 targets (`fuzz_sql_parser`, `fuzz_wal_recover`) under AddressSanitizer — found and fixed a parser stack-overflow on day one |

### Multi-language SDKs

| Language | Package | Status |
|---|---|---|
| Rust | `motedb` (crates.io) | stable |
| Python | `motedb-python` (PyPI, `import motedb`) | stable |
| Node.js | `motedb` (npm, bindings/node, napi-rs v3) | preview — prebuilt for macOS arm64/x64, Linux x64/arm64 (glibc); Windows = build from source for now (win32 prebuild pending), musl/Alpine in progress |
| Go | `bindings/go` (cgo) | preview |

Installation problems? See [docs/18-install-troubleshooting.md](docs/18-install-troubleshooting.md).

## Installation

```bash
cargo add motedb
```

Or in `Cargo.toml`:
```toml
[dependencies]
motedb = "0.18"
```

For minimal edge builds (no tokenizer, no parallelism), disable default features:
```toml
[dependencies]
motedb = { version = "0.18", default-features = false, features = ["jemalloc"] }
```

## Configuration

Pick a preset that matches your device, or start from one and override fields:

```rust
use motedb::{Database, DBConfig};

// Edge device (low memory, periodic fsync, single write partition)
let config = DBConfig::for_edge();

// Robotics (fast sensor ingestion, vector support)
let config = DBConfig::for_robotics();

// Embodied AI (vision-language models, real-time control loops)
let config = DBConfig::for_embodied();

let db = Database::create_with_config("my_data", config)?;
```

See [`docs/`](docs/) for the full configuration reference and per-field docs.

## SQL Support

**Supported:** `CREATE TABLE` / `CREATE INDEX` (column, vector, text, spatial,
timestamp) / `CREATE TEXT|VECTOR|SPATIAL|TIMESTAMP INDEX`, `DROP TABLE [IF
EXISTS]` / `DROP INDEX`, `ALTER TABLE` (`ADD COLUMN`, `DROP COLUMN` — logical
delete, `RENAME COLUMN`, `AUTO_INCREMENT = N`), `CREATE CONTINUOUS VIEW`
(time-series continuous aggregation — see below), `INSERT` (including
`INSERT ... SELECT`), `UPDATE`,
`DELETE`, `SELECT` with:

- `WHERE`, `JOIN` (INNER / LEFT / RIGHT / FULL), subqueries in `WHERE`,
  `FROM (SELECT ...)` derived tables
- `GROUP BY` (columns, expressions like `id % 5`, and SELECT aliases), `HAVING` (incl. aggregate aliases), `ORDER BY` (multi-key, expressions, non-projected columns, `NULLS FIRST/LAST`), `LIMIT/OFFSET`
- `DISTINCT` (rows) and `COUNT(DISTINCT col)` aggregates
- Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV`, `VARIANCE`, and
  time-series `FIRST(v, ts)` / `LAST(v, ts)` (value at min/max timestamp,
  2-argument TimescaleDB-style semantics)
- `UNION` / `UNION ALL`
- `CASE WHEN ... THEN ... ELSE ... END`
- `WITH` / Common Table Expressions incl. **`WITH RECURSIVE`** (semi-naive
  fixed-point, UNION ALL bodies, chained CTEs)
- Window functions: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`, `LAG`,
  `LEAD`, `FIRST_VALUE`, `LAST_VALUE` + aggregates `OVER (PARTITION BY …
  ORDER BY … [ROWS|RANGE BETWEEN <bound> AND <bound>])` — explicit frames
  (ROWS physical rows; RANGE peer groups or value intervals like
  `RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING` on the numeric/timestamp
  order key; default RANGE peer-group frame)
- Scalar functions: `UPPER`/`LOWER`/`LENGTH`/`TRIM`/`SUBSTR`/`REPLACE`/`CONCAT` (skips NULL args, SQLite/Postgres semantics; `||` propagates NULL)/`INSTR`/`COALESCE`/`ROUND` (binary-exact decimal rounding)/`ABS`/arithmetic
- Interop: `EXPORT TABLE t TO 'x.parquet'` / `IMPORT PARQUET 'x' INTO t`
  (pyarrow/pandas/DuckDB round-trip), `EXPORT LEROBOT <episode-table> TO
  'dir/' [FPS n] [TASKS col]` — a LeRobot v3 dataset the official
  `LeRobotDataset` loads directly
- Multimodal predicates: `MATCH(col) AGAINST('q')` (BM25 ranked FTS;
  multi-word default is AND, explicit `a OR b` unions — FTS5-compatible),
  vector `<->`/`<~>` ordering (DiskANN ANN), geometry `loc <-> ST_POINT(x, y)`
  distance ordering, `ST_WITHIN`, `ST_DISTANCE`, `ST_KNN`
- Transactions: `BEGIN` / `COMMIT` / `ROLLBACK`, savepoints, read-your-writes
  visibility inside a transaction

**Continuous views (v0.18):** `CREATE CONTINUOUS VIEW cv AS SELECT
TIME_BUCKET('1m', ts) AS b, device, AVG(v) AS v_avg FROM sensor GROUP BY b,
device` — query `cv` like a table (outer `WHERE` / aggregates / `ORDER BY`
compose normally); refresh is lazy at query time with full recompute (v1:
query-point consistency; `watermark` records the last refresh). Bucket-level
incremental refresh is deferred. See
[SQL operations → continuous views](docs/03-sql-operations.md).

**Multi-process access (v0.18):** one writer (flock EX, the default) **or**
N concurrent read-only readers on the live directory —
`Database::open_shared_readonly` takes a shared lock, performs a strict
clean-open check (clean-close flag + empty WAL + manifest-parity) and writes
zero bytes to disk. Readers never repair; repair is the writer's job.

**BLOB type:** `payload BLOB` columns with SQLite-style `X'0aff00'` hex
literals and Python `bytes` parameters (byte-exact round-trip, 16 MiB
cap). Known v0.13 gaps: scalar functions over BLOB columns
(`LENGTH(payload)`) and BLOB output under `ORDER BY` are not yet wired
through all executor paths — bare projection, equality (`WHERE payload =
X'…'`), time-window scans and parameters are solid.

**Not yet supported:** `DECIMAL`/`DATE` types, cross-statement server-side
cursors, BLOB scalar functions and BLOB output under `ORDER BY` (see above),
and `RENAME TABLE` (directory relocation — tracked separately).

## Documentation

Full guides live in [`docs/`](docs/):

- [Quick start](docs/01-quick-start.md) · [Installation & config](docs/02-installation.md) · [SQL operations](docs/03-sql-operations.md)
- [Batch operations](docs/04-batch-operations.md) · [Transactions](docs/05-transactions.md)
- Indexes: [overview](docs/06-indexes-overview.md) · [column](docs/07-column-index.md) · [vector](docs/08-vector-index.md) · [text](docs/09-text-index.md) · [spatial](docs/10-spatial-index.md) · [timestamp](docs/11-timestamp-index.md)
- [Performance tuning](docs/12-performance.md) · [Data types](docs/13-data-types.md) · [API reference](docs/14-api-reference.md) · [Best practices](docs/15-best-practices.md) · [FAQ](docs/16-faq.md)
- [Hybrid search & Arrow/pandas interop](docs/17-hybrid-arrow.md) · [Install troubleshooting](docs/18-install-troubleshooting.md)

API docs: <https://docs.rs/motedb>

## License

MIT — see [LICENSE](LICENSE).
