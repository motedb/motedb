# Capability Manifest

> This file is the **single source of truth** for what MoteDB can do.
> The website, the README, blog posts and any external material should cite
> this manifest rather than restating feature claims.
> The point: every line below should map to an implementation and a test.

**Version baseline:** v0.15.2 · **Last verified:** 2026-10-24

Legend: `✅ supported` · `🟡 partial (see note)` · `❌ not supported` · `🟢 differentiator`

> If you find a line here that does not match actual behaviour, please open an
> issue — **those are the ones we triage first**.

---

## 1. SQL engine

| Capability | Status | Notes |
|---|---|---|
| `CREATE TABLE` / `CREATE INDEX` / `DROP TABLE [IF EXISTS]` / `DROP INDEX` | ✅ | Column, vector, text, spatial, timestamp index variants |
| `ALTER TABLE` | ✅ | `ADD COLUMN`, `AUTO_INCREMENT = N` |
| `INSERT` (incl. `INSERT ... SELECT`) | ✅ | |
| `UPDATE` / `DELETE` | ✅ | |
| `SELECT` with `WHERE` | ✅ | |
| `JOIN` — INNER / LEFT / RIGHT / FULL | ✅ | |
| Subqueries in `WHERE`, derived tables (`FROM (SELECT ...)`) | ✅ | |
| `GROUP BY` | ✅ | Columns, expressions (`id % 5`), and SELECT aliases |
| `HAVING` | ✅ | Incl. aggregate aliases |
| `ORDER BY` | ✅ | Multi-key, expressions, non-projected columns, `NULLS FIRST/LAST` |
| `LIMIT` / `OFFSET` | ✅ | |
| `DISTINCT` (rows) / `COUNT(DISTINCT col)` | ✅ | |
| Aggregates: `COUNT` `SUM` `AVG` `MIN` `MAX` `STDDEV` `VARIANCE` | ✅ | |
| `UNION` / `UNION ALL` | ✅ | |
| `CASE WHEN ... THEN ... ELSE ... END` | ✅ | |
| `WITH` / CTEs, incl. `WITH RECURSIVE` | ✅ | Semi-naive fixed-point iteration, `UNION ALL` bodies, chained CTEs |
| Window functions: `ROW_NUMBER` `RANK` `DENSE_RANK` `NTILE` `LAG` `LEAD` `FIRST_VALUE` `LAST_VALUE` + aggregates | ✅ | `OVER (PARTITION BY … ORDER BY …)` |
| Explicit window frames (`ROWS` / `RANGE BETWEEN <bound> AND <bound>`) | ✅ 🟢 | ROWS = physical rows; RANGE = peer groups, or value intervals (`RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING`) on a numeric/timestamp order key. Default frame is the RANGE peer-group frame. Tested in `tests/test_window_frames.rs` |
| Scalar functions: `UPPER` `LOWER` `LENGTH` `TRIM` `SUBSTR` `REPLACE` `CONCAT` `INSTR` `COALESCE` `ROUND` `ABS` + arithmetic | ✅ | `CONCAT` skips NULL args (SQLite/Postgres semantics); `\|\|` propagates NULL; `ROUND` is binary-exact decimal rounding |
| `EXPORT TABLE t TO 'x.parquet'` / `IMPORT PARQUET 'x' INTO t` | ✅ 🟢 | Columnar dump / batch load through the insert pipeline; pyarrow / pandas / DuckDB round-trip |
| `EXPORT LEROBOT <table> TO 'dir/' [FPS n] [TASKS col]` | ✅ 🟢 | Writes a LeRobot v3 dataset on disk that the official `LeRobotDataset` (lerobot ≥ 0.6) loads directly |
| `DECIMAL` / `DATE` types | ❌ | Planned — see [ROADMAP](./ROADMAP.md) |
| Cross-statement server-side cursors | ❌ | Planned — see [ROADMAP](./ROADMAP.md) |

---

## 2. Multimodal types

| Type | Status | Notes |
|---|---|---|
| `VECTOR(n)` | ✅ 🟢 | Index-accelerated KNN; `ORDER BY emb <-> '[…]'` (L2) or `<~>` (cosine) |
| `BLOB` | 🟡 | Byte-exact round-trip, **16 MiB cap per value** |
| `TIMESTAMP` | ✅ 🟢 | Microsecond precision; `TIMESERIES(ts)` adds time-series semantics |
| `TEXT` | ✅ | Built-in tokenizer (jieba and others configurable) |
| Spatial coordinates | ✅ 🟢 | R-Tree index; `ST_POINT` / `ST_WITHIN` / `ST_DISTANCE` / `ST_KNN`, and `loc <-> ST_POINT(x, y)` distance ordering |
| Array types (`TEXT[]` / `ARRAY<FLOAT>`) | ❌ | Use BLOB, a separate table, or TEXT + JOIN |

### ⚠️ Known BLOB gaps (unchanged since v0.13)

- Scalar functions over BLOB columns (e.g. `LENGTH(payload)`) are **not wired through all executor paths**
- BLOB output under `ORDER BY` is **not wired through all executor paths**
- **Verified working:** bare projection, equality (`WHERE payload = X'…'`), time-window scans, parameter binding

---

## 3. Indexes

| Index | Status | Notes |
|---|---|---|
| Column index | ✅ 🟢 | Equality / range |
| Vector index | ✅ 🟢 | **DiskANN** (FreshDiskANN + rerank). **Not** HNSW, not IVF |
| Full-text index | ✅ 🟢 | BM25 ranking + snippet; `MATCH(col) AGAINST('q')`; multi-word defaults to AND (SQLite FTS5-compatible), explicit `a OR b` unions |
| Spatial index | ✅ 🟢 | R-Tree |
| Time-series index | ✅ 🟢 | Range scan + compaction + `TIME_BUCKET` aggregation + TTL |

> 📌 **Correction notice.** Older material describing the vector index as
> "HNSW / IVF" is out of date. The current implementation is **DiskANN**.
> This file and `docs/08-vector-index.md` are authoritative.

---

## 4. Transactions & consistency

| Capability | Status | Notes |
|---|---|---|
| `BEGIN` / `COMMIT` / `ROLLBACK` | ✅ | |
| Savepoints | ✅ | |
| Read-your-writes inside a transaction | ✅ | Scans, aggregates, point lookups |
| MVCC | ✅ | |
| WAL group commit | ✅ 🟢 | |
| `LATEST BY` on tables > 1M rows | 🟡 | Performance not yet optimized (large-table merge path) |

---

## 5. Retrieval

| Capability | Status | Notes |
|---|---|---|
| Vector KNN | ✅ 🟢 | `ORDER BY emb <-> '[…]'`, index-accelerated |
| Full-text search | ✅ 🟢 | BM25 ranking |
| **Hybrid search (BM25 × KNN, RRF fusion)** | ✅ 🟢 | One call — you don't implement score calibration yourself |
| Time-series window aggregation | ✅ 🟢 | `TIME_BUCKET` + TTL |
| Spatial range / KNN | ✅ 🟢 | `ST_WITHIN` / `ST_DISTANCE` / `ST_KNN` |

---

## 6. Data shapes & deployment

| Capability | Status | Notes |
|---|---|---|
| Collection: high-performance directory | ✅ 🟢 | Atomic publish via temp + fsync + rename, plus WAL group commit |
| Transport: single-file snapshot | ✅ 🟢 | `EXPORT DB TO '…'`; per-file CRC32, optional zstd, incremental re-export |
| Deployment: read-only container | ✅ 🟢 | All write paths rejected, multiple concurrent readers |
| MCAP import | ✅ 🟢 | |
| Index self-healing | ✅ 🟢 | Rebuilds a corrupt index from source data |
| Crash recovery | ✅ 🟢 | WAL replay + segment migration + orphan segment adoption |
| `for_edge()` low-memory profile | ✅ | Tuned for small-memory devices |

---

## 7. Interop & SDKs

| Surface | Status | Notes |
|---|---|---|
| Rust crate | ✅ | `cargo add motedb` (Rust ≥ 1.87) |
| Python | ✅ | `pip install motedb-python` — imported as `motedb`. The `motedb` name on PyPI is **not yet claimed**; use `motedb-python` |
| Arrow | ✅ 🟢 | `query_arrow()` → canonical `pyarrow.Table`, vector columns as `FixedSizeList<float32>` |
| pandas | ✅ | `query_pandas()` |
| numpy bulk load | ✅ | `insert_arrays()` |
| Node.js | 🟡 | npm package **is published** (`motedb@0.15.2`). Prebuilt binaries cover macOS (arm64/x64) and Linux (x64/arm64, glibc); **musl and Windows prebuilds are not published yet**, so `npm install motedb` on Windows currently yields no native binary |
| Go | 🟡 | Binding lives in `bindings/go` (with `go.mod`), but is **not published as a tagged Go module** yet — the module proxy only resolves a pseudo-version off the default branch |

---

## 8. Engineering quality

| Practice | Status | Notes |
|---|---|---|
| Crash-injection tests | ✅ 🟢 | Resident in CI |
| Differential fuzzing (SQLite as oracle) | ✅ 🟢 | Permanent regression suite, including JOIN diffing |
| Corruption-recovery tests | ✅ 🟢 | |
| Machine-independent performance gate | ✅ 🟢 | A complexity regression turns CI red |
| `cargo fmt --check` strict gate | ✅ 🟢 | Whole tree is fmt-clean |
| cargo-deny supply-chain audit | ✅ 🟢 | Vulnerabilities + licenses |
| clippy | 🟡 advisory | CI runs clippy in advisory mode (`continue-on-error`); a backlog of style lints is being cleared. **New commits must not introduce new warnings** |
| Prebuilt binaries | 🟡 | macOS (arm64/x64) and Linux (x64/arm64) prebuilt. Windows works through `cargo add motedb` (source build); a Windows prebuild is on the way |

---

## 9. Explicitly not supported

> Written down so you don't have to discover it the hard way.

| Capability | Why |
|---|---|
| **Built-in inference engine / ONNX** | MoteDB is a storage and query layer, not an inference runtime. We invest in **Arrow interop** and leave inference in your own stack |
| **Distributed / multi-node replication** | The current target is single-machine edge |
| **Server mode / multi-process concurrent writes** | Contradicts the "embedded, zero-ops" positioning |
| **Multi-user permission system** | In embedded deployments the host application owns that |
| **Array types** | Use BLOB, a separate table, or TEXT + JOIN |
| **`DECIMAL` / `DATE` types** | Planned |
| **Cross-statement server-side cursors** | Planned |

---

## 10. Performance methodology

Full benchmark data: [`BENCHMARK.md`](./BENCHMARK.md),
[`PERFORMANCE_REPORT.md`](./PERFORMANCE_REPORT.md), and
[`docs/benchmark_methodology.md`](./docs/benchmark_methodology.md).

> ⚠️ **Citation rule.** Every performance number published anywhere must carry
> its **machine configuration, data scale, comparison baseline, and measurement
> method**. Numbers without that context should not be released.

---

*Found something here that does not match real behaviour? Open an issue.*
