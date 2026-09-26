# MoteDB

**AI-native embedded multimodal database for embodied intelligence.**
Columnar storage engine with ACID transactions, vector search, full-text search, and spatial indexing — in a single embedded library.

[![Rust](https://img.shields.io/badge/rust-1.87+-orange.svg)](https://rust-lang.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![crates.io](https://img.shields.io/crates/v/motedb.svg)](https://crates.io/crates/motedb)
[![CI](https://github.com/motedb/motedb/actions/workflows/ci.yml/badge.svg)](https://github.com/motedb/motedb/actions/workflows/ci.yml)
[![Perf Gate](https://github.com/motedb/motedb/actions/workflows/perf-gate.yml/badge.svg)](https://github.com/motedb/motedb/actions/workflows/perf-gate.yml)

> **Status: pre-1.0.** The Rust embedding API and storage engine are stable and
> heavily tested; the SQL surface and multi-language FFI are still evolving.
> See [Supported SQL](#sql-support) for the current feature set.

## Quick Start

```bash
cargo add motedb
```

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

Python bindings (`pip install motedb`) — bulk load and columnar fetch are
first-class:

```python
import motedb, numpy as np

db = motedb.Database("app.mote")
db.execute("CREATE TABLE ev (id INT PRIMARY KEY, ts TIMESTAMP, emb VECTOR(384), tag TEXT)")

# 🚀 列式批量导入 (numpy 直通, GroupCommit 耐久档 197K rows/s):
db.insert_arrays("ev", {
    "id":    np.arange(N),
    "ts":    ts_micros_array,          # i64 micros
    "emb":   emb_2d_float32,           # (N, 384) f32
    "tag":   [f"t{i%16}" for i in range(N)],
})

# 🚀 列式取回 (同质数值列 → numpy 零拷贝; TEXT/NULL → Python 列表):
cols, arrays = db.fetch_arrays("SELECT id, ts, emb_norm... FROM ev WHERE ts > ?",
                               params=[t0])

# INSERT ... SELECT (物化后走同一批量管线, 1.6M rows/s):
db.execute("INSERT INTO archive SELECT * FROM ev WHERE ts < ?", params=[cutoff])
```

## Performance

Benchmark: 100K rows × 384-dim vectors (+TEXT/FLOAT/TIMESTAMP) on Apple
Silicon M-series, official harness `bindings/python/bench/compete_bench.py`
(SQLite 参照同形状; 详细方法学与历史曲线见 `bindings/python/bench/README.md`).

| Operation | MoteDB | SQLite | 倍数 |
|-----------|--------|--------|------|
| 批量导入 `insert_arrays` (GroupCommit 耐久档) | 197K rows/s | 2.4M rows/s* | *SQLite 无向量列 |
| 批量导入 (NoSync/Periodic preset) | 286K rows/s | — | |
| PK 点查 | 17µs | 7µs | |
| 范围 COUNT+AVG (融合+并行) | 0.19ms | 0.39ms | 2.1x |
| GROUP BY (morsel 并行) | 0.71ms | 58ms | 82x |
| equi-JOIN + GROUP BY (批 hash join) | 0.44ms | 10.9ms | 25x |
| top-k ORDER LIMIT | 0.45ms | 45.7ms | 100x |
| 向量 top-10 (无索引精确扫描, SIMD+并行) | 18ms | 18.4ms** | **SQLite 无向量类型, 参照仅同数据量 |
| FTS BM25 | 0.13ms | — | |
| 笛卡尔积 COUNT(*) (6 亿对, 折叠) | 0.5ms | — | |

**查询内存**: 全部查询形状 RSS 增量 ≈0 (steady-state 实测 <20MB, 默认
档约束 ≤100MB); 资源画像全套快照见 `resource_bench_*.json`。

**执行内核**: 向量化 (VEC) + morsel 并行默认开启 (`MOTE_VEC=off` 一键
回退旧行式路径); 无索引向量扫描为字节距离核零拷贝 (页缓存带宽地板)。

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

- 🔍 **运维自检** — `db.doctor()` / `motedb-cli doctor <path>`：表布局、内存预算、索引覆盖率、磁盘占用逐项 PASS/WARN 报告。

### Multimodal

| Modality | Index | Query |
|----------|-------|-------|
| Tabular | Column Value (B-tree) | `WHERE`, `ORDER BY`, `GROUP BY` |
| Vector | DiskANN (Vamana graph) | `ORDER BY col <-> query LIMIT k` |
| Text | FTS (Inverted Index) | `WHERE MATCH(col) AGAINST('query')` |
| Spatial | i-Octree (3D) | `ST_DISTANCE`, KNN, radius search |

### Embedded Optimized

- **Low memory**: 222 B/row (vs SQLite's 335 B — 34% less); 查询期 RSS 增量 ≈0
  (默认档向量缓存预算 64MB, `set_vector_cache_budget` 按表调整)
- **Zero-copy reads**: mmap with on-demand page loading; 无索引向量扫描为
  字节距离核零拷贝 (非对齐 SIMD 直读页缓存)
- **Fast writes**: 列式批量导入 insert_arrays — GroupCommit 耐久档 197K
  rows/s / NoSync 档 286K (100K×384 口径); fast path 段直写耐久
  (temp+fsync+rename 原子发布 + manifest fsync), WAL 只服务慢路径
- **Auto-checkpoint 双触发**: WAL 大小 + 段计数 (`max_segment_count`,
  默认 32; edge preset 16) — WAL-less 批量导入的段阵有界
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

## Installation

```bash
cargo add motedb
```

Or in `Cargo.toml`:
```toml
[dependencies]
motedb = "0.9"
```

For minimal edge builds (no tokenizer, no parallelism), disable default features:
```toml
[dependencies]
motedb = { version = "0.9", default-features = false, features = ["jemalloc"] }
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
EXISTS]` / `DROP INDEX`, `ALTER TABLE` (`ADD COLUMN`, `AUTO_INCREMENT = N`),
`INSERT` (含 `INSERT ... SELECT`), `UPDATE`, `DELETE`, `SELECT` with:

- `WHERE`, `JOIN` (INNER / LEFT / RIGHT / FULL), subqueries in `WHERE`,
  `FROM (SELECT ...)` 派生表
- `GROUP BY` (columns, expressions like `id % 5`, and SELECT aliases), `HAVING` (incl. aggregate aliases), `ORDER BY` (multi-key, expressions, non-projected columns, `NULLS FIRST/LAST`), `LIMIT/OFFSET`
- `DISTINCT` (rows) and `COUNT(DISTINCT col)` aggregates
- Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV`, `VARIANCE`
- `UNION` / `UNION ALL`
- `CASE WHEN ... THEN ... ELSE ... END`
- `WITH` / Common Table Expressions (non-recursive; `WITH name [(cols)] AS
  (SELECT ...), ... <main query>`)
- Scalar functions: `UPPER`/`LOWER`/`LENGTH`/`TRIM`/`SUBSTR`/`REPLACE`/`CONCAT` (skips NULL args, SQLite/Postgres semantics; `||` propagates NULL)/`INSTR`/`COALESCE`/`ROUND` (binary-exact decimal rounding)/`ABS`/arithmetic
- Multimodal predicates: `MATCH(col) AGAINST('q')` (BM25 ranked FTS;
  multi-word default is AND, explicit `a OR b` unions — FTS5-compatible),
  vector `<->`/`<~>` ordering (DiskANN ANN), geometry `loc <-> ST_POINT(x, y)`
  distance ordering, `ST_WITHIN`, `ST_DISTANCE`, `ST_KNN`
- Transactions: `BEGIN` / `COMMIT` / `ROLLBACK`, savepoints, read-your-writes
  visibility inside a transaction

**Not yet supported:** `WITH RECURSIVE` (the keyword is accepted but
self-referencing CTEs error out), `DECIMAL`/`DATE`/`BLOB` types, window
functions, and cross-statement server-side cursors.

## Documentation

Full guides live in [`docs/`](docs/):

- [Quick start](docs/01-quick-start.md) · [Installation & config](docs/02-installation.md) · [SQL operations](docs/03-sql-operations.md)
- [Batch operations](docs/04-batch-operations.md) · [Transactions](docs/05-transactions.md)
- Indexes: [overview](docs/06-indexes-overview.md) · [column](docs/07-column-index.md) · [vector](docs/08-vector-index.md) · [text](docs/09-text-index.md) · [spatial](docs/10-spatial-index.md) · [timestamp](docs/11-timestamp-index.md)
- [Performance tuning](docs/12-performance.md) · [Data types](docs/13-data-types.md) · [API reference](docs/14-api-reference.md) · [Best practices](docs/15-best-practices.md) · [FAQ](docs/16-faq.md)

API docs: <https://docs.rs/motedb>

## License

MIT — see [LICENSE](LICENSE).
