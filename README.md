# MoteDB

**AI-native embedded multimodal database for embodied intelligence.**
Columnar storage engine with ACID transactions, vector search, full-text search, and spatial indexing — in a single embedded library.

[![Rust](https://img.shields.io/badge/rust-1.74+-orange.svg)](https://rust-lang.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![crates.io](https://img.shields.io/crates/v/motedb.svg)](https://crates.io/crates/motedb)

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

## Performance

Benchmark: 300K rows × 4 columns on Apple Silicon M-series vs SQLite 3.x WAL mode.

| Operation | MoteDB | SQLite | Winner |
|-----------|--------|--------|--------|
| INSERT 300K | 125ms | 85ms | MoteDB (1.5x) |
| CREATE INDEX ×2 | 30ms | 90ms | MoteDB (3x) |
| WHERE = | 11ms | 14ms | MoteDB (1.3x) |
| ORDER BY LIMIT | 2.6ms | 6.5ms | MoteDB (2.5x) |
| COUNT/SUM/AVG WHERE | 2.8ms | 14ms | MoteDB (5x) |
| PK SELECT | <1μs | 1μs | MoteDB |
| LIKE | 13ms | 10ms | SQLite (1.3x) |
| DISTINCT | 8.5ms | 4.6ms | SQLite (1.9x) |
| SELECT * | 27ms | 9.7ms | SQLite (2.8x) |

**Memory: 257 B/row (vs SQLite 369 B/row — 30% less)**

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
│  │ (fsync) │  │ Buffer   │  │ (mmap + Snappy)     │ │
│  └─────────┘  └──────────┘  └────────────────────┘ │
├─────────────────────────────────────────────────────┤
│  MVCC Transaction │ Snapshot Isolation │ Conflict  │
└─────────────────────────────────────────────────────┘
```

- **Storage**: Columnar SSTable with Snappy compression, mmap zero-copy access
- **Write Path**: WAL (durability) → columnar buffer → auto-finalize → SSTable
- **Read Path**: SelectColumnar (zero-materialization), typed array access, predicate pushdown
- **Transactions**: VersionStore MVCC with snapshot isolation and conflict detection

## Features

### Multimodal

| Modality | Index | Query |
|----------|-------|-------|
| Tabular | Column Value (B-tree) | `WHERE`, `ORDER BY`, `GROUP BY` |
| Vector | DiskANN (Vamana graph) | `ORDER BY col <-> query LIMIT k` |
| Text | FTS (Inverted Index) | `WHERE MATCH(col) AGAINST('query')` |
| Spatial | i-Octree (3D) | `ST_DISTANCE`, KNN, radius search |

### Embedded Optimized

- **Low memory**: 257 B/row (30% less than SQLite)
- **Zero-copy reads**: mmap with on-demand page loading
- **Fast writes**: Zero-encode columnar INSERT (2.4M rows/s)
- **Small disk**: Snappy compression (~1.8x, 68 B/row)
- **No daemon**: Single library, embedded directly

### ACID

- **Atomic**: WAL-based crash recovery
- **Consistent**: PK uniqueness, NOT NULL, type coercion
- **Isolated**: MVCC snapshot isolation
- **Durable**: WAL fsync + auto-finalize

## Installation

```bash
cargo add motedb
```

Or in `Cargo.toml`:
```toml
[dependencies]
motedb = "0.5"
```

For minimal edge builds (no tokenizer, no parallelism), disable default features:
```toml
[dependencies]
motedb = { version = "0.5", default-features = false, features = ["jemalloc"] }
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

**Supported:** `CREATE TABLE` / `CREATE INDEX` (column, vector, text, spatial),
`DROP TABLE [IF EXISTS]`, `INSERT`, `UPDATE`, `DELETE`, `SELECT` with
`WHERE`, `JOIN` (INNER/LEFT/RIGHT/FULL), `GROUP BY`, `ORDER BY`, `LIMIT/OFFSET`,
`DISTINCT`, aggregates (`COUNT`, `SUM`, `MIN`, `MAX`), and transactions
(`BEGIN`/`COMMIT`/`ROLLBACK`).

**Not yet supported:** `WITH`/CTE/recursive queries, `COUNT(DISTINCT ...)`,
multi-column `GROUP BY`, `DECIMAL`/`DATE`/`BLOB` types, and full-text search
predicate functions (the FTS index builds, but `MATCH ... AGAINST` is incomplete).

## Documentation

Full guides live in [`docs/`](docs/):

- [Quick start](docs/01-quick-start.md) · [Installation & config](docs/02-installation.md) · [SQL operations](docs/03-sql-operations.md)
- [Batch operations](docs/04-batch-operations.md) · [Transactions](docs/05-transactions.md)
- Indexes: [overview](docs/06-indexes-overview.md) · [column](docs/07-column-index.md) · [vector](docs/08-vector-index.md) · [text](docs/09-text-index.md) · [spatial](docs/10-spatial-index.md) · [timestamp](docs/11-timestamp-index.md)
- [Performance tuning](docs/12-performance.md) · [Data types](docs/13-data-types.md) · [API reference](docs/14-api-reference.md) · [Best practices](docs/15-best-practices.md) · [FAQ](docs/16-faq.md)

API docs: <https://docs.rs/motedb>

## License

MIT — see [LICENSE](LICENSE).
