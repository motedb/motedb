# MoteDB

**AI-native embedded multimodal database for embodied intelligence.**
Columnar storage engine with ACID transactions, vector search, full-text search, and spatial indexing — in a single embedded library.

[![Rust](https://img.shields.io/badge/rust-1.70+-orange.svg)](https://rust-lang.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## Quick Start

```rust
use motedb::{Database, DBConfig};

// Create or open an embedded database
let db = Database::create("my_data")?;

// SQL with multimodal support
db.execute("CREATE TABLE products (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name TEXT,
    price FLOAT,
    embedding VECTOR(128)
)")?;

// Fast batch insert
db.batch_insert("products", rows)?;

// Point query
let result = db.execute("SELECT * FROM products WHERE id = 42")?;

// Full-text search
db.execute("CREATE TEXT INDEX idx_name ON products(name)")?;
let results = db.execute("SELECT * FROM products WHERE MATCH(name) AGAINST('wireless')")?;

// Vector similarity search
db.execute("CREATE VECTOR INDEX idx_vec ON products(embedding)")?;
let neighbors = db.vector_search("idx_vec", &query_vector, 10)?;

// Spatial KNN (3D point cloud)
db.execute("CREATE SPATIAL INDEX idx_pos ON products(position)")?;
let nearby = db.ioctree_knn_query("idx_pos", &point, 5)?;
```

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
motedb = "0.3"
```

## Configuration

```rust
use motedb::DBConfig;

// Edge device (low memory, periodic fsync)
let config = DBConfig::for_edge();

// Robotics (fast writes, vector support)
let config = DBConfig::for_robotics();

// Custom
let config = DBConfig {
    wal_config: WALConfig { durability_level: DurabilityLevel::GroupCommit { max_interval_ms: 10 }, .. },
    lsm_config: LSMConfig { memtable_size: 1 * 1024 * 1024, .. },
    ..DBConfig::for_edge()
};
let db = Database::create_with_config("my_data", config)?;
```

## License

MIT
