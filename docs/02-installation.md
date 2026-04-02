# Installation & Configuration

Detailed installation instructions and database configuration guide.

## System Requirements

- **Rust**: 1.70 or later
- **Operating System**: Linux, macOS, Windows
- **Memory**: 512MB or more recommended
- **Disk**: Depends on data volume

## Installation Methods

### Method 1: Using Cargo (Recommended)

Add the dependency in your project's `Cargo.toml`:

```toml
[dependencies]
motedb = "0.1"
```

### Method 2: Build from Source

```bash
# Clone the repository
git clone https://github.com/yourusername/motedb.git
cd motedb

# Build the project
cargo build --release

# Run tests
cargo test

# Run examples
cargo run --example quick_start --release
```

## Basic Configuration

### Default Configuration

Create a database with default settings:

```rust
use motedb::Database;

let db = Database::open("myapp.mote")?;
```

### Custom Configuration

Customize the database configuration using `DBConfig`:

```rust
use motedb::{Database, DBConfig};

let config = DBConfig {
    // Memtable size (MB)
    memtable_size_mb: 16,

    // Row cache size (number of entries)
    row_cache_size: 10000,

    // LSM tree levels
    lsm_max_levels: 4,

    // Compression strategy
    compression: true,

    // Enable WAL
    enable_wal: true,

    // Auto-flush interval (seconds)
    auto_flush_interval: 60,

    // Default durability level
    durability_level: motedb::DurabilityLevel::Full,
};

let db = Database::create_with_config("myapp.mote", config)?;
```

## Configuration Parameters in Detail

### Memory Configuration

| Parameter | Default | Description | Recommended Value |
|-----|-------|-----|-------|
| `memtable_size_mb` | 8 | Memtable size | 8-32 MB |
| `row_cache_size` | 10000 | Row cache capacity | 1000-50000 |

#### Example: Low Memory Environment

```rust
let config = DBConfig {
    memtable_size_mb: 4,
    row_cache_size: 1000,
    ..Default::default()
};
```

#### Example: High Performance Environment

```rust
let config = DBConfig {
    memtable_size_mb: 32,
    row_cache_size: 50000,
    ..Default::default()
};
```

### Persistence Configuration

#### DurabilityLevel

Controls the level of data persistence guarantee:

```rust
use motedb::DurabilityLevel;

pub enum DurabilityLevel {
    /// No persistence guarantee (fastest, data may be lost)
    None,

    /// Memory-only flush (faster, data may be lost on process crash)
    Memory,

    /// Full persistence (safest, slightly lower performance)
    Full,
}
```

#### Configuration Examples

```rust
// High performance mode (data loss possible)
let config = DBConfig {
    durability_level: DurabilityLevel::Memory,
    enable_wal: false,
    ..Default::default()
};

// Safe mode (recommended for production)
let config = DBConfig {
    durability_level: DurabilityLevel::Full,
    enable_wal: true,
    auto_flush_interval: 30,
    ..Default::default()
};
```

### LSM-Tree Configuration

```rust
let config = DBConfig {
    // LSM maximum levels
    lsm_max_levels: 4,

    // Enable compression
    compression: true,

    // Bloom Filter (reduces disk reads)
    bloom_filter_bits: 10,

    ..Default::default()
};
```

### Index Configuration

#### Vector Index

```rust
// Configure when creating a vector index
db.execute("CREATE VECTOR INDEX docs_embedding ON documents(embedding)")?;

// High recall configuration (via API)
db.create_vector_index("docs_embedding", 128)?;
// Default configuration: R=32, L=50, alpha=1.2
```

#### Spatial Index

```rust
use motedb::BoundingBox;

let bounds = BoundingBox {
    min_x: -180.0,
    min_y: -90.0,
    max_x: 180.0,
    max_y: 90.0,
};

db.create_spatial_index("locations_coords", bounds)?;
```

## Performance Tuning

### Scenario 1: Write-Heavy Workloads

```rust
let config = DBConfig {
    memtable_size_mb: 32,       // Larger memtable
    row_cache_size: 1000,       // Smaller cache
    enable_wal: false,          // Disable WAL (improves write speed)
    durability_level: DurabilityLevel::Memory,
    auto_flush_interval: 120,   // Longer flush interval
    ..Default::default()
};
```

### Scenario 2: Read-Heavy Workloads

```rust
let config = DBConfig {
    memtable_size_mb: 8,        // Standard memtable
    row_cache_size: 50000,      // Larger cache
    enable_wal: true,
    durability_level: DurabilityLevel::Full,
    bloom_filter_bits: 12,      // Larger Bloom Filter
    ..Default::default()
};
```

### Scenario 3: Balanced Mode (Recommended)

```rust
let config = DBConfig {
    memtable_size_mb: 16,
    row_cache_size: 10000,
    enable_wal: true,
    durability_level: DurabilityLevel::Full,
    auto_flush_interval: 60,
    compression: true,
    ..Default::default()
};
```

## File Structure

MoteDB creates the following files in the data directory:

```
myapp.mote/
├── manifest.json          # Metadata manifest
├── wal/                   # Write-Ahead Log
│   └── 000001.wal
├── tables/                # Table data
│   └── users/
│       ├── data.sst       # SSTable data
│       └── data.idx       # Index files
├── indexes/               # Index data
│   ├── users_email.idx
│   ├── docs_embedding.diskann
│   └── locations_coords.rtree
└── checkpoints/           # Checkpoints
    └── checkpoint_001.dat
```

## Environment Variables

Optional environment variable configuration:

```bash
# Log level
export MOTEDB_LOG_LEVEL=debug

# Data directory
export MOTEDB_DATA_DIR=/var/lib/motedb

# Maximum concurrent connections
export MOTEDB_MAX_CONNECTIONS=100
```

## Common Configuration Issues

### Q1: How to reduce memory usage?

```rust
let config = DBConfig {
    memtable_size_mb: 4,
    row_cache_size: 1000,
    bloom_filter_bits: 8,
    ..Default::default()
};
```

### Q2: How to improve write performance?

```rust
let config = DBConfig {
    memtable_size_mb: 32,
    enable_wal: false,
    durability_level: DurabilityLevel::Memory,
    ..Default::default()
};

// Use batch inserts
db.batch_insert_map("users", rows)?;

// Periodically flush manually
db.flush()?;
```

### Q3: How to ensure data safety?

```rust
let config = DBConfig {
    enable_wal: true,
    durability_level: DurabilityLevel::Full,
    auto_flush_interval: 30,
    ..Default::default()
};

// Manually flush after critical operations
db.execute("INSERT INTO critical_data VALUES (...)")?;
db.flush()?;
```

## Verify Installation

Run the following code to verify the installation:

```rust
use motedb::{Database, Result};

fn main() -> Result<()> {
    let db = Database::open("test.mote")?;
    db.execute("CREATE TABLE test (id INT, name TEXT)")?;
    db.execute("INSERT INTO test VALUES (1, 'Hello MoteDB')")?;
    let results = db.query("SELECT * FROM test")?;

    assert_eq!(results.row_count(), 1);
    println!("MoteDB installed successfully!");

    Ok(())
}
```

## Next Steps

- [Quick Start](./01-quick-start.md) - Learn basic usage
- [SQL Operations](./03-sql-operations.md) - Learn the SQL syntax
- [Performance Tuning](./12-performance.md) - Deep dive into performance optimization

---

**Previous**: [Documentation Home](./README.md)
**Next**: [Quick Start](./01-quick-start.md)
