# MoteDB Documentation

The world's first AI-native embedded database designed for embodied intelligence scenarios.

It is an embedded database purpose-built for edge devices such as home robots, AR glasses, and industrial robotic arms, with native support for unified storage and querying of vectors, text, time series, and spatial coordinates. MoteDB treats multimodal data types as first-class citizens, providing low-latency online retrieval, strongly consistent data semantics, and query extensions tailored for embodied intelligence perception and decision-making.
## 📚 Documentation Index

### Getting Started
- **Quick Start Guide**: [./01-quick-start.md](./01-quick-start.md) - Get started with MoteDB in 5 minutes
- **Installation & Configuration**: [./02-installation.md](./02-installation.md) - Installation, configuration, and deployment parameters

### Core Features
- **SQL Operations**: [./03-sql-operations.md](./03-sql-operations.md) - Complete SQL syntax and query templates
- **Batch Operations**: [./04-batch-operations.md](./04-batch-operations.md) - High-performance batch writes (10-20x improvement)
- **Transaction Management**: [./05-transactions.md](./05-transactions.md) - MVCC, WAL, and savepoint usage
- **API Reference**: [./14-api-reference.md](./14-api-reference.md) - All public APIs with examples

### Index System
- **Index Overview**: [./06-indexes-overview.md](./06-indexes-overview.md) - How the five index types work together
- **Column Index**: [./07-column-index.md](./07-column-index.md) - Equality and range queries
- **Vector Index**: [./08-vector-index.md](./08-vector-index.md) - FreshDiskANN & rerank
- **Full-Text Index**: [./09-text-index.md](./09-text-index.md) - BM25 and tokenization plugins
- **Spatial Index**: [./10-spatial-index.md](./10-spatial-index.md) - R-Tree and geospatial queries
- **Time Series Index**: [./11-timestamp-index.md](./11-timestamp-index.md) - Range scans and compression

### Advanced Topics
- **Performance Tuning**: [./12-performance.md](./12-performance.md) - Configuration, tuning, and monitoring
- **Data Types**: [./13-data-types.md](./13-data-types.md) - `Value` enum and schema design

### Best Practices
- **Production Experience**: [./15-best-practices.md](./15-best-practices.md) - Architecture, writes, and indexing strategies
- **FAQ**: [./16-faq.md](./16-faq.md) - Debugging, deployment, and troubleshooting

## 🚀 Core Features

### 1. **SQL Engine**
Full SQL support, including subqueries, aggregation, JOINs, and index management.

```rust
let db = Database::open("data.mote")?;
db.execute("CREATE TABLE users (id INT, name TEXT, email TEXT)")?;
db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')")?;
let results = db.query("SELECT * FROM users WHERE id = 1")?;
```

### 2. **Multimodal Indexes**
Five index types supporting different data scenarios:

| Index Type | Use Case | Performance Boost |
|---------|------|---------|
| Column Index (COLUMN) | Equality/range queries | 40x |
| Vector Index (VECTOR) | KNN similarity search | 100x |
| Full-Text Index (TEXT) | BM25 text search | 50x |
| Spatial Index (SPATIAL) | Geospatial queries | 30x |
| Time Series (TIMESTAMP) | Time range queries | 20x |

### 3. **High-Performance Batch Operations**
Batch inserts are 10-20x faster than row-by-row inserts:

```rust
// Batch insert 10,000 records
let mut rows = Vec::new();
for i in 0..10000 {
    let mut row = HashMap::new();
    row.insert("id".to_string(), Value::Integer(i));
    row.insert("name".to_string(), Value::Text(format!("User{}", i)));
    rows.push(row);
}

let row_ids = db.batch_insert_map("users", rows)?;
// Throughput: 737,112 rows/sec
```

### 4. **MVCC Transactions**
Full transaction support, including savepoints:

```rust
let tx_id = db.begin_transaction()?;

db.execute("INSERT INTO users VALUES (1, 'Alice', 25)")?;
db.savepoint(tx_id, "sp1")?;

db.execute("INSERT INTO users VALUES (2, 'Bob', 30)")?;
db.rollback_to_savepoint(tx_id, "sp1")?; // Only rolls back Bob

db.commit_transaction(tx_id)?;
```

## 🎯 Use Cases

- **Embedded AI Applications**: Robots, edge computing devices
- **Vector Database**: RAG, semantic search, recommendation systems
- **Spatiotemporal Data**: Geospatial locations, sensor data
- **Full-Text Search**: Document retrieval, log analysis
- **Real-Time Analytics**: Time series data

## 📊 Performance Metrics

- **Batch Insert**: 737,112 rows/sec (10,000 records)
- **Vector Search**: Latency < 5ms (95% recall)
- **Column Index Query**: 40x improvement
- **Memory Usage**: Core data structures < 100MB
- **Transaction Throughput**: 10,000 TPS

## 💡 Recommended Usage

1. **Use the SQL API primarily** - concise, powerful, and easy to use
2. **Prefer batch operations** - use `batch_insert_map()` instead of row-by-row inserts
3. **Use indexes wisely** - choose the appropriate index type based on your query patterns
4. **Enable transactions** - ensure data consistency

## 🔗 Quick Links

- [GitHub Repository](https://github.com/yourusername/motedb)
- [API Documentation](https://docs.rs/motedb)

## 📝 Version Information

Documentation version: v0.1.0
Last updated: 2026-01-11

---

**Next**: Read the [Quick Start Guide](./01-quick-start.md) to begin using MoteDB
