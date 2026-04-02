# Index System Overview

MoteDB supports five major index types, each optimized for different query scenarios.

## Index Types Summary

| Index Type | Use Case | Performance Boost | Applicable Scenarios |
|---------|------|---------|---------|
| [Column Index](#column-index-column) | Equality/range queries | 40x | WHERE clauses, JOINs |
| [Vector Index](#vector-index-vector) | KNN similarity search | 100x | RAG, recommendation systems |
| [Full-text Index](#full-text-index-text) | BM25 text search | 50x | Document retrieval, log analysis |
| [Spatial Index](#spatial-index-spatial) | Geolocation queries | 30x | LBS, trajectory analysis |
| [Time Series](#time-series-index-timestamp) | Time range queries | 20x | Logs, sensor data |

## Column Index (COLUMN)

### Overview
LSM-Tree-based column index supporting fast equality and range queries.

### Creating an Index

```rust
// Method 1: SQL
db.execute("CREATE INDEX users_email ON users(email)")?;

// Method 2: API
db.create_column_index("users", "email")?;
```

### Use Cases

```rust
// WHERE equality query (40x performance boost)
db.query("SELECT * FROM users WHERE email = 'alice@example.com'")?;

// WHERE range query
db.query("SELECT * FROM users WHERE age >= 20 AND age <= 30")?;

// JOIN query optimization
db.query("
    SELECT u.name, o.order_id
    FROM users u
    JOIN orders o ON u.id = o.user_id
    WHERE u.email = 'alice@example.com'
")?;
```

### Performance Data
- **Equality query**: 40x improvement (100ms -> 2.5ms)
- **Range query**: 25x improvement
- **Memory usage**: ~500KB per 10,000 rows

**Detailed documentation**: [Column Index Guide](./07-column-index.md)

## Vector Index (VECTOR)

### Overview
DiskANN-based vector index supporting high-performance KNN similarity search.

### Creating an Index

```rust
// Method 1: SQL
db.execute("CREATE VECTOR INDEX docs_embedding ON documents(embedding)")?;

// Method 2: API
db.create_vector_index("docs_embedding", 128)?;
```

### Use Cases

```rust
// KNN search (find the 10 most similar vectors)
db.query("
    SELECT * FROM documents
    ORDER BY embedding <-> [0.1, 0.2, ..., 0.5]
    LIMIT 10
")?;

// Three supported distance metrics
// <->  L2 Distance (Euclidean)
// <#>  Inner Product
// <=>  Cosine Distance
```

### Performance Data
- **Query latency**: < 5ms (100K vectors)
- **Recall**: 95%+ (@k=10)
- **Throughput**: 74,761 vectors/sec (bulk insert)

**Detailed documentation**: [Vector Index Guide](./08-vector-index.md)

## Full-text Index (TEXT)

### Overview
BM25 algorithm-based full-text index supporting Chinese and English tokenization and keyword search.

### Creating an Index

```rust
// Method 1: SQL
db.execute("CREATE TEXT INDEX articles_content ON articles(content)")?;

// Method 2: API
db.create_text_index("articles_content")?;
```

### Use Cases

```rust
// Full-text search
db.query("
    SELECT * FROM articles
    WHERE MATCH(content, 'rust database')
")?;

// Sorted by BM25 score
db.query("
    SELECT *, BM25_SCORE(content, 'rust database') as score
    FROM articles
    WHERE MATCH(content, 'rust database')
    ORDER BY score DESC
    LIMIT 20
")?;
```

### Performance Data
- **Query latency**: < 10ms (10,000 documents)
- **Index size**: ~6.78MB (10,000 documents)
- **Chinese and English tokenization**: jieba tokenizer

**Detailed documentation**: [Full-text Index Guide](./09-text-index.md)

## Spatial Index (SPATIAL)

### Overview
R-Tree-based spatial index supporting two-dimensional spatial range queries.

### Creating an Index

```rust
use motedb::BoundingBox;

// Define spatial bounds
let bounds = BoundingBox {
    min_x: -180.0,
    min_y: -90.0,
    max_x: 180.0,
    max_y: 90.0,
};

// Method 1: SQL (requires bounds to be set via API first)
db.create_spatial_index("locations_coords", bounds)?;
db.execute("CREATE SPATIAL INDEX locations_coords ON locations(coords)")?;

// Method 2: API
db.create_spatial_index("locations_coords", bounds)?;
```

### Use Cases

```rust
// Bounding box range query
db.query("
    SELECT * FROM locations
    WHERE ST_WITHIN(coords, 116.0, 39.0, 117.0, 40.0)
")?;

// Distance query
db.query("
    SELECT *, ST_DISTANCE(coords, 116.4, 39.9) as distance
    FROM locations
    WHERE ST_DISTANCE(coords, 116.4, 39.9) < 10.0
    ORDER BY distance ASC
")?;
```

### Performance Data
- **Query latency**: < 5ms (50,000 points)
- **Bulk insert**: 50,000 points in ~85ms
- **Memory usage**: ~2MB (50,000 points)

**Detailed documentation**: [Spatial Index Guide](./10-spatial-index.md)

## Time Series Index (TIMESTAMP)

### Overview
Index optimized for time series data, supporting efficient time range queries.

### Creating an Index

```rust
// Automatically supported when a table includes a TIMESTAMP field
db.execute("CREATE TABLE sensor_data (
    id INT,
    sensor_id INT,
    value FLOAT,
    timestamp TIMESTAMP
)")?;
```

### Use Cases

```rust
// Time range query
db.query("
    SELECT * FROM sensor_data
    WHERE timestamp >= 1609459200
      AND timestamp <= 1640995200
")?;

// Combined with aggregation
db.query("
    SELECT
        sensor_id,
        AVG(value) as avg_value,
        COUNT(*) as count
    FROM sensor_data
    WHERE timestamp >= 1609459200
      AND timestamp <= 1640995200
    GROUP BY sensor_id
")?;
```

### Performance Data
- **Query latency**: < 8ms (100,000 data points)
- **Range query**: 20x improvement

**Detailed documentation**: [Time Series Index Guide](./11-timestamp-index.md)

## Index Selection Guide

### Scenario 1: User Management System

```rust
db.execute("CREATE TABLE users (
    id INT,
    email TEXT,
    age INT,
    created_at TIMESTAMP
)")?;

// Create column index (email is frequently used in WHERE)
db.execute("CREATE INDEX users_email ON users(email)")?;

// TIMESTAMP field automatically supports time range queries
```

### Scenario 2: Document Retrieval System (RAG)

```rust
db.execute("CREATE TABLE documents (
    id INT,
    title TEXT,
    content TEXT,
    embedding VECTOR(768),
    created_at TIMESTAMP
)")?;

// Vector index (semantic search)
db.execute("CREATE VECTOR INDEX docs_embedding ON documents(embedding)")?;

// Full-text index (keyword search)
db.execute("CREATE TEXT INDEX docs_content ON documents(content)")?;
```

### Scenario 3: LBS Application

```rust
db.execute("CREATE TABLE pois (
    id INT,
    name TEXT,
    category TEXT,
    coords VECTOR(2)
)")?;

// Spatial index (geolocation queries)
let bounds = BoundingBox {
    min_x: -180.0, min_y: -90.0,
    max_x: 180.0, max_y: 90.0
};
db.create_spatial_index("pois_coords", bounds)?;

// Column index (category filtering)
db.execute("CREATE INDEX pois_category ON pois(category)")?;
```

### Scenario 4: IoT Sensor Data

```rust
db.execute("CREATE TABLE sensor_readings (
    id INT,
    sensor_id INT,
    value FLOAT,
    location VECTOR(2),
    timestamp TIMESTAMP
)")?;

// Column index (query by sensor ID)
db.execute("CREATE INDEX readings_sensor ON sensor_readings(sensor_id)")?;

// Spatial index (query by location)
db.create_spatial_index("readings_location", bounds)?;

// TIMESTAMP (time range queries) automatically supported
```

## Index Management

### View Indexes

```rust
let result = db.query("SHOW INDEXES FROM users")?;
```

### Drop an Index

```rust
db.execute("DROP INDEX users_email ON users")?;
```

### Index Statistics

```rust
// Vector index statistics
let stats = db.vector_index_stats("docs_embedding")?;
println!("Total vectors: {}", stats.total_vectors);

// Spatial index statistics
let stats = db.spatial_index_stats("locations_coords")?;
println!("Total spatial points: {}", stats.total_entries);
```

## Performance Comparison

### Without Index vs. With Index

```rust
// Test data: 10,000 records

// Without index: 100ms (full table scan)
db.query("SELECT * FROM users WHERE email = 'alice@example.com'")?;

// With column index: 2.5ms (40x performance improvement)
db.execute("CREATE INDEX users_email ON users(email)")?;
db.query("SELECT * FROM users WHERE email = 'alice@example.com'")?;
```

### Bulk Insert Performance

| Data Type | Count | Insert Time | Throughput |
|---------|-------|---------|--------|
| Regular data | 10,000 | 14ms | 737,112 rows/sec |
| Vector data (128-dim) | 1,000 | 13ms | 74,761 vectors/sec |
| Spatial data | 50,000 | 85ms | 588,235 points/sec |

## Best Practices

1. **Insert data first, create indexes later** (for large datasets)
2. **Use indexes judiciously**: more is not always better; indexes consume memory
3. **Regularly review statistics**: monitor index performance
4. **Prefer batch operations**: use `batch_insert_map()` for better performance

## Next Steps

- [Column Index Guide](./07-column-index.md)
- [Vector Index Guide](./08-vector-index.md)
- [Full-text Index Guide](./09-text-index.md)
- [Spatial Index Guide](./10-spatial-index.md)
- [Time Series Index Guide](./11-timestamp-index.md)

---

**Previous**: [Transaction Management](./05-transactions.md)
**Next**: [Column Index](./07-column-index.md)
