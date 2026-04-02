# Batch Operations

High-performance bulk inserts, 10-20x faster than row-by-row inserts.

## Core API

```rust
// Batch insert (using HashMap, recommended)
db.batch_insert_map(
    table_name: &str,
    rows: Vec<HashMap<String, Value>>
) -> Result<Vec<RowId>>

// Batch insert with vector data
db.batch_insert_with_vectors_map(
    table_name: &str,
    rows: Vec<HashMap<String, Value>>,
    vector_columns: &[&str]
) -> Result<Vec<RowId>>
```

## Performance Comparison

| Method | 10,000 Rows | Throughput | Speedup |
|-----|------------|--------|---------|
| Row-by-row SQL INSERT | ~5000ms | 2,000 rows/sec | Baseline |
| batch_insert_map | ~14ms | 737,112 rows/sec | **368x** |

## Basic Batch Insert

### Example 1: Simple Data

```rust
use motedb::{Database, types::{Value, SqlRow}};
use std::collections::HashMap;

let db = Database::open("data.mote")?;

// Create table
db.execute("CREATE TABLE users (
    id INT,
    name TEXT,
    email TEXT,
    age INT
)")?;

// Prepare data
let mut rows = Vec::new();
for i in 0..10000 {
    let mut row = HashMap::new();
    row.insert("id".to_string(), Value::Integer(i));
    row.insert("name".to_string(), Value::Text(format!("User{}", i)));
    row.insert("email".to_string(), Value::Text(format!("user{}@example.com", i)));
    row.insert("age".to_string(), Value::Integer(20 + (i % 50)));
    rows.push(row);
}

// Batch insert (very fast!)
let row_ids = db.batch_insert_map("users", rows)?;
println!("Inserted {} rows", row_ids.len());

// Persist to disk
db.flush()?;
```

**Performance**: 10,000 rows in ~14ms, throughput 737,112 rows/sec

### Example 2: Mixed Data Types

```rust
let mut rows = Vec::new();
for i in 0..1000 {
    let mut row = HashMap::new();
    row.insert("id".to_string(), Value::Integer(i));
    row.insert("name".to_string(), Value::Text(format!("Product{}", i)));
    row.insert("price".to_string(), Value::Float((i as f64) * 9.99));
    row.insert("in_stock".to_string(), Value::Bool(i % 2 == 0));
    row.insert("created_at".to_string(), Value::Integer(1609459200 + i));
    rows.push(row);
}

db.batch_insert_map("products", rows)?;
```

## Batch Insert with Vector Data

### Example 3: Document Embeddings

```rust
// Create table (with vector column)
db.execute("CREATE TABLE documents (
    id INT,
    title TEXT,
    content TEXT,
    embedding VECTOR(128)
)")?;

// Create vector index
db.execute("CREATE VECTOR INDEX docs_embedding ON documents(embedding)")?;

// Prepare vector data
let mut rows = Vec::new();
for i in 0..1000 {
    let mut row = HashMap::new();
    row.insert("id".to_string(), Value::Integer(i));
    row.insert("title".to_string(), Value::Text(format!("Document {}", i)));
    row.insert("content".to_string(), Value::Text(format!("Content of document {}", i)));

    // Generate 128-dimensional vector (in practice, use real embeddings)
    let embedding: Vec<f32> = (0..128).map(|j| (i as f32 + j as f32) / 1000.0).collect();
    row.insert("embedding".to_string(), Value::Vector(embedding));

    rows.push(row);
}

// Batch insert (automatically triggers vector index construction)
let row_ids = db.batch_insert_with_vectors_map("documents", rows, &["embedding"])?;

db.flush()?;
```

**Performance**: 1,000 rows x 128-dimensional vectors in ~13ms, throughput 74,761 vectors/sec

## Batch Insert with Spatial Data

### Example 4: Geographic Locations

```rust
use motedb::BoundingBox;

// Create table
db.execute("CREATE TABLE locations (
    id INT,
    name TEXT,
    coords VECTOR(2),
    category TEXT
)")?;

// Create spatial index
let bounds = BoundingBox {
    min_x: -180.0, min_y: -90.0,
    max_x: 180.0, max_y: 90.0,
};
db.create_spatial_index("locations_coords", bounds)?;

// Batch insert location data
let mut rows = Vec::new();
for i in 0..5000 {
    let mut row = HashMap::new();
    row.insert("id".to_string(), Value::Integer(i));
    row.insert("name".to_string(), Value::Text(format!("Location {}", i)));

    // Generate coordinates (longitude, latitude)
    let lon = -180.0 + (i as f32 * 0.072) % 360.0;
    let lat = -90.0 + (i as f32 * 0.036) % 180.0;
    row.insert("coords".to_string(), Value::Vector(vec![lon, lat]));

    row.insert("category".to_string(), Value::Text(
        if i % 3 == 0 { "Restaurant" }
        else if i % 3 == 1 { "Hotel" }
        else { "Attraction" }.to_string()
    ));

    rows.push(row);
}

db.batch_insert_map("locations", rows)?;
db.flush()?;
```

## Batch Insert + Indexes

### Recommended Pattern 1: Insert First, Create Index After

```rust
// 1. Batch insert data
let row_ids = db.batch_insert_map("users", rows)?;

// 2. Create column index (built automatically)
db.execute("CREATE INDEX users_email ON users(email)")?;

// 3. Persist to disk
db.flush()?;
```

**Advantage**: Index is built in a single pass, highest efficiency.

### Recommended Pattern 2: Create Index First, Auto-update During Batch Insert

```rust
// 1. Create index first
db.execute("CREATE INDEX users_email ON users(email)")?;

// 2. Batch insert (index is incrementally updated automatically)
let row_ids = db.batch_insert_map("users", rows)?;

// 3. Persist to disk
db.flush()?;
```

**Advantage**: Suitable for continuous write scenarios.

## Batch Insert for Large Datasets

For very large datasets (millions of rows), batch insertion in chunks is recommended:

```rust
const BATCH_SIZE: usize = 10000;
let total_rows = 1_000_000;

for batch_start in (0..total_rows).step_by(BATCH_SIZE) {
    let batch_end = (batch_start + BATCH_SIZE).min(total_rows);

    // Prepare current batch data
    let mut batch_rows = Vec::new();
    for i in batch_start..batch_end {
        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::Integer(i as i64));
        row.insert("data".to_string(), Value::Text(format!("Data {}", i)));
        batch_rows.push(row);
    }

    // Batch insert
    db.batch_insert_map("large_table", batch_rows)?;

    // Flush every 10 batches
    if (batch_start / BATCH_SIZE) % 10 == 0 {
        db.flush()?;
        println!("Processed {} rows", batch_end);
    }
}

// Final flush
db.flush()?;
```

## Batch Insert + Transactions

```rust
// Begin transaction
let tx_id = db.begin_transaction()?;

// Batch insert
let row_ids = db.batch_insert_map("users", rows)?;

// Check result
if row_ids.len() == rows.len() {
    db.commit_transaction(tx_id)?;
    println!("Successfully inserted {} rows", row_ids.len());
} else {
    db.rollback_transaction(tx_id)?;
    println!("Insert failed, transaction rolled back");
}
```

## Performance Optimization Tips

### 1. Use an Appropriate Batch Size

```rust
// Recommended batch sizes
const OPTIMAL_BATCH_SIZE: usize = 10000;

// Small batches (< 1000): marginal performance improvement
// Medium batches (1000-10000): optimal performance
// Large batches (> 50000): increased memory usage
```

### 2. Deferred Index Construction

```rust
// Large datasets: insert first, create index after
db.batch_insert_map("users", rows)?;
db.execute("CREATE INDEX users_email ON users(email)")?;
```

### 3. Adjust Memory Configuration

```rust
use motedb::DBConfig;

let config = DBConfig {
    memtable_size_mb: 32,  // Increase memtable size
    ..Default::default()
};

let db = Database::create_with_config("data.mote", config)?;
```

### 4. Disable Auto-flush (for Bulk Write Scenarios)

```rust
let config = DBConfig {
    auto_flush_interval: 300,  // Extend to 5 minutes
    ..Default::default()
};

// Or manually control flushing
db.batch_insert_map("users", batch1)?;
db.batch_insert_map("users", batch2)?;
db.batch_insert_map("users", batch3)?;
db.flush()?;  // Single flush
```

## Common Errors

### Error 1: Field Name Mismatch

```rust
// Wrong: field names don't match
let mut row = HashMap::new();
row.insert("user_id".to_string(), Value::Integer(1));  // table field is 'id'
row.insert("user_name".to_string(), Value::Text("Alice".into()));

// Correct: field names must match the table schema
let mut row = HashMap::new();
row.insert("id".to_string(), Value::Integer(1));
row.insert("name".to_string(), Value::Text("Alice".into()));
```

### Error 2: Type Mismatch

```rust
// Wrong: type mismatch
row.insert("age".to_string(), Value::Text("25".into()));  // should be Integer

// Correct
row.insert("age".to_string(), Value::Integer(25));
```

### Error 3: Forgetting to Flush

```rust
// Wrong: data may be lost
db.batch_insert_map("users", rows)?;
// Program exits, data not persisted

// Correct
db.batch_insert_map("users", rows)?;
db.flush()?;  // Ensure data is persisted
```

## Complete Example: Multi-modal Data Bulk Import

```rust
use motedb::{Database, types::{Value, SqlRow}, BoundingBox};
use std::collections::HashMap;

fn main() -> motedb::Result<()> {
    let db = Database::open("multimodal.mote")?;

    // Create table
    db.execute("CREATE TABLE robot_observations (
        id INT,
        timestamp INT,
        location VECTOR(2),
        image_embedding VECTOR(512),
        description TEXT,
        confidence FLOAT
    )")?;

    // Create indexes
    db.execute("CREATE VECTOR INDEX obs_embedding ON robot_observations(image_embedding)")?;

    let bounds = BoundingBox { min_x: 0.0, min_y: 0.0, max_x: 100.0, max_y: 100.0 };
    db.create_spatial_index("obs_location", bounds)?;

    // Batch insert
    let mut rows = Vec::new();
    for i in 0..1000 {
        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::Integer(i));
        row.insert("timestamp".to_string(), Value::Integer(1609459200 + i));
        row.insert("location".to_string(), Value::Vector(vec![
            (i as f32) % 100.0,
            ((i * 2) as f32) % 100.0
        ]));
        row.insert("image_embedding".to_string(), Value::Vector(
            (0..512).map(|j| (i as f32 + j as f32) / 10000.0).collect()
        ));
        row.insert("description".to_string(), Value::Text(format!("Observation {}", i)));
        row.insert("confidence".to_string(), Value::Float(0.8 + (i % 20) as f64 / 100.0));
        rows.push(row);
    }

    let row_ids = db.batch_insert_with_vectors_map(
        "robot_observations",
        rows,
        &["image_embedding"]
    )?;

    println!("Inserted {} observations", row_ids.len());

    db.flush()?;
    Ok(())
}
```

## Next Steps

- [Transaction Management](./05-transactions.md) - Transactions + batch operations
- [Index System](./06-indexes-overview.md) - Optimize queries after batch inserts
- [Performance Tuning](./12-performance.md) - In-depth performance optimization

---

**Previous**: [SQL Operations](./03-sql-operations.md)
**Next**: [Transaction Management](./05-transactions.md)
