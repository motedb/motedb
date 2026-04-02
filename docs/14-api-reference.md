# API Reference

Complete API documentation for MoteDB.

## Database Struct

```rust
pub struct Database {
    inner: Arc<MoteDB>,
}
```

## Lifecycle Management

### create

Create a new database.

```rust
pub fn create<P: AsRef<Path>>(path: P) -> Result<Self>
```

**Example**:
```rust
let db = Database::create("myapp.mote")?;
```

### create_with_config

Create a database with custom configuration.

```rust
pub fn create_with_config<P: AsRef<Path>>(
    path: P,
    config: DBConfig
) -> Result<Self>
```

**Example**:
```rust
let config = DBConfig {
    memtable_size_mb: 16,
    row_cache_size: 10000,
    ..Default::default()
};
let db = Database::create_with_config("myapp.mote", config)?;
```

### open

Open an existing database.

```rust
pub fn open<P: AsRef<Path>>(path: P) -> Result<Self>
```

**Example**:
```rust
let db = Database::open("myapp.mote")?;
```

### flush

Flush all data to disk.

```rust
pub fn flush(&self) -> Result<()>
```

**Example**:
```rust
db.execute("INSERT INTO users VALUES (1, 'Alice', 25)")?;
db.flush()?;  // Ensure data is persisted
```

### close

Close the database (explicit call; normally handled automatically by Drop).

```rust
pub fn close(&self) -> Result<()>
```

## SQL Operations

### query

Execute a SQL query and return results.

```rust
pub fn query(&self, sql: &str) -> Result<QueryResult>
```

**Example**:
```rust
let results = db.query("SELECT * FROM users WHERE age > 18")?;
```

### execute

Execute a SQL statement (INSERT/UPDATE/DELETE/CREATE/DROP).

```rust
pub fn execute(&self, sql: &str) -> Result<QueryResult>
```

**Example**:
```rust
db.execute("CREATE TABLE users (id INT, name TEXT)")?;
db.execute("INSERT INTO users VALUES (1, 'Alice')")?;
db.execute("UPDATE users SET name = 'Bob' WHERE id = 1")?;
```

## Transaction Management

### begin_transaction

Begin a new transaction.

```rust
pub fn begin_transaction(&self) -> Result<u64>
```

**Returns**: Transaction ID

**Example**:
```rust
let tx_id = db.begin_transaction()?;
db.execute("INSERT INTO users VALUES (1, 'Alice', 25)")?;
db.commit_transaction(tx_id)?;
```

### commit_transaction

Commit a transaction.

```rust
pub fn commit_transaction(&self, tx_id: u64) -> Result<()>
```

**Example**:
```rust
let tx_id = db.begin_transaction()?;
db.execute("INSERT INTO users VALUES (1, 'Alice', 25)")?;
db.commit_transaction(tx_id)?;
```

### rollback_transaction

Roll back a transaction.

```rust
pub fn rollback_transaction(&self, tx_id: u64) -> Result<()>
```

**Example**:
```rust
let tx_id = db.begin_transaction()?;
db.execute("INSERT INTO users VALUES (1, 'Alice', 25)")?;
db.rollback_transaction(tx_id)?;  // Undo all changes
```

### savepoint

Create a savepoint (a checkpoint within a transaction).

```rust
pub fn savepoint(&self, tx_id: u64, name: &str) -> Result<()>
```

**Example**:
```rust
let tx_id = db.begin_transaction()?;
db.execute("INSERT INTO users VALUES (1, 'Alice', 25)")?;
db.savepoint(tx_id, "sp1")?;

db.execute("INSERT INTO users VALUES (2, 'Bob', 30)")?;
db.rollback_to_savepoint(tx_id, "sp1")?;  // Only roll back Bob

db.commit_transaction(tx_id)?;  // Alice is kept
```

### rollback_to_savepoint

Roll back to a savepoint.

```rust
pub fn rollback_to_savepoint(&self, tx_id: u64, name: &str) -> Result<()>
```

### release_savepoint

Release a savepoint.

```rust
pub fn release_savepoint(&self, tx_id: u64, name: &str) -> Result<()>
```

## Batch Operations

### batch_insert_map

Batch insert rows (using HashMap; 10-20x faster than row-by-row insertion).

```rust
pub fn batch_insert_map(
    &self,
    table_name: &str,
    sql_rows: Vec<SqlRow>
) -> Result<Vec<RowId>>
```

**Parameters**:
- `table_name`: Table name
- `sql_rows`: `Vec<HashMap<String, Value>>`

**Returns**: List of inserted row IDs

**Example**:
```rust
let mut rows = Vec::new();
for i in 0..1000 {
    let mut row = HashMap::new();
    row.insert("id".to_string(), Value::Integer(i));
    row.insert("name".to_string(), Value::Text(format!("User{}", i)));
    rows.push(row);
}

let row_ids = db.batch_insert_map("users", rows)?;
```

### batch_insert_with_vectors_map

Batch insert rows with vectors (automatically builds vector indexes).

```rust
pub fn batch_insert_with_vectors_map(
    &self,
    table_name: &str,
    sql_rows: Vec<SqlRow>,
    vector_columns: &[&str]
) -> Result<Vec<RowId>>
```

**Example**:
```rust
let mut rows = Vec::new();
for i in 0..1000 {
    let mut row = HashMap::new();
    row.insert("id".to_string(), Value::Integer(i));
    row.insert("embedding".to_string(), Value::Vector(vec![0.1; 128]));
    rows.push(row);
}

let row_ids = db.batch_insert_with_vectors_map(
    "documents",
    rows,
    &["embedding"]
)?;
```

## Index Management

### create_column_index

Create a column index (for fast equality and range queries).

```rust
pub fn create_column_index(
    &self,
    table_name: &str,
    column_name: &str
) -> Result<()>
```

**Example**:
```rust
db.create_column_index("users", "email")?;

// Queries will automatically use the index (40x performance improvement)
let results = db.query("SELECT * FROM users WHERE email = 'alice@example.com'")?;
```

### create_vector_index

Create a vector index (for KNN similarity search).

```rust
pub fn create_vector_index(
    &self,
    index_name: &str,
    dimension: usize
) -> Result<()>
```

**Example**:
```rust
db.create_vector_index("docs_embedding", 128)?;

// SQL vector search
let results = db.query("
    SELECT * FROM docs
    ORDER BY embedding <-> [0.1, 0.2, ..., 0.5]
    LIMIT 10
")?;
```

### create_text_index

Create a full-text index (for BM25 text search).

```rust
pub fn create_text_index(&self, index_name: &str) -> Result<()>
```

**Example**:
```rust
db.create_text_index("articles_content")?;

let results = db.query("
    SELECT * FROM articles
    WHERE MATCH(content, 'rust database')
")?;
```

### create_spatial_index

Create a spatial index (for geographic location queries).

```rust
pub fn create_spatial_index(
    &self,
    index_name: &str,
    bounds: BoundingBox
) -> Result<()>
```

**Example**:
```rust
use motedb::BoundingBox;

let bounds = BoundingBox {
    min_x: -180.0,
    min_y: -90.0,
    max_x: 180.0,
    max_y: 90.0,
};
db.create_spatial_index("locations_coords", bounds)?;

let results = db.query("
    SELECT * FROM locations
    WHERE ST_WITHIN(coords, 116.0, 39.0, 117.0, 40.0)
")?;
```

### drop_index

Drop an index.

```rust
pub fn drop_index(
    &self,
    table_name: &str,
    index_name: &str
) -> Result<()>
```

## Query API

### query_by_column

Query by column value (uses column index for equality lookups).

```rust
pub fn query_by_column(
    &self,
    table_name: &str,
    column_name: &str,
    value: &Value
) -> Result<Vec<RowId>>
```

**Example**:
```rust
let row_ids = db.query_by_column(
    "users",
    "email",
    &Value::Text("alice@example.com".into())
)?;
```

### query_by_column_range

Query by column range (uses column index).

```rust
pub fn query_by_column_range(
    &self,
    table_name: &str,
    column_name: &str,
    start: &Value,
    end: &Value
) -> Result<Vec<RowId>>
```

**Example**:
```rust
let row_ids = db.query_by_column_range(
    "users",
    "age",
    &Value::Integer(20),
    &Value::Integer(30)
)?;
```

### vector_search

Vector KNN search.

```rust
pub fn vector_search(
    &self,
    index_name: &str,
    query: &[f32],
    k: usize
) -> Result<Vec<(RowId, f32)>>
```

**Returns**: List of `(row_id, distance)` tuples

**Example**:
```rust
let query_vec = vec![0.1; 128];
let results = db.vector_search("docs_embedding", &query_vec, 10)?;

for (row_id, distance) in results {
    println!("RowID: {}, Distance: {}", row_id, distance);
}
```

### text_search_ranked

Full-text search (BM25 ranked).

```rust
pub fn text_search_ranked(
    &self,
    index_name: &str,
    query: &str,
    top_k: usize
) -> Result<Vec<(RowId, f32)>>
```

**Returns**: List of `(row_id, bm25_score)` tuples

**Example**:
```rust
let results = db.text_search_ranked("articles_content", "rust database", 10)?;

for (row_id, score) in results {
    println!("RowID: {}, BM25 Score: {}", row_id, score);
}
```

### spatial_search

Spatial bounding box query.

```rust
pub fn spatial_search(
    &self,
    index_name: &str,
    bbox: &BoundingBox
) -> Result<Vec<RowId>>
```

**Example**:
```rust
let bbox = BoundingBox {
    min_x: 116.0,
    min_y: 39.0,
    max_x: 117.0,
    max_y: 40.0,
};
let results = db.spatial_search("locations_coords", &bbox)?;
```

### query_timestamp_range

Time-series range query.

```rust
pub fn query_timestamp_range(
    &self,
    start: i64,
    end: i64
) -> Result<Vec<RowId>>
```

**Example**:
```rust
let start_ts = 1609459200;  // 2021-01-01 00:00:00
let end_ts = 1640995200;    // 2022-01-01 00:00:00
let row_ids = db.query_timestamp_range(start_ts, end_ts)?;
```

## Statistics

### vector_index_stats

Get vector index statistics.

```rust
pub fn vector_index_stats(
    &self,
    index_name: &str
) -> Result<VectorIndexStats>
```

**Returns**:
```rust
pub struct VectorIndexStats {
    pub total_vectors: usize,
    pub dimension: usize,
    pub avg_neighbors: f32,
    pub memory_usage_mb: f64,
}
```

**Example**:
```rust
let stats = db.vector_index_stats("docs_embedding")?;
println!("Total vectors: {}", stats.total_vectors);
println!("Average neighbors: {}", stats.avg_neighbors);
```

### spatial_index_stats

Get spatial index statistics.

```rust
pub fn spatial_index_stats(
    &self,
    index_name: &str
) -> Result<SpatialIndexStats>
```

**Returns**:
```rust
pub struct SpatialIndexStats {
    pub total_entries: usize,
    pub tree_height: usize,
    pub memory_usage_mb: f64,
}
```

### transaction_stats

Get transaction statistics.

```rust
pub fn transaction_stats(&self) -> TransactionStats
```

**Returns**:
```rust
pub struct TransactionStats {
    pub active_transactions: usize,
    pub total_committed: u64,
    pub total_aborted: u64,
}
```

**Example**:
```rust
let stats = db.transaction_stats();
println!("Active transactions: {}", stats.active_transactions);
println!("Committed transactions: {}", stats.total_committed);
```

## CRUD Operations

### insert_row_map

Insert a row (using HashMap).

```rust
pub fn insert_row_map(
    &self,
    table_name: &str,
    sql_row: SqlRow
) -> Result<RowId>
```

**Example**:
```rust
let mut row = HashMap::new();
row.insert("id".to_string(), Value::Integer(1));
row.insert("name".to_string(), Value::Text("Alice".into()));

let row_id = db.insert_row_map("users", row)?;
```

### get_row_map

Get a row (returns HashMap format).

```rust
pub fn get_row_map(
    &self,
    table_name: &str,
    row_id: RowId
) -> Result<Option<SqlRow>>
```

**Example**:
```rust
if let Some(row) = db.get_row_map("users", 1)? {
    println!("Name: {:?}", row.get("name"));
}
```

### update_row_map

Update a row (using HashMap).

```rust
pub fn update_row_map(
    &self,
    table_name: &str,
    row_id: RowId,
    new_sql_row: SqlRow
) -> Result<()>
```

**Example**:
```rust
let mut new_row = HashMap::new();
new_row.insert("id".to_string(), Value::Integer(1));
new_row.insert("name".to_string(), Value::Text("Bob".into()));

db.update_row_map("users", 1, new_row)?;
```

## Data Types

### Value

```rust
pub enum Value {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Bool(bool),
    Vector(Vec<f32>),
    Timestamp(i64),
}
```

### SqlRow

```rust
pub type SqlRow = HashMap<String, Value>;
```

### BoundingBox

```rust
pub struct BoundingBox {
    pub min_x: f32,
    pub min_y: f32,
    pub max_x: f32,
    pub max_y: f32,
}
```

### QueryResult

```rust
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<SqlRow>,
    pub affected_rows: usize,
}
```

## DBConfig

```rust
pub struct DBConfig {
    pub memtable_size_mb: usize,
    pub row_cache_size: usize,
    pub lsm_max_levels: usize,
    pub compression: bool,
    pub enable_wal: bool,
    pub auto_flush_interval: u64,
    pub durability_level: DurabilityLevel,
    pub bloom_filter_bits: usize,
}
```

**Default values**:
```rust
impl Default for DBConfig {
    fn default() -> Self {
        Self {
            memtable_size_mb: 8,
            row_cache_size: 10000,
            lsm_max_levels: 4,
            compression: true,
            enable_wal: true,
            auto_flush_interval: 60,
            durability_level: DurabilityLevel::Full,
            bloom_filter_bits: 10,
        }
    }
}
```

## DurabilityLevel

```rust
pub enum DurabilityLevel {
    None,    // No durability guarantee (fastest)
    Memory,  // Memory-only flush
    Full,    // Full durability (safest)
}
```

---

**Previous**: [Data Types](./13-data-types.md)
**Next**: [Best Practices](./15-best-practices.md)
