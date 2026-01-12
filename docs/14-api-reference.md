# API 参考

MoteDB 完整 API 文档。

## Database 结构体

```rust
pub struct Database {
    inner: Arc<MoteDB>,
}
```

## 生命周期管理

### create

创建新数据库。

```rust
pub fn create<P: AsRef<Path>>(path: P) -> Result<Self>
```

**示例**:
```rust
let db = Database::create("myapp.mote")?;
```

### create_with_config

使用自定义配置创建数据库。

```rust
pub fn create_with_config<P: AsRef<Path>>(
    path: P, 
    config: DBConfig
) -> Result<Self>
```

**示例**:
```rust
let config = DBConfig {
    memtable_size_mb: 16,
    row_cache_size: 10000,
    ..Default::default()
};
let db = Database::create_with_config("myapp.mote", config)?;
```

### open

打开已存在的数据库。

```rust
pub fn open<P: AsRef<Path>>(path: P) -> Result<Self>
```

**示例**:
```rust
let db = Database::open("myapp.mote")?;
```

### flush

刷新所有数据到磁盘。

```rust
pub fn flush(&self) -> Result<()>
```

**示例**:
```rust
db.execute("INSERT INTO users VALUES (1, 'Alice', 25)")?;
db.flush()?;  // 确保数据持久化
```

### close

关闭数据库（显式调用，通常由 Drop 自动处理）。

```rust
pub fn close(&self) -> Result<()>
```

## SQL 操作

### query

执行 SQL 查询并返回结果。

```rust
pub fn query(&self, sql: &str) -> Result<QueryResult>
```

**示例**:
```rust
let results = db.query("SELECT * FROM users WHERE age > 18")?;
```

### execute

执行 SQL 语句（INSERT/UPDATE/DELETE/CREATE/DROP）。

```rust
pub fn execute(&self, sql: &str) -> Result<QueryResult>
```

**示例**:
```rust
db.execute("CREATE TABLE users (id INT, name TEXT)")?;
db.execute("INSERT INTO users VALUES (1, 'Alice')")?;
db.execute("UPDATE users SET name = 'Bob' WHERE id = 1")?;
```

## 事务管理

### begin_transaction

开始新事务。

```rust
pub fn begin_transaction(&self) -> Result<u64>
```

**返回**: 事务 ID

**示例**:
```rust
let tx_id = db.begin_transaction()?;
db.execute("INSERT INTO users VALUES (1, 'Alice', 25)")?;
db.commit_transaction(tx_id)?;
```

### commit_transaction

提交事务。

```rust
pub fn commit_transaction(&self, tx_id: u64) -> Result<()>
```

**示例**:
```rust
let tx_id = db.begin_transaction()?;
db.execute("INSERT INTO users VALUES (1, 'Alice', 25)")?;
db.commit_transaction(tx_id)?;
```

### rollback_transaction

回滚事务。

```rust
pub fn rollback_transaction(&self, tx_id: u64) -> Result<()>
```

**示例**:
```rust
let tx_id = db.begin_transaction()?;
db.execute("INSERT INTO users VALUES (1, 'Alice', 25)")?;
db.rollback_transaction(tx_id)?;  // 撤销所有修改
```

### savepoint

创建保存点（事务内的检查点）。

```rust
pub fn savepoint(&self, tx_id: u64, name: &str) -> Result<()>
```

**示例**:
```rust
let tx_id = db.begin_transaction()?;
db.execute("INSERT INTO users VALUES (1, 'Alice', 25)")?;
db.savepoint(tx_id, "sp1")?;

db.execute("INSERT INTO users VALUES (2, 'Bob', 30)")?;
db.rollback_to_savepoint(tx_id, "sp1")?;  // 只回滚 Bob

db.commit_transaction(tx_id)?;  // Alice 保留
```

### rollback_to_savepoint

回滚到保存点。

```rust
pub fn rollback_to_savepoint(&self, tx_id: u64, name: &str) -> Result<()>
```

### release_savepoint

释放保存点。

```rust
pub fn release_savepoint(&self, tx_id: u64, name: &str) -> Result<()>
```

## 批量操作

### batch_insert_map

批量插入行（使用 HashMap，比逐行插入快 10-20 倍）。

```rust
pub fn batch_insert_map(
    &self, 
    table_name: &str, 
    sql_rows: Vec<SqlRow>
) -> Result<Vec<RowId>>
```

**参数**:
- `table_name`: 表名
- `sql_rows`: `Vec<HashMap<String, Value>>`

**返回**: 插入的行 ID 列表

**示例**:
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

批量插入带向量的数据（自动构建向量索引）。

```rust
pub fn batch_insert_with_vectors_map(
    &self, 
    table_name: &str,
    sql_rows: Vec<SqlRow>,
    vector_columns: &[&str]
) -> Result<Vec<RowId>>
```

**示例**:
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

## 索引管理

### create_column_index

创建列索引（用于快速等值/范围查询）。

```rust
pub fn create_column_index(
    &self, 
    table_name: &str, 
    column_name: &str
) -> Result<()>
```

**示例**:
```rust
db.create_column_index("users", "email")?;

// 查询会自动使用索引（性能提升 40 倍）
let results = db.query("SELECT * FROM users WHERE email = 'alice@example.com'")?;
```

### create_vector_index

创建向量索引（用于 KNN 相似度搜索）。

```rust
pub fn create_vector_index(
    &self, 
    index_name: &str, 
    dimension: usize
) -> Result<()>
```

**示例**:
```rust
db.create_vector_index("docs_embedding", 128)?;

// SQL 向量搜索
let results = db.query("
    SELECT * FROM docs 
    ORDER BY embedding <-> [0.1, 0.2, ..., 0.5] 
    LIMIT 10
")?;
```

### create_text_index

创建全文索引（用于 BM25 文本搜索）。

```rust
pub fn create_text_index(&self, index_name: &str) -> Result<()>
```

**示例**:
```rust
db.create_text_index("articles_content")?;

let results = db.query("
    SELECT * FROM articles 
    WHERE MATCH(content, 'rust database')
")?;
```

### create_spatial_index

创建空间索引（用于地理位置查询）。

```rust
pub fn create_spatial_index(
    &self, 
    index_name: &str, 
    bounds: BoundingBox
) -> Result<()>
```

**示例**:
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

删除索引。

```rust
pub fn drop_index(
    &self, 
    table_name: &str, 
    index_name: &str
) -> Result<()>
```

## 查询 API

### query_by_column

按列值查询（使用列索引，等值查询）。

```rust
pub fn query_by_column(
    &self, 
    table_name: &str, 
    column_name: &str, 
    value: &Value
) -> Result<Vec<RowId>>
```

**示例**:
```rust
let row_ids = db.query_by_column(
    "users", 
    "email", 
    &Value::Text("alice@example.com".into())
)?;
```

### query_by_column_range

按列范围查询（使用列索引）。

```rust
pub fn query_by_column_range(
    &self, 
    table_name: &str, 
    column_name: &str,
    start: &Value, 
    end: &Value
) -> Result<Vec<RowId>>
```

**示例**:
```rust
let row_ids = db.query_by_column_range(
    "users",
    "age",
    &Value::Integer(20),
    &Value::Integer(30)
)?;
```

### vector_search

向量 KNN 搜索。

```rust
pub fn vector_search(
    &self, 
    index_name: &str, 
    query: &[f32], 
    k: usize
) -> Result<Vec<(RowId, f32)>>
```

**返回**: `(row_id, distance)` 元组列表

**示例**:
```rust
let query_vec = vec![0.1; 128];
let results = db.vector_search("docs_embedding", &query_vec, 10)?;

for (row_id, distance) in results {
    println!("RowID: {}, Distance: {}", row_id, distance);
}
```

### text_search_ranked

全文搜索（BM25 排序）。

```rust
pub fn text_search_ranked(
    &self, 
    index_name: &str, 
    query: &str, 
    top_k: usize
) -> Result<Vec<(RowId, f32)>>
```

**返回**: `(row_id, bm25_score)` 元组列表

**示例**:
```rust
let results = db.text_search_ranked("articles_content", "rust database", 10)?;

for (row_id, score) in results {
    println!("RowID: {}, BM25 Score: {}", row_id, score);
}
```

### spatial_search

空间范围查询。

```rust
pub fn spatial_search(
    &self, 
    index_name: &str, 
    bbox: &BoundingBox
) -> Result<Vec<RowId>>
```

**示例**:
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

时间序列范围查询。

```rust
pub fn query_timestamp_range(
    &self, 
    start: i64, 
    end: i64
) -> Result<Vec<RowId>>
```

**示例**:
```rust
let start_ts = 1609459200;  // 2021-01-01 00:00:00
let end_ts = 1640995200;    // 2022-01-01 00:00:00
let row_ids = db.query_timestamp_range(start_ts, end_ts)?;
```

## 统计信息

### vector_index_stats

获取向量索引统计信息。

```rust
pub fn vector_index_stats(
    &self, 
    index_name: &str
) -> Result<VectorIndexStats>
```

**返回**:
```rust
pub struct VectorIndexStats {
    pub total_vectors: usize,
    pub dimension: usize,
    pub avg_neighbors: f32,
    pub memory_usage_mb: f64,
}
```

**示例**:
```rust
let stats = db.vector_index_stats("docs_embedding")?;
println!("向量数量: {}", stats.total_vectors);
println!("平均邻居数: {}", stats.avg_neighbors);
```

### spatial_index_stats

获取空间索引统计信息。

```rust
pub fn spatial_index_stats(
    &self, 
    index_name: &str
) -> Result<SpatialIndexStats>
```

**返回**:
```rust
pub struct SpatialIndexStats {
    pub total_entries: usize,
    pub tree_height: usize,
    pub memory_usage_mb: f64,
}
```

### transaction_stats

获取事务统计信息。

```rust
pub fn transaction_stats(&self) -> TransactionStats
```

**返回**:
```rust
pub struct TransactionStats {
    pub active_transactions: usize,
    pub total_committed: u64,
    pub total_aborted: u64,
}
```

**示例**:
```rust
let stats = db.transaction_stats();
println!("活跃事务数: {}", stats.active_transactions);
println!("已提交事务数: {}", stats.total_committed);
```

## CRUD 操作

### insert_row_map

插入行（使用 HashMap）。

```rust
pub fn insert_row_map(
    &self, 
    table_name: &str, 
    sql_row: SqlRow
) -> Result<RowId>
```

**示例**:
```rust
let mut row = HashMap::new();
row.insert("id".to_string(), Value::Integer(1));
row.insert("name".to_string(), Value::Text("Alice".into()));

let row_id = db.insert_row_map("users", row)?;
```

### get_row_map

获取行（返回 HashMap 格式）。

```rust
pub fn get_row_map(
    &self, 
    table_name: &str, 
    row_id: RowId
) -> Result<Option<SqlRow>>
```

**示例**:
```rust
if let Some(row) = db.get_row_map("users", 1)? {
    println!("Name: {:?}", row.get("name"));
}
```

### update_row_map

更新行（使用 HashMap）。

```rust
pub fn update_row_map(
    &self, 
    table_name: &str, 
    row_id: RowId, 
    new_sql_row: SqlRow
) -> Result<()>
```

**示例**:
```rust
let mut new_row = HashMap::new();
new_row.insert("id".to_string(), Value::Integer(1));
new_row.insert("name".to_string(), Value::Text("Bob".into()));

db.update_row_map("users", 1, new_row)?;
```

## 数据类型

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

**默认值**:
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
    None,    // 无持久化保证（最快）
    Memory,  // 仅内存刷新
    Full,    // 完整持久化（最安全）
}
```

---

**上一篇**: [数据类型](./13-data-types.md)  
**下一篇**: [最佳实践](./15-best-practices.md)
