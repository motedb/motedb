# 批量操作

高性能批量插入，比逐行插入快 10-20 倍。

## 核心 API

```rust
// 批量插入（使用 HashMap，推荐）
db.batch_insert_map(
    table_name: &str, 
    rows: Vec<HashMap<String, Value>>
) -> Result<Vec<RowId>>

// 批量插入带向量数据
db.batch_insert_with_vectors_map(
    table_name: &str,
    rows: Vec<HashMap<String, Value>>,
    vector_columns: &[&str]
) -> Result<Vec<RowId>>
```

## 性能对比

| 方式 | 10000条数据 | 吞吐量 | 性能提升 |
|-----|------------|--------|---------|
| 逐行SQL INSERT | ~5000ms | 2,000 rows/sec | 基准 |
| batch_insert_map | ~14ms | 737,112 rows/sec | **368x** |

## 基础批量插入

### 示例 1: 简单数据

```rust
use motedb::{Database, types::{Value, SqlRow}};
use std::collections::HashMap;

let db = Database::open("data.mote")?;

// 创建表
db.execute("CREATE TABLE users (
    id INT,
    name TEXT,
    email TEXT,
    age INT
)")?;

// 准备数据
let mut rows = Vec::new();
for i in 0..10000 {
    let mut row = HashMap::new();
    row.insert("id".to_string(), Value::Integer(i));
    row.insert("name".to_string(), Value::Text(format!("User{}", i)));
    row.insert("email".to_string(), Value::Text(format!("user{}@example.com", i)));
    row.insert("age".to_string(), Value::Integer(20 + (i % 50)));
    rows.push(row);
}

// 批量插入（非常快！）
let row_ids = db.batch_insert_map("users", rows)?;
println!("Inserted {} rows", row_ids.len());

// 持久化
db.flush()?;
```

**性能**: 10000条 ~14ms，吞吐量 737,112 rows/sec

### 示例 2: 混合数据类型

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

## 批量插入向量数据

### 示例 3: 文档向量

```rust
// 创建表（带向量字段）
db.execute("CREATE TABLE documents (
    id INT,
    title TEXT,
    content TEXT,
    embedding VECTOR(128)
)")?;

// 创建向量索引
db.execute("CREATE VECTOR INDEX docs_embedding ON documents(embedding)")?;

// 准备向量数据
let mut rows = Vec::new();
for i in 0..1000 {
    let mut row = HashMap::new();
    row.insert("id".to_string(), Value::Integer(i));
    row.insert("title".to_string(), Value::Text(format!("Document {}", i)));
    row.insert("content".to_string(), Value::Text(format!("Content of document {}", i)));
    
    // 生成 128 维向量（实际使用中应该是真实的 embedding）
    let embedding: Vec<f32> = (0..128).map(|j| (i as f32 + j as f32) / 1000.0).collect();
    row.insert("embedding".to_string(), Value::Vector(embedding));
    
    rows.push(row);
}

// 批量插入（自动触发向量索引构建）
let row_ids = db.batch_insert_with_vectors_map("documents", rows, &["embedding"])?;

db.flush()?;
```

**性能**: 1000条×128维向量 ~13ms，吞吐量 74,761 vectors/sec

## 批量插入空间数据

### 示例 4: 地理位置

```rust
use motedb::BoundingBox;

// 创建表
db.execute("CREATE TABLE locations (
    id INT,
    name TEXT,
    coords VECTOR(2),
    category TEXT
)")?;

// 创建空间索引
let bounds = BoundingBox {
    min_x: -180.0, min_y: -90.0,
    max_x: 180.0, max_y: 90.0,
};
db.create_spatial_index("locations_coords", bounds)?;

// 批量插入位置数据
let mut rows = Vec::new();
for i in 0..5000 {
    let mut row = HashMap::new();
    row.insert("id".to_string(), Value::Integer(i));
    row.insert("name".to_string(), Value::Text(format!("Location {}", i)));
    
    // 生成随机坐标（经度，纬度）
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

## 批量插入 + 索引

### 推荐模式 1: 先插入，后创建索引

```rust
// 1. 批量插入数据
let row_ids = db.batch_insert_map("users", rows)?;

// 2. 创建列索引（自动构建）
db.execute("CREATE INDEX users_email ON users(email)")?;

// 3. 持久化
db.flush()?;
```

**优点**: 索引构建一次性完成，效率最高。

### 推荐模式 2: 先创建索引，批量插入时自动更新

```rust
// 1. 先创建索引
db.execute("CREATE INDEX users_email ON users(email)")?;

// 2. 批量插入（索引自动增量更新）
let row_ids = db.batch_insert_map("users", rows)?;

// 3. 持久化
db.flush()?;
```

**优点**: 适合持续写入场景。

## 分批插入大数据

对于超大数据集（百万级），建议分批插入：

```rust
const BATCH_SIZE: usize = 10000;
let total_rows = 1_000_000;

for batch_start in (0..total_rows).step_by(BATCH_SIZE) {
    let batch_end = (batch_start + BATCH_SIZE).min(total_rows);
    
    // 准备当前批次数据
    let mut batch_rows = Vec::new();
    for i in batch_start..batch_end {
        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::Integer(i as i64));
        row.insert("data".to_string(), Value::Text(format!("Data {}", i)));
        batch_rows.push(row);
    }
    
    // 批量插入
    db.batch_insert_map("large_table", batch_rows)?;
    
    // 每 10 批 flush 一次
    if (batch_start / BATCH_SIZE) % 10 == 0 {
        db.flush()?;
        println!("Processed {} rows", batch_end);
    }
}

// 最终 flush
db.flush()?;
```

## 批量插入 + 事务

```rust
// 开始事务
let tx_id = db.begin_transaction()?;

// 批量插入
let row_ids = db.batch_insert_map("users", rows)?;

// 检查结果
if row_ids.len() == rows.len() {
    db.commit_transaction(tx_id)?;
    println!("成功插入 {} 行", row_ids.len());
} else {
    db.rollback_transaction(tx_id)?;
    println!("插入失败，已回滚");
}
```

## 性能优化技巧

### 1. 使用合适的批次大小

```rust
// 推荐批次大小
const OPTIMAL_BATCH_SIZE: usize = 10000;

// 小批次（< 1000）：性能提升不明显
// 中批次（1000-10000）：性能最优
// 大批次（> 50000）：内存占用增加
```

### 2. 延迟索引构建

```rust
// 大数据集：先插入，后创建索引
db.batch_insert_map("users", rows)?;
db.execute("CREATE INDEX users_email ON users(email)")?;
```

### 3. 调整内存配置

```rust
use motedb::DBConfig;

let config = DBConfig {
    memtable_size_mb: 32,  // 增大内存表
    ..Default::default()
};

let db = Database::create_with_config("data.mote", config)?;
```

### 4. 关闭自动 flush（批量写入场景）

```rust
let config = DBConfig {
    auto_flush_interval: 300,  // 延长到 5 分钟
    ..Default::default()
};

// 或者手动控制 flush
db.batch_insert_map("users", batch1)?;
db.batch_insert_map("users", batch2)?;
db.batch_insert_map("users", batch3)?;
db.flush()?;  // 一次性刷新
```

## 常见错误

### 错误 1: 字段不匹配

```rust
// ❌ 错误：字段名不匹配
let mut row = HashMap::new();
row.insert("user_id".to_string(), Value::Integer(1));  // 表中字段是 'id'
row.insert("user_name".to_string(), Value::Text("Alice".into()));

// ✅ 正确：字段名必须与表结构一致
let mut row = HashMap::new();
row.insert("id".to_string(), Value::Integer(1));
row.insert("name".to_string(), Value::Text("Alice".into()));
```

### 错误 2: 类型不匹配

```rust
// ❌ 错误：类型不匹配
row.insert("age".to_string(), Value::Text("25".into()));  // 应该是 Integer

// ✅ 正确
row.insert("age".to_string(), Value::Integer(25));
```

### 错误 3: 忘记 flush

```rust
// ❌ 数据可能丢失
db.batch_insert_map("users", rows)?;
// 程序退出，数据未持久化

// ✅ 正确
db.batch_insert_map("users", rows)?;
db.flush()?;  // 确保持久化
```

## 完整示例：多模态数据批量导入

```rust
use motedb::{Database, types::{Value, SqlRow}, BoundingBox};
use std::collections::HashMap;

fn main() -> motedb::Result<()> {
    let db = Database::open("multimodal.mote")?;
    
    // 创建表
    db.execute("CREATE TABLE robot_observations (
        id INT,
        timestamp INT,
        location VECTOR(2),
        image_embedding VECTOR(512),
        description TEXT,
        confidence FLOAT
    )")?;
    
    // 创建索引
    db.execute("CREATE VECTOR INDEX obs_embedding ON robot_observations(image_embedding)")?;
    
    let bounds = BoundingBox { min_x: 0.0, min_y: 0.0, max_x: 100.0, max_y: 100.0 };
    db.create_spatial_index("obs_location", bounds)?;
    
    // 批量插入
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

## 下一步

- [事务管理](./05-transactions.md) - 事务 + 批量操作
- [索引系统](./06-indexes-overview.md) - 优化批量插入后的查询
- [性能优化](./12-performance.md) - 深入性能调优

---

**上一篇**: [SQL 操作](./03-sql-operations.md)  
**下一篇**: [事务管理](./05-transactions.md)
