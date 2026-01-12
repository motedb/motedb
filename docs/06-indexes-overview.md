# 索引系统概览

MoteDB 支持五大索引类型，针对不同查询场景进行优化。

## 索引类型总览

| 索引类型 | 用途 | 性能提升 | 适用场景 |
|---------|------|---------|---------|
| [列索引](#列索引-column) | 等值/范围查询 | 40x | WHERE 条件、JOIN |
| [向量索引](#向量索引-vector) | KNN 相似度搜索 | 100x | RAG、推荐系统 |
| [全文索引](#全文索引-text) | BM25 文本搜索 | 50x | 文档检索、日志分析 |
| [空间索引](#空间索引-spatial) | 地理位置查询 | 30x | LBS、轨迹分析 |
| [时间序列](#时间序列-timestamp) | 时间范围查询 | 20x | 日志、传感器数据 |

## 列索引 (COLUMN)

### 概述
基于 LSM-Tree 的列索引，支持快速等值查询和范围查询。

### 创建索引

```rust
// 方式 1: SQL
db.execute("CREATE INDEX users_email ON users(email)")?;

// 方式 2: API
db.create_column_index("users", "email")?;
```

### 使用场景

```rust
// WHERE 等值查询（性能提升 40 倍）
db.query("SELECT * FROM users WHERE email = 'alice@example.com'")?;

// WHERE 范围查询
db.query("SELECT * FROM users WHERE age >= 20 AND age <= 30")?;

// JOIN 查询优化
db.query("
    SELECT u.name, o.order_id
    FROM users u
    JOIN orders o ON u.id = o.user_id
    WHERE u.email = 'alice@example.com'
")?;
```

### 性能数据
- **等值查询**: 40x 提升（100ms → 2.5ms）
- **范围查询**: 25x 提升
- **内存占用**: 约 500KB/10000 行

**详细文档**: [列索引详解](./07-column-index.md)

## 向量索引 (VECTOR)

### 概述
基于 DiskANN 的向量索引，支持高性能 KNN 相似度搜索。

### 创建索引

```rust
// 方式 1: SQL
db.execute("CREATE VECTOR INDEX docs_embedding ON documents(embedding)")?;

// 方式 2: API
db.create_vector_index("docs_embedding", 128)?;
```

### 使用场景

```rust
// KNN 搜索（找最相似的 10 个向量）
db.query("
    SELECT * FROM documents
    ORDER BY embedding <-> [0.1, 0.2, ..., 0.5]
    LIMIT 10
")?;

// 支持三种距离度量
// <->  L2 距离（欧氏距离）
// <#>  内积（Inner Product）
// <=>  余弦距离（Cosine Distance）
```

### 性能数据
- **查询延迟**: < 5ms（100K 向量）
- **召回率**: 95%+（@k=10）
- **吞吐量**: 74,761 vectors/sec（批量插入）

**详细文档**: [向量索引详解](./08-vector-index.md)

## 全文索引 (TEXT)

### 概述
基于 BM25 算法的全文索引，支持中英文分词和关键词搜索。

### 创建索引

```rust
// 方式 1: SQL
db.execute("CREATE TEXT INDEX articles_content ON articles(content)")?;

// 方式 2: API
db.create_text_index("articles_content")?;
```

### 使用场景

```rust
// 全文搜索
db.query("
    SELECT * FROM articles
    WHERE MATCH(content, 'rust database')
")?;

// 带 BM25 分数排序
db.query("
    SELECT *, BM25_SCORE(content, 'rust database') as score
    FROM articles
    WHERE MATCH(content, 'rust database')
    ORDER BY score DESC
    LIMIT 20
")?;
```

### 性能数据
- **查询延迟**: < 10ms（10000 文档）
- **索引大小**: 约 6.78MB（10000 文档）
- **支持中英文分词**: jieba 分词器

**详细文档**: [全文索引详解](./09-text-index.md)

## 空间索引 (SPATIAL)

### 概述
基于 R-Tree 的空间索引，支持二维空间范围查询。

### 创建索引

```rust
use motedb::BoundingBox;

// 定义空间范围
let bounds = BoundingBox {
    min_x: -180.0,
    min_y: -90.0,
    max_x: 180.0,
    max_y: 90.0,
};

// 方式 1: SQL（需先通过 API 指定 bounds）
db.create_spatial_index("locations_coords", bounds)?;
db.execute("CREATE SPATIAL INDEX locations_coords ON locations(coords)")?;

// 方式 2: API
db.create_spatial_index("locations_coords", bounds)?;
```

### 使用场景

```rust
// 矩形范围查询
db.query("
    SELECT * FROM locations
    WHERE ST_WITHIN(coords, 116.0, 39.0, 117.0, 40.0)
")?;

// 距离查询
db.query("
    SELECT *, ST_DISTANCE(coords, 116.4, 39.9) as distance
    FROM locations
    WHERE ST_DISTANCE(coords, 116.4, 39.9) < 10.0
    ORDER BY distance ASC
")?;
```

### 性能数据
- **查询延迟**: < 5ms（50000 点）
- **批量插入**: 50000 点 ~85ms
- **内存占用**: 约 2MB（50000 点）

**详细文档**: [空间索引详解](./10-spatial-index.md)

## 时间序列索引 (TIMESTAMP)

### 概述
专为时间序列数据优化的索引，支持高效时间范围查询。

### 创建索引

```rust
// 在表中包含 TIMESTAMP 字段即可自动支持
db.execute("CREATE TABLE sensor_data (
    id INT,
    sensor_id INT,
    value FLOAT,
    timestamp TIMESTAMP
)")?;
```

### 使用场景

```rust
// 时间范围查询
db.query("
    SELECT * FROM sensor_data
    WHERE timestamp >= 1609459200 
      AND timestamp <= 1640995200
")?;

// 结合聚合
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

### 性能数据
- **查询延迟**: < 8ms（100000 数据点）
- **范围查询**: 20x 提升

**详细文档**: [时间序列索引详解](./11-timestamp-index.md)

## 索引选择指南

### 场景 1: 用户管理系统

```rust
db.execute("CREATE TABLE users (
    id INT,
    email TEXT,
    age INT,
    created_at TIMESTAMP
)")?;

// 创建列索引（email 频繁用于 WHERE）
db.execute("CREATE INDEX users_email ON users(email)")?;

// 时间戳自动支持时间范围查询
```

### 场景 2: 文档检索系统（RAG）

```rust
db.execute("CREATE TABLE documents (
    id INT,
    title TEXT,
    content TEXT,
    embedding VECTOR(768),
    created_at TIMESTAMP
)")?;

// 向量索引（语义搜索）
db.execute("CREATE VECTOR INDEX docs_embedding ON documents(embedding)")?;

// 全文索引（关键词搜索）
db.execute("CREATE TEXT INDEX docs_content ON documents(content)")?;
```

### 场景 3: LBS 应用

```rust
db.execute("CREATE TABLE pois (
    id INT,
    name TEXT,
    category TEXT,
    coords VECTOR(2)
)")?;

// 空间索引（地理位置查询）
let bounds = BoundingBox { 
    min_x: -180.0, min_y: -90.0, 
    max_x: 180.0, max_y: 90.0 
};
db.create_spatial_index("pois_coords", bounds)?;

// 列索引（类别筛选）
db.execute("CREATE INDEX pois_category ON pois(category)")?;
```

### 场景 4: IoT 传感器数据

```rust
db.execute("CREATE TABLE sensor_readings (
    id INT,
    sensor_id INT,
    value FLOAT,
    location VECTOR(2),
    timestamp TIMESTAMP
)")?;

// 列索引（按传感器 ID 查询）
db.execute("CREATE INDEX readings_sensor ON sensor_readings(sensor_id)")?;

// 空间索引（按位置查询）
db.create_spatial_index("readings_location", bounds)?;

// 时间戳（时间范围查询）自动支持
```

## 索引管理

### 查看索引

```rust
let result = db.query("SHOW INDEXES FROM users")?;
```

### 删除索引

```rust
db.execute("DROP INDEX users_email ON users")?;
```

### 索引统计

```rust
// 向量索引统计
let stats = db.vector_index_stats("docs_embedding")?;
println!("向量数量: {}", stats.total_vectors);

// 空间索引统计
let stats = db.spatial_index_stats("locations_coords")?;
println!("空间点数量: {}", stats.total_entries);
```

## 性能对比

### 无索引 vs 有索引

```rust
// 测试数据：10000 条记录

// ❌ 无索引查询：100ms（全表扫描）
db.query("SELECT * FROM users WHERE email = 'alice@example.com'")?;

// ✅ 有列索引：2.5ms（性能提升 40 倍）
db.execute("CREATE INDEX users_email ON users(email)")?;
db.query("SELECT * FROM users WHERE email = 'alice@example.com'")?;
```

### 批量插入性能

| 数据类型 | 数据量 | 插入时间 | 吞吐量 |
|---------|-------|---------|--------|
| 普通数据 | 10000 | 14ms | 737,112 rows/sec |
| 向量数据(128维) | 1000 | 13ms | 74,761 vectors/sec |
| 空间数据 | 50000 | 85ms | 588,235 points/sec |

## 最佳实践

1. **先插入数据，后创建索引**（大数据集）
2. **合理使用索引**：不是越多越好，索引会占用内存
3. **定期查看统计信息**：监控索引性能
4. **批量操作优先**：使用 `batch_insert_map()` 提升性能

## 下一步

- [列索引详解](./07-column-index.md)
- [向量索引详解](./08-vector-index.md)
- [全文索引详解](./09-text-index.md)
- [空间索引详解](./10-spatial-index.md)
- [时间序列索引详解](./11-timestamp-index.md)

---

**上一篇**: [事务管理](./05-transactions.md)  
**下一篇**: [列索引](./07-column-index.md)
