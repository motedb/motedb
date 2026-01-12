# MoteDB 文档

面向嵌入式具身智能的高性能多模态数据库完整指南。

## 📚 文档目录

### 快速开始
- **快速开始指南**: [./01-quick-start.md](./01-quick-start.md) - 5分钟入门 MoteDB
- **安装配置**: [./02-installation.md](./02-installation.md) - 安装、配置与部署参数

### 核心功能
- **SQL 操作**: [./03-sql-operations.md](./03-sql-operations.md) - 完整 SQL 语法与查询模板
- **批量操作**: [./04-batch-operations.md](./04-batch-operations.md) - 高性能批量写入（10-20× 提升）
- **事务管理**: [./05-transactions.md](./05-transactions.md) - MVCC、WAL、保存点用法
- **API 参考**: [./14-api-reference.md](./14-api-reference.md) - 所有公开 API 与示例

### 索引系统
- **索引概览**: [./06-indexes-overview.md](./06-indexes-overview.md) - 五大索引如何协同
- **列索引**: [./07-column-index.md](./07-column-index.md) - 等值/范围查询
- **向量索引**: [./08-vector-index.md](./08-vector-index.md) - FreshDiskANN & rerank
- **全文索引**: [./09-text-index.md](./09-text-index.md) - BM25 与分词插件
- **空间索引**: [./10-spatial-index.md](./10-spatial-index.md) - R-Tree 与地理查询
- **时间序列索引**: [./11-timestamp-index.md](./11-timestamp-index.md) - 范围扫描与压缩

### 高级主题
- **性能优化**: [./12-performance.md](./12-performance.md) - 配置、调优与监控
- **数据类型**: [./13-data-types.md](./13-data-types.md) - `Value` 枚举与 Schema 设计

### 最佳实践
- **生产经验**: [./15-best-practices.md](./15-best-practices.md) - 架构、写入、索引策略
- **常见问题**: [./16-faq.md](./16-faq.md) - 调试、部署与故障排查

## 🚀 核心特性

### 1. **SQL 引擎**
完整 SQL 支持，包含子查询、聚合、JOIN、索引管理。

```rust
let db = Database::open("data.mote")?;
db.execute("CREATE TABLE users (id INT, name TEXT, email TEXT)")?;
db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')")?;
let results = db.query("SELECT * FROM users WHERE id = 1")?;
```

### 2. **多模态索引**
五大索引类型支持不同数据场景：

| 索引类型 | 用途 | 性能提升 |
|---------|------|---------|
| 列索引 (COLUMN) | 等值/范围查询 | 40x |
| 向量索引 (VECTOR) | KNN 相似度搜索 | 100x |
| 全文索引 (TEXT) | BM25 文本搜索 | 50x |
| 空间索引 (SPATIAL) | 地理位置查询 | 30x |
| 时间序列 (TIMESTAMP) | 时间范围查询 | 20x |

### 3. **高性能批量操作**
批量插入比逐行插入快 10-20 倍：

```rust
// 批量插入 10000 条数据
let mut rows = Vec::new();
for i in 0..10000 {
    let mut row = HashMap::new();
    row.insert("id".to_string(), Value::Integer(i));
    row.insert("name".to_string(), Value::Text(format!("User{}", i)));
    rows.push(row);
}

let row_ids = db.batch_insert_map("users", rows)?;
// 吞吐量: 737,112 rows/sec
```

### 4. **MVCC 事务**
完整的事务支持，包含 Savepoint：

```rust
let tx_id = db.begin_transaction()?;

db.execute("INSERT INTO users VALUES (1, 'Alice', 25)")?;
db.savepoint(tx_id, "sp1")?;

db.execute("INSERT INTO users VALUES (2, 'Bob', 30)")?;
db.rollback_to_savepoint(tx_id, "sp1")?; // 只回滚 Bob

db.commit_transaction(tx_id)?;
```

## 🎯 适用场景

- **嵌入式 AI 应用**: 机器人、边缘计算设备
- **向量数据库**: RAG、语义搜索、推荐系统
- **时空数据**: 地理位置、传感器数据
- **全文搜索**: 文档检索、日志分析
- **实时分析**: 时间序列数据

## 📊 性能指标

- **批量插入**: 737,112 rows/sec (10000条)
- **向量搜索**: 延迟 < 5ms (召回率 95%)
- **列索引查询**: 提升 40 倍
- **内存占用**: 核心数据结构 < 10MB
- **事务吞吐**: 10000 TPS

## 💡 推荐使用方式

1. **主要使用 SQL API** - 简洁、强大、易用
2. **批量操作优先** - 使用 `batch_insert_map()` 而非逐行插入
3. **合理使用索引** - 根据查询模式选择合适的索引类型
4. **启用事务** - 保证数据一致性

## 🔗 快速链接

- [GitHub 仓库](https://github.com/yourusername/motedb)
- [API 文档](https://docs.rs/motedb)
- [示例代码](../examples/)

## 📝 版本信息

当前文档对应版本: v0.1.0  
最后更新: 2026-01-11

---

**下一步**: 阅读 [快速开始指南](./01-quick-start.md) 开始使用 MoteDB
