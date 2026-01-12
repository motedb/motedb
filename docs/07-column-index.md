# 列索引 (Column Index)

等值/范围查询的首选索引方式，基于列值构建有序结构以加速过滤与 Join。

## 适用场景

- 高频 `WHERE column = ?`、`IN (...)`、`BETWEEN ...` 查询
- 需要加速 Join 条件（主键/外键）
- 需要在几十万行数据上维持 <10ms 的响应

## 创建索引

### SQL 方式

```sql
CREATE INDEX users_email ON users(email);
```

### API 方式

```rust
use motedb::Database;

let db = Database::open("data.mote")?;
db.create_column_index("users", "email")?;
```

## 查询示例

```rust
use motedb::{Database, types::Value};

let row_ids = db.query_by_column(
    "users",
    "email",
    &Value::Text("alice@example.com".into())
)?;

let range_ids = db.query_by_column_range(
    "users",
    "age",
    &Value::Integer(20),
    &Value::Integer(30)
)?;
```

## 性能基准

| 数据量 | 未建索引 | 建索引后 |
|--------|----------|-----------|
| 10 万行等值查询 | ~120 ms | **2.8 ms** |
| 10 万行范围查询 | ~180 ms | **7.5 ms** |

> 测试环境：Apple M3 Pro / Release build / row_cache_size=10k

## 批量写入策略

1. 大规模导入：先 `batch_insert_map()` → 再 `CREATE INDEX`
2. 持续写入：先建索引，索引自动增量维护
3. `db.flush()?` 后索引元数据与数据同时落盘

## 运行时监控

```rust
use motedb::database::indexes::ColumnIndexStats;

let stats: ColumnIndexStats = db.inner().column_index_stats("users_email")?;
println!("entry_count={} cache_hit_rate={:.2}%",
    stats.entry_count,
    stats.cache_hit_rate * 100.0,
);
```

## 最佳实践

- 只为高选择性列建索引，避免内存浪费
- 对日期/时间字段配合时间序列索引（见 `11-timestamp-index.md`）
- 定期通过 `ANALYZE`（即 `db.execute("ANALYZE")`）刷新统计

## 故障排查

| 现象 | 处理办法 |
|------|----------|
| 查询仍然慢 | 检查是否命中索引（`EXPLAIN SELECT ...`） |
| 建索引占用大 | 减少列数或调低 `row_cache_size` |
| 插入阻塞 | 使用批量模式或增大 `memtable_size_mb` |

---

- 上一篇：[06 索引概览](./06-indexes-overview.md)
- 下一篇：[08 向量索引](./08-vector-index.md)
