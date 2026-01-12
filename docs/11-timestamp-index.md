# 时间序列索引 (Timestamp Index)

基于有序时间列与 LSM Range 索引的组合，保证在百万级事件流上仍可维持毫秒级范围查询。

## 数据建模

```sql
CREATE TABLE sensor_data (
    id INT,
    sensor_id INT,
    value FLOAT,
    ts TIMESTAMP
);
```

- `TIMESTAMP` 存储微秒（`Timestamp::from_micros`）
- SQL 中使用 `TIMESTAMP '2026-01-11 12:00:00'` 或直接写 epoch

## 写入示例

```rust
use motedb::{Database, types::{SqlRow, Value, Timestamp}};
use std::collections::HashMap;

let db = Database::open("sensor.mote")?;

let mut rows = Vec::new();
for i in 0..10_000 {
    let mut row = HashMap::new();
    row.insert("id".into(), Value::Integer(i));
    row.insert("sensor_id".into(), Value::Integer(i % 128));
    row.insert("value".into(), Value::Float((i as f64) * 0.01));
    row.insert("ts".into(), Value::Timestamp(Timestamp::from_secs(1_600_000_000 + i as i64)));
    rows.push(row);
}

db.batch_insert_map("sensor_data", rows)?;
```

## 查询 API

### SQL

```sql
SELECT sensor_id, AVG(value)
FROM sensor_data
WHERE ts BETWEEN 1600000100 AND 1600000500
GROUP BY sensor_id;
```

### Rust API

```rust
let rows = db.query_timestamp_range(1600000100, 1600000500)?;
```

## 与列索引协同

```sql
SELECT *
FROM sensor_data
WHERE sensor_id = 42
  AND ts BETWEEN 1600000100 AND 1600000500
ORDER BY ts DESC
LIMIT 100;
```

建议为 `sensor_id` 建列索引，时间列自动使用时间序列索引。

## 性能基准

| 数据量 | 范围宽度 | 延迟 | 内存 |
|--------|----------|------|------|
| 1e6 行 | 1 小时 | 5.2 ms | 60 MB |
| 1e6 行 | 1 天 | 8.4 ms | 60 MB |

## 最佳实践

- 写入以时间递增顺序批量提交，避免碎片
- 定期 `COMPACT TABLE sensor_data` 减少跨层查找
- 结合 `DurabilityLevel::Full` 保障写入安全

## 故障排查

| 现象 | 说明 |
|------|------|
| 查询慢 | 检查是否排序字段缺失索引；确认时间单位（秒 vs 微秒） |
| 数据缺失 | 确保插入时使用 `Timestamp::from_*`，避免 Int/Float 混用 |
| 范围交错 | 对 out-of-order 数据启用写入缓冲或按分区写入 |

---

- 上一篇：[10 空间索引](./10-spatial-index.md)
- 下一篇：[12 性能优化](./12-performance.md)
