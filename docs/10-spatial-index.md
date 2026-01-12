# 空间索引 (Spatial Index)

使用混合 R-Tree（`SpatialHybridIndex`）为二维坐标提供毫秒级范围查询能力，适用于 LBS、机器人定位、物联网轨迹等场景。

## 数据建模

```sql
CREATE TABLE locations (
    id INT,
    name TEXT,
    coords VECTOR(2), -- [lon, lat]
    category TEXT
);
```

> 向量长度=2，分别表示 `x=longitude`、`y=latitude`

## 创建索引

```rust
use motedb::{Database, types::BoundingBox};

let db = Database::open("geo.mote")?;
let bounds = BoundingBox::new(-180.0, -90.0, 180.0, 90.0);
db.create_spatial_index("locations_coords", bounds)?;
```

索引名推荐：`<table>_<column>`，方便与 `db.spatial_search()` 对应。

## 写入示例

```rust
use motedb::types::{Value, SqlRow};
use std::collections::HashMap;

let mut rows = Vec::new();
for i in 0..5000 {
    let mut row = HashMap::new();
    row.insert("id".into(), Value::Integer(i));
    row.insert("name".into(), Value::Text(format!("POI {}", i)));
    row.insert("coords".into(), Value::Vector(vec![116.0 + i as f32 * 0.01, 39.0]));
    row.insert("category".into(), Value::Text("restaurant".into()));
    rows.push(row);
}

db.batch_insert_map("locations", rows)?;
db.flush()?;
```

## 查询 API

### SQL

```sql
SELECT id, name
FROM locations
WHERE ST_WITHIN(coords, 116.0, 39.0, 117.0, 40.0);
```

### Rust API

```rust
use motedb::types::BoundingBox;

let bbox = BoundingBox::new(116.0, 39.0, 117.0, 40.0);
let ids = db.spatial_search("locations_coords", &bbox)?;
```

## 性能

| 数据量 | P95 查询延迟 | 内存 | 构建时间 |
|--------|--------------|------|----------|
| 50k 点 | 3.4 ms | 18 MB | 85 ms |
| 500k 点 | 6.8 ms | 146 MB | 1.1 s |

## 维护

- 定期 `VACUUM SPATIAL INDEX locations_coords` 回收碎片
- 使用 `db.spatial_index_stats()` 观察树高、节点数、缓存命中率
- 跨区域数据可拆分为多个索引（按国家/城市划分）

## 常见问题

| 问题 | 解决方案 |
|------|----------|
| 查询结果不完整 | 检查 `BoundingBox` 是否覆盖全部区域；确认坐标顺序 |
| 精度不足 | 将 `coords` 类型改为 `Value::Spatial(Geometry::Point)`，保留 double 精度 |
| 写入速度慢 | 批量插入后再建索引；适当增大 `memtable_size_mb` |

---

- 上一篇：[09 全文索引](./09-text-index.md)
- 下一篇：[11 时间序列索引](./11-timestamp-index.md)
