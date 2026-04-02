# Spatial Index

A hybrid R-Tree (`SpatialHybridIndex`) providing millisecond-level range queries for two-dimensional coordinates, suitable for LBS, robot localization, IoT trajectory tracking, and similar scenarios.

## Data Modeling

```sql
CREATE TABLE locations (
    id INT,
    name TEXT,
    coords VECTOR(2), -- [lon, lat]
    category TEXT
);
```

> Vector length = 2, representing `x=longitude` and `y=latitude` respectively.

## Creating an Index

```rust
use motedb::{Database, types::BoundingBox};

let db = Database::open("geo.mote")?;
let bounds = BoundingBox::new(-180.0, -90.0, 180.0, 90.0);
db.create_spatial_index("locations_coords", bounds)?;
```

Recommended index naming: `<table>_<column>`, for easy correspondence with `db.spatial_search()`.

## Write Example

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

## Query API

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

## Performance

| Dataset | P95 Query Latency | Memory | Build Time |
|---------|-------------------|--------|------------|
| 50k points | 3.4 ms | 18 MB | 85 ms |
| 500k points | 6.8 ms | 146 MB | 1.1 s |

## Maintenance

- Periodically run `VACUUM SPATIAL INDEX locations_coords` to reclaim fragmented space
- Use `db.spatial_index_stats()` to observe tree height, node count, and cache hit rate
- Cross-region data can be split into multiple indexes (partitioned by country/city)

## Common Issues

| Issue | Solution |
|-------|----------|
| Incomplete query results | Check that `BoundingBox` covers the full area; verify coordinate ordering |
| Insufficient precision | Change the `coords` type to `Value::Spatial(Geometry::Point)` to retain double precision |
| Slow write speed | Create the index after batch insertion; increase `memtable_size_mb` appropriately |

---

- Previous: [09 Text Index](./09-text-index.md)
- Next: [11 Timestamp Index](./11-timestamp-index.md)
