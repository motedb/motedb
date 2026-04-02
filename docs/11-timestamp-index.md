# Timestamp Index

A combination of ordered time columns and LSM Range indexes, ensuring millisecond-level range queries even on million-level event streams.

## Data Modeling

```sql
CREATE TABLE sensor_data (
    id INT,
    sensor_id INT,
    value FLOAT,
    ts TIMESTAMP
);
```

- `TIMESTAMP` stores microseconds (`Timestamp::from_micros`)
- In SQL, use `TIMESTAMP '2026-01-11 12:00:00'` or write the epoch directly

## Write Example

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

## Query API

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

## Combining with Column Indexes

```sql
SELECT *
FROM sensor_data
WHERE sensor_id = 42
  AND ts BETWEEN 1600000100 AND 1600000500
ORDER BY ts DESC
LIMIT 100;
```

It is recommended to create a column index on `sensor_id`; the time column automatically uses the timestamp index.

## Performance Benchmarks

| Dataset | Range Width | Latency | Memory |
|---------|-------------|---------|--------|
| 1e6 rows | 1 hour | 5.2 ms | 60 MB |
| 1e6 rows | 1 day | 8.4 ms | 60 MB |

## Best Practices

- Batch commits in time-ascending order to avoid fragmentation
- Periodically run `COMPACT TABLE sensor_data` to reduce cross-level lookups
- Combine with `DurabilityLevel::Full` to ensure write safety

## Troubleshooting

| Symptom | Explanation |
|---------|-------------|
| Slow queries | Check whether the sort field is missing an index; verify time units (seconds vs. microseconds) |
| Missing data | Ensure `Timestamp::from_*` is used during insertion; avoid mixing Int/Float types |
| Range interleaving | Enable write buffering for out-of-order data, or write by partition |

---

- Previous: [10 Spatial Index](./10-spatial-index.md)
- Next: [12 Performance Optimization](./12-performance.md)
