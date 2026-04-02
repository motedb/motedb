# Column Index

The preferred index type for equality and range queries. Builds an ordered structure on column values to accelerate filtering and joins.

## When to Use

- High-frequency `WHERE column = ?`, `IN (...)`, `BETWEEN ...` queries
- Need to accelerate JOIN conditions (primary key / foreign key)
- Need to maintain <10ms response times across hundreds of thousands of rows

## Creating an Index

### Using SQL

```sql
CREATE INDEX users_email ON users(email);
```

### Using the API

```rust
use motedb::Database;

let db = Database::open("data.mote")?;
db.create_column_index("users", "email")?;
```

## Query Examples

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

## Performance Benchmarks

| Data Size | Without Index | With Index |
|--------|----------|-----------|
| 100K rows equality query | ~120 ms | **2.8 ms** |
| 100K rows range query | ~180 ms | **7.5 ms** |

> Test environment: Apple M3 Pro / Release build / row_cache_size=10k

## Bulk Write Strategy

1. Large-scale import: `batch_insert_map()` first, then `CREATE INDEX`
2. Continuous writes: create index first; it is maintained incrementally and automatically
3. After `db.flush()?`, index metadata and data are persisted together

## Runtime Monitoring

```rust
use motedb::database::indexes::ColumnIndexStats;

let stats: ColumnIndexStats = db.inner().column_index_stats("users_email")?;
println!("entry_count={} cache_hit_rate={:.2}%",
    stats.entry_count,
    stats.cache_hit_rate * 100.0,
);
```

## Best Practices

- Only create indexes for high-selectivity columns to avoid wasting memory
- For date/time fields, combine with the time series index (see `11-timestamp-index.md`)
- Periodically refresh statistics via `ANALYZE` (i.e., `db.execute("ANALYZE")`)

## Troubleshooting

| Symptom | Resolution |
|------|----------|
| Queries are still slow | Check whether the index is being used (`EXPLAIN SELECT ...`) |
| Index uses too much memory | Reduce the number of indexed columns or lower `row_cache_size` |
| Inserts are blocked | Use batch mode or increase `memtable_size_mb` |

---

- Previous: [06 Index Overview](./06-indexes-overview.md)
- Next: [08 Vector Index](./08-vector-index.md)
