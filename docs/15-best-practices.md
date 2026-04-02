# Best Practices

A summary of lessons learned from deploying MoteDB in production, covering schema design, performance, and maintainability.

## 1. Architecture and Schema

- **Primary keys**: Always explicitly create a `PRIMARY KEY` or unique column index for efficient point lookups
- **Multi-modal fields**: Separate text, vector, and spatial fields to avoid oversized single-table columns
- **Schema versioning**: Use `ALTER TABLE` instead of directly modifying SST files, and track changes via `manifest`

## 2. Data Ingestion

- For small batches (<= 100 rows), use direct SQL INSERT; for large batches, use `batch_insert_map()`
- Call `db.flush()?` explicitly after writes, or set a reasonable `auto_flush_interval`
- Enable WAL (`enable_wal=true`) and run a checkpoint every 5 minutes

## 3. Queries and Indexes

- Create column indexes on all hot columns used in `WHERE`/`JOIN` clauses
- Enable vector/text/spatial indexes only as needed to avoid unnecessary index overhead
- Run `ANALYZE` after every major data change to refresh statistics

## 4. Transaction Strategy

- Open only one explicit transaction per business operation and `COMMIT` immediately when done
- Use savepoints only for partially rollable batch operations
- Monitor active transactions with `transaction_stats()` and keep the count below 2 x CPU cores

## 5. Resource Management

- `memtable_size_mb`: Increase for write-heavy workloads; keep default for read-heavy workloads
- `row_cache_size`: Set based on hot data size to maintain a cache hit rate >80%
- Separate data and index directories to avoid single-disk bottlenecks

## 6. Operations and Maintenance

- Run `VACUUM INDEX` daily to clean up full-text/vector/spatial index fragmentation
- Run `REBUILD TEXT INDEX` weekly (can be scripted)
- Before upgrading, run `cargo test` + `cargo bench` to confirm no performance regression

## 7. Monitoring Metrics

| Metric | Description | Threshold |
|--------|-------------|-----------|
| `txn.total_aborted` | Transaction rollback count | Investigate hotspots if consistently rising |
| `vector_index_stats.memory_usage_mb` | Vector index memory usage | Requires compression if exceeding plan |
| `spatial_index_stats.tree_height` | R-tree height | Rebuild needed if >12 |

## 8. Security and Durability

- Enable both `DurabilityLevel::Full` and WAL for maximum safety
- Back up the `.mote` directory + `manifest` regularly
- Use `checkpoint` to persist state before version upgrades

## 9. Debugging Tips

- Set `MOTEDB_LOG_LEVEL=debug` to get SQL/index execution plans
- Use `EXPLAIN SELECT ...` to check whether indexes are being hit
- Run `test_all_api.sh` for end-to-end API coverage

## 10. Upgrade Path

1. Restore a production snapshot in a staging environment
2. Compile the new version + run `cargo test` + `cargo bench`
3. Run key examples (`examples/api_*`) to ensure API compatibility
4. Switch production; keep the old version available for rollback

---

- Related docs: [12 Performance Tuning](./12-performance.md), [16 FAQ](./16-faq.md)
