# Performance Tuning Guide

A summary of key configurations and operational patterns that affect MoteDB performance, targeted at high-concurrency and high-throughput deployment scenarios.

## 1. Data Ingestion

### Prefer Batch Operations

| Mode | Use Case | Characteristics |
|------|----------|-----------------|
| `batch_insert_map()` | Large structured datasets | Throughput up to 737k rows/sec |
| `batch_insert_with_vectors_map()` | RAG / embedding pipelines | Automatically triggers vector indexing |

**Recommendation**: Use batches of 1k~10k rows and build indexes after insertion.

### Memtable & Flush

- `memtable_size_mb`: Increase to reduce flush frequency (16~64MB when memory is sufficient)
- `auto_flush_interval`: Increase to 120s for write-heavy, read-light workloads
- Call `db.flush()?` manually to control peak memory usage

## 2. Read Queries

### Row Cache

- `row_cache_size`: Default is 10k; adjust based on the size of your hot data
- Set a higher cache for hot tables and run `ANALYZE` periodically

### Indexes

- Column indexes: essential for WHERE/ORDER BY/JOIN operations
- Vector indexes: combine with reranking and PQ to balance recall and latency
- Text/Spatial indexes: verify their existence via `SHOW INDEXES`

## 3. Data Types and Encoding

- Use `Value::Integer` instead of `Text` for storing enums/booleans
- For large-scale floating-point writes, consider `Value::Tensor`/PQ for dimensionality reduction
- Use `Value::Vector([lon, lat])` for coordinates to facilitate spatial indexing

## 4. Concurrent Transactions

- Split large transactions into multiple batch operations
- Use `transaction_stats()` to observe conflicts and identify hotspots
- During write peaks, temporarily lower `DurabilityLevel` to `Memory`, then switch back to `Full` after the batch completes

## 5. Index Maintenance

| Operation | Recommended Frequency | Effect |
|-----------|----------------------|--------|
| `VACUUM INDEX <name>` | Daily | Clean up deleted entries |
| `REBUILD TEXT INDEX` | After bulk imports | Rebuild inverted index structure |
| `ANALYZE` | After schema changes or data doubling | Refresh statistics |

## 6. Monitoring Metrics

```rust
let txn = db.transaction_stats();
let vec_stats = db.vector_index_stats("docs_embedding")?;
let spatial_stats = db.spatial_index_stats("locations_coords")?;
```

Key metrics:
- `txn.active_transactions`: should be below CPU cores x 2
- `vec_stats.avg_neighbors`: low values indicate a sparse graph
- `spatial_stats.tree_height`: >12 indicates a rebuild is needed

## 7. Hardware Recommendations

- NVMe SSD (>2GB/s) for better flush performance
- When memory is abundant, increase `row_cache_size` and enable column compression
- For vector workloads, prefer CPUs with AVX512/NEON support

## 8. Tuning Workflow

1. Run benchmarks with `cargo bench`/`run_perf_test.sh`
2. Enable tracing (`MOTEDB_LOG_LEVEL=debug`)
3. Adjust `DBConfig` parameters one at a time and record metrics
4. Finalize the configuration and write it into README/deployment scripts

---

- Related docs: [05 Transaction Management](./05-transactions.md), [07~11 Index Topics](./07-column-index.md)
