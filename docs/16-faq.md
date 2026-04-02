# Frequently Asked Questions (FAQ)

## Writes & Transactions

**Q: Why is batch insertion faster?**
A: `batch_insert_map()` constructs `Row` objects in bulk and writes them to the MemTable in one pass. WAL is flushed only once, achieving throughput up to 737k rows/sec.

**Q: What should I do if a transaction is stuck?**
A: Check `transaction_stats()` for a large number of `active_transactions`. If needed, abort long-running transactions or increase `memtable_size_mb`.

## Indexes

**Q: Queries are still slow after creating an index?**
A: Use `EXPLAIN SELECT` to inspect the execution plan and confirm that the WHERE columns match the indexed columns. Run `ANALYZE` to update statistics if necessary.

**Q: Low recall from vector index?**
A: Ensure the `Vector` dimensions match those used at index creation time. Increase `R/L` parameters or add a reranking step at the SQL layer.

**Q: Full-text index is too large?**
A: Enable `ngram`, compress long texts, or run `VACUUM TEXT INDEX` periodically.

## Data Types

**Q: How do I insert a timestamp?**
A: Use `Value::Timestamp(Timestamp::from_secs(...))` or an integer epoch value in SQL. Internally, all values are normalized to microseconds.

**Q: What type should I use for coordinates?**
A: Use `VECTOR(2)` or `Value::Spatial(Geometry::Point)`, combined with `create_spatial_index()`.

## Persistence

**Q: What is the difference between flush and checkpoint?**
A: `flush` writes the MemTable/WAL to SST files; `checkpoint` persists the manifest and metadata for recovery purposes.

**Q: Can I disable WAL?**
A: Yes, but this sacrifices crash recovery. Only use this when data loss is acceptable, and ensure you take regular backups.

## Debugging

**Q: How can I verify that an index is being used?**
A: Run `SHOW INDEXES FROM <table>` to list indexes. Use `EXPLAIN` to display which index names are used in the query plan.

**Q: How can I check current memory and index status?**
A: Use the `vector_index_stats()` / `spatial_index_stats()` / `transaction_stats()` APIs.

## Deployment

**Q: What is the recommended deployment approach?**
A: For single-machine setups, embed MoteDB directly in your application. For HA requirements, use an upper-layer replication framework. Memory <10MB is sufficient to start; organize disk by table directories.

**Q: How do I back up?**
A: Quiesce the application -> `db.flush()?` -> copy the entire `.mote` directory + `manifest` + `wal/`.

---

If your question is not covered here, please open an issue with reproduction steps or refer to the `examples/` directory.
