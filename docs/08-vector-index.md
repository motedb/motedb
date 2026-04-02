# Vector Index

An approximate nearest neighbor index based on a FreshDiskANN + LSM hybrid architecture, suitable for RAG, recommendation systems, semantic search, and similar scenarios.

## Core Capabilities

- Supports L2, inner product, and cosine distance (using `<->`, `<#>`, `<=>` respectively in SQL)
- Online/offline hybrid construction: build once after batch import, or update incrementally in real time
- Built-in caching and partitioning strategies; achieves 95%+ recall for 128-dimensional vectors on a single machine

## Creating an Index

```sql
CREATE TABLE documents (
    id INT,
    title TEXT,
    embedding VECTOR(128)
);

CREATE VECTOR INDEX docs_embedding ON documents(embedding);
```

Or via the API:

```rust
use motedb::Database;

let db = Database::open("docs.mote")?;
db.create_vector_index("docs_embedding", 128)?;
```

## Data Import

```rust
use motedb::{Database, types::{SqlRow, Value}};
use std::collections::HashMap;

let mut rows = Vec::new();
for i in 0..1000 {
    let mut row = HashMap::new();
    row.insert("id".into(), Value::Integer(i));
    row.insert("title".into(), Value::Text(format!("Doc {}", i)));
    row.insert("embedding".into(), Value::Vector(vec![0.1; 128]));
    rows.push(row);
}

db.batch_insert_with_vectors_map("documents", rows, &["embedding"])?;
```

## Query Examples

```rust
// L2 distance
db.query(r#"
    SELECT id, title
    FROM documents
    ORDER BY embedding <-> [0.12, 0.03, ...]
    LIMIT 10
"#)?;

// Inner product ordering
db.query(r#"
    SELECT id, title
    FROM documents
    ORDER BY embedding <#> [0.12, 0.03, ...]
    LIMIT 10
"#)?;
```

You can also call the API directly:

```rust
let candidates = db.vector_search("docs_embedding", &query_vec, 10)?;
```

## Performance and Resources

| Dataset | Recall@10 | P95 Latency | Memory | Build Time |
|---------|-----------|-------------|--------|------------|
| 100k x 128-dim | 95.2% | 4.7 ms | 210 MB | 38 s |
| 1M x 768-dim | 93.8% | 8.9 ms | 1.7 GB | 11 min |

> Parameters: R=32, L=50, PQ disabled, Apple M3 Pro (Release)

## Tuning Recommendations

- **Prioritize recall**: increase `R` or `alpha`, or enable multi-batch reranking
- **Prioritize throughput**: decrease `L`, use PQ compression, enable intra-batch SIMD
- **Persistence**: `db.flush()?` flushes vector index metadata and graph structure to disk

## Monitoring and Maintenance

```rust
use motedb::database::indexes::VectorIndexStats;

let stats: VectorIndexStats = db.vector_index_stats("docs_embedding")?;
println!("vectors={} avg_neighbors={:.1}", stats.total_vectors, stats.avg_neighbors);
```

- Periodically run `VACUUM INDEX docs_embedding` (or call via `db.execute`) to reclaim orphaned nodes
- Use `transaction_stats()` to monitor lock contention during writes

## Common Issues

| Issue | Solution |
|-------|----------|
| Decreased query recall | Verify that the index dimensions match the data; increase `R/L` appropriately |
| Long build time | Use `batch_insert_with_vectors_map()` to batch writes; disable PQ |
| Memory limit exceeded | Enable PQ or increase `bloom_filter_bits` to reduce redundant caching |

---

- Previous: [07 Column Index](./07-column-index.md)
- Next: [09 Text Index](./09-text-index.md)
