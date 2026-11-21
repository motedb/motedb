# Vector Index


## Dimension Validation

A KNN query point whose dimension differs from the column's declared
dimension returns a clear error
(`Vector dimension mismatch: query has N dims, column 'emb' declares M`).

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

## Query Routing and Recall Trade-offs

Two SQL spellings reach the vector index, with different quality/speed
trade-offs (measured on real bge-small-zh 512-dim document embeddings,
740 vectors, 44 zh/en queries — ground truth = exact cosine scan):

| Spelling | Path | Recall@10 | Latency (740 rows) |
|---|---|---|---|
| `ORDER BY emb <-> ?` | graph candidates (overfetch ~4x) + exact rerank | ~99.8% | ~0.6 ms |
| `WHERE KNN_SEARCH(emb, ?, k)` | direct graph search | ~97-98% | ~0.25 ms |

Routing rules for `ORDER BY emb <-> ?`:

- **Below `MOTE_VEC_INDEX_MIN_ROWS` (default 20,000 live rows) the engine
  always runs an exact brute-force scan** — at small scale exactness is
  free and the graph adds latency without benefit.
- With an index and enough rows, the graph serves the query unless more
  than `MOTE_VEC_REBUILD_RATIO` (default 0.05) of live rows are newer
  than the last graph build — then the query falls back to an exact scan
  and a background rebuild is triggered.
- Rows inserted after the graph build are still visible to both paths
  (write-buffer merge) — recall degrades only inside the incremental
  window above the freshness ratio, which triggers the fallback.

The query vector accepts both `'[1.0, 2.0]'` (quoted) and `[1.0, 2.0]`
(bare) literals in both spellings; a wrong-dimension query vector raises
`Invalid argument: Vector dimension mismatch` in both spellings.

**Recall vs data distribution** (25k x 64-dim synthetic, self-queries,
default build R=64/L=300): uniform R@10 = 0.990, moderately clustered
(cluster jitter ±0.4) R@10 = 0.998. Pathologically tight clusters (jitter
±0.1 with 20x centroid separation — near-degenerate neighbor ties) degrade
defaults to R@10 ≈ 0.62; rebuilding with
`WITH (search_list = 1000, max_degree = 128)` restores R@10 = 1.000 at
~0.8 ms p50. If your embeddings are extremely clustered (very low
intra-class variance), raise `search_list` / `max_degree` before
concluding the index lost data.

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


## Build/query tuning knobs (v0.15.3+)

```sql
CREATE VECTOR INDEX vi ON t(emb) WITH (
  metric = 'l2',             -- or 'cosine'
  max_degree = 32,           -- R: neighbors per node (default 64, range 8..=256)
  build_search_list = 40,    -- construction beam (default: adaptive 300/120/60
                             --   by graph size; lower = faster build)
  search_list = 300          -- query-time L (default 300)
);
```

Validated on 100K×384 synthetic corpora (clustered + uniform, self- and
noised-queries, recall@10 = 1.0000 in all cells):

| shape | config | build | vs default |
|---|---|---|---|
| clustered | default (R64/adaptive) | 80.5s | 1× |
| clustered | R=32, bef=40 | 33.7s | **2.4×** |
| uniform | default | 277.3s | 1× |
| uniform | R=32, bef=40 | 163.3s | **1.7×** |
| uniform | R=16, bef=40 | 104.9s | **2.6×** |

Build time floors around ~33s at 100K (vector write + pin + graph flush);
below R=32 the knob savings shrink. For production embeddings, validate
recall on YOUR data before pushing aggressive knobs — synthetic corpora
flatter graph quality.

**Default ladder recalibration (2026-10-24)**: the size-adaptive
construction beam moved from 300/150/80 to **300/120/60** — dual-distribution
calibrated (clustered prof_ann + uniform-random, 50K×128 both, recall@10
1.0000; clustered build 14.5→11.8s, uniform 32.9→28.8s). At industry scale
(220K×384, the benchmark_report_2026-10-21 shape) the new default builds in
**98.7s vs 171.4s (-42%)** with recall@10 0.9990 and query p50 0.52ms;
strict 1.0000 recall remains reachable via `WITH (search_list = 300+)` or the
aggressive R=32/bef=40 row above. The first-1K segment stays at 300 (A1/A5
tight-cluster recall gate).

**Query forms**: `emb <-> [1,2,3]` (vector literal), `emb <-> '[1, 2, 3]'`
(quoted bracketed string), and `emb <-> ?` (parameter) are all accepted.
