# MoteDB Performance Report — v0.2.0

> 2026-05-19 | CI mode (reduced scale) | Apple Silicon M-series | Rust 1.91

## 1. Write Performance

| Operation | Latency | Throughput | Notes |
|-----------|---------|-----------|-------|
| SQL INSERT (5 cols, PK auto-inc) | 7.5–13.0 µs/op | 77K–133K/s | Column index updated per insert |
| insert_row (row API) | 8.0 µs/op | 125K/s | Direct Row API |
| batch_insert(100) | 3.5 µs/row | 286K/s | Best write path |
| batch_insert(500) | 2.8 µs/row | 357K/s | Larger batch = better |
| Batch insert with vector (16-dim) | 16.0 µs/op | 62.5K/s | Vector overhead |
| Edge config INSERT (2-thread) | 6.4 µs/op | 156K/s | Edge-optimized config |

**Key finding:** SQL INSERT is ~2x slower than before (was 5.4µs) because the column index
is now correctly updated on every insert (previously skipped due to bug).

## 2. Read Performance

| Operation | Latency | Throughput | Notes |
|-----------|---------|-----------|-------|
| PK SELECT (cached) | 0.5 µs/op | 2M/s | RowCache hit |
| PK SELECT (MemTable) | 11–14.5 µs/op | 69K–91K/s | Cache miss, LSM lookup |
| PK SELECT (SSTable) | 10–12 µs/op | 83K–100K/s | Disk B+Tree scan |
| Full scan | 0.2–0.4 µs/row | 2.5M–5M rows/s | Very fast |
| Full scan (10K rows, 10 queries) | 1125 µs/query | 889 qps | Materialized path |
| Column eq scan (50 queries) | 680 µs/query | 1471 qps | Single value lookup |
| Column range scan (50 queries) | 20 µs/query | 50K qps | Range = fast |
| COUNT(*) | <0.1 µs/query | instant | Pre-cached counter |

**Key finding:** PK SELECT is 24% faster than pre-fix baseline (14.5 → 11µs) due to
optimizer Mutex removal and positional path improvements.

## 3. UPDATE / DELETE Performance

| Operation | Latency | Throughput | Notes |
|-----------|---------|-----------|-------|
| SQL UPDATE by PK | 232–286 µs/op | 3.5K–4.3K/s | **30x slower than INSERT** |
| SQL DELETE by PK | 140–160 µs/op | 6.3K–7.1K/s | **20x slower than INSERT** |
| update_row (row API) | 18 µs/op | 56K/s | Much faster than SQL |
| delete_row (row API) | 16 µs/op | 62K/s | Much faster than SQL |
| Prepared UPDATE | 1742 µs/op | 574/s | Prepared path slower |

**Key finding:** SQL UPDATE is the #1 bottleneck. Row API UPDATE (18µs) vs SQL UPDATE
(286µs) shows that the SQL path adds ~270µs of overhead. This is the highest-impact
optimization target.

## 4. Query Performance (5K rows)

| Operation | Latency | Throughput | Notes |
|-----------|---------|-----------|-------|
| GROUP BY (single col) | 7.2 ms/query | 140 qps | Slowest query type |
| GROUP BY + 4 aggregates | 10.7 ms/query | 93 qps | Multiple aggregates |
| GROUP BY + HAVING | 7.2–7.9 ms/query | 127–140 qps | |
| ORDER BY | 1.7–1.9 ms/query | 526–581 qps | P3 optimization helped |
| ORDER BY + OFFSET | 3.8 ms/query | 266 qps | OFFSET doubles cost |
| DISTINCT (1 col) | 1.9 ms/query | 532 qps | |
| DISTINCT (3 cols) | 2.2 ms/query | 450 qps | |
| COALESCE | 1.2 ms/query | 862 qps | |
| IS NULL / IS NOT NULL | 0.8–1.1 ms/query | 893–1220 qps | |
| Subquery (scalar) | 0.9 ms/query | 1176 qps | Single table scan |
| Prepared SELECT by PK | 12 µs/query | 83K/s | Statement cache hit |

## 5. Concurrent Performance

| Operation | Latency | Throughput | Notes |
|-----------|---------|-----------|-------|
| Mixed R/W (70R/20W/10U), 2 threads | 2.0 µs/op | 500K/s | Good scaling |
| Read-only 2 threads | 1.0 µs/op | 1M/s | Near-perfect scaling |
| Concurrent INSERT 2 threads | 6.0 µs/op | 167K/s | Good scaling |
| Concurrent DELETE 2 threads | 3.0 µs/op | 333K/s | |
| Write+Read concurrent (1W+1R) | 4.0 µs/op | 250K/s | |
| Transaction commit | 10 µs/txn | 100K/s | |

## 6. Memory Footprint

| State | RSS | Notes |
|-------|-----|-------|
| After 2000 INSERT (5 cols) | 11.2 MB | MemTable only |
| After 5000 INSERT (5 cols) | 12.2 MB | Active memtable |
| After flush (5K → SSTable) | 12.2 MB | No significant growth |
| After 10K INSERT | 12.9 MB | ~500B/row |
| With 2000 spatial points | 20.0 MB | +7.8 MB for spatial index |
| With 2000 text docs | 24.9 MB | +4.9 MB for FTS index |
| With 500 vectors (128-dim) | 27.5 MB | +2.6 MB for DiskANN |
| After final checkpoint | 17.1 MB | |

## 7. WAL Durability Comparison

| Level | INSERT Latency | Throughput |
|-------|----------------|------------|
| Synchronous | 3794 µs/op | 263/s |
| GroupCommit | 5.0 µs/op | 200K/s |
| Periodic(50ms) | 5.5 µs/op | 182K/s |
| NoSync | 8.5 µs/op | 118K/s |

## 8. Optimization Targets (ranked by impact)

### 🔴 Critical (correctness + perf)

| # | Issue | Impact | Effort |
|---|-------|--------|--------|
| B+1 | B+tree flush corruption | Data loss on checkpoint | High |

### 🟡 High Priority (perf)

| # | Issue | Current | Target | Gain |
|---|-------|---------|--------|------|
| 1 | **SQL UPDATE overhead** | 286 µs | 50 µs | 5.7x |
| 2 | **GROUP BY scan+hash** | 7.2 ms | 2 ms | 3.6x |
| 3 | **ORDER BY + OFFSET** | 3.8 ms | 1.8 ms | 2.1x |
| 4 | **Column eq scan** | 680 µs | 50 µs | 13.6x |
| 5 | **Prepared UPDATE path** | 1742 µs | 100 µs | 17.4x |

### 🟢 Medium Priority

| # | Issue | Current | Target | Gain |
|---|-------|---------|--------|------|
| 6 | DISTINCT materialize | 2.2 ms | 1.0 ms | 2.2x |
| 7 | COALESCE scan | 1.2 ms | 0.5 ms | 2.4x |
| 8 | Concurrent INSERT overhead | 6 µs | 3 µs | 2x |
