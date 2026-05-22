# MoteDB Performance Report — v0.2.0

> 2026-05-20 | CI mode (reduced scale) | Apple Silicon M-series | Rust 1.91

## Changes Since 2026-05-19

- **INSERT 2.6x faster**: Deferred B+Tree deletes in `ColumnValueIndex::update`, `try_lock` for
  tombstones and LRU invalidation, reusable index key buffer
- **UPDATE ~20% faster**: Eliminated `raw_new.clone()` via `log_update_raw_ref`, deferred B+Tree
  delete to drain phase
- **B+Tree flush robustness**: Corrupt pages gracefully skipped during checkpoint
- **Compilation fixes**: `ArcString` `From` traits, `Box<Geometry>` in test code, `get_visible_version` signature

## 1. Write Performance

| Operation | Latency | Throughput | vs 05-19 |
|-----------|---------|-----------|----------|
| SQL INSERT (5 cols, PK auto-inc) | **5.0–11.2 µs/op** | 90K–200K/s | **2.6x faster** |
| batch_insert(100) | 3.5 µs/row | 286K/s | Same |
| batch_insert(500) | 2.8 µs/row | 357K/s | Same |

**Key improvement**: INSERT was 13µs after Bug 1 fix (column index always updated). Now 5µs by
deferring B+Tree operations to drain phase and using non-blocking tombstone/LRU invalidation.

## 2. Read Performance

| Operation | Latency | Throughput | vs 05-19 |
|-----------|---------|-----------|----------|
| PK SELECT (cached) | 0.5 µs/op | 2M/s | Same |
| PK SELECT (MemTable) | 11.0 µs/op | 91K/s | Same |
| PK SELECT (SSTable) | 11.0 µs/op | 91K/s | Same |
| Full scan (MemTable) | 0.2 µs/row | 5M rows/s | +100% |
| Full scan (SSTable) | 0.2–0.4 µs/row | 2.5M–5M rows/s | Same |
| COUNT(*) | <0.1 µs/query | instant | Same |

## 3. UPDATE / DELETE Performance

| Operation | Latency | Throughput | vs 05-19 |
|-----------|---------|-----------|----------|
| SQL UPDATE by PK | 228 µs/op | 4.4K/s | ~Same |
| SQL DELETE by PK | 144 µs/op | 6.9K/s | ~Same |
| Prepared UPDATE | 321 µs/op | 3.1K/s | 5.4x faster |
| Prepared DELETE+INSERT | 2.5 µs/op | 400K/s | 362x faster |

**Note**: SQL UPDATE at ~228µs is a structural limit — WAL encode (~50µs) + row encode (~40µs) +
LSM put (~15µs) + column index update (~30µs) + SQL parse (~50µs) + row read (~30µs) ≈ 215µs minimum.

## 4. Query Performance (5K rows)

| Operation | Latency | Throughput | vs 05-19 |
|-----------|---------|-----------|----------|
| GROUP BY (single col) | 7.1 ms/query | 141 qps | Same |
| GROUP BY + 4 aggregates | 10.6 ms/query | 94 qps | Same |
| GROUP BY + HAVING | 7.25 ms/query | 138 qps | Same |
| ORDER BY | 1.76–1.92 ms/query | 521–568 qps | Same |
| ORDER BY + OFFSET | 3.72 ms/query | 269 qps | Same |
| DISTINCT (1 col) | 1.98 ms/query | 505 qps | Same |
| DISTINCT (3 cols) | 2.2 ms/query | 455 qps | Same |
| COALESCE | 1.04 ms/query | 962 qps | +13% |
| IS NULL / IS NOT NULL | 0.82–1.1 ms/query | 909–1220 qps | Same |
| Subquery (scalar) | 10.3 ms/query | 97 qps | — |
| Prepared SELECT by PK | 13 µs/query | 77K/s | Same |
| WHERE BETWEEN | 1.64 ms/query | 610 qps | — |
| WHERE IN (3 vals) | 1.8 ms/query | 556 qps | — |
| WHERE LIKE 'A%' | 2.72 ms/query | 368 qps | — |
| WHERE compound (AND+OR) | 2.18 ms/query | 459 qps | — |

## 5. Concurrent Performance

| Operation | Latency | Throughput | vs 05-19 |
|-----------|---------|-----------|----------|
| Concurrent INSERT (2 threads) | 11 µs/op | 91K/s | Same |
| Mixed CRUD (70R/20W/10U) | 98 µs/op | 10K/s | Same |

## 6. Checkpoint & Recovery

| Operation | Latency | vs 05-19 |
|-----------|---------|----------|
| Fast checkpoint (5K rows) | 83 ms | Same |
| Full checkpoint (10K rows) | 82 ms | Same |
| WAL recovery | 6 ms | Same |
| Auto-increment recovery | 6 ms | Same |

## 7. Remaining Optimization Targets (ranked by impact)

### Priority 1 — Query Path (highest user impact)

| # | Issue | Current | Target | Approach |
|---|-------|---------|--------|----------|
| Q1 | GROUP BY per-row HashMap conversion | 7.1 ms | 2 ms | Positional eval (skip `row_to_sql_row`) |
| Q2 | ORDER BY + OFFSET materialization | 3.7 ms | 1.8 ms | Streaming top-K + skip |
| Q3 | Column eq scan overhead | 640 µs | 50 µs | Direct index probe without scan |

### Priority 2 — Write Path (structural changes needed)

| # | Issue | Current | Target | Approach |
|---|-------|---------|--------|----------|
| W1 | UPDATE WAL encode old row | ~25 µs | 0 µs | Skip old row for non-Sync durability |
| W2 | UPDATE redundant schema lookup | ~8 µs | 0 µs | `_with_schema` variant |
| W3 | UPDATE row clone from cache | ~20 µs | ~5 µs | Return `Arc<Row>` instead of `Row` |

### Priority 3 — Index Optimization

| # | Issue | Current | Target | Approach |
|---|-------|---------|--------|----------|
| I1 | Column eq scan full materialize | 640 µs | 50 µs | Count-only fast path for WHERE col = val |
| I2 | ST_DISTANCE KNN regression | 7 ms | 2 ms | Fix `find_by_column` after checkpoint |
| I3 | Vector INSERT graph rebuild | 3870 µs | 100 µs | Batch insert API |
