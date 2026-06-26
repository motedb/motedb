# Changelog

## [0.5.0] — 2026-06-26

### Performance (vs SQLite, 300K rows — MoteDB wins 7/11)

- **WHERE col='val' (high selectivity): 9245µs → 10µs (925x)** — secondary column
  index point-lookup replaces full scan
- **SELECT DISTINCT region: 9825µs → 501µs (19x)** — adaptive early-exit for
  low-cardinality columns (no cardinality hint needed)
- **ORDER BY col LIMIT K**: top-K bounded-heap + per-column decode cache
  (O(N log K), zero per-row allocation)
- **GROUP BY + aggregates: 7.3ms vs 51.6ms (7x faster)** — columnar aggregate pushdown
- **IN subquery: 4.4ms vs 31.3ms (7x faster)**
- **COUNT/SUM/MIN/MAX WHERE: 4.5ms vs 14.8ms (3x faster)**

### Scale (50K → 1M rows)

- P99 < 18ms at 1M rows (target was <100ms) ✅
- RSS 37.2MB at 1M rows (target was <100MB) ✅
- Steady-state <50MB for 80%+ of runtime ✅
- Linear latency scaling across scan/WHERE/GROUP BY/aggregate

### Multimodal (vs competitors)

- FTS search: P50=1µs, P99=2µs (parity with SQLite FTS5)
- Spatial KNN: 1.5x faster than SQLite RTree
- Vector KNN: DiskANN-based, P99=554µs for 10K 128-dim vectors

### Bug Fixes

- **bulk_load multi-page corruption**: leaf page capacity used 16384 but
  `read_page_arc` requires `content_len ≤ PAGE_SIZE (4096)` — caused index
  reads to fail for any dataset spanning 2+ leaf pages (300+ entries). Fixed by
  using `PAGE_SIZE` consistently for leaf + internal page sizing.
- **Compaction merge unsorted keys**: merging multiple segments appended rows
  newest-first, producing an unsorted `row_map` that broke `find_key()` binary
  search — all PK point lookups returned empty after `vacuum()`. Fixed by
  collecting all rows, sorting by key, then writing (newest-version-wins dedup).
- **DELETE → COUNT(*) inconsistency**: tombstones left only in the in-memory
  write buffer were invisible to some read paths (COUNT/SELECT via
  materialize_as_streaming), causing deleted rows to reappear. Fixed by flushing
  the tombstone segment on DELETE so all read paths observe it.
- **count_live_rows newest-version-wins**: a tombstone that lands after its live
  row in the same segment (tombstone appended last = newest) was missed because
  the scan iterated rows oldest→first, recording the live row and skipping the
  tombstone. Fixed by iterating rows newest→oldest within each segment. Also
  fixed buffered-tombstone handling across buffer + segments.

### Code Cleanup

- Removed dead code: `BatchBlockCursor` struct + impl, `next_entry_raw`,
  `try_aggregate_columnar` (superseded by `_fast` / `_partial_scan` variants)
- Eliminated duplicate `RowMap::compute_sizes` call in segment load (minor perf)
- Compiler warnings reduced 61 → 34

## [0.4.0] — 2026-06

### Architecture

- ColSegmentStore: append-only multi-segment columnar storage (source of truth)
- DELETE path writes columnar tombstones (LSM reduced to recovery-only)
- fast_batch_insert: AUTO_INCREMENT tables skip SQL parsing, write directly to store
- jemalloc arena purge for RSS control (`arena.<i>.purge` via tikv-jemalloc-ctl)
- FTS top-K result cache (LRU of token→row_ids)
- Zero-copy scan infrastructure (raw SSTable path + CRC skip)

### Performance

- INSERT: 1.7M rows/s via fast_batch_insert
- CREATE INDEX: 109ms (bulk_load B+Tree + rayon sort)
- FTS: 536µs → 1µs via MATCH fast path + top-K cache

## [0.3.0] — 2026-06-08

### Major: Columnar Storage Engine

- **Columnar SSTable** — column-oriented storage with Snappy compression, mmap zero-copy access
- **Zero-encode INSERT** — Values pushed directly to per-column buffers, no RawRow encoding
- **SelectColumnar** — zero-materialization result type, lazy Vec<Value> conversion
- **6 columnar fast paths**: full scan, equality filter, prefix filter (LIKE), Top-K (ORDER BY), aggregate pushdown (COUNT/SUM), GROUP BY pushdown

### Performance

- INSERT: 354ms → 125ms (2.8x faster, 2.4M rows/s)
- CREATE INDEX: 2900ms → 30ms (97x faster)
- WHERE =: 57ms → 11ms (5.2x faster)
- ORDER BY LIMIT: 32ms → 2.6ms (12x faster)
- COUNT WHERE: 67ms → 2.8ms (24x faster)
- Memory: 621 B/row → 257 B/row (59% less)
- Disk: Snappy compression (~1.8x)

### Multimodal

- Vector index: columnar build via `read_vectors` (zero-copy from mmap)
- Text index: columnar bulk build via `build_text_index_from_columnar`
- Spatial index: columnar build via `read_spatial` + `build_ioctree_from_columnar`
- Timestamp index: columnar build via `FixedSegment`

### ACID

- WAL protection on all write paths (INSERT/UPDATE/DELETE)
- VersionStore MVCC with snapshot isolation
- Auto-finalize at 10K rows + checkpoint
- Crash recovery: WAL replay + `*_col.sst` auto-discovery
- UPDATE/DELETE lazy-init columnar buffer

### Architecture

- LSM reduced to recovery-only (memtable 1MB)
- Column indexes skipped when columnar active (-40MB)
- RowMap/FixedSegment/TextSegment zero-copy from mmap
- Sequential file write (no BufWriter seek)
- String interning pool in materialize

### Fixes

- CachedIndex hash collision (FastKey: Arc<str>)
- MVCC update conflict detection
- GroupCommit durability (wait for fsync)
- Integer→Float precision loss (>2^53)
- PK uniqueness TOCTOU race
- Spatial/Vector columnar encoding
- COUNT/SUM/MIN/MAX WHERE aggregate bug
- UPDATE/DELETE columnar buffer creation race

## [0.2.1] — 2026-05

- Zero-copy scan via ValueBytes (Arc-shared block data)
- SchemaDecodeContext with skip_magic_check, has_nullable_columns
- StringPool text interning (Arc<str> dedup)
- Streaming ORDER BY LIMIT Top-K heap
- mmap SSTable, buffer reuse, O(1) fixed_idx

## [0.1.0] — 2026-03

- LSM storage engine (MemTable + SSTable + Compaction)
- SQL parser and query executor
- Row-based binary format (RawRow)
- Transaction support (BEGIN/COMMIT/ROLLBACK)
- Column value indexes (B-tree)
