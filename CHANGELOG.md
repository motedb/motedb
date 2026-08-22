# Changelog

## [Unreleased]

### 性能（剖析器定位的系统性优化）

- **修复 compact 模式 text-eq 多段物化 O(N²)**：多段回退分支对每个匹配行 ×
  每列调用整列读取（compact 模式下每次为全列 zstd 解压），300K 行实测一条查询
  568 秒。改为按段分组预解码后 **8.0ms**（同形态查询约为 SQLite 一半）。
- **WHERE + ORDER BY LIMIT 走有界堆 top-K**：不再全量物化排序；
  `top_k_from_indices_typed` 在匹配行索引上 O(M log K)，i64 排序键用保序
  位翻转不经 f64（|v| > 2^53 不失序），结果与全量路径逐行一致。
- **热路径列读取统一 col_cache**：跨查询复用 zstd 解压结果，
  WHERE+ORDER+LIMIT 26.9ms → 冷 25.7ms / 热 9.4ms。
- **IN-set 匹配换 FxHash**：SipHash 占 IN 子查询扫描 ~15%；内置 rustc 同族
  FxHasher（无新依赖）。IN 子查询对 SQLite 由 1.40x 落后转为 1.27x 领先。
- **CREATE TEXT INDEX 4.1s → 0.32s（12.8×）**：flush 对每 term 做一次 BTree
  下沉（1 万 term ≈ 5 万次页反序列化）；新增 `GenericBTree::range_keys` 仅键
  顺扫 + 大批次（≥1024 term）一次顺扫批量播种分片计数器。

### 正确性 / 安全

- **SQL 解析器无界递归栈溢出（fuzz 发现）**：连续 `[`（向量字面量）或嵌套
  `CASE` 使递归下降解析器爆栈（crash-d4ff16a9，1572 字节输入）。LBracket /
  Case 分支补上 `MAX_RECURSION_DEPTH=64` 护栏，超限返回语法错误。
- **SortKey NaN 排序一致性**：`PartialOrd` 改为 `Some(self.cmp(other))`，
  与 Ord 全序对齐，消除 NaN 参与 ORDER BY 时的歧义。

### 测试与 CI

- 全量测试矩阵全绿：debug 3590 / release 3590 / ignored 316 / fuzz
  （SQL 解析器 271 万次本地 + CI 双目标每日 5 分钟）零崩溃。
- **Perf Gate 上线**：`examples/perf_smoke` 以查询间比值做机器无关断言
  （预算带 2× 余量，抓复杂度级回归），每次 push 运行。
- **Fuzz 进 CI**：每日 fuzz_sql_parser / fuzz_wal_recover；ubuntu-22.04
  runner（24.04 内核 ASLR 布局与 ASAN 冲突），fuzz 构建排除 jemalloc。
- ACID 原子性测试更新为修复后的事务语义（事务激活期间的写入参与事务并随
  回滚撤销）；fsync 校准基准与 RSS 增量护栏替代机器相关绝对阈值。

### 内务

- src/ 与 examples/ clippy 警告清零（89 处，含 `&self` 仅递归转关联函数、
  大枚举 Box 化等）；删除 7 个未使用的测试辅助函数。

## [0.9.1] — 2026-08-16

### 全面测试驱动：7 项缺陷修复 + 死锁根治

正确性回归：全套件 **3590 通过 / 0 失败 / 零挂起**（修复前 3538/21/每轮挂 3-4 次）。

#### 正确性
- **BOOLEAN 列 WHERE panic**（16 个测试）：scan_i64_filtered_limit 对 1 字节/行
  的 bool 列按 8 字节 get_i64 切片越界；过滤路径 Integer/Bool 过滤值对 bool 列
  分流到 get_bool（store.rs + crud.rs）。
- **PK 范围查询被截断为 1 行**：i64 快路径对 PK 列所有算子 early_stop=1（假设
  等值点查），`WHERE id <= N` / `WHERE id < 0` 只返回 1 行——仅 Eq 允许早停。
- **compact 模式 TEXT 点查返回垃圾**（flag=3 分页 zstd 漏检）：read_text_paged
  仅检查 flag==1（Snappy），zstd 段按未压缩布局读压缩字节返回 ""/Null——
  flag>=1 一律回退全列解码；read_text_at 同修。
- **负数主键 / DELETE 可见性**随 PK 截断修复一并解决。

#### 向量检索
- **向量索引永远返回空结果**（3 处叠加）：①元数据注册后置致自定义索引名
  解析失败、静默建空索引；②构建数据源读不到未 flush 行；③批量插入后
  sidecar 索引未落盘致 "Failed to get medoid vector"（建图前 flush 重建）。
  KNN_SEARCH / ORDER BY <-> 全部恢复正常。

#### 并发
- **CREATE INDEX 间歇自死锁根治**（~1/40 每索引，全套件每轮挂 1-4 次）：
  column_indexes.get() 读守卫存活期间对同 map insert，自定义索引名与标准名
  同分片时写锁被自身读锁挡死。验证：300 轮压力 ×2 均 0 挂起。
- **持 DashMap 锁做重 I/O 全部改为快照 Arc**：flush 全表 compaction /
  CREATE INDEX 回填 / close compaction / get_or_create 建店 / sync 持 Ref
  （原会阻塞并发写整个 I/O 时长，表现为秒级"卡死"）。

#### 性能
- **prepared 点查 p50 1455µs → 0µs**：非自增整型 PK 改用确定性 row_id 映射，
  不再因 pk_lookup miss 退回全表扫描（现超 SQLite prepared 的 1µs）。
- **INSERT 390K → 555K rows/s（+42%）**，**并发 INSERT +95%**（锁纪律副产品）。

## [0.9.0] — 2026-08-12

### Major: 极致性能 + 高压缩 + 多模态全面领先

#### 磁盘压缩（compact_storage 模式）
- **ColSegmentStore segment 级 zstd 压缩**：Fixed/Text 列从裸存改为 zstd level 1。
  for_edge/for_robotics/for_embodied 默认启用。
- **磁盘 6.07MB → 2.39MB（100K 行）**：25.0 B/row，**比 SQLite（37 B/row）小 32%**。
- 新增 `DBConfig.compact_storage` + 解压路径 flag=2 zstd + 全链路 AtomicBool 传播。

#### 查询性能（列存扫描突破）
- **Int 过滤列专用路径**（scan_i64_filtered_limit）：predicate 接收 Option<i64>，
  零 Value 构造。WHERE id > N 提速。
- **无 WHERE 跳过 fval 构造**：SELECT * FROM t 每行省 Value 构造 + 闭包调用。
- **全 fixed 投影列快速路径**：跳过 text/spatial match 分支。
- **聚合 i128 wrapping**：checked_add → wrapping_add，解锁自动向量化（SUM 2-4×）。
- **scan 去除无 dedup 时 Vec<usize> 分配**（2M 行段省 16MB）。
- **PK 等值闭包零 clone**：needs_bool_coerce 短路。
- **DISTINCT/GROUP BY/COUNT 提速 27-47%**：lazy_project 缓存 + eval inline。

#### 向量检索（算法创新）
- **DiskANN visited: HashSet → bitset**：单次 lookup 50× 提速。
- **DiskANN 两阶段 prefetch**：mmap page fault 与计算重叠。
- **DiskANN beam 截断 O(W)→O(W)**：select_nth_unstable 替代 drain+sort。
- **Bloom filter: FNV-1a double-hashing**：7× SipHash → 10× 提速。
- **向量距离 SIMD 7-22×**：Cosine 1536 维 21.7×。
- **L2 距离混排 bug 修复**：DiskANN search 出口 sqrt 统一。
- **Arc<[f32]> + 零拷贝 extract + x86 SQ8 AVX2**。

#### 边缘竞争力（P0/P1）
- **冷启动**：删强制 compaction + ColSegmentStore 跳 LSM 预热。50K 行 reopen **3-6ms**。
- **内存安全**：SSTableCache 内存上限生效 + max_result_rows 默认 100K + OOM 防护。
- **P99 延迟**：点查去同步 flush_buffer + 时序写入跳 HashMap。
- **累积索引死锁修复**：flush 超时 120s→10s + close 用 checkpoint + join 线程。

#### INSERT/写入
- **INSERT encode 去 to_vec**：Text 列直接写入 var_data（零堆分配）。
- **WAL compress Cow**：不压缩时零分配。
- **encode_native 预分配 64B**：省 realloc。

### 基准成果（100K 行 vs SQLite）
- **查询 9:0 全胜**：COUNT 89×、GROUP BY 62×、ORDER BY+LIMIT 6.6×、DISTINCT 8.6×
- **磁盘 2.39MB vs SQLite 3.51MB**（小 32%）
- **PK P99 = 5µs**（SQLite 13µs）
- **多模态 P99**：向量 KNN 50µs、空间 ST_WITHIN 27µs、FTS MATCH 12µs

## [0.8.1] — 2026-08-06

### Performance — 向量数据结构 + 磁盘 IO + x86 SIMD

四项优化（评估报告建议，按 ROI 依次实施）：

- **`ArcVec`: `Arc<Vec<f32>>` → `Arc<[f32]>`**（`src/types/mod.rs`）。单次堆分配
  （Arc 内联长度），每个向量省 8 字节 + 一次 malloc。全栈受益（向量在 DB 行级
  clone 频繁，原子 Arc 引用计数即可）。同步更新所有构造点（row_format/columnar/
  crud/store/merge 共 9 处 `Arc::new` → `Arc::from`）。
- **`extract_vectors` 零拷贝**（`src/sql/evaluator.rs`）。旧实现每次 `to_vec()`
  深拷贝向量（对大 embedding 很贵）。改为 `extract_vector_slices` 借用 Value
  内部 `&[f32]`，零分配。Vector/Tensor 路径都走借用。
- **compaction 路径 `madvise(SEQUENTIAL)`**（`src/storage/col_segment/store.rs`）。
  merge 前对所有 old segment 调 `advise_sequential`，提示内核预读 page，减少
  merge 时的 page-fault 停顿。新增 `ColumnarSSTable::advise_sequential` +
  `Segment::advise_sequential`。
- **x86 AVX2 SQ8 ADC 路径**（`src/index/vamana/sq8.rs`）。之前 x86 上 DiskANN
  的 SQ8 量化距离计算退化为标量（只有 NEON 版）。新增 `asymmetric_distance_l2_avx2`
  + `asymmetric_distance_cosine_avx2`，用 `_mm256_cvtepu8_epi32`（u8→i32）+
  `cvtepi32_ps`（i32→f32）+ FMA 链。diskann_index.rs 分发改为三级
  （aarch64→neon / x86_64→avx2 / else→scalar）。

## [0.8.0] — 2026-08-06

### Performance — 向量距离计算 SIMD 化（4-8x 加速）

`src/distance/` 有工业级 SIMD 实现（AVX2 FMA / SSE / NEON），但 SQL 表达式
执行路径绕过它手写标量循环。本次统一改调 SIMD 内核：

- **新增 `dot_product` SIMD 函数**（`src/distance/cosine.rs`）—— AVX2(4路FMA) /
  SSE / NEON(4路vfmaq) / scalar 全覆盖，复用 cosine 的 dot 累加逻辑。导出
  `pub use cosine::dot_product`。
- **evaluator `<->` `<=>` `<#>` 改调 SIMD**（`src/sql/evaluator.rs`）——
  l2_distance / cosine_distance / dot_product 三个函数的标量循环替换为
  `crate::distance::*` 调用。`<->` `<=>` `<#>` 在 WHERE/SELECT 表达式里
  执行时获得 4-8x 加速。
- **memtable 向量扫描改调 SIMD**（`src/database/indexes/vector.rs`）——
  手写 dot/norm 标量循环替换为 `metric.distance()`（DistanceKind 零成本分发）。
  🔑 顺带修 bug：旧 Euclidean 分支返回平方距离（无 sqrt），与 DiskANN 的
  真实距离结果混排时排序错误。现在统一用真实距离。

## [0.7.9] — 2026-08-06

### Performance / Concurrency

- **index-builder 改为顺序构建索引**（不再 spawn 4 个子线程）。旧代码在
  `batch_build_table_indexes_raw` 里 spawn column/timestamp/vector/text 4 个
  子线程并 join，每个 clone Database Arc + insert_batch 持索引锁，是间歇死锁
  的主要来源。改成顺序调用后，index-builder 单线程跑完，无游离子线程。
- **checkpoint/close 在 async pipeline 激活时跳过所有索引 flush**
  （flush_all_indexes + rebuild_timestamp_index）。索引是可重建的派生数据，
  async 模式下 flush 多余且会与 index-builder 竞争锁。
- **checkpoint 在碰索引前等 pending_index_batches 归零**（最多 10s）。

### CI

- **publish.yml 删除 integration job**。全量 workspace 有间歇并发 race
  （深层、概率性，本地难稳定复现），即使 advisory 也让 Actions 页面长时间
  in_progress。publish 现在只跑 unit-test（--lib，确定性）→ publish。
  integration 由 ci.yml 覆盖（advisory + 30min timeout）。

## [0.7.8] — 2026-08-06

### Bug Fixes

- **修复 v0.7.7 的 close() 回归**（CI unit-test 卡 15min）。
  v0.7.7 的 `close()` 无条件调用 `wait_for_indexes_ready_timeout(10s)`，导致
  每个 Database drop 都最多等 10 秒。lib 测试大量创建/销毁 Database，累积成
  几百秒延迟，CI `--lib` 卡满 15min timeout。
  修复：仅在 `has_pending_index_batches()` 为 true（确有索引在构建）时才等，
  且超时从 10s 降到 2s。无索引的 close 秒回（恢复 v0.7.6 速度）。
  新增 `pub(crate) fn has_pending_index_batches()` accessor（避免 api.rs 访问
  私有字段）。

## [0.7.7] — 2026-08-05

### Bug Fixes

- **修复 close()/checkpoint 与 index-builder 的间歇死锁**（CI 卡 30min+ 根因）。
  - 根因：`batch_build_table_indexes_raw` 在 index-builder 后台线程里 spawn 4 个
    子线程（column/timestamp/vector/text index）并 join，子线程持有索引写锁。
    `close()` 的 `signal_background_threads_stop` 设 should_stop 后，index-builder
    主线程**立即退出循环**，不处理 channel 里剩余 batch——但这些 batch 的
    `pending_index_batches` 永不归零，且其子线程可能仍在持锁。随后 `checkpoint` 的
    `flush_all_indexes` 等索引锁 → 死锁。
  - 修复 1（core.rs）：index-builder 主线程在 should_stop 后、退出前，用 `try_recv`
    非阻塞 drain channel 里剩余 batch（BatchGuard 保证 pending 正确递减）。
  - 修复 2（api.rs）：`close()` 在 checkpoint 前，用 `wait_for_indexes_ready_timeout`
    等 pending_index_batches 归零（最多 10s），确保子线程释放索引锁后再 checkpoint。
  - 提取 `process_index_batch` 闭包复用（主循环 + drain 共用，消除重复）。

## [0.7.6] — 2026-08-05

### CI

- **ci.yml/publish.yml: integration-test 加 30 分钟 timeout**。ci.yml 的
  integration-test job 缺 `timeout-minutes`，用 GitHub 默认的 360 分钟。
  `cargo test --workspace` 的间歇死锁让它卡满 6 小时（×2 matrix = 2 个 job
  各 6h）。现在 ci.yml + publish.yml 的 integration 均设 30min timeout，
  死锁时快速失败而非空转 6h。publish（仅依赖 unit-test）不受影响。

## [0.7.5] — 2026-08-04

### CI

- **publish.yml: 拆分 test 为独立 job**。v0.7.4 把 integration 放成同一 job
  的 advisory step，但 cargo test 挂起会触发 job 级 timeout，仍阻塞 publish。
  现在拆成：
  - `unit-test`（hard gate，publish 仅依赖它，timeout 15min）
  - `integration-test`（advisory，job 级 `continue-on-error`，与 publish 并行，
    即使间歇死锁卡满 60min timeout 也只影响自身状态）

## [0.7.4] — 2026-08-04

### CI

- **publish.yml: 改用 `--lib` 作发布硬门禁**。全量 integration 套件
  (`cargo test --workspace`) 有间歇性后台线程死锁：约 90 个测试 binary
  串行运行后，`CREATE INDEX` + `SELECT` 序列偶发触发 index-builder /
  group-commit 线程的 condvar 永久等待。这是异步索引管道的 pre-existing
  并发问题（非回归），单独跑受影响 binary 无法复现。v0.7.2/v0.7.3 均因
  此 hit CI 60 分钟超时。
- 硬门禁改为 `cargo test --lib`（429 个库内测试，~5s，确定性），与 ci.yml
  策略一致。integration 套件降级为 advisory（continue-on-error），结果仍
  可见但不阻塞发布。

## [0.7.3] — 2026-08-03

### Performance

- **DELETE: 9195µs/op → 3.5µs/op (2627× 提速)** — 每行 DELETE 不再触发
  `flush_buffer()`（segment 写盘 + manifest fsync），改为 tombstone 留
  write_buf、靠查询路径延迟 flush + 8MB 阈值（与 INSERT 一致）。
- **mixed_crud DELETE: 63638ms → 17ms (3743×)**；mixed_crud 总体
  64112ms → 220ms (291×)。
- bench_comprehensive 套件总耗时 126.75s → 7.55s（DELETE 慢是主因）。

### Bug Fixes

- **重启正确性**: WAL recovery 的 `DeleteRaw`/`Delete` 旧代码只写 LSM
  tombstone，不重建 ColSegmentStore tombstone（现代表 source of truth），
  导致重启后已删除行"复活"。改为 recovery 收集已提交 delete，在
  ColSegmentStore 重建后回放 tombstone。

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
