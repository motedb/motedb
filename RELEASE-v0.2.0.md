# MoteDB v0.2.0 Release Report

**Release Date**: 2026-05-09
**Previous Release**: v0.1.7
**Commits**: 21 commits, 117 files changed, +26,531 / -13,952 lines

---

## Highlights

- **Columnar Time-Series Store**: 高频传感器数据专用列式存储，Gorilla 压缩 + 谓词下推
- **3x Performance**: PK 查询 218x 加速、LSM 流式扫描消除 420MB 物化、DiskANN 插入提速
- **Edge/Robotics Presets**: 一行配置适配嵌入式设备 (`DBConfig::for_edge()`)
- **50+ Bug Fixes**: 4 轮对抗审计，修复数据安全、并发死锁、恢复正确性
- **2200 行死代码清理**: 模块化重构，单体 4800 行拆分为 12 个子模块

---

## New Features

### 1. Columnar Time-Series Store

面向高频传感器数据（IMU、电机控制器）的列式存储引擎：

```sql
CREATE TIMESERIES TABLE sensor_data (ts TIMESTAMP, temp FLOAT, zone INT, label TEXT)
  TIMESERIES(ts) TTL(7d);
```

- **Gorilla 压缩**: timestamp delta-of-delta + float XOR + int delta-varint，10-20x 存储压缩
- **Zone Maps + Bloom Filter**: 查询时按 segment 裁剪，避免全量解码
- **TTL GC**: O(1) 过期 segment 文件删除
- **后台 Segment 合并**: 小文件自动合并，保持查询效率
- **预设配置**: `ColumnarConfig::for_robotics()`, `ColumnarConfig::for_edge()`

### 2. Write Controller (Backpressure)

令牌桶限速器 + L0 SSTable 监控，写入压力过大时自动 `SlowDown` / `Stop`：

```rust
let status = db.write_controller.status();
// Normal / SlowDown / Stop
```

### 3. Auto-Checkpoint

默认启用的 WAL 自动检查点：

```rust
let config = DBConfig {
    auto_checkpoint: Some(AutoCheckpointConfig {
        max_wal_size_bytes: 16 * 1024 * 1024,
        min_interval_secs: 60,
    }),
    ..Default::default()
};
```

### 4. Edge/Robotics Configuration

```rust
// 嵌入式设备：4MB memtable、2 分区、500 行缓存、120s 检查点
let db = Database::create_with_config("data.mote", DBConfig::for_edge())?;

// 机器人：8MB memtable、高频写入、传感器优化
let db = Database::create_with_config("data.mote", DBConfig::for_robotics())?;
```

### 5. Streaming Query Result

内存高效的流式查询接口：

```rust
let stream = db.execute("SELECT * FROM large_table")?;
// StreamingQueryResult — 按需迭代，不一次性物化所有行
```

### 6. Tokenizer Plugin System

```rust
use motedb::tokenizers::Tokenizer;
// 自定义分词器，扩展全文索引能力
```

---

## Performance Benchmarks

All benchmarks run on macOS (Apple Silicon), release build, single-threaded.

### Core Operations (50K rows)

| Operation | Throughput | Latency |
|---|---|---|
| INSERT (5 cols, auto PK) | **21,796 ops/s** | 45.9 µs/op |
| PK SELECT (memtable) | **133,333 ops/s** | 7.5 µs/op |
| PK SELECT (SSTable + cache) | **135,135 ops/s** | 7.4 µs/op |
| PK SELECT (fully cached) | **2,500,000 ops/s** | 0.4 µs/op |
| Full scan (50K rows) | **3,125,000 rows/s** | 0.3 µs/row |
| COUNT(*) | **instant** | <1 µs |
| WAL Recovery (30K rows) | — | **5 ms** |
| Auto-increment Recovery (50K) | — | **8 ms** |
| Cold Start (baseline) | — | **<1 ms** |

### Concurrent Operations

| Operation | Throughput |
|---|---|
| 4-thread INSERT (10K total) | **6,313 ops/s** |
| Mixed CRUD (51K ops) | **7,271 ops/s** |

### Index Performance

| Index Type | Operation | Latency |
|---|---|---|
| Vector (DiskANN, 2K×128d) | ANN top-10 | p50 < 1µs |
| Vector (SQL) | ORDER BY emb <-> q LIMIT 10 | p50 = 18µs |
| Text (FTS, 10K docs) | MATCH AGAINST (2 terms) | p50 = 74µs |
| Text (FTS, 10K docs) | MATCH AGAINST (3 terms) | p50 = 177µs |
| Text (API) | text_search_ranked top-10 | p50 = 14µs |
| Spatial (i-Octree, 10K pts) | ST_WITHIN bbox | p50 = 1µs |
| Spatial (i-Octree, 10K pts) | ST_DISTANCE ORDER BY | p50 = 6.5ms |
| Spatial (i-Octree, 10K pts) | ST_KNN top-10 | p50 = 6.5ms |

### Time-Series Store (50K rows)

| Operation | Performance |
|---|---|
| Ingest | **486,182 rows/s** |
| Full scan (50K) | **2,583,089 rows/s** |
| Narrow time range (2.5K) | 1.31ms |
| Zone filter (5K) | 7.72ms |
| Label bloom miss | **3.5µs** (instant skip) |
| Combined time+zone (2.5K) | 4.52ms |
| Pruning ratio | **~10x** reduction |

### Lifecycle & Durability

| Operation | Time |
|---|---|
| INSERT 50K → flush → checkpoint → close → reopen → query | 2.35s + 508ms + 24ms + 9ms |
| Fast checkpoint (30K rows) | 47ms |
| Full checkpoint (60K rows, with rebuild) | 105ms |
| Fast/Full speedup | **2.2x** |
| Second fast checkpoint (no work) | **<1ms** |

### Multimodal Memory (10K rows × 3 modalities)

| Phase | RSS |
|---|---|
| Baseline | 4.1 MB |
| After 10K inserts | 27.1 MB |
| After checkpoint | 28.5 MB |
| After 50 vector + 50 spatial + 50 text queries | 41.1 MB |
| **Total Δ** | **36.9 MB** (~3.7 KB/row for 3 indexes) |

---

## Bug Fixes (50+ fixes across 4 audit rounds)

### Data Safety (P0)
- CRC32 checksum验证 SSTable 数据完整性
- WAL flush 重试机制 + bounds check
- 14 个数据安全 bug 修复 + 18 项 compaction 完整性测试套件
- BTree split-leaf index OOB panic 修复

### Concurrency & Deadlocks
- 文本索引 pipeline 3 处死锁修复
- 异步 pipeline 双重插入导致 posting list 损坏修复
- `close()` 信号机制：先停止后台线程再 checkpoint
- 共享 `is_pipeline_active` Arc 标志解决 clone 实例竞态

### Recovery & Correctness
- PK lookup 重启后 column index 缺失时的 fallback
- WAL compression 向后兼容解压
- BTreeMap scan 物化消除（420MB → 20KB）
- SUM 精度修复
- DiskANN 缺失向量 unwrap panic → filter_map 安全处理

---

## Architecture Changes

### Modularization
```
database_legacy.rs (4,798 lines)
  → core.rs + crud.rs + table.rs + helpers.rs
  + persistence.rs + indexes/ + transaction.rs
  + pk_cache.rs + timeseries.rs + ttl_gc.rs
  + write_controller.rs + mem_buffer.rs
```

### LSM Streaming Scan
- Old: `scan_range()` → materialize full `BTreeMap` (~420MB for 300K rows)
- New: `scan_range_streaming()` → 13 iterators × ~1.5KB = ~20KB total
- **99.995% memory reduction**

### WAL Compression
- Records > 128 bytes → zstd compressed
- Backward compatible: auto-detects legacy format

### Index Update Strategy
```rust
enum IndexUpdateStrategy {
    BatchOnly,    // 默认：写入时不更新索引，异步 pipeline 批量构建
    Hybrid,       // 重要索引实时，其余批量
    Realtime,     // 所有索引立即更新
}
```

---

## Breaking Changes

1. **`auto_checkpoint` 默认启用**: v0.1.7 没有自动检查点；v0.2.0 默认 16MB WAL / 60s 间隔。如需禁用：
   ```rust
   let config = DBConfig { auto_checkpoint: None, ..Default::default() };
   ```

2. **`IndexUpdateStrategy::BatchOnly` 为默认策略**: 写入时不再同步更新索引，由异步 pipeline 批量处理。查询可能有短暂延迟。如需实时索引：
   ```rust
   let config = DBConfig {
       index_update_strategy: IndexUpdateStrategy::Realtime,
       ..Default::default()
   };
   ```

3. **`execute_sql()` 移除**: 使用 `Database::execute()` 替代

4. **`DBConfig` 新增字段**: `columnar_config`、`pk_lookup_capacity`、`auto_checkpoint`。反序列化旧配置使用 serde default 自动填充。

---

## Test Coverage

| Suite | Tests | Status |
|---|---|---|
| Library unit tests | 319 | ✅ All pass |
| Fast paths (SQL) | 22 | ✅ All pass |
| Compaction integrity | 18 | ✅ All pass |
| ACID benchmark | 8 | ✅ All pass |
| Comprehensive benchmark | 12 | ✅ All pass |
| Resource benchmark | 4 | ✅ All pass |
| Multimodal benchmark | 4 | ✅ All pass |
| Time-series benchmark | 2 | ✅ All pass |
| **Total** | **389** | **✅ All pass** |

---

## Upgrade Guide

```toml
# Cargo.toml
[dependencies]
motedb = "0.2.0"
```

Most applications can upgrade without code changes. If you need real-time index consistency:

```rust
let config = DBConfig {
    index_update_strategy: IndexUpdateStrategy::Realtime,
    ..Default::default()
};
let db = Database::create_with_config("data.mote", config)?;
```
