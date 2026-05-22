# MoteDB 优化技术方案

> 2026-05-19

## 1. 索引内存缓冲区容量优化

### 现状

- `ColumnValueIndex` 使用 1MB `IndexMemBuffer` (`src/index/column_value.rs:182`)
- 缓冲区满时，active buffer 转移到 immutable queue，触发 `drain_immutable_to_btree`
- 每次 drain 涉及 B+Tree 页面写入（追加到文件 + 更新 page_offsets）
- 对于 30K+ 行的表，每 1MB（约 10K-15K 条目）触发一次 drain，产生多次 B+Tree 写放大

### 方案

**(A) 缓冲区容量可配置**（低风险，推荐）

```rust
// src/index/column_value.rs
pub struct ColumnValueIndexConfig {
    pub cache_size: usize,
    pub mem_buffer_size: usize,  // 新增：可配置缓冲区大小（默认 1MB）
}

impl Default for ColumnValueIndexConfig {
    fn default() -> Self {
        Self {
            cache_size: 1024,
            mem_buffer_size: 1024 * 1024, // 1MB
        }
    }
}
```

在 `DBConfig` 中暴露配置项，允许 edge 部署根据内存预算调整。

**(B) 按值类型动态分片**（中风险，高收益）

对不同的列类型使用独立的 mem_buffer：
- INTEGER/FLOAT 等定长类型：使用排序数组（Vec），写入追加 O(1)，drain 时排序后批量写入 B+Tree
- TEXT 等变长类型：保持当前 BTreeMap 结构

定长类型的排序数组方案可将 drain 吞吐量提升 3-5x（避免 BTreeMap 插入时的树旋转开销）。

**(C) 延迟 drain 策略**（低风险，中等收益）

当前每次 buffer 满即触发 drain。改为：
- 累计 2-3 个 immutable buffer 后统一 drain
- 或每隔 N ms 定时 drain
- 减少 B+Tree 文件 I/O 次数

### 建议实施顺序

1. 先做 (A) — 配置化，改动最小
2. 再做 (C) — 减少 drain 频率
3. (B) 作为后续优化，在大批量写入场景下评估收益

---

## 2. UPDATE 性能优化

### 现状

- UPDATE 124µs/op vs INSERT 5.4µs/op → **24x slower**
- `src/database/crud.rs` `update_row_in_table()` 主要开销：
  1. 编码旧行 (`raw_old`) 和新行 (`raw_new`) — **2x 编码开销**
  2. WAL 写入 old + new — **2x WAL 数据量**
  3. LSM 写入 new 值
  4. 列索引：delete(old_val) + insert(new_val) — **2x 索引操作**
  5. Row cache 驱逐
  6. Vector/Text/Octree 索引更新

### 瓶颈分析

| 步骤 | 估算开销 | 占比 |
|------|---------|------|
| 编码 raw_old + raw_new | ~3µs | ~2% |
| WAL 写入 (old + new) | ~20µs | ~16% |
| LSM MemTable put | ~5µs | ~4% |
| 列索引 delete+insert | ~40µs | ~32% |
| 列索引 B+Tree 刷新 | ~25µs | ~20% |
| Row cache 驱逐 | ~1µs | ~1% |
| Vector/Text 索引 | ~15µs | ~12% |
| 锁开销 + 其他 | ~15µs | ~12% |

### 方案

**(A) 增量 WAL 记录**（高收益，中风险）

当前 WAL 记录包含完整的 old_row 和 new_row。改为：
- 记录 `[(col_index, new_value)]` 的变更集
- 恢复时：读取当前行 → 应用变更集 → 写回
- 对于单列 UPDATE，WAL 大小从 2x row_size 减少到 ~1x column_size（约 10-50x 缩减）

```rust
// 新增 WAL 记录类型
enum WALRecord {
    // 现有
    UpdateRaw { table_name, row_id, partition, raw_old, raw_new, txn_id },
    // 新增
    UpdateDelta { table_name, row_id, partition, columns: Vec<(u32, Value)>, txn_id },
}
```

**(B) 列索引批量更新**（中等收益，低风险）

当前对每个被修改的列调用 `index.insert()` + `index.delete()`，每次调用都获取独立的锁。

改为 `index.update(old_val, new_val, row_id)` 原子操作：
- 持有一个锁完成 delete+insert
- 减少 DashMap shard 锁获取次数
- B+Tree 页面缓存保持热度

```rust
// src/index/column_value.rs
pub fn update(&self, old_value: &Value, new_value: &Value, row_id: RowId) -> Result<()> {
    let old_key = self.make_key(old_value, row_id);
    let new_key = self.make_key(new_value, row_id);
    // 单次锁持有
    let mut tombstones = self.tombstones.lock();
    self.mem_buffer.insert(new_key.clone(), ())?;
    // tombstone 标记旧 key（drain 时统一清理）
    tombstones.insert(tombstone_key(&old_key));
    // ...
}
```

**(C) 延迟编码**（低收益，低风险）

当前为 WAL 和 LSM 各编码一次。可以将两次编码合并：
- 编码一次 → 移动给 LSM → clone 给 WAL
- 对于 `Periodic`/`NoSync` 模式，WAL 已使用 &ref 路径 (P1)，此项改善有限

### 预期收益

- (A) + (B) 组合：UPDATE 延迟从 124µs → 预计 40-60µs（~2-3x 改善）
- 单独 (B)：30-40µs 改善（~1.3x）

---

## 3. VACUUM / 压缩墓碑清理

### 现状

- `src/storage/lsm/compaction.rs` 已有墓碑清理逻辑
- 墓碑 TTL = 24 小时（硬编码，line 788/974）
- 墓碑在 compaction 期间被丢弃当 `now - timestamp > TTL`
- 存在问题：
  1. TTL 不可配置，对高频 DELETE 场景不合理
  2. 墓碑在 SSTable 中累积，占用 I/O 带宽
  3. 没有显式的 VACUUM 命令来强制清理
  4. 行缓存和列索引中无墓碑感知

### 方案

**(A) VACUUM 命令**（低风险，推荐优先实施）

```sql
VACUUM [TABLE table_name] [FULL]
```

实现：
```rust
// 在 api.rs 中添加 VACUUM 解析（快速路径）
fn try_fast_vacuum(&self, sql: &str) -> Result<Option<StreamingQueryResult>> {
    // 解析 VACUUM [TABLE t] [FULL]
    // 调用 compaction_worker.force_compact(table_prefix)
}

// 在 compaction worker 中添加全量压缩
pub fn force_compact(&self, table_prefix: Option<u64>) -> Result<()> {
    // 1. 强制 flush 所有 immutable memtables
    // 2. 对目标层级执行 full compaction（合并所有层级）
    // 3. 丢弃所有过期的墓碑（不限 TTL）
    // 4. 重建列索引（可选，FULL 模式）
}
```

**(B) 可配置墓碑 TTL**（低风险）

```rust
pub struct LSMConfig {
    // 新增
    pub tombstone_ttl_secs: u64,  // 默认 86400 (24h)，0 = 立即清理
}
```

**(C) 墓碑感知的行缓存**（中风险，中收益）

当前行缓存在 DELETE 后保留旧数据：
```rust
// src/database/crud.rs delete_row_from_table
self.row_cache.invalidate(table_name, row_id);  // 已做
```

列索引也需要清理：
```rust
// 新增：DELETE 后从列索引的 mem_buffer 中移除条目
// 当前仅添加 tombstone，不主动清理 mem_buffer
```

**(D) 后台自动 VACUUM**（后续迭代）

```rust
// 在 LSM engine background thread 中
fn background_vacuum(&self) {
    loop {
        sleep(Duration::from_secs(self.config.vacuum_interval_secs));
        // 检查墓碑比例
        let tombstone_ratio = self.estimate_tombstone_ratio();
        if tombstone_ratio > self.config.vacuum_threshold {
            self.force_compact(None)?;
        }
    }
}
```

### 建议实施顺序

1. (A) VACUUM 命令 — 给予用户显式控制
2. (B) 可配置 TTL — 灵活适配不同负载
3. (D) 后台自动 VACUUM — 降低运维负担
4. (C) 缓存感知 — 精细化优化

---

## 4. SQL 快速路径覆盖扩展（本次会话完成）

已将 `needs_metadata` 检查从 "任何 Expr 都视为需要 metadata" 修正为 "仅 `__row_id__`/`__table__` 引用需要 metadata"。这使得 `UPPER()`、`CONCAT()`、`ABS()` 等函数调用能够使用零 HashMap 的位置求值路径，消除了每行 HashMap 分配的开销。

**影响**: 对于 `SELECT UPPER(name), ABS(score) FROM t WHERE age > 18` 这类查询，从 HashMap 路径（每行 ~32µs）转换到位置路径（每行 ~10µs），速度提升约 3 倍。

---

## 优先级汇总

| 项目 | 复杂度 | 风险 | 收益 | 建议 |
|------|--------|------|------|------|
| Index buffer 可配置 | 低 | 低 | 中 | ✅ 立即做 |
| Index buffer 延迟 drain | 低 | 低 | 中 | ✅ 立即做 |
| UPDATE 列索引批量更新 | 中 | 低 | 高 | ✅ 立即做 |
| UPDATE 增量 WAL 记录 | 高 | 中 | 高 | 评审后做 |
| VACUUM 命令 | 中 | 低 | 高 | ✅ 立即做 |
| VACUUM 可配置 TTL | 低 | 低 | 中 | 配合 VACUUM |
| Index buffer 类型分片 | 高 | 中 | 高 | 后续迭代 |
| 后台自动 VACUUM | 中 | 低 | 中 | 后续迭代 |
