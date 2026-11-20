# Columnar Append-Only Multi-Segment Store (方案 C1)

**日期**: 2026-06-16
**状态**: 已批准，待实现
**作者**: 设计协作产出
**关联**: 解决 v0.3.0 columnar CREATE INDEX 退化（673ms→<30ms）+ IN 子查询返回 0 行 + mmap 越界 panic

---

## 1. 背景与动机

### 1.1 产品定位

MoteDB 是**嵌入式多模态数据库**，对内存、CPU、IO、响应延迟高度敏感。支持标量列（Integer/Float/Bool/Timestamp/Text）与多模态列（Tensor 向量、Spatial 几何）。

### 1.2 当前问题

v0.3.0 的 columnar 存储采用**单 SSTable 全量重写**模型：每次 `finalize_columnar_buffer` 都要把整个表的旧数据逐行读出、塞回 builder、重写整个文件 + fsync。这导致三个问题：

| 问题 | 根因 | 影响 |
|------|------|------|
| CREATE INDEX 慢 ~45x | CREATE INDEX 前调用 finalize，触发全表 merge；且 merge 内 `get_row` 每行重解压整个列 segment（N×cols 次解压） | 60K 行: 346ms/索引 vs 基线 ~15ms |
| IN 子查询返回 0 行 | streaming scan 走 LSM 行存，columnar 表数据不在 LSM（**已修复**） | 功能错误 |
| merge 后 mmap 越界 panic | merge 写出的文件，row_map offset 与实际布局不一致 | mmap 不可用，被迫 read_exact 全文件入堆，内存浪费 |

### 1.3 设计决策（用户确认）

1. **后台异步 compaction** — 写入只 append 新 segment（O(1)），后台线程异步合并
2. **多路归并读（LSM 风格）** — 查询时多 segment 按 (key, timestamp) 归并，同 key 取最新版本
3. **先改造标量 + Text 列** — Vector/Spatial 暂走现有单文件路径（有独立索引）
4. **manifest 文件 + 原子更新** — 保证崩溃后数据一致、不读半完成 segment

---

## 2. 整体架构（§1）

### 2.1 模块定位

新建 `src/storage/columnar_store/` 子系统，与现有 LSM 引擎平级。`MoteDB` 持有 `columnar_stores: DashMap<String, Arc<ColumnarStore>>`（替换现有的 `columnar_sstables`）。每个 columnar 表对应一个 `ColumnarStore`。

```
MoteDB
├── lsm_engine              行存 KV + WAL（非 columnar 表，不变）
├── columnar_stores         新：DashMap<TableName, Arc<ColumnarStore>>
│   └── ColumnarStore
│        ├── segments: RwLock<VecDeque<Arc<Segment>>>   活跃 segment 列表（升序）
│        ├── manifest:  Manifest                        持久化 segment 清单
│        ├── compactor: 后台线程控制
│        └── write_buf: Mutex<ColumnarSSTableBuilder>   内存写入 buffer
└── column_indexes          B-Tree 索引（不变）
```

### 2.2 磁盘布局

每张 columnar 表独立目录：`<db_dir>/columnar/<table_name>/`

```
columnar/sales/
├── 0000000001.sst     不可变 segment 文件（格式 = 现有 ColumnarSSTable）
├── 0000000002.sst
├── 0000000003.sst
└── MANIFEST           segment 清单（唯一 fsync 的文件）
```

### 2.3 关键数据结构

```rust
// src/storage/columnar_store/store.rs
pub struct ColumnarStore {
    table_name: String,
    dir: PathBuf,
    /// 活跃 segments，按生成时间升序（新的在尾部）。查询归并时新的优先。
    segments: RwLock<VecDeque<Arc<Segment>>>,
    /// 内存写入 buffer。flush 时生成新 segment（只含增量，不读旧数据）。
    write_buf: Mutex<ColumnarSSTableBuilder>,
    /// 下一个 segment 编号（原子递增，保证文件名唯一）。
    next_segment_id: AtomicU64,
    /// manifest 持久化（见 §4）
    manifest: Mutex<Manifest>,
    /// compactor 控制（见 §4）
    compaction_state: CompactionState,
    col_types: Vec<ColumnType>,
}

/// Segment = 现有 ColumnarSSTable 的薄包装，附带元数据。
/// 文件格式、RowMap、列段压缩（Snappy）全部原样复用。
pub struct Segment {
    sst: ColumnarSSTable,
    id: u64,                   // 文件编号
    row_count: usize,          // 活跃（非删除）行数
    created_at: Instant,       // 用于 compaction 选段
}
```

### 2.4 不变的东西

- `ColumnarSSTable` 的文件格式、`RowMap`（key/timestamp/deleted）、列段压缩（Snappy）全部原样复用
- LSM 引擎、WAL、checkpoint 机制不动
- column_indexes（B-Tree）不动

---

## 3. 写入路径（§2）

### 3.1 写入流程

```
INSERT batch
  → ColumnarStore::append_rows(rows)        // O(rows)，只写内存 buffer
      → write_buf.add_values(...)           // 现有 builder，零改动
      → 若 buffer 行数 ≥ flush_threshold → flush_buffer()   // 只生成增量 segment
```

`flush_buffer` **只把 buffer 里这批数据写成新 segment，完全不读旧 segment**：

```rust
impl ColumnarStore {
    fn flush_buffer(&self) -> Result<()> {
        // 1. take：把 buffer 内容换出来（新 builder 接管后续写入，锁极短）
        let buf = { let mut g = self.write_buf.lock(); std::mem::take(&mut *g) };
        if buf.num_rows() == 0 { return Ok(()); }

        // 2. 写新 segment 文件（只含这批增量，O(this_batch)），不 fsync
        let id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
        let path = self.dir.join(format!("{:010}.sst", id));
        let sst = buf.finish_into(path)?;

        // 3. append 到 segments（尾部），manifest 记录
        let seg = Arc::new(Segment { sst, id, row_count, created_at: Instant::now() });
        self.segments.write().push_back(seg);
        self.manifest.add_segment(id);   // 异步落盘，不阻塞写入

        // 4. 通知 compactor 检查
        self.maybe_trigger_compaction();
        Ok(())
    }
}
```

**写入延迟**：只有 `add_values`（内存 memcpy）+ 偶尔的 `flush_buffer`（写增量文件）。没有全表读、没有 merge。

### 3.2 资源控制（嵌入式优先）

**buffer 阈值（嵌入式档，收进 DBConfig 可调）**：

```rust
const FLUSH_ROW_THRESHOLD: usize = 1_000;             // 行数
const FLUSH_BYTES_THRESHOLD: usize = 1 * 1024 * 1024; // 1MB
```

buffer 到达**行数或字节**任一阈值就 flush。新增字节计数，避免大 Text/Vector 行未到行数阈值但内存暴涨。

**Segment 数量上限 + 写入背压**：

```rust
const MAX_SEGMENTS: usize = 4;   // 查询归并扇出上限
```

当活跃 segment 达 `MAX_SEGMENTS` 时，`flush_buffer` **同步等待 compaction** 腾出空间（背压），而非无限堆积。保证归并扇出恒定 ≤4。

**Compaction 触发阈值**：

```rust
const COMPACTION_SEGMENT_THRESHOLD: usize = 3;  // ≥3 个 segment 触发后台合并
```

**fsync 策略**：
- segment 文件不 fsync（write 只进页缓存）
- 持久性由现有 WAL + checkpoint 保证
- 只有 manifest 更新 fsync

### 3.3 现有调用点衔接

| 调用点 | 现状 | 新行为 |
|--------|------|--------|
| `persistence.rs:201` checkpoint | 全表 merge | `store.flush_buffer()`（增量）|
| `crud.rs:1221` batch_insert 末尾 | 全表 merge | `store.flush_buffer()`（增量）|
| `column.rs:55` CREATE INDEX 前 | 全表 merge（退化根源） | `store.flush_buffer()`，CREATE INDEX 直接多 segment 扫描 |
| `api.rs:791` 手动 flush | 全表 merge | `store.flush_buffer()` |
| `executor.rs:3131` 全表扫描前 | 全表 merge | **不再需要**（多路归并天然见全部数据）|
| `executor.rs:6822` IN 子查询前 | 全表 merge | **不再需要** |

后两个"不再需要"是关键收益：**查询不再触发任何写操作**。

---

## 4. 查询路径（§3）

### 4.1 归并规则

- **同 key 取 timestamp 最大的**（新覆盖旧）
- **deleted=true 的墓碑覆盖同 key 旧版本**（查询跳过）
- segment 按生成时间**降序**扫描（新优先），命中即短路

### 4.2 堆归并 MergeCursor（O(1) 内存）

利用 segment 内 key 升序，用最小堆归并，避免全量去重集合：

```rust
// src/storage/columnar_store/merge.rs
pub struct MergeCursor {
    /// 最小堆：(key, segment_idx)。peek 出当前全局最小 key。
    heap: BinaryHeap<Reverse<(u64, usize)>>,
    cursors: Vec<SegmentCursor>,
}

impl Iterator for MergeCursor {
    type Item = (u64 /*key*/, u64 /*ts*/, Row);
    fn next(&mut self) -> Option<Self::Item> {
        // 1. 弹出全局最小 key
        // 2. 从所有 peek==min_key 的 cursor 取最新版本（ts 最大，最新 segment 优先）
        // 3. 各自 advance，O(1) 额外内存
    }
}
```

**内存占用恒定**：堆大小 = N（segment 数 ≤4），与表大小无关。

### 4.3 SegmentCursor（单 segment 顺序遍历）

```rust
struct SegmentCursor {
    sst: Arc<ColumnarSSTable>,
    row_indices: Vec<usize>,            // 按 key 升序的行索引
    pos: usize,
    segments_cache: Vec<Option<ColumnarSegment>>,  // 懒加载：用到哪列才解哪列
}
```

列段**懒加载**：查询只 SELECT 几列时只解压那几列。WHERE 过滤列先解，命中后再解 SELECT 列。

### 4.4 统一查询接口

```rust
impl ColumnarStore {
    pub fn scan(&self) -> MergeCursor { ... }
    pub fn scan_projected(&self, col_positions: &[usize]) -> ProjectedMergeCursor { ... }
    pub fn scan_filtered(&self, filter: &ColumnarFilter) -> FilteredMergeCursor { ... }

    /// 单 key 点查：从最新 segment 往旧找，命中即返回。
    pub fn get(&self, key: u64) -> Option<Row> {
        for seg in self.segments.read().iter().rev() {
            if let Some(row) = seg.sst.get_row(key, &self.col_types) {
                return Some(row);
            }
        }
        None
    }
}
```

单 segment 时 `MergeCursor` 退化为单游标透传（零开销）。

### 4.5 多模态列处理

Vector/Spatial 不走新归并路径。`scan` 遇到这些列类型时，委托回现有 `read_vectors` / `read_geometries`（透传，不优化）。本次只优化标量 + Text。

---

## 5. Compaction 与 Manifest（§4）

### 5.1 Compaction 职责

把多个旧 segment 合并成 1 个新 segment：
1. 物理消除墓碑（被覆盖/删除的行不写入新 segment）
2. 合并多版本（同 key 只保留 timestamp 最大版本）
3. 缩减 segment 数（控制归并扇出）

复用 §4 的 `MergeCursor`：

```rust
impl ColumnarStore {
    fn pick_compaction_segments(&self) -> Option<Vec<Arc<Segment>>> {
        let segs = self.segments.read();
        if segs.len() < COMPACTION_SEGMENT_THRESHOLD { return None; }  // 3
        Some(segs.iter().cloned().collect())  // 全量合并（≤4，最简单）
    }

    fn compact(&self) -> Result<()> {
        let old_segs = self.pick_compaction_segments().map(Some)?;
        // 1. 多路归并读，去重去墓碑，写新 segment（不 fsync）
        // 2. apply_compaction：原子切换 segments + manifest 记录
        // 3. GC 旧文件
    }
}
```

### 5.2 Compactor 后台线程

**一个全局线程服务所有表**（嵌入式 CPU 敏感）：

```rust
fn compactor_loop(stores: Arc<DashMap<String, Arc<ColumnarStore>>>, shutdown: Arc<AtomicBool>) {
    while !shutdown.load(Relaxed) {
        let mut compacted = false;
        for entry in stores.iter() {
            if entry.compaction_state.should_compact() {
                let _ = entry.compact();
                compacted = true;
            }
        }
        if !compacted { shutdown.wait_timeout(Duration::from_millis(500)); }
    }
}
```

复用现有 `motedb-auto-flush` 线程的 spawn/shutdown 模式（core.rs 已有 3 个同类线程）。

### 5.3 Manifest（崩溃恢复真相源）

唯一 fsync 的文件。二进制追加记录：

```
[MAGIC: 4B "MOTS"] [version: u16] [record_count: u32]
records:
  AddSegment:     type=1, [id: u64]
  Compaction:     type=2, [new_id: u64] [old_count: u16] [old_ids: u64 × count]
  GcCompleted:    type=3, [count: u16] [ids: u64 × count]
```

**写入协议（崩溃安全）**：
1. compaction 写出新 segment 文件（不 fsync）
2. 追加 `Compaction { new_id, old_ids }`，**fsync manifest**
3. 更新内存 segments 列表
4. 物理删除 old_ids 的 .sst 文件，追加 `GcCompleted`

**崩溃恢复（DB open）**：
1. 读 manifest，重放所有 entry，得到 `(活跃 segments, 待 GC 文件)`
2. 加载活跃 segments 到内存
3. 删除待 GC 孤儿文件 + 不在 manifest 的半完成 segment

manifest 累积超 64 条记录时，重写紧凑快照（只含当前活跃 segment 的 AddSegment），原子替换。

### 5.4 并发模型

compaction 只锁定选定的旧 segment 集合，新 segment 照常 append 到列表尾部。compaction 与写入完全并发，不互斥。代价是 compaction 完成瞬间短暂持有 segments 写锁（重建列表）。

### 5.5 资源控制

| 资源 | 控制 |
|------|------|
| CPU | 单 compactor 线程，低频触发，闲时 sleep |
| IO | segment 不 fsync，只 manifest fsync；compaction 顺序读写 |
| 内存 | compaction 流式（MergeCursor O(1)），不全量加载 |
| 磁盘 | obsolete segment 及时 GC |

---

## 6. 迁移策略与测试（§5）

### 6.1 增量迁移（双轨过渡）

引入 `ColumnarStore`，单 segment 时行为与旧 `ColumnarSSTable` 完全一致（透传，MergeCursor 退化为零开销单游标）。

### 6.2 九步迁移（每步独立验证）

| 步骤 | 内容 | 验证标准 |
|------|------|----------|
| S1 | 新建 ColumnarStore + Segment + MergeCursor 骨架，格式不变 | 单段 store scan/get 与旧 ColumnarSSTable 一致 |
| S2 | append_rows + flush_buffer（增量写） | 多次 flush 生成多 segment，数据完整 |
| S3 | MergeCursor 多路归并 + get 点查短路 | 多 segment 同 key 取最新、墓碑覆盖、O(1) 内存 |
| S4 | manifest（追加写 + fsync + 崩溃重放） | 杀进程重启后 segment 正确、无孤儿 |
| S5 | compaction（后台线程 + 选段 + 原子切换） | compact 后墓碑清除、segment 数降、并发不阻塞 |
| S6 | 切写入路径：columnar_write_bufs → append_rows | repro: CREATE INDEX 60K < 30ms |
| S7 | 切查询路径：executor 扫描点 → scan* | repro: IN 子查询正确、全表扫描正确 |
| S8 | 切 CREATE INDEX：去掉 finalize，多 segment 扫描 | bench_vs_sqlite: CREATE INDEX 接近基线 |
| S9 | 删除旧 columnar_sstables 字段 + 清理死代码 | 全量测试套件通过 |

**S1-S5 纯新增（不碰现有代码）**，S6-S9 才动现有路径。任何一步失败可回退。

### 6.3 已有代码处理

- IN 子查询修复（executor.rs columnar 分支 + build_in_hashset_from_columnar）：保留，S7 改为调用 scan_filtered
- scan_table_rows_streaming 的 enum 改造：保留，S7 委托给 ColumnarStore
- finish_and_reset 的原子 rename：保留，作为 flush_buffer 底层
- is_columnar_table：保留，查询分发仍需

### 6.4 测试策略（四层）

1. **单元测试**（`tests/test_columnar_store.rs`）：MergeCursor 归并正确性、内存恒定、manifest 重放、compaction
2. **崩溃恢复测试**：compaction 中途崩溃，重启后一致
3. **回归测试**（repro_regression.rs）：CREATE INDEX 性能、IN 子查询、WHERE、扫描、点查
4. **基准测试**（bench_vs_sqlite）：完整对比 SQLite，确认无退化 + RSS 稳定

### 6.5 性能验收目标

| 指标 | 现状（退化） | 目标 |
|------|-------------|------|
| CREATE INDEX（60K行，单索引） | 346ms | < 30ms |
| INSERT 批量（60K行） | 65ms | ≤ 65ms |
| IN 子查询（60K行） | 已修 20000 | 20000 正确 |
| 全表扫描（60K行） | 4.8ms | ≤ 5ms |
| 内存峰值（60K行写入） | 未测 | 稳定（背压 + 1MB 上限）|

---

## 7. 风险与缓解

| 风险 | 缓解 |
|------|------|
| manifest 崩溃恢复有 bug | S4 专门的崩溃测试覆盖（杀进程注入）|
| compaction 并发竞争 | S5 并发测试；写锁粒度极小（一次 Vec 重建）|
| 多 segment 归并正确性 | S3 构造多版本/墓碑场景的单元测试 |
| 迁移期间新旧并存混乱 | S1-S5 纯新增不动现有代码；每步独立验证可回退 |
| mmap 越界（历史 bug） | 本次 segment 用 read_exact（已验证正确）；mmap 留待后续单独优化 |
