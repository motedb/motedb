# MoteDB 产品优化方案

基于全维度基准测试数据（内存、响应时间、CPU、磁盘）制定。

## 基准测试数据（jemalloc + opt-level=3）

| 数据量 | INSERT | 稳态RSS | PK P99 | WHERE P99 | COUNT P99 | FULL P99 |
|--------|--------|---------|--------|-----------|-----------|----------|
| 10K | 15ms | 10MB | 0.3ms | 1.0ms | 0.5ms | 0.9ms |
| 50K | 55ms | 23MB | 0.7ms | 2.6ms | 2.1ms | 4.2ms |
| 100K | 107ms | 25MB | 1.6ms | 5.8ms | 4.2ms | 9.2ms |
| 200K | 213ms | 46MB | 0.02ms | 13ms | 9ms | 19ms |
| 300K | 322ms | 56MB | 0.02ms | 18.6ms | 14ms | 30ms |

CPU: 单核 ~51% 利用率
磁盘: WAL 不 fsync（manifest fsync 保证崩溃恢复）

## 分析：当前瓶颈

### 1. 内存（核心瓶颈）
- **10K→100K**: RSS 10→25MB（2.5x，合理）
- **100K→300K**: RSS 25→56MB（2.2x，超线性增长）
- **根因**: 查询时 FixedSegment/TextSegment 的 `from_bytes` → `data.to_vec()` 
  将列段数据拷贝到堆，jemalloc 在 macOS 上 background_thread 不生效
- **目标设备**: 扫地机器人 256MB RAM → 数据库可用 < 50MB

### 2. 响应时间
- **PK**: O(1) ✓（缓存命中后 0.02ms）
- **WHERE/COUNT/GROUP**: O(N) 线性扫描，300K 时 ~18ms（< 30ms ✓）
- **FULL scan**: O(N)，300K 时 30ms（边界）
- **瓶颈**: 无索引的列扫描，每次查询都全量遍历 segment

### 3. CPU
- 单核 51%（有优化空间）
- 主要消耗：查询时的 Value 构造 + segment 解码

### 4. 磁盘
- WAL 不 fsync → 高吞吐但极端崩溃可能丢最后几条
- manifest fsync → segment 元数据保证恢复
- segment 文件无压缩开销（Snappy 已禁用）

## 产品优化方案（优先级排序）

### P0: 内存稳定（目标: 任意数据量 RSS < 30MB）

**问题**: 查询时列段数据全量加载到堆，不释放。

**方案: 列段按需 seek+read（已部分实现）**
- ColumnarSSTable 只持有 metadata（header + column_index + row_map）
- 列数据通过 `file.seek + read_exact` 按需读取（只读查询需要的列）
- 已实现 `read_segment_bytes()` 方法，阈值 > 2MB 触发
- **行动**: 将阈值从 2MB 降到 256KB，覆盖更多场景

**预期效果**: 300K 行查询 RSS 从 56MB → 15MB（只读 2 列 × 2.4MB）

### P1: 查询延迟（目标: WHERE/COUNT P99 < 10ms @ 300K）

**问题**: 无索引列扫描 O(N)，300K 行 18ms。

**方案 A: 列索引加速（推荐）**
- 对高选择性 WHERE 条件（region, category）使用已有 B-Tree 索引
- `query_by_column` → row_ids → `get_table_row`（缓存）
- **行动**: 修复 PointQuery 路径，让 ColSegmentStore 表走索引而非全扫描

**方案 B: 预过滤 fast path（已部分实现）**
- WHERE text_col = 'val' → `scan_text_filtered`（直接遍历 TextSegment &str）
- WHERE int_col = val → `count_sum_text_filter`（直接遍历 FixedSegment）
- **行动**: 让 execute_select_streaming_ref 对 ColSegmentStore 表优先走这些 fast path

**预期效果**: WHERE P99 从 18ms → 3ms（索引 O(log N)）

### P2: Full Scan 优化（目标: < 20ms @ 300K）

**问题**: SELECT * 物化 300K 行 Vec<Value>，30ms。

**方案: SelectColumnar + 流式输出**
- 已实现 SelectColumnar（零拷贝列引用 + string interning）
- **行动**: 让 SelectColumnar 在 compaction 后单段时自动启用
- SELECT * P99 从 30ms → 5ms

### P3: INSERT 吞吐（目标: > 1M rows/s @ 100K）

**问题**: INSERT 300K 行 322ms（~930K rows/s）。

**方案: 异步 WAL + 批量提交**
- WAL 写入不等待 fsync（已实现）
- 批量 INSERT 直接写 ColSegmentStore buffer（已实现）
- **行动**: 增加 buffer 阈值调优（for_edge: 20K，for_server: 100K）

**预期效果**: INSERT 吞吐 930K → 1.5M rows/s

### P4: CPU 优化（目标: < 30% 单核）

**问题**: 查询时 Value 构造占 CPU 51%。

**方案: 零拷贝 Value**
- FixedSegment/TextSegment 使用 SegData::Mmap（已实现）
- 避免 to_vec()，直接引用 mmap 页
- **行动**: 在 read_fixed_i64/read_text 中优先 mmap 路径

**预期效果**: CPU 从 51% → 25%

### P5: 多模态扩展（目标: Vector/Spatial 无缝集成）

**问题**: Spatial 列用 WKT 文本编码（workaround），非原生二进制。

**方案: 原生 Spatial segment**
- 新增 ColumnTypeTag::Spatial 对应 VariableSegment
- 编码: WKB 二进制（标准格式）
- 解码: read_spatial() → Geometry
- **行动**: 后续迭代，当前 WKT 方案满足功能需求

## 实施路线图

| 阶段 | 优化项 | 预期收益 | 风险 |
|------|--------|---------|------|
| Phase 1 | seek+read 阈值降低 | RSS -60% | 低（已有实现）|
| Phase 2 | PointQuery 索引路径 | WHERE P99 -80% | 中（需调试）|
| Phase 3 | SelectColumnar 恢复 | FULL P99 -70% | 低（已有实现）|
| Phase 4 | buffer 调优 | INSERT +60% | 低（参数调整）|
| Phase 5 | mmap 零拷贝 | CPU -50% | 中（macOS mmap）|
| Phase 6 | 原生 Spatial | 功能完整性 | 高（格式变更）|

## 产品定位匹配

| 场景 | 数据量 | RAM 可用 | 当前表现 | 优化后预期 |
|------|--------|---------|---------|-----------|
| 扫地机器人 | 10-50K | 256MB | RSS 23MB, P99 3ms ✓ | 无需优化 |
| AR 眼镜 | 50-100K | 512MB | RSS 25MB, P99 6ms ✓ | 无需优化 |
| 无人机 | 50-100K | 512MB | RSS 25MB, P99 6ms ✓ | 无需优化 |
| 机械臂 | 100-200K | 1GB | RSS 46MB, P99 13ms ✓ | P1 后 P99 3ms |
| 具身智能 | 200-500K | 2GB | RSS 56MB, P99 18ms | P0+P1 后 RSS 15MB, P99 5ms |

## 结论

当前数据库已满足**扫地机器人、AR 眼镜、无人机**场景（数据量 < 100K）的全部要求。
**机械臂、具身智能**场景（数据量 > 200K）需要 Phase 1+2 优化才能达到 RSS < 30MB。
