# 性能优化指南

面向高并发、高吞吐的部署场景，汇总影响 MoteDB 性能的关键配置与操作模式。

## 1. 数据写入

### 批量优先

| 模式 | 适用场景 | 特点 |
|------|----------|------|
| `batch_insert_map()` | 大量结构化数据 | 吞吐 737k rows/sec |
| `batch_insert_with_vectors_map()` | RAG / embedding | 自动触发向量索引 |

**建议**：每批 1k~10k 行，插入后再建索引。

### Memtable & Flush

- `memtable_size_mb`: 增大以减少 flush 次数（内存足够时 16~64MB）
- `auto_flush_interval`: 写多读少可增大至 120s
- 手动 `db.flush()?` 以控制峰值

## 2. 读查询

### 行缓存

- `row_cache_size`: 默认 10k，可根据热数据大小调整
- 热点表设置更高缓存并定期 `ANALYZE`

### 索引

- 列索引：WHERE/ORDER BY/JOIN 必备
- 向量索引：结合 rerank、PQ 以平衡召回和延迟
- 文本/空间索引：通过 `SHOW INDEXES` 检查是否存在

## 3. 数据类型与编码

- 使用 `Value::Integer` 替代 `Text` 存储枚举/布尔
- 浮点大批量写入时考虑 `Value::Tensor`/PQ 降维
- 坐标使用 `Value::Vector([lon, lat])` 便于空间索引

## 4. 并发事务

- 大事务拆分为多个批处理
- 通过 `transaction_stats()` 观察冲突并定位热点
- 写入高峰期可下调 `DurabilityLevel` 到 `Memory`，批量完成后再切回 `Full`

## 5. 索引维护

| 操作 | 建议频率 | 作用 |
|------|----------|------|
| `VACUUM INDEX <name>` | 每日 | 清理删除条目 |
| `REBUILD TEXT INDEX` | 批量导入后 | 重建倒排结构 |
| `ANALYZE` | schema 变化或数据量翻倍 | 刷新统计 |

## 6. 监控指标

```rust
let txn = db.transaction_stats();
let vec_stats = db.vector_index_stats("docs_embedding")?;
let spatial_stats = db.spatial_index_stats("locations_coords")?;
```

关键指标：
- `txn.active_transactions`：应低于 CPU 核数 × 2
- `vec_stats.avg_neighbors`：偏低代表图稀疏
- `spatial_stats.tree_height`：>12 表示需要重建

## 7. 硬件建议

- NVMe SSD（>2GB/s）提供更佳 flush 性能
- 内存充足时提高 `row_cache_size` 并启用列压缩
- 向量场景优先使用 AVX512/NEON 支持的 CPU

## 8. 调优流程范式

1. 基准运行 `cargo bench`/`run_perf_test.sh`
2. 开启 tracing (`MOTEDB_LOG_LEVEL=debug`)
3. 逐项调整 `DBConfig` → 记录指标
4. 固化配置，写入 README/部署脚本

---

- 相关文档：[05 事务管理](./05-transactions.md)、[07~11 索引专题](./07-column-index.md)
