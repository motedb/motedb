# 最佳实践

总结在生产环境部署 MoteDB 的经验，包括模式设计、性能、可维护性等方面。

## 1. 架构与模式

- **主键**：始终显式创建 `PRIMARY KEY` 或唯一列索引，方便点查
- **多模态字段**：将文本、向量、空间字段拆分，避免单表巨型列
- **Schema 版本**：使用 `ALTER TABLE` 而非直接修改 SST，配合 `manifest` 追踪

## 2. 数据写入

- 小批量（<= 100）直接 SQL INSERT，大批量使用 `batch_insert_map()`
- 写入后显式 `db.flush()?` 或设置合理 `auto_flush_interval`
- 开启 WAL (`enable_wal=true`) 并每 5min checkpoint 一次

## 3. 查询与索引

- 对所有 `WHERE`/`JOIN` 热列建立列索引
- 向量 / 文本 / 空间索引按需开启，避免无用的索引占用
- 每次大规模变更后执行 `ANALYZE` 刷新统计

## 4. 事务策略

- 一次业务事务只打开一个显式事务，完成后立即 `COMMIT`
- 保存点只用于局部可回滚的批处理
- 使用 `transaction_stats()` 监控活跃事务，确保 < 2 × 核心数

## 5. 资源管理

- `memtable_size_mb`: 写多读少场景提高，读多写少保持默认
- `row_cache_size`: 依据热数据规模设置，保证缓存命中率 >80%
- 分离数据与索引目录，避免单磁盘瓶颈

## 6. 运维

- 每日 `VACUUM INDEX` 清理全文/向量/空间索引碎片
- 每周 `REBUILD TEXT INDEX`（可脚本化）
- 升级前先运行 `cargo test` + `cargo bench` 确认性能不回退

## 7. 监控指标

| 指标 | 说明 | 阈值 |
|------|------|------|
| `txn.total_aborted` | 事务回滚数 | 持续上升需排查热点 |
| `vector_index_stats.memory_usage_mb` | 向量索引内存 | 超过规划需压缩 |
| `spatial_index_stats.tree_height` | R 树高度 | >12 需重建 |

## 8. 安全与持久化

- 开启 `DurabilityLevel::Full` 与 WAL 双保险
- 定期备份 `.mote` 目录 + `manifest`
- 使用 `checkpoint` 在版本切换前固化状态

## 9. 调试技巧

- `MOTEDB_LOG_LEVEL=debug` 获取 SQL/索引执行计划
- `EXPLAIN SELECT ...` 判断是否命中索引
- `test_all_api.sh` 运行端到端 API 覆盖

## 10. 升级路径

1. 在 staging 环境恢复生产快照
2. 编译新版本 + `cargo test` + `cargo bench`
3. 运行关键示例（`examples/api_*`）确保 API 兼容
4. 切换生产，保留旧版本可回退

---

- 相关文档：[12 性能优化](./12-performance.md)、[16 FAQ](./16-faq.md)
