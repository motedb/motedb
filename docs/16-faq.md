# 常见问题 (FAQ)

## 写入 & 事务

**Q: 为什么批量插入更快？**  
A: `batch_insert_map()` 会一次性构建 `Row` 并批量写入 MemTable，WAL 只 flush 一次，吞吐可达 737k rows/sec。

**Q: 事务卡住怎么办？**  
A: 检查 `transaction_stats()` 是否存在大量 `active_transactions`，必要时中止长事务或提升 `memtable_size_mb`。

## 索引

**Q: 创建索引后查询仍然慢？**  
A: 使用 `EXPLAIN SELECT` 查看执行计划，确认 WHERE 列与索引列一致；必要时 `ANALYZE` 更新统计。

**Q: 向量索引召回率低？**  
A: 确保 `Vector` 维度与建索引时一致，增大 `R/L` 或在 SQL 层增加 rerank。

**Q: 全文索引体积过大？**  
A: 启用 `ngram`、压缩长文本或定期 `VACUUM TEXT INDEX`。

## 数据类型

**Q: 如何插入时间戳？**  
A: 使用 `Value::Timestamp(Timestamp::from_secs(...))` 或 SQL 的整数 epoch；内部统一为微秒。

**Q: 坐标用什么类型？**  
A: 推荐 `VECTOR(2)` 或 `Value::Spatial(Geometry::Point)`，并结合 `create_spatial_index()`。

## 持久化

**Q: flush 与 checkpoint 有什么区别？**  
A: `flush` 将 MemTable/WAL 写入 SST；`checkpoint` 固定 manifest + 元数据，方便恢复。

**Q: 可以关闭 WAL 吗？**  
A: 可以，但会牺牲崩溃恢复能力，仅在可接受数据丢失时使用，并确保定期备份。

## 调试

**Q: 如何确认索引是否生效？**  
A: `SHOW INDEXES FROM <table>` 查看；`EXPLAIN` 可显示使用的索引名称。

**Q: 如何查看当前内存/索引状态？**  
A: `vector_index_stats()` / `spatial_index_stats()` / `transaction_stats()` 等 API。

## 部署

**Q: 推荐的部署方式？**  
A: 单机模式直接嵌入应用；需要 HA 时建议配合上层复制框架。内存 <10MB、磁盘按表划分目录即可。

**Q: 如何备份？**  
A: quiesce 应用 → `db.flush()?` → 复制整个 `.mote` 目录 + `manifest` + `wal/`。

---

如未覆盖，可在 issue 中提供重现步骤或参考 `examples/` 目录。
