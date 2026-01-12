# 向量索引 (Vector Index)

基于 FreshDiskANN + LSM 融合架构的近似最近邻索引，适用于 RAG、推荐、语义检索等场景。

## 核心能力

- 支持 L2、内积、余弦距离（SQL 中分别使用 `<->`、`<#>`、`<=>`）
- 在线/离线混合构建：批量导入后一次性构建，或实时增量更新
- 内置缓存与分区策略，单机 128 维向量可达 95%+ 召回

## 创建索引

```sql
CREATE TABLE documents (
    id INT,
    title TEXT,
    embedding VECTOR(128)
);

CREATE VECTOR INDEX docs_embedding ON documents(embedding);
```

或通过 API：

```rust
use motedb::Database;

let db = Database::open("docs.mote")?;
db.create_vector_index("docs_embedding", 128)?;
```

## 数据导入

```rust
use motedb::{Database, types::{SqlRow, Value}};
use std::collections::HashMap;

let mut rows = Vec::new();
for i in 0..1000 {
    let mut row = HashMap::new();
    row.insert("id".into(), Value::Integer(i));
    row.insert("title".into(), Value::Text(format!("Doc {}", i)));
    row.insert("embedding".into(), Value::Vector(vec![0.1; 128]));
    rows.push(row);
}

db.batch_insert_with_vectors_map("documents", rows, &["embedding"])?;
```

## 查询示例

```rust
// L2 距离
db.query(r#"
    SELECT id, title
    FROM documents
    ORDER BY embedding <-> [0.12, 0.03, ...]
    LIMIT 10
"#)?;

// 内积排序
db.query(r#"
    SELECT id, title
    FROM documents
    ORDER BY embedding <#> [0.12, 0.03, ...]
    LIMIT 10
"#)?;
```

也可直接调用 API：

```rust
let candidates = db.vector_search("docs_embedding", &query_vec, 10)?;
```

## 性能与资源

| 数据量 | 召回@10 | P95 延迟 | 内存 | 构建耗时 |
|--------|---------|----------|------|-----------|
| 100k × 128 维 | 95.2% | 4.7 ms | 210 MB | 38 s |
| 1M × 768 维 | 93.8% | 8.9 ms | 1.7 GB | 11 min |

> 参数：R=32, L=50, PQ 关闭，Apple M3 Pro (Release)

## 调优建议

- **召回优先**：增大 `R` 或 `alpha`，或开启多批 rerank
- **吞吐优先**：减小 `L`、使用 PQ 压缩、启用批内 SIMD
- **持久化**：`db.flush()?` 会将向量索引元数据与图结构刷盘

## 监控与维护

```rust
use motedb::database::indexes::VectorIndexStats;

let stats: VectorIndexStats = db.vector_index_stats("docs_embedding")?;
println!("vectors={} avg_neighbors={:.1}", stats.total_vectors, stats.avg_neighbors);
```

- 定期运行 `VACUUM INDEX docs_embedding`（或 `db.execute` 调用）回收孤立节点
- 结合 `transaction_stats()` 监控写入期间的锁等待

## 常见问题

| 问题 | 解决方案 |
|------|----------|
| 查询召回降低 | 确认建索引时维度与数据一致；适当增大 `R/L` |
| 构建耗时长 | 使用 `batch_insert_with_vectors_map()` 合并写入；关闭 PQ |
| 内存超限 | 开启 PQ 或增大 `bloom_filter_bits` 减少冗余缓存 |

---

- 上一篇：[07 列索引](./07-column-index.md)
- 下一篇：[09 全文索引](./09-text-index.md)
