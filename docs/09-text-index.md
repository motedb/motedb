# 全文索引 (Text Index)

基于自研 BM25 + 可插拔分词器的全文检索引擎，支持中文、英文及 n-gram token。

## 特性概览

- BM25 排序 + TF/IDF 统计
- Tokenizer 插件：Whitespace、N-Gram、Jieba（通过 `tokenizers` 模块扩展）
- 语法：`MATCH(column, 'query')` + `BM25_SCORE`
- 支持与 SQL 条件组合 (WHERE、ORDER BY、LIMIT)

## 建表与索引

```sql
CREATE TABLE articles (
    id INT,
    title TEXT,
    content TEXT,
    tags TEXT
);

CREATE TEXT INDEX articles_content ON articles(content);
```

## 查询范式

```sql
SELECT id, title, BM25_SCORE(content, 'rust database') AS score
FROM articles
WHERE MATCH(content, 'rust database')
ORDER BY score DESC
LIMIT 20;
```

API 风格：

```rust
let hits = db.text_search_ranked("articles_content", "rust database", 20)?;
for (row_id, score) in hits {
    println!("row_id={} score={:.4}", row_id, score);
}
```

## 数据导入与刷新

- 标准 `INSERT`/`batch_insert_map()` 即可；全文索引会监听 WAL 更新并增量合并
- 大规模导入可以：
  1. `batch_insert_map()`
  2. `db.execute("REBUILD TEXT INDEX articles_content")`
  3. `db.flush()?`

## Tokenizer 配置

```sql
CREATE TEXT INDEX articles_content
ON articles(content)
USING TOKENIZER ngram(2);
```

可选项：
- `whitespace`（默认）
- `ngram(n)`
- `jieba`（需启用插件）

## 性能指标

| 语料 | QPS | 平均延迟 | 索引体积 |
|------|-----|----------|----------|
| 10k 中文段落 | 2.1k | 3.8 ms | 6.8 MB |
| 100k 英文段落 | 1.4k | 6.2 ms | 41 MB |

## 排序与过滤

```sql
SELECT id, title
FROM articles
WHERE MATCH(content, 'vector database')
  AND tags LIKE '%Rust%'
ORDER BY published_at DESC
LIMIT 10;
```

## 调优建议

- **召回优先**：增大 n-gram、保留停用词
- **性能优先**：启用前缀压缩、定期 `VACUUM TEXT INDEX`
- **多语言**：通过 `tokenizers` 模块注册自定义分词器

## 故障排查

| 现象 | 解决方法 |
|------|----------|
| 查询无结果 | 确认 `MATCH` 字段与索引字段一致；检查停用词表 |
| 索引体积过大 | 启用分段压缩，或对长文本预截断 |
| 分词错误 | 切换 Tokenizer，或更新插件词典 |

---

- 上一篇：[08 向量索引](./08-vector-index.md)
- 下一篇：[10 空间索引](./10-spatial-index.md)
