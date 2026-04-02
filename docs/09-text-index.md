# Text Index

A full-text search engine based on a custom BM25 implementation with pluggable tokenizers, supporting Chinese, English, and n-gram tokens.

## Feature Overview

- BM25 ranking + TF/IDF statistics
- Tokenizer plugins: Whitespace, N-Gram, Jieba (extensible via the `tokenizers` module)
- Syntax: `MATCH(column, 'query')` + `BM25_SCORE`
- Supports combination with SQL conditions (WHERE, ORDER BY, LIMIT)

## Table Creation and Indexing

```sql
CREATE TABLE articles (
    id INT,
    title TEXT,
    content TEXT,
    tags TEXT
);

CREATE TEXT INDEX articles_content ON articles(content);
```

## Query Patterns

```sql
SELECT id, title, BM25_SCORE(content, 'rust database') AS score
FROM articles
WHERE MATCH(content, 'rust database')
ORDER BY score DESC
LIMIT 20;
```

API style:

```rust
let hits = db.text_search_ranked("articles_content", "rust database", 20)?;
for (row_id, score) in hits {
    println!("row_id={} score={:.4}", row_id, score);
}
```

## Data Import and Refresh

- Standard `INSERT` / `batch_insert_map()` works; the full-text index listens for WAL updates and merges incrementally
- For large-scale imports:
  1. `batch_insert_map()`
  2. `db.execute("REBUILD TEXT INDEX articles_content")`
  3. `db.flush()?`

## Tokenizer Configuration

```sql
CREATE TEXT INDEX articles_content
ON articles(content)
USING TOKENIZER ngram(2);
```

Available options:
- `whitespace` (default)
- `ngram(n)`
- `jieba` (requires plugin enabled)

## Performance Metrics

| Corpus | QPS | Avg Latency | Index Size |
|--------|-----|-------------|------------|
| 10k Chinese paragraphs | 2.1k | 3.8 ms | 6.8 MB |
| 100k English paragraphs | 1.4k | 6.2 ms | 41 MB |

## Sorting and Filtering

```sql
SELECT id, title
FROM articles
WHERE MATCH(content, 'vector database')
  AND tags LIKE '%Rust%'
ORDER BY published_at DESC
LIMIT 10;
```

## Tuning Recommendations

- **Prioritize recall**: increase n-gram size, keep stop words
- **Prioritize performance**: enable prefix compression, periodically run `VACUUM TEXT INDEX`
- **Multi-language**: register custom tokenizers via the `tokenizers` module

## Troubleshooting

| Symptom | Solution |
|---------|----------|
| No query results | Verify that the `MATCH` field matches the indexed field; check the stop word list |
| Index size too large | Enable segment compression, or pre-truncate long texts |
| Incorrect tokenization | Switch tokenizer, or update the plugin dictionary |

---

- Previous: [08 Vector Index](./08-vector-index.md)
- Next: [10 Spatial Index](./10-spatial-index.md)
