# 混合检索与 Arrow/pandas 互操作（0.12 新增）

## 混合检索（BM25 + 向量 RRF）

同一查询里融合全文（BM25）与向量（KNN）两路结果，按 **Reciprocal Rank
Fusion** 排序——免跨引擎分数标定，两路都命中的文档自然浮顶。

```python
import motedb

db = motedb.Database("app.mote", preset="general")
db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT, emb VECTOR(384))")
db.execute("CREATE TEXT INDEX docs_body ON docs (body)")
db.execute("CREATE VECTOR INDEX docs_emb ON docs (emb)")

hits = db.hybrid_search(
    "docs_body", "rust database",        # 文本索引 + 查询词
    "docs_emb", [0.1, 0.2, 0.3, 0.4],    # 向量索引 + 查询向量（384 维）
    k=10,          # 返回条数
    rrf_k=60,      # RRF 平滑常数（默认 60，越大越按名次平权）
    fetch_mult=4,  # 每路候选深度 = k × fetch_mult
)
for h in hits:
    # 行数据 + 三个附加键：
    print(h["id"], h["__rrf__"], h["__bm25__"], h["__distance__"])
```

语义：`__bm25__` / `__distance__` 为 None 表示该路列表未命中此文档；
两路都命中的文档融合分最高。行数据按融合分降序返回。

## Arrow / pandas 互操作

```python
tbl = db.query_arrow("SELECT id, body, emb FROM docs")   # pyarrow.Table
df  = db.query_pandas("SELECT id, v FROM t WHERE v > 10")  # pandas.DataFrame
```

- 标量列走 numpy 直通（int64 / float64 / bool / str）；
- `VECTOR(n)` 列输出为 Arrow 规范的 `fixed_size_list<float32>[n]`；
- `query_pandas` 经 Arrow 转换，pyarrow / pandas 为可选依赖（缺失时给
  出安装指引）。

## 已知限制（Known Limitations）

| 项 | 说明 |
|---|---|
| `geom` 保留字 | `geom` 不能作列名（`GEOMETRY` 类型别名冲突），换名即可 |
| `LIMIT ?` 与 SetOp | `UNION/EXCEPT/INTERSECT` 结果集不支持参数化 LIMIT/OFFSET（普通 SELECT 支持） |
| 事务内 MATCH 读己之写 | 事务内未提交 INSERT 对 `MATCH` 全文谓词不可见（提交后可见） |
| 时序 `ts INT` | 支持但推荐 `TIMESTAMP`（Gorilla 编码 + zone map 剪枝全开） |
