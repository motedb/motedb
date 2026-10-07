# MoteDB v0.12.0

多模态产品定位闭环版：**混合检索**（BM25+向量 RRF 融合）与 **Python
Arrow/pandas 互操作**落地；写侧与读侧一波性能战役（时序 top-k 680×、1M
TopK 追平 DuckDB、ANN 尾部 p99 30→1.5ms、scan-UPDATE/DELETE 反超
SQLite）；四个差分实抓的正确性修复；编译告警清零、全量 239 bin /
3,955 例测试绿。

## ⚠️ 破坏性变更（升级注意）

- **MATCH 多词默认语义 OR → AND**（对齐 SQLite FTS5）：此前
  `MATCH 'a b'` 按 OR 解释，现按 AND（两词都命中）。需要 OR 的写法用
  `MATCH 'a OR b'`。FTS5 兼容语义，旧行为属意外宽松。

## 新能力

- **混合检索 hybrid_search**（Rust + Python）：同一查询里 BM25 全文列表
  与向量 KNN 列表按 **Reciprocal Rank Fusion** 融合（
  `rrf(d) = Σ 1/(rrf_k + rank)`，rrf_k 默认 60）。RRF 免两引擎分数
  标定（行业标准）；候选深度 k×fetch_mult，两列表都命中的文档自然
  浮顶。返回行带 `__rrf__` / `__bm25__` / `__distance__` 三键。
  Python: `db.hybrid_search(text_index, text_query, vector_index,
  query_vector, k=10)`
- **Arrow / pandas 互操作**：`query_arrow(sql, params)` →
  `pyarrow.Table`（列式 numpy 直通；VECTOR 列映射 Arrow 规范的
  `fixed_size_list<float32>[N]`）；`query_pandas` 经 Arrow 转
  DataFrame。原生扩展改名 `motedb._native`，新增真 Python 包装层
  （`import motedb` 面不变，pyarrow/pandas 为可选依赖）
- **`LIMIT ?` / `OFFSET ?` 参数化**：匿名 `?` 与 `?N` 均支持，
  负数/非整数/未绑定给清晰 InvalidArgument（UNION/EXCEPT/INTERSECT
  结果集上明确报不支持）

## 性能

- **时序 `ORDER BY ts LIMIT k`（ts INT 列）: 1.2s → 1.76ms（680×）**
  — INT 时间列的 top-k 全链路三重失效根治（解码点只认 Gorilla
  时间戳 / 段元数据恒 (0,0) 被 zone gate 全剪 / 未提交行不可见）；
  1M 行实测快于 DuckDB 全扫
- **1M TopK: 9.2 → 1.5ms（6.2×，追平 DuckDB）** — 128K morsel 并行
  top-k + 列缓存 Arc 化（缓存命中从全列 memcpy 变指针递增）
- **ANN 尾部: p99 30ms → 1.5ms** — 根因是打开后前 30 个查询的冷缺页
  斜坡（首查询 337ms）；`madvise(WILLNEED)` 预热，首查询 1.9ms；
  同 recall 档 p50 对 FAISS Flat 快 9×
- **scan-UPDATE 谓词下推: 9.3K → 50K rows/s（5.4×）**；scan-DELETE
  镜像同达 112.6K rows/s，与 SQLite（110K）持平；periodic 耐久档
  115.2K rows/s **反超 SQLite**。写侧配套：谓词字节级预检、去重集
  FxHash、批量 WAL 单次刷（硬崩溃矩阵验证 782/782 行存活）
- **FTS 构建零 per-token 分配**: CREATE TEXT INDEX 0.72 → 0.48s
  （TokenizedText 借用化分词，自 W3 前累计 1.72×）；刚建索引的
  派生视图缓存修复 ~100µs/查询税（105 → 9.4µs）
- **点查投影下推: 17µs → 5.4µs**（超 SQLite）；乱序 INSERT 33×；
  存储大段读去锁（pread）；压缩流式 k 路归并
- **时序/空间 SOTA 对照建档**（1M 行时序 + 500K 3D 点 × 3 引擎）：
  范围聚合 p50 0.004ms（SQLite/DuckDB 的 100-1000×）；bbox WITHIN
  0.001ms（400-1300×）；KNN10 0.002ms

## 正确性修复（差分实抓）

- **过滤向量检索迭代加深**：`WHERE flag=1 ORDER BY emb <-> ? LIMIT k`
  高选择性谓词不再漏结果（旧代码过滤在 top-k 之后，存活数 < k 甚至
  为 0；现候选深度 ×4 迭代加深直到够数）
- **事务聚合两个漏行 bug**：(1) 纯 INSERT 事务里带 WHERE 的 COUNT/SUM
  漏未提交行（路由门控只查 pending updates/deletes）；(2) overlay
  聚合路径完全忽略 WHERE（返回未过滤总数）
- **事务内 MATCH 读己之写**：FTS 快路径从索引应答，未提交 INSERT/
  DELETE/UPDATE 此前不可见/不隐藏/不重算；现按缓冲写折叠（七形状
  回归含 ROLLBACK 精确恢复）
- FTS shard 发现跨词污染修复（重开丢词根因）；FTS 缓存四处变更点
  补失效；参数化 MATCH + 事务 DELETE 快路径对抗验证修复

## 质量基建

- 编译告警清零：rustc 38 → 0，clippy lib 53 → 0；删除死代码约 200
  行（`CompiledWhere::Not`、`VecAcc::fold_all` 等）；顺手修复 2 个
  存量 clippy error（hash-join 恒真条件、`never_loop`）
- 全量 release 套件 239 个测试二进制 / 3,955 例通过；perf gate
  （机器无关比值预算）全绿
- 差分回归新增 12+ 例（topk 并行内核对照全排序、scan-UPDATE/DELETE
  谓词下推对照通用路径、事务聚合 RYW、过滤向量检索等）

## Known Limitations

- `geom` 为保留字，不能作列名（`GEOMETRY` 类型别名冲突）
- `LIMIT ?` 参数化不支持 UNION/EXCEPT/INTERSECT 结果集（普通 SELECT
  支持）
- 事务内未提交 INSERT 对 `MATCH` 全文谓词不可见（读己之写覆盖到
  扫描/聚合/点查，FTS 快路径待补）
- 时序表推荐 `ts TIMESTAMP`；`ts INT` 功能完整但 Gorilla/zone-map
  优化需要 0.12.0 之后的段重写才能完全生效
- LATEST BY 大表（>1M 行）性能待优化（段级归并，0.12.x 计划）
- executemany 与 SQLite 差 2.4-2.7×（Python 逐行过桥开销，批量 API
  可达 189-457K rows/s）
