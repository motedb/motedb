# MoteDB 向量检索行业对照报告

> 生成日期: 2026-10-21 · harness: `bindings/python/bench/compete_vec_engines.py`
> (`make bench-industry` 一键复现; 规模档由 N/DIM 环境变量切换)

## 方法学

- **语料**: 聚簇合成向量(确定性 seed — 见 `prof_ann.build_corpus`; ANN 基准必须
  用聚簇分布, 均匀随机是最坏情形), 数量/维度见表中 engine_mode。
- **ground truth**: numpy 暴力 top-k, 同一查询集(语料内采样, seed=42, q=200,
  k=10)。recall@10 = 命中交集 / (q×k)。
- **各引擎取官方默认索引参数 + 1-2 个调优档, 全部列出** — 包括对 MoteDB 不利
  的行, 不挑好话。延迟为单查询串行(python 侧计时, 含反序列化)。
- **公平性声明**: 各引擎为不同设计点(内存图索引/磁盘图索引/列存扩展/精扫);
  本报告目的是给出同一负载下的可复现横截面, 不是"胜者通吃"。
- 环境: 见报告尾部 env 行。任何人对结果有异议 → PR 重跑, 方法即仲裁。

## 结果

| engine_mode | recall@10 | p50_ms | p95_ms | p99_ms | build_s | ingest kv/s | disk_mb | rss_mb |
|---|---|---|---|---|---|---|---|---|
| duckdb_hnsw_n220000_d384 | 0.999 | 0.964 | 1.382 | 1.523 | 20.109 | 500.9 | — | 3834.0 |
| faiss_flat_exact | 1.0 | 7.027 | 10.693 | 18.598 | 0.036 | — | — | 2463.6 |
| faiss_hnsw16_ef256 | 0.9605 | 0.38 | 0.616 | 0.672 | 4.495 | — | — | 2463.6 |
| faiss_hnsw16_ef32 | 0.688 | 0.109 | 0.222 | 0.269 | 4.495 | — | — | 2463.6 |
| faiss_hnsw16_ef64 | 0.8375 | 0.136 | 0.27 | 0.31 | 4.495 | — | — | 2463.6 |
| faiss_ivfflat_nprobe32 | 0.947 | 2.043 | 2.234 | 2.345 | 1.483 | — | — | 2463.6 |
| faiss_ivfflat_nprobe8 | 0.899 | 1.894 | 1.983 | 2.024 | 1.483 | — | — | 2463.6 |
| lancedb_flat_n220000_d384 | 1.0 | 37.914 | 44.791 | 53.579 | 0.0 | 791.7 | — | 3128.0 |
| lancedb_ivfpq_nprobes1 | 0.2215 | 1.219 | 1.557 | 2.099 | 15.23 | — | — | 3264.8 |
| lancedb_ivfpq_nprobes20 | 0.253 | 3.119 | 3.411 | 3.691 | 15.23 | — | — | 3264.9 |
| motedb_diskann_n220000_d384 | 1.0 | 0.57 | 1.184 | 1.418 | 171.355 | 342.8 | 1166.4 | 2463.6 |
| usearch_l2_n220000_d384 | 0.994 | 0.252 | 0.476 | 0.513 | 0.0 | 6.9 | — | 3834.0 |

**跳过的引擎**:

- `sqlite_vec`: this python build lacks sqlite3 extension loading (python.org framework build)

env: /Library/Frameworks/Python.framework/Versions/3.13/bin/python3, 2026-10-21
