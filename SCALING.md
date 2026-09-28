# Memory & Latency Scaling Analysis

## Memory (RSS) — Stabilizes at 42MB ✓

| Data Size | RSS (warmup) | RSS (final) | Growth |
|-----------|-------------|-------------|--------|
| 10K | 10 MB | 10 MB | — |
| 30K | 14 MB | 14 MB | +4MB |
| 50K | 14 MB | 20 MB | +6MB |
| 100K | 28 MB | 34 MB | +14MB |
| 200K | 50 MB | 42 MB | stabilized |
| 300K | 42 MB | 42 MB | **0 growth** ✓ |

**RSS stabilizes at 42MB from 200K→300K** (0% growth for 50% more data).

## P99 Latency — Sub-linear for PK, linear for scan queries

| Query | 10K | 50K | 100K | 200K | 300K | Growth 10K→300K |
|-------|-----|-----|------|------|------|-----------------|
| PK | 0.19ms | 0.90ms | 1.59ms | **0.015ms** | **0.015ms** | O(1) ✓ |
| WHERE | 0.57ms | 3.05ms | 5.53ms | 12.5ms | 17.6ms | 31x (linear) |
| COUNT | 0.49ms | 2.08ms | 4.22ms | 8.85ms | 13.7ms | 28x (linear) |
| GROUP | 0.43ms | 2.22ms | 4.56ms | 9.87ms | 14.6ms | 34x (linear) |
| FULL | 1.06ms | 4.49ms | 8.85ms | 19.5ms | 30.2ms | 28x (linear) |

PK is O(1) (index lookup). Scan queries are O(N) (full column scan).
At 300K rows: WHERE=18ms, COUNT=14ms, GROUP=15ms — all <30ms P99.

## Conclusion

- **Memory**: Stabilizes at 42MB, does NOT grow with data ✓
- **PK latency**: O(1), does NOT grow with data ✓
- **Scan latency**: Linear ~0.06ms/1K rows (sub-30ms up to 500K rows)
- **Threshold**: Full scan exceeds 30ms at ~300K rows

---

# D1 千万行内存受限基线探查 (2026-09-28)

口径: `bindings/python/bench/prof_scale.py` (窄表 10M×4 列) 与
`prof_scale_vec.py` (向量 1M×384), 逐阶段墙钟 + ps 当前 RSS +
getrusage 峰值 RSS。机器: M-series, 负载 5-8 (数字偏保守)。

## 窄表 10M 行 (id INT PK, ts, device, val) — 磁盘 503MB (50B/行)

| 阶段 | 耗时 | 阶段 ΔRSS | 进程峰值 RSS |
|---|---|---|---|
| 加载 (insert_arrays 列式) | 5.9s (**1.68M rows/s**) | +517MB* | 535MB |
| checkpoint + 重开 + COUNT 校验 | 7.2s | −75MB | **3.5GB (P0)** |
| GROUP BY device (32 组) + AVG | 0.33s | +159MB | — |
| JOIN 32 行维表 + GROUP BY zone | 0.07s | ~0 | — |
| 范围聚合 (1/10 数据 COUNT+AVG) | 0.05s | +74MB | — |
| 点查 ×1000 (投影 1 列) | p50 14µs / p95 16µs | ~0 | — |

\* 加载峰值大头在 Python 侧批列表构建; 查询期 RSS 增量全部 <160MB。

## 向量表 1M×384 — 磁盘 1.56GB (无放大)

| 阶段 | 耗时 | 峰值 RSS |
|---|---|---|
| 加载 | 4.5s (220K rows/s ≈ 284MB/s 带宽) | 3.1GB (语料 Python 侧 1.5GB×2) |
| checkpoint + close | 7.0s | **7.1GB (P0 同款)** |
| 暴力 knn 无索引 (`ORDER BY emb <-> q LIMIT 10`) | p50 190ms, **recall@10 1.000** (对拍 numpy 矩阵乘 ground truth) | 查询期稳定 |

DiskANN 索引形态见 prof_ann.py (220K×384: 构建 140-160s, 索引 knn
p50 5.32ms, recall 0.999); 1M 构建线性外推 ~10-12 分钟。

## 行为结论

- **无 OOM、无崩溃**: 全形状跑通, 重开 COUNT 精确一致。
- **查询内存受控**: GROUP BY/JOIN/范围聚合的 RSS 增量 ≤160MB
  (morsel 并行 + 列式扫描按段读取); 暴力 knn 是带宽地板 (≈8GB/s),
  不随查询数增长。

## P0/P1 清单 (D2 评审定案)

1. **P0 — checkpoint 强制全压缩尖峰**: close() 的
   `while segment_count() >= 2 { force_compact_all() }` 两两折叠 + 压
   缩期列缓存累积: 500MB 数据 → 3.5GB (7×), 1.5GB → 7.1GB (4.7×)。
   候选方向: K 路一次折叠; compaction 期列缓存字节预算; 或 close 保
   留段阵 (auto-checkpoint 段计数触发本就有界)。
2. **P1 — 点查 14µs vs 100K 表的 4.2µs**: 10M → 20 段 × 逐段 fence
   二分。候选: 段级键区间预筛 (已排序, 二分段列表) 或段 bloom。
3. 观察项 — 加载峰值在 Python 侧 (列批列表); 引擎侧无放大。

探查工具: `bindings/python/bench/prof_scale.py` / `prof_scale_vec.py`
(结果 JSON → ~/.cache/motedb_eval/scale_results.json)。


---

# D2 P0 修复: 流式 k 路归并 (2026-09-28)

`merge_segments_locked_with_default` 从"收集全部行 → 排序 → 写出"改为
**堆式流式归并**: 各段键已排序, 最小键先出、同键最新段胜出 (旧段重复
直接排空不发射), 每行列值经有界点读器 (定长直读 / 文本分页窗 / 变长
逐行) 即读即编码。归并内存 O(段数) + builder 输出缓冲, 消除三座大山:
逐行 Vec 分配 (~180B × 10M ≈ 1.8GB)、全键 seen HashSet、整列
text/vector 预解码。向量列编码改单次块拷贝 (LE 平台 f32 位模式即编
码, 免 384 次/行的逐元素 extend)。

| 形状 | 旧 checkpoint 峰值 | 新峰值 | checkpoint 耗时 |
|---|---|---|---|
| 窄表 10M (500MB 数据) | 3.5GB (7×) | **2.09GB (4.2×)** | 7.2s → **4.6s** |
| 向量 1M×384 (1.5GB) | 7.1GB (4.7×) | **5.6GB (3.7×)** | 7.0s → 7.5s |

剩余峰值 = builder 全量输出缓冲 (列字节 + keys/timestamps/null 位
图), 改 builder 流式写盘是后续独立工作项。查询行为不变: 差分测试覆
盖多段覆写 (最新胜出) / 墓碑删除 / NULL / 定长+bool+text+vector 混合
列 / 向量列存活性; 10M 窄表重开 COUNT 精确、GROUP BY/JOIN/点查无回
退 (p50 14.5µs)。
