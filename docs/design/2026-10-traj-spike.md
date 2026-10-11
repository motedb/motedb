# W5 轨迹索引 Spike 备忘录 — 决议: SAX 倒排 NO-GO,PAA+DTW 两段 GO

> 2026-10-11 · `examples/traj_spike.rs`(可复跑:`cargo run --release --example traj_spike`)
> 预注册门(roadmap §5):recall@10 ≥ 0.90(structured 查询)· p99 ≤ 500ms · 索引 ≤ 8%

## 实验设置

- 语料 110k × 64 点:90k z-norm 随机游走 + 20k 结构化形状(10 个运动
  族 × 2k,频率按族、相位/幅值/噪声按成员抖动 —— 机器人"动作基元"模型)
- 查询 100:50 结构化(同族新噪声/相位)+ 50 随机游走(真值病态,只报不判)
- 真值:并行暴力 DTW top-10(8 线程 ~11.6s)
- 策略 A(计划方案):z-norm → PAA(8) → SAX 词(8 符号/字母表 8)→ 倒排桶;
  查询 = Hamming≤2 邻域(1+56+1372 词)→ DTW 精排(堆满后 early-abandon)
- 策略 B(预注册降级):PAA 欧氏 top-500 → DTW 精排(无倒排索引)

## 结果(三次运行稳定)

| 指标 | A SAX→DTW | B PAA→DTW | 门 |
|---|---|---|---|
| structured recall@10 | **0.762** | **1.000** | ≥ 0.90 |
| p50 / p99 延迟 | 450µs / 3.4ms | 3.3ms / 25.8ms | ≤ 500ms |
| 索引开销 | 4.7%(1.32MB) | **~1.1%**(仅 PAA 列 32B/条,无倒排) | ≤ 8% |
| 平均候选数 | 206 | 500 | — |

## 结论

1. **SAX 词倒排 NO-GO**:8 符号词分辨率不足 —— 相位移动使 DTW 相似的
   轨迹在 ≥3/8 个 PAA 段上跨越量化边界,Hamming≤2 邻域系统性漏召
   (0.762 < 0.90)。加长词/放大邻域会指数级放大候选与实现复杂度,
   而降级路线已满分,无迭代价值。spike 一下午击毙了复杂设计 —— 目标达成。
2. **PAA+DTW 两段 GO**:structured recall 1.000、p99 25.8ms(门余量 19×)、
   存储仅 PAA 列(~1%)。且架构上更契合嵌入式定位:**无独立索引结构需要
   维护** —— 与向量检索"预计算紧凑表示 + 精确扫描 + 精排"是同一模式,
   复用 episode close 时的异步预计算管线。

## v0.14 产品化含义(修订 §5-§6)

- **不建** `TRAJECTORY INDEX` 倒排;`traj_sim(trajectory, ?)` ORDER BY 实现
  为:episode close 时预计算 PAA(8) 列(f32×8)→ 查询时欧氏 top-500
  (可 morsel 并行,110k 单线程 3.3ms;10M 级再并行/IVF-flat)→ DTW 精排
- 原始 z-norm 序列已在列存中(精排零额外存储);PAA 列为唯一派生数据
- `CREATE TRAJECTORY INDEX` 语法保留为元数据标记(声明该列参与
  traj_sim),不再是物理结构 —— 对用户心智模型不变
- 短名单 500 有余量(B 在 250 时预计仍 ≥0.95,产品化时按 Perf Gate 调参)
