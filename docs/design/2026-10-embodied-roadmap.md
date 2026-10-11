# 技术方案:具身数据工作流 — v0.13「入场」与 v0.14「品类定义」

> 状态:**Phase 1 完成**(W1a 文档对齐 ✅、W1b BLOB ✅ 含词法平方级修复、
> W2-3 MCAP 核心/CLI/Python 钩子/重放幂等 ✅、W4 RRF+bm25+snippet ✅ 含
> 非 Table FROM 执行器修复)。顺延项:MCAP 断点续导清单表、BLOB 标量函数
> 求值/ORDER BY 输出缺口。下一阶段:W5 轨迹索引 spike(go/no-go)。
> 范围:Phase 1 = v0.13(MCAP/BLOB/RRF/FTS 补齐/文档对齐),Phase 2 = v0.14(Episode 模型/轨迹索引/数据集版本化/跨模态融合)
> 原则:所有设计锚定现有代码结构;每个特性附验收判据;先验证后开工(轨迹索引)

---

## 0. 总览与依赖关系

```
W1  文档对齐 ─────────────────────────────┐ (独立,先行发布)
W1  BLOB 类型全链路 ──┐                   │
W2  MCAP 导入核心 ◄──┤                   │
W3  MCAP Python/恢复/向量钩子 ◄──┘        │
W4  RRF 融合 + bm25()/snippet() ──────────┼──► v0.13 发布
                                          │
W5  轨迹索引技术验证(SAX spike, go/no-go) │
W6  Episode 数据模型 ─────────────────────┤
W7  数据集版本化 + LeRobot 导出雏形        │
W8  轨迹索引产品化 + 多模态融合查询 ───────┴──► v0.14 发布
```

不做清单(已定):分布式/集群、DuckDB 级 SQL 全量对齐、对象存储直查、DECIMAL(顺延)。

---

## Phase 1 — v0.13

### 1. BLOB 类型(地基,先行)

**目标**:原始传感器消息、图像帧、序列化 payload 有家可归;为 MCAP 导入铺路。

**类型系统**(`src/types/table.rs` / `src/types/mod.rs`)
- `ColumnType` 增 `Blob`(第 8 个成员,现有:Timestamp/Text/Tensor/Integer/Float/Boolean/Spatial)
- `Value::Blob(Vec<u8>)`;Python 绑定 `bytes`/`bytearray` 直通(参照 Tensor 的 `Vec<f32>` 转换层)
- 大小上限默认 **16MB**/值(`DBConfig::max_blob_bytes`,超限报 `InvalidData`,防单行打爆内存)

**编码链**
- `row_format::encode`(src/storage/row_format.rs:908):镜像 Text 的 `[len:u32][bytes]`;null_bitmap 位照常
- 列存段:新增 **`BinarySegment`**(src/storage/lsm/columnar.rs,与 `TextSegment`(859 行)同构:null bitmap + offsets + raw bytes,**关闭** `trust_utf8` 路径与 UTF-8 校验);页级 zstd 复用
- 比较语义:v1 仅 `IS NULL` / 等值(字节比较);不可索引、不参与 ORDER BY(报清晰错误)

**SQL 语法**:十六进制字面量 `X'0AFF00'`(SQLite 兼容);`CAST(x AS BLOB)` 暂缓

**涉及文件**:`types/table.rs`、`types/mod.rs`、`storage/row_format.rs`、`storage/lsm/columnar.rs`(read_binary/get_str 镜像)、`storage/col_segment/store.rs`(列编码分派)、`sql/parser.rs`(X'' 字面量)、`sql/evaluator.rs`(等值比较分支)、`bindings/python/src/lib.rs`(bytes 参数/取回)、`fetch_arrays`(BLOB 列 → Python bytes list,不进 numpy)

**测试**
- 单元:16MB 边界、空 blob、NULL、往返无损(含非法 UTF-8 字节)
- 集成:10 万行混合列(INT/TEXT/BLOB VECTOR)重开一致性;kill -9 后 blob 完整
- 差分:对 SQLite `BLOB X''` 等值/计数对拍(其原生支持,零成本入 `test_differential_correctness`)

**验收**:`INSERT INTO t VALUES (1, X'00FF')` → 重开 → 取回字节相同;`fetch_arrays` 取回为 bytes。

### 2. MCAP 导入(矛头)

**目标**:`motedb-cli import mcap demo.mote bag.mcap` 之后,机器人数据可 SQL 查询/可向量检索。

**依赖**:官方 `mcap` crate(foxglove/mcap,Rust,MIT)——只读 API;经 cargo-deny 供应链检查后引入(`mcap` 本体依赖少:scroll/enum-primitive 等)。

**核心设计决策:Rust 侧 schema 无关,Python 侧解码回调**

MCAP 消息体是 ROS2 CDR / protobuf 等编码,v1 不在引擎里做 CDR 解码(范围爆炸)。边界:
- **Rust 核心按 channel 扫描**,每条消息落一行:raw = BLOB(原始消息字节),元数据列解好
- **用户解码钩子**(仅 Python):`import_mcap(path, decode={topic: callable})`,callable(raw: bytes, schema: str) → dict/None,dict 的列并入该行(可产出 emb 列 → 导入结束触发向量索引批量构建,复用现有 batch_build 管线)
- 无钩子 = 纯 raw 落库,零依赖可用;有钩子 = 类型化列 + 向量

**表布局**(默认,前缀可配)
```
topic__<sanitized>(                    -- 每 topic 一表,列:
  _uid INTEGER PRIMARY KEY,            -- (channel_id << 40) | sequence,天然去重
  log_time TIMESTAMP,                  -- 自动建 TIMESTAMP 索引
  publish_time TIMESTAMP,
  data BLOB,                           -- 原始消息
  <解码钩子产出的类型化列>             -- 可含 VECTOR(n)
)
_mcap_imports(                         -- 导入清单(断点续导)
  file_path, file_hash, topic, last_log_time, rows, finished_at
)
```

**断点续导**:按 `_mcap_imports.last_log_time` 跳过已导入区间;kill -9 中断后重跑同命令自动续(`_uid` 去重兜底,INSERT OR IGNORE 语义)。

**增量/内存纪律**:流式读 chunk → 每 5k 行一批 `insert_arrays` 管线(197K rows/s 档);RSS 增量 ≤100MB 与现有资源档一致。

**接口**
- CLI:`motedb-cli import mcap <db> <file> [--include-topic RE] [--exclude-topic RE] [--prefix p]`
- Python:`db.import_mcap(path, *, include=None, exclude=None, prefix="topic", decode=None) -> {"topics": {name: rows}, "skipped": ...}`
- 附件/元数据:`_mcap_attachments`(name, media_type, BLOB)与 `_mcap_channels` 元数据表

**涉及文件**:`Cargo.toml`(+mcap,建议 `default-features=false` 收窄)、新 `src/mcap_import.rs`(纯函数:file → 批回调,便于测试)、`src/bin/motedb-cli.rs`(子命令)、`bindings/python/src/lib.rs`(import_mcap + decode 回调桥)

**测试**
- 合成回环:用 mcap crate 的 **write API** 生成含 3 topic × 2 schema 的测试 bag(入 `tests/`,CI 可跑,无需真机数据)
- kill -9 注入:导入中途杀 → 重跑 → 计数与全量一致(复用 `test_crash_recovery.py` 的子进程模式)
- 真实样例:仓库外 `examples/` 提供 Foxglove 示例 bag 的可选下载脚本(README 链接,不进 CI)

**验收**:合成 bag 导入后,以下两条 SQL 成立——
`SELECT COUNT(*) FROM topic__joint_states WHERE log_time BETWEEN a AND b`;
带 decode 钩子导入 joint_states → `ORDER BY emb <-> ? LIMIT 10` 返回精确 top-10。

### 3. RRF 混合检索 + bm25()/snippet()

**SQL 设计(表函数式,复用已有派生表设施)**

```sql
SELECT id, title FROM RRF(
  (SELECT id FROM docs WHERE MATCH(body, 'grasp failure') ORDER BY bm25(body) LIMIT 100),
  (SELECT id FROM docs ORDER BY emb <-> ? LIMIT 100),
  k = 10, rrk = 60, w1 = 0.5, w2 = 0.5
);
```
- `FROM RRF(t1, t2, ...)`(2..4 路):每路子查询必须恰好投影 1 列 id;`k/rrk/w*` 命名参数可省略(默认 10/60/均权)
- 评分:`Σ wᵢ / (rrk + rankᵢ)`;并列按 id 升序 tie-break(**确定性**,差分测试可对拍)
- 子查询走现有 `FROM (SELECT …)` 执行路径(已有),融合层只做小表缝合(≤4×100 行物化,微秒级)

**parser**:新增 `TableRef::TableFn { name, subqueries, named_args }` 产生式(独立分支,避免与现有 `parse_table_ref` 冲突);只认 `rrf`(白名单,未知名报错)

**executor**:新 `execute_rrf_table_fn`(executor/mod.rs,镜像 `execute_from_subquery_statement` 的两步式:先逐路物化,再融合);`LIMIT k` 下推各路(每路取 `min(limit, k*10)` 由 RRF 实现注入)

**bm25(col)**(evaluator.rs 派发表注册):仅当同 SELECT 的 WHERE 含该列的 MATCH 时合法(否则报错);实现走 FTS ranked 路径已算好的分——在 FTS 快路径(`executor` 21083 区域)把 score 附加为隐藏列(复用窗口函数 `__winval_k` marker 的既有机制)

**snippet(col, 'query', width=64)**:取回行文本后纯函数实现(高亮 `<b>` 包裹 + 省略号),不做位置索引依赖(phrase positions 已存,可后续升级)

**测试**
- 差分:RRF(两路) vs Python 手工融合参考实现,1 万行语料 × 200 查询,精确一致
- 与单路对比:混合 recall@10 ≥ max(单路) - 5%(语料构造一半仅词匹配、一半仅语义匹配)
- 并发:RRF 查询 × 写 churn(进 `test_concurrent_sql_features`)

**验收**:README「混合检索」示例一条 SQL 可复制运行;差分零发散。

### 4. 文档对齐(半天,W1 先行)

README `SQL Support` 段落已过时(写着不支持 WITH RECURSIVE/窗口函数,0.12 均已实现)——刷新为自动生成式清单(从 `test_fuzz_sql_combinations` 模板表反查,防再腐化);同步 docs/03/13/14 三篇;CHANGELOG 补「能力勘误」条目。

---

## Phase 2 — v0.14

### 5. 轨迹相似度索引(先技术验证,W5)

**动机**:`ORDER BY traj_sim(joint_traj, ?) LIMIT k` ——无主流数据库具备的原语(王炸,可发表、可防守)。

**W5 Spike(一周,go/no-go 门)**
- `examples/traj_spike.rs`:纯内存原型,PAA 64 点 → SAX 8×8 词 → 桶 → DTW 验证
- 判据(10 万条合成轨迹 + 1 万真实形状混合):recall@10 ≥ 0.90(对暴力 DTW)、p99 ≤ 500ms、索引 ≤ 原始数据 8%
- **不过关的降级路线**:PAA 粗筛(向量化欧氏)top-500 → DTW 精排(无词索引,仍比暴力快 ~50×);再不行则该特性移出 v0.14

**产品化设计(spike 通过后)**
- 索引单元 = **episode 内某列重采样后的序列**(依赖 §6);`CREATE TRAJECTORY INDEX ti ON teleop(joint)`
- 存储:SAX 词 → episode_id 列表,复用 `GenericBTree<u64>`(与 FTS posting 同构);查询词邻域枚举(NUMOFSAX=2)
- 查询:`ORDER BY traj_sim(joint, '[...]') LIMIT k`(executor 走向量 KNN 同款快路径,23619 区域镜像)
- 增量:episode close 时异步建(SAX 词插入);写路径零阻塞

### 6. Episode 数据模型

**DDL**:
```sql
CREATE EPISODE TABLE teleop (
  session TEXT, ts TIMESTAMP,
  joint VECTOR(7), gripper FLOAT, frame BLOB, note TEXT
) EPISODE BY (session) GAP '5s';
```
- 存储 = **普通表**(零新存储格式),叠加 episode 注册表:`_episodes(id, session, t0, t1, rows, closed)`
- `GAP '5s'`:时间间隔超阈自动开新 episode(写入路径在 episode 注册表上 CAS 续尾,不做热点行);`CLOSE EPISODE teleop WHERE session=?` 显式收口
- 查询原语:`FROM EPISODES(teleop)` 一行一 episode(聚合 + `traj(joint)` 嵌套列 → arrow/numpy list);`EPISODES(teleop, WHERE joint IS NOT NULL)` 内过滤

**对齐查询**(v0.14 最小版):`AS OF JOIN`(每行找另一表 ts 前最近行)——机器人数据"帧-关节状态对齐"的刚需原语,执行器按 ts 归并(两路已按 TIMESTAMP 索引序取流,内存 O(1))

### 7. 数据集版本化 + LeRobot 导出

- `COMMIT DATASET 'fixpos-v3' NOTE '...'`:快照 = 当前各表 `segments_snapshot()` 的段清单 + 索引文件哈希,落 `_dataset_commits`(**段不可变 ⇒ 快照近乎免费**——这是选型红利)
- GC 保护:compaction 的 `pending_gc`/`sync_manifest` 删除段前查引用集(被任一 commit 引用的段不删,物理改写延后到无人引用)
- `CHECKOUT`(v0.14 只读视图):按 commit 的段清单过滤打开;写需显式 `FORK`
- `db.export_dataset('fixpos-v3', path)`:每 episode 一 parquet + metadata.json(LeRobot 目录形状);parquet 依赖 `arrow` 已在树内,加 `parquet` crate feature-gated

### 8. 跨模态融合查询

- RRF 表函数扩至 N 路 + `rerank = 'name'`(Python `db.register_rerank('name', fn)`,引擎侧召回 top-200 后回调重排)
- 演示性目标 SQL(README 头图):
```sql
SELECT * FROM RRF(
  (SELECT ep_id FROM EPISODES(teleop) WHERE MATCH(note, 'grasp failure')),
  (SELECT ep_id FROM EPISODES(teleop) ORDER BY traj_sim(joint, :demo)),
  (SELECT ep_id FROM EPISODES(teleop) ORDER BY frame_emb <-> :query_emb),
  k = 20, rerank = 'cross_enc');
```

---

## 测试与质量门(新增部分)

| 层 | 手段 |
|---|---|
| 解析 | 新语法进 `fuzz_sql_parser`(ASAN 每日已跑) |
| 语义 | RRF 差分、BLOB 对 SQLite 差分、ASOF JOIN vs pandas merge_asof 对拍 |
| 耐久 | MCAP 导入中断恢复、dataset commit 后 kill -9(断言 checkout 可开) |
| 并发 | RRF/episode 写入并发混合负载(进现有套件) |
| 性能 | Perf Gate 新形状:BLOB 扫描、RRF 融合(预算 = 各单路和 ×1.5)、traj_sim(对暴力比值) |
| 资源 | MCAP 导入全程 RSS ≤ 100MB 快照断言 |

## 里程碑与发布

| 周 | 内容 | 出口判据 |
|---|---|---|
| W1 | 文档对齐 + BLOB 全链路 | 差分绿;README 勘误上线 |
| W2-3 | MCAP(核心→Python→恢复) | 合成 bag 回环 + kill -9 续导 |
| W4 | RRF + bm25/snippet | 差分零发散 → **v0.13 发布** |
| W5 | 轨迹 spike | go/no-go 备忘录入 docs/design/ |
| W6-8 | Episode + 版本化 + 轨迹产品化 + 融合 | EmbodiedBench 雏形数据 → **v0.14 发布** |

## 风险登记

| 风险 | 缓解 |
|---|---|
| mcap crate 供应链/MSRV | cargo-deny 门禁先行;不合适则自实现 MCAP 读(chunk 索引格式公开且简单,~1k 行兜底) |
| CDR 解码范围爆炸 | v1 用 Python 回调做边界,引擎不碰 ROS 消息语义 |
| SAX recall 不达标 | spike 周门禁 + PAA 降级路线已定义 |
| RRF 语法与现有 parser 冲突 | 独立 TableFn 产生式 + fuzz 覆盖 |
| 版本化 GC 与 compaction 交互 | 引用集检查进 `sync_manifest`,加专项并发测试(引用段不被删) |
| episode 元数据写热点 | 注册表 CAS + 内存缓存,批量导入路径只在段 finalize 时触碰 |
| 16MB blob 与行宽放大 | 列存段独立落盘阈值(>1MB 的 blob 段单独文件,避免 offsets 数组膨胀)——首版可先不做,Perf Gate 观察 |
