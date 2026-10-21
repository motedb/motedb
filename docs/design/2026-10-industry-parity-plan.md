# 技术方案:行业对齐提升计划 — v0.16「装得上」/ v0.17「连得上」/ v1.0-rc「立得住」

> 状态:**Phase 1 进行中**(2026-10-20 开工,Phase 0 已收口)。基于 v0.15.0 代码库通读 + 2026-10
> 行业调研(LanceDB 10B 公开基准 / sqlite-vec / DuckDB-VSS / Rerun 0.32 data layer /
> Foxglove agentic 平台 / LeRobot v3 事实标准)。
> 结论:**内核工程与具身原语已在行业水准之上;差距集中在分发渠道、训练生态
> 互操作、基准公信力三项**。本计划 12 周三阶段,前 6 周全部投向这三项。
> 原则:延续 2026-10-embodied-roadmap 的不做清单;每项附涉及文件与验收判据;
> **已有能力不重做**(SQ8 量化、EXPLAIN、FAISS 对照基准、query_arrow、
> fresh_graph 增量构建均已在库内,本计划只做暴露与扩面)。
>
> **Phase 1 进度(2026-10-21)**:§5 的 Parquet 核心已落地 — `EXPORT TABLE t TO
> 'x.parquet'` / `IMPORT PARQUET 'x' INTO t`(src/export/parquet.rs,arrow-array/
> schema/parquet 59.3.1,MSRV 1.85,feature `parquet` 默认开、default-edge 可关)。
> 全类型往返(INT/FLOAT/BOOL/TEXT/TIMESTAMP/VECTOR→FixedSizeList<f32>/BLOB→Binary)、
> NULL 保留、按列名映射(乱序/子集/缺列补 NULL)、重开持久;**pyarrow 双向互认
> 实测通过**(MoteDB 写 → pyarrow 逐值校验含 fixed_size_list/ts[us]/NULL/blob;
> pyarrow 写 snappy → 导入)。SPATIAL 列明确报错。examples/parquet_interop.rs。
>
> §5 后半 **EXPORT LEROBOT 已落地并互认**(eaf5c06):`EXPORT LEROBOT <table> TO
> '<dir>' [FPS n] [TASKS col]`,表须为 CREATE EPISODE TABLE。规格以官方 writer
> (lerobot 0.6.1, v3.0)参考产物逐文件解剖定标(info.json/stats.json/tasks/
> episodes/data 五件套, shape(1,)→标量列, VECTOR→fixed_size_list<element>,
> 逐维统计)。**官方 LeRobotDataset 加载导出目录实测通过**(episodes/frames/
> tasks 计数、joint 逐值、task join、frame_index 全对)— MCAP→SQL→LeRobot
> 管线打通。examples/lerobot_interop.rs + 测试 ×3。已知边界:全部 episode 落
> file-000(按大小轮转后续)、视频列不支持(v1 范围外)、分位数 nearest-rank 近似。
>
> 顺带发现并修复一个数据丢失级引擎 bug:read_vectors/read_spatial 跳过 NULL 行,
> 消费侧 7 处只前进线性归并在遇到 NULL 后游标冲尾 → **任何含 NULL 向量的表,
> 无 WHERE 全表扫描整列静默变 NULL,compaction 会把丢失永久写回**(merge.rs 两处)。
> 修复 = ColumnarSSTable::scatter_by_row_id 精确散射,回归测试 ×2。
> 遗留:CI ubuntu integration 连续失败(macOS/本地全绿;已修 ci.yml 诊断步骤的
> 未配对大括号但注解仍未产出,Docker Linux 容器复现中);Python 绑定
> export_parquet/import_parquet/export_lerobot;DataLoader(§6)、公开基准(§7)、
> MCP(§8)。
>
> **开工时修正**:核对三个注册表后发现 crates.io `motedb` 0.15.0 与 PyPI
> `motedb-python` 0.15.0 **已上线**(RELEASE_NOTES 的"待 token/待登记"已过时),
> 只有 npm `motedb` 未首发。Phase 0 的发布动作因此收窄为 npm 一项。
>
> **Phase 0 进度(2026-10-20)**:工程部分已完成 — npm 加载器 libc 检测 +
> 流水线 NPM_TOKEN 守卫 + musl Alpine 容器构建步骤(矩阵条目暂注释,待 PR
> 验证后放开 —— 首发不与未验证目标耦合);`scripts/release_smoke.sh` + `make
> release-smoke[-local]` + `release-smoke.yml`(tag 触发,失败自动开 issue),
> Python/Rust 两路对已发布 0.15.0 实测通过;README/docs/01/02/RELEASE_NOTES
> 八处失真修正;docs/18 排障;CONTRIBUTING/SECURITY/issue 表单 ×3/PR 模板;
> good-first-issue 草稿 ×10(`2026-10-good-first-issues.md`)。
> **v0.15.1 发布(2026-10-20 收口)**:crates.io / PyPI 全绿;npm 根包 + 4/5
> 平台包(darwin×2、linux-gnu×2)0.15.1 已上线,本地实测安装+冒烟通过。唯一
> 遗留:`motedb-win32-x64-msvc`(全新包名)被 npm 新账号反垃圾拦截,tag 运行
> 3 次重试 + 手动重发 5 次递增退避(120/300/600/900s)均未在窗口内解除 —
> **恢复路径 = 冷却数小时后 Actions 里再点一次 Run workflow(node-publish,
> main)**,幂等检查只补这一个包;或从失败的 run 下载 win32 构建产物本地
> `npm publish`。Windows 用户在此之前 `npm install motedb` 得到 optional-dep
> 警告(macOS/Linux 不受影响)。release-smoke 的 tag 运行因等根包超时开过
> issue #1 — npm 齐后 dispatch release-smoke(version=0.15.1)取绿色记录并关闭。
> **待管理员**:① 开通 Discussions(Q&A/Show & Tell/Ideas/Robotics data);
> ② 按草稿创建 10 条 issue 并建标签;③ 官网安装卡片在 motedb.github.io 仓库
> (本仓库仅 llms.txt 源);④ musl:开一个只改 package.json targets + 取消
> 矩阵注释的 PR,node-publish 的 PR 触发会 build-only 验证 7 目标,绿了即合。

---

## 0. 总览与依赖关系

```
Phase 0  W1-2   三渠道发布 + release smoke + 文档一致性 + 社区地基 ────► v0.16.0
Phase 1  W3-4   Parquet 双向 + LeRobot v3 互导 ──┐
         W5     PyTorch DataLoader recipe ────────┤
         W6     公开基准 v1 (扩引擎×扩规模) ──────┼──► v0.17.0
         W5-6   MCP server (并行小项) ────────────┘
Phase 2  W7-8   向量索引工业化 (旋钮 SQL 化/写入吞吐/PQ spike) ──┐
         W9-10  SQL 补齐 (DECIMAL/DATE/窗口帧/ts 复合索引) ──────┤
         W11    1.0 准备 (迁移矩阵/API 冻结/VERSIONING) ─────────┼──► v1.0.0-rc1
         W12    内容战役 (博客 ×3 + 发布) ────────────────────────┘
```

不做清单(沿用 embodied-roadmap,新增两条):
分布式/集群、对象存储直查、DuckDB 级 SQL 全量对齐、客户端/服务器模式;
**不自研可视化/回放前端**(走 Rerun/Foxglove 互操作)、**不做视频编码转码进引擎**
(LeRobot 视频走引用,引擎只管遥测/标注/嵌入层)。

## 1. 差距总账(现状 → 行业参照 → 目标)

| 维度 | 现状 (v0.15.0) | 行业参照 (2026-10) | 目标 (W12) |
|---|---|---|---|
| 可安装性 | crates.io `motedb` 0.15.0 ✅ 已上线;PyPI `motedb-python` 0.15.0 ✅ 已上线;npm `motedb` 随 v0.15.1 首发(5 平台);README/docs 安装指令多处失真(已修) | `pip install` 三秒跑通是底线;竞品全渠道首发即通 | 三渠道同版本、7 平台(npm musl ×2 经 PR 验证后放开)、指令零失真、release smoke 守门 |
| 训练生态 | MCAP 进得来、无出口:Parquet 支持 0、LeRobot 支持 0、DataLoader 0 | LeRobot v3 = 事实标准(46+ 本体社区集);Rerun 0.32 自带 PyTorch dataloader + SQL catalog | LeRobot v3 双向互认 + 官方 DataLoader recipe |
| 基准公信力 | 自测 220K×384 vs FAISS(compete_vec_industry.py);Apple Silicon 自跑 | LanceDB 发布 10B 级公开基准;第三方横评(LanceDB/Chroma/sqlite-vec)已成文 | +1M 档、+LanceDB/sqlite-vec/DuckDB-VSS,`make bench-industry` 可复现 |
| 向量索引 | Vamana + SQ8(4x)已实现;旋钮(max_degree/search_list_size/alpha/beam_width)未暴露到 SQL/Python;写入 24K vecs/s;PQ 仅注释 | 多索引/量化/旋钮是标配;写入 ~100K vecs/s 量级 | 旋钮 SQL 化、batch 写入 ≥80K vecs/s、PQ go/no-go 有决议 |
| SQL 面 | 缺 DECIMAL/DATE/显式窗口帧;timestamp 索引仅 ts 首列 | — | README "Not yet supported" 清空(或仅剩游标) |
| 社区 | 0 外部用户;无 CONTRIBUTING/SECURITY/issue 模板/Discussions | — | Discussions 开通、≥1 外部真实 issue、good first issue ×10 |
| 版本承诺 | pre-1.0,每版格式迁移(兼容性做得好但无承诺) | 1.0 = semver + 格式冻结政策 | 1.0.0-rc1 + 迁移矩阵 v0.12→当前 + VERSIONING.md |

---

## Phase 0 — 装得上 (W1-W2) → v0.16.0

> 全部是管理员动作 + CI/文档工作,无引擎改动。价值大于后面所有技术项:
> 没有可安装性,其余一切不存在。

### 1. 三渠道发布落地

**crates.io** — ✅ `motedb` 0.15.0 已上线(`publish.yml` 的 CARGO_TOKEN 守卫
已配置生效)。后续版本 tag 触发即发。

**PyPI** — ✅ `motedb-python` 0.15.0 已上线(trusted publisher 已登记)。
**包名决策:保留 `motedb-python`**(已上线、官网与 llms.txt 一致);裸名
`motedb` 在 PyPI 仍空闲,未来若要占用可发一个依赖 `motedb-python` 的元包,
不改现有名字。README L58 原写 `pip install motedb` 已修正。

**npm** — `motedb` 随 **v0.15.1 首发**(5 平台:macOS arm64/x64、Linux
x64/arm64 glibc、Windows x64)。`node-publish.yml` 已切 registry.npmjs.org,
本次补齐:
- `index.js` 加载器按 `/usr/bin/ldd` / `process.report` 检测 musl,平台包后缀
  `-gnu|-musl|-msvc` 完整化;错误信息指向 docs/18
- publish job 增 NPM_TOKEN 缺失即明确报错;陈旧的 GitHub Packages 头注释更正;
  冒烟脚本落盘 `smoke_ci.js`(release smoke 复用)
- **musl ×2(x86_64 / aarch64)**:Alpine 容器构建步骤已写入工作流(napi-rs
  镜像,arm64 交叉编译显式设 CC/linker),但**矩阵条目注释、package.json 仍 5
  目标** —— `publish` 需全矩阵绿,未经 CI 验证的目标不能有机会卡住首发。
  放开路径:一个只改 package.json targets/optionalDependencies + 取消矩阵注释
  的 PR;node-publish 对 `bindings/node/**` 的 PR 触发会 build-only 验证 7 目标
  并 dry-run 发布,绿了合入,下个 patch 发出 musl 平台包(Node 绑定
  `default-features = false`,无 jemalloc musl 兼容风险)

### 2. release smoke(发布门,防"发了但装不上")

新增 `scripts/release_smoke.sh` + CI post-publish job(tag 触发):
干净 venv/容器里 `pip install` / `npm i` / `cargo add` 各拉**已发布**版本,跑
README 最小示例(建表 → insert_arrays → 向量 top-k → query_arrow)。
失败自动开 issue。冒烟脚本本地可跑:`make release-smoke`。

### 3. 文档/官网一致性(顺手项,半天)

- README L292 `motedb = "0.12"` → 当前版本;安装段与 PyPI 包名统一
- site/index.html 安装三卡片(三渠道 + 平台矩阵);llms.txt / llms-full.txt 同步
- 新增 `docs/18-install-troubleshooting.md`:glibc 版本、musl vs gnu、Windows
  无 jemalloc(系统分配器降级说明)、Python 3.9-3.13 轮子覆盖

### 4. 社区地基

- GitHub Discussions 开通(Q&A / Show & Tell / Ideas / Robotics data 四分类)
- `CONTRIBUTING.md`(构建/测试/Perf Gate 本地跑法)+ issue/PR 模板 + `SECURITY.md`
- 从已知缺口挑 10 个 `good first issue`(BLOB 标量函数残余、DATE 类型、文档
  勘误、bench 参数网格……),打标签

**验收(Phase 0 出口)**:三渠道同版本 v0.16.0 匿名可安装;release smoke 三语言
全绿;README/site/llms.txt 安装指令与实际包名一致;Discussions 开通。

---

## Phase 1 — 连得上 (W3-W6) → v0.17.0

> 定位陈述:MoteDB 不与 LeRobot 竞争,而是成为 **MCAP → 训练集** 管线中间的
> 查询/筛选/版本化/检索层——数据进得来(MCAP),也出得去(LeRobot/Parquet)。

### 5. Parquet 双向 + LeRobot v3 互导 (W3-W4)

**目标**:任意 MoteDB 表/数据集快照可导出为 LeRobot v3 目录;官方
`LeRobotDataset` 能无损读回;公开 v3 数据集能导入建索引检索。

**依赖**:`parquet` + `arrow` crates,新增 feature `parquet`(默认开,
`default-edge` 不含);cargo-deny 供应链门禁先行(参照 mcap 引入流程)。

**分层设计**(引擎 Rust 核心 / Python 映射胶水):

- **Rust 核心 Parquet** — 新 `src/export/parquet.rs`:
  列段 → Arrow RecordBatch(VECTOR→FixedSizeList<f32>,BLOB→Binary,
  TIMESTAMP→Timestamp(µs),INT/FLOAT/TEXT/BOOL 直映射)→ parquet writer;
  反向 reader → `insert_arrays` 批量管线。流式按 chunk(N 行/批),
  不整表物化。SQL:`EXPORT TABLE t TO 'x.parquet'` /
  `IMPORT PARQUET 'x.parquet' INTO t [(...)]`(parser 已有 EXPORT DB/DATASET
  产生式骨架,扩展即可)
- **LeRobot v3 映射层** — 新 `src/export/lerobot.rs`(纯函数:表+episode 元数据
  → 目录条目),Python 侧 `db.export_lerobot(dataset_tag, path, mapping=...)`:
  - EPISODE 表 `(key, ts)` → `episode_index` / `dataset_from/to_index` / `length`;
  - VECTOR 列按用户 mapping → `observation.state` / `action` 等 feature;
  - `meta/{info.json, episodes/*.parquet, tasks.parquet, stats.json}` +
    `data/chunk-XXX/file-XXX.parquet`(v3 多 episode/文件);
  - 视频:v1 不转码,`videos/` 留引用或跳过(文档明说"遥测+标注+嵌入层");
  - 反向 `db.import_lerobot(path, table)`:读 meta 建 EPISODE 表 + 数据列。

**涉及文件**:Cargo.toml(+parquet/arrow,deny.toml)、src/export/{parquet,lerobot}.rs、
src/sql/{parser,ast}.rs、src/sql/executor/mod.rs(EXECORT/IMPORT 执行臂)、
src/database/datasets.rs(快照 pinned 段复用)、src/bin/motedb-cli.rs
(`motedb export lerobot`)、bindings/python/src/lib.rs、Makefile(`lerobot-roundtrip`)

**测试**
- Parquet 全类型往返:INT/FLOAT/TEXT/TIMESTAMP/VECTOR/BLOB/NULL 混合 10 万行
  导出 → pyarrow 读回逐值一致;反向导入一致
- **互认测试(核心验收)**:导出目录用官方 `lerobot` 包 `LeRobotDataset(root=...)`
  加载成功,`len(ds)` / `ds[0]` 字段与源一致;选一个公开 v3 小集(如 pusht 类)
  导入 → `SELECT COUNT(*)` 与 `EPISODES(t)` 行数对 meta 校验
- 资源:100 万行导出 RSS ≤ 200MB(流式断言);kill -9 中断导出不留半目录
  (temp+rename,复用 MOTESNAP 纪律)

**验收**:一条公开 LeRobot v3 数据集导入后,`CREATE VECTOR INDEX` +
`ORDER BY emb <-> ? LIMIT 10` 与 `traj_sim` 检索可用;导出目录官方 loader 可读。

### 6. PyTorch DataLoader recipe (W5)

- `bindings/python/python/motedb/torch.py`:`MoteDataset(db, sql, params, batch_size)`
  — map-style(PK 范围切片)+ iterable(流式)两种;底层 `fetch_arrays` numpy
  零拷贝 → `torch.from_numpy`;多 worker 每 worker `open_readonly` 容器独立句柄
  (fork 安全,复用只读多读者能力)
- `docs/19-training-dataloader.md` + `examples/torch_dataloader.py`
- 基准:同一 LeRobot 集,MoteDB DataLoader vs 官方 `LeRobotDataset` 迭代吞吐
  (samples/s)与首批延迟;**结果如实写进文档,不达也写**

**验收**:example 跑通,吞吐对比数据进文档。

### 7. 公开基准 v1 (W6)

现状:`compete_vec_industry.py` 已对照 FAISS(Flat/IVFFlat/HNSW)于 220K×384,
recall 协议完整(corpus.npy + 暴力 ground truth + 200 查询)。扩面不重做:

- **引擎扩**:LanceDB(IVF_PQ/HNSW)、sqlite-vec、DuckDB-VSS、usearch(可选)
  — 各为独立 extras 依赖,缺失自动跳过;FTS 同步扩 compete_fts_industry.py
  (+LanceDB FTS / tantivy)
- **规模扩**:220K → +1M×384(磁盘档路径,语料生成器参数化)、+100K×1536
  (OpenAI/Claude 嵌入维度);写入吞吐(vecs/s)单列成表
- **产出**:`make bench-industry` 一键;JSON 落 `benchmark_results/`;
  `docs/benchmark_report_2026-11.md` 自动渲染;官网 Benchmarks 页
- **方法学声明**:硬件/各引擎版本/参数网格/ground truth 全公开,接受 PR 复跑
  修正 — 这是公信力的来源

**验收**:干净机器 clone 后 `make bench-industry` 可复现;报告含 ≥4 引擎 ×
≥2 规模档;MoteDB 不利项照登(边缘内存/ACID/多模态 SQL 维度另列)。

### 8. MCP server (W5-W6 并行小项)

- Python 包内 `motedb.mcp`(stdio;比 Rust bin 成本低、复用绑定):
  tools = `list_tables` / `describe` / `query`(只读门)/ `vector_search(table,
  col, vec, k)` / `episodes(table)` / `doctor`
- 用例:agent 直接问"找出上周抓取失败的 episode" → MATCH+向量 RRF 一条 SQL
- 文档 + 官网 "Use with AI agents" 卡片(Foxglove 已在打 agentic 叙事,跟进)

**验收**:主流 MCP 客户端一行配置接入;README 演示。

**Phase 1 出口 → v0.17.0**:LeRobot 双向 + DataLoader + 基准报告 + MCP。

---

## Phase 2 — 立得住 (W7-W12) → v1.0.0-rc1

### 9. 向量索引工业化 (W7-W8)

现状:VamanaConfig 已有 max_degree/search_list_size/alpha/beam_width/metric +
embedded()/performance() 预设;**SQ8 已实现**(src/index/vamana/sq8.rs,4x 压缩);
fresh_graph.rs 增量构建在库。缺口 = 暴露、吞吐、文档:

- **旋钮 SQL 化**:`CREATE VECTOR INDEX ... WITH (max_degree=64,
  search_list=100, quantization='sq8'|'none')`;会话级
  `SET vector_search_list = N`(查询时 recall/延迟旋钮);Python 侧
  `db.set_vector_search_list(n)`
- **写入吞吐专项**:profile 现 ~24K vecs/s 瓶颈(逐点插入 vs batch_build 路径、
  锁粒度、距离核);目标 batch 路径 ≥80K vecs/s;增量插入走 fresh_graph 延迟
  合并(现有机制,补文档与默认策略)
- **PQ spike(1 周 go/no-go)**:仅当 §7 的 1M 档数据显示 SQ8 内存仍吃紧才做;
  判据:内存再降 ≥2x 且 recall@10 跌幅 ≤1%;否则记 no-go 备忘
- **删除/更新召回稳定性**:tombstone 比例 0/10/30% 下 recall@10 不塌(>0.95)
- **文档**:docs/08-vector-index.md 增 recall/latency/内存三维调优表
  (数据来自 §7)

**涉及文件**:src/index/vamana/{config,diskann_index,fresh_graph}.rs、
src/sql/{parser,ast}.rs(WITH 子句)、src/sql/executor/(索引构建入口)、
bindings/python/src/lib.rs

**验收**:Perf Gate +向量写入吞吐形状;1M 档 recall@10≥0.95 时 p99 与最优竞品
差距 ≤2x(如实记录);README 向量段落补调优表链接。

### 10. SQL 补齐 (W9-W10)

- **DECIMAL**(i128+scale 定点)/ **DATE**(i32 天):ColumnType 两成员 →
  row_format → 列段(复用 Fixed)→ evaluator 算术/比较/聚合 → Python
  (decimal.Decimal / datetime.date)→ Arrow(Decimal128 / Date32)
- **显式窗口帧** `ROWS/RANGE BETWEEN`:parser + 窗口执行器帧边界
- **timestamp 索引放开列位限制**:非首列 ts 走扫描的已知缺口 — 允许
  `CREATE TIMESTAMP INDEX ON t(ts)` 任意列位(LazyTsIndex 扩展)
- BLOB 标量函数残余缺口清零(v0.13 遗留清单)
- 差分测试扩面:DECIMAL/DATE 对 SQLite/DuckDB 对拍(既有差分框架直接加形状)

**验收**:`test_fuzz_sql_combinations` 模板纳入新类型;README "Not yet
supported" 清空或仅剩服务端游标。

### 11. 1.0 准备 (W11)

- **格式迁移矩阵**:CI job 用 v0.12/0.13/0.14/0.15 各发布版二进制生成样本库
  (每版 ≤5MB 进 tests/fixtures)→ 当前版打开 → 全量查询校验;
- **API 冻结**:cargo public-api 清单 → 补 `#[non_exhaustive]`/收 `#[doc(hidden)]`;
  Python/Node API 冻结清单入 docs/14-api-reference.md
- **VERSIONING.md**:semver 政策 + 存储格式版本政策(1.x 内前向兼容,破坏性
  改动只在 2.0;段不可变设计使多数优化可新段生效)
- 安全收尾:cargo-audit 进 CI(deny 已有);fuzz +parquet reader 目标;
  SECURITY.md 挂漏洞报送
- 发布 **v1.0.0-rc1**,公告征集 2 周反馈

### 12. 内容战役 (W12 收口;素材自 Phase 1 起积累)

- 博客 ×3:
  ① 「为什么机器人数据需要 episode / ASOF / traj_sim 原语」(DTW spike 数据
  + 与 MCAP/LeRobot 的表达力对比);
  ② 「MCAP → SQL → LeRobot:一条管线」(§5 demo 全程);
  ③ 「嵌入式向量库横评:MoteDB vs LanceDB vs sqlite-vec vs DuckDB-VSS」
  (§7 数据,**含不利项** — 公信力即卖点)
- 渠道:Show HN、r/robotics、r/MachineLearning、LeRobot Discord、
  Foxglove 社区、知乎/掘金(中文)
- 官网 Blog 区 + RSS;llms-full.txt 同步(现已有,保持)

---

## 测试与质量门(新增)

| 层 | 手段 |
|---|---|
| 发布 | release smoke 三语言 post-publish job(装的是已发布产物) |
| 互操作 | Parquet 全类型往返;LeRobot 官方 loader 互认;Arrow schema 快照测试 |
| 基准 | `make bench-industry` 可复现;结果 JSON 入库、报告自动渲染 |
| 性能 | Perf Gate +形状:向量写入吞吐、Parquet 导出吞吐 |
| 兼容 | 格式迁移矩阵 v0.12→当前(CI 常驻) |
| 安全 | cargo-audit;fuzz +parquet reader 目标(ASAN 每日) |

## 里程碑与发布

| 周 | 内容 | 出口判据 |
|---|---|---|
| W1-2 | 三渠道发布 + smoke + 文档一致 + 社区地基 | **v0.16.0** 三渠道同版本可装 |
| W3-4 | Parquet 双向 + LeRobot v3 互导 | 官方 loader 互认测试绿 |
| W5 | DataLoader recipe + MCP server | examples 跑通 |
| W6 | 公开基准 v1 | 报告上官网 → **v0.17.0** |
| W7-8 | 向量旋钮 SQL 化 + 写入吞吐 + PQ spike | Perf Gate 新形状绿 |
| W9-10 | DECIMAL/DATE/窗口帧/ts 索引放开 | README 缺口清零 |
| W11 | 迁移矩阵 + API 冻结 + VERSIONING | **v1.0.0-rc1** |
| W12 | 博客 ×3 + 发布 | 外部反馈 ≥1 |

## 风险登记

| 风险 | 缓解 |
|---|---|
| 渠道注册受阻(npmjs 曾因区域限制受阻;crates.io/PyPI 待登记) | 并行兜底:GitHub Releases 附 wheel/.node/.crate 直链 + 文档 `pip install <url>`;渠道通了再撤 |
| LeRobot v3 spec 变动(HF 迭代快) | 互认测试锁定 lerobot 包版本;映射层独立模块(src/export/lerobot.rs),spec 变更只改映射不动引擎 |
| 视频不进引擎被质疑"导出不完整" | 文档直说定位:遥测+标注+嵌入层;视频引用路径留给 ffmpeg 管线 |
| 基准结果不利(1M 档 LanceDB 可能更快) | 如实发布;差异维度(嵌入式内存/ACID/多模态 SQL/断电安全)另列成表 — 不利项就是 1.x 路标 |
| parquet/arrow 体积拖累 edge 构建 | feature `parquet` 门控,default-edge 不含;二进制体积进 Perf Gate 观察 |
| musl 构建的 jemalloc 兼容 | musl 档关 jemalloc 走系统分配器;release smoke 覆盖 Alpine 容器 |
| 12 周带宽不足(单人/小团队) | Phase 0 可 3 天完成;Phase 1 四项两线并行;Phase 2 任一项可砍而不影响 1.0-rc 出口 |
| 1.0 格式冻结限制后续优化 | 段不可变 + 增量 MANIFEST 设计已证明"新段生效"模式;VERSIONING 写明演化通道 |

## 成功指标(W12 复盘)

- **可安装**:三渠道同版本;PyPI 周下载 ≥1K;GitHub Releases 兜底直链可用
- **生态**:LeRobot 双向 demo 公开可复现;≥1 外部真实 issue/PR;Discussions ≥10 帖
- **公信力**:基准报告被 ≥1 第三方引用或复跑;(可选)ann-benchmarks/XBench 提交
- **产品**:v1.0.0-rc1 发布;README "Not yet supported" 仅剩游标;向量调优表成文
