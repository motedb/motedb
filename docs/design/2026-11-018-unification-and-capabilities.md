# 技术方案: 0.18 — 求值器统一（技术债清偿）+ 产品能力硬伤补全

> 状态: **已批准 v2**（2026-11-04 批准开工；三个裁决点按推荐执行:
> PR-2 顺带修 Kleene 三值逻辑并 CHANGELOG 声明；B5 连续聚合纳入 0.18 排期
> 但排最后、可独立裁剪；单人串行 6 周节奏）
> 前序: v0.17.0 五轮修复的实证结论——四轮"同一谓词不同路径不同结果"类缺陷
> （`WHERE emb='[…]'` 九路径、DELETE/JOIN/距离算子）全部同根：SQL 比较语义
> 存在于 11 个互不相识的实现里。本方案先清这笔债（工作流 A），再在其
> 稳定地基上补产品能力硬伤（工作流 B）。
> 原则: 所有设计锚定现有代码结构（附 file:line）；每个特性附验收判据；
> 行为等价重构用特征测试兜底，能力新增用差分测试验收；每项独立 PR、
> 全量门禁（fmt / clippy 0 警告 / 4300+ 测试 / 崩溃与稳定性套件）绿。

## 0. 现状量化（2026-11-04 实测）

| 债项 | 实测值 | 采集方式 |
|---|---|---|
| SQL 比较语义实现入口 | **11 个**（另 2 处内联闭包） | grep `fn {eval_binary_op,…}` 逐一定位 |
| `src/sql` 内裸 `==` / `partial_cmp` 比较点 | **49 处**（含合法的类型层实现） | grep，迁移时逐点判归 |
| 固定 `/tmp` 路径测试 | **10 文件 / 30 处** | grep `"/tmp/` in tests/ |
| ALTER 支持面 | 仅 ADD COLUMN（+AUTO_INCREMENT） | `AlterTableAction` 枚举 |
| 打开模式 | 独占写 / 容器只读，无共享读 | api.rs flock |
| TS 聚合地基 | `time_bucket` 已有（evaluator.rs:2318） | 可作为连续聚合地基 |

---

## 工作流 A: 结构性技术债（周 1–2）

### A2. 测试卫生（0.5–1 天，最先做）

**锚点**: tests/ 下 10 文件 30 处 `"/tmp/motedb_*"`（test_mvcc_isolation.rs、
test_e2e_crash_and_stress.rs 等）。

**做法**: 机械替换 `TempDir::new()`；对依赖执行顺序的（如先写后读跨用例）
合并为单用例内顺序；并发敏感断言改为状态断言。crash-injection 类
（子进程 + journal）保持固定路径但加随机后缀目录 + 用例自清理。

**验收**: `cargo test --test <同文件>` 两份并发连跑互不影响；
`cargo test` 全绿两次（排除缓存偶然）。

### A1. 求值器统一（8–10 天，核心）

#### A1.1 现状点位表（迁移对象）

| # | 入口 | 位置 | 服务路径 | 现有语义（差异即风险） |
|---|---|---|---|---|
| 1 | `ExprEvaluator::eval_binary_op` | evaluator.rs:770 | SqlRow 解释器（DELETE 扫描、投影、materialize 路径） | coerce_bool_numeric + coerce_vector_text + 类型层 partial_cmp；NULL→返回 Null（三值） |
| 2 | `ExprEvaluator::values_equal_coerced` | evaluator.rs:744 | `=`/IN 相等 | Bool/Vector 特判，其余 PartialEq；不判 NULL（调用方负责） |
| 3/4 | `coerce_bool_numeric` / `coerce_vector_text` | evaluator.rs:708 / 725 | #1/#2 的两个半独立 coerce | 组合语义散在两个函数 |
| 5 | `eval_expr_simple` | executor/mod.rs:10341 | 位置式快路径（SqlRow） | Eq/Ne 走 #2（0.17 补），Lt..Ge 裸 partial_cmp，NULL 各臂手判 |
| 6 | `eval_expr_on_row` | executor/mod.rs:11687 | 位置式 `&[Value]`：JOIN finalize、KNN WHERE、materialize_subqueries | Eq coerced / Ne 取反（0.17 补）；Lt..Ge 裸；距离算子 0.17 补 |
| 7 | `eval_with_materialized` | executor/mod.rs:14913 | 物化慢路径（逗号 join 等） | 六比较委托 #1（0.17 补）；AND/OR 两值化（非 Kleene）⚠ |
| 8 | `compile_simple_comparison` | executor/mod.rs:23564 | RowPredicate 编译（物化过滤） | 0.17 重写：编译期向量预解析 + NULL 不匹配 |
| 9 | `apply_op_value` | executor/mod.rs:213 | JOIN 下推 / 写集过滤逐行比较 | 0.17 补消费端向量 coerce；NULL 不匹配（三值） |
| 10 | `VecPred::lit_cmp` + `coerce_pair` | vector_exec.rs:179 / 74 | 批扫描（VEC M1/M3） | 自有 coerce_pair（Bool/Date/Tensor 预解析），Text↔Timestamp 与 #1 不同源 |
| 11 | `normalize_for_in` | executor/mod.rs:62 | IN 集合成员归一 | Bool→Int、ISO→Timestamp、方括号→Vector |
| 12 | `coerce_vector_literal_for_column` | executor/mod.rs:11126 | 编译期按列型归一（CompiledWhere / 下推生产者） | 仅 Tensor；未来 Date/Timestamp 字面量应同点扩展 |
| — | `CompiledWhere::eval` | executor/mod.rs:~1394 | 段扫描编译谓词 | 位置比较 + needs_bool_coerce，向量已由 #12 前置归一 |
| — | `col_segment_projected_scan` Eq 闭包 | executor/scan.rs:~2258 | 单谓词投影扫描 | 0.17 补 #12 前置归一 |

#### A1.2 目标架构：三层

```
L3 执行层（只保留控制流：求值顺序 / 三值逻辑门 / 短路 / 批布局）
    │  只允许调用 ↓
L2 语义层  src/sql/semantics.rs   ★本方案唯一新增核心代码★
    │  建立在 ↓
L1 类型层  types::Value 的 PartialEq / PartialOrd（不动，只做同型与数值跨型）
```

**L2 API（初稿，PR-1 内定稿）**:

```rust
/// SQL 三值比较结果。
pub enum Cmp3 { True, False, Unknown }

/// 操作数归一（Bool↔Int/Float；方括号文本↔Vector——仅当另一侧为 Vector；
/// ISO 文本↔Timestamp/Date——仅当另一侧为对应类型）。Cow 借用优先，
/// 归一不命中时零分配。
pub fn coerce_pair<'a>(a: &'a Value, b: &'a Value) -> (Cow<'a, Value>, Cow<'a, Value>);

/// SQL `=`：任一 NULL → Unknown；否则 coerce 后相等判定。
#[inline] pub fn equal(a: &Value, b: &Value) -> Cmp3;

/// SQL `<`/`<=`/`>`/`>=`：任一 NULL → Unknown；不可比 → Unknown。
#[inline] pub fn compare(a: &Value, b: &Value, op: OrdOp) -> Cmp3;

/// 距离算子操作数归一（<-> / <=> / <#>）——coerce_pair 的超集。
pub fn coerce_distance_operands(a: &Value, b: &Value) -> (Cow<Value>, Cow<Value>);

/// IN 集合成员/探测值归一（吸收 normalize_for_in）。
pub fn normalize_set_member(v: &Value) -> Value;

/// 编译期按列类型的字面量归一（吸收 coerce_vector_literal_for_column；
/// 显式扩展点：Date/Timestamp 字面量未来同点接入）。
pub fn coerce_literal_for_column(schema: &TableSchema, pos: usize, val: Value) -> Value;
```

**约束**:
- L2 函数全部 `#[inline]`，非命中归一路径零分配（保 `apply_op_value`/
  `lit_cmp` 逐行热路径——bench 对比见 A1.6）；
- L2 不持有 schema、不做 IO；schema 感知只在编译期入口
  （`coerce_literal_for_column`）；
- 语义裁决表（谁说了算）：NULL 三值以 SQL 标准为准；Bool↔Int 沿用现行
  （TRUE=1）；Vector↔方括号文本沿用 0.17 已发布行为；Timestamp↔ISO 文本
  沿用类型层。**本方案不改任何已发布语义**，只消除实现分裂。

#### A1.3 特征测试先行（PR-1，2 天）

迁移前把"当前正确行为"固化成合同:
- **类型对矩阵**: {Integer, Float, Bool, Text, Vector, Timestamp, Date, NULL}
  代表性操作数对 ~25 组 × {`=`,`<>`,`<`,`<=`,`>`,`>=`,`IN`,`NOT IN`}，
  断言输出为 Bool/Null——表驱动，直接调 L2（先按 #1 现行为生成期望值）；
- **端到端快照**: 0.17 已落的 `vector_comparisons_agree_before_and_after_
  checkpoint`（12 形状）、`join_with_vector_predicates_all_shapes`（9 路径）、
  `dml_with_vector_predicates` 作为集成级特征测试基线；
- NULL 专项: `NULL = NULL → Unknown`、`NOT (NULL = x)`、AND/OR Kleene
  （#7 的两值化 AND/OR 是已知偏差，单列用例锁定现状，修复在 PR-2 顺带并
  在 CHANGELOG 声明）。

#### A1.4 迁移顺序（PR 粒度，依赖序）

| PR | 内容 | 风险 | 回归重点 |
|---|---|---|---|
| PR-1 | `semantics.rs` + 特征测试（不接线） | 无 | 新测试自洽 |
| PR-2 | 解释器四件套接线: #1/#2/#5/#6（+#7 的 Kleene 修正） | 中 | DELETE/UPDATE/投影/KNN 过滤全量 |
| PR-3 | 逐行比较三件套: #9 `apply_op_value`、#8 `compile_simple_comparison`、`CompiledWhere::eval` | 中高（热路径） | JOIN 全形状 + bench |
| PR-4 | 批与扫描: #10 `lit_cmp`/`coerce_pair`、投影扫描 Eq 闭包、聚合下推比较器 | 中 | VEC M1/M3、聚合差分 |
| PR-5 | 收编 #11/#12 进 L2、删旧实现、**CI 门禁** | 低 | 全量 |

**CI 门禁（防回潮）**: ci.yml 加一步 `scripts/check_sql_cmp.sh`——
`src/sql`（排除 `semantics.rs` 与 `#[cfg(test)]` 块）禁止白名单外的
`partial_cmp(`/`== &`/`v == target` 模式；白名单 = L1 委托点 + 显式
类型层实现（types/mod.rs 不受限）。

#### A1.5 差分 harness（PR-5 并入套件，2 天）

生成式对拍，种子可复现:
- 随机谓词生成（类型感知: 列-OP-字面量 + AND/OR/NOT 嵌套，含 NULL 与
  方括号向量拼写）；
- **九条路径同库同谓词**: SELECT WHERE / DELETE / UPDATE / INNER JOIN
  （两种表序）/ 逗号 join / 子查询 IN / 聚合 WHERE / GROUP BY+HAVING；
  DML 用副本库执行后比对行集；
- 断言: 九路结果集合一致；不一致 → 打印谓词 + 各路结果（复现种子）。

#### A1.6 性能护栏

- PR-2/3/4 各跑 `make compete-quick` + `apply_op_value` 微基准
  （新增 `benches/cmp_semantics.rs`: 1M 次整数/文本对比较，回归 ≤3%）；
- `lit_cmp` 批路径用现有 VEC M1 聚合基准盯住。

#### A1.7 风险与回滚

- 语义微差暴露（某路径历史行为与 L2 不一致）→ 特征测试先红 → 按
  "已发布行为优先"裁决，确属 bug 的单列修复 + CHANGELOG；
- 每 PR 独立可回滚；PR-3 热路径若 bench 超阈 → L2 加特化分支而非放宽门禁。

---

## 工作流 B: 产品能力硬伤（周 3–6）

### B1. 多进程只读（1.5 周，P0）

**锚点**: api.rs:180 `open_engine_shared`（flock EX）、api.rs:268
`open_readonly`（仅 MOTESNAP 容器，解包到缓存目录）、api.rs:699
`release_lock`（close 释放 flock 的既有机制）。

**设计**: 打开模式三分——

| 模式 | API | 锁 | 语义 |
|---|---|---|---|
| 独占写（默认，不变） | `open`/`create` | flock EX | 现状 |
| **共享只读（新）** | `open_shared_readonly` | flock SH | N 进程并发读 |
| 容器只读（不变） | `open_readonly` | 无（解包副本） | 现状 |

**v1 边界（诚实取舍）**: 只读者**不重放 WAL、不修复目录**——仅对
clean-close（close marker 存在）或恰好 checkpoint 边界的目录保证一致；
打开时检测: WAL 非空 / 检测到 B-16 残渣或待修复段 / pending_gc 非空 →
返回明确错误（"先以写模式 open+close 一次或 checkpoint"）。与 SQLite
rollback-journal 模式同语义，不承诺 WAL 并发读。

**实现步骤**:
1. `ColSegmentStore::open_readonly_snapshot()`——`recover_from_disk` 的
   strictly-no-mutation 变体: 跳过孤儿删除、修复合并、pending_gc、
   manifest 写（约 +150 行，核心工作量）;
2. `MoteDB::open_shared_readonly`——WAL 空校验、flock SH、写线程全不启;
3. 写拒绝层复用容器只读的拒绝点（写路径统一 `readonly` flag 检查）;
4. close: 只 munmap，无任何落盘。

**验收**: ① 2 读者进程并发查询一致; ② 读者开脏 WAL/待修复目录得明确
错误; ③ 写者持 EX 时读者 open 失败且提示; ④ kill -9 写者后 clean 目录
读者可开; ⑤ 读者进程 RSS 与单进程持平（无隐藏重建）。

### B2. Schema 演进补全（1.5 周）

**锚点**: executor/mod.rs:21985 `execute_alter_table`、22015
`AlterTableAction::AddColumn`（既有宽度机制: builder 按 builder 列数补
NULL、`coerce_row_to_schema` 补 DEFAULT、merge 回填）。

**范围裁决**:
- **DROP COLUMN（逻辑删除，v1）**: `ColumnDef` 增 `dropped: bool`;
  写路径: 该位置仍写 NULL 占位（**物理宽度不变**——段按位置存列，收窄
  会造成新旧段错位，这是 ADD COLUMN 机制已经证明的约束）; 读路径:
  schema 列迭代统一跳过 dropped（投影/聚合/索引重建点各一处过滤）;
  merge 沿用现有 raw 列复制（d 列照搬 NULL）——**零新存储机制**;
  物理回收留给后续 vacuum 变体（如实文档化）。
  约束: PK/NOT NULL 列禁止 DROP; 该列上有索引 → 明确报错（v1 不级联）。
- **RENAME COLUMN**: 纯 schema 层（列按位置存储）+ index_metadata/
  缓存键失效重建（列名进索引命名约定 `table_col`）。
- **RENAME TABLE**: 顺延——`columnar_ms/<table>/` 目录名含表名 + manifest
  register_table_dir + indexes 命名联动，涉及 fs 搬迁，单独立项。

**验收**: ALTER 全家桶 × {写缓冲, checkpoint 后, reopen, 崩溃注入恢复}
往返; dropped 列的 INSERT 短行/DEFAULT/WAL 回放兼容; 0.17 客户端打开新
目录的前向限制写入 VERSIONING.md。

### B3. 向量召回硬化（2 周，可与 B1 并行）

**锚点**: executor/mod.rs ~25093 `execute_vector_order_by_plan` 的
`use_index` 路由与 correctness-first 注释（内部已记录增量 stranded-node
recall 低至 0.03–0.09 的实测）、`rerank_exact`、core.rs:2475
`load_vector_indexes`。

**步骤**:
1. **先测量**: `make vec-recall`——建索引后增量插入 N 批（含 UPDATE/DELETE
   churn），top-k vs 暴力扫，输出 recall@{1,10,100} p50/p99; 把已知
   stranded 场景固化成回归（阈值起步 0.98@10）;
2. **新鲜度护栏**: 向量索引持久化 `built_row_count`; 查询时与表 live 行数
   比对，增插占比 > 阈值（默认 20%，env `MOTE_VEC_REBUILD_RATIO`）→
   触发后台 bulk 重建（复用现有重建通道 + gc_filter 防护 + 进度复用
   B-15 的 stderr 阶段输出）;
3. **路由降级**: `use_index` 增加 `index_fresh()` 条件——stale（重建中或
   增插超阈）→ 该查询走暴力精确扫（正确性优先，与现有路由注释一致）;
4. **对外**: README/08-vector-index.md 公布 recall 数字、护栏策略与
   `MOTE_VEC_INDEX_MIN_ROWS` 语义; BENCHMARK.md 增表。

**验收**: 增量场景回归 recall≥0.98@10; 后台重建期间查询不阻塞且结果仍
精确（降级路径）; 死锁/并发套件过。

### B4. EXPLAIN 全计划（1 周）

**锚点**: executor/mod.rs:19703（heuristic plan v1 注释: "set-based paths
not yet modeled"）。

**设计转向——从"启发式建模"改为"决策点实录"**: 与其维护第二套计划模型，
不如让执行器在**真实决策点**写 thread-local plan trace（扫描策略、
join build 侧与谓词下推、聚合下推命中、KNN index/brute、段数），
`EXPLAIN` 查询开启采集后原样输出——计划永远与实际一致，无漂移。

**步骤**: ① `plan_trace` 模块（thread-local + 采集开关）; ② 15–20 个
决策点插桩（各一行宏）; ③ EXPLAIN 入口切换为"重放执行 + 采集输出"，
保留 `plan_version` 列; ④ 删除 heuristic 模型的维护负担（保留单表
快路径描述作为无执行成本的降级输出）。

**验收**: 0.17 第四轮的 6 种 JOIN 形状 + 聚合下推命中/未命中 + KNN 索引/
暴力，EXPLAIN 输出与本轮 DBG 实证逐一一致（直接把探针转成断言）。

### B5. 时序连续聚合（1.5 周，可后置）

**锚点**: `time_bucket(interval, ts)` 已有（evaluator.rs:2318、
executor/mod.rs:9899）; GROUP BY 下推与列段聚合现成; TTL/gc 通道现成。

**设计**:
- DDL: `CREATE CONTINUOUS VIEW cv AS SELECT TIME_BUCKET('1m', ts) AS b,
  device, AVG(v) AS v_avg FROM sensor GROUP BY b, device`;
  元数据入 table_registry（kind=continuous_view，记基表/投影/分组/watermark）;
- **惰性刷新（v1）**: 查询 cv 时——读 watermark → 对基表 [watermark, now)
  增量聚合（复用 GROUP BY 下推）→ 合并入 cv 存储 → 推进 watermark;
  CHECKPOINT 一并物化; **语义如实声明**: 滞后到上次查询/检查点、晚到数据
  （ts < watermark）忽略——不做写时维护与乱序回填;
- 聚合函数: 现有 AVG/MIN/MAX/COUNT/SUM + 补 `FIRST/LAST`（TS 语义）;
- 验收: 随机写入（含多设备乱序）下 cv ≡ 等价即时 GROUP BY 差分;
  checkpoint/reopen 后 watermark 持久正确。

---

## 排期与里程碑（单人串行 6 周; 两人则 B1‖B3 压到 4 周）

```
周 1     A2 → A1 特征测试(PR-1) + semantics.rs
周 2     A1 PR-2..5 + CI 门禁 + 差分 harness          → M1: 债清
周 3–4   B1 多进程只读 ‖ B3 向量召回                    → M2
周 4–5   B2 schema 演进 → B4 EXPLAIN 实录              → M3
周 5–6   B5 连续聚合 + 文档/CHANGELOG → 0.18.0 发版     → M4
```

**0.18.0 发版清单捎带项**（本轮核查发现的"廉价专业度"）:
文档版本单源（README 写 0.16、installation 写 0.15 → 单一 source + CI
校验）; `motedb-cli` 随 workspace 发 crates.io; GitHub Release 页自动化
（tag 后建 Release 并填充 CHANGELOG 段落）; win32 若 npm 届时放行
（issue #5）则随版回归四件套并合并 PR #4。

## 明确不做（0.18 out of scope）

向量量化（PQ/SQ）/HNSW 备选; 多进程写; WAL 并发读; 分布式; RENAME TABLE
（涉及目录搬迁，单独立项）; npm musl/Alpine 预编译; 求值器以外的执行器
重构（优化器/join 算法保持现状）。

## 风险总表

| 风险 | 概率 | 缓解 |
|---|---|---|
| A1 迁移暴露历史语义分歧 | 高（预期内） | 特征测试先红先裁; 已发布行为优先; 单 PR 回滚 |
| A1 热路径性能回退 | 中 | inline+Cow 零分配; 微基准 + compete 门禁 ≤3% |
| B1 只读打开遇脏目录误读 | 中 | 严格检测（WAL/残渣/pending_gc）+ 明确报错; 不修复 |
| B2 dropped 列与旧版本前向兼容 | 中 | 物理宽度不变; VERSIONING.md 记前向限制 |
| B3 后台重建与查询死锁 | 中 | 复用现有锁序; 死锁套件 + 稳定性套件扩展 |
| B4 EXPLAIN 重放执行成本 | 低 | 仅 EXPLAIN 显式触发; 单表快路径降级输出保留 |
