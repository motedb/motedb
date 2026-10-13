# 存储文件布局收敛 — 综合评定

- 日期: 2026-10-13
- 状态: 评定完成,待排期实施
- 实施: 见 [2026-10-file-layout-plan.md](./2026-10-file-layout-plan.md)(P1 惰性化 → P2 全局 MANIFEST → P3 meta.bin → P4 EXPORT 快照)
- 范围: `.mote` 数据库目录的文件数量、fd 占用、以及每项收敛方案的性能影响
- 证据: 代码盘点（v0.14.0）+ 实测（`examples/file_layout_bench.rs`、lsof、目录树采样）

## TL;DR

1. **多文件"目录派"形态本身是对的**（列存 + LSM flush/compaction 的标准形态，RocksDB/LevelDB 同派），不应该学 SQLite/DuckDB 做单文件运行时格式。
2. **真正的病灶是固定开销**:一个 2 表库有 17 个文件,其中 13 个与数据无关;其中 6 个(4 个空 WAL 分区 + 空 `columnar/` + 空 LSM blob)是零字节的纯残留。
3. **每表 MANIFEST 是最大的结构性浪费**: 每张表 1 个文件 + 常驻 1 个 fd,100 张表 = 100 个 fd,直逼嵌入式默认 256 fd 软限。收敛为全局单一 MANIFEST 后每表只剩 1 个数据文件。
4. **实测证明收敛无性能代价**: WAL 分区 1 vs 4 在 GroupCommit 和 Synchronous 两种模式下吞吐均在 ±5% 噪声内;MANIFEST 追加本就不 fsync(孤儿领养设计),合并只影响 µs 级 write() 串行化。
5. 目标形态: **文件 ≈ 3 + 表数 + 用户索引数,固定 fd ≈ 3~7**(现状: 文件 ≈ 11 + 2×表数,fd ≈ 6 + 表数)。2 表库从 17 文件降到 5~6 个,达到"每表 1~2 个文件"的直觉预期。

## 1. 现状盘点(实测)

### 1.1 一个 2 表 + 1 索引库的完整产物

```
db.mote/                          ← 17 文件 / 10 目录
├── .lock                         锁(干净关闭后仍留盘)
├── catalog.bin                   表目录
├── index_metadata.bin            索引元数据
├── lsn_counter                   LSN 持久化
├── fts_indexes.fresh             干净关闭标记
├── columnar/                     ❌ 空(仅 TimeSeries 表使用)
├── columnar_ms/
│   ├── events/  0000000021.sst + MANIFEST    ← 真实数据
│   └── sensors/ 0000000021.sst + MANIFEST
├── indexes/
│   ├── column_idx_dev.idx        用户二级索引
│   └── timestamp.idx             ⚠️ 无条件创建,即使无 TIMESTAMP 列
├── lsm/blobs/00000001.blob       ⚠️ 空文件,LSM 初始化即建
└── wal/partition_{0..3}.wal      ⚠️ 4 个 0 字节,WAL-elimination 路径下从不写入
```

### 1.2 数量公式(20 表库实测: 50 文件 / 27 目录 / 27 fd)

| 维度 | 公式 | 100 表外推 |
|---|---|---|
| 文件数 | `11 + 2T + I`(T=表数, I=用户二级索引) | 211 |
| 目录数 | `7 + T + 2F`(F=FTS 索引数) | 107 |
| 运行时 fd | `6 + T`(MANIFEST 每表常驻 1 fd) | **106(危险)** |

fd 实测明细(20 表,lsof): 20×MANIFEST + 4×WAL + 1×LSM blob + 1×timestamp.idx + 1×lock = 27。**`.sst` 段不占 fd** — Unix 上 mmap 建立后 fd 即可关闭,段文件零 fd 常驻。

### 1.3 每类文件的完整画像

| 文件/目录 | 创建时机 | 写入频率 | 常驻 fd | 评估 |
|---|---|---|---|---|
| `columnar_ms/<tbl>/NNN.sst` | 每次 flush | 不可变(temp+fsync+rename 原子发布) | 0(mmap) | ✅ 数据本体,不可省 |
| `columnar_ms/<tbl>/MANIFEST` | 首次 flush | 每次 flush/compaction 追加(**不 fsync**,页缓存 write) | **1/表** | 🔴 结构性浪费,可全局化 |
| `wal/partition_N.wal` | 建库时**全部预建** | 按行哈希 `row_id % N` 分区写入;CHECKPOINT 截断 | 1/分区 | 🟡 可惰性化 |
| `columnar/`(旧引擎) | 建库时**无条件创建** | 仅 `TIMESERIES(ts)` 表(→ `<id>/seg_*.mcdb`) | 0 | 🟡 可惰性化 |
| `lsm/blobs/NNNN.blob` | LSM 初始化**即建首个** | 仅大值走 LSM blob(遗留路径) | 1 | 🟡 可惰性化 |
| `indexes/timestamp.idx` | 建库时**无条件创建** | 每次含 ts 的插入 | 1 | 🟡 可惰性化 |
| `catalog.bin`/`index_metadata.bin`/`lsn_counter`/`fts_indexes.fresh`/`.lock` | 建库时 | DDL/开关库/checkpoint | 0~1 | 🟡 可合并/隐藏 |
| `indexes/text_*.fts.d + .dict.d` | 每个 FTS 索引 | 文档写入 | 按需 | 🟡 6 文件/索引,可并目录 |

### 1.4 关键架构事实

- **双引擎并存**: Standard 表 → `columnar_ms/`(ColSegmentStore,权威);`TIMESERIES(ts)` 表 → 旧 `columnar/`(ColumnarStore,.mcdb)。空 `columnar/` 目录在纯 Standard 库中是死重。
- **compaction 有界**: 段数 ≥3 触发合并,稳态每表 1 个 .sst(20 表实测 20/20 都是 1)。
- **WAL 追加不阻塞在 fsync**: manifest.rs:104 注释明确 per-record 只 write+flush,持久化靠段文件 fsync + 孤儿领养;WAL 走 group-commit 后台批刷。

## 2. 实测: WAL 分区数对性能的影响

`examples/file_layout_bench.rs`,4 线程并发各写 4 万行 + 单表 16 万行,交错 5 轮:

| 配置 | 并发多表(rows/s) | 单表批量(rows/s) | close | reopen |
|---|---|---|---|---|
| partitions=1 (GroupCommit) | 500~584K | 443~509K | 198~293ms | 12~13ms |
| partitions=4 (GroupCommit) | 519~549K | 461~466K | 280~284ms | 14~17ms |
| partitions=1 (Synchronous) | 510~590K | 457~468K | 219~296ms | 6~7ms |
| partitions=4 (Synchronous) | 509~525K | 454~466K | 275~279ms | 6~6ms |

**结论: 两种持久化级别下,1 分区 vs 4 分区吞吐差异均在 ±5% 噪声内,close/reopen 同量级。** 理论上多分区提供并行 group-flush 和分锁,但实测瓶颈在段编码/写缓冲,WAL 路径不是热点。→ **分区文件惰性化(甚至默认 1)无性能代价**。

## 3. 收敛方案逐项评定

### 方案 A: 惰性创建 + 死文件清理(零风险,先行)

| 动作 | 省多少 | 性能影响 |
|---|---|---|
| WAL 分区文件惰性化(首次写入才 open+create) | −4 文件/−4 fd(零写入库) | 首写多一次 open(),µs 级;实测分区数无差异 |
| `columnar/` 仅在首个 TimeSeries 表 DDL 时创建引擎 | −1 目录(纯 Standard 库) | TS 表首用时一次性初始化;打开路径按 catalog 判断 |
| LSM blob 文件惰性化(首次 put 才 create) | −1 文件/−1 fd | 同上,µs 级 |
| `indexes/timestamp.idx` 仅首个 TIMESTAMP 列出现时创建 | −1 文件/−1 fd | 首次含 ts 插入多一次 create |
| `fts_indexes.fresh` → `.clean_close`(点前缀隐藏) | 观感 | 无 |
| `.lock` **保留不删**(修订) | 0 | flock 下 unlink 有双主竞争(第三方锁新 inode),RocksDB/LMDB 均保留锁文件;点前缀无观感成本 |

固定文件 11 → **5**(catalog/index_meta/lsn 三散 + clean 标记 + lock),零写入库的常驻 fd 6 → **1**(仅 lock)。

### 方案 B: 全局单一 MANIFEST(核心结构改动,收益最大)

现状: 每表 `columnar_ms/<tbl>/MANIFEST`,记录格式只有 segment id。改为: 全库一个 `columnar_ms/MANIFEST`,记录加 `table_id` 字段;段文件保持每表目录。

- **文件**: T 个 MANIFEST → 1 个。
- **fd**: T → 1(消除 fd 公式的线性项)。
- **性能**:
  - 追加路径: 全局 Mutex 串行化 ~50B 的 encode + BufWriter write + flush(**不 fsync**)。临界区 µs 级,而 flush 本身写 MB 级段数据 + fsync,ms 级 — 争用可忽略。
  - fsync 次数: 不变或略降(多表同 checkpoint 窗口共享一次 sync;`flush_buffer_durable` 的 per-flush sync 语义保留)。
  - 恢复/打开: 1 次 open + 顺序扫描 vs T 次 open — **更快**。
- **风险与缓解**: 单点损坏面变大,但 (a) 记录尾丢失本就由孤儿领养兜底(现有设计),(b) per-table 隔离是幻觉 — `catalog.bin` 损坏同样全库失联,(c) 迁移一次性: 打开时检测到旧 per-table MANIFEST → 读入 → 写全局 → 删旧。
- 工作量: ~1.5 天(含迁移 + 测试)。

### 方案 C: 根级元数据合并为一个 `meta.bin`

`catalog.bin` + `index_metadata.bin` + `lsn_counter` + 干净关闭标记 → 单文件分节(或头部 flag 字节复用干净关闭标记,消除 `.clean_close`)。

- **文件**: −3。
- **性能**: DDL 稀疏;lsn_counter 在 open/checkpoint/close 各写一次 — 3 次小文件写 → 1 次,**略优**。打开少读 2 个文件。
- 风险: 版本迁移(旧三文件 → 合并),一次性。

### 方案 D: FTS 索引目录并拢(可选)

`text_<name>.fts.d/` + `text_<name>.dict.d/` → 单个 `text_<name>.d/`。−1 目录/索引,纯改名迁移,无性能影响。

### 方案 E: TimeSeries 表统一到 columnar_ms(战略项,独立排期)

消灭旧 ColumnarStore 引擎,TS 表作为 (key,ts) 语义的 Standard 表存 columnar_ms。收益: 一套引擎一份维护、`columnar/` 彻底消失。工作量: 中(query_time_range 等专用路径映射到普通扫描)。**不阻塞 A~D**,做完后 A 中"columnar 惰性化"自然简化为删除。

### 方案 F: 单文件运行时格式 — **明确不做**

SQLite/DuckDB 的单文件需要自实现页分配器 + 原地 compaction,放弃 temp+fsync+rename 原子发布,增加写放大与碎片,mmap 局部性变差。这是产品定位不需要付出的复杂度。**替代**: `EXPORT DB TO 'snap.mote'`(tar 类打包 + 可选 zstd)作为分发/备份格式 — 面向机器人舰队同步场景,零运行时影响,单独立项。

## 4. 目标形态与汇总

```
db.mote/
├── .lock                      (干净关闭后删除)
├── meta.bin                   catalog + index_metadata + lsn + clean 标记
├── columnar_ms/
│   ├── MANIFEST               全局唯一
│   └── <table>/NNN.sst        数据(稳态每表 1 个)
├── indexes/                   用户索引 + FTS(全部按需)
└── wal/                       分区文件按需
```

| 指标 | 现状 | 目标 | 100 表外推对比 |
|---|---|---|---|
| 2 表库文件数 | 17 | **5~6** | — |
| 文件公式 | `11 + 2T + I` | `3 + T + I` | 211 → 103 |
| 固定 fd | 6 | **3~7 全局**(不再随表数线性) | 106 → ~5 |
| 空库文件 | 11 | **2~3** | — |

### 实施顺序建议

1. **P1(1 天,零风险)**: 方案 A 全部 — 立即消掉所有零字节残留文件。
2. **P2(1.5 天)**: 方案 B 全局 MANIFEST — 消掉 fd 线性项,顺带加快恢复。
3. **P3(0.5 天)**: 方案 C meta.bin 合并 + 方案 D FTS 并目录。
4. **P4(独立)**: 方案 E 引擎统一、EXPORT DB 单文件快照 — 按路线图另行排期。

每步都有旧格式读取兼容(惰性迁移),不需要停机或 dump/restore。

## 5. 结论

"十几个文件"的观感问题 80% 来自固定开销残留(方案 A 一天清零),20% 来自每表 MANIFEST(方案 B 消除)。数据本体的每表 1 个 .sst 不可也不应再压(方案 F 的代价不值得)。**全部 P1~P3 完成后,文件数 = 3 + 表数 + 索引数,fd 恒定个位数,且每一步都有实测/代码证据表明无性能回退** — 这才是"嵌入式、边缘部署、舰队分发"定位下该有的磁盘足迹。
