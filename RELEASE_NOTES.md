# MoteDB v0.15.0

四条主线:**多语言 SDK 生态**(Node.js / Go + npm 五平台预构建流水线)、
**存储文件布局收敛**(P1~P4 + 只读容器模式)、**TimeSeries 引擎统一**
(方案 E)、以及 **F1/F2 性能战役与三轮发布前审计**(共 10 项修复)。
核心 crate / Python / Node 版本对齐 0.15.0。

## 一、存储文件布局收敛 — "采集是目录, 运输是一个文件"

具身智能数据生命周期(采集→运输→训练→部署)对磁盘形态的要求不同:采集期
要断电安全与高性能(目录形态的强项),运输期要单文件可拷贝/校验/续传,
部署期要只读打开。本版按此定位收敛运行时布局,并补齐运输与部署形态。

### 数字总账(v0.14 → v0.15)

| 指标 | v0.14 | v0.15 |
|---|---|---|
| 空库固定文件 | 11 | **4**(.lock / meta.bin / columnar_ms/MANIFEST) |
| 50 表库文件 | ~111 | ~55(= 3+表数+索引数) |
| 50 表运行时 fd | 56+ | **个位数**(50 表进程总 fd 实测 <40) |
| 打开耗时(50 表×60 轮 churn) | 77ms,随历史线性恶化 | **17.6ms**,有界常量(F1) |
| manifest 体积 | 无上限线性增长 | **有界常量**(F1 compaction) |
| 脏 checkpoint 50 表 | ~42ms | **24.6ms 平坦**(F2) |
| 运输形态 | 目录拷贝 | **单文件 .mote 快照**(可压缩/校验) |
| 部署形态 | 无 | **open('snap.mote') 只读直开** |
| 断电语义 | — | 未动(temp+rename/孤儿领养/mmap) |
| 旧库升级 | — | 打开即自动迁移(每阶段前向兼容) |

- **P1 惰性创建**: WAL 分区首写物化、CHECKPOINT 删文件替代截断、
  columnar//LSM blob/timestamp.idx 全部按需 — 零字节残留消失
- **P2 全局单一 MANIFEST**: 记录携带 table_id,全库一个文件 — fd 线性项
  消除;旧 per-table 布局打开即迁移(崩溃安全幂等)
- **P3 meta.bin 四合一**: catalog/index 元数据/LSN/关闭旗标并入分节容器
  (每节 CRC32);catalog 节损坏 fail-fast 硬错误
- **P4 EXPORT DB TO 单文件快照 (MOTESNAP)**: 流式条目+每文件 CRC32+
  逐文件 zstd;静默纪律复用 backup_to 全套;五语言接线
- **只读容器模式**: `Database::open_readonly('snap.mote')` 及 open()
  自动路由 — 幂等解包缓存(多进程竞争安全)、引擎级只读门(全部写路径
  拒绝)、多读者共存、源容器字节不变

## 二、TimeSeries 引擎统一 (方案 E)

TIMESERIES(ts) 表与标准表同一条写/读/恢复路径,旧 ColumnarStore 引擎
退出运行时(仅留只读迁移):executor 10 处 TS 专属路由臂删除(净 -1226
行),WAL 回放并入标准路径,.mcdb 数据打开时一次性迁移(崩溃升级窗口
经审计修复:迁移先于回放+next_row_id 推进),TTL/DELETE 时间窗重写为
段存储 tombstone 版。实测 TS 与标准表 checkpoint 同价(18.2ms)。

顺带修复两个被旧路径掩盖的存量 bug:物化读回 TIMESTAMP 变 Integer;
**BTree superblock 内联页偏移表超 4096 保留区 — 任何 10 万行 ts-first
表 close 即炸**(v3: 64KB 保留 + 降级阀 + 空载自愈)。

## 三、性能战役 (F1/F2)

- **F1 全局 MANIFEST 增量状态 + 有界 compaction**: 打开时解析一次(原
  每表全量重解析);文件超阈值最简重写;退役 id 按段文件存在性裁剪 —
  打开 77ms→17.6ms(4.4×),manifest 210KB→17KB
- **F2 checkpoint 三项省略型优化**: 计数器脏标记/LSN+catalog 合并单
  重写/manifest 空转跳 fsync — 50 表 42→24.6ms 表数平坦化,零持久化
  语义变化

## 四、v0.13 已知缺口关闭

BLOB 在 int 过滤查询下的双缺陷(装载门只匹配 Text tag + 输出臂缺 Blob)
— 修复后裸投影/LENGTH/混合列/重开/ORDER BY 全部正确。

## 五、三轮发布前审计(10 项修复)

- **轮 1(新代码自身)**: BTree read_superblock EOF 容忍;execute_prepared
  绕过只读门;Python 构造器绕过容器自动路由;create+read_only 拒绝
- **轮 2(升级/并发窗口)**: v2 索引读兼容(曾升级即静默清空全部列索引);
  崩溃升级迁移序(曾丢 .mcdb 已 checkpoint 行);容器解包竞争(原子
  rename 舞步 + 同进程临时目录唯一化)
- **轮 3(派生数据自愈)**: 空载列索引打开时从源重建(降级阀/未来静默
  空路径自愈);容器陈旧替换链;孤儿 legacy 目录清扫

## 六、SDK cycle

- **Node.js SDK**(napi-rs v3): execute(?-params)/query/checkpoint/
  backupTo/exportSnapshot/openSnapshot/openReadonly/close/doctor
- **Go SDK**(cgo): Open/Exec/Query/Checkpoint/BackupTo/ExportSnapshot/
  ImportSnapshot;go test 7 项
- **Python**: export_to/import_snapshot + 构造器容器自动路由;e2e×3
- **npm 预构建流水线**: 五平台矩阵,tag 触发发布(需 NPM_TOKEN)
- **C ABI**: db_open/db_exec(JSON 参数化)/checkpoint/backup/db_export/
  db_import/last_error

## 升级与兼容

- 老库(v0.12~v0.14)直接打开:惰性迁移自动完成,无需停机;v2 索引文件
  保持可读,空/损坏索引自动从源重建
- `backup_to`(目录快照)与 `EXPORT DB`(单文件)并存;容器模式覆盖
  部署形态

## 已知限制

- npm 发布流水线待 NPM_TOKEN 配置后重跑
- timestamp 索引覆盖 ts 首列表;非首列 ts 的 query_timestamp_range
  走扫描(与 v0.14 相同)
