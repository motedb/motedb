# MoteDB v0.15.0

两条主线:**多语言 SDK 生态**(Node.js / Go + npm 五平台预构建流水线)与
**存储文件布局收敛**(P1~P4, docs/design/2026-10-file-layout-plan.md)。
核心 crate / Python / Node 版本对齐 0.15.0。

## 一、存储文件布局收敛 — "采集是目录, 运输是一个文件"

具身智能数据生命周期(采集→运输→训练→部署)对磁盘形态的要求不同:采集期
要断电安全与高性能(目录形态的强项),运输期要单文件可拷贝/校验/续传。
本版按此定位收敛运行时布局,并补上运输形态。

### 数字总账(v0.14 → v0.15)

| 指标 | v0.14 | v0.15 |
|---|---|---|
| 空库固定文件 | 11 | **4**(.lock / meta.bin / columnar_ms/MANIFEST) |
| 50 表库文件 | ~111 | ~55(= 3+表数+索引数) |
| 50 表运行时 fd | 56+ | **个位数**(50 表进程总 fd 实测 <40) |
| 运输形态 | 目录拷贝 | **单文件 .mote 快照**(可压缩/校验) |
| 断电语义 | — | 未动(temp+rename / 孤儿领养 / per-segment mmap) |
| 旧库升级 | — | 打开即自动迁移(每阶段前向兼容,无需停机) |

### P1 惰性创建 + 死文件清理

WAL 分区文件首写才物化,CHECKPOINT 从"截断成 0 字节壳"改为删除(缺失
文件与空文件在恢复语义下等价);纯 Standard 库不再创建空的旧引擎目录
`columnar/`;LSM blob 首写物化;`indexes/timestamp.idx` 惰性化(无
TIMESTAMP 列的库永不创建 `indexes/`);关闭标记改点前缀 `.clean_close`。
零字节残留文件全部消失。

### P2 全局单一 MANIFEST

原来每表一个 MANIFEST 文件 + 一个常驻 fd — 100 张表就是 100 个 fd,
直逼嵌入式 256 fd 软限。v2 记录携带 table_id,全库一个
`columnar_ms/MANIFEST`:fd 线性项消除,恢复只读一个文件(更快)。
v0.14 旧布局打开即自动迁移(先写全局后删旧文件,崩溃安全重入幂等);
manifest 尾部损坏的孤儿领养语义原样保留。

### P3 meta.bin 四合一

catalog / index 元数据 / LSN / 干净关闭旗标并入一个分节容器(每节
CRC32),任一节更新整文件原子重写,DDL 与 checkpoint 的元数据写单锁
串行化。**catalog 节损坏 fail-fast 硬错误**——绝不以空注册表静默打开,
杜绝新写覆盖可恢复备份(对齐旧版 corrupt catalog 行为)。

### P4 EXPORT DB TO 单文件快照 (MOTESNAP)

```sql
EXPORT DB TO 'fleet.mote' [COMPRESSED]
```

顺序流式条目 + 每文件 CRC32(对逻辑字节,压缩与否检测一致)+ 逐文件
zstd,1MiB 缓冲内存有界,适合 GB 级库;导出静默纪律复用 backup_to
全套(CAS + checkpoint_mutex + 全写条纹 + WAL 截断),期间提交过的数据
保证入档。导入带路径穿越防护与逐条校验,产物即最终布局。
五语言接线:Rust `Database::export_snapshot/open_snapshot` · C ABI
`motedb_db_export/motedb_db_import` · Python `db.export_to()/
motedb.import_snapshot()` · Node `exportSnapshot/openSnapshot` ·
Go `ExportSnapshot/ImportSnapshot`。

### 验证

- `tests/test_file_layout.rs`(5)· `test_global_manifest.rs`(8)·
  `test_snapshot.rs`(5)· `test_corruption_recovery.rs`(4):迁移/幂等/
  截断领养/DROP 隔离/损坏拒绝/多模态往返(VECTOR top-k · BLOB 全等 ·
  FTS MATCH · dataset 记录)
- 全量回归零 FAILED;clippy --all-targets / rustc 双零告警
- 同机 A/B(`examples/file_layout_bench`,worktree 对照 v0.14):
  并发与单表吞吐零回退(差值均在噪声区间)

## 二、SDK cycle

- **Node.js SDK**(bindings/node, napi-rs v3):Database — create-or-open /
  execute(?-params)/ query / checkpoint / backupTo / close / doctor;
  本版新增 exportSnapshot / openSnapshot。值映射含 VECTOR / BLOB(base64)/
  TIMESTAMP(µs)
- **Go SDK**(bindings/go, cgo):Open/Close/Exec/Query/Checkpoint/BackupTo,
  本版新增 ExportSnapshot / ImportSnapshot;JSON 边界 `{"$blob": base64}`
  出入对称;go test 6 项(CRUD/类型/重开/EPISODES/ASOF/backup/快照)
- **Python**:本版新增 `db.export_to(dest, compressed)` 与模块级
  `motedb.import_snapshot(src, dest)`(e2e 冒烟入库)
- **npm 预构建流水线**(.github/workflows/node-publish.yml):五平台矩阵
  (macOS arm64/x64 · Linux x64/arm64-gnu · Windows x64),tag 触发发布。
  ⚠️ 发布前需配置 NPM_TOKEN secret
- **C ABI 扩展**(src/ffi.rs):motedb_db_open/db_exec/db_checkpoint/
  db_backup/db_free_string/last_error + 本版 db_export/db_import

## 升级与兼容

- 老库(v0.12~v0.14)直接用新版本打开:惰性迁移自动完成,无需停机或
  dump/restore;迁移崩溃安全、重入幂等
- 旧的 `fts_indexes.fresh` 标记、per-table MANIFEST、catalog.bin 等
  打开时一次性消费
- `backup_to`(目录快照)与 `EXPORT DB`(单文件快照)并存,语义不同:
  前者产物是可直接打开的目录,后者是运输/归档用的单文件

## 已知限制

- TimeSeries 表(`TIMESERIES(ts)`)仍由旧 ColumnarStore 引擎服务
  (`columnar/` 目录,惰性创建)——引擎统一(方案 E)按路线图另排
- 只读容器模式(`open('snap.mote')` 直接 mmap)未实现;当前以
  import_snapshot 解包后打开代替
- npm 发布流水线待 NPM_TOKEN 配置
