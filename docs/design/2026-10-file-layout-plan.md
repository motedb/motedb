# 存储文件布局收敛 — 实施方案

- 日期: 2026-10-13
- 前置: [2026-10-file-layout.md](./2026-10-file-layout.md)(评定)及单文件格式分析
- 状态: **P1 已实施并验收**(惰性创建 + 死文件清理;tests/test_file_layout.rs 5/5,全量回归无 FAILED,clippy --all-targets 0 警告);P2~P4 待开工
- 定位依据: 运行时=收敛目录格式,运输/分发=单文件快照(EXPORT),部署=只读容器(远期)

## 0. 总览

| 阶段 | 内容 | 文件数变化 | fd 变化 | 工作量 | 风险 |
|---|---|---|---|---|---|
| P1 | 惰性创建 + 死文件清理 | 固定 11→5 | 固定 6→1 | 1 天 | 低 |
| P2 | 全局单一 MANIFEST | 每表 −1 | **−T(线性项消除)** | 1.5~2 天 | 中 |
| P3 | meta.bin 四合一 | −3 | 0 | 0.5~1 天 | 中(catalog 持久化) |
| P4 | EXPORT/IMPORT 单文件快照 | 新能力 | — | 1~1.5 天 | 低 |

目标态: 文件 `3 + 表数 + 索引数`,固定 fd 个位数,断电语义不变(不触碰 temp+fsync+rename、孤儿领养、per-segment mmap 三个机制)。

版本落点: 若 SDK 周期已发 v0.15.0,本方案为 **v0.16.0**(P1+P2) + **v0.16.x/v0.17**(P3+P4);否则 P1+P2 并入 v0.15.0。

---

## 1. P1 — 惰性创建与死文件清理

原则: **只推迟"文件的创建时机",不改变任何写入/恢复语义**。每项都是"首次使用时多一次 open()/create(),µs 级"。

### 1.1 WAL 分区文件惰性化

- 现状: `WALManager::create_with_config`(src/txn/wal.rs:1414)与 `open_with_config`(:1456)对 `0..num_partitions` **全部**创建/打开 `partition_N.wal`;`PartitionWAL.file: BufWriter<File>` 非空即持 fd。
- 改动:
  - `PartitionWAL.file` → `Option<BufWriter<File>>`;`create_with_config` 只构造路径与内存状态,不碰磁盘。
  - `open_with_config` 语义不变(存在的文件必须打开,恢复要读);**不存在时不再创建**——置 None。
  - 首次 append 时物化(open with create)。物化点在 `PartitionWAL` 的 append 入口,一行 `get_or_insert_with`。
  - `wal/` 目录仍在建库时创建(它是 `resolve_open_path` 的合法库判定标志,core.rs:400)。
- 兼容: 恢复扫描只遍历实际存在的分区文件,无影响;checkpoint/截断对 None 分区为 no-op。
- 验收: 纯 DDL+插入后关库,`wal/` 内文件数 = 实际收到写入的分区数;打开零写入库后 `wal/` 为空目录。

### 1.2 旧 ColumnarStore 目录惰性化

- 现状: `ColumnarStore::create`(src/storage/columnar/store.rs:91)`create_dir_all(base_dir)`;core.rs create(:491)/open(:1331) 无条件构造。仅 `TIMESERIES(ts)` 表真正写 `columnar/<table_id>/seg_*.mcdb`。
- 改动: 去掉 create 路径的 `create_dir_all(base_dir)`;目录创建下沉到 per-table 注册/首写(store.rs:609 已是 per-table `create_dir_all`,保留)。open 路径本就只在目录存在时读。
- 效果: 纯 Standard 库无 `columnar/` 目录。
- 兼容: TimeSeries 表首次注册时目录才出现,查询路径按内存 store 走,无行为差异。

### 1.3 LSM blob 文件惰性化

- 现状: `BlobStore::new`(src/storage/lsm/blobstore.rs:63)初始化即 `create_dir_all(blobs/)` + `BlobFile::create` 首个 blob。
- 改动: `current_file: Option<BlobFile>`;`put()` 首次调用时创建目录与文件。`lsm/` 目录保留(合法库判定 + lost+found)。
- 兼容: 现役写入 LSM blob 的路径仅有遗留大值路径;恢复逻辑按存在文件工作。

### 1.4 timestamp.idx 惰性化

- 现状: create_with_config 无条件建 `indexes/` + `BTree::with_config(timestamp.idx)`(core.rs:440-447)。
- 改动: BTree 构造延迟到"首个含 TIMESTAMP 列的表被创建或打开时"(catalog 加载后可判定);DbState 中字段改 `OnceLock<Arc<RwLock<BTree>>>` 或惰性 Option + 初始化锁。`indexes/` 目录随之惰性。
- 兼容: `index_row_timestamp` 调用点在初始化之后必然可达(有 ts 列才有调用)。

### 1.5 标记文件改名 + .lock 决策(修订)

- `fts_indexes.fresh` → `.clean_close`(点前缀隐藏)。打开时兼容: 若 `.clean_close` 不存在而旧名存在 → 视为干净并删除旧名。写入点 core.rs:3202(temp+rename 原子盖章,保留)。
- **`.lock` 保留,不做关闭删除**(修订评定稿): flock 语义下 unlink 存在经典竞争——A 关库删文件时,第三方 C 新建同名文件锁新 inode,与仍在持锁的 B 形成"双主"。RocksDB/LMDB 均保留锁文件。它是点前缀文件,观感成本为零。

P1 验收测试: 新增 `tests/test_file_layout.rs::p1_lazy_files` — DDL+插入+checkpoint+close 后断言精确文件集合;`ls -a` 级别断言无零字节残留。

---

## 2. P2 — 全局单一 MANIFEST(核心)

### 2.1 格式 v2

现记录(src/storage/col_segment/manifest.rs:28):`AddSegment(u64)`、`Compaction{new_id, old_ids}`。v2 增加 `table_id: u32`(registry 的 table_id 正是 u32,与 `columnar/<id>/` 目录同源):

```
header: MAGIC | VERSION=2 | record_count u32
tag 1: AddSegment   { table_id u32, seg_id u64 }
tag 2: Compaction   { table_id u32, new_id u64, old_count u32, old_ids u64.. }
tag 3: DropTable    { table_id u32 }              ← 新增: 替代 DROP TABLE 删整个 manifest 文件
```

文件: `columnar_ms/MANIFEST`(每库一个)。段文件命名/目录不变(每表目录内 `{:010}.sst`,id 只需表内唯一)。

### 2.2 组件改动

- `Manifest` → `Arc<GlobalManifest>`(内含 `Mutex<Manifest>`),在 Database create/open 时构造,注入 `get_or_create_col_segment_store`(crud.rs:419 等调用点,签名加一句柄或收进构造上下文)。
- `ColSegmentStore` 增持 `table_id: u32`;`add_segment/record_compaction` 追加时带上;**追加仍不 fsync**(页缓存 write,现有孤儿领养契约不变,manifest.rs:104)。
- `drop_all`(DROP TABLE): 从"删 manifest 文件"改为追加 `DropTable` 记录 + 删段文件(sync 点后物理删)。
- `sync_manifest`(dataset pinning GC,store.rs:4832): 删除判定仍按段 id + gc_filter;全局 manifest 的 sync 语义不变。
- 恢复: Database::open 一次读全局 manifest → `HashMap<table_id, Vec<seg_id>>` → 各表 store 构造时领走;`recover_from_disk` 的孤儿收编逻辑不变(目录内完整 .sst 未见于 manifest → 收编)。

### 2.3 一次性迁移(崩溃安全)

open 时序:
1. `columnar_ms/MANIFEST` 不存在 && 存在 per-table MANIFEST → 汇总写全局(temp+fsync+rename)。
2. 全局写成功后删除各旧 MANIFEST。中断重入幂等: 全局已存在而旧文件残留 → 直接删旧。
3. 不可能出现"全局缺失而部分旧文件已删"(先写后删的顺序保证)。

### 2.4 并发与性能论证(已实测背书)

- 全局锁只串行化 ~50B encode + BufWriter write(无 fsync),µs 级;flush 本身 ms 级(段数据 + fsync),争用可忽略。
- fsync 次数不变或略降(多表同 checkpoint 窗口合并为一次 sync)。
- WAL 分区数无关吞吐的实测(file_layout_bench)佐证: 写路径瓶颈不在元数据小写入。

### 2.5 P2 验收测试

- 双表并发 flush,manifest 记录交错正确(每表段集与单表一致)。
- 迁移: 用 v0.14 布局 fixture(手工构造 per-table manifest)打开 → 数据完整、旧文件删除、全局存在。
- 迁移中断: 截断全局 manifest 模拟 → 重开收编孤儿,行数不变。
- DROP TABLE 后重开: 表不存在、段文件删除、其他表不受影响。
- dataset pinning + VACUUM 回归(test_datasets.rs 全绿)。

---

## 3. P3 — meta.bin 四合一

### 3.1 格式

```
b"META1" | version u16 | flags u8 (bit0=clean_close) | section_count u16
每节: kind u8 | len u32 | crc32 u32 | payload
  kind 1 = catalog   (TableRegistry 序列化,内部格式不变)
  kind 2 = index_meta(IndexRegistry 序列化,内部格式不变)
  kind 3 = lsn       (u64)
```

写入纪律: 整文件 temp+fsync+rename 重写(文件 KB 级);所有写点收敛到单一 `Mutex<MetaStore>`。clean_close 标志: open 时置 0(重写一次),close 置 1——取代 `.clean_close`/`fts_indexes.fresh` 标记(语义照搬 core.rs:906/3202 的盖章逻辑)。

### 3.2 迁移

meta.bin 不存在而旧三文件存在 → 读旧 → 写 meta.bin → 删旧。lsn_counter 在 open/checkpoint/close 的持久化点(persistence.rs:621)改为整文件重写(净写次数 3→1,略优)。

### 3.3 风险与测试

- 风险集中在 catalog 持久化路径: 保持 payload 字节不变(仅换容器),`tests/test_schema_persistence` 类用例全量回归 + 迁移 fixture 用例。
- DDL 与 checkpoint 并发写 meta 的锁纪律测试(现有 is_flushing/checkpoint_mutex 纪律复用)。

---

## 4. P4 — EXPORT/IMPORT 单文件快照

### 4.1 容器格式 `MOTESNAP`

```
b"MOTESNAP" | version u16 | flags u16 (bit0=zstd) | entry_count u32
TOC: entry{ rel_path: u16 len + bytes | size u64 | crc32 u32 } × N
DATA: 按 TOC 顺序的(可选 zstd)文件字节
```

- 打包范围: `columnar_ms/**`、`indexes/**`(含 FTS 目录)、catalog/index 元数据;**排除** `.lock`、`wal/*`、`*.tmp`、clean 标记(导入方重置)。EXPORT 前强制 checkpoint(静默纪律直接复用 backup_to 的 is_flushing CAS + checkpoint_mutex,persistence.rs:76 起,v0.12.9 已打磨)。
- 校验: 每文件 crc32 + 导入时整体验证;`doctor` 增加 snapshot 校验模式。

### 4.2 接口面

- SQL: `EXPORT DB TO 'snap.mote' [COMPRESSED]`(运行库内执行)。
- API/FFI: `Database::import_snapshot(src, dest_dir)`(创建新库,静态方法);Python `motedb.import_snapshot`、FFI `motedb_db_import`、Node/Go 各加一函数。
- IMPORT 实现: 解包到目标目录 → 正常 open(走 P2/P3 的恢复与领养)。远期只读模式: `open('snap.mote')` 检测到文件而非目录 → 解包到缓存目录或直接只读 mmap(与 EXPORT 共用代码,独立排期)。

### 4.3 验收测试

- 往返: N 表(含 VECTOR/BLOB/FTS/dataset)EXPORT → IMPORT → 行数/向量召回/FTS MATCH 全等。
- 压缩开关、损坏文件(翻转字节)导入报 crc 错误。
- 大库(≥1GB)EXPORT 内存有界(流式拷贝,不整读)。

---

## 5. 总验收门

1. `tests/test_file_layout.rs`: T 表 + I 索引库,断言文件集合精确 = `3 + T + I`(P3 后);fd 断言: 50 表打开,`/dev/fd` 计数 < 20。
2. 全量回归: `cargo test --release` 全绿(重点 test_datasets/test_fts_recovery_staleness/test_blob_type)。
3. 性能门: `file_layout_bench` P50 与基线差 < ±10%(基线数据在评定稿 §2)。
4. 双零: `cargo clippy --all-targets` 与 `cargo check --all-targets` 零警告。
5. 迁移矩阵: v0.14 布局(per-table manifest + 三散文件)→ 新版本打开 → 数据完整 → 重开后为新布局。

## 6. 风险与回滚

- 每阶段独立成 commit,前向迁移 + 永久旧格式读兼容;最坏回滚 = revert 后旧版本仍能读(新格式只增文件不删语义)。
- P2 是唯一动恢复路径的阶段: 迁移代码路径单独测试类覆盖;上线前用 corruption_fuzz/differential_fuzz 各跑一轮。
- P3 若 catalog 合并出问题可单独 revert(P1/P2/P4 不依赖它)。

## 7. 实施顺序

P1(1d) → P2(2d) → P3(1d) → P4(1.5d + bindings),合计 ~5.5 天。P1 与 P2 可同版本发布;P4 的 bindings 面(Node/Go/Python/FFI)在 SDK 流水线上各加一个 smoke 用例。
