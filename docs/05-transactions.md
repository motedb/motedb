# Transaction Management

MoteDB uses an MVCC + WAL architecture, providing full support for BEGIN / COMMIT / ROLLBACK / SAVEPOINT / RELEASE. By default, all SQL statements execute within implicit transactions. Finer-grained control is available through the API.

## Quick Example

```rust
use motedb::Database;

let db = Database::open("bank.mote")?;
let tx = db.begin_transaction()?;

db.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")?;
db.execute("UPDATE accounts SET balance = balance + 100 WHERE id = 2")?;

db.commit_transaction(tx)?;
```

## Semantics: writes during an active transaction

Statements executed on the same session/database handle **while a transaction is
active join that transaction** (standard SQL behavior):

- `ROLLBACK` undoes all writes made since `BEGIN` — including statements issued
  through `db.execute(...)` without an explicit transaction parameter.
- `ROLLBACK TO SAVEPOINT sp` undoes writes made after `sp` was created; the
  commit afterwards keeps only the pre-savepoint writes.
- Writes made **before** `BEGIN` (plain auto-commit statements) are already
  durable and are never affected by a later rollback.

> Historical note: releases before 0.9.1 had auto-commit writes bypass MVCC
> (they survived rollback). That leak was fixed; the atomicity tests in
> `tests/test_acid_comprehensive.rs` now lock this behavior down.

## API Reference

| Feature | SQL | Rust API |
|------|-----|----------|
| Begin transaction | `BEGIN` | `db.begin_transaction()` |
| Commit transaction | `COMMIT` | `db.commit_transaction(tx_id)` |
| Rollback transaction | `ROLLBACK` | `db.rollback_transaction(tx_id)` |
| Savepoint | `SAVEPOINT sp1` | `db.savepoint(tx_id, "sp1")` |
| Rollback to savepoint | `ROLLBACK TO sp1` | `db.rollback_to_savepoint(tx_id, "sp1")` |
| Release savepoint | `RELEASE sp1` | `db.release_savepoint(tx_id, "sp1")` |

## Savepoint Example

```rust
let tx = db.begin_transaction()?;

db.execute("INSERT INTO orders VALUES (1001, 'Alice')")?;
db.savepoint(tx, "after_alice")?;

db.execute("INSERT INTO orders VALUES (1002, 'Bob')")?;
db.rollback_to_savepoint(tx, "after_alice")?; // Only undo Bob

db.commit_transaction(tx)?; // Alice is retained
```

## Auto-commit Mode

- Each SQL statement is automatically committed by default
- Use `BEGIN` or the API `begin_transaction()` to enter an explicit transaction
- Dropping the `Database` instance triggers an automatic `flush()`; uncommitted transactions will be rolled back

## Concurrency and Isolation

- MVCC: Read operations do not block write operations
- Write-write conflicts: The second transaction detects a version conflict at `commit` time and rolls back
- It is recommended to use `transaction_stats()` to monitor active transactions and conflict rates

```rust
let stats = db.transaction_stats();
println!("active={} committed={} aborted={}"
    , stats.active_transactions
    , stats.total_committed
    , stats.total_aborted);
```

## WAL & Checkpoint

- Transaction commit -> WAL writes to disk first -> LSM compaction -> Checkpoint
- For latency-sensitive scenarios, set `durability_level` to `Memory` in `DBConfig`
- For production environments, `DurabilityLevel::Full + enable_wal = true` is recommended

## Performance Recommendations

- Group bulk writes in a single transaction (reduces WAL flushes)
- Use savepoints only when needed to avoid excessive metadata overhead
- For long-running queries, consider executing on a replica or snapshot

## Troubleshooting

| Problem | Solution |
|------|----------|
| Slow transaction commits | Adjust `memtable_size_mb`, batch flush, check disk I/O |
| Frequent rollbacks | Monitor `stats.total_aborted`, optimize conflict hot-spot columns |
| Recovery failure | Verify WAL directory permissions / available disk space, run `db.execute("CHECKPOINT")` |

---

- Previous: [04 Batch Operations](./04-batch-operations.md)
- Next: [06 Index Overview](./06-indexes-overview.md)


## 并发写-写: first-committer-wins (2026-10-24 修复)

并发显式事务更新同一行时, 后提交者若在它**读到该行之后**有并发提交落
地, COMMIT 返回冲突错误 (`Write-write conflict … retry`), 应用侧回滚
重试整笔事务 (银行转账语义)。修复前: 并发 `BEGIN; UPDATE v=v+1;
COMMIT` 双线程循环丢失约一半增量且**无任何报错** (2000 次成功提交 →
1000 落盘, checkpoint+重开仍 1000 — 写路径真丢失)。

```sql
-- 应用侧重试模式
loop {
    BEGIN;
    SELECT bal INTO :a FROM acct WHERE id = :x;  -- 读
    UPDATE acct SET bal = :a - :amt WHERE id = :x;
    COMMIT;  -- 冲突 → 错误 → ROLLBACK 后重试整笔
}
```

实现要点: 行级提交戳注册表 (与快照同源 version_store 时钟) + 语句
起始水位线锚定 + 提交校验/应用全程与 autocommit 写同把锁; 事务内
UPDATE 的 PK 快路径行读窄持锁以关闭提交 validate→apply 间隙的脏读
窗口。autocommit 语句从未丢过更新 (整语句原子)。

**case 说明**: `tests/test_concurrent_txn_ww.rs` — 并发增量零丢失
(含 checkpoint+重开持久)、autocommit 并发精确、银行转账守恒/无负
余额/重开保持。
**已知边界**: 事务内非 PK 条件的全扫描 UPDATE 仅水位线保护 (窗口更
宽, 冲突率略高但无丢失); 冲突在紧乒乓循环下重试率可观 (~每 2 次成
功 1 次冲突), 生产负载间隔下可忽略。

## ✅ 并发双行转账漂移 (round-9 已归零, 2026-10-23)

3 写线程 × 双语句事务 (`UPDATE a-1; UPDATE b+1`) × 2 SUM 读线程高争用下,
历史表现: ~330-400 次成功提交静默丢 1-5 个净单位, 且 **checkpoint+重开
后仍丢失** (真持久损坏, 曾是发版阻断项)。round-9 通过三层探针实证
(commit 时 append 后立即可见性、语句时基准值 vs store 最新值、
append/flush/merge 全链路追踪) 定位出**三层叠加根因**, 全部修复:

1. **flush 不可见窗口** (`store.rs::flush_buffer_locked`): 旧协议先把
   write_buf 整体换出 → 写段文件 (磁盘 I/O) → 发布段, I/O 期间行在
   "buf 已清空 + 段未发布" 的双盲区, 并发点查返回旧段值。探针证据:
   `@@COMMIT-APPLY-INVISIBLE` 124 次/轮。新协议: 锁内 `snapshot()` →
   锁外写文件 → 锁内 `drain_prefix(n)` + 段发布 + 计数更新原子完成;
   点查 (`get`/`get_projected`/`get_projected_multi`) 在 write_buf 锁内
   完成 [buf 检查 + 段列表快照], 行永远恰好在一个视图。
2. **row_cache 投毒** (`row_cache.rs` + 读路径回填点): 读线程 cache miss
   后无条件 `put` 回填, 与提交者 (write_lock 内 put 新值) 交错时把旧值
   覆盖回去; 后续事务以毒值为 UPDATE 基准, 而语句水位线晚于冲突提交戳,
   first-committer-wins **验证无法察觉** (这是漂移逃过 round-7 校验的
   根本原因)。修复: 读路径回填全部改 `put_if_absent` (分片锁内原子),
   提交者保持覆盖式 put。
3. **合并集陈旧快照** (`store.rs::compact_once`/`force_compact_all`/
   `compact_small_tail`): 在 flush_merge_lock **之外**快照段列表, 快照
   与拿锁之间并发 flush 发布的新段 (含最新版本) 不在合并集; 合并输出
   (旧数据) 被 push 到列表**尾部**, 点查 `.rev()` 首先命中旧值遮蔽新值,
   后续合并的 rank 去重再把被遮蔽的新版本**永久丢弃** — 持久丢失的
   直接来源 (探针版本链转储实锤: 新版本 ts 更高却在任何段中不存在)。
   修复: 三个入口全部改为锁内快照; D2 合并同 key 胜者从纯 rank 改为
   `(timestamp, rank)` (时间戳来自单调 write_lsn, 纵深防御)。

**case 说明**: `tests/test_concurrent_transfer_drift.rs` — 8 秒高争用
(3 写 + 2 读) 转账守恒, 含 CHECKPOINT+重开断言; 曾 `#[ignore]` 已知失败,
round-9 修复后解禁并通过。`write_buf` 计数协议同步修复 (append/drain
都在锁内更新, 杜绝计数与缓冲状态漂移)。

**顺带发现的更早缺陷**: flush 旧协议的注释自己记录过 "row in neither
view" 曾导致 `ORDER BY` 偶发 0 行 — 同根因, 一并消除。

## ✅ round-10: 并发可见性四连修 (2026-10-23)

第 9 轮之后的新一轮并发审计, 四个独立缺陷 (各自的 case 见下):

1. **事务 apply 撕裂读**: commit 的存储突变逐行 append (每行独立
   write_buf 临界区), 不持 write_lock 的读者 flush 可在**两行之间**落盘
   → 段里短暂存在**半个事务** — 向量化 M1 聚合观察到 `SUM = 3001`
   (转账 +1 侧先落盘)。修复: `append_txn_ops` 把整个事务按表批量进
   **单一临界区**; WAL 记录顺序同步改为 [删除 → 插入 → 更新]。
2. **聚合快照漏 buf 行**: `count_filtered`/`aggregate_filtered`/
   `aggregate_multi_filtered` 先 flush 再只扫段 — 两次调用之间提交的行
   在 buffer 里, COUNT/SUM 看不见 (COUNT(*) 可回退)。修复:
   `consistent_agg_view` 在 write_buf 锁内 [快照段 + 收集 buf 最新行],
   buf 行 (必然新于段内同 key) 先折叠、其 key (含墓碑) 抑制段版本。
3. **overlap 标志丢失**: 无锁 `note_overlapping_write` 与全量合并的
   [空检查 + 清标志] 交错 → 重叠行 flush 后标志为 false → 多段扫描
   跳过去重 → SUM/COUNT 双计。修复: `append_rows_marked`/
   `append_row_ref_marked`/`append_tombstone` 把 [置标志 + 追加] 原子化,
   合并清标志也入锁内。
4. **事务内删除后重插同 PK**: 缓冲 DELETE 未提交时存储行 + pk_lookup
   映射仍在, 重插被误报 `Duplicate primary key` (标准 SQL 合法)。
   Integer PK 重插落**同一复合键** — WAL/batch 顺序必须 [墓碑 → 重插],
   否则 newest-wins (buf rposition / merge max-ts / 重放顺序) 会吞掉
   重插行。修复: 存储层查重对本事务 pending delete 放行 + 全链路顺序
   统一。

另加 row_cache **删除墓碑标记** (读者回填与 invalidate 竞争时不再复活
已删行 — 窗口极窄难以稳定复现, 以确定性终态压力测试 + 标记机制双保险)。

**case 说明**:
- `tests/test_concurrent_agg_consistency.rs` — 转账负载下**每一次** SUM
  观察精确守恒 (撕裂读/双计/漏行任一都会打破); 插入负载下 COUNT(*)/
  SUM 单调不减 + 终态等于已提交数。
- `tests/test_txn_delete_reinsert.rs` — 删除→重插 (含 链式
  delete→insert→update、真重复仍拒绝、回滚恢复原值、CHECKPOINT+重开
  持久性)。
- `tests/test_row_cache_delete_ghost.rs` — 删除+重插风暴后终态逐 PK
  点查必须全空 (缓存幽灵判定)。

**已知边界 (记录在案)**: 向量化 M1 聚合入口仍是 [flush → 段快照] —
事务原子化后它只会漏**整个**事务 (不再撕裂), 对守恒负载不可观测;
纯插入负载的 COUNT/SUM 已路由到修复后的聚合路径。跨表多行事务的
apply 以表为单位原子 (跨表撕裂窗口仍存在, 需要全局应用序才能消除)。

## round-11: 审计无新缺陷 + 三处对等加固 (2026-10-26)

对 round-9/10 新代码的复审轮。**结论: 未发现新的可观测缺陷** — 三个
审计方向全部健全, 产出为对等性加固与回归锁定:

1. **buf_value_matches 补 Timestamp/Date** (round-10 引入的缺口):
   `Value::Timestamp`/`Value::Date` 此前落入 `_ => false`。审计发现该
   缺口实际**不可观测** — 聚合入口的内部 flush 在快照前执行, 任何
   「查询开始前已提交」的行必然先落段 (即 round-10 的 buf-fold 本身
   也是纵深防御而非可观测修复; 真正可观测的修复是事务原子 apply)。
   Timestamp 按微秒、Date 按天数纳入数值比较 (与段侧 i64 语义对等),
   `fold_value_into_agg` 同步对等 — 保证未来若移除内部 flush 或窗口
   变宽时语义一致。
   🔑 附带的方法论教训: 单调性断言抓不到「漏已提交行」类缺陷 (段内
   已可见的行永不回退, buf 内的行从未可见 — 漏行表现为滞后而非回退);
   正确断言是 `c0 = committed 读取 → SELECT → 结果 ≥ c0`。
   (test_concurrent_agg_consistency 的 ts 变体即此断言。)
2. **事务 read-your-writes 全面验证为正确**: 聚合 (COUNT/SUM, 含
   WHERE 过滤) 与 PK 点查对事务自己的 buffered INSERT/UPDATE/DELETE
   全部立即可见, ROLLBACK 完全还原 — 探针测试原以为是缺陷的两处
   「失败」均为测试自身的期望值算错。行为以
   `tests/test_txn_read_your_writes.rs` 锁定。
3. **drain_prefix 退化条目单测** (round-9 flush 协议): NULL 向量
   (dim=0 → 2 字节)、空文本 (len=0 → 4 字节)、NULL 文本、交错形状的
   前缀解析逐字节精确 — 一字节错位即静默串列 (数据损坏非报错)。
