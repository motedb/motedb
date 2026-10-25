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

## 🔴 已知未修复: 并发双行转账漂移 (round-8, 发版阻断项)

3 写线程 × 双语句事务 (`UPDATE a-1; UPDATE b+1`) 高争用下, ~330 次成功
提交静默丢 1-3 个净单位 (单边转账丢弃)。判别: agg-SUM == row-SUM ==
checkpoint+重开后同值 → 真持久丢失; 单行自增形状精确零丢失。复现器:
`tests/test_concurrent_transfer_drift.rs` (`--ignored` 运行, 已知失败)。
同期已修的两个相关根因: ① WAL 数据记录曾后于 Commit 标记 — auto-
checkpoint 回放窗口吞掉已标记事务的数据 (已重排为数据先于标记);
② `may_have_duplicate_keys` 检查-快照竞态致聚合瞬间 ×2/×3 双计
(已加快照后复查)。剩余根因待查方向: 双语句水位线间隔内的提交应用序。
