# 事务管理 (Transactions)

MoteDB 采用 MVCC + WAL 架构，提供 BEGIN / COMMIT / ROLLBACK / SAVEPOINT/ RELEASE 等完整能力，默认所有 SQL 语句都在隐式事务中执行，可通过 API 获得更细粒度控制。

## 快速示例

```rust
use motedb::Database;

let db = Database::open("bank.mote")?;
let tx = db.begin_transaction()?;

db.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")?;
db.execute("UPDATE accounts SET balance = balance + 100 WHERE id = 2")?;

db.commit_transaction(tx)?;
```

## API 对照

| 功能 | SQL | Rust API |
|------|-----|----------|
| 开始事务 | `BEGIN` | `db.begin_transaction()` |
| 提交事务 | `COMMIT` | `db.commit_transaction(tx_id)` |
| 回滚事务 | `ROLLBACK` | `db.rollback_transaction(tx_id)` |
| 保存点 | `SAVEPOINT sp1` | `db.savepoint(tx_id, "sp1")` |
| 回滚到保存点 | `ROLLBACK TO sp1` | `db.rollback_to_savepoint(tx_id, "sp1")` |
| 释放保存点 | `RELEASE sp1` | `db.release_savepoint(tx_id, "sp1")` |

## 保存点示例

```rust
let tx = db.begin_transaction()?;

db.execute("INSERT INTO orders VALUES (1001, 'Alice')")?;
db.savepoint(tx, "after_alice")?;

db.execute("INSERT INTO orders VALUES (1002, 'Bob')")?;
db.rollback_to_savepoint(tx, "after_alice")?; // 仅撤销 Bob

db.commit_transaction(tx)?; // Alice 保留
```

## 自动提交模式

- 默认每条 SQL 自动提交
- 使用 `BEGIN` 或 API `begin_transaction()` 进入显式事务
- Drop `Database` 时会自动 `flush()`，未提交的事务将回滚

## 并发与隔离

- MVCC：读操作不会阻塞写操作
- 写-写冲突：第二个事务在 `commit` 时检测版本冲突并回滚
- 建议配合 `transaction_stats()` 监控活跃事务与冲突率

```rust
let stats = db.transaction_stats();
println!("active={} committed={} aborted={}"
    , stats.active_transactions
    , stats.total_committed
    , stats.total_aborted);
```

## WAL & Checkpoint

- 事务提交 → WAL 先落盘 → LSM 合并 → Checkpoint
- 对性能敏感场景可在 `DBConfig` 中将 `durability_level` 调整为 `Memory`
- 生产环境推荐 `DurabilityLevel::Full + enable_wal = true`

## 性能建议

- 批量写入放在同一个事务（减少 WAL flush）
- 只在需要时使用保存点，避免过多元数据
- Long-running 查询建议在副本或快照上执行

## 故障排查

| 问题 | 解决方案 |
|------|----------|
| 事务提交慢 | 调整 `memtable_size_mb`、批量 flush、检查磁盘 IO |
| 经常回滚 | 监控 `stats.total_aborted`，优化冲突热点列 |
| 恢复失败 | 确认 WAL 目录权限 / 空间充足，执行 `db.execute("CHECKPOINT")` |

---

- 上一篇：[04 批量操作](./04-batch-operations.md)
- 下一篇：[06 索引概览](./06-indexes-overview.md)
