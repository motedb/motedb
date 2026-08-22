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
