//! Transaction Operations (MVCC & Savepoints)
//!
//! Provides ACID transactions with snapshot isolation.
//! Transactional writes are buffered in the coordinator's write_set until commit,
//! then flushed to WAL and LSM atomically. Rollback simply discards the write_set.

use std::sync::Arc;

use crate::database::core::MoteDB;
use crate::types::{Row, RowId, Value, PartitionId};
use crate::txn::IsolationLevel;
use crate::{Result, StorageError};

type TransactionId = u64;

/// Transaction statistics
#[derive(Debug, Clone)]
pub struct TransactionStats {
    pub active_transactions: u64,
    pub total_committed: u64,
    pub total_versions: u64,
    pub total_rows_with_versions: u64,
    pub avg_versions_per_row: f64,
}

impl MoteDB {
    /// Begin a transaction with default isolation level (Read Committed)
    pub fn begin_transaction(&self) -> Result<TransactionId> {
        ensure_open!(self);
        let txn_id = self.txn_coordinator.begin(IsolationLevel::ReadCommitted)?;
        self.wal.log_begin(0, txn_id, 0)?;
        Ok(txn_id)
    }

    /// Insert a row within a transaction. The row is buffered in the coordinator's
    /// write_set and NOT written to LSM until commit. This is the transaction-safe
    /// alternative to `insert_row_to_table`.
    pub fn insert_row_with_txn(
        &self,
        table_name: &str,
        txn_id: TransactionId,
        mut row: Row,
    ) -> Result<RowId> {
        ensure_open!(self);
        let schema = self.table_registry.get_table(table_name)?;

        // Ensure row has enough slots for AUTO_INCREMENT PK column before validation
        if schema.is_primary_key_auto_increment() {
            if let Some(pk_col) = schema.primary_key().and_then(|n| schema.get_column(n)) {
                while row.len() <= pk_col.position {
                    row.push(Value::Null);
                }
            }
        }

        // Validate row against schema (before allocating ID to avoid waste on failure)
        schema.validate_row(&row).map_err(|e| {
            StorageError::InvalidData(format!("Row validation failed: {}", e))
        })?;

        // Primary key uniqueness check (same as non-transactional path)
        if !schema.is_primary_key_auto_increment() {
            if let Some(pk_name) = schema.primary_key() {
                if let Some(pk_col) = schema.get_column(pk_name) {
                    if let Some(pk_value) = row.get(pk_col.position) {
                        let pk_key = crate::database::pk_cache::PkKey::from_value(pk_value);
                        let exists_in_cache = self.pk_lookup.get(table_name)
                            .map(|lookup| lookup.get_pk(&pk_key).is_some())
                            .unwrap_or(false);
                        if exists_in_cache {
                            return Err(StorageError::InvalidData(format!(
                                "Duplicate primary key {:?} for table '{}'", pk_value, table_name
                            )));
                        }
                        if let Ok(found) = self.query_by_column(table_name, pk_name, pk_value) {
                            if !found.is_empty() {
                                let mut has_live = false;
                                for &rid in &found {
                                    if self.get_table_row(table_name, rid)?.is_some() {
                                        has_live = true;
                                        break;
                                    }
                                }
                                if has_live {
                                    return Err(StorageError::InvalidData(format!(
                                        "Duplicate primary key {:?} for table '{}'", pk_value, table_name
                                    )));
                                }
                            }
                        }
                    }
                }
            }
        }

        // Allocate row_id
        let row_id = if schema.is_primary_key_auto_increment() {
            let counter = self.table_auto_increment
                .entry(table_name.to_string())
                .or_insert_with(|| {
                    Arc::new(std::sync::atomic::AtomicI64::new(
                        schema.get_auto_increment_start(),
                    ))
                })
                .value()
                .clone();
            let id = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if !(0..=i64::MAX - 1000).contains(&id) {
                return Err(StorageError::AutoIncrementOverflow(table_name.to_string()));
            }
            if let Some(pk_col) = schema.primary_key().and_then(|n| schema.get_column(n)) {
                while row.len() <= pk_col.position { row.push(Value::Null); }
                row[pk_col.position] = Value::Integer(id);
            }
            id as RowId
        } else {
            self.next_row_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        };

        // Add to transaction write_set — NOT written to WAL or LSM yet
        let ctx = self.txn_coordinator.get_context(txn_id)?;
        let mut write_set = ctx.write_set.write();
        write_set.insert(row_id, (table_name.to_string(), row.clone()));

        // Record delta for savepoint rollback
        let _ = self.txn_coordinator.record_write_delta(txn_id,
            crate::txn::coordinator::DeltaOperation::Insert(row_id, table_name.to_string(), Arc::new(row)));
        drop(write_set);

        Ok(row_id)
    }

    /// Commit a transaction. Flushes buffered writes to WAL, coordinator (MVCC), and LSM.
    pub fn commit_transaction(&self, txn_id: TransactionId) -> Result<()> {
        ensure_open!(self);
        let ctx = self.txn_coordinator.get_context(txn_id)?;
        let write_set = ctx.write_set.read().clone();

        if write_set.is_empty() {
            // Nothing to commit — still finalize
            self.txn_coordinator.commit(txn_id)?;
            return Ok(());
        }

        // 1. Commit in transaction coordinator FIRST (MVCC validation).
        //    If this fails, nothing is written to WAL — no orphaned records.
        let commit_ts = self.txn_coordinator.commit(txn_id)?;

        // 2. Write each row to WAL (coordinator already committed)
        for (row_id, (table_name, row_data)) in &write_set {
            let partition = (*row_id % self.num_partitions as u64) as PartitionId;
            self.wal.log_insert(table_name, partition, *row_id, row_data.clone(), txn_id)?;
        }

        // 3. Write WAL Commit record
        self.wal.log_commit(0, txn_id, commit_ts)?;

        // 4. Flush all rows to LSM atomically via batch_put
        let mut kvs: Vec<(u64, crate::storage::lsm::Value)> = Vec::with_capacity(write_set.len());
        for (row_id, (table_name, row_data)) in &write_set {
            let composite_key = self.make_composite_key(table_name, *row_id);
            let tbl_schema = self.table_registry.get_table(table_name)?;
            let col_types = tbl_schema.col_types();
            let raw = crate::storage::row_format::encode(row_data, col_types)
                .or_else(|_| bincode::serialize(row_data)
                    .map_err(|e| StorageError::Serialization(format!("Row encode failed: {}", e))))?;
            let ts = self.write_lsn.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            kvs.push((composite_key, crate::storage::lsm::Value::new(raw, ts)));
        }
        // Atomic LSM write — all rows or none
        self.lsm_engine.batch_put(&kvs)?;

        // Now that LSM is updated, populate caches + ColSegmentStore
        for (row_id, (table_name, row_data)) in &write_set {
            self.row_cache.put(table_name.to_string(), *row_id, row_data.clone());

            // Write committed row to ColSegmentStore (so SELECT sees it).
            if self.col_segment_stores.contains_key(table_name) {
                if let Ok(store) = self.get_or_create_col_segment_store(table_name, vec![]) {
                    let table_id = self.table_registry.get_table_id(table_name).unwrap_or(0) as u64;
                    let key = (table_id << 32) | (row_id & 0xFFFFFFFF);
                    let ts = self.write_lsn.load(std::sync::atomic::Ordering::Relaxed);
                    store.append_rows(&[(key, ts, row_data.clone())])?;
                }
            }

            let tbl_schema = self.table_registry.get_table(table_name)?;
            if let Some(pk_name) = tbl_schema.primary_key() {
                if let Some(pk_col) = tbl_schema.get_column(pk_name) {
                    if let Some(pk_value) = row_data.get(pk_col.position) {
                        let pk_key = crate::database::pk_cache::PkKey::from_value(pk_value);
                        if let Some(lookup) = self.pk_lookup.get(table_name) {
                            lookup.insert(pk_key, *row_id);
                        }
                    }
                }
            }

            self.table_row_count
                .entry(table_name.to_string())
                .or_insert_with(|| Arc::new(std::sync::atomic::AtomicU64::new(0)))
                .value()
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        // 5. Update timestamp index — only if schema has a designated timeseries column
        for (row_id, (table_name, row_data)) in &write_set {
            let tbl_schema = self.table_registry.get_table(table_name)?;
            if let Some(ref ts_col_name) = tbl_schema.timeseries_column {
                if let Some(ts_col) = tbl_schema.get_column(ts_col_name) {
                    if let Some(Value::Timestamp(ts)) = row_data.get(ts_col.position) {
                        self.timestamp_index.write()
                            .insert(ts.as_micros_u64(), *row_id)?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Rollback a transaction. Discards all buffered writes.
    pub fn rollback_transaction(&self, txn_id: TransactionId) -> Result<()> {
        ensure_open!(self);
        self.wal.log_rollback(0, txn_id)?;
        self.txn_coordinator.rollback(txn_id)
    }

    /// Get transaction statistics
    pub fn transaction_stats(&self) -> TransactionStats {
        let coord_stats = self.txn_coordinator.stats();
        let version_stats = self.version_store.stats();

        TransactionStats {
            active_transactions: coord_stats.active_transactions,
            total_committed: coord_stats.total_committed,
            total_versions: version_stats.total_versions,
            total_rows_with_versions: version_stats.total_rows,
            avg_versions_per_row: version_stats.avg_versions_per_row,
        }
    }

    // ==================== Savepoint API ====================

    /// Create a savepoint within the current transaction
    pub fn create_savepoint(&self, txn_id: TransactionId, name: String) -> Result<()> {
        self.txn_coordinator.create_savepoint(txn_id, name)
    }

    /// Rollback to a savepoint, discarding all changes after it
    pub fn rollback_to_savepoint(&self, txn_id: TransactionId, name: &str) -> Result<()> {
        self.txn_coordinator.rollback_to_savepoint(txn_id, name)
    }

    /// Release a savepoint without rolling back
    pub fn release_savepoint(&self, txn_id: TransactionId, name: &str) -> Result<()> {
        self.txn_coordinator.release_savepoint(txn_id, name)
    }
}
