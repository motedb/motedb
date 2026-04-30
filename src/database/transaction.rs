//! Transaction Operations (MVCC & Savepoints)
//!
//! Extracted from database_legacy.rs
//! Provides ACID transactions with snapshot isolation

use crate::database::core::MoteDB;
use crate::types::{Row, RowId, Value, Timestamp, PartitionId};
use crate::txn::IsolationLevel;
use crate::Result;

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
    ///
    /// # Example
    /// ```ignore
    /// let txn = db.begin_transaction()?;
    /// db.commit_transaction(txn)?;
    /// ```
    pub fn begin_transaction(&self) -> Result<TransactionId> {
        ensure_open!(self);
        self.txn_coordinator.begin(IsolationLevel::ReadCommitted)
    }

    /// Commit a transaction (simplified version)
    /// 
    /// # Example
    /// ```ignore
    /// db.commit_transaction(txn)?;
    /// ```
    pub fn commit_transaction(&self, txn_id: TransactionId) -> Result<()> {
        ensure_open!(self);
        let ctx = self.txn_coordinator.get_context(txn_id)?;
        let write_set = ctx.write_set.read().clone();

        // 1. Write to WAL (now with table_name from write_set)
        for (row_id, (table_name, row_data)) in &write_set {
            let partition = (*row_id % self.num_partitions as u64) as PartitionId;
            self.wal.log_insert(table_name, partition, *row_id, row_data.clone(), txn_id)?;
        }

        // 2. Commit in transaction coordinator (applies to version store)
        let _commit_ts = self.txn_coordinator.commit(txn_id)?;

        // 3. Update indexes
        for (row_id, (_table_name, row_data)) in &write_set {
            // Update timestamp index
            if let Some(Value::Timestamp(ts)) = row_data.first() {
                self.timestamp_index.write().insert(ts.as_micros() as u64, *row_id)?;
            }
        }

        Ok(())
    }

    /// Rollback a transaction
    /// 
    /// # Example
    /// ```ignore
    /// db.rollback_transaction(txn)?;
    /// ```
    pub fn rollback_transaction(&self, txn_id: TransactionId) -> Result<()> {
        ensure_open!(self);
        self.txn_coordinator.rollback(txn_id)
    }
    
    /// Get transaction statistics
    /// 
    /// # Example
    /// ```ignore
    /// let stats = db.transaction_stats();
    /// println!("Active: {}, Committed: {}", stats.active_transactions, stats.total_committed);
    /// ```
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
    /// 
    /// # Example
    /// ```ignore
    /// let txn = db.begin_transaction()?;
    /// db.insert_row_to_table_txn(txn, "users", row1)?;
    /// db.create_savepoint(txn, "sp1".to_string())?;
    /// db.insert_row_to_table_txn(txn, "users", row2)?;
    /// db.rollback_to_savepoint(txn, "sp1")?; // row2 rolled back, row1 kept
    /// db.commit_transaction_full(txn)?; // only row1 committed
    /// ```
    pub fn create_savepoint(&self, txn_id: TransactionId, name: String) -> Result<()> {
        self.txn_coordinator.create_savepoint(txn_id, name)
    }
    
    /// Rollback to a savepoint, discarding all changes after it
    /// 
    /// This removes the savepoint and all savepoints created after it.
    pub fn rollback_to_savepoint(&self, txn_id: TransactionId, name: &str) -> Result<()> {
        self.txn_coordinator.rollback_to_savepoint(txn_id, name)
    }
    
    /// Release a savepoint without rolling back
    /// 
    /// Useful for cleaning up savepoints after critical sections complete successfully.
    pub fn release_savepoint(&self, txn_id: TransactionId, name: &str) -> Result<()> {
        self.txn_coordinator.release_savepoint(txn_id, name)
    }
}
