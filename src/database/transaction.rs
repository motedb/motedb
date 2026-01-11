//! Transaction Operations (MVCC & Savepoints)
//!
//! Extracted from database_legacy.rs
//! Provides ACID transactions with snapshot isolation

use crate::database::core::MoteDB;
use crate::types::{Row, RowId, Value, Timestamp, PartitionId};
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
    /// 
    /// # Example
    /// ```ignore
    /// let txn = db.begin_transaction()?;
    /// db.insert_in_transaction(txn, 12345)?;
    /// db.commit_transaction(txn)?;
    /// ```
    pub fn begin_transaction(&self) -> Result<TransactionId> {
        self.txn_coordinator.begin(IsolationLevel::ReadCommitted)
    }

    /// Begin a transaction with specific isolation level
    /// 
    /// # Example
    /// ```ignore
    /// let txn = db.begin_transaction_with_isolation(IsolationLevel::Serializable)?;
    /// ```
    pub fn begin_transaction_with_isolation(&self, isolation: IsolationLevel) -> Result<TransactionId> {
        self.txn_coordinator.begin(isolation)
    }

    /// Insert a row within a transaction
    /// 
    /// # Example
    /// ```ignore
    /// let row_id = db.insert_in_transaction(txn, timestamp)?;
    /// ```
    pub fn insert_in_transaction(&self, txn_id: TransactionId, timestamp: i64) -> Result<RowId> {
        // 1. Allocate row ID
        let row_id = {
            let mut next_id = self.next_row_id.write();
            let id = *next_id;
            *next_id += 1;
            id
        };

        // 2. Build row data
        let row: Row = vec![Value::Timestamp(Timestamp::from_micros(timestamp))];

        // 3. Add to transaction's write set (with table metadata)
        let ctx = self.txn_coordinator.get_context(txn_id)?;
        
        // Delta Snapshot: Record write operation
        let old_value = ctx.write_set.read().get(&row_id).map(|(_, r)| r.clone());
        ctx.record_write_delta(row_id, "_default", old_value, row.clone());
        
        ctx.write_set.write().insert(row_id, ("_default".to_string(), row.clone()));

        // 4. Add to read set for conflict detection
        ctx.read_set.write().insert(row_id);
        ctx.record_read_delta(row_id);

        Ok(row_id)
    }
    
    /// Insert a row to table within a transaction (MVCC-aware)
    /// 
    /// # Flow:
    /// 1. Validate row against schema
    /// 2. Allocate row_id
    /// 3. Add to transaction's write_set (local cache, uncommitted)
    /// 4. On commit: WAL → Version Store → LSM → Index
    /// 
    /// # Example
    /// ```ignore
    /// let row = vec![Value::Text("Alice".into()), Value::Integer(30)];
    /// let row_id = db.insert_row_to_table_txn(txn, "users", row)?;
    /// ```
    pub fn insert_row_to_table_txn(
        &self,
        txn_id: TransactionId,
        table_name: &str,
        row: Row,
    ) -> Result<RowId> {
        // 1. Get table schema and validate
        let schema = self.table_registry.get_table(table_name)?;
        schema.validate_row(&row).map_err(|e| {
            StorageError::InvalidData(format!(
                "Row validation failed for table '{}': {}",
                table_name, e
            ))
        })?;
        
        // 2. Allocate row ID
        let row_id = {
            let mut next_id = self.next_row_id.write();
            let id = *next_id;
            *next_id += 1;
            id
        };
        
        // 3. Add to transaction's write set (with table_name metadata)
        let ctx = self.txn_coordinator.get_context(txn_id)?;
        
        // Delta Snapshot: Record write operation for active savepoints
        let old_value = ctx.write_set.read().get(&row_id).map(|(_, r)| r.clone());
        ctx.record_write_delta(row_id, table_name, old_value, row.clone());
        
        // Store (table_name, row) directly in write_set
        ctx.write_set.write().insert(row_id, (table_name.to_string(), row));
        
        // 4. Add to read set for conflict detection (Serializable isolation)
        ctx.read_set.write().insert(row_id);
        ctx.record_read_delta(row_id);
        
        Ok(row_id)
    }
    
    /// Query rows within a transaction (MVCC snapshot isolation)
    /// 
    /// Returns rows visible to this transaction's snapshot.
    /// 
    /// # Example
    /// ```ignore
    /// let row_ids = db.query_in_transaction(txn, 0, 1000000)?;
    /// ```
    pub fn query_in_transaction(&self, txn_id: TransactionId, start: i64, end: i64) -> Result<Vec<RowId>> {
        let ctx = self.txn_coordinator.get_context(txn_id)?;
        let snapshot = &ctx.snapshot;

        // 1. Get candidates from timestamp index
        let start_u64 = start as u64;
        let end_u64 = end as u64;
        let index_results = self.timestamp_index.read().range(&start_u64, &end_u64)?;
        let candidates: Vec<RowId> = index_results.into_iter().map(|(_, row_id)| row_id).collect();

        // 2. Filter by visibility
        let mut results = Vec::new();
        for row_id in candidates {
            // Check if visible in snapshot
            if let Ok(Some(_row)) = self.version_store.get_visible_version(row_id, snapshot) {
                results.push(row_id);
            }
        }

        // 3. Also check transaction's own write set
        let write_set = ctx.write_set.read();
        for (row_id, (_table_name, row_data)) in write_set.iter() {
            // Check if this row matches the timestamp range
            if let Some(Value::Timestamp(ts)) = row_data.first() {
                let ts_micros = ts.as_micros();
                if ts_micros >= start && ts_micros <= end {
                    // Avoid duplicates
                    if !results.contains(row_id) {
                        results.push(*row_id);
                    }
                }
            }
        }

        Ok(results)
    }
    
    /// Query table rows within a transaction (MVCC snapshot isolation)
    /// 
    /// Returns rows visible to this transaction's snapshot:
    /// - Committed rows (commit_ts < snapshot.timestamp && txn_id not in active_txns)
    /// - Uncommitted rows from THIS transaction (in write_set)
    /// 
    /// # Example
    /// ```ignore
    /// let rows = db.query_table_in_transaction(txn, "users")?;
    /// ```
    pub fn query_table_in_transaction(
        &self,
        txn_id: TransactionId,
        table_name: &str,
    ) -> Result<Vec<(RowId, Row)>> {
        let ctx = self.txn_coordinator.get_context(txn_id)?;
        let snapshot = &ctx.snapshot;
        
        // 1. Scan LSM for all rows in this table (committed data)
        let composite_prefix = self.compute_table_prefix(table_name);
        let mut results = Vec::new();
        
        // Use LSM scan_prefix to get committed rows
        let lsm_rows = self.lsm_engine.scan_prefix(composite_prefix)?;
        
        for (row_id, value) in lsm_rows {
            // MVCC visibility check: only visible if committed before snapshot
            if value.timestamp <= snapshot.timestamp {
                // Decode row data
                if let crate::storage::lsm::ValueData::Inline(data) = &value.data {
                    if let Ok(row) = Self::decode_row_data(data) {
                        results.push((row_id, row));
                    }
                }
            }
        }
        
        // 2. Get uncommitted data from transaction's write set
        let write_set = ctx.write_set.read();
        for (row_id, (tbl, row_data)) in write_set.iter() {
            // Check if this row belongs to target table
            if tbl == table_name {
                results.push((*row_id, row_data.clone()));
            }
        }
        
        Ok(results)
    }

    /// Commit a transaction (simplified version)
    /// 
    /// # Example
    /// ```ignore
    /// db.commit_transaction(txn)?;
    /// ```
    pub fn commit_transaction(&self, txn_id: TransactionId) -> Result<()> {
        let ctx = self.txn_coordinator.get_context(txn_id)?;
        let write_set = ctx.write_set.read().clone();

        // 1. Write to WAL (now with table_name from write_set)
        for (row_id, (table_name, row_data)) in &write_set {
            let partition = (*row_id % self.num_partitions as u64) as PartitionId;
            self.wal.log_insert(table_name, partition, *row_id, row_data.clone())?;
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
    
    /// Commit a transaction with full LSM+Index integration (ACID-compliant)
    /// 
    /// # Flow:
    /// 1. WAL logging (durability)
    /// 2. Transaction validation (conflict detection)
    /// 3. Version Store commit (MVCC visibility)
    /// 4. LSM Engine write (persistent storage)
    /// 5. Index building (triggered by LSM flush callback)
    /// 
    /// # Example
    /// ```ignore
    /// db.commit_transaction_full(txn)?;
    /// ```
    pub fn commit_transaction_full(&self, txn_id: TransactionId) -> Result<()> {
        let ctx = self.txn_coordinator.get_context(txn_id)?;
        let write_set = ctx.write_set.read().clone();
        
        if write_set.is_empty() {
            // Empty transaction, just mark as committed
            let _commit_ts = self.txn_coordinator.commit(txn_id)?;
            return Ok(());
        }
        
        // Step 1: WAL logging (durability first)
        for (row_id, (table_name, row_data)) in &write_set {
            let partition = (*row_id % self.num_partitions as u64) as PartitionId;
            self.wal.log_insert(table_name, partition, *row_id, row_data.clone())?;
        }
        
        // Step 2: Transaction validation & Version Store commit
        let commit_ts = self.txn_coordinator.commit(txn_id)?;
        
        // Step 3: Write to LSM Engine (persistent storage)
        for (row_id, (table_name, row_data)) in &write_set {
            // Serialize and write to LSM
            let row_bytes = bincode::serialize(&row_data)?;
            let value = crate::storage::lsm::Value::new(row_bytes, commit_ts);
            let composite_key = self.make_composite_key(table_name, *row_id);
            
            self.lsm_engine.put(composite_key, value)?;
        }
        
        // Step 4: Update in-memory indexes (Primary Key only for real-time queries)
        for (row_id, (table_name, row_data)) in &write_set {
            // Only update PRIMARY KEY index immediately
            if let Ok(schema) = self.table_registry.get_table(table_name) {
                if let Some(pk_col) = schema.primary_key() {
                    let index_name = format!("{}.{}", table_name, pk_col);
                    if self.column_indexes.contains_key(&index_name) {
                        if let Some(col_def) = schema.columns.iter().find(|c| c.name == pk_col) {
                            if let Some(value) = row_data.get(col_def.position) {
                                let _ = self.insert_column_value(table_name, pk_col, *row_id, value);
                            }
                        }
                    }
                }
            }
            
            // Update timestamp index if present
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
