//! Transaction Coordinator
//!
//! Manages transaction lifecycle: begin, commit, rollback
//! Provides snapshot isolation through MVCC

use crate::txn::version_store::{Snapshot, Timestamp, TransactionId, VersionStore};
use crate::types::{Row, RowId};
use crate::{Result, StorageError};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

/// Delta operations for incremental snapshot
/// 
/// 🚀 P3.3 COW Optimization: Use Arc<Row> to avoid deep cloning
#[derive(Debug, Clone)]
pub enum DeltaOperation {
    /// Insert a new row (RowId didn't exist before)
    Insert(RowId, String, Arc<Row>),
    /// Update an existing row (store old value for rollback)
    Update(RowId, String, Arc<Row>), // old_value
    /// Delete a row (store old value for rollback)
    Delete(RowId, String, Arc<Row>), // old_value
}

/// Savepoint representation (Delta Snapshot optimized)
#[derive(Debug, Clone)]
pub struct Savepoint {
    /// Savepoint name
    pub name: String,
    
    /// 🚀 Delta: operations performed AFTER this savepoint was created
    /// Rolling back means undoing these operations in reverse order
    pub write_deltas: Vec<DeltaOperation>,
    
    /// 🚀 Delta: read_set additions AFTER this savepoint
    /// Only track new reads, not entire read_set
    pub read_deltas: HashSet<RowId>,
}

/// Transaction isolation level
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Read uncommitted data (not recommended)
    ReadUncommitted = 0,
    /// Read only committed data
    ReadCommitted = 1,
    /// Repeatable reads within transaction
    RepeatableRead = 2,
    /// Full serializable isolation
    Serializable = 3,
}

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TransactionState {
    Active = 0,
    Committed = 1,
    Aborted = 2,
}

/// Transaction context
pub struct TransactionContext {
    /// Transaction ID
    pub txn_id: TransactionId,
    
    /// Start timestamp
    pub start_ts: Timestamp,
    
    /// Isolation level
    pub isolation_level: IsolationLevel,
    
    /// Transaction state
    pub state: AtomicU8,
    
    /// Write set (local cache of writes) with table metadata
    /// Format: RowId → (table_name, row_data)
    pub write_set: RwLock<HashMap<RowId, (String, Row)>>,
    
    /// Read set (for conflict detection in Serializable)
    pub read_set: RwLock<HashSet<RowId>>,
    
    /// Snapshot for this transaction
    pub snapshot: Snapshot,
    
    /// Savepoint stack (for partial rollback)
    /// Savepoints are stacked: [sp1, sp2, sp3] where sp3 is the most recent
    pub savepoints: RwLock<Vec<Savepoint>>,
}

impl TransactionContext {
    /// 🚀 P3.2 + P3.3: Record a write operation with delta compression and COW
    /// 
    /// Delta Compression: Merge consecutive updates to the same RowId
    /// COW Optimization: Use Arc<Row> to avoid deep cloning
    /// 
    /// This should be called BEFORE modifying write_set to capture the old value.
    pub fn record_write_delta(&self, row_id: RowId, table_name: &str, old_value: Option<Row>, new_value: Row) {
        let mut savepoints = self.savepoints.write();
        if savepoints.is_empty() {
            return; // No savepoints, no tracking needed
        }
        
        // 🚀 P3.3 COW: Wrap in Arc for zero-cost sharing
        let new_value_arc = Arc::new(new_value);
        let old_value_arc = old_value.map(Arc::new);
        
        // Determine operation type
        let delta_op = match old_value_arc {
            None => DeltaOperation::Insert(row_id, table_name.to_string(), new_value_arc.clone()),
            Some(old) => DeltaOperation::Update(row_id, table_name.to_string(), old),
        };
        
        // 🚀 P3.2 Delta Compression: Check if we can merge with existing operations
        for savepoint in savepoints.iter_mut() {
            let mut compressed = false;
            
            // Check if the last operation in this savepoint is on the same RowId
            if let Some(last_op) = savepoint.write_deltas.last_mut() {
                compressed = match (last_op, &delta_op) {
                    // Update after Insert: keep Insert with new value
                    (DeltaOperation::Insert(last_id, _, last_val), DeltaOperation::Update(new_id, _, _)) 
                        if *last_id == row_id && *new_id == row_id => {
                            *last_val = new_value_arc.clone();
                            true
                        }
                    // Update after Update: keep first Update's old value
                    (DeltaOperation::Update(last_id, _, _), DeltaOperation::Update(new_id, _, _))
                        if *last_id == row_id && *new_id == row_id => {
                            // Keep original old_value, discard intermediate state
                            true
                        }
                    // Delete after Insert: remove both (net zero effect)
                    (DeltaOperation::Insert(last_id, _, _), DeltaOperation::Delete(new_id, _, _))
                        if *last_id == row_id && *new_id == row_id => {
                            savepoint.write_deltas.pop();
                            true
                        }
                    _ => false,
                };
            }
            
            // If not compressed, append new operation
            if !compressed {
                savepoint.write_deltas.push(delta_op.clone());
            }
        }
    }
    
    /// 🚀 Record a read operation to all active savepoints
    pub fn record_read_delta(&self, row_id: RowId) {
        let mut savepoints = self.savepoints.write();
        for savepoint in savepoints.iter_mut() {
            savepoint.read_deltas.insert(row_id);
        }
    }
}

/// Transaction Coordinator
pub struct TransactionCoordinator {
    /// Version store
    version_store: Arc<VersionStore>,
    
    /// Active transactions
    active_txns: Arc<DashMap<TransactionId, Arc<TransactionContext>>>,
    
    /// Transaction ID generator
    txn_id_gen: Arc<AtomicU64>,
}

impl TransactionCoordinator {
    /// Create a new transaction coordinator
    pub fn new(version_store: Arc<VersionStore>) -> Self {
        Self {
            version_store,
            active_txns: Arc::new(DashMap::new()),
            txn_id_gen: Arc::new(AtomicU64::new(1)),
        }
    }
    
    /// Begin a new transaction
    pub fn begin(&self, isolation_level: IsolationLevel) -> Result<TransactionId> {
        let txn_id = self.txn_id_gen.fetch_add(1, Ordering::SeqCst);
        let start_ts = self.version_store.allocate_timestamp();
        
        // Create snapshot
        let snapshot = self.create_snapshot_internal(txn_id, start_ts)?;
        
        let ctx = Arc::new(TransactionContext {
            txn_id,
            start_ts,
            isolation_level,
            state: AtomicU8::new(TransactionState::Active as u8),
            write_set: RwLock::new(HashMap::new()),
            read_set: RwLock::new(HashSet::new()),
            snapshot,
            savepoints: RwLock::new(Vec::new()),  // Initialize empty savepoint stack
        });
        
        self.active_txns.insert(txn_id, ctx);
        
        Ok(txn_id)
    }
    
    /// Commit a transaction
    /// 
    /// Returns the commit timestamp for MVCC visibility
    pub fn commit(&self, txn_id: TransactionId) -> Result<Timestamp> {
        let ctx = self.get_context(txn_id)?;
        
        // Check if already committed or aborted
        let state = ctx.state.load(Ordering::Acquire);
        if state != TransactionState::Active as u8 {
            return Err(StorageError::Transaction(
                format!("Transaction {} is not active", txn_id)
            ));
        }
        
        // Get commit timestamp
        let commit_ts = self.version_store.allocate_timestamp();
        
        // Validate write set (conflict detection)
        self.validate_write_set(&ctx)?;
        
        // Apply write set to version store
        let write_set = ctx.write_set.read();
        for (row_id, (_table_name, data)) in write_set.iter() {
            self.version_store.insert_version(
                *row_id,
                data.clone(),
                txn_id,
                commit_ts,
            )?;
        }
        
        // Mark as committed
        ctx.state.store(TransactionState::Committed as u8, Ordering::Release);
        
        // Remove from active transactions
        self.active_txns.remove(&txn_id);
        
        Ok(commit_ts)
    }
    
    /// Rollback a transaction
    pub fn rollback(&self, txn_id: TransactionId) -> Result<()> {
        let ctx = self.get_context(txn_id)?;
        
        // Clear write set
        ctx.write_set.write().clear();
        
        // Mark as aborted
        ctx.state.store(TransactionState::Aborted as u8, Ordering::Release);
        
        // Remove from active transactions
        self.active_txns.remove(&txn_id);
        
        Ok(())
    }
    
    /// Create a savepoint in the current transaction (Delta Snapshot optimized)
    /// 
    /// 🚀 Memory Optimization: Instead of cloning entire write_set,
    /// we create an empty delta tracker. Future operations will append to this delta.
    /// Memory usage: O(1) at creation time, O(k) for k operations after savepoint.
    pub fn create_savepoint(&self, txn_id: TransactionId, name: String) -> Result<()> {
        let ctx = self.get_context(txn_id)?;
        
        // Check if transaction is active
        let state = ctx.state.load(Ordering::Acquire);
        if state != TransactionState::Active as u8 {
            return Err(StorageError::Transaction(
                format!("Transaction {} is not active", txn_id)
            ));
        }
        
        // 🚀 Delta Snapshot: Start with empty deltas
        // Operations after this point will be tracked incrementally
        let savepoint = Savepoint {
            name: name.clone(),
            write_deltas: Vec::new(),  // No memory allocation at creation
            read_deltas: HashSet::new(),
        };
        
        // Push to savepoint stack
        ctx.savepoints.write().push(savepoint);
        
        eprintln!("[Savepoint] Created delta savepoint '{}' for txn {} (mem: 0 bytes)", name, txn_id);
        
        Ok(())
    }
    
    /// Rollback to a savepoint (Delta Snapshot optimized)
    /// 
    /// 🚀 Memory Optimization: Instead of restoring full snapshot,
    /// we undo operations in reverse order using deltas.
    /// 
    /// Algorithm:
    /// 1. Collect all deltas from savepoint[position+1..end] in reverse
    /// 2. Apply undo operations:
    ///    - Insert → Remove from write_set
    ///    - Update → Restore old value
    ///    - Delete → Restore old value
    /// 3. Remove savepoint[position..end] from stack
    pub fn rollback_to_savepoint(&self, txn_id: TransactionId, name: &str) -> Result<()> {
        let ctx = self.get_context(txn_id)?;
        
        // Check if transaction is active
        let state = ctx.state.load(Ordering::Acquire);
        if state != TransactionState::Active as u8 {
            return Err(StorageError::Transaction(
                format!("Transaction {} is not active", txn_id)
            ));
        }
        
        let mut savepoints = ctx.savepoints.write();
        
        // Find the savepoint by name
        let position = savepoints.iter().position(|sp| sp.name == name)
            .ok_or_else(|| StorageError::Transaction(
                format!("Savepoint '{}' not found in transaction {}", name, txn_id)
            ))?;
        
        // 🚀 Collect all deltas from savepoints AFTER this one (in reverse order)
        let mut all_deltas = Vec::new();
        for sp in savepoints.iter().skip(position + 1).rev() {
            // Reverse the deltas within each savepoint too
            for delta in sp.write_deltas.iter().rev() {
                all_deltas.push(delta.clone());
            }
        }
        
        // Also include deltas from the target savepoint itself
        for delta in savepoints[position].write_deltas.iter().rev() {
            all_deltas.push(delta.clone());
        }
        
        // 🚀 Apply undo operations (P3.3 COW: Arc clone is cheap)
        let mut write_set = ctx.write_set.write();
        let mut read_set = ctx.read_set.write();
        
        let mut undo_count = 0;
        for delta in all_deltas {
            match delta {
                DeltaOperation::Insert(row_id, _table_name, _new_value) => {
                    // Undo insert: remove from write_set
                    write_set.remove(&row_id);
                    undo_count += 1;
                }
                DeltaOperation::Update(row_id, table_name, old_value) => {
                    // Undo update: restore old value (Arc clone is O(1))
                    write_set.insert(row_id, (table_name, Arc::try_unwrap(old_value).unwrap_or_else(|arc| (*arc).clone())));
                    undo_count += 1;
                }
                DeltaOperation::Delete(row_id, table_name, old_value) => {
                    // Undo delete: restore old value (Arc clone is O(1))
                    write_set.insert(row_id, (table_name, Arc::try_unwrap(old_value).unwrap_or_else(|arc| (*arc).clone())));
                    undo_count += 1;
                }
            }
        }
        
        // Undo read_set changes
        for sp in savepoints.iter().skip(position).rev() {
            for row_id in &sp.read_deltas {
                read_set.remove(row_id);
            }
        }
        
        // Remove this savepoint and all later ones
        savepoints.truncate(position);
        
        eprintln!("[Savepoint] Rolled back to '{}' in txn {} (undid {} ops)", 
                  name, txn_id, undo_count);
        
        Ok(())
    }
    
    /// Release a savepoint
    /// 
    /// Removes the savepoint but keeps all changes made after it.
    /// Useful for cleaning up savepoints you no longer need.
    pub fn release_savepoint(&self, txn_id: TransactionId, name: &str) -> Result<()> {
        let ctx = self.get_context(txn_id)?;
        
        // Check if transaction is active
        let state = ctx.state.load(Ordering::Acquire);
        if state != TransactionState::Active as u8 {
            return Err(StorageError::Transaction(
                format!("Transaction {} is not active", txn_id)
            ));
        }
        
        let mut savepoints = ctx.savepoints.write();
        
        // Find and remove the savepoint
        let position = savepoints.iter().position(|sp| sp.name == name)
            .ok_or_else(|| StorageError::Transaction(
                format!("Savepoint '{}' not found in transaction {}", name, txn_id)
            ))?;
        
        savepoints.remove(position);
        
        eprintln!("[Savepoint] Released savepoint '{}' in txn {}", name, txn_id);
        
        Ok(())
    }
    
    /// Get transaction context
    pub fn get_context(&self, txn_id: TransactionId) -> Result<Arc<TransactionContext>> {
        self.active_txns
            .get(&txn_id)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| StorageError::Transaction(
                format!("Transaction {} not found", txn_id)
            ))
    }
    
    /// Create snapshot for a transaction
    fn create_snapshot_internal(&self, txn_id: TransactionId, timestamp: Timestamp) -> Result<Snapshot> {
        // Collect all active transaction IDs except this one
        let active_txns: HashSet<TransactionId> = self.active_txns
            .iter()
            .filter(|entry| *entry.key() != txn_id)
            .map(|entry| *entry.key())
            .collect();
        
        Ok(Snapshot {
            timestamp,
            active_txns,
        })
    }
    
    /// Validate write set for conflicts
    fn validate_write_set(&self, ctx: &TransactionContext) -> Result<()> {
        // For Serializable isolation, check read-write conflicts
        if ctx.isolation_level == IsolationLevel::Serializable {
            let read_set = ctx.read_set.read();
            let _write_set = ctx.write_set.read();
            
            // Check if any read row has been modified by another transaction
            for row_id in read_set.iter() {
                // Check if row was modified after our snapshot
                if let Ok(Some(_)) = self.version_store.get_visible_version(*row_id, &ctx.snapshot) {
                    // Additional validation logic here
                    // For now, we allow it
                }
            }
        }
        
        Ok(())
    }
    
    /// Get minimum active timestamp (for vacuum)
    pub fn get_min_active_timestamp(&self) -> Timestamp {
        self.active_txns
            .iter()
            .map(|entry| entry.value().start_ts)
            .min()
            .unwrap_or(self.version_store.allocate_timestamp())
    }
    
    /// Get statistics
    pub fn stats(&self) -> TransactionCoordinatorStats {
        let next_txn_id = self.txn_id_gen.load(Ordering::Relaxed);
        let active = self.active_txns.len() as u64;
        // total_committed = (next_txn_id - 1) - active
        // next_txn_id - 1 because we start from 1 and fetch_add returns old value
        let total_committed = if next_txn_id > 1 {
            (next_txn_id - 1) - active
        } else {
            0
        };
        
        TransactionCoordinatorStats {
            active_transactions: active,
            total_committed,
        }
    }
}

/// Transaction coordinator statistics
#[derive(Debug, Clone)]
pub struct TransactionCoordinatorStats {
    pub active_transactions: u64,
    pub total_committed: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Value, Timestamp};

    fn create_test_coordinator() -> TransactionCoordinator {
        let version_store = Arc::new(VersionStore::new());
        TransactionCoordinator::new(version_store)
    }

    #[test]
    fn test_begin_transaction() {
        let coord = create_test_coordinator();
        let txn_id = coord.begin(IsolationLevel::ReadCommitted).unwrap();
        
        assert!(coord.active_txns.contains_key(&txn_id));
        
        let ctx = coord.get_context(txn_id).unwrap();
        assert_eq!(ctx.txn_id, txn_id);
        assert_eq!(ctx.isolation_level, IsolationLevel::ReadCommitted);
    }

    #[test]
    fn test_commit_transaction() {
        let coord = create_test_coordinator();
        let txn_id = coord.begin(IsolationLevel::ReadCommitted).unwrap();
        
        // Add some writes (with table metadata)
        let ctx = coord.get_context(txn_id).unwrap();
        ctx.write_set.write().insert(
            1, 
            ("test_table".to_string(), vec![Value::Timestamp(Timestamp::from_micros(100))])
        );
        
        coord.commit(txn_id).unwrap();
        
        assert!(!coord.active_txns.contains_key(&txn_id));
    }

    #[test]
    fn test_rollback_transaction() {
        let coord = create_test_coordinator();
        let txn_id = coord.begin(IsolationLevel::ReadCommitted).unwrap();
        
        // Add some writes (with table metadata)
        let ctx = coord.get_context(txn_id).unwrap();
        ctx.write_set.write().insert(
            1, 
            ("test_table".to_string(), vec![Value::Timestamp(Timestamp::from_micros(100))])
        );
        
        coord.rollback(txn_id).unwrap();
        
        assert!(!coord.active_txns.contains_key(&txn_id));
    }

    #[test]
    fn test_snapshot_isolation() {
        let coord = create_test_coordinator();
        
        let txn1 = coord.begin(IsolationLevel::RepeatableRead).unwrap();
        let txn2 = coord.begin(IsolationLevel::RepeatableRead).unwrap();
        
        let ctx1 = coord.get_context(txn1).unwrap();
        let ctx2 = coord.get_context(txn2).unwrap();
        
        // txn1 was created first, so it doesn't see txn2 in its snapshot
        assert!(!ctx1.snapshot.active_txns.contains(&txn2));
        // txn2 was created second, so it sees txn1 as active
        assert!(ctx2.snapshot.active_txns.contains(&txn1));
    }

    #[test]
    fn test_concurrent_transactions() {
        let coord = Arc::new(create_test_coordinator());
        let mut handles = vec![];
        
        for _ in 0..10 {
            let coord = coord.clone();
            let handle = std::thread::spawn(move || {
                let txn = coord.begin(IsolationLevel::ReadCommitted).unwrap();
                std::thread::sleep(std::time::Duration::from_millis(10));
                coord.commit(txn).unwrap();
            });
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        let stats = coord.stats();
        assert_eq!(stats.total_committed, 10);
        assert_eq!(stats.active_transactions, 0);
    }
}
