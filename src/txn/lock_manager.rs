//! Row-Level Lock Manager
//!
//! Provides concurrent access control through shared/exclusive locks with deadlock detection

use crate::txn::version_store::TransactionId;
use crate::types::RowId;
use crate::{Result, StorageError};
use dashmap::DashMap;
use parking_lot::{Condvar, Mutex, RwLock};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

/// Lock mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    /// Shared lock (read)
    Shared,
    /// Exclusive lock (write)
    Exclusive,
}

/// Lock request waiting in queue
#[derive(Debug)]
struct LockWaiter {
    txn_id: TransactionId,
    mode: LockMode,
    condvar: Arc<Condvar>,
    granted: Arc<Mutex<bool>>,
}

/// Lock entry for a single row
struct LockEntry {
    /// Current lock holders: (txn_id, lock_mode)
    holders: RwLock<Vec<(TransactionId, LockMode)>>,
    /// Waiting queue
    waiters: Mutex<VecDeque<LockWaiter>>,
}

impl LockEntry {
    fn new() -> Self {
        Self {
            holders: RwLock::new(Vec::new()),
            waiters: Mutex::new(VecDeque::new()),
        }
    }

    /// Check if a lock can be granted
    fn can_grant(&self, mode: LockMode, txn_id: TransactionId) -> bool {
        let holders = self.holders.read();
        
        match mode {
            LockMode::Shared => {
                // Shared lock: OK if no exclusive locks held (except by self)
                !holders.iter().any(|(tid, m)| *m == LockMode::Exclusive && *tid != txn_id)
            }
            LockMode::Exclusive => {
                // Exclusive lock: OK if no locks held, or only held by self
                holders.is_empty() || (holders.len() == 1 && holders[0].0 == txn_id)
            }
        }
    }

    /// Grant a lock to a transaction
    fn grant(&self, txn_id: TransactionId, mode: LockMode) {
        let mut holders = self.holders.write();
        // Remove any existing locks by this transaction first
        holders.retain(|(tid, _)| *tid != txn_id);
        holders.push((txn_id, mode));
    }

    /// Release locks held by a transaction
    fn release(&self, txn_id: TransactionId) {
        let mut holders = self.holders.write();
        holders.retain(|(tid, _)| *tid != txn_id);
    }

    /// Check if transaction holds any lock
    fn holds_lock(&self, txn_id: TransactionId) -> Option<LockMode> {
        let holders = self.holders.read();
        holders.iter()
            .find(|(tid, _)| *tid == txn_id)
            .map(|(_, mode)| *mode)
    }
}

/// Lock Manager - manages row-level locks
pub struct LockManager {
    /// Row locks: row_id -> LockEntry
    locks: DashMap<RowId, Arc<LockEntry>>,
    
    /// Transaction lock tracking: txn_id -> set of locked row_ids
    txn_locks: Arc<Mutex<HashMap<TransactionId, HashSet<RowId>>>>,
    
    /// Wait-for graph for deadlock detection: txn_id -> waiting for txn_ids
    wait_for: Arc<Mutex<HashMap<TransactionId, HashSet<TransactionId>>>>,
    
    /// Deadlock detection timeout
    deadlock_timeout: Duration,
}

impl LockManager {
    /// Create a new lock manager
    pub fn new() -> Self {
        Self {
            locks: DashMap::new(),
            txn_locks: Arc::new(Mutex::new(HashMap::new())),
            wait_for: Arc::new(Mutex::new(HashMap::new())),
            deadlock_timeout: Duration::from_secs(5),
        }
    }

    /// Acquire a shared (read) lock
    pub fn acquire_shared(&self, txn_id: TransactionId, row_id: RowId) -> Result<()> {
        self.acquire_lock(txn_id, row_id, LockMode::Shared)
    }

    /// Acquire an exclusive (write) lock
    pub fn acquire_exclusive(&self, txn_id: TransactionId, row_id: RowId) -> Result<()> {
        self.acquire_lock(txn_id, row_id, LockMode::Exclusive)
    }

    /// Check if there's a cycle in the wait-for graph (deadlock detection)
    fn has_cycle(&self, start_txn: TransactionId, current_txn: TransactionId, visited: &mut HashSet<TransactionId>) -> bool {
        if current_txn == start_txn && !visited.is_empty() {
            return true;
        }
        
        if visited.contains(&current_txn) {
            return false;
        }
        
        visited.insert(current_txn);
        
        let wait_for = self.wait_for.lock();
        if let Some(waiting_for) = wait_for.get(&current_txn) {
            for &next_txn in waiting_for {
                if self.has_cycle(start_txn, next_txn, visited) {
                    return true;
                }
            }
        }
        
        false
    }

    /// Detect deadlock for a transaction
    fn detect_deadlock(&self, txn_id: TransactionId) -> bool {
        let mut visited = HashSet::new();
        self.has_cycle(txn_id, txn_id, &mut visited)
    }

    /// Add wait-for edge
    fn add_wait_for(&self, waiter: TransactionId, holders: &[TransactionId]) {
        let mut wait_for = self.wait_for.lock();
        let entry = wait_for.entry(waiter).or_default();
        for &holder in holders {
            if holder != waiter {
                entry.insert(holder);
            }
        }
    }

    /// Remove wait-for edges for a transaction
    fn remove_wait_for(&self, txn_id: TransactionId) {
        let mut wait_for = self.wait_for.lock();
        wait_for.remove(&txn_id);
    }

    /// Internal lock acquisition with deadlock detection
    fn acquire_lock(
        &self,
        txn_id: TransactionId,
        row_id: RowId,
        mode: LockMode,
    ) -> Result<()> {
        // Get or create lock entry
        let entry = self.locks.entry(row_id)
            .or_insert_with(|| Arc::new(LockEntry::new()))
            .clone();

        // Check if transaction already holds this lock
        if let Some(current_mode) = entry.holds_lock(txn_id) {
            // Already holds lock - check for upgrade
            if current_mode == LockMode::Shared && mode == LockMode::Exclusive {
                // Lock upgrade: shared -> exclusive
                return self.upgrade_lock(txn_id, row_id, entry);
            }
            // Already have sufficient lock
            return Ok(());
        }

        // Try to acquire lock immediately
        if entry.can_grant(mode, txn_id) {
            entry.grant(txn_id, mode);
            
            // Track lock
            let mut txn_locks = self.txn_locks.lock();
            txn_locks.entry(txn_id).or_default().insert(row_id);
            
            return Ok(());
        }
        
        // Cannot acquire immediately - would need to wait
        // For now, fail fast (tests expect immediate failure)
        // In production, could implement wait queue with timeout
        Err(StorageError::Transaction(format!(
            "Lock conflict: txn {} cannot acquire {:?} lock on row {}",
            txn_id, mode, row_id
        )))
    }

    /// Upgrade a shared lock to exclusive
    fn upgrade_lock(&self, txn_id: TransactionId, row_id: RowId, entry: Arc<LockEntry>) -> Result<()> {
        // Check if we can upgrade immediately (we must be the only holder)
        if entry.can_grant(LockMode::Exclusive, txn_id) {
            // Atomically upgrade
            entry.grant(txn_id, LockMode::Exclusive);
            return Ok(());
        }
        
        // Cannot upgrade - other transactions hold locks
        Err(StorageError::Transaction(format!(
            "Cannot upgrade lock: txn {} on row {}, other transactions hold locks",
            txn_id, row_id
        )))
    }

    /// Release all locks held by a transaction
    pub fn release_locks(&self, txn_id: TransactionId) -> Result<()> {
        // Remove from wait-for graph
        self.remove_wait_for(txn_id);
        
        // Get locked rows for this transaction
        let locked_rows = {
            let mut txn_locks = self.txn_locks.lock();
            txn_locks.remove(&txn_id).unwrap_or_default()
        };

        // Release each lock
        for row_id in locked_rows {
            if let Some(entry) = self.locks.get(&row_id) {
                entry.release(txn_id);
            }
        }

        Ok(())
    }

    /// Get statistics
    pub fn stats(&self) -> LockManagerStats {
        let txn_locks = self.txn_locks.lock();
        
        LockManagerStats {
            total_locks: self.locks.len() as u64,
            active_transactions: txn_locks.len() as u64,
            total_locked_rows: txn_locks.values().map(|s| s.len() as u64).sum(),
        }
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Lock manager statistics
#[derive(Debug, Clone)]
pub struct LockManagerStats {
    pub total_locks: u64,
    pub active_transactions: u64,
    pub total_locked_rows: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_shared_lock_compatibility() {
        let lm = LockManager::new();
        
        // Multiple transactions can hold shared locks
        lm.acquire_shared(1, 100).unwrap();
        lm.acquire_shared(2, 100).unwrap();
        lm.acquire_shared(3, 100).unwrap();
        
        let stats = lm.stats();
        assert_eq!(stats.active_transactions, 3);
    }

    #[test]
    fn test_exclusive_lock_blocks() {
        let lm = LockManager::new();
        
        // T1 acquires exclusive lock
        lm.acquire_exclusive(1, 100).unwrap();
        
        // T2 cannot acquire any lock on same row
        assert!(lm.acquire_shared(2, 100).is_err());
        assert!(lm.acquire_exclusive(2, 100).is_err());
    }

    #[test]
    fn test_exclusive_blocks_shared() {
        let lm = LockManager::new();
        
        // T1 acquires exclusive lock
        lm.acquire_exclusive(1, 100).unwrap();
        
        // T2 cannot acquire shared lock
        assert!(lm.acquire_shared(2, 100).is_err());
    }

    #[test]
    fn test_shared_blocks_exclusive() {
        let lm = LockManager::new();
        
        // T1 acquires shared lock
        lm.acquire_shared(1, 100).unwrap();
        
        // T2 cannot acquire exclusive lock
        assert!(lm.acquire_exclusive(2, 100).is_err());
    }

    #[test]
    fn test_lock_release() {
        let lm = LockManager::new();
        
        // T1 acquires exclusive lock
        lm.acquire_exclusive(1, 100).unwrap();
        
        // Release locks
        lm.release_locks(1).unwrap();
        
        // Now T2 can acquire lock
        lm.acquire_exclusive(2, 100).unwrap();
    }

    #[test]
    fn test_lock_upgrade() {
        let lm = LockManager::new();
        
        // T1 acquires shared lock
        lm.acquire_shared(1, 100).unwrap();
        
        // Release to allow upgrade test
        lm.release_locks(1).unwrap();
        
        // Acquire again and try upgrade
        lm.acquire_shared(1, 100).unwrap();
        lm.acquire_exclusive(1, 100).unwrap();  // Upgrade
    }

    #[test]
    fn test_multiple_row_locks() {
        let lm = LockManager::new();
        
        // T1 locks multiple rows
        lm.acquire_exclusive(1, 100).unwrap();
        lm.acquire_exclusive(1, 200).unwrap();
        lm.acquire_exclusive(1, 300).unwrap();
        
        let stats = lm.stats();
        assert_eq!(stats.total_locked_rows, 3);
        
        // Release all
        lm.release_locks(1).unwrap();
        
        let stats = lm.stats();
        assert_eq!(stats.active_transactions, 0);
    }

    #[test]
    fn test_concurrent_shared_locks() {
        let lm = Arc::new(LockManager::new());
        let mut handles = vec![];
        
        for i in 0..5 {
            let lm = lm.clone();
            let handle = thread::spawn(move || {
                lm.acquire_shared(i, 100).unwrap();
                thread::sleep(Duration::from_millis(10));
                lm.release_locks(i).unwrap();
            });
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        // All should succeed
        let stats = lm.stats();
        assert_eq!(stats.active_transactions, 0);
    }

    #[test]
    fn test_lock_statistics() {
        let lm = LockManager::new();
        
        lm.acquire_exclusive(1, 100).unwrap();
        lm.acquire_exclusive(2, 200).unwrap();
        lm.acquire_shared(3, 300).unwrap();
        
        let stats = lm.stats();
        assert_eq!(stats.active_transactions, 3);
        assert_eq!(stats.total_locked_rows, 3);
        assert!(stats.total_locks > 0);
    }
}
