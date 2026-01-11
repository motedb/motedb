//! MVCC Version Store
//!
//! Manages multiple versions of rows for snapshot isolation.
//! Each row can have multiple versions, organized as a linked list.

use crate::types::{Row, RowId};
use crate::{Result, StorageError};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// Transaction ID
pub type TransactionId = u64;

/// Timestamp (monotonically increasing)
pub type Timestamp = u64;

/// Version Store - manages all row versions
pub struct VersionStore {
    /// Row ID -> Version Chain
    versions: DashMap<RowId, VersionChain>,
    
    /// Global timestamp generator
    timestamp_gen: Arc<AtomicU64>,
}

/// Version Chain - linked list of versions for a single row
pub struct VersionChain {
    /// Head of the version chain (newest version)
    head: Arc<RwLock<Option<Box<RowVersion>>>>,
    
    /// Number of versions in the chain
    version_count: AtomicU64,
}

/// Single version of a row
pub struct RowVersion {
    /// Actual row data
    pub data: Row,
    
    /// Transaction that created this version
    pub txn_id: TransactionId,
    
    /// When this version became valid
    pub begin_ts: Timestamp,
    
    /// When this version became invalid (0 = still valid)
    pub end_ts: AtomicU64,
    
    /// Deletion marker
    pub deleted: AtomicBool,
    
    /// Next version in the chain (older version)
    pub next: Option<Box<RowVersion>>,
}

/// Snapshot for transaction isolation
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Snapshot timestamp
    pub timestamp: Timestamp,
    
    /// Active transaction IDs at snapshot time
    pub active_txns: std::collections::HashSet<TransactionId>,
}

impl VersionStore {
    /// Create a new version store
    pub fn new() -> Self {
        Self {
            versions: DashMap::new(),
            timestamp_gen: Arc::new(AtomicU64::new(1)),
        }
    }
    
    /// Allocate a new timestamp
    pub fn allocate_timestamp(&self) -> Timestamp {
        self.timestamp_gen.fetch_add(1, Ordering::SeqCst)
    }
    
    /// Insert a new version for a row
    pub fn insert_version(
        &self,
        row_id: RowId,
        data: Row,
        txn_id: TransactionId,
        timestamp: Timestamp,
    ) -> Result<()> {
        let new_version = Box::new(RowVersion {
            data,
            txn_id,
            begin_ts: timestamp,
            end_ts: AtomicU64::new(0),
            deleted: AtomicBool::new(false),
            next: None,
        });
        
        // OPTIMIZATION: Two-step approach to avoid holding DashMap lock during prepend
        // Step 1: Ensure chain exists
        if !self.versions.contains_key(&row_id) {
            self.versions.insert(row_id, VersionChain::new());
        }
        
        // Step 2: Get chain and prepend (DashMap lock released between steps)
        if let Some(chain) = self.versions.get(&row_id) {
            chain.prepend(new_version);
        }
        
        Ok(())
    }
    
    /// Update a row (creates a new version)
    pub fn update_version(
        &self,
        row_id: RowId,
        new_data: Row,
        txn_id: TransactionId,
        timestamp: Timestamp,
    ) -> Result<()> {
        // Mark old version as invalid
        if let Some(chain) = self.versions.get(&row_id) {
            let head = chain.head.read();
            if let Some(old_version) = head.as_ref() {
                old_version.end_ts.store(timestamp, Ordering::Release);
            }
        }
        
        // Insert new version
        self.insert_version(row_id, new_data, txn_id, timestamp)
    }
    
    /// Delete a row (marks latest version as deleted)
    pub fn delete_version(
        &self,
        row_id: RowId,
        txn_id: TransactionId,
        timestamp: Timestamp,
    ) -> Result<()> {
        let chain = self.versions.get(&row_id)
            .ok_or_else(|| StorageError::InvalidData(format!("Row {} not found", row_id)))?;
        
        // Mark current version as deleted
        {
            let head = chain.head.read();
            if let Some(version) = head.as_ref() {
                version.end_ts.store(timestamp, Ordering::Release);
            }
            // Drop read lock before prepend
        }
        
        // Insert tombstone
        let tombstone = Box::new(RowVersion {
            data: vec![],
            txn_id,
            begin_ts: timestamp,
            end_ts: AtomicU64::new(0),
            deleted: AtomicBool::new(true),
            next: None,
        });
        
        chain.prepend(tombstone);
        
        Ok(())
    }
    
    /// Get the visible version for a snapshot
    pub fn get_visible_version(
        &self,
        row_id: RowId,
        snapshot: &Snapshot,
    ) -> Result<Option<Row>> {
        let chain = match self.versions.get(&row_id) {
            Some(c) => c,
            None => return Ok(None), // Row doesn't exist yet
        };
        
        // Traverse version chain to find visible version
        let head = chain.head.read();
        let mut current = head.as_deref();
        
        while let Some(version) = current {
            if self.is_visible(version, snapshot) {
                if !version.deleted.load(Ordering::Acquire) {
                    return Ok(Some(version.data.clone()));
                } else {
                    return Ok(None); // Row was deleted
                }
            }
            current = version.next.as_deref();
        }
        
        Ok(None) // No visible version
    }
    
    /// Check if a version is visible to a snapshot
    fn is_visible(&self, version: &RowVersion, snapshot: &Snapshot) -> bool {
        // Rule 1: Version must have been created before snapshot
        if version.begin_ts > snapshot.timestamp {
            return false;
        }
        
        // Rule 2: Version must not have been invalidated before snapshot
        let end_ts = version.end_ts.load(Ordering::Acquire);
        if end_ts != 0 && end_ts <= snapshot.timestamp {
            return false;
        }
        
        // Rule 3: Creating transaction must not be active in snapshot
        if snapshot.active_txns.contains(&version.txn_id) {
            return false;
        }
        
        true
    }
    
    /// Get statistics about the version store
    pub fn stats(&self) -> VersionStoreStats {
        let mut total_versions = 0u64;
        let mut total_chains = 0u64;
        let mut max_chain_length = 0u64;
        
        for entry in self.versions.iter() {
            total_chains += 1;
            let chain_len = entry.value().version_count.load(Ordering::Relaxed);
            total_versions += chain_len;
            max_chain_length = max_chain_length.max(chain_len);
        }
        
        VersionStoreStats {
            total_rows: total_chains,
            total_versions,
            avg_versions_per_row: if total_chains > 0 {
                total_versions as f64 / total_chains as f64
            } else {
                0.0
            },
            max_chain_length,
            current_timestamp: self.timestamp_gen.load(Ordering::Relaxed),
        }
    }
    
    /// Vacuum - remove old versions that are no longer visible to any transaction
    pub fn vacuum(&self, min_active_timestamp: Timestamp) -> Result<usize> {
        let mut removed = 0;
        
        for mut entry in self.versions.iter_mut() {
            let chain = entry.value_mut();
            removed += chain.vacuum(min_active_timestamp);
        }
        
        Ok(removed)
    }
}

impl VersionChain {
    /// Create a new version chain
    fn new() -> Self {
        Self {
            head: Arc::new(RwLock::new(None)),
            version_count: AtomicU64::new(0),
        }
    }
    
    /// Prepend a new version to the chain
    fn prepend(&self, mut new_version: Box<RowVersion>) {
        let mut head = self.head.write();
        
        // Link new version to old head
        new_version.next = head.take();
        
        // Update head
        *head = Some(new_version);
        
        // Update count
        self.version_count.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Remove versions older than min_timestamp
    fn vacuum(&self, min_timestamp: Timestamp) -> usize {
        let mut head = self.head.write();
        let mut removed = 0;
        
        if let Some(first_version) = head.as_mut() {
            // Keep the first version, vacuum the rest
            removed += Self::vacuum_chain(&mut first_version.next, min_timestamp);
        }
        
        if removed > 0 {
            self.version_count.fetch_sub(removed as u64, Ordering::Relaxed);
        }
        
        removed
    }
    
    fn vacuum_chain(next: &mut Option<Box<RowVersion>>, min_timestamp: Timestamp) -> usize {
        let mut removed = 0;
        
        while let Some(version) = next {
            let end_ts = version.end_ts.load(Ordering::Acquire);
            
            // Can remove if version ended before min_timestamp
            if end_ts != 0 && end_ts < min_timestamp {
                *next = version.next.take();
                removed += 1;
            } else {
                // Recurse to next version
                removed += Self::vacuum_chain(&mut version.next, min_timestamp);
                break;
            }
        }
        
        removed
    }
}

/// Version store statistics
#[derive(Debug, Clone)]
pub struct VersionStoreStats {
    pub total_rows: u64,
    pub total_versions: u64,
    pub avg_versions_per_row: f64,
    pub max_chain_length: u64,
    pub current_timestamp: Timestamp,
}

impl Default for VersionStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Value, Timestamp};
    use std::collections::HashSet;

    #[test]
    fn test_version_store_create() {
        let store = VersionStore::new();
        let stats = store.stats();
        assert_eq!(stats.total_rows, 0);
        assert_eq!(stats.total_versions, 0);
    }

    #[test]
    fn test_insert_and_read_single_version() {
        let store = VersionStore::new();
        let row_id = 1;
        let data = vec![Value::Timestamp(Timestamp::from_micros(100))];
        
        store.insert_version(row_id, data.clone(), 1, 10).unwrap();
        
        let snapshot = Snapshot {
            timestamp: 15,
            active_txns: HashSet::new(),
        };
        
        let result = store.get_visible_version(row_id, &snapshot).unwrap();
        assert_eq!(result, Some(data));
    }

    #[test]
    fn test_multi_version_isolation() {
        let store = VersionStore::new();
        let row_id = 1;
        
        // T1: Insert initial value at ts=10
        store.insert_version(row_id, vec![Value::Timestamp(Timestamp::from_micros(100))], 1, 10).unwrap();
        
        // T2: Update at ts=20
        store.update_version(row_id, vec![Value::Timestamp(Timestamp::from_micros(200))], 2, 20).unwrap();
        
        // Snapshot at ts=15 should see old value
        let snapshot_old = Snapshot {
            timestamp: 15,
            active_txns: HashSet::new(),
        };
        let result = store.get_visible_version(row_id, &snapshot_old).unwrap();
        assert_eq!(result, Some(vec![Value::Timestamp(Timestamp::from_micros(100))]));
        
        // Snapshot at ts=25 should see new value
        let snapshot_new = Snapshot {
            timestamp: 25,
            active_txns: HashSet::new(),
        };
        let result = store.get_visible_version(row_id, &snapshot_new).unwrap();
        assert_eq!(result, Some(vec![Value::Timestamp(Timestamp::from_micros(200))]));
    }

    #[test]
    fn test_uncommitted_transaction_invisible() {
        let store = VersionStore::new();
        let row_id = 1;
        
        // T1: Insert at ts=10
        store.insert_version(row_id, vec![Value::Timestamp(Timestamp::from_micros(100))], 1, 10).unwrap();
        
        // Snapshot at ts=15 with T1 still active
        let mut active_txns = HashSet::new();
        active_txns.insert(1);
        
        let snapshot = Snapshot {
            timestamp: 15,
            active_txns,
        };
        
        // Should not see uncommitted data
        let result = store.get_visible_version(row_id, &snapshot).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_delete_version() {
        let store = VersionStore::new();
        let row_id = 1;
        
        // Insert
        store.insert_version(row_id, vec![Value::Timestamp(Timestamp::from_micros(100))], 1, 10).unwrap();
        
        // Delete
        store.delete_version(row_id, 2, 20).unwrap();
        
        // Snapshot before delete should see data
        let snapshot_before = Snapshot {
            timestamp: 15,
            active_txns: HashSet::new(),
        };
        let result = store.get_visible_version(row_id, &snapshot_before).unwrap();
        assert_eq!(result, Some(vec![Value::Timestamp(Timestamp::from_micros(100))]));
        
        // Snapshot after delete should not see data
        let snapshot_after = Snapshot {
            timestamp: 25,
            active_txns: HashSet::new(),
        };
        let result = store.get_visible_version(row_id, &snapshot_after).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_version_chain_statistics() {
        let store = VersionStore::new();
        
        // Insert multiple versions
        for i in 0..10 {
            store.insert_version(i, vec![Value::Timestamp(Timestamp::from_micros(i as i64))], 1, 10).unwrap();
        }
        
        let stats = store.stats();
        assert_eq!(stats.total_rows, 10);
        assert_eq!(stats.total_versions, 10);
        assert_eq!(stats.avg_versions_per_row, 1.0);
    }

    #[test]
    fn test_vacuum_old_versions() {
        let store = VersionStore::new();
        let row_id = 1;
        
        // Create multiple versions
        store.insert_version(row_id, vec![Value::Timestamp(Timestamp::from_micros(100))], 1, 10).unwrap();
        store.update_version(row_id, vec![Value::Timestamp(Timestamp::from_micros(200))], 2, 20).unwrap();
        store.update_version(row_id, vec![Value::Timestamp(Timestamp::from_micros(300))], 3, 30).unwrap();
        
        let stats_before = store.stats();
        assert_eq!(stats_before.total_versions, 3);
        
        // Vacuum versions older than ts=25
        let removed = store.vacuum(25).unwrap();
        
        // Should remove version at ts=10 (but keep ts=20 and ts=30)
        assert!(removed > 0);
        
        let stats_after = store.stats();
        assert!(stats_after.total_versions < stats_before.total_versions);
    }
}
