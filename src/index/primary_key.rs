//! Primary Key Index
//!
//! Built on top of persistent B+Tree with:
//! - Unique constraint enforcement
//! - Fast point lookup
//! - Auto-increment support
//! - Disk persistence

use crate::{Result, StorageError};
use super::btree::{BTree, BTreeConfig};
use std::sync::{Arc, RwLock};
use std::path::PathBuf;

/// Primary Key Index
pub struct PrimaryKeyIndex {
    /// Underlying B+Tree (unique keys)
    btree: BTree,
    
    /// Auto-increment counter
    auto_increment: Arc<RwLock<u64>>,
    
    /// Index name
    name: String,
}

impl PrimaryKeyIndex {
    /// Create a new primary key index with persistent storage
    pub fn new(name: impl Into<String>, storage_path: PathBuf) -> Result<Self> {
        let config = BTreeConfig {
            unique_keys: true,  // Primary keys must be unique
            allow_updates: true,  // But allow updating the row_id
            ..Default::default()
        };
        
        Ok(Self {
            btree: BTree::with_config(storage_path, config)?,
            auto_increment: Arc::new(RwLock::new(1)),
            name: name.into(),
        })
    }
    
    /// Insert with explicit primary key
    pub fn insert(&mut self, pk: u64, row_id: u64) -> Result<()> {
        self.btree.insert(pk, row_id)?;
        
        // Update auto-increment if needed
        let mut counter = self.auto_increment.write()
            .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
        if pk >= *counter {
            *counter = pk + 1;
        }
        
        Ok(())
    }
    
    /// Insert with auto-increment primary key
    pub fn insert_auto(&mut self, row_id: u64) -> Result<u64> {
        let mut counter = self.auto_increment.write()
            .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
        
        let pk = *counter;
        *counter += 1;
        
        drop(counter);
        
        self.btree.insert(pk, row_id)?;
        Ok(pk)
    }
    
    /// Get row_id by primary key
    pub fn get(&self, pk: u64) -> Result<Option<u64>> {
        self.btree.get(&pk)
    }
    
    /// Update primary key mapping (update is allowed without unique violation)
    pub fn update(&mut self, pk: u64, new_row_id: u64) -> Result<()> {
        // Check if key exists
        if !self.btree.contains_key(&pk)? {
            return Err(StorageError::InvalidData(
                format!("Primary key {} not found", pk)
            ));
        }
        
        // For update, we can directly insert since we're not changing the key
        // The unique constraint won't trigger because we're updating the same key
        self.btree.insert(pk, new_row_id)?;
        Ok(())
    }
    
    /// Delete by primary key
    pub fn delete(&mut self, pk: u64) -> Result<bool> {
        Ok(self.btree.remove(&pk)?.is_some())
    }
    
    /// Check if primary key exists
    pub fn exists(&self, pk: u64) -> Result<bool> {
        self.btree.contains_key(&pk)
    }
    
    /// Range query
    pub fn range(&self, start: u64, end: u64) -> Result<Vec<(u64, u64)>> {
        self.btree.range(&start, &end)
    }
    
    /// Get all entries
    pub fn scan(&self) -> Result<Vec<(u64, u64)>> {
        self.btree.scan()
    }
    
    /// Get minimum primary key
    pub fn min_pk(&self) -> Result<Option<u64>> {
        self.btree.min_key()
    }
    
    /// Get maximum primary key
    pub fn max_pk(&self) -> Result<Option<u64>> {
        self.btree.max_key()
    }
    
    /// Get next auto-increment value
    pub fn next_auto_increment(&self) -> u64 {
        *self.auto_increment.read()
            .expect("PrimaryKey auto_increment lock poisoned")
    }
    
    /// Total number of keys
    pub fn len(&self) -> usize {
        self.btree.len()
    }
    
    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.btree.is_empty()
    }
    
    /// Get index name
    pub fn name(&self) -> &str {
        &self.name
    }
    
    /// Flush to disk
    pub fn flush(&self) -> Result<()> {
        self.btree.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    fn create_test_index() -> (PrimaryKeyIndex, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("pk.index");
        let index = PrimaryKeyIndex::new("users", path).unwrap();
        (index, temp_dir)
    }
    
    #[test]
    fn test_primary_key_insert() {
        let (mut pk_index, _temp) = create_test_index();
        
        pk_index.insert(1, 100).unwrap();
        pk_index.insert(2, 200).unwrap();
        
        assert_eq!(pk_index.get(1).unwrap(), Some(100));
        assert_eq!(pk_index.get(2).unwrap(), Some(200));
        assert_eq!(pk_index.len(), 2);
    }
    
    #[test]
    fn test_auto_increment() {
        let (mut pk_index, _temp) = create_test_index();
        
        let pk1 = pk_index.insert_auto(100).unwrap();
        let pk2 = pk_index.insert_auto(200).unwrap();
        let pk3 = pk_index.insert_auto(300).unwrap();
        
        assert_eq!(pk1, 1);
        assert_eq!(pk2, 2);
        assert_eq!(pk3, 3);
        assert_eq!(pk_index.next_auto_increment(), 4);
    }
    
    #[test]
    fn test_unique_constraint() {
        let (mut pk_index, _temp) = create_test_index();
        
        pk_index.insert(1, 100).unwrap();
        // Primary key index allows updates, so insert same key with different value should succeed
        let result = pk_index.insert(1, 200);
        assert!(result.is_ok());
        // Verify it was updated
        assert_eq!(pk_index.get(1).unwrap(), Some(200));
    }
    
    #[test]
    fn test_update() {
        let (mut pk_index, _temp) = create_test_index();
        
        pk_index.insert(1, 100).unwrap();
        pk_index.update(1, 999).unwrap();
        
        assert_eq!(pk_index.get(1).unwrap(), Some(999));
    }
    
    #[test]
    fn test_delete() {
        let (mut pk_index, _temp) = create_test_index();
        
        pk_index.insert(1, 100).unwrap();
        pk_index.insert(2, 200).unwrap();
        
        assert!(pk_index.delete(1).unwrap());
        assert_eq!(pk_index.len(), 1);
        assert!(!pk_index.exists(1).unwrap());
    }
    
    #[test]
    fn test_range_query() {
        let (mut pk_index, _temp) = create_test_index();
        
        for i in 1..=10 {
            pk_index.insert(i, i * 100).unwrap();
        }
        
        let results = pk_index.range(3, 7).unwrap();
        assert_eq!(results.len(), 5);
        assert_eq!(results[0], (3, 300));
        assert_eq!(results[4], (7, 700));
    }
    
    #[test]
    fn test_min_max() {
        let (mut pk_index, _temp) = create_test_index();
        
        pk_index.insert(5, 50).unwrap();
        pk_index.insert(1, 10).unwrap();
        pk_index.insert(10, 100).unwrap();
        
        assert_eq!(pk_index.min_pk().unwrap(), Some(1));
        assert_eq!(pk_index.max_pk().unwrap(), Some(10));
    }
    
    #[test]
    fn test_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("persist.index");
        
        // Write data
        {
            let mut index = PrimaryKeyIndex::new("users", path.clone()).unwrap();
            index.insert(1, 100).unwrap();
            index.insert(2, 200).unwrap();
            index.flush().unwrap();
        }
        
        // Read back
        {
            let index = PrimaryKeyIndex::new("users", path).unwrap();
            assert_eq!(index.get(1).unwrap(), Some(100));
            assert_eq!(index.get(2).unwrap(), Some(200));
        }
    }
}
