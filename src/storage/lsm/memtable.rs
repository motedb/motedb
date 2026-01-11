//! MemTable: In-memory write buffer using Skip List
//!
//! ## Performance
//! - Write: O(log n), ~10μs
//! - Read: O(log n), ~1μs
//! - Capacity: 4MB (50K entries)

use super::{Key, Value, LSMConfig};
use crate::{Result, StorageError};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::collections::BTreeMap;

/// In-memory write buffer
pub struct MemTable {
    /// Sorted key-value map (using BTreeMap as Skip List)
    /// Using BTreeMap for now (consider Skip List for better concurrent performance in future)
    data: Arc<RwLock<BTreeMap<Key, Value>>>,
    
    /// Current size in bytes
    size: AtomicUsize,
    
    /// Maximum size before flush
    max_size: usize,
    
    /// Sequence number (for ordering)
    next_seq: AtomicUsize,
}

impl MemTable {
    /// Create a new MemTable
    pub fn new(config: &LSMConfig) -> Self {
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
            size: AtomicUsize::new(0),
            max_size: config.memtable_size,
            next_seq: AtomicUsize::new(0),
        }
    }
    
    /// Insert a key-value pair
    pub fn put(&self, key: Key, value: Value) -> Result<()> {
        let key_size = 8; // u64 is always 8 bytes
        let value_size = value.data.len() + 16; // data + metadata
        let entry_size = key_size + value_size;
        
        let mut data = self.data.write()
            .map_err(|_| StorageError::Lock("MemTable lock poisoned".into()))?;
        
        // Update size
        if let Some(old_value) = data.get(&key) {
            let old_size = key_size + old_value.data.len() + 16;
            self.size.fetch_sub(old_size, Ordering::Relaxed);
        }
        
        data.insert(key, value);
        self.size.fetch_add(entry_size, Ordering::Relaxed);
        self.next_seq.fetch_add(1, Ordering::Relaxed);
        
        Ok(())
    }
    
    /// Get a value by key
    pub fn get(&self, key: Key) -> Result<Option<Value>> {
        let data = self.data.read()
            .map_err(|_| StorageError::Lock("MemTable lock poisoned".into()))?;
        
        Ok(data.get(&key).cloned())
    }
    
    /// Delete a key (insert tombstone)
    pub fn delete(&self, key: Key, timestamp: u64) -> Result<()> {
        self.put(key, Value::tombstone(timestamp))
    }
    
    /// Check if MemTable should be flushed
    pub fn should_flush(&self) -> bool {
        self.size.load(Ordering::Relaxed) >= self.max_size
    }
    
    /// Get current size in bytes
    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }
    
    /// Get number of entries
    pub fn len(&self) -> usize {
        self.data.read()
            .map(|data| data.len())
            .unwrap_or(0)  // Fallback if poisoned
    }
    
    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    
    /// Iterate over all entries (for flushing to SSTable)
    /// OPTIMIZED: O(n) instead of O(n²)
    pub fn iter(&self) -> MemTableIteratorOptimized {
        MemTableIteratorOptimized::new(self.data.clone())
    }
    
    /// Get snapshot of all data (for testing)
    pub fn snapshot(&self) -> Vec<(Key, Value)> {
        let data = self.data.read()
            .expect("MemTable snapshot: lock poisoned (unrecoverable in test)");
        data.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
    
    /// Scan a range of keys [start, end) - Zero-copy with callback
    /// 
    /// ✅ Zero-copy optimization: No Vec allocation, processes items in-place
    pub fn scan_with<F>(&self, start: Key, end: Key, mut f: F) -> Result<()>
    where
        F: FnMut(Key, &Value) -> Result<()>,
    {
        let data = self.data.read()
            .map_err(|_| StorageError::Lock("MemTable lock poisoned".into()))?;
        
        // Use BTreeMap's range() for efficient range query: O(log n + k)
        use std::ops::Bound;
        let range = data.range((
            Bound::Included(&start), 
            Bound::Excluded(&end)
        ));
        
        for (k, v) in range {
            // Skip tombstones (deleted entries)
            if !v.deleted {
                f(*k, v)?;  // ✅ Zero-copy: pass reference to Value
            }
        }
        
        Ok(())
    }
    
    /// Scan a range of keys [start, end) - Legacy API (allocates Vec)
    /// 
    /// ⚠️ Prefer scan_with() for zero-copy iteration
    pub fn scan(&self, start: Key, end: Key) -> Result<Vec<(Key, Value)>> {
        // 🚀 P3 优化：预分配容量（估算范围大小）
        let estimated_size = ((end - start) as usize).min(1000);
        let mut results = Vec::with_capacity(estimated_size);
        self.scan_with(start, end, |k, v| {
            results.push((k, v.clone()));
            Ok(())
        })?;
        Ok(results)
    }
    
    /// Scan all entries with callback - Zero-copy
    /// 
    /// ✅ Zero-copy optimization: No Vec allocation
    pub fn scan_all_with<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(Key, &Value) -> Result<()>,
    {
        let data = self.data.read()
            .map_err(|_| StorageError::Lock("MemTable lock poisoned".into()))?;
        
        for (k, v) in data.iter() {
            if !v.deleted {
                f(*k, v)?;  // ✅ Zero-copy: pass reference
            }
        }
        
        Ok(())
    }
    
    /// Get all entries (for full table scan) - Legacy API
    /// 
    /// ⚠️ Prefer scan_all_with() for zero-copy iteration
    pub fn scan_all(&self) -> Result<Vec<(Key, Value)>> {
        // 🚀 P3 优化：预分配容量
        let mut results = Vec::with_capacity(1000);
        self.scan_all_with(|k, v| {
            results.push((k, v.clone()));
            Ok(())
        })?;
        Ok(results)
    }
}

/// Legacy iterator - O(n²) performance, kept for compatibility
/// Use MemTableIteratorOptimized instead for O(n) performance
#[allow(dead_code)]
pub struct MemTableIterator {
    data: Arc<RwLock<BTreeMap<Key, Value>>>,
    index: usize,
}

#[allow(dead_code)]
impl Iterator for MemTableIterator {
    type Item = (Key, Value);
    
    fn next(&mut self) -> Option<Self::Item> {
        // Note: O(n²) complexity - nth() walks from start each time
        // Use MemTableIteratorOptimized for production code
        let data = self.data.read()
            .expect("MemTableIterator: lock poisoned (test-only code)");
        let item = data.iter().nth(self.index)?;
        self.index += 1;
        Some((item.0.clone(), item.1.clone()))
    }
}

/// Optimized iterator that clones data once
pub struct MemTableIteratorOptimized {
    entries: std::vec::IntoIter<(Key, Value)>,
}

impl MemTableIteratorOptimized {
    pub fn new(data: Arc<RwLock<BTreeMap<Key, Value>>>) -> Self {
        let data = data.read()
            .expect("MemTableIteratorOptimized: lock poisoned (unrecoverable)");
        let entries: Vec<(Key, Value)> = data.iter()
            .map(|(k, v)| (*k, v.clone()))  // ✅ u64 copy is cheap, no clone()
            .collect();
        Self {
            entries: entries.into_iter(),
        }
    }
}

impl Iterator for MemTableIteratorOptimized {
    type Item = (Key, Value);
    
    fn next(&mut self) -> Option<Self::Item> {
        self.entries.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    fn create_memtable() -> MemTable {
        MemTable::new(&LSMConfig::default())
    }
    
    #[test]
    fn test_put_get() {
        let memtable = create_memtable();
        
        let key = 12345u64;  // ✅ u64 key
        let value = Value::new(b"test_value".to_vec(), 1);
        
        memtable.put(key, value.clone()).unwrap();
        
        let retrieved = memtable.get(key).unwrap().unwrap();
        assert_eq!(retrieved.data, value.data);
        assert_eq!(retrieved.timestamp, 1);
        assert_eq!(retrieved.deleted, false);
    }
    
    #[test]
    fn test_delete() {
        let memtable = create_memtable();
        
        let key = 12345u64;  // ✅ u64 key
        memtable.put(key, Value::new(b"value".to_vec(), 1)).unwrap();
        memtable.delete(key, 2).unwrap();
        
        let retrieved = memtable.get(key).unwrap().unwrap();
        assert_eq!(retrieved.deleted, true);
        assert_eq!(retrieved.timestamp, 2);
    }
    
    #[test]
    fn test_size_tracking() {
        let memtable = create_memtable();
        
        assert_eq!(memtable.size(), 0);
        
        let key = 123u64;  // ✅ u64 key
        let value = Value::new(b"value".to_vec(), 1);
        memtable.put(key, value).unwrap();
        
        assert!(memtable.size() > 0);
        
        // Update should replace old value
        let new_value = Value::new(b"new_value".to_vec(), 2);
        memtable.put(key, new_value).unwrap();
        
        assert!(memtable.size() > 0);
    }
    
    #[test]
    fn test_should_flush() {
        let mut config = LSMConfig::default();
        config.memtable_size = 100; // Small size for testing
        let memtable = MemTable::new(&config);
        
        assert_eq!(memtable.should_flush(), false);
        
        // Insert data until flush is needed
        for i in 0..10 {
            let key = i as u64;  // ✅ u64 key
            let value = Value::new(vec![0u8; 20], i);
            memtable.put(key, value).unwrap();
        }
        
        assert_eq!(memtable.should_flush(), true);
    }
    
    #[test]
    fn test_iterator() {
        let memtable = create_memtable();
        
        // Insert data
        for i in 0..5 {
            let key = i as u64;  // ✅ u64 key (naturally sorted)
            let value = Value::new(format!("value_{}", i).into_bytes(), i as u64);
            memtable.put(key, value).unwrap();
        }
        
        // Iterate and verify order
        let items: Vec<_> = memtable.iter().collect();
        assert_eq!(items.len(), 5);
        
        // BTreeMap should maintain sorted order
        for (i, (key, _)) in items.iter().enumerate() {
            let expected_key = i as u64;
            assert_eq!(*key, expected_key);
        }
    }
}
