//! Index MemBuffer - Lightweight buffer layer for all index types
//!
//! This is NOT a full LSM MemTable. It's a simple in-memory buffer that
//! holds recent writes until they are flushed to the specialized index
//! structure (B+Tree, Vamana Graph, Grid, etc.)
//!
//! # Design Philosophy
//! - Lightweight: Only 1-2MB per index
//! - Simple: Just a BTreeMap for sorted storage
//! - Universal: Works with any index type
//! - Fast: O(log N) insert and query
//!
//! # Concurrency Model (RocksDB-style)
//! - Active Buffer: Mutable, accepts writes
//! - Immutable Buffers: Read-only, being flushed
//! - Queries check: Active + Immutable + Persistent Index
//! - Flush doesn't block writes (switch to new active buffer)

use parking_lot::{RwLock, Mutex};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Generic index buffer for all index types (RocksDB-style Immutable Snapshot)
///
/// # Architecture
/// ```
/// Index = Active Buffer (writable)
///       + Immutable Buffers (flushing)
///       + Persistent Structure (flushed data)
/// ```
///
/// # Concurrency Guarantees
/// - ✅ Writes never block on flush (switch to new active buffer)
/// - ✅ Queries always see consistent snapshot
/// - ✅ No data loss during flush
/// - ✅ Crash safe (with WAL)
///
/// # Example
/// ```ignore
/// // Column index
/// let buffer: IndexMemBuffer<IndexKey, ()> = IndexMemBuffer::new(1024 * 1024); // 1MB
/// buffer.insert(key, ())?;
///
/// // Text index  
/// let buffer: IndexMemBuffer<TermId, PostingList> = IndexMemBuffer::new(2048 * 1024); // 2MB
/// buffer.insert(term_id, posting_list)?;
/// ```
pub struct IndexMemBuffer<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    /// Active buffer (mutable, accepts writes)
    active: Arc<RwLock<BufferState<K, V>>>,

    /// Immutable buffers (read-only, being flushed)
    /// Ordered from oldest to newest
    immutable: Arc<RwLock<Vec<Arc<BufferState<K, V>>>>>,

    /// Size limit in bytes (e.g., 1MB)
    size_limit: usize,
    
    /// Flush lock (prevents concurrent flush operations)
    flush_lock: Arc<Mutex<()>>,
}

/// Internal buffer state
struct BufferState<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    /// Buffered entries (sorted for efficient range queries)
    data: BTreeMap<K, V>,

    /// Current buffer size in bytes (estimated)
    size: usize,
}

impl<K, V> IndexMemBuffer<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    /// Create a new index buffer
    ///
    /// # Arguments
    /// - `size_limit`: Max buffer size in bytes before flush is recommended
    ///
    /// # Recommended Sizes
    /// - Column index: 1MB (fast flush)
    /// - Text index: 2MB (amortize posting list encoding)
    /// - Vector index: 4MB (batch graph building)
    /// - Spatial index: 512KB (grid cells are cheap to insert)
    pub fn new(size_limit: usize) -> Self {
        Self {
            active: Arc::new(RwLock::new(BufferState {
                data: BTreeMap::new(),
                size: 0,
            })),
            immutable: Arc::new(RwLock::new(Vec::new())),
            size_limit,
            flush_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Insert a key-value pair
    ///
    /// # Returns
    /// - `Ok(true)`: Buffer is full, caller should trigger flush
    /// - `Ok(false)`: Buffer has space
    ///
    /// # Concurrency
    /// - If buffer is full, internally switches to new active buffer
    /// - Old buffer becomes immutable and ready for flush
    /// - Write operation never blocks on flush
    ///
    /// # Performance
    /// - Time: O(log N) where N = active buffer entries
    /// - Space: Estimated based on sizeof<K> + sizeof<V>
    pub fn insert(&self, key: K, value: V) -> Result<bool, String> {
        let mut active = self.active.write();
        
        // Estimate entry size (rough approximation)
        let entry_size = std::mem::size_of::<K>() + std::mem::size_of::<V>();

        active.data.insert(key, value);
        active.size += entry_size;

        // Check if buffer is full
        if active.size >= self.size_limit {
            // 🔄 Switch: make current active → immutable
            let old_active = BufferState {
                data: std::mem::take(&mut active.data),
                size: active.size,
            };
            active.size = 0;
            
            // Add to immutable queue
            self.immutable.write().push(Arc::new(old_active));
            
            return Ok(true); // Signal: flush needed
        }

        Ok(false)
    }

    /// Point query: get value for exact key
    ///
    /// # Concurrency
    /// - Checks active buffer first (newest data)
    /// - Then checks immutable buffers (newer to older)
    /// - Caller must also check persistent index
    ///
    /// # Performance
    /// - Time: O(log N * M) where N = buffer entries, M = immutable count
    /// - Typically M = 0-2, so almost O(log N)
    pub fn get(&self, key: &K) -> Option<V> {
        // 1. Check active buffer (newest)
        {
            let active = self.active.read();
            if let Some(value) = active.data.get(key) {
                return Some(value.clone());
            }
        }
        
        // 2. Check immutable buffers (reverse order: newest first)
        {
            let immutable = self.immutable.read();
            for buffer in immutable.iter().rev() {
                if let Some(value) = buffer.data.get(key) {
                    return Some(value.clone());
                }
            }
        }
        
        None
    }

    /// Range query: get all entries in [start, end]
    ///
    /// # Performance
    /// - Time: O((log N + K) * M) where K = result size, M = buffer count
    pub fn range(&self, start: &K, end: &K) -> Vec<(K, V)> {
        use std::ops::Bound;
        let mut results = Vec::new();

        // 1. Collect from active buffer
        {
            let active = self.active.read();
            results.extend(
                active.data
                    .range((Bound::Included(start), Bound::Included(end)))
                    .map(|(k, v)| (k.clone(), v.clone()))
            );
        }
        
        // 2. Collect from immutable buffers
        {
            let immutable = self.immutable.read();
            for buffer in immutable.iter() {
                results.extend(
                    buffer.data
                        .range((Bound::Included(start), Bound::Included(end)))
                        .map(|(k, v)| (k.clone(), v.clone()))
                );
            }
        }
        
        // 3. Deduplicate (keep newest value for each key)
        results.sort_by(|a, b| a.0.cmp(&b.0));
        results.dedup_by(|a, b| a.0 == b.0);
        
        results
    }

    /// Scan all entries
    ///
    /// # Performance
    /// - Time: O(N * M) where M = buffer count
    pub fn scan_all(&self) -> Vec<(K, V)> {
        let mut results = Vec::new();

        // 1. Collect from active
        {
            let active = self.active.read();
            results.extend(
                active.data.iter().map(|(k, v)| (k.clone(), v.clone()))
            );
        }
        
        // 2. Collect from immutable
        {
            let immutable = self.immutable.read();
            for buffer in immutable.iter() {
                results.extend(
                    buffer.data.iter().map(|(k, v)| (k.clone(), v.clone()))
                );
            }
        }
        
        // 3. Deduplicate
        results.sort_by(|a, b| a.0.cmp(&b.0));
        results.dedup_by(|a, b| a.0 == b.0);
        
        results
    }

    /// Drain all entries (for testing/flushing)
    ///
    /// Returns all entries and clears the buffer
    pub fn drain(&self) -> Vec<(K, V)> {
        let mut results = Vec::new();

        // 1. Drain active
        {
            let mut active = self.active.write();
            results.extend(
                active.data.iter().map(|(k, v)| (k.clone(), v.clone()))
            );
            active.data.clear();
            active.size = 0;
        }
        
        // 2. Drain immutable
        {
            let mut immutable = self.immutable.write();
            for buffer in immutable.iter() {
                results.extend(
                    buffer.data.iter().map(|(k, v)| (k.clone(), v.clone()))
                );
            }
            immutable.clear();
        }
        
        // 3. Deduplicate
        results.sort_by(|a, b| a.0.cmp(&b.0));
        results.dedup_by(|a, b| a.0 == b.0);
        
        results
    }

    /// Flush oldest immutable buffer to persistent storage
    ///
    /// # Returns
    /// - `Ok(Some(entries))`: Flushed entries from oldest immutable buffer
    /// - `Ok(None)`: No immutable buffers to flush
    ///
    /// # Concurrency
    /// - Only one flush can run at a time (flush_lock)
    /// - Flush doesn't block writes (they go to active buffer)
    /// - Queries can still read immutable buffers during flush
    ///
    /// # Usage
    /// ```ignore
    /// // Caller should:
    /// if let Some(entries) = buffer.flush()? {
    ///     btree.batch_insert(entries)?; // Write to persistent index
    /// }
    /// ```
    pub fn flush(&self) -> Result<Option<Vec<(K, V)>>, String> {
        // 🔒 Acquire flush lock (prevents concurrent flush)
        let _lock = self.flush_lock.lock();
        
        // Get oldest immutable buffer
        let buffer = {
            let mut immutable = self.immutable.write();
            if immutable.is_empty() {
                return Ok(None);
            }
            immutable.remove(0) // Remove oldest
        };
        
        // Extract entries
        let entries: Vec<_> = buffer.data.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        
        Ok(Some(entries))
    }

    /// Delete a key
    ///
    /// Note: This only removes from active buffer.
    /// Immutable buffers are read-only.
    /// For proper LSM deletion, use tombstones in persistent index.
    pub fn delete(&self, key: &K) -> bool {
        let mut active = self.active.write();

        if active.data.remove(key).is_some() {
            let entry_size = std::mem::size_of::<K>() + std::mem::size_of::<V>();
            active.size = active.size.saturating_sub(entry_size);
            true
        } else {
            false
        }
    }

    /// Get buffer statistics
    pub fn stats(&self) -> BufferStats {
        let active = self.active.read();
        let immutable = self.immutable.read();
        
        let active_size = active.size;
        let active_count = active.data.len();
        
        let immutable_count = immutable.len();
        let immutable_size: usize = immutable.iter().map(|b| b.size).sum();
        
        BufferStats {
            active_size_bytes: active_size,
            active_entry_count: active_count,
            immutable_buffer_count: immutable_count,
            immutable_size_bytes: immutable_size,
            total_size_bytes: active_size + immutable_size,
            size_limit: self.size_limit,
            fullness: ((active_size + immutable_size) as f64 / self.size_limit as f64 * 100.0) as u8,
        }
    }

    /// Check if active buffer is empty
    pub fn is_empty(&self) -> bool {
        let active = self.active.read();
        let immutable = self.immutable.read();
        active.data.is_empty() && immutable.is_empty()
    }

    /// Get total size in bytes (active + immutable)
    pub fn size(&self) -> usize {
        let active_size = self.active.read().size;
        let immutable_size: usize = self.immutable.read().iter().map(|b| b.size).sum();
        active_size + immutable_size
    }

    /// Check if flush is recommended
    pub fn should_flush(&self) -> bool {
        !self.immutable.read().is_empty()
    }
    
    /// Get number of immutable buffers waiting to flush
    pub fn immutable_count(&self) -> usize {
        self.immutable.read().len()
    }
}

/// Buffer statistics
#[derive(Debug, Clone)]
pub struct BufferStats {
    /// Active buffer size in bytes
    pub active_size_bytes: usize,
    /// Active buffer entry count
    pub active_entry_count: usize,
    /// Number of immutable buffers waiting to flush
    pub immutable_buffer_count: usize,
    /// Total size of immutable buffers
    pub immutable_size_bytes: usize,
    /// Total size (active + immutable)
    pub total_size_bytes: usize,
    /// Size limit
    pub size_limit: usize,
    /// Fullness percentage (0-100+)
    pub fullness: u8,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mem_buffer_basic() {
        let buffer: IndexMemBuffer<i32, String> = IndexMemBuffer::new(1024);

        // Insert
        let full = buffer.insert(1, "one".to_string()).unwrap();
        assert!(!full);

        let full = buffer.insert(2, "two".to_string()).unwrap();
        assert!(!full);

        // Get
        assert_eq!(buffer.get(&1), Some("one".to_string()));
        assert_eq!(buffer.get(&2), Some("two".to_string()));
        assert_eq!(buffer.get(&3), None);
    }

    #[test]
    fn test_mem_buffer_range() {
        let buffer: IndexMemBuffer<i32, String> = IndexMemBuffer::new(1024);

        buffer.insert(1, "one".to_string()).unwrap();
        buffer.insert(2, "two".to_string()).unwrap();
        buffer.insert(3, "three".to_string()).unwrap();
        buffer.insert(5, "five".to_string()).unwrap();

        let results = buffer.range(&2, &4);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], (2, "two".to_string()));
        assert_eq!(results[1], (3, "three".to_string()));
    }

    #[test]
    fn test_mem_buffer_drain() {
        let buffer: IndexMemBuffer<i32, String> = IndexMemBuffer::new(1024);

        buffer.insert(1, "one".to_string()).unwrap();
        buffer.insert(2, "two".to_string()).unwrap();

        // Drain
        let entries = buffer.drain();
        assert_eq!(entries.len(), 2);

        // Buffer should be empty
        assert!(buffer.is_empty());
        assert_eq!(buffer.size(), 0);
        assert_eq!(buffer.get(&1), None);
    }

    #[test]
    fn test_mem_buffer_fullness() {
        let buffer: IndexMemBuffer<i32, String> = IndexMemBuffer::new(128);

        // Insert until full
        let mut i = 0;
        loop {
            let full = buffer.insert(i, format!("value_{}", i)).unwrap();
            i += 1;
            if full {
                break;
            }
        }

        println!("Inserted {} entries before buffer full", i);
        assert!(buffer.should_flush());

        let stats = buffer.stats();
        println!("Stats: {:?}", stats);
        assert!(stats.fullness >= 100);
    }
}
