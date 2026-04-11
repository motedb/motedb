//! Bounded PK Lookup Cache
//!
//! LRU-bounded cache mapping PK values to RowIds.
//! When capacity is exceeded, least-recently-used entries are evicted
//! and queries fall back to the disk-based column index.

use crate::types::RowId;
use parking_lot::Mutex;
use std::num::NonZeroUsize;

/// Thread-safe LRU-bounded PK → RowId cache.
///
/// Memory: ~80 bytes/entry × capacity.
/// - 50K entries ≈ 4MB (default)
/// - 10K entries ≈ 800KB (edge/embedded)
pub struct PkLookupCache {
    cache: Mutex<lru::LruCache<String, RowId>>,
}

impl PkLookupCache {
    /// Create a new cache with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: Mutex::new(lru::LruCache::new(
                NonZeroUsize::new(capacity.max(1)).unwrap(),
            )),
        }
    }

    /// Insert a PK value → RowId mapping.
    /// If at capacity, evicts the least-recently-used entry.
    pub fn insert(&self, key: String, row_id: RowId) {
        let mut cache = self.cache.lock();
        cache.put(key, row_id);
    }

    /// Look up a PK value. Returns None if not cached (cache miss).
    /// Updates LRU ordering on hit.
    pub fn get(&self, key: &str) -> Option<RowId> {
        let mut cache = self.cache.lock();
        cache.get(key).copied()
    }

    /// Remove a PK value (used during DELETE).
    pub fn remove(&self, key: &str) {
        let mut cache = self.cache.lock();
        cache.pop(key);
    }

    /// Number of entries currently cached.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        let cache = self.cache.lock();
        cache.len()
    }

    /// Check if cache is empty.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        let cache = self.cache.lock();
        cache.is_empty()
    }
}
