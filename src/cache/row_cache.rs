//! Row Cache - LRU cache with sequential prefetching
//!
//! **Purpose**: Reduce LSM read latency for hot data
//!
//! **Performance**: Cache hit = <1µs, Cache miss = ~46µs (LSM read)
//!
//! **Memory**: Default 10,000 rows ≈ 10MB (assuming 1KB/row average)
//!
//! **P2 Prefetching**: Detects sequential access patterns and prefetches ahead

use crate::types::{Row, RowId};
use lru::LruCache;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Row cache key: (table_name, row_id)
pub type CacheKey = (String, RowId);

/// Access pattern tracker for sequential detection
#[derive(Debug, Clone)]
struct AccessPattern {
    /// Last accessed row_id
    last_row_id: RowId,
    /// Detected stride (difference between consecutive accesses)
    stride: i64,
    /// Consecutive sequential accesses count
    sequential_count: usize,
    /// Last access timestamp (for aging)
    last_access: std::time::Instant,
}

/// Row cache with LRU eviction and prefetching
pub struct RowCache {
    /// LRU cache: (table_name, row_id) -> Arc<Row>
    cache: Arc<RwLock<LruCache<CacheKey, Arc<Row>>>>,

    /// 🚀 Atomic counters (no lock needed for stats — eliminates double-write-lock per get())
    hits: AtomicU64,
    misses: AtomicU64,
    size: AtomicUsize,
    capacity: usize,
    prefetch_triggered: AtomicU64,
    prefetch_useful: AtomicU64,

    /// 🚀 Replaced DashMap with RwLock<HashMap> (single lock, no sharding overhead on edge)
    access_patterns: Arc<RwLock<HashMap<String, AccessPattern>>>,

    /// 🚀 P2: Prefetch configuration
    prefetch_config: PrefetchConfig,
}

/// Cache statistics (snapshot of atomic counters)
#[derive(Debug, Default, Clone)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub size: usize,
    pub capacity: usize,
    pub prefetch_triggered: u64,
    pub prefetch_useful: u64,
}

impl CacheStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 { 0.0 } else { self.hits as f64 / total as f64 }
    }

    pub fn prefetch_efficiency(&self) -> f64 {
        if self.prefetch_triggered == 0 { 0.0 }
        else { self.prefetch_useful as f64 / self.prefetch_triggered as f64 }
    }
}

/// 🚀 P2: Prefetch configuration
#[derive(Debug, Clone)]
pub struct PrefetchConfig {
    pub enabled: bool,
    pub min_sequential_count: usize,
    pub prefetch_size: usize,
    pub max_stride: i64,
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_sequential_count: 3,
            prefetch_size: 32,
            max_stride: 100,
        }
    }
}

impl RowCache {
    pub fn new(capacity: usize) -> Self {
        Self::with_prefetch_config(capacity, PrefetchConfig::default())
    }

    pub fn with_prefetch_config(capacity: usize, prefetch_config: PrefetchConfig) -> Self {
        let capacity = capacity.max(1);

        Self {
            cache: Arc::new(RwLock::new(
                LruCache::new(NonZeroUsize::new(capacity).unwrap())
            )),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            size: AtomicUsize::new(0),
            capacity,
            prefetch_triggered: AtomicU64::new(0),
            prefetch_useful: AtomicU64::new(0),
            access_patterns: Arc::new(RwLock::new(HashMap::new())),
            prefetch_config,
        }
    }

    /// Get a row from cache
    ///
    /// 🚀 Optimization: Single write-lock for cache, stats use atomics (no lock)
    pub fn get(&self, table_name: &str, row_id: RowId) -> Option<Arc<Row>> {
        let key = (table_name.to_string(), row_id);

        let mut cache = self.cache.write();

        if let Some(row) = cache.get(&key) {
            // Cache hit — clone Arc before dropping lock
            let result = Arc::clone(row);
            self.hits.fetch_add(1, Ordering::Relaxed);
            drop(cache);
            self.update_access_pattern(table_name, row_id);
            Some(result)
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            drop(cache);
            self.update_access_pattern(table_name, row_id);
            None
        }
    }

    /// 🚀 P2: Update access pattern and detect sequential scans
    fn update_access_pattern(&self, table_name: &str, row_id: RowId) -> Option<(RowId, usize)> {
        if !self.prefetch_config.enabled {
            return None;
        }

        let now = std::time::Instant::now();
        let mut patterns = self.access_patterns.write();

        let should_prefetch = match patterns.get_mut(table_name) {
            Some(pattern) => {
                if now.duration_since(pattern.last_access).as_secs() > 1 {
                    pattern.last_row_id = row_id;
                    pattern.stride = 0;
                    pattern.sequential_count = 1;
                    pattern.last_access = now;
                    return None;
                }

                let stride = row_id as i64 - pattern.last_row_id as i64;

                if stride == pattern.stride && stride.abs() <= self.prefetch_config.max_stride {
                    pattern.sequential_count += 1;
                    pattern.last_row_id = row_id;
                    pattern.last_access = now;

                    if pattern.sequential_count >= self.prefetch_config.min_sequential_count {
                        let next_row_id = (row_id as i64 + stride) as RowId;
                        Some((next_row_id, self.prefetch_config.prefetch_size))
                    } else {
                        None
                    }
                } else if stride.abs() <= self.prefetch_config.max_stride {
                    pattern.stride = stride;
                    pattern.sequential_count = 2;
                    pattern.last_row_id = row_id;
                    pattern.last_access = now;
                    None
                } else {
                    pattern.stride = 0;
                    pattern.sequential_count = 1;
                    pattern.last_row_id = row_id;
                    pattern.last_access = now;
                    None
                }
            }
            None => {
                patterns.insert(table_name.to_string(), AccessPattern {
                    last_row_id: row_id,
                    stride: 0,
                    sequential_count: 1,
                    last_access: now,
                });
                None
            }
        };

        should_prefetch
    }

    /// 🚀 P2: Check if prefetch should be triggered
    pub fn check_prefetch(&self, table_name: &str, row_id: RowId) -> Option<(RowId, usize, i64)> {
        if !self.prefetch_config.enabled {
            return None;
        }

        let patterns = self.access_patterns.read();
        if let Some(pattern) = patterns.get(table_name) {
            if pattern.last_access.elapsed().as_secs() > 1 {
                return None;
            }
            if pattern.sequential_count >= self.prefetch_config.min_sequential_count {
                let stride = pattern.stride;
                let next_row_id = (row_id as i64 + stride) as RowId;
                return Some((next_row_id, self.prefetch_config.prefetch_size, stride));
            }
        }
        None
    }

    /// Put a row into cache
    pub fn put(&self, table_name: String, row_id: RowId, row: Row) {
        let key = (table_name, row_id);
        let row_arc = Arc::new(row);

        let mut cache = self.cache.write();
        cache.put(key, row_arc);
        self.size.store(cache.len(), Ordering::Relaxed);
    }

    /// Batch put rows into cache
    pub fn put_batch(&self, table_name: &str, rows: Vec<(RowId, Row)>) {
        let mut cache = self.cache.write();

        for (row_id, row) in rows {
            let key = (table_name.to_string(), row_id);
            cache.put(key, Arc::new(row));
        }
        self.size.store(cache.len(), Ordering::Relaxed);
    }

    /// Invalidate a single row
    pub fn invalidate(&self, table_name: &str, row_id: RowId) {
        let key = (table_name.to_string(), row_id);

        let mut cache = self.cache.write();
        cache.pop(&key);
        self.size.store(cache.len(), Ordering::Relaxed);
    }

    /// Invalidate all rows for a table
    pub fn invalidate_table(&self, table_name: &str) {
        let mut cache = self.cache.write();

        let keys_to_remove: Vec<CacheKey> = cache
            .iter()
            .filter(|(key, _)| key.0 == table_name)
            .map(|(key, _)| key.clone())
            .collect();

        for key in keys_to_remove {
            cache.pop(&key);
        }
        self.size.store(cache.len(), Ordering::Relaxed);
    }

    /// Clear entire cache
    pub fn clear(&self) {
        let mut cache = self.cache.write();
        cache.clear();

        self.size.store(0, Ordering::Relaxed);
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.prefetch_triggered.store(0, Ordering::Relaxed);
        self.prefetch_useful.store(0, Ordering::Relaxed);

        self.access_patterns.write().clear();
    }

    /// 🚀 P2: Record that a prefetch was triggered
    pub fn record_prefetch(&self, count: usize) {
        self.prefetch_triggered.fetch_add(count as u64, Ordering::Relaxed);
    }

    /// 🚀 P2: Record that a prefetched row was actually used
    pub fn record_prefetch_hit(&self) {
        self.prefetch_useful.fetch_add(1, Ordering::Relaxed);
    }

    /// Get cache statistics (snapshot of atomics)
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            size: self.size.load(Ordering::Relaxed),
            capacity: self.capacity,
            prefetch_triggered: self.prefetch_triggered.load(Ordering::Relaxed),
            prefetch_useful: self.prefetch_useful.load(Ordering::Relaxed),
        }
    }

    /// Print cache statistics
    pub fn print_stats(&self) {
        let stats = self.stats();
        debug_log!("Row Cache Statistics:");
        debug_log!("   Hits: {}, Misses: {}", stats.hits, stats.misses);
        debug_log!("   Hit Rate: {:.2}%", stats.hit_rate() * 100.0);
        debug_log!("   Size: {}/{} rows", stats.size, stats.capacity);

        if self.prefetch_config.enabled {
            debug_log!("   Prefetch: triggered={}, useful={}", stats.prefetch_triggered, stats.prefetch_useful);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    #[test]
    fn test_row_cache_basic() {
        let cache = RowCache::new(100);

        let mut row = Row::new();
        row.push(Value::Integer(1));
        row.push(Value::Text("test".to_string()));

        assert!(cache.get("users", 1).is_none());

        cache.put("users".to_string(), 1, row.clone());

        let cached_row = cache.get("users", 1).unwrap();
        assert_eq!(cached_row.len(), 2);

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hit_rate(), 0.5);
    }

    #[test]
    fn test_row_cache_invalidation() {
        let cache = RowCache::new(100);

        let mut row = Row::new();
        row.push(Value::Integer(1));

        cache.put("users".to_string(), 1, row.clone());
        assert!(cache.get("users", 1).is_some());

        cache.invalidate("users", 1);
        assert!(cache.get("users", 1).is_none());
    }

    #[test]
    fn test_row_cache_lru_eviction() {
        let cache = RowCache::new(3);

        for i in 1..=3 {
            let mut row = Row::new();
            row.push(Value::Integer(i));
            cache.put("users".to_string(), i as u64, row);
        }

        let stats = cache.stats();
        assert_eq!(stats.size, 3);

        let mut row = Row::new();
        row.push(Value::Integer(4));
        cache.put("users".to_string(), 4, row);

        let stats = cache.stats();
        assert_eq!(stats.size, 3);

        assert!(cache.get("users", 1).is_none());
        assert!(cache.get("users", 4).is_some());
    }
}
