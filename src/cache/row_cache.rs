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
use dashmap::DashMap;
use lru::LruCache;
use parking_lot::RwLock;
use std::num::NonZeroUsize;
use std::sync::Arc;

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
    /// 
    /// Using Arc<Row> to allow cheap cloning when returning cached data
    cache: Arc<RwLock<LruCache<CacheKey, Arc<Row>>>>,
    
    /// Statistics
    stats: Arc<RwLock<CacheStats>>,
    
    /// 🚀 P2: Access pattern tracker per table - 使用 DashMap 提升并发性能
    /// Tracks recent access patterns to detect sequential scans
    access_patterns: Arc<DashMap<String, AccessPattern>>,
    
    /// 🚀 P2: Prefetch configuration
    prefetch_config: PrefetchConfig,
}

/// Cache statistics
#[derive(Debug, Default, Clone)]
pub struct CacheStats {
    /// Total cache hits
    pub hits: u64,
    /// Total cache misses
    pub misses: u64,
    /// Current cache size
    pub size: usize,
    /// Maximum cache size
    pub capacity: usize,
    /// 🚀 P2: Prefetch statistics
    pub prefetch_triggered: u64,
    pub prefetch_useful: u64,  // Prefetched rows that were actually used
}

impl CacheStats {
    /// Calculate hit rate
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
    
    /// 🚀 P2: Calculate prefetch efficiency
    pub fn prefetch_efficiency(&self) -> f64 {
        if self.prefetch_triggered == 0 {
            0.0
        } else {
            self.prefetch_useful as f64 / self.prefetch_triggered as f64
        }
    }
}

/// 🚀 P2: Prefetch configuration
#[derive(Debug, Clone)]
pub struct PrefetchConfig {
    /// Enable prefetching
    pub enabled: bool,
    /// Minimum sequential accesses to trigger prefetching
    pub min_sequential_count: usize,
    /// Number of rows to prefetch ahead
    pub prefetch_size: usize,
    /// Maximum stride (gap between consecutive IDs) to consider sequential
    pub max_stride: i64,
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_sequential_count: 3,  // Trigger after 3 consecutive sequential accesses
            prefetch_size: 32,        // Prefetch 32 rows ahead
            max_stride: 100,          // Max gap of 100 to consider sequential
        }
    }
}

impl RowCache {
    /// Create a new row cache
    /// 
    /// # Arguments
    /// * `capacity` - Maximum number of rows to cache (default: 10000)
    /// 
    /// # Memory Usage
    /// Assuming 1KB/row average: 10000 rows ≈ 10MB
    pub fn new(capacity: usize) -> Self {
        Self::with_prefetch_config(capacity, PrefetchConfig::default())
    }
    
    /// 🚀 P2: Create a new row cache with custom prefetch configuration
    pub fn with_prefetch_config(capacity: usize, prefetch_config: PrefetchConfig) -> Self {
        let capacity = capacity.max(1);  // Minimum 1 row (changed from 100 for testing)
        
        Self {
            cache: Arc::new(RwLock::new(
                LruCache::new(NonZeroUsize::new(capacity).unwrap())
            )),
            stats: Arc::new(RwLock::new(CacheStats {
                hits: 0,
                misses: 0,
                size: 0,
                capacity,
                prefetch_triggered: 0,
                prefetch_useful: 0,
            })),
            access_patterns: Arc::new(DashMap::new()),
            prefetch_config,
        }
    }
    
    /// Get a row from cache
    /// 
    /// Returns Some(Arc<Row>) if found (cache hit), None otherwise (cache miss)
    /// 
    /// 🚀 P2: Also updates access pattern for prefetch detection
    pub fn get(&self, table_name: &str, row_id: RowId) -> Option<Arc<Row>> {
        let key = (table_name.to_string(), row_id);
        
        let mut cache = self.cache.write();
        
        if let Some(row) = cache.get(&key) {
            // Cache hit
            let mut stats = self.stats.write();
            stats.hits += 1;
            
            // 🚀 P2: Update access pattern
            drop(stats);  // Release lock before pattern update
            self.update_access_pattern(table_name, row_id);
            
            Some(Arc::clone(row))
        } else {
            // Cache miss
            let mut stats = self.stats.write();
            stats.misses += 1;
            
            // 🚀 P2: Update access pattern
            drop(stats);  // Release lock before pattern update
            self.update_access_pattern(table_name, row_id);
            
            None
        }
    }
    
    /// 🚀 P2: Update access pattern and detect sequential scans
    /// 
    /// Returns prefetch recommendation: Some((start_row_id, count)) if prefetch should be triggered
    fn update_access_pattern(&self, table_name: &str, row_id: RowId) -> Option<(RowId, usize)> {
        if !self.prefetch_config.enabled {
            return None;
        }
        
        let now = std::time::Instant::now();
        
        // 🚀 使用 DashMap entry API 进行原子更新
        let should_prefetch = match self.access_patterns.entry(table_name.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let pattern = entry.get_mut();
                
                // Age out old patterns (>1 second old)
                if now.duration_since(pattern.last_access).as_secs() > 1 {
                    // Reset pattern
                    pattern.last_row_id = row_id;
                    pattern.stride = 0;
                    pattern.sequential_count = 1;
                    pattern.last_access = now;
                    return None;
                }
                
                let stride = row_id as i64 - pattern.last_row_id as i64;
                
                // Check if stride matches (sequential access)
                if stride == pattern.stride && stride.abs() <= self.prefetch_config.max_stride {
                    // Consecutive sequential access
                    pattern.sequential_count += 1;
                    pattern.last_row_id = row_id;
                    pattern.last_access = now;
                    
                    // Trigger prefetch if threshold reached
                    if pattern.sequential_count >= self.prefetch_config.min_sequential_count {
                        // Calculate prefetch range
                        let next_row_id = (row_id as i64 + stride) as RowId;
                        Some((next_row_id, self.prefetch_config.prefetch_size))
                    } else {
                        None
                    }
                } else if stride.abs() <= self.prefetch_config.max_stride {
                    // New stride detected, reset count
                    pattern.stride = stride;
                    pattern.sequential_count = 2;  // Current + previous
                    pattern.last_row_id = row_id;
                    pattern.last_access = now;
                    None
                } else {
                    // Random access, reset
                    pattern.stride = 0;
                    pattern.sequential_count = 1;
                    pattern.last_row_id = row_id;
                    pattern.last_access = now;
                    None
                }
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                // First access for this table
                entry.insert(AccessPattern {
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
    
    /// 🚀 P2: Check if prefetch should be triggered for current access
    /// 
    /// Returns Some((start_row_id, count, stride)) if prefetch is recommended
    pub fn check_prefetch(&self, table_name: &str, row_id: RowId) -> Option<(RowId, usize, i64)> {
        if !self.prefetch_config.enabled {
            return None;
        }
        
        // 🚀 使用 DashMap 的 get() 方法
        if let Some(pattern_ref) = self.access_patterns.get(table_name) {
            let pattern = pattern_ref.value();
            
            // Check if pattern is fresh (accessed within 1 second)
            if pattern.last_access.elapsed().as_secs() > 1 {
                return None;
            }
            
            // Check if enough sequential accesses observed
            if pattern.sequential_count >= self.prefetch_config.min_sequential_count {
                let stride = pattern.stride;
                let next_row_id = (row_id as i64 + stride) as RowId;
                
                return Some((next_row_id, self.prefetch_config.prefetch_size, stride));
            }
        }
        
        None
    }
    
    /// Put a row into cache
    /// 
    /// # Arguments
    /// * `table_name` - Table name
    /// * `row_id` - Row ID
    /// * `row` - Row data (will be wrapped in Arc for cheap cloning)
    pub fn put(&self, table_name: String, row_id: RowId, row: Row) {
        let key = (table_name, row_id);
        let row_arc = Arc::new(row);
        
        let mut cache = self.cache.write();
        cache.put(key, row_arc);
        
        // Update stats
        let mut stats = self.stats.write();
        stats.size = cache.len();
    }
    
    /// Batch put rows into cache
    /// 
    /// More efficient than calling put() multiple times
    pub fn put_batch(&self, table_name: &str, rows: Vec<(RowId, Row)>) {
        let mut cache = self.cache.write();
        
        for (row_id, row) in rows {
            let key = (table_name.to_string(), row_id);
            cache.put(key, Arc::new(row));
        }
        
        // Update stats
        let mut stats = self.stats.write();
        stats.size = cache.len();
    }
    
    /// Invalidate a single row
    /// 
    /// Called when row is updated or deleted
    pub fn invalidate(&self, table_name: &str, row_id: RowId) {
        let key = (table_name.to_string(), row_id);
        
        let mut cache = self.cache.write();
        cache.pop(&key);
        
        // Update stats
        let mut stats = self.stats.write();
        stats.size = cache.len();
    }
    
    /// Invalidate all rows for a table
    /// 
    /// Called when table is dropped or truncated
    pub fn invalidate_table(&self, table_name: &str) {
        let mut cache = self.cache.write();
        
        // Collect keys to remove (can't remove while iterating)
        let keys_to_remove: Vec<CacheKey> = cache
            .iter()
            .filter(|(key, _)| key.0 == table_name)
            .map(|(key, _)| key.clone())
            .collect();
        
        for key in keys_to_remove {
            cache.pop(&key);
        }
        
        // Update stats
        let mut stats = self.stats.write();
        stats.size = cache.len();
    }
    
    /// Clear entire cache
    pub fn clear(&self) {
        let mut cache = self.cache.write();
        cache.clear();
        
        let mut stats = self.stats.write();
        stats.size = 0;
        stats.hits = 0;
        stats.misses = 0;
        stats.prefetch_triggered = 0;
        stats.prefetch_useful = 0;
        
        // 🚀 P2: Clear access patterns - DashMap 的 clear() 方法
        self.access_patterns.clear();
    }
    
    /// 🚀 P2: Record that a prefetch was triggered
    pub fn record_prefetch(&self, count: usize) {
        let mut stats = self.stats.write();
        stats.prefetch_triggered += count as u64;
    }
    
    /// 🚀 P2: Record that a prefetched row was actually used
    pub fn record_prefetch_hit(&self) {
        let mut stats = self.stats.write();
        stats.prefetch_useful += 1;
    }
    
    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        self.stats.read().clone()
    }
    
    /// Print cache statistics
    pub fn print_stats(&self) {
        let stats = self.stats();
        println!("📊 Row Cache Statistics:");
        println!("   Hits: {}, Misses: {}", stats.hits, stats.misses);
        println!("   Hit Rate: {:.2}%", stats.hit_rate() * 100.0);
        println!("   Size: {}/{} rows", stats.size, stats.capacity);
        
        // 🚀 P2: Prefetch stats
        if self.prefetch_config.enabled {
            println!("🚀 Prefetch Statistics:");
            println!("   Triggered: {} rows", stats.prefetch_triggered);
            println!("   Useful: {} rows", stats.prefetch_useful);
            println!("   Efficiency: {:.2}%", stats.prefetch_efficiency() * 100.0);
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
        
        // Create test row
        let mut row = Row::new();
        row.push(Value::Integer(1));
        row.push(Value::Text("test".to_string()));
        
        // Cache miss
        assert!(cache.get("users", 1).is_none());
        
        // Put into cache
        cache.put("users".to_string(), 1, row.clone());
        
        // Cache hit
        let cached_row = cache.get("users", 1).unwrap();
        assert_eq!(cached_row.len(), 2);
        
        // Stats check
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
        
        // Invalidate
        cache.invalidate("users", 1);
        assert!(cache.get("users", 1).is_none());
    }
    
    #[test]
    fn test_row_cache_lru_eviction() {
        let cache = RowCache::new(3);  // Small cache
        
        // Fill cache
        for i in 1..=3 {
            let mut row = Row::new();
            row.push(Value::Integer(i));
            cache.put("users".to_string(), i as u64, row);
        }
        
        // Cache is full
        let stats = cache.stats();
        assert_eq!(stats.size, 3);
        
        // Add one more -> LRU eviction
        let mut row = Row::new();
        row.push(Value::Integer(4));
        cache.put("users".to_string(), 4, row);
        
        // Size should still be 3
        let stats = cache.stats();
        assert_eq!(stats.size, 3);
        
        // Oldest entry (1) should be evicted
        assert!(cache.get("users", 1).is_none());
        assert!(cache.get("users", 4).is_some());
    }
}
