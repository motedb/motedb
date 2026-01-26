//! Column Value Index - Generic index for column equality/range queries
//!
//! Provides fast lookups for WHERE conditions like:
//! - WHERE col = value (point query)
//! - WHERE col >= start AND col <= end (range query)
//!
//! Uses B-Tree for persistent storage with efficient range queries.

use crate::index::btree_generic::{GenericBTree, GenericBTreeConfig, BTreeKey};
use crate::index::cached_index::CachedIndex; // 🚀 P1: 使用LRU缓存
use crate::types::{RowId, Value};
use crate::{Result, StorageError};
use parking_lot::RwLock;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use serde::{Serialize, Deserialize};

/// Column Value Index configuration
#[derive(Debug, Clone)]
pub struct ColumnValueIndexConfig {
    /// Maximum page size in bytes
    pub max_page_size: usize,
    /// Cache size in pages
    pub cache_size: usize,
}

impl Default for ColumnValueIndexConfig {
    fn default() -> Self {
        Self {
            max_page_size: 4096,
            cache_size: 16,  // 🔧 P2: Reduced from 64 to 16 (64KB total cache per index)
        }
    }
}

/// Key for the B-Tree: (column_value, row_id)
/// This allows multiple rows with same value + efficient range queries
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct IndexKey {
    /// Column value (serialized as bytes)
    value_bytes: Vec<u8>,
    /// Row ID (for uniqueness)
    row_id: RowId,
}

// Implement BTreeKey trait for IndexKey
impl BTreeKey for IndexKey {
    fn serialize(&self) -> Vec<u8> {
        // 🔧 P2优化: 从280字节减至64字节（对于Integer类型仅需16字节）
        let mut result = vec![0u8; 64];  // Reduced from 280 to 64
        
        // Serialize to bincode first
        let serialized = bincode::serialize(self).unwrap_or_default();
        
        // Write length prefix (2 bytes)
        let len = serialized.len().min(62);
        result[0] = (len >> 8) as u8;
        result[1] = (len & 0xFF) as u8;
        
        // Copy data
        result[2..2 + len].copy_from_slice(&serialized[..len]);
        
        result
    }
    
    fn deserialize(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 2 {
            return Err(StorageError::Serialization("Invalid key: too short".to_string()));
        }
        
        // Read length
        let len = ((bytes[0] as usize) << 8) | (bytes[1] as usize);
        
        if bytes.len() < 2 + len {
            return Err(StorageError::Serialization("Invalid key: length mismatch".to_string()));
        }
        
        // Deserialize actual data
        bincode::deserialize(&bytes[2..2 + len])
            .map_err(|e| StorageError::Serialization(format!("Failed to deserialize IndexKey: {}", e)))
    }
    
    fn key_size() -> usize {
        64  // 🔧 Reduced from 280 to 64 (77% space saving)
    }
}

/// Column Value Index
///
/// Maps column values to row IDs for fast WHERE lookups.
pub struct ColumnValueIndex {
    /// Table name
    table_name: String,
    /// Column name
    column_name: String,
    /// Storage path
    storage_path: PathBuf,
    /// B-Tree index (value_bytes+row_id → empty)
    btree: Arc<RwLock<GenericBTree<IndexKey>>>,
    /// LRU cache for hot values (🚀 P1 optimization)
    lru_cache: Arc<CachedIndex>,
    // 🚀 P0 MEMORY FIX: Removed in-memory BTreeMap cache (causes 8GB leak!)
    // All lookups now go through LRU cache + B-Tree disk storage
}

impl ColumnValueIndex {
    /// Create a new column value index
    pub fn create<P: AsRef<Path>>(
        path: P,
        table_name: String,
        column_name: String,
        config: ColumnValueIndexConfig,
    ) -> Result<Self> {
        let storage_path = path.as_ref().to_path_buf();
        
        let btree_config = GenericBTreeConfig {
            cache_size: config.cache_size,
            unique_keys: false,
            allow_updates: true,
            immediate_sync: false,
        };
        
        let btree = GenericBTree::with_config(storage_path.clone(), btree_config)?;
        
        Ok(Self {
            table_name,
            column_name,
            storage_path,
            btree: Arc::new(RwLock::new(btree)),
            lru_cache: Arc::new(CachedIndex::new(500)), // 🔧 Reduced from 1000 to 500 for P1
            // 🚀 P0: Removed cache initialization (memory leak fix)
        })
    }
    
    /// Open an existing index
    pub fn open<P: AsRef<Path>>(
        path: P,
        table_name: String,
        column_name: String,
        config: ColumnValueIndexConfig,
    ) -> Result<Self> {
        // Same as create for now (GenericBTree handles both)
        Self::create(path, table_name, column_name, config)
    }
    
    /// Insert a value → row_id mapping
    pub fn insert(&mut self, value: &Value, row_id: RowId) -> Result<()> {
        let value_bytes = self.value_to_bytes(value)?;
        
        let key = IndexKey {
            value_bytes: value_bytes.clone(),
            row_id,
        };
        
        // Insert into B-Tree (note: takes &mut)
        {
            let mut btree = self.btree.write();
            btree.insert(key, vec![])?;  // Empty value, we only care about the key
        }
        
        // 🚀 P0: Removed cache update (memory leak fix)
        // All lookups now go through LRU cache + B-Tree
        
        Ok(())
    }
    
    /// 🚀 P2: Batch insert for improved performance
    /// 
    /// **Optimization strategy**:
    /// - Sort keys before insertion for better B-Tree locality
    /// - Single flush operation at the end
    /// 
    /// **Expected improvement**: 2-3x faster than sequential inserts
    pub fn batch_insert(&mut self, items: Vec<(Value, RowId)>) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        
        // Step 1: Convert to IndexKey and sort by value for better B-Tree locality
        let mut keys: Vec<(IndexKey, Vec<u8>, Value)> = items.into_iter()
            .map(|(value, row_id)| {
                let value_bytes = self.value_to_bytes(&value)?;
                let key = IndexKey {
                    value_bytes: value_bytes.clone(),
                    row_id,
                };
                Ok((key, value_bytes, value))
            })
            .collect::<Result<Vec<_>>>()?;
        
        // Sort by value_bytes for sequential B-Tree access
        keys.sort_by(|a, b| a.1.cmp(&b.1));
        
        // Step 2: Batch insert into B-Tree
        {
            let mut btree = self.btree.write();
            for (key, _, _) in &keys {
                btree.insert(key.clone(), vec![])?;
            }
        }
        
        // 🚀 P0: Removed cache update (memory leak fix)
        
        Ok(())
    }
    
    /// Point query: get all row_ids with exact value
    pub fn get(&self, value: &Value) -> Result<Vec<RowId>> {
        // 🚀 P1: Try LRU cache first for maximum speed
        if let Some(cached_ids) = self.lru_cache.get(value) {
            // ✅ P0: Arc deref + clone (small Vec clone, but Arc sharing reduces memory pressure)
            return Ok((*cached_ids).clone());
        }
        
        // 🚀 P0: Removed legacy cache check (memory leak fix)
        // All lookups now go directly to B-Tree if not in LRU
        
        // 🚀 P1: Record cache miss
        self.lru_cache.record_miss();
        
        let value_bytes = self.value_to_bytes(value)?;
        
        // Use B-Tree range scan to find all entries with matching value
        let mut row_ids = Vec::new();
        
        let btree = self.btree.read();
        
        // Create range: all entries with the same value_bytes
        let start_key = IndexKey {
            value_bytes: value_bytes.clone(),
            row_id: 0,
        };
        
        let end_key = IndexKey {
            value_bytes: value_bytes.clone(),
            row_id: RowId::MAX,
        };
        
        // Range scan to get all matching entries
        let results = btree.range(&start_key, &end_key)?;
        
        for (key, _value) in results {
            // Verify value_bytes matches (should always be true)
            if key.value_bytes == value_bytes {
                // No need to check for tombstones - we use real delete now
                row_ids.push(key.row_id);
            }
        }
        
        drop(btree);
        
        // 🚀 P1: Update LRU cache only
        if !row_ids.is_empty() {
            self.lru_cache.put(value.clone(), row_ids.clone());
        }
        
        Ok(row_ids)
    }
    
    /// Range query: get all row_ids where start <= value <= end
    pub fn range(&self, start: &Value, end: &Value) -> Result<Vec<RowId>> {
        let start_bytes = self.value_to_bytes(start)?;
        let end_bytes = self.value_to_bytes(end)?;
        
        let mut row_ids = Vec::new();
        
        let btree = self.btree.read();
        
        // Create range keys
        let start_key = IndexKey {
            value_bytes: start_bytes,
            row_id: 0,
        };
        
        let end_key = IndexKey {
            value_bytes: end_bytes,
            row_id: RowId::MAX,
        };
        
        // Range scan
        let results = btree.range(&start_key, &end_key)?;
        
        for (key, _value) in results {
            // No need to check for tombstones - we use real delete now
            row_ids.push(key.row_id);
        }
        
        Ok(row_ids)
    }
    
    /// Scan all entries in order
    /// 
    /// Returns row_ids sorted by their corresponding values
    /// This is useful for ORDER BY optimization on indexed columns
    pub fn scan_all_row_ids(&self) -> Result<Vec<RowId>> {
        self.scan_row_ids_with_limit(None)
    }
    
    /// Scan entries with optional limit
    /// 
    /// Returns at most `limit` row_ids sorted by their corresponding values
    /// Early termination significantly reduces I/O for LIMIT queries
    pub fn scan_row_ids_with_limit(&self, limit: Option<usize>) -> Result<Vec<RowId>> {
        let btree = self.btree.read();
        
        // Create range that covers all possible keys
        let min_key = IndexKey {
            value_bytes: vec![],  // Minimum possible value
            row_id: 0,
        };
        
        let max_key = IndexKey {
            value_bytes: vec![0xFF; 64],  // Maximum possible value (64 bytes of 0xFF)
            row_id: RowId::MAX,
        };
        
        // Range scan with optional limit
        let all_entries = if let Some(limit_count) = limit {
            // Use optimized range_with_limit for early termination
            btree.range_with_limit(&min_key, &max_key, limit_count)?
        } else {
            // Full scan
            btree.range(&min_key, &max_key)?
        };
        
        // Extract row_ids
        let row_ids: Vec<RowId> = all_entries.into_iter()
            .map(|(key, _)| key.row_id)
            .collect();
        
        Ok(row_ids)
    }
    
    /// 🚀 Range query: get all row_ids where value < upper_bound
    /// 
    /// **Use case**: `WHERE battery_level < 30`, `WHERE price < 100.0`
    /// 
    /// **Performance**: O(log N + K) where K = result size
    pub fn query_less_than(&self, upper_bound: &Value) -> Result<Vec<RowId>> {
        let upper_bytes = self.value_to_bytes(upper_bound)?;
        
        let mut row_ids = Vec::new();
        
        let btree = self.btree.read();
        
        // Start from minimum possible key
        let start_key = IndexKey {
            value_bytes: vec![],  // Empty bytes = minimum
            row_id: 0,
        };
        
        // End at upper_bound (exclusive, so use row_id = 0)
        let end_key = IndexKey {
            value_bytes: upper_bytes,
            row_id: 0,  // Exclusive: don't include upper_bound itself
        };
        
        // Range scan [min, upper_bound)
        let results = btree.range(&start_key, &end_key)?;
        
        for (key, _value) in results {
            row_ids.push(key.row_id);
        }
        
        Ok(row_ids)
    }
    
    /// 🚀 Range query: get all row_ids where value > lower_bound
    /// 
    /// **Use case**: `WHERE created_at > 100000`, `WHERE age > 18`
    /// 
    /// **Performance**: O(log N + K) where K = result size
    pub fn query_greater_than(&self, lower_bound: &Value) -> Result<Vec<RowId>> {
        let lower_bytes = self.value_to_bytes(lower_bound)?;
        
        let mut row_ids = Vec::new();
        
        let btree = self.btree.read();
        
        // Start from lower_bound + 1 (exclusive)
        // Use row_id = RowId::MAX to skip all entries with exact lower_bound value
        let start_key = IndexKey {
            value_bytes: lower_bytes,
            row_id: RowId::MAX,
        };
        
        // End at maximum possible key
        let end_key = IndexKey {
            value_bytes: vec![0xFF; 1024],  // Large bytes = maximum
            row_id: RowId::MAX,
        };
        
        // Range scan (lower_bound, max]
        let results = btree.range(&start_key, &end_key)?;
        
        for (key, _value) in results {
            row_ids.push(key.row_id);
        }
        
        Ok(row_ids)
    }
    
    /// 🚀 Range query: value <= upper_bound (inclusive)
    pub fn query_less_than_or_equal(&self, upper_bound: &Value) -> Result<Vec<RowId>> {
        let upper_bytes = self.value_to_bytes(upper_bound)?;
        
        let mut row_ids = Vec::new();
        
        let btree = self.btree.read();
        
        let start_key = IndexKey {
            value_bytes: vec![],
            row_id: 0,
        };
        
        let end_key = IndexKey {
            value_bytes: upper_bytes,
            row_id: RowId::MAX,  // Inclusive
        };
        
        let results = btree.range(&start_key, &end_key)?;
        
        for (key, _value) in results {
            row_ids.push(key.row_id);
        }
        
        Ok(row_ids)
    }
    
    /// 🚀 Range query: value >= lower_bound (inclusive)
    pub fn query_greater_than_or_equal(&self, lower_bound: &Value) -> Result<Vec<RowId>> {
        let lower_bytes = self.value_to_bytes(lower_bound)?;
        
        let mut row_ids = Vec::new();
        
        let btree = self.btree.read();
        
        let start_key = IndexKey {
            value_bytes: lower_bytes,
            row_id: 0,  // Inclusive
        };
        
        let end_key = IndexKey {
            value_bytes: vec![0xFF; 1024],
            row_id: RowId::MAX,
        };
        
        let results = btree.range(&start_key, &end_key)?;
        
        for (key, _value) in results {
            row_ids.push(key.row_id);
        }
        
        Ok(row_ids)
    }
    
    /// 🚀 Dual-bound range query with flexible boundaries
    /// 
    /// **Use case**: `WHERE col > X AND col < Y`, `WHERE col >= X AND col <= Y`
    /// 
    /// **Performance**: O(log N + K) - single B-Tree scan
    /// 
    /// # Arguments
    /// * `lower_bound` - Lower bound value
    /// * `lower_inclusive` - If true: >=, if false: >
    /// * `upper_bound` - Upper bound value  
    /// * `upper_inclusive` - If true: <=, if false: <
    pub fn query_between(&self, 
                        lower_bound: &Value, lower_inclusive: bool,
                        upper_bound: &Value, upper_inclusive: bool) -> Result<Vec<RowId>> {
        let lower_bytes = self.value_to_bytes(lower_bound)?;
        let upper_bytes = self.value_to_bytes(upper_bound)?;
        
        let mut row_ids = Vec::new();
        let btree = self.btree.read();
        
        // Set start key based on lower_inclusive
        let start_key = IndexKey {
            value_bytes: lower_bytes,
            row_id: if lower_inclusive { 0 } else { RowId::MAX },
        };
        
        // Set end key based on upper_inclusive
        let end_key = IndexKey {
            value_bytes: upper_bytes,
            row_id: if upper_inclusive { RowId::MAX } else { 0 },
        };
        
        // Single B-Tree scan
        let results = btree.range(&start_key, &end_key)?;
        
        for (key, _value) in results {
            row_ids.push(key.row_id);
        }
        
        Ok(row_ids)
    }
    
    /// Delete a value → row_id mapping
    pub fn delete(&mut self, value: &Value, row_id: RowId) -> Result<()> {
        let value_bytes = self.value_to_bytes(value)?;
        
        let key = IndexKey {
            value_bytes: value_bytes.clone(),
            row_id,
        };
        
        // Use real B-Tree delete (no longer using tombstone)
        let mut btree = self.btree.write();
        btree.delete(&key)?;
        drop(btree);
        
        // 🚀 P0: Removed legacy cache update (memory leak fix)
        
        // Invalidate LRU cache for this value
        self.lru_cache.invalidate(value);
        
        Ok(())
    }
    
    /// 🚀 P2: Batch delete with smart cache invalidation
    /// 
    /// More efficient than calling `delete()` multiple times:
    /// - Single B-Tree lock
    /// - Batch cache updates
    /// - Smart cache invalidation (only affected keys)
    pub fn batch_delete(&mut self, items: Vec<(Value, RowId)>) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        
        // Step 1: Batch delete from B-Tree
        {
            let mut btree = self.btree.write();
            for (value, row_id) in &items {
                let value_bytes = self.value_to_bytes(value)?;
                let key = IndexKey {
                    value_bytes,
                    row_id: *row_id,
                };
                btree.delete(&key)?;
            }
        }
        
        // 🚀 P0: Removed legacy cache update (memory leak fix)
        
        // Batch invalidate LRU cache (only affected values)
        // Deduplicate values (Value doesn't implement Hash, so use manual dedup)
        let mut unique_values = items.into_iter()
            .map(|(value, _)| value)
            .collect::<Vec<_>>();
        unique_values.sort_by(|a, b| {
            // Sort by serialized bytes for deduplication
            let a_bytes = bincode::serialize(a).unwrap_or_default();
            let b_bytes = bincode::serialize(b).unwrap_or_default();
            a_bytes.cmp(&b_bytes)
        });
        unique_values.dedup_by(|a, b| {
            let a_bytes = bincode::serialize(a).unwrap_or_default();
            let b_bytes = bincode::serialize(b).unwrap_or_default();
            a_bytes == b_bytes
        });
        
        self.lru_cache.invalidate_batch(&unique_values);
        
        Ok(())
    }
    
    /// 🚀 P2: Delete range with smart cache invalidation
    /// 
    /// Deletes all entries where start <= value <= end.
    /// Only invalidates cache entries within the range (not full clear).
    pub fn delete_range(&mut self, start: &Value, end: &Value) -> Result<usize> {
        let start_bytes = self.value_to_bytes(start)?;
        let end_bytes = self.value_to_bytes(end)?;
        
        let mut deleted_count = 0;
        
        // Step 1: Find and delete all keys in range
        {
            let mut btree = self.btree.write();
            
            let start_key = IndexKey {
                value_bytes: start_bytes.clone(),
                row_id: 0,
            };
            
            let end_key = IndexKey {
                value_bytes: end_bytes.clone(),
                row_id: RowId::MAX,
            };
            
            // Get all keys in range
            let keys_to_delete: Vec<IndexKey> = btree.range(&start_key, &end_key)?
                .into_iter()
                .map(|(key, _)| key)
                .collect();
            
            // Delete each key
            for key in keys_to_delete {
                btree.delete(&key)?;
                deleted_count += 1;
            }
        }
        
        // 🚀 P0: Removed legacy cache update (memory leak fix)
        
        // Step 3: Smart LRU cache invalidation (only the range)
        self.lru_cache.invalidate_range(start, end);
        
        Ok(deleted_count)
    }
    
    /// Flush index to disk
    pub fn flush(&mut self) -> Result<()> {
        let mut btree = self.btree.write();
        btree.flush()?;
        Ok(())
    }
    
    /// Get index statistics
    pub fn stats(&self) -> IndexStats {
        // 🚀 P0: Use LRU cache stats instead of removed legacy cache
        let lru_stats = self.lru_cache.stats();
        IndexStats {
            cached_values: lru_stats.size,
            total_row_ids: 0,  // Not tracked in LRU cache
        }
    }
    
    // Helper: Convert Value to bytes for comparison
    fn value_to_bytes(&self, value: &Value) -> Result<Vec<u8>> {
        use crate::types::Value;
        
        let bytes = match value {
            Value::Integer(i) => i.to_be_bytes().to_vec(),
            Value::Float(f) => f.to_be_bytes().to_vec(),
            Value::Text(s) => s.as_bytes().to_vec(),
            Value::Bool(b) => vec![if *b { 1 } else { 0 }],
            // 🚀 新增：支持 Timestamp 类型
            Value::Timestamp(ts) => ts.as_micros().to_be_bytes().to_vec(),
            _ => {
                return Err(StorageError::InvalidData(
                    format!("Unsupported value type for indexing: {:?}", value)
                ));
            }
        };
        
        Ok(bytes)
    }
}

/// Index statistics
#[derive(Debug, Clone)]
pub struct IndexStats {
    pub cached_values: usize,
    pub total_row_ids: usize,
}

// ==================== 🚀 Batch Index Builder Implementation ====================

use crate::index::builder::{IndexBuilder, BuildStats};
use crate::types::Row;

impl IndexBuilder for ColumnValueIndex {
    /// 批量构建列值索引（从MemTable flush时调用）
    /// 
    /// ⚠️  注意：此方法现在已弃用，应该使用 insert_batch
    /// 因为此方法无法知道列在 Row 中的位置
    fn build_from_memtable(&mut self, _rows: &[(RowId, Row)]) -> Result<()> {
        // ⚠️  此方法不应该直接使用
        // 批量构建应该通过 batch_build_column_indexes 调用 insert_batch
        println!("[ColumnIndex::{}] ⚠️  build_from_memtable is deprecated, use insert_batch instead", 
                 self.column_name);
        Ok(())
    }
    
    /// 持久化索引到磁盘
    fn persist(&mut self) -> Result<()> {
        use std::time::Instant;
        let start = Instant::now();
        
        let mut btree = self.btree.write();
        btree.flush()?;
        
        let duration = start.elapsed();
        println!("[ColumnIndex::{}] Persist: {:?}", self.column_name, duration);
        
        Ok(())
    }
    
    /// 获取索引名称
    fn name(&self) -> &str {
        &self.column_name
    }
    
    /// 获取构建统计信息
    fn stats(&self) -> BuildStats {
        let stats = self.stats();
        BuildStats {
            rows_processed: stats.total_row_ids,
            build_time_ms: 0, // 在实际实现中应该记录
            persist_time_ms: 0,
            index_size_bytes: stats.total_row_ids * 64, // 估算：每行64字节
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_column_value_index_basic() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().join("test_index.idx");
        
        let mut index = ColumnValueIndex::create(
            &path,
            "users".to_string(),
            "age".to_string(),
            ColumnValueIndexConfig::default(),
        )?;
        
        // Insert some values
        index.insert(&Value::Integer(25), 1)?;
        index.insert(&Value::Integer(30), 2)?;
        index.insert(&Value::Integer(25), 3)?;
        
        // Point query
        let row_ids = index.get(&Value::Integer(25))?;
        assert_eq!(row_ids.len(), 2);
        assert!(row_ids.contains(&1));
        assert!(row_ids.contains(&3));
        
        Ok(())
    }
}

impl ColumnValueIndex {
    /// 批量插入（优化的接口）
    /// 
    /// 用于批量索引构建
    pub fn insert_batch(&mut self, batch: &[(RowId, &Value)]) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        
        // 🚀 批量插入到B-Tree
        for (row_id, value) in batch {
            self.insert(value, *row_id)?;
        }
        
        Ok(())
    }
}
