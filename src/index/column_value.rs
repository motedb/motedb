//! Column Value Index - Generic index for column equality/range queries
//!
//! Provides fast lookups for WHERE conditions like:
//! - WHERE col = value (point query)
//! - WHERE col >= start AND col <= end (range query)
//!
//! Uses B-Tree for persistent storage with efficient range queries.
//! Uses IndexMemBuffer for lock-free reads: writes go to an in-memory
//! BTreeMap, reads check the buffer first (no btree lock needed).
//!
//! Concurrency safety:
//! - `drain_lock`: serializes drain_immutable_to_btree to prevent thundering herd
//! - `tombstones`: tracks deleted keys to prevent resurrection from immutable buffers
//! - Reads collect from buffer + btree, then filter tombstones (no deadlock)
//!
//! Tombstone key normalization:
//! - BTreeKey serialization truncates value_bytes to 12 bytes (VALUE_DATA_SIZE)
//! - Tombstone keys are normalized to the same 12-byte prefix so that
//!   deserialized btree results match their tombstones correctly for long text

use crate::database::mem_buffer::IndexMemBuffer;
use crate::index::btree_generic::{GenericBTree, GenericBTreeConfig, BTreeKey};
use crate::index::cached_index::CachedIndex;
use crate::types::{RowId, Value};
use crate::{Result, StorageError};
use parking_lot::{Mutex, RwLock};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
            cache_size: 16,
        }
    }
}

/// Compact key layout: [value_data: 12B zero-padded][row_id: 8B BE][value_len: 2B BE] = 22 bytes
/// - Integer/Float/Timestamp: value_data = 8 bytes BE + 4 bytes zero pad
/// - Text: value_data = up to 12 bytes UTF-8 + zero pad
/// - Bool: value_data = 1 byte + 11 bytes zero pad
const VALUE_DATA_SIZE: usize = 12;
const ROW_ID_SIZE: usize = 8;
const VALUE_LEN_SIZE: usize = 2;

/// Key for the B-Tree: (column_value, row_id)
/// Compact binary encoding — no bincode, no Vec allocation in serialized form.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct IndexKey {
    value_bytes: Vec<u8>,
    row_id: RowId,
}

impl std::hash::Hash for IndexKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value_bytes.hash(state);
        self.row_id.hash(state);
    }
}

/// Normalize an IndexKey for tombstone operations.
///
/// BTreeKey::serialize truncates value_bytes to VALUE_DATA_SIZE (12 bytes).
/// After a round-trip through btree, deserialized keys have truncated value_bytes
/// for long text. Tombstones must use the same normalized form so that both
/// mem-buffer keys (full) and btree keys (truncated) match correctly.
fn tombstone_key(key: &IndexKey) -> IndexKey {
    IndexKey {
        value_bytes: if key.value_bytes.len() > VALUE_DATA_SIZE {
            key.value_bytes[..VALUE_DATA_SIZE].to_vec()
        } else {
            key.value_bytes.clone()
        },
        row_id: key.row_id,
    }
}

impl BTreeKey for IndexKey {
    fn serialize(&self) -> Vec<u8> {
        let key_size = Self::key_size();
        let mut result = vec![0u8; key_size];

        // Value data (zero-padded to VALUE_DATA_SIZE)
        let val_len = self.value_bytes.len().min(VALUE_DATA_SIZE);
        result[..val_len].copy_from_slice(&self.value_bytes[..val_len]);

        // Row ID (big-endian for proper ordering)
        result[VALUE_DATA_SIZE..VALUE_DATA_SIZE + ROW_ID_SIZE]
            .copy_from_slice(&self.row_id.to_be_bytes());

        // Value length (for deserialization)
        let vlen = self.value_bytes.len() as u16;
        result[VALUE_DATA_SIZE + ROW_ID_SIZE..VALUE_DATA_SIZE + ROW_ID_SIZE + VALUE_LEN_SIZE]
            .copy_from_slice(&vlen.to_be_bytes());

        result
    }

    fn deserialize(bytes: &[u8]) -> Result<Self> {
        let key_size = Self::key_size();
        if bytes.len() < key_size {
            return Err(StorageError::Serialization("Invalid key: too short".to_string()));
        }

        // Read value length
        let vlen = u16::from_be_bytes(
            bytes[VALUE_DATA_SIZE + ROW_ID_SIZE..VALUE_DATA_SIZE + ROW_ID_SIZE + VALUE_LEN_SIZE]
                .try_into()
                .map_err(|_| StorageError::Serialization("Invalid value_len".to_string()))?
        ) as usize;

        // Reconstruct value_bytes
        let value_bytes = if vlen <= VALUE_DATA_SIZE {
            bytes[..vlen].to_vec()
        } else {
            // Truncated during serialization — use what we have
            // Range scans still work because the prefix is preserved
            bytes[..VALUE_DATA_SIZE].to_vec()
        };

        // Row ID
        let row_id = u64::from_be_bytes(
            bytes[VALUE_DATA_SIZE..VALUE_DATA_SIZE + ROW_ID_SIZE]
                .try_into()
                .map_err(|_| StorageError::Serialization("Invalid row_id".to_string()))?
        );

        Ok(IndexKey { value_bytes, row_id })
    }

    fn key_size() -> usize {
        VALUE_DATA_SIZE + ROW_ID_SIZE + VALUE_LEN_SIZE // 22 bytes
    }
}

/// Column Value Index
///
/// Maps column values to row IDs for fast WHERE lookups.
/// Uses a two-layer architecture for lock-free reads:
/// 1. IndexMemBuffer (active + immutable) for recent writes — cheap RwLock on BTreeMap
/// 2. GenericBTree for flushed data — RwLock only taken during flush (background)
///
/// Concurrency safety:
/// - `drain_lock` serializes immutable-to-btree drains, preventing thundering herd
/// - `tombstones` track deleted keys so they don't resurrect from immutable buffers
/// - Reads never hold both btree lock and tombstone lock simultaneously (no deadlock)
pub struct ColumnValueIndex {
    /// Table name
    _table_name: String,
    /// Column name
    column_name: String,
    /// Storage path
    _storage_path: PathBuf,
    /// B-Tree index (value_bytes+row_id → empty) — only written during flush
    btree: Arc<RwLock<GenericBTree<IndexKey>>>,
    /// LRU cache for hot values
    lru_cache: Arc<CachedIndex>,
    /// In-memory buffer for recent writes (RocksDB-style active/immutable)
    mem_buffer: IndexMemBuffer<IndexKey, ()>,
    /// Deleted keys not yet purged from immutable buffers.
    /// Keys are normalized via `tombstone_key()` to match btree's truncated format.
    tombstones: Mutex<HashSet<IndexKey>>,
    /// Serializes drain_immutable_to_btree to prevent thundering herd.
    drain_lock: Mutex<()>,
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
            _table_name: table_name,
            column_name,
            _storage_path: storage_path,
            btree: Arc::new(RwLock::new(btree)),
            lru_cache: Arc::new(CachedIndex::new(500)),
            mem_buffer: IndexMemBuffer::new(1024 * 1024), // 1MB buffer
            tombstones: Mutex::new(HashSet::new()),
            drain_lock: Mutex::new(()),
        })
    }

    /// Open an existing index
    pub fn open<P: AsRef<Path>>(
        path: P,
        table_name: String,
        column_name: String,
        config: ColumnValueIndexConfig,
    ) -> Result<Self> {
        Self::create(path, table_name, column_name, config)
    }

    /// Insert a value → row_id mapping
    pub fn insert(&self, value: &Value, row_id: RowId) -> Result<()> {
        let value_bytes = self.value_to_bytes(value)?;
        let key = IndexKey {
            value_bytes,
            row_id,
        };

        // Write to mem buffer (cheap RwLock on BTreeMap)
        let full = self.mem_buffer.insert(key.clone(), ()).map_err(|e| {
            StorageError::InvalidData(e)
        })?;

        // Re-insert cancels any pending tombstone (normalized key)
        self.tombstones.lock().remove(&tombstone_key(&key));

        // If buffer is full, drain immutable buffers to btree (non-blocking)
        if full {
            if let Some(_guard) = self.drain_lock.try_lock() {
                self.drain_immutable_to_btree()?;
            }
        }

        // Invalidate LRU cache for this value (new entry changes result set)
        self.lru_cache.invalidate(value);

        Ok(())
    }

    /// Batch insert for improved performance
    pub fn batch_insert(&self, items: Vec<(Value, RowId)>) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }

        // Sort keys by value for sequential access
        let mut keys: Vec<(IndexKey, Value)> = items.into_iter()
            .map(|(value, row_id)| {
                let value_bytes = self.value_to_bytes(&value)?;
                let key = IndexKey {
                    value_bytes,
                    row_id,
                };
                Ok((key, value))
            })
            .collect::<Result<Vec<_>>>()?;

        keys.sort_by(|a, b| a.0.value_bytes.cmp(&b.0.value_bytes));

        // Cancel tombstones for all keys in one lock acquisition (normalized keys)
        {
            let mut tombstones = self.tombstones.lock();
            for (key, _) in &keys {
                tombstones.remove(&tombstone_key(key));
            }
        }

        for (key, value) in &keys {
            let full = self.mem_buffer.insert(key.clone(), ()).map_err(|e| {
                StorageError::InvalidData(e)
            })?;
            if full {
                if let Some(_guard) = self.drain_lock.try_lock() {
                    self.drain_immutable_to_btree()?;
                }
            }
            self.lru_cache.invalidate(value);
        }

        Ok(())
    }

    /// Point query: get all row_ids with exact value
    pub fn get(&self, value: &Value) -> Result<Vec<RowId>> {
        // Try LRU cache first
        if let Some(cached_ids) = self.lru_cache.get(value) {
            return Ok((*cached_ids).clone());
        }

        self.lru_cache.record_miss();

        let value_bytes = self.value_to_bytes(value)?;
        let mut results: Vec<IndexKey> = Vec::new();
        let mut seen = HashSet::new();

        let start_key = IndexKey {
            value_bytes: value_bytes.clone(),
            row_id: 0,
        };
        let end_key = IndexKey {
            value_bytes: value_bytes.clone(),
            row_id: RowId::MAX,
        };

        // 1. Check mem buffer
        let buffer_results = self.mem_buffer.range(&start_key, &end_key);
        for (key, _) in buffer_results {
            if key.value_bytes == value_bytes && seen.insert(key.row_id) {
                results.push(key);
            }
        }

        // 2. Check persistent btree
        {
            let btree = self.btree.read();
            let btree_results = btree.range(&start_key, &end_key)?;
            for (key, _) in btree_results {
                if key.value_bytes == value_bytes && seen.insert(key.row_id) {
                    results.push(key);
                }
            }
        }

        // 3. Filter tombstones AND cache atomically.
        // Holding the tombstone lock during cache put prevents a concurrent delete
        // from adding a tombstone between the filter and the cache (TOCTOU fix).
        let row_ids = {
            let tombstones = self.tombstones.lock();
            let filtered: Vec<RowId> = results.into_iter()
                .filter(|key| !tombstones.contains(&tombstone_key(key)))
                .map(|key| key.row_id)
                .collect();
            if !filtered.is_empty() {
                self.lru_cache.put(value.clone(), filtered.clone());
            }
            filtered
        };

        Ok(row_ids)
    }

    /// Range query: get all row_ids where start <= value <= end
    pub fn range(&self, start: &Value, end: &Value) -> Result<Vec<RowId>> {
        let start_bytes = self.value_to_bytes(start)?;
        let end_bytes = self.value_to_bytes(end)?;

        let mut results: Vec<IndexKey> = Vec::new();
        let mut seen = HashSet::new();

        let start_key = IndexKey {
            value_bytes: start_bytes,
            row_id: 0,
        };
        let end_key = IndexKey {
            value_bytes: end_bytes,
            row_id: RowId::MAX,
        };

        // 1. Mem buffer
        let buffer_results = self.mem_buffer.range(&start_key, &end_key);
        for (key, _) in buffer_results {
            if seen.insert(key.row_id) {
                results.push(key);
            }
        }

        // 2. Btree
        {
            let btree = self.btree.read();
            let btree_results = btree.range(&start_key, &end_key)?;
            for (key, _) in btree_results {
                if seen.insert(key.row_id) {
                    results.push(key);
                }
            }
        }

        // 3. Filter tombstones
        let tombstones = self.tombstones.lock();
        let row_ids: Vec<RowId> = results.into_iter()
            .filter(|key| !tombstones.contains(&tombstone_key(key)))
            .map(|key| key.row_id)
            .collect();
        Ok(row_ids)
    }

    /// Scan all entries in order
    pub fn scan_all_row_ids(&self) -> Result<Vec<RowId>> {
        self.scan_row_ids_with_limit(None)
    }

    /// Scan entries with optional limit
    pub fn scan_row_ids_with_limit(&self, limit: Option<usize>) -> Result<Vec<RowId>> {
        let mut results: Vec<IndexKey> = Vec::new();
        let mut seen = HashSet::new();

        let min_key = IndexKey {
            value_bytes: vec![],
            row_id: 0,
        };
        let max_key = IndexKey {
            value_bytes: vec![0xFF; VALUE_DATA_SIZE],
            row_id: RowId::MAX,
        };

        // 1. Mem buffer
        let buffer_results = self.mem_buffer.range(&min_key, &max_key);
        for (key, _) in buffer_results {
            if seen.insert(key.row_id) {
                results.push(key);
            }
        }

        // 2. Btree
        {
            let btree = self.btree.read();
            let all_entries = if let Some(limit_count) = limit {
                btree.range_with_limit(&min_key, &max_key, limit_count)?
            } else {
                btree.range(&min_key, &max_key)?
            };
            for (key, _) in all_entries {
                if seen.insert(key.row_id) {
                    results.push(key);
                }
            }
        }

        // 3. Filter tombstones
        let tombstones = self.tombstones.lock();
        let row_ids: Vec<RowId> = results.into_iter()
            .filter(|key| !tombstones.contains(&tombstone_key(key)))
            .map(|key| key.row_id)
            .collect();
        Ok(row_ids)
    }

    /// Range query: value < upper_bound
    pub fn query_less_than(&self, upper_bound: &Value) -> Result<Vec<RowId>> {
        let upper_bytes = self.value_to_bytes(upper_bound)?;

        let mut results: Vec<IndexKey> = Vec::new();
        let mut seen = HashSet::new();

        let start_key = IndexKey {
            value_bytes: vec![],
            row_id: 0,
        };
        let end_key = IndexKey {
            value_bytes: upper_bytes,
            row_id: 0,
        };

        // 1. Mem buffer
        let buffer_results = self.mem_buffer.range(&start_key, &end_key);
        for (key, _) in buffer_results {
            if seen.insert(key.row_id) {
                results.push(key);
            }
        }

        // 2. Btree
        {
            let btree = self.btree.read();
            let btree_results = btree.range(&start_key, &end_key)?;
            for (key, _) in btree_results {
                if seen.insert(key.row_id) {
                    results.push(key);
                }
            }
        }

        // 3. Filter tombstones
        let tombstones = self.tombstones.lock();
        let row_ids: Vec<RowId> = results.into_iter()
            .filter(|key| !tombstones.contains(&tombstone_key(key)))
            .map(|key| key.row_id)
            .collect();
        Ok(row_ids)
    }

    /// Range query: value > lower_bound
    pub fn query_greater_than(&self, lower_bound: &Value) -> Result<Vec<RowId>> {
        let lower_bytes = self.value_to_bytes(lower_bound)?;

        let mut results: Vec<IndexKey> = Vec::new();
        let mut seen = HashSet::new();

        let start_key = IndexKey {
            value_bytes: lower_bytes,
            row_id: RowId::MAX,
        };
        let end_key = IndexKey {
            value_bytes: vec![0xFF; VALUE_DATA_SIZE],
            row_id: RowId::MAX,
        };

        // 1. Mem buffer
        let buffer_results = self.mem_buffer.range(&start_key, &end_key);
        for (key, _) in buffer_results {
            if seen.insert(key.row_id) {
                results.push(key);
            }
        }

        // 2. Btree
        {
            let btree = self.btree.read();
            let btree_results = btree.range(&start_key, &end_key)?;
            for (key, _) in btree_results {
                if seen.insert(key.row_id) {
                    results.push(key);
                }
            }
        }

        // 3. Filter tombstones
        let tombstones = self.tombstones.lock();
        let row_ids: Vec<RowId> = results.into_iter()
            .filter(|key| !tombstones.contains(&tombstone_key(key)))
            .map(|key| key.row_id)
            .collect();
        Ok(row_ids)
    }

    /// Range query: value <= upper_bound (inclusive)
    pub fn query_less_than_or_equal(&self, upper_bound: &Value) -> Result<Vec<RowId>> {
        let upper_bytes = self.value_to_bytes(upper_bound)?;

        let mut results: Vec<IndexKey> = Vec::new();
        let mut seen = HashSet::new();

        let start_key = IndexKey {
            value_bytes: vec![],
            row_id: 0,
        };
        let end_key = IndexKey {
            value_bytes: upper_bytes,
            row_id: RowId::MAX,
        };

        // 1. Mem buffer
        let buffer_results = self.mem_buffer.range(&start_key, &end_key);
        for (key, _) in buffer_results {
            if seen.insert(key.row_id) {
                results.push(key);
            }
        }

        // 2. Btree
        {
            let btree = self.btree.read();
            let btree_results = btree.range(&start_key, &end_key)?;
            for (key, _) in btree_results {
                if seen.insert(key.row_id) {
                    results.push(key);
                }
            }
        }

        // 3. Filter tombstones
        let tombstones = self.tombstones.lock();
        let row_ids: Vec<RowId> = results.into_iter()
            .filter(|key| !tombstones.contains(&tombstone_key(key)))
            .map(|key| key.row_id)
            .collect();
        Ok(row_ids)
    }

    /// Range query: value >= lower_bound (inclusive)
    pub fn query_greater_than_or_equal(&self, lower_bound: &Value) -> Result<Vec<RowId>> {
        let lower_bytes = self.value_to_bytes(lower_bound)?;

        let mut results: Vec<IndexKey> = Vec::new();
        let mut seen = HashSet::new();

        let start_key = IndexKey {
            value_bytes: lower_bytes,
            row_id: 0,
        };
        let end_key = IndexKey {
            value_bytes: vec![0xFF; VALUE_DATA_SIZE],
            row_id: RowId::MAX,
        };

        // 1. Mem buffer
        let buffer_results = self.mem_buffer.range(&start_key, &end_key);
        for (key, _) in buffer_results {
            if seen.insert(key.row_id) {
                results.push(key);
            }
        }

        // 2. Btree
        {
            let btree = self.btree.read();
            let btree_results = btree.range(&start_key, &end_key)?;
            for (key, _) in btree_results {
                if seen.insert(key.row_id) {
                    results.push(key);
                }
            }
        }

        // 3. Filter tombstones
        let tombstones = self.tombstones.lock();
        let row_ids: Vec<RowId> = results.into_iter()
            .filter(|key| !tombstones.contains(&tombstone_key(key)))
            .map(|key| key.row_id)
            .collect();
        Ok(row_ids)
    }

    /// Dual-bound range query with flexible boundaries
    pub fn query_between(&self,
                        lower_bound: &Value, lower_inclusive: bool,
                        upper_bound: &Value, upper_inclusive: bool) -> Result<Vec<RowId>> {
        let lower_bytes = self.value_to_bytes(lower_bound)?;
        let upper_bytes = self.value_to_bytes(upper_bound)?;

        let mut results: Vec<IndexKey> = Vec::new();
        let mut seen = HashSet::new();

        let start_key = IndexKey {
            value_bytes: lower_bytes,
            row_id: if lower_inclusive { 0 } else { RowId::MAX },
        };
        let end_key = IndexKey {
            value_bytes: upper_bytes,
            row_id: if upper_inclusive { RowId::MAX } else { 0 },
        };

        // 1. Mem buffer
        let buffer_results = self.mem_buffer.range(&start_key, &end_key);
        for (key, _) in buffer_results {
            if seen.insert(key.row_id) {
                results.push(key);
            }
        }

        // 2. Btree
        {
            let btree = self.btree.read();
            let btree_results = btree.range(&start_key, &end_key)?;
            for (key, _) in btree_results {
                if seen.insert(key.row_id) {
                    results.push(key);
                }
            }
        }

        // 3. Filter tombstones
        let tombstones = self.tombstones.lock();
        let row_ids: Vec<RowId> = results.into_iter()
            .filter(|key| !tombstones.contains(&tombstone_key(key)))
            .map(|key| key.row_id)
            .collect();
        Ok(row_ids)
    }

    /// Delete a value → row_id mapping
    pub fn delete(&self, value: &Value, row_id: RowId) -> Result<()> {
        let value_bytes = self.value_to_bytes(value)?;
        let key = IndexKey {
            value_bytes,
            row_id,
        };

        // Remove from active buffer
        self.mem_buffer.delete(&key);

        // Mark as tombstoned (normalized key prevents resurrection from immutable buffers)
        self.tombstones.lock().insert(tombstone_key(&key));

        // Remove from persistent btree (may have been flushed already)
        let mut btree = self.btree.write();
        btree.delete(&key)?;
        drop(btree);

        self.lru_cache.invalidate(value);

        Ok(())
    }

    /// Batch delete with smart cache invalidation
    pub fn batch_delete(&self, items: Vec<(Value, RowId)>) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }

        // 1. Delete from mem buffer, mark tombstones, delete from btree
        {
            let mut tombstones = self.tombstones.lock();
            let mut btree = self.btree.write();
            for (value, row_id) in &items {
                let value_bytes = self.value_to_bytes(value)?;
                let key = IndexKey {
                    value_bytes,
                    row_id: *row_id,
                };
                self.mem_buffer.delete(&key);
                tombstones.insert(tombstone_key(&key));
                btree.delete(&key)?;
            }
        }

        // 2. Batch invalidate LRU cache
        let mut unique_values = items.into_iter()
            .map(|(value, _)| value)
            .collect::<Vec<_>>();
        unique_values.sort_by(|a, b| {
            let a_bytes = Self::value_to_bytes_helper(a).unwrap_or_default();
            let b_bytes = Self::value_to_bytes_helper(b).unwrap_or_default();
            a_bytes.cmp(&b_bytes)
        });
        unique_values.dedup_by(|a, b| {
            let a_bytes = Self::value_to_bytes_helper(a).unwrap_or_default();
            let b_bytes = Self::value_to_bytes_helper(b).unwrap_or_default();
            a_bytes == b_bytes
        });

        self.lru_cache.invalidate_batch(&unique_values);

        Ok(())
    }

    /// Delete range with smart cache invalidation
    pub fn delete_range(&self, start: &Value, end: &Value) -> Result<usize> {
        let start_bytes = self.value_to_bytes(start)?;
        let end_bytes = self.value_to_bytes(end)?;

        let start_key = IndexKey {
            value_bytes: start_bytes.clone(),
            row_id: 0,
        };
        let end_key = IndexKey {
            value_bytes: end_bytes.clone(),
            row_id: RowId::MAX,
        };

        let mut deleted_count = 0;

        // Hold tombstones + btree write lock for the entire operation
        let mut tombstones = self.tombstones.lock();
        let mut btree = self.btree.write();

        // 1. Find and delete all keys in btree
        let keys_to_delete: Vec<IndexKey> = btree.range(&start_key, &end_key)?
            .into_iter()
            .map(|(key, _)| key)
            .collect();

        for key in &keys_to_delete {
            btree.delete(key)?;
            tombstones.insert(tombstone_key(key));
            deleted_count += 1;
        }
        drop(btree);

        // 2. Delete from mem buffer
        let buffer_results = self.mem_buffer.range(&start_key, &end_key);
        for (key, _) in &buffer_results {
            self.mem_buffer.delete(key);
            tombstones.insert(tombstone_key(key));
            deleted_count += 1;
        }
        drop(tombstones);

        self.lru_cache.invalidate_range(start, end);

        Ok(deleted_count)
    }

    /// Flush mem buffer to persistent btree, then btree to disk
    pub fn flush(&self) -> Result<()> {
        self.flush_buffer()?;
        let mut btree = self.btree.write();
        btree.flush()?;
        Ok(())
    }

    /// Drain immutable buffers to btree (called when buffer is full or during checkpoint)
    ///
    /// Caller must hold drain_lock.
    fn drain_immutable_to_btree(&self) -> Result<()> {
        while self.mem_buffer.should_flush() {
            if let Some(entries) = self.mem_buffer.flush().map_err(|e| StorageError::InvalidData(e))? {
                if !entries.is_empty() {
                    let tombstones = self.tombstones.lock();
                    let mut btree = self.btree.write();
                    for (key, _) in entries {
                        if !tombstones.contains(&tombstone_key(&key)) {
                            btree.insert(key, vec![])?;
                        }
                    }
                }
            } else {
                break;
            }
        }
        Ok(())
    }

    /// Flush all buffered entries (active + immutable) to persistent btree
    pub fn flush_buffer(&self) -> Result<()> {
        let entries = self.mem_buffer.drain();
        if !entries.is_empty() {
            let tombstones = self.tombstones.lock();
            let mut btree = self.btree.write();
            for (key, _) in &entries {
                if !tombstones.contains(&tombstone_key(key)) {
                    btree.insert(key.clone(), vec![])?;
                }
            }
            drop(btree);
            drop(tombstones);
            // All buffers drained — tombstones no longer needed for resurrection prevention
            self.tombstones.lock().clear();
        }
        Ok(())
    }

    /// Get index statistics
    pub fn stats(&self) -> IndexStats {
        let lru_stats = self.lru_cache.stats();
        IndexStats {
            cached_values: lru_stats.size,
            total_row_ids: 0,
        }
    }

    /// Get the approximate number of entries in the index
    pub fn entry_count(&self) -> usize {
        let btree = self.btree.read();
        btree.approximate_entry_count()
    }

    // Helper: Convert Value to bytes for comparison
    fn value_to_bytes(&self, value: &Value) -> Result<Vec<u8>> {
        Self::value_to_bytes_helper(value)
    }

    fn value_to_bytes_helper(value: &Value) -> Result<Vec<u8>> {
        let bytes = match value {
            Value::Integer(i) => i.to_be_bytes().to_vec(),
            Value::Float(f) => f.to_be_bytes().to_vec(),
            Value::Text(s) => s.as_bytes().to_vec(),
            Value::Bool(b) => vec![if *b { 1 } else { 0 }],
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

// ==================== Batch Index Builder Implementation ====================

use crate::index::builder::{IndexBuilder, BuildStats};
use crate::types::Row;

impl IndexBuilder for ColumnValueIndex {
    fn build_from_memtable(&mut self, _rows: &[(RowId, Row)]) -> Result<()> {
        debug_log!("[ColumnIndex::{}] ⚠️  build_from_memtable is deprecated, use insert_batch instead",
                 self.column_name);
        Ok(())
    }

    fn persist(&mut self) -> Result<()> {
        use std::time::Instant;
        let start = Instant::now();

        self.flush()?;

        let duration = start.elapsed();
        debug_log!("[ColumnIndex::{}] Persist: {:?}", self.column_name, duration);

        Ok(())
    }

    fn name(&self) -> &str {
        &self.column_name
    }

    fn stats(&self) -> BuildStats {
        let stats = self.stats();
        BuildStats {
            rows_processed: stats.total_row_ids,
            build_time_ms: 0,
            persist_time_ms: 0,
            index_size_bytes: stats.total_row_ids * 22,
        }
    }
}

impl ColumnValueIndex {
    /// Batch insert (optimized interface for bulk index building)
    pub fn insert_batch(&self, batch: &[(RowId, &Value)]) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        for (row_id, value) in batch {
            self.insert(value, *row_id)?;
        }

        Ok(())
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

        let index = ColumnValueIndex::create(
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

    #[test]
    fn test_column_value_index_delete_tombstone() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().join("test_tombstone.idx");

        let index = ColumnValueIndex::create(
            &path,
            "users".to_string(),
            "age".to_string(),
            ColumnValueIndexConfig::default(),
        )?;

        // Insert and delete
        index.insert(&Value::Integer(25), 1)?;
        index.insert(&Value::Integer(25), 2)?;
        index.delete(&Value::Integer(25), 1)?;

        // Only row 2 should remain
        let row_ids = index.get(&Value::Integer(25))?;
        assert_eq!(row_ids.len(), 1);
        assert!(row_ids.contains(&2));

        // Re-insert deleted key cancels tombstone
        index.insert(&Value::Integer(25), 1)?;
        let row_ids = index.get(&Value::Integer(25))?;
        assert_eq!(row_ids.len(), 2);
        assert!(row_ids.contains(&1));
        assert!(row_ids.contains(&2));

        Ok(())
    }

    #[test]
    fn test_column_value_index_range_with_delete() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().join("test_range_delete.idx");

        let index = ColumnValueIndex::create(
            &path,
            "users".to_string(),
            "age".to_string(),
            ColumnValueIndexConfig::default(),
        )?;

        // Insert range of values
        for i in 10..20 {
            index.insert(&Value::Integer(i), i as RowId)?;
        }

        // Delete range 13..=17
        let deleted = index.delete_range(&Value::Integer(13), &Value::Integer(17))?;
        assert!(deleted > 0);

        // Check remaining values
        let row_ids = index.range(&Value::Integer(10), &Value::Integer(19))?;
        let expected: Vec<RowId> = vec![10, 11, 12, 18, 19];
        assert_eq!(row_ids.len(), expected.len());
        for id in &expected {
            assert!(row_ids.contains(id));
        }

        Ok(())
    }

    #[test]
    fn test_tombstone_key_normalization() {
        let short = IndexKey { value_bytes: b"hello".to_vec(), row_id: 42 };
        let tk_short = tombstone_key(&short);
        assert_eq!(tk_short.value_bytes, b"hello".to_vec()); // no truncation

        let long = IndexKey {
            value_bytes: b"abcdefghijklmno_xtralong".to_vec(),
            row_id: 99,
        };
        let tk_long = tombstone_key(&long);
        assert_eq!(tk_long.value_bytes, b"abcdefghijkl".to_vec()); // truncated to 12
        assert_eq!(tk_long.row_id, 99);
    }

    #[test]
    fn test_column_value_index_long_text_tombstone() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().join("test_long_text.idx");

        let index = ColumnValueIndex::create(
            &path,
            "users".to_string(),
            "bio".to_string(),
            ColumnValueIndexConfig::default(),
        )?;

        let long_val = Value::Text("abcdefghijklmno_xtralong_value".to_string());

        // Insert, flush to btree, then delete
        index.insert(&long_val, 1)?;
        index.insert(&long_val, 2)?;
        index.flush_buffer()?; // force into btree

        index.delete(&long_val, 1)?;
        let row_ids = index.get(&long_val)?;
        assert_eq!(row_ids.len(), 1);
        assert!(row_ids.contains(&2));
        assert!(!row_ids.contains(&1));

        Ok(())
    }

    /// Concurrent stress test: validates tombstone + drain correctness under contention.
    #[test]
    fn test_column_value_index_concurrent_stress() -> Result<()> {
        use std::sync::atomic::{AtomicBool, Ordering};

        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().join("test_concurrent.idx");

        let index = Arc::new(ColumnValueIndex::create(
            &path,
            "users".to_string(),
            "age".to_string(),
            ColumnValueIndexConfig::default(),
        )?);

        let stop = Arc::new(AtomicBool::new(false));
        let n = 500;

        for i in 0..n {
            index.insert(&Value::Integer(i % 50), i as RowId)?;
        }

        let mut handles = vec![];

        // Writer thread
        {
            let index = Arc::clone(&index);
            let stop = Arc::clone(&stop);
            handles.push(std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    for i in 0..100 {
                        let _ = index.insert(&Value::Integer(i % 50), i as RowId);
                    }
                }
            }));
        }

        // Deleter thread
        {
            let index = Arc::clone(&index);
            let stop = Arc::clone(&stop);
            handles.push(std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    for i in 0..50 {
                        let _ = index.delete(&Value::Integer(i), i as RowId);
                        let _ = index.insert(&Value::Integer(i), i as RowId);
                    }
                }
            }));
        }

        // Reader thread
        {
            let index = Arc::clone(&index);
            let stop = Arc::clone(&stop);
            handles.push(std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if let Ok(ids) = index.get(&Value::Integer(25)) {
                        for &id in &ids {
                            assert!(id < n as RowId,
                                "get() returned unexpected row_id {}", id);
                        }
                    }
                    if let Ok(ids) = index.query_less_than_or_equal(&Value::Integer(10)) {
                        for &id in &ids {
                            assert!(id < n as RowId,
                                "range() returned unexpected row_id {}", id);
                        }
                    }
                }
            }));
        }

        std::thread::sleep(std::time::Duration::from_millis(500));
        stop.store(true, Ordering::Relaxed);

        for handle in handles {
            handle.join().unwrap();
        }

        // Final consistency: delete then verify gone
        for i in 0..10 {
            index.delete(&Value::Integer(i), i as RowId)?;
        }
        for i in 0..10 {
            let ids = index.get(&Value::Integer(i))?;
            assert!(!ids.contains(&(i as RowId)),
                "Deleted key (value={}, row_id={}) still present", i, i);
        }

        Ok(())
    }
}
