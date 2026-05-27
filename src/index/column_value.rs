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
//! - BTreeKey serialization truncates value_bytes to 64 bytes (VALUE_DATA_SIZE)
//! - Tombstone keys are normalized to the same 64-byte prefix so that
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
    /// In-memory buffer size for writes before draining to B+Tree (bytes)
    pub mem_buffer_size: usize,
    /// Minimum number of immutable buffers before draining to B+Tree.
    /// Higher values reduce B+Tree write amplification at the cost of memory.
    /// Default: 2 (drain only when 2+ immutable buffers accumulated).
    pub drain_threshold: usize,
}

impl Default for ColumnValueIndexConfig {
    fn default() -> Self {
        Self {
            max_page_size: 4096,
            cache_size: 1024,
            mem_buffer_size: 1024 * 1024, // 1MB
            drain_threshold: 2,
        }
    }
}

/// Compact key layout: [value_data: 64B zero-padded][row_id: 8B BE][value_len: 2B BE] = 74 bytes
/// - Integer/Float/Timestamp: value_data = 8 bytes BE + 56 bytes zero pad
/// - Text: value_data = up to 64 bytes UTF-8 + zero pad
/// - Bool: value_data = 1 byte + 63 bytes zero pad
const VALUE_DATA_SIZE: usize = 64;
const ROW_ID_SIZE: usize = 8;
const VALUE_LEN_SIZE: usize = 2;

/// Key for the B-Tree: (column_value, row_id)
/// value_bytes is a fixed 64-byte stack array — zero heap allocation on clone.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct IndexKey {
    value_bytes: [u8; VALUE_DATA_SIZE],
    row_id: RowId,
}

impl std::hash::Hash for IndexKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value_bytes.hash(state);
        self.row_id.hash(state);
    }
}

/// Normalize an IndexKey for tombstone operations. With fixed-size value_bytes,
/// this is a simple stack copy — no heap allocation.
fn tombstone_key(key: &IndexKey) -> IndexKey {
    IndexKey {
        value_bytes: key.value_bytes,
        row_id: key.row_id,
    }
}

impl BTreeKey for IndexKey {
    fn serialize(&self) -> Vec<u8> {
        let key_size = Self::key_size();
        let mut result = vec![0u8; key_size];

        // Value data: copy VALUE_DATA_SIZE bytes as-is
        result[..VALUE_DATA_SIZE].copy_from_slice(&self.value_bytes[..]);

        // Row ID (big-endian for proper ordering)
        result[VALUE_DATA_SIZE..VALUE_DATA_SIZE + ROW_ID_SIZE]
            .copy_from_slice(&self.row_id.to_be_bytes());

        // Value length = VALUE_DATA_SIZE (always 64 for fixed-size)
        let vlen = VALUE_DATA_SIZE as u16;
        result[VALUE_DATA_SIZE + ROW_ID_SIZE..VALUE_DATA_SIZE + ROW_ID_SIZE + VALUE_LEN_SIZE]
            .copy_from_slice(&vlen.to_be_bytes());

        result
    }

    fn deserialize(bytes: &[u8]) -> Result<Self> {
        let key_size = Self::key_size();
        if bytes.len() < key_size {
            return Err(StorageError::Serialization("Invalid key: too short".to_string()));
        }

        // Reconstruct fixed-size value_bytes
        let mut value_bytes = [0u8; VALUE_DATA_SIZE];
        value_bytes.copy_from_slice(&bytes[..VALUE_DATA_SIZE]);

        // Row ID
        let row_id = u64::from_be_bytes(
            bytes[VALUE_DATA_SIZE..VALUE_DATA_SIZE + ROW_ID_SIZE]
                .try_into()
                .map_err(|_| StorageError::Serialization("Invalid row_id".to_string()))?
        );

        Ok(IndexKey { value_bytes, row_id })
    }

    fn key_size() -> usize {
        VALUE_DATA_SIZE + ROW_ID_SIZE + VALUE_LEN_SIZE // 74 bytes
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
    /// Keys to delete from B+Tree during next drain (deferred from update path).
    pending_deletes: Mutex<Vec<IndexKey>>,
    /// Serializes drain_immutable_to_btree to prevent thundering herd.
    drain_lock: Mutex<()>,
    /// Minimum immutable buffers before triggering drain (default 2).
    drain_threshold: usize,
    /// Set to true when the index is first created or has been stale.
    /// The async pipeline checks this flag; if false, the index is already
    /// up-to-date from synchronous INSERT/UPDATE/DELETE paths.
    needs_rebuild: std::sync::atomic::AtomicBool,
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
            mem_buffer: IndexMemBuffer::new(config.mem_buffer_size),
            tombstones: Mutex::new(HashSet::new()),
            pending_deletes: Mutex::new(Vec::new()),
            drain_lock: Mutex::new(()),
            drain_threshold: config.drain_threshold,
            needs_rebuild: std::sync::atomic::AtomicBool::new(true),
        })
    }

    /// Open an existing index from disk.
    ///
    /// Unlike `create()`, this marks `needs_rebuild = false` because the on-disk
    /// B+Tree already contains all data from prior sessions. The sync INSERT/UPDATE
    /// path keeps the index up-to-date; the async pipeline can safely skip it.
    pub fn open<P: AsRef<Path>>(
        path: P,
        table_name: String,
        column_name: String,
        config: ColumnValueIndexConfig,
    ) -> Result<Self> {
        let index = Self::create(path, table_name, column_name, config)?;
        index.needs_rebuild.store(false, std::sync::atomic::Ordering::Relaxed);
        Ok(index)
    }

    /// Insert a value → row_id mapping
    pub fn insert(&self, value: &Value, row_id: RowId) -> Result<()> {
        let value_bytes = self.value_to_bytes(value)?;
        let key = IndexKey {
            value_bytes,
            row_id,
        };

        // Write to mem buffer (primary write path)
        let full = self.mem_buffer.insert(key.clone(), ()).map_err(|e| {
            StorageError::InvalidData(e)
        })?;

        // Re-insert cancels any pending tombstone — must succeed (blocking).
        // A skipped tombstone removal would leave the re-inserted key invisible.
        self.tombstones.lock().remove(&tombstone_key(&key));

        // If buffer is full, drain immutable buffers to btree (non-blocking)
        if full {
            if let Some(_guard) = self.drain_lock.try_lock() {
                self.drain_immutable_to_btree()?;
            }
        }

        // Invalidate LRU cache — skip if cache is empty or lock is contended
        self.lru_cache.try_invalidate(value);

        Ok(())
    }

    /// Atomic update: delete old_value→row_id and insert new_value→row_id.
    /// Acquires locks once instead of twice, and drains at most once.
    pub fn update(&self, old_value: &Value, new_value: &Value, row_id: RowId) -> Result<()> {
        let old_value_bytes = self.value_to_bytes(old_value)?;
        let new_value_bytes = self.value_to_bytes(new_value)?;
        let old_key = IndexKey { value_bytes: old_value_bytes, row_id };
        let new_key = IndexKey { value_bytes: new_value_bytes, row_id };

        // 1. Remove old key from active mem_buffer
        self.mem_buffer.delete(&old_key);

        // 2. Defer B+Tree delete to drain (avoid write lock on hot path)
        //    Skip if value unchanged (no-op update would delete the entry we just re-inserted).
        let pending_len = if old_key != new_key {
            let mut pending = self.pending_deletes.lock();
            pending.push(old_key.clone());
            pending.len()
        } else {
            0
        };

        // 3. Tombstone old key (prevents resurrection from immutable buffers during drain).
        //    Remove any prior tombstone on the new key so it becomes visible.
        {
            let mut tombstones = self.tombstones.lock();
            tombstones.remove(&tombstone_key(&new_key)); // cancel prior tombstone
            if old_key != new_key {
                tombstones.insert(tombstone_key(&old_key)); // mark old entry for removal
            }
        }

        // 4. Write new key to mem_buffer
        let full = self.mem_buffer.insert(new_key.clone(), ()).map_err(|e| {
            StorageError::InvalidData(e)
        })?;

        // 5. Drain if buffer is full OR pending_deletes accumulated too many
        if full || pending_len > 10_000 {
            if let Some(_guard) = self.drain_lock.try_lock() {
                self.drain_immutable_to_btree()?;
            }
        }

        // 6. Invalidate LRU cache — non-blocking
        self.lru_cache.try_invalidate(old_value);
        self.lru_cache.try_invalidate(new_value);

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
    /// Get row IDs for a value — returns Arc to avoid cloning the Vec on cache hits.
    pub fn get_arc(&self, value: &Value) -> Result<Arc<Vec<RowId>>> {
        // Try LRU cache first (no locks needed)
        if let Some(cached_ids) = self.lru_cache.get(value) {
            return Ok(cached_ids);
        }

        self.lru_cache.record_miss();

        let value_bytes = self.value_to_bytes(value)?;
        let start_key = IndexKey {
            value_bytes: value_bytes.clone(),
            row_id: 0,
        };
        let end_key = IndexKey {
            value_bytes: value_bytes.clone(),
            row_id: RowId::MAX,
        };

        // 🔒 Acquire tombstones BEFORE btree to prevent deadlock with flush_buffer.
        // All write paths (flush_buffer, delete, delete_range, update) follow the
        // order tombstones → btree. Readers must follow the same order.
        let tombstones = self.tombstones.lock();
        let mut results: Vec<IndexKey> = Vec::new();
        let mut seen = HashSet::new();

        // Collect from mem_buffer (filter tombstones inline)
        let buffer_results = self.mem_buffer.range(&start_key, &end_key);
        for (key, _) in buffer_results {
            if key.value_bytes == value_bytes
                && !tombstones.contains(&tombstone_key(&key))
                && seen.insert(key.row_id)
            {
                results.push(key);
            }
        }

        // Collect from persistent btree (filter tombstones inline)
        {
            let btree = self.btree.read();
            let btree_results = btree.range(&start_key, &end_key)?;
            for (key, _) in btree_results {
                if key.value_bytes == value_bytes
                    && !tombstones.contains(&tombstone_key(&key))
                    && seen.insert(key.row_id)
                {
                    results.push(key);
                }
            }
        }

        // Cache atomically while still holding tombstone lock.
        // This prevents TOCTOU: a concurrent delete could add a tombstone
        // between our filter and cache, making the cache stale.
        let row_ids: Vec<RowId> = results.into_iter().map(|key| key.row_id).collect();
        let arc = Arc::new(row_ids);
        if !arc.is_empty() {
            self.lru_cache.put(value.clone(), (*arc).clone());
        }
        drop(tombstones);
        Ok(arc)
    }

    pub fn get(&self, value: &Value) -> Result<Vec<RowId>> {
        // Try LRU cache first (no locks needed)
        if let Some(cached_ids) = self.lru_cache.get(value) {
            return Ok((*cached_ids).clone());
        }

        self.lru_cache.record_miss();

        let value_bytes = self.value_to_bytes(value)?;
        let start_key = IndexKey {
            value_bytes: value_bytes.clone(),
            row_id: 0,
        };
        let end_key = IndexKey {
            value_bytes: value_bytes.clone(),
            row_id: RowId::MAX,
        };

        // 🔒 tombstones before btree — consistent with flush_buffer/deletion lock order
        let tombstones = self.tombstones.lock();
        let mut results: Vec<IndexKey> = Vec::new();
        let mut seen = HashSet::new();

        // 1. Check mem buffer (filter tombstones inline)
        let buffer_results = self.mem_buffer.range(&start_key, &end_key);
        for (key, _) in buffer_results {
            if key.value_bytes == value_bytes
                && !tombstones.contains(&tombstone_key(&key))
                && seen.insert(key.row_id)
            {
                results.push(key);
            }
        }

        // 2. Check persistent btree (filter tombstones inline)
        {
            let btree = self.btree.read();
            let btree_results = btree.range(&start_key, &end_key)?;
            for (key, _) in btree_results {
                if key.value_bytes == value_bytes
                    && !tombstones.contains(&tombstone_key(&key))
                    && seen.insert(key.row_id)
                {
                    results.push(key);
                }
            }
        }

        // 3. Cache atomically while holding tombstone lock (TOCTOU prevention)
        let filtered: Vec<RowId> = results.into_iter().map(|key| key.row_id).collect();
        if !filtered.is_empty() {
            self.lru_cache.put(value.clone(), filtered.clone());
        }
        drop(tombstones);
        Ok(filtered)
    }

    /// Range query: get all row_ids where start <= value <= end
    pub fn range(&self, start: &Value, end: &Value) -> Result<Vec<RowId>> {
        let start_bytes = self.value_to_bytes(start)?;
        let end_bytes = self.value_to_bytes(end)?;

        let start_key = IndexKey {
            value_bytes: start_bytes,
            row_id: 0,
        };
        let end_key = IndexKey {
            value_bytes: end_bytes,
            row_id: RowId::MAX,
        };

        // 🔒 tombstones before btree — consistent lock order
        let tombstones = self.tombstones.lock();
        let mut results: Vec<IndexKey> = Vec::with_capacity(64);
        let mut seen: HashSet<u64> = HashSet::with_capacity(64);

        // 1. Mem buffer (filter tombstones inline)
        let buffer_results = self.mem_buffer.range(&start_key, &end_key);
        for (key, _) in buffer_results {
            if !tombstones.contains(&tombstone_key(&key)) && seen.insert(key.row_id) {
                results.push(key);
            }
        }

        // 2. Btree (filter tombstones inline)
        {
            let btree = self.btree.read();
            let btree_results = btree.range(&start_key, &end_key)?;
            for (key, _) in btree_results {
                if !tombstones.contains(&tombstone_key(&key)) && seen.insert(key.row_id) {
                    results.push(key);
                }
            }
        }
        drop(tombstones);

        let row_ids: Vec<RowId> = results.into_iter().map(|key| key.row_id).collect();
        Ok(row_ids)
    }

    /// Scan entries with optional limit
    pub fn scan_row_ids_with_limit(&self, limit: Option<usize>) -> Result<Vec<RowId>> {
        let min_key = IndexKey {
            value_bytes: [0u8; VALUE_DATA_SIZE],
            row_id: 0,
        };
        let max_key = IndexKey {
            value_bytes: [0xFFu8; VALUE_DATA_SIZE],
            row_id: RowId::MAX,
        };

        // 🔒 tombstones before btree — consistent lock order
        let tombstones = self.tombstones.lock();
        let mut results: Vec<IndexKey> = Vec::new();
        let mut seen = HashSet::new();

        // 1. Mem buffer (filter tombstones inline)
        let buffer_results = self.mem_buffer.range(&min_key, &max_key);
        for (key, _) in buffer_results {
            if !tombstones.contains(&tombstone_key(&key)) && seen.insert(key.row_id) {
                results.push(key);
            }
        }

        // 2. Btree (filter tombstones inline)
        {
            let btree = self.btree.read();
            let all_entries = if let Some(limit_count) = limit {
                btree.range_with_limit(&min_key, &max_key, limit_count)?
            } else {
                btree.range(&min_key, &max_key)?
            };
            for (key, _) in all_entries {
                if !tombstones.contains(&tombstone_key(&key)) && seen.insert(key.row_id) {
                    results.push(key);
                }
            }
        }
        drop(tombstones);

        let row_ids: Vec<RowId> = results.into_iter().map(|key| key.row_id).collect();
        Ok(row_ids)
    }

    /// Range query: value < upper_bound
    pub fn query_less_than(&self, upper_bound: &Value) -> Result<Vec<RowId>> {
        let upper_bytes = self.value_to_bytes(upper_bound)?;

        let start_key = IndexKey {
            value_bytes: [0u8; VALUE_DATA_SIZE],
            row_id: 0,
        };
        let end_key = IndexKey {
            value_bytes: upper_bytes.clone(),
            row_id: RowId::MAX,
        };

        // 🔒 tombstones before btree — consistent lock order
        let tombstones = self.tombstones.lock();
        let mut results: Vec<IndexKey> = Vec::new();
        let mut seen = HashSet::new();

        // 1. Mem buffer (filter tombstones inline)
        let buffer_results = self.mem_buffer.range(&start_key, &end_key);
        for (key, _) in buffer_results {
            if key.value_bytes != upper_bytes
                && !tombstones.contains(&tombstone_key(&key))
                && seen.insert(key.row_id)
            {
                results.push(key);
            }
        }

        // 2. Btree (filter tombstones inline)
        {
            let btree = self.btree.read();
            let btree_results = btree.range(&start_key, &end_key)?;
            for (key, _) in btree_results {
                if key.value_bytes != upper_bytes
                    && !tombstones.contains(&tombstone_key(&key))
                    && seen.insert(key.row_id)
                {
                    results.push(key);
                }
            }
        }
        drop(tombstones);

        let row_ids: Vec<RowId> = results.into_iter().map(|key| key.row_id).collect();
        Ok(row_ids)
    }

    /// Range query: value > lower_bound
    pub fn query_greater_than(&self, lower_bound: &Value) -> Result<Vec<RowId>> {
        let lower_bytes = self.value_to_bytes(lower_bound)?;

        let start_key = IndexKey {
            value_bytes: lower_bytes.clone(),
            row_id: 0,
        };
        let end_key = IndexKey {
            value_bytes: [0xFFu8; VALUE_DATA_SIZE],
            row_id: RowId::MAX,
        };

        // 🔒 tombstones before btree — consistent lock order
        let tombstones = self.tombstones.lock();
        let mut results: Vec<IndexKey> = Vec::new();
        let mut seen = HashSet::new();

        // 1. Mem buffer (filter inline)
        let buffer_results = self.mem_buffer.range(&start_key, &end_key);
        for (key, _) in buffer_results {
            if key.value_bytes != lower_bytes
                && !tombstones.contains(&tombstone_key(&key))
                && seen.insert(key.row_id)
            {
                results.push(key);
            }
        }

        // 2. Btree (filter inline)
        {
            let btree = self.btree.read();
            let btree_results = btree.range(&start_key, &end_key)?;
            for (key, _) in btree_results {
                if key.value_bytes != lower_bytes
                    && !tombstones.contains(&tombstone_key(&key))
                    && seen.insert(key.row_id)
                {
                    results.push(key);
                }
            }
        }
        drop(tombstones);

        let row_ids: Vec<RowId> = results.into_iter().map(|key| key.row_id).collect();
        Ok(row_ids)
    }

    /// Range query: value <= upper_bound (inclusive)
    pub fn query_less_than_or_equal(&self, upper_bound: &Value) -> Result<Vec<RowId>> {
        let upper_bytes = self.value_to_bytes(upper_bound)?;

        let start_key = IndexKey {
            value_bytes: [0u8; VALUE_DATA_SIZE],
            row_id: 0,
        };
        let end_key = IndexKey {
            value_bytes: upper_bytes,
            row_id: RowId::MAX,
        };

        // 🔒 tombstones before btree — consistent lock order
        let tombstones = self.tombstones.lock();
        let mut results: Vec<IndexKey> = Vec::new();
        let mut seen = HashSet::new();

        // 1. Mem buffer (filter inline)
        let buffer_results = self.mem_buffer.range(&start_key, &end_key);
        for (key, _) in buffer_results {
            if !tombstones.contains(&tombstone_key(&key)) && seen.insert(key.row_id) {
                results.push(key);
            }
        }

        // 2. Btree (filter inline)
        {
            let btree = self.btree.read();
            let btree_results = btree.range(&start_key, &end_key)?;
            for (key, _) in btree_results {
                if !tombstones.contains(&tombstone_key(&key)) && seen.insert(key.row_id) {
                    results.push(key);
                }
            }
        }
        drop(tombstones);

        let row_ids: Vec<RowId> = results.into_iter().map(|key| key.row_id).collect();
        Ok(row_ids)
    }

    /// Range query: value >= lower_bound (inclusive)
    pub fn query_greater_than_or_equal(&self, lower_bound: &Value) -> Result<Vec<RowId>> {
        let lower_bytes = self.value_to_bytes(lower_bound)?;

        let start_key = IndexKey {
            value_bytes: lower_bytes,
            row_id: 0,
        };
        let end_key = IndexKey {
            value_bytes: [0xFFu8; VALUE_DATA_SIZE],
            row_id: RowId::MAX,
        };

        // 🔒 tombstones before btree — consistent lock order
        let tombstones = self.tombstones.lock();
        let mut results: Vec<IndexKey> = Vec::new();
        let mut seen = HashSet::new();

        // 1. Mem buffer (filter inline)
        let buffer_results = self.mem_buffer.range(&start_key, &end_key);
        for (key, _) in buffer_results {
            if !tombstones.contains(&tombstone_key(&key)) && seen.insert(key.row_id) {
                results.push(key);
            }
        }

        // 2. Btree (filter inline)
        {
            let btree = self.btree.read();
            let btree_results = btree.range(&start_key, &end_key)?;
            for (key, _) in btree_results {
                if !tombstones.contains(&tombstone_key(&key)) && seen.insert(key.row_id) {
                    results.push(key);
                }
            }
        }
        drop(tombstones);

        let row_ids: Vec<RowId> = results.into_iter().map(|key| key.row_id).collect();
        Ok(row_ids)
    }

    /// Dual-bound range query with flexible boundaries
    pub fn query_between(&self,
                        lower_bound: &Value, lower_inclusive: bool,
                        upper_bound: &Value, upper_inclusive: bool) -> Result<Vec<RowId>> {
        let lower_bytes = self.value_to_bytes(lower_bound)?;
        let upper_bytes = self.value_to_bytes(upper_bound)?;

        let start_key = IndexKey {
            value_bytes: lower_bytes.clone(),
            row_id: if lower_inclusive { 0 } else { RowId::MAX },
        };
        // For exclusive upper: scan one value past upper, then post-filter.
        // Using (upper_bytes, 0) would incorrectly include row_id=0 entries.
        let end_key = IndexKey {
            value_bytes: upper_bytes.clone(),
            row_id: RowId::MAX,
        };

        // 🔒 tombstones before btree — consistent lock order
        let tombstones = self.tombstones.lock();
        let mut results: Vec<IndexKey> = Vec::new();
        let mut seen = HashSet::new();

        let mut accept = |key: &IndexKey| -> bool {
            // Post-filter exclusive boundaries
            if !lower_inclusive && key.value_bytes == start_key.value_bytes {
                return false;
            }
            if !upper_inclusive && key.value_bytes == end_key.value_bytes {
                return false;
            }
            !tombstones.contains(&tombstone_key(key)) && seen.insert(key.row_id)
        };

        // 1. Mem buffer
        let buffer_results = self.mem_buffer.range(&start_key, &end_key);
        for (key, _) in buffer_results {
            if accept(&key) {
                results.push(key);
            }
        }

        // 2. Btree
        {
            let btree = self.btree.read();
            let btree_results = btree.range(&start_key, &end_key)?;
            for (key, _) in btree_results {
                if accept(&key) {
                    results.push(key);
                }
            }
        }
        drop(tombstones);

        let row_ids: Vec<RowId> = results.into_iter().map(|key| key.row_id).collect();
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

        // Phase 1: Collect mem_buffer keys (takes active.read() briefly, no tombstones held)
        let buffer_keys: Vec<IndexKey> = self.mem_buffer.range(&start_key, &end_key)
            .into_iter()
            .map(|(k, _)| k)
            .collect();

        // Phase 2: BTree deletion (tombstones + btree, no mem_buffer access)
        let mut tombstones = self.tombstones.lock();
        let mut btree = self.btree.write();

        let btree_keys: Vec<IndexKey> = btree.range(&start_key, &end_key)?
            .into_iter()
            .map(|(key, _)| key)
            .collect();

        for key in &btree_keys {
            btree.delete(key)?;
            tombstones.insert(tombstone_key(key));
            deleted_count += 1;
        }
        drop(btree);

        // Tombstone mem_buffer keys while still holding tombstones lock
        let mem_tombstone_keys: Vec<IndexKey> = buffer_keys.iter()
            .map(|k| tombstone_key(k))
            .collect();
        for tk in &mem_tombstone_keys {
            tombstones.insert(tk.clone());
            deleted_count += 1;
        }
        drop(tombstones);

        // Phase 3: Delete from mem_buffer (no locks held — avoids lock inversion
        // with flush_buffer which takes active.write() then tombstones)
        for key in &buffer_keys {
            self.mem_buffer.delete(key);
        }

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
    ///
    /// Uses `drain_threshold` to batch multiple immutable buffers into a single
    /// B+Tree write cycle, reducing write amplification.
    fn drain_immutable_to_btree(&self) -> Result<()> {
        self.drain_immutable_to_btree_impl(false)
    }

    fn drain_immutable_to_btree_impl(&self, force: bool) -> Result<()> {
        if !force && self.mem_buffer.immutable_count() < self.drain_threshold {
            return Ok(());
        }
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

        // Process deferred deletes from update path
        let deletes: Vec<IndexKey> = {
            let mut pending = self.pending_deletes.lock();
            std::mem::take(&mut *pending)
        };
        if !deletes.is_empty() {
            let mut btree = self.btree.write();
            for key in &deletes {
                let _ = btree.delete(key);
            }
        }

        Ok(())
    }

    /// Flush all buffered entries (active + immutable) to persistent btree.
    /// Drains everything including the active buffer (used by checkpoint/flush).
    pub fn flush_buffer(&self) -> Result<()> {
        let entries = self.mem_buffer.drain();
        let has_entries = !entries.is_empty();
        let deletes: Vec<IndexKey> = {
            let mut pending = self.pending_deletes.lock();
            std::mem::take(&mut *pending)
        };
        let has_deletes = !deletes.is_empty();

        if has_entries || has_deletes {
            // Collect tombstone keys to clear while holding the lock.
            // We must NOT call clear() on re-acquire because a concurrent
            // update()/delete() may have set new tombstones between the drop
            // and re-acquire (clearing them would resurrect the deleted key).
            let tombstone_keys_to_clear: Vec<IndexKey> = {
                let tombstones = self.tombstones.lock();
                let mut btree = self.btree.write();
                let mut keys_to_clear = Vec::new();
                // Insert buffered entries (skip tombstoned)
                for (key, _) in &entries {
                    let tk = tombstone_key(key);
                    if tombstones.contains(&tk) {
                        keys_to_clear.push(tk);
                    } else {
                        btree.insert(key.clone(), vec![])?;
                    }
                }
                // Process deferred deletes from update path
                for key in &deletes {
                    let _ = btree.delete(key);
                    keys_to_clear.push(tombstone_key(key));
                }
                drop(btree);
                drop(tombstones);
                keys_to_clear
            };

            // Remove only the tombstones we actually consumed — don't touch
            // tombstones set concurrently by other threads.
            let mut tombstones = self.tombstones.lock();
            for tk in &tombstone_keys_to_clear {
                tombstones.remove(tk);
            }
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

    /// Returns true if this index needs to be rebuilt by the async pipeline.
    /// Newly created indexes or those that missed synchronous updates need rebuilding.
    pub fn needs_rebuild(&self) -> bool {
        self.needs_rebuild.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Clear the rebuild flag after the async pipeline successfully builds the index.
    pub fn mark_rebuilt(&self) {
        self.needs_rebuild.store(false, std::sync::atomic::Ordering::Release);
    }

    /// Get the approximate number of entries in the index
    pub fn entry_count(&self) -> usize {
        let btree = self.btree.read();
        btree.approximate_entry_count()
    }

    // Helper: Convert Value to fixed 12-byte key (zero-padded for short types)
    fn value_to_bytes(&self, value: &Value) -> Result<[u8; VALUE_DATA_SIZE]> {
        Self::value_to_bytes_helper(value)
    }

    fn value_to_bytes_helper(value: &Value) -> Result<[u8; VALUE_DATA_SIZE]> {
        let mut buf = [0u8; VALUE_DATA_SIZE];
        match value {
            Value::Integer(i) => buf[..8].copy_from_slice(&i.to_be_bytes()),
            Value::Float(f) => {
                // Convert to sortable bytes: ensures negative < positive in byte order
                let canonical = if *f == 0.0 { 0.0f64 } else { *f }; // normalize -0.0
                let bits = canonical.to_bits();
                let sortable = if bits & (1u64 << 63) != 0 {
                    !bits // negative: flip all bits so -inf sorts first
                } else {
                    bits ^ (1u64 << 63) // positive: flip sign bit
                };
                buf[..8].copy_from_slice(&sortable.to_be_bytes());
            },
            Value::Timestamp(ts) => buf[..8].copy_from_slice(&ts.as_micros().to_be_bytes()),
            Value::Bool(b) => buf[0] = if *b { 1 } else { 0 },
            Value::Text(s) => {
                let raw = s.as_bytes();
                let len = raw.len().min(VALUE_DATA_SIZE);
                buf[..len].copy_from_slice(&raw[..len]);
            }
            _ => {
                return Err(StorageError::InvalidData(
                    format!("Unsupported value type for indexing: {:?}", value)
                ));
            }
        };
        Ok(buf)
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
            index_size_bytes: stats.total_row_ids * IndexKey::key_size(),
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
        let mut vb = [0u8; VALUE_DATA_SIZE];
        vb[..5].copy_from_slice(b"hello");
        let short = IndexKey { value_bytes: vb, row_id: 42 };
        let tk_short = tombstone_key(&short);
        assert_eq!(tk_short.value_bytes, vb);

        let mut vb2 = [0u8; VALUE_DATA_SIZE];
        vb2[..12].copy_from_slice(b"abcdefghijkl");
        let long = IndexKey { value_bytes: vb2, row_id: 99 };
        let tk_long = tombstone_key(&long);
        assert_eq!(tk_long.value_bytes, vb2);
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

        let long_val = Value::text("abcdefghijklmno_xtralong_value".to_string());

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

    /// Regression: concurrent get_arc + flush_buffer must not deadlock.
    /// Ensures both paths follow tombstones → btree lock order.
    #[test]
    fn test_concurrent_read_and_flush_no_deadlock() -> Result<()> {
        use std::sync::Arc;
        use std::time::Duration;

        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().join("test_deadlock2.idx");
        let index = Arc::new(ColumnValueIndex::create(
            &path, "t".to_string(), "c".to_string(),
            ColumnValueIndexConfig::default(),
        )?);

        for i in 0..2000i64 {
            index.insert(&Value::Integer(i % 100), i as RowId)?;
        }

        let idx_reader = Arc::clone(&index);
        let idx_writer = Arc::clone(&index);
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let s1 = Arc::clone(&stop);
        let s2 = Arc::clone(&stop);

        let reader = std::thread::spawn(move || {
            while !s1.load(std::sync::atomic::Ordering::Relaxed) {
                for v in 0..50i64 {
                    let _ = idx_reader.get_arc(&Value::Integer(v));
                }
            }
        });

        let writer = std::thread::spawn(move || {
            while !s2.load(std::sync::atomic::Ordering::Relaxed) {
                for i in 0..100i64 {
                    let _ = idx_writer.insert(&Value::Integer(i % 50), 10000 + i as RowId);
                }
                let _ = idx_writer.flush();
            }
        });

        // 3 seconds: deadlock would hang forever
        std::thread::sleep(Duration::from_secs(3));
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        reader.join().unwrap();
        writer.join().unwrap();
        eprintln!("  OK: concurrent read+flush deadlock regression passed");
        Ok(())
    }

    /// Verify update(old_value, new_value) atomically moves a row_id.
    #[test]
    fn test_update_moves_row_id() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().join("test_update.idx");
        let index = ColumnValueIndex::create(
            &path, "t".to_string(), "c".to_string(),
            ColumnValueIndexConfig::default(),
        )?;

        // Insert row 1 with value 100
        index.insert(&Value::Integer(100), 1)?;
        assert_eq!(index.get(&Value::Integer(100))?, vec![1]);
        assert!(index.get(&Value::Integer(200))?.is_empty());

        // Update: row 1 from 100 → 200
        index.update(&Value::Integer(100), &Value::Integer(200), 1)?;

        // Old value should NOT have row 1 anymore
        assert!(!index.get(&Value::Integer(100))?.contains(&1),
            "old value should not contain updated row");
        // New value SHOULD have row 1
        assert!(index.get(&Value::Integer(200))?.contains(&1),
            "new value should contain updated row");
        // Only one entry for row 1 across both values
        assert_eq!(index.get(&Value::Integer(100))?.len() + index.get(&Value::Integer(200))?.len(), 1);

        Ok(())
    }

    /// Verify update with same value (noop) doesn't lose the row_id.
    #[test]
    fn test_update_same_value_noop() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().join("test_update_same.idx");
        let index = ColumnValueIndex::create(
            &path, "t".to_string(), "c".to_string(),
            ColumnValueIndexConfig::default(),
        )?;

        index.insert(&Value::Integer(100), 5)?;
        // Update to same value — should be a noop, not a delete
        index.update(&Value::Integer(100), &Value::Integer(100), 5)?;

        assert!(index.get(&Value::Integer(100))?.contains(&5),
            "row should still be present after same-value update");
        Ok(())
    }

    /// Verify data survives flush: insert → flush → get
    #[test]
    fn test_insert_flush_get() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().join("test_flush_get.idx");
        let index = ColumnValueIndex::create(
            &path, "t".to_string(), "c".to_string(),
            ColumnValueIndexConfig::default(),
        )?;

        // Insert many entries to force btree writes
        for i in 0..500i64 {
            index.insert(&Value::Integer(i % 20), i as RowId)?;
        }

        // Flush mem_buffer → btree, then btree → disk
        index.flush()?;

        // Verify data is still correct after flush
        let ids = index.get(&Value::Integer(5))?;
        assert!(!ids.is_empty(), "data should survive flush");

        // Verify range query works after flush
        let range_ids = index.range(&Value::Integer(0), &Value::Integer(10))?;
        assert!(!range_ids.is_empty(), "range query should work after flush");

        Ok(())
    }

    /// Batch insert many entries and verify after flush.
    #[test]
    fn test_batch_insert_and_flush() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().join("test_batch2.idx");
        let index = ColumnValueIndex::create(
            &path, "t".to_string(), "c".to_string(),
            ColumnValueIndexConfig::default(),
        )?;
        let items: Vec<(Value, RowId)> = (0..1000i64)
            .map(|i| (Value::Integer(i % 10), i as RowId))
            .collect();
        index.batch_insert(items)?;
        index.flush()?;
        for v in 0..10i64 {
            assert_eq!(index.get(&Value::Integer(v))?.len(), 100,
                "value {} should have 100 row_ids", v);
        }
        Ok(())
    }

    /// Delete then verify gone.
    #[test]
    fn test_delete_makes_entry_invisible() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().join("test_del2.idx");
        let index = ColumnValueIndex::create(
            &path, "t".to_string(), "c".to_string(),
            ColumnValueIndexConfig::default(),
        )?;
        index.insert(&Value::Integer(42), 100)?;
        index.delete(&Value::Integer(42), 100)?;
        assert!(index.get(&Value::Integer(42))?.is_empty());
        Ok(())
    }

    /// Update then flush — verify data survives.
    #[test]
    fn test_update_survives_flush() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().join("test_upd_flush2.idx");
        let index = ColumnValueIndex::create(
            &path, "t".to_string(), "c".to_string(),
            ColumnValueIndexConfig::default(),
        )?;
        index.insert(&Value::Integer(10), 1)?;
        index.update(&Value::Integer(10), &Value::Integer(20), 1)?;
        index.flush()?;
        assert!(index.get(&Value::Integer(20))?.contains(&1),
            "after update+flush: row 1 should be at new value");
        assert!(!index.get(&Value::Integer(10))?.contains(&1),
            "after update+flush: row 1 should NOT be at old value");
        Ok(())
    }

    #[test]
    fn test_query_between_exclusive_boundaries() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().join("test_between.idx");
        let index = ColumnValueIndex::create(
            path,
            "t".to_string(),
            "v".to_string(),
            ColumnValueIndexConfig::default(),
        )?;

        // Insert values: 10, 20, 30 with row_id = 0, 1, 2
        index.insert(&Value::Integer(10), 0)?;
        index.insert(&Value::Integer(20), 1)?;
        index.insert(&Value::Integer(30), 2)?;

        // Inclusive both ends: [10, 30] → should find all 3
        let result = index.query_between(&Value::Integer(10), true, &Value::Integer(30), true)?;
        assert_eq!(result.len(), 3, "[10,30] inclusive should find 3, got {}", result.len());

        // Exclusive both ends: (10, 30) → should find only 20
        let result = index.query_between(&Value::Integer(10), false, &Value::Integer(30), false)?;
        assert_eq!(result.len(), 1, "(10,30) exclusive should find 1, got {}", result.len());
        assert!(result.contains(&1), "should contain row_id=1 (value=20)");

        // Lower exclusive, upper inclusive: (10, 30] → should find 20, 30
        let result = index.query_between(&Value::Integer(10), false, &Value::Integer(30), true)?;
        assert_eq!(result.len(), 2, "(10,30] should find 2, got {}", result.len());

        // Lower inclusive, upper exclusive: [10, 30) → should find 10, 20
        let result = index.query_between(&Value::Integer(10), true, &Value::Integer(30), false)?;
        assert_eq!(result.len(), 2, "[10,30) should find 2, got {}", result.len());

        Ok(())
    }
}
