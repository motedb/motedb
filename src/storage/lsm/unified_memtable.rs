//! Unified MemTable: 支持数据 + 向量的统一内存表
//!
//! ## 核心设计
//! - 数据和向量分离存储：DataEntry 只含 row data，无 Option<Vec> 开销
//! - 向量数据单独 BTreeMap，仅向量表创建
//! - 集成 FreshVamanaGraph 用于向量搜索
//! - 16 分片 BTreeMap 减少写入锁竞争
//!
//! ## 性能优化
//! - Arc<DataEntry> 避免每次 get() 的 clone（8 bytes vs 全行 memcpy）
//! - 非 ACP 表省 24 bytes/row 的 Option<Vec> 开销
//! - 分片设计：put/get 只锁单个分片，并发写入 ~16x 扩展

use super::{Key, Value, ValueData, LSMConfig};
use crate::index::fresh_graph::{FreshVamanaGraph, FreshGraphConfig};
use crate::distance::DistanceKind;
use crate::{Result, StorageError};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use parking_lot::RwLock;
use std::collections::BTreeMap;

/// Type alias for the vector storage map
type VectorMap = Arc<RwLock<BTreeMap<Key, Vec<f32>>>>;

const SHARD_COUNT: usize = 16;
const SHARD_MASK: usize = SHARD_COUNT - 1;

/// Data entry (row data only, no vector overhead)
#[derive(Clone, Debug)]
pub struct DataEntry {
    pub data: ValueData,
    pub timestamp: u64,
    pub deleted: bool,
}

impl DataEntry {
    pub fn memory_size(&self) -> usize {
        let data_size = match &self.data {
            ValueData::Inline(data) => data.len(),
            ValueData::Blob(_) => 16,
        };
        data_size + 16
    }
}

/// Unified Entry (数据 + 向量) — returned by get() for compatibility
#[derive(Clone, Debug)]
pub struct UnifiedEntry {
    pub data: ValueData,
    pub vector: Option<Vec<f32>>,
    pub timestamp: u64,
    pub deleted: bool,
}

impl UnifiedEntry {
    pub fn new(data: ValueData, vector: Option<Vec<f32>>, timestamp: u64) -> Self {
        Self { data, vector, timestamp, deleted: false }
    }

    pub fn tombstone(timestamp: u64) -> Self {
        Self {
            data: ValueData::Inline(std::sync::Arc::new(Vec::new())),
            vector: None,
            timestamp,
            deleted: true,
        }
    }

    pub fn memory_size(&self) -> usize {
        let data_size = match &self.data {
            ValueData::Inline(data) => data.len(),
            ValueData::Blob(_) => 16,
        };
        let vector_size = self.vector.as_ref().map(|v| v.len() * 4).unwrap_or(0);
        data_size + vector_size + 16
    }
}

impl From<Arc<DataEntry>> for UnifiedEntry {
    fn from(e: Arc<DataEntry>) -> Self {
        UnifiedEntry {
            data: e.data.clone(),
            vector: None,
            timestamp: e.timestamp,
            deleted: e.deleted,
        }
    }
}

/// Unified MemTable (数据 + 向量) — 16-shard concurrent design
pub struct UnifiedMemTable {
    /// 分片存储：16 个独立 BTreeMap，按 key & 0xF 路由
    shards: [RwLock<BTreeMap<Key, Arc<DataEntry>>>; SHARD_COUNT],

    /// 向量数据 (仅向量表创建，避免非向量表的 Option<Vec> 开销)
    vectors: Option<VectorMap>,

    /// 向量图索引：Fresh Vamana Graph
    vector_graph: Option<Arc<FreshVamanaGraph>>,

    /// 向量维度
    vector_dimension: Option<usize>,

    /// 当前内存占用（bytes）
    size: AtomicUsize,

    /// 最大内存限制（bytes）
    max_size: usize,

    /// 序列号（MVCC）
    next_seq: AtomicUsize,
}

impl UnifiedMemTable {
    #[inline]
    fn shard_index(key: Key) -> usize {
        (key as usize) & SHARD_MASK
    }

    /// 创建不支持向量的 MemTable（兼容旧代码）
    pub fn new(config: &LSMConfig) -> Self {
        Self {
            shards: core::array::from_fn(|_| RwLock::new(BTreeMap::new())),
            vectors: None,
            vector_graph: None,
            vector_dimension: None,
            size: AtomicUsize::new(0),
            max_size: config.memtable_size,
            next_seq: AtomicUsize::new(0),
        }
    }

    /// 创建支持向量的 MemTable
    pub fn new_with_vector_support(config: &LSMConfig, dimension: usize) -> Self {
        let fresh_config = FreshGraphConfig {
            max_nodes: 5000,
            max_degree: 32,
            search_list_size: 64,
            alpha: 1.2,
            memory_threshold: 20 * 1024 * 1024,
        };

        let metric = DistanceKind::Cosine;
        let vector_graph = FreshVamanaGraph::new(fresh_config, metric);

        Self {
            shards: core::array::from_fn(|_| RwLock::new(BTreeMap::new())),
            vectors: Some(Arc::new(RwLock::new(BTreeMap::new()))),
            vector_graph: Some(Arc::new(vector_graph)),
            vector_dimension: Some(dimension),
            size: AtomicUsize::new(0),
            max_size: config.memtable_size,
            next_seq: AtomicUsize::new(0),
        }
    }

    /// 插入数据（不含向量）
    pub fn put(&self, key: Key, value: Value) -> Result<()> {
        let entry = Arc::new(DataEntry {
            data: value.data,
            timestamp: value.timestamp,
            deleted: value.deleted,
        });
        self.insert_entry(key, entry)
    }

    /// 插入数据 + 向量
    pub fn put_with_vector(&self, key: Key, data: ValueData, vector: Vec<f32>, timestamp: u64) -> Result<()> {
        if let Some(expected_dim) = self.vector_dimension {
            if vector.len() != expected_dim {
                return Err(StorageError::InvalidData(
                    format!("Vector dimension mismatch: expected {}, got {}", expected_dim, vector.len())
                ));
            }
        }

        let entry = Arc::new(DataEntry {
            data,
            timestamp,
            deleted: false,
        });
        self.insert_entry(key, entry)?;

        // Store vector separately and track memory (each f32 = 4 bytes)
        if let Some(ref vec_map) = self.vectors {
            let vec_size = vector.len() * 4;
            self.size.fetch_add(vec_size, Ordering::Relaxed);
            vec_map.write().insert(key, vector.clone());
        }

        if let Some(ref graph) = self.vector_graph {
            graph.insert(key, vector)?;
        }

        Ok(())
    }

    /// Internal insert with Arc — single shard lock
    fn insert_entry(&self, key: Key, entry: Arc<DataEntry>) -> Result<()> {
        let entry_size = entry.memory_size();

        let mut shard = self.shards[Self::shard_index(key)].write();

        if let Some(old_entry) = shard.get(&key) {
            let old_size = old_entry.memory_size();
            self.size.fetch_sub(old_size, Ordering::Relaxed);
        }

        shard.insert(key, entry);
        self.size.fetch_add(entry_size, Ordering::Relaxed);
        self.next_seq.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Batch insert (grouped by shard for minimal lock contention)
    pub fn batch_put(&self, kvs: &[(Key, Value)]) -> Result<()> {
        if kvs.is_empty() {
            return Ok(());
        }

        // Group by shard
        let mut groups: [Vec<(Key, Arc<DataEntry>)>; SHARD_COUNT] = core::array::from_fn(|_| Vec::new());
        for (key, value) in kvs {
            let entry = Arc::new(DataEntry {
                data: value.data.clone(),
                timestamp: value.timestamp,
                deleted: value.deleted,
            });
            groups[Self::shard_index(*key)].push((*key, entry));
        }

        let mut total_size_change: i64 = 0;
        for (shard_idx, group) in groups.into_iter().enumerate() {
            if group.is_empty() {
                continue;
            }
            let mut shard = self.shards[shard_idx].write();
            for (key, entry) in group {
                let entry_size = entry.memory_size();
                if let Some(old_entry) = shard.get(&key) {
                    total_size_change -= old_entry.memory_size() as i64;
                }
                shard.insert(key, entry);
                total_size_change += entry_size as i64;
            }
        }

        if total_size_change > 0 {
            self.size.fetch_add(total_size_change as usize, Ordering::Relaxed);
        } else if total_size_change < 0 {
            self.size.fetch_sub((-total_size_change) as usize, Ordering::Relaxed);
        }

        self.next_seq.fetch_add(kvs.len(), Ordering::Relaxed);
        Ok(())
    }

    /// Get data — single shard read lock
    pub fn get(&self, key: Key) -> Result<Option<UnifiedEntry>> {
        let shard = self.shards[Self::shard_index(key)].read();
        let Some(arc_entry) = shard.get(&key) else {
            return Ok(None);
        };

        let vector = self.vectors.as_ref().and_then(|vm| {
            vm.read().get(&key).cloned()
        });

        Ok(Some(UnifiedEntry {
            data: arc_entry.data.clone(),
            vector,
            timestamp: arc_entry.timestamp,
            deleted: arc_entry.deleted,
        }))
    }

    /// Delete (insert tombstone) — single shard write lock
    pub fn delete(&self, key: Key, timestamp: u64) -> Result<()> {
        let entry = Arc::new(DataEntry {
            data: ValueData::Inline(std::sync::Arc::new(Vec::new())),
            timestamp,
            deleted: true,
        });

        let mut shard = self.shards[Self::shard_index(key)].write();

        if let Some(old_entry) = shard.get(&key) {
            self.size.fetch_sub(old_entry.memory_size(), Ordering::Relaxed);
        }

        let entry_size = entry.memory_size();
        shard.insert(key, entry);
        self.size.fetch_add(entry_size, Ordering::Relaxed);

        // Remove vector if present
        if let Some(ref vec_map) = self.vectors {
            if let Some(old_vec) = vec_map.write().remove(&key) {
                let vec_size = old_vec.len() * 4;
                self.size.fetch_sub(vec_size, Ordering::Relaxed);
            }
        }

        Ok(())
    }

    /// Get all keys in [start, end] range — merge from all shards
    pub fn keys_in_range(&self, start: Key, end: Key) -> Vec<Key> {
        let mut all_keys = Vec::new();
        for shard in &self.shards {
            let s = shard.read();
            all_keys.extend(s.range(start..end).map(|(k, _)| *k));
        }
        all_keys.sort();
        all_keys
    }

    pub fn should_flush(&self) -> bool {
        self.size.load(Ordering::Relaxed) >= self.max_size
    }

    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.read().is_empty())
    }

    /// Vector search (in-memory graph) — per-key single shard lookup
    pub fn vector_search(&self, query: &[f32], k: usize) -> Result<Vec<(Key, UnifiedEntry, f32)>> {
        let graph = self.vector_graph.as_ref()
            .ok_or_else(|| StorageError::Index("Vector search not supported".into()))?;

        let ef = k * 5;
        let candidates = graph.search(query, k, ef)?;

        let vec_map = self.vectors.as_ref();

        let mut results = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let shard = self.shards[Self::shard_index(candidate.id)].read();
            if let Some(arc_entry) = shard.get(&candidate.id) {
                if !arc_entry.deleted {
                    let vector = vec_map.and_then(|vm| vm.read().get(&candidate.id).cloned());
                    results.push((candidate.id, UnifiedEntry {
                        data: arc_entry.data.clone(),
                        vector,
                        timestamp: arc_entry.timestamp,
                        deleted: false,
                    }, candidate.distance));
                }
            }
        }

        Ok(results)
    }

    pub fn iter(&self) -> UnifiedMemTableIterator {
        let mut all: Vec<(Key, Arc<DataEntry>)> = Vec::new();
        for shard in &self.shards {
            let s = shard.read();
            all.extend(s.iter().map(|(k, v)| (*k, Arc::clone(v))));
        }
        all.sort_by_key(|(k, _)| *k);

        let vec_map = self.vectors.clone();
        let items: Vec<(Key, UnifiedEntry)> = all.into_iter().map(|(k, arc)| {
            let vector = vec_map.as_ref().and_then(|vm| vm.read().get(&k).cloned());
            (k, UnifiedEntry {
                data: arc.data.clone(),
                vector,
                timestamp: arc.timestamp,
                deleted: arc.deleted,
            })
        }).collect();

        UnifiedMemTableIterator { entries: items.into_iter() }
    }

    pub fn snapshot(&self) -> Vec<(Key, UnifiedEntry)> {
        let mut all: Vec<(Key, Arc<DataEntry>)> = Vec::new();
        for shard in &self.shards {
            let s = shard.read();
            all.extend(s.iter().map(|(k, v)| (*k, Arc::clone(v))));
        }
        all.sort_by_key(|(k, _)| *k);

        let vec_map = self.vectors.as_ref();
        all.into_iter().map(|(k, arc)| {
            let vector = vec_map.and_then(|vm| vm.read().get(&k).cloned());
            (k, UnifiedEntry {
                data: arc.data.clone(),
                vector,
                timestamp: arc.timestamp,
                deleted: arc.deleted,
            })
        }).collect()
    }

    /// Range scan — merge ranges from all shards
    pub fn scan(&self, start: Key, end: Key) -> Result<Vec<(Key, UnifiedEntry)>> {
        let mut all: Vec<(Key, Arc<DataEntry>)> = Vec::new();
        for shard in &self.shards {
            let s = shard.read();
            use std::ops::Bound;
            let range = s.range((
                Bound::Included(&start),
                Bound::Excluded(&end)
            ));
            all.extend(range.map(|(k, v)| (*k, Arc::clone(v))));
        }
        all.sort_by_key(|(k, _)| *k);

        let vec_map = self.vectors.as_ref();
        let results: Vec<(Key, UnifiedEntry)> = all.into_iter().map(|(k, arc)| {
            let vector = vec_map.and_then(|vm| vm.read().get(&k).cloned());
            (k, UnifiedEntry {
                data: arc.data.clone(),
                vector,
                timestamp: arc.timestamp,
                deleted: arc.deleted,
            })
        }).collect();

        Ok(results)
    }

    /// Lightweight scan: returns Arc<DataEntry> references without cloning row bytes.
    /// Memory usage is O(N * 24) instead of O(N * avg_data_size).
    /// Skips vector map lookup — use scan_arcs_with_vectors() if vectors needed.
    pub fn scan_arcs(&self, start: Key, end: Key) -> Vec<(Key, Arc<DataEntry>)> {
        let mut all: Vec<(Key, Arc<DataEntry>)> = Vec::new();
        for shard in &self.shards {
            let s = shard.read();
            use std::ops::Bound;
            let range = s.range((
                Bound::Included(&start),
                Bound::Excluded(&end)
            ));
            all.extend(range.map(|(k, v)| (*k, Arc::clone(v))));
        }
        all.sort_by_key(|(k, _)| *k);
        all
    }

    /// Like scan_arcs but also fetches vector data (for vector search paths).
    pub fn scan_arcs_with_vectors(&self, start: Key, end: Key) -> Vec<(Key, Arc<DataEntry>, Option<Vec<f32>>)> {
        let all = self.scan_arcs(start, end);
        let vec_map = self.vectors.as_ref();
        all.into_iter().map(|(k, arc)| {
            let vector = vec_map.and_then(|vm| vm.read().get(&k).cloned());
            (k, arc, vector)
        }).collect()
    }

    /// Full table scan — merge from all shards
    pub fn scan_all(&self) -> Result<Vec<(Key, UnifiedEntry)>> {
        let mut all: Vec<(Key, Arc<DataEntry>)> = Vec::new();
        for shard in &self.shards {
            let s = shard.read();
            all.extend(s.iter().map(|(k, v)| (*k, Arc::clone(v))));
        }
        all.sort_by_key(|(k, _)| *k);

        let vec_map = self.vectors.as_ref();
        let results: Vec<(Key, UnifiedEntry)> = all.into_iter().map(|(k, arc)| {
            let vector = vec_map.and_then(|vm| vm.read().get(&k).cloned());
            (k, UnifiedEntry {
                data: arc.data.clone(),
                vector,
                timestamp: arc.timestamp,
                deleted: arc.deleted,
            })
        }).collect();

        Ok(results)
    }

    pub fn vector_dimension(&self) -> Option<usize> {
        self.vector_dimension
    }
}

/// Unified MemTable Iterator
pub struct UnifiedMemTableIterator {
    entries: std::vec::IntoIter<(Key, UnifiedEntry)>,
}

impl Iterator for UnifiedMemTableIterator {
    type Item = (Key, UnifiedEntry);

    fn next(&mut self) -> Option<Self::Item> {
        self.entries.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_memtable() -> UnifiedMemTable {
        UnifiedMemTable::new(&LSMConfig::default())
    }

    fn create_vector_memtable(dimension: usize) -> UnifiedMemTable {
        UnifiedMemTable::new_with_vector_support(&LSMConfig::default(), dimension)
    }

    #[test]
    fn test_put_get() {
        let memtable = create_memtable();

        let key = 12345u64;
        let value = Value::new(b"test_value".to_vec(), 1);

        memtable.put(key, value.clone()).unwrap();

        let retrieved = memtable.get(key).unwrap().unwrap();
        assert_eq!(retrieved.timestamp, 1);
        assert!(!retrieved.deleted);
    }

    #[test]
    fn test_put_with_vector() {
        let memtable = create_vector_memtable(128);

        let key = 1u64;
        let data = ValueData::Inline(std::sync::Arc::new(b"test_data".to_vec()));
        let vector = vec![1.0f32; 128];

        memtable.put_with_vector(key, data, vector, 1).unwrap();

        let retrieved = memtable.get(key).unwrap().unwrap();
        assert!(retrieved.vector.is_some());
        assert_eq!(retrieved.vector.unwrap().len(), 128);
    }

    #[test]
    fn test_vector_search() {
        let memtable = create_vector_memtable(3);

        for i in 0..10 {
            let key = i;
            let data = ValueData::Inline(std::sync::Arc::new(format!("data_{}", i).into_bytes()));
            let vector = vec![i as f32, (i + 1) as f32, (i + 2) as f32];
            memtable.put_with_vector(key, data, vector, i).unwrap();
        }

        let query = vec![5.0, 6.0, 7.0];
        let results = memtable.vector_search(&query, 3).unwrap();

        assert!(!results.is_empty());
        assert!(results.len() <= 3);

        for (row_id, entry, distance) in results {
            assert!(!entry.deleted);
            assert!(entry.vector.is_some());
            debug_log!("Found row_id={}, distance={:.4}", row_id, distance);
        }
    }

    #[test]
    fn test_size_tracking() {
        let memtable = create_vector_memtable(128);

        assert_eq!(memtable.size(), 0);

        let key = 1u64;
        let data = ValueData::Inline(std::sync::Arc::new(b"test".to_vec()));
        let vector = vec![1.0f32; 128];

        memtable.put_with_vector(key, data, vector, 1).unwrap();

        let size_after = memtable.size();
        assert!(size_after > 0);
        debug_log!("Memory size: {} bytes", size_after);

        // data (4 bytes + 16 overhead) + vector (128 f32 * 4 bytes)
        assert!(size_after > 500, "expected >500 bytes with vector, got {}", size_after);
    }

    #[test]
    fn test_should_flush() {
        let config = LSMConfig { memtable_size: 5000, ..Default::default() };

        let memtable = UnifiedMemTable::new_with_vector_support(&config, 128);

        assert!(!memtable.should_flush());

        for i in 0..250 {
            let data = ValueData::Inline(std::sync::Arc::new(vec![0u8; 10]));
            let vector = vec![1.0f32; 128];
            memtable.put_with_vector(i, data, vector, i).unwrap();
        }

        assert!(memtable.should_flush());
    }

    #[test]
    fn test_non_vector_no_overhead() {
        let memtable = create_memtable();

        // Insert 100 non-vector entries — should not allocate Option<Vec> overhead
        for i in 0..100 {
            let value = Value::new(format!("data_{}", i).into_bytes(), i);
            memtable.put(i, value).unwrap();
        }

        // Verify get works correctly
        for i in 0..100 {
            let entry = memtable.get(i).unwrap().unwrap();
            assert_eq!(entry.vector, None);
            assert_eq!(entry.timestamp, i);
        }
    }

    #[test]
    fn test_shard_distribution() {
        let memtable = create_memtable();
        for i in 0..160u64 {
            let value = Value::new(format!("v_{}", i).into_bytes(), i);
            memtable.put(i, value).unwrap();
        }
        // All 16 shards should have entries
        for (i, shard) in memtable.shards.iter().enumerate() {
            let len = shard.read().len();
            assert!(len > 0, "Shard {} is empty", i);
        }
        assert_eq!(memtable.len(), 160);
    }

    #[test]
    fn test_scan_ordering() {
        let memtable = create_memtable();
        // Insert in non-sequential order
        for i in [50, 10, 99, 1, 77, 23, 42] {
            let value = Value::new(format!("v_{}", i).into_bytes(), i);
            memtable.put(i, value).unwrap();
        }
        let result = memtable.scan(0, 100).unwrap();
        let keys: Vec<Key> = result.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![1, 10, 23, 42, 50, 77, 99]);
    }

    #[test]
    fn test_concurrent_puts() {
        use std::sync::Arc;
        use std::thread;

        let memtable = Arc::new(create_memtable());
        let mut handles = Vec::new();

        for t in 0..4 {
            let mt = Arc::clone(&memtable);
            handles.push(thread::spawn(move || {
                let base = t * 1000;
                for i in 0..1000 {
                    let key = base + i;
                    let value = Value::new(format!("t{}_{}", t, i).into_bytes(), key);
                    mt.put(key, value).unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(memtable.len(), 4000);

        // Verify all entries readable
        for t in 0..4 {
            for i in 0..1000 {
                let key = t * 1000 + i;
                let entry = memtable.get(key).unwrap().unwrap();
                assert_eq!(entry.timestamp, key);
            }
        }
    }
}
