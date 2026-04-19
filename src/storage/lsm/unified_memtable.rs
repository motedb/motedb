//! Unified MemTable: 支持数据 + 向量的统一内存表
//!
//! ## 核心设计
//! - 数据和向量分离存储：DataEntry 只含 row data，无 Option<Vec> 开销
//! - 向量数据单独 BTreeMap，仅向量表创建
//! - 集成 FreshVamanaGraph 用于向量搜索
//!
//! ## 性能优化
//! - Arc<DataEntry> 避免每次 get() 的 clone（8 bytes vs 全行 memcpy）
//! - 非 ACP 表省 24 bytes/row 的 Option<Vec<f32>> 开销

use super::{Key, Value, ValueData, LSMConfig};
use crate::index::fresh_graph::{FreshVamanaGraph, FreshGraphConfig, VectorNode};
use crate::distance::DistanceKind;
use crate::{Result, StorageError};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use parking_lot::RwLock;
use std::collections::BTreeMap;

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
            data: ValueData::Inline(Vec::new()),
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

/// Unified MemTable (数据 + 向量)
pub struct UnifiedMemTable {
    /// 主存储：row_id → Arc<DataEntry> (Arc avoids clone on get)
    entries: Arc<RwLock<BTreeMap<Key, Arc<DataEntry>>>>,

    /// 向量数据 (仅向量表创建，避免非向量表的 Option<Vec> 开销)
    vectors: Option<Arc<RwLock<BTreeMap<Key, Vec<f32>>>>>,

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
    /// 创建不支持向量的 MemTable（兼容旧代码）
    pub fn new(config: &LSMConfig) -> Self {
        Self {
            entries: Arc::new(RwLock::new(BTreeMap::new())),
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
            entries: Arc::new(RwLock::new(BTreeMap::new())),
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

        // Store vector separately
        if let Some(ref vec_map) = self.vectors {
            vec_map.write().insert(key, vector.clone());
        }

        if let Some(ref graph) = self.vector_graph {
            graph.insert(key, vector)?;
        }

        Ok(())
    }

    /// Internal insert with Arc
    fn insert_entry(&self, key: Key, entry: Arc<DataEntry>) -> Result<()> {
        let entry_size = entry.memory_size();

        let mut entries = self.entries.write();

        if let Some(old_entry) = entries.get(&key) {
            let old_size = old_entry.memory_size();
            self.size.fetch_sub(old_size, Ordering::Relaxed);
        }

        entries.insert(key, entry);
        self.size.fetch_add(entry_size, Ordering::Relaxed);
        self.next_seq.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Batch insert (single lock acquisition)
    pub fn batch_put(&self, kvs: &[(Key, Value)]) -> Result<()> {
        if kvs.is_empty() {
            return Ok(());
        }

        let mut entries = self.entries.write();
        let mut total_size_change: i64 = 0;

        for (key, value) in kvs {
            let entry = Arc::new(DataEntry {
                data: value.data.clone(),
                timestamp: value.timestamp,
                deleted: value.deleted,
            });

            let entry_size = entry.memory_size();

            if let Some(old_entry) = entries.get(key) {
                total_size_change -= old_entry.memory_size() as i64;
            }

            entries.insert(*key, entry);
            total_size_change += entry_size as i64;
        }

        if total_size_change > 0 {
            self.size.fetch_add(total_size_change as usize, Ordering::Relaxed);
        } else if total_size_change < 0 {
            self.size.fetch_sub((-total_size_change) as usize, Ordering::Relaxed);
        }

        self.next_seq.fetch_add(kvs.len(), Ordering::Relaxed);
        Ok(())
    }

    /// Get data — returns UnifiedEntry for compatibility (Arc clone + optional vector lookup)
    pub fn get(&self, key: Key) -> Result<Option<UnifiedEntry>> {
        let entries = self.entries.read();
        let Some(arc_entry) = entries.get(&key) else {
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

    /// Delete (insert tombstone)
    pub fn delete(&self, key: Key, timestamp: u64) -> Result<()> {
        let entry = Arc::new(DataEntry {
            data: ValueData::Inline(Vec::new()),
            timestamp,
            deleted: true,
        });

        let mut entries = self.entries.write();

        if let Some(old_entry) = entries.get(&key) {
            self.size.fetch_sub(old_entry.memory_size(), Ordering::Relaxed);
        }

        let entry_size = entry.memory_size();
        entries.insert(key, entry);
        self.size.fetch_add(entry_size, Ordering::Relaxed);

        // Remove vector if present
        if let Some(ref vec_map) = self.vectors {
            vec_map.write().remove(&key);
        }

        Ok(())
    }

    /// Get all keys in [start, end] range (for range delete)
    pub fn keys_in_range(&self, start: Key, end: Key) -> Vec<Key> {
        let entries = self.entries.read();
        entries.range(start..=end).map(|(k, _)| *k).collect()
    }

    pub fn should_flush(&self) -> bool {
        self.size.load(Ordering::Relaxed) >= self.max_size
    }

    #[inline]
    pub fn should_flush_atomic(&self) -> bool {
        self.size.load(Ordering::Relaxed) >= self.max_size
    }

    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Vector search (in-memory graph)
    pub fn vector_search(&self, query: &[f32], k: usize) -> Result<Vec<(Key, UnifiedEntry, f32)>> {
        let graph = self.vector_graph.as_ref()
            .ok_or_else(|| StorageError::Index("Vector search not supported".into()))?;

        let ef = k * 5;
        let candidates = graph.search(query, k, ef)?;

        let entries = self.entries.read();
        let vec_map = self.vectors.as_ref();

        let mut results = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            if let Some(arc_entry) = entries.get(&candidate.id) {
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
        UnifiedMemTableIterator::new(self.entries.clone(), self.vectors.clone())
    }

    pub fn snapshot(&self) -> Vec<(Key, UnifiedEntry)> {
        let entries = self.entries.read();
        let vec_map = self.vectors.as_ref();
        entries.iter().map(|(k, arc)| {
            let vector = vec_map.and_then(|vm| vm.read().get(k).cloned());
            (*k, UnifiedEntry {
                data: arc.data.clone(),
                vector,
                timestamp: arc.timestamp,
                deleted: arc.deleted,
            })
        }).collect()
    }

    /// Range scan
    pub fn scan(&self, start: Key, end: Key) -> Result<Vec<(Key, UnifiedEntry)>> {
        let entries = self.entries.read();
        let vec_map = self.vectors.as_ref();

        use std::ops::Bound;
        let range = entries.range((
            Bound::Included(&start),
            Bound::Excluded(&end)
        ));

        let estimated_size = ((end - start) as usize).min(1000);
        let mut results = Vec::with_capacity(estimated_size);

        for (k, arc) in range {
            let vector = vec_map.and_then(|vm| vm.read().get(k).cloned());
            results.push((*k, UnifiedEntry {
                data: arc.data.clone(),
                vector,
                timestamp: arc.timestamp,
                deleted: arc.deleted,
            }));
        }

        Ok(results)
    }

    /// Full table scan
    pub fn scan_all(&self) -> Result<Vec<(Key, UnifiedEntry)>> {
        let entries = self.entries.read();
        let vec_map = self.vectors.as_ref();

        let mut results = Vec::with_capacity(entries.len());

        for (k, arc) in entries.iter() {
            let vector = vec_map.and_then(|vm| vm.read().get(k).cloned());
            results.push((*k, UnifiedEntry {
                data: arc.data.clone(),
                vector,
                timestamp: arc.timestamp,
                deleted: arc.deleted,
            }));
        }

        Ok(results)
    }

    pub fn export_vector_nodes(&self) -> Result<Vec<(Key, VectorNode)>> {
        let graph = self.vector_graph.as_ref()
            .ok_or_else(|| StorageError::Index("Vector graph not available".into()))?;
        graph.export_nodes()
    }

    pub fn vector_dimension(&self) -> Option<usize> {
        self.vector_dimension
    }
}

/// Unified MemTable Iterator
pub struct UnifiedMemTableIterator {
    entries: std::vec::IntoIter<(Key, UnifiedEntry)>,
}

impl UnifiedMemTableIterator {
    pub fn new(entries: Arc<RwLock<BTreeMap<Key, Arc<DataEntry>>>>, vectors: Option<Arc<RwLock<BTreeMap<Key, Vec<f32>>>>>) -> Self {
        let entries_guard = entries.read();
        let vec_map = vectors.as_ref();
        let items: Vec<(Key, UnifiedEntry)> = entries_guard.iter().map(|(k, arc)| {
            let vector = vec_map.and_then(|vm| vm.read().get(k).cloned());
            (*k, UnifiedEntry {
                data: arc.data.clone(),
                vector,
                timestamp: arc.timestamp,
                deleted: arc.deleted,
            })
        }).collect();
        Self { entries: items.into_iter() }
    }
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
        let data = ValueData::Inline(b"test_data".to_vec());
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
            let data = ValueData::Inline(format!("data_{}", i).into_bytes());
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
        let data = ValueData::Inline(b"test".to_vec());
        let vector = vec![1.0f32; 128];

        memtable.put_with_vector(key, data, vector, 1).unwrap();

        let size_after = memtable.size();
        assert!(size_after > 0);
        debug_log!("Memory size: {} bytes", size_after);

        // DataEntry(4 data + 16 meta) = 20, no vector overhead
        assert!((15..=30).contains(&size_after));
    }

    #[test]
    fn test_should_flush() {
        let config = LSMConfig { memtable_size: 5000, ..Default::default() };

        let memtable = UnifiedMemTable::new_with_vector_support(&config, 128);

        assert!(!memtable.should_flush());

        for i in 0..250 {
            let data = ValueData::Inline(vec![0u8; 10]);
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
}
