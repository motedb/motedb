//! Unified MemTable: 支持数据 + 向量的统一内存表
//!
//! ## 核心设计
//! - 数据和向量在同一个 Entry 中存储
//! - 集成 FreshVamanaGraph 用于向量搜索
//! - 统一的 flush 时机（数据 + 向量）
//!
//! ## 性能目标
//! - 内存占用: < 15MB (4000 行 × (1KB 数据 + 512 bytes 向量 + 图结构))
//! - 向量搜索: < 2ms (内存图)
//! - Flush 延迟: < 100ms (单次 fsync)

use super::{Key, Value, ValueData, LSMConfig};
use crate::index::diskann::fresh_graph::{FreshVamanaGraph, FreshGraphConfig, VectorNode};
use crate::distance::{Cosine, DistanceMetric};
use crate::{Result, StorageError};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::collections::BTreeMap;

/// Unified Entry (数据 + 向量)
#[derive(Clone, Debug)]
pub struct UnifiedEntry {
    /// Row data (原始列数据)
    pub data: ValueData,
    
    /// Vector (可选，如果该表有向量列)
    pub vector: Option<Vec<f32>>,
    
    /// MVCC metadata
    pub timestamp: u64,
    pub deleted: bool,
}

impl UnifiedEntry {
    pub fn new(data: ValueData, vector: Option<Vec<f32>>, timestamp: u64) -> Self {
        Self {
            data,
            vector,
            timestamp,
            deleted: false,
        }
    }
    
    pub fn tombstone(timestamp: u64) -> Self {
        Self {
            data: ValueData::Inline(Vec::new()),
            vector: None,
            timestamp,
            deleted: true,
        }
    }
    
    /// 计算 entry 的内存大小
    pub fn memory_size(&self) -> usize {
        let data_size = match &self.data {
            ValueData::Inline(data) => data.len(),
            ValueData::Blob(_) => 16, // BlobRef size
        };
        let vector_size = self.vector.as_ref().map(|v| v.len() * 4).unwrap_or(0);
        data_size + vector_size + 16 // +16 for metadata
    }
}

/// Unified MemTable (数据 + 向量)
pub struct UnifiedMemTable {
    /// 主存储：row_id → entry
    entries: Arc<RwLock<BTreeMap<Key, UnifiedEntry>>>,
    
    /// 🆕 向量图索引：Fresh Vamana Graph
    /// - 只在有向量的表中使用
    /// - 图的节点 ID = row_id
    vector_graph: Option<Arc<FreshVamanaGraph>>,
    
    /// 向量维度（如果支持向量）
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
            max_nodes: 5000,  // Fresh Graph 最多 5000 个向量
            max_degree: 32,
            search_list_size: 64,
            alpha: 1.2,
            memory_threshold: 20 * 1024 * 1024, // 20MB
        };
        
        let metric = Arc::new(Cosine) as Arc<dyn DistanceMetric>;
        let vector_graph = FreshVamanaGraph::new(fresh_config, metric);
        
        Self {
            entries: Arc::new(RwLock::new(BTreeMap::new())),
            vector_graph: Some(Arc::new(vector_graph)),
            vector_dimension: Some(dimension),
            size: AtomicUsize::new(0),
            max_size: config.memtable_size,
            next_seq: AtomicUsize::new(0),
        }
    }
    
    /// 插入数据（不含向量）
    pub fn put(&self, key: Key, value: Value) -> Result<()> {
        let entry = UnifiedEntry {
            data: value.data,
            vector: None,
            timestamp: value.timestamp,
            deleted: value.deleted,
        };
        self.put_unified(key, entry)
    }
    
    /// 插入数据 + 向量
    pub fn put_with_vector(&self, key: Key, data: ValueData, vector: Vec<f32>, timestamp: u64) -> Result<()> {
        // 验证向量维度
        if let Some(expected_dim) = self.vector_dimension {
            if vector.len() != expected_dim {
                return Err(StorageError::InvalidData(
                    format!("Vector dimension mismatch: expected {}, got {}", expected_dim, vector.len())
                ));
            }
        }
        
        let entry = UnifiedEntry::new(data, Some(vector.clone()), timestamp);
        
        // 插入 entry
        self.put_unified(key, entry)?;
        
        // 插入向量到 Fresh Graph
        if let Some(ref graph) = self.vector_graph {
            graph.insert(key, vector)?;
        }
        
        Ok(())
    }
    
    /// 内部统一插入逻辑
    fn put_unified(&self, key: Key, entry: UnifiedEntry) -> Result<()> {
        let entry_size = entry.memory_size();
        
        let mut entries = self.entries.write()
            .map_err(|_| StorageError::Lock("UnifiedMemTable lock poisoned".into()))?;
        
        // 如果是更新，先减去旧 entry 的大小
        if let Some(old_entry) = entries.get(&key) {
            let old_size = old_entry.memory_size();
            self.size.fetch_sub(old_size, Ordering::Relaxed);
        }
        
        // 插入新 entry
        entries.insert(key, entry);
        self.size.fetch_add(entry_size, Ordering::Relaxed);
        self.next_seq.fetch_add(1, Ordering::Relaxed);
        
        Ok(())
    }
    
    /// 获取数据
    pub fn get(&self, key: Key) -> Result<Option<UnifiedEntry>> {
        let entries = self.entries.read()
            .map_err(|_| StorageError::Lock("UnifiedMemTable lock poisoned".into()))?;
        
        Ok(entries.get(&key).cloned())
    }
    
    /// 删除（插入 tombstone）
    pub fn delete(&self, key: Key, timestamp: u64) -> Result<()> {
        let entry = UnifiedEntry::tombstone(timestamp);
        
        let mut entries = self.entries.write()
            .map_err(|_| StorageError::Lock("UnifiedMemTable lock poisoned".into()))?;
        
        if let Some(old_entry) = entries.get(&key) {
            let old_size = old_entry.memory_size();
            self.size.fetch_sub(old_size, Ordering::Relaxed);
        }
        
        entries.insert(key, entry.clone());
        self.size.fetch_add(entry.memory_size(), Ordering::Relaxed);
        
        // TODO: 从 Fresh Graph 删除向量
        // if let Some(ref graph) = self.vector_graph {
        //     graph.delete(key)?;
        // }
        
        Ok(())
    }
    
    /// 检查是否需要 flush
    pub fn should_flush(&self) -> bool {
        self.size.load(Ordering::Relaxed) >= self.max_size
    }
    
    /// 获取当前内存占用
    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }
    
    /// 获取 entry 数量
    pub fn len(&self) -> usize {
        self.entries.read()
            .map(|entries| entries.len())
            .unwrap_or(0)
    }
    
    /// 检查是否为空
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    
    /// 🎯 向量搜索（内存图）
    /// 
    /// 返回: Vec<(row_id, UnifiedEntry, distance)>
    /// - 包含完整的 row data，无需再次查询
    pub fn vector_search(&self, query: &[f32], k: usize) -> Result<Vec<(Key, UnifiedEntry, f32)>> {
        let graph = self.vector_graph.as_ref()
            .ok_or_else(|| StorageError::Index("Vector search not supported".into()))?;
        
        // 1. 使用 Fresh Graph 搜索
        let ef = k * 5; // ef = 5k
        let candidates = graph.search(query, k, ef)?;
        
        // 2. 获取完整的 entry
        let entries = self.entries.read()
            .map_err(|_| StorageError::Lock("UnifiedMemTable lock poisoned".into()))?;
        
        // 🚀 P3 优化：预分配 k 个结果
        let mut results = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            if let Some(entry) = entries.get(&candidate.id) {
                if !entry.deleted {
                    results.push((candidate.id, entry.clone(), candidate.distance));
                }
            }
        }
        
        Ok(results)
    }
    
    /// 迭代所有 entries（用于 flush）
    pub fn iter(&self) -> UnifiedMemTableIterator {
        UnifiedMemTableIterator::new(self.entries.clone())
    }
    
    /// 获取 snapshot（测试用）
    pub fn snapshot(&self) -> Vec<(Key, UnifiedEntry)> {
        let entries = self.entries.read()
            .expect("UnifiedMemTable snapshot: lock poisoned (unrecoverable in test)");
        entries.iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }
    
    /// 范围扫描
    pub fn scan(&self, start: Key, end: Key) -> Result<Vec<(Key, UnifiedEntry)>> {
        let entries = self.entries.read()
            .map_err(|_| StorageError::Lock("UnifiedMemTable lock poisoned".into()))?;
        
        use std::ops::Bound;
        let range = entries.range((
            Bound::Included(&start),
            Bound::Excluded(&end)
        ));
        
        // 🚀 P3 优化：预分配容量（估算范围大小）
        let estimated_size = ((end - start) as usize).min(1000);
        let mut results = Vec::with_capacity(estimated_size);
        
        // ⚠️ CRITICAL: 不要在这里过滤 deleted entries
        // tombstone 必须返回给 scan_range() 以确保后续层不会返回已删除的旧数据
        // 过滤在 scan_range() 的最后阶段进行
        for (k, v) in range {
            results.push((*k, v.clone()));
        }
        
        Ok(results)
    }
    
    /// 全表扫描
    pub fn scan_all(&self) -> Result<Vec<(Key, UnifiedEntry)>> {
        let entries = self.entries.read()
            .map_err(|_| StorageError::Lock("UnifiedMemTable lock poisoned".into()))?;
        
        // 🚀 P3 优化：预分配容量
        let mut results = Vec::with_capacity(entries.len());
        
        // ⚠️ CRITICAL: 不要过滤 deleted entries（tombstones 需要返回）
        for (k, v) in entries.iter() {
            results.push((*k, v.clone()));
        }
        
        Ok(results)
    }
    
    /// 导出向量图的所有节点（用于 flush 到 SST）
    pub fn export_vector_nodes(&self) -> Result<Vec<(Key, VectorNode)>> {
        let graph = self.vector_graph.as_ref()
            .ok_or_else(|| StorageError::Index("Vector graph not available".into()))?;
        
        graph.export_nodes()
    }
    
    /// 🆕 获取向量维度（如果支持向量）
    pub fn vector_dimension(&self) -> Option<usize> {
        self.vector_dimension
    }
}

/// Unified MemTable 迭代器
pub struct UnifiedMemTableIterator {
    entries: std::vec::IntoIter<(Key, UnifiedEntry)>,
}

impl UnifiedMemTableIterator {
    pub fn new(entries: Arc<RwLock<BTreeMap<Key, UnifiedEntry>>>) -> Self {
        let entries_guard = entries.read()
            .expect("UnifiedMemTableIterator: lock poisoned (unrecoverable)");
        let entries: Vec<(Key, UnifiedEntry)> = entries_guard.iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        Self {
            entries: entries.into_iter(),
        }
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
        assert_eq!(retrieved.deleted, false);
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
        
        // 插入向量
        for i in 0..10 {
            let key = i;
            let data = ValueData::Inline(format!("data_{}", i).into_bytes());
            let vector = vec![i as f32, (i + 1) as f32, (i + 2) as f32];
            memtable.put_with_vector(key, data, vector, i).unwrap();
        }
        
        // 搜索
        let query = vec![5.0, 6.0, 7.0];
        let results = memtable.vector_search(&query, 3).unwrap();
        
        assert!(results.len() > 0);
        assert!(results.len() <= 3);
        
        // 验证返回的是完整 entry
        for (row_id, entry, distance) in results {
            assert!(!entry.deleted);
            assert!(entry.vector.is_some());
            println!("Found row_id={}, distance={:.4}", row_id, distance);
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
        println!("Memory size: {} bytes", size_after);
        
        // 应该约等于: 4 (data) + 512 (vector) + 16 (metadata) = 532 bytes
        assert!(size_after >= 500 && size_after <= 600);
    }
    
    #[test]
    fn test_should_flush() {
        let mut config = LSMConfig::default();
        config.memtable_size = 5000; // 5KB for testing
        
        let memtable = UnifiedMemTable::new_with_vector_support(&config, 128);
        
        assert_eq!(memtable.should_flush(), false);
        
        // 插入数据直到需要 flush
        // 每个 entry 约 532 bytes, 5000/532 ≈ 9 个
        for i in 0..10 {
            let data = ValueData::Inline(vec![0u8; 10]);
            let vector = vec![1.0f32; 128];
            memtable.put_with_vector(i, data, vector, i).unwrap();
        }
        
        assert_eq!(memtable.should_flush(), true);
    }
}
