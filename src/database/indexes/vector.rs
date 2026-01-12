//! Vector Index Operations (Similarity Search)
//!
//! Extracted from database_legacy.rs
//! Provides DiskANN-based vector similarity search

use crate::database::core::MoteDB;
use crate::types::{Row, RowId, Value};
use crate::{Result, StorageError};
use crate::index::vamana::{DiskANNIndex, VamanaConfig};
use parking_lot::RwLock;
use std::sync::Arc;

/// Vector index statistics
#[derive(Debug)]
pub struct VectorIndexStats {
    pub total_vectors: usize,
    pub dimension: usize,
    pub cache_hit_rate: f32,  // Changed from f64 to f32
    pub memory_usage: usize,
    pub disk_usage: usize,
}

impl MoteDB {
    /// Create a vector index with DiskANN
    /// 
    /// 🚀 **方案B（高性能）**: 使用scan_range一次性扫描LSM
    /// 
    /// # Performance
    /// - 方案A（旧）: O(N × log M) - 逐个get()，N=行数，M=SST数量
    /// - 方案B（新）: O(N) - 顺序扫描，自动跳过已删除数据
    /// 
    /// # Example
    /// ```ignore
    /// db.create_vector_index("products_embedding", 768)?;
    /// ```
    pub fn create_vector_index(&self, name: &str, dimension: usize) -> Result<()> {
        // 🎯 统一路径：{db}.mote/indexes/vector_{name}/
        let indexes_dir = self.path.join("indexes");
        std::fs::create_dir_all(&indexes_dir)?;
        let index_dir = indexes_dir.join(format!("vector_{}", name));
        std::fs::create_dir_all(&index_dir)?;
        
        let config = VamanaConfig::default();
        let index = DiskANNIndex::create(&index_dir, dimension, config)?;
        let index_arc = Arc::new(RwLock::new(index));
        self.vector_indexes.insert(name.to_string(), index_arc.clone());
        
        // 🚀 方案B：使用scan_range高性能扫描
        // name格式: "table_column"，需要解析出表名和列名
        let parts: Vec<&str> = name.split('_').collect();
        if parts.len() >= 2 {
            let table_name = parts[0];
            let column_name = parts[1..].join("_");
            
            // 获取列在schema中的位置
            if let Ok(schema) = self.table_registry.get_table(table_name) {
                if let Some(col_def) = schema.columns.iter().find(|c| c.name == column_name) {
                    let col_position = col_def.position;
                    
                    println!("[create_vector_index] 🔍 使用scan_range扫描LSM（方案B高性能）...");
                    let start_time = std::time::Instant::now();
                    
                    // 🚀 关键：计算该表的key范围
                    use std::collections::hash_map::DefaultHasher;
                    use std::hash::{Hash, Hasher};
                    let mut hasher = DefaultHasher::new();
                    table_name.hash(&mut hasher);
                    let table_hash = (hasher.finish() & 0xFFFFFFFF) as u64;
                    
                    // composite_key格式: [table_hash:32位][row_id:32位]
                    let start_key = table_hash << 32;              // table的起始key
                    let end_key = (table_hash + 1) << 32;          // table的结束key
                    
                    // 🚀 高性能：一次scan_range扫描所有数据
                    let mut vectors_to_index = Vec::new();
                    match self.lsm_engine.scan_range(start_key, end_key) {
                        Ok(entries) => {
                            for (composite_key, value) in entries {
                                // 提取row_id
                                let row_id = (composite_key & 0xFFFFFFFF) as RowId;
                                
                                // 反序列化行数据
                                let data_bytes = match &value.data {
                                    crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                                    crate::storage::lsm::ValueData::Blob(_) => continue,
                                };
                                
                                if let Ok(row) = bincode::deserialize::<Row>(data_bytes) {
                                    if let Some(crate::types::Value::Vector(vec_data)) = row.get(col_position) {
                                        vectors_to_index.push((row_id, vec_data.clone()));
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("[create_vector_index] ⚠️ scan_range失败: {}", e);
                        }
                    }
                    
                    let scan_time = start_time.elapsed();
                    
                    if !vectors_to_index.is_empty() {
                        println!("[create_vector_index] 🚀 扫描完成：{} 个向量，耗时 {:?}", 
                                 vectors_to_index.len(), scan_time);
                        
                        let build_time = std::time::Instant::now();
                        index_arc.write().batch_insert(&vectors_to_index)?;
                        println!("[create_vector_index] ✅ 批量建索引完成！耗时 {:?}", build_time.elapsed());
                    } else {
                        println!("[create_vector_index] ⚠️ 未找到任何向量数据（扫描耗时 {:?}）", scan_time);
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Update vector for a row
    /// 
    /// # Example
    /// ```ignore
    /// let embedding = vec![0.1, 0.2, 0.3, ...]; // 768-dim vector
    /// db.update_vector(row_id, "products_embedding", &embedding)?;
    /// ```
    pub fn update_vector(&self, row_id: RowId, index_name: &str, vector: &[f32]) -> Result<()> {
        let index_ref = self.vector_indexes.get(index_name)
            .ok_or_else(|| StorageError::Index(format!("Vector index '{}' not found", index_name)))?;
        
        index_ref.value().write().insert(row_id, vector.to_vec())?;
        Ok(())
    }
    
    /// Delete vector from index
    /// 
    /// # Example
    /// ```ignore
    /// db.delete_vector(row_id, "products_embedding")?;
    /// ```
    pub fn delete_vector(&self, row_id: RowId, index_name: &str) -> Result<bool> {
        let index_ref = self.vector_indexes.get(index_name)
            .ok_or_else(|| StorageError::Index(format!("Vector index '{}' not found", index_name)))?;
        
        let deleted = index_ref.value().write().delete(row_id)?;
        Ok(deleted)
    }
    
    /// Batch update vectors for multiple rows (optimized)
    /// 
    /// # Performance
    /// - 10-100x faster than individual inserts
    /// - Batches graph building operations
    /// 
    /// # Example
    /// ```ignore
    /// let vectors = vec![
    ///     (1, vec![0.1, 0.2, 0.3]),
    ///     (2, vec![0.4, 0.5, 0.6]),
    ///     (3, vec![0.7, 0.8, 0.9]),
    /// ];
    /// db.batch_update_vectors("products_embedding", vectors)?;
    /// ```
    pub fn batch_update_vectors(&self, index_name: &str, vectors: Vec<(RowId, Vec<f32>)>) -> Result<usize> {
        let index_ref = self.vector_indexes.get(index_name)
            .ok_or_else(|| StorageError::Index(format!("Vector index '{}' not found", index_name)))?;
        
        let count = index_ref.value().write().batch_insert(&vectors)?;
        Ok(count)
    }
    
    /// Batch insert vectors (alias for batch_update_vectors)
    pub fn batch_insert_vectors(&self, index_name: &str, vectors: &[(RowId, Vec<f32>)]) -> Result<usize> {
        self.batch_update_vectors(index_name, vectors.to_vec())
    }
    
    /// 🔧 FIX: Find vector index name by table and column
    /// This returns the actual user-specified index name, not auto-generated
    pub fn find_vector_index_name(&self, table_name: &str, column_name: &str) -> Result<String> {
        self.table_registry.find_vector_index(table_name, column_name)
    }
    
    /// Check if a vector index exists
    pub fn has_vector_index(&self, index_name: &str) -> bool {
        self.vector_indexes.contains_key(index_name)
    }

    /// Search for nearest neighbors (merges DiskANN index + memtable data)
    /// 
    /// # LSM Architecture
    /// - Searches both persisted DiskANN index (SSTable data)
    /// - Scans MemTable for new vectors
    /// - Merges and re-ranks results
    /// 
    /// # Example
    /// ```ignore
    /// let query = vec![0.5, 0.5, 0.5]; // 3-dim query vector
    /// let results = db.vector_search("products_embedding", &query, 10)?;
    /// for (row_id, distance) in results {
    ///     println!("ID: {}, Distance: {:.4}", row_id, distance);
    /// }
    /// ```
    pub fn vector_search(&self, index_name: &str, query: &[f32], k: usize) -> Result<Vec<(RowId, f32)>> {
        debug_log!("[vector_search] START: index={}, k={}", index_name, k);
        
        let index_ref = self.vector_indexes.get(index_name)
            .ok_or_else(|| StorageError::Index(format!("Vector index '{}' not found", index_name)))?;
        
        debug_log!("[vector_search] 获取index_guard...");
        let index_guard = index_ref.value().read();
        
        debug_log!("[vector_search] 开始搜索DiskANN index...");
        // 1. Search from DiskANN index (persisted data in SST)
        let mut index_results = index_guard.search(query, k * 2)?;  // 🔧 取 2k 为后续合并留空间
        drop(index_guard);
        
        // 🔍 Debug: 打印前5个结果
        if !index_results.is_empty() {
            debug_log!("[vector_search] 🔍 DiskANN返回的前5个结果:");
            for (i, (id, dist)) in index_results.iter().take(5).enumerate() {
                debug_log!("[vector_search]   {}. id={}, distance={:.4}", i+1, id, dist);
            }
        }
        
        debug_log!("[vector_search] DiskANN index搜索完成，结果数: {}", index_results.len());
        
        // 2. 🆕 Scan memtable for vector data
        // Extract table name and column name from index_name (format: "table_column")
        let parts: Vec<&str> = index_name.split('_').collect();
        if parts.len() < 2 {
            // If parsing fails, just return index results (backward compatible)
            index_results.truncate(k);
            return Ok(index_results);
        }
        
        let table_name = parts[0];
        let column_name = parts[1..].join("_");
        
        // Get column position from table registry
        let col_position = match self.table_registry.get_table(table_name) {
            Ok(schema) => {
                schema.columns.iter()
                    .find(|c| c.name == column_name)
                    .map(|c| c.position)
            }
            Err(_) => None,
        };
        
        if col_position.is_none() {
            // Schema not found, just return index results (backward compatible)
            index_results.truncate(k);
            return Ok(index_results);
        }
        let col_position = col_position.unwrap();
        
        // Scan memtable for vectors in this column
        let mut memtable_results = Vec::new();
        self.lsm_engine.scan_memtable_incremental_with(|composite_key, row_bytes| {
            // 🔧 FIX: Extract real row_id from composite_key
            // composite_key format: [table_hash:32bits][row_id:32bits]
            let row_id = (composite_key & 0xFFFFFFFF) as RowId;
            
            // Parse row to get vector value at col_position
            // Row format: bincode-serialized Vec<Value>
            if let Ok(row_values) = bincode::deserialize::<Vec<Value>>(row_bytes) {
                if let Some(Value::Vector(vec_data)) = row_values.get(col_position) {
                    if vec_data.len() == query.len() {
                        // Compute L2 distance
                        let distance: f32 = vec_data.iter()
                            .zip(query.iter())
                            .map(|(a, b)| (a - b).powi(2))
                            .sum::<f32>()
                            .sqrt();
                        
                        memtable_results.push((row_id, distance));
                    }
                }
            }
            Ok(())
        })?;
        
        // 🔍 Debug: 打印memtable扫描结果
        if !memtable_results.is_empty() {
            debug_log!("[vector_search] 🔍 Memtable扫描到{}个向量", memtable_results.len());
            debug_log!("[vector_search] 🔍 Memtable前5个: {:?}", 
                &memtable_results.iter().take(5).map(|(id, dist)| (id, format!("{:.4}", dist))).collect::<Vec<_>>());
        } else {
            debug_log!("[vector_search] 🔍 Memtable为空（数据已全部flush到SST）");
        }
        
        // 3. Merge index_results and memtable_results
        if !memtable_results.is_empty() {
            debug_log!("[vector_search] ⚠️ 合并memtable结果...");
            let before_len = index_results.len();
            index_results.extend(memtable_results);
            debug_log!("[vector_search] 合并后: {} -> {} 个结果", before_len, index_results.len());
            
            // Sort by distance and take top-k
            index_results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
            
            // 🔍 Debug: 打印合并后的前5个
            debug_log!("[vector_search] 🔍 合并排序后前5个:");
            for (i, (id, dist)) in index_results.iter().take(5).enumerate() {
                debug_log!("[vector_search]   {}. id={}, distance={:.4}", i+1, id, dist);
            }
        }
        index_results.truncate(k);
        
        debug_log!("[vector_search] 🔍 最终返回{}个结果", index_results.len());
        if !index_results.is_empty() {
            debug_log!("[vector_search] 🔍 最终结果前5个ID: {:?}", 
                &index_results.iter().take(5).map(|(id, _)| id).collect::<Vec<_>>());
        }
        
        Ok(index_results)
    }
    
    /// Get vector index statistics
    /// 
    /// # Example
    /// ```ignore
    /// let stats = db.vector_index_stats("products_embedding")?;
    /// println!("Total vectors: {}", stats.total_vectors);
    /// println!("Dimension: {}", stats.dimension);
    /// println!("Cache hit rate: {:.2}%", stats.cache_hit_rate * 100.0);
    /// ```
    pub fn vector_index_stats(&self, name: &str) -> Result<VectorIndexStats> {
        let index_ref = self.vector_indexes.get(name)
            .ok_or_else(|| StorageError::Index(format!("Vector index '{}' not found", name)))?;
        
        let index_guard = index_ref.value().read();
        let stats = index_guard.stats();
        let storage_stats = index_guard.storage_stats();
        
        Ok(VectorIndexStats {
            total_vectors: stats.node_count,
            dimension: stats.dimension,
            cache_hit_rate: storage_stats.cache_hit_rate,
            memory_usage: (storage_stats.vector_memory_kb + storage_stats.graph_memory_kb) * 1024,
            disk_usage: (storage_stats.vector_disk_kb + storage_stats.graph_disk_kb) * 1024,
        })
    }
    
    /// Flush vector indexes to disk
    /// 
    /// Persists DiskANN graph and vectors to disk
    pub fn flush_vector_indexes(&self) -> Result<()> {
        // 🚀 DashMap: 直接遍历，无需收集
        for entry in self.vector_indexes.iter() {
            entry.value().write().flush()?;
        }
        Ok(())
    }
}
