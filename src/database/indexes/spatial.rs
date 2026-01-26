//! Spatial Index Operations (Geospatial Queries)
//!
//! Extracted from database_legacy.rs
//! Provides hybrid grid+RTree spatial indexing

use crate::database::core::MoteDB;
use crate::types::{Row, RowId, BoundingBox, Point, Geometry};
use crate::{Result, StorageError};
use crate::index::{SpatialHybridIndex, SpatialHybridConfig, BoundingBoxF32};
use parking_lot::RwLock;
use std::sync::Arc;

/// Spatial index statistics
#[derive(Debug)]
pub struct SpatialIndexStats {
    pub total_entries: usize,
    pub memory_usage: usize,
    pub bytes_per_entry: usize,  // Changed from f64 to usize
}

impl MoteDB {
    /// Create a spatial index with hybrid grid+rtree
    /// 
    /// 🚀 **方案B（高性能）**: 使用scan_range一次性扫描LSM
    /// 
    /// # Example
    /// ```ignore
    /// let bounds = BoundingBox { min_x: 0.0, min_y: 0.0, max_x: 1000.0, max_y: 1000.0 };
    /// db.create_spatial_index("locations", bounds)?;
    /// ```
    pub fn create_spatial_index(&self, name: &str, bounds: BoundingBox) -> Result<()> {
        // 🎯 统一路径：{db}.mote/indexes/spatial_{name}/
        let indexes_dir = self.path.join("indexes");
        std::fs::create_dir_all(&indexes_dir)?;
        let index_dir = indexes_dir.join(format!("spatial_{}", name));
        std::fs::create_dir_all(&index_dir)?;
        
        // Convert BoundingBox (f64) to BoundingBoxF32
        let bounds_f32 = BoundingBoxF32::new(
            bounds.min_x as f32,
            bounds.min_y as f32,
            bounds.max_x as f32,
            bounds.max_y as f32,
        );
        
        let config = SpatialHybridConfig::new(bounds_f32)
            .with_cache_size(128)  // 降低默认 cache，强制使用 mmap
            .with_adaptive(true)
            .with_mmap(true, Some(index_dir.clone()));
        
        let index = SpatialHybridIndex::new(config);
        let index_arc = Arc::new(RwLock::new(index));
        self.spatial_indexes.insert(name.to_string(), index_arc.clone());
        
        // 🚀 方案B：使用scan_range高性能扫描
        // name格式: "table_column"
        let parts: Vec<&str> = name.split('_').collect();
        if parts.len() >= 2 {
            let table_name = parts[0];
            let column_name = parts[1..].join("_");
            
            if let Ok(schema) = self.table_registry.get_table(table_name) {
                if let Some(col_def) = schema.columns.iter().find(|c| c.name == column_name) {
                    let col_position = col_def.position;
                    
                    println!("[create_spatial_index] 🔍 使用scan_range扫描LSM（方案B）...");
                    let start_time = std::time::Instant::now();
                    
                    // 计算表的key范围
                    use std::collections::hash_map::DefaultHasher;
                    use std::hash::{Hash, Hasher};
                    let mut hasher = DefaultHasher::new();
                    table_name.hash(&mut hasher);
                    let table_hash = hasher.finish() & 0xFFFFFFFF;
                    
                    let start_key = table_hash << 32;
                    let end_key = (table_hash + 1) << 32;
                    
                    // 一次scan_range扫描所有数据
                    let mut geometries_to_index = Vec::new();
                    match self.lsm_engine.scan_range(start_key, end_key) {
                        Ok(entries) => {
                            for (composite_key, value) in entries {
                                let row_id = (composite_key & 0xFFFFFFFF) as RowId;
                                
                                let data_bytes = match &value.data {
                                    crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                                    crate::storage::lsm::ValueData::Blob(_) => continue,
                                };
                                
                                if let Ok(row) = bincode::deserialize::<Row>(data_bytes) {
                                    if let Some(crate::types::Value::Spatial(geom)) = row.get(col_position) {
                                        geometries_to_index.push((row_id, geom.clone()));
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("[create_spatial_index] ⚠️ scan_range失败: {}", e);
                        }
                    }
                    
                    let scan_time = start_time.elapsed();
                    
                    if !geometries_to_index.is_empty() {
                        println!("[create_spatial_index] 🚀 扫描完成：{} 个几何对象，耗时 {:?}", 
                                 geometries_to_index.len(), scan_time);
                        
                        let build_time = std::time::Instant::now();
                        for (row_id, geom) in geometries_to_index {
                            if let Err(e) = index_arc.write().insert(row_id, geom) {
                                eprintln!("[create_spatial_index] ⚠️ 插入失败 row_id={}: {}", row_id, e);
                            }
                        }
                        println!("[create_spatial_index] ✅ 批量建索引完成！耗时 {:?}", build_time.elapsed());
                    } else {
                        println!("[create_spatial_index] ⚠️ 未找到任何几何数据（扫描耗时 {:?}）", scan_time);
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Insert geometry into spatial index
    /// 
    /// # Example
    /// ```ignore
    /// let point = Geometry::Point(Point::new(10.5, 20.3));
    /// db.insert_geometry(row_id, "locations", point)?;
    /// ```
    pub fn insert_geometry(&self, row_id: RowId, index_name: &str, geometry: Geometry) -> Result<()> {
        let index_ref = self.spatial_indexes.get(index_name)
            .ok_or_else(|| StorageError::Index(format!("Spatial index '{}' not found", index_name)))?;
        
        index_ref.value().write().insert(row_id, geometry)?;
        Ok(())
    }
    
    /// Batch insert geometries (10-100x faster than individual inserts)
    ///
    /// # Performance Optimization
    /// - Avoids repeated lock acquisition
    /// - Leverages internal batch optimization (adaptive grid)
    /// - Auto triggers incremental persistence if threshold reached
    ///
    /// # Example
    /// ```ignore
    /// let geometries = vec![
    ///     (1, Geometry::Point(Point::new(10.0, 20.0))),
    ///     (2, Geometry::Point(Point::new(30.0, 40.0))),
    ///     (3, Geometry::Point(Point::new(50.0, 60.0))),
    /// ];
    /// db.batch_insert_geometries("locations", geometries)?;
    /// ```
    pub fn batch_insert_geometries(&self, index_name: &str, geometries: Vec<(RowId, Geometry)>) -> Result<usize> {
        if geometries.is_empty() {
            return Ok(0);
        }
        
        let index_ref = self.spatial_indexes.get(index_name)
            .ok_or_else(|| StorageError::Index(format!("Spatial index '{}' not found", index_name)))?;
        
        // Batch insert (acquire write lock once)
        let mut index_guard = index_ref.value().write();
        let count = geometries.len();
        for (row_id, geometry) in geometries {
            index_guard.insert(row_id, geometry)?;
        }
        drop(index_guard);
        
        // Incremental persistence: update counter and check if flush needed
        {
            let mut pending = self.pending_spatial_updates.write();
            *pending += count;
            
            // Strategy: consistent threshold with LSM's pending_updates
            if *pending >= 1_000 {
                // ✅ Reset counter IMMEDIATELY
                *pending = 0;
                drop(pending);
                
                // Trigger incremental flush (background thread)
                let db_clone = self.clone_for_callback();
                std::thread::spawn(move || {
                    let _ = db_clone.flush_spatial_indexes();
                });
            }
        }
        
        Ok(count)
    }
    
    /// Delete geometry from spatial index
    /// 
    /// # Example
    /// ```ignore
    /// db.delete_geometry(row_id, "locations")?;
    /// ```
    pub fn delete_geometry(&self, row_id: RowId, index_name: &str) -> Result<bool> {
        let index_ref = self.spatial_indexes.get(index_name)
            .ok_or_else(|| StorageError::Index(format!("Spatial index '{}' not found", index_name)))?;
        
        let deleted = index_ref.value().write().delete(row_id)?;
        Ok(deleted)
    }
    
    /// Range query on spatial index
    /// 
    /// Returns all geometries within the bounding box
    /// 
    /// # Example
    /// ```ignore
    /// let bbox = BoundingBox { min_x: 10.0, min_y: 10.0, max_x: 50.0, max_y: 50.0 };
    /// let results = db.spatial_range_query("locations", &bbox)?;
    /// ```
    pub fn spatial_range_query(&self, index_name: &str, bbox: &BoundingBox) -> Result<Vec<RowId>> {
        let index_ref = self.spatial_indexes.get(index_name)
            .ok_or_else(|| StorageError::Index(format!("Spatial index '{}' not found", index_name)))?;
        
        let results = index_ref.value().read().range_query(bbox);
        Ok(results)
    }
    
    /// KNN query on spatial index
    /// 
    /// Returns k nearest neighbors to the query point
    /// 
    /// # Example
    /// ```ignore
    /// let point = Point::new(25.0, 25.0);
    /// let nearest = db.spatial_knn_query("locations", &point, 10)?;
    /// for (row_id, distance) in nearest {
    ///     println!("ID: {}, Distance: {:.2}", row_id, distance);
    /// }
    /// ```
    pub fn spatial_knn_query(&self, index_name: &str, point: &Point, k: usize) -> Result<Vec<(RowId, f64)>> {
        let index_ref = self.spatial_indexes.get(index_name)
            .ok_or_else(|| StorageError::Index(format!("Spatial index '{}' not found", index_name)))?;
        
        let results = index_ref.value().read().knn_query(point, k);
        Ok(results)
    }
    
    /// Get spatial index statistics
    /// 
    /// # Example
    /// ```ignore
    /// let stats = db.spatial_index_stats("locations")?;
    /// println!("Total entries: {}", stats.total_entries);
    /// println!("Memory usage: {} bytes", stats.memory_usage);
    /// ```
    pub fn spatial_index_stats(&self, name: &str) -> Result<SpatialIndexStats> {
        let index_ref = self.spatial_indexes.get(name)
            .ok_or_else(|| StorageError::Index(format!("Spatial index '{}' not found", name)))?;
        
        let index_guard = index_ref.value().read();
        let mem_stats = index_guard.memory_usage();
        
        Ok(SpatialIndexStats {
            total_entries: index_guard.len(),
            memory_usage: mem_stats.grid_overhead + mem_stats.rtree_memory,
            bytes_per_entry: mem_stats.bytes_per_entry,
        })
    }
    
    /// Flush spatial indexes to disk
    /// 
    /// Persists all spatial index structures (grid + RTree) to disk
    pub fn flush_spatial_indexes(&self) -> Result<()> {
        // 🚀 DashMap: 直接遍历
        for entry in self.spatial_indexes.iter() {
            let name = entry.key();
            let index = entry.value();
            
            // ⭐ 修复路径：应该是 {db}.mote/indexes/spatial_{name}
            let index_dir = self.path.join("indexes").join(format!("spatial_{}", name));
            
            index.write().save(&index_dir)?;
        }
        Ok(())
    }
    
    /// Debug spatial index memory usage (detailed analysis)
    /// 
    /// Prints detailed memory breakdown to stdout
    pub fn debug_spatial_index_memory(&self, name: &str) {
        if let Some(index_ref) = self.spatial_indexes.get(name) {
            index_ref.value().read().debug_memory_usage();
        } else {
            println!("空间索引 '{}' 不存在", name);
        }
    }
}
