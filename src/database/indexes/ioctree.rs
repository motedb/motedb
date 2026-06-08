//! i-Octree Index Operations (3D Point Cloud for Embodied Intelligence)
//!
//! Provides i-Octree spatial indexing for SLAM, robotics, and 3D perception

use crate::database::core::MoteDB;
use crate::types::{RowId, BoundingBox3D, Point3D, Geometry};
use crate::{Result, StorageError};
use crate::index::ioctree::{IOctreeIndex, IOctreeConfig};
use parking_lot::RwLock;
use std::sync::Arc;

impl MoteDB {
    /// Create an i-Octree index for 3D point cloud data
    pub fn create_ioctree_index(&self, name: &str) -> Result<()> {
        ensure_open!(self);
        let indexes_dir = self.path.join("indexes");
        std::fs::create_dir_all(&indexes_dir)?;
        let index_dir = indexes_dir.join(format!("ioctree_{}", name));
        std::fs::create_dir_all(&index_dir)?;

        let config = IOctreeConfig {
            data_dir: Some(index_dir.join("ioctree.bin")),
            ..Default::default()
        };

        let index = IOctreeIndex::new(config, name.to_string())?;
        self.ioctree_indexes.insert(name.to_string(), Arc::new(RwLock::new(index)));
        Ok(())
    }

    /// Insert a 3D point into an i-Octree index
    pub fn insert_ioctree_point(&self, row_id: RowId, index_name: &str, geometry: &Geometry) -> Result<()> {
        if let Some(index) = self.ioctree_indexes.get(index_name) {
            index.write().insert(row_id, geometry)?;
            Ok(())
        } else {
            Err(StorageError::Index(format!("i-Octree index '{}' not found", index_name)))
        }
    }

    /// Delete a point from an i-Octree index by row_id
    pub fn delete_ioctree_point(&self, row_id: RowId, index_name: &str) -> Result<bool> {
        if let Some(index) = self.ioctree_indexes.get(index_name) {
            Ok(index.write().delete(row_id))
        } else {
            Err(StorageError::Index(format!("i-Octree index '{}' not found", index_name)))
        }
    }

    /// 3D range query: find all points within a bounding box
    pub fn ioctree_range_query(&self, index_name: &str, bbox: &BoundingBox3D) -> Result<Vec<RowId>> {
        if let Some(index) = self.ioctree_indexes.get(index_name) {
            return Ok(index.read().range_query(bbox));
        }
        Err(StorageError::Index(format!("i-Octree index '{}' not found", index_name)))
    }

    /// 3D KNN query: find k nearest neighbors
    pub fn ioctree_knn_query(&self, index_name: &str, point: &Point3D, k: usize) -> Result<Vec<(RowId, f64)>> {
        if let Some(index) = self.ioctree_indexes.get(index_name) {
            return Ok(index.read().knn_query(point, k));
        }
        Err(StorageError::Index(format!("i-Octree index '{}' not found", index_name)))
    }

    /// 3D radius search: find all points within radius
    pub fn ioctree_radius_search(&self, index_name: &str, center: &Point3D, radius: f64) -> Result<Vec<(RowId, f64)>> {
        if let Some(index) = self.ioctree_indexes.get(index_name) {
            return Ok(index.read().radius_search(center, radius));
        }
        Err(StorageError::Index(format!("i-Octree index '{}' not found", index_name)))
    }

    /// 🚀 Build i-Octree from columnar SSTable data.
    /// Reads geometries directly from column segment — O(N), zero per-row decode.
    pub fn build_ioctree_from_columnar(
        &self,
        index_name: &str,
        table_name: &str,
        col_position: usize,
    ) -> Result<usize> {
        let col_sst = match self.columnar_sstables.get(table_name) {
            Some(sst) => sst.clone(),
            None => return Ok(0),
        };
        let geoms = col_sst.read_spatial(col_position)?;
        if geoms.is_empty() { return Ok(0); }

        let index_ref = self.ioctree_indexes.get(index_name)
            .ok_or_else(|| StorageError::Index(format!("i-Octree '{}' not found", index_name)))?;
        let mut index = index_ref.value().write();
        for (row_id, geom) in &geoms {
            index.insert(*row_id, geom)?;
        }
        index.flush()?;
        Ok(geoms.len())
    }

    /// Flush all i-Octree indexes to disk
    pub fn flush_ioctree_indexes(&self) -> Result<()> {
        for entry in self.ioctree_indexes.iter() {
            let mut index = entry.value().write();
            if let Err(e) = index.flush() {
                eprintln!("[flush_ioctree] Failed to flush index '{}': {:?}", entry.key(), e);
            }
        }
        Ok(())
    }
}
