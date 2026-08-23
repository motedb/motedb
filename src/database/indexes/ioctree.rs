//! i-Octree Index Operations (3D Point Cloud for Embodied Intelligence)
//!
//! Provides i-Octree spatial indexing for SLAM, robotics, and 3D perception

use crate::database::core::MoteDB;
use crate::index::ioctree::{IOctreeConfig, IOctreeIndex};
use crate::types::{BoundingBox3D, Geometry, Point3D, RowId};
use crate::{Result, StorageError};
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
        let is_new = !self.ioctree_indexes.contains_key(name);
        self.ioctree_indexes
            .insert(name.to_string(), Arc::new(RwLock::new(index)));

        // 🔑 Register + backfill. Without a registry entry (table, column),
        // the INSERT-time backfill (find_by_column … IndexType::Octree) can
        // never resolve this index and it stays empty forever (knn always 0).
        // The name-only API resolves via the documented "{table}_{column}"
        // convention against Spatial columns.
        if let Some((table, column)) = self.resolve_octree_target(name) {
            // Tolerant registration: the SQL CREATE INDEX path also registers
            // (its own metadata is authoritative); "already exists" is fine.
            let metadata = crate::database::index_metadata::IndexMetadata::new(
                name.to_string(),
                table.clone(),
                column.clone(),
                crate::database::index_metadata::IndexType::Octree,
            );
            if let Err(e) = self.index_registry.register(metadata) {
                debug_log!("[create_ioctree_index] registry: {:?}", e);
            }
            // Backfill existing rows — ONLY on a fresh index (the SQL
            // CREATE INDEX path backfills separately; running both
            // double-inserted every point).
            if is_new {
                if let Ok(schema) = self.table_registry.get_table(&table) {
                    if let Some(col_def) = schema.columns.iter().find(|c| c.name == column) {
                        let pos = col_def.position;
                        let iter = self.scan_table_rows_streaming(&table)?;
                        for result in iter {
                            let (row_id, row) = match result {
                                Ok(r) => r,
                                Err(_) => continue,
                            };
                            if let Some(crate::types::Value::Spatial(geometry)) = row.get(pos) {
                                if geometry.is_3d() {
                                    let _ = self.insert_ioctree_point(row_id, name, geometry);
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Resolve "{table}_{column}" against registered Spatial columns.
    /// Longest table-name match wins (tables may contain underscores).
    fn resolve_octree_target(&self, index_name: &str) -> Option<(String, String)> {
        let tables = self.table_registry.list_tables().ok()?;
        let mut best: Option<(usize, String, String)> = None;
        for table in tables {
            let prefix = format!("{}_", table);
            if let Some(col) = index_name.strip_prefix(&prefix) {
                if let Ok(schema) = self.table_registry.get_table(&table) {
                    if schema.columns.iter().any(|c| {
                        c.name == col && matches!(c.col_type, crate::types::ColumnType::Spatial)
                    }) {
                        let score = table.len();
                        if best.as_ref().map(|(s, _, _)| score > *s).unwrap_or(true) {
                            best = Some((score, table, col.to_string()));
                        }
                    }
                }
            }
        }
        best.map(|(_, t, c)| (t, c))
    }

    /// Number of indexed points (None if the index doesn't exist). Used by
    /// the CREATE INDEX path to skip a second backfill when the index was
    /// already populated at creation.
    pub fn ioctree_point_count(&self, index_name: &str) -> Option<usize> {
        self.ioctree_indexes
            .get(index_name)
            .map(|i| i.value().read().len())
    }

    /// Insert a 3D point into an i-Octree index
    pub fn insert_ioctree_point(
        &self,
        row_id: RowId,
        index_name: &str,
        geometry: &Geometry,
    ) -> Result<()> {
        if let Some(index) = self.ioctree_indexes.get(index_name) {
            index.write().insert(row_id, geometry)?;
            Ok(())
        } else {
            Err(StorageError::Index(format!(
                "i-Octree index '{}' not found",
                index_name
            )))
        }
    }

    /// Delete a point from an i-Octree index by row_id
    pub fn delete_ioctree_point(&self, row_id: RowId, index_name: &str) -> Result<bool> {
        if let Some(index) = self.ioctree_indexes.get(index_name) {
            Ok(index.write().delete(row_id))
        } else {
            Err(StorageError::Index(format!(
                "i-Octree index '{}' not found",
                index_name
            )))
        }
    }

    /// 3D range query: find all points within a bounding box
    pub fn ioctree_range_query(
        &self,
        index_name: &str,
        bbox: &BoundingBox3D,
    ) -> Result<Vec<RowId>> {
        if let Some(index) = self.ioctree_indexes.get(index_name) {
            return Ok(index.read().range_query(bbox));
        }
        Err(StorageError::Index(format!(
            "i-Octree index '{}' not found",
            index_name
        )))
    }

    /// 3D KNN query: find k nearest neighbors
    pub fn ioctree_knn_query(
        &self,
        index_name: &str,
        point: &Point3D,
        k: usize,
    ) -> Result<Vec<(RowId, f64)>> {
        if let Some(index) = self.ioctree_indexes.get(index_name) {
            return Ok(index.read().knn_query(point, k));
        }
        Err(StorageError::Index(format!(
            "i-Octree index '{}' not found",
            index_name
        )))
    }

    /// 3D radius search: find all points within radius
    pub fn ioctree_radius_search(
        &self,
        index_name: &str,
        center: &Point3D,
        radius: f64,
    ) -> Result<Vec<(RowId, f64)>> {
        if let Some(index) = self.ioctree_indexes.get(index_name) {
            return Ok(index.read().radius_search(center, radius));
        }
        Err(StorageError::Index(format!(
            "i-Octree index '{}' not found",
            index_name
        )))
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
        if geoms.is_empty() {
            return Ok(0);
        }

        let index_ref = self
            .ioctree_indexes
            .get(index_name)
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
                eprintln!(
                    "[flush_ioctree] Failed to flush index '{}': {:?}",
                    entry.key(),
                    e
                );
            }
        }
        Ok(())
    }
}
