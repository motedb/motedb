//! i-Octree: Disk-first Incremental Octree for 3D point cloud spatial indexing
//!
//! Based on "i-Octree: A Fast, Lightweight, and Dynamic Octree for Proximity Search"
//! (ICRA 2024, Tsinghua University).
//!
//! Architecture: 2-tier bounded memory
//! - Tier 0: Pending buffer (~2048 points, ~40KB)
//! - Tier 1: LeafStore (disk pages, LRU cache = 4096 slots ≈ 2MB)
//!
//! Crash recovery: covered by the main WAL (row-level insert/delete records).
//!
//! Total memory budget: ~2.5MB regardless of data volume.

mod leaf_store;
mod node;
mod pending;
mod persistence;
mod search;

use crate::types::{BoundingBox3D, Geometry, Point3D};
use crate::{Result, StorageError};
use leaf_store::LeafStore;
use pending::PendingBuffer;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub use node::{IndexedPoint3D, Octant};

/// Default LeafStore LRU cache capacity (4096 slots ~ 2MB)
const DEFAULT_LEAF_CACHE_CAPACITY: usize = 4096;

/// i-Octree configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IOctreeConfig {
    /// Maximum points per leaf before splitting (default: 32)
    pub bucket_size: usize,
    /// Minimum half-extent of any octant in meters (default: 0.01)
    pub min_extent: f64,
    /// Enable lossy down-sampling for dense point clouds (default: false)
    pub down_size: bool,
    /// Persistence directory
    pub data_dir: Option<PathBuf>,
    /// LeafStore LRU cache capacity in slots (default: 4096 ~ 2MB).
    /// Reduce for memory-constrained edge devices (e.g. 1024 ~ 512KB).
    pub leaf_cache_capacity: Option<usize>,
}

impl Default for IOctreeConfig {
    fn default() -> Self {
        Self {
            bucket_size: 32,
            min_extent: 0.01,
            down_size: false,
            data_dir: None,
            leaf_cache_capacity: None,
        }
    }
}

impl IOctreeConfig {
    /// Return the effective leaf cache capacity
    pub fn cache_capacity(&self) -> usize {
        self.leaf_cache_capacity.unwrap_or(DEFAULT_LEAF_CACHE_CAPACITY)
    }
}

/// i-Octree spatial index: disk-first with bounded memory
pub struct IOctreeIndex {
    root: Octant,
    config: IOctreeConfig,
    size: usize,
    world_bounds: BoundingBox3D,
    name: String,
    /// Tier 0: bounded insert/delete buffer (reserved for future batch optimization)
    #[allow(dead_code)]
    pending: PendingBuffer,
    /// Tier 1: disk-backed leaf storage with LRU cache
    leaf_store: LeafStore,
}

impl IOctreeIndex {
    /// Create a new i-Octree with given config
    pub fn new(config: IOctreeConfig, name: String) -> Self {
        let world_bounds = BoundingBox3D::new(-500.0, -500.0, -500.0, 500.0, 500.0, 500.0);
        let center = world_bounds.center().to_f32();
        let extent = world_bounds.extent() as f32;

        // data_dir may point to a file (ioctree.bin) or directory; use parent for LeafStore/WAL
        let work_dir = config.data_dir.as_ref()
            .map(|p| {
                if p.extension().map(|e| e == "bin").unwrap_or(false) {
                    p.parent().unwrap_or(p).to_path_buf()
                } else {
                    p.clone()
                }
            })
            .unwrap_or_else(|| std::env::temp_dir().join(format!("motedb_ioctree_{}", name)));

        let leaf_store = LeafStore::open(&work_dir, config.cache_capacity()).expect("Failed to create LeafStore");
        let root_leaf_id = leaf_store.create_leaf(vec![]).expect("Failed to create root leaf");

        Self {
            root: Octant::new_leaf(center, extent, root_leaf_id),
            config,
            size: 0,
            world_bounds,
            name,
            pending: PendingBuffer::new(),
            leaf_store,
        }
    }

    /// Create with default config
    pub fn new_default(name: String) -> Self {
        Self::new(IOctreeConfig::default(), name)
    }

    /// Insert a 3D point into the index
    pub fn insert(&mut self, row_id: u64, geometry: &Geometry) -> Result<()> {
        let owned;
        let point = match geometry {
            Geometry::Point3D(p) => p,
            Geometry::Point(p) => {
                owned = Point3D::new(p.x as f64, p.y as f64, 0.0);
                &owned
            }
            _ => return Err(StorageError::InvalidData("i-Octree only accepts point geometry".into())),
        };

        let indexed = IndexedPoint3D::from_point3d(point, row_id);

        // Expand world bounds
        self.world_bounds.expand(point);

        // Expand root upward if point outside bounds
        let p = [point.x as f32, point.y as f32, point.z as f32];
        while !self.root_contains(&p) {
            self.expand_root();
        }

        // Direct insert into tree (data goes to LeafStore with bounded LRU cache)
        self.insert_into_tree(indexed)?;
        self.size += 1;
        Ok(())
    }

    /// Insert a point directly into the octree structure
    fn insert_into_tree(&mut self, point: IndexedPoint3D) -> Result<()> {
        let bucket_size = self.config.bucket_size;
        let min_extent = self.config.min_extent as f32;
        tree_insert(&self.leaf_store, &mut self.root, point, bucket_size, min_extent)
    }

    /// Batch insert multiple 3D points with Morton-code sorting for locality.
    ///
    /// Sorts points by Z-order curve before inserting, which fills leaves
    /// sequentially and reduces tree splits significantly compared to
    /// random-order insertion.
    pub fn batch_insert(&mut self, items: Vec<(u64, Geometry)>) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        if items.len() < 64 {
            for (row_id, geom) in items {
                self.insert(row_id, &geom)?;
            }
            return Ok(());
        }

        // Convert to IndexedPoint3D, expanding bounds in one pass
        let mut points: Vec<IndexedPoint3D> = Vec::with_capacity(items.len());
        for (row_id, geom) in &items {
            match geom {
                Geometry::Point3D(p) => {
                    self.world_bounds.expand(p);
                    points.push(IndexedPoint3D::from_point3d(p, *row_id));
                }
                Geometry::Point(p) => {
                    let p3 = Point3D::new(p.x as f64, p.y as f64, 0.0);
                    self.world_bounds.expand(&p3);
                    points.push(IndexedPoint3D::from_point3d(&p3, *row_id));
                }
                _ => {}
            }
        }

        // Expand root once to cover all points
        let max_coord = points.iter().map(|p| p.x.max(p.y).max(p.z)).fold(0.0f32, f32::max);
        let min_coord = points.iter().map(|p| p.x.min(p.y).min(p.z)).fold(0.0f32, f32::min);
        let abs_max = max_coord.abs().max(min_coord.abs()) + 1.0;
        while !self.root_contains(&[abs_max, abs_max, abs_max]) {
            self.expand_root();
        }

        // Sort by Morton code for spatial locality
        let bounds_min = [
            self.world_bounds.min_x as f32,
            self.world_bounds.min_y as f32,
            self.world_bounds.min_z as f32,
        ];
        let bounds_max = [
            (self.world_bounds.max_x - self.world_bounds.min_x) as f32,
            (self.world_bounds.max_y - self.world_bounds.min_y) as f32,
            (self.world_bounds.max_z - self.world_bounds.min_z) as f32,
        ];
        points.sort_by_key(|p| morton_encode_3d(p, &bounds_min, &bounds_max));

        // Insert in sorted order (sequential leaf fills, fewer splits)
        for point in points {
            self.insert_into_tree(point)?;
            self.size += 1;
        }
        Ok(())
    }

    /// Delete a point by row_id
    pub fn delete(&mut self, row_id: u64) -> bool {
        let removed = tree_delete(&self.leaf_store, &mut self.root, row_id);
        if removed {
            self.size = self.size.saturating_sub(1);
        }
        removed
    }

    /// Box-wise delete: remove all points within a 3D bounding box
    pub fn box_delete(&mut self, bbox: &BoundingBox3D) -> usize {
        let min = [bbox.min_x as f32, bbox.min_y as f32, bbox.min_z as f32];
        let max = [bbox.max_x as f32, bbox.max_y as f32, bbox.max_z as f32];
        let removed = tree_box_delete(&self.leaf_store, &mut self.root, &min, &max);
        self.size = self.size.saturating_sub(removed);
        removed
    }

    /// Range query: find all points within a 3D bounding box
    pub fn range_query(&self, bbox: &BoundingBox3D) -> Vec<u64> {
        let min = [bbox.min_x as f32, bbox.min_y as f32, bbox.min_z as f32];
        let max = [bbox.max_x as f32, bbox.max_y as f32, bbox.max_z as f32];
        search::range_search(&self.root, &min, &max, &self.leaf_store)
    }

    /// KNN query: find k nearest neighbors
    pub fn knn_query(&self, point: &Point3D, k: usize) -> Vec<(u64, f64)> {
        let query = [point.x as f32, point.y as f32, point.z as f32];
        search::knn_search(&self.root, &query, k, &self.leaf_store)
    }

    /// Radius search: find all points within a given radius
    pub fn radius_search(&self, center: &Point3D, radius: f64) -> Vec<(u64, f64)> {
        let c = [center.x as f32, center.y as f32, center.z as f32];
        search::radius_search(&self.root, &c, radius as f32, &self.leaf_store)
    }

    /// Number of indexed points
    pub fn len(&self) -> usize {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Estimated memory usage (tree structure only, not including leaf data)
    pub fn memory_usage(&self) -> usize {
        self.root.memory_usage() + std::mem::size_of::<Self>()
    }

    /// Save to disk
    pub fn save(&self, path: &std::path::Path) -> Result<()> {
        persistence::save(self, path)
    }

    /// Load from disk
    pub fn load(path: &std::path::Path, config: IOctreeConfig, name: String) -> Result<Self> {
        persistence::load(path, config, name)
    }

    /// Load from disk (path-only convenience wrapper)
    pub fn load_from_path(path: &std::path::Path) -> Result<Self> {
        let config = IOctreeConfig::default();
        let name = String::new();
        persistence::load(path, config, name)
    }

    /// Flush index to disk
    pub fn flush(&mut self) -> Result<()> {
        // Flush leaf store
        self.leaf_store.flush()?;

        // Save tree structure
        if let Some(ref path) = self.config.data_dir {
            let save_path = if path.extension().map(|e| e == "bin").unwrap_or(false) {
                path.clone()
            } else {
                path.join("ioctree.bin")
            };
            self.save(&save_path)?;
        }
        Ok(())
    }

    /// Get the index name
    pub fn name(&self) -> &str {
        &self.name
    }

    fn root_contains(&self, p: &[f32; 3]) -> bool {
        let (center, extent) = match &self.root {
            Octant::Inner { center, extent, .. } => (center, extent),
            Octant::Leaf { center, extent, .. } => (center, extent),
        };
        let e = *extent;
        p[0] >= center[0] - e && p[0] <= center[0] + e
            && p[1] >= center[1] - e && p[1] <= center[1] + e
            && p[2] >= center[2] - e && p[2] <= center[2] + e
    }

    fn expand_root(&mut self) {
        let (center, extent) = match &self.root {
            Octant::Inner { center, extent, .. } => (*center, *extent * 2.0),
            Octant::Leaf { center, extent, .. } => (*center, *extent * 2.0),
        };
        let old_root = std::mem::replace(&mut self.root, Octant::new_inner(center, extent));
        if let Octant::Inner { ref mut children, .. } = self.root {
            let code = node::octant_code(&center, &center);
            children[code] = Some(Box::new(old_root));
        }
        self.root.recount_size();
    }
}

// === Free functions for tree operations (avoids borrow checker issues) ===

fn tree_insert(store: &LeafStore, octant: &mut Octant, point: IndexedPoint3D, bucket_size: usize, min_extent: f32) -> Result<()> {
    match octant {
        Octant::Leaf { center: _, extent, leaf_id, point_count } => {
            store.add_point(*leaf_id, point)?;
            *point_count = store.point_count(*leaf_id)? as u32;

            if *point_count as usize > bucket_size && *extent > 2.0 * min_extent {
                split_leaf(store, octant)?;
            }
        }
        Octant::Inner { center, extent, children, size } => {
            *size += 1;
            let code = node::octant_code(center, &point.as_array());
            let child_ctr = node::child_center(center, *extent, code);

            if children[code].is_none() {
                let new_leaf_id = store.create_leaf(vec![])?;
                let child_ext = *extent / 2.0;
                children[code] = Some(Box::new(Octant::new_leaf(child_ctr, child_ext, new_leaf_id)));
            }
            if let Some(ref mut child) = children[code] {
                tree_insert(store, child, point, bucket_size, min_extent)?;
            }
        }
    }
    Ok(())
}

fn split_leaf(store: &LeafStore, octant: &mut Octant) -> Result<()> {
    let (center, extent, old_leaf_id) = match octant {
        Octant::Leaf { center, extent, leaf_id, .. } => (*center, *extent, *leaf_id),
        _ => unreachable!(),
    };

    let old_points = store.get_points(old_leaf_id)?;
    let child_extent = extent / 2.0;
    *octant = Octant::new_inner(center, extent);

    if let Octant::Inner { children, size, .. } = octant {
        for point in old_points {
            *size += 1;
            let code = node::octant_code(&center, &point.as_array());
            if children[code].is_none() {
                let child_ctr = node::child_center(&center, extent, code);
                let new_leaf_id = store.create_leaf(vec![])?;
                children[code] = Some(Box::new(Octant::new_leaf(child_ctr, child_extent, new_leaf_id)));
            }
            if let Some(ref mut child) = children[code] {
                match child.as_mut() {
                    Octant::Leaf { leaf_id, point_count, .. } => {
                        store.add_point(*leaf_id, point)?;
                        *point_count = store.point_count(*leaf_id)? as u32;
                    }
                    _ => {}
                }
            }
        }
    }

    store.free_leaf(old_leaf_id)?;
    Ok(())
}

fn tree_delete(store: &LeafStore, octant: &mut Octant, row_id: u64) -> bool {
    match octant {
        Octant::Leaf { leaf_id, point_count, .. } => {
            match store.remove_point(*leaf_id, row_id) {
                Ok(true) => {
                    *point_count = store.point_count(*leaf_id).unwrap_or(0) as u32;
                    true
                }
                _ => false,
            }
        }
        Octant::Inner { children, size, .. } => {
            for child in children.iter_mut() {
                if let Some(ref mut c) = child {
                    if tree_delete(store, c, row_id) {
                        *size = size.saturating_sub(1);
                        if let Some(ref c2) = child {
                            if c2.size() == 0 {
                                if let Some(lid) = c2.leaf_id() {
                                    let _ = store.free_leaf(lid);
                                }
                                *child = None;
                            }
                        }
                        return true;
                    }
                }
            }
            false
        }
    }
}

fn tree_box_delete(store: &LeafStore, octant: &mut Octant, min: &[f32; 3], max: &[f32; 3]) -> usize {
    match octant {
        Octant::Leaf { center, extent, leaf_id, point_count } => {
            if !node::overlaps(center, *extent, min, max) {
                return 0;
            }
            if node::contains_box(center, *extent, min, max) {
                let count = *point_count as usize;
                let _ = store.clear_leaf(*leaf_id);
                *point_count = 0;
                return count;
            }
            let min = *min;
            let max = *max;
            match store.retain_points(*leaf_id, |p| {
                p.x < min[0] || p.x > max[0]
                    || p.y < min[1] || p.y > max[1]
                    || p.z < min[2] || p.z > max[2]
            }) {
                Ok(removed) => {
                    *point_count = store.point_count(*leaf_id).unwrap_or(0) as u32;
                    removed
                }
                Err(_) => 0,
            }
        }
        Octant::Inner { center, extent, children, size } => {
            if !node::overlaps(center, *extent, min, max) {
                return 0;
            }
            let mut total_removed = 0;
            for child in children.iter_mut() {
                if let Some(ref mut c) = child {
                    let removed = tree_box_delete(store, c, min, max);
                    total_removed += removed;
                    if c.size() == 0 {
                        if let Some(lid) = c.leaf_id() {
                            let _ = store.free_leaf(lid);
                        }
                        *child = None;
                    }
                }
            }
            *size = size.saturating_sub(total_removed);
            total_removed
        }
    }
}

/// Encode a 3D point into a 64-bit Morton code (Z-order curve key).
/// Normalizes coordinates to [0, 2^21) per axis and interleaves bits.
fn morton_encode_3d(p: &IndexedPoint3D, bounds_min: &[f32; 3], bounds_range: &[f32; 3]) -> u64 {
    let mut code: u64 = 0;
    for axis in 0..3 {
        let range = bounds_range[axis];
        let normalized = if range > 0.0 {
            ((p.as_array()[axis] - bounds_min[axis]) / range)
                .clamp(0.0, 1.0)
        } else {
            0.5
        };
        // 21 bits per axis → 63 bits total
        let v = (normalized * ((1u64 << 21) - 1) as f32) as u64;
        // Spread bits: bit i → bit (i * 3 + axis)
        let mut bits = v;
        let mut shift = 0u64;
        while bits != 0 {
            if bits & 1 != 0 {
                code |= 1u64 << (shift * 3 + axis as u64);
            }
            bits >>= 1;
            shift += 1;
        }
    }
    code
}

pub use node::child_center;
