//! Octant node implementation for i-Octree
//!
//! Leaf nodes hold a `leaf_id` referencing data in the LeafStore,
//! keeping memory bounded regardless of data volume.

use serde::{Deserialize, Serialize};

/// Compact 3D point with associated row ID (f32 for memory savings)
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct IndexedPoint3D {
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub row_id: u64,
}

impl IndexedPoint3D {
    pub fn from_point3d(p: &crate::types::Point3D, row_id: u64) -> Self {
        Self { x: p.x as f32, y: p.y as f32, z: p.z as f32, row_id }
    }

    pub fn distance_squared(&self, other: &[f32; 3]) -> f32 {
        let dx = self.x - other[0];
        let dy = self.y - other[1];
        let dz = self.z - other[2];
        dx * dx + dy * dy + dz * dz
    }

    pub fn as_array(&self) -> [f32; 3] {
        [self.x, self.y, self.z]
    }
}

/// Octant node: inner node with 8 children, or leaf with disk-backed data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Octant {
    Inner {
        center: [f32; 3],
        extent: f32,
        children: Box<[Option<Box<Octant>>; 8]>,
        size: usize,
    },
    Leaf {
        center: [f32; 3],
        extent: f32,
        /// Leaf ID referencing data in LeafStore
        leaf_id: u64,
        /// Cached point count (kept in sync with LeafStore)
        point_count: u32,
    },
}

/// Compute which child octant a point belongs to (Morton code)
pub fn octant_code(center: &[f32; 3], point: &[f32; 3]) -> usize {
    let mut code = 0u8;
    if point[0] >= center[0] { code |= 1; }
    if point[1] >= center[1] { code |= 2; }
    if point[2] >= center[2] { code |= 4; }
    code as usize
}

/// Compute child center given parent center, extent, and octant code
pub fn child_center(parent_center: &[f32; 3], parent_extent: f32, code: usize) -> [f32; 3] {
    let half = parent_extent / 2.0;
    let mut center = *parent_center;
    if code & 1 != 0 { center[0] += half; } else { center[0] -= half; }
    if code & 2 != 0 { center[1] += half; } else { center[1] -= half; }
    if code & 4 != 0 { center[2] += half; } else { center[2] -= half; }
    center
}

impl Octant {
    pub fn new_leaf(center: [f32; 3], extent: f32, leaf_id: u64) -> Self {
        Octant::Leaf { center, extent, leaf_id, point_count: 0 }
    }

    pub fn new_inner(center: [f32; 3], extent: f32) -> Self {
        Octant::Inner {
            center,
            extent,
            children: Box::new([None, None, None, None, None, None, None, None]),
            size: 0,
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Octant::Inner { size, .. } => *size,
            Octant::Leaf { point_count, .. } => *point_count as usize,
        }
    }

    pub fn extent(&self) -> f32 {
        match self {
            Octant::Inner { extent, .. } | Octant::Leaf { extent, .. } => *extent,
        }
    }

    pub fn center(&self) -> &[f32; 3] {
        match self {
            Octant::Inner { center, .. } | Octant::Leaf { center, .. } => center,
        }
    }

    pub fn leaf_id(&self) -> Option<u64> {
        match self {
            Octant::Leaf { leaf_id, .. } => Some(*leaf_id),
            _ => None,
        }
    }

    /// Recount total size from children
    pub fn recount_size(&mut self) {
        if let Octant::Inner { children, size, .. } = self {
            *size = 0;
            for child in children.iter() {
                if let Some(ref c) = child {
                    *size += c.size();
                }
            }
        }
    }

    /// Estimate memory usage of the tree structure (not including leaf data in LeafStore)
    pub fn memory_usage(&self) -> usize {
        match self {
            Octant::Leaf { .. } => std::mem::size_of::<Octant>(),
            Octant::Inner { children, .. } => {
                let mut total = std::mem::size_of::<Octant>();
                for child in children.iter() {
                    if let Some(ref c) = child {
                        total += c.memory_usage();
                    }
                }
                total
            }
        }
    }
}

/// Check if an octant (center + extent) overlaps with a 3D box [min, max]
pub fn overlaps(center: &[f32; 3], extent: f32, min: &[f32; 3], max: &[f32; 3]) -> bool {
    center[0] + extent >= min[0] && center[0] - extent <= max[0]
        && center[1] + extent >= min[1] && center[1] - extent <= max[1]
        && center[2] + extent >= min[2] && center[2] - extent <= max[2]
}

/// Check if an octant fully contains a 3D box [min, max]
pub fn contains_box(center: &[f32; 3], extent: f32, min: &[f32; 3], max: &[f32; 3]) -> bool {
    center[0] - extent <= min[0] && center[0] + extent >= max[0]
        && center[1] - extent <= min[1] && center[1] + extent >= max[1]
        && center[2] - extent <= min[2] && center[2] + extent >= max[2]
}

/// Minimum squared distance from a point to an octant's boundary (0 if inside)
pub fn min_dist_sq_to_octant(center: &[f32; 3], extent: f32, point: &[f32; 3]) -> f32 {
    let mut dist_sq = 0.0f32;
    for i in 0..3 {
        let lo = center[i] - extent;
        let hi = center[i] + extent;
        if point[i] < lo {
            dist_sq += (point[i] - lo).powi(2);
        } else if point[i] > hi {
            dist_sq += (point[i] - hi).powi(2);
        }
    }
    dist_sq
}
