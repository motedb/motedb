//! Search algorithms for i-Octree: KNN, Range, Radius
//!
//! Reads leaf data through LeafStore (disk-first with LRU cache).

use super::leaf_store::LeafStore;
use super::node::{min_dist_sq_to_octant, overlaps, Octant};
use std::collections::BinaryHeap;

/// KNN search using priority-ordered child traversal with pruning.
///
/// Uses a max-heap of size k where the heap top is the WORST (largest distance)
/// among the k closest candidates found so far. This serves as the pruning
/// threshold: points/octants further than the threshold can be skipped.
pub fn knn_search(root: &Octant, query: &[f32; 3], k: usize, store: &LeafStore) -> Vec<(u64, f64)> {
    if k == 0 {
        return Vec::new();
    }

    let mut result: BinaryHeap<(OrderedF32, u64)> = BinaryHeap::new();
    let mut stack: Vec<(&Octant, f32)> = vec![(root, 0.0)];

    while let Some((octant, min_dist)) = stack.pop() {
        // Prune: if we already have k results and this octant is further
        // than the worst (largest) among the k closest, skip it.
        if result.len() >= k {
            if let Some((OrderedF32(threshold), _)) = result.peek() {
                if min_dist >= *threshold {
                    continue;
                }
            }
        }

        match octant {
            Octant::Leaf { leaf_id, .. } => {
                if let Ok(points) = store.get_points(*leaf_id) {
                    for point in &points {
                        let dist_sq = point.distance_squared(query);
                        push_knn_result(&mut result, dist_sq, point.row_id, k);
                    }
                }
            }
            Octant::Inner { children, .. } => {
                let mut child_dist: Vec<(&Octant, f32)> = Vec::new();
                for child in children.iter().flatten() {
                    let d = min_dist_sq_to_octant(child.center(), child.extent(), query);
                    if result.len() >= k {
                        if let Some((OrderedF32(threshold), _)) = result.peek() {
                            if d >= *threshold {
                                continue;
                            }
                        }
                    }
                    child_dist.push((child, d));
                }
                // Sort descending by distance so that closest children
                // are pushed last and popped first (stack is LIFO).
                child_dist.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                stack.extend(child_dist);
            }
        }
    }

    // Max-heap → into_sorted_vec() returns ascending (smallest distance first).
    // This is the correct KNN order: closest point first.
    result
        .into_sorted_vec()
        .into_iter()
        .map(|(OrderedF32(d), id)| (id, d as f64))
        .collect()
}

/// Push a candidate point into the KNN result heap, maintaining exactly k entries.
/// The heap is a max-heap where the top is the WORST (largest distance) among
/// the k closest points found so far.
fn push_knn_result(
    result: &mut BinaryHeap<(OrderedF32, u64)>,
    dist_sq: f32,
    row_id: u64,
    k: usize,
) {
    if result.len() < k {
        result.push((OrderedF32(dist_sq), row_id));
    } else if let Some((OrderedF32(threshold), _)) = result.peek() {
        // threshold = worst (largest distance) among the k closest
        if dist_sq < *threshold {
            result.pop();  // remove the worst
            result.push((OrderedF32(dist_sq), row_id));  // insert the better
        }
    }
}

/// Range search: find all row IDs within a 3D bounding box
pub fn range_search(root: &Octant, min: &[f32; 3], max: &[f32; 3], store: &LeafStore) -> Vec<u64> {
    let mut results = Vec::new();
    range_search_recursive(root, min, max, store, &mut results);
    results
}

fn range_search_recursive(octant: &Octant, min: &[f32; 3], max: &[f32; 3], store: &LeafStore, results: &mut Vec<u64>) {
    match octant {
        Octant::Leaf { center, extent, leaf_id, .. } => {
            if octant_inside_query(center, *extent, min, max) {
                if let Ok(points) = store.get_points(*leaf_id) {
                    results.extend(points.iter().map(|p| p.row_id));
                }
            } else if overlaps(center, *extent, min, max) {
                if let Ok(points) = store.get_points(*leaf_id) {
                    for point in &points {
                        if point.x >= min[0] && point.x <= max[0]
                            && point.y >= min[1] && point.y <= max[1]
                            && point.z >= min[2] && point.z <= max[2]
                        {
                            results.push(point.row_id);
                        }
                    }
                }
            }
        }
        Octant::Inner { center, extent, children, .. } => {
            if !overlaps(center, *extent, min, max) {
                return;
            }
            if octant_inside_query(center, *extent, min, max) {
                collect_all_row_ids(octant, store, results);
                return;
            }
            for child in children.iter().flatten() {
                range_search_recursive(child, min, max, store, results);
            }
        }
    }
}

/// Radius search: find all points within distance `radius` of center
pub fn radius_search(root: &Octant, center: &[f32; 3], radius: f32, store: &LeafStore) -> Vec<(u64, f64)> {
    let mut results = Vec::new();
    let radius_sq = radius * radius;
    radius_search_recursive(root, center, radius_sq, store, &mut results);
    results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    results
}

fn radius_search_recursive(octant: &Octant, center: &[f32; 3], radius_sq: f32, store: &LeafStore, results: &mut Vec<(u64, f64)>) {
    match octant {
        Octant::Leaf { leaf_id, .. } => {
            if let Ok(points) = store.get_points(*leaf_id) {
                for point in &points {
                    let d_sq = point.distance_squared(center);
                    if d_sq <= radius_sq {
                        results.push((point.row_id, d_sq as f64));
                    }
                }
            }
        }
        Octant::Inner { children, .. } => {
            for child in children.iter().flatten() {
                let d = min_dist_sq_to_octant(child.center(), child.extent(), center);
                if d <= radius_sq {
                    radius_search_recursive(child, center, radius_sq, store, results);
                }
            }
        }
    }
}

fn octant_inside_query(center: &[f32; 3], extent: f32, min: &[f32; 3], max: &[f32; 3]) -> bool {
    center[0] - extent >= min[0] && center[0] + extent <= max[0]
        && center[1] - extent >= min[1] && center[1] + extent <= max[1]
        && center[2] - extent >= min[2] && center[2] + extent <= max[2]
}

fn collect_all_row_ids(octant: &Octant, store: &LeafStore, results: &mut Vec<u64>) {
    match octant {
        Octant::Leaf { leaf_id, .. } => {
            if let Ok(points) = store.get_points(*leaf_id) {
                results.extend(points.iter().map(|p| p.row_id));
            }
        }
        Octant::Inner { children, .. } => {
            for child in children.iter().flatten() {
                collect_all_row_ids(child, store, results);
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct OrderedF32(f32);

impl PartialEq for OrderedF32 {
    fn eq(&self, other: &Self) -> bool { self.0 == other.0 }
}

impl Eq for OrderedF32 {}

impl PartialOrd for OrderedF32 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedF32 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(std::cmp::Ordering::Equal)
    }
}
