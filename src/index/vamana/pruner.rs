//! Robust Prune algorithm for Vamana
//!
//! The pruning strategy maintains graph connectivity while limiting node degree.

use crate::types::RowId;
use std::cmp::Ordering;

/// Candidate neighbor with distance
#[derive(Debug, Clone)]
pub struct Candidate {
    pub id: RowId,
    pub distance: f32,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Reverse order for min-heap
        other.distance.partial_cmp(&self.distance)
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

/// Robust Prune algorithm
///
/// Prunes a set of candidates to at most R neighbors while maintaining diversity.
///
/// # Arguments
/// * `candidates` - List of candidate neighbors with distances
/// * `max_degree` - Maximum number of neighbors (R parameter)
/// * `alpha` - Diversity parameter (typically 1.2)
/// * `distance_fn` - Function to compute distance between two vectors
///
/// # Returns
/// Pruned list of at most max_degree neighbors
pub fn robust_prune<F>(
    candidates: Vec<Candidate>,
    max_degree: usize,
    alpha: f32,
    distance_fn: F,
) -> Vec<RowId>
where
    F: Fn(RowId, RowId) -> f32,
{
    if candidates.len() <= max_degree {
        return candidates.into_iter().map(|c| c.id).collect();
    }

    // Sort candidates by distance (ascending)
    let mut sorted_candidates: Vec<_> = candidates.into_iter().collect();
    sorted_candidates.sort_by(|a, b| {
        a.distance.partial_cmp(&b.distance).unwrap_or(Ordering::Equal)
    });

    let mut pruned = Vec::with_capacity(max_degree);
    
    for candidate in sorted_candidates {
        if pruned.len() >= max_degree {
            break;
        }

        // Check diversity constraint
        let mut should_add = true;
        
        for &selected_id in &pruned {
            let dist_to_selected = distance_fn(candidate.id, selected_id);
            
            // If candidate is too close to an already selected neighbor,
            // and the selected neighbor is closer to the target, skip this candidate
            if dist_to_selected < alpha * candidate.distance {
                should_add = false;
                break;
            }
        }

        if should_add {
            pruned.push(candidate.id);
        }
    }

    pruned
}

/// Simple pruning without diversity constraint
///
/// Just keeps the k nearest neighbors.
pub fn simple_prune(candidates: Vec<Candidate>, max_degree: usize) -> Vec<RowId> {
    let mut sorted: Vec<_> = candidates.into_iter().collect();
    sorted.sort_by(|a, b| {
        a.distance.partial_cmp(&b.distance).unwrap_or(Ordering::Equal)
    });
    
    sorted
        .into_iter()
        .take(max_degree)
        .map(|c| c.id)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_candidate_ordering() {
        let c1 = Candidate { id: 1, distance: 1.0 };
        let c2 = Candidate { id: 2, distance: 2.0 };
        
        // c1 should be "greater" (min-heap)
        assert!(c1 > c2);
    }

    #[test]
    fn test_simple_prune_basic() {
        let candidates = vec![
            Candidate { id: 1, distance: 1.0 },
            Candidate { id: 2, distance: 3.0 },
            Candidate { id: 3, distance: 2.0 },
            Candidate { id: 4, distance: 4.0 },
        ];

        let pruned = simple_prune(candidates, 2);
        
        assert_eq!(pruned.len(), 2);
        assert_eq!(pruned[0], 1); // Closest
        assert_eq!(pruned[1], 3); // Second closest
    }

    #[test]
    fn test_simple_prune_no_pruning_needed() {
        let candidates = vec![
            Candidate { id: 1, distance: 1.0 },
            Candidate { id: 2, distance: 2.0 },
        ];

        let pruned = simple_prune(candidates, 5);
        
        assert_eq!(pruned.len(), 2);
    }

    #[test]
    fn test_robust_prune_basic() {
        let candidates = vec![
            Candidate { id: 1, distance: 1.0 },
            Candidate { id: 2, distance: 2.0 },
            Candidate { id: 3, distance: 3.0 },
        ];

        // Mock distance function (returns constant)
        let dist_fn = |_a: RowId, _b: RowId| 10.0;

        let pruned = robust_prune(candidates, 2, 1.2, dist_fn);
        
        assert_eq!(pruned.len(), 2);
        assert_eq!(pruned[0], 1); // Closest should always be included
    }

    #[test]
    fn test_robust_prune_diversity() {
        let candidates = vec![
            Candidate { id: 1, distance: 1.0 },
            Candidate { id: 2, distance: 1.1 }, // Very close to id:1
            Candidate { id: 3, distance: 5.0 },
        ];

        // Distance function: id:1 and id:2 are very close
        let dist_fn = |a: RowId, b: RowId| {
            if (a == 1 && b == 2) || (a == 2 && b == 1) {
                0.5 // Close together
            } else {
                10.0 // Far apart
            }
        };

        let pruned = robust_prune(candidates, 2, 1.2, dist_fn);
        
        // With alpha=1.2, id:2 should be pruned because:
        // dist(2, 1) = 0.5 < 1.2 * 1.1 = 1.32
        assert!(pruned.contains(&1));
        assert!(pruned.contains(&3));
    }
}
