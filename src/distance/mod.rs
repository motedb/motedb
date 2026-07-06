//! Distance metrics for vector similarity computation
//!
//! Optimized distance functions for embedded environments.

pub mod cosine;
pub mod euclidean;

pub use cosine::{cosine_distance, cosine_similarity};
pub use euclidean::euclidean_distance;

/// Distance metric trait
pub trait DistanceMetric: Send + Sync {
    /// Compute distance between two vectors
    fn distance(&self, a: &[f32], b: &[f32]) -> f32;
}

/// Euclidean distance metric
#[derive(Debug, Clone, Copy)]
pub struct Euclidean;

impl DistanceMetric for Euclidean {
    #[inline]
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        euclidean_distance(a, b)
    }
}

/// Cosine distance metric (1 - cosine_similarity)
#[derive(Debug, Clone, Copy)]
pub struct Cosine;

impl DistanceMetric for Cosine {
    #[inline]
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        cosine_distance(a, b)
    }
}

/// Monomorphized distance metric enum (zero-cost alternative to `Arc<dyn DistanceMetric>`)
///
/// Eliminates virtual dispatch overhead for inner-loop distance computations
/// in DiskANN/Vamana graph search (saves ~2-5ns per distance call on ARM).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceKind {
    Euclidean,
    Cosine,
}

impl DistanceKind {
    #[inline]
    pub fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            DistanceKind::Euclidean => euclidean_distance(a, b),
            DistanceKind::Cosine => cosine_distance(a, b),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_euclidean_metric() {
        let metric = Euclidean;
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        let dist = metric.distance(&a, &b);
        assert!((dist - 5.196152).abs() < 0.001);
    }

    #[test]
    fn test_cosine_metric() {
        let metric = Cosine;
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        let dist = metric.distance(&a, &b);
        assert!(dist < 0.01); // Same vector should have ~0 distance
    }
}
