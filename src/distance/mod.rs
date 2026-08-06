//! Distance metrics for vector similarity computation
//!
//! Optimized distance functions for embedded environments.

pub mod cosine;
pub mod euclidean;

pub use cosine::{cosine_distance, cosine_similarity, dot_product};
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

    #[test]
    fn test_dot_product() {
        // 正交向量 dot = 0
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        assert!(dot_product(&a, &b).abs() < 1e-6);

        // 相同向量 dot = 模长平方
        let a = vec![3.0, 4.0];
        assert!((dot_product(&a, &a) - 25.0).abs() < 1e-5);

        // 一般情况 [1,2,3]·[4,5,6] = 4+10+18 = 32
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        assert!((dot_product(&a, &b) - 32.0).abs() < 1e-5);

        // 大向量（触发 SIMD 主循环 + remainder）
        let a: Vec<f32> = (0..100).map(|i| i as f32).collect();
        let b: Vec<f32> = (0..100).map(|i| (i + 1) as f32).collect();
        let expected: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        assert!((dot_product(&a, &b) - expected).abs() < 1e-2);
    }
}
