//! Tensor (Vector) data type implementation

use serde::{Deserialize, Serialize};

/// Tensor data type for storing high-dimensional vectors
///
/// Stored as Float32 for compatibility with SQ8 quantization
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Tensor {
    /// Vector dimension
    dimension: usize,
    
    /// Data stored as Float32
    data: Vec<f32>,
}

impl Tensor {
    /// Create a new tensor from Float32 values
    pub fn new(values: Vec<f32>) -> Self {
        let dimension = values.len();
        Self { dimension, data: values }
    }

    /// Get dimension
    pub fn dimension(&self) -> usize {
        self.dimension
    }

    /// Get data as Float32 slice (zero-copy)
    pub fn as_f32(&self) -> &[f32] {
        &self.data
    }

    /// Convert to Float32 vec (for compatibility)
    pub fn to_f32(&self) -> Vec<f32> {
        self.data.clone()
    }

    /// Compute cosine similarity with another tensor
    pub fn cosine_similarity(&self, other: &Tensor) -> f32 {
        assert_eq!(self.dimension, other.dimension, "Dimension mismatch");
        
        let a = &self.data;
        let b = &other.data;
        
        let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        
        if norm_a == 0.0 || norm_b == 0.0 {
            0.0
        } else {
            dot / (norm_a * norm_b)
        }
    }

    /// Compute L2 distance with another tensor
    pub fn l2_distance(&self, other: &Tensor) -> f32 {
        assert_eq!(self.dimension, other.dimension, "Dimension mismatch");
        
        let a = &self.data;
        let b = &other.data;
        
        a.iter()
            .zip(b.iter())
            .map(|(x, y)| (x - y).powi(2))
            .sum::<f32>()
            .sqrt()
    }

    /// Memory size in bytes (Float32 storage)
    pub fn memory_size(&self) -> usize {
        self.dimension * std::mem::size_of::<f32>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tensor_creation() {
        let values = vec![1.0, 2.0, 3.0];
        let tensor = Tensor::new(values.clone());
        
        assert_eq!(tensor.dimension(), 3);
        
        let reconstructed = tensor.to_f32();
        for (a, b) in values.iter().zip(reconstructed.iter()) {
            assert!((a - b).abs() < 0.001);
        }
    }

    #[test]
    fn test_cosine_similarity() {
        let t1 = Tensor::new(vec![1.0, 0.0, 0.0]);
        let t2 = Tensor::new(vec![1.0, 0.0, 0.0]);
        let t3 = Tensor::new(vec![0.0, 1.0, 0.0]);
        
        assert!((t1.cosine_similarity(&t2) - 1.0).abs() < 0.01);
        assert!((t1.cosine_similarity(&t3) - 0.0).abs() < 0.01);
    }

    #[test]
    fn test_l2_distance() {
        let t1 = Tensor::new(vec![0.0, 0.0, 0.0]);
        let t2 = Tensor::new(vec![3.0, 4.0, 0.0]);
        
        assert!((t1.l2_distance(&t2) - 5.0).abs() < 0.001);
    }
}
