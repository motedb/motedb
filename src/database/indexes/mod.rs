//! Index Operations Module
//!
//! Modular structure for different index types:
//! - timestamp: Timestamp range queries
//! - column: Column value indexes for WHERE optimization
//! - text: Full-text search with BM25 ranking
//! - vector: Vector similarity search with DiskANN
//! - ioctree: i-Octree 3D point cloud for embodied intelligence

pub mod timestamp;
pub mod column;
pub mod text;
pub mod vector;
pub mod ioctree;

// Re-export for convenience
pub use timestamp::{QueryProfile, MemTableScanProfile};
pub use vector::VectorIndexStats;
