//! Index Operations Module
//!
//! Modular structure for different index types:
//! - timestamp: Timestamp range queries
//! - column: Column value indexes for WHERE optimization
//! - text: Full-text search with BM25 ranking
//! - spatial: Geospatial queries with hybrid grid+RTree
//! - vector: Vector similarity search with DiskANN

pub mod timestamp;
pub mod column;
pub mod text;
pub mod spatial;
pub mod vector;

// Re-export for convenience
pub use timestamp::{QueryProfile, MemTableScanProfile};
pub use spatial::SpatialIndexStats;
pub use vector::VectorIndexStats;
