//! Database Module - Modularized Architecture
//!
//! Refactored from 4,798-line monolithic database_legacy.rs
//!
//! # Module Structure
//! - `core`: MoteDB struct and create/open methods
//! - `crud`: Complete CRUD operations (insert, get, update, delete, scan)
//! - `table`: Table management (create/drop/list/schema)
//! - `helpers`: Batch index building methods
//! - `indexes`: Index operations (timestamp, vector, spatial, text, column)
//! - `persistence`: Flush and checkpoint operations
//! - `transaction`: MVCC transactions and savepoints
//! - `mem_buffer`: Universal MemBuffer for all indexes
//! - `index_metadata`: Index metadata management

pub mod core;
pub mod crud;
pub mod table;
pub mod helpers;
pub mod indexes;
pub mod persistence;
pub mod transaction;
pub mod mem_buffer;
pub mod index_metadata;

// Re-export main types
pub use core::{MoteDB, DatabaseStats, VectorIndexStats, SpatialIndexStats};
pub use mem_buffer::{IndexMemBuffer, BufferStats};
pub use indexes::{QueryProfile, MemTableScanProfile};
pub use transaction::TransactionStats;
pub use index_metadata::{IndexRegistry, IndexMetadata, IndexType};
