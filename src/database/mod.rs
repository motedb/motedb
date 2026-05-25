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

/// Check if the database is closed, return error if so.
/// Called at the entry point of all public operations.
macro_rules! ensure_open {
    ($self:expr) => {
        if $self.is_closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(crate::StorageError::InvalidData("Database is closed".into()));
        }
    };
}

pub mod core;
pub mod crud;
pub mod table;
pub mod helpers;
pub mod indexes;
pub mod persistence;
pub mod transaction;
pub mod mem_buffer;
pub mod index_metadata;
pub mod pk_cache;
pub mod timeseries;

// Re-export main types
pub use core::MoteDB;
pub use mem_buffer::{IndexMemBuffer, BufferStats};
pub use indexes::{QueryProfile, MemTableScanProfile};
pub use transaction::TransactionStats;
pub use index_metadata::{IndexRegistry, IndexMetadata, IndexType};
