//! Storage layer implementation
//!
//! Manages physical data storage using LSM-Tree architecture
//! plus Columnar Segment Store for time-series data.

pub mod lsm;
pub mod manifest;
pub mod file_manager;
pub mod checksum;
pub mod columnar;
pub mod row_format;
pub mod col_segment;

pub use lsm::{LSMEngine, LSMConfig, MemTable, SSTable};
pub use manifest::{Manifest, FileMetadata, FileType};
pub use file_manager::{FileRefManager, FileHandle};
pub use checksum::{Checksum, ChecksumType, ChecksumError};
pub use columnar::ColumnarStore;
