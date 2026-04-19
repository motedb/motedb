//! Columnar Segment Store for time-series data.
//!
//! Stores TimeSeries table data in immutable columnar segment files with
//! Gorilla compression. Provides 10-12x compression vs bincode row storage,
//! O(1) TTL GC (delete whole segment files), and column projection.
//!
//! # Architecture
//! ```text
//! Write Path:
//!   ingest() → ColumnarWriteBuffer → flush → SegmentFile (.mcdb)
//!                                              ↓
//!                                       SegmentManager (catalog)
//!
//! Read Path:
//!   query_time_range() → prune segments → column projection → decode → Row
//!
//! TTL GC:
//!   gc_expired() → delete segment files with max_ts < cutoff (O(1))
//! ```

pub mod config;
pub mod gorilla;
pub mod column_encoding;
pub mod write_buffer;
pub mod segment;
pub mod segment_manager;
pub mod store;

pub use config::ColumnarConfig;
pub use store::ColumnarStore;
