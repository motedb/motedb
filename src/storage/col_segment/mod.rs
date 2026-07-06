//! Append-only multi-segment columnar store.
//!
//! Each table owns a `ColSegmentStore` holding an ordered list of immutable
//! `Segment`s (each backed by an existing `ColumnarSSTable`). Writes append
//! delta segments (O(1)); reads multi-way merge (O(1) memory); a background
//! compactor merges segments to bound the count.
//!
//! Naming: this is distinct from `storage::columnar::ColumnarStore` (the
//! time-series store). This module serves tables using the v0.3.0 columnar
//! SSTable format with multi-segment + compaction semantics.

mod manifest;
mod merge;
mod segment;
mod store;

pub use manifest::{Manifest, ManifestState};
pub use merge::MergeCursor;
pub use segment::Segment;
pub use store::ColSegmentStore;
