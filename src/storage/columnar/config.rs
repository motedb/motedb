//! Columnar store configuration.

use serde::{Deserialize, Serialize};

/// Configuration for the columnar segment store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnarConfig {
    /// Maximum rows per write buffer before flushing to segment.
    pub buffer_row_capacity: usize,
    /// Maximum byte size of write buffer before flushing.
    pub buffer_byte_capacity: usize,
    /// Target row count per segment file (used for merge decisions).
    pub segment_target_rows: usize,
    /// Whether to enable background segment merging.
    pub enable_merge: bool,
    /// Merge when a table has >= this many small segments.
    pub merge_threshold_segments: usize,
    /// Whether to write per-column statistics (zone maps) to segments.
    pub enable_column_stats: bool,
    /// Whether to build bloom filters for Text columns in segments.
    pub enable_bloom_filters: bool,
    /// Whether to sort rows by timestamp before flushing.
    pub enable_timestamp_sort: bool,
    /// Bits per key for bloom filters (default 10 = ~1% FPR).
    pub bloom_filter_bits_per_key: usize,
}

impl Default for ColumnarConfig {
    fn default() -> Self {
        Self {
            buffer_row_capacity: 8192,
            buffer_byte_capacity: 4 * 1024 * 1024, // 4MB
            segment_target_rows: 100_000,
            enable_merge: true,
            merge_threshold_segments: 8,
            enable_column_stats: true,
            enable_bloom_filters: true,
            enable_timestamp_sort: true,
            bloom_filter_bits_per_key: 10,
        }
    }
}

impl ColumnarConfig {
    /// Preset for robotics (IMU, motor controllers).
    /// Smaller buffers for lower latency, frequent flushes.
    pub fn for_robotics() -> Self {
        Self {
            buffer_row_capacity: 4096,
            buffer_byte_capacity: 2 * 1024 * 1024, // 2MB
            segment_target_rows: 50_000,
            enable_merge: true,
            merge_threshold_segments: 6,
            enable_column_stats: true,
            enable_bloom_filters: true,
            enable_timestamp_sort: true,
            bloom_filter_bits_per_key: 10,
        }
    }

    /// Preset for edge/embedded devices.
    /// Minimal memory footprint.
    pub fn for_edge() -> Self {
        Self {
            buffer_row_capacity: 2048,
            buffer_byte_capacity: 1024 * 1024, // 1MB
            segment_target_rows: 30_000,
            enable_merge: false, // no background threads on edge
            merge_threshold_segments: 16,
            enable_column_stats: true,
            enable_bloom_filters: false, // save memory on edge
            enable_timestamp_sort: true,
            bloom_filter_bits_per_key: 8,
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = ColumnarConfig::default();
        assert_eq!(config.buffer_row_capacity, 8192);
        assert_eq!(config.buffer_byte_capacity, 4 * 1024 * 1024);
        assert_eq!(config.segment_target_rows, 100_000);
        assert!(config.enable_merge);
    }

    #[test]
    fn test_config_robotics() {
        let config = ColumnarConfig::for_robotics();
        assert_eq!(config.buffer_row_capacity, 4096);
        assert_eq!(config.buffer_byte_capacity, 2 * 1024 * 1024);
        assert_eq!(config.segment_target_rows, 50_000);
    }

    #[test]
    fn test_config_edge() {
        let config = ColumnarConfig::for_edge();
        assert_eq!(config.buffer_row_capacity, 2048);
        assert!(!config.enable_merge);
    }
}
