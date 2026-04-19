//! Segment manager: tracks segment files, handles pruning, column projection, and TTL GC.

use super::segment::{raw_bytes_compare_bytes, ColumnBlock, SegmentMetadata, SegmentReader};
use crate::storage::lsm::BloomFilter;
use crate::types::Value;
use crate::{Result, StorageError};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Name of the merge manifest file used for crash-safe segment replacement.
const MERGE_MANIFEST_NAME: &str = "merge_manifest.json";

/// Manifest tracking an in-progress merge for crash recovery.
#[derive(Serialize, Deserialize)]
struct MergeManifest {
    /// Path(s) of the new merged segment(s).
    new: Vec<String>,
    /// Paths of the old segments to be deleted.
    old: Vec<String>,
}

/// Maximum number of segment readers to cache.
const READER_CACHE_SIZE: usize = 32;

/// A condition for column-based segment pruning.
#[derive(Debug, Clone)]
pub enum ColumnCondition {
    /// Column equals a specific value.
    Equals { column_idx: usize, value: Value },
    /// Column value is within [low, high] range (inclusive).
    Range { column_idx: usize, low: Value, high: Value },
}

/// Manages all segment files for a single table.
pub struct SegmentManager {
    table_id: u32,
    directory: PathBuf,
    segments: RwLock<Vec<Arc<SegmentMetadata>>>,
    /// LRU-ish cache: path → opened reader.
    reader_cache: RwLock<HashMap<PathBuf, Arc<SegmentReader>>>,
}

impl SegmentManager {
    /// Open a segment manager, scanning the directory for existing .mcdb files.
    ///
    /// Also recovers from interrupted merge operations by checking for a
    /// `merge_manifest.json` left behind by a crash during `replace_segments()`.
    pub fn open(directory: &Path, table_id: u32) -> Result<Self> {
        std::fs::create_dir_all(directory)
            .map_err(StorageError::Io)?;

        // Recover from interrupted merges BEFORE scanning for segments
        Self::recover_merge_manifest(directory);

        let mut segments = Vec::new();

        // Scan for existing segment files
        if let Ok(entries) = std::fs::read_dir(directory) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().map_or(false, |e| e == "mcdb") {
                    match SegmentReader::open(&path) {
                        Ok(reader) => {
                            segments.push(Arc::new(reader.metadata()));
                        }
                        Err(e) => {
                            // Log but don't fail — corrupted segments can be recovered from WAL
                            eprintln!("[WARN] Failed to open segment {:?}: {}", path, e);
                        }
                    }
                }
            }
        }

        // Sort by min_timestamp for ordered scanning
        segments.sort_by_key(|s| s.min_timestamp);

        Ok(Self {
            table_id,
            directory: directory.to_path_buf(),
            segments: RwLock::new(segments),
            reader_cache: RwLock::new(HashMap::new()),
        })
    }

    /// Check for a `merge_manifest.json` and resume or clean up an interrupted merge.
    ///
    /// - If the new file exists AND old files exist: delete old files (resume merge).
    /// - If the new file is missing: the merge was incomplete; just delete the manifest.
    fn recover_merge_manifest(directory: &Path) {
        let manifest_path = directory.join(MERGE_MANIFEST_NAME);
        if !manifest_path.exists() {
            return;
        }

        let data = match std::fs::read_to_string(&manifest_path) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("[WARN] Failed to read merge manifest {:?}: {}", manifest_path, e);
                let _ = std::fs::remove_file(&manifest_path);
                return;
            }
        };

        let manifest: MergeManifest = match serde_json::from_str(&data) {
            Ok(m) => m,
            Err(e) => {
                eprintln!("[WARN] Failed to parse merge manifest: {}", e);
                let _ = std::fs::remove_file(&manifest_path);
                return;
            }
        };

        // Check whether the new segment file was written
        let new_exists = manifest.new.iter().any(|p| Path::new(p).exists());

        if new_exists {
            // The new segment exists — clean up old segments that were meant to be replaced
            for old_path in &manifest.old {
                if Path::new(old_path).exists() {
                    if let Err(e) = std::fs::remove_file(Path::new(old_path)) {
                        eprintln!("[WARN] Failed to delete old segment {:?}: {}", old_path, e);
                    }
                }
            }
        }
        // In any case, remove the manifest — the merge is either complete or abandoned
        let _ = std::fs::remove_file(&manifest_path);
    }

    /// Register a newly written segment file.
    pub fn register_segment(&self, path: &Path) -> Result<()> {
        let reader = SegmentReader::open(path)?;
        let meta = Arc::new(reader.metadata());

        let mut segments = self.segments.write();
        segments.push(meta);
        // Keep sorted by min_timestamp
        segments.sort_by_key(|s| s.min_timestamp);

        Ok(())
    }

    /// Return segments whose time range overlaps [start_ts, end_ts].
    /// Uses binary search on sorted segment list for O(log n + k).
    pub fn prune_by_time(&self, start_ts: i64, end_ts: i64) -> Vec<Arc<SegmentMetadata>> {
        let segments = self.segments.read();
        if segments.is_empty() {
            return Vec::new();
        }

        // Binary search: find upper bound where min_timestamp > end_ts.
        // All segments before this point have min_timestamp <= end_ts.
        let upper = segments.partition_point(|s| s.min_timestamp <= end_ts);

        // In [0..upper), filter by max_timestamp >= start_ts
        segments[..upper]
            .iter()
            .filter(|s| s.max_timestamp >= start_ts)
            .cloned()
            .collect()
    }

    /// Return segments matching time range AND column conditions.
    /// Applies zone map pruning and bloom filter checks.
    pub fn prune_by_conditions(
        &self,
        start_ts: i64,
        end_ts: i64,
        conditions: &[ColumnCondition],
    ) -> Vec<Arc<SegmentMetadata>> {
        let candidates = self.prune_by_time(start_ts, end_ts);
        if conditions.is_empty() {
            return candidates;
        }

        let mut result = Vec::with_capacity(candidates.len());
        for seg in &candidates {
            if self.segment_matches_conditions(seg, conditions) {
                result.push(seg.clone());
            }
        }
        result
    }

    /// Check if a segment's zone maps / bloom filters can match the given conditions.
    fn segment_matches_conditions(
        &self,
        segment: &SegmentMetadata,
        conditions: &[ColumnCondition],
    ) -> bool {
        // Load column statistics for this segment
        let stats = match self.get_reader(&segment.path) {
            Ok(reader) => match reader.read_statistics() {
                Ok(Some(s)) => s,
                _ => return true, // No stats → can't prune, assume match
            },
            Err(_) => return true,
        };

        for condition in conditions {
            match condition {
                ColumnCondition::Equals { column_idx, value } => {
                    let col_id = *column_idx as u16;
                    if let Some(stat) = stats.iter().find(|s| s.column_id == col_id) {
                        let query_bytes = super::segment::value_to_raw_bytes(value);

                        // Check: value must be >= min AND <= max
                        if raw_bytes_compare_bytes(&query_bytes, &stat.min_value_raw) == std::cmp::Ordering::Less
                            || raw_bytes_compare_bytes(&query_bytes, &stat.max_value_raw) == std::cmp::Ordering::Greater
                        {
                            return false; // Value outside zone map range
                        }

                        // For Text columns, also check bloom filter
                        if let Value::Text(ref text_val) = value {
                            if segment.has_bloom_filters {
                                match self.may_contain_text(segment, *column_idx, text_val) {
                                    Ok(false) => return false, // Bloom filter says no
                                    _ => {} // Can't determine, assume match
                                }
                            }
                        }
                    }
                }
                ColumnCondition::Range { column_idx, low, high } => {
                    let col_id = *column_idx as u16;
                    if let Some(stat) = stats.iter().find(|s| s.column_id == col_id) {
                        let low_bytes = super::segment::value_to_raw_bytes(low);
                        let high_bytes = super::segment::value_to_raw_bytes(high);

                        // Range [low, high] overlaps with segment [min, max] iff
                        // high >= min AND low <= max
                        if raw_bytes_compare_bytes(&high_bytes, &stat.min_value_raw) == std::cmp::Ordering::Less
                            || raw_bytes_compare_bytes(&low_bytes, &stat.max_value_raw) == std::cmp::Ordering::Greater
                        {
                            return false;
                        }
                    }
                }
            }
        }
        true
    }

    /// Check if a segment's bloom filter may contain a text value for the given column.
    pub fn may_contain_text(&self, segment: &SegmentMetadata, column_idx: usize, value: &str) -> Result<bool> {
        let reader = self.get_reader(&segment.path)?;
        let filters = reader.read_bloom_filters()?;
        match filters {
            Some(map) => {
                let col_id = column_idx as u16;
                match map.get(&col_id) {
                    Some(data) => {
                        let bloom = BloomFilter::from_bytes_full(data);
                        Ok(bloom.map_or(false, |b| b.may_contain(value.as_bytes())))
                    }
                    None => Ok(true), // No filter for this column → assume match
                }
            }
            None => Ok(true), // No bloom filters → assume match
        }
    }

    /// Return all segments.
    pub fn all_segments(&self) -> Vec<Arc<SegmentMetadata>> {
        self.segments.read().clone()
    }

    /// Read specified columns from a segment (column projection).
    pub fn read_columns(
        &self,
        segment: &SegmentMetadata,
        column_ids: &[u16],
    ) -> Result<Vec<ColumnBlock>> {
        let reader = self.get_reader(&segment.path)?;
        let mut blocks = Vec::with_capacity(column_ids.len());
        for &col_id in column_ids {
            blocks.push(reader.read_column(col_id)?);
        }
        Ok(blocks)
    }

    /// Read all columns from a segment.
    pub fn read_all_columns(&self, segment: &SegmentMetadata) -> Result<Vec<ColumnBlock>> {
        let reader = self.get_reader(&segment.path)?;
        reader.read_all_columns()
    }

    /// Delete segments with max_timestamp < cutoff_ts. Returns count deleted.
    pub fn delete_expired(&self, cutoff_ts: i64) -> Result<usize> {
        // Lock ordering: segments first, then reader_cache.
        // I/O deferred until after locks are released.
        let (expired, count) = {
            let mut segments = self.segments.write();
            let expired: Vec<Arc<SegmentMetadata>> = segments
                .iter()
                .filter(|s| s.max_timestamp < cutoff_ts)
                .cloned()
                .collect();
            let count = expired.len();
            segments.retain(|s| s.max_timestamp >= cutoff_ts);
            (expired, count)
        }; // segments write lock released here

        // Evict from reader cache
        for meta in &expired {
            self.reader_cache.write().remove(&meta.path);
        }

        // Delete files (I/O outside of locks)
        for meta in &expired {
            if meta.path.exists() {
                std::fs::remove_file(&meta.path).map_err(StorageError::Io)?;
            }
        }

        if count > 0 {
            debug_log!(
                "[Columnar] Table {}: deleted {} expired segments (cutoff={})",
                self.table_id, count, cutoff_ts
            );
        }

        Ok(count)
    }

    /// Delete all segments (used by DROP TABLE).
    pub fn delete_all(&self) -> Result<usize> {
        // Lock ordering: segments first, then reader_cache.
        let paths: Vec<PathBuf> = {
            let mut segments = self.segments.write();
            let paths: Vec<PathBuf> = segments.iter()
                .filter(|s| s.path.exists())
                .map(|s| s.path.clone())
                .collect();
            segments.clear();
            paths
        }; // segments write lock released

        self.reader_cache.write().clear();

        // Delete files (I/O outside of locks)
        for path in &paths {
            let _ = std::fs::remove_file(path);
        }

        Ok(paths.len())
    }

    /// Return segments sorted by row_count ascending (smallest first) for merge candidate selection.
    pub fn small_segments(&self, target_rows: usize) -> Vec<Arc<SegmentMetadata>> {
        let segments = self.segments.read();
        let mut small: Vec<Arc<SegmentMetadata>> = segments
            .iter()
            .filter(|s| (s.row_count as usize) < target_rows)
            .cloned()
            .collect();
        small.sort_by_key(|s| s.row_count);
        small
    }

    /// Replace old segments with a new merged segment.
    ///
    /// Uses a `merge_manifest.json` for crash safety:
    /// 1. Write manifest listing new + old paths, fsync it
    /// 2. Update in-memory state
    /// 3. Delete old files
    /// 4. Delete the manifest
    ///
    /// If a crash occurs between steps, startup recovery in `open()` will
    /// either resume the deletion or abandon the incomplete merge.
    pub fn replace_segments(
        &self,
        old_segments: &[Arc<SegmentMetadata>],
        new_segment_path: &Path,
    ) -> Result<()> {
        let old_paths: Vec<PathBuf> = old_segments.iter().map(|s| s.path.clone()).collect();

        // Open the new segment to get its metadata (I/O before any lock)
        let new_reader = SegmentReader::open(new_segment_path)?;
        let new_meta = Arc::new(new_reader.metadata());

        // Step 1: Write merge manifest and fsync it for crash safety
        let manifest_path = self.directory.join(MERGE_MANIFEST_NAME);
        let manifest = MergeManifest {
            new: vec![new_segment_path.to_string_lossy().into_owned()],
            old: old_paths.iter().map(|p| p.to_string_lossy().into_owned()).collect(),
        };
        {
            let json = serde_json::to_string_pretty(&manifest)
                .map_err(|e| StorageError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
            let mut file = std::fs::File::create(&manifest_path)
                .map_err(StorageError::Io)?;
            file.write_all(json.as_bytes())
                .map_err(StorageError::Io)?;
            file.sync_all()
                .map_err(StorageError::Io)?;
        }

        // Collect file deletions, perform I/O outside of locks
        let paths_to_delete: Vec<PathBuf> = old_paths.iter()
            .filter(|p| p.exists())
            .cloned()
            .collect();

        // Step 2: Lock ordering: always segments first, then reader_cache
        {
            let mut segments = self.segments.write();
            let old_path_set: std::collections::HashSet<PathBuf> = old_paths.iter().cloned().collect();
            segments.retain(|s| !old_path_set.contains(&s.path));
            segments.push(new_meta);
            segments.sort_by_key(|s| s.min_timestamp);
        }

        // Evict from reader cache (after segments lock released)
        for path in &old_paths {
            self.reader_cache.write().remove(path);
        }

        // Step 3: Delete old files (I/O after locks released)
        for path in &paths_to_delete {
            let _ = std::fs::remove_file(path);
        }

        // Step 4: Delete the merge manifest
        let _ = std::fs::remove_file(&manifest_path);

        Ok(())
    }

    /// Get the number of segments.
    pub fn segment_count(&self) -> usize {
        self.segments.read().len()
    }

    /// Get the table_id.
    pub fn table_id(&self) -> u32 {
        self.table_id
    }

    /// Get the directory path.
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Get or open a cached segment reader.
    fn get_reader(&self, path: &Path) -> Result<Arc<SegmentReader>> {
        // Check cache
        {
            let cache = self.reader_cache.read();
            if let Some(reader) = cache.get(path) {
                return Ok(reader.clone());
            }
        }

        // Open new reader
        let reader = Arc::new(SegmentReader::open(path)?);
        let mut cache = self.reader_cache.write();

        // Evict if cache is full
        if cache.len() >= READER_CACHE_SIZE {
            // Simple eviction: remove first entry
            if let Some(key) = cache.keys().next().cloned() {
                cache.remove(&key);
            }
        }

        cache.insert(path.to_path_buf(), reader.clone());
        Ok(reader)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::columnar::segment::{ColumnEncoding, SegmentBuilder};
    use tempfile::TempDir;

    fn write_test_segment(dir: &Path, table_id: u32, min_ts: i64, max_ts: i64) -> PathBuf {
        let path = dir.join(format!("seg_{}_{}.mcdb", min_ts, max_ts));
        let mut builder = SegmentBuilder::new(&path, table_id, 1).unwrap();
        let ts_data = super::super::gorilla::encode_timestamps(&[min_ts, max_ts]);
        builder.write_column(0, ColumnEncoding::GorillaTimestamp, &ts_data, 16, 0).unwrap();
        builder.finish(2, min_ts, max_ts, 0, 1).unwrap();
        path
    }

    #[test]
    fn test_segment_manager_open_empty() {
        let dir = TempDir::new().unwrap();
        let mgr = SegmentManager::open(dir.path(), 1).unwrap();
        assert_eq!(mgr.segment_count(), 0);
    }

    #[test]
    fn test_segment_manager_register_and_prune() {
        let dir = TempDir::new().unwrap();
        let sub = dir.path().join("1");
        let mgr = SegmentManager::open(&sub, 1).unwrap();

        // Write 3 segments: [0, 100], [200, 300], [400, 500]
        let p1 = write_test_segment(&sub, 1, 0, 100);
        let p2 = write_test_segment(&sub, 1, 200, 300);
        let p3 = write_test_segment(&sub, 1, 400, 500);

        mgr.register_segment(&p1).unwrap();
        mgr.register_segment(&p2).unwrap();
        mgr.register_segment(&p3).unwrap();
        assert_eq!(mgr.segment_count(), 3);

        // Query range [150, 250] — should match segment [200, 300] only
        let pruned = mgr.prune_by_time(150, 250);
        assert_eq!(pruned.len(), 1);
        assert_eq!(pruned[0].min_timestamp, 200);

        // Query range [0, 500] — should match all
        let all = mgr.prune_by_time(0, 500);
        assert_eq!(all.len(), 3);

        // Query range [50, 150] — should match [0, 100]
        let partial = mgr.prune_by_time(50, 150);
        assert_eq!(partial.len(), 1);
        assert_eq!(partial[0].min_timestamp, 0);
    }

    #[test]
    fn test_segment_manager_delete_expired() {
        let dir = TempDir::new().unwrap();
        let sub = dir.path().join("1");
        let mgr = SegmentManager::open(&sub, 1).unwrap();

        let p1 = write_test_segment(&sub, 1, 0, 100);
        let p2 = write_test_segment(&sub, 1, 200, 300);
        let p3 = write_test_segment(&sub, 1, 400, 500);

        mgr.register_segment(&p1).unwrap();
        mgr.register_segment(&p2).unwrap();
        mgr.register_segment(&p3).unwrap();

        // Delete segments with max_ts < 250
        let deleted = mgr.delete_expired(250).unwrap();
        assert_eq!(deleted, 1); // [0, 100] deleted
        assert_eq!(mgr.segment_count(), 2);

        // Remaining: [200, 300] and [400, 500]
        let all = mgr.all_segments();
        assert_eq!(all[0].min_timestamp, 200);
        assert_eq!(all[1].min_timestamp, 400);
    }

    #[test]
    fn test_segment_manager_read_columns() {
        let dir = TempDir::new().unwrap();
        let sub = dir.path().join("1");
        let mgr = SegmentManager::open(&sub, 1).unwrap();

        let path = write_test_segment(&sub, 1, 1000, 2000);
        mgr.register_segment(&path).unwrap();

        let seg = &mgr.all_segments()[0];
        let columns = mgr.read_columns(seg, &[0]).unwrap();
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].encoding, ColumnEncoding::GorillaTimestamp);
    }

    #[test]
    fn test_segment_manager_delete_all() {
        let dir = TempDir::new().unwrap();
        let sub = dir.path().join("1");
        let mgr = SegmentManager::open(&sub, 1).unwrap();

        let p1 = write_test_segment(&sub, 1, 0, 100);
        mgr.register_segment(&p1).unwrap();
        assert_eq!(mgr.segment_count(), 1);

        let deleted = mgr.delete_all().unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(mgr.segment_count(), 0);
    }
}
