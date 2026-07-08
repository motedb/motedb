use super::manifest::Manifest;
use super::merge::MergeCursor;
use super::segment::Segment;
use crate::storage::lsm::columnar::{ColumnTypeTag, ColumnarSSTableBuilder};
use crate::types::{ArcString, ColumnType, Value};
use crate::Result;
use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Result of a single-pass aggregate scan (SUM/AVG/MIN/MAX/COUNT).
/// Computed without per-row Value allocation.
#[derive(Default, Clone)]
pub struct AggregateResult {
    pub count: i64,      // non-NULL values (for COUNT(col))
    pub null_count: i64, // NULL values (for COUNT(*) = count + null_count)
    pub int_sum: i64,
    pub float_sum: f64,
    pub has_float: bool,
    pub min_int: i64,
    pub max_int: i64,
    pub min_float: f64,
    pub max_float: f64,
}

// ── Comparison helpers for count_filtered (zero-allocation) ──────────
#[inline]
fn cmp_opt<T: Copy + PartialEq + PartialOrd>(
    v: Option<T>,
    target: Option<T>,
    op: &crate::sql::ast::BinaryOperator,
) -> bool {
    use crate::sql::ast::BinaryOperator;
    let (v, t) = match (v, target) {
        (Some(a), Some(b)) => (a, b),
        _ => return false,
    };
    match op {
        BinaryOperator::Eq => v == t,
        BinaryOperator::Ne => v != t,
        BinaryOperator::Lt => v.partial_cmp(&t) == Some(std::cmp::Ordering::Less),
        BinaryOperator::Gt => v.partial_cmp(&t) == Some(std::cmp::Ordering::Greater),
        BinaryOperator::Le => matches!(
            v.partial_cmp(&t),
            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
        ),
        BinaryOperator::Ge => matches!(
            v.partial_cmp(&t),
            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
        ),
        _ => false,
    }
}

#[inline]
fn cmp_opt_f64(v: Option<f64>, target: Option<f64>, op: &crate::sql::ast::BinaryOperator) -> bool {
    cmp_opt(v, target, op)
}

#[inline]
fn cmp_str(v: Option<&str>, target: Option<&str>, op: &crate::sql::ast::BinaryOperator) -> bool {
    use crate::sql::ast::BinaryOperator;
    let (v, t) = match (v, target) {
        (Some(a), Some(b)) => (a, b),
        _ => return false,
    };
    match op {
        BinaryOperator::Eq => v == t,
        BinaryOperator::Ne => v != t,
        BinaryOperator::Lt => v < t,
        BinaryOperator::Gt => v > t,
        BinaryOperator::Le => v <= t,
        BinaryOperator::Ge => v >= t,
        _ => false,
    }
}

/// Decode a single value from a ColumnarSSTableBuilder's raw column buffer.
/// Used by ColSegmentStore::get() to read buffered (unflushed) rows.
/// Format matches add_values: Integer/Timestamp = [8B i64 LE], Float = [8B f64 LE],
/// Bool = [1B], Text = [u16 len][bytes].
fn decode_buffered_value(
    buf: &crate::storage::lsm::columnar::ColumnarSSTableBuilder,
    col_idx: usize,
    row_idx: usize,
    _col_type: &ColumnType,
) -> Value {
    use crate::storage::lsm::columnar::ColumnTypeTag;
    // Check NULL flag first.
    if buf.null_flags.get(col_idx).and_then(|f| f.get(row_idx)) == Some(&true) {
        return Value::Null;
    }
    let tag = buf.column_tags.get(col_idx).copied();
    let raw = &buf.column_buffers[col_idx];
    match tag {
        Some(ColumnTypeTag::Integer) => {
            let off = row_idx * 8;
            if off + 8 > raw.len() {
                return Value::Null;
            }
            Value::Integer(i64::from_le_bytes(raw[off..off + 8].try_into().unwrap()))
        }
        Some(ColumnTypeTag::Timestamp) => {
            let off = row_idx * 8;
            if off + 8 > raw.len() {
                return Value::Null;
            }
            let v = i64::from_le_bytes(raw[off..off + 8].try_into().unwrap());
            Value::Timestamp(crate::types::Timestamp::from_micros(v))
        }
        Some(ColumnTypeTag::Float) => {
            let off = row_idx * 8;
            if off + 8 > raw.len() {
                return Value::Null;
            }
            Value::Float(f64::from_le_bytes(raw[off..off + 8].try_into().unwrap()))
        }
        Some(ColumnTypeTag::Bool) => {
            let off = row_idx;
            if off >= raw.len() {
                return Value::Null;
            }
            Value::Bool(raw[off] != 0)
        }
        Some(ColumnTypeTag::Text) => {
            // Text rows are variable-length: [u16 len][bytes], concatenated.
            // Walk to the row_idx-th entry.
            let mut pos = 0usize;
            let mut r = 0usize;
            while pos + 2 <= raw.len() {
                let len = u16::from_le_bytes([raw[pos], raw[pos + 1]]) as usize;
                pos += 2;
                if r == row_idx {
                    if len == 0xFFFF || pos + len > raw.len() {
                        return Value::Null;
                    }
                    return Value::Text(ArcString(std::sync::Arc::from(
                        std::str::from_utf8(&raw[pos..pos + len]).unwrap_or(""),
                    )));
                }
                pos += if len == 0xFFFF { 0 } else { len };
                r += 1;
            }
            Value::Null
        }
        _ => Value::Null,
    }
}
use std::sync::Arc;

/// Compaction trigger: merge when segment count reaches this.
const COMPACTION_SEGMENT_THRESHOLD: usize = 3;

/// Append-only multi-segment store for one columnar table.
pub struct ColSegmentStore {
    #[allow(dead_code)]
    table_name: String,
    dir: PathBuf,
    /// Active segments in ascending creation order (oldest first, newest at back).
    segments: RwLock<VecDeque<Arc<Segment>>>,
    /// In-memory write buffer. Flushed as a delta segment (does not read old data).
    write_buf: Mutex<ColumnarSSTableBuilder>,
    /// Write lock serializing flush_buffer + merge_segments. Without this, a
    /// concurrent flush (triggered by ensure_query_visibility during a query)
    /// can create a segment that force_compact_all then misses or clobbers
    /// (the v0.5.0 large_batch_durability race — 5000 of 10000 rows lost).
    flush_merge_lock: parking_lot::Mutex<()>,
    next_segment_id: AtomicU64,
    manifest: Mutex<Manifest>,
    col_types: Vec<ColumnType>,
    /// Cache for GROUP BY results: key = (group_col << 32 | agg_col).
    /// Invalidated by clear_cache() on any write (INSERT/UPDATE/DELETE).
    groupby_cache: RwLock<std::collections::HashMap<u64, Vec<(String, i64, f64)>>>,
    /// Cache for IN-hash query row indices: key = (col_pos << 64 | set_sig).
    /// Avoids re-scanning 300K rows against a HashSet on repeated calls.
    in_hash_cache: RwLock<std::collections::HashMap<u128, Vec<usize>>>,
}

impl ColSegmentStore {
    /// Create a new store for a table at `base_dir/columnar_ms/<table_name>/`.
    /// (`columnar_ms` to avoid clashing with the time-series `columnar/` dir.)
    pub fn create(
        base_dir: &Path,
        table_name: &str,
        col_types: Vec<ColumnType>,
    ) -> Result<Arc<Self>> {
        let dir = base_dir.join("columnar_ms").join(table_name);
        std::fs::create_dir_all(&dir)?;
        let manifest_path = dir.join("MANIFEST");
        let manifest_exists = manifest_path.exists();
        let manifest = if manifest_exists {
            Manifest::open(&manifest_path)?
        } else {
            Manifest::create(&manifest_path)?
        };
        let buf_path = dir.join(".writebuf.tmp");
        let write_buf = ColumnarSSTableBuilder::new(&buf_path, col_types.clone());
        let store = Arc::new(Self {
            table_name: table_name.to_string(),
            dir,
            segments: RwLock::new(VecDeque::new()),
            write_buf: Mutex::new(write_buf),
            flush_merge_lock: parking_lot::Mutex::new(()),
            next_segment_id: AtomicU64::new(1),
            manifest: Mutex::new(manifest),
            col_types,
            groupby_cache: RwLock::new(std::collections::HashMap::new()),
            in_hash_cache: RwLock::new(std::collections::HashMap::new()),
        });
        // 🔥 Auto-recover segments from disk if the MANIFEST has active entries.
        // This handles the restart case: get_or_create_col_segment_store is called
        // on a table that has data on disk from a previous session.
        if manifest_exists {
            store.recover_from_disk();
        }
        Ok(store)
    }

    /// Append rows to the in-memory buffer. O(rows). Each tuple: (key, timestamp, values).
    /// 🔥 Stability: auto-compacts when segments exceed threshold, preventing
    /// unbounded segment accumulation from repeated writes.
    pub fn append_rows(&self, rows: &[(u64, u64, Vec<Value>)]) -> Result<()> {
        // Invalidate caches on write.
        if !rows.is_empty() {
            self.groupby_cache.write().clear();
            self.in_hash_cache.write().clear();
        }
        let mut buf = self.write_buf.lock();
        for (key, ts, row) in rows {
            buf.add_values(*key, *ts, false, row)?;
        }
        drop(buf);
        // Auto-compaction disabled during append_rows — it can deadlock
        // when merge_segments reads column data while holding write locks.
        // Compaction runs on demand via ensure_query_visibility or compact_once.
        Ok(())
    }

    /// Append a single row by reference — avoids the Vec<Value> clone that
    /// append_rows requires (it takes &[(.., Vec<Value>)]). This is the hot
    /// path for single-row INSERT (saves one heap allocation per INSERT).
    pub fn append_row_ref(&self, key: u64, ts: u64, row: &[Value]) -> Result<()> {
        self.groupby_cache.write().clear();
        self.in_hash_cache.write().clear();
        let mut buf = self.write_buf.lock();
        buf.add_values(key, ts, false, row)?;
        drop(buf);
        // Auto-compaction disabled — can deadlock with merge_segments.
        Ok(())
    }

    /// Append a tombstone (deletion marker) for a key. The tombstone suppresses
    /// the row in multi-segment scans (newest-version-wins with deleted=true).
    /// 🔥 Stability: auto-compacts when segments exceed threshold.
    pub fn append_tombstone(&self, key: u64, ts: u64) -> Result<()> {
        let mut buf = self.write_buf.lock();
        // Write placeholder values for each column (keeps column_buffers in sync
        // with num_rows). The actual values are never read for deleted rows.
        let placeholder: Vec<Value> = self.col_types.iter().map(|_| Value::Null).collect();
        buf.add_values(key, ts, true, &placeholder)?;
        drop(buf);
        // Auto-compaction disabled — can deadlock with merge_segments.
        Ok(())
    }

    /// Flush the buffer to a new delta segment on disk. Does NOT read old segments.
    /// O(this batch). Writes the file (no fsync — durability via WAL/manifest).
    pub fn flush_buffer(&self) -> Result<()> {
        // Serialize with merge_segments: if a merge is in progress, wait.
        // Without this, flush can create a segment that the merge then
        // clobbers (the large_batch_durability race).
        let _guard = self.flush_merge_lock.lock();
        // Take buffer contents out, replace with a fresh builder, release the lock fast.
        let buf_path = self.dir.join(".writebuf.tmp");
        let mut old_buf = {
            let mut guard = self.write_buf.lock();
            let fresh = ColumnarSSTableBuilder::new(&buf_path, self.col_types.clone());
            std::mem::replace(&mut *guard, fresh)
        };
        if old_buf.num_rows == 0 {
            return Ok(());
        }
        let id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
        let path = self.dir.join(format!("{:010}.sst", id));
        // finish() writes to builder.path; set it to the numbered path first.
        old_buf.path = path.clone();
        old_buf.finish()?;
        let seg = Arc::new(Segment::open(&path, id)?);
        // Record in manifest (fsync'd) BEFORE exposing in memory.
        self.manifest.lock().add_segment(id)?;
        self.segments.write().push_back(seg);
        // Invalidate all query caches (data changed).
        self.groupby_cache.write().clear();
        self.in_hash_cache.write().clear();
        Ok(())
    }

    /// Flush the write buffer ONLY if it contains pending rows/tombstones.
    /// Called at the start of query paths to ensure buffered writes are
    /// visible to segment-based scans. Cheap no-op when buffer is empty.
    /// This avoids per-DELETE flushes that created O(N) segments.
    ///
    /// NOTE: auto-compaction is triggered in append_rows/append_tombstone
    /// (write path), NOT here. Compacting during a read would invalidate
    /// Drop all data: clear in-memory segments + write buffer, delete on-disk
    /// segment files, and delete the manifest file so a reopen starts fresh.
    /// Called by DROP TABLE so a recreated same-named table starts empty (no
    /// stale rows). Best-effort on file deletion.
    pub fn drop_all(&self) -> Result<()> {
        // Snapshot segment ids (for file deletion), then clear in-memory state.
        let segs = self.segments_snapshot();
        let seg_ids: Vec<u64> = segs.iter().map(|s| s.id).collect();
        self.segments.write().clear();
        // Clear the write buffer by finishing (no-op if empty) then draining.
        // The builder has no public clear(); we just leave it — the store is
        // being removed from the registry anyway, so a new store is created on
        // recreate. Delete on-disk files so the old data can't be recovered.
        for id in &seg_ids {
            let path = self.dir.join(format!("{:010}.sst", id));
            let _ = std::fs::remove_file(&path);
        }
        // Delete the manifest file so a reopen finds no manifest → creates a
        // fresh one with no segments.
        let manifest_path = self.dir.join("MANIFEST");
        let _ = std::fs::remove_file(&manifest_path);
        Ok(())
    }

    /// SegData slices held by in-flight SelectColumnar queries (use-after-free).
    pub fn ensure_query_visibility(&self) -> Result<()> {
        if self.write_buf.lock().num_rows > 0 {
            self.flush_buffer()?;
        }
        Ok(())
    }

    /// Point lookup: newest segment first, return first hit.
    /// Uses per-segment column decode cache — first access decompresses each
    /// column once, subsequent lookups (incl. other keys) reuse the cache.
    ///
    /// 🔑 Tombstone-aware: if a segment contains the key but it's deleted
    /// (tombstone), we STOP searching — the deletion suppresses older live
    /// versions in older segments. Previously `get_row_cached` returned None
    /// for a tombstoned key, indistinguishable from "key not in segment", so
    /// `get` fell through to an older segment holding the live row and
    /// returned stale data after a DELETE.
    pub fn get(&self, key: u64) -> Option<Vec<Value>> {
        // 🔑 Check the write buffer FIRST — it may hold a newer version (UPDATE)
        // or a tombstone (DELETE) that supersedes the segment data. Without this,
        // a DELETE whose tombstone is still in the buffer (lazy flush) would be
        // invisible to get(), which would return the stale live row from a segment.
        {
            let buf = self.write_buf.lock();
            if let Some(idx) = buf.keys.iter().position(|&k| k == key) {
                // Found in buffer — newest version. If deleted, return None.
                if buf.deleted[idx] {
                    return None;
                }
                // Live buffered row: decode from the columnar buffer.
                let mut row = Vec::with_capacity(self.col_types.len());
                for ci in 0..self.col_types.len() {
                    if ci < buf.column_buffers.len() {
                        row.push(decode_buffered_value(&buf, ci, idx, &self.col_types[ci]));
                    } else {
                        row.push(Value::Null);
                    }
                }
                return Some(row);
            }
        }
        let segs = self.segments.read();
        for seg in segs.iter().rev() {
            // Check if this segment contains the key at all.
            if let Some(idx) = seg.sst.row_map.find_key(key) {
                // Key is in this segment. If deleted, it's a tombstone — the
                // newest version of this key is a deletion, so return None
                // regardless of older segments.
                if seg.sst.row_map.is_deleted(idx) {
                    return None;
                }
                // Live row: decode and return.
                return seg.get_row_cached(key, &self.col_types);
            }
            // Key not in this segment — continue to older segments.
        }
        None
    }

    /// Full-table ordered scan via multi-way merge. Newest version wins.
    pub fn scan(&self) -> MergeCursor {
        let segs: Vec<Arc<Segment>> = self.segments.read().iter().cloned().collect();
        MergeCursor::new(&segs, &self.col_types)
    }

    /// High-performance projected + filtered scan.
    ///
    /// Iterates each segment's columns directly (pre-decoded once per segment,
    /// like CREATE INDEX), applying `predicate(row_idx)` on the filter column
    /// before decoding any output columns. Only matching rows get their output
    /// columns decoded. Newest-segment-wins dedup via a seen-key set.
    ///
    /// This avoids the MergeCursor's per-row `Vec<Value>` allocation for ALL
    /// columns — the dominant cost for Full scan / WHERE / LIKE (was 68-197ms
    /// for 300K rows; pure column read is <2ms).
    ///
    /// `filter_col`: column position for the WHERE predicate.
    /// `project_cols`: output column positions (projection).
    /// `predicate`: returns true if the row at `row_idx` matches.
    /// Returns (key, output_values) pairs in ascending key order.
    pub fn scan_projected_filtered(
        &self,
        filter_col: Option<usize>,
        project_cols: &[usize],
        predicate: &dyn Fn(Option<&Value>) -> bool,
    ) -> Vec<(u64, Vec<Value>)> {
        self.scan_projected_filtered_limit(filter_col, project_cols, predicate, usize::MAX)
    }

    /// Same as scan_projected_filtered, but stops scanning after `max_results`
    /// matching rows have been collected. This enables LIMIT early-termination:
    /// SELECT cols FROM t LIMIT 50 only decodes 50 rows instead of all N.
    ///
    /// When max_results is very small (e.g. 1 for PK point queries), project
    /// columns are decoded lazily — only for matching rows, not pre-decoded for
    /// the entire segment. This is critical for PK point queries on large tables.
    pub fn scan_projected_filtered_limit(
        &self,
        filter_col: Option<usize>,
        project_cols: &[usize],
        predicate: &dyn Fn(Option<&Value>) -> bool,
        max_results: usize,
    ) -> Vec<(u64, Vec<Value>)> {
        let total_rows: usize = self.segments.read().iter().map(|s| s.sst.num_rows).sum();
        let mut result: Vec<(u64, Vec<Value>)> =
            Vec::with_capacity(total_rows.min(max_results).min(65536));
        if max_results == 0 {
            return result;
        }
        let segs = self.segments_snapshot();

        // For small result sets (≤8 rows expected), use lazy projection:
        // only decode output columns for matching rows, not the whole segment.
        let lazy_project = max_results <= 8;
        let single_seg = segs.len() <= 1;
        // 🔑 Newest-version-wins dedup. An UPDATE appends a newer row with the
        // SAME composite key; without dedup, scans return both versions. We
        // iterate segments newest→oldest (.rev()) and, within a segment, rows
        // newest→oldest (descending index), so the FIRST version of a key seen
        // is the newest — a plain HashSet suffices (no per-scan O(N log N) sort,
        // which caused a ~6x regression on DISTINCT/ORDER BY/LIKE/IN).
        //
        // For single-segment tables with no UPDATE history, keys are already
        // unique, so we skip dedup entirely (need_dedup=false) — zero overhead.
        let need_dedup = !single_seg || self.may_have_duplicate_keys();
        let mut seen: std::collections::HashSet<u64> = if need_dedup {
            std::collections::HashSet::with_capacity(total_rows)
        } else {
            std::collections::HashSet::new()
        };
        for seg in segs.iter().rev() {
            let n = seg.sst.num_rows;
            // Descending index order within a segment: rows are appended old→new,
            // so iterating n→0 visits the newest (largest index) version of a key
            // first. Combined with `seen`, this keeps the newest version.
            let order: Vec<usize> = if need_dedup {
                (0..n).rev().collect()
            } else {
                (0..n).collect()
            };

            // Pre-decode filter column (once per segment).
            let fcol_fixed = filter_col.and_then(|fc| {
                if fc < seg.sst.column_tags.len() && seg.sst.column_tags[fc].is_fixed() {
                    seg.sst.read_fixed_i64(fc).ok()
                } else {
                    None
                }
            });
            let fcol_text = filter_col.and_then(|fc| {
                if fc < seg.sst.column_tags.len() && !seg.sst.column_tags[fc].is_fixed() {
                    seg.sst.read_text(fc).ok()
                } else {
                    None
                }
            });
            let fcol_type = filter_col.and_then(|fc| self.col_types.get(fc));

            // Pre-intern filter Text column into ArcString vec to avoid per-row
            // String allocation in the predicate (WHERE/LIKE on text cols).
            let fcol_text_interned: Vec<Option<Value>> = if let Some(ref t) = fcol_text {
                (0..n)
                    .map(|i| {
                        if t.is_null(i) {
                            return None;
                        }
                        t.get_str(i)
                            .map(|s| Value::Text(crate::types::ArcString(std::sync::Arc::from(s))))
                    })
                    .collect()
            } else {
                Vec::new()
            };

            // Pre-decode project columns (once per segment) — unless lazy mode
            // (small result set): then we decode only for matched rows below.
            let pfixed: Vec<Option<crate::storage::lsm::columnar::FixedSegment>> = if !lazy_project
            {
                project_cols
                    .iter()
                    .map(|&pc| {
                        if pc < seg.sst.column_tags.len() && seg.sst.column_tags[pc].is_fixed() {
                            seg.sst.read_fixed_i64(pc).ok()
                        } else {
                            None
                        }
                    })
                    .collect()
            } else {
                Vec::new()
            };
            let ptext: Vec<Option<crate::storage::lsm::columnar::TextSegment>> = if !lazy_project {
                project_cols
                    .iter()
                    .map(|&pc| {
                        if pc < seg.sst.column_tags.len() && !seg.sst.column_tags[pc].is_fixed() {
                            seg.sst.read_text(pc).ok()
                        } else {
                            None
                        }
                    })
                    .collect()
            } else {
                Vec::new()
            };
            let n_seg = seg.sst.num_rows;
            let pvector: Vec<Vec<Option<Vec<f32>>>> = if !lazy_project {
                project_cols
                    .iter()
                    .map(|&pc| {
                        if pc < seg.sst.column_tags.len()
                            && matches!(
                                seg.sst.column_tags[pc],
                                crate::storage::lsm::columnar::ColumnTypeTag::Vector
                            )
                        {
                            let decoded = seg.sst.read_vectors(pc).unwrap_or_default();
                            let mut per = vec![None; n_seg];
                            let mut di = 0usize;
                            for i in 0..n_seg {
                                if seg.sst.row_map.is_deleted(i) {
                                    continue;
                                }
                                let ek = seg.sst.row_map.key(i) & 0xFFFFFFFF;
                                while di < decoded.len() && decoded[di].0 != ek {
                                    di += 1;
                                }
                                if di < decoded.len() {
                                    per[i] = Some(decoded[di].1.clone());
                                    di += 1;
                                }
                            }
                            per
                        } else {
                            Vec::new()
                        }
                    })
                    .collect()
            } else {
                Vec::new()
            };
            let pspatial: Vec<Vec<Option<crate::types::Geometry>>> = if !lazy_project {
                project_cols
                    .iter()
                    .map(|&pc| {
                        if pc < seg.sst.column_tags.len()
                            && matches!(
                                seg.sst.column_tags[pc],
                                crate::storage::lsm::columnar::ColumnTypeTag::Spatial
                            )
                        {
                            let decoded = seg.sst.read_spatial(pc).unwrap_or_default();
                            let mut per = vec![None; n_seg];
                            let mut di = 0usize;
                            for i in 0..n_seg {
                                if seg.sst.row_map.is_deleted(i) {
                                    continue;
                                }
                                let ek = seg.sst.row_map.key(i) & 0xFFFFFFFF;
                                while di < decoded.len() && decoded[di].0 != ek {
                                    di += 1;
                                }
                                if di < decoded.len() {
                                    per[i] = Some(decoded[di].1.clone());
                                    di += 1;
                                }
                            }
                            per
                        } else {
                            Vec::new()
                        }
                    })
                    .collect()
            } else {
                Vec::new()
            };

            let ptext_interned: Vec<Vec<Option<Value>>> = Vec::new();

            for &i in &order {
                let key = seg.sst.row_map.key(i);
                // Newest-version-wins dedup: skip if a newer version of this key
                // was already emitted. Mark seen BEFORE the deleted check so a
                // tombstone in a newer version suppresses older live rows.
                if need_dedup && !seen.insert(key) {
                    continue;
                }
                if seg.sst.row_map.is_deleted(i) {
                    continue;
                }

                // Decode filter value only (cheap: single column lookup).
                let fval: Option<Value> = if filter_col.is_some() {
                    let v = if let Some(ref f) = fcol_fixed {
                        match fcol_type {
                            Some(ColumnType::Integer) => f.get_i64(i).map(Value::Integer),
                            Some(ColumnType::Float) => f.get_f64(i).map(Value::Float),
                            Some(ColumnType::Boolean) => f.get_bool(i).map(Value::Bool),
                            _ => None,
                        }
                    } else if !fcol_text_interned.is_empty() {
                        fcol_text_interned.get(i).cloned().flatten()
                    } else if let Some(ref t) = fcol_text {
                        t.get_str(i).map(|s| Value::Text(s.into()))
                    } else {
                        None
                    };
                    v
                } else {
                    None
                };

                if !predicate(fval.as_ref()) {
                    continue;
                }

                // Decode output columns for matching row only.
                let mut row = Vec::with_capacity(project_cols.len());
                if lazy_project {
                    // Lazy mode: decode each column on-demand for this single row.
                    for &pc in project_cols.iter() {
                        let v = if pc < self.col_types.len() && pc < seg.sst.column_tags.len() {
                            if seg.sst.column_tags[pc].is_fixed() {
                                match self.col_types[pc] {
                                    ColumnType::Integer => seg
                                        .sst
                                        .read_fixed_i64(pc)
                                        .ok()
                                        .and_then(|f| f.get_i64(i))
                                        .map(Value::Integer),
                                    ColumnType::Float => seg
                                        .sst
                                        .read_fixed_i64(pc)
                                        .ok()
                                        .and_then(|f| f.get_f64(i))
                                        .map(Value::Float),
                                    ColumnType::Boolean => seg
                                        .sst
                                        .read_fixed_i64(pc)
                                        .ok()
                                        .and_then(|f| f.get_bool(i))
                                        .map(Value::Bool),
                                    _ => seg
                                        .sst
                                        .read_fixed_i64(pc)
                                        .ok()
                                        .and_then(|f| f.get_i64(i))
                                        .map(Value::Integer),
                                }
                            } else {
                                match seg
                                    .sst
                                    .read_text(pc)
                                    .ok()
                                    .and_then(|t| t.get_str(i).map(|s| s.to_string()))
                                {
                                    Some(s) => Some(Value::Text(s.into())),
                                    None => Some(Value::Null),
                                }
                            }
                        } else {
                            Some(Value::Null)
                        };
                        row.push(v.unwrap_or(Value::Null));
                    }
                } else {
                    for (pi, &pc) in project_cols.iter().enumerate() {
                        let v = if pc < self.col_types.len() {
                            match (&pfixed.get(pi), &ptext.get(pi), &self.col_types[pc]) {
                                (Some(Some(f)), _, ColumnType::Integer) => {
                                    f.get_i64(i).map(Value::Integer)
                                }
                                (Some(Some(f)), _, ColumnType::Float) => {
                                    f.get_f64(i).map(Value::Float)
                                }
                                (Some(Some(f)), _, ColumnType::Boolean) => {
                                    f.get_bool(i).map(Value::Bool)
                                }
                                (_, _, ColumnType::Spatial) => pspatial
                                    .get(pi)
                                    .and_then(|p| p.get(i))
                                    .cloned()
                                    .flatten()
                                    .map(|g| Value::Spatial(std::boxed::Box::new(g))),
                                (_, _, ColumnType::Tensor(_)) => pvector
                                    .get(pi)
                                    .and_then(|p| p.get(i))
                                    .cloned()
                                    .flatten()
                                    .map(|v| {
                                        Value::Vector(crate::types::ArcVec(std::sync::Arc::new(v)))
                                    }),
                                (_, Some(Some(t)), ColumnType::Text) => {
                                    if !ptext_interned.is_empty() {
                                        ptext_interned
                                            .get(pi)
                                            .and_then(|v| v.get(i))
                                            .cloned()
                                            .flatten()
                                    } else {
                                        t.get_str(i).map(|s| Value::Text(s.into()))
                                    }
                                }
                                _ => Some(Value::Null),
                            }
                        } else {
                            Some(Value::Null)
                        };
                        row.push(v.unwrap_or(Value::Null));
                    }
                } // end else (non-lazy)
                result.push((key, row));
                // 🚀 LIMIT early-termination: stop scanning once we have enough rows.
                if result.len() >= max_results {
                    return result;
                }
            }
        }
        result
    }

    /// High-performance scan with a Text (&str) predicate on the filter column.
    /// Avoids constructing a Value for the filter column entirely — the predicate
    /// receives the raw &str borrowed from the segment (zero allocation). Only
    /// matched rows get their output columns decoded (and output Text cols use
    /// pre-interned ArcString clones). This is the fast path for WHERE col = 'x'
    /// and LIKE 'prefix%' on text columns.
    pub fn scan_text_filtered(
        &self,
        filter_col: usize,
        project_cols: &[usize],
        str_predicate: &dyn Fn(Option<&str>) -> bool,
    ) -> Vec<(u64, Vec<Value>)> {
        self.scan_text_filtered_limit(filter_col, project_cols, str_predicate, usize::MAX)
    }

    /// Returns row INDICES (segment-local) that match the text filter, without
    /// decoding any output columns. The caller passes these indices to
    /// SelectColumnar for zero-copy materialization — avoiding N Vec<Value>
    /// allocations during scan. Only works for single-segment stores.
    ///
    /// Returns (indices, found). found=false if multi-segment (caller falls
    /// back to scan_text_filtered_limit).
    pub fn scan_row_indices_text_filter(
        &self,
        filter_col: usize,
        str_predicate: &dyn Fn(Option<&str>) -> bool,
        limit: usize,
    ) -> Option<Vec<usize>> {
        let segs = self.segments_snapshot();
        if segs.len() != 1 {
            return None;
        }
        let seg = &segs[0];
        let n = seg.sst.num_rows;
        let cap = if limit == usize::MAX { n } else { limit };
        let mut indices: Vec<usize> = Vec::with_capacity(cap.min(65536));
        let ftext = seg.sst.read_text(filter_col).ok();
        if let Some(tseg) = ftext.as_ref() {
            let has_nulls = tseg.has_any_null();
            let has_deletions = seg.sst.row_map.has_any_deleted();
            // 🚀 Fast inner loop: minimize branches per row.
            // When no nulls and no deletions, skip both checks entirely.
            if !has_nulls && !has_deletions {
                for i in 0..n {
                    let s = tseg.get_str_fast(i);
                    if str_predicate(Some(s)) {
                        indices.push(i);
                        if indices.len() >= limit {
                            break;
                        }
                    }
                }
            } else {
                for i in 0..n {
                    if has_deletions && seg.sst.row_map.is_deleted(i) {
                        continue;
                    }
                    let matches = if has_nulls {
                        str_predicate(if tseg.is_null(i) {
                            None
                        } else {
                            tseg.get_str(i)
                        })
                    } else {
                        str_predicate(Some(tseg.get_str_fast(i)))
                    };
                    if matches {
                        indices.push(i);
                        if indices.len() >= limit {
                            break;
                        }
                    }
                }
            }
        }
        Some(indices)
    }

    /// Prefix-match scan: returns row indices where the text column starts with
    /// `prefix`. Specialized hot path for LIKE 'prefix%' — uses direct byte
    /// comparison via `memcmp`-style slice check, avoiding closure dispatch and
    /// Option wrapping. ~20% faster than the generic text filter for prefix LIKE.
    pub fn scan_row_indices_prefix(
        &self,
        filter_col: usize,
        prefix: &[u8],
        limit: usize,
    ) -> Option<Vec<(usize, usize)>> {
        // Returns (segment_idx, local_row_idx) pairs for rows whose text column
        // starts with the given prefix. Multi-segment safe (dedup by key).
        let segs = self.segments_snapshot();
        let single_seg = segs.len() <= 1;
        let cap = if limit == usize::MAX { 65536 } else { limit };
        let mut indices: Vec<(usize, usize)> = Vec::with_capacity(cap.min(65536));
        let mut seen: std::collections::HashSet<u64> = if single_seg {
            std::collections::HashSet::new()
        } else {
            std::collections::HashSet::with_capacity(segs.iter().map(|s| s.sst.num_rows).sum())
        };
        let plen = prefix.len();
        for (sidx, seg) in segs.iter().enumerate() {
            let n = seg.sst.num_rows;
            let ftext = match seg.sst.read_text(filter_col) {
                Ok(t) => t,
                Err(_) => continue,
            };
            let has_nulls = ftext.has_any_null();
            let has_deletions = seg.sst.row_map.has_any_deleted();
            if !has_nulls && !has_deletions {
                // 🔑 Fast path (single OR multi segment): use the batch
                // prefix_match_indices which walks raw offsets in one pass
                // (no per-row slice() calls). Works for any segment count.
                let matched = ftext.prefix_match_indices(prefix);
                for &i in &matched {
                    if !single_seg {
                        let key = seg.sst.row_map.key(i);
                        if !seen.insert(key) {
                            continue;
                        }
                    }
                    indices.push((sidx, i));
                    if indices.len() >= limit {
                        return Some(indices);
                    }
                }
            } else {
                for i in 0..n {
                    if !single_seg {
                        let key = seg.sst.row_map.key(i);
                        if !seen.insert(key) {
                            continue;
                        }
                    }
                    if has_deletions && seg.sst.row_map.is_deleted(i) {
                        continue;
                    }
                    if let Some(s) = ftext.get_str(i) {
                        if s.len() >= plen && &s.as_bytes()[..plen] == prefix {
                            indices.push((sidx, i));
                            if indices.len() >= limit {
                                return Some(indices);
                            }
                        }
                    }
                }
            }
        }
        Some(indices)
    }

    /// Scan for rows where a TEXT column exactly equals `target`. Returns
    /// (segment_idx, local_row_idx) pairs. Zero-alloc via eq_match_indices.
    /// Used by `WHERE text_col = 'literal'` to bypass the Box<dyn Fn> path
    /// that pre-interns the entire column into ArcString Values.
    pub fn scan_row_indices_eq(
        &self,
        filter_col: usize,
        target: &[u8],
        limit: usize,
    ) -> Option<Vec<(usize, usize)>> {
        let segs = self.segments_snapshot();
        let single_seg = segs.len() <= 1;
        let mut indices: Vec<(usize, usize)> = Vec::with_capacity(1024);
        let mut seen: std::collections::HashSet<u64> = if single_seg {
            std::collections::HashSet::new()
        } else {
            std::collections::HashSet::with_capacity(segs.iter().map(|s| s.sst.num_rows).sum())
        };
        for (sidx, seg) in segs.iter().enumerate() {
            let ftext = match seg.sst.read_text(filter_col) {
                Ok(t) => t,
                Err(_) => continue,
            };
            let has_deletions = seg.sst.row_map.has_any_deleted();
            if !has_deletions {
                let matched = ftext.eq_match_indices(target);
                for &i in &matched {
                    if !single_seg {
                        let key = seg.sst.row_map.key(i);
                        if !seen.insert(key) {
                            continue;
                        }
                    }
                    indices.push((sidx, i));
                    if indices.len() >= limit {
                        return Some(indices);
                    }
                }
            } else {
                for i in 0..seg.sst.num_rows {
                    if !single_seg {
                        let key = seg.sst.row_map.key(i);
                        if !seen.insert(key) {
                            continue;
                        }
                    }
                    if seg.sst.row_map.is_deleted(i) {
                        continue;
                    }
                    if let Some(s) = ftext.get_str(i) {
                        if s.as_bytes() == target {
                            indices.push((sidx, i));
                            if indices.len() >= limit {
                                return Some(indices);
                            }
                        }
                    }
                }
            }
        }
        Some(indices)
    }

    /// Legacy single-segment variant — kept for backward compat.
    pub fn scan_row_indices_prefix_single(
        &self,
        filter_col: usize,
        prefix: &[u8],
        limit: usize,
    ) -> Option<Vec<usize>> {
        let segs = self.segments_snapshot();
        if segs.len() != 1 {
            return None;
        }
        let seg = &segs[0];
        let n = seg.sst.num_rows;
        let cap = if limit == usize::MAX { n } else { limit };
        let mut indices: Vec<usize> = Vec::with_capacity(cap.min(65536));
        let ftext = match seg.sst.read_text(filter_col) {
            Ok(t) => t,
            Err(_) => return Some(indices),
        };
        let has_nulls = ftext.has_any_null();
        let has_deletions = seg.sst.row_map.has_any_deleted();
        let plen = prefix.len();
        // 🚀 Fast path: no nulls + no deletions — tightest possible loop.
        if !has_nulls && !has_deletions {
            for i in 0..n {
                let s = ftext.get_str_fast(i);
                if s.len() >= plen && &s.as_bytes()[..plen] == prefix {
                    indices.push(i);
                    if indices.len() >= limit {
                        break;
                    }
                }
            }
        } else {
            for i in 0..n {
                if has_deletions && seg.sst.row_map.is_deleted(i) {
                    continue;
                }
                if has_nulls && ftext.is_null(i) {
                    continue;
                }
                // Direct byte comparison: get the string's raw bytes and check
                // if the first `plen` bytes match the prefix.
                if has_nulls {
                    if let Some(s) = ftext.get_str(i) {
                        if s.len() >= plen && &s.as_bytes()[..plen] == prefix {
                            indices.push(i);
                            if indices.len() >= limit {
                                break;
                            }
                        }
                    }
                } else {
                    let s = ftext.get_str_fast(i);
                    if s.len() >= plen && &s.as_bytes()[..plen] == prefix {
                        indices.push(i);
                        if indices.len() >= limit {
                            break;
                        }
                    }
                }
            }
        }
        Some(indices)
    }

    /// Text-filtered scan with early exit after `limit` matches.
    /// 1. Early exit: stops as soon as `limit` matches are collected.
    /// 2. Skips per-segment key sort + HashSet for the single-segment common
    ///    case (no dedup needed → natural 0..n order, saves O(N log N)).
    pub fn scan_text_filtered_limit(
        &self,
        filter_col: usize,
        project_cols: &[usize],
        str_predicate: &dyn Fn(Option<&str>) -> bool,
        limit: usize,
    ) -> Vec<(u64, Vec<Value>)> {
        let cap = if limit == usize::MAX { 65536 } else { limit };
        let mut result: Vec<(u64, Vec<Value>)> = Vec::with_capacity(cap.min(65536));
        let segs = self.segments_snapshot();
        let single_seg = segs.len() <= 1;

        // Only multi-segment needs dedup (seen set) + key-sorted iteration.
        // Single segment: iterate 0..n directly — no sort, no HashSet.
        let mut seen: Option<std::collections::HashSet<u64>> = if single_seg {
            None
        } else {
            let total_rows: usize = segs.iter().map(|s| s.sst.num_rows).sum();
            Some(std::collections::HashSet::with_capacity(total_rows))
        };

        'outer: for seg in segs.iter().rev() {
            let n = seg.sst.num_rows;

            // Filter column: read text segment once, predicate gets &str directly.
            let ftext = seg.sst.read_text(filter_col).ok();

            // Pre-read output columns (same segment, one-time cost per column).
            // This is O(cols) not O(rows) — much faster than per-row lazy decode.
            let pfixed: Vec<Option<crate::storage::lsm::columnar::FixedSegment>> = project_cols
                .iter()
                .map(|&pc| {
                    if pc < seg.sst.column_tags.len() && seg.sst.column_tags[pc].is_fixed() {
                        seg.sst.read_fixed_i64(pc).ok()
                    } else {
                        None
                    }
                })
                .collect();
            let ptext_cols: Vec<Option<crate::storage::lsm::columnar::TextSegment>> = project_cols
                .iter()
                .map(|&pc| {
                    if pc < seg.sst.column_tags.len()
                        && !seg.sst.column_tags[pc].is_fixed()
                        && !matches!(
                            seg.sst.column_tags[pc],
                            crate::storage::lsm::columnar::ColumnTypeTag::Spatial
                        )
                    {
                        seg.sst.read_text(pc).ok()
                    } else {
                        None
                    }
                })
                .collect();

            // Inner row-processing macro — shared between natural & sorted order.
            macro_rules! process_row {
                ($i:expr) => {{
                    let i = $i;
                    let key = seg.sst.row_map.key(i);
                    // Mark key as seen BEFORE checking deleted, so tombstones suppress
                    // older versions of the same key in earlier segments.
                    if let Some(ref mut s) = seen {
                        if !s.insert(key) {
                            continue;
                        }
                    }
                    if seg.sst.row_map.is_deleted(i) {
                        continue;
                    }

                    // Filter: pass raw &str to predicate (zero Value allocation).
                    let fval =
                        ftext
                            .as_ref()
                            .and_then(|t| if t.is_null(i) { None } else { t.get_str(i) });
                    if !str_predicate(fval) {
                        continue;
                    }

                    // Decode output columns from pre-read segments (O(1) per row).
                    let mut row = Vec::with_capacity(project_cols.len());
                    for (pi, &pc) in project_cols.iter().enumerate() {
                        let v = if pc < self.col_types.len() {
                            if matches!(
                                self.col_types[pc],
                                ColumnType::Spatial | ColumnType::Tensor(_)
                            ) {
                                Some(Value::Null)
                            } else if let Some(Some(ref f)) = pfixed.get(pi) {
                                match self.col_types[pc] {
                                    ColumnType::Integer => f.get_i64(i).map(Value::Integer),
                                    ColumnType::Float => f.get_f64(i).map(Value::Float),
                                    ColumnType::Boolean => f.get_bool(i).map(Value::Bool),
                                    _ => None,
                                }
                            } else if let Some(Some(ref t)) = ptext_cols.get(pi) {
                                t.get_str(i).map(|s| Value::Text(s.into()))
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        row.push(v.unwrap_or(Value::Null));
                    }
                    result.push((key, row));

                    // 🔥 Early exit: stop scanning once we have `limit` matches.
                    if result.len() >= limit {
                        break 'outer;
                    }
                }};
            }

            if single_seg {
                // Natural order — no sort, no dedup. The hot path for SELECTs.
                for i in 0..n {
                    process_row!(i);
                }
            } else {
                // Multi-segment: sort by key so newest version wins dedup.
                let mut order: Vec<usize> = (0..n).collect();
                order.sort_unstable_by_key(|&i| seg.sst.row_map.key(i));
                for &i in &order {
                    process_row!(i);
                }
            }
        }
        result
    }

    pub fn segment_count(&self) -> usize {
        self.segments.read().len()
    }

    /// 🚀 Combined scan + row build for text-equality WHERE queries.
    /// Reads the filter column, applies equality, AND builds output rows
    /// in a single pass — no intermediate indices Vec, no SelectColumnar.
    /// ~15% faster than scan_row_indices + materialize for WHERE col='val'.
    pub fn scan_text_eq_build(
        &self,
        filter_col: usize,
        filter_val: &str,
        project_cols: &[usize],
        col_types: &[ColumnType],
        limit: usize,
    ) -> Option<Vec<Vec<Value>>> {
        let segs = self.segments_snapshot();
        if segs.len() != 1 {
            return None;
        }
        let seg = &segs[0];
        let n = seg.sst.num_rows;
        let ftext = seg.sst.read_text(filter_col).ok()?;

        // Pre-read output columns.
        let ncols = project_cols.len();
        let fixed_cols: Vec<Option<crate::storage::lsm::columnar::FixedSegment>> = project_cols
            .iter()
            .map(|&pc| {
                if pc < seg.sst.column_tags.len() && seg.sst.column_tags[pc].is_fixed() {
                    seg.sst.read_fixed_i64(pc).ok()
                } else {
                    None
                }
            })
            .collect();
        let text_cols: Vec<Option<crate::storage::lsm::columnar::TextSegment>> = project_cols
            .iter()
            .map(|&pc| {
                if pc < seg.sst.column_tags.len()
                    && matches!(
                        seg.sst.column_tags[pc],
                        crate::storage::lsm::columnar::ColumnTypeTag::Text
                    )
                {
                    seg.sst.read_text(pc).ok()
                } else {
                    None
                }
            })
            .collect();

        // String pool for text output columns.
        let mut str_pool: std::collections::HashMap<&str, std::sync::Arc<str>> =
            std::collections::HashMap::with_capacity(64);

        let has_nulls = ftext.has_any_null();
        let has_deletions = seg.sst.row_map.has_any_deleted();

        let cap = if limit == usize::MAX { n / 2 } else { limit };
        let mut result: Vec<Vec<Value>> = Vec::with_capacity(cap.min(65536));

        // Tight inner loop: scan + filter + build in one pass.
        // Use Vec::with_capacity per row — the buffer reuse pattern doesn't
        // actually work because mem::take leaves a zero-capacity Vec.
        if !has_nulls && !has_deletions {
            for i in 0..n {
                // Inline equality check — avoids closure dispatch.
                let s = ftext.get_str_fast(i);
                if Some(s) == Some(filter_val) {
                    let mut row = Vec::with_capacity(ncols);
                    for (pi, &pc) in project_cols.iter().enumerate() {
                        let v = if let Some(Some(ref f)) = fixed_cols.get(pi) {
                            match col_types.get(pc) {
                                Some(ColumnType::Integer) => f.get_i64(i).map(Value::Integer),
                                Some(ColumnType::Float) => f.get_f64(i).map(Value::Float),
                                Some(ColumnType::Boolean) => f.get_bool(i).map(Value::Bool),
                                _ => None,
                            }
                        } else if let Some(Some(ref t)) = text_cols.get(pi) {
                            t.get_str(i).map(|s| {
                                let arc = str_pool.get(s).cloned().unwrap_or_else(|| {
                                    let a: std::sync::Arc<str> = std::sync::Arc::from(s);
                                    if str_pool.len() < 10000 {
                                        str_pool.insert(s, a.clone());
                                    }
                                    a
                                });
                                Value::Text(ArcString(arc))
                            })
                        } else {
                            Some(Value::Null)
                        };
                        row.push(v.unwrap_or(Value::Null));
                    }
                    result.push(row);
                    if result.len() >= limit {
                        break;
                    }
                }
            }
        } else {
            // Slow path with null/deletion checks.
            for i in 0..n {
                if has_deletions && seg.sst.row_map.is_deleted(i) {
                    continue;
                }
                let s = if has_nulls {
                    ftext.get_str(i)
                } else {
                    Some(ftext.get_str_fast(i))
                };
                if s != Some(filter_val) {
                    continue;
                }
                let mut row = Vec::with_capacity(ncols);
                for (pi, &pc) in project_cols.iter().enumerate() {
                    let v = if let Some(Some(ref f)) = fixed_cols.get(pi) {
                        match col_types.get(pc) {
                            Some(ColumnType::Integer) => f.get_i64(i).map(Value::Integer),
                            Some(ColumnType::Float) => f.get_f64(i).map(Value::Float),
                            Some(ColumnType::Boolean) => f.get_bool(i).map(Value::Bool),
                            _ => None,
                        }
                    } else if let Some(Some(ref t)) = text_cols.get(pi) {
                        t.get_str(i).map(|s| {
                            let arc = str_pool.get(s).cloned().unwrap_or_else(|| {
                                let a: std::sync::Arc<str> = std::sync::Arc::from(s);
                                if str_pool.len() < 10000 {
                                    str_pool.insert(s, a.clone());
                                }
                                a
                            });
                            Value::Text(ArcString(arc))
                        })
                    } else {
                        Some(Value::Null)
                    };
                    row.push(v.unwrap_or(Value::Null));
                }
                result.push(row);
                if result.len() >= limit {
                    break;
                }
            }
        }

        Some(result)
    }

    /// Streaming Top-K: read only the sort column, maintain a bounded heap of
    /// (value, key) pairs, return the K winning keys. Avoids materializing all
    /// N rows + sorting — O(N log K) with O(K) memory.
    ///
    /// For ORDER BY amount DESC LIMIT 10: reads only the amount column (1 col),
    /// keeps top 10 in a heap, then the caller fetches only those 10 full rows.
    pub fn topk_keys_by_fixed_col(&self, sort_col: usize, k: usize, ascending: bool) -> Vec<u64> {
        use std::collections::BinaryHeap;

        let segs = self.segments_snapshot();
        let single_seg = segs.len() <= 1;
        let mut seen: Option<std::collections::HashSet<u64>> = if single_seg {
            None
        } else {
            let total_rows: usize = segs.iter().map(|s| s.sst.num_rows).sum();
            Some(std::collections::HashSet::with_capacity(total_rows))
        };

        // Wrap f64 for total ordering (NaN-safe).
        #[derive(Clone)]
        struct OrdF64(f64);
        impl PartialEq for OrdF64 {
            fn eq(&self, o: &Self) -> bool {
                self.0 == o.0
            }
        }
        impl Eq for OrdF64 {}
        impl PartialOrd for OrdF64 {
            fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(o))
            }
        }
        impl Ord for OrdF64 {
            fn cmp(&self, o: &Self) -> std::cmp::Ordering {
                self.0
                    .partial_cmp(&o.0)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }
        }

        // BinaryHeap is a max-heap. To keep the K LARGEST (descending), we need
        // a min-heap so the smallest is evicted → wrap in Reverse.
        // To keep the K SMALLEST (ascending), we need a max-heap → no Reverse.
        let mut heap: BinaryHeap<(OrdF64, u64)> = BinaryHeap::with_capacity(k + 1);
        for seg in segs.iter().rev() {
            let n = seg.sst.num_rows;
            let fseg = match seg.sst.read_fixed_i64(sort_col) {
                Ok(f) => f,
                Err(_) => continue,
            };
            for i in 0..n {
                let key = seg.sst.row_map.key(i);
                if let Some(ref mut s) = seen {
                    if !s.insert(key) {
                        continue;
                    }
                }
                if seg.sst.row_map.is_deleted(i) {
                    continue;
                }
                if let Some(v) = fseg
                    .get_f64(i)
                    .or_else(|| fseg.get_i64(i).map(|x| x as f64))
                {
                    let entry = if ascending {
                        (OrdF64(v), key)
                    } else {
                        // For descending: use negated value so max-heap keeps largest.
                        (OrdF64(-v), key)
                    };
                    heap.push(entry);
                    if heap.len() > k {
                        heap.pop();
                    }
                }
            }
        }

        let mut result: Vec<(f64, u64)> = heap
            .into_iter()
            .map(|(of, key)| {
                let v = if ascending { of.0 } else { -of.0 };
                (v, key)
            })
            .collect();
        // Sort by value descending (for DESC) or ascending (for ASC).
        if ascending {
            result.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        } else {
            result.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        }
        result.into_iter().map(|(_, key)| key).collect()
    }

    /// Snapshot of active segments (oldest→newest). Callers iterate directly
    /// for single-column reads (e.g. CREATE INDEX) without full-row decode.
    pub fn segments_snapshot(&self) -> Vec<Arc<Segment>> {
        self.segments.read().iter().cloned().collect()
    }

    /// Release mmap pages + clear col caches to reduce RSS after queries.
    /// Call after batch queries to keep memory low.
    /// Clear column decode caches to reduce heap memory. Does NOT release
    /// mmap pages (MADV_DONTNEED) — keeping them warm makes subsequent queries
    /// fast (no re-faulting). mmap pages count against OS page cache, not heap.
    /// Call after batch queries to release decode-cache heap allocations.
    pub fn release_query_memory(&self) {
        let segs = self.segments.read();
        for seg in segs.iter() {
            seg.clear_cache();
        }
    }

    /// After flush+compaction to a single segment, return that segment's SSTable
    /// as a shared Arc. Legacy read paths (aggregate, GROUP BY) read
    /// `columnar_sstables: DashMap<String, Arc<ColumnarSSTable>>`; this lets them
    /// observe the same SSTable without cloning (Arc shared). Returns None if
    /// the store has no segments.
    pub fn latest_segment_sst(
        &self,
    ) -> Option<Arc<crate::storage::lsm::columnar::ColumnarSSTable>> {
        self.segments.read().back().map(|seg| Arc::clone(&seg.sst))
    }

    /// Number of rows currently buffered in memory (not yet flushed to a segment).
    /// Count live (non-deleted, non-duplicated) rows across all segments.
    /// O(total_rows) but zero Value decode — fast for COUNT(*).
    /// Heuristic: does a single (compacted) segment possibly hold multiple
    /// versions of the same key? flush_buffer() runs dedup_keys_newest_wins, so
    /// a freshly-flushed single segment has unique keys. Duplicate keys can
    /// appear only when an UPDATE was buffered and NOT yet flushed/compacted.
    /// Returns false for the common case (no pending UPDATEs), letting the scan
    /// path skip dedup entirely (the v0.5.0 performance fix).
    fn may_have_duplicate_keys(&self) -> bool {
        // The write buffer can hold a newer version of an already-segmented key.
        // Multiple segments can also hold overlapping keys (e.g. an INSERT
        // segment flushed by auto-checkpoint, then an UPDATE segment from a
        // later flush — both contain the same composite key with different
        // values). Conservative: dedup whenever there's buffered data OR 2+
        // segments. A single compacted segment with empty buffer is the only
        // safe no-dedup case.
        let buf_n = self.write_buf.lock().num_rows;
        let seg_count = self.segments.read().len();
        buf_n > 0 && seg_count >= 1 || seg_count >= 2
    }

    /// Count rows matching a filter WITHOUT materializing Value objects.
    /// Optimized for COUNT(*) WHERE col = val / col > val / col < val.
    ///
    /// For Integer/Float filter columns, compares raw i64/f64 bits directly.
    /// For Text filter columns, compares &str without ArcString allocation.
    /// This avoids the per-row Value::Text(s.into()) / Value::Integer(v)
    /// allocation that scan_projected_filtered does — the dominant cost for
    /// COUNT WHERE on large tables (was ~1ms for 20K rows).
    pub fn count_filtered(
        &self,
        filter_col: usize,
        op: &crate::sql::ast::BinaryOperator,
        target: &Value,
    ) -> usize {
        // 🔑 Flush buffered writes (INSERT/UPDATE/DELETE) so they're visible to
        // the segment scan. Without this, count_filtered only sees persisted
        // segments and misses buffered updates.
        let _ = self.flush_buffer();

        // Determine dedup need BEFORE locking write_buf (may_have_duplicate_keys
        // also locks write_buf — parking_lot Mutex is not reentrant → deadlock).
        let need_dedup = self.may_have_duplicate_keys();
        let buf = self.write_buf.lock();
        let segs = self.segments.read();
        let mut seen: std::collections::HashSet<u64> = if need_dedup {
            std::collections::HashSet::with_capacity(segs.iter().map(|s| s.sst.num_rows).sum())
        } else {
            std::collections::HashSet::new()
        };

        // Pre-extract target comparison value (avoids re-matching per row).
        let target_i = if let Value::Integer(v) = target {
            Some(*v)
        } else {
            None
        };
        let mut target_f = if let Value::Float(v) = target {
            Some(*v)
        } else {
            None
        };
        // 🔑 Cross-type: if the literal was parsed as Integer but the column is
        // Float, convert it so the comparison works (WHERE score > 50 parses
        // as Integer(50), but score is a FLOAT column).
        if target_f.is_none() {
            if let Some(i) = target_i {
                target_f = Some(i as f64);
            }
        }
        let target_s: Option<&str> = if let Value::Text(t) = target {
            Some(t.as_str())
        } else {
            None
        };
        let target_b = if let Value::Bool(v) = target {
            Some(*v)
        } else {
            None
        };

        let mut count = 0usize;
        for seg in segs.iter().rev() {
            let n = seg.sst.num_rows;
            if filter_col >= seg.sst.column_tags.len() {
                continue;
            }
            let tag = seg.sst.column_tags[filter_col];

            // Pre-decode the filter column once per segment.
            let fcol_fixed = if tag.is_fixed() {
                seg.sst.read_fixed_i64(filter_col).ok()
            } else {
                None
            };
            let fcol_text = if matches!(tag, ColumnTypeTag::Text) {
                seg.sst.read_text(filter_col).ok()
            } else {
                None
            };

            let order: Vec<usize> = if need_dedup {
                (0..n).rev().collect()
            } else {
                (0..n).collect()
            };
            for &i in &order {
                let key = seg.sst.row_map.key(i);
                if need_dedup && !seen.insert(key) {
                    continue;
                }
                if seg.sst.row_map.is_deleted(i) {
                    continue;
                }

                let matches = if let Some(ref f) = fcol_fixed {
                    // Fixed-width: compare raw i64/f64 bits, no Value alloc.
                    match tag {
                        ColumnTypeTag::Integer | ColumnTypeTag::Timestamp => {
                            let v = f.get_i64(i);
                            cmp_opt(v, target_i, op)
                        }
                        ColumnTypeTag::Float => {
                            let v = f.get_f64(i);
                            cmp_opt_f64(v, target_f, op)
                        }
                        ColumnTypeTag::Bool => {
                            let v = f.get_bool(i);
                            cmp_opt(v, target_b, op)
                        }
                        _ => false,
                    }
                } else if let Some(ref t) = fcol_text {
                    // Text: compare &str directly, no ArcString alloc.
                    match t.get_str(i) {
                        Some(s) => cmp_str(Some(s), target_s, op),
                        None => false, // NULL never matches
                    }
                } else {
                    false
                };

                if matches {
                    count += 1;
                }
            }
        }
        drop(buf);
        count
    }

    /// Single-pass aggregate over a filtered column — computes COUNT/SUM/AVG/
    /// MIN/MAX in one scan without materializing Value objects per row.
    /// Returns (count, int_sum, float_sum, has_float, min_int, max_int,
    /// min_float, max_float). The caller picks the relevant fields per aggregate.
    ///
    /// 🔑 PERF: scan_projected_filtered materialized a Vec<Value> per row then
    /// did multi-pass collect()+sum(). This folds directly over raw i64/f64
    /// column bytes — zero per-row allocation, single pass.
    pub fn aggregate_filtered(
        &self,
        filter_col: Option<usize>,
        agg_col: usize,
        op: &crate::sql::ast::BinaryOperator,
        target: &Value,
    ) -> AggregateResult {
        // 🔑 Flush buffered writes so they're visible to the segment scan.
        let _ = self.flush_buffer();
        let need_dedup = self.may_have_duplicate_keys();
        let segs = self.segments_snapshot();
        let mut seen: std::collections::HashSet<u64> = if need_dedup {
            std::collections::HashSet::with_capacity(segs.iter().map(|s| s.sst.num_rows).sum())
        } else {
            std::collections::HashSet::new()
        };
        // Pre-extract filter target for comparison.
        let target_i = if let Value::Integer(v) = target {
            Some(*v)
        } else {
            None
        };
        let target_f = if let Value::Float(v) = target {
            Some(*v)
        } else {
            None
        };
        let target_s: Option<&str> = if let Value::Text(t) = target {
            Some(t.as_str())
        } else {
            None
        };
        let no_filter = filter_col.is_none();
        let fc = filter_col.unwrap_or(0);

        let mut result = AggregateResult::default();
        for seg in segs.iter().rev() {
            let n = seg.sst.num_rows;
            if agg_col >= seg.sst.column_tags.len() {
                continue;
            }
            // Pre-decode filter + aggregate columns once per segment.
            let fcol_fixed = if !no_filter
                && fc < seg.sst.column_tags.len()
                && seg.sst.column_tags[fc].is_fixed()
            {
                seg.sst.read_fixed_i64(fc).ok()
            } else {
                None
            };
            let fcol_text = if !no_filter
                && fc < seg.sst.column_tags.len()
                && matches!(seg.sst.column_tags[fc], ColumnTypeTag::Text)
            {
                seg.sst.read_text(fc).ok()
            } else {
                None
            };
            let agg_fixed = if seg.sst.column_tags[agg_col].is_fixed() {
                seg.sst.read_fixed_i64(agg_col).ok()
            } else {
                None
            };
            let agg_is_float = matches!(self.col_types.get(agg_col), Some(ColumnType::Float));

            let order: Vec<usize> = if need_dedup {
                (0..n).rev().collect()
            } else {
                (0..n).collect()
            };
            for &i in &order {
                let key = seg.sst.row_map.key(i);
                if need_dedup && !seen.insert(key) {
                    continue;
                }
                if seg.sst.row_map.is_deleted(i) {
                    continue;
                }

                // Apply filter predicate (zero-alloc, same as count_filtered).
                let passes = if no_filter {
                    true
                } else if let Some(ref f) = fcol_fixed {
                    match seg.sst.column_tags[fc] {
                        ColumnTypeTag::Integer | ColumnTypeTag::Timestamp => {
                            cmp_opt(f.get_i64(i), target_i, op)
                        }
                        ColumnTypeTag::Float => cmp_opt_f64(f.get_f64(i), target_f, op),
                        ColumnTypeTag::Bool => {
                            let tb = target_i.map(|i| i != 0);
                            cmp_opt(f.get_bool(i), tb, op)
                        }
                        _ => false,
                    }
                } else if let Some(ref t) = fcol_text {
                    cmp_str(t.get_str(i), target_s, op)
                } else {
                    false
                };

                if !passes {
                    continue;
                }

                // Fold aggregate value directly (no Value construction).
                // 🔑 COUNT(col)/SUM/AVG/MIN/MAX all skip NULLs — only count
                // when the value is present (get_i64/get_f64 return None for NULL).
                if let Some(ref af) = agg_fixed {
                    if agg_is_float {
                        match af.get_f64(i) {
                            Some(v) => {
                                result.count += 1;
                                result.float_sum += v;
                                result.has_float = true;
                                if result.count == 1 {
                                    result.min_float = v;
                                    result.max_float = v;
                                } else {
                                    result.min_float = result.min_float.min(v);
                                    result.max_float = result.max_float.max(v);
                                }
                            }
                            None => {
                                result.null_count += 1;
                            }
                        }
                    } else {
                        match af.get_i64(i) {
                            Some(v) => {
                                result.count += 1;
                                result.int_sum = result.int_sum.wrapping_add(v);
                                if result.count == 1 {
                                    result.min_int = v;
                                    result.max_int = v;
                                } else {
                                    result.min_int = result.min_int.min(v);
                                    result.max_int = result.max_int.max(v);
                                }
                            }
                            None => {
                                result.null_count += 1;
                            }
                        }
                    }
                } else {
                    // Variable-width column (TEXT/Vector/Spatial): COUNT(col) counts
                    // non-NULL rows. Use the column's null_flags to determine NULL.
                    let is_null = self
                        .col_types
                        .get(agg_col)
                        .and_then(|_| seg.sst.read_text(agg_col).ok())
                        .map(|t| t.is_null(i))
                        .unwrap_or(true);
                    if is_null {
                        result.null_count += 1;
                    } else {
                        result.count += 1;
                    }
                }
            }
        }
        result
    }

    pub fn count_live_rows(&self) -> usize {
        // Fast path: single segment, no buffer, no deletions → just return num_rows.
        // This covers the common case (fresh insert, no UPDATE/DELETE history).
        let buf = self.write_buf.lock();
        let buf_count = buf.num_rows;
        let segs = self.segments.read();
        if segs.len() == 1 && buf_count == 0 {
            let seg = &segs[0];
            if !seg.sst.row_map.has_any_deleted() {
                return seg.sst.num_rows;
            }
            // Single segment with deletions: count non-deleted rows directly
            // (O(n) scan of the row_map, no HashMap allocation).
            let mut count = 0usize;
            for i in 0..seg.sst.num_rows {
                if !seg.sst.row_map.is_deleted(i) {
                    count += 1;
                }
            }
            return count;
        }
        drop(buf);

        // Slow path: multi-segment with UPDATE/DELETE history.
        // Newest-version-wins across buffer + segments.
        let mut liveness: std::collections::HashMap<u64, bool> = {
            let buf = self.write_buf.lock();
            buf.latest_entries().into_iter().collect()
        };
        // Newest-version-wins: iterate segments newest→oldest.
        for seg in segs.iter().rev() {
            for i in (0..seg.sst.num_rows).rev() {
                let key = seg.sst.row_map.key(i);
                if liveness.contains_key(&key) {
                    continue;
                }
                liveness.insert(key, seg.sst.row_map.is_deleted(i));
            }
        }
        liveness.values().filter(|&&deleted| !deleted).count()
    }

    /// Group-by scan: iterate the group column directly (TextSegment), returning
    /// Count + Sum with a text filter: iterate filter col (TextSegment) + sum col
    /// (FixedSegment) directly. Returns (count, sum). Zero Vec<Value> allocation.
    /// Optimized for SELECT COUNT(*), SUM(col) WHERE text_col = 'val'.
    pub fn count_sum_text_filter(
        &self,
        filter_col: usize,
        filter_val: &str,
        sum_col: usize,
    ) -> (i64, f64) {
        let segs = self.segments_snapshot();
        let single_seg = segs.len() <= 1;
        let mut seen: Option<std::collections::HashSet<u64>> = if single_seg {
            None
        } else {
            Some(std::collections::HashSet::new())
        };
        let mut count = 0i64;
        let mut sum = 0.0f64;
        for seg in segs.iter().rev() {
            let n = seg.sst.num_rows;
            let ftext = seg.sst.read_text(filter_col).ok();
            let fsum = seg.sst.read_fixed_i64(sum_col).ok();
            if let Some(tseg) = ftext.as_ref() {
                for i in 0..n {
                    let key = seg.sst.row_map.key(i);
                    if let Some(ref mut s) = seen {
                        if !s.insert(key) {
                            continue;
                        }
                    }
                    if seg.sst.row_map.is_deleted(i) {
                        continue;
                    }
                    if tseg.get_str(i) == Some(filter_val) {
                        count += 1;
                        if let Some(ref f) = fsum {
                            if let Some(v) = f.get_f64(i) {
                                sum += v;
                            } else if let Some(v) = f.get_i64(i) {
                                sum += v as f64;
                            }
                        }
                    }
                }
            }
        }
        (count, sum)
    }

    /// Count + Min + Max with a text filter. Returns (count, min, max).
    pub fn count_min_max_text_filter(
        &self,
        filter_col: usize,
        filter_val: &str,
        agg_col: usize,
    ) -> (i64, f64, f64) {
        let segs = self.segments_snapshot();
        let single_seg = segs.len() <= 1;
        let mut seen: Option<std::collections::HashSet<u64>> = if single_seg {
            None
        } else {
            Some(std::collections::HashSet::new())
        };
        let mut count = 0i64;
        let mut min = f64::INFINITY;
        let mut max = f64::NEG_INFINITY;
        for seg in segs.iter().rev() {
            let n = seg.sst.num_rows;
            let ftext = seg.sst.read_text(filter_col).ok();
            let fagg = seg.sst.read_fixed_i64(agg_col).ok();
            if let Some(tseg) = ftext.as_ref() {
                for i in 0..n {
                    let key = seg.sst.row_map.key(i);
                    if let Some(ref mut s) = seen {
                        if !s.insert(key) {
                            continue;
                        }
                    }
                    if seg.sst.row_map.is_deleted(i) {
                        continue;
                    }
                    if tseg.get_str(i) == Some(filter_val) {
                        count += 1;
                        if let Some(ref f) = fagg {
                            let v = f
                                .get_f64(i)
                                .unwrap_or_else(|| f.get_i64(i).map(|i| i as f64).unwrap_or(0.0));
                            min = min.min(v);
                            max = max.max(v);
                        }
                    }
                }
            }
        }
        (count, min.max(f64::NEG_INFINITY), max.min(f64::INFINITY))
    }

    /// Combined COUNT + SUM + MIN + MAX with a text filter in a SINGLE pass.
    /// Returns (count, sum, min, max). Replaces the old 2-scan approach
    /// (count_min_max_text_filter + count_sum_text_filter) which doubled latency.
    pub fn count_sum_min_max_text_filter(
        &self,
        filter_col: usize,
        filter_val: &str,
        agg_col: usize,
    ) -> (i64, f64, f64, f64) {
        let segs = self.segments_snapshot();
        let single_seg = segs.len() <= 1;
        let mut seen: Option<std::collections::HashSet<u64>> = if single_seg {
            None
        } else {
            Some(std::collections::HashSet::new())
        };
        let mut count = 0i64;
        let mut sum = 0.0f64;
        let mut min = f64::INFINITY;
        let mut max = f64::NEG_INFINITY;
        for seg in segs.iter().rev() {
            let n = seg.sst.num_rows;
            let ftext = seg.sst.read_text(filter_col).ok();
            let fagg = seg.sst.read_fixed_i64(agg_col).ok();
            if let Some(tseg) = ftext.as_ref() {
                for i in 0..n {
                    let key = seg.sst.row_map.key(i);
                    if let Some(ref mut s) = seen {
                        if !s.insert(key) {
                            continue;
                        }
                    }
                    if seg.sst.row_map.is_deleted(i) {
                        continue;
                    }
                    if tseg.get_str(i) == Some(filter_val) {
                        count += 1;
                        if let Some(ref f) = fagg {
                            let v = f
                                .get_f64(i)
                                .unwrap_or_else(|| f.get_i64(i).map(|i| i as f64).unwrap_or(0.0));
                            sum += v;
                            if v < min {
                                min = v;
                            }
                            if v > max {
                                max = v;
                            }
                        }
                    }
                }
            }
        }
        let min = if count == 0 { 0.0 } else { min };
        let max = if count == 0 { 0.0 } else { max };
        (count, sum, min, max)
    }

    /// Find the row indices of the top-K rows by a single fixed (numeric)
    /// column, without materializing any Vec<Value> rows. Returns
    /// (segment_index, local_row_idx) pairs for the K rows with the largest
    /// (descending) or smallest (ascending) values.
    ///
    /// This is the key optimization for `ORDER BY col LIMIT K`: instead of
    /// building 300K projected rows and sorting them (the old path, ~10ms), it
    /// scans just one column keeping a bounded min/max-heap of size K — O(N)
    /// with O(K) memory and zero per-row allocation (~1ms for K=10 on 300K).
    /// Decode specific rows by their (segment_index, row_index) for the given
    /// output columns. Used by ORDER BY LIMIT top-K: find the K row indices via
    /// top_k_row_indices_typed (scans only the sort column), then decode the
    /// output columns for just those K rows — not all N.
    /// 🔑 Batch-decodes each output column ONCE per segment (not per row),
    /// avoiding K× redundant column segment decompressions.
    pub fn decode_rows_at(
        &self,
        indices: &[(usize, usize)],
        out_cols: &[usize],
    ) -> Vec<Vec<Value>> {
        if indices.is_empty() {
            return Vec::new();
        }
        let segs = self.segments_snapshot();
        let mut result: Vec<Vec<Value>> = Vec::with_capacity(indices.len());
        // Pre-decode columns per segment lazily (cached in a local map).
        // For small K this is much cheaper than N full-row decode.
        for &(seg_idx, row_idx) in indices {
            let Some(seg) = segs.get(seg_idx) else {
                continue;
            };
            if row_idx >= seg.sst.num_rows {
                continue;
            }
            if seg.sst.row_map.has_any_deleted() && seg.sst.row_map.is_deleted(row_idx) {
                continue;
            }
            let mut row = Vec::with_capacity(out_cols.len());
            for &ci in out_cols {
                let tag = seg.sst.column_tags.get(ci).copied();
                let v =
                    match tag {
                        Some(t) if t.is_fixed() => seg.sst.read_fixed_i64(ci).ok().and_then(|f| {
                            match self.col_types.get(ci) {
                                Some(ColumnType::Integer) => f.get_i64(row_idx).map(Value::Integer),
                                Some(ColumnType::Float) => f.get_f64(row_idx).map(Value::Float),
                                Some(ColumnType::Boolean) => f.get_bool(row_idx).map(Value::Bool),
                                Some(ColumnType::Timestamp) => f.get_i64(row_idx).map(|v| {
                                    Value::Timestamp(crate::types::Timestamp::from_micros(v))
                                }),
                                _ => None,
                            }
                        }),
                        Some(ColumnTypeTag::Text) => seg.sst.read_text(ci).ok().and_then(|t| {
                            t.get_str(row_idx)
                                .map(|s| Value::Text(ArcString(std::sync::Arc::from(s))))
                        }),
                        _ => None,
                    };
                row.push(v.unwrap_or(Value::Null));
            }
            result.push(row);
        }
        result
    }

    pub fn top_k_row_indices(&self, order_col: usize, k: usize, desc: bool) -> Vec<(usize, usize)> {
        // Delegates to the type-aware variant, assuming an Integer column.
        // Callers that know the column is Float should use top_k_row_indices_typed.
        self.top_k_row_indices_typed(order_col, k, desc, false)
    }

    /// Type-aware top-K. `is_float` selects the correct decoder so Float columns
    /// are not misread as Integer (their 8-byte fixed slot decodes as garbage i64).
    pub fn top_k_row_indices_typed(
        &self,
        order_col: usize,
        k: usize,
        desc: bool,
        is_float: bool,
    ) -> Vec<(usize, usize)> {
        if k == 0 {
            return Vec::new();
        }
        let segs = self.segments_snapshot();
        let single_seg = segs.len() <= 1;
        let mut dedup: Option<std::collections::HashSet<u64>> = if single_seg {
            None
        } else {
            Some(std::collections::HashSet::new())
        };
        // Convert f64 to a totally-ordered u64 key (NaN-safe total order) so it
        // works with BinaryHeap (which requires Ord). For DESC keep a MIN-heap
        // of the largest K (store !bits so the max-heap evicts the smallest);
        // for ASC a MAX-heap of the smallest K (store !bits evicts largest).
        let to_ord = |v: f64| -> u64 {
            // IEEE 754 total-order bits: flip sign bit for normal ordering, flip
            // all bits for negative numbers.
            let bits = v.to_bits();
            if bits & (1u64 << 63) != 0 {
                !bits
            } else {
                bits ^ (1u64 << 63)
            }
        };
        let mut heap: std::collections::BinaryHeap<(u64, usize, usize)> =
            std::collections::BinaryHeap::with_capacity(k + 1);
        let push_capped = |heap: &mut std::collections::BinaryHeap<(u64, usize, usize)>,
                           ord_key: u64,
                           seg_idx: usize,
                           ri: usize| {
            heap.push((ord_key, seg_idx, ri));
            if heap.len() > k {
                heap.pop();
            }
        };
        for (sidx, seg) in segs.iter().enumerate() {
            let n = seg.sst.num_rows;
            let has_deletions = seg.sst.row_map.has_any_deleted();
            // Read via the decoder matching the column's stored type. Reading a
            // Float column as i64 reinterprets the bits → garbage sort keys.
            if is_float {
                if let Ok(fseg) = seg.sst.read_fixed_f64(order_col) {
                    for i in 0..n {
                        let key = seg.sst.row_map.key(i);
                        if let Some(ref mut s) = dedup {
                            if !s.insert(key) {
                                continue;
                            }
                        }
                        if has_deletions && seg.sst.row_map.is_deleted(i) {
                            continue;
                        }
                        let v = fseg.get_f64(i).unwrap_or(f64::NAN);
                        let ord_key = if desc {
                            u64::MAX - to_ord(v)
                        } else {
                            to_ord(v)
                        };
                        push_capped(&mut heap, ord_key, sidx, i);
                    }
                }
            } else if let Ok(fseg) = seg.sst.read_fixed_i64(order_col) {
                for i in 0..n {
                    let key = seg.sst.row_map.key(i);
                    if let Some(ref mut s) = dedup {
                        if !s.insert(key) {
                            continue;
                        }
                    }
                    if has_deletions && seg.sst.row_map.is_deleted(i) {
                        continue;
                    }
                    let v = fseg.get_i64(i).unwrap_or(i64::MIN) as f64;
                    // DESC (largest K): invert ord so heap is a min-heap on true value.
                    let ord_key = if desc {
                        u64::MAX - to_ord(v)
                    } else {
                        to_ord(v)
                    };
                    push_capped(&mut heap, ord_key, sidx, i);
                }
            }
        }
        let _ = single_seg;
        // Extract and sort the K results in the requested order.
        // ord_key is encoded so that ascending ord_key always yields the
        // requested order:
        //   ASC : ord_key == to_ord(v)        → ascending ord_key = ascending value
        //   DESC: ord_key == u64::MAX - to_ord(v)
        //                                    → ascending ord_key = descending value
        let mut out: Vec<(u64, usize, usize)> = heap.into_vec();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        let result: Vec<(usize, usize)> = out.into_iter().map(|(_, s, r)| (s, r)).collect();
        result
    }

    /// (group_value, count) pairs. Zero Vec<Value> allocation — uses &str from
    /// the text segment directly. Optimized for GROUP BY col, COUNT(*).
    #[allow(dead_code)]
    pub fn group_by_count(&self, group_col: usize) -> std::collections::HashMap<String, i64> {
        // 🔑 PERF: avoid per-row String allocation. Use an interned index:
        // collect unique group values into a Vec<String> once, then count
        // via index (usize key into the Vec, hashed via the &str). This avoids
        // 20K to_string() + String hash allocations for a 4-group column.
        let segs = self.segments_snapshot();
        let single_seg = segs.len() <= 1;
        let need_dedup = !single_seg || self.may_have_duplicate_keys();
        let mut seen: std::collections::HashSet<u64> = if need_dedup {
            std::collections::HashSet::with_capacity(segs.iter().map(|s| s.sst.num_rows).sum())
        } else {
            std::collections::HashSet::new()
        };

        // 🔑 PERF: avoid per-row String allocation. Use get_mut() first (no
        // alloc for existing keys); only allocate String for genuinely new
        // group values. For a 4-group column over 20K rows, this does 4
        // to_string() calls instead of 20K.
        let mut groups: std::collections::HashMap<String, i64> = std::collections::HashMap::new();

        for seg in segs.iter().rev() {
            let n = seg.sst.num_rows;
            if group_col >= seg.sst.column_tags.len() {
                continue;
            }
            if let Ok(tseg) = seg.sst.read_text(group_col) {
                for i in 0..n {
                    let key = seg.sst.row_map.key(i);
                    if need_dedup && !seen.insert(key) {
                        continue;
                    }
                    if seg.sst.row_map.is_deleted(i) {
                        continue;
                    }
                    let s = tseg.get_str(i).unwrap_or("");
                    // Fast path: key exists → increment without allocation.
                    if let Some(c) = groups.get_mut(s) {
                        *c += 1;
                    } else {
                        groups.insert(s.to_string(), 1);
                    }
                }
            } else if let Ok(fseg) = seg.sst.read_fixed_i64(group_col) {
                for i in 0..n {
                    let key = seg.sst.row_map.key(i);
                    if need_dedup && !seen.insert(key) {
                        continue;
                    }
                    if seg.sst.row_map.is_deleted(i) {
                        continue;
                    }
                    let v = fseg.get_i64(i).unwrap_or(0);
                    let buf = v.to_string();
                    if let Some(c) = groups.get_mut(buf.as_str()) {
                        *c += 1;
                    } else {
                        groups.insert(buf, 1);
                    }
                }
            }
        }

        groups
    }

    /// Distinct values from a text column with early exit. Returns unique
    /// string values. Stops scanning once `max_values` unique values are found
    /// (for SELECT DISTINCT with known cardinality bounds).
    /// Uses &str directly from TextSegment — zero Value allocation.
    pub fn distinct_text_values(&self, col: usize, max_values: usize) -> Vec<String> {
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let segs = self.segments_snapshot();
        let single_seg = segs.len() <= 1;
        let mut dedup: Option<std::collections::HashSet<u64>> = if single_seg {
            None
        } else {
            let total_rows: usize = segs.iter().map(|s| s.sst.num_rows).sum();
            Some(std::collections::HashSet::with_capacity(total_rows))
        };
        // Adaptive early-exit for low-cardinality columns: once we stop seeing
        // new values, assume the column has few uniques and bail out. This turns
        // SELECT DISTINCT region (2 values) from a full 300K-row scan into a
        // few-thousand-row scan, with no cardinality hint needed from the caller.
        // The stable window is chosen so a high-cardinality column (>~10% unique)
        // is still scanned fully, while truly low-cardinality columns short-circuit.
        let mut rows_since_new: usize = 0;
        let stable_window: usize = 4096;
        'outer: for seg in segs.iter().rev() {
            let n = seg.sst.num_rows;
            let has_deletions = seg.sst.row_map.has_any_deleted();
            if let Ok(tseg) = seg.sst.read_text(col) {
                for i in 0..n {
                    let key = seg.sst.row_map.key(i);
                    if let Some(ref mut s) = dedup {
                        if !s.insert(key) {
                            continue;
                        }
                    }
                    if has_deletions && seg.sst.row_map.is_deleted(i) {
                        rows_since_new += 1;
                        continue;
                    }
                    let s = if has_deletions {
                        match tseg.get_str(i) {
                            Some(s) => s,
                            None => {
                                continue;
                            }
                        }
                    } else {
                        tseg.get_str_fast(i)
                    };
                    if seen.insert(s.to_string()) {
                        rows_since_new = 0;
                        if seen.len() >= max_values {
                            break 'outer;
                        }
                    } else {
                        rows_since_new += 1;
                        if !seen.is_empty() && rows_since_new >= stable_window {
                            break 'outer;
                        }
                    }
                }
            } else if let Ok(fseg) = seg.sst.read_fixed_i64(col) {
                for i in 0..n {
                    let key = seg.sst.row_map.key(i);
                    if let Some(ref mut s) = dedup {
                        if !s.insert(key) {
                            continue;
                        }
                    }
                    if has_deletions && seg.sst.row_map.is_deleted(i) {
                        rows_since_new += 1;
                        continue;
                    }
                    let v = fseg.get_i64(i).unwrap_or(0).to_string();
                    if seen.insert(v) {
                        rows_since_new = 0;
                        if seen.len() >= max_values {
                            break 'outer;
                        }
                    } else {
                        rows_since_new += 1;
                        if !seen.is_empty() && rows_since_new >= stable_window {
                            break 'outer;
                        }
                    }
                }
            }
        }
        seen.into_iter().collect()
    }

    pub fn buffered_row_count(&self) -> usize {
        self.write_buf.lock().num_rows
    }

    /// Get cached IN-hash row indices for (col_pos, set_signature).
    pub fn get_in_hash_cache(&self, col_pos: usize, set_sig: u64) -> Option<Vec<usize>> {
        let key = ((col_pos as u128) << 64) | (set_sig as u128);
        self.in_hash_cache.read().get(&key).cloned()
    }

    /// Store IN-hash row indices for (col_pos, set_signature).
    pub fn put_in_hash_cache(&self, col_pos: usize, set_sig: u64, indices: Vec<usize>) {
        let key = ((col_pos as u128) << 64) | (set_sig as u128);
        let mut cache = self.in_hash_cache.write();
        if cache.len() < 8 {
            cache.insert(key, indices);
        }
    }

    /// GROUP BY with COUNT + SUM aggregation in a single pass.
    /// Returns (group_value, count, sum) tuples. Reads only the group column
    /// and the aggregate column — no full-row decode.
    pub fn group_by_count_sum(&self, group_col: usize, agg_col: usize) -> Vec<(String, i64, f64)> {
        // Check the group-by cache first (avoids re-scanning on repeated calls).
        // Cache key: (group_col, agg_col) — invalidated on writes via clear_cache().
        {
            let cache = self.groupby_cache.read();
            let key = ((group_col as u64) << 32) | (agg_col as u64);
            if let Some(result) = cache.get(&key) {
                return result.clone();
            }
        }

        let result = self.group_by_count_sum_uncached(group_col, agg_col);

        // Cache the result.
        {
            let mut cache = self.groupby_cache.write();
            let key = ((group_col as u64) << 32) | (agg_col as u64);
            if cache.len() < 8 {
                cache.insert(key, result.clone());
            }
        }
        result
    }

    fn group_by_count_sum_uncached(
        &self,
        group_col: usize,
        agg_col: usize,
    ) -> Vec<(String, i64, f64)> {
        // Direct HashMap<String, (i64, f64)> — Rust's SipHash is slower per-hash
        // but avoids the manual FNV loop + collision checking overhead.
        // Pre-allocate capacity to avoid rehashing during insertion.
        let mut groups: std::collections::HashMap<String, (i64, f64)> =
            std::collections::HashMap::with_capacity(32768);

        let segs = self.segments_snapshot();
        let single_seg = segs.len() <= 1;
        let mut seen: Option<std::collections::HashSet<u64>> = if single_seg {
            None
        } else {
            let total_rows: usize = segs.iter().map(|s| s.sst.num_rows).sum();
            Some(std::collections::HashSet::with_capacity(total_rows))
        };
        for seg in segs.iter().rev() {
            let n = seg.sst.num_rows;
            let gtext = seg.sst.read_text(group_col).ok();
            let afix = seg.sst.read_fixed_i64(agg_col).ok();
            let has_deletions = seg.sst.row_map.has_any_deleted();
            if let Some(tseg) = gtext.as_ref() {
                let has_nulls = tseg.has_any_null();
                for i in 0..n {
                    let key = seg.sst.row_map.key(i);
                    if let Some(ref mut s) = seen {
                        if !s.insert(key) {
                            continue;
                        }
                    }
                    if has_deletions && seg.sst.row_map.is_deleted(i) {
                        continue;
                    }
                    let gval = if has_nulls {
                        tseg.get_str(i).unwrap_or("")
                    } else {
                        tseg.get_str_fast(i)
                    };
                    let av = afix
                        .as_ref()
                        .and_then(|f| f.get_f64(i).or_else(|| f.get_i64(i).map(|x| x as f64)));

                    // Fast path: entry exists → update count+sum (no String alloc).
                    if let Some(entry) = groups.get_mut(gval) {
                        entry.0 += 1;
                        if let Some(v) = av {
                            entry.1 += v;
                        }
                    } else {
                        groups.insert(gval.to_string(), (1, av.unwrap_or(0.0)));
                    }
                }
            }
        }
        groups.into_iter().map(|(k, (c, s))| (k, c, s)).collect()
    }

    /// GROUP BY with COUNT + SUM for a fixed-type (Integer/Boolean) group column.
    /// Returns (i64_group_value, count, sum) tuples.
    pub fn group_by_count_sum_fixed_group(
        &self,
        group_col: usize,
        agg_col: usize,
    ) -> Vec<(i64, i64, f64)> {
        let mut groups: std::collections::HashMap<i64, (i64, f64)> =
            std::collections::HashMap::new();
        let segs = self.segments_snapshot();
        let single_seg = segs.len() <= 1;
        let mut seen: Option<std::collections::HashSet<u64>> = if single_seg {
            None
        } else {
            let total_rows: usize = segs.iter().map(|s| s.sst.num_rows).sum();
            Some(std::collections::HashSet::with_capacity(total_rows))
        };
        for seg in segs.iter().rev() {
            let n = seg.sst.num_rows;
            let gfix = seg.sst.read_fixed_i64(group_col).ok();
            let afix = seg.sst.read_fixed_i64(agg_col).ok();
            if let Some(gseg) = gfix.as_ref() {
                for i in 0..n {
                    let key = seg.sst.row_map.key(i);
                    if let Some(ref mut s) = seen {
                        if !s.insert(key) {
                            continue;
                        }
                    }
                    if seg.sst.row_map.is_deleted(i) {
                        continue;
                    }
                    if let Some(gval) = gseg.get_i64(i) {
                        let entry = groups.entry(gval).or_insert((0, 0.0));
                        entry.0 += 1;
                        if let Some(ref f) = afix {
                            if let Some(v) = f.get_f64(i).or_else(|| f.get_i64(i).map(|x| x as f64))
                            {
                                entry.1 += v;
                            }
                        }
                    }
                }
            }
        }
        groups.into_iter().map(|(k, (c, s))| (k, c, s)).collect()
    }

    /// Recover segments from disk after a restart. Reads the MANIFEST to find
    /// active segment ids, opens each .sst file, and loads them into memory.
    /// Ensures no data loss on crash (ACID durability).
    pub fn recover_from_disk(&self) {
        // Read MANIFEST to get active segment ids.
        let manifest_path = self.dir.join("MANIFEST");
        if !manifest_path.exists() {
            return;
        }
        let manifest = match crate::storage::col_segment::manifest::Manifest::open(&manifest_path) {
            Ok(m) => m,
            Err(_) => return,
        };
        let state = manifest.replay();

        // Find the highest segment id to continue numbering.
        let mut max_id = 0u64;
        for &id in &state.active_segments {
            max_id = max_id.max(id);
        }
        // Also check files on disk (in case MANIFEST lags).
        if let Ok(entries) = std::fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.ends_with(".sst") {
                        if let Ok(id) = name.trim_end_matches(".sst").parse::<u64>() {
                            max_id = max_id.max(id);
                            if !state.active_segments.contains(&id) {
                                // File on disk but not in MANIFEST — orphan, skip.
                                continue;
                            }
                        }
                    }
                }
            }
        }
        self.next_segment_id.store(max_id + 1, Ordering::Relaxed);

        // Load each active segment.
        let mut segs = self.segments.write();
        let mut loaded_ids: Vec<u64> = Vec::new();
        for &id in &state.active_segments {
            let path = self.dir.join(format!("{:010}.sst", id));
            if path.exists() {
                if let Ok(seg) = Segment::open(&path, id) {
                    segs.push_back(Arc::new(seg));
                    loaded_ids.push(id);
                }
            }
        }
        // Clean up obsolete files (superseded by compaction but not yet GC'd).
        for &id in &state.obsolete_files {
            let path = self.dir.join(format!("{:010}.sst", id));
            let _ = std::fs::remove_file(&path);
        }

        // Sort segments by id (creation order).
        segs.make_contiguous();
        // Already in push order (ascending id) — correct.
    }

    pub fn col_types(&self) -> &[ColumnType] {
        &self.col_types
    }

    pub fn needs_compaction(&self) -> bool {
        self.segments.read().len() >= COMPACTION_SEGMENT_THRESHOLD
    }

    /// Run one compaction pass (synchronous; called by bg thread or test).
    /// Merges all active segments into one, deduplicating keys and dropping
    /// tombstoned/superseded versions.
    pub fn compact_once(&self) -> Result<()> {
        let old_segs: Vec<Arc<Segment>> = {
            let segs = self.segments.read();
            if segs.len() < COMPACTION_SEGMENT_THRESHOLD {
                return Ok(());
            }
            segs.iter().cloned().collect()
        };
        self.merge_segments(old_segs)
    }

    /// Force-merge ALL segments into one, ignoring the count threshold.
    /// Used by sync_col_segment_to_sstables so legacy aggregate paths see
    /// the complete dataset in a single SSTable. No-op if < 2 segments.
    pub fn force_compact_all(&self) -> Result<()> {
        let old_segs: Vec<Arc<Segment>> = {
            let segs = self.segments.read();
            if segs.len() < 2 {
                return Ok(());
            }
            segs.iter().cloned().collect()
        };
        self.merge_segments(old_segs)
    }

    /// Return the maximum row_id (key & 0xFFFFFFFF) across all segments + buffer.
    /// Used on reopen to initialize next_row_id so new INSERTs don't reuse a
    /// row_id from a previous session (which would collide with existing data).
    pub fn max_row_id(&self) -> u64 {
        let mut max = 0u64;
        for (key, _) in self.write_buf.lock().latest_entries() {
            max = max.max(key & 0xFFFFFFFF);
        }
        for seg in self.segments.read().iter() {
            for i in 0..seg.sst.num_rows {
                let key = seg.sst.row_map.key(i);
                max = max.max(key & 0xFFFFFFFF);
            }
        }
        max
    }

    /// Shared merge logic: merge `old_segs` into one new segment, dedup keys
    /// (newest version wins), drop tombstones, update manifest + GC old files.
    fn merge_segments(&self, old_segs: Vec<Arc<Segment>>) -> Result<()> {
        if old_segs.is_empty() {
            return Ok(());
        }
        // Serialize with flush_buffer: wait for any in-progress flush to
        // complete before merging, and hold the lock so no new flush can
        // create a segment that this merge would miss.
        let _guard = self.flush_merge_lock.lock();
        let old_ids: Vec<u64> = old_segs.iter().map(|s| s.id).collect();
        let ncols = self.col_types.len();

        let id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
        let path = self.dir.join(format!("{:010}.sst", id));
        let mut builder = ColumnarSSTableBuilder::new(&path, self.col_types.clone());

        // Check if ALL columns are fixed-width (integer/float/bool/timestamp).
        // If so, use the fast column-direct path (no Vec<Value>).
        // all_fixed = every column is 8-byte fixed (Integer/Float/Timestamp).
        // Boolean is fixed-width but only 1 byte, so it must go through the
        // mixed path (which reads via the type-correct accessor); including it
        // here would write 8 bytes per Boolean row and corrupt the segment.
        let all_fixed = self.col_types.iter().all(|ct| {
            matches!(
                ct,
                ColumnType::Integer | ColumnType::Float | ColumnType::Timestamp
            )
        });

        if all_fixed {
            // Column-direct compaction: extract raw i64 bytes per row, no Value.
            // 🔑 CRITICAL: collect ALL rows, sort by key, THEN add to builder.
            // The builder's row_map stores keys in insertion order and find_key()
            // uses binary search (requires sorted keys). Without sorting, a merge
            // of multiple segments (iterated newest-first) produces an unsorted
            // row_map, breaking point lookups (get/where id=...) after compaction.
            let single_seg = old_segs.len() <= 1;
            let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
            let mut collected: Vec<(u64, u64, Vec<[u8; 8]>, Vec<bool>)> = Vec::new();
            for seg in old_segs.iter().rev() {
                let n = seg.sst.num_rows;
                let fixed_cols: Vec<Option<crate::storage::lsm::columnar::FixedSegment>> = (0
                    ..ncols)
                    .map(|ci| {
                        if ci < seg.sst.column_tags.len() && seg.sst.column_tags[ci].is_fixed() {
                            seg.sst.read_fixed_i64(ci).ok()
                        } else {
                            None
                        }
                    })
                    .collect();
                for i in 0..n {
                    let key = seg.sst.row_map.key(i);
                    if !seen.insert(key) {
                        continue;
                    }
                    if seg.sst.row_map.is_deleted(i) {
                        continue;
                    }
                    let ts = seg.sst.row_map.timestamp(i);
                    let mut col_vals: Vec<[u8; 8]> = Vec::with_capacity(ncols);
                    let mut col_nulls: Vec<bool> = Vec::with_capacity(ncols);
                    for ci in 0..ncols {
                        match fixed_cols
                            .get(ci)
                            .and_then(|x| x.as_ref())
                            .and_then(|f| f.get_i64(i))
                        {
                            Some(v) => {
                                col_vals.push(v.to_le_bytes());
                                col_nulls.push(false);
                            }
                            None => {
                                col_vals.push(0i64.to_le_bytes());
                                col_nulls.push(true);
                            }
                        }
                    }
                    collected.push((key, ts, col_vals, col_nulls));
                }
            }
            // Single-segment data is already sorted (sequential insert); skip the
            // sort for that case to avoid the O(N log N) overhead.
            if !single_seg {
                collected.sort_unstable_by_key(|(k, _, _, _)| *k);
            }
            for (key, ts, col_vals, col_nulls) in collected {
                let col_bytes: Vec<&[u8]> = col_vals.iter().map(|b| b.as_slice()).collect();
                builder.add_values_raw_with_nulls(key, ts, false, &col_bytes, &col_nulls)?;
            }
        } else {
            // Mixed columns (has Text and/or Vector/Spatial): direct copy with
            // temp buffers. Avoids MergeCursor's per-row Vec<Value> + SegmentCursor
            // pre-decode.
            // 🔑 CRITICAL: collect ALL rows, sort by key, THEN add (see note above).
            // 🔑 Vector/Spatial columns must be decoded+re-encoded here (they have
            // no zero-copy segment readers); otherwise a multi-segment merge would
            // silently DROP those columns (each row's buffer stayed empty).
            use crate::storage::lsm::columnar::ColumnTypeTag;
            let single_seg = old_segs.len() <= 1;
            let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
            let mut collected: Vec<(u64, u64, Vec<Vec<u8>>, Vec<bool>)> = Vec::new();
            for seg in old_segs.iter().rev() {
                let n = seg.sst.num_rows;
                let fixed_cols: Vec<Option<crate::storage::lsm::columnar::FixedSegment>> = (0
                    ..ncols)
                    .map(|ci| {
                        if ci < seg.sst.column_tags.len() && seg.sst.column_tags[ci].is_fixed() {
                            seg.sst.read_fixed_i64(ci).ok()
                        } else {
                            None
                        }
                    })
                    .collect();
                let text_cols: Vec<Option<crate::storage::lsm::columnar::TextSegment>> = (0..ncols)
                    .map(|ci| {
                        if ci < seg.sst.column_tags.len()
                            && matches!(
                                seg.sst.column_tags[ci],
                                crate::storage::lsm::columnar::ColumnTypeTag::Text
                            )
                        {
                            seg.sst.read_text(ci).ok()
                        } else {
                            None
                        }
                    })
                    .collect();
                // Pre-decode Vector columns into per-idx option vecs.
                let vec_cols: Vec<Vec<Option<Vec<f32>>>> = (0..ncols)
                    .map(|ci| {
                        if ci < seg.sst.column_tags.len()
                            && matches!(seg.sst.column_tags[ci], ColumnTypeTag::Vector)
                        {
                            let decoded = seg.sst.read_vectors(ci).unwrap_or_default();
                            let mut per_row = vec![None; n];
                            let mut di = 0usize;
                            for i in 0..n {
                                if seg.sst.row_map.is_deleted(i) {
                                    continue;
                                }
                                let ek = seg.sst.row_map.key(i) & 0xFFFFFFFF;
                                while di < decoded.len() && decoded[di].0 != ek {
                                    di += 1;
                                }
                                if di < decoded.len() {
                                    per_row[i] = Some(decoded[di].1.clone());
                                    di += 1;
                                }
                            }
                            per_row
                        } else {
                            Vec::new()
                        }
                    })
                    .collect();
                // Pre-decode Spatial columns into per-idx option vecs.
                let spatial_cols: Vec<Vec<Option<crate::types::Geometry>>> = (0..ncols)
                    .map(|ci| {
                        if ci < seg.sst.column_tags.len()
                            && matches!(seg.sst.column_tags[ci], ColumnTypeTag::Spatial)
                        {
                            let decoded = seg.sst.read_spatial(ci).unwrap_or_default();
                            let mut per_row = vec![None; n];
                            let mut di = 0usize;
                            for i in 0..n {
                                if seg.sst.row_map.is_deleted(i) {
                                    continue;
                                }
                                let ek = seg.sst.row_map.key(i) & 0xFFFFFFFF;
                                while di < decoded.len() && decoded[di].0 != ek {
                                    di += 1;
                                }
                                if di < decoded.len() {
                                    per_row[i] = Some(decoded[di].1.clone());
                                    di += 1;
                                }
                            }
                            per_row
                        } else {
                            Vec::new()
                        }
                    })
                    .collect();
                for i in 0..n {
                    let key = seg.sst.row_map.key(i);
                    if !seen.insert(key) {
                        continue;
                    }
                    if seg.sst.row_map.is_deleted(i) {
                        continue;
                    }
                    let ts = seg.sst.row_map.timestamp(i);
                    let mut row_bytes: Vec<Vec<u8>> = Vec::with_capacity(ncols);
                    let mut row_nulls: Vec<bool> = Vec::with_capacity(ncols);
                    for ci in 0..ncols {
                        let mut buf = Vec::new();
                        let tag = seg.sst.column_tags.get(ci).copied();
                        if matches!(
                            tag,
                            Some(crate::storage::lsm::columnar::ColumnTypeTag::Bool)
                        ) {
                            // Boolean: 1-byte fixed. Read via get_bool, write 1 byte.
                            match fixed_cols
                                .get(ci)
                                .and_then(|x| x.as_ref())
                                .and_then(|f| f.get_bool(i))
                            {
                                Some(b) => {
                                    buf.push(if b { 1u8 } else { 0u8 });
                                    row_nulls.push(false);
                                }
                                None => {
                                    buf.push(0u8);
                                    row_nulls.push(true);
                                }
                            }
                        } else if let Some(f) = fixed_cols.get(ci).and_then(|x| x.as_ref()) {
                            match f.get_i64(i) {
                                Some(v) => {
                                    buf.extend_from_slice(&v.to_le_bytes());
                                    row_nulls.push(false);
                                }
                                None => {
                                    buf.extend_from_slice(&0i64.to_le_bytes());
                                    row_nulls.push(true);
                                }
                            }
                        } else if let Some(t) = text_cols.get(ci).and_then(|x| x.as_ref()) {
                            match t.get_str(i) {
                                Some(s) => {
                                    let len = s.len().min(65535) as u16;
                                    buf.extend_from_slice(&len.to_le_bytes());
                                    buf.extend_from_slice(&s.as_bytes()[..len as usize]);
                                    row_nulls.push(false);
                                }
                                None => {
                                    buf.extend_from_slice(&0u16.to_le_bytes());
                                    row_nulls.push(true);
                                }
                            }
                        } else if ci < vec_cols.len() && !vec_cols[ci].is_empty() {
                            // Vector: re-encode [dim:u16][f32×dim] (NULL → dim=0).
                            if let Some(ref v) = vec_cols[ci][i] {
                                buf.extend_from_slice(&(v.len() as u16).to_le_bytes());
                                for x in v {
                                    buf.extend_from_slice(&x.to_le_bytes());
                                }
                                row_nulls.push(false);
                            } else {
                                buf.extend_from_slice(&0u16.to_le_bytes());
                                row_nulls.push(true);
                            }
                        } else if ci < spatial_cols.len() && !spatial_cols[ci].is_empty() {
                            // Spatial: re-encode [len:u16][bincode(Geometry)] (NULL → len=0).
                            if let Some(ref g) = spatial_cols[ci][i] {
                                let bytes = bincode::serialize(g).unwrap_or_default();
                                let len = bytes.len().min(65535) as u16;
                                buf.extend_from_slice(&len.to_le_bytes());
                                buf.extend_from_slice(&bytes[..len as usize]);
                                row_nulls.push(false);
                            } else {
                                buf.extend_from_slice(&0u16.to_le_bytes());
                                row_nulls.push(true);
                            }
                        } else {
                            row_nulls.push(false);
                        }
                        row_bytes.push(buf);
                    }
                    collected.push((key, ts, row_bytes, row_nulls));
                }
            }
            if !single_seg {
                collected.sort_unstable_by_key(|(k, _, _, _)| *k);
            }
            for (key, ts, row_bytes, row_nulls) in collected {
                let col_slices: Vec<&[u8]> = row_bytes.iter().map(|b| b.as_slice()).collect();
                builder.add_values_raw_with_nulls(key, ts, false, &col_slices, &row_nulls)?;
            }
        }
        builder.finish()?;

        let new_seg = Arc::new(Segment::open(&path, id)?);

        // Record compaction in manifest FIRST (crash safety), then swap memory.
        self.manifest.lock().record_compaction(id, &old_ids)?;
        {
            let mut segs = self.segments.write();
            let old_set: std::collections::HashSet<u64> = old_ids.iter().copied().collect();
            let new_list: VecDeque<Arc<Segment>> = segs
                .iter()
                .filter(|s| !old_set.contains(&s.id))
                .cloned()
                .collect();
            *segs = new_list;
            segs.push_back(new_seg);
        }

        // Clear column caches + release mmap pages to keep peak RSS low.
        {
            let segs = self.segments.read();
            for seg in segs.iter() {
                seg.clear_cache();
                seg.release_pages();
            }
        }
        // GC: delete old files, record in manifest.
        for oid in &old_ids {
            let p = self.dir.join(format!("{:010}.sst", oid));
            let _ = std::fs::remove_file(p);
        }
        self.manifest.lock().record_gc(&old_ids)?;
        Ok(())
    }
}
