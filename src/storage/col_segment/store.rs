use super::segment::Segment;
use super::merge::MergeCursor;
use super::manifest::Manifest;
use crate::storage::lsm::columnar::ColumnarSSTableBuilder;
use crate::types::{ColumnType, Value};
use crate::Result;
use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Compaction trigger: merge when segment count reaches this.
const COMPACTION_SEGMENT_THRESHOLD: usize = 3;

/// Append-only multi-segment store for one columnar table.
pub struct ColSegmentStore {
    table_name: String,
    dir: PathBuf,
    /// Active segments in ascending creation order (oldest first, newest at back).
    segments: RwLock<VecDeque<Arc<Segment>>>,
    /// In-memory write buffer. Flushed as a delta segment (does not read old data).
    write_buf: Mutex<ColumnarSSTableBuilder>,
    next_segment_id: AtomicU64,
    manifest: Mutex<Manifest>,
    col_types: Vec<ColumnType>,
}

impl ColSegmentStore {
    /// Create a new store for a table at `base_dir/columnar_ms/<table_name>/`.
    /// (`columnar_ms` to avoid clashing with the time-series `columnar/` dir.)
    pub fn create(base_dir: &Path, table_name: &str, col_types: Vec<ColumnType>) -> Result<Arc<Self>> {
        let dir = base_dir.join("columnar_ms").join(table_name);
        std::fs::create_dir_all(&dir)?;
        let manifest_path = dir.join("MANIFEST");
        let manifest = if manifest_path.exists() {
            Manifest::open(&manifest_path)?
        } else {
            Manifest::create(&manifest_path)?
        };
        let buf_path = dir.join(".writebuf.tmp");
        let write_buf = ColumnarSSTableBuilder::new(&buf_path, col_types.clone());
        Ok(Arc::new(Self {
            table_name: table_name.to_string(),
            dir,
            segments: RwLock::new(VecDeque::new()),
            write_buf: Mutex::new(write_buf),
            next_segment_id: AtomicU64::new(1),
            manifest: Mutex::new(manifest),
            col_types,
        }))
    }

    /// Append rows to the in-memory buffer. O(rows). Each tuple: (key, timestamp, values).
    pub fn append_rows(&self, rows: &[(u64, u64, Vec<Value>)]) -> Result<()> {
        let mut buf = self.write_buf.lock();
        for (key, ts, row) in rows {
            buf.add_values(*key, *ts, false, row)?;
        }
        Ok(())
    }

    /// Append a tombstone (deletion marker) for a key. The tombstone suppresses
    /// the row in multi-segment scans (newest-version-wins with deleted=true).
    pub fn append_tombstone(&self, key: u64, ts: u64) -> Result<()> {
        let mut buf = self.write_buf.lock();
        // Write placeholder values for each column (keeps column_buffers in sync
        // with num_rows). The actual values are never read for deleted rows.
        let placeholder: Vec<Value> = self.col_types.iter().map(|_| Value::Null).collect();
        buf.add_values(key, ts, true, &placeholder)?;
        Ok(())
    }

    /// Flush the buffer to a new delta segment on disk. Does NOT read old segments.
    /// O(this batch). Writes the file (no fsync — durability via WAL/manifest).
    pub fn flush_buffer(&self) -> Result<()> {
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
        Ok(())
    }

    /// Point lookup: newest segment first, return first hit.
    /// Uses per-segment column decode cache — first access decompresses each
    /// column once, subsequent lookups (incl. other keys) reuse the cache.
    pub fn get(&self, key: u64) -> Option<Vec<Value>> {
        let segs = self.segments.read();
        for seg in segs.iter().rev() {
            if let Some(row) = seg.get_row_cached(key, &self.col_types) {
                return Some(row);
            }
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
        // Pre-estimate result size to avoid Vec reallocations (300K rows = ~18 reallocs otherwise).
        let total_rows: usize = self.segments.read().iter().map(|s| s.sst.num_rows).sum();
        let mut result: Vec<(u64, Vec<Value>)> = Vec::with_capacity(total_rows);
        let segs = self.segments_snapshot();
        let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::with_capacity(total_rows);

        for seg in segs.iter().rev() {
            let n = seg.sst.num_rows;
            let mut order: Vec<usize> = (0..n).collect();
            order.sort_by_key(|&i| seg.sst.row_map.key(i));

            // Pre-decode filter column (once per segment).
            let fcol_fixed = filter_col.and_then(|fc| {
                if fc < seg.sst.column_tags.len() && seg.sst.column_tags[fc].is_fixed() {
                    seg.sst.read_fixed_i64(fc).ok()
                } else { None }
            });
            let fcol_text = filter_col.and_then(|fc| {
                if fc < seg.sst.column_tags.len() && !seg.sst.column_tags[fc].is_fixed() {
                    seg.sst.read_text(fc).ok()
                } else { None }
            });
            let fcol_type = filter_col.and_then(|fc| self.col_types.get(fc));

            // Pre-intern filter Text column into ArcString vec to avoid per-row
            // String allocation in the predicate (WHERE/LIKE on text cols).
            let fcol_text_interned: Vec<Option<Value>> = if let Some(ref t) = fcol_text {
                (0..n).map(|i| {
                    if t.is_null(i) { return None; }
                    t.get_str(i).map(|s| Value::Text(crate::types::ArcString(std::sync::Arc::from(s))))
                }).collect()
            } else { Vec::new() };

            // Pre-decode project columns (once per segment).
            let pfixed: Vec<Option<crate::storage::lsm::columnar::FixedSegment>> = project_cols.iter().map(|&pc| {
                if pc < seg.sst.column_tags.len() && seg.sst.column_tags[pc].is_fixed() {
                    seg.sst.read_fixed_i64(pc).ok()
                } else { None }
            }).collect();
            let ptext: Vec<Option<crate::storage::lsm::columnar::TextSegment>> = project_cols.iter().map(|&pc| {
                if pc < seg.sst.column_tags.len() && !seg.sst.column_tags[pc].is_fixed() {
                    seg.sst.read_text(pc).ok()
                } else { None }
            }).collect();

            // Lazy text decode: do NOT pre-intern all rows (saves 300K ArcString
            // allocations = ~20MB). Decode only matched rows on demand below.
            let ptext_interned: Vec<Vec<Option<Value>>> = Vec::new();

            for &i in &order {
                let key = seg.sst.row_map.key(i);
                // Mark key as seen BEFORE checking deleted, so tombstones suppress
                // older versions of the same key in earlier segments.
                if !seen.insert(key) { continue; }
                if seg.sst.row_map.is_deleted(i) { continue; }

                // Decode filter value only (cheap: single column lookup).
                let fval: Option<Value> = if let Some(fc) = filter_col {
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
                        t.get_str(i).map(|s| Value::Text(s.to_string().into()))
                    } else { None };
                    v
                } else { None };

                if !predicate(fval.as_ref()) { continue; }

                // Decode output columns for matching row only.
                let mut row = Vec::with_capacity(project_cols.len());
                for (pi, &pc) in project_cols.iter().enumerate() {
                    let v = if pc < self.col_types.len() {
                        match (&pfixed.get(pi), &ptext.get(pi), &self.col_types[pc]) {
                            (Some(Some(f)), _, ColumnType::Integer) => f.get_i64(i).map(Value::Integer),
                            (Some(Some(f)), _, ColumnType::Float) => f.get_f64(i).map(Value::Float),
                            (Some(Some(f)), _, ColumnType::Boolean) => f.get_bool(i).map(Value::Bool),
                            (_, Some(Some(t)), ColumnType::Text) => {
                                if !ptext_interned.is_empty() {
                                    ptext_interned.get(pi).and_then(|v| v.get(i)).cloned().flatten()
                                } else {
                                    t.get_str(i).map(|s| Value::Text(s.to_string().into()))
                                }
                            }
                            _ => Some(Value::Null),
                        }
                    } else { Some(Value::Null) };
                    row.push(v.unwrap_or(Value::Null));
                }
                result.push((key, row));
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
        let total_rows: usize = self.segments.read().iter().map(|s| s.sst.num_rows).sum();
        let mut result: Vec<(u64, Vec<Value>)> = Vec::with_capacity(total_rows.min(65536));
        let segs = self.segments_snapshot();
        let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::with_capacity(total_rows);

        for seg in segs.iter().rev() {
            let n = seg.sst.num_rows;
            let mut order: Vec<usize> = (0..n).collect();
            order.sort_by_key(|&i| seg.sst.row_map.key(i));

            // Filter column: read text segment once, predicate gets &str directly.
            let ftext = seg.sst.read_text(filter_col).ok();

            // Output fixed columns pre-decoded.
            let pfixed: Vec<Option<crate::storage::lsm::columnar::FixedSegment>> = project_cols.iter().map(|&pc| {
                if pc < seg.sst.column_tags.len() && seg.sst.column_tags[pc].is_fixed() {
                    seg.sst.read_fixed_i64(pc).ok()
                } else { None }
            }).collect();
            // Output text columns: pre-intern into ArcString for clone-on-match.
            let ptext_cols: Vec<Option<crate::storage::lsm::columnar::TextSegment>> = project_cols.iter().map(|&pc| {
                if pc < seg.sst.column_tags.len() && !seg.sst.column_tags[pc].is_fixed() {
                    seg.sst.read_text(pc).ok()
                } else { None }
            }).collect();
            // Per-column dedup cache for output text values.
            let mut text_dedup: Vec<std::collections::HashMap<&str, std::sync::Arc<str>>> =
                ptext_cols.iter().map(|_| std::collections::HashMap::with_capacity(64)).collect();

            for &i in &order {
                let key = seg.sst.row_map.key(i);
                // Mark key as seen BEFORE checking deleted, so tombstones suppress
                // older versions of the same key in earlier segments.
                if !seen.insert(key) { continue; }
                if seg.sst.row_map.is_deleted(i) { continue; }

                // Filter: pass raw &str to predicate (zero Value allocation).
                let fval = ftext.as_ref().and_then(|t| {
                    if t.is_null(i) { None } else { t.get_str(i) }
                });
                if !str_predicate(fval) { continue; }

                // Decode output columns for this matched row.
                let mut row = Vec::with_capacity(project_cols.len());
                for (pi, &pc) in project_cols.iter().enumerate() {
                    let v = if pc < self.col_types.len() {
                        match (&pfixed.get(pi), &ptext_cols.get(pi), &self.col_types[pc]) {
                            (Some(Some(f)), _, ColumnType::Integer) => f.get_i64(i).map(Value::Integer),
                            (Some(Some(f)), _, ColumnType::Float) => f.get_f64(i).map(Value::Float),
                            (Some(Some(f)), _, ColumnType::Boolean) => f.get_bool(i).map(Value::Bool),
                            (_, Some(Some(t)), ColumnType::Text) => {
                                if t.is_null(i) { Some(Value::Null) }
                                else { t.get_str(i).map(|s| {
                                    // Dedup: reuse Arc for repeated text values.
                                    let cache = &mut text_dedup[pi];
                                    let arc = cache.get(s).cloned().unwrap_or_else(|| {
                                        let a: std::sync::Arc<str> = std::sync::Arc::from(s);
                                        cache.insert(s, std::sync::Arc::clone(&a));
                                        a
                                    });
                                    Value::Text(crate::types::ArcString(arc))
                                }) }
                            }
                            _ => Some(Value::Null),
                        }
                    } else { Some(Value::Null) };
                    row.push(v.unwrap_or(Value::Null));
                }
                result.push((key, row));
            }
        }
        result
    }

    pub fn segment_count(&self) -> usize {
        self.segments.read().len()
    }

    /// Snapshot of active segments (oldest→newest). Callers iterate directly
    /// for single-column reads (e.g. CREATE INDEX) without full-row decode.
    pub fn segments_snapshot(&self) -> Vec<Arc<Segment>> {
        self.segments.read().iter().cloned().collect()
    }

    /// After flush+compaction to a single segment, return that segment's SSTable
    /// as a shared Arc. Legacy read paths (aggregate, GROUP BY) read
    /// `columnar_sstables: DashMap<String, Arc<ColumnarSSTable>>`; this lets them
    /// observe the same SSTable without cloning (Arc shared). Returns None if
    /// the store has no segments.
    pub fn latest_segment_sst(&self) -> Option<Arc<crate::storage::lsm::columnar::ColumnarSSTable>> {
        self.segments.read().back().map(|seg| Arc::clone(&seg.sst))
    }

    /// Number of rows currently buffered in memory (not yet flushed to a segment).
    /// Count live (non-deleted, non-duplicated) rows across all segments.
    /// O(total_rows) but zero Value decode — fast for COUNT(*).
    pub fn count_live_rows(&self) -> usize {
        // Count buffered (unflushed) rows first.
        let buffered = self.write_buf.lock().num_rows;
        let segs = self.segments.read();
        let mut seen = std::collections::HashSet::new();
        let mut count = buffered;
        for seg in segs.iter().rev() {
            for i in 0..seg.sst.num_rows {
                let key = seg.sst.row_map.key(i);
                if !seen.insert(key) { continue; }
                if seg.sst.row_map.is_deleted(i) { continue; }
                count += 1;
            }
        }
        count
    }

    /// Group-by scan: iterate the group column directly (TextSegment), returning
    /// Count + Sum with a text filter: iterate filter col (TextSegment) + sum col
    /// (FixedSegment) directly. Returns (count, sum). Zero Vec<Value> allocation.
    /// Optimized for SELECT COUNT(*), SUM(col) WHERE text_col = 'val'.
    pub fn count_sum_text_filter(&self, filter_col: usize, filter_val: &str, sum_col: usize) -> (i64, f64) {
        let segs = self.segments_snapshot();
        let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
        let mut count = 0i64;
        let mut sum = 0.0f64;
        for seg in segs.iter().rev() {
            let n = seg.sst.num_rows;
            let ftext = seg.sst.read_text(filter_col).ok();
            let fsum = seg.sst.read_fixed_i64(sum_col).ok();
            if let Some(tseg) = ftext.as_ref() {
                for i in 0..n {
                    let key = seg.sst.row_map.key(i);
                    if !seen.insert(key) { continue; }
                    if seg.sst.row_map.is_deleted(i) { continue; }
                    if tseg.get_str(i) == Some(filter_val) {
                        count += 1;
                        if let Some(ref f) = fsum {
                            if let Some(v) = f.get_f64(i) { sum += v; }
                            else if let Some(v) = f.get_i64(i) { sum += v as f64; }
                        }
                    }
                }
            }
        }
        (count, sum)
    }

    /// Count + Min + Max with a text filter. Returns (count, min, max).
    pub fn count_min_max_text_filter(&self, filter_col: usize, filter_val: &str, agg_col: usize) -> (i64, f64, f64) {
        let segs = self.segments_snapshot();
        let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
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
                    if !seen.insert(key) { continue; }
                    if seg.sst.row_map.is_deleted(i) { continue; }
                    if tseg.get_str(i) == Some(filter_val) {
                        count += 1;
                        if let Some(ref f) = fagg {
                            let v = f.get_f64(i).unwrap_or_else(|| f.get_i64(i).map(|i| i as f64).unwrap_or(0.0));
                            min = min.min(v);
                            max = max.max(v);
                        }
                    }
                }
            }
        }
        (count, min.max(f64::NEG_INFINITY), max.min(f64::INFINITY))
    }

    /// (group_value, count) pairs. Zero Vec<Value> allocation — uses &str from
    /// the text segment directly. Optimized for GROUP BY col, COUNT(*).
    pub fn group_by_count(&self, group_col: usize) -> std::collections::HashMap<String, i64> {
        let mut groups: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
        let segs = self.segments_snapshot();
        let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for seg in segs.iter().rev() {
            let n = seg.sst.num_rows;
            if let Ok(tseg) = seg.sst.read_text(group_col) {
                for i in 0..n {
                let key = seg.sst.row_map.key(i);
                if !seen.insert(key) { continue; }
                if seg.sst.row_map.is_deleted(i) { continue; }
                let gval = tseg.get_str(i).unwrap_or("").to_string();
                    *groups.entry(gval).or_insert(0) += 1;
                }
            } else if let Ok(fseg) = seg.sst.read_fixed_i64(group_col) {
                for i in 0..n {
                    let key = seg.sst.row_map.key(i);
                    if !seen.insert(key) { continue; }
                    if seg.sst.row_map.is_deleted(i) { continue; }
                    let gval = fseg.get_i64(i).unwrap_or(0).to_string();
                    *groups.entry(gval).or_insert(0) += 1;
                }
            }
        }
        groups
    }

    pub fn buffered_row_count(&self) -> usize {
        self.write_buf.lock().num_rows
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
            if segs.len() < 2 { return Ok(()); }
            segs.iter().cloned().collect()
        };
        self.merge_segments(old_segs)
    }

    /// Shared merge logic: merge `old_segs` into one new segment, dedup keys
    /// (newest version wins), drop tombstones, update manifest + GC old files.
    fn merge_segments(&self, old_segs: Vec<Arc<Segment>>) -> Result<()> {
        if old_segs.is_empty() { return Ok(()); }
        let old_ids: Vec<u64> = old_segs.iter().map(|s| s.id).collect();
        let ncols = self.col_types.len();

        let id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
        let path = self.dir.join(format!("{:010}.sst", id));
        let mut builder = ColumnarSSTableBuilder::new(&path, self.col_types.clone());

        // Check if ALL columns are fixed-width (integer/float/bool/timestamp).
        // If so, use the fast column-direct path (no Vec<Value>).
        let all_fixed = self.col_types.iter().all(|ct| matches!(ct,
            ColumnType::Integer | ColumnType::Float | ColumnType::Boolean | ColumnType::Timestamp));

        if all_fixed {
            // Column-direct compaction: extract raw i64 bytes per row, no Value.
            let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
            for seg in old_segs.iter().rev() {
                let n = seg.sst.num_rows;
                let fixed_cols: Vec<Option<crate::storage::lsm::columnar::FixedSegment>> =
                    (0..ncols).map(|ci| {
                        if ci < seg.sst.column_tags.len() && seg.sst.column_tags[ci].is_fixed() {
                            seg.sst.read_fixed_i64(ci).ok()
                        } else { None }
                    }).collect();
                for i in 0..n {
                    let key = seg.sst.row_map.key(i);
                    if !seen.insert(key) { continue; }
                    if seg.sst.row_map.is_deleted(i) { continue; }
                    let ts = seg.sst.row_map.timestamp(i);
                    let mut col_bytes: Vec<&[u8]> = Vec::with_capacity(ncols);
                    let mut bufs: Vec<[u8; 8]> = Vec::with_capacity(ncols);
                    for ci in 0..ncols {
                        let v = fixed_cols.get(ci).and_then(|x| x.as_ref())
                            .and_then(|f| f.get_i64(i)).unwrap_or(i64::MIN);
                        bufs.push(v.to_le_bytes());
                    }
                    for b in &bufs { col_bytes.push(b); }
                    builder.add_values_raw(key, ts, false, &col_bytes)?;
                }
            }
        } else {
            // Mixed columns (has Text): direct copy with temp buffers.
            // Avoids MergeCursor's per-row Vec<Value> + SegmentCursor pre-decode.
            let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
            for seg in old_segs.iter().rev() {
                let n = seg.sst.num_rows;
                let fixed_cols: Vec<Option<crate::storage::lsm::columnar::FixedSegment>> =
                    (0..ncols).map(|ci| {
                        if ci < seg.sst.column_tags.len() && seg.sst.column_tags[ci].is_fixed() {
                            seg.sst.read_fixed_i64(ci).ok()
                        } else { None }
                    }).collect();
                let text_cols: Vec<Option<crate::storage::lsm::columnar::TextSegment>> =
                    (0..ncols).map(|ci| {
                        if ci < seg.sst.column_tags.len() && !seg.sst.column_tags[ci].is_fixed() {
                            seg.sst.read_text(ci).ok()
                        } else { None }
                    }).collect();
                // Per-row reusable byte buffers (avoid per-row allocation).
                let mut row_bytes: Vec<Vec<u8>> = vec![Vec::new(); ncols];
                for i in 0..n {
                    let key = seg.sst.row_map.key(i);
                    if !seen.insert(key) { continue; }
                    if seg.sst.row_map.is_deleted(i) { continue; }
                    let ts = seg.sst.row_map.timestamp(i);
                    // Phase 1: fill row_bytes (mutable, no outstanding borrows).
                    for ci in 0..ncols {
                        if let Some(ref f) = fixed_cols.get(ci).and_then(|x| x.as_ref()) {
                            let v = f.get_i64(i).unwrap_or(i64::MIN);
                            row_bytes[ci].clear();
                            row_bytes[ci].extend_from_slice(&v.to_le_bytes());
                        } else if let Some(ref t) = text_cols.get(ci).and_then(|x| x.as_ref()) {
                            let s = t.get_str(i).unwrap_or("");
                            row_bytes[ci].clear();
                            let len = s.len().min(65535) as u16;
                            row_bytes[ci].extend_from_slice(&len.to_le_bytes());
                            row_bytes[ci].extend_from_slice(&s.as_bytes()[..len as usize]);
                        }
                    }
                    // Phase 2: collect immutable slices (row_bytes not modified here).
                    let col_slices: Vec<&[u8]> = row_bytes.iter().map(|b| b.as_slice()).collect();
                    builder.add_values_raw(key, ts, false, &col_slices)?;
                }
            }
        }
        builder.finish()?;

        let new_seg = Arc::new(Segment::open(&path, id)?);

        // Record compaction in manifest FIRST (crash safety), then swap memory.
        self.manifest.lock().record_compaction(id, &old_ids)?;
        {
            let mut segs = self.segments.write();
            let old_set: std::collections::HashSet<u64> = old_ids.iter().copied().collect();
            let new_list: VecDeque<Arc<Segment>> = segs.iter()
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
