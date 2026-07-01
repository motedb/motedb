//! Multi-way merge cursor over segments.
//!
//! Performance design (embedded-sensitive):
//! - Each `SegmentCursor` pre-decodes the needed column segments ONCE (not per
//!   row). `get_row` in the legacy path re-decompresses the whole column on
//!   every call — O(N × cols × decompress). We do O(cols × decompress) total.
//! - A binary min-heap drives ascending-key iteration: O(N log S) where S is
//!   the segment count, not O(N × S).
//! - Memory: heap size = S (bounded by MAX_SEGMENTS), independent of table size.

use super::segment::Segment;
use crate::storage::lsm::columnar::{ColumnarSSTable, ColumnTypeTag, FixedSegment, TextSegment};
use crate::types::{ColumnType, Value};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

/// A column's decoded data for one segment, indexed by row.
enum ColData {
    Fixed(FixedSegment),
    Text(TextSegment),
    /// Pre-decoded Vector column: one Vec<f32> per row index (None = NULL).
    Vector(Vec<Option<Vec<f32>>>),
    /// Pre-decoded Spatial column: one Geometry per row index (None = NULL).
    Spatial(Vec<Option<crate::types::Geometry>>),
    /// Fallback for unsupported column types.
    Opaque,
}

/// Cursor over one segment, iterating rows in ascending key order.
/// Column data is pre-decoded once at construction.
struct SegmentCursor {
    row_map_keys: Vec<u64>,
    row_map_ts: Vec<u64>,
    row_map_deleted: Vec<bool>,
    /// row indices sorted ascending by key.
    order: Vec<usize>,
    pos: usize,
    /// Pre-decoded column data (one entry per column). Indexed by original row index.
    col_data: Vec<ColData>,
    col_types: Vec<ColumnType>,
}

impl SegmentCursor {
    fn new(seg: &Segment, col_types: Vec<ColumnType>) -> Self {
        let n = seg.sst.num_rows;
        // Snapshot row_map into owned vectors (avoids repeated method calls).
        let mut row_map_keys = Vec::with_capacity(n);
        let mut row_map_ts = Vec::with_capacity(n);
        let mut row_map_deleted = Vec::with_capacity(n);
        for i in 0..n {
            row_map_keys.push(seg.sst.row_map.key(i));
            row_map_ts.push(seg.sst.row_map.timestamp(i));
            row_map_deleted.push(seg.sst.row_map.is_deleted(i));
        }
        // Sort row indices by key ascending.
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_by_key(|&i| row_map_keys[i]);

        // Pre-decode each column ONCE.
        let mut col_data = Vec::with_capacity(col_types.len());
        for ci in 0..col_types.len() {
            let cd = if ci < seg.sst.column_tags.len() && seg.sst.column_tags[ci].is_fixed() {
                match seg.sst.read_fixed_i64(ci) {
                    Ok(seg_data) => ColData::Fixed(seg_data),
                    Err(_) => ColData::Opaque,
                }
            } else if ci < seg.sst.column_tags.len() && matches!(seg.sst.column_tags[ci], ColumnTypeTag::Text) {
                match seg.sst.read_text(ci) {
                    Ok(seg_data) => ColData::Text(seg_data),
                    Err(_) => ColData::Opaque,
                }
            } else if ci < seg.sst.column_tags.len() && matches!(seg.sst.column_tags[ci], ColumnTypeTag::Vector) {
                // Map read_vectors (row_id, vec) pairs to per-row-index options.
                let decoded = seg.sst.read_vectors(ci).unwrap_or_default();
                let mut per = vec![None; n];
                let mut di = 0usize;
                for i in 0..n {
                    if seg.sst.row_map.is_deleted(i) { continue; }
                    let ek = seg.sst.row_map.key(i) & 0xFFFFFFFF;
                    while di < decoded.len() && decoded[di].0 != ek { di += 1; }
                    if di < decoded.len() { per[i] = Some(decoded[di].1.clone()); di += 1; }
                }
                ColData::Vector(per)
            } else if ci < seg.sst.column_tags.len() && matches!(seg.sst.column_tags[ci], ColumnTypeTag::Spatial) {
                let decoded = seg.sst.read_spatial(ci).unwrap_or_default();
                let mut per = vec![None; n];
                let mut di = 0usize;
                for i in 0..n {
                    if seg.sst.row_map.is_deleted(i) { continue; }
                    let ek = seg.sst.row_map.key(i) & 0xFFFFFFFF;
                    while di < decoded.len() && decoded[di].0 != ek { di += 1; }
                    if di < decoded.len() { per[i] = Some(decoded[di].1.clone()); di += 1; }
                }
                ColData::Spatial(per)
            } else {
                ColData::Opaque
            };
            col_data.push(cd);
        }

        Self {
            row_map_keys,
            row_map_ts,
            row_map_deleted,
            order,
            pos: 0,
            col_data,
            col_types,
        }
    }

    #[inline]
    fn peek_key(&self) -> Option<u64> {
        self.order.get(self.pos).map(|&i| self.row_map_keys[i])
    }

    /// Advance and return (key, ts, deleted, row_values) for the row at the cursor head.
    fn advance(&mut self) -> Option<(u64, u64, bool, Vec<Value>)> {
        let &i = self.order.get(self.pos)?;
        self.pos += 1;
        let key = self.row_map_keys[i];
        let ts = self.row_map_ts[i];
        let deleted = self.row_map_deleted[i];
        let row = if deleted {
            Vec::new()
        } else {
            self.decode_row(i)
        };
        Some((key, ts, deleted, row))
    }

    /// Decode row values at original index `i` from pre-decoded column data.
    fn decode_row(&self, i: usize) -> Vec<Value> {
        let mut row = Vec::with_capacity(self.col_types.len());
        for (ci, ct) in self.col_types.iter().enumerate() {
            let v = match self.col_data.get(ci) {
                Some(ColData::Fixed(f)) => match ct {
                    ColumnType::Integer => f.get_i64(i).map(Value::Integer),
                    ColumnType::Float => f.get_f64(i).map(Value::Float),
                    ColumnType::Boolean => f.get_bool(i).map(Value::Bool),
                    ColumnType::Timestamp => {
                        f.get_i64(i).map(|v| Value::Timestamp(crate::types::Timestamp::from_micros(v)))
                    }
                    _ => None,
                },
                Some(ColData::Text(t)) => t.get_str(i).map(|s| Value::Text(s.to_string().into())),
                Some(ColData::Vector(cols)) => cols.get(i).cloned().flatten()
                    .map(|v| Value::Vector(crate::types::ArcVec(Arc::new(v)))),
                Some(ColData::Spatial(cols)) => cols.get(i).cloned().flatten()
                    .map(|g| Value::Spatial(std::boxed::Box::new(g))),
                _ => None,
            }
            .unwrap_or(Value::Null);
            row.push(v);
        }
        row
    }
}

/// Multi-segment merge iterator. Yields (key, timestamp, row) for the newest
/// LIVE version of each key, skipping tombstones and superseded versions.
///
/// `segments` must be in ascending creation order (oldest first). When the same
/// key appears in multiple segments, the one with the highest timestamp wins;
/// ties go to the later (newer) segment.
pub struct MergeCursor {
    cursors: Vec<SegmentCursor>,
    /// Min-heap of (key, cursor_index). Drives ascending-key iteration.
    heap: BinaryHeap<Reverse<(u64, usize)>>,
}

impl MergeCursor {
    pub fn new(segments: &[Arc<Segment>], col_types: &[ColumnType]) -> Self {
        let cursors: Vec<SegmentCursor> = segments
            .iter()
            .map(|s| SegmentCursor::new(s, col_types.to_vec()))
            .collect();
        let mut heap = BinaryHeap::with_capacity(cursors.len());
        for (idx, c) in cursors.iter().enumerate() {
            if let Some(k) = c.peek_key() {
                heap.push(Reverse((k, idx)));
            }
        }
        Self { cursors, heap }
    }
}

impl Iterator for MergeCursor {
    type Item = (u64 /*key*/, u64 /*ts*/, Vec<Value>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let min_key = self.heap.peek().map(|Reverse((k, _))| *k)?;

            // Collect all cursor indices sitting on min_key.
            // (Pop matching heap entries; we'll re-push after advancing.)
            let mut at_key: Vec<usize> = Vec::new();
            while let Some(&Reverse((k, _))) = self.heap.peek() {
                if k != min_key {
                    break;
                }
                let Reverse((_, idx)) = self.heap.pop().unwrap();
                at_key.push(idx);
            }

            // Among those, pick highest timestamp (ties → larger idx = newer segment).
            let mut best_idx: Option<usize> = None;
            let mut best_ts: u64 = 0;
            for &idx in &at_key {
                let c = &self.cursors[idx];
                if let Some(&row_i) = c.order.get(c.pos) {
                    let ts = c.row_map_ts[row_i];
                    if best_idx.is_none() || ts >= best_ts {
                        best_idx = Some(idx);
                        best_ts = ts;
                    }
                }
            }

            // Advance all at-key cursors; capture the winner's row.
            let mut emitted: Option<(u64, u64, Vec<Value>)> = None;
            for &idx in &at_key {
                let c = &mut self.cursors[idx];
                if let Some((key, ts, deleted, row)) = c.advance() {
                    if Some(idx) == best_idx && !deleted {
                        emitted = Some((key, ts, row));
                    }
                }
                // Re-push if the cursor still has rows.
                if let Some(k) = c.peek_key() {
                    self.heap.push(Reverse((k, idx)));
                }
            }

            if let Some(e) = emitted {
                return Some(e);
            }
            // else: winner was a tombstone → key suppressed; continue loop.
        }
    }
}

// Reference ColumnarSSTable to ensure we only build when it's in scope.
#[allow(dead_code)]
fn _type_anchor(_s: &ColumnarSSTable) {}
