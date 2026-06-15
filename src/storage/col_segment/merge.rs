//! Multi-way merge cursor over segments. O(1) extra memory via scanning for
//! the global minimum key each step; newest version (highest timestamp, and for
//! ties the newer segment) wins. Tombstones suppress their key entirely.

use super::segment::Segment;
use crate::storage::lsm::columnar::ColumnarSSTable;
use crate::types::{ColumnType, Value};
use std::sync::Arc;

/// Cursor over one segment, iterating rows in ascending key order.
struct SegmentCursor {
    sst: Arc<ColumnarSSTable>,
    /// Row indices sorted ascending by key.
    order: Vec<usize>,
    pos: usize,
    col_types: Vec<ColumnType>,
}

impl SegmentCursor {
    fn new(seg: &Segment, col_types: Vec<ColumnType>) -> Self {
        let sst = Arc::clone(&seg.sst);
        let mut order: Vec<usize> = (0..seg.sst.num_rows).collect();
        // Sort by key ascending (stable) for merge correctness.
        order.sort_by_key(|&i| seg.sst.row_map.key(i));
        Self { sst, order, pos: 0, col_types }
    }

    fn peek_key(&self) -> Option<u64> {
        self.order.get(self.pos).map(|&i| self.sst.row_map.key(i))
    }

    fn peek_ts(&self) -> Option<u64> {
        self.order.get(self.pos).map(|&i| self.sst.row_map.timestamp(i))
    }

    /// Advance past current row; return (key, ts, deleted, row_values).
    fn advance(&mut self) -> Option<(u64, u64, bool, Vec<Value>)> {
        let &i = self.order.get(self.pos)?;
        self.pos += 1;
        let key = self.sst.row_map.key(i);
        let ts = self.sst.row_map.timestamp(i);
        let deleted = self.sst.row_map.is_deleted(i);
        let row = if deleted {
            vec![]
        } else {
            self.sst.get_row(key, &self.col_types).unwrap_or_default()
        };
        Some((key, ts, deleted, row))
    }
}

/// Multi-segment merge iterator. Yields (key, timestamp, row) for the newest
/// LIVE version of each key, skipping tombstones and superseded versions.
///
/// `segments` must be in ascending creation order (oldest first).
pub struct MergeCursor {
    cursors: Vec<SegmentCursor>,
    done: bool,
}

impl MergeCursor {
    pub fn new(segments: &[Arc<Segment>], col_types: &[ColumnType]) -> Self {
        let cursors: Vec<SegmentCursor> = segments
            .iter()
            .map(|s| SegmentCursor::new(s, col_types.to_vec()))
            .collect();
        Self { cursors, done: false }
    }

    /// Find the global minimum key across all cursors' current positions.
    fn min_key(&self) -> Option<u64> {
        let mut min: Option<u64> = None;
        for c in &self.cursors {
            if let Some(k) = c.peek_key() {
                min = Some(match min {
                    Some(m) if m <= k => m,
                    _ => k,
                });
            }
        }
        min
    }
}

impl Iterator for MergeCursor {
    type Item = (u64 /*key*/, u64 /*ts*/, Vec<Value>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            let min_key = self.min_key()?;
            // Among cursors sitting on min_key, pick the highest timestamp.
            // Ties broken by larger cursor index (newer segment wins).
            let mut best_idx: Option<usize> = None;
            let mut best_ts: u64 = 0;
            for (idx, c) in self.cursors.iter().enumerate() {
                if c.peek_key() == Some(min_key) {
                    let ts = c.peek_ts().unwrap_or(0);
                    if best_idx.is_none() || ts >= best_ts {
                        best_idx = Some(idx);
                        best_ts = ts;
                    }
                }
            }

            // Advance ALL cursors sitting on min_key (consume the key everywhere).
            let mut emitted: Option<(u64, u64, Vec<Value>)> = None;
            for (idx, c) in self.cursors.iter_mut().enumerate() {
                if c.peek_key() == Some(min_key) {
                    if let Some((key, ts, deleted, row)) = c.advance() {
                        if Some(idx) == best_idx && !deleted {
                            emitted = Some((key, ts, row));
                        }
                    }
                }
            }

            if let Some(e) = emitted {
                return Some(e);
            }
            // else: the winning version was a tombstone → key suppressed, continue.
        }
    }
}
