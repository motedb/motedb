use crate::storage::lsm::columnar::{ColumnTypeTag, ColumnarSSTable, FixedSegment, TextSegment};
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Instant;

/// Cached decoded column segment (text only — fixed columns use O(1) direct read).
enum CachedCol {
    #[allow(dead_code)]
    Fixed(FixedSegment),
    Text(TextSegment),
}

/// Max columns cached per segment. Each entry is O(rows) decoded data, so we
/// bound to avoid unbounded growth on wide tables with many point queries.
/// For typical 5-column schemas this caches all columns; for wide tables it
/// keeps the most-recently-used set.
const COL_CACHE_CAP: usize = 16;

/// A simple bounded LRU for column decode cache. Keeps memory bounded even
/// under adversarial access patterns.
struct BoundedColCache {
    entries: std::collections::VecDeque<(usize, CachedCol)>,
}

impl BoundedColCache {
    fn new() -> Self {
        Self {
            entries: std::collections::VecDeque::with_capacity(COL_CACHE_CAP),
        }
    }

    #[allow(dead_code)]
    fn get(&mut self, col_idx: usize) -> Option<&CachedCol> {
        // Move to front (MRU).
        if let Some(pos) = self.entries.iter().position(|(k, _)| *k == col_idx) {
            if pos != 0 {
                if let Some(entry) = self.entries.remove(pos) {
                    self.entries.push_front(entry);
                }
            }
            return self.entries.front().map(|(_, v)| v);
        }
        None
    }

    fn insert(&mut self, col_idx: usize, val: CachedCol) {
        // Evict oldest if at capacity.
        while self.entries.len() >= COL_CACHE_CAP {
            self.entries.pop_back();
        }
        // Remove existing entry for this key (if any).
        self.entries.retain(|(k, _)| *k != col_idx);
        self.entries.push_front((col_idx, val));
    }

    fn clear(&mut self) {
        self.entries.clear();
    }
}

/// Immutable columnar segment = a `ColumnarSSTable` plus bookkeeping metadata,
/// with a bounded lazy per-column decode cache. The cache avoids re-decompressing
/// a column segment on every `get_row` call — critical for PK point query latency.
/// Bounded to COL_CACHE_CAP entries so memory never grows unbounded.
pub struct Segment {
    /// Shared so merge cursors can hold a ref without cloning the SSTable.
    pub sst: Arc<ColumnarSSTable>,
    pub id: u64,
    pub row_count: usize,
    pub created_at: Instant,
    /// Bounded column decode cache: col_idx → decoded segment (max COL_CACHE_CAP).
    col_cache: Mutex<BoundedColCache>,
}

impl Segment {
    /// Clear the column decode cache to free memory (call after bulk operations).
    pub fn clear_cache(&self) {
        self.col_cache.lock().clear();
    }

    /// Release mmap pages from RSS via MADV_DONTNEED. The OS will re-fault
    /// pages on next access. Call after bulk reads (e.g. compaction) to keep
    /// peak RSS low on memory-constrained embedded devices.
    pub fn release_pages(&self) {
        self.sst.release_pages();
    }

    pub fn open(path: &std::path::Path, id: u64) -> crate::Result<Self> {
        let sst = ColumnarSSTable::open(path)?;
        let row_count = sst.num_rows;
        Ok(Self {
            sst: Arc::new(sst),
            id,
            row_count,
            created_at: Instant::now(),
            col_cache: Mutex::new(BoundedColCache::new()),
        })
    }

    /// Get a row by composite key. For fixed-width columns, uses O(1) direct
    /// byte read (no full-column decode). For text columns, uses O(1)
    /// `read_text_at` on point queries (single-row), avoiding the O(N)
    /// full-column decode that dominates point query latency at scale.
    pub fn get_row_cached(
        &self,
        key: u64,
        col_types: &[crate::types::ColumnType],
    ) -> Option<Vec<crate::types::Value>> {
        self.get_row_inner(key, col_types, true)
    }

    /// Get a row for scan paths: text columns use the bounded col_cache (full
    /// column decode once, reused across rows). This is faster than per-row
    /// `read_text_at` when scanning many rows from the same segment.
    pub fn get_row_for_scan(
        &self,
        key: u64,
        col_types: &[crate::types::ColumnType],
    ) -> Option<Vec<crate::types::Value>> {
        self.get_row_inner(key, col_types, false)
    }

    fn get_row_inner(
        &self,
        key: u64,
        col_types: &[crate::types::ColumnType],
        point_query: bool,
    ) -> Option<Vec<crate::types::Value>> {
        use crate::types::Value;

        // Find row index via sparse fence index (O(1) memory).
        let idx = self.sst.find_row_by_key(key)?;
        if self.sst.row_map.is_deleted(idx) {
            return None;
        }

        let mut row = Vec::with_capacity(col_types.len());
        for (ci, ct) in col_types.iter().enumerate() {
            let tag = self.sst.column_tags.get(ci).copied();

            if matches!(tag, Some(t) if t.is_fixed()) {
                // Try O(1) direct byte read first. For uncompressed segments
                // (flag=0) this succeeds without touching the rest of the column.
                // For Snappy-compressed segments (flag=1) it falls back to a
                // full-column decode — so we cache the decoded column in col_cache
                // to avoid re-decompressing on every point query.
                match self.sst.read_fixed_i64_at(ci, idx) {
                    Ok(Some(v)) => {
                        push_fixed_value(&mut row, v, ct);
                        continue;
                    }
                    Ok(None) => {
                        row.push(Value::Null);
                        continue;
                    }
                    Err(_) => {
                        // Compressed segment or read error — fall through to
                        // cached full-column decode below.
                    }
                }
                // Cached full-column decode (same path as scan).
                {
                    let mut cache = self.col_cache.lock();
                    if let Some(cached) = cache.get(ci) {
                        row.push(decode_cached_value(cached, idx, ct));
                        continue;
                    }
                }
                if let Ok(seg) = self.sst.read_fixed_i64(ci) {
                    let cached = CachedCol::Fixed(seg);
                    row.push(decode_cached_value(&cached, idx, ct));
                    self.col_cache.lock().insert(ci, cached);
                } else {
                    row.push(Value::Null);
                }
                continue;
            }

            // Text column.
            if matches!(tag, Some(ColumnTypeTag::Text)) {
                if point_query {
                    // Use the bounded col_cache: decode the text column once,
                    // reuse across point queries. This avoids per-row disk
                    // seeks (read_text_at does 3 seek+read per call, which on
                    // a cold page cache costs 30ms+ each on macOS). Decoding
                    // the full column once is ~10-20ms but stays in the page
                    // cache (sequential read), and subsequent queries are O(1).
                    {
                        let mut cache = self.col_cache.lock();
                        if let Some(cached) = cache.get(ci) {
                            row.push(decode_cached_value(cached, idx, ct));
                            continue;
                        }
                    }
                    let decoded = self.sst.read_text(ci).ok().map(CachedCol::Text);
                    if let Some(d) = decoded {
                        row.push(decode_cached_value(&d, idx, ct));
                        self.col_cache.lock().insert(ci, d);
                    } else {
                        row.push(Value::Null);
                    }
                    continue;
                }
                // Scan path: use col_cache (full-column decode, reused).
                {
                    let mut cache = self.col_cache.lock();
                    if let Some(cached) = cache.get(ci) {
                        row.push(decode_cached_value(cached, idx, ct));
                        continue;
                    }
                }
                let decoded = self.sst.read_text(ci).ok().map(CachedCol::Text);
                if let Some(d) = decoded {
                    row.push(decode_cached_value(&d, idx, ct));
                    self.col_cache.lock().insert(ci, d);
                } else {
                    row.push(Value::Null);
                }
                continue;
            }

            // Unknown column type.
            row.push(Value::Null);
        }
        Some(row)
    }
}

/// Push a decoded fixed-width value into a row based on the column type.
fn push_fixed_value(row: &mut Vec<crate::types::Value>, v: i64, ct: &crate::types::ColumnType) {
    use crate::types::{ColumnType, Value};
    match ct {
        ColumnType::Integer => row.push(Value::Integer(v)),
        ColumnType::Float => row.push(Value::Float(f64::from_bits(v as u64))),
        ColumnType::Boolean => row.push(Value::Bool(v != 0)),
        ColumnType::Timestamp => {
            row.push(Value::Timestamp(crate::types::Timestamp::from_micros(v)))
        }
        _ => row.push(Value::Null),
    }
}

fn decode_cached_value(
    cached: &CachedCol,
    idx: usize,
    ct: &crate::types::ColumnType,
) -> crate::types::Value {
    use crate::types::{ColumnType, Value};
    match (cached, ct) {
        (CachedCol::Fixed(f), ColumnType::Integer) => {
            f.get_i64(idx).map(Value::Integer).unwrap_or(Value::Null)
        }
        (CachedCol::Fixed(f), ColumnType::Float) => {
            f.get_f64(idx).map(Value::Float).unwrap_or(Value::Null)
        }
        (CachedCol::Fixed(f), ColumnType::Boolean) => {
            f.get_bool(idx).map(Value::Bool).unwrap_or(Value::Null)
        }
        (CachedCol::Fixed(f), ColumnType::Timestamp) => f
            .get_i64(idx)
            .map(|v| Value::Timestamp(crate::types::Timestamp::from_micros(v)))
            .unwrap_or(Value::Null),
        (CachedCol::Text(t), ColumnType::Text) => t
            .get_str(idx)
            .map(|s| Value::Text(s.into()))
            .unwrap_or(Value::Null),
        _ => Value::Null,
    }
}
