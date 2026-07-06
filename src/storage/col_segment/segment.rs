use crate::storage::lsm::columnar::{ColumnTypeTag, ColumnarSSTable, FixedSegment, TextSegment};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

/// Cached decoded column segment (fixed or text).
enum CachedCol {
    Fixed(FixedSegment),
    Text(TextSegment),
}

/// Immutable columnar segment = a `ColumnarSSTable` plus bookkeeping metadata,
/// with a lazy per-column decode cache. The cache avoids re-decompressing an
/// entire column segment on every `get_row` call — the dominant cost for PK
/// point queries (was ~11ms due to per-call Snappy decompression of all columns).
pub struct Segment {
    /// Shared so merge cursors can hold a ref without cloning the SSTable.
    pub sst: Arc<ColumnarSSTable>,
    pub id: u64,
    pub row_count: usize,
    pub created_at: Instant,
    /// Lazy column decode cache: col_idx → decoded segment. Populated on first
    /// access, reused thereafter. RwLock so concurrent reads don't block.
    col_cache: RwLock<HashMap<usize, CachedCol>>,
}

impl Segment {
    /// Clear the column decode cache to free memory (call after bulk operations).
    pub fn clear_cache(&self) {
        self.col_cache.write().clear();
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
            col_cache: RwLock::new(HashMap::new()),
        })
    }

    /// Get a row by composite key, using cached column segments. First access
    /// to each column decompresses it once; subsequent accesses are O(1) decode.
    /// This is the fast path for PK point queries.
    pub fn get_row_cached(
        &self,
        key: u64,
        col_types: &[crate::types::ColumnType],
    ) -> Option<Vec<crate::types::Value>> {
        use crate::types::Value;

        // Binary search in RowMap for the row index.
        let idx = self.sst.row_map.find_key(key)?;
        if self.sst.row_map.is_deleted(idx) {
            return None;
        }

        // Decode each column from cache (lazy populate).
        let mut row = Vec::with_capacity(col_types.len());
        for (ci, ct) in col_types.iter().enumerate() {
            // Fast path: already cached.
            {
                let cache = self.col_cache.read();
                if let Some(cached) = cache.get(&ci) {
                    row.push(decode_cached_value(cached, idx, ct));
                    continue;
                }
            }
            // Cache miss: decode + insert.
            let tag = self.sst.column_tags.get(ci).copied();
            let decoded = if matches!(tag, Some(t) if t.is_fixed()) {
                self.sst.read_fixed_i64(ci).ok().map(CachedCol::Fixed)
            } else if matches!(tag, Some(ColumnTypeTag::Text)) {
                self.sst.read_text(ci).ok().map(CachedCol::Text)
            } else {
                None
            };
            if let Some(d) = decoded {
                row.push(decode_cached_value(&d, idx, ct));
                self.col_cache.write().insert(ci, d);
            } else {
                row.push(Value::Null);
            }
        }
        Some(row)
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
