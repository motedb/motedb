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

/// Page size for the text offset window cache. Each page covers this many rows'
/// offset entries (4 bytes each). For PAGE_ROWS=512, a page is 512×4 = 2KB.
/// This keeps the cache tiny (~2KB per text column) while covering enough rows
/// for sequential point-query access patterns.
const TEXT_PAGE_ROWS: usize = 512;

/// A compact cache for text column point reads: stores a window of offset
/// entries (2KB) + a small string data page (8KB) instead of the full column
/// (~31MB for 2M rows). Designed for sequential PK point queries where
/// consecutive rows are accessed.
struct TextPageCache {
    /// (col_idx, page_idx) → offset entries for rows [page_idx*TEXT_PAGE_ROWS .. +TEXT_PAGE_ROWS+1]
    offset_pages: std::collections::VecDeque<(usize, usize, Vec<u32>)>,
    /// (col_idx) → string data bytes for the most recently accessed region
    string_pages: std::collections::VecDeque<(usize, Vec<u8>, u32)>, // (col, data, base_offset)
}

impl TextPageCache {
    fn new() -> Self {
        Self {
            offset_pages: std::collections::VecDeque::with_capacity(4),
            string_pages: std::collections::VecDeque::with_capacity(4),
        }
    }

    fn get_offsets(&self, col_idx: usize, page_idx: usize) -> Option<&[u32]> {
        for (ci, pi, offsets) in &self.offset_pages {
            if *ci == col_idx && *pi == page_idx {
                return Some(offsets);
            }
        }
        None
    }

    fn put_offsets(&mut self, col_idx: usize, page_idx: usize, offsets: Vec<u32>) {
        while self.offset_pages.len() >= 4 {
            self.offset_pages.pop_back();
        }
        self.offset_pages
            .retain(|(c, p, _)| !(*c == col_idx && *p == page_idx));
        self.offset_pages.push_front((col_idx, page_idx, offsets));
    }

    fn get_strings(&self, col_idx: usize) -> Option<(&[u8], u32)> {
        for (ci, data, base) in &self.string_pages {
            if *ci == col_idx {
                return Some((data, *base));
            }
        }
        None
    }

    fn put_strings(&mut self, col_idx: usize, data: Vec<u8>, base_offset: u32) {
        while self.string_pages.len() >= 4 {
            self.string_pages.pop_back();
        }
        self.string_pages.retain(|(c, _, _)| *c != col_idx);
        self.string_pages.push_front((col_idx, data, base_offset));
    }

    fn clear(&mut self) {
        self.offset_pages.clear();
        self.string_pages.clear();
    }
}

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
    /// Used by SCAN paths (multiple rows from same column).
    col_cache: Mutex<BoundedColCache>,
    /// Page-level text cache for POINT QUERIES: stores small windows of offset
    /// entries + string data (~10KB total) instead of full column decode (~31MB).
    /// This keeps point-query peak RSS low while maintaining 6µs latency.
    text_page_cache: Mutex<TextPageCache>,
}

impl Segment {
    /// Clear the column decode cache to free memory (call after bulk operations).
    pub fn clear_cache(&self) {
        self.col_cache.lock().clear();
        // 🚀 Keep text_page_cache: it's tiny (~40KB/segment) and provides
        // critical cross-query locality for point queries. Clearing it on
        // every 4096th query (POINT_QUERY_EVICT_INTERVAL) caused text column
        // re-decode thrashing. Only clear on writes (clear_all_caches).
    }

    /// Clear ALL caches including text_page_cache (used on writes/compaction).
    pub fn clear_all_caches(&self) {
        self.col_cache.lock().clear();
        self.text_page_cache.lock().clear();
    }

    /// Read a fixed-width column, using the cross-query col_cache.
    /// On cache miss, decodes and caches the column segment. On hit,
    /// returns the cached FixedSegment (zero allocation, zero decode).
    pub fn read_fixed_cached(&self, col_idx: usize) -> Option<FixedSegment> {
        {
            let mut cache = self.col_cache.lock();
            if let Some(cached) = cache.get(col_idx) {
                if let CachedCol::Fixed(ref f) = cached {
                    return Some(f.clone());
                }
            }
        }
        let seg = self.sst.read_fixed_i64(col_idx).ok()?;
        self.col_cache
            .lock()
            .insert(col_idx, CachedCol::Fixed(seg.clone()));
        Some(seg)
    }

    /// Read a text column, using the cross-query col_cache.
    /// On cache miss, decodes and caches. On hit, returns cached TextSegment.
    pub fn read_text_cached(&self, col_idx: usize) -> Option<TextSegment> {
        {
            let mut cache = self.col_cache.lock();
            if let Some(cached) = cache.get(col_idx) {
                if let CachedCol::Text(ref t) = cached {
                    return Some(t.clone());
                }
            }
        }
        let seg = self.sst.read_text(col_idx).ok()?;
        self.col_cache
            .lock()
            .insert(col_idx, CachedCol::Text(seg.clone()));
        Some(seg)
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
            text_page_cache: Mutex::new(TextPageCache::new()),
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
        // Find row index via sparse fence index, then decode.
        let idx = self.sst.find_row_by_key(key)?;
        if self.sst.row_map.is_deleted(idx) {
            return None;
        }
        Some(self.decode_row_at(idx, col_types, true))
    }

    /// Decode a row at a known index — skips the fence-index lookup.
    /// Used by store.get() which already found the index via find_row_by_key,
    /// avoiding a duplicate binary search + key block read (~2-3µs saved).
    /// Caller MUST verify the row is not deleted before calling this.
    pub fn get_row_at_idx(
        &self,
        idx: usize,
        col_types: &[crate::types::ColumnType],
    ) -> Vec<crate::types::Value> {
        self.decode_row_at(idx, col_types, true)
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
        // Find row index via sparse fence index (O(1) memory).
        let idx = self.sst.find_row_by_key(key)?;
        if self.sst.row_map.is_deleted(idx) {
            return None;
        }
        Some(self.decode_row_at(idx, col_types, point_query))
    }

    /// Decode all columns of a row at a known index. No fence-index lookup.
    fn decode_row_at(
        &self,
        idx: usize,
        col_types: &[crate::types::ColumnType],
        point_query: bool,
    ) -> Vec<crate::types::Value> {
        use crate::types::Value;

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
                    // 🚀 Page-level cache: read a small window of offsets
                    // (2KB for 512 rows) + string data via a single batch
                    // read, instead of caching the entire text column (~31MB).
                    // This keeps peak RSS low (<5MB per text column) while
                    // serving sequential point queries at 6µs latency.
                    let val = self.read_text_paged(ci, idx);
                    match val {
                        Some(s) => row.push(Value::Text(s.into())),
                        None => row.push(Value::Null),
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
        row
    }

    /// Read a text value using page-level caching. Reads a small window of
    /// offset entries (covering TEXT_PAGE_ROWS rows around `row_idx`) and the
    /// corresponding string data, caching both for subsequent point queries.
    /// Memory per text column: ~10KB (vs ~31MB for full-column cache).
    fn read_text_paged(&self, col_idx: usize, row_idx: usize) -> Option<String> {
        use std::io::{Read, Seek, SeekFrom};

        let entry = &self.sst.column_index.get(col_idx)?;
        let num_rows = self.sst.num_rows;
        let null_bytes = num_rows.div_ceil(8);

        // 🔑 Check if the text column is Snappy-compressed (flag=1).
        // If so, the page-level cache can't read raw offsets from the file —
        // the data is compressed. Fall back to full-column decode via col_cache.
        let flag = if !self.sst.file_data.is_empty() {
            self.sst
                .file_data
                .get(entry.offset as usize)
                .copied()
                .unwrap_or(0)
        } else {
            let mut buf = [0u8; 1];
            if self.sst.read_raw(entry.offset as usize, &mut buf).is_err() {
                return None;
            }
            buf[0]
        };
        if flag == 1 {
            // Compressed text — fall back to full-column cache.
            {
                let mut cache = self.col_cache.lock();
                if let Some(cached) = cache.get(col_idx) {
                    return match cached {
                        CachedCol::Text(t) => t.get_str(row_idx).map(|s| s.to_string()),
                        _ => None,
                    };
                }
            }
            if let Ok(t) = self.sst.read_text(col_idx) {
                let result = t.get_str(row_idx).map(|s| s.to_string());
                self.col_cache.lock().insert(col_idx, CachedCol::Text(t));
                return result;
            }
            return None;
        }

        // Uncompressed text — use page-level cache (file layout below).
        // File layout: [flag:1B][null_bitmap][offsets (num_rows+1)×4][strings]
        let data_base = entry.offset as usize + 1; // skip flag
        let offsets_region = data_base + null_bytes;
        let strings_region = offsets_region + (num_rows + 1) * 4;

        // Null check: read 1 byte for this row's null bit.
        let null_off = data_base + row_idx / 8;
        let null_byte = if !self.sst.file_data.is_empty() {
            self.sst.file_data.get(null_off).copied().unwrap_or(0)
        } else {
            let mut buf = [0u8; 1];
            if self.sst.read_raw(null_off, &mut buf).is_err() {
                return None;
            }
            buf[0]
        };
        if (null_byte >> (row_idx % 8)) & 1 != 0 {
            return None; // NULL
        }

        // Read the offset pair for this row (start, end) — 8 bytes.
        let off_pos = offsets_region + row_idx * 4;
        let (start, end) = if !self.sst.file_data.is_empty() {
            if off_pos + 8 > self.sst.file_data.len() {
                return None;
            }
            let s = u32::from_le_bytes(self.sst.file_data[off_pos..off_pos + 4].try_into().unwrap())
                as usize;
            let e = u32::from_le_bytes(
                self.sst.file_data[off_pos + 4..off_pos + 8]
                    .try_into()
                    .unwrap(),
            ) as usize;
            (s, e)
        } else {
            // Check page cache for this row's offset window.
            let page_idx = row_idx / TEXT_PAGE_ROWS;
            {
                let cache = self.text_page_cache.lock();
                if let Some(offsets) = cache.get_offsets(col_idx, page_idx) {
                    let local_idx = row_idx - page_idx * TEXT_PAGE_ROWS;
                    if local_idx + 1 < offsets.len() {
                        let start = offsets[local_idx] as usize;
                        let end = offsets[local_idx + 1] as usize;
                        // Try string page cache.
                        if let Some((sdata, sbase)) = cache.get_strings(col_idx) {
                            if start >= sbase as usize && end <= sbase as usize + sdata.len() {
                                let bytes = &sdata[start as usize - sbase as usize
                                    ..end as usize - sbase as usize];
                                return Some(String::from_utf8_lossy(bytes).into_owned());
                            }
                        }
                        // Read string from file directly.
                        let str_pos = strings_region + start;
                        let len = end.saturating_sub(start);
                        if len == 0 {
                            return Some(String::new());
                        }
                        // Sanity check: cap string length to prevent capacity overflow.
                        if len > 65536 {
                            return None;
                        }
                        let mut str_buf = vec![0u8; len];
                        if self.sst.read_raw(str_pos, &mut str_buf).is_ok() {
                            return Some(String::from_utf8_lossy(&str_buf).into_owned());
                        }
                        return None;
                    }
                }
            }
            // Cache miss — read the offset window for this page.
            let window_start = page_idx * TEXT_PAGE_ROWS;
            let window_end = (window_start + TEXT_PAGE_ROWS + 1).min(num_rows + 1);
            let window_count = window_end - window_start;
            let buf_start = offsets_region + window_start * 4;
            let mut off_buf = vec![0u8; window_count * 4];
            if self.sst.read_raw(buf_start, &mut off_buf).is_err() {
                // Fallback: read just this row's offset pair.
                let mut buf8 = [0u8; 8];
                if self.sst.read_raw(off_pos, &mut buf8).is_err() {
                    return None;
                }
                let s = u32::from_le_bytes([buf8[0], buf8[1], buf8[2], buf8[3]]) as usize;
                let e = u32::from_le_bytes([buf8[4], buf8[5], buf8[6], buf8[7]]) as usize;
                (s, e)
            } else {
                let offsets: Vec<u32> = off_buf
                    .chunks_exact(4)
                    .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                    .collect();
                let local_idx = row_idx - window_start;
                let start = offsets.get(local_idx).copied().unwrap_or(0) as usize;
                let end = offsets.get(local_idx + 1).copied().unwrap_or(0) as usize;
                // Cache the offset window.
                self.text_page_cache
                    .lock()
                    .put_offsets(col_idx, page_idx, offsets);
                (start, end)
            }
        };

        // Read string bytes.
        let len = end.saturating_sub(start);
        if len == 0 {
            return Some(String::new());
        }
        // Sanity check: cap string length to prevent capacity overflow.
        if len > 65536 {
            return None;
        }
        let str_pos = strings_region + start;
        if !self.sst.file_data.is_empty() {
            if str_pos + len <= self.sst.file_data.len() {
                return Some(
                    String::from_utf8_lossy(&self.sst.file_data[str_pos..str_pos + len])
                        .into_owned(),
                );
            }
        } else {
            let mut str_buf = vec![0u8; len];
            if self.sst.read_raw(str_pos, &mut str_buf).is_ok() {
                return Some(String::from_utf8_lossy(&str_buf).into_owned());
            }
        }
        None
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
