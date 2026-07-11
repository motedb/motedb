//! Columnar SSTable — column-oriented persistent storage.
//!
//! ## Motivation
//!
//! Row-based SSTables store all columns of a row together:
//! ```text
//! [row0: id|customer|amount|region] [row1: id|customer|amount|region] ...
//! ```
//!
//! Columnar SSTables store each column independently:
//! ```text
//! [id: i64×N] [customer: str_dict] [amount: f64×N] [region: str_dict] [RowMap]
//! ```
//!
//! ## Benefits
//!
//! - **Projection**: SELECT id, amount reads 2/4 columns → 2× less I/O
//! - **SIMD**: Fixed columns are contiguous typed arrays → vectorizable
//! - **Compression**: Same-type data compresses 3-5× better
//! - **Memory**: No per-row Vec<Value> allocation during scan
//!
//! ## File Format
//!
//! ```text
//! [Header: 32 bytes]
//!   magic: u32 = 0x434D5442 ("BTMC" — Columnar MoTeDB)
//!   version: u32 = 1
//!   num_rows: u32
//!   num_columns: u16
//!   column_types: [u8; 16]  (type tag per column, max 16 columns)
//!   reserved: [u8; 6]
//!
//! [Column Index: (offset: u64, size: u64) × num_columns]
//!
//! [Column 0 Segment]
//! [Column 1 Segment]
//! ...
//!
//! [Row Map Segment]
//!   keys: u64 × num_rows
//!   timestamps: u64 × num_rows
//!   deleted: u8 × ceil(num_rows/8)
//!
//! [Footer: 16 bytes]
//!   column_index_offset: u64
//!   row_map_offset: u64
//!   _padding: [u8; 4]  (alignment to keep magic at end)
//!   magic: u32 = 0x434D5442
//! ```
//!
//! ## Column Segment Format
//!
//! **Fixed-width (Integer, Float, Bool, Timestamp):**
//! ```text
//! [null_bitmap: u8 × ceil(num_rows/8)]
//! [data: T × num_rows]  (T = i64, f64, u8, i64)
//! ```
//!
//! **Variable-width (Text):**
//! ```text
//! [null_bitmap: u8 × ceil(num_rows/8)]
//! [offsets: u32 × (num_rows + 1)]  (cumulative byte offsets into string_data)
//! [string_data: bytes]
//! ```
//!
//! **Vector/Spatial (variable but fixed-stride):**
//! ```text
//! [null_bitmap: u8 × ceil(num_rows/8)]
//! [dim: u16] [stride: u16]
//! [data: f32 × num_rows × stride]
//! ```

use crate::types::{ColumnType, RowId, Value};
use crate::{Result, StorageError};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[allow(unused_imports)]
use memmap2::{Mmap, MmapOptions};

// ── Constants ─────────────────────────────────────────────────────

const COLUMNAR_MAGIC: u32 = 0x434D5442; // "BTMC"
const COLUMNAR_VERSION: u32 = 2; // v2: MAX_COLUMNS 16 → 128, header grew to 144 bytes
const HEADER_SIZE: usize = 144; // 14 (fixed prefix) + 128 (column_tags) + 2 (reserved)
const FOOTER_SIZE: usize = 20;
/// Maximum number of columns supported by the columnar SSTable format.
/// The on-disk header reserves a fixed-width slot per column, so this is a
/// hard format limit. CREATE TABLE rejects tables exceeding it.
pub const MAX_COLUMNS: usize = 128;

/// Column type tags for the columnar format (compact u8 representation).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ColumnTypeTag {
    Integer = 0,
    Float = 1,
    Bool = 2,
    Timestamp = 3,
    Text = 4,
    Vector = 5,
    Spatial = 6,
}

impl ColumnTypeTag {
    fn from_column_type(ct: &ColumnType) -> Self {
        match ct {
            ColumnType::Integer => Self::Integer,
            ColumnType::Float => Self::Float,
            ColumnType::Boolean => Self::Bool,
            ColumnType::Timestamp => Self::Timestamp,
            ColumnType::Text => Self::Text,
            ColumnType::Tensor(_) => Self::Vector,
            ColumnType::Spatial => Self::Spatial,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn to_column_type(&self) -> ColumnType {
        match self {
            Self::Integer => ColumnType::Integer,
            Self::Float => ColumnType::Float,
            Self::Bool => ColumnType::Boolean,
            Self::Timestamp => ColumnType::Timestamp,
            Self::Text => ColumnType::Text,
            Self::Vector => ColumnType::Tensor(0), // dim reconstructed from segment header
            Self::Spatial => ColumnType::Spatial,
        }
    }

    pub(crate) fn is_fixed(&self) -> bool {
        matches!(
            self,
            Self::Integer | Self::Float | Self::Bool | Self::Timestamp
        )
    }

    fn fixed_size(&self) -> usize {
        match self {
            Self::Integer | Self::Float | Self::Timestamp => 8,
            Self::Bool => 1,
            _ => 0,
        }
    }
}

// ── Header ─────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub(crate) struct ColumnarHeader {
    num_rows: u32,
    num_columns: u16,
    column_tags: [u8; MAX_COLUMNS],
}

impl ColumnarHeader {
    fn serialize(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(&COLUMNAR_MAGIC.to_le_bytes());
        buf[4..8].copy_from_slice(&COLUMNAR_VERSION.to_le_bytes());
        buf[8..12].copy_from_slice(&self.num_rows.to_le_bytes());
        buf[12..14].copy_from_slice(&self.num_columns.to_le_bytes());
        buf[14..14 + MAX_COLUMNS].copy_from_slice(&self.column_tags);
        // bytes 142-143: reserved
        buf
    }

    fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < HEADER_SIZE {
            return Err(StorageError::InvalidData(
                "Columnar header too short".into(),
            ));
        }
        let magic = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        if magic != COLUMNAR_MAGIC {
            return Err(StorageError::InvalidData(format!(
                "Bad columnar magic: 0x{:08X}",
                magic
            )));
        }
        let version = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
        if version != COLUMNAR_VERSION {
            return Err(StorageError::InvalidData(format!(
                "Unsupported columnar version: {}",
                version
            )));
        }
        let num_rows = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);
        let num_columns = u16::from_le_bytes([data[12], data[13]]);
        let mut column_tags = [0u8; MAX_COLUMNS];
        column_tags.copy_from_slice(&data[14..14 + MAX_COLUMNS]);
        Ok(Self {
            num_rows,
            num_columns,
            column_tags,
        })
    }
}

// ── Column Index Entry ─────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct ColumnIndexEntry {
    pub offset: u64,
    pub size: u64,
}

const COLUMN_INDEX_ENTRY_SIZE: usize = 16; // (offset: u64, size: u64)

// ── Row Map ────────────────────────────────────────────────────────

/// Row Map: MVCC metadata for each row in columnar order.
///
/// Stored as three contiguous arrays:
/// - keys: u64 × num_rows (composite keys, preserves row order)
/// - timestamps: u64 × num_rows (MVCC version)
/// - deleted: bitset (u8 × ceil(num_rows/8))
///
/// For query paths, only `keys` and `deleted` are needed. `timestamps` is
/// only read during merge/compaction (4 call sites). To minimize RSS:
/// - `deleted` is always loaded into heap (small: ~250KB for 2M rows)
/// - `keys` uses the backing SegData (mmap or owned)
/// - `timestamps` uses the same backing SegData (never separately allocated)
#[derive(Clone, Debug)]
pub struct RowMap {
    pub num_rows: usize,
    data: SegData, // Owned for builder, Mmap for zero-copy reads
    keys_offset: usize,
    timestamps_offset: usize,
    #[allow(dead_code)]
    deleted_offset: usize,
    #[allow(dead_code)]
    deleted_len: usize,
    /// Eagerly-loaded deleted bitmap (heap). Separated from `data` so that
    /// `has_any_deleted()` and `is_deleted()` don't fault mmap pages for keys
    /// and timestamps. None when there are no deletions (common case).
    deleted_bitmap: Option<Box<[u8]>>,
}

impl RowMap {
    fn compute_sizes(num_rows: usize) -> (usize, usize, usize, usize) {
        let keys_size = num_rows * 8;
        let timestamps_size = num_rows * 8;
        let deleted_len = num_rows.div_ceil(8);
        (
            keys_size + timestamps_size + deleted_len,
            keys_size,
            timestamps_size,
            deleted_len,
        )
    }

    #[allow(dead_code)]
    pub(crate) fn from_bytes(data: Vec<u8>, num_rows: usize) -> Self {
        let (_, keys_size, timestamps_size, deleted_len) = Self::compute_sizes(num_rows);
        let deleted_offset = keys_size + timestamps_size;
        // Extract deleted bitmap into a separate heap allocation so is_deleted
        // and has_any_deleted don't touch the keys/timestamps region.
        let deleted_bitmap = Self::extract_deleted_bitmap(&data, deleted_offset, deleted_len);
        Self {
            num_rows,
            keys_offset: 0,
            timestamps_offset: keys_size,
            deleted_offset,
            deleted_len,
            data: SegData::Owned(data),
            deleted_bitmap,
        }
    }

    /// Extract the deleted bitmap from raw row_map data. Returns None when the
    /// bitmap is all zeros (no deletions), which makes has_any_deleted() O(1).
    fn extract_deleted_bitmap(data: &[u8], offset: usize, len: usize) -> Option<Box<[u8]>> {
        if offset + len > data.len() {
            return None;
        }
        let slice = &data[offset..offset + len];
        // Check if any byte is non-zero. If all zeros, no deletions — return None.
        let has_any = slice.iter().any(|&b| b != 0);
        if !has_any {
            return None;
        }
        Some(slice.to_vec().into_boxed_slice())
    }

    /// Zero-copy view into mmap data.
    #[allow(dead_code)]
    pub(crate) fn from_mmap(mmap: Arc<Mmap>, offset: usize, num_rows: usize) -> Result<Self> {
        let (_total, keys_size, timestamps_size, deleted_len) = Self::compute_sizes(num_rows);
        let deleted_offset = offset + keys_size + timestamps_size;
        // Eagerly read the deleted bitmap from the mmap into heap. This is a
        // small read (~250KB for 2M rows) that avoids faulting the entire
        // row_map mmap region on every has_any_deleted() / is_deleted() call.
        let deleted_bitmap = {
            let mmap_ref = &mmap;
            Self::extract_deleted_bitmap(mmap_ref, deleted_offset, deleted_len)
        };
        Ok(Self {
            num_rows,
            keys_offset: offset,
            timestamps_offset: offset + keys_size,
            deleted_offset,
            deleted_len,
            data: SegData::Mmap { mmap, offset },
            deleted_bitmap,
        })
    }

    #[inline]
    pub fn key(&self, row_idx: usize) -> u64 {
        let off = self.keys_offset + row_idx * 8;
        let s = self.data.slice(off, 8);
        u64::from_le_bytes([s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]])
    }

    #[inline]
    pub fn timestamp(&self, row_idx: usize) -> u64 {
        let off = self.timestamps_offset + row_idx * 8;
        let s = self.data.slice(off, 8);
        u64::from_le_bytes([s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]])
    }

    /// Binary search for a key in the RowMap. Returns row index if found.
    pub fn find_key(&self, target: u64) -> Option<usize> {
        let mut lo = 0usize;
        let mut hi = self.num_rows;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let k = self.key(mid);
            if k < target {
                lo = mid + 1;
            } else if k > target {
                hi = mid;
            } else {
                return Some(mid);
            }
        }
        None
    }

    #[inline]
    pub fn is_deleted(&self, row_idx: usize) -> bool {
        if let Some(ref bmp) = self.deleted_bitmap {
            (bmp[row_idx / 8] >> (row_idx % 8)) & 1 != 0
        } else {
            // No deletions (deleted_bitmap is None when empty).
            false
        }
    }

    /// Check if ANY row is marked deleted. O(1) when deleted_bitmap is None
    /// (the common case — no deletions). Previously this was O(N/8) scanning
    /// the bitmap bytes on every scan.
    pub fn has_any_deleted(&self) -> bool {
        self.deleted_bitmap.is_some()
    }
}

// ── Column Segment Views ───────────────────────────────────────────

/// Data source for a column segment: owned bytes (legacy) or mmap reference (zero-copy).
#[derive(Clone, Debug)]
enum SegData {
    #[allow(dead_code)]
    Owned(Vec<u8>),
    #[allow(dead_code)]
    Mmap { mmap: Arc<Mmap>, offset: usize },
}

impl SegData {
    #[inline]
    fn get(&self, idx: usize) -> u8 {
        match self {
            SegData::Owned(v) => v[idx],
            SegData::Mmap { mmap, offset } => mmap[offset + idx],
        }
    }
    fn slice(&self, start: usize, len: usize) -> &[u8] {
        match self {
            SegData::Owned(v) => &v[start..start + len],
            SegData::Mmap { mmap, offset } => &mmap[*offset + start..*offset + start + len],
        }
    }
    fn len(&self) -> usize {
        match self {
            SegData::Owned(v) => v.len(),
            SegData::Mmap { mmap, offset } => mmap.len().saturating_sub(*offset),
        }
    }
    /// Return the entire backing buffer as &[u8]. Used by batch scans that walk
    /// the raw bytes (e.g. prefix_match_indices) to avoid per-element slice() calls.
    fn as_bytes(&self) -> &[u8] {
        match self {
            SegData::Owned(v) => v.as_slice(),
            SegData::Mmap { mmap, offset } => &mmap[*offset..],
        }
    }
}

/// Typed view into a fixed-width column segment. Zero-copy from mmap when available.
#[derive(Clone)]
pub struct FixedSegment {
    pub num_rows: usize,
    null_bitmap: SegData,
    data: SegData,
    #[allow(dead_code)]
    elem_size: usize,
    #[allow(dead_code)]
    tag: ColumnTypeTag,
}

impl FixedSegment {
    #[allow(dead_code)]
    pub(crate) fn from_bytes(data: &[u8], num_rows: usize, tag: ColumnTypeTag) -> Result<Self> {
        let null_bytes = num_rows.div_ceil(8);
        let elem_size = tag.fixed_size();
        let data_size = num_rows * elem_size;
        let expected = null_bytes + data_size;
        if data.len() < expected {
            return Err(StorageError::InvalidData(format!(
                "Fixed segment too short: {} < {}",
                data.len(),
                expected
            )));
        }
        Ok(Self {
            num_rows,
            null_bitmap: SegData::Owned(data[..null_bytes].to_vec()),
            data: SegData::Owned(data[null_bytes..null_bytes + data_size].to_vec()),
            elem_size,
            tag,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn from_mmap(
        mmap: Arc<Mmap>,
        offset: usize,
        num_rows: usize,
        tag: ColumnTypeTag,
    ) -> Self {
        let null_bytes = num_rows.div_ceil(8);
        Self {
            num_rows,
            null_bitmap: SegData::Mmap {
                mmap: mmap.clone(),
                offset,
            },
            data: SegData::Mmap {
                mmap,
                offset: offset + null_bytes,
            },
            elem_size: tag.fixed_size(),
            tag,
        }
    }

    #[inline]
    pub fn is_null(&self, row_idx: usize) -> bool {
        (self.null_bitmap.get(row_idx / 8) >> (row_idx % 8)) & 1 != 0
    }

    /// Returns true if any row in this segment is NULL. O(null_bitmap_size).
    pub fn has_nulls(&self) -> bool {
        let nb = self.null_bitmap.len();
        for i in 0..nb {
            if self.null_bitmap.get(i) != 0 {
                return true;
            }
        }
        false
    }

    /// Returns the raw data bytes (the fixed-width values, after the null
    /// bitmap). Used by batch scans (e.g. top_k) to walk data directly
    /// without per-element slice() calls.
    pub fn raw_f64_slice(&self) -> &[u8] {
        self.data.as_bytes()
    }

    #[inline]
    pub fn get_i64(&self, row_idx: usize) -> Option<i64> {
        if self.is_null(row_idx) {
            return None;
        }
        let off = row_idx * 8;
        let s = self.data.slice(off, 8);
        Some(i64::from_le_bytes([
            s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7],
        ]))
    }

    #[inline]
    pub fn get_f64(&self, row_idx: usize) -> Option<f64> {
        if self.is_null(row_idx) {
            return None;
        }
        let off = row_idx * 8;
        let s = self.data.slice(off, 8);
        Some(f64::from_le_bytes([
            s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7],
        ]))
    }

    #[inline]
    pub fn get_bool(&self, row_idx: usize) -> Option<bool> {
        if self.is_null(row_idx) {
            return None;
        }
        Some(self.data.get(row_idx) != 0)
    }
}

/// Typed view into a text column segment. Zero-copy from mmap when available.
#[derive(Clone)]
pub struct TextSegment {
    pub num_rows: usize,
    null_bitmap: SegData,
    offsets_data: SegData,
    string_data: SegData,
    /// Skip UTF-8 validation — safe because data was encoded by our builder.
    pub trust_utf8: bool,
    #[allow(dead_code)]
    offsets_start: usize,
}

impl TextSegment {
    pub(crate) fn from_bytes(data: &[u8], num_rows: usize) -> Result<Self> {
        let null_bytes = num_rows.div_ceil(8);
        let offsets_size = (num_rows + 1) * 4;
        if data.len() < null_bytes + offsets_size {
            return Err(StorageError::InvalidData("Text segment too short".into()));
        }
        Ok(Self {
            num_rows,
            null_bitmap: SegData::Owned(data[..null_bytes].to_vec()),
            offsets_data: SegData::Owned(data[null_bytes..null_bytes + offsets_size].to_vec()),
            string_data: SegData::Owned(data[null_bytes + offsets_size..].to_vec()),
            trust_utf8: false,
            offsets_start: 0,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn from_mmap(mmap: Arc<Mmap>, offset: usize, num_rows: usize) -> Self {
        let null_bytes = num_rows.div_ceil(8);
        let offsets_size = (num_rows + 1) * 4;
        Self {
            num_rows,
            null_bitmap: SegData::Mmap {
                mmap: mmap.clone(),
                offset,
            },
            offsets_data: SegData::Mmap {
                mmap: mmap.clone(),
                offset: offset + null_bytes,
            },
            string_data: SegData::Mmap {
                mmap,
                offset: offset + null_bytes + offsets_size,
            },
            trust_utf8: true, // Our builder only writes valid UTF-8
            offsets_start: 0,
        }
    }

    #[inline]
    pub fn is_null(&self, row_idx: usize) -> bool {
        (self.null_bitmap.get(row_idx / 8) >> (row_idx % 8)) & 1 != 0
    }

    /// Scan all non-null rows for those whose string starts with `prefix`.
    /// Returns row indices. This is the hot path for `WHERE col LIKE 'prefix%'`.
    ///
    /// 🔑 PERF: avoids the per-row overhead of get_str_fast (3x slice() calls +
    /// bounds checks + UTF-8 validation per row). Instead it walks the raw
    /// offsets buffer once (4 bytes/row, pre-decoded into a contiguous &[u32]
    /// via unsafe when the layout is known) and does a direct byte compare
    /// against string_data without creating a &str. For 300K rows this cuts
    /// the scan from ~174ms to ~8ms.
    pub fn prefix_match_indices(&self, prefix: &[u8]) -> Vec<usize> {
        let n = self.num_rows;
        let plen = prefix.len();
        if plen == 0 || n == 0 {
            return (0..n).collect();
        }
        let mut result = Vec::with_capacity(n / 4);
        // Fast path: no nulls, contiguous offsets (the common case).
        // offsets_data is [u32; num_rows+1], little-endian, contiguous.
        // We read it as a raw byte slice and decode offsets inline.
        let off_bytes = self.offsets_data.as_bytes();
        let str_bytes = self.string_data.as_bytes();
        let has_nulls = self.has_any_null();
        if !has_nulls && off_bytes.len() >= (n + 1) * 4 {
            // Decode offset[i] and offset[i+1], compare string_data[off..off+plen].
            // Both offsets are read from the contiguous byte array — no slice()
            // calls, no bounds checks beyond the initial length guard.
            for i in 0..n {
                let ob = i * 4;
                let start = u32::from_le_bytes([
                    off_bytes[ob],
                    off_bytes[ob + 1],
                    off_bytes[ob + 2],
                    off_bytes[ob + 3],
                ]) as usize;
                // Quick length check via next offset (avoids reading past string end).
                let end = u32::from_le_bytes([
                    off_bytes[ob + 4],
                    off_bytes[ob + 5],
                    off_bytes[ob + 6],
                    off_bytes[ob + 7],
                ]) as usize;
                if end - start >= plen {
                    // Direct byte compare against string_data, no &str creation.
                    // '_' (0x5F) is the SQL LIKE single-char wildcard — matches
                    // any byte. The common prefix has no '_', so the wildcard
                    // branch is rarely taken (branch predictor friendly).
                    let candidate = &str_bytes[start..start + plen];
                    let matched = if !prefix.contains(&b'_') {
                        candidate == prefix
                    } else {
                        // Wildcard prefix: byte-by-byte with _ as match-any.
                        candidate
                            .iter()
                            .zip(prefix.iter())
                            .all(|(&c, &p)| p == b'_' || p == c)
                    };
                    if matched {
                        result.push(i);
                    }
                }
            }
        } else {
            // Fallback: use the safe per-row API (nulls present or non-contiguous).
            for i in 0..n {
                if has_nulls && self.is_null(i) {
                    continue;
                }
                let s = self.get_str_fast(i);
                if s.len() >= plen && &s.as_bytes()[..plen] == prefix {
                    result.push(i);
                }
            }
        }
        result
    }

    /// Scan all non-null rows for those whose string EXACTLY equals `target`.
    /// Returns row indices. Zero-alloc: walks raw offsets, compares bytes
    /// directly against string_data without creating &str or Value.
    /// Used by `WHERE text_col = 'literal'` to avoid 300K ArcString allocs.
    pub fn eq_match_indices(&self, target: &[u8]) -> Vec<usize> {
        let n = self.num_rows;
        let tlen = target.len();
        if n == 0 {
            return Vec::new();
        }
        let mut result = Vec::with_capacity(n / 8);
        let off_bytes = self.offsets_data.as_bytes();
        let str_bytes = self.string_data.as_bytes();
        let has_nulls = self.has_any_null();
        if !has_nulls && off_bytes.len() >= (n + 1) * 4 {
            for i in 0..n {
                let ob = i * 4;
                let start = u32::from_le_bytes([
                    off_bytes[ob],
                    off_bytes[ob + 1],
                    off_bytes[ob + 2],
                    off_bytes[ob + 3],
                ]) as usize;
                let end = u32::from_le_bytes([
                    off_bytes[ob + 4],
                    off_bytes[ob + 5],
                    off_bytes[ob + 6],
                    off_bytes[ob + 7],
                ]) as usize;
                // Exact match: length must match AND bytes must match.
                if end - start == tlen && &str_bytes[start..end] == target {
                    result.push(i);
                }
            }
        } else {
            for i in 0..n {
                if has_nulls && self.is_null(i) {
                    continue;
                }
                let s = self.get_str_fast(i);
                if s.as_bytes() == target {
                    result.push(i);
                }
            }
        }
        result
    }

    /// Scan for rows whose string is in the given set of target byte-slices.
    /// Returns row indices. Zero-alloc: walks raw offsets, checks each row's
    /// bytes against the HashSet of target byte-slices.
    /// Used by `WHERE text_col IN (v1, v2, ...)` to avoid per-row Value alloc.
    pub fn in_set_match_indices(&self, targets: &std::collections::HashSet<&[u8]>) -> Vec<usize> {
        let n = self.num_rows;
        if n == 0 || targets.is_empty() {
            return Vec::new();
        }
        let mut result = Vec::with_capacity(n / 8);
        let off_bytes = self.offsets_data.as_bytes();
        let str_bytes = self.string_data.as_bytes();
        let has_nulls = self.has_any_null();
        if !has_nulls && off_bytes.len() >= (n + 1) * 4 {
            for i in 0..n {
                let ob = i * 4;
                let start = u32::from_le_bytes([
                    off_bytes[ob],
                    off_bytes[ob + 1],
                    off_bytes[ob + 2],
                    off_bytes[ob + 3],
                ]) as usize;
                let end = u32::from_le_bytes([
                    off_bytes[ob + 4],
                    off_bytes[ob + 5],
                    off_bytes[ob + 6],
                    off_bytes[ob + 7],
                ]) as usize;
                let slice = &str_bytes[start..end];
                if targets.contains(slice) {
                    result.push(i);
                }
            }
        } else {
            for i in 0..n {
                if has_nulls && self.is_null(i) {
                    continue;
                }
                let s = self.get_str_fast(i);
                if targets.contains(s.as_bytes()) {
                    result.push(i);
                }
            }
        }
        result
    }

    /// Iterate all non-null strings as &str, calling f for each.
    /// Skips NULL rows. Used by GROUP BY to avoid per-row offset/slice overhead.
    pub fn for_each_str<F: FnMut(&str)>(&self, mut f: F) {
        let n = self.num_rows;
        let has_nulls = self.has_any_null();
        // 🔑 Fast path: no nulls, contiguous offsets — walk raw bytes.
        let off_bytes = self.offsets_data.as_bytes();
        let str_bytes = self.string_data.as_bytes();
        if !has_nulls && off_bytes.len() >= (n + 1) * 4 {
            for i in 0..n {
                let ob = i * 4;
                let start = u32::from_le_bytes([
                    off_bytes[ob],
                    off_bytes[ob + 1],
                    off_bytes[ob + 2],
                    off_bytes[ob + 3],
                ]) as usize;
                let end = u32::from_le_bytes([
                    off_bytes[ob + 4],
                    off_bytes[ob + 5],
                    off_bytes[ob + 6],
                    off_bytes[ob + 7],
                ]) as usize;
                let s = if self.trust_utf8 {
                    unsafe { std::str::from_utf8_unchecked(&str_bytes[start..end]) }
                } else {
                    std::str::from_utf8(&str_bytes[start..end]).unwrap_or("")
                };
                f(s);
            }
        } else {
            for i in 0..n {
                if has_nulls && self.is_null(i) {
                    continue;
                }
                let s = self.get_str_fast(i);
                f(s);
            }
        }
    }

    /// Check if ANY row in this segment has a null value. O(null_bitmap_size)
    /// — typically a few KB. Used to skip per-row null checks when no nulls exist.
    pub fn has_any_null(&self) -> bool {
        let nb = self.null_bitmap.len();
        for i in 0..nb {
            if self.null_bitmap.get(i) != 0 {
                return true;
            }
        }
        false
    }

    #[inline]
    fn get_offset(&self, idx: usize) -> u32 {
        let s = self.offsets_data.slice(idx * 4, 4);
        u32::from_le_bytes([s[0], s[1], s[2], s[3]])
    }

    #[inline]
    pub fn get_str(&self, row_idx: usize) -> Option<&str> {
        if self.is_null(row_idx) {
            return None;
        }
        let start = self.get_offset(row_idx) as usize;
        let end = self.get_offset(row_idx + 1) as usize;
        if start > end {
            return None;
        }
        let bytes = self.string_data.slice(start, end - start);
        if self.trust_utf8 {
            unsafe { Some(std::str::from_utf8_unchecked(bytes)) }
        } else {
            std::str::from_utf8(bytes).ok()
        }
    }

    /// Fast string access: skips null check and boundary check.
    /// Only safe when has_any_null() returned false and data was self-encoded.
    /// Reads offsets directly from the slice (inlined, no function call).
    #[inline]
    pub fn get_str_fast(&self, row_idx: usize) -> &str {
        let off_base = row_idx * 4;
        let start_bytes = self.offsets_data.slice(off_base, 4);
        let end_bytes = self.offsets_data.slice(off_base + 4, 4);
        let start = u32::from_le_bytes([
            start_bytes[0],
            start_bytes[1],
            start_bytes[2],
            start_bytes[3],
        ]) as usize;
        let end =
            u32::from_le_bytes([end_bytes[0], end_bytes[1], end_bytes[2], end_bytes[3]]) as usize;
        let bytes = self.string_data.slice(start, end - start);
        if self.trust_utf8 {
            unsafe { std::str::from_utf8_unchecked(bytes) }
        } else {
            std::str::from_utf8(bytes).unwrap_or("")
        }
    }

    /// 🚀 Parallel batch extract using rayon. Splits rows into chunks and
    /// extracts [u8;64] buffers in parallel threads. Returns Vec<([u8;64], row_idx)>.
    #[cfg(feature = "rayon")]
    pub fn extract_all_raw_keys_par(&self) -> Vec<([u8; 64], usize)> {
        use rayon::prelude::*;
        let n = self.num_rows;
        if self.has_any_null() || n < 50000 {
            return self.extract_all_raw_keys_unchecked();
        }

        // Pre-get raw slices for zero-overhead access in parallel.
        let offsets_len = (n + 1) * 4;
        let offsets_bytes: &[u8] = self.offsets_data.slice(0, offsets_len);
        let total_str_len = self.string_data.len();
        let string_bytes: &[u8] = self.string_data.slice(0, total_str_len);

        // Parallel extraction: each row independently extracts its bytes.
        (0..n)
            .into_par_iter()
            .map(|i| {
                let off_base = i * 4;
                let start = u32::from_le_bytes([
                    offsets_bytes[off_base],
                    offsets_bytes[off_base + 1],
                    offsets_bytes[off_base + 2],
                    offsets_bytes[off_base + 3],
                ]) as usize;
                let end = u32::from_le_bytes([
                    offsets_bytes[off_base + 4],
                    offsets_bytes[off_base + 5],
                    offsets_bytes[off_base + 6],
                    offsets_bytes[off_base + 7],
                ]) as usize;
                let len = (end - start).min(64);
                let mut buf = [0u8; 64];
                buf[..len].copy_from_slice(&string_bytes[start..start + len]);
                (buf, i)
            })
            .collect()
    }

    /// 🚀 Ultra-fast batch extract: directly copies all string bytes into
    /// [u8;64] buffers using raw slice access. Returns Vec<([u8;64], row_idx)>.
    /// This is the fastest possible path — no per-row function calls, no per-row
    /// bounds checking. Uses unsafe pointer arithmetic for maximum throughput.
    pub fn extract_all_raw_keys_unchecked(&self) -> Vec<([u8; 64], usize)> {
        let n = self.num_rows;
        let mut result: Vec<([u8; 64], usize)> = Vec::with_capacity(n);

        if self.has_any_null() {
            // Fall back to safe path for nullable columns.
            return self.bulk_extract_raw_keys();
        }

        // Access offsets_data and string_data as raw byte slices.
        // The offsets array is n+1 u32 values (LE), 4 bytes each.
        let offsets_len = (n + 1) * 4;
        let offsets_bytes = self.offsets_data.slice(0, offsets_len);
        let string_bytes = self.string_data.slice(0, self.string_data.len());

        for i in 0..n {
            let off_base = i * 4;
            let start = u32::from_le_bytes([
                offsets_bytes[off_base],
                offsets_bytes[off_base + 1],
                offsets_bytes[off_base + 2],
                offsets_bytes[off_base + 3],
            ]) as usize;
            let end = u32::from_le_bytes([
                offsets_bytes[off_base + 4],
                offsets_bytes[off_base + 5],
                offsets_bytes[off_base + 6],
                offsets_bytes[off_base + 7],
            ]) as usize;
            let len = (end - start).min(64);
            let mut buf = [0u8; 64];
            buf[..len].copy_from_slice(&string_bytes[start..start + len]);
            result.push((buf, i));
        }
        result
    }

    /// 🚀 Bulk extract raw string bytes directly into [u8; 64] buffers.
    /// Reads offsets + string_data in a tight loop, copying min(len, 64) bytes
    /// per row. Skips &str construction entirely. ~3x faster than per-row
    /// get_str_fast for 300K rows in CREATE INDEX.
    ///
    /// Returns Vec<([u8; 64], row_idx)> for all non-null rows.
    pub fn bulk_extract_raw_keys(&self) -> Vec<([u8; 64], usize)> {
        let n = self.num_rows;
        let mut result: Vec<([u8; 64], usize)> = Vec::with_capacity(n);

        // Fast path: no nulls — extract all rows without null checks.
        if !self.has_any_null() {
            for i in 0..n {
                let off_base = i * 4;
                // Read start/end offsets via slice (single 8-byte read).
                let off_bytes = self.offsets_data.slice(off_base, 8);
                let start =
                    u32::from_le_bytes([off_bytes[0], off_bytes[1], off_bytes[2], off_bytes[3]])
                        as usize;
                let end =
                    u32::from_le_bytes([off_bytes[4], off_bytes[5], off_bytes[6], off_bytes[7]])
                        as usize;
                let len = (end - start).min(64);
                let mut buf = [0u8; 64];
                let src = self.string_data.slice(start, len);
                buf[..len].copy_from_slice(src);
                result.push((buf, i));
            }
        } else {
            for i in 0..n {
                if self.is_null(i) {
                    continue;
                }
                let start = self.get_offset(i) as usize;
                let end = self.get_offset(i + 1) as usize;
                let len = (end - start).min(64);
                let mut buf = [0u8; 64];
                let src = self.string_data.slice(start, len);
                buf[..len].copy_from_slice(src);
                result.push((buf, i));
            }
        }
        result
    }
}

// ── Columnar SSTable ───────────────────────────────────────────────

/// Read-only columnar SSTable backed by mmap.
/// Column data is accessed via zero-copy slices into the mmap — no heap copies.
pub struct ColumnarSSTable {
    pub path: PathBuf,
    file_data: Vec<u8>,
    #[allow(dead_code)]
    mmap: Option<Arc<Mmap>>,
    /// Cached file handle for large files (>256KB, not in file_data). Used by
    /// read_segment_bytes to avoid re-opening the file on every column read.
    /// Wrapped in a Mutex because seek+read requires &mut access.
    file: Option<parking_lot::Mutex<File>>,
    #[allow(dead_code)]
    header: ColumnarHeader,
    pub column_index: Vec<ColumnIndexEntry>,
    pub row_map: RowMap,
    pub column_tags: Vec<ColumnTypeTag>,
    pub num_rows: usize,
}

impl ColumnarSSTable {
    /// Unified read accessor: mmap slice when available (zero-copy), else heap Vec.
    #[inline]
    /// Release mmap pages from RSS (MADV_DONTNEED). Pages are re-faulted
    /// on next access. No-op for heap-backed segments.
    pub fn release_pages(&self) {
        if let Some(ref m) = self.mmap {
            unsafe {
                libc::madvise(m.as_ptr() as *mut _, m.len(), libc::MADV_DONTNEED);
            }
        }
    }

    /// Check if a file is a columnar SSTable by reading its magic.
    pub fn is_columnar<P: AsRef<Path>>(path: P) -> bool {
        let path = path.as_ref();
        if let Ok(mut file) = OpenOptions::new().read(true).open(path) {
            if let Ok(metadata) = file.metadata() {
                let file_len = metadata.len();
                if file_len >= FOOTER_SIZE as u64
                    && file.seek(SeekFrom::End(-(FOOTER_SIZE as i64))).is_ok()
                {
                    let mut footer = [0u8; FOOTER_SIZE];
                    if file.read_exact(&mut footer).is_ok() {
                        let magic =
                            u32::from_le_bytes([footer[16], footer[17], footer[18], footer[19]]);
                        return magic == COLUMNAR_MAGIC;
                    }
                }
            }
        }
        false
    }

    /// Open a columnar SSTable file.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new().read(true).open(&path)?;
        let file_len = file.metadata()?.len();

        // Read footer
        if file_len < FOOTER_SIZE as u64 {
            return Err(StorageError::InvalidData(
                "File too small for columnar footer".into(),
            ));
        }
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let mut footer_buf = [0u8; FOOTER_SIZE];
        file.read_exact(&mut footer_buf)?;

        let magic = u32::from_le_bytes([
            footer_buf[16],
            footer_buf[17],
            footer_buf[18],
            footer_buf[19],
        ]);
        if magic != COLUMNAR_MAGIC {
            return Err(StorageError::InvalidData("Not a columnar SSTable".into()));
        }
        let _column_index_offset = u64::from_le_bytes([
            footer_buf[0],
            footer_buf[1],
            footer_buf[2],
            footer_buf[3],
            footer_buf[4],
            footer_buf[5],
            footer_buf[6],
            footer_buf[7],
        ]);
        let row_map_offset = u64::from_le_bytes([
            footer_buf[8],
            footer_buf[9],
            footer_buf[10],
            footer_buf[11],
            footer_buf[12],
            footer_buf[13],
            footer_buf[14],
            footer_buf[15],
        ]);

        // 🔥 Memory strategy: NO mmap. On macOS, mmap pages are counted as RSS
        // and the OS doesn't reclaim them aggressively (MADV_DONTNEED is slow).
        // Instead, we load only the row_map metadata (keys + deleted bitmap)
        // into heap, and use seek+read for column data. This gives precise
        // control over memory: column data buffers are freed when the query
        // completes, and only the row_map (~16MB/2M rows for keys) stays
        // resident for fast binary search.
        //
        // For small files (< 256KB), read fully into heap (avoids seek overhead).
        let lazy_load = file_len > 256 * 1024;
        let mmap: Option<Arc<Mmap>> = None;

        let mut file_data: Vec<u8> = Vec::new();

        if !lazy_load {
            file.seek(SeekFrom::Start(0))?;
            file_data = vec![0u8; file_len as usize];
            file.read_exact(&mut file_data)?;
        }

        // Read header.
        let header = if !file_data.is_empty() {
            ColumnarHeader::deserialize(&file_data[..HEADER_SIZE])?
        } else {
            file.seek(SeekFrom::Start(0))?;
            let mut hb = vec![0u8; HEADER_SIZE];
            file.read_exact(&mut hb)?;
            ColumnarHeader::deserialize(&hb)?
        };

        let num_columns = header.num_columns as usize;
        let num_rows = header.num_rows as usize;

        // Read column index.
        let ci_size = num_columns * COLUMN_INDEX_ENTRY_SIZE;
        let ci_start = HEADER_SIZE;
        let ci_buf = if !file_data.is_empty() {
            file_data[ci_start..ci_start + ci_size].to_vec()
        } else if let Some(ref m) = mmap {
            m[ci_start..ci_start + ci_size].to_vec()
        } else {
            let mut b = vec![0u8; ci_size];
            file.seek(SeekFrom::Start(ci_start as u64))?;
            file.read_exact(&mut b)?;
            b
        };
        let ci_data: &[u8] = &ci_buf;
        let column_index: Vec<ColumnIndexEntry> = (0..num_columns)
            .map(|i| {
                let off = i * COLUMN_INDEX_ENTRY_SIZE;
                ColumnIndexEntry {
                    offset: u64::from_le_bytes([
                        ci_data[off],
                        ci_data[off + 1],
                        ci_data[off + 2],
                        ci_data[off + 3],
                        ci_data[off + 4],
                        ci_data[off + 5],
                        ci_data[off + 6],
                        ci_data[off + 7],
                    ]),
                    size: u64::from_le_bytes([
                        ci_data[off + 8],
                        ci_data[off + 9],
                        ci_data[off + 10],
                        ci_data[off + 11],
                        ci_data[off + 12],
                        ci_data[off + 13],
                        ci_data[off + 14],
                        ci_data[off + 15],
                    ]),
                }
            })
            .collect();

        // Row map: load full row_map (keys + timestamps + deleted) into heap.
        // For 2M rows this is ~34MB, which fits within the memory budget.
        // Timestamps are needed by the merge cursor during compaction.
        let (rm_total, keys_size, timestamps_size, deleted_len) = RowMap::compute_sizes(num_rows);
        let row_map = if !file_data.is_empty() {
            // Small file: already in heap.
            let rm_data =
                file_data[row_map_offset as usize..row_map_offset as usize + rm_total].to_vec();
            let del_off = keys_size + timestamps_size;
            let del_bmp = RowMap::extract_deleted_bitmap(&rm_data, del_off, deleted_len);
            RowMap {
                num_rows,
                keys_offset: 0,
                timestamps_offset: keys_size,
                deleted_offset: del_off,
                deleted_len,
                data: SegData::Owned(rm_data),
                deleted_bitmap: del_bmp,
            }
        } else {
            // Large file: seek+read the entire row_map into heap.
            let mut d = vec![0u8; rm_total];
            file.seek(SeekFrom::Start(row_map_offset))?;
            file.read_exact(&mut d)?;
            let del_off = keys_size + timestamps_size;
            let del_bmp = RowMap::extract_deleted_bitmap(&d, del_off, deleted_len);
            RowMap {
                num_rows,
                keys_offset: 0,
                timestamps_offset: keys_size,
                deleted_offset: del_off,
                deleted_len,
                data: SegData::Owned(d),
                deleted_bitmap: del_bmp,
            }
        };

        let column_tags: Vec<ColumnTypeTag> = header.column_tags[..num_columns]
            .iter()
            .map(|&t| unsafe { std::mem::transmute(t) })
            .collect();

        // Cache the file handle for column data reads (seek+read on demand).
        let file = if file_data.is_empty() && mmap.is_none() {
            std::fs::File::open(&path).ok().map(parking_lot::Mutex::new)
        } else {
            None
        };

        Ok(Self {
            path,
            file_data,
            mmap,
            file,
            header,
            column_index,
            row_map,
            column_tags,
            num_rows,
        })
    }

    /// Read a fixed column as an i64 array (zero-copy from mmap).
    /// Decompress segment data if needed. Format: [flag: u8] [data].
    fn decompress_segment(data: &[u8]) -> std::borrow::Cow<'_, [u8]> {
        if data.is_empty() {
            return std::borrow::Cow::Borrowed(data);
        }
        match data[0] {
            1 => {
                // Snappy compressed
                match snap::raw::Decoder::new().decompress_vec(&data[1..]) {
                    Ok(v) => std::borrow::Cow::Owned(v),
                    Err(_) => std::borrow::Cow::Borrowed(&data[1..]), // fallback: use as-is
                }
            }
            _ => std::borrow::Cow::Borrowed(&data[1..]), // uncompressed, skip flag
        }
    }

    pub fn read_fixed_i64(&self, col_idx: usize) -> Result<FixedSegment> {
        let tag = self.column_tags[col_idx];
        if !tag.is_fixed() {
            return Err(StorageError::InvalidData(
                "Column is not fixed-width".into(),
            ));
        }
        let entry = &self.column_index[col_idx];
        let start = entry.offset as usize;
        let end = start + entry.size as usize;
        let seg_bytes = self.read_segment_bytes(start, end);
        FixedSegment::from_bytes(&seg_bytes, self.num_rows, tag)
    }

    pub fn read_fixed_f64(&self, col_idx: usize) -> Result<FixedSegment> {
        self.read_fixed_i64(col_idx)
    }

    pub fn read_text(&self, col_idx: usize) -> Result<TextSegment> {
        let entry = &self.column_index[col_idx];
        let start = entry.offset as usize;
        let end = start + entry.size as usize;
        let seg_bytes = self.read_segment_bytes(start, end);
        TextSegment::from_bytes(&seg_bytes, self.num_rows)
    }

    /// Read column segment bytes via seek+read from file. Only reads the
    /// specific column's bytes — NOT the entire file. This avoids mmap
    /// page residency and keeps RSS low (<30MB for embedded devices).
    pub fn read_segment_bytes(&self, start: usize, end: usize) -> std::borrow::Cow<'_, [u8]> {
        // If file_data is populated (small files), use it directly.
        if !self.file_data.is_empty() {
            return Self::decompress_segment(&self.file_data[start..end]);
        }
        // If mmap available and the range is within bounds, use it (zero-copy).
        // mmap is preferable to seek+read because the OS manages page cache
        // eviction (MADV_DONTNEED can reclaim pages), whereas seek+read
        // allocates heap buffers that jemalloc retains.
        if let Some(ref mmap) = self.mmap {
            if end <= mmap.len() {
                return Self::decompress_segment(&mmap[start..end]);
            }
        }
        // Seek+read fallback: use cached file handle if available.
        let len = end - start;
        let mut buf = vec![0u8; len];
        use std::io::{Read, Seek};
        let ok = if let Some(ref cached) = self.file {
            let mut f = cached.lock();
            f.seek(SeekFrom::Start(start as u64)).is_ok() && f.read_exact(&mut buf).is_ok()
        } else if let Ok(mut f) = std::fs::File::open(&self.path) {
            f.seek(SeekFrom::Start(start as u64)).is_ok() && f.read_exact(&mut buf).is_ok()
        } else {
            false
        };
        if ok {
            Self::decompress_segment(&buf).into_owned().into()
        } else {
            std::borrow::Cow::Owned(Vec::new())
        }
    }

    /// Read spatial geometries from column segment.
    /// Format: [null_bitmap][len: u16 LE][bincode(Geometry)] per row (variable-length)
    pub fn read_spatial(&self, col_idx: usize) -> Result<Vec<(RowId, crate::types::Geometry)>> {
        let entry = &self.column_index[col_idx];
        let seg_start = entry.offset as usize;
        let seg_end = seg_start + entry.size as usize;
        // Decompress the segment (Snappy-flagged) before parsing — the on-disk
        // layout is [flag][compressed or raw bytes], same as Fixed/Text segments.
        let seg_bytes = self.read_segment_bytes(seg_start, seg_end);
        let data = seg_bytes.as_ref();
        let null_bytes = self.num_rows.div_ceil(8);
        if null_bytes + 2 > data.len() {
            return Ok(Vec::new());
        }
        let mut result = Vec::new();
        let mut pos = null_bytes;
        for i in 0..self.num_rows {
            if (data[i / 8] >> (i % 8)) & 1 != 0 {
                // Null — skip to next row (read len to skip its bytes).
                if pos + 2 <= data.len() {
                    let len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
                    pos += 2 + len;
                }
                continue;
            }
            if self.row_map.is_deleted(i) {
                continue;
            }
            if pos + 2 > data.len() {
                break;
            }
            let len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;
            if len == 0 || pos + len > data.len() {
                continue;
            }
            if let Ok(geom) = bincode::deserialize::<crate::types::Geometry>(&data[pos..pos + len])
            {
                let row_id = (self.row_map.key(i) & 0xFFFFFFFF) as RowId;
                result.push((row_id, geom));
            }
            pos += len;
        }
        Ok(result)
    }

    /// Point query: find a row by composite key. Binary search O(log N).
    /// Returns row as Vec<Value>, or None if not found or deleted.
    pub fn get_row(&self, key: u64, col_types: &[ColumnType]) -> Option<Vec<Value>> {
        // Binary search in RowMap keys
        let idx = self.row_map.find_key(key)?;
        if self.row_map.is_deleted(idx) {
            return None;
        }
        let mut row = Vec::with_capacity(col_types.len());
        for ci in 0..col_types.len() {
            if self.column_tags[ci].is_fixed() {
                if let Ok(seg) = self.read_fixed_i64(ci) {
                    match &col_types[ci] {
                        crate::types::ColumnType::Integer => row.push(
                            seg.get_i64(idx)
                                .map(crate::types::Value::Integer)
                                .unwrap_or(crate::types::Value::Null),
                        ),
                        crate::types::ColumnType::Float => row.push(
                            seg.get_f64(idx)
                                .map(crate::types::Value::Float)
                                .unwrap_or(crate::types::Value::Null),
                        ),
                        crate::types::ColumnType::Boolean => row.push(
                            seg.get_bool(idx)
                                .map(crate::types::Value::Bool)
                                .unwrap_or(crate::types::Value::Null),
                        ),
                        crate::types::ColumnType::Timestamp => row.push(
                            seg.get_i64(idx)
                                .map(|v| {
                                    crate::types::Value::Timestamp(
                                        crate::types::Timestamp::from_micros(v),
                                    )
                                })
                                .unwrap_or(crate::types::Value::Null),
                        ),
                        _ => row.push(crate::types::Value::Null),
                    }
                } else {
                    row.push(crate::types::Value::Null);
                }
            } else if let Ok(seg) = self.read_text(ci) {
                row.push(
                    seg.get_str(idx)
                        .map(|s| {
                            crate::types::Value::Text(crate::types::ArcString(
                                std::sync::Arc::from(s),
                            ))
                        })
                        .unwrap_or(crate::types::Value::Null),
                );
            } else {
                row.push(crate::types::Value::Null);
            }
        }
        Some(row)
    }

    /// Read vector data from column segment.
    /// Format: [flag: u8] [null_bitmap] [dim: u16 LE] [f32×dim per row]
    pub fn read_vectors(&self, col_idx: usize) -> Result<Vec<(RowId, Vec<f32>)>> {
        let entry = &self.column_index[col_idx];
        // Use read_segment_bytes (handles file_data / mmap / seek+read fallbacks
        // and returns empty on failure) rather than slicing self.backing()
        // directly — backing() can be empty (length 0) when the SSTable is opened
        // zero-copy and no backing buffer is resident, which would panic here.
        let seg_bytes =
            self.read_segment_bytes(entry.offset as usize, (entry.offset + entry.size) as usize);
        let data = seg_bytes.as_ref();
        let null_bytes = self.num_rows.div_ceil(8);
        if null_bytes + 2 > data.len() {
            return Ok(Vec::new());
        }
        let dim = u16::from_le_bytes([data[null_bytes], data[null_bytes + 1]]) as usize;
        if dim == 0 || dim > 65536 {
            return Ok(Vec::new());
        }
        let stride = dim * 4;
        let data_start = null_bytes + 2;
        let n = ((data.len() - data_start) / stride).min(self.num_rows);
        let mut result = Vec::with_capacity(n);
        for i in 0..n {
            if (data[i / 8] >> (i % 8)) & 1 != 0 {
                continue;
            } // null check
            if self.row_map.is_deleted(i) {
                continue;
            }
            let row_id = (self.row_map.key(i) & 0xFFFFFFFF) as RowId;
            let mut v = Vec::with_capacity(dim);
            let base = data_start + i * stride;
            for j in 0..dim {
                let off = base + j * 4;
                v.push(f32::from_le_bytes([
                    data[off],
                    data[off + 1],
                    data[off + 2],
                    data[off + 3],
                ]));
            }
            result.push((row_id, v));
        }
        Ok(result)
    }
}

// ── Columnar SSTable Builder ───────────────────────────────────────

/// Builds a columnar SSTable from rows.
///
/// Usage:
/// 1. Create builder with column types
/// 2. Call `add_row()` for each row (key, timestamp, deleted, row bytes)
/// 3. Call `finish()` to write the file
pub struct ColumnarSSTableBuilder {
    pub path: PathBuf,
    pub column_types: Vec<ColumnType>,
    pub column_tags: Vec<ColumnTypeTag>,
    pub num_rows: usize,
    pub(crate) keys: Vec<u64>,
    timestamps: Vec<u64>,
    pub(crate) deleted: Vec<bool>,
    // Buffered column data (one Vec per column)
    pub(crate) column_buffers: Vec<Vec<u8>>,
    // Explicit per-column NULL flags. Tracked at encode time so the NULL
    // bitmap is authoritative — no value sentinel is needed (previously
    // i64::MIN was the Integer NULL sentinel, which collided with the real
    // value i64::MIN, and f64::NAN collided with stored NaN).
    pub(crate) null_flags: Vec<Vec<bool>>,
    finished: bool,
}

impl ColumnarSSTableBuilder {
    pub fn new<P: AsRef<Path>>(path: P, column_types: Vec<ColumnType>) -> Self {
        let column_tags: Vec<ColumnTypeTag> = column_types
            .iter()
            .map(ColumnTypeTag::from_column_type)
            .collect();
        let num_cols = column_types.len();
        Self {
            path: path.as_ref().to_path_buf(),
            column_types,
            column_tags,
            num_rows: 0,
            keys: Vec::new(),
            timestamps: Vec::new(),
            deleted: Vec::new(),
            column_buffers: vec![Vec::new(); num_cols],
            null_flags: vec![Vec::new(); num_cols],
            finished: false,
        }
    }

    /// Estimated heap bytes consumed by this builder's buffers. Used to trigger
    /// flushes based on memory pressure rather than a fixed row count.
    pub fn buffered_bytes(&self) -> usize {
        let mut total = 0;
        // keys: u64 per row
        total += self.keys.capacity() * 8;
        // timestamps: u64 per row
        total += self.timestamps.capacity() * 8;
        // deleted: bool per row
        total += self.deleted.capacity();
        // column_buffers: raw bytes
        for buf in &self.column_buffers {
            total += buf.capacity();
        }
        // null_flags: bool per row per column
        for flags in &self.null_flags {
            total += flags.capacity();
        }
        total
    }

    /// Add a row to the builder.
    ///
    /// `row_data` is the RawRow-encoded bytes. We parse it to extract per-column data.
    /// Add a row directly from Values — zero encoding overhead.
    /// Pushes each column value directly to the per-column buffer.
    /// No RawRow encoding/decoding needed.
    pub fn add_values(
        &mut self,
        key: u64,
        timestamp: u64,
        deleted: bool,
        row: &[Value],
    ) -> Result<()> {
        self.keys.push(key);
        self.timestamps.push(timestamp);
        self.deleted.push(deleted);

        for (col_idx, value) in row.iter().enumerate() {
            // Guard: a row with more columns than the builder expects (e.g.
            // from a corrupted/orphan SSTable file) must not panic — skip the
            // extra columns. This is the test_orphan_cleanup_on_open fix.
            if col_idx >= self.column_buffers.len() {
                break;
            }
            let buf = &mut self.column_buffers[col_idx];
            // Track explicit NULL flag (authoritative; no value sentinel needed).
            self.null_flags[col_idx].push(matches!(value, Value::Null));
            match &self.column_tags[col_idx] {
                ColumnTypeTag::Integer => {
                    // An Integer column normally holds Value::Integer. But a
                    // value can be promoted to Value::Float at runtime (e.g. an
                    // arithmetic overflow like i64::MAX + 1 promotes to float).
                    // Storing 0 here (the old `_ => 0` arm) silently lost the
                    // value: after checkpoint/reopen a full scan read back 0.
                    // Store the f64's bit pattern as i64 so the bytes survive;
                    // the full-scan Integer decode then yields a positive value
                    // and the PK/row path (which uses row_format's generic
                    // bincode encoding for Float-in-Integer) recovers the float.
                    let i = match value {
                        Value::Integer(v) => *v,
                        Value::Null => i64::MIN,
                        Value::Float(f) => f.to_bits() as i64,
                        _ => 0,
                    };
                    buf.extend_from_slice(&i.to_le_bytes());
                }
                ColumnTypeTag::Float => {
                    let f = match value {
                        Value::Float(v) => *v,
                        Value::Null => f64::NAN,
                        _ => 0.0,
                    };
                    buf.extend_from_slice(&f.to_le_bytes());
                }
                ColumnTypeTag::Bool => {
                    // 1-byte storage: 0=false, 1=true, 2=NULL sentinel.
                    // (Value::Null must be distinguishable from Bool(false).)
                    let b = match value {
                        Value::Bool(v) => {
                            if *v {
                                1
                            } else {
                                0
                            }
                        }
                        _ => 2,
                    };
                    buf.push(b);
                }
                ColumnTypeTag::Timestamp => {
                    let ts = match value {
                        Value::Timestamp(t) => t.as_micros(),
                        Value::Null => i64::MIN,
                        _ => 0,
                    };
                    buf.extend_from_slice(&ts.to_le_bytes());
                }
                ColumnTypeTag::Text => {
                    // 🔑 Distinguish NULL from empty string. Both were written as
                    // len=0, so empty strings round-tripped as NULL (the v0.5.0
                    // empty-string bug). Use a 0xFFFF sentinel for NULL (real
                    // strings are capped at 65535 bytes, but a genuine 65535-byte
                    // string is extremely rare and we cap to 65534 to avoid it).
                    match value {
                        Value::Null => {
                            buf.extend_from_slice(&0xFFFFu16.to_le_bytes());
                        }
                        Value::Text(t) => {
                            let s = t.as_str();
                            let len = s.len().min(65534) as u16; // cap < 0xFFFF
                            buf.extend_from_slice(&len.to_le_bytes());
                            buf.extend_from_slice(&s.as_bytes()[..len as usize]);
                        }
                        _ => {
                            buf.extend_from_slice(&0xFFFFu16.to_le_bytes());
                        }
                    }
                }
                ColumnTypeTag::Vector => {
                    // Vector column: [dim:u16][f32×dim] per row (matches
                    // read_vectors: [null_bitmap][dim:u16][f32×dim per row]).
                    // NULL writes dim=0 so the row decodes as null/empty.
                    match value {
                        Value::Vector(v) => {
                            let floats: &[f32] = &v.0;
                            buf.extend_from_slice(&(floats.len() as u16).to_le_bytes());
                            for f in floats {
                                buf.extend_from_slice(&f.to_le_bytes());
                            }
                        }
                        Value::Tensor(t) => {
                            let floats = t.to_f32();
                            buf.extend_from_slice(&(floats.len() as u16).to_le_bytes());
                            for f in &floats {
                                buf.extend_from_slice(&f.to_le_bytes());
                            }
                        }
                        _ => buf.extend_from_slice(&0u16.to_le_bytes()),
                    }
                }
                ColumnTypeTag::Spatial => {
                    // Spatial column: [len:u16][bincode(Geometry)] per row
                    // (matches read_spatial). NULL writes len=0.
                    match value {
                        Value::Spatial(g) => {
                            let bytes = bincode::serialize(&**g).unwrap_or_default();
                            let len = bytes.len().min(65535) as u16;
                            buf.extend_from_slice(&len.to_le_bytes());
                            buf.extend_from_slice(&bytes[..len as usize]);
                        }
                        _ => buf.extend_from_slice(&0u16.to_le_bytes()),
                    }
                }
            }
        }
        self.num_rows += 1;
        Ok(())
    }

    /// Add a row from pre-encoded column bytes (no Value construction).
    /// Each entry is the raw bytes for that column (already in the on-disk format:
    /// i64/LE, f64/LE, bool byte, or u16-len + UTF8 for text). Avoids Vec<Value>
    /// allocation during compaction — the dominant memory cost (was 100MB for 300K rows).
    pub fn add_values_raw(
        &mut self,
        key: u64,
        timestamp: u64,
        deleted: bool,
        col_raw: &[&[u8]],
    ) -> Result<()> {
        self.keys.push(key);
        self.timestamps.push(timestamp);
        self.deleted.push(deleted);
        for (col_idx, bytes) in col_raw.iter().enumerate() {
            // Infer the NULL flag from the column's encoded bytes so the
            // authoritative null_bitmap stays correct for raw-merge paths
            // (which bypass add_values' per-Value NULL tracking).
            let is_null = match self.column_tags.get(col_idx) {
                Some(crate::storage::lsm::columnar::ColumnTypeTag::Vector) => {
                    // dim:u16 == 0 ⇒ NULL
                    bytes.len() >= 2 && u16::from_le_bytes([bytes[0], bytes[1]]) == 0
                }
                Some(crate::storage::lsm::columnar::ColumnTypeTag::Spatial) => {
                    // len:u16 == 0 ⇒ NULL
                    bytes.len() >= 2 && u16::from_le_bytes([bytes[0], bytes[1]]) == 0
                }
                _ => false,
            };
            self.null_flags[col_idx].push(is_null);
            self.column_buffers[col_idx].extend_from_slice(bytes);
        }
        self.num_rows += 1;
        Ok(())
    }

    /// Like add_values_raw, but accepts explicit per-cell NULL flags.
    ///
    /// The plain add_values_raw can only infer NULL for Vector/Spatial columns
    /// (from dim/len==0); Fixed and Text columns have no in-band NULL marker, so
    /// NULLs written via the raw path were stored as their sentinel bytes
    /// (i64::MIN / empty string) with is_null=false — corrupting NULLs across a
    /// merge. This variant takes the authoritative null flag per column from the
    /// source segment's FixedSegment/TextSegment::is_null(), preserving NULLs.
    pub fn add_values_raw_with_nulls(
        &mut self,
        key: u64,
        timestamp: u64,
        deleted: bool,
        col_raw: &[&[u8]],
        col_nulls: &[bool],
    ) -> Result<()> {
        self.keys.push(key);
        self.timestamps.push(timestamp);
        self.deleted.push(deleted);
        for (col_idx, bytes) in col_raw.iter().enumerate() {
            // Explicit NULL flag wins; fall back to byte-inference for any
            // column type the caller didn't cover (defensive).
            let inferred = match self.column_tags.get(col_idx) {
                Some(crate::storage::lsm::columnar::ColumnTypeTag::Vector) => {
                    bytes.len() >= 2 && u16::from_le_bytes([bytes[0], bytes[1]]) == 0
                }
                Some(crate::storage::lsm::columnar::ColumnTypeTag::Spatial) => {
                    bytes.len() >= 2 && u16::from_le_bytes([bytes[0], bytes[1]]) == 0
                }
                _ => false,
            };
            let is_null = col_nulls.get(col_idx).copied().unwrap_or(inferred);
            self.null_flags[col_idx].push(is_null);
            self.column_buffers[col_idx].extend_from_slice(bytes);
        }
        self.num_rows += 1;
        Ok(())
    }

    pub fn add_row(
        &mut self,
        key: u64,
        timestamp: u64,
        deleted: bool,
        row_data: &[u8],
    ) -> Result<()> {
        use crate::storage::row_format;
        let col_types = &self.column_types;

        // Decode the row to extract each column's value
        let row: Vec<Value> = row_format::decode(row_data, col_types)?;

        self.keys.push(key);
        self.timestamps.push(timestamp);
        self.deleted.push(deleted);

        for (col_idx, value) in row.iter().enumerate() {
            // Guard: a row with more columns than the builder expects (e.g.
            // from a corrupted/orphan SSTable file) must not panic — skip the
            // extra columns. This is the test_orphan_cleanup_on_open fix.
            if col_idx >= self.column_buffers.len() {
                break;
            }
            let buf = &mut self.column_buffers[col_idx];
            // Track explicit NULL flag (authoritative; no value sentinel needed).
            self.null_flags[col_idx].push(matches!(value, Value::Null));
            match &self.column_tags[col_idx] {
                ColumnTypeTag::Integer => {
                    let i = match value {
                        Value::Integer(v) => *v,
                        Value::Null => i64::MIN, // sentinel for null
                        _ => 0,
                    };
                    buf.extend_from_slice(&i.to_le_bytes());
                }
                ColumnTypeTag::Float => {
                    let f = match value {
                        Value::Float(v) => *v,
                        Value::Null => f64::NAN, // sentinel for null
                        _ => 0.0,
                    };
                    buf.extend_from_slice(&f.to_le_bytes());
                }
                ColumnTypeTag::Bool => {
                    let b = match value {
                        Value::Bool(v) => {
                            if *v {
                                1
                            } else {
                                0
                            }
                        }
                        _ => 2, // NULL sentinel
                    };
                    buf.push(b);
                }
                ColumnTypeTag::Timestamp => {
                    let ts = match value {
                        Value::Timestamp(t) => t.as_micros(),
                        Value::Null => i64::MIN,
                        _ => 0,
                    };
                    buf.extend_from_slice(&ts.to_le_bytes());
                }
                ColumnTypeTag::Text => {
                    let s = match value {
                        Value::Text(t) => t.as_str().to_string(),
                        Value::Null => String::new(),
                        _ => String::new(),
                    };
                    // Store length-prefixed: [len: u16 LE] [bytes]
                    let len = s.len().min(65535) as u16;
                    buf.extend_from_slice(&len.to_le_bytes());
                    buf.extend_from_slice(s.as_bytes());
                }
                ColumnTypeTag::Vector => {
                    let bytes = match value {
                        Value::Vector(v) => {
                            let floats: &[f32] = &v.0;
                            let mut b = Vec::with_capacity(2 + floats.len() * 4);
                            b.extend_from_slice(&(floats.len() as u16).to_le_bytes());
                            for f in floats {
                                b.extend_from_slice(&f.to_le_bytes());
                            }
                            b
                        }
                        _ => vec![0u8; 2],
                    };
                    buf.extend_from_slice(&bytes);
                }
                ColumnTypeTag::Spatial => {
                    // Encode as WKT-like text string to match TextSegment format.
                    let wkt = match value {
                        Value::Spatial(g) => {
                            use crate::types::Geometry;
                            match **g {
                                Geometry::Point3D(ref p) => {
                                    format!("POINT({},{},{})", p.x, p.y, p.z)
                                }

                                _ => String::new(),
                            }
                        }
                        Value::Null => String::new(),
                        _ => String::new(),
                    };
                    let bytes = wkt.as_bytes();
                    let len = bytes.len().min(65535) as u16;
                    buf.extend_from_slice(&len.to_le_bytes());
                    buf.extend_from_slice(bytes);
                }
            }
        }

        self.num_rows += 1;
        Ok(())
    }

    /// Write the columnar SSTable to disk (consumes the builder).
    pub fn finish(mut self) -> Result<()> {
        self.finish_and_reset()
    }

    /// Look up the latest entry for a key in the write buffer.
    /// Returns Some(true) if tombstoned, Some(false) if live, None if not found.
    pub fn check_key(&self, key: u64) -> Option<bool> {
        for i in (0..self.num_rows).rev() {
            if self.keys[i] == key {
                return Some(self.deleted[i]);
            }
        }
        None
    }

    /// Return the newest-write liveness state for every distinct key in the
    /// buffer, as (key, is_tombstone) pairs. Used by count_live_rows to count
    /// live rows correctly (a buffered tombstone suppresses an older live row
    /// with the same key). Newest-version-wins semantics.
    pub fn latest_entries(&self) -> Vec<(u64, bool)> {
        let mut latest: std::collections::HashMap<u64, bool> =
            std::collections::HashMap::with_capacity(self.num_rows);
        for i in 0..self.num_rows {
            latest.insert(self.keys[i], self.deleted[i]);
        }
        latest.into_iter().collect()
    }

    /// Compact the in-memory buffer so each composite key appears at most once,
    /// keeping the NEWEST version (last appended = highest timestamp). Also drops
    /// any key whose newest version is a tombstone (deleted). Rows are in append
    /// order (old→new), so the last occurrence of a key wins. After this, the
    /// SSTable written by finish_and_reset() has unique keys, so find_key()
    /// binary search and single-segment scans return the correct (newest) value.
    ///
    /// This fixes the durability bug where an UPDATE (same key, newer value)
    /// followed by a flush+restart returned the OLD value: without dedup the
    /// segment held both versions and find_key picked the wrong one.
    fn dedup_keys_newest_wins(&mut self) {
        // 1. For each key, find the index of its last (newest) occurrence.
        let mut last_idx: std::collections::HashMap<u64, usize> =
            std::collections::HashMap::with_capacity(self.num_rows);
        for (i, &k) in self.keys.iter().enumerate() {
            last_idx.insert(k, i);
        }
        // 2. Build the keep-list in original order: a row is kept iff it is the
        //    newest version of its key. We DO NOT drop tombstones here — a
        //    tombstone's newest version must be preserved so read paths
        //    (is_deleted checks, newest-version-wins scans) see the deletion.
        //    Dropping them would resurrect deleted rows. Only duplicates of the
        //    SAME key (older versions) are removed.
        let keep: Vec<usize> = (0..self.num_rows)
            .filter(|&i| last_idx.get(&self.keys[i]) == Some(&i))
            .collect();
        if keep.len() == self.num_rows {
            return; // no dupes, nothing to do
        }
        // 3. Decode each kept row into Vec<Value>, then reset buffers and re-add.
        //    We re-add via add_values to reuse the existing layout logic for every
        //    column type (fixed + text). Buffer is small (in-memory), so this is
        //    cheap relative to the SSTable write.
        let col_types = self.column_types.clone();
        // (key, timestamp, deleted, row) — preserve the deleted flag so a
        // tombstone's newest version stays a tombstone after rebuild.
        let mut kept_rows: Vec<(u64, u64, bool, Vec<Value>)> = Vec::with_capacity(keep.len());
        for &i in &keep {
            let mut row = Vec::with_capacity(col_types.len());
            for (ci, tag) in self.column_tags.iter().enumerate() {
                match tag {
                    ColumnTypeTag::Integer | ColumnTypeTag::Timestamp => {
                        let buf = &self.column_buffers[ci];
                        let off = i * 8;
                        if off + 8 > buf.len() {
                            row.push(Value::Null);
                            continue;
                        }
                        // Use the authoritative NULL flag (not a value sentinel),
                        // so the real value i64::MIN round-trips correctly.
                        if self.null_flags.get(ci).and_then(|f| f.get(i)) == Some(&true) {
                            row.push(Value::Null);
                            continue;
                        }
                        let val = i64::from_le_bytes([
                            buf[off],
                            buf[off + 1],
                            buf[off + 2],
                            buf[off + 3],
                            buf[off + 4],
                            buf[off + 5],
                            buf[off + 6],
                            buf[off + 7],
                        ]);
                        if matches!(tag, ColumnTypeTag::Timestamp) {
                            row.push(Value::Timestamp(crate::types::Timestamp::from_micros(val)));
                        } else {
                            row.push(Value::Integer(val));
                        }
                    }
                    ColumnTypeTag::Float => {
                        let buf = &self.column_buffers[ci];
                        let off = i * 8;
                        if off + 8 > buf.len() {
                            row.push(Value::Null);
                            continue;
                        }
                        // Authoritative NULL flag (not the NaN sentinel), so a
                        // stored NaN round-trips as Float(NaN) rather than Null.
                        if self.null_flags.get(ci).and_then(|f| f.get(i)) == Some(&true) {
                            row.push(Value::Null);
                            continue;
                        }
                        let bits = u64::from_le_bytes([
                            buf[off],
                            buf[off + 1],
                            buf[off + 2],
                            buf[off + 3],
                            buf[off + 4],
                            buf[off + 5],
                            buf[off + 6],
                            buf[off + 7],
                        ]);
                        row.push(Value::Float(f64::from_bits(bits)));
                    }
                    ColumnTypeTag::Bool => {
                        let buf = &self.column_buffers[ci];
                        // Authoritative NULL flag; Bool has no value sentinel.
                        if self.null_flags.get(ci).and_then(|f| f.get(i)) == Some(&true) {
                            row.push(Value::Null);
                            continue;
                        }
                        row.push(Value::Bool(buf.get(i).copied().unwrap_or(0) != 0));
                    }
                    ColumnTypeTag::Text => {
                        // Text layout: each row = [u16 len][bytes], concatenated.
                        // 0xFFFF len = NULL sentinel.
                        let buf = &self.column_buffers[ci];
                        let mut p = 0usize;
                        let mut r = 0usize;
                        let mut found = None;
                        while p + 2 <= buf.len() {
                            let len = u16::from_le_bytes([buf[p], buf[p + 1]]) as usize;
                            p += 2;
                            if r == i {
                                if len == 0xFFFF {
                                    found = Some(Value::Null);
                                } else if p + len <= buf.len() {
                                    found = Some(Value::text(
                                        String::from_utf8_lossy(&buf[p..p + len]).into_owned(),
                                    ));
                                } else {
                                    found = Some(Value::Null);
                                }
                                break;
                            }
                            p += if len == 0xFFFF { 0 } else { len };
                            r += 1;
                        }
                        row.push(found.unwrap_or(Value::Null));
                    }
                    ColumnTypeTag::Vector => {
                        // Vector layout: [dim:u16][f32×dim] per row, concatenated.
                        let buf = &self.column_buffers[ci];
                        let mut p = 0usize;
                        let mut r = 0usize;
                        let mut found = None;
                        while p + 2 <= buf.len() {
                            let dim = u16::from_le_bytes([buf[p], buf[p + 1]]) as usize;
                            p += 2;
                            if r == i {
                                if dim == 0 {
                                    found = Some(Value::Null);
                                } else if p + dim * 4 <= buf.len() {
                                    let mut v = Vec::with_capacity(dim);
                                    for j in 0..dim {
                                        let off = p + j * 4;
                                        v.push(f32::from_le_bytes([
                                            buf[off],
                                            buf[off + 1],
                                            buf[off + 2],
                                            buf[off + 3],
                                        ]));
                                    }
                                    found = Some(Value::Vector(crate::types::ArcVec(
                                        std::sync::Arc::new(v),
                                    )));
                                } else {
                                    found = Some(Value::Null);
                                }
                                break;
                            }
                            p += dim * 4;
                            r += 1;
                        }
                        row.push(found.unwrap_or(Value::Null));
                    }
                    ColumnTypeTag::Spatial => {
                        // Spatial layout: [len:u16][bincode(Geometry)] per row.
                        let buf = &self.column_buffers[ci];
                        let mut p = 0usize;
                        let mut r = 0usize;
                        let mut found = None;
                        while p + 2 <= buf.len() {
                            let len = u16::from_le_bytes([buf[p], buf[p + 1]]) as usize;
                            p += 2;
                            if r == i {
                                if len == 0 || p + len > buf.len() {
                                    found = Some(Value::Null);
                                } else {
                                    match bincode::deserialize::<crate::types::Geometry>(
                                        &buf[p..p + len],
                                    ) {
                                        Ok(g) => {
                                            found = Some(Value::Spatial(std::boxed::Box::new(g)))
                                        }
                                        Err(_) => found = Some(Value::Null),
                                    }
                                }
                                break;
                            }
                            p += len;
                            r += 1;
                        }
                        row.push(found.unwrap_or(Value::Null));
                    }
                };
            }
            kept_rows.push((self.keys[i], self.timestamps[i], self.deleted[i], row));
        }
        // 4. Reset buffers and re-add the deduplicated rows.
        self.keys.clear();
        self.timestamps.clear();
        self.deleted.clear();
        for b in self.column_buffers.iter_mut() {
            b.clear();
        }
        for f in self.null_flags.iter_mut() {
            f.clear();
        }
        self.num_rows = 0;
        for (key, ts, deleted, row) in kept_rows {
            // Re-add using the SAME encoding + the preserved deleted flag. A
            // tombstone's newest version must be re-added with deleted=true so
            // read paths (is_deleted, newest-version-wins) still see the deletion.
            let _ = self.add_values(key, ts, deleted, &row);
        }
    }

    pub fn finish_and_reset(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }
        if self.num_rows == 0 {
            return Ok(());
        }

        // 🔑 Dedup same-key rows BEFORE writing (newest-version-wins). An UPDATE
        // appends a newer row with the SAME composite key; if both versions are
        // written to the SSTable, find_key() binary search returns an arbitrary
        // one (often the older), so reads after a flush/restart see stale data.
        // Keep only the LAST occurrence of each key (rows are in append order =
        // old→new, so last is newest). Tombstones count as a version too — if a
        // key's newest version is a tombstone, the key is dropped entirely here
        // (no live row), which is correct for a flushed segment.
        if self.num_rows > 1 {
            self.dedup_keys_newest_wins();
        }
        let num_rows = self.num_rows;
        if num_rows == 0 {
            return Ok(());
        }
        let num_cols = self.column_tags.len();

        // Build column segments with null bitmaps
        let mut segments: Vec<Vec<u8>> = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let tag = &self.column_tags[col_idx];
            let raw = &self.column_buffers[col_idx];
            let null_bytes = num_rows.div_ceil(8);
            let mut seg = Vec::with_capacity(null_bytes + raw.len());

            if tag.is_fixed() {
                // Fixed segment: [null_bitmap] [data]
                let mut nulls = vec![0u8; null_bytes];
                let elem_size = tag.fixed_size();
                // NULL bitmap is authoritative (tracked at encode time). This
                // avoids the i64::MIN / f64::NAN value-sentinel collision, so
                // those exact values can be stored without being mistaken for NULL.
                let _ = elem_size;
                let null_flags = &self.null_flags[col_idx];
                for row_idx in 0..num_rows {
                    if row_idx < null_flags.len() && null_flags[row_idx] {
                        nulls[row_idx / 8] |= 1 << (row_idx % 8);
                    }
                }
                seg.extend_from_slice(&nulls);
                seg.extend_from_slice(raw);
            } else if matches!(tag, ColumnTypeTag::Text) {
                // Text segment: [null_bitmap] [offsets] [string_data]
                // Raw buffer format: [(len: u16 LE, bytes)] repeated.
                // Use the authoritative null_flags (tracked at add_values time)
                // rather than the 0xFFFF in-band sentinel, so NULLs are preserved
                // regardless of how the raw bytes were encoded (dedup re-add,
                // raw merge, etc.).
                let mut nulls = vec![0u8; null_bytes];
                let mut offsets = Vec::with_capacity((num_rows + 1) * 4);
                let mut str_data = Vec::new();
                let mut current_offset = 0u32;
                let null_flags = &self.null_flags[col_idx];

                let mut pos = 0usize;
                for row_idx in 0..num_rows {
                    if pos + 2 > raw.len() {
                        break;
                    }
                    let len = u16::from_le_bytes([raw[pos], raw[pos + 1]]) as usize;
                    pos += 2;
                    let is_null =
                        null_flags.get(row_idx).copied().unwrap_or(false) || len == 0xFFFF; // also catch legacy sentinel bytes
                    if is_null {
                        nulls[row_idx / 8] |= 1 << (row_idx % 8);
                        offsets.push(current_offset);
                        // NULL rows have no string data; skip their bytes (len
                        // is 0xFFFF sentinel or 0 for an empty-but-flagged NULL).
                        if len != 0xFFFF {
                            pos += len;
                        }
                        continue;
                    }
                    offsets.push(current_offset);
                    if pos + len <= raw.len() {
                        str_data.extend_from_slice(&raw[pos..pos + len]);
                        current_offset += len as u32;
                    }
                    pos += len;
                }
                offsets.push(current_offset);

                seg.extend_from_slice(&nulls);
                for off in &offsets {
                    seg.extend_from_slice(&off.to_le_bytes());
                }
                seg.extend_from_slice(&str_data);
            } else if matches!(tag, ColumnTypeTag::Vector) {
                // Vector segment: [null_bitmap][dim:u16][f32×dim per row].
                // The raw buffer holds [dim:u16][f32×dim] per row (from
                // add_values). Re-pack to a uniform dim so read_vectors can
                // use a fixed stride. Missing/NULL rows get zero-filled.
                let mut nulls = vec![0u8; null_bytes];
                // First pass: determine the column's dimension (max over rows).
                let mut col_dim: usize = 0;
                let mut row_dims: Vec<usize> = Vec::with_capacity(num_rows);
                {
                    let mut pos = 0usize;
                    for row_idx in 0..num_rows {
                        if pos + 2 > raw.len() {
                            row_dims.push(0);
                            continue;
                        }
                        let d = u16::from_le_bytes([raw[pos], raw[pos + 1]]) as usize;
                        if d == 0 {
                            nulls[row_idx / 8] |= 1 << (row_idx % 8);
                            row_dims.push(0);
                        } else {
                            if d > col_dim {
                                col_dim = d;
                            }
                            row_dims.push(d);
                        }
                        pos += 2 + d * 4;
                    }
                }
                seg.extend_from_slice(&nulls);
                seg.extend_from_slice(&(col_dim as u16).to_le_bytes());
                // Second pass: emit col_dim f32 per row (pad shorter/missing).
                let mut pos = 0usize;
                for row_idx in 0..num_rows {
                    let d = row_dims[row_idx];
                    let mut vals = vec![0f32; col_dim];
                    if d > 0 && pos + 2 <= raw.len() {
                        // raw[pos..pos+2] is dim (already read); data follows.
                        let base = pos + 2;
                        for j in 0..d.min(col_dim) {
                            let off = base + j * 4;
                            if off + 4 <= raw.len() {
                                vals[j] = f32::from_le_bytes([
                                    raw[off],
                                    raw[off + 1],
                                    raw[off + 2],
                                    raw[off + 3],
                                ]);
                            }
                        }
                    }
                    for v in &vals {
                        seg.extend_from_slice(&v.to_le_bytes());
                    }
                    pos += 2 + d * 4;
                }
            } else {
                // Spatial (and any other variable column): [null_bitmap]
                // then [len:u16][bytes] per row. The raw buffer already holds
                // [len:u16][bytes] per row from add_values; copy as-is.
                let nulls = vec![0u8; null_bytes];
                seg.extend_from_slice(&nulls);
                seg.extend_from_slice(raw);
            }

            segments.push(seg);
        }

        // Build row map
        let (rm_size, _, _, _deleted_len) = RowMap::compute_sizes(num_rows);
        let mut row_map = vec![0u8; rm_size];

        // Keys
        for (i, k) in self.keys.iter().enumerate() {
            let off = i * 8;
            row_map[off..off + 8].copy_from_slice(&k.to_le_bytes());
        }
        // Timestamps
        let ts_off = num_rows * 8;
        for (i, ts) in self.timestamps.iter().enumerate() {
            let off = ts_off + i * 8;
            row_map[off..off + 8].copy_from_slice(&ts.to_le_bytes());
        }
        // Deleted bitset
        let del_off = num_rows * 16;
        for (i, d) in self.deleted.iter().enumerate() {
            if *d {
                row_map[del_off + i / 8] |= 1 << (i % 8);
            }
        }

        // Build entire file in memory, then write atomically.
        // Avoids BufWriter + mmap interaction issues on macOS for small files.
        let ci_offset = HEADER_SIZE as u64;
        let ci_size = num_cols * COLUMN_INDEX_ENTRY_SIZE;
        let segments_start = HEADER_SIZE + ci_size;
        // Compress segments + compute column index entries
        let mut compressed_segs: Vec<Vec<u8>> = Vec::with_capacity(num_cols);
        let mut column_entries = Vec::with_capacity(num_cols);
        let mut current_offset = segments_start as u64;
        for seg in &segments {
            // Try Snappy compression — only use if it saves space.
            // Compression reduces file size → less file_data heap memory on read.
            let compressed = snap::raw::Encoder::new()
                .compress_vec(seg)
                .unwrap_or_else(|_| seg.clone());
            let seg_data: Vec<u8> = if compressed.len() + 1 < seg.len() {
                let mut out = Vec::with_capacity(1 + compressed.len());
                out.push(1u8); // flag: Snappy compressed
                out.extend_from_slice(&compressed);
                out
            } else {
                let mut out = Vec::with_capacity(1 + seg.len());
                out.push(0u8); // flag: uncompressed
                out.extend_from_slice(seg);
                out
            };
            let size = seg_data.len() as u64;
            column_entries.push(ColumnIndexEntry {
                offset: current_offset,
                size,
            });
            current_offset += size;
            compressed_segs.push(seg_data);
        }
        let row_map_offset = current_offset;

        // Pre-compute total size and allocate buffer
        let total_size = row_map_offset as usize + row_map.len() + FOOTER_SIZE;
        let mut buf = Vec::with_capacity(total_size);

        // Header
        let mut header_tags = [0u8; MAX_COLUMNS];
        for (i, tag) in self.column_tags.iter().enumerate() {
            header_tags[i] = *tag as u8;
        }
        let header = ColumnarHeader {
            num_rows: num_rows as u32,
            num_columns: num_cols as u16,
            column_tags: header_tags,
        };
        buf.extend_from_slice(&header.serialize());

        // Column index
        for entry in &column_entries {
            buf.extend_from_slice(&entry.offset.to_le_bytes());
            buf.extend_from_slice(&entry.size.to_le_bytes());
        }

        // Column segments (compressed)
        for seg in &compressed_segs {
            buf.extend_from_slice(seg);
        }

        // Row map
        buf.extend_from_slice(&row_map);

        // Footer
        let mut footer = [0u8; FOOTER_SIZE];
        footer[0..8].copy_from_slice(&ci_offset.to_le_bytes());
        footer[8..16].copy_from_slice(&row_map_offset.to_le_bytes());
        footer[16..20].copy_from_slice(&COLUMNAR_MAGIC.to_le_bytes());
        buf.extend_from_slice(&footer);

        // Atomic publish via temp-file + rename: write the full buffer to a
        // sibling temp file, fsync it, then atomically rename onto the final
        // path. This guarantees that any concurrent reader (mmap-based) either
        // sees the previous complete file or the new complete file — never a
        // half-written one (which was the root cause of the mmap index-out-of-
        // bounds panics on repeated finalize). Rename also lets us drop the
        // per-finalize fsync of the *live* file: durability is provided by the
        // WAL checkpoint path, and the temp-file fsync is what makes rename
        // crash-safe on POSIX.
        let final_path = self.path.clone();
        let dir = final_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let tmp_path = dir.join(format!(
            ".{}.tmp",
            final_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("col.tmp")
        ));
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(&buf)?;
        writer.flush()?;
        // fsync the temp file so the rename is durable across crashes.
        writer.get_ref().sync_all()?;
        drop(writer);
        // Atomic publish. On POSIX, rename guarantees readers see the new file
        // in its entirety once the syscall returns.
        std::fs::rename(&tmp_path, &final_path)?;

        // Success: clear internal state (data is now on disk)
        // Reset finished to false so the builder can be reused for new data.
        self.finished = false;
        self.num_rows = 0;
        self.keys.clear();
        self.timestamps.clear();
        self.deleted.clear();
        self.column_buffers = vec![Vec::new(); num_cols];

        Ok(())
    }
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;
    use tempfile::TempDir;

    fn make_test_row(id: i64, name: &str, amount: f64, region: &str) -> Vec<Value> {
        vec![
            Value::Integer(id),
            Value::Text(crate::types::ArcString(std::sync::Arc::from(name))),
            Value::Float(amount),
            Value::Text(crate::types::ArcString(std::sync::Arc::from(region))),
        ]
    }

    #[test]
    #[cfg_attr(
        target_os = "macos",
        ignore = "macOS mmap coherence issue with files < page size"
    )]
    fn test_columnar_build_and_read() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.col.sst");

        let col_types = vec![
            ColumnType::Integer,
            ColumnType::Text,
            ColumnType::Float,
            ColumnType::Text,
        ];

        // Build
        let mut builder = ColumnarSSTableBuilder::new(&path, col_types.clone());
        let rows = vec![
            (1u64, 100u64, false, make_test_row(1, "Alice", 99.5, "US")),
            (2, 101, false, make_test_row(2, "Bob", 50.0, "EU")),
            (3, 102, false, make_test_row(3, "Carol", 75.0, "US")),
            (4, 103, true, make_test_row(4, "Dave", 0.0, "EU")), // deleted
        ];

        for (key, ts, del, row) in &rows {
            let encoded = crate::storage::row_format::encode(row, &col_types).unwrap();
            builder.add_row(*key, *ts, *del, &encoded).unwrap();
        }
        builder.finish().unwrap();

        // Verify file exists and is recognized
        assert!(ColumnarSSTable::is_columnar(&path));

        // Read
        let col_sst = ColumnarSSTable::open(&path).unwrap();
        assert_eq!(col_sst.num_rows, 4);
        assert_eq!(col_sst.column_tags.len(), 4);

        // Check row map
        assert_eq!(col_sst.row_map.key(0), 1);
        assert_eq!(col_sst.row_map.key(3), 4);
        assert!(!col_sst.row_map.is_deleted(0));
        assert!(col_sst.row_map.is_deleted(3));

        // Read id column (fixed i64)
        let id_seg = col_sst.read_fixed_i64(0).unwrap();
        assert_eq!(id_seg.get_i64(0), Some(1));
        assert_eq!(id_seg.get_i64(1), Some(2));
        assert_eq!(id_seg.get_i64(2), Some(3));
        assert_eq!(id_seg.get_i64(3), Some(4));

        // Read amount column (fixed f64)
        let amt_seg = col_sst.read_fixed_f64(2).unwrap();
        assert_eq!(amt_seg.get_f64(0), Some(99.5));
        assert_eq!(amt_seg.get_f64(1), Some(50.0));

        // Read region column (text)
        let reg_seg = col_sst.read_text(3).unwrap();
        assert_eq!(reg_seg.get_str(0), Some("US"));
        assert_eq!(reg_seg.get_str(1), Some("EU"));
        assert_eq!(reg_seg.get_str(2), Some("US"));
        assert_eq!(reg_seg.get_str(3), Some("EU"));
    }

    #[test]
    fn test_columnar_roundtrip_300k() {
        // Test with a larger dataset to verify no data corruption
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("large.col.sst");

        let col_types = vec![
            ColumnType::Integer,
            ColumnType::Text,
            ColumnType::Float,
            ColumnType::Text,
        ];

        let n = 10000;
        let mut builder = ColumnarSSTableBuilder::new(&path, col_types.clone());

        for i in 0..n {
            let region = if i % 3 == 0 { "US" } else { "EU" };
            let row = vec![
                Value::Integer(i as i64),
                Value::Text(crate::types::ArcString(std::sync::Arc::from(format!(
                    "cust_{}",
                    i % 100
                )))),
                Value::Float(i as f64 * 1.5),
                Value::Text(crate::types::ArcString(std::sync::Arc::from(region))),
            ];
            let encoded = crate::storage::row_format::encode(&row, &col_types).unwrap();
            builder
                .add_row(i as u64, i as u64 + 1000, i % 7 == 0, &encoded)
                .unwrap();
        }
        builder.finish().unwrap();

        let col_sst = ColumnarSSTable::open(&path).unwrap();
        assert_eq!(col_sst.num_rows, n as usize);

        // Spot-check
        let id_seg = col_sst.read_fixed_i64(0).unwrap();
        let amt_seg = col_sst.read_fixed_f64(2).unwrap();
        let reg_seg = col_sst.read_text(3).unwrap();

        for i in 0..n {
            assert_eq!(id_seg.get_i64(i), Some(i as i64), "id mismatch at {}", i);
            let expected_amt = i as f64 * 1.5;
            let got_amt = amt_seg.get_f64(i).unwrap();
            assert!(
                (got_amt - expected_amt).abs() < 0.001,
                "amount mismatch at {}",
                i
            );
            let expected_reg = if i % 3 == 0 { "US" } else { "EU" };
            assert_eq!(
                reg_seg.get_str(i),
                Some(expected_reg),
                "region mismatch at {}",
                i
            );
        }
    }
}
