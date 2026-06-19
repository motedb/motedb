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

use crate::{Result, StorageError};
use crate::types::{ColumnType, Value, RowId};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use memmap2::{Mmap, MmapOptions};

// ── Constants ─────────────────────────────────────────────────────

const COLUMNAR_MAGIC: u32 = 0x434D5442; // "BTMC"
const COLUMNAR_VERSION: u32 = 1;
const HEADER_SIZE: usize = 32;
const FOOTER_SIZE: usize = 20;
const MAX_COLUMNS: usize = 16;

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
        matches!(self, Self::Integer | Self::Float | Self::Bool | Self::Timestamp)
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
        buf[14..30].copy_from_slice(&self.column_tags);
        // bytes 30-31: reserved
        buf
    }

    fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < HEADER_SIZE {
            return Err(StorageError::InvalidData("Columnar header too short".into()));
        }
        let magic = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        if magic != COLUMNAR_MAGIC {
            return Err(StorageError::InvalidData(
                format!("Bad columnar magic: 0x{:08X}", magic)
            ));
        }
        let version = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
        if version != COLUMNAR_VERSION {
            return Err(StorageError::InvalidData(
                format!("Unsupported columnar version: {}", version)
            ));
        }
        let num_rows = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);
        let num_columns = u16::from_le_bytes([data[12], data[13]]);
        let mut column_tags = [0u8; MAX_COLUMNS];
        column_tags.copy_from_slice(&data[14..30]);
        Ok(Self { num_rows, num_columns, column_tags })
    }
}

// ── Column Index Entry ─────────────────────────────────────────────

#[derive(Clone, Debug)]
pub(crate) struct ColumnIndexEntry {
    offset: u64,
    size: u64,
}

const COLUMN_INDEX_ENTRY_SIZE: usize = 16; // (offset: u64, size: u64)

// ── Row Map ────────────────────────────────────────────────────────

/// Row Map: MVCC metadata for each row in columnar order.
///
/// Stored as three contiguous arrays:
/// - keys: u64 × num_rows (composite keys, preserves row order)
/// - timestamps: u64 × num_rows (MVCC version)
/// - deleted: bitset (u8 × ceil(num_rows/8))
#[derive(Clone, Debug)]
pub struct RowMap {
    pub num_rows: usize,
    data: SegData, // Owned for builder, Mmap for zero-copy reads
    keys_offset: usize,
    timestamps_offset: usize,
    deleted_offset: usize,
    #[allow(dead_code)]
    deleted_len: usize,
}

impl RowMap {
    fn compute_sizes(num_rows: usize) -> (usize, usize, usize, usize) {
        let keys_size = num_rows * 8;
        let timestamps_size = num_rows * 8;
        let deleted_len = (num_rows + 7) / 8;
        (keys_size + timestamps_size + deleted_len, keys_size, timestamps_size, deleted_len)
    }

    #[allow(dead_code)]
    pub(crate) fn from_bytes(data: Vec<u8>, num_rows: usize) -> Self {
        let (_, keys_size, timestamps_size, deleted_len) = Self::compute_sizes(num_rows);
        Self { num_rows, keys_offset: 0, timestamps_offset: keys_size,
            deleted_offset: keys_size + timestamps_size, deleted_len, data: SegData::Owned(data) }
    }

    /// Zero-copy view into mmap data.
    pub(crate) fn from_mmap(mmap: Arc<Mmap>, offset: usize, num_rows: usize) -> Result<Self> {
        let (_total, keys_size, timestamps_size, deleted_len) = Self::compute_sizes(num_rows);
        Ok(Self { num_rows, keys_offset: offset, timestamps_offset: offset + keys_size,
            deleted_offset: offset + keys_size + timestamps_size, deleted_len,
            data: SegData::Mmap { mmap, offset } })
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
            if k < target { lo = mid + 1; }
            else if k > target { hi = mid; }
            else { return Some(mid); }
        }
        None
    }

    #[inline]
    pub fn is_deleted(&self, row_idx: usize) -> bool {
        let byte = self.data.get(self.deleted_offset + row_idx / 8);
        (byte >> (row_idx % 8)) & 1 != 0
    }
}

// ── Column Segment Views ───────────────────────────────────────────

/// Data source for a column segment: owned bytes (legacy) or mmap reference (zero-copy).
#[derive(Clone, Debug)]
enum SegData {
    #[allow(dead_code)]
    Owned(Vec<u8>),
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
            SegData::Owned(v) => &v[start..start+len],
            SegData::Mmap { mmap, offset } => &mmap[*offset + start..*offset + start + len],
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
        let null_bytes = (num_rows + 7) / 8;
        let elem_size = tag.fixed_size();
        let data_size = num_rows * elem_size;
        let expected = null_bytes + data_size;
        if data.len() < expected {
            return Err(StorageError::InvalidData(
                format!("Fixed segment too short: {} < {}", data.len(), expected)
            ));
        }
        Ok(Self {
            num_rows,
            null_bitmap: SegData::Owned(data[..null_bytes].to_vec()),
            data: SegData::Owned(data[null_bytes..null_bytes + data_size].to_vec()),
            elem_size, tag,
        })
    }

    pub(crate) fn from_mmap(mmap: Arc<Mmap>, offset: usize, num_rows: usize, tag: ColumnTypeTag) -> Self {
        let null_bytes = (num_rows + 7) / 8;
        Self {
            num_rows,
            null_bitmap: SegData::Mmap { mmap: mmap.clone(), offset },
            data: SegData::Mmap { mmap, offset: offset + null_bytes },
            elem_size: tag.fixed_size(), tag,
        }
    }

    #[inline]
    pub fn is_null(&self, row_idx: usize) -> bool {
        (self.null_bitmap.get(row_idx / 8) >> (row_idx % 8)) & 1 != 0
    }

    #[inline]
    pub fn get_i64(&self, row_idx: usize) -> Option<i64> {
        if self.is_null(row_idx) { return None; }
        let off = row_idx * 8;
        let s = self.data.slice(off, 8);
        Some(i64::from_le_bytes([s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]]))
    }

    #[inline]
    pub fn get_f64(&self, row_idx: usize) -> Option<f64> {
        if self.is_null(row_idx) { return None; }
        let off = row_idx * 8;
        let s = self.data.slice(off, 8);
        Some(f64::from_le_bytes([s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]]))
    }

    #[inline]
    pub fn get_bool(&self, row_idx: usize) -> Option<bool> {
        if self.is_null(row_idx) { return None; }
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
        let null_bytes = (num_rows + 7) / 8;
        let offsets_size = (num_rows + 1) * 4;
        if data.len() < null_bytes + offsets_size {
            return Err(StorageError::InvalidData("Text segment too short".into()));
        }
        Ok(Self {
            num_rows,
            null_bitmap: SegData::Owned(data[..null_bytes].to_vec()),
            offsets_data: SegData::Owned(data[null_bytes..null_bytes + offsets_size].to_vec()),
            string_data: SegData::Owned(data[null_bytes + offsets_size..].to_vec()),
            trust_utf8: false, offsets_start: 0,
        })
    }

    pub(crate) fn from_mmap(mmap: Arc<Mmap>, offset: usize, num_rows: usize) -> Self {
        let null_bytes = (num_rows + 7) / 8;
        let offsets_size = (num_rows + 1) * 4;
        Self {
            num_rows,
            null_bitmap: SegData::Mmap { mmap: mmap.clone(), offset },
            offsets_data: SegData::Mmap { mmap: mmap.clone(), offset: offset + null_bytes },
            string_data: SegData::Mmap { mmap, offset: offset + null_bytes + offsets_size },
            trust_utf8: true, // Our builder only writes valid UTF-8
            offsets_start: 0,
        }
    }

    #[inline]
    pub fn is_null(&self, row_idx: usize) -> bool {
        (self.null_bitmap.get(row_idx / 8) >> (row_idx % 8)) & 1 != 0
    }

    #[inline]
    fn get_offset(&self, idx: usize) -> u32 {
        let s = self.offsets_data.slice(idx * 4, 4);
        u32::from_le_bytes([s[0], s[1], s[2], s[3]])
    }

    #[inline]
    pub fn get_str(&self, row_idx: usize) -> Option<&str> {
        if self.is_null(row_idx) { return None; }
        let start = self.get_offset(row_idx) as usize;
        let end = self.get_offset(row_idx + 1) as usize;
        if start > end { return None; }
        let bytes = self.string_data.slice(start, end - start);
        if self.trust_utf8 {
            unsafe { Some(std::str::from_utf8_unchecked(bytes)) }
        } else {
            std::str::from_utf8(bytes).ok()
        }
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
    #[allow(dead_code)]
    file: Option<File>,
    #[allow(dead_code)]
    header: ColumnarHeader,
    column_index: Vec<ColumnIndexEntry>,
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
                libc::madvise(
                    m.as_ptr() as *mut _,
                    m.len(),
                    libc::MADV_DONTNEED,
                );
            }
        }
    }

    fn backing(&self) -> &[u8] {
        if let Some(ref m) = self.mmap { &m[..] } else { &self.file_data }
    }

    /// Check if a file is a columnar SSTable by reading its magic.
    pub fn is_columnar<P: AsRef<Path>>(path: P) -> bool {
        let path = path.as_ref();
        if let Ok(mut file) = OpenOptions::new().read(true).open(path) {
            if let Ok(metadata) = file.metadata() {
                let file_len = metadata.len();
                if file_len >= FOOTER_SIZE as u64 {
                    if file.seek(SeekFrom::End(-(FOOTER_SIZE as i64))).is_ok() {
                        let mut footer = [0u8; FOOTER_SIZE];
                        if file.read_exact(&mut footer).is_ok() {
                            let magic = u32::from_le_bytes([
                                footer[16], footer[17], footer[18], footer[19],
                            ]);
                            return magic == COLUMNAR_MAGIC;
                        }
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
            return Err(StorageError::InvalidData("File too small for columnar footer".into()));
        }
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let mut footer_buf = [0u8; FOOTER_SIZE];
        file.read_exact(&mut footer_buf)?;

        let magic = u32::from_le_bytes([
            footer_buf[16], footer_buf[17], footer_buf[18], footer_buf[19],
        ]);
        if magic != COLUMNAR_MAGIC {
            return Err(StorageError::InvalidData("Not a columnar SSTable".into()));
        }
        let _column_index_offset = u64::from_le_bytes([
            footer_buf[0], footer_buf[1], footer_buf[2], footer_buf[3],
            footer_buf[4], footer_buf[5], footer_buf[6], footer_buf[7],
        ]);
        let row_map_offset = u64::from_le_bytes([
            footer_buf[8], footer_buf[9], footer_buf[10], footer_buf[11],
            footer_buf[12], footer_buf[13], footer_buf[14], footer_buf[15],
        ]);

        // mmap for zero-copy (saves RSS — critical for <100MB embedded budget).
        // Falls back to heap read if mmap fails or file too small for row_map.
        let row_map_min_end = row_map_offset as usize + 64;
        let mmap: Option<Arc<Mmap>> = if (file_len as usize) >= row_map_min_end {
            unsafe { MmapOptions::new().map(&file) }.ok()
                .filter(|m| m.len() >= row_map_min_end)
                .map(|m| {
                    let _ = unsafe { libc::madvise(m.as_ptr() as *mut _, m.len(), libc::MADV_RANDOM) };
                    Arc::new(m)
                })
        } else { None };

        let mut file_data: Vec<u8> = Vec::new();
        if mmap.is_none() {
            file.seek(SeekFrom::Start(0))?;
            file_data = vec![0u8; file_len as usize];
            file.read_exact(&mut file_data)?;
        }
        let data: &[u8] = if let Some(ref m) = mmap { &m[..] } else { &file_data };

        // Read header
        let header_bytes = &data[..HEADER_SIZE];
        let header = ColumnarHeader::deserialize(header_bytes)?;
        let num_columns = header.num_columns as usize;
        let num_rows = header.num_rows as usize;

        // Read column index
        let ci_size = num_columns * COLUMN_INDEX_ENTRY_SIZE;
        let ci_start = HEADER_SIZE;
        let ci_data = &data[ci_start..ci_start + ci_size];
        let column_index: Vec<ColumnIndexEntry> = (0..num_columns)
            .map(|i| {
                let off = i * COLUMN_INDEX_ENTRY_SIZE;
                ColumnIndexEntry {
                    offset: u64::from_le_bytes([
                        ci_data[off], ci_data[off+1], ci_data[off+2], ci_data[off+3],
                        ci_data[off+4], ci_data[off+5], ci_data[off+6], ci_data[off+7],
                    ]),
                    size: u64::from_le_bytes([
                        ci_data[off+8], ci_data[off+9], ci_data[off+10], ci_data[off+11],
                        ci_data[off+12], ci_data[off+13], ci_data[off+14], ci_data[off+15],
                    ]),
                }
            })
            .collect();

        // Row map: copy from file_data
        let (rm_total, keys_size, timestamps_size, deleted_len) = RowMap::compute_sizes(num_rows);
        let rm_data = data[row_map_offset as usize..row_map_offset as usize + rm_total].to_vec();
        let row_map = RowMap {
            num_rows,
            keys_offset: 0,
            timestamps_offset: keys_size,
            deleted_offset: keys_size + timestamps_size,
            deleted_len,
            data: SegData::Owned(rm_data),
        };

        let column_tags: Vec<ColumnTypeTag> = header.column_tags[..num_columns]
            .iter()
            .map(|&t| unsafe { std::mem::transmute(t) })
            .collect();

        Ok(Self {
            path,
            file_data,
            mmap: mmap,
            file: None,
            header,
            column_index,
            row_map,
            column_tags,
            num_rows,
        })
    }

    /// Read a fixed column as an i64 array (zero-copy from mmap).
    /// Decompress segment data if needed. Format: [flag: u8] [data].
    fn decompress_segment(data: &[u8]) -> std::borrow::Cow<[u8]> {
        if data.is_empty() { return std::borrow::Cow::Borrowed(data); }
        match data[0] {
            1 => { // Snappy compressed
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
            return Err(StorageError::InvalidData("Column is not fixed-width".into()));
        }
        let entry = &self.column_index[col_idx];
        let start = entry.offset as usize;
        let end = start + entry.size as usize;
        let decompressed = Self::decompress_segment(&self.backing()[start..end]);
        FixedSegment::from_bytes(&decompressed, self.num_rows, tag)
    }

    pub fn read_fixed_f64(&self, col_idx: usize) -> Result<FixedSegment> {
        self.read_fixed_i64(col_idx)
    }

    pub fn read_text(&self, col_idx: usize) -> Result<TextSegment> {
        let entry = &self.column_index[col_idx];
        let start = entry.offset as usize;
        let end = start + entry.size as usize;
        let decompressed = Self::decompress_segment(&self.backing()[start..end]);
        TextSegment::from_bytes(&decompressed, self.num_rows)
    }

    /// Read spatial geometries from column segment.
    /// Format: [null_bitmap][len: u16 LE][bincode(Geometry)] per row (variable-length)
    pub fn read_spatial(&self, col_idx: usize) -> Result<Vec<(RowId, crate::types::Geometry)>> {
        let entry = &self.column_index[col_idx];
        let seg_start = entry.offset as usize;
        let seg_end = seg_start + entry.size as usize;
        let null_bytes = (self.num_rows + 7) / 8;
        if seg_start + null_bytes > seg_end { return Ok(Vec::new()); }
        let mut result = Vec::new();
        let mut pos = seg_start + null_bytes;
        for i in 0..self.num_rows {
            if (self.backing()[seg_start + i/8] >> (i%8)) & 1 != 0 {
                // Null — skip to next row (need to read len to skip bytes)
                if pos + 2 <= seg_end {
                    let len = u16::from_le_bytes([self.backing()[pos], self.backing()[pos+1]]) as usize;
                    pos += 2 + len;
                }
                continue;
            }
            if self.row_map.is_deleted(i) { continue; }
            if pos + 2 > seg_end { break; }
            let len = u16::from_le_bytes([self.backing()[pos], self.backing()[pos+1]]) as usize;
            pos += 2;
            if len == 0 || pos + len > seg_end { continue; }
            if let Ok(geom) = bincode::deserialize::<crate::types::Geometry>(&self.backing()[pos..pos+len]) {
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
        let idx = match self.row_map.find_key(key) {
            Some(i) => i,
            None => return None,
        };
        if self.row_map.is_deleted(idx) { return None; }
        let mut row = Vec::with_capacity(col_types.len());
        for ci in 0..col_types.len() {
            if self.column_tags[ci].is_fixed() {
                if let Ok(seg) = self.read_fixed_i64(ci) {
                    match &col_types[ci] {
                        crate::types::ColumnType::Integer => row.push(seg.get_i64(idx).map(crate::types::Value::Integer).unwrap_or(crate::types::Value::Null)),
                        crate::types::ColumnType::Float => row.push(seg.get_f64(idx).map(crate::types::Value::Float).unwrap_or(crate::types::Value::Null)),
                        crate::types::ColumnType::Boolean => row.push(seg.get_bool(idx).map(crate::types::Value::Bool).unwrap_or(crate::types::Value::Null)),
                        crate::types::ColumnType::Timestamp => row.push(seg.get_i64(idx).map(|v| crate::types::Value::Timestamp(crate::types::Timestamp::from_micros(v))).unwrap_or(crate::types::Value::Null)),
                        _ => row.push(crate::types::Value::Null),
                    }
                } else { row.push(crate::types::Value::Null); }
            } else if let Ok(seg) = self.read_text(ci) {
                row.push(seg.get_str(idx).map(|s| crate::types::Value::Text(crate::types::ArcString(std::sync::Arc::from(s)))).unwrap_or(crate::types::Value::Null));
            } else { row.push(crate::types::Value::Null); }
        }
        Some(row)
    }

    /// Read vector data from column segment.
    /// Format: [flag: u8] [null_bitmap] [dim: u16 LE] [f32×dim per row]
    pub fn read_vectors(&self, col_idx: usize) -> Result<Vec<(RowId, Vec<f32>)>> {
        let entry = &self.column_index[col_idx];
        let raw = Self::decompress_segment(
            &self.backing()[entry.offset as usize..(entry.offset + entry.size) as usize]
        );
        let data = raw.as_ref();
        let null_bytes = (self.num_rows + 7) / 8;
        if null_bytes + 2 > data.len() { return Ok(Vec::new()); }
        let dim = u16::from_le_bytes([data[null_bytes], data[null_bytes+1]]) as usize;
        if dim == 0 || dim > 65536 { return Ok(Vec::new()); }
        let stride = dim * 4;
        let data_start = null_bytes + 2;
        let n = ((data.len() - data_start) / stride).min(self.num_rows);
        let mut result = Vec::with_capacity(n);
        for i in 0..n {
            if (data[i/8] >> (i%8)) & 1 != 0 { continue; } // null check
            if self.row_map.is_deleted(i) { continue; }
            let row_id = (self.row_map.key(i) & 0xFFFFFFFF) as RowId;
            let mut v = Vec::with_capacity(dim);
            let base = data_start + i * stride;
            for j in 0..dim {
                let off = base + j * 4;
                v.push(f32::from_le_bytes([data[off],data[off+1],data[off+2],data[off+3]]));
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
    keys: Vec<u64>,
    timestamps: Vec<u64>,
    deleted: Vec<bool>,
    // Buffered column data (one Vec per column)
    column_buffers: Vec<Vec<u8>>,
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
            finished: false,
        }
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
            let buf = &mut self.column_buffers[col_idx];
            match &self.column_tags[col_idx] {
                ColumnTypeTag::Integer => {
                    let i = match value { Value::Integer(v) => *v, Value::Null => i64::MIN, _ => 0 };
                    buf.extend_from_slice(&i.to_le_bytes());
                }
                ColumnTypeTag::Float => {
                    let f = match value { Value::Float(v) => *v, Value::Null => f64::NAN, _ => 0.0 };
                    buf.extend_from_slice(&f.to_le_bytes());
                }
                ColumnTypeTag::Bool => {
                    let b = match value { Value::Bool(v) => *v, _ => false };
                    buf.push(if b { 1 } else { 0 });
                }
                ColumnTypeTag::Timestamp => {
                    let ts = match value { Value::Timestamp(t) => t.as_micros(), Value::Null => i64::MIN, _ => 0 };
                    buf.extend_from_slice(&ts.to_le_bytes());
                }
                ColumnTypeTag::Text => {
                    let s = match value { Value::Text(t) => t.as_str(), Value::Null => "", _ => "" };
                    let len = s.len().min(65535) as u16;
                    buf.extend_from_slice(&len.to_le_bytes());
                    buf.extend_from_slice(s.as_bytes());
                }
                _ => {
                    let bytes = match value { Value::Null => vec![], _ => bincode::serialize(value).unwrap_or_default() };
                    buf.extend_from_slice(&bytes);
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
            let buf = &mut self.column_buffers[col_idx];
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
                        Value::Bool(v) => *v,
                        _ => false,
                    };
                    buf.push(if b { 1 } else { 0 });
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
                            for f in floats { b.extend_from_slice(&f.to_le_bytes()); }
                            b
                        }
                        _ => vec![0u8; 2],
                    };
                    buf.extend_from_slice(&bytes);
                }
                ColumnTypeTag::Spatial => {
                    let geometry = match value {
                        Value::Spatial(g) => (**g).clone(),
                        Value::Null => {
                            buf.extend_from_slice(&[0u8; 2]);
                            continue;
                        }
                        _ => { buf.extend_from_slice(&[0u8; 2]); continue; }
                    };
                    let bytes = bincode::serialize(&geometry).unwrap_or_default();
                    // Store: [len: u16 LE] [bincode bytes]
                    let len = bytes.len().min(65535) as u16;
                    buf.extend_from_slice(&len.to_le_bytes());
                    buf.extend_from_slice(&bytes);
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

    /// Write the columnar SSTable to disk WITHOUT consuming self.
    /// On success: clears internal buffers (data is now on disk).
    /// On failure: ALL data is preserved for retry.
    pub fn finish_and_reset(&mut self) -> Result<()> {
        if self.finished { return Ok(()); }
        if self.num_rows == 0 { return Ok(()); }

        let num_cols = self.column_tags.len();
        let num_rows = self.num_rows;

        // Build column segments with null bitmaps
        let mut segments: Vec<Vec<u8>> = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let tag = &self.column_tags[col_idx];
            let raw = &self.column_buffers[col_idx];
            let null_bytes = (num_rows + 7) / 8;
            let mut seg = Vec::with_capacity(null_bytes + raw.len());

            if tag.is_fixed() {
                // Fixed segment: [null_bitmap] [data]
                let mut nulls = vec![0u8; null_bytes];
                let elem_size = tag.fixed_size();
                // Check for null sentinels
                for row_idx in 0..num_rows {
                    let off = row_idx * elem_size;
                    let is_null = match tag {
                        ColumnTypeTag::Integer | ColumnTypeTag::Timestamp => {
                            raw[off..off+8] == i64::MIN.to_le_bytes()[..]
                        }
                        ColumnTypeTag::Float => {
                            raw[off..off+8] == f64::NAN.to_le_bytes()[..]
                        }
                        ColumnTypeTag::Bool => false,
                        _ => false,
                    };
                    if is_null {
                        nulls[row_idx / 8] |= 1 << (row_idx % 8);
                    }
                }
                seg.extend_from_slice(&nulls);
                seg.extend_from_slice(raw);
            } else if matches!(tag, ColumnTypeTag::Text) {
                // Text segment: [null_bitmap] [offsets] [string_data]
                // Raw buffer format: [(len: u16 LE, bytes)] repeated
                let mut nulls = vec![0u8; null_bytes];
                let mut offsets = Vec::with_capacity((num_rows + 1) * 4);
                let mut str_data = Vec::new();
                let mut current_offset = 0u32;

                let mut pos = 0usize;
                for row_idx in 0..num_rows {
                    if pos + 2 > raw.len() { break; }
                    let len = u16::from_le_bytes([raw[pos], raw[pos+1]]) as usize;
                    pos += 2;
                    let is_empty = len == 0;
                    if is_empty {
                        nulls[row_idx / 8] |= 1 << (row_idx % 8);
                    }
                    offsets.push(current_offset);
                    if !is_empty && pos + len <= raw.len() {
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
            } else {
                // Vector/Spatial: store raw bytes as-is
                let mut nulls = vec![0u8; null_bytes];
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
            row_map[off..off+8].copy_from_slice(&k.to_le_bytes());
        }
        // Timestamps
        let ts_off = num_rows * 8;
        for (i, ts) in self.timestamps.iter().enumerate() {
            let off = ts_off + i * 8;
            row_map[off..off+8].copy_from_slice(&ts.to_le_bytes());
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
            column_entries.push(ColumnIndexEntry { offset: current_offset, size });
            current_offset += size;
            compressed_segs.push(seg_data);
        }
        let row_map_offset = current_offset;

        // Pre-compute total size and allocate buffer
        let total_size = row_map_offset as usize + row_map.len() + FOOTER_SIZE;
        let mut buf = Vec::with_capacity(total_size);

        // Header
        let mut header_tags = [0u8; MAX_COLUMNS];
        for (i, tag) in self.column_tags.iter().enumerate() { header_tags[i] = *tag as u8; }
        let header = ColumnarHeader { num_rows: num_rows as u32, num_columns: num_cols as u16, column_tags: header_tags };
        buf.extend_from_slice(&header.serialize());

        // Column index
        for entry in &column_entries {
            buf.extend_from_slice(&entry.offset.to_le_bytes());
            buf.extend_from_slice(&entry.size.to_le_bytes());
        }

        // Column segments (compressed)
        for seg in &compressed_segs { buf.extend_from_slice(seg); }

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
        let dir = final_path.parent().unwrap_or_else(|| std::path::Path::new("."));
        let tmp_path = dir.join(format!(
            ".{}.tmp",
            final_path.file_name().and_then(|n| n.to_str()).unwrap_or("col.tmp")
        ));
        let file = OpenOptions::new().write(true).create(true).truncate(true).open(&tmp_path)?;
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
    #[cfg_attr(target_os = "macos", ignore = "macOS mmap coherence issue with files < page size")]
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

        // DEBUG: check offsets
        eprintln!("File size: {}", std::fs::metadata(&path).unwrap().len());
        for (i, entry) in col_sst.column_index.iter().enumerate() {
            eprintln!("  col {}: offset={}, size={}", i, entry.offset, entry.size);
        }
        eprintln!("RowMap num_rows={}, data_offset={}", col_sst.row_map.num_rows,
            match &col_sst.row_map.data { crate::storage::lsm::columnar::SegData::Mmap { offset, .. } => *offset, _ => 0 });

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
                Value::Text(crate::types::ArcString(std::sync::Arc::from(
                    format!("cust_{}", i % 100)
                ))),
                Value::Float(i as f64 * 1.5),
                Value::Text(crate::types::ArcString(std::sync::Arc::from(region))),
            ];
            let encoded = crate::storage::row_format::encode(&row, &col_types).unwrap();
            builder.add_row(i as u64, i as u64 + 1000, i % 7 == 0, &encoded).unwrap();
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
            assert!((got_amt - expected_amt).abs() < 0.001, "amount mismatch at {}", i);
            let expected_reg = if i % 3 == 0 { "US" } else { "EU" };
            assert_eq!(reg_seg.get_str(i), Some(expected_reg), "region mismatch at {}", i);
        }
    }
}
