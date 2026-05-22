//! Columnar segment file format: read and write.
//!
//! File layout (v2):
//! ```text
//! +========================================+
//! | HEADER (64 bytes)                      |
//! |   magic:          u32  = 0x4D434442    |
//! |   version:        u32  = 2             |
//! |   table_id:       u32                  |
//! |   row_count:      u32                  |
//! |   min_timestamp:  i64                  |
//! |   max_timestamp:  i64                  |
//! |   column_count:   u16                  |
//! |   flags:          u16                  |
//! |   header_crc32:   u32                  |
//! |   reserved:       [u8; 24]            |
//! +========================================+
//! | COLUMN DATA BLOCKS                     |
//! |   Per column (in schema order):        |
//! |   [Column Block Header (24 bytes)]     |
//! |   [compressed bytes]                   |
//! +========================================+
//! | [STATISTICS BLOCK] (optional, v2)      |
//! |   stats_block_len: u32                 |
//! |   per column: 22 bytes each            |
//! |   stats_crc32: u32                     |
//! +========================================+
//! | [BLOOM FILTER BLOCK] (optional, v2)    |
//! |   bloom_block_len: u32                 |
//! |   num_filters: u16                     |
//! |   per filter: col_id + len + data      |
//! |   bloom_crc32: u32                     |
//! +========================================+
//! | FOOTER (v2: +2 u64 fields)             |
//! |   column_offsets: [u64; column_count]  |
//! |   stats_block_offset: u64              |
//! |   bloom_block_offset: u64              |
//! |   min_row_id:     u64                  |
//! |   max_row_id:     u64                  |
//! |   footer_crc32:   u32                  |
//! +========================================+
//! ```

use crate::{Result, StorageError};
use memmap2::Mmap;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Magic number for columnar segment files: "MCDB"
pub const SEGMENT_MAGIC: u32 = 0x4D434442;
/// Current format version.
pub const SEGMENT_VERSION: u32 = 2;
/// Legacy v1 version.
pub const SEGMENT_VERSION_V1: u32 = 1;
/// Header size in bytes.
pub const HEADER_SIZE: usize = 64;
/// Column block header size in bytes.
pub const COLUMN_BLOCK_HEADER_SIZE: usize = 24;

/// Flag: segment has an extra row_id column (bit 0).
pub const FLAG_HAS_ROW_ID_COLUMN: u16 = 0x0001;
/// Flag: segment has per-column statistics (zone maps) (bit 1).
pub const FLAG_HAS_COLUMN_STATS: u16 = 0x0002;
/// Flag: rows are sorted by timestamp (bit 2).
pub const FLAG_TIMESTAMP_SORTED: u16 = 0x0004;
/// Flag: segment has bloom filters for Text columns (bit 3).
pub const FLAG_HAS_BLOOM_FILTERS: u16 = 0x0008;

/// Column encoding type (stored in column block header).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ColumnEncoding {
    Raw = 0,
    GorillaTimestamp = 1,
    GorillaXorFloat = 2,
    DeltaVarint = 3,
    Dictionary = 4,
    BoolPacked = 5,
}

impl TryFrom<u8> for ColumnEncoding {
    type Error = StorageError;
    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Raw),
            1 => Ok(Self::GorillaTimestamp),
            2 => Ok(Self::GorillaXorFloat),
            3 => Ok(Self::DeltaVarint),
            4 => Ok(Self::Dictionary),
            5 => Ok(Self::BoolPacked),
            _ => Err(StorageError::InvalidData(format!(
                "Unknown column encoding: {}", value
            ))),
        }
    }
}

/// Segment header (64 bytes).
#[derive(Debug, Clone)]
pub struct SegmentHeader {
    pub magic: u32,
    pub version: u32,
    pub table_id: u32,
    pub row_count: u32,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub column_count: u16,
    pub flags: u16,
}

/// A single column's compressed data block.
#[derive(Debug, Clone)]
pub struct ColumnBlock {
    pub column_id: u16,
    pub encoding: ColumnEncoding,
    pub uncompressed_size: u32,
    pub data: Vec<u8>,
    pub null_count: u32,
}

/// Lightweight metadata for segment pruning (loaded without reading column data).
#[derive(Debug, Clone)]
pub struct SegmentMetadata {
    pub path: PathBuf,
    pub table_id: u32,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub min_row_id: u64,
    pub max_row_id: u64,
    pub row_count: u32,
    pub column_count: u16,
    /// Whether the segment stores an extra row_id column.
    pub has_row_id_column: bool,
    /// Whether rows are sorted by timestamp (enables binary search).
    pub is_timestamp_sorted: bool,
    /// Whether the segment has bloom filters for Text columns.
    pub has_bloom_filters: bool,
    pub file_size: u64,
}

/// Per-column statistics for zone map pruning.
/// Fixed-size (22 bytes per column) for efficient storage.
#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    pub column_id: u16,
    /// Min value encoded as raw bytes (i64/f64: to_le_bytes(); bool: 0/1; text: first 8 bytes of UTF-8).
    pub min_value_raw: [u8; 8],
    /// Max value encoded as raw bytes.
    pub max_value_raw: [u8; 8],
    pub null_count: u32,
}

impl ColumnStatistics {
    /// Per-column serialized size: col_id(2) + min(8) + max(8) + null_count(4) = 22 bytes.
    pub const SERIALIZED_SIZE: usize = 22;

    /// Serialize to bytes.
    pub fn to_bytes(&self) -> [u8; Self::SERIALIZED_SIZE] {
        let mut buf = [0u8; Self::SERIALIZED_SIZE];
        buf[0..2].copy_from_slice(&self.column_id.to_le_bytes());
        buf[2..10].copy_from_slice(&self.min_value_raw);
        buf[10..18].copy_from_slice(&self.max_value_raw);
        buf[18..22].copy_from_slice(&self.null_count.to_le_bytes());
        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < Self::SERIALIZED_SIZE {
            return None;
        }
        Some(Self {
            column_id: u16::from_le_bytes([data[0], data[1]]),
            min_value_raw: data[2..10].try_into().ok()?,
            max_value_raw: data[10..18].try_into().ok()?,
            null_count: u32::from_le_bytes([data[18], data[19], data[20], data[21]]),
        })
    }
}

/// Helper: encode a Value's raw bytes for column statistics.
/// Returns [u8; 8] suitable for min/max comparison.
pub fn value_to_raw_bytes(val: &crate::types::Value) -> [u8; 8] {
    use crate::types::Value;
    match val {
        Value::Integer(i) => i.to_le_bytes(),
        Value::Float(f) => f.to_le_bytes(),
        Value::Bool(b) => {
            let mut buf = [0u8; 8];
            buf[0] = if *b { 1 } else { 0 };
            buf
        }
        Value::Timestamp(ts) => ts.as_micros().to_le_bytes(),
        Value::Text(s) => {
            let mut buf = [0u8; 8];
            let bytes = s.as_bytes();
            let len = bytes.len().min(8);
            buf[..len].copy_from_slice(&bytes[..len]);
            buf
        }
        _ => [0u8; 8],
    }
}

/// Compare two raw byte arrays as the given type for zone map pruning.
/// Returns true if a value with `val_raw` could match `query_raw` for the given comparison.
pub fn raw_bytes_compare_bytes(a: &[u8; 8], b: &[u8; 8]) -> std::cmp::Ordering {
    // Compare as i64 (works for Integer, Timestamp, Bool)
    let a_i = i64::from_le_bytes(*a);
    let b_i = i64::from_le_bytes(*b);
    a_i.cmp(&b_i)
}

// ==================== SegmentBuilder ====================

/// Writes a new columnar segment file.
pub struct SegmentBuilder {
    writer: BufWriter<std::fs::File>,
    path: PathBuf,
    table_id: u32,
    column_count: u16,
    column_offsets: Vec<u64>,
    current_offset: u64,
    /// Per-column statistics (optional, written before footer if present).
    statistics: Option<Vec<ColumnStatistics>>,
    /// Bloom filters per column (optional, written before footer if present).
    bloom_filters: Option<Vec<(u16, Vec<u8>)>>,
    /// Whether the data is sorted by timestamp.
    is_timestamp_sorted: bool,
}

impl SegmentBuilder {
    /// Create a new segment builder. Writes to `path.tmp`, renamed on `finish()`.
    pub fn new(path: &Path, table_id: u32, column_count: u16) -> Result<Self> {
        let tmp_path = path.with_extension("mcdb.tmp");
        let file = std::fs::File::create(&tmp_path)
            .map_err(StorageError::Io)?;
        let mut writer = BufWriter::with_capacity(64 * 1024, file);

        // Reserve space for header (64 bytes)
        writer.write_all(&[0u8; HEADER_SIZE])
            .map_err(StorageError::Io)?;

        Ok(Self {
            writer,
            path: path.to_path_buf(),
            table_id,
            column_count,
            column_offsets: Vec::with_capacity(column_count as usize),
            current_offset: HEADER_SIZE as u64,
            statistics: None,
            bloom_filters: None,
            is_timestamp_sorted: false,
        })
    }

    /// Write a single column's compressed data.
    pub fn write_column(
        &mut self,
        column_id: u16,
        encoding: ColumnEncoding,
        data: &[u8],
        uncompressed_size: u32,
        null_count: u32,
    ) -> Result<()> {
        // Record offset
        self.column_offsets.push(self.current_offset);

        // Compute CRC of data
        let data_crc = crc32fast::hash(data);

        // Write column block header (24 bytes)
        let mut header = [0u8; COLUMN_BLOCK_HEADER_SIZE];
        header[0..2].copy_from_slice(&column_id.to_le_bytes());
        header[2] = encoding as u8;
        header[3..7].copy_from_slice(&uncompressed_size.to_le_bytes());
        header[7..11].copy_from_slice(&(data.len() as u32).to_le_bytes());
        header[11..15].copy_from_slice(&null_count.to_le_bytes());
        header[15..19].copy_from_slice(&data_crc.to_le_bytes());
        // 5 bytes reserved

        self.writer.write_all(&header).map_err(StorageError::Io)?;
        self.writer.write_all(data).map_err(StorageError::Io)?;

        self.current_offset += COLUMN_BLOCK_HEADER_SIZE as u64 + data.len() as u64;

        Ok(())
    }

    /// Set per-column statistics for this segment. Must be called before `finish()`.
    pub fn set_statistics(&mut self, stats: Vec<ColumnStatistics>) {
        self.statistics = Some(stats);
    }

    /// Set bloom filters for Text columns. Must be called before `finish()`.
    pub fn set_bloom_filters(&mut self, filters: Vec<(u16, Vec<u8>)>) {
        self.bloom_filters = Some(filters);
    }

    /// Mark the segment as timestamp-sorted. Must be called before `finish()`.
    pub fn set_timestamp_sorted(&mut self, sorted: bool) {
        self.is_timestamp_sorted = sorted;
    }

    /// Finalize segment: write header + footer, fsync, atomic rename.
    pub fn finish(
        mut self,
        row_count: u32,
        min_timestamp: i64,
        max_timestamp: i64,
        min_row_id: u64,
        max_row_id: u64,
    ) -> Result<()> {
        // --- Write optional blocks (v2) ---

        let mut stats_block_offset: u64 = 0;
        let mut bloom_block_offset: u64 = 0;

        // Write statistics block (if present)
        if let Some(ref stats) = self.statistics {
            stats_block_offset = self.current_offset;

            // Serialize: stats_block_len(u32) + N * 22 bytes + crc32(u32)
            let num_stats = stats.len() as u32;
            let data_len = num_stats as usize * ColumnStatistics::SERIALIZED_SIZE;
            let block_len = 4 + data_len + 4; // len prefix + data + crc

            let mut block = Vec::with_capacity(block_len);
            block.extend_from_slice(&num_stats.to_le_bytes());
            for stat in stats {
                block.extend_from_slice(&stat.to_bytes());
            }
            let crc = crc32fast::hash(&block);
            block.extend_from_slice(&crc.to_le_bytes());

            self.writer.write_all(&block).map_err(StorageError::Io)?;
            self.current_offset += block_len as u64;
        }

        // Write bloom filter block (if present)
        if let Some(ref filters) = self.bloom_filters {
            bloom_block_offset = self.current_offset;

            let num_filters = filters.len() as u16;
            let mut block = Vec::new();
            block.extend_from_slice(&num_filters.to_le_bytes());
            for &(col_id, ref data) in filters {
                block.extend_from_slice(&col_id.to_le_bytes());
                block.extend_from_slice(&(data.len() as u32).to_le_bytes());
                block.extend_from_slice(data);
            }
            let crc = crc32fast::hash(&block);
            block.extend_from_slice(&crc.to_le_bytes());

            // Prepend block length
            let mut full_block = Vec::with_capacity(4 + block.len());
            full_block.extend_from_slice(&(block.len() as u32).to_le_bytes());
            full_block.extend_from_slice(&block);

            self.writer.write_all(&full_block).map_err(StorageError::Io)?;
            self.current_offset += full_block.len() as u64;
        }

        // --- Write footer (v2 format) ---
        let mut footer_bytes = Vec::with_capacity(
            self.column_offsets.len() * 8 + 8 + 8 + 8 + 8 + 4
        );

        // Column offsets
        for &offset in &self.column_offsets {
            footer_bytes.extend_from_slice(&offset.to_le_bytes());
        }

        // v2: stats_block_offset + bloom_block_offset
        footer_bytes.extend_from_slice(&stats_block_offset.to_le_bytes());
        footer_bytes.extend_from_slice(&bloom_block_offset.to_le_bytes());

        // min_row_id, max_row_id
        footer_bytes.extend_from_slice(&min_row_id.to_le_bytes());
        footer_bytes.extend_from_slice(&max_row_id.to_le_bytes());

        // Compute footer CRC
        let footer_crc = crc32fast::hash(&footer_bytes);

        // Write footer
        self.writer.write_all(&footer_bytes).map_err(StorageError::Io)?;
        self.writer.write_all(&footer_crc.to_le_bytes())
            .map_err(StorageError::Io)?;

        // --- Write header ---
        // Compute flags
        let has_extra_col = self.column_offsets.len() > self.column_count as usize;
        let mut flags: u16 = 0;
        if has_extra_col {
            flags |= FLAG_HAS_ROW_ID_COLUMN;
        }
        if self.statistics.is_some() {
            flags |= FLAG_HAS_COLUMN_STATS;
        }
        if self.is_timestamp_sorted {
            flags |= FLAG_TIMESTAMP_SORTED;
        }
        if self.bloom_filters.is_some() {
            flags |= FLAG_HAS_BLOOM_FILTERS;
        }

        let mut header = [0u8; HEADER_SIZE];
        header[0..4].copy_from_slice(&SEGMENT_MAGIC.to_le_bytes());
        header[4..8].copy_from_slice(&SEGMENT_VERSION.to_le_bytes());
        header[8..12].copy_from_slice(&self.table_id.to_le_bytes());
        header[12..16].copy_from_slice(&row_count.to_le_bytes());
        header[16..24].copy_from_slice(&min_timestamp.to_le_bytes());
        header[24..32].copy_from_slice(&max_timestamp.to_le_bytes());
        header[32..34].copy_from_slice(&self.column_count.to_le_bytes());
        header[34..36].copy_from_slice(&flags.to_le_bytes());

        // Header CRC over bytes 0..52 (with CRC field zeroed)
        let header_crc = crc32fast::hash(&header[..52]);
        header[36..40].copy_from_slice(&header_crc.to_le_bytes());

        // Seek to beginning and write header
        self.writer.seek(SeekFrom::Start(0)).map_err(StorageError::Io)?;
        self.writer.write_all(&header).map_err(StorageError::Io)?;

        // Flush and sync
        self.writer.flush().map_err(StorageError::Io)?;
        self.writer.get_ref().sync_all().map_err(StorageError::Io)?;

        // Atomic rename
        let tmp_path = self.path.with_extension("mcdb.tmp");
        std::fs::rename(&tmp_path, &self.path).map_err(StorageError::Io)?;

        Ok(())
    }
}

// ==================== SegmentReader ====================

/// Reads an existing columnar segment file.
/// Uses mmap for zero-syscall column reads on immutable segment files.
pub struct SegmentReader {
    path: PathBuf,
    header: SegmentHeader,
    column_offsets: Vec<u64>,
    min_row_id: u64,
    max_row_id: u64,
    /// Offset to statistics block (0 if none).
    stats_block_offset: u64,
    /// Offset to bloom filter block (0 if none).
    bloom_block_offset: u64,
    /// mmap of the entire segment file (zero-syscall reads).
    mmap: Option<Mmap>,
    /// Cached file handle (fallback when mmap unavailable).
    file: Mutex<std::fs::File>,
}

impl SegmentReader {
    /// Open a segment file and read header + footer.
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = std::fs::File::open(path)
            .map_err(StorageError::Io)?;

        // Read header
        let mut header_bytes = [0u8; HEADER_SIZE];
        file.read_exact(&mut header_bytes).map_err(StorageError::Io)?;

        let magic = u32::from_le_bytes(header_bytes[0..4].try_into().unwrap());
        if magic != SEGMENT_MAGIC {
            return Err(StorageError::CorruptedFile(path.to_path_buf()));
        }

        let version = u32::from_le_bytes(header_bytes[4..8].try_into().unwrap());
        if version != SEGMENT_VERSION && version != SEGMENT_VERSION_V1 {
            return Err(StorageError::InvalidData(format!(
                "Unsupported segment version: {}", version
            )));
        }

        // Verify header CRC (computed over bytes 0..36 + 40..52, excluding the CRC field)
        let stored_crc = u32::from_le_bytes(header_bytes[36..40].try_into().unwrap());
        let mut crc_buf = [0u8; 52];
        crc_buf[..36].copy_from_slice(&header_bytes[..36]);
        crc_buf[36..40].copy_from_slice(&[0u8; 4]); // zero CRC field
        crc_buf[40..52].copy_from_slice(&header_bytes[40..52]);
        let computed_crc = crc32fast::hash(&crc_buf);
        if stored_crc != computed_crc {
            return Err(StorageError::CorruptedFile(path.to_path_buf()));
        }

        let header = SegmentHeader {
            magic,
            version,
            table_id: u32::from_le_bytes(header_bytes[8..12].try_into().unwrap()),
            row_count: u32::from_le_bytes(header_bytes[12..16].try_into().unwrap()),
            min_timestamp: i64::from_le_bytes(header_bytes[16..24].try_into().unwrap()),
            max_timestamp: i64::from_le_bytes(header_bytes[24..32].try_into().unwrap()),
            column_count: u16::from_le_bytes(header_bytes[32..34].try_into().unwrap()),
            flags: u16::from_le_bytes(header_bytes[34..36].try_into().unwrap()),
        };

        // Read footer — format depends on version
        let column_count = header.column_count as usize;
        let has_row_id_col = header.flags & FLAG_HAS_ROW_ID_COLUMN != 0;
        let total_col_offsets = column_count + if has_row_id_col { 1 } else { 0 };

        let file_size = file.metadata().map_err(StorageError::Io)?.len();

        let (column_offsets, min_row_id, max_row_id, stats_block_offset, bloom_block_offset) =
            if version == SEGMENT_VERSION_V1 {
                // v1 footer: column_offsets + min_row_id + max_row_id + crc
                let footer_data_size = total_col_offsets * 8 + 8 + 8 + 4;
                let footer_start = file_size - footer_data_size as u64;
                file.seek(SeekFrom::Start(footer_start)).map_err(StorageError::Io)?;

                let mut footer_buf = vec![0u8; footer_data_size];
                file.read_exact(&mut footer_buf).map_err(StorageError::Io)?;

                let mut cursor = 0usize;
                let mut col_offsets = Vec::with_capacity(total_col_offsets);
                for _ in 0..total_col_offsets {
                    let offset = u64::from_le_bytes(footer_buf[cursor..cursor + 8].try_into().unwrap());
                    col_offsets.push(offset);
                    cursor += 8;
                }
                let min_rid = u64::from_le_bytes(footer_buf[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;
                let max_rid = u64::from_le_bytes(footer_buf[cursor..cursor + 8].try_into().unwrap());

                (col_offsets, min_rid, max_rid, 0u64, 0u64)
            } else {
                // v2 footer: column_offsets + stats_offset + bloom_offset + min_row_id + max_row_id + crc
                let footer_data_size = total_col_offsets * 8 + 8 + 8 + 8 + 8 + 4;
                let footer_start = file_size - footer_data_size as u64;
                file.seek(SeekFrom::Start(footer_start)).map_err(StorageError::Io)?;

                let mut footer_buf = vec![0u8; footer_data_size];
                file.read_exact(&mut footer_buf).map_err(StorageError::Io)?;

                let mut cursor = 0usize;
                let mut col_offsets = Vec::with_capacity(total_col_offsets);
                for _ in 0..total_col_offsets {
                    let offset = u64::from_le_bytes(footer_buf[cursor..cursor + 8].try_into().unwrap());
                    col_offsets.push(offset);
                    cursor += 8;
                }
                let stats_off = u64::from_le_bytes(footer_buf[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;
                let bloom_off = u64::from_le_bytes(footer_buf[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;
                let min_rid = u64::from_le_bytes(footer_buf[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;
                let max_rid = u64::from_le_bytes(footer_buf[cursor..cursor + 8].try_into().unwrap());

                (col_offsets, min_rid, max_rid, stats_off, bloom_off)
            };

        // mmap for zero-syscall column reads
        let mmap = unsafe { Mmap::map(&file).ok() };

        Ok(Self {
            path: path.to_path_buf(),
            header,
            column_offsets,
            min_row_id,
            max_row_id,
            stats_block_offset,
            bloom_block_offset,
            mmap,
            file: Mutex::new(file),
        })
    }

    /// Get segment header.
    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    /// Get lightweight metadata for pruning.
    pub fn metadata(&self) -> SegmentMetadata {
        let file_size = std::fs::metadata(&self.path)
            .map(|m| m.len())
            .unwrap_or(0);
        let has_row_id_column = self.column_offsets.len() > self.header.column_count as usize;
        let is_timestamp_sorted = self.header.flags & FLAG_TIMESTAMP_SORTED != 0;
        let has_bloom_filters = self.header.flags & FLAG_HAS_BLOOM_FILTERS != 0;
        SegmentMetadata {
            path: self.path.clone(),
            table_id: self.header.table_id,
            min_timestamp: self.header.min_timestamp,
            max_timestamp: self.header.max_timestamp,
            min_row_id: self.min_row_id,
            max_row_id: self.max_row_id,
            row_count: self.header.row_count,
            column_count: self.header.column_count,
            has_row_id_column,
            is_timestamp_sorted,
            has_bloom_filters,
            file_size,
        }
    }

    /// Read a single column's data (column projection).
    /// Uses mmap for zero-syscall reads, falls back to seek+read.
    pub fn read_column(&self, column_id: u16) -> Result<ColumnBlock> {
        let idx = column_id as usize;
        if idx >= self.column_offsets.len() {
            return Err(StorageError::InvalidData(format!(
                "Column {} out of range (max {})", column_id, self.column_offsets.len()
            )));
        }

        // Try mmap path (zero syscall)
        if let Some(ref mmap) = self.mmap {
            let off = self.column_offsets[idx] as usize;
            if off + COLUMN_BLOCK_HEADER_SIZE > mmap.len() {
                return Err(StorageError::CorruptedFile(self.path.clone()));
            }
            let cb_header = &mmap[off..off + COLUMN_BLOCK_HEADER_SIZE];

            let stored_col_id = u16::from_le_bytes(cb_header[0..2].try_into().unwrap());
            debug_assert_eq!(stored_col_id, column_id);

            let encoding = ColumnEncoding::try_from(cb_header[2])?;
            let uncompressed_size = u32::from_le_bytes(cb_header[3..7].try_into().unwrap());
            let compressed_size = u32::from_le_bytes(cb_header[7..11].try_into().unwrap());
            let null_count = u32::from_le_bytes(cb_header[11..15].try_into().unwrap());
            let stored_crc = u32::from_le_bytes(cb_header[15..19].try_into().unwrap());

            let data_start = off + COLUMN_BLOCK_HEADER_SIZE;
            let data_end = data_start + compressed_size as usize;
            if data_end > mmap.len() {
                return Err(StorageError::CorruptedFile(self.path.clone()));
            }
            let data = mmap[data_start..data_end].to_vec();

            let computed_crc = crc32fast::hash(&data);
            if stored_crc != computed_crc {
                return Err(StorageError::CorruptedFile(self.path.clone()));
            }

            return Ok(ColumnBlock {
                column_id,
                encoding,
                uncompressed_size,
                data,
                null_count,
            });
        }

        // Fallback: seek+read
        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(self.column_offsets[idx]))
            .map_err(StorageError::Io)?;

        let mut cb_header = [0u8; COLUMN_BLOCK_HEADER_SIZE];
        file.read_exact(&mut cb_header).map_err(StorageError::Io)?;

        let stored_col_id = u16::from_le_bytes(cb_header[0..2].try_into().unwrap());
        debug_assert_eq!(stored_col_id, column_id);

        let encoding = ColumnEncoding::try_from(cb_header[2])?;
        let uncompressed_size = u32::from_le_bytes(cb_header[3..7].try_into().unwrap());
        let compressed_size = u32::from_le_bytes(cb_header[7..11].try_into().unwrap());
        let null_count = u32::from_le_bytes(cb_header[11..15].try_into().unwrap());
        let stored_crc = u32::from_le_bytes(cb_header[15..19].try_into().unwrap());

        let mut data = vec![0u8; compressed_size as usize];
        file.read_exact(&mut data).map_err(StorageError::Io)?;

        let computed_crc = crc32fast::hash(&data);
        if stored_crc != computed_crc {
            return Err(StorageError::CorruptedFile(self.path.clone()));
        }

        Ok(ColumnBlock {
            column_id,
            encoding,
            uncompressed_size,
            data,
            null_count,
        })
    }

    /// Read all columns.
    pub fn read_all_columns(&self) -> Result<Vec<ColumnBlock>> {
        let mut columns = Vec::with_capacity(self.header.column_count as usize);
        for i in 0..self.header.column_count {
            columns.push(self.read_column(i)?);
        }
        Ok(columns)
    }

    /// Read per-column statistics (zone maps) from the statistics block.
    /// Returns None if the segment has no statistics block (v1 or no stats).
    pub fn read_statistics(&self) -> Result<Option<Vec<ColumnStatistics>>> {
        if self.stats_block_offset == 0 {
            return Ok(None);
        }

        // Try mmap path
        if let Some(ref mmap) = self.mmap {
            let off = self.stats_block_offset as usize;
            if off + 4 > mmap.len() {
                return Err(StorageError::CorruptedFile(self.path.clone()));
            }
            let buf = &mmap[off..off+4];
            let num_stats = u32::from_le_bytes(buf.try_into().unwrap()) as usize;

            let stats_data_size = num_stats * ColumnStatistics::SERIALIZED_SIZE;
            let total_needed = 4 + stats_data_size + 4; // num_stats + data + CRC
            if off + total_needed > mmap.len() {
                return Err(StorageError::CorruptedFile(self.path.clone()));
            }

            let stats_buf = &mmap[off+4..off+4+stats_data_size+4];

            // Verify CRC
            let mut crc_input = Vec::with_capacity(4 + stats_data_size);
            crc_input.extend_from_slice(buf);
            crc_input.extend_from_slice(&stats_buf[..stats_data_size]);
            let stored_crc = u32::from_le_bytes(
                stats_buf[stats_data_size..stats_data_size+4].try_into().unwrap()
            );
            let computed_crc = crc32fast::hash(&crc_input);
            if stored_crc != computed_crc {
                return Err(StorageError::CorruptedFile(self.path.clone()));
            }

            let mut stats = Vec::with_capacity(num_stats);
            for i in 0..num_stats {
                let s_off = i * ColumnStatistics::SERIALIZED_SIZE;
                if let Some(stat) = ColumnStatistics::from_bytes(
                    &stats_buf[s_off..s_off + ColumnStatistics::SERIALIZED_SIZE]
                ) {
                    stats.push(stat);
                }
            }
            return Ok(Some(stats));
        }

        // Fallback: seek+read
        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(self.stats_block_offset))
            .map_err(StorageError::Io)?;

        let mut buf = [0u8; 4];
        file.read_exact(&mut buf).map_err(StorageError::Io)?;
        let num_stats = u32::from_le_bytes(buf) as usize;

        let stats_data_size = num_stats * ColumnStatistics::SERIALIZED_SIZE;
        let mut stats_buf = vec![0u8; stats_data_size + 4];
        file.read_exact(&mut stats_buf).map_err(StorageError::Io)?;

        let mut crc_input = Vec::with_capacity(4 + stats_data_size);
        crc_input.extend_from_slice(&buf);
        crc_input.extend_from_slice(&stats_buf[..stats_data_size]);
        let stored_crc = u32::from_le_bytes(
            stats_buf[stats_data_size..stats_data_size + 4].try_into().unwrap()
        );
        let computed_crc = crc32fast::hash(&crc_input);
        if stored_crc != computed_crc {
            return Err(StorageError::CorruptedFile(self.path.clone()));
        }

        let mut stats = Vec::with_capacity(num_stats);
        for i in 0..num_stats {
            let offset = i * ColumnStatistics::SERIALIZED_SIZE;
            if let Some(stat) = ColumnStatistics::from_bytes(
                &stats_buf[offset..offset + ColumnStatistics::SERIALIZED_SIZE]
            ) {
                stats.push(stat);
            }
        }

        Ok(Some(stats))
    }

    /// Read bloom filters from the bloom filter block.
    /// Returns None if the segment has no bloom filters.
    /// Returns a HashMap<column_id, bloom_bytes>.
    pub fn read_bloom_filters(&self) -> Result<Option<HashMap<u16, Vec<u8>>>> {
        if self.bloom_block_offset == 0 {
            return Ok(None);
        }

        // Try mmap path
        if let Some(ref mmap) = self.mmap {
            let off = self.bloom_block_offset as usize;
            if off + 4 > mmap.len() {
                return Err(StorageError::CorruptedFile(self.path.clone()));
            }
            let block_len = u32::from_le_bytes(mmap[off..off+4].try_into().unwrap()) as usize;
            if block_len < 4 || off + 4 + block_len > mmap.len() {
                return Err(StorageError::CorruptedFile(self.path.clone()));
            }
            let block_buf = &mmap[off+4..off+4+block_len];

            let data_len = block_len - 4;
            let stored_crc = u32::from_le_bytes(
                block_buf[data_len..data_len+4].try_into().unwrap()
            );
            let computed_crc = crc32fast::hash(&block_buf[..data_len]);
            if stored_crc != computed_crc {
                return Err(StorageError::CorruptedFile(self.path.clone()));
            }

            let mut cursor = 0usize;
            if data_len < 2 { return Ok(None); }
            let num_filters = u16::from_le_bytes(block_buf[cursor..cursor+2].try_into().unwrap()) as usize;
            cursor += 2;

            let mut filters = HashMap::new();
            for _ in 0..num_filters {
                if cursor + 6 > data_len { break; }
                let col_id = u16::from_le_bytes(block_buf[cursor..cursor+2].try_into().unwrap());
                cursor += 2;
                let filter_len = u32::from_le_bytes(block_buf[cursor..cursor+4].try_into().unwrap()) as usize;
                cursor += 4;
                if cursor + filter_len > data_len { break; }
                filters.insert(col_id, block_buf[cursor..cursor+filter_len].to_vec());
                cursor += filter_len;
            }
            return Ok(Some(filters));
        }

        // Fallback: seek+read
        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(self.bloom_block_offset))
            .map_err(StorageError::Io)?;

        // Read block_len (u32)
        let mut buf = [0u8; 4];
        file.read_exact(&mut buf).map_err(StorageError::Io)?;
        let block_len = u32::from_le_bytes(buf) as usize;

        // Read the rest of the block
        let mut block_buf = vec![0u8; block_len];
        file.read_exact(&mut block_buf).map_err(StorageError::Io)?;

        // Verify CRC (stored at end of block_buf)
        if block_len < 4 {
            return Err(StorageError::CorruptedFile(self.path.clone()));
        }
        let data_len = block_len - 4;
        let stored_crc = u32::from_le_bytes(
            block_buf[data_len..data_len + 4].try_into().unwrap()
        );
        let computed_crc = crc32fast::hash(&block_buf[..data_len]);
        if stored_crc != computed_crc {
            return Err(StorageError::CorruptedFile(self.path.clone()));
        }

        // Parse: num_filters(u16) + per filter: col_id(u16) + len(u32) + data
        let mut cursor = 0usize;
        let num_filters = u16::from_le_bytes(
            block_buf[cursor..cursor + 2].try_into().unwrap()
        ) as usize;
        cursor += 2;

        let mut filters = HashMap::with_capacity(num_filters);
        for _ in 0..num_filters {
            if cursor + 6 > data_len {
                break;
            }
            let col_id = u16::from_le_bytes(
                block_buf[cursor..cursor + 2].try_into().unwrap()
            );
            cursor += 2;
            let filter_data_len = u32::from_le_bytes(
                block_buf[cursor..cursor + 4].try_into().unwrap()
            ) as usize;
            cursor += 4;
            if cursor + filter_data_len > data_len {
                break;
            }
            let filter_data = block_buf[cursor..cursor + filter_data_len].to_vec();
            cursor += filter_data_len;
            filters.insert(col_id, filter_data);
        }

        Ok(Some(filters))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_segment_path(dir: &TempDir) -> PathBuf {
        dir.path().join("seg_1000_2000.mcdb")
    }

    #[test]
    fn test_segment_write_read_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = test_segment_path(&dir);

        // Write segment with 3 columns
        {
            let mut builder = SegmentBuilder::new(&path, 42, 3).unwrap();

            // Column 0: timestamp (Gorilla encoded)
            let ts_data = super::super::gorilla::encode_timestamps(&[1000i64, 2000, 3000]);
            builder.write_column(0, ColumnEncoding::GorillaTimestamp, &ts_data, 24, 0).unwrap();

            // Column 1: float (XOR encoded)
            let float_data = super::super::gorilla::encode_floats(&[25.0f64, 26.0, 27.0]);
            builder.write_column(1, ColumnEncoding::GorillaXorFloat, &float_data, 24, 0).unwrap();

            // Column 2: integer (delta-varint)
            let int_data = super::super::gorilla::encode_integers(&[100i64, 200, 300]);
            builder.write_column(2, ColumnEncoding::DeltaVarint, &int_data, 24, 0).unwrap();

            builder.finish(3, 1000, 3000, 10, 12).unwrap();
        }

        // Read back
        let reader = SegmentReader::open(&path).unwrap();
        let header = reader.header();
        assert_eq!(header.table_id, 42);
        assert_eq!(header.row_count, 3);
        assert_eq!(header.min_timestamp, 1000);
        assert_eq!(header.max_timestamp, 3000);
        assert_eq!(header.column_count, 3);

        // Verify metadata
        let meta = reader.metadata();
        assert_eq!(meta.min_row_id, 10);
        assert_eq!(meta.max_row_id, 12);
        assert_eq!(meta.row_count, 3);

        // Read individual columns (projection)
        let col0 = reader.read_column(0).unwrap();
        assert_eq!(col0.encoding, ColumnEncoding::GorillaTimestamp);
        let ts = super::super::gorilla::decode_timestamps(&col0.data, 3);
        assert_eq!(ts, vec![1000i64, 2000, 3000]);

        let col1 = reader.read_column(1).unwrap();
        assert_eq!(col1.encoding, ColumnEncoding::GorillaXorFloat);
        let floats = super::super::gorilla::decode_floats(&col1.data, 3);
        assert_eq!(floats, vec![25.0f64, 26.0, 27.0]);

        let col2 = reader.read_column(2).unwrap();
        assert_eq!(col2.encoding, ColumnEncoding::DeltaVarint);
        let ints = super::super::gorilla::decode_integers(&col2.data, 3);
        assert_eq!(ints, vec![100i64, 200, 300]);
    }

    #[test]
    fn test_segment_column_projection() {
        let dir = TempDir::new().unwrap();
        let path = test_segment_path(&dir);

        // Write segment with 5 columns
        {
            let mut builder = SegmentBuilder::new(&path, 1, 5).unwrap();
            for i in 0..5u16 {
                builder.write_column(i, ColumnEncoding::Raw, &[i as u8; 100], 100, 0).unwrap();
            }
            builder.finish(10, 0, 100, 0, 9).unwrap();
        }

        let reader = SegmentReader::open(&path).unwrap();

        // Only read column 2 and 4
        let col2 = reader.read_column(2).unwrap();
        assert_eq!(col2.data, vec![2u8; 100]);

        let col4 = reader.read_column(4).unwrap();
        assert_eq!(col4.data, vec![4u8; 100]);
    }

    #[test]
    fn test_segment_corrupted_header() {
        let dir = TempDir::new().unwrap();
        let path = test_segment_path(&dir);

        // Write a valid segment
        {
            let mut builder = SegmentBuilder::new(&path, 1, 1).unwrap();
            builder.write_column(0, ColumnEncoding::Raw, &[1, 2, 3], 3, 0).unwrap();
            builder.finish(1, 0, 1, 0, 0).unwrap();
        }

        // Corrupt the file
        let data = std::fs::read(&path).unwrap();
        let mut corrupted = data.clone();
        corrupted[10] ^= 0xFF; // flip a byte in header
        std::fs::write(&path, &corrupted).unwrap();

        let result = SegmentReader::open(&path);
        assert!(result.is_err());
    }

    #[test]
    fn test_segment_read_all_columns() {
        let dir = TempDir::new().unwrap();
        let path = test_segment_path(&dir);

        {
            let mut builder = SegmentBuilder::new(&path, 1, 3).unwrap();
            builder.write_column(0, ColumnEncoding::Raw, &[0u8; 50], 50, 0).unwrap();
            builder.write_column(1, ColumnEncoding::Raw, &[1u8; 50], 50, 0).unwrap();
            builder.write_column(2, ColumnEncoding::Raw, &[2u8; 50], 50, 0).unwrap();
            builder.finish(5, 0, 100, 0, 4).unwrap();
        }

        let reader = SegmentReader::open(&path).unwrap();
        let all = reader.read_all_columns().unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].data, vec![0u8; 50]);
        assert_eq!(all[1].data, vec![1u8; 50]);
        assert_eq!(all[2].data, vec![2u8; 50]);
    }

    #[test]
    fn test_segment_v2_statistics_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg_stats.mcdb");

        // Write segment with statistics
        {
            let mut builder = SegmentBuilder::new(&path, 1, 2).unwrap();
            let ts_data = super::super::gorilla::encode_timestamps(&[1000i64, 2000, 3000]);
            builder.write_column(0, ColumnEncoding::GorillaTimestamp, &ts_data, 24, 0).unwrap();
            let float_data = super::super::gorilla::encode_floats(&[10.0f64, 20.0, 30.0]);
            builder.write_column(1, ColumnEncoding::GorillaXorFloat, &float_data, 24, 0).unwrap();

            let stats = vec![
                ColumnStatistics {
                    column_id: 0,
                    min_value_raw: 1000i64.to_le_bytes(),
                    max_value_raw: 3000i64.to_le_bytes(),
                    null_count: 0,
                },
                ColumnStatistics {
                    column_id: 1,
                    min_value_raw: 10.0f64.to_le_bytes(),
                    max_value_raw: 30.0f64.to_le_bytes(),
                    null_count: 1,
                },
            ];
            builder.set_statistics(stats);
            builder.set_timestamp_sorted(true);
            builder.finish(3, 1000, 3000, 0, 2).unwrap();
        }

        // Read back
        let reader = SegmentReader::open(&path).unwrap();
        let meta = reader.metadata();
        assert!(meta.is_timestamp_sorted);
        assert!(!meta.has_bloom_filters);

        let stats = reader.read_statistics().unwrap().unwrap();
        assert_eq!(stats.len(), 2);
        assert_eq!(stats[0].column_id, 0);
        assert_eq!(i64::from_le_bytes(stats[0].min_value_raw), 1000);
        assert_eq!(i64::from_le_bytes(stats[0].max_value_raw), 3000);
        assert_eq!(stats[0].null_count, 0);
        assert_eq!(stats[1].column_id, 1);
        assert_eq!(f64::from_le_bytes(stats[1].min_value_raw), 10.0);
        assert_eq!(f64::from_le_bytes(stats[1].max_value_raw), 30.0);
        assert_eq!(stats[1].null_count, 1);
    }

    #[test]
    fn test_segment_v2_bloom_filter_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg_bloom.mcdb");

        use crate::storage::lsm::BloomFilter;

        // Build a bloom filter
        let mut bloom = BloomFilter::new(100, 10);
        bloom.insert(b"hello");
        bloom.insert(b"world");
        let bloom_bytes = bloom.to_bytes();

        // Write segment with bloom filter
        {
            let mut builder = SegmentBuilder::new(&path, 1, 1).unwrap();
            let text_data = vec![0u8; 50]; // dummy
            builder.write_column(0, ColumnEncoding::Raw, &text_data, 50, 0).unwrap();
            builder.set_bloom_filters(vec![(0u16, bloom_bytes)]);
            builder.finish(10, 0, 100, 0, 9).unwrap();
        }

        // Read back
        let reader = SegmentReader::open(&path).unwrap();
        let meta = reader.metadata();
        assert!(meta.has_bloom_filters);
        assert!(!meta.is_timestamp_sorted);

        let filters = reader.read_bloom_filters().unwrap().unwrap();
        assert!(filters.contains_key(&0));
        let filter_data = &filters[&0];

        let bloom = BloomFilter::from_bytes_full(filter_data).unwrap();
        assert!(bloom.may_contain(b"hello"));
        assert!(bloom.may_contain(b"world"));
        assert!(!bloom.may_contain(b"nonexistent"));
    }

    #[test]
    fn test_segment_v2_no_stats_no_bloom() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg_plain_v2.mcdb");

        // Write v2 segment without stats or bloom
        {
            let mut builder = SegmentBuilder::new(&path, 1, 2).unwrap();
            builder.write_column(0, ColumnEncoding::Raw, &[1u8; 20], 20, 0).unwrap();
            builder.write_column(1, ColumnEncoding::Raw, &[2u8; 20], 20, 0).unwrap();
            builder.finish(5, 0, 100, 0, 4).unwrap();
        }

        let reader = SegmentReader::open(&path).unwrap();
        assert!(reader.read_statistics().unwrap().is_none());
        assert!(reader.read_bloom_filters().unwrap().is_none());

        let meta = reader.metadata();
        assert!(!meta.is_timestamp_sorted);
        assert!(!meta.has_bloom_filters);
    }

    #[test]
    fn test_column_statistics_serialization() {
        let stat = ColumnStatistics {
            column_id: 5,
            min_value_raw: 42i64.to_le_bytes(),
            max_value_raw: 100i64.to_le_bytes(),
            null_count: 3,
        };

        let bytes = stat.to_bytes();
        let restored = ColumnStatistics::from_bytes(&bytes).unwrap();
        assert_eq!(restored.column_id, 5);
        assert_eq!(restored.min_value_raw, 42i64.to_le_bytes());
        assert_eq!(restored.max_value_raw, 100i64.to_le_bytes());
        assert_eq!(restored.null_count, 3);
    }

    #[test]
    fn test_value_to_raw_bytes() {
        use crate::types::{Timestamp, Value, ArcString};
        use std::sync::Arc;

        let v_int = Value::Integer(42);
        let bytes = value_to_raw_bytes(&v_int);
        assert_eq!(i64::from_le_bytes(bytes), 42);

        let v_float = Value::Float(3.14);
        let bytes = value_to_raw_bytes(&v_float);
        assert_eq!(f64::from_le_bytes(bytes), 3.14);

        let v_ts = Value::Timestamp(Timestamp::from_micros(9999));
        let bytes = value_to_raw_bytes(&v_ts);
        assert_eq!(i64::from_le_bytes(bytes), 9999);

        let v_text = Value::Text(ArcString(Arc::new("hello".to_string())));
        let bytes = value_to_raw_bytes(&v_text);
        assert_eq!(&bytes[..5], b"hello");
    }
}
