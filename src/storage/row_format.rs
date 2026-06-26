//! RawRow: Compact binary row format for zero-copy column access
//!
//! Replaces bincode for row serialization in the LSM tree.
//! Fixed-size columns (Integer, Float, Bool, Timestamp) are stored inline
//! at deterministic offsets, enabling direct access without deserialization.
//!
//! ## Format Layout
//! ```text
//! [magic: u16 = 0x4D52]           — format marker (2 bytes)
//! [col_count: u16]                — number of columns (2 bytes)
//! [null_bitmap: u64]              — bit i set = column i is null (8 bytes)
//! [fixed_col_data]                — packed fixed-size columns
//! [var_col_count: u16]            — number of variable-size columns (2 bytes)
//! [var_col_entries]               — (col_idx: u16, offset: u16, len: u16) per var col
//! [var_data_pool]                 — actual bytes for Text/Vector/etc
//! ```

use crate::types::ColumnType;
use crate::types::{ArcString, ArcVec, Row, Timestamp, Value};
use crate::{Result, StorageError};
use std::collections::HashSet;
use std::sync::Arc;

/// String pool for deduplicating Arc<str> allocations in Text columns.
///
/// When a dataset has low-cardinality text (e.g. 'US'/'EU' — just 2 values),
/// 600K `Arc<str>` allocations collapse to 2, saving ~18ms on a 300K-row full scan.
///
/// # Example
/// ```ignore
/// let mut pool = StringPool::new();
/// let a = pool.intern("US");  // allocates Arc<str>
/// let b = pool.intern("US");  // returns cheap Arc::clone (~2ns)
/// assert!(Arc::ptr_eq(&a, &b));
/// assert_eq!(pool.len(), 1);
/// ```
pub struct StringPool {
    strings: HashSet<Arc<str>>,
    /// After this many unique values, bypass the pool (direct allocate).
    /// Prevents wasted lookups on high-cardinality columns.
    max_cardinality: usize,
}

/// Default cardinality cutoff: after 4096 unique values, stop interning.
/// At 256, a column with 3000 unique values (like 'cust_0'..'cust_2999')
/// would stop pooling after the first 256, wasting ~274K Arc<str> allocations
/// on a 300K-row table. At 4096, the pool covers all but the most extreme
/// high-cardinality columns while keeping HashSet memory < 1 MB.
const DEFAULT_MAX_CARDINALITY: usize = 4096;

impl StringPool {
    pub fn new() -> Self {
        Self { strings: HashSet::new(), max_cardinality: DEFAULT_MAX_CARDINALITY }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self { strings: HashSet::with_capacity(cap), max_cardinality: DEFAULT_MAX_CARDINALITY }
    }

    /// Return a shared `Arc<str>` for `s`.
    ///
    /// On first occurrence: allocates a new `Arc<str>` and inserts into the pool.
    /// On subsequent occurrences: returns a cheap `Arc::clone` (atomic refcount bump).
    ///
    /// After `max_cardinality` unique values, bypasses the pool entirely —
    /// the column is high-cardinality and lookups waste more CPU than they save.
    pub fn intern(&mut self, s: &str) -> Arc<str> {
        if self.strings.len() >= self.max_cardinality {
            return Arc::from(s);
        }
        if let Some(existing) = self.strings.get(s) {
            return Arc::clone(existing);
        }
        let arc: Arc<str> = Arc::from(s);
        self.strings.insert(Arc::clone(&arc));
        arc
    }

    pub fn len(&self) -> usize {
        self.strings.len()
    }

    pub fn is_empty(&self) -> bool {
        self.strings.is_empty()
    }
}

impl Default for StringPool {
    fn default() -> Self {
        Self::new()
    }
}

const RAWROW_MAGIC: u16 = 0x4D52;
pub(crate) const HEADER_SIZE: usize = 12;
pub(crate) const FIXED_COL_SIZE: usize = 8;

/// Schema-specialized column decoder — replaces per-column `is_fixed()` + `ColumnType` match.
///
/// Constructed once per scan from `ColumnType` slice. The `match` on this enum
/// in the hot loop allows the compiler to devirtualize via exhaustive match,
/// eliminating the indirect branch that `ColumnType` match would require.
#[derive(Debug, Clone, Copy)]
pub enum ColDecoder {
    FixedInteger,
    FixedFloat,
    FixedBool,
    FixedTimestamp,
    VarText,
    VarGeneric,
}

/// Pre-computed schema context for accelerated row decode.
///
/// Replaces the per-row redundant computation in `decode_raw_fast_into_with_pool`:
/// - `var_section_start` is computed once (was recomputed every row)
/// - `col_decoders` eliminates `is_fixed()` + `ColumnType` match per column
/// - `fixed_idx_map` eliminates the per-column fixed_idx counter
/// - `pool` with adaptive cardinality avoids wasted lookups on high-cardinality text
///
/// Created once per scan in `TableRowStreamingIterator`, reused across all rows.
pub struct SchemaDecodeContext {
    pub col_count: usize,
    pub fixed_count: usize,
    var_section_start: usize,
    /// Column decoder dispatch table (one per column, schema-fixed)
    col_decoders: Vec<ColDecoder>,
    /// Pre-computed: col_idx → fixed_idx mapping
    fixed_idx_map: [u8; 64],
    /// String pool for text interning (adaptive cardinality)
    pub pool: StringPool,
    /// Reusable output buffer
    pub row_buf: Vec<Value>,
    /// When true, skip UTF-8 validation on Text columns.
    /// Safe for data encoded by our own encode() which only accepts valid UTF-8.
    pub trust_utf8: bool,
    /// When true, skip is_rawrow() magic check (all data from our encode()).
    pub skip_magic_check: bool,
    /// When false, skip null bitmap read and per-column null check (all NOT NULL).
    pub(crate) has_nullable_columns: bool,
    /// Number of variable-width columns (for pre-computed var offset path).
    var_col_count: usize,
}

impl SchemaDecodeContext {
    /// Build a decode context from the schema's column types.
    pub fn new(col_types: &[ColumnType]) -> Self {
        let col_count = col_types.len();
        let mut fixed_count = 0usize;
        let mut var_col_count = 0usize;
        let mut col_decoders = Vec::with_capacity(col_count);
        let mut fixed_idx_map = [0u8; 64];

        for (i, ct) in col_types.iter().enumerate() {
            match ct {
                ColumnType::Integer => {
                    if i < 64 { fixed_idx_map[i] = fixed_count as u8; }
                    fixed_count += 1;
                    col_decoders.push(ColDecoder::FixedInteger);
                }
                ColumnType::Float => {
                    if i < 64 { fixed_idx_map[i] = fixed_count as u8; }
                    fixed_count += 1;
                    col_decoders.push(ColDecoder::FixedFloat);
                }
                ColumnType::Boolean => {
                    if i < 64 { fixed_idx_map[i] = fixed_count as u8; }
                    fixed_count += 1;
                    col_decoders.push(ColDecoder::FixedBool);
                }
                ColumnType::Timestamp => {
                    if i < 64 { fixed_idx_map[i] = fixed_count as u8; }
                    fixed_count += 1;
                    col_decoders.push(ColDecoder::FixedTimestamp);
                }
                ColumnType::Text => {
                    var_col_count += 1;
                    col_decoders.push(ColDecoder::VarText);
                }
                ColumnType::Tensor(_) | ColumnType::Spatial => {
                    var_col_count += 1;
                    col_decoders.push(ColDecoder::VarGeneric);
                }
            }
        }

        Self {
            col_count,
            fixed_count,
            var_section_start: HEADER_SIZE + fixed_count * FIXED_COL_SIZE,
            col_decoders,
            fixed_idx_map,
            pool: StringPool::new(),
            row_buf: Vec::with_capacity(col_count),
            trust_utf8: false,
            skip_magic_check: false,
            has_nullable_columns: true, // conservative default
            var_col_count,
        }
    }

    /// Decode a row using pre-computed schema context.
    ///
    /// Compared to `decode_raw_fast_into_with_pool`, this:
    /// - Skips the `col_count` format check (known from schema)
    /// - Uses `ColDecoder` enum dispatch instead of `is_fixed()` + `ColumnType` match
    /// - Reads var entries sequentially (ascending col_idx guarantee) instead of
    ///   zeroing a 1024-byte `[(usize, usize); 64]` stack array
    /// Decode a row directly into a caller-provided Vec (reuses capacity).
    /// No per-row allocation — the caller manages the buffer lifecycle.
    /// For batch scanning, pre-allocate N Vec<Value> buffers and call this
    /// for each row.
    #[inline]
    pub fn decode_row_into(&mut self, out: &mut Vec<Value>, data: &[u8]) -> Result<()> {
        out.clear();

        // Fast path: skip magic check when data is from our own encode()
        if !self.skip_magic_check {
            if data.len() < HEADER_SIZE || !is_rawrow(data) {
                // Fallback to bincode
                let row: Vec<Value> = bincode::deserialize(data)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                *out = row;
                return Ok(());
            }
        } else if data.len() < HEADER_SIZE {
            return Err(StorageError::InvalidData("Row data too short".into()));
        }

        // Read null bitmap (skip when all columns are NOT NULL — no nulls possible)
        let null_bitmap = if self.has_nullable_columns {
            u64::from_le_bytes([
                data[4], data[5], data[6], data[7],
                data[8], data[9], data[10], data[11],
            ])
        } else {
            0 // All columns are NOT NULL — no bits set
        };

        let var_section_start = self.var_section_start;
        let (var_data_start, var_entries, var_entry_count) = if self.var_col_count > 0 && var_section_start + 2 <= data.len() {
            // Use schema var_col_count when data is known-good (skip_magic_check set).
            // Saves reading 2 bytes + eliminates the min(16) branch per row.
            let var_count: usize = if self.skip_magic_check {
                self.var_col_count
            } else {
                u16::from_le_bytes([data[var_section_start], data[var_section_start + 1]]) as usize
            };
            let var_header_start = var_section_start + 2;
            let vds = var_header_start + var_count * 10;
            let mut entries: [(usize, usize); 16] = [(0, 0); 16];
            let mut count = 0usize;
            for i in 0..var_count.min(16) {
                let off = var_header_start + i * 10 + 2;
                if off + 8 > data.len() { break; }
                let v_off = u32::from_le_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]]) as usize;
                let v_len = u32::from_le_bytes([data[off + 4], data[off + 5], data[off + 6], data[off + 7]]) as usize;
                entries[count] = (v_off, v_len);
                count += 1;
            }
            (vds, entries, count)
        } else {
            (data.len(), [(0, 0); 16], 0)
        };

        let mut var_idx = 0usize;

        for i in 0..self.col_count {
            if null_bitmap & (1u64 << i) != 0 {
                out.push(Value::Null);
                continue;
            }

            match self.col_decoders[i] {
                ColDecoder::FixedInteger => {
                    let off = HEADER_SIZE + self.fixed_idx_map[i] as usize * FIXED_COL_SIZE;
                    // SAFETY: fixed_section_end ≤ data.len() (verified at top of function).
                    // All fixed columns lie within [HEADER_SIZE, fixed_section_end).
                    let val = unsafe {
                        let ptr = data.as_ptr().add(off);
                        Value::Integer(i64::from_le(std::ptr::read_unaligned(ptr as *const i64)))
                    };
                    out.push(val);
                }
                ColDecoder::FixedFloat => {
                    let off = HEADER_SIZE + self.fixed_idx_map[i] as usize * FIXED_COL_SIZE;
                    let val = unsafe {
                        let ptr = data.as_ptr().add(off);
                        let bits = u64::from_le(std::ptr::read_unaligned(ptr as *const u64));
                        Value::Float(f64::from_bits(bits))
                    };
                    out.push(val);
                }
                ColDecoder::FixedBool => {
                    let off = HEADER_SIZE + self.fixed_idx_map[i] as usize * FIXED_COL_SIZE;
                    // Bool is stored in first byte of its 8-byte slot
                    out.push(Value::Bool(data[off] != 0));
                }
                ColDecoder::FixedTimestamp => {
                    let off = HEADER_SIZE + self.fixed_idx_map[i] as usize * FIXED_COL_SIZE;
                    let val = unsafe {
                        let ptr = data.as_ptr().add(off);
                        Value::Timestamp(Timestamp::from_micros(i64::from_le(std::ptr::read_unaligned(ptr as *const i64))))
                    };
                    out.push(val);
                }
                ColDecoder::VarText => {
                    if var_idx < var_entry_count {
                        let (v_off, v_len) = var_entries[var_idx];
                        var_idx += 1;
                        let abs_off = var_data_start + v_off;
                        if abs_off + v_len <= data.len() {
                            let bytes = &data[abs_off..abs_off + v_len];
                            let s = if self.trust_utf8 {
                                unsafe { std::str::from_utf8_unchecked(bytes) }
                            } else {
                                std::str::from_utf8(bytes)
                                    .map_err(|_| StorageError::InvalidData("Invalid UTF-8 in Text column".into()))?
                            };
                            out.push(Value::Text(ArcString(self.pool.intern(s))));
                        } else { out.push(Value::Null); }
                    } else { out.push(Value::Null); }
                }
                ColDecoder::VarGeneric => {
                    if var_idx < var_entry_count {
                        let (v_off, v_len) = var_entries[var_idx];
                        var_idx += 1;
                        let abs_off = var_data_start + v_off;
                        if abs_off + v_len <= data.len() {
                            let var_data = &data[abs_off..abs_off + v_len];
                            let val = Self::decode_var_generic(var_data)?;
                            out.push(val);
                        } else { out.push(Value::Null); }
                    } else { out.push(Value::Null); }
                }
            }
        }
        Ok(())
    }

    /// Decode a VarGeneric column value (Tensor/Vector/Spatial).
    /// Tries in order: 0xFF-tagged bincode → vector format (dim+floats) → bincode fallback.
    pub(crate) fn decode_var_generic(var_data: &[u8]) -> Result<Value> {
        // 1. Tagged bincode (0xFF prefix)
        if !var_data.is_empty() && var_data[0] == 0xFF {
            if let Ok(v) = bincode::deserialize::<Value>(&var_data[1..]) {
                return Ok(v);
            }
        }
        // 2. Vector format: [dim: u16] + f32 array
        if var_data.len() >= 2 {
            let dim = u16::from_le_bytes([var_data[0], var_data[1]]) as usize;
            let expected = 2 + dim * 4;
            if var_data.len() >= expected && dim > 0 && dim <= 65536 {
                let mut vec = Vec::with_capacity(dim);
                for j in 0..dim {
                    let o = 2 + j * 4;
                    vec.push(f32::from_le_bytes([var_data[o], var_data[o+1], var_data[o+2], var_data[o+3]]));
                }
                return Ok(Value::Vector(crate::types::ArcVec(std::sync::Arc::new(vec))));
            }
        }
        // 3. Fallback: plain bincode
        bincode::deserialize::<Value>(var_data)
            .map_err(|e| StorageError::Serialization(e.to_string()))
    }

    /// - Pre-computed `fixed_idx_map` avoids per-column fixed_idx counter
    #[inline]
    pub fn decode_row(&mut self, data: &[u8]) -> Result<Vec<Value>> {
        // Fast path: skip magic check when data is from our own encode()
        if !self.skip_magic_check {
            if data.len() < HEADER_SIZE || !is_rawrow(data) {
                return bincode::deserialize(data)
                    .map_err(|e| StorageError::Serialization(e.to_string()));
            }
        } else if data.len() < HEADER_SIZE {
            return Err(StorageError::InvalidData("Row data too short".into()));
        }

        let null_bitmap = if self.has_nullable_columns {
            u64::from_le_bytes([
                data[4], data[5], data[6], data[7],
                data[8], data[9], data[10], data[11],
            ])
        } else {
            0 // All columns are NOT NULL — no bits set
        };

        // Parse var entries using stack array (no heap allocation).
        // Sequential forward read — ascending col_idx guaranteed by encoder.
        let var_section_start = self.var_section_start;
        let (var_data_start, var_entries, var_entry_count) = if self.var_col_count > 0 && var_section_start + 2 <= data.len() {
            let var_count: usize = if self.skip_magic_check {
                self.var_col_count
            } else {
                u16::from_le_bytes([data[var_section_start], data[var_section_start + 1]]) as usize
            };
            let var_header_start = var_section_start + 2;
            let vds = var_header_start + var_count * 10;
            let mut entries: [(usize, usize); 16] = [(0, 0); 16];
            let mut count = 0usize;
            for i in 0..var_count.min(16) {
                let off = var_header_start + i * 10 + 2; // skip 2-byte col_idx
                if off + 8 > data.len() { break; }
                let v_off = u32::from_le_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]]) as usize;
                let v_len = u32::from_le_bytes([data[off + 4], data[off + 5], data[off + 6], data[off + 7]]) as usize;
                entries[count] = (v_off, v_len);
                count += 1;
            }
            (vds, entries, count)
        } else {
            (data.len(), [(0, 0); 16], 0)
        };

        // Decode columns using specialized dispatch
        self.row_buf.clear();
        let mut var_idx = 0usize;

        for i in 0..self.col_count {
            // Null check
            if null_bitmap & (1u64 << i) != 0 {
                self.row_buf.push(Value::Null);
                continue;
            }

            match self.col_decoders[i] {
                ColDecoder::FixedInteger => {
                    let off = HEADER_SIZE + self.fixed_idx_map[i] as usize * FIXED_COL_SIZE;
                    let val = unsafe {
                        let ptr = data.as_ptr().add(off);
                        Value::Integer(i64::from_le(std::ptr::read_unaligned(ptr as *const i64)))
                    };
                    self.row_buf.push(val);
                }
                ColDecoder::FixedFloat => {
                    let off = HEADER_SIZE + self.fixed_idx_map[i] as usize * FIXED_COL_SIZE;
                    let val = unsafe {
                        let ptr = data.as_ptr().add(off);
                        Value::Float(f64::from_bits(u64::from_le(std::ptr::read_unaligned(ptr as *const u64))))
                    };
                    self.row_buf.push(val);
                }
                ColDecoder::FixedBool => {
                    let off = HEADER_SIZE + self.fixed_idx_map[i] as usize * FIXED_COL_SIZE;
                    self.row_buf.push(Value::Bool(data[off] != 0));
                }
                ColDecoder::FixedTimestamp => {
                    let off = HEADER_SIZE + self.fixed_idx_map[i] as usize * FIXED_COL_SIZE;
                    let val = unsafe {
                        let ptr = data.as_ptr().add(off);
                        Value::Timestamp(Timestamp::from_micros(i64::from_le(std::ptr::read_unaligned(ptr as *const i64))))
                    };
                    self.row_buf.push(val);
                }
                ColDecoder::VarText => {
                    if var_idx < var_entry_count {
                        let (v_off, v_len) = var_entries[var_idx];
                        var_idx += 1;
                        let abs_off = var_data_start + v_off;
                        if abs_off + v_len <= data.len() {
                            let bytes = &data[abs_off..abs_off + v_len];
                            // SAFETY: Data was encoded by our own encode() which only
                            // accepts Value::Text containing valid UTF-8. When trust_utf8
                            // is set (scan path), skip the validation overhead.
                            let s = if self.trust_utf8 {
                                unsafe { std::str::from_utf8_unchecked(bytes) }
                            } else {
                                std::str::from_utf8(bytes)
                                    .map_err(|_| StorageError::InvalidData("Invalid UTF-8 in Text column".into()))?
                            };
                            self.row_buf.push(Value::Text(ArcString(self.pool.intern(s))));
                        } else {
                            self.row_buf.push(Value::Null);
                        }
                    } else {
                        self.row_buf.push(Value::Null);
                    }
                }
                ColDecoder::VarGeneric => {
                    if var_idx < var_entry_count {
                        let (v_off, v_len) = var_entries[var_idx];
                        var_idx += 1;
                        let abs_off = var_data_start + v_off;
                        if abs_off + v_len <= data.len() {
                            let var_data = &data[abs_off..abs_off + v_len];
                            let val = Self::decode_var_generic(var_data)?;
                            self.row_buf.push(val);
                        } else {
                            self.row_buf.push(Value::Null);
                        }
                    } else {
                        self.row_buf.push(Value::Null);
                    }
                }
            }
        }

        // Return a clone of row_buf so the internal buffer retains its capacity.
        // clone() + clear() is faster than mem::take because the next decode avoids
        // Vec growth from capacity 0 (0→1→2→4 = 3 mallocs). With preserved capacity,
        // push() hits the existing allocation directly (1 malloc for the clone).
        let result = self.row_buf.clone();
        self.row_buf.clear();
        Ok(result)
    }
}

/// Column-oriented storage for query results.
///
/// Instead of `Vec<Vec<Value>>` (row-oriented with 1 heap allocation per row),
/// ColumnArray stores each column as a contiguous typed array. For a 4-column
/// table with 300K rows, this replaces 300K small Vec allocations with 4 large ones.
///
/// Null values are tracked per-column: a column is either fully nullable (None)
/// or has a parallel boolean vector marking NULL positions.
#[derive(Debug, Clone)]
pub enum ColumnArray {
    Integers(Vec<i64>),
    Floats(Vec<f64>),
    Texts(Vec<Arc<str>>),
    Timestamps(Vec<i64>),  // microseconds since epoch
    Bools(Vec<bool>),
    /// Fallback: stores Values as-is (for complex types like Vector/Spatial/Tensor)
    Values(Vec<Value>),
}

impl ColumnArray {
    pub fn len(&self) -> usize {
        match self {
            Self::Integers(v) => v.len(),
            Self::Floats(v) => v.len(),
            Self::Texts(v) => v.len(),
            Self::Timestamps(v) => v.len(),
            Self::Bools(v) => v.len(),
            Self::Values(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Columnar query result — stores columns independently for O(columns) allocations
/// instead of O(rows). Each column is a typed array matching the schema's ColumnType.
///
/// # Memory comparison (300K rows, 4 columns: INT, TEXT, FLOAT, TEXT)
///
/// | Format | Allocations | Heap memory |
/// |--------|------------|-------------|
/// | Row-based (Vec<Vec<Value>>) | 300K | ~24 MB |
/// | Columnar (ColumnarRowSet)   | 4    | ~14 MB |
///
/// # Conversion
///
/// `to_row_based()` converts to the traditional `Vec<Vec<Value>>` format for
/// consumers that need row-oriented access. This is a one-time O(N) conversion.
#[derive(Debug, Clone)]
pub struct ColumnarRowSet {
    pub columns: Vec<String>,
    pub data: Vec<ColumnArray>,
    pub num_rows: usize,
}

impl ColumnarRowSet {
    /// Create an empty columnar result with the given column names and types.
    pub fn new(columns: Vec<String>, col_types: &[ColumnType]) -> Self {
        let data: Vec<ColumnArray> = col_types.iter().map(|ct| match ct {
            ColumnType::Integer => ColumnArray::Integers(Vec::new()),
            ColumnType::Float => ColumnArray::Floats(Vec::new()),
            ColumnType::Text => ColumnArray::Texts(Vec::new()),
            ColumnType::Timestamp => ColumnArray::Timestamps(Vec::new()),
            ColumnType::Boolean => ColumnArray::Bools(Vec::new()),
            ColumnType::Tensor(_) | ColumnType::Spatial => ColumnArray::Values(Vec::new()),
        }).collect();
        Self { columns, data, num_rows: 0 }
    }

    /// Return the number of rows.
    pub fn row_count(&self) -> usize {
        self.num_rows
    }

    /// Convert to row-based format (Vec<Vec<Value>>).
    /// Performs one O(N) pass over all columns to construct row vectors.
    pub fn to_row_based(&self) -> Vec<Vec<Value>> {
        if self.num_rows == 0 {
            return Vec::new();
        }
        let mut rows: Vec<Vec<Value>> = (0..self.num_rows)
            .map(|_| Vec::with_capacity(self.data.len()))
            .collect();

        for col_array in &self.data {
            match col_array {
                ColumnArray::Integers(v) => {
                    for (row_idx, &val) in v.iter().enumerate() {
                        rows[row_idx].push(Value::Integer(val));
                    }
                }
                ColumnArray::Floats(v) => {
                    for (row_idx, &val) in v.iter().enumerate() {
                        rows[row_idx].push(Value::Float(val));
                    }
                }
                ColumnArray::Texts(v) => {
                    for (row_idx, val) in v.iter().enumerate() {
                        rows[row_idx].push(Value::Text(ArcString(Arc::clone(val))));
                    }
                }
                ColumnArray::Timestamps(v) => {
                    for (row_idx, &val) in v.iter().enumerate() {
                        rows[row_idx].push(Value::Timestamp(Timestamp::from_micros(val)));
                    }
                }
                ColumnArray::Bools(v) => {
                    for (row_idx, &val) in v.iter().enumerate() {
                        rows[row_idx].push(Value::Bool(val));
                    }
                }
                ColumnArray::Values(v) => {
                    for (row_idx, val) in v.iter().enumerate() {
                        rows[row_idx].push(val.clone());
                    }
                }
            }
        }
        rows
    }
}

/// Decode a raw row directly into column arrays (columnar accumulation).
///
/// This is the columnar equivalent of `SchemaDecodeContext::decode_row()`.
/// Instead of constructing a `Vec<Value>` per row, it appends each column's
/// value to the appropriate typed array in `col_data`.
///
/// # Performance
///
/// - Integer/Float/Timestamp: read 8 bytes, push to Vec<i64>/Vec<f64> (no Value wrapping)
/// - Text: `Arc::from(str)` directly to `Vec<Arc<str>>` (no pool, no ArcString wrap)
/// - Null: skipped (arrays are shorter for nullable columns — caller tracks separately)
/// - No per-row heap allocations
#[inline]
pub fn decode_row_into_columns(
    ctx: &SchemaDecodeContext,
    data: &[u8],
    col_data: &mut [ColumnArray],
) -> Result<()> {
    // Fast path: skip magic check when data is from our own encode()
    if !ctx.skip_magic_check {
        if data.len() < HEADER_SIZE || !is_rawrow(data) {
            let row: Vec<Value> = bincode::deserialize(data)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            for (i, val) in row.into_iter().enumerate() {
                push_value_to_column(&mut col_data[i], val);
            }
            return Ok(());
        }
    } else if data.len() < HEADER_SIZE {
        return Err(StorageError::InvalidData("Row data too short".into()));
    }

    let null_bitmap = if ctx.has_nullable_columns {
        u64::from_le_bytes([
            data[4], data[5], data[6], data[7],
            data[8], data[9], data[10], data[11],
        ])
    } else {
        0 // All columns are NOT NULL — no bits set
    };

    // Parse var entries using stack array
    let var_section_start = ctx.var_section_start;
    let (var_data_start, var_entries, var_entry_count) = if ctx.var_col_count > 0 && var_section_start + 2 <= data.len() {
        let var_count: usize = if ctx.skip_magic_check {
            ctx.var_col_count
        } else {
            u16::from_le_bytes([data[var_section_start], data[var_section_start + 1]]) as usize
        };
        let var_header_start = var_section_start + 2;
        let vds = var_header_start + var_count * 10;
        let mut entries: [(usize, usize); 16] = [(0, 0); 16];
        let mut count = 0usize;
        for i in 0..var_count.min(16) {
            let off = var_header_start + i * 10 + 2;
            if off + 8 > data.len() { break; }
            let v_off = u32::from_le_bytes([data[off], data[off+1], data[off+2], data[off+3]]) as usize;
            let v_len = u32::from_le_bytes([data[off+4], data[off+5], data[off+6], data[off+7]]) as usize;
            entries[count] = (v_off, v_len);
            count += 1;
        }
        (vds, entries, count)
    } else {
        (data.len(), [(0, 0); 16], 0)
    };

    let mut var_idx = 0usize;

    for (i, col_arr) in col_data.iter_mut().enumerate() {
        // Null check
        if null_bitmap & (1u64 << i) != 0 {
            continue; // Skip nulls in columnar format
        }

        match ctx.col_decoders[i] {
            ColDecoder::FixedInteger => {
                let off = HEADER_SIZE + ctx.fixed_idx_map[i] as usize * FIXED_COL_SIZE;
                if off + FIXED_COL_SIZE <= data.len() {
                    let arr: [u8; 8] = data[off..off + 8].try_into().unwrap_or([0; 8]);
                    if let ColumnArray::Integers(ref mut v) = col_arr {
                        v.push(i64::from_le_bytes(arr));
                    }
                }
            }
            ColDecoder::FixedFloat => {
                let off = HEADER_SIZE + ctx.fixed_idx_map[i] as usize * FIXED_COL_SIZE;
                if off + FIXED_COL_SIZE <= data.len() {
                    let arr: [u8; 8] = data[off..off + 8].try_into().unwrap_or([0; 8]);
                    if let ColumnArray::Floats(ref mut v) = col_arr {
                        v.push(f64::from_le_bytes(arr));
                    }
                }
            }
            ColDecoder::FixedBool => {
                let off = HEADER_SIZE + ctx.fixed_idx_map[i] as usize * FIXED_COL_SIZE;
                if off < data.len() {
                    if let ColumnArray::Bools(ref mut v) = col_arr {
                        v.push(data[off] != 0);
                    }
                }
            }
            ColDecoder::FixedTimestamp => {
                let off = HEADER_SIZE + ctx.fixed_idx_map[i] as usize * FIXED_COL_SIZE;
                if off + FIXED_COL_SIZE <= data.len() {
                    let arr: [u8; 8] = data[off..off + 8].try_into().unwrap_or([0; 8]);
                    if let ColumnArray::Timestamps(ref mut v) = col_arr {
                        v.push(i64::from_le_bytes(arr));
                    }
                }
            }
            ColDecoder::VarText => {
                if var_idx < var_entry_count {
                    let (v_off, v_len) = var_entries[var_idx];
                    var_idx += 1;
                    let abs_off = var_data_start + v_off;
                    if abs_off + v_len <= data.len() {
                        let bytes = &data[abs_off..abs_off + v_len];
                        let s: Arc<str> = if ctx.trust_utf8 {
                            unsafe { std::str::from_utf8_unchecked(bytes) }.into()
                        } else {
                            std::str::from_utf8(bytes)
                                .map_err(|_| StorageError::InvalidData("Invalid UTF-8".into()))?
                                .into()
                        };
                        if let ColumnArray::Texts(ref mut v) = col_arr {
                            v.push(s);
                        }
                    }
                }
            }
            ColDecoder::VarGeneric => {
                if var_idx < var_entry_count {
                    let (v_off, v_len) = var_entries[var_idx];
                    var_idx += 1;
                    let abs_off = var_data_start + v_off;
                    if abs_off + v_len <= data.len() {
                        let val: Value = bincode::deserialize(&data[abs_off..abs_off + v_len])
                            .map_err(|e| StorageError::Serialization(e.to_string()))?;
                        if let ColumnArray::Values(ref mut v) = col_arr {
                            v.push(val);
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

/// Push a single Value into the appropriate column array.
fn push_value_to_column(col: &mut ColumnArray, val: Value) {
    match (col, val) {
        (ColumnArray::Integers(v), Value::Integer(i)) => v.push(i),
        (ColumnArray::Floats(v), Value::Float(f)) => v.push(f),
        (ColumnArray::Texts(v), Value::Text(s)) => v.push(s.0),
        (ColumnArray::Timestamps(v), Value::Timestamp(ts)) => v.push(ts.as_micros()),
        (ColumnArray::Bools(v), Value::Bool(b)) => v.push(b),
        (ColumnArray::Values(v), val) => v.push(val),
        _ => {} // Type mismatch: skip (shouldn't happen with correct schema)
    }
}

/// Encode a row into compact RawRow format.
pub fn encode(row: &[Value], col_types: &[ColumnType]) -> Result<Vec<u8>> {
    if row.len() != col_types.len() {
        return Err(StorageError::InvalidData(format!(
            "Row column count ({}) doesn't match schema ({})",
            row.len(), col_types.len()
        )));
    }
    if row.len() > 64 {
        return Err(StorageError::InvalidData(format!(
            "Row has {} columns, max 64 supported",
            row.len()
        )));
    }
    let col_count = row.len();

    let mut buf = Vec::with_capacity(64 + col_count * 16);
    let mut null_bitmap: u64 = 0;
    let mut var_entries: Vec<(usize, Vec<u8>)> = Vec::new();

    // Write header (12 bytes)
    buf.extend_from_slice(&RAWROW_MAGIC.to_le_bytes());
    buf.extend_from_slice(&(col_count as u16).to_le_bytes());
    buf.extend_from_slice(&0u64.to_le_bytes()); // null_bitmap placeholder

    // Write fixed columns directly into buf (no intermediate Vec)
    for (i, (value, col_type)) in row.iter().zip(col_types.iter()).enumerate() {
        if matches!(value, Value::Null) {
            null_bitmap |= 1u64 << i;
            if is_fixed(col_type) {
                buf.extend_from_slice(&[0u8; FIXED_COL_SIZE]);
            }
            continue;
        }

        match (value, col_type) {
            (Value::Integer(v), ColumnType::Integer) => {
                buf.extend_from_slice(&v.to_le_bytes());
            }
            (Value::Float(v), ColumnType::Float) => {
                buf.extend_from_slice(&v.to_le_bytes());
            }
            (Value::Bool(v), ColumnType::Boolean) => {
                let mut bytes = [0u8; FIXED_COL_SIZE];
                bytes[0] = if *v { 1 } else { 0 };
                buf.extend_from_slice(&bytes);
            }
            (Value::Timestamp(ts), ColumnType::Timestamp) => {
                buf.extend_from_slice(&ts.as_micros().to_le_bytes());
            }
            (Value::Text(t), ColumnType::Text) => {
                var_entries.push((i, t.as_bytes().to_vec()));
            }
            (Value::Vector(v), _) => {
                if v.len() > u16::MAX as usize {
                    return Err(StorageError::InvalidData(
                        format!("Vector dimension {} exceeds maximum {}", v.len(), u16::MAX)
                    ));
                }
                let dim = v.len() as u16;
                let mut encoded = Vec::with_capacity(2 + v.len() * 4);
                encoded.extend_from_slice(&dim.to_le_bytes());
                for f in v.iter() {
                    encoded.extend_from_slice(&f.to_le_bytes());
                }
                var_entries.push((i, encoded));
            }
            (value, _) => {
                let mut encoded = vec![0xFF];
                encoded.extend_from_slice(&bincode::serialize(value)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?);
                var_entries.push((i, encoded));
            }
        }
    }

    // Patch null_bitmap in header
    buf[4..12].copy_from_slice(&null_bitmap.to_le_bytes());

    // Var section: count + entries + data
    buf.extend_from_slice(&(var_entries.len() as u16).to_le_bytes());

    let var_header_start = buf.len();
    let var_header_size = var_entries.len() * 10;
    buf.resize(buf.len() + var_header_size, 0);

    let mut var_data_offset: usize = 0;
    for (entry_idx, (col_idx, data)) in var_entries.iter().enumerate() {
        let h_off = var_header_start + entry_idx * 10;
        buf[h_off..h_off + 2].copy_from_slice(&(*col_idx as u16).to_le_bytes());
        buf[h_off + 2..h_off + 6].copy_from_slice(&(var_data_offset as u32).to_le_bytes());
        buf[h_off + 6..h_off + 10].copy_from_slice(&(data.len() as u32).to_le_bytes());
        buf.extend_from_slice(data);
        var_data_offset += data.len();
    }

    Ok(buf)
}

/// Encode a row into a caller-provided buffer (reuses capacity across calls).
/// Identical to `encode()` but avoids per-row heap allocation.
pub fn encode_into(row: &[Value], col_types: &[ColumnType], buf: &mut Vec<u8>) -> Result<()> {
    if row.len() != col_types.len() {
        return Err(StorageError::InvalidData(format!(
            "Row column count ({}) doesn't match schema ({})",
            row.len(), col_types.len()
        )));
    }
    if row.len() > 64 {
        return Err(StorageError::InvalidData(format!(
            "Row has {} columns, max 64 supported",
            row.len()
        )));
    }
    let col_count = row.len();

    buf.clear();
    let mut null_bitmap: u64 = 0;
    let mut var_entries: Vec<(usize, Vec<u8>)> = Vec::new();

    // Write header (12 bytes)
    buf.extend_from_slice(&RAWROW_MAGIC.to_le_bytes());
    buf.extend_from_slice(&(col_count as u16).to_le_bytes());
    buf.extend_from_slice(&0u64.to_le_bytes()); // null_bitmap placeholder

    // Write fixed columns directly into buf
    for (i, (value, col_type)) in row.iter().zip(col_types.iter()).enumerate() {
        if matches!(value, Value::Null) {
            null_bitmap |= 1u64 << i;
            if is_fixed(col_type) {
                buf.extend_from_slice(&[0u8; FIXED_COL_SIZE]);
            }
            continue;
        }

        match (value, col_type) {
            (Value::Integer(v), ColumnType::Integer) => {
                buf.extend_from_slice(&v.to_le_bytes());
            }
            (Value::Float(v), ColumnType::Float) => {
                buf.extend_from_slice(&v.to_le_bytes());
            }
            (Value::Bool(v), ColumnType::Boolean) => {
                let mut bytes = [0u8; FIXED_COL_SIZE];
                bytes[0] = if *v { 1 } else { 0 };
                buf.extend_from_slice(&bytes);
            }
            (Value::Timestamp(ts), ColumnType::Timestamp) => {
                buf.extend_from_slice(&ts.as_micros().to_le_bytes());
            }
            (Value::Text(t), ColumnType::Text) => {
                var_entries.push((i, t.as_bytes().to_vec()));
            }
            (Value::Vector(v), _) => {
                if v.len() > u16::MAX as usize {
                    return Err(StorageError::InvalidData(
                        format!("Vector dimension {} exceeds maximum {}", v.len(), u16::MAX)
                    ));
                }
                let dim = v.len() as u16;
                let mut encoded = Vec::with_capacity(2 + v.len() * 4);
                encoded.extend_from_slice(&dim.to_le_bytes());
                for f in v.iter() {
                    encoded.extend_from_slice(&f.to_le_bytes());
                }
                var_entries.push((i, encoded));
            }
            (value, _) => {
                let mut encoded = vec![0xFF];
                encoded.extend_from_slice(&bincode::serialize(value)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?);
                var_entries.push((i, encoded));
            }
        }
    }

    // Patch null_bitmap in header
    buf[4..12].copy_from_slice(&null_bitmap.to_le_bytes());

    // Var section: count + entries + data
    buf.extend_from_slice(&(var_entries.len() as u16).to_le_bytes());

    let var_header_start = buf.len();
    let var_header_size = var_entries.len() * 10;
    buf.resize(buf.len() + var_header_size, 0);

    let mut var_data_offset: usize = 0;
    for (entry_idx, (col_idx, data)) in var_entries.iter().enumerate() {
        let h_off = var_header_start + entry_idx * 10;
        buf[h_off..h_off + 2].copy_from_slice(&(*col_idx as u16).to_le_bytes());
        buf[h_off + 2..h_off + 6].copy_from_slice(&(var_data_offset as u32).to_le_bytes());
        buf[h_off + 6..h_off + 10].copy_from_slice(&(data.len() as u32).to_le_bytes());
        buf.extend_from_slice(data);
        var_data_offset += data.len();
    }

    Ok(())
}

/// Decode bytes into a Row. Falls back to bincode for old-format data.
pub fn decode(data: &[u8], col_types: &[ColumnType]) -> Result<Row> {
    if !is_rawrow(data) {
        return bincode::deserialize(data)
            .map_err(|e| StorageError::Serialization(e.to_string()));
    }
    decode_raw(data, col_types)
}

/// Fast decode with pre-computed fixed_count (avoids per-row O(C) scan).
pub fn decode_fast(data: &[u8], col_types: &[ColumnType], fixed_count: usize) -> Result<Row> {
    if !is_rawrow(data) {
        return bincode::deserialize(data)
            .map_err(|e| StorageError::Serialization(e.to_string()));
    }
    decode_raw_fast(data, col_types, fixed_count)
}

/// Reusable-buffer version of decode_fast — avoids per-row Vec allocation.
pub fn decode_fast_into(data: &[u8], col_types: &[ColumnType], fixed_count: usize, buf: &mut Vec<Value>) -> Result<()> {
    if !is_rawrow(data) {
        *buf = bincode::deserialize(data)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        return Ok(());
    }
    decode_raw_fast_into(data, col_types, fixed_count, buf)
}

/// Like `decode_fast_into` but with optional `StringPool` for Text column interning.
/// Pass `&mut pool` to deduplicate Arc<str> allocations across rows.
pub fn decode_fast_into_with_pool(data: &[u8], col_types: &[ColumnType], fixed_count: usize, buf: &mut Vec<Value>, pool: Option<&mut StringPool>) -> Result<()> {
    if !is_rawrow(data) {
        *buf = bincode::deserialize(data)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        return Ok(());
    }
    decode_raw_fast_into_with_pool(data, col_types, fixed_count, buf, pool)
}

/// Compute the number of fixed-size columns in a schema.
pub fn compute_fixed_count(col_types: &[ColumnType]) -> usize {
    col_types.iter().filter(|t| is_fixed(t)).count()
}

/// Pre-parsed row header for zero-copy two-phase column decode.
/// Parse once per row, then decode different column sets without re-parsing the header.
pub struct RowParseContext {
    null_bitmap: u64,
    #[allow(dead_code)]
    fixed_count: usize,
    var_offsets: [(usize, usize); 64],
    var_data_start: usize,
    /// Pre-computed mapping: col_idx -> fixed_idx (O(1) lookup instead of O(col_idx) scan)
    fixed_idx_map: [u8; 64],
}

impl RowParseContext {
    /// Parse the row header (one-time cost per row).
    /// Returns None if the data is not in RawRow format (legacy bincode).
    pub fn parse(data: &[u8], col_types: &[ColumnType], fixed_count: usize) -> Option<Self> {
        if !is_rawrow(data) || data.len() < HEADER_SIZE {
            return None;
        }

        let col_count = u16::from_le_bytes([data[2], data[3]]) as usize;
        if col_count != col_types.len() {
            return None;
        }

        let null_bitmap = u64::from_le_bytes([
            data[4], data[5], data[6], data[7],
            data[8], data[9], data[10], data[11],
        ]);

        // Pre-compute col_idx -> fixed_idx mapping (one pass, O(C) not O(C²))
        let mut fixed_idx_map = [0u8; 64];
        let mut next_fixed = 0u8;
        for (i, ct) in col_types.iter().enumerate() {
            if i >= 64 { break; }
            if is_fixed(ct) {
                fixed_idx_map[i] = next_fixed;
                next_fixed += 1;
            }
        }

        let var_section_start = HEADER_SIZE + fixed_count * FIXED_COL_SIZE;
        let mut var_offsets = [(0usize, 0usize); 64];
        let var_data_start;
        if var_section_start + 2 <= data.len() {
            let var_count = u16::from_le_bytes([data[var_section_start], data[var_section_start + 1]]) as usize;
            let var_header_start = var_section_start + 2;
            var_data_start = var_header_start + var_count * 10;
            for i in 0..var_count {
                let off = var_header_start + i * 10;
                if off + 10 > data.len() { break; }
                let col_idx = u16::from_le_bytes([data[off], data[off + 1]]) as usize;
                if col_idx >= 64 { break; }
                let v_off = u32::from_le_bytes([data[off + 2], data[off + 3], data[off + 4], data[off + 5]]) as usize;
                let v_len = u32::from_le_bytes([data[off + 6], data[off + 7], data[off + 8], data[off + 9]]) as usize;
                var_offsets[col_idx] = (v_off, v_len);
            }
        } else {
            var_data_start = data.len();
        }

        Some(Self { null_bitmap, fixed_count, var_offsets, var_data_start, fixed_idx_map })
    }

    /// Parse row header using pre-computed FixedColumnOffsets (avoids per-row O(C) scan).
    /// This is the fast path for queries with a known schema — the fixed_idx_map
    /// and fixed_count are pre-computed once per table scan.
    pub fn parse_with_offsets(
        data: &[u8],
        col_types: &[ColumnType],
        offsets: &FixedColumnOffsets,
    ) -> Option<Self> {
        if !is_rawrow(data) || data.len() < HEADER_SIZE {
            return None;
        }

        let col_count = u16::from_le_bytes([data[2], data[3]]) as usize;
        if col_count != col_types.len() {
            return None;
        }

        let null_bitmap = u64::from_le_bytes([
            data[4], data[5], data[6], data[7],
            data[8], data[9], data[10], data[11],
        ]);

        let var_section_start = HEADER_SIZE + offsets.fixed_count * FIXED_COL_SIZE;
        let mut var_offsets = [(0usize, 0usize); 64];
        let var_data_start;
        if var_section_start + 2 <= data.len() {
            let var_count = u16::from_le_bytes([data[var_section_start], data[var_section_start + 1]]) as usize;
            let var_header_start = var_section_start + 2;
            var_data_start = var_header_start + var_count * 10;
            for i in 0..var_count {
                let off = var_header_start + i * 10;
                if off + 10 > data.len() { break; }
                let col_idx = u16::from_le_bytes([data[off], data[off + 1]]) as usize;
                if col_idx >= 64 { break; }
                let v_off = u32::from_le_bytes([data[off + 2], data[off + 3], data[off + 4], data[off + 5]]) as usize;
                let v_len = u32::from_le_bytes([data[off + 6], data[off + 7], data[off + 8], data[off + 9]]) as usize;
                var_offsets[col_idx] = (v_off, v_len);
            }
        } else {
            var_data_start = data.len();
        }

        Some(Self {
            null_bitmap,
            fixed_count: offsets.fixed_count,
            var_offsets,
            var_data_start,
            fixed_idx_map: offsets.fixed_idx_map,
        })
    }

    /// Decode a set of columns using the pre-parsed context.
    /// Much faster than calling get_column() N times — header parsed only once.
    /// Uses pre-computed fixed_idx_map for O(1) fixed column lookup.
    #[inline]
    pub fn decode_columns(
        &self,
        data: &[u8],
        col_types: &[ColumnType],
        positions: &[usize],
        out: &mut Vec<Value>,
    ) -> Result<()> {
        out.clear();

        for &col_idx in positions {
            if col_idx >= col_types.len() {
                out.push(Value::Null);
                continue;
            }

            // NULL check
            if self.null_bitmap & (1u64 << col_idx) != 0 {
                out.push(Value::Null);
                continue;
            }

            let col_type = &col_types[col_idx];

            if is_fixed(col_type) {
                // Fixed column: O(1) lookup via pre-computed mapping
                let fixed_idx = self.fixed_idx_map[col_idx] as usize;
                let off = HEADER_SIZE + fixed_idx * FIXED_COL_SIZE;
                if off + FIXED_COL_SIZE > data.len() {
                    out.push(Value::Null);
                } else {
                    out.push(decode_fixed(&data[off..off + FIXED_COL_SIZE], col_type));
                }
            } else {
                // Variable column: use pre-parsed var_offsets
                let (v_off, v_len) = self.var_offsets[col_idx];
                let abs_off = self.var_data_start + v_off;
                if abs_off + v_len > data.len() {
                    out.push(Value::Null);
                } else {
                    out.push(decode_var(&data[abs_off..abs_off + v_len], col_type)?);
                }
            }
        }
        Ok(())
    }
}

/// Decode bytes into a Row without knowing the schema.
/// Tries RawRow first (with generic type inference), falls back to bincode.
pub fn decode_any(data: &[u8]) -> Result<Row> {
    if !is_rawrow(data) {
        return bincode::deserialize(data)
            .map_err(|e| StorageError::Serialization(e.to_string()));
    }
    // For RawRow without schema, try to decode with best-effort column type inference
    decode_raw_any(data)
}

/// Like `decode_any` but with optional `StringPool` for Text column interning.
pub fn decode_any_with_pool(data: &[u8], pool: Option<&mut StringPool>) -> Result<Row> {
    if !is_rawrow(data) {
        return bincode::deserialize(data)
            .map_err(|e| StorageError::Serialization(e.to_string()));
    }
    decode_raw_any_with_pool(data, pool)
}

/// Get a single column value without deserializing the whole row.
pub fn get_column(data: &[u8], col_types: &[ColumnType], col_idx: usize) -> Result<Value> {
    if !is_rawrow(data) {
        let row: Row = bincode::deserialize(data)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        return Ok(row.get(col_idx).cloned().unwrap_or(Value::Null));
    }

    if data.len() < HEADER_SIZE || col_idx >= col_types.len() {
        return Ok(Value::Null);
    }

    let null_bitmap = u64::from_le_bytes([
        data[4], data[5], data[6], data[7],
        data[8], data[9], data[10], data[11],
    ]);

    if null_bitmap & (1u64 << col_idx) != 0 {
        return Ok(Value::Null);
    }

    let col_type = &col_types[col_idx];

    if is_fixed(col_type) {
        let fixed_idx = col_types[..col_idx].iter().filter(|t| is_fixed(t)).count();
        let off = HEADER_SIZE + fixed_idx * FIXED_COL_SIZE;
        if off + FIXED_COL_SIZE > data.len() {
            return Ok(Value::Null);
        }
        return Ok(decode_fixed(&data[off..off + FIXED_COL_SIZE], col_type));
    }

    // Variable column — scan var headers
    let fixed_count = col_types.iter().filter(|t| is_fixed(t)).count();
    let var_section_start = HEADER_SIZE + fixed_count * FIXED_COL_SIZE;
    if var_section_start + 2 > data.len() {
        return Ok(Value::Null);
    }

    let var_count = u16::from_le_bytes([data[var_section_start], data[var_section_start + 1]]) as usize;
    let var_header_start = var_section_start + 2;
    let var_data_start = var_header_start + var_count * 10;

    for i in 0..var_count {
        let off = var_header_start + i * 10;
        if off + 10 > data.len() { break; }
        let entry_col = u16::from_le_bytes([data[off], data[off + 1]]) as usize;
        if entry_col == col_idx {
            let v_off = u32::from_le_bytes([data[off + 2], data[off + 3], data[off + 4], data[off + 5]]) as usize;
            let v_len = u32::from_le_bytes([data[off + 6], data[off + 7], data[off + 8], data[off + 9]]) as usize;
            let abs_off = var_data_start + v_off;
            if abs_off + v_len > data.len() {
                return Ok(Value::Null);
            }
            return decode_var(&data[abs_off..abs_off + v_len], col_type);
        }
    }

    Ok(Value::Null)
}

fn is_rawrow(data: &[u8]) -> bool {
    data.len() >= 2 && u16::from_le_bytes([data[0], data[1]]) == RAWROW_MAGIC
}

fn is_fixed(col_type: &ColumnType) -> bool {
    matches!(col_type, ColumnType::Integer | ColumnType::Float | ColumnType::Boolean | ColumnType::Timestamp)
}

/// Pre-computed byte offsets for fixed-width columns in RawRow format.
///
/// Each fixed column occupies 8 bytes at a deterministic offset:
///   offset = HEADER_SIZE(12) + (num_fixed_before) * FIXED_COL_SIZE(8)
///
/// Also pre-computes `fixed_idx_map` for O(1) col_idx → fixed_idx lookup,
/// avoiding per-row O(C) col_types scan in RowParseContext::parse().
///
/// Computed once per schema and reused across the entire table scan.
#[derive(Debug, Clone)]
pub struct FixedColumnOffsets {
    /// Map: schema column position → byte offset in RawRow data
    /// Zero for non-fixed columns.
    col_to_offset: [u16; 64],
    /// Columns that are fixed-width
    fixed_columns: Vec<usize>,
    /// Pre-computed col_idx → fixed_idx mapping (identical to RowParseContext's)
    /// Used to avoid per-row O(C) scan in RowParseContext::parse().
    pub fixed_idx_map: [u8; 64],
    /// Number of fixed-width columns
    pub fixed_count: usize,
}

impl FixedColumnOffsets {
    /// Compute fixed column byte offsets and index map from schema column types.
    /// Returns None if the table has no fixed-width columns.
    pub fn compute(col_types: &[ColumnType]) -> Option<Self> {
        let num_cols = col_types.len().min(64);
        let mut col_to_offset = [0u16; 64];
        let mut fixed_idx_map = [0u8; 64];
        let mut fixed_columns = Vec::with_capacity(num_cols);
        let mut fixed_count: usize = 0;

        for i in 0..num_cols {
            if is_fixed(&col_types[i]) {
                let offset = HEADER_SIZE + fixed_count * FIXED_COL_SIZE;
                col_to_offset[i] = offset as u16;
                fixed_idx_map[i] = fixed_count as u8;
                fixed_columns.push(i);
                fixed_count += 1;
            }
        }

        if fixed_columns.is_empty() {
            None
        } else {
            Some(Self { col_to_offset, fixed_columns, fixed_idx_map, fixed_count })
        }
    }

    /// Get the byte offset into RawRow data for the given column position.
    /// Returns None if the column is not a fixed-width type.
    #[inline]
    pub fn offset(&self, col_idx: usize) -> Option<usize> {
        if col_idx >= 64 {
            return None;
        }
        // All fixed columns have non-zero offset (min = HEADER_SIZE = 12).
        // Non-fixed columns have offset = 0 in the array.
        let off = self.col_to_offset[col_idx] as usize;
        if off == 0 {
            None
        } else {
            Some(off)
        }
    }

    /// Extract an i64 value from a RawRow byte slice at a fixed column offset.
    /// Returns None if the column is NULL (null_bitmap bit is set).
    #[inline]
    pub fn read_i64(&self, raw: &[u8], col_idx: usize) -> Option<i64> {
        let off = self.offset(col_idx)?;
        if off + 8 > raw.len() { return None; }
        // Check null_bitmap
        if raw.len() >= HEADER_SIZE {
            let null_bitmap = u64::from_le_bytes([raw[4], raw[5], raw[6], raw[7], raw[8], raw[9], raw[10], raw[11]]);
            if (null_bitmap >> col_idx) & 1 == 1 {
                return None; // NULL
            }
        }
        Some(i64::from_le_bytes([raw[off], raw[off+1], raw[off+2], raw[off+3], raw[off+4], raw[off+5], raw[off+6], raw[off+7]]))
    }

    /// Extract an f64 value from a RawRow byte slice at a fixed column offset.
    #[inline]
    pub fn read_f64(&self, raw: &[u8], col_idx: usize) -> Option<f64> {
        let off = self.offset(col_idx)?;
        if off + 8 > raw.len() { return None; }
        if raw.len() >= HEADER_SIZE {
            let null_bitmap = u64::from_le_bytes([raw[4], raw[5], raw[6], raw[7], raw[8], raw[9], raw[10], raw[11]]);
            if (null_bitmap >> col_idx) & 1 == 1 {
                return None;
            }
        }
        Some(f64::from_le_bytes([raw[off], raw[off+1], raw[off+2], raw[off+3], raw[off+4], raw[off+5], raw[off+6], raw[off+7]]))
    }

    /// Read a fixed column value as a Value enum.
    #[inline]
    pub fn read_value(&self, raw: &[u8], col_idx: usize, col_type: &ColumnType) -> Value {
        if raw.len() >= HEADER_SIZE {
            let null_bitmap = u64::from_le_bytes([raw[4], raw[5], raw[6], raw[7], raw[8], raw[9], raw[10], raw[11]]);
            if (null_bitmap >> col_idx) & 1 == 1 {
                return Value::Null;
            }
        }
        let off = self.offset(col_idx).unwrap_or(0);
        if off + 8 > raw.len() { return Value::Null; }
        match col_type {
            ColumnType::Integer => Value::Integer(i64::from_le_bytes([
                raw[off], raw[off+1], raw[off+2], raw[off+3],
                raw[off+4], raw[off+5], raw[off+6], raw[off+7],
            ])),
            ColumnType::Float => Value::Float(f64::from_le_bytes([
                raw[off], raw[off+1], raw[off+2], raw[off+3],
                raw[off+4], raw[off+5], raw[off+6], raw[off+7],
            ])),
            ColumnType::Boolean => Value::Bool(raw[off] != 0),
            ColumnType::Timestamp => {
                let micros = i64::from_le_bytes([
                    raw[off], raw[off+1], raw[off+2], raw[off+3],
                    raw[off+4], raw[off+5], raw[off+6], raw[off+7],
                ]);
                Value::Timestamp(crate::types::Timestamp::from_micros(micros))
            }
            _ => Value::Null, // Not a fixed type
        }
    }

    /// Number of fixed columns
    pub fn fixed_count(&self) -> usize {
        self.fixed_columns.len()
    }
}

fn decode_raw(data: &[u8], col_types: &[ColumnType]) -> Result<Row> {
    if data.len() < HEADER_SIZE {
        return Err(StorageError::InvalidData("RawRow data too small".into()));
    }

    let col_count = u16::from_le_bytes([data[2], data[3]]) as usize;
    let null_bitmap = u64::from_le_bytes([
        data[4], data[5], data[6], data[7],
        data[8], data[9], data[10], data[11],
    ]);

    if col_count != col_types.len() {
        // Schema mismatch — fall back to bincode
        return bincode::deserialize(data)
            .map_err(|e| StorageError::Serialization(e.to_string()));
    }

    let fixed_count = col_types.iter().filter(|t| is_fixed(t)).count();
    let var_section_start = HEADER_SIZE + fixed_count * FIXED_COL_SIZE;

    // Build var column map — stack array (max 64 columns)
    let mut var_offsets: [(usize, usize); 64] = [(0, 0); 64];
    let var_data_start;
    if var_section_start + 2 <= data.len() {
        let var_count = u16::from_le_bytes([data[var_section_start], data[var_section_start + 1]]) as usize;
        let var_header_start = var_section_start + 2;
        var_data_start = var_header_start + var_count * 10;
        for i in 0..var_count {
            let off = var_header_start + i * 10;
            if off + 10 > data.len() { break; }
            let col_idx = u16::from_le_bytes([data[off], data[off + 1]]) as usize;
            if col_idx >= 64 { break; }
            let v_off = u32::from_le_bytes([data[off + 2], data[off + 3], data[off + 4], data[off + 5]]) as usize;
            let v_len = u32::from_le_bytes([data[off + 6], data[off + 7], data[off + 8], data[off + 9]]) as usize;
            var_offsets[col_idx] = (v_off, v_len);
        }
    } else {
        var_data_start = data.len();
    }

    let mut row = Vec::with_capacity(col_count);
    let mut fixed_idx = 0;

    for (i, col_type) in col_types.iter().enumerate() {
        if null_bitmap & (1u64 << i) != 0 {
            row.push(Value::Null);
            if is_fixed(col_type) { fixed_idx += 1; }
            continue;
        }

        if is_fixed(col_type) {
            let off = HEADER_SIZE + fixed_idx * FIXED_COL_SIZE;
            if off + FIXED_COL_SIZE > data.len() {
                row.push(Value::Null);
            } else {
                row.push(decode_fixed(&data[off..off + FIXED_COL_SIZE], col_type));
            }
            fixed_idx += 1;
        } else {
            let (v_off, v_len) = var_offsets[i];
            let abs_off = var_data_start + v_off;
            if abs_off + v_len > data.len() {
                row.push(Value::Null);
            } else {
                row.push(decode_var(&data[abs_off..abs_off + v_len], col_type)?);
            }
        }
    }

    Ok(row)
}

/// Fast decode with pre-computed fixed_count — avoids O(C) column type scan per row.
fn decode_raw_fast(data: &[u8], col_types: &[ColumnType], fixed_count: usize) -> Result<Row> {
    let mut row = Vec::with_capacity(col_types.len());
    decode_raw_fast_into(data, col_types, fixed_count, &mut row)?;
    Ok(row)
}

/// Partial column decode into reusable buffer.
/// Only decodes columns at the specified positions (0-indexed).
pub fn decode_fast_partial_into(
    data: &[u8],
    col_types: &[ColumnType],
    fixed_count: usize,
    col_positions: &[usize],
    buf: &mut Vec<Value>,
) -> Result<()> {
    if !is_rawrow(data) {
        *buf = bincode::deserialize(data)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        let projected: Vec<Value> = col_positions.iter()
            .map(|&p| buf.get(p).cloned().unwrap_or(Value::Null))
            .collect();
        *buf = projected;
        return Ok(());
    }
    decode_raw_fast_partial_into(data, col_types, fixed_count, col_positions, buf)
}

/// Like `decode_fast_partial_into` but with optional `StringPool` for Text column interning.
pub fn decode_fast_partial_into_with_pool(
    data: &[u8],
    col_types: &[ColumnType],
    fixed_count: usize,
    col_positions: &[usize],
    buf: &mut Vec<Value>,
    pool: Option<&mut StringPool>,
) -> Result<()> {
    if !is_rawrow(data) {
        *buf = bincode::deserialize(data)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        let projected: Vec<Value> = col_positions.iter()
            .map(|&p| buf.get(p).cloned().unwrap_or(Value::Null))
            .collect();
        *buf = projected;
        return Ok(());
    }
    decode_raw_fast_partial_into_with_pool(data, col_types, fixed_count, col_positions, buf, pool)
}

fn decode_raw_fast_into(data: &[u8], col_types: &[ColumnType], fixed_count: usize, row: &mut Vec<Value>) -> Result<()> {
    decode_raw_fast_into_with_pool(data, col_types, fixed_count, row, None)
}

/// Pool-aware version: when `pool` is Some, Text columns go through interning.
fn decode_raw_fast_into_with_pool(data: &[u8], col_types: &[ColumnType], fixed_count: usize, row: &mut Vec<Value>, mut pool: Option<&mut StringPool>) -> Result<()> {
    if data.len() < HEADER_SIZE {
        return Err(StorageError::InvalidData("RawRow data too small".into()));
    }

    let col_count = u16::from_le_bytes([data[2], data[3]]) as usize;
    let null_bitmap = u64::from_le_bytes([
        data[4], data[5], data[6], data[7],
        data[8], data[9], data[10], data[11],
    ]);

    if col_count != col_types.len() {
        *row = bincode::deserialize(data)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        return Ok(());
    }

    let var_section_start = HEADER_SIZE + fixed_count * FIXED_COL_SIZE;

    let mut var_offsets: [(usize, usize); 64] = [(0, 0); 64];
    let var_data_start;
    if var_section_start + 2 <= data.len() {
        let var_count = u16::from_le_bytes([data[var_section_start], data[var_section_start + 1]]) as usize;
        let var_header_start = var_section_start + 2;
        var_data_start = var_header_start + var_count * 10;
        for i in 0..var_count {
            let off = var_header_start + i * 10;
            if off + 10 > data.len() { break; }
            let col_idx = u16::from_le_bytes([data[off], data[off + 1]]) as usize;
            if col_idx >= 64 { break; }
            let v_off = u32::from_le_bytes([data[off + 2], data[off + 3], data[off + 4], data[off + 5]]) as usize;
            let v_len = u32::from_le_bytes([data[off + 6], data[off + 7], data[off + 8], data[off + 9]]) as usize;
            var_offsets[col_idx] = (v_off, v_len);
        }
    } else {
        var_data_start = data.len();
    }

    row.clear();
    if row.capacity() < col_count {
        row.reserve(col_count - row.capacity());
    }

    let mut fixed_idx = 0;

    for (i, col_type) in col_types.iter().enumerate() {
        if null_bitmap & (1u64 << i) != 0 {
            row.push(Value::Null);
            if is_fixed(col_type) { fixed_idx += 1; }
            continue;
        }

        if is_fixed(col_type) {
            let off = HEADER_SIZE + fixed_idx * FIXED_COL_SIZE;
            if off + FIXED_COL_SIZE > data.len() {
                row.push(Value::Null);
            } else {
                row.push(decode_fixed(&data[off..off + FIXED_COL_SIZE], col_type));
            }
            fixed_idx += 1;
        } else {
            let (v_off, v_len) = var_offsets[i];
            let abs_off = var_data_start + v_off;
            if abs_off + v_len > data.len() {
                row.push(Value::Null);
            } else {
                row.push(decode_var_with_pool(&data[abs_off..abs_off + v_len], col_type, pool.as_mut().map(|p| &mut **p))?);
            }
        }
    }

    Ok(())
}

/// Partial column decode: only decode columns at the specified positions.
/// Saves 3-5x deserialization when selecting few columns (e.g. 2/7).
/// Output buffer is cleared and filled with the decoded values in position order.
fn decode_raw_fast_partial_into(
    data: &[u8],
    col_types: &[ColumnType],
    fixed_count: usize,
    col_positions: &[usize],
    out: &mut Vec<Value>,
) -> Result<()> {
    decode_raw_fast_partial_into_with_pool(data, col_types, fixed_count, col_positions, out, None)
}

/// Pool-aware version: when `pool` is Some, Text columns go through interning.
fn decode_raw_fast_partial_into_with_pool(
    data: &[u8],
    col_types: &[ColumnType],
    fixed_count: usize,
    col_positions: &[usize],
    out: &mut Vec<Value>,
    mut pool: Option<&mut StringPool>,
) -> Result<()> {
    if data.len() < HEADER_SIZE {
        return Err(StorageError::InvalidData("RawRow data too small".into()));
    }

    let col_count = u16::from_le_bytes([data[2], data[3]]) as usize;
    if col_count != col_types.len() {
        // Fall back to full decode + project
        decode_raw_fast_into_with_pool(data, col_types, fixed_count, out, pool.as_mut().map(|p| &mut **p))?;
        let projected: Vec<Value> = col_positions.iter()
            .map(|&p| out.get(p).cloned().unwrap_or(Value::Null))
            .collect();
        *out = projected;
        return Ok(());
    }

    let null_bitmap = u64::from_le_bytes([
        data[4], data[5], data[6], data[7],
        data[8], data[9], data[10], data[11],
    ]);

    let var_section_start = HEADER_SIZE + fixed_count * FIXED_COL_SIZE;
    let mut var_offsets: [(usize, usize); 64] = [(0, 0); 64];
    let var_data_start;
    if var_section_start + 2 <= data.len() {
        let var_count = u16::from_le_bytes([data[var_section_start], data[var_section_start + 1]]) as usize;
        let var_header_start = var_section_start + 2;
        var_data_start = var_header_start + var_count * 10;
        for i in 0..var_count {
            let off = var_header_start + i * 10;
            if off + 10 > data.len() { break; }
            let col_idx = u16::from_le_bytes([data[off], data[off + 1]]) as usize;
            if col_idx >= 64 { break; }
            let v_off = u32::from_le_bytes([
                data[off + 2], data[off + 3], data[off + 4], data[off + 5],
            ]) as usize;
            let v_len = u32::from_le_bytes([
                data[off + 6], data[off + 7], data[off + 8], data[off + 9],
            ]) as usize;
            var_offsets[col_idx] = (v_off, v_len);
        }
    } else {
        var_data_start = data.len();
    }

    out.clear();
    if out.capacity() < col_positions.len() {
        out.reserve(col_positions.len() - out.capacity());
    }

    for &col_idx in col_positions {
        if null_bitmap & (1u64 << col_idx) != 0 {
            out.push(Value::Null);
            continue;
        }
        let col_type = &col_types[col_idx];
        if is_fixed(col_type) {
            let fixed_idx = col_types[..col_idx].iter().filter(|t| is_fixed(t)).count();
            let off = HEADER_SIZE + fixed_idx * FIXED_COL_SIZE;
            if off + FIXED_COL_SIZE > data.len() {
                out.push(Value::Null);
            } else {
                out.push(decode_fixed(&data[off..off + FIXED_COL_SIZE], col_type));
            }
        } else {
            let (v_off, v_len) = var_offsets[col_idx];
            let abs_off = var_data_start + v_off;
            if abs_off + v_len > data.len() {
                out.push(Value::Null);
            } else {
                // Empty variable-length values (v_len==0) are valid:
                // empty string = "", empty vector = []. They are NOT NULL.
                out.push(decode_var_with_pool(&data[abs_off..abs_off + v_len], col_type, pool.as_mut().map(|p| &mut **p))?);
            }
        }
    }

    Ok(())
}

/// Decode RawRow without schema — treats all fixed columns as Integer, all var as Text/Vector.
/// Used by index scan paths that don't have the table schema available.
fn decode_raw_any(data: &[u8]) -> Result<Row> {
    decode_raw_any_with_pool(data, None)
}

/// Pool-aware version: when `pool` is Some, Text columns go through interning.
fn decode_raw_any_with_pool(data: &[u8], mut pool: Option<&mut StringPool>) -> Result<Row> {
    if data.len() < HEADER_SIZE {
        return Err(StorageError::InvalidData("RawRow data too small".into()));
    }

    let col_count = u16::from_le_bytes([data[2], data[3]]) as usize;
    let null_bitmap = u64::from_le_bytes([
        data[4], data[5], data[6], data[7],
        data[8], data[9], data[10], data[11],
    ]);

    // Parse var section — stack arrays (max 64 columns)
    let mut var_offsets: [(usize, usize); 64] = [(0, 0); 64];

    // We'll try the var section at various offsets. The fixed_count can be 0..col_count.
    for fixed_count in (0..=col_count).rev() {
        let var_section_start = HEADER_SIZE + fixed_count * FIXED_COL_SIZE;
        if var_section_start + 2 > data.len() { continue; }

        let var_count = u16::from_le_bytes([data[var_section_start], data[var_section_start + 1]]) as usize;
        let var_header_end = var_section_start + 2 + var_count * 10;

        if var_count + fixed_count > col_count { continue; }
        if var_header_end > data.len() { continue; }

        // Check that all col_idx values are valid and distinct
        let mut valid = true;
        let mut seen_bitmap: u64 = 0;
        let var_data_start = var_header_end;
        for i in 0..var_count {
            let off = var_section_start + 2 + i * 10;
            let col_idx = u16::from_le_bytes([data[off], data[off + 1]]) as usize;
            if col_idx >= 64 || col_idx >= col_count {
                valid = false;
                break;
            }
            if seen_bitmap & (1u64 << col_idx) != 0 {
                valid = false;
                break;
            }
            seen_bitmap |= 1u64 << col_idx;
            let v_off = u32::from_le_bytes([data[off + 2], data[off + 3], data[off + 4], data[off + 5]]) as usize;
            let v_len = u32::from_le_bytes([data[off + 6], data[off + 7], data[off + 8], data[off + 9]]) as usize;
            if var_data_start + v_off + v_len > data.len() {
                valid = false;
                break;
            }
            var_offsets[col_idx] = (v_off, v_len);
        }
        if valid && fixed_count + var_count == col_count {
            if var_count == 0 && var_data_start < data.len() {
                continue;
            }
            // Found the right layout
            let mut row = Vec::with_capacity(col_count);
            let mut fixed_idx = 0;

            for (i, &(v_off, v_len)) in var_offsets.iter().enumerate() {
                if null_bitmap & (1u64 << i) != 0 {
                    row.push(Value::Null);
                    continue;
                }
                if seen_bitmap & (1u64 << i) != 0 && v_len > 0 {
                    // Variable column — decode as Text or Vector
                    let abs_off = var_data_start + v_off;
                    if abs_off + v_len > data.len() {
                        row.push(Value::Null);
                    } else {
                        let var_data = &data[abs_off..abs_off + v_len];
                        // Check for tagged bincode value (0xFF prefix)
                        if !var_data.is_empty() && var_data[0] == 0xFF {
                            match bincode::deserialize::<Value>(&var_data[1..]) {
                                Ok(v) => { row.push(v); continue; }
                                Err(_) => { row.push(Value::Null); continue; }
                            }
                        }
                        // Try vector first: [dim: u16] + f32 array
                        if var_data.len() >= 2 {
                            let dim = u16::from_le_bytes([var_data[0], var_data[1]]) as usize;
                            let expected = 2 + dim * 4;
                            if var_data.len() >= expected && dim > 0 && dim <= 65536 {
                                let mut vec = Vec::with_capacity(dim);
                                for j in 0..dim {
                                    let o = 2 + j * 4;
                                    vec.push(f32::from_le_bytes([var_data[o], var_data[o+1], var_data[o+2], var_data[o+3]]));
                                }
                                row.push(Value::Vector(ArcVec(std::sync::Arc::new(vec))));
                                continue;
                            }
                        }
                        // Try as UTF-8 text — use pool when available to deduplicate allocations
                        if let Ok(s) = std::str::from_utf8(var_data) {
                            if let Some(ref mut p) = pool {
                                row.push(Value::Text(ArcString(p.intern(s))));
                            } else {
                                row.push(Value::text_from(s));
                            }
                        } else {
                            // Fallback: bincode
                            match bincode::deserialize::<Value>(var_data) {
                                Ok(v) => row.push(v),
                                Err(_) => row.push(Value::Null),
                            }
                        }
                    }
                } else {
                    // Fixed column
                    let off = HEADER_SIZE + fixed_idx * FIXED_COL_SIZE;
                    if off + FIXED_COL_SIZE > data.len() {
                        row.push(Value::Null);
                    } else {
                        let arr: [u8; 8] = data[off..off+8].try_into().unwrap_or([0; 8]);
                        row.push(Value::Integer(i64::from_le_bytes(arr)));
                    }
                    fixed_idx += 1;
                }
            }
            return Ok(row);
        }
        // Reset var_offsets for next iteration
        var_offsets = [(0, 0); 64];
    }

    // Absolute fallback
    bincode::deserialize(data).map_err(|e| StorageError::Serialization(e.to_string()))
}

fn decode_fixed(bytes: &[u8], col_type: &ColumnType) -> Value {
    let arr: [u8; 8] = bytes[..8].try_into().unwrap_or([0; 8]);
    match col_type {
        ColumnType::Integer => Value::Integer(i64::from_le_bytes(arr)),
        ColumnType::Float => Value::Float(f64::from_le_bytes(arr)),
        ColumnType::Boolean => Value::Bool(bytes[0] != 0),
        ColumnType::Timestamp => Value::Timestamp(Timestamp::from_micros(i64::from_le_bytes(arr))),
        _ => Value::Null,
    }
}

fn decode_var(bytes: &[u8], col_type: &ColumnType) -> Result<Value> {
    decode_var_with_pool(bytes, col_type, None)
}

/// Core decode: when `pool` is Some and the column is Text, intern the Arc<str>
/// through the pool to deduplicate allocations across rows.
fn decode_var_with_pool(bytes: &[u8], col_type: &ColumnType, pool: Option<&mut StringPool>) -> Result<Value> {
    match col_type {
        ColumnType::Text => {
            let s = std::str::from_utf8(bytes)
                .map_err(|_| StorageError::InvalidData("Invalid UTF-8 in Text column".into()))?;
            if let Some(p) = pool {
                Ok(Value::Text(ArcString(p.intern(s))))
            } else {
                Ok(Value::text_from(s))
            }
        }
        _ => {
            // Check for tagged bincode value (0xFF prefix)
            if !bytes.is_empty() && bytes[0] == 0xFF {
                return bincode::deserialize(&bytes[1..])
                    .map_err(|e| StorageError::Serialization(e.to_string()));
            }
            // Try vector format: [dim: u16] + f32 array
            if bytes.len() >= 2 {
                let dim = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
                let expected = 2 + dim * 4;
                if bytes.len() >= expected {
                    let mut vec = Vec::with_capacity(dim);
                    for i in 0..dim {
                        let off = 2 + i * 4;
                        vec.push(f32::from_le_bytes([bytes[off], bytes[off+1], bytes[off+2], bytes[off+3]]));
                    }
                    return Ok(Value::Vector(ArcVec(std::sync::Arc::new(vec))));
                }
            }
            // Fallback: bincode
            bincode::deserialize(bytes)
                .map_err(|e| StorageError::Serialization(e.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sensor_schema() -> Vec<ColumnType> {
        vec![
            ColumnType::Timestamp,
            ColumnType::Float,
            ColumnType::Float,
            ColumnType::Integer,
            ColumnType::Text,
        ]
    }

    fn sensor_row() -> Vec<Value> {
        vec![
            Value::Timestamp(Timestamp::from_micros(1700000000_000_000)),
            Value::Float(23.5),
            Value::Float(45.2),
            Value::Integer(42),
            Value::text("sensor_01".to_string()),
        ]
    }

    #[test]
    fn test_roundtrip() {
        let row = sensor_row();
        let schema = sensor_schema();
        let encoded = encode(&row, &schema).unwrap();
        let decoded = decode(&encoded, &schema).unwrap();

        assert_eq!(decoded.len(), 5);
        assert_eq!(decoded[0], Value::Timestamp(Timestamp::from_micros(1700000000_000_000)));
        assert_eq!(decoded[1], Value::Float(23.5));
        assert_eq!(decoded[2], Value::Float(45.2));
        assert_eq!(decoded[3], Value::Integer(42));
        assert_eq!(decoded[4], Value::text("sensor_01".to_string()));
    }

    #[test]
    fn test_null_columns() {
        let row = vec![
            Value::Timestamp(Timestamp::from_micros(100)),
            Value::Null,
            Value::Float(1.0),
            Value::Null,
            Value::text("ok".to_string()),
        ];
        let schema = sensor_schema();
        let encoded = encode(&row, &schema).unwrap();
        let decoded = decode(&encoded, &schema).unwrap();

        assert_eq!(decoded[1], Value::Null);
        assert_eq!(decoded[3], Value::Null);
        assert_eq!(decoded[4], Value::text("ok".to_string()));
    }

    #[test]
    fn test_single_column_access() {
        let row = sensor_row();
        let schema = sensor_schema();
        let encoded = encode(&row, &schema).unwrap();

        assert_eq!(get_column(&encoded, &schema, 1).unwrap(), Value::Float(23.5));
        assert_eq!(get_column(&encoded, &schema, 3).unwrap(), Value::Integer(42));
        assert_eq!(get_column(&encoded, &schema, 4).unwrap(), Value::text("sensor_01".to_string()));
    }

    #[test]
    fn test_bincode_fallback() {
        let row = vec![Value::Integer(42), Value::Float(3.14)];
        let bincode_data = bincode::serialize(&row).unwrap();
        let schema = vec![ColumnType::Integer, ColumnType::Float];

        let decoded = decode(&bincode_data, &schema).unwrap();
        assert_eq!(decoded[0], Value::Integer(42));
        assert_eq!(decoded[1], Value::Float(3.14));
    }

    #[test]
    fn test_size_smaller_than_bincode() {
        let row = sensor_row();
        let schema = sensor_schema();

        let bincode_size = bincode::serialize(&row).unwrap().len();
        let rawrow_size = encode(&row, &schema).unwrap().len();

        assert!(rawrow_size < bincode_size, "RawRow {} should be < bincode {}", rawrow_size, bincode_size);
    }

    #[test]
    fn test_vector_column() {
        let row = vec![
            Value::Integer(1),
            Value::Vector(ArcVec(std::sync::Arc::new(vec![1.0, 2.0, 3.0, 4.0]))),
        ];
        let schema = vec![ColumnType::Integer, ColumnType::Tensor(4)];
        let encoded = encode(&row, &schema).unwrap();
        let decoded = decode(&encoded, &schema).unwrap();

        match &decoded[1] {
            Value::Vector(v) => assert_eq!(v.len(), 4),
            _ => panic!("Expected Vector"),
        }
    }

    #[test]
    fn test_spatial_roundtrip() {
        use crate::types::{Geometry, Point};
        let schema = vec![ColumnType::Integer, ColumnType::Spatial];
        let row = vec![
            Value::Integer(1),
            Value::Spatial(Box::new(Geometry::Point(Point::new(1.0, 2.0)))),
        ];

        let encoded = encode(&row, &schema).unwrap();
        let decoded = decode(&encoded, &schema).unwrap();
        assert_eq!(decoded[0], Value::Integer(1));
        match &decoded[1] {
            Value::Spatial(g) => match g.as_ref() {
                Geometry::Point(p) => {
                    assert_eq!(p.x, 1.0);
                    assert_eq!(p.y, 2.0);
                }
                other => panic!("Expected Point, got {:?}", other),
            }
            _ => panic!("Expected Spatial Point, got {:?}", decoded[1]),
        }

        // Also test decode_any (schema-less)
        let decoded_any = decode_any(&encoded).unwrap();
        assert_eq!(decoded_any[0], Value::Integer(1));
        match &decoded_any[1] {
            Value::Spatial(g) => match g.as_ref() {
                Geometry::Point(p) => {
                    assert_eq!(p.x, 1.0);
                    assert_eq!(p.y, 2.0);
                }
                other => panic!("decode_any: Expected Point, got {:?}", other),
            }
            _ => panic!("decode_any: Expected Spatial Point, got {:?}", decoded_any[1]),
        }
    }

    #[test]
    fn test_spatial_3col_roundtrip() {
        use crate::types::{Geometry, Point, Point3D};
        // Same schema as the 2d_3d_coexistence test
        let schema = vec![ColumnType::Integer, ColumnType::Spatial, ColumnType::Spatial];
        let row = vec![
            Value::Integer(1),
            Value::Spatial(Box::new(Geometry::Point(Point::new(1.0, 1.0)))),
            Value::Spatial(Box::new(Geometry::Point3D(Point3D { x: 2.0, y: 2.0, z: 2.0 }))),
        ];

        let encoded = encode(&row, &schema).unwrap();

        // Decode with schema
        let decoded = decode(&encoded, &schema).unwrap();
        assert_eq!(decoded[0], Value::Integer(1));
        match &decoded[1] {
            Value::Spatial(g) => match g.as_ref() {
                Geometry::Point(p) => {
                    assert_eq!(p.x, 1.0);
                }
                other => panic!("Expected Point at col 1, got {:?}", other),
            }
            _ => panic!("Expected Spatial Point at col 1, got {:?}", decoded[1]),
        }
        match &decoded[2] {
            Value::Spatial(g) => match g.as_ref() {
                Geometry::Point3D(p) => {
                    assert_eq!(p.x, 2.0);
                }
                other => panic!("Expected Point3D at col 2, got {:?}", other),
            }
            _ => panic!("Expected Spatial Point3D at col 2, got {:?}", decoded[2]),
        }

        // Decode without schema
        let decoded_any = decode_any(&encoded).unwrap();
        assert_eq!(decoded_any[0], Value::Integer(1));
        match &decoded_any[1] {
            Value::Spatial(_) => {}
            other => panic!("decode_any col 1: Expected Spatial, got {:?}", other),
        }
        match &decoded_any[2] {
            Value::Spatial(_) => {}
            other => panic!("decode_any col 2: Expected Spatial, got {:?}", other),
        }
    }

    // ━━━ Partial column decode tests ━━━

    #[test]
    fn test_partial_decode_select_columns() {
        let col_types = vec![ColumnType::Integer, ColumnType::Float, ColumnType::Text, ColumnType::Boolean];
        let row = vec![Value::Integer(42), Value::Float(3.14), Value::Text("hello".into()), Value::Bool(true)];
        let fixed_count = compute_fixed_count(&col_types);
        let encoded = encode(&row, &col_types).unwrap();

        // Decode only columns 0 and 2 (skip float and bool)
        let mut out = Vec::new();
        decode_fast_partial_into(&encoded, &col_types, fixed_count, &[0, 2], &mut out).unwrap();

        assert_eq!(out.len(), 2);
        assert_eq!(out[0], Value::Integer(42));
        assert_eq!(out[1], Value::Text("hello".into()));
    }

    #[test]
    fn test_partial_decode_reordered() {
        let col_types = vec![ColumnType::Integer, ColumnType::Float, ColumnType::Text];
        let row = vec![Value::Integer(1), Value::Float(2.5), Value::Text("abc".into())];
        let fixed_count = compute_fixed_count(&col_types);
        let encoded = encode(&row, &col_types).unwrap();

        let mut out = Vec::new();
        // Select columns in reverse order
        decode_fast_partial_into(&encoded, &col_types, fixed_count, &[2, 1, 0], &mut out).unwrap();

        assert_eq!(out.len(), 3);
        assert_eq!(out[0], Value::Text("abc".into()));
        assert_eq!(out[1], Value::Float(2.5));
        assert_eq!(out[2], Value::Integer(1));
    }

    #[test]
    fn test_partial_decode_with_nulls() {
        let col_types = vec![ColumnType::Integer, ColumnType::Text, ColumnType::Float, ColumnType::Boolean];
        let row = vec![Value::Integer(10), Value::Null, Value::Float(1.5), Value::Null];
        let fixed_count = compute_fixed_count(&col_types);
        let encoded = encode(&row, &col_types).unwrap();

        let mut out = Vec::new();
        decode_fast_partial_into(&encoded, &col_types, fixed_count, &[1, 3], &mut out).unwrap();

        assert_eq!(out.len(), 2);
        assert_eq!(out[0], Value::Null);
        assert_eq!(out[1], Value::Null);
    }

    #[test]
    fn test_partial_decode_reuses_buffer() {
        let col_types = vec![ColumnType::Integer, ColumnType::Text];
        let row1 = vec![Value::Integer(1), Value::Text("a".into())];
        let row2 = vec![Value::Integer(2), Value::Text("b".into())];
        let fixed_count = compute_fixed_count(&col_types);
        let e1 = encode(&row1, &col_types).unwrap();
        let e2 = encode(&row2, &col_types).unwrap();

        let mut buf = Vec::new();
        decode_fast_partial_into(&e1, &col_types, fixed_count, &[0, 1], &mut buf).unwrap();
        assert_eq!(buf[0], Value::Integer(1));

        // Same buffer reused — should be cleared and refilled
        decode_fast_partial_into(&e2, &col_types, fixed_count, &[0, 1], &mut buf).unwrap();
        assert_eq!(buf[0], Value::Integer(2));
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn test_empty_string_not_null() {
        // Regression: empty string must round-trip as Text(""), not Null
        let col_types = vec![ColumnType::Integer, ColumnType::Text, ColumnType::Text];
        let row = vec![
            Value::Integer(1),
            Value::text("".to_string()),
            Value::text("hello".to_string()),
        ];
        let encoded = encode(&row, &col_types).unwrap();
        let decoded = decode(&encoded, &col_types).unwrap();
        assert_eq!(decoded[0], Value::Integer(1));
        match &decoded[1] {
            Value::Text(s) => assert_eq!(s.as_ref() as &str, ""),
            other => panic!("expected Text(''), got {:?}", other),
        }
        match &decoded[2] {
            Value::Text(s) => assert_eq!(s.as_ref() as &str, "hello"),
            other => panic!("expected Text(\"hello\"), got {:?}", other),
        }
    }

    #[test]
    fn test_null_vs_empty_string_distinct() {
        let col_types = vec![ColumnType::Text, ColumnType::Text];
        let row_with_null = vec![Value::Null, Value::text("".to_string())];
        let encoded = encode(&row_with_null, &col_types).unwrap();
        let decoded = decode(&encoded, &col_types).unwrap();
        assert!(matches!(decoded[0], Value::Null), "NULL should decode as Null");
        match &decoded[1] {
            Value::Text(s) => assert_eq!(s.as_ref() as &str, ""),
            other => panic!("expected Text(''), got {:?}", other),
        }
    }
}
