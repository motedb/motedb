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
use crate::types::{ArcVec, Row, Timestamp, Value};
use crate::{Result, StorageError};

const RAWROW_MAGIC: u16 = 0x4D52;
const HEADER_SIZE: usize = 12;
const FIXED_COL_SIZE: usize = 8;

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

/// Compute the number of fixed-size columns in a schema.
pub fn compute_fixed_count(col_types: &[ColumnType]) -> usize {
    col_types.iter().filter(|t| is_fixed(t)).count()
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
            if v_len > 0 {
                let abs_off = var_data_start + v_off;
                if abs_off + v_len > data.len() {
                    row.push(Value::Null);
                } else {
                    row.push(decode_var(&data[abs_off..abs_off + v_len], col_type)?);
                }
            } else {
                row.push(Value::Null);
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

fn decode_raw_fast_into(data: &[u8], col_types: &[ColumnType], fixed_count: usize, row: &mut Vec<Value>) -> Result<()> {
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
            if v_len > 0 {
                let abs_off = var_data_start + v_off;
                if abs_off + v_len > data.len() {
                    row.push(Value::Null);
                } else {
                    row.push(decode_var(&data[abs_off..abs_off + v_len], col_type)?);
                }
            } else {
                row.push(Value::Null);
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
    if data.len() < HEADER_SIZE {
        return Err(StorageError::InvalidData("RawRow data too small".into()));
    }

    let col_count = u16::from_le_bytes([data[2], data[3]]) as usize;
    if col_count != col_types.len() {
        // Fall back to full decode + project
        decode_raw_fast_into(data, col_types, fixed_count, out)?;
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
            if v_len > 0 {
                let abs_off = var_data_start + v_off;
                if abs_off + v_len > data.len() {
                    out.push(Value::Null);
                } else {
                    out.push(decode_var(&data[abs_off..abs_off + v_len], col_type)?);
                }
            } else {
                out.push(Value::Null);
            }
        }
    }

    Ok(())
}

/// Decode RawRow without schema — treats all fixed columns as Integer, all var as Text/Vector.
/// Used by index scan paths that don't have the table schema available.
fn decode_raw_any(data: &[u8]) -> Result<Row> {
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
                        // Try as UTF-8 text
                        if let Ok(s) = std::str::from_utf8(var_data) {
                            row.push(Value::text(s.to_string()));
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
    match col_type {
        ColumnType::Text => {
            let s = std::str::from_utf8(bytes)
                .map_err(|_| StorageError::InvalidData("Invalid UTF-8 in Text column".into()))?;
            Ok(Value::text(s.to_string()))
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
}
