//! Column encoding utilities: dictionary encoding for strings, bit-packing for bools.

use std::collections::HashMap;

/// Sentinel index value for null strings in dictionary encoding.
const NULL_SENTINEL: u16 = u16::MAX;
/// Maximum dictionary size before falling back to raw encoding.
const MAX_DICT_SIZE: u16 = 65000;

/// Encode a slice of optional strings using dictionary encoding.
///
/// Format:
/// ```text
/// [dict_len: u32 LE]
/// [per entry: len: u16 LE, bytes: [u8; len]]
/// [per value: index: u16 LE]  (u16::MAX = null)
/// ```
///
/// Falls back to raw encoding if unique strings > 65000.
pub fn encode_strings(values: &[Option<String>]) -> (Vec<u8>, StringEncoding) {
    if values.is_empty() {
        return (Vec::new(), StringEncoding::Dictionary);
    }

    // Build dictionary and check cardinality
    let mut dict: HashMap<String, u16> = HashMap::new();
    let mut dict_entries: Vec<String> = Vec::new();

    for val in values {
        if let Some(s) = val {
            if !dict.contains_key(s) {
                if dict.len() >= MAX_DICT_SIZE as usize {
                    // Too many unique values — fall back to raw
                    return encode_strings_raw(values);
                }
                let idx = dict_entries.len() as u16;
                dict.insert(s.clone(), idx);
                dict_entries.push(s.clone());
            }
        }
    }

    let mut buf = Vec::new();

    // Dictionary size
    buf.extend_from_slice(&(dict_entries.len() as u32).to_le_bytes());

    // Dictionary entries
    for entry in &dict_entries {
        let bytes = entry.as_bytes();
        buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(bytes);
    }

    // Indices
    for val in values {
        match val {
            None => buf.extend_from_slice(&NULL_SENTINEL.to_le_bytes()),
            Some(s) => {
                let idx = dict[s];
                buf.extend_from_slice(&idx.to_le_bytes());
            }
        }
    }

    (buf, StringEncoding::Dictionary)
}

/// Raw string encoding fallback for high-cardinality columns.
///
/// Format: per value `[is_null: u8][len: u16 LE][bytes]` (no len if null)
fn encode_strings_raw(values: &[Option<String>]) -> (Vec<u8>, StringEncoding) {
    let mut buf = Vec::new();
    for val in values {
        match val {
            None => buf.push(0),
            Some(s) => {
                buf.push(1);
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
        }
    }
    (buf, StringEncoding::Raw)
}

/// Decode dictionary-encoded strings.
pub fn decode_strings(data: &[u8], count: usize, encoding: StringEncoding) -> Vec<Option<String>> {
    if count == 0 {
        return Vec::new();
    }

    match encoding {
        StringEncoding::Dictionary => decode_strings_dictionary(data, count),
        StringEncoding::Raw => decode_strings_raw(data, count),
    }
}

fn decode_strings_dictionary(data: &[u8], count: usize) -> Vec<Option<String>> {
    let mut cursor = 0usize;

    // Read dictionary size
    let dict_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
    cursor += 4;

    // Read dictionary entries
    let mut dict_entries: Vec<String> = Vec::with_capacity(dict_len);
    for _ in 0..dict_len {
        let len = u16::from_le_bytes(data[cursor..cursor + 2].try_into().unwrap()) as usize;
        cursor += 2;
        let s = String::from_utf8_lossy(&data[cursor..cursor + len]).to_string();
        cursor += len;
        dict_entries.push(s);
    }

    // Read indices
    let mut result = Vec::with_capacity(count);
    for _ in 0..count {
        let idx = u16::from_le_bytes(data[cursor..cursor + 2].try_into().unwrap());
        cursor += 2;
        if idx == NULL_SENTINEL {
            result.push(None);
        } else {
            result.push(Some(dict_entries[idx as usize].clone()));
        }
    }

    result
}

fn decode_strings_raw(data: &[u8], count: usize) -> Vec<Option<String>> {
    let mut cursor = 0usize;
    let mut result = Vec::with_capacity(count);

    for _ in 0..count {
        let is_null = data[cursor] == 0;
        cursor += 1;
        if is_null {
            result.push(None);
        } else {
            let len = u16::from_le_bytes(data[cursor..cursor + 2].try_into().unwrap()) as usize;
            cursor += 2;
            let s = String::from_utf8_lossy(&data[cursor..cursor + len]).to_string();
            cursor += len;
            result.push(Some(s));
        }
    }

    result
}

/// String encoding variant (stored in segment metadata).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StringEncoding {
    Dictionary = 0,
    Raw = 1,
}

/// Encode bools with bit-packing (8 bools per byte).
/// Returns (packed_bytes, null_bitmap) where null_bitmap is set if any nulls exist.
pub fn encode_bools(values: &[Option<bool>]) -> (Vec<u8>, Option<Vec<u8>>) {
    let n = values.len();
    let packed_bytes = (n + 7) / 8;
    let mut packed = vec![0u8; packed_bytes];

    let has_nulls = values.iter().any(|v| v.is_none());
    let null_bitmap = if has_nulls {
        let mut bm = vec![0u8; packed_bytes];
        for (i, val) in values.iter().enumerate() {
            if val.is_none() {
                bm[i / 8] |= 1 << (i % 8);
            }
        }
        Some(bm)
    } else {
        None
    };

    for (i, val) in values.iter().enumerate() {
        match val {
            Some(true) => packed[i / 8] |= 1 << (i % 8),
            Some(false) => {} // already 0
            None => {}        // null: value doesn't matter, null_bitmap tracks it
        }
    }

    (packed, null_bitmap)
}

/// Decode bit-packed bools.
pub fn decode_bools(packed: &[u8], null_bitmap: Option<&[u8]>, count: usize) -> Vec<Option<bool>> {
    let mut result = Vec::with_capacity(count);

    for i in 0..count {
        let is_null = null_bitmap.map_or(false, |bm| (bm[i / 8] >> (i % 8)) & 1 == 1);
        if is_null {
            result.push(None);
        } else {
            let val = (packed[i / 8] >> (i % 8)) & 1 == 1;
            result.push(Some(val));
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Dictionary string encoding ---

    #[test]
    fn test_string_dictionary_roundtrip() {
        let values = vec![
            Some("hello".to_string()),
            Some("world".to_string()),
            None,
            Some("hello".to_string()), // duplicate
            Some("foo".to_string()),
            None,
        ];
        let (encoded, enc_type) = encode_strings(&values);
        assert_eq!(enc_type, StringEncoding::Dictionary);

        let decoded = decode_strings(&encoded, values.len(), enc_type);
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_string_all_nulls() {
        let values: Vec<Option<String>> = vec![None, None, None];
        let (encoded, enc_type) = encode_strings(&values);
        let decoded = decode_strings(&encoded, values.len(), enc_type);
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_string_empty() {
        let values: Vec<Option<String>> = vec![];
        let (encoded, enc_type) = encode_strings(&values);
        assert!(encoded.is_empty());
        let decoded = decode_strings(&encoded, 0, enc_type);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_string_single_value() {
        let values = vec![Some("test".to_string())];
        let (encoded, enc_type) = encode_strings(&values);
        let decoded = decode_strings(&encoded, 1, enc_type);
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_string_high_cardinality_fallback() {
        // 70000 unique strings — should fall back to raw
        let values: Vec<Option<String>> = (0..70000)
            .map(|i| Some(format!("unique_string_{}", i)))
            .collect();
        let (_, enc_type) = encode_strings(&values);
        assert_eq!(enc_type, StringEncoding::Raw);
    }

    #[test]
    fn test_string_raw_roundtrip() {
        let values: Vec<Option<String>> = (0..1000)
            .map(|i| if i % 10 == 0 { None } else { Some(format!("val_{}", i)) })
            .collect();
        let (encoded, enc_type) = encode_strings(&values);
        // With 900 unique strings, should use dictionary
        let decoded = decode_strings(&encoded, values.len(), enc_type);
        assert_eq!(decoded, values);
    }

    // --- Bool bit-packing ---

    #[test]
    fn test_bool_roundtrip_no_nulls() {
        let values: Vec<Option<bool>> = vec![Some(true), Some(false), Some(true), Some(true)];
        let (packed, null_bm) = encode_bools(&values);
        assert!(null_bm.is_none());

        let decoded = decode_bools(&packed, null_bm.as_deref(), values.len());
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_bool_roundtrip_with_nulls() {
        let values: Vec<Option<bool>> = vec![Some(true), None, Some(false), None, Some(true)];
        let (packed, null_bm) = encode_bools(&values);
        assert!(null_bm.is_some());

        let decoded = decode_bools(&packed, null_bm.as_deref(), values.len());
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_bool_all_false() {
        let values: Vec<Option<bool>> = vec![Some(false); 16];
        let (packed, null_bm) = encode_bools(&values);
        assert!(null_bm.is_none());
        // All packed bytes should be 0
        assert!(packed.iter().all(|&b| b == 0));

        let decoded = decode_bools(&packed, null_bm.as_deref(), values.len());
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_bool_all_true() {
        let values: Vec<Option<bool>> = vec![Some(true); 16];
        let (packed, null_bm) = encode_bools(&values);
        // 16 bools = 2 bytes, all bits set = 0xFF
        assert_eq!(packed.len(), 2);
        assert_eq!(packed[0], 0xFF);
        assert_eq!(packed[1], 0xFF);

        let decoded = decode_bools(&packed, null_bm.as_deref(), values.len());
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_bool_large_count() {
        let values: Vec<Option<bool>> = (0..1000)
            .map(|i| if i % 7 == 0 { None } else { Some(i % 2 == 0) })
            .collect();
        let (packed, null_bm) = encode_bools(&values);
        let decoded = decode_bools(&packed, null_bm.as_deref(), values.len());
        assert_eq!(decoded, values);
    }
}
