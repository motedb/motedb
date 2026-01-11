//! Text FTS Encoding - Varint/Delta Compression
//!
//! Based on SQLite FTS5 encoding strategy:
//! - Varint encoding for space efficiency (1-9 bytes per integer)
//! - Delta encoding for document IDs (only store differences)
//! - Position deltas (if enabled)
//! - Segmented posting lists (split large lists into segments)

use crate::{Result, StorageError};

/// Encode a u64 using Varint encoding (1-9 bytes)
/// 
/// Format:
/// - Each byte: [continuation_bit:1][data:7]
/// - If continuation_bit=1, more bytes follow
/// - Little-endian order
pub fn encode_varint(mut value: u64) -> Vec<u8> {
    let mut bytes = Vec::new();
    
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        
        if value != 0 {
            byte |= 0x80; // Set continuation bit
        }
        
        bytes.push(byte);
        
        if value == 0 {
            break;
        }
    }
    
    bytes
}

/// Decode a u64 from Varint encoding
/// 
/// Returns: (decoded_value, bytes_consumed)
pub fn decode_varint(bytes: &[u8]) -> Result<(u64, usize)> {
    let mut value = 0u64;
    let mut shift = 0;
    let mut pos = 0;
    
    loop {
        if pos >= bytes.len() {
            return Err(StorageError::InvalidData("Incomplete varint".into()));
        }
        
        let byte = bytes[pos];
        pos += 1;
        
        // Extract 7 data bits
        value |= ((byte & 0x7F) as u64) << shift;
        shift += 7;
        
        // Check continuation bit
        if (byte & 0x80) == 0 {
            break;
        }
        
        if shift >= 64 {
            return Err(StorageError::InvalidData("Varint overflow".into()));
        }
    }
    
    Ok((value, pos))
}

/// Encode a posting list with delta compression
/// 
/// Format:
/// ```
/// [num_docs: varint]
/// [doc_id_0: varint]
/// [doc_id_delta_1: varint]
/// [doc_id_delta_2: varint]
/// ...
/// [positions_flag: 1 byte] (0=no positions, 1=has positions)
/// [if positions_flag=1]:
///   [num_positions_for_doc_0: varint]
///   [pos_0: varint]
///   [pos_delta_1: varint]
///   ...
///   [num_positions_for_doc_1: varint]
///   ...
/// ```
pub fn encode_posting_list(
    doc_ids: &[u64],
    positions: Option<&Vec<Vec<u32>>>,
) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    
    // 1. Encode number of documents
    bytes.extend_from_slice(&encode_varint(doc_ids.len() as u64));
    
    // 2. Encode document IDs with delta compression
    let mut prev_doc_id = 0u64;
    for &doc_id in doc_ids {
        if doc_id < prev_doc_id {
            return Err(StorageError::InvalidData(
                "Document IDs must be sorted".into()
            ));
        }
        
        let delta = doc_id - prev_doc_id;
        bytes.extend_from_slice(&encode_varint(delta));
        prev_doc_id = doc_id;
    }
    
    // 3. Encode positions flag
    if let Some(pos_lists) = positions {
        bytes.push(1); // Has positions
        
        // 4. Encode positions for each document
        for pos_list in pos_lists {
            // Number of positions in this document
            bytes.extend_from_slice(&encode_varint(pos_list.len() as u64));
            
            // Delta-encode positions
            let mut prev_pos = 0u32;
            for &pos in pos_list {
                let delta = pos - prev_pos;
                bytes.extend_from_slice(&encode_varint(delta as u64));
                prev_pos = pos;
            }
        }
    } else {
        bytes.push(0); // No positions
    }
    
    Ok(bytes)
}

/// Decode a posting list
/// 
/// Returns: (doc_ids, positions_if_present)
pub fn decode_posting_list(bytes: &[u8]) -> Result<(Vec<u64>, Option<Vec<Vec<u32>>>)> {
    let mut pos = 0;
    
    // 1. Decode number of documents
    let (num_docs, consumed) = decode_varint(&bytes[pos..])?;
    pos += consumed;
    
    // 2. Decode document IDs with delta decompression
    let mut doc_ids = Vec::with_capacity(num_docs as usize);
    let mut prev_doc_id = 0u64;
    
    for _ in 0..num_docs {
        let (delta, consumed) = decode_varint(&bytes[pos..])?;
        pos += consumed;
        
        let doc_id = prev_doc_id + delta;
        doc_ids.push(doc_id);
        prev_doc_id = doc_id;
    }
    
    // 3. Check positions flag
    if pos >= bytes.len() {
        return Err(StorageError::InvalidData("Missing positions flag".into()));
    }
    
    let has_positions = bytes[pos] != 0;
    pos += 1;
    
    // 4. Decode positions if present
    let positions = if has_positions {
        let mut pos_lists = Vec::with_capacity(num_docs as usize);
        
        for _ in 0..num_docs {
            let (num_positions, consumed) = decode_varint(&bytes[pos..])?;
            pos += consumed;
            
            let mut position_list = Vec::with_capacity(num_positions as usize);
            let mut prev_pos = 0u32;
            
            for _ in 0..num_positions {
                let (delta, consumed) = decode_varint(&bytes[pos..])?;
                pos += consumed;
                
                let position = prev_pos + delta as u32;
                position_list.push(position);
                prev_pos = position;
            }
            
            pos_lists.push(position_list);
        }
        
        Some(pos_lists)
    } else {
        None
    };
    
    Ok((doc_ids, positions))
}

/// Segment size for large posting lists (SQLite FTS5 style)
/// 
/// Large posting lists are split into segments to:
/// 1. Reduce memory usage (load only needed segments)
/// 2. Improve update performance (rewrite only changed segments)
/// 3. Enable efficient merging during compaction
pub const SEGMENT_SIZE: usize = 256;

/// Encode posting list with segmentation
/// 
/// Returns: Vec<(segment_id, encoded_bytes)>
pub fn encode_segmented_posting_list(
    doc_ids: &[u64],
    positions: Option<&Vec<Vec<u32>>>,
) -> Result<Vec<(u32, Vec<u8>)>> {
    let mut segments = Vec::new();
    let mut segment_id = 0u32;
    
    let num_docs = doc_ids.len();
    let mut offset = 0;
    
    while offset < num_docs {
        let end = (offset + SEGMENT_SIZE).min(num_docs);
        let segment_doc_ids = &doc_ids[offset..end];
        
        let segment_positions = positions.as_ref().map(|pos_lists| {
            pos_lists[offset..end].to_vec()
        });
        
        let encoded = encode_posting_list(
            segment_doc_ids,
            segment_positions.as_ref(),
        )?;
        
        segments.push((segment_id, encoded));
        
        segment_id += 1;
        offset = end;
    }
    
    Ok(segments)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_varint_encoding() {
        // Test various values
        let test_cases = vec![
            0u64,
            127,
            128,
            16383,
            16384,
            u64::MAX,
        ];
        
        for value in test_cases {
            let encoded = encode_varint(value);
            let (decoded, consumed) = decode_varint(&encoded).unwrap();
            
            assert_eq!(decoded, value);
            assert_eq!(consumed, encoded.len());
        }
    }
    
    #[test]
    fn test_posting_list_no_positions() {
        let doc_ids = vec![10, 25, 30, 100, 250];
        
        let encoded = encode_posting_list(&doc_ids, None).unwrap();
        let (decoded_ids, decoded_pos) = decode_posting_list(&encoded).unwrap();
        
        assert_eq!(decoded_ids, doc_ids);
        assert!(decoded_pos.is_none());
    }
    
    #[test]
    fn test_posting_list_with_positions() {
        let doc_ids = vec![10, 25, 30];
        let positions = vec![
            vec![0, 5, 10],
            vec![2, 8],
            vec![1, 3, 7, 15],
        ];
        
        let encoded = encode_posting_list(&doc_ids, Some(&positions)).unwrap();
        let (decoded_ids, decoded_pos) = decode_posting_list(&encoded).unwrap();
        
        assert_eq!(decoded_ids, doc_ids);
        assert_eq!(decoded_pos.unwrap(), positions);
    }
    
    #[test]
    fn test_segmented_posting_list() {
        let mut doc_ids = Vec::new();
        for i in 0..1000 {
            doc_ids.push(i);
        }
        
        let segments = encode_segmented_posting_list(&doc_ids, None).unwrap();
        
        // Should split into multiple segments
        assert!(segments.len() > 1);
        
        // Each segment should have sequential IDs
        for (i, (seg_id, _)) in segments.iter().enumerate() {
            assert_eq!(*seg_id, i as u32);
        }
    }
    
    #[test]
    fn test_delta_compression_efficiency() {
        // Sequential IDs compress well
        let doc_ids = vec![100, 101, 102, 103, 104];
        let encoded = encode_posting_list(&doc_ids, None).unwrap();
        
        // Should be much smaller than 5 * 8 bytes (raw u64)
        // Varint of 5 (num_docs) = 1 byte
        // First ID (100) = 2 bytes
        // Deltas (1,1,1,1) = 4 bytes
        // Flag (0) = 1 byte
        // Total: ~8 bytes vs 40 bytes raw
        assert!(encoded.len() < 15);
    }
}
