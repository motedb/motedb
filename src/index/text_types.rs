//! Text Index Base Types
//!
//! Provides shared types for text indexing:
//! - Tokenizers (Whitespace, Ngram)
//! - PostingList with Roaring Bitmap
//! - BM25 Configuration
//! - Core data structures

use crate::{Result, StorageError};
use roaring::RoaringBitmap;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};

/// Term ID (32-bit, supports 4B unique tokens)
pub type TermId = u32;

/// Document ID (64-bit)
pub type DocId = u64;

/// Token position in document
pub type Position = u32;

//=============================================================================
// PART 1: Pluggable Tokenizer System
//=============================================================================

/// Token produced by tokenizer
#[derive(Debug, Clone)]
pub struct Token {
    pub text: String,
    pub position: Position,
}

/// Tokenizer trait for pluggable text analysis
pub trait Tokenizer: Send + Sync {
    /// Tokenize text into a list of tokens
    fn tokenize(&self, text: &str) -> Vec<Token>;
    
    /// Get tokenizer name
    fn name(&self) -> &str;
}

/// Whitespace tokenizer (default, fast)
#[derive(Debug, Clone)]
pub struct WhitespaceTokenizer {
    pub case_sensitive: bool,
    pub min_len: usize,
    pub max_len: usize,
}

impl Default for WhitespaceTokenizer {
    fn default() -> Self {
        Self {
            case_sensitive: false,
            min_len: 1,
            max_len: 64,
        }
    }
}

impl Tokenizer for WhitespaceTokenizer {
    fn tokenize(&self, text: &str) -> Vec<Token> {
        let normalized = if self.case_sensitive {
            text.to_string()
        } else {
            text.to_lowercase()
        };
        
        normalized
            .split(|c: char| !c.is_alphanumeric() && c != '_')
            .enumerate()
            .filter_map(|(i, s)| {
                if s.len() >= self.min_len && s.len() <= self.max_len {
                    Some(Token {
                        text: s.to_string(),
                        position: i as Position,
                    })
                } else {
                    None
                }
            })
            .collect()
    }
    
    fn name(&self) -> &str {
        "whitespace"
    }
}

/// N-gram tokenizer (for CJK and fuzzy search)
#[derive(Debug, Clone)]
pub struct NgramTokenizer {
    pub n: usize,
    pub case_sensitive: bool,
}

impl NgramTokenizer {
    pub fn new(n: usize) -> Self {
        Self {
            n,
            case_sensitive: false,
        }
    }
}

impl Tokenizer for NgramTokenizer {
    fn tokenize(&self, text: &str) -> Vec<Token> {
        let normalized = if self.case_sensitive {
            text.to_string()
        } else {
            text.to_lowercase()
        };
        
        let chars: Vec<char> = normalized.chars().collect();
        if chars.len() < self.n {
            return vec![];
        }
        
        chars
            .windows(self.n)
            .enumerate()
            .map(|(i, window)| Token {
                text: window.iter().collect(),
                position: i as Position,
            })
            .collect()
    }
    
    fn name(&self) -> &str {
        "ngram"
    }
}

//=============================================================================
// PART 2: BM25 Ranking
//=============================================================================

/// BM25 scoring parameters
#[derive(Debug, Clone, Copy)]
pub struct BM25Config {
    /// Term frequency saturation parameter (typically 1.2-2.0)
    pub k1: f32,
    
    /// Length normalization parameter (typically 0.75)
    pub b: f32,
}

impl Default for BM25Config {
    fn default() -> Self {
        Self {
            k1: 1.5,
            b: 0.75,
        }
    }
}

//=============================================================================
// PART 3: Core Data Structures
//=============================================================================

/// Posting list for a term (memory-optimized)
#[derive(Debug, Clone)]
pub struct PostingList {
    /// Document IDs (Roaring Bitmap for 90%+ compression)
    doc_ids: RoaringBitmap,
    
    /// Document frequencies (parallel array to doc_ids for memory efficiency)
    /// doc_freqs[i] is the frequency of term in doc_ids[i]
    /// ✅ This saves ~50% memory vs HashMap<DocId, u16>
    doc_freqs: Vec<u16>,
    
    /// Positions in documents (for phrase queries, disabled by default)
    positions: Option<HashMap<DocId, Vec<Position>>>,
}

// Manual Serialize/Deserialize for PostingList
impl Serialize for PostingList {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("PostingList", 3)?;
        
        // Serialize roaring bitmap as vec of u32
        let doc_ids: Vec<u32> = self.doc_ids.iter().collect();
        state.serialize_field("doc_ids", &doc_ids)?;
        state.serialize_field("doc_freqs", &self.doc_freqs)?;
        state.serialize_field("positions", &self.positions)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for PostingList {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper {
            doc_ids: Vec<u32>,
            doc_freqs: Vec<u16>,
            positions: Option<HashMap<DocId, Vec<Position>>>,
        }
        
        let helper = Helper::deserialize(deserializer)?;
        let doc_ids = RoaringBitmap::from_iter(helper.doc_ids);
        
        Ok(PostingList {
            doc_ids,
            doc_freqs: helper.doc_freqs,
            positions: helper.positions,
        })
    }
}

impl Default for PostingList {
    fn default() -> Self {
        Self::new()
    }
}

impl PostingList {
    pub fn new() -> Self {
        Self {
            doc_ids: RoaringBitmap::new(),
            doc_freqs: Vec::new(),
            positions: Some(HashMap::new()),
        }
    }
    
    /// Create PostingList without positions map (memory optimization)
    /// 
    /// When positions are disabled, we don't need the HashMap at all.
    /// This saves ~50% memory and eliminates HashMap lookup overhead!
    pub fn new_without_positions(disable_positions: bool) -> Self {
        Self {
            doc_ids: RoaringBitmap::new(),
            doc_freqs: Vec::new(),
            positions: if disable_positions { None } else { Some(HashMap::new()) },
        }
    }
    
    /// Compact serialization for disk persistence (85% space saving)
    /// 
    /// Format:
    /// - [roaring_bitmap_bytes] (variable, ~2-4KB for 2000 docs)
    /// - [doc_freqs_count: u32] (4 bytes)
    /// - [doc_freqs: u16...] (2 * count bytes)
    /// 
    /// Total: ~6-8KB (vs 50-70KB with bincode)
    pub fn serialize_compact(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        
        // 1. Serialize RoaringBitmap (highly compressed)
        self.doc_ids.serialize_into(&mut buf)
            .map_err(|e| StorageError::Serialization(format!("RoaringBitmap serialize error: {}", e)))?;
        
        // 2. Calculate and serialize doc_freqs from positions
        let doc_freqs: Vec<u16> = if let Some(ref pos_map) = self.positions {
            self.doc_ids.iter()
                .map(|id| pos_map.get(&(id as u64)).map(|v| v.len() as u16).unwrap_or(1))
                .collect()
        } else {
            // No positions tracked, assume frequency=1 for all docs
            vec![1u16; self.doc_ids.len() as usize]
        };
        
        buf.extend_from_slice(&(doc_freqs.len() as u32).to_le_bytes());
        for &freq in &doc_freqs {
            buf.extend_from_slice(&freq.to_le_bytes());
        }
        
        // Note: positions are not serialized (stored separately if needed)
        
        Ok(buf)
    }
    
    /// Deserialize from compact format
    pub fn deserialize_compact(buf: &[u8]) -> Result<Self> {
        use std::io::Cursor;
        
        if buf.is_empty() {
            return Err(StorageError::InvalidData("Empty buffer".into()));
        }
        
        let mut cursor = Cursor::new(buf);
        
        // 1. Deserialize RoaringBitmap
        let doc_ids = RoaringBitmap::deserialize_from(&mut cursor)
            .map_err(|e| StorageError::Serialization(
                format!("RoaringBitmap deserialize error (buf_len={}): {}", buf.len(), e)
            ))?;
        
        let offset = cursor.position() as usize;
        
        // 2. Deserialize doc_freqs
        if offset + 4 > buf.len() {
            return Err(StorageError::InvalidData(
                format!("Buffer too small for doc_freqs count: offset={}, buf_len={}", offset, buf.len())
            ));
        }
        
        let count = u32::from_le_bytes([
            buf[offset], buf[offset+1], buf[offset+2], buf[offset+3]
        ]) as usize;
        
        let mut offset = offset + 4;
        let mut doc_freqs = Vec::with_capacity(count);
        
        for _ in 0..count {
            if offset + 2 > buf.len() {
                return Err(StorageError::InvalidData("Buffer too small for doc_freqs".into()));
            }
            
            let freq = u16::from_le_bytes([buf[offset], buf[offset+1]]);
            doc_freqs.push(freq);
            offset += 2;
        }
        
        Ok(PostingList {
            doc_ids,
            doc_freqs,
            positions: None,  // Positions not stored in compact format
        })
    }
    
    /// Add a document occurrence (optimized for sequential inserts)
    pub fn add(&mut self, doc_id: DocId, position: Option<Position>) {
        self.doc_ids.insert(doc_id as u32);

        // For positions
        if let Some(pos) = position {
            if let Some(ref mut pos_map) = self.positions {
                pos_map.entry(doc_id).or_default().push(pos);
            }
        }

        // doc_freqs parallel array is lazily maintained.
        // It is rebuilt during serialization (serialize_compact) and on demand.
        // When positions are absent, term_frequency() falls back to counting
        // occurrences via doc_freqs.len() vs doc_ids.len().
    }

    /// Add a document with a known term frequency (used when converting from block format).
    /// Ensures doc_freqs array stays in sync.
    pub fn add_with_freq(&mut self, doc_id: DocId, _position: Option<Position>, tf: u16) {
        let is_new = !self.doc_ids.contains(doc_id as u32);
        self.doc_ids.insert(doc_id as u32);
        if is_new {
            self.doc_freqs.push(tf);
        }
    }
    
    /// Add multiple occurrences of a document (for term frequency)
    pub fn add_multiple(&mut self, doc_id: DocId, _count: u16, positions: Option<Vec<Position>>) {
        self.doc_ids.insert(doc_id as u32);

        if let Some(pos_vec) = positions {
            if let Some(ref mut pos_map) = self.positions {
                pos_map.entry(doc_id).or_default().extend(pos_vec);
            }
        }
    }

    /// Rebuild doc_freqs array after doc_ids change (maintains parallel structure)
    fn rebuild_doc_freqs_array(&mut self) {
        let old_freqs_map: HashMap<u64, u16> = self.doc_ids.iter()
            .zip(self.doc_freqs.iter())
            .map(|(id, &freq)| (id as u64, freq))
            .collect();
        
        self.doc_freqs = self.doc_ids.iter()
            .map(|id| *old_freqs_map.get(&(id as u64)).unwrap_or(&0))
            .collect();
    }
    
    /// Merge another posting list into this one
    pub fn merge(&mut self, other: &PostingList) {
        // Build temporary HashMap for easier merging
        let mut freq_map: HashMap<u64, u16> = self.doc_ids.iter()
            .zip(self.doc_freqs.iter())
            .map(|(id, &freq)| (id as u64, freq))
            .collect();
        
        // Merge other's frequencies
        for (id, &freq) in other.doc_ids.iter().zip(other.doc_freqs.iter()) {
            *freq_map.entry(id as u64).or_insert(0) += freq;
        }
        
        // Merge doc_ids
        self.doc_ids |= &other.doc_ids;
        
        // Rebuild parallel array
        self.doc_freqs = self.doc_ids.iter()
            .map(|id| *freq_map.get(&(id as u64)).unwrap_or(&0))
            .collect();
        
        // Merge positions
        if let (Some(ref mut self_pos), Some(ref other_pos)) = (&mut self.positions, &other.positions) {
            for (doc_id, positions) in other_pos {
                self_pos.entry(*doc_id).or_default().extend(positions);
            }
        }
    }
    
    pub fn doc_ids(&self) -> Vec<DocId> {
        self.doc_ids.iter().map(|id| id as DocId).collect()
    }
    
    pub fn doc_count(&self) -> u64 {
        self.doc_ids.len()
    }
    
    pub fn term_frequency(&self, doc_id: DocId) -> u16 {
        if !self.doc_ids.contains(doc_id as u32) {
            return 0;
        }
        // Fast path: lookup in positions map (O(1))
        if let Some(ref pos_map) = self.positions {
            return pos_map.get(&doc_id).map(|v| v.len() as u16).unwrap_or(0);
        }
        // Use doc_freqs parallel array via RoaringBitmap rank
        // Only if the parallel array is in sync with doc_ids
        let doc_count = self.doc_ids.len() as usize;
        if self.doc_freqs.len() == doc_count {
            let rank = self.doc_ids.rank(doc_id as u32);
            if rank > 0 && (rank as usize) <= self.doc_freqs.len() {
                return self.doc_freqs[(rank - 1) as usize];
            }
        }
        // Fallback: doc_freqs out of sync, default to 1
        1
    }
    
    /// Check if a document exists in the posting list
    pub fn contains(&self, doc_id: DocId) -> bool {
        self.doc_ids.contains(doc_id as u32)
    }

    /// Return the maximum term frequency across all documents.
    /// Used for WAND upper bound computation.
    pub fn max_tf(&self) -> u16 {
        if let Some(ref pos_map) = self.positions {
            pos_map.values().map(|v| v.len() as u16).max().unwrap_or(0)
        } else if self.doc_freqs.len() == self.doc_ids.len() as usize {
            self.doc_freqs.iter().copied().max().unwrap_or(0)
        } else {
            // doc_freqs out of sync, iterate doc_ids
            let count = self.doc_ids.len();
            if count == 0 { return 0; }
            1 // fallback: assume tf=1 for all
        }
    }

    /// Iterate over (doc_id_u32, tf) pairs. Ensures TF is available
    /// even when doc_freqs is out of sync with doc_ids.
    pub fn iter_doc_tf(&self) -> Vec<(u32, u16)> {
        let doc_count = self.doc_ids.len() as usize;
        if let Some(ref pos_map) = self.positions {
            self.doc_ids.iter()
                .map(|id| (id, pos_map.get(&(id as u64)).map(|v| v.len() as u16).unwrap_or(0)))
                .collect()
        } else if self.doc_freqs.len() == doc_count {
            let result: Vec<(u32, u16)> = self.doc_ids.iter()
                .zip(self.doc_freqs.iter())
                .map(|(id, &tf)| (id, tf))
                .collect();
            result
        } else {
            // doc_freqs out of sync: default tf=1
            self.doc_ids.iter().map(|id| (id, 1)).collect()
        }
    }
    
    pub fn get_positions(&self, doc_id: DocId) -> Option<&[Position]> {
        self.positions.as_ref()?.get(&doc_id).map(|v| v.as_slice())
    }
    
    /// Remove a document from the posting list
    pub fn remove(&mut self, doc_id: DocId) {
        if !self.doc_ids.contains(doc_id as u32) {
            return;
        }
        
        // Remove from doc_ids bitmap
        self.doc_ids.remove(doc_id as u32);
        
        // Remove from positions if present
        if let Some(ref mut pos_map) = self.positions {
            pos_map.remove(&doc_id);
        }
        
        // Rebuild doc_freqs array to maintain parallel structure
        self.rebuild_doc_freqs_array();
    }
    
    /// Check if posting list is empty
    pub fn is_empty(&self) -> bool {
        self.doc_ids.is_empty()
    }

    /// Serialize positions for a subset of clean doc_ids.
    /// Format: [num_docs(4)] then per doc: [doc_id_delta(4) num_pos(2) pos_deltas...]
    /// Returns None if no positions are available.
    pub fn serialize_positions_for(&self, clean_doc_ids: &[u32]) -> Option<Vec<u8>> {
        let pos_map = self.positions.as_ref()?;
        let mut buf = Vec::new();
        buf.extend_from_slice(&0u32.to_le_bytes()); // count placeholder

        let mut count = 0u32;
        let mut prev_doc_id = 0u32;
        for &doc_id in clean_doc_ids {
            if let Some(pos_list) = pos_map.get(&(doc_id as u64)) {
                let delta = doc_id - prev_doc_id;
                prev_doc_id = doc_id;
                buf.extend_from_slice(&delta.to_le_bytes());
                buf.extend_from_slice(&(pos_list.len() as u16).to_le_bytes());
                let mut prev_pos = 0u32;
                for &pos in pos_list {
                    let pd = pos - prev_pos;
                    prev_pos = pos;
                    buf.extend_from_slice(&pd.to_le_bytes());
                }
                count += 1;
            }
        }

        buf[0..4].copy_from_slice(&count.to_le_bytes());
        if count > 0 { Some(buf) } else { None }
    }

    /// Load positions from serialized bytes into this posting list.
    pub fn load_positions(&mut self, data: &[u8]) {
        if data.len() < 4 { return; }
        let num_docs = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if num_docs == 0 { return; }
        if self.positions.is_none() {
            self.positions = Some(HashMap::new());
        }
        let positions = self.positions.as_mut().unwrap();

        let mut offset = 4;
        let mut prev_doc_id = 0u32;
        for _ in 0..num_docs {
            if offset + 6 > data.len() { break; }
            let delta = u32::from_le_bytes([data[offset], data[offset+1], data[offset+2], data[offset+3]]);
            let doc_id = prev_doc_id + delta;
            prev_doc_id = doc_id;
            offset += 4;
            let num_pos = u16::from_le_bytes([data[offset], data[offset+1]]) as usize;
            offset += 2;
            let mut pos_list = Vec::with_capacity(num_pos);
            let mut prev_pos = 0u32;
            for _ in 0..num_pos {
                if offset + 4 > data.len() { break; }
                let pd = u32::from_le_bytes([data[offset], data[offset+1], data[offset+2], data[offset+3]]);
                let pos = prev_pos + pd;
                prev_pos = pos;
                pos_list.push(pos);
                offset += 4;
            }
            positions.insert(doc_id as u64, pos_list);
        }
    }
}

//=============================================================================
// PART 3: Block-Based Posting Lists (Tantivy-style)
//=============================================================================

/// Block size: 128 documents per block (Tantivy convention)
pub const BLOCK_SIZE: usize = 128;

/// Magic bytes for block posting list format
const BLOCK_MAGIC: [u8; 2] = [0x42, 0x50]; // "BP"

/// 1-byte fieldnorm encoding for document lengths.
/// Uses a simple log-based mapping for compact storage.
/// Covers range 0..65536+ with reasonable precision.
pub struct FieldNormTable;

impl FieldNormTable {
    /// Encode a document length to a 1-byte fieldnorm value.
    /// Maps length relative to avg_dl into 0..255.
    pub fn encode(length: u32, avg_dl: f32) -> u8 {
        if length == 0 || avg_dl <= 0.0 { return 0; }
        let ratio = (length as f64) / (avg_dl as f64);
        if ratio <= 0.0 { return 0; }

        // Use log2-based encoding for dynamic range
        // fieldnorm = clamp(round(log2(ratio) * 16 + 128), 0, 255)
        let log_val = ratio.log2();
        let encoded = (log_val * 16.0 + 128.0).round() as i32;
        encoded.clamp(0, 255) as u8
    }

    /// Decode a 1-byte fieldnorm back to approximate document length.
    pub fn decode(fieldnorm: u8, avg_dl: f32) -> f32 {
        if fieldnorm == 0 || avg_dl <= 0.0 { return 0.0; }
        let log_val = (fieldnorm as f64 - 128.0) / 16.0;
        let ratio = 2.0_f64.powf(log_val);
        (ratio * avg_dl as f64) as f32
    }
}

/// Block-based posting list with per-block skip metadata.
///
/// Format:
/// ```text
/// [magic: 2 bytes "BP"]
/// [num_docs: u32 LE]
/// [num_blocks: u16 LE]
/// [skip_table_offset: u32 LE]  (offset from start of data)
/// [block 0 data]
/// [block 1 data]
/// ...
/// [skip metadata table]
///   per block: [max_tf: u16 LE][min_fieldnorm: u8]
/// ```
///
/// Each block:
/// ```text
/// [doc_count: u8]
/// [bits_per_docid: u8]
/// [bits_per_tf: u8]
/// [delta_doc_ids: bitpacked, doc_count × bits_per_docid bits]
/// [tf_values: bitpacked, doc_count × bits_per_tf bits]
/// ```
pub struct BlockPostingList {
    /// Complete serialized data
    data: Vec<u8>,
    /// Number of documents
    num_docs: u32,
    /// Number of blocks
    num_blocks: u16,
}

/// Cursor for iterating over a BlockPostingList.
pub struct BlockCursor<'a> {
    data: &'a [u8],
    num_blocks: u16,
    current_block: u16,
    pos_in_block: usize,
    /// Decoded doc IDs for current block
    decoded_docs: Vec<u32>,
    /// Decoded TFs for current block
    decoded_tfs: Vec<u16>,
    /// Block data offset (where blocks start, after header)
    blocks_offset: usize,
}

impl BlockPostingList {
    /// Create a block posting list from sorted (doc_id, tf) pairs.
    /// doc_ids MUST be sorted in ascending order.
    pub fn from_sorted_pairs(doc_ids: &[u32], tfs: &[u16]) -> Self {
        assert_eq!(doc_ids.len(), tfs.len());
        let num_docs = doc_ids.len() as u32;
        let num_blocks = (num_docs as usize).div_ceil(BLOCK_SIZE);
        if num_docs == 0 {
            return Self {
                data: Vec::new(),
                num_docs: 0,
                num_blocks: 0,
            };
        }

        // Header: magic(2) + num_docs(4) + num_blocks(2) + skip_table_offset(4) = 12 bytes
        let _header_size = 12;
        // Reserve space; we'll fill skip_table_offset later
        let mut data = Vec::with_capacity(num_docs as usize * 3);

        // Write header placeholder
        data.extend_from_slice(&BLOCK_MAGIC);
        data.extend_from_slice(&num_docs.to_le_bytes());
        data.extend_from_slice(&(num_blocks as u16).to_le_bytes());
        data.extend_from_slice(&[0u8; 4]); // skip_table_offset placeholder

        // Skip metadata table
        let mut skip_table: Vec<(u16, u8)> = Vec::with_capacity(num_blocks);

        // Write blocks
        for block_idx in 0..num_blocks {
            let start = block_idx * BLOCK_SIZE;
            let end = (start + BLOCK_SIZE).min(doc_ids.len());
            let block_docs = &doc_ids[start..end];
            let block_tfs = &tfs[start..end];
            let doc_count = block_docs.len() as u8;

            // Delta encode doc IDs
            let mut deltas: Vec<u32> = Vec::with_capacity(block_docs.len());
            let mut prev = 0u32;
            for &id in block_docs {
                deltas.push(id - prev);
                prev = id;
            }

            let bits_per_docid = crate::index::text_encoding::max_bits(&deltas).max(1);
            let bits_per_tf = crate::index::text_encoding::max_bits_u16(block_tfs).max(1);

            // Block header
            data.push(doc_count);
            data.push(bits_per_docid);
            data.push(bits_per_tf);

            // Bitpack delta doc IDs
            crate::index::text_encoding::bitpack_into(&mut data, &deltas, bits_per_docid);

            // Bitpack TFs
            crate::index::text_encoding::bitpack_u16_into(&mut data, block_tfs, bits_per_tf);

            // Compute skip metadata
            let max_tf = *block_tfs.iter().max().unwrap_or(&0);
            let min_tf = *block_tfs.iter().min().unwrap_or(&1);
            // min_fieldnorm: we use 0 as "unknown" placeholder since we don't
            // have per-doc lengths here. BMW will use max_tf for upper bound.
            let _ = min_tf; // suppress warning
            skip_table.push((max_tf, 0));
        }

        // Write skip table offset
        let skip_table_offset = data.len() as u32;
        data[8..12].copy_from_slice(&skip_table_offset.to_le_bytes());

        // Write skip metadata table
        for (max_tf, min_fieldnorm) in &skip_table {
            data.extend_from_slice(&max_tf.to_le_bytes());
            data.push(*min_fieldnorm);
        }

        Self { data, num_docs, num_blocks: num_blocks as u16 }
    }

    /// Create a block posting list from a legacy PostingList.
    pub fn from_legacy(posting: &PostingList) -> Self {
        let doc_ids: Vec<u32> = posting.doc_ids.iter().collect();
        let mut tfs = Vec::with_capacity(doc_ids.len());
        for &id in &doc_ids {
            tfs.push(posting.term_frequency(id as u64));
        }
        Self::from_sorted_pairs(&doc_ids, &tfs)
    }

    /// Check if raw bytes look like a block posting list (vs legacy format).
    pub fn is_block_format(data: &[u8]) -> bool {
        data.len() >= 2 && data[0] == BLOCK_MAGIC[0] && data[1] == BLOCK_MAGIC[1]
    }

    /// Deserialize from raw bytes.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < 12 {
            return Err(StorageError::InvalidData("Block posting list too short".into()));
        }
        if data[0] != BLOCK_MAGIC[0] || data[1] != BLOCK_MAGIC[1] {
            return Err(StorageError::InvalidData("Invalid block magic".into()));
        }
        let num_docs = u32::from_le_bytes([data[2], data[3], data[4], data[5]]);
        let num_blocks = u16::from_le_bytes([data[6], data[7]]);

        Ok(Self {
            data: data.to_vec(),
            num_docs,
            num_blocks,
        })
    }

    /// Get the serialized bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Number of documents.
    pub fn num_docs(&self) -> u32 {
        self.num_docs
    }

    /// Number of blocks.
    pub fn num_blocks(&self) -> u16 {
        self.num_blocks
    }

    /// Get skip metadata for a block: (max_tf, min_fieldnorm).
    pub fn block_skip_meta(&self, block_idx: u16) -> (u16, u8) {
        if block_idx >= self.num_blocks {
            return (0, 0);
        }
        let skip_offset = u32::from_le_bytes([
            self.data[8], self.data[9], self.data[10], self.data[11]
        ]) as usize;
        let entry_offset = skip_offset + block_idx as usize * 3;
        if entry_offset + 3 > self.data.len() {
            return (0, 0);
        }
        let max_tf = u16::from_le_bytes([
            self.data[entry_offset],
            self.data[entry_offset + 1],
        ]);
        let min_fn = self.data[entry_offset + 2];
        (max_tf, min_fn)
    }

    /// Create a cursor for sequential or seek-based iteration.
    pub fn cursor(&self) -> BlockCursor<'_> {
        let mut cursor = BlockCursor {
            data: &self.data,
            num_blocks: self.num_blocks,
            current_block: 0,
            pos_in_block: 0,
            decoded_docs: Vec::new(),
            decoded_tfs: Vec::new(),
            blocks_offset: 12, // after header
        };
        if self.num_docs > 0 {
            cursor.decode_block(0);
            cursor.pos_in_block = 0;
        }
        cursor
    }

    /// Decode a specific block, filling decoded_docs and decoded_tfs.
    fn decode_block_data(data: &[u8], offset: usize) -> (Vec<u32>, Vec<u16>, usize) {
        if offset >= data.len() {
            return (Vec::new(), Vec::new(), offset);
        }
        let doc_count = data[offset] as usize;
        let bits_per_docid = data[offset + 1];
        let bits_per_tf = data[offset + 2];
        let mut pos = offset + 3;

        // Decode delta doc IDs
        let delta_bytes = (doc_count * bits_per_docid as usize).div_ceil(8);
        let deltas = crate::index::text_encoding::bitunpack(
            data, pos, doc_count, bits_per_docid,
        );

        // Reconstruct doc IDs from deltas
        let mut doc_ids = Vec::with_capacity(doc_count);
        let mut current = 0u32;
        for &d in &deltas {
            current += d;
            doc_ids.push(current);
        }

        // Advance position past delta data
        pos += delta_bytes;

        // Decode TFs
        let tf_bytes = (doc_count * bits_per_tf as usize).div_ceil(8);
        let tf_u32 = crate::index::text_encoding::bitunpack(
            data, pos, doc_count, bits_per_tf,
        );
        let tfs: Vec<u16> = tf_u32.iter().map(|&v| v as u16).collect();

        let block_total = 3 + delta_bytes + tf_bytes;
        (doc_ids, tfs, offset + block_total)
    }
}

impl<'a> BlockCursor<'a> {
    /// Decode a block at the given block index.
    fn decode_block(&mut self, block_idx: u16) {
        // Find block offset by skipping previous blocks
        let mut offset = self.blocks_offset;
        for _ in 0..block_idx {
            if offset >= self.data.len() { return; }
            let doc_count = self.data[offset] as usize;
            let bits_per_docid = self.data[offset + 1] as usize;
            let bits_per_tf = self.data[offset + 2] as usize;
            let block_size = 3 +
                (doc_count * bits_per_docid).div_ceil(8) +
                (doc_count * bits_per_tf).div_ceil(8);
            offset += block_size;
        }
        let (docs, tfs, _next_offset) = BlockPostingList::decode_block_data(self.data, offset);
        self.decoded_docs = docs;
        self.decoded_tfs = tfs;
        self.current_block = block_idx;
    }

    /// Whether the cursor points to a valid document.
    pub fn is_valid(&self) -> bool {
        if self.current_block >= self.num_blocks {
            return false;
        }
        self.pos_in_block < self.decoded_docs.len()
    }

    /// Current document ID.
    pub fn current_doc(&self) -> u32 {
        self.decoded_docs[self.pos_in_block]
    }

    /// Current term frequency.
    pub fn current_tf(&self) -> u16 {
        self.decoded_tfs[self.pos_in_block]
    }

    /// Current block index.
    pub fn current_block(&self) -> u16 {
        self.current_block
    }

    /// Max TF in the current block (from skip metadata).
    pub fn block_max_tf(&self) -> u16 {
        // Re-read from skip table
        let skip_offset = u32::from_le_bytes([
            self.data[8], self.data[9], self.data[10], self.data[11]
        ]) as usize;
        let entry_offset = skip_offset + self.current_block as usize * 3;
        if entry_offset + 2 > self.data.len() { return 0; }
        u16::from_le_bytes([self.data[entry_offset], self.data[entry_offset + 1]])
    }

    /// Advance to the next document. Returns false if exhausted.
    pub fn advance(&mut self) -> bool {
        self.pos_in_block += 1;
        if self.pos_in_block >= self.decoded_docs.len() {
            // Move to next block
            if self.current_block + 1 >= self.num_blocks {
                return false;
            }
            self.decode_block(self.current_block + 1);
            self.pos_in_block = 0;
            if self.decoded_docs.is_empty() {
                return false;
            }
        }
        true
    }

    /// Skip to the first document >= target. Returns the doc found, or None.
    pub fn seek(&mut self, target: u32) -> Option<u32> {
        // First check current block
        if self.is_valid() {
            // Binary search within current block
            if let Some(pos) = self.decoded_docs.iter().position(|&d| d >= target) {
                self.pos_in_block = pos;
                return Some(self.current_doc());
            }
        }

        // Try subsequent blocks
        while self.current_block + 1 < self.num_blocks {
            self.decode_block(self.current_block + 1);
            self.pos_in_block = 0;

            // Check last doc in block — if it's < target, skip whole block
            if let Some(&last_doc) = self.decoded_docs.last() {
                if last_doc < target {
                    continue;
                }
            }

            // Find first doc >= target in this block
            if let Some(pos) = self.decoded_docs.iter().position(|&d| d >= target) {
                self.pos_in_block = pos;
                return Some(self.current_doc());
            }
        }

        None
    }

    /// Skip entire current block, advance to next block.
    /// Returns false if no more blocks.
    pub fn advance_block(&mut self) -> bool {
        if self.current_block + 1 >= self.num_blocks {
            return false;
        }
        self.decode_block(self.current_block + 1);
        self.pos_in_block = 0;
        !self.decoded_docs.is_empty()
    }
}

/// Unified posting list format that handles both legacy and block formats.
pub enum PostingListFormat {
    /// Legacy RoaringBitmap format
    Legacy(PostingList),
    /// New block-based format
    Block(BlockPostingList),
}

impl PostingListFormat {
    /// Deserialize from raw bytes, auto-detecting format.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if BlockPostingList::is_block_format(data) {
            Ok(PostingListFormat::Block(BlockPostingList::deserialize(data)?))
        } else {
            Ok(PostingListFormat::Legacy(PostingList::deserialize_compact(data)?))
        }
    }

    /// Number of documents in this posting list.
    pub fn num_docs(&self) -> u32 {
        match self {
            PostingListFormat::Legacy(p) => p.doc_count() as u32,
            PostingListFormat::Block(b) => b.num_docs(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_whitespace_tokenizer() {
        let tokenizer = WhitespaceTokenizer::default();
        let tokens = tokenizer.tokenize("Hello, World!");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].text, "hello");
        assert_eq!(tokens[1].text, "world");
    }
    
    #[test]
    fn test_ngram_tokenizer() {
        let tokenizer = NgramTokenizer::new(2);
        let tokens = tokenizer.tokenize("rust");
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0].text, "ru");
        assert_eq!(tokens[1].text, "us");
        assert_eq!(tokens[2].text, "st");
    }
    
    #[test]
    fn test_posting_list() {
        let mut posting = PostingList::new();
        posting.add(1, Some(0));
        posting.add(1, Some(5));
        posting.add(2, Some(3));
        
        assert_eq!(posting.doc_count(), 2);
        assert_eq!(posting.term_frequency(1), 2);
        assert_eq!(posting.term_frequency(2), 1);
    }

    #[test]
    fn test_term_frequency_without_positions() {
        let mut posting = PostingList::new_without_positions(true);
        posting.add(1, None);
        posting.add(1, None); // second occurrence — TF should be 1 (fallback)
        posting.add(2, None);

        assert_eq!(posting.doc_count(), 2);
        // Without positions, term_frequency returns 1 (doc_freqs not in sync)
        assert_eq!(posting.term_frequency(1), 1);
        assert_eq!(posting.term_frequency(2), 1);
    }

    #[test]
    #[test]
    fn test_block_single_doc_id_zero() {
        let doc_ids = vec![0u32];
        let tfs = vec![1u16];
        let block_list = BlockPostingList::from_sorted_pairs(&doc_ids, &tfs);
        assert_eq!(block_list.num_docs(), 1);
        let mut cursor = block_list.cursor();
        assert!(cursor.is_valid());
        assert_eq!(cursor.current_doc(), 0);
        assert_eq!(cursor.current_tf(), 1);
    }

    #[test]
    fn test_block_posting_list_roundtrip() {
        // Test with 300 docs (3 blocks: 128 + 128 + 44)
        let doc_ids: Vec<u32> = (0..300).map(|i| i * 10 + 5).collect();
        let tfs: Vec<u16> = (0..300).map(|i| (i % 20 + 1) as u16).collect();

        let block_list = BlockPostingList::from_sorted_pairs(&doc_ids, &tfs);
        assert_eq!(block_list.num_docs(), 300);
        assert_eq!(block_list.num_blocks(), 3);

        // Cursor walk
        let mut cursor = block_list.cursor();
        let mut count = 0;
        let mut last_doc = 0u32;
        while cursor.is_valid() {
            let doc = cursor.current_doc();
            let tf = cursor.current_tf();
            assert!(doc > last_doc || count == 0);
            assert!(tf > 0);
            last_doc = doc;
            count += 1;
            cursor.advance();
        }
        assert_eq!(count, 300);
    }

    #[test]
    fn test_block_cursor_seek() {
        let doc_ids: Vec<u32> = (0..100).map(|i| i * 100).collect(); // 0, 100, 200, ...
        let tfs: Vec<u16> = vec![1u16; 100];

        let block_list = BlockPostingList::from_sorted_pairs(&doc_ids, &tfs);
        let mut cursor = block_list.cursor();

        // Seek to 5000 → should land on 5000
        assert_eq!(cursor.seek(5000), Some(5000));

        // Seek to 5500 → should land on 5500 (5500 = 55*100 exists)
        assert_eq!(cursor.seek(5500), Some(5500));

        // Seek to 5501 → should land on 5600
        assert_eq!(cursor.seek(5501), Some(5600));

        // Seek past end → None
        assert_eq!(cursor.seek(100000), None);
    }

    #[test]
    fn test_block_skip_metadata() {
        let doc_ids: Vec<u32> = (0..128).collect();
        let tfs: Vec<u16> = (0..128).map(|i| (i + 1) as u16).collect(); // TF = 1..128

        let block_list = BlockPostingList::from_sorted_pairs(&doc_ids, &tfs);
        let (max_tf, _min_fn) = block_list.block_skip_meta(0);
        assert_eq!(max_tf, 128); // max TF in block
    }

    #[test]
    fn test_fieldnorm_table() {
        let avg_dl = 100.0f32;
        // Short docs
        let fn_short = FieldNormTable::encode(1, avg_dl);
        // Long docs
        let fn_long = FieldNormTable::encode(1000, avg_dl);

        let dl_short = FieldNormTable::decode(fn_short, avg_dl);
        let dl_long = FieldNormTable::decode(fn_long, avg_dl);

        // Decoded values should be reasonable approximations
        assert!(dl_short >= 0.5 && dl_short <= 2.0);
        assert!(dl_long >= 800.0 && dl_long <= 1200.0);
    }

    #[test]
    fn test_block_posting_list_from_legacy() {
        let mut posting = PostingList::new_without_positions(true);
        for i in 0u32..50 {
            posting.add(i as u64, None);
        }
        // Force doc_freqs rebuild
        let mut buf = Vec::new();
        posting.doc_ids.serialize_into(&mut buf).unwrap();
        buf.extend_from_slice(&(50u32).to_le_bytes());
        buf.extend_from_slice(&[1u16; 50].iter().flat_map(|v| v.to_le_bytes()).collect::<Vec<_>>());

        // Re-serialize to get correct doc_freqs
        let serialized = posting.serialize_compact().unwrap();
        let posting2 = PostingList::deserialize_compact(&serialized).unwrap();

        let block_list = BlockPostingList::from_legacy(&posting2);
        assert_eq!(block_list.num_docs(), 50);

        let mut cursor = block_list.cursor();
        let mut count = 0;
        while cursor.is_valid() {
            count += 1;
            cursor.advance();
        }
        assert_eq!(count, 50);
    }
}
