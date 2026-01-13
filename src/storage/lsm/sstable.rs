//! SSTable: Sorted String Table (persistent storage)
//!
//! ## File Format
//! ```text
//! [Data Block 1 (compressed)] [Data Block 2 (compressed)] ... [Data Block N]
//! [Index Block]
//! [Bloom Filter]
//! [Footer]
//! ```text
//!
//! ## Compression
//! - Algorithm: Snappy (fast, ~2.5-3x ratio)
//! - Granularity: Block-level (64KB blocks)
//! - Trade-off: CPU vs I/O (compression reduces disk I/O)
//!
//! ## Performance
//! - Read: O(log n) with Bloom filter
//! - Compression: 2.5-3:1 ratio (Snappy on 64KB blocks)
//! - Block size: 64KB

use super::{Key, Value, BloomFilter, LSMConfig, ValueData, BlobRef};
use crate::{Result, StorageError};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom, BufWriter, BufReader};
use std::path::{Path, PathBuf};

/// Magic number for SSTable files (ASCII "LSMT")
const SSTABLE_MAGIC: u32 = 0x4C534D54;

/// SSTable version
const SSTABLE_VERSION: u32 = 1;

/// SSTable (read-only)
pub struct SSTable {
    /// File path
    path: PathBuf,
    
    /// File handle
    file: File,
    
    /// Block index (first_key -> offset)
    index: BlockIndex,
    
    /// Bloom filter
    bloom: BloomFilter,
    
    /// Footer metadata
    footer: Footer,
}

/// Block index for binary search
#[derive(Clone, Debug)]
pub struct BlockIndex {
    /// Entries: (first_key, offset, size)
    entries: Vec<(Key, u64, u32)>,
}

/// SSTable footer (stored at end of file)
#[derive(Clone, Debug)]
struct Footer {
    magic: u32,
    version: u32,
    index_offset: u64,
    index_size: u32,
    bloom_offset: u64,
    bloom_size: u32,
    num_entries: u64,
    min_timestamp: u64,
    max_timestamp: u64,
}

impl SSTable {
    /// Open an existing SSTable
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .read(true)
            .open(&path)?;
        
        // Read footer
        let footer = Self::read_footer(&mut file)?;
        
        // Read index
        file.seek(SeekFrom::Start(footer.index_offset))?;
        let mut index_buf = vec![0u8; footer.index_size as usize];
        file.read_exact(&mut index_buf)?;
        let index = BlockIndex::deserialize(&index_buf)?;
        
        // Read bloom filter
        file.seek(SeekFrom::Start(footer.bloom_offset))?;
        let mut bloom_buf = vec![0u8; footer.bloom_size as usize];
        file.read_exact(&mut bloom_buf)?;
        let bloom = BloomFilter::from_bytes_full(&bloom_buf)
            .ok_or_else(|| StorageError::InvalidData("Invalid Bloom filter".into()))?;
        
        Ok(Self {
            path,
            file,
            index,
            bloom,
            footer,
        })
    }
    
    /// Get a value by key
    pub fn get(&mut self, key: Key) -> Result<Option<Value>> {
        // Convert u64 key to bytes for bloom filter and block search
        let key_bytes = key.to_be_bytes();
        
        // Fast negative lookup
        if !self.bloom.may_contain(&key_bytes) {
            return Ok(None);
        }
        
        // Binary search in index
        let block_entry = match self.index.find_block(&key_bytes) {
            Some(entry) => entry,
            None => return Ok(None),
        };
        
        // Read and search block
        self.file.seek(SeekFrom::Start(block_entry.1))?;
        let mut block_buf = vec![0u8; block_entry.2 as usize];
        self.file.read_exact(&mut block_buf)?;
        
        let block = DataBlock::deserialize(&block_buf)?;
        Ok(block.get(&key_bytes))
    }
    
    /// Scan a range [start, end)
    pub fn scan(&mut self, start: Key, end: Key) -> Result<Vec<(Key, Value)>> {
        // 🚀 P3 优化：预分配容量（估算范围大小）
        let estimated_size = ((end - start) as usize).min(1000);
        let mut results = Vec::with_capacity(estimated_size);
        
        let start_bytes = start.to_be_bytes();
        
        // Find starting block
        let start_idx = self.index.find_block_index(&start_bytes);
        
        // Scan blocks
        for i in start_idx..self.index.entries.len() {
            let (_, offset, size) = &self.index.entries[i];
            
            self.file.seek(SeekFrom::Start(*offset))?;
            let mut block_buf = vec![0u8; *size as usize];
            self.file.read_exact(&mut block_buf)?;
            
            let block = DataBlock::deserialize(&block_buf)?;
            
            for (k, v) in block.entries.iter() {
                // Check if key is in range [start, end)
                if k >= &start && k < &end {
                    results.push((*k, v.clone()));
                }
                if k >= &end {
                    return Ok(results);
                }
            }
        }
        
        Ok(results)
    }
    
    /// 🆕 Scan all entries in SSTable
    /// 
    /// Used by scan_prefix() to scan entire table and filter by prefix
    pub fn scan_all(&mut self) -> Result<Vec<(Key, Value)>> {
        // Estimate capacity based on footer
        let estimated_size = (self.footer.num_entries as usize).min(10000);
        let mut results = Vec::with_capacity(estimated_size);
        
        // Scan all blocks
        for (_first_key, offset, size) in &self.index.entries {
            self.file.seek(SeekFrom::Start(*offset))?;
            let mut block_buf = vec![0u8; *size as usize];
            self.file.read_exact(&mut block_buf)?;
            
            let block = DataBlock::deserialize(&block_buf)?;
            
            // Add all entries from this block
            for (k, v) in block.entries.iter() {
                results.push((*k, v.clone()));
            }
        }
        
        Ok(results)
    }
    
    /// Get file path
    pub fn path(&self) -> &Path {
        &self.path
    }
    
    /// Iterate over all entries in this SSTable
    pub fn iter(&mut self) -> Result<SSTableIterator> {
        SSTableIterator::new(self)
    }
    
    /// Get statistics
    pub fn stats(&self) -> SSTableStats {
        SSTableStats {
            num_entries: self.footer.num_entries,
            file_size: std::fs::metadata(&self.path)
                .map(|m| m.len())
                .unwrap_or(0),
            num_blocks: self.index.entries.len(),
            min_timestamp: self.footer.min_timestamp,
            max_timestamp: self.footer.max_timestamp,
        }
    }
    
    // Internal helper
    fn read_footer(file: &mut File) -> Result<Footer> {
        let file_size = file.metadata()?.len();
        if file_size < 64 {
            return Err(StorageError::InvalidData("File too small".into()));
        }
        
        // Footer is last 64 bytes
        file.seek(SeekFrom::End(-64))?;
        let mut buf = [0u8; 64];
        file.read_exact(&mut buf)?;
        
        Footer::deserialize(&buf)
    }
}

/// SSTable builder (write-only)
pub struct SSTableBuilder {
    /// Output file
    writer: BufWriter<File>,
    
    /// File path (store separately)
    path: PathBuf,
    
    /// Current block
    current_block: DataBlock,
    
    /// Block index being built
    index: BlockIndex,
    
    /// Bloom filter
    bloom: BloomFilter,
    
    /// Configuration
    config: LSMConfig,
    
    /// Statistics
    num_entries: u64,
    min_timestamp: u64,
    max_timestamp: u64,
    
    /// 🔧 Key range tracking
    min_key: Option<Key>,
    max_key: Option<Key>,
    
    /// Current file offset
    offset: u64,
}

impl SSTableBuilder {
    /// Create a new SSTable builder
    pub fn new<P: AsRef<Path>>(path: P, config: LSMConfig, estimated_keys: usize) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path_buf)?;
        
        Ok(Self {
            // 🚀 P1 优化：增大 BufWriter 容量到 64KB（减少系统调用）
            writer: BufWriter::with_capacity(64 * 1024, file),
            path: path_buf,
            current_block: DataBlock::new(),
            index: BlockIndex::new(),
            bloom: BloomFilter::new(estimated_keys, config.bloom_bits_per_key),
            config,
            num_entries: 0,
            min_timestamp: u64::MAX,
            max_timestamp: 0,
            min_key: None,
            max_key: None,
            offset: 0,
        })
    }
    
    /// Add a key-value pair (must be in sorted order)
    pub fn add(&mut self, key: Key, value: Value) -> Result<()> {
        // Update bloom filter (convert u64 to bytes)
        self.bloom.insert(&key.to_be_bytes());
        
        // Update statistics
        self.num_entries += 1;
        self.min_timestamp = self.min_timestamp.min(value.timestamp);
        self.max_timestamp = self.max_timestamp.max(value.timestamp);
        
        // 🔧 Track min/max keys
        if self.min_key.is_none() {
            self.min_key = Some(key);
        }
        self.max_key = Some(key);
        
        // Add to current block
        self.current_block.add(key, value)?;
        
        // Flush block if full
        if self.current_block.size() >= self.config.block_size {
            self.flush_block()?;
        }
        
        Ok(())
    }
    
    /// Finish building and write footer
    pub fn finish(mut self) -> Result<super::compaction::SSTableMeta> {
        // Flush last block
        if !self.current_block.is_empty() {
            self.flush_block()?;
        }
        
        // 🔧 Use tracked min/max keys
        let min_key = self.min_key.unwrap_or_default();
        let max_key = self.max_key.unwrap_or_default();
        
        // Write index
        let index_offset = self.offset;
        let index_data = self.index.serialize()?;
        let index_size = index_data.len() as u32;
        self.writer.write_all(&index_data)?;
        self.offset += index_size as u64;
        
        // Write bloom filter
        let bloom_offset = self.offset;
        let bloom_data = self.bloom.to_bytes();
        let bloom_size = bloom_data.len() as u32;
        self.writer.write_all(&bloom_data)?;
        self.offset += bloom_size as u64;
        
        // Write footer
        let footer = Footer {
            magic: SSTABLE_MAGIC,
            version: SSTABLE_VERSION,
            index_offset,
            index_size,
            bloom_offset,
            bloom_size,
            num_entries: self.num_entries,
            min_timestamp: if self.min_timestamp == u64::MAX { 0 } else { self.min_timestamp },
            max_timestamp: self.max_timestamp,
        };
        
        let footer_data = footer.serialize()?;
        self.writer.write_all(&footer_data)?;
        
        // 🚀 P0 优化：确保数据持久化到磁盘
        self.writer.flush()?;  // 刷新 BufWriter 到 OS
        self.writer.get_mut().sync_data()?;  // fsync 数据到磁盘（不同步元数据，更快）
        
        // Get file size
        let file_size = self.offset + footer_data.len() as u64;
        
        // Return metadata
        Ok(super::compaction::SSTableMeta {
            path: self.path,
            size: file_size,
            num_entries: self.num_entries,
            min_key,
            max_key,
            min_timestamp: if self.min_timestamp == u64::MAX { 0 } else { self.min_timestamp },
            max_timestamp: self.max_timestamp,
        })
    }
    
    // Internal helper
    fn flush_block(&mut self) -> Result<()> {
        if self.current_block.is_empty() {
            return Ok(());
        }
        
        let first_key = self.current_block.entries.first()
            .map(|(k, _)| *k)  // ✅ u64 copy is cheap, no clone()
            .ok_or_else(|| StorageError::InvalidData("Empty block".into()))?;
        
        // Serialize with compression
        let block_data = self.current_block.serialize_compressed(self.config.enable_compression)?;
        let block_size = block_data.len() as u32;
        
        // Record in index
        self.index.entries.push((first_key, self.offset, block_size));
        
        // Write to file
        self.writer.write_all(&block_data)?;
        self.offset += block_size as u64;
        
        // Reset block
        self.current_block = DataBlock::new();
        
        Ok(())
    }
}

/// Data block (in-memory representation)
struct DataBlock {
    entries: Vec<(Key, Value)>,
}

impl DataBlock {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
    
    fn add(&mut self, key: Key, value: Value) -> Result<()> {
        self.entries.push((key, value));
        Ok(())
    }
    
    fn get(&self, key_bytes: &[u8]) -> Option<Value> {
        // Convert bytes back to u64 for comparison
        if key_bytes.len() != 8 {
            return None;
        }
        let key = u64::from_be_bytes([
            key_bytes[0], key_bytes[1], key_bytes[2], key_bytes[3],
            key_bytes[4], key_bytes[5], key_bytes[6], key_bytes[7],
        ]);
        
        // Binary search
        self.entries.binary_search_by_key(&key, |(k, _)| *k)
            .ok()
            .map(|idx| self.entries[idx].1.clone())
    }
    
    fn size(&self) -> usize {
        self.entries.iter()
            .map(|(_, v)| 8 + v.data.len() + 24)  // u64 key is always 8 bytes
            .sum()
    }
    
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    
    fn serialize(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        
        // Number of entries
        buf.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        
        // Entries
        for (key, value) in &self.entries {
            // Key as u64 (8 bytes, BIG-ENDIAN for ordering)
            buf.extend_from_slice(&key.to_be_bytes());
            
            // Value metadata
            buf.extend_from_slice(&value.timestamp.to_le_bytes());
            buf.extend_from_slice(&[if value.deleted { 1 } else { 0 }]);
            
            // Value data (inline or blob ref)
            match &value.data {
                ValueData::Inline(data) => {
                    // Type: 0 = inline
                    buf.push(0);
                    buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
                    buf.extend_from_slice(data);
                }
                ValueData::Blob(blob_ref) => {
                    // Type: 1 = blob
                    buf.push(1);
                    buf.extend_from_slice(&blob_ref.file_id.to_le_bytes());
                    buf.extend_from_slice(&blob_ref.offset.to_le_bytes());
                    buf.extend_from_slice(&blob_ref.size.to_le_bytes());
                }
            }
        }
        
        Ok(buf)
    }
    
    fn serialize_compressed(&self, enable_compression: bool) -> Result<Vec<u8>> {
        let uncompressed = self.serialize()?;
        
        if !enable_compression || uncompressed.len() < 1024 {
            // Very small blocks: compression overhead > benefit
            // Prepend flag: 0 = uncompressed
            let mut result = vec![0u8];
            result.extend_from_slice(&uncompressed);
            return Ok(result);
        }
        
        // Snappy compression
        let mut encoder = snap::raw::Encoder::new();
        let compressed = encoder.compress_vec(&uncompressed)
            .map_err(|e| StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::Other, 
                format!("Compression failed: {}", e)
            )))?;
        
        // Only use compressed if it's actually smaller
        if compressed.len() < uncompressed.len() {
            // Prepend flag: 1 = compressed
            let mut result = vec![1u8];
            result.extend_from_slice(&compressed);
            Ok(result)
        } else {
            // Prepend flag: 0 = uncompressed
            let mut result = vec![0u8];
            result.extend_from_slice(&uncompressed);
            Ok(result)
        }
    }
    
    fn deserialize(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(StorageError::InvalidData("Empty block data".into()));
        }
        
        // Check compression flag (first byte)
        let compression_flag = data[0];
        let actual_data = &data[1..];
        
        let uncompressed = match compression_flag {
            0 => {
                // Uncompressed
                actual_data.to_vec()
            }
            1 => {
                // Compressed with Snappy
                let mut decoder = snap::raw::Decoder::new();
                decoder.decompress_vec(actual_data)
                    .map_err(|e| StorageError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Decompression failed: {}", e)
                    )))?
            }
            _ => {
                return Err(StorageError::InvalidData(
                    format!("Unknown compression flag: {}", compression_flag)
                ));
            }
        };
        
        // Now deserialize the uncompressed data
        Self::deserialize_raw(&uncompressed)
    }
    
    fn deserialize_raw(data: &[u8]) -> Result<Self> {
        let mut offset = 0;
        
        // Read number of entries
        if data.len() < 4 {
            return Err(StorageError::InvalidData("Block too small".into()));
        }
        let num_entries = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        offset += 4;
        
        let mut entries = Vec::with_capacity(num_entries);
        
        for _ in 0..num_entries {
            // Read key as u64 (8 bytes, BIG-ENDIAN)
            if offset + 8 > data.len() {
                return Err(StorageError::InvalidData("Insufficient data for key".into()));
            }
            let key = u64::from_be_bytes([
                data[offset], data[offset+1], data[offset+2], data[offset+3],
                data[offset+4], data[offset+5], data[offset+6], data[offset+7],
            ]);
            offset += 8;
            
            // Read value metadata
            let timestamp = u64::from_le_bytes([
                data[offset], data[offset+1], data[offset+2], data[offset+3],
                data[offset+4], data[offset+5], data[offset+6], data[offset+7],
            ]);
            offset += 8;
            let deleted = data[offset] != 0;
            offset += 1;
            
            // Read value data (inline or blob)
            let value_type = data[offset];
            offset += 1;
            
            let value_data = match value_type {
                0 => {
                    // Inline
                    let value_len = u32::from_le_bytes([
                        data[offset], data[offset+1], data[offset+2], data[offset+3]
                    ]) as usize;
                    offset += 4;
                    let inline_data = data[offset..offset+value_len].to_vec();
                    offset += value_len;
                    ValueData::Inline(inline_data)
                }
                1 => {
                    // Blob reference
                    let file_id = u32::from_le_bytes([
                        data[offset], data[offset+1], data[offset+2], data[offset+3]
                    ]);
                    offset += 4;
                    let blob_offset = u64::from_le_bytes([
                        data[offset], data[offset+1], data[offset+2], data[offset+3],
                        data[offset+4], data[offset+5], data[offset+6], data[offset+7],
                    ]);
                    offset += 8;
                    let size = u32::from_le_bytes([
                        data[offset], data[offset+1], data[offset+2], data[offset+3]
                    ]);
                    offset += 4;
                    ValueData::Blob(BlobRef {
                        file_id,
                        offset: blob_offset,
                        size,
                    })
                }
                _ => {
                    return Err(StorageError::InvalidData(format!("Unknown value type: {}", value_type)));
                }
            };
            
            entries.push((key, Value {
                data: value_data,
                timestamp,
                deleted,
            }));
        }
        
        Ok(Self { entries })
    }
}

impl BlockIndex {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
    
    fn find_block(&self, key_bytes: &[u8]) -> Option<&(Key, u64, u32)> {
        if key_bytes.len() != 8 {
            return None;
        }
        
        // Convert bytes to u64 for comparison
        let key = u64::from_be_bytes([
            key_bytes[0], key_bytes[1], key_bytes[2], key_bytes[3],
            key_bytes[4], key_bytes[5], key_bytes[6], key_bytes[7],
        ]);
        
        // Binary search for the block that might contain this key
        match self.entries.binary_search_by(|(k, _, _)| k.cmp(&key)) {
            Ok(idx) => Some(&self.entries[idx]),
            Err(idx) => {
                if idx == 0 {
                    None
                } else {
                    Some(&self.entries[idx - 1])
                }
            }
        }
    }
    
    fn find_block_index(&self, key_bytes: &[u8]) -> usize {
        if key_bytes.len() != 8 {
            return 0;
        }
        
        let key = u64::from_be_bytes([
            key_bytes[0], key_bytes[1], key_bytes[2], key_bytes[3],
            key_bytes[4], key_bytes[5], key_bytes[6], key_bytes[7],
        ]);
        
        match self.entries.binary_search_by(|(k, _, _)| k.cmp(&key)) {
            Ok(idx) => idx,
            Err(idx) => if idx == 0 { 0 } else { idx - 1 },
        }
    }
    
    fn serialize(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        
        // Number of entries
        buf.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        
        for (key, offset, size) in &self.entries {
            // Serialize u64 key as BIG-ENDIAN (for ordering)
            buf.extend_from_slice(&key.to_be_bytes());
            buf.extend_from_slice(&offset.to_le_bytes());
            buf.extend_from_slice(&size.to_le_bytes());
        }
        
        Ok(buf)
    }
    
    fn deserialize(data: &[u8]) -> Result<Self> {
        let mut offset = 0;
        
        let num_entries = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        offset += 4;
        
        let mut entries = Vec::with_capacity(num_entries);
        
        for _ in 0..num_entries {
            // Read u64 key (8 bytes, BIG-ENDIAN)
            let key = u64::from_be_bytes([
                data[offset], data[offset+1], data[offset+2], data[offset+3],
                data[offset+4], data[offset+5], data[offset+6], data[offset+7],
            ]);
            offset += 8;
            
            let block_offset = u64::from_le_bytes([
                data[offset], data[offset+1], data[offset+2], data[offset+3],
                data[offset+4], data[offset+5], data[offset+6], data[offset+7],
            ]);
            offset += 8;
            
            let block_size = u32::from_le_bytes([
                data[offset], data[offset+1], data[offset+2], data[offset+3]
            ]);
            offset += 4;
            
            entries.push((key, block_offset, block_size));
        }
        
        Ok(Self { entries })
    }
}

impl Footer {
    fn serialize(&self) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; 64];
        let mut offset = 0;
        
        buf[offset..offset+4].copy_from_slice(&self.magic.to_le_bytes());
        offset += 4;
        buf[offset..offset+4].copy_from_slice(&self.version.to_le_bytes());
        offset += 4;
        buf[offset..offset+8].copy_from_slice(&self.index_offset.to_le_bytes());
        offset += 8;
        buf[offset..offset+4].copy_from_slice(&self.index_size.to_le_bytes());
        offset += 4;
        buf[offset..offset+8].copy_from_slice(&self.bloom_offset.to_le_bytes());
        offset += 8;
        buf[offset..offset+4].copy_from_slice(&self.bloom_size.to_le_bytes());
        offset += 4;
        buf[offset..offset+8].copy_from_slice(&self.num_entries.to_le_bytes());
        offset += 8;
        buf[offset..offset+8].copy_from_slice(&self.min_timestamp.to_le_bytes());
        offset += 8;
        buf[offset..offset+8].copy_from_slice(&self.max_timestamp.to_le_bytes());
        
        Ok(buf)
    }
    
    fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < 64 {
            return Err(StorageError::InvalidData("Footer too small".into()));
        }
        
        let mut offset = 0;
        
        let magic = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        offset += 4;
        
        if magic != SSTABLE_MAGIC {
            return Err(StorageError::InvalidData("Invalid SSTable magic".into()));
        }
        
        let version = u32::from_le_bytes([data[offset], data[offset+1], data[offset+2], data[offset+3]]);
        offset += 4;
        
        let index_offset = u64::from_le_bytes([
            data[offset], data[offset+1], data[offset+2], data[offset+3],
            data[offset+4], data[offset+5], data[offset+6], data[offset+7],
        ]);
        offset += 8;
        
        let index_size = u32::from_le_bytes([
            data[offset], data[offset+1], data[offset+2], data[offset+3]
        ]);
        offset += 4;
        
        let bloom_offset = u64::from_le_bytes([
            data[offset], data[offset+1], data[offset+2], data[offset+3],
            data[offset+4], data[offset+5], data[offset+6], data[offset+7],
        ]);
        offset += 8;
        
        let bloom_size = u32::from_le_bytes([
            data[offset], data[offset+1], data[offset+2], data[offset+3]
        ]);
        offset += 4;
        
        let num_entries = u64::from_le_bytes([
            data[offset], data[offset+1], data[offset+2], data[offset+3],
            data[offset+4], data[offset+5], data[offset+6], data[offset+7],
        ]);
        offset += 8;
        
        let min_timestamp = u64::from_le_bytes([
            data[offset], data[offset+1], data[offset+2], data[offset+3],
            data[offset+4], data[offset+5], data[offset+6], data[offset+7],
        ]);
        offset += 8;
        
        let max_timestamp = u64::from_le_bytes([
            data[offset], data[offset+1], data[offset+2], data[offset+3],
            data[offset+4], data[offset+5], data[offset+6], data[offset+7],
        ]);
        
        Ok(Self {
            magic,
            version,
            index_offset,
            index_size,
            bloom_offset,
            bloom_size,
            num_entries,
            min_timestamp,
            max_timestamp,
        })
    }
}

/// SSTable statistics
#[derive(Debug, Clone)]
pub struct SSTableStats {
    pub num_entries: u64,
    pub file_size: u64,
    pub num_blocks: usize,
    pub min_timestamp: u64,
    pub max_timestamp: u64,
}

/// SSTable iterator for sequential scan
/// 🔧 Streaming iterator: reads blocks on-demand instead of loading all data
pub struct SSTableIterator {
    /// File reader
    file: BufReader<File>,
    /// Block index (offset, size pairs)
    index_entries: Vec<(Key, u64, u32)>,
    /// Current block index
    current_block_idx: usize,
    /// Current block's entries
    current_block_entries: Vec<(Key, Value)>,
    /// Position within current block
    position_in_block: usize,
}

impl SSTableIterator {
    fn new(sstable: &mut SSTable) -> Result<Self> {
        // Clone file handle for independent reading
        let file = BufReader::new(
            File::open(&sstable.path)
                .map_err(|e| StorageError::Io(e))?
        );
        
        // Clone index entries (small metadata, not data)
        let index_entries = sstable.index.entries.clone();
        
        Ok(Self {
            file,
            index_entries,
            current_block_idx: 0,
            current_block_entries: Vec::new(),
            position_in_block: 0,
        })
    }
    
    /// Load next block into memory
    fn load_next_block(&mut self) -> Result<bool> {
        if self.current_block_idx >= self.index_entries.len() {
            return Ok(false); // No more blocks
        }
        
        let (_, offset, size) = self.index_entries[self.current_block_idx];
        
        // Seek to block position
        self.file.seek(SeekFrom::Start(offset))?;
        
        // Read block data
        let mut block_buf = vec![0u8; size as usize];
        self.file.read_exact(&mut block_buf)?;
        
        // Deserialize block
        let block = DataBlock::deserialize(&block_buf)?;
        
        // Replace current block entries
        self.current_block_entries = block.entries;
        self.position_in_block = 0;
        self.current_block_idx += 1;
        
        Ok(true)
    }
}

impl Iterator for SSTableIterator {
    type Item = (Key, Value);
    
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try to get entry from current block
            if self.position_in_block < self.current_block_entries.len() {
                let entry = self.current_block_entries[self.position_in_block].clone();
                self.position_in_block += 1;
                return Some(entry);
            }
            
            // Current block exhausted, load next block
            match self.load_next_block() {
                Ok(true) => continue,  // Successfully loaded next block
                Ok(false) => return None,  // No more blocks
                Err(_) => return None,  // Error reading block
            }
        }
    }
}

// Helper trait to get file path
trait FileExt {
    fn path(&self) -> Option<&Path>;
}

impl FileExt for File {
    fn path(&self) -> Option<&Path> {
        None // Not available in std, would need platform-specific code
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_sstable_basic() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.sst");
        
        // Build SSTable
        {
            let mut builder = SSTableBuilder::new(&path, LSMConfig::default(), 100).unwrap();
            
            for i in 0..100 {
                let key = i as u64;  // ✅ u64 key
                let value = Value::new(format!("value_{}", i).into_bytes(), i as u64);
                builder.add(key, value).unwrap();
            }
            
            builder.finish().unwrap();
        }
        
        // Read SSTable
        {
            let mut sst = SSTable::open(&path).unwrap();
            
            // Test get
            let key = 50u64;  // ✅ u64 key
            let value = sst.get(key).unwrap().unwrap();
            assert_eq!(value.data, ValueData::Inline(b"value_50".to_vec()));
            assert_eq!(value.timestamp, 50);
            
            // Test non-existent key
            let result = sst.get(999u64).unwrap();
            assert!(result.is_none());
        }
    }
}
