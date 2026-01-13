//! Generic B+Tree Implementation with Variable-Length Values
//!
//! ## Design Principles
//! - **Generic Keys**: Support any fixed-size serializable key (u32, u64, etc.)
//! - **Variable-Length Values**: Support arbitrary byte arrays (posting lists, etc.)
//! - **Overflow Pages**: Large values (>50KB) stored in separate overflow page chains
//! - **Disk-First**: Page-aligned storage with efficient layout
//! - **High Performance**: O(log n) operations with LRU page cache
//!
//! ## Page Layout (64KB)
//! ```text
//! [Header: 16 bytes]
//!   - is_leaf: 1 byte
//!   - num_keys: 4 bytes (u32)
//!   - next_leaf: 8 bytes (u64)
//!   - reserved: 3 bytes
//! 
//! [Keys Section: N * key_size bytes]
//!   - key[0], key[1], ..., key[N-1]
//! 
//! [Value Offsets: N * 4 bytes] (leaf only)
//!   - offset[0]: u32 (relative to value_data start)
//!   - offset[1]: u32
//!   - ...
//! 
//! [Value Data: variable] (leaf only)
//!   - Small value: [len: u32][data: bytes]
//!   - Large value (overflow): [len: 0xFFFFFFFF][overflow_page_id: u64][total_size: u64]
//! 
//! [Children: (N+1) * 8 bytes] (internal only)
//!   - child[0]: u64 (page_id)
//!   - child[1]: u64
//!   - ...
//! ```text
//!
//! ## Overflow Page Chain
//! ```text
//! [next_page_id: u64][data_len: u32][data: bytes...]
//! ```text
use crate::{Result, StorageError};
use std::sync::{Arc, RwLock, Mutex};
use std::path::PathBuf;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::marker::PhantomData;

/// Page size (16KB - optimized for text index with frequent splits)
pub const PAGE_SIZE: usize = 16384; // 16KB

/// Header size
const HEADER_SIZE: usize = 16;

/// Invalid page ID
const INVALID_PAGE_ID: u64 = u64::MAX;

/// Overflow threshold: values larger than this are stored in overflow pages
/// Set to 2KB to ensure pages don't overflow even with multiple medium-sized values
const OVERFLOW_THRESHOLD: usize = 1024; // 1KB - balance between page utilization and overflow frequency

/// Overflow marker: indicates value is stored in overflow page chain
const OVERFLOW_MARKER: u32 = 0xFFFFFFFF;

/// Overflow page header size: [next_page_id: 8][data_len: 4] = 12 bytes
const OVERFLOW_PAGE_HEADER: usize = 12;

/// Available data space in overflow page
const OVERFLOW_DATA_SIZE: usize = PAGE_SIZE - OVERFLOW_PAGE_HEADER;

/// Magic number for generic B+Tree files
const BTREE_MAGIC: u32 = 0x47425452; // "GBTR" (Generic BTree)

/// Format version
const BTREE_VERSION: u32 = 2; // Bumped to v2 for overflow support

/// Generic B+Tree with fixed-size keys and variable-length values
pub struct GenericBTree<K: BTreeKey> {
    /// Root page ID
    root_page_id: Arc<RwLock<u64>>,
    
    /// Page cache (page_id -> Page)
    page_cache: Arc<RwLock<LruCache<u64, Arc<RwLock<Page<K>>>>>>,
    
    /// Next free page ID
    next_page_id: Arc<RwLock<u64>>,
    
    /// Storage file
    storage_file: Arc<RwLock<File>>,
    
    /// Flush lock
    flush_lock: Arc<Mutex<()>>,
    
    /// Storage path
    storage_path: PathBuf,
    
    /// Configuration
    config: GenericBTreeConfig,
    
    /// Key size in bytes
    key_size: usize,
    
    /// Max keys per page (calculated based on key_size)
    max_keys: usize,
    
    _phantom: PhantomData<K>,
}

/// Configuration for generic B+Tree
#[derive(Clone)]
pub struct GenericBTreeConfig {
    /// Page cache size
    pub cache_size: usize,
    
    /// Unique key constraint
    pub unique_keys: bool,
    
    /// Allow key updates
    pub allow_updates: bool,
    
    /// Immediate sync
    pub immediate_sync: bool,
}

impl Default for GenericBTreeConfig {
    fn default() -> Self {
        Self {
            cache_size: 1024,
            unique_keys: false,
            allow_updates: true,
            immediate_sync: false,
        }
    }
}

/// Trait for B+Tree keys (must be fixed-size)
pub trait BTreeKey: Clone + Ord + Sized {
    /// Serialize key to fixed-size bytes
    fn serialize(&self) -> Vec<u8>;
    
    /// Deserialize key from bytes
    fn deserialize(bytes: &[u8]) -> Result<Self>;
    
    /// Key size in bytes
    fn key_size() -> usize;
}

/// Implement BTreeKey for u32 (term_id)
impl BTreeKey for u32 {
    fn serialize(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
    
    fn deserialize(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 4 {
            return Err(StorageError::InvalidData("Key too short".into()));
        }
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }
    
    fn key_size() -> usize {
        4
    }
}

/// Implement BTreeKey for u64
impl BTreeKey for u64 {
    fn serialize(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
    
    fn deserialize(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 8 {
            return Err(StorageError::InvalidData("Key too short".into()));
        }
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&bytes[0..8]);
        Ok(u64::from_le_bytes(arr))
    }
    
    fn key_size() -> usize {
        8
    }
}

/// B+Tree page with generic key type
#[derive(Clone)]
struct Page<K: BTreeKey> {
    /// Page ID
    page_id: u64,
    
    /// Is this a leaf node?
    is_leaf: bool,
    
    /// Number of keys in this page
    num_keys: usize,
    
    /// Keys array
    keys: Vec<K>,
    
    /// Values array (byte arrays) - for leaf nodes
    values: Vec<Vec<u8>>,
    
    /// Child page IDs - for internal nodes
    children: Vec<u64>,
    
    /// Next leaf page (for sequential scan)
    next_leaf: u64,
    
    /// Dirty flag
    dirty: bool,
}

impl<K: BTreeKey> Page<K> {
    /// Create a new leaf page
    fn new_leaf(page_id: u64, max_keys: usize) -> Self {
        Self {
            page_id,
            is_leaf: true,
            num_keys: 0,
            keys: Vec::with_capacity(max_keys),
            values: Vec::with_capacity(max_keys),
            children: Vec::new(),
            next_leaf: INVALID_PAGE_ID,
            dirty: true,
        }
    }
    
    /// Create a new internal page
    fn new_internal(page_id: u64, max_keys: usize) -> Self {
        Self {
            page_id,
            is_leaf: false,
            num_keys: 0,
            keys: Vec::with_capacity(max_keys),
            values: Vec::new(),
            children: Vec::with_capacity(max_keys + 1),
            next_leaf: INVALID_PAGE_ID,
            dirty: true,
        }
    }
    
    /// Calculate the serialized size this page would need
    /// IMPORTANT: This assumes all large values will be converted to overflow markers
    fn calculate_serialized_size(&self, key_size: usize) -> usize {
        let mut size = HEADER_SIZE; // 16 bytes header
        
        // Keys
        size += self.num_keys * key_size;
        
        if self.is_leaf {
            // Offsets (4 bytes per key)
            size += self.num_keys * 4;
            
            // Values - CRITICAL: count each value properly considering overflow conversion
            for value in &self.values {
                let is_overflow_marker = value.len() == 20 
                    && value[0..4] == OVERFLOW_MARKER.to_le_bytes();
                
                if is_overflow_marker {
                    size += 20; // Already a marker
                } else if value.len() > OVERFLOW_THRESHOLD {
                    // Large value that WILL be converted to marker in write_page
                    size += 20;
                } else {
                    size += 4 + value.len(); // len + data
                }
            }
        } else {
            // Children (8 bytes per child, N+1 children)
            size += (self.num_keys + 1) * 8;
        }
        
        size
    }
    
    /// Check if adding a new key-value pair would overflow the page
    fn would_overflow_with(&self, key_size: usize, value_size: usize) -> bool {
        let current_size = self.calculate_serialized_size(key_size);
        
        // Calculate additional space needed for new key-value
        let additional_space = if self.is_leaf {
            key_size + 4 + // key + offset
            if value_size > OVERFLOW_THRESHOLD {
                20 // overflow marker
            } else {
                4 + value_size // len + data
            }
        } else {
            key_size + 8 // key + child pointer
        };
        
        current_size + additional_space > PAGE_SIZE
    }
    
    /// Serialize page to bytes
    fn serialize(&self, key_size: usize) -> Result<Vec<u8>> {
        // ASSUMPTION: All large values have already been converted to overflow markers
        // by write_page() before calling this method
        
        let mut buf = vec![0u8; PAGE_SIZE];
        let mut offset = 0;
        
        // Header: [is_leaf:1][num_keys:4][next_leaf:8][reserved:3] = 16 bytes
        buf[offset] = if self.is_leaf { 1 } else { 0 };
        offset += 1;
        
        buf[offset..offset+4].copy_from_slice(&(self.num_keys as u32).to_le_bytes());
        offset += 4;
        
        buf[offset..offset+8].copy_from_slice(&self.next_leaf.to_le_bytes());
        offset += 8;
        
        // Reserved (3 bytes)
        offset += 3;
        
        // Keys section - ONLY serialize num_keys elements
        // CRITICAL: After delete, self.keys.len() may equal num_keys (if properly cleaned)
        // But we must ensure we only iterate over valid keys
        for i in 0..self.num_keys {
            let key = &self.keys[i];
            let key_bytes = key.serialize();
            if key_bytes.len() != key_size {
                return Err(StorageError::InvalidData(
                    format!("Key size mismatch: expected {}, got {}", key_size, key_bytes.len())
                ));
            }
            buf[offset..offset+key_size].copy_from_slice(&key_bytes);
            offset += key_size;
        }
        
        if self.is_leaf {
            // Value offsets and data
            let mut value_offset = 0u32;
            let value_data_start = offset + self.num_keys * 4;
            
            // Pre-check: calculate total space needed
            let mut total_space_needed = value_data_start;
            let mut marker_count = 0;
            let mut large_count = 0;
            let mut normal_count = 0;
            
            // CRITICAL: Only iterate over num_keys values
            for i in 0..self.num_keys {
                let value = &self.values[i];
                let is_overflow_marker = value.len() == 20 
                    && value[0..4] == OVERFLOW_MARKER.to_le_bytes();
                
                if is_overflow_marker {
                    total_space_needed += 20;
                    marker_count += 1;
                } else if value.len() > OVERFLOW_THRESHOLD {
                    // ERROR: Large value should have been converted before serialize()
                    large_count += 1;
                    eprintln!("DEBUG serialize: Page {}: Found unconverted large value #{}: {} bytes",
                             self.page_id, large_count, value.len());
                    return Err(StorageError::InvalidData(
                        format!("Page {}: Found unconverted large value ({} bytes) in serialize(). Must convert in write_page() first.",
                                self.page_id, value.len())
                    ));
                } else {
                    total_space_needed += 4 + value.len();
                    normal_count += 1;
                }
            }
            
            if total_space_needed > PAGE_SIZE {
                eprintln!("DEBUG serialize: Page {} overflow! {} keys, {} markers, {} normal, {} bytes needed",
                         self.page_id, self.num_keys, marker_count, normal_count, total_space_needed);
                return Err(StorageError::InvalidData(
                    format!("Page {} cannot fit {} keys: needs {} bytes but PAGE_SIZE is {}",
                            self.page_id, self.num_keys, total_space_needed, PAGE_SIZE)
                ));
            }
            
            // First pass: write offsets - ONLY num_keys values
            for i in 0..self.num_keys {
                let value = &self.values[i];
                buf[offset..offset+4].copy_from_slice(&value_offset.to_le_bytes());
                offset += 4;
                
                let is_overflow_marker = value.len() == 20 
                    && value[0..4] == OVERFLOW_MARKER.to_le_bytes();
                
                if is_overflow_marker {
                    value_offset += 20;
                } else {
                    value_offset += 4 + value.len() as u32;
                }
            }
            
            // Second pass: write value data - ONLY num_keys values
            for i in 0..self.num_keys {
                let value = &self.values[i];
                let is_overflow_marker = value.len() == 20 
                    && value[0..4] == OVERFLOW_MARKER.to_le_bytes();
                
                if is_overflow_marker {
                    // Copy the entire 20-byte overflow marker
                    let overflow_page_id = u64::from_le_bytes([
                        value[4], value[5], value[6], value[7],
                        value[8], value[9], value[10], value[11],
                    ]);
                    
                    if overflow_page_id == 0 {
                        return Err(StorageError::InvalidData(
                            format!("Page {}: Overflow marker with zero page_id", self.page_id)
                        ));
                    }
                    
                    buf[offset..offset+20].copy_from_slice(value);
                    offset += 20;
                } else {
                    // Normal inline value
                    let len = value.len() as u32;
                    buf[offset..offset+4].copy_from_slice(&len.to_le_bytes());
                    offset += 4;
                    buf[offset..offset+value.len()].copy_from_slice(value);
                    offset += value.len();
                }
            }
        } else {
            // Children section
            for (i, &child) in self.children.iter().enumerate() {
                if child == 0 || child > 1_000_000_000 {
                    eprintln!("ERROR serialize: Page {} (internal) has invalid child[{}] = {}",
                             self.page_id, i, child);
                    return Err(StorageError::InvalidData(
                        format!("Invalid child page_id {} at index {} in page {}", child, i, self.page_id)
                    ));
                }
                buf[offset..offset+8].copy_from_slice(&child.to_le_bytes());
                offset += 8;
            }
        }
        
        Ok(buf)
    }
    
    /// Deserialize page from bytes
    fn deserialize(page_id: u64, buf: &[u8], key_size: usize) -> Result<Self> {
        if buf.len() < PAGE_SIZE {
            return Err(StorageError::InvalidData(
                format!("Page size too small: {}", buf.len())
            ));
        }
        
        let mut offset = 0;
        
        // Header
        let is_leaf = buf[offset] == 1;
        offset += 1;
        
        let num_keys = u32::from_le_bytes([
            buf[offset], buf[offset+1], buf[offset+2], buf[offset+3]
        ]) as usize;
        offset += 4;
        
        let next_leaf = u64::from_le_bytes([
            buf[offset], buf[offset+1], buf[offset+2], buf[offset+3],
            buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7],
        ]);
        offset += 8;
        
        // Skip reserved
        offset += 3;
        
        // Read keys
        let mut keys = Vec::with_capacity(num_keys);
        for _ in 0..num_keys {
            let key = K::deserialize(&buf[offset..offset+key_size])?;
            keys.push(key);
            offset += key_size;
        }
        
        let (values, children) = if is_leaf {
            // Read value offsets
            let value_offsets_start = offset;
            let mut value_offsets = Vec::with_capacity(num_keys);
            for _ in 0..num_keys {
                let off = u32::from_le_bytes([
                    buf[offset], buf[offset+1], buf[offset+2], buf[offset+3]
                ]);
                value_offsets.push(off);
                offset += 4;
            }
            
            // Read values (with overflow support)
            let value_data_start = value_offsets_start + num_keys * 4;
            let mut values = Vec::with_capacity(num_keys);
            
            for &val_offset in &value_offsets {
                let abs_offset = value_data_start + val_offset as usize;
                
                let len_or_marker = u32::from_le_bytes([
                    buf[abs_offset], buf[abs_offset+1], buf[abs_offset+2], buf[abs_offset+3]
                ]);
                
                if len_or_marker == OVERFLOW_MARKER {
                    // Overflow value: [marker:4][overflow_page_id:8][total_size:8]
                    let overflow_page_id = u64::from_le_bytes([
                        buf[abs_offset+4], buf[abs_offset+5], buf[abs_offset+6], buf[abs_offset+7],
                        buf[abs_offset+8], buf[abs_offset+9], buf[abs_offset+10], buf[abs_offset+11],
                    ]);
                    
                    let total_size = u64::from_le_bytes([
                        buf[abs_offset+12], buf[abs_offset+13], buf[abs_offset+14], buf[abs_offset+15],
                        buf[abs_offset+16], buf[abs_offset+17], buf[abs_offset+18], buf[abs_offset+19],
                    ]);
                    
                    // Mark as overflow - will be read on-demand
                    // Store metadata as special marker: [0xFF, 0xFF, 0xFF, 0xFF, overflow_page_id, total_size]
                    // This is a placeholder; actual read happens in get()
                    let mut overflow_marker = Vec::with_capacity(20);
                    overflow_marker.extend_from_slice(&OVERFLOW_MARKER.to_le_bytes());
                    overflow_marker.extend_from_slice(&overflow_page_id.to_le_bytes());
                    overflow_marker.extend_from_slice(&total_size.to_le_bytes());
                    values.push(overflow_marker);
                } else {
                    // Normal inline value
                    let len = len_or_marker as usize;
                    let data = buf[abs_offset+4..abs_offset+4+len].to_vec();
                    values.push(data);
                }
            }
            
            (values, Vec::new())
        } else {
            // Read children
            let mut children = Vec::with_capacity(num_keys + 1);
            for _ in 0..=num_keys {
                let child = u64::from_le_bytes([
                    buf[offset], buf[offset+1], buf[offset+2], buf[offset+3],
                    buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7],
                ]);
                
                if child == 0 || child > 1_000_000_000 {
                    return Err(StorageError::InvalidData(
                        format!("Invalid child page_id {} in page {}", child, page_id)
                    ));
                }
                
                children.push(child);
                offset += 8;
            }
            
            (Vec::new(), children)
        };
        
        Ok(Self {
            page_id,
            is_leaf,
            num_keys,
            keys,
            values,
            children,
            next_leaf,
            dirty: false,
        })
    }
}

impl<K: BTreeKey> GenericBTree<K> {
    /// Create a new generic B+Tree
    pub fn new(storage_path: PathBuf) -> Result<Self> {
        Self::with_config(storage_path, GenericBTreeConfig::default())
    }
    
    /// Create with custom configuration
    pub fn with_config(storage_path: PathBuf, config: GenericBTreeConfig) -> Result<Self> {
        let key_size = K::key_size();
        
        // Conservative calculation considering overflow support
        // Assume worst case: all values need overflow (20 bytes each)
        // Layout: Header(16) + Keys(N*key_size) + Offsets(N*4) + Values(N*20)
        let available_space = PAGE_SIZE - HEADER_SIZE;
        let per_key_overhead = key_size + 4 + 20; // key + offset + overflow_marker
        let max_keys = available_space / per_key_overhead;
        
        // Sanity check: max_keys should be at least 4 for B+Tree to function
        let max_keys = max_keys.max(4);
        
        if max_keys < 4 {
            return Err(StorageError::InvalidData(
                format!("Key size too large: {} (max_keys = {})", key_size, max_keys)
            ));
        }
        
        let exists = storage_path.exists();
        
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&storage_path)?;
        
        let (root_page_id, next_page_id) = if !exists {
            // New file: write superblock
            let superblock = SuperBlock {
                magic: BTREE_MAGIC,
                version: BTREE_VERSION,
                root_page_id: 1,
                next_page_id: 2,
                key_size: key_size as u32,
            };
            
            let sb_bytes = bincode::serialize(&superblock)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            
            // Pad superblock to 256 bytes
            let mut padded_sb = vec![0u8; 256];
            padded_sb[..sb_bytes.len()].copy_from_slice(&sb_bytes);
            
            file.write_all(&padded_sb)?;
            
            // Pre-allocate space for root page to avoid read errors
            let root_page_offset = 256 + PAGE_SIZE as u64;
            file.set_len(root_page_offset)?;
            
            file.sync_all()?;
            
            (1u64, 2u64)
        } else {
            // Load superblock
            let mut sb_bytes = vec![0u8; 256];
            file.read_exact(&mut sb_bytes)?;
            
            let superblock: SuperBlock = bincode::deserialize(&sb_bytes)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            
            if superblock.magic != BTREE_MAGIC {
                return Err(StorageError::InvalidData("Invalid magic number".into()));
            }
            
            // Support version 1 (without overflow) and version 2 (with overflow)
            if superblock.version != 1 && superblock.version != BTREE_VERSION {
                return Err(StorageError::InvalidData(
                    format!("Unsupported version: {} (expected 1 or {})", superblock.version, BTREE_VERSION)
                ));
            }
            
            // Note: Version 1 files can be read by version 2 code (backward compatible)
            // Version 2 features (overflow) will be automatically used when writing
            
            if superblock.key_size as usize != key_size {
                return Err(StorageError::InvalidData(
                    format!("Key size mismatch: expected {}, got {}", key_size, superblock.key_size)
                ));
            }
            
            (superblock.root_page_id, superblock.next_page_id)
        };
        
        let cache_size = NonZeroUsize::new(config.cache_size)
            .ok_or_else(|| StorageError::InvalidData("Cache size must be > 0".into()))?;
        
        let tree = Self {
            root_page_id: Arc::new(RwLock::new(root_page_id)),
            page_cache: Arc::new(RwLock::new(LruCache::new(cache_size))),
            next_page_id: Arc::new(RwLock::new(next_page_id)),
            storage_file: Arc::new(RwLock::new(file)),
            flush_lock: Arc::new(Mutex::new(())),
            storage_path,
            config,
            key_size,
            max_keys,
            _phantom: PhantomData,
        };
        
        // Create root page if new
        if !exists {
            let root_page = Page::new_leaf(root_page_id, max_keys);
            tree.write_page(&root_page)?;
        }
        
        Ok(tree)
    }
    
    /// Insert or update key-value pair
    pub fn insert(&mut self, key: K, value: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let root_id = *self.root_page_id.read()
            .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
        
        // Recursive insert with split handling
        let (old_value, split_info) = self.insert_internal(root_id, key, value)?;
        
        // If root was split, create new root
        if let Some((split_key, new_page_id)) = split_info {
            let new_root_id = {
                let mut next = self.next_page_id.write()
                    .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
                let id = *next;
                *next += 1;
                id
            };
            
            let mut new_root = Page::new_internal(new_root_id, self.max_keys);
            new_root.keys.push(split_key);
            new_root.children.push(root_id);
            new_root.children.push(new_page_id);
            new_root.num_keys = 1;
            new_root.dirty = true;
            
            // Write new root
            self.write_page(&new_root)?;
            
            // Update root ID
            {
                let mut root = self.root_page_id.write()
                    .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
                *root = new_root_id;
            } // Release write lock here
            
            // Update superblock
            self.sync_superblock()?;
        }
        
        Ok(old_value)
    }
    
    /// Internal iterative insert with split handling (avoids stack overflow)
    fn insert_internal(&mut self, mut page_id: u64, key: K, value: Vec<u8>) 
        -> Result<(Option<Vec<u8>>, Option<(K, u64)>)> {
        
        // 🔧 ITERATIVE IMPLEMENTATION: Use a stack to track path instead of recursion
        let mut path_stack: Vec<(u64, usize)> = Vec::new(); // (page_id, child_index)
        
        // Phase 1: Find the leaf node and record path
        loop {
            let page = self.read_page(page_id)?;
            
            if page.is_leaf {
                // Found leaf, break to insert
                break;
            }
            
            // Internal node: find child
            let child_idx = match page.keys.binary_search(&key) {
                Ok(idx) => idx + 1,  // Key exists, go to right child
                Err(idx) => idx,     // Insert position
            };
            
            let child_id = page.children[child_idx];
            path_stack.push((page_id, child_idx));
            page_id = child_id;
        }
        
        // Phase 2: Insert into leaf
        let mut page = self.read_page(page_id)?;
        let mut current_split_info: Option<(K, u64)> = None;
        let mut old_value_result: Option<Vec<u8>> = None;
        
        if page.is_leaf {
            // Leaf node: first try normal insert
            let search_result = page.keys.binary_search(&key);
            old_value_result = match search_result {
                Ok(idx) => {
                    // Key exists - update
                    if !self.config.allow_updates {
                        return Err(StorageError::InvalidData(
                            "Key already exists and updates are disabled".into()
                        ));
                    }
                    let old = Some(page.values[idx].clone());
                    page.values[idx] = value;
                    page.dirty = true;
                    
                    // Check if update caused overflow
                    let serialized_size = page.calculate_serialized_size(K::key_size());
                    if serialized_size > PAGE_SIZE {
                        // Revert and split
                        let temp_value = page.values[idx].clone();
                        page.values[idx] = old.clone().unwrap();
                        
                        let split_key_clone = page.keys[page.num_keys * 2 / 5].clone();
                        let split_info = self.split_leaf(&mut page)?;
                        self.write_page(&page)?;
                        
                        // Re-apply update
                        let mut left_page = self.read_page(page.page_id)?;
                        let mut right_page = self.read_page(split_info.1)?;
                        
                        let target_key = page.keys[idx].clone();
                        if target_key < split_key_clone {
                            let update_idx = left_page.keys.binary_search(&target_key).unwrap();
                            left_page.values[update_idx] = temp_value;
                            left_page.dirty = true;
                            self.write_page(&left_page)?;
                        } else {
                            let update_idx = right_page.keys.binary_search(&target_key).unwrap();
                            right_page.values[update_idx] = temp_value;
                            right_page.dirty = true;
                            self.write_page(&right_page)?;
                        }
                        
                        current_split_info = Some(split_info);
                    } else {
                        self.write_page(&page)?;
                    }
                    
                    old
                }
                Err(idx) => {
                    // Normal insert
                    page.keys.insert(idx, key.clone());
                    page.values.insert(idx, value);
                    page.num_keys += 1;
                    page.dirty = true;
                    
                    // Check if we need to split
                    let serialized_size = page.calculate_serialized_size(K::key_size());
                    
                    if page.num_keys >= self.max_keys || serialized_size > PAGE_SIZE {
                        // Remove temporarily
                        let temp_key = page.keys.remove(idx);
                        let temp_value = page.values.remove(idx);
                        page.num_keys -= 1;
                        
                        let split_key_clone = page.keys[page.num_keys * 2 / 5].clone();
                        let split_info = self.split_leaf(&mut page)?;
                        self.write_page(&page)?;
                        
                        // Re-insert
                        let mut left_page = self.read_page(page.page_id)?;
                        let mut right_page = self.read_page(split_info.1)?;
                        
                        if temp_key < split_key_clone {
                            let ins_idx = left_page.keys.binary_search(&temp_key).unwrap_err();
                            left_page.keys.insert(ins_idx, temp_key);
                            left_page.values.insert(ins_idx, temp_value);
                            left_page.num_keys += 1;
                            left_page.dirty = true;
                            self.write_page(&left_page)?;
                        } else {
                            let ins_idx = right_page.keys.binary_search(&temp_key).unwrap_err();
                            right_page.keys.insert(ins_idx, temp_key);
                            right_page.values.insert(ins_idx, temp_value);
                            right_page.num_keys += 1;
                            right_page.dirty = true;
                            self.write_page(&right_page)?;
                        }
                        
                        current_split_info = Some(split_info);
                    } else {
                        self.write_page(&page)?;
                    }
                    
                    None
                }
            };
        }
        
        // Phase 3: Propagate splits upward iteratively
        while let Some((split_key, new_page_id)) = current_split_info {
            if path_stack.is_empty() {
                // Root split - handled by caller
                return Ok((old_value_result, Some((split_key, new_page_id))));
            }
            
            // Pop parent from stack
            let (parent_id, child_idx) = path_stack.pop().unwrap();
            let mut parent_page = self.read_page(parent_id)?;
            
            // Insert split key into parent
            let idx = match parent_page.keys.binary_search(&split_key) {
                Ok(existing_idx) => existing_idx,
                Err(insert_idx) => insert_idx,
            };
            
            if idx < parent_page.keys.len() && parent_page.keys[idx] == split_key {
                parent_page.children[idx + 1] = new_page_id;
            } else {
                parent_page.keys.insert(idx, split_key.clone());
                parent_page.children.insert(idx + 1, new_page_id);
                parent_page.num_keys += 1;
            }
            parent_page.dirty = true;
            
            // Check if parent needs split
            let serialized_size = parent_page.calculate_serialized_size(K::key_size());
            let needs_split = parent_page.num_keys >= self.max_keys || serialized_size > PAGE_SIZE;
            
            if needs_split {
                let parent_split_info = self.split_internal(&mut parent_page)?;
                self.write_page(&parent_page)?;
                current_split_info = Some(parent_split_info);
            } else {
                self.write_page(&parent_page)?;
                current_split_info = None;
            }
        }
        
        Ok((old_value_result, None))
    }
    
    /// OLD RECURSIVE VERSION (kept for reference, not used)
    #[allow(dead_code)]
    fn insert_internal_recursive(&mut self, page_id: u64, key: K, value: Vec<u8>) 
        -> Result<(Option<Vec<u8>>, Option<(K, u64)>)> {
        // This is kept for reference only and is not called
        // The actual implementation uses the iterative version above
        unimplemented!("Use iterative insert_internal instead")
    }
    
    /// Split a leaf page
    fn split_leaf(&mut self, page: &mut Page<K>) -> Result<(K, u64)> {
        // NOTE: Do NOT convert values here - write_page will handle it
        // This prevents double conversion bugs
        
        // Find split point based on actual byte size (not just key count)
        // Goal: Keep left page under 50% of PAGE_SIZE to leave room for growth
        let key_size = K::key_size();
        let target_left_size = (PAGE_SIZE as f64 * 0.4) as usize; // Target 40% for left page
        let mut left_size = HEADER_SIZE; // Start with header
        let mut split_idx = 0;
        
        // Calculate cumulative sizes to find best split point
        for i in 0..page.num_keys {
            let key_size_bytes = key_size;
            let value_size = if page.values[i].len() > OVERFLOW_THRESHOLD {
                20 // overflow marker
            } else {
                4 + page.values[i].len() // len + data
            };
            let entry_size = key_size_bytes + 4 + value_size; // key + offset + value
            
            // Check if adding this entry would exceed target
            if left_size + entry_size > target_left_size && split_idx > 0 {
                // This is a good split point
                break;
            }
            
            left_size += entry_size;
            split_idx = i + 1;
        }
        
        // Ensure we don't create empty pages - split at least at 25%, at most at 75%
        let min_split = (page.num_keys / 4).max(1);
        let max_split = (page.num_keys * 3 / 4).max(min_split + 1).min(page.num_keys - 1);
        split_idx = split_idx.clamp(min_split, max_split);
        
        // Allocate new page
        let new_page_id = {
            let mut next = self.next_page_id.write()
                .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
            let id = *next;
            *next += 1;
            id
        };
        
        let mut new_page = Page::new_leaf(new_page_id, self.max_keys);
        
        // Move entries from split_idx onwards to new page
        new_page.keys = page.keys.split_off(split_idx);
        new_page.values = page.values.split_off(split_idx);
        
        
        // Debug: Check if any values are overflow markers with zero page_id
        for (i, value) in new_page.values.iter().enumerate() {
            if value.len() == 20 && value[0..4] == OVERFLOW_MARKER.to_le_bytes() {
                let overflow_page_id = u64::from_le_bytes([
                    value[4], value[5], value[6], value[7],
                    value[8], value[9], value[10], value[11],
                ]);
                if overflow_page_id == 0 {
                } else {
                }
            }
        }
        
        new_page.num_keys = new_page.keys.len();
        new_page.next_leaf = page.next_leaf;
        new_page.dirty = true;
        
        // Update original page
        page.num_keys = page.keys.len();
        page.next_leaf = new_page_id;
        page.dirty = true;
        
        // Split key is the first key of new page
        let split_key = new_page.keys[0].clone();
        
        // Write new page
        self.write_page(&new_page)?;
        
        Ok((split_key, new_page_id))
    }
    
    /// Split an internal page
    fn split_internal(&mut self, page: &mut Page<K>) -> Result<(K, u64)> {
        let mid = page.num_keys / 2;
        
        // Allocate new page
        let new_page_id = {
            let mut next = self.next_page_id.write()
                .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
            let id = *next;
            *next += 1;
            id
        };
        
        let mut new_page = Page::new_internal(new_page_id, self.max_keys);
        
        // The middle key is promoted to parent
        let split_key = page.keys[mid].clone();
        
        // Move keys after mid to new page
        new_page.keys = page.keys.split_off(mid + 1);
        new_page.children = page.children.split_off(mid + 1);
        new_page.num_keys = new_page.keys.len();
        new_page.dirty = true;
        
        // Remove promoted key from original page
        page.keys.pop();
        page.num_keys = page.keys.len();
        page.dirty = true;
        
        // Write new page
        self.write_page(&new_page)?;
        
        Ok((split_key, new_page_id))
    }
    
    /// Get value by key
    pub fn get(&self, key: &K) -> Result<Option<Vec<u8>>> {
        let root_id = *self.root_page_id.read()
            .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
        
        self.search_internal(root_id, key)
    }
    
    /// Internal recursive search
    fn search_internal(&self, page_id: u64, key: &K) -> Result<Option<Vec<u8>>> {
        let page = self.read_page(page_id)?;
        
        if page.is_leaf {
            // Leaf node: binary search
            match page.keys.binary_search(key) {
                Ok(idx) => {
                    let value = &page.values[idx];
                    
                    // Check if this is an overflow value
                    if value.len() == 20 && value[0..4] == OVERFLOW_MARKER.to_le_bytes() {
                        // Overflow value: read from overflow chain
                        let overflow_page_id = u64::from_le_bytes([
                            value[4], value[5], value[6], value[7],
                            value[8], value[9], value[10], value[11],
                        ]);
                        
                        let total_size = u64::from_le_bytes([
                            value[12], value[13], value[14], value[15],
                            value[16], value[17], value[18], value[19],
                        ]);
                        
                        if overflow_page_id == 0 {
                            return Err(StorageError::InvalidData(
                                format!("Overflow page_id is 0 for key in page {}", page_id)
                            ));
                        }
                        
                        let full_value = self.read_overflow_chain(overflow_page_id, total_size)?;
                        Ok(Some(full_value))
                    } else {
                        // Normal inline value
                        Ok(Some(value.clone()))
                    }
                },
                Err(_) => Ok(None),
            }
        } else {
            // Internal node: find child and recurse
            let child_idx = match page.keys.binary_search(key) {
                Ok(idx) => idx + 1,  // Key exists, go to right child
                Err(idx) => idx,     // Insert position
            };
            
            
            if child_idx >= page.children.len() {
                return Err(StorageError::InvalidData(
                    format!("Child index {} out of bounds (num_children={}) in page {}", 
                            child_idx, page.children.len(), page_id)
                ));
            }
            
            let child_id = page.children[child_idx];
            self.search_internal(child_id, key)
        }
    }
    
    /// Delete key from B+Tree
    /// Returns the old value if the key existed
    /// 
    /// Note: This is a simplified delete that doesn't implement full rebalancing.
    /// It works by marking keys as deleted (lazy deletion) to avoid complex tree restructuring.
    /// Periodic compaction can be added later to reclaim space.
    pub fn delete(&mut self, key: &K) -> Result<Option<Vec<u8>>> {
        let root_id = *self.root_page_id.read()
            .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
        
        if root_id == 0 {
            return Ok(None);
        }
        
        // Find and remove the key
        let result = self.delete_from_tree(root_id, key)?;
        
        // After deletion, check if root needs to be updated
        // If root is internal node with 0 keys, promote its only child
        let root_page = self.read_page(root_id)?;
        if !root_page.is_leaf && root_page.num_keys == 0 && root_page.children.len() == 1 {
            let new_root_id = root_page.children[0];
            let mut root_write = self.root_page_id.write()
                .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
            *root_write = new_root_id;
            drop(root_write);
            
            // Sync to disk
            self.sync_superblock()?;
        }
        
        Ok(result)
    }
    
    /// Delete key from tree (recursive helper)
    fn delete_from_tree(&self, page_id: u64, key: &K) -> Result<Option<Vec<u8>>> {
        let mut page = self.read_page(page_id)?;
        
        if page.is_leaf {
            // Leaf node: find and remove key
            match page.keys[..page.num_keys].binary_search(key) {
                Ok(pos) => {
                    // Found the key, remove it
                    let old_value = page.values[pos].clone();
                    
                    // Safety check: ensure vectors have enough elements
                    if pos >= page.keys.len() || pos >= page.values.len() {
                        return Err(StorageError::InvalidData(
                            format!("Delete position {} out of bounds (keys={}, values={})", 
                                    pos, page.keys.len(), page.values.len())
                        ));
                    }
                    
                    // Use Vec::remove for safe element removal
                    page.keys.remove(pos);
                    page.values.remove(pos);
                    page.num_keys -= 1;
                    page.dirty = true;
                    
                    // Write page back
                    self.write_page(&page)?;
                    
                    Ok(Some(old_value))
                }
                Err(_) => {
                    // Key not found
                    Ok(None)
                }
            }
        } else {
            // Internal node: find child to descend into
            let child_pos = match page.keys[..page.num_keys].binary_search(key) {
                Ok(pos) => pos + 1,
                Err(pos) => pos,
            };
            
            // Safety check: ensure child_pos is valid
            if child_pos >= page.children.len() {
                return Err(StorageError::InvalidData(
                    format!("Child position {} out of bounds (children={})", 
                            child_pos, page.children.len())
                ));
            }
            
            let child_id = page.children[child_pos];
            self.delete_from_tree(child_id, key)
        }
    }
    
    /// Calculate minimum number of keys for a node (for future rebalancing)
    fn min_keys(&self) -> usize {
        // For B+Tree: ceil(max_keys/2)
        (self.max_keys + 1) / 2
    }
    
    /// Sync superblock to disk
    fn sync_superblock(&self) -> Result<()> {
        let root_id = *self.root_page_id.read()
            .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
        let next_id = *self.next_page_id.read()
            .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
        
        let superblock = SuperBlock {
            magic: BTREE_MAGIC,
            version: BTREE_VERSION,
            root_page_id: root_id,
            next_page_id: next_id,
            key_size: self.key_size as u32,
        };
        
        let sb_bytes = bincode::serialize(&superblock)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        
        let mut file = self.storage_file.write()
            .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
        
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&sb_bytes)?;
        file.sync_all()?;
        
        Ok(())
    }
    
    /// Write overflow page chain for large value
    /// Returns the first overflow page ID
    fn write_overflow_chain(&self, data: &[u8]) -> Result<u64> {
        let mut remaining = data;
        let mut first_page_id = None;
        let mut prev_page_id = None;
        
        while !remaining.is_empty() {
            // Allocate new overflow page
            let page_id = {
                let mut next = self.next_page_id.write()
                    .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
                let id = *next;
                *next += 1;
                if *next % 1000 == 0 || *next > 10000 {
                }
                id
            };
            
            if first_page_id.is_none() {
                first_page_id = Some(page_id);
            }
            
            // Determine chunk size
            let chunk_size = remaining.len().min(OVERFLOW_DATA_SIZE);
            let chunk = &remaining[..chunk_size];
            
            // Build overflow page: [next_page_id:8][data_len:4][data:chunk_size]
            let mut page_buf = vec![0u8; PAGE_SIZE];
            
            // Next page ID (0 if this is the last page)
            let next_page_id = if remaining.len() > chunk_size {
                // More data remaining, will allocate next page
                let next = self.next_page_id.read()
                    .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
                *next // Peek at next ID
            } else {
                0 // Last page
            };
            
            page_buf[0..8].copy_from_slice(&next_page_id.to_le_bytes());
            
            // Safety check for u32 conversion
            if chunk_size > u32::MAX as usize {
                return Err(StorageError::InvalidData(
                    format!("Chunk size {} exceeds u32::MAX", chunk_size)
                ));
            }
            
            page_buf[8..12].copy_from_slice(&(chunk_size as u32).to_le_bytes());
            page_buf[12..12+chunk_size].copy_from_slice(chunk);
            
            // Write overflow page to disk
            let mut file = self.storage_file.write()
                .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
            
            let offset = 256 + (page_id - 1) * PAGE_SIZE as u64;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&page_buf)?;
            
            remaining = &remaining[chunk_size..];
            prev_page_id = Some(page_id);
        }
        
        Ok(first_page_id.unwrap())
    }
    
    /// Read overflow page chain
    fn read_overflow_chain(&self, first_page_id: u64, total_size: u64) -> Result<Vec<u8>> {
        
        let mut result = Vec::with_capacity(total_size as usize);
        let mut page_id = first_page_id;
        let mut iteration = 0;
        
        while page_id != 0 {
            iteration += 1;
            if iteration > 1000 {
                return Err(StorageError::InvalidData(
                    format!("Overflow chain too long ({}+ pages), possible corruption", iteration)
                ));
            }
            
            
            // Read overflow page
            let mut file = self.storage_file.write()
                .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
            
            let offset = 256 + (page_id - 1) * PAGE_SIZE as u64;
            file.seek(SeekFrom::Start(offset))?;
            
            let mut page_buf = vec![0u8; PAGE_SIZE];
            file.read_exact(&mut page_buf)?;
            
            // Parse overflow page
            let next_page_id = u64::from_le_bytes([
                page_buf[0], page_buf[1], page_buf[2], page_buf[3],
                page_buf[4], page_buf[5], page_buf[6], page_buf[7],
            ]);
            
            let data_len = u32::from_le_bytes([
                page_buf[8], page_buf[9], page_buf[10], page_buf[11],
            ]) as usize;
            
            
            // Append data
            result.extend_from_slice(&page_buf[12..12+data_len]);
            
            page_id = next_page_id;
        }
        
        Ok(result)
    }
    
    /// Range query: get all key-value pairs where start <= key <= end
    /// Returns Vec<(K, Vec<u8>)>
    /// 
    /// Algorithm:
    /// 1. Find first leaf containing keys >= start
    /// 2. Scan leaf chain sequentially using next_leaf pointers
    /// 3. Stop when key > end
    pub fn range(&self, start: &K, end: &K) -> Result<Vec<(K, Vec<u8>)>> {
        let root_id = *self.root_page_id.read()
            .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
        
        if root_id == 0 {
            return Ok(Vec::new());
        }
        
        // Step 1: Find first leaf that may contain keys >= start
        let first_leaf_id = self.find_leaf_for_key(root_id, start)?;
        
        // Step 2: Scan leaf chain
        // 🚀 P1 优化：预分配容量（估算范围大小，假设平均 10 个结果）
        let mut results = Vec::with_capacity(10);
        self.scan_leaf_chain(first_leaf_id, start, end, &mut results)?;
        
        Ok(results)
    }
    
    /// Find leaf node that should contain the given key
    fn find_leaf_for_key(&self, page_id: u64, key: &K) -> Result<u64> {
        let page = self.read_page(page_id)?;
        
        if page.is_leaf {
            return Ok(page_id);
        }
        
        // Internal node: binary search to find child
        let child_idx = match page.keys.binary_search(key) {
            Ok(idx) => idx + 1,  // Key found, go to right child
            Err(idx) => idx,     // Insert position
        };
        
        if child_idx >= page.children.len() {
            return Err(StorageError::Index(
                format!("Child index {} out of bounds (num_children={}, num_keys={})", 
                        child_idx, page.children.len(), page.num_keys)
            ));
        }
        
        let child_id = page.children[child_idx];
        
        if child_id == 0 {
            return Err(StorageError::Corruption(
                format!("Invalid child_id=0 at page {}, child_idx={}", page_id, child_idx)
            ));
        }
        
        self.find_leaf_for_key(child_id, key)
    }
    
    /// Scan leaf chain sequentially
    /// 
    /// ✅ FIX: Scan all leaf pages without early termination
    /// Reason: Page splits may cause out-of-order keys across pages
    fn scan_leaf_chain(&self, start_leaf_id: u64, start: &K, end: &K, 
                       results: &mut Vec<(K, Vec<u8>)>) -> Result<()> {
        let mut current_leaf_id = start_leaf_id;
        
        while current_leaf_id != INVALID_PAGE_ID {
            let page = self.read_page(current_leaf_id)?;
            
            if !page.is_leaf {
                return Err(StorageError::Index("Expected leaf node".into()));
            }
            
            for i in 0..page.num_keys {
                let key = &page.keys[i];
                
                // ✅ Don't break early - continue scanning all pages
                // Only add keys within the range
                if key <= end && key >= start {
                    let value = &page.values[i];
                    
                    // Check if this is an overflow value
                    let actual_value = if value.len() == 20 && value[0..4] == OVERFLOW_MARKER.to_le_bytes() {
                        // Overflow value: read from overflow chain
                        let overflow_page_id = u64::from_le_bytes([
                            value[4], value[5], value[6], value[7],
                            value[8], value[9], value[10], value[11],
                        ]);
                        
                        let total_size = u64::from_le_bytes([
                            value[12], value[13], value[14], value[15],
                            value[16], value[17], value[18], value[19],
                        ]);
                        
                        if overflow_page_id == 0 {
                            return Err(StorageError::InvalidData(
                                format!("Overflow page_id is 0 for key in page {}", current_leaf_id)
                            ));
                        }
                        
                        self.read_overflow_chain(overflow_page_id, total_size)?
                    } else {
                        // Normal inline value
                        value.clone()
                    };
                    
                    results.push((key.clone(), actual_value));
                }
            }
            
            // Move to next leaf
            current_leaf_id = page.next_leaf;
        }
        
        Ok(())
    }
    
    /// Flush all dirty pages to disk
    pub fn flush(&mut self) -> Result<()> {
        let _lock = self.flush_lock.lock()
            .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
        
        let cache = self.page_cache.read()
            .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
        
        for (_, page_arc) in cache.iter() {
            let page = page_arc.read()
                .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
            
            if page.dirty {
                // 🔧 CRITICAL FIX: Check page size BEFORE write
                // Problem: flush() bypasses insert_internal() split checks
                // Solution: Validate serialized size before writing
                let serialized_size = page.calculate_serialized_size(self.key_size);
                
                if serialized_size > PAGE_SIZE {
                    let page_id = page.page_id;
                    let num_keys = page.num_keys;
                    let is_leaf = page.is_leaf;
                    drop(page);
                    
                    return Err(StorageError::Corruption(
                        format!(
                            "BTree flush error: Page {} ({}) has {} keys requiring {} bytes (exceeds PAGE_SIZE {}). \
                            This indicates the page wasn't split during insert. \
                            Root cause: Batch inserts accumulated too many entries before flush. \
                            Solution: Reduce pending buffer size or insert through normal insert_internal() path.",
                            page_id,
                            if is_leaf { "leaf" } else { "internal" },
                            num_keys,
                            serialized_size,
                            PAGE_SIZE
                        )
                    ));
                }
                
                drop(page);
                let mut page_ref = page_arc.write()
                    .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
                self.write_page(&*page_ref)?;
                page_ref.dirty = false;  // Mark as clean after write
            }
        }
        
        drop(cache);
        
        // Sync file
        let file = self.storage_file.read()
            .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
        file.sync_all()?;
        drop(file);
        
        // CRITICAL: Sync superblock to persist next_page_id
        self.sync_superblock()?;
        
        // 🔥 MEMORY OPTIMIZATION: Aggressively clear cache after flush
        // Problem: Cache accumulates pages, causing memory to grow from 10MB → 59MB
        // Solution: Clear all cached pages after flush (they're all on disk now)
        // Trade-off: Next read will reload from disk, but memory stays bounded
        {
            let mut cache = self.page_cache.write()
                .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
            
            let old_size = cache.len();
            cache.clear();  // ✅ Clear all cached pages
            
            // Optionally keep root page for faster next insert
            // (skipped for now to minimize memory)
            
            if old_size > self.config.cache_size {
                // println!("    ↳ [CACHE-CLEAR] Cleared {} pages", old_size);
            }
        }
        
        Ok(())
    }
    
    /// Write page to disk
    fn write_page(&self, page: &Page<K>) -> Result<()> {
        // CRITICAL: Convert all large values to overflow markers BEFORE serialization
        // This must be done on a working copy to avoid modifying the input
        let mut working_page = page.clone();
        
        if working_page.is_leaf {
            for i in 0..working_page.values.len() {
                let value = &working_page.values[i];
                let is_overflow_marker = value.len() == 20 
                    && value[0..4] == OVERFLOW_MARKER.to_le_bytes();
                
                // Only convert raw large values that aren't already markers
                if value.len() > OVERFLOW_THRESHOLD && !is_overflow_marker {
                    // Write overflow chain and get page ID
                    let overflow_page_id = self.write_overflow_chain(value)?;
                    
                    // Replace with overflow marker in working copy
                    let mut marker = Vec::with_capacity(20);
                    marker.extend_from_slice(&OVERFLOW_MARKER.to_le_bytes());
                    marker.extend_from_slice(&overflow_page_id.to_le_bytes());
                    let total_size = value.len() as u64;
                    marker.extend_from_slice(&total_size.to_le_bytes());
                    working_page.values[i] = marker;
                }
            }
        }
        
        // Now serialize the working page (all large values are now markers)
        let buf = working_page.serialize(self.key_size)?;
        
        // No need to track assigned IDs - they're already in working_page.values
        
        // All values have been converted, no need for additional processing
        
        let mut file = self.storage_file.write()
            .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
        
        // Superblock takes 256 bytes, pages start at offset 256
        let offset = 256 + (working_page.page_id - 1) * PAGE_SIZE as u64;
        
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&buf)?;
        
        if self.config.immediate_sync {
            file.sync_all()?;
        }
        
        // Update cache with the working page (which has all conversions applied)
        let mut cache = self.page_cache.write()
            .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
        working_page.dirty = false;  // Mark as clean after write
        
        cache.put(working_page.page_id, Arc::new(RwLock::new(working_page)));
        
        Ok(())
    }
    
    /// Read page from disk
    fn read_page(&self, page_id: u64) -> Result<Page<K>> {
        if page_id == 0 || page_id > 1_000_000_000 {
            return Err(StorageError::InvalidData(
                format!("Invalid page_id: {}", page_id)
            ));
        }
        
        
        // Check cache first
        {
            let mut cache = self.page_cache.write()
                .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
            
            if let Some(page_arc) = cache.get(&page_id) {
                let page = page_arc.read()
                    .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
                
                // Count large values for debugging
                let large_count = if page.is_leaf {
                    page.values.iter().filter(|v| v.len() > OVERFLOW_THRESHOLD).count()
                } else {
                    0
                };
                
                if large_count > 0 {
                    eprintln!("DEBUG read_page: Page {} from cache has {} UNCONVERTED large values!", 
                             page_id, large_count);
                }
                
                return Ok((*page).clone());
            }
        }
        
        // Read from disk
        let mut file = self.storage_file.write()
            .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
        
        let offset = 256 + (page_id - 1) * PAGE_SIZE as u64;
        
        // Check if file is large enough
        let file_len = file.metadata()?.len();
        let required_len = offset + PAGE_SIZE as u64;
        
        if file_len < required_len {
            // File too small, need to extend it
            if required_len > 1_000_000_000 {
                println!("WARN: Extending file to {} bytes (page_id={})", required_len, page_id);
            }
            file.set_len(required_len).map_err(|e| {
                StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to extend file to {} bytes for page {}: {}", required_len, page_id, e)
                ))
            })?;
        }
        
        file.seek(SeekFrom::Start(offset))?;
        
        let mut buf = vec![0u8; PAGE_SIZE];
        file.read_exact(&mut buf)?;
        
        let mut page = Page::deserialize(page_id, &buf, self.key_size)?;
        page.dirty = false;  // Mark as clean since we just read from disk
        
        // Add to cache
        let mut cache = self.page_cache.write()
            .map_err(|_| StorageError::Index("Lock poisoned".into()))?;
        cache.put(page_id, Arc::new(RwLock::new(page.clone())));
        
        Ok(page)
    }
}

/// SuperBlock for generic B+Tree
#[derive(serde::Serialize, serde::Deserialize)]
struct SuperBlock {
    magic: u32,
    version: u32,
    root_page_id: u64,
    next_page_id: u64,
    key_size: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_u32_key_trait() {
        let key = 12345u32;
        let bytes = key.serialize();
        assert_eq!(bytes.len(), 4);
        
        let decoded = u32::deserialize(&bytes).unwrap();
        assert_eq!(key, decoded);
    }
    
    #[test]
    fn test_create_btree() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.gbtree");
        
        let _tree = GenericBTree::<u32>::new(path).unwrap();
    }
    
    #[test]
    fn test_insert_and_get() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.gbtree");
        
        let mut tree = GenericBTree::<u32>::new(path.clone()).unwrap();
        
        // Insert some key-value pairs
        let key1 = 100u32;
        let value1 = b"Hello World".to_vec();
        let result = tree.insert(key1, value1.clone());
        println!("Insert result for key {}: {:?}", key1, result);
        
        let key2 = 200u32;
        let value2 = b"Rust BTree".to_vec();
        tree.insert(key2, value2.clone()).unwrap();
        
        // Flush to disk
        tree.flush().unwrap();
        println!("Flushed to disk");
        
        // Retrieve values
        let result1 = tree.get(&key1);
        println!("Get result for key {}: {:?}", key1, result1);
        assert_eq!(result1.unwrap(), Some(value1.clone()));
        
        let result2 = tree.get(&key2).unwrap();
        assert_eq!(result2, Some(value2.clone()));
        
        // Query non-existent key
        let result3 = tree.get(&300u32).unwrap();
        assert_eq!(result3, None);
    }
    
    #[test]
    fn test_update_existing_key() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.gbtree");
        
        let mut tree = GenericBTree::<u32>::new(path).unwrap();
        
        let key = 100u32;
        let value1 = b"First Value".to_vec();
        let value2 = b"Updated Value".to_vec();
        
        // Insert
        tree.insert(key, value1.clone()).unwrap();
        assert_eq!(tree.get(&key).unwrap(), Some(value1));
        
        // Update
        tree.insert(key, value2.clone()).unwrap();
        assert_eq!(tree.get(&key).unwrap(), Some(value2));
    }
    
    #[test]
    fn test_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.gbtree");
        
        // Create tree, insert data, flush
        {
            let mut tree = GenericBTree::<u32>::new(path.clone()).unwrap();
            tree.insert(42u32, b"persisted data".to_vec()).unwrap();
            tree.flush().unwrap();
        }
        
        // Reopen tree and verify data persisted
        {
            let tree = GenericBTree::<u32>::new(path).unwrap();
            let result = tree.get(&42u32).unwrap();
            assert_eq!(result, Some(b"persisted data".to_vec()));
        }
    }
    
    #[test]
    fn test_large_values() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.gbtree");
        
        let mut tree = GenericBTree::<u32>::new(path).unwrap();
        
        // Insert large value (1KB)
        let large_value = vec![0x42u8; 1024];
        tree.insert(1u32, large_value.clone()).unwrap();
        
        // Retrieve and verify
        let result = tree.get(&1u32).unwrap();
        assert_eq!(result, Some(large_value));
    }
}
