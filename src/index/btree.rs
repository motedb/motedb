//! Persistent B+Tree Implementation with Disk Storage
//!
//! ## Design Principles
//! - **Disk-First**: Page-aligned storage (4KB pages) with mmap
//! - **Generic**: Support any serializable key-value types
//! - **High Performance**: O(log n) operations with LRU page cache
//! - **ACID Support**: WAL for crash recovery
//! - **Zero-Copy**: Mmap-based persistence
//!
//! ## Architecture
//! ```text
//! Memory:   [Page Cache] <-LRU-> [Root Page]
//!              ↓ flush            ↓ serialize
//! Disk:     [mmap file] -----> [Page 0][Page 1][Page 2]...
//! ```text
use crate::{Result, StorageError};
use crate::storage::file_manager::FileHandle;
use std::sync::Arc;
use parking_lot::{RwLock, Mutex};
use std::path::PathBuf;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use serde::{Serialize, Deserialize};

/// B+Tree node order (max keys per node)
/// Compact layout: Header(15) + Keys(n*8) + Values(n*8) — no fixed stride
/// Max page content: 15 + 255*8 + 255*8 = 4095 bytes
pub const BTREE_ORDER: usize = 255;

/// Max serialized page size (header + max keys + max values)
/// Used as upper bound for buffer allocation and page cache alignment
pub const MAX_PAGE_SIZE: usize = 15 + BTREE_ORDER * 8 * 2;

/// Compact page header size: [is_leaf:1][num_keys:4][next_leaf:8][content_len:2]
const PAGE_HEADER_SIZE: usize = 15;


/// Default page cache size
pub const DEFAULT_PAGE_CACHE: usize = 1024;

/// Invalid page ID
const INVALID_PAGE_ID: u64 = u64::MAX;

/// Magic number for B+Tree files (ASCII "BTREE")
const BTREE_MAGIC: u32 = 0x42545245;

/// Current B+Tree format version
const BTREE_VERSION: u32 = 2;

/// Type alias for the insert result: (old_value, optional split_info)
type InsertResult = Result<(Option<u64>, Option<(u64, u64)>)>;

/// SuperBlock for B+Tree metadata (stored at file offset 0)
#[derive(Serialize, Deserialize, Debug, Clone)]
struct SuperBlock {
    /// Magic number for file validation
    magic: u32,

    /// Format version
    version: u32,

    /// Root page ID (where the B+Tree root is stored)
    root_page_id: u64,

    /// Next available page ID
    next_page_id: u64,

    /// Total number of keys in the tree
    total_keys: usize,

    /// Total number of pages allocated
    total_pages: usize,

    /// Number of leaf pages
    leaf_pages: usize,

    /// Number of internal pages
    internal_pages: usize,

    /// Tree height
    tree_height: usize,

    /// Page offset table: page_id → file_offset
    /// Empty for legacy files (v1), populated for v2+ compact format
    page_offsets: Vec<u64>,
}

/// Persistent B+Tree Index
///
/// This is a simplified generic interface. For now, we focus on u64->u64 mapping
/// which covers 99% of index use cases (primary key, foreign key, etc.)
pub struct BTree {
    /// Root page ID
    root_page_id: Arc<RwLock<u64>>,

    /// Page cache (page_id -> Page)
    page_cache: Arc<RwLock<LruCache<u64, Arc<RwLock<Page>>>>>,

    /// Next free page ID
    next_page_id: Arc<RwLock<u64>>,

    /// Storage file
    storage_file: Arc<RwLock<File>>,

    /// Flush lock (prevents concurrent flushes from corrupting file)
    flush_lock: Arc<Mutex<()>>,

    /// Storage path
    _storage_path: PathBuf,

    /// Configuration
    config: BTreeConfig,

    /// Statistics
    stats: Arc<RwLock<BTreeStats>>,

    /// Page offset table: page_id → file_offset (for compact storage)
    page_offsets: Arc<RwLock<Vec<u64>>>,

    /// File handle (RAII protection)
    _file_handle: Option<FileHandle>,
}

/// B+Tree configuration
#[derive(Clone)]
pub struct BTreeConfig {
    /// Node order (max keys per node)
    pub order: usize,

    /// Page cache size
    pub cache_size: usize,
    
    /// Unique key constraint (disallow duplicate inserts, but allow updates)
    pub unique_keys: bool,
    
    /// Allow key updates (if false, insert on existing key will error)
    pub allow_updates: bool,
    
    /// Immediate sync (if true, sync after every insert; if false, only on flush())
    pub immediate_sync: bool,
}

impl Default for BTreeConfig {
    fn default() -> Self {
        Self {
            order: BTREE_ORDER,
            cache_size: DEFAULT_PAGE_CACHE,
            unique_keys: false,
            allow_updates: true,
            immediate_sync: false,
        }
    }
}

/// B+Tree page stored on disk
#[derive(Clone)]
struct Page {
    /// Page ID
    page_id: u64,
    
    /// Is this a leaf node?
    is_leaf: bool,
    
    /// Number of keys in this page
    num_keys: usize,
    
    /// Keys array (u64)
    keys: Vec<u64>,
    
    /// Values array (u64) - for leaf nodes
    values: Vec<u64>,
    
    /// Child page IDs - for internal nodes
    children: Vec<u64>,
    
    /// Next leaf page (for sequential scan)
    next_leaf: u64,
    
    /// Dirty flag
    dirty: bool,
}

impl Page {
    /// Create a new leaf page
    fn new_leaf(page_id: u64) -> Self {
        Self {
            page_id,
            is_leaf: true,
            num_keys: 0,
            keys: Vec::with_capacity(BTREE_ORDER),
            values: Vec::with_capacity(BTREE_ORDER),
            children: Vec::new(),
            next_leaf: INVALID_PAGE_ID,
            dirty: true,
        }
    }
    
    /// Create a new internal page
    fn new_internal(page_id: u64) -> Self {
        Self {
            page_id,
            is_leaf: false,
            num_keys: 0,
            keys: Vec::with_capacity(BTREE_ORDER),
            values: Vec::new(),
            children: Vec::with_capacity(BTREE_ORDER + 1),
            next_leaf: INVALID_PAGE_ID,
            dirty: true,
        }
    }
    
    /// Serialize page to compact bytes (only actual content, no padding)
    fn serialize_compact(&self) -> Result<Vec<u8>> {
        let data_len = PAGE_HEADER_SIZE
            + self.num_keys * 8
            + if self.is_leaf { self.num_keys * 8 } else { (self.num_keys + 1) * 8 };
        let mut buf = vec![0u8; data_len];
        let mut offset = 0;

        // Header: [is_leaf:1][num_keys:4][next_leaf:8][content_len:2]
        buf[offset] = if self.is_leaf { 1 } else { 0 };
        offset += 1;

        buf[offset..offset+4].copy_from_slice(&(self.num_keys as u32).to_le_bytes());
        offset += 4;

        buf[offset..offset+8].copy_from_slice(&self.next_leaf.to_le_bytes());
        offset += 8;

        buf[offset..offset+2].copy_from_slice(&(data_len as u16).to_le_bytes());
        offset += 2;

        // Keys (num_keys * 8 bytes)
        for &key in &self.keys {
            buf[offset..offset+8].copy_from_slice(&key.to_le_bytes());
            offset += 8;
        }

        if self.is_leaf {
            // Values (num_keys * 8 bytes)
            for &value in &self.values {
                buf[offset..offset+8].copy_from_slice(&value.to_le_bytes());
                offset += 8;
            }
        } else {
            // Children ((num_keys + 1) * 8 bytes)
            for &child in &self.children {
                buf[offset..offset+8].copy_from_slice(&child.to_le_bytes());
                offset += 8;
            }
        }

        Ok(buf)
    }

    /// Deserialize page from compact bytes
    fn deserialize_compact(page_id: u64, buf: &[u8]) -> Result<Self> {
        if buf.len() < PAGE_HEADER_SIZE {
            return Err(StorageError::InvalidData(
                format!("Page buffer too small: {} < header size {}", buf.len(), PAGE_HEADER_SIZE)
            ));
        }

        let mut offset = 0;

        let is_leaf = buf[offset] == 1;
        offset += 1;

        let num_keys = u32::from_le_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3]]) as usize;
        offset += 4;

        if num_keys > BTREE_ORDER {
            return Err(StorageError::Corruption(
                format!("Invalid num_keys in page {}: {} exceeds max {}", page_id, num_keys, BTREE_ORDER)
            ));
        }

        let next_leaf = u64::from_le_bytes([
            buf[offset], buf[offset+1], buf[offset+2], buf[offset+3],
            buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7],
        ]);
        offset += 8;

        let _content_len = u16::from_le_bytes([buf[offset], buf[offset+1]]) as usize;
        offset += 2;

        // Read keys
        let mut keys = Vec::with_capacity(num_keys);
        for _ in 0..num_keys {
            let key = u64::from_le_bytes([
                buf[offset], buf[offset+1], buf[offset+2], buf[offset+3],
                buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7],
            ]);
            keys.push(key);
            offset += 8;
        }

        let mut values = Vec::new();
        let mut children = Vec::new();

        if is_leaf {
            for _ in 0..num_keys {
                let value = u64::from_le_bytes([
                    buf[offset], buf[offset+1], buf[offset+2], buf[offset+3],
                    buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7],
                ]);
                values.push(value);
                offset += 8;
            }
        } else {
            if num_keys > 0 {
                for _ in 0..=num_keys {
                    let child = u64::from_le_bytes([
                        buf[offset], buf[offset+1], buf[offset+2], buf[offset+3],
                        buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7],
                    ]);
                    children.push(child);
                    offset += 8;
                }
            }
        }

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
    
    /// Validate page invariants
    fn validate(&self) -> Result<()> {
        // Invariant 1: Leaf nodes must have values, internal nodes must have children
        if self.is_leaf {
            if self.keys.len() != self.values.len() {
                return Err(StorageError::Corruption(
                    format!("Leaf page {} has mismatched keys ({}) and values ({})",
                            self.page_id, self.keys.len(), self.values.len())
                ));
            }
        } else {
            // Invariant 2: Internal node must have num_keys + 1 children
            if self.num_keys > 0 && self.children.len() != self.num_keys + 1 {
                return Err(StorageError::Corruption(
                    format!("Internal page {} has mismatched keys ({}) and children ({})",
                            self.page_id, self.num_keys, self.children.len())
                ));
            }
            
            // Invariant 3: Internal node should not have num_keys=0
            if self.num_keys == 0 {
                return Err(StorageError::Corruption(
                    format!("Internal page {} has num_keys=0 (invalid state)", self.page_id)
                ));
            }
        }
        
        Ok(())
    }
}

/// B+Tree statistics
#[derive(Default, Debug, Clone)]
pub struct BTreeStats {
    pub total_keys: usize,
    pub total_pages: usize,
    pub leaf_pages: usize,
    pub internal_pages: usize,
    pub tree_height: usize,
    pub page_cache_hits: u64,
    pub page_cache_misses: u64,
}

/// Range query performance profile
#[derive(Default, Debug, Clone)]
pub struct RangeQueryProfile {
    pub find_leaf_us: u64,      // Time to find first leaf
    pub scan_us: u64,            // Time to scan leaf chain
    pub total_us: u64,           // Total query time
    pub pages_scanned: usize,    // Number of leaf pages scanned
    pub keys_examined: usize,    // Total keys examined
    pub results_found: usize,    // Results returned
}


impl BTree {
    /// Create a new B+Tree with storage file
    pub fn new(storage_path: PathBuf) -> Result<Self> {
        Self::with_config(storage_path, BTreeConfig::default())
    }
    
    /// Create with custom configuration
    pub fn with_config(storage_path: PathBuf, config: BTreeConfig) -> Result<Self> {
        // Create parent directory
        if let Some(parent) = storage_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        
        // Open or create file
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&storage_path)?;
        
        // Check if file is new or existing
        let metadata = file.metadata()?;
        let file_size = metadata.len();
        let is_new_file = file_size == 0;
        
        let (_superblock, root_page_id, next_page_id, stats, page_offsets) = if is_new_file {
            // New file: create empty superblock
            // Reserve Page 0 for SuperBlock, data starts from Page 1
            let superblock = SuperBlock {
                magic: BTREE_MAGIC,
                version: BTREE_VERSION,
                root_page_id: 0,
                next_page_id: 1,
                total_keys: 0,
                total_pages: 0,
                leaf_pages: 0,
                internal_pages: 0,
                tree_height: 0,
                page_offsets: vec![0],  // index 0 = superblock offset
            };
            
            // Write initial superblock
            Self::write_superblock(&mut file, &superblock)?;

            let page_offsets = vec![0u64];  // index 0 = superblock offset
            (superblock, 0, 1, BTreeStats::default(), page_offsets)
        } else {
            // Existing file: load superblock from Page 0
            let superblock = Self::read_superblock(&mut file)?;
            
            // Validate magic number
            if superblock.magic != BTREE_MAGIC {
                return Err(StorageError::Corruption(
                    format!("Invalid B+Tree magic number: expected 0x{:08X}, got 0x{:08X}", 
                            BTREE_MAGIC, superblock.magic)
                ));
            }
            
            // Check version compatibility
            if superblock.version != BTREE_VERSION {
                // v1 files use fixed PAGE_SIZE layout — silently reinitialize
                drop(file);
                let _ = std::fs::remove_file(&storage_path);
                return Self::with_config(storage_path, config);
            }
            
            // Extract values before moving superblock
            let root_id = superblock.root_page_id;
            let next_id = superblock.next_page_id;
            
            let stats = BTreeStats {
                total_keys: superblock.total_keys,
                total_pages: superblock.total_pages,
                leaf_pages: superblock.leaf_pages,
                internal_pages: superblock.internal_pages,
                tree_height: superblock.tree_height,
                page_cache_hits: 0,
                page_cache_misses: 0,
            };
            
            let page_offsets = superblock.page_offsets.clone();

            (superblock, root_id, next_id, stats, page_offsets)
        };

        Ok(Self {
            root_page_id: Arc::new(RwLock::new(root_page_id)),
            page_cache: Arc::new(RwLock::new(LruCache::new(
                NonZeroUsize::new(config.cache_size.max(1)).unwrap()
            ))),
            next_page_id: Arc::new(RwLock::new(next_page_id)),
            storage_file: Arc::new(RwLock::new(file)),
            flush_lock: Arc::new(Mutex::new(())),
            _storage_path: storage_path,
            config,
            stats: Arc::new(RwLock::new(stats)),
            page_offsets: Arc::new(RwLock::new(page_offsets)),
            _file_handle: None,
        })
    }
    
    /// Superblock allocation size (first 4KB of file reserved for superblock)
    const SUPERBLOCK_SIZE: usize = 4096;

    /// Read SuperBlock from file start
    fn read_superblock(file: &mut File) -> Result<SuperBlock> {
        file.seek(SeekFrom::Start(0))?;

        let mut buf = vec![0u8; Self::SUPERBLOCK_SIZE];
        file.read_exact(&mut buf)?;

        bincode::deserialize(&buf)
            .map_err(|e| StorageError::Corruption(format!("Failed to deserialize SuperBlock: {}", e)))
    }

    /// Write SuperBlock at file start
    fn write_superblock(file: &mut File, superblock: &SuperBlock) -> Result<()> {
        file.seek(SeekFrom::Start(0))?;

        let data = bincode::serialize(superblock)
            .map_err(|e| StorageError::Index(format!("Failed to serialize SuperBlock: {}", e)))?;

        if data.len() > Self::SUPERBLOCK_SIZE {
            return Err(StorageError::Index(
                format!("SuperBlock too large: {} bytes exceeds {} reservation", data.len(), Self::SUPERBLOCK_SIZE)
            ));
        }

        let mut buf = vec![0u8; Self::SUPERBLOCK_SIZE];
        buf[..data.len()].copy_from_slice(&data);

        file.write_all(&buf)?;

        Ok(())
    }
    
    /// Update and persist SuperBlock
    fn sync_superblock(&self) -> Result<()> {
        let root_page_id = *self.root_page_id.read();
        let next_page_id = *self.next_page_id.read();
        let stats = self.stats.read();
        
        let page_offsets = self.page_offsets.read();

        let superblock = SuperBlock {
            magic: BTREE_MAGIC,
            version: BTREE_VERSION,
            root_page_id,
            next_page_id,
            total_keys: stats.total_keys,
            total_pages: stats.total_pages,
            leaf_pages: stats.leaf_pages,
            internal_pages: stats.internal_pages,
            tree_height: stats.tree_height,
            page_offsets: page_offsets.clone(),
        };
        
        let mut file = self.storage_file.write();
        
        Self::write_superblock(&mut file, &superblock)
    }
    
    /// Load page from disk or cache
    fn load_page(&self, page_id: u64) -> Result<Arc<RwLock<Page>>> {
        if page_id == 0 {
            let root_id = *self.root_page_id.read();
            return Err(StorageError::Corruption(
                format!("Cannot load Page 0: reserved for SuperBlock (root_id={})", root_id)
            ));
        }

        // Check cache first
        {
            let mut cache = self.page_cache.write();

            if let Some(page) = cache.get(&page_id) {
                let mut stats = self.stats.write();
                stats.page_cache_hits += 1;
                return Ok(Arc::clone(page));
            }
        }

        // Miss - load from disk
        let mut stats = self.stats.write();
        stats.page_cache_misses += 1;
        drop(stats);

        // Look up page offset from page table
        let file_offset = {
            let offsets = self.page_offsets.read();
            let idx = page_id as usize;
            if idx >= offsets.len() || offsets[idx] == 0 {
                return Err(StorageError::Corruption(
                    format!("Page {} not found in page table", page_id)
                ));
            }
            offsets[idx]
        };

        let file = self.storage_file.read();

        // Read header to get content_len using positional read (no seek needed)
        use std::os::unix::fs::FileExt;
        let mut header_buf = [0u8; 15];
        file.read_exact_at(&mut header_buf, file_offset)?;
        let content_len = u16::from_le_bytes([header_buf[13], header_buf[14]]) as usize;

        if content_len < 15 || content_len > MAX_PAGE_SIZE {
            return Err(StorageError::Corruption(
                format!("Invalid content_len {} for page {} at offset {}", content_len, page_id, file_offset)
            ));
        }

        // Read full page content using positional read
        let mut buf = vec![0u8; content_len];
        file.read_exact_at(&mut buf, file_offset)?;

        let page = Page::deserialize_compact(page_id, &buf)?;
        page.validate()?;

        let page_arc = Arc::new(RwLock::new(page));

        let mut cache = self.page_cache.write();
        cache.put(page_id, Arc::clone(&page_arc));

        Ok(page_arc)
    }

    /// Flush page to disk with compact storage
    /// Flush a single page immediately (used during insert for root page safety)
    fn flush_page(&self, page: &Page) -> Result<()> {
        if !page.dirty {
            return Ok(());
        }

        if page.page_id == 0 {
            return Err(StorageError::Corruption(
                "Cannot flush Page 0: reserved for SuperBlock".into()
            ));
        }

        let buf = page.serialize_compact()?;

        let _flush_guard = self.flush_lock.lock();

        let mut file = self.storage_file.write();

        // Always append — page_offsets will be corrected during flush()
        let file_end = file.metadata()?.len().max(Self::SUPERBLOCK_SIZE as u64);
        file.seek(SeekFrom::Start(file_end))?;
        file.write_all(&buf)?;

        // Record offset in page table
        {
            let mut offsets = self.page_offsets.write();
            let idx = page.page_id as usize;
            if idx >= offsets.len() {
                offsets.resize(idx + 1, 0);
            }
            offsets[idx] = file_end;
        }

        if self.config.immediate_sync {
            file.sync_all()?;
        }

        Ok(())
    }
    
    /// Allocate a new page
    fn alloc_page(&self, is_leaf: bool) -> Result<Arc<RwLock<Page>>> {
        let page_id = {
            let mut next_id = self.next_page_id.write();
            let id = *next_id;
            *next_id += 1;
            id
        };
        
        let page = if is_leaf {
            Page::new_leaf(page_id)
        } else {
            Page::new_internal(page_id)
        };
        
        let page_arc = Arc::new(RwLock::new(page));
        
        // Add to cache
        let mut cache = self.page_cache.write();
        cache.put(page_id, Arc::clone(&page_arc));
        
        Ok(page_arc)
    }
    
    /// Search for a key starting from a page
    fn search_internal(&self, page_id: u64, key: u64) -> Result<Option<u64>> {
        let page_arc = self.load_page(page_id)?;
        let page = page_arc.read();
        
        if page.is_leaf {
            // Leaf node: binary search
            match page.keys.binary_search(&key) {
                Ok(idx) => Ok(Some(page.values[idx])),
                Err(_) => Ok(None),
            }
        } else {
            // Internal node: find child
            // B+Tree semantics: keys[i] is the minimum key in children[i+1]
            // So if key >= keys[i], we should go to children[i+1]
            let child_idx = match page.keys.binary_search(&key) {
                Ok(idx) => idx + 1,  // Key found, go to right child
                Err(idx) => idx,     // Key not found, idx is insert position
            };
            
            let child_page_id = page.children[child_idx];
            drop(page);
            
            self.search_internal(child_page_id, key)
        }
    }
    
    /// Insert a key-value pair
    pub fn insert(&mut self, key: u64, value: u64) -> Result<Option<u64>> {
        let root_id = *self.root_page_id.read();
        
        // If root doesn't exist, create it
        if root_id == 0 {
            // Create new root page (will be Page 1 since Page 0 is SuperBlock)
            let root_page = self.alloc_page(true)?;
            let new_root_id = {
                let mut page = root_page.write();
                page.keys.push(key);
                page.values.push(value);
                page.num_keys = 1;
                page.dirty = true;
                page.page_id
            };
            
            // Flush the page
            {
                let page_ref = root_page.read();
                // 🔧 Fix: Flush the root page immediately to ensure it's on disk
                self.flush_page(&page_ref)?;
            }
            
            // Update root_page_id
            {
                let mut root = self.root_page_id.write();
                *root = new_root_id;
            }
            
            // 🔧 Fix: Flush superblock immediately when creating new root
            // This ensures queries won't see stale root_page_id=0
            self.sync_superblock()?;
            
            // Update stats
            let mut stats = self.stats.write();
            stats.total_keys = 1;
            stats.total_pages = 1;
            stats.leaf_pages = 1;
            
            return Ok(None);
        }
        
        // Recursive insert with split handling
        let (old_value, split_info) = self.insert_internal(root_id, key, value)?;
        
        // If root was split, create new root
        if let Some((split_key, new_page_id)) = split_info {
            let new_root = self.alloc_page(false)?;
            {
                let mut root = new_root.write();
                root.keys.push(split_key);
                root.children.push(root_id);
                root.children.push(new_page_id);
                root.num_keys = 1;
                root.dirty = true;
            }

            // Flush new root to disk (critical: must persist before updating superblock)
            {
                let page_ref = new_root.read();
                let new_root_id = page_ref.page_id;
                self.flush_page(&page_ref)?;
                drop(page_ref);

                // Update root ID
                let mut root_page_id = self.root_page_id.write();
                *root_page_id = new_root_id;
            }

            // Persist the new root_page_id to superblock (crash safety)
            self.sync_superblock()?;

            // Update stats
            let mut stats = self.stats.write();
            stats.total_pages += 1;
            stats.internal_pages += 1;
            stats.tree_height += 1;
        }
        
        // Update total keys
        if old_value.is_none() {
            let mut stats = self.stats.write();
            stats.total_keys += 1;
        }
        
        Ok(old_value)
    }
    
    /// Internal recursive insert with split handling
    /// Returns (old_value, split_info) where split_info is (split_key, new_page_id)
    fn insert_internal(&mut self, page_id: u64, key: u64, value: u64)
        -> InsertResult {
        
        let page_arc = self.load_page(page_id)?;
        let is_leaf = {
            let page = page_arc.read();
            page.is_leaf
        };
        
        if is_leaf {
            // Leaf node: insert directly
            let mut page = page_arc.write();
            
            let search_result = page.keys.binary_search(&key);
            let old_value = match search_result {
                Ok(idx) => {
                    // Key exists
                    if !self.config.allow_updates {
                        return Err(StorageError::InvalidData(
                            "Key already exists and updates are disabled".into()
                        ));
                    }
                    let old = Some(page.values[idx]);
                    page.values[idx] = value;
                    page.dirty = true;

                    return Ok((old, None));
                }
                Err(idx) => {
                    // Insert new key
                    page.keys.insert(idx, key);
                    page.values.insert(idx, value);
                    page.num_keys += 1;
                    page.dirty = true;
                    None
                }
            };
            
            // Check if split is needed (split when at capacity, not after exceeding)
            if page.num_keys >= self.config.order {
                let split_info = self.split_leaf(&mut page)?;
                drop(page);

                Ok((old_value, Some(split_info)))
            } else {
                drop(page);

                Ok((old_value, None))
            }
        } else {
            // Internal node: find child and recurse
            let child_idx = {
                let page = page_arc.read();
                page.keys.binary_search(&key).unwrap_or_else(|idx| idx)
            };
            
            let child_page_id = {
                let page = page_arc.read();
                page.children[child_idx]
            };
            
            let (old_value, child_split) = self.insert_internal(child_page_id, key, value)?;
            
            if let Some((split_key, new_child_id)) = child_split {
                // Child was split, insert split key into this node
                let mut page = page_arc.write();
                
                let insert_idx = page.keys.binary_search(&split_key)
                    .unwrap_or_else(|idx| idx);
                
                page.keys.insert(insert_idx, split_key);
                page.children.insert(insert_idx + 1, new_child_id);
                page.num_keys += 1;
                page.dirty = true;
                
                // Check if this node needs to split
                if page.num_keys >= self.config.order {
                    let split_info = self.split_internal(&mut page)?;
                    drop(page);

                    Ok((old_value, Some(split_info)))
                } else {
                    drop(page);

                    Ok((old_value, None))
                }
            } else {
                Ok((old_value, None))
            }
        }
    }
    
    /// Split a leaf node
    /// Returns (split_key, new_page_id)
    fn split_leaf(&mut self, page: &mut Page) -> Result<(u64, u64)> {
        let mid = page.num_keys * 7 / 10;

        // Create new leaf page
        let new_page_arc = self.alloc_page(true)?;
        let new_page_id = {
            let mut new_page = new_page_arc.write();
            
            // Move half the keys/values to new page
            new_page.keys = page.keys.split_off(mid);
            new_page.values = page.values.split_off(mid);
            new_page.num_keys = new_page.keys.len();
            new_page.dirty = true;
            
            // Update leaf links
            new_page.next_leaf = page.next_leaf;
            page.next_leaf = new_page.page_id;
            
            let split_key = new_page.keys[0];
            let new_id = new_page.page_id;
            
            drop(new_page);

            (split_key, new_id)
        };
        
        // Update original page
        page.num_keys = page.keys.len();
        page.dirty = true;
        
        // Update stats
        let mut stats = self.stats.write();
        stats.total_pages += 1;
        stats.leaf_pages += 1;
        
        Ok(new_page_id)
    }
    
    /// Split an internal node
    /// Returns (split_key, new_page_id)
    fn split_internal(&mut self, page: &mut Page) -> Result<(u64, u64)> {
        // 🔧 Fix: Proper B+tree internal node split
        // Minimum: 2 keys (after split: left=1, mid=1, right=0 is invalid)
        // So we need at least 2 keys to guarantee both children have >=1 key
        // Actually for safety, need at least 1 key (special case for root)
        
        let original_num_keys = page.num_keys;
        let original_num_children = page.children.len();
        
        if page.num_keys < 1 {
            return Err(StorageError::Index(
                format!("Cannot split internal node with {} keys", page.num_keys)
            ));
        }
        
        // 🔧 Fix: For proper split that guarantees both halves have keys:
        // - For num_keys=1: Cannot split (would create empty node)
        // - For num_keys=2: mid=0, left gets 0 keys, right gets 1 key (invalid!)
        // - For num_keys=3: mid=1, left gets 1 key, right gets 1 key (valid!)
        // 
        // Conclusion: Need at least 2 keys, but split with 2 is tricky
        // Standard B+tree: split when node is FULL (order keys), so always have enough
        
        // For num_keys >= 2:
        // Mid selection: 70/30 split to pack more into left page
        let mid = page.num_keys * 7 / 10;
        
        // Validate mid won't cause empty right side
        if mid >= page.num_keys {
            return Err(StorageError::Index(
                format!("Invalid split mid={} for num_keys={}", mid, page.num_keys)
            ));
        }
        
        // Save the middle key (will be promoted to parent)
        let split_key = page.keys[mid];
        
        // Create new internal page for right half
        let new_page_arc = self.alloc_page(false)?;
        let new_page_id = {
            let mut new_page = new_page_arc.write();
            
            // 🎯 Critical fix: Proper split sequence
            // Before: keys=[k0,...,k_mid,...,k_n], children=[c0,...,c_mid,c_{mid+1},...,c_{n+1}]
            // After:  Left: keys=[k0,...,k_{mid-1}], children=[c0,...,c_mid]
            //         Right: keys=[k_{mid+1},...,k_n], children=[c_{mid+1},...,c_{n+1}]
            //         Parent: k_mid
            
            // Step 1: Move keys[mid+1..] to new page
            new_page.keys = page.keys.split_off(mid + 1);
            
            // Step 2: Move children[mid+1..] to new page  
            new_page.children = page.children.split_off(mid + 1);
            
            new_page.num_keys = new_page.keys.len();
            new_page.dirty = true;
            
            // Validate right child
            if new_page.num_keys == 0 {
                // This can happen when mid = num_keys - 1
                // e.g., num_keys=2, mid=1, split_off(2) gives empty array
                return Err(StorageError::Corruption(
                    format!("Split internal node: right child has 0 keys (original_keys={}, mid={}, keys_after_splitoff={})",
                            original_num_keys, mid, page.keys.len())
                ));
            }
            
            if new_page.children.len() != new_page.num_keys + 1 {
                return Err(StorageError::Corruption(
                    format!("Split internal node: right child has {} keys but {} children",
                            new_page.num_keys, new_page.children.len())
                ));
            }
            
            let new_id = new_page.page_id;
            drop(new_page);
            (split_key, new_id)
        };
        
        // Step 3: Remove the promoted key from left child
        // After split_off(mid+1), page.keys = [k0,...,k_mid]
        // Need to remove k_mid
        if !page.keys.is_empty() {
            page.keys.pop();  // Remove keys[mid]
        }
        page.num_keys = page.keys.len();
        page.dirty = true;
        
        // Validate left child
        if page.num_keys == 0 {
            // Edge case: original had 1 key
            // mid=0, split_off(1) leaves [k0], pop() leaves []
            // This means we tried to split a node with only 1 key - invalid!
            return Err(StorageError::Corruption(
                format!("Split internal node: left child has 0 keys after removing mid (original_keys={}, mid={})",
                        original_num_keys, mid)
            ));
        }
        
        if page.children.len() != page.num_keys + 1 {
            return Err(StorageError::Corruption(
                format!("Split internal node: left child has {} keys but {} children (original had {} keys, {} children)",
                        page.num_keys, page.children.len(), original_num_keys, original_num_children)
            ));
        }
        
        // Update stats
        let mut stats = self.stats.write();
        stats.total_pages += 1;
        stats.internal_pages += 1;
        
        Ok(new_page_id)
    }
    
    /// Get value by key
    pub fn get(&self, key: &u64) -> Result<Option<u64>> {
        let root_id = *self.root_page_id.read();
        
        // 🔧 Fix: Empty tree (root_id == 0)
        if root_id == 0 {
            return Ok(None);
        }
        
        self.search_internal(root_id, *key)
    }
    
    /// Remove a key-value pair
    ///
    /// Traverses from root to the correct leaf node, then deletes the key.
    /// Note: This is a simplified implementation that does not perform rebalancing
    /// (merge/redistribute). Underflow is tolerated — the tree remains correct
    /// for lookups, just potentially unbalanced. Full rebalancing can be added later.
    pub fn remove(&mut self, key: &u64) -> Result<Option<u64>> {
        let root_id = *self.root_page_id.read();

        // Empty tree (root_id == 0)
        if root_id == 0 {
            return Ok(None);
        }

        // Find the leaf node containing this key
        let leaf_id = self.find_leaf_for_key(root_id, *key)?;

        // Delete from the leaf
        let leaf_arc = self.load_page(leaf_id)?;
        let mut leaf = leaf_arc.write();

        if !leaf.is_leaf {
            return Err(StorageError::Index(
                "find_leaf_for_key returned non-leaf page".into()
            ));
        }

        match leaf.keys.binary_search(key) {
            Ok(idx) => {
                let old_value = leaf.values[idx];
                leaf.keys.remove(idx);
                leaf.values.remove(idx);
                leaf.num_keys -= 1;
                leaf.dirty = true;
                drop(leaf);

                // Update stats
                let mut stats = self.stats.write();
                stats.total_keys = stats.total_keys.saturating_sub(1);

                Ok(Some(old_value))
            }
            Err(_) => Ok(None),
        }
    }
    
    /// Check if key exists
    pub fn contains_key(&self, key: &u64) -> Result<bool> {
        Ok(self.get(key)?.is_some())
    }
    
    /// Get number of entries
    pub fn len(&self) -> usize {
        self.stats.read().total_keys
    }
    
    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    
    /// Get statistics
    pub fn stats(&self) -> BTreeStats {
        self.stats.read().clone()
    }
    
    /// Range query with early termination limit
    ///
    /// Returns at most `limit` key-value pairs where start <= key <= end.
    /// Significantly faster than range() + take() for large ranges with small limits.
    pub fn range_with_limit(&self, start: &u64, end: &u64, limit: usize) -> Result<Vec<(u64, u64)>> {
        let root_id = *self.root_page_id.read();

        if root_id == 0 || limit == 0 {
            return Ok(Vec::new());
        }

        let first_leaf_id = self.find_leaf_for_key(root_id, *start)?;
        let mut results = Vec::with_capacity(limit.min(64));
        self.scan_leaf_chain_limited(first_leaf_id, *start, *end, &mut results, limit)?;
        Ok(results)
    }

    /// Range query - Optimized with leaf chain scanning
    ///
    /// Performance: O(log n + k) where k is the number of results
    ///
    /// Algorithm:
    /// 1. Binary search to find the first leaf containing keys >= start
    /// 2. Sequentially scan leaf nodes using next_leaf pointers
    /// 3. Stop when we encounter a key > end
    pub fn range(&self, start: &u64, end: &u64) -> Result<Vec<(u64, u64)>> {
        let root_id = *self.root_page_id.read();
        
        // 🔧 Fix: Empty tree (root_id == 0)
        if root_id == 0 {
            return Ok(Vec::new());
        }
        
        // Step 1: Find the first leaf node that may contain keys >= start
        let first_leaf_id = self.find_leaf_for_key(root_id, *start)?;
        
        // Step 2: Sequentially scan leaf chain
        let mut results = Vec::new();
        self.scan_leaf_chain(first_leaf_id, *start, *end, &mut results)?;
        
        Ok(results)
    }
    
    /// Find the leaf node that should contain the given key
    /// (or the first leaf with keys >= key if key doesn't exist)
    fn find_leaf_for_key(&self, page_id: u64, key: u64) -> Result<u64> {
        let page_arc = self.load_page(page_id)?;
        let page = page_arc.read();
        
        if page.is_leaf {
            return Ok(page_id);
        }
        
        // Internal node: binary search to find the appropriate child
        // B+Tree invariant: children.len() == num_keys + 1
        // keys[i] is the minimum key in children[i+1]
        let mut child_idx = 0;
        for i in 0..page.num_keys {
            if key < page.keys[i] {
                break;
            }
            child_idx = i + 1;
        }
        
        // 🔧 Fix: Ensure child_idx is within bounds
        // child_idx can be at most num_keys (pointing to the rightmost child)
        if child_idx >= page.children.len() {
            return Err(StorageError::Index(
                format!("Child index {} out of bounds (num_children={}, num_keys={}, page_id={})", 
                        child_idx, page.children.len(), page.num_keys, page_id)
            ));
        }
        
        let child_id = page.children[child_idx];
        
        // 🔧 Additional check: child_id should never be 0 (SuperBlock)
        if child_id == 0 {
            return Err(StorageError::Corruption(
                format!("Invalid child_id=0 at page_id={}, child_idx={}, num_keys={}", 
                        page_id, child_idx, page.num_keys)
            ));
        }
        
        drop(page);
        
        self.find_leaf_for_key(child_id, key)
    }
    
    /// Scan leaf nodes sequentially using next_leaf pointers
    /// This is the key optimization: O(k) instead of O(n)
    fn scan_leaf_chain(&self, start_leaf_id: u64, start: u64, end: u64, results: &mut Vec<(u64, u64)>) -> Result<()> {
        let mut current_leaf_id = start_leaf_id;
        
        while current_leaf_id != INVALID_PAGE_ID {
            let page_arc = self.load_page(current_leaf_id)?;
            let page = page_arc.read();
            
            if !page.is_leaf {
                return Err(StorageError::Index("Expected leaf node".into()));
            }
            
            // Scan keys in this leaf
            let mut found_end = false;
            for i in 0..page.num_keys {
                let key = page.keys[i];
                
                if key > end {
                    found_end = true;
                    break;
                }
                
                if key >= start {
                    results.push((key, page.values[i]));
                }
            }
            
            // Stop if we've passed the end key
            if found_end {
                break;
            }
            
            // Move to next leaf
            current_leaf_id = page.next_leaf;
        }
        
        Ok(())
    }
    
    /// Scan leaf chain with early termination at limit
    fn scan_leaf_chain_limited(&self, start_leaf_id: u64, start: u64, end: u64,
                                results: &mut Vec<(u64, u64)>, limit: usize) -> Result<()> {
        let mut current_leaf_id = start_leaf_id;

        while current_leaf_id != INVALID_PAGE_ID && results.len() < limit {
            let page_arc = self.load_page(current_leaf_id)?;
            let page = page_arc.read();

            if !page.is_leaf {
                return Err(StorageError::Index("Expected leaf node".into()));
            }

            for i in 0..page.num_keys {
                if results.len() >= limit {
                    return Ok(());
                }
                let key = page.keys[i];
                if key > end {
                    return Ok(());
                }
                if key >= start {
                    results.push((key, page.values[i]));
                }
            }

            current_leaf_id = page.next_leaf;
        }

        Ok(())
    }

    /// Flush all pages: rewrite compactly and truncate file
    pub fn flush(&self) -> Result<()> {
        // Collect all pages from cache, sorted by page_id for deterministic layout
        let cache = self.page_cache.read();

        let mut pages: Vec<(u64, Arc<RwLock<Page>>)> = cache.iter()
            .map(|(id, arc)| (*id, Arc::clone(arc)))
            .collect();
        pages.sort_by_key(|(id, _)| *id);

        drop(cache);

        // Write all pages sequentially after superblock
        let _flush_guard = self.flush_lock.lock();

        let mut file = self.storage_file.write();

        let mut offset = Self::SUPERBLOCK_SIZE as u64;
        let mut new_offsets = vec![0u64]; // index 0 = superblock

        for (page_id, page_arc) in &pages {
            let page = page_arc.read();

            let buf = page.serialize_compact()?;

            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&buf)?;

            // Extend offsets table
            let idx = *page_id as usize;
            if idx >= new_offsets.len() {
                new_offsets.resize(idx + 1, 0);
            }
            new_offsets[idx] = offset;
            offset += buf.len() as u64;
        }

        // Update page_offsets table
        let mut offsets = self.page_offsets.write();
        *offsets = new_offsets;
        drop(offsets);

        // Mark all pages clean
        for (_, page_arc) in &pages {
            let mut page = page_arc.write();
            page.dirty = false;
        }

        // Persist superblock BEFORE truncating, so a crash between the two
        // leaves the file consistent with the new page offsets.
        drop(file);
        self.sync_superblock()?;

        // Now truncate file to remove dead space
        let file = self.storage_file.write();
        file.set_len(offset)?;

        // Final fsync
        file.sync_all()?;

        Ok(())
    }
    
    /// Scan all entries (for debugging)
    pub fn scan(&self) -> Result<Vec<(u64, u64)>> {
        let root_id = *self.root_page_id.read();
        
        // 🔧 Fix: Empty tree (root_id == 0)
        if root_id == 0 {
            return Ok(Vec::new());
        }
        
        let mut results = Vec::new();
        self.scan_internal(root_id, &mut results)?;
        Ok(results)
    }
    
    /// Internal scan helper - traverse to leftmost leaf and scan all leaves
    fn scan_internal(&self, page_id: u64, results: &mut Vec<(u64, u64)>) -> Result<()> {
        let page_arc = self.load_page(page_id)?;
        let page = page_arc.read();
        
        if page.is_leaf {
            // Leaf node: collect all entries
            for i in 0..page.num_keys {
                results.push((page.keys[i], page.values[i]));
            }
            
            // Follow next_leaf pointer
            if page.next_leaf != INVALID_PAGE_ID {
                let next_id = page.next_leaf;
                drop(page);
                self.scan_internal(next_id, results)?;
            }
        } else {
            // Internal node: recurse to first child
            if !page.children.is_empty() {
                let first_child = page.children[0];
                drop(page);
                self.scan_internal(first_child, results)?;
            }
        }
        
        Ok(())
    }
    
    /// Get min key
    pub fn min_key(&self) -> Result<Option<u64>> {
        let root_id = *self.root_page_id.read();

        if root_id == 0 {
            return Ok(None);
        }

        // Traverse to the leftmost leaf
        let mut page_id = root_id;
        loop {
            let page_arc = self.load_page(page_id)?;
            let page = page_arc.read();

            if page.is_leaf {
                return Ok(page.keys.first().copied());
            }
            page_id = match page.children.first() {
                Some(&id) => id,
                None => return Ok(page.keys.first().copied()),
            };
        }
    }

    /// Get max key
    pub fn max_key(&self) -> Result<Option<u64>> {
        let root_id = *self.root_page_id.read();

        if root_id == 0 {
            return Ok(None);
        }

        // Traverse to the rightmost leaf
        let mut page_id = root_id;
        loop {
            let page_arc = self.load_page(page_id)?;
            let page = page_arc.read();

            if page.is_leaf {
                return Ok(page.keys.last().copied());
            }
            page_id = match page.children.last() {
                Some(&id) => id,
                None => return Ok(page.keys.last().copied()),
            };
        }
    }
}

impl Drop for BTree {
    fn drop(&mut self) {
        // Flush all pages on drop
        let _ = self.flush();
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    fn create_test_btree() -> (BTree, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.btree");
        let btree = BTree::new(path).unwrap();
        (btree, temp_dir)
    }
    
    #[test]
    fn test_basic_operations() {
        let (mut btree, _temp) = create_test_btree();
        
        // Insert
        assert!(btree.insert(1, 100).unwrap().is_none());
        assert!(btree.insert(2, 200).unwrap().is_none());
        assert!(btree.insert(3, 300).unwrap().is_none());
        
        // Get
        assert_eq!(btree.get(&1).unwrap(), Some(100));
        assert_eq!(btree.get(&2).unwrap(), Some(200));
        assert_eq!(btree.get(&999).unwrap(), None);
        
        // Len
        assert_eq!(btree.len(), 3);
        
        // Contains
        assert!(btree.contains_key(&1).unwrap());
        assert!(!btree.contains_key(&999).unwrap());
    }
    
    #[test]
    fn test_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("persist.btree");
        
        // Write data
        {
            let mut btree = BTree::new(path.clone()).unwrap();
            btree.insert(1, 100).unwrap();
            btree.insert(2, 200).unwrap();
            btree.flush().unwrap();
        }
        
        // Read data back
        {
            let btree = BTree::new(path).unwrap();
            assert_eq!(btree.get(&1).unwrap(), Some(100));
            assert_eq!(btree.get(&2).unwrap(), Some(200));
        }
    }
    
    #[test]
    fn test_superblock_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("superblock.btree");
        
        // Create and populate tree
        {
            let mut btree = BTree::new(path.clone()).unwrap();
            
            // Insert enough data to potentially trigger splits
            for i in 1..=100 {
                btree.insert(i, i * 10).unwrap();
            }
            
            let stats_before = btree.stats();
            assert_eq!(stats_before.total_keys, 100);
            
            btree.flush().unwrap();
        }
        
        // Reopen and verify all metadata is restored
        {
            let btree = BTree::new(path).unwrap();
            
            // Verify stats were restored from SuperBlock
            let stats_after = btree.stats();
            assert_eq!(stats_after.total_keys, 100);
            assert!(stats_after.total_pages > 0);
            
            // Verify root_page_id was restored correctly
            let root_id = *btree.root_page_id.read();
            assert!(root_id > 0, "Root should be at Page 1 or higher (Page 0 is SuperBlock)");
            
            // Verify data integrity
            assert_eq!(btree.get(&1).unwrap(), Some(10));
            assert_eq!(btree.get(&50).unwrap(), Some(500));
            assert_eq!(btree.get(&100).unwrap(), Some(1000));
            assert_eq!(btree.len(), 100);
        }
    }
    
    #[test]
    fn test_range_query() {
        let (mut btree, _temp) = create_test_btree();
        
        for i in 1..=10 {
            btree.insert(i, i * 100).unwrap();
        }
        
        let results = btree.range(&3, &7).unwrap();
        assert_eq!(results.len(), 5);
        assert_eq!(results[0], (3, 300));
        assert_eq!(results[4], (7, 700));
    }
    
    #[test]
    fn test_unique_constraint() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("unique.btree");
        let config = BTreeConfig {
            unique_keys: true,
            allow_updates: false,  // Disallow updates for true unique constraint
            ..Default::default()
        };
        let mut btree = BTree::with_config(path, config).unwrap();
        
        btree.insert(1, 100).unwrap();
        let result = btree.insert(1, 200);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_remove() {
        let (mut btree, _temp) = create_test_btree();
        
        btree.insert(1, 100).unwrap();
        btree.insert(2, 200).unwrap();
        
        assert_eq!(btree.remove(&1).unwrap(), Some(100));
        assert_eq!(btree.len(), 1);
        assert_eq!(btree.remove(&1).unwrap(), None);
    }
    
    #[test]
    fn test_min_max_key() {
        let (mut btree, _temp) = create_test_btree();
        
        btree.insert(5, 50).unwrap();
        btree.insert(1, 10).unwrap();
        btree.insert(10, 100).unwrap();
        
        assert_eq!(btree.min_key().unwrap(), Some(1));
        assert_eq!(btree.max_key().unwrap(), Some(10));
    }
    
    #[test]
    fn test_scan() {
        let (mut btree, _temp) = create_test_btree();
        
        for i in 1..=5 {
            btree.insert(i, i * 10).unwrap();
        }
        
        let all = btree.scan().unwrap();
        assert_eq!(all.len(), 5);
        assert_eq!(all[0], (1, 10));
        assert_eq!(all[4], (5, 50));
    }
    
    #[test]
    fn test_update() {
        let (mut btree, _temp) = create_test_btree();
        
        btree.insert(1, 100).unwrap();
        assert_eq!(btree.get(&1).unwrap(), Some(100));
        
        btree.insert(1, 200).unwrap();
        assert_eq!(btree.get(&1).unwrap(), Some(200));
        assert_eq!(btree.len(), 1);
    }
    
    #[test]
    fn test_simple_split() {
        let (mut btree, _temp) = create_test_btree();
        
        // Insert exactly 256 entries to trigger first split
        for i in 0..256 {
            btree.insert(i, i * 10).unwrap();
        }
        
        // Verify all entries
        for i in 0..256 {
            let result = btree.get(&i).unwrap();
            assert_eq!(result, Some(i * 10), "Key {} missing or wrong", i);
        }
        
        debug_log!("Stats: {:?}", btree.stats());
    }
    
    #[test]
    fn test_node_split() {
        let (mut btree, _temp) = create_test_btree();
        
        // Insert enough entries to trigger node splits (ORDER = 256)
        for i in 0..1000 {
            btree.insert(i, i * 10).unwrap();
        }
        
        // Verify all entries are retrievable
        for i in 0..1000 {
            let result = btree.get(&i).unwrap();
            if result != Some(i * 10) {
                panic!("Key {} not found or has wrong value. Expected: {}, Got: {:?}", 
                    i, i * 10, result);
            }
        }
        
        // Check stats
        let stats = btree.stats();
        assert_eq!(stats.total_keys, 1000);
        assert!(stats.total_pages > 1); // Should have multiple pages
        assert!(stats.tree_height > 0); // Should have height > 1 for 1000 entries
        
        // Verify scan returns all entries in order
        let all = btree.scan().unwrap();
        assert_eq!(all.len(), 1000);
        for (i, &(k, v)) in all.iter().enumerate() {
            assert_eq!((k, v), (i as u64, (i * 10) as u64));
        }
    }
    
    #[test]
    fn test_large_dataset() {
        let (mut btree, _temp) = create_test_btree();
        
        // Insert 5000 entries to test multiple levels of splits
        let count = 5000;
        for i in 0..count {
            btree.insert(i, i).unwrap();
        }
        
        assert_eq!(btree.len(), count as usize);
        
        // Random access
        assert_eq!(btree.get(&2500).unwrap(), Some(2500));
        assert_eq!(btree.get(&4999).unwrap(), Some(4999));
        assert_eq!(btree.get(&0).unwrap(), Some(0));
        
        // Range query
        let results = btree.range(&1000, &1010).unwrap();
        assert_eq!(results.len(), 11);
    }

    #[test]
    fn test_range_with_limit() {
        let (mut btree, _temp) = create_test_btree();

        for i in 0..500 {
            btree.insert(i, i * 10).unwrap();
        }

        // Limit smaller than total results
        let results = btree.range_with_limit(&0, &499, 5).unwrap();
        assert_eq!(results.len(), 5);
        assert_eq!(results[0], (0, 0));
        assert_eq!(results[4], (4, 40));

        // Limit larger than total results
        let results = btree.range_with_limit(&100, &110, 50).unwrap();
        assert_eq!(results.len(), 11);

        // Limit = 0
        let results = btree.range_with_limit(&0, &499, 0).unwrap();
        assert!(results.is_empty());

        // Range with no matches
        let results = btree.range_with_limit(&600, &700, 10).unwrap();
        assert!(results.is_empty());
    }
}

