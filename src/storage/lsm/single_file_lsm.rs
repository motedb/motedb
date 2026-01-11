//! Single-File LSM Engine
//!
//! ## Design
//! - One file per table (e.g., users.sst)
//! - Page-based storage (4KB pages)
//! - Multi-level structure (Level 0-6)
//! - Free list for space reclamation
//!
//! ## File Layout
//! ```
//! Page 0: Superblock (metadata)
//! Page 1+: Level 0 data (unsorted, new data)
//! Page X+: Level 1 data (sorted)
//! Page Y+: Level 2-6 data (progressively larger)
//! Free List: Reclaimed pages from compaction
//! ```

use crate::{Result, StorageError};
use crate::storage::file_manager::{FileRefManager, FileHandle};
use crate::storage::checksum::{Checksum, ChecksumType};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, Mutex};
use serde::{Serialize, Deserialize};

/// Page size (4KB, standard OS page size)
pub const PAGE_SIZE: usize = 4096;

/// Magic number for single-file LSM (ASCII "MOTE")
const MAGIC: u32 = 0x4D4F5445;

/// Current format version
const VERSION: u32 = 1;

/// Maximum number of levels
const MAX_LEVELS: usize = 7;

/// Superblock (stored at Page 0)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Superblock {
    /// Magic number
    pub magic: u32,
    
    /// Format version
    pub version: u32,
    
    /// Page size
    pub page_size: u32,
    
    /// Total number of pages
    pub total_pages: u64,
    
    /// Level metadata: (start_page, num_pages, num_entries)
    pub levels: [LevelMeta; MAX_LEVELS],
    
    /// Free list head (page number of first free page)
    pub free_list_head: u64,
    
    /// Total free pages
    pub free_pages_count: u64,
    
    /// Next entry ID
    pub next_entry_id: u64,
    
    /// Write sequence number (for ordering)
    pub write_seq: u64,
    
    /// CRC32C checksum of superblock data (excluding this field)
    pub checksum: u32,
}

/// Level metadata
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct LevelMeta {
    /// Starting page number
    pub start_page: u64,
    
    /// Number of pages in this level
    pub num_pages: u64,
    
    /// Number of entries in this level
    pub num_entries: u64,
    
    /// Min key in this level
    pub min_key: u64,
    
    /// Max key in this level
    pub max_key: u64,
}

impl Default for LevelMeta {
    fn default() -> Self {
        Self {
            start_page: 0,
            num_pages: 0,
            num_entries: 0,
            min_key: u64::MAX,
            max_key: 0,
        }
    }
}

impl Default for Superblock {
    fn default() -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            page_size: PAGE_SIZE as u32,
            total_pages: 1, // Start with superblock only
            levels: [LevelMeta::default(); MAX_LEVELS],
            free_list_head: 0,
            free_pages_count: 0,
            next_entry_id: 0,
            write_seq: 0,
            checksum: 0, // Will be computed before writing
        }
    }
}

/// Free list node (stored in a free page)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FreeListNode {
    /// Next free page
    next: u64,
    
    /// Number of consecutive free pages starting from this page
    count: u64,
}

/// Entry in the LSM file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    /// Entry ID (unique, monotonically increasing)
    pub id: u64,
    
    /// Key (for sorting)
    pub key: Vec<u8>,
    
    /// Value
    pub value: Vec<u8>,
    
    /// Write sequence (for MVCC)
    pub seq: u64,
    
    /// Deletion marker
    pub deleted: bool,
}

/// Single-File LSM Engine
pub struct SingleFileLSM {
    /// File path
    path: PathBuf,
    
    /// File handle
    file: Arc<Mutex<File>>,
    
    /// Superblock (cached in memory)
    superblock: Arc<RwLock<Superblock>>,
    
    /// Dirty flag (needs fsync)
    dirty: Arc<Mutex<bool>>,
    
    /// File reference manager (for mmap safety)
    file_manager: Option<Arc<FileRefManager>>,
    
    /// File handle (RAII protection)
    _file_handle: Option<FileHandle>,
}

impl SingleFileLSM {
    /// Create a new single-file LSM
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::create_with_manager(path, None)
    }
    
    /// Create with file manager for mmap safety
    pub fn create_with_manager<P: AsRef<Path>>(path: P, file_manager: Option<Arc<FileRefManager>>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        
        // Acquire file handle if manager provided
        let file_handle = if let Some(ref manager) = file_manager {
            Some(manager.acquire(&path)?)
        } else {
            None
        };
        
        // Create file
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        
        // Initialize superblock
        let superblock = Superblock::default();
        
        // Write superblock to page 0
        Self::write_superblock(&mut file, &superblock)?;
        
        // Sync to disk
        file.sync_all()?;
        
        Ok(Self {
            path,
            file: Arc::new(Mutex::new(file)),
            superblock: Arc::new(RwLock::new(superblock)),
            dirty: Arc::new(Mutex::new(false)),
            file_manager,
            _file_handle: file_handle,
        })
    }
    
    /// Open an existing single-file LSM
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_manager(path, None)
    }
    
    /// Open with file manager
    pub fn open_with_manager<P: AsRef<Path>>(path: P, file_manager: Option<Arc<FileRefManager>>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        
        // Acquire file handle if manager provided
        let file_handle = if let Some(ref manager) = file_manager {
            Some(manager.acquire(&path)?)
        } else {
            None
        };
        
        // Open file
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;
        
        // Read superblock
        let superblock = Self::read_superblock(&mut file)?;
        
        // Verify magic number
        if superblock.magic != MAGIC {
            return Err(StorageError::InvalidData(
                format!("Invalid magic number: 0x{:X}", superblock.magic)
            ));
        }
        
        // Verify version
        if superblock.version != VERSION {
            return Err(StorageError::InvalidData(
                format!("Unsupported version: {}", superblock.version)
            ));
        }
        
        Ok(Self {
            path,
            file: Arc::new(Mutex::new(file)),
            superblock: Arc::new(RwLock::new(superblock)),
            dirty: Arc::new(Mutex::new(false)),
            file_manager,
            _file_handle: file_handle,
        })
    }
    
    /// Write superblock to page 0
    fn write_superblock(file: &mut File, superblock: &Superblock) -> Result<()> {
        // Create a temporary superblock with zero checksum for computation
        let mut sb_for_checksum = superblock.clone();
        sb_for_checksum.checksum = 0;
        
        // Serialize without checksum
        let data_without_checksum = bincode::serialize(&sb_for_checksum)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        
        // Compute checksum
        let checksum = Checksum::compute(ChecksumType::CRC32C, &data_without_checksum);
        
        // Create final superblock with checksum
        let mut final_superblock = superblock.clone();
        final_superblock.checksum = checksum;
        
        // Serialize final superblock
        let data = bincode::serialize(&final_superblock)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        
        if data.len() > PAGE_SIZE {
            return Err(StorageError::InvalidData(
                format!("Superblock too large: {} bytes", data.len())
            ));
        }
        
        // Prepare page (pad to PAGE_SIZE)
        let mut page = vec![0u8; PAGE_SIZE];
        page[..data.len()].copy_from_slice(&data);
        
        // Write to page 0
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&page)?;
        
        Ok(())
    }
    
    /// Read superblock from page 0
    fn read_superblock(file: &mut File) -> Result<Superblock> {
        // Read page 0
        let mut page = vec![0u8; PAGE_SIZE];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut page)?;
        
        // Deserialize
        let superblock: Superblock = bincode::deserialize(&page)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        
        // Verify checksum
        let stored_checksum = superblock.checksum;
        let mut sb_for_verification = superblock.clone();
        sb_for_verification.checksum = 0;
        
        let data_without_checksum = bincode::serialize(&sb_for_verification)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        
        Checksum::verify(ChecksumType::CRC32C, &data_without_checksum, stored_checksum)
            .map_err(|e| StorageError::InvalidData(format!("Superblock checksum verification failed: {}", e)))?;
        
        Ok(superblock)
    }
    
    /// Allocate pages (prefer from free list)
    fn allocate_pages(&mut self, num_pages: u64) -> Result<Vec<u64>> {
        let mut superblock = self.superblock.write()
            .map_err(|_| StorageError::Lock("Superblock lock poisoned".into()))?;
        
        let mut allocated = Vec::new();
        let mut remaining = num_pages;
        
        // Try to allocate from free list first
        let mut current_free = superblock.free_list_head;
        let mut prev_free = 0u64;
        
        while remaining > 0 && current_free != 0 {
            // Read free list node
            let node = self.read_free_list_node(current_free)?;
            
            if node.count >= remaining {
                // This node has enough pages
                for i in 0..remaining {
                    allocated.push(current_free + i);
                }
                
                // Update free list
                if node.count == remaining {
                    // Remove entire node
                    if prev_free == 0 {
                        superblock.free_list_head = node.next;
                    } else {
                        let mut prev_node = self.read_free_list_node(prev_free)?;
                        prev_node.next = node.next;
                        self.write_free_list_node(prev_free, &prev_node)?;
                    }
                } else {
                    // Partial allocation, update node
                    let new_node = FreeListNode {
                        next: node.next,
                        count: node.count - remaining,
                    };
                    self.write_free_list_node(current_free + remaining, &new_node)?;
                    
                    if prev_free == 0 {
                        superblock.free_list_head = current_free + remaining;
                    } else {
                        let mut prev_node = self.read_free_list_node(prev_free)?;
                        prev_node.next = current_free + remaining;
                        self.write_free_list_node(prev_free, &prev_node)?;
                    }
                }
                
                superblock.free_pages_count -= remaining;
                remaining = 0;
                break;
            } else {
                // Use all pages from this node
                for i in 0..node.count {
                    allocated.push(current_free + i);
                }
                remaining -= node.count;
                superblock.free_pages_count -= node.count;
                
                prev_free = current_free;
                current_free = node.next;
            }
        }
        
        // Allocate new pages if needed
        while remaining > 0 {
            allocated.push(superblock.total_pages);
            superblock.total_pages += 1;
            remaining -= 1;
        }
        
        // Mark dirty
        *self.dirty.lock()
            .map_err(|_| StorageError::Lock("Dirty flag lock poisoned".into()))? = true;
        
        Ok(allocated)
    }
    
    /// Free pages (add to free list)
    fn free_pages(&mut self, pages: &[u64]) -> Result<()> {
        if pages.is_empty() {
            return Ok(());
        }
        
        let mut superblock = self.superblock.write()
            .map_err(|_| StorageError::Lock("Superblock lock poisoned".into()))?;
        
        // Sort pages
        let mut sorted_pages = pages.to_vec();
        sorted_pages.sort_unstable();
        
        // Coalesce consecutive pages
        let mut start = sorted_pages[0];
        let mut count = 1u64;
        
        for i in 1..sorted_pages.len() {
            if sorted_pages[i] == sorted_pages[i - 1] + 1 {
                count += 1;
            } else {
                // Add current range to free list
                let node = FreeListNode {
                    next: superblock.free_list_head,
                    count,
                };
                self.write_free_list_node(start, &node)?;
                superblock.free_list_head = start;
                superblock.free_pages_count += count;
                
                // Start new range
                start = sorted_pages[i];
                count = 1;
            }
        }
        
        // Add last range
        let node = FreeListNode {
            next: superblock.free_list_head,
            count,
        };
        self.write_free_list_node(start, &node)?;
        superblock.free_list_head = start;
        superblock.free_pages_count += count;
        
        // Mark dirty
        *self.dirty.lock()
            .map_err(|_| StorageError::Lock("Dirty flag lock poisoned".into()))? = true;
        
        Ok(())
    }
    
    /// Read free list node from a page
    fn read_free_list_node(&self, page: u64) -> Result<FreeListNode> {
        let mut file = self.file.lock()
            .map_err(|_| StorageError::Lock("File lock poisoned".into()))?;
        let mut buf = vec![0u8; PAGE_SIZE];
        
        file.seek(SeekFrom::Start(page * PAGE_SIZE as u64))?;
        file.read_exact(&mut buf)?;
        
        let node: FreeListNode = bincode::deserialize(&buf)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        
        Ok(node)
    }
    
    /// Write free list node to a page
    fn write_free_list_node(&self, page: u64, node: &FreeListNode) -> Result<()> {
        let data = bincode::serialize(node)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        
        let mut buf = vec![0u8; PAGE_SIZE];
        buf[..data.len()].copy_from_slice(&data);
        
        let mut file = self.file.lock()
            .map_err(|_| StorageError::Lock("File lock poisoned".into()))?;
        file.seek(SeekFrom::Start(page * PAGE_SIZE as u64))?;
        file.write_all(&buf)?;
        
        Ok(())
    }
    
    /// Sync superblock to disk
    pub fn sync_superblock(&self) -> Result<()> {
        let superblock = self.superblock.read()
            .map_err(|_| StorageError::Lock("Superblock lock poisoned".into()))?;
        
        let mut file = self.file.lock()
            .map_err(|_| StorageError::Lock("File lock poisoned".into()))?;
        Self::write_superblock(&mut *file, &superblock)?;
        file.sync_data()?;
        
        *self.dirty.lock()
            .map_err(|_| StorageError::Lock("Dirty flag lock poisoned".into()))? = false;
        
        Ok(())
    }
    
    /// Get superblock (read-only)
    pub fn get_superblock(&self) -> Result<Superblock> {
        let superblock = self.superblock.read()
            .map_err(|_| StorageError::Lock("Superblock lock poisoned".into()))?;
        Ok(superblock.clone())
    }
    
    /// Get file path
    pub fn path(&self) -> &Path {
        &self.path
    }
    
    /// Write entries to Level 0 (unsorted, direct append)
    pub fn write_level0(&mut self, entries: Vec<Entry>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        
        // Calculate pages needed
        let entries_size: usize = entries.iter()
            .map(|e| {
                bincode::serialized_size(e)
                    .unwrap_or(0) as usize
            })
            .sum();
        
        let pages_needed = (entries_size + PAGE_SIZE - 1) / PAGE_SIZE;
        
        // Allocate pages
        let pages = self.allocate_pages(pages_needed as u64)?;
        
        // Serialize entries
        let mut buffer = Vec::new();
        for entry in &entries {
            let entry_data = bincode::serialize(entry)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            
            // Write length prefix (u32)
            buffer.extend_from_slice(&(entry_data.len() as u32).to_le_bytes());
            buffer.extend_from_slice(&entry_data);
        }
        
        // Pad to page boundary
        let padding = (PAGE_SIZE - (buffer.len() % PAGE_SIZE)) % PAGE_SIZE;
        buffer.resize(buffer.len() + padding, 0);
        
        // Write pages
        let mut file = self.file.lock()
            .map_err(|_| StorageError::Lock("File lock poisoned".into()))?;
        for (i, page) in pages.iter().enumerate() {
            let offset = *page * PAGE_SIZE as u64;
            let start = i * PAGE_SIZE;
            let end = std::cmp::min(start + PAGE_SIZE, buffer.len());
            
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&buffer[start..end])?;
        }
        
        // Sync data
        file.sync_data()?;
        drop(file);
        
        // Update superblock
        let mut superblock = self.superblock.write()
            .map_err(|_| StorageError::Lock("Superblock lock poisoned".into()))?;
        
        let level0 = &mut superblock.levels[0];
        if level0.num_pages == 0 {
            // First write to Level 0
            level0.start_page = pages[0];
        }
        level0.num_pages += pages.len() as u64;
        level0.num_entries += entries.len() as u64;
        
        // Update min/max keys
        if !entries.is_empty() {
            // Use first 8 bytes of key as sorting key (or pad if shorter)
            for entry in &entries {
                let mut key_bytes = [0u8; 8];
                let copy_len = std::cmp::min(entry.key.len(), 8);
                key_bytes[..copy_len].copy_from_slice(&entry.key[..copy_len]);
                let key = u64::from_le_bytes(key_bytes);
                
                if level0.num_entries == 0 {
                    level0.min_key = key;
                    level0.max_key = key;
                } else {
                    level0.min_key = level0.min_key.min(key);
                    level0.max_key = level0.max_key.max(key);
                }
            }
        }
        
        superblock.write_seq += 1;
        drop(superblock);
        
        // Sync superblock
        self.sync_superblock()?;
        
        Ok(())
    }
    
    /// Read entries from a level
    pub fn read_level(&self, level: usize) -> Result<Vec<Entry>> {
        if level >= MAX_LEVELS {
            return Err(StorageError::InvalidData(
                format!("Invalid level: {}", level)
            ));
        }
        
        let superblock = self.superblock.read()
            .map_err(|_| StorageError::Lock("Superblock lock poisoned".into()))?;
        
        let level_meta = &superblock.levels[level];
        if level_meta.num_pages == 0 {
            return Ok(Vec::new());
        }
        
        // Read pages
        let mut file = self.file.lock()
            .map_err(|_| StorageError::Lock("File lock poisoned".into()))?;
        let mut buffer = vec![0u8; level_meta.num_pages as usize * PAGE_SIZE];
        
        file.seek(SeekFrom::Start(level_meta.start_page * PAGE_SIZE as u64))?;
        file.read_exact(&mut buffer)?;
        drop(file);
        
        // Deserialize entries
        let mut entries = Vec::new();
        let mut offset = 0;
        
        while offset + 4 <= buffer.len() {
            // Read length prefix
            let len = u32::from_le_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ]) as usize;
            
            offset += 4;
            
            if len == 0 || offset + len > buffer.len() {
                break; // End of data or invalid
            }
            
            // Deserialize entry
            if let Ok(entry) = bincode::deserialize(&buffer[offset..offset + len]) {
                entries.push(entry);
            }
            
            offset += len;
        }
        
        Ok(entries)
    }
    
    /// Get entry by key (scan all levels, newest first)
    pub fn get(&self, key: &[u8]) -> Result<Option<Entry>> {
        // Scan from Level 0 to Level 6 (newer to older)
        for level in 0..MAX_LEVELS {
            let entries = self.read_level(level)?;
            
            // Scan in reverse order (newest entries last in Level 0)
            for entry in entries.iter().rev() {
                if entry.key == key {
                    if entry.deleted {
                        return Ok(None); // Tombstone - key is deleted
                    }
                    return Ok(Some(entry.clone()));
                }
            }
        }
        
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_create_and_open() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.sst");
        
        // Create
        {
            let lsm = SingleFileLSM::create(&path).unwrap();
            let superblock = lsm.get_superblock().unwrap();
            
            assert_eq!(superblock.magic, MAGIC);
            assert_eq!(superblock.version, VERSION);
            assert_eq!(superblock.total_pages, 1);
        }
        
        // Open
        {
            let lsm = SingleFileLSM::open(&path).unwrap();
            let superblock = lsm.get_superblock().unwrap();
            
            assert_eq!(superblock.magic, MAGIC);
            assert_eq!(superblock.version, VERSION);
        }
    }
    
    #[test]
    fn test_allocate_free_pages() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.sst");
        let mut lsm = SingleFileLSM::create(&path).unwrap();
        
        // Allocate 10 pages
        let pages = lsm.allocate_pages(10).unwrap();
        assert_eq!(pages.len(), 10);
        assert_eq!(pages[0], 1); // Page 0 is superblock
        
        let sb1 = lsm.get_superblock().unwrap();
        assert_eq!(sb1.total_pages, 11); // 1 superblock + 10 allocated
        
        // Free 5 pages
        lsm.free_pages(&pages[0..5]).unwrap();
        
        let sb2 = lsm.get_superblock().unwrap();
        assert_eq!(sb2.free_pages_count, 5);
        
        // Allocate 3 pages (should come from free list)
        let pages2 = lsm.allocate_pages(3).unwrap();
        assert_eq!(pages2.len(), 3);
        assert!(pages2.iter().all(|&p| p >= 1 && p <= 5));
        
        let sb3 = lsm.get_superblock().unwrap();
        assert_eq!(sb3.free_pages_count, 2);
        assert_eq!(sb3.total_pages, 11); // No new pages allocated
    }
    
    #[test]
    fn test_sync_superblock() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.sst");
        
        {
            let mut lsm = SingleFileLSM::create(&path).unwrap();
            lsm.allocate_pages(5).unwrap();
            lsm.sync_superblock().unwrap();
        }
        
        // Reopen and verify
        {
            let lsm = SingleFileLSM::open(&path).unwrap();
            let sb = lsm.get_superblock().unwrap();
            assert_eq!(sb.total_pages, 6); // 1 + 5
        }
    }
    
    #[test]
    fn test_write_read_level0() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.sst");
        let mut lsm = SingleFileLSM::create(&path).unwrap();
        
        // Create test entries
        let entries = vec![
            Entry {
                id: 1,
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
                seq: 1,
                deleted: false,
            },
            Entry {
                id: 2,
                key: b"key2".to_vec(),
                value: b"value2".to_vec(),
                seq: 2,
                deleted: false,
            },
            Entry {
                id: 3,
                key: b"key3".to_vec(),
                value: b"value3".to_vec(),
                seq: 3,
                deleted: false,
            },
        ];
        
        // Write to Level 0
        lsm.write_level0(entries.clone()).unwrap();
        
        // Read back
        let read_entries = lsm.read_level(0).unwrap();
        assert_eq!(read_entries.len(), 3);
        
        assert_eq!(read_entries[0].key, b"key1");
        assert_eq!(read_entries[0].value, b"value1");
        assert_eq!(read_entries[1].key, b"key2");
        assert_eq!(read_entries[2].key, b"key3");
    }
    
    #[test]
    fn test_get_entry() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.sst");
        let mut lsm = SingleFileLSM::create(&path).unwrap();
        
        // Write entries
        let entries = vec![
            Entry {
                id: 1,
                key: b"alice".to_vec(),
                value: b"data_alice".to_vec(),
                seq: 1,
                deleted: false,
            },
            Entry {
                id: 2,
                key: b"bob".to_vec(),
                value: b"data_bob".to_vec(),
                seq: 2,
                deleted: false,
            },
        ];
        
        lsm.write_level0(entries).unwrap();
        
        // Get existing key
        let entry = lsm.get(b"alice").unwrap();
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().value, b"data_alice");
        
        // Get non-existing key
        let entry = lsm.get(b"charlie").unwrap();
        assert!(entry.is_none());
    }
    
    #[test]
    fn test_tombstone() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.sst");
        let mut lsm = SingleFileLSM::create(&path).unwrap();
        
        // Write entry and tombstone in same batch
        let entries = vec![
            Entry {
                id: 1,
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
                seq: 1,
                deleted: false,
            },
            Entry {
                id: 2,
                key: b"key1".to_vec(),
                value: Vec::new(),
                seq: 2,
                deleted: true,  // Tombstone
            },
        ];
        lsm.write_level0(entries).unwrap();
        
        // Get should return None (deleted)
        let entry = lsm.get(b"key1").unwrap();
        assert!(entry.is_none());
    }
}
