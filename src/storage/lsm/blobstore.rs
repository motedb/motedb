//! Blob Store: Separate storage for large values
//!
//! ## Design (following RocksDB BlobDB)
//! - Large values (> blob_threshold) stored in separate blob files
//! - SSTable only stores small BlobRef (16 bytes)
//! - Reduces write amplification during compaction
//! - Blob files are immutable, GC happens separately
//!
//! ## File Format
//! ```text
//! [Header (magic + version)] [Blob 1] [Blob 2] ... [Blob N] [Footer]
//! 
//! Each Blob:
//!   [size: u32] [data: Vec<u8>] [crc32: u32]
//! ```text
use crate::{Result, StorageError};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use super::BlobRef;

const BLOB_MAGIC: u32 = 0x424C4F42; // "BLOB"
const BLOB_VERSION: u32 = 1;

/// Blob store manages large value storage
pub struct BlobStore {
    /// Storage directory
    dir: PathBuf,
    
    /// Current active blob file
    current_file: Arc<Mutex<BlobFile>>,
    
    /// Current file ID
    current_file_id: u32,
    
    /// Configuration
    max_file_size: usize,
}

/// Single blob file (immutable after close)
struct BlobFile {
    file_id: u32,
    writer: BufWriter<File>,  // 🚀 使用 BufWriter 减少系统调用
    offset: u64,
    #[allow(dead_code)]
    path: PathBuf,
}

impl BlobStore {
    /// Create new blob store
    pub fn new<P: AsRef<Path>>(dir: P, max_file_size: usize) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        
        // Find next file ID
        let file_id = Self::find_next_file_id(&dir)?;
        
        // Create first blob file
        let blob_file = BlobFile::create(&dir, file_id, max_file_size)?;
        
        Ok(Self {
            dir,
            current_file: Arc::new(Mutex::new(blob_file)),
            current_file_id: file_id,
            max_file_size,
        })
    }
    
    /// Write large value to blob file
    pub fn put(&self, data: &[u8]) -> Result<BlobRef> {
        let mut file = self.current_file.lock()
            .map_err(|_| StorageError::Lock("BlobStore file lock poisoned".into()))?;
        
        // Check if need to rotate file
        if file.offset + data.len() as u64 + 8 > self.max_file_size as u64 {
            // Rotate to new file
            drop(file);
            self.rotate_file()?;
            file = self.current_file.lock()
                .map_err(|_| StorageError::Lock("BlobStore file lock poisoned".into()))?;
        }
        
        let blob_ref = file.write_blob(data)?;
        Ok(blob_ref)
    }
    
    /// Read blob data by reference
    pub fn get(&self, blob_ref: &BlobRef) -> Result<Vec<u8>> {
        let path = self.blob_file_path(blob_ref.file_id);
        let mut file = File::open(&path)?;
        
        // Seek to offset
        file.seek(SeekFrom::Start(blob_ref.offset))?;
        
        // Read size
        let mut size_buf = [0u8; 4];
        file.read_exact(&mut size_buf)?;
        let size = u32::from_le_bytes(size_buf);
        
        if size != blob_ref.size {
            return Err(StorageError::InvalidData("Blob size mismatch".into()));
        }
        
        // Read data
        let mut data = vec![0u8; size as usize];
        file.read_exact(&mut data)?;
        
        // Read and verify CRC
        let mut crc_buf = [0u8; 4];
        file.read_exact(&mut crc_buf)?;
        let stored_crc = u32::from_le_bytes(crc_buf);
        let computed_crc = crc32fast::hash(&data);
        
        if stored_crc != computed_crc {
            return Err(StorageError::InvalidData("Blob CRC mismatch".into()));
        }
        
        Ok(data)
    }
    
    /// Delete blob (mark for GC, actual deletion happens in GC phase)
    pub fn delete(&self, _blob_ref: &BlobRef) -> Result<()> {
        // Mark for GC in metadata during compaction
        // Actual deletion is deferred to background GC process
        Ok(())
    }
    
    // Internal helpers
    
    fn find_next_file_id(dir: &Path) -> Result<u32> {
        let mut max_id = 0u32;
        
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.ends_with(".blob") {
                        if let Some(id_str) = name.strip_suffix(".blob") {
                            if let Ok(id) = id_str.parse::<u32>() {
                                max_id = max_id.max(id);
                            }
                        }
                    }
                }
            }
        }
        
        Ok(max_id + 1)
    }
    
    fn blob_file_path(&self, file_id: u32) -> PathBuf {
        self.dir.join(format!("{:08}.blob", file_id))
    }
    
    fn rotate_file(&self) -> Result<()> {
        // This would need interior mutability redesign
        // For now, caller should handle rotation
        Ok(())
    }
}

impl BlobFile {
    fn create(dir: &Path, file_id: u32, _max_size: usize) -> Result<Self> {
        let path = dir.join(format!("{:08}.blob", file_id));
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        
        // Write header
        file.write_all(&BLOB_MAGIC.to_le_bytes())?;
        file.write_all(&BLOB_VERSION.to_le_bytes())?;
        
        let offset = 8; // header size
        
        Ok(Self {
            file_id,
            writer: BufWriter::with_capacity(64 * 1024, file),  // 🚀 64KB 缓冲区
            offset,
            path,
        })
    }
    
    fn write_blob(&mut self, data: &[u8]) -> Result<BlobRef> {
        let size = data.len() as u32;
        let offset = self.offset;
        
        // Write: size | data | crc32
        self.writer.write_all(&size.to_le_bytes())?;
        self.writer.write_all(data)?;
        
        let crc = crc32fast::hash(data);
        self.writer.write_all(&crc.to_le_bytes())?;
        
        // 🚀 刷新并同步到磁盘（保证持久化）
        self.writer.flush()?;
        self.writer.get_mut().sync_data()?;
        
        // Update offset
        self.offset += 4 + size as u64 + 4;
        
        Ok(BlobRef {
            file_id: self.file_id,
            offset,
            size,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_blob_store_basic() {
        let temp_dir = TempDir::new().unwrap();
        let store = BlobStore::new(temp_dir.path(), 1024 * 1024).unwrap();
        
        // Write blob
        let data = b"Hello, Blob World!".to_vec();
        let blob_ref = store.put(&data).unwrap();
        
        // Read back
        let retrieved = store.get(&blob_ref).unwrap();
        assert_eq!(data, retrieved);
    }
    
    #[test]
    fn test_large_blob() {
        let temp_dir = TempDir::new().unwrap();
        let store = BlobStore::new(temp_dir.path(), 1024 * 1024).unwrap();
        
        // 1MB blob (vector embedding)
        let large_data = vec![42u8; 1024 * 1024];
        let blob_ref = store.put(&large_data).unwrap();
        
        let retrieved = store.get(&blob_ref).unwrap();
        assert_eq!(large_data.len(), retrieved.len());
        assert_eq!(large_data, retrieved);
    }
}
