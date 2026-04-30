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
use std::sync::Mutex;
use super::BlobRef;

const BLOB_MAGIC: u32 = 0x424C4F42; // "BLOB"
const BLOB_VERSION: u32 = 1;

/// Internal mutable state for BlobStore, protected by a single Mutex
struct BlobState {
    /// Current active blob file
    current_file: BlobFile,
    /// Current file ID
    current_file_id: u32,
}

/// Blob store manages large value storage
pub struct BlobStore {
    /// Storage directory
    dir: PathBuf,

    /// Mutable state (file + id) behind a single Mutex
    state: Mutex<BlobState>,

    /// Configuration
    max_file_size: usize,
}

/// Single blob file (immutable after close)
struct BlobFile {
    file_id: u32,
    writer: BufWriter<File>,  // 🚀 使用 BufWriter 减少系统调用
    offset: u64,
}

impl BlobStore {
    /// Create new blob store
    pub fn new<P: AsRef<Path>>(dir: P, max_file_size: usize) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        // Crash recovery: validate and truncate the last blob file to the
        // last valid entry. This handles partial writes from crashes.
        Self::recover_last_blob_file(&dir)?;

        // Find next file ID
        let file_id = Self::find_next_file_id(&dir)?;

        // Create first blob file
        let blob_file = BlobFile::create(&dir, file_id)?;

        Ok(Self {
            dir,
            state: Mutex::new(BlobState {
                current_file: blob_file,
                current_file_id: file_id,
            }),
            max_file_size,
        })
    }

    /// Write large value to blob file
    pub fn put(&self, data: &[u8]) -> Result<BlobRef> {
        let mut state = self.state.lock()
            .map_err(|_| StorageError::Lock("BlobStore state lock poisoned".into()))?;

        // Check if need to rotate file
        if state.current_file.offset + data.len() as u64 + 12 > self.max_file_size as u64 {
            self.rotate_file_locked(&mut state)?;
        }

        state.current_file.write_blob(data)
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

    /// Delete blob — marks blob file as eligible for GC.
    ///
    /// Actual space reclamation happens when `gc_blob_files()` is called
    /// (typically during compaction). Files with no live references are removed.
    pub fn delete(&self, _blob_ref: &BlobRef) -> Result<()> {
        // GC is handled externally via gc_blob_files()
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

    /// Rotate to a new blob file. Caller must hold the state lock.
    fn rotate_file_locked(&self, state: &mut BlobState) -> Result<()> {
        // Flush current file
        state.current_file.flush()?;

        // Allocate new file ID
        let new_id = state.current_file_id + 1;
        let new_file = BlobFile::create(&self.dir, new_id)?;

        state.current_file = new_file;
        state.current_file_id = new_id;

        Ok(())
    }

    /// Crash recovery: validate the last blob file and truncate to the
    /// last valid entry boundary. Handles partial writes from crashes.
    fn recover_last_blob_file(dir: &Path) -> Result<()> {
        let max_id = Self::find_next_file_id(dir)?;
        if max_id == 0 {
            return Ok(()); // No blob files exist
        }

        // The file with max_id was just created by find_next_file_id returning max+1,
        // so the actual last file is max_id-1. But find_next_file_id returns max_id+1
        // when it scans. Let me re-check: it returns max_existing_id + 1.
        // So the last existing file is max_id - 1.
        let last_file_id = max_id.saturating_sub(1);
        if last_file_id == 0 {
            return Ok(());
        }

        let path = dir.join(format!("{:08}.blob", last_file_id));
        if !path.exists() {
            return Ok(());
        }

        // Scan the file entry-by-entry, checking CRC at each entry
        let mut file = match File::open(&path) {
            Ok(f) => f,
            Err(_) => return Ok(()),
        };

        // Skip header
        let mut header = [0u8; 8];
        if file.read_exact(&mut header).is_err() {
            // Can't even read header — delete the file
            let _ = std::fs::remove_file(&path);
            return Ok(());
        }

        let mut valid_offset: u64 = 8; // header size

        loop {
            // Read size
            let mut size_buf = [0u8; 4];
            match file.read_exact(&mut size_buf) {
                Ok(_) => {}
                Err(_) => break, // EOF or partial read — stop here
            }
            let size = u32::from_le_bytes(size_buf);

            // Read data
            let mut data = vec![0u8; size as usize];
            match file.read_exact(&mut data) {
                Ok(_) => {}
                Err(_) => break, // Partial data — stop
            }

            // Read CRC
            let mut crc_buf = [0u8; 4];
            match file.read_exact(&mut crc_buf) {
                Ok(_) => {}
                Err(_) => break, // Partial CRC — stop
            }
            let stored_crc = u32::from_le_bytes(crc_buf);

            // Verify CRC
            let computed_crc = crc32fast::hash(&data);
            if stored_crc != computed_crc {
                // CRC mismatch — this entry is corrupt. Stop here.
                debug_log!("[BlobStore] CRC mismatch in blob file {}, truncating to offset {}", last_file_id, valid_offset);
                break;
            }

            // Entry is valid, advance offset
            valid_offset += 4 + size as u64 + 4;
        }

        // Truncate file to valid_offset if it was longer
        let file_size = file.metadata().map(|m| m.len()).unwrap_or(0);
        if valid_offset < file_size {
            drop(file);
            // Truncate to last valid entry
            let file = OpenOptions::new().write(true).open(&path)?;
            file.set_len(valid_offset)?;
            debug_log!("[BlobStore] Recovered blob file {}: truncated from {} to {} bytes",
                     last_file_id, file_size, valid_offset);
        }

        Ok(())
    }
}

impl BlobFile {
    fn create(dir: &Path, file_id: u32) -> Result<Self> {
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
            writer: BufWriter::with_capacity(64 * 1024, file),
            offset,
        })
    }

    /// Flush buffered data to disk
    fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_mut().sync_data()?;
        Ok(())
    }
    
    fn write_blob(&mut self, data: &[u8]) -> Result<BlobRef> {
        // Prevent u32 overflow for blob size
        if data.len() > u32::MAX as usize {
            return Err(crate::StorageError::InvalidData(
                format!("Blob too large: {} bytes (max {})", data.len(), u32::MAX)
            ));
        }
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
