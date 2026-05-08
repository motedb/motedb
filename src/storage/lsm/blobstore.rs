//! Blob Store: Separate storage for large values
//!
//! ## Design (following RocksDB BlobDB)
//! - Large values (> blob_threshold) stored in separate blob files
//! - SSTable only stores small BlobRef (16 bytes)
//! - Reduces write amplification during compaction
//! - Blob files are immutable, GC happens separately
//!
//! ## File Format (v2 — with compression)
//! ```text
//! [Header (magic + version)] [Blob 1] [Blob 2] ... [Blob N] [Footer]
//!
//! Each Blob:
//!   [original_size: u32] [compress_flag: u8] [data_len: u32] [data] [crc32: u32]
//!   - compress_flag = 0: data is raw, data_len = original_size
//!   - compress_flag = 1: data is Zstd compressed, data_len = compressed size
//!   - crc32 covers [compress_flag][data_len][data]
//! ```

use crate::{Result, StorageError};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use super::BlobRef;

const BLOB_MAGIC: u32 = 0x424C4F42; // "BLOB"
const BLOB_VERSION_V2: u32 = 2;
const BLOB_COMPRESS_NONE: u8 = 0;
const BLOB_COMPRESS_ZSTD: u8 = 1;
/// Minimum blob size to consider compression (small blobs aren't worth the overhead)
const BLOB_COMPRESS_THRESHOLD: usize = 256;

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

    /// Read blob data by reference (supports V1 and V2 formats)
    pub fn get(&self, blob_ref: &BlobRef) -> Result<Vec<u8>> {
        let path = self.blob_file_path(blob_ref.file_id);
        let mut file = File::open(&path)?;

        // Read version from header to determine format
        file.seek(SeekFrom::Start(4))?;
        let mut ver_buf = [0u8; 4];
        file.read_exact(&mut ver_buf)?;
        let version = u32::from_le_bytes(ver_buf);

        // Seek to blob entry offset
        file.seek(SeekFrom::Start(blob_ref.offset))?;

        if version >= 2 {
            // V2 format: [original_size: u32][compress_flag: u8][data_len: u32][data][crc32: u32]
            let mut size_buf = [0u8; 4];
            file.read_exact(&mut size_buf)?;
            let original_size = u32::from_le_bytes(size_buf);
            if original_size != blob_ref.size {
                return Err(StorageError::InvalidData("Blob size mismatch".into()));
            }

            let mut flag_buf = [0u8; 1];
            file.read_exact(&mut flag_buf)?;
            let compress_flag = flag_buf[0];

            let mut dlen_buf = [0u8; 4];
            file.read_exact(&mut dlen_buf)?;
            let data_len = u32::from_le_bytes(dlen_buf) as usize;

            let mut stored_data = vec![0u8; data_len];
            file.read_exact(&mut stored_data)?;

            // Verify CRC
            let mut crc_buf = [0u8; 4];
            file.read_exact(&mut crc_buf)?;
            let stored_crc = u32::from_le_bytes(crc_buf);
            let mut crc_input = Vec::with_capacity(1 + 4 + data_len);
            crc_input.push(compress_flag);
            crc_input.extend_from_slice(&(data_len as u32).to_le_bytes());
            crc_input.extend_from_slice(&stored_data);
            let computed_crc = crc32fast::hash(&crc_input);
            if stored_crc != computed_crc {
                return Err(StorageError::InvalidData("Blob CRC mismatch".into()));
            }

            // Decompress if needed
            if compress_flag == BLOB_COMPRESS_ZSTD {
                let decompressed = zstd::decode_all(&stored_data[..])
                    .map_err(|e| StorageError::InvalidData(format!("Blob decompress failed: {}", e)))?;
                if decompressed.len() != original_size as usize {
                    return Err(StorageError::InvalidData("Blob decompressed size mismatch".into()));
                }
                Ok(decompressed)
            } else {
                Ok(stored_data)
            }
        } else {
            // V1 format (legacy): [size: u32][data][crc32: u32]
            let mut size_buf = [0u8; 4];
            file.read_exact(&mut size_buf)?;
            let size = u32::from_le_bytes(size_buf);
            if size != blob_ref.size {
                return Err(StorageError::InvalidData("Blob size mismatch".into()));
            }

            let mut data = vec![0u8; size as usize];
            file.read_exact(&mut data)?;

            let mut crc_buf = [0u8; 4];
            file.read_exact(&mut crc_buf)?;
            let stored_crc = u32::from_le_bytes(crc_buf);
            let computed_crc = crc32fast::hash(&data);
            if stored_crc != computed_crc {
                return Err(StorageError::InvalidData("Blob CRC mismatch".into()));
            }

            Ok(data)
        }
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

        let last_file_id = max_id.saturating_sub(1);
        if last_file_id == 0 {
            return Ok(());
        }

        let path = dir.join(format!("{:08}.blob", last_file_id));
        if !path.exists() {
            return Ok(());
        }

        let mut file = match File::open(&path) {
            Ok(f) => f,
            Err(_) => return Ok(()),
        };

        // Read header to determine version
        let mut header = [0u8; 8];
        if file.read_exact(&mut header).is_err() {
            let _ = std::fs::remove_file(&path);
            return Ok(());
        }
        let version = u32::from_le_bytes(header[4..8].try_into().unwrap_or([0,0,0,0]));

        let mut valid_offset: u64 = 8; // header size

        if version >= 2 {
            // V2 format: [original_size: u32][compress_flag: u8][data_len: u32][data][crc32]
            loop {
                let entry_start = valid_offset;

                // Read original_size
                let mut size_buf = [0u8; 4];
                match file.read_exact(&mut size_buf) {
                    Ok(_) => {}
                    Err(_) => break,
                }
                let _original_size = u32::from_le_bytes(size_buf);

                // Read compress_flag
                let mut flag_buf = [0u8; 1];
                match file.read_exact(&mut flag_buf) {
                    Ok(_) => {}
                    Err(_) => break,
                };

                // Read data_len
                let mut dlen_buf = [0u8; 4];
                match file.read_exact(&mut dlen_buf) {
                    Ok(_) => {}
                    Err(_) => break,
                }
                let data_len = u32::from_le_bytes(dlen_buf) as u64;

                // Read data
                let mut data = vec![0u8; data_len as usize];
                match file.read_exact(&mut data) {
                    Ok(_) => {}
                    Err(_) => break,
                }

                // Read CRC
                let mut crc_buf = [0u8; 4];
                match file.read_exact(&mut crc_buf) {
                    Ok(_) => {}
                    Err(_) => break,
                }
                let stored_crc = u32::from_le_bytes(crc_buf);

                // Verify CRC
                let mut crc_input = Vec::with_capacity(1 + 4 + data.len());
                crc_input.push(flag_buf[0]);
                crc_input.extend_from_slice(&(data_len as u32).to_le_bytes());
                crc_input.extend_from_slice(&data);
                let computed_crc = crc32fast::hash(&crc_input);
                if stored_crc != computed_crc {
                    debug_log!("[BlobStore] CRC mismatch in blob file {}, truncating to offset {}", last_file_id, entry_start);
                    break;
                }

                valid_offset = entry_start + 4 + 1 + 4 + data_len + 4;
            }
        } else {
            // V1 format (legacy): [size: u32][data][crc32]
            loop {
                let mut size_buf = [0u8; 4];
                match file.read_exact(&mut size_buf) {
                    Ok(_) => {}
                    Err(_) => break,
                }
                let size = u32::from_le_bytes(size_buf);

                let mut data = vec![0u8; size as usize];
                match file.read_exact(&mut data) {
                    Ok(_) => {}
                    Err(_) => break,
                }

                let mut crc_buf = [0u8; 4];
                match file.read_exact(&mut crc_buf) {
                    Ok(_) => {}
                    Err(_) => break,
                }
                let stored_crc = u32::from_le_bytes(crc_buf);

                let computed_crc = crc32fast::hash(&data);
                if stored_crc != computed_crc {
                    debug_log!("[BlobStore] CRC mismatch in blob file {}, truncating to offset {}", last_file_id, valid_offset);
                    break;
                }

                valid_offset += 4 + size as u64 + 4;
            }
        }

        // Truncate file to valid_offset if it was longer
        let file_size = file.metadata().map(|m| m.len()).unwrap_or(0);
        if valid_offset < file_size {
            drop(file);
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

        // Write header: V2 with compression support
        file.write_all(&BLOB_MAGIC.to_le_bytes())?;
        file.write_all(&BLOB_VERSION_V2.to_le_bytes())?;

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
        let original_size = data.len() as u32;
        let offset = self.offset;

        // Try compression for large blobs
        let (compress_flag, stored_data): (u8, Vec<u8>) = if data.len() >= BLOB_COMPRESS_THRESHOLD {
            if let Ok(compressed) = zstd::encode_all(data, 1) {
                if compressed.len() < data.len() * 9 / 10 {
                    (BLOB_COMPRESS_ZSTD, compressed)
                } else {
                    (BLOB_COMPRESS_NONE, data.to_vec())
                }
            } else {
                (BLOB_COMPRESS_NONE, data.to_vec())
            }
        } else {
            (BLOB_COMPRESS_NONE, data.to_vec())
        };

        let data_len = stored_data.len() as u32;

        // Write: original_size | compress_flag | data_len | data | crc32
        self.writer.write_all(&original_size.to_le_bytes())?;
        self.writer.write_all(&[compress_flag])?;
        self.writer.write_all(&data_len.to_le_bytes())?;
        self.writer.write_all(&stored_data)?;

        // CRC covers [compress_flag][data_len][data]
        let mut crc_input = Vec::with_capacity(1 + 4 + stored_data.len());
        crc_input.push(compress_flag);
        crc_input.extend_from_slice(&data_len.to_le_bytes());
        crc_input.extend_from_slice(&stored_data);
        let crc = crc32fast::hash(&crc_input);
        self.writer.write_all(&crc.to_le_bytes())?;

        // Flush and sync to disk (guarantee persistence)
        self.writer.flush()?;
        self.writer.get_mut().sync_data()?;

        // Update offset: 4 (orig_size) + 1 (flag) + 4 (data_len) + data + 4 (crc)
        self.offset += 4 + 1 + 4 + data_len as u64 + 4;

        Ok(BlobRef {
            file_id: self.file_id,
            offset,
            size: original_size,
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

        // Write blob (small — won't be compressed)
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

        // 1MB blob (highly compressible — Zstd should compress well)
        let large_data = vec![42u8; 1024 * 1024];
        let blob_ref = store.put(&large_data).unwrap();

        let retrieved = store.get(&blob_ref).unwrap();
        assert_eq!(large_data.len(), retrieved.len());
        assert_eq!(large_data, retrieved);
    }

    #[test]
    fn test_blob_compression_saves_space() {
        let temp_dir = TempDir::new().unwrap();
        let store = BlobStore::new(temp_dir.path(), 1024 * 1024).unwrap();

        // Write a large, highly compressible blob
        let data = vec![0xABu8; 100_000];
        let blob_ref = store.put(&data).unwrap();
        assert_eq!(blob_ref.size, 100_000); // original size preserved

        // Read back and verify
        let retrieved = store.get(&blob_ref).unwrap();
        assert_eq!(data, retrieved);

        // Verify on-disk savings: the blob file should be much smaller than raw data
        let blob_path = store.blob_file_path(blob_ref.file_id);
        let file_size = std::fs::metadata(&blob_path).unwrap().len();
        // File: 8 (header) + 4 (orig_size) + 1 (flag) + 4 (data_len) + compressed + 4 (crc)
        // For 100KB of repeated bytes, Zstd-1 should compress to ~1KB or less
        assert!(file_size < data.len() as u64 / 2,
            "Blob file ({}) should be much smaller than raw data ({})", file_size, data.len());
    }
}
