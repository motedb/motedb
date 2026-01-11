//! File Reference Manager
//!
//! Provides mmap-safe file management with reference counting.
//! 
//! ## Features
//! - Reference counting for open files
//! - Delayed deletion (waits for all references to close)
//! - RAII-based handle management
//! - Thread-safe operations
//!
//! ## Safety
//! - Prevents mmap invalidation from premature file deletion
//! - Automatic cleanup when last reference is dropped
//! - No manual file lifecycle management needed

use crate::{Result, StorageError};
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

/// File reference with metadata
struct FileRef {
    /// File handle
    file: Arc<File>,
    
    /// Reference count (number of active handles)
    ref_count: AtomicUsize,
    
    /// Pending deletion flag
    delete_pending: AtomicBool,
}

/// File reference manager (thread-safe)
#[derive(Clone)]
pub struct FileRefManager {
    /// Map of path -> FileRef
    refs: Arc<RwLock<HashMap<PathBuf, Arc<FileRef>>>>,
}

impl FileRefManager {
    /// Create a new file reference manager
    pub fn new() -> Self {
        Self {
            refs: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Open a file (or get existing reference)
    /// 
    /// Returns a FileHandle that automatically manages the reference count.
    pub fn open<P: AsRef<Path>>(&self, path: P) -> Result<FileHandle> {
        let path = path.as_ref().to_path_buf();
        let mut refs = self.refs.write()
            .map_err(|_| StorageError::Lock("FileRefManager lock poisoned".into()))?;
        
        let file_ref = if let Some(existing) = refs.get(&path) {
            // File already open, increment ref count
            existing.ref_count.fetch_add(1, Ordering::SeqCst);
            existing.clone()
        } else {
            // Open new file
            let file = File::open(&path)?;
            let file_ref = Arc::new(FileRef {
                file: Arc::new(file),
                ref_count: AtomicUsize::new(1),
                delete_pending: AtomicBool::new(false),
            });
            refs.insert(path.clone(), file_ref.clone());
            file_ref
        };
        
        Ok(FileHandle {
            file: file_ref.file.clone(),
            path: path.clone(),
            file_ref,
            manager: self.clone(),
        })
    }
    
    /// Acquire a reference to a file (alias for open)
    pub fn acquire<P: AsRef<Path>>(&self, path: P) -> Result<FileHandle> {
        self.open(path)
    }
    
    /// Mark a file for deletion (deferred until all references are dropped)
    pub fn mark_for_deletion<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path = path.as_ref();
        let refs = self.refs.read()
            .map_err(|_| StorageError::Lock("FileRefManager lock poisoned".into()))?;
        
        if let Some(file_ref) = refs.get(path) {
            file_ref.delete_pending.store(true, Ordering::SeqCst);
        }
        // If file not in refs, it's either already deleted or never opened
        // In both cases, we can safely ignore
        
        Ok(())
    }
    
    /// Close a file (decrement ref count, delete if pending)
    fn close(&self, path: &Path, file_ref: &Arc<FileRef>) {
        let count = file_ref.ref_count.fetch_sub(1, Ordering::SeqCst);
        
        // If this was the last reference and deletion is pending
        if count == 1 && file_ref.delete_pending.load(Ordering::SeqCst) {
            // Remove from map
            if let Ok(mut refs) = self.refs.write() {
                refs.remove(path);
            }
            
            // Delete file (best effort)
            let _ = std::fs::remove_file(path);
        }
    }
    
    /// Get current reference count for a file (for testing/debugging)
    pub fn ref_count<P: AsRef<Path>>(&self, path: P) -> usize {
        let path = path.as_ref();
        if let Ok(refs) = self.refs.read() {
            if let Some(file_ref) = refs.get(path) {
                return file_ref.ref_count.load(Ordering::SeqCst);
            }
        }
        0
    }
    
    /// Check if a file is marked for deletion
    pub fn is_pending_deletion<P: AsRef<Path>>(&self, path: P) -> bool {
        let path = path.as_ref();
        if let Ok(refs) = self.refs.read() {
            if let Some(file_ref) = refs.get(path) {
                return file_ref.delete_pending.load(Ordering::SeqCst);
            }
        }
        false
    }
}

impl Default for FileRefManager {
    fn default() -> Self {
        Self::new()
    }
}

/// File handle (RAII-managed)
pub struct FileHandle {
    /// File reference
    pub file: Arc<File>,
    
    /// File path
    path: PathBuf,
    
    /// File ref (for ref counting)
    file_ref: Arc<FileRef>,
    
    /// Manager reference
    manager: FileRefManager,
}

impl Drop for FileHandle {
    fn drop(&mut self) {
        // Automatically close when handle is dropped
        self.manager.close(&self.path, &self.file_ref);
    }
}

impl FileHandle {
    /// Get path
    pub fn path(&self) -> &Path {
        &self.path
    }
    
    /// Get file reference
    pub fn file(&self) -> &Arc<File> {
        &self.file
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;
    
    #[test]
    fn test_open_and_close() {
        let manager = FileRefManager::new();
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"test data").unwrap();
        temp_file.flush().unwrap();
        let path = temp_file.path();
        
        // Open file
        {
            let handle = manager.open(path).unwrap();
            assert_eq!(manager.ref_count(path), 1);
            assert_eq!(handle.path(), path);
        }
        
        // After drop, ref count should be 0
        assert_eq!(manager.ref_count(path), 0);
    }
    
    #[test]
    fn test_multiple_references() {
        let manager = FileRefManager::new();
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"test data").unwrap();
        temp_file.flush().unwrap();
        let path = temp_file.path();
        
        // Open multiple handles
        let handle1 = manager.open(path).unwrap();
        assert_eq!(manager.ref_count(path), 1);
        
        let handle2 = manager.open(path).unwrap();
        assert_eq!(manager.ref_count(path), 2);
        
        let handle3 = manager.open(path).unwrap();
        assert_eq!(manager.ref_count(path), 3);
        
        // Drop one handle
        drop(handle1);
        assert_eq!(manager.ref_count(path), 2);
        
        // Drop all
        drop(handle2);
        drop(handle3);
        assert_eq!(manager.ref_count(path), 0);
    }
    
    #[test]
    fn test_deferred_deletion() {
        let manager = FileRefManager::new();
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.into_temp_path();
        
        // Write some data
        std::fs::write(&path, b"test data").unwrap();
        
        // Open file
        let handle = manager.open(&path).unwrap();
        assert_eq!(manager.ref_count(&path), 1);
        assert!(path.exists());
        
        // Mark for deletion (but file still has reference)
        manager.mark_for_deletion(&path).unwrap();
        assert!(manager.is_pending_deletion(&path));
        assert!(path.exists()); // File still exists
        
        // Drop handle - file should be deleted
        drop(handle);
        
        // Give OS time to delete file
        std::thread::sleep(std::time::Duration::from_millis(100));
        
        assert!(!path.exists()); // File deleted
        assert_eq!(manager.ref_count(&path), 0);
    }
    
    #[test]
    fn test_concurrent_access() {
        use std::thread;
        
        let manager = FileRefManager::new();
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"test data").unwrap();
        temp_file.flush().unwrap();
        let path = temp_file.path().to_path_buf();
        
        let manager_clone = manager.clone();
        let path_clone = path.clone();
        
        // Thread 1: Open and hold
        let handle1 = manager.open(&path).unwrap();
        
        // Thread 2: Try to open concurrently
        let thread = thread::spawn(move || {
            let handle2 = manager_clone.open(&path_clone).unwrap();
            assert_eq!(manager_clone.ref_count(&path_clone), 2);
            handle2
        });
        
        let handle2 = thread.join().unwrap();
        assert_eq!(manager.ref_count(&path), 2);
        
        drop(handle1);
        drop(handle2);
        assert_eq!(manager.ref_count(&path), 0);
    }
}
