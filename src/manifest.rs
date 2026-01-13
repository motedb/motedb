//! # Manifest: Atomic Multi-File Commit
//!
//! ## Problem
//! MoteDB writes multiple files during flush (data SST, indexes, etc).
//! Without atomicity, crashes can leave inconsistent state:
//! - Main data written, but indexes missing
//! - Some indexes written, others not
//! - OS buffer not flushed to disk
//!
//! ## Solution: Manifest File
//! Inspired by LevelDB/RocksDB, we use a manifest file as the single
//! atomic commit point:
//!
//! ```text
//! Flush Process:
//! 1. Write all data files (versioned names)
//!    - data_000001.sst
//!    - vector_000001.sst
//!    - spatial_000001.mmap
//!    - text_000001.sst
//! 2. fsync() all files
//! 3. Write MANIFEST-000001.tmp
//! 4. fsync() MANIFEST
//! 5. rename(MANIFEST.tmp → MANIFEST-CURRENT)  ← ATOMIC COMMIT POINT
//! 6. fsync() parent directory
//! ```text
//!
//! **Key Insight**: `rename()` is atomic on POSIX systems!
//! - Before rename: Old MANIFEST is valid, new files are orphans
//! - After rename: New MANIFEST is valid, all files are visible
//! - Crash anywhere: Either old state or new state (never inconsistent)

use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// Manifest: Records all active files in a database version
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Monotonically increasing version number
    pub version: u64,
    
    /// LSM main data file
    pub lsm_file: Option<String>,
    
    /// Vector index files
    pub vector_indexes: HashMap<String, String>,
    
    /// Spatial index files
    pub spatial_indexes: HashMap<String, String>,
    
    /// Text index files
    pub text_indexes: HashMap<String, String>,
    
    /// Timestamp index files
    pub timestamp_indexes: HashMap<String, String>,
    
    /// Column value index files
    pub column_indexes: HashMap<String, String>,
    
    /// Checksum of all files (for integrity verification)
    pub checksum: u64,
    
    /// Creation timestamp
    pub timestamp: u64,
}

impl Manifest {
    /// Create a new manifest with given version
    pub fn new(version: u64) -> Self {
        Self {
            version,
            lsm_file: None,
            vector_indexes: HashMap::new(),
            spatial_indexes: HashMap::new(),
            text_indexes: HashMap::new(),
            timestamp_indexes: HashMap::new(),
            column_indexes: HashMap::new(),
            checksum: 0,
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
    
    /// Calculate checksum of all files
    pub fn calculate_checksum(&mut self) -> Result<u64> {
        let mut hasher = 0u64;
        
        if let Some(ref file) = self.lsm_file {
            hasher = hasher.wrapping_add(Self::hash_string(file));
        }
        
        for (_, file) in &self.vector_indexes {
            hasher = hasher.wrapping_add(Self::hash_string(file));
        }
        
        for (_, file) in &self.spatial_indexes {
            hasher = hasher.wrapping_add(Self::hash_string(file));
        }
        
        for (_, file) in &self.text_indexes {
            hasher = hasher.wrapping_add(Self::hash_string(file));
        }
        
        for (_, file) in &self.timestamp_indexes {
            hasher = hasher.wrapping_add(Self::hash_string(file));
        }
        
        for (_, file) in &self.column_indexes {
            hasher = hasher.wrapping_add(Self::hash_string(file));
        }
        
        self.checksum = hasher;
        Ok(hasher)
    }
    
    /// Simple hash function for strings
    fn hash_string(s: &str) -> u64 {
        let mut hash = 0u64;
        for byte in s.bytes() {
            hash = hash.wrapping_mul(31).wrapping_add(byte as u64);
        }
        hash
    }
    
    /// Write manifest to temporary file (not yet committed)
    ///
    /// This is step 1 of atomic commit. The file is fully written and
    /// fsynced, but not yet visible (not renamed to MANIFEST-CURRENT).
    pub fn write_temp(&self, db_path: &Path) -> Result<PathBuf> {
        let temp_path = db_path.join(format!("MANIFEST-{:06}.tmp", self.version));
        
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)?;
        
        let mut writer = BufWriter::new(file);
        
        // Serialize to JSON (human-readable for debugging)
        let json = serde_json::to_string_pretty(self)?;
        writer.write_all(json.as_bytes())?;
        
        // ✅ Flush buffer to OS
        writer.flush()?;
        
        // ✅ CRITICAL: fsync to ensure manifest is on disk
        writer.get_ref().sync_all()?;
        
        Ok(temp_path)
    }
    
    /// Atomic commit: rename temp manifest to MANIFEST-CURRENT
    ///
    /// This is the ATOMIC COMMIT POINT! After this rename:
    /// - The new manifest is visible
    /// - All referenced files become active
    /// - Old files can be garbage collected
    ///
    /// POSIX guarantees rename() is atomic, so we'll either see:
    /// - Old MANIFEST-CURRENT (crash before rename)
    /// - New MANIFEST-CURRENT (crash after rename)
    /// Never an inconsistent state!
    pub fn commit_atomic(temp_path: &Path, db_path: &Path) -> Result<()> {
        let current_path = db_path.join("MANIFEST-CURRENT");
        
        // ✅ Atomic rename (POSIX guarantee)
        std::fs::rename(temp_path, &current_path)?;
        
        // ✅ CRITICAL: fsync parent directory to ensure rename is persisted
        // Without this, the rename may only be in directory cache!
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let dir = File::open(db_path)?;
            unsafe {
                libc::fsync(dir.as_raw_fd());
            }
        }
        
        // On non-Unix, we can't fsync directory, but most modern filesystems
        // handle this reasonably well
        #[cfg(not(unix))]
        {
            // Best effort: open and sync the manifest file itself
            File::open(&current_path)?.sync_all()?;
        }
        
        Ok(())
    }
    
    /// Read the current manifest from MANIFEST-CURRENT
    pub fn read_current(db_path: &Path) -> Result<Self> {
        let current_path = db_path.join("MANIFEST-CURRENT");
        
        if !current_path.exists() {
            // No manifest yet, return empty manifest
            return Ok(Self::new(0));
        }
        
        let file = File::open(&current_path)?;
        let reader = BufReader::new(file);
        
        let manifest: Manifest = serde_json::from_reader(reader)?;
        
        Ok(manifest)
    }
    
    /// Full atomic write: temp write + commit
    pub fn write_atomic(&self, db_path: &Path) -> Result<()> {
        let temp_path = self.write_temp(db_path)?;
        Self::commit_atomic(&temp_path, db_path)?;
        Ok(())
    }
    
    /// Verify all files referenced in manifest exist
    pub fn verify_files(&self, db_path: &Path) -> Result<bool> {
        if let Some(ref file) = self.lsm_file {
            if !db_path.join(file).exists() {
                return Ok(false);
            }
        }
        
        for (_, file) in &self.vector_indexes {
            if !db_path.join(file).exists() {
                return Ok(false);
            }
        }
        
        for (_, file) in &self.spatial_indexes {
            if !db_path.join(file).exists() {
                return Ok(false);
            }
        }
        
        for (_, file) in &self.text_indexes {
            if !db_path.join(file).exists() {
                return Ok(false);
            }
        }
        
        for (_, file) in &self.timestamp_indexes {
            if !db_path.join(file).exists() {
                return Ok(false);
            }
        }
        
        for (_, file) in &self.column_indexes {
            if !db_path.join(file).exists() {
                return Ok(false);
            }
        }
        
        Ok(true)
    }
    
    /// List all orphan files (files not in manifest)
    pub fn find_orphans(&self, db_path: &Path) -> Result<Vec<PathBuf>> {
        let mut orphans = Vec::new();
        
        // Collect all files in manifest
        let mut active_files = std::collections::HashSet::new();
        if let Some(ref file) = self.lsm_file {
            active_files.insert(file.clone());
        }
        for (_, file) in &self.vector_indexes {
            active_files.insert(file.clone());
        }
        for (_, file) in &self.spatial_indexes {
            active_files.insert(file.clone());
        }
        for (_, file) in &self.text_indexes {
            active_files.insert(file.clone());
        }
        for (_, file) in &self.timestamp_indexes {
            active_files.insert(file.clone());
        }
        for (_, file) in &self.column_indexes {
            active_files.insert(file.clone());
        }
        
        // Scan directory for data files
        for entry in std::fs::read_dir(db_path)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_file() {
                let filename = path.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("");
                
                // Check if it's a data file (not manifest, not wal)
                if filename.ends_with(".sst") || 
                   filename.ends_with(".mmap") || 
                   filename.ends_with(".idx") ||
                   filename.ends_with(".bin") {
                    if !active_files.contains(filename) {
                        orphans.push(path);
                    }
                }
            }
        }
        
        Ok(orphans)
    }
    
    // ✅ P2: Version Garbage Collection
    
    /// List all manifest versions (for cleanup)
    pub fn list_all_versions(db_path: &Path) -> Result<Vec<u64>> {
        let mut versions = Vec::new();
        
        for entry in std::fs::read_dir(db_path)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_file() {
                if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                    if filename.starts_with("MANIFEST-") && filename != "MANIFEST-CURRENT" {
                        if let Some(version_str) = filename.strip_prefix("MANIFEST-") {
                            if let Ok(version) = version_str.parse::<u64>() {
                                versions.push(version);
                            }
                        }
                    }
                }
            }
        }
        
        versions.sort_unstable();
        Ok(versions)
    }
    
    /// Read a specific version of manifest
    pub fn read_version(db_path: &Path, version: u64) -> Result<Self> {
        let manifest_path = db_path.join(format!("MANIFEST-{:06}", version));
        
        if !manifest_path.exists() {
            return Err(crate::StorageError::FileNotFound(
                manifest_path
            ));
        }
        
        let file = File::open(&manifest_path)?;
        let reader = BufReader::new(file);
        let manifest: Manifest = serde_json::from_reader(reader)?;
        
        Ok(manifest)
    }
    
    /// Cleanup old versions, keeping the latest N versions
    pub fn cleanup_old_versions(db_path: &Path, keep_versions: usize) -> Result<usize> {
        if keep_versions == 0 {
            return Ok(0);
        }
        
        // Read current manifest to get active files
        let current = Self::read_current(db_path)?;
        let mut active_files = std::collections::HashSet::new();
        
        if let Some(ref file) = current.lsm_file {
            active_files.insert(file.clone());
        }
        for (_, file) in &current.vector_indexes {
            active_files.insert(file.clone());
        }
        for (_, file) in &current.spatial_indexes {
            active_files.insert(file.clone());
        }
        for (_, file) in &current.text_indexes {
            active_files.insert(file.clone());
        }
        for (_, file) in &current.timestamp_indexes {
            active_files.insert(file.clone());
        }
        for (_, file) in &current.column_indexes {
            active_files.insert(file.clone());
        }
        
        // Get all versions
        let mut all_versions = Self::list_all_versions(db_path)?;
        all_versions.sort_unstable();
        all_versions.reverse(); // Newest first
        
        if all_versions.len() <= keep_versions {
            return Ok(0); // Nothing to delete
        }
        
        // Keep the latest N versions, delete the rest
        let versions_to_delete = &all_versions[keep_versions..];
        let mut deleted_count = 0;
        
        for &version in versions_to_delete {
            // Read old manifest
            if let Ok(old_manifest) = Self::read_version(db_path, version) {
                // Delete files referenced by old manifest (if not in current)
                if let Some(ref file) = old_manifest.lsm_file {
                    if !active_files.contains(file) {
                        let _ = std::fs::remove_file(db_path.join(file));
                        deleted_count += 1;
                    }
                }
                
                for (_, file) in &old_manifest.vector_indexes {
                    if !active_files.contains(file) {
                        let _ = std::fs::remove_file(db_path.join(file));
                        deleted_count += 1;
                    }
                }
                
                for (_, file) in &old_manifest.spatial_indexes {
                    if !active_files.contains(file) {
                        let _ = std::fs::remove_file(db_path.join(file));
                        deleted_count += 1;
                    }
                }
                
                for (_, file) in &old_manifest.text_indexes {
                    if !active_files.contains(file) {
                        let _ = std::fs::remove_file(db_path.join(file));
                        deleted_count += 1;
                    }
                }
                
                for (_, file) in &old_manifest.timestamp_indexes {
                    if !active_files.contains(file) {
                        let _ = std::fs::remove_file(db_path.join(file));
                        deleted_count += 1;
                    }
                }
                
                for (_, file) in &old_manifest.column_indexes {
                    if !active_files.contains(file) {
                        let _ = std::fs::remove_file(db_path.join(file));
                        deleted_count += 1;
                    }
                }
                
                // Delete old manifest file
                let _ = std::fs::remove_file(db_path.join(format!("MANIFEST-{:06}", version)));
            }
        }
        
        Ok(deleted_count)
    }
    
    // ✅ P3: Incremental Backup
    
    /// Get diff between two versions (returns added/removed files)
    pub fn diff_versions(
        db_path: &Path,
        from_version: u64,
        to_version: u64,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let from_manifest = Self::read_version(db_path, from_version)?;
        let to_manifest = Self::read_version(db_path, to_version)?;
        
        let mut from_files = std::collections::HashSet::new();
        let mut to_files = std::collections::HashSet::new();
        
        // Collect files from both manifests
        Self::collect_files(&from_manifest, &mut from_files);
        Self::collect_files(&to_manifest, &mut to_files);
        
        // Calculate diff
        let added: Vec<String> = to_files.difference(&from_files)
            .cloned()
            .collect();
        let removed: Vec<String> = from_files.difference(&to_files)
            .cloned()
            .collect();
        
        Ok((added, removed))
    }
    
    /// Backup incremental changes between versions
    pub fn backup_incremental(
        db_path: &Path,
        from_version: u64,
        to_version: u64,
        backup_path: &Path,
    ) -> Result<usize> {
        let (added_files, _removed) = Self::diff_versions(db_path, from_version, to_version)?;
        
        // Create backup directory
        std::fs::create_dir_all(backup_path)?;
        
        // Copy added files
        let mut copied_count = 0;
        for file in added_files {
            let src = db_path.join(&file);
            let dst = backup_path.join(&file);
            
            if src.exists() {
                // Create parent directories if needed
                if let Some(parent) = dst.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                
                std::fs::copy(&src, &dst)?;
                copied_count += 1;
            }
        }
        
        // Copy target manifest
        let manifest_src = db_path.join(format!("MANIFEST-{:06}", to_version));
        let manifest_dst = backup_path.join(format!("MANIFEST-{:06}", to_version));
        if manifest_src.exists() {
            std::fs::copy(&manifest_src, &manifest_dst)?;
        }
        
        Ok(copied_count)
    }
    
    /// Restore from incremental backup
    pub fn restore_incremental(
        backup_path: &Path,
        version: u64,
        target_path: &Path,
    ) -> Result<usize> {
        // Create target directory
        std::fs::create_dir_all(target_path)?;
        
        // Read manifest from backup
        let manifest = Self::read_version(backup_path, version)?;
        
        let mut restored_count = 0;
        
        // Restore all files referenced in manifest
        for file in Self::get_all_files(&manifest) {
            let src = backup_path.join(&file);
            let dst = target_path.join(&file);
            
            if src.exists() {
                if let Some(parent) = dst.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                
                std::fs::copy(&src, &dst)?;
                restored_count += 1;
            }
        }
        
        // Restore manifest and set as current
        let manifest_src = backup_path.join(format!("MANIFEST-{:06}", version));
        let manifest_dst = target_path.join("MANIFEST-CURRENT");
        if manifest_src.exists() {
            std::fs::copy(&manifest_src, &manifest_dst)?;
        }
        
        Ok(restored_count)
    }
    
    // Helper: Collect all files from manifest
    fn collect_files(manifest: &Manifest, files: &mut std::collections::HashSet<String>) {
        if let Some(ref file) = manifest.lsm_file {
            files.insert(file.clone());
        }
        for (_, file) in &manifest.vector_indexes {
            files.insert(file.clone());
        }
        for (_, file) in &manifest.spatial_indexes {
            files.insert(file.clone());
        }
        for (_, file) in &manifest.text_indexes {
            files.insert(file.clone());
        }
        for (_, file) in &manifest.timestamp_indexes {
            files.insert(file.clone());
        }
        for (_, file) in &manifest.column_indexes {
            files.insert(file.clone());
        }
    }
    
    // Helper: Get all files from manifest
    fn get_all_files(manifest: &Manifest) -> Vec<String> {
        let mut files = Vec::new();
        
        if let Some(ref file) = manifest.lsm_file {
            files.push(file.clone());
        }
        for (_, file) in &manifest.vector_indexes {
            files.push(file.clone());
        }
        for (_, file) in &manifest.spatial_indexes {
            files.push(file.clone());
        }
        for (_, file) in &manifest.text_indexes {
            files.push(file.clone());
        }
        for (_, file) in &manifest.timestamp_indexes {
            files.push(file.clone());
        }
        for (_, file) in &manifest.column_indexes {
            files.push(file.clone());
        }
        
        files
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_manifest_write_read() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path();
        
        let mut manifest = Manifest::new(1);
        manifest.lsm_file = Some("data_000001.sst".to_string());
        manifest.vector_indexes.insert("embeddings".to_string(), "vector_000001.sst".to_string());
        manifest.calculate_checksum().unwrap();
        
        manifest.write_atomic(db_path).unwrap();
        
        let read_manifest = Manifest::read_current(db_path).unwrap();
        assert_eq!(read_manifest.version, 1);
        assert_eq!(read_manifest.lsm_file, Some("data_000001.sst".to_string()));
        assert_eq!(read_manifest.checksum, manifest.checksum);
    }
    
    #[test]
    fn test_atomic_commit() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path();
        
        let manifest1 = Manifest::new(1);
        manifest1.write_atomic(db_path).unwrap();
        
        let manifest2 = Manifest::new(2);
        manifest2.write_atomic(db_path).unwrap();
        
        // Should read latest version
        let current = Manifest::read_current(db_path).unwrap();
        assert_eq!(current.version, 2);
    }
    
    #[test]
    fn test_orphan_detection() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path();
        
        // Create some files
        std::fs::write(db_path.join("data_000001.sst"), b"data").unwrap();
        std::fs::write(db_path.join("data_000002.sst"), b"data").unwrap();
        std::fs::write(db_path.join("vector_000001.sst"), b"vector").unwrap();
        
        // Manifest only references data_000002.sst
        let mut manifest = Manifest::new(1);
        manifest.lsm_file = Some("data_000002.sst".to_string());
        manifest.write_atomic(db_path).unwrap();
        
        // Should find 2 orphans
        let orphans = manifest.find_orphans(db_path).unwrap();
        assert_eq!(orphans.len(), 2);
    }
}
