//! Manifest 文件管理和持久化

use super::version::{Version, VersionEdit, FileMetadata, FileType};
use crate::{Result, StorageError};
use std::fs::{self, File, OpenOptions};
use std::io::{Write, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use serde::{Deserialize, Serialize};
use crc32fast::Hasher;

/// Manifest 记录类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ManifestRecord {
    /// 添加文件
    AddFile(FileMetadata),
    /// 删除文件
    DeleteFile { file_id: u64, file_type: FileType },
    /// 版本提交标记
    VersionCommit { version: u64 },
}

/// Manifest 管理器
pub struct Manifest {
    /// 数据目录
    data_dir: PathBuf,
    /// 当前版本
    current_version: Arc<Mutex<Version>>,
    /// Manifest 文件
    manifest_file: Arc<Mutex<File>>,
    /// 下一个版本号
    next_version: Arc<Mutex<u64>>,
    /// Manifest 文件编号
    manifest_number: u64,
}

impl Manifest {
    /// 创建或加载 Manifest
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir)?;
        
        let current_path = data_dir.join("CURRENT");
        
        // 读取 CURRENT 文件获取当前 Manifest
        let (manifest_number, version) = if current_path.exists() {
            let manifest_name = fs::read_to_string(&current_path)?;
            let manifest_path = data_dir.join(manifest_name.trim());
            
            // 恢复版本信息
            let version = Self::recover_version(&manifest_path)?;
            
            // 提取 Manifest 编号
            let manifest_number = manifest_name
                .trim()
                .strip_prefix("MANIFEST-")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(1);
            
            (manifest_number, version)
        } else {
            // 新建 Manifest
            (1, Version::new(0))
        };
        
        let manifest_path = data_dir.join(format!("MANIFEST-{:06}", manifest_number));
        let manifest_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&manifest_path)?;
        
        // 更新 CURRENT 文件
        let mut current_file = File::create(&current_path)?;
        writeln!(current_file, "MANIFEST-{:06}", manifest_number)?;
        current_file.sync_all()?;
        
        let next_version = version.version_number + 1;
        
        Ok(Self {
            data_dir,
            current_version: Arc::new(Mutex::new(version)),
            manifest_file: Arc::new(Mutex::new(manifest_file)),
            next_version: Arc::new(Mutex::new(next_version)),
            manifest_number,
        })
    }
    
    /// 从 Manifest 文件恢复版本
    fn recover_version(manifest_path: &Path) -> Result<Version> {
        let mut file = File::open(manifest_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        
        let mut current_version = Version::new(0);
        let mut last_committed_version = Version::new(0);
        
        // 使用 bincode 反序列化记录列表
        // 格式：每条记录的长度(u32) + 记录数据
        let mut offset = 0;
        while offset < buffer.len() {
            if offset + 4 > buffer.len() {
                break;
            }
            
            // 读取记录长度
            let len = u32::from_le_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ]) as usize;
            offset += 4;
            
            if offset + len > buffer.len() {
                break;
            }
            
            // 反序列化记录
            if let Ok(record) = bincode::deserialize::<ManifestRecord>(&buffer[offset..offset + len]) {
                match &record {
                    ManifestRecord::AddFile(meta) => {
                        current_version.add_file(meta.clone());
                    }
                    ManifestRecord::DeleteFile { file_id, file_type } => {
                        current_version.delete_file(*file_id, file_type);
                    }
                    ManifestRecord::VersionCommit { version } => {
                        // 提交当前版本
                        current_version.version_number = *version;
                        last_committed_version = current_version.clone();
                    }
                }
            }
            offset += len;
        }
        
        // 返回最后一个提交的版本（崩溃前的完整版本）
        Ok(last_committed_version)
    }
    
    /// 获取当前版本（只读）
    pub fn current_version(&self) -> Version {
        self.current_version.lock()
            .expect("Manifest current_version lock poisoned")
            .clone()
    }
    
    /// 应用版本编辑（原子性提交，带文件验证）
    pub fn apply_edit(&self, edit: VersionEdit) -> Result<u64> {
        if edit.is_empty() {
            return Ok(self.current_version.lock()
                .expect("Manifest lock poisoned")
                .version_number);
        }
        
        // Step 1: 验证所有文件存在且完整
        for meta in &edit.add_files {
            let file_path = self.data_dir.join(&meta.path);
            
            // 检查文件存在
            if !file_path.exists() {
                return Err(StorageError::FileNotFound(file_path));
            }
            
            // 验证文件大小
            let actual_size = fs::metadata(&file_path)
                .map_err(|e| StorageError::Io(e))?
                .len();
            
            if actual_size != meta.size {
                return Err(StorageError::Corruption(
                    format!("File size mismatch: {} (expected {}, got {})",
                        meta.path, meta.size, actual_size)
                ));
            }
            
            // 验证 CRC32 校验码
            let actual_checksum = Self::calculate_checksum(&file_path)?;
            if actual_checksum != meta.checksum {
                return Err(StorageError::CorruptedFile(file_path));
            }
        }
        
        let mut version = self.current_version.lock()
            .map_err(|_| StorageError::Lock("Version lock poisoned".into()))?;
        let mut file = self.manifest_file.lock()
            .map_err(|_| StorageError::Lock("Manifest file lock poisoned".into()))?;
        let mut next_ver = self.next_version.lock()
            .map_err(|_| StorageError::Lock("Next version lock poisoned".into()))?;
        
        // Step 2: 写入添加文件记录
        for meta in &edit.add_files {
            let record = ManifestRecord::AddFile(meta.clone());
            let data = bincode::serialize(&record)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            // 写入记录长度 + 数据
            file.write_all(&(data.len() as u32).to_le_bytes())?;
            file.write_all(&data)?;
        }
        
        // Step 3: 写入删除文件记录
        for (file_id, file_type) in &edit.delete_files {
            let record = ManifestRecord::DeleteFile {
                file_id: *file_id,
                file_type: file_type.clone(),
            };
            let data = bincode::serialize(&record)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            file.write_all(&(data.len() as u32).to_le_bytes())?;
            file.write_all(&data)?;
        }
        
        // Step 4: fsync（确保元数据写入）
        file.sync_all()?;
        
        // Step 5: 写入版本提交标记（原子性边界）
        let commit_record = ManifestRecord::VersionCommit { version: *next_ver };
        let data = bincode::serialize(&commit_record)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        file.write_all(&(data.len() as u32).to_le_bytes())?;
        file.write_all(&data)?;
        
        // Step 6: fsync 提交记录
        file.sync_all()?;
        
        // Step 7: 更新内存中的版本
        for meta in &edit.add_files {
            version.add_file(meta.clone());
        }
        for (file_id, file_type) in &edit.delete_files {
            version.delete_file(*file_id, file_type);
        }
        version.version_number = *next_ver;
        
        let committed_version = *next_ver;
        *next_ver += 1;
        
        Ok(committed_version)
    }
    
    /// 计算文件的 CRC32 校验码
    fn calculate_checksum(path: &Path) -> Result<u32> {
        let mut file = File::open(path)?;
        let mut hasher = Hasher::new();
        let mut buffer = vec![0u8; 65536]; // 64KB buffer
        
        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            hasher.update(&buffer[..n]);
        }
        
        Ok(hasher.finalize())
    }
    
    /// 清理未在当前版本中的文件
    pub fn garbage_collect(&self) -> Result<Vec<String>> {
        let version = self.current_version.lock()
            .map_err(|_| StorageError::Lock("Version lock poisoned".into()))?;
        let active_files = version.all_file_names();
        
        let mut deleted_files = Vec::new();
        
        // 扫描数据目录
        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy().to_string();
            
            // 跳过 MANIFEST 和 CURRENT
            if file_name_str.starts_with("MANIFEST") || file_name_str == "CURRENT" {
                continue;
            }
            
            // 删除不在当前版本中的文件
            if !active_files.contains(&file_name_str) {
                fs::remove_file(entry.path())?;
                deleted_files.push(file_name_str);
            }
        }
        
        Ok(deleted_files)
    }
    
    /// 获取数据目录
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_manifest_atomic_commit() {
        let temp_dir = TempDir::new().unwrap();
        let manifest = Manifest::open(temp_dir.path()).unwrap();
        
        // 创建假的测试文件
        let sst_path = temp_dir.path().join("sstable_00001.sst");
        let ts_path = temp_dir.path().join("timestamp_idx_00001.idx");
        let text_path = temp_dir.path().join("text_idx_00001.idx");
        
        std::fs::write(&sst_path, vec![0u8; 1024]).unwrap();
        std::fs::write(&ts_path, vec![0u8; 512]).unwrap();
        std::fs::write(&text_path, vec![0u8; 256]).unwrap();
        
        // 计算实际的 checksum
        let sst_checksum = Manifest::calculate_checksum(&sst_path).unwrap();
        let ts_checksum = Manifest::calculate_checksum(&ts_path).unwrap();
        let text_checksum = Manifest::calculate_checksum(&text_path).unwrap();
        
        // 提交第一个版本（数据 + 多个索引）
        let mut edit = VersionEdit::new();
        edit.add_file(FileMetadata {
            file_id: 1,
            file_type: FileType::SSTable,
            path: "sstable_00001.sst".to_string(),
            size: 1024,
            checksum: sst_checksum,
            min_key: Some(0),
            max_key: Some(100),
            level: Some(0),
        });
        edit.add_file(FileMetadata {
            file_id: 1,
            file_type: FileType::TimestampIndex,
            path: "timestamp_idx_00001.idx".to_string(),
            size: 512,
            checksum: ts_checksum,
            min_key: None,
            max_key: None,
            level: None,
        });
        edit.add_file(FileMetadata {
            file_id: 1,
            file_type: FileType::TextIndex,
            path: "text_idx_00001.idx".to_string(),
            size: 256,
            checksum: text_checksum,
            min_key: None,
            max_key: None,
            level: None,
        });
        
        let v1 = manifest.apply_edit(edit).unwrap();
        assert_eq!(v1, 1);
        
        let version = manifest.current_version();
        assert_eq!(version.files.len(), 3);
        assert_eq!(version.files[&FileType::SSTable].len(), 1);
        assert_eq!(version.files[&FileType::TimestampIndex].len(), 1);
        assert_eq!(version.files[&FileType::TextIndex].len(), 1);
    }
    
    #[test]
    fn test_crash_recovery() {
        let temp_dir = TempDir::new().unwrap();
        
        // 创建假的测试文件
        let sst_path = temp_dir.path().join("sstable_00001.sst");
        std::fs::write(&sst_path, vec![0u8; 1024]).unwrap();
        let sst_checksum = Manifest::calculate_checksum(&sst_path).unwrap();
        
        // 第一次运行：提交版本
        {
            let manifest = Manifest::open(temp_dir.path()).unwrap();
            let mut edit = VersionEdit::new();
            edit.add_file(FileMetadata {
                file_id: 1,
                file_type: FileType::SSTable,
                path: "sstable_00001.sst".to_string(),
                size: 1024,
                checksum: sst_checksum,
                min_key: Some(0),
                max_key: Some(100),
                level: Some(0),
            });
            manifest.apply_edit(edit).unwrap();
        }
        
        // 模拟崩溃重启：重新打开 Manifest
        {
            let manifest = Manifest::open(temp_dir.path()).unwrap();
            let version = manifest.current_version();
            
            // 应该恢复已提交的版本
            assert_eq!(version.version_number, 1);
            assert_eq!(version.files[&FileType::SSTable].len(), 1);
        }
    }
}
