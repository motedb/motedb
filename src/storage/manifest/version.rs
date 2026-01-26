//! Version 和文件元数据管理

use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};

/// 文件类型
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum FileType {
    /// 单文件 LSM 数据文件 (新)
    LSMData,
    /// SSTable 数据文件 (旧，兼容)
    SSTable,
    /// B+Tree 索引文件 (通用索引)
    BTreeIndex,
    /// 时间戳索引文件 (使用 BTree)
    TimestampIndex,
    /// 文本索引文件 (LSM 存储)
    TextIndexLSM,
    /// 文本索引字典文件
    TextIndexDict,
    /// 向量索引文件
    VectorIndex,
    /// 空间索引文件
    SpatialIndex,
    /// Blob 文件
    Blob,
}

/// 文件元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    /// 文件 ID
    pub file_id: u64,
    /// 文件类型
    pub file_type: FileType,
    /// 文件路径（相对于数据目录）
    pub path: String,
    /// 文件大小（字节）
    pub size: u64,
    /// CRC32 校验码
    pub checksum: u32,
    /// 最小 key (对于 SSTable)
    pub min_key: Option<u64>,
    /// 最大 key (对于 SSTable)
    pub max_key: Option<u64>,
    /// LSM Level (对于 SSTable)
    pub level: Option<u32>,
}

impl FileMetadata {
    pub fn file_name(&self) -> String {
        // 使用 path 字段，向后兼容旧格式
        if !self.path.is_empty() {
            return self.path.clone();
        }
        
        // 旧格式兼容
        match self.file_type {
            FileType::LSMData => format!("lsm_{:05}.sst", self.file_id),
            FileType::SSTable => format!("sstable_{:05}.sst", self.file_id),
            FileType::BTreeIndex => format!("btree_{:05}.btree", self.file_id),
            FileType::TimestampIndex => format!("timestamp_idx_{:05}.idx", self.file_id),
            FileType::TextIndexLSM => format!("text_{:05}.lsm", self.file_id),
            FileType::TextIndexDict => format!("text_{:05}.dict", self.file_id),
            FileType::VectorIndex => format!("vector_idx_{:05}.idx", self.file_id),
            FileType::SpatialIndex => format!("spatial_idx_{:05}.idx", self.file_id),
            FileType::Blob => format!("blob_{:05}.blob", self.file_id),
        }
    }
}

/// 版本快照
#[derive(Debug, Clone)]
pub struct Version {
    /// 版本号
    pub version_number: u64,
    /// 所有活跃文件（按类型分组）
    pub files: HashMap<FileType, Vec<FileMetadata>>,
}

impl Version {
    pub fn new(version_number: u64) -> Self {
        Self {
            version_number,
            files: HashMap::new(),
        }
    }
    
    /// 添加文件
    pub fn add_file(&mut self, meta: FileMetadata) {
        self.files
            .entry(meta.file_type.clone())
            .or_default()
            .push(meta);
    }
    
    /// 删除文件
    pub fn delete_file(&mut self, file_id: u64, file_type: &FileType) {
        if let Some(files) = self.files.get_mut(file_type) {
            files.retain(|f| f.file_id != file_id);
        }
    }
    
    /// 获取所有文件的文件名集合
    pub fn all_file_names(&self) -> HashSet<String> {
        self.files
            .values()
            .flatten()
            .map(|f| f.file_name())
            .collect()
    }
}

/// 版本编辑器（批量变更）
pub struct VersionEdit {
    /// 待添加的文件
    pub add_files: Vec<FileMetadata>,
    /// 待删除的文件
    pub delete_files: Vec<(u64, FileType)>,
}

impl VersionEdit {
    pub fn new() -> Self {
        Self {
            add_files: Vec::new(),
            delete_files: Vec::new(),
        }
    }
    
    /// 添加文件
    pub fn add_file(&mut self, meta: FileMetadata) {
        self.add_files.push(meta);
    }
    
    /// 删除文件
    pub fn delete_file(&mut self, file_id: u64, file_type: FileType) {
        self.delete_files.push((file_id, file_type));
    }
    
    /// 检查是否为空
    pub fn is_empty(&self) -> bool {
        self.add_files.is_empty() && self.delete_files.is_empty()
    }
}

impl Default for VersionEdit {
    fn default() -> Self {
        Self::new()
    }
}
