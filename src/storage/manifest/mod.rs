//! Manifest: 版本管理和原子性提交
//!
//! ## 核心功能
//! 1. **原子性提交**: 所有文件变更（数据 + 多索引）一次性提交
//! 2. **版本管理**: 每次刷盘生成新版本，记录完整文件快照
//! 3. **崩溃恢复**: 只加载 Manifest 中已提交的版本

mod manifest;
mod version;

pub use manifest::{Manifest, ManifestRecord};
pub use version::{Version, VersionEdit, FileMetadata, FileType};
