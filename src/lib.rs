//! MoteDB Storage Engine
//!
//! 面向嵌入式具身智能的高性能多模态数据库存储引擎
//! 
//! ## 核心特性
//! - 支持 TENSOR/SPATIAL/TEXT/TIMESTAMP 多模态数据
//! - 查询延迟 P99 ≤50ms
//! - 写入吞吐 ≥200 rows/sec
//! - 内存占用 ≤35MB
//!
//! ## 架构
//! - 存储层: WAL + Fragmented Column Store (5K rows/fragment)
//! - 索引层: Vamana (vector) + R-Tree (spatial) + Inverted (text) + B+Tree (timestamp)
//! - 查询层: Cost-based optimizer + Volcano-style executor
//! - 事务层: MVCC + Write-Ahead Logging

// 🔧 移除 jemalloc 以减小二进制大小
// 使用系统默认分配器
// #[cfg(not(target_env = "msvc"))]
// use tikv_jemallocator::Jemalloc;

// #[cfg(not(target_env = "msvc"))]
// #[global_allocator]
// static GLOBAL: Jemalloc = Jemalloc;

pub mod config;
pub mod storage;
pub mod index;
pub mod query;
pub mod txn;
pub mod types;
pub mod distance;
pub mod catalog;
pub mod sql;
pub mod ffi;  // FFI 接口，用于 C/Python/Node.js
pub mod cache;  // 🚀 P1: Row cache for performance

// 🔄 Modular database module (refactored from database_legacy.rs)
pub mod database;

mod error;
mod api;  // 内部 API 包装层

pub use config::{DBConfig, DurabilityLevel, LSMConfig, WALConfig};
pub use error::{Result, StorageError, MoteDBError};

// 主要对外 API (now using modular database)
pub use database::{MoteDB, DatabaseStats, VectorIndexStats, SpatialIndexStats, QueryProfile, TransactionStats};
pub use api::Database;  // 简化 API 包装
pub use catalog::TableRegistry;
pub use sql::{execute_sql, QueryResult};

// 🔌 导出分词器插件系统（方便用户直接使用）
pub mod tokenizers {
    pub use crate::index::tokenizers::*;
}
