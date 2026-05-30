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
//! - 索引层: Vamana (vector) + i-Octree (spatial) + Inverted (text) + B+Tree (timestamp)
//! - 查询层: Cost-based optimizer + streaming executor
//! - 事务层: MVCC + Write-Ahead Logging

// 🧠 jemalloc: background thread returns freed memory to OS (RSS plateaus instead of growing forever)
#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
use tikv_jemallocator::Jemalloc;

#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

/// Debug-only logging macro
/// Only prints in debug builds, silenced in release builds
#[macro_export]
macro_rules! debug_log {
    ($($arg:tt)*) => {
        #[cfg(debug_assertions)]
        {
            println!($($arg)*);
        }
        #[cfg(not(debug_assertions))]
        let _ = || {
            let _ = format_args!($($arg)*);
        };
    };
}

/// Always-on warning log — prints to stderr in all builds
#[macro_export]
macro_rules! warn_log {
    ($($arg:tt)*) => {
        eprintln!("[MoteDB WARN] {}", format_args!($($arg)*));
    };
}

/// Info log — prints to stderr when MOTEDB_LOG env var is set
#[macro_export]
macro_rules! info_log {
    ($($arg:tt)*) => {
        if std::env::var("MOTEDB_LOG").is_ok() {
            eprintln!("[MoteDB] {}", format_args!($($arg)*));
        }
    };
}

pub mod config;
pub mod storage;
pub mod index;
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

pub use config::{DBConfig, DurabilityLevel, LSMConfig, WALConfig, AutoCheckpointConfig};
pub use error::{Result, StorageError, MoteDBError};

// 主要对外 API (now using modular database)
pub use database::{MoteDB, QueryProfile, TransactionStats};
pub use api::Database;  // 简化 API 包装
pub use catalog::TableRegistry;
pub use sql::{ForEachResult, QueryResult, StreamingControl, StreamingQueryResult};

// 🔌 导出分词器插件系统（方便用户直接使用）
pub mod tokenizers {
    pub use crate::index::tokenizers::*;
}
