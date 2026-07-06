//! # MoteDB
//!
//! AI-native embedded multimodal database for embodied intelligence (robots,
//! AR glasses, industrial arms). A single embedded Rust library providing
//! columnar storage with ACID transactions, vector search, full-text search,
//! and spatial indexing.
//!
//! ## Status
//!
//! Pre-1.0. The [`Database`] embedding API and the storage/transaction engine
//! are stable and heavily tested. The SQL surface and the FFI bindings
//! ([`ffi`], [`tokenizers`]) are still evolving — see the crate README for the
//! supported SQL subset. The internal modules ([`storage`], [`index`], [`txn`],
//! [`database`]) are exposed for advanced/embedded use but their exact types
//! are **not** part of the stable API yet.
//!
//! ## Quick start
//!
//! ```no_run
//! use motedb::{Database, QueryResult};
//!
//! let db = Database::create("my_data")?;
//! db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")?;
//! db.execute("INSERT INTO t VALUES (1, 100)")?;
//! let r = db.execute("SELECT * FROM t")?;
//! if let QueryResult::Select { rows, .. } = r.materialize()? {
//!     println!("{:?}", rows);
//! }
//! # Ok::<(), motedb::StorageError>(())
//! ```
//!
//! ## Architecture
//!
//! - **Storage:** append-only columnar segments (source of truth) + WAL for
//!   durability, with Snappy/Zstd compression and mmap zero-copy reads.
//! - **Indexes:** DiskANN/Vamana (vector) + i-Octree (spatial) + inverted
//!   index (text) + B+Tree (column/timestamp).
//! - **Transactions:** MVCC version store with snapshot isolation and
//!   write-ahead logging for crash recovery.
//!
//! ## Performance (indicative, Apple Silicon)
//!
//! On a 300K-row × 4-column workload vs SQLite WAL: COUNT/SUM under WHERE
//! ~5×, ORDER BY + LIMIT ~2.5×, PK point lookup sub-microsecond. See
//! `BENCHMARK.md` and the docs for methodology and full numbers.

// Crate-wide clippy allowances for lint classes that are design choices in a
// columnar DB rather than bugs. Per-site cleanups are still welcome, but these
// fire often enough on the hot paths that we silence them globally rather than
// annotate every signature.
#![allow(
    clippy::type_complexity,    // heavily-typed columnar decoders/iterators
    clippy::too_many_arguments, // batch write/scan constructors
    clippy::needless_range_loop, // index loops over fixed-width column slots
    // Doc-comment markdown rendering preferences (mixed Chinese/English comments
    // trip these); not code defects.
    clippy::doc_lazy_continuation,
    clippy::empty_line_after_doc_comments
)]

// 🧠 jemalloc: background thread returns freed memory to OS (RSS plateaus instead of growing forever)
#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
use tikv_jemallocator::Jemalloc;

#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

/// Explicitly purge jemalloc's dirty pages back to the OS.
/// Call after bulk operations (CREATE INDEX, compaction) that create
/// large transient allocations, to keep RSS low on edge devices.
/// No-op when jemalloc is not enabled.
pub fn purge_memory_to_os() {
    #[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
    {
        // Advance epoch to refresh arena stats, then purge each arena.
        // "arena.<i>.purge" forces all dirty/muzzy pages back to the OS.
        use tikv_jemalloc_ctl::{arenas, epoch};
        let _ = epoch::advance();
        if let Ok(n) = arenas::narenas::read() {
            for i in 0..n {
                let name = format!("arena.{}.purge\0", i);
                // write(name) triggers immediate purge of arena i.
                let _ = unsafe { tikv_jemalloc_ctl::raw::write(name.as_bytes(), ()) };
            }
        }
    }
    #[cfg(not(all(feature = "jemalloc", not(target_env = "msvc"))))]
    {
        // System allocator: no manual purge available.
    }
}

/// Debug-only logging macro
/// Only prints in debug builds, silenced in release builds
#[macro_export]
macro_rules! debug_log {
    ($($arg:tt)*) => {
        #[cfg(debug_assertions)]
        {
            println!($($arg)*);
        }
        // In release builds this is a true no-op. The arguments are still
        // type-checked because the debug branch references them, but they are
        // not evaluated at runtime in release.
        #[cfg(not(debug_assertions))]
        {
            // Expand (but don't evaluate) the args so they remain part of the
            // syntax tree; `format_args!` is const-evaluated away to nothing.
            // Wrapped in a block (not `let _ = || ...`) to avoid clippy's
            // non-binding-lock-on-sync false positive.
            if false {
                let _ = format_args!($($arg)*);
            }
        }
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

pub mod catalog;
pub mod config;
pub mod distance;
pub mod index;
pub mod sql;
pub mod storage;
pub mod txn;
pub mod types;

// ⚠️ EXPERIMENTAL: the C ABI in `ffi` is incomplete (no open_with_config,
// no transaction/batch APIs, no error reporting, execute() returns a Debug
// string). There is no C header file and no versioned symbol scheme yet.
// Do not rely on it for production bindings until it stabilizes — it will
// change without a SemVer bump. Tracked as a pre-1.0 limitation.
pub mod cache;
pub mod ffi; // 🚀 P1: Row cache for performance

// 🔄 Modular database module (refactored from database_legacy.rs)
pub mod database;

mod api;
mod error; // 内部 API 包装层

pub use config::{AutoCheckpointConfig, DBConfig, DurabilityLevel, LSMConfig, WALConfig};
pub use error::{MoteDBError, Result, StorageError};

// 主要对外 API (now using modular database)
pub use api::Database; // 简化 API 包装
pub use catalog::TableRegistry;
pub use database::{MoteDB, QueryProfile, TransactionStats};
pub use sql::{ForEachResult, QueryResult, StreamingControl, StreamingQueryResult};

// 🔌 导出分词器插件系统（方便用户直接使用）
pub mod tokenizers {
    pub use crate::index::tokenizers::*;
}
