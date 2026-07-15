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
/// Fsync the parent directory of a file path. This ensures that a file rename
/// or creation is durable across crashes on POSIX systems (Linux ext4/xfs,
/// macOS APFS). Without this, a rename is not guaranteed to survive a crash
/// even if the file itself was fsync'd.
pub fn fsync_dir<P: AsRef<std::path::Path>>(path: P) {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        let parent = path.as_ref().parent();
        if let Some(dir) = parent {
            if let Ok(f) = std::fs::File::open(dir) {
                let _ = unsafe { libc::fsync(f.as_raw_fd()) };
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
}

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

// ── Logging ─────────────────────────────────────────────────────────────
// The crate emits log records via the `log` crate facade (our
// `debug_log!`/`info_log!`/`warn_log!`/`error_log!` macros delegate to
// `log::debug!`/`info!`/`warn!`/`error!`). The library installs NO global
// logger — that is the application's responsibility (e.g. `env_logger::init()`
// or a `tracing` subscriber). With no logger installed, all logging is
// compiled to a no-op (zero runtime cost). With one installed, the app
// controls verbosity, e.g. `RUST_LOG=motedb=info`.
//
// This replaces the prior hand-rolled macros that unconditionally `eprintln!`'d
// to stderr (`warn_log!`) or produced nothing in release builds (`debug_log!`),
// which gave production deployments zero useful observability.

/// Debug-level log. Compiled in but a no-op unless a logger is installed and
/// debug level is enabled for the `motedb` target.
#[macro_export]
macro_rules! debug_log {
    ($($arg:tt)*) => { ::log::debug!($($arg)*) };
}

/// Info-level log (e.g. lifecycle events: open/close/checkpoint).
#[macro_export]
macro_rules! info_log {
    ($($arg:tt)*) => { ::log::info!($($arg)*) };
}

/// Warn-level log (degraded-but-functional conditions, retries, fallbacks).
#[macro_export]
macro_rules! warn_log {
    ($($arg:tt)*) => { ::log::warn!($($arg)*) };
}

/// Error-level log (operation failed; the calling path returns an error too).
#[macro_export]
macro_rules! error_log {
    ($($arg:tt)*) => { ::log::error!($($arg)*) };
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
