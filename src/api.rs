//! MoteDB Public API
//!
//! 面向嵌入式具身智能的高性能多模态数据库API
//!
//! # 核心特性
//! - **SQL 引擎**: 完整 SQL 支持，包含子查询、聚合、JOIN、索引管理
//! - **多模态索引**: 向量(VECTOR) / 空间(SPATIAL) / 文本(TEXT) / 时间序列(TIMESTAMP) / 列索引(COLUMN)
//! - **事务支持**: MVCC 事务 + Savepoint
//! - **批量操作**: 高性能批量插入和索引构建
//! - **性能监控**: 统计信息和性能分析

use crate::database::indexes::VectorIndexStats;
use crate::database::{MoteDB, TransactionStats};
use crate::sql::ast::Statement;
use crate::sql::StreamingQueryResult;
use crate::types::{Row, RowId, SqlRow, Value};
use crate::StorageError;
use crate::{DBConfig, Result};
use std::path::Path;
use std::sync::Arc;

/// Outcome of fast-PK value → row_id resolution (shared by the single-
/// statement fast path and the executemany batch kernel).
enum FastPkRowId {
    /// Deterministic or cache-resolved row_id.
    Resolved(RowId),
    /// Definitively no such row (e.g. negative/odd value on an AUTO_INCREMENT
    /// PK) — the statement matches 0 rows.
    Absent,
    /// PK cache miss — not proof of absence; the full executor must run.
    Defer,
}

/// 🔑 J1: one fused-retrieval hit — RRF score plus the per-engine scores
/// when that list contained the document.
#[derive(Debug, Clone)]
pub struct HybridHit {
    pub row_id: RowId,
    /// Fused Reciprocal Rank Fusion score (higher = better).
    pub rrf: f32,
    /// BM25 score from the text list (None = not in the text top-N).
    pub bm25: Option<f32>,
    /// Vector distance from the KNN list (None = not in the vector top-N).
    pub distance: Option<f32>,
}

/// Pre-computed metadata for fast PK SELECT execution.
struct FastPkMeta {
    /// "select", "update", or "delete"
    stmt_type: &'static str,
    table_name: String,
    /// Pre-stored table registry id (C3): composite-key building used to
    /// hit the table registry on every point read.
    table_id: u64,
    param_idx: usize,
    /// Only for SELECT: whether it's SELECT *
    is_star: bool,
    /// Only for SELECT: column positions to project
    select_col_positions: Vec<usize>,
    /// 🔑 Only for SELECT (non-star): OUTPUT column names, one per projected
    /// position. The fast path used to return the full table's column_names
    /// while emitting only the projected VALUES — `SELECT v FROM t WHERE
    /// id = ?` came back as columns=['id','v','s'] with data=[v_value],
    /// silently mislabeling every partial projection (BUG #46, found by an
    /// external production review). Must stay 1:1 with select_col_positions.
    select_col_names: Vec<String>,
    /// Only for UPDATE: (col_position, param_idx) for SET col = ?
    set_param_positions: Vec<(usize, usize)>,
    /// Only for UPDATE: (col_position, literal) for SET col = <literal>.
    /// 🔑 字面量赋值必须单独保存 —— fast 路径曾只应用 Parameter 赋值，
    /// `UPDATE t SET v = 0 WHERE id = ?` 的 SET 被静默丢弃，克隆旧行
    /// 原样写回还报 affected=1（BUG #45）。
    set_literal_positions: Vec<(usize, crate::types::Value)>,
    is_auto_increment: bool,
    column_names: Arc<Vec<String>>,
    schema: Arc<crate::types::TableSchema>,
}

/// Cached statement entry — statement + optional fast-PK metadata
struct CachedStmt {
    stmt: Arc<Statement>,
    /// Pre-computed fast PK path metadata (set on first call if pattern matches)
    fast_pk: Option<FastPkMeta>,
}

/// MoteDB 数据库实例
///
/// # 快速开始
///
/// ```ignore
/// use motedb::Database;
///
/// // 打开数据库
/// let db = Database::open("data.mote")?;
///
/// // SQL 操作
/// db.execute("CREATE TABLE users (id INT, name TEXT, email TEXT)")?;
/// db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')")?;
/// let results = db.query("SELECT * FROM users WHERE id = 1")?;
///
/// // 多模态索引
/// db.execute("CREATE INDEX users_email ON users(email)")?;  // 列索引
/// db.execute("CREATE VECTOR INDEX docs_vec ON docs(embedding)")?;  // 向量索引
/// ```ignore///
/// # 核心功能
///
/// ## 1. SQL 操作
/// - `query()` / `execute()`: 执行 SQL 语句
///
/// ## 2. 事务管理
/// - `begin_transaction()`: 开始事务
/// - `commit_transaction()`: 提交事务
/// - `rollback_transaction()`: 回滚事务
/// - `savepoint()`: 创建保存点
///
/// ## 3. 批量操作
/// - `batch_insert()`: 批量插入行
/// - `batch_insert_with_vectors()`: 批量插入向量数据
///
/// ## 4. 索引管理
/// - `create_column_index()`: 创建列索引（快速等值/范围查询）
/// - `create_vector_index()`: 创建向量索引（KNN搜索）
/// - `create_text_index()`: 创建全文索引（BM25搜索）
/// - `create_ioctree_index()`: 创建i-Octree 3D空间索引
///
/// ## 5. 查询API
/// - `query_by_column()`: 按列值查询（使用索引）
/// - `vector_search()`: 向量KNN搜索
/// - `text_search()`: 全文搜索（BM25）
/// - `query_timestamp_range()`: 时间序列查询
///
/// ## 6. 统计信息
/// - `stats()`: 数据库统计信息
/// - `vector_index_stats()`: 向量索引统计
/// - `transaction_stats()`: 事务统计
///
/// ## 7. 持久化
/// - `flush()`: 刷新数据到磁盘
/// - `checkpoint()`: 创建检查点
/// - `close()`: 关闭数据库
pub struct Database {
    inner: Arc<MoteDB>,
    /// 🚀 Prepared statement cache: SQL string → CachedStmt
    /// Uses RwLock for concurrent reads + Arc<Statement> for O(1) clone on cache hit
    /// 🚀 DashMap（分片锁）语句缓存：单把 RwLock 曾把并发 execute 的
    /// 读取/写入全部串行化（miss 时 Lexer+Parser 还在写锁内）。分片锁
    /// + 解析移出锁外后并发解析近线性。越界（2×cap）整体收缩。
    stmt_cache: Arc<dashmap::DashMap<String, CachedStmt>>,
    stmt_cache_cap: usize,
    /// Reused QueryExecutor — avoids per-call allocation of pattern_cache, optimizer state
    query_executor: crate::sql::QueryExecutor,
    /// Registry key of this connection (shared-handle bookkeeping). Empty
    /// for engines that bypassed the registry (direct MoteDB construction).
    reg_key: Option<std::path::PathBuf>,
}

/// 🔑 In-process shared-handle registry: canonical db dir ->
/// (engine Weak, live-connection counter). flock() locks are per
/// open-file-description, so a second open() in the same process used to
/// fail with "already open by another process"; multi-connection
/// in-process use (threads, agents) attaches to the SAME engine instead.
fn connection_registry() -> &'static std::sync::Mutex<
    std::collections::HashMap<
        std::path::PathBuf,
        (std::sync::Weak<MoteDB>, std::sync::atomic::AtomicUsize),
    >,
> {
    static REGISTRY: std::sync::OnceLock<
        std::sync::Mutex<
            std::collections::HashMap<
                std::path::PathBuf,
                (std::sync::Weak<MoteDB>, std::sync::atomic::AtomicUsize),
            >,
        >,
    > = std::sync::OnceLock::new();
    REGISTRY.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// Attach-or-open: returns (engine, registry key).
fn open_engine_shared(
    db_path: std::path::PathBuf,
    open: impl FnOnce(&std::path::Path) -> Result<MoteDB>,
) -> Result<(Arc<MoteDB>, std::path::PathBuf)> {
    // 🔑 Key stability across create→open: at create() time the target
    // directory does not exist yet, so canonicalize fails and would leave
    // an un-normalized key — the later open() canonicalizes successfully
    // (incl. symlink resolution like /tmp → /private/tmp) and MISSES the
    // registry, hitting the flock. Normalize the PARENT (which exists) and
    // re-join the leaf instead.
    let key = std::fs::canonicalize(&db_path).unwrap_or_else(|_| {
        match (db_path.parent(), db_path.file_name()) {
            (Some(parent), Some(name)) if !parent.as_os_str().is_empty() => {
                std::fs::canonicalize(parent)
                    .unwrap_or_else(|_| parent.to_path_buf())
                    .join(name)
            }
            _ => db_path.clone(),
        }
    });
    let mut reg = connection_registry().lock().unwrap();
    if let Some((weak, live)) = reg.get(&key) {
        if let Some(arc) = weak.upgrade() {
            live.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            return Ok((arc, key));
        }
    }
    reg.retain(|_, (w, _)| w.upgrade().is_some());
    let engine = Arc::new(open(&db_path)?);
    reg.insert(
        key.clone(),
        (
            Arc::downgrade(&engine),
            std::sync::atomic::AtomicUsize::new(1),
        ),
    );
    Ok((engine, key))
}

impl Database {
    // ============================================================================
    // 1. 数据库生命周期管理
    // ============================================================================

    /// 创建新数据库
    ///
    /// Path resolution: `foo.mote` is used verbatim; an existing directory
    /// (e.g. a TempDir) holds the database INSIDE itself; anything else
    /// creates the legacy sibling `{stem}.mote` directory.
    ///
    /// # Examples
    /// ```ignore
    /// let db = Database::create("data.mote")?;
    /// ```
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db_path = MoteDB::resolve_create_path_public(path.as_ref());
        let (inner, reg_key) = open_engine_shared(db_path, |p| MoteDB::create(p))?;
        let query_executor = crate::sql::QueryExecutor::new(inner.clone());
        Ok(Self {
            inner,
            stmt_cache: Arc::new(dashmap::DashMap::new()),
            stmt_cache_cap: 256,
            query_executor,
            reg_key: Some(reg_key),
        })
    }

    /// 使用自定义配置创建数据库
    ///
    /// # Examples
    /// ```ignore
    /// use motedb::DBConfig;
    ///
    /// let config = DBConfig {
    ///     memtable_size_mb: 16,
    ///     ..Default::default()
    /// };
    /// let db = Database::create_with_config("data.mote", config)?;
    /// ```
    pub fn create_with_config<P: AsRef<Path>>(path: P, config: DBConfig) -> Result<Self> {
        let db_path = MoteDB::resolve_create_path_public(path.as_ref());
        let (inner, reg_key) =
            open_engine_shared(db_path, |p| MoteDB::create_with_config(p, config.clone()))?;
        let query_executor = crate::sql::QueryExecutor::new(inner.clone());
        Ok(Self {
            inner,
            stmt_cache: Arc::new(dashmap::DashMap::new()),
            stmt_cache_cap: 256,
            query_executor,
            reg_key: Some(reg_key),
        })
    }

    /// 打开已存在的数据库
    ///
    /// # Examples
    /// ```ignore
    /// let db = Database::open("data.mote")?;
    /// ```
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        // 🔑 Shared handle: in-process reopens of the same directory attach
        // to the SAME engine (flock is per-file-description — a second
        // plain open used to fail with "already open by another process").
        let db_path = MoteDB::resolve_open_path_public(path.as_ref());
        let (inner, reg_key) = open_engine_shared(db_path, |p| MoteDB::open(p))?;
        let query_executor = crate::sql::QueryExecutor::new(inner.clone());
        Ok(Self {
            inner,
            stmt_cache: Arc::new(dashmap::DashMap::new()),
            stmt_cache_cap: 256,
            query_executor,
            reg_key: Some(reg_key),
        })
    }

    /// Open an existing database with custom configuration
    ///
    /// Use this to apply edge-optimized settings when reopening:
    /// ```ignore
    /// let config = DBConfig::for_edge();
    /// let db = Database::open_with_config("data.mote", config)?;
    /// ```
    pub fn open_with_config<P: AsRef<Path>>(path: P, config: DBConfig) -> Result<Self> {
        let db_path = MoteDB::resolve_open_path_public(path.as_ref());
        let (inner, reg_key) =
            open_engine_shared(db_path, |p| MoteDB::open_with_config(p, config.clone()))?;
        let query_executor = crate::sql::QueryExecutor::new(inner.clone());
        Ok(Self {
            inner,
            stmt_cache: Arc::new(dashmap::DashMap::new()),
            stmt_cache_cap: 256,
            query_executor,
            reg_key: Some(reg_key),
        })
    }

    /// 刷新所有数据到磁盘
    ///
    /// # Examples
    /// ```ignore
    /// db.execute("INSERT INTO users VALUES (1, 'Alice', 25)")?;
    /// db.flush()?; // 确保数据持久化
    /// ```
    pub fn flush(&self) -> Result<()> {
        self.inner.flush()
    }

    /// Wait until all pending index build batches have been processed.
    ///
    /// Call after `flush()` to ensure indexes are fully built before querying.
    /// Returns `true` if all batches completed, `false` on timeout.
    pub fn wait_for_indexes_ready(&self) -> bool {
        self.inner.wait_for_indexes_ready()
    }

    /// Access the columnar segment store (for TimeSeries tables).
    pub fn columnar_store(&self) -> &crate::storage::ColumnarStore {
        &self.inner.columnar_store
    }

    /// Checkpoint: flush data + persist indexes + truncate WAL
    ///
    /// Stronger durability guarantee than flush() alone.
    /// Use before closing to ensure full recoverability.
    /// Diagnostic passthrough: resident heap bytes of internal structures.
    #[doc(hidden)]
    pub fn debug_memory_report(&self) -> String {
        self.inner.debug_memory_report()
    }

    /// Operational self-check: table layout, memory budgets, index coverage,
    /// build errors and disk breakdown as a structured PASS/WARN report.
    /// Read-only; safe on a live database. See `motedb-cli doctor <path>`.
    pub fn doctor(&self) -> crate::database::doctor::DoctorReport {
        self.inner.doctor()
    }

    pub fn checkpoint(&self) -> Result<()> {
        self.inner.checkpoint()
    }

    /// Online backup: copy a consistent point-in-time snapshot of the whole
    /// database to `dest` (a new directory) while the database stays open.
    ///
    /// Every transaction committed before the call is present in the
    /// snapshot. Concurrent autocommit writes pause for the duration of the
    /// copy; in-flight explicit transactions are captured at their last
    /// durable state. The copy is fsync'd file-by-file, so it is durable the
    /// moment the call returns.
    ///
    /// Restore is simply opening the copy:
    /// ```ignore
    /// db.backup_to("/mnt/usb/robot_backup")?;
    /// // ... later, possibly on another device ...
    /// let db = Database::open("/mnt/usb/robot_backup")?;
    /// ```
    ///
    /// `dest` must not already exist. Errors if a flush/checkpoint is in
    /// progress (retry in that case).
    pub fn backup_to<P: AsRef<std::path::Path>>(&self, dest: P) -> Result<()> {
        self.inner.backup_to(dest.as_ref())
    }

    /// Full checkpoint with index rebuild (slower but thorough).
    /// Used internally on shutdown to ensure index completeness.
    pub fn checkpoint_full(&self) -> Result<()> {
        self.inner.checkpoint_full()
    }

    /// VACUUM: reclaim space by forcing compaction and dropping tombstones.
    ///
    /// This runs a full compaction cycle across all LSM levels, dropping
    /// tombstone entries and reclaiming disk space. Also flushes column
    /// indexes to disk and ensures they are consistent.
    ///
    /// # Cost
    /// - Blocks writes during compaction (may take seconds to minutes).
    /// - Rewrites all SSTables.
    ///
    /// # When to use
    /// - After bulk DELETE operations
    /// - Before taking a backup
    /// - Periodically in long-running deployments (e.g., weekly)
    pub fn vacuum(&self) -> Result<()> {
        self.inner.vacuum()
    }

    /// 关闭数据库（显式调用，通常由 Drop 自动处理）
    ///
    /// Sets the closed flag so all subsequent operations return `DatabaseClosed` error.
    /// Idempotent: safe to call multiple times.
    ///
    /// # Examples
    /// ```ignore
    /// db.close()?;
    /// // All subsequent operations will return an error
    /// ```
    pub fn close(&self) -> Result<()> {
        if self
            .inner
            .is_closed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Ok(());
        }

        // 🔑 Shared-handle etiquette: other connections may still hold this
        // engine (the executor's own Arc makes strong_count unreliable).
        // The registry's live-connection counter is authoritative: the LAST
        // connection performs the real shutdown; earlier closes detach.
        if let Some(key) = &self.reg_key {
            let last = {
                let reg = connection_registry().lock().unwrap();
                match reg.get(key) {
                    Some((weak, live)) => {
                        let remains = live.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                        if remains <= 1 {
                            // detach the registry entry so a later open
                            // starts a fresh engine after shutdown
                            let _ = weak;
                            true
                        } else {
                            false
                        }
                    }
                    None => true,
                }
            };
            if last {
                let mut reg = connection_registry().lock().unwrap();
                reg.remove(key);
            } else {
                return Ok(());
            }
        }

        // 🔑 Rollback any active transaction before closing. Without this,
        // uncommitted UPDATEs (which write directly to storage + record an
        // undo delta) persist on reopen — a crash recovery / data integrity
        // bug. The undo delta is replayed to restore the original values.
        let active_txn = self.query_executor.current_txn_id();
        if let Some(txn_id) = active_txn {
            warn_log!(
                "[close] Active transaction {} not committed — rolling back",
                txn_id
            );
            self.query_executor.replay_undo_log(txn_id);
            self.query_executor.clear_txn_context();
        }

        // Signal background threads to stop
        self.inner.signal_background_threads_stop();

        // Wait for threads to actually finish before checkpoint to prevent
        // deadlock (threads may hold locks that checkpoint needs).
        if !self
            .inner
            .wait_for_background_threads_stop(std::time::Duration::from_secs(5))
        {
            warn_log!("[close] Background threads did not stop within 5s — joining");
            // 🚀 强制等待线程退出。index-builder 持有 lsm_engine 的 Arc clone，
            // 不等它退出就继续 close，lsm_engine Arc 不归零 → LSM 后台线程泄漏
            // → 累积导致后续测试死锁。给它最多 10s 自然退出。
            self.inner
                .join_background_threads(std::time::Duration::from_secs(10));
        }

        // 🔑 仅在有 pending index batch 时才等（避免无索引的 close 付代价）。
        // 注意：不能无条件等 —— 大量 lib 测试创建/销毁 Database，每个 close 都等
        // 会让 --lib 套件慢几百秒（v0.7.7 的 CI 回归根因）。只在确有 pending 时
        // 短暂等（2s），让 index-builder 的 drain 处理完。
        if self.inner.has_pending_index_batches() {
            self.inner
                .wait_for_indexes_ready_timeout(std::time::Duration::from_secs(2));
        }

        // 🚀 Flush ColSegmentStore buffers BEFORE checkpoint. Without this,
        // in-memory INSERT data (the write buffer) is lost on close — the
        // large_batch_durability bug (10000 rows → 5000 after reopen). The
        // second batch was in the buffer, never flushed, dropped on close.
        // 🔒 Snapshot Arcs first — iter() holds shard read locks while the loop
        // runs flush/compaction I/O; that stalls writers on this map (the
        // intermittent-hang signature).
        let stores: Vec<Arc<crate::storage::col_segment::ColSegmentStore>> = self
            .inner
            .col_segment_stores
            .iter()
            .map(|e| Arc::clone(e.value()))
            .collect();
        for store in stores {
            let _ = store.flush_buffer();
            // Compact to a single segment so the reopen sees all data in one place.
            while store.segment_count() >= 2 {
                if store.force_compact_all().is_err() {
                    break;
                }
            }
        }

        // 🚀 close 用 checkpoint_impl(false)（不 rebuild 索引）替代 checkpoint_full。
        // checkpoint_full 的 rebuild_timestamp_index + flush_all_indexes 会和
        // index-builder 竞争锁，累积效应下卡死。close 只需持久化数据（WAL/列存），
        // 索引可重建（重启时 lazy load）。
        //
        // 🔑 The background threads (including the index-builder) are stopped
        // and pending batches drained above, so it is now SAFE to flush
        // indexes. Clear the pipeline flag so flush_all_indexes inside
        // checkpoint_impl actually runs — it early-returns while the flag is
        // set, which silently skipped EVERY index flush at close (vector
        // graphs, text postings and column-index buffers stayed in memory;
        // the flag was one-shot from open() and never cleared here).
        self.inner.mark_index_pipeline_stopped();
        let result = self.inner.checkpoint();
        self.inner
            .is_closed
            .store(true, std::sync::atomic::Ordering::Release);
        // 🔒 v0.12.9: stamp the FTS freshness marker while we still hold the
        // exclusive lock (release_lock is below). Stamping here — not only in
        // MoteDB::Drop — makes the marker prompt for explicit close() users
        // (the Python binding's primary path; Drop may wait for GC), and the
        // lock ordering guarantees no NEW instance can have opened yet, so
        // the marker can never attest freshness for someone else's session.
        if result.is_ok() {
            self.inner.stamp_fts_freshness_marker("[close]");
        }
        // Release the exclusive flock so a subsequent open() on the same
        // directory (or another process) can acquire it. Without this, the
        // lock is held until the MoteDB is dropped — which may be much later
        // if the caller keeps the handle alive after close().
        self.inner.release_lock();
        // Stop WAL background threads + final sync_flush. Without this the old
        // flush thread keeps owning the WAL partition file handles, and a
        // reopen deadlocks on the partition mutex / file lock. (WALManager is
        // held via Arc, so its Drop — which does this — never runs while close
        // leaves the handle alive.)
        self.inner.wal.shutdown();
        result
    }

    // ============================================================================
    // 2. SQL 操作（核心功能）
    // ============================================================================

    /// 🚀 执行 SQL 查询（流式零内存开销）
    ///
    /// 返回流式结果，支持：
    /// 1. 流式遍历（零内存开销）
    /// 2. 物化为 Vec（等同于旧的 execute）
    ///
    /// # Examples
    /// ```ignore
    /// // 方式 1: 流式处理大结果集（推荐）
    /// let result = db.execute("SELECT * FROM users WHERE age > 18")?;
    /// result.for_each(|columns, row| {
    ///     println!("{:?}: {:?}", columns, row);
    ///     Ok(())
    /// })?;
    ///
    /// // 方式 2: 物化为 Vec（兼容旧 API）
    /// let result = db.execute("SELECT * FROM users")?;
    /// let materialized = result.materialize()?;
    /// match materialized {
    ///     QueryResult::Select { columns, rows } => {
    ///         println!("Found {} rows", rows.len());
    ///     }
    ///     _ => {}
    /// }
    ///
    /// // 其他语句（INSERT/UPDATE/DELETE/CREATE/DROP）
    /// db.execute("CREATE TABLE users (id INT, name TEXT, email TEXT)")?;
    /// db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')")?;
    /// db.execute("UPDATE users SET email = 'new@example.com' WHERE id = 1")?;
    /// db.execute("DELETE FROM users WHERE id = 1")?;
    /// db.execute("CREATE INDEX users_email ON users(email)")?;
    /// db.execute("CREATE VECTOR INDEX docs_vec ON docs(embedding)")?;
    /// ```

    /// Returns the configured max_result_rows limit, if any.
    /// Use with `for_each()` or `materialize_with_limit()` for bounded queries.
    pub fn max_result_rows(&self) -> Option<usize> {
        self.inner.max_result_rows
    }

    /// Convenience method: execute a SELECT query and return rows directly.
    /// This is shorthand for `execute(sql)?.materialize()?` + pattern match.
    ///
    /// Returns an empty Vec for non-SELECT statements.
    ///
    /// # Example
    /// ```ignore
    /// let rows = db.query("SELECT * FROM users WHERE age > 18")?;
    /// for row in rows {
    ///     println!("{:?}", row);
    /// }
    /// ```
    pub fn query(&self, sql: &str) -> Result<Vec<Vec<Value>>> {
        match self.execute(sql)?.materialize()? {
            crate::QueryResult::Select { rows, .. } => Ok(rows),
            _ => Ok(vec![]),
        }
    }

    /// Get the approximate row count for a table without executing SQL.
    /// Returns the live row count from the ColSegmentStore if available,
    /// otherwise falls back to the LSM row counter.
    pub fn row_count(&self, table_name: &str) -> Result<usize> {
        Ok(self.inner.fast_row_count(table_name).unwrap_or(0) as usize)
    }

    /// Per-table budget for decoded VECTOR columns (see DBConfig
    /// `vector_cache_budget_mb`; presets cap this for edge devices). Exposed
    /// for memory-constrained deployments to tune at runtime.
    pub fn set_vector_cache_budget(&self, table_name: &str, bytes: usize) -> Result<()> {
        let schema = self.inner.get_table_schema(table_name)?;
        let store = self
            .inner
            .get_or_create_col_segment_store(table_name, schema.col_types())?;
        store.set_vector_cache_budget(bytes);
        Ok(())
    }

    /// Current per-table decoded-VECTOR cache budget in bytes.
    pub fn vector_cache_budget_bytes(&self, table_name: &str) -> Result<usize> {
        let schema = self.inner.get_table_schema(table_name)?;
        let store = self
            .inner
            .get_or_create_col_segment_store(table_name, schema.col_types())?;
        Ok(store.vector_cache_budget())
    }

    pub fn execute(&self, sql: &str) -> Result<StreamingQueryResult> {
        use crate::sql::{Lexer, Parser};

        // 🛡️ Guard: reject all operations after close() (including read paths
        // that bypass the inner executor's own checks).
        if self
            .inner
            .is_closed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(crate::StorageError::InvalidData(
                "Database is closed".into(),
            ));
        }

        // In transaction mode, skip fast INSERT paths so rows go through
        // insert_row_with_txn (buffered in write_set until COMMIT).
        let in_txn = self.query_executor.is_in_transaction();

        // 🔑 PERF: dispatch on the first SQL keyword ONCE instead of running
        // 4 sequential try_fast_* probes (each re-calling trim_start + prefix
        // match). A SELECT previously paid INSERT-check + UPDATE-check +
        // DELETE-check + SELECT-check = 4× trim_start + 4× prefix compare.
        // Now it's 1× trim_start + 1 match → calls only the relevant path.
        let trimmed = sql.trim_start();

        // Handle CHECKPOINT and VACUUM SQL commands (not part of the parser's
        // Statement enum — intercepted here as DB operations).
        // 🔑 PERF: byte-slice case-insensitive check — avoids a per-call
        // to_ascii_uppercase() String allocation (was ~40ns + heap alloc on
        // every execute(), even for SELECTs that never match).
        let starts_checkpoint = trimmed
            .as_bytes()
            .get(..10)
            .map(|b| b.eq_ignore_ascii_case(b"CHECKPOINT"))
            .unwrap_or(false);
        if starts_checkpoint {
            self.inner.checkpoint()?;
            return Ok(StreamingQueryResult::Modification { affected_rows: 0 });
        }
        let starts_vacuum = trimmed
            .as_bytes()
            .get(..6)
            .map(|b| b.eq_ignore_ascii_case(b"VACUUM"))
            .unwrap_or(false);
        if starts_vacuum {
            self.inner.vacuum()?;
            return Ok(StreamingQueryResult::Modification { affected_rows: 0 });
        }

        // 🔑 Serialize autocommit writes (INSERT/UPDATE/DELETE) to prevent
        // lost-update races (concurrent v=v+1 reading same old value).
        // Explicit transactions are NOT serialized (they use MVCC isolation).
        let is_autocommit_write = !in_txn
            && matches!(
                trimmed.as_bytes().get(0..6),
                Some(b"INSERT")
                    | Some(b"insert")
                    | Some(b"UPDATE")
                    | Some(b"update")
                    | Some(b"DELETE")
                    | Some(b"delete")
            );
        let _write_guard = if is_autocommit_write {
            // 🚀 条带化：同表串行（RMW/PK 语义不变），跨表并行 —— 全局锁
            // 曾把所有并发 autocommit 写完全串行化，GroupCommit 的攒批
            // 永远无法形成（实测 4 线程并发写入零收益）。
            Some(self.inner.lock_autocommit_write(trimmed))
        } else {
            None
        };

        if let Some(kw) = trimmed.as_bytes().get(0..6) {
            match kw {
                b"INSERT" | b"insert" if !in_txn => {
                    if let Some(r) = self.try_fast_insert(sql)? {
                        return Ok(r);
                    }
                }
                b"UPDATE" | b"update" if !in_txn => {
                    if let Some(r) = self.try_fast_update(sql)? {
                        return Ok(r);
                    }
                }
                b"DELETE" | b"delete" if !in_txn => {
                    if let Some(r) = self.try_fast_delete(sql)? {
                        return Ok(r);
                    }
                }
                b"SELECT" | b"select" => {
                    if let Some(r) = self.try_fast_select(sql)? {
                        return Ok(r);
                    }
                }
                _ => {}
            }
        }

        // 🚀 Prepared statement cache: skip re-parsing on repeated queries.
        // 🔑 分片读（DashMap）+ 解析在锁外：旧路径 miss 时在写锁内跑
        // Lexer+Parser，全部线程串行排队；现在各线程并行解析，仅在
        // 插入瞬间占用各自分片。
        let statement: Arc<Statement> = {
            if let Some(cached) = self.stmt_cache.get(sql) {
                Arc::clone(&cached.stmt)
            } else {
                let mut lexer = Lexer::new(sql);
                let tokens = lexer.tokenize()?;
                let mut parser = Parser::new(tokens);
                let stmt = parser.parse()?;
                let stmt_arc = Arc::new(stmt);
                self.insert_stmt_cached(sql.to_string(), &stmt_arc);
                stmt_arc
            }
        };

        // Reuse shared QueryExecutor (preserves pattern_cache + optimizer state)
        self.query_executor.reset_last_insert_id();
        self.query_executor.execute_streaming_ref(&statement)
    }

    /// Execute one INSERT statement once per parameter set (executemany).
    ///
    /// The SQL must be a single `INSERT ... VALUES (?, ...)` whose VALUES
    /// holds exactly one row template of literals/parameters. Every set in
    /// `batch` is substituted into that template, and all N rows are executed
    /// as ONE multi-row INSERT — the engine's batch path takes a single WAL
    /// fsync and batched index updates for the whole batch, which is an order
    /// of magnitude faster than N separate `execute_prepared` calls.
    ///
    /// Returns the total number of affected rows.
    /// 🔥 列式批量插入 (numpy 数组直通): 跳过 SQL 解析与逐行 Python 对象,
    /// 一次 WAL 批 + 单次锁获取。绑定层把 numpy 缓冲区组装成行后走这里。
    pub fn insert_rows(&self, table: &str, rows: Vec<Vec<Value>>) -> Result<u64> {
        let ids = self.inner.batch_insert_rows_to_table(table, rows)?;
        Ok(ids.len() as u64)
    }

    /// 表的列名列表（schema 序）。绑定层 insert_arrays 按位放置列值用 —
    /// 字典序 ≠ schema 序曾导致错位损毁 (值静默落错列)。
    pub fn table_columns(&self, table: &str) -> Result<Vec<String>> {
        let schema = self.inner.table_registry.get_table(table)?;
        Ok(schema
            .column_names_cache
            .as_ref()
            .map(|c| (**c).clone())
            .unwrap_or_else(|| schema.columns.iter().map(|c| c.name.clone()).collect()))
    }

    pub fn execute_prepared_many(&self, sql: &str, batch: Vec<Vec<Value>>) -> Result<u64> {
        if self
            .inner
            .is_closed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(crate::StorageError::InvalidData(
                "Database is closed".into(),
            ));
        }
        if batch.is_empty() {
            return Ok(0);
        }

        // Parse once through the shared statement cache (same as execute()).
        let statement: Arc<Statement> = {
            if let Some(cached) = self.stmt_cache.get(sql) {
                Arc::clone(&cached.stmt)
            } else {
                use crate::sql::{Lexer, Parser};
                let mut lexer = Lexer::new(sql);
                let tokens = lexer.tokenize()?;
                let mut parser = Parser::new(tokens);
                let stmt_arc = Arc::new(parser.parse()?);
                self.insert_stmt_cached(sql.to_string(), &stmt_arc);
                stmt_arc
            }
        };

        // 🔑 UPDATE / DELETE batches: wrap the whole batch in ONE transaction
        // and replay execute_prepared per row. Single parse, single commit
        // (one WAL group barrier + one durability fsync for the entire
        // batch), and each statement rides the PK fast path (txn-safe since
        // the in-txn DELETE/PK fix). The old API rejected these outright —
        // Python users had to fall back to per-row execute(), paying parse
        // + fsync per row.
        //
        // 🔒 SQLite semantics: when the caller is ALREADY inside an explicit
        // transaction, the batch must JOIN it (the outer COMMIT/ROLLBACK
        // covers every row) instead of opening a private one the outer
        // rollback cannot undo.
        if matches!(
            statement.as_ref(),
            Statement::Update(_) | Statement::Delete(_)
        ) {
            let outer_txn = self.query_executor.is_in_transaction();
            let tx_id = if outer_txn {
                None
            } else {
                Some(self.begin_transaction()?)
            };
            let tid = if outer_txn {
                self.query_executor
                    .current_txn_id()
                    .expect("outer_txn implies an active txn id")
            } else {
                tx_id.expect("private txn just begun")
            };
            let mut affected: u64 = 0;
            let result = (|| -> Result<()> {
                // 🚀 W1: buffered batch kernel — statement machinery (bind/
                // dispatch/result) once per BATCH; per row only pk→row_id +
                // row read + pending-record. Exotic per-row states fall back
                // to the executor inside the kernel, so semantics cannot
                // drift from the per-row loop below.
                if let Some(lean) =
                    self.executemany_fast_pk_buffered(&statement, sql, &batch, tid)?
                {
                    affected = lean;
                    return Ok(());
                }
                for params in &batch {
                    let r = self.execute_prepared(sql, params.clone())?;
                    if let StreamingQueryResult::Modification { affected_rows } = r {
                        affected += affected_rows as u64;
                    }
                }
                Ok(())
            })();
            match result {
                Ok(()) => {
                    if let Some(tx_id) = tx_id {
                        self.commit_transaction(tx_id)?;
                    }
                    Ok(affected)
                }
                Err(e) => {
                    if let Some(tx_id) = tx_id {
                        let _ = self.rollback_transaction(tx_id);
                    }
                    // Inside an outer transaction the error propagates and the
                    // CALLER decides whether to roll back (SQLite behavior).
                    Err(e)
                }
            }
        } else {
            self.execute_prepared_many_insert(statement, batch)
        }
    }

    /// INSERT-only body of execute_prepared_many (multi-row VALUES rewrite
    /// into one batched statement).
    fn execute_prepared_many_insert(
        &self,
        statement: Arc<Statement>,
        batch: Vec<Vec<Value>>,
    ) -> Result<u64> {
        let insert = match statement.as_ref() {
            Statement::Insert(stmt) => stmt.clone(),
            _ => {
                return Err(crate::error::MoteDBError::InvalidArgument(
                    "execute_prepared_many only supports INSERT/UPDATE/DELETE statements"
                        .to_string(),
                ))
            }
        };

        // The template must be exactly one VALUES row of Parameter/Literal.
        if insert.values.len() != 1 {
            return Err(crate::error::MoteDBError::InvalidArgument(
                "execute_prepared_many requires a single-row VALUES template".to_string(),
            ));
        }
        let template = &insert.values[0];

        let mut rows: Vec<Vec<crate::sql::ast::Expr>> = Vec::with_capacity(batch.len());
        for params in &batch {
            let row: Result<Vec<crate::sql::ast::Expr>> = template
                .iter()
                .map(|e| match e {
                    crate::sql::ast::Expr::Parameter(idx) => {
                        let i = *idx;
                        if i == 0 || i > params.len() {
                            Err(crate::error::MoteDBError::InvalidArgument(format!(
                                "Parameter ?{} out of range ({} provided)",
                                i,
                                params.len()
                            )))
                        } else {
                            Ok(crate::sql::ast::Expr::Literal(params[i - 1].clone()))
                        }
                    }
                    crate::sql::ast::Expr::Literal(v) => {
                        Ok(crate::sql::ast::Expr::Literal(v.clone()))
                    }
                    other => Err(crate::error::MoteDBError::InvalidArgument(format!(
                        "execute_prepared_many VALUES must be literals or parameters, got {:?}",
                        other
                    ))),
                })
                .collect();
            rows.push(row?);
        }

        let batched = crate::sql::ast::InsertStmt {
            table: insert.table.clone(),
            columns: insert.columns.clone(),
            values: rows,
            select: None,
            on_conflict: insert.on_conflict.clone(),
        };
        self.query_executor.reset_last_insert_id();
        let result = self
            .query_executor
            .execute_streaming_ref(&Statement::Insert(batched));
        match result? {
            StreamingQueryResult::Modification { affected_rows } => Ok(affected_rows as u64),
            _ => Ok(0),
        }
    }

    /// Execute a parameterized query.
    ///
    /// The SQL string is parsed once and cached (by the same LRU statement cache
    /// as `execute()`). On subsequent calls with the same SQL text, the cached
    /// AST is reused — only the bind values change. This eliminates the
    /// Lexer → Parser overhead for repeated queries.
    ///
    /// Use `?` for positional parameters:
    /// ```ignore
    /// // First call: parses + caches
    /// let result = db.execute_prepared("SELECT * FROM users WHERE id = ?", vec![Value::Integer(42)])?;
    /// // Second call: cache hit, skips parser
    /// let result = db.execute_prepared("SELECT * FROM users WHERE id = ?", vec![Value::Integer(99)])?;
    /// ```
    pub fn execute_prepared(&self, sql: &str, params: Vec<Value>) -> Result<StreamingQueryResult> {
        use crate::sql::{Lexer, Parser};

        // 🛡️ Guard: reject all operations after close() (execute() has this
        // check; execute_prepared was missing it — writes could silently
        // proceed against a closed database).
        if self
            .inner
            .is_closed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(crate::StorageError::InvalidData(
                "Database is closed".into(),
            ));
        }

        // Get or parse the statement — check for cached fast PK metadata.
        // 🔑 同 execute()：分片读 + 解析在锁外。
        let (statement, cached_fast_pk): (Arc<Statement>, bool) = {
            if let Some(cached) = self.stmt_cache.get(sql) {
                // 🚀 Fast path: use pre-computed PK metadata
                if let Some(ref meta) = cached.fast_pk {
                    if let Some(result) = self.execute_fast_pk_with_meta(meta, &params)? {
                        return Ok(result);
                    }
                    // PK cache miss (e.g. after recovery) — fall through to full path.
                    (Arc::clone(&cached.stmt), false)
                } else {
                    (Arc::clone(&cached.stmt), false)
                }
            } else {
                let mut lexer = Lexer::new(sql);
                let tokens = lexer.tokenize()?;
                let mut parser = Parser::new(tokens);
                let stmt = parser.parse()?;
                let stmt_arc = Arc::new(stmt);
                self.insert_stmt_cached(sql.to_string(), &stmt_arc);
                (stmt_arc, true)
            }
        };

        // 🚀 First call (no fast_pk yet): detect pattern, cache metadata, execute immediately
        if cached_fast_pk {
            if let Some(meta) = Self::detect_fast_pk_pattern(&statement, &self.inner)? {
                // Execute using the metadata we just computed (no extra lock)
                if let Some(result) = self.execute_fast_pk_with_meta(&meta, &params)? {
                    // Cache for future calls (write lock only, no read-back)
                    if let Some(mut cached) = self.stmt_cache.get_mut(sql) {
                        cached.fast_pk = Some(meta);
                    }
                    return Ok(result);
                }
                // PK cache miss — fall through to full path without caching meta.
            }
        }

        // Fall through: not a fast PK pattern or first call — use full path
        self.query_executor.reset_last_insert_id();

        // Validate parameter count
        if !params.is_empty()
            || matches!(
                statement.as_ref(),
                Statement::Select { stmt: s, .. } if s.where_clause.is_some()
            )
        {
            let max_idx = crate::sql::QueryExecutor::max_parameter_index(&statement);
            if max_idx > 0 && params.len() < max_idx {
                return Err(crate::error::MoteDBError::InvalidArgument(format!(
                    "Query has {} parameter(s) but only {} were provided",
                    max_idx,
                    params.len()
                )));
            }
        }

        self.query_executor.bind_params(params);
        let result = self.query_executor.execute_streaming_ref(&statement);
        self.query_executor.clear_params();
        result
    }

    /// Insert into the statement cache with a coarse bound: beyond 2×cap
    /// the map is cleared wholesale (statement working sets are small; a
    /// clear triggers a bounded re-parse burst, sub-ms for ≤512 entries).
    fn insert_stmt_cached(&self, sql: String, stmt: &Arc<Statement>) {
        self.stmt_cache.insert(
            sql,
            CachedStmt {
                stmt: Arc::clone(stmt),
                fast_pk: None,
            },
        );
        if self.stmt_cache.len() >= self.stmt_cache_cap * 2 {
            self.stmt_cache.clear();
        }
    }

    /// Detect if a statement is a simple PK SELECT pattern.
    /// Returns pre-computed FastPkMeta if it matches.
    fn detect_fast_pk_pattern(statement: &Statement, db: &MoteDB) -> Result<Option<FastPkMeta>> {
        use crate::sql::ast::{BinaryOperator, Expr, SelectColumn, Statement as S, TableRef};

        let (stmt_type, table_ref, where_expr, select_cols) = match statement {
            S::Select { stmt: s, .. } => (
                "select",
                s.from.as_ref().and_then(|t| match t {
                    TableRef::Table { name, .. } => Some(name.as_str()),
                    _ => None,
                }),
                s.where_clause.as_ref(),
                Some(s.columns.as_slice()),
            ),
            S::Update(s) => (
                "update",
                Some(s.table.as_str()),
                s.where_clause.as_ref(),
                None,
            ),
            S::Delete(s) => (
                "delete",
                Some(s.table.as_str()),
                s.where_clause.as_ref(),
                None,
            ),
            _ => return Ok(None),
        };

        let table_ref = match table_ref {
            Some(name) => name,
            None => return Ok(None),
        };

        let (col_name, param_idx) = match where_expr {
            Some(Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            }) => match (left.as_ref(), right.as_ref()) {
                (Expr::Column(c), Expr::Parameter(idx)) => (c.as_str(), *idx),
                (Expr::Parameter(idx), Expr::Column(c)) => (c.as_str(), *idx),
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };

        let schema = match db.table_registry.get_table(table_ref) {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };

        let is_pk = schema
            .primary_key()
            .map(|pk| {
                let pk_bare = pk.rsplit('.').next().unwrap_or(pk);
                pk_bare == col_name || pk == col_name
            })
            .unwrap_or(false);

        // 🔑 Only optimize PK lookups. Non-PK WHERE clauses (e.g. WHERE name = ?)
        // must NOT use the fast PK path — it would try to resolve the column as
        // a PK and return Modification/empty instead of a proper scan.
        if !is_pk {
            return Ok(None);
        }

        // 🔒 SELECT-shape guard (BUG #46, external review): the fast path
        // returns the point row (projected or whole). It must NOT claim
        // statements whose result shape depends on more than that one row:
        //   - aggregates / GROUP BY / HAVING / LATEST BY re-shape the output;
        //   - OFFSET > 0 (or LIMIT ?/OFFSET ? — value unknown at detect
        //     time) must yield zero rows for a single-row point query;
        //   - LIMIT 0 must yield zero rows.
        // Any of these defers to the full executor.
        if stmt_type == "select" {
            if let S::Select { stmt: s, .. } = statement {
                if s.group_by.is_some()
                    || s.having.is_some()
                    || s.latest_by.is_some()
                    || s.limit == Some(0)
                    || s.offset.is_some_and(|o| o > 0)
                    || s.limit_param.is_some()
                    || s.offset_param.is_some()
                {
                    return Ok(None);
                }
            }
        }

        let is_star = select_cols
            .is_some_and(|cols| cols.len() == 1 && matches!(cols[0], SelectColumn::Star));

        // 🔒 Strict projection validation (BUG #46): every select item must be
        // a bare (optionally qualified / aliased) column that exists in the
        // schema. Anything else — expressions (`v + 1`), aggregates
        // (`COUNT(*)`), literals, a `*` mixed with columns, or an unknown
        // column — must defer to the full executor. The old filter_map
        // silently DROPPED unresolvable items, so `SELECT COUNT(*) ... WHERE
        // id = ?` returned a whole mislabeled row instead of one aggregate.
        let mut select_col_names: Vec<String> = Vec::new();
        let select_col_positions: Vec<usize> = if let Some(cols) = select_cols {
            if is_star {
                vec![]
            } else {
                let mut positions = Vec::with_capacity(cols.len());
                for col_spec in cols {
                    match col_spec {
                        SelectColumn::Column(n) | SelectColumn::ColumnWithAlias(n, _) => {
                            let lookup = if n.contains('.') {
                                n.rsplit('.').next().unwrap_or(n)
                            } else {
                                n
                            };
                            let pos = match schema.get_column_position(lookup) {
                                Some(p) => p,
                                None => return Ok(None), // unknown column → full path errors properly
                            };
                            let out_name = match col_spec {
                                SelectColumn::ColumnWithAlias(_, alias) => alias.as_str(),
                                // Keep the user-written form (incl. qualifier)
                                // — matches build_select_columns naming.
                                _ => n.as_str(),
                            };
                            select_col_names.push(out_name.to_string());
                            positions.push(pos);
                        }
                        // Expr / bare Star mixed with columns → full path
                        // (eval_expr_on_row / star expansion live there).
                        _ => return Ok(None),
                    }
                }
                positions
            }
        } else {
            vec![]
        };

        // For UPDATE: detect SET col = ? patterns AND SET col = <literal>
        let (set_param_positions, set_literal_positions) = if stmt_type == "update" {
            if let S::Update(s) = statement {
                let mut params_out: Vec<(usize, usize)> = Vec::new();
                let mut literals_out: Vec<(usize, crate::types::Value)> = Vec::new();
                for (col_name, expr) in &s.assignments {
                    // 🔒 An assignment the fast path cannot represent as a raw
                    // value (`SET v = v + 1`, `SET v = other_col`, functions,
                    // …) must REJECT the fast path. Writing the old row back
                    // unmodified used to report affected=1 while silently
                    // dropping the SET (BUG #45 was the literal form; this is
                    // the expression form). Unknown columns likewise defer so
                    // the executor errors properly.
                    let pos = match schema.get_column_position(col_name) {
                        Some(p) => p,
                        None => return Ok(None),
                    };
                    match expr {
                        Expr::Parameter(idx) => {
                            params_out.push((pos, *idx));
                        }
                        // 🔑 字面量（含负号折叠 UnaryOp(Minus, Literal)）也要
                        // 应用 —— 否则 fast 路径丢 SET（BUG #45）
                        Expr::Literal(v) => {
                            literals_out.push((pos, v.clone()));
                        }
                        Expr::UnaryOp {
                            op: crate::sql::ast::UnaryOperator::Minus,
                            expr: inner,
                        } => match inner.as_ref() {
                            Expr::Literal(crate::types::Value::Integer(i)) => {
                                literals_out.push((pos, crate::types::Value::Integer(-*i)));
                            }
                            Expr::Literal(crate::types::Value::Float(f)) => {
                                literals_out.push((pos, crate::types::Value::Float(-*f)));
                            }
                            _ => return Ok(None),
                        },
                        _ => return Ok(None),
                    }
                }
                (params_out, literals_out)
            } else {
                (vec![], vec![])
            }
        } else {
            (vec![], vec![])
        };

        let table_id = db.table_registry.get_table_id(table_ref).unwrap_or(0) as u64;
        Ok(Some(FastPkMeta {
            stmt_type,
            table_name: table_ref.to_string(),
            table_id,
            param_idx,
            is_star,
            select_col_positions,
            select_col_names,
            set_param_positions,
            set_literal_positions,
            is_auto_increment: schema.is_primary_key_auto_increment(),
            column_names: schema.column_names_arc(),
            schema,
        }))
    }

    /// Execute a fast PK query (SELECT, UPDATE, DELETE) using pre-computed metadata.
    fn execute_fast_pk_with_meta(
        &self,
        meta: &FastPkMeta,
        params: &[Value],
    ) -> Result<Option<StreamingQueryResult>> {
        // 🔒 M1/M2 guard: the fast PK write paths below apply straight to
        // storage (autocommit semantics). Inside an explicit transaction that
        // breaks buffered-write semantics — ROLLBACK could not revert the
        // change and other connections would see uncommitted rows — so defer
        // to the executor's txn-aware UPDATE/DELETE paths (Ok(None) =
        // fall through to the full executor).
        if matches!(meta.stmt_type, "update" | "delete") && self.query_executor.is_in_transaction()
        {
            return Ok(None);
        }
        let pk_value = match params.get(meta.param_idx - 1) {
            Some(v) => v,
            None => {
                return Err(crate::error::MoteDBError::InvalidArgument(format!(
                    "Parameter ?{} is unbound",
                    meta.param_idx
                )))
            }
        };

        // Resolve PK → row_id (shared with the executemany batch kernel).
        let row_id = match Self::resolve_fast_pk_row_id(self, meta, pk_value) {
            FastPkRowId::Resolved(rid) => rid,
            FastPkRowId::Absent => {
                // 🔑 A PK value that can never match (e.g. `WHERE id = ?`
                // bound to NULL on an AUTO_INCREMENT table) must still answer
                // a SELECT with an EMPTY result set — returning Modification
                // here made Python query() raise "expects a SELECT
                // statement" instead of `[]`.
                if meta.stmt_type == "select" {
                    return Ok(Some(StreamingQueryResult::SelectReady {
                        columns: if meta.is_star {
                            (*meta.column_names).clone()
                        } else {
                            meta.select_col_names.clone()
                        },
                        rows: vec![],
                    }));
                }
                return Ok(Some(StreamingQueryResult::Modification {
                    affected_rows: 0,
                }));
            }
            FastPkRowId::Defer => return Ok(None), // PK cache miss — fall back to full path
        };

        match meta.stmt_type {
            "delete" => {
                let row = match self.inner.get_table_row(&meta.table_name, row_id)? {
                    Some(r) => r,
                    None => {
                        return Ok(Some(StreamingQueryResult::Modification {
                            affected_rows: 0,
                        }))
                    }
                };
                self.inner
                    .delete_row_from_table(&meta.table_name, row_id, row)?;
                Ok(Some(StreamingQueryResult::Modification {
                    affected_rows: 1,
                }))
            }
            "update" => {
                let old_row_arc =
                    match self
                        .inner
                        .get_table_row_arc(&meta.table_name, row_id, &meta.schema)?
                    {
                        Some(r) => r,
                        None => {
                            return Ok(Some(StreamingQueryResult::Modification {
                                affected_rows: 0,
                            }))
                        }
                    };
                let mut new_row = (*old_row_arc).clone();
                // 🔑 SET col = <literal> 必须应用 —— 见 FastPkMeta 注释
                for &(col_pos, ref literal) in &meta.set_literal_positions {
                    while new_row.len() <= col_pos {
                        new_row.push(Value::Null);
                    }
                    new_row[col_pos] = literal.clone();
                }
                for &(col_pos, param_idx) in &meta.set_param_positions {
                    if let Some(new_val) = params.get(param_idx - 1) {
                        while new_row.len() <= col_pos {
                            new_row.push(Value::Null);
                        }
                        new_row[col_pos] = new_val.clone();
                    }
                }
                // Pass &Arc<Row> as &Row — avoids cloning old_row
                self.inner.update_row_with_schema_ref(
                    &meta.table_name,
                    row_id,
                    &old_row_arc,
                    new_row,
                    &meta.schema,
                )?;
                Ok(Some(StreamingQueryResult::Modification {
                    affected_rows: 1,
                }))
            }
            _ => {
                // SELECT. 🔒 M1 read-your-writes: inside a transaction the
                // row may be buffered (write_set INSERT / pending UPDATE) or
                // deleted in-txn — the storage read below must not leak the
                // pre-transaction state.
                if self.query_executor.is_in_transaction() {
                    if let Some(txn_row) = self
                        .query_executor
                        .txn_lookup_row_pub(&meta.table_name, row_id)
                    {
                        // 🔑 BUG #46: non-star output must carry the PROJECTED
                        // column names — full table names mislabeled the row.
                        let out_columns: Vec<String> = if meta.is_star {
                            (*meta.column_names).clone()
                        } else {
                            meta.select_col_names.clone()
                        };
                        let result_vec: Vec<Vec<Value>> = match txn_row {
                            Some(row) => {
                                if meta.is_star {
                                    vec![row]
                                } else {
                                    vec![meta
                                        .select_col_positions
                                        .iter()
                                        .map(|&pos| row.get(pos).cloned().unwrap_or(Value::Null))
                                        .collect()]
                                }
                            }
                            None => vec![],
                        };
                        return Ok(Some(StreamingQueryResult::SelectReady {
                            columns: out_columns,
                            rows: result_vec,
                        }));
                    }
                }
                // C1: non-star queries read ONLY the projected
                // columns straight from the store (one row-location resolve,
                // one decode per requested column) — the old path decoded
                // EVERY column of the row, including 1.5KB vectors and text,
                // then threw the rest away.
                if !meta.is_star && !meta.select_col_positions.is_empty() {
                    if let Some(store) = self.inner.get_col_segment_store(&meta.table_name) {
                        let composite = (meta.table_id << 32) | (row_id & 0xFFFFFFFF);
                        if let Some(vals) =
                            store.get_projected_multi(composite, &meta.select_col_positions)
                        {
                            return Ok(Some(StreamingQueryResult::SelectReady {
                                // 🔑 BUG #46: values here are the projected
                                // subset — the labels must be too.
                                columns: meta.select_col_names.clone(),
                                rows: vec![vals],
                            }));
                        }
                        // None → fall through to the full path (absent row,
                        // or a table split between store and LSM).
                    }
                }
                let row_opt =
                    self.inner
                        .get_table_row_arc(&meta.table_name, row_id, &meta.schema)?;
                let result_vec: Vec<Vec<Value>> = match row_opt {
                    Some(row_arc) => {
                        if meta.is_star {
                            vec![(*row_arc).clone()]
                        } else {
                            vec![meta
                                .select_col_positions
                                .iter()
                                .map(|&pos| row_arc.get(pos).cloned().unwrap_or(Value::Null))
                                .collect()]
                        }
                    }
                    None => vec![],
                };
                Ok(Some(StreamingQueryResult::SelectReady {
                    columns: if meta.is_star {
                        (*meta.column_names).clone()
                    } else {
                        // 🔑 BUG #46: projected values + projected names.
                        meta.select_col_names.clone()
                    },
                    rows: result_vec,
                }))
            }
        }
    }

    /// PK value → row_id resolution shared by the single-statement fast path
    /// and the executemany batch kernel. Integer PKs have a deterministic
    /// row_id mapping (see the insert path in crud.rs): row_id = pk for ≥ 0,
    /// negatives map to the high-u32 range. Non-Integer PKs need the
    /// lazily-populated pk_lookup cache; a miss does NOT mean absence — the
    /// caller must fall back to the full executor (which scans the columnar
    /// store) instead of returning a wrong-typed/empty result.
    fn resolve_fast_pk_row_id(&self, meta: &FastPkMeta, pk_value: &Value) -> FastPkRowId {
        if meta.is_auto_increment {
            match pk_value {
                Value::Integer(id) if *id >= 0 => FastPkRowId::Resolved(*id as RowId),
                _ => FastPkRowId::Absent,
            }
        } else {
            match pk_value {
                Value::Integer(id) => {
                    if *id >= 0 {
                        FastPkRowId::Resolved(*id as RowId)
                    } else {
                        FastPkRowId::Resolved(0x8000_0000u64 | (*id as u64 & 0x7FFF_FFFF))
                    }
                }
                _ => {
                    let pk_key = crate::database::pk_cache::PkKey::from_value(pk_value);
                    match self
                        .inner
                        .pk_lookup
                        .get(&meta.table_name)
                        .and_then(|lookup| lookup.get_pk(&pk_key))
                    {
                        Some(rid) => FastPkRowId::Resolved(rid),
                        None => FastPkRowId::Defer,
                    }
                }
            }
        }
    }

    /// 🚀 W1: buffered batch kernel for executemany UPDATE/DELETE. The
    /// statement machinery (bind_params / executor dispatch / result
    /// materialization) runs ONCE per batch in the caller; per row this does
    /// only pk→row_id + one row read + pending-map recording — the same
    /// coordinator calls the executor's M1/M2 txn branches make.
    ///
    /// Semantics are mirrored from execute_update_pk / execute_delete_pk for
    /// the `WHERE pk = ?` + all-absolute-SET shape; ANY row in a state the
    /// kernel doesn't cover (write_set overlap, already-pending/buffered,
    /// PK-cache miss) is delegated to the per-row executor, so behavior
    /// cannot drift. Returns None when the statement itself doesn't qualify
    /// (caller uses the per-row loop for the whole batch).
    fn executemany_fast_pk_buffered(
        &self,
        statement: &Statement,
        sql: &str,
        batch: &[Vec<Value>],
        tid: u64,
    ) -> Result<Option<u64>> {
        // 🚀 W1/B: inline plan extraction (NOT detect_fast_pk_pattern — that
        // rejects expression SETs, and the kernel now evaluates them). Any
        // shape the kernel can't represent defers the WHOLE batch to the
        // per-row executor, so semantics can't drift.
        enum Plan<'a> {
            Update {
                set_params: Vec<(usize, usize)>,   // (col_pos, param_idx)
                set_literals: Vec<(usize, Value)>, // (col_pos, value)
                set_exprs: Vec<(usize, &'a crate::sql::ast::Expr)>, // no subqueries
            },
            Delete,
        }
        let (table, pk_param_idx, plan) = match statement {
            Statement::Update(su) => {
                // WHERE must be exactly `pk = ?N` (either operand order).
                let pk_param_idx = match su.where_clause.as_ref() {
                    Some(crate::sql::ast::Expr::BinaryOp {
                        left,
                        op: crate::sql::ast::BinaryOperator::Eq,
                        right,
                    }) => match (left.as_ref(), right.as_ref()) {
                        (
                            crate::sql::ast::Expr::Column(c),
                            crate::sql::ast::Expr::Parameter(idx),
                        )
                        | (
                            crate::sql::ast::Expr::Parameter(idx),
                            crate::sql::ast::Expr::Column(c),
                        ) => {
                            let schema = self.inner.table_registry.get_table(&su.table)?;
                            let is_pk = schema
                                .primary_key()
                                .map(|pk| {
                                    let bare = pk.rsplit('.').next().unwrap_or(pk);
                                    bare == c.as_str() || pk == c.as_str()
                                })
                                .unwrap_or(false);
                            if !is_pk {
                                return Ok(None);
                            }
                            *idx
                        }
                        _ => return Ok(None),
                    },
                    _ => return Ok(None),
                };
                let schema = self.inner.table_registry.get_table(&su.table)?;
                let pk_pos = match schema.primary_key().and_then(|n| schema.get_column(n)) {
                    Some(cd) => cd.position,
                    None => return Ok(None),
                };
                let mut set_params = Vec::new();
                let mut set_literals = Vec::new();
                let mut set_exprs = Vec::new();
                for (col_name, expr) in &su.assignments {
                    let pos = match schema.get_column_position(col_name) {
                        Some(p) => p,
                        None => return Ok(None), // unknown column → executor errors properly
                    };
                    if pos == pk_pos {
                        return Ok(None); // PK relocation is the executor's job
                    }
                    match expr {
                        crate::sql::ast::Expr::Parameter(idx) => set_params.push((pos, *idx)),
                        crate::sql::ast::Expr::Literal(v) => set_literals.push((pos, v.clone())),
                        crate::sql::ast::Expr::UnaryOp {
                            op: crate::sql::ast::UnaryOperator::Minus,
                            expr: inner,
                        } => match inner.as_ref() {
                            crate::sql::ast::Expr::Literal(crate::types::Value::Integer(i)) => {
                                set_literals.push((pos, Value::Integer(-*i)))
                            }
                            crate::sql::ast::Expr::Literal(crate::types::Value::Float(f)) => {
                                set_literals.push((pos, Value::Float(-*f)))
                            }
                            _ => return Ok(None),
                        },
                        other => {
                            // Expression SET (v = v + ?, v = other_col, …).
                            // Subqueries need per-row materialization machinery
                            // — defer those (rare).
                            if crate::sql::executor::QueryExecutor::expr_contains_subquery(other) {
                                return Ok(None);
                            }
                            set_exprs.push((pos, other));
                        }
                    }
                }
                let max_idx = set_params
                    .iter()
                    .map(|&(_, idx)| idx)
                    .chain(std::iter::once(pk_param_idx))
                    .max()
                    .unwrap_or(0);
                if batch.iter().any(|p| p.len() < max_idx) {
                    return Ok(None);
                }
                (
                    su.table.as_str(),
                    pk_param_idx,
                    Plan::Update {
                        set_params,
                        set_literals,
                        set_exprs,
                    },
                )
            }
            Statement::Delete(sd) => {
                let pk_param_idx = match sd.where_clause.as_ref() {
                    Some(crate::sql::ast::Expr::BinaryOp {
                        left,
                        op: crate::sql::ast::BinaryOperator::Eq,
                        right,
                    }) => match (left.as_ref(), right.as_ref()) {
                        (
                            crate::sql::ast::Expr::Column(c),
                            crate::sql::ast::Expr::Parameter(idx),
                        )
                        | (
                            crate::sql::ast::Expr::Parameter(idx),
                            crate::sql::ast::Expr::Column(c),
                        ) => {
                            let schema = self.inner.table_registry.get_table(&sd.table)?;
                            let is_pk = schema
                                .primary_key()
                                .map(|pk| {
                                    let bare = pk.rsplit('.').next().unwrap_or(pk);
                                    bare == c.as_str() || pk == c.as_str()
                                })
                                .unwrap_or(false);
                            if !is_pk {
                                return Ok(None);
                            }
                            *idx
                        }
                        _ => return Ok(None),
                    },
                    _ => return Ok(None),
                };
                if batch.iter().any(|p| p.len() < pk_param_idx) {
                    return Ok(None);
                }
                (sd.table.as_str(), pk_param_idx, Plan::Delete)
            }
            _ => return Ok(None),
        };
        let schema = self.inner.table_registry.get_table(table)?;
        let pk_pos = match schema.primary_key().and_then(|n| schema.get_column(n)) {
            Some(cd) => cd.position,
            None => return Ok(None),
        };
        // A paramless FastPkMeta view of THIS statement, only so the shared
        // pk→row_id resolution helper can be reused.
        let is_update = matches!(plan, Plan::Update { .. });
        let meta = FastPkMeta {
            stmt_type: if is_update { "update" } else { "delete" },
            table_name: table.to_string(),
            table_id: self.inner.table_registry.get_table_id(table).unwrap_or(0) as u64,
            param_idx: pk_param_idx,
            is_star: false,
            select_col_positions: Vec::new(),
            select_col_names: Vec::new(),
            set_param_positions: Vec::new(),
            set_literal_positions: Vec::new(),
            is_auto_increment: schema.is_primary_key_auto_increment(),
            column_names: schema.column_names_arc(),
            schema: schema.clone(),
        };

        // Uncommitted INSERTs of this txn are invisible to pk resolution —
        // pre-collect their PK values once and defer those rows to the
        // executor's write_set pass (O(1) membership per row).
        let ctx = self.inner.txn_coordinator.get_context(tid)?;
        let ws_pks: std::collections::HashSet<crate::database::pk_cache::PkKey> = ctx
            .write_set
            .read()
            .iter()
            .filter(|((t, _), _)| t == table)
            .filter_map(|(_, row)| {
                row.get(pk_pos)
                    .map(crate::database::pk_cache::PkKey::from_value)
            })
            .collect();

        let mut affected: u64 = 0;
        macro_rules! fallback {
            ($n:expr) => {{
                affected += $n;
            }};
        }
        for params in batch {
            let pk_value = &params[pk_param_idx - 1];
            let row_id = match Self::resolve_fast_pk_row_id(self, &meta, pk_value) {
                FastPkRowId::Resolved(rid) => rid,
                FastPkRowId::Absent => continue,
                FastPkRowId::Defer => {
                    fallback!(self.fallback_row_prepared(sql, params)?);
                    continue;
                }
            };
            if ws_pks.contains(&crate::database::pk_cache::PkKey::from_value(pk_value)) {
                fallback!(self.fallback_row_prepared(sql, params)?);
                continue;
            }
            match &plan {
                Plan::Update {
                    set_params,
                    set_literals,
                    set_exprs,
                } => {
                    match self.query_executor.txn_lookup_row_pub(table, row_id) {
                        // deleted earlier in this txn → matches 0 rows
                        Some(None) => {}
                        // write_set row or already-pending update → executor
                        // chains/relocates with full statement semantics
                        Some(Some(_)) => {
                            fallback!(self.fallback_row_prepared(sql, params)?);
                        }
                        None => {
                            let row = match self.inner.get_table_row(table, row_id)? {
                                Some(r) => r,
                                None => continue, // no such row → 0 affected
                            };
                            let mut new_row = row.clone();
                            for &(pos, pidx) in set_params {
                                new_row[pos] = params[pidx - 1].clone();
                            }
                            for &(pos, ref lit) in set_literals {
                                new_row[pos] = lit.clone();
                            }
                            for &(pos, expr) in set_exprs {
                                // B: expression SET evaluated against THIS row
                                // with THIS row's params substituted (mirrors
                                // execute_streaming_ref's UPDATE substitution
                                // + execute_update_pk's per-row eval).
                                let resolved =
                                    crate::sql::executor::QueryExecutor::substitute_expr(
                                        expr, params,
                                    )?;
                                let v = crate::sql::executor::QueryExecutor::eval_expr_on_row(
                                    &resolved, &row, &schema,
                                )?;
                                new_row[pos] = v;
                            }
                            MoteDB::coerce_row_to_schema(&schema, &mut new_row);
                            let prior = self
                                .inner
                                .txn_coordinator
                                .record_pending_update(tid, table, row_id, row, new_row)?;
                            self.inner
                                .txn_coordinator
                                .record_pending_snapshot(tid, row_id, table, prior)?;
                            affected += 1;
                        }
                    }
                }
                Plan::Delete => {
                    // Mirror execute_delete_pk: drop a pending UPDATE first so
                    // COMMIT can't resurrect the row via newest-wins.
                    let _ = self
                        .inner
                        .txn_coordinator
                        .remove_pending_update(tid, table, row_id);
                    match self.query_executor.txn_lookup_row_pub(table, row_id) {
                        Some(None) => {}
                        Some(Some(_)) => {
                            fallback!(self.fallback_row_prepared(sql, params)?);
                        }
                        None => match self.inner.get_table_row(table, row_id)? {
                            None => {}
                            Some(row) => {
                                self.inner
                                    .txn_coordinator
                                    .record_pending_delete(tid, table, row_id, row)?;
                                affected += 1;
                            }
                        },
                    }
                }
            }
        }
        Ok(Some(affected))
    }

    /// One row of an executemany batch the batch kernel deferred: run the
    /// normal per-statement path (txn-aware; in-txn it buffers exactly as a
    /// standalone statement would).
    fn fallback_row_prepared(&self, sql: &str, params: &[Value]) -> Result<u64> {
        match self.execute_prepared(sql, params.to_vec())? {
            StreamingQueryResult::Modification { affected_rows } => Ok(affected_rows as u64),
            _ => Ok(0),
        }
    }

    /// Fast INSERT path: parses `INSERT INTO <table> VALUES (<literals>)` directly
    /// from the string without going through the full tokenizer + parser + cache.
    ///
    /// Returns None if the SQL doesn't match the simple INSERT pattern.
    fn try_fast_insert(&self, sql: &str) -> Result<Option<StreamingQueryResult>> {
        // Quick check: must start with "INSERT" (case-insensitive)
        let trimmed = sql.trim_start();
        if !trimmed
            .as_bytes()
            .get(0..6)
            .map(|b| b.eq_ignore_ascii_case(b"INSERT"))
            .unwrap_or(false)
        {
            return Ok(None);
        }

        // Find "INSERT INTO <table>"
        let rest = &trimmed[6..].trim_start();
        if !rest
            .as_bytes()
            .get(0..4)
            .map(|b| b.eq_ignore_ascii_case(b"INTO"))
            .unwrap_or(false)
        {
            return Ok(None);
        }
        let after_into = rest[4..].trim_start();

        // Extract table name (skip optional column list)
        let (table_name, after_table) =
            match after_into.find(|c: char| c.is_whitespace() || c == '(') {
                Some(pos) => (&after_into[..pos], after_into[pos..].trim_start()),
                None => return Ok(None),
            };
        if table_name.is_empty() {
            return Ok(None);
        }

        // Parse optional column list: INSERT INTO t (col1, col2) VALUES ...
        let (col_names, after_cols) = if after_table.starts_with('(') {
            match after_table.find(')') {
                Some(p) => {
                    let col_str = &after_table[1..p];
                    let cols: Vec<String> = col_str
                        .split(',')
                        .map(|s| {
                            let s = s.trim();
                            // Strip surrounding double-quotes (quoted identifier)
                            if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
                                s[1..s.len() - 1].to_string()
                            } else {
                                s.to_string()
                            }
                        })
                        .collect();
                    (Some(cols), after_table[p + 1..].trim_start())
                }
                None => return Ok(None),
            }
        } else {
            (None, after_table)
        };

        // Must be followed by "VALUES"
        if !after_cols
            .as_bytes()
            .get(0..6)
            .map(|b| b.eq_ignore_ascii_case(b"VALUES"))
            .unwrap_or(false)
        {
            return Ok(None);
        }
        let values_part = after_cols[6..].trim_start();

        // Resolve schema
        let schema = match self.inner.table_registry.get_table(table_name) {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };
        // 🔑 TimeSeries tables must take the AST path (execute_columnar_
        // insert → ColumnarStore). Writing them through the standard
        // row path here created a ColSegmentStore that shadowed the
        // authoritative ColumnarStore data in every index fast path.
        if schema.table_type == crate::types::TableType::TimeSeries {
            return Ok(None);
        }

        // Parse multiple value tuples: (a,b,c),(d,e,f),...
        let mut rows: Vec<Vec<Value>> = Vec::new();
        let mut pos = 0usize;
        let bytes = values_part.as_bytes();

        while pos < bytes.len() {
            // Skip whitespace and commas
            while pos < bytes.len()
                && (bytes[pos] == b' '
                    || bytes[pos] == b'\n'
                    || bytes[pos] == b'\t'
                    || bytes[pos] == b',')
            {
                pos += 1;
            }
            if pos >= bytes.len() {
                break;
            }
            if bytes[pos] != b'(' {
                return Ok(None);
            }
            pos += 1; // skip '('

            // Find matching ')'
            let mut depth = 1;
            let start = pos;
            while pos < bytes.len() && depth > 0 {
                match bytes[pos] {
                    b'(' => depth += 1,
                    b')' => depth -= 1,
                    b'\'' => {
                        // Skip quoted string
                        pos += 1;
                        while pos < bytes.len() && bytes[pos] != b'\'' {
                            if bytes[pos] == b'\\' {
                                pos += 1;
                            }
                            pos += 1;
                        }
                    }
                    _ => {}
                }
                if depth > 0 {
                    pos += 1;
                }
            }
            if depth != 0 {
                return Ok(None);
            }
            let tuple_str = std::str::from_utf8(&bytes[start..pos]).unwrap_or("");
            pos += 1; // skip ')'

            // Parse values in this tuple
            let values = match Self::parse_literal_list(tuple_str) {
                Some(v) => v,
                None => return Ok(None),
            };
            if values.is_empty() {
                continue;
            }

            // Build row: map values to schema positions using column list or default order
            let row = if let Some(ref cols) = col_names {
                if values.len() != cols.len() {
                    return Err(crate::error::MoteDBError::InvalidArgument(format!(
                        "Column count mismatch: expected {}, got {}",
                        cols.len(),
                        values.len()
                    )));
                }
                match crate::sql::row_converter::values_to_row_by_columns(&values, cols, &schema) {
                    Ok(r) => r,
                    Err(_) => return Ok(None),
                }
            } else {
                // Without an explicit column list, the value count must match the
                // table's column count exactly (else fall back for error reporting).
                if values.len() != schema.columns.len() {
                    return Err(crate::error::MoteDBError::InvalidArgument(format!(
                        "Column count mismatch: expected {}, got {}",
                        schema.columns.len(),
                        values.len()
                    )));
                }
                crate::sql::row_converter::values_to_row_schema_order(&values, &schema)?
            };
            rows.push(row);
        }

        if rows.is_empty() {
            return Ok(None);
        }

        let affected = rows.len();
        if rows.len() == 1 {
            self.inner
                .insert_row_to_table(table_name, rows.into_iter().next().unwrap())?;
        } else {
            self.inner.batch_insert_rows_to_table(table_name, rows)?;
        }

        Ok(Some(StreamingQueryResult::Modification {
            affected_rows: affected,
        }))
    }

    /// Find a keyword in haystack case-insensitively, requiring word boundaries.
    /// Returns the byte offset of the keyword start, or None.
    /// Matches " from " (space-padded), "FROM ..." (at start), or "... FROM" (at end).
    fn find_keyword_ci(haystack: &str, keyword: &str) -> Option<usize> {
        let klen = keyword.len();
        let hbytes = haystack.as_bytes();
        let kbytes = keyword.as_bytes();
        if hbytes.len() < klen {
            return None;
        }

        for i in 0..=hbytes.len() - klen {
            // Quick check: first char must match (case-insensitive)
            if !hbytes[i].eq_ignore_ascii_case(&kbytes[0]) {
                continue;
            }
            // Full keyword match
            if !hbytes[i..i + klen].eq_ignore_ascii_case(kbytes) {
                continue;
            }
            // Word boundary before keyword
            if i > 0 && !hbytes[i - 1].is_ascii_whitespace() {
                continue;
            }
            // Word boundary after keyword
            if i + klen < hbytes.len() && !hbytes[i + klen].is_ascii_whitespace() {
                continue;
            }
            return Some(i);
        }
        None
    }

    /// Fast SELECT path: handles `SELECT cols FROM table WHERE pk = value`
    /// Bypasses tokenizer + parser + statement cache (~280µs overhead).
    fn try_fast_select(&self, sql: &str) -> Result<Option<StreamingQueryResult>> {
        let trimmed = sql.trim_start();
        if !trimmed
            .as_bytes()
            .get(0..6)
            .map(|b| b.eq_ignore_ascii_case(b"SELECT"))
            .unwrap_or(false)
        {
            return Ok(None);
        }
        let after_select = trimmed[6..].trim_start();

        // Find "FROM" keyword (case-insensitive, word boundary)
        let from_pos = match Self::find_keyword_ci(after_select, "from") {
            Some(p) => p,
            None => return Ok(None),
        };
        let after_from = after_select[from_pos + 4..].trim_start();

        // Extract table name
        let (table_name, after_table) = match after_from.find(|c: char| c.is_whitespace()) {
            Some(p) => (&after_from[..p], after_from[p..].trim_start()),
            None => return Ok(None),
        };
        if table_name.is_empty() {
            return Ok(None);
        }
        // 🔑 TimeSeries tables: bail to the AST path — they are served by
        // the ColumnarStore, which this hand-rolled parser knows nothing
        // about (its scan would return 0 rows).
        if let Ok(schema) = self.inner.table_registry.get_table(table_name) {
            if schema.table_type == crate::types::TableType::TimeSeries {
                return Ok(None);
            }
        }

        // 🆕 ColSegmentStore tables: we no longer bail out wholesale. The PK
        // point-query fast path below handles them too (routing directly to
        // store.get() → fence-index binary search, skipping the SQL parser).
        // Non-PK WHERE on ColSegmentStore tables still falls through to the
        // full parse path (handled in the !is_pk branch below).
        let has_col_seg = self.inner.has_col_segment_store(table_name);
        // 🔑 When inside a transaction, also treat the table as "having a
        // col segment store" if there are buffered writes for it — so the
        // PK fast path below checks the write_set even when no committed
        // data exists yet (txn-only INSERTs).
        let has_txn_writes = self.query_executor.is_in_transaction()
            && !self
                .query_executor
                .txn_write_set_rows(table_name)
                .is_empty();
        if !has_col_seg && !has_txn_writes {
            // Legacy single-SSTable tables: finalize write buffer so the
            // columnar paths below see all data.
            self.inner.finalize_columnar_buffer(table_name);
        }
        let has_col_seg = has_col_seg || has_txn_writes;

        // Check for "WHERE" keyword (word boundary)
        let where_pos = match Self::find_keyword_ci(after_table, "where") {
            Some(p) => p,
            None => return Ok(None),
        };
        let after_where = after_table[where_pos + 5..].trim_start();

        // Parse: column = value (only simple equality)
        let eq_pos = match after_where.find('=') {
            Some(p) => p,
            None => return Ok(None),
        };
        let col_name = after_where[..eq_pos].trim();
        let val_str = after_where[eq_pos + 1..].trim();

        // Truncate trailing SQL keywords (ORDER BY, LIMIT, etc).
        // For quoted strings ('...'), find the closing quote first to preserve spaces.
        let val_str = if let Some(after_open) = val_str.strip_prefix('\'') {
            // Find matching closing quote
            if let Some(end) = after_open.find('\'') {
                &val_str[..end + 2] // include both quotes
            } else {
                val_str
            }
        } else {
            val_str.split_whitespace().next().unwrap_or(val_str)
        };
        let value = match Self::parse_single_literal(val_str) {
            Some(v) => v,
            None => return Ok(None),
        };

        // 🔑 Reject set operations (UNION/INTERSECT/EXCEPT) — the fast path
        // treats this as a single SELECT, which would silently drop the right
        // side of the query. Check the tail after the parsed value for set-op
        // keywords and fall through to the full parser if present.
        let after_val_pos = after_where[eq_pos + 1..]
            .find(val_str)
            .map(|p| eq_pos + 1 + p + val_str.len())
            .unwrap_or(after_where.len());
        let after_val = after_where[after_val_pos..].trim_start();
        if Self::starts_with_set_op(after_val) {
            return Ok(None);
        }

        // 🚨 This fast path only implements `col = value` filtering followed by
        // a direct row fetch — it does NOT apply ORDER BY / LIMIT / OFFSET /
        // DISTINCT. Previously it silently ignored those trailing clauses,
        // so `SELECT v FROM t WHERE cat='c0' ORDER BY v DESC LIMIT 5` returned
        // ALL 20 matching rows instead of the top 5. If any such clause is
        // present after the value, fall through to the full parser/executor
        // which handles them correctly.
        if Self::find_keyword_ci(after_val, "order").is_some()
            || Self::find_keyword_ci(after_val, "limit").is_some()
            || Self::find_keyword_ci(after_val, "offset").is_some()
        {
            return Ok(None);
        }

        // 🚨 Compound predicates: this fast path only implements a single
        // `col = value` filter. `a = 1 AND b = 2` was parsed as `a = 1` (the
        // value parser took "1" via split_whitespace, dropping `AND b = 2`),
        // returning rows matching only the first predicate. Fall through to
        // the full parser/executor if AND/OR follows the value.
        if Self::find_keyword_ci(after_val, "and").is_some()
            || Self::find_keyword_ci(after_val, "or").is_some()
        {
            return Ok(None);
        }

        // Resolve schema
        let schema = match self.inner.table_registry.get_table(table_name) {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };

        // Only optimize primary key lookups
        let is_pk = schema
            .primary_key()
            .map(|pk| pk == col_name)
            .unwrap_or(false);

        // Determine select columns (shared by both PK and column-index paths)
        let select_part = after_select[..from_pos].trim();
        let is_star = select_part == "*";

        // Check for aggregates (COUNT, SUM, AVG, MIN, MAX) — fast path can't handle them.
        // Must fall through to full SQL path for proper aggregation.
        let has_aggregates = Self::contains_aggregate_function(select_part);
        if has_aggregates {
            return Ok(None);
        }

        // 🚨 DISTINCT: this fast path returns raw matching rows without dedup,
        // so `SELECT DISTINCT cat FROM t WHERE ...` would return duplicates.
        // Fall through to the full parser which applies DISTINCT correctly.
        if Self::find_keyword_ci(select_part, "distinct").is_some() {
            return Ok(None);
        }

        // 🔒 Strict select-list validation (BUG #46 class): every projected
        // column below is built with filter_map/get_column on this comma
        // split. An item that doesn't resolve (unknown column, qualified
        // `t.v`, alias, function call) used to be SILENTLY DROPPED — the
        // output kept the full column_names but rows had fewer values.
        // Validate up front and defer the whole statement to the full parser.
        if !is_star {
            for item in select_part.split(',').map(|s| s.trim()) {
                if schema.get_column_position(item).is_none() {
                    return Ok(None);
                }
            }
        }

        // 🚀 ColSegmentStore PK point query: route directly to store.get() →
        // fence-index binary search, completely bypassing the SQL parser +
        // AST executor. This is the hottest path for OLTP read workloads.
        // Non-PK WHERE on ColSegmentStore tables falls through to the full
        // parse path below (the !is_pk branch checks columnar_sstables,
        // which sync_col_segment_to_sstables keeps populated).
        if has_col_seg && is_pk {
            return self.fast_col_segment_pk_select(
                table_name,
                &schema,
                &value,
                select_part,
                is_star,
            );
        }

        if !is_pk {
            // 🚀 For ColSegmentStore tables, check if the filter column has a
            // column index FIRST — the index fast path below (line ~1241) does
            // an O(log N) B+tree lookup + batch row fetch, much faster than the
            // full SQL parse path. Previously this was gated off entirely for
            // ColSeg tables (the `return Ok(None)`), forcing 89µs of re-parsing
            // per query. Now we only fall through to the parse path when there
            // is NO index on the filter column.
            let has_col_index = self
                .inner
                .index_registry
                .find_by_column(
                    table_name,
                    col_name,
                    crate::database::index_metadata::IndexType::Column,
                )
                .is_some();
            if has_col_seg && !has_col_index {
                // No index: fall through to full parse path (columnar scan).
                return Ok(None);
            }
            // 🚀 Columnar SSTable fast path: use columnar filtered scan instead of
            // per-row batch fetch. Much faster for low-selectivity filters.
            // NOTE: only use this when there is NO column index — the index path
            // below (line ~1306) is O(log N + K) vs this full scan's O(N).
            // EXCEPTION: for ColSegmentStore tables where the index matches many
            // rows (high cardinality), per-row get() is extremely expensive
            // (100K rows × lock+scan+decode). The columnar scan is a single-pass
            // filter, much faster in that case. The index path below has a
            // cardinality check that skips to this path when len > 1000.
            if !has_col_index && self.inner.columnar_sstables.contains_key(table_name) {
                let col_types = schema.col_types();
                let filter_pos = schema.get_column_position(col_name);
                if let Some(pos) = filter_pos {
                    if let Ok(iter) = self
                        .inner
                        .scan_columnar_sstable_filtered(table_name, col_types, pos, &value)
                    {
                        let column_names: Vec<String> = if is_star {
                            schema.column_names()
                        } else {
                            select_part
                                .split(',')
                                .map(|s| s.trim().to_string())
                                .collect()
                        };
                        let rows: Vec<Vec<Value>> = iter.collect();
                        return Ok(Some(StreamingQueryResult::SelectReady {
                            columns: column_names,
                            rows,
                        }));
                    }
                }
            }

            // Column index fast path: bypass parser for indexed non-PK columns
            let index_name = self
                .inner
                .index_registry
                .find_by_column(
                    table_name,
                    col_name,
                    crate::database::index_metadata::IndexType::Column,
                )
                .unwrap_or_else(|| format!("{}.{}", table_name, col_name));
            if let Some(index_ref) = self.inner.column_indexes.get(&index_name) {
                // 🔒 M1/M2 read-your-writes: the column index holds COMMITTED
                // state only. With buffered pending writes for this table,
                // skip the fast path — the parsed path folds pending state
                // (txn merge scan).
                if self.query_executor.is_in_transaction() {
                    let has_pending = !self.query_executor.txn_pending_rows(table_name).is_empty()
                        || !self
                            .query_executor
                            .txn_pending_delete_ids(table_name)
                            .is_empty();
                    if has_pending {
                        return Ok(None);
                    }
                }
                let row_ids_arc = index_ref.value().get_arc(&value)?;

                // 🚀 High-cardinality redirect: for ColSegmentStore tables, when
                // the index matches a very large row set, the batch row fetch
                // (K sorted point reads) eventually loses to a single-pass
                // columnar scan. Threshold aligned with the executor's index
                // fast path (10000) so mid-selectivity filters (1K-10K
                // matches) keep using the index: O(log N + K) targeted reads
                // beat scanning the whole table (e.g. 1600 matches in a 1.6M
                // row table: ~15ms index fetch vs ~180ms full scan).
                if has_col_seg && row_ids_arc.len() > 10000 {
                    if let Some(store) = self.inner.get_col_segment_store(table_name) {
                        let col_types = schema.col_types();
                        let filter_pos = schema.get_column_position(col_name);
                        if let Some(fc) = filter_pos {
                            let _ = store.flush_buffer();
                            let out_pos: Vec<usize> = if is_star {
                                (0..col_types.len()).collect()
                            } else {
                                select_part
                                    .split(',')
                                    .filter_map(|s| schema.get_column_position(s.trim()))
                                    .collect()
                            };
                            let target = value.clone();
                            let scanned = store.scan_projected_filtered(
                                Some(fc),
                                &out_pos,
                                &move |fv: Option<&Value>| fv == Some(&target),
                            );
                            let column_names: Vec<String> = if is_star {
                                schema.column_names()
                            } else {
                                select_part
                                    .split(',')
                                    .map(|s| s.trim().to_string())
                                    .collect()
                            };
                            let rows: Vec<Vec<Value>> =
                                scanned.into_iter().map(|(_, r)| r).collect();
                            return Ok(Some(StreamingQueryResult::SelectReady {
                                columns: column_names,
                                rows,
                            }));
                        }
                    }
                }

                // If index returns empty, the async pipeline may not have built it yet.
                // Fall through to full SQL path to avoid false empty results.
                if !row_ids_arc.is_empty() {
                    drop(index_ref);
                    // Post-filter: column index truncates Text values to a prefix,
                    // so verify the actual row value matches the search value.
                    let filter_col_pos = schema.get_column_position(col_name);
                    let column_names: Vec<String> = if is_star {
                        schema.column_names()
                    } else {
                        select_part
                            .split(',')
                            .map(|s| s.trim().to_string())
                            .collect()
                    };

                    if is_star {
                        // SELECT * — batch fetch (ArcString makes clone cheap)
                        let batch = self
                            .inner
                            .get_table_rows_batch_arc(table_name, &row_ids_arc)?;
                        let rows: Vec<Vec<Value>> = batch
                            .into_iter()
                            .filter_map(|(_, opt)| {
                                opt.map(|a| match Arc::try_unwrap(a) {
                                    Ok(row) => row,
                                    Err(arc) => (*arc).clone(),
                                })
                            })
                            .filter(|row| {
                                filter_col_pos
                                    .map(|pos| row.get(pos) == Some(&value))
                                    .unwrap_or(true)
                            })
                            .collect();
                        return Ok(Some(StreamingQueryResult::SelectReady {
                            columns: column_names,
                            rows,
                        }));
                    }

                    // Non-star: project specific columns
                    let batch = self
                        .inner
                        .get_table_rows_batch_arc(table_name, &row_ids_arc)?;
                    let mut result_vec = Vec::with_capacity(batch.len());
                    {
                        let col_list: Vec<&str> =
                            select_part.split(',').map(|s| s.trim()).collect();
                        let col_positions: Vec<usize> = col_list
                            .iter()
                            .filter_map(|c| schema.get_column(c).map(|cd| cd.position))
                            .collect();
                        result_vec.extend(batch.into_iter().filter_map(|(_, opt_arc)| {
                            opt_arc.and_then(|a| {
                                // Post-filter: verify actual column value matches search value
                                if let Some(pos) = filter_col_pos {
                                    if a.get(pos) != Some(&value) {
                                        return None;
                                    }
                                }
                                Some(
                                    col_positions
                                        .iter()
                                        .map(|&pos| a.get(pos).cloned().unwrap_or(Value::Null))
                                        .collect(),
                                )
                            })
                        }));
                    }
                    return Ok(Some(StreamingQueryResult::SelectReady {
                        columns: column_names,
                        rows: result_vec,
                    }));
                }
            }
            return Ok(None);
        }

        let is_ai = schema.is_primary_key_auto_increment();

        // Fetch row using Arc<Row> (avoids cloning row data for cache hits)
        let row_opt = if is_ai {
            match &value {
                Value::Integer(id) if *id >= 0 => {
                    self.inner
                        .get_table_row_arc(table_name, *id as RowId, &schema)?
                }
                _ => return Ok(None),
            }
        } else {
            // Non-AUTO_INCREMENT PK: use pk_lookup cache (O(1)), fall back to column index
            let pk_key = crate::database::pk_cache::PkKey::from_value(&value);
            let resolve_fallback =
                |db: &MoteDB, table: &str, col: &str, val: &Value| -> Option<RowId> {
                    match db.query_by_column(table, col, val) {
                        Ok(ids) if !ids.is_empty() => ids.into_iter().next(),
                        _ => {
                            // Column index missing (e.g. after restart) — full scan
                            let s = db.get_table_schema(table).ok()?;
                            let pos = s.get_column_position(col)?;
                            let rows = db.scan_table_rows_streaming(table).ok()?;
                            for (rid, row) in rows.flatten() {
                                if row.get(pos)? == val {
                                    return Some(rid);
                                }
                            }
                            None
                        }
                    }
                };
            let row_id = if let Some(lookup) = self.inner.pk_lookup.get(table_name) {
                if let Some(rid) = lookup.get_pk(&pk_key) {
                    Some(rid)
                } else {
                    let rid = resolve_fallback(&self.inner, table_name, col_name, &value);
                    if let Some(r) = rid {
                        lookup.insert(pk_key, r);
                    }
                    rid
                }
            } else {
                resolve_fallback(&self.inner, table_name, col_name, &value)
            };
            match row_id {
                Some(rid) => self.inner.get_table_row_arc(table_name, rid, &schema)?,
                None => None,
            }
        };

        // Build result — clone values from Arc<Row>
        let result_vec: Vec<Vec<Value>> = match row_opt {
            Some(row_arc) => {
                if is_star {
                    vec![(*row_arc).clone()]
                } else {
                    let col_list: Vec<&str> = select_part.split(',').map(|s| s.trim()).collect();
                    let mut vals = Vec::with_capacity(col_list.len());
                    for cname in &col_list {
                        if let Some(cd) = schema.get_column(cname) {
                            vals.push(row_arc.get(cd.position).cloned().unwrap_or(Value::Null));
                        } else {
                            return Ok(None);
                        }
                    }
                    vec![vals]
                }
            }
            None => vec![],
        };

        let column_names: Vec<String> = if is_star {
            schema.column_names()
        } else {
            select_part
                .split(',')
                .map(|s| s.trim().to_string())
                .collect()
        };

        Ok(Some(StreamingQueryResult::SelectReady {
            columns: column_names,
            rows: result_vec,
        }))
    }

    /// 🚀 ColSegmentStore PK point query — no-parse fast path.
    ///
    /// Called from `try_fast_select` when the table has a ColSegmentStore AND
    /// the WHERE clause is `pk = literal`. Builds the composite key and does
    /// a direct `store.get()` (fence-index binary search), completely skipping
    /// the SQL lexer + parser + AST executor.
    ///
    /// Mirrors the logic in `try_col_segment_pk_point_query` (executor.rs) but
    /// operates on the pre-parsed `col_name`/`value` from `try_fast_select`,
    /// so there is zero parsing overhead.
    /// 🔑 Read-your-writes point lookup for the no-parse API fast path.
    /// Mirrors QueryExecutor::txn_lookup_row but accessible from Database
    /// (which holds query_executor + inner directly). Returns:
    /// - Some(Some(row)) — row in write_set (uncommitted INSERT)
    /// - Some(None) — row was DELETEd by this transaction
    /// - None — no transactional info
    fn txn_lookup_row_api(&self, table: &str, row_id: u64) -> Option<Option<Vec<Value>>> {
        let txn_id = self.query_executor.current_txn_id()?;
        let ctx = self.inner.txn_coordinator.get_context(txn_id).ok()?;
        // 🔒 M2 buffered DELETE check first.
        let deletes = ctx.pending_deletes.read();
        let deleted = deletes.contains_key(&(table.to_string(), row_id));
        drop(deletes);
        let ws = ctx.write_set.read();
        if let Some(row) = ws.get(&(table.to_string(), row_id)) {
            return Some(Some(row.clone()));
        }
        drop(ws);
        // 🔒 M1: buffered pending UPDATE — the new value is current in-txn.
        let pending = ctx.pending_updates.read();
        if let Some((_, new)) = pending.get(&(table.to_string(), row_id)) {
            return Some(Some(new.clone()));
        }
        if deleted {
            return Some(None);
        }
        None
    }

    fn fast_col_segment_pk_select(
        &self,
        table_name: &str,
        schema: &crate::types::TableSchema,
        value: &Value,
        select_part: &str,
        is_star: bool,
    ) -> Result<Option<StreamingQueryResult>> {
        // Resolve column positions for projection up front (cheap, no alloc
        // unless non-star). Pre-compute so the not-found and found paths share.
        let column_names: Vec<String> = if is_star {
            schema.column_names()
        } else {
            select_part
                .split(',')
                .map(|s| s.trim().to_string())
                .collect()
        };

        // Build composite key (table_id << 32 | row_id), matching the insert
        // path in crud.rs. Negative Integer PKs map to high u32 range.
        let table_id = self
            .inner
            .table_registry
            .get_table_id(table_name)
            .unwrap_or(0) as u64;
        let composite_key = if schema.is_primary_key_auto_increment() {
            match value {
                Value::Integer(id) if *id >= 0 => (table_id << 32) | (*id as u64 & 0xFFFFFFFF),
                _ => return Ok(None), // non-int or negative AI PK → full parse
            }
        } else {
            match value {
                Value::Integer(id) => {
                    let row_id = if *id >= 0 {
                        *id as u64
                    } else {
                        0x8000_0000u64 | (*id as u64 & 0x7FFF_FFFF)
                    };
                    (table_id << 32) | (row_id & 0xFFFFFFFF)
                }
                _ => {
                    // Non-Integer PK: try pk_lookup cache. On miss, fall through
                    // to the full parse path (which scans + populates cache).
                    let pk_key = crate::database::pk_cache::PkKey::from_value(value);
                    match self
                        .inner
                        .pk_lookup
                        .get(table_name)
                        .and_then(|l| l.get_pk(&pk_key))
                    {
                        Some(rid) => (table_id << 32) | (rid & 0xFFFFFFFF),
                        None => return Ok(None),
                    }
                }
            }
        };

        // 🔑 Read-your-writes: check transaction write_set / undo_log first.
        // write_set keys by raw row_id (low 32 bits of composite_key).
        let row_id = composite_key as u32 as u64;
        let txn_row = self.txn_lookup_row_api(table_name, row_id);
        // If the row is in the transaction (insert or tombstone), we can answer
        // without a store lookup — even if no store exists yet (txn-only data).
        if let Some(opt_row) = &txn_row {
            let row: Vec<Value> = match opt_row {
                Some(r) => r.clone(), // transaction inserted this row
                None => {
                    // transaction deleted this row → empty
                    return Ok(Some(StreamingQueryResult::SelectReady {
                        columns: column_names,
                        rows: vec![],
                    }));
                }
            };
            return self.finish_fast_pk_select(
                table_name,
                schema,
                row,
                select_part,
                is_star,
                column_names,
            );
        }
        // No transactional info — consult storage.
        let store = match self.inner.get_col_segment_store(table_name) {
            Some(s) => s,
            None => return Ok(None), // no store yet → full parse path
        };
        // C1: non-star SELECTs decode ONLY the projected columns (one row
        // resolve + per-column read) instead of materializing the full row.
        if !is_star {
            let mut positions: Vec<usize> = Vec::new();
            for cname in select_part.split(',').map(|s| s.trim()) {
                match schema.get_column(cname) {
                    Some(cd) => positions.push(cd.position),
                    None => return Ok(None), // unknown column → full parser error
                }
            }
            return match store.get_projected_multi(composite_key, &positions) {
                Some(vals) => Ok(Some(StreamingQueryResult::SelectReady {
                    columns: column_names,
                    rows: vec![vals],
                })),
                None => Ok(Some(StreamingQueryResult::SelectReady {
                    columns: column_names,
                    rows: vec![],
                })),
            };
        }
        let row: Vec<Value> = match store.get(composite_key) {
            Some(r) => r,
            None => {
                return Ok(Some(StreamingQueryResult::SelectReady {
                    columns: column_names,
                    rows: vec![],
                }));
            }
        };
        self.finish_fast_pk_select(table_name, schema, row, select_part, is_star, column_names)
    }

    /// Shared projection tail for fast_col_segment_pk_select — used by both
    /// the transaction-write_set path and the storage path.
    fn finish_fast_pk_select(
        &self,
        table_name: &str,
        schema: &crate::types::TableSchema,
        row: Vec<Value>,
        select_part: &str,
        is_star: bool,
        column_names: Vec<String>,
    ) -> Result<Option<StreamingQueryResult>> {
        let _ = table_name;
        // Project columns. SELECT * → full row (no clone); SELECT col → project.
        let result_row: Vec<Value> = if is_star {
            row
        } else {
            let mut projected = Vec::with_capacity(column_names.len());
            for cname in select_part.split(',').map(|s| s.trim()) {
                if let Some(cd) = schema.get_column(cname) {
                    projected.push(row.get(cd.position).cloned().unwrap_or(Value::Null));
                } else {
                    // Unknown column → fall back to parser for proper error.
                    return Ok(None);
                }
            }
            projected
        };

        Ok(Some(StreamingQueryResult::SelectReady {
            columns: column_names,
            rows: vec![result_row],
        }))
    }

    /// Parse a single SQL literal (integer, float, string, or simple expr like col + lit).
    /// Returns None if the value isn't a literal (falls through to full parser).
    /// Check if a SELECT column expression contains aggregate functions.
    /// Returns true if any of COUNT, SUM, AVG, MIN, MAX are found (case-insensitive).
    fn contains_aggregate_function(select_part: &str) -> bool {
        // 🔑 零分配：旧实现每次 execute 都 to_uppercase() 分配整段 String，
        // 在并发下挤爆 jemalloc arena 互斥（点查路径 ~7 次小分配之一）。
        // 直接在原字节上做 ASCII 大小写不敏感的窗口扫描。
        const KEYWORDS: [&str; 5] = ["COUNT", "SUM", "AVG", "MIN", "MAX"];
        let hay = select_part.as_bytes();
        for keyword in KEYWORDS {
            let kb = keyword.as_bytes();
            let klen = kb.len();
            if hay.len() < klen {
                continue;
            }
            for i in 0..=hay.len() - klen {
                if !hay[i].eq_ignore_ascii_case(&kb[0]) {
                    continue;
                }
                if !hay[i..i + klen].eq_ignore_ascii_case(kb) {
                    continue;
                }
                // 词边界：前后不能是字母数字/下划线（max_value 不算 MAX）
                let before_ok =
                    i == 0 || (!hay[i - 1].is_ascii_alphanumeric() && hay[i - 1] != b'_');
                let after = i + klen;
                let after_ok = after >= hay.len()
                    || (!hay[after].is_ascii_alphanumeric() && hay[after] != b'_');
                if before_ok && after_ok {
                    return true;
                }
            }
        }
        false
    }

    /// Check if a string starts with a SQL set operation keyword (UNION,
    /// INTERSECT, EXCEPT), case-insensitive, with word boundary. Used by
    /// the SELECT fast path to reject multi-statement queries.
    fn starts_with_set_op(s: &str) -> bool {
        let s = s.trim_start();
        for kw in &["UNION", "INTERSECT", "EXCEPT"] {
            let kb = kw.as_bytes();
            if s.as_bytes()
                .get(..kb.len())
                .map(|b| b.eq_ignore_ascii_case(kb))
                .unwrap_or(false)
            {
                // Word boundary: next char is whitespace or end.
                let after = s.get(kb.len()..).unwrap_or("");
                if after.is_empty() || after.as_bytes()[0].is_ascii_whitespace() {
                    return true;
                }
            }
        }
        false
    }

    fn parse_single_literal(s: &str) -> Option<Value> {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }
        if s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2 {
            let inner = &s[1..s.len() - 1];
            let mut text = String::with_capacity(inner.len());
            let mut chars = inner.chars().peekable();
            while let Some(c) = chars.next() {
                match c {
                    '\\' => match chars.next() {
                        Some('n') => text.push('\n'),
                        Some('t') => text.push('\t'),
                        Some('r') => text.push('\r'),
                        Some('\\') => text.push('\\'),
                        Some('\'') => text.push('\''),
                        Some(c2) => {
                            text.push('\\');
                            text.push(c2);
                        }
                        None => text.push('\\'),
                    },
                    '\'' if chars.peek() == Some(&'\'') => {
                        // Doubled quote: '' → literal single quote
                        text.push('\'');
                        chars.next();
                    }
                    c => text.push(c),
                }
            }
            return Some(Value::text(text));
        }
        if s.starts_with('-') || s.as_bytes().first()?.is_ascii_digit() {
            if let Ok(i) = s.parse::<i64>() {
                return Some(Value::Integer(i));
            }
            if let Ok(f) = s.parse::<f64>() {
                return Some(Value::Float(f));
            }
        }
        if s.eq_ignore_ascii_case("NULL") {
            return Some(Value::Null);
        }
        None
    }

    /// Try to evaluate a simple SET expression like `col + 10` or `col * 2`
    /// against the old row. Returns None if the expression is too complex.
    fn evaluate_simple_set_expr(
        expr_str: &str,
        old_row: &[Value],
        schema: &crate::types::TableSchema,
    ) -> Option<Value> {
        // Pattern: column_name operator literal
        let expr_str = expr_str.trim();
        for &op in &[" + ", " - ", " * ", " / "] {
            if let Some(pos) = expr_str.find(op) {
                let col_name = expr_str[..pos].trim();
                let lit_str = expr_str[pos + op.len()..].trim();
                let lit = Self::parse_single_literal(lit_str)?;
                let col_pos = schema.get_column_position(col_name)?;
                let old_val = old_row.get(col_pos)?;
                return match op.trim() {
                    "+" => Self::positional_fast_add(old_val, &lit),
                    "-" => Self::positional_fast_sub(old_val, &lit),
                    "*" => Self::positional_fast_mul(old_val, &lit),
                    "/" => Self::positional_fast_div(old_val, &lit),
                    _ => None,
                };
            }
        }
        None
    }

    /// Fast arithmetic (no HashMap, no evaluator) for simple UPDATE expressions.
    fn positional_fast_add(a: &Value, b: &Value) -> Option<Value> {
        use crate::types::Value;
        match (a, b) {
            // 🔑 On overflow, promote to Float (matches eval_expr_on_row behavior).
            // Previously returned None, which caused try_fast_update to fall through
            // to the full parser — but the full parser's PK path silently kept the
            // old value (the Float→Integer coercion saturated back to i64::MAX).
            (Value::Integer(a), Value::Integer(b)) => match a.checked_add(*b) {
                Some(v) => Some(Value::Integer(v)),
                None => Some(Value::Float(*a as f64 + *b as f64)),
            },
            (Value::Float(a), Value::Float(b)) => Some(Value::Float(a + b)),
            (Value::Integer(a), Value::Float(b)) => Some(Value::Float(*a as f64 + b)),
            (Value::Float(a), Value::Integer(b)) => Some(Value::Float(a + *b as f64)),
            _ => None,
        }
    }
    fn positional_fast_sub(a: &Value, b: &Value) -> Option<Value> {
        use crate::types::Value;
        match (a, b) {
            (Value::Integer(a), Value::Integer(b)) => a.checked_sub(*b).map(Value::Integer),
            (Value::Float(a), Value::Float(b)) => Some(Value::Float(a - b)),
            (Value::Integer(a), Value::Float(b)) => Some(Value::Float(*a as f64 - b)),
            (Value::Float(a), Value::Integer(b)) => Some(Value::Float(a - *b as f64)),
            _ => None,
        }
    }
    fn positional_fast_mul(a: &Value, b: &Value) -> Option<Value> {
        use crate::types::Value;
        match (a, b) {
            (Value::Integer(a), Value::Integer(b)) => a.checked_mul(*b).map(Value::Integer),
            (Value::Float(a), Value::Float(b)) => Some(Value::Float(a * b)),
            (Value::Integer(a), Value::Float(b)) => Some(Value::Float(*a as f64 * b)),
            (Value::Float(a), Value::Integer(b)) => Some(Value::Float(a * *b as f64)),
            _ => None,
        }
    }
    fn positional_fast_div(a: &Value, b: &Value) -> Option<Value> {
        use crate::types::Value;
        match (a, b) {
            (Value::Float(a), Value::Float(b)) if *b != 0.0 => Some(Value::Float(a / b)),
            (Value::Float(a), Value::Integer(b)) if *b != 0 => Some(Value::Float(a / *b as f64)),
            (Value::Integer(a), Value::Float(b)) if *b != 0.0 => Some(Value::Float(*a as f64 / b)),
            (Value::Integer(a), Value::Integer(b)) if *b != 0 => {
                // Integer division truncates toward zero, matching SQL semantics
                a.checked_div(*b).map(Value::Integer)
            }
            _ => None,
        }
    }

    /// Fast UPDATE path: parses `UPDATE <table> SET col1=v1, col2=v2 WHERE pk = value`
    fn try_fast_update(&self, sql: &str) -> Result<Option<StreamingQueryResult>> {
        let trimmed = sql.trim_start();
        if !trimmed
            .as_bytes()
            .get(0..6)
            .map(|b| b.eq_ignore_ascii_case(b"UPDATE"))
            .unwrap_or(false)
        {
            return Ok(None);
        }
        let after_update = trimmed[6..].trim_start();

        // Extract table name
        let (table_name, after_table) = match after_update.find(|c: char| c.is_whitespace()) {
            Some(p) => (&after_update[..p], after_update[p..].trim_start()),
            None => return Ok(None),
        };
        if table_name.is_empty() {
            return Ok(None);
        }

        // Must have "SET" (word boundary at start)
        if !after_table
            .as_bytes()
            .get(0..3)
            .map(|b| b.eq_ignore_ascii_case(b"set"))
            .unwrap_or(false)
        {
            return Ok(None);
        }
        if after_table.len() > 3 && !after_table.as_bytes()[3].is_ascii_whitespace() {
            return Ok(None);
        }
        let after_set = after_table[3..].trim_start();

        // Find "WHERE" keyword (word boundary, search from end for rfind semantics)
        let where_pos = match after_set.as_bytes().windows(7).rposition(|w| {
            w[0].is_ascii_whitespace()
                && w[1..6].eq_ignore_ascii_case(b"where".as_ref())
                && w[6].is_ascii_whitespace()
        }) {
            Some(p) => p + 1,
            None => return Ok(None),
        };
        let set_part = after_set[..where_pos].trim();
        let after_where = after_set[where_pos + 5..].trim_start();

        // Parse WHERE: col = value (PK only)
        let eq_pos = match after_where.find('=') {
            Some(p) => p,
            None => return Ok(None),
        };
        let where_col = after_where[..eq_pos].trim();
        let where_val_str = after_where[eq_pos + 1..].trim();
        let where_value = match Self::parse_single_literal(where_val_str) {
            Some(v) => v,
            None => return Ok(None),
        };

        // Resolve schema — check this is a PK lookup
        let schema = match self.inner.table_registry.get_table(table_name) {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };
        let is_pk = schema
            .primary_key()
            .map(|pk| pk == where_col)
            .unwrap_or(false);
        // This fast path only accelerates `WHERE pk = value`. For non-PK WHERE
        // columns query_by_column would error (no index) — bail to the general
        // UPDATE path, which scans + filters positionally.
        if !is_pk {
            return Ok(None);
        }

        // Parse SET assignments: col1=v1, col2=v2 (store raw value strings)
        let mut set_items: Vec<(String, String)> = Vec::new();
        for pair in set_part.split(',') {
            let eq = match pair.find('=') {
                Some(p) => p,
                None => return Ok(None),
            };
            let col = pair[..eq].trim().to_string();
            let val_str = pair[eq + 1..].trim().to_string();
            set_items.push((col, val_str));
        }

        // Resolve PK → row_id
        let row_id = if schema.is_primary_key_auto_increment() {
            match &where_value {
                Value::Integer(id) if *id >= 0 => *id as RowId,
                _ => return Ok(None),
            }
        } else if self.inner.has_col_segment_store(table_name) {
            // ColSegmentStore tables: for non-AUTO_INCREMENT Integer PK, the PK
            // value maps to the row_id (see crud.rs insert path). This gives
            // O(log N) binary search via store.get() without needing the
            // pk_lookup cache or a disk index.
            // 🔑 Negative PK values are mapped to high u32 range (matching
            // crud.rs insert path) to avoid collision with next_row_id.
            match &where_value {
                Value::Integer(id) if *id >= 0 => *id as RowId,
                Value::Integer(id) => {
                    // Negative PK → high u32 range (0x80000000 + |pk_val|).
                    (0x8000_0000u64 | (*id as u64 & 0x7FFF_FFFF)) as RowId
                }
                _ => {
                    // Non-Integer PK: try pk_lookup cache.
                    let pk_key = crate::database::pk_cache::PkKey::from_value(&where_value);
                    if let Some(lookup) = self.inner.pk_lookup.get(table_name) {
                        match lookup.get_pk(&pk_key) {
                            Some(rid) => rid,
                            None => return Ok(None), // cache miss → fall through to scan
                        }
                    } else {
                        return Ok(None);
                    }
                }
            }
        } else {
            let pk_key = crate::database::pk_cache::PkKey::from_value(&where_value);
            if let Some(lookup) = self.inner.pk_lookup.get(table_name) {
                if let Some(rid) = lookup.get_pk(&pk_key) {
                    rid
                } else {
                    // Cache miss. Try column index first; if no index exists
                    // (ColSegmentStore tables don't auto-create a PK index),
                    // fall through to the general UPDATE path (full scan).
                    match self
                        .inner
                        .query_by_column(table_name, where_col, &where_value)
                    {
                        Ok(row_ids) => match row_ids.into_iter().next() {
                            Some(rid) => {
                                lookup.insert(pk_key, rid);
                                rid
                            }
                            None => {
                                return Ok(Some(StreamingQueryResult::Modification {
                                    affected_rows: 0,
                                }))
                            }
                        },
                        Err(_) => {
                            // No index — fall through to general path (full scan).
                            return Ok(None);
                        }
                    }
                }
            } else {
                // No pk_lookup cache for this table — fall through to general path.
                return Ok(None);
            }
        };

        // Load old row, resolve SET values, apply updates, write back
        let old_row = match self
            .inner
            .get_table_row_with_schema(table_name, row_id, &schema)?
        {
            Some(r) => r,
            None => {
                return Ok(Some(StreamingQueryResult::Modification {
                    affected_rows: 0,
                }))
            }
        };

        let mut new_row = old_row.clone();
        for (col_name, val_str) in &set_items {
            let cd = match schema.get_column(col_name) {
                Some(cd) => cd,
                None => {
                    return Err(StorageError::ColumnNotFound(format!(
                        "'{}' in table '{}'",
                        col_name, table_name
                    )));
                }
            };
            let val = match Self::parse_single_literal(val_str) {
                Some(v) => v,
                None => match Self::evaluate_simple_set_expr(val_str, &old_row, &schema) {
                    // 🔑 div-by-zero: positional_fast_div returns None when divisor
                    // is 0 (checked_div). Fall through to the full parser, which
                    // now propagates the DivisionByZero error instead of swallowing it.
                    Some(v) => v,
                    None => return Ok(None), // complex expression → fall through to full parser
                },
            };
            while new_row.len() <= cd.position {
                new_row.push(Value::Null);
            }
            new_row[cd.position] = val;
        }

        self.inner
            .update_row_in_table_with_schema(table_name, row_id, old_row, new_row, &schema)?;
        Ok(Some(StreamingQueryResult::Modification {
            affected_rows: 1,
        }))
    }

    /// Fast DELETE path: parses `DELETE FROM <table> WHERE pk = value`
    fn try_fast_delete(&self, sql: &str) -> Result<Option<StreamingQueryResult>> {
        let trimmed = sql.trim_start();
        if !trimmed
            .as_bytes()
            .get(0..6)
            .map(|b| b.eq_ignore_ascii_case(b"DELETE"))
            .unwrap_or(false)
        {
            return Ok(None);
        }
        let after_delete = trimmed[6..].trim_start();

        // Must have "FROM"
        if !after_delete
            .as_bytes()
            .get(0..4)
            .map(|b| b.eq_ignore_ascii_case(b"FROM"))
            .unwrap_or(false)
        {
            return Ok(None);
        }
        let after_from = after_delete[4..].trim_start();

        // Extract table name
        let (table_name, after_table) = match after_from.find(|c: char| c.is_whitespace()) {
            Some(p) => (&after_from[..p], after_from[p..].trim_start()),
            None => return Ok(None),
        };
        if table_name.is_empty() {
            return Ok(None);
        }

        // Check for "WHERE" (word boundary at start)
        if !after_table
            .as_bytes()
            .get(0..5)
            .map(|b| b.eq_ignore_ascii_case(b"where"))
            .unwrap_or(false)
        {
            return Ok(None);
        }
        if after_table.len() > 5 && !after_table.as_bytes()[5].is_ascii_whitespace() {
            return Ok(None);
        }
        let after_where = after_table[5..].trim_start();

        // Parse: col = value (PK only)
        let eq_pos = match after_where.find('=') {
            Some(p) => p,
            None => return Ok(None),
        };
        let col_name = after_where[..eq_pos].trim();
        let val_str = after_where[eq_pos + 1..].trim();
        let value = match Self::parse_single_literal(val_str) {
            Some(v) => v,
            None => return Ok(None),
        };

        // Resolve schema — PK check
        let schema = match self.inner.table_registry.get_table(table_name) {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };
        let is_pk = schema
            .primary_key()
            .map(|pk| pk == col_name)
            .unwrap_or(false);
        // This fast path only accelerates `WHERE pk = value`. For non-PK WHERE
        // columns query_by_column would error (no index) — bail to the general
        // DELETE path, which scans + filters positionally.
        if !is_pk {
            return Ok(None);
        }

        // Resolve PK → row_id
        let row_id = if schema.is_primary_key_auto_increment() {
            match &value {
                Value::Integer(id) if *id >= 0 => *id as RowId,
                _ => return Ok(None),
            }
        } else if self.inner.has_col_segment_store(table_name) {
            // ColSegmentStore tables: for non-AUTO_INCREMENT Integer PK, the PK
            // value maps to the row_id (see crud.rs insert path).
            // 🔑 Negative PK → high u32 range (matching crud.rs insert path).
            match &value {
                Value::Integer(id) if *id >= 0 => *id as RowId,
                Value::Integer(id) => (0x8000_0000u64 | (*id as u64 & 0x7FFF_FFFF)) as RowId,
                _ => {
                    let pk_key = crate::database::pk_cache::PkKey::from_value(&value);
                    if let Some(lookup) = self.inner.pk_lookup.get(table_name) {
                        match lookup.get_pk(&pk_key) {
                            Some(rid) => rid,
                            None => return Ok(None),
                        }
                    } else {
                        return Ok(None);
                    }
                }
            }
        } else {
            let pk_key = crate::database::pk_cache::PkKey::from_value(&value);
            if let Some(lookup) = self.inner.pk_lookup.get(table_name) {
                if let Some(rid) = lookup.get_pk(&pk_key) {
                    rid
                } else {
                    // Cache miss. Try column index; if no index, fall through
                    // to general DELETE path (full scan).
                    match self.inner.query_by_column(table_name, col_name, &value) {
                        Ok(row_ids) => match row_ids.into_iter().next() {
                            Some(rid) => {
                                lookup.insert(pk_key, rid);
                                rid
                            }
                            None => {
                                return Ok(Some(StreamingQueryResult::Modification {
                                    affected_rows: 0,
                                }))
                            }
                        },
                        Err(_) => {
                            return Ok(None);
                        }
                    }
                }
            } else {
                return Ok(None);
            }
        };

        // Load old row, then delete
        let old_row = match self
            .inner
            .get_table_row_with_schema(table_name, row_id, &schema)?
        {
            Some(r) => r,
            None => {
                return Ok(Some(StreamingQueryResult::Modification {
                    affected_rows: 0,
                }))
            }
        };

        self.inner
            .delete_row_from_table(table_name, row_id, old_row)?;
        Ok(Some(StreamingQueryResult::Modification {
            affected_rows: 1,
        }))
    }

    /// Parse a comma-separated list of SQL literals from a VALUES clause.
    /// Returns None if any value is not a simple literal.
    fn parse_literal_list(s: &str) -> Option<Vec<Value>> {
        let mut values = Vec::new();
        let mut chars = s.char_indices().peekable();
        let len = s.len();

        while chars.peek().is_some() {
            // Skip whitespace
            while let Some(&(_i, c)) = chars.peek() {
                if c.is_whitespace() {
                    chars.next();
                } else {
                    break;
                }
            }
            if chars.peek().is_none() {
                break;
            }

            let (start_idx, start_char) = chars.peek().copied().unwrap();

            if start_char == '\'' {
                // String literal
                chars.next(); // consume opening quote
                let mut text = String::new();
                loop {
                    match chars.next() {
                        Some((_, '\'')) => {
                            // SQL doubled-quote: '' → literal single quote
                            if chars.peek().map(|(_, c)| *c == '\'').unwrap_or(false) {
                                text.push('\'');
                                chars.next(); // consume second quote
                            } else {
                                break; // end of string
                            }
                        }
                        Some((_, '\\')) => match chars.next() {
                            Some((_, 'n')) => text.push('\n'),
                            Some((_, 't')) => text.push('\t'),
                            Some((_, 'r')) => text.push('\r'),
                            Some((_, '\\')) => text.push('\\'),
                            Some((_, '\'')) => text.push('\''),
                            Some((_, c)) => {
                                text.push('\\');
                                text.push(c);
                            }
                            None => return None,
                        },
                        Some((_, c)) => text.push(c),
                        None => return None,
                    }
                }
                values.push(Value::text(text));
            } else if start_char == '-' || start_char.is_ascii_digit() {
                // Number (integer or float)
                let mut num_str = String::new();
                if start_char == '-' {
                    num_str.push('-');
                    chars.next();
                }
                let mut has_dot = false;
                while let Some(&(_, c)) = chars.peek() {
                    if c.is_ascii_digit() {
                        num_str.push(c);
                        chars.next();
                    } else if c == '.' && !has_dot {
                        has_dot = true;
                        num_str.push(c);
                        chars.next();
                    } else {
                        break;
                    }
                }
                if num_str.is_empty() || num_str == "-" || num_str == "-." {
                    return None;
                }
                if has_dot {
                    values.push(Value::Float(num_str.parse().ok()?));
                } else {
                    values.push(Value::Integer(num_str.parse().ok()?));
                }
            } else if len - start_idx >= 4
                && s[start_idx..start_idx + 4].eq_ignore_ascii_case("NULL")
            {
                values.push(Value::Null);
                for _ in 0..4 {
                    chars.next();
                }
            } else if len - start_idx >= 4
                && s[start_idx..start_idx + 4].eq_ignore_ascii_case("TRUE")
            {
                values.push(Value::Bool(true));
                for _ in 0..4 {
                    chars.next();
                }
            } else if len - start_idx >= 5
                && s[start_idx..start_idx + 5].eq_ignore_ascii_case("FALSE")
            {
                values.push(Value::Bool(false));
                for _ in 0..5 {
                    chars.next();
                }
            } else {
                return None; // unsupported literal, fall back to full parser
            }

            // Skip whitespace and comma
            while let Some(&(_, c)) = chars.peek() {
                if c.is_whitespace() {
                    chars.next();
                } else {
                    break;
                }
            }
            if let Some(&(_, ',')) = chars.peek() {
                chars.next(); // consume comma
            }
        }

        Some(values)
    }

    // ============================================================================
    // 3. 事务管理
    // ============================================================================

    /// 开始新事务
    ///
    /// # Examples
    /// ```ignore
    /// let tx_id = db.begin_transaction()?;
    ///
    /// db.execute("INSERT INTO users VALUES (1, 'Alice', 25)")?;
    /// db.execute("INSERT INTO users VALUES (2, 'Bob', 30)")?;
    ///
    /// db.commit_transaction(tx_id)?;
    /// ```
    pub fn begin_transaction(&self) -> Result<u64> {
        let tx_id = self.inner.begin_transaction()?;
        // 🔑 Keep the SQL executor in sync so execute()/execute_prepared()
        // route writes through the transaction coordinator (buffered in
        // write_set until commit). Without this the executor writes directly
        // to storage and rollback cannot undo the writes.
        self.query_executor.begin_txn_context(tx_id);
        Ok(tx_id)
    }

    /// 提交事务
    ///
    /// # Examples
    /// ```ignore
    /// let tx_id = db.begin_transaction()?;
    /// db.execute("INSERT INTO users VALUES (1, 'Alice', 25)")?;
    /// db.commit_transaction(tx_id)?;
    /// ```
    pub fn commit_transaction(&self, tx_id: u64) -> Result<()> {
        self.inner.commit_transaction(tx_id)?;
        self.query_executor.clear_txn_context();
        Ok(())
    }

    /// 回滚事务
    ///
    /// # Examples
    /// ```ignore
    /// let tx_id = db.begin_transaction()?;
    /// db.execute("INSERT INTO users VALUES (1, 'Alice', 25)")?;
    /// db.rollback_transaction(tx_id)?; // 撤销所有修改
    /// ```
    pub fn rollback_transaction(&self, tx_id: u64) -> Result<()> {
        // 🔑 Replay the undo log BEFORE delegating to the coordinator. UPDATE
        // and DELETE write directly to storage during the transaction (recording
        // old values in the undo log). Without this replay, rollback would
        // silently fail to undo those changes — the coordinator's rollback()
        // only discards the write_set and clears bookkeeping.
        self.query_executor.replay_undo_log(tx_id);
        self.inner.rollback_transaction(tx_id)?;
        self.query_executor.clear_txn_context();
        Ok(())
    }

    /// 创建保存点（事务内的检查点）
    ///
    /// # Examples
    /// ```ignore
    /// let tx_id = db.begin_transaction()?;
    ///
    /// db.execute("INSERT INTO users VALUES (1, 'Alice', 25)")?;
    /// db.savepoint(tx_id, "sp1")?;
    ///
    /// db.execute("INSERT INTO users VALUES (2, 'Bob', 30)")?;
    /// db.rollback_to_savepoint(tx_id, "sp1")?; // 只回滚 Bob 的插入
    ///
    /// db.commit_transaction(tx_id)?;
    /// ```
    pub fn savepoint(&self, tx_id: u64, name: &str) -> Result<()> {
        self.inner.create_savepoint(tx_id, name.to_string())
    }

    /// 回滚到保存点
    pub fn rollback_to_savepoint(&self, tx_id: u64, name: &str) -> Result<()> {
        self.inner.rollback_to_savepoint(tx_id, name).map(|_| ())
    }

    /// 释放保存点
    pub fn release_savepoint(&self, tx_id: u64, name: &str) -> Result<()> {
        self.inner.release_savepoint(tx_id, name)
    }

    // ============================================================================
    // 4. 批量操作（高性能）
    // ============================================================================

    /// 批量插入行（比逐行插入快10-20倍）
    ///
    /// **注意：** 此方法接受底层 `Row` 类型（`Vec<Value>`），如果需要使用 HashMap，请使用 `batch_insert_map()`。
    ///
    /// # Examples
    /// ```ignore
    /// use motedb::types::{Value, Row};
    ///
    /// let mut rows = Vec::new();
    /// for i in 0..1000 {
    ///     let row = vec![
    ///         Value::Integer(i),
    ///         Value::Text(format!("User{}", i)),
    ///     ];
    ///     rows.push(row);
    /// }
    ///
    /// let row_ids = db.batch_insert("users", rows)?;
    /// println!("Inserted {} rows", row_ids.len());
    /// ```
    pub fn batch_insert(&self, table_name: &str, rows: Vec<Row>) -> Result<Vec<RowId>> {
        self.inner.batch_insert_rows_to_table(table_name, rows)
    }

    /// 批量插入行（使用 HashMap，比逐行插入快10-20倍）
    ///
    /// 这是 `batch_insert()` 的友好版本，接受 `HashMap<String, Value>` 格式的行数据。
    ///
    /// # Examples
    /// ```ignore
    /// use motedb::types::{Value, SqlRow};
    /// use std::collections::HashMap;
    ///
    /// let mut rows = Vec::new();
    /// for i in 0..1000 {
    ///     let mut row = HashMap::new();
    ///     row.insert("id".to_string(), Value::Integer(i));
    ///     row.insert("name".to_string(), Value::Text(format!("User{}", i)));
    ///     rows.push(row);
    /// }
    ///
    /// let row_ids = db.batch_insert_map("users", rows)?;
    /// println!("Inserted {} rows", row_ids.len());
    /// ```
    pub fn batch_insert_map(&self, table_name: &str, sql_rows: Vec<SqlRow>) -> Result<Vec<RowId>> {
        // 获取表结构
        let schema = self.inner.get_table_schema(table_name)?;

        // 将 SqlRow (HashMap) 转换为 Row (Vec<Value>)
        let rows: Result<Vec<Row>> = sql_rows
            .into_iter()
            .map(|sql_row| crate::sql::row_converter::sql_row_to_row(&sql_row, &schema))
            .collect();

        // 🚀 使用新的 batch_insert_rows_to_table (支持增量索引更新)
        self.inner.batch_insert_rows_to_table(table_name, rows?)
    }

    pub fn batch_insert_with_vectors_map(
        &self,
        table_name: &str,
        sql_rows: Vec<SqlRow>,
        vector_columns: &[&str],
    ) -> Result<Vec<RowId>> {
        let schema = self.inner.get_table_schema(table_name)?;
        let rows: Result<Vec<Row>> = sql_rows
            .into_iter()
            .map(|sql_row| crate::sql::row_converter::sql_row_to_row(&sql_row, &schema))
            .collect();
        self.batch_insert_with_vectors(table_name, rows?, vector_columns)
    }

    /// 批量插入带向量的数据（自动构建向量索引）
    ///
    /// **注意：** 此方法接受底层 `Row` 类型（`Vec<Value>`），如果需要使用 HashMap，请使用 `batch_insert_with_vectors_map()`。
    ///
    /// # Examples
    /// ```ignore
    /// use motedb::types::{Value, Row};
    ///
    /// let mut rows = Vec::new();
    /// for i in 0..1000 {
    ///     let row = vec![
    ///         Value::Integer(i),
    ///         Value::Vector(vec![0.1; 128]),
    ///     ];
    ///     rows.push(row);
    /// }
    ///
    /// let row_ids = db.batch_insert_with_vectors("documents", rows, &["embedding"])?;
    /// ```
    pub fn batch_insert_with_vectors(
        &self,
        table_name: &str,
        rows: Vec<Row>,
        _vector_columns: &[&str],
    ) -> Result<Vec<RowId>> {
        // 🚀 使用新的 batch_insert_rows_to_table (已包含向量索引增量更新)
        self.inner.batch_insert_rows_to_table(table_name, rows)
    }

    /// 批量插入带向量的数据（使用 HashMap，自动构建向量索引）
    ///
    /// # Examples
    /// ```ignore
    /// use motedb::types::{Value, SqlRow};
    /// use std::collections::HashMap;
    ///
    /// let mut rows = Vec::new();
    /// for i in 0..1000 {
    ///     let mut row = HashMap::new();
    ///     row.insert("id".to_string(), Value::Integer(i));
    ///     row.insert("embedding".to_string(), Value::Vector(vec![0.1; 128]));
    ///     rows.push(row);
    /// }
    ///
    /// let row_ids = db.batch_insert_with_vectors_map("documents", rows, &["embedding"])?;
    /// ```
    // ============================================================================
    // 5. 索引管理
    // ============================================================================

    /// 创建列索引（用于快速等值/范围查询）
    ///
    /// # Examples
    /// ```ignore
    /// // 创建列索引后，WHERE email = '...' 查询速度提升40倍
    /// db.create_column_index("users", "email")?;
    ///
    /// // 查询会自动使用索引
    /// let results = db.query("SELECT * FROM users WHERE email = 'alice@example.com'")?;
    /// ```
    pub fn create_column_index(&self, table_name: &str, column_name: &str) -> Result<()> {
        self.inner.create_column_index(table_name, column_name)
    }

    /// 创建向量索引（用于KNN相似度搜索）
    ///
    /// # Examples
    /// ```ignore
    /// // 为128维向量创建索引
    /// db.create_vector_index("docs_embedding", 128)?;
    ///
    /// // SQL 向量搜索
    /// let query = "SELECT * FROM docs
    ///              ORDER BY embedding <-> [0.1, 0.2, ...]
    ///              LIMIT 10";
    /// let results = db.query(query)?;
    /// ```
    pub fn create_vector_index(&self, index_name: &str, dimension: usize) -> Result<()> {
        self.inner.create_vector_index(index_name, dimension, None)
    }

    /// 创建全文索引（用于BM25文本搜索）
    ///
    /// # Examples
    /// ```ignore
    /// // 创建全文索引
    /// db.create_text_index("articles_content")?;
    ///
    /// // SQL 全文搜索
    /// let results = db.query(
    ///     "SELECT * FROM articles WHERE MATCH(content, 'rust database')"
    /// )?;
    /// ```
    pub fn create_text_index(&self, index_name: &str) -> Result<()> {
        self.inner.create_text_index(index_name)
    }

    // ============================================================================
    // 6. 查询 API（使用索引）
    // ============================================================================

    /// 按列值查询（使用列索引，等值查询）
    ///
    /// # Examples
    /// ```ignore
    /// use motedb::Value;
    ///
    /// // 前提：已创建列索引
    /// db.create_column_index("users", "email")?;
    ///
    /// // 快速查询（使用索引）
    /// let row_ids = db.query_by_column(
    ///     "users",
    ///     "email",
    ///     &Value::Text("alice@example.com".into())
    /// )?;
    /// ```
    pub fn query_by_column(
        &self,
        table_name: &str,
        column_name: &str,
        value: &Value,
    ) -> Result<Vec<RowId>> {
        self.inner.query_by_column(table_name, column_name, value)
    }

    /// 按列范围查询（使用列索引）
    ///
    /// # Examples
    /// ```ignore
    /// use motedb::Value;
    ///
    /// // 查询年龄在 20-30 之间的用户
    /// let row_ids = db.query_by_column_range(
    ///     "users",
    ///     "age",
    ///     &Value::Integer(20),
    ///     &Value::Integer(30)
    /// )?;
    /// ```
    pub fn query_by_column_range(
        &self,
        table_name: &str,
        column_name: &str,
        start: &Value,
        end: &Value,
    ) -> Result<Vec<RowId>> {
        self.inner
            .query_by_column_range(table_name, column_name, start, end)
    }

    /// 按列范围查询（精确控制边界，使用列索引）
    ///
    /// ## 边界语义
    /// - `start_inclusive`: 下界是否包含（>= vs >）
    /// - `end_inclusive`: 上界是否包含（<= vs <）
    ///
    /// # Examples
    /// ```ignore
    /// use motedb::Value;
    ///
    /// // 查询 id >= 100 AND id < 200 (左闭右开)
    /// let row_ids = db.query_by_column_between(
    ///     "users",
    ///     "id",
    ///     &Value::Integer(100), true,
    ///     &Value::Integer(200), false
    /// )?;
    /// ```
    pub fn query_by_column_between(
        &self,
        table_name: &str,
        column_name: &str,
        start: &Value,
        start_inclusive: bool,
        end: &Value,
        end_inclusive: bool,
    ) -> Result<Vec<RowId>> {
        self.inner.query_by_column_between(
            table_name,
            column_name,
            start,
            start_inclusive,
            end,
            end_inclusive,
        )
    }

    /// 向量KNN搜索
    ///
    /// # Examples
    /// ```ignore
    /// // 查找最相似的10个向量
    /// let query_vec = vec![0.1; 128];
    /// let results = db.vector_search("docs_embedding", &query_vec, 10)?;
    ///
    /// for (row_id, distance) in results {
    ///     println!("RowID: {}, Distance: {}", row_id, distance);
    /// }
    /// ```
    pub fn vector_search(
        &self,
        index_name: &str,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<(RowId, f32)>> {
        self.inner.vector_search(index_name, query, k)
    }

    /// 🔑 J1 hybrid retrieval: fuse a BM25-ranked text list and a vector KNN
    /// list with Reciprocal Rank Fusion:
    ///     rrf(d) = Σ_lists 1 / (rrf_k + rank_in_list)      (rank 1-based)
    /// RRF needs no score calibration between the two engines (the standard
    /// industry choice). Candidate depth is k × fetch_mult from each list —
    /// docs that rank low on BOTH lists can still surface; docs absent from
    /// a list simply contribute 0 from it.
    ///
    /// Returns up to k hits, descending by fused score, with the per-list
    /// scores attached when a list contained the doc. Rows are fetched for
    /// the final hits (columnar batch) — the Python binding projects them
    /// straight into dicts.
    pub fn hybrid_search(
        &self,
        text_index: &str,
        text_query: &str,
        vector_index: &str,
        query_vector: &[f32],
        k: usize,
        rrf_k: usize,
        fetch_mult: usize,
    ) -> Result<Vec<HybridHit>> {
        if k == 0 {
            return Ok(Vec::new());
        }
        let fetch = (k.saturating_mul(fetch_mult.max(1))).clamp(16, 512);
        let bm25_list = self.text_search_ranked(text_index, text_query, fetch)?;
        let vec_list = self.vector_search(vector_index, query_vector, fetch)?;

        let rrf_denom = rrf_k.max(1) as f32;
        // rank → contribution: rank 1 → 1/(rrf_k+1), rank r → 1/(rrf_k+r).
        let mut scores: std::collections::HashMap<RowId, (f32, Option<f32>, Option<f32>)> =
            std::collections::HashMap::new();
        for (rank, (rid, score)) in bm25_list.iter().enumerate() {
            let e = scores.entry(*rid).or_insert((0.0, None, None));
            e.0 += 1.0 / (rrf_denom + rank as f32 + 1.0);
            e.1 = Some(*score);
        }
        for (rank, (rid, dist)) in vec_list.iter().enumerate() {
            let e = scores.entry(*rid).or_insert((0.0, None, None));
            e.0 += 1.0 / (rrf_denom + rank as f32 + 1.0);
            e.2 = Some(*dist);
        }
        let mut hits: Vec<HybridHit> = scores
            .into_iter()
            .map(|(row_id, (rrf, bm25, distance))| HybridHit {
                row_id,
                rrf,
                bm25,
                distance,
            })
            .collect();
        // Descending fused score; deterministic tiebreak by row_id asc.
        hits.sort_by(|a, b| {
            b.rrf
                .partial_cmp(&a.rrf)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.row_id.cmp(&b.row_id))
        });
        hits.truncate(k);
        Ok(hits)
    }

    /// 🔑 J1: fetch full rows for hybrid-search hits (columnar batch, order
    /// preserved). Returns (column_names, rows) — rows carry the original
    /// values; scores live in HybridHit.
    pub fn hybrid_search_rows(
        &self,
        text_index: &str,
        text_query: &str,
        vector_index: &str,
        query_vector: &[f32],
        k: usize,
        rrf_k: usize,
        fetch_mult: usize,
    ) -> Result<(Vec<String>, Vec<Row>, Vec<HybridHit>)> {
        let hits = self.hybrid_search(
            text_index,
            text_query,
            vector_index,
            query_vector,
            k,
            rrf_k,
            fetch_mult,
        )?;
        // Table name comes from the vector index's registration (text and
        // vector indexes must target the same table for fusion to make sense).
        let resolved = self.inner.index_registry.resolve_index_name(vector_index);
        let table_name: String = match &resolved {
            Some((t, _)) => t.clone(),
            None => vector_index
                .split('_')
                .next()
                .unwrap_or(vector_index)
                .to_string(),
        };
        let schema = self.inner.get_table_schema(&table_name)?;
        let ids: Vec<RowId> = hits.iter().map(|h| h.row_id).collect();
        let batch = self.inner.get_table_rows_batch(&table_name, &ids)?;
        let mut lookup: std::collections::HashMap<RowId, &Row> =
            std::collections::HashMap::with_capacity(batch.len());
        for (rid, r) in &batch {
            if let Some(row) = r {
                lookup.insert(*rid, row);
            }
        }
        let rows: Vec<Row> = hits
            .iter()
            .filter_map(|h| lookup.get(&h.row_id).cloned().cloned())
            .collect();
        let names: Vec<String> = schema.column_names();
        Ok((names, rows, hits))
    }

    /// 全文搜索（BM25排序）
    ///
    /// # Examples
    /// ```ignore
    /// // 搜索包含关键词的文档（BM25排序）
    /// let results = db.text_search_ranked("articles_content", "rust database", 10)?;
    ///
    /// for (row_id, score) in results {
    ///     println!("RowID: {}, BM25 Score: {}", row_id, score);
    /// }
    /// ```
    pub fn text_search_ranked(
        &self,
        index_name: &str,
        query: &str,
        top_k: usize,
    ) -> Result<Vec<(RowId, f32)>> {
        self.inner.text_search_ranked(index_name, query, top_k)
    }

    /// Phrase search: documents containing the exact word sequence.
    pub fn text_search_phrase(&self, index_name: &str, phrase: &str) -> Result<Vec<RowId>> {
        self.inner.text_search_phrase(index_name, phrase)
    }

    /// 时间序列范围查询
    ///
    /// # Examples
    /// ```ignore
    /// // 查询指定时间范围内的记录
    /// let start_ts = 1609459200; // 2021-01-01 00:00:00
    /// let end_ts = 1640995200;   // 2022-01-01 00:00:00
    /// let row_ids = db.query_timestamp_range(start_ts, end_ts)?;
    /// ```
    pub fn query_timestamp_range(&self, start: i64, end: i64) -> Result<Vec<RowId>> {
        self.inner.query_timestamp_range(start, end)
    }

    // ============================================================================
    // 7. 统计信息和监控
    // ============================================================================

    /// 获取向量索引统计信息
    ///
    /// # Examples
    /// ```ignore
    /// let stats = db.vector_index_stats("docs_embedding")?;
    /// println!("向量数量: {}", stats.vector_count);
    /// println!("平均邻居数: {}", stats.avg_neighbors);
    /// ```
    pub fn vector_index_stats(&self, index_name: &str) -> Result<VectorIndexStats> {
        self.inner.vector_index_stats(index_name)
    }

    // ==================== i-Octree 3D Spatial Index (Embodied Intelligence) ====================

    /// Create an i-Octree 3D spatial index for point cloud data
    ///
    /// Use for SLAM, robotics, and 3D perception workloads.
    pub fn create_ioctree_index(&self, index_name: &str) -> Result<()> {
        self.inner.create_ioctree_index(index_name)
    }

    /// 3D KNN query: find k nearest neighbors
    ///
    /// Returns `(row_id, distance)` pairs sorted by distance.
    pub fn ioctree_knn_search(
        &self,
        index_name: &str,
        point: &crate::types::Point3D,
        k: usize,
    ) -> Result<Vec<(RowId, f64)>> {
        self.inner.ioctree_knn_query(index_name, point, k)
    }

    /// 3D radius search: find all points within radius
    /// 获取事务统计信息
    ///
    /// # Examples
    /// ```ignore
    /// let stats = db.transaction_stats();
    /// println!("活跃事务数: {}", stats.active_transactions);
    /// println!("已提交事务数: {}", stats.committed_transactions);
    /// ```
    pub fn transaction_stats(&self) -> TransactionStats {
        self.inner.transaction_stats()
    }

    // ============================================================================
    // 8. CRUD 操作（底层 API，通常使用 SQL 更方便）
    // ============================================================================

    /// 插入行（底层API，推荐使用 SQL INSERT）
    ///
    /// **注意：** 此方法接受底层 `Row` 类型（`Vec<Value>`），如果需要使用 HashMap，请使用 `insert_row_map()`。
    ///
    /// # Examples
    /// ```ignore
    /// use motedb::types::{Value, Row};
    ///
    /// let row = vec![
    ///     Value::Integer(1),
    ///     Value::Text("Alice".into()),
    /// ];
    ///
    /// let row_id = db.insert_row("users", row)?;
    /// ```
    pub fn insert_row(&self, table_name: &str, row: Row) -> Result<RowId> {
        self.inner.insert_row_to_table(table_name, row)
    }

    /// Insert a row within a transaction. The row is buffered and only written
    /// to storage when the transaction commits. Use this instead of `insert_row`
    /// when operating inside a transaction.
    pub fn insert_row_with_txn(&self, table_name: &str, txn_id: u64, row: Row) -> Result<RowId> {
        self.inner.insert_row_with_txn(table_name, txn_id, row)
    }

    /// 插入行（使用 HashMap）
    ///
    /// 这是 `insert_row()` 的友好版本，接受 `HashMap<String, Value>` 格式的行数据。
    ///
    /// # Examples
    /// ```ignore
    /// use motedb::types::{Value, SqlRow};
    /// use std::collections::HashMap;
    ///
    /// let mut row = HashMap::new();
    /// row.insert("id".to_string(), Value::Integer(1));
    /// row.insert("name".to_string(), Value::Text("Alice".into()));
    ///
    /// let row_id = db.insert_row_map("users", row)?;
    /// ```
    pub fn insert_row_map(&self, table_name: &str, sql_row: SqlRow) -> Result<RowId> {
        // 获取表结构
        let schema = self.inner.get_table_schema(table_name)?;

        // 将 SqlRow (HashMap) 转换为 Row (Vec<Value>)
        let row = crate::sql::row_converter::sql_row_to_row(&sql_row, &schema)?;

        self.inner.insert_row_to_table(table_name, row)
    }

    /// 获取行（底层API，推荐使用 SQL SELECT）
    pub fn get_row(&self, table_name: &str, row_id: RowId) -> Result<Option<Row>> {
        self.inner.get_table_row(table_name, row_id)
    }

    /// 获取行（返回 HashMap 格式）
    ///
    /// # Examples
    /// ```ignore
    /// if let Some(row) = db.get_row_map("users", 1)? {
    ///     println!("Name: {:?}", row.get("name"));
    /// }
    /// ```
    pub fn get_row_map(&self, table_name: &str, row_id: RowId) -> Result<Option<SqlRow>> {
        if let Some(row) = self.inner.get_table_row(table_name, row_id)? {
            let schema = self.inner.get_table_schema(table_name)?;
            Ok(Some(crate::sql::row_converter::row_to_sql_row(
                &row, &schema,
            )?))
        } else {
            Ok(None)
        }
    }

    /// 更新行（底层API，推荐使用 SQL UPDATE）
    pub fn update_row(&self, table_name: &str, row_id: RowId, new_row: Row) -> Result<()> {
        // 先获取旧行
        let old_row = self
            .inner
            .get_table_row(table_name, row_id)?
            .ok_or_else(|| {
                crate::StorageError::InvalidData(format!(
                    "Row {} not found in table '{}'",
                    row_id, table_name
                ))
            })?;
        self.inner
            .update_row_in_table(table_name, row_id, old_row, new_row)
    }

    /// 删除行（底层API，推荐使用 SQL DELETE）
    pub fn delete_row(&self, table_name: &str, row_id: RowId) -> Result<()> {
        // 先获取旧行
        let old_row = self
            .inner
            .get_table_row(table_name, row_id)?
            .ok_or_else(|| {
                crate::StorageError::InvalidData(format!(
                    "Row {} not found in table '{}'",
                    row_id, table_name
                ))
            })?;
        self.inner
            .delete_row_from_table(table_name, row_id, old_row)
    }
}

// 自动在 Drop 时关闭数据库
impl Drop for Database {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            warn_log!("[Database::Drop] close() failed: {}", e);
        }
    }
}
