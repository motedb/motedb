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
use lru::LruCache;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

/// Pre-computed metadata for fast PK SELECT execution.
struct FastPkMeta {
    /// "select", "update", or "delete"
    stmt_type: &'static str,
    table_name: String,
    param_idx: usize,
    /// Only for SELECT: whether it's SELECT *
    is_star: bool,
    /// Only for SELECT: column positions to project
    select_col_positions: Vec<usize>,
    /// Only for UPDATE: (col_position, param_idx) for SET col = ?
    set_param_positions: Vec<(usize, usize)>,
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
    stmt_cache: Arc<parking_lot::RwLock<LruCache<String, CachedStmt>>>,
    /// Reused QueryExecutor — avoids per-call allocation of pattern_cache, optimizer state
    query_executor: crate::sql::QueryExecutor,
}

impl Database {
    // ============================================================================
    // 1. 数据库生命周期管理
    // ============================================================================

    /// 创建新数据库
    ///
    /// # Examples
    /// ```ignore
    /// let db = Database::create("data.mote")?;
    /// ```
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let inner = Arc::new(MoteDB::create(path)?);
        let query_executor = crate::sql::QueryExecutor::new(inner.clone());
        Ok(Self {
            inner,
            stmt_cache: Arc::new(parking_lot::RwLock::new(LruCache::new(
                NonZeroUsize::new(256).unwrap(),
            ))),
            query_executor,
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
        let inner = Arc::new(MoteDB::create_with_config(path, config)?);
        let query_executor = crate::sql::QueryExecutor::new(inner.clone());
        Ok(Self {
            inner,
            stmt_cache: Arc::new(parking_lot::RwLock::new(LruCache::new(
                NonZeroUsize::new(256).unwrap(),
            ))),
            query_executor,
        })
    }

    /// 打开已存在的数据库
    ///
    /// # Examples
    /// ```ignore
    /// let db = Database::open("data.mote")?;
    /// ```
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let inner = Arc::new(MoteDB::open(path)?);
        let query_executor = crate::sql::QueryExecutor::new(inner.clone());
        Ok(Self {
            inner,
            stmt_cache: Arc::new(parking_lot::RwLock::new(LruCache::new(
                NonZeroUsize::new(256).unwrap(),
            ))),
            query_executor,
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
        let inner = Arc::new(MoteDB::open_with_config(path, config)?);
        let query_executor = crate::sql::QueryExecutor::new(inner.clone());
        Ok(Self {
            inner,
            stmt_cache: Arc::new(parking_lot::RwLock::new(LruCache::new(
                NonZeroUsize::new(256).unwrap(),
            ))),
            query_executor,
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
    pub fn checkpoint(&self) -> Result<()> {
        self.inner.checkpoint()
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

        // Signal background threads to stop
        self.inner.signal_background_threads_stop();

        // Wait for threads to actually finish before checkpoint to prevent
        // deadlock (threads may hold locks that checkpoint needs).
        if !self
            .inner
            .wait_for_background_threads_stop(std::time::Duration::from_secs(30))
        {
            warn_log!("[close] Background threads did not stop within timeout");
        }

        // 🚀 Flush ColSegmentStore buffers BEFORE checkpoint. Without this,
        // in-memory INSERT data (the write buffer) is lost on close — the
        // large_batch_durability bug (10000 rows → 5000 after reopen). The
        // second batch was in the buffer, never flushed, dropped on close.
        for entry in self.inner.col_segment_stores.iter() {
            let _ = entry.value().flush_buffer();
            // Compact to a single segment so the reopen sees all data in one place.
            while entry.value().segment_count() >= 2 {
                if entry.value().force_compact_all().is_err() {
                    break;
                }
            }
        }

        let result = self.inner.checkpoint_full();
        self.inner
            .is_closed
            .store(true, std::sync::atomic::Ordering::Release);
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
        let upper = trimmed.to_ascii_uppercase();
        if upper.starts_with("CHECKPOINT") {
            self.inner.checkpoint()?;
            return Ok(StreamingQueryResult::Modification { affected_rows: 0 });
        }
        if upper.starts_with("VACUUM") {
            self.inner.vacuum()?;
            return Ok(StreamingQueryResult::Modification { affected_rows: 0 });
        }

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

        // 🚀 Prepared statement cache: skip re-parsing on repeated queries
        let statement: Arc<Statement> = {
            let read_cache = self.stmt_cache.read();
            if let Some(cached) = read_cache.peek(sql) {
                Arc::clone(&cached.stmt)
            } else {
                drop(read_cache);
                let mut cache = self.stmt_cache.write();
                if let Some(cached) = cache.get(sql) {
                    Arc::clone(&cached.stmt)
                } else {
                    let mut lexer = Lexer::new(sql);
                    let tokens = lexer.tokenize()?;
                    let mut parser = Parser::new(tokens);
                    let stmt = parser.parse()?;
                    let stmt_arc = Arc::new(stmt);
                    cache.put(
                        sql.to_string(),
                        CachedStmt {
                            stmt: Arc::clone(&stmt_arc),
                            fast_pk: None,
                        },
                    );
                    stmt_arc
                }
            }
        };

        // Reuse shared QueryExecutor (preserves pattern_cache + optimizer state)
        self.query_executor.reset_last_insert_id();
        self.query_executor.execute_streaming_ref(&statement)
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

        // Get or parse the statement — check for cached fast PK metadata
        let (statement, cached_fast_pk): (Arc<Statement>, bool) = {
            let read_cache = self.stmt_cache.read();
            if let Some(cached) = read_cache.peek(sql) {
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
                drop(read_cache);
                let mut cache = self.stmt_cache.write();
                if let Some(cached) = cache.get(sql) {
                    if let Some(ref meta) = cached.fast_pk {
                        if let Some(result) = self.execute_fast_pk_with_meta(meta, &params)? {
                            return Ok(result);
                        }
                        // PK cache miss — fall through to full path.
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
                    cache.put(
                        sql.to_string(),
                        CachedStmt {
                            stmt: Arc::clone(&stmt_arc),
                            fast_pk: None,
                        },
                    );
                    (stmt_arc, true)
                }
            }
        };

        // 🚀 First call (no fast_pk yet): detect pattern, cache metadata, execute immediately
        if cached_fast_pk {
            if let Some(meta) = Self::detect_fast_pk_pattern(&statement, &self.inner)? {
                // Execute using the metadata we just computed (no extra lock)
                if let Some(result) = self.execute_fast_pk_with_meta(&meta, &params)? {
                    // Cache for future calls (write lock only, no read-back)
                    {
                        let mut cache = self.stmt_cache.write();
                        if let Some(cached) = cache.get_mut(sql) {
                            cached.fast_pk = Some(meta);
                        }
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
            || matches!(statement.as_ref(), Statement::Select(s) if s.where_clause.is_some())
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

    /// Detect if a statement is a simple PK SELECT pattern.
    /// Returns pre-computed FastPkMeta if it matches.
    fn detect_fast_pk_pattern(statement: &Statement, db: &MoteDB) -> Result<Option<FastPkMeta>> {
        use crate::sql::ast::{BinaryOperator, Expr, SelectColumn, Statement as S, TableRef};

        let (stmt_type, table_ref, where_expr, select_cols) = match statement {
            S::Select(s) => (
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

        let is_star = select_cols
            .is_some_and(|cols| cols.len() == 1 && matches!(cols[0], SelectColumn::Star));

        let select_col_positions: Vec<usize> = if let Some(cols) = select_cols {
            if is_star {
                vec![]
            } else {
                cols.iter()
                    .filter_map(|col_spec| {
                        let cname = match col_spec {
                            SelectColumn::Column(n) => n.as_str(),
                            SelectColumn::ColumnWithAlias(n, _) => n.as_str(),
                            _ => return None,
                        };
                        let lookup = if cname.contains('.') {
                            cname.rsplit('.').next().unwrap_or(cname)
                        } else {
                            cname
                        };
                        schema.get_column_position(lookup)
                    })
                    .collect()
            }
        } else {
            vec![]
        };

        // For UPDATE: detect SET col = ? patterns
        let set_param_positions: Vec<(usize, usize)> = if stmt_type == "update" {
            if let S::Update(s) = statement {
                s.assignments
                    .iter()
                    .filter_map(|(col_name, expr)| {
                        if let Expr::Parameter(idx) = expr {
                            schema.get_column_position(col_name).map(|pos| (pos, *idx))
                        } else {
                            None
                        }
                    })
                    .collect()
            } else {
                vec![]
            }
        } else {
            vec![]
        };

        Ok(Some(FastPkMeta {
            stmt_type,
            table_name: table_ref.to_string(),
            param_idx,
            is_star,
            select_col_positions,
            set_param_positions,
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
        let pk_value = match params.get(meta.param_idx - 1) {
            Some(v) => v,
            None => {
                return Err(crate::error::MoteDBError::InvalidArgument(format!(
                    "Parameter ?{} is unbound",
                    meta.param_idx
                )))
            }
        };

        // Resolve PK → row_id.
        // For AUTO_INCREMENT PKs the value IS the row_id, so we can always
        // resolve. For non-AUTO_INCREMENT PKs we depend on the pk_lookup cache,
        // which is lazily populated and may be empty after recovery (open) — a
        // cache miss does NOT mean the row doesn't exist. In that case return
        // None so the caller falls back to the full executor path (which scans
        // the columnar store) instead of returning a wrong-typed/empty result.
        let row_id = if meta.is_auto_increment {
            match pk_value {
                Value::Integer(id) if *id >= 0 => *id as RowId,
                _ => {
                    return Ok(Some(StreamingQueryResult::Modification {
                        affected_rows: 0,
                    }))
                }
            }
        } else {
            let pk_key = crate::database::pk_cache::PkKey::from_value(pk_value);
            match self
                .inner
                .pk_lookup
                .get(&meta.table_name)
                .and_then(|lookup| lookup.get_pk(&pk_key))
            {
                Some(rid) => rid,
                None => return Ok(None), // PK cache miss — fall back to full path
            }
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
                // SELECT
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
                    columns: (*meta.column_names).clone(),
                    rows: result_vec,
                }))
            }
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
                    let cols: Vec<String> =
                        col_str.split(',').map(|s| s.trim().to_string()).collect();
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
                match crate::sql::row_converter::values_to_row_schema_order(&values, &schema) {
                    Ok(r) => r,
                    Err(_) => return Ok(None),
                }
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

        // 🆕 S9: ColSegmentStore tables use multi-segment storage. The fast
        // SELECT path below fetches rows via column index + get_table_row,
        // which works, but routing through the optimizer/full-scan path is
        // simpler and already correct for these tables. Bail out here so the
        // query goes through execute_full_scan_streaming → ColSegmentStore.
        if self.inner.has_col_segment_store(table_name) {
            return Ok(None);
        }

        // Finalize write buffer (with merge) so columnar paths see all data
        self.inner.finalize_columnar_buffer(table_name);

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

        if !is_pk {
            // 🚀 Columnar SSTable fast path: use columnar filtered scan instead of
            // per-row batch fetch. Much faster for low-selectivity filters.
            if self.inner.columnar_sstables.contains_key(table_name) {
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
                let row_ids_arc = index_ref.value().get_arc(&value)?;

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

    /// Parse a single SQL literal (integer, float, string, or simple expr like col + lit).
    /// Returns None if the value isn't a literal (falls through to full parser).
    /// Check if a SELECT column expression contains aggregate functions.
    /// Returns true if any of COUNT, SUM, AVG, MIN, MAX are found (case-insensitive).
    fn contains_aggregate_function(select_part: &str) -> bool {
        let upper = select_part.to_uppercase();
        for keyword in &["COUNT", "SUM", "AVG", "MIN", "MAX"] {
            if upper.contains(keyword) {
                // Verify it's a word, not part of a column name like "max_value"
                // Simple check: preceded by non-alphanumeric or start-of-string
                if let Some(pos) = upper.find(keyword) {
                    let before_ok = pos == 0 || {
                        let b = upper.as_bytes()[pos - 1];
                        !b.is_ascii_alphanumeric() && b != b'_'
                    };
                    let after_pos = pos + keyword.len();
                    let after_ok = after_pos >= upper.len() || {
                        let b = upper.as_bytes()[after_pos];
                        !b.is_ascii_alphanumeric() && b != b'_'
                    };
                    if before_ok && after_ok {
                        return true;
                    }
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
            (Value::Integer(a), Value::Integer(b)) => a.checked_add(*b).map(Value::Integer),
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
            // value IS the row_id (see crud.rs insert path). This gives O(log N)
            // binary search via store.get() without needing the pk_lookup cache
            // or a disk index.
            match &where_value {
                Value::Integer(id) if *id >= 0 => *id as RowId,
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
            // value IS the row_id (see crud.rs insert path).
            match &value {
                Value::Integer(id) if *id >= 0 => *id as RowId,
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
        self.inner.begin_transaction()
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
        self.inner.commit_transaction(tx_id)
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
        self.inner.rollback_transaction(tx_id)
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
        self.inner.rollback_to_savepoint(tx_id, name)
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
