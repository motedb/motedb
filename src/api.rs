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

use crate::database::{MoteDB, TransactionStats};
use crate::database::indexes::VectorIndexStats;
use crate::sql::StreamingQueryResult;
use crate::sql::sql_row_to_row;
use crate::sql::ast::Statement;
use crate::types::{Value, Row, RowId, SqlRow};
use crate::{Result, DBConfig};
use lru::LruCache;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

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
    /// 🚀 Prepared statement cache: SQL string → parsed Statement
    /// Uses RwLock for concurrent reads + Arc<Statement> for O(1) clone on cache hit
    stmt_cache: Arc<parking_lot::RwLock<LruCache<String, Arc<Statement>>>>,
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
        Ok(Self {
            inner: Arc::new(MoteDB::create(path)?),
            stmt_cache: Arc::new(parking_lot::RwLock::new(LruCache::new(NonZeroUsize::new(256).unwrap()))),
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
        Ok(Self {
            inner: Arc::new(MoteDB::create_with_config(path, config)?),
            stmt_cache: Arc::new(parking_lot::RwLock::new(LruCache::new(NonZeroUsize::new(256).unwrap()))),
        })
    }

    /// 打开已存在的数据库
    ///
    /// # Examples
    /// ```ignore
    /// let db = Database::open("data.mote")?;
    /// ```
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(MoteDB::open(path)?),
            stmt_cache: Arc::new(parking_lot::RwLock::new(LruCache::new(NonZeroUsize::new(256).unwrap()))),
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
        Ok(Self {
            inner: Arc::new(MoteDB::open_with_config(path, config)?),
            stmt_cache: Arc::new(parking_lot::RwLock::new(LruCache::new(NonZeroUsize::new(256).unwrap()))),
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
        // Idempotent: if already closed, just return Ok
        if self.inner.is_closed.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }

        // Full checkpoint (flush + index persist + WAL truncate)
        // NOT just flush — close should ensure clean restart
        let result = self.inner.checkpoint_full();

        // Set closed flag AFTER checkpoint (so checkpoint itself can run)
        self.inner.is_closed.store(true, std::sync::atomic::Ordering::Relaxed);

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
    pub fn execute(&self, sql: &str) -> Result<StreamingQueryResult> {
        use crate::sql::{Lexer, Parser, QueryExecutor};

        // 🚀 Fast path: simple INSERT INTO <table> VALUES (...)
        // Bypasses tokenizer + parser + cache for the most common write pattern.
        // Handles: INSERT INTO table VALUES (v1, v2, ...)
        if let Some(result) = self.try_fast_insert(sql)? {
            return Ok(result);
        }

        // 🚀 Fast path: UPDATE table SET col = val WHERE pk = value
        if let Some(result) = self.try_fast_update(sql)? {
            return Ok(result);
        }

        // 🚀 Fast path: DELETE FROM table WHERE pk = value
        if let Some(result) = self.try_fast_delete(sql)? {
            return Ok(result);
        }

        // 🚀 Fast path: SELECT ... FROM <table> WHERE <pk> = <value>
        // Bypasses ~280µs of tokenizer + parser overhead for the most common read pattern.
        if let Some(result) = self.try_fast_select(sql)? {
            return Ok(result);
        }

        // 🚀 Prepared statement cache: skip re-parsing on repeated queries
        let statement: Arc<Statement> = {
            // Fast path: read lock for cache hits (concurrent with other readers)
            let read_cache = self.stmt_cache.read();
            if let Some(stmt) = read_cache.peek(sql) {
                Arc::clone(stmt)
            } else {
                drop(read_cache);
                let mut cache = self.stmt_cache.write();
                if let Some(stmt) = cache.get(sql) {
                    Arc::clone(stmt)
                } else {
                    let mut lexer = Lexer::new(sql);
                    let tokens = lexer.tokenize()?;
                    let mut parser = Parser::new(tokens);
                    let stmt = parser.parse()?;
                    let stmt_arc = Arc::new(stmt);
                    cache.put(sql.to_string(), Arc::clone(&stmt_arc));
                    stmt_arc
                }
            }
        };

        let executor = QueryExecutor::new(self.inner.clone());
        executor.execute_streaming_ref(&statement)
    }

    /// Fast INSERT path: parses `INSERT INTO <table> VALUES (<literals>)` directly
    /// from the string without going through the full tokenizer + parser + cache.
    ///
    /// Returns None if the SQL doesn't match the simple INSERT pattern.
    fn try_fast_insert(&self, sql: &str) -> Result<Option<StreamingQueryResult>> {
        // Quick check: must start with "INSERT" (case-insensitive)
        let trimmed = sql.trim_start();
        if !trimmed.as_bytes().get(0..6).map(|b| b.eq_ignore_ascii_case(b"INSERT")).unwrap_or(false) {
            return Ok(None);
        }

        // Find "INSERT INTO <table> VALUES ("
        let rest = &trimmed[6..].trim_start();
        if !rest.as_bytes().get(0..4).map(|b| b.eq_ignore_ascii_case(b"INTO")).unwrap_or(false) {
            return Ok(None);
        }

        let after_into = rest[4..].trim_start();

        // Extract table name
        let (table_name, after_table) = match after_into.find(|c: char| c.is_whitespace() || c == '(') {
            Some(pos) => (&after_into[..pos], after_into[pos..].trim_start()),
            None => return Ok(None),
        };
        if table_name.is_empty() { return Ok(None); }

        // Must be followed by "VALUES ("
        if !after_table.as_bytes().get(0..6).map(|b| b.eq_ignore_ascii_case(b"VALUES")).unwrap_or(false) {
            return Ok(None);
        }
        let after_values = after_table[6..].trim_start();
        if !after_values.starts_with('(') { return Ok(None); }

        // Extract values between ( and )
        let close_paren = match after_values.rfind(')') {
            Some(p) => p,
            None => return Ok(None),
        };
        let values_str = &after_values[1..close_paren];

        // Parse values: split by comma, handling quoted strings
        let values = match Self::parse_literal_list(values_str) {
            Some(v) => v,
            None => return Ok(None), // fall back to full parser
        };

        // Resolve schema and build row
        let schema = match self.inner.table_registry.get_table(table_name) {
            Ok(s) => s,
            Err(_) => return Ok(None), // let full parser handle the error
        };

        let columns: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        if values.len() != columns.len() { return Ok(None); }

        // Build SqlRow
        let mut sql_row = crate::types::SqlRow::new();
        for (i, col_def) in schema.columns.iter().enumerate() {
            let pk_name = schema.primary_key();
            let should_ignore = pk_name.map(|pk| pk == col_def.name && schema.is_primary_key_auto_increment()).unwrap_or(false);
            if !should_ignore {
                sql_row.insert(col_def.name.clone(), values[i].clone());
            }
        }

        // Convert to storage Row
        let row = match sql_row_to_row(&sql_row, &schema) {
            Ok(r) => r,
            Err(_) => return Ok(None),
        };

        // Insert
        let _row_id = self.inner.insert_row_to_table(table_name, row)?;

        Ok(Some(StreamingQueryResult::Modification { affected_rows: 1 }))
    }

    /// Fast SELECT path: handles `SELECT cols FROM table WHERE pk = value`
    /// Bypasses tokenizer + parser + statement cache (~280µs overhead).
    fn try_fast_select(&self, sql: &str) -> Result<Option<StreamingQueryResult>> {
        let trimmed = sql.trim_start();
        if !trimmed.as_bytes().get(0..6).map(|b| b.eq_ignore_ascii_case(b"SELECT")).unwrap_or(false) {
            return Ok(None);
        }
        let after_select = trimmed[6..].trim_start();

        // Find "FROM" keyword (case-insensitive, word boundary)
        let lower_select = after_select.to_ascii_lowercase();
        let from_pos = match lower_select.find(" from ") {
            Some(p) => p + 1, // skip leading space
            None => return Ok(None),
        };
        let after_from = after_select[from_pos + 4..].trim_start();

        // Extract table name
        let (table_name, after_table) = match after_from.find(|c: char| c.is_whitespace()) {
            Some(p) => (&after_from[..p], after_from[p..].trim_start()),
            None => return Ok(None),
        };
        if table_name.is_empty() { return Ok(None); }

        // Check for "WHERE" keyword (at start or after space)
        let lower_after = after_table.to_ascii_lowercase();
        let where_pos = if lower_after.starts_with("where ") || lower_after.starts_with("where\t") {
            0
        } else if let Some(p) = lower_after.find(" where ") {
            p + 1
        } else {
            return Ok(None);
        };
        let after_where = after_table[where_pos + 5..].trim_start();

        // Parse: column = value (only simple equality)
        let eq_pos = match after_where.find('=') {
            Some(p) => p,
            None => return Ok(None),
        };
        let col_name = after_where[..eq_pos].trim();
        let val_str = after_where[eq_pos + 1..].trim();

        // Parse the literal value
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
        let is_pk = schema.primary_key().map(|pk| pk == col_name).unwrap_or(false);
        if !is_pk { return Ok(None); }
        let is_ai = schema.is_primary_key_auto_increment();

        // Fetch row using existing get_table_row (handles cache + LSM internally)
        let row_opt = if is_ai {
            match &value {
                Value::Integer(id) if *id >= 0 => self.inner.get_table_row(table_name, *id as RowId)?,
                _ => return Ok(None),
            }
        } else {
            // Non-AUTO_INCREMENT PK: use pk_lookup cache (O(1)), fall back to column index
            let pk_key = crate::database::pk_cache::PkKey::from_value(&value);
            let row_id = if let Some(lookup) = self.inner.pk_lookup.get(table_name) {
                if let Some(rid) = lookup.get_pk(&pk_key) {
                    Some(rid)
                } else {
                    // Cache miss — column index fallback + refill cache
                    let row_ids = self.inner.query_by_column(table_name, col_name, &value)?;
                    if let Some(&rid) = row_ids.first() {
                        if let Some(lookup) = self.inner.pk_lookup.get(table_name) {
                            lookup.insert(pk_key, rid);
                        }
                    }
                    row_ids.into_iter().next()
                }
            } else {
                let row_ids = self.inner.query_by_column(table_name, col_name, &value)?;
                row_ids.into_iter().next()
            };
            match row_id {
                Some(rid) => self.inner.get_table_row(table_name, rid)?,
                None => None,
            }
        };

        // Determine select columns
        let select_part = after_select[..from_pos].trim();
        let is_star = select_part == "*";

        // Build result
        let result_vec: Vec<Vec<Value>> = match row_opt {
            Some(row) => {
                if is_star {
                    vec![schema.columns.iter()
                        .map(|c| row.get(c.position).cloned().unwrap_or(Value::Null))
                        .collect()]
                } else {
                    let col_list: Vec<&str> = select_part.split(',').map(|s| s.trim()).collect();
                    let mut vals = Vec::with_capacity(col_list.len());
                    for cname in &col_list {
                        if let Some(cd) = schema.get_column(cname) {
                            vals.push(row.get(cd.position).cloned().unwrap_or(Value::Null));
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
            schema.columns.iter().map(|c| c.name.clone()).collect()
        } else if result_vec.is_empty() {
            select_part.split(',').map(|s| s.trim().to_string()).collect()
        } else {
            select_part.split(',').map(|s| s.trim().to_string()).collect()
        };

        let rows = result_vec.into_iter().map(Ok);
        Ok(Some(StreamingQueryResult::SelectStreaming {
            columns: column_names,
            rows: Box::new(rows),
            order_by: None,
            limit: None,
            offset: None,
            distinct: false,
        }))
    }

    /// Parse a single SQL literal (integer, float, or string).
    fn parse_single_literal(s: &str) -> Option<Value> {
        let s = s.trim();
        if s.is_empty() { return None; }
        if s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2 {
            return Some(Value::Text(s[1..s.len()-1].to_string()));
        }
        if s.starts_with('-') || s.as_bytes().get(0)?.is_ascii_digit() {
            if let Ok(i) = s.parse::<i64>() { return Some(Value::Integer(i)); }
            if let Ok(f) = s.parse::<f64>() { return Some(Value::Float(f)); }
        }
        if s.eq_ignore_ascii_case("NULL") { return Some(Value::Null); }
        None
    }

    /// Fast UPDATE path: parses `UPDATE <table> SET col1=v1, col2=v2 WHERE pk = value`
    fn try_fast_update(&self, sql: &str) -> Result<Option<StreamingQueryResult>> {
        let trimmed = sql.trim_start();
        if !trimmed.as_bytes().get(0..6).map(|b| b.eq_ignore_ascii_case(b"UPDATE")).unwrap_or(false) {
            return Ok(None);
        }
        let after_update = trimmed[6..].trim_start();

        // Extract table name
        let (table_name, after_table) = match after_update.find(|c: char| c.is_whitespace()) {
            Some(p) => (&after_update[..p], after_update[p..].trim_start()),
            None => return Ok(None),
        };
        if table_name.is_empty() { return Ok(None); }

        // Must have "SET"
        let lower_after = after_table.to_ascii_lowercase();
        if !lower_after.starts_with("set ") && !lower_after.starts_with("set\t") {
            return Ok(None);
        }
        let after_set = after_table[3..].trim_start();

        // Find "WHERE" keyword
        let lower_set = after_set.to_ascii_lowercase();
        let where_pos = match lower_set.rfind(" where ") {
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
        let is_pk = schema.primary_key().map(|pk| pk == where_col).unwrap_or(false);
        if !is_pk { return Ok(None); }

        // Parse SET assignments: col1=v1, col2=v2
        let mut assignments: Vec<(String, Value)> = Vec::new();
        for pair in set_part.split(',') {
            let eq = match pair.find('=') {
                Some(p) => p,
                None => return Ok(None),
            };
            let col = pair[..eq].trim().to_string();
            let val_str = pair[eq + 1..].trim();
            let val = match Self::parse_single_literal(val_str) {
                Some(v) => v,
                None => return Ok(None),
            };
            assignments.push((col, val));
        }

        // Resolve PK → row_id
        let row_id = if schema.is_primary_key_auto_increment() {
            match &where_value {
                Value::Integer(id) if *id >= 0 => *id as RowId,
                _ => return Ok(None),
            }
        } else {
            let pk_key = crate::database::pk_cache::PkKey::from_value(&where_value);
            if let Some(lookup) = self.inner.pk_lookup.get(table_name) {
                if let Some(rid) = lookup.get_pk(&pk_key) {
                    rid
                } else {
                    let row_ids = self.inner.query_by_column(table_name, where_col, &where_value)?;
                    match row_ids.into_iter().next() {
                        Some(rid) => {
                            if let Some(lookup) = self.inner.pk_lookup.get(table_name) {
                                lookup.insert(pk_key, rid);
                            }
                            rid
                        }
                        None => return Ok(Some(StreamingQueryResult::Modification { affected_rows: 0 })),
                    }
                }
            } else {
                let row_ids = self.inner.query_by_column(table_name, where_col, &where_value)?;
                match row_ids.into_iter().next() {
                    Some(rid) => rid,
                    None => return Ok(Some(StreamingQueryResult::Modification { affected_rows: 0 })),
                }
            }
        };

        // Load old row, apply updates, write back
        let old_row = match self.inner.get_table_row(table_name, row_id)? {
            Some(r) => r,
            None => return Ok(Some(StreamingQueryResult::Modification { affected_rows: 0 })),
        };

        let mut new_row = old_row.clone();
        for (col_name, val) in &assignments {
            if let Some(cd) = schema.get_column(col_name) {
                while new_row.len() <= cd.position {
                    new_row.push(Value::Null);
                }
                new_row[cd.position] = val.clone();
            }
        }

        self.inner.update_row_in_table(table_name, row_id, old_row, new_row)?;
        Ok(Some(StreamingQueryResult::Modification { affected_rows: 1 }))
    }

    /// Fast DELETE path: parses `DELETE FROM <table> WHERE pk = value`
    fn try_fast_delete(&self, sql: &str) -> Result<Option<StreamingQueryResult>> {
        let trimmed = sql.trim_start();
        if !trimmed.as_bytes().get(0..6).map(|b| b.eq_ignore_ascii_case(b"DELETE")).unwrap_or(false) {
            return Ok(None);
        }
        let after_delete = trimmed[6..].trim_start();

        // Must have "FROM"
        if !after_delete.as_bytes().get(0..4).map(|b| b.eq_ignore_ascii_case(b"FROM")).unwrap_or(false) {
            return Ok(None);
        }
        let after_from = after_delete[4..].trim_start();

        // Extract table name
        let (table_name, after_table) = match after_from.find(|c: char| c.is_whitespace()) {
            Some(p) => (&after_from[..p], after_from[p..].trim_start()),
            None => return Ok(None),
        };
        if table_name.is_empty() { return Ok(None); }

        // Check for "WHERE"
        let lower_after = after_table.to_ascii_lowercase();
        if !lower_after.starts_with("where ") && !lower_after.starts_with("where\t") {
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
        let is_pk = schema.primary_key().map(|pk| pk == col_name).unwrap_or(false);
        if !is_pk { return Ok(None); }

        // Resolve PK → row_id
        let row_id = if schema.is_primary_key_auto_increment() {
            match &value {
                Value::Integer(id) if *id >= 0 => *id as RowId,
                _ => return Ok(None),
            }
        } else {
            let pk_key = crate::database::pk_cache::PkKey::from_value(&value);
            if let Some(lookup) = self.inner.pk_lookup.get(table_name) {
                if let Some(rid) = lookup.get_pk(&pk_key) {
                    rid
                } else {
                    let row_ids = self.inner.query_by_column(table_name, col_name, &value)?;
                    match row_ids.into_iter().next() {
                        Some(rid) => {
                            if let Some(lookup) = self.inner.pk_lookup.get(table_name) {
                                lookup.insert(pk_key, rid);
                            }
                            rid
                        }
                        None => return Ok(Some(StreamingQueryResult::Modification { affected_rows: 0 })),
                    }
                }
            } else {
                let row_ids = self.inner.query_by_column(table_name, col_name, &value)?;
                match row_ids.into_iter().next() {
                    Some(rid) => rid,
                    None => return Ok(Some(StreamingQueryResult::Modification { affected_rows: 0 })),
                }
            }
        };

        // Load old row, then delete
        let old_row = match self.inner.get_table_row(table_name, row_id)? {
            Some(r) => r,
            None => return Ok(Some(StreamingQueryResult::Modification { affected_rows: 0 })),
        };

        self.inner.delete_row_from_table(table_name, row_id, old_row)?;
        Ok(Some(StreamingQueryResult::Modification { affected_rows: 1 }))
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
                if c.is_whitespace() { chars.next(); } else { break; }
            }
            if chars.peek().is_none() { break; }

            let (start_idx, start_char) = chars.peek().copied().unwrap();

            if start_char == '\'' {
                // String literal
                chars.next(); // consume opening quote
                let mut text = String::new();
                let mut escaped = false;
                loop {
                    match chars.next() {
                        Some((_, '\'')) if !escaped => break,
                        Some((_, '\\')) => { escaped = true; text.push('\\'); }
                        Some((_, c)) => { escaped = false; text.push(c); }
                        None => return None, // unterminated string
                    }
                }
                values.push(Value::Text(text));
            } else if start_char == '-' || start_char.is_ascii_digit() {
                // Number (integer or float)
                let mut num_str = String::new();
                if start_char == '-' { num_str.push('-'); chars.next(); }
                let mut has_dot = false;
                while let Some(&(_, c)) = chars.peek() {
                    if c.is_ascii_digit() { num_str.push(c); chars.next(); }
                    else if c == '.' && !has_dot { has_dot = true; num_str.push(c); chars.next(); }
                    else { break; }
                }
                if num_str.is_empty() || num_str == "-" || num_str == "-." { return None; }
                if has_dot {
                    values.push(Value::Float(num_str.parse().ok()?));
                } else {
                    values.push(Value::Integer(num_str.parse().ok()?));
                }
            } else if len - start_idx >= 4 && s[start_idx..start_idx+4].eq_ignore_ascii_case("NULL") {
                values.push(Value::Null);
                for _ in 0..4 { chars.next(); }
            } else if len - start_idx >= 4 && s[start_idx..start_idx+4].eq_ignore_ascii_case("TRUE") {
                values.push(Value::Bool(true));
                for _ in 0..4 { chars.next(); }
            } else if len - start_idx >= 5 && s[start_idx..start_idx+5].eq_ignore_ascii_case("FALSE") {
                values.push(Value::Bool(false));
                for _ in 0..5 { chars.next(); }
            } else {
                return None; // unsupported literal, fall back to full parser
            }

            // Skip whitespace and comma
            while let Some(&(_, c)) = chars.peek() {
                if c.is_whitespace() { chars.next(); } else { break; }
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
        let rows: Result<Vec<Row>> = sql_rows.into_iter().map(|sql_row| {
            crate::sql::row_converter::sql_row_to_row(&sql_row, &schema)
        }).collect();
        
        // 🚀 使用新的 batch_insert_rows_to_table (支持增量索引更新)
        self.inner.batch_insert_rows_to_table(table_name, rows?)
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
    pub fn batch_insert_with_vectors(&self, table_name: &str, rows: Vec<Row>, _vector_columns: &[&str]) -> Result<Vec<RowId>> {
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
    pub fn batch_insert_with_vectors_map(&self, table_name: &str, sql_rows: Vec<SqlRow>, vector_columns: &[&str]) -> Result<Vec<RowId>> {
        // 获取表结构
        let schema = self.inner.get_table_schema(table_name)?;
        
        // 将 SqlRow (HashMap) 转换为 Row (Vec<Value>)
        let rows: Result<Vec<Row>> = sql_rows.into_iter().map(|sql_row| {
            crate::sql::row_converter::sql_row_to_row(&sql_row, &schema)
        }).collect();
        
        self.batch_insert_with_vectors(table_name, rows?, vector_columns)
    }

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

    /// 删除索引
    ///
    /// # Examples
    /// ```ignore
    /// db.drop_index("users", "email")?;
    /// ```
    pub fn drop_index(&self, table_name: &str, index_name: &str) -> Result<()> {
        // 通过SQL执行
        let sql = format!("DROP INDEX {} ON {}", index_name, table_name);
        self.execute(&sql)?;
        Ok(())
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
    pub fn query_by_column(&self, table_name: &str, column_name: &str, value: &Value) -> Result<Vec<RowId>> {
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
    pub fn query_by_column_range(&self, table_name: &str, column_name: &str, 
                                 start: &Value, end: &Value) -> Result<Vec<RowId>> {
        self.inner.query_by_column_range(table_name, column_name, start, end)
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
    pub fn query_by_column_between(&self, table_name: &str, column_name: &str,
                                  start: &Value, start_inclusive: bool,
                                  end: &Value, end_inclusive: bool) -> Result<Vec<RowId>> {
        self.inner.query_by_column_between(table_name, column_name, start, start_inclusive, end, end_inclusive)
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
    pub fn vector_search(&self, index_name: &str, query: &[f32], k: usize) -> Result<Vec<(RowId, f32)>> {
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
    pub fn text_search_ranked(&self, index_name: &str, query: &str, top_k: usize) -> Result<Vec<(RowId, f32)>> {
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
    pub fn ioctree_radius_search(
        &self,
        index_name: &str,
        center: &crate::types::Point3D,
        radius: f64,
    ) -> Result<Vec<(RowId, f64)>> {
        self.inner.ioctree_radius_search(index_name, center, radius)
    }

    /// 3D range search: find all points within a 3D bounding box
    pub fn ioctree_range_search(
        &self,
        index_name: &str,
        bbox: &crate::types::BoundingBox3D,
    ) -> Result<Vec<RowId>> {
        self.inner.ioctree_range_query(index_name, bbox)
    }

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
            Ok(Some(crate::sql::row_converter::row_to_sql_row(&row, &schema)?))
        } else {
            Ok(None)
        }
    }

    /// 更新行（底层API，推荐使用 SQL UPDATE）
    pub fn update_row(&self, table_name: &str, row_id: RowId, new_row: Row) -> Result<()> {
        // 先获取旧行
        let old_row = self.inner.get_table_row(table_name, row_id)?
            .ok_or_else(|| crate::StorageError::InvalidData(
                format!("Row {} not found in table '{}'", row_id, table_name)
            ))?;
        self.inner.update_row_in_table(table_name, row_id, old_row, new_row)
    }

    /// 更新行（使用 HashMap）
    ///
    /// # Examples
    /// ```ignore
    /// use motedb::types::{Value, SqlRow};
    /// use std::collections::HashMap;
    ///
    /// let mut updated_row = HashMap::new();
    /// updated_row.insert("name".to_string(), Value::Text("Bob".into()));
    /// updated_row.insert("age".to_string(), Value::Integer(30));
    ///
    /// db.update_row_map("users", 1, updated_row)?;
    /// ```
    pub fn update_row_map(&self, table_name: &str, row_id: RowId, sql_row: SqlRow) -> Result<()> {
        // 先获取旧行
        let old_row = self.inner.get_table_row(table_name, row_id)?
            .ok_or_else(|| crate::StorageError::InvalidData(
                format!("Row {} not found in table '{}'", row_id, table_name)
            ))?;
        
        // 获取表结构
        let schema = self.inner.get_table_schema(table_name)?;
        
        // 将 SqlRow (HashMap) 转换为 Row (Vec<Value>)
        let new_row = crate::sql::row_converter::sql_row_to_row(&sql_row, &schema)?;
        
        self.inner.update_row_in_table(table_name, row_id, old_row, new_row)
    }

    /// 删除行（底层API，推荐使用 SQL DELETE）
    pub fn delete_row(&self, table_name: &str, row_id: RowId) -> Result<()> {
        // 先获取旧行
        let old_row = self.inner.get_table_row(table_name, row_id)?
            .ok_or_else(|| crate::StorageError::InvalidData(
                format!("Row {} not found in table '{}'", row_id, table_name)
            ))?;
        self.inner.delete_row_from_table(table_name, row_id, old_row)
    }

    /// 扫描表的所有行（底层API，推荐使用 SQL SELECT）
    pub fn scan_table(&self, table_name: &str) -> Result<Vec<(RowId, Row)>> {
        self.inner.scan_table_rows(table_name)
    }
}

// 自动在 Drop 时关闭数据库
impl Drop for Database {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
