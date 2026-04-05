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
use crate::database::indexes::{VectorIndexStats, SpatialIndexStats};
use crate::sql::StreamingQueryResult;
use crate::sql::ast::Statement;
use crate::types::{Value, Row, RowId, SqlRow};
use crate::{Result, DBConfig};
use lru::LruCache;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;

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
/// - `create_spatial_index()`: 创建空间索引（地理位置查询）
///
/// ## 5. 查询API
/// - `query_by_column()`: 按列值查询（使用索引）
/// - `vector_search()`: 向量KNN搜索
/// - `text_search()`: 全文搜索（BM25）
/// - `spatial_search()`: 空间范围查询
/// - `query_timestamp_range()`: 时间序列查询
///
/// ## 6. 统计信息
/// - `stats()`: 数据库统计信息
/// - `vector_index_stats()`: 向量索引统计
/// - `spatial_index_stats()`: 空间索引统计
/// - `transaction_stats()`: 事务统计
///
/// ## 7. 持久化
/// - `flush()`: 刷新数据到磁盘
/// - `checkpoint()`: 创建检查点
/// - `close()`: 关闭数据库
pub struct Database {
    inner: Arc<MoteDB>,
    /// 🚀 Prepared statement cache: SQL string → parsed Statement
    stmt_cache: Arc<Mutex<LruCache<String, Statement>>>,
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
            stmt_cache: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(256).unwrap()))),
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
            stmt_cache: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(256).unwrap()))),
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
            stmt_cache: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(256).unwrap()))),
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
            stmt_cache: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(256).unwrap()))),
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

    /// Checkpoint: flush data + persist indexes + truncate WAL
    ///
    /// Stronger durability guarantee than flush() alone.
    /// Use before closing to ensure full recoverability.
    pub fn checkpoint(&self) -> Result<()> {
        self.inner.checkpoint()
    }

    /// 关闭数据库（显式调用，通常由 Drop 自动处理）
    ///
    /// # Examples
    /// ```ignore
    /// db.close()?;
    /// ```
    pub fn close(&self) -> Result<()> {
        self.flush()
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

        // 🚀 Prepared statement cache: skip re-parsing on repeated queries
        let statement = {
            let mut cache = self.stmt_cache.lock().unwrap();
            if let Some(stmt) = cache.get(sql) {
                stmt.clone()
            } else {
                let mut lexer = Lexer::new(sql);
                let tokens = lexer.tokenize()?;
                let mut parser = Parser::new(tokens);
                let stmt = parser.parse()?;
                cache.put(sql.to_string(), stmt.clone());
                stmt
            }
        };

        let executor = QueryExecutor::new(self.inner.clone());
        executor.execute_streaming(statement)
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
    pub fn batch_insert(&self, _table_name: &str, rows: Vec<Row>) -> Result<Vec<RowId>> {
        self.inner.batch_insert_rows(rows)
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
        self.inner.create_vector_index(index_name, dimension)
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

    /// 创建空间索引（用于地理位置查询）
    ///
    /// # Examples
    /// ```ignore
    /// use motedb::BoundingBox;
    ///
    /// // 创建空间索引（指定世界范围）
    /// let bounds = BoundingBox {
    ///     min_x: -180.0,
    ///     min_y: -90.0,
    ///     max_x: 180.0,
    ///     max_y: 90.0,
    /// };
    /// db.create_spatial_index("locations_coords", bounds)?;
    ///
    /// // SQL 空间查询
    /// let results = db.query(
    ///     "SELECT * FROM locations 
    ///      WHERE ST_WITHIN(coords, 116.0, 39.0, 117.0, 40.0)"
    /// )?;
    /// ```
    pub fn create_spatial_index(&self, index_name: &str, bounds: crate::types::BoundingBox) -> Result<()> {
        self.inner.create_spatial_index(index_name, bounds)
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

    /// 空间范围查询
    ///
    /// # Examples
    /// ```ignore
    /// use motedb::BoundingBox;
    ///
    /// // 查询矩形区域内的所有点
    /// let bbox = BoundingBox {
    ///     min_x: 116.0,
    ///     min_y: 39.0,
    ///     max_x: 117.0,
    ///     max_y: 40.0,
    /// };
    /// let results = db.spatial_search("locations_coords", &bbox)?;
    /// ```
    pub fn spatial_search(&self, index_name: &str, bbox: &crate::types::BoundingBox) -> Result<Vec<RowId>> {
        self.inner.spatial_range_query(index_name, bbox)
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

    /// 获取空间索引统计信息
    pub fn spatial_index_stats(&self, index_name: &str) -> Result<SpatialIndexStats> {
        self.inner.spatial_index_stats(index_name)
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
    pub fn get_row(&self, row_id: RowId) -> Result<Option<Row>> {
        self.inner.get_row(row_id)
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
        if let Some(row) = self.inner.get_row(row_id)? {
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

// 自动在 Drop 时刷新数据
impl Drop for Database {
    fn drop(&mut self) {
        let _ = self.inner.flush();
    }
}
