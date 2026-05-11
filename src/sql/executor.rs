/// Query executor - executes SQL statements against storage engine
use super::ast::*;
use super::evaluator::ExprEvaluator;
use super::row_converter::{row_to_sql_row, sql_row_to_row, rows_to_sql_rows};
use crate::database::MoteDB;
use crate::error::{Result, MoteDBError};
use crate::{StorageError};
use crate::types::{Value, SqlRow, TableSchema, ColumnType, RowId, Row};
use crate::storage::row_format;
use std::sync::Arc;
use std::sync::Mutex;

fn decode_row(data: &[u8], schema: &TableSchema) -> crate::Result<Row> {
    row_format::decode(data, schema.col_types())
}

#[allow(clippy::type_complexity)]
type FromScanResult = Result<(Vec<(u64, SqlRow)>, Arc<TableSchema>)>;

#[allow(clippy::type_complexity)]
type RowPredicate = Option<Box<dyn Fn(&SqlRow) -> bool + Send + Sync>>;

/// Query result
#[derive(Debug)]
pub enum QueryResult {
    /// SELECT result
    Select {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    
    /// INSERT/UPDATE/DELETE result
    Modification {
        affected_rows: usize,
    },
    
    /// CREATE/DROP result
    Definition {
        message: String,
    },
}

impl QueryResult {
    pub fn affected_rows(&self) -> usize {
        match self {
            QueryResult::Modification { affected_rows } => *affected_rows,
            _ => 0,
        }
    }
    
    /// Get columns and rows from SELECT result
    /// Returns None if not a SELECT result
    pub fn select_rows(&self) -> Option<(&[String], &[Vec<Value>])> {
        match self {
            QueryResult::Select { columns, rows } => Some((columns.as_slice(), rows.as_slice())),
            _ => None,
        }
    }

    pub fn row_count(&self) -> usize {
        match self {
            QueryResult::Select { rows, .. } => rows.len(),
            QueryResult::Modification { affected_rows } => *affected_rows,
            _ => 0,
        }
    }
}



/// 🚀 流式查询结果（方案 C：零内存开销）
/// 
/// 返回迭代器而不是 Vec，实现真正的流式查询。
/// 
/// # 示例
/// ```ignore
/// // 新 API：流式迭代
/// let result = db.execute_streaming("SELECT * FROM robots WHERE age < 25")?;
/// result.for_each(|columns, row| {
///     println!("{:?}: {:?}", columns, row);
///     Ok(())
/// })?;
/// ```
pub enum StreamingQueryResult {
    /// SELECT 流式结果
    SelectStreaming {
        columns: Vec<String>,
        rows: Box<dyn Iterator<Item = Result<Vec<Value>>> + Send>,
        /// 🔧 ORDER BY 子句（在 materialize() 时应用）
        order_by: Option<Vec<OrderByExpr>>,
        /// 🔧 LIMIT 子句（在 materialize() 时应用）
        limit: Option<usize>,
        /// 🔧 OFFSET 子句（在 materialize() 时应用）
        offset: Option<usize>,
        /// 🔧 DISTINCT 标志（在 materialize() 时应用）
        distinct: bool,
    },

    /// 🚀 Pre-materialized SELECT result (zero-overhead for fast PK paths)
    SelectReady {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },

    /// INSERT/UPDATE/DELETE result
    Modification {
        affected_rows: usize,
    },

    /// CREATE/DROP result
    Definition {
        message: String,
    },
}

impl StreamingQueryResult {
    /// 🔥 物化结果集（供向后兼容的 execute() 使用）
    /// 
    /// 将流式结果立即加载到内存中，转换为 `QueryResult`。
    pub fn materialize(self) -> Result<QueryResult> {
        self.materialize_with_hint(None)
    }
    
    /// 🚀 优化版物化：支持容量预分配
    /// 
    /// # 优化点
    /// - Vec::with_capacity() 预分配容量，避免多次扩容
    /// - 减少内存重分配次数，提升性能 20-30%
    /// - 🔧 在物化时应用 ORDER BY、LIMIT、OFFSET、DISTINCT
    /// 
    /// # 参数
    /// - `size_hint`: 预估的结果行数（来自优化器统计信息）
    pub fn materialize_with_hint(self, size_hint: Option<usize>) -> Result<QueryResult> {
        match self {
            Self::SelectReady { columns, rows } => {
                Ok(QueryResult::Select { columns, rows })
            }
            Self::SelectStreaming { columns, rows, order_by, limit, offset, distinct } => {
                // 🔧 Step 1: 收集所有行
                let estimated_size = size_hint.unwrap_or(1024);
                let mut materialized_rows = Vec::with_capacity(estimated_size);

                for row_result in rows {
                    materialized_rows.push(row_result?);
                }

                // 🔧 Step 2: 应用 ORDER BY
                if let Some(order_clauses) = order_by {
                    Self::apply_order_by(&mut materialized_rows, &columns, &order_clauses)?;
                }

                // 🔧 Step 3: 应用 DISTINCT
                if distinct {
                    materialized_rows = Self::apply_distinct(materialized_rows);
                }

                // 🔧 Step 4: 应用 OFFSET 和 LIMIT
                let offset_val = offset.unwrap_or(0);
                let final_rows: Vec<Vec<Value>> = materialized_rows
                    .into_iter()
                    .skip(offset_val)
                    .take(limit.unwrap_or(usize::MAX))
                    .collect();

                Ok(QueryResult::Select {
                    columns,
                    rows: final_rows,
                })
            }
            Self::Modification { affected_rows } => {
                Ok(QueryResult::Modification { affected_rows })
            }
            Self::Definition { message } => {
                Ok(QueryResult::Definition { message })
            }
        }
    }
    
    /// 便利方法：逐行处理（零内存开销）

    /// 获取影响行数
    pub fn affected_rows(&self) -> usize {
        match self {
            Self::Modification { affected_rows } => *affected_rows,
            _ => 0,
        }
    }

    /// 获取列名（仅 SELECT）
    pub fn columns(&self) -> Option<&[String]> {
        match self {
            Self::SelectStreaming { columns, .. } => Some(columns),
            Self::SelectReady { columns, .. } => Some(columns),
            _ => None,
        }
    }
    
    /// 🔧 应用 ORDER BY（静态方法，在 materialize() 中调用）
    fn apply_order_by(
        rows: &mut [Vec<Value>],
        columns: &[String],
        order_clauses: &[OrderByExpr],
    ) -> Result<()> {
        use std::cmp::Ordering;
        
        rows.sort_by(|a, b| {
            for clause in order_clauses {
                // ORDER BY 支持表达式，但这里先只处理简单列名
                let col_name = match &clause.expr {
                    Expr::Column(name) => name,
                    _ => continue, // 暂时跳过复杂表达式
                };
                
                // 找到排序列的索引
                let col_idx = match columns.iter().position(|c| c == col_name) {
                    Some(idx) => idx,
                    None => continue, // 列不存在，跳过
                };
                
                if col_idx >= a.len() || col_idx >= b.len() {
                    continue;
                }
                
                let val_a = &a[col_idx];
                let val_b = &b[col_idx];
                
                let cmp = match (val_a, val_b) {
                    (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
                    (Value::Float(a), Value::Float(b)) => {
                        if a.is_nan() && b.is_nan() {
                            Ordering::Equal
                        } else if a.is_nan() {
                            Ordering::Greater
                        } else if b.is_nan() {
                            Ordering::Less
                        } else {
                            a.partial_cmp(b).unwrap_or(Ordering::Equal)
                        }
                    }
                    (Value::Text(a), Value::Text(b)) => a.cmp(b),
                    (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
                    (Value::Null, Value::Null) => Ordering::Equal,
                    (Value::Null, _) => Ordering::Less,
                    (_, Value::Null) => Ordering::Greater,
                    _ => Ordering::Equal, // 不同类型，视为相等
                };
                
                let final_cmp = if clause.asc {
                    cmp
                } else {
                    cmp.reverse()
                };
                
                if final_cmp != Ordering::Equal {
                    return final_cmp;
                }
            }
            Ordering::Equal
        });
        
        Ok(())
    }
    
    /// 🔧 应用 DISTINCT（静态方法，在 materialize() 中调用）
    fn apply_distinct(rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
        use std::collections::HashSet;
        
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        
        for row in rows {
            // 使用调试格式作为哈希键（简单但有效）
            let key = format!("{:?}", row);
            if seen.insert(key) {
                result.push(row);
            }
        }
        
        result
    }
}

pub struct QueryExecutor {
    db: Arc<MoteDB>,
    evaluator: ExprEvaluator,
    optimizer: Mutex<super::optimizer::QueryOptimizer>,
    /// Store the last AUTO_INCREMENT value inserted (mirrors evaluator)
    last_insert_id: std::sync::atomic::AtomicI64,
}

impl QueryExecutor {
    pub fn new(db: Arc<MoteDB>) -> Self {
        Self {
            evaluator: ExprEvaluator::with_db(db.clone()),
            optimizer: Mutex::new(super::optimizer::QueryOptimizer::new(db.clone())),
            last_insert_id: std::sync::atomic::AtomicI64::new(i64::MIN),
            db,
        }
    }

    /// Reset per-query state. Called before each execute.
    pub fn reset_last_insert_id(&self) {
        self.last_insert_id.store(i64::MIN, std::sync::atomic::Ordering::Relaxed);
        self.evaluator.last_insert_id.store(i64::MIN, std::sync::atomic::Ordering::Relaxed);
        self.evaluator.clear_params();
    }

    /// Bind parameters for a parameterized query.
    pub fn bind_params(&self, params: Vec<Value>) {
        self.evaluator.set_params(params);
    }

    /// Clear bind parameters after execution.
    pub fn clear_params(&self) {
        self.evaluator.clear_params();
    }
    
    pub fn execute(&self, stmt: Statement) -> Result<QueryResult> {
        match stmt {
            Statement::Select(s) => self.execute_select(s),
            Statement::Insert(i) => self.execute_insert(i),
            Statement::Update(u) => self.execute_update(u),
            Statement::Delete(d) => self.execute_delete(d),
            Statement::CreateTable(c) => self.execute_create_table(c),
            Statement::CreateIndex(c) => self.execute_create_index(c),
            Statement::DropTable(d) => self.execute_drop_table(d),
            Statement::DropIndex(d) => self.execute_drop_index(d),
            Statement::AlterTable(a) => self.execute_alter_table(a),
            Statement::ShowTables => self.execute_show_tables(),
            Statement::DescribeTable(table_name) => self.execute_describe_table(table_name),
        }
    }
    
    /// 🚀 流式执行（方案 C：零内存开销）
    /// 
    /// 返回迭代器而不是 Vec，实现真正的流式查询。
    /// 
    /// # 示例
    /// ```ignore
    /// let result = executor.execute_streaming(stmt)?;
    /// result.for_each(|columns, row| {
    ///     println!("{:?}: {:?}", columns, row);
    ///     Ok(())
    /// })?;
    /// ```
    /// Execute a statement by reference (avoids cloning the AST).
    ///
    /// For SELECT: only clones the SelectStmt (cheap relative to full query).
    /// For other statements: clones only the specific variant needed.
    pub fn execute_streaming_ref(&self, stmt: &Statement) -> Result<StreamingQueryResult> {
        match stmt {
            Statement::Select(s) => self.execute_select_streaming_ref(s),
            Statement::Insert(i) => {
                let result = self.execute_insert(i.clone())?;
                Ok(StreamingQueryResult::Modification {
                    affected_rows: result.affected_rows(),
                })
            }
            Statement::Update(u) => {
                let result = self.execute_update(u.clone())?;
                Ok(StreamingQueryResult::Modification {
                    affected_rows: result.affected_rows(),
                })
            }
            Statement::Delete(d) => {
                let result = self.execute_delete(d.clone())?;
                Ok(StreamingQueryResult::Modification {
                    affected_rows: result.affected_rows(),
                })
            }
            Statement::CreateTable(c) => {
                let result = self.execute_create_table(c.clone())?;
                Ok(StreamingQueryResult::Definition {
                    message: match result {
                        QueryResult::Definition { message } => message,
                        _ => "Table created".to_string(),
                    },
                })
            }
            Statement::CreateIndex(c) => {
                let result = self.execute_create_index(c.clone())?;
                Ok(StreamingQueryResult::Definition {
                    message: match result {
                        QueryResult::Definition { message } => message,
                        _ => "Index created".to_string(),
                    },
                })
            }
            Statement::DropTable(d) => {
                let result = self.execute_drop_table(d.clone())?;
                Ok(StreamingQueryResult::Definition {
                    message: match result {
                        QueryResult::Definition { message } => message,
                        _ => "Table dropped".to_string(),
                    },
                })
            }
            Statement::DropIndex(d) => {
                let result = self.execute_drop_index(d.clone())?;
                Ok(StreamingQueryResult::Definition {
                    message: match result {
                        QueryResult::Definition { message } => message,
                        _ => "Index dropped".to_string(),
                    },
                })
            }
            Statement::ShowTables => {
                let result = self.execute_show_tables()?;
                Ok(StreamingQueryResult::Definition {
                    message: match result {
                        QueryResult::Definition { message } => message,
                        _ => "Tables shown".to_string(),
                    },
                })
            }
            Statement::DescribeTable(table_name) => {
                let result = self.execute_describe_table(table_name.clone())?;
                Ok(StreamingQueryResult::Definition {
                    message: match result {
                        QueryResult::Definition { message } => message,
                        _ => "Table described".to_string(),
                    },
                })
            }
            Statement::AlterTable(a) => {
                let result = self.execute_alter_table(a.clone())?;
                Ok(StreamingQueryResult::Definition {
                    message: match result {
                        QueryResult::Definition { message } => message,
                        _ => "Table altered".to_string(),
                    },
                })
            }
        }
    }

    pub fn execute_streaming(&self, stmt: Statement) -> Result<StreamingQueryResult> {
        match stmt {
            Statement::Select(s) => self.execute_select_streaming(s),
            // 其他语句直接物化（无需流式）
            Statement::Insert(i) => {
                let result = self.execute_insert(i)?;
                Ok(StreamingQueryResult::Modification {
                    affected_rows: result.affected_rows(),
                })
            }
            Statement::Update(u) => {
                let result = self.execute_update(u)?;
                Ok(StreamingQueryResult::Modification {
                    affected_rows: result.affected_rows(),
                })
            }
            Statement::Delete(d) => {
                let result = self.execute_delete(d)?;
                Ok(StreamingQueryResult::Modification {
                    affected_rows: result.affected_rows(),
                })
            }
            Statement::CreateTable(c) => {
                let result = self.execute_create_table(c)?;
                Ok(StreamingQueryResult::Definition {
                    message: match result {
                        QueryResult::Definition { message } => message,
                        _ => "Table created".to_string(),
                    },
                })
            }
            Statement::CreateIndex(c) => {
                let result = self.execute_create_index(c)?;
                Ok(StreamingQueryResult::Definition {
                    message: match result {
                        QueryResult::Definition { message } => message,
                        _ => "Index created".to_string(),
                    },
                })
            }
            Statement::DropTable(d) => {
                let result = self.execute_drop_table(d)?;
                Ok(StreamingQueryResult::Definition {
                    message: match result {
                        QueryResult::Definition { message } => message,
                        _ => "Table dropped".to_string(),
                    },
                })
            }
            Statement::DropIndex(d) => {
                let result = self.execute_drop_index(d)?;
                Ok(StreamingQueryResult::Definition {
                    message: match result {
                        QueryResult::Definition { message } => message,
                        _ => "Index dropped".to_string(),
                    },
                })
            }
            Statement::ShowTables => {
                let result = self.execute_show_tables()?;
                Ok(StreamingQueryResult::Definition {
                    message: match result {
                        QueryResult::Definition { message } => message,
                        _ => "Tables shown".to_string(),
                    },
                })
            }
            Statement::DescribeTable(table_name) => {
                let result = self.execute_describe_table(table_name)?;
                Ok(StreamingQueryResult::Definition {
                    message: match result {
                        QueryResult::Definition { message } => message,
                        _ => "Table described".to_string(),
                    },
                })
            }
            Statement::AlterTable(a) => {
                let result = self.execute_alter_table(a)?;
                Ok(StreamingQueryResult::Definition {
                    message: match result {
                        QueryResult::Definition { message } => message,
                        _ => "Table altered".to_string(),
                    },
                })
            }
        }
    }
    
    /// Execute SELECT statement
    fn execute_select(&self, stmt: SelectStmt) -> Result<QueryResult> {
        self.execute_select_internal(&stmt)
    }
    
    /// 🚀 Execute SELECT statement (streaming version, zero-clone)
    ///
    /// Takes &SelectStmt — no cloning of the AST at all.
    /// This is the primary entry point from the statement cache.
    fn execute_select_streaming_ref(&self, stmt: &SelectStmt) -> Result<StreamingQueryResult> {
        // Aggregate queries (COUNT, SUM, etc.) require materialization — fall back
        if self.has_aggregates(&stmt.columns) {
            let result = self.execute_select_internal(stmt)?;
            return match result {
                QueryResult::Select { columns, rows } => {
                    Ok(StreamingQueryResult::SelectStreaming {
                        columns,
                        rows: Box::new(rows.into_iter().map(Ok)),
                        order_by: None,
                        limit: None,
                        offset: None,
                        distinct: false,
                    })
                }
                _ => unreachable!(),
            };
        }

        // Handle JOIN/Subquery by falling back to materialization
        match stmt.from.as_ref().unwrap() {
            TableRef::Join { .. } | TableRef::Subquery { .. } => {
                let result = self.execute_select_internal(stmt)?;
                return match result {
                    QueryResult::Select { columns, rows } => {
                        Ok(StreamingQueryResult::SelectStreaming {
                            columns,
                            rows: Box::new(rows.into_iter().map(Ok)),
                            order_by: None,
                            limit: None,
                            offset: None,
                            distinct: false,
                        })
                    }
                    _ => unreachable!(),
                };
            }
            _ => {}
        }

        // 🔥 核心改进：使用查询优化器生成执行计划
        // 🚀 Fast path: Text search and spatial queries must go through execute_select_internal
        // which has the optimized index pushdown paths. The streaming path only handles
        // FullScan which would be O(N) for these queries.
        if let Some(ref where_clause) = stmt.where_clause {
            if Self::expr_needs_materialized_path(where_clause) {
                let result = self.execute_select_internal(stmt)?;
                return match result {
                    QueryResult::Select { columns, rows } => {
                        Ok(StreamingQueryResult::SelectStreaming {
                            columns,
                            rows: Box::new(rows.into_iter().map(Ok)),
                            order_by: None,
                            limit: None,
                            offset: None,
                            distinct: false,
                        })
                    }
                    _ => unreachable!(),
                };
            }
        }

        // 🚀 Fast path: ORDER BY ST_DISTANCE should use spatial KNN index
        // Check if ORDER BY contains ST_DISTANCE or a column alias referencing ST_DISTANCE
        if let Some(ref order_by) = stmt.order_by {
            if order_by.iter().any(|ob| Self::expr_is_or_aliases_st_distance(&ob.expr, &stmt.columns)) {
                let result = self.execute_select_internal(stmt)?;
                return match result {
                    QueryResult::Select { columns, rows } => {
                        Ok(StreamingQueryResult::SelectStreaming {
                            columns,
                            rows: Box::new(rows.into_iter().map(Ok)),
                            order_by: None,
                            limit: None,
                            offset: None,
                            distinct: false,
                        })
                    }
                    _ => unreachable!(),
                };
            }
        }

        // 🚀 Pass bind parameters to optimizer (resolves ? inline, no AST clone needed).
        let has_params = Self::contains_parameter_stmt(stmt);
        if has_params {
            let params = self.evaluator.get_params();
            if let Some(err) = Self::validate_params_bound(stmt, &params) {
                return Err(err);
            }
            self.optimizer.lock().unwrap().set_params(params);
        }

        let plan = self.optimizer.lock().unwrap().optimize_select(stmt)?;

        if has_params {
            self.optimizer.lock().unwrap().clear_params();
        }

        // For PointQuery/RangeQuery, the plan already has resolved values — use original stmt.
        // For FullScan, WHERE still contains Parameter nodes — substitute needed.
        match plan.scan_method {
            super::optimizer::ScanMethod::PointQuery { ref table, ref column, ref value } => {
                self.execute_point_query_streaming(stmt, table, column, value)
            }
            super::optimizer::ScanMethod::RangeQuery { ref table, ref column, ref start, start_inclusive, ref end, end_inclusive } => {
                self.execute_range_query_streaming(stmt, table, column, start, start_inclusive, end, end_inclusive)
            }
            super::optimizer::ScanMethod::FullScan { .. } if has_params => {
                // FullScan with params: need to substitute WHERE for correct evaluation
                let resolved = self.substitute_params_stmt(stmt)?;
                self.execute_full_scan_streaming(&resolved, plan.scan_method.table_name())
            }
            super::optimizer::ScanMethod::FullScan { ref table } => {
                self.execute_full_scan_streaming(stmt, table)
            }
            _ => {
                // Fallback to materialized path (handles params via eval())
                let result = self.execute_select_internal(stmt)?;
                match result {
                    QueryResult::Select { columns, rows } => {
                        Ok(StreamingQueryResult::SelectStreaming {
                            columns,
                            rows: Box::new(rows.into_iter().map(Ok)),
                            order_by: None,
                            limit: None,
                            offset: None,
                            distinct: false,
                        })
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    /// Execute SELECT streaming (owned version — clones into ref version)
    fn execute_select_streaming(&self, stmt: SelectStmt) -> Result<StreamingQueryResult> {
        self.execute_select_streaming_ref(&stmt)
    }

    /// Check if an expression contains MATCH, ST_WITHIN, or ST_KNN that needs
    /// the materialized execution path with index pushdown fast paths.
    fn expr_needs_materialized_path(expr: &Expr) -> bool {
        match expr {
            Expr::Match { .. }
            | Expr::StWithin3D { .. } | Expr::StKnn3D { .. } | Expr::StRadius3D { .. } => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_needs_materialized_path(left) || Self::expr_needs_materialized_path(right)
            }
            _ => false,
        }
    }

    /// Check if ORDER BY expression is ST_DISTANCE or aliases a SELECT column that is ST_DISTANCE
    fn expr_is_or_aliases_st_distance(expr: &Expr, select_cols: &[SelectColumn]) -> bool {
        match expr {
            Expr::StDistance3D { .. } => true,
            Expr::Column(alias) => {
                for col in select_cols {
                    match col {
                        SelectColumn::Expr(e, Some(a)) if a == alias => {
                            return matches!(e, Expr::StDistance3D { .. });
                        }
                        _ => {}
                    }
                }
                false
            }
            _ => false,
        }
    }

    /// 🔥 点查询流式扫描（使用列索引）
    /// 
    /// ⚠️ 注意：这个方法通常只返回少量行（点查询），不需要批量优化
    fn execute_point_query_streaming(
        &self,
        stmt: &SelectStmt,
        table: &str,
        column: &str,
        value: &Value,
    ) -> Result<StreamingQueryResult> {
        let schema = self.db.get_table_schema(table)?;
        let columns = self.build_select_columns(&stmt.columns, &schema)?;

        // 🚀 Fast path for AUTO_INCREMENT primary key: skip column index, use direct LSM get
        let is_pk = schema.primary_key()
            .map(|pk| pk == column)
            .unwrap_or(false);

        let is_auto_increment_pk = is_pk && schema.is_primary_key_auto_increment();

        // 🚀 Fast path for non-AUTO_INCREMENT PK: use in-memory PK lookup
        // Bypasses disk-based column index (1.5ms → <5µs)
        let is_non_auto_pk = is_pk && !schema.is_primary_key_auto_increment();

        if is_non_auto_pk {
            // In-memory PK lookup: O(1) LRU cache instead of disk B-Tree
            let pk_key = crate::database::pk_cache::PkKey::from_value(value);
            let row_id = self.resolve_pk_with_cache(table, &pk_key, column, value)?;

            if let Some(rid) = row_id {
                let row = self.db.get_table_row_with_schema(table, rid, &schema)?;
                let result_rows: Vec<Result<Vec<Value>>> = match row {
                    Some(row) => {
                        let projected = Self::project_row_direct(&row, &stmt.columns, &columns, &schema);
                        vec![Ok(projected)]
                    }
                    None => vec![],
                };
                return Ok(StreamingQueryResult::SelectStreaming {
                    columns,
                    rows: Box::new(result_rows.into_iter()),
                    order_by: stmt.order_by.clone(),
                    limit: stmt.limit,
                    offset: stmt.offset,
                    distinct: stmt.distinct,
                });
            }
            // PK not found — return empty
            return Ok(StreamingQueryResult::SelectStreaming {
                columns,
                rows: Box::new(std::iter::empty()),
                order_by: stmt.order_by.clone(),
                limit: stmt.limit,
                offset: stmt.offset,
                distinct: stmt.distinct,
            });
        }

        if is_auto_increment_pk {
            // Direct LSM get by row_id — no column index needed
            let row_id = match value {
                Value::Integer(id) if *id >= 0 => *id as RowId,
                _ => {
                    // Non-integer or negative PK — return empty result
                    let column_names = self.build_select_columns(&stmt.columns, &schema)?;
                    return Ok(StreamingQueryResult::SelectStreaming {
                        columns: column_names,
                        rows: Box::new(std::iter::empty()),
                        order_by: stmt.order_by.clone(),
                        limit: stmt.limit,
                        offset: stmt.offset,
                        distinct: stmt.distinct,
                    });
                }
            };

            let row = self.db.get_table_row_with_schema(table, row_id, &schema)?;
            let result_rows: Vec<Result<Vec<Value>>> = match row {
                Some(row) => {
                    let projected = Self::project_row_direct(&row, &stmt.columns, &columns, &schema);
                    vec![Ok(projected)]
                }
                None => vec![],
            };

            return Ok(StreamingQueryResult::SelectStreaming {
                columns,
                rows: Box::new(result_rows.into_iter()),
                order_by: stmt.order_by.clone(),
                limit: stmt.limit,
                offset: stmt.offset,
                distinct: stmt.distinct,
            });
        }

        // Fallback: use column index
        let row_ids = self.db.query_by_column(table, column, value)?;

        if row_ids.is_empty() {
            // If the async pipeline is active, column indexes may not be built yet.
            // Fall back to full scan to avoid returning wrong empty results.
            if self.db.is_async_index_pipeline_active() {
                return self.execute_full_scan_streaming(stmt, table);
            }
            return Ok(StreamingQueryResult::SelectStreaming {
                columns,
                rows: Box::new(std::iter::empty()),
                order_by: stmt.order_by.clone(),
                limit: stmt.limit,
                offset: stmt.offset,
                distinct: stmt.distinct,
            });
        }

        // Sort row_ids and choose optimal fetch strategy
        let mut sorted_ids = row_ids;
        sorted_ids.sort_unstable();
        let min_id = sorted_ids[0];
        let max_id = *sorted_ids.last().unwrap();
        let density = sorted_ids.len() as f64 / (max_id - min_id + 1) as f64;

        let result_rows: Vec<Result<Vec<Value>>> = if density > 0.1 {
            // Dense result set: single range scan (sequential I/O >> random I/O)
            let id_set: std::collections::HashSet<u64> =
                sorted_ids.into_iter().map(|id| id as u64).collect();
            let start_key = self.db.make_composite_key(table, min_id);
            let end_key = self.db.make_composite_key(table, max_id + 1);
            let schema_c = schema.clone();
            let sel_c = stmt.columns.clone();
            let col_c = columns.clone();

            let lsm_rows = self.db.lsm_engine.scan_range(start_key, end_key)
                .unwrap_or_default();

            lsm_rows.into_iter().filter_map(move |(key, vd)| {
                let rid = (key & 0xFFFFFFFF) as RowId;
                if !id_set.contains(&(rid as u64)) || vd.deleted {
                    return None;
                }
                let data = match &vd.data {
                    crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                    _ => return None,
                };
                decode_row(data, &schema_c)
                    .ok()
                    .map(|row| Ok(Self::project_row_direct(&row, &sel_c, &col_c, &schema_c)))
            }).collect()
        } else {
            // Sparse result set: individual LSM gets
            sorted_ids.into_iter().filter_map(|row_id| {
                let key = self.db.make_composite_key(table, row_id);
                match self.db.lsm_engine.get(key) {
                    Ok(Some(vd)) if !vd.deleted => {
                        let data = match &vd.data {
                            crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                            _ => return None,
                        };
                        decode_row(data, &schema)
                            .ok()
                            .map(|row| Ok(Self::project_row_direct(&row, &stmt.columns, &columns, &schema)))
                    }
                    _ => None,
                }
            }).collect()
        };

        Ok(StreamingQueryResult::SelectStreaming {
            columns,
            rows: Box::new(result_rows.into_iter()),
            order_by: stmt.order_by.clone(),
            limit: stmt.limit,
            offset: stmt.offset,
            distinct: stmt.distinct,
        })
    }
    
    /// 🔥 范围查询流式扫描（智能路由：主键用 LSM scan，非主键用列索引）
    /// 
    /// ## 性能优化
    /// - **主键范围查询**：使用 LSM range scan（顺序扫描，6x 提速）
    /// - **非主键查询**：使用列索引 + batch_get（减少锁竞争）
    /// - 批次大小：1000 条（平衡内存与性能）
    /// - 内存友好：仍然是流式返回，不会一次性加载全部数据
    /// 
    /// ## 边界正确性
    /// - `start_inclusive`: 下界是否包含（>= vs >）
    /// - `end_inclusive`: 上界是否包含（<= vs <）
    #[allow(clippy::too_many_arguments)]
    fn execute_range_query_streaming(
        &self,
        stmt: &SelectStmt,
        table: &str,
        column: &str,
        start: &Value,
        start_inclusive: bool,
        end: &Value,
        end_inclusive: bool,
    ) -> Result<StreamingQueryResult> {
        let schema = self.db.get_table_schema(table)?;
        let columns = self.build_select_columns(&stmt.columns, &schema)?;
        
        // 🚀 优化路径1：主键范围查询使用 LSM range scan（顺序扫描）
        let pk_col = schema.primary_key().unwrap_or("id");
        if column == pk_col {
            return self.execute_primary_key_range_streaming(stmt, table, start, start_inclusive, end, end_inclusive);
        }
        
        // 🔧 路径2：非主键列使用列索引 + batch_get
        let row_ids = self.db.query_by_column_between(table, column, start, start_inclusive, end, end_inclusive)?;
        
        // 🚀 批量读取行数据（减少锁竞争）
        let db = self.db.clone();
        let table_name = table.to_string();
        let schema_clone = schema.clone();
        let select_cols = stmt.columns.clone();
        let columns_clone = columns.clone();
        
        // 批量 get 迭代器
        const BATCH_SIZE: usize = 1000;
        let total_rows = row_ids.len();
        
        let rows_iter = (0..total_rows).step_by(BATCH_SIZE).flat_map(move |batch_start| {
            let batch_end = (batch_start + BATCH_SIZE).min(total_rows);
            let batch_row_ids = &row_ids[batch_start..batch_end];
            
            // 构造批量 keys
            let keys: Vec<u64> = batch_row_ids.iter()
                .map(|&row_id| db.make_composite_key(&table_name, row_id))
                .collect();
            
            // 🔥 批量 get（关键优化）
            let batch_results = match db.lsm_engine.batch_get(&keys) {
                Ok(results) => results,
                Err(e) => {
                    debug_log!("[range_streaming] batch_get failed: {:?}", e);
                    return vec![Err(e)];
                }
            };
            
            // 反序列化并投影
            let mut processed = Vec::with_capacity(batch_results.len());
            for value_opt in batch_results {
                match value_opt {
                    Some(value_data) if !value_data.deleted => {
                        // 反序列化行
                        let data = match &value_data.data {
                            crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                            _ => {
                                processed.push(Err(StorageError::InvalidData("Unexpected blob".into())));
                                continue;
                            }
                        };
                        
                        match decode_row(data, &schema_clone) {
                            Ok(row) => {
                                let projected = Self::project_row_direct(&row, &select_cols, &columns_clone, &schema_clone);
                                processed.push(Ok(projected));
                            }
                            Err(e) => processed.push(Err(StorageError::InvalidData(format!("Deserialization failed: {}", e)))),
                        }
                    }
                    _ => {} // Deleted or not found, skip
                }
            }
            
            processed
        });
        
        Ok(StreamingQueryResult::SelectStreaming {
            columns,
            rows: Box::new(rows_iter),
            order_by: stmt.order_by.clone(),
            limit: stmt.limit,
            offset: stmt.offset,
            distinct: stmt.distinct,
        })
    }
    
    /// 🚀 主键范围查询流式扫描（使用 LSM range scan）
    /// 
    /// ## 关键优化
    /// - 直接使用 LSM range scan（顺序扫描 SSTables）
    /// - 避免遍历 425 个 L0 SSTables（batch_get 的瓶颈）
    /// - 利用 SSTable 的有序性，只扫描相关区间
    /// 
    /// ## 性能提升
    /// - 延迟：308ms → ~50ms（**6x 提速** ✅）
    /// - Bloom Filter 检查：425,000 次 → ~50 次（减少 **8500x**）
    /// - SSTable 锁操作：425,000 次 → ~50 次（减少 **8500x**）
    /// - 内存：0.30 MB（不变）
    fn execute_primary_key_range_streaming(
        &self,
        stmt: &SelectStmt,
        table: &str,
        start: &Value,
        start_inclusive: bool,
        end: &Value,
        end_inclusive: bool,
    ) -> Result<StreamingQueryResult> {
        let schema = self.db.get_table_schema(table)?;
        let columns = self.build_select_columns(&stmt.columns, &schema)?;
        
        // 提取 row_id 范围
        let start_row_id = match start {
            Value::Integer(i) => *i as u64,
            _ => return Err(StorageError::InvalidData(format!("Primary key must be integer, got {:?}", start))),
        };
        let end_row_id = match end {
            Value::Integer(i) => *i as u64,
            _ => return Err(StorageError::InvalidData(format!("Primary key must be integer, got {:?}", end))),
        };
        
        // 构造 LSM key range
        let mut start_key = self.db.make_composite_key(table, start_row_id);
        let mut end_key = self.db.make_composite_key(table, end_row_id);
        
        // 处理边界（将 > 转换为 >=，< 转换为 <=）
        if !start_inclusive {
            start_key += 1; // id > 100 等价于 id >= 101
        }
        if end_inclusive {
            end_key += 1; // id <= 200 等价于 id < 201
        }
        
        // 🚀 P2: 使用真正的流式迭代器（O(1) 内存占用，~20 KB）
        let lsm_iter = self.db.lsm_engine.scan_range_streaming(start_key, end_key)?;
        
        // 转换为 SQL 行并投影
        let schema_clone = schema.clone();
        let select_cols = stmt.columns.clone();
        let columns_clone = columns.clone();
        
        let rows_iter = lsm_iter.map(move |result| {
            // 处理迭代器错误
            let (_key, value_data) = match result {
                Ok(kv) => kv,
                Err(e) => return Err(e),
            };

            // 反序列化行
            let data = match &value_data.data {
                crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                _ => return Err(StorageError::InvalidData("Unexpected blob".into())),
            };

            match decode_row(data, &schema_clone) {
                Ok(row) => {
                    let projected = Self::project_row_direct(&row, &select_cols, &columns_clone, &schema_clone);
                    Ok(projected)
                }
                Err(e) => Err(StorageError::InvalidData(format!("Deserialization failed: {}", e))),
            }
        });
        
        Ok(StreamingQueryResult::SelectStreaming {
            columns,
            rows: Box::new(rows_iter),
            order_by: stmt.order_by.clone(),
            limit: stmt.limit,
            offset: stmt.offset,
            distinct: stmt.distinct,
        })
    }
    
    /// 🔥 全表扫描流式（现有实现）
    fn execute_full_scan_streaming(&self, stmt: &SelectStmt, table: &str) -> Result<StreamingQueryResult> {
        let schema = self.db.get_table_schema(table)?;
        let columns = self.build_select_columns(&stmt.columns, &schema)?;

        let row_iter = self.db.scan_table_rows_streaming(table)?;

        let where_clause = stmt.where_clause.clone();
        let _db = self.db.clone();
        let schema_clone = schema.clone();
        let columns_clone = columns.clone();
        let select_cols = stmt.columns.clone();
        let table_clone = table.to_string();

        // Check if WHERE can be evaluated positionally (bypasses HashMap)
        let use_positional = where_clause.as_ref().is_none_or(Self::can_eval_positional);
        // Metadata columns (__row_id__, __table__) are only needed for JOINs.
        // SELECT * is handled by project_row_direct without HashMap.
        let needs_metadata = select_cols.iter().any(|c| matches!(c,
            SelectColumn::Expr(_, _)
        )) || columns.iter().any(|c| c.starts_with("__"));

        if use_positional && !needs_metadata {
            // Fast path: no HashMap at all
            let filtered_iter = row_iter.filter_map(move |result| {
                match result {
                    Ok((_row_id, row)) => {
                        // WHERE filter using positional evaluation
                        if let Some(ref clause) = where_clause {
                            let matches = match Self::eval_expr_on_row(clause, &row, &schema_clone) {
                                Ok(Value::Bool(b)) => b,
                                Ok(Value::Integer(i)) => i != 0,
                                Ok(Value::Float(f)) => f != 0.0 && !f.is_nan(),
                                _ => false,
                            };
                            if !matches { return None; }
                        }

                        // Direct projection from Vec<Value>
                        let projected = Self::project_row_direct(&row, &select_cols, &columns_clone, &schema_clone);
                        Some(Ok(projected))
                    }
                    Err(e) => Some(Err(e)),
                }
            });

            return Ok(StreamingQueryResult::SelectStreaming {
                columns,
                rows: Box::new(filtered_iter),
                order_by: stmt.order_by.clone(),
                limit: stmt.limit,
                offset: stmt.offset,
                distinct: stmt.distinct,
            });
        }

        // Fallback: HashMap path for complex expressions / metadata columns
        let filtered_iter = row_iter.filter_map(move |result| {
            match result {
                Ok((row_id, row)) => {
                    let mut sql_row = match row_to_sql_row(&row, &schema_clone) {
                        Ok(r) => r,
                        Err(e) => return Some(Err(e)),
                    };

                    sql_row.insert("__row_id__".to_string(), Value::Integer(row_id as i64));
                    sql_row.insert("__table__".to_string(), Value::Text(table_clone.clone()));

                    if let Some(ref clause) = where_clause {
                        let matches = match Self::eval_expr_simple(clause, &sql_row) {
                            Ok(Value::Bool(b)) => b,
                            Ok(Value::Integer(i)) => i != 0,
                            Ok(Value::Float(f)) => f != 0.0 && !f.is_nan(),
                            _ => false,
                        };
                        if !matches { return None; }
                    }

                    let projected = Self::project_row_static(&sql_row, &select_cols, &columns_clone, &schema_clone);
                    Some(Ok(projected))
                }
                Err(e) => Some(Err(e)),
            }
        });
        
        Ok(StreamingQueryResult::SelectStreaming {
            columns,
            rows: Box::new(filtered_iter),
            order_by: stmt.order_by.clone(),
            limit: stmt.limit,
            offset: stmt.offset,
            distinct: stmt.distinct,
        })
    }
    
    /// 🔧 Helper: 构建 SELECT 列列表
    fn build_select_columns(&self, select_cols: &[SelectColumn], schema: &TableSchema) -> Result<Vec<String>> {
        let columns = if select_cols.len() == 1 && matches!(select_cols[0], SelectColumn::Star) {
            // 🚀 SELECT *: use cached column names (zero-alloc after first build)
            schema.column_names()
        } else {
            // 显式列名或表达式
            select_cols.iter().enumerate().map(|(idx, col)| {
                match col {
                    SelectColumn::Column(name) => name.clone(),
                    SelectColumn::ColumnWithAlias(_, alias) => alias.clone(),
                    SelectColumn::Expr(_, Some(alias)) => alias.clone(),
                    SelectColumn::Expr(_, None) => format!("expr_{}", idx),
                    SelectColumn::Star => "*".to_string(),
                }
            }).collect()
        };
        Ok(columns)
    }
    
    /// 🔧 Static helper for row projection (used in closures)
    /// 🚀 Lightweight expression evaluation for WHERE filters (no allocations)
    /// Handles simple comparisons, AND/OR, column references, and literals.
    /// Falls back to creating a QueryExecutor for complex expressions (MATCH, KNN, etc.)
    fn is_truthy(v: &Value) -> bool {
        match v {
            Value::Bool(b) => *b,
            Value::Integer(n) => *n != 0,
            Value::Float(f) => *f != 0.0 && !f.is_nan(),
            _ => false,
        }
    }

    /// Simple LIKE pattern matching: % = any sequence, _ = single char
    fn simple_like_match(text: &str, pattern: &str) -> bool {
        let t: Vec<char> = text.chars().collect();
        let p: Vec<char> = pattern.chars().collect();
        let mut dp = vec![vec![false; p.len() + 1]; t.len() + 1];
        dp[0][0] = true;
        for j in 1..=p.len() {
            if p[j - 1] == '%' { dp[0][j] = dp[0][j - 1]; }
        }
        for i in 1..=t.len() {
            for j in 1..=p.len() {
                if p[j - 1] == '%' {
                    dp[i][j] = dp[i][j - 1] || dp[i - 1][j];
                } else if p[j - 1] == '_' || p[j - 1] == t[i - 1] {
                    dp[i][j] = dp[i - 1][j - 1];
                }
            }
        }
        dp[t.len()][p.len()]
    }

    fn positional_add(l: &Value, r: &Value) -> Result<Value> {
        match (l, r) {
            (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a.wrapping_add(*b))),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
            (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 + b)),
            (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a + *b as f64)),
            _ => Ok(Value::Null),
        }
    }
    fn positional_sub(l: &Value, r: &Value) -> Result<Value> {
        match (l, r) {
            (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a.wrapping_sub(*b))),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
            (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 - b)),
            (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a - *b as f64)),
            _ => Ok(Value::Null),
        }
    }
    fn positional_mul(l: &Value, r: &Value) -> Result<Value> {
        match (l, r) {
            (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a.wrapping_mul(*b))),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a * b)),
            (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 * b)),
            (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a * *b as f64)),
            _ => Ok(Value::Null),
        }
    }
    fn positional_div(l: &Value, r: &Value) -> Result<Value> {
        match (l, r) {
            (Value::Integer(a), Value::Integer(b)) => {
                if *b == 0 { return Err(MoteDBError::DivisionByZero); }
                Ok(Value::Integer(a / b))
            }
            (Value::Float(a), Value::Float(b)) => {
                if *b == 0.0 { return Err(MoteDBError::DivisionByZero); }
                Ok(Value::Float(a / b))
            }
            (Value::Integer(a), Value::Float(b)) => {
                if *b == 0.0 { return Err(MoteDBError::DivisionByZero); }
                Ok(Value::Float(*a as f64 / b))
            }
            (Value::Float(a), Value::Integer(b)) => {
                if *b == 0 { return Err(MoteDBError::DivisionByZero); }
                Ok(Value::Float(a / *b as f64))
            }
            _ => Ok(Value::Null),
        }
    }
    fn positional_mod(l: &Value, r: &Value) -> Result<Value> {
        match (l, r) {
            (Value::Integer(a), Value::Integer(b)) => {
                if *b == 0 { return Err(MoteDBError::DivisionByZero); }
                Ok(Value::Integer(a % b))
            }
            _ => Ok(Value::Null),
        }
    }

    fn extract_f32_slice(v: &Value) -> Option<Vec<f32>> {
        match v {
            Value::Vector(vec) => Some(vec.iter().copied().collect()),
            _ => None,
        }
    }

    fn positional_vector_l2(l: &Value, r: &Value) -> Result<Value> {
        let v1 = Self::extract_f32_slice(l);
        let v2 = Self::extract_f32_slice(r);
        match (v1, v2) {
            (Some(a), Some(b)) => {
                if a.len() != b.len() {
                    return Err(MoteDBError::TypeError(format!(
                        "Vector dimension mismatch: {} vs {}", a.len(), b.len()
                    )));
                }
                let dist: f32 = a.iter().zip(b.iter()).map(|(x, y)| (x - y).powi(2)).sum::<f32>().sqrt();
                Ok(Value::Float(dist as f64))
            }
            _ => Ok(Value::Null),
        }
    }

    fn positional_vector_cosine(l: &Value, r: &Value) -> Result<Value> {
        let v1 = Self::extract_f32_slice(l);
        let v2 = Self::extract_f32_slice(r);
        match (v1, v2) {
            (Some(a), Some(b)) => {
                if a.len() != b.len() {
                    return Err(MoteDBError::TypeError(format!(
                        "Vector dimension mismatch: {} vs {}", a.len(), b.len()
                    )));
                }
                let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                let n1: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
                let n2: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
                if n1 == 0.0 || n2 == 0.0 { return Ok(Value::Float(1.0)); }
                let sim = (dot / (n1 * n2)).clamp(-1.0, 1.0);
                Ok(Value::Float((1.0 - sim) as f64))
            }
            _ => Ok(Value::Null),
        }
    }

    fn positional_vector_dot(l: &Value, r: &Value) -> Result<Value> {
        let v1 = Self::extract_f32_slice(l);
        let v2 = Self::extract_f32_slice(r);
        match (v1, v2) {
            (Some(a), Some(b)) => {
                if a.len() != b.len() {
                    return Err(MoteDBError::TypeError(format!(
                        "Vector dimension mismatch: {} vs {}", a.len(), b.len()
                    )));
                }
                let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                Ok(Value::Float(dot as f64))
            }
            _ => Ok(Value::Null),
        }
    }

    /// Evaluate function calls in the positional (no-HashMap) path.
    fn eval_function_positional(name: &str, args: &[Expr], row: &[Value], schema: &TableSchema) -> Result<Value> {
        let fname = name.to_lowercase();
        match fname.as_str() {
            "concat" => {
                let mut result = String::new();
                for arg in args {
                    match Self::eval_expr_on_row(arg, row, schema)? {
                        Value::Text(s) => result.push_str(&s),
                        Value::Integer(i) => { use std::fmt::Write; let _ = write!(result, "{}", i); }
                        Value::Float(f) => { use std::fmt::Write; let _ = write!(result, "{}", f); }
                        Value::Bool(b) => result.push_str(if b { "true" } else { "false" }),
                        Value::Null => return Ok(Value::Null),
                        other => result.push_str(&format!("{:?}", other)),
                    }
                }
                Ok(Value::Text(result))
            }
            "upper" | "lower" | "length" | "trim" | "ltrim" | "rtrim" => {
                let val = Self::eval_expr_on_row(&args[0], row, schema)?;
                match val {
                    Value::Text(s) => match fname.as_str() {
                        "upper" => Ok(Value::Text(s.to_uppercase())),
                        "lower" => Ok(Value::Text(s.to_lowercase())),
                        "length" => Ok(Value::Integer(s.chars().count() as i64)),
                        "trim" => Ok(Value::Text(s.trim().to_string())),
                        "ltrim" => Ok(Value::Text(s.trim_start().to_string())),
                        "rtrim" => Ok(Value::Text(s.trim_end().to_string())),
                        _ => Ok(Value::Text(s)),
                    },
                    _ => Ok(Value::Null),
                }
            }
            "abs" | "round" | "floor" | "ceil" | "log" | "ln" | "log10" | "sqrt" | "exp" => {
                let val = Self::eval_expr_on_row(&args[0], row, schema)?;
                match val {
                    Value::Integer(i) => match fname.as_str() {
                        "abs" => Ok(Value::Integer(i.abs())),
                        _ => {
                            let f = i as f64;
                            Ok(Value::Float(match fname.as_str() {
                                "round" => f.round(),
                                "floor" => f.floor(),
                                "ceil" => f.ceil(),
                                "log" | "log10" => f.log10(),
                                "ln" => f.ln(),
                                "sqrt" => f.sqrt(),
                                "exp" => f.exp(),
                                _ => f,
                            }))
                        }
                    },
                    Value::Float(f) => match fname.as_str() {
                        "abs" => Ok(Value::Float(f.abs())),
                        "round" => Ok(Value::Float(f.round())),
                        "floor" => Ok(Value::Float(f.floor())),
                        "ceil" => Ok(Value::Float(f.ceil())),
                        "log" | "log10" => Ok(Value::Float(f.log10())),
                        "ln" => Ok(Value::Float(f.ln())),
                        "sqrt" => Ok(Value::Float(f.sqrt())),
                        "exp" => Ok(Value::Float(f.exp())),
                        _ => Ok(Value::Float(f)),
                    },
                    _ => Ok(Value::Null),
                }
            }
            "coalesce" => {
                for arg in args {
                    let val = Self::eval_expr_on_row(arg, row, schema)?;
                    if !matches!(val, Value::Null) {
                        return Ok(val);
                    }
                }
                Ok(Value::Null)
            }
            "if" => {
                if args.len() >= 3 {
                    let cond = Self::eval_expr_on_row(&args[0], row, schema)?;
                    if Self::is_truthy(&cond) {
                        Self::eval_expr_on_row(&args[1], row, schema)
                    } else {
                        Self::eval_expr_on_row(&args[2], row, schema)
                    }
                } else {
                    Ok(Value::Null)
                }
            }
            "within_radius" => {
                if args.len() != 3 {
                    return Err(MoteDBError::InvalidArgument("WITHIN_RADIUS() takes 3 arguments".to_string()));
                }
                let point = Self::eval_expr_on_row(&args[0], row, schema)?;
                let center = Self::eval_expr_on_row(&args[1], row, schema)?;
                let radius = Self::eval_expr_on_row(&args[2], row, schema)?;

                use crate::types::Geometry;
                let (px, py) = match point {
                    Value::Spatial(Geometry::Point(p)) => (p.x, p.y),
                    Value::Spatial(Geometry::Point3D(p)) => (p.x, p.y),
                    _ => return Ok(Value::Bool(false)),
                };
                let (cx, cy) = match center {
                    Value::Spatial(Geometry::Point(p)) => (p.x, p.y),
                    Value::Spatial(Geometry::Point3D(p)) => (p.x, p.y),
                    _ => return Ok(Value::Bool(false)),
                };
                let r = match radius {
                    Value::Float(f) => f,
                    Value::Integer(i) => i as f64,
                    _ => return Ok(Value::Bool(false)),
                };
                let dist = ((px - cx).powi(2) + (py - cy).powi(2)).sqrt();
                Ok(Value::Bool(dist <= r))
            }
            "st_distance" => {
                if args.len() == 2 {
                    let p1 = Self::eval_expr_on_row(&args[0], row, schema)?;
                    let p2 = Self::eval_expr_on_row(&args[1], row, schema)?;
                    match (&p1, &p2) {
                        (Value::Spatial(a), Value::Spatial(b)) => {
                            let (x1, y1) = match a {
                                crate::types::Geometry::Point(p) => (p.x, p.y),
                                crate::types::Geometry::Point3D(p) => (p.x, p.y),
                                _ => return Ok(Value::Null),
                            };
                            let (x2, y2) = match b {
                                crate::types::Geometry::Point(p) => (p.x, p.y),
                                crate::types::Geometry::Point3D(p) => (p.x, p.y),
                                _ => return Ok(Value::Null),
                            };
                            Ok(Value::Float(((x1 - x2).powi(2) + (y1 - y2).powi(2)).sqrt()))
                        }
                        _ => Ok(Value::Null),
                    }
                } else {
                    Ok(Value::Null)
                }
            }
            "match" => {
                if args.len() != 2 {
                    return Ok(Value::Bool(false));
                }
                let col_name = match &args[0] {
                    Expr::Column(n) => n.clone(),
                    _ => return Ok(Value::Bool(false)),
                };
                let query_val = Self::eval_expr_on_row(&args[1], row, schema)?;
                let query_text = match query_val {
                    Value::Text(s) => s,
                    _ => return Ok(Value::Bool(false)),
                };
                let pos = schema.get_column_position(&col_name);
                let col_val = match pos {
                    Some(p) => row.get(p).cloned().unwrap_or(Value::Null),
                    None => return Ok(Value::Bool(false)),
                };
                match col_val {
                    Value::Text(ref text) => {
                        let text_lower = text.to_lowercase();
                        let query_lower = query_text.to_lowercase();
                        let terms: Vec<&str> = query_lower.split_whitespace().collect();
                        Ok(Value::Bool(terms.iter().all(|t| text_lower.contains(t))))
                    }
                    _ => Ok(Value::Bool(false)),
                }
            }
            _ => Ok(Value::Bool(false)),
        }
    }

    fn eval_expr_simple(expr: &Expr, row: &SqlRow) -> Result<Value> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let lv = Self::eval_expr_simple(left, row)?;
                let rv = Self::eval_expr_simple(right, row)?;
                match op {
                    BinaryOperator::Eq => {
                        // NULL = NULL should return false (SQL standard)
                        if matches!(&lv, Value::Null) || matches!(&rv, Value::Null) {
                            Ok(Value::Bool(false))
                        } else {
                            Ok(Value::Bool(lv == rv))
                        }
                    }
                    BinaryOperator::Ne => {
                        if matches!(&lv, Value::Null) || matches!(&rv, Value::Null) {
                            Ok(Value::Bool(false))
                        } else {
                            Ok(Value::Bool(lv != rv))
                        }
                    }
                    BinaryOperator::Lt | BinaryOperator::Le | BinaryOperator::Gt | BinaryOperator::Ge => {
                        if matches!(&lv, Value::Null) || matches!(&rv, Value::Null) {
                            Ok(Value::Bool(false))
                        } else {
                            Ok(Value::Bool(match op {
                                BinaryOperator::Lt => lv < rv,
                                BinaryOperator::Le => lv <= rv,
                                BinaryOperator::Gt => lv > rv,
                                BinaryOperator::Ge => lv >= rv,
                                _ => unreachable!(),
                            }))
                        }
                    }
                    BinaryOperator::And => {
                        let lb = Self::is_truthy(&lv);
                        let rb = Self::is_truthy(&rv);
                        Ok(Value::Bool(lb && rb))
                    }
                    BinaryOperator::Or => {
                        let lb = Self::is_truthy(&lv);
                        let rb = Self::is_truthy(&rv);
                        Ok(Value::Bool(lb || rb))
                    }
                    BinaryOperator::Add => Self::positional_add(&lv, &rv),
                    BinaryOperator::Sub => Self::positional_sub(&lv, &rv),
                    BinaryOperator::Mul => Self::positional_mul(&lv, &rv),
                    BinaryOperator::Div => Self::positional_div(&lv, &rv),
                    BinaryOperator::Mod => Self::positional_mod(&lv, &rv),
                    BinaryOperator::L2Distance => Self::positional_vector_l2(&lv, &rv),
                    BinaryOperator::CosineDistance => Self::positional_vector_cosine(&lv, &rv),
                    BinaryOperator::DotProduct => Self::positional_vector_dot(&lv, &rv),
                }
            }
            Expr::Column(name) => {
                // Try direct lookup, then strip table prefix (e.g., "users.age" → "age")
                if let Some(v) = row.get(name) {
                    Ok(v.clone())
                } else if name.contains('.') {
                    let col = name.rsplit('.').next().unwrap_or(name);
                    row.get(col).cloned().ok_or_else(|| MoteDBError::ColumnNotFound(name.clone()))
                } else {
                    Err(MoteDBError::ColumnNotFound(name.clone()))
                }
            }
            Expr::Literal(val) => Ok(val.clone()),
            Expr::UnaryOp { op: UnaryOperator::Not, expr } => {
                let v = Self::eval_expr_simple(expr, row)?;
                Ok(Value::Bool(!Self::is_truthy(&v)))
            }
            // For complex expressions that require the materialized path,
            // return the pre-computed result if available, otherwise false.
            // These expressions should never reach eval_expr_simple — they are
            // redirected to execute_select_internal by expr_needs_materialized_path().
            // The false fallback is a safety net to avoid returning wrong results.
            Expr::Match { column, query, .. } => {
                let has_score = row.keys().any(|k| k.starts_with("__text_score_"));
                if has_score {
                    Ok(Value::Bool(true))
                } else {
                    // Fallback: naive text scan when no FTS index
                    match row.get(column) {
                        Some(Value::Text(text)) => {
                            let text_lower = text.to_lowercase();
                            let query_lower = query.to_lowercase();
                            let terms: Vec<&str> = query_lower.split_whitespace().collect();
                            Ok(Value::Bool(terms.iter().all(|t| text_lower.contains(t))))
                        }
                        _ => Ok(Value::Bool(false)),
                    }
                }
            }
            Expr::FunctionCall { name, args, .. } => {
                let fname = name.to_lowercase();
                match fname.as_str() {
                    "concat" => {
                        let mut result = String::new();
                        for arg in args {
                            match Self::eval_expr_simple(arg, row)? {
                                Value::Text(s) => result.push_str(&s),
                                Value::Integer(i) => { use std::fmt::Write; let _ = write!(result, "{}", i); }
                                Value::Float(f) => { use std::fmt::Write; let _ = write!(result, "{}", f); }
                                Value::Bool(b) => result.push_str(if b { "true" } else { "false" }),
                                Value::Null => return Ok(Value::Null),
                                other => result.push_str(&format!("{:?}", other)),
                            }
                        }
                        Ok(Value::Text(result))
                    }
                    "upper" | "lower" | "length" | "trim" | "ltrim" | "rtrim" => {
                        let val = Self::eval_expr_simple(&args[0], row)?;
                        match val {
                            Value::Text(s) => match fname.as_str() {
                                "upper" => Ok(Value::Text(s.to_uppercase())),
                                "lower" => Ok(Value::Text(s.to_lowercase())),
                                "length" => Ok(Value::Integer(s.chars().count() as i64)),
                                "trim" => Ok(Value::Text(s.trim().to_string())),
                                "ltrim" => Ok(Value::Text(s.trim_start().to_string())),
                                "rtrim" => Ok(Value::Text(s.trim_end().to_string())),
                                _ => Ok(Value::Text(s)),
                            },
                            _ => Ok(Value::Null),
                        }
                    }
                    "abs" | "round" | "floor" | "ceil" | "log" | "ln" | "log10" | "sqrt" | "exp" => {
                        let val = Self::eval_expr_simple(&args[0], row)?;
                        match val {
                            Value::Integer(i) => match fname.as_str() {
                                "abs" => Ok(Value::Integer(i.abs())),
                                _ => {
                                    let f = i as f64;
                                    Ok(Value::Float(match fname.as_str() {
                                        "round" => f.round(),
                                        "floor" => f.floor(),
                                        "ceil" => f.ceil(),
                                        "log" | "log10" => f.log10(),
                                        "ln" => f.ln(),
                                        "sqrt" => f.sqrt(),
                                        "exp" => f.exp(),
                                        _ => f,
                                    }))
                                }
                            },
                            Value::Float(f) => match fname.as_str() {
                                "abs" => Ok(Value::Float(f.abs())),
                                "round" => Ok(Value::Float(f.round())),
                                "floor" => Ok(Value::Float(f.floor())),
                                "ceil" => Ok(Value::Float(f.ceil())),
                                "log" | "log10" => Ok(Value::Float(f.log10())),
                                "ln" => Ok(Value::Float(f.ln())),
                                "sqrt" => Ok(Value::Float(f.sqrt())),
                                "exp" => Ok(Value::Float(f.exp())),
                                _ => Ok(Value::Float(f)),
                            },
                            _ => Ok(Value::Null),
                        }
                    }
                    _ => Ok(Value::Bool(false)),
                }
            }
            _ => Ok(Value::Bool(false)),
        }
    }

    /// Check if an expression can be evaluated positionally (no complex features).
    /// Simple: Column, Literal, BinaryOp (comparison + AND/OR), UnaryOp::Not, IsNull.
    /// Check if expression tree contains any Expr::Parameter nodes
    fn contains_parameter(expr: &Expr) -> bool {
        match expr {
            Expr::Parameter(_) => true,
            Expr::BinaryOp { left, right, .. } =>
                Self::contains_parameter(left) || Self::contains_parameter(right),
            Expr::UnaryOp { expr, .. } => Self::contains_parameter(expr),
            Expr::IsNull { expr, .. } => Self::contains_parameter(expr),
            Expr::In { expr, list, .. } =>
                Self::contains_parameter(expr) || list.iter().any(Self::contains_parameter),
            Expr::Between { expr, low, high, .. } =>
                Self::contains_parameter(expr) || Self::contains_parameter(low) || Self::contains_parameter(high),
            Expr::Like { expr, pattern, .. } =>
                Self::contains_parameter(expr) || Self::contains_parameter(pattern),
            Expr::FunctionCall { args, .. } => args.iter().any(Self::contains_parameter),
            _ => false,
        }
    }

    /// Count the highest parameter index referenced in a statement.
    /// Returns 0 if no parameters found.
    pub fn max_parameter_index(stmt: &Statement) -> usize {
        fn walk_expr(expr: &Expr) -> usize {
            match expr {
                Expr::Parameter(idx) => *idx,
                Expr::BinaryOp { left, right, .. } =>
                    walk_expr(left).max(walk_expr(right)),
                Expr::UnaryOp { expr, .. } => walk_expr(expr),
                Expr::IsNull { expr, .. } => walk_expr(expr),
                Expr::In { expr, list, .. } =>
                    list.iter().fold(walk_expr(expr), |acc, e| acc.max(walk_expr(e))),
                Expr::Between { expr, low, high, .. } =>
                    walk_expr(expr).max(walk_expr(low)).max(walk_expr(high)),
                Expr::Like { expr, pattern, .. } =>
                    walk_expr(expr).max(walk_expr(pattern)),
                Expr::FunctionCall { args, .. } =>
                    args.iter().fold(0, |acc, e| acc.max(walk_expr(e))),
                _ => 0,
            }
        }
        fn walk_stmt(stmt: &Statement) -> usize {
            match stmt {
                Statement::Select(s) => s.where_clause.as_ref().map(walk_expr).unwrap_or(0)
                    .max(s.columns.iter().fold(0, |acc, c| acc.max(match c {
                        SelectColumn::Expr(e, _) => walk_expr(e),
                        _ => 0,
                    }))),
                Statement::Insert(i) => i.values.iter().fold(0, |acc, row| {
                    acc.max(row.iter().fold(0, |a, e| a.max(walk_expr(e))))
                }),
                Statement::Update(u) => {
                    let where_max = u.where_clause.as_ref().map(walk_expr).unwrap_or(0);
                    let set_max = u.assignments.iter().fold(0, |acc, (_, e)| acc.max(walk_expr(e)));
                    where_max.max(set_max)
                }
                Statement::Delete(d) => d.where_clause.as_ref().map(walk_expr).unwrap_or(0),
                _ => 0,
            }
        }
        walk_stmt(stmt)
    }

    fn can_eval_positional(expr: &Expr) -> bool {
        match expr {
            Expr::Column(_) | Expr::Literal(_) => true,
            Expr::BinaryOp { left, op, right } => {
                matches!(op,
                    BinaryOperator::Eq | BinaryOperator::Ne |
                    BinaryOperator::Lt | BinaryOperator::Le |
                    BinaryOperator::Gt | BinaryOperator::Ge |
                    BinaryOperator::And | BinaryOperator::Or |
                    BinaryOperator::Add | BinaryOperator::Sub |
                    BinaryOperator::Mul | BinaryOperator::Div |
                    BinaryOperator::Mod |
                    BinaryOperator::L2Distance | BinaryOperator::CosineDistance | BinaryOperator::DotProduct
                ) && Self::can_eval_positional(left)
                  && Self::can_eval_positional(right)
            }
            Expr::UnaryOp { op: UnaryOperator::Not, expr } => Self::can_eval_positional(expr),
            Expr::IsNull { .. } => true,
            Expr::In { .. } | Expr::Between { .. } | Expr::Like { .. } => true,
            Expr::FunctionCall { name, args, .. } => {
                let fname = name.to_lowercase();
                let handled = matches!(fname.as_str(),
                    "concat" | "upper" | "lower" | "length" | "trim" | "ltrim" | "rtrim" |
                    "abs" | "round" | "floor" | "ceil" | "log" | "ln" | "log10" | "sqrt" | "exp" |
                    "coalesce" | "if" |
                    "within_radius" | "st_distance" | "match"
                );
                handled && args.iter().all(Self::can_eval_positional)
            }
            Expr::Match { .. } => true,
            _ => false,
        }
    }

    /// Check if a SelectStmt contains any Parameter nodes.
    fn contains_parameter_stmt(stmt: &SelectStmt) -> bool {
        stmt.where_clause.as_ref().is_some_and(Self::contains_parameter)
            || stmt.columns.iter().any(|c| match c {
                SelectColumn::Expr(e, _) => Self::contains_parameter(e),
                _ => false,
            })
    }

    /// Validate that all Parameter nodes in stmt are bound to a value in params.
    fn validate_params_bound(stmt: &SelectStmt, params: &[Value]) -> Option<MoteDBError> {
        fn check_expr(expr: &Expr, params: &[Value]) -> Option<MoteDBError> {
            match expr {
                Expr::Parameter(idx) if *idx == 0 => Some(MoteDBError::InvalidArgument(
                    "Unnamed ? parameter not resolved (internal error)".to_string()
                )),
                Expr::Parameter(idx) => {
                    if params.get(idx - 1).is_none() {
                        return Some(MoteDBError::InvalidArgument(format!(
                            "Parameter ?{} not bound ({} parameters provided)", idx, params.len()
                        )));
                    }
                    None
                }
                Expr::BinaryOp { left, right, .. } =>
                    check_expr(left, params).or_else(|| check_expr(right, params)),
                Expr::UnaryOp { expr, .. } => check_expr(expr, params),
                Expr::IsNull { expr, .. } => check_expr(expr, params),
                _ => None,
            }
        }
        stmt.where_clause.as_ref().and_then(|w| check_expr(w, params))
    }

    /// Substitute all Expr::Parameter nodes with Expr::Literal using bound params.
    /// Returns a cloned SelectStmt with resolved values, enabling fast-path matching.
    fn substitute_params_stmt(&self, stmt: &SelectStmt) -> Result<SelectStmt> {
        let params = self.evaluator.get_params();
        let sub = |expr: &Expr| -> Result<Expr> { Self::substitute_expr(expr, &params) };

        let where_clause = match &stmt.where_clause {
            Some(w) => Some(sub(w)?),
            None => None,
        };

        let columns: Vec<SelectColumn> = stmt.columns.iter().map(|c| {
            match c {
                SelectColumn::Expr(e, alias) => {
                    match sub(e) {
                        Ok(resolved) => SelectColumn::Expr(resolved, alias.clone()),
                        Err(_) => c.clone(),
                    }
                }
                _ => c.clone(),
            }
        }).collect();

        Ok(SelectStmt {
            columns,
            from: stmt.from.clone(),
            where_clause,
            order_by: stmt.order_by.clone(),
            limit: stmt.limit,
            offset: stmt.offset,
            distinct: stmt.distinct,
            group_by: stmt.group_by.clone(),
            having: stmt.having.clone(),
            latest_by: stmt.latest_by.clone(),
        })
    }

    /// Recursively substitute Parameter nodes in an expression tree.
    fn substitute_expr(expr: &Expr, params: &[Value]) -> Result<Expr> {
        match expr {
            Expr::Parameter(idx) => {
                if *idx == 0 {
                    return Err(MoteDBError::InvalidArgument(
                        "Unnamed ? parameter not resolved (internal error)".to_string()
                    ));
                }
                let i = idx - 1;
                params.get(i).cloned()
                    .map(Expr::Literal)
                    .ok_or_else(|| MoteDBError::InvalidArgument(format!(
                        "Parameter ?{} not bound ({} parameters provided)", idx, params.len()
                    )))
            }
            Expr::BinaryOp { left, op, right } => {
                let l = Self::substitute_expr(left, params)?;
                let r = Self::substitute_expr(right, params)?;
                Ok(Expr::BinaryOp { left: Box::new(l), op: op.clone(), right: Box::new(r) })
            }
            Expr::UnaryOp { op, expr: inner } => {
                let e = Self::substitute_expr(inner, params)?;
                Ok(Expr::UnaryOp { op: op.clone(), expr: Box::new(e) })
            }
            Expr::IsNull { expr: inner, negated } => {
                let e = Self::substitute_expr(inner, params)?;
                Ok(Expr::IsNull { expr: Box::new(e), negated: *negated })
            }
            Expr::In { expr: inner, list, negated } => {
                let e = Self::substitute_expr(inner, params)?;
                let list2: Result<Vec<Expr>> = list.iter().map(|x| Self::substitute_expr(x, params)).collect();
                Ok(Expr::In { expr: Box::new(e), list: list2?, negated: *negated })
            }
            Expr::Between { expr: inner, low, high, negated } => {
                let e = Self::substitute_expr(inner, params)?;
                let l = Self::substitute_expr(low, params)?;
                let h = Self::substitute_expr(high, params)?;
                Ok(Expr::Between { expr: Box::new(e), low: Box::new(l), high: Box::new(h), negated: *negated })
            }
            Expr::Like { expr: inner, pattern, negated } => {
                let e = Self::substitute_expr(inner, params)?;
                let p = Self::substitute_expr(pattern, params)?;
                Ok(Expr::Like { expr: Box::new(e), pattern: Box::new(p), negated: *negated })
            }
            Expr::FunctionCall { name, args, distinct } => {
                let args2: Result<Vec<Expr>> = args.iter().map(|x| Self::substitute_expr(x, params)).collect();
                Ok(Expr::FunctionCall { name: name.clone(), args: args2?, distinct: *distinct })
            }
            // All other variants are cloned as-is (Column, Literal, etc.)
            _ => Ok(expr.clone()),
        }
    }

    /// Evaluate expression directly on Vec<Value> using schema positions.
    /// Bypasses HashMap creation entirely.
    fn eval_expr_on_row(expr: &Expr, row: &[Value], schema: &TableSchema) -> Result<Value> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let lv = Self::eval_expr_on_row(left, row, schema)?;
                let rv = Self::eval_expr_on_row(right, row, schema)?;
                match op {
                    BinaryOperator::Eq => {
                        if matches!(&lv, Value::Null) || matches!(&rv, Value::Null) {
                            Ok(Value::Bool(false))
                        } else {
                            Ok(Value::Bool(lv == rv))
                        }
                    }
                    BinaryOperator::Ne => {
                        if matches!(&lv, Value::Null) || matches!(&rv, Value::Null) {
                            Ok(Value::Bool(false))
                        } else {
                            Ok(Value::Bool(lv != rv))
                        }
                    }
                    BinaryOperator::Lt | BinaryOperator::Le | BinaryOperator::Gt | BinaryOperator::Ge => {
                        if matches!(&lv, Value::Null) || matches!(&rv, Value::Null) {
                            Ok(Value::Bool(false))
                        } else {
                            Ok(Value::Bool(match op {
                                BinaryOperator::Lt => lv < rv,
                                BinaryOperator::Le => lv <= rv,
                                BinaryOperator::Gt => lv > rv,
                                BinaryOperator::Ge => lv >= rv,
                                _ => unreachable!(),
                            }))
                        }
                    }
                    BinaryOperator::And => {
                        Ok(Value::Bool(Self::is_truthy(&lv) && Self::is_truthy(&rv)))
                    }
                    BinaryOperator::Or => {
                        Ok(Value::Bool(Self::is_truthy(&lv) || Self::is_truthy(&rv)))
                    }
                    BinaryOperator::Add => Self::positional_add(&lv, &rv),
                    BinaryOperator::Sub => Self::positional_sub(&lv, &rv),
                    BinaryOperator::Mul => Self::positional_mul(&lv, &rv),
                    BinaryOperator::Div => Self::positional_div(&lv, &rv),
                    BinaryOperator::Mod => Self::positional_mod(&lv, &rv),
                    BinaryOperator::L2Distance => Self::positional_vector_l2(&lv, &rv),
                    BinaryOperator::CosineDistance => Self::positional_vector_cosine(&lv, &rv),
                    BinaryOperator::DotProduct => Self::positional_vector_dot(&lv, &rv),
                }
            }
            Expr::Column(name) => {
                // Try direct lookup, then strip table prefix (e.g., "users.id" → "id")
                let col_name = if name.contains('.') {
                    name.rsplit('.').next().unwrap_or(name)
                } else {
                    name
                };
                schema.get_column_position(col_name)
                    .and_then(|pos| row.get(pos).cloned())
                    .ok_or_else(|| MoteDBError::ColumnNotFound(name.clone()))
            }
            Expr::Literal(val) => Ok(val.clone()),
            Expr::Parameter(_) => {
                // Parameters need evaluator state, skip for positional eval
                Ok(Value::Bool(false))
            }
            Expr::UnaryOp { op: UnaryOperator::Not, expr: inner } => {
                let v = Self::eval_expr_on_row(inner, row, schema)?;
                // NOT NULL should be false (NULL), not true
                if matches!(v, Value::Null) {
                    Ok(Value::Bool(false))
                } else {
                    Ok(Value::Bool(!Self::is_truthy(&v)))
                }
            }
            Expr::IsNull { expr, negated } => {
                let v = Self::eval_expr_on_row(expr, row, schema)?;
                let is_null = matches!(v, Value::Null);
                Ok(Value::Bool(if *negated { !is_null } else { is_null }))
            }
            Expr::In { expr, list, negated } => {
                let val = Self::eval_expr_on_row(expr, row, schema)?;
                if matches!(val, Value::Null) {
                    return Ok(Value::Bool(false));
                }
                let mut found = false;
                for item in list {
                    let item_val = Self::eval_expr_on_row(item, row, schema)?;
                    if val == item_val {
                        found = true;
                        break;
                    }
                }
                Ok(Value::Bool(if *negated { !found } else { found }))
            }
            Expr::Between { expr, low, high, negated } => {
                let val = Self::eval_expr_on_row(expr, row, schema)?;
                let low_val = Self::eval_expr_on_row(low, row, schema)?;
                let high_val = Self::eval_expr_on_row(high, row, schema)?;
                if matches!(val, Value::Null) || matches!(low_val, Value::Null) || matches!(high_val, Value::Null) {
                    return Ok(Value::Bool(false));
                }
                let in_range = val >= low_val && val <= high_val;
                Ok(Value::Bool(if *negated { !in_range } else { in_range }))
            }
            Expr::Like { expr, pattern, negated } => {
                let val = Self::eval_expr_on_row(expr, row, schema)?;
                let pat = Self::eval_expr_on_row(pattern, row, schema)?;
                // NULL LIKE anything = false, NULL NOT LIKE anything = false (SQL NULL semantics)
                if matches!(val, Value::Null) || matches!(pat, Value::Null) {
                    return Ok(Value::Bool(false));
                }
                let matches = match (&val, &pat) {
                    (Value::Text(s), Value::Text(p)) => {
                        Self::simple_like_match(s, p)
                    }
                    _ => false,
                };
                Ok(Value::Bool(if *negated { !matches } else { matches }))
            }
            Expr::FunctionCall { name, args, .. } => {
                Self::eval_function_positional(name, args, row, schema)
            }
            Expr::Match { column, query, .. } => {
                let pos = schema.get_column_position(column);
                match pos {
                    Some(p) => {
                        match row.get(p) {
                            Some(Value::Text(text)) => {
                                let text_lower = text.to_lowercase();
                                let query_lower = query.to_lowercase();
                                let terms: Vec<&str> = query_lower.split_whitespace().collect();
                                Ok(Value::Bool(terms.iter().all(|t| text_lower.contains(t))))
                            }
                            _ => Ok(Value::Bool(false)),
                        }
                    }
                    None => Ok(Value::Bool(false)),
                }
            }
            _ => Ok(Value::Bool(false)),
        }
    }

    fn project_row_static(
        sql_row: &SqlRow,
        select_cols: &[SelectColumn],
        columns: &[String],
        schema: &TableSchema,
    ) -> Vec<Value> {
        if select_cols.len() == 1 && matches!(select_cols[0], SelectColumn::Star) {
            // SELECT * - 按 schema 顺序返回所有列
            let table_name = schema.name.as_str();
            schema.columns.iter()
                .map(|col_def| {
                    sql_row.get(&col_def.name).cloned().unwrap_or_else(|| {
                        // Fallback: try qualified name (e.g., "table.column")
                        if !table_name.is_empty() {
                            let qname = format!("{}.{}", table_name, col_def.name);
                            sql_row.get(&qname).cloned().unwrap_or(Value::Null)
                        } else {
                            Value::Null
                        }
                    })
                })
                .collect()
        } else {
            // 显式列名
            columns.iter().zip(select_cols.iter())
                .map(|(_alias, col_spec)| {
                    match col_spec {
                        SelectColumn::Column(name) => {
                            sql_row.get(name).cloned().unwrap_or(Value::Null)
                        }
                        SelectColumn::ColumnWithAlias(name, _) => {
                            sql_row.get(name).cloned().unwrap_or(Value::Null)
                        }
                        SelectColumn::Star => Value::Null,
                        SelectColumn::Expr(expr, _) => {
                            // Evaluate expression against the SQL row
                            match Self::eval_expr_simple(expr, sql_row) {
                                Ok(v) => v,
                                Err(_) => Value::Null,
                            }
                        }
                    }
                })
                .collect()
        }
    }

    /// 🚀 P0 Optimization: Direct row projection (skips HashMap conversion)
    ///
    /// For PK point queries, the old path was:
    ///   Row(Vec<Value>) → SqlRow(HashMap) → project → Vec<Value>
    ///   = N clones + N HashMap inserts + N lookups
    ///
    /// New path:
    ///   Row(Vec<Value>) → direct index → Vec<Value>
    ///   = M clones (M = selected columns, no HashMap)
    fn project_row_direct(
        row: &Row,
        select_cols: &[SelectColumn],
        columns: &[String],
        schema: &TableSchema,
    ) -> Vec<Value> {
        if select_cols.len() == 1 && matches!(select_cols[0], SelectColumn::Star) {
            // SELECT * — return all columns in schema order (cheap clone)
            row.to_vec()
        } else {
            // Explicit columns — use column position as index into Vec
            columns.iter().zip(select_cols.iter())
                .map(|(_alias, col_spec)| {
                    let col_name = match col_spec {
                        SelectColumn::Column(name) => name,
                        SelectColumn::ColumnWithAlias(name, _) => name,
                        SelectColumn::Star => return Value::Null,
                        SelectColumn::Expr(expr, _) => {
                            return match Self::eval_expr_on_row(expr, row, schema) {
                                Ok(v) => v,
                                Err(_) => Value::Null,
                            };
                        }
                    };
                    // Look up column position in schema (O(1) via column_map HashMap)
                    // Handle table-qualified names: "users.id" → "id"
                    let lookup_name = if col_name.contains('.') {
                        col_name.rsplit('.').next().unwrap_or(col_name)
                    } else {
                        col_name
                    };
                    if let Some(pos) = schema.get_column_position(lookup_name) {
                        row.get(pos).cloned().unwrap_or(Value::Null)
                    } else {
                        Value::Null
                    }
                })
                .collect()
        }
    }

    /// Internal SELECT execution (takes &SelectStmt to allow reuse in subqueries)
    fn execute_select_internal(&self, stmt: &SelectStmt) -> Result<QueryResult> {
        // 🚀 Substitute bind parameters before executing
        let resolved_stmt;
        let stmt = if Self::contains_parameter_stmt(stmt) {
            match self.substitute_params_stmt(stmt) {
                Ok(s) => { resolved_stmt = s; &resolved_stmt as &SelectStmt }
                Err(e) => return Err(e),
            }
        } else {
            stmt
        };

        // 🆕 FAST PATH -4: SELECT without FROM clause (e.g., SELECT LAST_INSERT_ID())
        // → Evaluate expressions directly without table scan
        if stmt.from.is_none() {
            let empty_row = SqlRow::new();
            let mut result_row = Vec::new();
            let mut column_names = Vec::new();
            
            for col in &stmt.columns {
                match col {
                    SelectColumn::Expr(expr, alias) => {
                        let value = self.evaluator.eval(expr, &empty_row)?;
                        let col_name = alias.clone().unwrap_or_else(|| format!("{:?}", expr));
                        column_names.push(col_name);
                        result_row.push(value);
                    }
                    SelectColumn::Star => {
                        return Err(MoteDBError::InvalidArgument(
                            "SELECT * requires a FROM clause".to_string()
                        ));
                    }
                    SelectColumn::Column(name) | SelectColumn::ColumnWithAlias(name, _) => {
                        return Err(MoteDBError::InvalidArgument(
                            format!("Column {} requires a FROM clause", name)
                        ));
                    }
                }
            }
            
            return Ok(QueryResult::Select {
                columns: column_names,
                rows: vec![result_row],
            });
        }
        
        // From here on, we know stmt.from is Some, so unwrap is safe

        // 🆕 Columnar SELECT for TimeSeries tables
        // Pattern: SELECT cols FROM ts_table WHERE ts BETWEEN a AND b
        // → Route to columnar store with time-range pruning + column projection
        if let TableRef::Table { name: table_name, .. } = stmt.from.as_ref().unwrap() {
            if let Ok(schema) = self.db.get_table_schema(table_name) {
                if schema.table_type == crate::types::TableType::TimeSeries {
                    if let Some(result) = self.try_columnar_select(stmt, &schema)? {
                        return Ok(result);
                    }
                    // Fall through to LSM full scan for complex queries (JOINs, subqueries, etc.)
                }
            }
        }

        // 🚀 FAST PATH -3: Primary key point query optimization (P0)
        // Pattern: SELECT * FROM table WHERE primary_key = value
        // → Direct LSM get by row_id (165x faster, no MemTable scan!)
        if let Some(result) = self.try_optimize_primary_key_point_query(stmt)? {
            return Ok(result);
        }
        
        // 🚀 FAST PATH -2: ORDER BY primary key optimization (P0)
        // Pattern: SELECT * FROM table ORDER BY id [ASC/DESC] [LIMIT k]
        // → Use primary key index scan (600x faster, 280x less memory!)
        if let Some(result) = self.try_optimize_primary_key_order_by(stmt)? {
            return Ok(result);
        }
        
        // 🚀 FAST PATH -1: ORDER BY vector distance optimization (P0)
        // Pattern: SELECT * FROM table ORDER BY column <-> [...] LIMIT k
        // → Directly use vector index search (724x faster!)
        if let Some(plan) = self.try_optimize_vector_order_by(stmt)? {
            return self.execute_vector_order_by_plan(stmt, &plan);
        }

        // 🚀 FAST PATH -1b: Spatial ORDER BY ST_DISTANCE optimization
        // Pattern: SELECT ... FROM table ORDER BY ST_DISTANCE(col, x, y) LIMIT k
        // → Use spatial KNN index (50x faster than full scan + per-row distance calc)
        if let Some(result) = self.try_optimize_spatial_order_by(stmt)? {
            return Ok(result);
        }

        // 🚀 FAST PATH 0: Vector search optimization (P0)
        // Pattern: SELECT * FROM table WHERE VECTOR_SEARCH(column, [...], k)
        if let Some(ref where_clause) = stmt.where_clause {
            if let Some((table_name, col_name, query_vector, k)) = self.try_extract_vector_search(where_clause, stmt.from.as_ref().unwrap()) {
                // ⚡ Ultra-fast path: Use vector index directly
                // Resolve index name via registry (supports custom index names)
                let index_name = self.db.index_registry.find_by_column(
                    &table_name, &col_name,
                    crate::database::index_metadata::IndexType::Vector
                ).unwrap_or_else(|| format!("{}_{}", table_name, col_name));
                match self.db.vector_search(&index_name, &query_vector, k) {
                    Ok(results) => {
                        // Load rows for the result row_ids
                        let schema = self.db.get_table_schema(&table_name)?;
                        
                        // 🚀 P1 优化：预分配 k 个结果
                        let mut sql_rows = Vec::with_capacity(k.min(results.len()));
                        
                        for (row_id, _distance) in results {
                            if let Ok(Some(row)) = self.db.get_table_row(&table_name, row_id) {
                                let sql_row = row_to_sql_row(&row, &schema)?;
                                sql_rows.push((row_id, sql_row));
                            }
                        }
                        
                        // Add table prefix
                        // 🚀 P1 优化：使用 take() 避免克隆所有值
                        for (row_id, sql_row) in &mut sql_rows {
                            let mut new_sql_row = SqlRow::new();
                            new_sql_row.insert("__row_id__".to_string(), Value::Integer(*row_id as i64));
                            new_sql_row.insert("__table__".to_string(), Value::Text(table_name.clone()));
                            
                            // 使用 drain() 移动值而不是克隆
                            let old_row = std::mem::take(sql_row);
                            for (col_name, val) in old_row.into_iter() {
                                let qualified_name = Self::make_qualified_name(&table_name, &col_name);
                                new_sql_row.insert(qualified_name, val);  // ✅ 移动，不克隆
                            }
                            *sql_row = new_sql_row;
                        }
                        
                        // Project columns and return
                        let (column_names, result_rows) = self.project_columns(&stmt.columns, &sql_rows, &schema)?;
                        
                        return Ok(QueryResult::Select {
                            columns: column_names,
                            rows: result_rows,
                        });
                    }
                    Err(_) => {
                        // Fallback to normal execution if vector search fails
                    }
                }
            }
        }

        // 🚀 FAST PATH 0a: Text Search (MATCH AGAINST) optimization
        // Pattern: SELECT ... FROM table WHERE MATCH(col) AGAINST('query') [ORDER BY score] [LIMIT k]
        // → Use text index directly (50x faster than full table scan + per-row search_ranked)
        if let Some(ref where_clause) = stmt.where_clause {
            if let TableRef::Table { name: table_name, .. } = stmt.from.as_ref().unwrap() {
                if let Some(result) = self.try_text_search_fast_path(stmt, where_clause, table_name)? {
                    return Ok(result);
                }
            }
        }

        // 🚀 FAST PATH 0b: Spatial (ST_WITHIN / ST_KNN) optimization
        // Pattern: SELECT ... FROM table WHERE ST_WITHIN(col, ...) [LIMIT k]
        //          SELECT ... FROM table WHERE ST_KNN(col, ...) [LIMIT k]
        // → Use spatial index directly (50x faster than full table scan + per-row spatial query)
        if let Some(ref where_clause) = stmt.where_clause {
            if let TableRef::Table { name: table_name, .. } = stmt.from.as_ref().unwrap() {
                if let Some(result) = self.try_spatial_fast_path(stmt, where_clause, table_name)? {
                    return Ok(result);
                }
            }
        }

        // 🚀 FAST PATH 1: Aggregate query optimization (P0-2)
        // Pattern: SELECT COUNT(*) FROM table [WHERE indexed_col = value]
        if self.has_only_count_aggregate(&stmt.columns) && stmt.group_by.is_none() {
            // Check if WHERE clause can use index
            if let Some(ref where_clause) = stmt.where_clause {
                if let Some((col_name, target_value)) = self.try_extract_point_query(where_clause) {
                    if let TableRef::Table { name: table_name, .. } = stmt.from.as_ref().unwrap() {
                        let index_name = format!("{}.{}", table_name, col_name);
                        if self.db.column_indexes.contains_key(&index_name) {
                            // ⚡ Ultra-fast path: Use index to get count
                            match self.db.query_by_column(table_name, &col_name, &target_value) {
                                Ok(row_ids) if !row_ids.is_empty() || !self.db.is_async_index_pipeline_active() => {
                                    let count = row_ids.len() as i64;
                                    return Ok(QueryResult::Select {
                                        columns: vec!["COUNT(*)".to_string()],
                                        rows: vec![vec![Value::Integer(count)]],
                                    });
                                }
                                Ok(_) | Err(_) => {
                                    // Fallback: index empty + pipeline active, or query error
                                }
                            }
                        }
                    }
                }
            } else {
                // 🚀 COUNT(*) without WHERE — O(1) from row counter
                if let TableRef::Table { name: table_name, .. } = stmt.from.as_ref().unwrap() {
                    let count = if let Some(counter) = self.db.table_row_count.get(table_name) {
                        counter.load(std::sync::atomic::Ordering::Relaxed) as i64
                    } else {
                        // Fallback: streaming scan if counter not initialized
                        let row_iter = self.db.scan_table_rows_streaming(table_name)?;
                        let mut c = 0i64;
                        for result in row_iter {
                            let _ = result?;
                            c += 1;
                        }
                        c
                    };

                    return Ok(QueryResult::Select {
                        columns: vec!["COUNT(*)".to_string()],
                        rows: vec![vec![Value::Integer(count)]],
                    });
                }
            }
        }
        
        // 🚀 FAST PATH 2: Try to use column index for WHERE optimization
        // 🆕 P0 OPTIMIZATION: Extract LIMIT early and pass to storage layer
        let storage_limit = self.calculate_storage_limit(stmt);
        
        // Priority: Range query > Point query > Full scan
        let (all_sql_rows, combined_schema) = if let Some(ref where_clause) = stmt.where_clause {
            // Try range query first (dual-bound: col > X AND col < Y)
            if let Some((col_name, lower_value, lower_op, upper_value, upper_op)) = self.try_extract_range_query(where_clause) {
                if let TableRef::Table { name: table_name, .. } = stmt.from.as_ref().unwrap() {
                    let index_name = format!("{}.{}", table_name, col_name);
                    let index_exists = self.db.column_indexes.contains_key(&index_name);
                    
                    if index_exists {
                        // ⚡ Fast path: Use optimized dual-bound range query (single B-Tree scan)
                        use crate::sql::ast::BinaryOperator;
                        
                        // Convert operators to inclusive flags
                        let lower_inclusive = matches!(lower_op, BinaryOperator::Ge);
                        let upper_inclusive = matches!(upper_op, BinaryOperator::Le);
                        
                        // Single index scan with proper boundaries
                        let row_ids = self.db.query_by_column_between(
                            table_name, &col_name,
                            &lower_value, lower_inclusive,
                            &upper_value, upper_inclusive
                        )?;

                        // If column index is empty (async pipeline not yet built), fall back to full scan
                        if row_ids.is_empty() && self.db.is_async_index_pipeline_active() {
                            let row_iter = self.db.scan_table_rows_streaming(table_name)?;
                            let schema = self.db.get_table_schema(table_name)?;
                            let mut sql_rows = Vec::new();
                            for result in row_iter {
                                let (row_id, row) = result?;
                                let sql_row = row_to_sql_row(&row, &schema)?;
                                sql_rows.push((row_id, sql_row));
                            }
                            let prefix = table_name;
                            for (row_id, sql_row) in &mut sql_rows {
                                let mut new_sql_row = SqlRow::new();
                                new_sql_row.insert("__row_id__".to_string(), Value::Integer(*row_id as i64));
                                new_sql_row.insert("__table__".to_string(), Value::Text(table_name.clone()));
                                let old_row = std::mem::take(sql_row);
                                for (col_name, val) in old_row.into_iter() {
                                    let qualified_name = Self::make_qualified_name(prefix, &col_name);
                                    new_sql_row.insert(qualified_name, val);
                                }
                                *sql_row = new_sql_row;
                            }
                            let mut prefixed_schema = (*schema).clone();
                            for col in &mut prefixed_schema.columns {
                                col.name = format!("{}.{}", prefix, col.name);
                            }
                            (sql_rows, Arc::new(prefixed_schema))
                        } else {
                        
                        // 🚀 P0 OPTIMIZATION: Smart index selection based on selectivity
                        // 
                        // Strategy:
                        // - Selectivity < 10%: Use index (faster for small result sets)
                        // - Selectivity >= 10%: Use table scan (faster for large result sets)
                        // 
                        // Why? Index scan has overhead:
                        // - B-Tree lookup cost
                        // - 30K random reads (fragmented access)
                        // - Cache unfriendly
                        // 
                        // Table scan is sequential:
                        // - Single range scan
                        // - Cache friendly
                        // - Better for large result sets
                        let result_count = row_ids.len();
                        let table_count = self.db.estimate_table_row_count(table_name)?;
                        let selectivity = if table_count > 0 {
                            result_count as f64 / table_count as f64
                        } else {
                            0.0
                        };
                        
                        const SELECTIVITY_THRESHOLD: f64 = 0.15; // 15%
                        
                        if selectivity < SELECTIVITY_THRESHOLD {
                            // ✅ Low selectivity (< 10%): Use index (faster!)
                            debug_log!(
                                "[Smart Index] Using INDEX SCAN: {} rows / {} total = {:.1}% selectivity",
                                result_count, table_count, selectivity * 100.0
                            );
                        
                        // 🚀 Use batch get for better performance (auto-optimizes for continuous IDs)
                        let schema = self.db.get_table_schema(table_name)?;
                        let batch_rows = self.db.get_table_rows_batch(table_name, &row_ids)?;
                        
                        // Convert to sql_rows
                        // 🚀 P1 优化：预分配 row_ids 大小
                        let mut sql_rows = Vec::with_capacity(row_ids.len());
                        for (row_id, row_opt) in batch_rows {
                            if let Some(row) = row_opt {
                                let sql_row = row_to_sql_row(&row, &schema)?;
                                sql_rows.push((row_id, sql_row));
                            }
                        }
                        
                        // Add table prefix
                        // 🚀 P1 优化：使用 take() 避免克隆所有值
                        let prefix = table_name;
                        for (row_id, sql_row) in &mut sql_rows {
                            let mut new_sql_row = SqlRow::new();
                            new_sql_row.insert("__row_id__".to_string(), Value::Integer(*row_id as i64));
                            new_sql_row.insert("__table__".to_string(), Value::Text(table_name.clone()));
                            
                            // 使用 drain() 移动值而不是克隆
                            let old_row = std::mem::take(sql_row);
                            for (col_name, val) in old_row.into_iter() {
                                let qualified_name = Self::make_qualified_name(prefix, &col_name);
                                new_sql_row.insert(qualified_name, val);  // ✅ 移动，不克隆
                            }
                            *sql_row = new_sql_row;
                        }
                        
                        let mut prefixed_schema = (*schema).clone();
                        for col in &mut prefixed_schema.columns {
                            col.name = format!("{}.{}", prefix, col.name);
                        }
                        
                        (sql_rows, Arc::new(prefixed_schema))
                        } else {
                            // 🚀 High selectivity (>= 15%): Use真正的流式扫描 (O(1) memory!)
                            debug_log!(
                                "[Smart Index] Using STREAMING SCAN: {} rows / {} total = {:.1}% selectivity (>= 15%)",
                                result_count, table_count, selectivity * 100.0
                            );
                            
                            // 🚀 Use真正的流式扫描 - 每次只在内存中保留一行
                            let row_iter = self.db.scan_table_rows_streaming(table_name)?;
                            let schema = self.db.get_table_schema(table_name)?;
                            
                            let mut filtered_rows = Vec::new();
                            
                            for result in row_iter {
                                let (row_id, row) = result?;
                                
                                // Get column value
                                let col_index = schema.columns.iter()
                                    .position(|c| c.name == col_name)
                                    .ok_or_else(|| StorageError::InvalidData(
                                        format!("Column '{}' not found", col_name)
                                    ))?;
                                
                                let col_value = row.get(col_index)
                                    .ok_or_else(|| StorageError::InvalidData(
                                        "Column value missing".into()
                                    ))?;
                                
                                // Check range condition
                                let lower_ok = if lower_inclusive {
                                    col_value >= &lower_value
                                } else {
                                    col_value > &lower_value
                                };
                                
                                let upper_ok = if upper_inclusive {
                                    col_value <= &upper_value
                                    } else {
                                        col_value < &upper_value
                                    };
                                    
                                    if lower_ok && upper_ok {
                                        let sql_row = row_to_sql_row(&row, &schema)?;
                                        filtered_rows.push((row_id, sql_row));
                                    }
                            }
                            
                            // Add table prefix
                            let prefix = table_name;
                            for (row_id, sql_row) in &mut filtered_rows {
                                let mut new_sql_row = SqlRow::new();
                                new_sql_row.insert("__row_id__".to_string(), Value::Integer(*row_id as i64));
                                new_sql_row.insert("__table__".to_string(), Value::Text(table_name.clone()));
                                
                                let old_row = std::mem::take(sql_row);
                                for (col_name, val) in old_row.into_iter() {
                                    let qualified_name = Self::make_qualified_name(prefix, &col_name);
                                    new_sql_row.insert(qualified_name, val);
                                }
                                *sql_row = new_sql_row;
                            }
                            
                            let mut prefixed_schema = (*schema).clone();
                            for col in &mut prefixed_schema.columns {
                                col.name = format!("{}.{}", prefix, col.name);
                            }
                            
                            (filtered_rows, Arc::new(prefixed_schema))
                        }
                        } // row_ids non-empty or pipeline inactive
                    } else {
                        // No index, use table scan
                        self.execute_from_with_limit(stmt.from.as_ref().unwrap(), storage_limit)?
                    }
                } else {
                    self.execute_from_with_limit(stmt.from.as_ref().unwrap(), storage_limit)?
                }
            }
            // Try point query
            else if let Some((col_name, target_value)) = self.try_extract_point_query(where_clause) {
                // Extract table name from FROM clause
                if let TableRef::Table { name: table_name, .. } = stmt.from.as_ref().unwrap() {
                    // Try to use column index
                    let index_name = format!("{}.{}", table_name, col_name);
                    let index_exists = self.db.column_indexes.contains_key(&index_name);
                    
                    if index_exists {
                        // ⚡ Fast path: Use column index (40x faster!)
                        match self.db.query_by_column(table_name, &col_name, &target_value) {
                            Ok(row_ids) if !row_ids.is_empty() || !self.db.is_async_index_pipeline_active() => {
                                // 🚀 Use batch get
                                let schema = self.db.get_table_schema(table_name)?;
                                let batch_rows = self.db.get_table_rows_batch(table_name, &row_ids)?;

                                // 🚀 P1 优化：预分配 row_ids 大小
                                let mut sql_rows = Vec::with_capacity(row_ids.len());
                                for (row_id, row_opt) in batch_rows {
                                    if let Some(row) = row_opt {
                                        let sql_row = row_to_sql_row(&row, &schema)?;
                                        sql_rows.push((row_id, sql_row));
                                    }
                                }

                                // Add table prefix
                                // 🚀 P1 优化：使用 take() 避免克隆所有值
                                let prefix = table_name;
                                for (row_id, sql_row) in &mut sql_rows {
                                    let mut new_sql_row = SqlRow::new();
                                    new_sql_row.insert("__row_id__".to_string(), Value::Integer(*row_id as i64));
                                    new_sql_row.insert("__table__".to_string(), Value::Text(table_name.clone()));

                                    // 使用 drain() 移动值而不是克隆
                                    let old_row = std::mem::take(sql_row);
                                    for (col_name, val) in old_row.into_iter() {
                                        let qualified_name = format!("{}.{}", prefix, col_name);
                                        new_sql_row.insert(qualified_name, val);  // ✅ 移动，不克隆
                                    }
                                    *sql_row = new_sql_row;
                                }

                                let mut prefixed_schema = (*schema).clone();
                                for col in &mut prefixed_schema.columns {
                                    col.name = format!("{}.{}", prefix, col.name);
                                }

                                (sql_rows, Arc::new(prefixed_schema))
                            }
                            Ok(_) | Err(_) => {
                                // Fallback: index empty + pipeline active, or query error
                                self.execute_from(stmt.from.as_ref().unwrap())?
                            }
                        }
                    } else {
                        // No index, use table scan
                        self.execute_from(stmt.from.as_ref().unwrap())?
                    }
                } else {
                    // Not a simple table (e.g., subquery or join)
                    self.execute_from(stmt.from.as_ref().unwrap())?
                }
            }
            // 🚀 Try inequality query (col < value, col > value, etc.)
            else if let Some((col_name, op, value)) = self.try_extract_inequality(where_clause) {
                if let TableRef::Table { name: table_name, .. } = stmt.from.as_ref().unwrap() {
                    let index_name = format!("{}.{}", table_name, col_name);
                    let index_exists = self.db.column_indexes.contains_key(&index_name);
                    
                    if index_exists {
                        // ⚡ Fast path: Use column index inequality scan
                        let row_ids_result = match op {
                            BinaryOperator::Lt => self.db.query_by_column_less_than(table_name, &col_name, &value),
                            BinaryOperator::Le => self.db.query_by_column_less_than_or_equal(table_name, &col_name, &value),
                            BinaryOperator::Gt => self.db.query_by_column_greater_than(table_name, &col_name, &value),
                            BinaryOperator::Ge => self.db.query_by_column_greater_than_or_equal(table_name, &col_name, &value),
                            _ => {
                                // Unsupported operator, fallback to table scan
                                Err(crate::error::MoteDBError::NotImplemented("Unsupported operator".into()))
                            }
                        };
                        
                        match row_ids_result {
                            Ok(row_ids) if !row_ids.is_empty() || !self.db.is_async_index_pipeline_active() => {
                                // 🚀 Use batch get
                                let schema = self.db.get_table_schema(table_name)?;
                                let batch_rows = self.db.get_table_rows_batch(table_name, &row_ids)?;

                                // 🚀 P1 优化：预分配 row_ids 大小
                                let mut sql_rows = Vec::with_capacity(row_ids.len());
                                for (row_id, row_opt) in batch_rows {
                                    if let Some(row) = row_opt {
                                        let sql_row = row_to_sql_row(&row, &schema)?;
                                        sql_rows.push((row_id, sql_row));
                                    }
                                }

                                // Add table prefix
                                // 🚀 P1 优化：使用 take() 避免克隆所有值
                                let prefix = table_name;
                                for (row_id, sql_row) in &mut sql_rows {
                                    let mut new_sql_row = SqlRow::new();
                                    new_sql_row.insert("__row_id__".to_string(), Value::Integer(*row_id as i64));
                                    new_sql_row.insert("__table__".to_string(), Value::Text(table_name.clone()));

                                    // 使用 drain() 移动值而不是克隆
                                    let old_row = std::mem::take(sql_row);
                                    for (col_name, val) in old_row.into_iter() {
                                        let qualified_name = format!("{}.{}", prefix, col_name);
                                        new_sql_row.insert(qualified_name, val);  // ✅ 移动，不克隆
                                    }
                                    *sql_row = new_sql_row;
                                }

                                let mut prefixed_schema = (*schema).clone();
                                for col in &mut prefixed_schema.columns {
                                    col.name = format!("{}.{}", prefix, col.name);
                                }

                                (sql_rows, Arc::new(prefixed_schema))
                            }
                            Ok(_) | Err(_) => {
                                // Fallback: index empty + pipeline active, or query error
                                self.execute_from(stmt.from.as_ref().unwrap())?
                            }
                        }
                    } else {
                        // No index, use table scan
                        self.execute_from(stmt.from.as_ref().unwrap())?
                    }
                } else {
                    // Not a simple table
                    self.execute_from(stmt.from.as_ref().unwrap())?
                }
            } else {
                // Not a simple point/range query
                self.execute_from_with_limit(stmt.from.as_ref().unwrap(), storage_limit)?
            }
        } else {
            // No WHERE clause - use standard scan with limit
            self.execute_from_with_limit(stmt.from.as_ref().unwrap(), storage_limit)?
        };
        
        // 🎯 Filter rows (WHERE clause) - Apply remaining conditions
        let filtered_rows: Vec<(u64, SqlRow)> = if let Some(ref where_clause) = stmt.where_clause {
            // Check if we already used the index (in which case, no need to filter again)
            let used_index = if self.try_extract_range_query(where_clause).is_some() {
                // Range query - check if we used index
                if let TableRef::Table { name: table_name, .. } = stmt.from.as_ref().unwrap() {
                    if let Some((col_name, _, _, _, _)) = self.try_extract_range_query(where_clause) {
                        let index_name = format!("{}.{}", table_name, col_name);
                        self.db.column_indexes.contains_key(&index_name)
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else if let Some((col_name, _)) = self.try_extract_point_query(where_clause) {
                // Point query - check if we used index
                if let TableRef::Table { name: table_name, .. } = stmt.from.as_ref().unwrap() {
                    let index_name = format!("{}.{}", table_name, col_name);
                    self.db.column_indexes.contains_key(&index_name)
                } else {
                    false
                }
            } else {
                false
            };
            
            if used_index {
                // Already filtered by index
                all_sql_rows
            } else {
                // Apply WHERE clause in memory
                if let Some((col_name, target_value)) = self.try_extract_point_query(where_clause) {
                    // Fast path: Only evaluate the point query condition
                    all_sql_rows.into_iter()
                        .filter(|(_, row)| {
                            // 尝试直接匹配
                            if let Some(row_value) = row.get(&col_name) {
                                return row_value == &target_value;
                            }
                            
                            // 尝试匹配带表前缀的列名 (e.g., "users.id")
                            for (key, row_value) in row.iter() {
                                if key.ends_with(&format!(".{}", col_name)) || key == &col_name {
                                    return row_value == &target_value;
                                }
                            }
                            
                            false
                        })
                        .collect()
                } else {
                    // 🚀 OPTIMIZATION: Fast path for simple comparison expressions
                    // Pattern: col > value, col < value, col >= value, col <= value
                    if let Some(fast_filter) = self.compile_simple_comparison(where_clause) {
                        // Use compiled filter (避免重复解释表达式)
                        all_sql_rows.into_iter()
                            .filter(|(_, row)| fast_filter(row))
                            .collect()
                    } else {
                        // Slow path: Full expression evaluation with subquery support
                        let materialized_where = self.materialize_subqueries(where_clause)?;
                        
                        all_sql_rows.into_iter()
                            .filter(|(_, row)| {
                                self.eval_with_materialized(&materialized_where, row)
                                    .and_then(|val| self.to_bool(&val))
                                    .unwrap_or(false)
                            })
                            .collect()
                    }
                }
            }
        } else {
            all_sql_rows
        };
        
        // 🚀 P0 OPTIMIZATION: Apply storage_limit early to reduce memory usage
        // This prevents loading all rows when LIMIT is small and no ORDER BY/GROUP BY/DISTINCT
        // 
        // Safety checks:
        // - ORDER BY: Need all rows to sort first
        // - GROUP BY: Need all rows to group first  
        // - DISTINCT: Need all rows to deduplicate first
        // - Aggregates: Need all rows to compute aggregates
        //
        // If none of above, we can safely truncate early!
        let filtered_rows = if stmt.order_by.is_none() 
            && stmt.group_by.is_none() 
            && !stmt.distinct
            && !self.has_aggregates(&stmt.columns) 
        {
            if let Some(limit) = storage_limit {
                // ✅ Safe to truncate early!
                // This prevents processing millions of rows when LIMIT is small
                filtered_rows.into_iter().take(limit).collect()
            } else {
                filtered_rows
            }
        } else {
            // ❌ Not safe to truncate - need all rows for ORDER BY/GROUP BY/DISTINCT
            filtered_rows
        };
        
        // GROUP BY aggregation (if present) OR implicit aggregation (if columns contain aggregates)
        let (column_names, projected_rows) = if let Some(ref group_by_cols) = stmt.group_by {
            // Explicit GROUP BY
            self.apply_group_by(&stmt.columns, &filtered_rows, group_by_cols, stmt.having.as_ref())?
        } else if self.has_aggregates(&stmt.columns) {
            // Implicit aggregation (e.g., SELECT COUNT(*) FROM table)
            // Treat as GROUP BY with no grouping columns (entire table is one group)
            self.apply_group_by(&stmt.columns, &filtered_rows, &[], None)?
        } else {
            // No aggregation - simple projection
            self.project_columns(&stmt.columns, &filtered_rows, &combined_schema)?
        };
        
        // Order by (with alias resolution)
        let mut sorted_rows = projected_rows;
        if let Some(ref order_by) = stmt.order_by {
            // Build alias map: alias -> projected column index
            let mut alias_map = std::collections::HashMap::new();
            for (idx, col_spec) in stmt.columns.iter().enumerate() {
                let alias = match col_spec {
                    SelectColumn::ColumnWithAlias(_, alias) => Some(alias.clone()),
                    SelectColumn::Expr(_, Some(alias)) => Some(alias.clone()),
                    _ => None,
                };
                if let Some(alias) = alias {
                    alias_map.insert(alias, idx);
                }
            }
            
            // Create temporary rows with full data for sorting
            let mut rows_with_keys: Vec<(Vec<Value>, Vec<Value>)> = sorted_rows.into_iter()
                .zip(filtered_rows.iter())
                .map(|(proj_row, (_, full_row))| {
                    // Compute sort keys
                    let sort_keys: Result<Vec<Value>> = order_by.iter()
                        .map(|order| {
                            // Try to resolve alias first
                            if let Expr::Column(col_name) = &order.expr {
                                if let Some(&idx) = alias_map.get(col_name) {
                                    // Use projected column value
                                    return Ok(proj_row[idx].clone());
                                }
                            }
                            // Otherwise, evaluate expression against original row
                            self.evaluator.eval(&order.expr, full_row)
                        })
                        .collect();
                    
                    sort_keys.map(|keys| (keys, proj_row))
                })
                .collect::<Result<Vec<_>>>()?;
            
            // Sort
            rows_with_keys.sort_by(|a, b| {
                for (i, order) in order_by.iter().enumerate() {
                    let cmp = a.0[i].partial_cmp(&b.0[i]).unwrap_or(std::cmp::Ordering::Equal);
                    if cmp != std::cmp::Ordering::Equal {
                        return if order.asc { cmp } else { cmp.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
            
            sorted_rows = rows_with_keys.into_iter().map(|(_, row)| row).collect();
        }
        
        // Apply LATEST BY (time-series deduplication)
        let final_sorted_rows = if let Some(ref latest_by_cols) = stmt.latest_by {
            self.apply_latest_by(sorted_rows, &filtered_rows, latest_by_cols, &combined_schema)?
        } else {
            sorted_rows
        };
        
        // Apply DISTINCT (deduplication)
        let deduplicated_rows = if stmt.distinct {
            self.apply_distinct(final_sorted_rows)
        } else {
            final_sorted_rows
        };
        
        // Apply LIMIT and OFFSET
        let offset = stmt.offset.unwrap_or(0);
        let limit = stmt.limit;
        
        let final_rows: Vec<Vec<Value>> = deduplicated_rows.into_iter()
            .skip(offset)
            .take(limit.unwrap_or(usize::MAX))
            .collect();
        
        Ok(QueryResult::Select {
            columns: column_names,
            rows: final_rows,
        })
    }
    
    /// 🚀 P0 OPTIMIZATION: Calculate the limit to pass to storage layer
    /// 
    /// This prevents loading all rows when LIMIT is specified:
    /// - `SELECT * FROM users LIMIT 10` → only load 10 rows from storage
    /// - `SELECT * FROM users WHERE ... LIMIT 10` → load more (WHERE filtering)
    /// - `SELECT * FROM users ORDER BY ... LIMIT 10` → load all (need to sort first)
    fn calculate_storage_limit(&self, stmt: &SelectStmt) -> Option<usize> {
        // If there's ORDER BY, we need all rows to sort first
        if stmt.order_by.is_some() {
            return None;
        }
        
        // If there's GROUP BY, we need all rows
        if stmt.group_by.is_some() {
            return None;
        }
        
        // Check if SELECT columns contain aggregates
        for col in &stmt.columns {
            if let SelectColumn::Expr(expr, _) = col {
                if self.expr_has_aggregates(expr) {
                    return None; // Aggregates need all rows
                }
            }
        }
        
        // If there's WHERE clause that hasn't been resolved by an index,
        // we must scan all rows — the selectivity is unknown and any
        // pre-truncation risks returning wrong (empty) results.
        if stmt.where_clause.is_some() {
            return None;
        }

        // No WHERE: safe to use exact limit at storage level
        let limit = stmt.limit?;
        let offset = stmt.offset.unwrap_or(0);
        Some(limit + offset)
    }
    
    /// Check if expression contains aggregates (recursive)
    #[allow(clippy::only_used_in_recursion)]
    fn expr_has_aggregates(&self, expr: &Expr) -> bool {
        match expr {
            Expr::FunctionCall { name, .. } => {
                matches!(name.to_uppercase().as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
            }
            Expr::BinaryOp { left, right, .. } => {
                self.expr_has_aggregates(left) || self.expr_has_aggregates(right)
            }
            _ => false,
        }
    }
    
    /// Execute FROM clause - handles single table or JOINs
    /// Returns all rows with combined schema
    fn execute_from(&self, table_ref: &TableRef) -> FromScanResult {
        self.execute_from_with_limit(table_ref, None)
    }

    /// 🚀 P0 OPTIMIZATION: Execute FROM clause with limit passed to storage layer
    fn execute_from_with_limit(&self, table_ref: &TableRef, limit: Option<usize>) -> FromScanResult {
        match table_ref {
            TableRef::Table { name, alias } => {
                // Single table - use table-specific scan with limit
                let schema = self.db.get_table_schema(name)?;
                
                // 🚀 P0: Scan table with streaming to reduce memory (with optional limit)
                let all_rows: Result<Vec<_>> = if let Some(limit_val) = limit {
                    // With limit: collect only up to limit rows
                    self.db.scan_table_rows_streaming(name)?
                        .take(limit_val)
                        .collect()
                } else {
                    // No limit: collect all (unavoidable for full table scan)
                    self.db.scan_table_rows_streaming(name)?
                        .collect()
                };
                let all_rows = all_rows?;

                let mut sql_rows = rows_to_sql_rows(all_rows, &schema)?;
                
                // Always prefix column names with table or alias for JOIN compatibility
                let prefix = alias.as_ref().unwrap_or(name);
                
                // Update SqlRow keys to include table prefix + add metadata
                for (row_id, sql_row) in &mut sql_rows {
                    let mut new_sql_row = SqlRow::new();
                    new_sql_row.insert("__row_id__".to_string(), Value::Integer(*row_id as i64));
                    new_sql_row.insert("__table__".to_string(), Value::Text(name.clone()));
                    
                    let old_row = std::mem::take(sql_row);
                    for (col_name, val) in old_row.into_iter() {
                        let qualified_name = format!("{}.{}", prefix, col_name);
                        new_sql_row.insert(qualified_name, val);
                    }
                    *sql_row = new_sql_row;
                }
                
                // Update schema column names
                let mut prefixed_schema = (*schema).clone();
                for col in &mut prefixed_schema.columns {
                    col.name = format!("{}.{}", prefix, col.name);
                }

                Ok((sql_rows, Arc::new(prefixed_schema)))
            }
            TableRef::Subquery { query, alias } => {
                // Execute subquery
                let subquery_result = self.execute_select_internal(query)?;
                
                // Convert QueryResult to (Vec<(u64, SqlRow)>, TableSchema)
                match subquery_result {
                    QueryResult::Select { columns, rows } => {
                        // Build schema from subquery columns - infer types from first row
                        let mut schema_cols = Vec::new();
                        for (idx, col_name) in columns.iter().enumerate() {
                            // Infer type from first row value
                            let col_type = if let Some(first_row) = rows.first() {
                                if let Some(value) = first_row.get(idx) {
                                    match value {
                                        Value::Integer(_) => ColumnType::Integer,
                                        Value::Float(_) => ColumnType::Float,
                                        Value::Text(_) | Value::TextDoc(_) => ColumnType::Text,
                                        Value::Bool(_) => ColumnType::Boolean,
                                        Value::Timestamp(_) => ColumnType::Timestamp,
                                        Value::Tensor(t) => ColumnType::Tensor(t.dimension()),
                                        Value::Spatial(_) => ColumnType::Spatial,
                                        Value::Vector(v) => ColumnType::Tensor(v.len()),
                                        Value::Null => ColumnType::Text, // Default for NULL
                                    }
                                } else {
                                    ColumnType::Text
                                }
                            } else {
                                ColumnType::Text
                            };
                            
                            schema_cols.push(crate::types::ColumnDef::new(
                                col_name.clone(),
                                col_type,
                                idx,
                            ));
                        }
                        let mut schema = TableSchema::new(alias.clone(), schema_cols);
                        
                        // Convert rows to SqlRow format with alias prefix
                        // 🚀 P1 优化：预分配 rows 大小
                        let mut sql_rows = Vec::with_capacity(rows.len());
                        for (row_id, row_values) in rows.iter().enumerate() {
                            let mut sql_row = SqlRow::new();
                            for (col_name, value) in columns.iter().zip(row_values.iter()) {
                                // Strip table prefix from column name (e.g., "users.age" -> "age")
                                let base_col_name = if let Some(dot_pos) = col_name.rfind('.') {
                                    &col_name[dot_pos + 1..]
                                } else {
                                    col_name.as_str()
                                };
                                let qualified_name = format!("{}.{}", alias, base_col_name);
                                sql_row.insert(qualified_name, value.clone());
                            }
                            sql_rows.push((row_id as u64, sql_row));
                        }
                        
                        // Update schema column names with alias prefix (strip original prefix)
                        for col in &mut schema.columns {
                            let base_name = if let Some(dot_pos) = col.name.rfind('.') {
                                &col.name[dot_pos + 1..]
                            } else {
                                &col.name
                            };
                            col.name = format!("{}.{}", alias, base_name);
                        }
                        
                        Ok((sql_rows, Arc::new(schema)))
                    }
                    _ => Err(MoteDBError::Query("Subquery must be a SELECT".into())),
                }
            }
            TableRef::Join { left, right, join_type, on_condition } => {
                // Recursive: evaluate left and right
                let (left_rows, left_schema) = self.execute_from(left)?;
                let (right_rows, right_schema) = self.execute_from(right)?;
                
                // Combine schemas
                let mut combined_schema = (*left_schema).clone();
                combined_schema.columns.extend(right_schema.columns.clone());
                
                // Perform JOIN based on type
                let joined_rows = match join_type {
                    JoinType::Inner => self.inner_join(&left_rows, &right_rows, on_condition)?,
                    JoinType::Left => self.left_join(&left_rows, &right_rows, on_condition)?,
                    JoinType::Right => self.right_join(&left_rows, &right_rows, on_condition)?,
                    JoinType::Full => self.full_join(&left_rows, &right_rows, on_condition)?,
                };
                
                Ok((joined_rows, Arc::new(combined_schema)))
            }
        }
    }
    
    /// INNER JOIN: only rows that match condition in both tables
    /// 
    /// 🚀 Optimized with Hash Join for equi-joins
    fn inner_join(
        &self,
        left_rows: &[(u64, SqlRow)],
        right_rows: &[(u64, SqlRow)],
        on_condition: &Expr,
    ) -> Result<Vec<(u64, SqlRow)>> {
        // Try to detect equi-join (col1 = col2) for Hash Join optimization
        if let Some((left_col, right_col)) = self.extract_equi_join_columns(on_condition) {
            // 🚀 Use Hash Join (O(N + M))
            return self.hash_join_inner(left_rows, right_rows, &left_col, &right_col);
        }
        
        // Fallback: Nested Loop Join (O(N × M))
        let mut result = Vec::new();
        let mut next_id = 1u64;
        
        for (_, left_row) in left_rows {
            for (_, right_row) in right_rows {
                // Combine rows
                let combined_row = self.combine_rows(left_row, right_row);
                
                // Evaluate JOIN condition
                if self.evaluator.eval(on_condition, &combined_row)
                    .and_then(|val| self.to_bool(&val))
                    .unwrap_or(false)
                {
                    result.push((next_id, combined_row));
                    next_id += 1;
                }
            }
        }
        
        Ok(result)
    }
    
    /// 🚀 Hash Join for equi-join (col1 = col2)
    /// Time complexity: O(N + M) instead of O(N × M)
    /// ⚡ P0 Optimization: Use typed HashKey instead of format!("{:?}")
    fn hash_join_inner(
        &self,
        left_rows: &[(u64, SqlRow)],
        right_rows: &[(u64, SqlRow)],
        left_col: &str,
        right_col: &str,
    ) -> Result<Vec<(u64, SqlRow)>> {
        use std::collections::HashMap;
        
        // Hash key type (supports Eq + Hash, no string allocation)
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        enum HashKey {
            Integer(i64),
            Text(String),
            Bool(bool),
            Float(u64), // Use bits representation for float
            Null,
        }
        
        // Fast conversion from Value to HashKey
        #[inline]
        fn to_hash_key(value: &Value) -> Option<HashKey> {
            match value {
                Value::Integer(i) => Some(HashKey::Integer(*i)),
                Value::Text(s) => Some(HashKey::Text(s.clone())),
                Value::Bool(b) => Some(HashKey::Bool(*b)),
                Value::Float(f) => Some(HashKey::Float(f.to_bits())),
                Value::Null => Some(HashKey::Null),
                _ => None, // Vector/Tensor cannot hash directly
            }
        }
        
        // Step 1: Build hash table on smaller table (right)
        // 🚀 预分配：假设负载因子 0.75
        let mut hash_table: HashMap<HashKey, Vec<&SqlRow>> = HashMap::with_capacity(
            (right_rows.len() as f64 / 0.75) as usize
        );
        
        for (_, right_row) in right_rows {
            if let Some(key_val) = right_row.get(right_col) {
                // ⚡ Zero-allocation hash key (no format!)
                if let Some(key) = to_hash_key(key_val) {
                    hash_table.entry(key).or_default().push(right_row);
                }
            }
        }
        
        // Step 2: Probe with left table
        // 🚀 预分配：预估每行匹配 1 个
        let mut result = Vec::with_capacity(left_rows.len());
        let mut next_id = 1u64;
        
        for (_, left_row) in left_rows {
            if let Some(key_val) = left_row.get(left_col) {
                // ⚡ Zero-allocation hash key
                if let Some(key) = to_hash_key(key_val) {
                    // O(1) lookup in hash table
                    if let Some(matching_right_rows) = hash_table.get(&key) {
                        for right_row in matching_right_rows {
                            let combined_row = self.combine_rows(left_row, right_row);
                            result.push((next_id, combined_row));
                            next_id += 1;
                        }
                    }
                }
            }
        }
        
        Ok(result)
    }
    
    /// Extract equi-join columns from ON condition
    /// Returns Some((left_col, right_col)) if condition is "col1 = col2", otherwise None
    fn extract_equi_join_columns(&self, expr: &Expr) -> Option<(String, String)> {
        match expr {
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Eq => {
                // Check if both sides are column references
                if let (Expr::Column(left_col), Expr::Column(right_col)) = (left.as_ref(), right.as_ref()) {
                    return Some((left_col.clone(), right_col.clone()));
                }
            }
            _ => {}
        }
        None
    }
    
    /// LEFT JOIN: all rows from left, matched rows from right (NULL if no match)
    fn left_join(
        &self,
        left_rows: &[(u64, SqlRow)],
        right_rows: &[(u64, SqlRow)],
        on_condition: &Expr,
    ) -> Result<Vec<(u64, SqlRow)>> {
        let mut result = Vec::new();
        let mut next_id = 1u64;
        
        for (_, left_row) in left_rows {
            let mut matched = false;
            
            for (_, right_row) in right_rows {
                let combined_row = self.combine_rows(left_row, right_row);
                
                if self.evaluator.eval(on_condition, &combined_row)
                    .and_then(|val| self.to_bool(&val))
                    .unwrap_or(false)
                {
                    result.push((next_id, combined_row));
                    next_id += 1;
                    matched = true;
                }
            }
            
            // If no match, add left row with NULL values for right columns
            if !matched {
                let null_right_row: SqlRow = right_rows.first()
                    .map(|(_, row)| row.keys().map(|k| (k.clone(), Value::Null)).collect())
                    .unwrap_or_default();
                let combined_row = self.combine_rows(left_row, &null_right_row);
                result.push((next_id, combined_row));
                next_id += 1;
            }
        }
        
        Ok(result)
    }
    
    /// RIGHT JOIN: all rows from right, matched rows from left (NULL if no match)
    fn right_join(
        &self,
        left_rows: &[(u64, SqlRow)],
        right_rows: &[(u64, SqlRow)],
        on_condition: &Expr,
    ) -> Result<Vec<(u64, SqlRow)>> {
        // RIGHT JOIN = LEFT JOIN with tables swapped, but condition order matters
        // We swap left and right, then swap back in the combined row
        let mut result = Vec::new();
        let mut next_id = 1u64;
        
        for (_, right_row) in right_rows {
            let mut matched = false;
            
            for (_, left_row) in left_rows {
                let combined_row = self.combine_rows(left_row, right_row);
                
                if self.evaluator.eval(on_condition, &combined_row)
                    .and_then(|val| self.to_bool(&val))
                    .unwrap_or(false)
                {
                    result.push((next_id, combined_row));
                    next_id += 1;
                    matched = true;
                }
            }
            
            // If no match, add right row with NULL values for left columns
            if !matched {
                let null_left_row: SqlRow = left_rows.first()
                    .map(|(_, row)| row.keys().map(|k| (k.clone(), Value::Null)).collect())
                    .unwrap_or_default();
                let combined_row = self.combine_rows(&null_left_row, right_row);
                result.push((next_id, combined_row));
                next_id += 1;
            }
        }
        
        Ok(result)
    }
    
    /// FULL OUTER JOIN: all rows from both tables (NULL where no match)
    fn full_join(
        &self,
        left_rows: &[(u64, SqlRow)],
        right_rows: &[(u64, SqlRow)],
        on_condition: &Expr,
    ) -> Result<Vec<(u64, SqlRow)>> {
        let mut result = Vec::new();
        let mut next_id = 1u64;
        let mut right_matched = vec![false; right_rows.len()];
        
        // First pass: process all left rows
        for (_, left_row) in left_rows {
            let mut left_matched = false;
            
            for (right_idx, (_, right_row)) in right_rows.iter().enumerate() {
                let combined_row = self.combine_rows(left_row, right_row);
                
                if self.evaluator.eval(on_condition, &combined_row)
                    .and_then(|val| self.to_bool(&val))
                    .unwrap_or(false)
                {
                    result.push((next_id, combined_row));
                    next_id += 1;
                    left_matched = true;
                    right_matched[right_idx] = true;
                }
            }
            
            // If left row didn't match, add with NULL right values
            if !left_matched {
                let null_right_row: SqlRow = right_rows.first()
                    .map(|(_, row)| row.keys().map(|k| (k.clone(), Value::Null)).collect())
                    .unwrap_or_default();
                let combined_row = self.combine_rows(left_row, &null_right_row);
                result.push((next_id, combined_row));
                next_id += 1;
            }
        }
        
        // Second pass: add unmatched right rows
        for (right_idx, (_, right_row)) in right_rows.iter().enumerate() {
            if !right_matched[right_idx] {
                let null_left_row: SqlRow = left_rows.first()
                    .map(|(_, row)| row.keys().map(|k| (k.clone(), Value::Null)).collect())
                    .unwrap_or_default();
                let combined_row = self.combine_rows(&null_left_row, right_row);
                result.push((next_id, combined_row));
                next_id += 1;
            }
        }
        
        Ok(result)
    }
    
    /// Combine two SqlRows (for JOIN operations)
    /// ✅ 优化：使用 with_capacity 预分配，减少 reallocation
    fn combine_rows(&self, left: &SqlRow, right: &SqlRow) -> SqlRow {
        let mut combined = SqlRow::with_capacity(left.len() + right.len());
        // 直接 extend，HashMap 的 clone 仍然必要（因为我们需要保留原始行）
        combined.extend(left.iter().map(|(k, v)| (k.clone(), v.clone())));
        combined.extend(right.iter().map(|(k, v)| (k.clone(), v.clone())));
        combined
    }
    
    /// Materialize subqueries in an expression (convert to literal value lists)
    /// 
    /// Example: WHERE id IN (SELECT user_id FROM orders)
    /// Becomes: WHERE id IN (1, 2, 3) [after executing subquery]
    fn materialize_subqueries(&self, expr: &Expr) -> Result<Expr> {
        match expr {
            Expr::Subquery(subquery) => {
                // Execute subquery
                let result = self.execute_select_internal(subquery)?;
                
                match result {
                    QueryResult::Select { rows, .. } => {
                        // Scalar subquery: return single value
                        if rows.len() == 1 && rows[0].len() == 1 {
                            Ok(Expr::Literal(rows[0][0].clone()))
                        } else if rows.is_empty() {
                            Ok(Expr::Literal(Value::Null))
                        } else {
                            // Non-scalar subquery error (should be used with IN)
                            Err(MoteDBError::Query(
                                "Subquery returns more than one row/column (use IN instead of =)".into()
                            ))
                        }
                    }
                    _ => Err(MoteDBError::Query("Subquery must return SELECT result".into())),
                }
            }
            
            Expr::In { expr, list, negated } => {
                // Check if list contains a subquery
                let materialized_list: Result<Vec<Expr>> = if list.len() == 1 {
                    if let Expr::Subquery(subquery) = &list[0] {
                        // Execute subquery and convert to literal list
                        let result = self.execute_select_internal(subquery)?;
                        
                        match result {
                            QueryResult::Select { rows, .. } => {
                                // Extract first column values
                                let literals: Vec<Expr> = rows.iter()
                                    .filter_map(|row| row.first().cloned())
                                    .map(Expr::Literal)
                                    .collect();
                                Ok(literals)
                            }
                            _ => Err(MoteDBError::Query("Subquery must return SELECT result".into())),
                        }
                    } else {
                        Ok(list.clone())
                    }
                } else {
                    Ok(list.clone())
                };
                
                Ok(Expr::In {
                    expr: Box::new(self.materialize_subqueries(expr)?),
                    list: materialized_list?,
                    negated: *negated,
                })
            }
            
            Expr::BinaryOp { left, op, right } => {
                Ok(Expr::BinaryOp {
                    left: Box::new(self.materialize_subqueries(left)?),
                    op: op.clone(),
                    right: Box::new(self.materialize_subqueries(right)?),
                })
            }
            
            Expr::UnaryOp { op, expr } => {
                Ok(Expr::UnaryOp {
                    op: op.clone(),
                    expr: Box::new(self.materialize_subqueries(expr)?),
                })
            }
            
            Expr::Between { expr, low, high, negated } => {
                Ok(Expr::Between {
                    expr: Box::new(self.materialize_subqueries(expr)?),
                    low: Box::new(self.materialize_subqueries(low)?),
                    high: Box::new(self.materialize_subqueries(high)?),
                    negated: *negated,
                })
            }
            
            Expr::Like { expr, pattern, negated } => {
                Ok(Expr::Like {
                    expr: Box::new(self.materialize_subqueries(expr)?),
                    pattern: Box::new(self.materialize_subqueries(pattern)?),
                    negated: *negated,
                })
            }
            
            Expr::IsNull { expr, negated } => {
                Ok(Expr::IsNull {
                    expr: Box::new(self.materialize_subqueries(expr)?),
                    negated: *negated,
                })
            }
            
            Expr::FunctionCall { name, args, distinct } => {
                let materialized_args: Result<Vec<Expr>> = args.iter()
                    .map(|arg| self.materialize_subqueries(arg))
                    .collect();
                
                Ok(Expr::FunctionCall {
                    name: name.clone(),
                    args: materialized_args?,
                    distinct: *distinct,
                })
            }
            
            // Leaf nodes - no subqueries to materialize
            Expr::Column(_) | Expr::Literal(_) | Expr::Parameter(_) | Expr::Match { .. } |
            Expr::KnnSearch { .. } | Expr::KnnDistance { .. } |
            Expr::StWithin3D { .. } | Expr::StDistance3D { .. } | Expr::StKnn3D { .. } | Expr::StRadius3D { .. } |
            Expr::WindowFunction { .. } => Ok(expr.clone()),
        }
    }
    
    /// Helper: Get column value from row, trying both exact match and table-prefixed match
    fn get_column_value(&self, row: &SqlRow, column: &str) -> Option<Value> {
        row.get(column).cloned().or_else(|| {
            // If column name doesn't contain '.', try prefixed versions
            if !column.contains('.') {
                row.iter()
                    .find(|(k, _)| k.ends_with(&format!(".{}", column)))
                    .map(|(_, v)| v.clone())
            } else {
                None
            }
        })
    }
    
    /// Evaluate expression with materialized subqueries
    fn eval_with_materialized(&self, expr: &Expr, row: &SqlRow) -> Result<Value> {
        // Special handling for MATCH and KNN expressions
        match expr {
            // 🔧 Recursively handle Binary Operations (e.g., ST_DISTANCE(...) < 10)
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.eval_with_materialized(left, row)?;
                let right_val = self.eval_with_materialized(right, row)?;
                // Use simple comparison logic
                match op {
                    BinaryOperator::Lt => Ok(Value::Bool(left_val < right_val)),
                    BinaryOperator::Le => Ok(Value::Bool(left_val <= right_val)),
                    BinaryOperator::Gt => Ok(Value::Bool(left_val > right_val)),
                    BinaryOperator::Ge => Ok(Value::Bool(left_val >= right_val)),
                    BinaryOperator::Eq => Ok(Value::Bool(left_val == right_val)),
                    BinaryOperator::Ne => Ok(Value::Bool(left_val != right_val)),
                    BinaryOperator::And => {
                        let left_bool = match left_val {
                            Value::Bool(b) => b,
                            Value::Integer(i) => i != 0,
                            Value::Float(f) => f != 0.0 && !f.is_nan(),
                            _ => false,
                        };
                        let right_bool = match right_val {
                            Value::Bool(b) => b,
                            Value::Integer(i) => i != 0,
                            Value::Float(f) => f != 0.0 && !f.is_nan(),
                            _ => false,
                        };
                        Ok(Value::Bool(left_bool && right_bool))
                    }
                    BinaryOperator::Or => {
                        let left_bool = match left_val {
                            Value::Bool(b) => b,
                            Value::Integer(i) => i != 0,
                            Value::Float(f) => f != 0.0 && !f.is_nan(),
                            _ => false,
                        };
                        let right_bool = match right_val {
                            Value::Bool(b) => b,
                            Value::Integer(i) => i != 0,
                            Value::Float(f) => f != 0.0 && !f.is_nan(),
                            _ => false,
                        };
                        Ok(Value::Bool(left_bool || right_bool))
                    }
                    _ => self.evaluator.eval(expr, row),  // Fall back to evaluator for complex ops
                }
            }
            
            Expr::Match { column, query, .. } => {
                // 🚀 Fast path: use pre-computed score if available (from text search fast path)
                let score_key = format!("__text_score_{}__", column);
                if let Some(Value::Float(score)) = row.get(&score_key) {
                    return Ok(Value::Float(*score));
                }

                // Get row_id from the row
                let row_id_opt = row.get("__row_id__")
                    .and_then(|v| match v {
                        Value::Integer(i) => Some(*i as u64),
                    _ => None,
                });

            // 🔧 Get table name from row
            let table_name_opt = row.get("__table__")
                .and_then(|v| match v {
                    Value::Text(s) => Some(s.as_str()),
                    _ => None,
                });

            // Try index-based match if metadata is available
            if let (Some(row_id), Some(table_name)) = (row_id_opt, table_name_opt) {
                let index_name = self.db.index_registry.find_by_column(
                    table_name,
                    column,
                    crate::database::index_metadata::IndexType::Text
                );
                if let Some(index_name) = index_name {
                    if let Some(index_ref) = self.db.text_indexes.get(&index_name) {
                        let results = index_ref.value().read().search_ranked(query, 1000)?;
                        let score = results.iter()
                            .find(|(doc_id, _)| *doc_id == row_id)
                            .map(|(_, score)| *score)
                            .unwrap_or(0.0);
                        return Ok(Value::Float(score as f64));
                    }
                }
            }

            // Fallback: naive text scan when no FTS index
            let text_val = row.get(column)
                .or_else(|| table_name_opt.and_then(|t| row.get(&format!("{}.{}", t, column))));
            match text_val {
                Some(Value::Text(text)) => {
                    let text_lower = text.to_lowercase();
                    let query_lower = query.to_lowercase();
                    let terms: Vec<&str> = query_lower.split_whitespace().collect();
                    let matched = terms.iter().all(|t| text_lower.contains(t));
                    Ok(Value::Bool(matched))
                }
                _ => Ok(Value::Bool(false)),
            }
            }
            
            Expr::KnnSearch { column, query_vector, k } => {
                // KNN_SEARCH returns Bool - true if this row is in top-k results
                let row_id = row.get("__row_id__")
                    .and_then(|v| match v {
                        Value::Integer(i) => Some(*i as u64),
                        _ => None,
                    })
                    .ok_or_else(|| MoteDBError::Query("KNN_SEARCH requires __row_id__ in row".into()))?;
                
                // 🔧 Get table name
                let table_name = row.get("__table__")
                    .and_then(|v| match v {
                        Value::Text(s) => Some(s.as_str()),
                        _ => None,
                    })
                    .ok_or_else(|| MoteDBError::Query("KNN_SEARCH requires __table__ in row".into()))?;
                
                // 🔧 Use index_registry to find the correct user-specified index name
                let index_name = self.db.index_registry.find_by_column(
                    table_name,
                    column,
                    crate::database::index_metadata::IndexType::Vector
                ).ok_or_else(|| MoteDBError::Query(format!("No vector index found for column '{}.{}'", table_name, column)))?;
                
                // Perform KNN search using public API
                let results = self.db.vector_search(&index_name, query_vector.as_slice(), *k)?;
                
                // Check if row_id is in results
                let in_results = results.iter().any(|(id, _)| *id == row_id);
                Ok(Value::Bool(in_results))
            }
            
            Expr::KnnDistance { column, query_vector } => {
                // KNN_DISTANCE returns Float - distance/similarity score
                // Get vector value from row
                let vector = self.get_column_value(row, column)
                    .ok_or_else(|| MoteDBError::ColumnNotFound(column.clone()))?;
                
                let vec_data = match vector {
                    Value::Vector(v) => v,
                    _ => return Err(MoteDBError::TypeError(format!("Column '{}' is not a vector", column))),
                };
                
                // Compute distance (using L2 distance)
                if vec_data.len() != query_vector.len() {
                    return Err(MoteDBError::InvalidArgument(
                        format!("Vector dimension mismatch: {} vs {}", vec_data.len(), query_vector.len())
                    ));
                }
                
                let distance: f32 = vec_data.iter()
                    .zip(query_vector.iter())
                    .map(|(a, b)| (a - b).powi(2))
                    .sum::<f32>()
                    .sqrt();

                Ok(Value::Float(distance as f64))
            }

            // ==================== 3D Spatial Expressions (i-Octree) ====================

            Expr::StDistance3D { column, x, y, z } => {
                // Fast path: use pre-computed distance if available
                if let Some(Value::Float(dist)) = row.get("__spatial_distance__") {
                    return Ok(Value::Float(*dist));
                }
                let point_value = self.get_column_value(row, column)
                    .ok_or_else(|| MoteDBError::ColumnNotFound(column.clone()))?;

                use crate::types::Geometry;
                let geom = match point_value {
                    Value::Spatial(Geometry::Point3D(p)) => p,
                    Value::Spatial(Geometry::Point(p)) => {
                        // 2D point treated as z=0
                        crate::types::Point3D::new(p.x, p.y, 0.0)
                    }
                    _ => return Err(MoteDBError::TypeError(format!("Column '{}' is not a 3D Point", column))),
                };

                let dx = geom.x - x;
                let dy = geom.y - y;
                let dz = geom.z - z;
                Ok(Value::Float((dx * dx + dy * dy + dz * dz).sqrt()))
            }

            Expr::StWithin3D { column, min_x, min_y, min_z, max_x, max_y, max_z } => {
                if row.get("__spatial_within__").is_some() {
                    return Ok(Value::Bool(true));
                }
                let point_value = self.get_column_value(row, column)
                    .ok_or_else(|| MoteDBError::ColumnNotFound(column.clone()))?;

                use crate::types::Geometry;
                let geom = match point_value {
                    Value::Spatial(Geometry::Point3D(p)) => p,
                    Value::Spatial(Geometry::Point(p)) => {
                        crate::types::Point3D::new(p.x, p.y, 0.0)
                    }
                    _ => return Ok(Value::Bool(false)),
                };

                Ok(Value::Bool(
                    geom.x >= *min_x && geom.x <= *max_x &&
                    geom.y >= *min_y && geom.y <= *max_y &&
                    geom.z >= *min_z && geom.z <= *max_z
                ))
            }

            Expr::StKnn3D { column, x, y, z, k } => {
                // Fast path: already filtered by i-Octree KNN
                if row.get("__spatial_knn__").is_some() {
                    return Ok(Value::Bool(true));
                }
                let row_id = row.get("__row_id__")
                    .and_then(|v| match v { Value::Integer(i) => Some(*i as u64), _ => None })
                    .ok_or_else(|| MoteDBError::Query("ST_KNN_3D requires __row_id__ in row".into()))?;
                let table_name = row.get("__table__")
                    .and_then(|v| match v { Value::Text(s) => Some(s.as_str()), _ => None })
                    .ok_or_else(|| MoteDBError::Query("ST_KNN_3D requires __table__ in row".into()))?;

                let index_name = self.db.index_registry.find_by_column(
                    table_name, column, crate::database::index_metadata::IndexType::Octree
                ).ok_or_else(|| MoteDBError::Query(format!("No ioctree index for '{}.{}'", table_name, column)))?;

                let query_point = crate::types::Point3D::new(*x, *y, *z);
                let results = self.db.ioctree_knn_query(&index_name, &query_point, *k)?;
                Ok(Value::Bool(results.iter().any(|(id, _)| *id == row_id)))
            }

            Expr::StRadius3D { column, x, y, z, radius } => {
                if row.get("__spatial_knn__").is_some() {
                    return Ok(Value::Bool(true));
                }
                let point_value = self.get_column_value(row, column)
                    .ok_or_else(|| MoteDBError::ColumnNotFound(column.clone()))?;

                use crate::types::Geometry;
                let geom = match point_value {
                    Value::Spatial(Geometry::Point3D(p)) => p,
                    Value::Spatial(Geometry::Point(p)) => {
                        crate::types::Point3D::new(p.x, p.y, 0.0)
                    }
                    _ => return Ok(Value::Bool(false)),
                };

                let dx = geom.x - x;
                let dy = geom.y - y;
                let dz = geom.z - z;
                let dist = (dx * dx + dy * dy + dz * dz).sqrt();
                Ok(Value::Bool(dist <= *radius))
            }

            _ => self.evaluator.eval(expr, row)
        }
    }
    
    /// Apply DISTINCT clause - remove duplicate rows
    fn apply_distinct(&self, rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
        use std::collections::HashSet;
        
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        
        for row in rows {
            // Create a hashable key from the row
            let key: Vec<String> = row.iter().map(|v| format!("{:?}", v)).collect();
            
            if seen.insert(key) {
                result.push(row);
            }
        }
        
        result
    }
    
    /// Apply LATEST BY clause - keep only the latest record per group
    fn apply_latest_by(
        &self,
        projected_rows: Vec<Vec<Value>>,
        filtered_rows: &[(u64, SqlRow)],
        latest_by_cols: &[String],
        schema: &TableSchema,
    ) -> Result<Vec<Vec<Value>>> {
        use std::collections::HashMap;
        
        // Find timestamp column (must exist in schema)
        let timestamp_col = schema.columns.iter()
            .find(|c| c.col_type == ColumnType::Timestamp)
            .ok_or_else(|| MoteDBError::Query(
                "LATEST BY requires a TIMESTAMP column in the table".to_string()
            ))?;
        
        let timestamp_col_name = &timestamp_col.name;
        
        // Build grouping key -> (max_timestamp, projected_row) map
        let mut groups: HashMap<Vec<String>, (i64, Vec<Value>)> = HashMap::new();
        
        for (i, (_, full_row)) in filtered_rows.iter().enumerate() {
            // Extract grouping key
            let group_key: Result<Vec<String>> = latest_by_cols.iter()
                .map(|col_name| {
                    full_row.get(col_name)
                        .ok_or_else(|| MoteDBError::ColumnNotFound(col_name.clone()))
                        .map(|val| match val {
                            Value::Text(s) => s.clone(),
                            Value::Integer(i) => i.to_string(),
                            Value::Float(f) => f.to_string(),
                            _ => format!("{:?}", val),
                        })
                })
                .collect();
            let group_key = group_key?;
            
            // Extract timestamp
            let timestamp = full_row.get(timestamp_col_name)
                .ok_or_else(|| MoteDBError::ColumnNotFound(timestamp_col_name.clone()))?;
            
            let ts_value = match timestamp {
                Value::Timestamp(ts) => ts.as_micros(),
                Value::Integer(i) => *i,
                _ => return Err(MoteDBError::Query(
                    format!("Timestamp column '{}' must be TIMESTAMP or INTEGER type", timestamp_col_name)
                )),
            };
            
            // Update group if this is a newer record
            let projected_row = projected_rows[i].clone();
            groups.entry(group_key)
                .and_modify(|(max_ts, row)| {
                    if ts_value > *max_ts {
                        *max_ts = ts_value;
                        *row = projected_row.clone();
                    }
                })
                .or_insert((ts_value, projected_row));
        }
        
        // Extract all latest records
        Ok(groups.into_values().map(|(_, row)| row).collect())
    }
    
    /// Apply GROUP BY aggregation
    fn apply_group_by(
        &self,
        columns: &[SelectColumn],
        rows: &[(u64, SqlRow)],
        group_by_cols: &[String],
        having: Option<&Expr>,
    ) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
        use std::collections::HashMap;
        
        // Build groups: group_key -> list of rows
        let mut groups: HashMap<Vec<String>, Vec<&SqlRow>> = HashMap::new();
        
        for (_, row) in rows {
            // Extract grouping key
            let group_key: Result<Vec<String>> = group_by_cols.iter()
                .map(|col_name| {
                    // ⭐ 支持自动解析列名：先尝试原名，再尝试所有带前缀的版本
                    let value = if let Some(val) = row.get(col_name) {
                        Some(val)
                    } else {
                        // 尝试查找带表前缀的列名 (table.column)
                        row.iter()
                            .find(|(key, _)| {
                                key.ends_with(&format!(".{}", col_name)) || key == &col_name
                            })
                            .map(|(_, val)| val)
                    };
                    
                    value
                        .ok_or_else(|| MoteDBError::ColumnNotFound(col_name.clone()))
                        .map(|val| match val {
                            Value::Text(s) => s.clone(),
                            Value::Integer(i) => i.to_string(),
                            Value::Float(f) => f.to_string(),
                            _ => format!("{:?}", val),
                        })
                })
                .collect();
            let group_key = group_key?;
            
            groups.entry(group_key).or_default().push(row);
        }
        
        // Compute aggregates for each group
        let mut column_names = Vec::new();
        let mut result_rows = Vec::new();

        // Handle implicit aggregation with zero input rows:
        // SQL standard requires aggregate queries with no GROUP BY to return
        // exactly one row (e.g., COUNT(*) over empty table returns 0, not empty set)
        if groups.is_empty() && group_by_cols.is_empty() {
            // Compute column names from column specs
            for col_spec in columns {
                let col_name = match col_spec {
                    SelectColumn::Column(name) => name.clone(),
                    SelectColumn::ColumnWithAlias(_, alias) => alias.clone(),
                    SelectColumn::Expr(_, Some(alias)) => alias.clone(),
                    SelectColumn::Expr(expr, None) => format!("{:?}", expr),
                    SelectColumn::Star => {
                        return Err(MoteDBError::Query(
                            "SELECT * not allowed with GROUP BY".to_string()
                        ));
                    }
                };
                column_names.push(col_name);
            }

            // Compute aggregates over empty row set
            let empty_rows: Vec<&SqlRow> = Vec::new();
            let mut result_row = Vec::new();
            for col_spec in columns {
                let col_value = match col_spec {
                    SelectColumn::Expr(expr, _) => {
                        self.eval_aggregate(expr, &empty_rows)?
                    }
                    SelectColumn::Column(_)
                    | SelectColumn::ColumnWithAlias(_, _)
                    | SelectColumn::Star => Value::Null,
                };
                result_row.push(col_value);
            }
            result_rows.push(result_row);
            return Ok((column_names, result_rows));
        }

        // First pass: determine column names
        if !groups.is_empty() {
            for col_spec in columns {
                let col_name = match col_spec {
                    SelectColumn::Column(name) => name.clone(),
                    SelectColumn::ColumnWithAlias(_, alias) => alias.clone(),
                    SelectColumn::Expr(_, Some(alias)) => alias.clone(),
                    SelectColumn::Expr(expr, None) => format!("{:?}", expr),
                    SelectColumn::Star => {
                        return Err(MoteDBError::Query(
                            "SELECT * not allowed with GROUP BY".to_string()
                        ));
                    }
                };
                column_names.push(col_name);
            }
        }
        
        for (_group_key, group_rows) in groups {
            // Compute aggregate/column values
            let mut result_row = Vec::new();
            
            for col_spec in columns {
                let col_value = match col_spec {
                    SelectColumn::Column(name) => {
                        // Regular column (must be in GROUP BY)
                        if !group_by_cols.contains(name) {
                            return Err(MoteDBError::Query(
                                format!("Column '{}' must appear in GROUP BY or be in aggregate function", name)
                            ));
                        }
                        // ⭐ 支持自动解析列名
                        if let Some(val) = group_rows[0].get(name) {
                            val.clone()
                        } else {
                            // 尝试查找带表前缀的列名 (table.column)
                            group_rows[0].iter()
                                .find(|(key, _)| key.ends_with(&format!(".{}", name)))
                                .map(|(_, val)| val.clone())
                                .unwrap_or(Value::Null)
                        }
                    }
                    SelectColumn::ColumnWithAlias(name, _) => {
                        if !group_by_cols.contains(name) {
                            return Err(MoteDBError::Query(
                                format!("Column '{}' must appear in GROUP BY", name)
                            ));
                        }
                        // ⭐ 支持自动解析列名
                        if let Some(val) = group_rows[0].get(name) {
                            val.clone()
                        } else {
                            // 尝试查找带表前缀的列名 (table.column)
                            group_rows[0].iter()
                                .find(|(key, _)| key.ends_with(&format!(".{}", name)))
                                .map(|(_, val)| val.clone())
                                .unwrap_or(Value::Null)
                        }
                    }
                    SelectColumn::Expr(expr, _) => {
                        // Aggregate function or expression
                        self.eval_aggregate(expr, &group_rows)?
                    }
                    SelectColumn::Star => {
                        return Err(MoteDBError::Query(
                            "SELECT * not allowed with GROUP BY".to_string()
                        ));
                    }
                };
                
                result_row.push(col_value);
            }
            
            // Apply HAVING filter
            if let Some(having_expr) = having {
                // Create temporary row for HAVING evaluation
                let mut temp_row = SqlRow::new();
                for (i, name) in column_names.iter().enumerate() {
                    temp_row.insert(name.clone(), result_row[i].clone());
                }
                
                let passes = self.evaluator.eval(having_expr, &temp_row)
                    .and_then(|val| self.to_bool(&val))
                    .unwrap_or(false);
                
                if !passes {
                    continue;
                }
            }
            
            result_rows.push(result_row);
        }
        
        Ok((column_names, result_rows))
    }
    
    /// Evaluate aggregate function over a group of rows
    fn eval_aggregate(&self, expr: &Expr, rows: &[&SqlRow]) -> Result<Value> {
        match expr {
            Expr::FunctionCall { name, args, distinct } => {
                let func_name = name.to_uppercase();
                match func_name.as_str() {
                    "COUNT" => {
                        if *distinct {
                            // COUNT(DISTINCT column)
                            if args.is_empty() || matches!(args[0], Expr::Column(ref c) if c == "*") {
                                return Err(MoteDBError::InvalidArgument(
                                    "COUNT(DISTINCT *) is not supported".to_string()
                                ));
                            }
                            
                            use std::collections::HashSet;
                            let mut distinct_values = HashSet::new();
                            
                            for row in rows {
                                let val = self.evaluator.eval(&args[0], row)?;
                                if !matches!(val, Value::Null) {
                                    // Create a hashable key
                                    let key = format!("{:?}", val);
                                    distinct_values.insert(key);
                                }
                            }
                            
                            Ok(Value::Integer(distinct_values.len() as i64))
                        } else if args.is_empty() || matches!(args[0], Expr::Column(ref c) if c == "*") {
                            // COUNT(*)
                            Ok(Value::Integer(rows.len() as i64))
                        } else {
                            // COUNT(column) - count non-null values
                            let mut count = 0i64;
                            for row in rows {
                                let val = self.evaluator.eval(&args[0], row)?;
                                if !matches!(val, Value::Null) {
                                    count += 1;
                                }
                            }
                            Ok(Value::Integer(count))
                        }
                    }
                    "SUM" => {
                        if args.is_empty() {
                            return Err(MoteDBError::InvalidArgument("SUM requires an argument".to_string()));
                        }
                        let mut int_sum: i64 = 0;
                        let mut float_sum: f64 = 0.0;
                        let mut has_float = false;
                        let mut has_value = false;
                        for row in rows {
                            let val = self.evaluator.eval(&args[0], row)?;
                            match val {
                                Value::Integer(i) => {
                                    has_value = true;
                                    if has_float {
                                        float_sum += i as f64;
                                    } else if let Some(s) = int_sum.checked_add(i) {
                                        int_sum = s;
                                    } else {
                                        has_float = true;
                                        float_sum = int_sum as f64 + i as f64;
                                    }
                                }
                                Value::Float(f) => {
                                    has_value = true;
                                    if !has_float {
                                        has_float = true;
                                        float_sum = int_sum as f64;
                                    }
                                    float_sum += f;
                                }
                                Value::Null => {},
                                _ => return Err(MoteDBError::TypeError("SUM requires numeric values".to_string())),
                            }
                        }
                        if !has_value {
                            Ok(Value::Null)
                        } else if has_float {
                            Ok(Value::Float(float_sum))
                        } else {
                            Ok(Value::Integer(int_sum))
                        }
                    }
                    "AVG" => {
                        if args.is_empty() {
                            return Err(MoteDBError::InvalidArgument("AVG requires an argument".to_string()));
                        }
                        let mut sum = 0.0;
                        let mut count = 0;
                        for row in rows {
                            let val = self.evaluator.eval(&args[0], row)?;
                            match val {
                                Value::Integer(i) => {
                                    sum += i as f64;
                                    count += 1;
                                }
                                Value::Float(f) => {
                                    sum += f;
                                    count += 1;
                                }
                                Value::Null => {},
                                _ => return Err(MoteDBError::TypeError("AVG requires numeric values".to_string())),
                            }
                        }
                        if count == 0 {
                            Ok(Value::Null)
                        } else {
                            Ok(Value::Float(sum / count as f64))
                        }
                    }
                    "MIN" => {
                        if args.is_empty() {
                            return Err(MoteDBError::InvalidArgument("MIN requires an argument".to_string()));
                        }
                        let mut min_val: Option<Value> = None;
                        for row in rows {
                            let val = self.evaluator.eval(&args[0], row)?;
                            if !matches!(val, Value::Null) {
                                min_val = Some(match min_val {
                                    None => val,
                                    Some(current) => {
                                        if val.partial_cmp(&current) == Some(std::cmp::Ordering::Less) {
                                            val
                                        } else {
                                            current
                                        }
                                    }
                                });
                            }
                        }
                        Ok(min_val.unwrap_or(Value::Null))
                    }
                    "MAX" => {
                        if args.is_empty() {
                            return Err(MoteDBError::InvalidArgument("MAX requires an argument".to_string()));
                        }
                        let mut max_val: Option<Value> = None;
                        for row in rows {
                            let val = self.evaluator.eval(&args[0], row)?;
                            if !matches!(val, Value::Null) {
                                max_val = Some(match max_val {
                                    None => val,
                                    Some(current) => {
                                        if val.partial_cmp(&current) == Some(std::cmp::Ordering::Greater) {
                                            val
                                        } else {
                                            current
                                        }
                                    }
                                });
                            }
                        }
                        Ok(max_val.unwrap_or(Value::Null))
                    }
                    _ => Err(MoteDBError::UnknownFunction(name.clone())),
                }
            }
            _ => {
                // Non-aggregate expression in GROUP BY context
                Err(MoteDBError::Query(
                    "Non-aggregate expressions in SELECT with GROUP BY must be in GROUP BY clause".to_string()
                ))
            }
        }
    }
    
    /// Check if column list contains any aggregate functions
    fn has_aggregates(&self, columns: &[SelectColumn]) -> bool {
        columns.iter().any(|col| {
            match col {
                SelectColumn::Expr(expr, _) => self.is_aggregate_expr(expr),
                _ => false,
            }
        })
    }
    
    /// Check if an expression is an aggregate function
    fn is_aggregate_expr(&self, expr: &Expr) -> bool {
        match expr {
            Expr::FunctionCall { name, args: _, distinct: _ } => {
                matches!(name.to_uppercase().as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
            }
            _ => false,
        }
    }
    
    /// 🆕 Check if columns only contain COUNT(*) aggregate (for fast-path optimization)
    fn has_only_count_aggregate(&self, columns: &[SelectColumn]) -> bool {
        if columns.len() != 1 {
            return false;
        }
        
        match &columns[0] {
            SelectColumn::Expr(Expr::FunctionCall { name, args, .. }, _) => {
                let func_name = name.to_uppercase();
                if func_name == "COUNT" {
                    // COUNT(*) or COUNT(column)
                    args.is_empty() || matches!(args.first(), Some(Expr::Column(c)) if c == "*")
                } else {
                    false
                }
            }
            _ => false,
        }
    }
    
    fn project_columns(
        &self,
        columns: &[SelectColumn],
        rows: &[(u64, SqlRow)],
        schema: &TableSchema,
    ) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
        // Determine column names
        let column_names: Vec<String> = if columns.len() == 1 && matches!(columns[0], SelectColumn::Star) {
            // SELECT * — strip table prefix from column names for output
            // (schema may be "polluted" with qualified names after execute_from_with_limit)
            schema.columns.iter().map(|c| {
                if let Some(pos) = c.name.find('.') {
                    c.name[pos + 1..].to_string()
                } else {
                    c.name.clone()
                }
            }).collect()
        } else {
            columns.iter().map(|col| match col {
                SelectColumn::Star => "*".to_string(),
                SelectColumn::Column(name) => name.clone(),
                SelectColumn::ColumnWithAlias(_, alias) => alias.clone(),
                SelectColumn::Expr(_, Some(alias)) => alias.clone(),
                SelectColumn::Expr(expr, None) => format!("{:?}", expr), // Use debug format as default
            }).collect()
        };
        
        // 🚀 OPTIMIZATION: Reduce cloning in projection
        // Pre-calculate which columns we need to avoid repeated lookups
        // Determine table name for qualified lookups
        let table_name_for_qualify = schema.name.as_str();

        let projected_rows: Vec<Vec<Value>> = if columns.len() == 1 && matches!(columns[0], SelectColumn::Star) {
            // SELECT * - optimized path
            rows.iter().map(|(_, row)| {
                schema.columns.iter()
                    .map(|col| {
                        row.get(&col.name).cloned().unwrap_or_else(|| {
                            // Fallback: try qualified name (e.g., "items.val")
                            if !table_name_for_qualify.is_empty() {
                                let qname = format!("{}.{}", table_name_for_qualify, col.name);
                                row.get(&qname).cloned().unwrap_or(Value::Null)
                            } else {
                                Value::Null
                            }
                        })
                    })
                    .collect()
            }).collect()
        } else {
            // Specific columns - optimize column lookup
            rows.iter().map(|(_, row)| {
                columns.iter().map(|col| {
                    match col {
                        SelectColumn::Column(name) | SelectColumn::ColumnWithAlias(name, _) => {
                            // Try exact match first, then try with table prefix
                            row.get(name).cloned().or_else(|| {
                                // If column name doesn't contain '.', try prefixed versions
                                if !name.contains('.') {
                                    // Try all possible table prefixes
                                    row.iter()
                                        .find(|(k, _)| k.ends_with(&format!(".{}", name)))
                                        .map(|(_, v)| v.clone())
                                } else {
                                    None
                                }
                            }).unwrap_or(Value::Null)
                        }
                        SelectColumn::Expr(expr, _) => {
                            self.eval_with_materialized(expr, row).unwrap_or(Value::Null)
                        }
                        SelectColumn::Star => Value::Null, // Shouldn't happen
                    }
                }).collect()
            }).collect()
        };
        
        Ok((column_names, projected_rows))
    }
    
    /// Execute INSERT statement
    fn execute_insert(&self, stmt: InsertStmt) -> Result<QueryResult> {
        let schema = self.db.get_table_schema(&stmt.table)?;
        
        // Determine column order
        let columns = if let Some(ref cols) = stmt.columns {
            cols.clone()
        } else {
            // Use schema order
            schema.columns.iter().map(|c| c.name.clone()).collect()
        };

        // Route TimeSeries INSERT to columnar store
        if schema.table_type == crate::types::TableType::TimeSeries {
            return self.execute_columnar_insert(&stmt, &schema, &columns);
        }

        // 🔥 召回率优化: 使用批量插入提升向量索引图质量
        // 原因: 逐条插入导致DiskANN图连通性差，批量插入可以构建更优质的图
        // 策略: 
        // 1. 先批量准备所有行（不写入数据库）
        // 2. 判断是否涉及向量索引（检测TENSOR列）
        // 3. 如果有向量列，使用批量向量插入 API（会触发图重建）
        // 4. 如果无向量列，使用普通逐条插入
        
        let has_vector_column = schema.columns.iter()
            .any(|col| matches!(col.col_type, crate::types::ColumnType::Tensor(_)));
        
        // Prepare all rows first
        let mut prepared_rows = Vec::new();
        
        for value_row in &stmt.values {
            if value_row.len() != columns.len() {
                return Err(MoteDBError::InvalidArgument(
                    format!("Column count mismatch: expected {}, got {}", columns.len(), value_row.len())
                ));
            }
            
            // Build SqlRow
            let mut sql_row = SqlRow::new();
            for (i, col_name) in columns.iter().enumerate() {
                let val = match &value_row[i] {
                    Expr::Literal(v) => v.clone(),
                    Expr::Parameter(_) => {
                        let empty_row = SqlRow::new();
                        self.evaluator.eval(&value_row[i], &empty_row)?
                    }
                    expr => return Err(MoteDBError::InvalidArgument(
                        format!("INSERT VALUES must be literals or parameters, got {:?}", expr)
                    )),
                };

                // 🚀 P3 OPTIMIZATION: For AUTO_INCREMENT primary key, ignore user-provided value
                // The system will use row_id as primary key value automatically
                let should_ignore = schema.primary_key()
                    .map(|pk| pk == col_name && schema.is_primary_key_auto_increment())
                    .unwrap_or(false);
                
                if !should_ignore {
                    sql_row.insert(col_name.clone(), val);
                }
                // If ignored, skip inserting this column (system will fill in row_id later)
            }
            
            // Convert to storage Row
            let row = sql_row_to_row(&sql_row, &schema)?;
            prepared_rows.push((sql_row, row));
        }
        
        let affected_rows = prepared_rows.len();
        
        // 🔥 Track last_insert_id for AUTO_INCREMENT primary key
        let mut last_row_id: Option<u64> = None;
        
        if has_vector_column && prepared_rows.len() > 1 {
            // 🚀 批量插入路径：提升向量索引质量
            debug_log!("[SQL] 🔥 Batch inserting {} rows with vector columns...", prepared_rows.len());
            
            // 提取所有row_id和向量数据
            let mut vector_batches: std::collections::HashMap<String, Vec<(u64, Vec<f32>)>> = 
                std::collections::HashMap::new();
            
            // 先插入所有行到表（获取row_id）
            for (_sql_row, row) in prepared_rows {
                let row_id = self.db.insert_row_to_table(&stmt.table, row.clone())?;
                last_row_id = Some(row_id); // Track last inserted row_id
                
                // 检查是否有向量列需要索引
                for (idx, col_def) in schema.columns.iter().enumerate() {
                    if let crate::types::ColumnType::Tensor(_dim) = col_def.col_type {
                        // 提取向量值
                        if let Some(Value::Vector(vec)) = row.get(idx) {
                            let index_name = format!("{}_{}", stmt.table, col_def.name);
                            vector_batches.entry(index_name)
                                .or_default()
                                .push((row_id, vec.to_vec()));
                        }
                    }
                }
            }
            
            // 批量插入向量到索引（使用公开API）
            // 🔧 修复：如果索引不存在，跳过（稍后通过CREATE INDEX构建）
            for (index_name, batch) in vector_batches {
                debug_log!("[SQL]   ↳ Batch indexing {} vectors to '{}'...", batch.len(), index_name);
                let insert_start = std::time::Instant::now();
                match self.db.batch_update_vectors(&index_name, batch) {
                    Ok(_) => {
                        debug_log!("[SQL]   ✓ Indexed in {:?}", insert_start.elapsed());
                    },
                    Err(e) if e.to_string().contains("not found") => {
                        debug_log!("[SQL]   ⚠️  Index '{}' not found, skipping (will be built by CREATE INDEX)", index_name);
                    },
                    Err(e) => return Err(e),
                }
            }
        } else {
            // 普通逐条插入路径（无向量列或单行插入）
            for (_sql_row, row) in prepared_rows {
                let row_id = self.db.insert_row_to_table(&stmt.table, row)?;
                last_row_id = Some(row_id); // Track last inserted row_id
            }
        }
        
        // Update last_insert_id if table has AUTO_INCREMENT primary key
        if schema.is_primary_key_auto_increment() {
            if let Some(row_id) = last_row_id {
                self.last_insert_id.store(row_id as i64, std::sync::atomic::Ordering::Relaxed);
                self.evaluator.last_insert_id.store(row_id as i64, std::sync::atomic::Ordering::Relaxed);
            }
        }
        
        Ok(QueryResult::Modification { affected_rows })
    }
    
    /// Execute UPDATE statement
    fn execute_update(&self, stmt: UpdateStmt) -> Result<QueryResult> {
        let schema = self.db.get_table_schema(&stmt.table)?;

        // 🚀 PK fast path: skip full table scan for WHERE pk = value
        if let Some(ref where_clause) = stmt.where_clause {
            if let Some((col_name, target_value)) = self.try_extract_point_query(where_clause) {
                let is_pk = schema.primary_key()
                    .map(|pk| pk == col_name)
                    .unwrap_or(false);

                if is_pk {
                    return self.execute_update_pk(&stmt, &schema, &target_value);
                }

                // 🚀 Column index fast path: use index to find matching rows
                if let Some(index_name) = self.db.index_registry.find_by_column(
                    &stmt.table, &col_name,
                    crate::database::index_metadata::IndexType::Column
                ) {
                    if let Some(index) = self.db.column_indexes.get(&index_name) {
                        let matching_row_ids = index.value().read()
                            .get(&target_value)
                            .unwrap_or_default();
                        if matching_row_ids.is_empty() {
                            return Ok(QueryResult::Modification { affected_rows: 0 });
                        }
                        return self.execute_update_by_row_ids(&stmt, &schema, &matching_row_ids, &col_name, &target_value);
                    }
                }
            }
        }

        // 🚀 Use真正的流式扫描 (O(1) memory)
        let row_iter = self.db.scan_table_rows_streaming(&stmt.table)?;
        
        let mut affected_rows = 0;
        
        for result in row_iter {
            let (row_id, row) = result?;
            let sql_row = row_to_sql_row(&row, &schema)?;
            
            // Filter rows (WHERE clause)
            let should_update = if let Some(ref where_clause) = stmt.where_clause {
                self.evaluator.eval(where_clause, &sql_row)
                    .and_then(|val| self.to_bool(&val))
                    .unwrap_or(false)
            } else {
                true
            };
            
            if !should_update {
                continue;
            }
            
            let mut sql_row = sql_row;

            // Evaluate all assignments against the ORIGINAL row values (SQL semantics)
            let original_row = sql_row.clone();
            let new_values: Vec<(String, Value)> = stmt.assignments.iter().map(|(col_name, expr)| {
                let new_val = if let Expr::Literal(v) = expr {
                    v.clone()
                } else {
                    self.evaluator.eval(expr, &original_row).unwrap_or(Value::Null)
                };
                (col_name.clone(), new_val)
            }).collect();

            for (col_name, new_val) in new_values {
                sql_row.insert(col_name, new_val);
            }
            
            // Convert back to storage Row
            let new_row = sql_row_to_row(&sql_row, &schema)?;
            
            // 🚀 底层已实现增量索引更新，传入 old_row 避免重复加载
            self.db.update_row_in_table(&stmt.table, row_id, row, new_row)?;
            
            affected_rows += 1;
        }
        
        Ok(QueryResult::Modification { affected_rows })
    }
    
    /// Execute DELETE statement
    fn execute_delete(&self, stmt: DeleteStmt) -> Result<QueryResult> {
        let schema = self.db.get_table_schema(&stmt.table)?;

        // 🚀 PK fast path: skip full table scan for WHERE pk = value
        if let Some(ref where_clause) = stmt.where_clause {
            if let Some((col_name, target_value)) = self.try_extract_point_query(where_clause) {
                let is_pk = schema.primary_key()
                    .map(|pk| pk == col_name)
                    .unwrap_or(false);

                if is_pk {
                    return self.execute_delete_pk(&stmt, &schema, &target_value);
                }

                // 🚀 Column index fast path: use index to find matching rows
                if let Some(index_name) = self.db.index_registry.find_by_column(
                    &stmt.table, &col_name,
                    crate::database::index_metadata::IndexType::Column
                ) {
                    if let Some(index) = self.db.column_indexes.get(&index_name) {
                        let matching_row_ids = index.value().read()
                            .get(&target_value)
                            .unwrap_or_default();
                        if matching_row_ids.is_empty() {
                            return Ok(QueryResult::Modification { affected_rows: 0 });
                        }
                        return self.execute_delete_by_row_ids(&stmt, &schema, &matching_row_ids, &col_name, &target_value);
                    }
                }
            }
        }

        // 🚀 Use真正的流式扫描 (O(1) memory)
        let row_iter = self.db.scan_table_rows_streaming(&stmt.table)?;
        
        let mut affected_rows = 0;
        
        for result in row_iter {
            let (row_id, row) = result?;
            let sql_row = row_to_sql_row(&row, &schema)?;
            
            // Filter rows (WHERE clause)
            let should_delete = if let Some(ref where_clause) = stmt.where_clause {
                self.evaluator.eval(where_clause, &sql_row)
                    .and_then(|val| self.to_bool(&val))
                    .unwrap_or(false)
            } else {
                true
            };
            
            if !should_delete {
                continue;
            }
            
            // Delete row - 底层已实现增量索引维护，传入 old_row 避免重复加载
            self.db.delete_row_from_table(&stmt.table, row_id, row)?;
            affected_rows += 1;
        }
        
        Ok(QueryResult::Modification { affected_rows })
    }

    /// 🚀 PK fast path for UPDATE: direct lookup instead of full table scan
    ///
    /// For `UPDATE t SET ... WHERE pk = value`:
    /// - AUTO_INCREMENT: direct LSM get by row_id (O(log n))
    /// - Non-AUTO_INCREMENT: column index lookup then LSM get
    ///   Resolve PK value to RowId using pk_lookup cache.
    ///   On cache miss, falls back to disk-based column index and refills the cache.
    ///   This ensures that repeated lookups for the same PK value are fast (O(1) after first access).
    fn resolve_pk_with_cache(
        &self,
        table: &str,
        pk_key: &crate::database::pk_cache::PkKey,
        pk_col_name: &str,
        pk_value: &Value,
    ) -> Result<Option<RowId>> {
        // Try LRU cache first
        if let Some(lookup) = self.db.pk_lookup.get(table) {
            if let Some(rid) = lookup.get_pk(pk_key) {
                return Ok(Some(rid));
            }
        }

        // Cache miss — fall back to column index, or full scan if index missing
        let row_ids = match self.db.query_by_column(table, pk_col_name, pk_value) {
            Ok(ids) => ids,
            Err(_) => {
                // Column index not available (e.g. after restart) — full scan fallback
                let schema = self.db.get_table_schema(table)?;
                let pk_pos = schema.get_column_position(pk_col_name).unwrap_or(0);
                let rows = self.db.scan_table_rows_streaming(table)?;
                let mut found = Vec::new();
                for item in rows {
                    let (row_id, row) = item?;
                    if let Some(val) = row.get(pk_pos) {
                        if val == pk_value {
                            found.push(row_id);
                            break;
                        }
                    }
                }
                found
            }
        };

        // Refill cache from disk result so next lookup is O(1)
        if let Some(&rid) = row_ids.first() {
            if let Some(lookup) = self.db.pk_lookup.get(table) {
                lookup.insert(pk_key.clone(), rid);
            }
        }

        Ok(row_ids.into_iter().next())
    }

    fn execute_update_pk(
        &self,
        stmt: &UpdateStmt,
        schema: &crate::types::TableSchema,
        target_value: &Value,
    ) -> Result<QueryResult> {
        let pk_col_name = schema.primary_key()
            .ok_or_else(|| StorageError::InvalidData("No primary key".into()))?;

        // Resolve row_id(s) for the PK value
        let row_ids = if schema.is_primary_key_auto_increment() {
            // AUTO_INCREMENT: pk value IS row_id — direct O(1) mapping
            match target_value {
                Value::Integer(id) if *id >= 0 => vec![*id as RowId],
                _ => return Ok(QueryResult::Modification { affected_rows: 0 }),
            }
        } else {
            // Non-AUTO_INCREMENT: resolve via pk_lookup cache (with disk fallback + cache refill)
            let pk_key = crate::database::pk_cache::PkKey::from_value(target_value);
            match self.resolve_pk_with_cache(&stmt.table, &pk_key, pk_col_name, target_value)? {
                Some(rid) => vec![rid],
                None => vec![],
            }
        };

        let mut affected_rows = 0;
        for row_id in row_ids {
            let row = match self.db.get_table_row(&stmt.table, row_id)? {
                Some(r) => r,
                None => continue,
            };

            let mut sql_row = row_to_sql_row(&row, schema)?;
            // Evaluate all assignments against ORIGINAL row (SQL semantics)
            let original_row = sql_row.clone();
            let new_values: Vec<(String, Value)> = stmt.assignments.iter().map(|(col_name, expr)| {
                let new_val = if let Expr::Literal(v) = expr {
                    v.clone()
                } else {
                    self.evaluator.eval(expr, &original_row).unwrap_or(Value::Null)
                };
                (col_name.clone(), new_val)
            }).collect();
            for (col_name, new_val) in new_values {
                sql_row.insert(col_name, new_val);
            }

            let new_row = sql_row_to_row(&sql_row, schema)?;
            self.db.update_row_in_table(&stmt.table, row_id, row, new_row)?;
            affected_rows += 1;
        }

        Ok(QueryResult::Modification { affected_rows })
    }

    /// 🚀 PK fast path for DELETE: direct lookup instead of full table scan
    ///
    /// For `DELETE FROM t WHERE pk = value`:
    /// - AUTO_INCREMENT: direct LSM get by row_id (O(log n))
    /// - Non-AUTO_INCREMENT: column index lookup then LSM get
    fn execute_delete_pk(
        &self,
        stmt: &DeleteStmt,
        schema: &crate::types::TableSchema,
        target_value: &Value,
    ) -> Result<QueryResult> {
        let pk_col_name = schema.primary_key()
            .ok_or_else(|| StorageError::InvalidData("No primary key".into()))?;

        // Resolve row_id(s) for the PK value
        let row_ids = if schema.is_primary_key_auto_increment() {
            // AUTO_INCREMENT: pk value IS row_id — direct O(1) mapping
            match target_value {
                Value::Integer(id) if *id >= 0 => vec![*id as RowId],
                _ => return Ok(QueryResult::Modification { affected_rows: 0 }),
            }
        } else {
            // Non-AUTO_INCREMENT: resolve via pk_lookup cache (with disk fallback + cache refill)
            let pk_key = crate::database::pk_cache::PkKey::from_value(target_value);
            match self.resolve_pk_with_cache(&stmt.table, &pk_key, pk_col_name, target_value)? {
                Some(rid) => vec![rid],
                None => vec![],
            }
        };

        let mut affected_rows = 0;
        for row_id in row_ids {
            let row = match self.db.get_table_row(&stmt.table, row_id)? {
                Some(r) => r,
                None => continue,
            };

            self.db.delete_row_from_table(&stmt.table, row_id, row)?;
            affected_rows += 1;
        }

        Ok(QueryResult::Modification { affected_rows })
    }

    /// 🚀 Column index fast path for UPDATE: lookup by row_ids from index
    fn execute_update_by_row_ids(
        &self,
        stmt: &UpdateStmt,
        schema: &crate::types::TableSchema,
        row_ids: &[RowId],
        where_col: &str,
        where_val: &crate::types::Value,
    ) -> Result<QueryResult> {
        let mut affected_rows = 0;
        for &row_id in row_ids {
            let row = match self.db.get_table_row(&stmt.table, row_id)? {
                Some(r) => r,
                None => continue,
            };

            // Re-check WHERE condition against actual row data to guard
            // against stale index entries pointing to modified rows.
            if let Some(col) = schema.get_column(where_col) {
                if let Some(actual_val) = row.get(col.position) {
                    if actual_val != where_val { continue; }
                } else { continue; }
            }

            let mut sql_row = row_to_sql_row(&row, schema)?;
            // Evaluate all assignments against ORIGINAL row (SQL semantics)
            let original_row = sql_row.clone();
            let new_values: Vec<(String, Value)> = stmt.assignments.iter().map(|(col_name, expr)| {
                let new_val = if let Expr::Literal(v) = expr {
                    v.clone()
                } else {
                    self.evaluator.eval(expr, &original_row).unwrap_or(Value::Null)
                };
                (col_name.clone(), new_val)
            }).collect();
            for (col_name, new_val) in new_values {
                sql_row.insert(col_name, new_val);
            }

            let new_row = sql_row_to_row(&sql_row, schema)?;
            self.db.update_row_in_table(&stmt.table, row_id, row, new_row)?;
            affected_rows += 1;
        }

        Ok(QueryResult::Modification { affected_rows })
    }

    /// 🚀 Column index fast path for DELETE: lookup by row_ids from index
    fn execute_delete_by_row_ids(
        &self,
        stmt: &DeleteStmt,
        schema: &crate::types::TableSchema,
        row_ids: &[RowId],
        where_col: &str,
        where_val: &crate::types::Value,
    ) -> Result<QueryResult> {
        let mut affected_rows = 0;
        for &row_id in row_ids {
            let row = match self.db.get_table_row(&stmt.table, row_id)? {
                Some(r) => r,
                None => continue,
            };

            // Re-check WHERE condition against actual row data
            if let Some(col) = schema.get_column(where_col) {
                if let Some(actual_val) = row.get(col.position) {
                    if actual_val != where_val { continue; }
                } else { continue; }
            }

            self.db.delete_row_from_table(&stmt.table, row_id, row)?;
            affected_rows += 1;
        }

        Ok(QueryResult::Modification { affected_rows })
    }

    /// Execute CREATE TABLE statement
    fn execute_create_table(&self, stmt: CreateTableStmt) -> Result<QueryResult> {
        // Convert AST column defs to TableSchema
        let columns: Vec<crate::types::ColumnDef> = stmt.columns.iter().enumerate().map(|(pos, col)| {
            let column_type = match col.data_type {
                DataType::Integer => ColumnType::Integer,
                DataType::BigInt => ColumnType::Integer,  // 🚀 Phase 4: Map BIGINT to Integer (both i64)
                DataType::Float => ColumnType::Float,
                DataType::Text => ColumnType::Text,
                DataType::Boolean => ColumnType::Boolean,
                DataType::Timestamp => ColumnType::Timestamp,
                DataType::Vector(dim) => ColumnType::Tensor(dim.unwrap_or(128)),
                DataType::Geometry => ColumnType::Spatial,
            };
            
            let mut col_def = crate::types::ColumnDef::new(
                col.name.clone(),
                column_type,
                pos,
            );
            if !col.nullable {
                col_def = col_def.not_null();
            }
            // 🚀 AUTO_INCREMENT flag with optional start value (Phase 5)
            if col.auto_increment {
                if let Some(start) = col.auto_increment_start {
                    col_def = col_def.auto_increment_with_start(start);
                } else {
                    col_def = col_def.auto_increment();
                }
            }
            col_def
        }).collect();
        
        // 🆕 STEP 1: Find primary key columns
        let primary_key_cols: Vec<&super::ast::ColumnDef> = stmt.columns.iter()
            .filter(|col| col.primary_key)
            .collect();
        
        // 🆕 STEP 2: Set primary key in schema
        let mut schema = TableSchema::new(stmt.table.clone(), columns);
        if let Some(pk_col) = primary_key_cols.first() {
            schema = schema.with_primary_key(pk_col.name.clone());

            // 🚀 Phase 5: Set AUTO_INCREMENT flag with optional start value
            if pk_col.auto_increment {
                if let Some(start) = pk_col.auto_increment_start {
                    schema = schema.with_auto_increment_start(start);
                } else {
                    schema = schema.with_auto_increment();
                }
            }
        }

        // TimeSeries table type and TTL
        if let Some(ref ts_col) = stmt.timeseries_column {
            schema = schema.with_timeseries(ts_col.clone());
        }
        if let Some(ref ttl) = stmt.ttl {
            schema = schema.with_ttl(*ttl);
        }
        
        self.db.create_table(schema.clone())?;
        
        // 🚀 P0 FIX: Auto-create column index for primary key (ONLY if NOT AUTO_INCREMENT)
        // AUTO_INCREMENT主键不需要列索引（主键值 = row_id，直接查询）
        if let Some(pk_col) = primary_key_cols.first() {
            if !pk_col.auto_increment {
                let _pk_index_name = format!("{}.{}", stmt.table, pk_col.name);
                self.db.create_column_index(&stmt.table, &pk_col.name)?;
            }
        }
        
        // 🚨 DEADLOCK FIX: create_table() already auto-creates primary key index
        // No need to manually create it again (prevents double creation deadlock)
        let pk_info = if !primary_key_cols.is_empty() {
            let pk_names: Vec<String> = primary_key_cols.iter().map(|c| c.name.clone()).collect();
            let auto_inc = if primary_key_cols[0].auto_increment { " AUTO_INCREMENT" } else { "" };
            format!(" (Primary key: {}{}, auto-index: ✓)", pk_names.join(", "), auto_inc)
        } else {
            String::new()
        };

        let ts_info = match &stmt.timeseries_column {
            Some(col) => format!(", timeseries({})", col),
            None => String::new(),
        };
        let ttl_info = match &stmt.ttl {
            Some(ttl) => format!(", TTL {}", ttl),
            None => String::new(),
        };

        Ok(QueryResult::Definition {
            message: format!("Table '{}' created successfully{}{}{}", stmt.table, pk_info, ts_info, ttl_info),
        })
    }
    
    /// Execute CREATE INDEX statement
    fn execute_create_index(&self, stmt: CreateIndexStmt) -> Result<QueryResult> {
        // Get table schema to find column type
        let schema = self.db.get_table_schema(&stmt.table)?;
        let column = schema.columns.iter()
            .find(|c| c.name == stmt.column)
            .ok_or_else(|| MoteDBError::ColumnNotFound(stmt.column.clone()))?;
        
        // Determine index type: use explicit type from AST, or infer from column type
        let index_type = match stmt.index_type {
            IndexType::Text => {
                // Verify column is compatible with text index
                if !matches!(column.col_type, ColumnType::Text) {
                    return Err(MoteDBError::TypeError(
                        format!("TEXT index requires TEXT column, got {:?}", column.col_type)
                    ));
                }
                IndexType::Text
            }
            IndexType::Vector => {
                // Verify column is tensor/vector
                if let ColumnType::Tensor(_dim) = column.col_type {
                    IndexType::Vector
                } else {
                    return Err(MoteDBError::TypeError(
                        format!("VECTOR index requires TENSOR column, got {:?}", column.col_type)
                    ));
                }
            }
            IndexType::Timestamp => {
                // Verify column is timestamp
                if !matches!(column.col_type, ColumnType::Timestamp) {
                    return Err(MoteDBError::TypeError(
                        format!("TIMESTAMP index requires TIMESTAMP column, got {:?}", column.col_type)
                    ));
                }
                IndexType::Timestamp
            }
            IndexType::Octree => {
                // Verify column is spatial (3D points)
                if !matches!(column.col_type, ColumnType::Spatial) {
                    return Err(MoteDBError::TypeError(
                        format!("OCTREE index requires SPATIAL column, got {:?}", column.col_type)
                    ));
                }
                IndexType::Octree
            }
            IndexType::BTree | IndexType::Column => {
                // B-Tree/Column index can be used for any comparable type
                stmt.index_type.clone()
            }
        };
        
        // Create index based on type
        // 🆕 Use user-specified index name or generate default
        let index_name = if !stmt.index_name.is_empty() {
            stmt.index_name.clone()
        } else {
            // Fallback to default naming: {table}_{column}
            format!("{}_{}", stmt.table, stmt.column)
        };
        
        match index_type {
            IndexType::Text => {
                // 1️⃣ Create empty text index
                self.db.create_text_index(&index_name)?;
                
                // 2️⃣ ✅ P0 FIX: 批量流式回填（避免内存爆炸 + 锁风暴）
                let column_pos = schema.get_column_position(&stmt.column)
                    .ok_or_else(|| MoteDBError::ColumnNotFound(stmt.column.clone()))?;
                
                let start_time = std::time::Instant::now();
                let mut backfill_count = 0;
                
                // ✅ 使用批量流式扫描（每批10000行，避免内存爆炸）
                let batch_iter = self.db.scan_table_rows_batched(&stmt.table, 10000)?;
                
                for batch_result in batch_iter {
                    let batch = batch_result?;
                    
                    // 收集本批次的文本数据
                    let texts_in_batch: Vec<_> = batch.iter()
                        .filter_map(|(row_id, row)| {
                            row.get(column_pos).and_then(|v| {
                                if let Value::Text(text) = v {
                                    Some((*row_id, text.as_str()))
                                } else {
                                    None
                                }
                            })
                        })
                        .collect();
                    
                    // ✅ 一次写锁，批量插入整个batch
                    if !texts_in_batch.is_empty() {
                        if let Some(index_arc) = self.db.text_indexes.get(&index_name) {
                            let mut index = index_arc.write();
                            for (row_id, text) in texts_in_batch {
                                if let Err(e) = index.insert(row_id, text) {
                                    debug_log!("⚠️ Failed to backfill text index for row {}: {}", row_id, e);
                                } else {
                                    backfill_count += 1;
                                }
                            }
                            // 锁在此处释放（每10000条释放一次，允许并发查询）
                        }
                    }
                }
                
                if backfill_count > 0 {
                    debug_log!("Built text index in {:?}, indexed {} rows", start_time.elapsed(), backfill_count);
                }
                
                // 3️⃣ Register metadata
                let metadata = crate::database::index_metadata::IndexMetadata::new(
                    index_name.clone(),
                    stmt.table.clone(),
                    stmt.column.clone(),
                    crate::database::index_metadata::IndexType::Text,
                );
                self.db.index_registry.register(metadata)?;
            }
            IndexType::Vector => {
                // 1️⃣ Create empty vector index
                if let ColumnType::Tensor(dim) = column.col_type {
                    self.db.create_vector_index(&index_name, dim, stmt.metric.as_deref())?;
                    
                    // 2️⃣ Backfill existing data (critical fix!)
                    let column_pos = schema.get_column_position(&stmt.column)
                        .ok_or_else(|| MoteDBError::ColumnNotFound(stmt.column.clone()))?;
                    
                    let mut vectors_to_insert = Vec::new();
                    let iter = self.db.scan_table_rows_streaming(&stmt.table)?;

                    for result in iter {
                        let (row_id, row) = result?;
                        if let Some(Value::Tensor(tensor)) = row.get(column_pos) {
                            let f32_vec = tensor.to_f32();
                            vectors_to_insert.push((row_id, f32_vec));
                        }
                    }
                    
                    if !vectors_to_insert.is_empty() {
                        let _backfill_count = self.db.batch_insert_vectors(&index_name, &vectors_to_insert)?;
                    }
                    
                    // 3️⃣ Register metadata
                    let mut metadata = crate::database::index_metadata::IndexMetadata::new(
                        index_name.clone(),
                        stmt.table.clone(),
                        stmt.column.clone(),
                        crate::database::index_metadata::IndexType::Vector,
                    );
                    metadata.metric = stmt.metric.clone();
                    self.db.index_registry.register(metadata)?;
                } else {
                    unreachable!("Already validated column type");
                }
            }
            IndexType::Timestamp => {
                // Timestamp index is global and already created with database
                // No-op, but return success
            }
            IndexType::Octree => {
                // Create i-Octree index for 3D point cloud data
                self.db.create_ioctree_index(&index_name)?;

                // Backfill existing 3D point data
                let column_pos = schema.get_column_position(&stmt.column)
                    .ok_or_else(|| MoteDBError::ColumnNotFound(stmt.column.clone()))?;

                let iter = self.db.scan_table_rows_streaming(&stmt.table)?;
                let mut backfill_count = 0;

                for result in iter {
                    let (row_id, row) = result?;
                    if let Some(Value::Spatial(geometry)) = row.get(column_pos) {
                        if geometry.is_3d() {
                            if let Err(e) = self.db.insert_ioctree_point(row_id, &index_name, geometry) {
                                debug_log!("⚠️ Failed to backfill ioctree index for row {}: {}", row_id, e);
                            } else {
                                backfill_count += 1;
                            }
                        }
                    }
                }

                if backfill_count > 0 {
                    debug_log!("Backfilled {} rows into ioctree index '{}'", backfill_count, index_name);
                }

                // Register metadata
                let metadata = crate::database::index_metadata::IndexMetadata::new(
                    index_name.clone(),
                    stmt.table.clone(),
                    stmt.column.clone(),
                    crate::database::index_metadata::IndexType::Octree,
                );
                self.db.index_registry.register(metadata)?;
            }
            IndexType::BTree | IndexType::Column => {
                // 🚀 Column/BTree index creation
                // Column index works for any comparable type (Integer, Float, Text, etc.)
                // Bulk backfill is now handled internally by create_column_index()
                
                self.db.create_column_index_with_name(&stmt.table, &stmt.column, &index_name)?;
                
                // 🔥 OPTIMIZATION FIX: Also register with standard "{table}.{column}" name
                // This allows WHERE optimization to find the index
                let standard_name = format!("{}.{}", stmt.table, stmt.column);
                if index_name != standard_name {
                    // Clone the index reference and register with standard name
                    if let Some(index_ref) = self.db.column_indexes.get(&index_name) {
                        self.db.column_indexes.insert(standard_name.clone(), index_ref.clone());
                    }
                }
                
                // 🆕 Register metadata
                let metadata = crate::database::index_metadata::IndexMetadata::new(
                    index_name.clone(),
                    stmt.table.clone(),
                    stmt.column.clone(),
                    crate::database::index_metadata::IndexType::Column,
                );
                self.db.index_registry.register(metadata)?;
            }
        }
        
        Ok(QueryResult::Definition {
            message: format!("Index '{}' created successfully on {}.{}", 
                index_name, stmt.table, stmt.column),
        })
    }
    
    /// Execute DROP TABLE statement
    fn execute_drop_table(&self, stmt: DropTableStmt) -> Result<QueryResult> {
        let table_name = &stmt.table;

        // Verify table exists
        let schema = self.db.get_table_schema(table_name)?;

        // 1. Drop column indexes for this table
        let prefix = format!("{}.", table_name);
        let index_names: Vec<String> = self.db.column_indexes.iter()
            .filter(|entry| entry.key().starts_with(&prefix))
            .map(|entry| entry.key().clone())
            .collect();
        for idx_name in index_names {
            self.db.column_indexes.remove(&idx_name);
        }

        // 2. Drop vector indexes for this table
        let vector_idx_names: Vec<String> = self.db.vector_indexes.iter()
            .filter(|entry| entry.key().starts_with(&prefix) || entry.key().contains(&format!("_{}", table_name)))
            .map(|entry| entry.key().clone())
            .collect();
        for idx_name in vector_idx_names {
            self.db.vector_indexes.remove(&idx_name);
        }

        // 3. Drop text indexes for this table
        let text_idx_names: Vec<String> = self.db.text_indexes.iter()
            .filter(|entry| entry.key().starts_with(&prefix) || entry.key().contains(&format!("_{}", table_name)))
            .map(|entry| entry.key().clone())
            .collect();
        for idx_name in text_idx_names {
            self.db.text_indexes.remove(&idx_name);
        }

        // 5. Drop table metadata (schema, auto_increment, pk_lookup)
        self.db.drop_table(table_name)?;

        // 6. Remove index registry entries
        self.db.index_registry.remove_by_table(table_name);

        // 7. Delete data from LSM using range delete (best effort)
        // Composite key = (table_id << 32) | row_id
        // We scan the entire range for this table_id
        let table_id = self.db.table_registry.get_table_id(table_name).unwrap_or(0);
        let start_key = (table_id as u64) << 32;
        let end_key = start_key | 0xFFFFFFFF;
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        if let Err(e) = self.db.lsm_engine.delete_range(start_key, end_key, timestamp) {
            debug_log!("[DROP TABLE] Warning: LSM range delete failed: {}", e);
        }

        let _ = schema; // used above for validation

        Ok(QueryResult::Definition {
            message: format!("Table '{}' dropped successfully", table_name),
        })
    }
    
    /// Execute DROP INDEX statement
    fn execute_drop_index(&self, stmt: DropIndexStmt) -> Result<QueryResult> {
        use crate::database::index_metadata::IndexType;

        // Look up index metadata to know which collection to remove from
        let meta = self.db.index_registry.get(&stmt.index_name)
            .ok_or_else(|| MoteDBError::IndexNotFound(stmt.index_name.clone()))?;

        let index_name = &stmt.index_name;

        // Remove from the appropriate DashMap collection
        match meta.index_type {
            IndexType::Vector => {
                self.db.vector_indexes.remove(index_name);
            }
            IndexType::Text => {
                self.db.text_indexes.remove(index_name);
            }
            IndexType::Column => {
                self.db.column_indexes.remove(index_name);
                // Also remove the "table.column" alias if it exists
                let alias = format!("{}.{}", meta.table_name, meta.column_name);
                if alias != *index_name {
                    self.db.column_indexes.remove(&alias);
                }
            }
            IndexType::Octree => {
                self.db.ioctree_indexes.remove(index_name);
            }
        }

        // Remove from index registry (also persists)
        self.db.index_registry.remove(index_name)?;

        Ok(QueryResult::Definition {
            message: format!("Index '{}' dropped", index_name),
        })
    }
    
    /// 🆕 Execute ALTER TABLE statement
    fn execute_alter_table(&self, stmt: AlterTableStmt) -> Result<QueryResult> {
        use super::ast::AlterTableAction;
        
        match stmt.action {
            AlterTableAction::SetAutoIncrement(new_value) => {
                // Verify table exists and has AUTO_INCREMENT primary key
                let schema = self.db.get_table_schema(&stmt.table)?;
                
                if !schema.is_primary_key_auto_increment() {
                    return Err(MoteDBError::InvalidArgument(
                        format!("Table {} does not have AUTO_INCREMENT primary key", stmt.table)
                    ));
                }
                
                // Update the AUTO_INCREMENT counter
                self.db.set_auto_increment_value(&stmt.table, new_value)?;
                
                Ok(QueryResult::Definition {
                    message: format!("Table {} AUTO_INCREMENT set to {}", stmt.table, new_value),
                })
            }
        }
    }
    
    /// Execute SHOW TABLES
    fn execute_show_tables(&self) -> Result<QueryResult> {
        let tables = self.db.list_tables()?;
        
        let columns = vec!["Tables".to_string()];
        let rows = tables.into_iter()
            .map(|table_name| vec![Value::Text(table_name)])
            .collect();
        
        Ok(QueryResult::Select { columns, rows })
    }
    
    /// Execute DESCRIBE TABLE
    fn execute_describe_table(&self, table_name: String) -> Result<QueryResult> {
        let schema = self.db.get_table_schema(&table_name)?;
        
        let columns = vec![
            "Field".to_string(),
            "Type".to_string(),
            "Nullable".to_string(),
            "Position".to_string(),
        ];
        
        let rows = schema.columns.iter().map(|col| {
            vec![
                Value::Text(col.name.clone()),
                Value::Text(format!("{:?}", col.col_type)),
                Value::Text(if col.nullable { "YES" } else { "NO" }.into()),
                Value::Integer(col.position as i64),
            ]
        }).collect();
        
        Ok(QueryResult::Select { columns, rows })
    }
    
    // Helper methods
    
    /// ✅ 优化辅助函数：高效构造 qualified name (table.column)
    #[inline]
    fn make_qualified_name(prefix: &str, col_name: &str) -> String {
        let mut qualified = String::with_capacity(prefix.len() + 1 + col_name.len());
        qualified.push_str(prefix);
        qualified.push('.');
        qualified.push_str(col_name);
        qualified
    }
    
    /// 🎯 Try to extract range query: WHERE col >= start AND col <= end
    /// Returns Some((column_name, start_value, end_value))
    /// 🚀 Try to extract dual-bound range query: WHERE col > X AND col < Y
    /// Returns (column_name, lower_bound, lower_op, upper_bound, upper_op)
    fn try_extract_range_query(&self, expr: &Expr) -> Option<(String, Value, BinaryOperator, Value, BinaryOperator)> {
        use crate::sql::ast::{BinaryOperator, Expr};
        
        match expr {
            Expr::BinaryOp { left, op, right } => {
                // Check for AND expressions
                if *op == BinaryOperator::And {
                    // Try to extract range from both sides
                    if let (Expr::BinaryOp { left: l1, op: op1, right: r1 }, 
                            Expr::BinaryOp { left: l2, op: op2, right: r2 }) 
                        = (left.as_ref(), right.as_ref()) {
                        
                        // Check if both sides reference the same column
                        let col1 = match (l1.as_ref(), r1.as_ref()) {
                            (Expr::Column(c), Expr::Literal(_)) => Some(c),
                            (Expr::Literal(_), Expr::Column(c)) => Some(c),
                            _ => None,
                        };
                        
                        let col2 = match (l2.as_ref(), r2.as_ref()) {
                            (Expr::Column(c), Expr::Literal(_)) => Some(c),
                            (Expr::Literal(_), Expr::Column(c)) => Some(c),
                            _ => None,
                        };
                        
                        if let (Some(c1), Some(c2)) = (&col1, &col2) {
                            if c1 == c2 {
                                let col_name = (*c1).clone();

                                // Extract bounds with operators
                                let (val1, is_lower1, op1_normalized) = match (l1.as_ref(), op1, r1.as_ref()) {
                                    (Expr::Column(_), BinaryOperator::Ge, Expr::Literal(v)) => Some((v.clone(), true, BinaryOperator::Ge)),
                                    (Expr::Column(_), BinaryOperator::Gt, Expr::Literal(v)) => Some((v.clone(), true, BinaryOperator::Gt)),
                                    (Expr::Literal(v), BinaryOperator::Le, Expr::Column(_)) => Some((v.clone(), true, BinaryOperator::Ge)),
                                    (Expr::Literal(v), BinaryOperator::Lt, Expr::Column(_)) => Some((v.clone(), true, BinaryOperator::Gt)),
                                    (Expr::Column(_), BinaryOperator::Le, Expr::Literal(v)) => Some((v.clone(), false, BinaryOperator::Le)),
                                    (Expr::Column(_), BinaryOperator::Lt, Expr::Literal(v)) => Some((v.clone(), false, BinaryOperator::Lt)),
                                    (Expr::Literal(v), BinaryOperator::Ge, Expr::Column(_)) => Some((v.clone(), false, BinaryOperator::Le)),
                                    (Expr::Literal(v), BinaryOperator::Gt, Expr::Column(_)) => Some((v.clone(), false, BinaryOperator::Lt)),
                                    _ => None,
                                }?;

                                let (val2, is_lower2, op2_normalized) = match (l2.as_ref(), op2, r2.as_ref()) {
                                    (Expr::Column(_), BinaryOperator::Ge, Expr::Literal(v)) => Some((v.clone(), true, BinaryOperator::Ge)),
                                    (Expr::Column(_), BinaryOperator::Gt, Expr::Literal(v)) => Some((v.clone(), true, BinaryOperator::Gt)),
                                    (Expr::Literal(v), BinaryOperator::Le, Expr::Column(_)) => Some((v.clone(), true, BinaryOperator::Ge)),
                                    (Expr::Literal(v), BinaryOperator::Lt, Expr::Column(_)) => Some((v.clone(), true, BinaryOperator::Gt)),
                                    (Expr::Column(_), BinaryOperator::Le, Expr::Literal(v)) => Some((v.clone(), false, BinaryOperator::Le)),
                                    (Expr::Column(_), BinaryOperator::Lt, Expr::Literal(v)) => Some((v.clone(), false, BinaryOperator::Lt)),
                                    (Expr::Literal(v), BinaryOperator::Ge, Expr::Column(_)) => Some((v.clone(), false, BinaryOperator::Le)),
                                    (Expr::Literal(v), BinaryOperator::Gt, Expr::Column(_)) => Some((v.clone(), false, BinaryOperator::Lt)),
                                    _ => None,
                                }?;

                                // One should be lower bound, one should be upper bound
                                if is_lower1 && !is_lower2 {
                                    return Some((col_name, val1, op1_normalized, val2, op2_normalized));
                                } else if !is_lower1 && is_lower2 {
                                    return Some((col_name, val2, op2_normalized, val1, op1_normalized));
                                }
                            }
                        }
                    }
                }
                None
            }
            _ => None,
        }
    }
    
    /// 🎯 Try to extract a simple point query pattern: WHERE column = value
    /// 
    /// Returns Some((column_name, value)) if the WHERE clause is a simple equality,
    /// allowing us to skip complex expression evaluation.
    fn try_extract_point_query(&self, expr: &Expr) -> Option<(String, Value)> {
        use crate::sql::ast::{BinaryOperator, Expr};
        
        match expr {
            Expr::BinaryOp { left, op, right } => {
                // Only optimize simple equality: col = value
                if *op == BinaryOperator::Eq {
                    // Pattern 1: Column = Literal
                    if let (Expr::Column(col), Expr::Literal(val)) = (left.as_ref(), right.as_ref()) {
                        // 注意: 列名可能没有表前缀 (例如 "id"),但 SqlRow 中的键有前缀 ("users.id")
                        // 我们返回不带前缀的列名,在过滤时需要匹配任何表前缀
                        return Some((col.clone(), val.clone()));
                    }
                    // Pattern 2: Literal = Column (reversed)
                    if let (Expr::Literal(val), Expr::Column(col)) = (left.as_ref(), right.as_ref()) {
                        return Some((col.clone(), val.clone()));
                    }
                }
                None
            }
            _ => None,
        }
    }
    
    /// 🚀 Try to extract simple inequality: WHERE column < value or WHERE column > value
    /// 
    /// Returns Some((column_name, operator, value))
    fn try_extract_inequality(&self, expr: &Expr) -> Option<(String, BinaryOperator, Value)> {
        use crate::sql::ast::{BinaryOperator, Expr};
        
        match expr {
            Expr::BinaryOp { left, op, right } => {
                // Check for <, >, <=, >=
                match op {
                    BinaryOperator::Lt | BinaryOperator::Le | 
                    BinaryOperator::Gt | BinaryOperator::Ge => {
                        // Pattern 1: Column op Literal
                        if let (Expr::Column(col), Expr::Literal(val)) = (left.as_ref(), right.as_ref()) {
                            return Some((col.clone(), op.clone(), val.clone()));
                        }
                        // Pattern 2: Literal op Column (reversed, need to flip operator)
                        if let (Expr::Literal(val), Expr::Column(col)) = (left.as_ref(), right.as_ref()) {
                            let flipped_op = match op {
                                BinaryOperator::Lt => BinaryOperator::Gt,
                                BinaryOperator::Le => BinaryOperator::Ge,
                                BinaryOperator::Gt => BinaryOperator::Lt,
                                BinaryOperator::Ge => BinaryOperator::Le,
                                _ => return None,
                            };
                            return Some((col.clone(), flipped_op, val.clone()));
                        }
                    }
                    _ => {}
                }
                None
            }
            _ => None,
        }
    }
    
    /// 🎯 Try to extract vector search pattern: VECTOR_SEARCH(column, [...], k)
    /// Returns Some((table_name, column_name, query_vector, k))
    fn try_extract_vector_search(&self, expr: &Expr, from: &TableRef) -> Option<(String, String, Vec<f32>, usize)> {
        use crate::sql::ast::Expr;
        
        // Extract table name
        let table_name = match from {
            TableRef::Table { name, .. } => name.clone(),
            _ => return None,
        };
        
        // Match VECTOR_SEARCH function
        match expr {
            Expr::FunctionCall { name, args, .. } if name.to_uppercase() == "VECTOR_SEARCH" => {
                if args.len() != 3 {
                    return None;
                }
                
                // Extract column name
                let column = match &args[0] {
                    Expr::Column(col) => col.clone(),
                    _ => return None,
                };
                
                // Extract query vector (expecting a Vector value)
                let query_vector = match &args[1] {
                    Expr::Literal(Value::Vector(vec)) => vec.clone(),
                    _ => return None,
                };
                
                // Extract k
                let k = match &args[2] {
                    Expr::Literal(Value::Integer(k)) => *k as usize,
                    _ => return None,
                };
                
                Some((table_name, column, query_vector.to_vec(), k))
            }
            _ => None,
        }
    }

    /// 🚀 FAST PATH 0a: Text search (MATCH AGAINST) — single index lookup
    ///
    /// Detects WHERE MATCH(col) AGAINST('query') and uses the text index directly
    /// instead of scanning all rows and calling search_ranked() per row.
    fn try_text_search_fast_path(
        &self,
        stmt: &SelectStmt,
        where_clause: &Expr,
        table_name: &str,
    ) -> Result<Option<QueryResult>> {
        // Extract MATCH expression from WHERE clause
        let (column, query, phrase) = match where_clause {
            Expr::Match { column, query, phrase } => (column.clone(), query.clone(), *phrase),
            // Handle AND: MATCH(...) AND other_conditions — only if MATCH is the dominant filter
            Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
                // Try both sides for a MATCH expression
                if let Expr::Match { column, query, phrase } = left.as_ref() {
                    (column.clone(), query.clone(), *phrase)
                } else if let Expr::Match { column, query, phrase } = right.as_ref() {
                    (column.clone(), query.clone(), *phrase)
                } else {
                    return Ok(None);
                }
            }
            _ => return Ok(None),
        };

        // Find text index for this column
        let index_name = match self.db.index_registry.find_by_column(
            table_name, &column,
            crate::database::index_metadata::IndexType::Text
        ) {
            Some(name) => name,
            None => return Ok(None),
        };

        if !self.db.text_indexes.contains_key(&index_name) {
            return Ok(None);
        }

        // Determine limit (use LIMIT from query, or default to top 1000 for scoring)
        let limit = stmt.limit.unwrap_or(1000);

        // Phrase search or ranked search depending on query type
        let row_ids: Vec<u64> = if phrase {
            let ids = match self.db.text_search_phrase(&index_name, &query) {
                Ok(r) => r,
                Err(_) => return Ok(None),
            };
            ids.into_iter().take(limit).collect()
        } else {
            let results = match self.db.text_search_ranked(&index_name, &query, limit) {
                Ok(r) => r,
                Err(_) => return Ok(None),
            };
            results.into_iter().map(|(id, _score)| id).collect()
        };

        if row_ids.is_empty() {
            return Ok(Some(QueryResult::Select {
                columns: vec![],
                rows: vec![],
            }));
        }

        // Load rows for matching row_ids — use batch fetch for efficiency
        let schema = self.db.get_table_schema(table_name)?;
        let mut sql_rows = Vec::with_capacity(row_ids.len());

        let batch_rows = self.db.get_table_rows_batch(table_name, &row_ids)?;

        for (i, row_id) in row_ids.iter().enumerate() {
            if let Some(row) = batch_rows.get(i).and_then(|(_, opt)| opt.as_ref()) {
                let mut sql_row = row_to_sql_row(row, &schema)?;
                sql_row.insert("__row_id__".to_string(), Value::Integer(*row_id as i64));
                sql_row.insert("__table__".to_string(), Value::Text(table_name.to_string()));
                let score = 1.0f32;
                sql_row.insert(format!("__text_score_{}__", column), Value::Float(score as f64));
                let old_row = std::mem::take(&mut sql_row);
                let mut qualified = SqlRow::new();
                qualified.insert("__row_id__".to_string(), Value::Integer(*row_id as i64));
                qualified.insert("__table__".to_string(), Value::Text(table_name.to_string()));
                qualified.insert(format!("__text_score_{}__", column), Value::Float(score as f64));
                for (col_name, val) in old_row.into_iter() {
                    let qname = Self::make_qualified_name(table_name, &col_name);
                    qualified.insert(qname, val);
                }
                sql_rows.push((*row_id, qualified));
            }
        }

        // Handle ORDER BY score DESC — results are already sorted by BM25 score descending
        // Check if ORDER BY is compatible (DESC or absent)
        let order_compatible = stmt.order_by.as_ref().is_none_or(|ob| {
            // If ORDER BY exists, it must be DESC (which matches BM25 ranking)
            ob.len() == 1 && !ob[0].asc
        });

        if !order_compatible {
            // Non-compatible ORDER BY — fall back to normal path
            return Ok(None);
        }

        // Build scores from sql_rows metadata
        let scores: Vec<(RowId, f32)> = sql_rows.iter().map(|(id, row)| {
            let score = row.get(&format!("__text_score_{}__", column))
                .and_then(|v| if let Value::Float(f) = v { Some(*f as f32) } else { None })
                .unwrap_or(1.0);
            (*id, score)
        }).collect();

        let (column_names, result_rows) = self.project_text_search_columns(
            stmt, &sql_rows, &schema, &column, &scores
        )?;

        Ok(Some(QueryResult::Select {
            columns: column_names,
            rows: result_rows,
        }))
    }

    /// Project columns for text search fast path, handling MATCH score columns
    fn project_text_search_columns(
        &self,
        stmt: &SelectStmt,
        rows: &[(u64, SqlRow)],
        schema: &TableSchema,
        match_column: &str,
        scores: &[(RowId, f32)],
    ) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
        // Build score lookup
        let score_map: std::collections::HashMap<u64, f64> = scores.iter()
            .map(|(id, s)| (*id, *s as f64))
            .collect();

        let (column_names, result_rows) = if stmt.columns.len() == 1 && matches!(stmt.columns[0], SelectColumn::Star) {
            // SELECT *
            let col_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
            let projected: Vec<Vec<Value>> = rows.iter().map(|(_, row)| {
                schema.columns.iter()
                    .map(|col| row.get(&col.name).cloned().unwrap_or(Value::Null))
                    .collect()
            }).collect();
            (col_names, projected)
        } else {
            // Specific columns — handle MATCH expressions
            let col_names: Vec<String> = stmt.columns.iter().map(|col| match col {
                SelectColumn::Star => "*".to_string(),
                SelectColumn::Column(name) => name.clone(),
                SelectColumn::ColumnWithAlias(_, alias) => alias.clone(),
                SelectColumn::Expr(_, Some(alias)) => alias.clone(),
                SelectColumn::Expr(expr, None) => format!("{:?}", expr),
            }).collect();

            let projected: Vec<Vec<Value>> = rows.iter().map(|(row_id, row)| {
                stmt.columns.iter().map(|col| {
                    match col {
                        SelectColumn::Column(name) | SelectColumn::ColumnWithAlias(name, _) => {
                            row.get(name).cloned().or_else(|| {
                                if !name.contains('.') {
                                    row.iter()
                                        .find(|(k, _)| k.ends_with(&format!(".{}", name)))
                                        .map(|(_, v)| v.clone())
                                } else {
                                    None
                                }
                            }).unwrap_or(Value::Null)
                        }
                        SelectColumn::Expr(expr, _) => {
                            // Check if this is a MATCH expression for our column
                            if let Expr::Match { column, .. } = expr {
                                if column == match_column {
                                    return score_map.get(row_id)
                                        .map(|s| Value::Float(*s))
                                        .unwrap_or(Value::Float(0.0));
                                }
                            }
                            self.eval_with_materialized(expr, row).unwrap_or(Value::Null)
                        }
                        SelectColumn::Star => Value::Null,
                    }
                }).collect()
            }).collect();
            (col_names, projected)
        };

        Ok((column_names, result_rows))
    }

    /// 🚀 FAST PATH 0b: Spatial (ST_WITHIN / ST_KNN) — single index lookup
    ///
    /// Detects WHERE ST_WITHIN(col, ...) or WHERE ST_KNN(col, ...) and uses
    /// the spatial index directly instead of scanning all rows.
    fn try_spatial_fast_path(
        &self,
        stmt: &SelectStmt,
        where_clause: &Expr,
        table_name: &str,
    ) -> Result<Option<QueryResult>> {
        match where_clause {
            // 3D spatial fast paths (i-Octree)
            Expr::StWithin3D { column, min_x, min_y, min_z, max_x, max_y, max_z } => {
                self.execute_ioctree_within_fast(stmt, table_name, column, *min_x, *min_y, *min_z, *max_x, *max_y, *max_z)
            }
            Expr::StKnn3D { column, x, y, z, k } => {
                self.execute_ioctree_knn_fast(stmt, table_name, column, *x, *y, *z, *k)
            }
            Expr::StRadius3D { column, x, y, z, radius } => {
                self.execute_ioctree_radius_fast(stmt, table_name, column, *x, *y, *z, *radius)
            }
            _ => Ok(None),
        }
    }

    /// 🚀 FAST PATH -1b: ORDER BY ST_DISTANCE(col, x, y) LIMIT k
    /// Detects ORDER BY ST_DISTANCE and uses spatial KNN index instead of full scan.
    fn try_optimize_spatial_order_by(&self, stmt: &SelectStmt) -> Result<Option<QueryResult>> {
        let order_by = match &stmt.order_by {
            Some(o) if o.len() == 1 => &o[0],
            _ => return Ok(None),
        };
        let limit = match stmt.limit {
            Some(k) if k > 0 => k,
            _ => return Ok(None),
        };
        // Must be ASC for distance (closer first)
        if !order_by.asc {
            return Ok(None);
        }
        // WHERE must be absent or trivially true
        if stmt.where_clause.is_some() {
            return Ok(None);
        }

        // Match ORDER BY ST_DISTANCE_3D(column, x, y, z) or ORDER BY alias
        let dist_expr = match &order_by.expr {
            Expr::StDistance3D { column, x, y, z } => (column.clone(), *x, *y, *z),
            Expr::Column(alias) => {
                // Look up alias in SELECT columns to find the ST_DISTANCE_3D expression
                let mut found = None;
                for col in &stmt.columns {
                    match col {
                        SelectColumn::Expr(expr, Some(a)) if a == alias => {
                            if let Expr::StDistance3D { column, x, y, z } = expr {
                                found = Some((column.clone(), *x, *y, *z));
                            }
                            break;
                        }
                        SelectColumn::ColumnWithAlias(_, a) if a == alias => {
                            break;
                        }
                        _ => {}
                    }
                }
                match found {
                    Some(v) => v,
                    None => return Ok(None),
                }
            }
            _ => return Ok(None),
        };

        let table_name = match stmt.from.as_ref().unwrap() {
            TableRef::Table { name, .. } => name.clone(),
            _ => return Ok(None),
        };

        let (column, x, y, z) = dist_expr;
        let results: Vec<(RowId, f64)> = {
            let index_name = match self.db.index_registry.find_by_column(
                &table_name, &column,
                crate::database::index_metadata::IndexType::Octree
            ) {
                Some(name) => name,
                None => return Ok(None),
            };
            if !self.db.ioctree_indexes.contains_key(&index_name) {
                return Ok(None);
            }
            let point = crate::types::Point3D::new(x, y, z);
            match self.db.ioctree_knn_query(&index_name, &point, limit) {
                Ok(r) => r,
                Err(_) => return Ok(None),
            }
        };

        if results.is_empty() {
            return Ok(Some(QueryResult::Select { columns: vec![], rows: vec![] }));
        }

        // Load rows and project
        let schema = self.db.get_table_schema(&table_name)?;
        let dist_map: std::collections::HashMap<u64, f64> = results.iter().cloned().collect();
        let row_ids: Vec<RowId> = results.into_iter().map(|(id, _)| id).collect();

        let mut sql_rows = Vec::with_capacity(row_ids.len());
        for &row_id in &row_ids {
            if let Ok(Some(row)) = self.db.get_table_row(&table_name, row_id) {
                let mut sql_row = row_to_sql_row(&row, &schema)?;
                sql_row.insert("__row_id__".to_string(), Value::Integer(row_id as i64));
                sql_row.insert("__table__".to_string(), Value::Text(table_name.clone()));
                if let Some(d) = dist_map.get(&row_id) {
                    sql_row.insert("__spatial_distance__".to_string(), Value::Float(*d));
                }
                let old_row = std::mem::take(&mut sql_row);
                let mut qualified = SqlRow::new();
                qualified.insert("__row_id__".to_string(), Value::Integer(row_id as i64));
                qualified.insert("__table__".to_string(), Value::Text(table_name.clone()));
                if let Some(d) = dist_map.get(&row_id) {
                    qualified.insert("__spatial_distance__".to_string(), Value::Float(*d));
                }
                for (col_name, val) in old_row.into_iter() {
                    let qname = Self::make_qualified_name(&table_name, &col_name);
                    qualified.insert(qname, val);
                }
                sql_rows.push((row_id, qualified));
            }
        }

        let (column_names, result_rows) = self.project_columns(&stmt.columns, &sql_rows, &schema)?;
        Ok(Some(QueryResult::Select {
            columns: column_names,
            rows: result_rows,
        }))
    }

    /// Load rows by row_ids and project columns for spatial fast path
    fn load_and_project_spatial_rows(
        &self,
        stmt: &SelectStmt,
        table_name: &str,
        row_ids: &[RowId],
        dist_map: Option<&std::collections::HashMap<u64, f64>>,
        _is_within: bool,
    ) -> Result<Option<QueryResult>> {
        if row_ids.is_empty() {
            return Ok(Some(QueryResult::Select {
                columns: vec![],
                rows: vec![],
            }));
        }

        let schema = self.db.get_table_schema(table_name)?;
        let limit = stmt.limit.unwrap_or(row_ids.len());
        let row_ids_to_load = &row_ids[..row_ids.len().min(limit)];
        let columns = self.build_select_columns(&stmt.columns, &schema)?;

        let batch_rows = self.db.get_table_rows_batch(table_name, row_ids_to_load)?;

        let mut result_rows = Vec::with_capacity(batch_rows.len());
        for (row_id, row_opt) in batch_rows {
            if let Some(row) = row_opt {
                let mut projected = Self::project_row_direct(&row, &stmt.columns, &columns, &schema);
                if let Some(dm) = dist_map {
                    if let Some(d) = dm.get(&row_id) {
                        projected.push(Value::Float(*d));
                    }
                }
                result_rows.push(projected);
            }
        }

        let mut column_names = columns.clone();
        if dist_map.is_some() {
            column_names.push("distance".to_string());
        }

        Ok(Some(QueryResult::Select {
            columns: column_names,
            rows: result_rows,
        }))
    }

    // ==================== 3D Spatial Fast Paths (i-Octree) ====================

    /// Execute ST_WITHIN_3D using i-Octree index directly
    #[allow(clippy::too_many_arguments)]
    fn execute_ioctree_within_fast(
        &self,
        stmt: &SelectStmt,
        table_name: &str,
        column: &str,
        min_x: f64, min_y: f64, min_z: f64,
        max_x: f64, max_y: f64, max_z: f64,
    ) -> Result<Option<QueryResult>> {
        let index_name = match self.db.index_registry.find_by_column(
            table_name, column,
            crate::database::index_metadata::IndexType::Octree
        ) {
            Some(name) => name,
            None => return Ok(None),
        };

        if !self.db.ioctree_indexes.contains_key(&index_name) {
            return Ok(None);
        }

        let bbox = crate::types::BoundingBox3D::new(min_x, min_y, min_z, max_x, max_y, max_z);
        let row_ids = match self.db.ioctree_range_query(&index_name, &bbox) {
            Ok(ids) => ids,
            Err(_) => return Ok(None),
        };

        self.load_and_project_spatial_rows(stmt, table_name, &row_ids, None, true)
    }

    /// Execute ST_KNN_3D using i-Octree index directly
    #[allow(clippy::too_many_arguments)]
    fn execute_ioctree_knn_fast(
        &self,
        stmt: &SelectStmt,
        table_name: &str,
        column: &str,
        x: f64, y: f64, z: f64, k: usize,
    ) -> Result<Option<QueryResult>> {
        let index_name = match self.db.index_registry.find_by_column(
            table_name, column,
            crate::database::index_metadata::IndexType::Octree
        ) {
            Some(name) => name,
            None => return Ok(None),
        };

        if !self.db.ioctree_indexes.contains_key(&index_name) {
            return Ok(None);
        }

        let point = crate::types::Point3D::new(x, y, z);
        let results = match self.db.ioctree_knn_query(&index_name, &point, k) {
            Ok(r) => r,
            Err(_) => return Ok(None),
        };

        let row_ids: Vec<RowId> = results.iter().map(|(id, _)| *id).collect();
        let dist_map: std::collections::HashMap<u64, f64> = results.into_iter().collect();

        self.load_and_project_spatial_rows(stmt, table_name, &row_ids, Some(&dist_map), false)
    }

    /// Execute ST_RADIUS_3D using i-Octree index directly
    #[allow(clippy::too_many_arguments)]
    fn execute_ioctree_radius_fast(
        &self,
        stmt: &SelectStmt,
        table_name: &str,
        column: &str,
        x: f64, y: f64, z: f64, radius: f64,
    ) -> Result<Option<QueryResult>> {
        let index_name = match self.db.index_registry.find_by_column(
            table_name, column,
            crate::database::index_metadata::IndexType::Octree
        ) {
            Some(name) => name,
            None => return Ok(None),
        };

        if !self.db.ioctree_indexes.contains_key(&index_name) {
            return Ok(None);
        }

        let center = crate::types::Point3D::new(x, y, z);
        let results = match self.db.ioctree_radius_search(&index_name, &center, radius) {
            Ok(r) => r,
            Err(_) => return Ok(None),
        };

        let row_ids: Vec<RowId> = results.iter().map(|(id, _)| *id).collect();
        let dist_map: std::collections::HashMap<u64, f64> = results.into_iter().collect();

        self.load_and_project_spatial_rows(stmt, table_name, &row_ids, Some(&dist_map), false)
    }

    fn to_bool(&self, val: &Value) -> Result<bool> {
        match val {
            Value::Bool(b) => Ok(*b),
            Value::Integer(i) => Ok(*i != 0),
            Value::Float(f) => Ok(*f != 0.0 && !f.is_nan()),  // 🔧 Support Float: non-zero and non-NaN is true
            Value::Null => Ok(false),
            _ => Err(MoteDBError::TypeError("Cannot convert to boolean".to_string())),
        }
    }

    /// 🚀 PHASE A OPTIMIZATION: Compile simple comparison to fast closure
    /// 
    /// Converts simple patterns like:
    /// - col > 30 → |row| row.get("col") > 30
    /// - col = 'text' → |row| row.get("col") == "text"
    /// - age >= 18 AND age <= 65 → |row| row.get("age") >= 18 && row.get("age") <= 65
    /// 
    /// Returns None for complex expressions (falls back to interpreter)
    #[allow(clippy::only_used_in_recursion)]
    fn compile_simple_comparison(&self, expr: &Expr) -> RowPredicate {
        match expr {
            // Simple binary comparison: col op value
            Expr::BinaryOp { left, op, right } => {
                // Check if this is col op value pattern
                if let Expr::Column(col_name) = left.as_ref() {
                    if let Expr::Literal(value) = right.as_ref() {
                        let col = col_name.clone();
                        let val = value.clone();
                        
                        match op {
                            BinaryOperator::Gt => {
                                return Some(Box::new(move |row: &SqlRow| {
                                    Self::get_column_value_static(row, &col)
                                        .and_then(|v| Self::compare_values(v, &val))
                                        .map(|ord| ord == std::cmp::Ordering::Greater)
                                        .unwrap_or(false)
                                }));
                            }
                            BinaryOperator::Lt => {
                                return Some(Box::new(move |row: &SqlRow| {
                                    Self::get_column_value_static(row, &col)
                                        .and_then(|v| Self::compare_values(v, &val))
                                        .map(|ord| ord == std::cmp::Ordering::Less)
                                        .unwrap_or(false)
                                }));
                            }
                            BinaryOperator::Ge => {
                                return Some(Box::new(move |row: &SqlRow| {
                                    Self::get_column_value_static(row, &col)
                                        .and_then(|v| Self::compare_values(v, &val))
                                        .map(|ord| ord != std::cmp::Ordering::Less)
                                        .unwrap_or(false)
                                }));
                            }
                            BinaryOperator::Le => {
                                return Some(Box::new(move |row: &SqlRow| {
                                    Self::get_column_value_static(row, &col)
                                        .and_then(|v| Self::compare_values(v, &val))
                                        .map(|ord| ord != std::cmp::Ordering::Greater)
                                        .unwrap_or(false)
                                }));
                            }
                            BinaryOperator::Eq => {
                                return Some(Box::new(move |row: &SqlRow| {
                                    Self::get_column_value_static(row, &col)
                                        .map(|v| v == &val)
                                        .unwrap_or(false)
                                }));
                            }
                            BinaryOperator::Ne => {
                                return Some(Box::new(move |row: &SqlRow| {
                                    Self::get_column_value_static(row, &col)
                                        .map(|v| v != &val)
                                        .unwrap_or(false)
                                }));
                            }
                            _ => {}
                        }
                    }
                }
                
                // AND combination of two simple comparisons
                if *op == BinaryOperator::And {
                    if let (Some(left_fn), Some(right_fn)) = (
                        self.compile_simple_comparison(left),
                        self.compile_simple_comparison(right)
                    ) {
                        return Some(Box::new(move |row: &SqlRow| {
                            left_fn(row) && right_fn(row)
                        }));
                    }
                }
                
                None
            }
            _ => None,
        }
    }
    
    /// Helper: Get column value from row (handles table prefixes)
    fn get_column_value_static<'a>(row: &'a SqlRow, col_name: &str) -> Option<&'a Value> {
        // Try exact match first
        if let Some(val) = row.get(col_name) {
            return Some(val);
        }
        
        // Try with table prefix
        if !col_name.contains('.') {
            for (key, val) in row.iter() {
                if key.ends_with(&format!(".{}", col_name)) {
                    return Some(val);
                }
            }
        }
        
        None
    }
    
    /// Helper: Compare two values
    fn compare_values(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
        match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Some(a.cmp(b)),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Text(a), Value::Text(b)) => Some(a.cmp(b)),
            (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
            _ => None,
        }
    }
    
    // 🚀 P0 FIX: Primary Key Point Query optimization
    
    /// Try to optimize WHERE primary_key = value pattern
    /// 
    /// Detects patterns like:
    /// - `SELECT * FROM table WHERE id = 12345`
    /// - `SELECT col1, col2 FROM table WHERE id = 100`
    /// 
    /// Benefits:
    /// - 165x faster: 0.1ms vs 16.5ms (with 703 MemTable rows)
    /// - No MemTable scan: Direct LSM get by composite_key
    /// - No memory growth: Stable 2MB instead of 11MB spike
    /// - O(log n) complexity instead of O(n)
    fn try_optimize_primary_key_point_query(&self, stmt: &SelectStmt) -> Result<Option<QueryResult>> {
        // Must have WHERE clause
        let where_clause = match &stmt.where_clause {
            Some(w) => w,
            None => return Ok(None),
        };
        
        // Extract point query: column = value
        let (col_name, target_value) = match self.try_extract_point_query(where_clause) {
            Some(pair) => pair,
            None => return Ok(None),
        };
        
        // Get table name
        let table_name = match stmt.from.as_ref().unwrap() {
            TableRef::Table { name, .. } => name,
            _ => return Ok(None),
        };
        
        // Check if this column is the primary key
        let schema = self.db.get_table_schema(table_name)?;
        let is_primary_key = schema.primary_key()
            .map(|pk| pk == col_name)
            .unwrap_or(false);
        
        if !is_primary_key {
            return Ok(None);  // Not primary key, fallback to normal query
        }
        
        // 🚀 P3 CRITICAL OPTIMIZATION: AUTO_INCREMENT primary key
        // 
        // For AUTO_INCREMENT tables:
        // - Primary key value == row_id (always)
        // - No need for column index lookup
        // - Direct LSM get: O(log n) instead of O(2 * log n)
        // 
        // Performance improvement:
        // - Before: 20 ms (column index B-Tree + LSM get)
        // - After:  < 5 ms (direct LSM get only)
        // - Speedup: **4x faster** 🚀
        //
        if schema.is_primary_key_auto_increment() {
            // 🚀 Fast path: Primary key value IS row_id
            let row_id = match &target_value {
                Value::Integer(id) => {
                    if *id < 0 {
                        // Negative ID is invalid, return empty result
                        let (column_names, _) = self.project_columns(&stmt.columns, &[], &schema)?;
                        return Ok(Some(QueryResult::Select {
                            columns: column_names,
                            rows: vec![],
                        }));
                    }
                    *id as RowId
                }
                _ => {
                    // Primary key must be INTEGER, return empty result
                    let (column_names, _) = self.project_columns(&stmt.columns, &[], &schema)?;
                    return Ok(Some(QueryResult::Select {
                        columns: column_names,
                        rows: vec![],
                    }));
                }
            };
            
            // 🚀 Check row_cache first (microsecond-level hit, skips deserialize)
            if let Some(cached_row) = self.db.row_cache.get(table_name, row_id) {
                let is_select_star = stmt.columns.len() == 1
                    && matches!(stmt.columns[0], SelectColumn::Star);

                if is_select_star {
                    let column_names = (*schema.column_names_arc()).clone();
                    let result_row: Vec<Value> = schema.columns.iter()
                        .map(|col| cached_row.get(col.position).cloned().unwrap_or(Value::Null))
                        .collect();
                    return Ok(Some(QueryResult::Select {
                        columns: column_names,
                        rows: vec![result_row],
                    }));
                }

                let sql_row = row_to_sql_row(&cached_row, &schema)?;
                let mut prefixed_row = SqlRow::new();
                prefixed_row.insert("__row_id__".to_string(), Value::Integer(row_id as i64));
                prefixed_row.insert("__table__".to_string(), Value::Text(table_name.clone()));
                for (col_name, val) in sql_row {
                    let qualified_name = format!("{}.{}", table_name, col_name);
                    prefixed_row.insert(qualified_name, val);
                }
                let sql_rows = vec![(row_id, prefixed_row)];
                let (column_names, result_rows) = self.project_columns(&stmt.columns, &sql_rows, &schema)?;
                return Ok(Some(QueryResult::Select {
                    columns: column_names,
                    rows: result_rows,
                }));
            }

            // 🚀 Direct LSM get (skip column index completely!)
            let composite_key = self.db.make_composite_key(table_name, row_id);
            match self.db.lsm_engine.get(composite_key)? {
                Some(value_data) => {
                    // Check tombstone
                    if value_data.deleted {
                        let (column_names, _) = self.project_columns(&stmt.columns, &[], &schema)?;
                        return Ok(Some(QueryResult::Select {
                            columns: column_names,
                            rows: vec![],
                        }));
                    }
                    
                    // Deserialize row data
                    let data = match &value_data.data {
                        crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                        _ => return Err(StorageError::InvalidData("Unexpected blob".into())),
                    };
                    
                    let row = decode_row(data, &schema)
                        .map_err(|e| StorageError::InvalidData(format!("Deserialization failed: {}", e)))?;

                    // Populate row_cache for future hot-path lookups
                    self.db.row_cache.put(table_name.to_string(), row_id, row.clone());

                    // 🚀 Fast path for SELECT *: skip HashMap conversion entirely
                    //     Direct positional projection from Vec<Value> — saves 2*N HashMap
                    //     inserts + N format!() calls for prefix rewriting.
                    let is_select_star = stmt.columns.len() == 1
                        && matches!(stmt.columns[0], SelectColumn::Star);

                    if is_select_star {
                        let column_names = (*schema.column_names_arc()).clone();
                        let result_row: Vec<Value> = schema.columns.iter()
                            .map(|col| row.get(col.position).cloned().unwrap_or(Value::Null))
                            .collect();

                        return Ok(Some(QueryResult::Select {
                            columns: column_names,
                            rows: vec![result_row],
                        }));
                    }

                    // Slow path: column projection needs HashMap-based SqlRow
                    // Convert to SqlRow
                    let sql_row = row_to_sql_row(&row, &schema)?;
                    
                    // Add table prefix
                    let mut prefixed_row = SqlRow::new();
                    prefixed_row.insert("__row_id__".to_string(), Value::Integer(row_id as i64));
                    prefixed_row.insert("__table__".to_string(), Value::Text(table_name.clone()));
                    
                    for (col_name, val) in sql_row {
                        let qualified_name = format!("{}.{}", table_name, col_name);
                        prefixed_row.insert(qualified_name, val);
                    }
                    
                    let sql_rows = vec![(row_id, prefixed_row)];
                    
                    // Project columns
                    let (column_names, result_rows) = self.project_columns(&stmt.columns, &sql_rows, &schema)?;
                    
                    return Ok(Some(QueryResult::Select {
                        columns: column_names,
                        rows: result_rows,
                    }));
                }
                None => {
                    // Row not found, return empty result
                    let (column_names, _) = self.project_columns(&stmt.columns, &[], &schema)?;
                    return Ok(Some(QueryResult::Select {
                        columns: column_names,
                        rows: vec![],
                    }));
                }
            }
        }
        
        // 🔧 Non-AUTO_INCREMENT primary key: Use column index to lookup row_id
        // The primary key column has an auto-created index at table creation
        let row_ids = self.db.query_by_column(table_name, &col_name, &target_value)?;
        
        if row_ids.is_empty() {
            // Row not found, return empty result
            let (column_names, _) = self.project_columns(&stmt.columns, &[], &schema)?;
            return Ok(Some(QueryResult::Select {
                columns: column_names,
                rows: vec![],
            }));
        }
        
        // Primary key should be unique, take the first row_id
        let row_id = row_ids[0];
        
        // 🚀 P3++ 优化：直接使用 LSM get（跳过 get_table_row 的额外开销）
        // 
        // ## 性能提升
        // - 延迟：20.65 ms → **~10-15 ms**（**1.5-2x 提速** 🚀）
        // - 跳过 get_table_row 的额外逻辑
        // 
        let composite_key = self.db.make_composite_key(table_name, row_id);
        match self.db.lsm_engine.get(composite_key)? {
            Some(value_data) => {
                // 检查 tombstone
                if value_data.deleted {
                    let (column_names, _) = self.project_columns(&stmt.columns, &[], &schema)?;
                    return Ok(Some(QueryResult::Select {
                        columns: column_names,
                        rows: vec![],
                    }));
                }
                
                // 反序列化行数据
                let data = match &value_data.data {
                    crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                    _ => return Err(StorageError::InvalidData("Unexpected blob".into())),
                };
                
                let row = decode_row(data, &schema)
                    .map_err(|e| StorageError::InvalidData(format!("Deserialization failed: {}", e)))?;

                // 🚀 Fast path for SELECT *: skip HashMap conversion entirely
                let is_select_star = stmt.columns.len() == 1
                    && matches!(stmt.columns[0], SelectColumn::Star);

                if is_select_star {
                    let column_names: Vec<String> = schema.columns.iter()
                        .map(|c| c.name.clone())
                        .collect();
                    let result_row: Vec<Value> = schema.columns.iter()
                        .map(|col| row.get(col.position).cloned().unwrap_or(Value::Null))
                        .collect();

                    return Ok(Some(QueryResult::Select {
                        columns: column_names,
                        rows: vec![result_row],
                    }));
                }

                // 转换为 SqlRow
                let sql_row = row_to_sql_row(&row, &schema)?;
                
                // Add table prefix
                let mut prefixed_row = SqlRow::new();
                prefixed_row.insert("__row_id__".to_string(), Value::Integer(row_id as i64));
                prefixed_row.insert("__table__".to_string(), Value::Text(table_name.clone()));
                
                for (col_name, val) in sql_row {
                    let qualified_name = format!("{}.{}", table_name, col_name);
                    prefixed_row.insert(qualified_name, val);
                }
                
                let sql_rows = vec![(row_id, prefixed_row)];
                
                // Project columns
                let (column_names, result_rows) = self.project_columns(&stmt.columns, &sql_rows, &schema)?;
                
                Ok(Some(QueryResult::Select {
                    columns: column_names,
                    rows: result_rows,
                }))
            }
            None => {
                // Row not found, return empty result
                let (column_names, _) = self.project_columns(&stmt.columns, &[], &schema)?;
                Ok(Some(QueryResult::Select {
                    columns: column_names,
                    rows: vec![],
                }))
            }
        }
    }
    
    // 🚀 P0 FIX: Primary Key ORDER BY optimization
    
    /// Try to optimize ORDER BY primary_key [ASC/DESC] [LIMIT k]
    /// 
    /// Detects patterns like:
    /// - `SELECT * FROM table ORDER BY id LIMIT 10`
    /// - `SELECT * FROM table ORDER BY id DESC`
    /// 
    /// Benefits:
    /// - 600x faster: 1ms vs 611ms (300K rows)
    /// - 280x less memory: 0.1MB vs 28MB
    /// - O(k) complexity instead of O(n log n)
    fn try_optimize_primary_key_order_by(&self, stmt: &SelectStmt) -> Result<Option<QueryResult>> {
        // Must have ORDER BY with single column
        let order_by = match &stmt.order_by {
            Some(o) if o.len() == 1 => &o[0],
            _ => return Ok(None),
        };
        
        // ORDER BY must be a simple column reference
        let order_column = match &order_by.expr {
            Expr::Column(col) => col,
            _ => return Ok(None),
        };
        
        // Get table name
        let table_name = match stmt.from.as_ref().unwrap() {
            TableRef::Table { name, .. } => name,
            _ => return Ok(None),
        };
        
        // Check if this column is the primary key
        let schema = self.db.get_table_schema(table_name)?;
        let is_primary_key = schema.primary_key()
            .map(|pk| pk == order_column)
            .unwrap_or(false);
        
        if !is_primary_key {
            return Ok(None);
        }
        
        // Check that there's no WHERE clause (for now)
        if stmt.where_clause.is_some() {
            return Ok(None);
        }
        
        // Check that we're selecting all columns or simple column list
        let is_simple_select = matches!(&stmt.columns[..], [SelectColumn::Star]);
        if !is_simple_select {
            // Allow explicit column lists but not complex expressions
            let has_complex_expr = stmt.columns.iter().any(|col| {
                matches!(col, SelectColumn::Expr(_, _))
            });
            if has_complex_expr {
                return Ok(None);
            }
        }
        
        // Get primary key column index
        let pk_index_name = format!("{}.{}", table_name, order_column);

        // Check if index exists
        if !self.db.column_indexes.contains_key(&pk_index_name) {
            // No index, fallback to normal execution
            return Ok(None);
        }

        // Scan primary key index to get row_ids in order
        let index_arc = self.db.column_indexes
            .get(&pk_index_name)
            .ok_or_else(|| crate::StorageError::Index(format!("Primary key index not found: {}", pk_index_name)))?
            .clone();  // Clone Arc<RwLock<ColumnValueIndex>>
        
        // Calculate how many entries we need to scan
        let offset = stmt.offset.unwrap_or(0);
        let limit = stmt.limit.unwrap_or(usize::MAX);
        let scan_limit = if limit == usize::MAX {
            None  // No limit, scan all
        } else {
            Some(offset + limit)  // Scan enough to cover offset + limit
        };
        
        let row_ids = index_arc.read().scan_row_ids_with_limit(scan_limit)?;

        // If the column index is empty (async pipeline may not have built it yet),
        // fall back to full scan to avoid returning wrong empty results.
        if row_ids.is_empty() {
            return Ok(None);
        }

        // Apply sort order (ascending or descending)
        let sorted_row_ids = if order_by.asc {
            row_ids
        } else {
            let mut rev = row_ids;
            rev.reverse();
            rev
        };
        
        // Apply LIMIT and OFFSET
        let limit = stmt.limit.unwrap_or(usize::MAX);
        let offset = stmt.offset.unwrap_or(0);
        
        let limited_row_ids: Vec<_> = sorted_row_ids
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect();
        
        // Load rows
        let mut sql_rows = Vec::with_capacity(limited_row_ids.len());
        for row_id in limited_row_ids {
            if let Ok(Some(row)) = self.db.get_table_row(table_name, row_id) {
                let sql_row = row_to_sql_row(&row, &schema)?;
                sql_rows.push((row_id, sql_row));
            }
        }
        
        // Add table prefix
        for (row_id, sql_row) in &mut sql_rows {
            let mut new_sql_row = SqlRow::new();
            new_sql_row.insert("__row_id__".to_string(), Value::Integer(*row_id as i64));
            new_sql_row.insert("__table__".to_string(), Value::Text(table_name.clone()));
            
            let old_row = std::mem::take(sql_row);
            for (col_name, val) in old_row.into_iter() {
                let qualified_name = Self::make_qualified_name(table_name, &col_name);
                new_sql_row.insert(qualified_name, val);
            }
            *sql_row = new_sql_row;
        }
        
        // Project columns
        let (column_names, result_rows) = self.project_columns(&stmt.columns, &sql_rows, &schema)?;
        
        Ok(Some(QueryResult::Select {
            columns: column_names,
            rows: result_rows,
        }))
    }
    
    // 🚀 P0 FIX: Vector ORDER BY optimization helpers
    
    /// Try to optimize ORDER BY with vector distance
    fn try_optimize_vector_order_by(&self, stmt: &SelectStmt) -> Result<Option<VectorOrderByPlan>> {
        // 必须有 ORDER BY 和 LIMIT
        let order_by = match &stmt.order_by {
            Some(o) if o.len() == 1 => &o[0],
            _ => return Ok(None),
        };
        
        let limit = match stmt.limit {
            Some(k) if k > 0 => k,
            _ => return Ok(None),
        };
        
        // 解析 ORDER BY 表达式
        let (column, query_vector, asc) = match &order_by.expr {
            // 匹配: column <-> [vector] (L2Distance)
            Expr::BinaryOp { op: BinaryOperator::L2Distance | BinaryOperator::CosineDistance, left, right } => {
                match (&**left, &**right) {
                    (Expr::Column(col), Expr::Literal(Value::Vector(vec))) => {
                        (col.clone(), vec.clone(), order_by.asc)
                    }
                    _ => return Ok(None),
                }
            }
            _ => return Ok(None),
        };
        
        // 向量距离必须是升序
        if !asc {
            return Ok(None);
        }
        
        // 获取表名
        let table_name = match stmt.from.as_ref().unwrap() {
            TableRef::Table { name, .. } => name.clone(),
            _ => return Ok(None),
        };
        
        // 检查索引
        let index_name = format!("{}_{}", table_name, column);
        if !self.db.has_vector_index(&index_name) {
            return Ok(None);
        }
        
        Ok(Some(VectorOrderByPlan {
            table: table_name,
            column,
            query_vector: query_vector.to_vec(),
            k: limit,
        }))
    }
    
    /// Execute SELECT using vector ORDER BY optimization
    fn execute_vector_order_by_plan(&self, stmt: &SelectStmt, plan: &VectorOrderByPlan) -> Result<QueryResult> {
        debug_log!("[Executor] ✅ 使用向量索引优化 ORDER BY: {} <-> [...] LIMIT {}", plan.column, plan.k);

        // Resolve index name via registry (supports custom index names)
        let index_name = self.db.index_registry.find_by_column(
            &plan.table, &plan.column,
            crate::database::index_metadata::IndexType::Vector
        ).unwrap_or_else(|| format!("{}_{}", plan.table, plan.column));
        
        // 1. 向量搜索获取 Top-K row_ids
        let candidates = self.db.vector_search(&index_name, &plan.query_vector, plan.k)?;
        debug_log!("[Executor] 🔍 vector_search返回了{}个候选", candidates.len());
        
        let row_ids: Vec<u64> = candidates.iter().map(|(id, _dist)| *id).collect();
        
        if !row_ids.is_empty() {
            debug_log!("[Executor] 🔍 row_ids前5个: {:?}", &row_ids[..5.min(row_ids.len())]);
        }
        
        if row_ids.is_empty() {
            // 返回空结果
            let schema = self.db.get_table_schema(&plan.table)?;
            return Ok(QueryResult::Select {
                columns: schema.columns.iter().map(|c| c.name.clone()).collect(),
                rows: vec![],
            });
        }
        
        // 2. 批量获取行数据
        let schema = self.db.get_table_schema(&plan.table)?;
        let batch_rows = self.db.get_table_rows_batch(&plan.table, &row_ids)?;
        
        debug_log!("[Executor] 🔍 get_table_rows_batch返回了{}个行", batch_rows.len());
        
        // 3. 转换为SQL行格式（保持向量搜索的顺序）
        let mut sql_rows = Vec::with_capacity(row_ids.len());
        for (row_id, row_opt) in batch_rows {
            if let Some(row) = row_opt {
                let sql_row = row_to_sql_row(&row, &schema)?;
                
                // 🔍 Debug: 打印前3个的row_id和id列
                if sql_rows.len() < 3 {
                    if let Some(_id_value) = sql_row.get("id") {
                        debug_log!("[Executor] 🔍 row_id={} → id列={:?}", row_id, _id_value);
                    }
                }
                
                sql_rows.push((row_id, sql_row));
            }
        }
        
        // 4. 应用WHERE条件（如果有）
        let filtered_rows: Vec<(u64, SqlRow)> = if let Some(ref where_clause) = stmt.where_clause {
            sql_rows.into_iter()
                .filter(|(_, row)| {
                    self.evaluator.eval(where_clause, row)
                        .and_then(|val| self.to_bool(&val))
                        .unwrap_or(false)
                })
                .collect()
        } else {
            sql_rows
        };
        
        // 5. 简单列投影（避免递归调用 project_columns）
        let column_names: Vec<String> = if stmt.columns.len() == 1 && matches!(stmt.columns[0], SelectColumn::Star) {
            // SELECT *
            schema.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            stmt.columns.iter().map(|col| match col {
                SelectColumn::Star => "*".to_string(),
                SelectColumn::Column(name) | SelectColumn::ColumnWithAlias(name, _) => name.clone(),
                SelectColumn::Expr(_, Some(alias)) => alias.clone(),
                SelectColumn::Expr(expr, None) => format!("{:?}", expr),
            }).collect()
        };
        
        let projected_rows: Vec<Vec<Value>> = filtered_rows.iter().map(|(_, row)| {
            if stmt.columns.len() == 1 && matches!(stmt.columns[0], SelectColumn::Star) {
                // SELECT * - return all columns in schema order
                schema.columns.iter()
                    .map(|col| row.get(&col.name).cloned().unwrap_or(Value::Null))
                    .collect()
            } else {
                stmt.columns.iter().map(|col| {
                    match col {
                        SelectColumn::Column(name) | SelectColumn::ColumnWithAlias(name, _) => {
                            row.get(name).cloned().unwrap_or(Value::Null)
                        }
                        SelectColumn::Expr(expr, _) => {
                            // ⚠️ 只对简单表达式求值，避免递归
                            self.evaluator.eval(expr, row).unwrap_or(Value::Null)
                        }
                        SelectColumn::Star => Value::Null,
                    }
                }).collect()
            }
        }).collect();
        
        // 6. 应用 OFFSET（如果有）
        let offset = stmt.offset.unwrap_or(0);
        let final_rows: Vec<Vec<Value>> = projected_rows.into_iter()
            .skip(offset)
            .take(plan.k)
            .collect();
        
        Ok(QueryResult::Select {
            columns: column_names,
            rows: final_rows,
        })
    }

    // ==================== Columnar Store Routing ====================

    /// Try to serve a SELECT from the columnar store for TimeSeries tables.
    /// Returns Ok(Some(result)) if handled, Ok(None) if it should fall through to LSM.
    fn try_columnar_select(
        &self,
        stmt: &SelectStmt,
        schema: &TableSchema,
    ) -> Result<Option<QueryResult>> {
        // Only handle simple FROM table (no JOINs, subqueries)
        let table_name = match stmt.from.as_ref() {
            Some(TableRef::Table { name, .. }) => name.clone(),
            _ => return Ok(None),
        };

        // Extract time range from WHERE clause
        let ts_col = match &schema.timeseries_column {
            Some(col) => col.clone(),
            None => return Ok(None),
        };

        let (start_ts, end_ts) = match self.extract_time_range(&stmt.where_clause, &ts_col) {
            Some(range) => range,
            None => return Ok(None), // Can't determine time range → fall through
        };

        // Don't handle aggregates or GROUP BY via columnar fast path;
        // let the standard executor handle them (data is also in LSM via WAL replay).
        if stmt.group_by.is_some() || self.has_aggregates(&stmt.columns) {
            return Ok(None);
        }

        // Extract requested column names
        let column_names: Vec<String> = stmt.columns.iter().map(|col| {
            match col {
                SelectColumn::Star => "*".to_string(),
                SelectColumn::Column(name) | SelectColumn::ColumnWithAlias(name, _) => name.clone(),
                SelectColumn::Expr(_, alias) => alias.clone().unwrap_or_default(),
            }
        }).collect();

        // If star, pass empty vec (means all columns)
        let query_cols: Vec<String> = if column_names.iter().any(|c| c == "*") {
            vec![]
        } else {
            column_names.clone()
        };

        // Extract non-timestamp column conditions for pruning
        let conditions = self.extract_column_conditions(&stmt.where_clause, schema, &ts_col);

        let results = if conditions.is_empty() {
            self.db.columnar_store.query_time_range(
                &table_name,
                start_ts,
                end_ts,
                &query_cols,
            )?
        } else {
            self.db.columnar_store.query_with_conditions(
                &table_name,
                start_ts,
                end_ts,
                &conditions,
                &query_cols,
            )?
        };

        // Build result rows
        let output_columns: Vec<String> = if query_cols.is_empty() {
            schema.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            column_names
        };

        let mut rows = Vec::new();
        for (_row_id, sql_row) in &results {
            let mut row = Vec::new();
            for col_name in &output_columns {
                row.push(sql_row.get(col_name).cloned().unwrap_or(Value::Null));
            }
            rows.push(row);
        }

        // P1: Handle ORDER BY for columnar results
        if let Some(ref order_by) = stmt.order_by {
            for order_item in order_by.iter().rev() {
                let col_name = match &order_item.expr {
                    Expr::Column(name) => name.clone(),
                    _ => continue,
                };
                let col_idx = output_columns.iter().position(|c| *c == col_name);
                if let Some(idx) = col_idx {
                    let ascending = order_item.asc;
                    rows.sort_by(|a, b| {
                        let va = a.get(idx).unwrap_or(&Value::Null);
                        let vb = b.get(idx).unwrap_or(&Value::Null);
                        let cmp = match (va, vb) {
                            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
                            (Value::Null, _) => std::cmp::Ordering::Less,
                            (_, Value::Null) => std::cmp::Ordering::Greater,
                            _ => va.partial_cmp(vb).unwrap_or(std::cmp::Ordering::Equal),
                        };
                        if ascending { cmp } else { cmp.reverse() }
                    });
                }
            }
        }

        // P1: Handle OFFSET and LIMIT
        let offset = stmt.offset.unwrap_or(0);
        if offset > 0 {
            let _ = rows.drain(..offset.min(rows.len()));
        }
        if let Some(limit) = stmt.limit {
            rows.truncate(limit);
        }

        Ok(Some(QueryResult::Select {
            columns: output_columns,
            rows,
        }))
    }

    /// Extract time range from WHERE clause.
    /// Looks for patterns: ts BETWEEN a AND b, ts >= a AND ts <= b, ts > a, ts < b
    /// Also handles reverse comparisons: a >= ts → ts <= a, etc.
    fn extract_time_range(&self, where_clause: &Option<Expr>, ts_col: &str) -> Option<(i64, i64)> {
        let expr = where_clause.as_ref()?;

        match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::And => {
                        let left_range = self.extract_time_range(&Some(*left.clone()), ts_col)?;
                        let right_range = self.extract_time_range(&Some(*right.clone()), ts_col)?;
                        let start = left_range.0.max(right_range.0);
                        let end = left_range.1.min(right_range.1);
                        Some((start, end))
                    }
                    BinaryOperator::Ge => {
                        // ts >= val OR val >= ts (reverse: ts <= val)
                        if let Expr::Column(col) = left.as_ref() {
                            if col == ts_col {
                                let val = self.eval_literal_to_i64(right)?;
                                return Some((val, i64::MAX));
                            }
                        }
                        // Reverse: literal >= ts → ts <= literal
                        if let Expr::Column(col) = right.as_ref() {
                            if col == ts_col {
                                let val = self.eval_literal_to_i64(left)?;
                                return Some((i64::MIN, val));
                            }
                        }
                        None
                    }
                    BinaryOperator::Gt => {
                        if let Expr::Column(col) = left.as_ref() {
                            if col == ts_col {
                                let val = self.eval_literal_to_i64(right)?;
                                return Some((val + 1, i64::MAX));
                            }
                        }
                        // Reverse: literal > ts → ts < literal
                        if let Expr::Column(col) = right.as_ref() {
                            if col == ts_col {
                                let val = self.eval_literal_to_i64(left)?;
                                return Some((i64::MIN, val - 1));
                            }
                        }
                        None
                    }
                    BinaryOperator::Le => {
                        if let Expr::Column(col) = left.as_ref() {
                            if col == ts_col {
                                let val = self.eval_literal_to_i64(right)?;
                                return Some((i64::MIN, val));
                            }
                        }
                        // Reverse: literal <= ts → ts >= literal
                        if let Expr::Column(col) = right.as_ref() {
                            if col == ts_col {
                                let val = self.eval_literal_to_i64(left)?;
                                return Some((val, i64::MAX));
                            }
                        }
                        None
                    }
                    BinaryOperator::Lt => {
                        if let Expr::Column(col) = left.as_ref() {
                            if col == ts_col {
                                let val = self.eval_literal_to_i64(right)?;
                                return Some((i64::MIN, val - 1));
                            }
                        }
                        // Reverse: literal < ts → ts > literal
                        if let Expr::Column(col) = right.as_ref() {
                            if col == ts_col {
                                let val = self.eval_literal_to_i64(left)?;
                                return Some((val + 1, i64::MAX));
                            }
                        }
                        None
                    }
                    BinaryOperator::Eq => {
                        if let Expr::Column(col) = left.as_ref() {
                            if col == ts_col {
                                let val = self.eval_literal_to_i64(right)?;
                                return Some((val, val));
                            }
                        }
                        // Reverse: literal = ts
                        if let Expr::Column(col) = right.as_ref() {
                            if col == ts_col {
                                let val = self.eval_literal_to_i64(left)?;
                                return Some((val, val));
                            }
                        }
                        None
                    }
                    _ => None,
                }
            }
            Expr::Between { expr: col, low, high, negated: _ } => {
                if let Expr::Column(name) = col.as_ref() {
                    if name == ts_col {
                        let start = self.eval_literal_to_i64(low)?;
                        let end = self.eval_literal_to_i64(high)?;
                        return Some((start, end));
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// Extract non-timestamp column conditions from WHERE clause for columnar pruning.
    /// Returns conditions that can be pushed down to segment-level zone maps and bloom filters.
    fn extract_column_conditions(
        &self,
        where_clause: &Option<Expr>,
        schema: &TableSchema,
        ts_col: &str,
    ) -> Vec<crate::storage::columnar::segment_manager::ColumnCondition> {
        let expr = match where_clause {
            Some(e) => e,
            None => return Vec::new(),
        };

        let mut conditions = Vec::new();
        self.collect_conditions_recursive(expr, schema, ts_col, &mut conditions);
        conditions
    }

    fn collect_conditions_recursive(
        &self,
        expr: &Expr,
        schema: &TableSchema,
        ts_col: &str,
        conditions: &mut Vec<crate::storage::columnar::segment_manager::ColumnCondition>,
    ) {
        if let Expr::BinaryOp { left, op, right } = expr {
            match op {
                BinaryOperator::And => {
                    // Recurse into both sides of AND
                    self.collect_conditions_recursive(left, schema, ts_col, conditions);
                    self.collect_conditions_recursive(right, schema, ts_col, conditions);
                }
                BinaryOperator::Eq => {
                    // col = value OR value = col (non-ts column)
                    if let Some(cond) = self.try_extract_equality(left, right, schema, ts_col) {
                        conditions.push(cond);
                    } else if let Some(cond) = self.try_extract_equality(right, left, schema, ts_col) {
                        conditions.push(cond);
                    }
                }
                BinaryOperator::Ge | BinaryOperator::Gt | BinaryOperator::Le | BinaryOperator::Lt => {
                    // Try to extract range conditions
                    if let Some(cond) = self.try_extract_range(left, right, op, schema, ts_col) {
                        conditions.push(cond);
                    }
                }
                _ => {}
            }
        }
    }

    /// Try to extract an Equals condition from `col_expr = value_expr`.
    fn try_extract_equality(
        &self,
        col_expr: &Expr,
        value_expr: &Expr,
        schema: &TableSchema,
        ts_col: &str,
    ) -> Option<crate::storage::columnar::segment_manager::ColumnCondition> {
        use crate::storage::columnar::segment_manager::ColumnCondition;

        if let Expr::Column(col_name) = col_expr {
            if col_name == ts_col {
                return None; // Skip timestamp column
            }
            let col_idx = schema.columns.iter().position(|c| c.name == *col_name)?;
            let value = match value_expr {
                Expr::Literal(v) => v.clone(),
                _ => return None,
            };
            Some(ColumnCondition::Equals { column_idx: col_idx, value })
        } else {
            None
        }
    }

    /// Try to extract a Range condition from comparison ops.
    fn try_extract_range(
        &self,
        left: &Expr,
        right: &Expr,
        op: &BinaryOperator,
        schema: &TableSchema,
        ts_col: &str,
    ) -> Option<crate::storage::columnar::segment_manager::ColumnCondition> {
        use crate::storage::columnar::segment_manager::ColumnCondition;

        // Determine which side is the column and which is the value
        let (col_name, value, is_col_left) = match (left, right) {
            (Expr::Column(c), Expr::Literal(v)) => (c, v, true),
            (Expr::Literal(v), Expr::Column(c)) => (c, v, false),
            _ => return None,
        };

        if col_name == ts_col {
            return None;
        }

        let col_idx = schema.columns.iter().position(|c| c.name == *col_name)?;

        // Convert comparison to a range [low, high]
        let (low, high) = match (op, is_col_left) {
            (BinaryOperator::Ge, true) => (value.clone(), Value::Integer(i64::MAX)),  // col >= val
            (BinaryOperator::Gt, true) => {
                // col > val → [val+1, MAX]
                let bumped = self.increment_value(value)?;
                (bumped, Value::Integer(i64::MAX))
            }
            (BinaryOperator::Le, true) => (Value::Integer(i64::MIN), value.clone()),  // col <= val
            (BinaryOperator::Lt, true) => {
                let decremented = self.decrement_value(value)?;
                (Value::Integer(i64::MIN), decremented)
            }
            (BinaryOperator::Ge, false) => (Value::Integer(i64::MIN), value.clone()), // val >= col → col <= val
            (BinaryOperator::Gt, false) => {
                let decremented = self.decrement_value(value)?;
                (Value::Integer(i64::MIN), decremented)
            }
            (BinaryOperator::Le, false) => (value.clone(), Value::Integer(i64::MAX)), // val <= col → col >= val
            (BinaryOperator::Lt, false) => {
                let bumped = self.increment_value(value)?;
                (bumped, Value::Integer(i64::MAX))
            }
            _ => return None,
        };

        Some(ColumnCondition::Range { column_idx: col_idx, low, high })
    }

    fn increment_value(&self, v: &Value) -> Option<Value> {
        match v {
            Value::Integer(i) => Some(Value::Integer(i + 1)),
            Value::Float(f) => Some(Value::Float(f + 1.0)),
            _ => None,
        }
    }

    fn decrement_value(&self, v: &Value) -> Option<Value> {
        match v {
            Value::Integer(i) => Some(Value::Integer(i - 1)),
            Value::Float(f) => Some(Value::Float(f - 1.0)),
            _ => None,
        }
    }

    /// Evaluate a literal expression to i64 (for time range extraction).
    fn eval_literal_to_i64(&self, expr: &Expr) -> Option<i64> {
        match expr {
            Expr::Literal(Value::Timestamp(ts)) => Some(ts.as_micros()),
            Expr::Literal(Value::Integer(i)) => Some(*i),
            Expr::Literal(Value::Float(f)) => Some(*f as i64),
            _ => None,
        }
    }

    /// Execute INSERT for TimeSeries tables via the columnar store.
    fn execute_columnar_insert(
        &self,
        stmt: &InsertStmt,
        schema: &crate::types::TableSchema,
        columns: &[String],
    ) -> Result<QueryResult> {
        let mut rows: Vec<Vec<crate::types::Value>> = Vec::new();

        for value_row in &stmt.values {
            if value_row.len() != columns.len() {
                return Err(MoteDBError::InvalidArgument(
                    format!("Column count mismatch: expected {}, got {}", columns.len(), value_row.len())
                ));
            }

            // Build SqlRow first (reuses existing type coercion via sql_row_to_row)
            let mut sql_row = SqlRow::new();
            for (i, col_name) in columns.iter().enumerate() {
                let val = match &value_row[i] {
                    Expr::Literal(v) => v.clone(),
                    Expr::Parameter(_) => {
                        let empty_row = SqlRow::new();
                        self.evaluator.eval(&value_row[i], &empty_row)?
                    }
                    expr => return Err(MoteDBError::InvalidArgument(
                        format!("INSERT VALUES must be literals or parameters, got {:?}", expr)
                    )),
                };
                sql_row.insert(col_name.clone(), val);
            }

            // Convert to storage Row (handles type coercion)
            let row = sql_row_to_row(&sql_row, schema)?;
            rows.push(row);
        }

        let result = self.db.columnar_store.ingest(&stmt.table, rows)?;
        Ok(QueryResult::Modification {
            affected_rows: result.row_ids.len(),
        })
    }
}

/// Helper struct for vector ORDER BY plan
struct VectorOrderByPlan {
    table: String,
    column: String,
    query_vector: Vec<f32>,
    k: usize,
}
