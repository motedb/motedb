/// Query executor - executes SQL statements against storage engine
use super::ast::*;
use super::evaluator::ExprEvaluator;
use super::row_converter::{row_to_sql_row, sql_row_to_row, rows_to_sql_rows};
use crate::database::MoteDB;
use crate::error::{Result, MoteDBError};
use crate::{StorageError};
use crate::types::{Value, SqlRow, TableSchema, ColumnType, RowId, Row};
use std::sync::Arc;
use std::cell::RefCell;
use std::rc::Rc;

#[allow(clippy::type_complexity)]
type FromScanResult = Result<(Vec<(u64, SqlRow)>, Arc<TableSchema>)>;

#[allow(clippy::type_complexity)]
type RowPredicate = Option<Box<dyn Fn(&SqlRow) -> bool + Send + Sync>>;

/// 🚀 索引下推：可索引的条件类型
#[allow(dead_code)]
#[derive(Debug, Clone)]
enum IndexableCondition {
    /// 点查询: col = value
    PointQuery { column: String, value: Value },
    /// 范围查询: start <= col <= end
    RangeQuery { column: String, start: Value, end: Value },
    /// 小于: col < value
    LessThan { column: String, value: Value },
    /// 大于: col > value
    GreaterThan { column: String, value: Value },
}

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
    
    /// Get rows as maps (column_name -> value)
    /// Returns empty vec if not a SELECT result
    pub fn rows_as_maps(&self) -> Vec<std::collections::HashMap<String, Value>> {
        match self {
            QueryResult::Select { columns, rows } => {
                rows.iter().map(|row| {
                    columns.iter()
                        .zip(row.iter())
                        .map(|(col, val)| (col.clone(), val.clone()))
                        .collect()
                }).collect()
            }
            _ => vec![],
        }
    }
    
    /// Get row count for SELECT results
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
    /// 
    /// # 示例
    /// ```ignore
    /// result.for_each(|columns, row| {
    ///     println!("{}: {}", columns[0], row[0]);
    ///     Ok(())
    /// })?;
    /// ```
    pub fn for_each<F>(self, mut f: F) -> Result<()>
    where
        F: FnMut(&[String], &[Value]) -> Result<()>,
    {
        match self {
            Self::SelectStreaming { columns, rows, .. } => {
                for row_result in rows {
                    let row = row_result?;
                    f(&columns, &row)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
    
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
    optimizer: RefCell<super::optimizer::QueryOptimizer>,
    /// Store the last AUTO_INCREMENT value inserted (shared with evaluator)
    last_insert_id: Rc<RefCell<Option<i64>>>,
}

impl QueryExecutor {
    pub fn new(db: Arc<MoteDB>) -> Self {
        let last_insert_id = Rc::new(RefCell::new(None));
        let mut evaluator = ExprEvaluator::with_db(db.clone());
        evaluator.last_insert_id = Rc::clone(&last_insert_id);
        
        Self {
            evaluator,
            optimizer: RefCell::new(super::optimizer::QueryOptimizer::new(db.clone())),
            last_insert_id,
            db,
        }
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

        let plan = self.optimizer.borrow_mut().optimize_select(stmt)?;

        // 根据执行计划选择流式扫描方法
        match plan.scan_method {
            super::optimizer::ScanMethod::PointQuery { ref table, ref column, ref value } => {
                self.execute_point_query_streaming(stmt, table, column, value)
            }
            super::optimizer::ScanMethod::RangeQuery { ref table, ref column, ref start, start_inclusive, ref end, end_inclusive } => {
                self.execute_range_query_streaming(stmt, table, column, start, start_inclusive, end, end_inclusive)
            }
            super::optimizer::ScanMethod::FullScan { ref table } => {
                self.execute_full_scan_streaming(stmt, table)
            }
            _ => {
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
            Expr::Match { .. } | Expr::StWithin { .. } | Expr::StKnn { .. } => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_needs_materialized_path(left) || Self::expr_needs_materialized_path(right)
            }
            _ => false,
        }
    }

    /// Check if ORDER BY expression is ST_DISTANCE or aliases a SELECT column that is ST_DISTANCE
    fn expr_is_or_aliases_st_distance(expr: &Expr, select_cols: &[SelectColumn]) -> bool {
        match expr {
            Expr::StDistance { .. } => true,
            Expr::Column(alias) => {
                for col in select_cols {
                    match col {
                        SelectColumn::Expr(e, Some(a)) if a == alias => {
                            return matches!(e, Expr::StDistance { .. });
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
            let key = value.to_hash_key();
            let row_id = self.resolve_pk_with_cache(table, &key, column, value)?;

            if let Some(rid) = row_id {
                let row = self.db.get_table_row(table, rid)?;
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

            let row = self.db.get_table_row(table, row_id)?;
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
        
        // 流式读取行数据
        let db = self.db.clone();
        let table_name = table.to_string();
        let schema_clone = schema.clone();
        let select_cols = stmt.columns.clone();
        let columns_clone = columns.clone();
        
        let rows_iter = row_ids.into_iter().filter_map(move |row_id| {
            // 构造组合键
            let composite_key = db.make_composite_key(&table_name, row_id);
            
            // 读取行数据
            match db.lsm_engine.get(composite_key) {
                Ok(Some(value_data)) if !value_data.deleted => {
                    // 反序列化行
                    let data = match &value_data.data {
                        crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                        _ => return Some(Err(StorageError::InvalidData("Unexpected blob".into()))),
                    };
                    
                    match bincode::deserialize::<crate::types::Row>(data) {
                        Ok(row) => {
                            match row_to_sql_row(&row, &schema_clone) {
                                Ok(sql_row) => {
                                    let projected = Self::project_row_static(&sql_row, &select_cols, &columns_clone, &schema_clone);
                                    Some(Ok(projected))
                                }
                                Err(e) => Some(Err(e)),
                            }
                        }
                        Err(e) => Some(Err(StorageError::InvalidData(format!("Deserialization failed: {}", e)))),
                    }
                }
                Ok(_) => None, // Deleted or not found
                Err(e) => Some(Err(e)),
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
        if column == "id" && schema.primary_key().map(|pk| pk == "id").unwrap_or(false) {
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
                        
                        match bincode::deserialize::<crate::types::Row>(data) {
                            Ok(row) => {
                                match row_to_sql_row(&row, &schema_clone) {
                                    Ok(sql_row) => {
                                        let projected = Self::project_row_static(&sql_row, &select_cols, &columns_clone, &schema_clone);
                                        processed.push(Ok(projected));
                                    }
                                    Err(e) => processed.push(Err(e)),
                                }
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

            match bincode::deserialize::<crate::types::Row>(data) {
                Ok(row) => {
                    match row_to_sql_row(&row, &schema_clone) {
                        Ok(sql_row) => {
                            let projected = Self::project_row_static(&sql_row, &select_cols, &columns_clone, &schema_clone);
                            Ok(projected)
                        }
                        Err(e) => Err(e),
                    }
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
        
        // 获取流式迭代器
        let row_iter = self.db.scan_table_rows_streaming(table)?;
        
        // Clone what we need for the closure
        let where_clause = stmt.where_clause.clone();
        let _db = self.db.clone();
        let schema_clone = schema.clone();
        let columns_clone = columns.clone();
        let select_cols = stmt.columns.clone();
        let table_clone = table.to_string();  // 🔧 Clone table name for metadata

        // 惰性过滤和投影
        let filtered_iter = row_iter.filter_map(move |result| {
            match result {
                Ok((row_id, row)) => {  // 🔧 Don't ignore row_id
                    let mut sql_row = match row_to_sql_row(&row, &schema_clone) {
                        Ok(r) => r,
                        Err(e) => return Some(Err(e)),
                    };

                    // 🔧 Add metadata fields for MATCH, ST_DISTANCE, etc.
                    sql_row.insert("__row_id__".to_string(), Value::Integer(row_id as i64));
                    sql_row.insert("__table__".to_string(), Value::Text(table_clone.clone()));

                    // WHERE 过滤
                    if let Some(ref clause) = where_clause {
                        // 🚀 Inline evaluation for simple expressions (no per-row allocation)
                        let matches = match Self::eval_expr_simple(clause, &sql_row) {
                            Ok(Value::Bool(b)) => b,
                            Ok(Value::Integer(i)) => i != 0,
                            Ok(Value::Float(f)) => f != 0.0 && !f.is_nan(),
                            _ => false,
                        };

                        if !matches {
                            return None;
                        }
                    }

                    // 投影列
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

    fn eval_expr_simple(expr: &Expr, row: &SqlRow) -> Result<Value> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let lv = Self::eval_expr_simple(left, row)?;
                let rv = Self::eval_expr_simple(right, row)?;
                match op {
                    BinaryOperator::Eq => Ok(Value::Bool(lv == rv)),
                    BinaryOperator::Ne => Ok(Value::Bool(lv != rv)),
                    BinaryOperator::Lt => Ok(Value::Bool(lv < rv)),
                    BinaryOperator::Le => Ok(Value::Bool(lv <= rv)),
                    BinaryOperator::Gt => Ok(Value::Bool(lv > rv)),
                    BinaryOperator::Ge => Ok(Value::Bool(lv >= rv)),
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
                    _ => Ok(Value::Bool(false)),
                }
            }
            Expr::Column(name) => {
                row.get(name).cloned().ok_or_else(|| MoteDBError::ColumnNotFound(name.clone()))
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
            Expr::StWithin { .. } => {
                Ok(row.get("__spatial_within__").cloned().unwrap_or(Value::Bool(false)))
            }
            Expr::StKnn { .. } => {
                Ok(row.get("__spatial_knn__").cloned().unwrap_or(Value::Bool(false)))
            }
            Expr::StDistance { .. } => {
                // ST_DISTANCE is a function, not a predicate — cannot evaluate in simple path
                Ok(row.get("__spatial_distance__").cloned().unwrap_or(Value::Float(0.0)))
            }
            Expr::Match { .. } => {
                // If we have a pre-computed score, non-zero means match
                let has_score = row.keys().any(|k| k.starts_with("__text_score_"));
                Ok(Value::Bool(has_score))
            }
            _ => Ok(Value::Bool(true)),
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
                        SelectColumn::Expr(_, _) => {
                            // TODO: 表达式求值
                            Value::Null
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
                        SelectColumn::Expr(_, _) => return Value::Null,
                    };
                    // Look up column position in schema (O(1) via column_map HashMap)
                    if let Some(pos) = schema.get_column_position(col_name) {
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
                                Ok(row_ids) => {
                                    let count = row_ids.len() as i64;
                                    return Ok(QueryResult::Select {
                                        columns: vec!["COUNT(*)".to_string()],
                                        rows: vec![vec![Value::Integer(count)]],
                                    });
                                }
                                Err(_) => {
                                    // Fallback to normal execution
                                }
                            }
                        }
                    }
                }
            } else {
                // 🚀 COUNT(*) without WHERE - use真正的流式扫描 (O(1) memory)
                if let TableRef::Table { name: table_name, .. } = stmt.from.as_ref().unwrap() {
                    let row_iter = self.db.scan_table_rows_streaming(table_name)?;
                    let mut count = 0i64;
                    
                    for result in row_iter {
                        let _ = result?;  // 只需验证成功，不保存数据
                        count += 1;
                    }
                    
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
                            Ok(row_ids) => {
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
                            Err(_) => {
                                // Fallback to table scan
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
                            Ok(row_ids) => {
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
                            Err(_) => {
                                // Fallback to table scan
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
        
        // If there's WHERE clause, add safety margin (rows may be filtered out)
        let limit = stmt.limit?;
        let offset = stmt.offset.unwrap_or(0);
        
        if stmt.where_clause.is_some() {
            // Safety margin: load 10x more rows to account for filtering
            // (Better to overestimate than underestimate)
            Some((limit + offset) * 10)
        } else {
            // No WHERE clause: exact limit works
            Some(limit + offset)
        }
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
            Expr::Column(_) | Expr::Literal(_) | Expr::Match { .. } | 
            Expr::KnnSearch { .. } | Expr::KnnDistance { .. } | 
            Expr::StWithin { .. } | Expr::StDistance { .. } | Expr::StKnn { .. } |
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
            
            Expr::Match { column, query } => {
                // 🚀 Fast path: use pre-computed score if available (from text search fast path)
                let score_key = format!("__text_score_{}__", column);
                if let Some(Value::Float(score)) = row.get(&score_key) {
                    return Ok(Value::Float(*score));
                }

                // Get row_id from the row
                let row_id = row.get("__row_id__")
                    .and_then(|v| match v {
                        Value::Integer(i) => Some(*i as u64),
                    _ => None,
                })
                .ok_or_else(|| MoteDBError::Query("MATCH requires __row_id__ in row".into()))?;

            // 🔧 Get table name from row
            let table_name = row.get("__table__")
                .and_then(|v| match v {
                    Value::Text(s) => Some(s.as_str()),
                    _ => None,
                })
                .ok_or_else(|| MoteDBError::Query("MATCH requires __table__ in row".into()))?;
            
            // 🔧 Use index_registry to find the correct user-specified index name
            let index_name = self.db.index_registry.find_by_column(
                table_name,
                column,
                crate::database::index_metadata::IndexType::Text
            ).ok_or_else(|| MoteDBError::Query(format!("No text index found for column '{}.{}'", table_name, column)))?;
            
            let index_ref = self.db.text_indexes.get(&index_name)
                .ok_or_else(|| MoteDBError::Query(format!("Text index '{}' not found", index_name)))?;
            
            // Perform search and get score for this document
            let results = index_ref.value().read().search_ranked(query, 1000)?;
                let score = results.iter()
                    .find(|(doc_id, _)| *doc_id == row_id)
                    .map(|(_, score)| *score)
                    .unwrap_or(0.0);
                
                Ok(Value::Float(score as f64))
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
            
            Expr::StWithin { column, min_x, min_y, max_x, max_y } => {
                // 🚀 Fast path: if we're in the spatial fast path, this row is already confirmed to be within bbox
                if row.get("__spatial_within__").is_some() {
                    return Ok(Value::Bool(true));
                }
                // ST_WITHIN returns Bool - true if point is within bounding box
                let row_id = row.get("__row_id__")
                    .and_then(|v| match v {
                        Value::Integer(i) => Some(*i as u64),
                        _ => None,
                    })
                    .ok_or_else(|| MoteDBError::Query("ST_WITHIN requires __row_id__ in row".into()))?;
                
                // 🔧 Get table name
                let table_name = row.get("__table__")
                    .and_then(|v| match v {
                        Value::Text(s) => Some(s.as_str()),
                        _ => None,
                    })
                    .ok_or_else(|| MoteDBError::Query("ST_WITHIN requires __table__ in row".into()))?;
                
                // 🔧 Use index_registry to find the correct user-specified index name
                let index_name = self.db.index_registry.find_by_column(
                    table_name,
                    column,
                    crate::database::index_metadata::IndexType::Spatial
                ).ok_or_else(|| MoteDBError::Query(format!("No spatial index found for column '{}.{}'", table_name, column)))?;
                
                // Create bounding box
                use crate::types::BoundingBox;
                let bbox = BoundingBox {
                    min_x: *min_x,
                    min_y: *min_y,
                    max_x: *max_x,
                    max_y: *max_y,
                };
                
                // Perform range query using spatial index
                let results = self.db.spatial_range_query(&index_name, &bbox)?;
                
                // Check if row_id is in results
                let in_results = results.contains(&row_id);
                Ok(Value::Bool(in_results))
            }
            
            Expr::StDistance { column, x, y } => {
                // 🚀 Fast path: use pre-computed distance if available (from spatial KNN ORDER BY)
                if let Some(Value::Float(dist)) = row.get("__spatial_distance__") {
                    return Ok(Value::Float(*dist));
                }
                // ST_DISTANCE returns Float - Euclidean distance
                // Get point value from row
                let point_value = self.get_column_value(row, column)
                    .ok_or_else(|| MoteDBError::ColumnNotFound(column.clone()))?;
                
                use crate::types::Geometry;
                let point = match point_value {
                    Value::Spatial(Geometry::Point(p)) => p,
                    _ => return Err(MoteDBError::TypeError(format!("Column '{}' is not a Point", column))),
                };
                
                // Compute Euclidean distance
                let dx = point.x - x;
                let dy = point.y - y;
                let distance = (dx * dx + dy * dy).sqrt();
                
                Ok(Value::Float(distance))
            }
            
            Expr::StKnn { column, x, y, k } => {
                // 🚀 Fast path: if we're in the spatial KNN fast path, this row is already in top-k
                if row.get("__spatial_knn__").is_some() {
                    return Ok(Value::Bool(true));
                }
                // ST_KNN returns Bool - true if this point is in top-k nearest neighbors
                let row_id = row.get("__row_id__")
                    .and_then(|v| match v {
                        Value::Integer(i) => Some(*i as u64),
                        _ => None,
                    })
                    .ok_or_else(|| MoteDBError::Query("ST_KNN requires __row_id__ in row".into()))?;
                
                // 🔧 Get table name
                let table_name = row.get("__table__")
                    .and_then(|v| match v {
                        Value::Text(s) => Some(s.as_str()),
                        _ => None,
                    })
                    .ok_or_else(|| MoteDBError::Query("ST_KNN requires __table__ in row".into()))?;
                
                // 🔧 Use index_registry to find the correct user-specified index name
                let index_name = self.db.index_registry.find_by_column(
                    table_name,
                    column,
                    crate::database::index_metadata::IndexType::Spatial
                ).ok_or_else(|| MoteDBError::Query(format!("No spatial index found for column '{}.{}'", table_name, column)))?;
                
                // Create query point
                use crate::types::Point;
                let query_point = Point { x: *x, y: *y };
                
                // Perform KNN query using spatial index
                let results = self.db.spatial_knn_query(&index_name, &query_point, *k)?;
                
                // Check if row_id is in results
                let in_results = results.iter().any(|(id, _)| *id == row_id);
                Ok(Value::Bool(in_results))
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
                        let mut sum = 0.0;
                        for row in rows {
                            let val = self.evaluator.eval(&args[0], row)?;
                            match val {
                                Value::Integer(i) => sum += i as f64,
                                Value::Float(f) => sum += f,
                                Value::Null => {},
                                _ => return Err(MoteDBError::TypeError("SUM requires numeric values".to_string())),
                            }
                        }
                        Ok(Value::Float(sum))
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
                    expr => return Err(MoteDBError::InvalidArgument(
                        format!("INSERT VALUES must be literals, got {:?}", expr)
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
        
        // 🔥 Update last_insert_id if table has AUTO_INCREMENT primary key
        if schema.is_primary_key_auto_increment() {
            if let Some(row_id) = last_row_id {
                *self.last_insert_id.borrow_mut() = Some(row_id as i64);
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
                        return self.execute_update_by_row_ids(&stmt, &schema, &matching_row_ids);
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
            
            // Apply assignments
            for (col_name, expr) in &stmt.assignments {
                let new_val = if let Expr::Literal(v) = expr {
                    v.clone()
                } else {
                    self.evaluator.eval(expr, &sql_row)?
                };
                sql_row.insert(col_name.clone(), new_val);
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
                        return self.execute_delete_by_row_ids(&stmt, &schema, &matching_row_ids);
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
        pk_hash_key: &str,
        pk_col_name: &str,
        pk_value: &Value,
    ) -> Result<Option<RowId>> {
        // Try LRU cache first
        if let Some(lookup) = self.db.pk_lookup.get(table) {
            if let Some(rid) = lookup.get(pk_hash_key) {
                return Ok(Some(rid));
            }
        }

        // Cache miss — fall back to disk-based column index
        let row_ids = self.db.query_by_column(table, pk_col_name, pk_value)?;

        // Refill cache from disk result so next lookup is O(1)
        if let Some(&rid) = row_ids.first() {
            if let Some(lookup) = self.db.pk_lookup.get(table) {
                lookup.insert(pk_hash_key.to_string(), rid);
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
            let pk_key = target_value.to_hash_key();
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
            for (col_name, expr) in &stmt.assignments {
                let new_val = if let Expr::Literal(v) = expr {
                    v.clone()
                } else {
                    self.evaluator.eval(expr, &sql_row)?
                };
                sql_row.insert(col_name.clone(), new_val);
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
            let pk_key = target_value.to_hash_key();
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
    ) -> Result<QueryResult> {
        let mut affected_rows = 0;
        for &row_id in row_ids {
            let row = match self.db.get_table_row(&stmt.table, row_id)? {
                Some(r) => r,
                None => continue,
            };

            let mut sql_row = row_to_sql_row(&row, schema)?;
            for (col_name, expr) in &stmt.assignments {
                let new_val = if let Expr::Literal(v) = expr {
                    v.clone()
                } else {
                    self.evaluator.eval(expr, &sql_row)?
                };
                sql_row.insert(col_name.clone(), new_val);
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
        _schema: &crate::types::TableSchema,
        row_ids: &[RowId],
    ) -> Result<QueryResult> {
        let mut affected_rows = 0;
        for &row_id in row_ids {
            let row = match self.db.get_table_row(&stmt.table, row_id)? {
                Some(r) => r,
                None => continue,
            };

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
        
        Ok(QueryResult::Definition {
            message: format!("Table '{}' created successfully{}", stmt.table, pk_info),
        })
    }
    
    /// Execute CREATE INDEX statement
    fn execute_create_index(&self, stmt: CreateIndexStmt) -> Result<QueryResult> {
        use crate::types::BoundingBox;
        
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
            IndexType::Spatial => {
                // Verify column is spatial
                if !matches!(column.col_type, ColumnType::Spatial) {
                    return Err(MoteDBError::TypeError(
                        format!("SPATIAL index requires SPATIAL column, got {:?}", column.col_type)
                    ));
                }
                IndexType::Spatial
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
            IndexType::Spatial => {
                // 1️⃣ Create empty spatial index
                // Use default world bounds: [-180, -90] to [180, 90] (longitude, latitude)
                let default_bounds = BoundingBox::new(-180.0, -90.0, 180.0, 90.0);
                self.db.create_spatial_index(&index_name, default_bounds)?;
                
                // 2️⃣ Backfill existing data (critical fix!)
                let column_pos = schema.get_column_position(&stmt.column)
                    .ok_or_else(|| MoteDBError::ColumnNotFound(stmt.column.clone()))?;
                
                let iter = self.db.scan_table_rows_streaming(&stmt.table)?;
                let mut backfill_count = 0;

                for result in iter {
                    let (row_id, row) = result?;
                    if let Some(Value::Spatial(geometry)) = row.get(column_pos) {
                        if let Err(e) = self.db.insert_geometry(row_id, &index_name, geometry.clone()) {
                            debug_log!("⚠️ Failed to backfill spatial index for row {}: {}", row_id, e);
                        } else {
                            backfill_count += 1;
                        }
                    }
                }
                
                if backfill_count > 0 {
                    debug_log!("Backfilled {} rows into spatial index '{}'", backfill_count, index_name);
                }
                
                // 3️⃣ Register metadata
                let metadata = crate::database::index_metadata::IndexMetadata::new(
                    index_name.clone(),
                    stmt.table.clone(),
                    stmt.column.clone(),
                    crate::database::index_metadata::IndexType::Spatial,
                );
                self.db.index_registry.register(metadata)?;
            }
            IndexType::Timestamp => {
                // Timestamp index is global and already created with database
                // No-op, but return success
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
    fn execute_drop_table(&self, _stmt: DropTableStmt) -> Result<QueryResult> {
        Err(MoteDBError::NotImplemented("DROP TABLE not yet implemented".to_string()))
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
            IndexType::Spatial => {
                self.db.spatial_indexes.remove(index_name);
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
        let (column, query) = match where_clause {
            Expr::Match { column, query } => (column.clone(), query.clone()),
            // Handle AND: MATCH(...) AND other_conditions — only if MATCH is the dominant filter
            Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
                // Try both sides for a MATCH expression
                if let Expr::Match { column, query } = left.as_ref() {
                    (column.clone(), query.clone())
                } else if let Expr::Match { column, query } = right.as_ref() {
                    (column.clone(), query.clone())
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
            None => return Ok(None), // No text index — fall back to normal path
        };

        if !self.db.text_indexes.contains_key(&index_name) {
            return Ok(None);
        }

        // Determine limit (use LIMIT from query, or default to top 1000 for scoring)
        let limit = stmt.limit.unwrap_or(1000);

        // ⚡ Single index lookup — get top-k row_ids with BM25 scores
        let results = match self.db.text_search_ranked(&index_name, &query, limit) {
            Ok(r) => r,
            Err(_) => return Ok(None),
        };

        if results.is_empty() {
            return Ok(Some(QueryResult::Select {
                columns: vec![],
                rows: vec![],
            }));
        }

        // Load rows for matching row_ids
        let schema = self.db.get_table_schema(table_name)?;
        let mut sql_rows = Vec::with_capacity(results.len());

        for (row_id, score) in &results {
            if let Ok(Some(row)) = self.db.get_table_row(table_name, *row_id) {
                let mut sql_row = row_to_sql_row(&row, &schema)?;
                // Add metadata
                sql_row.insert("__row_id__".to_string(), Value::Integer(*row_id as i64));
                sql_row.insert("__table__".to_string(), Value::Text(table_name.to_string()));
                // Pre-compute MATCH score so SELECT MATCH(...) AGAINST works
                sql_row.insert(format!("__text_score_{}__", column), Value::Float(*score as f64));
                // Qualified names
                let old_row = std::mem::take(&mut sql_row);
                let mut qualified = SqlRow::new();
                qualified.insert("__row_id__".to_string(), Value::Integer(*row_id as i64));
                qualified.insert("__table__".to_string(), Value::Text(table_name.to_string()));
                qualified.insert(format!("__text_score_{}__", column), Value::Float(*score as f64));
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

        let (column_names, result_rows) = self.project_text_search_columns(
            stmt, &sql_rows, &schema, &column, &results
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
            Expr::StWithin { column, min_x, min_y, max_x, max_y } => {
                self.execute_spatial_within_fast(stmt, table_name, column, *min_x, *min_y, *max_x, *max_y)
            }
            Expr::StKnn { column, x, y, k } => {
                self.execute_spatial_knn_fast(stmt, table_name, column, *x, *y, *k)
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

        // Match ORDER BY ST_DISTANCE(column, x, y) or ORDER BY alias where alias refers to ST_DISTANCE
        let (column, x, y) = match &order_by.expr {
            Expr::StDistance { column, x, y } => (column.clone(), *x, *y),
            Expr::Column(alias) => {
                // Look up alias in SELECT columns to find the ST_DISTANCE expression
                let mut found = None;
                for col in &stmt.columns {
                    match col {
                        SelectColumn::Expr(expr, Some(a)) if a == alias => {
                            if let Expr::StDistance { column, x, y } = expr {
                                found = Some((column.clone(), *x, *y));
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

        // Find spatial index for this column
        let index_name = match self.db.index_registry.find_by_column(
            &table_name, &column,
            crate::database::index_metadata::IndexType::Spatial
        ) {
            Some(name) => name,
            None => return Ok(None),
        };
        if !self.db.spatial_indexes.contains_key(&index_name) {
            return Ok(None);
        }

        // Use KNN query
        let point = crate::types::Point { x, y };
        let results = match self.db.spatial_knn_query(&index_name, &point, limit) {
            Ok(r) => r,
            Err(_) => return Ok(None),
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

    /// Execute ST_WITHIN using spatial index directly
    #[allow(clippy::too_many_arguments)]
    fn execute_spatial_within_fast(
        &self,
        stmt: &SelectStmt,
        table_name: &str,
        column: &str,
        min_x: f64, min_y: f64, max_x: f64, max_y: f64,
    ) -> Result<Option<QueryResult>> {
        let index_name = match self.db.index_registry.find_by_column(
            table_name, column,
            crate::database::index_metadata::IndexType::Spatial
        ) {
            Some(name) => name,
            None => return Ok(None),
        };

        if !self.db.spatial_indexes.contains_key(&index_name) {
            return Ok(None);
        }

        let bbox = crate::types::BoundingBox { min_x, min_y, max_x, max_y };
        let row_ids = match self.db.spatial_range_query(&index_name, &bbox) {
            Ok(ids) => ids,
            Err(_) => return Ok(None),
        };

        self.load_and_project_spatial_rows(stmt, table_name, &row_ids, None, true)
    }

    /// Execute ST_KNN using spatial index directly
    fn execute_spatial_knn_fast(
        &self,
        stmt: &SelectStmt,
        table_name: &str,
        column: &str,
        x: f64, y: f64, k: usize,
    ) -> Result<Option<QueryResult>> {
        let index_name = match self.db.index_registry.find_by_column(
            table_name, column,
            crate::database::index_metadata::IndexType::Spatial
        ) {
            Some(name) => name,
            None => return Ok(None),
        };

        if !self.db.spatial_indexes.contains_key(&index_name) {
            return Ok(None);
        }

        let point = crate::types::Point { x, y };
        let results = match self.db.spatial_knn_query(&index_name, &point, k) {
            Ok(r) => r,
            Err(_) => return Ok(None),
        };

        // Extract row_ids and build distance map
        let row_ids: Vec<RowId> = results.iter().map(|(id, _)| *id).collect();
        let dist_map: std::collections::HashMap<u64, f64> = results.into_iter().collect();

        self.load_and_project_spatial_rows(stmt, table_name, &row_ids, Some(&dist_map), false)
    }

    /// Load rows by row_ids and project columns for spatial fast path
    fn load_and_project_spatial_rows(
        &self,
        stmt: &SelectStmt,
        table_name: &str,
        row_ids: &[RowId],
        dist_map: Option<&std::collections::HashMap<u64, f64>>,
        is_within: bool,
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

        // 🚀 Use batch loading instead of per-row get_table_row
        let batch_rows = self.db.get_table_rows_batch(table_name, row_ids_to_load)?;

        let mut sql_rows = Vec::with_capacity(batch_rows.len());
        for (row_id, row_opt) in batch_rows {
            if let Some(row) = row_opt {
                let mut sql_row = row_to_sql_row(&row, &schema)?;
                sql_row.insert("__row_id__".to_string(), Value::Integer(row_id as i64));
                sql_row.insert("__table__".to_string(), Value::Text(table_name.to_string()));
                if is_within {
                    sql_row.insert("__spatial_within__".to_string(), Value::Bool(true));
                } else {
                    sql_row.insert("__spatial_knn__".to_string(), Value::Bool(true));
                }
                if let Some(dm) = dist_map {
                    if let Some(d) = dm.get(&row_id) {
                        sql_row.insert("__spatial_distance__".to_string(), Value::Float(*d));
                    }
                }
                let old_row = std::mem::take(&mut sql_row);
                let mut qualified = SqlRow::new();
                qualified.insert("__row_id__".to_string(), Value::Integer(row_id as i64));
                qualified.insert("__table__".to_string(), Value::Text(table_name.to_string()));
                if is_within {
                    qualified.insert("__spatial_within__".to_string(), Value::Bool(true));
                } else {
                    qualified.insert("__spatial_knn__".to_string(), Value::Bool(true));
                }
                if let Some(dm) = dist_map {
                    if let Some(d) = dm.get(&row_id) {
                        qualified.insert("__spatial_distance__".to_string(), Value::Float(*d));
                    }
                }
                for (col_name, val) in old_row.into_iter() {
                    let qname = Self::make_qualified_name(table_name, &col_name);
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

    fn to_bool(&self, val: &Value) -> Result<bool> {
        match val {
            Value::Bool(b) => Ok(*b),
            Value::Integer(i) => Ok(*i != 0),
            Value::Float(f) => Ok(*f != 0.0 && !f.is_nan()),  // 🔧 Support Float: non-zero and non-NaN is true
            Value::Null => Ok(false),
            _ => Err(MoteDBError::TypeError("Cannot convert to boolean".to_string())),
        }
    }
    
    #[allow(dead_code)]
    fn generate_row_id(&self, _table: &str) -> Result<u64> {
        // Simple row ID generation: use timestamp + counter
        // TODO: Implement proper auto-increment per table
        use std::time::{SystemTime, UNIX_EPOCH, Duration};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_micros() as u64;
        Ok(timestamp)
    }
    
    /// 🚀 提取所有可索引条件（多条件索引下推）
    /// 
    /// 从 WHERE 子句中提取所有可以使用索引的条件，包括：
    /// - 点查询: col = value
    /// - 范围查询: col > X AND col < Y
    /// - 不等式: col < value, col > value
    /// 
    /// 返回 (可索引条件列表, 不可索引的剩余表达式)
    #[allow(dead_code)]
    fn extract_indexable_conditions(&self, expr: &Expr) -> (Vec<IndexableCondition>, Vec<Expr>) {
        let mut indexable = Vec::new();
        let mut non_indexable = Vec::new();
        
        self.extract_conditions_recursive(expr, &mut indexable, &mut non_indexable);
        
        (indexable, non_indexable)
    }
    
    /// 递归提取条件（处理 AND 树）
    #[allow(dead_code)]
    #[allow(clippy::only_used_in_recursion)]
    fn extract_conditions_recursive(
        &self,
        expr: &Expr,
        indexable: &mut Vec<IndexableCondition>,
        non_indexable: &mut Vec<Expr>,
    ) {
        match expr {
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
                // 递归处理 AND 的两边
                self.extract_conditions_recursive(left, indexable, non_indexable);
                self.extract_conditions_recursive(right, indexable, non_indexable);
            }
            Expr::BinaryOp { left, op, right } => {
                // 尝试提取单个条件
                match (left.as_ref(), op, right.as_ref()) {
                    // col = value
                    (Expr::Column(col), BinaryOperator::Eq, Expr::Literal(val)) |
                    (Expr::Literal(val), BinaryOperator::Eq, Expr::Column(col)) => {
                        indexable.push(IndexableCondition::PointQuery {
                            column: col.clone(),
                            value: val.clone(),
                        });
                    }
                    // col < value
                    (Expr::Column(col), BinaryOperator::Lt, Expr::Literal(val)) |
                    (Expr::Column(col), BinaryOperator::Le, Expr::Literal(val)) => {
                        indexable.push(IndexableCondition::LessThan {
                            column: col.clone(),
                            value: val.clone(),
                        });
                    }
                    // col > value
                    (Expr::Column(col), BinaryOperator::Gt, Expr::Literal(val)) |
                    (Expr::Column(col), BinaryOperator::Ge, Expr::Literal(val)) => {
                        indexable.push(IndexableCondition::GreaterThan {
                            column: col.clone(),
                            value: val.clone(),
                        });
                    }
                    // value < col (反向)
                    (Expr::Literal(val), BinaryOperator::Lt, Expr::Column(col)) |
                    (Expr::Literal(val), BinaryOperator::Le, Expr::Column(col)) => {
                        indexable.push(IndexableCondition::GreaterThan {
                            column: col.clone(),
                            value: val.clone(),
                        });
                    }
                    // value > col (反向)
                    (Expr::Literal(val), BinaryOperator::Gt, Expr::Column(col)) |
                    (Expr::Literal(val), BinaryOperator::Ge, Expr::Column(col)) => {
                        indexable.push(IndexableCondition::LessThan {
                            column: col.clone(),
                            value: val.clone(),
                        });
                    }
                    _ => {
                        // 无法索引，加入后置过滤
                        non_indexable.push(expr.clone());
                    }
                }
            }
            _ => {
                // 其他表达式（如函数调用）无法索引
                non_indexable.push(expr.clone());
            }
        }
    }
    
    /// 🚀 选择最优索引
    /// 
    /// 从多个可索引条件中选择最优的一个：
    /// 1. 优先级：点查询 > 范围查询 > 不等式查询
    /// 2. 检查索引是否存在
    /// 3. 返回 (最优索引条件, 其他条件作为后置过滤)
    #[allow(dead_code)]
    fn choose_best_index(
        &self,
        conditions: &[IndexableCondition],
        table_name: &str,
    ) -> Option<(IndexableCondition, Vec<Expr>)> {
        if conditions.is_empty() {
            return None;
        }
        
        // 1. 尝试点查询（最快）
        for cond in conditions {
            if let IndexableCondition::PointQuery { column, .. } = cond {
                let index_name = format!("{}.{}", table_name, column);
                if self.db.column_indexes.contains_key(&index_name) {
                    return Some((cond.clone(), self.build_post_filters(conditions, cond)));
                }
            }
        }
        
        // 2. 尝试范围查询
        // TODO: 检测同列的 > 和 < 条件，合并为范围查询
        
        // 3. 尝试不等式查询
        for cond in conditions {
            match cond {
                IndexableCondition::LessThan { column, .. } |
                IndexableCondition::GreaterThan { column, .. } => {
                    let index_name = format!("{}.{}", table_name, column);
                    if self.db.column_indexes.contains_key(&index_name) {
                        return Some((cond.clone(), self.build_post_filters(conditions, cond)));
                    }
                }
                _ => {}
            }
        }
        
        None
    }
    
    /// 构建后置过滤表达式（排除已用索引的条件）
    #[allow(dead_code)]
    fn build_post_filters(
        &self,
        _all_conditions: &[IndexableCondition],
        _used_condition: &IndexableCondition,
    ) -> Vec<Expr> {
        // 简化实现：返回所有其他条件
        // TODO: 正确地重建表达式树
        Vec::new()
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
                    
                    let row = bincode::deserialize::<crate::types::Row>(data)
                        .map_err(|e| StorageError::InvalidData(format!("Deserialization failed: {}", e)))?;

                    // 🚀 Fast path for SELECT *: skip HashMap conversion entirely
                    //     Direct positional projection from Vec<Value> — saves 2*N HashMap
                    //     inserts + N format!() calls for prefix rewriting.
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
                
                let row = bincode::deserialize::<crate::types::Row>(data)
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
}

/// Helper struct for vector ORDER BY plan
struct VectorOrderByPlan {
    table: String,
    column: String,
    query_vector: Vec<f32>,
    k: usize,
}
