/// Query executor - executes SQL statements against storage engine

use super::ast::*;
use super::evaluator::ExprEvaluator;
use super::row_converter::{row_to_sql_row, sql_row_to_row, rows_to_sql_rows};
use crate::database::MoteDB;
use crate::error::{Result, MoteDBError};
use crate::types::{Value, SqlRow, TableSchema, ColumnType};
use std::sync::Arc;

/// 🚀 索引下推：可索引的条件类型
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

pub struct QueryExecutor {
    db: Arc<MoteDB>,
    evaluator: ExprEvaluator,
}

impl QueryExecutor {
    pub fn new(db: Arc<MoteDB>) -> Self {
        Self {
            evaluator: ExprEvaluator::with_db(db.clone()),
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
            Statement::ShowTables => self.execute_show_tables(),
            Statement::DescribeTable(table_name) => self.execute_describe_table(table_name),
        }
    }
    
    /// Execute SELECT statement
    fn execute_select(&self, stmt: SelectStmt) -> Result<QueryResult> {
        self.execute_select_internal(&stmt)
    }
    
    /// Internal SELECT execution (takes &SelectStmt to allow reuse in subqueries)
    fn execute_select_internal(&self, stmt: &SelectStmt) -> Result<QueryResult> {
        // 🚀 FAST PATH -1: ORDER BY vector distance optimization (P0)
        // Pattern: SELECT * FROM table ORDER BY column <-> [...] LIMIT k
        // → Directly use vector index search (724x faster!)
        if let Some(plan) = self.try_optimize_vector_order_by(stmt)? {
            return self.execute_vector_order_by_plan(stmt, &plan);
        }
        
        // 🚀 FAST PATH 0: Vector search optimization (P0)
        // Pattern: SELECT * FROM table WHERE VECTOR_SEARCH(column, [...], k)
        if let Some(ref where_clause) = stmt.where_clause {
            if let Some((table_name, col_name, query_vector, k)) = self.try_extract_vector_search(where_clause, &stmt.from) {
                // ⚡ Ultra-fast path: Use vector index directly
                let index_name = format!("{}_{}", table_name, col_name);
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
        
        // 🚀 FAST PATH 1: Aggregate query optimization (P0-2)
        // Pattern: SELECT COUNT(*) FROM table [WHERE indexed_col = value]
        if self.has_only_count_aggregate(&stmt.columns) && stmt.group_by.is_none() {
            // Check if WHERE clause can use index
            if let Some(ref where_clause) = stmt.where_clause {
                if let Some((col_name, target_value)) = self.try_extract_point_query(where_clause) {
                    if let TableRef::Table { name: table_name, .. } = &stmt.from {
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
                // COUNT(*) without WHERE - use table row count
                if let TableRef::Table { name: table_name, .. } = &stmt.from {
                    let all_rows = self.db.scan_table_rows(table_name)?;
                    let count = all_rows.len() as i64;
                    return Ok(QueryResult::Select {
                        columns: vec!["COUNT(*)".to_string()],
                        rows: vec![vec![Value::Integer(count)]],
                    });
                }
            }
        }
        
        // 🚀 FAST PATH 2: Try to use column index for WHERE optimization
        // Priority: Range query > Point query > Full scan
        let (all_sql_rows, combined_schema) = if let Some(ref where_clause) = stmt.where_clause {
            // Try range query first (dual-bound: col > X AND col < Y)
            if let Some((col_name, lower_value, lower_op, upper_value, upper_op)) = self.try_extract_range_query(where_clause) {
                if let TableRef::Table { name: table_name, .. } = &stmt.from {
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
                        
                        let mut prefixed_schema = schema.clone();
                        for col in &mut prefixed_schema.columns {
                            col.name = format!("{}.{}", prefix, col.name);
                        }
                        
                        (sql_rows, prefixed_schema)
                    } else {
                        // No index, use table scan
                        self.execute_from(&stmt.from)?
                    }
                } else {
                    self.execute_from(&stmt.from)?
                }
            }
            // Try point query
            else if let Some((col_name, target_value)) = self.try_extract_point_query(where_clause) {
                // Extract table name from FROM clause
                if let TableRef::Table { name: table_name, .. } = &stmt.from {
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
                                
                                let mut prefixed_schema = schema.clone();
                                for col in &mut prefixed_schema.columns {
                                    col.name = format!("{}.{}", prefix, col.name);
                                }
                                
                                (sql_rows, prefixed_schema)
                            }
                            Err(_) => {
                                // Fallback to table scan
                                self.execute_from(&stmt.from)?
                            }
                        }
                    } else {
                        // No index, use table scan
                        self.execute_from(&stmt.from)?
                    }
                } else {
                    // Not a simple table (e.g., subquery or join)
                    self.execute_from(&stmt.from)?
                }
            }
            // 🚀 Try inequality query (col < value, col > value, etc.)
            else if let Some((col_name, op, value)) = self.try_extract_inequality(where_clause) {
                if let TableRef::Table { name: table_name, .. } = &stmt.from {
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
                                
                                let mut prefixed_schema = schema.clone();
                                for col in &mut prefixed_schema.columns {
                                    col.name = format!("{}.{}", prefix, col.name);
                                }
                                
                                (sql_rows, prefixed_schema)
                            }
                            Err(_) => {
                                // Fallback to table scan
                                self.execute_from(&stmt.from)?
                            }
                        }
                    } else {
                        // No index, use table scan
                        self.execute_from(&stmt.from)?
                    }
                } else {
                    // Not a simple table
                    self.execute_from(&stmt.from)?
                }
            } else {
                // Not a simple point/range query
                self.execute_from(&stmt.from)?
            }
        } else {
            // No WHERE clause
            self.execute_from(&stmt.from)?
        };
        
        // 🎯 Filter rows (WHERE clause) - Apply remaining conditions
        let filtered_rows: Vec<(u64, SqlRow)> = if let Some(ref where_clause) = stmt.where_clause {
            // Check if we already used the index (in which case, no need to filter again)
            let used_index = if let Some(_) = self.try_extract_range_query(where_clause) {
                // Range query - check if we used index
                if let TableRef::Table { name: table_name, .. } = &stmt.from {
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
                if let TableRef::Table { name: table_name, .. } = &stmt.from {
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
        } else {
            all_sql_rows
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
    
    /// Execute FROM clause - handles single table or JOINs
    /// Returns all rows with combined schema
    fn execute_from(&self, table_ref: &TableRef) -> Result<(Vec<(u64, SqlRow)>, TableSchema)> {
        match table_ref {
            TableRef::Table { name, alias } => {
                // Single table - use table-specific scan
                let schema = self.db.get_table_schema(name)?;
                let all_rows = self.db.scan_table_rows(name)?; // ⭐ Use table-specific scan
                let mut sql_rows = rows_to_sql_rows(all_rows, &schema)?;
                
                // Always prefix column names with table or alias for JOIN compatibility
                let prefix = alias.as_ref().unwrap_or(name);
                
                // Update SqlRow keys to include table prefix + add metadata
                // 🚀 P1 优化：使用 take() 避免克隆所有值
                for (row_id, sql_row) in &mut sql_rows {
                    let mut new_sql_row = SqlRow::new();
                    // Add metadata for MATCH expressions
                    new_sql_row.insert("__row_id__".to_string(), Value::Integer(*row_id as i64));
                    new_sql_row.insert("__table__".to_string(), Value::Text(name.clone()));
                    
                    // 使用 drain() 移动值而不是克隆
                    let old_row = std::mem::take(sql_row);
                    for (col_name, val) in old_row.into_iter() {
                        let qualified_name = format!("{}.{}", prefix, col_name);
                        new_sql_row.insert(qualified_name, val);  // ✅ 移动，不克隆
                    }
                    *sql_row = new_sql_row;
                }
                
                // Update schema column names
                let mut prefixed_schema = schema.clone();
                for col in &mut prefixed_schema.columns {
                    col.name = format!("{}.{}", prefix, col.name);
                }
                
                Ok((sql_rows, prefixed_schema))
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
                        
                        Ok((sql_rows, schema))
                    }
                    _ => Err(MoteDBError::Query("Subquery must be a SELECT".into())),
                }
            }
            TableRef::Join { left, right, join_type, on_condition } => {
                // Recursive: evaluate left and right
                let (left_rows, left_schema) = self.execute_from(left)?;
                let (right_rows, right_schema) = self.execute_from(right)?;
                
                // Combine schemas
                let mut combined_schema = left_schema.clone();
                combined_schema.columns.extend(right_schema.columns.clone());
                
                // Perform JOIN based on type
                let joined_rows = match join_type {
                    JoinType::Inner => self.inner_join(&left_rows, &right_rows, on_condition)?,
                    JoinType::Left => self.left_join(&left_rows, &right_rows, on_condition)?,
                    JoinType::Right => self.right_join(&left_rows, &right_rows, on_condition)?,
                    JoinType::Full => self.full_join(&left_rows, &right_rows, on_condition)?,
                };
                
                Ok((joined_rows, combined_schema))
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
                    hash_table.entry(key).or_insert_with(Vec::new).push(right_row);
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
            Expr::Match { column, query } => {
                // Get row_id from the row
                let row_id = row.get("__row_id__")
                    .and_then(|v| match v {
                        Value::Integer(i) => Some(*i as u64),
                    _ => None,
                })
                .ok_or_else(|| MoteDBError::Query("MATCH requires __row_id__ in row".into()))?;
            
            // Get text index (default for now)
            let index_name = format!("{}_{}", row.get("__table__")
                .and_then(|v| match v {
                    Value::Text(s) => Some(s.as_str()),
                    _ => None,
                })
                .unwrap_or("default"), column);
            
            let index_ref = self.db.text_indexes.get(&index_name)
                .or_else(|| self.db.text_indexes.get("default"))
                .ok_or_else(|| MoteDBError::Query(format!("Text index for column '{}' not found", column)))?;
            
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
                
                // Get table name and construct index name
                let table_name = row.get("__table__")
                    .and_then(|v| match v {
                        Value::Text(s) => Some(s.as_str()),
                        _ => None,
                    })
                    .unwrap_or("default");
                let index_name = format!("{}_{}", table_name, column);
                
                // Perform KNN search using public API
                let results = self.db.vector_search(&index_name, query_vector, *k)?;
                
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
                // ST_WITHIN returns Bool - true if point is within bounding box
                let row_id = row.get("__row_id__")
                    .and_then(|v| match v {
                        Value::Integer(i) => Some(*i as u64),
                        _ => None,
                    })
                    .ok_or_else(|| MoteDBError::Query("ST_WITHIN requires __row_id__ in row".into()))?;
                
                // Get table name and construct index name
                let table_name = row.get("__table__")
                    .and_then(|v| match v {
                        Value::Text(s) => Some(s.as_str()),
                        _ => None,
                    })
                    .unwrap_or("default");
                let index_name = format!("{}_{}", table_name, column);
                
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
                let in_results = results.iter().any(|id| *id == row_id);
                Ok(Value::Bool(in_results))
            }
            
            Expr::StDistance { column, x, y } => {
                // ST_DISTANCE returns Float - Euclidean distance
                // Get point value from row
                let point_value = self.get_column_value(row, column)
                    .ok_or_else(|| MoteDBError::ColumnNotFound(column.clone()))?;
                
                use crate::types::{Geometry, Point};
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
                // ST_KNN returns Bool - true if this point is in top-k nearest neighbors
                let row_id = row.get("__row_id__")
                    .and_then(|v| match v {
                        Value::Integer(i) => Some(*i as u64),
                        _ => None,
                    })
                    .ok_or_else(|| MoteDBError::Query("ST_KNN requires __row_id__ in row".into()))?;
                
                // Get table name and construct index name
                let table_name = row.get("__table__")
                    .and_then(|v| match v {
                        Value::Text(s) => Some(s.as_str()),
                        _ => None,
                    })
                    .unwrap_or("default");
                let index_name = format!("{}_{}", table_name, column);
                
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
                        .and_then(|val| match val {
                            Value::Text(s) => Ok(s.clone()),
                            Value::Integer(i) => Ok(i.to_string()),
                            Value::Float(f) => Ok(f.to_string()),
                            _ => Ok(format!("{:?}", val)),
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
                        .and_then(|val| match val {
                            Value::Text(s) => Ok(s.clone()),
                            Value::Integer(i) => Ok(i.to_string()),
                            Value::Float(f) => Ok(f.to_string()),
                            _ => Ok(format!("{:?}", val)),
                        })
                })
                .collect();
            let group_key = group_key?;
            
            groups.entry(group_key).or_insert_with(Vec::new).push(row);
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
        
        for (group_key, group_rows) in groups {
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
            // SELECT *
            schema.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            columns.iter().map(|col| match col {
                SelectColumn::Star => "*".to_string(),
                SelectColumn::Column(name) => name.clone(),
                SelectColumn::ColumnWithAlias(_, alias) => alias.clone(),
                SelectColumn::Expr(_, Some(alias)) => alias.clone(),
                SelectColumn::Expr(expr, None) => format!("{:?}", expr), // Use debug format as default
            }).collect()
        };
        
        // Project rows
        let projected_rows: Vec<Vec<Value>> = rows.iter().map(|(_, row)| {
            if columns.len() == 1 && matches!(columns[0], SelectColumn::Star) {
                // SELECT * - return all columns in schema order
                schema.columns.iter()
                    .map(|col| row.get(&col.name).cloned().unwrap_or(Value::Null))
                    .collect()
            } else {
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
            }
        }).collect();
        
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
                sql_row.insert(col_name.clone(), val);
            }
            
            // Convert to storage Row
            let row = sql_row_to_row(&sql_row, &schema)?;
            prepared_rows.push((sql_row, row));
        }
        
        let affected_rows = prepared_rows.len();
        
        if has_vector_column && prepared_rows.len() > 1 {
            // 🚀 批量插入路径：提升向量索引质量
            eprintln!("[SQL] 🔥 Batch inserting {} rows with vector columns...", prepared_rows.len());
            
            // 提取所有row_id和向量数据
            let mut vector_batches: std::collections::HashMap<String, Vec<(u64, Vec<f32>)>> = 
                std::collections::HashMap::new();
            
            // 先插入所有行到表（获取row_id）
            for (_sql_row, row) in prepared_rows {
                let row_id = self.db.insert_row_to_table(&stmt.table, row.clone())?;
                
                // 检查是否有向量列需要索引
                for (idx, col_def) in schema.columns.iter().enumerate() {
                    if let crate::types::ColumnType::Tensor(_dim) = col_def.col_type {
                        // 提取向量值
                        if let Some(Value::Vector(vec)) = row.get(idx) {
                            let index_name = format!("{}_{}", stmt.table, col_def.name);
                            vector_batches.entry(index_name)
                                .or_insert_with(Vec::new)
                                .push((row_id, vec.clone()));
                        }
                    }
                }
            }
            
            // 批量插入向量到索引（使用公开API）
            // 🔧 修复：如果索引不存在，跳过（稍后通过CREATE INDEX构建）
            for (index_name, batch) in vector_batches {
                eprintln!("[SQL]   ↳ Batch indexing {} vectors to '{}'...", batch.len(), index_name);
                let insert_start = std::time::Instant::now();
                match self.db.batch_update_vectors(&index_name, batch) {
                    Ok(_) => {
                        eprintln!("[SQL]   ✓ Indexed in {:?}", insert_start.elapsed());
                    },
                    Err(e) if e.to_string().contains("not found") => {
                        eprintln!("[SQL]   ⚠️  Index '{}' not found, skipping (will be built by CREATE INDEX)", index_name);
                    },
                    Err(e) => return Err(e),
                }
            }
        } else {
            // 普通逐条插入路径（无向量列或单行插入）
            for (_sql_row, row) in prepared_rows {
                let _row_id = self.db.insert_row_to_table(&stmt.table, row)?;
            }
        }
        
        Ok(QueryResult::Modification { affected_rows })
    }
    
    /// Execute UPDATE statement
    fn execute_update(&self, stmt: UpdateStmt) -> Result<QueryResult> {
        let schema = self.db.get_table_schema(&stmt.table)?;
        
        // Scan rows for specific table and convert to SqlRow
        let all_rows = self.db.scan_table_rows(&stmt.table)?;
        let all_sql_rows = rows_to_sql_rows(all_rows, &schema)?;
        
        // Filter rows (WHERE clause)
        let filtered_rows: Vec<(u64, SqlRow)> = if let Some(ref where_clause) = stmt.where_clause {
            all_sql_rows.into_iter()
                .filter(|(_, row)| {
                    self.evaluator.eval(where_clause, row)
                        .and_then(|val| self.to_bool(&val))
                        .unwrap_or(false)
                })
                .collect()
        } else {
            all_sql_rows
        };
        
        let mut affected_rows = 0;
        
        for (row_id, mut sql_row) in filtered_rows {
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
            let row = sql_row_to_row(&sql_row, &schema)?;
            
            // 🚀 底层已实现增量索引更新，无需上层维护
            self.db.update_row_in_table(&stmt.table, row_id, row)?;
            
            affected_rows += 1;
        }
        
        Ok(QueryResult::Modification { affected_rows })
    }
    
    /// Execute DELETE statement
    fn execute_delete(&self, stmt: DeleteStmt) -> Result<QueryResult> {
        let schema = self.db.get_table_schema(&stmt.table)?;
        
        // Scan rows for specific table and convert to SqlRow
        let all_rows = self.db.scan_table_rows(&stmt.table)?;
        let all_sql_rows = rows_to_sql_rows(all_rows, &schema)?;
        
        // Filter rows (WHERE clause)
        let filtered_rows: Vec<(u64, SqlRow)> = if let Some(ref where_clause) = stmt.where_clause {
            all_sql_rows.into_iter()
                .filter(|(_, row)| {
                    self.evaluator.eval(where_clause, row)
                        .and_then(|val| self.to_bool(&val))
                        .unwrap_or(false)
                })
                .collect()
        } else {
            all_sql_rows
        };
        
        let affected_rows = filtered_rows.len();
        
        // Delete rows - 底层已实现增量索引维护
        for (row_id, _sql_row) in filtered_rows {
            self.db.delete_row_from_table(&stmt.table, row_id)?;
        }
        
        Ok(QueryResult::Modification { affected_rows })
    }
    
    /// Execute CREATE TABLE statement
    fn execute_create_table(&self, stmt: CreateTableStmt) -> Result<QueryResult> {
        // Convert AST column defs to TableSchema
        let columns: Vec<crate::types::ColumnDef> = stmt.columns.iter().enumerate().map(|(pos, col)| {
            let column_type = match col.data_type {
                DataType::Integer => ColumnType::Integer,
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
        }
        
        self.db.create_table(schema)?;
        
        // 🆕 STEP 3: Auto-create B-Tree index for primary key
        let pk_info = if !primary_key_cols.is_empty() {
            let pk_names: Vec<String> = primary_key_cols.iter().map(|c| c.name.clone()).collect();
            
            // Create index for each primary key column
            for pk_col in &primary_key_cols {
                let index_name = format!("{}_pk_{}", stmt.table, pk_col.name);
                
                // Use create_column_index to create B-Tree index
                self.db.create_column_index(&stmt.table, &pk_col.name)?;
                
                println!("  ✓ Auto-created primary key index: {}.{}", stmt.table, pk_col.name);
            }
            
            format!(" (Primary key: {}, auto-index: ✓)", pk_names.join(", "))
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
                // Create text index with user-specified or default name
                self.db.create_text_index(&index_name)?;
                
                // 🆕 Register metadata
                let metadata = crate::database::index_metadata::IndexMetadata::new(
                    index_name.clone(),
                    stmt.table.clone(),
                    stmt.column.clone(),
                    crate::database::index_metadata::IndexType::Text,
                );
                self.db.index_registry.register(metadata)?;
            }
            IndexType::Vector => {
                // Create vector index with user-specified or default name
                if let ColumnType::Tensor(dim) = column.col_type {
                    self.db.create_vector_index(&index_name, dim)?;
                    
                    // 🆕 Register metadata
                    let metadata = crate::database::index_metadata::IndexMetadata::new(
                        index_name.clone(),
                        stmt.table.clone(),
                        stmt.column.clone(),
                        crate::database::index_metadata::IndexType::Vector,
                    );
                    self.db.index_registry.register(metadata)?;
                } else {
                    unreachable!("Already validated column type");
                }
            }
            IndexType::Spatial => {
                // Create spatial index with user-specified or default name
                // Use default world bounds: [-180, -90] to [180, 90] (longitude, latitude)
                let default_bounds = BoundingBox::new(-180.0, -90.0, 180.0, 90.0);
                self.db.create_spatial_index(&index_name, default_bounds)?;
                
                // 🆕 Register metadata
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
    fn execute_drop_index(&self, _stmt: DropIndexStmt) -> Result<QueryResult> {
        Err(MoteDBError::NotImplemented("DROP INDEX not yet implemented".to_string()))
    }
    
    /// Execute SHOW TABLES
    fn execute_show_tables(&self) -> Result<QueryResult> {
        let tables = self.db.list_tables()?;
        
        let columns = vec!["Tables".to_string()];
        let rows = tables.into_iter()
            .map(|table_name| vec![Value::Text(table_name.into())])
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
                Value::Text(col.name.clone().into()),
                Value::Text(format!("{:?}", col.col_type).into()),
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
                        
                        if col1.is_some() && col2.is_some() && col1 == col2 {
                            let col_name = col1.expect("col1 already checked to be Some").clone();
                            
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
                
                Some((table_name, column, query_vector, k))
            }
            _ => None,
        }
    }
    
    fn to_bool(&self, val: &Value) -> Result<bool> {
        match val {
            Value::Bool(b) => Ok(*b),
            Value::Integer(i) => Ok(*i != 0),
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
    fn extract_indexable_conditions(&self, expr: &Expr) -> (Vec<IndexableCondition>, Vec<Expr>) {
        let mut indexable = Vec::new();
        let mut non_indexable = Vec::new();
        
        self.extract_conditions_recursive(expr, &mut indexable, &mut non_indexable);
        
        (indexable, non_indexable)
    }
    
    /// 递归提取条件（处理 AND 树）
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
    fn build_post_filters(
        &self,
        all_conditions: &[IndexableCondition],
        used_condition: &IndexableCondition,
    ) -> Vec<Expr> {
        // 简化实现：返回所有其他条件
        // TODO: 正确地重建表达式树
        Vec::new()
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
            Expr::BinaryOp { op, left, right } if matches!(op, BinaryOperator::L2Distance | BinaryOperator::CosineDistance) => {
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
        let table_name = match &stmt.from {
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
            query_vector,
            k: limit,
        }))
    }
    
    /// Execute SELECT using vector ORDER BY optimization
    fn execute_vector_order_by_plan(&self, stmt: &SelectStmt, plan: &VectorOrderByPlan) -> Result<QueryResult> {
        debug_log!("[Executor] ✅ 使用向量索引优化 ORDER BY: {} <-> [...] LIMIT {}", plan.column, plan.k);
        
        let index_name = format!("{}_{}", plan.table, plan.column);
        
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
                    if let Some(id_value) = sql_row.get("id") {
                        debug_log!("[Executor] 🔍 row_id={} → id列={:?}", row_id, id_value);
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
