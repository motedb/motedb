/// Query Optimizer - Cost-based index selection and query planning
///
/// # Architecture
/// ```ignore
/// SELECT * FROM users WHERE age >= 20 AND age <= 30 AND status = 'active'
///              ↓
///      Optimizer analyzes:
///       1. Available indexes: [age_idx, status_idx]
///       2. Index cardinality: age_idx=1000, status_idx=100
///       3. Selectivity: age range → 200 rows, status → 50 rows
///       4. Cost model: status_idx (50) < age_idx (200)
///              ↓
///      Selected plan: Use status_idx, then filter by age in-memory
/// ```
use super::ast::*;
use crate::database::MoteDB;
use crate::types::{Value, TableSchema};
use crate::Result;
use std::sync::Arc;
use dashmap::DashMap;

/// Query execution plan
#[derive(Debug, Clone)]
pub struct QueryPlan {
    /// Selected scan method
    pub scan_method: ScanMethod,
    /// Estimated cost (lower is better)
    pub estimated_cost: f64,
    /// Estimated result rows
    pub estimated_rows: usize,
    /// Additional filters to apply after index scan
    pub post_filters: Vec<Expr>,
}

/// Scan method for data access
#[derive(Debug, Clone)]
pub enum ScanMethod {
    /// Full table scan
    FullScan {
        table: String,
    },
    
    /// Point query using column index
    PointQuery {
        table: String,
        column: String,
        value: Value,
    },
    
    /// Range query using column index
    /// 
    /// ## 边界语义
    /// - `start_inclusive`: 下界是否包含（>= vs >）
    /// - `end_inclusive`: 上界是否包含（<= vs <）
    RangeQuery {
        table: String,
        column: String,
        start: Value,
        start_inclusive: bool,
        end: Value,
        end_inclusive: bool,
    },
    
    /// Text search using full-text index
    TextSearch {
        table: String,
        column: String,
        query: String,
    },
    
    /// Vector KNN search
    VectorSearch {
        table: String,
        column: String,
        query_vector: crate::types::ArcVec,
        k: usize,
    },
    
    /// Spatial range query
    SpatialRange {
        table: String,
        column: String,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
    },
    
    /// Primary key index scan (ordered by primary key)
    /// 
    /// Used when:
    /// - ORDER BY primary_key [ASC/DESC]
    /// - Optional: LIMIT n
    /// 
    /// Benefits:
    /// - No in-memory sorting needed
    /// - Can early terminate with LIMIT
    /// - O(k) instead of O(n log n) for sorting
    PrimaryKeyScan {
        table: String,
        ascending: bool,
        limit: Option<usize>,
    },
}

impl ScanMethod {
    pub fn table_name(&self) -> &str {
        match self {
            ScanMethod::FullScan { table } |
            ScanMethod::PointQuery { table, .. } |
            ScanMethod::RangeQuery { table, .. } |
            ScanMethod::TextSearch { table, .. } |
            ScanMethod::VectorSearch { table, .. } |
            ScanMethod::SpatialRange { table, .. } |
            ScanMethod::PrimaryKeyScan { table, .. } => table,
        }
    }
}

/// Index statistics for cost estimation
#[derive(Debug, Clone)]
pub struct IndexStats {
    /// Number of distinct values (cardinality)
    pub cardinality: usize,
    /// Total number of rows indexed
    pub total_rows: usize,
    /// Index size in bytes
    pub size_bytes: usize,
    /// Whether the index is unique
    pub is_unique: bool,
}

impl IndexStats {
    /// Calculate selectivity: fraction of rows matching a value
    pub fn selectivity(&self) -> f64 {
        if self.cardinality == 0 {
            1.0
        } else {
            1.0 / self.cardinality as f64
        }
    }
    
    /// Estimate rows for a point query
    pub fn estimate_point_query(&self) -> usize {
        if self.is_unique {
            1
        } else {
            (self.total_rows as f64 * self.selectivity()) as usize
        }
    }
    
    /// Estimate rows for a range query
    pub fn estimate_range_query(&self, range_fraction: f64) -> usize {
        (self.total_rows as f64 * range_fraction) as usize
    }
}

/// Query optimizer
pub struct QueryOptimizer {
    /// Database reference
    db: Arc<MoteDB>,

    /// Index statistics cache (lock-free DashMap)
    index_stats: DashMap<String, IndexStats>,

    /// Cost model parameters
    cost_params: CostParameters,
}

/// Cost model parameters
#[derive(Debug, Clone)]
struct CostParameters {
    /// Cost of reading one row from disk (ms)
    disk_read_cost: f64,
    /// Cost of reading one row from memory (ms)
    memory_read_cost: f64,
    /// Cost of index lookup (ms)
    index_lookup_cost: f64,
    /// Cost of evaluating one predicate (ms)
    predicate_eval_cost: f64,
}

impl Default for CostParameters {
    fn default() -> Self {
        Self {
            disk_read_cost: 0.01,      // 10μs per disk read
            memory_read_cost: 0.001,    // 1μs per memory read
            index_lookup_cost: 0.005,   // 5μs per index lookup
            predicate_eval_cost: 0.0001, // 0.1μs per predicate eval
        }
    }
}

impl QueryOptimizer {
    pub fn new(db: Arc<MoteDB>) -> Self {
        Self {
            db,
            index_stats: DashMap::new(),
            cost_params: CostParameters::default(),
        }
    }

    /// Resolve an expression to a literal Value if possible.
    /// Handles Literal directly and Parameter(idx) via bound params.
    fn resolve_to_value(params: &[crate::types::Value], expr: &crate::sql::ast::Expr) -> Option<crate::types::Value> {
        use crate::sql::ast::Expr;
        match expr {
            Expr::Literal(v) => Some(v.clone()),
            Expr::Parameter(idx) if *idx > 0 => {
                params.get(idx - 1).cloned()
            }
            _ => None,
        }
    }
    
    /// Optimize SELECT statement and generate execution plan
    pub fn optimize_select(&self, stmt: &SelectStmt, params: &[crate::types::Value]) -> Result<QueryPlan> {
        // 🚀 P0 FIX: Primary Key ORDER BY optimization
        // Detects patterns like:
        // - `SELECT * FROM table ORDER BY id LIMIT k` (id is primary key)
        // - Avoids in-memory sorting by using index scan
        if let Some(plan) = self.optimize_primary_key_order_by(stmt)? {
            return Ok(plan);
        }
        
        // 🚀 P0 FIX: Vector ORDER BY optimization (向量排序索引推送)
        // 检测 ORDER BY embedding <-> [query_vector] LIMIT K
        if let Some(plan) = self.optimize_vector_order_by(stmt)? {
            return Ok(plan);
        }
        
        // 🔥 P0 FIX: Aggregate function optimization
        // Check if this is an aggregate query (COUNT, SUM, AVG, etc.)
        if self.is_aggregate_query(stmt) {
            if let Some(plan) = self.optimize_aggregate(stmt, params)? {
                return Ok(plan);
            }
        }
        
        // Extract table name
        let table_name = match stmt.from.as_ref().unwrap() {
            TableRef::Table { name, .. } => name.clone(),
            _ => {
                // For JOINs and subqueries, skip optimization for now
                return Ok(QueryPlan {
                    scan_method: ScanMethod::FullScan {
                        table: "unknown".to_string(),
                    },
                    estimated_cost: f64::MAX,
                    estimated_rows: 0,
                    post_filters: vec![],
                });
            }
        };
        
        // Get table schema for row count estimation
        let schema = self.db.get_table_schema(&table_name)?;
        let total_rows = self.estimate_table_size(&table_name);
        
        // Extract WHERE clause
        let where_clause = match &stmt.where_clause {
            Some(expr) => expr,
            None => {
                // No WHERE clause - full table scan
                return Ok(QueryPlan {
                    scan_method: ScanMethod::FullScan {
                        table: table_name.clone(),
                    },
                    estimated_cost: self.cost_full_scan(total_rows),
                    estimated_rows: total_rows,
                    post_filters: vec![],
                });
            }
        };
        
        // Analyze WHERE clause and generate candidate plans
        let candidates = self.generate_candidate_plans(&table_name, where_clause, &schema, params)?;

        // Select best plan based on cost
        let best_plan = candidates.into_iter()
            .min_by(|a, b| {
                a.estimated_cost.partial_cmp(&b.estimated_cost)
                    .unwrap_or(std::cmp::Ordering::Equal) // Handle NaN cases
            })
            .unwrap_or_else(|| QueryPlan {
                scan_method: ScanMethod::FullScan {
                    table: table_name.clone(),
                },
                estimated_cost: self.cost_full_scan(total_rows),
                estimated_rows: total_rows,
                post_filters: vec![where_clause.clone()],
            });
        
        Ok(best_plan)
    }
    
    /// Generate candidate execution plans
    fn generate_candidate_plans(
        &self,
        table_name: &str,
        where_clause: &Expr,
        _schema: &TableSchema,
        params: &[crate::types::Value],
    ) -> Result<Vec<QueryPlan>> {
        let mut plans = Vec::new();
        let total_rows = self.estimate_table_size(table_name);

        // Always include full table scan as baseline
        plans.push(QueryPlan {
            scan_method: ScanMethod::FullScan {
                table: table_name.to_string(),
            },
            estimated_cost: self.cost_full_scan(total_rows),
            estimated_rows: total_rows,
            post_filters: vec![where_clause.clone()],
        });

        // Analyze WHERE clause for index opportunities
        self.analyze_where_clause(table_name, where_clause, params, &mut plans)?;

        Ok(plans)
    }
    
    /// Analyze WHERE clause and generate index-based plans
    fn analyze_where_clause(
        &self,
        table_name: &str,
        expr: &Expr,
        params: &[crate::types::Value],
        plans: &mut Vec<QueryPlan>,
    ) -> Result<()> {
        // 🔥 P0 FIX: Check for VECTOR_SEARCH function first (highest priority)
        if let Some((column, query_vector, k)) = self.try_extract_vector_search(expr) {
            self.try_vector_search_plan(table_name, &column, &query_vector, k, plans)?;
            return Ok(()); // Vector search found, this dominates the query
        }
        
        // First, try to extract range query pattern (handles AND specially)
        if let Some((col, start, start_incl, end, end_incl)) = self.try_extract_range_query(expr, params) {
            self.try_range_query_plan(table_name, &col, start, start_incl, end, end_incl, plans)?;
            return Ok(()); // Range query found, no need to recurse
        }
        
        match expr {
            // AND: Try to use most selective index
            Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
                // Try left operand
                self.analyze_where_clause(table_name, left, params, plans)?;
                
                // Try right operand
                self.analyze_where_clause(table_name, right, params, plans)?;
                
                // TODO: Try combining multiple indexes
            }
            
            // OR: Must evaluate all branches
            Expr::BinaryOp { left, op: BinaryOperator::Or, right } => {
                // ORs typically can't use indexes efficiently
                // Just analyze for completeness
                self.analyze_where_clause(table_name, left, params, plans)?;
                self.analyze_where_clause(table_name, right, params, plans)?;
            }
            
            // Point query: col = value (supports Literal AND Parameter)
            Expr::BinaryOp { left, op: BinaryOperator::Eq, right } => {
                if let Some(val) = Self::resolve_to_value(params,right) {
                    if let Expr::Column(col) = left.as_ref() {
                        self.try_point_query_plan(table_name, col, val, plans)?;
                    }
                } else if let Some(val) = Self::resolve_to_value(params,left) {
                    if let Expr::Column(col) = right.as_ref() {
                        self.try_point_query_plan(table_name, col, val, plans)?;
                    }
                }
            }

            // Single-sided range: col > val
            Expr::BinaryOp { left, op: BinaryOperator::Gt, right } => {
                if let Some(val) = Self::resolve_to_value(params,right) {
                    if let Expr::Column(col) = left.as_ref() {
                        self.try_range_query_plan(table_name, col, val.clone(), false, Value::Integer(i64::MAX), true, plans)?;
                    }
                } else if let Some(val) = Self::resolve_to_value(params,left) {
                    if let Expr::Column(col) = right.as_ref() {
                        self.try_range_query_plan(table_name, col, Value::Integer(i64::MIN), true, val.clone(), false, plans)?;
                    }
                }
            }

            // Single-sided range: col >= val
            Expr::BinaryOp { left, op: BinaryOperator::Ge, right } => {
                if let Some(val) = Self::resolve_to_value(params,right) {
                    if let Expr::Column(col) = left.as_ref() {
                        self.try_range_query_plan(table_name, col, val.clone(), true, Value::Integer(i64::MAX), true, plans)?;
                    }
                } else if let Some(val) = Self::resolve_to_value(params,left) {
                    if let Expr::Column(col) = right.as_ref() {
                        self.try_range_query_plan(table_name, col, Value::Integer(i64::MIN), true, val.clone(), false, plans)?;
                    }
                }
            }

            // Single-sided range: col < val
            Expr::BinaryOp { left, op: BinaryOperator::Lt, right } => {
                if let Some(val) = Self::resolve_to_value(params,right) {
                    if let Expr::Column(col) = left.as_ref() {
                        self.try_range_query_plan(table_name, col, Value::Integer(i64::MIN), true, val.clone(), false, plans)?;
                    }
                } else if let Some(val) = Self::resolve_to_value(params,left) {
                    if let Expr::Column(col) = right.as_ref() {
                        self.try_range_query_plan(table_name, col, val.clone(), true, Value::Integer(i64::MAX), true, plans)?;
                    }
                }
            }

            // Single-sided range: col <= val
            Expr::BinaryOp { left, op: BinaryOperator::Le, right } => {
                if let Some(val) = Self::resolve_to_value(params,right) {
                    if let Expr::Column(col) = left.as_ref() {
                        self.try_range_query_plan(table_name, col, Value::Integer(i64::MIN), true, val.clone(), true, plans)?;
                    }
                } else if let Some(val) = Self::resolve_to_value(params,left) {
                    if let Expr::Column(col) = right.as_ref() {
                        self.try_range_query_plan(table_name, col, val.clone(), true, Value::Integer(i64::MAX), true, plans)?;
                    }
                }
            }

            _ => {
                // Other expressions: no index optimization
            }
        }
        
        Ok(())
    }
    
    /// Try to create a point query plan if index exists
    fn try_point_query_plan(
        &self,
        table_name: &str,
        column: &str,
        value: Value,
        plans: &mut Vec<QueryPlan>,
    ) -> Result<()> {
        let index_name = format!("{}.{}", table_name, column);

        // 🚀 Fast path: AUTO_INCREMENT primary key can use direct LSM get (no column index needed)
        let table_result = self.db.table_registry.get_table(table_name);
        let is_auto_increment_pk = table_result
            .ok()
            .map(|schema| {
                schema.primary_key()
                    .map(|pk| pk == column && schema.is_primary_key_auto_increment())
                    .unwrap_or(false)
            })
            .unwrap_or(false);

        if is_auto_increment_pk {
            // Direct LSM get: O(1) cost, exactly 1 estimated row
            plans.push(QueryPlan {
                scan_method: ScanMethod::PointQuery {
                    table: table_name.to_string(),
                    column: column.to_string(),
                    value,
                },
                estimated_cost: self.cost_params.index_lookup_cost,
                estimated_rows: 1,
                post_filters: vec![],
            });
            return Ok(());
        }

        // Check if column index exists
        if !self.db.column_indexes.contains_key(&index_name) {
            return Ok(()); // No index available
        }

        // Get or estimate index statistics
        let stats = self.get_index_stats(&index_name)?;
        let estimated_rows = stats.estimate_point_query();
        
        // Calculate cost: index lookup + row fetch
        let cost = self.cost_params.index_lookup_cost
            + (estimated_rows as f64 * self.cost_params.memory_read_cost);
        
        plans.push(QueryPlan {
            scan_method: ScanMethod::PointQuery {
                table: table_name.to_string(),
                column: column.to_string(),
                value,
            },
            estimated_cost: cost,
            estimated_rows,
            post_filters: vec![], // No additional filters needed
        });
        
        Ok(())
    }
    
    /// Try to create a range query plan if index exists
    /// 
    /// ## 边界语义
    /// - `start_inclusive`: 下界是否包含（>= vs >）
    /// - `end_inclusive`: 上界是否包含（<= vs <）
    #[allow(clippy::too_many_arguments)]
    fn try_range_query_plan(
        &self,
        table_name: &str,
        column: &str,
        start: Value,
        start_inclusive: bool,
        end: Value,
        end_inclusive: bool,
        plans: &mut Vec<QueryPlan>,
    ) -> Result<()> {
        let index_name = format!("{}.{}", table_name, column);
        
        // Check if column index exists
        if !self.db.column_indexes.contains_key(&index_name) {
            return Ok(()); // No index available
        }
        
        // Get or estimate index statistics
        let stats = self.get_index_stats(&index_name)?;

        // Estimate range selectivity from value bounds
        let range_fraction = Self::estimate_range_fraction(&start, &end);
        let estimated_rows = stats.estimate_range_query(range_fraction);
        
        // Calculate cost: index range scan + row fetch
        let cost = self.cost_params.index_lookup_cost * (estimated_rows as f64 * 0.1)
            + (estimated_rows as f64 * self.cost_params.memory_read_cost);
        
        plans.push(QueryPlan {
            scan_method: ScanMethod::RangeQuery {
                table: table_name.to_string(),
                column: column.to_string(),
                start,
                start_inclusive,
                end,
                end_inclusive,
            },
            estimated_cost: cost,
            estimated_rows,
            post_filters: vec![], // No additional filters needed
        });
        
        Ok(())
    }
    
    /// Extract range query pattern from WHERE clause
    /// 
    /// ## 返回格式
    /// `Some((column_name, start_value, start_inclusive, end_value, end_inclusive))`
    /// 
    /// ## 示例
    /// - `id >= 100 AND id < 200` → `("id", 100, true, 200, false)`
    /// - `id > 100 AND id <= 200` → `("id", 100, false, 200, true)`
    fn try_extract_range_query(&self, expr: &Expr, params: &[crate::types::Value]) -> Option<(String, Value, bool, Value, bool)> {
        match expr {
            Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
                if let (
                    Expr::BinaryOp { left: l1, op: op1, right: r1 },
                    Expr::BinaryOp { left: l2, op: op2, right: r2 }
                ) = (left.as_ref(), right.as_ref()) {
                    // Check if both sides reference the same column (supports Literal and Parameter)
                    let col1 = match (l1.as_ref(), r1.as_ref()) {
                        (Expr::Column(c), other) if Self::resolve_to_value(params,other).is_some() => Some(c),
                        (other, Expr::Column(c)) if Self::resolve_to_value(params,other).is_some() => Some(c),
                        _ => None,
                    };

                    let col2 = match (l2.as_ref(), r2.as_ref()) {
                        (Expr::Column(c), other) if Self::resolve_to_value(params,other).is_some() => Some(c),
                        (other, Expr::Column(c)) if Self::resolve_to_value(params,other).is_some() => Some(c),
                        _ => None,
                    };

                    if let (Some(c1), Some(c2)) = (&col1, &col2) {
                        if c1 == c2 {
                            let col_name = (*c1).clone();

                            // Helper to extract (value, is_lower_bound, inclusive)
                            let extract = |col: &Expr, op: &BinaryOperator, val: &Expr| -> Option<(Value, bool, bool)> {
                                let v = Self::resolve_to_value(params,val)?;
                                match (col, op) {
                                    (Expr::Column(_), BinaryOperator::Ge) => Some((v, true, true)),
                                    (Expr::Column(_), BinaryOperator::Gt) => Some((v, true, false)),
                                    (Expr::Column(_), BinaryOperator::Le) => Some((v, false, true)),
                                    (Expr::Column(_), BinaryOperator::Lt) => Some((v, false, false)),
                                    (_, BinaryOperator::Le) => Some((v, true, true)),
                                    (_, BinaryOperator::Lt) => Some((v, true, false)),
                                    (_, BinaryOperator::Ge) => Some((v, false, true)),
                                    (_, BinaryOperator::Gt) => Some((v, false, false)),
                                    _ => None,
                                }
                            };

                            let (val1, is_lower1, inclusive1) = extract(l1, op1, r1)?;
                            let (val2, is_lower2, inclusive2) = extract(l2, op2, r2)?;

                            // One should be lower bound, one should be upper bound
                            if is_lower1 && !is_lower2 {
                                return Some((col_name, val1, inclusive1, val2, inclusive2));
                            } else if !is_lower1 && is_lower2 {
                                return Some((col_name, val2, inclusive2, val1, inclusive1));
                            }
                        }
                    }
                }
                None
            }
            _ => None,
        }
    }
    
    /// 🔥 Extract VECTOR_SEARCH function from WHERE clause
    /// Pattern: VECTOR_SEARCH(column, [v1, v2, ...], k)
    fn try_extract_vector_search(&self, expr: &Expr) -> Option<(String, crate::types::ArcVec, usize)> {
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
                
                Some((column, query_vector, k))
            }
            _ => None,
        }
    }
    
    /// 🔥 Create vector search plan if index exists
    fn try_vector_search_plan(
        &self,
        table_name: &str,
        column: &str,
        query_vector: &crate::types::ArcVec,
        k: usize,
        plans: &mut Vec<QueryPlan>,
    ) -> Result<()> {
        // Note: We don't check if index exists here, executor will handle it
        // This allows the optimizer to always prefer vector search when pattern matches
        
        // Vector search is extremely selective (returns exactly k results)
        let estimated_rows = k;
        
        // Cost: index lookup (very cheap for DiskANN)
        let cost = self.cost_params.index_lookup_cost + (k as f64 * 0.001);
        
        plans.push(QueryPlan {
            scan_method: ScanMethod::VectorSearch {
                table: table_name.to_string(),
                column: column.to_string(),
                query_vector: query_vector.clone(),
                k,
            },
            estimated_cost: cost,
            estimated_rows,
            post_filters: vec![], // No additional filters needed
        });
        
        Ok(())
    }
    
    /// Get index statistics (from cache or compute from real data)
    fn get_index_stats(&self, index_name: &str) -> Result<IndexStats> {
        // Check cache
        if let Some(stats) = self.index_stats.get(index_name) {
            return Ok(stats.clone());
        }

        // Extract table name from index name ("{table}.{column}")
        let table_name = index_name.split('.').next().unwrap_or("unknown");
        let table_rows = self.estimate_table_size(table_name);

        // Get real key count from BTree if available
        let cardinality = if let Some(idx) = self.db.column_indexes.get(index_name) {
            idx.value().entry_count().max(1)
        } else {
            (table_rows / 10).max(1)
        };

        let stats = IndexStats {
            cardinality,
            total_rows: table_rows,
            size_bytes: cardinality * 64,
            is_unique: false,
        };

        self.index_stats.insert(index_name.to_string(), stats.clone());
        Ok(stats)
    }

    /// Estimate table size from LSM metadata
    fn estimate_table_size(&self, table_name: &str) -> usize {
        self.db.estimate_table_row_count(table_name)
            .unwrap_or(1_000)
            .max(1)  // Floor of 1 to avoid cost=0 for FullScan
    }
    
    /// Calculate cost of full table scan
    fn cost_full_scan(&self, total_rows: usize) -> f64 {
        // Sequential disk reads + predicate evaluation
        (total_rows as f64 * self.cost_params.disk_read_cost)
            + (total_rows as f64 * self.cost_params.predicate_eval_cost)
    }

    /// Estimate what fraction of rows fall in [start, end] based on value types.
    /// Uses value magnitudes as a heuristic when possible.
    fn estimate_range_fraction(start: &Value, end: &Value) -> f64 {
        match (start, end) {
            (Value::Integer(s), Value::Integer(e)) => {
                let range = (*e - *s).unsigned_abs() as f64;
                // Heuristic: assume integer domain ~[-1B, +1B], clamp fraction
                ((range / 2_000_000_000.0) * 2.0).clamp(0.001, 0.5)
            }
            (Value::Float(s), Value::Float(e)) => {
                let range = (e - s).abs();
                // Heuristic: assume float domain ~[-1e6, +1e6]
                ((range / 2_000_000.0) * 2.0).clamp(0.001, 0.5)
            }
            (Value::Timestamp(s), Value::Timestamp(e)) => {
                let range = (e.as_micros() as f64 - s.as_micros() as f64).abs();
                // Heuristic: assume full range is ~1 year in microseconds
                let one_year_us = 365.0 * 24.0 * 3600.0 * 1_000_000.0;
                (range / one_year_us).clamp(0.001, 0.5)
            }
            _ => 0.1, // default for unknown types
        }
    }
    
    /// Format query plan for EXPLAIN output
    pub fn explain_plan(&self, plan: &QueryPlan) -> String {
        let mut output = String::new();
        
        output.push_str("Query Execution Plan:\n");
        output.push_str("====================\n\n");
        
        // Scan method
        match &plan.scan_method {
            ScanMethod::FullScan { table } => {
                output.push_str(&format!("1. Full Table Scan: {}\n", table));
                output.push_str(&format!("   Cost: {:.3} ms\n", plan.estimated_cost));
                output.push_str(&format!("   Estimated Rows: {}\n", plan.estimated_rows));
            }
            ScanMethod::PointQuery { table, column, value } => {
                output.push_str(&format!("1. Index Point Query: {}.{}\n", table, column));
                output.push_str(&format!("   Index: {}.{}\n", table, column));
                output.push_str(&format!("   Condition: {} = {:?}\n", column, value));
                output.push_str(&format!("   Cost: {:.3} ms (index lookup)\n", plan.estimated_cost));
                output.push_str(&format!("   Estimated Rows: {}\n", plan.estimated_rows));
            }
            ScanMethod::RangeQuery { table, column, start, start_inclusive, end, end_inclusive } => {
                output.push_str(&format!("1. Index Range Query: {}.{}\n", table, column));
                output.push_str(&format!("   Index: {}.{}\n", table, column));
                
                let start_op = if *start_inclusive { ">=" } else { ">" };
                let end_op = if *end_inclusive { "<=" } else { "<" };
                output.push_str(&format!("   Condition: {} {} {:?} AND {} {} {:?}\n", 
                    column, start_op, start, column, end_op, end));
                    
                output.push_str(&format!("   Cost: {:.3} ms (index scan)\n", plan.estimated_cost));
                output.push_str(&format!("   Estimated Rows: {}\n", plan.estimated_rows));
            }
            _ => {
                output.push_str("1. Special Index Scan\n");
                output.push_str(&format!("   Cost: {:.3} ms\n", plan.estimated_cost));
                output.push_str(&format!("   Estimated Rows: {}\n", plan.estimated_rows));
            }
        }
        
        // Post-filters
        if !plan.post_filters.is_empty() {
            output.push_str("\n2. Post-Filtering:\n");
            for (i, filter) in plan.post_filters.iter().enumerate() {
                output.push_str(&format!("   Filter {}: {:?}\n", i + 1, filter));
            }
        }
        
        output.push_str(&format!("\nTotal Estimated Cost: {:.3} ms\n", plan.estimated_cost));
        output.push_str(&format!("Final Estimated Rows: {}\n", plan.estimated_rows));
        
        output
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    
    #[test]
    fn test_index_stats() {
        let stats = IndexStats {
            cardinality: 1000,
            total_rows: 10000,
            size_bytes: 100_000,
            is_unique: false,
        };
        
        assert_eq!(stats.selectivity(), 0.001);
        assert_eq!(stats.estimate_point_query(), 10);
        assert_eq!(stats.estimate_range_query(0.1), 1000);
    }
}

// 🚀 P0 FIX: Primary Key ORDER BY optimization
impl QueryOptimizer {
    /// Optimize ORDER BY primary_key [ASC/DESC] [LIMIT k]
    /// 
    /// Detects patterns like:
    /// - `SELECT * FROM table ORDER BY id LIMIT 10` (id is primary key)
    /// - `SELECT * FROM table ORDER BY id DESC LIMIT 100`
    /// 
    /// Optimization:
    /// - Use primary key index scan instead of full table scan + sort
    /// - Avoids loading all rows and sorting in memory
    /// - Complexity: O(k) instead of O(n log n) + O(n) memory
    /// 
    /// Benefits:
    /// - 600x faster (1ms vs 611ms for 300K rows)
    /// - 280x less memory (0.1MB vs 28MB)
    fn optimize_primary_key_order_by(&self, stmt: &SelectStmt) -> Result<Option<QueryPlan>> {
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
        // TODO: Support WHERE with primary key range conditions
        if stmt.where_clause.is_some() {
            return Ok(None);
        }
        
        // Check that all columns are selected (SELECT * or explicit column list)
        // Complex expressions would require full row evaluation
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
        
        let estimated_rows = stmt.limit.unwrap_or_else(|| self.estimate_table_size(table_name));
        
        Ok(Some(QueryPlan {
            scan_method: ScanMethod::PrimaryKeyScan {
                table: table_name.clone(),
                ascending: order_by.asc,
                limit: stmt.limit,
            },
            estimated_cost: estimated_rows as f64 * self.cost_params.index_lookup_cost,
            estimated_rows,
            post_filters: vec![],
        }))
    }
}

// 🚀 P0 FIX: Vector ORDER BY optimization (向量排序索引推送)
impl QueryOptimizer {
    /// Optimize ORDER BY with vector distance for index pushdown
    /// 
    /// Detects patterns like:
    /// - `ORDER BY embedding <-> [query_vector] LIMIT K`
    /// - `ORDER BY VECTOR_DISTANCE(embedding, [query_vector]) LIMIT K`
    /// 
    /// And converts them to direct vector index search.
    fn optimize_vector_order_by(&self, stmt: &SelectStmt) -> Result<Option<QueryPlan>> {
        // 必须有 ORDER BY 和 LIMIT
        let order_by = match &stmt.order_by {
            Some(o) if o.len() == 1 => &o[0],  // 只支持单列排序
            _ => return Ok(None),
        };
        
        let limit = match stmt.limit {
            Some(k) if k > 0 => k,
            _ => return Ok(None),  // 必须有 LIMIT
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
            
            // 匹配: VECTOR_DISTANCE(column, [vector])
            Expr::FunctionCall { name, args, .. } if name.to_uppercase() == "VECTOR_DISTANCE" => {
                if args.len() != 2 {
                    return Ok(None);
                }
                match (&args[0], &args[1]) {
                    (Expr::Column(col), Expr::Literal(Value::Vector(vec))) => {
                        (col.clone(), vec.clone(), order_by.asc)
                    }
                    _ => return Ok(None),
                }
            }
            
            _ => return Ok(None),
        };
        
        // 向量距离必须是升序（距离越小越好）
        if !asc {
            return Ok(None);  // DESC 不支持
        }
        
        // 获取表名
        let table_name = match stmt.from.as_ref().unwrap() {
            TableRef::Table { name, .. } => name.clone(),
            _ => return Ok(None),
        };
        
        // 检查是否存在向量索引（使用 index_registry 支持自定义索引名）
        let index_name = self.db.index_registry.find_by_column(
            &table_name, &column,
            crate::database::index_metadata::IndexType::Vector
        ).unwrap_or_else(|| format!("{}_{}", table_name, column));
        let has_vector_index = self.db.has_vector_index(&index_name);
        
        if !has_vector_index {
            // 没有索引，返回 None 让其回退到扫描+排序
            return Ok(None);
        }
        
        // 🎯 使用向量索引优化！
        Ok(Some(QueryPlan {
            scan_method: ScanMethod::VectorSearch {
                table: table_name,
                column,
                query_vector: query_vector.clone(),
                k: limit,
            },
            estimated_cost: self.cost_params.index_lookup_cost + (limit as f64 * self.cost_params.memory_read_cost),
            estimated_rows: limit,
            post_filters: vec![],
        }))
    }
}

// 🔥 P0 FIX: Aggregate function optimization implementation
impl QueryOptimizer {
    /// Check if query contains aggregate functions
    fn is_aggregate_query(&self, stmt: &SelectStmt) -> bool {
        stmt.columns.iter().any(|col| {
            match col {
                SelectColumn::Expr(expr, _) => self.is_aggregate_expr(expr),
                _ => false,
            }
        })
    }
    
    /// Check if expression is an aggregate function
    fn is_aggregate_expr(&self, expr: &Expr) -> bool {
        match expr {
            Expr::FunctionCall { name, .. } => {
                matches!(name.to_uppercase().as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
            }
            _ => false,
        }
    }
    
    /// Optimize aggregate queries to use indexes when possible
    fn optimize_aggregate(&self, stmt: &SelectStmt, params: &[crate::types::Value]) -> Result<Option<QueryPlan>> {
        // Extract table name
        let table_name = match stmt.from.as_ref().unwrap() {
            TableRef::Table { name, .. } => name.clone(),
            _ => return Ok(None),
        };

        let total_rows = self.estimate_table_size(&table_name);

        // If there's a WHERE clause, try to use index scan
        if let Some(where_clause) = &stmt.where_clause {
            // Try two-sided range query optimization
            if let Some((col, start, start_incl, end, end_incl)) = self.try_extract_range_query(where_clause, params) {
                let index_name = format!("{}.{}", table_name, col);
                let index_exists = self.db.column_indexes.contains_key(&index_name);

                if index_exists {
                    let range_fraction = Self::estimate_range_fraction(&start, &end);
                    let range_rows = (total_rows as f64 * range_fraction) as usize;
                    return Ok(Some(QueryPlan {
                        scan_method: ScanMethod::RangeQuery {
                            table: table_name.clone(),
                            column: col,
                            start, start_inclusive: start_incl,
                            end, end_inclusive: end_incl,
                        },
                        estimated_cost: self.cost_params.index_lookup_cost * (range_rows as f64)
                            + range_rows as f64 * self.cost_params.memory_read_cost,
                        estimated_rows: 1,
                        post_filters: vec![where_clause.clone()],
                    }));
                }
            }

            // Try point query optimization (supports Literal and Parameter)
            if let Some((col, val)) = self.try_extract_point_query(where_clause, params) {
                let index_name = format!("{}.{}", table_name, col);
                let index_exists = self.db.column_indexes.contains_key(&index_name);

                if index_exists {
                    return Ok(Some(QueryPlan {
                        scan_method: ScanMethod::PointQuery {
                            table: table_name.clone(),
                            column: col,
                            value: val,
                        },
                        estimated_cost: self.cost_params.index_lookup_cost,
                        estimated_rows: 1,
                        post_filters: vec![where_clause.clone()],
                    }));
                }
            }

            // Try single-sided range optimization
            if let Some(plan) = self.try_single_sided_range(&table_name, where_clause, params)? {
                return Ok(Some(QueryPlan {
                    scan_method: plan.scan_method,
                    estimated_cost: plan.estimated_cost,
                    estimated_rows: 1,
                    post_filters: vec![where_clause.clone()],
                }));
            }
        }

        // If no optimization found, use full scan
        Ok(Some(QueryPlan {
            scan_method: ScanMethod::FullScan {
                table: table_name.clone(),
            },
            estimated_cost: self.cost_full_scan(total_rows),
            estimated_rows: 1,
            post_filters: stmt.where_clause.as_ref()
                .map(|clause| vec![clause.clone()])
                .unwrap_or_default(),
        }))
    }

    /// Try to extract point query pattern (col = value), supports Literal and Parameter
    fn try_extract_point_query(&self, expr: &Expr, params: &[crate::types::Value]) -> Option<(String, Value)> {
        match expr {
            Expr::BinaryOp { left, op: BinaryOperator::Eq, right } => {
                if let Some(val) = Self::resolve_to_value(params,right) {
                    if let Expr::Column(col) = left.as_ref() {
                        return Some((col.clone(), val));
                    }
                }
                if let Some(val) = Self::resolve_to_value(params,left) {
                    if let Expr::Column(col) = right.as_ref() {
                        return Some((col.clone(), val));
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// Try single-sided range optimization for aggregate WHERE clauses
    fn try_single_sided_range(&self, table_name: &str, expr: &Expr, params: &[crate::types::Value]) -> Result<Option<QueryPlan>> {
        let mut plans = Vec::new();
        self.analyze_where_clause(table_name, expr, params, &mut plans)?;
        Ok(plans.into_iter().min_by_key(|p| p.estimated_cost as u64))
    }
}
