/// Index Nested Loop Join implementation
/// 
/// Algorithm:
/// FOR each row in outer table:
///     key = row[join_column]
///     matches = inner_table.index_lookup(key)  // O(log n)
///     OUTPUT matches
/// 
/// Time complexity: O(n log m)
/// Space complexity: O(1) (除了结果集)

use std::sync::Arc;
use crate::database::MoteDB;
use crate::types::{Value, SqlRow};
use crate::error::Result;

/// Index nested loop join executor
pub struct IndexNestedLoopJoin {
    db: Arc<MoteDB>,
}

impl IndexNestedLoopJoin {
    pub fn new(db: Arc<MoteDB>) -> Self {
        Self { db }
    }
    
    /// Execute index nested loop join
    /// 
    /// # Arguments
    /// * `outer_table` - Outer (driving) table name
    /// * `inner_table` - Inner table name (with index)
    /// * `join_column` - Join column name
    /// * `outer_rows` - Pre-scanned outer table rows
    /// 
    /// # Returns
    /// Vector of joined rows
    pub fn execute(
        &self,
        outer_table: &str,
        inner_table: &str,
        join_column: &str,
        outer_rows: Vec<SqlRow>,
    ) -> Result<Vec<SqlRow>> {
        // 🚀 P1 优化：预分配容量（估算每行匹配 1 个）
        let mut results = Vec::with_capacity(outer_rows.len());
        
        // Try to find index on inner table's join column
        let has_index = self.check_index_exists(inner_table, join_column);
        
        if !has_index {
            // No index: fallback to nested loop join
            return self.nested_loop_join(outer_rows, inner_table, join_column);
        }
        
        // Index exists: use index lookup for each outer row
        for outer_row in outer_rows {
            if let Some(key) = outer_row.get(join_column) {
                // Index lookup (O(log n))
                match self.index_lookup(inner_table, join_column, key) {
                    Ok(inner_row_ids) => {
                        // Load matching inner rows
                        for row_id in inner_row_ids {
                            if let Ok(Some(inner_row_data)) = self.db.get_table_row(inner_table, row_id) {
                                // Convert Vec<Value> to SqlRow
                                let inner_row = self.vec_to_sql_row(&inner_row_data, inner_table)?;
                                
                                // Merge outer and inner rows
                                let merged = Self::merge_rows(&outer_row, &inner_row);
                                results.push(merged);
                            }
                        }
                    }
                    Err(_) => {
                        // Index lookup failed, skip this row
                        continue;
                    }
                }
            }
        }
        
        Ok(results)
    }
    
    /// Check if index exists
    fn check_index_exists(&self, table_name: &str, column_name: &str) -> bool {
        // 尝试查询一个虚拟值来检测索引是否存在
        self.db.query_by_column(table_name, column_name, &Value::Integer(0)).is_ok()
    }
    
    /// Index lookup: query by column value
    fn index_lookup(&self, table_name: &str, column_name: &str, key: &Value) -> Result<Vec<u64>> {
        self.db.query_by_column(table_name, column_name, key)
    }
    
    /// Fallback: nested loop join (O(n*m))
    fn nested_loop_join(
        &self,
        outer_rows: Vec<SqlRow>,
        inner_table: &str,
        join_column: &str,
    ) -> Result<Vec<SqlRow>> {
        // 🚀 P1 优化：预分配容量（嵌套循环可能返回很多行）
        let mut results = Vec::with_capacity(outer_rows.len() * 2);
        
        // Scan inner table (estimate max 100K rows)
        let max_rows = 100_000_u64;
        
        for outer_row in outer_rows {
            let outer_key = outer_row.get(join_column);
            
            for row_id in 0..max_rows {
                match self.db.get_table_row(inner_table, row_id) {
                    Ok(Some(inner_row_data)) => {
                        let inner_row = self.vec_to_sql_row(&inner_row_data, inner_table)?;
                        
                        // Check if join keys match
                        if let Some(inner_key) = inner_row.get(join_column) {
                            if outer_key == Some(inner_key) {
                                let merged = Self::merge_rows(&outer_row, &inner_row);
                                results.push(merged);
                            }
                        }
                    }
                    Ok(None) => break, // No more rows
                    Err(_) => break,   // Error or end of table
                }
            }
        }
        
        Ok(results)
    }
    
    /// Convert Vec<Value> to SqlRow
    fn vec_to_sql_row(&self, values: &[Value], table_name: &str) -> Result<SqlRow> {
        let schema = self.db.get_table_schema(table_name)?;
        let mut row = SqlRow::new();
        
        for (i, col) in schema.columns.iter().enumerate() {
            if i < values.len() {
                let qualified_name = format!("{}.{}", table_name, col.name);
                row.insert(qualified_name, values[i].clone());
            }
        }
        
        Ok(row)
    }
    
    /// Merge two rows
    /// 🚀 P2 优化：使用移动语义和预分配
    fn merge_rows(outer_row: &SqlRow, inner_row: &SqlRow) -> SqlRow {
        // 预分配容量避免 realloc
        let mut merged = SqlRow::with_capacity(outer_row.len() + inner_row.len());
        
        // Clone outer_row（必须）
        for (col, val) in outer_row.iter() {
            merged.insert(col.clone(), val.clone());
        }
        
        // Clone inner_row（必须）
        for (col, val) in inner_row.iter() {
            merged.insert(col.clone(), val.clone());
        }
        
        merged
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_index_join_basic() {
        // Integration test - requires real database setup
        // See examples/index_join_test.rs for full test
    }
}
