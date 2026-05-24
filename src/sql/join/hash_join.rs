/// Hash Join implementation
/// 
/// Algorithm:
/// 1. Build phase: construct hash table from smaller table
/// 2. Probe phase: scan larger table and probe hash table
/// 
/// Time complexity: O(n + m)
/// Space complexity: O(min(n, m))

use std::collections::HashMap;
use crate::types::{Value, SqlRow};
use crate::error::Result;

/// Hash key wrapper (supports Eq + Hash)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum HashKey {
    Integer(i64),
    Float(u64), // f64 bits for hashable representation
    Text(String),
    Bool(bool),
}

impl HashKey {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Integer(i) => Some(HashKey::Integer(*i)),
            Value::Float(f) => {
                let canonical = if *f == 0.0 { 0.0f64 } else { *f };
                Some(HashKey::Float(canonical.to_bits()))
            }
            Value::Text(s) => Some(HashKey::Text(s.clone())),
            Value::Bool(b) => Some(HashKey::Bool(*b)),
            Value::Null => None, // SQL: NULL != NULL in joins
            _ => None, // Vector/Tensor etc. cannot hash directly
        }
    }
}

/// Hash join executor
pub struct HashJoinExecutor {
    /// Hash table: join key -> rows
    hash_table: HashMap<HashKey, Vec<SqlRow>>,
}

impl HashJoinExecutor {
    pub fn new() -> Self {
        Self {
            hash_table: HashMap::new(),
        }
    }
    
    /// Build phase: construct hash table from build-side table
    /// 
    /// # Arguments
    /// * `rows` - Rows from the build-side table (smaller table)
    /// * `key_col` - Join column name
    pub fn build(&mut self, rows: Vec<SqlRow>, key_col: &str) -> Result<()> {
        for row in rows {
            if let Some(value) = row.get(key_col) {
                if let Some(key) = HashKey::from_value(value) {
                    self.hash_table
                        .entry(key)
                        .or_default()
                        .push(row);
                }
            }
        }
        Ok(())
    }
    
    /// Probe phase: scan probe-side table and find matches
    /// 
    /// # Arguments
    /// * `rows` - Rows from the probe-side table (larger table)
    /// * `key_col` - Join column name
    /// 
    /// # Returns
    /// Vector of joined rows
    pub fn probe(&self, rows: Vec<SqlRow>, key_col: &str) -> Result<Vec<SqlRow>> {
        // 🚀 P1 优化：预分配容量（估算每行匹配 1 个）
        let mut results = Vec::with_capacity(rows.len());
        
        for probe_row in rows {
            if let Some(value) = probe_row.get(key_col) {
                if let Some(key) = HashKey::from_value(value) {
                    if let Some(build_rows) = self.hash_table.get(&key) {
                        // Found match(es) in hash table
                        for build_row in build_rows {
                            // Merge build row and probe row
                            let merged = Self::merge_rows(build_row, &probe_row);
                            results.push(merged);
                        }
                    }
                }
            }
        }
        
        Ok(results)
    }
    
    /// LEFT OUTER JOIN probe
    /// Returns all probe rows, with NULLs for non-matching build rows
    pub fn probe_left(&self, rows: Vec<SqlRow>, key_col: &str, build_columns: &[String]) -> Result<Vec<SqlRow>> {
        let mut results = Vec::with_capacity(rows.len());

        for probe_row in rows {
            let mut matched = false;
            if let Some(value) = probe_row.get(key_col) {
                if let Some(key) = HashKey::from_value(value) {
                    if let Some(build_rows) = self.hash_table.get(&key) {
                        for build_row in build_rows {
                            let merged = Self::merge_rows(build_row, &probe_row);
                            results.push(merged);
                            matched = true;
                        }
                    }
                }
            }
            if !matched {
                // No match: add probe row with NULLs for build columns
                let mut merged = SqlRow::with_capacity(probe_row.len() + build_columns.len());
                for (col, val) in probe_row.iter() {
                    merged.insert(col.clone(), val.clone());
                }
                for col in build_columns {
                    merged.insert(col.clone(), Value::Null);
                }
                results.push(merged);
            }
        }

        Ok(results)
    }
    
    /// Merge two rows (build row + probe row)
    /// 🚀 P2 优化：使用移动语义减少 clone
    fn merge_rows(build_row: &SqlRow, probe_row: &SqlRow) -> SqlRow {
        // 预分配容量避免 realloc
        let mut merged = SqlRow::with_capacity(build_row.len() + probe_row.len());
        
        // Clone build_row（必须）
        for (col, val) in build_row.iter() {
            merged.insert(col.clone(), val.clone());
        }
        
        // Clone probe_row（必须）
        for (col, val) in probe_row.iter() {
            merged.insert(col.clone(), val.clone());
        }
        
        merged
    }
    
    /// Get total rows in hash table
    pub fn total_rows(&self) -> usize {
        self.hash_table.values().map(|v| v.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    fn make_row(id: i64, name: &str) -> SqlRow {
        let mut row = SqlRow::new();
        row.insert("id".to_string(), Value::Integer(id));
        row.insert("name".to_string(), Value::Text(name.to_string()));
        row
    }
    
    fn make_order_row(order_id: i64, user_id: i64, amount: i64) -> SqlRow {
        let mut row = SqlRow::new();
        row.insert("order_id".to_string(), Value::Integer(order_id));
        row.insert("user_id".to_string(), Value::Integer(user_id));
        row.insert("amount".to_string(), Value::Integer(amount));
        row
    }
    
    #[test]
    fn test_hash_join_basic() {
        let mut executor = HashJoinExecutor::new();
        
        // Build: users table
        let users = vec![
            make_row(1, "Alice"),
            make_row(2, "Bob"),
        ];
        executor.build(users, "id").unwrap();
        
        // Probe: orders table
        let orders = vec![
            make_order_row(101, 1, 100),
            make_order_row(102, 2, 200),
        ];
        let results = executor.probe(orders, "user_id").unwrap();
        
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get("name"), Some(&Value::Text("Alice".to_string())));
        assert_eq!(results[0].get("amount"), Some(&Value::Integer(100)));
    }
    
    #[test]
    fn test_hash_join_no_match() {
        let mut executor = HashJoinExecutor::new();
        
        let users = vec![make_row(1, "Alice")];
        executor.build(users, "id").unwrap();
        
        let orders = vec![make_order_row(101, 999, 100)];
        let results = executor.probe(orders, "user_id").unwrap();
        
        assert_eq!(results.len(), 0); // No match
    }
    
    #[test]
    fn test_hash_join_multiple_matches() {
        let mut executor = HashJoinExecutor::new();
        
        let users = vec![make_row(1, "Alice")];
        executor.build(users, "id").unwrap();
        
        // Multiple orders for same user
        let orders = vec![
            make_order_row(101, 1, 100),
            make_order_row(102, 1, 200),
        ];
        let results = executor.probe(orders, "user_id").unwrap();
        
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get("name"), Some(&Value::Text("Alice".to_string())));
        assert_eq!(results[1].get("name"), Some(&Value::Text("Alice".to_string())));
    }
    
    #[test]
    fn test_left_join() {
        let mut executor = HashJoinExecutor::new();
        
        let users = vec![make_row(1, "Alice")];
        executor.build(users, "id").unwrap();
        
        let orders = vec![
            make_order_row(101, 1, 100),
            make_order_row(102, 999, 200), // No matching user
        ];
        
        let build_columns = vec!["id".to_string(), "name".to_string()];
        let results = executor.probe_left(orders, "user_id", &build_columns).unwrap();
        
        assert_eq!(results.len(), 2);
        assert_eq!(results[1].get("name"), Some(&Value::Null)); // NULL for non-match
    }

    #[test]
    fn test_hash_join_float_keys() {
        // Before fix: HashKey didn't support Float, so Float column joins returned 0 results.
        let mut executor = HashJoinExecutor::new();

        let mut left1 = SqlRow::new();
        left1.insert("price".to_string(), Value::Float(9.99));
        left1.insert("item".to_string(), Value::Text("apple".to_string()));

        let mut left2 = SqlRow::new();
        left2.insert("price".to_string(), Value::Float(19.99));
        left2.insert("item".to_string(), Value::Text("banana".to_string()));

        executor.build(vec![left1, left2], "price").unwrap();

        let mut probe_row = SqlRow::new();
        probe_row.insert("price".to_string(), Value::Float(9.99));
        probe_row.insert("order_id".to_string(), Value::Integer(100));

        let results = executor.probe(vec![probe_row], "price").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("item"), Some(&Value::Text("apple".to_string())));
        assert_eq!(results[0].get("order_id"), Some(&Value::Integer(100)));
    }

    #[test]
    fn test_hash_join_float_no_false_match() {
        // Verify that 9.99 doesn't match 9.98
        let mut executor = HashJoinExecutor::new();

        let mut build_row = SqlRow::new();
        build_row.insert("val".to_string(), Value::Float(9.99));
        executor.build(vec![build_row], "val").unwrap();

        let mut probe_row = SqlRow::new();
        probe_row.insert("val".to_string(), Value::Float(9.98));
        let results = executor.probe(vec![probe_row], "val").unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_hash_join_float_zero() {
        // Verify +0.0 and -0.0 match (IEEE 754 equivalence)
        let mut executor = HashJoinExecutor::new();

        let mut build_row = SqlRow::new();
        build_row.insert("val".to_string(), Value::Float(0.0));
        executor.build(vec![build_row], "val").unwrap();

        let mut probe_row = SqlRow::new();
        probe_row.insert("val".to_string(), Value::Float(-0.0));
        let results = executor.probe(vec![probe_row], "val").unwrap();
        assert_eq!(results.len(), 1);
    }
}
