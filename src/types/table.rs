/// Table metadata and schema definitions for SQL engine integration
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Column data type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnType {
    /// Timestamp (i64 microseconds)
    Timestamp,
    /// Text/String
    Text,
    /// Tensor/Vector with dimension
    Tensor(usize),
    /// Integer
    Integer,
    /// Float
    Float,
    /// Boolean
    Boolean,
    /// Spatial (Geometry type for 2D/3D points, polygons, etc.)
    Spatial,
}

/// Column definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column name
    pub name: String,
    /// Column data type
    pub col_type: ColumnType,
    /// Position in Row (0-indexed)
    pub position: usize,
    /// Whether this column is nullable
    pub nullable: bool,
    /// 🚀 AUTO_INCREMENT flag (only for primary key)
    /// 
    /// When true:
    /// - INSERT ignores user-provided value, uses row_id instead
    /// - SELECT can skip column index (value == row_id)
    /// - Performance: 20ms → < 5ms for point queries
    #[serde(default)]
    pub auto_increment: bool,
    /// 🚀 Phase 4: AUTO_INCREMENT 起始值 (默认为 1)
    #[serde(default)]
    pub auto_increment_start: Option<i64>,
}

/// Alias for SQL compatibility
pub type Column = ColumnDef;

impl ColumnDef {
    pub fn new(name: String, col_type: ColumnType, position: usize) -> Self {
        Self {
            name,
            col_type,
            position,
            nullable: true,
            auto_increment: false,
            auto_increment_start: None,
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }
    
    /// 🚀 Mark this column as AUTO_INCREMENT
    pub fn auto_increment(mut self) -> Self {
        self.auto_increment = true;
        self
    }
    
    /// 🚀 Phase 4: Set AUTO_INCREMENT starting value
    pub fn auto_increment_with_start(mut self, start: i64) -> Self {
        self.auto_increment = true;
        self.auto_increment_start = Some(start);
        self
    }
}

/// Index type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IndexType {
    /// B-Tree index (general purpose)
    BTree,
    /// Full-text search index
    FullText,
    /// Vector similarity index (DiskANN)
    Vector { dimension: usize },
    /// Spatial index (Grid + R-Tree)
    Spatial,
    /// Timestamp index (B+Tree)
    Timestamp,
}

/// Index definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    /// Index name (must be unique in database)
    pub name: String,
    /// Table name
    pub table_name: String,
    /// Column name
    pub column_name: String,
    /// Index type
    pub index_type: IndexType,
}

impl IndexDef {
    pub fn new(
        name: String,
        table_name: String,
        column_name: String,
        index_type: IndexType,
    ) -> Self {
        Self {
            name,
            table_name,
            column_name,
            index_type,
        }
    }
}

/// Table schema definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    /// Table name
    pub name: String,
    /// Column definitions (ordered)
    pub columns: Vec<ColumnDef>,
    /// Index definitions
    pub indexes: Vec<IndexDef>,
    /// Primary key column name (optional)
    pub primary_key_column: Option<String>,
    /// 🚀 AUTO_INCREMENT flag for primary key (optimization hint)
    /// 
    /// When true:
    /// - Primary key value == row_id (always)
    /// - No need to maintain column index for primary key
    /// - Point queries can directly use PK value as row_id
    #[serde(default)]
    pub primary_key_auto_increment: bool,
    /// 🚀 Phase 4: AUTO_INCREMENT 起始值 (默认为 1)
    #[serde(default)]
    pub auto_increment_start: Option<i64>,
    /// Column name -> position mapping
    #[serde(skip)]
    column_map: HashMap<String, usize>,
}

impl TableSchema {
    /// Create a new table schema
    pub fn new(name: String, columns: Vec<ColumnDef>) -> Self {
        let mut column_map = HashMap::new();
        for col in &columns {
            column_map.insert(col.name.clone(), col.position);
        }

        Self {
            name,
            columns,
            indexes: Vec::new(),
            primary_key_column: None,
            primary_key_auto_increment: false,
            auto_increment_start: None,
            column_map,
        }
    }
    
    /// Create a new table schema with primary key
    pub fn with_primary_key(mut self, pk_column: String) -> Self {
        self.primary_key_column = Some(pk_column);
        self
    }
    
    /// 🚀 Mark primary key as AUTO_INCREMENT
    pub fn with_auto_increment(mut self) -> Self {
        self.primary_key_auto_increment = true;
        
        // Also mark the column itself
        if let Some(pk_col_name) = &self.primary_key_column {
            if let Some(col) = self.columns.iter_mut().find(|c| &c.name == pk_col_name) {
                col.auto_increment = true;
            }
        }
        
        self
    }
    
    /// 🚀 Phase 4: Mark primary key as AUTO_INCREMENT with custom start value
    pub fn with_auto_increment_start(mut self, start: i64) -> Self {
        self.primary_key_auto_increment = true;
        self.auto_increment_start = Some(start);
        
        // Also mark the column itself
        if let Some(pk_col_name) = &self.primary_key_column {
            if let Some(col) = self.columns.iter_mut().find(|c| &c.name == pk_col_name) {
                col.auto_increment = true;
                col.auto_increment_start = Some(start);
            }
        }
        
        self
    }
    
    /// 🚀 Phase 4: Get AUTO_INCREMENT starting value
    pub fn get_auto_increment_start(&self) -> i64 {
        self.auto_increment_start.unwrap_or(1)
    }
    
    /// Get primary key column name
    pub fn primary_key(&self) -> Option<&str> {
        self.primary_key_column.as_deref()
    }
    
    /// 🚀 Check if primary key is AUTO_INCREMENT
    pub fn is_primary_key_auto_increment(&self) -> bool {
        self.primary_key_auto_increment
    }

    /// Add an index to the table
    pub fn add_index(&mut self, index: IndexDef) {
        self.indexes.push(index);
    }

    /// Get column by name
    pub fn get_column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Get column position by name
    pub fn get_column_position(&self, name: &str) -> Option<usize> {
        self.column_map.get(name).copied()
    }

    /// Get number of columns
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Rebuild column map (call after deserialization)
    pub fn rebuild_column_map(&mut self) {
        self.column_map.clear();
        for col in &self.columns {
            self.column_map.insert(col.name.clone(), col.position);
        }
    }

    /// Validate a row against this schema
    pub fn validate_row(&self, row: &[crate::types::Value]) -> Result<(), String> {
        if row.len() != self.columns.len() {
            return Err(format!(
                "Column count mismatch: expected {}, got {}",
                self.columns.len(),
                row.len()
            ));
        }

        for (i, col) in self.columns.iter().enumerate() {
            let value = &row[i];
            
            // Check null constraint
            if !col.nullable && matches!(value, crate::types::Value::Text(t) if t.is_empty()) {
                return Err(format!("Column '{}' cannot be null", col.name));
            }

            // Type checking
            let type_match = match (&col.col_type, value) {
                // New SQL types
                (ColumnType::Integer, crate::types::Value::Integer(_)) => true,
                (ColumnType::Float, crate::types::Value::Float(_)) => true,
                (ColumnType::Float, crate::types::Value::Integer(_)) => true, // Allow integer to float conversion
                (ColumnType::Boolean, crate::types::Value::Bool(_)) => true,
                (ColumnType::Text, crate::types::Value::Text(_)) => true,
                (ColumnType::Spatial, crate::types::Value::Spatial(_)) => true,
                
                // Legacy types
                (ColumnType::Timestamp, crate::types::Value::Timestamp(_)) => true,
                (ColumnType::Tensor(dim), crate::types::Value::Tensor(t)) => t.dimension() == *dim,
                (ColumnType::Tensor(dim), crate::types::Value::Vector(v)) => v.len() == *dim,
                
                // Backward compatibility
                (ColumnType::Integer, crate::types::Value::Timestamp(_)) => true,
                (ColumnType::Float, crate::types::Value::Tensor(t)) if t.dimension() == 1 => true, // Single float can be stored as 1D tensor
                
                _ => false,
            };

            if !type_match {
                return Err(format!(
                    "Type mismatch for column '{}': expected {:?}",
                    col.name, col.col_type
                ));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Timestamp, Value};

    #[test]
    fn test_column_def() {
        let col = ColumnDef::new("id".into(), ColumnType::Integer, 0).not_null();
        assert_eq!(col.name, "id");
        assert_eq!(col.position, 0);
        assert!(!col.nullable);
    }

    #[test]
    fn test_table_schema() {
        let mut schema = TableSchema::new(
            "users".into(),
            vec![
                ColumnDef::new("id".into(), ColumnType::Integer, 0).not_null(),
                ColumnDef::new("name".into(), ColumnType::Text, 1),
                ColumnDef::new("created_at".into(), ColumnType::Timestamp, 2),
            ],
        );

        assert_eq!(schema.column_count(), 3);
        assert_eq!(schema.get_column_position("name"), Some(1));
        
        // Add index
        schema.add_index(IndexDef::new(
            "users_name_idx".into(),
            "users".into(),
            "name".into(),
            IndexType::FullText,
        ));
        
        assert_eq!(schema.indexes.len(), 1);
    }

    #[test]
    fn test_validate_row() {
        let schema = TableSchema::new(
            "test".into(),
            vec![
                ColumnDef::new("id".into(), ColumnType::Timestamp, 0),
                ColumnDef::new("name".into(), ColumnType::Text, 1),
            ],
        );

        // Valid row
        let row = vec![
            Value::Timestamp(Timestamp::from_micros(123)),
            Value::Text("test".to_string()),
        ];
        assert!(schema.validate_row(&row).is_ok());

        // Invalid: wrong column count
        let row = vec![Value::Timestamp(Timestamp::from_micros(123))];
        assert!(schema.validate_row(&row).is_err());
    }
}
