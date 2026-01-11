//! Database Table Management
//!
//! Extracted from database_legacy.rs
//! Contains table schema management and helper methods

use crate::types::{TableSchema, IndexDef, RowId};
use crate::{Result, StorageError};

use super::core::MoteDB;

impl MoteDB {
    /// Create a new table with schema
    /// 
    /// Automatically creates a primary key index if defined
    /// 
    /// # Example
    /// ```ignore
    /// use motedb::types::{TableSchema, ColumnDef, ColumnType};
    /// 
    /// let schema = TableSchema::new("users")
    ///     .with_column(ColumnDef::new("id", ColumnType::Integer).primary_key())
    ///     .with_column(ColumnDef::new("name", ColumnType::Text));
    /// 
    /// db.create_table(schema)?;
    /// ```
    pub fn create_table(&self, schema: TableSchema) -> Result<()> {
        // Register table in catalog
        self.table_registry.create_table(schema.clone())?;
        
        // 🔥 P0 FIX: Auto-create primary key index for fast lookups
        if let Some(pk_col) = schema.primary_key() {
            let index_name = format!("{}_pk_idx", schema.name);
            println!("✓ Auto-creating primary key index: {}", index_name);
            
            // Create column index for primary key
            if let Err(e) = self.create_column_index(&schema.name, pk_col) {
                eprintln!("⚠️  Warning: Failed to create primary key index: {}", e);
                // Don't fail table creation if index creation fails
            }
        }
        
        Ok(())
    }
    
    /// Drop a table
    /// 
    /// Note: This only removes the table metadata. 
    /// Existing rows and indexes are not automatically deleted.
    /// 
    /// # Example
    /// ```ignore
    /// db.drop_table("users")?;
    /// ```
    pub fn drop_table(&self, table_name: &str) -> Result<()> {
        self.table_registry.drop_table(table_name)
    }
    
    /// Get table schema
    /// 
    /// # Example
    /// ```ignore
    /// let schema = db.get_table_schema("users")?;
    /// println!("Table has {} columns", schema.column_count());
    /// ```
    pub fn get_table_schema(&self, table_name: &str) -> Result<TableSchema> {
        self.table_registry.get_table(table_name)
    }
    
    /// List all tables
    /// 
    /// # Example
    /// ```ignore
    /// let tables = db.list_tables()?;
    /// for table in tables {
    ///     println!("Table: {}", table);
    /// }
    /// ```
    pub fn list_tables(&self) -> Result<Vec<String>> {
        self.table_registry.list_tables()
    }
    
    /// Check if table exists
    /// 
    /// # Example
    /// ```ignore
    /// if db.table_exists("users") {
    ///     println!("Table exists");
    /// }
    /// ```
    pub fn table_exists(&self, table_name: &str) -> bool {
        self.table_registry.table_exists(table_name)
    }
    
    /// Add index to existing table
    /// 
    /// # Example
    /// ```ignore
    /// use motedb::types::{IndexDef, IndexType};
    /// 
    /// let index = IndexDef::new(
    ///     "users_name_idx".into(),
    ///     "users".into(),
    ///     "name".into(),
    ///     IndexType::FullText,
    /// );
    /// 
    /// db.add_table_index(index)?;
    /// ```
    pub fn add_table_index(&self, index: IndexDef) -> Result<()> {
        self.table_registry.add_index(index)
    }
    
    // ==================== Internal Helper Methods ====================
    
    /// Make composite key from table name and row ID
    /// 
    /// Format: [table_hash:32bits][row_id:32bits]
    /// 
    /// 🚀 P1: Uses DashMap cache to avoid repeated hash computation
    pub(crate) fn make_composite_key(&self, table_name: &str, row_id: RowId) -> u64 {
        // Try cache first (lock-free read)
        if let Some(table_hash) = self.table_hash_cache.get(table_name) {
            return (*table_hash.value() << 32) | (row_id & 0xFFFFFFFF);
        }
        
        // Cache miss - compute and cache
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        table_name.hash(&mut hasher);
        let table_hash = (hasher.finish() & 0xFFFFFFFF) as u64;
        
        // Cache it (atomic insert)
        self.table_hash_cache.insert(table_name.to_string(), table_hash);
        
        (table_hash << 32) | (row_id & 0xFFFFFFFF)
    }
    
    /// Compute table prefix (upper 32 bits of composite key)
    /// 
    /// 🚀 P1: Uses DashMap cache to avoid repeated hash computation
    pub(crate) fn compute_table_prefix(&self, table_name: &str) -> u64 {
        // Try cache first (lock-free read)
        if let Some(table_hash) = self.table_hash_cache.get(table_name) {
            return *table_hash.value();
        }
        
        // Cache miss - compute and cache
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        table_name.hash(&mut hasher);
        let table_hash = (hasher.finish() & 0xFFFFFFFF) as u64;
        
        // Cache it (atomic insert)
        self.table_hash_cache.insert(table_name.to_string(), table_hash);
        
        table_hash
    }
    
    /// Extract row_id from composite key
    pub(crate) fn extract_row_id(&self, composite_key: u64) -> RowId {
        composite_key & 0xFFFFFFFF
    }
    
    /// Check if composite key belongs to table
    pub(crate) fn key_matches_table(&self, composite_key: u64, table_name: &str) -> bool {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        table_name.hash(&mut hasher);
        let table_hash = (hasher.finish() & 0xFFFFFFFF) as u64;
        
        (composite_key >> 32) == table_hash
    }
    
    /// 🚀 P2: Get row cache for statistics and monitoring
    pub fn get_row_cache(&self) -> &std::sync::Arc<crate::cache::RowCache> {
        &self.row_cache
    }
    
    /// Decode row data from LSM storage format
    /// 
    /// Internal helper for query operations
    pub(crate) fn decode_row_data(data: &[u8]) -> Result<crate::types::Row> {
        bincode::deserialize(data)
            .map_err(|e| StorageError::Serialization(format!("Failed to decode row: {}", e)))
    }
    
    
    // Note: create_column_index() has been moved to indexes/column.rs
    // Removed duplicate definition to avoid E0592
}
