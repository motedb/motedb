//! Index Metadata Management
//!
//! Tracks all indexes created on tables, supporting:
//! - Custom index names
//! - Index type tracking (Column/Vector/Text/Spatial)
//! - Table/column relationships
//! - Persistent metadata storage

use crate::{Result, StorageError};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;

/// Index type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum IndexType {
    Column,
    Vector,
    Text,
    Spatial,
}

/// Index metadata entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    /// Index name (user-specified or auto-generated)
    pub name: String,
    
    /// Table name
    pub table_name: String,
    
    /// Column name
    pub column_name: String,
    
    /// Index type
    pub index_type: IndexType,
    
    /// Creation timestamp
    pub created_at: u64,
}

impl IndexMetadata {
    pub fn new(name: String, table_name: String, column_name: String, index_type: IndexType) -> Self {
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        Self {
            name,
            table_name,
            column_name,
            index_type,
            created_at,
        }
    }
}

/// Index metadata registry
pub struct IndexRegistry {
    /// Map: index_name -> IndexMetadata
    indexes: Arc<DashMap<String, IndexMetadata>>,
    
    /// Persistence path
    metadata_path: std::path::PathBuf,
}

impl IndexRegistry {
    /// Create a new index registry
    pub fn new(db_path: &Path) -> Self {
        let metadata_path = db_path.join("index_metadata.bin");
        
        Self {
            indexes: Arc::new(DashMap::new()),
            metadata_path,
        }
    }
    
    /// Load metadata from disk
    pub fn load(&self) -> Result<()> {
        if !self.metadata_path.exists() {
            return Ok(());
        }
        
        let data = std::fs::read(&self.metadata_path)
            .map_err(StorageError::Io)?;
        
        let metadata_list: Vec<IndexMetadata> = bincode::deserialize(&data)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        
        for metadata in metadata_list {
            self.indexes.insert(metadata.name.clone(), metadata);
        }
        
        Ok(())
    }
    
    /// Save metadata to disk
    pub fn save(&self) -> Result<()> {
        let metadata_list: Vec<IndexMetadata> = self.indexes.iter()
            .map(|entry| entry.value().clone())
            .collect();
        
        let data = bincode::serialize(&metadata_list)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        
        std::fs::write(&self.metadata_path, data)
            .map_err(StorageError::Io)?;
        
        Ok(())
    }
    
    /// Register a new index
    pub fn register(&self, metadata: IndexMetadata) -> Result<()> {
        // Check if index name already exists
        if self.indexes.contains_key(&metadata.name) {
            return Err(StorageError::Index(format!(
                "Index '{}' already exists",
                metadata.name
            )));
        }
        
        self.indexes.insert(metadata.name.clone(), metadata);
        self.save()?;
        
        Ok(())
    }
    
    /// Remove an index
    pub fn remove(&self, index_name: &str) -> Result<()> {
        self.indexes.remove(index_name);
        self.save()?;
        Ok(())
    }
    
    /// Get index metadata
    pub fn get(&self, index_name: &str) -> Option<IndexMetadata> {
        self.indexes.get(index_name).map(|entry| entry.value().clone())
    }
    
    /// List all indexes for a table
    pub fn list_table_indexes(&self, table_name: &str) -> Vec<IndexMetadata> {
        self.indexes.iter()
            .filter(|entry| entry.value().table_name == table_name)
            .map(|entry| entry.value().clone())
            .collect()
    }
    
    /// Find index by table and column
    pub fn find_by_column(&self, table_name: &str, column_name: &str, index_type: IndexType) -> Option<String> {
        self.indexes.iter()
            .find(|entry| {
                let meta = entry.value();
                meta.table_name == table_name 
                    && meta.column_name == column_name
                    && meta.index_type == index_type
            })
            .map(|entry| entry.key().clone())
    }
    
    /// Generate default index name
    pub fn generate_name(table_name: &str, column_name: &str, index_type: &IndexType) -> String {
        match index_type {
            IndexType::Column => format!("{}.{}", table_name, column_name),
            IndexType::Vector | IndexType::Text | IndexType::Spatial => {
                format!("{}_{}", table_name, column_name)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    
    #[test]
    fn test_index_metadata_registry() {
        let dir = tempdir().unwrap();
        let registry = IndexRegistry::new(dir.path());
        
        // Register index
        let metadata = IndexMetadata::new(
            "idx_users_age".to_string(),
            "users".to_string(),
            "age".to_string(),
            IndexType::Column,
        );
        
        registry.register(metadata.clone()).unwrap();
        
        // Get index
        let retrieved = registry.get("idx_users_age").unwrap();
        assert_eq!(retrieved.name, "idx_users_age");
        assert_eq!(retrieved.table_name, "users");
        assert_eq!(retrieved.column_name, "age");
        
        // List table indexes
        let indexes = registry.list_table_indexes("users");
        assert_eq!(indexes.len(), 1);
        
        // Find by column
        let found = registry.find_by_column("users", "age", IndexType::Column);
        assert_eq!(found, Some("idx_users_age".to_string()));
        
        // Remove index
        registry.remove("idx_users_age").unwrap();
        assert!(registry.get("idx_users_age").is_none());
    }
}
