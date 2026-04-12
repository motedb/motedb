//! Index Metadata Management
//!
//! Tracks all indexes created on tables, supporting:
//! - Custom index names
//! - Index type tracking (Column/Vector/Text/Spatial)
//! - Table/column relationships
//! - Persistent metadata storage
//! - Stale marking for indexes that failed to update

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

    /// Whether this index is stale (out-of-sync with data).
    /// Set when an index update fails; cleared on successful rebuild.
    #[serde(default)]
    pub stale: bool,

    /// Distance metric for vector indexes ("l2" or "cosine")
    #[serde(default)]
    pub metric: Option<String>,
}

impl IndexMetadata {
    pub fn new(name: String, table_name: String, column_name: String, index_type: IndexType) -> Self {
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            name,
            table_name,
            column_name,
            index_type,
            created_at,
            stale: false,
            metric: None,
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
    
    /// Save metadata to disk (atomic via temp-file rename)
    pub fn save(&self) -> Result<()> {
        let metadata_list: Vec<IndexMetadata> = self.indexes.iter()
            .map(|entry| entry.value().clone())
            .collect();

        let data = bincode::serialize(&metadata_list)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        // Write to temp file first, then rename for atomicity
        let tmp_path = self.metadata_path.with_extension("bin.tmp");
        {
            let mut f = std::fs::File::create(&tmp_path)
                .map_err(StorageError::Io)?;
            std::io::Write::write_all(&mut f, &data)
                .map_err(StorageError::Io)?;
            f.sync_all()
                .map_err(StorageError::Io)?;
        }
        std::fs::rename(&tmp_path, &self.metadata_path)
            .map_err(StorageError::Io)?;

        Ok(())
    }
    
    /// Register a new index (atomic: check + insert via DashMap entry API)
    pub fn register(&self, metadata: IndexMetadata) -> Result<()> {
        use dashmap::mapref::entry::Entry;

        match self.indexes.entry(metadata.name.clone()) {
            Entry::Occupied(_) => {
                Err(StorageError::Index(format!(
                    "Index '{}' already exists",
                    metadata.name
                )))
            }
            Entry::Vacant(entry) => {
                entry.insert(metadata);
                self.save()?;
                Ok(())
            }
        }
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

    /// Get table_name and column_name from index name
    pub fn resolve_index_name(&self, index_name: &str) -> Option<(String, String)> {
        self.indexes.get(index_name)
            .map(|entry| (entry.value().table_name.clone(), entry.value().column_name.clone()))
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

    /// Mark an index as stale (out-of-sync with data).
    /// Called when an index update fails during insert/update/delete.
    /// Stale indexes will be skipped during queries until rebuilt.
    pub fn mark_stale(&self, index_name: &str) {
        if let Some(mut entry) = self.indexes.get_mut(index_name) {
            entry.stale = true;
        }
        // Best-effort persist (ignore error — will be retried on next save)
        let _ = self.save();
    }

    /// Check whether an index is stale.
    /// Returns `true` if the index exists and is marked stale.
    /// Returns `false` if the index doesn't exist (not an error — just not tracked).
    pub fn is_stale(&self, index_name: &str) -> bool {
        self.indexes.get(index_name)
            .map(|e| e.stale)
            .unwrap_or(false)
    }

    /// Clear stale flag for an index (after successful rebuild).
    pub fn clear_stale(&self, index_name: &str) {
        if let Some(mut entry) = self.indexes.get_mut(index_name) {
            entry.stale = false;
        }
        let _ = self.save();
    }

    /// List all stale indexes for a table (useful during checkpoint rebuild).
    pub fn list_stale_indexes(&self, table_name: &str) -> Vec<IndexMetadata> {
        self.indexes.iter()
            .filter(|entry| {
                let meta = entry.value();
                meta.table_name == table_name && meta.stale
            })
            .map(|entry| entry.value().clone())
            .collect()
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
