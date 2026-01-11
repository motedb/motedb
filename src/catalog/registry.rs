/// Table registry for managing table metadata
use crate::error::{Result, StorageError};
use crate::types::{TableSchema, IndexDef};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

/// Table registry metadata (persisted to disk)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RegistryMetadata {
    /// Table name -> TableSchema
    tables: HashMap<String, TableSchema>,
    /// Index name -> (table_name, column_name)
    index_map: HashMap<String, (String, String)>,
}

/// Table registry for managing table schemas
pub struct TableRegistry {
    /// Metadata (protected by RwLock)
    metadata: Arc<RwLock<RegistryMetadata>>,
    /// Persistence file path
    persist_path: PathBuf,
}

impl TableRegistry {
    /// Create a new table registry
    pub fn new<P: AsRef<Path>>(data_dir: P) -> Result<Self> {
        let persist_path = data_dir.as_ref().join("catalog.bin");
        
        // Create directory if it doesn't exist
        if let Some(parent) = persist_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| StorageError::Io(e))?;
        }
        
        // Try to load existing metadata
        let metadata = if persist_path.exists() {
            let data = fs::read(&persist_path)
                .map_err(|e| StorageError::Io(e))?;
            let mut meta: RegistryMetadata = bincode::deserialize(&data)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            
            // Rebuild column maps after deserialization
            for schema in meta.tables.values_mut() {
                schema.rebuild_column_map();
            }
            
            meta
        } else {
            RegistryMetadata {
                tables: HashMap::new(),
                index_map: HashMap::new(),
            }
        };

        Ok(Self {
            metadata: Arc::new(RwLock::new(metadata)),
            persist_path,
        })
    }

    /// Create a new table
    pub fn create_table(&self, mut schema: TableSchema) -> Result<()> {
        let mut meta = self.metadata.write()
            .map_err(|e| StorageError::InvalidData(e.to_string()))?;

        // Check if table already exists
        if meta.tables.contains_key(&schema.name) {
            return Err(StorageError::InvalidData(format!(
                "Table '{}' already exists",
                schema.name
            )));
        }

        // Validate and register indexes
        for index in &schema.indexes {
            if meta.index_map.contains_key(&index.name) {
                return Err(StorageError::InvalidData(format!(
                    "Index '{}' already exists",
                    index.name
                )));
            }
        }

        // Rebuild column map
        schema.rebuild_column_map();

        // Register indexes
        for index in &schema.indexes {
            meta.index_map.insert(
                index.name.clone(),
                (index.table_name.clone(), index.column_name.clone()),
            );
        }

        // Insert table
        meta.tables.insert(schema.name.clone(), schema);

        // Persist to disk
        drop(meta);
        self.persist()?;

        Ok(())
    }

    /// Drop a table
    pub fn drop_table(&self, table_name: &str) -> Result<()> {
        let mut meta = self.metadata.write()
            .map_err(|e| StorageError::InvalidData(e.to_string()))?;

        // Check if table exists
        let schema = meta.tables.remove(table_name)
            .ok_or_else(|| StorageError::InvalidData(format!(
                "Table '{}' not found",
                table_name
            )))?;

        // Remove indexes
        for index in &schema.indexes {
            meta.index_map.remove(&index.name);
        }

        // Persist to disk
        drop(meta);
        self.persist()?;

        Ok(())
    }

    /// Get table schema
    pub fn get_table(&self, table_name: &str) -> Result<TableSchema> {
        let meta = self.metadata.read()
            .map_err(|e| StorageError::InvalidData(e.to_string()))?;

        meta.tables.get(table_name)
            .cloned()
            .ok_or_else(|| StorageError::InvalidData(format!(
                "Table '{}' not found",
                table_name
            )))
    }

    /// List all tables
    pub fn list_tables(&self) -> Result<Vec<String>> {
        let meta = self.metadata.read()
            .map_err(|e| StorageError::InvalidData(e.to_string()))?;

        Ok(meta.tables.keys().cloned().collect())
    }

    /// Check if table exists
    pub fn table_exists(&self, table_name: &str) -> bool {
        self.metadata.read()
            .map(|meta| meta.tables.contains_key(table_name))
            .unwrap_or(false)
    }

    /// Add index to existing table
    pub fn add_index(&self, index: IndexDef) -> Result<()> {
        let mut meta = self.metadata.write()
            .map_err(|e| StorageError::InvalidData(e.to_string()))?;

        // Check if index already exists
        if meta.index_map.contains_key(&index.name) {
            return Err(StorageError::InvalidData(format!(
                "Index '{}' already exists",
                index.name
            )));
        }

        // Check if table exists and column exists
        if !meta.tables.contains_key(&index.table_name) {
            return Err(StorageError::InvalidData(format!(
                "Table '{}' not found",
                index.table_name
            )));
        }

        if let Some(table) = meta.tables.get(&index.table_name) {
            if table.get_column(&index.column_name).is_none() {
                return Err(StorageError::InvalidData(format!(
                    "Column '{}' not found in table '{}'",
                    index.column_name, index.table_name
                )));
            }
        }

        // Register index
        meta.index_map.insert(
            index.name.clone(),
            (index.table_name.clone(), index.column_name.clone()),
        );

        // Add index to table
        if let Some(table) = meta.tables.get_mut(&index.table_name) {
            table.add_index(index);
        }

        // Persist to disk
        drop(meta);
        self.persist()?;

        Ok(())
    }

    /// Get index definition
    pub fn get_index(&self, index_name: &str) -> Result<IndexDef> {
        let meta = self.metadata.read()
            .map_err(|e| StorageError::InvalidData(e.to_string()))?;

        let (table_name, column_name) = meta.index_map.get(index_name)
            .ok_or_else(|| StorageError::InvalidData(format!(
                "Index '{}' not found",
                index_name
            )))?;

        let table = meta.tables.get(table_name)
            .ok_or_else(|| StorageError::InvalidData(format!(
                "Table '{}' not found",
                table_name
            )))?;

        table.indexes.iter()
            .find(|idx| &idx.name == index_name)
            .cloned()
            .ok_or_else(|| StorageError::InvalidData(format!(
                "Index '{}' not found",
                index_name
            )))
    }

    /// 🔧 FIX: Find vector index by table and column name
    /// Returns the actual index name (user-specified, not auto-generated)
    pub fn find_vector_index(&self, table_name: &str, column_name: &str) -> Result<String> {
        let meta = self.metadata.read()
            .map_err(|e| StorageError::InvalidData(e.to_string()))?;

        // Search through index_map for a matching (table, column) pair
        for (index_name, (idx_table, idx_col)) in meta.index_map.iter() {
            if idx_table == table_name && idx_col == column_name {
                // Verify it's a vector index
                if let Some(table) = meta.tables.get(table_name) {
                    if let Some(index) = table.indexes.iter().find(|idx| &idx.name == index_name) {
                        if matches!(index.index_type, crate::types::IndexType::Vector { .. }) {
                            return Ok(index_name.clone());
                        }
                    }
                }
            }
        }

        Err(StorageError::InvalidData(format!(
            "No vector index found for {}.{}",
            table_name, column_name
        )))
    }

    /// Persist metadata to disk
    fn persist(&self) -> Result<()> {
        let meta = self.metadata.read()
            .map_err(|e| StorageError::InvalidData(e.to_string()))?;

        let data = bincode::serialize(&*meta)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        fs::write(&self.persist_path, data)
            .map_err(|e| StorageError::Io(e))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColumnDef, ColumnType, IndexType};

    #[test]
    fn test_create_and_get_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let registry = TableRegistry::new(temp_dir.path()).unwrap();

        let schema = TableSchema::new(
            "users".into(),
            vec![
                ColumnDef::new("id".into(), ColumnType::Integer, 0),
                ColumnDef::new("name".into(), ColumnType::Text, 1),
            ],
        );

        registry.create_table(schema.clone()).unwrap();

        let retrieved = registry.get_table("users").unwrap();
        assert_eq!(retrieved.name, "users");
        assert_eq!(retrieved.column_count(), 2);
    }

    #[test]
    fn test_drop_table() {
        let temp_dir = tempfile::tempdir().unwrap();
        let registry = TableRegistry::new(temp_dir.path()).unwrap();

        let schema = TableSchema::new("test".into(), vec![]);
        registry.create_table(schema).unwrap();

        assert!(registry.table_exists("test"));

        registry.drop_table("test").unwrap();
        assert!(!registry.table_exists("test"));
    }

    #[test]
    fn test_list_tables() {
        let temp_dir = tempfile::tempdir().unwrap();
        let registry = TableRegistry::new(temp_dir.path()).unwrap();

        registry.create_table(TableSchema::new("t1".into(), vec![])).unwrap();
        registry.create_table(TableSchema::new("t2".into(), vec![])).unwrap();

        let tables = registry.list_tables().unwrap();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"t1".to_string()));
        assert!(tables.contains(&"t2".to_string()));
    }

    #[test]
    fn test_add_index() {
        let temp_dir = tempfile::tempdir().unwrap();
        let registry = TableRegistry::new(temp_dir.path()).unwrap();

        let mut schema = TableSchema::new(
            "articles".into(),
            vec![
                ColumnDef::new("id".into(), ColumnType::Integer, 0),
                ColumnDef::new("title".into(), ColumnType::Text, 1),
            ],
        );

        registry.create_table(schema.clone()).unwrap();

        // Add index
        let index = IndexDef::new(
            "articles_title_idx".into(),
            "articles".into(),
            "title".into(),
            IndexType::FullText,
        );

        registry.add_index(index).unwrap();

        // Verify index exists
        let retrieved_index = registry.get_index("articles_title_idx").unwrap();
        assert_eq!(retrieved_index.column_name, "title");
    }

    #[test]
    fn test_persistence() {
        let temp_dir = tempfile::tempdir().unwrap();
        
        // Create registry and add table
        {
            let registry = TableRegistry::new(temp_dir.path()).unwrap();
            let schema = TableSchema::new(
                "persistent".into(),
                vec![ColumnDef::new("id".into(), ColumnType::Integer, 0)],
            );
            registry.create_table(schema).unwrap();
        }

        // Reload registry
        {
            let registry = TableRegistry::new(temp_dir.path()).unwrap();
            assert!(registry.table_exists("persistent"));
            let schema = registry.get_table("persistent").unwrap();
            assert_eq!(schema.column_count(), 1);
        }
    }
}
