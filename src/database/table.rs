//! Database Table Management
//!
//! Extracted from database_legacy.rs
//! Contains table schema management and helper methods

use crate::types::{TableSchema, IndexDef, RowId};
use crate::Result;
use std::sync::Arc;

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
        ensure_open!(self);
        // Register table in catalog (acquires metadata.write() lock)
        self.table_registry.create_table(schema.clone())?;
        // 🔓 Lock released here

        // Initialize row count counter for COUNT(*) fast path
        self.table_row_count.insert(schema.name.clone(), Arc::new(std::sync::atomic::AtomicU64::new(0)));

        // 🚀 Auto-create column index for PRIMARY KEY (if not AUTO_INCREMENT)
        // AUTO_INCREMENT PKs don't need a column index because PK value == row_id.
        // Non-AUTO_INCREMENT PKs need an index for point queries to be O(log N) instead of O(N).
        if let Some(pk_col) = schema.primary_key() {
            if !schema.is_primary_key_auto_increment() {
                // Create disk-based column index (for persistence + range queries)
                if let Err(e) = self.create_column_index(&schema.name, pk_col) {
                    eprintln!("[WARN] Failed to auto-create PK index for {}.{}: {}",
                        schema.name, pk_col, e);
                }

                // Create in-memory PK lookup (for O(1) PK → row_id resolution)
                // Bounded by LRU eviction — falls back to disk index on cache miss.
                let pk_cache = Arc::new(crate::database::pk_cache::PkLookupCache::new(self.pk_lookup_capacity));
                self.pk_lookup.insert(schema.name.clone(), pk_cache);
            }
        }

        // Register TimeSeries tables with the columnar store
        if schema.table_type == crate::types::TableType::TimeSeries {
            if let Ok(table_id) = self.table_registry.get_table_id(&schema.name) {
                if let Err(e) = self.columnar_store.register_table(table_id, &schema) {
                    eprintln!("[WARN] Failed to register columnar table '{}': {}", schema.name, e);
                }
            }
        }

        Ok(())
    }
    
    /// Drop a table
    ///
    /// Deletes row data (LSM tombstones), drops all indexes, cleans up caches,
    /// and removes table metadata.
    pub fn drop_table(&self, table_name: &str) -> Result<()> {
        ensure_open!(self);

        // 1. Delete row data from LSM (tombstones for compaction to reclaim)
        let table_prefix = self.compute_table_prefix(table_name);
        let start_key = table_prefix << 32;
        let end_key = (table_prefix << 32) | 0xFFFF_FFFF;
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        let _ = self.lsm_engine.delete_range(start_key, end_key, timestamp);

        // 2. Flush so tombstones reach SSTables (enables compaction cleanup)
        let _ = self.lsm_engine.flush();

        // 3. Drop columnar store for TimeSeries tables
        let _ = self.columnar_store.drop_table(table_name);

        // 4. Drop in-memory index handles
        self.vector_indexes.remove(table_name);
        self.ioctree_indexes.remove(table_name);
        self.text_indexes.remove(table_name);
        self.column_indexes.remove(table_name);

        // 5. Invalidate row cache for this table
        self.row_cache.invalidate_table(table_name);

        // 6. Remove catalog metadata
        self.table_registry.drop_table(table_name)?;
        self.pk_lookup.remove(table_name);
        self.table_auto_increment.remove(table_name);
        Ok(())
    }
    
    /// Get table schema
    /// 
    /// # Example
    /// ```ignore
    /// let schema = db.get_table_schema("users")?;
    /// println!("Table has {} columns", schema.column_count());
    /// ```
    pub fn get_table_schema(&self, table_name: &str) -> Result<Arc<TableSchema>> {
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

    /// Get total disk usage in bytes (WAL + LSM + indexes)
    pub fn disk_usage(&self) -> u64 {
        let mut total: u64 = 0;

        // WAL directory
        if let Ok(entries) = std::fs::read_dir(self.path.join("wal")) {
            for entry in entries.flatten() {
                if let Ok(meta) = entry.metadata() {
                    total += meta.len();
                }
            }
        }

        // LSM directory
        if let Ok(entries) = std::fs::read_dir(self.path.join("lsm")) {
            for entry in entries.flatten() {
                if let Ok(meta) = entry.metadata() {
                    total += meta.len();
                }
            }
        }

        // Indexes directory
        if let Ok(entries) = std::fs::read_dir(self.path.join("indexes")) {
            for entry in entries.flatten() {
                if let Ok(meta) = entry.metadata() {
                    total += meta.len();
                }
            }
        }

        total
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
    /// Format: [table_id:32bits][row_id:32bits]
    ///
    /// Uses stable sequential table_id from registry (collision-free),
    /// replacing the old hash-based scheme that had birthday-attack collision risk.
    pub(crate) fn make_composite_key(&self, table_name: &str, row_id: RowId) -> u64 {
        let table_id = self.table_registry.get_table_id(table_name)
            .unwrap_or(0); // fallback to 0 for unregistered tables
        ((table_id as u64) << 32) | (row_id & 0xFFFFFFFF)
    }

    /// Compute table prefix (upper 32 bits of composite key)
    ///
    /// Uses stable sequential table_id from registry (collision-free).
    pub(crate) fn compute_table_prefix(&self, table_name: &str) -> u64 {
        let table_id = self.table_registry.get_table_id(table_name)
            .unwrap_or(0);
        table_id as u64
    }

    /// 🚀 P2: Get row cache for statistics and monitoring
    pub fn get_row_cache(&self) -> &std::sync::Arc<crate::cache::RowCache> {
        &self.row_cache
    }
    
    /// Decode row data from LSM storage format
    /// 
    /// Internal helper for query operations
    pub(crate) fn decode_row_data(data: &[u8]) -> Result<crate::types::Row> {
        crate::storage::row_format::decode_any(data)
    }
    
    
    // Note: create_column_index() has been moved to indexes/column.rs
    // Removed duplicate definition to avoid E0592
}
