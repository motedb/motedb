//! Database Table Management
//!
//! Extracted from database_legacy.rs
//! Contains table schema management and helper methods

use crate::types::{RowId, TableSchema};
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
        self.table_row_count.insert(
            schema.name.clone(),
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
        );

        // 🚀 Auto-create column index for PRIMARY KEY (if not AUTO_INCREMENT)
        // AUTO_INCREMENT PKs don't need a column index because PK value == row_id.
        // Non-AUTO_INCREMENT PKs need an index for point queries to be O(log N) instead of O(N).
        if let Some(pk_col) = schema.primary_key() {
            if !schema.is_primary_key_auto_increment() {
                // Create disk-based column index (for persistence + range queries)
                if let Err(e) = self.create_column_index(&schema.name, pk_col) {
                    eprintln!(
                        "[WARN] Failed to auto-create PK index for {}.{}: {}",
                        schema.name, pk_col, e
                    );
                }

                // Create in-memory PK lookup (for O(1) PK → row_id resolution)
                // Bounded by LRU eviction — falls back to disk index on cache miss.
                let pk_cache = Arc::new(crate::database::pk_cache::PkLookupCache::new(
                    self.pk_lookup_capacity,
                ));
                self.pk_lookup.insert(schema.name.clone(), pk_cache);
            }
        }

        // Register TimeSeries tables with the columnar store
        if schema.table_type == crate::types::TableType::TimeSeries {
            if let Ok(table_id) = self.table_registry.get_table_id(&schema.name) {
                if let Err(e) = self.columnar_store.register_table(table_id, &schema) {
                    eprintln!(
                        "[WARN] Failed to register columnar table '{}': {}",
                        schema.name, e
                    );
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

        // 1. Remove from catalog FIRST — prevents concurrent INSERT/UPDATE/DELETE
        //    from writing new data while we're cleaning up. Operations on this
        //    table will get "table not found" from this point forward.
        self.table_registry.drop_table(table_name)?;

        // 2. Delete row data from LSM (tombstones for compaction to reclaim)
        let table_prefix = self.compute_table_prefix(table_name);
        let start_key = table_prefix << 32;
        let end_key = (table_prefix << 32) | 0xFFFF_FFFF;
        // Use write_lsn for tombstone timestamp, same as every other write path.
        // SystemTime can jump backward (NTP, VM migration), which would cause the
        // tombstone to be ignored and table rows to reappear after DROP.
        let timestamp = self
            .write_lsn
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if let Err(e) = self.lsm_engine.delete_range(start_key, end_key, timestamp) {
            warn_log!(
                "[drop_table] delete_range failed for '{}': {:?}",
                table_name,
                e
            );
        }

        // 3. Flush so tombstones reach SSTables (enables compaction cleanup)
        if let Err(e) = self.lsm_engine.flush() {
            warn_log!("[drop_table] flush failed for '{}': {:?}", table_name, e);
        }

        // 4. Drop columnar store for TimeSeries tables
        if let Err(e) = self.columnar_store.drop_table(table_name) {
            warn_log!(
                "[drop_table] columnar drop failed for '{}': {:?}",
                table_name,
                e
            );
        }

        // 5. Drop in-memory index handles (iterate by prefix since keys are "table.column" format)
        let prefix = format!("{}.", table_name);
        let vec_keys: Vec<String> = self
            .vector_indexes
            .iter()
            .filter(|e| e.key().starts_with(&prefix) || e.key() == table_name)
            .map(|e| e.key().clone())
            .collect();
        for k in vec_keys {
            self.vector_indexes.remove(&k);
        }

        let ioct_keys: Vec<String> = self
            .ioctree_indexes
            .iter()
            .filter(|e| e.key().starts_with(&prefix) || e.key() == table_name)
            .map(|e| e.key().clone())
            .collect();
        for k in ioct_keys {
            self.ioctree_indexes.remove(&k);
        }

        let txt_keys: Vec<String> = self
            .text_indexes
            .iter()
            .filter(|e| e.key().starts_with(&prefix) || e.key() == table_name)
            .map(|e| e.key().clone())
            .collect();
        for k in txt_keys {
            self.text_indexes.remove(&k);
        }

        let col_keys: Vec<String> = self
            .column_indexes
            .iter()
            .filter(|e| e.key().starts_with(&prefix) || e.key() == table_name)
            .map(|e| e.key().clone())
            .collect();
        for k in col_keys {
            self.column_indexes.remove(&k);
        }

        // 6. Invalidate row cache for this table
        self.row_cache.invalidate_table(table_name);

        // 7. Remove remaining runtime state
        self.pk_lookup.remove(table_name);
        self.table_auto_increment.remove(table_name);
        self.table_row_count.remove(table_name);
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

    // ==================== Internal Helper Methods ====================

    /// Make composite key from table name and row ID
    ///
    /// Format: [table_id:32bits][row_id:32bits]
    ///
    /// Uses stable sequential table_id from registry (collision-free),
    /// replacing the old hash-based scheme that had birthday-attack collision risk.
    pub(crate) fn make_composite_key(&self, table_name: &str, row_id: RowId) -> u64 {
        let table_id = match self.table_registry.get_table_id(table_name) {
            Ok(id) => id,
            Err(_) => {
                // Table not found — use a fallback that won't collide with valid
                // sequential table_ids (which start at 1). This is defensive;
                // callers should validate table_name before reaching this point.
                debug_log!(
                    "[make_composite_key] table '{}' not registered, using reserved id",
                    table_name
                );
                u32::MAX
            }
        };
        ((table_id as u64) << 32) | (row_id & 0xFFFFFFFF)
    }

    /// Compute table prefix (upper 32 bits of composite key)
    ///
    /// Uses stable sequential table_id from registry (collision-free).
    pub(crate) fn compute_table_prefix(&self, table_name: &str) -> u64 {
        let table_id = self.table_registry.get_table_id(table_name).unwrap_or(0);
        table_id as u64
    }

    // Note: create_column_index() has been moved to indexes/column.rs
    // Removed duplicate definition to avoid E0592
}

#[cfg(test)]
mod tests {
    use crate::Database;
    use tempfile::TempDir;

    fn setup_db() -> (Database, TempDir) {
        let dir = TempDir::new().unwrap();
        let db = Database::create(dir.path()).unwrap();
        (db, dir)
    }

    #[test]
    fn test_create_table_basic() {
        let (db, _dir) = setup_db();
        db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, score FLOAT)")
            .unwrap();
        // Should not panic
    }

    #[test]
    fn test_create_table_duplicate_name() {
        let (db, _dir) = setup_db();
        db.execute("CREATE TABLE t1 (a INT)").unwrap();
        let result = db.execute("CREATE TABLE t1 (b INT)");
        assert!(result.is_err(), "duplicate table name should fail");
    }

    #[test]
    fn test_create_table_with_pk_insert_select() {
        let (db, _dir) = setup_db();
        db.execute("CREATE TABLE pk_test (id INT PRIMARY KEY, val INT)")
            .unwrap();
        db.execute("INSERT INTO pk_test VALUES (1, 100)").unwrap();
        db.execute("INSERT INTO pk_test VALUES (2, 200)").unwrap();

        let result = db
            .execute("SELECT id, val FROM pk_test ORDER BY id")
            .unwrap();
        use crate::sql::QueryResult;
        if let QueryResult::Select { rows, .. } = result.materialize().unwrap() {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][0], crate::types::Value::Integer(1));
        }
    }

    #[test]
    fn test_drop_table() {
        let (db, _dir) = setup_db();
        db.execute("CREATE TABLE t (x INT)").unwrap();
        db.execute("DROP TABLE t").unwrap();
        let result = db.execute("INSERT INTO t VALUES (1)");
        assert!(result.is_err(), "insert into dropped table should fail");
    }

    #[test]
    fn test_show_tables() {
        let (db, _dir) = setup_db();
        db.execute("CREATE TABLE a (x INT)").unwrap();
        db.execute("CREATE TABLE b (y INT)").unwrap();
        let result = db.execute("SHOW TABLES").unwrap();
        // Should return table names
        assert!(result.materialize().is_ok());
    }

    #[test]
    fn test_describe_table() {
        let (db, _dir) = setup_db();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, val FLOAT)")
            .unwrap();
        let result = db.execute("DESCRIBE t").unwrap();
        assert!(result.materialize().is_ok());
    }
}
