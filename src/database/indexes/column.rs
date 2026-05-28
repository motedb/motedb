//! Column Index Operations
//!
//! Extracted from database_legacy.rs
//! Provides column value indexing for WHERE clause optimization

use crate::database::core::MoteDB;
use crate::types::{RowId, Value};
use crate::{Result, StorageError};
use crate::index::column_value::{ColumnValueIndex, ColumnValueIndexConfig};
use std::sync::Arc;

impl MoteDB {
    /// Create a column value index for WHERE clause optimization
    pub fn create_column_index(&self, table_name: &str, column_name: &str) -> Result<()> {
        let index_name = format!("{}.{}", table_name, column_name);
        self.create_column_index_with_name(table_name, column_name, &index_name)
    }

    /// Create a column value index with custom name
    pub fn create_column_index_with_name(&self, table_name: &str, column_name: &str, index_name: &str) -> Result<()> {
        ensure_open!(self);
        let indexes_dir = self.path.join("indexes");
        std::fs::create_dir_all(&indexes_dir)?;
        let index_path = indexes_dir.join(format!("column_{}.idx", index_name));

        let mut config = ColumnValueIndexConfig::default();
        config.mem_buffer_size = self.column_index_buffer_size;
        let index = ColumnValueIndex::create(
            index_path,
            table_name.to_string(),
            column_name.to_string(),
            config
        )?;

        let index_arc = Arc::new(index);
        self.column_indexes.insert(index_name.to_string(), index_arc.clone());

        // Populate from existing data using scan_range
        if let Ok(schema) = self.table_registry.get_table(table_name) {
            if let Some(col_def) = schema.columns.iter().find(|c| c.name == column_name) {
                let col_position = col_def.position;

                debug_log!("[create_column_index] Using scan_range...");
                let start_time = std::time::Instant::now();

                let table_id = self.table_registry.get_table_id(table_name)
                    .unwrap_or(0) as u64;
                let start_key = table_id << 32;
                let end_key = (table_id + 1) << 32;

                let mut indexed_count = 0;
                const BATCH_SIZE: usize = 500;

                match self.lsm_engine.scan_range(start_key, end_key) {
                    Ok(entries) => {
                        for (batch_idx, chunk) in entries.chunks(BATCH_SIZE).enumerate() {
                            for (composite_key, value) in chunk {
                                // Skip deleted (tombstone) entries — they don't belong in the index
                                if value.deleted {
                                    continue;
                                }
                                let row_id = (composite_key & 0xFFFFFFFF) as RowId;

                                let data_bytes: Vec<u8> = match &value.data {
                                    crate::storage::lsm::ValueData::Inline(bytes) => bytes.to_vec(),
                                    crate::storage::lsm::ValueData::Blob(blob_ref) => {
                                        match self.lsm_engine.resolve_blob(blob_ref) {
                                            Ok(data) => data,
                                            Err(e) => {
                                                debug_log!("[create_column_index] Failed to resolve blob for row {}: {}", row_id, e);
                                                continue;
                                            }
                                        }
                                    }
                                };

                                if let Ok(row) = crate::storage::row_format::decode_any(&data_bytes) {
                                    if let Some(value) = row.get(col_position) {
                                        if let Err(e) = index_arc.insert(value, row_id) {
                                            debug_log!("[create_column_index] Insert failed row_id={}: {}", row_id, e);
                                        } else {
                                            indexed_count += 1;
                                        }
                                    }
                                }
                            }

                            if (batch_idx + 1) % 4 == 0 || (batch_idx + 1) * BATCH_SIZE >= entries.len() {
                                if let Err(e) = index_arc.flush() {
                                    debug_log!("[create_column_index] Flush failed: {}", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        debug_log!("[create_column_index] scan_range failed: {}", e);
                    }
                }

                let _scan_time = start_time.elapsed();
                if indexed_count > 0 {
                    debug_log!("[create_column_index] Indexed {} values in {:?}", indexed_count, _scan_time);
                }

                // Index is fully populated from LSM — clear the rebuild flag.
                // The async pipeline will skip this index since it's already up-to-date.
                index_arc.mark_rebuilt();
            }
        }

        Ok(())
    }

    /// Get all column indexes for a table
    pub fn get_table_column_indexes(&self, table_name: &str) -> Vec<String> {
        let prefix = format!("{}.", table_name);

        self.column_indexes.iter()
            .filter(|entry| entry.key().starts_with(&prefix))
            .map(|entry| entry.key().strip_prefix(&prefix).unwrap().to_string())
            .collect()
    }

    /// Flush column index to disk
    pub fn flush_column_index(&self, table_name: &str, column_name: &str) -> Result<()> {
        let index_name = format!("{}.{}", table_name, column_name);
        let index_ref = self.column_indexes.get(&index_name)
            .ok_or_else(|| StorageError::Index(format!("Column index '{}' not found", index_name)))?;

        index_ref.value().flush()?;
        Ok(())
    }

    /// Query by column value (point query)
    pub fn query_by_column(&self, table_name: &str, column_name: &str, value: &Value) -> Result<Vec<RowId>> {
        ensure_open!(self);
        let index_name = format!("{}.{}", table_name, column_name);

        let index_ref = self.column_indexes.get(&index_name)
            .ok_or_else(|| StorageError::Index(format!("Column index '{}' not found", index_name)))?;

        index_ref.value().get(value)
    }

    /// Query column value index with range (WHERE col >= start AND col <= end)
    pub fn query_by_column_range(&self, table_name: &str, column_name: &str,
                                start: &Value, end: &Value) -> Result<Vec<RowId>> {
        self.query_by_column_between(table_name, column_name, start, true, end, true)
    }

    /// Query column value index: WHERE col < value
    pub fn query_by_column_less_than(&self, table_name: &str, column_name: &str,
                                    value: &Value) -> Result<Vec<RowId>> {
        let index_name = format!("{}.{}", table_name, column_name);
        let index_ref = self.column_indexes.get(&index_name)
            .ok_or_else(|| StorageError::Index(format!("Column index '{}' not found", index_name)))?;

        index_ref.value().query_less_than(value)
    }

    /// Query column value index: WHERE col > value
    pub fn query_by_column_greater_than(&self, table_name: &str, column_name: &str,
                                       value: &Value) -> Result<Vec<RowId>> {
        let index_name = format!("{}.{}", table_name, column_name);
        let index_ref = self.column_indexes.get(&index_name)
            .ok_or_else(|| StorageError::Index(format!("Column index '{}' not found", index_name)))?;

        index_ref.value().query_greater_than(value)
    }

    /// Query column value index: WHERE col <= value
    pub fn query_by_column_less_than_or_equal(&self, table_name: &str, column_name: &str,
                                             value: &Value) -> Result<Vec<RowId>> {
        let index_name = format!("{}.{}", table_name, column_name);
        let index_ref = self.column_indexes.get(&index_name)
            .ok_or_else(|| StorageError::Index(format!("Column index '{}' not found", index_name)))?;

        index_ref.value().query_less_than_or_equal(value)
    }

    /// Query column value index: WHERE col >= value
    pub fn query_by_column_greater_than_or_equal(&self, table_name: &str, column_name: &str,
                                                value: &Value) -> Result<Vec<RowId>> {
        let index_name = format!("{}.{}", table_name, column_name);
        let index_ref = self.column_indexes.get(&index_name)
            .ok_or_else(|| StorageError::Index(format!("Column index '{}' not found", index_name)))?;

        index_ref.value().query_greater_than_or_equal(value)
    }

    /// Query column value index: dual-bound range query
    pub fn query_by_column_between(&self, table_name: &str, column_name: &str,
                                  lower_bound: &Value, lower_inclusive: bool,
                                  upper_bound: &Value, upper_inclusive: bool) -> Result<Vec<RowId>> {
        let index_name = format!("{}.{}", table_name, column_name);
        let index_ref = self.column_indexes.get(&index_name)
            .ok_or_else(|| StorageError::Index(format!("Column index '{}' not found", index_name)))?;

        index_ref.value().query_between(lower_bound, lower_inclusive, upper_bound, upper_inclusive)
    }
}

#[cfg(test)]
mod tests {
    use crate::Database;
    use tempfile::TempDir;

    #[test]
    fn test_create_column_index_and_query() {
        let dir = TempDir::new().unwrap();
        let db = Database::create(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, tag TEXT, val INT)").unwrap();

        // Insert data first, then create index
        for i in 0..100i64 {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'tag_{}', {})", i, i % 5, i)).unwrap();
        }

        db.execute("CREATE INDEX idx_tag ON t (tag) USING COLUMN").unwrap();
        db.wait_for_indexes_ready();
        db.flush().unwrap();

        // Query should return results (either via index or full scan)
        let result = db.execute("SELECT id FROM t WHERE tag = 'tag_3'").unwrap();
        use crate::sql::QueryResult;
        if let QueryResult::Select { rows, .. } = result.materialize().unwrap() {
            assert_eq!(rows.len(), 20, "should find 20 rows with tag='tag_3'");
        }
    }

    #[test]
    fn test_create_index_then_insert() {
        let dir = TempDir::new().unwrap();
        let db = Database::create(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, category TEXT)").unwrap();
        db.execute("CREATE INDEX idx_cat ON t (category) USING COLUMN").unwrap();
        db.wait_for_indexes_ready();

        // Insert after index exists
        db.execute("INSERT INTO t VALUES (1, 'alpha')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'beta')").unwrap();
        db.execute("INSERT INTO t VALUES (3, 'alpha')").unwrap();

        // Query should find newly inserted data (synchronous index update)
        let result = db.execute("SELECT id FROM t WHERE category = 'alpha' ORDER BY id").unwrap();
        use crate::sql::QueryResult;
        if let QueryResult::Select { rows, .. } = result.materialize().unwrap() {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][0], crate::types::Value::Integer(1));
            assert_eq!(rows[1][0], crate::types::Value::Integer(3));
        }
    }

    #[test]
    fn test_drop_index() {
        let dir = TempDir::new().unwrap();
        let db = Database::create(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, tag TEXT)").unwrap();
        db.execute("CREATE INDEX idx_tag ON t (tag) USING COLUMN").unwrap();
        db.wait_for_indexes_ready();

        db.execute("DROP INDEX idx_tag").unwrap();

        // Query should still work (falls back to full scan)
        let result = db.execute("SELECT * FROM t WHERE tag = 'test'").unwrap();
        assert!(result.materialize().is_ok());
    }

    #[test]
    fn test_wait_for_indexes_ready_completes_quickly() {
        // Regression test: wait_for_indexes_ready must return quickly
        // even if pending_index_batches is stale (e.g., due to a leaked counter
        // from a panicked index builder thread).
        let dir = TempDir::new().unwrap();
        let db = Database::create(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").unwrap();

        // With no flushes, pending should be 0 and return immediately
        let start = std::time::Instant::now();
        let ready = db.wait_for_indexes_ready();
        assert!(ready, "should be ready with no pending batches");
        assert!(start.elapsed() < std::time::Duration::from_secs(2),
            "wait_for_indexes_ready took {:?}", start.elapsed());

        // Insert a row — still shouldn't trigger flush with small data
        db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();

        let start = std::time::Instant::now();
        let ready = db.wait_for_indexes_ready();
        assert!(ready, "should be ready after small insert");
        assert!(start.elapsed() < std::time::Duration::from_secs(2),
            "wait_for_indexes_ready took {:?}", start.elapsed());
    }

    #[test]
    fn test_create_index_and_concurrent_insert() {
        // Regression test: CREATE INDEX + INSERT shouldn't deadlock
        let dir = TempDir::new().unwrap();
        let db = Database::create(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, category TEXT)").unwrap();
        db.execute("CREATE INDEX idx_cat ON t (category) USING COLUMN").unwrap();

        // Insert multiple rows rapidly (exercises index + mem_buffer)
        for i in 0..20 {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'cat_{}')", i, i % 3)).unwrap();
        }

        db.wait_for_indexes_ready();

        let result = db.execute("SELECT id FROM t WHERE category = 'cat_0' ORDER BY id").unwrap();
        use crate::sql::QueryResult;
        if let QueryResult::Select { rows, .. } = result.materialize().unwrap() {
            assert_eq!(rows.len(), 7, "should find 7 rows with cat_0");
        }
    }
}
