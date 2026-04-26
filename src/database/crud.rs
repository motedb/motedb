//! CRUD Operations Module
//!
//! Provides complete Create, Read, Update, Delete operations for rows
//! 
//! # Features
//! - Row-level operations (insert_row, get_row, update_row, delete_row)
//! - Table-aware operations (insert_row_to_table, get_table_row, etc.)
//! - Batch operations (batch_insert_rows, batch_get_rows)
//! - Scan operations (scan_all_rows, scan_table_rows)
//! - Prefetching and caching for sequential access

use crate::{Result, StorageError};
use crate::types::{ColumnType, Row, RowId, PartitionId, Value, SqlRow};
use crate::txn::wal::WALRecord;
use crate::storage::row_format;
use super::core::MoteDB;
use std::sync::Arc;

/// Extract column types from a table schema for RawRow encoding.
/// Deserialize a row, trying RawRow first (with schema) and falling back to bincode.
fn deserialize_row(data: &[u8], col_types: &[ColumnType]) -> crate::Result<Row> {
    row_format::decode(data, col_types)
}

impl MoteDB {
    // ==================== Table-Aware CRUD Operations ====================
    
    /// Insert a row to a specific table (table-aware API)
    /// 
    /// # Arguments
    /// * `table_name` - Name of the table
    /// * `row` - Row data to insert
    /// 
    /// # Example
    /// ```ignore
    /// let row_id = db.insert_row_to_table("users", vec![
    ///     Value::Integer(1),
    ///     Value::Text("Alice".into()),
    /// ])?;
    /// ```ignore
    pub fn insert_row_to_table(&self, table_name: &str, mut row: Row) -> Result<RowId> {
        ensure_open!(self);
        // 1. Get table schema
        let schema = self.table_registry.get_table(table_name)?;

        // 1.5 Check primary key uniqueness for non-AUTO_INCREMENT tables
        if !schema.is_primary_key_auto_increment() {
            if let Some(pk_name) = schema.primary_key() {
                if let Some(pk_col) = schema.get_column(pk_name) {
                    if let Some(pk_value) = row.get(pk_col.position) {
                        // Fast path: check in-memory PK cache
                        let pk_key = crate::database::pk_cache::PkKey::from_value(pk_value);
                        let exists_in_cache = self.pk_lookup.get(table_name)
                            .map(|lookup| lookup.get_pk(&pk_key).is_some())
                            .unwrap_or(false);

                        if exists_in_cache {
                            return Err(StorageError::InvalidData(format!(
                                "Duplicate primary key {:?} for table '{}'", pk_value, table_name
                            )));
                        }

                        // Slow path: check column index (covers cache misses after restart)
                        match self.query_by_column(table_name, pk_name, pk_value) {
                            Ok(found) if !found.is_empty() => {
                                return Err(StorageError::InvalidData(format!(
                                    "Duplicate primary key {:?} for table '{}'", pk_value, table_name
                                )));
                            }
                            _ => {} // Not found or index not available — proceed
                        }
                    }
                }
            }
        }
        
        // 2. 🚀 P3+4: For AUTO_INCREMENT primary key, use per-table counter
        let row_id = if schema.is_primary_key_auto_increment() {
            // 🚀 Phase 4: Use per-table AUTO_INCREMENT counter (lock-free AtomicI64)
            // 🚀 Optimized: DashMap — first insert per table acquires shard lock, then lock-free
            let counter = {
                self.table_auto_increment.entry(table_name.to_string())
                    .or_insert_with(|| {
                        Arc::new(std::sync::atomic::AtomicI64::new(schema.get_auto_increment_start()))
                    })
                    .value()
                    .clone()
            };

            // 🚀 Phase 5: Overflow protection (B1)
            let id = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if !(0..=i64::MAX - 1000).contains(&id) {
                return Err(StorageError::AutoIncrementOverflow(table_name.to_string()));
            }

            // P2: Update persisted counter (lazy — persisted during checkpoint)
            let _ = self.table_registry.update_auto_increment_counter(table_name, id);
            
            // Fill AUTO_INCREMENT primary key with id
            if let Some(pk_col_name) = schema.primary_key() {
                if let Some(pk_col) = schema.get_column(pk_col_name) {
                    // Ensure row has enough slots
                    while row.len() <= pk_col.position {
                        row.push(Value::Null);
                    }
                    // Fill in id as primary key value
                    row[pk_col.position] = Value::Integer(id);
                }
            }
            
            id as RowId
        } else {
            // Non-AUTO_INCREMENT: use global row_id (lock-free atomic)
            self.next_row_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        };
        
        // 3. Validate row
        schema.validate_row(&row)
            .map_err(|e| StorageError::InvalidData(format!(
                "Row validation failed for table '{}': {}",
                table_name, e
            )))?;

        // 4. Determine partition
        let composite_key = self.make_composite_key(table_name, row_id);
        let partition = (composite_key % self.num_partitions as u64) as PartitionId;

        // 5. Encode row to raw bytes (shared between WAL and LSM — zero-copy recovery)
        let col_types = schema.col_types();
        let row_data = row_format::encode(&row, col_types)
            .unwrap_or_else(|_| bincode::serialize(&row).unwrap());

        // 6. Write to WAL first (durability) — by reference, zero clone
        self.wal.log_insert_raw_ref(table_name, partition, row_id, &row_data, 0)?;

        // 7. Write to LSM MemTable with table prefix (move row_data, no clone)
        let composite_key = self.make_composite_key(table_name, row_id);
        let value = crate::storage::lsm::Value::new(row_data, row_id);
        self.lsm_engine.put(composite_key, value)?;

        // 7. Update indexes
        {
        let mut index_errors: Vec<String> = Vec::new();

        for col_def in &schema.columns {
            let col_name = &col_def.name;
            let col_value = row.get(col_def.position);

            if col_value.is_none() {
                continue;
            }
            let col_value = col_value.unwrap();

            // 🚀 In-memory PK lookup (O(1) resolution, bypasses disk-based B-Tree)
            if let Some(pk_name) = schema.primary_key() {
                if col_name == pk_name && !schema.is_primary_key_auto_increment() {
                    if let Some(lookup) = self.pk_lookup.get(table_name) {
                        lookup.insert(crate::database::pk_cache::PkKey::from_value(col_value), row_id);
                    }
                }
            }

            // 7.1 Column Index — build key once, check DashMap
            {
                let mut col_index_key = String::with_capacity(table_name.len() + 1 + col_name.len());
                col_index_key.push_str(table_name);
                col_index_key.push('.');
                col_index_key.push_str(col_name);
                if self.column_indexes.contains_key(&col_index_key) {
                    if let Err(_e) = self.insert_column_value(table_name, col_name, row_id, col_value) {
                        debug_log!("[insert_row] Failed to update column index '{}': {}", col_name, _e);
                        index_errors.push(format!("{}.{}", table_name, col_name));
                    }
                }
            } // end column index block

            // 7.2 Vector Index
            if let crate::types::ColumnType::Tensor(_dim) = col_def.col_type {
                if let Some(index_name) = self.index_registry.find_by_column(
                    table_name,
                    col_name,
                    crate::database::index_metadata::IndexType::Vector
                ) {
                    if let crate::types::Value::Vector(vec) = col_value {
                        if let Err(_e) = self.update_vector(row_id, &index_name, vec.as_slice()) {
                            debug_log!("[insert_row] Failed to update vector index '{}': {}", index_name, _e);
                            index_errors.push(index_name.clone());
                        }
                    }
                }
            }

            // 7.3 Text Index
            if matches!(col_def.col_type, crate::types::ColumnType::Text) {
                if let Some(index_name) = self.index_registry.find_by_column(table_name, col_name, crate::database::index_metadata::IndexType::Text) {
                    if let crate::types::Value::Text(text) = col_value {
                        if let Err(_e) = self.insert_text(row_id, &index_name, text) {
                            debug_log!("[insert_row] Failed to update text index '{}': {}", index_name, _e);
                            index_errors.push(index_name.clone());
                        }
                    }
                }
            }

            // 7.4 i-Octree Index (3D point cloud)
            if matches!(col_def.col_type, crate::types::ColumnType::Spatial) {
                if let Some(index_name) = self.index_registry.find_by_column(table_name, col_name, crate::database::index_metadata::IndexType::Octree) {
                    if let crate::types::Value::Spatial(geom) = col_value {
                        if let Err(_e) = self.insert_ioctree_point(row_id, &index_name, geom) {
                            debug_log!("[insert_row] Failed to update ioctree index '{}': {}", index_name, _e);
                            index_errors.push(index_name.clone());
                        }
                    }
                }
            }
        }

        // If any index update failed, mark ALL indexes for this table stale
        // so queries fall back to full scan consistently
        if !index_errors.is_empty() {
            debug_log!("[insert_row] {} index updates failed for table '{}', marking all stale",
                     index_errors.len(), table_name);
            for idx_name in &index_errors {
                self.index_registry.mark_stale(idx_name);
            }
            for meta in self.index_registry.list_table_indexes(table_name) {
                self.index_registry.mark_stale(&meta.name);
            }
        }
        } // end index_update_strategy check

        // 9. Increment pending counter
        self.increment_pending_updates();

        // 10. Increment row count for COUNT(*) fast path
        if let Some(counter) = self.table_row_count.get(table_name) {
            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        Ok(row_id)
    }
    
    /// Get a row from a specific table (table-aware API)
    /// 
    /// # Arguments
    /// * `table_name` - Name of the table
    /// * `row_id` - Internal row ID
    /// 
    /// # Example
    /// ```ignore
    /// let row = db.get_table_row("users", row_id)?;
    /// ```ignore
    pub fn get_table_row(&self, table_name: &str, row_id: RowId) -> Result<Option<Row>> {
        ensure_open!(self);
        // Get schema for RawRow decoding
        let schema = self.table_registry.get_table(table_name)?;

        // Try cache first
        if let Some(row_arc) = self.row_cache.get(table_name, row_id) {
            // Check if prefetch should be triggered
            if let Some((next_row_id, count, stride)) = self.row_cache.check_prefetch(table_name, row_id) {
                self.trigger_prefetch(table_name, next_row_id, count, stride);
            }
            
            return Ok(Some((*row_arc).clone()));
        }
        
        // Cache miss - load from LSM
        let composite_key = self.make_composite_key(table_name, row_id);
        
        if let Some(value) = self.lsm_engine.get(composite_key)? {
            // Check if row is deleted (tombstone)
            if value.deleted {
                return Ok(None);
            }
            
            // Extract data
            let data = match &value.data {
                crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                crate::storage::lsm::ValueData::Blob(_) => {
                    return Err(StorageError::InvalidData(
                        "Blob values not yet supported in get_table_row".into()
                    ));
                }
            };
            
            // Deserialize row (RawRow with bincode fallback)
            let col_types = schema.col_types();
            let row: Row = row_format::decode(data, col_types)
                .map_err(|e| StorageError::Serialization(format!(
                    "Failed to deserialize row {}: {}",
                    row_id, e
                )))?;

            // Update cache
            self.row_cache.put(table_name.to_string(), row_id, row.clone());
            
            // Check if prefetch should be triggered
            if let Some((next_row_id, count, stride)) = self.row_cache.check_prefetch(table_name, row_id) {
                self.trigger_prefetch(table_name, next_row_id, count, stride);
            }
            
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }
    
    /// Update a row in a specific table (table-aware API)
    /// 
    /// # Arguments
    /// * `table_name` - Name of the table
    /// * `row_id` - Internal row ID
    /// * `old_row` - Old row data (to avoid re-loading)
    /// * `new_row` - New row data
    /// 
    /// # Example
    /// ```ignore
    /// db.update_row_in_table("users", row_id, old_row, vec![Value::Integer(1), Value::Text("Bob".into())])?;
    /// ```ignore
    pub fn update_row_in_table(&self, table_name: &str, row_id: RowId, old_row: Row, new_row: Row) -> Result<()> {
        ensure_open!(self);
        // 1. Get schema (old_row is now passed in to avoid re-loading)
        let schema = self.table_registry.get_table(table_name)?;
        
        // 2. Construct composite key
        let composite_key = self.make_composite_key(table_name, row_id);
        
        // 3. Determine partition
        let partition = (composite_key % self.num_partitions as u64) as PartitionId;

        // 4. Encode rows to raw bytes
        let col_types = schema.col_types();
        let raw_old = row_format::encode(&old_row, col_types)
            .unwrap_or_else(|_| bincode::serialize(&old_row).unwrap());
        let raw_new = row_format::encode(&new_row, col_types)
            .unwrap_or_else(|_| bincode::serialize(&new_row).unwrap());

        // 5. Write to WAL first (durability) — raw bytes
        self.wal.log_update_raw(table_name, partition, composite_key, raw_old, raw_new.clone(), 0)?;

        // 6. Update in LSM MemTable (same bytes as WAL)
        // Use microsecond timestamp so successive UPDATEs to the same row have
        // monotonically increasing timestamps. Using composite_key was a bug:
        // all updates to the same row shared the same timestamp, so compaction
        // merge (which keeps the higher-timestamp entry) couldn't distinguish
        // between update versions and could keep a stale one.
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| StorageError::InvalidData(e.to_string()))?
            .as_micros() as u64;
        let value = crate::storage::lsm::Value::new(raw_new, timestamp);
        self.lsm_engine.put(composite_key, value)?;

        // Invalidate cache after update (prevent stale reads)
        self.row_cache.invalidate(table_name, row_id);
        
        // 6. Update indexes. Collect failures, then mark ALL stale consistently.
        let mut index_errors = Vec::new();

        // DashMap direct lookup for indexed columns (zero alloc)
        let col_index_prefix = format!("{}.", table_name);

        for col_def in &schema.columns {
            let col_name = &col_def.name;
            let old_value = old_row.get(col_def.position);
            let new_value = new_row.get(col_def.position);

            // Skip unchanged columns
            if old_value == new_value {
                continue;
            }

            // 6.1 Column Index — direct DashMap lookup (zero alloc)
            let col_index_key = format!("{}{}", col_index_prefix, col_name);
            if self.column_indexes.contains_key(&col_index_key) {
                let old_is_null = old_value.is_none() || matches!(old_value, Some(Value::Null));
                let new_is_null = new_value.is_none() || matches!(new_value, Some(Value::Null));

                if !old_is_null && !new_is_null {
                    // value → value: update index
                    if let (Some(old_val), Some(new_val)) = (old_value, new_value) {
                        if let Err(_e) = self.update_column_value(table_name, col_name, row_id, old_val, new_val) {
                            debug_log!("[update_row] Failed to update column index '{}': {}", col_name, _e);
                            index_errors.push(format!("{}.{}", table_name, col_name));
                        }
                    }
                } else if !old_is_null && new_is_null {
                    // value → NULL: remove from index
                    if let Some(old_val) = old_value {
                        if let Err(_e) = self.delete_column_value(table_name, col_name, row_id, old_val) {
                            debug_log!("[update_row] Failed to delete column index '{}': {}", col_name, _e);
                            index_errors.push(format!("{}.{}", table_name, col_name));
                        }
                    }
                } else if old_is_null && !new_is_null {
                    // NULL → value: insert into index
                    if let Some(new_val) = new_value {
                        if let Err(_e) = self.insert_column_value(table_name, col_name, row_id, new_val) {
                            debug_log!("[update_row] Failed to insert column index '{}': {}", col_name, _e);
                            index_errors.push(format!("{}.{}", table_name, col_name));
                        }
                    }
                }
                // NULL → NULL: no index change needed
            }

            // 6.2 Vector Index
            if let crate::types::ColumnType::Tensor(_dim) = col_def.col_type {
                let index_name = format!("{}_{}", table_name, col_name);
                if self.vector_indexes.contains_key(&index_name) {
                    let mut failed = false;
                    if let Err(_e) = self.delete_vector(row_id, &index_name) {
                        debug_log!("[update_row] Failed to delete old vector '{}': {}", index_name, _e);
                        failed = true;
                    }

                    if let Some(crate::types::Value::Vector(new_vec)) = new_value {
                        if let Err(_e) = self.update_vector(row_id, &index_name, new_vec.as_slice()) {
                            debug_log!("[update_row] Failed to update vector index '{}': {}", index_name, _e);
                            failed = true;
                        }
                    }
                    if failed {
                        index_errors.push(index_name.clone());
                    }
                }
            }

            // 6.3 Text Index
            if matches!(col_def.col_type, crate::types::ColumnType::Text) {
                let index_name = format!("{}_{}", table_name, col_name);
                if self.text_indexes.contains_key(&index_name) {
                    if let (Some(crate::types::Value::Text(old_text)), Some(crate::types::Value::Text(new_text))) = (old_value, new_value) {
                        if let Err(_e) = self.update_text(row_id, &index_name, old_text, new_text) {
                            debug_log!("[update_row] Failed to update text index '{}': {}", index_name, _e);
                            index_errors.push(index_name.clone());
                        }
                    }
                }
            }

            // 6.4 i-Octree Index (3D point cloud)
            if matches!(col_def.col_type, crate::types::ColumnType::Spatial) {
                if let Some(octree_name) = self.index_registry.find_by_column(table_name, col_name, crate::database::index_metadata::IndexType::Octree) {
                    let mut failed = false;
                    if let Err(_e) = self.delete_ioctree_point(row_id, &octree_name) {
                        debug_log!("[update_row] Failed to delete old ioctree point '{}': {}", octree_name, _e);
                        failed = true;
                    }
                    if let Some(crate::types::Value::Spatial(new_geom)) = new_value {
                        if let Err(_e) = self.insert_ioctree_point(row_id, &octree_name, new_geom) {
                            debug_log!("[update_row] Failed to update ioctree index '{}': {}", octree_name, _e);
                            failed = true;
                        }
                    }
                    if failed {
                        index_errors.push(octree_name.clone());
                    }
                }
            }
        }

        // 7. Update PK lookup cache if primary key value changed
        if let Some(pk_name) = schema.primary_key() {
            if !schema.is_primary_key_auto_increment() {
                if let Some(pk_col) = schema.get_column(pk_name) {
                    let old_pk = old_row.get(pk_col.position);
                    let new_pk = new_row.get(pk_col.position);
                    if old_pk != new_pk {
                        if let Some(pk_lookup) = self.pk_lookup.get(table_name) {
                            if let Some(old_val) = old_pk {
                                let old_key = crate::database::pk_cache::PkKey::from_value(old_val);
                                pk_lookup.remove_pk(&old_key);
                            }
                            if let Some(new_val) = new_pk {
                                let new_key = crate::database::pk_cache::PkKey::from_value(new_val);
                                pk_lookup.insert(new_key, row_id);
                            }
                        }
                    }
                }
            }
        }

        // If any index update failed, mark ALL indexes for this table stale
        if !index_errors.is_empty() {
            debug_log!("[update_row] {} index updates failed for table '{}', marking all stale",
                     index_errors.len(), table_name);
            for meta in self.index_registry.list_table_indexes(table_name) {
                self.index_registry.mark_stale(&meta.name);
            }
        }

        Ok(())
    }

    /// Delete a row from a specific table (table-aware API)
    /// 
    /// # Arguments
    /// * `table_name` - Name of the table
    /// * `row_id` - Internal row ID
    /// * `old_row` - Old row data (to avoid re-loading)
    /// 
    /// # Example
    /// ```ignore
    /// db.delete_row_from_table("users", row_id, old_row)?;
    /// ```ignore
    pub fn delete_row_from_table(&self, table_name: &str, row_id: RowId, old_row: Row) -> Result<()> {
        ensure_open!(self);
        // 1. Get schema (old_row is now passed in to avoid re-loading)
        let schema = self.table_registry.get_table(table_name)?;

        // 2. Construct composite key
        let composite_key = self.make_composite_key(table_name, row_id);

        // 3. Determine partition
        let partition = (composite_key % self.num_partitions as u64) as PartitionId;

        // 4. Compute timestamp (used by both WAL and LSM)
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| StorageError::InvalidData(e.to_string()))?
            .as_micros() as u64;

        // 5. Write to WAL first (durability guarantee)
        //    WAL must be written BEFORE any mutation so that a crash at any
        //    point below can be recovered correctly.
        // 5. Write to WAL first (durability guarantee) — raw bytes
        let col_types = schema.col_types();
        let raw_old = row_format::encode(&old_row, col_types)
            .unwrap_or_else(|_| bincode::serialize(&old_row).unwrap());
        self.wal.log_delete_raw(table_name, partition, composite_key, raw_old, timestamp, 0)?;

        // 6. Delete from LSM (using tombstone)
        self.lsm_engine.delete(composite_key, timestamp)?;

        // 7. Invalidate cache (prevent reading deleted data)
        self.row_cache.invalidate(table_name, row_id);

        // 7.1 Decrement row count for COUNT(*) fast path
        // Guard against underflow on double-delete (counter wraps on u64)
        if let Some(counter) = self.table_row_count.get(table_name) {
            use std::sync::atomic::Ordering::SeqCst;
            let mut current = counter.load(SeqCst);
            while current > 0 {
                match counter.compare_exchange_weak(current, current - 1, SeqCst, SeqCst) {
                    Ok(_) => break,
                    Err(actual) => current = actual,
                }
            }
        }

        // 7.2 Remove from PK lookup cache (prevents stale lookups)
        if let Some(pk_name) = schema.primary_key() {
            if !schema.is_primary_key_auto_increment() {
                if let Some(pk_col) = schema.get_column(pk_name) {
                    if let Some(pk_value) = old_row.get(pk_col.position) {
                        if let Some(lookup) = self.pk_lookup.get(table_name) {
                            lookup.remove_pk(&crate::database::pk_cache::PkKey::from_value(pk_value));
                        }
                    }
                }
            }
        }

        // 8. Update indexes (after data is durable).
        //    If an index deletion fails, the index is marked stale and can be
        //    rebuilt later. Since indexes are derived data, this is safe.

        // DashMap direct lookup for indexed columns (zero alloc)
        let col_index_prefix = format!("{}.", table_name);

        for col_def in &schema.columns {
            let col_name = &col_def.name;
            let col_value = old_row.get(col_def.position);

            if col_value.is_none() {
                continue;
            }
            let col_value = col_value.unwrap();

            // Column Index — direct DashMap lookup (zero alloc)
            let col_index_key = format!("{}{}", col_index_prefix, col_name);
            if self.column_indexes.contains_key(&col_index_key) {
                if let Err(_e) = self.delete_column_value(table_name, col_name, row_id, col_value) {
                    debug_log!("[delete_row] Failed to delete from column index '{}': {}", col_name, _e);
                    self.index_registry.mark_stale(&format!("{}.{}", table_name, col_name));
                }
            }

            // Vector Index
            if let crate::types::ColumnType::Tensor(_dim) = col_def.col_type {
                let index_name = format!("{}_{}", table_name, col_name);
                if self.vector_indexes.contains_key(&index_name) {
                    if let Err(_e) = self.delete_vector(row_id, &index_name) {
                        debug_log!("[delete_row] Failed to delete from vector index '{}': {}", index_name, _e);
                        self.index_registry.mark_stale(&index_name);
                    }
                }
            }

            // Text Index
            if matches!(col_def.col_type, crate::types::ColumnType::Text) {
                let index_name = format!("{}_{}", table_name, col_name);
                if self.text_indexes.contains_key(&index_name) {
                    if let crate::types::Value::Text(text) = col_value {
                        if let Err(_e) = self.delete_text(row_id, &index_name, text) {
                            debug_log!("[delete_row] Failed to delete from text index '{}': {}", index_name, _e);
                            self.index_registry.mark_stale(&index_name);
                        }
                    }
                }
            }

            // i-Octree Index (3D point cloud)
            if matches!(col_def.col_type, crate::types::ColumnType::Spatial) {
                if let Some(octree_name) = self.index_registry.find_by_column(table_name, col_name, crate::database::index_metadata::IndexType::Octree) {
                    if let Err(_e) = self.delete_ioctree_point(row_id, &octree_name) {
                        debug_log!("[delete_row] Failed to delete from ioctree index '{}': {}", octree_name, _e);
                        self.index_registry.mark_stale(&octree_name);
                    }
                }
            }
        }

        Ok(())
    }
    
    /// Scan all rows in a specific table
    /// 
    /// # Arguments
    /// * `table_name` - Name of the table
    /// 
    /// # Example
    /// ```ignore
    /// let rows = db.scan_table_rows("users")?;
    /// ```ignore
    pub fn scan_table_rows(&self, table_name: &str) -> Result<Vec<(RowId, Row)>> {
        ensure_open!(self);
        let schema = self.table_registry.get_table(table_name)?;
        let col_types = schema.col_types();

        let table_prefix = self.compute_table_prefix(table_name);
        let start_key = table_prefix << 32;
        let end_key = (table_prefix + 1) << 32;

        // Use streaming scan to avoid materializing full BTreeMap (saves ~420 MB for 300K rows)
        let lsm_iter = self.lsm_engine.scan_range_streaming(start_key, end_key)?;

        let mut result = Vec::new();
        for item in lsm_iter {
            let (composite_key, value) = item?;
            if value.deleted {
                continue;
            }

            let row_id = (composite_key & 0xFFFFFFFF) as RowId;

            let data = match &value.data {
                crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                crate::storage::lsm::ValueData::Blob(_) => {
                    return Err(StorageError::InvalidData(
                        "Blob references should be resolved by LSM engine".into()
                    ));
                }
            };
            
            // Deserialize row
            let row: Row = deserialize_row(data, col_types)?;
            result.push((row_id, row));
        }
        
        Ok(result)
    }
    
    /// 🚀 流式扫描表行（批量迭代器，内存友好）
    /// 
    /// 返回一个迭代器，每次产出一批行数据（默认 1000 行），而不是一次性加载全部。
    /// 
    /// # 性能对比
    /// - `scan_table_rows()`: 30 万行 × 1.4 KB = 420 MB 内存峰值 🔴
    /// - `scan_table_rows_batched()`: 1000 行 × 1.4 KB = 1.4 MB 内存峰值 ✅
    /// 
    /// # 使用场景
    /// - COUNT(*) - 只需遍历不需要保存全部数据
    /// - WHERE 过滤 - 逐批过滤，只保留匹配的行
    /// - UPDATE/DELETE - 逐批处理，减少内存占用
    /// 
    /// # 示例
    /// ```ignore
    /// let iter = db.scan_table_rows_batched("users", 1000)?;
    /// let mut count = 0;
    /// for batch_result in iter {
    ///     let batch = batch_result?;
    ///     count += batch.len();
    /// }
    /// println!("Total rows: {}", count);
    /// ```
    pub fn scan_table_rows_batched(
        &self,
        table_name: &str,
        batch_size: usize,
    ) -> Result<TableRowBatchedIterator> {
        ensure_open!(self);
        // Get table schema first (validates table exists)
        let schema = self.table_registry.get_table(table_name)?;

        // Use LSM batched scan
        let table_prefix = self.compute_table_prefix(table_name);
        let start_key = table_prefix << 32;
        let end_key = (table_prefix + 1) << 32;

        let lsm_iter = self.lsm_engine.scan_range_batched(start_key, end_key, batch_size)?;

        Ok(TableRowBatchedIterator {
            lsm_iter,
            _table_name: table_name.to_string(),
            col_types: Some(schema.col_types().to_vec()),
        })
    }
    
    /// 🚀 真正的流式扫描表行（O(1) 内存占用）
    /// 
    /// 使用多路归并迭代器，逐个返回行数据，**真正的流式处理**，不预先加载任何数据到内存。
    /// 
    /// # 内存对比
    /// - `scan_table_rows()`: 30 万行 × 1.4 KB = 420 MB 🔴
    /// - `scan_table_rows_batched()`: 仍需合并所有数据 = 420 MB 🔴
    /// - `scan_table_rows_streaming()`: 13 个迭代器 × 1.5 KB = 20 KB ✅
    /// - **节省 99.995% 内存**
    /// 
    /// # 使用场景
    /// - COUNT(*) - 只需遍历不需要保存数据
    /// - WHERE 过滤 - 逐行过滤，只保留匹配的行
    /// - 大表查询 - 避免内存溢出
    /// 
    /// # 示例
    /// ```ignore
    /// let iter = db.scan_table_rows_streaming("users")?;
    /// let mut count = 0;
    /// for result in iter {
    ///     let (row_id, row) = result?;
    ///     count += 1;
    /// }
    /// println!("Total rows: {}", count);
    /// ```
    pub fn scan_table_rows_streaming(
        &self,
        table_name: &str,
    ) -> Result<TableRowStreamingIterator> {
        ensure_open!(self);
        let schema = self.table_registry.get_table(table_name)?;
        let col_types = schema.col_types();

        // Use LSM streaming scan
        let table_prefix = self.compute_table_prefix(table_name);
        let start_key = table_prefix << 32;
        let end_key = (table_prefix + 1) << 32;

        let lsm_iter = self.lsm_engine.scan_range_streaming(start_key, end_key)?;

        Ok(TableRowStreamingIterator {
            lsm_iter,
            col_types: Some(col_types.to_vec()),
        })
    }
    
    /// Get approximate row count for a table (fast estimation)
    /// 
    /// Uses LSM storage statistics to estimate row count without full scan.
    /// Useful for query optimization (e.g., index selectivity calculation).
    /// 
    /// # Performance
    /// - Full scan: O(n) - 300ms for 300K rows
    /// - Estimation: O(1) - <1ms (reads metadata only)
    /// 
    /// # Accuracy
    /// - ±5% error rate (due to tombstones and MemTable)
    /// - Accurate enough for query planning
    /// 
    /// # Example
    /// ```ignore
    /// let count = db.estimate_table_row_count("users")?;
    /// // count ≈ 100,000 (actual: 95,000-105,000)
    /// ```
    pub fn estimate_table_row_count(&self, table_name: &str) -> Result<usize> {
        // Validate table exists
        let _schema = self.table_registry.get_table(table_name)?;
        
        // Use LSM metadata to estimate count
        let table_prefix = self.compute_table_prefix(table_name);
        let start_key = table_prefix << 32;
        let end_key = (table_prefix + 1) << 32;
        
        // Count SSTable entries (fast - reads metadata only)
        let sst_count = self.lsm_engine.estimate_key_count_in_range(start_key, end_key)?;
        
        // MemTable typically contains 1-5% of data, add 5% buffer for safety
        let estimated_total = (sst_count as f64 * 1.05) as usize;
        
        Ok(estimated_total)
    }
    
    /// 🚀 PHASE B.2: Scan table rows with partial deserialization
    /// 
    /// Only deserializes the columns specified in `required_columns`, skipping others.
    /// This significantly reduces deserialization overhead when selecting few columns.
    /// 
    /// ## Performance
    /// - SELECT 2/10 columns: 5x faster (400µs → 80µs)
    /// - SELECT 5/10 columns: 2x faster (400µs → 200µs)
    /// - SELECT * : fallback to full deserialization
    /// 
    /// ## Example
    /// ```ignore
    /// // Only deserialize id and name columns
    /// let rows = db.scan_table_rows_partial("users", &["id", "name"])?;
    /// ```
    pub fn scan_table_rows_partial(
        &self,
        table_name: &str,
        required_columns: &[String],
    ) -> Result<Vec<(RowId, SqlRow)>> {
        use crate::types::SqlRow;
        
        // Get table schema
        let schema = self.table_registry.get_table(table_name)?;
        
        // If all columns required, fallback to full scan
        if required_columns.len() >= schema.columns.len() {
            let rows = self.scan_table_rows(table_name)?;
            return Ok(rows.into_iter()
                .map(|(row_id, row)| {
                    let mut sql_row = SqlRow::new();
                    for (i, col_def) in schema.columns.iter().enumerate() {
                        let value = row.get(i).cloned().unwrap_or(Value::Null);
                        sql_row.insert(col_def.name.clone(), value);
                    }
                    (row_id, sql_row)
                })
                .collect());
        }
        
        // Use streaming scan to avoid materializing full BTreeMap
        let table_prefix = self.compute_table_prefix(table_name);
        let start_key = table_prefix << 32;
        let end_key = (table_prefix + 1) << 32;

        let lsm_iter = self.lsm_engine.scan_range_streaming(start_key, end_key)?;

        let mut result = Vec::new();

        // Process results with partial deserialization
        for item in lsm_iter {
            let (composite_key, value) = item?;
            if value.deleted {
                continue;
            }
            let row_id = (composite_key & 0xFFFFFFFF) as RowId;
            
            let data = match &value.data {
                crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                crate::storage::lsm::ValueData::Blob(_) => {
                    return Err(StorageError::InvalidData(
                        "Blob references should be resolved by LSM engine".into()
                    ));
                }
            };
            
            // 🚀 Partial deserialization: only deserialize required columns
            let sql_row = deserialize_partial(data, required_columns, &schema)?;
            result.push((row_id, sql_row));
        }
        
        Ok(result)
    }
    
    // ==================== Batch Operations ====================
    
    /// Batch insert rows to a specific table with incremental index updates
    /// 
    /// **NOTE**: This method updates indexes incrementally for each row, ensuring consistency
    /// even for small datasets (< 500 rows) that don't trigger batch index building.
    /// 
    /// # Example
    /// ```ignore
    /// let rows = vec![
    ///     vec![Value::Integer(1), Value::Text("Alice".into())],
    ///     vec![Value::Integer(2), Value::Text("Bob".into())],
    /// ];
    /// let row_ids = db.batch_insert_rows_to_table("users", rows)?;
    /// ```ignore
    pub fn batch_insert_rows_to_table(&self, table_name: &str, rows: Vec<Row>) -> Result<Vec<RowId>> {
        ensure_open!(self);
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        
        // 1. Get table schema
        let schema = self.table_registry.get_table(table_name)?;
        
        // 2. Validate all rows
        for (idx, row) in rows.iter().enumerate() {
            schema.validate_row(row)
                .map_err(|e| StorageError::InvalidData(format!(
                    "Row {} validation failed for table '{}': {}",
                    idx, table_name, e
                )))?;
        }

        // 2.5 Check primary key uniqueness for non-AUTO_INCREMENT tables
        if !schema.is_primary_key_auto_increment() {
            if let Some(pk_name) = schema.primary_key() {
                if let Some(pk_col) = schema.get_column(pk_name) {
                    for (idx, row) in rows.iter().enumerate() {
                        if let Some(pk_value) = row.get(pk_col.position) {
                            let pk_key = crate::database::pk_cache::PkKey::from_value(pk_value);
                            let exists = self.pk_lookup.get(table_name)
                                .map(|lookup| lookup.get_pk(&pk_key).is_some())
                                .unwrap_or(false);
                            if exists {
                                return Err(StorageError::InvalidData(format!(
                                    "Batch row {}: duplicate primary key {:?} for table '{}'", idx, pk_value, table_name
                                )));
                            }
                            match self.query_by_column(table_name, pk_name, pk_value) {
                                Ok(found) if !found.is_empty() => {
                                    return Err(StorageError::InvalidData(format!(
                                        "Batch row {}: duplicate primary key {:?} for table '{}'", idx, pk_value, table_name
                                    )));
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }

        // 3. Batch allocate row IDs
        let mut row_ids = Vec::with_capacity(rows.len());
        let auto_inc = schema.is_primary_key_auto_increment();
        if auto_inc {
            // Use per-table AUTO_INCREMENT counter (consistent with insert_row_to_table)
            let counter = {
                self.table_auto_increment.entry(table_name.to_string())
                    .or_insert_with(|| {
                        Arc::new(std::sync::atomic::AtomicI64::new(schema.get_auto_increment_start()))
                    })
                    .value()
                    .clone()
            };
            for _ in 0..rows.len() {
                let id = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if !(0..=i64::MAX - 1000).contains(&id) {
                    return Err(StorageError::AutoIncrementOverflow(table_name.to_string()));
                }
                row_ids.push(id as u64);
            }
        } else {
            // Non-AUTO_INCREMENT: use global row_id
            let start_id = self.next_row_id.fetch_add(rows.len() as u64, std::sync::atomic::Ordering::SeqCst);
            for i in 0..rows.len() {
                row_ids.push(start_id + i as u64);
            }
        }

        // 3.5 Fill AUTO_INCREMENT PK column values in rows
        let mut rows = rows;
        if auto_inc {
            if let Some(pk_name) = schema.primary_key() {
                if let Some(pk_col) = schema.get_column(pk_name) {
                    for (i, row) in rows.iter_mut().enumerate() {
                        while row.len() <= pk_col.position {
                            row.push(Value::Null);
                        }
                        row[pk_col.position] = Value::Integer(row_ids[i] as i64);
                    }
                }
            }
            // Re-validate rows after filling PK
            for (idx, row) in rows.iter().enumerate() {
                schema.validate_row(row)
                    .map_err(|e| StorageError::InvalidData(format!(
                        "Row {} validation failed for table '{}': {}",
                        idx, table_name, e
                    )))?;
            }
        }

        // 4. Encode rows and build WAL records (shared raw bytes for WAL + LSM)
        let col_types = schema.col_types();
        let mut wal_records = Vec::with_capacity(rows.len());
        let mut kvs = Vec::with_capacity(rows.len());
        for (row_id, row) in row_ids.iter().zip(rows.iter()) {
            let composite_key = self.make_composite_key(table_name, *row_id);
            let partition = (composite_key % self.num_partitions as u64) as PartitionId;
            let row_data = row_format::encode(row, col_types)
                .unwrap_or_else(|_| bincode::serialize(row).unwrap());

            wal_records.push(WALRecord::InsertRaw {
                table_name: table_name.to_string(),
                row_id: *row_id,
                partition,
                raw_data: row_data.clone(),
                txn_id: 0,
            });

            let value = crate::storage::lsm::Value::new(row_data, *row_id);
            let composite_key = self.make_composite_key(table_name, *row_id);
            kvs.push((composite_key, value));
        }

        // 5. Batch write WAL (single fsync)
        self.wal.batch_append(0, wal_records)?;

        // 6. Batch write LSM MemTable (single lock)
        self.lsm_engine.batch_put(&kvs)?;

        // 7. Batch update all indexes
        debug_log!("[batch_insert_rows_to_table] Batch updating indexes for {} rows in table '{}'", rows.len(), table_name);

        // 7.1 按列聚合数据，批量更新 Column Index
        // DashMap direct lookup for indexed columns (zero alloc)
        let col_index_prefix = format!("{}.", table_name);

        for col_def in &schema.columns {
            let col_name = &col_def.name;

            let col_index_key = format!("{}{}", col_index_prefix, col_name);
            if self.column_indexes.contains_key(&col_index_key) {
                // 收集该列的所有数据
                let mut column_data: Vec<(RowId, Value)> = Vec::with_capacity(rows.len());
                for (row_id, row) in row_ids.iter().zip(rows.iter()) {
                    if let Some(col_value) = row.get(col_def.position) {
                        column_data.push((*row_id, col_value.clone()));
                    }
                }
                
                // 批量插入列索引
                if !column_data.is_empty() {
                    if let Err(_e) = self.batch_insert_column_values(table_name, col_name, column_data) {
                        debug_log!("[batch_insert] Failed to batch update column index '{}': {}", col_name, _e);
                        // Mark index stale so queries fall back to full scan instead of using incomplete data
                        let idx_name = format!("{}.{}", table_name, col_name);
                        self.index_registry.mark_stale(&idx_name);
                    }
                }
            }
            
            // 7.2 批量更新 Vector Index
            if let crate::types::ColumnType::Tensor(_dim) = col_def.col_type {
                if let Some(index_name) = self.index_registry.find_by_column(table_name, col_name, crate::database::index_metadata::IndexType::Vector) {
                    let mut vectors: Vec<(RowId, Vec<f32>)> = Vec::with_capacity(rows.len());
                    for (row_id, row) in row_ids.iter().zip(rows.iter()) {
                        if let Some(crate::types::Value::Vector(arc_vec)) = row.get(col_def.position) {
                            // ArcVec 是 Arc<Vec<f32>> 的包装，需要解引用
                            vectors.push((*row_id, (*arc_vec.0).clone()));
                        }
                    }
                    
                    if !vectors.is_empty() {
                        if let Err(_e) = self.batch_insert_vectors(&index_name, &vectors) {
                            debug_log!("[batch_insert] Failed to batch update vector index '{}': {}", index_name, _e);
                            self.index_registry.mark_stale(&index_name);
                        }
                    }
                }
            }
            
            // 7.3 批量更新 Text Index
            if matches!(col_def.col_type, crate::types::ColumnType::Text) {
                if let Some(index_name) = self.index_registry.find_by_column(table_name, col_name, crate::database::index_metadata::IndexType::Text) {
                    let mut texts: Vec<(RowId, String)> = Vec::with_capacity(rows.len());
                    for (row_id, row) in row_ids.iter().zip(rows.iter()) {
                        if let Some(crate::types::Value::Text(text)) = row.get(col_def.position) {
                            texts.push((*row_id, text.clone()));
                        }
                    }
                    
                    if !texts.is_empty() {
                        let texts_ref: Vec<(RowId, &str)> = texts.iter()
                            .map(|(id, s)| (*id, s.as_str()))
                            .collect();
                        if let Err(_e) = self.batch_insert_texts(&index_name, &texts_ref) {
                            debug_log!("[batch_insert] Failed to batch update text index '{}': {}", index_name, _e);
                            self.index_registry.mark_stale(&index_name);
                        }
                    }
                }
            }
            
            // 7.4 i-Octree Index (3D point cloud)
            if matches!(col_def.col_type, crate::types::ColumnType::Spatial) {
                if let Some(octree_name) = self.index_registry.find_by_column(table_name, col_name, crate::database::index_metadata::IndexType::Octree) {
                    for (row_id, row) in row_ids.iter().zip(rows.iter()) {
                        if let Some(crate::types::Value::Spatial(geom)) = row.get(col_def.position) {
                            if let Err(_e) = self.insert_ioctree_point(*row_id, &octree_name, geom) {
                                debug_log!("[batch_insert] Failed to update ioctree index '{}': {}", octree_name, _e);
                                self.index_registry.mark_stale(&octree_name);
                            }
                        }
                    }
                }
            }
            
            // 7.5 Timestamp Index (legacy single-index architecture, handled by batch build)
            // Note: Timestamp index uses a different architecture (single BTree index)
            // and is updated during flush via batch building
        }
        
        // 8. Update row count for COUNT(*) fast path
        if let Some(counter) = self.table_row_count.get(table_name) {
            use std::sync::atomic::Ordering;
            counter.fetch_add(rows.len() as u64, Ordering::SeqCst);
        }

        // 9. Increment pending counter
        // 🚀 P0 CRITICAL FIX: 使用原子操作避免锁竞争
        {
            use std::sync::atomic::Ordering;
            let old_count = self.pending_updates.fetch_add(rows.len(), Ordering::Relaxed);
            
            // 每2000条触发flush（与LSM一致）
            if old_count / 2_000 != (old_count + rows.len()) / 2_000 {
                debug_log!("[AUTO-FLUSH] Batch insert triggered after {} writes", old_count + rows.len());
                self.request_auto_flush();
            }
        }

        Ok(row_ids)
    }

    /// Batch get rows from a table (smart optimization for continuous IDs)
    /// 
    /// **Smart Strategy**:
    /// - If row_ids are continuous (e.g. [100,101,102,...]): Use LSM range scan (22-45x faster)
    /// - Otherwise: Batch point query (4-9x faster than individual calls)
    /// 
    /// # Performance
    /// - Continuous IDs: ~1-2ms for 1000 rows
    /// - Random IDs: ~5-10ms for 1000 rows
    /// - Single calls: ~45ms for 1000 rows (baseline)
    /// 
    /// # Example
    /// ```ignore
    /// let row_ids = vec![100, 101, 102, 103]; // Continuous
    /// let rows = db.get_table_rows_batch("robots", &row_ids)?;
    /// ```ignore
    pub fn get_table_rows_batch(&self, table_name: &str, row_ids: &[RowId]) -> Result<Vec<(RowId, Option<Row>)>> {
        if row_ids.is_empty() {
            return Ok(Vec::new());
        }
        
        // Validate table exists
        let _schema = self.table_registry.get_table(table_name)?;
        
        // Smart path selection: Detect continuous row_ids
        let is_continuous = self.is_continuous_row_ids(row_ids);
        
        if is_continuous {
            // Use LSM range scan (much faster for continuous IDs)
            self.get_table_rows_batch_range(table_name, row_ids)
        } else {
            // Use batch point query
            self.get_table_rows_batch_point(table_name, row_ids)
        }
    }
    
    // ==================== Internal Helpers ====================
    
    /// Increment pending updates counter and trigger auto-flush if needed
    /// 🚀 P0 CRITICAL FIX: 使用原子操作避免锁竞争，解决 CPU 飙升问题
    fn increment_pending_updates(&self) {
        use std::sync::atomic::Ordering;
        
        let count = self.pending_updates.fetch_add(1, Ordering::Relaxed);
        
        // 每2000条触发一次flush（与LSM一致）
        if count.is_multiple_of(2_000) && count > 0 {
            debug_log!("[AUTO-FLUSH] Triggered after {} writes", count);
            self.request_auto_flush();
        }
    }
    
    /// Trigger background prefetch
    /// 
    /// ⚠️ IMPORTANT: This method MUST NOT call get_table_rows_batch() to avoid infinite recursion!
    fn trigger_prefetch(&self, table_name: &str, start_row_id: RowId, count: usize, stride: i64) {
        let mut row_ids_to_fetch = Vec::with_capacity(count);
        let mut current_id = start_row_id as i64;

        // Generate row_ids based on stride
        for _ in 0..count {
            if current_id > 0 {
                row_ids_to_fetch.push(current_id as RowId);
            }
            current_id += stride;

            // Safety check
            if !(0..=i64::MAX / 2).contains(&current_id) {
                break;
            }
        }

        // Record prefetch attempt
        self.row_cache.record_prefetch(row_ids_to_fetch.len());

        // Get schema for correct type-aware decoding (decode_any treats all fixed cols as Integer!)
        let col_types = match self.table_registry.get_table(table_name) {
            Ok(schema) => schema.col_types().to_vec(),
            Err(_) => return,
        };

        // Directly fetch from LSM without triggering get_table_rows_batch (avoid recursion)
        for row_id in row_ids_to_fetch {
            let composite_key = self.make_composite_key(table_name, row_id);

            if let Ok(Some(value)) = self.lsm_engine.get(composite_key) {
                if !value.deleted {
                    if let crate::storage::lsm::ValueData::Inline(bytes) = &value.data {
                        if let Ok(row) = crate::storage::row_format::decode(bytes, &col_types) {
                            self.row_cache.put(table_name.to_string(), row_id, row);
                            self.row_cache.record_prefetch_hit();
                        }
                    }
                }
            }
        }
    }
    
    /// Check if row_ids are continuous
    fn is_continuous_row_ids(&self, row_ids: &[RowId]) -> bool {
        if row_ids.len() < 2 {
            return false;
        }
        
        for i in 1..row_ids.len() {
            if row_ids[i] != row_ids[i - 1] + 1 {
                return false;
            }
        }
        
        true
    }
    
    /// Batch get using LSM range scan (for continuous row_ids)
    fn get_table_rows_batch_range(&self, table_name: &str, row_ids: &[RowId]) -> Result<Vec<(RowId, Option<Row>)>> {
        let min_id = *row_ids.iter().min().unwrap();
        let max_id = *row_ids.iter().max().unwrap();
        
        let start_key = self.make_composite_key(table_name, min_id);
        let end_key = self.make_composite_key(table_name, max_id + 1);
        
        let lsm_rows = self.lsm_engine.scan_range(start_key, end_key)?;
        
        let mut result = Vec::new();
        for (composite_key, value) in lsm_rows {
            let row_id = (composite_key & 0xFFFFFFFF) as RowId;
            
            if value.deleted {
                result.push((row_id, None));
                continue;
            }
            
            let data = match &value.data {
                crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                crate::storage::lsm::ValueData::Blob(_) => {
                    return Err(StorageError::InvalidData("Blob not supported".into()));
                }
            };
            
            let row: Row = if let Ok(s) = self.table_registry.get_table(table_name) {
                    crate::storage::row_format::decode(data, s.col_types())
                        .map_err(|e| StorageError::Serialization(e.to_string()))?
                } else {
                    crate::storage::row_format::decode_any(data)
                        .map_err(|e| StorageError::Serialization(e.to_string()))?
                };
            
            // Cache row
            self.row_cache.put(table_name.to_string(), row_id, row.clone());
            result.push((row_id, Some(row)));
        }
        
        Ok(result)
    }
    
    /// Batch get using point queries (for random row_ids)
    /// 
    /// 🚀 OPTIMIZED: Detects continuous segments and uses range scan
    /// 
    /// ## Strategy
    /// - Segments >= 10 IDs: Use LSM range scan (~0.3ms/100 rows)
    /// - Segments < 10 IDs: Use point query (~4ms/row)
    /// 
    /// ## Performance
    /// Example: 30K row_ids in 300 segments (100 IDs each)
    /// - Old: 30K × 4ms = 120s
    /// - New: 300 × 0.3ms = 90ms (1333x faster!)
    /// 
    /// 🌊 STREAMING: Processes in batches to avoid loading all rows into memory
    /// - Old: 30K rows × 1KB = 30MB peak memory
    /// - New: 1K rows × 1KB = 1MB peak memory (30x reduction!)
    fn get_table_rows_batch_point(&self, table_name: &str, row_ids: &[RowId]) -> Result<Vec<(RowId, Option<Row>)>> {
        if row_ids.is_empty() {
            return Ok(Vec::new());
        }
        
        // 🌊 STREAMING OPTIMIZATION: Process in batches to reduce memory usage
        // Batch size: 1000 rows (~1MB memory, good balance)
        const STREAMING_BATCH_SIZE: usize = 1000;
        
        // Only use streaming for large datasets (> 5K rows)
        if row_ids.len() <= 5_000 {
            // Small dataset: use original implementation (no memory issue)
            return self.get_table_rows_batch_point_internal(table_name, row_ids);
        }
        
        // Large dataset: use streaming
        debug_log!(
            "[Streaming] Processing {} rows in batches of {} (memory-efficient mode)",
            row_ids.len(), STREAMING_BATCH_SIZE
        );
        
        let mut result = Vec::with_capacity(row_ids.len());
        
        // Process in chunks
        for chunk in row_ids.chunks(STREAMING_BATCH_SIZE) {
            let batch_result = self.get_table_rows_batch_point_internal(table_name, chunk)?;
            result.extend(batch_result);
            
            // Optional: Log progress for very large batches
            if row_ids.len() > 20_000 {
                debug_log!(
                    "[Streaming] Progress: {}/{} rows ({:.1}%)",
                    result.len(), row_ids.len(),
                    (result.len() as f64 / row_ids.len() as f64) * 100.0
                );
            }
        }
        
        Ok(result)
    }
    
    /// Internal implementation of batch point query (without streaming)
    /// 
    /// Called by `get_table_rows_batch_point` for each streaming batch.
    fn get_table_rows_batch_point_internal(&self, table_name: &str, row_ids: &[RowId]) -> Result<Vec<(RowId, Option<Row>)>> {
        if row_ids.is_empty() {
            return Ok(Vec::new());
        }
        
        // 🚀 Detect continuous segments
        let segments = self.detect_continuous_segments(row_ids);
        
        let mut result = Vec::with_capacity(row_ids.len());
        
        for segment in segments {
            if segment.len() >= 10 {
                // 🚀 Use LSM range scan for continuous segment
                let min_id = segment[0];
                let max_id = segment[segment.len() - 1];
                
                let start_key = self.make_composite_key(table_name, min_id);
                let end_key = self.make_composite_key(table_name, max_id + 1);
                
                let lsm_rows = self.lsm_engine.scan_range(start_key, end_key)?;
                
                for (composite_key, value) in lsm_rows {
                    let row_id = (composite_key & 0xFFFFFFFF) as RowId;
                    
                    if value.deleted {
                        result.push((row_id, None));
                        continue;
                    }
                    
                    let data = match &value.data {
                        crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                        crate::storage::lsm::ValueData::Blob(_) => {
                            return Err(StorageError::InvalidData("Blob not supported".into()));
                        }
                    };
                    
                    let row: Row = if let Ok(s) = self.table_registry.get_table(table_name) {
                            crate::storage::row_format::decode(data, s.col_types())
                                .map_err(|e| StorageError::Serialization(e.to_string()))?
                        } else {
                            crate::storage::row_format::decode_any(data)
                                .map_err(|e| StorageError::Serialization(e.to_string()))?
                        };

                        self.row_cache.put(table_name.to_string(), row_id, row.clone());
                        result.push((row_id, Some(row)));
                    }
                } else {
                    // Use point query for small segments
                    for &row_id in &segment {
                        let row = self.get_table_row(table_name, row_id)?;
                        result.push((row_id, row));
                    }
                }
            }

            Ok(result)
        }
    
    /// Detect continuous segments in sorted row_ids
    ///
    /// ## Example
    /// ```text
    /// Input:  [100, 101, 102, 105, 106, 200, 201, 202]
    /// Output: [[100,101,102], [105,106], [200,201,202]]
    /// ```
    fn detect_continuous_segments(&self, row_ids: &[RowId]) -> Vec<Vec<RowId>> {
        if row_ids.is_empty() {
            return Vec::new();
        }
        
        let mut segments = Vec::new();
        let mut current_segment = vec![row_ids[0]];
        
        for i in 1..row_ids.len() {
            if row_ids[i] == row_ids[i-1] + 1 {
                // Continuous
                current_segment.push(row_ids[i]);
            } else {
                // Gap detected, start new segment
                segments.push(current_segment);
                current_segment = vec![row_ids[i]];
            }
        }
        
        // Don't forget the last segment
        segments.push(current_segment);
        
        segments
    }
}

// ==================== Helper Functions ====================

/// 🚀 PHASE B.2: Partial deserialization - only deserialize required columns
/// 
/// Uses serde's `IgnoredAny` to skip unwanted columns without allocating memory.
/// 
/// ## Performance
/// - Deserializing 2/10 columns: 5x faster (400µs → 80µs)
/// - Deserializing 5/10 columns: 2x faster (400µs → 200µs)
/// 
/// ## How it works
/// ```text
/// Row format: Vec<Value> = [val1, val2, val3, ...]
///
/// For each column:
///   if required → Deserialize to Value
///   else       → Deserialize to IgnoredAny (skip bytes, no allocation)
/// ```
fn deserialize_partial(
    data: &[u8],
    required_columns: &[String],
    schema: &crate::types::TableSchema,
) -> Result<crate::types::SqlRow> {
    use serde::de::{Deserialize, IgnoredAny};
    use crate::types::{SqlRow, Value};
    
    let mut sql_row = SqlRow::new();
    
    // Create deserializer
    let mut deserializer = bincode::Deserializer::from_slice(
        data,
        bincode::options()
    );
    
    // Bincode Vec format: [length][element1][element2]...
    // First, deserialize the Vec length
    let _len: usize = match Deserialize::deserialize(&mut deserializer) {
        Ok(l) => l,
        Err(e) => return Err(StorageError::Serialization(format!("Failed to deserialize Vec length: {}", e))),
    };
    
    // Then deserialize each element (column value)
    for col_def in &schema.columns {
        if required_columns.contains(&col_def.name) {
            // Deserialize this column
            let value: Value = match Deserialize::deserialize(&mut deserializer) {
                Ok(v) => v,
                Err(e) => return Err(StorageError::Serialization(
                    format!("Failed to deserialize column {}: {}", col_def.name, e)
                )),
            };
            sql_row.insert(col_def.name.clone(), value);
        } else {
            // 🚀 Skip this column (only advance deserializer pointer, no allocation)
            let _: IgnoredAny = match Deserialize::deserialize(&mut deserializer) {
                Ok(v) => v,
                Err(e) => return Err(StorageError::Serialization(
                    format!("Failed to skip column {}: {}", col_def.name, e)
                )),
            };
        }
    }
    
    Ok(sql_row)
}

/// 🚀 表行批量迭代器
/// 
/// 每次返回一批行数据，避免一次性加载全部数据到内存。
pub struct TableRowBatchedIterator {
    lsm_iter: crate::storage::lsm::LSMBatchedIterator,
    _table_name: String,
    col_types: Option<Vec<crate::types::ColumnType>>,
}

impl Iterator for TableRowBatchedIterator {
    type Item = Result<Vec<(RowId, Row)>>;
    
    fn next(&mut self) -> Option<Self::Item> {
        match self.lsm_iter.next() {
            Some(Ok(batch)) => {
                let mut result = Vec::with_capacity(batch.len());
                
                for (composite_key, value) in batch {
                    // Skip tombstones (deleted rows)
                    if value.deleted {
                        continue;
                    }

                    // Extract row_id from composite_key
                    let row_id = (composite_key & 0xFFFFFFFF) as RowId;
                    
                    // Extract data
                    let data = match &value.data {
                        crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                        crate::storage::lsm::ValueData::Blob(_) => {
                            return Some(Err(StorageError::InvalidData(
                                "Blob references should be resolved by LSM engine".into()
                            )));
                        }
                    };
                    
                    // Deserialize row: prefer schema-aware decode
                    let row: Row = if let Some(ref col_types) = self.col_types {
                        match crate::storage::row_format::decode(data, col_types) {
                            Ok(row) => row,
                            Err(_) => match crate::storage::row_format::decode_any(data) {
                                Ok(row) => row,
                                Err(e) => return Some(Err(StorageError::Serialization(e.to_string()))),
                            },
                        }
                    } else {
                        match crate::storage::row_format::decode_any(data) {
                            Ok(row) => row,
                            Err(e) => return Some(Err(StorageError::Serialization(e.to_string()))),
                        }
                    };
                    
                    result.push((row_id, row));
                }
                
                Some(Ok(result))
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

/// 🚀 表行流式迭代器（真正的 O(1) 内存占用）
///
/// 逐个返回行数据，不预先加载任何数据到内存。
pub struct TableRowStreamingIterator {
    lsm_iter: crate::storage::lsm::MergingIterator,
    col_types: Option<Vec<crate::types::ColumnType>>,
}

impl Iterator for TableRowStreamingIterator {
    type Item = Result<(RowId, Row)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.lsm_iter.next() {
            Some(Ok((composite_key, value))) => {
                let row_id = (composite_key & 0xFFFFFFFF) as RowId;

                let data = match &value.data {
                    crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                    crate::storage::lsm::ValueData::Blob(_) => {
                        return Some(Err(StorageError::InvalidData(
                            "Blob references should be resolved by LSM engine".into()
                        )));
                    }
                };

                // Deserialize row: prefer schema-aware decode, fallback to decode_any
                let row: Row = if let Some(ref col_types) = self.col_types {
                    match crate::storage::row_format::decode(data, col_types) {
                        Ok(row) => row,
                        Err(_) => match crate::storage::row_format::decode_any(data) {
                            Ok(row) => row,
                            Err(e) => return Some(Err(e)),
                        },
                    }
                } else {
                    match crate::storage::row_format::decode_any(data) {
                        Ok(row) => row,
                        Err(e) => return Some(Err(e)),
                    }
                };

                Some(Ok((row_id, row)))
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}
