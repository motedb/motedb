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
use crate::types::{ColumnType, Row, RowId, PartitionId, Value};
use crate::txn::wal::WALRecord;
use crate::storage::row_format;
use super::core::MoteDB;
use std::sync::Arc;
use std::collections::HashSet;

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
                                // Verify at least one RowId still exists in LSM.
                                // Column indexes can return stale RowIds after compaction.
                                let mut has_live = false;
                                for &rid in &found {
                                    if self.get_table_row(table_name, rid)?.is_some() {
                                        has_live = true;
                                        break;
                                    }
                                }
                                if has_live {
                                    return Err(StorageError::InvalidData(format!(
                                        "Duplicate primary key {:?} for table '{}'", pk_value, table_name
                                    )));
                                }
                            }
                            _ => {} // Not found or index not available — proceed
                        }
                    }
                }
            }
        }
        
        // 2. 🚀 P3+4: For AUTO_INCREMENT primary key, use per-table counter
        // Ensure row has enough slots for AUTO_INCREMENT PK column before validation
        if schema.is_primary_key_auto_increment() {
            if let Some(pk_col_name) = schema.primary_key() {
                if let Some(pk_col) = schema.get_column(pk_col_name) {
                    while row.len() <= pk_col.position {
                        row.push(Value::Null);
                    }
                }
            }
        }

        // 3. Validate row (before allocating AUTO_INCREMENT to avoid ID waste)
        schema.validate_row(&row)
            .map_err(|e| StorageError::InvalidData(format!(
                "Row validation failed for table '{}': {}",
                table_name, e
            )))?;

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
            let id = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if !(0..=i64::MAX - 1000).contains(&id) {
                return Err(StorageError::AutoIncrementOverflow(table_name.to_string()));
            }

            // P2: Update persisted counter (lazy — persisted during checkpoint)
            if let Err(e) = self.table_registry.update_auto_increment_counter(table_name, id) {
                warn_log!("[MoteDB] Auto-increment counter update failed for {}: {}", table_name, e);
            }

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
            self.next_row_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        };

        // 4. Determine partition
        let composite_key = self.make_composite_key(table_name, row_id);
        let partition = (composite_key % self.num_partitions as u64) as PartitionId;

        // 5. Encode row to raw bytes (shared between WAL and LSM — zero-copy recovery)
        let col_types = schema.col_types();
        let row_data = row_format::encode(&row, col_types)
            .or_else(|_| bincode::serialize(&row)
                .map_err(|e| StorageError::Serialization(format!("Row encode failed: {}", e))))?;

        // 6. Increment pending counter BEFORE WAL write (checkpoint uses this as barrier)
        self.increment_pending_updates();

        // 7. Write to WAL first (durability) — by reference, zero clone
        self.wal.log_insert_raw_ref(table_name, partition, row_id, &row_data, 0)?;

        // 7. Write to LSM MemTable (invalidate cache first for TOCTOU safety)
        let composite_key = self.make_composite_key(table_name, row_id);
        self.row_cache.invalidate(table_name, row_id);
        let ts = self.write_lsn.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let value = crate::storage::lsm::Value::new(row_data, ts);
        self.lsm_engine.put(composite_key, value)?;
        self.row_cache.put(table_name.to_string(), row_id, row.clone());

        // 7. Update indexes
        {
        let mut index_errors: Vec<String> = Vec::new();

        // Reusable index key buffer (allocated once, reused per column)
        let mut index_key_buf = String::with_capacity(table_name.len() + 1 + 16);

        for col_def in &schema.columns {
            let col_name = &col_def.name;
            let col_value = row.get(col_def.position);

            let Some(col_value) = col_value else { continue; };

            // In-memory PK lookup (O(1) resolution, bypasses disk-based B-Tree)
            if let Some(pk_name) = schema.primary_key() {
                if col_name == pk_name && !schema.is_primary_key_auto_increment() {
                    if let Some(lookup) = self.pk_lookup.get(table_name) {
                        lookup.insert(crate::database::pk_cache::PkKey::from_value(col_value), row_id);
                    }
                }
            }

            // 7.1 Column Index — reuse key buffer to avoid per-column allocation
            {
                index_key_buf.clear();
                index_key_buf.push_str(table_name);
                index_key_buf.push('.');
                index_key_buf.push_str(col_name);
                if let Some(index_ref) = self.column_indexes.get(&index_key_buf) {
                    // NULL values are valid SQL but not indexable — skip silently
                    if !matches!(col_value, Value::Null) {
                        if let Err(_e) = index_ref.value().insert(col_value, row_id) {
                            debug_log!("[insert_row] Failed to update column index '{}': {}", col_name, _e);
                            index_errors.push(index_key_buf.clone());
                        }
                    }
                }
            }

            // 7.2 Vector Index
            if let crate::types::ColumnType::Tensor(_dim) = col_def.col_type {
                if let Some(index_name) = self.index_registry.find_by_column(
                    table_name,
                    col_name,
                    crate::database::index_metadata::IndexType::Vector
                ) {
                    let f32_vec = match col_value {
                        crate::types::Value::Vector(vec) => Some(vec.as_slice().to_vec()),
                        crate::types::Value::Tensor(tensor) => Some(tensor.to_f32()),
                        _ => None,
                    };
                    if let Some(vec) = f32_vec {
                        if let Err(_e) = self.update_vector(row_id, &index_name, &vec) {
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

        // Mark only the individual failed indexes as stale
        if !index_errors.is_empty() {
            debug_log!("[insert_row] {} index updates failed for table '{}', marking stale",
                     index_errors.len(), table_name);
            for idx_name in &index_errors {
                self.index_registry.mark_stale(idx_name);
            }
        }
        } // end index_update_strategy check

        // 9. Increment row count for COUNT(*) fast path
        if let Some(counter) = self.table_row_count.get(table_name) {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
        let schema = self.table_registry.get_table(table_name)?;
        self.get_table_row_with_schema(table_name, row_id, &schema)
    }

    /// Get a row using a pre-fetched schema (avoids redundant RwLock acquisition).
    pub fn get_table_row_with_schema(&self, table_name: &str, row_id: RowId, schema: &crate::types::TableSchema) -> Result<Option<Row>> {
        // Try cache first
        if let Some(row_arc) = self.row_cache.get(table_name, row_id) {
            if let Some((next_row_id, count, stride)) = self.row_cache.check_prefetch(table_name, row_id) {
                self.trigger_prefetch(table_name, next_row_id, count, stride);
            }
            return Ok(Some((*row_arc).clone()));
        }

        // Cache miss - load from LSM
        let composite_key = self.make_composite_key(table_name, row_id);

        if let Some(value) = self.lsm_engine.get(composite_key)? {
            if value.deleted {
                return Ok(None);
            }

            let data = match &value.data {
                crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                crate::storage::lsm::ValueData::Blob(_) => {
                    return Err(StorageError::InvalidData(
                        "Blob values not yet supported in get_table_row".into()
                    ));
                }
            };

            let col_types = schema.col_types();
            let fc = row_format::compute_fixed_count(col_types);
            let row: Row = row_format::decode_fast(data, col_types, fc)
                .map_err(|e| StorageError::Serialization(format!(
                    "Failed to deserialize row {}: {}",
                    row_id, e
                )))?;

            let row_arc = Arc::new(row);
            self.row_cache.put_arc(table_name.to_string(), row_id, Arc::clone(&row_arc));

            if let Some((next_row_id, count, stride)) = self.row_cache.check_prefetch(table_name, row_id) {
                self.trigger_prefetch(table_name, next_row_id, count, stride);
            }

            Ok(Some(Arc::try_unwrap(row_arc).unwrap_or_else(|a| (*a).clone())))
        } else {
            Ok(None)
        }
    }

    /// Get a row as Arc<Row> — avoids cloning the row data for cache hits.
    /// Use when the caller doesn't need to modify the row (PK SELECT fast path).
    pub fn get_table_row_arc(&self, table_name: &str, row_id: RowId, schema: &crate::types::TableSchema) -> Result<Option<Arc<Row>>> {
        // Fast path: skip prefetch tracking for single-row lookups
        if let Some(row_arc) = self.row_cache.get_fast(table_name, row_id) {
            if let Some((next_row_id, count, stride)) = self.row_cache.check_prefetch(table_name, row_id) {
                self.trigger_prefetch(table_name, next_row_id, count, stride);
            }
            return Ok(Some(row_arc));
        }

        // Cache miss — load from LSM (no prefetch for single-row PK lookup)
        let composite_key = self.make_composite_key(table_name, row_id);
        if let Some(value) = self.lsm_engine.get(composite_key)? {
            if value.deleted { return Ok(None); }
            let data = match &value.data {
                crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                crate::storage::lsm::ValueData::Blob(_) => {
                    return Err(StorageError::InvalidData("Blob values not yet supported".into()));
                }
            };
            let col_types = schema.col_types();
            let fc = row_format::compute_fixed_count(col_types);
            let row: Row = row_format::decode_fast(data, col_types, fc)
                .map_err(|e| StorageError::Serialization(format!("Failed to deserialize row {}: {}", row_id, e)))?;
            let row_arc = Arc::new(row);
            self.row_cache.put_arc(table_name.to_string(), row_id, Arc::clone(&row_arc));
            Ok(Some(row_arc))
        } else {
            Ok(None)
        }
    }

    /// Read a row with MVCC snapshot isolation. For transactional reads, the version
    /// store's `get_visible_version` is consulted to filter out rows that are not yet
    /// committed under the given snapshot. Rows that were inserted via the auto-commit
    /// path (not through a transaction) have no version-store entry and are always visible.
    pub fn get_table_row_arc_with_mvcc(
        &self,
        table_name: &str,
        row_id: RowId,
        schema: &crate::types::TableSchema,
        snapshot: &crate::txn::Snapshot,
        isolation: crate::txn::IsolationLevel,
    ) -> Result<Option<Arc<Row>>> {
        // Check version store first — if this row_id has transactional data,
        // version store visibility rules take precedence over LSM.
        if let Some(visible) = self.version_store.get_visible_version(row_id, snapshot, isolation)? {
            return Ok(Some(Arc::new(visible)));
        }

        // Check if version store has any entry (even if not visible). If so, the row
        // exists transactionally but is hidden by MVCC — don't fall through to LSM.
        if self.version_store.versions.get(&row_id).is_some() {
            return Ok(None); // Row exists but is not visible under this snapshot
        }

        // No version store entry — fall through to LSM (auto-commit path)
        self.get_table_row_arc(table_name, row_id, schema)
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
        let schema = self.table_registry.get_table(table_name)?;
        self.update_row_in_table_with_schema(table_name, row_id, old_row, new_row, &schema)
    }

    /// Update a row with pre-resolved schema (avoids redundant lookup).
    pub fn update_row_in_table_with_schema(&self, table_name: &str, row_id: RowId, old_row: Row, new_row: Row, schema: &crate::types::TableSchema) -> Result<()> {
        ensure_open!(self);
        
        // 2. Construct composite key
        let composite_key = self.make_composite_key(table_name, row_id);
        
        // 3. Determine partition
        let partition = (composite_key % self.num_partitions as u64) as PartitionId;

        // 4. Encode rows to raw bytes
        let col_types = schema.col_types();
        let raw_old = row_format::encode(&old_row, col_types)
            .or_else(|_| bincode::serialize(&old_row)
                .map_err(|e| StorageError::Serialization(format!("Row encode failed: {}", e))))?;
        let raw_new = row_format::encode(&new_row, col_types)
            .or_else(|_| bincode::serialize(&new_row)
                .map_err(|e| StorageError::Serialization(format!("Row encode failed: {}", e))))?;

        // 5. Increment pending counter BEFORE WAL write (checkpoint barrier)
        self.increment_pending_updates();

        // 6. Write to WAL first (durability) — raw bytes
        self.wal.log_update_raw_ref(table_name, partition, row_id, &raw_old, &raw_new, 0)?;

        // 6. Update in LSM MemTable (same bytes as WAL)
        // Invalidate cache BEFORE LSM write to prevent TOCTOU stale reads
        self.row_cache.invalidate(table_name, row_id);

        let timestamp = self.write_lsn.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let value = crate::storage::lsm::Value::new(raw_new, timestamp);
        self.lsm_engine.put(composite_key, value)?;

        // Re-invalidate: catch concurrent readers that cached old data between
        // the first invalidate and the LSM write
        self.row_cache.invalidate(table_name, row_id);

        // 6. Update indexes. Collect failures, then mark ALL stale consistently.
        let mut index_errors = Vec::new();

        // Reusable index key buffer
        let mut index_key_buf = String::with_capacity(table_name.len() + 1 + 16);

        for col_def in &schema.columns {
            let col_name = &col_def.name;
            let old_value = old_row.get(col_def.position);
            let new_value = new_row.get(col_def.position);

            // Skip unchanged columns
            if old_value == new_value {
                continue;
            }

            // 6.1 Column Index — reuse key buffer
            {
                index_key_buf.clear();
                index_key_buf.push_str(table_name);
                index_key_buf.push('.');
                index_key_buf.push_str(col_name);
            }
            if let Some(index_ref) = self.column_indexes.get(&index_key_buf) {
                let index = index_ref.value();
                let old_is_null = old_value.is_none() || matches!(old_value, Some(Value::Null));
                let new_is_null = new_value.is_none() || matches!(new_value, Some(Value::Null));

                if !old_is_null && !new_is_null {
                    if let (Some(old_val), Some(new_val)) = (old_value, new_value) {
                        if let Err(_e) = index.update(old_val, new_val, row_id) {
                            debug_log!("[update_row] Failed to update column index '{}': {}", col_name, _e);
                            index_errors.push(index_key_buf.clone());
                        }
                    }
                } else if !old_is_null && new_is_null {
                    if let Some(old_val) = old_value {
                        if let Err(_e) = index.delete(old_val, row_id) {
                            debug_log!("[update_row] Failed to delete column index '{}': {}", col_name, _e);
                            index_errors.push(index_key_buf.clone());
                        }
                    }
                } else if old_is_null && !new_is_null {
                    if let Some(new_val) = new_value {
                        if let Err(_e) = index.insert(new_val, row_id) {
                            debug_log!("[update_row] Failed to insert column index '{}': {}", col_name, _e);
                            index_errors.push(index_key_buf.clone());
                        }
                    }
                }
                // NULL -> NULL: no index change needed
            }

            // 6.2 Vector Index
            if let crate::types::ColumnType::Tensor(_dim) = col_def.col_type {
                if let Some(index_name) = self.index_registry.find_by_column(
                    table_name,
                    col_name,
                    crate::database::index_metadata::IndexType::Vector
                ) {
                    let mut failed = false;
                    if let Err(_e) = self.delete_vector(row_id, &index_name) {
                        debug_log!("[update_row] Failed to delete old vector '{}': {}", index_name, _e);
                        failed = true;
                    }

                    if let Some(new_vec) = new_value.and_then(|v| match v {
                        crate::types::Value::Vector(vec) => Some(vec.as_slice().to_vec()),
                        crate::types::Value::Tensor(tensor) => Some(tensor.to_f32()),
                        _ => None,
                    }) {
                        if let Err(_e) = self.update_vector(row_id, &index_name, &new_vec) {
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
                if let Some(index_name) = self.index_registry.find_by_column(table_name, col_name, crate::database::index_metadata::IndexType::Text) {
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
        let timestamp = self.write_lsn.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // 5. Write to WAL first (durability guarantee)
        //    WAL must be written BEFORE any mutation so that a crash at any
        //    point below can be recovered correctly.
        // 5. Write to WAL first (durability guarantee) — raw bytes
        let col_types = schema.col_types();
        let raw_old = row_format::encode(&old_row, col_types)
            .or_else(|_| bincode::serialize(&old_row)
                .map_err(|e| StorageError::Serialization(format!("Row encode failed: {}", e))))?;
        self.increment_pending_updates();
        self.wal.log_delete_raw(table_name, partition, composite_key, raw_old, timestamp, 0)?;

        // 6. Invalidate cache BEFORE tombstone write (prevent TOCTOU stale reads)
        self.row_cache.invalidate(table_name, row_id);

        // 7. Delete from LSM (using tombstone)
        self.lsm_engine.delete(composite_key, timestamp)?;

        // Re-invalidate: catch concurrent readers that cached old data between
        // the first invalidate and the LSM write
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

        // DashMap direct lookup for indexed columns
        let prefix_len = table_name.len() + 1;

        for col_def in &schema.columns {
            let col_name = &col_def.name;
            let col_value = old_row.get(col_def.position);

            let Some(col_value) = col_value else { continue; };

            // Column Index — single DashMap lookup
            let mut col_index_key = String::with_capacity(prefix_len + col_name.len());
            col_index_key.push_str(table_name);
            col_index_key.push('.');
            col_index_key.push_str(col_name);
            if let Some(index_ref) = self.column_indexes.get(&col_index_key) {
                if let Err(_e) = index_ref.value().delete(col_value, row_id) {
                    debug_log!("[delete_row] Failed to delete from column index '{}': {}", col_name, _e);
                    self.index_registry.mark_stale(&col_index_key);
                }
            }

            // Vector Index
            if let crate::types::ColumnType::Tensor(_dim) = col_def.col_type {
                if let Some(index_name) = self.index_registry.find_by_column(
                    table_name,
                    col_name,
                    crate::database::index_metadata::IndexType::Vector
                ) {
                    if let Err(_e) = self.delete_vector(row_id, &index_name) {
                        debug_log!("[delete_row] Failed to delete from vector index '{}': {}", index_name, _e);
                        self.index_registry.mark_stale(&index_name);
                    }
                }
            }

            // Text Index
            if matches!(col_def.col_type, crate::types::ColumnType::Text) {
                if let Some(index_name) = self.index_registry.find_by_column(table_name, col_name, crate::database::index_metadata::IndexType::Text) {
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
            fixed_count: crate::storage::row_format::compute_fixed_count(schema.col_types()),
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
            fixed_count: crate::storage::row_format::compute_fixed_count(&col_types),
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

    /// Fast row count from atomic counter (O(1), may be approximate)
    pub fn fast_row_count(&self, table_name: &str) -> Option<u64> {
        self.table_row_count.get(table_name).map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
    }

    /// 🚀 PHASE B.2: Scan table rows with partial deserialization
    ///
    /// Only deserializes the columns specified in `col_positions`, skipping others.
    /// Uses a reusable output buffer to avoid per-row allocations.
    ///
    /// ## Performance
    /// - SELECT 2/10 columns: 5x faster (400µs → 80µs)
    /// - SELECT 5/10 columns: 2x faster (400µs → 200µs)
    /// - SELECT * : fallback to full deserialization
    pub fn scan_table_rows_partial(
        &self,
        table_name: &str,
        col_positions: &[usize],
    ) -> Result<TableRowPartialIterator> {
        ensure_open!(self);
        let schema = self.table_registry.get_table(table_name)?;
        let col_types = schema.col_types();
        let fixed_count = crate::storage::row_format::compute_fixed_count(&col_types);

        let table_prefix = self.compute_table_prefix(table_name);
        let start_key = table_prefix << 32;
        let end_key = (table_prefix + 1) << 32;

        let lsm_iter = self.lsm_engine.scan_range_streaming(start_key, end_key)?;

        Ok(TableRowPartialIterator {
            lsm_iter,
            col_types: col_types.to_vec(),
            fixed_count,
            col_positions: col_positions.to_vec(),
            out_buf: Vec::new(),
        })
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
    pub fn batch_insert_rows_to_table(&self, table_name: &str, mut rows: Vec<Row>) -> Result<Vec<RowId>> {
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
                    let mut batch_pks: HashSet<crate::database::pk_cache::PkKey> = HashSet::with_capacity(rows.len());
                    for (idx, row) in rows.iter().enumerate() {
                        if let Some(pk_value) = row.get(pk_col.position) {
                            let pk_key = crate::database::pk_cache::PkKey::from_value(pk_value);
                            // Intra-batch duplicate check
                            if !batch_pks.insert(pk_key.clone()) {
                                return Err(StorageError::InvalidData(format!(
                                    "Batch row {}: duplicate primary key {:?} within batch for table '{}'", idx, pk_value, table_name
                                )));
                            }
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
                                    // Verify at least one RowId still exists in LSM
                                    let mut has_live = false;
                                    for &rid in &found {
                                        if self.get_table_row(table_name, rid)?.is_some() {
                                            has_live = true;
                                            break;
                                        }
                                    }
                                    if has_live {
                                        return Err(StorageError::InvalidData(format!(
                                            "Batch row {}: duplicate primary key {:?} for table '{}'", idx, pk_value, table_name
                                        )));
                                    }
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

        // Ensure all rows have enough slots for AUTO_INCREMENT PK column
        if auto_inc {
            if let Some(pk_name) = schema.primary_key() {
                if let Some(pk_col) = schema.get_column(pk_name) {
                    for row in rows.iter_mut() {
                        while row.len() <= pk_col.position {
                            row.push(Value::Null);
                        }
                    }
                }
            }
        }

        // Pre-validate all rows before allocating IDs (avoid wasting AUTO_INCREMENT IDs on invalid rows)
        {
            for (idx, row) in rows.iter().enumerate() {
                schema.validate_row(row)
                    .map_err(|e| StorageError::InvalidData(format!(
                        "Row {} validation failed for table '{}': {}",
                        idx, table_name, e
                    )))?;
            }
        }

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
                let id = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
        }

        // 4. Encode rows and build WAL records (shared raw bytes for WAL + LSM)
        let col_types = schema.col_types();
        let mut wal_records = Vec::with_capacity(rows.len());
        let mut kvs = Vec::with_capacity(rows.len());
        for (row_id, row) in row_ids.iter().zip(rows.iter()) {
            let composite_key = self.make_composite_key(table_name, *row_id);
            let partition = (composite_key % self.num_partitions as u64) as PartitionId;
            let row_data = row_format::encode(row, col_types)
                .or_else(|_| bincode::serialize(row)
                    .map_err(|e| StorageError::Serialization(format!("Row encode failed: {}", e))))?;

            wal_records.push(WALRecord::InsertRaw {
                table_name: table_name.to_string(),
                row_id: *row_id,
                partition,
                raw_data: row_data.clone(),
                txn_id: 0,
            });

            let ts = self.write_lsn.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let value = crate::storage::lsm::Value::new(row_data, ts);
            let composite_key = self.make_composite_key(table_name, *row_id);
            kvs.push((composite_key, value));
        }

        // 5. Increment pending counter BEFORE batch WAL write (checkpoint barrier)
        let old_count = self.pending_updates.fetch_add(rows.len(), std::sync::atomic::Ordering::Release);

        // 6. Batch write WAL (single fsync)
        self.wal.batch_append(0, wal_records)?;

        // 6. Batch write LSM MemTable (single lock)
        self.lsm_engine.batch_put(&kvs)?;

        // 6.5 Update PK cache for non-auto_increment tables
        if !auto_inc {
            if let Some(pk_name) = schema.primary_key() {
                if let Some(pk_col) = schema.get_column(pk_name) {
                    if let Some(lookup) = self.pk_lookup.get(table_name) {
                        for (i, row) in rows.iter().enumerate() {
                            if let Some(pk_value) = row.get(pk_col.position) {
                                lookup.insert(
                                    crate::database::pk_cache::PkKey::from_value(pk_value),
                                    row_ids[i],
                                );
                            }
                        }
                    }
                }
            }
        }

        // 7. Batch update all indexes
        debug_log!("[batch_insert_rows_to_table] Batch updating indexes for {} rows in table '{}'", rows.len(), table_name);

        // 7.1 按列聚合数据，批量更新 Column Index
        let prefix_len = table_name.len() + 1;

        for col_def in &schema.columns {
            let col_name = &col_def.name;

            let mut col_index_key = String::with_capacity(prefix_len + col_name.len());
            col_index_key.push_str(table_name);
            col_index_key.push('.');
            col_index_key.push_str(col_name);
            if let Some(index_ref) = self.column_indexes.get(&col_index_key) {
                // 收集该列的所有数据
                let mut column_data: Vec<(Value, RowId)> = Vec::with_capacity(rows.len());
                for (row_id, row) in row_ids.iter().zip(rows.iter()) {
                    if let Some(col_value) = row.get(col_def.position) {
                        column_data.push((col_value.clone(), *row_id));
                    }
                }

                // 批量插入列索引
                if !column_data.is_empty() {
                    if let Err(_e) = index_ref.value().batch_insert(column_data) {
                        debug_log!("[batch_insert] Failed to batch update column index '{}': {}", col_name, _e);
                        self.index_registry.mark_stale(&col_index_key);
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
                            texts.push((*row_id, (**text).clone()));
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

        // Auto-flush trigger (pending counter already incremented before WAL write)
        if old_count / 2_000 != (old_count + rows.len()) / 2_000 {
            self.request_auto_flush();
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
        let arc_results = self.get_table_rows_batch_arc(table_name, row_ids)?;
        Ok(arc_results.into_iter()
            .map(|(rid, opt)| (rid, opt.map(|a| (*a).clone())))
            .collect())
    }

    /// Batch fetch rows, returning Arc<Row> to avoid clone on cache hit
    pub fn get_table_rows_batch_arc(&self, table_name: &str, row_ids: &[RowId]) -> Result<Vec<(RowId, Option<Arc<Row>>)>> {
        if row_ids.is_empty() {
            return Ok(Vec::new());
        }

        let _schema = self.table_registry.get_table(table_name)?;

        // Batch cache check — single lock acquisition for all rows
        let cached = self.row_cache.batch_get(table_name, row_ids);

        let mut missed_ids: Vec<RowId> = Vec::new();
        let mut missed_indices: Vec<usize> = Vec::new();
        let mut results: Vec<(RowId, Option<Arc<Row>>)> = Vec::with_capacity(row_ids.len());

        for (&row_id, opt) in row_ids.iter().zip(cached.into_iter()) {
            match opt {
                Some(arc) => results.push((row_id, Some(arc))),
                None => {
                    results.push((row_id, None));
                    missed_ids.push(row_id);
                    missed_indices.push(results.len() - 1);
                }
            }
        }

        if missed_ids.is_empty() {
            return Ok(results);
        }

        let is_continuous = self.is_continuous_row_ids(&missed_ids);
        let fetched: Vec<(RowId, Option<Row>)> = if missed_ids.len() == 1 {
            // Single row_id: direct point get is faster than scan+filter
            let schema = self.table_registry.get_table(table_name)?;
            let row_id = missed_ids[0];
            let opt = self.get_table_row_arc(table_name, row_id, &schema)?
                .map(|arc| match Arc::try_unwrap(arc) {
                    Ok(row) => row,
                    Err(arc) => (*arc).clone(),
                });
            vec![(row_id, opt)]
        } else if is_continuous {
            self.get_table_rows_batch_range(table_name, &missed_ids)?
        } else {
            let mut sorted_ids = missed_ids.clone();
            sorted_ids.sort_unstable();
            sorted_ids.dedup();
            self.get_table_rows_scan_with_filter(table_name, &sorted_ids)?
        };

        // Sort fetched results by row_id for binary search (avoids HashMap allocation)
        let mut fetched_sorted: Vec<(RowId, Row)> = fetched
            .into_iter()
            .filter_map(|(rid, opt)| opt.map(|r| (rid, r)))
            .collect();
        fetched_sorted.sort_unstable_by_key(|(rid, _)| *rid);

        for (i, &row_id) in missed_ids.iter().enumerate() {
            if let Some(&result_idx) = missed_indices.get(i) {
                if let Ok(pos) = fetched_sorted.binary_search_by_key(&row_id, |(rid, _)| *rid) {
                    let row_arc = Arc::new(fetched_sorted[pos].1.clone());
                    results[result_idx] = (row_id, Some(row_arc));
                }
            }
        }

        Ok(results)
    }
    
    // ==================== Internal Helpers ====================
    
    /// Increment pending updates counter and trigger auto-flush if needed
    /// 🚀 P0 CRITICAL FIX: 使用原子操作避免锁竞争，解决 CPU 飙升问题
    fn increment_pending_updates(&self) {
        use std::sync::atomic::Ordering;
        
        let count = self.pending_updates.fetch_add(1, Ordering::Release);
        
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
    /// Fetch rows for sorted, non-continuous row_ids using a single LSM range scan + HashSet filter.
    /// Converts N random reads into 1 sequential scan — ~100x faster for scattered IDs.
    fn get_table_rows_scan_with_filter(&self, table_name: &str, sorted_ids: &[RowId]) -> Result<Vec<(RowId, Option<Row>)>> {
        if sorted_ids.is_empty() {
            return Ok(Vec::new());
        }

        let min_id = sorted_ids[0];
        let max_id = sorted_ids[sorted_ids.len() - 1];

        let start_key = self.make_composite_key(table_name, min_id);
        let end_key = self.make_composite_key(table_name, max_id + 1);

        // Use streaming scan to avoid materializing all rows into a Vec.
        let lsm_iter = self.lsm_engine.scan_range_streaming(start_key, end_key)?;

        // Pre-compute decode info outside the loop
        let decode_info = self.table_registry.get_table(table_name).ok()
            .map(|s| {
                let col_types = s.col_types();
                let fc = crate::storage::row_format::compute_fixed_count(col_types);
                (col_types.to_vec(), fc)
            });

        let mut result = Vec::with_capacity(sorted_ids.len());
        for item in lsm_iter {
            let (composite_key, value) = item?;
            let row_id = (composite_key & 0xFFFFFFFF) as RowId;

            // Binary search instead of HashSet — avoids heap allocation for id_set
            if sorted_ids.binary_search(&row_id).is_err() {
                continue;
            }

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

            let row: Row = if let Some((ref col_types, fc)) = decode_info {
                    crate::storage::row_format::decode_fast(data, col_types, fc)
                        .map_err(|e| StorageError::Serialization(e.to_string()))?
                } else {
                    crate::storage::row_format::decode_any(data)
                        .map_err(|e| StorageError::Serialization(e.to_string()))?
                };

            self.row_cache.put(table_name.to_string(), row_id, row.clone());
            result.push((row_id, Some(row)));
        }

        Ok(result)
    }

    fn get_table_rows_batch_range(&self, table_name: &str, row_ids: &[RowId]) -> Result<Vec<(RowId, Option<Row>)>> {
        let min_id = *row_ids.iter().min().unwrap();
        let max_id = *row_ids.iter().max().unwrap();

        let start_key = self.make_composite_key(table_name, min_id);
        let end_key = self.make_composite_key(table_name, max_id + 1);

        let lsm_rows = self.lsm_engine.scan_range(start_key, end_key)?;

        // Pre-compute decode info outside the loop
        let decode_info = self.table_registry.get_table(table_name).ok()
            .map(|s| {
                let col_types = s.col_types();
                let fc = crate::storage::row_format::compute_fixed_count(col_types);
                (col_types.to_vec(), fc)
            });

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

            let row: Row = if let Some((ref col_types, fc)) = decode_info {
                    crate::storage::row_format::decode_fast(data, col_types, fc)
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
    
}

/// 🚀 表行批量迭代器
/// 
/// 每次返回一批行数据，避免一次性加载全部数据到内存。
pub struct TableRowBatchedIterator {
    lsm_iter: crate::storage::lsm::LSMBatchedIterator,
    _table_name: String,
    col_types: Option<Vec<crate::types::ColumnType>>,
    fixed_count: usize,
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
                        match crate::storage::row_format::decode_fast(data, col_types, self.fixed_count) {
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
    fixed_count: usize,
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

                let row: Row = if let Some(ref col_types) = self.col_types {
                    match crate::storage::row_format::decode_fast(data, col_types, self.fixed_count) {
                        Ok(row) => row,
                        Err(e) => return Some(Err(e)),
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

/// Streaming iterator that only decodes specified columns.
/// Owns a reusable decode buffer — no lifetime constraints on the iterator.
pub struct TableRowPartialIterator {
    lsm_iter: crate::storage::lsm::MergingIterator,
    col_types: Vec<crate::types::ColumnType>,
    fixed_count: usize,
    col_positions: Vec<usize>,
    out_buf: Vec<Value>,
}

impl Iterator for TableRowPartialIterator {
    type Item = Result<(RowId, Vec<Value>)>;

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
                match crate::storage::row_format::decode_fast_partial_into(
                    data, &self.col_types, self.fixed_count, &self.col_positions, &mut self.out_buf,
                ) {
                    Ok(_) => {
                        let projected = self.out_buf.clone();
                        Some(Ok((row_id, projected)))
                    }
                    Err(e) => Some(Err(e)),
                }
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Database;
    use crate::types::Value;
    use tempfile::TempDir;

    fn setup() -> (Database, TempDir) {
        let dir = TempDir::new().unwrap();
        let db = Database::create(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, val INT)").unwrap();
        (db, dir)
    }

    fn select_rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
        use crate::sql::QueryResult;
        match db.execute(sql).unwrap().materialize().unwrap() {
            QueryResult::Select { rows, .. } => rows,
            other => panic!("Expected Select, got {:?}", other),
        }
    }

    #[test]
    fn test_insert_and_select_pk() {
        let (db, _dir) = setup();
        db.execute("INSERT INTO t VALUES (1, 'alice', 100)").unwrap();
        let rows = select_rows(&db, "SELECT * FROM t WHERE id = 1");
        assert_eq!(rows[0][0], Value::Integer(1));
        assert_eq!(rows[0][1], Value::Text(crate::types::ArcString::from("alice")));
        assert_eq!(rows[0][2], Value::Integer(100));
    }

    #[test]
    fn test_insert_many_count() {
        let (db, _dir) = setup();
        for i in 0..100i64 {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'n{}', {})", i, i, i * 10)).unwrap();
        }
        assert_eq!(select_rows(&db, "SELECT COUNT(*) FROM t")[0][0], Value::Integer(100));
    }

    #[test]
    fn test_update_value() {
        let (db, _dir) = setup();
        db.execute("INSERT INTO t VALUES (1, 'alice', 100)").unwrap();
        db.execute("UPDATE t SET name = 'bob', val = 200 WHERE id = 1").unwrap();
        let rows = select_rows(&db, "SELECT name, val FROM t WHERE id = 1");
        assert_eq!(rows[0][0], Value::Text(crate::types::ArcString::from("bob")));
        assert_eq!(rows[0][1], Value::Integer(200));
    }

    #[test]
    fn test_delete_removes_row() {
        let (db, _dir) = setup();
        db.execute("INSERT INTO t VALUES (1, 'a', 1)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'b', 2)").unwrap();
        db.execute("DELETE FROM t WHERE id = 1").unwrap();
        assert_eq!(select_rows(&db, "SELECT COUNT(*) FROM t")[0][0], Value::Integer(1));
        assert_eq!(select_rows(&db, "SELECT * FROM t WHERE id = 2").len(), 1);
    }

    #[test]
    fn test_insert_null_columns() {
        let (db, _dir) = setup();
        db.execute("INSERT INTO t VALUES (1, NULL, NULL)").unwrap();
        let rows = select_rows(&db, "SELECT * FROM t WHERE id = 1");
        assert_eq!(rows[0][1], Value::Null);
        assert_eq!(rows[0][2], Value::Null);
    }

    #[test]
    fn test_order_by_asc() {
        let (db, _dir) = setup();
        for i in 0..10i64 {
            db.execute(&format!("INSERT INTO t VALUES ({}, '', {})", i, i * 10)).unwrap();
        }
        let rows = select_rows(&db, "SELECT id FROM t ORDER BY id ASC");
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row[0], Value::Integer(i as i64));
        }
    }

    #[test]
    fn test_partial_column_scan_select() {
        let (db, _dir) = setup();
        db.execute("INSERT INTO t VALUES (1, 'x', 10)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'y', 20)").unwrap();
        let rows = select_rows(&db, "SELECT id, val FROM t ORDER BY id ASC");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![Value::Integer(1), Value::Integer(10)]);
        assert_eq!(rows[1], vec![Value::Integer(2), Value::Integer(20)]);
    }
}
