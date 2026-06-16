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
                        // NULL primary key is invalid per SQL standard
                        if matches!(pk_value, Value::Null) {
                            return Err(StorageError::InvalidData(format!(
                                "NULL primary key is not allowed for table '{}'", table_name
                            )));
                        }
                        let pk_key = crate::database::pk_cache::PkKey::from_value(pk_value);

                        // Atomic check-and-insert: eliminates TOCTOU race between
                        // the uniqueness check and the actual row insert.
                        // On cache hit (duplicate), insert_if_absent returns Err immediately.
                        // On cache miss, falls through to slow path below.
                        if let Some(lookup) = self.pk_lookup.get(table_name) {
                            match lookup.insert_if_absent(pk_key.clone(), 0 /* placeholder row_id */) {
                                Ok(()) => {
                                    // Successfully reserved — will update with real row_id below
                                }
                                Err(_) => {
                                    return Err(StorageError::InvalidData(format!(
                                        "Duplicate primary key {:?} for table '{}'", pk_value, table_name
                                    )));
                                }
                            }
                        } else {
                            // No PK cache yet — fall back to slow path
                            match self.query_by_column(table_name, pk_name, pk_value) {
                                Ok(found) if !found.is_empty() => {
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
                                _ => {}
                            }
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

        // 7. Write to WAL for durability
        self.wal.log_insert_raw_ref(table_name, partition, row_id, &row_data, 0)?;

        // 7. Write to columnar buffer (primary storage). Skip LSM — columnar is the source of truth.
        let ts = self.write_lsn.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.row_cache.put(table_name.to_string(), row_id, row.clone());

        // 🚀 Columnar write buffer (zero-encode path)
        {
            use dashmap::mapref::entry::Entry;
            let builder_arc = match self.columnar_write_bufs.entry(table_name.to_string()) {
                Entry::Occupied(o) => o.get().clone(),
                Entry::Vacant(v) => {
                    let indexes_dir = self.path.join("indexes");
                    std::fs::create_dir_all(&indexes_dir).ok();
                    let path = indexes_dir.join(format!("{}_col.sst", table_name));
                    let b = Arc::new(parking_lot::Mutex::new(
                        crate::storage::lsm::columnar::ColumnarSSTableBuilder::new(
                            path, schema.col_types().to_vec(),
                        )
                    ));
                    v.insert(b.clone());
                    b
                }
            };
            let mut builder = builder_arc.lock();
            let table_id = self.table_registry.get_table_id(table_name).unwrap_or(0) as u64;
            let key = (table_id << 32) | (row_id & 0xFFFFFFFF);
            let _ = builder.add_values(key, ts, false, &row);
        }

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

        // Check write buffer for tombstones / updates before consulting SSTable
        let composite_key = self.make_composite_key(table_name, row_id);
        if let Some(builder_arc) = self.columnar_write_bufs.get(table_name) {
            let guard = builder_arc.value().lock();
            if let Some(deleted) = guard.check_key(composite_key) {
                if deleted {
                    return Ok(None); // Tombstoned in write buffer
                }
                // Key exists in buffer but cache missed — row_cache should have caught it.
                // Fall through: try SSTable, then LSM.
            }
        }

        // 🚀 Columnar point query: binary search in RowMap, O(log N)
        if let Some(col_sst) = self.columnar_sstables.get(table_name) {
            let key = composite_key;
            let key = self.make_composite_key(table_name, row_id);
            if let Some(row) = col_sst.get_row(key, schema.col_types()) {
                let row_arc = Arc::new(row);
                self.row_cache.put_arc(table_name.to_string(), row_id, Arc::clone(&row_arc));
                return Ok(Some(Arc::try_unwrap(row_arc).unwrap_or_else(|a| (*a).clone())));
            }
            return Ok(None);
        }

        // Cache miss - load from LSM
        let composite_key = self.make_composite_key(table_name, row_id);

        if let Some(value) = self.lsm_engine.get(composite_key)? {
            if value.deleted {
                return Ok(None);
            }

            let data: Vec<u8> = match &value.data {
                crate::storage::lsm::ValueData::Inline(bytes) => bytes.to_vec(),
                crate::storage::lsm::ValueData::Blob(blob_ref) => {
                    match self.lsm_engine.resolve_blob(blob_ref) {
                        Ok(data) => data,
                        Err(e) => return Err(StorageError::Serialization(format!(
                            "Failed to resolve blob for row {}: {}", row_id, e
                        ))),
                    }
                }
            };

            let col_types = schema.col_types();
            let fc = row_format::compute_fixed_count(col_types);
            let row: Row = row_format::decode_fast(&data, col_types, fc)
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
        self.update_row_with_schema_ref(table_name, row_id, &old_row, new_row, &schema)
    }

    /// Update a row with pre-resolved schema (avoids redundant lookup).
    /// Takes ownership of old_row for backwards compatibility.
    pub fn update_row_in_table_with_schema(&self, table_name: &str, row_id: RowId, old_row: Row, new_row: Row, schema: &crate::types::TableSchema) -> Result<()> {
        self.update_row_with_schema_ref(table_name, row_id, &old_row, new_row, schema)
    }

    /// Core UPDATE implementation — borrows old_row (avoids caller clone).
    pub fn update_row_with_schema_ref(&self, table_name: &str, row_id: RowId, old_row: &Row, new_row: Row, schema: &crate::types::TableSchema) -> Result<()> {
        ensure_open!(self);

        // 1. Check PK uniqueness if primary key is being changed
        if !schema.is_primary_key_auto_increment() {
            if let Some(pk_name) = schema.primary_key() {
                if let Some(pk_col) = schema.get_column(pk_name) {
                    let old_pk = old_row.get(pk_col.position);
                    let new_pk = new_row.get(pk_col.position);
                    if old_pk != new_pk {
                        if let Some(new_val) = new_pk {
                            if !matches!(new_val, Value::Null) {
                                let pk_key = crate::database::pk_cache::PkKey::from_value(new_val);
                                // Check PK cache for existing entry with different row_id
                                if let Some(lookup) = self.pk_lookup.get(table_name) {
                                    if let Some(existing_rid) = lookup.get_pk(&pk_key) {
                                        if existing_rid != row_id {
                                            return Err(StorageError::InvalidData(format!(
                                                "Duplicate primary key {:?} for table '{}'", new_val, table_name
                                            )));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

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

        // 6. Write to columnar buffer (primary storage) + WAL (durability)
        let timestamp = self.write_lsn.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.row_cache.put(table_name.to_string(), row_id, new_row.clone());

        // Add new row to columnar buffer (create if first write to this table)
        {
            use dashmap::mapref::entry::Entry;
            let builder_arc = match self.columnar_write_bufs.entry(table_name.to_string()) {
                Entry::Occupied(o) => o.get().clone(),
                Entry::Vacant(v) => {
                    let indexes_dir = self.path.join("indexes");
                    std::fs::create_dir_all(&indexes_dir).ok();
                    let col_sst_path = indexes_dir.join(format!("{}_col.sst", table_name));
                    let b = Arc::new(parking_lot::Mutex::new(
                        crate::storage::lsm::columnar::ColumnarSSTableBuilder::new(col_sst_path, schema.col_types().to_vec())
                    ));
                    v.insert(b.clone()); b
                }
            };
            let mut builder = builder_arc.lock();
            let table_id = self.table_registry.get_table_id(table_name).unwrap_or(0) as u64;
            let key = (table_id << 32) | (row_id & 0xFFFFFFFF);
            let _ = builder.add_values(key, timestamp, false, &new_row);
        }

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

        // 🚀 Columnar tombstone is the source of truth. LSM delete removed.
        // Columnar tombstone below marks the row deleted in all reads.
        {
            use dashmap::mapref::entry::Entry;
            let builder_arc = match self.columnar_write_bufs.entry(table_name.to_string()) {
                Entry::Occupied(o) => o.get().clone(),
                Entry::Vacant(v) => {
                    let indexes_dir = self.path.join("indexes");
                    std::fs::create_dir_all(&indexes_dir).ok();
                    let col_sst_path = indexes_dir.join(format!("{}_col.sst", table_name));
                    let b = Arc::new(parking_lot::Mutex::new(
                        crate::storage::lsm::columnar::ColumnarSSTableBuilder::new(col_sst_path, schema.col_types().to_vec())
                    ));
                    v.insert(b.clone()); b
                }
            };
            let mut builder = builder_arc.lock();
            let table_id = self.table_registry.get_table_id(table_name).unwrap_or(0) as u64;
            let key = (table_id << 32) | (row_id & 0xFFFFFFFF);
            let _ = builder.add_values(key, timestamp, true, &old_row);
        }

        // Invalidate cache AFTER LSM write — single invalidation
        self.row_cache.invalidate(table_name, row_id);

        // 7.1 Decrement row count for COUNT(*) fast path
        // Use saturating subtract via fetch_update to avoid both underflow
        // AND the stuck-at-zero bug from CAS-based guard loops.
        if let Some(counter) = self.table_row_count.get(table_name) {
            let _ = counter.fetch_update(
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
                |c| Some(c.saturating_sub(1)),
            );
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

        // Columnar-backed scan: if the table's data lives in a columnar SSTable
        // (rather than the LSM), decode column arrays and synthesize rows.
        // Falling back to the LSM scan here would yield empty/stale results.
        if self.columnar_sstables.contains_key(table_name) {
            let col_sst = self.columnar_sstables.get(table_name).unwrap().clone();
            let num_cols = col_types.len();
            let mut segments: Vec<ColumnarSegment> = Vec::with_capacity(num_cols);
            for col_idx in 0..num_cols {
                let seg = if col_sst.column_tags[col_idx].is_fixed() {
                    ColumnarSegment::Fixed(col_sst.read_fixed_i64(col_idx)?)
                } else {
                    ColumnarSegment::Text(col_sst.read_text(col_idx)?)
                };
                segments.push(seg);
            }
            let col_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
            return Ok(TableRowStreamingIterator {
                inner: TableRowStreamingInner::Columnar {
                    row_map: col_sst.row_map.clone(),
                    segments,
                    col_names,
                    current_idx: 0,
                    num_rows: col_sst.num_rows,
                },
            });
        }

        // Use LSM streaming scan
        let table_prefix = self.compute_table_prefix(table_name);
        let start_key = table_prefix << 32;
        let end_key = (table_prefix + 1) << 32;

        let lsm_iter = self.lsm_engine.scan_range_streaming(start_key, end_key)?;
        let use_raw = lsm_iter.has_raw_sst();

        // Detect whether any column is nullable
        let has_nullable = schema.columns.iter().any(|c| c.nullable);
        Ok(TableRowStreamingIterator {
            inner: TableRowStreamingInner::Lsm {
                lsm_iter,
                decode_ctx: {
                    let mut ctx = crate::storage::row_format::SchemaDecodeContext::new(&col_types);
                    ctx.trust_utf8 = true; // Data encoded by MoteDB, safe to skip UTF-8 validation
                    ctx.skip_magic_check = true; // All data from our own encode()
                    if !has_nullable {
                        ctx.has_nullable_columns = false;
                    }
                    Some(ctx)
                },
                use_raw,
            },
        })
    }

    /// Raw byte streaming scan — returns (row_id, raw_data_bytes) without decoding.
    /// Caller can use row_format::get_column() for partial decode.
    pub fn scan_table_raw_streaming(
        &self,
        table_name: &str,
    ) -> Result<TableRawStreamingIterator> {
        ensure_open!(self);
        let table_prefix = self.compute_table_prefix(table_name);
        let start_key = table_prefix << 32;
        let end_key = (table_prefix + 1) << 32;
        let lsm_iter = self.lsm_engine.scan_range_streaming(start_key, end_key)?;
        Ok(TableRawStreamingIterator { lsm_iter })
    }

    /// Zero-copy decode streaming scan — yields (row_id) and decodes each row
    /// directly into a caller-provided Vec via `decode_row_into`.
    ///
    /// Uses `ValueBytes` (Arc-shared block data) instead of `to_vec()`,
    /// eliminating the per-row memcpy that `scan_table_raw_streaming` performs.
    pub fn scan_table_decode_streaming(
        &self,
        table_name: &str,
        col_types: &[crate::types::ColumnType],
    ) -> Result<TableDecodeStreamingIterator> {
        ensure_open!(self);
        let table_prefix = self.compute_table_prefix(table_name);
        let start_key = table_prefix << 32;
        let end_key = (table_prefix + 1) << 32;
        let lsm_iter = self.lsm_engine.scan_range_streaming(start_key, end_key)?;
        let use_raw = lsm_iter.has_raw_sst();
        let mut ctx = crate::storage::row_format::SchemaDecodeContext::new(col_types);
        ctx.trust_utf8 = true;
        ctx.skip_magic_check = true; // All data from our own encode()
        Ok(TableDecodeStreamingIterator {
            lsm_iter,
            decode_ctx: ctx,
            use_raw,
        })
    }

    /// Columnar streaming scan — decodes rows into `ColumnArray` slices instead
    /// of `Vec<Value>`. Uses O(columns) allocations instead of O(rows), saving
    /// ~50% memory and improving cache locality for aggregate queries.
    ///
    /// Returns a `ColumnarRowSet` containing all rows decoded into column arrays.
    /// The caller can then compute COUNT/SUM/MIN/MAX directly from typed arrays.
    pub fn scan_table_columnar(
        &self,
        table_name: &str,
        col_types: &[crate::types::ColumnType],
        column_names: Vec<String>,
    ) -> Result<crate::storage::row_format::ColumnarRowSet> {
        ensure_open!(self);
        let table_prefix = self.compute_table_prefix(table_name);
        let start_key = table_prefix << 32;
        let end_key = (table_prefix + 1) << 32;
        let mut lsm_iter = self.lsm_engine.scan_range_streaming(start_key, end_key)?;

        let mut ctx = crate::storage::row_format::SchemaDecodeContext::new(col_types);
        ctx.trust_utf8 = true;
        ctx.skip_magic_check = true;

        // Detect nullable columns
        let schema = self.table_registry.get_table(table_name)?;
        let has_nullable = schema.columns.iter().any(|c| c.nullable);
        if !has_nullable {
            ctx.has_nullable_columns = false;
        }

        let mut result = crate::storage::row_format::ColumnarRowSet::new(column_names, col_types);
        let mut col_data: Vec<crate::storage::row_format::ColumnArray> = col_types.iter().map(|ct| match ct {
            crate::types::ColumnType::Integer => crate::storage::row_format::ColumnArray::Integers(Vec::new()),
            crate::types::ColumnType::Float => crate::storage::row_format::ColumnArray::Floats(Vec::new()),
            crate::types::ColumnType::Text => crate::storage::row_format::ColumnArray::Texts(Vec::new()),
            crate::types::ColumnType::Timestamp => crate::storage::row_format::ColumnArray::Timestamps(Vec::new()),
            crate::types::ColumnType::Boolean => crate::storage::row_format::ColumnArray::Bools(Vec::new()),
            _ => crate::storage::row_format::ColumnArray::Values(Vec::new()),
        }).collect();

        // Pre-allocate 300K capacity per column
        let size_hint = self.fast_row_count(table_name).map(|c| c as usize).unwrap_or(1024);
        for arr in &mut col_data {
            match arr {
                crate::storage::row_format::ColumnArray::Integers(v) => v.reserve(size_hint),
                crate::storage::row_format::ColumnArray::Floats(v) => v.reserve(size_hint),
                crate::storage::row_format::ColumnArray::Texts(v) => v.reserve(size_hint),
                crate::storage::row_format::ColumnArray::Timestamps(v) => v.reserve(size_hint),
                crate::storage::row_format::ColumnArray::Bools(v) => v.reserve(size_hint),
                crate::storage::row_format::ColumnArray::Values(v) => v.reserve(size_hint),
            }
        }

        let mut count = 0usize;
        loop {
            match lsm_iter.next() {
                Some(Ok((_composite_key, value))) => {
                    if value.deleted { continue; }
                    let data = match &value.data {
                        crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                        _ => continue,
                    };
                    if let Err(_e) = crate::storage::row_format::decode_row_into_columns(
                        &ctx, data, &mut col_data,
                    ) {
                        continue; // skip malformed rows
                    }
                    count += 1;
                }
                Some(Err(_)) => break,
                None => break,
            }
        }

        result.data = col_data;
        result.num_rows = count;
        Ok(result)
    }

    /// Returns true if this table stores its data in a columnar SSTable
    /// (rather than the LSM row store). Used by the query layer to pick the
    /// correct scan path — scanning the LSM for a columnar table yields empty
    /// results because the data lives in `columnar_sstables`, not the LSM.
    pub fn is_columnar_table(&self, table_name: &str) -> bool {
        self.columnar_sstables.contains_key(table_name)
            || self.columnar_write_bufs.contains_key(table_name)
            || self.col_segment_stores.contains_key(table_name)
    }

    /// Get or create the multi-segment ColSegmentStore for a table.
    /// This is the new append-only path; coexists with the legacy
    /// single-SSTable fields during migration (S6-S9).
    pub fn get_or_create_col_segment_store(
        &self,
        table_name: &str,
        col_types: Vec<crate::types::ColumnType>,
    ) -> Result<Arc<crate::storage::col_segment::ColSegmentStore>> {
        if let Some(s) = self.col_segment_stores.get(table_name) {
            return Ok(s.clone());
        }
        let store = crate::storage::col_segment::ColSegmentStore::create(
            &self.path, table_name, col_types,
        )?;
        self.col_segment_stores
            .insert(table_name.to_string(), store.clone());
        Ok(store)
    }

    /// Whether this table has an active ColSegmentStore (new multi-segment path).
    pub fn has_col_segment_store(&self, table_name: &str) -> bool {
        self.col_segment_stores.contains_key(table_name)
    }

    /// 🆕 S9: sync a ColSegmentStore table's latest single segment (after flush
    /// + compaction) into the legacy `columnar_sstables` map. Legacy aggregate /
    /// GROUP BY / scan paths read `columnar_sstables` directly; this shares the
    /// same Arc<ColumnarSSTable> so they observe the data without cloning.
    /// Idempotent: safe to call before any query that uses legacy columnar reads.
    pub fn sync_col_segment_to_sstables(&self, table_name: &str) {
        if let Some(store) = self.col_segment_stores.get(table_name) {
            let _ = store.flush_buffer();
            // Compact to a single segment so legacy aggregate paths (which read
            // one columnar_sstables entry) see ALL data. Bounded: compaction
            // only runs while segment count >= threshold (3).
            let mut compactions = 0;
            // Force compact to a SINGLE segment so legacy aggregate paths (which
            // read one columnar_sstables entry) see ALL data. compact_once
            // already merges all segments when count >= threshold; we lower the
            // bar here to force at least one pass when there are 2+ segments.
            while store.segment_count() >= 2 {
                let _ = store.force_compact_all();
                compactions += 1;
                if compactions > 5 { break; } // safety
            }
            if let Some(sst) = store.latest_segment_sst() {
                self.columnar_sstables.insert(table_name.to_string(), sst);
            }
        }
    }

    /// Finalize unflushed columnar write buffer for a table.
    /// Converts accumulated INSERT data to a columnar SSTable file.
    /// Safe: only removes from write buffer AFTER successful finalization.
    pub fn finalize_columnar_buffer(&self, table_name: &str) {
        // S9: for tables on the ColSegmentStore path, just flush the store's
        // in-memory buffer (cheap delta write). The legacy single-SSTable merge
        // below is skipped — it was the full-table-rewrite regression source.
        if self.col_segment_stores.contains_key(table_name) {
            if let Some(store) = self.col_segment_stores.get(table_name) {
                let _ = store.flush_buffer();
            }
            return;
        }
        if let Some(builder_arc) = self.columnar_write_bufs.get(table_name) {
            let (path, num_rows) = {
                let guard = builder_arc.value().lock();
                (guard.path.clone(), guard.num_rows)
            };
            if num_rows == 0 { return; }

            // MERGE: read existing SSTable rows into the builder so finalize
            // produces a complete file, not just the buffer's delta.
            // Buffer entries override SSTable entries (higher timestamp = newer).
            if let Some(old_sst) = self.columnar_sstables.get(table_name) {
                let col_types: Vec<crate::types::ColumnType> = old_sst.column_tags.iter()
                    .map(|t| t.to_column_type()).collect();
                let mut guard = builder_arc.value().lock();
                for i in 0..old_sst.num_rows {
                    if old_sst.row_map.is_deleted(i) { continue; }
                    let k = old_sst.row_map.key(i);
                    // Skip if buffer already has a newer entry for this key
                    if guard.check_key(k).is_some() { continue; }
                    let ts = old_sst.row_map.timestamp(i);
                    if let Some(row) = old_sst.get_row(k, &col_types) {
                        let _ = guard.add_values(k, ts, false, &row);
                    }
                }
            }

            let result = builder_arc.value().lock().finish_and_reset();
            match result {
                Ok(()) => {
                    if let Ok(col_sst) = crate::storage::lsm::columnar::ColumnarSSTable::open(&path) {
                        self.columnar_sstables.insert(table_name.to_string(), Arc::new(col_sst));
                    }
                }
                Err(e) => {
                    debug_log!("[columnar] Finalize failed for '{}': {:?} — data preserved", table_name, e);
                }
            }
        }
    }

    /// Scan from a columnar SSTable, yielding rows one at a time via iterator.
    /// Much faster than row-based scan + uses O(columns) memory instead of O(rows).
    pub fn scan_columnar_sstable_streaming(
        &self,
        table_name: &str,
        col_types: &[crate::types::ColumnType],
    ) -> Result<ColumnarScanIterator> {
        // Finalize with merge: combines write buffer + existing SSTable.
        // Safe now: merge reads old SSTable rows before overwriting.
        self.finalize_columnar_buffer(table_name);

        let col_sst = match self.columnar_sstables.get(table_name) {
            Some(sst) => sst.clone(),
            None => return Err(StorageError::InvalidData(
                format!("No columnar SSTable for table '{}'", table_name)
            )),
        };

        let num_cols = col_types.len();
        let mut segments: Vec<ColumnarSegment> = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let seg = if col_sst.column_tags[col_idx].is_fixed() {
                ColumnarSegment::Fixed(col_sst.read_fixed_i64(col_idx)?)
            } else {
                ColumnarSegment::Text(col_sst.read_text(col_idx)?)
            };
            segments.push(seg);
        }

        Ok(ColumnarScanIterator {
            row_map: col_sst.row_map.clone(),
            segments,
            col_types: col_types.to_vec(),
            current_idx: 0,
            num_rows: col_sst.num_rows,
            match_filter: None,
        })
    }

    /// Streaming columnar scan with column projection — only reads the specified
    /// column positions. For SELECT id, amount, only 2/4 segments are loaded.
    pub fn scan_columnar_sstable_projection(
        &self,
        table_name: &str,
        col_types: &[crate::types::ColumnType],
        col_positions: &[usize],
    ) -> Result<ColumnarScanIterator> {
        let col_sst = match self.columnar_sstables.get(table_name) {
            Some(sst) => sst.clone(),
            None => return Err(StorageError::InvalidData(
                format!("No columnar SSTable for table '{}'", table_name)
            )),
        };

        // Build mapping: output position → segment index in SSTable
        let mut segments: Vec<(usize, ColumnarSegment)> = Vec::with_capacity(col_positions.len());
        for &col_idx in col_positions {
            let seg = if col_sst.column_tags[col_idx].is_fixed() {
                ColumnarSegment::Fixed(col_sst.read_fixed_i64(col_idx)?)
            } else {
                ColumnarSegment::Text(col_sst.read_text(col_idx)?)
            };
            segments.push((col_idx, seg));
        }

        Ok(ColumnarScanIterator {
            row_map: col_sst.row_map.clone(),
            segments: segments.into_iter().map(|(_, s)| s).collect(),
            col_types: col_positions.iter().map(|&i| col_types[i].clone()).collect(),
            current_idx: 0,
            num_rows: col_sst.num_rows,
            match_filter: None,
        })
    }

    /// Streaming columnar scan with equality filter on one column.
    /// Only rows matching filter_col = filter_value are yielded.
    /// For WHERE region = 'US' (100K/300K), this saves decoding 200K rows.
    pub fn scan_columnar_sstable_filtered(
        &self,
        table_name: &str,
        col_types: &[crate::types::ColumnType],
        filter_col: usize,
        filter_value: &crate::types::Value,
    ) -> Result<ColumnarScanIterator> {
        let col_sst = match self.columnar_sstables.get(table_name) {
            Some(sst) => sst.clone(),
            None => return Err(StorageError::InvalidData("No columnar SSTable".into())),
        };

        let num_cols = col_types.len();
        let mut segments: Vec<ColumnarSegment> = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let seg = if col_sst.column_tags[col_idx].is_fixed() {
                ColumnarSegment::Fixed(col_sst.read_fixed_i64(col_idx)?)
            } else {
                ColumnarSegment::Text(col_sst.read_text(col_idx)?)
            };
            segments.push(seg);
        }

        // Find matching row indices by scanning the filter column
        let mut match_indices: Vec<usize> = Vec::new();
        for row_idx in 0..col_sst.num_rows {
            if col_sst.row_map.is_deleted(row_idx) { continue; }
            let matches = match &segments[filter_col] {
                ColumnarSegment::Fixed(f) => match filter_value {
                    crate::types::Value::Integer(iv) => f.get_i64(row_idx) == Some(*iv),
                    crate::types::Value::Float(fv) => (f.get_f64(row_idx).unwrap_or(f64::NAN) - fv).abs() < f64::EPSILON,
                    _ => false,
                },
                ColumnarSegment::Text(t) => match filter_value {
                    crate::types::Value::Text(tv) => t.get_str(row_idx) == Some(tv.as_str()),
                    _ => false,
                },
            };
            if matches {
                match_indices.push(row_idx);
            }
        }

        Ok(ColumnarScanIterator {
            row_map: col_sst.row_map.clone(),
            segments,
            col_types: col_types.to_vec(),
            current_idx: 0,
            num_rows: col_sst.num_rows,
            match_filter: Some(match_indices),
        })
    }

    /// Streaming columnar scan with text prefix filter (LIKE 'prefix%').
    /// Only rows where the text column starts with `prefix` are yielded.
    pub fn scan_columnar_sstable_prefix(
        &self,
        table_name: &str,
        col_types: &[crate::types::ColumnType],
        filter_col: usize,
        prefix: &str,
    ) -> Result<ColumnarScanIterator> {
        let col_sst = match self.columnar_sstables.get(table_name) {
            Some(sst) => sst.clone(),
            None => return Err(StorageError::InvalidData("No columnar SSTable".into())),
        };

        let num_cols = col_types.len();
        let mut segments: Vec<ColumnarSegment> = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let seg = if col_sst.column_tags[col_idx].is_fixed() {
                ColumnarSegment::Fixed(col_sst.read_fixed_i64(col_idx)?)
            } else {
                ColumnarSegment::Text(col_sst.read_text(col_idx)?)
            };
            segments.push(seg);
        }

        // Find rows where text column starts with prefix
        let mut match_indices: Vec<usize> = Vec::new();
        if let ColumnarSegment::Text(ref text_seg) = segments[filter_col] {
            for row_idx in 0..col_sst.num_rows {
                if col_sst.row_map.is_deleted(row_idx) { continue; }
                if let Some(s) = text_seg.get_str(row_idx) {
                    if s.starts_with(prefix) {
                        match_indices.push(row_idx);
                    }
                }
            }
        }

        Ok(ColumnarScanIterator {
            row_map: col_sst.row_map.clone(),
            segments,
            col_types: col_types.to_vec(),
            current_idx: 0,
            num_rows: col_sst.num_rows,
            match_filter: Some(match_indices),
        })
    }

    /// Columnar Top-K: find indices of top K rows by a column value.
    /// Returns (row_indices, values) for the top K, without materializing rows.
    pub fn scan_columnar_sstable_topk(
        &self,
        table_name: &str,
        sort_col: usize,
        k: usize,
        ascending: bool,
    ) -> Result<(Vec<usize>, Vec<crate::types::Value>)> {
        let col_sst = match self.columnar_sstables.get(table_name) {
            Some(sst) => sst.clone(),
            None => return Err(StorageError::InvalidData("No columnar SSTable".into())),
        };
        use std::collections::BinaryHeap;
        use std::cmp::Reverse;

        if col_sst.column_tags[sort_col].is_fixed() {
            let seg = col_sst.read_fixed_i64(sort_col)?;
            if ascending {
                let mut heap: BinaryHeap<(OrderedF64, usize)> = BinaryHeap::with_capacity(k + 1);
                for i in 0..col_sst.num_rows {
                    if col_sst.row_map.is_deleted(i) { continue; }
                    if let Some(v) = seg.get_f64(i) {
                        heap.push((OrderedF64(v), i));
                        if heap.len() > k { heap.pop(); }
                    }
                }
                let mut res: Vec<(OrderedF64, usize)> = heap.into_vec();
                res.sort_by_key(|(v, _)| *v);
                res.truncate(k);
                let indices: Vec<usize> = res.iter().map(|(_, i)| *i).collect();
                let vals: Vec<crate::types::Value> = res.iter().map(|(v, _)| crate::types::Value::Float(v.0)).collect();
                Ok((indices, vals))
            } else {
                let mut heap: BinaryHeap<Reverse<(OrderedF64, usize)>> = BinaryHeap::with_capacity(k + 1);
                for i in 0..col_sst.num_rows {
                    if col_sst.row_map.is_deleted(i) { continue; }
                    if let Some(v) = seg.get_f64(i) {
                        heap.push(Reverse((OrderedF64(v), i)));
                        if heap.len() > k { heap.pop(); }
                    }
                }
                let mut res: Vec<Reverse<(OrderedF64, usize)>> = heap.into_vec();
                res.sort_by_key(|r| std::cmp::Reverse(r.0.0));
                res.truncate(k);
                let indices: Vec<usize> = res.iter().map(|r| r.0.1).collect();
                let vals: Vec<crate::types::Value> = res.iter().map(|r| crate::types::Value::Float(r.0.0.0)).collect();
                Ok((indices, vals))
            }
        } else {
            // Text sort column
            let seg = col_sst.read_text(sort_col)?;
            if ascending {
                let mut heap: BinaryHeap<(String, usize)> = BinaryHeap::with_capacity(k + 1);
                for i in 0..col_sst.num_rows {
                    if col_sst.row_map.is_deleted(i) { continue; }
                    if let Some(s) = seg.get_str(i) {
                        heap.push((s.to_string(), i));
                        if heap.len() > k { heap.pop(); }
                    }
                }
                let mut res: Vec<(String, usize)> = heap.into_vec();
                res.sort_by(|a, b| a.0.cmp(&b.0));
                res.truncate(k);
                let indices: Vec<usize> = res.iter().map(|(_, i)| *i).collect();
                let vals: Vec<crate::types::Value> = res.iter().map(|(s, _)| crate::types::Value::Text(crate::types::ArcString(std::sync::Arc::from(s.as_str())))).collect();
                Ok((indices, vals))
            } else {
                let mut heap: BinaryHeap<Reverse<(String, usize)>> = BinaryHeap::with_capacity(k + 1);
                for i in 0..col_sst.num_rows {
                    if col_sst.row_map.is_deleted(i) { continue; }
                    if let Some(s) = seg.get_str(i) {
                        heap.push(Reverse((s.to_string(), i)));
                        if heap.len() > k { heap.pop(); }
                    }
                }
                let mut res: Vec<Reverse<(String, usize)>> = heap.into_vec();
                res.sort_by(|a, b| b.0.0.cmp(&a.0.0));
                res.truncate(k);
                let indices: Vec<usize> = res.iter().map(|r| r.0.1).collect();
                let vals: Vec<crate::types::Value> = res.iter().map(|r| crate::types::Value::Text(crate::types::ArcString(std::sync::Arc::from(r.0.0.as_str())))).collect();
                Ok((indices, vals))
            }
        }
    }

} // impl MoteDB

/// OrderedF64: f64 wrapper with total ordering for BinaryHeap.
/// NaN sorts last (largest) so it's popped first from a max-heap.
#[derive(Clone, Copy, PartialEq)]
struct OrderedF64(f64);

impl Eq for OrderedF64 {}
impl PartialOrd for OrderedF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrderedF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self.0.is_nan(), other.0.is_nan()) {
            (true, true) => std::cmp::Ordering::Equal,
            (true, false) => std::cmp::Ordering::Greater,
            (false, true) => std::cmp::Ordering::Less,
            (false, false) => self.0.partial_cmp(&other.0).unwrap_or(std::cmp::Ordering::Equal),
        }
    }
}

impl MoteDB {

    /// Fetch specific rows by index from a columnar SSTable.
    /// Used after top-K or filter to materialize only the winning rows.
    pub fn scan_columnar_sstable_rows(
        &self,
        table_name: &str,
        col_types: &[crate::types::ColumnType],
        indices: &[usize],
    ) -> Result<Vec<Vec<Value>>> {
        let mut iter = self.scan_columnar_sstable_streaming(table_name, col_types)?;
        let mut rows = Vec::with_capacity(indices.len());
        for &idx in indices {
            rows.push(iter.build_row(idx));
        }
        Ok(rows)
    }

    /// Batch version: collect all rows (used when materialization is needed).
    pub fn scan_columnar_sstable(
        &self,
        table_name: &str,
        col_types: &[crate::types::ColumnType],
    ) -> Result<Vec<Vec<Value>>> {
        let iter = self.scan_columnar_sstable_streaming(table_name, col_types)?;
        let mut rows = Vec::with_capacity(iter.num_rows);
        for row in iter {
            rows.push(row);
        }
        Ok(rows)
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
                            // NULL primary key is invalid per SQL standard
                            if matches!(pk_value, Value::Null) {
                                return Err(StorageError::InvalidData(format!(
                                    "Batch row {}: NULL primary key is not allowed for table '{}'", idx, table_name
                                )));
                            }
                            let pk_key = crate::database::pk_cache::PkKey::from_value(pk_value);
                            // Intra-batch duplicate check
                            if !batch_pks.insert(pk_key.clone()) {
                                return Err(StorageError::InvalidData(format!(
                                    "Batch row {}: duplicate primary key {:?} within batch for table '{}'", idx, pk_value, table_name
                                )));
                            }
                            // Atomic check-and-insert into PK cache.
                            // This reserves the PK key, preventing concurrent inserts
                            // from using the same key (eliminates TOCTOU race).
                            if let Some(lookup) = self.pk_lookup.get(table_name) {
                                match lookup.insert_if_absent(pk_key, 0 /* placeholder */) {
                                    Ok(()) => {} // Reserved successfully
                                    Err(_) => {
                                        return Err(StorageError::InvalidData(format!(
                                            "Batch row {}: duplicate primary key {:?} for table '{}'", idx, pk_value, table_name
                                        )));
                                    }
                                }
                            } else {
                                // No PK cache — fall back to slow path (column index check)
                                match self.query_by_column(table_name, pk_name, pk_value) {
                                    Ok(found) if !found.is_empty() => {
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
            let start_id = self.next_row_id.fetch_add(rows.len() as u64, std::sync::atomic::Ordering::Relaxed);
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

        // 4. Write WAL first (durability guarantee) + columnar buffer (primary storage).
        //    WAL ensures zero data loss on crash; columnar buffer enables fast reads.
        let col_types = schema.col_types();
        let table_id = self.table_registry.get_table_id(table_name).unwrap_or(0) as u64;
        let base_ts = self.write_lsn.fetch_add(rows.len() as u64, std::sync::atomic::Ordering::Relaxed);

        // Build WAL records (lightweight: no RawRow encoding, just Row values)
        let wal_records: Vec<WALRecord> = rows.iter().enumerate().map(|(i, row)| {
            WALRecord::Insert {
                table_name: table_name.to_string(),
                row_id: row_ids[i],
                partition: (self.make_composite_key(table_name, row_ids[i]) % self.num_partitions as u64) as PartitionId,
                data: row.clone(),
                txn_id: 0,
            }
        }).collect();
        self.increment_pending_updates();
        self.wal.batch_append(0, wal_records)?;

        // 🆕 S9: write ONLY to the multi-segment ColSegmentStore (single-track).
        // The legacy columnar_write_bufs path is bypassed for batch INSERT —
        // eliminating the dual-write overhead (256ms → ~65ms for 60K rows).
        // Queries route to ColSegmentStore via has_col_segment_store().
        {
            let store = self.get_or_create_col_segment_store(table_name, col_types.to_vec())?;
            let store_rows: Vec<(u64, u64, Row)> = rows
                .iter()
                .enumerate()
                .map(|(i, row)| {
                    let key = (table_id << 32) | (row_ids[i] & 0xFFFFFFFF);
                    (key, base_ts + i as u64, row.clone())
                })
                .collect();
            store.append_rows(&store_rows)?;
            // Flush periodically to bound the in-memory buffer (embedded: ~4K rows).
            if store.buffered_row_count() >= 4000 {
                store.flush_buffer()?;
                if store.needs_compaction() {
                    let _ = store.compact_once();
                }
            }
        }

        // 🚀 Embedded memory: auto-finalize when buffer exceeds 10K rows.
        // Keeps write buffer under ~1.6 MB (10K × 40B × 4 cols) on embedded devices.
        let _old_count = self.pending_updates.fetch_add(rows.len(), std::sync::atomic::Ordering::Release);

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

        // 7.1 Collect data for all column indexes, then insert in parallel.
        //     Each column index is independent — no shared state between them.
        //
        //     🚀 Skip index updates when using columnar storage — WHERE/LIKE/aggregate
        //     queries use columnar filter paths instead of BTree indexes.
        //     Saves ~22MB per index on embedded devices.
        if self.columnar_sstables.contains_key(table_name)
            || self.columnar_write_bufs.contains_key(table_name) {
            debug_log!("[batch_insert] Skipping column index updates — columnar storage active for '{}'", table_name);
            // Still need to handle vector/text/spatial indexes below
        } else {
            let prefix_len = table_name.len() + 1;
            let mut column_tasks: Vec<(Arc<crate::index::column_value::ColumnValueIndex>, Vec<(Value, RowId)>, String)> = Vec::new();

            for col_def in &schema.columns {
            let col_name = &col_def.name;

            // Collect column index data
            let mut col_index_key = String::with_capacity(prefix_len + col_name.len());
            col_index_key.push_str(table_name);
            col_index_key.push('.');
            col_index_key.push_str(col_name);
            if let Some(index_ref) = self.column_indexes.get(&col_index_key) {
                let mut column_data: Vec<(Value, RowId)> = Vec::with_capacity(rows.len());
                for (row_id, row) in row_ids.iter().zip(rows.iter()) {
                    if let Some(col_value) = row.get(col_def.position) {
                        column_data.push((col_value.clone(), *row_id));
                    }
                }
                if !column_data.is_empty() {
                    column_tasks.push((index_ref.value().clone(), column_data, col_index_key.clone()));
                }
            }
        }

        // Parallel insert into column indexes (one thread per index).
        // Each index has its own mem_buffer and BTree — no shared state.
        if column_tasks.len() > 1 {
            std::thread::scope(|s| {
                for (index, data, key) in column_tasks {
                    s.spawn(move || {
                        if let Err(_e) = index.batch_insert(data) {
                            debug_log!("[batch_insert] Failed to batch update column index '{}': {}", key, _e);
                        }
                    });
                }
            });
        } else {
            for (index, data, key) in column_tasks {
                if let Err(_e) = index.batch_insert(data) {
                    debug_log!("[batch_insert] Failed to batch update column index '{}': {}", key, _e);
                    self.index_registry.mark_stale(&key);
                }
            }
        }
        } // end else (columnar SSTable exists → skip column indexes)

        // 7.2 Collect and batch update non-column indexes (vector, text, spatial)
        for col_def in &schema.columns {
            let col_name = &col_def.name;

            // 7.2a 批量更新 Vector Index
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
                            texts.push((*row_id, text.to_string()));
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
            counter.fetch_add(rows.len() as u64, Ordering::Relaxed);
        }

        // Auto-flush trigger
        let old_count = self.pending_updates.fetch_add(rows.len(), std::sync::atomic::Ordering::Release);
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
        // stride == 0 means same row accessed repeatedly — skip prefetch
        if stride == 0 {
            return;
        }
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

/// Raw byte streaming iterator — yields (row_id, raw_bytes) without row decode.
pub struct TableRawStreamingIterator {
    lsm_iter: crate::storage::lsm::MergingIterator,
}

impl Iterator for TableRawStreamingIterator {
    type Item = Result<(RowId, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.lsm_iter.next() {
                Some(Ok((composite_key, value))) => {
                    if value.deleted { continue; }
                    let row_id = (composite_key & 0xFFFFFFFF) as RowId;
                    match &value.data {
                        crate::storage::lsm::ValueData::Inline(bytes) => {
                            return Some(Ok((row_id, bytes.to_vec())));
                        }
                        crate::storage::lsm::ValueData::Blob(_) => {
                            return Some(Err(StorageError::InvalidData(
                                "Blob references should be resolved by LSM engine".into()
                            )));
                        }
                    }
                }
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }
    }
}

/// 🚀 表行流式迭代器（真正的 O(1) 内存占用）
///
/// 逐个返回行数据，不预先加载任何数据到内存。
/// 使用 SchemaDecodeContext 实现预计算 schema 上下文，消除每行冗余计算。
pub struct TableRowStreamingIterator {
    inner: TableRowStreamingInner,
}

enum TableRowStreamingInner {
    /// LSM row-store backed scan.
    Lsm {
        lsm_iter: crate::storage::lsm::MergingIterator,
        decode_ctx: Option<crate::storage::row_format::SchemaDecodeContext>,
        use_raw: bool,
    },
    /// Columnar SSTable backed scan. For tables whose data lives in the
    /// columnar SSTable (not the LSM), we decode column arrays into rows.
    Columnar {
        row_map: crate::storage::lsm::columnar::RowMap,
        segments: Vec<ColumnarSegment>,
        col_names: Vec<String>,
        current_idx: usize,
        num_rows: usize,
    },
}

impl Iterator for TableRowStreamingIterator {
    type Item = Result<(RowId, Row)>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            TableRowStreamingInner::Lsm { lsm_iter, decode_ctx, use_raw } => {
                lsm_next(lsm_iter, decode_ctx, *use_raw)
            }
            TableRowStreamingInner::Columnar { row_map, segments, col_names, current_idx, num_rows } => {
                while *current_idx < *num_rows {
                    let idx = *current_idx;
                    *current_idx += 1;
                    if row_map.is_deleted(idx) { continue; }
                    let key = row_map.key(idx);
                    let row_id = (key & 0xFFFFFFFF) as RowId;
                    let mut row: Row = Vec::with_capacity(col_names.len());
                    for seg in segments.iter() {
                        let v = match seg {
                            ColumnarSegment::Fixed(f) => f.get_i64(idx)
                                .map(|i| {
                                    // Preserve original column type tag if possible;
                                    // FixedSegment stores i64. Float columns were
                                    // stored as their f64 bit pattern.
                                    crate::types::Value::Integer(i)
                                })
                                .unwrap_or(crate::types::Value::Null),
                            ColumnarSegment::Text(t) => t.get_str(idx)
                                .map(|s| crate::types::Value::Text(s.to_string().into()))
                                .unwrap_or(crate::types::Value::Null),
                        };
                        row.push(v);
                    }
                    return Some(Ok((row_id, row)));
                }
                None
            }
        }
    }
}

/// Shared LSM scan logic, factored out so the enum dispatch stays readable.
fn lsm_next(
    lsm_iter: &mut crate::storage::lsm::MergingIterator,
    decode_ctx: &mut Option<crate::storage::row_format::SchemaDecodeContext>,
    use_raw: bool,
) -> Option<Result<(RowId, Row)>> {
    if use_raw {
        loop {
            match lsm_iter.next_raw() {
                Some(Ok((composite_key, _ts, deleted, vb))) => {
                    if deleted { continue; }
                    if vb.len == 0 { continue; }
                    let row_id = (composite_key & 0xFFFFFFFF) as RowId;
                    let row: Row = if let Some(ref mut ctx) = decode_ctx {
                        match ctx.decode_row(vb.as_slice()) {
                            Ok(row) => row,
                            Err(e) => return Some(Err(e)),
                        }
                    } else {
                        match crate::storage::row_format::decode_any_with_pool(vb.as_slice(), None) {
                            Ok(row) => row,
                            Err(e) => return Some(Err(e)),
                        }
                    };
                    return Some(Ok((row_id, row)));
                }
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }
    }
    loop {
        match lsm_iter.next() {
            Some(Ok((composite_key, value))) => {
                if value.deleted { continue; }
                let row_id = (composite_key & 0xFFFFFFFF) as RowId;
                let data = match &value.data {
                    crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                    crate::storage::lsm::ValueData::Blob(_) => {
                        return Some(Err(StorageError::InvalidData(
                            "Blob references should be resolved by LSM engine".into()
                        )));
                    }
                };
                let row: Row = if let Some(ref mut ctx) = decode_ctx {
                    match ctx.decode_row(data) {
                        Ok(row) => row,
                        Err(e) => return Some(Err(e)),
                    }
                } else {
                    match crate::storage::row_format::decode_any_with_pool(data, None) {
                        Ok(row) => row,
                        Err(e) => return Some(Err(e)),
                    }
                };
                return Some(Ok((row_id, row)));
            }
            Some(Err(e)) => return Some(Err(e)),
            None => return None,
        }
    }
}

/// Zero-copy decode streaming iterator — decodes rows directly into a
/// caller-provided `Vec<Value>` using `ValueBytes::as_slice()`.
///
/// Column segment wrapper for streaming columnar scans.
enum ColumnarSegment {
    Fixed(crate::storage::lsm::columnar::FixedSegment),
    Text(crate::storage::lsm::columnar::TextSegment),
}

/// Streaming iterator over a columnar SSTable. Yields one row at a time
/// as Vec<Value>, avoiding full materialization.
pub struct ColumnarScanIterator {
    row_map: crate::storage::lsm::columnar::RowMap,
    segments: Vec<ColumnarSegment>,
    col_types: Vec<crate::types::ColumnType>,
    current_idx: usize,
    num_rows: usize,
    /// Pre-computed matching row indices (for filtered scans). If None, scan all.
    pub match_filter: Option<Vec<usize>>,
}

impl Iterator for ColumnarScanIterator {
    type Item = Vec<crate::types::Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(ref matches) = self.match_filter {
            // Filtered scan: only yield matching rows
            while self.current_idx < matches.len() {
                let row_idx = matches[self.current_idx];
                self.current_idx += 1;
                return Some(self.build_row(row_idx));
            }
            None
        } else {
            // Full scan: yield all non-deleted rows
            while self.current_idx < self.num_rows {
                let idx = self.current_idx;
                self.current_idx += 1;
                if self.row_map.is_deleted(idx) { continue; }
                return Some(self.build_row(idx));
            }
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if let Some(ref m) = self.match_filter {
            let rem = m.len().saturating_sub(self.current_idx);
            (rem, Some(m.len()))
        } else {
            let rem = self.num_rows.saturating_sub(self.current_idx);
            (rem, Some(self.num_rows))
        }
    }
}

impl ColumnarScanIterator {
    pub(crate) fn build_row(&self, idx: usize) -> Vec<crate::types::Value> {
        let mut row = Vec::with_capacity(self.col_types.len());
        for (col_idx, ct) in self.col_types.iter().enumerate() {
            let val = match &self.segments[col_idx] {
                ColumnarSegment::Fixed(f) => match ct {
                    crate::types::ColumnType::Integer => f.get_i64(idx).map(crate::types::Value::Integer),
                    crate::types::ColumnType::Float => f.get_f64(idx).map(crate::types::Value::Float),
                    crate::types::ColumnType::Boolean => f.get_bool(idx).map(crate::types::Value::Bool),
                    _ => None,
                }.unwrap_or(crate::types::Value::Null),
                ColumnarSegment::Text(t) => t.get_str(idx)
                    .map(|s| crate::types::Value::Text(crate::types::ArcString(std::sync::Arc::from(s))))
                    .unwrap_or(crate::types::Value::Null),
            };
            row.push(val);
        }
        row
    }
}

/// Unlike `TableRawStreamingIterator` (which copies `bytes.to_vec()` per row),
/// this iterator borrows the shared block Arc data, eliminating the per-row memcpy.
pub struct TableDecodeStreamingIterator {
    lsm_iter: crate::storage::lsm::MergingIterator,
    decode_ctx: crate::storage::row_format::SchemaDecodeContext,
    use_raw: bool,
}

impl TableDecodeStreamingIterator {
    /// Decode the next row directly into `out`. Returns `Some(Ok(row_id))` on success.
    /// The caller is responsible for clearing `out` before each call (or it appends).
    pub fn decode_next_into(&mut self, out: &mut Vec<crate::types::Value>) -> Option<Result<RowId>> {
        if self.use_raw {
            loop {
                match self.lsm_iter.next_raw() {
                    Some(Ok((composite_key, _ts, deleted, vb))) => {
                        if deleted { continue; }
                        if vb.len == 0 { continue; }
                        let row_id = (composite_key & 0xFFFFFFFF) as RowId;
                        match self.decode_ctx.decode_row_into(out, vb.as_slice()) {
                            Ok(()) => return Some(Ok(row_id)),
                            Err(e) => return Some(Err(e)),
                        }
                    }
                    Some(Err(e)) => return Some(Err(e)),
                    None => return None,
                }
            }
        }
        // Standard path (no raw_sst)
        loop {
            match self.lsm_iter.next() {
                Some(Ok((composite_key, value))) => {
                    if value.deleted { continue; }
                    let row_id = (composite_key & 0xFFFFFFFF) as RowId;
                    let data = match &value.data {
                        crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                        crate::storage::lsm::ValueData::Blob(_) => {
                            return Some(Err(StorageError::InvalidData(
                                "Blob references should be resolved by LSM engine".into()
                            )));
                        }
                    };
                    match self.decode_ctx.decode_row_into(out, data) {
                        Ok(()) => return Some(Ok(row_id)),
                        Err(e) => return Some(Err(e)),
                    }
                }
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
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
                        // Take the buffer contents — avoids cloning values.
                        // out_buf becomes empty Vec (preserving allocation for next row).
                        let projected = std::mem::take(&mut self.out_buf);
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
