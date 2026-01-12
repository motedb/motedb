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
use crate::types::{Row, RowId, Value, PartitionId};
use crate::txn::wal::WALRecord;
use super::core::MoteDB;

impl MoteDB {
    // ==================== Row-Level CRUD Operations ====================
    
    /// Insert a row (default table API)
    /// 
    /// # Example
    /// ```
    /// let row_id = db.insert_row(vec![Value::Integer(1), Value::Text("Alice".into())])?;
    /// ```
    pub fn insert_row(&self, row: Row) -> Result<RowId> {
        // 1. Allocate row ID
        let row_id = {
            let mut next_id = self.next_row_id.write();
            let id = *next_id;
            *next_id += 1;
            id
        };

        // Use "_default" table with composite key format
        let table_name = "_default";
        let composite_key = self.make_composite_key(table_name, row_id);

        // 2. Determine partition
        let partition = (composite_key % self.num_partitions as u64) as PartitionId;

        // 3. Write to WAL first (durability)
        self.wal.log_insert(table_name, partition, composite_key, row.clone())?;
        
        // 4. Write to LSM MemTable
        let row_data = bincode::serialize(&row)?;
        let value = crate::storage::lsm::Value::new(row_data, composite_key);
        self.lsm_engine.put(composite_key, value)?;

        // 5. Increment pending counter (for auto-flush)
        self.increment_pending_updates();

        Ok(row_id)
    }
    
    /// Get a row by row ID
    /// 
    /// # Example
    /// ```
    /// let row_id = db.insert_row(vec![Value::Text(Text::from("hello"))])?;
    /// let row = db.get_row(row_id)?.unwrap();
    /// ```
    pub fn get_row(&self, row_id: RowId) -> Result<Option<Row>> {
        let table_name = "_default";
        let composite_key = self.make_composite_key(table_name, row_id);
        
        // Read from LSM engine
        let value = self.lsm_engine.get(composite_key)?;
        
        match value {
            Some(v) => {
                // Check if row is deleted (tombstone)
                if v.deleted {
                    return Ok(None);
                }
                
                // Extract data from ValueData
                let data = match &v.data {
                    crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                    crate::storage::lsm::ValueData::Blob(_) => {
                        return Err(StorageError::InvalidData(
                            "Blob references should be resolved by LSM engine".into()
                        ));
                    }
                };
                
                // Deserialize row data
                let row: Row = bincode::deserialize(data)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                Ok(Some(row))
            }
            None => Ok(None),
        }
    }
    
    /// Update a row (replace entire row)
    /// 
    /// # Example
    /// ```
    /// let row_id = db.insert_row(vec![Value::Text(Text::from("old"))])?;
    /// db.update_row(row_id, vec![Value::Text(Text::from("new"))])?;
    /// ```
    pub fn update_row(&self, row_id: RowId, new_row: Row) -> Result<()> {
        let table_name = "_default";
        let composite_key = self.make_composite_key(table_name, row_id);
        
        // 1. Get old row data (needed for WAL)
        let old_row = self.get_row(row_id)?
            .ok_or_else(|| StorageError::InvalidData(format!("Row {} not found", row_id)))?;
        
        // 2. Determine partition
        let partition = (composite_key % self.num_partitions as u64) as PartitionId;
        
        // 3. Write to WAL first (durability)
        self.wal.log_update(table_name, partition, composite_key, old_row, new_row.clone())?;
        
        // 4. Update in LSM MemTable
        let row_data = bincode::serialize(&new_row)?;
        let value = crate::storage::lsm::Value::new(row_data, composite_key);
        self.lsm_engine.put(composite_key, value)?;
        
        Ok(())
    }
    
    /// Delete a row by row ID
    /// 
    /// # Example
    /// ```
    /// let row_id = db.insert_row(vec![Value::Text(Text::from("hello"))])?;
    /// db.delete_row(row_id)?;
    /// assert!(db.get_row(row_id)?.is_none());
    /// ```
    pub fn delete_row(&self, row_id: RowId) -> Result<()> {
        let table_name = "_default";
        let composite_key = self.make_composite_key(table_name, row_id);
        
        // 1. Get old row data (needed for WAL)
        let old_row = self.get_row(row_id)?
            .ok_or_else(|| StorageError::InvalidData(format!("Row {} not found", row_id)))?;
        
        // 2. Determine partition
        let partition = (composite_key % self.num_partitions as u64) as PartitionId;
        
        // 3. Write to WAL first (durability)
        self.wal.log_delete(table_name, partition, composite_key, old_row)?;
        
        // 4. Delete from LSM (using tombstone)
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| StorageError::InvalidData(e.to_string()))?
            .as_micros() as u64;
        
        self.lsm_engine.delete(composite_key, timestamp)?;
        
        Ok(())
    }
    
    /// Scan all row IDs (using timestamp index)
    /// 
    /// # Example
    /// ```
    /// let row_ids = db.scan_all_row_ids()?;
    /// for row_id in row_ids {
    ///     let row = db.get_row(row_id)?;
    ///     // process row
    /// }
    /// ```
    pub fn scan_all_row_ids(&self) -> Result<Vec<RowId>> {
        // Use timestamp index to get all rows (0 to i64::MAX)
        self.query_timestamp_range(0, i64::MAX)
    }
    
    /// Scan all rows (memory intensive, use with caution)
    /// 
    /// # Example
    /// ```
    /// let rows = db.scan_all_rows()?;
    /// println!("Total rows: {}", rows.len());
    /// ```
    pub fn scan_all_rows(&self) -> Result<Vec<(RowId, Row)>> {
        let row_ids = self.scan_all_row_ids()?;
        let mut rows = Vec::with_capacity(row_ids.len());
        
        for row_id in row_ids {
            if let Some(row) = self.get_row(row_id)? {
                rows.push((row_id, row));
            }
        }
        
        Ok(rows)
    }
    
    /// Scan rows with callback (streaming, memory-friendly)
    /// 
    /// The callback receives (row_id, row) and should return Ok(true) to continue
    /// scanning, or Ok(false) to stop.
    /// 
    /// # Example
    /// ```
    /// db.scan_rows_with(|row_id, row| {
    ///     println!("Row {}: {:?}", row_id, row);
    ///     Ok(true)  // continue scanning
    /// })?;
    /// ```
    pub fn scan_rows_with<F>(&self, mut callback: F) -> Result<()>
    where
        F: FnMut(RowId, Row) -> Result<bool>,
    {
        let row_ids = self.scan_all_row_ids()?;
        
        for row_id in row_ids {
            if let Some(row) = self.get_row(row_id)? {
                if !callback(row_id, row)? {
                    break;
                }
            }
        }
        
        Ok(())
    }
    
    /// Batch get rows (more efficient than multiple get_row calls)
    /// 
    /// # Example
    /// ```
    /// let row_ids = vec![1, 2, 3, 4, 5];
    /// let rows = db.batch_get_rows(&row_ids)?;
    /// ```
    pub fn batch_get_rows(&self, row_ids: &[RowId]) -> Result<Vec<Option<Row>>> {
        let mut rows = Vec::with_capacity(row_ids.len());
        
        for &row_id in row_ids {
            rows.push(self.get_row(row_id)?);
        }
        
        Ok(rows)
    }
    
    // ==================== Table-Aware CRUD Operations ====================
    
    /// Insert a row to a specific table (table-aware API)
    /// 
    /// # Arguments
    /// * `table_name` - Name of the table
    /// * `row` - Row data to insert
    /// 
    /// # Example
    /// ```
    /// let row_id = db.insert_row_to_table("users", vec![
    ///     Value::Integer(1),
    ///     Value::Text("Alice".into()),
    /// ])?;
    /// ```
    pub fn insert_row_to_table(&self, table_name: &str, row: Row) -> Result<RowId> {
        // 1. Get table schema
        let schema = self.table_registry.get_table(table_name)?;
        
        // 2. Validate row
        schema.validate_row(&row)
            .map_err(|e| StorageError::InvalidData(format!(
                "Row validation failed for table '{}': {}",
                table_name, e
            )))?;
        
        // 3. Allocate row ID
        let row_id = {
            let mut next_id = self.next_row_id.write();
            let id = *next_id;
            *next_id += 1;
            id
        };

        // 4. Determine partition
        let partition = (row_id % self.num_partitions as u64) as PartitionId;

        // 5. Write to WAL first (durability)
        self.wal.log_insert(table_name, partition, row_id, row.clone())?;
        
        // 6. Write to LSM MemTable with table prefix
        let row_data = bincode::serialize(&row)?;
        let value = crate::storage::lsm::Value::new(row_data, row_id);
        
        let composite_key = self.make_composite_key(table_name, row_id);
        self.lsm_engine.put(composite_key, value)?;

        // 7. 🚀 增量更新所有索引（INSERT时实时维护）
        for col_def in &schema.columns {
            let col_name = &col_def.name;
            let col_value = row.get(col_def.position);
            
            if col_value.is_none() {
                continue;
            }
            let col_value = col_value.unwrap();
            
            // 7.1 Column Index（包括主键）
            let column_index_name = format!("{}.{}", table_name, col_name);
            if self.column_indexes.contains_key(&column_index_name) {
                if let Err(e) = self.insert_column_value(table_name, col_name, row_id, col_value) {
                    eprintln!("[insert_row] ⚠️ Failed to update column index '{}': {}", column_index_name, e);
                }
            }
            
            // 7.2 Vector Index
            if let crate::types::ColumnType::Tensor(_dim) = col_def.col_type {
                let index_name = format!("{}_{}", table_name, col_name);
                if self.vector_indexes.contains_key(&index_name) {
                    if let crate::types::Value::Vector(vec) = col_value {
                        if let Err(e) = self.update_vector(row_id, &index_name, vec) {
                            eprintln!("[insert_row] ⚠️ Failed to update vector index '{}': {}", index_name, e);
                        }
                    }
                }
            }
            
            // 7.3 Text Index
            if matches!(col_def.col_type, crate::types::ColumnType::Text) {
                let index_name = format!("{}_{}", table_name, col_name);
                if self.text_indexes.contains_key(&index_name) {
                    if let crate::types::Value::Text(text) = col_value {
                        if let Err(e) = self.insert_text(row_id, &index_name, text) {
                            eprintln!("[insert_row] ⚠️ Failed to update text index '{}': {}", index_name, e);
                        }
                    }
                }
            }
            
            // 7.4 Spatial Index
            if matches!(col_def.col_type, crate::types::ColumnType::Spatial) {
                let index_name = format!("{}_{}", table_name, col_name);
                if self.spatial_indexes.contains_key(&index_name) {
                    if let crate::types::Value::Spatial(geom) = col_value {
                        if let Err(e) = self.insert_geometry(row_id, &index_name, geom.clone()) {
                            eprintln!("[insert_row] ⚠️ Failed to update spatial index '{}': {}", index_name, e);
                        }
                    }
                }
            }
        }

        // 9. Increment pending counter
        self.increment_pending_updates();

        Ok(row_id)
    }
    
    /// Get a row from a specific table (table-aware API)
    /// 
    /// # Arguments
    /// * `table_name` - Name of the table
    /// * `row_id` - Internal row ID
    /// 
    /// # Example
    /// ```
    /// let row = db.get_table_row("users", row_id)?;
    /// ```
    pub fn get_table_row(&self, table_name: &str, row_id: RowId) -> Result<Option<Row>> {
        // Validate table exists
        let _schema = self.table_registry.get_table(table_name)?;
        
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
            
            // Deserialize row
            let row: Row = bincode::deserialize(data)
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
    /// * `new_row` - New row data
    /// 
    /// # Example
    /// ```
    /// db.update_row_in_table("users", row_id, vec![Value::Integer(1), Value::Text("Bob".into())])?;
    /// ```
    pub fn update_row_in_table(&self, table_name: &str, row_id: RowId, new_row: Row) -> Result<()> {
        // 1. Get schema and old row
        let schema = self.table_registry.get_table(table_name)?;
        let old_row = self.get_table_row(table_name, row_id)?
            .ok_or_else(|| StorageError::InvalidData(format!("Row {} not found in table '{}'", row_id, table_name)))?;
        
        // 2. Construct composite key
        let composite_key = self.make_composite_key(table_name, row_id);
        
        // 3. Determine partition
        let partition = (composite_key % self.num_partitions as u64) as PartitionId;
        
        // 4. Write to WAL first (durability)
        self.wal.log_update(table_name, partition, composite_key, old_row.clone(), new_row.clone())?;
        
        // 5. Update in LSM MemTable
        let row_data = bincode::serialize(&new_row)?;
        let value = crate::storage::lsm::Value::new(row_data, composite_key);
        self.lsm_engine.put(composite_key, value)?;
        
        // 6. 🚀 增量更新所有索引（UPDATE时实时维护）
        for col_def in &schema.columns {
            let col_name = &col_def.name;
            let old_value = old_row.get(col_def.position);
            let new_value = new_row.get(col_def.position);
            
            // 跳过没有变化的列
            if old_value == new_value {
                continue;
            }
            
            // 6.1 Column Index
            let column_index_name = format!("{}.{}", table_name, col_name);
            if self.column_indexes.contains_key(&column_index_name) {
                if let (Some(old_val), Some(new_val)) = (old_value, new_value) {
                    if let Err(e) = self.update_column_value(table_name, col_name, row_id, old_val, new_val) {
                        eprintln!("[update_row] ⚠️ Failed to update column index '{}': {}", column_index_name, e);
                    }
                }
            }
            
            // 6.2 Vector Index
            if let crate::types::ColumnType::Tensor(_dim) = col_def.col_type {
                let index_name = format!("{}_{}", table_name, col_name);
                if self.vector_indexes.contains_key(&index_name) {
                    // ✅ 先删除旧向量（清理DiskANN图）
                    if let Err(e) = self.delete_vector(row_id, &index_name) {
                        eprintln!("[update_row] ⚠️ Failed to delete old vector '{}': {}", index_name, e);
                    }
                    
                    // 再插入新向量
                    if let Some(crate::types::Value::Vector(new_vec)) = new_value {
                        if let Err(e) = self.update_vector(row_id, &index_name, new_vec) {
                            eprintln!("[update_row] ⚠️ Failed to update vector index '{}': {}", index_name, e);
                        }
                    }
                }
            }
            
            // 6.3 Text Index
            if matches!(col_def.col_type, crate::types::ColumnType::Text) {
                let index_name = format!("{}_{}", table_name, col_name);
                if self.text_indexes.contains_key(&index_name) {
                    if let (Some(crate::types::Value::Text(old_text)), Some(crate::types::Value::Text(new_text))) = (old_value, new_value) {
                        if let Err(e) = self.update_text(row_id, &index_name, old_text, new_text) {
                            eprintln!("[update_row] ⚠️ Failed to update text index '{}': {}", index_name, e);
                        }
                    }
                }
            }
            
            // 6.4 Spatial Index
            if matches!(col_def.col_type, crate::types::ColumnType::Spatial) {
                let index_name = format!("{}_{}", table_name, col_name);
                if self.spatial_indexes.contains_key(&index_name) {
                    // ✅ 先删除旧几何体
                    if let Err(e) = self.delete_geometry(row_id, &index_name) {
                        eprintln!("[update_row] ⚠️ Failed to delete old spatial geometry '{}': {}", index_name, e);
                    }
                    
                    // 再插入新的
                    if let Some(crate::types::Value::Spatial(new_geom)) = new_value {
                        if let Err(e) = self.insert_geometry(row_id, &index_name, new_geom.clone()) {
                            eprintln!("[update_row] ⚠️ Failed to update spatial index '{}': {}", index_name, e);
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Delete a row from a specific table (table-aware API)
    /// 
    /// # Arguments
    /// * `table_name` - Name of the table
    /// * `row_id` - Internal row ID
    /// 
    /// # Example
    /// ```
    /// db.delete_row_from_table("users", row_id)?;
    /// ```
    pub fn delete_row_from_table(&self, table_name: &str, row_id: RowId) -> Result<()> {
        // 1. Get schema and old row
        let schema = self.table_registry.get_table(table_name)?;
        let old_row = self.get_table_row(table_name, row_id)?
            .ok_or_else(|| StorageError::InvalidData(format!("Row {} not found in table '{}'", row_id, table_name)))?;
        
        // 2. 🚀 增量更新所有索引（DELETE时先删除索引）
        for col_def in &schema.columns {
            let col_name = &col_def.name;
            let col_value = old_row.get(col_def.position);
            
            if col_value.is_none() {
                continue;
            }
            let col_value = col_value.unwrap();
            
            // 2.1 Column Index
            let column_index_name = format!("{}.{}", table_name, col_name);
            if self.column_indexes.contains_key(&column_index_name) {
                if let Err(e) = self.delete_column_value(table_name, col_name, row_id, col_value) {
                    eprintln!("[delete_row] ⚠️ Failed to delete from column index '{}': {}", column_index_name, e);
                }
            }
            
            // 2.2 Vector Index - 显式删除（清理DiskANN图）
            if let crate::types::ColumnType::Tensor(_dim) = col_def.col_type {
                let index_name = format!("{}_{}", table_name, col_name);
                if self.vector_indexes.contains_key(&index_name) {
                    if let Err(e) = self.delete_vector(row_id, &index_name) {
                        eprintln!("[delete_row] ⚠️ Failed to delete from vector index '{}': {}", index_name, e);
                    }
                }
            }
            
            // 2.3 Text Index
            if matches!(col_def.col_type, crate::types::ColumnType::Text) {
                let index_name = format!("{}_{}", table_name, col_name);
                if self.text_indexes.contains_key(&index_name) {
                    if let crate::types::Value::Text(text) = col_value {
                        if let Err(e) = self.delete_text(row_id, &index_name, text) {
                            eprintln!("[delete_row] ⚠️ Failed to delete from text index '{}': {}", index_name, e);
                        }
                    }
                }
            }
            
            // 2.4 Spatial Index - 显式删除
            if matches!(col_def.col_type, crate::types::ColumnType::Spatial) {
                let index_name = format!("{}_{}", table_name, col_name);
                if self.spatial_indexes.contains_key(&index_name) {
                    if let Err(e) = self.delete_geometry(row_id, &index_name) {
                        eprintln!("[delete_row] ⚠️ Failed to delete from spatial index '{}': {}", index_name, e);
                    }
                }
            }
        }
        
        // 3. Construct composite key
        let composite_key = self.make_composite_key(table_name, row_id);
        
        // 4. Determine partition
        let partition = (composite_key % self.num_partitions as u64) as PartitionId;
        
        // 5. Write to WAL first (durability)
        self.wal.log_delete(table_name, partition, composite_key, old_row)?;
        
        // 6. Delete from LSM (using tombstone)
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| StorageError::InvalidData(e.to_string()))?
            .as_micros() as u64;
        
        self.lsm_engine.delete(composite_key, timestamp)?;
        
        Ok(())
    }
    
    /// Scan all rows in a specific table
    /// 
    /// # Arguments
    /// * `table_name` - Name of the table
    /// 
    /// # Example
    /// ```
    /// let rows = db.scan_table_rows("users")?;
    /// ```
    pub fn scan_table_rows(&self, table_name: &str) -> Result<Vec<(RowId, Row)>> {
        // Get table schema first (validates table exists)
        let _schema = self.table_registry.get_table(table_name)?;
        
        // Use LSM range scan to scan keys for this table
        let table_prefix = self.compute_table_prefix(table_name);
        let start_key = table_prefix << 32;
        let end_key = (table_prefix + 1) << 32;
        
        // Scan LSM (MemTable + Immutable + SSTables)
        let lsm_rows = self.lsm_engine.scan_range(start_key, end_key)?;
        
        let mut result = Vec::new();
        
        // Process results
        for (composite_key, value) in lsm_rows {
            // Extract row_id from composite_key
            let row_id = (composite_key & 0xFFFFFFFF) as RowId;
            
            // Extract data
            let data = match &value.data {
                crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                crate::storage::lsm::ValueData::Blob(_) => {
                    return Err(StorageError::InvalidData(
                        "Blob references should be resolved by LSM engine".into()
                    ));
                }
            };
            
            // Deserialize row
            let row: Row = bincode::deserialize(data)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            result.push((row_id, row));
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
    /// ```
    /// let rows = vec![
    ///     vec![Value::Integer(1), Value::Text("Alice".into())],
    ///     vec![Value::Integer(2), Value::Text("Bob".into())],
    /// ];
    /// let row_ids = db.batch_insert_rows_to_table("users", rows)?;
    /// ```
    pub fn batch_insert_rows_to_table(&self, table_name: &str, rows: Vec<Row>) -> Result<Vec<RowId>> {
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
        
        // 3. Batch allocate row IDs
        let mut row_ids = Vec::with_capacity(rows.len());
        {
            let mut next_id = self.next_row_id.write();
            for _ in 0..rows.len() {
                row_ids.push(*next_id);
                *next_id += 1;
            }
        }
        
        // 4. Build WAL records
        let mut wal_records = Vec::with_capacity(rows.len());
        for (row_id, row) in row_ids.iter().zip(rows.iter()) {
            let partition = (*row_id % self.num_partitions as u64) as PartitionId;
            wal_records.push(WALRecord::Insert {
                table_name: table_name.to_string(),
                row_id: *row_id,
                partition,
                data: row.clone(),
            });
        }
        
        // 5. Batch write WAL (single fsync)
        self.wal.batch_append(0, wal_records)?;
        
        // 6. Write to LSM MemTable
        for (row_id, row) in row_ids.iter().zip(rows.iter()) {
            let row_data = bincode::serialize(row)?;
            let value = crate::storage::lsm::Value::new(row_data, *row_id);
            let composite_key = self.make_composite_key(table_name, *row_id);
            self.lsm_engine.put(composite_key, value)?;
        }
        
        // 7. 🚀 增量更新所有索引（与 insert_row_to_table 保持一致）
        debug_log!("[batch_insert_rows_to_table] Updating indexes incrementally for {} rows in table '{}'", rows.len(), table_name);
        
        for (row_id, row) in row_ids.iter().zip(rows.iter()) {
            for col_def in &schema.columns {
                let col_name = &col_def.name;
                let col_value = row.get(col_def.position);
                
                if col_value.is_none() {
                    continue;
                }
                let col_value = col_value.unwrap();
                
                // 7.1 Column Index
                let column_index_name = format!("{}.{}", table_name, col_name);
                if self.column_indexes.contains_key(&column_index_name) {
                    if let Err(e) = self.insert_column_value(table_name, col_name, *row_id, col_value) {
                        eprintln!("[batch_insert] ⚠️ Failed to update column index '{}': {}", column_index_name, e);
                    }
                }
                
                // 7.2 Vector Index
                if let crate::types::ColumnType::Tensor(_dim) = col_def.col_type {
                    // 🔧 Use index_registry to find the correct user-specified index name
                    if let Some(index_name) = self.index_registry.find_by_column(table_name, col_name, crate::database::index_metadata::IndexType::Vector) {
                        if let crate::types::Value::Vector(vec) = col_value {
                            if let Err(e) = self.update_vector(*row_id, &index_name, vec) {
                                eprintln!("[batch_insert] ⚠️ Failed to update vector index '{}': {}", index_name, e);
                            }
                        }
                    }
                }
                
                // 7.3 Text Index
                if matches!(col_def.col_type, crate::types::ColumnType::Text) {
                    // 🔧 Use index_registry to find the correct user-specified index name
                    if let Some(index_name) = self.index_registry.find_by_column(table_name, col_name, crate::database::index_metadata::IndexType::Text) {
                        if let crate::types::Value::Text(text) = col_value {
                            if let Err(e) = self.insert_text(*row_id, &index_name, text) {
                                eprintln!("[batch_insert] ⚠️ Failed to update text index '{}': {}", index_name, e);
                            }
                        }
                    }
                }
                
                // 7.4 Spatial Index
                if matches!(col_def.col_type, crate::types::ColumnType::Spatial) {
                    // 🔧 Use index_registry to find the correct user-specified index name
                    if let Some(index_name) = self.index_registry.find_by_column(table_name, col_name, crate::database::index_metadata::IndexType::Spatial) {
                        if let crate::types::Value::Spatial(geom) = col_value {
                            if let Err(e) = self.insert_geometry(*row_id, &index_name, geom.clone()) {
                                eprintln!("[batch_insert] ⚠️ Failed to update spatial index '{}': {}", index_name, e);
                            }
                        }
                    }
                }
                
                // 7.5 Timestamp Index (legacy single-index architecture, handled by batch build)
                // Note: Timestamp index uses a different architecture (single BTree index)
                // and is updated during flush via batch building
            }
        }
        
        // 8. Increment pending counter
        {
            let mut pending = self.pending_updates.write();
            *pending += rows.len();
            
            if *pending >= 1_000 {
                *pending = 0;
                drop(pending);
                
                let db_clone = self.clone_for_callback();
                std::thread::spawn(move || {
                    let _ = db_clone.flush();
                });
            }
        }
        
        Ok(row_ids)
    }
    
    /// Batch insert rows (10-20x faster than individual inserts)
    /// 
    /// **NOTE**: This is the legacy API without table name, kept for backward compatibility.
    /// For table-aware batch insert with index updates, use `batch_insert_rows_to_table()`.
    /// 
    /// # Example
    /// ```
    /// let rows = vec![
    ///     vec![Value::Integer(1)],
    ///     vec![Value::Integer(2)],
    ///     vec![Value::Integer(3)],
    /// ];
    /// let row_ids = db.batch_insert_rows(rows)?;
    /// ```
    pub fn batch_insert_rows(&self, rows: Vec<Row>) -> Result<Vec<RowId>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        // 1. Batch allocate row IDs
        let mut row_ids = Vec::with_capacity(rows.len());
        {
            let mut next_id = self.next_row_id.write();
            for _ in 0..rows.len() {
                row_ids.push(*next_id);
                *next_id += 1;
            }
        }

        // 2. Build WAL records
        let mut wal_records = Vec::with_capacity(rows.len());
        for (row_id, row) in row_ids.iter().zip(rows.iter()) {
            let partition = (*row_id % self.num_partitions as u64) as PartitionId;
            wal_records.push(WALRecord::Insert {
                table_name: "_default".to_string(),
                row_id: *row_id,
                partition,
                data: row.clone(),
            });
        }

        // 3. Batch write WAL (single fsync)
        self.wal.batch_append(0, wal_records)?;

        // 4. Write to LSM MemTable
        for (row_id, row) in row_ids.iter().zip(rows.iter()) {
            let row_data = bincode::serialize(row)?;
            let value = crate::storage::lsm::Value::new(row_data, *row_id);
            self.lsm_engine.put(*row_id, value)?;
        }

        // 5. Increment pending counter
        {
            let mut pending = self.pending_updates.write();
            *pending += rows.len();
            
            if *pending >= 1_000 {
                *pending = 0;
                drop(pending);
                
                let db_clone = self.clone_for_callback();
                std::thread::spawn(move || {
                    let _ = db_clone.flush();
                });
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
    /// ```
    /// let row_ids = vec![100, 101, 102, 103]; // Continuous
    /// let rows = db.get_table_rows_batch("robots", &row_ids)?;
    /// ```
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
    fn increment_pending_updates(&self) {
        let mut pending = self.pending_updates.write();
        *pending += 1;
        
        if *pending >= 1_000 {
            *pending = 0;
            drop(pending);
            
            let db_clone = self.clone_for_callback();
            std::thread::spawn(move || {
                let _ = db_clone.flush();
            });
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
            if current_id < 0 || current_id > i64::MAX / 2 {
                break;
            }
        }
        
        // Record prefetch attempt
        self.row_cache.record_prefetch(row_ids_to_fetch.len());
        
        // 🔧 FIX: Directly fetch from LSM without triggering get_table_rows_batch (avoid recursion)
        for row_id in row_ids_to_fetch {
            let composite_key = self.make_composite_key(table_name, row_id);
            
            if let Ok(Some(value)) = self.lsm_engine.get(composite_key) {
                if !value.deleted {
                    if let crate::storage::lsm::ValueData::Inline(bytes) = &value.data {
                        if let Ok(row) = bincode::deserialize::<Row>(bytes) {
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
            
            let row: Row = bincode::deserialize(data)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            
            // Cache row
            self.row_cache.put(table_name.to_string(), row_id, row.clone());
            result.push((row_id, Some(row)));
        }
        
        Ok(result)
    }
    
    /// Batch get using point queries (for random row_ids)
    fn get_table_rows_batch_point(&self, table_name: &str, row_ids: &[RowId]) -> Result<Vec<(RowId, Option<Row>)>> {
        let mut result = Vec::with_capacity(row_ids.len());
        
        for &row_id in row_ids {
            let row = self.get_table_row(table_name, row_id)?;
            result.push((row_id, row));
        }
        
        Ok(result)
    }
}
