//! Database Helpers - Batch Index Building
//!
//! Extracted from database_legacy.rs
//! Contains batch index building methods called during LSM flush

use crate::types::{Row, RowId, Value, TableSchema};
use crate::{Result, StorageError};


use super::core::MoteDB;

impl MoteDB {
    /// Batch build indexes from flushed MemTable data
    ///
    /// Called by LSM Engine's flush callback with all row data from MemTable.
    /// After P1.1 (table_id replaces hash), the deadlock is eliminated because
    /// find_table_name_by_id is a pure in-memory registry lookup — no LSM scan.
    #[allow(dead_code)]
    pub(crate) fn batch_build_indexes_from_flush(&self, memtable: &crate::storage::lsm::UnifiedMemTable) -> Result<()> {
        use std::time::Instant;
        let _start = Instant::now();

        let memtable_len = memtable.len();
        debug_log!("[BatchIndexBuilder] Flush callback received, MemTable entries: {}", memtable_len);

        if memtable_len == 0 {
            return Ok(());
        }

        // Skip batch building for very small datasets (rely on incremental indexing)
        const MIN_BATCH_SIZE: usize = 100;
        if memtable_len < MIN_BATCH_SIZE {
            debug_log!("[BatchIndexBuilder] Skipping ({} < {}), relying on incremental indexing",
                     memtable_len, MIN_BATCH_SIZE);
            return Ok(());
        }

        debug_log!("[BatchIndexBuilder] Building indexes from {} flushed rows", memtable_len);

        // Phase 1: Group rows by table_name using collision-free table_id
        let mut tables_data: std::collections::HashMap<String, Vec<(RowId, Row)>> = std::collections::HashMap::new();

        for (composite_key, entry) in memtable.iter() {
            if entry.deleted {
                continue;
            }

            let row_id = (composite_key & 0xFFFFFFFF) as RowId;
            let table_id = (composite_key >> 32) as u32;

            let row_bytes: Vec<u8> = match &entry.data {
                crate::storage::lsm::ValueData::Inline(bytes) => bytes.clone(),
                crate::storage::lsm::ValueData::Blob(blob_ref) => {
                    match self.lsm_engine.resolve_blob(blob_ref) {
                        Ok(data) => data,
                        Err(e) => {
                            eprintln!("[BatchIndexBuilder] Failed to resolve blob for row {}: {}", row_id, e);
                            continue;
                        }
                    }
                }
            };

            let row: Row = match bincode::deserialize(&row_bytes) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("[BatchIndexBuilder] Failed to deserialize row {}: {}", row_id, e);
                    continue;
                }
            };

            // Pure in-memory lookup — no LSM scan, no deadlock risk
            let table_name = match self.find_table_name_by_id(table_id) {
                Ok(name) => name,
                Err(_) => continue, // Unknown table_id, skip
            };

            tables_data.entry(table_name)
                .or_default()
                .push((row_id, row));
        }

        debug_log!("[BatchIndexBuilder] Grouped into {} tables", tables_data.len());

        // Phase 2: Build indexes per table
        for (table_name, rows) in &tables_data {
            if let Err(e) = self.batch_build_table_indexes(table_name, rows) {
                eprintln!("[BatchIndexBuilder] Warning: index build failed for table '{}': {:?}", table_name, e);
                // Don't fail the entire flush — index can be rebuilt later
            }
        }

        debug_log!("[BatchIndexBuilder] Batch index building complete in {:?} ({} tables)",
                 _start.elapsed(), tables_data.len());
        Ok(())
        
        /* DISABLED CODE - CAUSES DEADLOCKS
        if memtable_len == 0 {
            return Ok(());
        }
        
        // 🚀 Performance: Skip batch building for small datasets
        const MIN_BATCH_SIZE: usize = 500;
        if memtable_len < MIN_BATCH_SIZE {
            debug_log!("[BatchIndexBuilder] ⚠️  跳过批量构建（数据量 {} < {}），依赖增量索引", 
                     memtable_len, MIN_BATCH_SIZE);
            return Ok(());
        }
        
        debug_log!("[BatchIndexBuilder] 🚀 Building indexes from {} flushed rows", memtable_len);
        
        // Phase 1: Group rows by table_name
        let mut tables_data: HashMap<String, Vec<(RowId, Row)>> = HashMap::new();
        
        for (composite_key, entry) in memtable.iter() {
            if entry.deleted {
                continue;
            }
            
            let row_bytes = match &entry.data {
                crate::storage::lsm::ValueData::Inline(bytes) => bytes,
                crate::storage::lsm::ValueData::Blob(_) => {
                    eprintln!("[BatchIndexBuilder] ⚠️  Blob not supported for index building yet");
                    continue;
                }
            };
            
            let row_id = (composite_key & 0xFFFFFFFF) as RowId;
            let table_hash = composite_key >> 32;
            
            let row: Row = match bincode::deserialize(&row_bytes) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("[BatchIndexBuilder] ⚠️  Failed to deserialize row {}: {}", row_id, e);
                    continue;
                }
            };
            
            let table_name = self.find_table_name_by_hash(table_hash)?;
            
            tables_data.entry(table_name)
                .or_default()
                .push((row_id, row));
        }
        
        debug_log!("[BatchIndexBuilder]   ↳ Grouped into {} tables", tables_data.len());
        
        // Phase 2: Build indexes (parallel if multiple tables)
        let tables_count = tables_data.len();
        
        if tables_count == 1 {
            for (table_name, rows) in tables_data {
                self.batch_build_table_indexes(&table_name, &rows)?;
            }
        } else {
            use std::thread;
            let handles: Vec<_> = tables_data.into_iter().map(|(table_name, rows)| {
                let db = self.clone_for_callback();
                thread::spawn(move || {
                    db.batch_build_table_indexes(&table_name, &rows)
                })
            }).collect();
            
            for (idx, handle) in handles.into_iter().enumerate() {
                match handle.join() {
                    Ok(Ok(())) => {},
                    Ok(Err(e)) => {
                        eprintln!("[BatchIndexBuilder] ⚠️  Table {} build failed: {}", idx, e);
                        return Err(e);
                    }
                    Err(_) => {
                        return Err(StorageError::Index("Thread panicked during batch build".into()));
                    }
                }
            }
        }
        
        debug_log!("[BatchIndexBuilder] ✅ Batch index building complete in {:?} ({} tables)", start.elapsed(), tables_count);
        Ok(())
        */
    }
    
    /// Find table name by its stable sequential table_id (reverse lookup).
    ///
    /// This is a pure in-memory operation on the TableRegistry's HashMap
    /// — no LSM scan, no deadlock risk.
    #[allow(dead_code)]
    fn find_table_name_by_id(&self, table_id: u32) -> Result<String> {
        self.table_registry.get_table_name_by_id(table_id)
    }
    
    /// Batch build all indexes for a specific table
    #[allow(dead_code)]
    fn batch_build_table_indexes(&self, table_name: &str, rows: &[(RowId, Row)]) -> Result<()> {
        use std::time::Instant;
        use std::thread;
        
        let _start = Instant::now();

        debug_log!("[BatchIndexBuilder]   📊 Table '{}': {} rows", table_name, rows.len());
        
        let schema = match self.table_registry.get_table(table_name) {
            Ok(s) => s,
            Err(_) => {
                debug_log!("[BatchIndexBuilder]   ⏭  Skipping table '{}' (no schema registered)", table_name);
                return Ok(());
            }
        };
        
        let table_name = table_name.to_string();
        let rows = rows.to_vec();
        
        let mut handles = vec![];
        
        // 1. Column indexes
        {
            let db = self.clone_for_callback();
            let table_name = table_name.clone();
            let schema = schema.clone();
            let rows = rows.clone();
            handles.push(thread::spawn(move || {
                db.batch_build_column_indexes(&table_name, &schema, &rows)
            }));
        }
        
        // 2. Timestamp indexes
        {
            let db = self.clone_for_callback();
            let schema = schema.clone();
            let rows = rows.clone();
            handles.push(thread::spawn(move || {
                db.batch_build_timestamp_indexes(&schema, &rows)
            }));
        }
        
        // 3. Vector indexes
        {
            let db = self.clone_for_callback();
            let table_name = table_name.clone();
            let schema = schema.clone();
            let rows = rows.clone();
            handles.push(thread::spawn(move || {
                db.batch_build_vector_indexes(&table_name, &schema, &rows)
            }));
        }
        
        // 4. Spatial indexes
        {
            let db = self.clone_for_callback();
            let table_name = table_name.clone();
            let schema = schema.clone();
            let rows = rows.clone();
            handles.push(thread::spawn(move || {
                db.batch_build_spatial_indexes(&table_name, &schema, &rows)
            }));
        }
        
        // 5. Text indexes
        {
            let db = self.clone_for_callback();
            let table_name_clone = table_name.clone();
            let schema = schema.clone();
            let rows = rows.clone();
            handles.push(thread::spawn(move || {
                db.batch_build_text_indexes(&table_name_clone, &schema, &rows)
            }));
        }
        
        // Wait for all threads
        for (idx, handle) in handles.into_iter().enumerate() {
            match handle.join() {
                Ok(Ok(())) => {},
                Ok(Err(e)) => {
                    eprintln!("[BatchIndexBuilder] ⚠️  Index type {} build failed: {}", idx, e);
                    return Err(e);
                }
                Err(_) => {
                    return Err(StorageError::Index("Thread panicked during index build".into()));
                }
            }
        }
        
        debug_log!("[BatchIndexBuilder]   ✓ Table '{}' indexes built in {:?} (5 parallel threads)", table_name, _start.elapsed());
        Ok(())
    }
    
    /// Batch build column indexes
    #[allow(dead_code)]
    fn batch_build_column_indexes(&self, table_name: &str, schema: &TableSchema, rows: &[(RowId, Row)]) -> Result<()> {
        use std::time::Instant;
        let start = Instant::now();
        
        // 先收集所有需要的索引和数据，避免持锁期间执行I/O
        let indexes_with_data: Vec<_> = {
            schema.columns.iter().filter_map(|col_def| {
                let index_name = format!("{}.{}", table_name, col_def.name);
                self.column_indexes.get(&index_name).map(|index_ref| {
                    let index = index_ref.value().clone();
                    let mut batch: Vec<(RowId, Value)> = Vec::with_capacity(rows.len());
                    for (row_id, row) in rows {
                        if let Some(value) = row.get(col_def.position) {
                            batch.push((*row_id, value.clone()));
                        }
                    }
                    (index, col_def.name.clone(), batch)
                })
            }).collect()
        };
        
        // 批量插入（不持有 column_indexes 锁）
        for (index, _col_name, batch) in indexes_with_data {
            if !batch.is_empty() {
                // 转换为引用
                let batch_refs: Vec<(RowId, &Value)> = batch.iter()
                    .map(|(row_id, value)| (*row_id, value))
                    .collect();
                
                index.write().insert_batch(&batch_refs)?;
                debug_log!("[ColumnIndex]   ✓ Built {} entries for column '{}'",
                         batch.len(), _col_name);
            }
        }
        
        let _duration = start.elapsed();
        debug_log!("[ColumnIndex] Batch build complete in {:?}", _duration);
        
        Ok(())
    }
    
    /// Batch build timestamp indexes
    #[allow(dead_code)]
    fn batch_build_timestamp_indexes(&self, schema: &TableSchema, rows: &[(RowId, Row)]) -> Result<()> {
        use std::time::Instant;
        let start = Instant::now();
        
        let ts_col = match schema.columns.iter().find(|c| c.col_type == crate::types::ColumnType::Timestamp) {
            Some(col) => col,
            None => return Ok(()),
        };
        
        let mut ts_index = self.timestamp_index.write();
        let mut count = 0;
        
        for (row_id, row) in rows {
            if let Some(crate::types::Value::Timestamp(ts)) = row.get(ts_col.position) {
                ts_index.insert(ts.as_micros() as u64, *row_id)?;
                count += 1;
            }
        }
        
        if count > 0 {
            println!("[TimestampIndex] Batch built {} entries in {:?}", count, start.elapsed());
        }
        
        Ok(())
    }
    
    /// Batch build vector indexes
    #[allow(dead_code)]
    fn batch_build_vector_indexes(&self, table_name: &str, schema: &TableSchema, rows: &[(RowId, Row)]) -> Result<()> {
        
        for col_def in &schema.columns {
            if let crate::types::ColumnType::Tensor(_dim) = col_def.col_type {
                let index_name = format!("{}_{}", table_name, col_def.name);
                if let Some(index_ref) = self.vector_indexes.get(&index_name) {
                    let index = index_ref.value();
                    let mut vectors = Vec::new();
                    for (row_id, row) in rows {
                        if let Some(crate::types::Value::Vector(vec)) = row.get(col_def.position) {
                            vectors.push((*row_id, vec.to_vec()));
                        }
                    }
                    
                    if !vectors.is_empty() {
                        index.write().batch_insert(&vectors)?;
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Batch build spatial indexes
    #[allow(dead_code)]
    fn batch_build_spatial_indexes(&self, table_name: &str, schema: &TableSchema, rows: &[(RowId, Row)]) -> Result<()> {
        
        for col_def in &schema.columns {
            if let crate::types::ColumnType::Spatial = col_def.col_type {
                let index_name = format!("{}_{}", table_name, col_def.name);
                if let Some(index_ref) = self.spatial_indexes.get(&index_name) {
                    let index = index_ref.value();
                    let mut geometries = Vec::new();
                    for (row_id, row) in rows {
                        if let Some(crate::types::Value::Spatial(geom)) = row.get(col_def.position) {
                            geometries.push((*row_id, geom.clone()));
                        }
                    }
                    
                    if !geometries.is_empty() {
                        index.write().batch_insert(geometries)?;
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Batch build text indexes
    #[allow(dead_code)]
    fn batch_build_text_indexes(&self, table_name: &str, schema: &TableSchema, rows: &[(RowId, Row)]) -> Result<()> {
        use crate::index::builder::IndexBuilder;
        
        for col_def in &schema.columns {
            if matches!(col_def.col_type, crate::types::ColumnType::Text) {
                let index_name = format!("{}_{}", table_name, col_def.name);
                if let Some(index_ref) = self.text_indexes.get(&index_name) {
                    let index = index_ref.value();
                    let mut index_guard = index.write();
                    index_guard.build_from_memtable(rows)?;
                }
            }
        }
        
        Ok(())
    }
}
