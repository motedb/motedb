//! Database Helpers - Batch Index Building
//!
//! Extracted from database_legacy.rs
//! Contains batch index building methods called during LSM flush

use crate::types::{Row, RowId, Value, TableSchema};
use crate::{Result, StorageError};
use std::collections::HashMap;

use super::core::MoteDB;

impl MoteDB {
    /// 🚀 Phase 3: Batch build indexes from flushed MemTable data
    /// 
    /// Called by LSM Engine's flush callback with all row data from MemTable
    pub(crate) fn batch_build_indexes_from_flush(&self, memtable: &crate::storage::lsm::UnifiedMemTable) -> Result<()> {
        use std::time::Instant;
        let start = Instant::now();
        
        let memtable_len = memtable.len();
        println!("[BatchIndexBuilder] 🔍 收到Flush回调，MemTable数据量: {}", memtable_len);
        
        if memtable_len == 0 {
            return Ok(());
        }
        
        // 🚀 Performance: Skip batch building for small datasets
        const MIN_BATCH_SIZE: usize = 500;
        if memtable_len < MIN_BATCH_SIZE {
            println!("[BatchIndexBuilder] ⚠️  跳过批量构建（数据量 {} < {}），依赖增量索引", 
                     memtable_len, MIN_BATCH_SIZE);
            return Ok(());
        }
        
        println!("[BatchIndexBuilder] 🚀 Building indexes from {} flushed rows", memtable_len);
        
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
            let table_hash = (composite_key >> 32) as u64;
            
            let row: Row = match bincode::deserialize(row_bytes) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("[BatchIndexBuilder] ⚠️  Failed to deserialize row {}: {}", row_id, e);
                    continue;
                }
            };
            
            let table_name = self.find_table_name_by_hash(table_hash)?;
            
            tables_data.entry(table_name)
                .or_insert_with(Vec::new)
                .push((row_id, row));
        }
        
        println!("[BatchIndexBuilder]   ↳ Grouped into {} tables", tables_data.len());
        
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
        
        println!("[BatchIndexBuilder] ✅ Batch index building complete in {:?} ({} tables)", start.elapsed(), tables_count);
        Ok(())
    }
    
    /// Find table name by hash (reverse lookup)
    fn find_table_name_by_hash(&self, table_hash: u64) -> Result<String> {
        // Try cache first (DashMap lock-free read)
        for entry in self.table_hash_cache.iter() {
            if *entry.value() == table_hash {
                return Ok(entry.key().clone());
            }
        }
        
        // Cache miss - compute and cache
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let tables = self.table_registry.list_tables()?;
        for table_name in tables {
            let mut hasher = DefaultHasher::new();
            table_name.hash(&mut hasher);
            let computed_hash = (hasher.finish() & 0xFFFFFFFF) as u64;
            
            if computed_hash == table_hash {
                self.table_hash_cache.insert(table_name.clone(), computed_hash);
                return Ok(table_name);
            }
        }
        
        Err(StorageError::Index(format!("Table not found for hash {}", table_hash)))
    }
    
    /// Batch build all indexes for a specific table
    fn batch_build_table_indexes(&self, table_name: &str, rows: &[(RowId, Row)]) -> Result<()> {
        use std::time::Instant;
        use std::thread;
        
        let start = Instant::now();
        
        println!("[BatchIndexBuilder]   📊 Table '{}': {} rows", table_name, rows.len());
        
        let schema = match self.table_registry.get_table(table_name) {
            Ok(s) => s,
            Err(_) => {
                println!("[BatchIndexBuilder]   ⏭  Skipping table '{}' (no schema registered)", table_name);
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
        
        println!("[BatchIndexBuilder]   ✓ Table '{}' indexes built in {:?} (5 parallel threads)", table_name, start.elapsed());
        Ok(())
    }
    
    /// Batch build column indexes
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
        for (index, col_name, batch) in indexes_with_data {
            if !batch.is_empty() {
                // 转换为引用
                let batch_refs: Vec<(RowId, &Value)> = batch.iter()
                    .map(|(row_id, value)| (*row_id, value))
                    .collect();
                
                index.write().insert_batch(&batch_refs)?;
                println!("[ColumnIndex]   ✓ Built {} entries for column '{}'", 
                         batch.len(), col_name);
            }
        }
        
        let duration = start.elapsed();
        println!("[ColumnIndex] Batch build complete in {:?}", duration);
        
        Ok(())
    }
    
    /// Batch build timestamp indexes
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
    fn batch_build_vector_indexes(&self, table_name: &str, schema: &TableSchema, rows: &[(RowId, Row)]) -> Result<()> {
        
        for col_def in &schema.columns {
            if let crate::types::ColumnType::Tensor(_dim) = col_def.col_type {
                let index_name = format!("{}_{}", table_name, col_def.name);
                if let Some(index_ref) = self.vector_indexes.get(&index_name) {
                    let index = index_ref.value();
                    let mut vectors = Vec::new();
                    for (row_id, row) in rows {
                        if let Some(crate::types::Value::Vector(vec)) = row.get(col_def.position) {
                            vectors.push((*row_id, vec.clone()));
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
