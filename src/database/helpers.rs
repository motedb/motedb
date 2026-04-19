//! Database Helpers - Batch Index Building
//!
//! Extracted from database_legacy.rs
//! Contains batch index building methods called during LSM flush

use crate::types::{Row, RowId, Value, TableSchema};
use crate::{Result, StorageError};


use super::core::MoteDB;

impl MoteDB {
    /// Batch build all indexes for a specific table
    pub(crate) fn batch_build_table_indexes(&self, table_name: &str, rows: &[(RowId, Row)]) -> Result<()> {
        use std::time::Instant;
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
            handles.push(std::thread::Builder::new().spawn(move || {
                db.batch_build_column_indexes(&table_name, &schema, &rows)
            }));
        }
        
        // 2. Timestamp indexes
        {
            let db = self.clone_for_callback();
            let schema = schema.clone();
            let rows = rows.clone();
            handles.push(std::thread::Builder::new().spawn(move || {
                db.batch_build_timestamp_indexes(&schema, &rows)
            }));
        }
        
        // 3. Vector indexes
        {
            let db = self.clone_for_callback();
            let table_name = table_name.clone();
            let schema = schema.clone();
            let rows = rows.clone();
            handles.push(std::thread::Builder::new().spawn(move || {
                db.batch_build_vector_indexes(&table_name, &schema, &rows)
            }));
        }

        // 4. Text indexes
        {
            let db = self.clone_for_callback();
            let table_name_clone = table_name.clone();
            let schema = schema.clone();
            let rows = rows.clone();
            handles.push(std::thread::Builder::new().spawn(move || {
                db.batch_build_text_indexes(&table_name_clone, &schema, &rows)
            }));
        }
        
        // Wait for all threads
        for (idx, handle_result) in handles.into_iter().enumerate() {
            let handle = match handle_result {
                Ok(h) => h,
                Err(e) => {
                    debug_log!("[BatchIndexBuilder] ⚠️  Index type {} thread spawn failed: {}", idx, e);
                    continue;
                }
            };
            match handle.join() {
                Ok(Ok(())) => {},
                Ok(Err(e)) => {
                    debug_log!("[BatchIndexBuilder] ⚠️  Index type {} build failed: {}", idx, e);
                    return Err(e);
                }
                Err(_) => {
                    return Err(StorageError::Index("Thread panicked during index build".into()));
                }
            }
        }
        
        debug_log!("[BatchIndexBuilder]   ✓ Table '{}' indexes built in {:?} (4 parallel threads)", table_name, _start.elapsed());
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
            debug_log!("[TimestampIndex] Batch built {} entries in {:?}", count, start.elapsed());
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
