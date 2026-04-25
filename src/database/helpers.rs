//! Database Helpers - Batch Index Building
//!
//! Extracted from database_legacy.rs
//! Contains batch index building methods called during LSM flush.
//! Rows are sent as raw bytes from the flush callback and decoded
//! lazily in the builder thread to minimize flush latency.

use crate::types::{Row, RowId, Value, TableSchema};
use crate::{Result, StorageError};

use super::core::MoteDB;

impl MoteDB {
    /// Batch build all indexes for a specific table.
    ///
    /// Receives raw bytes from the flush callback, decodes them using schema,
    /// then dispatches to 4 parallel index builder threads sharing one Arc.
    pub(crate) fn batch_build_table_indexes_raw(&self, table_name: &str, raw_rows: &[(RowId, Vec<u8>)]) -> Result<()> {
        use std::sync::Arc;
        use std::time::Instant;
        let _start = Instant::now();

        let schema = match self.table_registry.get_table(table_name) {
            Ok(s) => s,
            Err(_) => return Ok(()),
        };

        // Decode all rows using schema (fast, no brute-force)
        let col_types = schema.col_types();
        let mut rows: Vec<(RowId, Row)> = Vec::with_capacity(raw_rows.len());
        for (row_id, raw) in raw_rows {
            match crate::storage::row_format::decode(raw, col_types) {
                Ok(r) => rows.push((*row_id, r)),
                Err(_) => {
                    if let Ok(r) = crate::storage::row_format::decode_any(raw) {
                        rows.push((*row_id, r));
                    }
                }
            }
        }

        if rows.is_empty() {
            return Ok(());
        }

        debug_log!("[BatchIndexBuilder]   📊 Table '{}': {} rows", table_name, rows.len());

        let rows = Arc::new(rows);
        let mut handles = vec![];

        // 1. Column indexes
        {
            let db = self.clone_for_callback();
            let table_name = table_name.to_string();
            let schema = schema.clone();
            let rows = Arc::clone(&rows);
            handles.push(std::thread::Builder::new().spawn(move || {
                db.batch_build_column_indexes(&table_name, &schema, &rows)
            }));
        }

        // 2. Timestamp indexes
        {
            let db = self.clone_for_callback();
            let schema = schema.clone();
            let rows = Arc::clone(&rows);
            handles.push(std::thread::Builder::new().spawn(move || {
                db.batch_build_timestamp_indexes(&schema, &rows)
            }));
        }

        // 3. Vector indexes
        {
            let db = self.clone_for_callback();
            let table_name = table_name.to_string();
            let schema = schema.clone();
            let rows = Arc::clone(&rows);
            handles.push(std::thread::Builder::new().spawn(move || {
                db.batch_build_vector_indexes(&table_name, &schema, &rows)
            }));
        }

        // 4. Text indexes
        {
            let db = self.clone_for_callback();
            let table_name = table_name.to_string();
            let schema = schema.clone();
            let rows = Arc::clone(&rows);
            handles.push(std::thread::Builder::new().spawn(move || {
                db.batch_build_text_indexes(&table_name, &schema, &rows)
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

        debug_log!("[BatchIndexBuilder]   ✓ Table '{}' indexes built in {:?}", table_name, _start.elapsed());
        Ok(())
    }

    /// Batch build column indexes
    fn batch_build_column_indexes(&self, table_name: &str, schema: &TableSchema, rows: &[(RowId, Row)]) -> Result<()> {
        use std::time::Instant;
        let start = Instant::now();

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

        for (index, _col_name, batch) in indexes_with_data {
            if !batch.is_empty() {
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
