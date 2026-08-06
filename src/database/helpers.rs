//! Database Helpers - Batch Index Building
//!
//! Extracted from database_legacy.rs
//! Contains batch index building methods called during LSM flush.
//! Rows are sent as raw bytes from the flush callback and decoded
//! lazily in the builder thread to minimize flush latency.

use crate::types::{Row, RowId, TableSchema, Value};
use crate::Result;

use super::core::MoteDB;

/// Get total size of all files in a directory
pub(crate) fn dir_size(dir: &std::path::Path) -> Result<u64> {
    let mut total = 0;
    if !dir.exists() {
        return Ok(0);
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_file() {
            total += metadata.len();
        }
    }
    Ok(total)
}

impl MoteDB {
    /// Batch build all indexes for a specific table.
    ///
    /// Receives raw bytes from the flush callback, decodes them using schema,
    /// then dispatches to 4 parallel index builder threads sharing one Arc.
    pub(crate) fn batch_build_table_indexes_raw(
        &self,
        table_name: &str,
        raw_rows: &[(RowId, Vec<u8>)],
    ) -> Result<()> {
        use std::sync::Arc;
        use std::time::Instant;
        let _start = Instant::now();

        let schema = match self.table_registry.get_table(table_name) {
            Ok(s) => s,
            Err(e) => {
                debug_log!(
                    "[BatchIndexBuilder] Table '{}' not found during index build: {}",
                    table_name,
                    e
                );
                return Ok(());
            }
        };

        // Decode all rows using schema (fast, no brute-force)
        let col_types = schema.col_types();
        let mut rows: Vec<(RowId, Row)> = Vec::with_capacity(raw_rows.len());
        let mut decode_failures = 0u32;
        for (row_id, raw) in raw_rows {
            match crate::storage::row_format::decode(raw, col_types) {
                Ok(r) => rows.push((*row_id, r)),
                Err(_) => {
                    if let Ok(r) = crate::storage::row_format::decode_any(raw) {
                        rows.push((*row_id, r));
                    } else {
                        decode_failures += 1;
                    }
                }
            }
        }
        if decode_failures > 0 {
            debug_log!(
                "[BatchIndexBuilder] Table '{}': {} rows failed to decode",
                table_name,
                decode_failures
            );
        }

        if rows.is_empty() {
            return Ok(());
        }

        debug_log!(
            "[BatchIndexBuilder]   📊 Table '{}': {} rows",
            table_name,
            rows.len()
        );

        let rows = Arc::new(rows);

        // 🔑 顺序构建所有索引类型（不再 spawn 子线程）。
        // 旧代码 spawn 4 个子线程并 join，每个 clone_for_callback() 持 Database Arc，
        // 子线程的 insert_batch 持索引写锁。这导致 close/checkpoint 时间歇死锁：
        //   - 子线程持索引锁 + join 等子线程 → index-builder 主线程不退出
        //   - close/checkpoint 的索引操作等同一把锁 → 死锁
        // 改成顺序调用后，index-builder 单线程跑完所有构建，无游离子线程，
        // close 时 join index-builder 即可保证不持任何锁。
        // 索引构建本身是批量操作，顺序 vs 并行的差异在典型批量下可忽略。
        if let Err(e) = self.batch_build_column_indexes(table_name, &schema, &rows) {
            debug_log!("[BatchIndexBuilder] ⚠️  Column index build failed: {}", e);
            return Err(e);
        }
        if let Err(e) = self.batch_build_timestamp_indexes(&schema, &rows) {
            debug_log!(
                "[BatchIndexBuilder] ⚠️  Timestamp index build failed: {}",
                e
            );
            return Err(e);
        }
        if let Err(e) = self.batch_build_vector_indexes(table_name, &schema, &rows) {
            debug_log!("[BatchIndexBuilder] ⚠️  Vector index build failed: {}", e);
            return Err(e);
        }
        if let Err(e) = self.batch_build_text_indexes(table_name, &schema, &rows) {
            debug_log!("[BatchIndexBuilder] ⚠️  Text index build failed: {}", e);
            return Err(e);
        }

        debug_log!(
            "[BatchIndexBuilder]   ✓ Table '{}' indexes built in {:?}",
            table_name,
            _start.elapsed()
        );
        Ok(())
    }

    /// Batch build column indexes
    fn batch_build_column_indexes(
        &self,
        table_name: &str,
        schema: &TableSchema,
        rows: &[(RowId, Row)],
    ) -> Result<()> {
        use std::time::Instant;
        let start = Instant::now();

        let indexes_with_data: Vec<_> = {
            schema
                .columns
                .iter()
                .filter_map(|col_def| {
                    let index_name = format!("{}.{}", table_name, col_def.name);
                    self.column_indexes.get(&index_name).and_then(|index_ref| {
                        let index = index_ref.value();
                        // Skip if index is already up-to-date from synchronous path
                        if !index.needs_rebuild() {
                            return None;
                        }
                        let mut batch: Vec<(RowId, Value)> = Vec::with_capacity(rows.len());
                        for (row_id, row) in rows {
                            if let Some(value) = row.get(col_def.position) {
                                batch.push((*row_id, value.clone()));
                            }
                        }
                        Some((index.clone(), col_def.name.clone(), batch))
                    })
                })
                .collect()
        };

        for (index, _col_name, batch) in indexes_with_data {
            if !batch.is_empty() {
                let batch_refs: Vec<(RowId, &Value)> = batch
                    .iter()
                    .map(|(row_id, value)| (*row_id, value))
                    .collect();

                index.insert_batch(&batch_refs)?;
                index.mark_rebuilt();
                debug_log!(
                    "[ColumnIndex]   ✓ Built {} entries for column '{}'",
                    batch.len(),
                    _col_name
                );
            }
        }

        let _duration = start.elapsed();
        debug_log!("[ColumnIndex] Batch build complete in {:?}", _duration);

        Ok(())
    }

    /// Batch build timestamp indexes
    fn batch_build_timestamp_indexes(
        &self,
        schema: &TableSchema,
        rows: &[(RowId, Row)],
    ) -> Result<()> {
        use std::time::Instant;
        let start = Instant::now();

        let ts_col = match schema
            .columns
            .iter()
            .find(|c| c.col_type == crate::types::ColumnType::Timestamp)
        {
            Some(col) => col,
            None => return Ok(()),
        };

        let mut ts_index = self.timestamp_index.write();
        let mut count = 0;

        for (row_id, row) in rows {
            if let Some(crate::types::Value::Timestamp(ts)) = row.get(ts_col.position) {
                ts_index.insert(ts.as_micros_u64(), *row_id)?;
                count += 1;
            }
        }

        if count > 0 {
            debug_log!(
                "[TimestampIndex] Batch built {} entries in {:?}",
                count,
                start.elapsed()
            );
        }

        Ok(())
    }

    /// Batch build vector indexes
    fn batch_build_vector_indexes(
        &self,
        table_name: &str,
        schema: &TableSchema,
        rows: &[(RowId, Row)],
    ) -> Result<()> {
        for col_def in &schema.columns {
            if let crate::types::ColumnType::Tensor(_dim) = col_def.col_type {
                // Look up actual index name from registry (supports custom names)
                let index_name = match self.index_registry.find_by_column(
                    table_name,
                    &col_def.name,
                    crate::database::index_metadata::IndexType::Vector,
                ) {
                    Some(name) => name,
                    None => continue,
                };
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
    fn batch_build_text_indexes(
        &self,
        table_name: &str,
        schema: &TableSchema,
        rows: &[(RowId, Row)],
    ) -> Result<()> {
        use crate::index::builder::IndexBuilder;

        for col_def in &schema.columns {
            if matches!(col_def.col_type, crate::types::ColumnType::Text) {
                // Look up actual index name from registry (supports custom names)
                let index_name = match self.index_registry.find_by_column(
                    table_name,
                    &col_def.name,
                    crate::database::index_metadata::IndexType::Text,
                ) {
                    Some(name) => name,
                    None => continue,
                };
                if let Some(index_ref) = self.text_indexes.get(&index_name) {
                    let index = index_ref.value();
                    let mut index_guard = index.write();
                    // Filter rows to only include the target column's text value
                    let col_pos = col_def.position;
                    let filtered: Vec<(RowId, Vec<Value>)> = rows
                        .iter()
                        .filter_map(|(row_id, row)| {
                            row.get(col_pos).and_then(|v| match v {
                                Value::Text(t) => Some((*row_id, vec![Value::text(t.to_string())])),
                                Value::TextDoc(t) => {
                                    Some((*row_id, vec![Value::text(t.content().to_string())]))
                                }
                                _ => None,
                            })
                        })
                        .collect();
                    if !filtered.is_empty() {
                        index_guard.build_from_memtable(&filtered)?;
                    }
                }
            }
        }

        Ok(())
    }
}
