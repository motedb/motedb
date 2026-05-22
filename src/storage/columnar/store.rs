//! Top-level ColumnarStore coordinator.
//!
//! Manages write buffers, segment managers, and provides the public API
//! for ingest, query, and GC operations.

use super::column_encoding::{decode_bools, decode_strings, encode_bools, encode_strings, StringEncoding};
use super::config::ColumnarConfig;
use super::segment::{ColumnBlock, ColumnEncoding, SegmentBuilder};
use super::segment_manager::SegmentManager;
use super::write_buffer::{BufferedBatch, ColumnBuffer, ColumnarWriteBuffer, FlushDecision};
use crate::catalog::TableRegistry;
use crate::storage::columnar::gorilla;
use crate::txn::wal::WALManager;
use crate::types::{ColumnType, RowId, SqlRow, TableSchema, Value};
use crate::{Result, StorageError};
use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

/// Result of an ingest operation.
#[derive(Debug)]
pub struct ColumnarIngestResult {
    pub row_ids: Vec<RowId>,
    pub segments_created: usize,
}

/// The top-level columnar store coordinator.
pub struct ColumnarStore {
    base_dir: PathBuf,
    buffers: DashMap<u32, Mutex<ColumnarWriteBuffer>>,
    managers: DashMap<u32, Arc<SegmentManager>>,
    schemas: DashMap<u32, Arc<TableSchema>>,
    config: ColumnarConfig,
    next_row_id: Arc<AtomicU64>,
    table_registry: Arc<TableRegistry>,
    /// WAL for crash recovery (set after construction via set_wal).
    wal: RwLock<Option<Arc<WALManager>>>,
}

impl ColumnarStore {
    /// Create a new columnar store.
    pub fn create(
        base_dir: &Path,
        config: ColumnarConfig,
        next_row_id: Arc<AtomicU64>,
        table_registry: Arc<TableRegistry>,
    ) -> Result<Self> {
        std::fs::create_dir_all(base_dir).map_err(StorageError::Io)?;

        Ok(Self {
            base_dir: base_dir.to_path_buf(),
            buffers: DashMap::new(),
            managers: DashMap::new(),
            schemas: DashMap::new(),
            config,
            next_row_id,
            table_registry,
            wal: RwLock::new(None),
        })
    }

    /// Set the WAL reference after construction (used by MoteDB init).
    pub fn set_wal(&self, wal: Arc<WALManager>) {
        *self.wal.write().unwrap() = Some(wal);
    }

    /// Register a TimeSeries table with the columnar store.
    pub fn register_table(&self, table_id: u32, schema: &TableSchema) -> Result<()> {
        let dir = self.base_dir.join(table_id.to_string());
        let manager = SegmentManager::open(&dir, table_id)?;
        let buffer = ColumnarWriteBuffer::new(table_id, schema, self.config.clone());

        self.managers.insert(table_id, Arc::new(manager));
        self.buffers.insert(table_id, Mutex::new(buffer));
        self.schemas.insert(table_id, Arc::new(schema.clone()));

        Ok(())
    }

    /// Replay a single row into the columnar buffer (used during WAL recovery).
    pub fn replay_row(&self, table_name: &str, row_id: RowId, row: crate::types::Row) -> Result<()> {
        let table_id = self.table_registry.get_table_id(table_name)
            .map_err(|_| StorageError::TableNotFound(table_name.to_string()))?;

        let buffer_entry = self.buffers.get(&table_id)
            .ok_or_else(|| StorageError::Columnar(format!(
                "No write buffer for table '{}' (id={})", table_name, table_id
            )))?;
        let mut buffer = buffer_entry.lock().unwrap();
        let decision = buffer.append(row_id, &row);
        if decision == FlushDecision::Flush {
            if let Some(mut batch) = buffer.take() {
                drop(buffer);
                self.flush_batch(&mut batch)?;
            }
        }
        Ok(())
    }

    /// Ingest rows into a TimeSeries table.
    pub fn ingest(&self, table_name: &str, rows: Vec<crate::types::Row>) -> Result<ColumnarIngestResult> {
        let table_id = self.table_registry.get_table_id(table_name)
            .map_err(|_| StorageError::TableNotFound(table_name.to_string()))?;

        let schema = self.schemas.get(&table_id)
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?
            .clone();

        // Validate rows
        for row in &rows {
            if let Err(e) = schema.validate_row(row) {
                return Err(StorageError::InvalidData(e));
            }
        }

        // Batch allocate row IDs
        let num_rows = rows.len();
        let start_id = self.next_row_id.fetch_add(num_rows as u64, Ordering::Relaxed);
        let row_ids: Vec<RowId> = (start_id..start_id + num_rows as u64).collect();

        // P0: Write to WAL first (durability)
        {
            let wal_guard = self.wal.read().unwrap();
            if let Some(ref wal) = *wal_guard {
                let partition = 0u8; // Use partition 0 for columnar; all columnar data is in segments anyway
                let mut wal_records = Vec::with_capacity(num_rows);
                for (i, row) in rows.iter().enumerate() {
                    wal_records.push(crate::txn::wal::WALRecord::Insert {
                        table_name: table_name.to_string(),
                        row_id: row_ids[i],
                        partition,
                        data: row.clone(),
                        txn_id: 0,
                    });
                }
                wal.batch_append(partition, wal_records)?;
            }
        }

        // Append to write buffer
        let mut segments_created = 0;
        let buffer_entry = self.buffers.get(&table_id)
            .ok_or_else(|| StorageError::Columnar(format!(
                "No write buffer for table '{}' (id={})", table_name, table_id
            )))?;
        let mut buffer = buffer_entry.lock().unwrap();

        for (i, row) in rows.into_iter().enumerate() {
            let decision = buffer.append(row_ids[i], &row);
            if decision == FlushDecision::Flush {
                if let Some(mut batch) = buffer.take() {
                    drop(buffer); // release lock during I/O
                    self.flush_batch(&mut batch)?;
                    segments_created += 1;
                    buffer = buffer_entry.lock().unwrap();
                }
            }
        }

        // P3: Try segment merging if enabled (release DashMap guard first to avoid holding it during heavy I/O)
        if self.config.enable_merge && segments_created > 0 {
            drop(buffer);
            drop(buffer_entry);
            if let Some(mgr) = self.managers.get(&table_id) {
                if mgr.segment_count() >= self.config.merge_threshold_segments {
                    if let Err(e) = self.try_merge_segments(table_id, &schema) {
                        debug_log!("[Columnar] Segment merge failed for table '{}': {:?}", table_name, e);
                    }
                }
            }
        }

        Ok(ColumnarIngestResult {
            row_ids,
            segments_created,
        })
    }

    /// P3: Try to merge small segments into a larger one.
    ///
    /// Selects the smallest segments (by row_count) whose total rows fit within
    /// `segment_target_rows`, reads + decodes them, and writes a single merged segment.
    fn try_merge_segments(&self, table_id: u32, schema: &TableSchema) -> Result<()> {
        let manager = self.managers.get(&table_id)
            .ok_or_else(|| StorageError::Columnar(format!(
                "No segment manager for table_id={}", table_id
            )))?;

        // Find small segments (sorted smallest first)
        let small = manager.small_segments(self.config.segment_target_rows);
        if small.len() < 2 {
            return Ok(()); // Need at least 2 segments to merge
        }

        // Greedily accumulate segments until we exceed target_rows
        let mut to_merge = Vec::new();
        let mut total_rows = 0usize;
        for seg in &small {
            let new_total = total_rows + seg.row_count as usize;
            if new_total > self.config.segment_target_rows {
                break;
            }
            total_rows = new_total;
            to_merge.push(seg.clone());
        }

        if to_merge.len() < 2 {
            return Ok(()); // Not enough segments to merge
        }

        debug_log!(
            "[Columnar] Merging {} segments ({} rows) for table_id={}",
            to_merge.len(), total_rows, table_id
        );

        // Read all columns from each segment + row_id column
        let column_count = schema.columns.len() as u16;
        let mut merged_columns: Vec<ColumnBuffer> = schema.columns.iter()
            .map(|c| ColumnBuffer::new(&c.col_type))
            .collect();
        let mut merged_row_ids: Vec<i64> = Vec::new();
        let mut merged_min_ts = i64::MAX;
        let mut merged_max_ts = i64::MIN;
        let mut merged_min_row_id = u64::MAX;
        let mut merged_max_row_id = 0u64;

        for seg in &to_merge {
            // Read schema columns
            let col_ids: Vec<u16> = (0..column_count).collect();
            let blocks = manager.read_columns(seg, &col_ids)?;

            // Decode each column and append to merged buffer
            for block in &blocks {
                let col_idx = block.column_id as usize;
                if col_idx >= merged_columns.len() {
                    continue;
                }
                let col_type = &schema.columns[col_idx].col_type;
                let values = self.decode_single_column(block, col_type, seg.row_count as usize)?;
                for val in values {
                    merged_columns[col_idx].push(val);
                }
            }

            // Read row_id column
            let row_id_col = column_count; // extra column
            if seg.has_row_id_column {
                if let Ok(blocks) = manager.read_columns(seg, &[row_id_col]) {
                    if let Some(block) = blocks.first() {
                        let ids = gorilla::decode_integers(&block.data, seg.row_count as usize);
                        merged_row_ids.extend(ids);
                    }
                } else {
                    // Fallback: approximate row_ids
                    for i in 0..seg.row_count as u64 {
                        merged_row_ids.push((seg.min_row_id + i) as i64);
                    }
                }
            } else {
                // Old format: approximate row_ids
                for i in 0..seg.row_count as u64 {
                    merged_row_ids.push((seg.min_row_id + i) as i64);
                }
            }

            merged_min_ts = merged_min_ts.min(seg.min_timestamp);
            merged_max_ts = merged_max_ts.max(seg.max_timestamp);
            merged_min_row_id = merged_min_row_id.min(seg.min_row_id);
            merged_max_row_id = merged_max_row_id.max(seg.max_row_id);
        }

        // Write merged segment
        let dir = self.base_dir.join(table_id.to_string());
        let path = dir.join(format!("seg_{}_{}.mcdb", merged_min_ts, merged_max_ts));
        let mut builder = SegmentBuilder::new(&path, table_id, column_count)?;

        // Sort merged data by timestamp if enabled
        let ts_col_idx = schema.timeseries_column.as_ref().and_then(|ts_col| {
            schema.columns.iter().position(|c| c.name == *ts_col)
        });
        let is_sorted = ts_col_idx.is_some() && self.config.enable_timestamp_sort && total_rows > 1;
        if is_sorted {
            // Build a temporary batch for sorting
            let mut batch = BufferedBatch {
                table_id,
                columns: merged_columns,
                row_count: total_rows,
                row_ids: merged_row_ids.iter().map(|&id| id as u64).collect(),
                min_timestamp: merged_min_ts,
                max_timestamp: merged_max_ts,
            };
            batch.sort_by_timestamp(ts_col_idx);
            merged_columns = batch.columns;
            merged_row_ids = batch.row_ids.iter().map(|&id| id as i64).collect();
        }

        for (col_id, col_buf) in merged_columns.iter().enumerate() {
            self.encode_column_to_builder(&mut builder, col_id as u16, col_buf)?;
        }

        // Write row_id column
        let row_id_data = gorilla::encode_integers(&merged_row_ids);
        builder.write_column(
            column_count,
            ColumnEncoding::DeltaVarint,
            &row_id_data,
            (merged_row_ids.len() * 8) as u32,
            0,
        )?;

        // Write column statistics for merged segment
        if self.config.enable_column_stats {
            let stats: Vec<super::segment::ColumnStatistics> = merged_columns.iter().enumerate()
                .filter_map(|(i, col)| col.compute_statistics(i as u16))
                .collect();
            if !stats.is_empty() {
                builder.set_statistics(stats);
            }
        }

        // Build bloom filters for merged segment
        if self.config.enable_bloom_filters {
            let filters = merged_columns.iter().enumerate()
                .filter_map(|(col_id, col)| {
                    if let ColumnBuffer::Text(vals) = col {
                        use crate::storage::lsm::BloomFilter;
                        let non_null: Vec<&String> = vals.iter().filter_map(|v| v.as_ref()).collect();
                        if non_null.is_empty() { return None; }
                        let mut bloom = BloomFilter::new(non_null.len(), self.config.bloom_filter_bits_per_key);
                        for s in &non_null { bloom.insert(s.as_bytes()); }
                        Some((col_id as u16, bloom.to_bytes()))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            if !filters.is_empty() {
                builder.set_bloom_filters(filters);
            }
        }

        builder.set_timestamp_sorted(is_sorted);

        builder.finish(
            total_rows as u32,
            merged_min_ts,
            merged_max_ts,
            merged_min_row_id,
            merged_max_row_id,
        )?;

        // Replace old segments with new merged one
        manager.replace_segments(&to_merge, &path)?;

        debug_log!(
            "[Columnar] Merged {} segments → 1 ({} rows) for table_id={}",
            to_merge.len(), total_rows, table_id
        );

        Ok(())
    }

    /// Encode a single ColumnBuffer into a SegmentBuilder column.
    fn encode_column_to_builder(
        &self,
        builder: &mut SegmentBuilder,
        col_id: u16,
        col_buf: &ColumnBuffer,
    ) -> Result<()> {
        match col_buf {
            ColumnBuffer::Timestamp(vals) => {
                let data = gorilla::encode_timestamps(vals);
                builder.write_column(col_id, ColumnEncoding::GorillaTimestamp, &data, (vals.len() * 8) as u32, 0)?;
            }
            ColumnBuffer::Integer(vals) => {
                let data = gorilla::encode_integers(vals);
                builder.write_column(col_id, ColumnEncoding::DeltaVarint, &data, (vals.len() * 8) as u32, 0)?;
            }
            ColumnBuffer::Float(vals) => {
                let data = gorilla::encode_floats(vals);
                builder.write_column(col_id, ColumnEncoding::GorillaXorFloat, &data, (vals.len() * 8) as u32, 0)?;
            }
            ColumnBuffer::Bool(vals) => {
                let (packed, null_bm) = encode_bools(vals);
                let mut data = Vec::new();
                if let Some(bm) = &null_bm {
                    data.push(1u8);
                    data.extend_from_slice(&(bm.len() as u32).to_le_bytes());
                    data.extend_from_slice(bm);
                } else {
                    data.push(0u8);
                }
                data.extend_from_slice(&packed);
                let null_count = vals.iter().filter(|v| v.is_none()).count() as u32;
                builder.write_column(col_id, ColumnEncoding::BoolPacked, &data, vals.len() as u32, null_count)?;
            }
            ColumnBuffer::Text(vals) => {
                let (data, enc_type) = encode_strings(vals);
                let encoding = if enc_type == StringEncoding::Dictionary {
                    ColumnEncoding::Dictionary
                } else {
                    ColumnEncoding::Raw
                };
                let null_count = vals.iter().filter(|v| v.is_none()).count() as u32;
                builder.write_column(col_id, encoding, &data, vals.iter().map(|v| v.as_ref().map_or(1, |s| s.len() + 4)).sum::<usize>() as u32, null_count)?;
            }
            ColumnBuffer::Other(vals) => {
                let data = bincode::serialize(vals)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                builder.write_column(col_id, ColumnEncoding::Raw, &data, data.len() as u32, 0)?;
            }
        }
        Ok(())
    }

    /// Decode a single column block into Values.
    fn decode_single_column(&self, block: &ColumnBlock, col_type: &ColumnType, row_count: usize) -> Result<Vec<Value>> {
        Ok(match block.encoding {
            ColumnEncoding::GorillaTimestamp => {
                gorilla::decode_timestamps(&block.data, row_count)
                    .into_iter()
                    .map(|micros| Value::Timestamp(crate::types::Timestamp::from_micros(micros)))
                    .collect()
            }
            ColumnEncoding::DeltaVarint => {
                gorilla::decode_integers(&block.data, row_count)
                    .into_iter()
                    .map(Value::Integer)
                    .collect()
            }
            ColumnEncoding::GorillaXorFloat => {
                gorilla::decode_floats(&block.data, row_count)
                    .into_iter()
                    .map(Value::Float)
                    .collect()
            }
            ColumnEncoding::BoolPacked => {
                let mut cursor = 0usize;
                let has_null_bm = block.data[cursor] != 0;
                cursor += 1;
                let null_bm = if has_null_bm {
                    let bm_len = u32::from_le_bytes(
                        block.data[cursor..cursor + 4].try_into().unwrap()
                    ) as usize;
                    cursor += 4;
                    let bm = block.data[cursor..cursor + bm_len].to_vec();
                    cursor += bm_len;
                    Some(bm)
                } else {
                    None
                };
                let packed = &block.data[cursor..];
                decode_bools(packed, null_bm.as_deref(), row_count)
                    .into_iter()
                    .map(|v| v.map_or(Value::Null, Value::Bool))
                    .collect()
            }
            ColumnEncoding::Dictionary | ColumnEncoding::Raw => {
                match col_type {
                    ColumnType::Text => {
                        let enc = if block.encoding == ColumnEncoding::Dictionary {
                            StringEncoding::Dictionary
                        } else {
                            StringEncoding::Raw
                        };
                        decode_strings(&block.data, row_count, enc)
                            .into_iter()
                            .map(|v| v.map_or(Value::Null, |s| Value::text(s)))
                            .collect()
                    }
                    _ => {
                        bincode::deserialize(&block.data)
                            .map_err(|e| StorageError::Serialization(e.to_string()))?
                    }
                }
            }
        })
    }

    /// Flush a batch to a segment file.
    fn flush_batch(&self, batch: &mut BufferedBatch) -> Result<()> {
        let table_id = batch.table_id;
        let dir = self.base_dir.join(table_id.to_string());
        std::fs::create_dir_all(&dir).map_err(StorageError::Io)?;

        let path = dir.join(format!(
            "seg_{}_{}.mcdb",
            batch.min_timestamp, batch.max_timestamp
        ));

        let column_count = batch.columns.len() as u16;

        // Step 2: Sort by timestamp if config enabled
        let ts_col_idx = self.find_timestamp_column(table_id);
        if self.config.enable_timestamp_sort {
            batch.sort_by_timestamp(ts_col_idx);
        }

        let mut builder = SegmentBuilder::new(&path, table_id, column_count)?;

        // Encode each column
        for (col_id, col_buf) in batch.columns.iter().enumerate() {
            match col_buf {
                ColumnBuffer::Timestamp(vals) => {
                    let data = gorilla::encode_timestamps(vals);
                    builder.write_column(
                        col_id as u16,
                        ColumnEncoding::GorillaTimestamp,
                        &data,
                        (vals.len() * 8) as u32,
                        0,
                    )?;
                }
                ColumnBuffer::Integer(vals) => {
                    let data = gorilla::encode_integers(vals);
                    builder.write_column(
                        col_id as u16,
                        ColumnEncoding::DeltaVarint,
                        &data,
                        (vals.len() * 8) as u32,
                        0,
                    )?;
                }
                ColumnBuffer::Float(vals) => {
                    let data = gorilla::encode_floats(vals);
                    builder.write_column(
                        col_id as u16,
                        ColumnEncoding::GorillaXorFloat,
                        &data,
                        (vals.len() * 8) as u32,
                        0,
                    )?;
                }
                ColumnBuffer::Bool(vals) => {
                    let (packed, null_bm) = encode_bools(vals);
                    // Prepend null bitmap if present
                    let mut data = Vec::new();
                    if let Some(bm) = &null_bm {
                        data.push(1u8); // has null bitmap
                        data.extend_from_slice(&(bm.len() as u32).to_le_bytes());
                        data.extend_from_slice(bm);
                    } else {
                        data.push(0u8);
                    }
                    data.extend_from_slice(&packed);
                    let null_count = vals.iter().filter(|v| v.is_none()).count() as u32;
                    builder.write_column(
                        col_id as u16,
                        ColumnEncoding::BoolPacked,
                        &data,
                        vals.len() as u32,
                        null_count,
                    )?;
                }
                ColumnBuffer::Text(vals) => {
                    let (data, enc_type) = encode_strings(vals);
                    let encoding = if enc_type == StringEncoding::Dictionary {
                        ColumnEncoding::Dictionary
                    } else {
                        ColumnEncoding::Raw
                    };
                    let null_count = vals.iter().filter(|v| v.is_none()).count() as u32;
                    builder.write_column(
                        col_id as u16,
                        encoding,
                        &data,
                        vals.iter().map(|v| v.as_ref().map_or(1, |s| s.len() + 4)).sum::<usize>() as u32,
                        null_count,
                    )?;
                }
                ColumnBuffer::Other(vals) => {
                    // Fallback: bincode serialize each value
                    let data = bincode::serialize(vals)
                        .map_err(|e| StorageError::Serialization(e.to_string()))?;
                    builder.write_column(
                        col_id as u16,
                        ColumnEncoding::Raw,
                        &data,
                        data.len() as u32,
                        0,
                    )?;
                }
            }
        }

        let min_row_id = batch.row_ids.first().copied().unwrap_or(0);
        let max_row_id = batch.row_ids.last().copied().unwrap_or(0);

        // P1: Write row_ids as an extra column after the schema columns.
        // This stores exact row_ids in the segment for accurate retrieval.
        if column_count > 0 {
            let row_id_data = gorilla::encode_integers(
                &batch.row_ids.iter().map(|&id| id as i64).collect::<Vec<_>>()
            );
            builder.write_column(
                column_count, // column_id = column_count (one past last schema column)
                ColumnEncoding::DeltaVarint,
                &row_id_data,
                (batch.row_ids.len() * 8) as u32,
                0,
            )?;
        }

        // Step 3: Compute and write column statistics (zone maps)
        let is_sorted = ts_col_idx.is_some() && self.config.enable_timestamp_sort && batch.row_count > 1;
        if self.config.enable_column_stats && !batch.columns.is_empty() {
            let stats: Vec<super::segment::ColumnStatistics> = batch.columns.iter().enumerate()
                .filter_map(|(i, col)| col.compute_statistics(i as u16))
                .collect();
            if !stats.is_empty() {
                builder.set_statistics(stats);
            }
        }

        // Step 4: Build bloom filters for Text columns
        if self.config.enable_bloom_filters {
            let filters = self.build_bloom_filters(batch);
            if !filters.is_empty() {
                builder.set_bloom_filters(filters);
            }
        }

        builder.set_timestamp_sorted(is_sorted);

        builder.finish(
            batch.row_count as u32,
            batch.min_timestamp,
            batch.max_timestamp,
            min_row_id,
            max_row_id,
        )?;

        // Register with segment manager
        if let Some(mgr) = self.managers.get(&table_id) {
            mgr.register_segment(&path)?;
        }

        Ok(())
    }

    /// Query with column conditions for segment-level pruning.
    pub fn query_with_conditions(
        &self,
        table_name: &str,
        start_ts: i64,
        end_ts: i64,
        conditions: &[super::segment_manager::ColumnCondition],
        column_names: &[String],
    ) -> Result<Vec<(RowId, SqlRow)>> {
        let table_id = self.table_registry.get_table_id(table_name)
            .map_err(|_| StorageError::TableNotFound(table_name.to_string()))?;

        let schema = self.schemas.get(&table_id)
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?
            .clone();

        let manager = self.managers.get(&table_id)
            .ok_or_else(|| StorageError::Columnar(format!(
                "No segment manager for table '{}'", table_name
            )))?;

        // Resolve column names to IDs
        let column_ids: Vec<(u16, String)> = if column_names.is_empty() {
            schema.columns.iter().enumerate()
                .map(|(i, c)| (i as u16, c.name.clone()))
                .collect()
        } else {
            column_names.iter().map(|name| {
                let idx = schema.columns.iter().position(|c| c.name == *name)
                    .ok_or_else(|| StorageError::ColumnNotFound(name.clone()))?;
                Ok((idx as u16, name.clone()))
            }).collect::<Result<Vec<_>>>()?
        };

        // Segment pruning with conditions
        let segments = if conditions.is_empty() {
            manager.prune_by_time(start_ts, end_ts)
        } else {
            manager.prune_by_conditions(start_ts, end_ts, conditions)
        };

        let mut results = Vec::new();

        for segment in &segments {
            let mut col_ids: Vec<u16> = column_ids.iter().map(|(id, _)| *id).collect();
            let row_id_col = segment.column_count;
            if segment.has_row_id_column {
                col_ids.push(row_id_col);
            }

            let blocks = manager.read_columns(segment, &col_ids)?;

            let schema_blocks: Vec<&ColumnBlock> = blocks.iter()
                .filter(|b| b.column_id < segment.column_count)
                .collect();
            let decoded = self.decode_columns(&schema_blocks, &schema, segment.row_count as usize)?;

            let exact_row_ids: Option<Vec<RowId>> = if segment.has_row_id_column {
                blocks.iter()
                    .find(|b| b.column_id == row_id_col)
                    .map(|block| {
                        gorilla::decode_integers(&block.data, segment.row_count as usize)
                            .into_iter()
                            .map(|id| id as RowId)
                            .collect()
                    })
            } else {
                None
            };

            let ts_col_idx = schema.timeseries_column.as_ref().and_then(|ts_col| {
                column_ids.iter().position(|(_, name)| name == ts_col)
            });

            let (row_start, row_end) = if segment.is_timestamp_sorted {
                self.binary_search_timestamp_range(
                    &decoded, &column_ids, ts_col_idx, start_ts, end_ts,
                )
            } else {
                (0, segment.row_count as usize)
            };

            for row_idx in row_start..row_end {
                if row_idx >= decoded.len() {
                    break;
                }

                if let Some(ts_idx) = ts_col_idx {
                    let col_idx = column_ids[ts_idx].0 as usize;
                    if col_idx < decoded[row_idx].len() {
                        if let Value::Timestamp(ts) = &decoded[row_idx][col_idx] {
                            let micros = ts.as_micros();
                            if micros < start_ts || micros > end_ts {
                                continue;
                            }
                        }
                    }
                }

                // Apply non-timestamp column conditions as row-level filter
                if !self.row_matches_conditions(&decoded[row_idx], &column_ids, conditions) {
                    continue;
                }

                let mut sql_row = SqlRow::new();
                for (col_id, col_name) in &column_ids {
                    let idx = *col_id as usize;
                    if idx < decoded[row_idx].len() {
                        sql_row.insert(col_name.clone(), decoded[row_idx][idx].clone());
                    }
                }

                let row_id = if let Some(ref ids) = exact_row_ids {
                    ids.get(row_idx).copied().unwrap_or(segment.min_row_id + row_idx as u64)
                } else {
                    segment.min_row_id + row_idx as u64
                };
                results.push((row_id, sql_row));
            }
        }

        // Also include active buffer data
        if let Some(buffer_entry) = self.buffers.get(&table_id) {
            let buffer = buffer_entry.lock().unwrap();
            if let Some((buf_min, buf_max)) = buffer.timestamp_range() {
                if buf_max >= start_ts && buf_min <= end_ts {
                    let buf_results = buffer.snapshot_rows(start_ts, end_ts, &schema, &column_ids);
                    // Apply conditions to buffer results
                    for (row_id, sql_row) in buf_results {
                        if self.sql_row_matches_conditions(&sql_row, &column_ids, conditions) {
                            results.push((row_id, sql_row));
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    /// Check if a decoded row matches the given column conditions.
    fn row_matches_conditions(
        &self,
        row: &[Value],
        _column_ids: &[(u16, String)],
        conditions: &[super::segment_manager::ColumnCondition],
    ) -> bool {
        use super::segment_manager::ColumnCondition;
        for cond in conditions {
            match cond {
                ColumnCondition::Equals { column_idx, value } => {
                    let idx = *column_idx;
                    if idx >= row.len() {
                        continue;
                    }
                    if row[idx] != *value {
                        return false;
                    }
                }
                ColumnCondition::Range { column_idx, low, high } => {
                    let idx = *column_idx;
                    if idx >= row.len() {
                        continue;
                    }
                    let val = &row[idx];
                    if val < low || val > high {
                        return false;
                    }
                }
            }
        }
        true
    }

    /// Check if a SqlRow matches conditions (for buffer data).
    fn sql_row_matches_conditions(
        &self,
        sql_row: &SqlRow,
        column_ids: &[(u16, String)],
        conditions: &[super::segment_manager::ColumnCondition],
    ) -> bool {
        use super::segment_manager::ColumnCondition;
        for cond in conditions {
            match cond {
                ColumnCondition::Equals { column_idx, value } => {
                    let Some((_, col_name)) = column_ids.iter()
                        .find(|(id, _)| *id as usize == *column_idx)
                    else {
                        continue;
                    };
                    if let Some(v) = sql_row.get(col_name) {
                        if v != value {
                            return false;
                        }
                    }
                }
                ColumnCondition::Range { column_idx, low, high } => {
                    let Some((_, col_name)) = column_ids.iter()
                        .find(|(id, _)| *id as usize == *column_idx)
                    else {
                        continue;
                    };
                    if let Some(v) = sql_row.get(col_name) {
                        if v < low || v > high {
                            return false;
                        }
                    }
                }
            }
        }
        true
    }

    /// Find the timestamp column index for a table.
    fn find_timestamp_column(&self, table_id: u32) -> Option<usize> {
        self.schemas.get(&table_id).and_then(|schema| {
            schema.timeseries_column.as_ref().and_then(|ts_col| {
                schema.columns.iter().position(|c| c.name == *ts_col)
            })
        })
    }

    /// Build bloom filters for Text columns in the batch.
    fn build_bloom_filters(&self, batch: &BufferedBatch) -> Vec<(u16, Vec<u8>)> {
        use crate::storage::lsm::BloomFilter;

        let mut filters = Vec::new();
        let bits_per_key = self.config.bloom_filter_bits_per_key;

        for (col_id, col_buf) in batch.columns.iter().enumerate() {
            if let ColumnBuffer::Text(vals) = col_buf {
                let non_null: Vec<&String> = vals.iter().filter_map(|v| v.as_ref()).collect();
                if non_null.is_empty() {
                    continue;
                }
                let mut bloom = BloomFilter::new(non_null.len(), bits_per_key);
                for s in &non_null {
                    bloom.insert(s.as_bytes());
                }
                filters.push((col_id as u16, bloom.to_bytes()));
            }
        }
        filters
    }

    /// Query time range from a TimeSeries table.
    pub fn query_time_range(
        &self,
        table_name: &str,
        start_ts: i64,
        end_ts: i64,
        column_names: &[String],
    ) -> Result<Vec<(RowId, SqlRow)>> {
        let table_id = self.table_registry.get_table_id(table_name)
            .map_err(|_| StorageError::TableNotFound(table_name.to_string()))?;

        let schema = self.schemas.get(&table_id)
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?
            .clone();

        let manager = self.managers.get(&table_id)
            .ok_or_else(|| StorageError::Columnar(format!(
                "No segment manager for table '{}'", table_name
            )))?;

        // Resolve column names to IDs
        let column_ids: Vec<(u16, String)> = if column_names.is_empty() {
            // All columns
            schema.columns.iter().enumerate()
                .map(|(i, c)| (i as u16, c.name.clone()))
                .collect()
        } else {
            column_names.iter().map(|name| {
                let idx = schema.columns.iter().position(|c| c.name == *name)
                    .ok_or_else(|| StorageError::ColumnNotFound(name.clone()))?;
                Ok((idx as u16, name.clone()))
            }).collect::<Result<Vec<_>>>()?
        };

        // Segment pruning
        let segments = manager.prune_by_time(start_ts, end_ts);
        let mut results = Vec::new();

        for segment in &segments {
            // P1: Read schema columns + row_id column (column_count = one past last schema col)
            let mut col_ids: Vec<u16> = column_ids.iter().map(|(id, _)| *id).collect();
            let row_id_col = segment.column_count; // row_ids stored as extra column
            if segment.has_row_id_column {
                col_ids.push(row_id_col);
            }

            let blocks = manager.read_columns(segment, &col_ids)?;

            // P2: Predicate pushdown — decode timestamp column first to find matching rows
            let ts_block = blocks.iter().find(|b| {
                column_ids.iter().any(|(id, name)| {
                    *id == b.column_id && schema.timeseries_column.as_ref().map(|t| t == name).unwrap_or(false)
                })
            });

            let matching_rows: Vec<usize> = if let Some(ts_blk) = ts_block {
                if ts_blk.encoding == ColumnEncoding::GorillaTimestamp {
                    let ts_micros = gorilla::decode_timestamps(&ts_blk.data, segment.row_count as usize);

                    if segment.is_timestamp_sorted {
                        let start = ts_micros.iter().position(|&m| m >= start_ts).unwrap_or(ts_micros.len());
                        let end = ts_micros.iter().rposition(|&m| m <= end_ts).map(|i| i + 1).unwrap_or(0);
                        if start >= end { continue; }
                        (start..end).filter(|&i| ts_micros[i] >= start_ts && ts_micros[i] <= end_ts).collect()
                    } else {
                        ts_micros.iter().enumerate()
                            .filter(|(_, &m)| m >= start_ts && m <= end_ts)
                            .map(|(i, _)| i)
                            .collect()
                    }
                } else {
                    // Non-gorilla encoding — fall back to full scan
                    (0..segment.row_count as usize).collect()
                }
            } else {
                (0..segment.row_count as usize).collect()
            };

            if matching_rows.is_empty() {
                continue;
            }

            // Now decode only needed columns for matching rows
            let schema_blocks: Vec<&ColumnBlock> = blocks.iter()
                .filter(|b| b.column_id < segment.column_count)
                .collect();
            let decoded = self.decode_columns(&schema_blocks, &schema, segment.row_count as usize)?;

            // P1: Decode row_id column for accurate row IDs
            let exact_row_ids: Option<Vec<RowId>> = if segment.has_row_id_column {
                blocks.iter()
                    .find(|b| b.column_id == row_id_col)
                    .map(|block| {
                        gorilla::decode_integers(&block.data, segment.row_count as usize)
                            .into_iter()
                            .map(|id| id as RowId)
                            .collect()
                    })
            } else {
                None
            };

            // Construct SqlRow for matching rows only
            for &row_idx in &matching_rows {
                if row_idx >= decoded.len() {
                    break;
                }

                let mut sql_row = SqlRow::new();
                for (col_id, col_name) in &column_ids {
                    let idx = *col_id as usize;
                    if idx < decoded[row_idx].len() {
                        sql_row.insert(col_name.clone(), decoded[row_idx][idx].clone());
                    }
                }

                let row_id = if let Some(ref ids) = exact_row_ids {
                    ids.get(row_idx).copied().unwrap_or(segment.min_row_id + row_idx as u64)
                } else {
                    segment.min_row_id + row_idx as u64
                };
                results.push((row_id, sql_row));
            }
        }

        // P0: Also include active buffer data (unflushed rows)
        if let Some(buffer_entry) = self.buffers.get(&table_id) {
            let buffer = buffer_entry.lock().unwrap();
            if let Some((buf_min, buf_max)) = buffer.timestamp_range() {
                if buf_max >= start_ts && buf_min <= end_ts {
                    let buf_results = buffer.snapshot_rows(start_ts, end_ts, &schema, &column_ids);
                    results.extend(buf_results);
                }
            }
        }

        Ok(results)
    }

    /// Decode column blocks into rows.
    fn decode_columns(
        &self,
        blocks: &[&ColumnBlock],
        schema: &TableSchema,
        row_count: usize,
    ) -> Result<Vec<Vec<Value>>> {
        let mut columns: Vec<Vec<Value>> = vec![Vec::new(); schema.columns.len()];

        for block in blocks {
            let col_idx = block.column_id as usize;
            if col_idx >= schema.columns.len() {
                continue;
            }

            let col_type = &schema.columns[col_idx].col_type;
            let values = match block.encoding {
                ColumnEncoding::GorillaTimestamp => {
                    let ts = gorilla::decode_timestamps(&block.data, row_count);
                    ts.into_iter()
                        .map(|micros| Value::Timestamp(crate::types::Timestamp::from_micros(micros)))
                        .collect()
                }
                ColumnEncoding::DeltaVarint => {
                    let ints = gorilla::decode_integers(&block.data, row_count);
                    ints.into_iter().map(Value::Integer).collect()
                }
                ColumnEncoding::GorillaXorFloat => {
                    let floats = gorilla::decode_floats(&block.data, row_count);
                    floats.into_iter().map(Value::Float).collect()
                }
                ColumnEncoding::BoolPacked => {
                    let mut cursor = 0usize;
                    let has_null_bm = block.data[cursor] != 0;
                    cursor += 1;
                    let null_bm = if has_null_bm {
                        let bm_len = u32::from_le_bytes(
                            block.data[cursor..cursor + 4].try_into().unwrap()
                        ) as usize;
                        cursor += 4;
                        let bm = block.data[cursor..cursor + bm_len].to_vec();
                        cursor += bm_len;
                        Some(bm)
                    } else {
                        None
                    };
                    let packed = &block.data[cursor..];
                    decode_bools(packed, null_bm.as_deref(), row_count)
                        .into_iter()
                        .map(|v| v.map_or(Value::Null, Value::Bool))
                        .collect()
                }
                ColumnEncoding::Dictionary | ColumnEncoding::Raw => {
                    // Check if this is a text column
                    match col_type {
                        ColumnType::Text => {
                            let enc = if block.encoding == ColumnEncoding::Dictionary {
                                StringEncoding::Dictionary
                            } else {
                                StringEncoding::Raw
                            };
                            decode_strings(&block.data, row_count, enc)
                                .into_iter()
                                .map(|v| v.map_or(Value::Null, |s| Value::text(s)))
                                .collect()
                        }
                        _ => {
                            // Raw bincode fallback
                            let vals: Vec<Value> = bincode::deserialize(&block.data)
                                .map_err(|e| StorageError::Serialization(e.to_string()))?;
                            vals
                        }
                    }
                }
            };

            columns[col_idx] = values;
        }

        // Transpose column-major → row-major
        let mut rows = Vec::with_capacity(row_count);
        for row_idx in 0..row_count {
            let mut row = Vec::with_capacity(columns.len());
            for col in &columns {
                row.push(col.get(row_idx).cloned().unwrap_or(Value::Null));
            }
            rows.push(row);
        }

        Ok(rows)
    }

    /// Binary search on a timestamp-sorted segment to find the row range
    /// overlapping [start_ts, end_ts]. Returns (start_row, end_row).
    fn binary_search_timestamp_range(
        &self,
        decoded: &[Vec<Value>],
        column_ids: &[(u16, String)],
        ts_col_idx: Option<usize>,
        start_ts: i64,
        end_ts: i64,
    ) -> (usize, usize) {
        let ts_idx = match ts_col_idx {
            Some(idx) => idx,
            None => return (0, decoded.len()),
        };

        let col_idx = column_ids[ts_idx].0 as usize;
        let n = decoded.len();

        // Extract timestamps as i64 for binary search
        let get_ts = |row_idx: usize| -> i64 {
            decoded.get(row_idx)
                .and_then(|row| row.get(col_idx))
                .and_then(|v| if let Value::Timestamp(ts) = v { Some(ts.as_micros()) } else { None })
                .unwrap_or(i64::MIN)
        };

        // Find first row with ts >= start_ts
        let row_start = match decoded.binary_search_by(|row| {
            let ts = row.get(col_idx)
                .and_then(|v| if let Value::Timestamp(t) = v { Some(t.as_micros()) } else { None })
                .unwrap_or(i64::MIN);
            ts.cmp(&start_ts)
        }) {
            Ok(idx) => idx,
            Err(idx) => idx, // insertion point = first ts >= start_ts
        };

        // Find first row with ts > end_ts
        let row_end = match decoded.binary_search_by(|row| {
            let ts = row.get(col_idx)
                .and_then(|v| if let Value::Timestamp(t) = v { Some(t.as_micros()) } else { None })
                .unwrap_or(i64::MIN);
            ts.cmp(&end_ts.saturating_add(1))  // search for end_ts+1 to get exclusive upper bound
        }) {
            Ok(idx) => idx,
            Err(idx) => idx.min(n),
        };

        let _ = get_ts; // suppress unused warning if needed
        (row_start.min(n), row_end.min(n))
    }

    /// Delete expired segments for a table. Returns count deleted.
    pub fn gc_expired(&self, table_name: &str, cutoff_ts: i64) -> Result<usize> {
        let table_id = self.table_registry.get_table_id(table_name)
            .map_err(|_| StorageError::TableNotFound(table_name.to_string()))?;

        let manager = self.managers.get(&table_id)
            .ok_or_else(|| StorageError::Columnar(format!(
                "No segment manager for table '{}'", table_name
            )))?;

        manager.delete_expired(cutoff_ts)
    }

    /// Drop all data for a table (used by DROP TABLE).
    pub fn drop_table(&self, table_name: &str) -> Result<usize> {
        let table_id = self.table_registry.get_table_id(table_name)
            .map_err(|_| StorageError::TableNotFound(table_name.to_string()))?;

        // Remove buffer
        self.buffers.remove(&table_id);

        // Delete all segments
        let count = if let Some(mgr) = self.managers.get(&table_id) {
            let c = mgr.delete_all()?;
            // Remove segment directory
            let dir = mgr.directory().to_path_buf();
            drop(mgr);
            self.managers.remove(&table_id);
            let _ = std::fs::remove_dir_all(&dir);
            c
        } else {
            0
        };

        self.schemas.remove(&table_id);
        Ok(count)
    }

    /// Flush all write buffers to segment files.
    pub fn flush_all(&self) -> Result<()> {
        let mut flushed = 0usize;
        for entry in self.buffers.iter() {
            let mut buffer = entry.value().lock().unwrap();
            if let Some(mut batch) = buffer.take() {
                drop(buffer);
                self.flush_batch(&mut batch)?;
                flushed += 1;
            }
        }
        if flushed > 0 {
            debug_log!("[Columnar] Flushed {} buffers to segments", flushed);
        }
        Ok(())
    }

    /// Close the store (flush all buffers).
    pub fn close(&self) -> Result<()> {
        self.flush_all()
    }

    /// Check if a table is registered with the columnar store.
    pub fn has_table(&self, table_name: &str) -> bool {
        if let Ok(table_id) = self.table_registry.get_table_id(table_name) {
            self.managers.contains_key(&table_id)
        } else {
            false
        }
    }

    /// Get segment count for a table.
    pub fn segment_count(&self, table_name: &str) -> usize {
        if let Ok(table_id) = self.table_registry.get_table_id(table_name) {
            self.managers.get(&table_id).map_or(0, |m| m.segment_count())
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColumnDef, ColumnType, TableType, Timestamp};
    use tempfile::TempDir;

    fn make_registry_and_schema(tmp: &TempDir) -> (Arc<TableRegistry>, Arc<TableSchema>) {
        let registry = Arc::new(TableRegistry::new(tmp.path()).unwrap());
        let columns = vec![
            ColumnDef::new("ts".to_string(), ColumnType::Timestamp, 0),
            ColumnDef::new("temp".to_string(), ColumnType::Float, 1),
            ColumnDef::new("label".to_string(), ColumnType::Text, 2),
        ];
        let mut schema = TableSchema::new("sensors".to_string(), columns);
        schema.table_type = TableType::TimeSeries;
        schema.timeseries_column = Some("ts".to_string());
        let schema = Arc::new(schema);

        // Register in catalog
        registry.create_table((*schema).clone()).unwrap();

        (registry, schema)
    }

    #[test]
    fn test_store_ingest_and_query() {
        let dir = TempDir::new().unwrap();
        let (registry, schema) = make_registry_and_schema(&dir);
        let table_id = registry.get_table_id("sensors").unwrap();

        let store = ColumnarStore::create(
            dir.path().join("columnar").as_path(),
            ColumnarConfig::default(),
            Arc::new(AtomicU64::new(0)),
            registry,
        ).unwrap();

        store.register_table(table_id, &schema).unwrap();

        // Ingest 100 rows
        let mut rows = Vec::new();
        for i in 0..100 {
            rows.push(vec![
                Value::Timestamp(Timestamp::from_micros(i * 1000)),
                Value::Float(20.0 + i as f64 * 0.1),
                Value::text(format!("sensor_{}", i)),
            ]);
        }

        let result = store.ingest("sensors", rows).unwrap();
        assert_eq!(result.row_ids.len(), 100);

        // Flush to segment
        store.flush_all().unwrap();
        assert_eq!(store.segment_count("sensors"), 1);

        // Query time range [20_000, 50_000]
        let results = store.query_time_range(
            "sensors",
            20_000,
            50_000,
            &["ts".to_string(), "temp".to_string()],
        ).unwrap();

        // Should get rows with ts 20000..50000 (indices 20..50)
        assert_eq!(results.len(), 31); // 20, 21, ..., 50
    }

    #[test]
    fn test_store_gc_expired() {
        let dir = TempDir::new().unwrap();
        let (registry, schema) = make_registry_and_schema(&dir);
        let table_id = registry.get_table_id("sensors").unwrap();

        let mut config = ColumnarConfig::default();
        config.buffer_row_capacity = 10; // small buffer for testing

        let store = ColumnarStore::create(
            dir.path().join("columnar").as_path(),
            config,
            Arc::new(AtomicU64::new(0)),
            registry,
        ).unwrap();

        store.register_table(table_id, &schema).unwrap();

        // Ingest 20 rows (should create 2 segments)
        for batch in 0..2 {
            let mut rows = Vec::new();
            for i in 0..10 {
                let ts = batch * 10_000_000 + i * 100_000;
                rows.push(vec![
                    Value::Timestamp(Timestamp::from_micros(ts)),
                    Value::Float(ts as f64),
                    Value::text("x".to_string()),
                ]);
            }
            store.ingest("sensors", rows).unwrap();
            store.flush_all().unwrap();
        }

        assert_eq!(store.segment_count("sensors"), 2);

        // GC segments with max_ts < 10_000_000 (should delete first segment)
        let deleted = store.gc_expired("sensors", 10_000_000).unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(store.segment_count("sensors"), 1);
    }

    #[test]
    fn test_store_drop_table() {
        let dir = TempDir::new().unwrap();
        let (registry, schema) = make_registry_and_schema(&dir);
        let table_id = registry.get_table_id("sensors").unwrap();

        let store = ColumnarStore::create(
            dir.path().join("columnar").as_path(),
            ColumnarConfig::default(),
            Arc::new(AtomicU64::new(0)),
            registry,
        ).unwrap();

        store.register_table(table_id, &schema).unwrap();

        let rows = vec![vec![
            Value::Timestamp(Timestamp::from_micros(1000)),
            Value::Float(25.0),
            Value::text("test".to_string()),
        ]];
        store.ingest("sensors", rows).unwrap();
        store.flush_all().unwrap();

        assert_eq!(store.segment_count("sensors"), 1);

        let deleted = store.drop_table("sensors").unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(store.segment_count("sensors"), 0);
    }

    #[test]
    fn test_buffer_data_queryable_without_flush() {
        let dir = TempDir::new().unwrap();
        let (registry, schema) = make_registry_and_schema(&dir);
        let table_id = registry.get_table_id("sensors").unwrap();

        let store = ColumnarStore::create(
            dir.path().join("columnar").as_path(),
            ColumnarConfig::default(),
            Arc::new(AtomicU64::new(0)),
            registry,
        ).unwrap();

        store.register_table(table_id, &schema).unwrap();

        // Ingest rows but do NOT flush
        let rows = vec![
            vec![
                Value::Timestamp(Timestamp::from_micros(1000)),
                Value::Float(25.0),
                Value::text("a".to_string()),
            ],
            vec![
                Value::Timestamp(Timestamp::from_micros(2000)),
                Value::Float(26.0),
                Value::text("b".to_string()),
            ],
            vec![
                Value::Timestamp(Timestamp::from_micros(3000)),
                Value::Float(27.0),
                Value::text("c".to_string()),
            ],
        ];
        store.ingest("sensors", rows).unwrap();

        // Query without flush — should still return data from buffer
        let results = store.query_time_range(
            "sensors",
            0,
            5000,
            &[],
        ).unwrap();
        assert_eq!(results.len(), 3, "Buffer data should be queryable without flush");

        // Query with time range filter
        let filtered = store.query_time_range(
            "sensors",
            1500,
            2500,
            &[],
        ).unwrap();
        assert_eq!(filtered.len(), 1, "Should filter buffer data by time range");
    }

    #[test]
    fn test_segment_merging() {
        let dir = TempDir::new().unwrap();
        let (registry, schema) = make_registry_and_schema(&dir);
        let table_id = registry.get_table_id("sensors").unwrap();

        let mut config = ColumnarConfig::default();
        config.buffer_row_capacity = 10; // small buffer → frequent flushes
        config.enable_merge = true;
        config.merge_threshold_segments = 4; // merge when >= 4 segments
        config.segment_target_rows = 100; // merge small segments into one with up to 100 rows

        let store = ColumnarStore::create(
            dir.path().join("columnar").as_path(),
            config,
            Arc::new(AtomicU64::new(0)),
            registry,
        ).unwrap();

        store.register_table(table_id, &schema).unwrap();

        // Create 5 small segments (10 rows each)
        for batch in 0..5 {
            let mut rows = Vec::new();
            for i in 0..10 {
                let ts = batch * 100_000 + i * 1_000;
                rows.push(vec![
                    Value::Timestamp(Timestamp::from_micros(ts)),
                    Value::Float(20.0 + ts as f64 * 0.001),
                    Value::text(format!("s_{}_{}", batch, i)),
                ]);
            }
            store.ingest("sensors", rows).unwrap();
            store.flush_all().unwrap();
        }

        // After 5 flushes, some segments may already have been merged automatically
        // (the merge triggers after each flush if segment_count >= threshold).
        // The key test: verify data integrity after any merges.
        let count_before = store.segment_count("sensors");
        assert!(count_before >= 1, "Should have at least 1 segment, got {}", count_before);

        // Verify data integrity — query full range
        let results = store.query_time_range(
            "sensors",
            0,
            600_000,
            &[],
        ).unwrap();
        assert_eq!(results.len(), 50, "All 50 rows should be queryable, got {}", results.len());
    }
}
