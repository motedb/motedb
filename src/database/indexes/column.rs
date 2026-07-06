//! Column Index Operations
//!
//! Extracted from database_legacy.rs
//! Provides column value indexing for WHERE clause optimization

use crate::database::core::MoteDB;
use crate::index::column_value::{ColumnValueIndex, ColumnValueIndexConfig};
use crate::types::{RowId, Value};
use crate::{Result, StorageError};
use std::sync::Arc;

impl MoteDB {
    /// Create a column value index for WHERE clause optimization
    pub fn create_column_index(&self, table_name: &str, column_name: &str) -> Result<()> {
        let index_name = format!("{}.{}", table_name, column_name);
        self.create_column_index_with_name(table_name, column_name, &index_name)
    }

    /// Create a column value index with custom name
    pub fn create_column_index_with_name(
        &self,
        table_name: &str,
        column_name: &str,
        index_name: &str,
    ) -> Result<()> {
        ensure_open!(self);
        let indexes_dir = self.path.join("indexes");
        std::fs::create_dir_all(&indexes_dir)?;
        let index_path = indexes_dir.join(format!("column_{}.idx", index_name));

        let mut config = ColumnValueIndexConfig::default();
        // During CREATE INDEX, use a larger buffer (32 MB) to reduce BTree flush
        // frequency. With default 1 MB, 300K entries require ~21 flushes.
        // With 32 MB, all entries fit in a single buffer → 1 flush.
        config.mem_buffer_size = (self.column_index_buffer_size).max(32 * 1024 * 1024);
        let index = ColumnValueIndex::create(
            index_path,
            table_name.to_string(),
            column_name.to_string(),
            config,
        )?;

        let index_arc = Arc::new(index);
        self.column_indexes
            .insert(index_name.to_string(), index_arc.clone());

        // Populate from existing data
        if let Ok(schema) = self.table_registry.get_table(table_name) {
            if let Some(col_def) = schema.columns.iter().find(|c| c.name == column_name) {
                let col_position = col_def.position;
                let col_types = schema.col_types();

                debug_log!("[create_column_index] Building index...");
                let start_time = std::time::Instant::now();

                let mut indexed_count = 0;
                const SORT_BATCH: usize = 50000;

                // S8: multi-segment ColSegmentStore path. Flush pending buffer
                // (cheap delta), then iterate each segment's target column directly
                // — same O(1)-per-value access as the legacy single-SSTable fast
                // path, extended to N segments with newest-version-wins dedup.
                if self.has_col_segment_store(table_name) {
                    let store = self.get_or_create_col_segment_store(table_name, col_types)?;
                    // Only flush if there are buffered rows. Avoid triggering compaction.
                    if store.buffered_row_count() > 0 {
                        store.flush_buffer()?;
                    }
                    // 🚀 Collect ALL entries as raw [u8;64] bytes.
                    let segs = store.segments_snapshot();
                    let single_seg = segs.len() == 1;
                    let mut raw_entries: Vec<([u8; 64], RowId)> = Vec::new();
                    use std::collections::HashSet;
                    let mut seen_keys: Option<HashSet<u64>> = if single_seg {
                        None
                    } else {
                        Some(HashSet::new())
                    };
                    for seg in segs.iter().rev() {
                        let n = seg.sst.num_rows;
                        let has_deletions = seg.sst.row_map.has_any_deleted();
                        if seg.sst.column_tags[col_position].is_fixed() {
                            if let Ok(fseg) = seg.sst.read_fixed_i64(col_position) {
                                raw_entries.reserve(n);
                                for i in 0..n {
                                    if has_deletions && seg.sst.row_map.is_deleted(i) {
                                        continue;
                                    }
                                    let key = seg.sst.row_map.key(i);
                                    if let Some(ref mut s) = seen_keys {
                                        if !s.insert(key) {
                                            continue;
                                        }
                                    }
                                    let row_id = (key & 0xFFFFFFFF) as RowId;
                                    let mut buf = [0u8; 64];
                                    let ok = match &col_types[col_position] {
                                        crate::types::ColumnType::Integer => fseg
                                            .get_i64(i)
                                            .map(|v| {
                                                buf[..8].copy_from_slice(&v.to_be_bytes());
                                            })
                                            .is_some(),
                                        crate::types::ColumnType::Float => fseg
                                            .get_f64(i)
                                            .map(|v| {
                                                let bits = v.to_bits();
                                                let sortable = if bits & (1u64 << 63) != 0 {
                                                    !bits
                                                } else {
                                                    bits ^ (1u64 << 63)
                                                };
                                                buf[..8].copy_from_slice(&sortable.to_be_bytes());
                                            })
                                            .is_some(),
                                        _ => false,
                                    };
                                    if ok {
                                        raw_entries.push((buf, row_id));
                                    }
                                }
                            }
                        } else if let Ok(tseg) = seg.sst.read_text(col_position) {
                            let n = seg.sst.num_rows;
                            raw_entries.reserve(n);
                            if has_deletions || !single_seg {
                                // Slow path: need deletion checks and/or dedup.
                                let extracted = tseg.bulk_extract_raw_keys();
                                for (buf, row_idx) in extracted {
                                    if has_deletions && seg.sst.row_map.is_deleted(row_idx) {
                                        continue;
                                    }
                                    let key = seg.sst.row_map.key(row_idx);
                                    if let Some(ref mut s) = seen_keys {
                                        if !s.insert(key) {
                                            continue;
                                        }
                                    }
                                    let row_id = (key & 0xFFFFFFFF) as RowId;
                                    raw_entries.push((buf, row_id));
                                }
                            } else {
                                // Fast path: single segment, no deletions.
                                let extracted = tseg.extract_all_raw_keys_unchecked();
                                for (buf, row_idx) in extracted {
                                    let key = seg.sst.row_map.key(row_idx);
                                    let row_id = (key & 0xFFFFFFFF) as RowId;
                                    raw_entries.push((buf, row_id));
                                }
                            }
                        }
                    }

                    indexed_count = raw_entries.len();
                    // Single bulk_insert_raw call — triggers bulk_load (fastest).
                    // bulk_load writes all pages + syncs superblock. No flush needed.
                    let _ = index_arc.bulk_insert_raw(raw_entries);
                    let elapsed = start_time.elapsed();
                    debug_log!(
                        "[create_column_index] ColSegment path: {} values in {:?}",
                        indexed_count,
                        elapsed
                    );
                } else if let Some(col_sst) = self.columnar_sstables.get(table_name) {
                    // Legacy single-SSTable path.
                    let num_rows = col_sst.num_rows;
                    let mut batch: Vec<(crate::types::Value, RowId)> =
                        Vec::with_capacity(SORT_BATCH);

                    if col_sst.column_tags[col_position].is_fixed() {
                        if let Ok(seg) = col_sst.read_fixed_i64(col_position) {
                            for i in 0..num_rows {
                                if col_sst.row_map.is_deleted(i) {
                                    continue;
                                }
                                let row_id = (col_sst.row_map.key(i) & 0xFFFFFFFF) as RowId;
                                let val = match &col_types[col_position] {
                                    crate::types::ColumnType::Integer => {
                                        seg.get_i64(i).map(crate::types::Value::Integer)
                                    }
                                    crate::types::ColumnType::Float => {
                                        seg.get_f64(i).map(crate::types::Value::Float)
                                    }
                                    _ => None,
                                }
                                .unwrap_or(crate::types::Value::Null);
                                if !matches!(val, crate::types::Value::Null) {
                                    batch.push((val, row_id));
                                    if batch.len() >= SORT_BATCH {
                                        batch.sort_by(|a, b| {
                                            a.0.partial_cmp(&b.0)
                                                .unwrap_or(std::cmp::Ordering::Equal)
                                        });
                                        indexed_count += batch.len();
                                        let _ = index_arc.batch_insert(std::mem::take(&mut batch));
                                        batch = Vec::with_capacity(SORT_BATCH);
                                    }
                                }
                            }
                        }
                    } else if let Ok(seg) = col_sst.read_text(col_position) {
                        for i in 0..num_rows {
                            if col_sst.row_map.is_deleted(i) {
                                continue;
                            }
                            let row_id = (col_sst.row_map.key(i) & 0xFFFFFFFF) as RowId;
                            if let Some(s) = seg.get_str(i) {
                                let val = crate::types::Value::Text(crate::types::ArcString(
                                    std::sync::Arc::from(s),
                                ));
                                batch.push((val, row_id));
                                if batch.len() >= SORT_BATCH {
                                    batch.sort_by(|a, b| {
                                        a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal)
                                    });
                                    indexed_count += batch.len();
                                    let _ = index_arc.batch_insert(std::mem::take(&mut batch));
                                    batch = Vec::with_capacity(SORT_BATCH);
                                }
                            }
                        }
                    }

                    // Flush remaining
                    if !batch.is_empty() {
                        batch.sort_by(|a, b| {
                            a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal)
                        });
                        indexed_count += batch.len();
                        let _ = index_arc.batch_insert(batch);
                    }
                    let _ = index_arc.flush();

                    let elapsed = start_time.elapsed();
                    debug_log!(
                        "[create_column_index] Columnar path: {} values in {:?}",
                        indexed_count,
                        elapsed
                    );
                } else {
                    // Fallback: scan LSM tree
                    let table_id = self.table_registry.get_table_id(table_name).unwrap_or(0) as u64;
                    let start_key = table_id << 32;
                    let end_key = (table_id + 1) << 32;

                    match self.lsm_engine.scan_range_streaming(start_key, end_key) {
                        Ok(mut lsm_iter) => {
                            let mut batch: Vec<(crate::types::Value, RowId)> =
                                Vec::with_capacity(SORT_BATCH);

                            loop {
                                match lsm_iter.next() {
                                    Some(Ok((composite_key, value))) => {
                                        if value.deleted {
                                            continue;
                                        }
                                        let row_id = (composite_key & 0xFFFFFFFF) as RowId;

                                        let col_value = match &value.data {
                                            crate::storage::lsm::ValueData::Inline(bytes) => {
                                                crate::storage::row_format::get_column(
                                                    bytes,
                                                    col_types,
                                                    col_position,
                                                )
                                                .unwrap_or(crate::types::Value::Null)
                                            }
                                            crate::storage::lsm::ValueData::Blob(blob_ref) => {
                                                match self.lsm_engine.resolve_blob(blob_ref) {
                                                    Ok(data) => {
                                                        crate::storage::row_format::get_column(
                                                            &data,
                                                            col_types,
                                                            col_position,
                                                        )
                                                        .unwrap_or(crate::types::Value::Null)
                                                    }
                                                    Err(_) => continue,
                                                }
                                            }
                                        };

                                        if !matches!(col_value, crate::types::Value::Null) {
                                            batch.push((col_value, row_id));
                                        }

                                        if batch.len() >= SORT_BATCH {
                                            // Sort by value before inserting — faster BTreeMap inserts
                                            batch.sort_by(|a, b| {
                                                a.0.partial_cmp(&b.0)
                                                    .unwrap_or(std::cmp::Ordering::Equal)
                                            });
                                            indexed_count += batch.len();
                                            if let Err(_e) =
                                                index_arc.batch_insert(std::mem::take(&mut batch))
                                            {
                                                debug_log!(
                                                    "[create_column_index] batch_insert failed: {}",
                                                    _e
                                                );
                                            }
                                            batch = Vec::with_capacity(SORT_BATCH);
                                        }
                                    }
                                    Some(Err(_e)) => {
                                        debug_log!("[create_column_index] scan error: {}", _e);
                                        break;
                                    }
                                    None => break,
                                }
                            }

                            // Sort and flush remaining batch
                            if !batch.is_empty() {
                                batch.sort_by(|a, b| {
                                    a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal)
                                });
                                indexed_count += batch.len();
                                if let Err(_e) = index_arc.batch_insert(batch) {
                                    debug_log!(
                                        "[create_column_index] final batch_insert failed: {}",
                                        _e
                                    );
                                }
                            }

                            // Final flush to disk
                            if let Err(e) = index_arc.flush() {
                                debug_log!("[create_column_index] Flush failed: {}", e);
                            }
                        }
                        Err(e) => {
                            debug_log!("[create_column_index] scan_range_streaming failed: {}", e);
                        }
                    }
                } // end else (fallback LSM scan)

                let _scan_time = start_time.elapsed();
                if indexed_count > 0 {
                    debug_log!(
                        "[create_column_index] Indexed {} values in {:?}",
                        indexed_count,
                        _scan_time
                    );
                }

                // Index is fully populated from LSM — clear the rebuild flag.
                index_arc.mark_rebuilt();
            }
        }

        Ok(())
    }

    /// Get all column indexes for a table
    pub fn get_table_column_indexes(&self, table_name: &str) -> Vec<String> {
        let prefix = format!("{}.", table_name);

        self.column_indexes
            .iter()
            .filter(|entry| entry.key().starts_with(&prefix))
            .map(|entry| entry.key().strip_prefix(&prefix).unwrap().to_string())
            .collect()
    }

    /// Flush column index to disk
    pub fn flush_column_index(&self, table_name: &str, column_name: &str) -> Result<()> {
        let index_name = format!("{}.{}", table_name, column_name);
        let index_ref = self.column_indexes.get(&index_name).ok_or_else(|| {
            StorageError::Index(format!("Column index '{}' not found", index_name))
        })?;

        index_ref.value().flush()?;
        Ok(())
    }

    /// Query by column value (point query)
    pub fn query_by_column(
        &self,
        table_name: &str,
        column_name: &str,
        value: &Value,
    ) -> Result<Vec<RowId>> {
        ensure_open!(self);
        let index_name = format!("{}.{}", table_name, column_name);

        let index_ref = self.column_indexes.get(&index_name).ok_or_else(|| {
            StorageError::Index(format!("Column index '{}' not found", index_name))
        })?;

        index_ref.value().get(value)
    }

    /// Query column value index with range (WHERE col >= start AND col <= end)
    pub fn query_by_column_range(
        &self,
        table_name: &str,
        column_name: &str,
        start: &Value,
        end: &Value,
    ) -> Result<Vec<RowId>> {
        self.query_by_column_between(table_name, column_name, start, true, end, true)
    }

    /// Query column value index: WHERE col < value
    pub fn query_by_column_less_than(
        &self,
        table_name: &str,
        column_name: &str,
        value: &Value,
    ) -> Result<Vec<RowId>> {
        let index_name = format!("{}.{}", table_name, column_name);
        let index_ref = self.column_indexes.get(&index_name).ok_or_else(|| {
            StorageError::Index(format!("Column index '{}' not found", index_name))
        })?;

        index_ref.value().query_less_than(value)
    }

    /// Query column value index: WHERE col > value
    pub fn query_by_column_greater_than(
        &self,
        table_name: &str,
        column_name: &str,
        value: &Value,
    ) -> Result<Vec<RowId>> {
        let index_name = format!("{}.{}", table_name, column_name);
        let index_ref = self.column_indexes.get(&index_name).ok_or_else(|| {
            StorageError::Index(format!("Column index '{}' not found", index_name))
        })?;

        index_ref.value().query_greater_than(value)
    }

    /// Query column value index: WHERE col <= value
    pub fn query_by_column_less_than_or_equal(
        &self,
        table_name: &str,
        column_name: &str,
        value: &Value,
    ) -> Result<Vec<RowId>> {
        let index_name = format!("{}.{}", table_name, column_name);
        let index_ref = self.column_indexes.get(&index_name).ok_or_else(|| {
            StorageError::Index(format!("Column index '{}' not found", index_name))
        })?;

        index_ref.value().query_less_than_or_equal(value)
    }

    /// Query column value index: WHERE col >= value
    pub fn query_by_column_greater_than_or_equal(
        &self,
        table_name: &str,
        column_name: &str,
        value: &Value,
    ) -> Result<Vec<RowId>> {
        let index_name = format!("{}.{}", table_name, column_name);
        let index_ref = self.column_indexes.get(&index_name).ok_or_else(|| {
            StorageError::Index(format!("Column index '{}' not found", index_name))
        })?;

        index_ref.value().query_greater_than_or_equal(value)
    }

    /// Query column value index: dual-bound range query
    pub fn query_by_column_between(
        &self,
        table_name: &str,
        column_name: &str,
        lower_bound: &Value,
        lower_inclusive: bool,
        upper_bound: &Value,
        upper_inclusive: bool,
    ) -> Result<Vec<RowId>> {
        let index_name = format!("{}.{}", table_name, column_name);
        let index_ref = self.column_indexes.get(&index_name).ok_or_else(|| {
            StorageError::Index(format!("Column index '{}' not found", index_name))
        })?;

        index_ref
            .value()
            .query_between(lower_bound, lower_inclusive, upper_bound, upper_inclusive)
    }
}

#[cfg(test)]
mod tests {
    use crate::Database;
    use tempfile::TempDir;

    #[test]
    fn test_create_column_index_and_query() {
        let dir = TempDir::new().unwrap();
        let db = Database::create(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, tag TEXT, val INT)")
            .unwrap();

        // Insert data first, then create index
        for i in 0..100i64 {
            db.execute(&format!(
                "INSERT INTO t VALUES ({}, 'tag_{}', {})",
                i,
                i % 5,
                i
            ))
            .unwrap();
        }

        db.execute("CREATE INDEX idx_tag ON t (tag) USING COLUMN")
            .unwrap();
        db.wait_for_indexes_ready();
        db.flush().unwrap();

        // Query should return results (either via index or full scan)
        let result = db.execute("SELECT id FROM t WHERE tag = 'tag_3'").unwrap();
        use crate::sql::QueryResult;
        if let QueryResult::Select { rows, .. } = result.materialize().unwrap() {
            assert_eq!(rows.len(), 20, "should find 20 rows with tag='tag_3'");
        }
    }

    #[test]
    fn test_create_index_then_insert() {
        let dir = TempDir::new().unwrap();
        let db = Database::create(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, category TEXT)")
            .unwrap();
        db.execute("CREATE INDEX idx_cat ON t (category) USING COLUMN")
            .unwrap();
        db.wait_for_indexes_ready();

        // Insert after index exists
        db.execute("INSERT INTO t VALUES (1, 'alpha')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'beta')").unwrap();
        db.execute("INSERT INTO t VALUES (3, 'alpha')").unwrap();

        // Query should find newly inserted data (synchronous index update)
        let result = db
            .execute("SELECT id FROM t WHERE category = 'alpha' ORDER BY id")
            .unwrap();
        use crate::sql::QueryResult;
        if let QueryResult::Select { rows, .. } = result.materialize().unwrap() {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][0], crate::types::Value::Integer(1));
            assert_eq!(rows[1][0], crate::types::Value::Integer(3));
        }
    }

    #[test]
    fn test_drop_index() {
        let dir = TempDir::new().unwrap();
        let db = Database::create(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, tag TEXT)")
            .unwrap();
        db.execute("CREATE INDEX idx_tag ON t (tag) USING COLUMN")
            .unwrap();
        db.wait_for_indexes_ready();

        db.execute("DROP INDEX idx_tag").unwrap();

        // Query should still work (falls back to full scan)
        let result = db.execute("SELECT * FROM t WHERE tag = 'test'").unwrap();
        assert!(result.materialize().is_ok());
    }

    #[test]
    fn test_wait_for_indexes_ready_completes_quickly() {
        // Regression test: wait_for_indexes_ready must return quickly
        // even if pending_index_batches is stale (e.g., due to a leaked counter
        // from a panicked index builder thread).
        let dir = TempDir::new().unwrap();
        let db = Database::create(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
            .unwrap();

        // With no flushes, pending should be 0 and return immediately
        let start = std::time::Instant::now();
        let ready = db.wait_for_indexes_ready();
        assert!(ready, "should be ready with no pending batches");
        assert!(
            start.elapsed() < std::time::Duration::from_secs(2),
            "wait_for_indexes_ready took {:?}",
            start.elapsed()
        );

        // Insert a row — still shouldn't trigger flush with small data
        db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();

        let start = std::time::Instant::now();
        let ready = db.wait_for_indexes_ready();
        assert!(ready, "should be ready after small insert");
        assert!(
            start.elapsed() < std::time::Duration::from_secs(2),
            "wait_for_indexes_ready took {:?}",
            start.elapsed()
        );
    }

    #[test]
    fn test_create_index_and_concurrent_insert() {
        // Regression test: CREATE INDEX + INSERT shouldn't deadlock
        let dir = TempDir::new().unwrap();
        let db = Database::create(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, category TEXT)")
            .unwrap();
        db.execute("CREATE INDEX idx_cat ON t (category) USING COLUMN")
            .unwrap();

        // Insert multiple rows rapidly (exercises index + mem_buffer)
        for i in 0..20 {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'cat_{}')", i, i % 3))
                .unwrap();
        }

        db.wait_for_indexes_ready();

        let result = db
            .execute("SELECT id FROM t WHERE category = 'cat_0' ORDER BY id")
            .unwrap();
        use crate::sql::QueryResult;
        if let QueryResult::Select { rows, .. } = result.materialize().unwrap() {
            assert_eq!(rows.len(), 7, "should find 7 rows with cat_0");
        }
    }
}
