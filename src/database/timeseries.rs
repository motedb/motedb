//! Time-Series Stream Ingestion API
//!
//! Optimized for high-frequency sensor data (IMU 100Hz-1kHz, motor controllers).
//! Bypasses SQL parser for minimal overhead.
//!
//! # Architecture
//! ```text
//! ingest(table, rows)
//!     ↓
//! WriteController.check()  ← backpressure gate
//!     ↓
//! Batch allocate row_ids (lock-free atomic)
//!     ↓
//! Single WAL batch_append (one fsync)
//!     ↓
//! Single LSM batch_put (one lock)
//!     ↓
//! Timestamp index only (skip secondary indexes)
//!     ↓
//! IngestResult { row_ids, backpressure_applied, queue_depth }
//! ```

use crate::database::write_controller::BackpressureSignal;
use crate::storage::lsm::Value as LSMValue;
use crate::txn::wal::WALRecord;
use crate::types::{Row, RowId};
use crate::{MoteDB, Result, StorageError};
use std::sync::atomic::Ordering;

/// Result of a stream ingestion operation
#[derive(Debug, Clone)]
pub struct IngestResult {
    /// Row IDs assigned to the ingested rows
    pub row_ids: Vec<RowId>,
    /// Whether backpressure was applied during this ingest
    pub backpressure_applied: bool,
    /// Current write queue depth (pending rows in memtable)
    pub queue_depth: usize,
}

// =========================================================================
// MoteDB ingest implementation
// =========================================================================

impl MoteDB {
    /// Stream ingestion API — optimized for high-frequency sensor data.
    ///
    /// Bypasses SQL parser entirely. Writes go through:
    /// 1. Schema validation (fast path)
    /// 2. Batch row_id allocation (lock-free atomic)
    /// 3. Single WAL batch_append
    /// 4. Single LSM batch_put
    /// 5. Timestamp index only (secondary indexes deferred to checkpoint)
    ///
    /// # Example
    /// ```ignore
    /// let rows = vec![
    ///     vec![Value::Timestamp(ts1), Value::Float(accel_x), Value::Float(accel_y)],
    ///     vec![Value::Timestamp(ts2), Value::Float(accel_x), Value::Float(accel_y)],
    /// ];
    /// let result = db.ingest("imu_readings", rows)?;
    /// println!("Ingested {} rows, queue depth: {}",
    ///     result.row_ids.len(), result.queue_depth);
    /// ```
    pub fn ingest(&self, table_name: &str, rows: Vec<Row>) -> Result<IngestResult> {
        if self.is_closed.load(Ordering::Relaxed) {
            return Err(StorageError::InvalidData("Database is closed".into()));
        }
        if rows.is_empty() {
            return Ok(IngestResult {
                row_ids: Vec::new(),
                backpressure_applied: false,
                queue_depth: 0,
            });
        }

        // 1. Fast schema lookup + validation
        let schema = self.table_registry.get_table(table_name)?;
        for (idx, row) in rows.iter().enumerate() {
            schema.validate_row(row).map_err(|e| StorageError::InvalidData(format!(
                "Row {} validation failed for table '{}': {}", idx, table_name, e
            )))?;
        }

        let num_rows = rows.len();

        // 2. Batch allocate row IDs (lock-free atomic)
        let mut row_ids = Vec::with_capacity(num_rows);
        let start_id = self.next_row_id.fetch_add(num_rows as u64, Ordering::Relaxed);
        for i in 0..num_rows {
            row_ids.push(start_id + i as u64);
        }

        // 3. Build WAL records (single batch)
        let mut wal_records = Vec::with_capacity(num_rows);
        for (row_id, row) in row_ids.iter().zip(rows.iter()) {
            let partition = (*row_id % self.num_partitions as u64) as u8;
            wal_records.push(WALRecord::Insert {
                table_name: table_name.to_string(),
                row_id: *row_id,
                partition,
                data: row.clone(),
                txn_id: 0,
            });
        }

        // 4. Single WAL batch write
        self.wal.batch_append(0, wal_records)?;

        // 5. Batch write to LSM (single lock acquisition)
        {
            let mut kvs = Vec::with_capacity(num_rows);
            for (row_id, row) in row_ids.iter().zip(rows.iter()) {
                let row_data = bincode::serialize(row)?;
                let value = LSMValue::new(row_data, *row_id);
                let composite_key = self.make_composite_key(table_name, *row_id);
                kvs.push((composite_key, value));
            }
            self.lsm_engine.batch_put(&kvs)?;
        }

        // 6. Update timestamp index only (skip all secondary indexes)
        self.ingest_update_timestamp_index(&row_ids, &rows, &schema);

        // 7. Update pending counter + trigger auto-flush
        let queue_depth = self.ingest_trigger_auto_flush(num_rows);

        // 8. Check backpressure for result reporting
        let backpressure = self.check_backpressure();

        Ok(IngestResult {
            row_ids,
            backpressure_applied: backpressure != BackpressureSignal::Normal,
            queue_depth,
        })
    }

    /// Check backpressure before writing.
    ///
    /// Returns the current signal level. Callers should check this before
    /// calling `ingest()` to avoid overwhelming the storage engine.
    pub fn check_backpressure(&self) -> BackpressureSignal {
        // Check L0 SSTable count via level_stats
        let l0_count = self.lsm_engine.level_stats()
            .map(|stats| stats.first().map(|s| s.1).unwrap_or(0))
            .unwrap_or(0);

        let config = crate::database::write_controller::WriteControllerConfig::default();

        if l0_count >= config.l0_stop_threshold {
            return BackpressureSignal::Stop;
        }
        if l0_count >= config.l0_slowdown_threshold {
            return BackpressureSignal::SlowDown;
        }

        BackpressureSignal::Normal
    }

    // ---- Internal helpers ----

    /// Update timestamp index for ingested rows
    fn ingest_update_timestamp_index(
        &self,
        row_ids: &[RowId],
        rows: &[Row],
        schema: &crate::types::TableSchema,
    ) {
        // Find timestamp columns
        let ts_positions: Vec<usize> = schema.columns.iter()
            .filter_map(|col| {
                if matches!(col.col_type, crate::types::ColumnType::Timestamp) {
                    Some(col.position)
                } else {
                    None
                }
            })
            .collect();

        if ts_positions.is_empty() {
            return;
        }

        let mut ts_index = self.timestamp_index.write();
        for (row_id, row) in row_ids.iter().zip(rows.iter()) {
            for &pos in &ts_positions {
                if let Some(crate::types::Value::Timestamp(ts)) = row.get(pos) {
                    let _ = ts_index.insert(ts.as_micros() as u64, *row_id);
                }
            }
        }
    }

    /// Increment pending counter and trigger auto-flush if threshold reached.
    /// Returns current queue depth.
    fn ingest_trigger_auto_flush(&self, num_rows: usize) -> usize {
        let old_count = self.pending_updates.fetch_add(num_rows, Ordering::Relaxed);
        let new_count = old_count + num_rows;

        // Auto-flush every 2000 writes — reuse single background thread
        if old_count / 2_000 != new_count / 2_000 {
            self.request_auto_flush();
        }

        new_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DBConfig;

    #[test]
    fn test_robotics_config() {
        let config = DBConfig::for_robotics();
        assert_eq!(config.num_partitions, 2);
        assert!(matches!(
            config.wal_config.durability_level,
            crate::config::DurabilityLevel::Periodic { interval_ms: 50 }
        ));
        assert_eq!(
            config.index_update_strategy,
            crate::config::IndexUpdateStrategy::BatchOnly
        );
    }

    #[test]
    fn test_backpressure_signal_display() {
        assert_eq!(format!("{}", BackpressureSignal::Normal), "Normal");
        assert_eq!(format!("{}", BackpressureSignal::SlowDown), "SlowDown");
        assert_eq!(format!("{}", BackpressureSignal::Stop), "Stop");
    }
}
