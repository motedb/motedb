//! In-memory columnar write buffer for accumulating rows before flushing to segment files.

use crate::types::{ColumnType, RowId, SqlRow, TableSchema, Timestamp, Value};

use super::config::ColumnarConfig;

/// Per-column typed buffer. Stores values in a type-homogeneous vector
/// for efficient compression during flush.
pub enum ColumnBuffer {
    Timestamp(Vec<Option<i64>>),
    Integer(Vec<Option<i64>>),
    Float(Vec<Option<f64>>),
    Bool(Vec<Option<bool>>),
    Text(Vec<Option<String>>),
    /// Fallback for Tensor, Spatial, Vector, etc. (rare in TimeSeries)
    Other(Vec<Value>),
}

impl ColumnBuffer {
    pub(crate) fn new(col_type: &ColumnType) -> Self {
        match col_type {
            ColumnType::Timestamp => Self::Timestamp(Vec::new()),
            ColumnType::Integer => Self::Integer(Vec::new()),
            ColumnType::Float => Self::Float(Vec::new()),
            ColumnType::Boolean => Self::Bool(Vec::new()),
            ColumnType::Text => Self::Text(Vec::new()),
            _ => Self::Other(Vec::new()),
        }
    }

    pub(crate) fn push(&mut self, value: Value) {
        match (self, value) {
            (Self::Timestamp(v), Value::Timestamp(ts)) => v.push(Some(ts.as_micros())),
            (Self::Timestamp(v), Value::Null) => v.push(None),
            (Self::Integer(v), Value::Integer(i)) => v.push(Some(i)),
            (Self::Integer(v), Value::Null) => v.push(None),
            (Self::Float(v), Value::Float(f)) => v.push(Some(f)),
            (Self::Float(v), Value::Null) => v.push(None),
            (Self::Bool(v), Value::Bool(b)) => v.push(Some(b)),
            (Self::Bool(v), Value::Null) => v.push(None),
            (Self::Text(v), Value::Text(s)) => v.push(Some(s.to_string())),
            (Self::Text(v), Value::Null) => v.push(None),
            (Self::Other(v), val) => v.push(val),
            // Type mismatch: push NULL to keep column alignment
            (Self::Timestamp(v), _) => v.push(None),
            (Self::Integer(v), _) => v.push(None),
            (Self::Float(v), _) => v.push(None),
            (Self::Bool(v), _) => v.push(None),
            (Self::Text(v), _) => v.push(None),
        }
    }

    fn byte_size(&self) -> usize {
        match self {
            Self::Timestamp(v) => v.len() * 8,
            Self::Integer(v) => v.len() * 8,
            Self::Float(v) => v.len() * 8,
            Self::Bool(v) => v.len(),
            Self::Text(v) => v
                .iter()
                .map(|s| s.as_ref().map_or(1, |s| s.len() + 8))
                .sum(),
            Self::Other(v) => v.len() * 32,
        }
    }

    /// Reorder elements in-place according to the given permutation.
    /// `perm[i]` = old index that should go to position i.
    fn reorder(&mut self, perm: &[usize]) {
        match self {
            Self::Timestamp(v) => perm_reorder(v, perm),
            Self::Integer(v) => perm_reorder(v, perm),
            Self::Float(v) => perm_reorder(v, perm),
            Self::Bool(v) => perm_reorder(v, perm),
            Self::Text(v) => perm_reorder(v, perm),
            Self::Other(v) => perm_reorder(v, perm),
        }
    }

    /// Compute column statistics (min/max/null_count) for zone map pruning.
    pub(crate) fn compute_statistics(
        &self,
        col_id: u16,
    ) -> Option<super::segment::ColumnStatistics> {
        use super::segment::{value_to_raw_bytes, ColumnStatistics};

        match self {
            Self::Timestamp(vals) if !vals.is_empty() => {
                let non_null: Vec<&i64> = vals.iter().filter_map(|v| v.as_ref()).collect();
                if non_null.is_empty() {
                    return Some(ColumnStatistics {
                        column_id: col_id,
                        min_value_raw: [0u8; 8],
                        max_value_raw: [0u8; 8],
                        null_count: vals.len() as u32,
                    });
                }
                let min = *non_null.iter().min().unwrap();
                let max = *non_null.iter().max().unwrap();
                let null_count = vals.iter().filter(|v| v.is_none()).count() as u32;
                Some(ColumnStatistics {
                    column_id: col_id,
                    min_value_raw: min.to_le_bytes(),
                    max_value_raw: max.to_le_bytes(),
                    null_count,
                })
            }
            Self::Integer(vals) if !vals.is_empty() => {
                let non_null: Vec<&i64> = vals.iter().filter_map(|v| v.as_ref()).collect();
                if non_null.is_empty() {
                    return Some(ColumnStatistics {
                        column_id: col_id,
                        min_value_raw: [0u8; 8],
                        max_value_raw: [0u8; 8],
                        null_count: vals.len() as u32,
                    });
                }
                let min = *non_null.iter().min().unwrap();
                let max = *non_null.iter().max().unwrap();
                let null_count = vals.iter().filter(|v| v.is_none()).count() as u32;
                Some(ColumnStatistics {
                    column_id: col_id,
                    min_value_raw: min.to_le_bytes(),
                    max_value_raw: max.to_le_bytes(),
                    null_count,
                })
            }
            Self::Float(vals) if !vals.is_empty() => {
                let non_null: Vec<&f64> = vals
                    .iter()
                    .filter_map(|v| v.as_ref())
                    .filter(|f| !f.is_nan())
                    .collect();
                if non_null.is_empty() {
                    let null_count = vals.iter().filter(|v| v.is_none()).count() as u32;
                    return Some(ColumnStatistics {
                        column_id: col_id,
                        min_value_raw: [0u8; 8],
                        max_value_raw: [0u8; 8],
                        null_count,
                    });
                }
                let min = non_null.iter().map(|&&v| v).fold(f64::INFINITY, f64::min);
                let max = non_null
                    .iter()
                    .map(|&&v| v)
                    .fold(f64::NEG_INFINITY, f64::max);
                let null_count = vals.iter().filter(|v| v.is_none()).count() as u32;
                Some(ColumnStatistics {
                    column_id: col_id,
                    min_value_raw: min.to_le_bytes(),
                    max_value_raw: max.to_le_bytes(),
                    null_count,
                })
            }
            Self::Bool(vals) if !vals.is_empty() => {
                let mut min_val = [0u8; 8];
                let mut max_val = [0u8; 8];
                let mut has_false = false;
                let mut has_true = false;
                let mut null_count = 0u32;
                for v in vals {
                    match v {
                        Some(false) => has_false = true,
                        Some(true) => has_true = true,
                        None => null_count += 1,
                    }
                }
                if has_false {
                    min_val[0] = 0;
                }
                if has_true {
                    min_val[0] = if has_false { 0 } else { 1 };
                    max_val[0] = 1;
                }
                if !has_false && !has_true && null_count > 0 {
                    return Some(ColumnStatistics {
                        column_id: col_id,
                        min_value_raw: [0u8; 8],
                        max_value_raw: [0u8; 8],
                        null_count: vals.len() as u32,
                    });
                }
                Some(ColumnStatistics {
                    column_id: col_id,
                    min_value_raw: min_val,
                    max_value_raw: max_val,
                    null_count,
                })
            }
            Self::Text(vals) if !vals.is_empty() => {
                let non_null: Vec<&String> = vals.iter().filter_map(|v| v.as_ref()).collect();
                if non_null.is_empty() {
                    return Some(ColumnStatistics {
                        column_id: col_id,
                        min_value_raw: [0u8; 8],
                        max_value_raw: [0u8; 8],
                        null_count: vals.len() as u32,
                    });
                }
                let min = non_null.iter().min().unwrap();
                let max = non_null.iter().max().unwrap();
                let null_count = vals.iter().filter(|v| v.is_none()).count() as u32;
                Some(ColumnStatistics {
                    column_id: col_id,
                    min_value_raw: value_to_raw_bytes(&Value::text(min.to_string())),
                    max_value_raw: value_to_raw_bytes(&Value::text(max.to_string())),
                    null_count,
                })
            }
            _ => None,
        }
    }
}

/// Whether the buffer should be flushed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushDecision {
    Continue,
    Flush,
}

/// Data extracted from the buffer for segment writing.
pub struct BufferedBatch {
    pub table_id: u32,
    pub columns: Vec<ColumnBuffer>,
    pub row_count: usize,
    pub row_ids: Vec<RowId>,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
}

/// In-memory columnar write buffer.
/// Generic in-place reorder using the given permutation.
/// `perm[i]` = old index that should go to position i.
fn perm_reorder<T: Clone>(v: &mut [T], perm: &[usize]) {
    let old: Vec<T> = v.to_vec();
    for (i, &old_idx) in perm.iter().enumerate() {
        v[i] = old[old_idx].clone();
    }
}

impl BufferedBatch {
    /// Sort all columns and row_ids by the timestamp column.
    /// If `ts_col_idx` is None or there are <= 1 rows, this is a no-op.
    pub fn sort_by_timestamp(&mut self, ts_col_idx: Option<usize>) {
        let ts_idx = match ts_col_idx {
            Some(idx) if idx < self.columns.len() => idx,
            _ => return,
        };

        if self.row_count <= 1 {
            return;
        }

        // Extract timestamps for sorting
        let timestamps: Vec<Option<i64>> = match &self.columns[ts_idx] {
            ColumnBuffer::Timestamp(vals) => vals.clone(),
            _ => return, // not a timestamp column
        };

        // Build sort permutation (ascending by timestamp)
        let mut indices: Vec<usize> = (0..self.row_count).collect();
        indices.sort_by_key(|&i| timestamps[i]);

        // Reorder all columns
        for col in &mut self.columns {
            col.reorder(&indices);
        }

        // Reorder row_ids
        let old_ids = self.row_ids.clone();
        for (i, &old_idx) in indices.iter().enumerate() {
            self.row_ids[i] = old_ids[old_idx];
        }
    }
}

/// In-memory columnar write buffer.
pub struct ColumnarWriteBuffer {
    table_id: u32,
    ts_column_idx: Option<usize>,
    columns: Vec<ColumnBuffer>,
    row_count: usize,
    byte_size: usize,
    row_ids: Vec<RowId>,
    min_timestamp: Option<i64>,
    max_timestamp: Option<i64>,
    config: ColumnarConfig,
}

impl ColumnarWriteBuffer {
    pub fn new(table_id: u32, schema: &TableSchema, config: ColumnarConfig) -> Self {
        let ts_column_idx = schema
            .timeseries_column
            .as_ref()
            .and_then(|ts_col| schema.columns.iter().position(|c| c.name == *ts_col));

        let columns = schema
            .columns
            .iter()
            .map(|c| ColumnBuffer::new(&c.col_type))
            .collect();

        Self {
            table_id,
            ts_column_idx,
            columns,
            row_count: 0,
            byte_size: 0,
            row_ids: Vec::new(),
            min_timestamp: None,
            max_timestamp: None,
            config,
        }
    }

    /// Append a row to the buffer. Returns FlushDecision.
    pub fn append(&mut self, row_id: RowId, row: &[Value]) -> FlushDecision {
        debug_assert_eq!(row.len(), self.columns.len());

        // Append each column value
        for (i, col) in self.columns.iter_mut().enumerate() {
            col.push(row[i].clone());
        }

        // Track timestamp
        if let Some(ts_idx) = self.ts_column_idx {
            if let Value::Timestamp(ts) = &row[ts_idx] {
                let micros = ts.as_micros();
                self.min_timestamp = Some(self.min_timestamp.map_or(micros, |m| m.min(micros)));
                self.max_timestamp = Some(self.max_timestamp.map_or(micros, |m| m.max(micros)));
            }
        }

        self.row_ids.push(row_id);
        self.row_count += 1;

        // Update byte size estimate
        self.byte_size =
            self.columns.iter().map(|c| c.byte_size()).sum::<usize>() + self.row_ids.len() * 8;

        // Flush decision
        if self.row_count >= self.config.buffer_row_capacity
            || self.byte_size >= self.config.buffer_byte_capacity
        {
            FlushDecision::Flush
        } else {
            FlushDecision::Continue
        }
    }

    /// Take all buffered data, clearing the buffer.
    pub fn take(&mut self) -> Option<BufferedBatch> {
        if self.row_count == 0 {
            return None;
        }

        let mut new_columns = Vec::with_capacity(self.columns.len());
        for col in &self.columns {
            new_columns.push(match col {
                ColumnBuffer::Timestamp(_) => ColumnBuffer::Timestamp(Vec::new()),
                ColumnBuffer::Integer(_) => ColumnBuffer::Integer(Vec::new()),
                ColumnBuffer::Float(_) => ColumnBuffer::Float(Vec::new()),
                ColumnBuffer::Bool(_) => ColumnBuffer::Bool(Vec::new()),
                ColumnBuffer::Text(_) => ColumnBuffer::Text(Vec::new()),
                ColumnBuffer::Other(_) => ColumnBuffer::Other(Vec::new()),
            });
        }

        // Swap columns
        let old_columns = std::mem::replace(&mut self.columns, new_columns);

        let batch = BufferedBatch {
            table_id: self.table_id,
            columns: old_columns,
            row_count: self.row_count,
            row_ids: std::mem::take(&mut self.row_ids),
            min_timestamp: self.min_timestamp.unwrap_or(0),
            max_timestamp: self.max_timestamp.unwrap_or(0),
        };

        self.row_count = 0;
        self.byte_size = 0;
        self.min_timestamp = None;
        self.max_timestamp = None;

        Some(batch)
    }

    /// Current row count.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Get the current min/max timestamps (for query overlap check).
    pub fn timestamp_range(&self) -> Option<(i64, i64)> {
        self.min_timestamp.zip(self.max_timestamp)
    }

    /// Get the table_id.
    pub fn table_id(&self) -> u32 {
        self.table_id
    }

    /// Snapshot buffer data for query (P0: buffer queryability).
    /// Returns matching (row_id, SqlRow) pairs for the given time range.
    pub fn snapshot_rows(
        &self,
        start_ts: i64,
        end_ts: i64,
        _schema: &TableSchema,
        column_ids: &[(u16, String)],
    ) -> Vec<(RowId, SqlRow)> {
        if self.row_count == 0 {
            return Vec::new();
        }

        let mut results = Vec::new();
        for row_idx in 0..self.row_count {
            // Check timestamp range
            if let Some(ts_idx) = self.ts_column_idx {
                let in_range = match &self.columns[ts_idx] {
                    ColumnBuffer::Timestamp(vals) => vals
                        .get(row_idx)
                        .is_some_and(|ts| ts.is_some_and(|ts| ts >= start_ts && ts <= end_ts)),
                    _ => false,
                };
                if !in_range {
                    continue;
                }
            }

            // Build SqlRow from requested columns
            let mut sql_row = SqlRow::new();
            for &(col_id, ref col_name) in column_ids {
                let idx = col_id as usize;
                if idx >= self.columns.len() {
                    continue;
                }
                let val = match &self.columns[idx] {
                    ColumnBuffer::Timestamp(vals) => vals
                        .get(row_idx)
                        .and_then(|v| v.map(|ts| Value::Timestamp(Timestamp::from_micros(ts))))
                        .or(Some(Value::Null)),
                    ColumnBuffer::Integer(vals) => vals
                        .get(row_idx)
                        .and_then(|v| v.map(Value::Integer))
                        .or(Some(Value::Null)),
                    ColumnBuffer::Float(vals) => vals
                        .get(row_idx)
                        .and_then(|v| v.map(Value::Float))
                        .or(Some(Value::Null)),
                    ColumnBuffer::Bool(vals) => vals
                        .get(row_idx)
                        .and_then(|v| v.map(Value::Bool))
                        .or(Some(Value::Null)),
                    ColumnBuffer::Text(vals) => vals
                        .get(row_idx)
                        .and_then(|v| v.as_ref().map(|s| Value::text(s.clone())))
                        .or(Some(Value::Null)),
                    ColumnBuffer::Other(vals) => vals.get(row_idx).cloned(),
                };
                if let Some(v) = val {
                    sql_row.insert(col_name.clone(), v);
                }
            }

            let row_id = self.row_ids.get(row_idx).copied().unwrap_or(0);
            results.push((row_id, sql_row));
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColumnDef, ColumnType, Timestamp};
    use std::sync::Arc;

    fn make_schema() -> Arc<TableSchema> {
        use crate::types::TableType;
        let columns = vec![
            ColumnDef::new("ts".to_string(), ColumnType::Timestamp, 0),
            ColumnDef::new("temp".to_string(), ColumnType::Float, 1),
            ColumnDef::new("label".to_string(), ColumnType::Text, 2),
        ];
        let mut schema = crate::types::TableSchema::new("sensors".to_string(), columns);
        schema.table_type = TableType::TimeSeries;
        schema.timeseries_column = Some("ts".to_string());
        Arc::new(schema)
    }

    #[test]
    fn test_buffer_append_and_count() {
        let schema = make_schema();
        let config = ColumnarConfig::default();
        let mut buf = ColumnarWriteBuffer::new(1, &schema, config);

        let row = vec![
            Value::Timestamp(Timestamp::from_micros(1000)),
            Value::Float(25.5),
            Value::text("ok".to_string()),
        ];

        let decision = buf.append(0, &row);
        assert_eq!(decision, FlushDecision::Continue);
        assert_eq!(buf.row_count(), 1);
        assert!(!buf.is_empty());
        assert_eq!(buf.timestamp_range(), Some((1000, 1000)));
    }

    #[test]
    fn test_buffer_flush_decision() {
        let schema = make_schema();
        let mut config = ColumnarConfig::default();
        config.buffer_row_capacity = 5;

        let mut buf = ColumnarWriteBuffer::new(1, &schema, config);

        for i in 0..5 {
            let row = vec![
                Value::Timestamp(Timestamp::from_micros(i * 1000)),
                Value::Float(i as f64),
                Value::text(format!("row_{}", i)),
            ];
            let decision = buf.append(i as u64, &row);
            if i < 4 {
                assert_eq!(decision, FlushDecision::Continue);
            } else {
                assert_eq!(decision, FlushDecision::Flush);
            }
        }
        assert_eq!(buf.row_count(), 5);
    }

    #[test]
    fn test_buffer_take_clears() {
        let schema = make_schema();
        let config = ColumnarConfig::default();
        let mut buf = ColumnarWriteBuffer::new(1, &schema, config);

        let row = vec![
            Value::Timestamp(Timestamp::from_micros(1000)),
            Value::Float(25.0),
            Value::text("hello".to_string()),
        ];
        buf.append(0, &row);

        let batch = buf.take().unwrap();
        assert_eq!(batch.row_count, 1);
        assert_eq!(batch.row_ids, vec![0]);
        assert!(buf.is_empty());
        assert_eq!(buf.row_count(), 0);

        // Second take returns None
        assert!(buf.take().is_none());
    }

    #[test]
    fn test_buffer_timestamp_tracking() {
        let schema = make_schema();
        let config = ColumnarConfig::default();
        let mut buf = ColumnarWriteBuffer::new(1, &schema, config);

        for i in 0..10 {
            let row = vec![
                Value::Timestamp(Timestamp::from_micros(1000 + i * 500)),
                Value::Float(i as f64),
                Value::text("x".to_string()),
            ];
            buf.append(i as u64, &row);
        }

        assert_eq!(buf.timestamp_range(), Some((1000, 5500)));

        let batch = buf.take().unwrap();
        assert_eq!(batch.min_timestamp, 1000);
        assert_eq!(batch.max_timestamp, 5500);
    }

    #[test]
    fn test_null_preserved_not_converted_to_zero() {
        // Before fix: NULL was stored as 0 in Integer/Timestamp/Float columns.
        // After fix: NULL is stored as None in Option<T>.
        let schema = make_schema();
        let config = ColumnarConfig::default();
        let mut buf = ColumnarWriteBuffer::new(1, &schema, config);

        // Insert row with NULL in the Float column
        let row = vec![
            Value::Timestamp(Timestamp::from_micros(1000)),
            Value::Null,
            Value::text("label".to_string()),
        ];
        buf.append(0, &row);

        // Verify the float column has None, not Some(0.0)
        let batch = buf.take().unwrap();
        match &batch.columns[1] {
            ColumnBuffer::Float(vals) => {
                assert_eq!(vals.len(), 1);
                assert!(vals[0].is_none(), "NULL should be None, not Some(0.0)");
            }
            _ => panic!("Expected Float column"),
        }
    }

    #[test]
    fn test_type_mismatch_preserves_alignment() {
        // Before fix: type mismatch silently dropped values, causing column misalignment.
        // After fix: type mismatch pushes NULL to maintain alignment.
        let schema = make_schema();
        let config = ColumnarConfig::default();
        let mut buf = ColumnarWriteBuffer::new(1, &schema, config);

        // Insert row with Integer in Float column (type mismatch)
        let row = vec![
            Value::Timestamp(Timestamp::from_micros(1000)),
            Value::Integer(42), // wrong type for Float column
            Value::text("ok".to_string()),
        ];
        buf.append(0, &row);

        let batch = buf.take().unwrap();
        assert_eq!(batch.row_count, 1);
        // Float column should have one entry (None for the mismatch)
        match &batch.columns[1] {
            ColumnBuffer::Float(vals) => {
                assert_eq!(vals.len(), 1, "Column alignment must be preserved");
                assert!(vals[0].is_none(), "Type mismatch should push NULL");
            }
            _ => panic!("Expected Float column"),
        }
    }

    #[test]
    fn test_null_tracking_in_statistics() {
        // Before fix: null_count was always 0 for Integer/Timestamp/Float.
        // After fix: null_count correctly reflects NULL values.
        let schema = make_schema();
        let config = ColumnarConfig::default();
        let mut buf = ColumnarWriteBuffer::new(1, &schema, config);

        // Insert two rows, one with NULL float
        buf.append(
            0,
            &[
                Value::Timestamp(Timestamp::from_micros(1000)),
                Value::Float(1.5),
                Value::text("a".to_string()),
            ],
        );
        buf.append(
            1,
            &[
                Value::Timestamp(Timestamp::from_micros(2000)),
                Value::Null,
                Value::text("b".to_string()),
            ],
        );

        let batch = buf.take().unwrap();
        let stats = batch.columns[1].compute_statistics(1);
        assert!(stats.is_some());
        let s = stats.unwrap();
        assert_eq!(s.null_count, 1, "Should count 1 NULL in Float column");
    }

    #[test]
    fn test_snapshot_rows_with_nulls() {
        // Verify snapshot_rows returns Null for NULL values, not 0.
        let schema = make_schema();
        let config = ColumnarConfig::default();
        let mut buf = ColumnarWriteBuffer::new(1, &schema, config);

        buf.append(
            0,
            &[
                Value::Timestamp(Timestamp::from_micros(1000)),
                Value::Null,
                Value::text("test".to_string()),
            ],
        );

        let column_ids = vec![
            (0u16, "ts".to_string()),
            (1u16, "temp".to_string()),
            (2u16, "label".to_string()),
        ];
        let rows = buf.snapshot_rows(0, 2000, &schema, &column_ids);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.get("temp"), Some(&Value::Null));
    }
}
