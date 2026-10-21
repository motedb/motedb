//! Parquet import/export — the bridge to the training-data ecosystem.
//!
//! SQL surface:
//! ```sql
//! EXPORT TABLE t TO 'dump.parquet';       -- columnar dump, chunked write
//! IMPORT PARQUET 'dump.parquet' INTO t;   -- batch-load through the insert pipeline
//! ```
//!
//! Type mapping (round-trip safe for the types MoteDB stores):
//!
//! | MoteDB            | Arrow / Parquet           |
//! |-------------------|---------------------------|
//! | INTEGER           | Int64                     |
//! | FLOAT             | Float64                   |
//! | BOOLEAN           | Boolean                   |
//! | TEXT              | Utf8                      |
//! | TIMESTAMP         | Timestamp(Microsecond)    |
//! | VECTOR(n)         | FixedSizeList<Float32>(n) |
//! | BLOB              | Binary                    |
//!
//! SPATIAL columns are rejected with a clear error (SELECT a projection into
//! a staging table first).
//!
//! Memory discipline: rows flow from the executor's streaming path in chunks
//! of [`CHUNK_ROWS`] — a table with VECTOR columns never materializes.
//! Scalar-only tables ride the zero-copy columnar scan, which today
//! materializes on conversion (same as the Python `fetch_arrays` path); a
//! chunked columnar reader is future work if the 1M-row benchmark shows it
//! matters.
//!
//! Read-only containers: both statements are classified as writes (they are
//! side-effecting) and rejected by the executor's container gate, matching
//! `EXPORT DB` / `EXPORT DATASET` policy.

use std::path::Path;

/// Rows per RecordBatch — the unit of both parquet I/O and batch inserts.
#[cfg(feature = "parquet")]
const CHUNK_ROWS: usize = 8192;

impl crate::sql::QueryExecutor {
    /// `EXPORT TABLE t TO 'x.parquet'` — write the table's live rows (what
    /// `SELECT *` sees) to a parquet file. Returns the rows written.
    pub fn export_table_parquet(&self, table: &str, path: &Path) -> crate::Result<u64> {
        #[cfg(feature = "parquet")]
        {
            imp::export_run(self, table, path)
        }
        #[cfg(not(feature = "parquet"))]
        {
            let _ = (table, path);
            Err(crate::MoteDBError::InvalidArgument(
                "parquet support is not compiled in — rebuild with the `parquet` feature".into(),
            ))
        }
    }

    /// `IMPORT PARQUET 'x.parquet' INTO t` — stream the file's record batches
    /// through the batch-insert pipeline. Table columns are matched by NAME;
    /// table columns absent from the file become NULL. Returns rows inserted.
    pub fn import_parquet(&self, path: &Path, table: &str) -> crate::Result<u64> {
        #[cfg(feature = "parquet")]
        {
            imp::import_run(self, path, table)
        }
        #[cfg(not(feature = "parquet"))]
        {
            let _ = (path, table);
            Err(crate::MoteDBError::InvalidArgument(
                "parquet support is not compiled in — rebuild with the `parquet` feature".into(),
            ))
        }
    }
}

#[cfg(feature = "parquet")]
pub(crate) mod imp {
    use std::fs::File;
    use std::path::Path;
    use std::sync::Arc;

    use arrow_array::builder::{BinaryBuilder, FixedSizeListBuilder, Float32Builder};
    use arrow_array::{
        Array, BinaryArray, BooleanArray, FixedSizeBinaryArray, FixedSizeListArray, Float32Array,
        Float64Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, ListArray,
        RecordBatch, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ArrowWriter;

    use crate::sql::{Lexer, Parser, QueryExecutor, QueryResult, StreamingQueryResult};
    use crate::types::{ArcBytes, ArcString, ArcVec, ColumnDef, ColumnType, Timestamp, Value};

    use super::CHUNK_ROWS;

    fn perr(e: impl std::fmt::Display) -> crate::MoteDBError {
        crate::MoteDBError::InvalidArgument(format!("parquet: {e}"))
    }

    // -----------------------------------------------------------------------
    // Export
    // -----------------------------------------------------------------------

    pub fn export_run(exec: &QueryExecutor, table: &str, path: &Path) -> crate::Result<u64> {
        let schema = exec.db_handle().table_registry.get_table(table)?;

        let fields = schema
            .columns
            .iter()
            .map(column_field)
            .collect::<crate::Result<Vec<_>>>()?;
        let arrow_schema = Arc::new(Schema::new(fields));

        // Read through the front door: parse + execute `SELECT * FROM t`, so
        // row visibility (tombstones, newest versions) matches what SQL sees.
        let sql = format!("SELECT * FROM {}", quote_ident(table));
        let stmt = Parser::new(Lexer::new(&sql).tokenize()?).parse()?;
        let rows = rows_of(exec.execute_streaming_ref(&stmt)?)?;

        let col_types: Vec<ColumnType> =
            schema.columns.iter().map(|c| c.col_type.clone()).collect();
        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), None).map_err(perr)?;

        let mut total: u64 = 0;
        let mut chunk: Vec<Vec<Value>> = Vec::with_capacity(CHUNK_ROWS);
        for row in rows {
            chunk.push(row?);
            if chunk.len() == CHUNK_ROWS {
                total += chunk.len() as u64;
                writer
                    .write(&build_batch(&arrow_schema, &col_types, &chunk)?)
                    .map_err(perr)?;
                chunk.clear();
            }
        }
        if !chunk.is_empty() {
            total += chunk.len() as u64;
            writer
                .write(&build_batch(&arrow_schema, &col_types, &chunk)?)
                .map_err(perr)?;
        }
        writer.close().map_err(perr)?;
        Ok(total)
    }

    fn column_field(col: &ColumnDef) -> crate::Result<Field> {
        let dt = match col.col_type {
            ColumnType::Integer => DataType::Int64,
            ColumnType::Float => DataType::Float64,
            ColumnType::Boolean => DataType::Boolean,
            ColumnType::Text => DataType::Utf8,
            ColumnType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            ColumnType::Tensor(dim) => DataType::FixedSizeList(
                // Nullable item: FixedSizeListBuilder (and pyarrow) emit
                // nullable elements; declaring non-null fails the writer's
                // schema-vs-batch validation.
                Arc::new(Field::new("item", DataType::Float32, true)),
                dim as i32,
            ),
            ColumnType::Blob => DataType::Binary,
            ColumnType::Spatial => {
                return Err(crate::MoteDBError::InvalidArgument(format!(
                    "column '{}' is SPATIAL — not exportable to parquet yet",
                    col.name
                )))
            }
        };
        Ok(Field::new(&col.name, dt, col.nullable))
    }

    /// One RecordBatch from a chunk of rows. A `Value` variant that does not
    /// match the column's declared type becomes a null slot — the insert
    /// path enforces types, so this only happens for NULLs.
    #[allow(clippy::too_many_lines)]
    fn build_batch(
        schema: &Arc<Schema>,
        col_types: &[ColumnType],
        rows: &[Vec<Value>],
    ) -> crate::Result<RecordBatch> {
        let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(col_types.len());
        for (ci, ct) in col_types.iter().enumerate() {
            let arr: Arc<dyn Array> = match ct {
                ColumnType::Integer => Arc::new(Int64Array::from(
                    rows.iter()
                        .map(|r| match r.get(ci) {
                            Some(Value::Integer(i)) => Some(*i),
                            _ => None,
                        })
                        .collect::<Vec<_>>(),
                )),
                ColumnType::Float => Arc::new(Float64Array::from(
                    rows.iter()
                        .map(|r| match r.get(ci) {
                            Some(Value::Float(f)) => Some(*f),
                            _ => None,
                        })
                        .collect::<Vec<_>>(),
                )),
                ColumnType::Boolean => Arc::new(BooleanArray::from(
                    rows.iter()
                        .map(|r| match r.get(ci) {
                            Some(Value::Bool(b)) => Some(*b),
                            _ => None,
                        })
                        .collect::<Vec<_>>(),
                )),
                ColumnType::Timestamp => Arc::new(TimestampMicrosecondArray::from(
                    rows.iter()
                        .map(|r| match r.get(ci) {
                            Some(Value::Timestamp(t)) => Some(t.as_micros()),
                            _ => None,
                        })
                        .collect::<Vec<_>>(),
                )),
                ColumnType::Text => Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| match r.get(ci) {
                            Some(Value::Text(s)) => Some(s.as_str()),
                            _ => None,
                        })
                        .collect::<Vec<_>>(),
                )),
                ColumnType::Blob => {
                    let mut b = BinaryBuilder::with_capacity(rows.len(), rows.len() * 16);
                    for r in rows {
                        match r.get(ci) {
                            Some(Value::Blob(bytes)) => b.append_value(bytes.as_slice()),
                            _ => b.append_null(),
                        }
                    }
                    Arc::new(b.finish())
                }
                ColumnType::Tensor(dim) => {
                    let mut b = FixedSizeListBuilder::<Float32Builder>::with_capacity(
                        Float32Builder::with_capacity(rows.len() * dim),
                        *dim as i32,
                        rows.len(),
                    );
                    for r in rows {
                        match r.get(ci) {
                            Some(Value::Vector(v)) if v.len() == *dim => {
                                for x in v.0.iter() {
                                    b.values().append_value(*x);
                                }
                                b.append(true);
                            }
                            _ => {
                                // NULL (or wrong arity, which typed inserts
                                // prevent) still occupies its value slots.
                                for _ in 0..*dim {
                                    b.values().append_null();
                                }
                                b.append(false);
                            }
                        }
                    }
                    Arc::new(b.finish())
                }
                ColumnType::Spatial => {
                    return Err(crate::MoteDBError::InvalidArgument(
                        "SPATIAL columns cannot be exported to parquet".into(),
                    ))
                }
            };
            arrays.push(arr);
        }
        RecordBatch::try_new(schema.clone(), arrays).map_err(perr)
    }

    /// Uniform row iterator over whichever result shape the executor
    /// produced. We control the SQL (`SELECT *` carries no clauses), so the
    /// streaming variant passes its iterator through untouched; the
    /// columnar variant materializes via the standard conversion path.
    pub(crate) fn rows_of(
        res: StreamingQueryResult,
    ) -> crate::Result<Box<dyn Iterator<Item = crate::Result<Vec<Value>>> + Send>> {
        if matches!(res, StreamingQueryResult::SelectColumnar { .. }) {
            return match res.materialize() {
                Ok(QueryResult::Select { rows, .. }) => Ok(Box::new(rows.into_iter().map(Ok))),
                Ok(_) => Err(crate::MoteDBError::InvalidArgument(
                    "internal: columnar result did not materialize to Select".into(),
                )),
                Err(e) => Err(e),
            };
        }
        match res {
            StreamingQueryResult::SelectReady { rows, .. } => {
                Ok(Box::new(rows.into_iter().map(Ok)))
            }
            StreamingQueryResult::SelectStreaming {
                rows,
                order_by,
                limit,
                offset,
                distinct,
                ..
            } => {
                if order_by.is_some() || limit.is_some() || offset.is_some() || distinct {
                    return Err(crate::MoteDBError::InvalidArgument(
                        "internal: EXPORT TABLE got a clause-carrying SELECT".into(),
                    ));
                }
                Ok(rows)
            }
            _ => Err(crate::MoteDBError::InvalidArgument(
                "EXPORT TABLE requires a SELECT result".into(),
            )),
        }
    }

    /// Minimal identifier quoting for the internal `SELECT * FROM t` —
    /// rejects the escape vector rather than becoming an injection sink.
    fn quote_ident(name: &str) -> String {
        if name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            && !name.is_empty()
            && !name.starts_with(|c: char| c.is_ascii_digit())
        {
            name.to_string()
        } else {
            format!("{:?}", name) // Rust debug-quoted string works as a literal
        }
    }

    // -----------------------------------------------------------------------
    // Import
    // -----------------------------------------------------------------------

    pub fn import_run(exec: &QueryExecutor, path: &Path, table: &str) -> crate::Result<u64> {
        let schema = exec.db_handle().table_registry.get_table(table)?;

        let file = File::open(path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(perr)?
            .with_batch_size(CHUNK_ROWS)
            .build()
            .map_err(perr)?;

        let mut total: u64 = 0;
        for batch in reader {
            let batch = batch.map_err(perr)?;
            let rows = batch_rows(&batch, &schema.columns)?;
            total += rows.len() as u64;
            exec.db_handle()
                .batch_insert_rows_to_table(table, rows)?
                .len();
        }
        Ok(total)
    }

    /// Map one record batch onto full-width rows in table-schema order.
    /// Parquet fields are matched to table columns by name; table columns
    /// with no matching field become NULL.
    fn batch_rows(batch: &RecordBatch, columns: &[ColumnDef]) -> crate::Result<Vec<Vec<Value>>> {
        let pq_schema = batch.schema();
        let mapping: Vec<Option<usize>> = columns
            .iter()
            .map(|col| {
                pq_schema
                    .fields()
                    .iter()
                    .position(|f| f.name() == col.name.as_str())
            })
            .collect();
        let mut rows = Vec::with_capacity(batch.num_rows());
        for r in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(columns.len());
            for (ci, col) in columns.iter().enumerate() {
                let value = match mapping[ci] {
                    Some(ai) => cell(batch.column(ai).as_ref(), r, &col.col_type)?,
                    None => Value::Null,
                };
                row.push(value);
            }
            rows.push(row);
        }
        Ok(rows)
    }

    /// Convert one cell, coercing by the TARGET column type — widening
    /// (int→float) is allowed, anything lossy errors instead of corrupting.
    #[allow(clippy::too_many_lines)]
    fn cell(arr: &dyn Array, r: usize, target: &ColumnType) -> crate::Result<Value> {
        if arr.is_null(r) {
            return Ok(Value::Null);
        }
        let mismatch = || {
            crate::MoteDBError::InvalidArgument(format!(
                "cannot load parquet arrow type {:?} into {} column",
                arr.data_type(),
                type_name(target)
            ))
        };
        match target {
            ColumnType::Integer => match arr.data_type() {
                DataType::Int64 => {
                    let a = arr
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(mismatch)?;
                    Ok(Value::Integer(a.value(r)))
                }
                DataType::Int32 => {
                    let a = arr
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or_else(mismatch)?;
                    Ok(Value::Integer(a.value(r) as i64))
                }
                _ => Err(mismatch()),
            },
            ColumnType::Float => match arr.data_type() {
                DataType::Float64 => {
                    let a = arr
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or_else(mismatch)?;
                    Ok(Value::Float(a.value(r)))
                }
                DataType::Float32 => {
                    let a = arr
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .ok_or_else(mismatch)?;
                    Ok(Value::Float(a.value(r) as f64))
                }
                DataType::Int64 => {
                    let a = arr
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(mismatch)?;
                    Ok(Value::Float(a.value(r) as f64))
                }
                DataType::Int32 => {
                    let a = arr
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or_else(mismatch)?;
                    Ok(Value::Float(a.value(r) as f64))
                }
                _ => Err(mismatch()),
            },
            ColumnType::Boolean => {
                let a = arr
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(mismatch)?;
                Ok(Value::Bool(a.value(r)))
            }
            ColumnType::Text => match arr.data_type() {
                DataType::Utf8 => {
                    let a = arr
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(mismatch)?;
                    Ok(Value::Text(ArcString::from(a.value(r))))
                }
                DataType::LargeUtf8 => {
                    let a = arr
                        .as_any()
                        .downcast_ref::<LargeStringArray>()
                        .ok_or_else(mismatch)?;
                    Ok(Value::Text(ArcString::from(a.value(r))))
                }
                _ => Err(mismatch()),
            },
            ColumnType::Timestamp => match arr.data_type() {
                DataType::Timestamp(unit, _) => match unit {
                    TimeUnit::Second => {
                        let a = arr
                            .as_any()
                            .downcast_ref::<TimestampSecondArray>()
                            .ok_or_else(mismatch)?;
                        Ok(Value::Timestamp(Timestamp::from_secs(a.value(r))))
                    }
                    TimeUnit::Millisecond => {
                        let a = arr
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .ok_or_else(mismatch)?;
                        Ok(Value::Timestamp(Timestamp::from_millis(a.value(r))))
                    }
                    TimeUnit::Microsecond => {
                        let a = arr
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .ok_or_else(mismatch)?;
                        Ok(Value::Timestamp(Timestamp::from_micros(a.value(r))))
                    }
                    TimeUnit::Nanosecond => {
                        let a = arr
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .ok_or_else(mismatch)?;
                        Ok(Value::Timestamp(Timestamp::from_micros(
                            a.value(r).div_euclid(1_000),
                        )))
                    }
                },
                _ => Err(mismatch()),
            },
            ColumnType::Tensor(dim) => match arr.data_type() {
                DataType::FixedSizeList(_, _) => {
                    let a = arr
                        .as_any()
                        .downcast_ref::<FixedSizeListArray>()
                        .ok_or_else(mismatch)?;
                    let inner = a.value(r);
                    list_f32(inner.as_ref(), *dim)
                }
                DataType::List(_) => {
                    let a = arr
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .ok_or_else(mismatch)?;
                    let inner = a.value(r);
                    list_f32(inner.as_ref(), *dim)
                }
                _ => Err(mismatch()),
            },
            ColumnType::Blob => match arr.data_type() {
                DataType::Binary => {
                    let a = arr
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .ok_or_else(mismatch)?;
                    Ok(Value::Blob(ArcBytes::new(a.value(r).to_vec())))
                }
                DataType::LargeBinary => {
                    let a = arr
                        .as_any()
                        .downcast_ref::<LargeBinaryArray>()
                        .ok_or_else(mismatch)?;
                    Ok(Value::Blob(ArcBytes::new(a.value(r).to_vec())))
                }
                DataType::FixedSizeBinary(_) => {
                    let a = arr
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .ok_or_else(mismatch)?;
                    Ok(Value::Blob(ArcBytes::new(a.value(r).to_vec())))
                }
                _ => Err(mismatch()),
            },
            ColumnType::Spatial => Err(crate::MoteDBError::InvalidArgument(
                "SPATIAL columns cannot be loaded from parquet".into(),
            )),
        }
    }

    /// Read one list-typed cell as `Value::Vector`, enforcing the declared
    /// dimension — mismatch is an error, never silent truncation.
    fn list_f32(inner: &dyn Array, dim: usize) -> crate::Result<Value> {
        if inner.len() != dim {
            return Err(crate::MoteDBError::InvalidArgument(format!(
                "vector arity mismatch: column declares VECTOR({dim}), parquet cell has {}",
                inner.len()
            )));
        }
        let mut out = Vec::with_capacity(dim);
        match inner.data_type() {
            DataType::Float32 => {
                let a = inner.as_any().downcast_ref::<Float32Array>().unwrap();
                for i in 0..dim {
                    if a.is_null(i) {
                        return Err(crate::MoteDBError::InvalidArgument(
                            "null element inside a vector cell".into(),
                        ));
                    }
                    out.push(a.value(i));
                }
            }
            DataType::Float64 => {
                let a = inner.as_any().downcast_ref::<Float64Array>().unwrap();
                for i in 0..dim {
                    if a.is_null(i) {
                        return Err(crate::MoteDBError::InvalidArgument(
                            "null element inside a vector cell".into(),
                        ));
                    }
                    out.push(a.value(i) as f32);
                }
            }
            _ => {
                return Err(crate::MoteDBError::InvalidArgument(format!(
                    "vector column must contain Float32/Float64 lists, got {:?}",
                    inner.data_type()
                )))
            }
        }
        Ok(Value::Vector(ArcVec::new(out)))
    }

    fn type_name(ct: &ColumnType) -> &'static str {
        match ct {
            ColumnType::Integer => "INTEGER",
            ColumnType::Float => "FLOAT",
            ColumnType::Boolean => "BOOLEAN",
            ColumnType::Text => "TEXT",
            ColumnType::Timestamp => "TIMESTAMP",
            ColumnType::Tensor(_) => "VECTOR",
            ColumnType::Blob => "BLOB",
            ColumnType::Spatial => "SPATIAL",
        }
    }
}
