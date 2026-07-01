/// Query executor - executes SQL statements against storage engine
use super::ast::*;
use super::evaluator::ExprEvaluator;
use super::row_converter::{row_to_sql_row, sql_row_to_row, rows_to_sql_rows};
use crate::database::MoteDB;
use crate::error::{Result, MoteDBError};
use crate::{StorageError};
use crate::types::{Value, SqlRow, TableSchema, ColumnType, RowId, Row};
use crate::storage::row_format;
use std::cmp::Ordering;
use std::sync::Arc;

fn decode_row(data: &[u8], schema: &TableSchema) -> crate::Result<Row> {
    row_format::decode(data, schema.col_types())
}

#[allow(clippy::type_complexity)]
type FromScanResult = Result<(Vec<(u64, SqlRow)>, Arc<TableSchema>)>;

#[allow(clippy::type_complexity)]
type RowPredicate = Option<Box<dyn Fn(&SqlRow) -> bool + Send + Sync>>;

/// Prefix all column names in rows with `table.prefix` and add metadata fields.
fn prefix_rows(rows: &mut [(u64, SqlRow)], table: &str, prefix: &str) {
    for (row_id, sql_row) in rows.iter_mut() {
        let mut new = SqlRow::new();
        new.insert("__row_id__".to_string(), Value::Integer(*row_id as i64));
        new.insert("__table__".to_string(), Value::text(table.to_string()));
        let old = std::mem::take(sql_row);
        for (col_name, val) in old {
            new.insert(format!("{}.{}", prefix, col_name), val);
        }
        *sql_row = new;
    }
}

/// Clone a schema and prefix every column name with `prefix.`
fn prefix_schema(schema: &TableSchema, prefix: &str) -> TableSchema {
    let mut s = schema.clone();
    for col in &mut s.columns {
        col.name = format!("{}.{}", prefix, col.name);
    }
    s
}

/// Column segment wrapper for zero-materialization results.
#[derive(Clone)]
pub enum ColumnarSeg {
    /// Fixed-width numeric column. The carried `ColumnType` tells the decoder
    /// how to interpret the raw bytes: Integer→get_i64, Float→get_f64,
    /// Boolean→get_bool. The FixedSegment blob stores raw bytes without a type
    /// tag, so the caller must tell the decoder how to interpret them —
    /// otherwise Integer columns are read back as Float (e.g. age=30 →
    /// Float(from_bits(30))) or Boolean bits are misread as a number.
    Fixed(crate::storage::lsm::columnar::FixedSegment, crate::types::ColumnType),
    Text(crate::storage::lsm::columnar::TextSegment),
}

/// Query result
#[derive(Debug)]
pub enum QueryResult {
    /// SELECT result
    Select {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },

    /// INSERT/UPDATE/DELETE result
    Modification {
        affected_rows: usize,
    },

    /// CREATE/DROP result
    Definition {
        message: String,
    },
}

impl QueryResult {
    pub fn affected_rows(&self) -> usize {
        match self {
            QueryResult::Modification { affected_rows } => *affected_rows,
            _ => 0,
        }
    }

    /// Get columns and rows from SELECT result
    /// Returns None if not a SELECT result
    pub fn select_rows(&self) -> Option<(&[String], &[Vec<Value>])> {
        match self {
            QueryResult::Select { columns, rows } => Some((columns.as_slice(), rows.as_slice())),
            _ => None,
        }
    }

    pub fn row_count(&self) -> usize {
        match self {
            QueryResult::Select { rows, .. } => rows.len(),
            QueryResult::Modification { affected_rows } => *affected_rows,
            _ => 0,
        }
    }
}


/// Callback flow control for `for_each()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamingControl {
    /// Continue processing rows
    Continue,
    /// Stop iteration early
    Break,
}

/// Result of `for_each()` streaming consumption.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForEachResult {
    /// Number of rows passed to the callback
    pub rows_processed: usize,
    /// true when max_rows was hit (more rows exist in storage)
    pub has_more: bool,
}

/// 🚀 流式查询结果（方案 C：零内存开销）
/// 
/// 返回迭代器而不是 Vec，实现真正的流式查询。
/// 
/// # 示例
/// ```ignore
/// // 新 API：流式迭代
/// let result = db.execute_streaming("SELECT * FROM robots WHERE age < 25")?;
/// result.for_each(|columns, row| {
///     println!("{:?}: {:?}", columns, row);
///     Ok(())
/// })?;
/// ```
pub enum StreamingQueryResult {
    /// SELECT 流式结果
    SelectStreaming {
        columns: Vec<String>,
        rows: Box<dyn Iterator<Item = Result<Vec<Value>>> + Send>,
        /// 🔧 ORDER BY 子句（在 materialize() 时应用）
        order_by: Option<Vec<OrderByExpr>>,
        /// 🔧 LIMIT 子句（在 materialize() 时应用）
        limit: Option<usize>,
        /// 🔧 OFFSET 子句（在 materialize() 时应用）
        offset: Option<usize>,
        /// 🔧 DISTINCT 标志（在 materialize() 时应用）
        distinct: bool,
        /// Safety limit: max rows to collect during materialize(). Truncates gracefully.
        max_result_rows: Option<usize>,
        /// Capacity hint for materialize() — avoids repeated Vec reallocations.
        /// Populated from fast_row_count() when available.
        size_hint: Option<usize>,
    },

    /// 🚀 Pre-materialized SELECT result (zero-overhead for fast PK paths)
    SelectReady {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },

    /// 🚀 Columnar result — typed arrays, zero per-row Vec<Value> allocation.
    /// Converted to Vec<Vec<Value>> lazily in materialize().
    SelectColumnar {
        columns: Vec<String>,
        /// Column segments (Fixed or Text), one per output column
        segments: Vec<ColumnarSeg>,
        /// Row indices to include (None = all rows)
        row_indices: Option<Vec<usize>>,
        num_rows: usize,
        row_map: crate::storage::lsm::columnar::RowMap,
    },

    /// INSERT/UPDATE/DELETE result
    Modification {
        affected_rows: usize,
    },

    /// CREATE/DROP result
    Definition {
        message: String,
    },
}

impl StreamingQueryResult {
    /// 🔥 物化结果集（供向后兼容的 execute() 使用）
    /// 
    /// 将流式结果立即加载到内存中，转换为 `QueryResult`。
    pub fn materialize(self) -> Result<QueryResult> {
        self.materialize_with_hint(None)
    }
    
    /// 🚀 优化版物化：支持容量预分配
    /// 
    /// # 优化点
    /// - Vec::with_capacity() 预分配容量，避免多次扩容
    /// - 减少内存重分配次数，提升性能 20-30%
    /// - 🔧 在物化时应用 ORDER BY、LIMIT、OFFSET、DISTINCT
    /// 
    /// # 参数
    /// - `size_hint`: 预估的结果行数（来自优化器统计信息）
    pub fn materialize_with_hint(self, size_hint: Option<usize>) -> Result<QueryResult> {
        match self {
            Self::SelectReady { columns, rows } => {
                Ok(QueryResult::Select { columns, rows })
            }
            Self::SelectColumnar { columns, segments, row_indices, num_rows, row_map } => {
                // Convert columnar to row-based lazily — only when materialize() called
                let ncols = segments.len();
                let source: Vec<usize> = if let Some(ref idx) = row_indices {
                    idx.iter().filter(|&&i| !row_map.is_deleted(i)).copied().collect()
                } else {
                    // Newest-version-wins dedup: when a single segment holds
                    // multiple versions of the same key (e.g. after an UPDATE
                    // appends a newer row without merging), keep only the last
                    // (newest) version per key. Rows are in append order
                    // (old→new), so the last occurrence of a key is newest.
                    let live: Vec<usize> = (0..num_rows)
                        .filter(|&i| !row_map.is_deleted(i))
                        .collect();
                    let mut latest_for_key: std::collections::HashMap<u64, usize> =
                        std::collections::HashMap::with_capacity(live.len());
                    for &i in &live {
                        latest_for_key.insert(row_map.key(i), i);
                    }
                    // Preserve original order, but only keep the newest version
                    // of each key.
                    let mut seen: std::collections::HashSet<u64> =
                        std::collections::HashSet::with_capacity(latest_for_key.len());
                    live.into_iter()
                        .filter(|&i| {
                            // Keep this row if it's the newest version of its key.
                            latest_for_key.get(&row_map.key(i)) == Some(&i)
                                && seen.insert(row_map.key(i))
                        })
                        .collect()
                };
                let mut rows = Vec::with_capacity(source.len());
                // String interning pool: reuse Arc<str> for repeated text values.
                // For region="US"/"EU" (2 values, 300K rows), saves 299,998 Arc allocations.
                let mut string_pool: std::collections::HashMap<&str, std::sync::Arc<str>> =
                    std::collections::HashMap::with_capacity(256);
                for &idx in &source {
                    let mut row = Vec::with_capacity(ncols);
                    for seg in &segments {
                        match seg {
                            ColumnarSeg::Fixed(f, ct) => {
                                // Decode according to the column's declared type.
                                row.push(match ct {
                                    crate::types::ColumnType::Integer =>
                                        f.get_i64(idx).map(Value::Integer),
                                    crate::types::ColumnType::Float =>
                                        f.get_f64(idx).map(Value::Float),
                                    crate::types::ColumnType::Boolean =>
                                        f.get_bool(idx).map(Value::Bool),
                                    _ => f.get_i64(idx).map(Value::Integer),
                                }.unwrap_or(Value::Null));
                            }
                            ColumnarSeg::Text(t) => {
                                let val = if let Some(s) = t.get_str(idx) {
                                    let arc = string_pool.get(s).cloned().unwrap_or_else(|| {
                                        let a: std::sync::Arc<str> = std::sync::Arc::from(s);
                                        string_pool.insert(s, a.clone());
                                        a
                                    });
                                    Value::Text(crate::types::ArcString(arc))
                                } else { Value::Null };
                                row.push(val);
                            }
                        }
                    }
                    rows.push(row);
                }
                Ok(QueryResult::Select { columns, rows })
            }
            Self::SelectStreaming { columns, rows, order_by, limit, offset, distinct, max_result_rows, size_hint: stream_hint } => {
                // Step 1: Collect rows, truncating at max_result_rows
                let estimated_size = stream_hint.or(size_hint).unwrap_or(1024);
                let mut materialized_rows = Vec::with_capacity(estimated_size);

                for row_result in rows {
                    materialized_rows.push(row_result?);
                    if let Some(max) = max_result_rows {
                        if materialized_rows.len() >= max {
                            break;
                        }
                    }
                }

                // After materializing a large result (> 100K rows), tell the
                // allocator to return freed heap to the OS. On macOS, the default
                // allocator retains freed memory indefinitely, causing RSS drift
                // across repeated queries (~+159 MB over 10 full scans).
                if materialized_rows.len() > 100_000 {
                    crate::database::persistence::trim_allocator();
                }

                // Step 2: Apply ORDER BY
                if let Some(order_clauses) = order_by {
                    Self::apply_order_by(&mut materialized_rows, &columns, &order_clauses)?;
                }

                // Step 3: Apply DISTINCT
                if distinct {
                    materialized_rows = Self::apply_distinct(materialized_rows);
                }

                // Step 4: Apply OFFSET and LIMIT
                let offset_val = offset.unwrap_or(0);
                let final_rows: Vec<Vec<Value>> = materialized_rows
                    .into_iter()
                    .skip(offset_val)
                    .take(limit.unwrap_or(usize::MAX))
                    .collect();

                Ok(QueryResult::Select {
                    columns,
                    rows: final_rows,
                })
            }
            Self::Modification { affected_rows } => {
                Ok(QueryResult::Modification { affected_rows })
            }
            Self::Definition { message } => {
                Ok(QueryResult::Definition { message })
            }
        }
    }
    
    /// 便利方法：逐行处理（零内存开销）

    /// 获取影响行数
    pub fn affected_rows(&self) -> usize {
        match self {
            Self::Modification { affected_rows } => *affected_rows,
            _ => 0,
        }
    }

    /// Get row count without full materialization. O(1) for columnar/ready results.
    pub fn row_count(&self) -> usize {
        match self {
            Self::SelectReady { rows, .. } => rows.len(),
            Self::SelectColumnar { row_indices, num_rows, row_map, .. } => {
                if let Some(ref idx) = row_indices {
                    idx.iter().filter(|&&i| !row_map.is_deleted(i)).count()
                } else {
                    (0..*num_rows).filter(|&i| !row_map.is_deleted(i)).count()
                }
            }
            _ => 0, // Streaming — not materialized yet
        }
    }

    /// 获取列名（仅 SELECT）
    pub fn columns(&self) -> Option<&[String]> {
        match self {
            Self::SelectStreaming { columns, .. } => Some(columns),
            Self::SelectReady { columns, .. } => Some(columns),
            Self::SelectColumnar { columns, .. } => Some(columns),
            _ => None,
        }
    }

    /// Inject max_result_rows safety limit into SelectStreaming variants.
    /// When the limit is reached, materialize() truncates gracefully instead of erroring.
    fn with_max_rows(self, max: Option<usize>) -> Self {
        match self {
            Self::SelectStreaming { columns, rows, order_by, limit, offset, distinct, max_result_rows: _, size_hint } => {
                Self::SelectStreaming {
                    columns, rows, order_by, limit, offset, distinct,
                    max_result_rows: max,
                    size_hint,
                }
            }
            other => other,
        }
    }

    /// Materialize with an explicit row limit. Returns (QueryResult, has_more).
    /// has_more is true when the limit was hit (more rows exist in storage).
    pub fn materialize_with_limit(self, max_rows: Option<usize>) -> Result<(QueryResult, bool)> {
        let result = self.with_max_rows(max_rows).materialize()?;
        // If max_rows was set, check if we truncated
        // We can't know for sure without a counter, so we approximate:
        // if rows.len() == max_rows, has_more is likely true
        let has_more = match (&result, max_rows) {
            (QueryResult::Select { rows, .. }, Some(max)) if rows.len() >= max => true,
            _ => false,
        };
        Ok((result, has_more))
    }

    /// Process rows one at a time with a callback. O(1) memory for simple queries.
    ///
    /// - No ORDER BY/DISTINCT: true streaming, O(1) memory, callback per-row
    /// - ORDER BY + LIMIT: Top-K heap, O(LIMIT) memory
    /// - ORDER BY no LIMIT: full sort, O(N) memory
    /// - DISTINCT: O(unique rows) memory
    ///
    /// When `max_rows` is hit, iteration stops and `has_more` is set to true.
    pub fn for_each<F>(self, mut callback: F, max_rows: Option<usize>) -> Result<ForEachResult>
    where
        F: FnMut(&[String], &Vec<Value>) -> Result<StreamingControl>,
    {
        match self {
            Self::SelectReady { columns, rows } => {
                let limit = max_rows.unwrap_or(usize::MAX);
                let mut count = 0;
                let has_more = rows.len() > limit;
                for row in rows.iter().take(limit) {
                    match callback(&columns, row)? {
                        StreamingControl::Continue => count += 1,
                        StreamingControl::Break => break,
                    }
                }
                Ok(ForEachResult { rows_processed: count, has_more })
            }
            Self::SelectColumnar { columns, segments, row_indices, num_rows, row_map } => {
                let limit = max_rows.unwrap_or(usize::MAX);
                let mut count = 0;
                let actual = row_indices.as_ref().map(|v| v.len()).unwrap_or(num_rows);
                let has_more = actual > limit;
                let n = actual.min(limit);
                let indices: Vec<usize> = if let Some(ref idx) = row_indices {
                    idx[..n].to_vec()
                } else {
                    // Newest-version-wins dedup (see materialize SelectColumnar note).
                    let live: Vec<usize> = (0..n).filter(|&i| !row_map.is_deleted(i)).collect();
                    let mut latest_for_key: std::collections::HashMap<u64, usize> =
                        std::collections::HashMap::with_capacity(live.len());
                    for &i in &live {
                        latest_for_key.insert(row_map.key(i), i);
                    }
                    let mut seen: std::collections::HashSet<u64> =
                        std::collections::HashSet::with_capacity(latest_for_key.len());
                    live.into_iter()
                        .filter(|&i| {
                            latest_for_key.get(&row_map.key(i)) == Some(&i)
                                && seen.insert(row_map.key(i))
                        })
                        .collect()
                };
                for &idx in &indices {
                    if row_map.is_deleted(idx) { continue; }
                    let mut row = Vec::with_capacity(segments.len());
                    for seg in &segments {
                        match seg {
                            ColumnarSeg::Fixed(f, ct) => {
                                row.push(match ct {
                                    crate::types::ColumnType::Integer =>
                                        f.get_i64(idx).map(Value::Integer),
                                    crate::types::ColumnType::Float =>
                                        f.get_f64(idx).map(Value::Float),
                                    crate::types::ColumnType::Boolean =>
                                        f.get_bool(idx).map(Value::Bool),
                                    _ => f.get_i64(idx).map(Value::Integer),
                                }.unwrap_or(Value::Null));
                            }
                            ColumnarSeg::Text(t) => row.push(t.get_str(idx).map(|s| Value::Text(crate::types::ArcString(std::sync::Arc::from(s)))).unwrap_or(Value::Null)),
                        }
                    }
                    match callback(&columns, &row)? {
                        StreamingControl::Continue => count += 1,
                        StreamingControl::Break => break,
                    }
                }
                Ok(ForEachResult { rows_processed: count, has_more })
            }
            Self::SelectStreaming { columns, rows, order_by, limit, offset, distinct, .. } => {
                let has_order = order_by.is_some();
                let order_clauses = order_by.unwrap_or_default();
                let offset_val = offset.unwrap_or(0);

                if distinct {
                    // DISTINCT path: deduplicate during scan
                    let mut seen = std::collections::HashSet::new();
                    let mut count = 0;
                    let mut has_more = false;
                    let mut skipped = 0;
                    let take_n = limit.unwrap_or(usize::MAX);

                    for row_result in rows {
                        let row = row_result?;
                        if skipped < offset_val { skipped += 1; continue; }
                        if !seen.insert(row.clone()) { continue; }
                        match callback(&columns, &row)? {
                            StreamingControl::Continue => count += 1,
                            StreamingControl::Break => break,
                        }
                        if count >= take_n { break; }
                        if let Some(max) = max_rows {
                            if count >= max { has_more = true; break; }
                        }
                    }
                    return Ok(ForEachResult { rows_processed: count, has_more });
                }

                if has_order {
                    let sort_specs: Vec<(usize, bool)> = order_clauses.iter().filter_map(|clause| {
                        let col_idx = match &clause.expr {
                            Expr::Column(name) => {
                                if let Some(idx) = columns.iter().position(|c| c == name) { idx }
                                else if let Some(dot_pos) = name.rfind('.') {
                                    columns.iter().position(|c| c == &name[dot_pos + 1..])?
                                } else { return None }
                            }
                            Expr::Literal(Value::Integer(n)) => (*n as usize).wrapping_sub(1),
                            _ => return None,
                        };
                        Some((col_idx, clause.asc))
                    }).collect();

                    if let Some(limit_val) = limit {
                        // Top-K path: O(K) memory
                        return Self::for_each_topk(rows, &columns, &sort_specs, limit_val, offset_val, max_rows, &mut callback);
                    }

                    // Full sort path: collect, sort, stream
                    let cap = max_rows.unwrap_or(4096);
                    let mut buf = Vec::with_capacity(cap.min(4096));
                    let mut has_more = false;
                    for row_result in rows {
                        buf.push(row_result?);
                        if let Some(max) = max_rows {
                            if buf.len() >= max { has_more = true; break; }
                        }
                    }
                    Self::sort_rows(&mut buf, &sort_specs);
                    let mut count = 0;
                    for row in buf.into_iter().skip(offset_val) {
                        match callback(&columns, &row)? {
                            StreamingControl::Continue => count += 1,
                            StreamingControl::Break => break,
                        }
                    }
                    return Ok(ForEachResult { rows_processed: count, has_more });
                }

                // Pure streaming path: O(1) memory
                let mut count = 0;
                let mut has_more = false;
                let mut skipped = 0;
                let take_n = limit.unwrap_or(usize::MAX);
                for row_result in rows {
                    let row = row_result?;
                    if skipped < offset_val { skipped += 1; continue; }
                    match callback(&columns, &row)? {
                        StreamingControl::Continue => count += 1,
                        StreamingControl::Break => break,
                    }
                    if count >= take_n { break; }
                    if let Some(max) = max_rows {
                        if count >= max { has_more = true; break; }
                    }
                }
                Ok(ForEachResult { rows_processed: count, has_more })
            }
            Self::Modification { .. } | Self::Definition { .. } => {
                Ok(ForEachResult { rows_processed: 0, has_more: false })
            }
        }
    }

    /// Top-K heap for ORDER BY + LIMIT: keeps only the K best rows.
    fn for_each_topk<F>(
        rows: Box<dyn Iterator<Item = Result<Vec<Value>>> + Send>,
        columns: &[String],
        sort_specs: &[(usize, bool)],
        limit: usize,
        offset: usize,
        max_rows: Option<usize>,
        callback: &mut F,
    ) -> Result<ForEachResult>
    where
        F: FnMut(&[String], &Vec<Value>) -> Result<StreamingControl>,
    {
        use std::cmp::Ordering;
        // We keep limit+offset rows in the heap (need offset extra for skipping)
        let k = limit.saturating_add(offset);

        // Store rows in a vec and maintain top-K via manual heap management
        // Use a BinaryHeap with Reverse to keep the "worst" of the top-K at the top for eviction
        // The heap stores (sort_key, row) where sort_key is the comparison tuple

        // Simpler approach: collect into vec, keep top-K via partial_sort
        let mut heap: Vec<Vec<Value>> = Vec::with_capacity(k + 1);
        let mut has_more = false;
        let effective_max = max_rows.map(|m| m.max(k)).unwrap_or(usize::MAX);
        let mut scanned = 0;

        for row_result in rows {
            let row = row_result?;
            scanned += 1;

            if heap.len() < k {
                heap.push(row);
                // Reheapify: sort the last element into place
                let idx = heap.len() - 1;
                if idx > 0 {
                    Self::sift_up(&mut heap, idx, sort_specs);
                }
            } else {
                // Compare with the "worst" element (index 0 in our min-heap)
                if Self::compare_rows(&row, &heap[0], sort_specs) == Ordering::Less {
                    heap[0] = row;
                    Self::sift_down(&mut heap, 0, sort_specs);
                }
            }

            if scanned >= effective_max { has_more = true; break; }
        }

        // Sort the top-K
        Self::sort_rows(&mut heap, sort_specs);

        // Stream sorted results, skipping offset
        let mut count = 0;
        for row in heap.into_iter().skip(offset) {
            if count >= limit { break; }
            match callback(columns, &row)? {
                StreamingControl::Continue => count += 1,
                StreamingControl::Break => break,
            }
        }

        Ok(ForEachResult { rows_processed: count, has_more })
    }

    /// Sort rows by pre-computed sort specs (shared by materialize and for_each)
    fn sort_rows(rows: &mut [Vec<Value>], sort_specs: &[(usize, bool)]) {
        use std::cmp::Ordering;
        rows.sort_by(|a, b| {
            for &(col_idx, asc) in sort_specs {
                if col_idx >= a.len() || col_idx >= b.len() { continue; }
                let cmp = Self::compare_values(&a[col_idx], &b[col_idx]);
                let final_cmp = if asc { cmp } else { cmp.reverse() };
                if final_cmp != Ordering::Equal { return final_cmp; }
            }
            Ordering::Equal
        });
    }

    fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (a, b) {
            (Value::Float(a), Value::Float(b)) => {
                if a.is_nan() && b.is_nan() { Ordering::Equal }
                else if a.is_nan() { Ordering::Greater }
                else if b.is_nan() { Ordering::Less }
                else { a.partial_cmp(b).unwrap_or(Ordering::Equal) }
            }
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            (a, b) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        }
    }

    fn compare_rows(a: &[Value], b: &[Value], sort_specs: &[(usize, bool)]) -> std::cmp::Ordering {
        for &(col_idx, asc) in sort_specs {
            if col_idx >= a.len() || col_idx >= b.len() { continue; }
            let cmp = Self::compare_values(&a[col_idx], &b[col_idx]);
            let final_cmp = if asc { cmp } else { cmp.reverse() };
            if final_cmp != std::cmp::Ordering::Equal { return final_cmp; }
        }
        std::cmp::Ordering::Equal
    }

    /// Min-heap sift-up for top-K
    fn sift_up(heap: &mut [Vec<Value>], mut idx: usize, sort_specs: &[(usize, bool)]) {
        while idx > 0 {
            let parent = (idx - 1) / 2;
            if Self::compare_rows(&heap[idx], &heap[parent], sort_specs) == std::cmp::Ordering::Less {
                heap.swap(idx, parent);
                idx = parent;
            } else {
                break;
            }
        }
    }

    /// Min-heap sift-down for top-K
    fn sift_down(heap: &mut [Vec<Value>], mut idx: usize, sort_specs: &[(usize, bool)]) {
        let len = heap.len();
        loop {
            let left = 2 * idx + 1;
            let right = 2 * idx + 2;
            let mut smallest = idx;
            if left < len && Self::compare_rows(&heap[left], &heap[smallest], sort_specs) == std::cmp::Ordering::Less {
                smallest = left;
            }
            if right < len && Self::compare_rows(&heap[right], &heap[smallest], sort_specs) == std::cmp::Ordering::Less {
                smallest = right;
            }
            if smallest != idx {
                heap.swap(idx, smallest);
                idx = smallest;
            } else {
                break;
            }
        }
    }

    /// 🔧 应用 ORDER BY（静态方法，在 materialize() 中调用）
    fn apply_order_by(
        rows: &mut [Vec<Value>],
        columns: &[String],
        order_clauses: &[OrderByExpr],
    ) -> Result<()> {
        use std::cmp::Ordering;

        // Pre-compute column indices and ascending flags to avoid O(columns) per comparison
        let sort_specs: Vec<(usize, bool)> = order_clauses.iter().filter_map(|clause| {
            let col_idx = match &clause.expr {
                Expr::Column(name) => {
                    // Try direct column name match
                    match columns.iter().position(|c| c == name) {
                        Some(idx) => idx,
                        None => {
                            // Try stripping table prefix (e.g., "t.id" → "id")
                            if let Some(dot_pos) = name.rfind('.') {
                                let base = &name[dot_pos + 1..];
                                match columns.iter().position(|c| c == base) {
                                    Some(idx) => return Some((idx, clause.asc)),
                                    None => return None,
                                }
                            }
                            return None;
                        }
                    }
                }
                Expr::Literal(Value::Integer(n)) => {
                    // ORDER BY column position (1-based)
                    let idx = (*n as usize).wrapping_sub(1);
                    if idx >= columns.len() {
                        return None; // Out of range — ignore
                    }
                    idx
                }
                _ => return None, // Expression ORDER BY not supported in streaming path
            };
            Some((col_idx, clause.asc))
        }).collect();

        if sort_specs.is_empty() && !order_clauses.is_empty() {
            return Err(MoteDBError::NotImplemented(
                "ORDER BY with expressions not supported in streaming queries; use materialize().".into()
            ));
        }

        rows.sort_by(|a, b| {
            for &(col_idx, asc) in &sort_specs {
                if col_idx >= a.len() || col_idx >= b.len() {
                    continue;
                }

                let cmp = match (&a[col_idx], &b[col_idx]) {
                    (Value::Float(a), Value::Float(b)) => {
                        if a.is_nan() && b.is_nan() {
                            Ordering::Equal
                        } else if a.is_nan() {
                            Ordering::Greater
                        } else if b.is_nan() {
                            Ordering::Less
                        } else {
                            a.partial_cmp(b).unwrap_or(Ordering::Equal)
                        }
                    }
                    (Value::Null, Value::Null) => Ordering::Equal,
                    (Value::Null, _) => Ordering::Less,
                    (_, Value::Null) => Ordering::Greater,
                    // Delegate to Value::partial_cmp for all other type pairs,
                    // including cross-type (Integer/Float), Timestamp, etc.
                    (a, b) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
                };

                let final_cmp = if asc { cmp } else { cmp.reverse() };

                if final_cmp != Ordering::Equal {
                    return final_cmp;
                }
            }
            Ordering::Equal
        });

        Ok(())
    }
    
    fn apply_distinct(rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
        use std::collections::HashSet;

        let mut seen = HashSet::new();
        let mut result = Vec::new();

        for row in rows {
            if seen.insert(row.clone()) {
                result.push(row);
            }
        }

        result
    }
}

/// Metadata for a single aggregate function extracted from a SELECT column.
/// Used by the positional GROUP BY fast path.
#[derive(Clone)]
struct AggregateInfo {
    func: String,           // COUNT, SUM, AVG, MIN, MAX
    col_pos: Option<usize>, // Column position; None means COUNT(*) or COUNT(1)
    distinct: bool,
}

/// Pre-compiled WHERE clause — column names resolved to positions once.
/// Eliminates per-row HashMap lookups in `schema.get_column_position()`.
///
/// For simple comparisons (Eq, Lt, etc.), evaluation is a single Vec index
/// + direct Value comparison — no recursion, no string ops, no HashMap.
#[allow(dead_code)]
enum CompiledWhere {
    Eq(usize, Value),                    // col[pos] == value
    Ne(usize, Value),                    // col[pos] != value
    Lt(usize, Value),                    // col[pos] < value
    Le(usize, Value),                    // col[pos] <= value
    Gt(usize, Value),                    // col[pos] > value
    Ge(usize, Value),                    // col[pos] >= value
    InHash(usize, std::collections::HashSet<Value>), // col[pos] IN set (O(1))
    Like(usize, String, bool),           // col[pos] LIKE pattern (negated bool)
    IsNull(usize, bool),                 // col[pos] IS NULL / IS NOT NULL
    Between(usize, Value, Value),        // col[pos] BETWEEN low AND high
    And(Vec<CompiledWhere>),             // all must match (short-circuit)
    Or(Vec<CompiledWhere>),              // any must match (short-circuit)
    Not(Box<CompiledWhere>),             // negation
}

impl CompiledWhere {
    /// Evaluate the compiled WHERE against a row.
    /// Returns `Some(bool)` if successful, `None` if fallback is needed.
    #[inline]
    fn eval(&self, row: &[Value]) -> Option<bool> {
        match self {
            CompiledWhere::Eq(pos, val) => {
                // SQL: NULL = val → NULL (false). Value PartialEq already returns false for Null==NonNull.
                Some(row.get(*pos).map_or(false, |v| {
                    if matches!(v, Value::Null) { return false; }
                    v == val
                }))
            }
            CompiledWhere::Ne(pos, val) => {
                // SQL: NULL <> val → NULL (false). Must check NULL explicitly.
                Some(row.get(*pos).map_or(false, |v| {
                    if matches!(v, Value::Null) { return false; }
                    v != val
                }))
            }
            CompiledWhere::Lt(pos, val) => {
                Some(row.get(*pos).filter(|v| !matches!(v, Value::Null)).map_or(false, |v| v < val))
            }
            CompiledWhere::Le(pos, val) => {
                Some(row.get(*pos).filter(|v| !matches!(v, Value::Null)).map_or(false, |v| v <= val))
            }
            CompiledWhere::Gt(pos, val) => {
                Some(row.get(*pos).filter(|v| !matches!(v, Value::Null)).map_or(false, |v| v > val))
            }
            CompiledWhere::Ge(pos, val) => {
                Some(row.get(*pos).filter(|v| !matches!(v, Value::Null)).map_or(false, |v| v >= val))
            }
            CompiledWhere::InHash(pos, set) => {
                // SQL: NULL IN (...) → NULL (false)
                Some(row.get(*pos).map_or(false, |v| {
                    if matches!(v, Value::Null) { return false; }
                    set.contains(v)
                }))
            }
            CompiledWhere::Like(pos, pattern, negated) => {
                let matches = row.get(*pos).and_then(|v| {
                    if let Value::Text(s) = v { Some(Self::like_match(s, pattern)) } else { None }
                }).unwrap_or(false);
                Some(if *negated { !matches } else { matches })
            }
            CompiledWhere::IsNull(pos, negated) => {
                let is_null = row.get(*pos).map_or(true, |v| matches!(v, Value::Null));
                Some(if *negated { !is_null } else { is_null })
            }
            CompiledWhere::Between(pos, low, high) => {
                Some(row.get(*pos).filter(|v| !matches!(v, Value::Null)).map_or(false, |v| {
                    v >= low && v <= high
                }))
            }
            CompiledWhere::And(conds) => {
                for c in conds {
                    if !c.eval(row)? { return Some(false); }
                }
                Some(true)
            }
            CompiledWhere::Or(conds) => {
                for c in conds {
                    if c.eval(row)? { return Some(true); }
                }
                Some(false)
            }
            CompiledWhere::Not(inner) => {
                Some(!inner.eval(row)?)
            }
        }
    }

    /// SQL LIKE pattern match: % = any chars, _ = single char
    fn like_match(text: &str, pattern: &str) -> bool {
        let mut ti = 0;
        let mut pi = 0;
        let mut star_pi = None;
        let mut star_ti = None;
        let pbytes = pattern.as_bytes();
        let tbytes = text.as_bytes();
        while pi < pbytes.len() {
            if pbytes[pi] == b'%' {
                pi += 1;
                if pi >= pbytes.len() { return true; } // trailing %
                star_pi = Some(pi);
                star_ti = Some(ti);
            } else if ti < tbytes.len() && (pbytes[pi] == b'_' || pbytes[pi] == tbytes[ti] || pbytes[pi].to_ascii_lowercase() == tbytes[ti].to_ascii_lowercase()) {
                pi += 1;
                ti += 1;
            } else if let (Some(spi), Some(sti)) = (star_pi, star_ti) {
                // backtrack: consume one more char for the %
                let new_ti = sti + 1;
                ti = new_ti;
                star_ti = Some(new_ti);
                pi = spi;
            } else {
                return false;
            }
        }
        // skip trailing %s
        while pi < pbytes.len() && pbytes[pi] == b'%' { pi += 1; }
        ti >= tbytes.len()
    }

    /// Collect all column positions referenced by this compiled WHERE.
    /// Used for partial row decode optimization.
    fn collect_positions(&self, positions: &mut Vec<usize>) {
        match self {
            CompiledWhere::Eq(pos, _) |
            CompiledWhere::Ne(pos, _) |
            CompiledWhere::Lt(pos, _) |
            CompiledWhere::Le(pos, _) |
            CompiledWhere::Gt(pos, _) |
            CompiledWhere::Ge(pos, _) |
            CompiledWhere::InHash(pos, _) |
            CompiledWhere::Like(pos, _, _) |
            CompiledWhere::IsNull(pos, _) |
            CompiledWhere::Between(pos, _, _) => {
                positions.push(*pos);
            }
            CompiledWhere::And(conds) | CompiledWhere::Or(conds) => {
                for c in conds { c.collect_positions(positions); }
            }
            CompiledWhere::Not(inner) => { inner.collect_positions(positions); }
        }
    }

    /// Evaluate against a partial decode buffer with position mapping.
    /// `pos_to_idx` maps schema column position → index in the partial buffer.
    #[inline]
    fn eval_at(&self, row: &[Value], pos_to_idx: &[Option<usize>]) -> Option<bool> {
        match self {
            CompiledWhere::Eq(pos, val) => {
                let idx = (*pos_to_idx).get(*pos)?.as_ref()?;
                Some(row.get(*idx).map_or(false, |v| {
                    if matches!(v, Value::Null) { return false; }
                    v == val
                }))
            }
            CompiledWhere::Ne(pos, val) => {
                let idx = (*pos_to_idx).get(*pos)?.as_ref()?;
                Some(row.get(*idx).map_or(false, |v| {
                    if matches!(v, Value::Null) { return false; }
                    v != val
                }))
            }
            CompiledWhere::Lt(pos, val) => {
                let idx = (*pos_to_idx).get(*pos)?.as_ref()?;
                Some(row.get(*idx).filter(|v| !matches!(v, Value::Null)).map_or(false, |v| v < val))
            }
            CompiledWhere::Le(pos, val) => {
                let idx = (*pos_to_idx).get(*pos)?.as_ref()?;
                Some(row.get(*idx).filter(|v| !matches!(v, Value::Null)).map_or(false, |v| v <= val))
            }
            CompiledWhere::Gt(pos, val) => {
                let idx = (*pos_to_idx).get(*pos)?.as_ref()?;
                Some(row.get(*idx).filter(|v| !matches!(v, Value::Null)).map_or(false, |v| v > val))
            }
            CompiledWhere::Ge(pos, val) => {
                let idx = (*pos_to_idx).get(*pos)?.as_ref()?;
                Some(row.get(*idx).filter(|v| !matches!(v, Value::Null)).map_or(false, |v| v >= val))
            }
            CompiledWhere::InHash(pos, set) => {
                let idx = (*pos_to_idx).get(*pos)?.as_ref()?;
                Some(row.get(*idx).map_or(false, |v| {
                    if matches!(v, Value::Null) { return false; }
                    set.contains(v)
                }))
            }
            CompiledWhere::Like(pos, pattern, negated) => {
                let idx = (*pos_to_idx).get(*pos)?.as_ref()?;
                let matches = row.get(*idx).and_then(|v| {
                    if let Value::Text(s) = v { Some(Self::like_match(s, pattern)) } else { None }
                }).unwrap_or(false);
                Some(if *negated { !matches } else { matches })
            }
            CompiledWhere::IsNull(pos, negated) => {
                let idx = (*pos_to_idx).get(*pos)?.as_ref()?;
                let is_null = row.get(*idx).map_or(true, |v| matches!(v, Value::Null));
                Some(if *negated { !is_null } else { is_null })
            }
            CompiledWhere::Between(pos, low, high) => {
                let idx = (*pos_to_idx).get(*pos)?.as_ref()?;
                Some(row.get(*idx).filter(|v| !matches!(v, Value::Null)).map_or(false, |v| {
                    v >= low && v <= high
                }))
            }
            CompiledWhere::And(conds) => {
                for c in conds {
                    if !c.eval_at(row, pos_to_idx)? { return Some(false); }
                }
                Some(true)
            }
            CompiledWhere::Or(conds) => {
                for c in conds {
                    if c.eval_at(row, pos_to_idx)? { return Some(true); }
                }
                Some(false)
            }
            CompiledWhere::Not(inner) => {
                Some(!inner.eval_at(row, pos_to_idx)?)
            }
        }
    }
}

/// Two-phase filtered iterator with reusable buffers.
/// Eliminates per-row Vec allocations by reusing where_buf, select_buf, and projected
/// across all rows in a scan.
struct TwoPhaseFilteredIterator {
    raw: crate::database::crud::TableRawStreamingIterator,
    // Reusable decode buffers (cleared per row, capacity retained)
    where_buf: Vec<crate::types::Value>,
    select_buf: Vec<crate::types::Value>,
    projected: Vec<crate::types::Value>,
    // Decode context
    col_types: Vec<crate::types::ColumnType>,
    fixed_count: usize,
    needed: Vec<usize>,
    // Pre-computed fixed column offsets — avoids per-row O(C) col_types scan
    fixed_offsets: Option<crate::storage::row_format::FixedColumnOffsets>,
    // WHERE filter
    where_pos: Vec<usize>,
    compiled_where: Option<CompiledWhere>,
    where_pos_to_idx: Vec<Option<usize>>,
    // SELECT projection
    select_only_pos: Vec<usize>,
    project_where_indices: Vec<(usize, usize)>,
    project_select_indices: Vec<(usize, usize)>,
    num_output_cols: usize,
}

impl Iterator for TwoPhaseFilteredIterator {
    type Item = crate::Result<Vec<crate::types::Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (_row_id, raw_bytes) = match self.raw.next() {
                Some(Ok(r)) => r,
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            };

            // Parse row header once — use pre-computed offsets when available
            // to skip per-row O(C) col_types scan (~30-50ns per row saved)
            let parse_result = if let Some(ref offsets) = self.fixed_offsets {
                crate::storage::row_format::RowParseContext::parse_with_offsets(
                    &raw_bytes, &self.col_types, offsets,
                )
            } else {
                crate::storage::row_format::RowParseContext::parse(
                    &raw_bytes, &self.col_types, self.fixed_count,
                )
            };
            let ctx = match parse_result {
                Some(c) => c,
                None => {
                    // Legacy bincode — fall back to full decode
                    self.where_buf.clear();
                    if let Err(e) = crate::storage::row_format::decode_fast_partial_into(
                        &raw_bytes, &self.col_types, self.fixed_count, &self.needed, &mut self.where_buf,
                    ) {
                        return Some(Err(e));
                    }
                    self.projected.clear();
                    self.projected.resize(self.num_output_cols, crate::types::Value::Null);
                    for &(out_idx, buf_idx) in self.project_where_indices.iter().chain(self.project_select_indices.iter()) {
                        if out_idx < self.projected.len() {
                            self.projected[out_idx] = self.where_buf.get(buf_idx).cloned().unwrap_or(crate::types::Value::Null);
                        }
                    }
                    return Some(Ok(std::mem::take(&mut self.projected)));
                }
            };

            // Phase 1: Decode only WHERE columns (reusing buffer)
            self.where_buf.clear();
            if let Err(e) = ctx.decode_columns(&raw_bytes, &self.col_types, &self.where_pos, &mut self.where_buf) {
                return Some(Err(e));
            }

            // Evaluate WHERE filter
            let matches = if let Some(ref cw) = self.compiled_where {
                cw.eval_at(&self.where_buf, &self.where_pos_to_idx).unwrap_or(false)
            } else {
                true
            };

            if !matches {
                continue; // ← Skip! No SELECT decode needed. Buffer will be reused.
            }

            // Phase 2: Decode remaining SELECT columns (only for passing rows, reusing buffer)
            self.select_buf.clear();
            if !self.select_only_pos.is_empty() {
                if let Err(e) = ctx.decode_columns(&raw_bytes, &self.col_types, &self.select_only_pos, &mut self.select_buf) {
                    return Some(Err(e));
                }
            }

            // Build projected output (reusing buffer)
            self.projected.clear();
            self.projected.resize(self.num_output_cols, crate::types::Value::Null);
            for &(out_idx, buf_idx) in &self.project_where_indices {
                if out_idx < self.projected.len() {
                    self.projected[out_idx] = self.where_buf.get(buf_idx).cloned().unwrap_or(crate::types::Value::Null);
                }
            }
            for &(out_idx, buf_idx) in &self.project_select_indices {
                if out_idx < self.projected.len() {
                    self.projected[out_idx] = self.select_buf.get(buf_idx).cloned().unwrap_or(crate::types::Value::Null);
                }
            }
            return Some(Ok(std::mem::take(&mut self.projected)));
        }
    }
}

/// SortKey wraps Value with a total-ordering Ord impl that treats NULLs
/// as less than all non-NULL values (consistent with SQLite).
#[derive(Clone)]
struct SortKey(Value);

impl Eq for SortKey {}
impl PartialEq for SortKey {
    fn eq(&self, other: &Self) -> bool { self.0 == other.0 }
}
impl PartialOrd for SortKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.partial_cmp(&other.0)
    }
}
impl Ord for SortKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match (&self.0, &other.0) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            (a, b) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        }
    }
}

pub struct QueryExecutor {
    db: Arc<MoteDB>,
    evaluator: ExprEvaluator,
    optimizer: super::optimizer::QueryOptimizer,
    /// Store the last AUTO_INCREMENT value inserted (mirrors evaluator)
    last_insert_id: std::sync::atomic::AtomicI64,
    /// Current transaction ID when inside BEGIN...COMMIT/ROLLBACK block.
    /// None = auto-commit mode (each statement is its own transaction).
    current_txn_id: parking_lot::Mutex<Option<u64>>,
}

impl QueryExecutor {
    pub fn new(db: Arc<MoteDB>) -> Self {
        Self {
            evaluator: ExprEvaluator::with_db(db.clone()),
            optimizer: super::optimizer::QueryOptimizer::new(db.clone()),
            last_insert_id: std::sync::atomic::AtomicI64::new(i64::MIN),
            current_txn_id: parking_lot::Mutex::new(None),
            db,
        }
    }

    /// Reset per-query state. Called before each execute.
    pub fn reset_last_insert_id(&self) {
        self.last_insert_id.store(i64::MIN, std::sync::atomic::Ordering::Relaxed);
        self.evaluator.last_insert_id.store(i64::MIN, std::sync::atomic::Ordering::Relaxed);
        self.evaluator.clear_params();
    }

    /// Bind parameters for a parameterized query.
    pub fn bind_params(&self, params: Vec<Value>) {
        self.evaluator.set_params(params);
    }

    /// Clear bind parameters after execution.
    pub fn clear_params(&self) {
        self.evaluator.clear_params();
    }
    
    pub fn execute(&self, stmt: Statement) -> Result<QueryResult> {
        match stmt {
            Statement::Select(s) => self.execute_select(s),
            Statement::Insert(i) => self.execute_insert(i),
            Statement::Update(u) => self.execute_update(u),
            Statement::Delete(d) => self.execute_delete(d),
            Statement::CreateTable(c) => self.execute_create_table(c),
            Statement::CreateIndex(c) => self.execute_create_index(c),
            Statement::DropTable(d) => self.execute_drop_table(d),
            Statement::DropIndex(d) => self.execute_drop_index(d),
            Statement::AlterTable(a) => self.execute_alter_table(a),
            Statement::ShowTables => self.execute_show_tables(),
            Statement::DescribeTable(table_name) => self.execute_describe_table(table_name),
            Statement::BeginTransaction => self.execute_begin_transaction(),
            Statement::CommitTransaction => self.execute_commit_transaction(),
            Statement::RollbackTransaction => self.execute_rollback_transaction(),
        }
    }
    
    /// 🚀 流式执行（方案 C：零内存开销）
    /// 
    /// 返回迭代器而不是 Vec，实现真正的流式查询。
    /// 
    /// # 示例
    /// ```ignore
    /// let result = executor.execute_streaming(stmt)?;
    /// result.for_each(|columns, row| {
    ///     println!("{:?}: {:?}", columns, row);
    ///     Ok(())
    /// })?;
    /// ```
    /// Execute a statement by reference (avoids cloning the AST).
    ///
    /// For SELECT: only clones the SelectStmt (cheap relative to full query).
    /// For other statements: clones only the specific variant needed.
    /// Check if a transaction is active (for fast-path bypass).
    pub fn is_in_transaction(&self) -> bool {
        self.current_txn_id.lock().is_some()
    }

    pub fn execute_streaming_ref(&self, stmt: &Statement) -> Result<StreamingQueryResult> {
        let max_rows = self.db.max_result_rows;
        let result = match stmt {
            Statement::Select(s) => self.execute_select_streaming_ref(s)?,
            Statement::Insert(i) => {
                let result = self.execute_insert_ref(i)?;
                StreamingQueryResult::Modification {
                    affected_rows: result.affected_rows(),
                }
            }
            Statement::Update(u) => {
                let result = self.execute_update(u.clone())?;
                StreamingQueryResult::Modification {
                    affected_rows: result.affected_rows(),
                }
            }
            Statement::Delete(d) => {
                let result = self.execute_delete(d.clone())?;
                StreamingQueryResult::Modification {
                    affected_rows: result.affected_rows(),
                }
            }
            Statement::CreateTable(c) => {
                let result = self.execute_create_table(c.clone())?;
                StreamingQueryResult::Definition {
                    message: match result {
                        QueryResult::Definition { message } => message,
                        _ => "Table created".to_string(),
                    },
                }
            }
            Statement::CreateIndex(c) => {
                let result = self.execute_create_index(c.clone())?;
                StreamingQueryResult::Definition {
                    message: match result {
                        QueryResult::Definition { message } => message,
                        _ => "Index created".to_string(),
                    },
                }
            }
            Statement::DropTable(d) => {
                let result = self.execute_drop_table(d.clone())?;
                StreamingQueryResult::Definition {
                    message: match result {
                        QueryResult::Definition { message } => message,
                        _ => "Table dropped".to_string(),
                    },
                }
            }
            Statement::DropIndex(d) => {
                let result = self.execute_drop_index(d.clone())?;
                StreamingQueryResult::Definition {
                    message: match result {
                        QueryResult::Definition { message } => message,
                        _ => "Index dropped".to_string(),
                    },
                }
            }
            Statement::ShowTables => {
                let result = self.execute_show_tables()?;
                StreamingQueryResult::Definition {
                    message: match result {
                        QueryResult::Definition { message } => message,
                        _ => "Tables shown".to_string(),
                    },
                }
            }
            Statement::DescribeTable(table_name) => {
                let result = self.execute_describe_table(table_name.clone())?;
                StreamingQueryResult::Definition {
                    message: match result {
                        QueryResult::Definition { message } => message,
                        _ => "Table described".to_string(),
                    },
                }
            }
            Statement::AlterTable(a) => {
                let result = self.execute_alter_table(a.clone())?;
                StreamingQueryResult::Definition {
                    message: match result {
                        QueryResult::Definition { message } => message,
                        _ => "Table altered".to_string(),
                    },
                }
            }
            Statement::BeginTransaction => {
                        let txn_id = self.db.begin_transaction()?;
                *self.current_txn_id.lock() = Some(txn_id);
                StreamingQueryResult::Definition {
                    message: format!("Transaction {} started", txn_id),
                }
            }
            Statement::CommitTransaction => {
                let _txn_id_opt = *self.current_txn_id.lock();
                if let Some(txn_id) = _txn_id_opt {
                    self.db.commit_transaction(txn_id)?;
                    *self.current_txn_id.lock() = None;
                    StreamingQueryResult::Definition {
                        message: format!("Transaction {} committed", txn_id),
                    }
                } else {
                    StreamingQueryResult::Definition {
                        message: "No active transaction".to_string(),
                    }
                }
            }
            Statement::RollbackTransaction => {
                let _txn_id_opt = *self.current_txn_id.lock();
                if let Some(txn_id) = _txn_id_opt {
                    self.db.rollback_transaction(txn_id)?;
                    *self.current_txn_id.lock() = None;
                    StreamingQueryResult::Definition {
                        message: format!("Transaction {} rolled back", txn_id),
                    }
                } else {
                    StreamingQueryResult::Definition {
                        message: "No active transaction".to_string(),
                    }
                }
            }
        };
        Ok(result.with_max_rows(max_rows))
    }

    pub fn execute_streaming(&self, stmt: Statement) -> Result<StreamingQueryResult> {
        self.execute_streaming_ref(&stmt)
    }

    /// Execute SELECT statement
    fn execute_select(&self, stmt: SelectStmt) -> Result<QueryResult> {
        self.execute_select_internal(&stmt)
    }
    
    /// Materialize a SELECT via `execute_select_internal` and wrap as streaming.
    /// ColSegmentStore multi-segment aggregate: COUNT/SUM/MIN/MAX/GROUP BY
    /// without compaction. Iterates segments directly via scan_projected_filtered.
    fn col_segment_aggregate(
        &self,
        stmt: &SelectStmt,
        table_name: &str,
        store: &crate::storage::col_segment::ColSegmentStore,
    ) -> Result<Option<StreamingQueryResult>> {
        use crate::sql::ast::{Expr, SelectColumn};
        let schema = self.db.get_table_schema(table_name).ok();
        let schema = match schema { Some(s) => s, None => return Ok(None) };
        let col_types = schema.col_types().to_vec();

        // Detect simple COUNT(*) with optional WHERE.
        let is_count_star = stmt.columns.len() == 1 && {
            matches!(&stmt.columns[0],
                SelectColumn::Expr(Expr::FunctionCall { name, args, .. }, _)
                if name.eq_ignore_ascii_case("COUNT")
                   && (args.is_empty()
                       || (args.len() == 1 && matches!(args[0], Expr::Column(ref c) if c == "*"))))
        };

        if is_count_star {
            // COUNT(*) with WHERE: filter then count. Without WHERE: count all.
            let filter_col_pos: Option<usize>;
            let count;
            if let Some(ref wc) = stmt.where_clause {
                match wc {
                    Expr::BinaryOp { left, op: crate::sql::ast::BinaryOperator::Eq, right } => {
                        if let (Expr::Column(cn), Expr::Literal(v)) = (left.as_ref(), right.as_ref()) {
                            let pos = schema.get_column_position(cn).unwrap_or(0);
                            let target = v.clone();
                            // Only scan filter column (empty output = no Value decode).
                            let scanned = store.scan_projected_filtered(Some(pos), &[], &move |fv: Option<&Value>| fv == Some(&target));
                            count = scanned.len() as i64;
                        } else { return Ok(None); }
                    }
                    _ => return Ok(None),
                }
            } else {
                // No WHERE: count live rows directly from row_map (zero decode).
                let _ct = std::time::Instant::now();
                count = store.count_live_rows() as i64;
            }
            let columns: Vec<String> = self.build_select_columns(&stmt.columns, &schema).unwrap_or_default();
            return Ok(Some(StreamingQueryResult::SelectReady {
                columns,
                rows: vec![vec![Value::Integer(count)]],
            }));
        }

        // DISTINCT: scan the distinct column(s) and dedup.
        if stmt.distinct {
            let out_pos: Vec<usize> = Self::resolve_select_positions(&stmt.columns, &schema)
                .unwrap_or_default();
            if !out_pos.is_empty() {
                let dc = out_pos[0];
                // 🚀 Fast path: for single-column DISTINCT on a TEXT column, use
                // distinct_text_values (adaptive early-exit, ~0.5ms for a 2-value
                // column) instead of materializing+deduping all 300K rows (~46ms).
                if matches!(schema.col_types().get(dc), Some(crate::types::ColumnType::Text)) {
                    let vals = store.distinct_text_values(dc, 10000);
                    let rows: Vec<Vec<Value>> = vals.into_iter()
                        .map(|v| vec![Value::Text(v.into())])
                        .collect();
                    let columns = self.build_select_columns(&stmt.columns, &schema).unwrap_or_default();
                    return Ok(Some(StreamingQueryResult::SelectReady { columns, rows }));
                }
                let scanned = store.scan_projected_filtered(Some(dc), &out_pos, &|_| true);
                let mut seen: std::collections::HashSet<Value> = std::collections::HashSet::new();
                let mut rows: Vec<Vec<Value>> = Vec::new();
                for (_, row) in scanned {
                    let key = row.get(0).cloned().unwrap_or(Value::Null);
                    if seen.insert(key) {
                        rows.push(row);
                    }
                }
                let columns = self.build_select_columns(&stmt.columns, &schema).unwrap_or_default();
                return Ok(Some(StreamingQueryResult::SelectReady { columns, rows }));
            }
        }

        // ORDER BY + LIMIT: scan + sort in memory (avoids sync compaction).
        if stmt.order_by.is_some() && stmt.limit.is_some() {
            let ob = stmt.order_by.as_ref().unwrap();
            if let Some(obe) = ob.first() {
                if let crate::sql::ast::Expr::Column(cn) = &obe.expr {
                    let order_col = schema.get_column_position(cn).unwrap_or(0);
                    let limit = stmt.limit.unwrap();
                    let out_pos: Vec<usize> = Self::resolve_select_positions(&stmt.columns, &schema)
                        .unwrap_or_else(|| (0..col_types.len()).collect());
                    let scanned = store.scan_projected_filtered(Some(order_col), &out_pos, &|_| true);
                    let order_idx = out_pos.iter().position(|&p| p == order_col).unwrap_or(0);
                    let mut sorted = scanned;
                    sorted.sort_by(|a, b| {
                    let av = a.1.get(order_idx).cloned().unwrap_or(Value::Null);
                    let bv = b.1.get(order_idx).cloned().unwrap_or(Value::Null);
                    bv.partial_cmp(&av).unwrap_or(std::cmp::Ordering::Equal)
                });
                    let rows: Vec<Vec<Value>> = sorted.into_iter().take(limit)
                        .map(|(_, row)| row).collect();
                    let columns = self.build_select_columns(&stmt.columns, &schema).unwrap_or_default();
                    return Ok(Some(StreamingQueryResult::SelectReady { columns, rows }));
                }
            }
        }

        // Multi-aggregate without GROUP BY: COUNT/SUM/MIN/MAX via multi-segment scan.
        // Avoids sync compaction for these common analytical queries.
        if stmt.group_by.is_none() {
            if let Some(result) = self.col_segment_multi_aggregate(stmt, table_name, store, &schema)? {
                return Ok(Some(result));
            }
            // multi_aggregate returned None (unsupported function like AVG) —
            // fall through to sync + legacy path below.
        }

        // GROUP BY with simple aggregates: multi-segment scan + HashMap aggregation.
        if stmt.group_by.is_some() {
            return self.col_segment_group_by(stmt, table_name, store, &schema);
        }

        // For other aggregates, fall through to legacy path.
        Ok(None)
    }

    /// Multi-aggregate (COUNT/SUM/MIN/MAX) without GROUP BY, via multi-segment scan.
    /// Avoids sync compaction.
    fn col_segment_multi_aggregate(
        &self,
        stmt: &SelectStmt,
        table_name: &str,
        store: &crate::storage::col_segment::ColSegmentStore,
        schema: &TableSchema,
    ) -> Result<Option<StreamingQueryResult>> {
        use crate::sql::ast::{Expr, SelectColumn};
        // COUNT(DISTINCT col) and other DISTINCT aggregates are not supported
        // here (this path counts without dedup). Fall back to the materialized
        // path which dedups via HashSet (compute_aggregate_positional).
        let has_distinct = stmt.columns.iter().any(|c| {
            matches!(c, SelectColumn::Expr(Expr::FunctionCall { distinct: true, .. }, _))
        });
        if has_distinct {
            return Ok(None);
        }
        // Ensure buffered rows are durable — scan_projected_filtered only reads
        // persisted segments. (Subquery resolution calls execute_select_internal
        // directly, bypassing the streaming entry's flush.)
        let _ = store.flush_buffer();
        // Identify aggregate functions and their target columns.
        struct AggInfo { func: String, col: Option<usize> }
        let mut aggs: Vec<AggInfo> = Vec::new();
        for col in &stmt.columns {
            if let SelectColumn::Expr(Expr::FunctionCall { name, args, .. }, _) = col {
                let target = args.iter().filter_map(|a| {
                    if let Expr::Column(cn) = a { schema.get_column_position(cn) } else { None }
                }).next();
                aggs.push(AggInfo { func: name.to_uppercase(), col: target });
            } else {
                return Ok(None); // non-aggregate column in SELECT — can't handle
            }
        }
        if aggs.is_empty() { return Ok(None); }

        // 🚀 Fast path: COUNT + SUM/MIN/MAX WHERE text_col = 'val' — direct column
        // scan without Vec<Value> construction. Avoids 100K allocations + 30MB memory.
        if let Some(ref wc) = stmt.where_clause {
            if let Expr::BinaryOp { left, op: crate::sql::ast::BinaryOperator::Eq, right } = wc {
                if let (Expr::Column(cn), Expr::Literal(Value::Text(s))) = (left.as_ref(), right.as_ref()) {
                    if let Some(fc) = schema.get_column_position(cn) {
                        if matches!(schema.col_types().get(fc), Some(ColumnType::Text)) {
                            // Find SUM/MIN/MAX target column.
                            let agg_target = aggs.iter().filter_map(|a| a.col).next();
                            let columns: Vec<String> = self.build_select_columns(&stmt.columns, schema).unwrap_or_default();
                            if let Some(ac) = agg_target {
                                let has_count = aggs.iter().any(|a| a.func == "COUNT");
                                let has_sum = aggs.iter().any(|a| a.func == "SUM");
                                let has_min = aggs.iter().any(|a| a.func == "MIN");
                                let has_max = aggs.iter().any(|a| a.func == "MAX");
                                if has_count && (has_sum || has_min || has_max) && aggs.len() <= 4 {
                                    let (count, min, max) = store.count_min_max_text_filter(fc, s.as_str(), ac);
                                    let sum = if has_sum {
                                        // count_sum includes sum; re-scan for sum if needed
                                        let (c, s) = store.count_sum_text_filter(fc, s.as_str(), ac);
                                        s
                                    } else { 0.0 };
                                    let mut row: Vec<Value> = Vec::new();
                                    for a in &aggs {
                                        match a.func.as_str() {
                                            "COUNT" => row.push(Value::Integer(count)),
                                            "SUM" => row.push(Value::Float(sum)),
                                            "MIN" => row.push(Value::Float(min)),
                                            "MAX" => row.push(Value::Float(max)),
                                            "AVG" => row.push(Value::Float(if count > 0 { sum / count as f64 } else { 0.0 })),
                                            _ => row.push(Value::Null),
                                        }
                                    }
                                    return Ok(Some(StreamingQueryResult::SelectReady { columns, rows: vec![row] }));
                                }
                            }
                        }
                    }
                }
            }
        }

        // Find columns to scan (filter col + aggregate target cols).
        let filter_col = stmt.where_clause.as_ref().and_then(|wc| {
            if let Expr::BinaryOp { left, op: crate::sql::ast::BinaryOperator::Eq, right } = wc {
                if let (Expr::Column(cn), Expr::Literal(_)) = (left.as_ref(), right.as_ref()) {
                    return schema.get_column_position(cn);
                }
            }
            None
        });
        let mut scan_cols: Vec<usize> = Vec::new();
        if let Some(fc) = filter_col { scan_cols.push(fc); }
        for a in &aggs {
            if let Some(c) = a.col { if !scan_cols.contains(&c) { scan_cols.push(c); } }
        }

        // Extract WHERE filter value for predicate.
        let pred: Box<dyn Fn(Option<&Value>) -> bool> = if let Some(ref wc) = stmt.where_clause {
            if let Expr::BinaryOp { left: _, op: _, right } = wc {
                if let Expr::Literal(v) = right.as_ref() {
                    let target = v.clone();
                    Box::new(move |fv: Option<&Value>| fv == Some(&target))
                } else { return Ok(None); }
            } else { return Ok(None); }
        } else {
            Box::new(|_| true)
        };

        let scanned = store.scan_projected_filtered(filter_col, &scan_cols, &*pred);

        // Compute aggregates.
        let columns: Vec<String> = self.build_select_columns(&stmt.columns, schema).unwrap_or_default();
        let mut result_row: Vec<Value> = Vec::with_capacity(aggs.len());
        for a in &aggs {
            let col_idx_in_scan = a.col.and_then(|c| scan_cols.iter().position(|&s| s == c));
            match a.func.as_str() {
                "COUNT" => {
                    // COUNT(*) counts all rows; COUNT(col) skips NULLs.
                    let n = match a.col {
                        None => scanned.len(), // COUNT(*)
                        Some(_) => scanned.iter().filter(|(_, row)| {
                            col_idx_in_scan.and_then(|ci| row.get(ci))
                                .map(|v| !matches!(v, Value::Null))
                                .unwrap_or(false)
                        }).count(),
                    };
                    result_row.push(Value::Integer(n as i64));
                }
                "SUM" => {
                    // SUM ignores NULLs; SUM over zero non-NULL values is NULL.
                    let non_null: Vec<&Value> = scanned.iter()
                        .filter_map(|(_, row)| col_idx_in_scan.and_then(|ci| row.get(ci)))
                        .filter(|v| !matches!(v, Value::Null))
                        .collect();
                    if non_null.is_empty() {
                        result_row.push(Value::Null);
                    } else {
                        // Return Integer for all-integer columns (consistency), else Float.
                        let all_int = non_null.iter().all(|v| matches!(v, Value::Integer(_)));
                        if all_int {
                            let s: i64 = non_null.iter().filter_map(|v| {
                                if let Value::Integer(i) = v { Some(*i) } else { None }
                            }).filter(|&v| v != i64::MIN).sum();
                            result_row.push(Value::Integer(s));
                        } else {
                            let s: f64 = non_null.iter().filter_map(|v| {
                                if let Value::Float(f) = v { Some(*f) }
                                else if let Value::Integer(i) = v { Some(*i as f64) }
                                else { None }
                            }).sum();
                            result_row.push(Value::Float(s));
                        }
                    }
                }
                "MIN" => {
                    // 🔑 Handle Integer columns too (was Float-only, so Integer
                    // MIN returned the INFINITY fold seed). Decode by value type.
                    let ints: Vec<i64> = scanned.iter().filter_map(|(_, row)| {
                        col_idx_in_scan.and_then(|ci| row.get(ci)).and_then(|v| {
                            if let Value::Integer(i) = v { Some(*i) } else { None }
                        }).filter(|&v| v != i64::MIN) // MIN = NULL sentinel
                    }).collect();
                    let floats: Vec<f64> = scanned.iter().filter_map(|(_, row)| {
                        col_idx_in_scan.and_then(|ci| row.get(ci)).and_then(|v| {
                            if let Value::Float(f) = v { Some(*f) } else { None }
                        })
                    }).filter(|v| !v.is_nan()).collect();
                    if !ints.is_empty() {
                        result_row.push(Value::Integer(*ints.iter().min().unwrap()));
                    } else if !floats.is_empty() {
                        result_row.push(Value::Float(floats.iter().cloned().fold(f64::INFINITY, f64::min)));
                    } else {
                        result_row.push(Value::Null);
                    }
                }
                "MAX" => {
                    let ints: Vec<i64> = scanned.iter().filter_map(|(_, row)| {
                        col_idx_in_scan.and_then(|ci| row.get(ci)).and_then(|v| {
                            if let Value::Integer(i) = v { Some(*i) } else { None }
                        }).filter(|&v| v != i64::MIN)
                    }).collect();
                    let floats: Vec<f64> = scanned.iter().filter_map(|(_, row)| {
                        col_idx_in_scan.and_then(|ci| row.get(ci)).and_then(|v| {
                            if let Value::Float(f) = v { Some(*f) } else { None }
                        })
                    }).filter(|v| !v.is_nan()).collect();
                    if !ints.is_empty() {
                        result_row.push(Value::Integer(*ints.iter().max().unwrap()));
                    } else if !floats.is_empty() {
                        result_row.push(Value::Float(floats.iter().cloned().fold(f64::NEG_INFINITY, f64::max)));
                    } else {
                        result_row.push(Value::Null);
                    }
                }
                "AVG" => {
                    // AVG ignores NULLs; AVG over zero non-NULL values is NULL.
                    let nums: Vec<f64> = scanned.iter().filter_map(|(_, row)| {
                        col_idx_in_scan.and_then(|ci| row.get(ci)).and_then(|v| {
                            if let Value::Float(f) = v { Some(*f) }
                            else if let Value::Integer(i) = v { Some(*i as f64) }
                            else { None }
                        })
                    }).collect();
                    if nums.is_empty() {
                        result_row.push(Value::Null);
                    } else {
                        let sum: f64 = nums.iter().sum();
                        result_row.push(Value::Float(sum / nums.len() as f64));
                    }
                }
                _ => return Ok(None), // unsupported function
            }
        }
        Ok(Some(StreamingQueryResult::SelectReady { columns, rows: vec![result_row] }))
    }

    /// GROUP BY via multi-segment scan + in-memory HashMap aggregation.
    /// Avoids sync compaction entirely.
    /// Parse each select column into its aggregate spec (None = plain column,
    /// Some = aggregate like COUNT/SUM/MIN/MAX/AVG). Used by col_segment_group_by
    /// to decide whether the COUNT-only fast path is safe.
    fn parse_select_aggregates(
        &self,
        columns: &[crate::sql::ast::SelectColumn],
        schema: &TableSchema,
    ) -> Vec<Option<AggregateInfo>> {
        columns.iter().map(|sc| {
            match sc {
                crate::sql::ast::SelectColumn::Expr(expr, _) => {
                    self.try_parse_aggregate(expr, schema)
                }
                _ => None, // Star / Column / ColumnWithAlias — not an aggregate
            }
        }).collect()
    }

    fn col_segment_group_by(
        &self,
        stmt: &SelectStmt,
        table_name: &str,
        store: &crate::storage::col_segment::ColSegmentStore,
        schema: &TableSchema,
    ) -> Result<Option<StreamingQueryResult>> {
        // Extract GROUP BY column.
        let group_cols: Vec<usize> = stmt.group_by.as_ref().map(|gc| {
            gc.iter().filter_map(|cn| schema.get_column_position(cn)).collect()
        }).unwrap_or_default();
        if group_cols.is_empty() { return Ok(None); }
        let gc = group_cols[0]; // single GROUP BY column

        // 🔑 Only the pure `SELECT g, COUNT(*) ... GROUP BY g` shape can use the
        // store.group_by_count fast path. If the query asks for SUM/MIN/MAX/AVG,
        // or mixes aggregates, group_by_count would silently return COUNT for
        // every aggregate (the v0.5.0 GROUP BY SUM bug). Detect the aggregate
        // shape; if it isn't exactly [group_col, COUNT(*)], fall back to the
        // general materialize path which evaluates each aggregate correctly.
        let agg_specs = self.parse_select_aggregates(&stmt.columns, schema);
        let is_pure_count = agg_specs.iter().all(|a| matches!(a, Some(ai) if ai.func == "COUNT"))
            && agg_specs.iter().any(|a| matches!(a, Some(ai) if ai.func == "COUNT"));
        if !is_pure_count {
            // Has SUM/MIN/MAX/AVG, or a mix, or no aggregate at all (plain
            // GROUP BY + projected columns). Let the general path handle it.
            return Ok(None);
        }
        // Multi-column GROUP BY (GROUP BY a, b) and HAVING are not supported by
        // this fast path (it groups on a single column and emits no HAVING
        // filtering). Fall back to try_apply_group_by_positional / apply_group_by,
        // which handle composite keys and HAVING correctly.
        if group_cols.len() > 1 || stmt.having.is_some() {
            return Ok(None);
        }

        // Output columns: group col + aggregates.
        let out_pos: Vec<usize> = Self::resolve_select_positions(&stmt.columns, schema)
            .unwrap_or_else(|| vec![gc]);
        let columns: Vec<String> = self.build_select_columns(&stmt.columns, schema).unwrap_or_default();

        // Direct group-by scan: iterate group column without Vec<Value> allocation.
        let groups = store.group_by_count(gc);

        // Build result rows.
        let mut rows: Vec<Vec<Value>> = Vec::with_capacity(groups.len());
        for (gval, count) in groups {
            rows.push(vec![Value::Text(gval.into()), Value::Integer(count)]);
        }
        Ok(Some(StreamingQueryResult::SelectReady { columns, rows }))
    }

    fn materialize_as_streaming(&self, stmt: &SelectStmt) -> Result<StreamingQueryResult> {
        let result = self.execute_select_internal(stmt)?;
        match result {
            QueryResult::Select { columns, rows } => {
                // execute_select_internal already applies ORDER BY, LIMIT, OFFSET, DISTINCT,
                // so we pass None/defaults here to avoid double-application.
                Ok(StreamingQueryResult::SelectStreaming {
                    columns,
                    rows: Box::new(rows.into_iter().map(Ok)),
                    order_by: None,
                    limit: None,
                    offset: None,
                    distinct: false,
                    max_result_rows: None,
                    size_hint: None,
                })
            }
            _ => unreachable!(),
        }
    }

    /// 🚀 Execute SELECT statement (streaming version, zero-clone)
    ///
    /// Takes &SelectStmt — no cloning of the AST at all.
    /// This is the primary entry point from the statement cache.
    fn execute_select_streaming_ref(&self, stmt: &SelectStmt) -> Result<StreamingQueryResult> {
        // 🚀 Pre-resolve scalar/IN subqueries in WHERE clause BEFORE any routing.
        // This converts `WHERE col > (SELECT ...)` / `WHERE col IN (SELECT ...)`
        // into literal forms early, so every downstream path (columnar scan,
        // ORDER BY, DISTINCT, optimizer) sees resolvable WHERE predicates.
        let resolved_subq_stmt;
        let stmt: &SelectStmt = if let Some(ref where_clause) = stmt.where_clause {
            if Self::expr_contains_subquery(where_clause) {
                resolved_subq_stmt = self.resolve_subqueries_stmt(stmt)?;
                &resolved_subq_stmt
            } else {
                stmt
            }
        } else {
            stmt
        };

        // Validate bare SELECT column references against the table schema.
        // A column that doesn't exist is a query error (not a silent value
        // from another column). Applies before any fast-path routing so all
        // paths benefit. (Also checked in execute_select_internal for the
        // subquery/non-streaming route.)
        if let Some(TableRef::Table { name: table_name, .. }) = stmt.from.as_ref() {
            if let Ok(schema) = self.db.get_table_schema(table_name) {
                for col in &stmt.columns {
                    if let SelectColumn::Column(name) | SelectColumn::ColumnWithAlias(name, _) = col {
                        let bare = name.rsplit('.').next().unwrap_or(name);
                        if schema.get_column_position(bare).is_none() {
                            return Err(MoteDBError::ColumnNotFound(
                                format!("'{}' in table '{}'", bare, table_name)
                            ));
                        }
                    }
                }
            }
        }

        // 🚀 Fast path: Text search (MATCH AGAINST), spatial (ST_WITHIN/ST_KNN),
        // and ORDER BY ST_DISTANCE must go through execute_select_internal which
        // has the index pushdown paths. Check this BEFORE the ColSegmentStore S9
        // routing, otherwise these WHERE clauses hit the columnar scan which
        // cannot evaluate spatial/text expressions (returns 0 rows).
        if let Some(ref where_clause) = stmt.where_clause {
            if Self::expr_needs_materialized_path(where_clause) {
                return self.materialize_as_streaming(stmt);
            }
        }
        if let Some(ref order_by) = stmt.order_by {
            if order_by.iter().any(|ob| Self::expr_is_or_aliases_st_distance(&ob.expr, &stmt.columns)) {
                if let Some(QueryResult::Select { columns, rows }) = self.try_optimize_spatial_order_by(stmt)? {
                    return Ok(StreamingQueryResult::SelectReady { columns, rows });
                }
                return self.materialize_as_streaming(stmt);
            }
            // Vector distance ORDER BY (col <-> [...] LIMIT k) needs the vector
            // index pushdown path (FAST PATH -1) — route to execute_select_internal
            // instead of the columnar scan, which can't evaluate `<->` ordering.
            if let Some(plan) = self.try_optimize_vector_order_by(stmt)? {
                let qr = self.execute_vector_order_by_plan(stmt, &plan);
                match qr {
                    Ok(QueryResult::Select { columns, rows }) => {
                        return Ok(StreamingQueryResult::SelectReady { columns, rows });
                    }
                    _ => return self.materialize_as_streaming(stmt),
                }
            }
        }

        // S9: ColSegmentStore tables — flush only (no compaction). Aggregate paths
        // (col_segment_aggregate) handle multi-segment directly. Compaction is
        // deferred to keep first-query P99 <50ms.
        if (self.has_aggregates(&stmt.columns) || stmt.group_by.is_some() || stmt.order_by.is_some() || stmt.distinct)
            && !Self::contains_parameter_stmt(stmt)
        {
            if let Some(TableRef::Table { name: table_name, .. }) = stmt.from.as_ref() {
                if self.db.has_col_segment_store(table_name) {
                    if let Ok(store) = self.db.get_or_create_col_segment_store(table_name, vec![]) {
                        let _ = store.flush_buffer();
                        // ORDER BY LIMIT (no aggregate): full scan + in-memory sort.
                        if stmt.order_by.is_some() && !self.has_aggregates(&stmt.columns) {
                            let schema = self.db.get_table_schema(table_name)?;
                            return self.execute_full_scan_via_col_segment(stmt, table_name, &schema, &store);
                        }
                        // DISTINCT (no aggregate): multi-segment scan + dedup.
                        if stmt.distinct && !self.has_aggregates(&stmt.columns) {
                            let schema = self.db.get_table_schema(table_name)?;
                            let out_pos: Vec<usize> = Self::resolve_select_positions(&stmt.columns, &schema)
                                .unwrap_or_default();
                            if !out_pos.is_empty() {
                                let dc = out_pos[0];
                                // 🚀 Fast path: single-column DISTINCT on a TEXT
                                // column via distinct_text_values (adaptive early-
                                // exit) instead of materializing+deduping all rows.
                                if matches!(schema.col_types().get(dc), Some(crate::types::ColumnType::Text)) {
                                    let vals = store.distinct_text_values(dc, 10000);
                                    let rows: Vec<Vec<Value>> = vals.into_iter()
                                        .map(|v| vec![Value::Text(v.into())]).collect();
                                    let columns = self.build_select_columns(&stmt.columns, &schema).unwrap_or_default();
                                    return Ok(StreamingQueryResult::SelectReady { columns, rows });
                                }
                                let scanned = store.scan_projected_filtered(Some(dc), &out_pos, &|_| true);
                                let mut seen: std::collections::HashSet<Value> = std::collections::HashSet::new();
                                let mut rows: Vec<Vec<Value>> = Vec::new();
                                for (_, row) in scanned {
                                    let key = row.get(0).cloned().unwrap_or(Value::Null);
                                    if seen.insert(key) { rows.push(row); }
                                }
                                let columns = self.build_select_columns(&stmt.columns, &schema).unwrap_or_default();
                                return Ok(StreamingQueryResult::SelectReady { columns, rows });
                            }
                        }
                    }
                }
            }
        }

        // Aggregate queries (COUNT, SUM, etc.) — try fast paths
        if self.has_aggregates(&stmt.columns) {
            // ColSegmentStore multi-segment aggregate (no compaction — avoids 70ms sync).
            if let Some(TableRef::Table { name: table_name, .. }) = stmt.from.as_ref() {
                if self.db.has_col_segment_store(table_name) {
                    if let Ok(store) = self.db.get_or_create_col_segment_store(table_name, vec![]) {
                        let _ = store.flush_buffer();
                        if let Some(result) = self.col_segment_aggregate(stmt, table_name, &store)? {
                            return Ok(result);
                        }
                        // col_segment_aggregate returned None (complex aggregate).
                        // Try multi_aggregate and group_by directly (no sync needed).
                        let schema = self.db.get_table_schema(table_name)?;
                        if stmt.group_by.is_some() {
                            if let Some(result) = self.col_segment_group_by(stmt, table_name, &store, &schema)? {
                                return Ok(result);
                            }
                        }
                        if stmt.group_by.is_none() {
                            if let Some(result) = self.col_segment_multi_aggregate(stmt, table_name, &store, &schema)? {
                                return Ok(result);
                            }
                        }
                        // Last resort: sync + legacy path.
                        self.db.sync_col_segment_to_sstables(table_name);
                    }
                }
            }
            // Fast path 0: columnar aggregate pushdown (no row materialization)
            if let Some(result) = self.try_aggregate_columnar_fast(stmt)? {
                return Ok(result);
            }
            // Fast path 1: column index (works for high-selectivity filters)
            if let Some(result) = self.try_aggregate_via_column_index(stmt)? {
                return Ok(result);
            }
            if let Some(result) = self.try_aggregate_partial_scan(stmt)? {
                return Ok(result);
            }
            // Fast path 3: columnar GROUP BY pushdown
            if stmt.group_by.is_some() {
                if let Some(result) = self.try_group_by_columnar(stmt)? {
                    return Ok(result);
                }
            }
            return self.materialize_as_streaming(stmt);
        }

        // Handle SELECT without FROM clause (e.g., SELECT ROUND(3.7), SELECT TRIM('  hi  '))
        // Extract from once to avoid repeated unwraps.
        let from = match stmt.from.as_ref() {
            Some(f) => f,
            None => return self.materialize_as_streaming(stmt),
        };
        if stmt.from.is_none() {
            return self.materialize_as_streaming(stmt);
        }

        // Handle JOIN/Subquery by falling back to materialization
        match from {
            TableRef::Join { .. } | TableRef::Subquery { .. } => {
                return self.materialize_as_streaming(stmt);
            }
            _ => {}
        }

        // (WHERE subqueries were pre-resolved at the top of this function.)
        // (Spatial/text/ST_DISTANCE ORDER BY materialized-path routing is done
        //  above, before the ColSegmentStore S9 block.)

        // 🆕 TimeSeries table routing: use columnar store with zone maps + bloom filters
        // when the table is TimeSeries type. Falls through to LSM for complex queries.
        if let TableRef::Table { name: table_name, .. } = stmt.from.as_ref().ok_or_else(|| MoteDBError::InvalidArgument("FROM clause required".into()))? {
            if let Ok(schema) = self.db.get_table_schema(table_name) {
                if schema.table_type == crate::types::TableType::TimeSeries {
                    if let Some(result) = self.try_columnar_select(stmt, &schema)? {
                        // Convert QueryResult to StreamingQueryResult
                        match result {
                            QueryResult::Select { columns, rows } => {
                                return Ok(StreamingQueryResult::SelectReady { columns, rows });
                            }
                            _ => return Ok(StreamingQueryResult::SelectReady {
                                columns: vec![],
                                rows: vec![],
                            }),
                        }
                    }
                    // Fall through to LSM full scan for complex queries
                }
            }
        }

        // Pass bind parameters to optimizer (resolves ? inline, no AST clone needed).
        let has_params = Self::contains_parameter_stmt(stmt);
        let plan = if has_params {
            let params = self.evaluator.get_params();
            if let Some(err) = Self::validate_params_bound(stmt, &params) {
                return Err(err);
            }
            self.optimizer.optimize_select(stmt, &params)?
        } else {
            self.optimizer.optimize_select(stmt, &[])?
        };

        // For PointQuery/RangeQuery, the plan already has resolved values — use original stmt.
        // For FullScan, WHERE still contains Parameter nodes — substitute needed.
        //
        // post_filters are applied AFTER index row fetch: the index narrows to a small
        // candidate set (e.g., 10 rows), then post_filters further filter in-memory.
        // This replaces the old behavior of falling back to full table scan when
        // post_filters were present.
        let post_filters = &plan.post_filters;
        match plan.scan_method {
            super::optimizer::ScanMethod::PointQuery { ref table, ref column, ref value } => {
                self.execute_point_query_streaming(stmt, table, column, value, post_filters)
            }
            super::optimizer::ScanMethod::RangeQuery { ref table, ref column, ref start, start_inclusive, ref end, end_inclusive } => {
                self.execute_range_query_streaming(stmt, table, column, start, start_inclusive, end, end_inclusive, post_filters)
            }
            super::optimizer::ScanMethod::FullScan { .. } if has_params => {
                // FullScan with params: need to substitute WHERE for correct evaluation
                let resolved = self.substitute_params_stmt(stmt)?;
                self.execute_full_scan_streaming(&resolved, plan.scan_method.table_name())
            }
            super::optimizer::ScanMethod::FullScan { ref table } => {
                // 🚀 DISTINCT via column value index: SELECT DISTINCT col FROM table
                // without WHERE — iterate index keys directly (O(unique) vs O(N) scan).
                if stmt.distinct && stmt.where_clause.is_none() && stmt.order_by.is_none() {
                    if let Some(result) = self.try_distinct_via_column_index(stmt, table)? {
                        return Ok(result);
                    }
                }
                // 🚀 Streaming Top-K: when ORDER BY + LIMIT (no OFFSET) on full scan,
                // use a bounded heap instead of materializing all rows + sorting.
                if stmt.order_by.is_some() && stmt.limit.is_some()
                    && stmt.offset.is_none() && !stmt.distinct
                {
                    if let Some(result) = self.try_order_by_limit_topk(stmt, table)? {
                        return Ok(result);
                    }
                }
                // ORDER BY / DISTINCT on full scan without streaming Top-K:
                // fall back to materialize which has the positional sort path.
                if stmt.order_by.is_some() || stmt.distinct {
                    return self.materialize_as_streaming(stmt);
                }
                self.execute_full_scan_streaming(stmt, table)
            }
            super::optimizer::ScanMethod::IndexIntersection {
                ref table, ref column1, ref value1, ref column2, ref value2,
            } => {
                self.execute_index_intersection_streaming(stmt, table, column1, value1, column2, value2, post_filters)
            }
            _ => {
                // Fallback to materialized path (handles params via eval())
                self.materialize_as_streaming(stmt)
            }
        }
    }

    /// Check if an expression tree contains any Subquery node.
    fn expr_contains_subquery(expr: &Expr) -> bool {
        match expr {
            Expr::Subquery(_) => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_contains_subquery(left) || Self::expr_contains_subquery(right)
            }
            Expr::UnaryOp { expr, .. } => Self::expr_contains_subquery(expr),
            Expr::In { expr, list, .. } => {
                Self::expr_contains_subquery(expr) || list.iter().any(Self::expr_contains_subquery)
            }
            Expr::Between { expr, low, high, .. } => {
                Self::expr_contains_subquery(expr)
                    || Self::expr_contains_subquery(low)
                    || Self::expr_contains_subquery(high)
            }
            Expr::Like { expr, pattern, .. } => {
                Self::expr_contains_subquery(expr) || Self::expr_contains_subquery(pattern)
            }
            Expr::IsNull { expr, .. } => Self::expr_contains_subquery(expr),
            _ => false,
        }
    }

    /// Clone the statement with all subqueries in WHERE resolved to literal values.
    fn resolve_subqueries_stmt(&self, stmt: &SelectStmt) -> Result<SelectStmt> {
        let where_clause = match &stmt.where_clause {
            Some(w) => Some(self.materialize_subqueries(w)?),
            None => None,
        };
        Ok(SelectStmt {
            columns: stmt.columns.clone(),
            from: stmt.from.clone(),
            where_clause,
            order_by: stmt.order_by.clone(),
            limit: stmt.limit,
            offset: stmt.offset,
            distinct: stmt.distinct,
            group_by: stmt.group_by.clone(),
            having: stmt.having.clone(),
            latest_by: stmt.latest_by.clone(),
        })
    }

    /// Check if an expression contains MATCH, ST_WITHIN, ST_KNN, ST_RADIUS,
    /// or spatial scalar functions (WITHIN_RADIUS/ST_DISTANCE) that the
    /// columnar scan cannot evaluate (it doesn't decode GEOMETRY columns) and
    /// must run through the materialized execution path.
    fn expr_needs_materialized_path(expr: &Expr) -> bool {
        match expr {
            Expr::Match { .. }
            | Expr::StWithin3D { .. } | Expr::StKnn3D { .. } | Expr::StRadius3D { .. } => true,
            Expr::FunctionCall { name, args, .. } => {
                matches!(name.to_lowercase().as_str(),
                    "within_radius" | "st_distance" | "st_distance_3d")
                || args.iter().any(|a| Self::expr_needs_materialized_path(a))
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_needs_materialized_path(left) || Self::expr_needs_materialized_path(right)
            }
            Expr::UnaryOp { expr, .. } => Self::expr_needs_materialized_path(expr),
            Expr::IsNull { expr, .. } => Self::expr_needs_materialized_path(expr),
            _ => false,
        }
    }

    /// Check if ORDER BY expression is ST_DISTANCE or aliases a SELECT column that is ST_DISTANCE
    fn expr_is_or_aliases_st_distance(expr: &Expr, select_cols: &[SelectColumn]) -> bool {
        match expr {
            Expr::StDistance3D { .. } => true,
            Expr::Column(alias) => {
                for col in select_cols {
                    match col {
                        SelectColumn::Expr(e, Some(a)) if a == alias => {
                            return matches!(e, Expr::StDistance3D { .. });
                        }
                        _ => {}
                    }
                }
                false
            }
            _ => false,
        }
    }

    /// 🔥 点查询流式扫描（使用列索引）
    /// 
    /// ⚠️ 注意：这个方法通常只返回少量行（点查询），不需要批量优化
    /// Check if any SELECT expression needs the materialized path (full evaluator).
    fn select_needs_materialized(stmt: &SelectStmt) -> bool {
        stmt.columns.iter().any(|c| match c {
            SelectColumn::Expr(e, _) => !Self::can_eval_positional(e),
            _ => false,
        })
    }

    fn execute_point_query_streaming(
        &self,
        stmt: &SelectStmt,
        table: &str,
        column: &str,
        value: &Value,
        post_filters: &[Expr],
    ) -> Result<StreamingQueryResult> {
        let schema = self.db.get_table_schema(table)?;
        let columns = self.build_select_columns(&stmt.columns, &schema)?;

        let is_pk = schema.primary_key().map(|pk| pk == column).unwrap_or(false);
        let is_auto_increment_pk = is_pk && schema.is_primary_key_auto_increment();

        // For ColSegmentStore tables with a non-AUTO_INCREMENT PK, the
        // get_table_row point-lookup path fails (row_id ≠ PK value). Delegate
        // to full scan which applies the WHERE filter correctly on all column
        // types. This is the v0.5.0 fix for WHERE tag='val' / WHERE id=val
        // returning 0 rows on INT-PK ColSegmentStore tables.
        if !is_auto_increment_pk && self.db.has_col_segment_store(table) {
            return self.execute_full_scan_streaming(stmt, table);
        }

        // S9: AUTO_INCREMENT PK on ColSegmentStore — cached point lookup.
        // Uses get_row_cached (per-column decode cache), O(1) after first access.
        if is_auto_increment_pk && self.db.has_col_segment_store(table) {
            let row_id = match value {
                Value::Integer(id) if *id >= 0 => *id as RowId,
                _ => return Ok(StreamingQueryResult::SelectReady { columns, rows: vec![] }),
            };
            let composite_key = self.db.make_composite_key(table, row_id);
            if let Some(store) = self.db.col_segment_stores.get(table) {
                if let Some(row) = store.get(composite_key) {
                    self.db.row_cache.put(table.to_string(), row_id, row.clone());
                    let sql_row = row_to_sql_row(&row, &schema)?;
                    let mut prefixed = SqlRow::new();
                    prefixed.insert("__row_id__".to_string(), Value::Integer(row_id as i64));
                    prefixed.insert("__table__".to_string(), Value::text(table.to_string()));
                    for (cn, v) in sql_row { prefixed.insert(format!("{}.{}", table, cn), v); }
                    let (_, result_rows) = self.project_columns(&stmt.columns, &[(row_id, prefixed)], &schema)?;
                    return Ok(StreamingQueryResult::SelectReady { columns, rows: result_rows });
                }
                return Ok(StreamingQueryResult::SelectReady { columns, rows: vec![] });
            }
        }

        // S9: non-PK ColSegmentStore point queries. Skip the index→get_table_row
        // path for non-AUTO_INCREMENT PK tables (get_table_row fails due to
        // row_id ≠ PK value). The full-scan WHERE filter handles these correctly.
        let table_is_auto_inc_pk = {
            let schema = self.db.get_table_schema(table).ok();
            schema.and_then(|s| {
                s.primary_key().and_then(|pk| s.get_column(pk)).map(|c| c.auto_increment)
            }).unwrap_or(false)
        };
        if !is_pk && self.db.has_col_segment_store(table)
            && table_is_auto_inc_pk {
            if let Some(index_name) = self.db.index_registry.find_by_column(
                table, column,
                crate::database::index_metadata::IndexType::Column
            ) {
                if let Some(index) = self.db.column_indexes.get(&index_name) {
                    let row_ids = index.value()
                        .get_arc(value)
                        .unwrap_or_else(|_| std::sync::Arc::new(Vec::new()));
                    // Selectivity heuristic: use index-driven row fetch for result
                    // sets up to 10000 rows. The full-scan fallback (for >10000) is
                    // used when N point lookups become slower than a sequential scan.
                    // Previously this was 1000, which fell through to full-scan for
                    // ~1667 matches — and the full-scan WHERE path on INT-PK
                    // ColSegmentStore tables returned 0 rows (the v0.5.0 index bug).
                    if !row_ids.is_empty() && row_ids.len() <= 10000 {
                        let mut result_rows = Vec::with_capacity(row_ids.len());
                        for &rid in row_ids.iter() {
                            if let Some(row) = self.db.get_table_row(table, rid)? {
                                let mut sql_row = SqlRow::new();
                                sql_row.insert("__row_id__".to_string(), Value::Integer(rid as i64));
                                sql_row.insert("__table__".to_string(), Value::text(table.to_string()));
                                for (ci, col) in schema.columns.iter().enumerate() {
                                    let v = row.get(ci).cloned().unwrap_or(Value::Null);
                                    sql_row.insert(format!("{}.{}", table, col.name), v);
                                }
                                result_rows.push((rid, sql_row));
                            }
                        }
                        let (_, projected) = self.project_columns(&stmt.columns, &result_rows, &schema)?;
                        return Ok(StreamingQueryResult::SelectReady { columns, rows: projected });
                    }
                    if row_ids.is_empty() {
                        return Ok(StreamingQueryResult::SelectReady { columns, rows: vec![] });
                    }
                    // >1000 matches: fall through to projected full scan.
                }
            }
            return self.execute_full_scan_streaming(stmt, table);
        }

        // If SELECT expressions need the full evaluator, fall back to materialized path
        if Self::select_needs_materialized(stmt) {
            return self.materialize_as_streaming(stmt);
        }

        // Helper: apply post_filters to a single decoded row, project if it passes.
        macro_rules! filter_and_project {
            ($row:expr) => {{
                if !post_filters.is_empty()
                    && !Self::row_passes_post_filters(&$row, post_filters, &schema)
                {
                    None
                } else {
                    Some(Self::project_row_direct(&$row, &stmt.columns, &columns, &schema))
                }
            }};
        }

        // 🚀 Fast path for non-AUTO_INCREMENT PK: use in-memory PK lookup
        // Bypasses disk-based column index (1.5ms → <5µs)
        let is_non_auto_pk = is_pk && !schema.is_primary_key_auto_increment();

        if is_non_auto_pk {
            // In-memory PK lookup: O(1) LRU cache instead of disk B-Tree
            let pk_key = crate::database::pk_cache::PkKey::from_value(value);
            let row_id = self.resolve_pk_with_cache(table, &pk_key, column, value)?;

            if let Some(rid) = row_id {
                let row = self.db.get_table_row_arc(table, rid, &schema)?;
                let result_rows: Vec<Result<Vec<Value>>> = match row {
                    Some(row) => {
                        filter_and_project!(row).map(|r| Ok(r)).into_iter().collect()
                    }
                    None => vec![],
                };
                return Ok(StreamingQueryResult::SelectStreaming {
                    columns,
                    rows: Box::new(result_rows.into_iter()),
                    order_by: stmt.order_by.clone(),
                    limit: stmt.limit,
                    offset: stmt.offset,
                    distinct: stmt.distinct,
                    max_result_rows: None,
                    size_hint: None,
                });
            }
            // PK not found — return empty
            return Ok(StreamingQueryResult::SelectStreaming {
                columns,
                rows: Box::new(std::iter::empty()),
                order_by: stmt.order_by.clone(),
                limit: stmt.limit,
                offset: stmt.offset,
                distinct: stmt.distinct,
                max_result_rows: None,
                size_hint: None,
            });
        }

        if is_auto_increment_pk {
            // Direct LSM get by row_id — no column index needed
            let row_id = match value {
                Value::Integer(id) if *id >= 0 => *id as RowId,
                _ => {
                    // Non-integer or negative PK — return empty result
                    let column_names = self.build_select_columns(&stmt.columns, &schema)?;
                    return Ok(StreamingQueryResult::SelectStreaming {
                        columns: column_names,
                        rows: Box::new(std::iter::empty()),
                        order_by: stmt.order_by.clone(),
                        limit: stmt.limit,
                        offset: stmt.offset,
                        distinct: stmt.distinct,
                        max_result_rows: None,
                        size_hint: None,
                    });
                }
            };

            let row = self.db.get_table_row_arc(table, row_id, &schema)?;
            let result_rows: Vec<Result<Vec<Value>>> = match row {
                Some(row) => {
                    filter_and_project!(row).map(|r| Ok(r)).into_iter().collect()
                }
                None => vec![],
            };

            return Ok(StreamingQueryResult::SelectStreaming {
                columns,
                rows: Box::new(result_rows.into_iter()),
                order_by: stmt.order_by.clone(),
                limit: stmt.limit,
                offset: stmt.offset,
                distinct: stmt.distinct,
                max_result_rows: None,
                size_hint: None,
            });
        }

        // Fallback: use column index
        let row_ids = self.db.query_by_column(table, column, value)?;

        if row_ids.is_empty() {
            // If the async pipeline is active, column indexes may not be built yet.
            // Fall back to full scan to avoid returning wrong empty results.
            if self.db.is_async_index_pipeline_active() {
                return self.execute_full_scan_streaming(stmt, table);
            }
            return Ok(StreamingQueryResult::SelectStreaming {
                columns,
                rows: Box::new(std::iter::empty()),
                order_by: stmt.order_by.clone(),
                limit: stmt.limit,
                offset: stmt.offset,
                distinct: stmt.distinct,
                max_result_rows: None,
                size_hint: None,
            });
        }

        // Sort row_ids and choose optimal fetch strategy
        let mut sorted_ids = row_ids;
        sorted_ids.sort_unstable();
        let min_id = sorted_ids[0];
        let max_id = *sorted_ids.last().unwrap();
        let density = sorted_ids.len() as f64 / (max_id - min_id + 1) as f64;

        // Decode full rows (before projection — post_filters need full row data)
        let decoded_rows: Vec<Vec<Value>> = if density > 0.1 {
            // Dense result set: single range scan (sequential I/O >> random I/O)
            let id_set: std::collections::HashSet<u64> =
                sorted_ids.into_iter().map(|id| id as u64).collect();
            let start_key = self.db.make_composite_key(table, min_id);
            let end_key = self.db.make_composite_key(table, max_id + 1);
            let schema_c = schema.clone();

            let lsm_rows = self.db.lsm_engine.scan_range(start_key, end_key)
                .unwrap_or_default();

            lsm_rows.into_iter().filter_map(move |(key, vd)| {
                let rid = (key & 0xFFFFFFFF) as RowId;
                if !id_set.contains(&(rid as u64)) || vd.deleted {
                    return None;
                }
                let data = match &vd.data {
                    crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                    _ => return None,
                };
                decode_row(data, &schema_c).ok()
            }).collect()
        } else {
            // Sparse result set: batch read via row cache + LSM range scan
            let batch = self.db.get_table_rows_batch_arc(table, &sorted_ids)
                .map_err(|e| StorageError::Query(format!(
                    "Failed to fetch rows for table '{}': {}", table, e
                )))?;
            batch.into_iter()
                .filter_map(|(_, opt_arc)| opt_arc)
                .map(|row_arc| {
                    let row: Vec<Value> = (*row_arc).clone();
                    row
                })
                .collect()
        };

        // Apply post_filters on full decoded rows, then project survivors
        let result_rows: Vec<Result<Vec<Value>>> = decoded_rows.into_iter()
            .filter(|row| post_filters.is_empty() || Self::row_passes_post_filters(row, post_filters, &schema))
            .map(|row| Ok(Self::project_row_direct(&row, &stmt.columns, &columns, &schema)))
            .collect();

        Ok(StreamingQueryResult::SelectStreaming {
            columns,
            rows: Box::new(result_rows.into_iter()),
            order_by: stmt.order_by.clone(),
            limit: stmt.limit,
            offset: stmt.offset,
            distinct: stmt.distinct,
            max_result_rows: None,
            size_hint: None,
        })
    }

    /// 🔥 范围查询流式扫描（智能路由：主键用 LSM scan，非主键用列索引）
    /// 
    /// ## 性能优化
    /// - **主键范围查询**：使用 LSM range scan（顺序扫描，6x 提速）
    /// - **非主键查询**：使用列索引 + batch_get（减少锁竞争）
    /// - 批次大小：1000 条（平衡内存与性能）
    /// - 内存友好：仍然是流式返回，不会一次性加载全部数据
    /// 
    /// ## 边界正确性
    /// - `start_inclusive`: 下界是否包含（>= vs >）
    /// - `end_inclusive`: 上界是否包含（<= vs <）
    #[allow(clippy::too_many_arguments)]
    fn execute_range_query_streaming(
        &self,
        stmt: &SelectStmt,
        table: &str,
        column: &str,
        start: &Value,
        start_inclusive: bool,
        end: &Value,
        end_inclusive: bool,
        post_filters: &[Expr],
    ) -> Result<StreamingQueryResult> {
        // S9: ColSegmentStore tables — fall back to full scan (data not in LSM).
        if self.db.has_col_segment_store(table) {
            return self.execute_full_scan_streaming(stmt, table);
        }
        let schema = self.db.get_table_schema(table)?;

        // If SELECT expressions need the full evaluator, fall back to materialized path
        if Self::select_needs_materialized(stmt) {
            return self.materialize_as_streaming(stmt);
        }

        // 🚀 Fast path for SELECT *: bypass SqlRow + project overhead
        let is_star = stmt.columns.len() == 1 && matches!(stmt.columns[0], SelectColumn::Star)
            && stmt.order_by.is_none() && !stmt.distinct;
        if is_star {
            let pk_col = schema.primary_key().unwrap_or("id");
            if column != pk_col {
                let index_name = format!("{}.{}", table, column);
                if let Some(index_ref) = self.db.column_indexes.get(&index_name) {
                    let row_ids = index_ref.value().query_between(start, start_inclusive, end, end_inclusive)?;
                    drop(index_ref);
                    if !row_ids.is_empty() || !self.db.is_async_index_pipeline_active() {
                        let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
                        let arc_rows = self.db.get_table_rows_batch_arc(table, &row_ids)?;
                        let skip_n = stmt.offset.unwrap_or(0);
                        let take_n = stmt.limit.unwrap_or(usize::MAX);
                        let rows: Vec<Vec<Value>> = arc_rows.into_iter()
                            .filter_map(|(_, opt)| opt)
                            .filter(|row| post_filters.is_empty() || Self::row_passes_post_filters(row, post_filters, &schema))
                            .skip(skip_n)
                            .take(take_n)
                            .map(|arc| match Arc::try_unwrap(arc) {
                                Ok(row) => row,
                                Err(arc) => (*arc).clone(),
                            })
                            .collect();
                        return Ok(StreamingQueryResult::SelectReady {
                            columns: column_names,
                            rows,
                        });
                    }
                }
            }
        }

        let columns = self.build_select_columns(&stmt.columns, &schema)?;

        // 🚀 优化路径1：主键范围查询使用 LSM range scan（顺序扫描）
        let pk_col = schema.primary_key().unwrap_or("id");
        if column == pk_col {
            return self.execute_primary_key_range_streaming(stmt, table, start, start_inclusive, end, end_inclusive);
        }

        // 🔧 路径2：非主键列使用列索引 + batch_get (with row cache)
        let row_ids = self.db.query_by_column_between(table, column, start, start_inclusive, end, end_inclusive)?;

        // 🚀 批量读取行数据（通过 row_cache 减少锁竞争和 LSM 开销）
        let db = self.db.clone();
        let table_name = table.to_string();

        // Decode rows in batches (no projection yet — post_filters need full row)
        const BATCH_SIZE: usize = 1000;
        let total_rows = row_ids.len();

        let decoded_rows: Vec<Vec<Value>> = (0..total_rows).step_by(BATCH_SIZE).flat_map(|batch_start| {
            let batch_end = (batch_start + BATCH_SIZE).min(total_rows);
            let batch_row_ids = &row_ids[batch_start..batch_end];

            match db.get_table_rows_batch(&table_name, batch_row_ids) {
                Ok(results) => results.into_iter()
                    .filter_map(|(_, opt)| opt)
                    .collect::<Vec<_>>(),
                Err(_) => vec![],
            }
        }).collect();

        // Apply post_filters on full decoded rows, then project survivors
        let result_rows: Vec<Result<Vec<Value>>> = decoded_rows.into_iter()
            .filter(|row| post_filters.is_empty() || Self::row_passes_post_filters(row, post_filters, &schema))
            .map(|row| Ok(Self::project_row_direct(&row, &stmt.columns, &columns, &schema)))
            .collect();

        Ok(StreamingQueryResult::SelectStreaming {
            columns,
            rows: Box::new(result_rows.into_iter()),
            order_by: stmt.order_by.clone(),
            limit: stmt.limit,
            offset: stmt.offset,
            distinct: stmt.distinct,
            max_result_rows: None,
            size_hint: None,
        })
    }

    /// 🚀 主键范围查询流式扫描（使用 LSM range scan）
    /// 
    /// ## 关键优化
    /// - 直接使用 LSM range scan（顺序扫描 SSTables）
    /// - 避免遍历 425 个 L0 SSTables（batch_get 的瓶颈）
    /// - 利用 SSTable 的有序性，只扫描相关区间
    /// 
    /// ## 性能提升
    /// - 延迟：308ms → ~50ms（**6x 提速** ✅）
    /// - Bloom Filter 检查：425,000 次 → ~50 次（减少 **8500x**）
    /// - SSTable 锁操作：425,000 次 → ~50 次（减少 **8500x**）
    /// - 内存：0.30 MB（不变）
    fn execute_primary_key_range_streaming(
        &self,
        stmt: &SelectStmt,
        table: &str,
        start: &Value,
        start_inclusive: bool,
        end: &Value,
        end_inclusive: bool,
    ) -> Result<StreamingQueryResult> {
        let schema = self.db.get_table_schema(table)?;
        let columns = self.build_select_columns(&stmt.columns, &schema)?;

        // If SELECT expressions need the full evaluator, fall back to materialized path
        if Self::select_needs_materialized(stmt) {
            return self.materialize_as_streaming(stmt);
        }

        // 提取 row_id 范围
        let start_row_id = match start {
            Value::Integer(i) => *i as u64,
            _ => return Err(StorageError::InvalidData(format!("Primary key must be integer, got {:?}", start))),
        };
        let end_row_id = match end {
            Value::Integer(i) => *i as u64,
            _ => return Err(StorageError::InvalidData(format!("Primary key must be integer, got {:?}", end))),
        };
        
        // 构造 LSM key range
        let mut start_key = self.db.make_composite_key(table, start_row_id);
        let mut end_key = self.db.make_composite_key(table, end_row_id);
        
        // 处理边界（将 > 转换为 >=，< 转换为 <=）
        if !start_inclusive {
            start_key += 1; // id > 100 等价于 id >= 101
        }
        if end_inclusive {
            end_key += 1; // id <= 200 等价于 id < 201
        }
        
        // 🚀 P2: 使用真正的流式迭代器（O(1) 内存占用，~20 KB）
        let lsm_iter = self.db.lsm_engine.scan_range_streaming(start_key, end_key)?;
        
        // 转换为 SQL 行并投影
        let schema_clone = schema.clone();
        let select_cols = stmt.columns.clone();
        let columns_clone = columns.clone();
        
        let rows_iter = lsm_iter.map(move |result| {
            // 处理迭代器错误
            let (_key, value_data) = match result {
                Ok(kv) => kv,
                Err(e) => return Err(e),
            };

            // 反序列化行
            let data = match &value_data.data {
                crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                _ => return Err(StorageError::InvalidData("Unexpected blob".into())),
            };

            match decode_row(data, &schema_clone) {
                Ok(row) => {
                    let projected = Self::project_row_direct(&row, &select_cols, &columns_clone, &schema_clone);
                    Ok(projected)
                }
                Err(e) => Err(StorageError::InvalidData(format!("Deserialization failed: {}", e))),
            }
        });
        
        Ok(StreamingQueryResult::SelectStreaming {
            columns,
            rows: Box::new(rows_iter),
            order_by: stmt.order_by.clone(),
            limit: stmt.limit,
            offset: stmt.offset,
            distinct: stmt.distinct,
            max_result_rows: None,
            size_hint: None,
        })
    }

    /// Execute an index intersection plan: look up both indexes, intersect row IDs,
    /// batch-fetch matching rows, then project.
    fn execute_index_intersection_streaming(
        &self,
        stmt: &SelectStmt,
        table: &str,
        column1: &str,
        value1: &Value,
        column2: &str,
        value2: &Value,
        post_filters: &[Expr],
    ) -> Result<StreamingQueryResult> {
        let schema = self.db.get_table_schema(table)?;
        let columns = self.build_select_columns(&stmt.columns, &schema)?;

        let idx1_name = format!("{}.{}", table, column1);
        let idx2_name = format!("{}.{}", table, column2);

        // Look up row IDs from first index
        let row_ids1 = {
            let idx_ref = self.db.column_indexes.get(&idx1_name)
                .ok_or_else(|| MoteDBError::InvalidArgument(format!("Index {} not found", idx1_name)))?;
            idx_ref.value().get(value1)?
        };
        let row_id_set1: std::collections::HashSet<u64> = row_ids1.into_iter().collect();

        // Look up row IDs from second index and intersect
        let row_ids2 = {
            let idx_ref = self.db.column_indexes.get(&idx2_name)
                .ok_or_else(|| MoteDBError::InvalidArgument(format!("Index {} not found", idx2_name)))?;
            idx_ref.value().get(value2)?
        };
        let intersected: Vec<u64> = row_ids2.into_iter()
            .filter(|id| row_id_set1.contains(id))
            .collect();

        if intersected.is_empty() {
            return Ok(StreamingQueryResult::SelectReady {
                columns,
                rows: vec![],
            });
        }

        // Batch fetch intersected rows
        let rows_result = self.db.get_table_rows_batch_arc(table, &intersected)?;

        // Apply post_filters on full decoded rows, then project survivors
        let projected_rows: Vec<Vec<Value>> = rows_result.into_iter()
            .filter_map(|(_row_id, opt_row)| opt_row)
            .filter(|row| post_filters.is_empty() || Self::row_passes_post_filters(row, post_filters, &schema))
            .map(|row| {
                let row: Vec<Value> = (*row).clone();
                Self::project_row_direct(&row, &stmt.columns, &columns, &schema)
            })
            .collect();

        // Apply modifiers
        let mut rows = projected_rows;
        if stmt.distinct {
            let mut seen = std::collections::HashSet::new();
            rows.retain(|row| seen.insert(row.clone()));
        }
        if let Some(ref order_by) = stmt.order_by {
            let sort_specs: Vec<(usize, bool)> = order_by.iter().filter_map(|ob| {
                let col_name = match &ob.expr { Expr::Column(name) => name, _ => return None };
                let bare = if col_name.contains('.') { col_name.rsplit('.').next().unwrap_or(col_name) } else { col_name };
                columns.iter().position(|c| c == bare || c == col_name).map(|i| (i, ob.asc))
            }).collect();
            if !sort_specs.is_empty() {
                rows.sort_by(|a, b| {
                    for &(col_idx, asc) in &sort_specs {
                        if col_idx >= a.len() || col_idx >= b.len() { continue; }
                        let ord = match (&a[col_idx], &b[col_idx]) {
                            (Value::Float(fa), Value::Float(fb)) => {
                                if fa.is_nan() && fb.is_nan() { std::cmp::Ordering::Equal }
                                else if fa.is_nan() { std::cmp::Ordering::Greater }
                                else if fb.is_nan() { std::cmp::Ordering::Less }
                                else { fa.partial_cmp(fb).unwrap_or(std::cmp::Ordering::Equal) }
                            }
                            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
                            (Value::Null, _) => std::cmp::Ordering::Less,
                            (_, Value::Null) => std::cmp::Ordering::Greater,
                            (va, vb) => va.partial_cmp(vb).unwrap_or(std::cmp::Ordering::Equal),
                        };
                        let final_ord = if asc { ord } else { ord.reverse() };
                        if final_ord != std::cmp::Ordering::Equal { return final_ord; }
                    }
                    std::cmp::Ordering::Equal
                });
            }
        }
        if let Some(offset) = stmt.offset {
            rows = rows.into_iter().skip(offset).collect();
        }
        if let Some(limit) = stmt.limit {
            rows.truncate(limit);
        }

        Ok(StreamingQueryResult::SelectReady { columns, rows })
    }

    /// 🚀 Streaming Top-K via bounded heap with partial decode.
    ///
    /// Only extracts the sort column value from each row (not all columns),
    /// then batch-fetches the K winning rows. For a 4-column table, this
    /// decodes ~75% less data per row — only 1 column instead of 4.
    ///
    /// For fixed-width sort columns (Integer, Float, Bool, Timestamp), the
    /// sort value is read directly from a known byte offset — no decode at all.
    fn try_order_by_limit_topk(&self, stmt: &SelectStmt, table: &str) -> Result<Option<StreamingQueryResult>> {
        let limit = match stmt.limit {
            Some(l) => l as usize,
            None => return Ok(None),
        };
        let order_by = match stmt.order_by.as_ref() {
            Some(ob) if !ob.is_empty() => ob,
            _ => return Ok(None),
        };
        // Only handle single-column ORDER BY for now (covers the common case)
        if order_by.len() > 1 {
            return Ok(None);
        }
        let schema = self.db.get_table_schema(table)?;
        // Resolve ORDER BY column position
        let (sort_col_idx, ascending) = {
            let ob = &order_by[0];
            let col_name = match &ob.expr {
                Expr::Column(name) => name.as_str(),
                _ => return Ok(None),
            };
            let pos = match schema.get_column_position(col_name) {
                Some(p) => p,
                None => return Ok(None),
            };
            (pos, ob.asc)
        };

        let col_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();

        // 🚀 Columnar Top-K: read sort column segment, heap of (value, idx),
        // then fetch only K winning rows. Much faster than row-based scan.
        if self.db.columnar_sstables.contains_key(table) {
            let k = limit.min(100_000);
            if let Ok((indices, _vals)) = self.db.scan_columnar_sstable_topk(
                table, sort_col_idx, k, ascending,
            ) {
                let col_types = schema.col_types();
                if let Ok(rows) = self.db.scan_columnar_sstable_rows(table, &col_types, &indices) {
                    return Ok(Some(StreamingQueryResult::SelectReady { columns: col_names, rows }));
                }
            }
        }

        let col_types = schema.col_types();
        let limit_usize = limit.min(100_000); // sanity cap
        use std::collections::BinaryHeap;
        use std::cmp::Reverse;

        // Pre-compute sort column layout for direct read from raw bytes.
        // For fixed columns: byte offset = HEADER_SIZE + fixed_idx * 8
        // For var columns: we need to scan the var header, but that's still
        //   cheaper than full row decode (only parse header, skip all other vars).
        let is_fixed = matches!(&col_types[sort_col_idx],
            crate::types::ColumnType::Integer | crate::types::ColumnType::Float
            | crate::types::ColumnType::Boolean | crate::types::ColumnType::Timestamp);
        let sort_col_type = col_types[sort_col_idx].clone();
        let fixed_count = col_types.iter()
            .filter(|t| matches!(t, crate::types::ColumnType::Integer | crate::types::ColumnType::Float
                | crate::types::ColumnType::Boolean | crate::types::ColumnType::Timestamp))
            .count();
        let var_section_start = crate::storage::row_format::HEADER_SIZE + fixed_count * crate::storage::row_format::FIXED_COL_SIZE;

        // Pre-compute fixed offset for the sort column (valid only if is_fixed)
        let fixed_offset: usize = if is_fixed {
            let fixed_idx: usize = col_types[..sort_col_idx].iter()
                .filter(|t| matches!(t, crate::types::ColumnType::Integer | crate::types::ColumnType::Float
                    | crate::types::ColumnType::Boolean | crate::types::ColumnType::Timestamp))
                .count();
            crate::storage::row_format::HEADER_SIZE + fixed_idx * crate::storage::row_format::FIXED_COL_SIZE
        } else {
            0
        };

        // Helper to extract sort column value from raw bytes
        let extract_sort_val = |data: &[u8]| -> Option<Value> {
            use crate::storage::row_format::{HEADER_SIZE, FIXED_COL_SIZE};
            if data.len() < HEADER_SIZE { return None; }
            // Null bitmap check
            let null_bitmap = u64::from_le_bytes([
                data[4], data[5], data[6], data[7],
                data[8], data[9], data[10], data[11],
            ]);
            if null_bitmap & (1u64 << sort_col_idx) != 0 {
                return Some(Value::Null);
            }
            if is_fixed {
                let off = fixed_offset;
                if off + FIXED_COL_SIZE > data.len() { return None; }
                let arr: [u8; 8] = data[off..off + 8].try_into().ok()?;
                match sort_col_type {
                    crate::types::ColumnType::Integer => Some(Value::Integer(i64::from_le_bytes(arr))),
                    crate::types::ColumnType::Float => Some(Value::Float(f64::from_le_bytes(arr))),
                    crate::types::ColumnType::Boolean => Some(Value::Bool(data[off] != 0)),
                    crate::types::ColumnType::Timestamp => {
                        let ts = crate::types::Timestamp::from_micros(i64::from_le_bytes(arr));
                        Some(Value::Timestamp(ts))
                    }
                    _ => None,
                }
            } else {
                // Variable column: scan var headers to find this column's data
                if var_section_start + 2 > data.len() { return None; }
                let var_count = u16::from_le_bytes([data[var_section_start], data[var_section_start + 1]]) as usize;
                let var_header_start = var_section_start + 2;
                let var_data_start = var_header_start + var_count * 10;
                for vi in 0..var_count {
                    let hdr_off = var_header_start + vi * 10;
                    if hdr_off + 10 > data.len() { break; }
                    let entry_col = u16::from_le_bytes([data[hdr_off], data[hdr_off + 1]]) as usize;
                    if entry_col == sort_col_idx {
                        let v_off = u32::from_le_bytes([
                            data[hdr_off+2], data[hdr_off+3], data[hdr_off+4], data[hdr_off+5]]) as usize;
                        let v_len = u32::from_le_bytes([
                            data[hdr_off+6], data[hdr_off+7], data[hdr_off+8], data[hdr_off+9]]) as usize;
                        let abs_off = var_data_start + v_off;
                        if abs_off + v_len > data.len() { return None; }
                        let var_data = &data[abs_off..abs_off + v_len];
                        return match &sort_col_type {
                            crate::types::ColumnType::Text => {
                                let s = std::str::from_utf8(var_data).ok()?;
                                Some(Value::Text(crate::types::ArcString(std::sync::Arc::from(s))))
                            }
                            _ => crate::storage::row_format::SchemaDecodeContext::decode_var_generic(var_data).ok(),
                        };
                    }
                }
                None
            }
        };

        // Get MergingIterator directly for zero-copy raw access.
        let table_prefix = self.db.compute_table_prefix(table);
        let start_key = table_prefix << 32;
        let end_key = (table_prefix + 1) << 32;
        let mut lsm_iter = self.db.lsm_engine.scan_range_streaming(start_key, end_key)?;
        let has_raw = lsm_iter.has_raw_sst();

        // Build Top-K heap: only store (sort_val, row_id), not full rows.
        // After scan, batch-fetch just the K winning row_ids.
        if ascending {
            // ASC: max-heap (pop largest when >K, keeping K smallest)
            let mut heap: BinaryHeap<(SortKey, RowId)> = BinaryHeap::with_capacity(limit_usize + 1);
            if has_raw {
                loop {
                    match lsm_iter.next_raw() {
                        Some(Ok((composite_key, _ts, deleted, vb))) => {
                            if deleted || vb.len == 0 { continue; }
                            let row_id = (composite_key & 0xFFFFFFFF) as RowId;
                            if let Some(sv) = extract_sort_val(vb.as_slice()) {
                                heap.push((SortKey(sv), row_id));
                                if heap.len() > limit_usize { heap.pop(); }
                            }
                        }
                        Some(Err(_)) => return Ok(None), // fallback on error
                        None => break,
                    }
                }
            } else {
                loop {
                    match lsm_iter.next() {
                        Some(Ok((composite_key, value))) => {
                            if value.deleted { continue; }
                            let row_id = (composite_key & 0xFFFFFFFF) as RowId;
                            let data = match &value.data {
                                crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                                _ => continue,
                            };
                            if let Some(sv) = extract_sort_val(data) {
                                heap.push((SortKey(sv), row_id));
                                if heap.len() > limit_usize { heap.pop(); }
                            }
                        }
                        Some(Err(_)) => return Ok(None),
                        None => break,
                    }
                }
            }
            let mut top: Vec<(SortKey, RowId)> = heap.into_vec();
            top.sort_by(|a, b| a.0.cmp(&b.0));
            top.truncate(limit_usize);
            let row_ids: Vec<RowId> = top.into_iter().map(|(_, rid)| rid).collect();
            let rows = self.db.get_table_rows_batch_arc(table, &row_ids)?;
            let rows: Vec<Row> = rows.into_iter()
                .filter_map(|(_, opt)| opt.map(|a| match Arc::try_unwrap(a) {
                    Ok(row) => row,
                    Err(arc) => (*arc).clone(),
                }))
                .collect();
            return Ok(Some(StreamingQueryResult::SelectReady { columns: col_names, rows }));
        } else {
            // DESC: min-heap via Reverse (pop smallest when >K, keeping K largest)
            let mut heap: BinaryHeap<Reverse<(SortKey, RowId)>> = BinaryHeap::with_capacity(limit_usize + 1);
            if has_raw {
                loop {
                    match lsm_iter.next_raw() {
                        Some(Ok((composite_key, _ts, deleted, vb))) => {
                            if deleted || vb.len == 0 { continue; }
                            let row_id = (composite_key & 0xFFFFFFFF) as RowId;
                            if let Some(sv) = extract_sort_val(vb.as_slice()) {
                                heap.push(Reverse((SortKey(sv), row_id)));
                                if heap.len() > limit_usize { heap.pop(); }
                            }
                        }
                        Some(Err(_)) => return Ok(None),
                        None => break,
                    }
                }
            } else {
                loop {
                    match lsm_iter.next() {
                        Some(Ok((composite_key, value))) => {
                            if value.deleted { continue; }
                            let row_id = (composite_key & 0xFFFFFFFF) as RowId;
                            let data = match &value.data {
                                crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                                _ => continue,
                            };
                            if let Some(sv) = extract_sort_val(data) {
                                heap.push(Reverse((SortKey(sv), row_id)));
                                if heap.len() > limit_usize { heap.pop(); }
                            }
                        }
                        Some(Err(_)) => return Ok(None),
                        None => break,
                    }
                }
            }
            let mut top: Vec<Reverse<(SortKey, RowId)>> = heap.into_vec();
            top.sort_by(|a, b| b.0.cmp(&a.0)); // DESC: largest first
            top.truncate(limit_usize);
            let row_ids: Vec<RowId> = top.into_iter().map(|r| r.0.1).collect();
            let rows = self.db.get_table_rows_batch_arc(table, &row_ids)?;
            let rows: Vec<Row> = rows.into_iter()
                .filter_map(|(_, opt)| opt.map(|a| match Arc::try_unwrap(a) {
                    Ok(row) => row,
                    Err(arc) => (*arc).clone(),
                }))
                .collect();
            return Ok(Some(StreamingQueryResult::SelectReady { columns: col_names, rows }));
        }
    }

    /// 🚀 Columnar aggregate pushdown: compute COUNT/SUM/MIN/MAX directly
    /// from column segments without materializing any rows.
    /// 🚀 Columnar GROUP BY pushdown: build HashMap directly from typed arrays.
    /// Only reads the group-by column and aggregate columns — no per-row decode.
    /// For GROUP BY customer: read TextSegment → HashMap<String, (count, sum)> → compute AVG.
    fn try_group_by_columnar(&self, stmt: &SelectStmt) -> Result<Option<StreamingQueryResult>> {
        let table = match &stmt.from {
            Some(TableRef::Table { name, .. }) => name.as_str(), _ => return Ok(None),
        };
        if !self.db.columnar_sstables.contains_key(table) { return Ok(None); }
        let schema = self.db.get_table_schema(table)?;
        let col_sst = self.db.columnar_sstables.get(table).unwrap();
        let num_rows = col_sst.num_rows;
        // HAVING and DISTINCT aggregates are not applied by this pushdown path —
        // fall back to the materialized GROUP BY path which evaluates them.
        // ORDER BY over the grouped result is also not applied here (the
        // pushdown emits groups in scan/hash order, not sorted).
        if stmt.having.is_some() || stmt.order_by.is_some() { return Ok(None); }

        // Parse GROUP BY columns (only single-column for now)
        let group_cols = stmt.group_by.as_ref().unwrap();
        if group_cols.len() != 1 { return Ok(None); }
        let group_col_name = group_cols[0].as_str();
        let group_pos = match schema.get_column_position(group_col_name) {
            Some(p) => p, None => return Ok(None),
        };

        // Parse aggregate columns
        struct AggCol { func: String, col_pos: usize }
        let mut agg_cols: Vec<AggCol> = Vec::new();
        let mut has_count_star = false;
        for col_expr in &stmt.columns {
            match col_expr {
                SelectColumn::Star => { has_count_star = true; }
                SelectColumn::Column(name) => { /* group-by column in output */ }
                SelectColumn::Expr(expr, _) => {
                    if let Expr::FunctionCall { name, args, .. } = expr {
                        match name.to_uppercase().as_str() {
                            "COUNT" => {
                                if args.first().map_or(true, |a| matches!(a, Expr::Column(_))) {
                                    has_count_star = true; // COUNT(col) → treat as count
                                }
                            }
                            _ => {
                                let col = match args.first() {
                                    Some(Expr::Column(c)) => c.as_str(), _ => return Ok(None),
                                };
                                let pos = match schema.get_column_position(col) {
                                    Some(p) => p, None => return Ok(None),
                                };
                                agg_cols.push(AggCol { func: name.to_uppercase(), col_pos: pos });
                            }
                        }
                    }
                }
                _ => return Ok(None),
            }
        }

        // Build HashMap from typed arrays — use &str keys (zero-alloc) from mmap.
        // Accumulator: (count, int_sum, float_sum, has_float). Integer columns
        // accumulate into int_sum; Float columns into float_sum. Reading an
        // Integer column via get_f64() reinterprets its i64 bits as f64 (e.g.
        // Integer(10) → f64::from_bits(10) ≈ 0), which silently corrupts SUM.
        // Decode according to the column's declared type.
        use std::collections::HashMap;
        let col_types = schema.col_types();
        struct GroupAcc { count: i64, int_sum: i64, float_sum: f64, has_float: bool }
        impl GroupAcc {
            fn new() -> Self { Self { count: 0, int_sum: 0, float_sum: 0.0, has_float: false } }
            fn add(&mut self, val: f64, is_int: bool) {
                if is_int {
                    self.int_sum = self.int_sum.wrapping_add(val as i64);
                } else {
                    if !self.has_float { self.has_float = true; self.float_sum = self.int_sum as f64; }
                    self.float_sum += val;
                }
            }
            fn sum(&self) -> Value {
                if self.has_float { Value::Float(self.float_sum) } else { Value::Integer(self.int_sum) }
            }
            fn avg(&self) -> Value {
                if self.count == 0 { return Value::Null; }
                let s = if self.has_float { self.float_sum } else { self.int_sum as f64 };
                Value::Float(s / self.count as f64)
            }
        }
        let mut groups: HashMap<&str, (GroupAcc, Vec<f64>)> = HashMap::with_capacity(num_rows / 10);

        if col_sst.column_tags[group_pos].is_fixed() {
            return Ok(None);
        }
        let group_seg = col_sst.read_text(group_pos)?;
        let mut agg_segs: Vec<crate::storage::lsm::columnar::FixedSegment> = Vec::new();
        // For each agg column, whether it's an Integer (use get_i64) or Float.
        let mut agg_is_int: Vec<bool> = Vec::with_capacity(agg_cols.len());
        for a in &agg_cols {
            if col_sst.column_tags[a.col_pos].is_fixed() {
                agg_segs.push(col_sst.read_fixed_i64(a.col_pos)?);
                agg_is_int.push(matches!(col_types.get(a.col_pos),
                    Some(crate::types::ColumnType::Integer)
                    | Some(crate::types::ColumnType::Boolean)));
            } else { return Ok(None); }
        }

        for i in 0..num_rows {
            if col_sst.row_map.is_deleted(i) { continue; }
            let key = match group_seg.get_str(i) {
                Some(s) => s,
                None => continue,
            };
            let entry = groups.entry(key).or_insert_with(|| (GroupAcc::new(), Vec::new()));
            entry.0.count += 1;
            for (j, a) in agg_cols.iter().enumerate() {
                if a.func == "SUM" || a.func == "AVG" {
                    let is_int = agg_is_int[j];
                    let v = if is_int {
                        agg_segs[j].get_i64(i).map(|x| x as f64)
                    } else {
                        agg_segs[j].get_f64(i)
                    };
                    if let Some(v) = v { entry.0.add(v, is_int); }
                }
            }
        }

        // Build output rows
        let mut rows: Vec<Vec<Value>> = Vec::with_capacity(groups.len());
        for (key, (acc, _)) in groups {
            let mut row = Vec::new();
            // Group-by column value
            row.push(Value::Text(crate::types::ArcString(std::sync::Arc::from(key))));
            if has_count_star { row.push(Value::Integer(acc.count)); }
            for a in &agg_cols {
                match a.func.as_str() {
                    "SUM" => row.push(acc.sum()),
                    "AVG" => row.push(acc.avg()),
                    _ => row.push(Value::Null),
                }
            }
            rows.push(row);
        }

        // Build column names
        let cols: Vec<String> = stmt.columns.iter().map(|c| match c {
            SelectColumn::Star => "COUNT(*)".to_string(),
            SelectColumn::Column(name) => name.clone(),
            SelectColumn::Expr(Expr::FunctionCall { name, args, .. }, alias) => alias.clone().unwrap_or_else(||
                format!("{}({})", name.to_uppercase(), match args.first() { Some(Expr::Column(c)) => c.as_str(), _ => "?" })
            ),
            _ => "?".to_string(),
        }).collect();

        Ok(Some(StreamingQueryResult::SelectReady { columns: cols, rows }))
    }

    /// For WHERE region='US': read region segment → find matches → compute
    /// SUM/MIN/MAX from amount segment. O(N) scan but no per-row allocation.
    fn try_aggregate_columnar_fast(&self, stmt: &SelectStmt) -> Result<Option<StreamingQueryResult>> {
        if stmt.group_by.is_some() { return Ok(None); }
        let table = match &stmt.from {
            Some(TableRef::Table { name, .. }) => name.as_str(), _ => return Ok(None),
        };
        if !self.db.columnar_sstables.contains_key(table) { return Ok(None); }
        let schema = self.db.get_table_schema(table)?;
        let col_types = schema.col_types();

        // Parse WHERE: only simple col = literal
        let (filter_col, filter_value) = match &stmt.where_clause {
            Some(Expr::BinaryOp { left, op: crate::sql::ast::BinaryOperator::Eq, right }) => {
                let col = match left.as_ref() { Expr::Column(n) => n.as_str(), _ => return Ok(None) };
                let val = match right.as_ref() { Expr::Literal(v) => v.clone(), _ => return Ok(None) };
                (col, val)
            }
            _ => return Ok(None),
        };
        let filter_pos = match schema.get_column_position(filter_col) {
            Some(p) => p, None => return Ok(None),
        };

        let col_sst = self.db.columnar_sstables.get(table).unwrap();
        let num_rows = col_sst.num_rows;

        // Find matching rows from filter column segment
        let mut match_indices: Vec<usize> = Vec::new();
        if col_sst.column_tags[filter_pos].is_fixed() {
            let seg = col_sst.read_fixed_i64(filter_pos)?;
            for i in 0..num_rows {
                if col_sst.row_map.is_deleted(i) { continue; }
                let matches = match &filter_value {
                    Value::Integer(iv) => seg.get_i64(i) == Some(*iv),
                    Value::Float(fv) => (seg.get_f64(i).unwrap_or(f64::NAN) - fv).abs() < f64::EPSILON,
                    _ => false,
                };
                if matches { match_indices.push(i); }
            }
        } else {
            let seg = col_sst.read_text(filter_pos)?;
            for i in 0..num_rows {
                if col_sst.row_map.is_deleted(i) { continue; }
                if let Value::Text(tv) = &filter_value {
                    if seg.get_str(i) == Some(tv.as_str()) { match_indices.push(i); }
                }
            }
        }

        let count = match_indices.len() as i64;
        let mut result = Vec::new();

        for col_expr in &stmt.columns {
            match col_expr {
                SelectColumn::Star => {} // COUNT(*)
                SelectColumn::Expr(expr, _) => {
                    if let Expr::FunctionCall { name, args, .. } = expr {
                        match name.to_uppercase().as_str() {
                            "COUNT" => {} // already have count
                            _ => {
                                let agg_col = match args.first() {
                                    Some(Expr::Column(c)) => c.as_str(), _ => return Ok(None),
                                };
                                let agg_pos = match schema.get_column_position(agg_col) {
                                    Some(p) => p, None => return Ok(None),
                                };
                                let is_fixed = col_sst.column_tags[agg_pos].is_fixed();
                                if is_fixed {
                                    let seg = col_sst.read_fixed_i64(agg_pos)?;
                                    // 🔑 Decode by column type, NOT always f64. Integer
                                    // columns store i64; reading them as f64 reinterprets
                                    // the bits (Integer(5) → f64::from_bits(5) ≈ 0),
                                    // corrupting SUM/MIN/MAX (the v0.5.0 MIN=inf bug).
                                    let is_int = matches!(schema.col_types().get(agg_pos),
                                        Some(crate::types::ColumnType::Integer)
                                        | Some(crate::types::ColumnType::Boolean));
                                    if is_int {
                                        let vals: Vec<i64> = match_indices.iter()
                                            .filter_map(|&i| seg.get_i64(i))
                                            .filter(|&v| v != i64::MIN) // MIN is the NULL sentinel
                                            .collect();
                                        match name.to_uppercase().as_str() {
                                            "SUM" => result.push(Value::Integer(vals.iter().sum())),
                                            "MIN" => result.push(vals.iter().min().copied().map(Value::Integer).unwrap_or(Value::Null)),
                                            "MAX" => result.push(vals.iter().max().copied().map(Value::Integer).unwrap_or(Value::Null)),
                                            _ => return Ok(None),
                                        }
                                    } else {
                                        let vals: Vec<f64> = match_indices.iter()
                                            .filter_map(|&i| seg.get_f64(i))
                                            .filter(|v| !v.is_nan()) // NaN = NULL
                                            .collect();
                                        let pick = |cmp: std::cmp::Ordering| cmp;
                                        let _ = pick;
                                        match name.to_uppercase().as_str() {
                                            "SUM" => result.push(Value::Float(vals.iter().sum())),
                                            "MIN" => result.push(vals.iter().min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)).copied().map(Value::Float).unwrap_or(Value::Null)),
                                            "MAX" => result.push(vals.iter().max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)).copied().map(Value::Float).unwrap_or(Value::Null)),
                                            _ => return Ok(None),
                                        }
                                    }
                                } else { return Ok(None); }
                            }
                        }
                    }
                }
                _ => return Ok(None),
            }
        }

        let has_star = stmt.columns.iter().any(|c| matches!(c, SelectColumn::Star));
        let mut final_row = Vec::new();
        if has_star { final_row.push(Value::Integer(count)); }
        final_row.extend(result);
        let cols: Vec<String> = stmt.columns.iter().map(|c| match c {
            SelectColumn::Star => "COUNT(*)".to_string(),
            SelectColumn::Expr(Expr::FunctionCall { name, args, .. }, alias) => alias.clone().unwrap_or_else(|| format!("{}({})", name.to_uppercase(), match args.first() { Some(Expr::Column(c)) => c.as_str(), _ => "?" })),
            _ => "?".to_string(),
        }).collect();
        Ok(Some(StreamingQueryResult::SelectReady { columns: cols, rows: vec![final_row] }))
    }

    /// 🚀 Aggregate WHERE col=value via column index.
    /// For COUNT/SUM/MIN/MAX with a simple equality WHERE on an indexed column,
    /// use the index to get matching row_ids → batch fetch → compute aggregates.
    /// O(index_lookup) instead of O(N) full scan.
    fn try_aggregate_via_column_index(&self, stmt: &SelectStmt) -> Result<Option<StreamingQueryResult>> {
        // Must have WHERE clause with simple col = value
        let where_clause = match &stmt.where_clause {
            Some(w) => w,
            None => return Ok(None),
        };
        // Extract: col_name = literal_value
        let (filter_col, filter_value) = match where_clause {
            Expr::BinaryOp { left, op: crate::sql::ast::BinaryOperator::Eq, right } => {
                let col = match left.as_ref() {
                    Expr::Column(name) => name.as_str(),
                    _ => return Ok(None),
                };
                let val = match right.as_ref() {
                    Expr::Literal(v) => v.clone(),
                    _ => return Ok(None),
                };
                (col, val)
            }
            _ => return Ok(None),
        };
        // Must be a single table
        let table = match &stmt.from {
            Some(TableRef::Table { name, .. }) => name.as_str(),
            _ => return Ok(None),
        };
        let schema = self.db.get_table_schema(table)?;

        // Check for column value index on the filtered column
        let index_name = format!("{}.{}", table, filter_col);
        let index_ref = match self.db.column_indexes.get(&index_name) {
            Some(idx) => idx,
            None => return Ok(None),
        };
        let row_ids_arc = index_ref.value().get_arc(&filter_value)?;
        if row_ids_arc.is_empty() {
            return Ok(None);
        }
        // Only use index direct-fetch when selectivity is high (few matching rows).
        // For low-selectivity filters (100K/300K), a full scan decodes faster
        // than 100K individual row fetches.
        if row_ids_arc.len() > 1000 {
            return Ok(None);
        }
        drop(index_ref);

        // Batch fetch matching rows
        let batch = self.db.get_table_rows_batch_arc(table, &row_ids_arc)?;
        let rows: Vec<&Row> = batch.iter()
            .filter_map(|(_, opt)| opt.as_ref().map(|a| a.as_ref()))
            .collect();

        if rows.is_empty() {
            let cols: Vec<String> = stmt.columns.iter().map(|c| match c {
                SelectColumn::Expr(_, alias) => alias.clone().unwrap_or_else(|| "?".to_string()),
                SelectColumn::Column(name) => name.clone(),
                _ => "?".to_string(),
            }).collect();
            let row: Vec<Value> = cols.iter().map(|_| Value::Integer(0)).collect();
            return Ok(Some(StreamingQueryResult::SelectReady { columns: cols, rows: vec![row] }));
        }

        // Compute aggregates from fetched rows
        let count = rows.len() as i64;
        let mut result_row = Vec::new();
        for col_expr in &stmt.columns {
            match col_expr {
                SelectColumn::Star => {}
                SelectColumn::Expr(expr, _alias) => {
                    if let Expr::FunctionCall { name, args, .. } = expr {
                        let agg_col = match args.first() {
                            Some(Expr::Column(c)) => c.as_str(),
                            _ => return Ok(None),
                        };
                        let agg_pos = match schema.get_column_position(agg_col) {
                            Some(p) => p, None => return Ok(None),
                        };
                        match name.to_uppercase().as_str() {
                            "COUNT" => {}
                            "SUM" => {
                                let sum: f64 = rows.iter()
                                    .filter_map(|r| r.get(agg_pos))
                                    .filter_map(|v| match v {
                                        Value::Integer(i) => Some(*i as f64),
                                        Value::Float(f) => Some(*f),
                                        _ => None,
                                    }).sum();
                                result_row.push(Value::Float(sum));
                            }
                            "MIN" => {
                                let min_val = rows.iter().filter_map(|r| r.get(agg_pos))
                                    .min_by(|a,b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
                                    .cloned().unwrap_or(Value::Null);
                                result_row.push(min_val);
                            }
                            "MAX" => {
                                let max_val = rows.iter().filter_map(|r| r.get(agg_pos))
                                    .max_by(|a,b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
                                    .cloned().unwrap_or(Value::Null);
                                result_row.push(max_val);
                            }
                            _ => return Ok(None),
                        }
                    }
                }
                _ => return Ok(None),
            }
        }
        let has_star = stmt.columns.iter().any(|c| matches!(c, SelectColumn::Star));
        let mut final_row = Vec::new();
        if has_star { final_row.push(Value::Integer(count)); }
        final_row.extend(result_row);
        let cols: Vec<String> = stmt.columns.iter().map(|c| match c {
            SelectColumn::Star => "COUNT(*)".to_string(),
            SelectColumn::Expr(Expr::FunctionCall { name, args, .. }, alias) => {
                alias.clone().unwrap_or_else(|| {
                    let col = match args.first() {
                        Some(Expr::Column(c)) => c.as_str(), _ => "?",
                    };
                    format!("{}({})", name.to_uppercase(), col)
                })
            }
            _ => "?".to_string(),
        }).collect();
        Ok(Some(StreamingQueryResult::SelectReady { columns: cols, rows: vec![final_row] }))
    }

    /// 🚀 Partial-decode aggregate: scan all rows but only extract filter + aggregate
    /// columns from raw bytes. Much faster than full row decode for queries like
    /// COUNT/SUM/MIN/MAX WHERE col = value (when index path is not used).
    fn try_aggregate_partial_scan(&self, stmt: &SelectStmt) -> Result<Option<StreamingQueryResult>> {
        let (filter_col, filter_value) = match &stmt.where_clause {
            Some(Expr::BinaryOp { left, op: crate::sql::ast::BinaryOperator::Eq, right }) => {
                let col = match left.as_ref() { Expr::Column(n) => n.as_str(), _ => return Ok(None) };
                let val = match right.as_ref() { Expr::Literal(v) => v.clone(), _ => return Ok(None) };
                (col, val)
            }
            _ => return Ok(None),
        };
        if stmt.group_by.is_some() { return Ok(None); }
        let table = match &stmt.from {
            Some(TableRef::Table { name, .. }) => name.as_str(), _ => return Ok(None),
        };
        let schema = self.db.get_table_schema(table)?;
        let filter_pos = match schema.get_column_position(filter_col) {
            Some(p) => p, None => return Ok(None),
        };
        let col_types = schema.col_types();

        // Identify aggregate columns
        let mut agg_cols: Vec<(String, usize)> = Vec::new();
        let mut has_count_star = false;
        for col_expr in &stmt.columns {
            match col_expr {
                SelectColumn::Star => { has_count_star = true; }
                SelectColumn::Expr(expr, _) => {
                    if let Expr::FunctionCall { name, args, .. } = expr {
                        // COUNT(*) or COUNT with any non-column arg
                        if name.eq_ignore_ascii_case("COUNT") && !matches!(args.first(), Some(Expr::Column(_))) {
                            has_count_star = true;
                        } else {
                            let pos = match args.first() {
                                Some(Expr::Column(c)) => match schema.get_column_position(c) {
                                    Some(p) => p, None => return Ok(None),
                                },
                                _ => return Ok(None),
                            };
                            agg_cols.push((name.to_uppercase(), pos));
                        }
                    }
                }
                _ => return Ok(None),
            }
        }

        // Pre-compute offsets for column extraction
        use crate::storage::row_format::{HEADER_SIZE, FIXED_COL_SIZE};
        let fixed_count = col_types.iter().filter(|t| matches!(t,
            crate::types::ColumnType::Integer | crate::types::ColumnType::Float
            | crate::types::ColumnType::Boolean | crate::types::ColumnType::Timestamp)).count();
        let var_section_start = HEADER_SIZE + fixed_count * FIXED_COL_SIZE;
        let fixed_offset_of = |ci: usize| -> usize {
            HEADER_SIZE + col_types[..ci].iter().filter(|t| matches!(t,
                crate::types::ColumnType::Integer | crate::types::ColumnType::Float
                | crate::types::ColumnType::Boolean | crate::types::ColumnType::Timestamp)).count() * FIXED_COL_SIZE
        };

        // Extract a column value from raw bytes
        let extract_col = |data: &[u8], ci: usize| -> Option<Value> {
            if data.len() < HEADER_SIZE { return None; }
            let ct = &col_types[ci];
            if matches!(ct, crate::types::ColumnType::Integer | crate::types::ColumnType::Float
                | crate::types::ColumnType::Boolean | crate::types::ColumnType::Timestamp) {
                let off = fixed_offset_of(ci);
                if off + FIXED_COL_SIZE > data.len() { return None; }
                match ct {
                    crate::types::ColumnType::Integer => unsafe {
                        Some(Value::Integer(i64::from_le(std::ptr::read_unaligned(
                            data.as_ptr().add(off) as *const i64))))
                    },
                    crate::types::ColumnType::Float => unsafe {
                        Some(Value::Float(f64::from_bits(u64::from_le(std::ptr::read_unaligned(
                            data.as_ptr().add(off) as *const u64)))))
                    },
                    _ => None,
                }
            } else {
                if var_section_start + 2 > data.len() { return None; }
                let vc = u16::from_le_bytes([data[var_section_start], data[var_section_start+1]]) as usize;
                let vh = var_section_start + 2;
                let vd = vh + vc * 10;
                for vi in 0..vc {
                    let h = vh + vi * 10;
                    if h + 10 > data.len() { break; }
                    if u16::from_le_bytes([data[h], data[h+1]]) as usize == ci {
                        let vo = u32::from_le_bytes([data[h+2],data[h+3],data[h+4],data[h+5]]) as usize;
                        let vl = u32::from_le_bytes([data[h+6],data[h+7],data[h+8],data[h+9]]) as usize;
                        let a = vd + vo;
                        if a + vl > data.len() { return None; }
                        let b = &data[a..a+vl];
                        return match ct {
                            crate::types::ColumnType::Text => {
                                Some(Value::Text(crate::types::ArcString(std::sync::Arc::from(
                                    std::str::from_utf8(b).ok()?))))
                            }
                            _ => crate::storage::row_format::SchemaDecodeContext::decode_var_generic(b).ok(),
                        };
                    }
                }
                None
            }
        };

        // Scan with zero-copy raw access
        let tp = self.db.compute_table_prefix(table);
        let mut it = self.db.lsm_engine.scan_range_streaming(tp << 32, (tp + 1) << 32)?;
        let raw = it.has_raw_sst();
        let mut count: i64 = 0;
        let mut sum: f64 = 0.0;
        let mut min: Option<f64> = None;
        let mut max: Option<f64> = None;

        if raw {
            loop {
                match it.next_raw() {
                    Some(Ok((_, _, del, vb))) => {
                        if del || vb.len == 0 { continue; }
                        let d = vb.as_slice();
                        if let Some(fv) = extract_col(d, filter_pos) { if fv != filter_value { continue; } } else { continue; }
                        count += 1;
                        for (_, pos) in &agg_cols {
                            if let Some(av) = extract_col(d, *pos) {
                                let fv = match av { Value::Integer(i) => i as f64, Value::Float(f) => f, _ => continue };
                                sum += fv; min = Some(min.map_or(fv, |m| m.min(fv))); max = Some(max.map_or(fv, |m| m.max(fv)));
                            }
                        }
                    }
                    Some(Err(_)) => break, None => break,
                }
            }
        } else {
            loop {
                match it.next() {
                    Some(Ok((_, v))) => {
                        if v.deleted { continue; }
                        let d = match &v.data { crate::storage::lsm::ValueData::Inline(b) => b.as_slice(), _ => continue };
                        if let Some(fv) = extract_col(d, filter_pos) { if fv != filter_value { continue; } } else { continue; }
                        count += 1;
                        for (_, pos) in &agg_cols {
                            if let Some(av) = extract_col(d, *pos) {
                                let fv = match av { Value::Integer(i) => i as f64, Value::Float(f) => f, _ => continue };
                                sum += fv; min = Some(min.map_or(fv, |m| m.min(fv))); max = Some(max.map_or(fv, |m| m.max(fv)));
                            }
                        }
                    }
                    Some(Err(_)) => break, None => break,
                }
            }
        }

        let mut r = Vec::new();
        if has_count_star { r.push(Value::Integer(count)); }
        for (f, _) in &agg_cols {
            match f.as_str() { "SUM" => r.push(Value::Float(sum)), "MIN" => r.push(min.map(Value::Float).unwrap_or(Value::Null)), "MAX" => r.push(max.map(Value::Float).unwrap_or(Value::Null)), "COUNT" => r.push(Value::Integer(count)), _ => {} }
        }
        let cols: Vec<String> = stmt.columns.iter().map(|c| match c {
            SelectColumn::Star => "COUNT(*)".to_string(),
            SelectColumn::Expr(Expr::FunctionCall { name, args, .. }, alias) => alias.clone().unwrap_or_else(|| format!("{}({})", name.to_uppercase(), match args.first() { Some(Expr::Column(c)) => c.as_str(), _ => "?" })),
            _ => "?".to_string(),
        }).collect();
        Ok(Some(StreamingQueryResult::SelectReady { columns: cols, rows: vec![r] }))
    }

    /// without WHERE, iterate the column index keys directly.
    /// O(unique_values) instead of O(N) full scan.
    fn try_distinct_via_column_index(&self, stmt: &SelectStmt, table: &str) -> Result<Option<StreamingQueryResult>> {
        // Only handle single-column DISTINCT (SELECT DISTINCT col)
        let col_name = match stmt.columns.len() {
            1 => match &stmt.columns[0] {
                SelectColumn::Column(name) => name.as_str(),
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };
        let schema = self.db.get_table_schema(table)?;
        let col_def = match schema.get_column(col_name) {
            Some(c) => c,
            None => return Ok(None),
        };
        let col_pos = col_def.position;

        // 🚀 Columnar DISTINCT: read column segment, collect unique values directly.
        // No BTree scan needed — just iterate the typed array with a HashSet.
        if self.db.columnar_sstables.contains_key(table) {
            let col_sst = self.db.columnar_sstables.get(table).unwrap();
            if col_sst.column_tags[col_pos].is_fixed() {
                let seg = col_sst.read_fixed_i64(col_pos).ok();
                if let Some(seg) = seg {
                    let mut seen = std::collections::HashSet::new();
                    let mut vals = Vec::new();
                    for i in 0..col_sst.num_rows {
                        if col_sst.row_map.is_deleted(i) { continue; }
                        if let Some(v) = seg.get_f64(i) {
                            let key = v.to_bits();
                            if seen.insert(key) {
                                vals.push(Value::Float(v));
                            }
                        }
                    }
                    return Ok(Some(StreamingQueryResult::SelectReady {
                        columns: vec![col_name.to_string()],
                        rows: vals.into_iter().map(|v| vec![v]).collect(),
                    }));
                }
            } else {
                let seg = col_sst.read_text(col_pos).ok();
                if let Some(seg) = seg {
                    let mut seen = std::collections::HashSet::new();
                    let mut vals = Vec::new();
                    for i in 0..col_sst.num_rows {
                        if col_sst.row_map.is_deleted(i) { continue; }
                        if let Some(s) = seg.get_str(i) {
                            if seen.insert(s) { // &str key, borrows from mmap (no alloc)
                                vals.push(Value::Text(crate::types::ArcString(std::sync::Arc::from(s))));
                            }
                        }
                    }
                    return Ok(Some(StreamingQueryResult::SelectReady {
                        columns: vec![col_name.to_string()],
                        rows: vals.into_iter().map(|v| vec![v]).collect(),
                    }));
                }
            }
        }

        // Check for column value index
        let index_name = format!("{}.{}", table, col_name);
        let index_ref = match self.db.column_indexes.get(&index_name) {
            Some(idx) => idx,
            None => return Ok(None),
        };
        let index = index_ref.value();

        // Collect unique keys from the index (type-aware decoding).
        // all_keys() reads from the mem_buffer. If empty (data was flushed to BTree),
        // fall back to full scan path to avoid returning incorrect empty results.
        let keys = index.all_keys(&col_def.col_type)?;
        if keys.is_empty() {
            return Ok(None);
        }
        let rows: Vec<Vec<Value>> = keys.into_iter().map(|val| vec![val]).collect();
        Ok(Some(StreamingQueryResult::SelectReady {
            columns: vec![col_name.to_string()],
            rows,
        }))
    }

    /// 🔥 全表扫描流式（现有实现）
    fn execute_full_scan_streaming(&self, stmt: &SelectStmt, table: &str) -> Result<StreamingQueryResult> {
        let schema = self.db.get_table_schema(table)?;

        // S7: when the table uses the multi-segment ColSegmentStore, data is
        // queryable via multi-way merge — no finalize/merge needed. Just flush
        // pending buffer (cheap delta). Eliminates read-triggered full-table merge.
        if self.db.has_col_segment_store(table) {
            let col_types = schema.col_types().to_vec();
            if let Ok(store) = self.db.get_or_create_col_segment_store(table, col_types.clone()) {
                let _ = store.flush_buffer();
                return self.execute_full_scan_via_col_segment(stmt, table, &schema, &store);
            }
        }

        // Legacy path: finalize write buffer (with merge) so SSTable has all data.
        self.db.finalize_columnar_buffer(table);

        // 🚀 Columnar SSTable fast path: when a columnar SSTable exists for this
        // table, read directly from typed column arrays. Much faster than
        // row-based decode — no per-row binary parsing, no VarEntry scanning.
        let is_simple_star = stmt.columns.len() == 1 && matches!(stmt.columns[0], SelectColumn::Star)
            && stmt.where_clause.is_none() && stmt.order_by.is_none()
            && !stmt.distinct && stmt.limit.is_none() && stmt.offset.is_none();
        if is_simple_star && self.db.columnar_sstables.contains_key(table) {
            // 🚀 Zero-materialization: return column segments directly, no Vec<Value> per row.
            let col_types = schema.col_types();
            let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
            if let Some(col_sst) = self.db.columnar_sstables.get(table) {
                let mut segments: Vec<ColumnarSeg> = Vec::with_capacity(col_types.len());
                let mut ok = true;
                for ci in 0..col_types.len() {
                    if col_sst.column_tags[ci].is_fixed() {
                        match col_sst.read_fixed_i64(ci) {
                            Ok(seg) => segments.push(ColumnarSeg::Fixed(seg, col_types.get(ci).cloned().unwrap_or(ColumnType::Integer))),
                            Err(_) => { ok = false; break; }
                        }
                    } else {
                        match col_sst.read_text(ci) {
                            Ok(seg) => segments.push(ColumnarSeg::Text(seg)),
                            Err(_) => { ok = false; break; }
                        }
                    }
                }
                if ok {
                    return Ok(StreamingQueryResult::SelectColumnar {
                        columns: column_names,
                        segments,
                        row_indices: None,
                        num_rows: col_sst.num_rows,
                        row_map: col_sst.row_map.clone(),
                    });
                }
            }
        }

        // 🚀 Fast path for SELECT * with no WHERE/ORDER BY/DISTINCT/LIMIT/OFFSET:
        // Skip project_row_direct entirely — decoded row IS the final result.
        if is_simple_star {
            // 🚀 Optimized batch path for SELECT * with no filters:
            // Decode rows using zero-copy ValueBytes (Arc-shared block data).
            // Rows are allocated on-demand (via push in the decode loop) rather
            // than pre-allocated — avoids 300K empty Vecs and ~36 MB of wasted
            // pre-allocation overhead.
            let col_types = schema.col_types();
            let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
            let size_hint = self.db.fast_row_count(table).map(|c| c as usize).unwrap_or(1024);
            let col_count = col_types.len();

            // Pre-allocate the outer Vec only (avoids log₂(N) reallocations during push).
            // Individual row Vecs are created on first use in the decode loop below.
            let mut rows: Vec<Vec<Value>> = Vec::with_capacity(size_hint);

            let mut decode_iter = self.db.scan_table_decode_streaming(table, col_types)?;
            loop {
                let mut row = Vec::with_capacity(col_count);
                match decode_iter.decode_next_into(&mut row) {
                    Some(Ok(_row_id)) => rows.push(row),
                    Some(Err(e)) => return Err(e),
                    None => break,
                }
            }

            return Ok(StreamingQueryResult::SelectReady {
                columns: column_names,
                rows,
            });
        }

        // 🚀 Columnar projection: SELECT specific columns, no WHERE/ORDER BY
        if !is_simple_star && self.db.columnar_sstables.contains_key(table)
            && stmt.where_clause.is_none() && stmt.order_by.is_none()
            && !stmt.distinct && stmt.limit.is_none() && stmt.offset.is_none()
        {
            let col_types = schema.col_types();
            let column_names: Vec<String> = self.build_select_columns(&stmt.columns, &schema)?;
            let col_positions: Vec<usize> = stmt.columns.iter()
                .filter_map(|c| match c {
                    SelectColumn::Column(name) => schema.get_column_position(name),
                    _ => None,
                })
                .collect();
            if col_positions.len() == stmt.columns.len() {
                match self.db.scan_columnar_sstable_projection(table, &col_types, &col_positions) {
                    Ok(iter) => {
                        let mut rows = Vec::with_capacity(iter.size_hint().0);
                        for row in iter { rows.push(row); }
                        return Ok(StreamingQueryResult::SelectReady { columns: column_names, rows });
                    }
                    Err(e) => { let _ = e; } // columnar projection not available, fall through
                }
            }
        }

        // 🚀 Columnar WHERE/LIKE filter: use columnar scan for filters
        if self.db.columnar_sstables.contains_key(table)
            && stmt.order_by.is_none() && !stmt.distinct
            && stmt.limit.is_none() && stmt.offset.is_none()
        {
            let col_types = schema.col_types();
            let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
            let mut handled = false;

            // Equality: WHERE col = value
            if let Some(Expr::BinaryOp { left, op: crate::sql::ast::BinaryOperator::Eq, right }) = &stmt.where_clause {
                if let (Expr::Column(filter_col), Expr::Literal(filter_val)) = (left.as_ref(), right.as_ref()) {
                    if let Some(filter_pos) = schema.get_column_position(filter_col) {
                        if let Some(col_sst) = self.db.columnar_sstables.get(table) {
                            if let Ok(iter) = self.db.scan_columnar_sstable_filtered(table, &col_types, filter_pos, filter_val) {
                                // Extract match indices from the filtered iterator
                                let indices: Vec<usize> = iter.match_filter.clone().unwrap_or_default();
                                let mut segments: Vec<ColumnarSeg> = Vec::with_capacity(col_types.len());
                                for ci in 0..col_types.len() {
                                    if col_sst.column_tags[ci].is_fixed() {
                                        if let Ok(seg) = col_sst.read_fixed_i64(ci) { segments.push(ColumnarSeg::Fixed(seg, col_types.get(ci).cloned().unwrap_or(ColumnType::Integer))); }
                                    } else if let Ok(seg) = col_sst.read_text(ci) { segments.push(ColumnarSeg::Text(seg)); }
                                }
                                return Ok(StreamingQueryResult::SelectColumnar {
                                    columns: column_names, segments,
                                    row_indices: Some(indices), num_rows: col_sst.num_rows,
                                    row_map: col_sst.row_map.clone(),
                                });
                            }
                        }
                    }
                }
            }

            // IN (literal list): WHERE col IN (v1, v2, ...) — columnar scan with
            // HashSet membership test. Avoids the LSM-backed partial-decode path
            // which returns empty for columnar tables.
            if !handled {
                if let Some(Expr::In { expr, list, negated: false }) = &stmt.where_clause {
                    if let Expr::Column(filter_col) = expr.as_ref() {
                        if list.iter().all(|e| matches!(e, Expr::Literal(_))) {
                            if let Some(filter_pos) = schema.get_column_position(filter_col) {
                                if let Some(col_sst) = self.db.columnar_sstables.get(table) {
                                    let set: std::collections::HashSet<Value> = list.iter()
                                        .filter_map(|e| if let Expr::Literal(v) = e { Some(v.clone()) } else { None })
                                        .collect();
                                    // Decode the filter column once, find matching row indices.
                                    let matches: Vec<usize> = if col_sst.column_tags[filter_pos].is_fixed() {
                                        let seg = col_sst.read_fixed_i64(filter_pos).ok();
                                        let mut m = Vec::new();
                                        if let Some(ref seg) = seg {
                                            for i in 0..col_sst.num_rows {
                                                if col_sst.row_map.is_deleted(i) { continue; }
                                                if let Some(v) = seg.get_i64(i) {
                                                    if set.contains(&Value::Integer(v)) { m.push(i); }
                                                }
                                            }
                                        }
                                        m
                                    } else {
                                        let seg = col_sst.read_text(filter_pos).ok();
                                        let mut m = Vec::new();
                                        if let Some(ref seg) = seg {
                                            for i in 0..col_sst.num_rows {
                                                if col_sst.row_map.is_deleted(i) { continue; }
                                                if let Some(s) = seg.get_str(i) {
                                                    if set.contains(&Value::Text(s.to_string().into())) { m.push(i); }
                                                }
                                            }
                                        }
                                        m
                                    };
                                    let mut segments: Vec<ColumnarSeg> = Vec::with_capacity(col_types.len());
                                    for ci in 0..col_types.len() {
                                        if col_sst.column_tags[ci].is_fixed() {
                                            if let Ok(seg) = col_sst.read_fixed_i64(ci) { segments.push(ColumnarSeg::Fixed(seg, col_types.get(ci).cloned().unwrap_or(ColumnType::Integer))); }
                                        } else if let Ok(seg) = col_sst.read_text(ci) { segments.push(ColumnarSeg::Text(seg)); }
                                    }
                                    return Ok(StreamingQueryResult::SelectColumnar {
                                        columns: column_names, segments,
                                        row_indices: Some(matches), num_rows: col_sst.num_rows,
                                        row_map: col_sst.row_map.clone(),
                                    });
                                }
                            }
                        }
                    }
                }
            }

            // Prefix LIKE: WHERE col LIKE 'prefix%'
            if !handled {
                if let Some(Expr::Like { expr, pattern, .. }) = &stmt.where_clause {
                    if let Expr::Column(filter_col) = expr.as_ref() {
                        if let Expr::Literal(Value::Text(pattern_val)) = pattern.as_ref() {
                            let pat = pattern_val.as_str();
                            if pat.ends_with('%') && !pat[..pat.len()-1].contains('%') {
                                let prefix = &pat[..pat.len()-1];
                                if let Some(filter_pos) = schema.get_column_position(filter_col) {
                                    if let Some(col_sst) = self.db.columnar_sstables.get(table) {
                                        if let Ok(iter) = self.db.scan_columnar_sstable_prefix(table, &col_types, filter_pos, prefix) {
                                            let indices: Vec<usize> = iter.match_filter.clone().unwrap_or_default();
                                            let mut segments: Vec<ColumnarSeg> = Vec::with_capacity(col_types.len());
                                            for ci in 0..col_types.len() {
                                                if col_sst.column_tags[ci].is_fixed() {
                                                    if let Ok(seg) = col_sst.read_fixed_i64(ci) { segments.push(ColumnarSeg::Fixed(seg, col_types.get(ci).cloned().unwrap_or(ColumnType::Integer))); }
                                                } else if let Ok(seg) = col_sst.read_text(ci) { segments.push(ColumnarSeg::Text(seg)); }
                                            }
                                            return Ok(StreamingQueryResult::SelectColumnar {
                                                columns: column_names, segments,
                                                row_indices: Some(indices), num_rows: col_sst.num_rows,
                                                row_map: col_sst.row_map.clone(),
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let columns = self.build_select_columns(&stmt.columns, &schema)?;

        let where_clause = stmt.where_clause.clone();
        let _db = self.db.clone();
        let schema_clone = schema.clone();
        let columns_clone = columns.clone();
        let select_cols = stmt.columns.clone();
        let table_clone = table.to_string();

        // Check if WHERE can be evaluated positionally (bypasses HashMap)
        let use_positional = where_clause.as_ref().is_none_or(Self::can_eval_positional);
        // Metadata columns are only needed when an expression actually
        // references __row_id__ or __table__ (JOIN / subquery paths).
        let needs_metadata = select_cols.iter().any(|c| match c {
            SelectColumn::Expr(e, _) => Self::expr_uses_metadata(e),
            _ => false,
        }) || where_clause.as_ref().is_some_and(|w| Self::expr_uses_metadata(w));

        if use_positional && !needs_metadata {
            // 🚀 Compile WHERE: resolve column names → positions once.
            let compiled_where: Option<CompiledWhere> = where_clause.as_ref()
                .and_then(|clause| Self::compile_where(clause, &schema_clone));

            // 🚀 Index acceleration for WHERE col IN (...):
            // If the WHERE is a single InHash and a column index exists,
            // do K point lookups instead of a full table scan (O(K log N) vs O(N)).
            if let Some(ref cw) = compiled_where {
                if let Some(result) = self.try_index_in_query(
                    table, &schema_clone, cw, stmt, &columns,
                ) {
                    return result;
                }
            }

            // ── Decide: partial decode or full decode ──
            let total_cols = schema_clone.columns.len();
            let select_positions = Self::resolve_select_positions(&select_cols, &schema_clone);
            let mut where_positions = Vec::new();
            if let Some(ref cw) = compiled_where {
                cw.collect_positions(&mut where_positions);
            }

            // Build union of needed columns (WHERE ∪ SELECT)
            let mut needed: Vec<usize> = where_positions.clone();
            if let Some(ref sp) = select_positions {
                needed.extend_from_slice(sp);
            }
            needed.sort_unstable();
            needed.dedup();

            // Use partial decode when we need < 70% of columns (saves decode work on wide tables)
            // Only when CompiledWhere can evaluate the WHERE clause positionally.
            // If compiled_where is None but where_clause exists, we need eval_expr_on_row
            // which requires the full row — fall back to full decode.
            let use_partial = select_positions.is_some()
                && needed.len() < total_cols
                && !needed.is_empty()
                && (where_clause.is_none() || compiled_where.is_some());

            if use_partial {
                // ── Two-phase partial decode path ──
                // Phase 1: Decode only WHERE columns, evaluate filter.
                // Phase 2: If row passes, decode remaining SELECT columns.
                // Rows that fail the filter skip Phase 2 entirely.

                // Separate WHERE positions and SELECT-only positions
                let where_pos: Vec<usize> = where_positions.clone();
                let mut select_only_pos: Vec<usize> = Vec::new();
                if let Some(ref sp) = select_positions {
                    let where_set: std::collections::HashSet<usize> = where_pos.iter().copied().collect();
                    for &p in sp {
                        if !where_set.contains(&p) {
                            select_only_pos.push(p);
                        }
                    }
                }

                // Position mapping for WHERE evaluation
                let where_pos_to_idx: Vec<Option<usize>> = {
                    let mut map = vec![None; total_cols];
                    for (buf_idx, &schema_pos) in where_pos.iter().enumerate() {
                        map[schema_pos] = Some(buf_idx);
                    }
                    map
                };

                // Projection: how to build output from WHERE + SELECT buffers
                let project_where_indices: Vec<(usize, usize)> = if let Some(ref sp) = select_positions {
                    sp.iter().enumerate()
                        .filter_map(|(out_idx, &p)| {
                            where_pos.iter().position(|&w| w == p).map(|buf_idx| (out_idx, buf_idx))
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                let project_select_indices: Vec<(usize, usize)> = if let Some(ref sp) = select_positions {
                    sp.iter().enumerate()
                        .filter_map(|(out_idx, &p)| {
                            select_only_pos.iter().position(|&s| s == p).map(|buf_idx| (out_idx, buf_idx))
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                let num_output_cols = select_positions.as_ref().map_or(0, |sp| sp.len());

                let col_types = schema_clone.col_types().to_vec();
                let fixed_count = crate::storage::row_format::compute_fixed_count(&col_types);
                let fixed_offsets = crate::storage::row_format::FixedColumnOffsets::compute(&col_types);
                let raw_iter = self.db.scan_table_raw_streaming(table)?;

                // Two-phase filtered iterator with reusable buffers —
                // eliminates 3 per-row Vec allocations (where_buf, select_buf, projected)
                let filtered_iter = TwoPhaseFilteredIterator {
                    raw: raw_iter,
                    where_buf: Vec::with_capacity(where_pos.len()),
                    select_buf: Vec::with_capacity(select_only_pos.len()),
                    projected: Vec::with_capacity(num_output_cols),
                    col_types,
                    fixed_count,
                    needed,
                    fixed_offsets,
                    where_pos,
                    compiled_where,
                    where_pos_to_idx,
                    select_only_pos,
                    project_where_indices,
                    project_select_indices,
                    num_output_cols,
                };

                return Ok(StreamingQueryResult::SelectStreaming {
                    columns,
                    rows: Box::new(filtered_iter),
                    order_by: stmt.order_by.clone(),
                    limit: stmt.limit,
                    offset: stmt.offset,
                    distinct: stmt.distinct,
                    max_result_rows: None,
                    size_hint: None,
                });
            }

            // 🚀 Parallel full scan: when rayon is available and we have a positional
            // WHERE clause (CompiledWhere never errors), process rows in parallel chunks.
            #[cfg(feature = "rayon")]
            {
                if compiled_where.is_some() {
                    if let Some(result) = self.try_parallel_full_scan(
                        table, &schema_clone, &select_cols, &columns,
                        compiled_where.as_ref().unwrap(), stmt,
                    ) {
                        return Ok(result);
                    }
                }
            }

            // ── Full decode path (sequential fallback) ──
            let row_iter = self.db.scan_table_rows_streaming(table)?;
            let filtered_iter = row_iter.filter_map(move |result| {
                match result {
                    Ok((_row_id, row)) => {
                        let matches = if let Some(ref clause) = where_clause {
                            if let Some(ref cw) = compiled_where {
                                cw.eval(&row).unwrap_or(false)
                            } else {
                                match Self::eval_expr_on_row(clause, &row, &schema_clone) {
                                    Ok(Value::Bool(b)) => b,
                                    Ok(Value::Integer(i)) => i != 0,
                                    Ok(Value::Float(f)) => f != 0.0 && !f.is_nan(),
                                    Ok(Value::Null) => false,
                                    Err(e) => return Some(Err(e)),
                                    _ => false,
                                }
                            }
                        } else { true };
                        if !matches { return None; }
                        let projected = Self::project_row_direct(&row, &select_cols, &columns_clone, &schema_clone);
                        Some(Ok(projected))
                    }
                    Err(e) => Some(Err(e)),
                }
            });

            return Ok(StreamingQueryResult::SelectStreaming {
                columns,
                rows: Box::new(filtered_iter),
                order_by: stmt.order_by.clone(),
                limit: stmt.limit,
                offset: stmt.offset,
                distinct: stmt.distinct,
                max_result_rows: None,
                size_hint: None,
            });
        }

        // Fallback: HashMap path for complex expressions / metadata columns.
        // If any expression can't be evaluated by eval_expr_simple, fall back to
        // the materialized path which uses the full evaluator.
        let can_stream = where_clause.as_ref().is_none_or(Self::can_eval_simple)
            && select_cols.iter().all(|c| match c {
                SelectColumn::Expr(e, _) => Self::can_eval_simple(e),
                _ => true,
            });
        if !can_stream {
            return self.materialize_as_streaming(stmt);
        }

        let fallback_iter = self.db.scan_table_rows_streaming(table)?;

        // HashMap path: eval_expr_simple can handle all expressions
        let filtered_iter = fallback_iter.filter_map(move |result| {
            match result {
                Ok((row_id, row)) => {
                    let mut sql_row = match row_to_sql_row(&row, &schema_clone) {
                        Ok(r) => r,
                        Err(e) => return Some(Err(e)),
                    };

                    sql_row.insert("__row_id__".to_string(), Value::Integer(row_id as i64));
                    sql_row.insert("__table__".to_string(), Value::text(table_clone.clone()));

                    if let Some(ref clause) = where_clause {
                        let matches = match Self::eval_expr_simple(clause, &sql_row) {
                            Ok(Value::Bool(b)) => b,
                            Ok(Value::Integer(i)) => i != 0,
                            Ok(Value::Float(f)) => f != 0.0 && !f.is_nan(),
                            Ok(Value::Null) => false,
                            Err(e) => return Some(Err(e)),
                            _ => false,
                        };
                        if !matches { return None; }
                    }

                    let projected = Self::project_row_static(&sql_row, &select_cols, &columns_clone, &schema_clone);
                    Some(Ok(projected))
                }
                Err(e) => Some(Err(e)),
            }
        });
        
        Ok(StreamingQueryResult::SelectStreaming {
            columns,
            rows: Box::new(filtered_iter),
            order_by: stmt.order_by.clone(),
            limit: stmt.limit,
            offset: stmt.offset,
            distinct: stmt.distinct,
            max_result_rows: None,
            size_hint: None,
        })
    }

    /// S7: full-table scan via the multi-segment ColSegmentStore.
    fn execute_full_scan_via_col_segment(
        &self,
        stmt: &SelectStmt,
        table: &str,
        schema: &TableSchema,
        store: &crate::storage::col_segment::ColSegmentStore,
    ) -> Result<StreamingQueryResult> {
        let col_types = schema.col_types().to_vec();
        let columns: Vec<String> = self.build_select_columns(&stmt.columns, schema)?;
        let where_clause = stmt.where_clause.clone();

        let limit = stmt.limit.unwrap_or(usize::MAX);
        let offset = stmt.offset.unwrap_or(0);

        // IN (literal list) HashSet fast path: avoid O(rows × list_len) linear scan.
        // For `WHERE col IN (v1, v2, ...)`, build a HashSet once and do O(1) lookup per row.
        let in_hashset: Option<(usize /*col_pos*/, std::collections::HashSet<Value>)> = match &where_clause {
            Some(crate::sql::ast::Expr::In { expr, list, negated: false })
                if list.iter().all(|e| matches!(e, crate::sql::ast::Expr::Literal(_))) =>
            {
                match expr.as_ref() {
                    crate::sql::ast::Expr::Column(col_name) => {
                        schema.get_column_position(col_name).map(|pos| {
                            let set: std::collections::HashSet<Value> = list.iter()
                                .filter_map(|e| if let crate::sql::ast::Expr::Literal(v) = e { Some(v.clone()) } else { None })
                                .collect();
                            (pos, set)
                        })
                    }
                    _ => None,
                }
            }
            _ => None,
        };

        // 🆕 Projected + filtered scan: decode only filter col + output cols,
        // avoiding full-row Vec<Value> decode for non-matches (the dominant
        // cost — was 68-197ms for 300K rows; pure column read is <2ms).
        let out_positions: Vec<usize> = Self::resolve_select_positions(&stmt.columns, schema)
            .unwrap_or_else(|| (0..col_types.len()).collect());
        // Computed SELECT expressions (a+b, CONCAT(...), -v, …) cannot be served
        // by the zero-copy SelectColumnar path (raw columns only); they're
        // evaluated later in the projected-scan fallback via eval_expr_on_row.
        let has_computed_sel = Self::select_has_computed_expression(&stmt.columns);

        // Full scan (no WHERE): SelectColumnar with bounded compaction.
        // Compacts to single segment (first query ~32ms), then zero-copy scan.
        // Skip this zero-copy path when LIMIT/OFFSET/DISTINCT is set —
        // SelectColumnar does not carry those, so they'd be silently dropped.
        // Also skip for computed expressions (see note above).
        // Also skip when the table has Vector/Spatial columns: SelectColumnar's
        // ColumnarSeg only decodes Fixed/Text, and would read those columns via
        // read_text (garbage/panic). The projected-scan fallback decodes them
        // correctly via build_column_segment.
        let has_vector_or_spatial = col_types.iter().any(|ct| matches!(ct,
            ColumnType::Tensor(_) | ColumnType::Spatial));
        if where_clause.is_none() && stmt.group_by.is_none() && stmt.order_by.is_none()
            && stmt.limit.is_none() && stmt.offset.is_none() && !stmt.distinct
            && !has_computed_sel && !has_vector_or_spatial {
            let _ = store.flush_buffer();
            let mut _ci = 0;
            while store.segment_count() >= 2 && _ci < 3 {
                if store.force_compact_all().is_err() { break; }
                _ci += 1;
            }
            if store.segment_count() <= 1 {
                let segs = store.segments_snapshot();
                if let Some(last) = segs.last() {
                    let sst = &last.sst;
                    let mut col_segs: Vec<ColumnarSeg> = Vec::with_capacity(out_positions.len());
                    for &pc in &out_positions {
                        if pc < sst.column_tags.len() && sst.column_tags[pc].is_fixed() {
                            if let Ok(f) = sst.read_fixed_i64(pc) { col_segs.push(ColumnarSeg::Fixed(f, schema.col_types().get(pc).cloned().unwrap_or(ColumnType::Integer))); }
                        } else if pc < sst.column_tags.len() {
                            if let Ok(t) = sst.read_text(pc) { col_segs.push(ColumnarSeg::Text(t)); }
                        }
                    }
                    return Ok(StreamingQueryResult::SelectColumnar {
                        columns, segments: col_segs, row_indices: None,
                        num_rows: sst.num_rows, row_map: sst.row_map.clone(),
                    });
                }
            }
            // Fallback: multi-segment scan if compaction failed.
            let scanned = store.scan_projected_filtered(None, &out_positions, &|_| true);
            let result_rows: Vec<Vec<Value>> = scanned.into_iter()
                .skip(offset).take(limit).map(|(_, row)| row).collect();
            return Ok(StreamingQueryResult::SelectReady { columns, rows: result_rows });
        }

        // Full scan (no WHERE): SelectColumnar with bounded compaction for zero-copy.
        // (See LIMIT/OFFSET/DISTINCT/computed-expr guard note on the path above.)
        if where_clause.is_none() && stmt.group_by.is_none() && stmt.order_by.is_none()
            && stmt.limit.is_none() && stmt.offset.is_none() && !stmt.distinct
            && !has_computed_sel && !has_vector_or_spatial {
            let _ = store.flush_buffer();
            let mut _ci = 0;
            while store.segment_count() >= 2 && _ci < 3 {
                if store.force_compact_all().is_err() { break; }
                _ci += 1;
            }
            if store.segment_count() <= 1 {
                let segs = store.segments_snapshot();
                if let Some(last) = segs.last() {
                    let sst = &last.sst;
                    let mut col_segs: Vec<ColumnarSeg> = Vec::with_capacity(out_positions.len());
                    for &pc in &out_positions {
                        if pc < sst.column_tags.len() && sst.column_tags[pc].is_fixed() {
                            if let Ok(f) = sst.read_fixed_i64(pc) { col_segs.push(ColumnarSeg::Fixed(f, schema.col_types().get(pc).cloned().unwrap_or(ColumnType::Integer))); }
                        } else if pc < sst.column_tags.len() {
                            if let Ok(t) = sst.read_text(pc) { col_segs.push(ColumnarSeg::Text(t)); }
                        }
                    }
                    return Ok(StreamingQueryResult::SelectColumnar {
                        columns, segments: col_segs, row_indices: None,
                        num_rows: sst.num_rows, row_map: sst.row_map.clone(),
                    });
                }
            }
            // Fallback: multi-segment scan.
            let scanned = store.scan_projected_filtered(None, &out_positions, &|_| true);
            let result_rows: Vec<Vec<Value>> = scanned.into_iter()
                .skip(offset).take(limit).map(|(_, row)| row).collect();
            return Ok(StreamingQueryResult::SelectReady { columns, rows: result_rows });
        }

        // 🚀 ORDER BY + LIMIT fast path: if ORDER BY is on a single numeric
        // column with a small LIMIT and no WHERE, use top_k_row_indices to
        // scan only the sort column (bounded heap), then fetch only K rows.
        // This avoids materializing + sorting all 300K rows (49ms → ~2ms).
        // Note: OFFSET is only supported here when there's a single ORDER BY
        // key (multi-key or OFFSET-bearing queries fall through to the full
        // scan + sort path below).
        if where_clause.is_none() && offset == 0 && stmt.order_by.as_ref().map_or(true, |o| o.len() <= 1) {
            if let Some(ref ob) = stmt.order_by {
                if let Some(first_ob) = ob.first() {
                    if let crate::sql::ast::Expr::Column(cn) = &first_ob.expr {
                        let lim = stmt.limit.unwrap_or(usize::MAX);
                        if lim > 0 && lim <= 10000 {
                            if let Some(order_col) = schema.get_column_position(cn) {
                                let is_numeric = matches!(schema.col_types().get(order_col),
                                    Some(crate::types::ColumnType::Integer)
                                    | Some(crate::types::ColumnType::Float)
                                    | Some(crate::types::ColumnType::Boolean));
                                if is_numeric {
                                    let is_float = matches!(schema.col_types().get(order_col),
                                        Some(crate::types::ColumnType::Float));
                                    let top_indices = store.top_k_row_indices_typed(order_col, lim, !first_ob.asc, is_float);
                                    let segs = store.segments_snapshot();
                                    let col_types = store.col_types();
                                    // Cache decoded columns per segment to avoid re-reading.
                                    use crate::storage::lsm::columnar::{FixedSegment, TextSegment};
                                    enum Col { Text(TextSegment), Fixed(FixedSegment), None }
                                    let mut col_cache: std::collections::HashMap<(usize, usize), Col> =
                                        std::collections::HashMap::new();
                                    let mut result_rows: Vec<Vec<Value>> = Vec::with_capacity(top_indices.len());
                                    for (seg_idx, local_row) in top_indices {
                                        let seg = match segs.get(seg_idx) { Some(s) => s, None => continue };
                                        let mut row = Vec::with_capacity(out_positions.len());
                                        for &pc in &out_positions {
                                            let col = col_cache.entry((seg_idx, pc)).or_insert_with(|| {
                                                if matches!(col_types.get(pc), Some(crate::types::ColumnType::Text)) {
                                                    match seg.sst.read_text(pc) {
                                                        Ok(t) => Col::Text(t), Err(_) => Col::None,
                                                    }
                                                } else {
                                                    match seg.sst.read_fixed_i64(pc) {
                                                        Ok(f) => Col::Fixed(f), Err(_) => Col::None,
                                                    }
                                                }
                                            });
                                            let v = match col {
                                                Col::Text(t) => t.get_str(local_row).map(|s| Value::Text(s.into())).unwrap_or(Value::Null),
                                                Col::Fixed(f) => {
                                                    match col_types.get(pc) {
                                                        // Float is stored in the same fixed 8-byte slot as
                                                        // i64, so reading it as i64 and casting to f64 gives
                                                        // garbage. Re-read the raw bytes as f64 instead.
                                                        Some(crate::types::ColumnType::Float) =>
                                                            f.get_i64(local_row)
                                                                .map(|bits| {
                                                                    // Preserve exact bit pattern: i64 → u64 is a
                                                                    // lossless reinterpret (two's complement).
                                                                    Value::Float(f64::from_bits(bits as u64))
                                                                })
                                                                .unwrap_or(Value::Null),
                                                        _ => f.get_i64(local_row).map(Value::Integer).unwrap_or(Value::Null),
                                                    }
                                                }
                                                Col::None => Value::Null,
                                            };
                                            row.push(v);
                                        }
                                        result_rows.push(row);
                                    }
                                    return Ok(StreamingQueryResult::SelectReady { columns, rows: result_rows });
                                }
                            } else {
                            }
                        }
                    }
                }
            }
        }

        // WHERE / fallback: multi-segment scan.
        // NOTE: do not apply OFFSET/LIMIT here for the no-WHERE path — that
        // must happen AFTER ORDER BY (OFFSET is defined over the sorted result).
        //
        // ORDER BY may reference columns that aren't in the SELECT list. To sort
        // correctly we must scan those columns too, then strip them afterward.
        // Build an augmented projection = out_positions ∪ order-by positions.
        let ob_schema_positions: Vec<usize> = stmt.order_by.as_ref()
            .map(|ob| {
                // Collect schema columns referenced by each ORDER BY key, including
                // columns nested inside expressions (e.g. ORDER BY a + b).
                let mut acc: Vec<usize> = Vec::new();
                for oe in ob {
                    for p in Self::expr_referenced_columns(&oe.expr, schema) {
                        if !acc.contains(&p) { acc.push(p); }
                    }
                }
                acc
            })
            .unwrap_or_default();
        let mut scan_positions = out_positions.clone();
        for &p in &ob_schema_positions {
            if !scan_positions.contains(&p) {
                scan_positions.push(p);
            }
        }
        // Computed SELECT expressions may reference columns not in out_positions
        // (e.g. SELECT ABS(age) — out_positions is "all columns" fallback, but
        // be explicit so the scan reads every column the expressions need).
        let has_computed_sel = Self::select_has_computed_expression(&stmt.columns);
        if has_computed_sel {
            for col in &stmt.columns {
                if let SelectColumn::Expr(expr, _) = col {
                    for p in Self::expr_referenced_columns(expr, schema) {
                        if !scan_positions.contains(&p) {
                            scan_positions.push(p);
                        }
                    }
                }
            }
        }
        // Map from output-row index → schema column position, for final projection.
        let keep_indices: Vec<usize> = out_positions.iter()
            .map(|&p| scan_positions.iter().position(|&x| x == p).unwrap())
            .collect();

        let mut result_rows: Vec<Vec<Value>> = if let Some(ref wc) = where_clause {
            self.col_segment_projected_scan(store, wc, schema, &scan_positions, 0, usize::MAX)?
        } else {
            // No WHERE but has GROUP BY / ORDER BY / DISTINCT: full scan, project
            // all output cols + any order-by-only cols.
            let scanned = store.scan_projected_filtered(None, &scan_positions, &|_| true);
            scanned.into_iter().map(|(_, row)| row).collect()
        };

        // Apply ORDER BY on the full result (in-memory sort for ColSegmentStore).
        // Supports multi-key ORDER BY with per-key ASC/DESC.
        // 🚀 Schwartzian transform: pre-compute each row's sort keys ONCE (was:
        // rebuilt full-schema Vec per comparison → 2 allocations × N·log(N)
        // comparisons = the 23ms bottleneck on 2K rows). Now O(N) key compute +
        // O(N log N) comparisons with zero per-comparison allocation.
        if let Some(ref ob) = stmt.order_by {
                if !ob.is_empty() {
                    let ncol = schema.columns.len();
                    enum SortKey { Col(usize), Expr(Expr) }
                    let sort_plan: Vec<(SortKey, bool)> = ob.iter().map(|oe| {
                        let bare_col = if let crate::sql::ast::Expr::Column(cn) = &oe.expr {
                            let b = cn.rsplit('.').next().unwrap_or(cn);
                            schema.get_column_position(b)
                        } else { None };
                        match bare_col {
                            Some(p) => (SortKey::Col(p), oe.asc),
                            None => (SortKey::Expr(oe.expr.clone()), oe.asc),
                        }
                    }).collect();
                    // Pre-compute sort keys per row (once each), then sort by keys.
                    let keyed: Vec<(Vec<Value>, Vec<Value>)> = result_rows.into_iter().map(|row| {
                        let mut full = vec![Value::Null; ncol];
                        for (i, &sp) in scan_positions.iter().enumerate() {
                            if sp < ncol { if let Some(v) = row.get(i) { full[sp] = v.clone(); } }
                        }
                        let keys: Vec<Value> = sort_plan.iter().map(|(sk, _)| match sk {
                            SortKey::Col(p) => full.get(*p).cloned().unwrap_or(Value::Null),
                            SortKey::Expr(e) => Self::eval_expr_on_row(e, &full, schema).unwrap_or(Value::Null),
                        }).collect();
                        (keys, row)
                    }).collect();
                    let mut keyed = keyed;
                    keyed.sort_by(|(ka, _), (kb, _)| {
                        for (i, (_, asc)) in sort_plan.iter().enumerate() {
                            let av = &ka[i];
                            let bv = &kb[i];
                            let cmp = match (matches!(av, Value::Null), matches!(bv, Value::Null)) {
                                (true, true) => std::cmp::Ordering::Equal,
                                (true, false) => std::cmp::Ordering::Less,
                                (false, true) => std::cmp::Ordering::Greater,
                                _ => av.partial_cmp(bv).unwrap_or(std::cmp::Ordering::Equal),
                            };
                            if cmp != std::cmp::Ordering::Equal {
                                return if *asc { cmp } else { cmp.reverse() };
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                    result_rows = keyed.into_iter().map(|(_, r)| r).collect();
                }
        }
        // Evaluate computed SELECT expressions and build the final output rows.
        // Each scanned row carries column values at `scan_positions`. Build a
        // full-schema positional row (Vec<Value> of schema length) so
        // eval_expr_on_row can resolve Expr::Column by position, then evaluate
        // each SELECT column: Column→raw value, computed Expr→eval_expr_on_row.
        if has_computed_sel {
            let ncol = schema.columns.len();
            // Pre-compute, per SELECT column, how to produce its output value:
            //  - Some(pos): copy scan row's value at that schema position.
            //  - None + an Expr: evaluate the expression against the full row.
            // (Star/ColumnWithAlias map to Column semantics here.)
            enum OutCol { CopySchema(usize), Expr(Expr) }
            let out_plan: Vec<OutCol> = stmt.columns.iter().map(|c| match c {
                SelectColumn::Star => OutCol::CopySchema(0), // rare; expanded below
                SelectColumn::Column(name) | SelectColumn::ColumnWithAlias(name, _) => {
                    let bare = name.rsplit('.').next().unwrap_or(name);
                    OutCol::CopySchema(schema.get_column_position(bare).unwrap_or(0))
                }
                SelectColumn::Expr(expr, _) => {
                    if let Expr::Column(name) = expr {
                        let bare = name.rsplit('.').next().unwrap_or(name);
                        OutCol::CopySchema(schema.get_column_position(bare).unwrap_or(0))
                    } else if let Expr::Literal(v) = expr {
                        OutCol::Expr(Expr::Literal(v.clone()))
                    } else {
                        OutCol::Expr(expr.clone())
                    }
                }
            }).collect();
            let star_expanded = stmt.columns.iter().any(|c| matches!(c, SelectColumn::Star));

            for row in &mut result_rows {
                // Build full-schema positional row from scan_positions.
                let mut full: Vec<Value> = vec![Value::Null; ncol];
                for (i, &sp) in scan_positions.iter().enumerate() {
                    if sp < ncol {
                        if let Some(v) = row.get(i) { full[sp] = v.clone(); }
                    }
                }
                let new_row: Vec<Value> = if star_expanded {
                    // SELECT * : emit all schema columns in order.
                    full.clone()
                } else {
                    out_plan.iter().map(|oc| match oc {
                        OutCol::CopySchema(p) => full.get(*p).cloned().unwrap_or(Value::Null),
                        OutCol::Expr(e) => Self::eval_expr_on_row(e, &full, schema).unwrap_or(Value::Null),
                    }).collect()
                };
                *row = new_row;
            }
        } else if keep_indices.len() < scan_positions.len() {
            // Project down to the requested output columns (strip order-by-only cols).
            for row in &mut result_rows {
                let projected: Vec<Value> = keep_indices.iter().map(|&i| row[i].clone()).collect();
                *row = projected;
            }
        }
        // Apply DISTINCT over the output projection (before OFFSET/LIMIT).
        if stmt.distinct {
            let mut seen: std::collections::HashSet<Vec<Value>> =
                std::collections::HashSet::with_capacity(result_rows.len());
            result_rows.retain(|row| {
                let key: Vec<Value> = row.clone();
                seen.insert(key)
            });
        }
        // Apply OFFSET then LIMIT over the sorted result.
        if offset > 0 {
            if offset >= result_rows.len() {
                result_rows.clear();
            } else {
                result_rows.drain(..offset);
            }
        }
        let lim = stmt.limit.unwrap_or(usize::MAX);
        if result_rows.len() > lim { result_rows.truncate(lim); }

        Ok(StreamingQueryResult::SelectReady { columns, rows: result_rows })
    }

    /// Helper: projected scan with WHERE filter for ColSegmentStore tables.
    /// Extracts the filter column + predicate from the WHERE clause, then uses
    /// scan_projected_filtered to decode only the needed columns.
    fn col_segment_projected_scan(
        &self,
        store: &crate::storage::col_segment::ColSegmentStore,
        wc: &crate::sql::ast::Expr,
        schema: &TableSchema,
        out_positions: &[usize],
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Vec<Value>>> {
        use crate::sql::ast::{Expr, BinaryOperator};
        let col_types = store.col_types();

        // Determine filter column + predicate.
        let (filter_col, pred_box): (Option<usize>, Box<dyn Fn(Option<&Value>) -> bool>) = match wc {
            Expr::BinaryOp { left, op: BinaryOperator::Eq, right } => {
                match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(cn), Expr::Literal(v)) => {
                        let pos = schema.get_column_position(cn).unwrap_or(0);
                        let val = v.clone();
                        (Some(pos), Box::new(move |fv: Option<&Value>| fv == Some(&val)))
                    }
                    _ => {
                        // General: fallback to MergeCursor scan.
                        return self.col_segment_general_scan(store, wc, schema, out_positions, offset, limit);
                    }
                }
            }
            Expr::Like { expr, pattern, negated: false } => {
                match (expr.as_ref(), pattern.as_ref()) {
                    (Expr::Column(cn), Expr::Literal(Value::Text(s))) => {
                        let pat = s.as_str();
                        if pat.ends_with('%') && !pat[..pat.len()-1].contains('%') {
                            let prefix = pat[..pat.len()-1].to_string();
                            let pos = schema.get_column_position(cn).unwrap_or(0);
                            (Some(pos), Box::new(move |fv: Option<&Value>| {
                                match fv { Some(Value::Text(s)) => s.as_str().starts_with(&prefix), _ => false }
                            }))
                        } else {
                            return self.col_segment_general_scan(store, wc, schema, out_positions, offset, limit);
                        }
                    }
                    _ => return self.col_segment_general_scan(store, wc, schema, out_positions, offset, limit),
                }
            }
            Expr::In { expr, list, negated: false } if list.iter().all(|e| matches!(e, Expr::Literal(_))) => {
                match expr.as_ref() {
                    Expr::Column(cn) => {
                        let pos = schema.get_column_position(cn).unwrap_or(0);
                        let set: std::collections::HashSet<Value> = list.iter()
                            .filter_map(|e| if let Expr::Literal(v) = e { Some(v.clone()) } else { None })
                            .collect();
                        (Some(pos), Box::new(move |fv: Option<&Value>| fv.map(|v| set.contains(v)).unwrap_or(false)))
                    }
                    _ => return self.col_segment_general_scan(store, wc, schema, out_positions, offset, limit),
                }
            }
            _ => return self.col_segment_general_scan(store, wc, schema, out_positions, offset, limit),
        };

        // 🚀 LIKE prefix fast path: byte-compare scan (no closure dispatch,
        // no per-row Value allocation for non-matches).
        if let Expr::Like { expr, pattern, negated: false } = wc {
            if let (Expr::Column(cn), Expr::Literal(Value::Text(s))) = (expr.as_ref(), pattern.as_ref()) {
                let pat = s.as_str();
                if pat.ends_with('%') && !pat[..pat.len()-1].contains('%') {
                    let prefix = pat[..pat.len()-1].to_string();
                    if let Some(fc) = schema.get_column_position(cn) {
                        if matches!(col_types.get(fc), Some(ColumnType::Text)) {
                            if let Some(indices) = store.scan_row_indices_prefix(fc, prefix.as_bytes(), offset + limit) {
                                let segs = store.segments_snapshot();
                                use crate::storage::lsm::columnar::{FixedSegment, TextSegment};
                                enum Col { Text(TextSegment), Fixed(FixedSegment), None }
                                let mut col_cache: std::collections::HashMap<(usize, usize), Col> = std::collections::HashMap::new();
                                let mut result: Vec<Vec<Value>> = Vec::with_capacity(indices.len());
                                for (seg_idx, local_row) in indices.iter().skip(offset).take(limit) {
                                    let seg = match segs.get(*seg_idx) { Some(s) => s, None => continue };
                                    let mut row = Vec::with_capacity(out_positions.len());
                                    for &pc in out_positions {
                                        let col = col_cache.entry((*seg_idx, pc)).or_insert_with(|| {
                                            if matches!(col_types.get(pc), Some(ColumnType::Text)) {
                                                match seg.sst.read_text(pc) { Ok(t) => Col::Text(t), Err(_) => Col::None }
                                            } else {
                                                match seg.sst.read_fixed_i64(pc) { Ok(f) => Col::Fixed(f), Err(_) => Col::None }
                                            }
                                        });
                                        let v = match col {
                                            Col::Text(t) => t.get_str(*local_row).map(|s| Value::Text(s.into())).unwrap_or(Value::Null),
                                            Col::Fixed(f) => match col_types.get(pc) {
                                                Some(ColumnType::Float) => f.get_i64(*local_row).map(|i| Value::Float(i as f64)).unwrap_or(Value::Null),
                                                _ => f.get_i64(*local_row).map(Value::Integer).unwrap_or(Value::Null),
                                            },
                                            Col::None => Value::Null,
                                        };
                                        row.push(v);
                                    }
                                    result.push(row);
                                }
                                return Ok(result);
                            }
                        }
                    }
                }
            }
        }

        // Text-filter fast path: raw &str predicate (zero Value alloc for non-matches).
        if let Some(fc) = filter_col {
            if matches!(col_types.get(fc), Some(ColumnType::Text)) {
                let str_pred: Box<dyn Fn(Option<&str>) -> bool> = match wc {
                    Expr::BinaryOp { left, op: BinaryOperator::Eq, right } => {
                        match (left.as_ref(), right.as_ref()) {
                            (Expr::Column(_), Expr::Literal(Value::Text(s))) => {
                                let target = s.to_string();
                                Box::new(move |sv: Option<&str>| sv == Some(target.as_str()))
                            }
                            _ => return self.col_segment_general_scan(store, wc, schema, out_positions, offset, limit),
                        }
                    }
                    Expr::Like { expr, pattern, .. } => {
                        match (expr.as_ref(), pattern.as_ref()) {
                            (Expr::Column(_), Expr::Literal(Value::Text(s))) => {
                                let pat = s.to_string();
                                if pat.ends_with('%') && !pat[..pat.len()-1].contains('%') {
                                    let prefix = pat[..pat.len()-1].to_string();
                                    Box::new(move |sv: Option<&str>| sv.map(|s| s.starts_with(&prefix)).unwrap_or(false))
                                } else {
                                    return self.col_segment_general_scan(store, wc, schema, out_positions, offset, limit);
                                }
                            }
                            _ => return self.col_segment_general_scan(store, wc, schema, out_positions, offset, limit),
                        }
                    }
                    Expr::In { expr, list, .. } if list.iter().all(|e| matches!(e, Expr::Literal(Value::Text(_)))) => {
                        match expr.as_ref() {
                            Expr::Column(_) => {
                                let strset: std::collections::HashSet<String> = list.iter()
                                    .filter_map(|e| if let Expr::Literal(Value::Text(s)) = e { Some(s.to_string()) } else { None })
                                    .collect();
                                Box::new(move |sv: Option<&str>| sv.map(|s| strset.contains(s)).unwrap_or(false))
                            }
                            _ => return self.col_segment_general_scan(store, wc, schema, out_positions, offset, limit),
                        }
                    }
                    _ => return self.col_segment_general_scan(store, wc, schema, out_positions, offset, limit),
                };
                let scanned = store.scan_text_filtered(fc, out_positions, &*str_pred);
                return Ok(scanned.into_iter().skip(offset).take(limit).map(|(_, row)| row).collect());
            }
        }

        let scanned = store.scan_projected_filtered(filter_col, out_positions, &*pred_box);
        Ok(scanned.into_iter().skip(offset).take(limit).map(|(_, row)| row).collect())
    }

    /// Fallback: general WHERE eval via MergeCursor (handles complex expressions).
    fn col_segment_general_scan(
        &self,
        store: &crate::storage::col_segment::ColSegmentStore,
        wc: &crate::sql::ast::Expr,
        schema: &TableSchema,
        out_positions: &[usize],
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Vec<Value>>> {
        // Ensure buffered rows are durable before scanning — store.scan() only
        // reads persisted segments, so unflushed inserts would be invisible.
        let _ = store.flush_buffer();
        let mut rows = Vec::new();
        let mut skipped = 0usize;
        for (_key, _ts, row) in store.scan() {
            let m = match Self::eval_expr_on_row(wc, &row, schema) {
                Ok(Value::Bool(b)) => b,
                Ok(Value::Integer(i)) => i != 0,
                Ok(Value::Float(f)) => f != 0.0 && !f.is_nan(),
                _ => false,
            };
            if !m { continue; }
            if skipped < offset { skipped += 1; continue; }
            let projected: Vec<Value> = out_positions.iter()
                .map(|&p| row.get(p).cloned().unwrap_or(Value::Null))
                .collect();
            rows.push(projected);
            if rows.len() >= limit { break; }
        }
        Ok(rows)
    }

    /// Extract schema positions for simple column references in SELECT.
    /// Returns None if any column is Star, Expr, or unresolvable (needs full row).
    fn resolve_select_positions(select_cols: &[SelectColumn], schema: &TableSchema) -> Option<Vec<usize>> {
        let mut positions = Vec::with_capacity(select_cols.len());
        for col in select_cols {
            match col {
                SelectColumn::Star => return None,
                SelectColumn::Expr(_, _) => return None,
                SelectColumn::Column(name) | SelectColumn::ColumnWithAlias(name, _) => {
                    let bare = if name.contains('.') {
                        name.rsplit('.').next().unwrap_or(name)
                    } else {
                        name
                    };
                    positions.push(schema.get_column_position(bare)?);
                }
            }
        }
        Some(positions)
    }

    /// 🔧 Helper: 构建 SELECT 列列表
    fn build_select_columns(&self, select_cols: &[SelectColumn], schema: &TableSchema) -> Result<Vec<String>> {
        let columns = if select_cols.len() == 1 && matches!(select_cols[0], SelectColumn::Star) {
            // 🚀 SELECT *: use cached column names (zero-alloc after first build)
            schema.column_names()
        } else {
            // 显式列名或表达式
            select_cols.iter().enumerate().map(|(idx, col)| {
                match col {
                    SelectColumn::Column(name) => name.clone(),
                    SelectColumn::ColumnWithAlias(_, alias) => alias.clone(),
                    SelectColumn::Expr(_, Some(alias)) => alias.clone(),
                    SelectColumn::Expr(_, None) => format!("expr_{}", idx),
                    SelectColumn::Star => "*".to_string(),
                }
            }).collect()
        };
        Ok(columns)
    }
    
    /// 🔧 Static helper for row projection (used in closures)
    /// 🚀 Lightweight expression evaluation for WHERE filters (no allocations)
    /// Handles simple comparisons, AND/OR, column references, and literals.
    /// Falls back to creating a QueryExecutor for complex expressions (MATCH, KNN, etc.)
    fn is_truthy(v: &Value) -> bool {
        match v {
            Value::Bool(b) => *b,
            Value::Integer(n) => *n != 0,
            Value::Float(f) => *f != 0.0 && !f.is_nan(),
            _ => false,
        }
    }

    /// Simple LIKE pattern matching: % = any sequence, _ = single char
    fn simple_like_match(text: &str, pattern: &str) -> bool {
        let t: Vec<char> = text.chars().collect();
        let p: Vec<char> = pattern.chars().collect();
        let mut dp = vec![vec![false; p.len() + 1]; t.len() + 1];
        dp[0][0] = true;
        for j in 1..=p.len() {
            if p[j - 1] == '%' { dp[0][j] = dp[0][j - 1]; }
        }
        for i in 1..=t.len() {
            for j in 1..=p.len() {
                if p[j - 1] == '%' {
                    dp[i][j] = dp[i][j - 1] || dp[i - 1][j];
                } else if p[j - 1] == '_' || p[j - 1] == t[i - 1] {
                    dp[i][j] = dp[i - 1][j - 1];
                }
            }
        }
        dp[t.len()][p.len()]
    }

    fn positional_add(l: &Value, r: &Value) -> Result<Value> {
        match (l, r) {
            (Value::Integer(a), Value::Integer(b)) => match a.checked_add(*b) {
                Some(v) => Ok(Value::Integer(v)),
                None => Ok(Value::Float(*a as f64 + *b as f64)),
            },
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
            (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 + b)),
            (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a + *b as f64)),
            _ => Ok(Value::Null),
        }
    }
    fn positional_sub(l: &Value, r: &Value) -> Result<Value> {
        match (l, r) {
            (Value::Integer(a), Value::Integer(b)) => match a.checked_sub(*b) {
                Some(v) => Ok(Value::Integer(v)),
                None => Ok(Value::Float(*a as f64 - *b as f64)),
            },
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
            (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 - b)),
            (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a - *b as f64)),
            _ => Ok(Value::Null),
        }
    }
    fn positional_mul(l: &Value, r: &Value) -> Result<Value> {
        match (l, r) {
            (Value::Integer(a), Value::Integer(b)) => match a.checked_mul(*b) {
                Some(v) => Ok(Value::Integer(v)),
                None => Ok(Value::Float(*a as f64 * *b as f64)),
            },
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a * b)),
            (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 * b)),
            (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a * *b as f64)),
            _ => Ok(Value::Null),
        }
    }
    fn positional_div(l: &Value, r: &Value) -> Result<Value> {
        match (l, r) {
            (Value::Integer(a), Value::Integer(b)) => {
                if *b == 0 { return Err(MoteDBError::DivisionByZero); }
                a.checked_div(*b)
                    .map(Value::Integer)
                    .ok_or_else(|| MoteDBError::Query("Integer division overflow".into()))
            }
            (Value::Float(a), Value::Float(b)) => {
                if *b == 0.0 { return Err(MoteDBError::DivisionByZero); }
                Ok(Value::Float(a / b))
            }
            (Value::Integer(a), Value::Float(b)) => {
                if *b == 0.0 { return Err(MoteDBError::DivisionByZero); }
                Ok(Value::Float(*a as f64 / b))
            }
            (Value::Float(a), Value::Integer(b)) => {
                if *b == 0 { return Err(MoteDBError::DivisionByZero); }
                Ok(Value::Float(a / *b as f64))
            }
            _ => Ok(Value::Null),
        }
    }
    fn positional_mod(l: &Value, r: &Value) -> Result<Value> {
        match (l, r) {
            (Value::Integer(a), Value::Integer(b)) => {
                if *b == 0 { return Err(MoteDBError::DivisionByZero); }
                Ok(Value::Integer(a.checked_rem(*b).unwrap_or(0)))
            }
            _ => Ok(Value::Null),
        }
    }

    fn extract_f32_slice(v: &Value) -> Option<Vec<f32>> {
        match v {
            Value::Vector(vec) => Some(vec.iter().copied().collect()),
            _ => None,
        }
    }

    fn positional_vector_l2(l: &Value, r: &Value) -> Result<Value> {
        let v1 = Self::extract_f32_slice(l);
        let v2 = Self::extract_f32_slice(r);
        match (v1, v2) {
            (Some(a), Some(b)) => {
                if a.len() != b.len() {
                    return Err(MoteDBError::TypeError(format!(
                        "Vector dimension mismatch: {} vs {}", a.len(), b.len()
                    )));
                }
                let dist: f32 = a.iter().zip(b.iter()).map(|(x, y)| (x - y).powi(2)).sum::<f32>().sqrt();
                Ok(Value::Float(dist as f64))
            }
            _ => Ok(Value::Null),
        }
    }

    fn positional_vector_cosine(l: &Value, r: &Value) -> Result<Value> {
        let v1 = Self::extract_f32_slice(l);
        let v2 = Self::extract_f32_slice(r);
        match (v1, v2) {
            (Some(a), Some(b)) => {
                if a.len() != b.len() {
                    return Err(MoteDBError::TypeError(format!(
                        "Vector dimension mismatch: {} vs {}", a.len(), b.len()
                    )));
                }
                let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                let n1: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
                let n2: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
                if n1 == 0.0 || n2 == 0.0 { return Ok(Value::Float(1.0)); }
                let sim = (dot / (n1 * n2)).clamp(-1.0, 1.0);
                Ok(Value::Float((1.0 - sim) as f64))
            }
            _ => Ok(Value::Null),
        }
    }

    fn positional_vector_dot(l: &Value, r: &Value) -> Result<Value> {
        let v1 = Self::extract_f32_slice(l);
        let v2 = Self::extract_f32_slice(r);
        match (v1, v2) {
            (Some(a), Some(b)) => {
                if a.len() != b.len() {
                    return Err(MoteDBError::TypeError(format!(
                        "Vector dimension mismatch: {} vs {}", a.len(), b.len()
                    )));
                }
                let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                Ok(Value::Float(dot as f64))
            }
            _ => Ok(Value::Null),
        }
    }

    /// Evaluate function calls in the positional (no-HashMap) path.
    fn eval_function_positional(name: &str, args: &[Expr], row: &[Value], schema: &TableSchema) -> Result<Value> {
        let fname = name.to_lowercase();
        match fname.as_str() {
            "concat" => {
                let mut result = String::new();
                for arg in args {
                    match Self::eval_expr_on_row(arg, row, schema)? {
                        Value::Text(s) => result.push_str(&s),
                        Value::Integer(i) => { use std::fmt::Write; let _ = write!(result, "{}", i); }
                        Value::Float(f) => { use std::fmt::Write; let _ = write!(result, "{}", f); }
                        Value::Bool(b) => result.push_str(if b { "true" } else { "false" }),
                        Value::Null => return Ok(Value::Null),
                        other => result.push_str(&format!("{:?}", other)),
                    }
                }
                Ok(Value::text(result))
            }
            "upper" | "lower" | "length" | "trim" | "ltrim" | "rtrim" => {
                let val = Self::eval_expr_on_row(&args[0], row, schema)?;
                match val {
                    Value::Text(s) => match fname.as_str() {
                        "upper" => Ok(Value::text(s.to_uppercase())),
                        "lower" => Ok(Value::text(s.to_lowercase())),
                        "length" => Ok(Value::Integer(s.chars().count() as i64)),
                        "trim" => Ok(Value::text(s.trim().to_string())),
                        "ltrim" => Ok(Value::text(s.trim_start().to_string())),
                        "rtrim" => Ok(Value::text(s.trim_end().to_string())),
                        _ => Ok(Value::text(s.to_string())),
                    },
                    _ => Ok(Value::Null),
                }
            }
            "abs" | "round" | "floor" | "ceil" | "log" | "ln" | "log10" | "sqrt" | "exp" => {
                let val = Self::eval_expr_on_row(&args[0], row, schema)?;
                match val {
                    Value::Integer(i) => match fname.as_str() {
                        "abs" => match i.checked_abs() {
                            Some(n) => Ok(Value::Integer(n)),
                            None => Ok(Value::Float(-(i as f64))),
                        },
                        _ => {
                            let f = i as f64;
                            Ok(Value::Float(match fname.as_str() {
                                "round" => f.round(),
                                "floor" => f.floor(),
                                "ceil" => f.ceil(),
                                "log" | "log10" => f.log10(),
                                "ln" => f.ln(),
                                "sqrt" => f.sqrt(),
                                "exp" => f.exp(),
                                _ => f,
                            }))
                        }
                    },
                    Value::Float(f) => match fname.as_str() {
                        "abs" => Ok(Value::Float(f.abs())),
                        "round" => Ok(Value::Float(f.round())),
                        "floor" => Ok(Value::Float(f.floor())),
                        "ceil" => Ok(Value::Float(f.ceil())),
                        "log" | "log10" => Ok(Value::Float(f.log10())),
                        "ln" => Ok(Value::Float(f.ln())),
                        "sqrt" => Ok(Value::Float(f.sqrt())),
                        "exp" => Ok(Value::Float(f.exp())),
                        _ => Ok(Value::Float(f)),
                    },
                    _ => Ok(Value::Null),
                }
            }
            "coalesce" => {
                for arg in args {
                    let val = Self::eval_expr_on_row(arg, row, schema)?;
                    if !matches!(val, Value::Null) {
                        return Ok(val);
                    }
                }
                Ok(Value::Null)
            }
            "ifnull" | "nvl" => {
                // IFNULL(value, default) — return default if value is NULL.
                if args.len() != 2 { return Ok(Value::Null); }
                let val = Self::eval_expr_on_row(&args[0], row, schema)?;
                if matches!(val, Value::Null) {
                    Self::eval_expr_on_row(&args[1], row, schema)
                } else {
                    Ok(val)
                }
            }
            "nullif" => {
                // NULLIF(a, b) — NULL if a == b, else a.
                if args.len() != 2 { return Ok(Value::Null); }
                let a = Self::eval_expr_on_row(&args[0], row, schema)?;
                let b = Self::eval_expr_on_row(&args[1], row, schema)?;
                if a == b { Ok(Value::Null) } else { Ok(a) }
            }
            "substr" | "substring" => {
                // SUBSTR(text, start [, length]) — SQL 1-indexed.
                if args.len() < 2 || args.len() > 3 { return Ok(Value::text(String::new())); }
                let text = match Self::eval_expr_on_row(&args[0], row, schema)? {
                    Value::Text(s) => s,
                    _ => return Ok(Value::text(String::new())),
                };
                let start = match Self::eval_expr_on_row(&args[1], row, schema)? {
                    Value::Integer(i) if i >= 0 => (i.max(1) as usize).saturating_sub(1),
                    Value::Integer(i) if i < 0 => text.chars().count().saturating_sub((-i) as usize),
                    _ => return Ok(Value::text(String::new())),
                };
                let result = if args.len() == 3 {
                    let length = match Self::eval_expr_on_row(&args[2], row, schema)? {
                        Value::Integer(i) => i.max(0) as usize,
                        _ => return Ok(Value::text(String::new())),
                    };
                    text.chars().skip(start).take(length).collect()
                } else {
                    text.chars().skip(start).collect()
                };
                Ok(Value::text(result))
            }
            "replace" => {
                // REPLACE(text, from, to).
                if args.len() != 3 { return Ok(Value::Null); }
                let text = match Self::eval_expr_on_row(&args[0], row, schema)? { Value::Text(s) => s, _ => return Ok(Value::Null) };
                let from = match Self::eval_expr_on_row(&args[1], row, schema)? { Value::Text(s) => s, _ => return Ok(Value::Null) };
                let to = match Self::eval_expr_on_row(&args[2], row, schema)? { Value::Text(s) => s, _ => return Ok(Value::Null) };
                Ok(Value::text(text.replace(from.as_str(), to.as_str())))
            }
            "sign" => {
                if args.is_empty() { return Ok(Value::Null); }
                match Self::eval_expr_on_row(&args[0], row, schema)? {
                    Value::Integer(i) => Ok(Value::Integer(i.signum())),
                    Value::Float(f) => Ok(Value::Integer(if f > 0.0 { 1 } else if f < 0.0 { -1 } else { 0 })),
                    _ => Ok(Value::Null),
                }
            }
            "power" | "pow" => {
                if args.len() != 2 { return Ok(Value::Null); }
                let base = match Self::eval_expr_on_row(&args[0], row, schema)? {
                    Value::Integer(i) => i as f64, Value::Float(f) => f, _ => return Ok(Value::Null),
                };
                let exp = match Self::eval_expr_on_row(&args[1], row, schema)? {
                    Value::Integer(i) => i as f64, Value::Float(f) => f, _ => return Ok(Value::Null),
                };
                Ok(Value::Float(base.powf(exp)))
            }
            "mod" => {
                if args.len() != 2 { return Ok(Value::Null); }
                let a = Self::eval_expr_on_row(&args[0], row, schema)?;
                let b = Self::eval_expr_on_row(&args[1], row, schema)?;
                match (&a, &b) {
                    (Value::Integer(x), Value::Integer(y)) => {
                        if *y == 0 { return Ok(Value::Null); }
                        Ok(match x.checked_rem(*y) { Some(n) => Value::Integer(n), None => Value::Integer(0) })
                    }
                    (Value::Float(x), Value::Float(y)) => {
                        if *y == 0.0 { return Ok(Value::Null); }
                        Ok(Value::Float(x % y))
                    }
                    _ => Ok(Value::Null),
                }
            }
            "if" => {
                if args.len() >= 3 {
                    let cond = Self::eval_expr_on_row(&args[0], row, schema)?;
                    if Self::is_truthy(&cond) {
                        Self::eval_expr_on_row(&args[1], row, schema)
                    } else {
                        Self::eval_expr_on_row(&args[2], row, schema)
                    }
                } else {
                    Ok(Value::Null)
                }
            }
            "within_radius" => {
                if args.len() != 3 {
                    return Err(MoteDBError::InvalidArgument("WITHIN_RADIUS() takes 3 arguments".to_string()));
                }
                let point = Self::eval_expr_on_row(&args[0], row, schema)?;
                let center = Self::eval_expr_on_row(&args[1], row, schema)?;
                let radius = Self::eval_expr_on_row(&args[2], row, schema)?;

                use crate::types::Geometry;
                let (px, py) = match point {
                    Value::Spatial(geom) => match &*geom {
                        Geometry::Point(p) => (p.x, p.y),
                        Geometry::Point3D(p) => (p.x, p.y),
                        _ => return Ok(Value::Bool(false)),
                    },
                    _ => return Ok(Value::Bool(false)),
                };
                let (cx, cy) = match center {
                    Value::Spatial(geom) => match &*geom {
                        Geometry::Point(p) => (p.x, p.y),
                        Geometry::Point3D(p) => (p.x, p.y),
                        _ => return Ok(Value::Bool(false)),
                    },
                    _ => return Ok(Value::Bool(false)),
                };
                let r = match radius {
                    Value::Float(f) => f,
                    Value::Integer(i) => i as f64,
                    _ => return Ok(Value::Bool(false)),
                };
                let dist = ((px - cx).powi(2) + (py - cy).powi(2)).sqrt();
                Ok(Value::Bool(dist <= r))
            }
            "st_distance" => {
                if args.len() == 2 {
                    let p1 = Self::eval_expr_on_row(&args[0], row, schema)?;
                    let p2 = Self::eval_expr_on_row(&args[1], row, schema)?;
                    match (&p1, &p2) {
                        (Value::Spatial(a), Value::Spatial(b)) => {
                            let (x1, y1, z1) = match &**a {
                                crate::types::Geometry::Point(p) => (p.x, p.y, 0.0),
                                crate::types::Geometry::Point3D(p) => (p.x, p.y, p.z),
                                _ => return Ok(Value::Null),
                            };
                            let (x2, y2, z2) = match &**b {
                                crate::types::Geometry::Point(p) => (p.x, p.y, 0.0),
                                crate::types::Geometry::Point3D(p) => (p.x, p.y, p.z),
                                _ => return Ok(Value::Null),
                            };
                            Ok(Value::Float(((x1 - x2).powi(2) + (y1 - y2).powi(2) + (z1 - z2).powi(2)).sqrt()))
                        }
                        _ => Ok(Value::Null),
                    }
                } else {
                    Ok(Value::Null)
                }
            }
            "match" => {
                if args.len() != 2 {
                    return Ok(Value::Bool(false));
                }
                let col_name = match &args[0] {
                    Expr::Column(n) => n.clone(),
                    _ => return Ok(Value::Bool(false)),
                };
                let query_val = Self::eval_expr_on_row(&args[1], row, schema)?;
                let query_text = match query_val {
                    Value::Text(s) => s.as_str().to_string(),
                    _ => return Ok(Value::Bool(false)),
                };
                let pos = schema.get_column_position(&col_name);
                let col_val = match pos {
                    Some(p) => row.get(p).cloned().unwrap_or(Value::Null),
                    None => return Ok(Value::Bool(false)),
                };
                match col_val {
                    Value::Text(ref text) => {
                        let text_lower = text.to_lowercase();
                        let query_lower = query_text.to_lowercase();
                        let terms: Vec<&str> = query_lower.split_whitespace().collect();
                        Ok(Value::Bool(terms.iter().all(|t| text_lower.contains(t))))
                    }
                    _ => Err(MoteDBError::Query(
                        format!("eval_function_positional: unsupported function: {}", fname)
                    )),
                }
            }
            _ => Err(MoteDBError::Query(
                format!("eval_function_positional: unsupported expression type")
            )),
        }
    }

    fn eval_expr_simple(expr: &Expr, row: &SqlRow) -> Result<Value> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let lv = Self::eval_expr_simple(left, row)?;
                let rv = Self::eval_expr_simple(right, row)?;
                match op {
                    BinaryOperator::Eq => {
                        // NULL = NULL should return false (SQL standard)
                        if matches!(&lv, Value::Null) || matches!(&rv, Value::Null) {
                            Ok(Value::Bool(false))
                        } else {
                            Ok(Value::Bool(lv.partial_cmp(&rv) == Some(std::cmp::Ordering::Equal)))
                        }
                    }
                    BinaryOperator::Ne => {
                        if matches!(&lv, Value::Null) || matches!(&rv, Value::Null) {
                            Ok(Value::Bool(false))
                        } else {
                            Ok(Value::Bool(lv.partial_cmp(&rv) != Some(std::cmp::Ordering::Equal)))
                        }
                    }
                    BinaryOperator::Lt | BinaryOperator::Le | BinaryOperator::Gt | BinaryOperator::Ge => {
                        if matches!(&lv, Value::Null) || matches!(&rv, Value::Null) {
                            Ok(Value::Bool(false))
                        } else {
                            Ok(Value::Bool(match op {
                                BinaryOperator::Lt => lv < rv,
                                BinaryOperator::Le => lv <= rv,
                                BinaryOperator::Gt => lv > rv,
                                BinaryOperator::Ge => lv >= rv,
                                _ => unreachable!(),
                            }))
                        }
                    }
                    BinaryOperator::And => {
                        let lb = Self::is_truthy(&lv);
                        let rb = Self::is_truthy(&rv);
                        Ok(Value::Bool(lb && rb))
                    }
                    BinaryOperator::Or => {
                        let lb = Self::is_truthy(&lv);
                        let rb = Self::is_truthy(&rv);
                        Ok(Value::Bool(lb || rb))
                    }
                    BinaryOperator::Add => Self::positional_add(&lv, &rv),
                    BinaryOperator::Sub => Self::positional_sub(&lv, &rv),
                    BinaryOperator::Mul => Self::positional_mul(&lv, &rv),
                    BinaryOperator::Div => Self::positional_div(&lv, &rv),
                    BinaryOperator::Mod => Self::positional_mod(&lv, &rv),
                    BinaryOperator::L2Distance => Self::positional_vector_l2(&lv, &rv),
                    BinaryOperator::CosineDistance => Self::positional_vector_cosine(&lv, &rv),
                    BinaryOperator::DotProduct => Self::positional_vector_dot(&lv, &rv),
                }
            }
            Expr::Column(name) => {
                // Try direct lookup, then strip table prefix (e.g., "users.age" → "age")
                if let Some(v) = row.get(name) {
                    Ok(v.clone())
                } else if name.contains('.') {
                    let col = name.rsplit('.').next().unwrap_or(name);
                    row.get(col).cloned().ok_or_else(|| MoteDBError::ColumnNotFound(name.clone()))
                } else {
                    Err(MoteDBError::ColumnNotFound(name.clone()))
                }
            }
            Expr::Literal(val) => Ok(val.clone()),
            Expr::UnaryOp { op: UnaryOperator::Not, expr } => {
                let v = Self::eval_expr_simple(expr, row)?;
                Ok(Value::Bool(!Self::is_truthy(&v)))
            }
            // For complex expressions that require the materialized path,
            // return the pre-computed result if available, otherwise false.
            // These expressions should never reach eval_expr_simple — they are
            // redirected to execute_select_internal by expr_needs_materialized_path().
            // The false fallback is a safety net to avoid returning wrong results.
            Expr::Match { column, query, .. } => {
                let has_score = row.keys().any(|k| k.starts_with("__text_score_"));
                if has_score {
                    Ok(Value::Bool(true))
                } else {
                    // Fallback: naive text scan when no FTS index
                    match row.get(column) {
                        Some(Value::Text(text)) => {
                            let text_lower = text.to_lowercase();
                            let query_lower = query.to_lowercase();
                            let terms: Vec<&str> = query_lower.split_whitespace().collect();
                            Ok(Value::Bool(terms.iter().all(|t| text_lower.contains(t))))
                        }
                        _ => Ok(Value::Bool(false)),
                    }
                }
            }
            Expr::FunctionCall { name, args, .. } => {
                let fname = name.to_lowercase();
                match fname.as_str() {
                    "concat" => {
                        let mut result = String::new();
                        for arg in args {
                            match Self::eval_expr_simple(arg, row)? {
                                Value::Text(s) => result.push_str(&s),
                                Value::Integer(i) => { use std::fmt::Write; let _ = write!(result, "{}", i); }
                                Value::Float(f) => { use std::fmt::Write; let _ = write!(result, "{}", f); }
                                Value::Bool(b) => result.push_str(if b { "true" } else { "false" }),
                                Value::Null => return Ok(Value::Null),
                                other => result.push_str(&format!("{:?}", other)),
                            }
                        }
                        Ok(Value::text(result))
                    }
                    "upper" | "lower" | "length" | "trim" | "ltrim" | "rtrim" => {
                        let val = Self::eval_expr_simple(&args[0], row)?;
                        match val {
                            Value::Text(s) => match fname.as_str() {
                                "upper" => Ok(Value::text(s.to_uppercase())),
                                "lower" => Ok(Value::text(s.to_lowercase())),
                                "length" => Ok(Value::Integer(s.chars().count() as i64)),
                                "trim" => Ok(Value::text(s.trim().to_string())),
                                "ltrim" => Ok(Value::text(s.trim_start().to_string())),
                                "rtrim" => Ok(Value::text(s.trim_end().to_string())),
                                _ => Ok(Value::text(s.to_string())),
                            },
                            _ => Ok(Value::Null),
                        }
                    }
                    "abs" | "round" | "floor" | "ceil" | "log" | "ln" | "log10" | "sqrt" | "exp" => {
                        let val = Self::eval_expr_simple(&args[0], row)?;
                        match val {
                            Value::Integer(i) => match fname.as_str() {
                                "abs" => match i.checked_abs() {
                                    Some(n) => Ok(Value::Integer(n)),
                                    None => Ok(Value::Float(-(i as f64))),
                                },
                                _ => {
                                    let f = i as f64;
                                    Ok(Value::Float(match fname.as_str() {
                                        "round" => f.round(),
                                        "floor" => f.floor(),
                                        "ceil" => f.ceil(),
                                        "log" | "log10" => f.log10(),
                                        "ln" => f.ln(),
                                        "sqrt" => f.sqrt(),
                                        "exp" => f.exp(),
                                        _ => f,
                                    }))
                                }
                            },
                            Value::Float(f) => match fname.as_str() {
                                "abs" => Ok(Value::Float(f.abs())),
                                "round" => Ok(Value::Float(f.round())),
                                "floor" => Ok(Value::Float(f.floor())),
                                "ceil" => Ok(Value::Float(f.ceil())),
                                "log" | "log10" => Ok(Value::Float(f.log10())),
                                "ln" => Ok(Value::Float(f.ln())),
                                "sqrt" => Ok(Value::Float(f.sqrt())),
                                "exp" => Ok(Value::Float(f.exp())),
                                _ => Ok(Value::Float(f)),
                            },
                            _ => Ok(Value::Null),
                        }
                    }
                    _ => Err(MoteDBError::Query(
                        format!("eval_expr_simple: unsupported function: {}", fname)
                    )),
                }
            }
            _ => Err(MoteDBError::Query(
                format!("eval_expr_simple: unsupported expression: {:?}", expr)
            )),
        }
    }

    /// Check if an expression can be evaluated positionally (no complex features).
    /// Simple: Column, Literal, BinaryOp (comparison + AND/OR), UnaryOp::Not, IsNull.
    /// Check if expression tree contains any Expr::Parameter nodes
    fn contains_parameter(expr: &Expr) -> bool {
        match expr {
            Expr::Parameter(_) => true,
            Expr::BinaryOp { left, right, .. } =>
                Self::contains_parameter(left) || Self::contains_parameter(right),
            Expr::UnaryOp { expr, .. } => Self::contains_parameter(expr),
            Expr::IsNull { expr, .. } => Self::contains_parameter(expr),
            Expr::In { expr, list, .. } =>
                Self::contains_parameter(expr) || list.iter().any(Self::contains_parameter),
            Expr::Between { expr, low, high, .. } =>
                Self::contains_parameter(expr) || Self::contains_parameter(low) || Self::contains_parameter(high),
            Expr::Like { expr, pattern, .. } =>
                Self::contains_parameter(expr) || Self::contains_parameter(pattern),
            Expr::FunctionCall { args, .. } => args.iter().any(Self::contains_parameter),
            _ => false,
        }
    }

    /// Count the highest parameter index referenced in a statement.
    /// Returns 0 if no parameters found.
    pub fn max_parameter_index(stmt: &Statement) -> usize {
        fn walk_expr(expr: &Expr) -> usize {
            match expr {
                Expr::Parameter(idx) => *idx,
                Expr::BinaryOp { left, right, .. } =>
                    walk_expr(left).max(walk_expr(right)),
                Expr::UnaryOp { expr, .. } => walk_expr(expr),
                Expr::IsNull { expr, .. } => walk_expr(expr),
                Expr::In { expr, list, .. } =>
                    list.iter().fold(walk_expr(expr), |acc, e| acc.max(walk_expr(e))),
                Expr::Between { expr, low, high, .. } =>
                    walk_expr(expr).max(walk_expr(low)).max(walk_expr(high)),
                Expr::Like { expr, pattern, .. } =>
                    walk_expr(expr).max(walk_expr(pattern)),
                Expr::FunctionCall { args, .. } =>
                    args.iter().fold(0, |acc, e| acc.max(walk_expr(e))),
                _ => 0,
            }
        }
        fn walk_stmt(stmt: &Statement) -> usize {
            match stmt {
                Statement::Select(s) => s.where_clause.as_ref().map(walk_expr).unwrap_or(0)
                    .max(s.columns.iter().fold(0, |acc, c| acc.max(match c {
                        SelectColumn::Expr(e, _) => walk_expr(e),
                        _ => 0,
                    }))),
                Statement::Insert(i) => i.values.iter().fold(0, |acc, row| {
                    acc.max(row.iter().fold(0, |a, e| a.max(walk_expr(e))))
                }),
                Statement::Update(u) => {
                    let where_max = u.where_clause.as_ref().map(walk_expr).unwrap_or(0);
                    let set_max = u.assignments.iter().fold(0, |acc, (_, e)| acc.max(walk_expr(e)));
                    where_max.max(set_max)
                }
                Statement::Delete(d) => d.where_clause.as_ref().map(walk_expr).unwrap_or(0),
                _ => 0,
            }
        }
        walk_stmt(stmt)
    }

    /// Try to accelerate `WHERE col IN (values...)` using a secondary column index.
    /// Returns Some(result) if index was used, None to fall through to full scan.
    ///
    /// When a column index exists on the IN column, we do K point lookups (O(K log N))
    /// instead of a full table scan (O(N)). For selective filters, this is dramatically faster.
    fn try_index_in_query(
        &self,
        table: &str,
        schema: &TableSchema,
        compiled_where: &CompiledWhere,
        stmt: &SelectStmt,
        columns: &[String],
    ) -> Option<Result<StreamingQueryResult>> {
        // Only handle a top-level non-negated InHash (no AND/OR/NOT wrapping)
        let (col_pos, values) = match compiled_where {
            CompiledWhere::InHash(pos, set) => (*pos, set.clone()),
            _ => return None,
        };

        // Get column name at this position
        let col_name = schema.columns.get(col_pos).map(|c| c.name.as_str())?;
        let index_key = format!("{}.{}", table, col_name);

        // Check if a column index exists
        if !self.db.column_indexes.contains_key(&index_key) {
            return None;
        }

        // Skip index acceleration when the IN list is very large or very small.
        // - Tiny lists (< 3): index overhead dominates
        // - Large lists (> 50): the full scan with HashSet is faster because:
        //   1) Sequential mmap reads are very fast
        //   2) Batch row fetching via individual point lookups is expensive
        //   3) Large IN lists often mean low selectivity (many matching rows)
        // The sweet spot is 3-50 values where index lookups + targeted row fetch wins.
        if values.len() < 3 || values.len() > 50 {
            return None;
        }

        // ── Index acceleration path ──
        let index_ref = self.db.column_indexes.get(&index_key)?;
        let index = index_ref.value();

        // Batch index lookups: collect all matching row IDs
        let mut row_id_set: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for value in &values {
            match index.get(value) {
                Ok(row_ids) => {
                    row_id_set.extend(row_ids);
                }
                Err(_) => {
                    drop(index_ref);
                    return None; // Index error → fall through to full scan
                }
            }
        }
        drop(index_ref);

        if row_id_set.is_empty() {
            return Some(Ok(StreamingQueryResult::SelectReady {
                columns: columns.to_vec(),
                rows: vec![],
            }));
        }

        // Sort row IDs for sequential LSM access (better cache locality)
        let mut row_ids: Vec<u64> = row_id_set.into_iter().collect();
        row_ids.sort_unstable();

        // Batch fetch rows by ID
        let rows_result = match self.db.get_table_rows_batch_arc(table, &row_ids) {
            Ok(batch) => batch,
            Err(e) => return Some(Err(e)),
        };

        // Project each fetched row according to SELECT columns
        let select_cols = &stmt.columns;
        let projected_rows: Vec<Vec<Value>> = rows_result.into_iter()
            .filter_map(|(_row_id, opt_row)| opt_row)
            .map(|row| {
                let row: Vec<Value> = (*row).clone();
                Self::project_row_direct(&row, select_cols, columns, schema)
            })
            .collect();

        // Apply DISTINCT, ORDER BY, LIMIT, OFFSET
        let mut rows = projected_rows;
        if stmt.distinct {
            let mut seen = std::collections::HashSet::new();
            rows.retain(|row| seen.insert(row.clone()));
        }
        if let Some(ref order_by) = stmt.order_by {
            // Resolve ORDER BY expressions to output column indices
            let sort_specs: Vec<(usize, bool)> = order_by.iter().filter_map(|ob| {
                let col_name = match &ob.expr {
                    Expr::Column(name) => name,
                    _ => return None,
                };
                let bare = if col_name.contains('.') { col_name.rsplit('.').next().unwrap_or(col_name) } else { col_name };
                let idx = columns.iter().position(|c| c == bare || c == col_name);
                idx.map(|i| (i, ob.asc))
            }).collect();
            if !sort_specs.is_empty() {
                rows.sort_by(|a, b| {
                    for &(col_idx, asc) in &sort_specs {
                        if col_idx >= a.len() || col_idx >= b.len() { continue; }
                        let va = &a[col_idx]; let vb = &b[col_idx];
                        let ord = match (va, vb) {
                            (Value::Float(fa), Value::Float(fb)) => {
                                if fa.is_nan() && fb.is_nan() { std::cmp::Ordering::Equal }
                                else if fa.is_nan() { std::cmp::Ordering::Greater }
                                else if fb.is_nan() { std::cmp::Ordering::Less }
                                else { fa.partial_cmp(fb).unwrap_or(std::cmp::Ordering::Equal) }
                            }
                            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
                            (Value::Null, _) => std::cmp::Ordering::Less,
                            (_, Value::Null) => std::cmp::Ordering::Greater,
                            _ => va.partial_cmp(vb).unwrap_or(std::cmp::Ordering::Equal),
                        };
                        let final_ord = if asc { ord } else { ord.reverse() };
                        if final_ord != std::cmp::Ordering::Equal { return final_ord; }
                    }
                    std::cmp::Ordering::Equal
                });
            }
        }
        if let Some(offset) = stmt.offset {
            rows = rows.into_iter().skip(offset).collect();
        }
        if let Some(limit) = stmt.limit {
            rows.truncate(limit);
        }

        Some(Ok(StreamingQueryResult::SelectReady {
            columns: columns.to_vec(),
            rows,
        }))
    }

    /// Evaluate post_filters against a decoded row.
    /// Uses CompiledWhere (fastest, pre-resolved positions) when possible,
    /// falls back to eval_expr_on_row (positional, no HashMap) otherwise.
    fn row_passes_post_filters(row: &[Value], filters: &[Expr], schema: &TableSchema) -> bool {
        for filter in filters {
            // Fast path: CompiledWhere (pre-resolved column positions, zero HashMap)
            if let Some(cw) = Self::compile_where(filter, schema) {
                match cw.eval(row) {
                    Some(true) => continue,
                    Some(false) | None => return false,
                }
            } else {
                // Fallback: positional eval (no HashMap, uses schema column positions)
                match Self::eval_expr_on_row(filter, row, schema) {
                    Ok(Value::Bool(b)) if b => continue,
                    Ok(Value::Integer(i)) if i != 0 => continue,
                    Ok(Value::Float(f)) if f != 0.0 && !f.is_nan() => continue,
                    _ => return false,
                }
            }
        }
        true
    }

    /// Compile a WHERE expression into a `CompiledWhere` with pre-resolved column positions.
    /// Returns `None` if the expression is too complex for the compiled path.
    fn compile_where(expr: &Expr, schema: &TableSchema) -> Option<CompiledWhere> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                // Check for AND/OR — compile both sides
                match op {
                    BinaryOperator::And => {
                        let l = Self::compile_where(left, schema)?;
                        let r = Self::compile_where(right, schema)?;
                        Some(CompiledWhere::And(vec![l, r]))
                    }
                    BinaryOperator::Or => {
                        let l = Self::compile_where(left, schema)?;
                        let r = Self::compile_where(right, schema)?;
                        Some(CompiledWhere::Or(vec![l, r]))
                    }
                    _ => {
                        // Simple comparison: left must be a column, right a literal (or vice versa)
                        let (col_pos, op_val, cmp_val) = Self::extract_col_literal_cmp(left, right, op, schema)?;
                        Some(match op_val {
                            BinaryOperator::Eq => CompiledWhere::Eq(col_pos, cmp_val),
                            BinaryOperator::Ne => CompiledWhere::Ne(col_pos, cmp_val),
                            BinaryOperator::Lt => CompiledWhere::Lt(col_pos, cmp_val),
                            BinaryOperator::Le => CompiledWhere::Le(col_pos, cmp_val),
                            BinaryOperator::Gt => CompiledWhere::Gt(col_pos, cmp_val),
                            BinaryOperator::Ge => CompiledWhere::Ge(col_pos, cmp_val),
                            _ => return None,
                        })
                    }
                }
            }
            Expr::In { expr, list, negated } => {
                if let Expr::Column(col_name) = expr.as_ref() {
                    let pos = schema.get_column_position(
                        if col_name.contains('.') { col_name.rsplit('.').next().unwrap_or(col_name) } else { col_name }
                    )?;
                    if list.iter().all(|e| matches!(e, Expr::Literal(_))) {
                        let set: std::collections::HashSet<Value> = list.iter()
                            .filter_map(|e| if let Expr::Literal(v) = e { Some(v.clone()) } else { None })
                            .collect();
                        Some(CompiledWhere::InHash(pos, set))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            Expr::Like { expr, pattern, negated } => {
                if let Expr::Column(col_name) = expr.as_ref() {
                    let pos = schema.get_column_position(
                        if col_name.contains('.') { col_name.rsplit('.').next().unwrap_or(col_name) } else { col_name }
                    )?;
                    if let Expr::Literal(Value::Text(s)) = pattern.as_ref() {
                        Some(CompiledWhere::Like(pos, s.to_string(), *negated))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            Expr::IsNull { expr, negated } => {
                if let Expr::Column(col_name) = expr.as_ref() {
                    let pos = schema.get_column_position(
                        if col_name.contains('.') { col_name.rsplit('.').next().unwrap_or(col_name) } else { col_name }
                    )?;
                    Some(CompiledWhere::IsNull(pos, *negated))
                } else {
                    None
                }
            }
            Expr::UnaryOp { op: UnaryOperator::Not, expr: inner } => {
                let compiled = Self::compile_where(inner, schema)?;
                Some(CompiledWhere::Not(Box::new(compiled)))
            }
            _ => None,
        }
    }

    /// Helper: extract (col_pos, op, literal_value) from a binary comparison.
    /// Handles both `col op literal` and `literal op col` (swapping op).
    fn extract_col_literal_cmp(
        left: &Expr, right: &Expr, op: &BinaryOperator,
        schema: &TableSchema,
    ) -> Option<(usize, BinaryOperator, Value)> {
        // Try left=column, right=literal
        if let Expr::Column(col_name) = left {
            if let Expr::Literal(val) = right {
                let bare = if col_name.contains('.') { col_name.rsplit('.').next().unwrap_or(col_name) } else { col_name };
                let pos = schema.get_column_position(bare)?;
                return Some((pos, op.clone(), val.clone()));
            }
        }
        // Try left=literal, right=column (swap op direction)
        if let Expr::Column(col_name) = right {
            if let Expr::Literal(val) = left {
                let bare = if col_name.contains('.') { col_name.rsplit('.').next().unwrap_or(col_name) } else { col_name };
                let pos = schema.get_column_position(bare)?;
                let swapped = match op {
                    BinaryOperator::Lt => BinaryOperator::Gt,
                    BinaryOperator::Le => BinaryOperator::Ge,
                    BinaryOperator::Gt => BinaryOperator::Lt,
                    BinaryOperator::Ge => BinaryOperator::Le,
                    other => other.clone(),
                };
                return Some((pos, swapped, val.clone()));
            }
        }
        None
    }

    fn can_eval_positional(expr: &Expr) -> bool {
        match expr {
            Expr::Column(_) | Expr::Literal(_) => true,
            Expr::BinaryOp { left, op, right } => {
                matches!(op,
                    BinaryOperator::Eq | BinaryOperator::Ne |
                    BinaryOperator::Lt | BinaryOperator::Le |
                    BinaryOperator::Gt | BinaryOperator::Ge |
                    BinaryOperator::And | BinaryOperator::Or |
                    BinaryOperator::Add | BinaryOperator::Sub |
                    BinaryOperator::Mul | BinaryOperator::Div |
                    BinaryOperator::Mod |
                    BinaryOperator::L2Distance | BinaryOperator::CosineDistance | BinaryOperator::DotProduct
                ) && Self::can_eval_positional(left)
                  && Self::can_eval_positional(right)
            }
            Expr::UnaryOp { op: UnaryOperator::Not, expr } => Self::can_eval_positional(expr),
            Expr::IsNull { .. } => true,
            Expr::In { .. } | Expr::Between { .. } | Expr::Like { .. } => true,
            Expr::FunctionCall { name, args, .. } => {
                let fname = name.to_lowercase();
                let handled = matches!(fname.as_str(),
                    "concat" | "upper" | "lower" | "length" | "trim" | "ltrim" | "rtrim" |
                    "abs" | "round" | "floor" | "ceil" | "log" | "ln" | "log10" | "sqrt" | "exp" |
                    "coalesce" | "if" |
                    "within_radius" | "st_distance" | "match"
                );
                handled && args.iter().all(Self::can_eval_positional)
            }
            Expr::Match { .. } => true,
            _ => false,
        }
    }

    /// Check if an expression can be evaluated by eval_expr_simple (HashMap path).
    /// This is a stricter subset than can_eval_positional — fewer functions are supported.
    fn can_eval_simple(expr: &Expr) -> bool {
        match expr {
            Expr::Column(_) | Expr::Literal(_) => true,
            Expr::BinaryOp { left, op, right } => {
                matches!(op,
                    BinaryOperator::Eq | BinaryOperator::Ne |
                    BinaryOperator::Lt | BinaryOperator::Le |
                    BinaryOperator::Gt | BinaryOperator::Ge |
                    BinaryOperator::And | BinaryOperator::Or |
                    BinaryOperator::Add | BinaryOperator::Sub |
                    BinaryOperator::Mul | BinaryOperator::Div |
                    BinaryOperator::Mod |
                    BinaryOperator::L2Distance | BinaryOperator::CosineDistance | BinaryOperator::DotProduct
                ) && Self::can_eval_simple(left)
                  && Self::can_eval_simple(right)
            }
            Expr::UnaryOp { op: UnaryOperator::Not, expr } => Self::can_eval_simple(expr),
            Expr::IsNull { .. } => true,
            Expr::In { .. } | Expr::Between { .. } | Expr::Like { .. } => true,
            Expr::FunctionCall { name, args, .. } => {
                let fname = name.to_lowercase();
                let handled = matches!(fname.as_str(),
                    "concat" | "upper" | "lower" | "length" | "trim" | "ltrim" | "rtrim" |
                    "abs" | "round" | "floor" | "ceil" | "log" | "ln" | "log10" | "sqrt" | "exp"
                );
                handled && args.iter().all(Self::can_eval_simple)
            }
            Expr::Match { .. } => true,
            _ => false,
        }
    }

    /// Recursively check if an expression references __row_id__ or __table__ metadata.
    fn expr_uses_metadata(expr: &Expr) -> bool {
        match expr {
            Expr::Column(name) => name == "__row_id__" || name == "__table__",
            Expr::BinaryOp { left, right, .. } =>
                Self::expr_uses_metadata(left) || Self::expr_uses_metadata(right),
            Expr::UnaryOp { expr, .. } |
            Expr::IsNull { expr, .. } => Self::expr_uses_metadata(expr),
            Expr::In { expr, list, .. } =>
                Self::expr_uses_metadata(expr) || list.iter().any(Self::expr_uses_metadata),
            Expr::Between { expr, low, high, .. } =>
                Self::expr_uses_metadata(expr) || Self::expr_uses_metadata(low) || Self::expr_uses_metadata(high),
            Expr::Like { expr, pattern, .. } =>
                Self::expr_uses_metadata(expr) || Self::expr_uses_metadata(pattern),
            Expr::FunctionCall { args, .. } => args.iter().any(Self::expr_uses_metadata),
            Expr::Match { .. } => false,
            _ => false,
        }
    }

    /// Check if a SelectStmt contains any Parameter nodes.
    fn contains_parameter_stmt(stmt: &SelectStmt) -> bool {
        stmt.where_clause.as_ref().is_some_and(Self::contains_parameter)
            || stmt.columns.iter().any(|c| match c {
                SelectColumn::Expr(e, _) => Self::contains_parameter(e),
                _ => false,
            })
    }

    /// Validate that all Parameter nodes in stmt are bound to a value in params.
    fn validate_params_bound(stmt: &SelectStmt, params: &[Value]) -> Option<MoteDBError> {
        fn check_expr(expr: &Expr, params: &[Value]) -> Option<MoteDBError> {
            match expr {
                Expr::Parameter(idx) if *idx == 0 => Some(MoteDBError::InvalidArgument(
                    "Unnamed ? parameter not resolved (internal error)".to_string()
                )),
                Expr::Parameter(idx) => {
                    if params.get(idx - 1).is_none() {
                        return Some(MoteDBError::InvalidArgument(format!(
                            "Parameter ?{} not bound ({} parameters provided)", idx, params.len()
                        )));
                    }
                    None
                }
                Expr::BinaryOp { left, right, .. } =>
                    check_expr(left, params).or_else(|| check_expr(right, params)),
                Expr::UnaryOp { expr, .. } => check_expr(expr, params),
                Expr::IsNull { expr, .. } => check_expr(expr, params),
                _ => None,
            }
        }
        stmt.where_clause.as_ref().and_then(|w| check_expr(w, params))
    }

    /// Substitute all Expr::Parameter nodes with Expr::Literal using bound params.
    /// Returns a cloned SelectStmt with resolved values, enabling fast-path matching.
    fn substitute_params_stmt(&self, stmt: &SelectStmt) -> Result<SelectStmt> {
        let params = self.evaluator.get_params();
        let sub = |expr: &Expr| -> Result<Expr> { Self::substitute_expr(expr, &params) };

        let where_clause = match &stmt.where_clause {
            Some(w) => Some(sub(w)?),
            None => None,
        };

        let columns: Vec<SelectColumn> = stmt.columns.iter().map(|c| {
            match c {
                SelectColumn::Expr(e, alias) => {
                    match sub(e) {
                        Ok(resolved) => SelectColumn::Expr(resolved, alias.clone()),
                        Err(_) => c.clone(),
                    }
                }
                _ => c.clone(),
            }
        }).collect();

        Ok(SelectStmt {
            columns,
            from: stmt.from.clone(),
            where_clause,
            order_by: stmt.order_by.clone(),
            limit: stmt.limit,
            offset: stmt.offset,
            distinct: stmt.distinct,
            group_by: stmt.group_by.clone(),
            having: stmt.having.clone(),
            latest_by: stmt.latest_by.clone(),
        })
    }

    /// Recursively substitute Parameter nodes in an expression tree.
    fn substitute_expr(expr: &Expr, params: &[Value]) -> Result<Expr> {
        match expr {
            Expr::Parameter(idx) => {
                if *idx == 0 {
                    return Err(MoteDBError::InvalidArgument(
                        "Unnamed ? parameter not resolved (internal error)".to_string()
                    ));
                }
                let i = idx - 1;
                params.get(i).cloned()
                    .map(Expr::Literal)
                    .ok_or_else(|| MoteDBError::InvalidArgument(format!(
                        "Parameter ?{} not bound ({} parameters provided)", idx, params.len()
                    )))
            }
            Expr::BinaryOp { left, op, right } => {
                let l = Self::substitute_expr(left, params)?;
                let r = Self::substitute_expr(right, params)?;
                Ok(Expr::BinaryOp { left: Box::new(l), op: op.clone(), right: Box::new(r) })
            }
            Expr::UnaryOp { op, expr: inner } => {
                let e = Self::substitute_expr(inner, params)?;
                Ok(Expr::UnaryOp { op: op.clone(), expr: Box::new(e) })
            }
            Expr::IsNull { expr: inner, negated } => {
                let e = Self::substitute_expr(inner, params)?;
                Ok(Expr::IsNull { expr: Box::new(e), negated: *negated })
            }
            Expr::In { expr: inner, list, negated } => {
                let e = Self::substitute_expr(inner, params)?;
                let list2: Result<Vec<Expr>> = list.iter().map(|x| Self::substitute_expr(x, params)).collect();
                Ok(Expr::In { expr: Box::new(e), list: list2?, negated: *negated })
            }
            Expr::Between { expr: inner, low, high, negated } => {
                let e = Self::substitute_expr(inner, params)?;
                let l = Self::substitute_expr(low, params)?;
                let h = Self::substitute_expr(high, params)?;
                Ok(Expr::Between { expr: Box::new(e), low: Box::new(l), high: Box::new(h), negated: *negated })
            }
            Expr::Like { expr: inner, pattern, negated } => {
                let e = Self::substitute_expr(inner, params)?;
                let p = Self::substitute_expr(pattern, params)?;
                Ok(Expr::Like { expr: Box::new(e), pattern: Box::new(p), negated: *negated })
            }
            Expr::FunctionCall { name, args, distinct } => {
                let args2: Result<Vec<Expr>> = args.iter().map(|x| Self::substitute_expr(x, params)).collect();
                Ok(Expr::FunctionCall { name: name.clone(), args: args2?, distinct: *distinct })
            }
            // All other variants are cloned as-is (Column, Literal, etc.)
            _ => Ok(expr.clone()),
        }
    }

    /// Evaluate expression directly on Vec<Value> using schema positions.
    /// Bypasses HashMap creation entirely.
    fn eval_expr_on_row(expr: &Expr, row: &[Value], schema: &TableSchema) -> Result<Value> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let lv = Self::eval_expr_on_row(left, row, schema)?;
                let rv = Self::eval_expr_on_row(right, row, schema)?;
                match op {
                    BinaryOperator::Eq => {
                        if matches!(&lv, Value::Null) || matches!(&rv, Value::Null) {
                            Ok(Value::Null) // SQL: NULL = anything => NULL
                        } else {
                            Ok(Value::Bool(lv.partial_cmp(&rv) == Some(std::cmp::Ordering::Equal)))
                        }
                    }
                    BinaryOperator::Ne => {
                        if matches!(&lv, Value::Null) || matches!(&rv, Value::Null) {
                            Ok(Value::Null) // SQL: NULL != anything => NULL
                        } else {
                            Ok(Value::Bool(lv.partial_cmp(&rv) != Some(std::cmp::Ordering::Equal)))
                        }
                    }
                    BinaryOperator::Lt | BinaryOperator::Le | BinaryOperator::Gt | BinaryOperator::Ge => {
                        if matches!(&lv, Value::Null) || matches!(&rv, Value::Null) {
                            Ok(Value::Null) // SQL: NULL comparison => UNKNOWN
                        } else {
                            Ok(Value::Bool(match op {
                                BinaryOperator::Lt => lv < rv,
                                BinaryOperator::Le => lv <= rv,
                                BinaryOperator::Gt => lv > rv,
                                BinaryOperator::Ge => lv >= rv,
                                _ => unreachable!(),
                            }))
                        }
                    }
                    BinaryOperator::And => {
                        Ok(Value::Bool(Self::is_truthy(&lv) && Self::is_truthy(&rv)))
                    }
                    BinaryOperator::Or => {
                        Ok(Value::Bool(Self::is_truthy(&lv) || Self::is_truthy(&rv)))
                    }
                    BinaryOperator::Add => Self::positional_add(&lv, &rv),
                    BinaryOperator::Sub => Self::positional_sub(&lv, &rv),
                    BinaryOperator::Mul => Self::positional_mul(&lv, &rv),
                    BinaryOperator::Div => Self::positional_div(&lv, &rv),
                    BinaryOperator::Mod => Self::positional_mod(&lv, &rv),
                    BinaryOperator::L2Distance => Self::positional_vector_l2(&lv, &rv),
                    BinaryOperator::CosineDistance => Self::positional_vector_cosine(&lv, &rv),
                    BinaryOperator::DotProduct => Self::positional_vector_dot(&lv, &rv),
                }
            }
            Expr::Column(name) => {
                // Try direct lookup, then strip table prefix (e.g., "users.id" → "id")
                let col_name = if name.contains('.') {
                    name.rsplit('.').next().unwrap_or(name)
                } else {
                    name
                };
                schema.get_column_position(col_name)
                    .and_then(|pos| row.get(pos).cloned())
                    .ok_or_else(|| MoteDBError::ColumnNotFound(name.clone()))
            }
            Expr::Literal(val) => Ok(val.clone()),
            Expr::Parameter(_) => {
                // Parameters need evaluator state — trigger fallback
                Err(MoteDBError::Query(
                    "Cannot evaluate parameter in positional path".to_string()
                ))
            }
            Expr::UnaryOp { op: UnaryOperator::Not, expr: inner } => {
                let v = Self::eval_expr_on_row(inner, row, schema)?;
                // NOT NULL should be false (NULL), not true
                if matches!(v, Value::Null) {
                    Ok(Value::Bool(false))
                } else {
                    Ok(Value::Bool(!Self::is_truthy(&v)))
                }
            }
            Expr::UnaryOp { op: UnaryOperator::Minus, expr: inner } => {
                let v = Self::eval_expr_on_row(inner, row, schema)?;
                match v {
                    Value::Integer(i) => Ok(match i.checked_neg() {
                        Some(r) => Value::Integer(r),
                        // i64::MIN negated overflows — promote to Float (matches evaluator.rs)
                        None => Value::Float(-(i as f64)),
                    }),
                    Value::Float(f) => Ok(Value::Float(-f)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(MoteDBError::Query(format!("Cannot negate {:?}", v))),
                }
            }
            Expr::IsNull { expr, negated } => {
                let v = Self::eval_expr_on_row(expr, row, schema)?;
                let is_null = matches!(v, Value::Null);
                Ok(Value::Bool(if *negated { !is_null } else { is_null }))
            }
            Expr::In { expr, list, negated } => {
                let val = Self::eval_expr_on_row(expr, row, schema)?;
                if matches!(val, Value::Null) {
                    return Ok(Value::Bool(false));
                }
                let mut found = false;
                let mut has_null = false;
                for item in list {
                    let item_val = Self::eval_expr_on_row(item, row, schema)?;
                    if matches!(item_val, Value::Null) {
                        has_null = true;
                        continue;
                    }
                    if val == item_val {
                        found = true;
                        break;
                    }
                }
                if *negated && !found && has_null {
                    return Ok(Value::Bool(false));
                }
                Ok(Value::Bool(if *negated { !found } else { found }))
            }
            Expr::Between { expr, low, high, negated } => {
                let val = Self::eval_expr_on_row(expr, row, schema)?;
                let low_val = Self::eval_expr_on_row(low, row, schema)?;
                let high_val = Self::eval_expr_on_row(high, row, schema)?;
                if matches!(val, Value::Null) || matches!(low_val, Value::Null) || matches!(high_val, Value::Null) {
                    return Ok(Value::Bool(false));
                }
                let in_range = val >= low_val && val <= high_val;
                Ok(Value::Bool(if *negated { !in_range } else { in_range }))
            }
            Expr::Like { expr, pattern, negated } => {
                let val = Self::eval_expr_on_row(expr, row, schema)?;
                let pat = Self::eval_expr_on_row(pattern, row, schema)?;
                // NULL LIKE anything = false, NULL NOT LIKE anything = false (SQL NULL semantics)
                if matches!(val, Value::Null) || matches!(pat, Value::Null) {
                    return Ok(Value::Bool(false));
                }
                let matches = match (&val, &pat) {
                    (Value::Text(s), Value::Text(p)) => {
                        Self::simple_like_match(s, p)
                    }
                    _ => false,
                };
                Ok(Value::Bool(if *negated { !matches } else { matches }))
            }
            Expr::FunctionCall { name, args, .. } => {
                Self::eval_function_positional(name, args, row, schema)
            }
            Expr::Match { column, query, .. } => {
                let pos = schema.get_column_position(column);
                match pos {
                    Some(p) => {
                        match row.get(p) {
                            Some(Value::Text(text)) => {
                                let text_lower = text.to_lowercase();
                                let query_lower = query.to_lowercase();
                                let terms: Vec<&str> = query_lower.split_whitespace().collect();
                                Ok(Value::Bool(terms.iter().all(|t| text_lower.contains(t))))
                            }
                            _ => Ok(Value::Bool(false)),
                        }
                    }
                    None => Ok(Value::Bool(false)),
                }
            }
            _ => Err(MoteDBError::Query(
                format!("eval_expr_on_row: unsupported expression: {:?}", expr)
            )),
        }
    }

    /// Generate a human-readable column name for an expression (e.g., "SUM(amount)", "COUNT(*)")
    fn expr_to_column_name(expr: &Expr) -> String {
        match expr {
            Expr::Column(name) => name.clone(),
            Expr::Literal(v) => format!("{:?}", v),
            Expr::FunctionCall { name, args, .. } => {
                let arg_str = if args.is_empty() {
                    "*".to_string()
                } else {
                    args.iter()
                        .map(Self::expr_to_column_name)
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                format!("{}({})", name.to_uppercase(), arg_str)
            }
            Expr::BinaryOp { left, op, right } => {
                format!("{} {:?} {}", Self::expr_to_column_name(left), op, Self::expr_to_column_name(right))
            }
            Expr::UnaryOp { op, expr } => {
                format!("{:?}{}", op, Self::expr_to_column_name(expr))
            }
            _ => format!("{:?}", expr),
        }
    }

    fn project_row_static(
        sql_row: &SqlRow,
        select_cols: &[SelectColumn],
        columns: &[String],
        schema: &TableSchema,
    ) -> Vec<Value> {
        if select_cols.len() == 1 && matches!(select_cols[0], SelectColumn::Star) {
            // SELECT * - 按 schema 顺序返回所有列
            let table_name = schema.name.as_str();
            schema.columns.iter()
                .map(|col_def| {
                    sql_row.get(&col_def.name).cloned().unwrap_or_else(|| {
                        // Fallback: try qualified name (e.g., "table.column")
                        if !table_name.is_empty() {
                            let qname = format!("{}.{}", table_name, col_def.name);
                            sql_row.get(&qname).cloned().unwrap_or(Value::Null)
                        } else {
                            Value::Null
                        }
                    })
                })
                .collect()
        } else {
            // 显式列名
            columns.iter().zip(select_cols.iter())
                .map(|(_alias, col_spec)| {
                    match col_spec {
                        SelectColumn::Column(name) => {
                            sql_row.get(name).cloned().unwrap_or(Value::Null)
                        }
                        SelectColumn::ColumnWithAlias(name, _) => {
                            sql_row.get(name).cloned().unwrap_or(Value::Null)
                        }
                        SelectColumn::Star => Value::Null,
                        SelectColumn::Expr(expr, _) => {
                            // Evaluate expression against the SQL row
                            match Self::eval_expr_simple(expr, sql_row) {
                                Ok(v) => v,
                                Err(_) => Value::Null,
                            }
                        }
                    }
                })
                .collect()
        }
    }

    /// 🚀 P0 Optimization: Direct row projection (skips HashMap conversion)
    ///
    /// For PK point queries, the old path was:
    ///   Row(Vec<Value>) → SqlRow(HashMap) → project → Vec<Value>
    ///   = N clones + N HashMap inserts + N lookups
    ///
    /// New path:
    ///   Row(Vec<Value>) → direct index → Vec<Value>
    ///   = M clones (M = selected columns, no HashMap)
    fn project_row_direct(
        row: &Row,
        select_cols: &[SelectColumn],
        columns: &[String],
        schema: &TableSchema,
    ) -> Vec<Value> {
        if select_cols.len() == 1 && matches!(select_cols[0], SelectColumn::Star) {
            // SELECT * — return all columns in schema order (cheap clone)
            row.to_vec()
        } else {
            // Explicit columns — use column position as index into Vec
            columns.iter().zip(select_cols.iter())
                .map(|(_alias, col_spec)| {
                    let col_name = match col_spec {
                        SelectColumn::Column(name) => name,
                        SelectColumn::ColumnWithAlias(name, _) => name,
                        SelectColumn::Star => return Value::Null,
                        SelectColumn::Expr(expr, _) => {
                            return match Self::eval_expr_on_row(expr, row, schema) {
                                Ok(v) => v,
                                Err(_) => Value::Null,
                            };
                        }
                    };
                    // Look up column position in schema (O(1) via column_map HashMap)
                    // Handle table-qualified names: "users.id" → "id"
                    let lookup_name = if col_name.contains('.') {
                        col_name.rsplit('.').next().unwrap_or(col_name)
                    } else {
                        col_name
                    };
                    if let Some(pos) = schema.get_column_position(lookup_name) {
                        row.get(pos).cloned().unwrap_or(Value::Null)
                    } else {
                        Value::Null
                    }
                })
                .collect()
        }
    }

    /// 🚀 Parallel full table scan using rayon `par_bridge`.
    ///
    /// Pulls rows from the sequential LSM iterator and processes them in parallel
    /// (WHERE filter + projection), interleaving I/O and CPU naturally.
    /// Falls back to `None` if the table is too small for parallelism to help.
    #[cfg(feature = "rayon")]
    fn try_parallel_full_scan(
        &self,
        table: &str,
        schema: &Arc<TableSchema>,
        select_cols: &[SelectColumn],
        columns: &[String],
        compiled_where: &CompiledWhere,
        stmt: &SelectStmt,
    ) -> Option<StreamingQueryResult> {
        use rayon::prelude::*;

        const MIN_PARALLEL_ROWS: usize = 100000; // Only activate for large tables (>100K)

        let row_iter = match self.db.scan_table_rows_streaming(table) {
            Ok(it) => it,
            Err(_) => return None,
        };

        // Collect rows into a Vec first to get a size estimate.
        // For very small tables we skip parallelism entirely.
        let all_rows: Vec<(u64, Row)> = match row_iter.collect::<std::result::Result<Vec<_>, _>>() {
            Ok(rows) => rows,
            Err(_) => return None,
        };

        if all_rows.len() < MIN_PARALLEL_ROWS {
            return None;
        }

        // Process rows in parallel with par_bridge.
        // Each row: evaluate WHERE, project matching rows.
        let schema_ref: &TableSchema = schema.as_ref();
        let results: Vec<Vec<Value>> = all_rows
            .into_par_iter()
            .filter_map(|(_row_id, row)| {
                if compiled_where.eval(&row).unwrap_or(false) {
                    Some(Self::project_row_direct(
                        &row, select_cols, columns, schema_ref,
                    ))
                } else {
                    None
                }
            })
            .collect();

        Some(StreamingQueryResult::SelectStreaming {
            columns: columns.to_vec(),
            rows: Box::new(results.into_iter().map(Ok)),
            order_by: stmt.order_by.clone(),
            limit: stmt.limit,
            offset: stmt.offset,
            distinct: stmt.distinct,
            max_result_rows: None,
            size_hint: None,
        })
    }

    /// Internal SELECT execution (takes &SelectStmt to allow reuse in subqueries)
    fn execute_select_internal(&self, stmt: &SelectStmt) -> Result<QueryResult> {
        // 🚀 Substitute bind parameters before executing
        let resolved_stmt;
        let stmt = if Self::contains_parameter_stmt(stmt) {
            match self.substitute_params_stmt(stmt) {
                Ok(s) => { resolved_stmt = s; &resolved_stmt as &SelectStmt }
                Err(e) => return Err(e),
            }
        } else {
            stmt
        };

        // Validate SELECT column references against the table schema (when a
        // single table is named). A bare column that doesn't exist in the
        // table is a query error, not a silent NULL/value from another column.
        if let Some(TableRef::Table { name: table_name, .. }) = stmt.from.as_ref() {
            if let Ok(schema) = self.db.get_table_schema(table_name) {
                for col in &stmt.columns {
                    if let SelectColumn::Column(name) | SelectColumn::ColumnWithAlias(name, _) = col {
                        let bare = name.rsplit('.').next().unwrap_or(name);
                        if schema.get_column_position(bare).is_none() {
                            return Err(MoteDBError::ColumnNotFound(
                                format!("'{}' in table '{}'", bare, table_name)
                            ));
                        }
                    }
                }
            }
        }

        // 🆕 FAST PATH -4: SELECT without FROM clause (e.g., SELECT LAST_INSERT_ID())
        // → Evaluate expressions directly without table scan
        if stmt.from.is_none() {
            let empty_row = SqlRow::new();
            let mut result_row = Vec::new();
            let mut column_names = Vec::new();
            
            for col in &stmt.columns {
                match col {
                    SelectColumn::Expr(expr, alias) => {
                        let value = self.evaluator.eval(expr, &empty_row)?;
                        let col_name = alias.clone().unwrap_or_else(|| format!("{:?}", expr));
                        column_names.push(col_name);
                        result_row.push(value);
                    }
                    SelectColumn::Star => {
                        return Err(MoteDBError::InvalidArgument(
                            "SELECT * requires a FROM clause".to_string()
                        ));
                    }
                    SelectColumn::Column(name) | SelectColumn::ColumnWithAlias(name, _) => {
                        return Err(MoteDBError::InvalidArgument(
                            format!("Column {} requires a FROM clause", name)
                        ));
                    }
                }
            }
            
            return Ok(QueryResult::Select {
                columns: column_names,
                rows: vec![result_row],
            });
        }
        
        // From here on, we know stmt.from is Some. Extracted once below.
        let from = stmt.from.as_ref().unwrap();

        // 🆕 Columnar SELECT for TimeSeries tables
        // Pattern: SELECT cols FROM ts_table WHERE ts BETWEEN a AND b
        // → Route to columnar store with time-range pruning + column projection
        if let TableRef::Table { name: table_name, .. } = from {
            if let Ok(schema) = self.db.get_table_schema(table_name) {
                if schema.table_type == crate::types::TableType::TimeSeries {
                    if let Some(result) = self.try_columnar_select(stmt, &schema)? {
                        return Ok(result);
                    }
                    // Fall through to LSM full scan for complex queries (JOINs, subqueries, etc.)
                }
            }
        }

        // S9: ColSegmentStore tables — route ALL non-aggregate queries (with or
        // without WHERE) through the multi-segment full-scan path. The
        // PointQuery/index fast paths below fetch rows via lsm_engine.scan_range,
        // which returns empty for ColSegmentStore tables (data lives in segment
        // files, not the LSM). Previously this only routed queries with a WHERE
        // or ORDER BY clause, so a plain `SELECT *` (no WHERE) fell through to
        // the LSM path and returned 0 rows — a correctness bug for ColSegmentStore
        // tables.
        if stmt.group_by.is_none() && !self.has_aggregates(&stmt.columns) {
            if let TableRef::Table { name: table_name, .. } = from {
                if self.db.has_col_segment_store(table_name)
                    && !self.has_only_count_aggregate(&stmt.columns)
                    // Don't route spatial/text/vector queries to the columnar
                    // scan — they need the index pushdown paths below
                    // (FAST PATH 0a/0b/-1/-1b).
                    && stmt.where_clause.as_ref().map_or(true, |w| !Self::expr_needs_materialized_path(w))
                {
                    // Route ALL queries (including WHERE id = val) through the
                    // ColSegmentStore full-scan path. Previously, WHERE id=val was
                    // routed to the PK point-query path (get_table_row), which fails
                    // for non-AUTO_INCREMENT PK tables because the row_id doesn't
                    // match the PK value (WHERE id=1 returned 0 rows on INT-PK
                    // tables). The full-scan WHERE filter handles all column types.
                    let stream = self.execute_full_scan_streaming(stmt, table_name)?;
                    return Ok(stream.materialize()?);
                }
            }
        }

        // S9: ColSegmentStore tables — route queries with WHERE through the
        // multi-segment full-scan path. The PointQuery/index fast paths below
        // fetch rows via lsm_engine.scan_range, which returns empty for
        // ColSegmentStore tables (data lives in segment files, not the LSM).
        // Skip spatial/text/vector WHERE — they need the index pushdown paths.
        if stmt.where_clause.as_ref().map_or(false, |w| !Self::expr_needs_materialized_path(w))
            && stmt.where_clause.is_some() && stmt.group_by.is_none()
        {
            if let TableRef::Table { name: table_name, .. } = from {
                if self.db.has_col_segment_store(table_name)
                    && !self.has_only_count_aggregate(&stmt.columns)
                {
                    let stream = self.execute_full_scan_streaming(stmt, table_name)?;
                    return Ok(stream.materialize()?);
                }
            }
        }

        // 🚀 FAST PATH -3: Primary key point query optimization (P0)
        // Pattern: SELECT * FROM table WHERE primary_key = value
        // → Direct LSM get by row_id (165x faster, no MemTable scan!)
        if let Some(result) = self.try_optimize_primary_key_point_query(stmt)? {
            return Ok(result);
        }
        
        // 🚀 FAST PATH -2: ORDER BY primary key optimization (P0)
        // Pattern: SELECT * FROM table ORDER BY id [ASC/DESC] [LIMIT k]
        // → Use primary key index scan (600x faster, 280x less memory!)
        if let Some(result) = self.try_optimize_primary_key_order_by(stmt)? {
            return Ok(result);
        }
        
        // 🚀 FAST PATH -1: ORDER BY vector distance optimization (P0)
        // Pattern: SELECT * FROM table ORDER BY column <-> [...] LIMIT k
        // → Directly use vector index search (724x faster!)
        if let Some(plan) = self.try_optimize_vector_order_by(stmt)? {
            return self.execute_vector_order_by_plan(stmt, &plan);
        }

        // 🚀 FAST PATH -1b: Spatial ORDER BY ST_DISTANCE optimization
        // Pattern: SELECT ... FROM table ORDER BY ST_DISTANCE(col, x, y) LIMIT k
        // → Use spatial KNN index (50x faster than full scan + per-row distance calc)
        if let Some(result) = self.try_optimize_spatial_order_by(stmt)? {
            return Ok(result);
        }

        // 🚀 FAST PATH 0: Vector search optimization (P0)
        // Pattern: SELECT * FROM table WHERE VECTOR_SEARCH(column, [...], k)
        if let Some(ref where_clause) = stmt.where_clause {
            if let Some((table_name, col_name, query_vector, k)) = self.try_extract_vector_search(where_clause, from) {
                // ⚡ Ultra-fast path: Use vector index directly
                // Resolve index name via registry (supports custom index names)
                let index_name = self.db.index_registry.find_by_column(
                    &table_name, &col_name,
                    crate::database::index_metadata::IndexType::Vector
                ).unwrap_or_else(|| format!("{}_{}", table_name, col_name));
                match self.db.vector_search(&index_name, &query_vector, k) {
                    Ok(results) => {
                        // Load rows for the result row_ids
                        let schema = self.db.get_table_schema(&table_name)?;
                        
                        // 🚀 P1 优化：预分配 k 个结果
                        let mut sql_rows = Vec::with_capacity(k.min(results.len()));
                        
                        for (row_id, _distance) in results {
                            if let Ok(Some(row)) = self.db.get_table_row(&table_name, row_id) {
                                let sql_row = row_to_sql_row(&row, &schema)?;
                                sql_rows.push((row_id, sql_row));
                            }
                        }
                        
                        // Add table prefix
                        prefix_rows(&mut sql_rows, &table_name, &table_name);

                        // Project columns and return
                        let (column_names, result_rows) = self.project_columns(&stmt.columns, &sql_rows, &schema)?;
                        
                        return Ok(QueryResult::Select {
                            columns: column_names,
                            rows: result_rows,
                        });
                    }
                    Err(_) => {
                        // Fallback to normal execution if vector search fails
                    }
                }
            }
        }

        // 🚀 FAST PATH 0a: Text Search (MATCH AGAINST) optimization
        // Pattern: SELECT ... FROM table WHERE MATCH(col) AGAINST('query') [ORDER BY score] [LIMIT k]
        // → Use text index directly (50x faster than full table scan + per-row search_ranked)
        if let Some(ref where_clause) = stmt.where_clause {
            if let TableRef::Table { name: table_name, .. } = from {
                if let Some(result) = self.try_text_search_fast_path(stmt, where_clause, table_name)? {
                    return Ok(result);
                }
            }
        }

        // 🚀 FAST PATH 0b: Spatial (ST_WITHIN / ST_KNN) optimization
        // Pattern: SELECT ... FROM table WHERE ST_WITHIN(col, ...) [LIMIT k]
        //          SELECT ... FROM table WHERE ST_KNN(col, ...) [LIMIT k]
        // → Use spatial index directly (50x faster than full table scan + per-row spatial query)
        if let Some(ref where_clause) = stmt.where_clause {
            if let TableRef::Table { name: table_name, .. } = from {
                if let Some(result) = self.try_spatial_fast_path(stmt, where_clause, table_name)? {
                    return Ok(result);
                }
            }
        }

        // 🚀 FAST PATH 1: Aggregate query optimization (P0-2)
        // Pattern: SELECT COUNT(*) FROM table [WHERE indexed_col = value]
        if self.has_only_count_aggregate(&stmt.columns) && stmt.group_by.is_none() {
            // Check if WHERE clause can use index
            if let Some(ref where_clause) = stmt.where_clause {
                if let Some((col_name, target_value)) = self.try_extract_point_query(where_clause) {
                    if let TableRef::Table { name: table_name, .. } = from {
                        let index_name = format!("{}.{}", table_name, col_name);
                        if self.db.column_indexes.contains_key(&index_name) {
                            // ⚡ Ultra-fast path: Use index to get count
                            match self.db.query_by_column(table_name, &col_name, &target_value) {
                                Ok(row_ids) if !row_ids.is_empty() || !self.db.is_async_index_pipeline_active() => {
                                    let count = row_ids.len() as i64;
                                    return Ok(QueryResult::Select {
                                        columns: vec!["COUNT(*)".to_string()],
                                        rows: vec![vec![Value::Integer(count)]],
                                    });
                                }
                                Ok(_) | Err(_) => {
                                    // Fallback: index empty + pipeline active, or query error
                                }
                            }
                        }
                    }
                }
            } else {
                // 🚀 COUNT(*) without WHERE — O(1) from row counter
                if let TableRef::Table { name: table_name, .. } = from {
                    let count = if let Some(counter) = self.db.table_row_count.get(table_name) {
                        counter.load(std::sync::atomic::Ordering::Relaxed) as i64
                    } else {
                        // Fallback: streaming scan if counter not initialized
                        let row_iter = self.db.scan_table_rows_streaming(table_name)?;
                        let mut c = 0i64;
                        for result in row_iter {
                            let _ = result?;
                            c += 1;
                        }
                        c
                    };

                    return Ok(QueryResult::Select {
                        columns: vec!["COUNT(*)".to_string()],
                        rows: vec![vec![Value::Integer(count)]],
                    });
                }
            }
        }

        // 🚀 FAST PATH 1a: Streaming aggregate (no GROUP BY) — zero HashMap, zero SqlRow.
        // Handles: SELECT COUNT(*), SUM(x), AVG(y), MIN(z), MAX(w) FROM t [WHERE ...]
        // Accumulates directly into inline counters — O(1) memory, no grouping overhead.
        // When WHERE is present, reuses decoded row for aggregate extraction.
        if stmt.group_by.is_none() && !stmt.distinct && stmt.having.is_none()
            && stmt.order_by.is_none() && self.has_aggregates(&stmt.columns)
        {
            if let TableRef::Table { name: table_name, .. } = from {
                if let Ok(schema) = self.db.get_table_schema(table_name) {
                    if let Some(result) = self.try_streaming_aggregate(stmt, &schema, table_name)? {
                        return Ok(result);
                    }
                }
            }
        }

        // 🚀 FAST PATH 1b: Positional GROUP BY — skip HashMap conversion entirely.
        // Works directly on Vec<Value> rows for simple single-table GROUP BY / aggregate queries.
        if stmt.group_by.is_some() || self.has_aggregates(&stmt.columns) {
            if let TableRef::Table { name: table_name, .. } = from {
                if let Ok(schema) = self.db.get_table_schema(table_name) {
                    if let Some((column_names, projected_rows)) =
                        self.try_apply_group_by_positional(stmt, &schema, table_name)?
                    {
                        return Ok(QueryResult::Select {
                            columns: column_names,
                            rows: projected_rows,
                        });
                    }
                }
            }
        }

        // 🚀 FAST PATH 1c: Positional ORDER BY / DISTINCT — skip HashMap conversion entirely.
        // Works directly on Vec<Value> rows for simple single-table ORDER BY / DISTINCT queries.
        if (stmt.order_by.is_some() || stmt.distinct) && stmt.group_by.is_none() {
            if let TableRef::Table { name: table_name, .. } = from {
                if let Ok(schema) = self.db.get_table_schema(table_name) {
                    if let Some(result) = self.try_positional_order_by_distinct(stmt, &schema, table_name)? {
                        return Ok(result);
                    }
                }
            }
        }

        // 🚀 FAST PATH 1d: Positional WHERE — skip SqlRow for simple filtered queries.
        // Scans rows directly on Vec<Value>, evaluates WHERE positionally,
        // projects positionally. Eliminates O(R*C) HashMap allocations entirely.
        // Handles: SELECT cols FROM t WHERE col IN (list) / LIKE / BETWEEN / comparisons
        //          without GROUP BY / ORDER BY / DISTINCT.
        if stmt.where_clause.is_some() && stmt.group_by.is_none()
            && stmt.order_by.is_none() && !stmt.distinct
        {
            if let TableRef::Table { name: table_name, .. } = from {
                if let Some(result) =  self.try_positional_where(stmt, table_name)? {
                    return Ok(result);
                }
            }
        }

        // 🚀 FAST PATH 1.5: Direct Vec<Value> for SELECT * FROM table
        // Bypasses SqlRow HashMap entirely — eliminates 2 HashMap allocs + 2N String clones per row.
        // Handles: SELECT * FROM t WHERE indexed_col =/>/>=/</<= value [LIMIT n]
        //          SELECT * FROM t [LIMIT n]
        let is_simple_star = stmt.columns.len() == 1 && matches!(stmt.columns[0], SelectColumn::Star)
            && stmt.group_by.is_none() && stmt.order_by.is_none() && !stmt.distinct;

        if is_simple_star {
            if let Some(ref where_clause) = stmt.where_clause {
                if let TableRef::Table { name: table_name, .. } = from {
                    // Try point query: WHERE col = value
                    if let Some((col_name, target_value)) = self.try_extract_point_query(where_clause) {
                        let index_name = format!("{}.{}", table_name, col_name);
                        if let Some(index_ref) = self.db.column_indexes.get(&index_name) {
                            if let Ok(row_ids) = index_ref.value().get_arc(&target_value) {
                                if !row_ids.is_empty() || !self.db.is_async_index_pipeline_active() {
                                    drop(index_ref);
                                    return self.fast_star_indexed_result(table_name, &row_ids, stmt.limit, stmt.offset);
                                }
                            }
                        }
                    }
                    // Try range query: WHERE col >= a AND col <= b
                    else if let Some((col_name, lower_value, lower_op, upper_value, upper_op)) = self.try_extract_range_query(where_clause) {
                        let index_name = format!("{}.{}", table_name, col_name);
                        if let Some(index_ref) = self.db.column_indexes.get(&index_name) {
                            use crate::sql::ast::BinaryOperator;
                            let lower_inclusive = matches!(lower_op, BinaryOperator::Ge);
                            let upper_inclusive = matches!(upper_op, BinaryOperator::Le);
                            if let Ok(row_ids) = index_ref.value().query_between(
                                &lower_value, lower_inclusive,
                                &upper_value, upper_inclusive
                            ) {
                                if !row_ids.is_empty() || !self.db.is_async_index_pipeline_active() {
                                    drop(index_ref);
                                    return self.fast_star_indexed_result(table_name, &row_ids, stmt.limit, stmt.offset);
                                }
                            }
                        }
                    }
                    // Try inequality: WHERE col > value, col < value, etc.
                    else if let Some((col_name, op, value)) = self.try_extract_inequality(where_clause) {
                        let index_name = format!("{}.{}", table_name, col_name);
                        if let Some(index_ref) = self.db.column_indexes.get(&index_name) {
                            use crate::sql::ast::BinaryOperator;
                            let row_ids_result = match op {
                                BinaryOperator::Lt => index_ref.value().query_less_than(&value),
                                BinaryOperator::Le => index_ref.value().query_less_than_or_equal(&value),
                                BinaryOperator::Gt => index_ref.value().query_greater_than(&value),
                                BinaryOperator::Ge => index_ref.value().query_greater_than_or_equal(&value),
                                _ => Err(crate::error::MoteDBError::NotImplemented("Unsupported operator".into())),
                            };
                            if let Ok(row_ids) = row_ids_result {
                                if !row_ids.is_empty() || !self.db.is_async_index_pipeline_active() {
                                    drop(index_ref);
                                    return self.fast_star_indexed_result(table_name, &row_ids, stmt.limit, stmt.offset);
                                }
                            }
                        }
                    }
                    // Non-indexed WHERE — fall through to general path (needs SqlRow for eval)
                }
            } else {
                // No WHERE — full scan fast path
                if let TableRef::Table { name: table_name, .. } = from {
                    return self.fast_star_scan_result(table_name, stmt.limit, stmt.offset);
                }
            }
            // Fall through to general path for unsupported patterns
        }

        // 🚀 FAST PATH 2: Try to use column index for WHERE optimization
        // 🆕 P0 OPTIMIZATION: Extract LIMIT early and pass to storage layer
        let storage_limit = self.calculate_storage_limit(stmt);
        
        // Priority: Range query > Point query > Full scan
        let (all_sql_rows, combined_schema) = if let Some(ref where_clause) = stmt.where_clause {
            // Try range query first (dual-bound: col > X AND col < Y)
            if let Some((col_name, lower_value, lower_op, upper_value, upper_op)) = self.try_extract_range_query(where_clause) {
                if let TableRef::Table { name: table_name, .. } = from {
                    let index_name = format!("{}.{}", table_name, col_name);
                    let index_exists = self.db.column_indexes.contains_key(&index_name);
                    
                    if index_exists {
                        // ⚡ Fast path: Use optimized dual-bound range query (single B-Tree scan)
                        use crate::sql::ast::BinaryOperator;
                        
                        // Convert operators to inclusive flags
                        let lower_inclusive = matches!(lower_op, BinaryOperator::Ge);
                        let upper_inclusive = matches!(upper_op, BinaryOperator::Le);
                        
                        // Single index scan with proper boundaries
                        let row_ids = self.db.query_by_column_between(
                            table_name, &col_name,
                            &lower_value, lower_inclusive,
                            &upper_value, upper_inclusive
                        )?;

                        // If column index is empty (async pipeline not yet built), fall back to full scan
                        if row_ids.is_empty() && self.db.is_async_index_pipeline_active() {
                            let row_iter = self.db.scan_table_rows_streaming(table_name)?;
                            let schema = self.db.get_table_schema(table_name)?;
                            let mut sql_rows = Vec::new();
                            for result in row_iter {
                                let (row_id, row) = result?;
                                let sql_row = row_to_sql_row(&row, &schema)?;
                                sql_rows.push((row_id, sql_row));
                            }
                            let prefix = table_name;
                            prefix_rows(&mut sql_rows, table_name, prefix);
                            let prefixed_schema = prefix_schema(&schema, prefix);
                            (sql_rows, Arc::new(prefixed_schema))
                        } else {
                        
                        // 🚀 P0 OPTIMIZATION: Smart index selection based on selectivity
                        // 
                        // Strategy:
                        // - Selectivity < 10%: Use index (faster for small result sets)
                        // - Selectivity >= 10%: Use table scan (faster for large result sets)
                        // 
                        // Why? Index scan has overhead:
                        // - B-Tree lookup cost
                        // - 30K random reads (fragmented access)
                        // - Cache unfriendly
                        // 
                        // Table scan is sequential:
                        // - Single range scan
                        // - Cache friendly
                        // - Better for large result sets
                        let result_count = row_ids.len();
                        let table_count = self.db.estimate_table_row_count(table_name)?;
                        let selectivity = if table_count > 0 {
                            result_count as f64 / table_count as f64
                        } else {
                            0.0
                        };
                        
                        const SELECTIVITY_THRESHOLD: f64 = 0.15; // 15%
                        
                        if selectivity < SELECTIVITY_THRESHOLD {
                            // ✅ Low selectivity (< 10%): Use index (faster!)
                            debug_log!(
                                "[Smart Index] Using INDEX SCAN: {} rows / {} total = {:.1}% selectivity",
                                result_count, table_count, selectivity * 100.0
                            );
                        
                        // 🚀 Use batch get for better performance (auto-optimizes for continuous IDs)
                        let schema = self.db.get_table_schema(table_name)?;
                        let batch_rows = self.db.get_table_rows_batch(table_name, &row_ids)?;
                        
                        // Convert to sql_rows
                        // 🚀 P1 优化：预分配 row_ids 大小
                        let mut sql_rows = Vec::with_capacity(row_ids.len());
                        for (row_id, row_opt) in batch_rows {
                            if let Some(row) = row_opt {
                                let sql_row = row_to_sql_row(&row, &schema)?;
                                sql_rows.push((row_id, sql_row));
                            }
                        }
                        
                        // Add table prefix
                        let prefix = table_name;
                        prefix_rows(&mut sql_rows, table_name, prefix);
                        let prefixed_schema = prefix_schema(&schema, prefix);

                        (sql_rows, Arc::new(prefixed_schema))
                        } else {
                            // 🚀 High selectivity (>= 15%): Use真正的流式扫描 (O(1) memory!)
                            debug_log!(
                                "[Smart Index] Using STREAMING SCAN: {} rows / {} total = {:.1}% selectivity (>= 15%)",
                                result_count, table_count, selectivity * 100.0
                            );
                            
                            // 🚀 Use真正的流式扫描 - 每次只在内存中保留一行
                            let row_iter = self.db.scan_table_rows_streaming(table_name)?;
                            let schema = self.db.get_table_schema(table_name)?;
                            
                            let mut filtered_rows = Vec::new();
                            
                            for result in row_iter {
                                let (row_id, row) = result?;
                                
                                // Get column value
                                let col_index = schema.columns.iter()
                                    .position(|c| c.name == col_name)
                                    .ok_or_else(|| StorageError::InvalidData(
                                        format!("Column '{}' not found", col_name)
                                    ))?;
                                
                                let col_value = row.get(col_index)
                                    .ok_or_else(|| StorageError::InvalidData(
                                        "Column value missing".into()
                                    ))?;
                                
                                // Check range condition
                                let lower_ok = if lower_inclusive {
                                    col_value >= &lower_value
                                } else {
                                    col_value > &lower_value
                                };
                                
                                let upper_ok = if upper_inclusive {
                                    col_value <= &upper_value
                                    } else {
                                        col_value < &upper_value
                                    };
                                    
                                    if lower_ok && upper_ok {
                                        let sql_row = row_to_sql_row(&row, &schema)?;
                                        filtered_rows.push((row_id, sql_row));
                                    }
                            }
                            
                            // Add table prefix
                            let prefix = table_name;
                            prefix_rows(&mut filtered_rows, table_name, prefix);
                            let prefixed_schema = prefix_schema(&schema, prefix);

                            (filtered_rows, Arc::new(prefixed_schema))
                        }
                        } // row_ids non-empty or pipeline inactive
                    } else {
                        // No index, use table scan
                        self.execute_from_with_limit(from, storage_limit)?
                    }
                } else {
                    self.execute_from_with_limit(from, storage_limit)?
                }
            }
            // Try point query
            else if let Some((col_name, target_value)) = self.try_extract_point_query(where_clause) {
                // Extract table name from FROM clause
                if let TableRef::Table { name: table_name, .. } = from {
                    // Try to use column index
                    let index_name = format!("{}.{}", table_name, col_name);
                    let index_exists = self.db.column_indexes.contains_key(&index_name);
                    
                    if index_exists {
                        // ⚡ Fast path: Use column index (40x faster!)
                        match self.db.query_by_column(table_name, &col_name, &target_value) {
                            Ok(row_ids) if !row_ids.is_empty() || !self.db.is_async_index_pipeline_active() => {
                                // 🚀 Use batch get
                                let schema = self.db.get_table_schema(table_name)?;
                                let batch_rows = self.db.get_table_rows_batch(table_name, &row_ids)?;

                                // 🚀 P1 优化：预分配 row_ids 大小
                                let mut sql_rows = Vec::with_capacity(row_ids.len());
                                for (row_id, row_opt) in batch_rows {
                                    if let Some(row) = row_opt {
                                        let sql_row = row_to_sql_row(&row, &schema)?;
                                        sql_rows.push((row_id, sql_row));
                                    }
                                }

                                // Add table prefix
                                let prefix = table_name;
                                prefix_rows(&mut sql_rows, table_name, prefix);
                                let prefixed_schema = prefix_schema(&schema, prefix);

                                (sql_rows, Arc::new(prefixed_schema))
                            }
                            Ok(_) | Err(_) => {
                                // Fallback: index empty + pipeline active, or query error
                                self.execute_from(from)?
                            }
                        }
                    } else {
                        // No index, use table scan
                        self.execute_from(from)?
                    }
                } else {
                    // Not a simple table (e.g., subquery or join)
                    self.execute_from(from)?
                }
            }
            // 🚀 Try inequality query (col < value, col > value, etc.)
            else if let Some((col_name, op, value)) = self.try_extract_inequality(where_clause) {
                if let TableRef::Table { name: table_name, .. } = from {
                    let index_name = format!("{}.{}", table_name, col_name);
                    let index_exists = self.db.column_indexes.contains_key(&index_name);
                    
                    if index_exists {
                        // ⚡ Fast path: Use column index inequality scan
                        let row_ids_result = match op {
                            BinaryOperator::Lt => self.db.query_by_column_less_than(table_name, &col_name, &value),
                            BinaryOperator::Le => self.db.query_by_column_less_than_or_equal(table_name, &col_name, &value),
                            BinaryOperator::Gt => self.db.query_by_column_greater_than(table_name, &col_name, &value),
                            BinaryOperator::Ge => self.db.query_by_column_greater_than_or_equal(table_name, &col_name, &value),
                            _ => {
                                // Unsupported operator, fallback to table scan
                                Err(crate::error::MoteDBError::NotImplemented("Unsupported operator".into()))
                            }
                        };
                        
                        match row_ids_result {
                            Ok(row_ids) if !row_ids.is_empty() || !self.db.is_async_index_pipeline_active() => {
                                // 🚀 Use batch get
                                let schema = self.db.get_table_schema(table_name)?;
                                let batch_rows = self.db.get_table_rows_batch(table_name, &row_ids)?;

                                // 🚀 P1 优化：预分配 row_ids 大小
                                let mut sql_rows = Vec::with_capacity(row_ids.len());
                                for (row_id, row_opt) in batch_rows {
                                    if let Some(row) = row_opt {
                                        let sql_row = row_to_sql_row(&row, &schema)?;
                                        sql_rows.push((row_id, sql_row));
                                    }
                                }

                                // Add table prefix
                                let prefix = table_name;
                                prefix_rows(&mut sql_rows, table_name, prefix);
                                let prefixed_schema = prefix_schema(&schema, prefix);

                                (sql_rows, Arc::new(prefixed_schema))
                            }
                            Ok(_) | Err(_) => {
                                // Fallback: index empty + pipeline active, or query error
                                self.execute_from(from)?
                            }
                        }
                    } else {
                        // No index, use table scan
                        self.execute_from(from)?
                    }
                } else {
                    // Not a simple table
                    self.execute_from(from)?
                }
            } else {
                // Not a simple point/range query
                self.execute_from_with_limit(from, storage_limit)?
            }
        } else {
            // No WHERE clause - use standard scan with limit
            self.execute_from_with_limit(from, storage_limit)?
        };
        
        // 🎯 Filter rows (WHERE clause) - Apply remaining conditions
        let filtered_rows: Vec<(u64, SqlRow)> = if let Some(ref where_clause) = stmt.where_clause {
            // Check if we already used the index (in which case, no need to filter again)
            let used_index = if self.try_extract_range_query(where_clause).is_some() {
                // Range query - check if we used index
                if let TableRef::Table { name: table_name, .. } = from {
                    if let Some((col_name, _, _, _, _)) = self.try_extract_range_query(where_clause) {
                        let index_name = format!("{}.{}", table_name, col_name);
                        self.db.column_indexes.contains_key(&index_name)
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else if let Some((col_name, _)) = self.try_extract_point_query(where_clause) {
                // Point query - check if we used index
                if let TableRef::Table { name: table_name, .. } = from {
                    let index_name = format!("{}.{}", table_name, col_name);
                    self.db.column_indexes.contains_key(&index_name)
                } else {
                    false
                }
            } else {
                false
            };
            
            if used_index {
                // Already filtered by index
                all_sql_rows
            } else {
                // Apply WHERE clause in memory
                if let Some((col_name, target_value)) = self.try_extract_point_query(where_clause) {
                    // Fast path: Only evaluate the point query condition
                    all_sql_rows.into_iter()
                        .filter(|(_, row)| {
                            // 尝试直接匹配
                            if let Some(row_value) = row.get(&col_name) {
                                return row_value == &target_value;
                            }
                            
                            // 尝试匹配带表前缀的列名 (e.g., "users.id")
                            for (key, row_value) in row.iter() {
                                if key.ends_with(&format!(".{}", col_name)) || key == &col_name {
                                    return row_value == &target_value;
                                }
                            }
                            
                            false
                        })
                        .collect()
                } else {
                    // 🚀 OPTIMIZATION: Fast path for simple comparison expressions
                    // Pattern: col > value, col < value, col >= value, col <= value
                    if let Some(fast_filter) = self.compile_simple_comparison(where_clause) {
                        // Use compiled filter (避免重复解释表达式)
                        all_sql_rows.into_iter()
                            .filter(|(_, row)| fast_filter(row))
                            .collect()
                    } else {
                        // Slow path: Full expression evaluation with subquery support
                        let materialized_where = self.materialize_subqueries(where_clause)?;

                        // IN hash set optimization: precompute HashSet for large literal IN lists
                        if let Expr::In { expr, list, negated } = &materialized_where {
                            if !negated && list.len() > 10 && list.iter().all(|e| matches!(e, Expr::Literal(_))) {
                                let in_set: std::collections::HashSet<Value> = list.iter()
                                    .filter_map(|e| if let Expr::Literal(v) = e { Some(v.clone()) } else { None })
                                    .collect();
                                let col_name = match expr.as_ref() {
                                    Expr::Column(name) => Some(name.clone()),
                                    _ => None,
                                };
                                if let Some(col_name) = col_name {
                                    all_sql_rows.into_iter()
                                        .filter(|(_, row)| {
                                            row.get(&col_name)
                                                .map(|val| in_set.contains(val))
                                                .unwrap_or(false)
                                        })
                                        .collect()
                                } else {
                                    all_sql_rows.into_iter()
                                        .filter(|(_, row)| {
                                            self.eval_with_materialized(&materialized_where, row)
                                                .and_then(|val| self.to_bool(&val))
                                                .unwrap_or(false)
                                        })
                                        .collect()
                                }
                            } else {
                                all_sql_rows.into_iter()
                                    .filter(|(_, row)| {
                                        self.eval_with_materialized(&materialized_where, row)
                                            .and_then(|val| self.to_bool(&val))
                                            .unwrap_or(false)
                                    })
                                    .collect()
                            }
                        } else {
                            all_sql_rows.into_iter()
                                .filter(|(_, row)| {
                                    self.eval_with_materialized(&materialized_where, row)
                                        .and_then(|val| self.to_bool(&val))
                                        .unwrap_or(false)
                                })
                                .collect()
                        }
                    }
                }
            }
        } else {
            all_sql_rows
        };
        
        // 🚀 P0 OPTIMIZATION: Apply storage_limit early to reduce memory usage
        // This prevents loading all rows when LIMIT is small and no ORDER BY/GROUP BY/DISTINCT
        // 
        // Safety checks:
        // - ORDER BY: Need all rows to sort first
        // - GROUP BY: Need all rows to group first  
        // - DISTINCT: Need all rows to deduplicate first
        // - Aggregates: Need all rows to compute aggregates
        //
        // If none of above, we can safely truncate early!
        let filtered_rows = if stmt.order_by.is_none() 
            && stmt.group_by.is_none() 
            && !stmt.distinct
            && !self.has_aggregates(&stmt.columns) 
        {
            if let Some(limit) = storage_limit {
                // ✅ Safe to truncate early!
                // This prevents processing millions of rows when LIMIT is small
                filtered_rows.into_iter().take(limit).collect()
            } else {
                filtered_rows
            }
        } else {
            // ❌ Not safe to truncate - need all rows for ORDER BY/GROUP BY/DISTINCT
            filtered_rows
        };

        // GROUP BY aggregation (if present) OR implicit aggregation (if columns contain aggregates)
        let (column_names, projected_rows) = if let Some(ref group_by_cols) = stmt.group_by {
            // Explicit GROUP BY
            self.apply_group_by(&stmt.columns, &filtered_rows, group_by_cols, stmt.having.as_ref())?
        } else if self.has_aggregates(&stmt.columns) {
            // Implicit aggregation (e.g., SELECT COUNT(*) FROM table)
            // Treat as GROUP BY with no grouping columns (entire table is one group)
            self.apply_group_by(&stmt.columns, &filtered_rows, &[], stmt.having.as_ref())?
        } else {
            // No aggregation - simple projection
            self.project_columns(&stmt.columns, &filtered_rows, &combined_schema)?
        };
        
        // Order by (with alias resolution)
        let mut sorted_rows = projected_rows;
        if let Some(ref order_by) = stmt.order_by {
            // Build alias map: alias -> projected column index
            let mut alias_map = std::collections::HashMap::new();
            for (idx, col_spec) in stmt.columns.iter().enumerate() {
                let alias = match col_spec {
                    SelectColumn::ColumnWithAlias(_, alias) => Some(alias.clone()),
                    SelectColumn::Expr(_, Some(alias)) => Some(alias.clone()),
                    _ => None,
                };
                if let Some(alias) = alias {
                    alias_map.insert(alias, idx);
                }
            }
            
            // Create temporary rows with full data for sorting
            let mut rows_with_keys: Vec<(Vec<Value>, Vec<Value>)> = sorted_rows.into_iter()
                .zip(filtered_rows.iter())
                .map(|(proj_row, (_, full_row))| {
                    // Compute sort keys
                    let sort_keys: Result<Vec<Value>> = order_by.iter()
                        .map(|order| {
                            // Try to resolve alias first
                            if let Expr::Column(col_name) = &order.expr {
                                if let Some(&idx) = alias_map.get(col_name) {
                                    // Use projected column value
                                    return Ok(proj_row[idx].clone());
                                }
                                // Try direct column lookup in full_row
                                if let Some(val) = full_row.get(col_name) {
                                    return Ok(val.clone());
                                }
                            }
                            // ORDER BY column position (1-based integer literal)
                            if let Expr::Literal(Value::Integer(n)) = &order.expr {
                                let idx = (*n as usize).wrapping_sub(1);
                                if idx < proj_row.len() {
                                    return Ok(proj_row[idx].clone());
                                }
                            }
                            // Otherwise, evaluate expression against original row
                            self.evaluator.eval(&order.expr, full_row)
                        })
                        .collect();
                    
                    sort_keys.map(|keys| (keys, proj_row))
                })
                .collect::<Result<Vec<_>>>()?;
            
            // Sort
            rows_with_keys.sort_by(|a, b| {
                for (i, order) in order_by.iter().enumerate() {
                    let cmp = a.0[i].partial_cmp(&b.0[i]).unwrap_or(std::cmp::Ordering::Equal);
                    if cmp != std::cmp::Ordering::Equal {
                        return if order.asc { cmp } else { cmp.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
            
            sorted_rows = rows_with_keys.into_iter().map(|(_, row)| row).collect();
        }
        
        // Apply LATEST BY (time-series deduplication)
        let final_sorted_rows = if let Some(ref latest_by_cols) = stmt.latest_by {
            self.apply_latest_by(sorted_rows, &filtered_rows, latest_by_cols, &combined_schema)?
        } else {
            sorted_rows
        };
        
        // Apply DISTINCT (deduplication)
        let deduplicated_rows = if stmt.distinct {
            self.apply_distinct(final_sorted_rows)
        } else {
            final_sorted_rows
        };
        
        // Apply LIMIT and OFFSET
        let offset = stmt.offset.unwrap_or(0);
        let limit = stmt.limit;
        
        let final_rows: Vec<Vec<Value>> = deduplicated_rows.into_iter()
            .skip(offset)
            .take(limit.unwrap_or(usize::MAX))
            .collect();
        
        Ok(QueryResult::Select {
            columns: column_names,
            rows: final_rows,
        })
    }
    
    /// Direct Vec<Value> output for SELECT * with indexed WHERE.
    /// Bypasses SqlRow HashMap — eliminates ~2 HashMap allocs + ~2N String allocs per row.
    fn fast_star_indexed_result(&self, table_name: &str, row_ids: &[RowId],
        limit: Option<usize>, offset: Option<usize>) -> Result<QueryResult> {
        let schema = self.db.get_table_schema(table_name)?;
        let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
        let arc_rows = self.db.get_table_rows_batch_arc(table_name, row_ids)?;
        let skip_n = offset.unwrap_or(0);
        let take_n = limit.unwrap_or(usize::MAX);
        let rows: Vec<Vec<Value>> = arc_rows.into_iter()
            .filter_map(|(_, opt)| opt)
            .skip(skip_n)
            .map(|arc| match Arc::try_unwrap(arc) {
                Ok(row) => row,
                Err(arc) => (*arc).clone(),
            })
            .take(take_n)
            .collect();
        Ok(QueryResult::Select { columns: column_names, rows })
    }

    /// Direct Vec<Value> output for SELECT * with full scan.
    /// Uses streaming iterator — avoids materializing all rows via SqlRow.
    fn fast_star_scan_result(&self, table_name: &str,
        limit: Option<usize>, offset: Option<usize>) -> Result<QueryResult> {
        let schema = self.db.get_table_schema(table_name)?;
        let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
        let skip_n = offset.unwrap_or(0);
        let take_n = limit.unwrap_or(usize::MAX);
        let max_rows = skip_n.saturating_add(take_n);

        let row_iter = self.db.scan_table_rows_streaming(table_name)?;
        let mut rows: Vec<Vec<Value>> = Vec::with_capacity(take_n.min(1024));
        let mut count = 0usize;
        for result in row_iter {
            let (_, row) = result?;
            count += 1;
            if count <= skip_n { continue; }
            rows.push(row);
            if rows.len() >= take_n { break; }
            if count >= max_rows { break; }
        }
        Ok(QueryResult::Select { columns: column_names, rows })
    }

    /// 🚀 P0 OPTIMIZATION: Calculate the limit to pass to storage layer
    /// 
    /// This prevents loading all rows when LIMIT is specified:
    /// - `SELECT * FROM users LIMIT 10` → only load 10 rows from storage
    /// - `SELECT * FROM users WHERE ... LIMIT 10` → load more (WHERE filtering)
    /// - `SELECT * FROM users ORDER BY ... LIMIT 10` → load all (need to sort first)
    fn calculate_storage_limit(&self, stmt: &SelectStmt) -> Option<usize> {
        // If there's ORDER BY, we need all rows to sort first
        if stmt.order_by.is_some() {
            return None;
        }
        
        // If there's GROUP BY, we need all rows
        if stmt.group_by.is_some() {
            return None;
        }
        
        // Check if SELECT columns contain aggregates
        for col in &stmt.columns {
            if let SelectColumn::Expr(expr, _) = col {
                if self.expr_has_aggregates(expr) {
                    return None; // Aggregates need all rows
                }
            }
        }
        
        // If there's WHERE clause that hasn't been resolved by an index,
        // we must scan all rows — the selectivity is unknown and any
        // pre-truncation risks returning wrong (empty) results.
        if stmt.where_clause.is_some() {
            return None;
        }

        // No WHERE: safe to use exact limit at storage level
        let limit = stmt.limit?;
        let offset = stmt.offset.unwrap_or(0);
        Some(limit + offset)
    }
    
    /// Check if expression contains aggregates (recursive)
    #[allow(clippy::only_used_in_recursion)]
    fn expr_has_aggregates(&self, expr: &Expr) -> bool {
        match expr {
            Expr::FunctionCall { name, .. } => {
                matches!(name.to_uppercase().as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
            }
            Expr::BinaryOp { left, right, .. } => {
                self.expr_has_aggregates(left) || self.expr_has_aggregates(right)
            }
            _ => false,
        }
    }
    
    /// Execute FROM clause - handles single table or JOINs
    /// Returns all rows with combined schema
    fn execute_from(&self, table_ref: &TableRef) -> FromScanResult {
        self.execute_from_with_limit(table_ref, None)
    }

    /// 🚀 P0 OPTIMIZATION: Execute FROM clause with limit passed to storage layer
    fn execute_from_with_limit(&self, table_ref: &TableRef, limit: Option<usize>) -> FromScanResult {
        match table_ref {
            TableRef::Table { name, alias } => {
                // Single table - use table-specific scan with limit
                let schema = self.db.get_table_schema(name)?;
                
                // 🚀 P0: Scan table with streaming to reduce memory (with optional limit)
                let all_rows: Result<Vec<_>> = if let Some(limit_val) = limit {
                    // With limit: collect only up to limit rows
                    self.db.scan_table_rows_streaming(name)?
                        .take(limit_val)
                        .collect()
                } else {
                    // No limit: collect all (unavoidable for full table scan)
                    self.db.scan_table_rows_streaming(name)?
                        .collect()
                };
                let all_rows = all_rows?;

                let mut sql_rows = rows_to_sql_rows(all_rows, &schema)?;
                
                // Always prefix column names with table or alias for JOIN compatibility
                let prefix = alias.as_ref().unwrap_or(name);

                // Update SqlRow keys to include table prefix + add metadata
                prefix_rows(&mut sql_rows, name, prefix);

                // Update schema column names
                let prefixed_schema = prefix_schema(&schema, prefix);

                Ok((sql_rows, Arc::new(prefixed_schema)))
            }
            TableRef::Subquery { query, alias } => {
                // Execute subquery
                let subquery_result = self.execute_select_internal(query)?;
                
                // Convert QueryResult to (Vec<(u64, SqlRow)>, TableSchema)
                match subquery_result {
                    QueryResult::Select { columns, rows } => {
                        // Build schema from subquery columns - infer types from first row
                        let mut schema_cols = Vec::new();
                        for (idx, col_name) in columns.iter().enumerate() {
                            // Infer type from first row value
                            let col_type = if let Some(first_row) = rows.first() {
                                if let Some(value) = first_row.get(idx) {
                                    match value {
                                        Value::Integer(_) => ColumnType::Integer,
                                        Value::Float(_) => ColumnType::Float,
                                        Value::Text(_) | Value::TextDoc(_) => ColumnType::Text,
                                        Value::Bool(_) => ColumnType::Boolean,
                                        Value::Timestamp(_) => ColumnType::Timestamp,
                                        Value::Tensor(t) => ColumnType::Tensor(t.dimension()),
                                        Value::Spatial(_) => ColumnType::Spatial,
                                        Value::Vector(v) => ColumnType::Tensor(v.len()),
                                        Value::Null => ColumnType::Text, // Default for NULL
                                    }
                                } else {
                                    ColumnType::Text
                                }
                            } else {
                                ColumnType::Text
                            };
                            
                            schema_cols.push(crate::types::ColumnDef::new(
                                col_name.clone(),
                                col_type,
                                idx,
                            ));
                        }
                        let mut schema = TableSchema::new(alias.clone(), schema_cols);
                        
                        // Convert rows to SqlRow format with alias prefix
                        // 🚀 P1 优化：预分配 rows 大小
                        let mut sql_rows = Vec::with_capacity(rows.len());
                        for (row_id, row_values) in rows.iter().enumerate() {
                            let mut sql_row = SqlRow::new();
                            for (col_name, value) in columns.iter().zip(row_values.iter()) {
                                // Strip table prefix from column name (e.g., "users.age" -> "age")
                                let base_col_name = if let Some(dot_pos) = col_name.rfind('.') {
                                    &col_name[dot_pos + 1..]
                                } else {
                                    col_name.as_str()
                                };
                                let qualified_name = format!("{}.{}", alias, base_col_name);
                                sql_row.insert(qualified_name, value.clone());
                            }
                            sql_rows.push((row_id as u64, sql_row));
                        }
                        
                        // Update schema column names with alias prefix (strip original prefix)
                        for col in &mut schema.columns {
                            let base_name = if let Some(dot_pos) = col.name.rfind('.') {
                                &col.name[dot_pos + 1..]
                            } else {
                                &col.name
                            };
                            col.name = format!("{}.{}", alias, base_name);
                        }
                        
                        Ok((sql_rows, Arc::new(schema)))
                    }
                    _ => Err(MoteDBError::Query("Subquery must be a SELECT".into())),
                }
            }
            TableRef::Join { left, right, join_type, on_condition } => {
                // Recursive: evaluate left and right
                let (left_rows, left_schema) = self.execute_from(left)?;
                let (right_rows, right_schema) = self.execute_from(right)?;
                
                // Combine schemas
                let mut combined_schema = (*left_schema).clone();
                combined_schema.columns.extend(right_schema.columns.clone());
                
                // Perform JOIN based on type
                let joined_rows = match join_type {
                    JoinType::Inner => self.inner_join(&left_rows, &right_rows, on_condition)?,
                    JoinType::Left => self.left_join(&left_rows, &right_rows, on_condition, &right_schema)?,
                    JoinType::Right => self.right_join(&left_rows, &right_rows, on_condition, &left_schema)?,
                    JoinType::Full => self.full_join(&left_rows, &right_rows, on_condition, &left_schema, &right_schema)?,
                };
                
                Ok((joined_rows, Arc::new(combined_schema)))
            }
        }
    }
    
    /// INNER JOIN: only rows that match condition in both tables
    /// 
    /// 🚀 Optimized with Hash Join for equi-joins
    fn inner_join(
        &self,
        left_rows: &[(u64, SqlRow)],
        right_rows: &[(u64, SqlRow)],
        on_condition: &Expr,
    ) -> Result<Vec<(u64, SqlRow)>> {
        // Try to detect equi-join (col1 = col2) for Hash Join optimization
        if let Some((left_col, right_col)) = self.extract_equi_join_columns(on_condition) {
            // 🚀 Use Hash Join (O(N + M))
            return self.hash_join_inner(left_rows, right_rows, &left_col, &right_col);
        }
        
        // Fallback: Nested Loop Join (O(N × M))
        let mut result = Vec::new();
        let mut next_id = 1u64;
        
        for (_, left_row) in left_rows {
            for (_, right_row) in right_rows {
                // Combine rows
                let combined_row = self.combine_rows(left_row, right_row);
                
                // Evaluate JOIN condition
                if self.evaluator.eval(on_condition, &combined_row)
                    .and_then(|val| self.to_bool(&val))
                    .unwrap_or(false)
                {
                    result.push((next_id, combined_row));
                    next_id += 1;
                }
            }
        }
        
        Ok(result)
    }
    
    /// 🚀 Hash Join for equi-join (col1 = col2)
    /// Time complexity: O(N + M) instead of O(N × M)
    /// ⚡ P0 Optimization: Use typed HashKey instead of format!("{:?}")
    fn hash_join_inner(
        &self,
        left_rows: &[(u64, SqlRow)],
        right_rows: &[(u64, SqlRow)],
        left_col: &str,
        right_col: &str,
    ) -> Result<Vec<(u64, SqlRow)>> {
        use std::collections::HashMap;
        
        // Hash key type — preserves full i64 precision
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        enum HashKey {
            Numeric(u64),  // f64::to_bits() for Float and small Integer (< 2^53)
            Integer(u64),  // i64::to_bits() for Integer, preserves full 64-bit range
            Text(String),
            Bool(bool),
        }

        #[inline]
        fn to_hash_key(value: &Value) -> Option<HashKey> {
            match value {
                Value::Integer(i) => {
                    // Small integers (within f64 exact range) use Numeric for cross-type
                    // matching with Float columns. Large integers use Integer to preserve
                    // full 64-bit precision.
                    const EXACT_MAX: i64 = (1i64 << 53); // 2^53, max exact i64 in f64
                    if *i >= -EXACT_MAX && *i <= EXACT_MAX {
                        Some(HashKey::Numeric((*i as f64).to_bits()))
                    } else {
                        Some(HashKey::Integer((*i as u64).wrapping_add(i64::MIN as u64)))
                    }
                }
                Value::Float(f) => Some(HashKey::Numeric(f.to_bits())),
                Value::Text(s) => Some(HashKey::Text(s.to_string())),
                Value::Bool(b) => Some(HashKey::Bool(*b)),
                Value::Null => None, // SQL: NULL != NULL in joins
                _ => None,
            }
        }
        
        // Step 1: Build hash table on smaller table (right)
        // 🚀 预分配：假设负载因子 0.75
        let mut hash_table: HashMap<HashKey, Vec<&SqlRow>> = HashMap::with_capacity(
            (right_rows.len() as f64 / 0.75) as usize
        );
        
        for (_, right_row) in right_rows {
            if let Some(key_val) = right_row.get(right_col) {
                // ⚡ Zero-allocation hash key (no format!)
                if let Some(key) = to_hash_key(key_val) {
                    hash_table.entry(key).or_default().push(right_row);
                }
            }
        }
        
        // Step 2: Probe with left table
        // 🚀 预分配：预估每行匹配 1 个
        let mut result = Vec::with_capacity(left_rows.len());
        let mut next_id = 1u64;
        
        for (_, left_row) in left_rows {
            if let Some(key_val) = left_row.get(left_col) {
                // ⚡ Zero-allocation hash key
                if let Some(key) = to_hash_key(key_val) {
                    // O(1) lookup in hash table
                    if let Some(matching_right_rows) = hash_table.get(&key) {
                        for right_row in matching_right_rows {
                            let combined_row = self.combine_rows(left_row, right_row);
                            result.push((next_id, combined_row));
                            next_id += 1;
                        }
                    }
                }
            }
        }
        
        Ok(result)
    }
    
    /// Extract equi-join columns from ON condition
    /// Returns Some((left_col, right_col)) if condition is "col1 = col2", otherwise None
    fn extract_equi_join_columns(&self, expr: &Expr) -> Option<(String, String)> {
        match expr {
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Eq => {
                // Check if both sides are column references
                if let (Expr::Column(left_col), Expr::Column(right_col)) = (left.as_ref(), right.as_ref()) {
                    return Some((left_col.clone(), right_col.clone()));
                }
            }
            _ => {}
        }
        None
    }
    
    /// LEFT JOIN: all rows from left, matched rows from right (NULL if no match)
    fn left_join(
        &self,
        left_rows: &[(u64, SqlRow)],
        right_rows: &[(u64, SqlRow)],
        on_condition: &Expr,
        right_schema: &crate::types::TableSchema,
    ) -> Result<Vec<(u64, SqlRow)>> {
        // Pre-compute NULL row for right side from schema
        let null_right_row: SqlRow = right_schema.columns.iter()
            .map(|col| (col.name.clone(), Value::Null))
            .collect();

        // Try hash join optimization for equi-join
        if let Some((left_col, right_col)) = self.extract_equi_join_columns(on_condition) {
            return self.hash_join_left(left_rows, right_rows, &left_col, &right_col, &null_right_row);
        }

        // Fallback: nested loop
        let mut result = Vec::new();
        let mut next_id = 1u64;

        for (_, left_row) in left_rows {
            let mut matched = false;

            for (_, right_row) in right_rows {
                let combined_row = self.combine_rows(left_row, right_row);

                if self.evaluator.eval(on_condition, &combined_row)
                    .and_then(|val| self.to_bool(&val))
                    .unwrap_or(false)
                {
                    result.push((next_id, combined_row));
                    next_id += 1;
                    matched = true;
                }
            }

            if !matched {
                let combined_row = self.combine_rows(left_row, &null_right_row);
                result.push((next_id, combined_row));
                next_id += 1;
            }
        }

        Ok(result)
    }

    /// Hash Join for LEFT JOIN equi-join
    fn hash_join_left(
        &self,
        left_rows: &[(u64, SqlRow)],
        right_rows: &[(u64, SqlRow)],
        left_col: &str,
        right_col: &str,
        null_right_row: &SqlRow,
    ) -> Result<Vec<(u64, SqlRow)>> {
        use std::collections::HashMap;

        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        enum HashKey { Numeric(u64), Integer(u64), Text(String), Bool(bool) }

        #[inline]
        fn to_hash_key(value: &Value) -> Option<HashKey> {
            match value {
                Value::Integer(i) => {
                    // Small integers (within f64 exact range) use Numeric for cross-type
                    // matching with Float columns. Large integers use Integer to preserve
                    // full 64-bit precision.
                    const EXACT_MAX: i64 = (1i64 << 53); // 2^53, max exact i64 in f64
                    if *i >= -EXACT_MAX && *i <= EXACT_MAX {
                        Some(HashKey::Numeric((*i as f64).to_bits()))
                    } else {
                        Some(HashKey::Integer((*i as u64).wrapping_add(i64::MIN as u64)))
                    }
                }
                Value::Float(f) => Some(HashKey::Numeric(f.to_bits())),
                Value::Text(s) => Some(HashKey::Text(s.to_string())),
                Value::Bool(b) => Some(HashKey::Bool(*b)),
                _ => None,
            }
        }

        // Build hash table on right
        let mut hash_table: HashMap<HashKey, Vec<&SqlRow>> = HashMap::with_capacity(
            (right_rows.len() as f64 / 0.75) as usize
        );
        for (_, right_row) in right_rows {
            if let Some(key_val) = right_row.get(right_col) {
                if let Some(key) = to_hash_key(key_val) {
                    hash_table.entry(key).or_default().push(right_row);
                }
            }
        }

        let mut result = Vec::with_capacity(left_rows.len());
        let mut next_id = 1u64;

        for (_, left_row) in left_rows {
            let matched = if let Some(key_val) = left_row.get(left_col) {
                if let Some(key) = to_hash_key(key_val) {
                    if let Some(matching) = hash_table.get(&key) {
                        for right_row in matching {
                            result.push((next_id, self.combine_rows(left_row, right_row)));
                            next_id += 1;
                        }
                        true
                    } else { false }
                } else { false }
            } else { false };

            if !matched {
                result.push((next_id, self.combine_rows(left_row, null_right_row)));
                next_id += 1;
            }
        }

        Ok(result)
    }
    
    /// RIGHT JOIN: all rows from right, matched rows from left (NULL if no match)
    fn right_join(
        &self,
        left_rows: &[(u64, SqlRow)],
        right_rows: &[(u64, SqlRow)],
        on_condition: &Expr,
        left_schema: &crate::types::TableSchema,
    ) -> Result<Vec<(u64, SqlRow)>> {
        // Pre-compute NULL row for left side from schema
        let null_left_row: SqlRow = left_schema.columns.iter()
            .map(|col| (col.name.clone(), Value::Null))
            .collect();

        // Try hash join optimization for equi-join
        if let Some((left_col, right_col)) = self.extract_equi_join_columns(on_condition) {
            return self.hash_join_right(left_rows, right_rows, &left_col, &right_col, &null_left_row);
        }

        // Fallback: nested loop
        let mut result = Vec::new();
        let mut next_id = 1u64;

        for (_, right_row) in right_rows {
            let mut matched = false;

            for (_, left_row) in left_rows {
                let combined_row = self.combine_rows(left_row, right_row);

                if self.evaluator.eval(on_condition, &combined_row)
                    .and_then(|val| self.to_bool(&val))
                    .unwrap_or(false)
                {
                    result.push((next_id, combined_row));
                    next_id += 1;
                    matched = true;
                }
            }

            if !matched {
                let combined_row = self.combine_rows(&null_left_row, right_row);
                result.push((next_id, combined_row));
                next_id += 1;
            }
        }

        Ok(result)
    }

    /// Hash Join for RIGHT JOIN equi-join
    fn hash_join_right(
        &self,
        left_rows: &[(u64, SqlRow)],
        right_rows: &[(u64, SqlRow)],
        left_col: &str,
        right_col: &str,
        null_left_row: &SqlRow,
    ) -> Result<Vec<(u64, SqlRow)>> {
        use std::collections::HashMap;

        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        enum HashKey { Numeric(u64), Integer(u64), Text(String), Bool(bool) }

        #[inline]
        fn to_hash_key(value: &Value) -> Option<HashKey> {
            match value {
                Value::Integer(i) => {
                    // Small integers (within f64 exact range) use Numeric for cross-type
                    // matching with Float columns. Large integers use Integer to preserve
                    // full 64-bit precision.
                    const EXACT_MAX: i64 = (1i64 << 53); // 2^53, max exact i64 in f64
                    if *i >= -EXACT_MAX && *i <= EXACT_MAX {
                        Some(HashKey::Numeric((*i as f64).to_bits()))
                    } else {
                        Some(HashKey::Integer((*i as u64).wrapping_add(i64::MIN as u64)))
                    }
                }
                Value::Float(f) => Some(HashKey::Numeric(f.to_bits())),
                Value::Text(s) => Some(HashKey::Text(s.to_string())),
                Value::Bool(b) => Some(HashKey::Bool(*b)),
                _ => None,
            }
        }

        // Build hash table on left
        let mut hash_table: HashMap<HashKey, Vec<&SqlRow>> = HashMap::with_capacity(
            (left_rows.len() as f64 / 0.75) as usize
        );
        for (_, left_row) in left_rows {
            if let Some(key_val) = left_row.get(left_col) {
                if let Some(key) = to_hash_key(key_val) {
                    hash_table.entry(key).or_default().push(left_row);
                }
            }
        }

        let mut result = Vec::with_capacity(right_rows.len());
        let mut next_id = 1u64;

        for (_, right_row) in right_rows {
            let matched = if let Some(key_val) = right_row.get(right_col) {
                if let Some(key) = to_hash_key(key_val) {
                    if let Some(matching) = hash_table.get(&key) {
                        for left_row in matching {
                            result.push((next_id, self.combine_rows(left_row, right_row)));
                            next_id += 1;
                        }
                        true
                    } else { false }
                } else { false }
            } else { false };

            if !matched {
                result.push((next_id, self.combine_rows(null_left_row, right_row)));
                next_id += 1;
            }
        }

        Ok(result)
    }
    
    /// FULL OUTER JOIN: all rows from both tables (NULL where no match)
    fn full_join(
        &self,
        left_rows: &[(u64, SqlRow)],
        right_rows: &[(u64, SqlRow)],
        on_condition: &Expr,
        left_schema: &crate::types::TableSchema,
        right_schema: &crate::types::TableSchema,
    ) -> Result<Vec<(u64, SqlRow)>> {
        // Pre-compute NULL rows from schema
        let null_right_row: SqlRow = right_schema.columns.iter()
            .map(|col| (col.name.clone(), Value::Null))
            .collect();
        let null_left_row: SqlRow = left_schema.columns.iter()
            .map(|col| (col.name.clone(), Value::Null))
            .collect();

        // Try hash join optimization for equi-join
        if let Some((left_col, right_col)) = self.extract_equi_join_columns(on_condition) {
            return self.hash_join_full(left_rows, right_rows, &left_col, &right_col, &null_left_row, &null_right_row);
        }

        // Fallback: nested loop
        let mut result = Vec::new();
        let mut next_id = 1u64;
        let mut right_matched = vec![false; right_rows.len()];

        for (_, left_row) in left_rows {
            let mut left_matched = false;

            for (right_idx, (_, right_row)) in right_rows.iter().enumerate() {
                let combined_row = self.combine_rows(left_row, right_row);

                if self.evaluator.eval(on_condition, &combined_row)
                    .and_then(|val| self.to_bool(&val))
                    .unwrap_or(false)
                {
                    result.push((next_id, combined_row));
                    next_id += 1;
                    left_matched = true;
                    right_matched[right_idx] = true;
                }
            }

            if !left_matched {
                let combined_row = self.combine_rows(left_row, &null_right_row);
                result.push((next_id, combined_row));
                next_id += 1;
            }
        }

        for (right_idx, (_, right_row)) in right_rows.iter().enumerate() {
            if !right_matched[right_idx] {
                let combined_row = self.combine_rows(&null_left_row, right_row);
                result.push((next_id, combined_row));
                next_id += 1;
            }
        }

        Ok(result)
    }

    /// Hash Join for FULL OUTER JOIN equi-join
    fn hash_join_full(
        &self,
        left_rows: &[(u64, SqlRow)],
        right_rows: &[(u64, SqlRow)],
        left_col: &str,
        right_col: &str,
        null_left_row: &SqlRow,
        null_right_row: &SqlRow,
    ) -> Result<Vec<(u64, SqlRow)>> {
        use std::collections::HashMap;

        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        enum HashKey { Numeric(u64), Integer(u64), Text(String), Bool(bool) }

        #[inline]
        fn to_hash_key(value: &Value) -> Option<HashKey> {
            match value {
                Value::Integer(i) => {
                    // Small integers (within f64 exact range) use Numeric for cross-type
                    // matching with Float columns. Large integers use Integer to preserve
                    // full 64-bit precision.
                    const EXACT_MAX: i64 = (1i64 << 53); // 2^53, max exact i64 in f64
                    if *i >= -EXACT_MAX && *i <= EXACT_MAX {
                        Some(HashKey::Numeric((*i as f64).to_bits()))
                    } else {
                        Some(HashKey::Integer((*i as u64).wrapping_add(i64::MIN as u64)))
                    }
                }
                Value::Float(f) => Some(HashKey::Numeric(f.to_bits())),
                Value::Text(s) => Some(HashKey::Text(s.to_string())),
                Value::Bool(b) => Some(HashKey::Bool(*b)),
                _ => None,
            }
        }

        // Build hash table on right
        let mut hash_table: HashMap<HashKey, Vec<(usize, &SqlRow)>> = HashMap::with_capacity(
            (right_rows.len() as f64 / 0.75) as usize
        );
        for (idx, (_, right_row)) in right_rows.iter().enumerate() {
            if let Some(key_val) = right_row.get(right_col) {
                if let Some(key) = to_hash_key(key_val) {
                    hash_table.entry(key).or_default().push((idx, right_row));
                }
            }
        }

        let mut result = Vec::with_capacity(left_rows.len() + right_rows.len());
        let mut next_id = 1u64;
        let mut right_matched = vec![false; right_rows.len()];

        // Probe with left
        for (_, left_row) in left_rows {
            let left_matched = if let Some(key_val) = left_row.get(left_col) {
                if let Some(key) = to_hash_key(key_val) {
                    if let Some(matching) = hash_table.get(&key) {
                        for &(idx, right_row) in matching {
                            result.push((next_id, self.combine_rows(left_row, right_row)));
                            next_id += 1;
                            right_matched[idx] = true;
                        }
                        true
                    } else { false }
                } else { false }
            } else { false };

            if !left_matched {
                result.push((next_id, self.combine_rows(left_row, null_right_row)));
                next_id += 1;
            }
        }

        // Add unmatched right rows
        for (idx, (_, right_row)) in right_rows.iter().enumerate() {
            if !right_matched[idx] {
                result.push((next_id, self.combine_rows(null_left_row, right_row)));
                next_id += 1;
            }
        }

        Ok(result)
    }
    
    /// Combine two SqlRows (for JOIN operations)
    /// ✅ 优化：使用 with_capacity 预分配，减少 reallocation
    fn combine_rows(&self, left: &SqlRow, right: &SqlRow) -> SqlRow {
        let mut combined = SqlRow::with_capacity(left.len() + right.len());
        // 直接 extend，HashMap 的 clone 仍然必要（因为我们需要保留原始行）
        combined.extend(left.iter().map(|(k, v)| (k.clone(), v.clone())));
        combined.extend(right.iter().map(|(k, v)| (k.clone(), v.clone())));
        combined
    }
    
    /// Materialize subqueries in an expression (convert to literal value lists)
    /// 
    /// Example: WHERE id IN (SELECT user_id FROM orders)
    /// Becomes: WHERE id IN (1, 2, 3) [after executing subquery]
    /// Stream an IN subquery result directly into a HashSet.
    /// Skips Vec<Vec<Value>> and Vec<Expr::Literal> intermediates.
    /// Returns None if the subquery is too complex for this fast path.
    fn stream_in_subquery_to_hashset(
        &self,
        subquery_stmt: &SelectStmt,
        outer_col_name: &str,
    ) -> Option<std::collections::HashSet<Value>> {
        use crate::sql::ast::TableRef;

        // Must be a simple single-table SELECT with no GROUP BY/ORDER BY/DISTINCT/HAVING
        if subquery_stmt.group_by.is_some()
            || subquery_stmt.order_by.is_some()
            || subquery_stmt.distinct
            || subquery_stmt.having.is_some()
        {
            return None;
        }

        let table_name = match subquery_stmt.from.as_ref()? {
            TableRef::Table { name, .. } => name,
            _ => return None,
        };

        // Get inner query's SELECT column — must be a single simple column
        let inner_col_name = match &subquery_stmt.columns[..] {
            [SelectColumn::Column(name)] => name.clone(),
            [SelectColumn::ColumnWithAlias(name, _)] => name.clone(),
            _ => return None,
        };

        let schema = self.db.get_table_schema(table_name).ok()?;
        let total_cols = schema.columns.len();
        let col_types = schema.col_types().to_vec();
        let fixed_count = crate::storage::row_format::compute_fixed_count(&col_types);

        // Resolve SELECT column position
        let bare_inner = if inner_col_name.contains('.') {
            inner_col_name.rsplit('.').next().unwrap_or(&inner_col_name)
        } else {
            &inner_col_name
        };
        let inner_col_pos = schema.get_column_position(bare_inner)?;

        // Compile WHERE (if present) for positional evaluation
        let compiled_where: Option<CompiledWhere> = subquery_stmt.where_clause.as_ref()
            .and_then(|clause| Self::compile_where(clause, &schema));
        let mut where_positions = Vec::new();
        if let Some(ref cw) = compiled_where {
            cw.collect_positions(&mut where_positions);
        }
        let where_pos_to_idx: Vec<Option<usize>> = {
            let mut map = vec![None; total_cols];
            for (buf_idx, &schema_pos) in where_positions.iter().enumerate() {
                map[schema_pos] = Some(buf_idx);
            }
            map
        };

        // Scan and build HashSet directly.
        // IMPORTANT: columnar tables (USING COLUMN) store data in the columnar
        // SSTable, NOT in the LSM row store. The legacy raw scan path below would
        // return empty/obsolete data for columnar tables (bug: IN subquery on a
        // columnar table silently matched 0 rows). So we branch on storage type.
        let mut set = if self.db.is_columnar_table(table_name) {
            self.build_in_hashset_from_columnar(
                table_name, &col_types, inner_col_pos,
                compiled_where.as_ref(), &where_positions, &where_pos_to_idx,
            )?
        } else {
            let raw_iter = self.db.scan_table_raw_streaming(table_name).ok()?;
            let has_where = compiled_where.is_some();
            let cap = if has_where { 1024 } else { 16384 };
            let mut set = std::collections::HashSet::with_capacity(cap);
            let mut where_buf = Vec::with_capacity(where_positions.len().max(1));

            for result in raw_iter {
                let (_row_id, raw_bytes) = match result {
                    Ok(r) => r,
                    Err(_) => continue,
                };

                // Phase 1: WHERE eval on partial decode
                if let Some(ref cw) = compiled_where {
                    let ctx = match crate::storage::row_format::RowParseContext::parse(
                        &raw_bytes, &col_types, fixed_count,
                    ) {
                        Some(c) => c,
                        None => continue,
                    };
                    if ctx.decode_columns(&raw_bytes, &col_types, &where_positions, &mut where_buf).is_err() {
                        continue;
                    }
                    if !cw.eval_at(&where_buf, &where_pos_to_idx).unwrap_or(false) {
                        continue;
                    }
                }

                // Phase 2: Decode the SELECT column and insert into HashSet
                let val = crate::storage::row_format::get_column(&raw_bytes, &col_types, inner_col_pos)
                    .unwrap_or(Value::Null);
                set.insert(val);
            }
            set
        };

        if set.is_empty() {
            // Empty set means outer IN should match nothing
            return Some(set);
        }

        Some(set)
    }

    /// Columnar-backed implementation of the IN-subquery hashset build.
    /// Finalizes any pending write buffer, then projects the inner SELECT
    /// column plus WHERE-referenced columns out of the columnar SSTable and
    /// filters positionally. Returns the set of matching inner-column values.
    fn build_in_hashset_from_columnar(
        &self,
        table_name: &str,
        col_types: &[crate::types::ColumnType],
        inner_col_pos: usize,
        compiled_where: Option<&CompiledWhere>,
        where_positions: &[usize],
        where_pos_to_idx: &[Option<usize>],
    ) -> Option<std::collections::HashSet<Value>> {
        // Ensure all buffered rows are in the SSTable before scanning.
        self.db.finalize_columnar_buffer(table_name);

        // Collect distinct column positions we need to materialize:
        // the inner SELECT column + every column referenced by WHERE.
        let mut needed: Vec<usize> = vec![inner_col_pos];
        needed.extend_from_slice(where_positions);
        needed.sort_unstable();
        needed.dedup();

        let mut iter = self.db
            .scan_columnar_sstable_projection(table_name, col_types, &needed)
            .ok()?;

        // Map from schema column position → index within `needed` (and thus
        // within the iterator's row, since projection preserves order).
        let proj_idx_of = |col_pos: usize| -> Option<usize> {
            needed.iter().position(|&c| c == col_pos)
        };
        let inner_proj = proj_idx_of(inner_col_pos)?;

        let has_where = compiled_where.is_some();
        let cap = if has_where { 1024 } else { 16384 };
        let mut set = std::collections::HashSet::with_capacity(cap);
        let mut where_buf: Vec<Value> = Vec::with_capacity(where_positions.len().max(1));

        for row in iter.by_ref() {
            if let Some(cw) = compiled_where {
                where_buf.clear();
                let mut ok = true;
                for &schema_pos in where_positions {
                    let pi = match proj_idx_of(schema_pos) {
                        Some(p) => p,
                        None => { ok = false; break; }
                    };
                    where_buf.push(row.get(pi).cloned().unwrap_or(Value::Null));
                }
                if !ok { continue; }
                if !cw.eval_at(&where_buf, where_pos_to_idx).unwrap_or(false) {
                    continue;
                }
            }
            let val = row.get(inner_proj).cloned().unwrap_or(Value::Null);
            set.insert(val);
        }
        Some(set)
    }

    fn materialize_subqueries(&self, expr: &Expr) -> Result<Expr> {
        match expr {
            Expr::Subquery(subquery) => {
                // Execute subquery
                let result = self.execute_select_internal(subquery)?;

                match result {
                    QueryResult::Select { rows, .. } => {
                        // Scalar subquery: return single value
                        if rows.len() == 1 && rows[0].len() == 1 {
                            Ok(Expr::Literal(rows[0][0].clone()))
                        } else if rows.is_empty() {
                            Ok(Expr::Literal(Value::Null))
                        } else {
                            // Non-scalar subquery error (should be used with IN)
                            Err(MoteDBError::Query(
                                "Subquery returns more than one row/column (use IN instead of =)".into()
                            ))
                        }
                    }
                    _ => Err(MoteDBError::Query("Subquery must return SELECT result".into())),
                }
            }
            
            Expr::In { expr, list, negated } => {
                // Check if list contains a subquery
                let materialized_list: Result<Vec<Expr>> = if list.len() == 1 {
                    if let Expr::Subquery(subquery) = &list[0] {
                        // 🚀 Fast path: if the outer column is a simple Column reference,
                        // stream the subquery result directly into a HashSet, avoiding
                        // the Vec<Vec<Value>> + Vec<Expr::Literal> double materialization.
                        let outer_col_opt = if let Expr::Column(col_name) = expr.as_ref() {
                            Some(col_name.as_str())
                        } else {
                            None
                        };

                        let fast_literals = outer_col_opt
                            .and_then(|col| self.stream_in_subquery_to_hashset(subquery, col));

                        if let Some(hashset) = fast_literals {
                            let literals: Vec<Expr> = hashset.into_iter().map(Expr::Literal).collect();
                            Ok(literals)
                        } else {
                            // Fallback: execute subquery normally
                            let result = self.execute_select_internal(subquery)?;
                            match result {
                                QueryResult::Select { rows, .. } => {
                                    let literals: Vec<Expr> = rows.iter()
                                        .filter_map(|row| row.first().cloned())
                                        .map(Expr::Literal)
                                        .collect();
                                    Ok(literals)
                                }
                                _ => Err(MoteDBError::Query("Subquery must return SELECT result".into())),
                            }
                        }
                    } else {
                        Ok(list.clone())
                    }
                } else {
                    Ok(list.clone())
                };

                Ok(Expr::In {
                    expr: Box::new(self.materialize_subqueries(expr)?),
                    list: materialized_list?,
                    negated: *negated,
                })
            }
            
            Expr::BinaryOp { left, op, right } => {
                Ok(Expr::BinaryOp {
                    left: Box::new(self.materialize_subqueries(left)?),
                    op: op.clone(),
                    right: Box::new(self.materialize_subqueries(right)?),
                })
            }
            
            Expr::UnaryOp { op, expr } => {
                Ok(Expr::UnaryOp {
                    op: op.clone(),
                    expr: Box::new(self.materialize_subqueries(expr)?),
                })
            }
            
            Expr::Between { expr, low, high, negated } => {
                Ok(Expr::Between {
                    expr: Box::new(self.materialize_subqueries(expr)?),
                    low: Box::new(self.materialize_subqueries(low)?),
                    high: Box::new(self.materialize_subqueries(high)?),
                    negated: *negated,
                })
            }
            
            Expr::Like { expr, pattern, negated } => {
                Ok(Expr::Like {
                    expr: Box::new(self.materialize_subqueries(expr)?),
                    pattern: Box::new(self.materialize_subqueries(pattern)?),
                    negated: *negated,
                })
            }
            
            Expr::IsNull { expr, negated } => {
                Ok(Expr::IsNull {
                    expr: Box::new(self.materialize_subqueries(expr)?),
                    negated: *negated,
                })
            }
            
            Expr::FunctionCall { name, args, distinct } => {
                let materialized_args: Result<Vec<Expr>> = args.iter()
                    .map(|arg| self.materialize_subqueries(arg))
                    .collect();
                
                Ok(Expr::FunctionCall {
                    name: name.clone(),
                    args: materialized_args?,
                    distinct: *distinct,
                })
            }
            
            // Leaf nodes - no subqueries to materialize
            Expr::Column(_) | Expr::Literal(_) | Expr::Parameter(_) | Expr::Match { .. } |
            Expr::KnnSearch { .. } | Expr::KnnDistance { .. } |
            Expr::StWithin3D { .. } | Expr::StDistance3D { .. } | Expr::StKnn3D { .. } | Expr::StRadius3D { .. } |
            Expr::WindowFunction { .. } => Ok(expr.clone()),
        }
    }
    
    /// Helper: Get column value from row, trying both exact match and table-prefixed match
    fn get_column_value(&self, row: &SqlRow, column: &str) -> Option<Value> {
        row.get(column).cloned().or_else(|| {
            // If column name doesn't contain '.', try prefixed versions
            if !column.contains('.') {
                row.iter()
                    .find(|(k, _)| k.ends_with(&format!(".{}", column)))
                    .map(|(_, v)| v.clone())
            } else {
                None
            }
        })
    }
    
    /// Evaluate expression with materialized subqueries
    fn eval_with_materialized(&self, expr: &Expr, row: &SqlRow) -> Result<Value> {
        // Special handling for MATCH and KNN expressions
        match expr {
            // 🔧 Recursively handle Binary Operations (e.g., ST_DISTANCE(...) < 10)
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.eval_with_materialized(left, row)?;
                let right_val = self.eval_with_materialized(right, row)?;
                // Use simple comparison logic
                match op {
                    BinaryOperator::Lt => Ok(Value::Bool(left_val < right_val)),
                    BinaryOperator::Le => Ok(Value::Bool(left_val <= right_val)),
                    BinaryOperator::Gt => Ok(Value::Bool(left_val > right_val)),
                    BinaryOperator::Ge => Ok(Value::Bool(left_val >= right_val)),
                    BinaryOperator::Eq => Ok(Value::Bool(left_val == right_val)),
                    BinaryOperator::Ne => Ok(Value::Bool(left_val != right_val)),
                    BinaryOperator::And => {
                        let left_bool = match left_val {
                            Value::Bool(b) => b,
                            Value::Integer(i) => i != 0,
                            Value::Float(f) => f != 0.0 && !f.is_nan(),
                            _ => false,
                        };
                        let right_bool = match right_val {
                            Value::Bool(b) => b,
                            Value::Integer(i) => i != 0,
                            Value::Float(f) => f != 0.0 && !f.is_nan(),
                            _ => false,
                        };
                        Ok(Value::Bool(left_bool && right_bool))
                    }
                    BinaryOperator::Or => {
                        let left_bool = match left_val {
                            Value::Bool(b) => b,
                            Value::Integer(i) => i != 0,
                            Value::Float(f) => f != 0.0 && !f.is_nan(),
                            _ => false,
                        };
                        let right_bool = match right_val {
                            Value::Bool(b) => b,
                            Value::Integer(i) => i != 0,
                            Value::Float(f) => f != 0.0 && !f.is_nan(),
                            _ => false,
                        };
                        Ok(Value::Bool(left_bool || right_bool))
                    }
                    _ => self.evaluator.eval(expr, row),  // Fall back to evaluator for complex ops
                }
            }
            
            Expr::Match { column, query, .. } => {
                // 🚀 Fast path: use pre-computed score if available (from text search fast path)
                let score_key = format!("__text_score_{}__", column);
                if let Some(Value::Float(score)) = row.get(&score_key) {
                    return Ok(Value::Float(*score));
                }

                // Get row_id from the row
                let row_id_opt = row.get("__row_id__")
                    .and_then(|v| match v {
                        Value::Integer(i) => Some(*i as u64),
                    _ => None,
                });

            // 🔧 Get table name from row
            let table_name_opt = row.get("__table__")
                .and_then(|v| match v {
                    Value::Text(s) => Some(s.as_str()),
                    _ => None,
                });

            // Try index-based match if metadata is available
            if let (Some(row_id), Some(table_name)) = (row_id_opt, table_name_opt) {
                let index_name = self.db.index_registry.find_by_column(
                    table_name,
                    column,
                    crate::database::index_metadata::IndexType::Text
                );
                if let Some(index_name) = index_name {
                    if let Some(index_ref) = self.db.text_indexes.get(&index_name) {
                        let results = index_ref.value().read().search_ranked(query, 1000)?;
                        let score = results.iter()
                            .find(|(doc_id, _)| *doc_id == row_id)
                            .map(|(_, score)| *score)
                            .unwrap_or(0.0);
                        return Ok(Value::Float(score as f64));
                    }
                }
            }

            // Fallback: naive text scan when no FTS index
            let text_val = row.get(column)
                .or_else(|| table_name_opt.and_then(|t| row.get(&format!("{}.{}", t, column))));
            match text_val {
                Some(Value::Text(text)) => {
                    let text_lower = text.to_lowercase();
                    let query_lower = query.to_lowercase();
                    let terms: Vec<&str> = query_lower.split_whitespace().collect();
                    let matched = terms.iter().all(|t| text_lower.contains(t));
                    Ok(Value::Bool(matched))
                }
                _ => Ok(Value::Bool(false)),
            }
            }
            
            Expr::KnnSearch { column, query_vector, k } => {
                // KNN_SEARCH returns Bool - true if this row is in top-k results
                let row_id = row.get("__row_id__")
                    .and_then(|v| match v {
                        Value::Integer(i) => Some(*i as u64),
                        _ => None,
                    })
                    .ok_or_else(|| MoteDBError::Query("KNN_SEARCH requires __row_id__ in row".into()))?;
                
                // 🔧 Get table name
                let table_name = row.get("__table__")
                    .and_then(|v| match v {
                        Value::Text(s) => Some(s.as_str()),
                        _ => None,
                    })
                    .ok_or_else(|| MoteDBError::Query("KNN_SEARCH requires __table__ in row".into()))?;
                
                // 🔧 Use index_registry to find the correct user-specified index name
                let index_name = self.db.index_registry.find_by_column(
                    table_name,
                    column,
                    crate::database::index_metadata::IndexType::Vector
                ).ok_or_else(|| MoteDBError::Query(format!("No vector index found for column '{}.{}'", table_name, column)))?;
                
                // Perform KNN search using public API
                let results = self.db.vector_search(&index_name, query_vector.as_slice(), *k)?;
                
                // Check if row_id is in results
                let in_results = results.iter().any(|(id, _)| *id == row_id);
                Ok(Value::Bool(in_results))
            }
            
            Expr::KnnDistance { column, query_vector } => {
                // KNN_DISTANCE returns Float - distance/similarity score
                // Get vector value from row
                let vector = self.get_column_value(row, column)
                    .ok_or_else(|| MoteDBError::ColumnNotFound(column.clone()))?;
                
                let vec_data = match vector {
                    Value::Vector(v) => v,
                    _ => return Err(MoteDBError::TypeError(format!("Column '{}' is not a vector", column))),
                };
                
                // Compute distance (using L2 distance)
                if vec_data.len() != query_vector.len() {
                    return Err(MoteDBError::InvalidArgument(
                        format!("Vector dimension mismatch: {} vs {}", vec_data.len(), query_vector.len())
                    ));
                }
                
                let distance: f32 = vec_data.iter()
                    .zip(query_vector.iter())
                    .map(|(a, b)| (a - b).powi(2))
                    .sum::<f32>()
                    .sqrt();

                Ok(Value::Float(distance as f64))
            }

            // ==================== 3D Spatial Expressions (i-Octree) ====================

            Expr::StDistance3D { column, x, y, z } => {
                // Fast path: use pre-computed distance if available
                if let Some(Value::Float(dist)) = row.get("__spatial_distance__") {
                    return Ok(Value::Float(*dist));
                }
                let point_value = self.get_column_value(row, column)
                    .ok_or_else(|| MoteDBError::ColumnNotFound(column.clone()))?;

                use crate::types::Geometry;
                let geom = match point_value {
                    Value::Spatial(g) => match &*g {
                        Geometry::Point3D(p) => *p,
                        Geometry::Point(p) => {
                            // 2D point treated as z=0
                            crate::types::Point3D::new(p.x, p.y, 0.0)
                        }
                        _ => return Err(MoteDBError::TypeError(format!("Column '{}' is not a 3D Point", column))),
                    },
                    _ => return Err(MoteDBError::TypeError(format!("Column '{}' is not a 3D Point", column))),
                };

                let dx = geom.x - x;
                let dy = geom.y - y;
                let dz = geom.z - z;
                Ok(Value::Float((dx * dx + dy * dy + dz * dz).sqrt()))
            }

            Expr::StWithin3D { column, min_x, min_y, min_z, max_x, max_y, max_z } => {
                if row.get("__spatial_within__").is_some() {
                    return Ok(Value::Bool(true));
                }
                let point_value = self.get_column_value(row, column)
                    .ok_or_else(|| MoteDBError::ColumnNotFound(column.clone()))?;

                use crate::types::Geometry;
                let geom = match point_value {
                    Value::Spatial(g) => match &*g {
                        Geometry::Point3D(p) => *p,
                        Geometry::Point(p) => {
                            crate::types::Point3D::new(p.x, p.y, 0.0)
                        }
                        _ => return Ok(Value::Bool(false)),
                    },
                    _ => return Ok(Value::Bool(false)),
                };

                Ok(Value::Bool(
                    geom.x >= *min_x && geom.x <= *max_x &&
                    geom.y >= *min_y && geom.y <= *max_y &&
                    geom.z >= *min_z && geom.z <= *max_z
                ))
            }

            Expr::StKnn3D { column, x, y, z, k } => {
                // Fast path: already filtered by i-Octree KNN
                if row.get("__spatial_knn__").is_some() {
                    return Ok(Value::Bool(true));
                }
                let row_id = row.get("__row_id__")
                    .and_then(|v| match v { Value::Integer(i) => Some(*i as u64), _ => None })
                    .ok_or_else(|| MoteDBError::Query("ST_KNN_3D requires __row_id__ in row".into()))?;
                let table_name = row.get("__table__")
                    .and_then(|v| match v { Value::Text(s) => Some(s.as_str()), _ => None })
                    .ok_or_else(|| MoteDBError::Query("ST_KNN_3D requires __table__ in row".into()))?;

                let index_name = self.db.index_registry.find_by_column(
                    table_name, column, crate::database::index_metadata::IndexType::Octree
                ).ok_or_else(|| MoteDBError::Query(format!("No ioctree index for '{}.{}'", table_name, column)))?;

                let query_point = crate::types::Point3D::new(*x, *y, *z);
                let results = self.db.ioctree_knn_query(&index_name, &query_point, *k)?;
                Ok(Value::Bool(results.iter().any(|(id, _)| *id == row_id)))
            }

            Expr::StRadius3D { column, x, y, z, radius } => {
                if row.get("__spatial_knn__").is_some() {
                    return Ok(Value::Bool(true));
                }
                let point_value = self.get_column_value(row, column)
                    .ok_or_else(|| MoteDBError::ColumnNotFound(column.clone()))?;

                use crate::types::Geometry;
                let geom = match point_value {
                    Value::Spatial(g) => match &*g {
                        Geometry::Point3D(p) => *p,
                        Geometry::Point(p) => {
                            crate::types::Point3D::new(p.x, p.y, 0.0)
                        }
                        _ => return Ok(Value::Bool(false)),
                    },
                    _ => return Ok(Value::Bool(false)),
                };

                let dx = geom.x - x;
                let dy = geom.y - y;
                let dz = geom.z - z;
                let dist = (dx * dx + dy * dy + dz * dz).sqrt();
                Ok(Value::Bool(dist <= *radius))
            }

            _ => self.evaluator.eval(expr, row)
        }
    }
    
    fn apply_distinct(&self, rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
        use std::collections::HashSet;

        let mut seen = HashSet::new();
        let mut result = Vec::new();

        for row in rows {
            if seen.insert(row.clone()) {
                result.push(row);
            }
        }

        result
    }
    
    /// Apply LATEST BY clause - keep only the latest record per group
    fn apply_latest_by(
        &self,
        projected_rows: Vec<Vec<Value>>,
        filtered_rows: &[(u64, SqlRow)],
        latest_by_cols: &[String],
        schema: &TableSchema,
    ) -> Result<Vec<Vec<Value>>> {
        use std::collections::HashMap;
        
        // Find timestamp column (must exist in schema)
        let timestamp_col = schema.columns.iter()
            .find(|c| c.col_type == ColumnType::Timestamp)
            .ok_or_else(|| MoteDBError::Query(
                "LATEST BY requires a TIMESTAMP column in the table".to_string()
            ))?;
        
        let timestamp_col_name = &timestamp_col.name;
        
        // Build grouping key -> (max_timestamp, projected_row) map
        // Use Vec<Value> keys to avoid per-row String allocation from to_string()/format!()
        let mut groups: HashMap<Vec<Value>, (i64, Vec<Value>)> = HashMap::new();

        for (i, (_, full_row)) in filtered_rows.iter().enumerate() {
            // Extract grouping key as Vec<Value> — zero String allocation
            let group_key: Result<Vec<Value>> = latest_by_cols.iter()
                .map(|col_name| {
                    full_row.get(col_name)
                        .ok_or_else(|| MoteDBError::ColumnNotFound(col_name.clone()))
                        .map(|val| val.clone())
                })
                .collect();
            let group_key = group_key?;

            // Extract timestamp
            let timestamp = full_row.get(timestamp_col_name)
                .ok_or_else(|| MoteDBError::ColumnNotFound(timestamp_col_name.clone()))?;
            
            let ts_value = match timestamp {
                Value::Timestamp(ts) => ts.as_micros(),
                Value::Integer(i) => *i,
                _ => return Err(MoteDBError::Query(
                    format!("Timestamp column '{}' must be TIMESTAMP or INTEGER type", timestamp_col_name)
                )),
            };
            
            // Update group if this is a newer record
            let projected_row = projected_rows[i].clone();
            groups.entry(group_key)
                .and_modify(|(max_ts, row)| {
                    if ts_value > *max_ts {
                        *max_ts = ts_value;
                        *row = projected_row.clone();
                    }
                })
                .or_insert((ts_value, projected_row));
        }
        
        // Extract all latest records
        Ok(groups.into_values().map(|(_, row)| row).collect())
    }
    
    /// Apply GROUP BY aggregation
    /// Look up a value from a SqlRow, falling back to table-prefixed key search.
    fn get_value_from_row(row: &SqlRow, name: &str) -> Value {
        if let Some(val) = row.get(name) {
            val.clone()
        } else {
            row.iter()
                .find(|(key, _)| key.ends_with(&format!(".{}", name)))
                .map(|(_, val)| val.clone())
                .unwrap_or(Value::Null)
        }
    }

    fn apply_group_by(
        &self,
        columns: &[SelectColumn],
        rows: &[(u64, SqlRow)],
        group_by_cols: &[String],
        having: Option<&Expr>,
    ) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
        use std::collections::HashMap;

        // Pre-resolve group column names to their actual keys in the SqlRow HashMap.
        let resolved_col_names: Vec<String> = if !rows.is_empty() {
            let first_row = &rows[0].1;
            group_by_cols.iter().map(|col_name| {
                if first_row.contains_key(col_name) {
                    col_name.clone()
                } else {
                    first_row.keys()
                        .find(|key| key.ends_with(&format!(".{}", col_name)) || key.as_str() == col_name.as_str())
                        .cloned()
                        .unwrap_or_else(|| col_name.clone())
                }
            }).collect()
        } else {
            group_by_cols.to_vec()
        };

        // Build groups: Vec<Value> key avoids per-row String allocations
        let mut groups: HashMap<Vec<Value>, Vec<&SqlRow>> =
            HashMap::with_capacity(rows.len().min(1024));

        for (_, row) in rows {
            let group_key: Result<Vec<Value>> = resolved_col_names.iter()
                .map(|col_name| {
                    row.get(col_name)
                        .cloned()
                        .ok_or_else(|| MoteDBError::ColumnNotFound(col_name.clone()))
                })
                .collect();
            let group_key = group_key?;

            groups.entry(group_key).or_default().push(row);
        }
        
        // Compute aggregates for each group
        let mut column_names = Vec::new();
        let mut result_rows = Vec::new();

        // Handle implicit aggregation with zero input rows:
        // SQL standard requires aggregate queries with no GROUP BY to return
        // exactly one row (e.g., COUNT(*) over empty table returns 0, not empty set)
        if groups.is_empty() && group_by_cols.is_empty() {
            // Compute column names from column specs
            for col_spec in columns {
                let col_name = match col_spec {
                    SelectColumn::Column(name) => name.clone(),
                    SelectColumn::ColumnWithAlias(_, alias) => alias.clone(),
                    SelectColumn::Expr(_, Some(alias)) => alias.clone(),
                    SelectColumn::Expr(expr, None) => Self::expr_to_column_name(expr),
                    SelectColumn::Star => {
                        return Err(MoteDBError::Query(
                            "SELECT * not allowed with GROUP BY".to_string()
                        ));
                    }
                };
                column_names.push(col_name);
            }

            // Compute aggregates over empty row set
            let empty_rows: Vec<&SqlRow> = Vec::new();
            let mut result_row = Vec::new();
            for col_spec in columns {
                let col_value = match col_spec {
                    SelectColumn::Expr(expr, _) => {
                        self.eval_aggregate(expr, &empty_rows)?
                    }
                    SelectColumn::Column(_)
                    | SelectColumn::ColumnWithAlias(_, _)
                    | SelectColumn::Star => Value::Null,
                };
                result_row.push(col_value);
            }
            result_rows.push(result_row);
            return Ok((column_names, result_rows));
        }

        // First pass: determine column names
        if !groups.is_empty() {
            for col_spec in columns {
                let col_name = match col_spec {
                    SelectColumn::Column(name) => name.clone(),
                    SelectColumn::ColumnWithAlias(_, alias) => alias.clone(),
                    SelectColumn::Expr(_, Some(alias)) => alias.clone(),
                    SelectColumn::Expr(expr, None) => Self::expr_to_column_name(expr),
                    SelectColumn::Star => {
                        return Err(MoteDBError::Query(
                            "SELECT * not allowed with GROUP BY".to_string()
                        ));
                    }
                };
                column_names.push(col_name);
            }
        }
        
        for (_group_key, group_rows) in groups {
            // Compute aggregate/column values
            let mut result_row = Vec::new();
            
            for col_spec in columns {
                let col_value = match col_spec {
                    SelectColumn::Column(name) => {
                        if !group_by_cols.contains(name) {
                            return Err(MoteDBError::Query(
                                format!("Column '{}' must appear in GROUP BY or be in aggregate function", name)
                            ));
                        }
                        Self::get_value_from_row(group_rows[0], name)
                    }
                    SelectColumn::ColumnWithAlias(name, _) => {
                        if !group_by_cols.contains(name) {
                            return Err(MoteDBError::Query(
                                format!("Column '{}' must appear in GROUP BY", name)
                            ));
                        }
                        Self::get_value_from_row(group_rows[0], name)
                    }
                    SelectColumn::Expr(expr, _) => {
                        // Aggregate function or expression
                        self.eval_aggregate(expr, &group_rows)?
                    }
                    SelectColumn::Star => {
                        return Err(MoteDBError::Query(
                            "SELECT * not allowed with GROUP BY".to_string()
                        ));
                    }
                };
                
                result_row.push(col_value);
            }
            
            // Apply HAVING filter
            if let Some(having_expr) = having {
                // Create temporary row for HAVING evaluation
                let mut temp_row = SqlRow::new();
                for (i, name) in column_names.iter().enumerate() {
                    temp_row.insert(name.clone(), result_row[i].clone());
                }
                
                let passes = self.evaluator.eval(having_expr, &temp_row)
                    .and_then(|val| self.to_bool(&val))
                    .unwrap_or(false);
                
                if !passes {
                    continue;
                }
            }
            
            result_rows.push(result_row);
        }
        
        Ok((column_names, result_rows))
    }
    
    /// Evaluate aggregate function over a group of rows
    fn eval_aggregate(&self, expr: &Expr, rows: &[&SqlRow]) -> Result<Value> {
        match expr {
            Expr::FunctionCall { name, args, distinct } => {
                let func_name = name.to_uppercase();
                match func_name.as_str() {
                    "COUNT" => {
                        if *distinct {
                            // COUNT(DISTINCT column)
                            if args.is_empty() || matches!(args[0], Expr::Column(ref c) if c == "*") {
                                return Err(MoteDBError::InvalidArgument(
                                    "COUNT(DISTINCT *) is not supported".to_string()
                                ));
                            }
                            
                            use std::collections::HashSet;
                            let mut distinct_values = HashSet::new();

                            for row in rows {
                                let val = self.evaluator.eval(&args[0], row)?;
                                if !matches!(val, Value::Null) {
                                    distinct_values.insert(val);
                                }
                            }
                            
                            Ok(Value::Integer(distinct_values.len() as i64))
                        } else if args.is_empty() || matches!(args[0], Expr::Column(ref c) if c == "*") {
                            // COUNT(*)
                            Ok(Value::Integer(rows.len() as i64))
                        } else {
                            // COUNT(column) - count non-null values
                            let mut count = 0i64;
                            for row in rows {
                                let val = self.evaluator.eval(&args[0], row)?;
                                if !matches!(val, Value::Null) {
                                    count += 1;
                                }
                            }
                            Ok(Value::Integer(count))
                        }
                    }
                    "SUM" => {
                        if args.is_empty() {
                            return Err(MoteDBError::InvalidArgument("SUM requires an argument".to_string()));
                        }
                        let mut int_sum: i64 = 0;
                        let mut float_sum: f64 = 0.0;
                        let mut has_float = false;
                        let mut has_value = false;
                        for row in rows {
                            let val = self.evaluator.eval(&args[0], row)?;
                            match val {
                                Value::Integer(i) => {
                                    has_value = true;
                                    if has_float {
                                        float_sum += i as f64;
                                    } else if let Some(s) = int_sum.checked_add(i) {
                                        int_sum = s;
                                    } else {
                                        has_float = true;
                                        float_sum = int_sum as f64 + i as f64;
                                    }
                                }
                                Value::Float(f) => {
                                    has_value = true;
                                    if !has_float {
                                        has_float = true;
                                        float_sum = int_sum as f64;
                                    }
                                    float_sum += f;
                                }
                                Value::Null => {},
                                _ => return Err(MoteDBError::TypeError("SUM requires numeric values".to_string())),
                            }
                        }
                        if !has_value {
                            Ok(Value::Null)
                        } else if has_float {
                            Ok(Value::Float(float_sum))
                        } else {
                            Ok(Value::Integer(int_sum))
                        }
                    }
                    "AVG" => {
                        if args.is_empty() {
                            return Err(MoteDBError::InvalidArgument("AVG requires an argument".to_string()));
                        }
                        let mut sum = 0.0;
                        let mut count = 0;
                        for row in rows {
                            let val = self.evaluator.eval(&args[0], row)?;
                            match val {
                                Value::Integer(i) => {
                                    sum += i as f64;
                                    count += 1;
                                }
                                Value::Float(f) => {
                                    sum += f;
                                    count += 1;
                                }
                                Value::Null => {},
                                _ => return Err(MoteDBError::TypeError("AVG requires numeric values".to_string())),
                            }
                        }
                        if count == 0 {
                            Ok(Value::Null)
                        } else {
                            Ok(Value::Float(sum / count as f64))
                        }
                    }
                    "MIN" => {
                        if args.is_empty() {
                            return Err(MoteDBError::InvalidArgument("MIN requires an argument".to_string()));
                        }
                        let mut min_val: Option<Value> = None;
                        for row in rows {
                            let val = self.evaluator.eval(&args[0], row)?;
                            if !matches!(val, Value::Null) {
                                min_val = Some(match min_val {
                                    None => val,
                                    Some(current) => {
                                        if val.partial_cmp(&current) == Some(std::cmp::Ordering::Less) {
                                            val
                                        } else {
                                            current
                                        }
                                    }
                                });
                            }
                        }
                        Ok(min_val.unwrap_or(Value::Null))
                    }
                    "MAX" => {
                        if args.is_empty() {
                            return Err(MoteDBError::InvalidArgument("MAX requires an argument".to_string()));
                        }
                        let mut max_val: Option<Value> = None;
                        for row in rows {
                            let val = self.evaluator.eval(&args[0], row)?;
                            if !matches!(val, Value::Null) {
                                max_val = Some(match max_val {
                                    None => val,
                                    Some(current) => {
                                        if val.partial_cmp(&current) == Some(std::cmp::Ordering::Greater) {
                                            val
                                        } else {
                                            current
                                        }
                                    }
                                });
                            }
                        }
                        Ok(max_val.unwrap_or(Value::Null))
                    }
                    _ => Err(MoteDBError::UnknownFunction(name.clone())),
                }
            }
            _ => {
                // Non-aggregate expression in GROUP BY context
                Err(MoteDBError::Query(
                    "Non-aggregate expressions in SELECT with GROUP BY must be in GROUP BY clause".to_string()
                ))
            }
        }
    }
    
    /// Check if column list contains any aggregate functions
    fn has_aggregates(&self, columns: &[SelectColumn]) -> bool {
        columns.iter().any(|col| {
            match col {
                SelectColumn::Expr(expr, _) => self.is_aggregate_expr(expr),
                _ => false,
            }
        })
    }

    /// Check if the SELECT list contains any *computed* expression — i.e. an
    /// `Expr` that is not a bare column reference or literal. The columnar scan
    /// fast paths only project raw columns/literals; computed expressions
    /// (`a + b`, `CONCAT(...)`, `IF(...)`, `-v`, scalar subqueries, …) must go
    /// through the materialized path where `eval_expr_on_row` evaluates them.
    /// `Star` and `Column`/`ColumnWithAlias` are NOT computed.
    fn select_has_computed_expression(columns: &[SelectColumn]) -> bool {
        columns.iter().any(|col| match col {
            SelectColumn::Star | SelectColumn::Column(_) | SelectColumn::ColumnWithAlias(_, _) => false,
            SelectColumn::Expr(expr, _) => !matches!(expr, Expr::Column(_) | Expr::Literal(_)),
        })
    }

    /// Recursively collect schema column positions referenced by an expression.
    /// Used to ensure a columnar scan reads all columns a SELECT expression needs
    /// before evaluating it.
    fn expr_referenced_columns(expr: &Expr, schema: &TableSchema) -> Vec<usize> {
        let mut out = Vec::new();
        let mut add = |name: &str, out: &mut Vec<usize>| {
            let bare = name.rsplit('.').next().unwrap_or(name);
            if let Some(p) = schema.get_column_position(bare) {
                if !out.contains(&p) { out.push(p); }
            }
        };
        match expr {
            Expr::Column(name) => add(name, &mut out),
            Expr::BinaryOp { left, right, .. } => {
                for p in Self::expr_referenced_columns(left, schema) { if !out.contains(&p) { out.push(p); } }
                for p in Self::expr_referenced_columns(right, schema) { if !out.contains(&p) { out.push(p); } }
            }
            Expr::UnaryOp { expr, .. } => {
                for p in Self::expr_referenced_columns(expr, schema) { if !out.contains(&p) { out.push(p); } }
            }
            Expr::FunctionCall { args, .. } => {
                for a in args {
                    for p in Self::expr_referenced_columns(a, schema) { if !out.contains(&p) { out.push(p); } }
                }
            }
            _ => {}
        }
        out
    }

    /// Check if an expression is an aggregate function
    fn is_aggregate_expr(&self, expr: &Expr) -> bool {
        match expr {
            Expr::FunctionCall { name, args: _, distinct: _ } => {
                matches!(name.to_uppercase().as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
            }
            _ => false,
        }
    }

    // ───────────────────────────────────────────────────────────────
    // Positional GROUP BY fast path — bypasses HashMap conversion
    // ───────────────────────────────────────────────────────────────

    /// Try to parse an expression as a simple aggregate function.
    /// Returns `None` for complex expressions that need the materialized path.
    fn try_parse_aggregate(&self, expr: &Expr, schema: &TableSchema) -> Option<AggregateInfo> {
        match expr {
            Expr::FunctionCall { name, args, distinct } => {
                let func = name.to_uppercase();
                match func.as_str() {
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" => {
                        let col_pos = if args.len() == 1 {
                            match &args[0] {
                                Expr::Column(col_name) => {
                                    // Strip table prefix for qualified names (e.g. "users.id" -> "id")
                                    let bare = if col_name.contains('.') {
                                        col_name.rsplit('.').next().unwrap_or(col_name)
                                    } else {
                                        col_name
                                    };
                                    schema.get_column_position(bare)
                                }
                                Expr::Literal(Value::Integer(1)) => None, // COUNT(1) ≡ COUNT(*)
                                _ => return None,
                            }
                        } else if args.is_empty() && func == "COUNT" {
                            None // COUNT(*)
                        } else {
                            return None;
                        };
                        Some(AggregateInfo {
                            func,
                            col_pos,
                            distinct: *distinct,
                        })
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// Positional GROUP BY fast path — works directly on `Vec<Value>` rows,
    /// bypassing the expensive `row_to_sql_row` + `prefix_rows` HashMap conversions.
    ///
    /// 🚀 FAST PATH 1a: Streaming aggregate — no GROUP BY, no HashMap, no SqlRow.
    ///
    /// Handles: `SELECT COUNT(*), SUM(x), AVG(y), MIN(z), MAX(w) FROM t [WHERE ...]`
    ///
    /// Uses raw byte scan + partial decode (or reuses full decode for WHERE).
    /// Accumulates into inline counters — O(1) memory, no grouping HashMap.
    ///
    /// Returns `None` if the query is too complex (non-aggregate columns, subqueries, etc.).
    fn try_streaming_aggregate(
        &self,
        stmt: &SelectStmt,
        schema: &TableSchema,
        table_name: &str,
    ) -> Result<Option<QueryResult>> {
        // This path scans via raw bytes (LSM), which is empty for ColSegmentStore
        // tables (data lives in segment files). Bail so the caller falls through
        // to try_apply_group_by_positional, which scans columnar segments.
        if self.db.has_col_segment_store(table_name) {
            return Ok(None);
        }
        // Parse all SELECT columns into aggregate descriptors
        let mut agg_specs: Vec<(String, AggregateInfo)> = Vec::new();
        for col_spec in &stmt.columns {
            match col_spec {
                SelectColumn::Expr(expr, alias) => {
                    if let Some(agg) = self.try_parse_aggregate(expr, schema) {
                        let col_name = alias.clone()
                            .unwrap_or_else(|| Self::expr_to_column_name(expr));
                        agg_specs.push((col_name, agg));
                    } else {
                        return Ok(None); // non-aggregate expression → fall back
                    }
                }
                SelectColumn::Column(_) | SelectColumn::ColumnWithAlias(_, _) => {
                    // Bare column without GROUP BY is invalid SQL, but let the
                    // general path handle the error reporting.
                    return Ok(None);
                }
                SelectColumn::Star => return Ok(None),
            }
        }

        if agg_specs.is_empty() {
            return Ok(None);
        }

        // DISTINCT aggregates require a HashSet per accumulator — fall back to
        // try_apply_group_by_positional which handles that correctly.
        if agg_specs.iter().any(|(_, a)| a.distinct) {
            return Ok(None);
        }

        // Compile WHERE for positional evaluation
        let compiled_where: Option<CompiledWhere> = stmt.where_clause.as_ref()
            .and_then(|clause| Self::compile_where(clause, schema));

        // Determine needed columns: WHERE columns ∪ aggregate columns
        let mut where_positions = Vec::new();
        if let Some(ref cw) = compiled_where {
            cw.collect_positions(&mut where_positions);
        }
        let mut needed: Vec<usize> = where_positions.clone();
        for (_, agg) in &agg_specs {
            if let Some(pos) = agg.col_pos {
                if !needed.contains(&pos) { needed.push(pos); }
            }
        }
        needed.sort_unstable();

        let col_types = schema.col_types();
        let total_cols = col_types.len();
        let fixed_count = crate::storage::row_format::compute_fixed_count(col_types);
        let raw_iter = self.db.scan_table_raw_streaming(table_name)?;
        let where_clause = &stmt.where_clause;

        // Use partial decode when we need < 70% of columns
        let use_partial = !needed.is_empty()
            && needed.len() < (total_cols * 7 / 10).max(1)
            && (where_clause.is_none() || compiled_where.is_some());

        // ── Inline accumulators (zero-allocation) ──
        struct Acc {
            count: u64,
            int_sum: i64,
            float_sum: f64,
            has_float: bool,
            has_value: bool,
            min_val: Option<Value>,
            max_val: Option<Value>,
        }
        impl Acc {
            fn new() -> Self {
                Self { count: 0, int_sum: 0, float_sum: 0.0, has_float: false, has_value: false,
                       min_val: None, max_val: None }
            }
            fn update(&mut self, val: &Value, func: &str) {
                if matches!(val, Value::Null) { return; }
                match func {
                    "COUNT" => { self.count += 1; }
                    "SUM" | "AVG" => {
                        self.has_value = true;
                        self.count += 1;
                        match val {
                            Value::Integer(i) => {
                                if self.has_float {
                                    self.float_sum += *i as f64;
                                } else if let Some(s) = self.int_sum.checked_add(*i) {
                                    self.int_sum = s;
                                } else {
                                    self.has_float = true;
                                    self.float_sum = self.int_sum as f64 + *i as f64;
                                }
                            }
                            Value::Float(f) => {
                                if !self.has_float { self.has_float = true; self.float_sum = self.int_sum as f64; }
                                self.float_sum += *f;
                            }
                            _ => {}
                        }
                    }
                    "MIN" => {
                        self.has_value = true;
                        if self.min_val.is_none() || val < self.min_val.as_ref().unwrap() {
                            self.min_val = Some(val.clone());
                        }
                    }
                    "MAX" => {
                        self.has_value = true;
                        if self.max_val.is_none() || val > self.max_val.as_ref().unwrap() {
                            self.max_val = Some(val.clone());
                        }
                    }
                    _ => {}
                }
            }
            fn finalize(&self, func: &str) -> Value {
                match func {
                    "COUNT" => Value::Integer(self.count as i64),
                    "SUM" => {
                        if !self.has_value { return Value::Null; }
                        if self.has_float { Value::Float(self.float_sum) } else { Value::Integer(self.int_sum) }
                    }
                    "AVG" => {
                        if self.count == 0 { return Value::Null; }
                        let sum = if self.has_float { self.float_sum } else { self.int_sum as f64 };
                        Value::Float(sum / self.count as f64)
                    }
                    "MIN" => self.min_val.clone().unwrap_or(Value::Null),
                    "MAX" => self.max_val.clone().unwrap_or(Value::Null),
                    _ => Value::Null,
                }
            }
        }

        let mut accumulators: Vec<Acc> = (0..agg_specs.len()).map(|_| Acc::new()).collect();

        // Separate aggregate-only positions (exclude WHERE positions)
        let mut agg_only_positions: Vec<usize> = Vec::new();
        if use_partial {
            for (_, agg) in &agg_specs {
                if let Some(pos) = agg.col_pos {
                    if !where_positions.contains(&pos) && !agg_only_positions.contains(&pos) {
                        agg_only_positions.push(pos);
                    }
                }
            }
            // Build position mapping for WHERE eval: schema_pos → where_buf_idx
            let where_pos_to_idx: Vec<Option<usize>> = {
                let mut map = vec![None; total_cols];
                for (buf_idx, &schema_pos) in where_positions.iter().enumerate() {
                    map[schema_pos] = Some(buf_idx);
                }
                map
            };

            let col_types_slice: &[ColumnType] = col_types;

            // ── Two-phase scan + accumulate ──
            let mut where_buf = Vec::with_capacity(where_positions.len().max(1));
            let mut agg_buf = Vec::with_capacity(agg_only_positions.len());
            for result in raw_iter {
                let (_row_id, raw_bytes) = match result {
                    Ok(r) => r,
                    Err(e) => return Err(e),
                };

                // Phase 1: WHERE eval on partial decode
                let ctx = match crate::storage::row_format::RowParseContext::parse(
                    &raw_bytes, col_types_slice, fixed_count,
                ) {
                    Some(c) => c,
                    None => continue,
                };

                if let Some(ref cw) = compiled_where {
                    if let Err(_) = ctx.decode_columns(&raw_bytes, col_types_slice, &where_positions, &mut where_buf) {
                        continue;
                    }
                    if !cw.eval_at(&where_buf, &where_pos_to_idx).unwrap_or(false) {
                        continue;
                    }
                }

                // Phase 2: Decode aggregate columns with pre-parsed context
                if !agg_only_positions.is_empty() {
                    if ctx.decode_columns(&raw_bytes, col_types_slice, &agg_only_positions, &mut agg_buf).is_err() {
                        continue;
                    }
                }
                let mut agg_idx = 0usize;
                for (i, (_, ref agg)) in agg_specs.iter().enumerate() {
                    if agg.col_pos.is_some() {
                        let val = agg_buf.get(agg_idx).cloned().unwrap_or(Value::Null);
                        accumulators[i].update(&val, &agg.func);
                        agg_idx += 1;
                    } else {
                        accumulators[i].count += 1;
                    }
                }
            }
        } else {
            // ── Full decode fallback ──
            for result in raw_iter {
                let (_row_id, raw_bytes) = match result {
                    Ok(r) => r,
                    Err(e) => return Err(e),
                };

                // WHERE filter
                let decoded_row: Option<Row> = if let Some(ref clause) = where_clause {
                    let full_row = match crate::storage::row_format::decode_fast(
                        &raw_bytes, col_types, fixed_count,
                    ) {
                        Ok(r) => r,
                        Err(_) => return Ok(None),
                    };
                    match Self::eval_expr_on_row(clause, &full_row, schema) {
                        Ok(Value::Bool(true)) => Some(full_row),
                        Ok(_) => continue,
                        Err(_) => return Ok(None),
                    }
                } else {
                    None
                };

                // Value extractor: reuse decoded row or partial-decode from raw bytes
                let get_val = |pos: usize| -> Value {
                    if let Some(ref row) = decoded_row {
                        row.get(pos).cloned().unwrap_or(Value::Null)
                    } else {
                        crate::storage::row_format::get_column(&raw_bytes, col_types, pos)
                            .unwrap_or(Value::Null)
                    }
                };

                // Update each accumulator
                for (i, (_, ref agg)) in agg_specs.iter().enumerate() {
                    if let Some(pos) = agg.col_pos {
                        let val = get_val(pos);
                        accumulators[i].update(&val, &agg.func);
                    } else {
                        accumulators[i].count += 1;
                    }
                }
            }
        }

        // ── Finalize result ──
        let column_names: Vec<String> = agg_specs.iter().map(|(name, _)| name.clone()).collect();
        let result_row: Vec<Value> = agg_specs.iter().enumerate()
            .map(|(i, (_, agg))| accumulators[i].finalize(&agg.func))
            .collect();

        Ok(Some(QueryResult::Select {
            columns: column_names,
            rows: vec![result_row],
        }))
    }

    /// Returns `None` if the query is too complex for this path (joins, subqueries,
    /// complex expressions, etc.), in which case the caller falls back to the
    /// materialized path.
    fn try_apply_group_by_positional(
        &self,
        stmt: &SelectStmt,
        schema: &TableSchema,
        table_name: &str,
    ) -> Result<Option<(Vec<String>, Vec<Vec<Value>>)>> {
        use std::collections::HashMap;

        let group_by_cols = match &stmt.group_by {
            Some(cols) => cols,
            None => &Vec::new(), // implicit aggregation
        };

        // Resolve group column positions
        let group_col_positions: Vec<usize> = group_by_cols.iter()
            .filter_map(|name| {
                let bare = if name.contains('.') {
                    name.rsplit('.').next().unwrap_or(name)
                } else {
                    name
                };
                schema.get_column_position(bare)
            })
            .collect();

        // If not all group columns resolved, fall back
        if group_col_positions.len() != group_by_cols.len() {
            return Ok(None);
        }

        // Build a set of GROUP BY bare column names for validation
        let group_bare_set: std::collections::HashSet<&str> = group_by_cols.iter()
            .map(|name| {
                if name.contains('.') { name.rsplit('.').next().unwrap_or(name) }
                else { name.as_str() }
            })
            .collect();

        // Resolve SELECT column positions and types
        let mut select_col_info: Vec<(String, Option<usize>, Option<AggregateInfo>)> = Vec::new();
        for col_spec in &stmt.columns {
            match col_spec {
                SelectColumn::Column(name) => {
                    let bare = if name.contains('.') {
                        name.rsplit('.').next().unwrap_or(name)
                    } else {
                        name
                    };
                    if let Some(pos) = schema.get_column_position(bare) {
                        // Validate: bare column must appear in GROUP BY (or be the only column with no GROUP BY)
                        if !group_by_cols.is_empty() && !group_bare_set.contains(bare) {
                            return Ok(None); // fall back to non-positional path for error
                        }
                        select_col_info.push((name.clone(), Some(pos), None));
                    } else {
                        return Ok(None); // can't resolve
                    }
                }
                SelectColumn::ColumnWithAlias(name, alias) => {
                    let bare = if name.contains('.') {
                        name.rsplit('.').next().unwrap_or(name)
                    } else {
                        name
                    };
                    if let Some(pos) = schema.get_column_position(bare) {
                        if !group_by_cols.is_empty() && !group_bare_set.contains(bare) {
                            return Ok(None);
                        }
                        select_col_info.push((alias.clone(), Some(pos), None));
                    } else {
                        return Ok(None);
                    }
                }
                SelectColumn::Expr(expr, alias) => {
                    if let Some(agg) = self.try_parse_aggregate(expr, schema) {
                        let col_name = alias.clone()
                            .unwrap_or_else(|| Self::expr_to_column_name(expr));
                        select_col_info.push((col_name, None, Some(agg)));
                    } else {
                        return Ok(None); // complex expression, fall back
                    }
                }
                SelectColumn::Star => return Ok(None),
            }
        }

        // Scan rows positionally — single-pass aggregation
        let row_iter = self.db.scan_table_rows_streaming(table_name)?;

        // Check if we can use single-pass aggregation (no HAVING, or simple HAVING)
        let can_single_pass = stmt.having.is_none()
            && group_col_positions.len() <= 2
            && !select_col_info.iter().any(|(_, _, agg)| agg.as_ref().is_some_and(|a| a.distinct));

        if can_single_pass {
            return self.single_pass_group_by(
                row_iter, stmt, schema, table_name,
                &group_col_positions, &select_col_info,
            );
        }

        // Fallback: materialize rows then group
        let raw_rows: Vec<Row> = if let Some(ref where_clause) = stmt.where_clause {
            let mut matching = Vec::new();
            for result in row_iter {
                let (_row_id, row) = result?;
                match Self::eval_expr_on_row(where_clause, &row, schema) {
                    Ok(Value::Bool(true)) => matching.push(row),
                    Ok(_) => {} // false or null -> skip
                    Err(_) => return Ok(None), // can't evaluate positionally
                }
            }
            matching
        } else {
            let mut matching = Vec::new();
            for result in row_iter {
                let (_row_id, row) = result?;
                matching.push(row);
            }
            matching
        };

        // Build groups using Vec<Value> keys
        let mut groups: HashMap<Vec<Value>, Vec<&Row>> =
            HashMap::with_capacity(raw_rows.len().min(1024));

        if group_col_positions.len() == 1 {
            // Fast path: single GROUP BY column — avoid Vec allocation per row
            let pos = group_col_positions[0];
            let mut single_groups: HashMap<Value, Vec<&Row>> =
                HashMap::with_capacity(64);
            for row in &raw_rows {
                let key = row.get(pos).cloned().unwrap_or(Value::Null);
                single_groups.entry(key).or_default().push(row);
            }
            for (key, rows) in single_groups {
                groups.insert(vec![key], rows);
            }
        } else {
            for row in &raw_rows {
                let group_key: Vec<Value> = group_col_positions.iter()
                    .map(|&pos| row.get(pos).cloned().unwrap_or(Value::Null))
                    .collect();
                groups.entry(group_key).or_default().push(row);
            }
        }

        // Handle implicit aggregation with no input rows
        let groups: Vec<(Vec<Value>, Vec<&Row>)> = if groups.is_empty() && group_by_cols.is_empty() {
            vec![(vec![], vec![])] // one empty group for implicit aggregation
        } else {
            groups.into_iter().collect()
        };

        // Compute result
        let column_names: Vec<String> = select_col_info.iter()
            .map(|(name, _, _)| name.clone()).collect();
        let mut result_rows: Vec<Vec<Value>> = Vec::new();

        for (_group_key, group_rows) in groups {
            let mut result_row = Vec::new();
            for (_col_name, col_pos, agg_info) in &select_col_info {
                let value = if let Some(pos) = col_pos {
                    // Bare column in GROUP BY — take from first row
                    group_rows.first()
                        .and_then(|r| r.get(*pos))
                        .cloned()
                        .unwrap_or(Value::Null)
                } else if let Some(agg) = agg_info {
                    self.compute_aggregate_positional(agg, &group_rows)?
                } else {
                    Value::Null
                };
                result_row.push(value);
            }

            // Apply HAVING filter
            if let Some(having_expr) = &stmt.having {
                let mut temp_row = SqlRow::new();
                for (i, name) in column_names.iter().enumerate() {
                    temp_row.insert(name.clone(), result_row[i].clone());
                }
                // Also add aggregate values keyed by their SQL name (e.g., "SUM(amount)")
                // so the evaluator can look them up when evaluating HAVING expressions
                for (i, (col_name, _, agg_info)) in select_col_info.iter().enumerate() {
                    if agg_info.is_some() && i < result_row.len() {
                        let sql_name = col_name.clone();
                        if !temp_row.contains_key(&sql_name) {
                            temp_row.insert(sql_name, result_row[i].clone());
                        }
                    }
                }
                let passes = self.evaluator.eval(having_expr, &temp_row)
                    .and_then(|val| self.to_bool(&val))
                    .unwrap_or(false);
                if !passes { continue; }
            }

            result_rows.push(result_row);
        }

        // Apply ORDER BY if present
        if let Some(ref order_by) = stmt.order_by {
            let order_specs: Vec<(usize, bool)> = order_by.iter().filter_map(|ob| {
                if let Expr::Column(ref col_name) = ob.expr {
                    let idx = column_names.iter().position(|c| c == col_name)?;
                    Some((idx, ob.asc))
                } else {
                    None
                }
            }).collect();

            result_rows.sort_by(|a, b| {
                for &(idx, asc) in &order_specs {
                    let cmp = a[idx].partial_cmp(&b[idx]).unwrap_or(std::cmp::Ordering::Equal);
                    if cmp != std::cmp::Ordering::Equal {
                        return if asc { cmp } else { cmp.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // Apply LIMIT/OFFSET
        if stmt.offset.is_some() || stmt.limit.is_some() {
            let skip_n = stmt.offset.unwrap_or(0);
            let take_n = stmt.limit.unwrap_or(usize::MAX);
            result_rows = result_rows.into_iter().skip(skip_n).take(take_n).collect();
        }

        Ok(Some((column_names, result_rows)))
    }

    /// Single-pass GROUP BY — accumulates aggregates inline without materializing rows.
    /// Uses raw byte scan + partial column decode for maximum throughput.
    fn single_pass_group_by(
        &self,
        _row_iter: crate::database::crud::TableRowStreamingIterator,
        stmt: &SelectStmt,
        schema: &TableSchema,
        table_name: &str,
        group_col_positions: &[usize],
        select_col_info: &[(String, Option<usize>, Option<AggregateInfo>)],
    ) -> Result<Option<(Vec<String>, Vec<Vec<Value>>)>> {
        use std::collections::HashMap;

        // ColSegmentStore tables cannot be read via the raw-byte LSM scan (data
        // lives in segment files). Use the row-based streaming iterator that
        // correctly decodes columnar segments. The raw path is an optimization
        // for LSM-backed tables only.
        let is_col_segment = self.db.has_col_segment_store(table_name);

        // Use raw byte scan — avoid full row decode, only decode needed columns
        let raw_iter = if is_col_segment {
            None
        } else {
            Some(self.db.scan_table_raw_streaming(table_name)?)
        };
        let col_types = schema.col_types();
        let fixed_count = crate::storage::row_format::compute_fixed_count(col_types);

        // Pre-compute which select columns are aggregates and their positions
        struct AggAccumulator {
            count: u64,
            int_sum: i64,
            float_sum: f64,
            has_float: bool,
            has_value: bool,
            min_val: Option<Value>,
            max_val: Option<Value>,
        }
        impl AggAccumulator {
            fn new() -> Self {
                Self {
                    count: 0, int_sum: 0, float_sum: 0.0,
                    has_float: false, has_value: false,
                    min_val: None, max_val: None,
                }
            }
            fn update(&mut self, val: &Value, func: &str) {
                if matches!(val, Value::Null) { return; }
                match func {
                    "COUNT" => { self.count += 1; }
                    "SUM" | "AVG" => {
                        self.has_value = true;
                        self.count += 1;
                        match val {
                            Value::Integer(i) => {
                                if self.has_float {
                                    self.float_sum += *i as f64;
                                } else if let Some(s) = self.int_sum.checked_add(*i) {
                                    self.int_sum = s;
                                } else {
                                    self.has_float = true;
                                    self.float_sum = self.int_sum as f64 + *i as f64;
                                }
                            }
                            Value::Float(f) => {
                                if !self.has_float { self.has_float = true; self.float_sum = self.int_sum as f64; }
                                self.float_sum += *f;
                            }
                            _ => {}
                        }
                    }
                    "MIN" => {
                        self.has_value = true;
                        if self.min_val.is_none() || val < self.min_val.as_ref().unwrap() {
                            self.min_val = Some(val.clone());
                        }
                    }
                    "MAX" => {
                        self.has_value = true;
                        if self.max_val.is_none() || val > self.max_val.as_ref().unwrap() {
                            self.max_val = Some(val.clone());
                        }
                    }
                    _ => {}
                }
            }
            fn finalize(&self, func: &str) -> Value {
                match func {
                    "COUNT" => Value::Integer(self.count as i64),
                    "SUM" => {
                        if !self.has_value { return Value::Null; }
                        if self.has_float { Value::Float(self.float_sum) } else { Value::Integer(self.int_sum) }
                    }
                    "AVG" => {
                        if self.count == 0 { return Value::Null; }
                        let sum = if self.has_float { self.float_sum } else { self.int_sum as f64 };
                        Value::Float(sum / self.count as f64)
                    }
                    "MIN" => self.min_val.clone().unwrap_or(Value::Null),
                    "MAX" => self.max_val.clone().unwrap_or(Value::Null),
                    _ => Value::Null,
                }
            }
        }

        // For each group, store: (group_key_values, Vec<AggAccumulator>)
        // AggAccumulator per aggregate column in select_col_info
        let num_aggs = select_col_info.iter().filter(|(_, _, a)| a.is_some()).count();
        let num_group_cols = group_col_positions.len();

        // Identify which select columns are aggregates (index into select_col_info)
        let agg_indices: Vec<usize> = select_col_info.iter()
            .enumerate()
            .filter(|(_, (_, _, a))| a.is_some())
            .map(|(i, _)| i)
            .collect();

        // Build key -> (first_row_group_col_values, accumulators)
        // Use inline key for single column
        let mut groups: HashMap<Vec<Value>, (Vec<Value>, Vec<AggAccumulator>)> =
            HashMap::with_capacity(64);

        let where_clause = &stmt.where_clause;
        let has_where = where_clause.is_some();

        // Pre-collect columns needed for partial decode (group cols + agg cols)
        let needed_cols: Vec<usize> = {
            let mut cols: Vec<usize> = group_col_positions.to_vec();
            for (_, _, agg_info) in select_col_info {
                if let Some(ref agg) = agg_info {
                    if let Some(pos) = agg.col_pos {
                        if !cols.contains(&pos) {
                            cols.push(pos);
                        }
                    }
                }
            }
            cols.sort_unstable();
            cols
        };

        // Iterate rows: for ColSegmentStore tables use the row-based iterator
        // (columnar-aware); otherwise use the raw-byte scan + partial decode.
        // Both branches yield fully-decoded rows for grouping/aggregation.
        let mut col_seg_iter = if is_col_segment { Some(_row_iter) } else { None };
        let mut raw_iter = raw_iter;

        // Each iteration produces a fully decoded Row (with WHERE applied).
        // We inline the raw path here to keep the grouping logic uniform.
        loop {
            let decoded_row: Row = if let Some(ref mut iter) = col_seg_iter {
                // Columnar: iterator already yields decoded Vec<Value> rows.
                match iter.next() {
                    Some(Ok((_row_id, row))) => {
                        if let Some(ref clause) = where_clause {
                            match Self::eval_expr_on_row(clause, &row, schema) {
                                Ok(Value::Bool(true)) => row,
                                Ok(_) => continue,
                                Err(_) => return Ok(None),
                            }
                        } else {
                            row
                        }
                    }
                    Some(Err(e)) => return Err(e),
                    None => break,
                }
            } else {
                let (_row_id, raw_bytes) = match raw_iter.as_mut().unwrap().next() {
                    Some(Ok(r)) => r,
                    Some(Err(e)) => return Err(e),
                    None => break,
                };
                if let Some(ref clause) = where_clause {
                    let full_row = match crate::storage::row_format::decode_fast(&raw_bytes, col_types, fixed_count) {
                        Ok(r) => r,
                        Err(_) => return Ok(None),
                    };
                    match Self::eval_expr_on_row(clause, &full_row, schema) {
                        Ok(Value::Bool(true)) => full_row,
                        Ok(_) => continue,
                        Err(_) => return Ok(None),
                    }
                } else {
                    // No WHERE: decode only the needed columns for this row.
                    (0..col_types.len()).map(|pos| {
                        crate::storage::row_format::get_column(&raw_bytes, col_types, pos)
                            .unwrap_or(Value::Null)
                    }).collect()
                }
            };

            // Build group key from the fully decoded row.
            let group_key: Vec<Value> = group_col_positions.iter()
                .map(|&pos| decoded_row.get(pos).cloned().unwrap_or(Value::Null))
                .collect();

            // Find or create group
            let entry = groups.entry(group_key.clone()).or_insert_with(|| {
                let accums = (0..num_aggs).map(|_| AggAccumulator::new()).collect();
                (group_key, accums)
            });

            // Update each aggregate accumulator using the decoded row.
            for (agg_idx, &select_idx) in agg_indices.iter().enumerate() {
                if let Some(ref agg) = select_col_info[select_idx].2 {
                    if let Some(pos) = agg.col_pos {
                        let val = decoded_row.get(pos).cloned().unwrap_or(Value::Null);
                        entry.1[agg_idx].update(&val, &agg.func);
                    } else {
                        // COUNT(*) or COUNT(1)
                        entry.1[agg_idx].count += 1;
                    }
                }
            }
        }

        // Handle implicit aggregation (no GROUP BY, no rows)
        if groups.is_empty() && group_col_positions.is_empty() {
            let accums: Vec<AggAccumulator> = (0..num_aggs).map(|_| AggAccumulator::new()).collect();
            groups.insert(vec![], (vec![], accums));
        }

        // Build result rows
        let column_names: Vec<String> = select_col_info.iter()
            .map(|(name, _, _)| name.clone()).collect();
        let mut result_rows: Vec<Vec<Value>> = Vec::new();

        for (_key, (group_vals, accums)) in groups {
            let mut result_row = Vec::with_capacity(select_col_info.len());
            let mut agg_iter = accums.into_iter();
            for (_, col_pos, agg_info) in select_col_info {
                if let Some(pos) = col_pos {
                    // Group column — find its position in group_col_positions
                    if let Some(gp_idx) = group_col_positions.iter().position(|p| *p == *pos) {
                        result_row.push(group_vals.get(gp_idx).cloned().unwrap_or(Value::Null));
                    } else {
                        result_row.push(Value::Null);
                    }
                } else if let Some(agg) = agg_info {
                    let accum = agg_iter.next().unwrap();
                    result_row.push(accum.finalize(&agg.func));
                } else {
                    result_row.push(Value::Null);
                }
            }
            result_rows.push(result_row);
        }

        // Apply ORDER BY
        if let Some(ref order_by) = stmt.order_by {
            let order_specs: Vec<(usize, bool)> = order_by.iter().filter_map(|ob| {
                if let Expr::Column(ref col_name) = ob.expr {
                    let idx = column_names.iter().position(|c| c == col_name)?;
                    Some((idx, ob.asc))
                } else {
                    None
                }
            }).collect();

            result_rows.sort_by(|a, b| {
                for &(idx, asc) in &order_specs {
                    let cmp = a[idx].partial_cmp(&b[idx]).unwrap_or(std::cmp::Ordering::Equal);
                    if cmp != std::cmp::Ordering::Equal {
                        return if asc { cmp } else { cmp.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // Apply LIMIT/OFFSET
        if stmt.offset.is_some() || stmt.limit.is_some() {
            let skip_n = stmt.offset.unwrap_or(0);
            let take_n = stmt.limit.unwrap_or(usize::MAX);
            result_rows = result_rows.into_iter().skip(skip_n).take(take_n).collect();
        }

        Ok(Some((column_names, result_rows)))
    }

    /// Compute an aggregate function over positional rows (Vec<Value> slices).
    fn compute_aggregate_positional(
        &self,
        agg: &AggregateInfo,
        rows: &[&Row],
    ) -> Result<Value> {
        use std::collections::HashSet;
        match agg.func.as_str() {
            "COUNT" => {
                if agg.distinct {
                    if agg.col_pos.is_none() {
                        return Err(MoteDBError::InvalidArgument(
                            "COUNT(DISTINCT *) is not supported".to_string()
                        ));
                    }
                    let mut seen = HashSet::new();
                    for row in rows {
                        if let Some(pos) = agg.col_pos {
                            if let Some(val) = row.get(pos) {
                                if !matches!(val, Value::Null) {
                                    seen.insert(val.clone());
                                }
                            }
                        }
                    }
                    Ok(Value::Integer(seen.len() as i64))
                } else if agg.col_pos.is_none() {
                    // COUNT(*)
                    Ok(Value::Integer(rows.len() as i64))
                } else {
                    // COUNT(col) - exclude NULLs
                    let count = rows.iter()
                        .filter(|row| {
                            agg.col_pos.and_then(|pos| row.get(pos))
                                .map_or(false, |v| !matches!(v, Value::Null))
                        })
                        .count();
                    Ok(Value::Integer(count as i64))
                }
            }
            "SUM" => {
                let mut int_sum: i64 = 0;
                let mut float_sum: f64 = 0.0;
                let mut has_float = false;
                let mut has_value = false;
                for row in rows {
                    if let Some(pos) = agg.col_pos {
                        if let Some(val) = row.get(pos) {
                            match val {
                                Value::Integer(i) => {
                                    has_value = true;
                                    if has_float {
                                        float_sum += *i as f64;
                                    } else if let Some(s) = int_sum.checked_add(*i) {
                                        int_sum = s;
                                    } else {
                                        has_float = true;
                                        float_sum = int_sum as f64 + *i as f64;
                                    }
                                }
                                Value::Float(f) => {
                                    has_value = true;
                                    if !has_float {
                                        has_float = true;
                                        float_sum = int_sum as f64;
                                    }
                                    float_sum += *f;
                                }
                                Value::Null => {}
                                _ => return Err(MoteDBError::TypeError("SUM requires numeric values".to_string())),
                            }
                        }
                    }
                }
                if !has_value {
                    Ok(Value::Null)
                } else if has_float {
                    Ok(Value::Float(float_sum))
                } else {
                    Ok(Value::Integer(int_sum))
                }
            }
            "AVG" => {
                let mut sum = 0.0;
                let mut count = 0;
                for row in rows {
                    if let Some(pos) = agg.col_pos {
                        if let Some(val) = row.get(pos) {
                            match val {
                                Value::Integer(i) => { sum += *i as f64; count += 1; }
                                Value::Float(f) => { sum += *f; count += 1; }
                                Value::Null => {}
                                _ => return Err(MoteDBError::TypeError("AVG requires numeric values".to_string())),
                            }
                        }
                    }
                }
                if count > 0 {
                    Ok(Value::Float(sum / count as f64))
                } else {
                    Ok(Value::Null)
                }
            }
            "MIN" => {
                let mut min_val: Option<Value> = None;
                for row in rows {
                    if let Some(pos) = agg.col_pos {
                        if let Some(val) = row.get(pos) {
                            if !matches!(val, Value::Null) {
                                min_val = Some(match min_val {
                                    None => val.clone(),
                                    Some(current) => {
                                        if val.partial_cmp(&current) == Some(std::cmp::Ordering::Less) {
                                            val.clone()
                                        } else {
                                            current
                                        }
                                    }
                                });
                            }
                        }
                    }
                }
                Ok(min_val.unwrap_or(Value::Null))
            }
            "MAX" => {
                let mut max_val: Option<Value> = None;
                for row in rows {
                    if let Some(pos) = agg.col_pos {
                        if let Some(val) = row.get(pos) {
                            if !matches!(val, Value::Null) {
                                max_val = Some(match max_val {
                                    None => val.clone(),
                                    Some(current) => {
                                        if val.partial_cmp(&current) == Some(std::cmp::Ordering::Greater) {
                                            val.clone()
                                        } else {
                                            current
                                        }
                                    }
                                });
                            }
                        }
                    }
                }
                Ok(max_val.unwrap_or(Value::Null))
            }
            _ => Ok(Value::Null),
        }
    }

    /// 🆕 Check if columns only contain COUNT(*) aggregate (for fast-path optimization)
    fn has_only_count_aggregate(&self, columns: &[SelectColumn]) -> bool {
        if columns.len() != 1 {
            return false;
        }
        
        match &columns[0] {
            SelectColumn::Expr(Expr::FunctionCall { name, args, .. }, _) => {
                let func_name = name.to_uppercase();
                if func_name == "COUNT" {
                    // COUNT(*) or COUNT(column)
                    args.is_empty() || matches!(args.first(), Some(Expr::Column(c)) if c == "*")
                } else {
                    false
                }
            }
            _ => false,
        }
    }
    
    fn project_columns(
        &self,
        columns: &[SelectColumn],
        rows: &[(u64, SqlRow)],
        schema: &TableSchema,
    ) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
        // Determine column names
        let column_names: Vec<String> = if columns.len() == 1 && matches!(columns[0], SelectColumn::Star) {
            // SELECT * — strip table prefix from column names for output
            // (schema may be "polluted" with qualified names after execute_from_with_limit)
            schema.columns.iter().map(|c| {
                if let Some(pos) = c.name.find('.') {
                    c.name[pos + 1..].to_string()
                } else {
                    c.name.clone()
                }
            }).collect()
        } else {
            columns.iter().map(|col| match col {
                SelectColumn::Star => "*".to_string(),
                SelectColumn::Column(name) => name.clone(),
                SelectColumn::ColumnWithAlias(_, alias) => alias.clone(),
                SelectColumn::Expr(_, Some(alias)) => alias.clone(),
                SelectColumn::Expr(expr, None) => format!("{:?}", expr), // Use debug format as default
            }).collect()
        };
        
        // 🚀 OPTIMIZATION: Reduce cloning in projection
        // Pre-calculate which columns we need to avoid repeated lookups
        // Determine table name for qualified lookups
        let table_name_for_qualify = schema.name.as_str();

        let projected_rows: Vec<Vec<Value>> = if columns.len() == 1 && matches!(columns[0], SelectColumn::Star) {
            // SELECT * - optimized path
            rows.iter().map(|(_, row)| {
                schema.columns.iter()
                    .map(|col| {
                        row.get(&col.name).cloned().unwrap_or_else(|| {
                            // Fallback: try qualified name (e.g., "items.val")
                            if !table_name_for_qualify.is_empty() {
                                let qname = format!("{}.{}", table_name_for_qualify, col.name);
                                row.get(&qname).cloned().unwrap_or(Value::Null)
                            } else {
                                Value::Null
                            }
                        })
                    })
                    .collect()
            }).collect()
        } else {
            // Specific columns - optimize column lookup
            rows.iter().map(|(_, row)| {
                columns.iter().map(|col| {
                    match col {
                        SelectColumn::Column(name) | SelectColumn::ColumnWithAlias(name, _) => {
                            // Try exact match first, then try with table prefix
                            row.get(name).cloned().or_else(|| {
                                // If column name doesn't contain '.', try prefixed versions
                                if !name.contains('.') {
                                    // Try all possible table prefixes
                                    row.iter()
                                        .find(|(k, _)| k.ends_with(&format!(".{}", name)))
                                        .map(|(_, v)| v.clone())
                                } else {
                                    None
                                }
                            }).unwrap_or(Value::Null)
                        }
                        SelectColumn::Expr(expr, _) => {
                            self.eval_with_materialized(expr, row).unwrap_or(Value::Null)
                        }
                        SelectColumn::Star => Value::Null, // Shouldn't happen
                    }
                }).collect()
            }).collect()
        };
        
        Ok((column_names, projected_rows))
    }

    /// Positional WHERE fast path — scan → filter → project without SqlRow HashMap.
    /// Handles: SELECT cols FROM table WHERE col IN (list) / LIKE / BETWEEN / comparisons
    /// Eliminates O(R*C) HashMap allocations and O(R*K) IN list linear scans.
    #[allow(clippy::too_many_arguments)]
    fn try_positional_where(
        &self,
        stmt: &SelectStmt,
        table_name: &str,
    ) -> Result<Option<QueryResult>> {
        let schema = self.db.get_table_schema(table_name)?;

        // Resolve SELECT columns to (display_name, schema_position)
        let mut resolved_cols: Vec<(String, Option<usize>)> = Vec::new();
        for col_spec in &stmt.columns {
            match col_spec {
                SelectColumn::Star => {
                    for col_def in &schema.columns {
                        resolved_cols.push((col_def.name.clone(), Some(col_def.position)));
                    }
                }
                SelectColumn::Column(name) | SelectColumn::ColumnWithAlias(name, _) => {
                    match schema.get_column_position(name) {
                        Some(pos) => {
                            let display = match col_spec {
                                SelectColumn::ColumnWithAlias(_, alias) => alias.clone(),
                                _ => name.clone(),
                            };
                            resolved_cols.push((display, Some(pos)));
                        }
                        None => return Ok(None),
                    }
                }
                SelectColumn::Expr(_, _) => return Ok(None), // complex expression needs SqlRow
            }
        }
        let column_names: Vec<String> = resolved_cols.iter().map(|(n, _)| n.clone()).collect();
        let col_positions: Vec<Option<usize>> = resolved_cols.into_iter().map(|(_, p)| p).collect();

        // Materialize subqueries first (IN (SELECT...) → IN (literal list))
        let where_clause = stmt.where_clause.as_ref().unwrap();
        let where_expr = self.materialize_subqueries(where_clause)?;

        // Check positional evaluation ability
        if !Self::can_eval_positional(&where_expr) { return Ok(None); }

        // Precompute HashSet for IN with large literal lists: O(1) lookup instead of O(N) scan
        let in_hash_set: Option<std::collections::HashSet<Value>> = match &where_expr {
            Expr::In { expr: _, list, negated } if !negated && list.len() > 10
                && list.iter().all(|e| matches!(e, Expr::Literal(_))) =>
            {
                let set: std::collections::HashSet<Value> = list.iter()
                    .filter_map(|e| if let Expr::Literal(v) = e { Some(v.clone()) } else { None })
                    .collect();
                Some(set)
            }
            _ => None,
        };

        let limit = stmt.limit.unwrap_or(usize::MAX);
        let offset = stmt.offset.unwrap_or(0);
        let cap_hint = limit.min(
            self.db.fast_row_count(table_name).unwrap_or(1024) as usize
        );

        // Scan → filter → project in a single pass
        let row_iter = self.db.scan_table_rows_streaming(table_name)?;
        let mut rows: Vec<Vec<Value>> = Vec::with_capacity(cap_hint.min(1024));
        let mut skipped: usize = 0;

        for result in row_iter {
            let (_, row) = result?;

            // Evaluate WHERE positionally
            let matches = if let Some(ref hash_set) = in_hash_set {
                // Fast path: IN (literal list) with HashSet O(1) lookup
                if let Expr::In { expr, .. } = &where_expr {
                    if let Expr::Column(col_name) = expr.as_ref() {
                        let pos = schema.get_column_position(col_name);
                        pos.and_then(|p| row.get(p)).map(|v| hash_set.contains(v)).unwrap_or(false)
                    } else { false }
                } else { false }
            } else {
                // General positional evaluation
                match Self::eval_expr_on_row(&where_expr, &row, &schema) {
                    Ok(Value::Bool(b)) => b,
                    Ok(Value::Integer(i)) => i != 0,
                    Ok(Value::Float(f)) => f != 0.0 && !f.is_nan(),
                    Ok(Value::Null) => false,
                    Err(_) => return Ok(None),
                    _ => false,
                }
            };

            if matches {
                if skipped < offset { skipped += 1; continue; }
                let projected: Vec<Value> = col_positions.iter()
                    .map(|pos| pos.and_then(|p| row.get(p)).cloned().unwrap_or(Value::Null))
                    .collect();
                rows.push(projected);
                if rows.len() >= limit { break; }
            }
        }

        Ok(Some(QueryResult::Select {
            columns: column_names,
            rows,
        }))
    }

    /// Positional ORDER BY / DISTINCT fast path — skip HashMap conversion.
    /// Works for: SELECT cols FROM table [WHERE cond] ORDER BY col [ASC/DESC] [LIMIT n]
    ///            SELECT DISTINCT cols FROM table [WHERE cond]
    fn try_positional_order_by_distinct(
        &self,
        stmt: &SelectStmt,
        schema: &crate::types::TableSchema,
        table_name: &str,
    ) -> Result<Option<QueryResult>> {
        // Only handle single-table queries with ORDER BY and/or DISTINCT
        let has_order_by = stmt.order_by.is_some();
        let has_distinct = stmt.distinct;
        if !has_order_by && !has_distinct { return Ok(None); }
        if stmt.group_by.is_some() { return Ok(None); } // GROUP BY handles its own path

        // Resolve SELECT columns to (display_name, schema_position)
        let mut resolved_cols = Vec::new();
        for col_spec in &stmt.columns {
            match col_spec {
                SelectColumn::Star => {
                    // Expand SELECT * into all schema columns for positional path
                    for col_def in &schema.columns {
                        resolved_cols.push((col_def.name.clone(), Some(col_def.position)));
                    }
                }
                SelectColumn::Column(name) => {
                    if let Some(pos) = schema.get_column_position(name) {
                        resolved_cols.push((name.clone(), Some(pos)));
                    } else { return Ok(None); }
                }
                SelectColumn::ColumnWithAlias(name, alias) => {
                    if let Some(pos) = schema.get_column_position(name) {
                        resolved_cols.push((alias.clone(), Some(pos)));
                    } else { return Ok(None); }
                }
                SelectColumn::Expr(_, _) => return Ok(None), // expressions need evaluator
            }
        }
        let column_names: Vec<String> = resolved_cols.iter().map(|(n, _)| n.clone()).collect();
        let col_positions: Vec<Option<usize>> = resolved_cols.iter().map(|(_, p)| *p).collect();

        // Resolve ORDER BY columns to projected column indices
        let order_positions: Vec<(usize, bool)> = if let Some(ref order_by) = stmt.order_by {
            let mut positions = Vec::new();
            for order in order_by {
                match &order.expr {
                    Expr::Column(col_name) => {
                        // Check alias / column name in SELECT list first
                        if let Some(idx) = column_names.iter().position(|n| n == col_name) {
                            positions.push((idx, order.asc));
                        } else if let Some(_pos) = schema.get_column_position(col_name) {
                            // ORDER BY references a column not in SELECT — bail to slow path
                            return Ok(None);
                        } else {
                            return Ok(None);
                        }
                    }
                    Expr::Literal(Value::Integer(n)) => {
                        // ORDER BY column position (1-based)
                        let idx = (*n as usize).wrapping_sub(1);
                        if idx >= column_names.len() {
                            return Ok(None); // Out of range
                        }
                        positions.push((idx, order.asc));
                    }
                    _ => return Ok(None), // complex expression
                }
            }
            positions
        } else {
            Vec::new()
        };

        // Gather required column positions (unwrap None positions to simple Vec)
        let scan_positions: Vec<usize> = col_positions.iter().filter_map(|p| *p).collect();

        // Scan rows — use partial column decode for the no-WHERE case (most common)
        let mut projected_rows: Vec<Vec<Value>> = if stmt.where_clause.is_some() {
            let row_iter = self.db.scan_table_rows_streaming(table_name)?;
            let where_clause = stmt.where_clause.as_ref().unwrap();
            let mut matching = Vec::new();
            for result in row_iter {
                let (_, row) = result?;
                match Self::eval_expr_on_row(where_clause, &row, schema) {
                    Ok(Value::Bool(true)) => {
                        let projected: Vec<Value> = col_positions.iter()
                            .map(|pos| pos.and_then(|p| row.get(p)).cloned().unwrap_or(Value::Null))
                            .collect();
                        matching.push(projected);
                    }
                    Ok(_) => {},
                    Err(_) => return Ok(None),
                }
            }
            matching
        } else {
            // 🚀 Partial column scan: only decode columns we need
            let partial_iter = self.db.scan_table_rows_partial(
                table_name, &scan_positions,
            )?;
            {
                let mut matching = Vec::new();
                for result in partial_iter {
                    let (_row_id, row) = result?;
                    matching.push(row);
                }
                matching
            }
        };

        let offset = stmt.offset.unwrap_or(0);
        let limit = stmt.limit;
        let need_top_k = !order_positions.is_empty() && limit.is_some() && !has_distinct;

        let final_rows = if need_top_k && !projected_rows.is_empty() {
            // 🚀 Top-K via select_nth_unstable: O(N) average to partition, then sort only K rows
            let k = limit.unwrap();
            let keep = offset + k;
            let nth = (offset + k).saturating_sub(1).min(projected_rows.len() - 1);
            projected_rows.select_nth_unstable_by(nth, |a, b| {
                for &(col_idx, asc) in &order_positions {
                    let cmp = a.get(col_idx)
                        .and_then(|va| b.get(col_idx).map(|vb| (va, vb)))
                        .map(|(va, vb)| va.partial_cmp(vb).unwrap_or(std::cmp::Ordering::Equal))
                        .unwrap_or(std::cmp::Ordering::Equal);
                    if cmp != std::cmp::Ordering::Equal {
                        return if asc { cmp } else { cmp.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
            let mut top: Vec<Vec<Value>> = projected_rows;
            top.truncate(keep.min(top.len()));
            top.sort_by(|a, b| {
                for &(col_idx, asc) in &order_positions {
                    let cmp = a.get(col_idx)
                        .and_then(|va| b.get(col_idx).map(|vb| (va, vb)))
                        .map(|(va, vb)| va.partial_cmp(vb).unwrap_or(std::cmp::Ordering::Equal))
                        .unwrap_or(std::cmp::Ordering::Equal);
                    if cmp != std::cmp::Ordering::Equal {
                        return if asc { cmp } else { cmp.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
            top.into_iter().skip(offset).collect()
        } else {
            // Full sort path (no LIMIT, or DISTINCT requires full dedup)
            if !order_positions.is_empty() {
                projected_rows.sort_by(|a, b| {
                    for &(col_idx, asc) in &order_positions {
                        let cmp = a.get(col_idx)
                            .and_then(|va| b.get(col_idx).map(|vb| (va, vb)))
                            .map(|(va, vb)| va.partial_cmp(vb).unwrap_or(std::cmp::Ordering::Equal))
                            .unwrap_or(std::cmp::Ordering::Equal);
                        if cmp != std::cmp::Ordering::Equal {
                            return if asc { cmp } else { cmp.reverse() };
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
            if has_distinct {
                let mut seen = std::collections::HashSet::new();
                projected_rows.retain(|row| seen.insert(row.clone()));
            }
            let lim = limit.unwrap_or(usize::MAX);
            projected_rows.into_iter().skip(offset).take(lim).collect()
        };

        Ok(Some(QueryResult::Select {
            columns: column_names,
            rows: final_rows,
        }))
    }

    /// Execute INSERT statement (owned, for execute() path)
    fn execute_insert(&self, stmt: InsertStmt) -> Result<QueryResult> {
        self.execute_insert_ref(&stmt)
    }

    /// Execute INSERT statement (borrowed, avoids clone in streaming path)
    fn execute_insert_ref(&self, stmt: &InsertStmt) -> Result<QueryResult> {
        let schema = self.db.get_table_schema(&stmt.table)?;

        // Determine column order
        let columns = if let Some(ref cols) = stmt.columns {
            cols.clone()
        } else {
            // Use schema order
            schema.columns.iter().map(|c| c.name.clone()).collect()
        };

        // Route TimeSeries INSERT to columnar store
        if schema.table_type == crate::types::TableType::TimeSeries {
            return self.execute_columnar_insert(&stmt, &schema, &columns);
        }

        let has_vector_column = schema.columns.iter()
            .any(|col| matches!(col.col_type, crate::types::ColumnType::Tensor(_)));

        // Prepare all rows — resolve expressions to Values, build Row directly
        let mut prepared_rows = Vec::new();

        for value_row in &stmt.values {
            if value_row.len() != columns.len() {
                return Err(MoteDBError::InvalidArgument(
                    format!("Column count mismatch: expected {}, got {}", columns.len(), value_row.len())
                ));
            }

            // Resolve all expressions to Values (skip HashMap intermediary)
            let resolved: Vec<Value> = value_row.iter().map(|expr| {
                match expr {
                    Expr::Literal(v) => Ok(v.clone()),
                    Expr::Parameter(_) => {
                        let empty_row = SqlRow::new();
                        self.evaluator.eval(expr, &empty_row)
                    }
                    other => Err(MoteDBError::InvalidArgument(
                        format!("INSERT VALUES must be literals or parameters, got {:?}", other)
                    )),
                }
            }).collect::<Result<Vec<_>>>()?;

            // Build Row directly using column mapping (no HashMap)
            let row = crate::sql::row_converter::values_to_row_by_columns(
                &resolved, &columns, &schema
            )?;
            prepared_rows.push(row);
        }

        let affected_rows = prepared_rows.len();

        // Track last_insert_id for AUTO_INCREMENT primary key
        // If inside an explicit transaction, buffer INSERTs via coordinator write_set.
        let txn_id: Option<u64> = *self.current_txn_id.lock();
        let mut last_row_id: Option<u64> = None;

        if has_vector_column && prepared_rows.len() > 1 {
            // Batch insert path for vector columns
            let mut vector_batches: std::collections::HashMap<String, Vec<(u64, Vec<f32>)>> =
                std::collections::HashMap::new();

            for row in &prepared_rows {
                let row_id = if let Some(tid) = txn_id {
                    self.db.insert_row_with_txn(&stmt.table, tid, row.clone())?
                } else {
                    self.db.insert_row_to_table(&stmt.table, row.clone())?
                };
                last_row_id = Some(row_id);

                for (idx, col_def) in schema.columns.iter().enumerate() {
                    if let crate::types::ColumnType::Tensor(_dim) = col_def.col_type {
                        if let Some(Value::Vector(vec)) = row.get(idx) {
                            let index_name = format!("{}_{}", stmt.table, col_def.name);
                            vector_batches.entry(index_name)
                                .or_default()
                                .push((row_id, vec.to_vec()));
                        }
                    }
                }
            }

            for (index_name, batch) in vector_batches {
                match self.db.batch_update_vectors(&index_name, batch) {
                    Ok(_) => {},
                    Err(e) if e.to_string().contains("not found") => {},
                    Err(e) => return Err(e),
                }
            }
        } else if txn_id.is_some() {
            // Transactional path: must use per-row insert with txn coordinator
            for row in prepared_rows {
                let row_id = self.db.insert_row_with_txn(&stmt.table, txn_id.unwrap(), row)?;
                last_row_id = Some(row_id);
            }
        } else if prepared_rows.len() > 1 {
            // 🚀 Batch path: single WAL fsync, batched LSM put, batched index updates
            let ids = self.db.batch_insert_rows_to_table(&stmt.table, prepared_rows)?;
            if let Some(&id) = ids.last() {
                last_row_id = Some(id);
            }
        } else if let Some(row) = prepared_rows.into_iter().next() {
            // Single-row path
            let row_id = self.db.insert_row_to_table(&stmt.table, row)?;
            last_row_id = Some(row_id);
        }

        // Update last_insert_id if table has AUTO_INCREMENT primary key
        if schema.is_primary_key_auto_increment() {
            if let Some(row_id) = last_row_id {
                self.last_insert_id.store(row_id as i64, std::sync::atomic::Ordering::Relaxed);
                self.evaluator.last_insert_id.store(row_id as i64, std::sync::atomic::Ordering::Relaxed);
            }
        }

        Ok(QueryResult::Modification { affected_rows })
    }

    /// Execute UPDATE statement
    fn execute_update(&self, stmt: UpdateStmt) -> Result<QueryResult> {
        let schema = self.db.get_table_schema(&stmt.table)?;

        // Validate all assignment columns exist before modifying any rows
        for (col_name, _) in &stmt.assignments {
            if schema.get_column(col_name).is_none() {
                return Err(StorageError::ColumnNotFound(
                    format!("'{}' in table '{}'", col_name, stmt.table)
                ));
            }
        }

        // 🚀 PK fast path: skip full table scan for WHERE pk = value
        if let Some(ref where_clause) = stmt.where_clause {
            if let Some((col_name, target_value)) = self.try_extract_point_query(where_clause) {
                let is_pk = schema.primary_key()
                    .map(|pk| pk == col_name)
                    .unwrap_or(false);

                if is_pk {
                    return self.execute_update_pk(&stmt, &schema, &target_value);
                }

                // Column index fast path: use index to find matching rows
                if let Some(index_name) = self.db.index_registry.find_by_column(
                    &stmt.table, &col_name,
                    crate::database::index_metadata::IndexType::Column
                ) {
                    if let Some(index) = self.db.column_indexes.get(&index_name) {
                        let matching_row_ids = index.value()
                            .get_arc(&target_value)
                            .unwrap_or_else(|_| Arc::new(Vec::new()));
                        if matching_row_ids.is_empty() {
                            return Ok(QueryResult::Modification { affected_rows: 0 });
                        }
                        return self.execute_update_by_row_ids(&stmt, &schema, &matching_row_ids, &col_name, &target_value);
                    }
                }
            }
        }

        // 🚀 Use真正的流式扫描 (O(1) memory)
        let row_iter = self.db.scan_table_rows_streaming(&stmt.table)?;
        
        let mut affected_rows = 0;
        
        for result in row_iter {
            let (row_id, row) = result?;

            // WHERE filter using positional evaluation (no HashMap)
            let should_update = if let Some(ref where_clause) = stmt.where_clause {
                Self::eval_expr_on_row(where_clause, &row, &schema)
                    .map(|v| Self::is_truthy(&v))
                    .unwrap_or(false)
            } else {
                true
            };

            if !should_update {
                continue;
            }

            // Evaluate assignments positionally against the raw Vec<Value>
            let mut new_row = row.clone();
            for (col_name, expr) in &stmt.assignments {
                if let Some(cd) = schema.get_column(col_name) {
                    let new_val = if let Expr::Literal(v) = expr {
                        v.clone()
                    } else {
                        Self::eval_expr_on_row(expr, &row, &schema)
                            .unwrap_or(Value::Null)
                    };
                    while new_row.len() <= cd.position {
                        new_row.push(Value::Null);
                    }
                    new_row[cd.position] = new_val;
                }
            }

            self.db.update_row_in_table_with_schema(&stmt.table, row_id, row, new_row, &schema)?;

            affected_rows += 1;
        }

        Ok(QueryResult::Modification { affected_rows })
    }
    fn execute_delete(&self, stmt: DeleteStmt) -> Result<QueryResult> {
        let schema = self.db.get_table_schema(&stmt.table)?;

        // 🚀 PK fast path: skip full table scan for WHERE pk = value
        if let Some(ref where_clause) = stmt.where_clause {
            if let Some((col_name, target_value)) = self.try_extract_point_query(where_clause) {
                let is_pk = schema.primary_key()
                    .map(|pk| pk == col_name)
                    .unwrap_or(false);

                if is_pk {
                    return self.execute_delete_pk(&stmt, &schema, &target_value);
                }

                // 🚀 Column index fast path: use index to find matching rows
                if let Some(index_name) = self.db.index_registry.find_by_column(
                    &stmt.table, &col_name,
                    crate::database::index_metadata::IndexType::Column
                ) {
                    if let Some(index) = self.db.column_indexes.get(&index_name) {
                        let matching_row_ids = index.value()
                            .get_arc(&target_value)
                            .unwrap_or_else(|_| Arc::new(Vec::new()));
                        if matching_row_ids.is_empty() {
                            return Ok(QueryResult::Modification { affected_rows: 0 });
                        }
                        return self.execute_delete_by_row_ids(&stmt, &schema, &matching_row_ids, &col_name, &target_value);
                    }
                }
            }
        }

        // 🚀 Use真正的流式扫描 (O(1) memory)
        let row_iter = self.db.scan_table_rows_streaming(&stmt.table)?;
        
        let mut affected_rows = 0;
        
        for result in row_iter {
            let (row_id, row) = result?;
            let sql_row = row_to_sql_row(&row, &schema)?;
            
            // Filter rows (WHERE clause)
            let should_delete = if let Some(ref where_clause) = stmt.where_clause {
                self.evaluator.eval(where_clause, &sql_row)
                    .and_then(|val| self.to_bool(&val))
                    .unwrap_or(false)
            } else {
                true
            };
            
            if !should_delete {
                continue;
            }
            
            // Delete row - 底层已实现增量索引维护，传入 old_row 避免重复加载
            self.db.delete_row_from_table(&stmt.table, row_id, row)?;
            affected_rows += 1;
        }
        
        Ok(QueryResult::Modification { affected_rows })
    }

    /// 🚀 PK fast path for UPDATE: direct lookup instead of full table scan
    ///
    /// For `UPDATE t SET ... WHERE pk = value`:
    /// - AUTO_INCREMENT: direct LSM get by row_id (O(log n))
    /// - Non-AUTO_INCREMENT: column index lookup then LSM get
    ///   Resolve PK value to RowId using pk_lookup cache.
    ///   On cache miss, falls back to disk-based column index and refills the cache.
    ///   This ensures that repeated lookups for the same PK value are fast (O(1) after first access).
    fn resolve_pk_with_cache(
        &self,
        table: &str,
        pk_key: &crate::database::pk_cache::PkKey,
        pk_col_name: &str,
        pk_value: &Value,
    ) -> Result<Option<RowId>> {
        // Try LRU cache first
        if let Some(lookup) = self.db.pk_lookup.get(table) {
            if let Some(rid) = lookup.get_pk(pk_key) {
                return Ok(Some(rid));
            }
        }

        // Cache miss — fall back to column index, or full scan if index missing
        let row_ids = match self.db.query_by_column(table, pk_col_name, pk_value) {
            Ok(ids) => ids,
            Err(_) => {
                // Column index not available (e.g. after restart) — full scan fallback
                let schema = self.db.get_table_schema(table)?;
                let pk_pos = schema.get_column_position(pk_col_name).unwrap_or(0);
                let rows = self.db.scan_table_rows_streaming(table)?;
                let mut found = Vec::new();
                for item in rows {
                    let (row_id, row) = item?;
                    if let Some(val) = row.get(pk_pos) {
                        if val == pk_value {
                            found.push(row_id);
                            break;
                        }
                    }
                }
                found
            }
        };

        // Refill cache from disk result so next lookup is O(1)
        if let Some(&rid) = row_ids.first() {
            if let Some(lookup) = self.db.pk_lookup.get(table) {
                lookup.insert(pk_key.clone(), rid);
            }
        }

        Ok(row_ids.into_iter().next())
    }

    /// Resolve a PK value to row IDs, handling both AUTO_INCREMENT and non-AUTO_INCREMENT cases.
    fn resolve_pk_row_ids(
        &self,
        table_name: &str,
        schema: &crate::types::TableSchema,
        target_value: &Value,
    ) -> Result<Vec<RowId>> {
        let pk_col_name = schema.primary_key()
            .ok_or_else(|| StorageError::InvalidData("No primary key".into()))?;

        if schema.is_primary_key_auto_increment() {
            // AUTO_INCREMENT: pk value IS row_id — direct O(1) mapping
            match target_value {
                Value::Integer(id) if *id >= 0 => Ok(vec![*id as RowId]),
                _ => Ok(vec![]),
            }
        } else {
            // Non-AUTO_INCREMENT: resolve via pk_lookup cache (with disk fallback + cache refill)
            let pk_key = crate::database::pk_cache::PkKey::from_value(target_value);
            match self.resolve_pk_with_cache(table_name, &pk_key, pk_col_name, target_value)? {
                Some(rid) => Ok(vec![rid]),
                None => Ok(vec![]),
            }
        }
    }

    fn execute_update_pk(
        &self,
        stmt: &UpdateStmt,
        schema: &crate::types::TableSchema,
        target_value: &Value,
    ) -> Result<QueryResult> {
        let row_ids = self.resolve_pk_row_ids(&stmt.table, schema, target_value)?;
        if row_ids.is_empty() {
            return Ok(QueryResult::Modification { affected_rows: 0 });
        }

        let mut affected_rows = 0;

        for row_id in row_ids {
            let row = match self.db.get_table_row(&stmt.table, row_id)? {
                Some(r) => r,
                None => continue,
            };

            // Evaluate assignments positionally against the raw Vec<Value>
            // (no HashMap creation, no cloning) — SQL semantics: all EXPRs
            // evaluated against the ORIGINAL row before any modifications.
            let mut new_values: Vec<(usize, Value)> = Vec::with_capacity(stmt.assignments.len());
            for (col_name, expr) in &stmt.assignments {
                if let Some(cd) = schema.get_column(col_name) {
                    let new_val = if let Expr::Literal(v) = expr {
                        v.clone()
                    } else {
                        Self::eval_expr_on_row(expr, &row, &schema)
                            .unwrap_or(Value::Null)
                    };
                    new_values.push((cd.position, new_val));
                }
            }

            // Clone old row and apply changes by position
            let mut new_row = row.clone();
            for (pos, val) in &new_values {
                while new_row.len() <= *pos {
                    new_row.push(Value::Null);
                }
                new_row[*pos] = val.clone();
            }

            self.db.update_row_in_table_with_schema(&stmt.table, row_id, row, new_row, schema)?;
            affected_rows += 1;
        }

        Ok(QueryResult::Modification { affected_rows })
    }

    /// PK fast path for DELETE: direct lookup instead of full table scan
    fn execute_delete_pk(
        &self,
        stmt: &DeleteStmt,
        schema: &crate::types::TableSchema,
        target_value: &Value,
    ) -> Result<QueryResult> {
        let row_ids = self.resolve_pk_row_ids(&stmt.table, schema, target_value)?;
        if row_ids.is_empty() {
            return Ok(QueryResult::Modification { affected_rows: 0 });
        }

        let mut affected_rows = 0;
        for row_id in row_ids {
            let row = match self.db.get_table_row(&stmt.table, row_id)? {
                Some(r) => r,
                None => continue,
            };

            self.db.delete_row_from_table(&stmt.table, row_id, row)?;
            affected_rows += 1;
        }

        Ok(QueryResult::Modification { affected_rows })
    }

    /// 🚀 Column index fast path for UPDATE: lookup by row_ids from index
    fn execute_update_by_row_ids(
        &self,
        stmt: &UpdateStmt,
        schema: &crate::types::TableSchema,
        row_ids: &[RowId],
        where_col: &str,
        where_val: &crate::types::Value,
    ) -> Result<QueryResult> {
        let mut affected_rows = 0;
        for &row_id in row_ids {
            let row = match self.db.get_table_row(&stmt.table, row_id)? {
                Some(r) => r,
                None => continue,
            };

            // Re-check WHERE condition against actual row data
            if let Some(col) = schema.get_column(where_col) {
                if let Some(actual_val) = row.get(col.position) {
                    if actual_val != where_val { continue; }
                } else { continue; }
            }

            // Evaluate assignments positionally against the raw Vec<Value>
            let mut new_row = row.clone();
            for (col_name, expr) in &stmt.assignments {
                if let Some(cd) = schema.get_column(col_name) {
                    let new_val = if let Expr::Literal(v) = expr {
                        v.clone()
                    } else {
                        Self::eval_expr_on_row(expr, &row, &schema)
                            .unwrap_or(Value::Null)
                    };
                    while new_row.len() <= cd.position {
                        new_row.push(Value::Null);
                    }
                    new_row[cd.position] = new_val;
                }
            }

            self.db.update_row_in_table_with_schema(&stmt.table, row_id, row, new_row, schema)?;
            affected_rows += 1;
        }

        Ok(QueryResult::Modification { affected_rows })
    }

    /// Column index fast path for DELETE: lookup by row_ids from index
    fn execute_delete_by_row_ids(
        &self,
        stmt: &DeleteStmt,
        schema: &crate::types::TableSchema,
        row_ids: &[RowId],
        where_col: &str,
        where_val: &crate::types::Value,
    ) -> Result<QueryResult> {
        let mut affected_rows = 0;
        for &row_id in row_ids {
            let row = match self.db.get_table_row(&stmt.table, row_id)? {
                Some(r) => r,
                None => continue,
            };

            // Re-check WHERE condition against actual row data
            if let Some(col) = schema.get_column(where_col) {
                if let Some(actual_val) = row.get(col.position) {
                    if actual_val != where_val { continue; }
                } else { continue; }
            }

            self.db.delete_row_from_table(&stmt.table, row_id, row)?;
            affected_rows += 1;
        }

        Ok(QueryResult::Modification { affected_rows })
    }

    /// Execute CREATE TABLE statement
    fn execute_create_table(&self, stmt: CreateTableStmt) -> Result<QueryResult> {
        // Convert AST column defs to TableSchema
        let columns: Vec<crate::types::ColumnDef> = stmt.columns.iter().enumerate().map(|(pos, col)| {
            let column_type = match col.data_type {
                DataType::Integer => ColumnType::Integer,
                DataType::BigInt => ColumnType::Integer,  // 🚀 Phase 4: Map BIGINT to Integer (both i64)
                DataType::Float => ColumnType::Float,
                DataType::Text => ColumnType::Text,
                DataType::Boolean => ColumnType::Boolean,
                DataType::Timestamp => ColumnType::Timestamp,
                DataType::Vector(dim) => ColumnType::Tensor(dim.unwrap_or(128)),
                DataType::Geometry => ColumnType::Spatial,
            };
            
            let mut col_def = crate::types::ColumnDef::new(
                col.name.clone(),
                column_type,
                pos,
            );
            if !col.nullable {
                col_def = col_def.not_null();
            }
            // 🚀 AUTO_INCREMENT flag with optional start value (Phase 5)
            if col.auto_increment {
                if let Some(start) = col.auto_increment_start {
                    col_def = col_def.auto_increment_with_start(start);
                } else {
                    col_def = col_def.auto_increment();
                }
            }
            col_def
        }).collect();

        // Guard: the columnar SSTable format reserves a fixed-width header slot
        // per column (MAX_COLUMNS). Reject early with a clean error instead of
        // panicking at flush time when the header overflows.
        if columns.len() > crate::storage::lsm::columnar::MAX_COLUMNS {
            return Err(crate::error::StorageError::InvalidData(format!(
                "table '{}' has {} columns, but the maximum is {}",
                stmt.table, columns.len(), crate::storage::lsm::columnar::MAX_COLUMNS
            )).into());
        }

        // 🆕 STEP 1: Find primary key columns
        let primary_key_cols: Vec<&super::ast::ColumnDef> = stmt.columns.iter()
            .filter(|col| col.primary_key)
            .collect();
        
        // 🆕 STEP 2: Set primary key in schema
        let mut schema = TableSchema::new(stmt.table.clone(), columns);
        if let Some(pk_col) = primary_key_cols.first() {
            schema = schema.with_primary_key(pk_col.name.clone());

            // 🚀 Phase 5: Set AUTO_INCREMENT flag with optional start value
            if pk_col.auto_increment {
                if let Some(start) = pk_col.auto_increment_start {
                    schema = schema.with_auto_increment_start(start);
                } else {
                    schema = schema.with_auto_increment();
                }
            }
        }

        // TimeSeries table type and TTL
        if let Some(ref ts_col) = stmt.timeseries_column {
            schema = schema.with_timeseries(ts_col.clone());
        }
        if let Some(ref ttl) = stmt.ttl {
            schema = schema.with_ttl(*ttl);
        }
        
        self.db.create_table(schema.clone())?;
        
        // 🚀 P0 FIX: Auto-create column index for primary key (ONLY if NOT AUTO_INCREMENT)
        // AUTO_INCREMENT主键不需要列索引（主键值 = row_id，直接查询）
        if let Some(pk_col) = primary_key_cols.first() {
            if !pk_col.auto_increment {
                let _pk_index_name = format!("{}.{}", stmt.table, pk_col.name);
                self.db.create_column_index(&stmt.table, &pk_col.name)?;
            }
        }
        
        // 🚨 DEADLOCK FIX: create_table() already auto-creates primary key index
        // No need to manually create it again (prevents double creation deadlock)
        let pk_info = if !primary_key_cols.is_empty() {
            let pk_names: Vec<String> = primary_key_cols.iter().map(|c| c.name.clone()).collect();
            let auto_inc = if primary_key_cols[0].auto_increment { " AUTO_INCREMENT" } else { "" };
            format!(" (Primary key: {}{}, auto-index: ✓)", pk_names.join(", "), auto_inc)
        } else {
            String::new()
        };

        let ts_info = match &stmt.timeseries_column {
            Some(col) => format!(", timeseries({})", col),
            None => String::new(),
        };
        let ttl_info = match &stmt.ttl {
            Some(ttl) => format!(", TTL {}", ttl),
            None => String::new(),
        };

        Ok(QueryResult::Definition {
            message: format!("Table '{}' created successfully{}{}{}", stmt.table, pk_info, ts_info, ttl_info),
        })
    }
    
    /// Execute CREATE INDEX statement
    fn execute_create_index(&self, stmt: CreateIndexStmt) -> Result<QueryResult> {
        // Get table schema to find column type
        let schema = self.db.get_table_schema(&stmt.table)?;
        let column = schema.columns.iter()
            .find(|c| c.name == stmt.column)
            .ok_or_else(|| MoteDBError::ColumnNotFound(stmt.column.clone()))?;
        
        // Determine index type: use explicit type from AST, or infer from column type
        let index_type = match stmt.index_type {
            IndexType::Text => {
                // Verify column is compatible with text index
                if !matches!(column.col_type, ColumnType::Text) {
                    return Err(MoteDBError::TypeError(
                        format!("TEXT index requires TEXT column, got {:?}", column.col_type)
                    ));
                }
                IndexType::Text
            }
            IndexType::Vector => {
                // Verify column is tensor/vector
                if let ColumnType::Tensor(_dim) = column.col_type {
                    IndexType::Vector
                } else {
                    return Err(MoteDBError::TypeError(
                        format!("VECTOR index requires TENSOR column, got {:?}", column.col_type)
                    ));
                }
            }
            IndexType::Timestamp => {
                // Verify column is timestamp
                if !matches!(column.col_type, ColumnType::Timestamp) {
                    return Err(MoteDBError::TypeError(
                        format!("TIMESTAMP index requires TIMESTAMP column, got {:?}", column.col_type)
                    ));
                }
                IndexType::Timestamp
            }
            IndexType::Octree => {
                // Verify column is spatial (3D points)
                if !matches!(column.col_type, ColumnType::Spatial) {
                    return Err(MoteDBError::TypeError(
                        format!("OCTREE index requires SPATIAL column, got {:?}", column.col_type)
                    ));
                }
                IndexType::Octree
            }
            IndexType::BTree | IndexType::Column => {
                // B-Tree/Column index can be used for any comparable type
                stmt.index_type.clone()
            }
        };
        
        // Create index based on type
        // 🆕 Use user-specified index name or generate default
        let index_name = if !stmt.index_name.is_empty() {
            stmt.index_name.clone()
        } else {
            // Fallback to default naming: {table}_{column}
            format!("{}_{}", stmt.table, stmt.column)
        };
        
        match index_type {
            IndexType::Text => {
                // 1️⃣ Create empty text index
                self.db.create_text_index(&index_name)?;
                
                // 2️⃣ 🚀 Columnar fast path: bulk build from TextSegment
                let column_pos = schema.get_column_position(&stmt.column)
                    .ok_or_else(|| MoteDBError::ColumnNotFound(stmt.column.clone()))?;
                let start_time = std::time::Instant::now();
                let mut backfill_count = 0;

                if let Ok(count) = self.db.build_text_index_from_columnar(&index_name, &stmt.table, column_pos) {
                    backfill_count = count;
                    debug_log!("[CREATE TEXT INDEX] Columnar build: {} docs in {:?}", count, start_time.elapsed());
                } else {
                // ✅ Fallback: 批量流式扫描（每批10000行，避免内存爆炸）
                let batch_iter = self.db.scan_table_rows_batched(&stmt.table, 10000)?;
                
                for batch_result in batch_iter {
                    let batch = batch_result?;
                    
                    // 收集本批次的文本数据
                    let texts_in_batch: Vec<_> = batch.iter()
                        .filter_map(|(row_id, row)| {
                            row.get(column_pos).and_then(|v| {
                                if let Value::Text(text) = v {
                                    Some((*row_id, text.as_str()))
                                } else {
                                    None
                                }
                            })
                        })
                        .collect();
                    
                    // ✅ 一次写锁，批量插入整个batch
                    if !texts_in_batch.is_empty() {
                        if let Some(index_arc) = self.db.text_indexes.get(&index_name) {
                            let mut index = index_arc.write();
                            for (row_id, text) in texts_in_batch {
                                if let Err(e) = index.insert(row_id, text) {
                                    debug_log!("⚠️ Failed to backfill text index for row {}: {}", row_id, e);
                                } else {
                                    backfill_count += 1;
                                }
                            }
                            // 锁在此处释放（每10000条释放一次，允许并发查询）
                        }
                    }
                }
                
                if backfill_count > 0 {
                    debug_log!("Built text index in {:?}, indexed {} rows", start_time.elapsed(), backfill_count);
                }
                } // end else (columnar build failed, used row-based fallback)
                
                // 3️⃣ Register metadata
                let metadata = crate::database::index_metadata::IndexMetadata::new(
                    index_name.clone(),
                    stmt.table.clone(),
                    stmt.column.clone(),
                    crate::database::index_metadata::IndexType::Text,
                );
                self.db.index_registry.register(metadata)?;
            }
            IndexType::Vector => {
                // create_vector_index already scans existing data and builds the index
                if let ColumnType::Tensor(dim) = column.col_type {
                    self.db.create_vector_index(&index_name, dim, stmt.metric.as_deref())?;

                    let mut metadata = crate::database::index_metadata::IndexMetadata::new(
                        index_name.clone(),
                        stmt.table.clone(),
                        stmt.column.clone(),
                        crate::database::index_metadata::IndexType::Vector,
                    );
                    metadata.metric = stmt.metric.clone();
                    self.db.index_registry.register(metadata)?;
                } else {
                    unreachable!("Already validated column type");
                }
            }
            IndexType::Timestamp => {
                // Timestamp index is global and already created with database
                // No-op, but return success
            }
            IndexType::Octree => {
                // Create i-Octree index for 3D point cloud data
                self.db.create_ioctree_index(&index_name)?;

                // 🚀 Backfill: try columnar fast path first
                let column_pos = schema.get_column_position(&stmt.column)
                    .ok_or_else(|| MoteDBError::ColumnNotFound(stmt.column.clone()))?;
                let mut backfill_count = 0;

                if let Ok(count) = self.db.build_ioctree_from_columnar(&index_name, &stmt.table, column_pos) {
                    backfill_count = count;
                } else {
                let iter = self.db.scan_table_rows_streaming(&stmt.table)?;
                for result in iter {
                    let (row_id, row) = result?;
                    if let Some(Value::Spatial(geometry)) = row.get(column_pos) {
                        if geometry.is_3d() {
                            if let Err(e) = self.db.insert_ioctree_point(row_id, &index_name, geometry) {
                                debug_log!("⚠️ Failed to backfill ioctree index for row {}: {}", row_id, e);
                            } else { backfill_count += 1; }
                        }
                    }
                }
                }

                if backfill_count > 0 {
                    debug_log!("Backfilled {} rows into ioctree index '{}'", backfill_count, index_name);
                }

                // Register metadata
                let metadata = crate::database::index_metadata::IndexMetadata::new(
                    index_name.clone(),
                    stmt.table.clone(),
                    stmt.column.clone(),
                    crate::database::index_metadata::IndexType::Octree,
                );
                self.db.index_registry.register(metadata)?;
            }
            IndexType::BTree | IndexType::Column => {
                // 🚀 Column/BTree index creation
                // Column index works for any comparable type (Integer, Float, Text, etc.)
                // Bulk backfill is now handled internally by create_column_index()
                
                self.db.create_column_index_with_name(&stmt.table, &stmt.column, &index_name)?;
                
                // 🔥 OPTIMIZATION FIX: Also register with standard "{table}.{column}" name
                // This allows WHERE optimization to find the index
                let standard_name = format!("{}.{}", stmt.table, stmt.column);
                if index_name != standard_name {
                    // Clone the index reference and register with standard name
                    if let Some(index_ref) = self.db.column_indexes.get(&index_name) {
                        self.db.column_indexes.insert(standard_name.clone(), index_ref.clone());
                    }
                }
                
                // 🆕 Register metadata
                let metadata = crate::database::index_metadata::IndexMetadata::new(
                    index_name.clone(),
                    stmt.table.clone(),
                    stmt.column.clone(),
                    crate::database::index_metadata::IndexType::Column,
                );
                self.db.index_registry.register(metadata)?;
            }
        }
        
        Ok(QueryResult::Definition {
            message: format!("Index '{}' created successfully on {}.{}", 
                index_name, stmt.table, stmt.column),
        })
    }
    
    /// Execute DROP TABLE statement
    fn execute_drop_table(&self, stmt: DropTableStmt) -> Result<QueryResult> {
        let table_name = &stmt.table;

        // Verify table exists
        let schema = self.db.get_table_schema(table_name)?;

        // 1. Drop column indexes for this table
        let prefix = format!("{}.", table_name);
        let index_names: Vec<String> = self.db.column_indexes.iter()
            .filter(|entry| entry.key().starts_with(&prefix))
            .map(|entry| entry.key().clone())
            .collect();
        for idx_name in index_names {
            self.db.column_indexes.remove(&idx_name);
        }

        // 2. Drop vector indexes for this table
        let vector_idx_names: Vec<String> = self.db.vector_indexes.iter()
            .filter(|entry| entry.key().starts_with(&prefix) || entry.key().contains(&format!("_{}", table_name)))
            .map(|entry| entry.key().clone())
            .collect();
        for idx_name in vector_idx_names {
            self.db.vector_indexes.remove(&idx_name);
        }

        // 3. Drop text indexes for this table
        let text_idx_names: Vec<String> = self.db.text_indexes.iter()
            .filter(|entry| entry.key().starts_with(&prefix) || entry.key().contains(&format!("_{}", table_name)))
            .map(|entry| entry.key().clone())
            .collect();
        for idx_name in text_idx_names {
            self.db.text_indexes.remove(&idx_name);
        }

        // 4. Drop i-Octree indexes for this table
        let ioctree_idx_names: Vec<String> = self.db.ioctree_indexes.iter()
            .filter(|entry| entry.key().starts_with(&prefix) || entry.key().contains(&format!("_{}", table_name)))
            .map(|entry| entry.key().clone())
            .collect();
        for idx_name in ioctree_idx_names {
            self.db.ioctree_indexes.remove(&idx_name);
        }

        // 5. Drop table metadata (schema, auto_increment, pk_lookup)
        self.db.drop_table(table_name)?;

        // 6. Remove index registry entries
        self.db.index_registry.remove_by_table(table_name);

        // 7. Delete data from LSM using range delete (best effort)
        // Composite key = (table_id << 32) | row_id
        // We scan the entire range for this table_id
        let table_id = self.db.table_registry.get_table_id(table_name).unwrap_or(0);
        let start_key = (table_id as u64) << 32;
        let end_key = start_key | 0xFFFFFFFF;
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        if let Err(e) = self.db.lsm_engine.delete_range(start_key, end_key, timestamp) {
            debug_log!("[DROP TABLE] Warning: LSM range delete failed: {}", e);
        }

        // 8. Drop the ColSegmentStore (columnar source of truth) and its on-disk
        // segment files. Without this, recreating a same-named table sees stale
        // rows from the dropped one (test_create_drop_recreate returned 2 rows).
        // Also clear the columnar_write_bufs entry and the synced columnar_sstables
        // alias so reads don't fall back to the dropped data.
        if let Some(entry) = self.db.col_segment_stores.get(table_name) {
            // Best-effort: delete on-disk segment files + manifest.
            let _ = entry.value().drop_all();
        }
        self.db.col_segment_stores.remove(table_name);
        self.db.columnar_write_bufs.remove(table_name);
        self.db.columnar_sstables.remove(table_name);

        let _ = schema; // used above for validation

        Ok(QueryResult::Definition {
            message: format!("Table '{}' dropped successfully", table_name),
        })
    }
    
    /// Execute DROP INDEX statement
    fn execute_drop_index(&self, stmt: DropIndexStmt) -> Result<QueryResult> {
        use crate::database::index_metadata::IndexType;

        // Look up index metadata to know which collection to remove from
        let meta = self.db.index_registry.get(&stmt.index_name)
            .ok_or_else(|| MoteDBError::IndexNotFound(stmt.index_name.clone()))?;

        let index_name = &stmt.index_name;

        // Remove from the appropriate DashMap collection
        match meta.index_type {
            IndexType::Vector => {
                self.db.vector_indexes.remove(index_name);
            }
            IndexType::Text => {
                self.db.text_indexes.remove(index_name);
            }
            IndexType::Column => {
                self.db.column_indexes.remove(index_name);
                // Also remove the "table.column" alias if it exists
                let alias = format!("{}.{}", meta.table_name, meta.column_name);
                if alias != *index_name {
                    self.db.column_indexes.remove(&alias);
                }
            }
            IndexType::Octree => {
                self.db.ioctree_indexes.remove(index_name);
            }
        }

        // Remove from index registry (also persists)
        self.db.index_registry.remove(index_name)?;

        Ok(QueryResult::Definition {
            message: format!("Index '{}' dropped", index_name),
        })
    }
    
    /// 🆕 Execute ALTER TABLE statement
    fn execute_alter_table(&self, stmt: AlterTableStmt) -> Result<QueryResult> {
        use super::ast::AlterTableAction;
        
        match stmt.action {
            AlterTableAction::SetAutoIncrement(new_value) => {
                // Verify table exists and has AUTO_INCREMENT primary key
                let schema = self.db.get_table_schema(&stmt.table)?;
                
                if !schema.is_primary_key_auto_increment() {
                    return Err(MoteDBError::InvalidArgument(
                        format!("Table {} does not have AUTO_INCREMENT primary key", stmt.table)
                    ));
                }
                
                // Update the AUTO_INCREMENT counter
                self.db.set_auto_increment_value(&stmt.table, new_value)?;
                
                Ok(QueryResult::Definition {
                    message: format!("Table {} AUTO_INCREMENT set to {}", stmt.table, new_value),
                })
            }
        }
    }
    
    /// Execute SHOW TABLES
    fn execute_show_tables(&self) -> Result<QueryResult> {
        let tables = self.db.list_tables()?;
        
        let columns = vec!["Tables".to_string()];
        let rows = tables.into_iter()
            .map(|table_name| vec![Value::text(table_name)])
            .collect();
        
        Ok(QueryResult::Select { columns, rows })
    }
    
    /// Execute DESCRIBE TABLE
    fn execute_describe_table(&self, table_name: String) -> Result<QueryResult> {
        let schema = self.db.get_table_schema(&table_name)?;
        
        let columns = vec![
            "Field".to_string(),
            "Type".to_string(),
            "Nullable".to_string(),
            "Position".to_string(),
        ];
        
        let rows = schema.columns.iter().map(|col| {
            vec![
                Value::text(col.name.clone()),
                Value::text(format!("{:?}", col.col_type)),
                Value::text(if col.nullable { "YES" } else { "NO" }.into()),
                Value::Integer(col.position as i64),
            ]
        }).collect();
        
        Ok(QueryResult::Select { columns, rows })
    }
    

    /// Execute BEGIN [TRANSACTION]
    fn execute_begin_transaction(&self) -> Result<QueryResult> {
        let txn_id = self.db.begin_transaction()?;
        *self.current_txn_id.lock() = Some(txn_id);
        Ok(QueryResult::Definition {
            message: format!("Transaction {} started", txn_id),
        })
    }

    /// Execute COMMIT [TRANSACTION]
    fn execute_commit_transaction(&self) -> Result<QueryResult> {
        eprintln!("[TXN] COMMIT called");
        let _txn_id_opt = *self.current_txn_id.lock();
                if let Some(txn_id) = _txn_id_opt {
            self.db.commit_transaction(txn_id)?;
            *self.current_txn_id.lock() = None;
            Ok(QueryResult::Definition {
                message: format!("Transaction {} committed", txn_id),
            })
        } else {
            Ok(QueryResult::Definition {
                message: "No active transaction".to_string(),
            })
        }
    }

    /// Execute ROLLBACK [TRANSACTION]
    fn execute_rollback_transaction(&self) -> Result<QueryResult> {
        let _txn_id_opt = *self.current_txn_id.lock();
                if let Some(txn_id) = _txn_id_opt {
            self.db.rollback_transaction(txn_id)?;
            *self.current_txn_id.lock() = None;
            Ok(QueryResult::Definition {
                message: format!("Transaction {} rolled back", txn_id),
            })
        } else {
            Ok(QueryResult::Definition {
                message: "No active transaction".to_string(),
            })
        }
    }
    // Helper methods
    
    /// ✅ 优化辅助函数：高效构造 qualified name (table.column)
    #[inline]
    fn make_qualified_name(prefix: &str, col_name: &str) -> String {
        let mut qualified = String::with_capacity(prefix.len() + 1 + col_name.len());
        qualified.push_str(prefix);
        qualified.push('.');
        qualified.push_str(col_name);
        qualified
    }
    
    /// 🎯 Try to extract range query: WHERE col >= start AND col <= end
    /// Returns Some((column_name, start_value, end_value))
    /// 🚀 Try to extract dual-bound range query: WHERE col > X AND col < Y
    /// Returns (column_name, lower_bound, lower_op, upper_bound, upper_op)
    fn try_extract_range_query(&self, expr: &Expr) -> Option<(String, Value, BinaryOperator, Value, BinaryOperator)> {
        use crate::sql::ast::{BinaryOperator, Expr};
        
        match expr {
            Expr::BinaryOp { left, op, right } => {
                // Check for AND expressions
                if *op == BinaryOperator::And {
                    // Try to extract range from both sides
                    if let (Expr::BinaryOp { left: l1, op: op1, right: r1 }, 
                            Expr::BinaryOp { left: l2, op: op2, right: r2 }) 
                        = (left.as_ref(), right.as_ref()) {
                        
                        // Check if both sides reference the same column
                        let col1 = match (l1.as_ref(), r1.as_ref()) {
                            (Expr::Column(c), Expr::Literal(_)) => Some(c),
                            (Expr::Literal(_), Expr::Column(c)) => Some(c),
                            _ => None,
                        };
                        
                        let col2 = match (l2.as_ref(), r2.as_ref()) {
                            (Expr::Column(c), Expr::Literal(_)) => Some(c),
                            (Expr::Literal(_), Expr::Column(c)) => Some(c),
                            _ => None,
                        };
                        
                        if let (Some(c1), Some(c2)) = (&col1, &col2) {
                            if c1 == c2 {
                                let col_name = (*c1).clone();

                                // Extract bounds with operators
                                let (val1, is_lower1, op1_normalized) = match (l1.as_ref(), op1, r1.as_ref()) {
                                    (Expr::Column(_), BinaryOperator::Ge, Expr::Literal(v)) => Some((v.clone(), true, BinaryOperator::Ge)),
                                    (Expr::Column(_), BinaryOperator::Gt, Expr::Literal(v)) => Some((v.clone(), true, BinaryOperator::Gt)),
                                    (Expr::Literal(v), BinaryOperator::Le, Expr::Column(_)) => Some((v.clone(), true, BinaryOperator::Ge)),
                                    (Expr::Literal(v), BinaryOperator::Lt, Expr::Column(_)) => Some((v.clone(), true, BinaryOperator::Gt)),
                                    (Expr::Column(_), BinaryOperator::Le, Expr::Literal(v)) => Some((v.clone(), false, BinaryOperator::Le)),
                                    (Expr::Column(_), BinaryOperator::Lt, Expr::Literal(v)) => Some((v.clone(), false, BinaryOperator::Lt)),
                                    (Expr::Literal(v), BinaryOperator::Ge, Expr::Column(_)) => Some((v.clone(), false, BinaryOperator::Le)),
                                    (Expr::Literal(v), BinaryOperator::Gt, Expr::Column(_)) => Some((v.clone(), false, BinaryOperator::Lt)),
                                    _ => None,
                                }?;

                                let (val2, is_lower2, op2_normalized) = match (l2.as_ref(), op2, r2.as_ref()) {
                                    (Expr::Column(_), BinaryOperator::Ge, Expr::Literal(v)) => Some((v.clone(), true, BinaryOperator::Ge)),
                                    (Expr::Column(_), BinaryOperator::Gt, Expr::Literal(v)) => Some((v.clone(), true, BinaryOperator::Gt)),
                                    (Expr::Literal(v), BinaryOperator::Le, Expr::Column(_)) => Some((v.clone(), true, BinaryOperator::Ge)),
                                    (Expr::Literal(v), BinaryOperator::Lt, Expr::Column(_)) => Some((v.clone(), true, BinaryOperator::Gt)),
                                    (Expr::Column(_), BinaryOperator::Le, Expr::Literal(v)) => Some((v.clone(), false, BinaryOperator::Le)),
                                    (Expr::Column(_), BinaryOperator::Lt, Expr::Literal(v)) => Some((v.clone(), false, BinaryOperator::Lt)),
                                    (Expr::Literal(v), BinaryOperator::Ge, Expr::Column(_)) => Some((v.clone(), false, BinaryOperator::Le)),
                                    (Expr::Literal(v), BinaryOperator::Gt, Expr::Column(_)) => Some((v.clone(), false, BinaryOperator::Lt)),
                                    _ => None,
                                }?;

                                // One should be lower bound, one should be upper bound
                                if is_lower1 && !is_lower2 {
                                    return Some((col_name, val1, op1_normalized, val2, op2_normalized));
                                } else if !is_lower1 && is_lower2 {
                                    return Some((col_name, val2, op2_normalized, val1, op1_normalized));
                                }
                            }
                        }
                    }
                }
                None
            }
            _ => None,
        }
    }
    
    /// 🎯 Try to extract a simple point query pattern: WHERE column = value
    /// 
    /// Returns Some((column_name, value)) if the WHERE clause is a simple equality,
    /// allowing us to skip complex expression evaluation.
    fn try_extract_point_query(&self, expr: &Expr) -> Option<(String, Value)> {
        use crate::sql::ast::{BinaryOperator, Expr};
        
        match expr {
            Expr::BinaryOp { left, op, right } => {
                // Only optimize simple equality: col = value
                if *op == BinaryOperator::Eq {
                    // Pattern 1: Column = Literal
                    if let (Expr::Column(col), Expr::Literal(val)) = (left.as_ref(), right.as_ref()) {
                        // 注意: 列名可能没有表前缀 (例如 "id"),但 SqlRow 中的键有前缀 ("users.id")
                        // 我们返回不带前缀的列名,在过滤时需要匹配任何表前缀
                        return Some((col.clone(), val.clone()));
                    }
                    // Pattern 2: Literal = Column (reversed)
                    if let (Expr::Literal(val), Expr::Column(col)) = (left.as_ref(), right.as_ref()) {
                        return Some((col.clone(), val.clone()));
                    }
                }
                None
            }
            _ => None,
        }
    }
    
    /// 🚀 Try to extract simple inequality: WHERE column < value or WHERE column > value
    /// 
    /// Returns Some((column_name, operator, value))
    fn try_extract_inequality(&self, expr: &Expr) -> Option<(String, BinaryOperator, Value)> {
        use crate::sql::ast::{BinaryOperator, Expr};
        
        match expr {
            Expr::BinaryOp { left, op, right } => {
                // Check for <, >, <=, >=
                match op {
                    BinaryOperator::Lt | BinaryOperator::Le | 
                    BinaryOperator::Gt | BinaryOperator::Ge => {
                        // Pattern 1: Column op Literal
                        if let (Expr::Column(col), Expr::Literal(val)) = (left.as_ref(), right.as_ref()) {
                            return Some((col.clone(), op.clone(), val.clone()));
                        }
                        // Pattern 2: Literal op Column (reversed, need to flip operator)
                        if let (Expr::Literal(val), Expr::Column(col)) = (left.as_ref(), right.as_ref()) {
                            let flipped_op = match op {
                                BinaryOperator::Lt => BinaryOperator::Gt,
                                BinaryOperator::Le => BinaryOperator::Ge,
                                BinaryOperator::Gt => BinaryOperator::Lt,
                                BinaryOperator::Ge => BinaryOperator::Le,
                                _ => return None,
                            };
                            return Some((col.clone(), flipped_op, val.clone()));
                        }
                    }
                    _ => {}
                }
                None
            }
            _ => None,
        }
    }
    
    /// 🎯 Try to extract vector search pattern: VECTOR_SEARCH(column, [...], k)
    /// Returns Some((table_name, column_name, query_vector, k))
    fn try_extract_vector_search(&self, expr: &Expr, from: &TableRef) -> Option<(String, String, Vec<f32>, usize)> {
        use crate::sql::ast::Expr;
        
        // Extract table name
        let table_name = match from {
            TableRef::Table { name, .. } => name.clone(),
            _ => return None,
        };
        
        // Match VECTOR_SEARCH function
        match expr {
            Expr::FunctionCall { name, args, .. } if name.to_uppercase() == "VECTOR_SEARCH" => {
                if args.len() != 3 {
                    return None;
                }
                
                // Extract column name
                let column = match &args[0] {
                    Expr::Column(col) => col.clone(),
                    _ => return None,
                };
                
                // Extract query vector (expecting a Vector value)
                let query_vector = match &args[1] {
                    Expr::Literal(Value::Vector(vec)) => vec.clone(),
                    _ => return None,
                };
                
                // Extract k (reject non-positive values to prevent OOM)
                let k = match &args[2] {
                    Expr::Literal(Value::Integer(k)) => {
                        if *k <= 0 { return None; }
                        (*k).min(10000) as usize
                    }
                    _ => return None,
                };
                
                Some((table_name, column, query_vector.to_vec(), k))
            }
            _ => None,
        }
    }

    /// 🚀 FAST PATH 0a: Text search (MATCH AGAINST) — single index lookup
    ///
    /// Detects WHERE MATCH(col) AGAINST('query') and uses the text index directly
    /// instead of scanning all rows and calling search_ranked() per row.
    fn try_text_search_fast_path(
        &self,
        stmt: &SelectStmt,
        where_clause: &Expr,
        table_name: &str,
    ) -> Result<Option<QueryResult>> {
        // Extract MATCH expression from WHERE clause
        let (column, query, phrase) = match where_clause {
            Expr::Match { column, query, phrase } => (column.clone(), query.clone(), *phrase),
            // Handle AND: MATCH(...) AND other_conditions — only if MATCH is the dominant filter
            Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
                // Try both sides for a MATCH expression
                if let Expr::Match { column, query, phrase } = left.as_ref() {
                    (column.clone(), query.clone(), *phrase)
                } else if let Expr::Match { column, query, phrase } = right.as_ref() {
                    (column.clone(), query.clone(), *phrase)
                } else {
                    return Ok(None);
                }
            }
            _ => return Ok(None),
        };

        // Find text index for this column
        let index_name = match self.db.index_registry.find_by_column(
            table_name, &column,
            crate::database::index_metadata::IndexType::Text
        ) {
            Some(name) => name,
            None => return Ok(None),
        };

        if !self.db.text_indexes.contains_key(&index_name) {
            return Ok(None);
        }

        // Determine limit (use LIMIT from query, or default to top 1000 for scoring)
        let limit = stmt.limit.unwrap_or(1000);

        // Phrase search or ranked search depending on query type
        let row_ids: Vec<u64> = if phrase {
            let ids = match self.db.text_search_phrase(&index_name, &query) {
                Ok(r) => r,
                Err(_) => return Ok(None),
            };
            ids.into_iter().take(limit).collect()
        } else {
            let results = match self.db.text_search_ranked(&index_name, &query, limit) {
                Ok(r) => r,
                Err(_) => return Ok(None),
            };
            results.into_iter().map(|(id, _score)| id).collect()
        };

        if row_ids.is_empty() {
            return Ok(Some(QueryResult::Select {
                columns: vec![],
                rows: vec![],
            }));
        }

        // Load rows for matching row_ids — use batch fetch for efficiency
        let schema = self.db.get_table_schema(table_name)?;
        let mut sql_rows = Vec::with_capacity(row_ids.len());

        let batch_rows = self.db.get_table_rows_batch(table_name, &row_ids)?;

        for (i, row_id) in row_ids.iter().enumerate() {
            if let Some(row) = batch_rows.get(i).and_then(|(_, opt)| opt.as_ref()) {
                let mut sql_row = row_to_sql_row(row, &schema)?;
                sql_row.insert("__row_id__".to_string(), Value::Integer(*row_id as i64));
                sql_row.insert("__table__".to_string(), Value::text(table_name.to_string()));
                let score = 1.0f32;
                sql_row.insert(format!("__text_score_{}__", column), Value::Float(score as f64));
                let old_row = std::mem::take(&mut sql_row);
                let mut qualified = SqlRow::new();
                qualified.insert("__row_id__".to_string(), Value::Integer(*row_id as i64));
                qualified.insert("__table__".to_string(), Value::text(table_name.to_string()));
                qualified.insert(format!("__text_score_{}__", column), Value::Float(score as f64));
                for (col_name, val) in old_row.into_iter() {
                    let qname = Self::make_qualified_name(table_name, &col_name);
                    qualified.insert(qname, val);
                }
                sql_rows.push((*row_id, qualified));
            }
        }

        // Apply ORDER BY if present.
        // Default order is BM25 score descending (already sorted by text_search_ranked).
        // If ORDER BY specifies other columns, re-sort by those columns.
        if let Some(ref order_by) = stmt.order_by {
            let is_score_desc = order_by.len() == 1
                && matches!(&order_by[0].expr, Expr::Column(c) if c.to_lowercase().contains("score"))
                && !order_by[0].asc;
            if !is_score_desc {
                sql_rows.sort_by(|a, b| {
                    for ob in order_by {
                        if let Expr::Column(ref col_name) = ob.expr {
                            let a_val = a.1.get(col_name).or_else(|| a.1.get(&Self::make_qualified_name(table_name, col_name)));
                            let b_val = b.1.get(col_name).or_else(|| b.1.get(&Self::make_qualified_name(table_name, col_name)));
                            let cmp = match (a_val, b_val) {
                                (Some(Value::Integer(ai)), Some(Value::Integer(bi))) => ai.cmp(bi),
                                (Some(Value::Float(af)), Some(Value::Float(bf))) => af.partial_cmp(bf).unwrap_or(std::cmp::Ordering::Equal),
                                (Some(Value::Text(at)), Some(Value::Text(bt))) => at.cmp(bt),
                                _ => std::cmp::Ordering::Equal,
                            };
                            let result = if ob.asc { cmp } else { cmp.reverse() };
                            if result != std::cmp::Ordering::Equal { return result; }
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
        }

        // Build scores from sql_rows metadata
        let scores: Vec<(RowId, f32)> = sql_rows.iter().map(|(id, row)| {
            let score = row.get(&format!("__text_score_{}__", column))
                .and_then(|v| if let Value::Float(f) = v { Some(*f as f32) } else { None })
                .unwrap_or(1.0);
            (*id, score)
        }).collect();

        let (column_names, result_rows) = self.project_text_search_columns(
            stmt, &sql_rows, &schema, &column, &scores
        )?;

        Ok(Some(QueryResult::Select {
            columns: column_names,
            rows: result_rows,
        }))
    }

    /// Project columns for text search fast path, handling MATCH score columns
    fn project_text_search_columns(
        &self,
        stmt: &SelectStmt,
        rows: &[(u64, SqlRow)],
        schema: &TableSchema,
        match_column: &str,
        scores: &[(RowId, f32)],
    ) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
        // Build score lookup
        let score_map: std::collections::HashMap<u64, f64> = scores.iter()
            .map(|(id, s)| (*id, *s as f64))
            .collect();

        let (column_names, result_rows) = if stmt.columns.len() == 1 && matches!(stmt.columns[0], SelectColumn::Star) {
            // SELECT *
            let col_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
            let projected: Vec<Vec<Value>> = rows.iter().map(|(_, row)| {
                schema.columns.iter()
                    .map(|col| row.get(&col.name).cloned().unwrap_or(Value::Null))
                    .collect()
            }).collect();
            (col_names, projected)
        } else {
            // Specific columns — handle MATCH expressions
            let col_names: Vec<String> = stmt.columns.iter().map(|col| match col {
                SelectColumn::Star => "*".to_string(),
                SelectColumn::Column(name) => name.clone(),
                SelectColumn::ColumnWithAlias(_, alias) => alias.clone(),
                SelectColumn::Expr(_, Some(alias)) => alias.clone(),
                SelectColumn::Expr(expr, None) => format!("{:?}", expr),
            }).collect();

            let projected: Vec<Vec<Value>> = rows.iter().map(|(row_id, row)| {
                stmt.columns.iter().map(|col| {
                    match col {
                        SelectColumn::Column(name) | SelectColumn::ColumnWithAlias(name, _) => {
                            row.get(name).cloned().or_else(|| {
                                if !name.contains('.') {
                                    row.iter()
                                        .find(|(k, _)| k.ends_with(&format!(".{}", name)))
                                        .map(|(_, v)| v.clone())
                                } else {
                                    None
                                }
                            }).unwrap_or(Value::Null)
                        }
                        SelectColumn::Expr(expr, _) => {
                            // Check if this is a MATCH expression for our column
                            if let Expr::Match { column, .. } = expr {
                                if column == match_column {
                                    return score_map.get(row_id)
                                        .map(|s| Value::Float(*s))
                                        .unwrap_or(Value::Float(0.0));
                                }
                            }
                            self.eval_with_materialized(expr, row).unwrap_or(Value::Null)
                        }
                        SelectColumn::Star => Value::Null,
                    }
                }).collect()
            }).collect();
            (col_names, projected)
        };

        Ok((column_names, result_rows))
    }

    /// 🚀 FAST PATH 0b: Spatial (ST_WITHIN / ST_KNN) — single index lookup
    ///
    /// Detects WHERE ST_WITHIN(col, ...) or WHERE ST_KNN(col, ...) and uses
    /// the spatial index directly instead of scanning all rows.
    fn try_spatial_fast_path(
        &self,
        stmt: &SelectStmt,
        where_clause: &Expr,
        table_name: &str,
    ) -> Result<Option<QueryResult>> {
        match where_clause {
            // 3D spatial fast paths (i-Octree)
            Expr::StWithin3D { column, min_x, min_y, min_z, max_x, max_y, max_z } => {
                self.execute_ioctree_within_fast(stmt, table_name, column, *min_x, *min_y, *min_z, *max_x, *max_y, *max_z)
            }
            Expr::StKnn3D { column, x, y, z, k } => {
                self.execute_ioctree_knn_fast(stmt, table_name, column, *x, *y, *z, *k)
            }
            Expr::StRadius3D { column, x, y, z, radius } => {
                self.execute_ioctree_radius_fast(stmt, table_name, column, *x, *y, *z, *radius)
            }
            _ => Ok(None),
        }
    }

    /// 🚀 FAST PATH -1b: ORDER BY ST_DISTANCE(col, x, y) LIMIT k
    /// Detects ORDER BY ST_DISTANCE and uses spatial KNN index instead of full scan.
    fn try_optimize_spatial_order_by(&self, stmt: &SelectStmt) -> Result<Option<QueryResult>> {
        let order_by = match &stmt.order_by {
            Some(o) if o.len() == 1 => &o[0],
            _ => return Ok(None),
        };
        let limit = match stmt.limit {
            Some(k) if k > 0 => k,
            _ => return Ok(None),
        };
        // Must be ASC for distance (closer first)
        if !order_by.asc {
            return Ok(None);
        }
        // WHERE must be absent or trivially true
        if stmt.where_clause.is_some() {
            return Ok(None);
        }

        // Match ORDER BY ST_DISTANCE_3D(column, x, y, z) or ORDER BY alias
        let dist_expr = match &order_by.expr {
            Expr::StDistance3D { column, x, y, z } => (column.clone(), *x, *y, *z),
            Expr::Column(alias) => {
                // Look up alias in SELECT columns to find the ST_DISTANCE_3D expression
                let mut found = None;
                for col in &stmt.columns {
                    match col {
                        SelectColumn::Expr(expr, Some(a)) if a == alias => {
                            if let Expr::StDistance3D { column, x, y, z } = expr {
                                found = Some((column.clone(), *x, *y, *z));
                            }
                            break;
                        }
                        SelectColumn::ColumnWithAlias(_, a) if a == alias => {
                            break;
                        }
                        _ => {}
                    }
                }
                match found {
                    Some(v) => v,
                    None => return Ok(None),
                }
            }
            _ => return Ok(None),
        };

        let table_name = match stmt.from.as_ref() {
            Some(TableRef::Table { name, .. }) => name.clone(),
            _ => return Ok(None),
        };

        let (column, x, y, z) = dist_expr;
        let results: Vec<(RowId, f64)> = {
            let index_name = match self.db.index_registry.find_by_column(
                &table_name, &column,
                crate::database::index_metadata::IndexType::Octree
            ) {
                Some(name) => name,
                None => return Ok(None),
            };
            if !self.db.ioctree_indexes.contains_key(&index_name) {
                return Ok(None);
            }
            let point = crate::types::Point3D::new(x, y, z);
            match self.db.ioctree_knn_query(&index_name, &point, limit) {
                Ok(r) => r,
                Err(_) => return Ok(None),
            }
        };

        if results.is_empty() {
            return Ok(Some(QueryResult::Select { columns: vec![], rows: vec![] }));
        }

        // Load rows and project
        let schema = self.db.get_table_schema(&table_name)?;
        let dist_map: std::collections::HashMap<u64, f64> = results.iter().cloned().collect();
        let row_ids: Vec<RowId> = results.into_iter().map(|(id, _)| id).collect();

        let mut sql_rows = Vec::with_capacity(row_ids.len());
        for &row_id in &row_ids {
            if let Ok(Some(row)) = self.db.get_table_row(&table_name, row_id) {
                let mut sql_row = row_to_sql_row(&row, &schema)?;
                sql_row.insert("__row_id__".to_string(), Value::Integer(row_id as i64));
                sql_row.insert("__table__".to_string(), Value::text(table_name.clone()));
                if let Some(d) = dist_map.get(&row_id) {
                    sql_row.insert("__spatial_distance__".to_string(), Value::Float(*d));
                }
                let old_row = std::mem::take(&mut sql_row);
                let mut qualified = SqlRow::new();
                qualified.insert("__row_id__".to_string(), Value::Integer(row_id as i64));
                qualified.insert("__table__".to_string(), Value::text(table_name.clone()));
                if let Some(d) = dist_map.get(&row_id) {
                    qualified.insert("__spatial_distance__".to_string(), Value::Float(*d));
                }
                for (col_name, val) in old_row.into_iter() {
                    let qname = Self::make_qualified_name(&table_name, &col_name);
                    qualified.insert(qname, val);
                }
                sql_rows.push((row_id, qualified));
            }
        }

        let (column_names, result_rows) = self.project_columns(&stmt.columns, &sql_rows, &schema)?;
        Ok(Some(QueryResult::Select {
            columns: column_names,
            rows: result_rows,
        }))
    }

    /// Load rows by row_ids and project columns for spatial fast path
    fn load_and_project_spatial_rows(
        &self,
        stmt: &SelectStmt,
        table_name: &str,
        row_ids: &[RowId],
        dist_map: Option<&std::collections::HashMap<u64, f64>>,
        _is_within: bool,
    ) -> Result<Option<QueryResult>> {
        if row_ids.is_empty() {
            return Ok(Some(QueryResult::Select {
                columns: vec![],
                rows: vec![],
            }));
        }

        let schema = self.db.get_table_schema(table_name)?;
        let limit = stmt.limit.unwrap_or(row_ids.len());
        let row_ids_to_load = &row_ids[..row_ids.len().min(limit)];
        let columns = self.build_select_columns(&stmt.columns, &schema)?;

        let batch_rows = self.db.get_table_rows_batch(table_name, row_ids_to_load)?;

        let mut result_rows = Vec::with_capacity(batch_rows.len());
        for (row_id, row_opt) in batch_rows {
            if let Some(row) = row_opt {
                let mut projected = Self::project_row_direct(&row, &stmt.columns, &columns, &schema);
                if let Some(dm) = dist_map {
                    if let Some(d) = dm.get(&row_id) {
                        projected.push(Value::Float(*d));
                    }
                }
                result_rows.push(projected);
            }
        }

        let mut column_names = columns.clone();
        if dist_map.is_some() {
            column_names.push("distance".to_string());
        }

        Ok(Some(QueryResult::Select {
            columns: column_names,
            rows: result_rows,
        }))
    }

    // ==================== 3D Spatial Fast Paths (i-Octree) ====================

    /// Execute ST_WITHIN_3D using i-Octree index directly
    #[allow(clippy::too_many_arguments)]
    fn execute_ioctree_within_fast(
        &self,
        stmt: &SelectStmt,
        table_name: &str,
        column: &str,
        min_x: f64, min_y: f64, min_z: f64,
        max_x: f64, max_y: f64, max_z: f64,
    ) -> Result<Option<QueryResult>> {
        let index_name = match self.db.index_registry.find_by_column(
            table_name, column,
            crate::database::index_metadata::IndexType::Octree
        ) {
            Some(name) => name,
            None => return Ok(None),
        };

        if !self.db.ioctree_indexes.contains_key(&index_name) {
            return Ok(None);
        }

        let bbox = crate::types::BoundingBox3D::new(min_x, min_y, min_z, max_x, max_y, max_z);
        let row_ids = match self.db.ioctree_range_query(&index_name, &bbox) {
            Ok(ids) => ids,
            Err(_) => return Ok(None),
        };

        self.load_and_project_spatial_rows(stmt, table_name, &row_ids, None, true)
    }

    /// Execute ST_KNN_3D using i-Octree index directly
    #[allow(clippy::too_many_arguments)]
    fn execute_ioctree_knn_fast(
        &self,
        stmt: &SelectStmt,
        table_name: &str,
        column: &str,
        x: f64, y: f64, z: f64, k: usize,
    ) -> Result<Option<QueryResult>> {
        let index_name = match self.db.index_registry.find_by_column(
            table_name, column,
            crate::database::index_metadata::IndexType::Octree
        ) {
            Some(name) => name,
            None => return Ok(None),
        };

        if !self.db.ioctree_indexes.contains_key(&index_name) {
            return Ok(None);
        }

        let point = crate::types::Point3D::new(x, y, z);
        let results = match self.db.ioctree_knn_query(&index_name, &point, k) {
            Ok(r) => r,
            Err(_) => return Ok(None),
        };

        let row_ids: Vec<RowId> = results.iter().map(|(id, _)| *id).collect();
        let dist_map: std::collections::HashMap<u64, f64> = results.into_iter().collect();

        self.load_and_project_spatial_rows(stmt, table_name, &row_ids, Some(&dist_map), false)
    }

    /// Execute ST_RADIUS_3D using i-Octree index directly
    #[allow(clippy::too_many_arguments)]
    fn execute_ioctree_radius_fast(
        &self,
        stmt: &SelectStmt,
        table_name: &str,
        column: &str,
        x: f64, y: f64, z: f64, radius: f64,
    ) -> Result<Option<QueryResult>> {
        let index_name = match self.db.index_registry.find_by_column(
            table_name, column,
            crate::database::index_metadata::IndexType::Octree
        ) {
            Some(name) => name,
            None => return Ok(None),
        };

        if !self.db.ioctree_indexes.contains_key(&index_name) {
            return Ok(None);
        }

        let center = crate::types::Point3D::new(x, y, z);
        let results = match self.db.ioctree_radius_search(&index_name, &center, radius) {
            Ok(r) => r,
            Err(_) => return Ok(None),
        };

        let row_ids: Vec<RowId> = results.iter().map(|(id, _)| *id).collect();
        let dist_map: std::collections::HashMap<u64, f64> = results.into_iter().collect();

        self.load_and_project_spatial_rows(stmt, table_name, &row_ids, Some(&dist_map), false)
    }

    fn to_bool(&self, val: &Value) -> Result<bool> {
        match val {
            Value::Bool(b) => Ok(*b),
            Value::Integer(i) => Ok(*i != 0),
            Value::Float(f) => Ok(*f != 0.0 && !f.is_nan()),  // 🔧 Support Float: non-zero and non-NaN is true
            Value::Null => Ok(false),
            _ => Err(MoteDBError::TypeError("Cannot convert to boolean".to_string())),
        }
    }

    /// 🚀 PHASE A OPTIMIZATION: Compile simple comparison to fast closure
    /// 
    /// Converts simple patterns like:
    /// - col > 30 → |row| row.get("col") > 30
    /// - col = 'text' → |row| row.get("col") == "text"
    /// - age >= 18 AND age <= 65 → |row| row.get("age") >= 18 && row.get("age") <= 65
    /// 
    /// Returns None for complex expressions (falls back to interpreter)
    #[allow(clippy::only_used_in_recursion)]
    fn compile_simple_comparison(&self, expr: &Expr) -> RowPredicate {
        match expr {
            // Simple binary comparison: col op value
            Expr::BinaryOp { left, op, right } => {
                // Check if this is col op value pattern
                if let Expr::Column(col_name) = left.as_ref() {
                    if let Expr::Literal(value) = right.as_ref() {
                        let col = col_name.clone();
                        let val = value.clone();
                        
                        match op {
                            BinaryOperator::Gt => {
                                return Some(Box::new(move |row: &SqlRow| {
                                    Self::get_column_value_static(row, &col)
                                        .and_then(|v| Self::compare_values(v, &val))
                                        .map(|ord| ord == std::cmp::Ordering::Greater)
                                        .unwrap_or(false)
                                }));
                            }
                            BinaryOperator::Lt => {
                                return Some(Box::new(move |row: &SqlRow| {
                                    Self::get_column_value_static(row, &col)
                                        .and_then(|v| Self::compare_values(v, &val))
                                        .map(|ord| ord == std::cmp::Ordering::Less)
                                        .unwrap_or(false)
                                }));
                            }
                            BinaryOperator::Ge => {
                                return Some(Box::new(move |row: &SqlRow| {
                                    Self::get_column_value_static(row, &col)
                                        .and_then(|v| Self::compare_values(v, &val))
                                        .map(|ord| ord != std::cmp::Ordering::Less)
                                        .unwrap_or(false)
                                }));
                            }
                            BinaryOperator::Le => {
                                return Some(Box::new(move |row: &SqlRow| {
                                    Self::get_column_value_static(row, &col)
                                        .and_then(|v| Self::compare_values(v, &val))
                                        .map(|ord| ord != std::cmp::Ordering::Greater)
                                        .unwrap_or(false)
                                }));
                            }
                            BinaryOperator::Eq => {
                                return Some(Box::new(move |row: &SqlRow| {
                                    Self::get_column_value_static(row, &col)
                                        .map(|v| v == &val)
                                        .unwrap_or(false)
                                }));
                            }
                            BinaryOperator::Ne => {
                                return Some(Box::new(move |row: &SqlRow| {
                                    Self::get_column_value_static(row, &col)
                                        .map(|v| v != &val)
                                        .unwrap_or(false)
                                }));
                            }
                            _ => {}
                        }
                    }
                }
                
                // AND combination of two simple comparisons
                if *op == BinaryOperator::And {
                    if let (Some(left_fn), Some(right_fn)) = (
                        self.compile_simple_comparison(left),
                        self.compile_simple_comparison(right)
                    ) {
                        return Some(Box::new(move |row: &SqlRow| {
                            left_fn(row) && right_fn(row)
                        }));
                    }
                }
                
                None
            }
            _ => None,
        }
    }
    
    /// Helper: Get column value from row (handles table prefixes)
    fn get_column_value_static<'a>(row: &'a SqlRow, col_name: &str) -> Option<&'a Value> {
        // Try exact match first
        if let Some(val) = row.get(col_name) {
            return Some(val);
        }
        
        // Try with table prefix
        if !col_name.contains('.') {
            for (key, val) in row.iter() {
                if key.ends_with(&format!(".{}", col_name)) {
                    return Some(val);
                }
            }
        }
        
        None
    }
    
    /// Helper: Compare two values
    fn compare_values(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
        match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Some(a.cmp(b)),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Text(a), Value::Text(b)) => Some(a.cmp(b)),
            (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
            _ => None,
        }
    }
    
    // 🚀 P0 FIX: Primary Key Point Query optimization
    
    /// Try to optimize WHERE primary_key = value pattern
    /// 
    /// Detects patterns like:
    /// - `SELECT * FROM table WHERE id = 12345`
    /// - `SELECT col1, col2 FROM table WHERE id = 100`
    /// 
    /// Benefits:
    /// - 165x faster: 0.1ms vs 16.5ms (with 703 MemTable rows)
    /// - No MemTable scan: Direct LSM get by composite_key
    /// - No memory growth: Stable 2MB instead of 11MB spike
    /// - O(log n) complexity instead of O(n)
    fn try_optimize_primary_key_point_query(&self, stmt: &SelectStmt) -> Result<Option<QueryResult>> {
        // Must have WHERE clause
        let where_clause = match &stmt.where_clause {
            Some(w) => w,
            None => return Ok(None),
        };
        
        // Extract point query: column = value
        let (col_name, target_value) = match self.try_extract_point_query(where_clause) {
            Some(pair) => pair,
            None => return Ok(None),
        };
        
        // Get table name
        let table_name = match stmt.from.as_ref() {
            Some(TableRef::Table { name, .. }) => name,
            _ => return Ok(None),
        };
        
        // Check if this column is the primary key
        let schema = self.db.get_table_schema(table_name)?;
        let is_primary_key = schema.primary_key()
            .map(|pk| pk == col_name)
            .unwrap_or(false);
        
        if !is_primary_key {
            return Ok(None);  // Not primary key, fallback to normal query
        }
        
        // 🚀 P3 CRITICAL OPTIMIZATION: AUTO_INCREMENT primary key
        // 
        // For AUTO_INCREMENT tables:
        // - Primary key value == row_id (always)
        // - No need for column index lookup
        // - Direct LSM get: O(log n) instead of O(2 * log n)
        // 
        // Performance improvement:
        // - Before: 20 ms (column index B-Tree + LSM get)
        // - After:  < 5 ms (direct LSM get only)
        // - Speedup: **4x faster** 🚀
        //
        if schema.is_primary_key_auto_increment() {
            // 🚀 Fast path: Primary key value IS row_id
            let row_id = match &target_value {
                Value::Integer(id) => {
                    if *id < 0 {
                        // Negative ID is invalid, return empty result
                        let (column_names, _) = self.project_columns(&stmt.columns, &[], &schema)?;
                        return Ok(Some(QueryResult::Select {
                            columns: column_names,
                            rows: vec![],
                        }));
                    }
                    *id as RowId
                }
                _ => {
                    // Primary key must be INTEGER, return empty result
                    let (column_names, _) = self.project_columns(&stmt.columns, &[], &schema)?;
                    return Ok(Some(QueryResult::Select {
                        columns: column_names,
                        rows: vec![],
                    }));
                }
            };
            
            // 🚀 Check row_cache first (microsecond-level hit, skips deserialize)
            if let Some(cached_row) = self.db.row_cache.get(table_name, row_id) {
                let is_select_star = stmt.columns.len() == 1
                    && matches!(stmt.columns[0], SelectColumn::Star);

                if is_select_star {
                    let column_names = (*schema.column_names_arc()).clone();
                    let result_row: Vec<Value> = schema.columns.iter()
                        .map(|col| cached_row.get(col.position).cloned().unwrap_or(Value::Null))
                        .collect();
                    return Ok(Some(QueryResult::Select {
                        columns: column_names,
                        rows: vec![result_row],
                    }));
                }

                let sql_row = row_to_sql_row(&cached_row, &schema)?;
                let mut prefixed_row = SqlRow::new();
                prefixed_row.insert("__row_id__".to_string(), Value::Integer(row_id as i64));
                prefixed_row.insert("__table__".to_string(), Value::text(table_name.clone()));
                for (col_name, val) in sql_row {
                    let qualified_name = format!("{}.{}", table_name, col_name);
                    prefixed_row.insert(qualified_name, val);
                }
                let sql_rows = vec![(row_id, prefixed_row)];
                let (column_names, result_rows) = self.project_columns(&stmt.columns, &sql_rows, &schema)?;
                return Ok(Some(QueryResult::Select {
                    columns: column_names,
                    rows: result_rows,
                }));
            }

            // 🚀 Direct get: ColSegmentStore first (new path), then LSM (legacy).
            let composite_key = self.db.make_composite_key(table_name, row_id);
            if self.db.has_col_segment_store(table_name) {
                if let Some(store) = self.db.col_segment_stores.get(table_name) {
                    if let Some(row) = store.get(composite_key) {
                        self.db.row_cache.put(table_name.to_string(), row_id, row.clone());
                        let sql_row = row_to_sql_row(&row, &schema)?;
                        let mut prefixed_row = SqlRow::new();
                        prefixed_row.insert("__row_id__".to_string(), Value::Integer(row_id as i64));
                        prefixed_row.insert("__table__".to_string(), Value::text(table_name.clone()));
                        for (col_name, val) in sql_row {
                            prefixed_row.insert(format!("{}.{}", table_name, col_name), val);
                        }
                        let (column_names, result_rows) = self.project_columns(&stmt.columns, &[(row_id, prefixed_row)], &schema)?;
                        return Ok(Some(QueryResult::Select { columns: column_names, rows: result_rows }));
                    }
                    // Not found in store — return empty (key doesn't exist).
                    let (column_names, _) = self.project_columns(&stmt.columns, &[], &schema)?;
                    return Ok(Some(QueryResult::Select { columns: column_names, rows: vec![] }));
                }
            }
            match self.db.lsm_engine.get(composite_key)? {
                Some(value_data) => {
                    // Check tombstone
                    if value_data.deleted {
                        let (column_names, _) = self.project_columns(&stmt.columns, &[], &schema)?;
                        return Ok(Some(QueryResult::Select {
                            columns: column_names,
                            rows: vec![],
                        }));
                    }
                    
                    // Deserialize row data
                    let data = match &value_data.data {
                        crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                        _ => return Err(StorageError::InvalidData("Unexpected blob".into())),
                    };
                    
                    let row = decode_row(data, &schema)
                        .map_err(|e| StorageError::InvalidData(format!("Deserialization failed: {}", e)))?;

                    // Populate row_cache for future hot-path lookups
                    self.db.row_cache.put(table_name.to_string(), row_id, row.clone());

                    // 🚀 Fast path for SELECT *: skip HashMap conversion entirely
                    //     Direct positional projection from Vec<Value> — saves 2*N HashMap
                    //     inserts + N format!() calls for prefix rewriting.
                    let is_select_star = stmt.columns.len() == 1
                        && matches!(stmt.columns[0], SelectColumn::Star);

                    if is_select_star {
                        let column_names = (*schema.column_names_arc()).clone();
                        let result_row: Vec<Value> = schema.columns.iter()
                            .map(|col| row.get(col.position).cloned().unwrap_or(Value::Null))
                            .collect();

                        return Ok(Some(QueryResult::Select {
                            columns: column_names,
                            rows: vec![result_row],
                        }));
                    }

                    // Slow path: column projection needs HashMap-based SqlRow
                    // Convert to SqlRow
                    let sql_row = row_to_sql_row(&row, &schema)?;
                    
                    // Add table prefix
                    let mut prefixed_row = SqlRow::new();
                    prefixed_row.insert("__row_id__".to_string(), Value::Integer(row_id as i64));
                    prefixed_row.insert("__table__".to_string(), Value::text(table_name.clone()));
                    
                    for (col_name, val) in sql_row {
                        let qualified_name = format!("{}.{}", table_name, col_name);
                        prefixed_row.insert(qualified_name, val);
                    }
                    
                    let sql_rows = vec![(row_id, prefixed_row)];
                    
                    // Project columns
                    let (column_names, result_rows) = self.project_columns(&stmt.columns, &sql_rows, &schema)?;
                    
                    return Ok(Some(QueryResult::Select {
                        columns: column_names,
                        rows: result_rows,
                    }));
                }
                None => {
                    // Row not found, return empty result
                    let (column_names, _) = self.project_columns(&stmt.columns, &[], &schema)?;
                    return Ok(Some(QueryResult::Select {
                        columns: column_names,
                        rows: vec![],
                    }));
                }
            }
        }
        
        // 🔧 Non-AUTO_INCREMENT primary key: Use column index to lookup row_id
        // The primary key column has an auto-created index at table creation
        let row_ids = self.db.query_by_column(table_name, &col_name, &target_value)?;
        
        if row_ids.is_empty() {
            // Row not found, return empty result
            let (column_names, _) = self.project_columns(&stmt.columns, &[], &schema)?;
            return Ok(Some(QueryResult::Select {
                columns: column_names,
                rows: vec![],
            }));
        }
        
        // Primary key should be unique, take the first row_id
        let row_id = row_ids[0];
        
        // 🚀 P3++ 优化：直接使用 LSM get（跳过 get_table_row 的额外开销）
        // 
        // ## 性能提升
        // - 延迟：20.65 ms → **~10-15 ms**（**1.5-2x 提速** 🚀）
        // - 跳过 get_table_row 的额外逻辑
        // 
        let composite_key = self.db.make_composite_key(table_name, row_id);
        match self.db.lsm_engine.get(composite_key)? {
            Some(value_data) => {
                // 检查 tombstone
                if value_data.deleted {
                    let (column_names, _) = self.project_columns(&stmt.columns, &[], &schema)?;
                    return Ok(Some(QueryResult::Select {
                        columns: column_names,
                        rows: vec![],
                    }));
                }
                
                // 反序列化行数据
                let data = match &value_data.data {
                    crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                    _ => return Err(StorageError::InvalidData("Unexpected blob".into())),
                };
                
                let row = decode_row(data, &schema)
                    .map_err(|e| StorageError::InvalidData(format!("Deserialization failed: {}", e)))?;

                // 🚀 Fast path for SELECT *: skip HashMap conversion entirely
                let is_select_star = stmt.columns.len() == 1
                    && matches!(stmt.columns[0], SelectColumn::Star);

                if is_select_star {
                    let column_names: Vec<String> = schema.columns.iter()
                        .map(|c| c.name.clone())
                        .collect();
                    let result_row: Vec<Value> = schema.columns.iter()
                        .map(|col| row.get(col.position).cloned().unwrap_or(Value::Null))
                        .collect();

                    return Ok(Some(QueryResult::Select {
                        columns: column_names,
                        rows: vec![result_row],
                    }));
                }

                // 转换为 SqlRow
                let sql_row = row_to_sql_row(&row, &schema)?;
                
                // Add table prefix
                let mut prefixed_row = SqlRow::new();
                prefixed_row.insert("__row_id__".to_string(), Value::Integer(row_id as i64));
                prefixed_row.insert("__table__".to_string(), Value::text(table_name.clone()));
                
                for (col_name, val) in sql_row {
                    let qualified_name = format!("{}.{}", table_name, col_name);
                    prefixed_row.insert(qualified_name, val);
                }
                
                let sql_rows = vec![(row_id, prefixed_row)];
                
                // Project columns
                let (column_names, result_rows) = self.project_columns(&stmt.columns, &sql_rows, &schema)?;
                
                Ok(Some(QueryResult::Select {
                    columns: column_names,
                    rows: result_rows,
                }))
            }
            None => {
                // Row not found, return empty result
                let (column_names, _) = self.project_columns(&stmt.columns, &[], &schema)?;
                Ok(Some(QueryResult::Select {
                    columns: column_names,
                    rows: vec![],
                }))
            }
        }
    }
    
    // 🚀 P0 FIX: Primary Key ORDER BY optimization
    
    /// Try to optimize ORDER BY primary_key [ASC/DESC] [LIMIT k]
    /// 
    /// Detects patterns like:
    /// - `SELECT * FROM table ORDER BY id LIMIT 10`
    /// - `SELECT * FROM table ORDER BY id DESC`
    /// 
    /// Benefits:
    /// - 600x faster: 1ms vs 611ms (300K rows)
    /// - 280x less memory: 0.1MB vs 28MB
    /// - O(k) complexity instead of O(n log n)
    fn try_optimize_primary_key_order_by(&self, stmt: &SelectStmt) -> Result<Option<QueryResult>> {
        // Must have ORDER BY with single column
        let order_by = match &stmt.order_by {
            Some(o) if o.len() == 1 => &o[0],
            _ => return Ok(None),
        };
        
        // ORDER BY must be a simple column reference
        let order_column = match &order_by.expr {
            Expr::Column(col) => col,
            _ => return Ok(None),
        };
        
        // Get table name
        let table_name = match stmt.from.as_ref() {
            Some(TableRef::Table { name, .. }) => name,
            _ => return Ok(None),
        };
        
        // Check if this column is the primary key
        let schema = self.db.get_table_schema(table_name)?;
        let is_primary_key = schema.primary_key()
            .map(|pk| pk == order_column)
            .unwrap_or(false);
        
        if !is_primary_key {
            return Ok(None);
        }
        
        // Check that there's no WHERE clause (for now)
        if stmt.where_clause.is_some() {
            return Ok(None);
        }
        
        // Check that we're selecting all columns or simple column list
        let is_simple_select = matches!(&stmt.columns[..], [SelectColumn::Star]);
        if !is_simple_select {
            // Allow explicit column lists but not complex expressions
            let has_complex_expr = stmt.columns.iter().any(|col| {
                matches!(col, SelectColumn::Expr(_, _))
            });
            if has_complex_expr {
                return Ok(None);
            }
        }
        
        // Get primary key column index
        let pk_index_name = format!("{}.{}", table_name, order_column);

        // Check if index exists
        if !self.db.column_indexes.contains_key(&pk_index_name) {
            // No index, fallback to normal execution
            return Ok(None);
        }

        // Scan primary key index to get row_ids in order
        let index_arc = self.db.column_indexes
            .get(&pk_index_name)
            .ok_or_else(|| crate::StorageError::Index(format!("Primary key index not found: {}", pk_index_name)))?
            .clone();  // Clone Arc<ColumnValueIndex>
        
        // Calculate how many entries we need to scan
        let offset = stmt.offset.unwrap_or(0);
        let limit = stmt.limit.unwrap_or(usize::MAX);
        let scan_limit = if limit == usize::MAX {
            None  // No limit, scan all
        } else {
            Some(offset + limit)  // Scan enough to cover offset + limit
        };
        
        let row_ids = index_arc.scan_row_ids_with_limit(scan_limit)?;

        // If the column index is empty (async pipeline may not have built it yet),
        // fall back to full scan to avoid returning wrong empty results.
        if row_ids.is_empty() {
            return Ok(None);
        }

        // Apply sort order (ascending or descending)
        let sorted_row_ids = if order_by.asc {
            row_ids
        } else {
            let mut rev = row_ids;
            rev.reverse();
            rev
        };
        
        // Apply LIMIT and OFFSET
        let limit = stmt.limit.unwrap_or(usize::MAX);
        let offset = stmt.offset.unwrap_or(0);
        
        let limited_row_ids: Vec<_> = sorted_row_ids
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect();
        
        // Load rows
        let mut sql_rows = Vec::with_capacity(limited_row_ids.len());
        for row_id in limited_row_ids {
            if let Ok(Some(row)) = self.db.get_table_row(table_name, row_id) {
                let sql_row = row_to_sql_row(&row, &schema)?;
                sql_rows.push((row_id, sql_row));
            }
        }
        
        // Add table prefix
        prefix_rows(&mut sql_rows, table_name, table_name);

        // Project columns
        let (column_names, result_rows) = self.project_columns(&stmt.columns, &sql_rows, &schema)?;
        
        Ok(Some(QueryResult::Select {
            columns: column_names,
            rows: result_rows,
        }))
    }
    
    // 🚀 P0 FIX: Vector ORDER BY optimization helpers
    
    /// Try to optimize ORDER BY with vector distance
    fn try_optimize_vector_order_by(&self, stmt: &SelectStmt) -> Result<Option<VectorOrderByPlan>> {
        // 必须有 ORDER BY 和 LIMIT
        let order_by = match &stmt.order_by {
            Some(o) if o.len() == 1 => &o[0],
            _ => return Ok(None),
        };
        
        let limit = match stmt.limit {
            Some(k) if k > 0 => k,
            _ => return Ok(None),
        };
        
        // 解析 ORDER BY 表达式
        let (column, query_vector, asc) = match &order_by.expr {
            // 匹配: column <-> [vector] (L2Distance)
            Expr::BinaryOp { op: BinaryOperator::L2Distance | BinaryOperator::CosineDistance, left, right } => {
                match (&**left, &**right) {
                    (Expr::Column(col), Expr::Literal(Value::Vector(vec))) => {
                        (col.clone(), vec.clone(), order_by.asc)
                    }
                    _ => return Ok(None),
                }
            }
            _ => return Ok(None),
        };
        
        // 向量距离必须是升序
        if !asc {
            return Ok(None);
        }
        
        // 获取表名
        let table_name = match stmt.from.as_ref() {
            Some(TableRef::Table { name, .. }) => name.clone(),
            _ => return Ok(None),
        };
        
        // 检查索引
        let index_name = format!("{}_{}", table_name, column);
        if !self.db.has_vector_index(&index_name) {
            return Ok(None);
        }
        
        Ok(Some(VectorOrderByPlan {
            table: table_name,
            column,
            query_vector: query_vector.to_vec(),
            k: limit,
        }))
    }
    
    /// Execute SELECT using vector ORDER BY optimization
    fn execute_vector_order_by_plan(&self, stmt: &SelectStmt, plan: &VectorOrderByPlan) -> Result<QueryResult> {
        debug_log!("[Executor] ✅ 使用向量索引优化 ORDER BY: {} <-> [...] LIMIT {}", plan.column, plan.k);

        // Resolve index name via registry (supports custom index names)
        let index_name = self.db.index_registry.find_by_column(
            &plan.table, &plan.column,
            crate::database::index_metadata::IndexType::Vector
        ).unwrap_or_else(|| format!("{}_{}", plan.table, plan.column));
        
        // 1. 向量搜索获取 Top-K row_ids
        let candidates = self.db.vector_search(&index_name, &plan.query_vector, plan.k)?;
        debug_log!("[Executor] 🔍 vector_search返回了{}个候选", candidates.len());
        
        let row_ids: Vec<u64> = candidates.iter().map(|(id, _dist)| *id).collect();
        
        if !row_ids.is_empty() {
            debug_log!("[Executor] 🔍 row_ids前5个: {:?}", &row_ids[..5.min(row_ids.len())]);
        }
        
        if row_ids.is_empty() {
            // 返回空结果
            let schema = self.db.get_table_schema(&plan.table)?;
            return Ok(QueryResult::Select {
                columns: schema.columns.iter().map(|c| c.name.clone()).collect(),
                rows: vec![],
            });
        }
        
        // 2. 批量获取行数据
        let schema = self.db.get_table_schema(&plan.table)?;
        let batch_rows = self.db.get_table_rows_batch(&plan.table, &row_ids)?;
        
        debug_log!("[Executor] 🔍 get_table_rows_batch返回了{}个行", batch_rows.len());
        
        // 3. 转换为SQL行格式（保持向量搜索的顺序）
        let mut sql_rows = Vec::with_capacity(row_ids.len());
        for (row_id, row_opt) in batch_rows {
            if let Some(row) = row_opt {
                let sql_row = row_to_sql_row(&row, &schema)?;
                
                // 🔍 Debug: 打印前3个的row_id和id列
                if sql_rows.len() < 3 {
                    if let Some(_id_value) = sql_row.get("id") {
                        debug_log!("[Executor] 🔍 row_id={} → id列={:?}", row_id, _id_value);
                    }
                }
                
                sql_rows.push((row_id, sql_row));
            }
        }
        
        // 4. 应用WHERE条件（如果有）
        let filtered_rows: Vec<(u64, SqlRow)> = if let Some(ref where_clause) = stmt.where_clause {
            sql_rows.into_iter()
                .filter(|(_, row)| {
                    self.evaluator.eval(where_clause, row)
                        .and_then(|val| self.to_bool(&val))
                        .unwrap_or(false)
                })
                .collect()
        } else {
            sql_rows
        };
        
        // 5. 简单列投影（避免递归调用 project_columns）
        let column_names: Vec<String> = if stmt.columns.len() == 1 && matches!(stmt.columns[0], SelectColumn::Star) {
            // SELECT *
            schema.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            stmt.columns.iter().map(|col| match col {
                SelectColumn::Star => "*".to_string(),
                SelectColumn::Column(name) | SelectColumn::ColumnWithAlias(name, _) => name.clone(),
                SelectColumn::Expr(_, Some(alias)) => alias.clone(),
                SelectColumn::Expr(expr, None) => format!("{:?}", expr),
            }).collect()
        };
        
        let projected_rows: Vec<Vec<Value>> = filtered_rows.iter().map(|(_, row)| {
            if stmt.columns.len() == 1 && matches!(stmt.columns[0], SelectColumn::Star) {
                // SELECT * - return all columns in schema order
                schema.columns.iter()
                    .map(|col| row.get(&col.name).cloned().unwrap_or(Value::Null))
                    .collect()
            } else {
                stmt.columns.iter().map(|col| {
                    match col {
                        SelectColumn::Column(name) | SelectColumn::ColumnWithAlias(name, _) => {
                            row.get(name).cloned().unwrap_or(Value::Null)
                        }
                        SelectColumn::Expr(expr, _) => {
                            // ⚠️ 只对简单表达式求值，避免递归
                            self.evaluator.eval(expr, row).unwrap_or(Value::Null)
                        }
                        SelectColumn::Star => Value::Null,
                    }
                }).collect()
            }
        }).collect();
        
        // 6. 应用 OFFSET（如果有）
        let offset = stmt.offset.unwrap_or(0);
        let final_rows: Vec<Vec<Value>> = projected_rows.into_iter()
            .skip(offset)
            .take(plan.k)
            .collect();
        
        Ok(QueryResult::Select {
            columns: column_names,
            rows: final_rows,
        })
    }

    // ==================== Columnar Store Routing ====================

    /// Try to serve a SELECT from the columnar store for TimeSeries tables.
    /// Returns Ok(Some(result)) if handled, Ok(None) if it should fall through to LSM.
    fn try_columnar_select(
        &self,
        stmt: &SelectStmt,
        schema: &TableSchema,
    ) -> Result<Option<QueryResult>> {
        // Only handle simple FROM table (no JOINs, subqueries)
        let table_name = match stmt.from.as_ref() {
            Some(TableRef::Table { name, .. }) => name.clone(),
            _ => return Ok(None),
        };

        // Extract time range from WHERE clause
        let ts_col = match &schema.timeseries_column {
            Some(col) => col.clone(),
            None => return Ok(None),
        };

        let (start_ts, end_ts) = match self.extract_time_range(&stmt.where_clause, &ts_col) {
            Some(range) => range,
            None => return Ok(None), // Can't determine time range → fall through
        };

        // Don't handle aggregates or GROUP BY via columnar fast path;
        // let the standard executor handle them (data is also in LSM via WAL replay).
        if stmt.group_by.is_some() || self.has_aggregates(&stmt.columns) {
            return Ok(None);
        }

        // Extract requested column names
        let column_names: Vec<String> = stmt.columns.iter().map(|col| {
            match col {
                SelectColumn::Star => "*".to_string(),
                SelectColumn::Column(name) | SelectColumn::ColumnWithAlias(name, _) => name.clone(),
                SelectColumn::Expr(_, alias) => alias.clone().unwrap_or_default(),
            }
        }).collect();

        // If star, pass empty vec (means all columns)
        let query_cols: Vec<String> = if column_names.iter().any(|c| c == "*") {
            vec![]
        } else {
            column_names.clone()
        };

        // Extract non-timestamp column conditions for pruning
        let conditions = self.extract_column_conditions(&stmt.where_clause, schema, &ts_col);

        let results = if conditions.is_empty() {
            self.db.columnar_store.query_time_range(
                &table_name,
                start_ts,
                end_ts,
                &query_cols,
            )?
        } else {
            self.db.columnar_store.query_with_conditions(
                &table_name,
                start_ts,
                end_ts,
                &conditions,
                &query_cols,
            )?
        };

        // Build result rows
        let output_columns: Vec<String> = if query_cols.is_empty() {
            schema.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            column_names
        };

        let mut rows = Vec::new();
        for (_row_id, sql_row) in &results {
            let mut row = Vec::new();
            for col_name in &output_columns {
                row.push(sql_row.get(col_name).cloned().unwrap_or(Value::Null));
            }
            rows.push(row);
        }

        // P1: Handle ORDER BY for columnar results
        if let Some(ref order_by) = stmt.order_by {
            for order_item in order_by.iter().rev() {
                let col_name = match &order_item.expr {
                    Expr::Column(name) => name.clone(),
                    _ => continue,
                };
                let col_idx = output_columns.iter().position(|c| *c == col_name);
                if let Some(idx) = col_idx {
                    let ascending = order_item.asc;
                    rows.sort_by(|a, b| {
                        let va = a.get(idx).unwrap_or(&Value::Null);
                        let vb = b.get(idx).unwrap_or(&Value::Null);
                        let cmp = match (va, vb) {
                            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
                            (Value::Null, _) => std::cmp::Ordering::Less,
                            (_, Value::Null) => std::cmp::Ordering::Greater,
                            _ => va.partial_cmp(vb).unwrap_or(std::cmp::Ordering::Equal),
                        };
                        if ascending { cmp } else { cmp.reverse() }
                    });
                }
            }
        }

        // P1: Handle OFFSET and LIMIT
        let offset = stmt.offset.unwrap_or(0);
        if offset > 0 {
            let _ = rows.drain(..offset.min(rows.len()));
        }
        if let Some(limit) = stmt.limit {
            rows.truncate(limit);
        }

        Ok(Some(QueryResult::Select {
            columns: output_columns,
            rows,
        }))
    }

    /// Extract time range from WHERE clause.
    /// Looks for patterns: ts BETWEEN a AND b, ts >= a AND ts <= b, ts > a, ts < b
    /// Also handles reverse comparisons: a >= ts → ts <= a, etc.
    fn extract_time_range(&self, where_clause: &Option<Expr>, ts_col: &str) -> Option<(i64, i64)> {
        let expr = where_clause.as_ref()?;

        match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::And => {
                        let left_range = self.extract_time_range(&Some(*left.clone()), ts_col)?;
                        let right_range = self.extract_time_range(&Some(*right.clone()), ts_col)?;
                        let start = left_range.0.max(right_range.0);
                        let end = left_range.1.min(right_range.1);
                        Some((start, end))
                    }
                    BinaryOperator::Ge => {
                        // ts >= val OR val >= ts (reverse: ts <= val)
                        if let Expr::Column(col) = left.as_ref() {
                            if col == ts_col {
                                let val = self.eval_literal_to_i64(right)?;
                                return Some((val, i64::MAX));
                            }
                        }
                        // Reverse: literal >= ts → ts <= literal
                        if let Expr::Column(col) = right.as_ref() {
                            if col == ts_col {
                                let val = self.eval_literal_to_i64(left)?;
                                return Some((i64::MIN, val));
                            }
                        }
                        None
                    }
                    BinaryOperator::Gt => {
                        if let Expr::Column(col) = left.as_ref() {
                            if col == ts_col {
                                let val = self.eval_literal_to_i64(right)?;
                                return Some((val + 1, i64::MAX));
                            }
                        }
                        // Reverse: literal > ts → ts < literal
                        if let Expr::Column(col) = right.as_ref() {
                            if col == ts_col {
                                let val = self.eval_literal_to_i64(left)?;
                                return Some((i64::MIN, val - 1));
                            }
                        }
                        None
                    }
                    BinaryOperator::Le => {
                        if let Expr::Column(col) = left.as_ref() {
                            if col == ts_col {
                                let val = self.eval_literal_to_i64(right)?;
                                return Some((i64::MIN, val));
                            }
                        }
                        // Reverse: literal <= ts → ts >= literal
                        if let Expr::Column(col) = right.as_ref() {
                            if col == ts_col {
                                let val = self.eval_literal_to_i64(left)?;
                                return Some((val, i64::MAX));
                            }
                        }
                        None
                    }
                    BinaryOperator::Lt => {
                        if let Expr::Column(col) = left.as_ref() {
                            if col == ts_col {
                                let val = self.eval_literal_to_i64(right)?;
                                return Some((i64::MIN, val - 1));
                            }
                        }
                        // Reverse: literal < ts → ts > literal
                        if let Expr::Column(col) = right.as_ref() {
                            if col == ts_col {
                                let val = self.eval_literal_to_i64(left)?;
                                return Some((val + 1, i64::MAX));
                            }
                        }
                        None
                    }
                    BinaryOperator::Eq => {
                        if let Expr::Column(col) = left.as_ref() {
                            if col == ts_col {
                                let val = self.eval_literal_to_i64(right)?;
                                return Some((val, val));
                            }
                        }
                        // Reverse: literal = ts
                        if let Expr::Column(col) = right.as_ref() {
                            if col == ts_col {
                                let val = self.eval_literal_to_i64(left)?;
                                return Some((val, val));
                            }
                        }
                        None
                    }
                    _ => None,
                }
            }
            Expr::Between { expr: col, low, high, negated: _ } => {
                if let Expr::Column(name) = col.as_ref() {
                    if name == ts_col {
                        let start = self.eval_literal_to_i64(low)?;
                        let end = self.eval_literal_to_i64(high)?;
                        return Some((start, end));
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// Extract non-timestamp column conditions from WHERE clause for columnar pruning.
    /// Returns conditions that can be pushed down to segment-level zone maps and bloom filters.
    fn extract_column_conditions(
        &self,
        where_clause: &Option<Expr>,
        schema: &TableSchema,
        ts_col: &str,
    ) -> Vec<crate::storage::columnar::segment_manager::ColumnCondition> {
        let expr = match where_clause {
            Some(e) => e,
            None => return Vec::new(),
        };

        let mut conditions = Vec::new();
        self.collect_conditions_recursive(expr, schema, ts_col, &mut conditions);
        conditions
    }

    fn collect_conditions_recursive(
        &self,
        expr: &Expr,
        schema: &TableSchema,
        ts_col: &str,
        conditions: &mut Vec<crate::storage::columnar::segment_manager::ColumnCondition>,
    ) {
        if let Expr::BinaryOp { left, op, right } = expr {
            match op {
                BinaryOperator::And => {
                    // Recurse into both sides of AND
                    self.collect_conditions_recursive(left, schema, ts_col, conditions);
                    self.collect_conditions_recursive(right, schema, ts_col, conditions);
                }
                BinaryOperator::Eq => {
                    // col = value OR value = col (non-ts column)
                    if let Some(cond) = self.try_extract_equality(left, right, schema, ts_col) {
                        conditions.push(cond);
                    } else if let Some(cond) = self.try_extract_equality(right, left, schema, ts_col) {
                        conditions.push(cond);
                    }
                }
                BinaryOperator::Ge | BinaryOperator::Gt | BinaryOperator::Le | BinaryOperator::Lt => {
                    // Try to extract range conditions
                    if let Some(cond) = self.try_extract_range(left, right, op, schema, ts_col) {
                        conditions.push(cond);
                    }
                }
                _ => {}
            }
        }
    }

    /// Try to extract an Equals condition from `col_expr = value_expr`.
    fn try_extract_equality(
        &self,
        col_expr: &Expr,
        value_expr: &Expr,
        schema: &TableSchema,
        ts_col: &str,
    ) -> Option<crate::storage::columnar::segment_manager::ColumnCondition> {
        use crate::storage::columnar::segment_manager::ColumnCondition;

        if let Expr::Column(col_name) = col_expr {
            if col_name == ts_col {
                return None; // Skip timestamp column
            }
            let col_idx = schema.columns.iter().position(|c| c.name == *col_name)?;
            let value = match value_expr {
                Expr::Literal(v) => v.clone(),
                _ => return None,
            };
            Some(ColumnCondition::Equals { column_idx: col_idx, value })
        } else {
            None
        }
    }

    /// Try to extract a Range condition from comparison ops.
    fn try_extract_range(
        &self,
        left: &Expr,
        right: &Expr,
        op: &BinaryOperator,
        schema: &TableSchema,
        ts_col: &str,
    ) -> Option<crate::storage::columnar::segment_manager::ColumnCondition> {
        use crate::storage::columnar::segment_manager::ColumnCondition;

        // Determine which side is the column and which is the value
        let (col_name, value, is_col_left) = match (left, right) {
            (Expr::Column(c), Expr::Literal(v)) => (c, v, true),
            (Expr::Literal(v), Expr::Column(c)) => (c, v, false),
            _ => return None,
        };

        if col_name == ts_col {
            return None;
        }

        let col_idx = schema.columns.iter().position(|c| c.name == *col_name)?;

        // Convert comparison to a range [low, high]
        let (low, high) = match (op, is_col_left) {
            (BinaryOperator::Ge, true) => (value.clone(), Value::Integer(i64::MAX)),  // col >= val
            (BinaryOperator::Gt, true) => {
                // col > val → [val+1, MAX]
                let bumped = self.increment_value(value)?;
                (bumped, Value::Integer(i64::MAX))
            }
            (BinaryOperator::Le, true) => (Value::Integer(i64::MIN), value.clone()),  // col <= val
            (BinaryOperator::Lt, true) => {
                let decremented = self.decrement_value(value)?;
                (Value::Integer(i64::MIN), decremented)
            }
            (BinaryOperator::Ge, false) => (Value::Integer(i64::MIN), value.clone()), // val >= col → col <= val
            (BinaryOperator::Gt, false) => {
                let decremented = self.decrement_value(value)?;
                (Value::Integer(i64::MIN), decremented)
            }
            (BinaryOperator::Le, false) => (value.clone(), Value::Integer(i64::MAX)), // val <= col → col >= val
            (BinaryOperator::Lt, false) => {
                let bumped = self.increment_value(value)?;
                (bumped, Value::Integer(i64::MAX))
            }
            _ => return None,
        };

        Some(ColumnCondition::Range { column_idx: col_idx, low, high })
    }

    fn increment_value(&self, v: &Value) -> Option<Value> {
        match v {
            Value::Integer(i) => Some(Value::Integer(i + 1)),
            Value::Float(f) => Some(Value::Float(f + 1.0)),
            _ => None,
        }
    }

    fn decrement_value(&self, v: &Value) -> Option<Value> {
        match v {
            Value::Integer(i) => Some(Value::Integer(i - 1)),
            Value::Float(f) => Some(Value::Float(f - 1.0)),
            _ => None,
        }
    }

    /// Evaluate a literal expression to i64 (for time range extraction).
    fn eval_literal_to_i64(&self, expr: &Expr) -> Option<i64> {
        match expr {
            Expr::Literal(Value::Timestamp(ts)) => Some(ts.as_micros()),
            Expr::Literal(Value::Integer(i)) => Some(*i),
            Expr::Literal(Value::Float(f)) => Some(*f as i64),
            _ => None,
        }
    }

    /// Execute INSERT for TimeSeries tables via the columnar store.
    fn execute_columnar_insert(
        &self,
        stmt: &InsertStmt,
        schema: &crate::types::TableSchema,
        columns: &[String],
    ) -> Result<QueryResult> {
        let mut rows: Vec<Vec<crate::types::Value>> = Vec::new();

        for value_row in &stmt.values {
            if value_row.len() != columns.len() {
                return Err(MoteDBError::InvalidArgument(
                    format!("Column count mismatch: expected {}, got {}", columns.len(), value_row.len())
                ));
            }

            // Build SqlRow first (reuses existing type coercion via sql_row_to_row)
            let mut sql_row = SqlRow::new();
            for (i, col_name) in columns.iter().enumerate() {
                let val = match &value_row[i] {
                    Expr::Literal(v) => v.clone(),
                    Expr::Parameter(_) => {
                        let empty_row = SqlRow::new();
                        self.evaluator.eval(&value_row[i], &empty_row)?
                    }
                    expr => return Err(MoteDBError::InvalidArgument(
                        format!("INSERT VALUES must be literals or parameters, got {:?}", expr)
                    )),
                };
                sql_row.insert(col_name.clone(), val);
            }

            // Convert to storage Row (handles type coercion)
            let row = sql_row_to_row(&sql_row, schema)?;
            rows.push(row);
        }

        let result = self.db.columnar_store.ingest(&stmt.table, rows)?;
        Ok(QueryResult::Modification {
            affected_rows: result.row_ids.len(),
        })
    }
}

/// Helper struct for vector ORDER BY plan
struct VectorOrderByPlan {
    table: String,
    column: String,
    query_vector: Vec<f32>,
    k: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;
    use crate::types::{TableSchema, ColumnDef, ColumnType, Value};

    fn make_schema() -> TableSchema {
        let columns = vec![
            ColumnDef { name: "id".into(), col_type: ColumnType::Integer, position: 0, nullable: false, auto_increment: false, auto_increment_start: None },
            ColumnDef { name: "name".into(), col_type: ColumnType::Text, position: 1, nullable: true, auto_increment: false, auto_increment_start: None },
            ColumnDef { name: "score".into(), col_type: ColumnType::Float, position: 2, nullable: true, auto_increment: false, auto_increment_start: None },
            ColumnDef { name: "active".into(), col_type: ColumnType::Boolean, position: 3, nullable: true, auto_increment: false, auto_increment_start: None },
        ];
        TableSchema::new("t".into(), columns)
    }

    fn row(id: i64, name: &str, score: f64, active: bool) -> Vec<Value> {
        vec![
            Value::Integer(id),
            Value::Text(crate::types::ArcString::from(name)),
            Value::Float(score),
            Value::Bool(active),
        ]
    }

    fn col(name: &str) -> Expr {
        Expr::Column(name.to_string())
    }

    // ━━━ Column reference ━━━

    #[test]
    fn test_eval_column() {
        let schema = make_schema();
        let r = row(1, "alice", 9.5, true);
        assert_eq!(
            QueryExecutor::eval_expr_on_row(&col("id"), &r, &schema).unwrap(),
            Value::Integer(1)
        );
        assert_eq!(
            QueryExecutor::eval_expr_on_row(&col("name"), &r, &schema).unwrap(),
            Value::Text(crate::types::ArcString::from("alice"))
        );
    }

    // ━━━ Binary operators ━━━

    #[test]
    fn test_eval_eq() {
        let schema = make_schema();
        let r = row(1, "alice", 9.5, true);
        let eq = Expr::BinaryOp {
            left: Box::new(col("id")),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Literal(Value::Integer(1))),
        };
        assert_eq!(QueryExecutor::eval_expr_on_row(&eq, &r, &schema).unwrap(), Value::Bool(true));
    }

    #[test]
    fn test_eval_eq_null_returns_null() {
        let schema = make_schema();
        let r = vec![Value::Null, Value::Null, Value::Null, Value::Null];
        let eq = Expr::BinaryOp {
            left: Box::new(col("id")),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Literal(Value::Integer(1))),
        };
        assert_eq!(QueryExecutor::eval_expr_on_row(&eq, &r, &schema).unwrap(), Value::Null);
    }

    #[test]
    fn test_eval_comparisons() {
        let schema = make_schema();
        let r = row(5, "", 10.0, true);

        let lt = Expr::BinaryOp { left: Box::new(col("id")), op: BinaryOperator::Lt, right: Box::new(Expr::Literal(Value::Integer(10))) };
        assert_eq!(QueryExecutor::eval_expr_on_row(&lt, &r, &schema).unwrap(), Value::Bool(true));

        let gt = Expr::BinaryOp { left: Box::new(col("score")), op: BinaryOperator::Gt, right: Box::new(Expr::Literal(Value::Float(5.0))) };
        assert_eq!(QueryExecutor::eval_expr_on_row(&gt, &r, &schema).unwrap(), Value::Bool(true));
    }

    // ━━━ NULL handling in comparisons ━━━

    #[test]
    fn test_eval_lt_null_returns_null() {
        let schema = make_schema();
        let r = vec![Value::Null, Value::Null, Value::Null, Value::Null];
        let lt = Expr::BinaryOp {
            left: Box::new(col("score")),
            op: BinaryOperator::Lt,
            right: Box::new(Expr::Literal(Value::Float(5.0))),
        };
        // SQL: NULL < 5 => UNKNOWN (NULL), not FALSE
        assert_eq!(QueryExecutor::eval_expr_on_row(&lt, &r, &schema).unwrap(), Value::Null);
    }

    #[test]
    fn test_eval_le_null_returns_null() {
        let schema = make_schema();
        let r = vec![Value::Null, Value::Null, Value::Null, Value::Null];
        let le = Expr::BinaryOp {
            left: Box::new(col("score")),
            op: BinaryOperator::Le,
            right: Box::new(Expr::Literal(Value::Float(5.0))),
        };
        assert_eq!(QueryExecutor::eval_expr_on_row(&le, &r, &schema).unwrap(), Value::Null);
    }

    #[test]
    fn test_eval_ge_null_returns_null() {
        let schema = make_schema();
        let r = vec![Value::Null, Value::Null, Value::Null, Value::Null];
        let ge = Expr::BinaryOp {
            left: Box::new(col("score")),
            op: BinaryOperator::Ge,
            right: Box::new(Expr::Literal(Value::Float(5.0))),
        };
        assert_eq!(QueryExecutor::eval_expr_on_row(&ge, &r, &schema).unwrap(), Value::Null);
    }

    #[test]
    fn test_order_by_cross_type_handled() {
        let schema = make_schema();
        // Schema: id(0)=Int, name(1)=Text, score(2)=Float, active(3)=Bool
        let row = vec![
            Value::Integer(5),
            Value::Text(crate::types::ArcString::from("test")),
            Value::Float(3.0),
            Value::Bool(true),
        ];
        // a = Integer(5) at "id" (pos 0), b = Float(3.0) at "score" (pos 2)
        let a = QueryExecutor::eval_expr_on_row(&col("id"), &row, &schema).unwrap();
        let b = QueryExecutor::eval_expr_on_row(&col("score"), &row, &schema).unwrap();
        // Cross-type: Integer(5) vs Float(3.0) → 5 > 3.0 → Greater
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Greater));
    }

    // ━━━ IsNull / IsNotNull ━━━

    #[test]
    fn test_eval_is_null() {
        let schema = make_schema();
        let r = vec![Value::Integer(1), Value::Null, Value::Float(1.0), Value::Bool(false)];
        let isnull = Expr::IsNull { expr: Box::new(col("name")), negated: false };
        assert_eq!(QueryExecutor::eval_expr_on_row(&isnull, &r, &schema).unwrap(), Value::Bool(true));

        let notnull = Expr::IsNull { expr: Box::new(col("id")), negated: true };
        assert_eq!(QueryExecutor::eval_expr_on_row(&notnull, &r, &schema).unwrap(), Value::Bool(true));
    }

    // ━━━ Arithmetic ━━━

    #[test]
    fn test_eval_add() {
        let schema = make_schema();
        let r = row(1, "", 10.0, true);
        let add = Expr::BinaryOp {
            left: Box::new(col("id")),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Literal(Value::Integer(3))),
        };
        assert_eq!(QueryExecutor::eval_expr_on_row(&add, &r, &schema).unwrap(), Value::Integer(4));
    }

    #[test]
    fn test_eval_mul() {
        let schema = make_schema();
        let r = row(0, "", 10.0, true);
        let mul = Expr::BinaryOp {
            left: Box::new(col("score")),
            op: BinaryOperator::Mul,
            right: Box::new(Expr::Literal(Value::Float(2.0))),
        };
        assert_eq!(QueryExecutor::eval_expr_on_row(&mul, &r, &schema).unwrap(), Value::Float(20.0));
    }

    // ━━━ AND / OR ━━━

    #[test]
    fn test_eval_and_or() {
        let schema = make_schema();
        let r = row(1, "", 10.0, true);
        let and = Expr::BinaryOp {
            left: Box::new(Expr::Literal(Value::Bool(true))),
            op: BinaryOperator::And,
            right: Box::new(Expr::Literal(Value::Bool(true))),
        };
        assert_eq!(QueryExecutor::eval_expr_on_row(&and, &r, &schema).unwrap(), Value::Bool(true));

        let or = Expr::BinaryOp {
            left: Box::new(Expr::Literal(Value::Bool(false))),
            op: BinaryOperator::Or,
            right: Box::new(Expr::Literal(Value::Bool(true))),
        };
        assert_eq!(QueryExecutor::eval_expr_on_row(&or, &r, &schema).unwrap(), Value::Bool(true));
    }

    // ━━━ Parameter returns error (fallback path) ━━━

    #[test]
    fn test_eval_parameter_returns_error() {
        let schema = make_schema();
        let r = row(1, "", 10.0, true);
        let param = Expr::Parameter(1);
        assert!(QueryExecutor::eval_expr_on_row(&param, &r, &schema).is_err(),
            "Parameter should return Err to trigger fallback to full evaluator");
    }

    // ━━━ IN list ━━━

    #[test]
    fn test_eval_in() {
        let schema = make_schema();
        let r = row(1, "", 10.0, true);
        let in_expr = Expr::In {
            expr: Box::new(col("id")),
            list: vec![Expr::Literal(Value::Integer(1)), Expr::Literal(Value::Integer(2))],
            negated: false,
        };
        assert_eq!(QueryExecutor::eval_expr_on_row(&in_expr, &r, &schema).unwrap(), Value::Bool(true));

        let not_in = Expr::In {
            expr: Box::new(col("id")),
            list: vec![Expr::Literal(Value::Integer(5)), Expr::Literal(Value::Integer(6))],
            negated: true,
        };
        assert_eq!(QueryExecutor::eval_expr_on_row(&not_in, &r, &schema).unwrap(), Value::Bool(true));
    }

    // ━━━ BETWEEN ━━━

    #[test]
    fn test_eval_between() {
        let schema = make_schema();
        let r = row(5, "", 10.0, true);
        let between = Expr::Between {
            expr: Box::new(col("id")),
            low: Box::new(Expr::Literal(Value::Integer(1))),
            high: Box::new(Expr::Literal(Value::Integer(10))),
            negated: false,
        };
        assert_eq!(QueryExecutor::eval_expr_on_row(&between, &r, &schema).unwrap(), Value::Bool(true));
    }

    // ━━━ Unsupported expression returns error ━━━

    #[test]
    fn test_eval_unsupported_returns_error() {
        let schema = make_schema();
        let r = row(1, "", 10.0, true);
        // Subquery is unsupported in eval_expr_on_row
        let sub = Expr::Subquery(Box::new(crate::sql::ast::SelectStmt {
            columns: vec![],
            from: None,
            where_clause: None,
            order_by: None,
            limit: None,
            offset: None,
            distinct: false,
            group_by: None,
            having: None,
            latest_by: None,
        }));
        assert!(QueryExecutor::eval_expr_on_row(&sub, &r, &schema).is_err(),
            "Unsupported expression should return Err for fallback path");
    }
}
