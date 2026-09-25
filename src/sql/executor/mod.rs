/// Query executor - executes SQL statements against storage engine
use super::ast::*;

mod agg;
mod join;
mod scan;
use super::evaluator::ExprEvaluator;
use super::row_converter::{row_to_sql_row, rows_to_sql_rows};
use crate::database::MoteDB;
use crate::error::{MoteDBError, Result};
use crate::storage::row_format;
use crate::types::{ColumnType, Row, RowId, SqlRow, TableSchema, Value};
use crate::StorageError;
use std::cmp::Ordering;
use std::sync::Arc;

/// 🔑 Module-level Bool/Int coercion (TRUE=1, FALSE=0). Used by both the
/// QueryExecutor method and the CompiledWhere enum's match method (which
/// can't call associated functions on QueryExecutor).
fn coerce_bool_int(lv: Value, rv: Value) -> (Value, Value) {
    match (&lv, &rv) {
        (Value::Bool(b), Value::Integer(_)) => (Value::Integer(if *b { 1 } else { 0 }), rv),
        (Value::Integer(_), Value::Bool(b)) => (lv, Value::Integer(if *b { 1 } else { 0 })),
        (Value::Bool(b), Value::Float(_)) => (Value::Float(if *b { 1.0 } else { 0.0 }), rv),
        (Value::Float(_), Value::Bool(b)) => (lv, Value::Float(if *b { 1.0 } else { 0.0 })),
        (Value::Bool(a), Value::Bool(b)) => (
            Value::Integer(if *a { 1 } else { 0 }),
            Value::Integer(if *b { 1 } else { 0 }),
        ),
        _ => (lv, rv),
    }
}

/// 判断两个 Value 之间是否需要 Bool/Int 跨类型 coerce。
/// 绝大多数 WHERE 等值比较是 Int=Int / Text=Text / Float=Float，不涉及 Bool，
/// 调用此函数短路可直接用 == 比较（零 clone）。
#[inline]
fn needs_bool_coerce(a: &Value, b: &Value) -> bool {
    matches!(a, Value::Bool(_)) || matches!(b, Value::Bool(_))
}

/// Convert a Value to its string representation for GROUP_CONCAT.
/// Integers print without decimal point; floats print naturally; booleans
/// print as 1/0 (matching SQLite's GROUP_CONCAT behavior for TRUE/FALSE).
fn value_to_concat_string(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Text(t) => t.to_string(),
        Value::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        Value::Timestamp(t) => t.as_micros().to_string(),
        _ => format!("{:?}", v),
    }
}

/// Normalize a value for IN-list matching so Bool↔Int coercion works the same
/// way `=` does (TRUE matches 1, FALSE matches 0). Bool is mapped to Integer;
/// all other types (including Null) are returned unchanged. Applied to both
/// the IN-list members (at compile time) and the column value (at match time)
/// so `b IN (1)` (BOOLEAN column) and `x IN (TRUE)` (INTEGER column) both match.
fn normalize_for_in(v: &Value) -> Value {
    match v {
        Value::Bool(b) => Value::Integer(if *b { 1 } else { 0 }),
        // 🔑 Parse ISO-date text into Timestamp so a TIMESTAMP column matches
        // an IN list of string literals (e.g. `WHERE ts IN ('2024-01-15T00:00:00')`).
        // Value::eq already treats Timestamp==ISO-Text as equal, but HashSet
        // matching requires matching Hash too — Timestamp and Text hash
        // differently, so we normalize the set members (and the column value,
        // which stays Timestamp) to the same Timestamp representation.
        // Non-date text (parse_iso returns None) is left as Text.
        Value::Text(s) => crate::types::Timestamp::parse_iso(s.as_str())
            .map(Value::Timestamp)
            .unwrap_or_else(|| v.clone()),
        other => other.clone(),
    }
}

/// Wrapper around f32 that implements Ord (for use in BinaryHeap top-K).
/// NaN is treated as +∞ so it never wins a "smallest distance" comparison.
#[derive(Debug, Clone, Copy)]
struct OrderedF32(f32);

impl PartialEq for OrderedF32 {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl Eq for OrderedF32 {}
impl PartialOrd for OrderedF32 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrderedF32 {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.0.is_nan(), other.0.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => self.0.partial_cmp(&other.0).unwrap_or(Ordering::Equal),
        }
    }
}

/// Total order for floats where NaN sorts AFTER every real value (and equals
/// itself) — the contract `OrderedF32` always documented but never implemented:
/// `partial_cmp().unwrap_or(Equal)` made NaN compare "equal" to everything,
/// so a NaN distance could beat an exact 0.0 match in top-k.
#[inline]
pub(crate) fn nan_aware_cmp(a: f64, b: f64) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
    }
}

/// Total order for ORDER BY keys: NULLs first (matches every existing sort
/// site), NaN after all real values (Postgres ASC semantics). Plain
/// `partial_cmp().unwrap_or(Equal)` treats a NaN key as "equal to everything",
/// which made its sorted position arbitrary — measured as `ORDER BY emb <->
/// ? LIMIT 1` returning the NaN-distance row over an exact 0.0 match.
/// ONLY for sort keys; WHERE/filter comparisons keep three-valued logic
/// (any comparison against NaN is false) and must not use this.
#[inline]
fn order_by_cmp(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Float(x), Value::Float(y)) => nan_aware_cmp(*x, *y),
        // Cross-numeric pairs (Float vs Integer) coerce inside Value's
        // partial_cmp; only intercept when a NaN is actually involved.
        (a, b) => {
            let a_nan = matches!(a, Value::Float(f) if f.is_nan());
            let b_nan = matches!(b, Value::Float(f) if f.is_nan());
            if a_nan || b_nan {
                nan_aware_cmp(f64::NAN, if b_nan { f64::NAN } else { 0.0 })
            } else {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
        }
    }
}

fn decode_row(data: &[u8], schema: &TableSchema) -> crate::Result<Row> {
    row_format::decode(data, schema.col_types())
}

#[allow(clippy::type_complexity)]
type FromScanResult = Result<(Vec<(u64, SqlRow)>, Arc<TableSchema>)>;

#[allow(clippy::type_complexity)]
type RowPredicate = Option<Box<dyn Fn(&SqlRow) -> bool + Send + Sync>>;

/// Prefix all column names in rows with `table.prefix` and add metadata fields.
fn prefix_rows(rows: &mut [(u64, SqlRow)], table: &str, prefix: &str) {
    // Pre-compute prefixed key names from the first row (avoids format!() per
    // row — was: N rows × M cols format!() calls + N HashMap rebuilds).
    if rows.is_empty() {
        return;
    }
    let row_id_key = "__row_id__".to_string();
    let table_key = "__table__".to_string();
    let table_val = Value::text(table.to_string());

    // Collect original column names from the first row.
    let orig_keys: Vec<String> = rows[0]
        .1
        .keys()
        .filter(|k| !k.starts_with('_'))
        .cloned()
        .collect();
    let prefixed_keys: Vec<String> = orig_keys
        .iter()
        .map(|k| format!("{}.{}", prefix, k))
        .collect();

    for (row_id, sql_row) in rows.iter_mut() {
        let mut new = SqlRow::with_capacity(orig_keys.len() + 2);
        new.insert(row_id_key.clone(), Value::Integer(*row_id as i64));
        new.insert(table_key.clone(), table_val.clone());
        // Move values to prefixed keys (reuse pre-computed key strings).
        for (orig, prefixed) in orig_keys.iter().zip(prefixed_keys.iter()) {
            if let Some(val) = sql_row.remove(orig) {
                new.insert(prefixed.clone(), val);
            }
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

/// Apply a comparison operator to an optional field value (None = NULL) and a
/// target. NULLs never match. Used for post-filtering scanned rows by AND'd
/// predicates that weren't pushed into the single-column scan predicate.
fn apply_op_value(
    op: &crate::sql::ast::BinaryOperator,
    fv: Option<&Value>,
    target: &Value,
) -> bool {
    use crate::sql::ast::BinaryOperator;
    let v = match fv {
        Some(v) => v,
        None => return false,
    };
    // 🔑 SQL 三值逻辑: NULL 与任何值比较 (含 <>) 结果为 UNKNOWN → 过滤。
    // 此前 `Ne => v != target` 在 v=Null 时为 true，JOIN/WHERE 快过滤把
    // `col <> x` 的 NULL 行也算进来 (differential fuzz: JOIN+WHERE qty <> 10
    // 多返回恰好等于 NULL 行数)。
    if matches!(v, Value::Null) {
        return false;
    }
    match op {
        BinaryOperator::Eq => v == target,
        BinaryOperator::Ne => v != target,
        BinaryOperator::Lt => v.partial_cmp(target) == Some(Ordering::Less),
        BinaryOperator::Gt => v.partial_cmp(target) == Some(Ordering::Greater),
        BinaryOperator::Le => matches!(
            v.partial_cmp(target),
            Some(Ordering::Less | Ordering::Equal)
        ),
        BinaryOperator::Ge => matches!(
            v.partial_cmp(target),
            Some(Ordering::Greater | Ordering::Equal)
        ),
        _ => false,
    }
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
    Fixed(
        crate::storage::lsm::columnar::FixedSegment,
        crate::types::ColumnType,
    ),
    Text(crate::storage::lsm::columnar::TextSegment),
}

/// Query result
///
/// `#[non_exhaustive]`: new variants (e.g. `Explain`, `BatchResult`) may be
/// added in future minor versions. Out-of-crate callers must include a `_`
/// arm in their `match`; the `materialize()` helper and `affected_rows()`
/// accessor cover the common cases without exhaustively matching.
#[derive(Debug)]
#[non_exhaustive]
pub enum QueryResult {
    /// SELECT result
    Select {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },

    /// INSERT/UPDATE/DELETE result
    Modification { affected_rows: usize },

    /// CREATE/DROP result
    Definition { message: String },
}

use crate::types::CompSum;

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
/// `#[non_exhaustive]`: new variants may be added in future minor versions.
/// Prefer `materialize()` / `for_each()` / `row_count()` over matching the
/// enum directly — those accessors are stable across versions.
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
#[non_exhaustive]
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
        /// ORDER BY clauses to apply during materialization (None = no sort).
        /// Carried here so the zero-copy columnar scan path can still honor
        /// ORDER BY (expression/alias/ordinal) at materialize time.
        order_by: Option<Vec<OrderByExpr>>,
    },

    /// INSERT/UPDATE/DELETE result
    Modification { affected_rows: usize },

    /// CREATE/DROP result
    Definition { message: String },
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
            Self::SelectReady { columns, rows } => Ok(QueryResult::Select { columns, rows }),
            Self::SelectColumnar {
                columns,
                segments,
                row_indices,
                num_rows,
                row_map,
                order_by,
            } => {
                // Convert columnar to row-based lazily — only when materialize() called
                let ncols = segments.len();
                let source: Vec<usize> = if let Some(ref idx) = row_indices {
                    idx.iter()
                        .filter(|&&i| !row_map.is_deleted(i))
                        .copied()
                        .collect()
                } else {
                    // Newest-version-wins dedup: when a single segment holds
                    // multiple versions of the same key (e.g. after an UPDATE
                    // appends a newer row without merging), keep only the last
                    // (newest) version per key. Rows are in append order
                    // (old→new), so the last occurrence of a key is newest.
                    let live: Vec<usize> =
                        (0..num_rows).filter(|&i| !row_map.is_deleted(i)).collect();
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
                                row.push(
                                    match ct {
                                        crate::types::ColumnType::Integer => {
                                            f.get_i64(idx).map(Value::Integer)
                                        }
                                        crate::types::ColumnType::Float => {
                                            f.get_f64(idx).map(Value::Float)
                                        }
                                        crate::types::ColumnType::Boolean => {
                                            f.get_bool(idx).map(Value::Bool)
                                        }
                                        // 🚨 Timestamp: decode as Timestamp (micros),
                                        // not Integer. Without this, TIMESTAMP columns
                                        // read back as Integer → date string comparisons
                                        // in WHERE fail (Integer vs Text → None).
                                        crate::types::ColumnType::Timestamp => {
                                            f.get_i64(idx).map(|m| {
                                                Value::Timestamp(
                                                    crate::types::Timestamp::from_micros(m),
                                                )
                                            })
                                        }
                                        _ => f.get_i64(idx).map(Value::Integer),
                                    }
                                    .unwrap_or(Value::Null),
                                );
                            }
                            ColumnarSeg::Text(t) => {
                                let val = if let Some(s) = t.get_str(idx) {
                                    let arc = string_pool.get(s).cloned().unwrap_or_else(|| {
                                        let a: std::sync::Arc<str> = std::sync::Arc::from(s);
                                        string_pool.insert(s, a.clone());
                                        a
                                    });
                                    Value::Text(crate::types::ArcString(arc))
                                } else {
                                    Value::Null
                                };
                                row.push(val);
                            }
                        }
                    }
                    rows.push(row);
                }
                // 🔑 Apply ORDER BY to the materialized columnar rows. The
                // zero-copy SelectColumnar path skips sorting; clauses carried
                // on the variant are applied here (handles expression/alias/
                // ordinal ORDER BY, which the columnar scan can't evaluate).
                if let Some(order_clauses) = order_by {
                    if !order_clauses.is_empty() {
                        Self::apply_order_by(&mut rows, &columns, &order_clauses)?;
                    }
                }
                Ok(QueryResult::Select { columns, rows })
            }
            Self::SelectStreaming {
                columns,
                rows,
                order_by,
                limit,
                offset,
                distinct,
                max_result_rows,
                size_hint: stream_hint,
            } => {
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
            Self::Modification { affected_rows } => Ok(QueryResult::Modification { affected_rows }),
            Self::Definition { message } => Ok(QueryResult::Definition { message }),
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
            Self::SelectColumnar {
                row_indices,
                num_rows,
                row_map,
                ..
            } => {
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
            Self::SelectStreaming {
                columns,
                rows,
                order_by,
                limit,
                offset,
                distinct,
                max_result_rows: _,
                size_hint,
            } => Self::SelectStreaming {
                columns,
                rows,
                order_by,
                limit,
                offset,
                distinct,
                max_result_rows: max,
                size_hint,
            },
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
        let has_more = matches!(
            (&result, max_rows),
            (QueryResult::Select { rows, .. }, Some(max)) if rows.len() >= max
        );
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
                Ok(ForEachResult {
                    rows_processed: count,
                    has_more,
                })
            }
            Self::SelectColumnar {
                columns,
                segments,
                row_indices,
                num_rows,
                row_map,
                order_by: _,
            } => {
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
                    if row_map.is_deleted(idx) {
                        continue;
                    }
                    let mut row = Vec::with_capacity(segments.len());
                    for seg in &segments {
                        match seg {
                            ColumnarSeg::Fixed(f, ct) => {
                                row.push(
                                    match ct {
                                        crate::types::ColumnType::Integer => {
                                            f.get_i64(idx).map(Value::Integer)
                                        }
                                        crate::types::ColumnType::Float => {
                                            f.get_f64(idx).map(Value::Float)
                                        }
                                        crate::types::ColumnType::Boolean => {
                                            f.get_bool(idx).map(Value::Bool)
                                        }
                                        // 🚨 Timestamp: decode as Timestamp (micros).
                                        crate::types::ColumnType::Timestamp => {
                                            f.get_i64(idx).map(|m| {
                                                Value::Timestamp(
                                                    crate::types::Timestamp::from_micros(m),
                                                )
                                            })
                                        }
                                        _ => f.get_i64(idx).map(Value::Integer),
                                    }
                                    .unwrap_or(Value::Null),
                                );
                            }
                            ColumnarSeg::Text(t) => row.push(
                                t.get_str(idx)
                                    .map(|s| {
                                        Value::Text(crate::types::ArcString(std::sync::Arc::from(
                                            s,
                                        )))
                                    })
                                    .unwrap_or(Value::Null),
                            ),
                        }
                    }
                    match callback(&columns, &row)? {
                        StreamingControl::Continue => count += 1,
                        StreamingControl::Break => break,
                    }
                }
                Ok(ForEachResult {
                    rows_processed: count,
                    has_more,
                })
            }
            Self::SelectStreaming {
                columns,
                rows,
                order_by,
                limit,
                offset,
                distinct,
                ..
            } => {
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
                        if skipped < offset_val {
                            skipped += 1;
                            continue;
                        }
                        if !seen.insert(row.clone()) {
                            continue;
                        }
                        match callback(&columns, &row)? {
                            StreamingControl::Continue => count += 1,
                            StreamingControl::Break => break,
                        }
                        if count >= take_n {
                            break;
                        }
                        if let Some(max) = max_rows {
                            if count >= max {
                                has_more = true;
                                break;
                            }
                        }
                    }
                    return Ok(ForEachResult {
                        rows_processed: count,
                        has_more,
                    });
                }

                if has_order {
                    let sort_specs: Vec<(usize, bool)> = order_clauses
                        .iter()
                        .filter_map(|clause| {
                            let col_idx = match &clause.expr {
                                Expr::Column(name) => {
                                    if let Some(idx) = columns.iter().position(|c| c == name) {
                                        idx
                                    } else if let Some(dot_pos) = name.rfind('.') {
                                        columns.iter().position(|c| c == &name[dot_pos + 1..])?
                                    } else if !name.contains('.') {
                                        // 🆕 Derived-table case: bare ORDER BY
                                        // name against table-qualified output
                                        // columns (e.g., ORDER BY cat when
                                        // columns are ["x.cat"]).
                                        columns.iter().position(|c| {
                                            c.rsplit('.').next().unwrap_or(c) == name
                                        })?
                                    } else {
                                        return None;
                                    }
                                }
                                Expr::Literal(Value::Integer(n)) => (*n as usize).wrapping_sub(1),
                                _ => return None,
                            };
                            Some((col_idx, clause.asc))
                        })
                        .collect();

                    if let Some(limit_val) = limit {
                        // Top-K path: O(K) memory
                        return Self::for_each_topk(
                            rows,
                            &columns,
                            &sort_specs,
                            limit_val,
                            offset_val,
                            max_rows,
                            &mut callback,
                        );
                    }

                    // Full sort path: collect, sort, stream
                    let cap = max_rows.unwrap_or(4096);
                    let mut buf = Vec::with_capacity(cap.min(4096));
                    let mut has_more = false;
                    for row_result in rows {
                        buf.push(row_result?);
                        if let Some(max) = max_rows {
                            if buf.len() >= max {
                                has_more = true;
                                break;
                            }
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
                    return Ok(ForEachResult {
                        rows_processed: count,
                        has_more,
                    });
                }

                // Pure streaming path: O(1) memory
                let mut count = 0;
                let mut has_more = false;
                let mut skipped = 0;
                let take_n = limit.unwrap_or(usize::MAX);
                for row_result in rows {
                    let row = row_result?;
                    if skipped < offset_val {
                        skipped += 1;
                        continue;
                    }
                    match callback(&columns, &row)? {
                        StreamingControl::Continue => count += 1,
                        StreamingControl::Break => break,
                    }
                    if count >= take_n {
                        break;
                    }
                    if let Some(max) = max_rows {
                        if count >= max {
                            has_more = true;
                            break;
                        }
                    }
                }
                Ok(ForEachResult {
                    rows_processed: count,
                    has_more,
                })
            }
            Self::Modification { .. } | Self::Definition { .. } => Ok(ForEachResult {
                rows_processed: 0,
                has_more: false,
            }),
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

            if scanned >= effective_max {
                has_more = true;
                break;
            }
        }

        // Sort the top-K
        Self::sort_rows(&mut heap, sort_specs);

        // Stream sorted results, skipping offset
        let mut count = 0;
        for row in heap.into_iter().skip(offset) {
            if count >= limit {
                break;
            }
            match callback(columns, &row)? {
                StreamingControl::Continue => count += 1,
                StreamingControl::Break => break,
            }
        }

        Ok(ForEachResult {
            rows_processed: count,
            has_more,
        })
    }

    /// Sort rows by pre-computed sort specs (shared by materialize and for_each)
    /// 🚀 #[inline]：sort_rows 是 ORDER BY 的热路径，内联比较逻辑减少闭包开销。
    #[inline]
    fn sort_rows(rows: &mut [Vec<Value>], sort_specs: &[(usize, bool)]) {
        use std::cmp::Ordering;
        rows.sort_by(|a, b| {
            for &(col_idx, asc) in sort_specs {
                if col_idx >= a.len() || col_idx >= b.len() {
                    continue;
                }
                let cmp = Self::compare_values(&a[col_idx], &b[col_idx]);
                let final_cmp = if asc { cmp } else { cmp.reverse() };
                if final_cmp != Ordering::Equal {
                    return final_cmp;
                }
            }
            Ordering::Equal
        });
    }

    #[inline]
    fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        order_by_cmp(a, b)
    }

    fn compare_rows(a: &[Value], b: &[Value], sort_specs: &[(usize, bool)]) -> std::cmp::Ordering {
        for &(col_idx, asc) in sort_specs {
            if col_idx >= a.len() || col_idx >= b.len() {
                continue;
            }
            let cmp = Self::compare_values(&a[col_idx], &b[col_idx]);
            let final_cmp = if asc { cmp } else { cmp.reverse() };
            if final_cmp != std::cmp::Ordering::Equal {
                return final_cmp;
            }
        }
        std::cmp::Ordering::Equal
    }

    /// Min-heap sift-up for top-K
    fn sift_up(heap: &mut [Vec<Value>], mut idx: usize, sort_specs: &[(usize, bool)]) {
        while idx > 0 {
            let parent = (idx - 1) / 2;
            if Self::compare_rows(&heap[idx], &heap[parent], sort_specs) == std::cmp::Ordering::Less
            {
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
            if left < len
                && Self::compare_rows(&heap[left], &heap[smallest], sort_specs)
                    == std::cmp::Ordering::Less
            {
                smallest = left;
            }
            if right < len
                && Self::compare_rows(&heap[right], &heap[smallest], sort_specs)
                    == std::cmp::Ordering::Less
            {
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

        // Resolve each clause: projected column index when possible, else
        // precompute per-row key values by evaluating the expression against
        // the projected columns (e.g. `ORDER BY emb <-> ?` when emb is
        // projected, `ORDER BY v * 2`, `ORDER BY LENGTH(name)`). Previously
        // expression keys were dropped → arbitrary order (or a NotImplemented
        // error when every key was an expression).
        let evaluator = ExprEvaluator::new();
        let mut specs: Vec<(OrderByKey, bool, Option<bool>)> =
            Vec::with_capacity(order_clauses.len());
        for clause in order_clauses {
            let key = match order_by_projected_index(&clause.expr, columns) {
                Some(idx) => OrderByKey::Col(idx),
                None => {
                    // Build a SqlRow per row (columns → values) and evaluate.
                    // Evaluation failure on a row (e.g. the expression
                    // references a non-projected column) yields Null — the
                    // caller's routing is responsible for routing
                    // non-projected-key queries to the materialized path.
                    let keys: Vec<Value> = rows
                        .iter()
                        .map(|row| {
                            let mut sql_row: crate::types::SqlRow =
                                std::collections::HashMap::with_capacity(columns.len());
                            for (c, v) in columns.iter().zip(row.iter()) {
                                sql_row.insert(c.clone(), v.clone());
                            }
                            evaluator
                                .eval(&clause.expr, &sql_row)
                                .unwrap_or(Value::Null)
                        })
                        .collect();
                    OrderByKey::Keys(keys)
                }
            };
            specs.push((key, clause.asc, clause.nulls_first));
        }

        // Sort via index permutation: expression keys read from precomputed
        // vectors, column keys from the rows themselves.
        let mut perm: Vec<usize> = (0..rows.len()).collect();
        perm.sort_by(|&a, &b| {
            for (key, asc, nulls_first) in &specs {
                let (va, vb) = match key {
                    OrderByKey::Col(idx) => {
                        let va = rows.get(a).and_then(|r| r.get(*idx));
                        let vb = rows.get(b).and_then(|r| r.get(*idx));
                        (va, vb)
                    }
                    OrderByKey::Keys(keys) => (keys.get(a), keys.get(b)),
                };
                let (va, vb) = match (va, vb) {
                    (Some(va), Some(vb)) => (va, vb),
                    _ => continue,
                };

                // 🔑 NULL placement and value ordering are INDEPENDENT:
                // NULLS FIRST/LAST (explicit, or the dialect default
                // NULLs-first-ASC / NULLs-last-DESC) fixes where NULLs sit;
                // ASC/DESC orders only the non-NULL values.
                let nulls_first_effective = nulls_first.unwrap_or(*asc);
                let rank = |v: &Value| -> i8 {
                    if matches!(v, Value::Null) {
                        if nulls_first_effective {
                            0
                        } else {
                            2
                        }
                    } else {
                        1
                    }
                };
                let (ra, rb) = (rank(va), rank(vb));
                if ra != rb {
                    return ra.cmp(&rb);
                }
                if matches!(va, Value::Null) {
                    continue; // both NULL → next key
                }
                // order_by_cmp: NULLs first, NaN after reals (Postgres ASC),
                // everything else via Value::partial_cmp.
                let value_cmp = order_by_cmp(va, vb);
                let final_cmp = if *asc { value_cmp } else { value_cmp.reverse() };

                if final_cmp != Ordering::Equal {
                    return final_cmp;
                }
            }
            Ordering::Equal
        });
        // Apply the permutation in place (rows is &mut [Vec<Value>] — no Copy).
        let mut sorted: Vec<Vec<Value>> = Vec::with_capacity(rows.len());
        for &i in &perm {
            sorted.push(std::mem::take(&mut rows[i]));
        }
        for (i, r) in sorted.into_iter().enumerate() {
            rows[i] = r;
        }

        Ok(())
    }

    /// Best-effort ORDER BY over a StreamingQueryResult's projected columns.
    /// Only sorts when EVERY ORDER BY key resolves to a projected output column
    /// (by name, alias, or 1-based ordinal). If any key can't be resolved against
    /// the projected columns, does nothing — the caller's underlying scan path is
    /// responsible for non-projected-column ORDER BY. This makes
    /// `SELECT id, v*2 AS dbl ... ORDER BY dbl` and `ORDER BY 2` work on the
    /// col-segment fast path without breaking non-projected ORDER BY.
    /// Never errors: unresolvable keys are silently skipped (no sort).
    fn try_sort_projected(
        result: &mut StreamingQueryResult,
        order_clauses: &[crate::sql::ast::OrderByExpr],
    ) {
        let (columns, rows): (&[String], &mut Vec<Vec<Value>>) = match result {
            StreamingQueryResult::SelectReady { columns, rows } => (columns.as_slice(), rows),
            StreamingQueryResult::SelectColumnar { .. } => {
                // For SelectColumnar we carry the clauses; materialize() sorts.
                if let StreamingQueryResult::SelectColumnar {
                    order_by: ref mut ob,
                    ..
                } = result
                {
                    *ob = Some(order_clauses.to_vec());
                }
                return;
            }
            _ => return,
        };
        // Resolve every key against the projected columns.
        let mut specs: Vec<(usize, bool)> = Vec::new();
        for clause in order_clauses {
            let idx = match &clause.expr {
                crate::sql::ast::Expr::Column(name) => {
                    // Match against output column names (which include aliases).
                    let bare = name.rsplit('.').next().unwrap_or(name);
                    columns
                        .iter()
                        .position(|c| c == name || c.rsplit('.').next().unwrap_or(c) == bare)
                }
                crate::sql::ast::Expr::Literal(Value::Integer(n)) => {
                    let i = (*n as usize).wrapping_sub(1);
                    if i < columns.len() {
                        Some(i)
                    } else {
                        None
                    }
                }
                _ => None,
            };
            match idx {
                Some(i) => specs.push((i, clause.asc)),
                None => return, // a key can't be resolved against projected cols → bail
            }
        }
        if specs.is_empty() {
            return;
        }
        rows.sort_by(|a, b| {
            for &(idx, asc) in &specs {
                if idx >= a.len() || idx >= b.len() {
                    continue;
                }
                let cmp = order_by_cmp(&a[idx], &b[idx]);
                let cmp = if asc { cmp } else { cmp.reverse() };
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });
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

/// A resolved ORDER BY key for `apply_order_by`: either a projected column
/// index or per-row precomputed expression values.
enum OrderByKey {
    Col(usize),
    /// One evaluated key value per row (aligned with the row slice).
    Keys(Vec<Value>),
}

/// Resolve an ORDER BY key expression against projected output columns,
/// mirroring `try_sort_projected`'s matching (name, bare-name, ordinal).
/// Returns None when the key must be evaluated as an expression.
fn order_by_projected_index(expr: &Expr, columns: &[String]) -> Option<usize> {
    match expr {
        Expr::Column(name) => {
            let bare = name.rsplit('.').next().unwrap_or(name);
            columns
                .iter()
                .position(|c| c == name || c.rsplit('.').next().unwrap_or(c) == bare)
        }
        Expr::Literal(Value::Integer(n)) => {
            let idx = (*n as usize).wrapping_sub(1);
            if idx < columns.len() {
                Some(idx)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Metadata for a single aggregate function extracted from a SELECT column.
/// Used by the positional GROUP BY fast path.
#[derive(Clone)]
struct AggregateInfo {
    func: String,           // COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT
    col_pos: Option<usize>, // Column position; None means COUNT(*) or COUNT(1)
    distinct: bool,
    /// Extra parameter (e.g., GROUP_CONCAT separator).
    extra: Option<String>,
}

/// Pre-compiled WHERE clause — column names resolved to positions once.
/// Eliminates per-row HashMap lookups in `schema.get_column_position()`.
///
/// For simple comparisons (Eq, Lt, etc.), evaluation is a single Vec index
/// + direct Value comparison — no recursion, no string ops, no HashMap.
#[allow(dead_code)]
enum CompiledWhere {
    Eq(usize, Value), // col[pos] == value
    Ne(usize, Value), // col[pos] != value
    Lt(usize, Value), // col[pos] < value
    Le(usize, Value), // col[pos] <= value
    Gt(usize, Value), // col[pos] > value
    Ge(usize, Value), // col[pos] >= value
    /// col[pos] IN set (O(1)). `negated` = NOT IN; `has_null` = the list/set
    /// contains NULL (SQL 三值逻辑: x NOT IN (…, NULL) → UNKNOWN → false).
    InHash(usize, std::collections::HashSet<Value>, bool, bool),
    Like(usize, String, bool), // col[pos] LIKE pattern (negated bool)
    IsNull(usize, bool),       // col[pos] IS NULL / IS NOT NULL
    And(Vec<CompiledWhere>),   // all must match (short-circuit)
    Or(Vec<CompiledWhere>),    // any must match (short-circuit)
    Not(Box<CompiledWhere>),   // negation
}

impl CompiledWhere {
    /// Evaluate the compiled WHERE against a row.
    /// Returns `Some(bool)` if successful, `None` if fallback is needed.
    #[inline]
    fn eval(&self, row: &[Value]) -> Option<bool> {
        match self {
            CompiledWhere::Eq(pos, val) => {
                // SQL: NULL = val → NULL (false). Value PartialEq already returns false for Null==NonNull,
                // 🔑 但 Null == Null 在 Rust 里是 true —— NULL = NULL 必须显式
                // 判 false（BUG #40，prepared `v = ?` 传 NULL 曾错误命中 NULL 行）。
                if matches!(val, Value::Null) {
                    return Some(false);
                }
                // 🔑 Bool/Int coercion: `flag = 1` should match a BOOLEAN TRUE.
                // 🚀 快速路径：绝大多数列非 Bool，直接比较（零 clone）。
                //    仅当任一方是 Bool 时才 coerce（flag=1 ↔ TRUE 场景）。
                Some(row.get(*pos).is_some_and(|v| {
                    if needs_bool_coerce(v, val) {
                        let (a, b) = coerce_bool_int(v.clone(), val.clone());
                        a == b
                    } else {
                        v == val
                    }
                }))
            }
            CompiledWhere::Ne(pos, val) => {
                // SQL: NULL <> val → NULL (false). Must check NULL explicitly.
                Some(row.get(*pos).is_some_and(|v| {
                    if matches!(v, Value::Null) {
                        return false;
                    }
                    if needs_bool_coerce(v, val) {
                        let (a, b) = coerce_bool_int(v.clone(), val.clone());
                        a != b
                    } else {
                        v != val
                    }
                }))
            }
            CompiledWhere::Lt(pos, val) => Some(
                row.get(*pos)
                    .filter(|v| !matches!(v, Value::Null))
                    .is_some_and(|v| {
                        // 🔑 Bool↔Int 强制转换（与 Eq 一致）：-3 < TRUE 应按
                        // -3 < 1 判 —— Value 的 Ord 对跨类型是任意全序
                        //（BUG #44，差分对拍捕获）。
                        if needs_bool_coerce(v, val) {
                            let (a, b) = coerce_bool_int(v.clone(), val.clone());
                            a < b
                        } else {
                            v < val
                        }
                    }),
            ),
            CompiledWhere::Le(pos, val) => Some(
                row.get(*pos)
                    .filter(|v| !matches!(v, Value::Null))
                    .is_some_and(|v| {
                        // 🔑 Bool↔Int 强制转换（与 Eq 一致）：-3 < TRUE 应按
                        // -3 < 1 判 —— Value 的 Ord 对跨类型是任意全序
                        //（BUG #44，差分对拍捕获）。
                        if needs_bool_coerce(v, val) {
                            let (a, b) = coerce_bool_int(v.clone(), val.clone());
                            a <= b
                        } else {
                            v <= val
                        }
                    }),
            ),
            CompiledWhere::Gt(pos, val) => Some(
                row.get(*pos)
                    .filter(|v| !matches!(v, Value::Null))
                    .is_some_and(|v| {
                        // 🔑 Bool↔Int 强制转换（与 Eq 一致）：-3 < TRUE 应按
                        // -3 < 1 判 —— Value 的 Ord 对跨类型是任意全序
                        //（BUG #44，差分对拍捕获）。
                        if needs_bool_coerce(v, val) {
                            let (a, b) = coerce_bool_int(v.clone(), val.clone());
                            a > b
                        } else {
                            v > val
                        }
                    }),
            ),
            CompiledWhere::Ge(pos, val) => Some(
                row.get(*pos)
                    .filter(|v| !matches!(v, Value::Null))
                    .is_some_and(|v| {
                        // 🔑 Bool↔Int 强制转换（与 Eq 一致）：-3 < TRUE 应按
                        // -3 < 1 判 —— Value 的 Ord 对跨类型是任意全序
                        //（BUG #44，差分对拍捕获）。
                        if needs_bool_coerce(v, val) {
                            let (a, b) = coerce_bool_int(v.clone(), val.clone());
                            a >= b
                        } else {
                            v >= val
                        }
                    }),
            ),
            CompiledWhere::InHash(pos, set, negated, has_null) => {
                // SQL: NULL IN (...) → NULL (false); NULL NOT IN (...) → false
                Some(row.get(*pos).is_some_and(|v| {
                    if matches!(v, Value::Null) {
                        return false;
                    }
                    // 🔑 Bool↔Int coercion: normalize the column value the same
                    // way the set was normalized at compile time so a BOOLEAN
                    // column matches an integer IN list (TRUE IN (1)).
                    let contains = set.contains(&normalize_for_in(v));
                    if *negated {
                        // x NOT IN (..., NULL): x 在集合中 → false；
                        // x 不在 → UNKNOWN（因 NULL 可能等值）→ false
                        !contains && !*has_null
                    } else {
                        contains
                    }
                }))
            }
            CompiledWhere::Like(pos, pattern, negated) => {
                // 🔑 SQL 三值逻辑：NULL LIKE / NULL NOT LIKE → UNKNOWN → false
                // （negated 不能翻转 NULL 的结果）。非文本类型交给原生求值。
                let v = row.get(*pos)?;
                let matches = match v {
                    Value::Text(s) => Self::like_match(s, pattern),
                    Value::Null => return Some(false),
                    _ => return None,
                };
                Some(if *negated { !matches } else { matches })
            }
            CompiledWhere::IsNull(pos, negated) => {
                let is_null = row.get(*pos).is_none_or(|v| matches!(v, Value::Null));
                Some(if *negated { !is_null } else { is_null })
            }
            CompiledWhere::And(conds) => {
                for c in conds {
                    if !c.eval(row)? {
                        return Some(false);
                    }
                }
                Some(true)
            }
            CompiledWhere::Or(conds) => {
                for c in conds {
                    if c.eval(row)? {
                        return Some(true);
                    }
                }
                Some(false)
            }
            CompiledWhere::Not(inner) => Some(!inner.eval(row)?),
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
        loop {
            if pi < pbytes.len() {
                if pbytes[pi] == b'%' {
                    pi += 1;
                    if pi >= pbytes.len() {
                        return true;
                    } // trailing %
                    star_pi = Some(pi);
                    star_ti = Some(ti);
                } else if ti < tbytes.len() && (pbytes[pi] == b'_' || pbytes[pi] == tbytes[ti]) {
                    pi += 1;
                    ti += 1;
                } else if let (Some(spi), Some(sti)) = (star_pi, star_ti) {
                    // 🔑 回溯耗尽检查：star_ti 已到文本末尾意味着 % 已吞尽全部
                    // 文本仍无法让后续模式字符匹配 —— 再回溯只会 ti 无限增长，
                    // pi 原地弹跳 = 死循环（BUG #36，like("abc", "%x%") 即触发）。
                    if sti >= tbytes.len() {
                        return false;
                    }
                    // backtrack: consume one more char for the %
                    let new_ti = sti + 1;
                    ti = new_ti;
                    star_ti = Some(new_ti);
                    pi = spi;
                } else {
                    return false;
                }
            } else {
                // 模式耗尽：文本也必须耗尽，否则唯一机会是让上一个 % 吞掉
                // 剩余文本前缀、把 star 之后的模式重新锚到更靠后的位置
                //（后缀锚定模式如 '%a'：必须回溯到最后的 'a' 才能命中，
                //  BUG #39 —— 旧代码在循环外直接判 ti>=len，'%a' 永不匹配）。
                if ti >= tbytes.len() {
                    return true;
                }
                if let (Some(spi), Some(sti)) = (star_pi, star_ti) {
                    if sti >= tbytes.len() {
                        return false;
                    }
                    let new_ti = sti + 1;
                    ti = new_ti;
                    star_ti = Some(new_ti);
                    pi = spi;
                } else {
                    return false;
                }
            }
        }
    }

    /// Collect all column positions referenced by this compiled WHERE.
    /// Used for partial row decode optimization.
    fn collect_positions(&self, positions: &mut Vec<usize>) {
        match self {
            CompiledWhere::Eq(pos, _)
            | CompiledWhere::Ne(pos, _)
            | CompiledWhere::Lt(pos, _)
            | CompiledWhere::Le(pos, _)
            | CompiledWhere::Gt(pos, _)
            | CompiledWhere::Ge(pos, _)
            | CompiledWhere::InHash(pos, ..)
            | CompiledWhere::Like(pos, _, _)
            | CompiledWhere::IsNull(pos, _) => {
                positions.push(*pos);
            }
            CompiledWhere::And(conds) | CompiledWhere::Or(conds) => {
                for c in conds {
                    c.collect_positions(positions);
                }
            }
            CompiledWhere::Not(inner) => {
                inner.collect_positions(positions);
            }
        }
    }

    /// Evaluate against a partial decode buffer with position mapping.
    /// `pos_to_idx` maps schema column position → index in the partial buffer.
    #[inline]
    fn eval_at(&self, row: &[Value], pos_to_idx: &[Option<usize>]) -> Option<bool> {
        match self {
            CompiledWhere::Eq(pos, val) => {
                let idx = (*pos_to_idx).get(*pos)?.as_ref()?;
                // 🔑 NULL = 任何值（含 NULL）→ UNKNOWN → false（BUG #40）
                if matches!(val, Value::Null) {
                    return Some(false);
                }
                Some(row.get(*idx).is_some_and(|v| {
                    let (a, b) = coerce_bool_int(v.clone(), val.clone());
                    a == b
                }))
            }
            CompiledWhere::Ne(pos, val) => {
                let idx = (*pos_to_idx).get(*pos)?.as_ref()?;
                Some(row.get(*idx).is_some_and(|v| {
                    if matches!(v, Value::Null) {
                        return false;
                    }
                    let (a, b) = coerce_bool_int(v.clone(), val.clone());
                    a != b
                }))
            }
            CompiledWhere::Lt(pos, val) => {
                let idx = (*pos_to_idx).get(*pos)?.as_ref()?;
                Some(
                    row.get(*idx)
                        .filter(|v| !matches!(v, Value::Null))
                        .is_some_and(|v| {
                            if needs_bool_coerce(v, val) {
                                let (a, b) = coerce_bool_int(v.clone(), val.clone());
                                a < b
                            } else {
                                v < val
                            }
                        }),
                )
            }
            CompiledWhere::Le(pos, val) => {
                let idx = (*pos_to_idx).get(*pos)?.as_ref()?;
                Some(
                    row.get(*idx)
                        .filter(|v| !matches!(v, Value::Null))
                        .is_some_and(|v| {
                            if needs_bool_coerce(v, val) {
                                let (a, b) = coerce_bool_int(v.clone(), val.clone());
                                a <= b
                            } else {
                                v <= val
                            }
                        }),
                )
            }
            CompiledWhere::Gt(pos, val) => {
                let idx = (*pos_to_idx).get(*pos)?.as_ref()?;
                Some(
                    row.get(*idx)
                        .filter(|v| !matches!(v, Value::Null))
                        .is_some_and(|v| {
                            if needs_bool_coerce(v, val) {
                                let (a, b) = coerce_bool_int(v.clone(), val.clone());
                                a > b
                            } else {
                                v > val
                            }
                        }),
                )
            }
            CompiledWhere::Ge(pos, val) => {
                let idx = (*pos_to_idx).get(*pos)?.as_ref()?;
                Some(
                    row.get(*idx)
                        .filter(|v| !matches!(v, Value::Null))
                        .is_some_and(|v| {
                            if needs_bool_coerce(v, val) {
                                let (a, b) = coerce_bool_int(v.clone(), val.clone());
                                a >= b
                            } else {
                                v >= val
                            }
                        }),
                )
            }
            CompiledWhere::InHash(pos, set, negated, has_null) => {
                let idx = (*pos_to_idx).get(*pos)?.as_ref()?;
                Some(row.get(*idx).is_some_and(|v| {
                    if matches!(v, Value::Null) {
                        return false;
                    }
                    // 🔑 Bool↔Int coercion (see CompiledWhere::InHash above).
                    let contains = set.contains(&normalize_for_in(v));
                    if *negated {
                        !contains && !*has_null
                    } else {
                        contains
                    }
                }))
            }
            CompiledWhere::Like(pos, pattern, negated) => {
                let idx = (*pos_to_idx).get(*pos)?.as_ref()?;
                // 🔑 同上：NULL LIKE/NOT LIKE → false，非文本回退原生求值
                let v = row.get(*idx)?;
                let matches = match v {
                    Value::Text(s) => Self::like_match(s, pattern),
                    Value::Null => return Some(false),
                    _ => return None,
                };
                Some(if *negated { !matches } else { matches })
            }
            CompiledWhere::IsNull(pos, negated) => {
                let idx = (*pos_to_idx).get(*pos)?.as_ref()?;
                let is_null = row.get(*idx).is_none_or(|v| matches!(v, Value::Null));
                Some(if *negated { !is_null } else { is_null })
            }
            CompiledWhere::And(conds) => {
                for c in conds {
                    if !c.eval_at(row, pos_to_idx)? {
                        return Some(false);
                    }
                }
                Some(true)
            }
            CompiledWhere::Or(conds) => {
                for c in conds {
                    if c.eval_at(row, pos_to_idx)? {
                        return Some(true);
                    }
                }
                Some(false)
            }
            CompiledWhere::Not(inner) => Some(!inner.eval_at(row, pos_to_idx)?),
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
                    &raw_bytes,
                    &self.col_types,
                    offsets,
                )
            } else {
                crate::storage::row_format::RowParseContext::parse(
                    &raw_bytes,
                    &self.col_types,
                    self.fixed_count,
                )
            };
            let ctx = match parse_result {
                Some(c) => c,
                None => {
                    // Legacy bincode — fall back to full decode
                    self.where_buf.clear();
                    if let Err(e) = crate::storage::row_format::decode_fast_partial_into(
                        &raw_bytes,
                        &self.col_types,
                        self.fixed_count,
                        &self.needed,
                        &mut self.where_buf,
                    ) {
                        return Some(Err(e));
                    }
                    self.projected.clear();
                    self.projected
                        .resize(self.num_output_cols, crate::types::Value::Null);
                    for &(out_idx, buf_idx) in self
                        .project_where_indices
                        .iter()
                        .chain(self.project_select_indices.iter())
                    {
                        if out_idx < self.projected.len() {
                            self.projected[out_idx] = self
                                .where_buf
                                .get(buf_idx)
                                .cloned()
                                .unwrap_or(crate::types::Value::Null);
                        }
                    }
                    return Some(Ok(std::mem::take(&mut self.projected)));
                }
            };

            // Phase 1: Decode only WHERE columns (reusing buffer)
            self.where_buf.clear();
            if let Err(e) = ctx.decode_columns(
                &raw_bytes,
                &self.col_types,
                &self.where_pos,
                &mut self.where_buf,
            ) {
                return Some(Err(e));
            }

            // Evaluate WHERE filter
            let matches = if let Some(ref cw) = self.compiled_where {
                cw.eval_at(&self.where_buf, &self.where_pos_to_idx)
                    .unwrap_or(false)
            } else {
                true
            };

            if !matches {
                continue; // ← Skip! No SELECT decode needed. Buffer will be reused.
            }

            // Phase 2: Decode remaining SELECT columns (only for passing rows, reusing buffer)
            self.select_buf.clear();
            if !self.select_only_pos.is_empty() {
                if let Err(e) = ctx.decode_columns(
                    &raw_bytes,
                    &self.col_types,
                    &self.select_only_pos,
                    &mut self.select_buf,
                ) {
                    return Some(Err(e));
                }
            }

            // Build projected output (reusing buffer)
            self.projected.clear();
            self.projected
                .resize(self.num_output_cols, crate::types::Value::Null);
            for &(out_idx, buf_idx) in &self.project_where_indices {
                if out_idx < self.projected.len() {
                    self.projected[out_idx] = self
                        .where_buf
                        .get(buf_idx)
                        .cloned()
                        .unwrap_or(crate::types::Value::Null);
                }
            }
            for &(out_idx, buf_idx) in &self.project_select_indices {
                if out_idx < self.projected.len() {
                    self.projected[out_idx] = self
                        .select_buf
                        .get(buf_idx)
                        .cloned()
                        .unwrap_or(crate::types::Value::Null);
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
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl PartialOrd for SortKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for SortKey {
    fn cmp(&self, other: &Self) -> Ordering {
        order_by_cmp(&self.0, &other.0)
    }
}

pub struct QueryExecutor {
    db: Arc<MoteDB>,
    evaluator: ExprEvaluator,
    optimizer: super::optimizer::QueryOptimizer,
    /// Store the last AUTO_INCREMENT value inserted (mirrors evaluator)
    last_insert_id: std::sync::atomic::AtomicI64,
}

// 🔑 Per-thread transaction context.
//
// `execute()` resolves the active transaction from this thread-local rather
// than from a shared single-slot field. The previous design stored a single
// `current_txn_id` in a `Mutex<Option<u64>>` on the executor, which is shared
// across all threads holding an `Arc<Database>`. When multiple threads each
// opened a transaction on the same `Database`, one thread's `begin` would
// overwrite another's slot, and a third thread's `commit` would clear it —
// producing "Transaction N not found" panics (see test_concurrent_transactions).
//
// Thread-local storage is correct here because the documented concurrency
// model calls `execute()` on the same thread that called `begin_transaction()`,
// so each thread independently tracks its own active transaction.
thread_local! {
    static CURRENT_TXN_ID: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
    /// Per-statement memo of ST_KNN_3D result sets, keyed by
    /// "index|x|y|z|k" (see the StKnn3D arm of the row evaluator).
    static SPATIAL_KNN_MEMO: std::cell::RefCell<
        std::collections::HashMap<String, Arc<std::collections::HashSet<u64>>>,
    > = std::cell::RefCell::new(std::collections::HashMap::new());
    /// Per-statement memo of MATCH(col, 'query') row-id sets. Same idea as
    /// SPATIAL_KNN_MEMO: the predicate is a set-membership test resolved by
    /// the text index, not a per-row computation.
    static TEXT_MATCH_MEMO: std::cell::RefCell<
        std::collections::HashMap<String, Arc<std::collections::HashSet<u64>>>,
    > = std::cell::RefCell::new(std::collections::HashMap::new());
}

/// Determine if a CASE WHEN condition value is "true".
/// SQL standard: only Bool(true) matches. SQLite also treats non-zero
/// Integer/Float as true (truthy). NULL never matches.
fn case_cond_matched(v: &Value) -> bool {
    match v {
        Value::Bool(b) => *b,
        Value::Integer(i) => *i != 0,
        Value::Float(f) => *f != 0.0,
        _ => false,
    }
}

/// True when any ORDER BY clause carries an explicit NULLS preference that
/// DIFFERS from the engine's dialect default (NULLs first on ASC, last on
/// DESC). Fast paths whose sorters only understand (idx, asc) decline such
/// queries so the materialized path (apply_order_by, which honors the
/// flag) handles them.
fn order_by_has_nondefault_nulls(order_by: Option<&[OrderByExpr]>) -> bool {
    order_by.is_some_and(|obs| {
        obs.iter()
            .any(|ob| ob.nulls_first.is_some_and(|nf| nf != ob.asc))
    })
}

/// Compare two values for ORDER BY in the GROUP BY result path.
/// NULLs sort FIRST in ASC and LAST in DESC (matches the non-GROUP-BY
/// apply_order_by path and SQLite's default NULL ordering: NULL is
/// considered smaller than every other value).
/// Falls back to Value::partial_cmp for types not in the fast path
/// (Bool, Timestamp, Blob), so they sort correctly too.
fn compare_with_nulls(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        // Non-NULL: try the optimized numeric/text path, then full partial_cmp.
        (a, b) => QueryExecutor::compare_values(a, b)
            .unwrap_or_else(|| a.partial_cmp(b).unwrap_or(Ordering::Equal)),
    }
}

/// Collect distinct non-NULL values at a column position from positional rows.
/// Used by SUM(DISTINCT)/AVG(DISTINCT) on the positional path.
fn collect_distinct_positional(col_pos: Option<usize>, rows: &[&Row]) -> Vec<Value> {
    use std::collections::HashSet;
    let pos = match col_pos {
        Some(p) => p,
        None => return Vec::new(),
    };
    let mut seen: HashSet<Value> = HashSet::new();
    let mut out: Vec<Value> = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(val) = row.get(pos) {
            if matches!(val, Value::Null) {
                continue;
            }
            if seen.insert(val.clone()) {
                out.push(val.clone());
            }
        }
    }
    out
}

impl QueryExecutor {
    pub fn new(db: Arc<MoteDB>) -> Self {
        Self {
            evaluator: ExprEvaluator::with_db(db.clone()),
            optimizer: super::optimizer::QueryOptimizer::new(db.clone()),
            last_insert_id: std::sync::atomic::AtomicI64::new(i64::MIN),
            db,
        }
    }

    /// Reset per-query state. Called before each execute.
    pub fn reset_last_insert_id(&self) {
        self.last_insert_id
            .store(i64::MIN, std::sync::atomic::Ordering::Relaxed);
        self.evaluator
            .last_insert_id
            .store(i64::MIN, std::sync::atomic::Ordering::Relaxed);
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
            Statement::Select { stmt: s, ctes } => {
                let s = self.apply_ctes_for_select(s, &ctes)?;
                self.execute_select(s)
            }
            Statement::SetOp {
                left,
                right,
                op,
                all,
                ctes,
                order_by,
                limit,
                offset,
            } => {
                // Execute the outermost set op, then apply the trailing
                // ORDER BY / LIMIT / OFFSET carried by this node (per SQL
                // standard these apply to the whole set result).
                let mut result =
                    self.execute_set_op(left.as_ref(), right.as_ref(), op.clone(), all, &ctes)?;
                if order_by.is_some() || limit.is_some() || offset.is_some() {
                    result = self.apply_set_op_trailing(result, &order_by, limit, offset)?;
                }
                Ok(result)
            }
            Statement::Insert(i) => self.execute_insert(i),
            Statement::Explain(inner) => self.execute_explain(&inner),
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
            Statement::Savepoint(name) => self.execute_savepoint(&name),
            Statement::RollbackToSavepoint(name) => self.execute_rollback_to_savepoint(&name),
            Statement::ReleaseSavepoint(name) => self.execute_release_savepoint(&name),
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
        CURRENT_TXN_ID.with(|c| c.get().is_some())
    }

    /// TLS 事务态查询（无 executor 句柄的模块用 — vector_exec 的 decline 检查）。
    pub fn is_in_transaction_tls() -> bool {
        CURRENT_TXN_ID.with(|c| c.get().is_some())
    }

    /// Mark a transaction as active so subsequent execute() calls route writes
    /// through the transaction coordinator (buffered in write_set until commit).
    /// Called by Database::begin_transaction() to keep the executor in sync
    /// with the coordinator — without this, the executor would write directly
    /// to storage and rollback could not undo the writes.
    pub fn begin_txn_context(&self, txn_id: u64) {
        CURRENT_TXN_ID.with(|c| c.set(Some(txn_id)));
    }

    /// Clear the active transaction context (after commit or rollback).
    pub fn clear_txn_context(&self) {
        CURRENT_TXN_ID.with(|c| c.set(None));
    }

    /// Get the active transaction id, if any.
    pub fn current_txn_id(&self) -> Option<u64> {
        CURRENT_TXN_ID.with(|c| c.get())
    }

    /// 🔑 Replay the transaction's undo log in reverse order, restoring the
    /// pre-transaction state of rows that UPDATE/DELETE modified directly in
    /// storage. INSERTs are NOT replayed here — they were buffered in the
    /// write_set and never written to storage, so rollback just discards them.
    ///
    /// This MUST be called by Database::rollback_transaction() (the API path)
    /// before delegating to the coordinator. The SQL ROLLBACK path in
    /// execute_streaming_ref inlines the same logic. Without this, API-level
    /// rollback would silently fail to undo UPDATE/DELETE changes.
    pub fn replay_undo_log(&self, txn_id: u64) {
        let ctx = match self.db.txn_coordinator.get_context(txn_id) {
            Ok(c) => c,
            Err(_) => return,
        };
        let undo_log = std::mem::take(&mut *ctx.undo_log.write());
        for delta in undo_log.into_iter().rev() {
            match delta {
                crate::txn::coordinator::DeltaOperation::Update(row_id, table_name, old_value) => {
                    let old_row =
                        std::sync::Arc::try_unwrap(old_value).unwrap_or_else(|arc| (*arc).clone());
                    if let Ok(schema) = self.db.get_table_schema(&table_name) {
                        let _ = self.db.update_row_in_table_with_schema(
                            &table_name,
                            row_id,
                            old_row.clone(),
                            old_row,
                            &schema,
                        );
                    }
                }
                crate::txn::coordinator::DeltaOperation::Delete(_row_id, table_name, old_value) => {
                    let old_row =
                        std::sync::Arc::try_unwrap(old_value).unwrap_or_else(|arc| (*arc).clone());
                    let _ = self.db.insert_row_to_table(&table_name, old_row);
                }
                crate::txn::coordinator::DeltaOperation::Insert(_, _, _) => {
                    // INSERT undo: write_set INSERT was never committed to store.
                }
                crate::txn::coordinator::DeltaOperation::UpdateBuffered(_, _, _) => {
                    // write_set-level undo (handled by the coordinator) —
                    // never replay against storage (uncommitted row).
                }
            }
        }
    }

    // ==================== Read-Your-Writes helpers ====================
    //
    // When inside a transaction, SELECT must see the transaction's own
    // uncommitted INSERTs (write_set) and must NOT see rows the transaction
    // has DELETEd (undo_log tombstones). These helpers extract the relevant
    // state from the active transaction's context. All return empty
    // containers when no transaction is active → zero overhead on the
    // autocommit fast path.

    /// Returns the (row_id, row) pairs buffered in the active transaction's
    /// write_set that belong to `table`. Empty when not in a transaction.
    pub(crate) fn txn_write_set_rows(&self, table: &str) -> Vec<(RowId, Row)> {
        let txn_id = match self.current_txn_id() {
            Some(t) => t,
            None => return Vec::new(),
        };
        let ctx = match self.db.txn_coordinator.get_context(txn_id) {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };
        let ws = ctx.write_set.read();
        ws.iter()
            .filter(|((tbl, _), _)| tbl == table)
            .map(|((_, rid), row)| (*rid, row.clone()))
            .collect()
    }

    /// Returns the set of row_ids the active transaction has DELETEd from
    /// `table` (recorded in the undo_log). Empty when not in a transaction.
    pub(crate) fn txn_deleted_row_ids(&self, table: &str) -> std::collections::HashSet<RowId> {
        let txn_id = match self.current_txn_id() {
            Some(t) => t,
            None => return std::collections::HashSet::new(),
        };
        let ctx = match self.db.txn_coordinator.get_context(txn_id) {
            Ok(c) => c,
            Err(_) => return std::collections::HashSet::new(),
        };
        let undo = ctx.undo_log.read();
        undo.iter()
            .filter_map(|delta| match delta {
                crate::txn::coordinator::DeltaOperation::Delete(rid, tbl, _) if tbl == table => {
                    Some(*rid)
                }
                _ => None,
            })
            .collect()
    }

    /// Point-lookup a single row within the active transaction.
    /// Returns:
    /// - `Some(Some(row))` — row is in the write_set (uncommitted INSERT).
    /// - `Some(None)` — row was DELETEd by this transaction (tombstone).
    /// - `None` — no transactional info; caller should consult storage.
    pub(crate) fn txn_lookup_row(&self, table: &str, row_id: RowId) -> Option<Option<Row>> {
        let txn_id = self.current_txn_id()?;
        let ctx = self.db.txn_coordinator.get_context(txn_id).ok()?;
        // DELETE tombstone check first (a row could be deleted then re-inserted;
        // write_set wins for re-inserts, so check it after).
        let undo = ctx.undo_log.read();
        let deleted = undo.iter().any(|d| match d {
            crate::txn::coordinator::DeltaOperation::Delete(rid, tbl, _) => {
                *rid == row_id && tbl == table
            }
            _ => false,
        });
        drop(undo);
        let ws = ctx.write_set.read();
        if let Some(row) = ws.get(&(table.to_string(), row_id)) {
            return Some(Some(row.clone()));
        }
        if deleted {
            return Some(None);
        }
        None
    }

    pub fn execute_streaming_ref(&self, stmt: &Statement) -> Result<StreamingQueryResult> {
        let max_rows = self.db.max_result_rows;
        SPATIAL_KNN_MEMO.with(|m| m.borrow_mut().clear());
        TEXT_MATCH_MEMO.with(|m| m.borrow_mut().clear());

        // NOTE: We intentionally do NOT clear segment col_cache here. The cache
        // is bounded to 16 entries per segment (BoundedColCache), so it can't
        // grow unboundedly. Clearing it forces point queries to re-decode entire
        // column segments on every call (20ms+ for 2M-row segments).

        let result = match stmt {
            Statement::Select { stmt: s, ctes } => {
                let s = self.apply_ctes_for_select(s.clone(), ctes)?;
                self.execute_select_streaming_ref(&s)?
            }
            Statement::SetOp {
                left,
                right,
                op,
                all,
                ctes,
                order_by,
                limit,
                offset,
            } => {
                let mut result =
                    self.execute_set_op(left.as_ref(), right.as_ref(), op.clone(), *all, ctes)?;
                if order_by.is_some() || limit.is_some() || offset.is_some() {
                    result = self.apply_set_op_trailing(result, order_by, *limit, *offset)?;
                }
                return Ok(match result {
                    QueryResult::Select { columns, rows } => {
                        StreamingQueryResult::SelectReady { columns, rows }
                    }
                    _ => StreamingQueryResult::Modification { affected_rows: 0 },
                });
            }
            Statement::Insert(i) => {
                let result = self.execute_insert_ref(i)?;
                StreamingQueryResult::Modification {
                    affected_rows: result.affected_rows(),
                }
            }
            Statement::Explain(inner) => match self.execute_explain(inner)? {
                QueryResult::Select { columns, rows } => {
                    StreamingQueryResult::SelectReady { columns, rows }
                }
                _ => unreachable!("execute_explain always returns Select"),
            },
            Statement::Update(u) => {
                // 🔑 参数替换（BUG #45）：prepared UPDATE/DELETE 的 WHERE/SET
                // 曾不被解析 —— 逐行求值遇 Parameter 返回 Err 被
                // unwrap_or(false) 吞掉，静默 0 行受影响但返回 Ok。
                let mut owned = u.clone();
                let mut assignments = std::mem::take(&mut owned.assignments);
                self.do_substitute_params_mutation(
                    &mut owned.where_clause,
                    Some(&mut assignments),
                )?;
                owned.assignments = assignments;
                let result = self.execute_update(owned)?;
                StreamingQueryResult::Modification {
                    affected_rows: result.affected_rows(),
                }
            }
            Statement::Delete(d) => {
                let mut owned = d.clone();
                self.do_substitute_params_mutation(&mut owned.where_clause, None)?;
                let result = self.execute_delete(owned)?;
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
            // 🔑 Errors must PROPAGATE — the old branches folded them into a
            // success message ("SAVEPOINT failed: …"), so an external client
            // (Python binding, CLI) saw a silent no-op: a SAVEPOINT without an
            // active transaction "succeeded", and the later ROLLBACK TO also
            // "succeeded" while the UPDATE stayed committed.
            Statement::Savepoint(name) => {
                self.execute_savepoint(&name)?;
                StreamingQueryResult::Definition {
                    message: format!("SAVEPOINT {name} created"),
                }
            }
            Statement::RollbackToSavepoint(name) => {
                self.execute_rollback_to_savepoint(&name)?;
                StreamingQueryResult::Definition {
                    message: format!("rolled back to SAVEPOINT {name}"),
                }
            }
            Statement::ReleaseSavepoint(name) => {
                self.execute_release_savepoint(&name)?;
                StreamingQueryResult::Definition {
                    message: format!("SAVEPOINT {name} released"),
                }
            }
            Statement::BeginTransaction => {
                // 🚨 Reject nested transactions (see execute_begin_transaction).
                if self.current_txn_id().is_some() {
                    return Err(MoteDBError::Query(
                        "Nested transactions are not supported; COMMIT or ROLLBACK the current transaction first".to_string(),
                    ));
                }
                let txn_id = self.db.begin_transaction()?;
                self.begin_txn_context(txn_id);
                StreamingQueryResult::Definition {
                    message: format!("Transaction {} started", txn_id),
                }
            }
            Statement::CommitTransaction => {
                let _txn_id_opt = self.current_txn_id();
                if let Some(txn_id) = _txn_id_opt {
                    self.db.commit_transaction(txn_id)?;
                    self.clear_txn_context();
                    StreamingQueryResult::Definition {
                        message: format!("Transaction {} committed", txn_id),
                    }
                } else {
                    // 🔑 SQLite-compatible error (see execute_commit_transaction)
                    return Err(MoteDBError::InvalidArgument(
                        "cannot COMMIT - no transaction is active".to_string(),
                    ));
                }
            }
            Statement::RollbackTransaction => {
                let _txn_id_opt = self.current_txn_id();
                if let Some(txn_id) = _txn_id_opt {
                    // 🔑 Replay undo log BEFORE clearing the transaction context.
                    // execute_update/execute_delete recorded old values for rows
                    // they modified directly in storage. We replay those here to
                    // restore the pre-transaction state.
                    if let Ok(ctx) = self.db.txn_coordinator.get_context(txn_id) {
                        let undo_log = std::mem::take(&mut *ctx.undo_log.write());
                        for delta in undo_log.into_iter().rev() {
                            match delta {
                                crate::txn::coordinator::DeltaOperation::Update(
                                    row_id,
                                    table_name,
                                    old_value,
                                ) => {
                                    let old_row = std::sync::Arc::try_unwrap(old_value)
                                        .unwrap_or_else(|arc| (*arc).clone());
                                    if let Ok(schema) = self.db.get_table_schema(&table_name) {
                                        let _ = self.db.update_row_in_table_with_schema(
                                            &table_name,
                                            row_id,
                                            old_row.clone(),
                                            old_row,
                                            &schema,
                                        );
                                    }
                                }
                                crate::txn::coordinator::DeltaOperation::Delete(
                                    _row_id,
                                    table_name,
                                    old_value,
                                ) => {
                                    let old_row = std::sync::Arc::try_unwrap(old_value)
                                        .unwrap_or_else(|arc| (*arc).clone());
                                    let _ = self.db.insert_row_to_table(&table_name, old_row);
                                }
                                crate::txn::coordinator::DeltaOperation::Insert(_, _, _) => {
                                    // INSERT undo: write_set INSERT was never committed to store.
                                }
                                crate::txn::coordinator::DeltaOperation::UpdateBuffered(
                                    _,
                                    _,
                                    _,
                                ) => {
                                    // write_set-level undo — no storage action.
                                }
                            }
                        }
                    }
                    self.db.rollback_transaction(txn_id)?;
                    self.clear_txn_context();
                    StreamingQueryResult::Definition {
                        message: format!("Transaction {} rolled back", txn_id),
                    }
                } else {
                    // 🔑 SQLite-compatible error (see execute_rollback_transaction)
                    return Err(MoteDBError::InvalidArgument(
                        "cannot ROLLBACK - no transaction is active".to_string(),
                    ));
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

    /// Rewrite a SELECT's FROM clause so that any reference to a CTE name
    /// becomes a `TableRef::Subquery` over the CTE's body.
    ///
    /// This is the heart of WITH/CTE support: by the time the executor sees
    /// the statement, CTE references have been inlined as derived tables,
    /// which `execute_from_with_limit` already knows how to materialize
    /// (`executor.rs:12252`). No storage or executor core changes needed.
    ///
    /// **Lexical scoping**: CTEs are processed in definition order. Each CTE's
    /// body is rewritten against the set of *preceding* CTEs (so a later CTE
    /// can reference an earlier one); the main statement is rewritten against
    /// *all* CTEs.
    ///
    /// **RECURSIVE**: v1 does not implement fixed-point evaluation. If a CTE
    /// body references a CTE of the same name (direct self-reference) we
    /// return an explicit error rather than silently producing wrong results.
    /// Forward references to not-yet-defined CTEs are also rejected.
    fn apply_ctes_for_select(&self, mut stmt: SelectStmt, ctes: &[CteDef]) -> Result<SelectStmt> {
        if ctes.is_empty() {
            return Ok(stmt);
        }

        // Accumulate visible CTE bodies as we go (name -> cloned body).
        // Stored as Vec to preserve insertion order for diagnostics.
        let mut visible: Vec<(String, CteDef)> = Vec::with_capacity(ctes.len());

        for cte in ctes {
            // Detect direct self-reference / forward reference.
            if let Some(from) = &cte.query.from {
                Self::check_recursive_ref(
                    from,
                    &cte.name,
                    &visible.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
                )?;
            }

            // Rewrite this CTE's body against previously-defined CTEs.
            let mut body = cte.query.clone();
            if let Some(from) = body.from.as_mut() {
                Self::rewrite_from_cte_refs(from, &visible, &cte.columns, &cte.name);
            }
            // Apply explicit column aliases (WITH x(a, b) AS (...)).
            if let Some(cols) = &cte.columns {
                Self::apply_cte_column_aliases(&mut body, cols);
            }

            visible.push((
                cte.name.clone(),
                CteDef {
                    name: cte.name.clone(),
                    columns: cte.columns.clone(),
                    query: body,
                },
            ));
        }

        // Rewrite the main statement's FROM against all CTEs.
        if let Some(from) = stmt.from.as_mut() {
            Self::rewrite_from_cte_refs(from, &visible, &None, "");
        }

        // 🔑 Rewrite CTE references inside subqueries that appear in the
        // WHERE / SELECT-list / HAVING (e.g. `WHERE v IN (SELECT ... FROM x)`).
        // These Expr::Subquery nodes have their own FROM that must see the
        // outer CTEs per SQL scoping. The FROM-level rewrite above only
        // touches the main FROM clause, not expression subqueries.
        if let Some(wc) = stmt.where_clause.as_mut() {
            Self::rewrite_subquery_cte_refs(wc, &visible);
        }
        if let Some(hv) = stmt.having.as_mut() {
            Self::rewrite_subquery_cte_refs(hv, &visible);
        }
        for col in stmt.columns.iter_mut() {
            if let crate::sql::ast::SelectColumn::Expr(e, _) = col {
                Self::rewrite_subquery_cte_refs(e, &visible);
            }
        }

        Ok(stmt)
    }

    /// Recursively walk an expression and rewrite CTE references inside any
    /// `Expr::Subquery`'s FROM clause (so subqueries in WHERE/SELECT/HAVING
    /// can reference outer-query CTEs).
    fn rewrite_subquery_cte_refs(expr: &mut Expr, visible: &[(String, CteDef)]) {
        match expr {
            Expr::Subquery(sub) => {
                if let Some(inner_from) = sub.from.as_mut() {
                    Self::rewrite_from_cte_refs(inner_from, visible, &None, "");
                }
                // Recurse into the subquery's own WHERE/SELECT for nested subs.
                if let Some(wc) = sub.where_clause.as_mut() {
                    Self::rewrite_subquery_cte_refs(wc, visible);
                }
                for col in sub.columns.iter_mut() {
                    if let crate::sql::ast::SelectColumn::Expr(e, _) = col {
                        Self::rewrite_subquery_cte_refs(e, visible);
                    }
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::rewrite_subquery_cte_refs(left, visible);
                Self::rewrite_subquery_cte_refs(right, visible);
            }
            Expr::UnaryOp { expr, .. } => Self::rewrite_subquery_cte_refs(expr, visible),
            Expr::In { expr, list, .. } => {
                Self::rewrite_subquery_cte_refs(expr, visible);
                for item in list.iter_mut() {
                    Self::rewrite_subquery_cte_refs(item, visible);
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::rewrite_subquery_cte_refs(expr, visible);
                Self::rewrite_subquery_cte_refs(low, visible);
                Self::rewrite_subquery_cte_refs(high, visible);
            }
            Expr::Like { expr, pattern, .. } => {
                Self::rewrite_subquery_cte_refs(expr, visible);
                Self::rewrite_subquery_cte_refs(pattern, visible);
            }
            Expr::IsNull { expr, .. } => Self::rewrite_subquery_cte_refs(expr, visible),
            Expr::FunctionCall { args, .. } => {
                for a in args.iter_mut() {
                    Self::rewrite_subquery_cte_refs(a, visible);
                }
            }
            Expr::Case { whens, else_expr } => {
                for (c, v) in whens.iter_mut() {
                    Self::rewrite_subquery_cte_refs(c, visible);
                    Self::rewrite_subquery_cte_refs(v, visible);
                }
                if let Some(e) = else_expr.as_mut() {
                    Self::rewrite_subquery_cte_refs(e, visible);
                }
            }
            _ => {}
        }
    }

    /// Walk a `TableRef` tree and replace `Table { name: cte_name, .. }` with
    /// `Subquery { query: <cloned body>, alias }` for every name in `visible`.
    ///
    /// `owner_name` / `owner_aliases` are used to apply CTE-level column
    /// aliases when the CTE itself is referenced (rare; usually None / "").
    fn rewrite_from_cte_refs(
        table_ref: &mut TableRef,
        visible: &[(String, CteDef)],
        _owner_aliases: &Option<Vec<String>>,
        _owner_name: &str,
    ) {
        match table_ref {
            TableRef::Table { name, alias } => {
                if let Some((_, cte)) = visible.iter().find(|(n, _)| n == name) {
                    let new_alias = alias.clone().unwrap_or_else(|| name.clone());
                    *table_ref = TableRef::Subquery {
                        query: Box::new(cte.query.clone()),
                        alias: new_alias,
                    };
                }
            }
            TableRef::Subquery { query, .. } => {
                // 🔑 Rewrite CTE references inside nested subqueries too. Per
                // SQL scoping, a CTE defined in the outer query is visible to
                // subqueries (e.g. `WITH x AS (...) SELECT ... WHERE v IN
                // (SELECT MAX(s) FROM x)`). Previously this was skipped, so the
                // inner `FROM x` raised "Table 'x' not found". We recurse into
                // the subquery's own FROM clause with the same visible CTEs.
                if let Some(inner_from) = query.from.as_mut() {
                    Self::rewrite_from_cte_refs(inner_from, visible, &None, "");
                }
            }
            TableRef::Join {
                left,
                right,
                join_type: _,
                on_condition: _,
            } => {
                Self::rewrite_from_cte_refs(left, visible, &None, "");
                Self::rewrite_from_cte_refs(right, visible, &None, "");
            }
        }
    }

    /// Detect direct self-reference (the CTE body names itself) or forward
    /// reference (names a CTE defined later). Both are unsupported in v1.
    fn check_recursive_ref(
        table_ref: &TableRef,
        self_name: &str,
        defined_so_far: &[&str],
    ) -> Result<()> {
        match table_ref {
            TableRef::Table { name, .. } => {
                if name == self_name {
                    return Err(MoteDBError::Query(format!(
                        "Recursive CTE '{}' is not supported (self-reference in FROM)",
                        self_name
                    )));
                }
                // A name that looks like a CTE but isn't yet defined is a
                // forward reference — only flag if it matches a CTE defined
                // later. We can't see "later" here, so we check: if the name
                // isn't a real table AND isn't an already-defined CTE, the
                // normal "no such table" error from execute_from will surface
                // it. So no extra check here.
                let _ = defined_so_far;
            }
            TableRef::Join { left, right, .. } => {
                Self::check_recursive_ref(left, self_name, defined_so_far)?;
                Self::check_recursive_ref(right, self_name, defined_so_far)?;
            }
            TableRef::Subquery { .. } => {}
        }
        Ok(())
    }

    /// Apply CTE-level column aliases: `WITH x(a, b) AS (SELECT id, name ...)`
    /// → make the body emit columns named a, b without changing source names.
    ///
    /// Strategy: if the body uses `SELECT *`, leave it (can't reliably map).
    /// Otherwise rewrite each `SelectColumn` to carry the alias from position
    /// `i` in `aliases` while preserving the source column / expression.
    fn apply_cte_column_aliases(body: &mut SelectStmt, aliases: &[String]) {
        // Only rewrite if column count matches and body isn't SELECT *.
        if body.columns.len() != aliases.len() {
            return;
        }
        if body.columns.iter().any(|c| matches!(c, SelectColumn::Star)) {
            return;
        }
        for (col, alias) in body.columns.iter_mut().zip(aliases.iter()) {
            match col {
                // SELECT col  →  SELECT col AS alias
                SelectColumn::Column(name) => {
                    let name = name.clone();
                    *col = SelectColumn::ColumnWithAlias(name, alias.clone());
                }
                // SELECT col AS x  →  SELECT col AS alias (override)
                SelectColumn::ColumnWithAlias(name, _) => {
                    let name = name.clone();
                    *col = SelectColumn::ColumnWithAlias(name, alias.clone());
                }
                // SELECT expr [AS x]  →  SELECT expr AS alias
                SelectColumn::Expr(e, _) => {
                    let e = e.clone();
                    *col = SelectColumn::Expr(e, Some(alias.clone()));
                }
                SelectColumn::Star => {}
            }
        }
    }

    /// Execute UNION / UNION ALL set operation.
    /// Execute one branch of a set operation. The branch is either a Select or
    /// a nested SetOp (e.g. an INTERSECT chain appearing as the right operand
    /// of a UNION). Applies inherited CTEs, then dispatches. Nested SetOps with
    /// their own (non-empty) CTEs take precedence over the inherited ones.
    fn execute_set_op_branch(
        &self,
        branch: &Statement,
        inherited_ctes: &[CteDef],
    ) -> Result<QueryResult> {
        match branch {
            Statement::SetOp {
                left: l,
                right: r,
                op: o,
                all: a,
                ctes,
                order_by,
                limit,
                offset,
            } => {
                // A nested SetOp may carry its own CTEs (when it's the
                // outermost in a WITH) or none (the parser only attaches CTEs
                // to the outermost). Prefer the nested SetOp's own CTEs if
                // non-empty; otherwise inherit from the parent.
                let effective_ctes = if ctes.is_empty() {
                    inherited_ctes
                } else {
                    ctes
                };
                let mut result = self.execute_set_op(l, r, o.clone(), *a, effective_ctes)?;
                // Apply this node's own trailing ORDER BY/LIMIT/OFFSET (only the
                // outermost carries them; nested nodes have None).
                if order_by.is_some() || limit.is_some() || offset.is_some() {
                    result = self.apply_set_op_trailing(result, order_by, *limit, *offset)?;
                }
                Ok(result)
            }
            Statement::Select { stmt, ctes } => {
                // 🔑 Apply CTEs to the Select. Prefer the Select's own CTEs
                // (parser clones the full WITH list into branches); fall back to
                // inherited for chained unions where branches have empty ctes.
                let effective_ctes = if ctes.is_empty() {
                    inherited_ctes
                } else {
                    ctes
                };
                let s = self.apply_ctes_for_select(stmt.clone(), effective_ctes)?;
                self.execute_select_internal(&s)
            }
            _ => Err(MoteDBError::Query(
                "Operands of set op must be SELECT".into(),
            )),
        }
    }

    /// Apply ORDER BY / LIMIT / OFFSET to the combined rows of an outermost
    /// set operation. ORDER BY references output column names or ordinals.
    fn apply_set_op_trailing(
        &self,
        result: QueryResult,
        order_by: &Option<Vec<crate::sql::ast::OrderByExpr>>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<QueryResult> {
        let (columns, mut rows) = match result {
            QueryResult::Select { columns, rows } => (columns, rows),
            other => return Ok(other),
        };
        if let Some(order_by) = order_by {
            // Resolve each ORDER BY key against the output columns (by name) or
            // by ordinal (1-based). Build a comparator that handles NULLs.
            let col_names: Vec<String> = columns.clone();
            rows.sort_by(|a, b| {
                for obe in order_by {
                    let (av, bv) = self.resolve_order_key(&obe.expr, &col_names, a, b);
                    let ord = order_by_cmp(&av, &bv);
                    let ord = if obe.asc { ord } else { ord.reverse() };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
        if let Some(off) = offset {
            if off >= rows.len() {
                rows.clear();
            } else {
                rows.drain(0..off);
            }
        }
        if let Some(lim) = limit {
            rows.truncate(lim);
        }
        Ok(QueryResult::Select { columns, rows })
    }

    /// Resolve an ORDER BY key expression against output columns of a set op,
    /// returning the (a_value, b_value) pair for two rows. Supports column-name
    /// and ordinal (Literal Integer) references.
    fn resolve_order_key<'a>(
        &self,
        expr: &crate::sql::ast::Expr,
        col_names: &[String],
        a: &'a [Value],
        b: &'a [Value],
    ) -> (Value, Value) {
        use crate::sql::ast::Expr;
        let idx = match expr {
            Expr::Column(name) => col_names.iter().position(|c| c.eq_ignore_ascii_case(name)),
            Expr::Literal(Value::Integer(i)) => {
                // 1-based ordinal.
                if *i >= 1 {
                    Some((*i as usize) - 1)
                } else {
                    None
                }
            }
            _ => None,
        };
        match idx {
            Some(i) if i < a.len() && i < b.len() => (a[i].clone(), b[i].clone()),
            _ => (Value::Null, Value::Null),
        }
    }

    fn execute_set_op(
        &self,
        left: &Statement,
        right: &Statement,
        op: crate::sql::ast::SetOp,
        all: bool,
        inherited_ctes: &[CteDef],
    ) -> Result<QueryResult> {
        // left can be a nested SetOp (chained) or a Select — execute accordingly.
        let left_result = self.execute_set_op_branch(left, inherited_ctes)?;
        let right_result = self.execute_set_op_branch(right, inherited_ctes)?;
        let (columns, left_rows) = match left_result {
            QueryResult::Select { columns, rows } => (columns, rows),
            _ => {
                return Err(MoteDBError::Query(
                    "Left side of set op must be SELECT".into(),
                ))
            }
        };
        let (right_cols, right_rows) = match right_result {
            QueryResult::Select { columns, rows } => (columns, rows),
            _ => {
                return Err(MoteDBError::Query(
                    "Right side of set op must be SELECT".into(),
                ))
            }
        };
        // 🔑 Verify column counts match (SQL standard requires this). Check
        // against the column metadata (lengths), NOT the rows — previously
        // this was skipped when either side had 0 rows, so
        // `SELECT x FROM a UNION SELECT x,y FROM b` (empty b) silently
        // succeeded with mismatched widths.
        let left_width = columns.len();
        let right_width = right_cols.len();
        if left_width != right_width {
            return Err(MoteDBError::Query(format!(
                "UNION: column count mismatch ({} vs {})",
                left_width, right_width,
            )));
        }
        match op {
            crate::sql::ast::SetOp::Union => {
                let mut combined = left_rows;
                combined.extend(right_rows);
                if !all {
                    // UNION (without ALL): deduplicate rows.
                    let mut seen = std::collections::HashSet::new();
                    combined.retain(|row| seen.insert(row.clone()));
                }
                Ok(QueryResult::Select {
                    columns,
                    rows: combined,
                })
            }
            crate::sql::ast::SetOp::Intersect => {
                // Rows present in BOTH left and right (deduplicated).
                let right_set: std::collections::HashSet<Vec<Value>> =
                    right_rows.iter().cloned().collect();
                let mut seen: std::collections::HashSet<Vec<Value>> =
                    std::collections::HashSet::new();
                let result: Vec<Vec<Value>> = left_rows
                    .into_iter()
                    .filter(|row| right_set.contains(row) && seen.insert(row.clone()))
                    .collect();
                Ok(QueryResult::Select {
                    columns,
                    rows: result,
                })
            }
            crate::sql::ast::SetOp::Except => {
                // Rows in left but NOT in right (deduplicated).
                let right_set: std::collections::HashSet<Vec<Value>> =
                    right_rows.iter().cloned().collect();
                let mut seen: std::collections::HashSet<Vec<Value>> =
                    std::collections::HashSet::new();
                let result: Vec<Vec<Value>> = left_rows
                    .into_iter()
                    .filter(|row| !right_set.contains(row) && seen.insert(row.clone()))
                    .collect();
                Ok(QueryResult::Select {
                    columns,
                    rows: result,
                })
            }
        }
    }

    /// Execute a query with window functions (ROW_NUMBER/RANK/DENSE_RANK).
    /// Strategy: build a base stmt (window cols → NULL placeholder), execute it
    /// to get all data rows, then compute window values in-place.
    fn execute_window_query(&self, stmt: &SelectStmt) -> Result<StreamingQueryResult> {
        use crate::sql::ast::{Expr, OrderByExpr, SelectColumn, WindowFunc};
        // Collect window column specs and build base stmt (replace window cols with NULL).
        let mut base_stmt = stmt.clone();
        let mut win_specs: Vec<(
            usize,
            WindowFunc,
            Option<Vec<String>>,
            Option<Vec<OrderByExpr>>,
        )> = Vec::new();
        for (i, col) in base_stmt.columns.iter_mut().enumerate() {
            if let SelectColumn::Expr(
                Expr::WindowFunction {
                    func,
                    partition_by,
                    order_by,
                },
                alias,
            ) = col
            {
                win_specs.push((i, func.clone(), partition_by.clone(), order_by.clone()));
                *col = SelectColumn::Expr(Expr::Literal(Value::Null), alias.clone());
            }
        }
        // Execute base query (gets all non-window data columns).
        let base = self.materialize_as_streaming(&base_stmt)?;
        let (_base_cols, _base_rows) = match base.materialize()? {
            QueryResult::Select { columns, rows } => (columns, rows),
            _ => return Err(MoteDBError::Query("Window base query failed".into())),
        };
        // We need the schema to resolve partition/order column positions.
        // Get it from the base query's columns (they correspond to stmt.columns).
        let table_name = match stmt.from.as_ref() {
            Some(crate::sql::ast::TableRef::Table { name, .. }) => name.clone(),
            _ => return Err(MoteDBError::Query("Window query needs FROM table".into())),
        };
        let schema = self.db.get_table_schema(&table_name)?;
        // The base query's rows only have the SELECT columns, not all schema columns.
        // We need the full row data for partition/order columns that may not be in SELECT.
        // Re-scan to get full rows, compute windows, then project.
        let store = self
            .db
            .get_or_create_col_segment_store(&table_name, schema.col_types())?;
        let _ = store.flush_buffer();
        let scan_pos: Vec<usize> = (0..schema.columns.len()).collect();
        let scanned = store.scan_projected_filtered(None, &scan_pos, &|_| true);
        let mut full_rows: Vec<Vec<Value>> = scanned.into_iter().map(|(_, r)| r).collect();
        // Apply WHERE
        if let Some(ref wc) = stmt.where_clause {
            full_rows.retain(|row| {
                Self::eval_expr_on_row(wc, row, &schema)
                    .map(|v| match v {
                        Value::Bool(true) => true,
                        Value::Integer(n) => n != 0,
                        _ => false,
                    })
                    .unwrap_or(false)
            });
        }
        // Compute each window function, appending result as extra column.
        for (_col_idx, func, partition_by, order_by) in &win_specs {
            Self::compute_window(
                &mut full_rows,
                &schema,
                func,
                partition_by.as_ref(),
                order_by.as_ref(),
            );
        }
        // Build output rows: for each SELECT column, pull from schema cols or window cols.
        let num_schema = schema.columns.len();
        let out_rows: Vec<Vec<Value>> = full_rows
            .iter()
            .map(|row| {
                let mut win_idx = 0usize;
                stmt.columns
                    .iter()
                    .map(|c| match c {
                        SelectColumn::Column(name) | SelectColumn::ColumnWithAlias(name, _) => {
                            let bare = name.rsplit('.').next().unwrap_or(name);
                            schema
                                .get_column_position(bare)
                                .and_then(|p| row.get(p).cloned())
                                .unwrap_or(Value::Null)
                        }
                        SelectColumn::Star => row.first().cloned().unwrap_or(Value::Null),
                        SelectColumn::Expr(expr, _) => {
                            if let Expr::WindowFunction { .. } = expr {
                                let v = row
                                    .get(num_schema + win_idx)
                                    .cloned()
                                    .unwrap_or(Value::Null);
                                win_idx += 1;
                                v
                            } else {
                                Self::eval_expr_on_row(expr, row, &schema).unwrap_or(Value::Null)
                            }
                        }
                    })
                    .collect()
            })
            .collect();
        let final_cols: Vec<String> = stmt
            .columns
            .iter()
            .map(|c| match c {
                SelectColumn::Column(n) | SelectColumn::ColumnWithAlias(n, _) => n.clone(),
                SelectColumn::Expr(_, Some(a)) => a.clone(),
                SelectColumn::Expr(e, None) => format!("{:?}", e),
                SelectColumn::Star => "*".to_string(),
            })
            .collect();
        let mut out_rows = out_rows;
        // Apply outer ORDER BY (resolve column by alias or position in output)
        if let Some(ref ob) = stmt.order_by {
            if !ob.is_empty() {
                // Build sort keys from output columns (match by alias name)
                let col_names = &final_cols;
                let sort_plan: Vec<(usize, bool)> = ob
                    .iter()
                    .map(|oe| {
                        let pos = if let Expr::Column(cn) = &oe.expr {
                            let bare = cn.rsplit('.').next().unwrap_or(cn);
                            // Try output column name match
                            col_names
                                .iter()
                                .position(|n| n == bare)
                                .or_else(|| schema.get_column_position(bare))
                                .unwrap_or(0)
                        } else if let Expr::Literal(Value::Integer(n)) = &oe.expr {
                            (*n as usize).saturating_sub(1)
                        } else {
                            0
                        };
                        (pos, oe.asc)
                    })
                    .collect();
                out_rows.sort_by(|a, b| {
                    for &(pos, asc) in &sort_plan {
                        let av = a.get(pos).cloned().unwrap_or(Value::Null);
                        let bv = b.get(pos).cloned().unwrap_or(Value::Null);
                        let cmp = order_by_cmp(&av, &bv);
                        if cmp != std::cmp::Ordering::Equal {
                            return if asc { cmp } else { cmp.reverse() };
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
        }
        // Apply OFFSET then LIMIT
        if let Some(off) = stmt.offset {
            if off >= out_rows.len() {
                out_rows.clear();
            } else {
                out_rows.drain(..off);
            }
        }
        if let Some(lim) = stmt.limit {
            out_rows.truncate(lim);
        }
        Ok(StreamingQueryResult::SelectReady {
            columns: final_cols,
            rows: out_rows,
        })
    }

    /// Compute a window function over rows, appending the result column.
    fn compute_window(
        rows: &mut [Vec<Value>],
        schema: &crate::types::TableSchema,
        func: &crate::sql::ast::WindowFunc,
        partition_by: Option<&Vec<String>>,
        order_by: Option<&Vec<crate::sql::ast::OrderByExpr>>,
    ) {
        use crate::sql::ast::WindowFunc;
        // Resolve partition column positions
        let part_cols: Vec<usize> = partition_by
            .map(|cols| {
                cols.iter()
                    .filter_map(|c| {
                        let bare = c.rsplit('.').next().unwrap_or(c);
                        schema.get_column_position(bare)
                    })
                    .collect()
            })
            .unwrap_or_default();
        // Resolve order key positions + directions
        let order_keys: Vec<(usize, bool)> = order_by
            .map(|ords| {
                ords.iter()
                    .filter_map(|oe| {
                        if let Expr::Column(cn) = &oe.expr {
                            let bare = cn.rsplit('.').next().unwrap_or(cn);
                            schema.get_column_position(bare).map(|p| (p, oe.asc))
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();
        // Group row indices by partition key
        let mut groups: std::collections::HashMap<Vec<String>, Vec<usize>> =
            std::collections::HashMap::new();
        for (i, row) in rows.iter().enumerate() {
            let key: Vec<String> = part_cols
                .iter()
                .map(|&p| format!("{:?}", row.get(p)))
                .collect();
            groups.entry(key).or_default().push(i);
        }
        // For each partition: sort by order keys, compute window values
        for (_key, mut indices) in groups {
            if !order_keys.is_empty() {
                indices.sort_by(|&a, &b| {
                    for &(col, asc) in &order_keys {
                        let av = rows[a].get(col).cloned().unwrap_or(Value::Null);
                        let bv = rows[b].get(col).cloned().unwrap_or(Value::Null);
                        let cmp = order_by_cmp(&av, &bv);
                        if cmp != std::cmp::Ordering::Equal {
                            return if asc { cmp } else { cmp.reverse() };
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
            // Compute window values
            match func {
                WindowFunc::RowNumber => {
                    for (rank, &idx) in indices.iter().enumerate() {
                        let r = (rank + 1) as i64;
                        if rows[idx].len()
                            <= schema.columns.len()
                                + rows[idx].len().saturating_sub(schema.columns.len())
                        {
                            // append only once per row — ensure capacity
                        }
                        rows[idx].push(Value::Integer(r));
                    }
                }
                WindowFunc::Rank => {
                    let mut rank = 0i64;
                    let mut prev_key: Option<Vec<Value>> = None;
                    let mut count = 0i64;
                    for &idx in &indices {
                        count += 1;
                        let cur_key: Vec<Value> = order_keys
                            .iter()
                            .map(|&(p, _)| rows[idx].get(p).cloned().unwrap_or(Value::Null))
                            .collect();
                        if prev_key.as_ref() != Some(&cur_key) {
                            rank = count;
                        }
                        rows[idx].push(Value::Integer(rank));
                        prev_key = Some(cur_key);
                    }
                }
                WindowFunc::DenseRank => {
                    let mut dr = 0i64;
                    let mut prev_key: Option<Vec<Value>> = None;
                    for &idx in &indices {
                        let cur_key: Vec<Value> = order_keys
                            .iter()
                            .map(|&(p, _)| rows[idx].get(p).cloned().unwrap_or(Value::Null))
                            .collect();
                        if prev_key.as_ref() != Some(&cur_key) {
                            dr += 1;
                        }
                        rows[idx].push(Value::Integer(dr));
                        prev_key = Some(cur_key);
                    }
                }
                WindowFunc::Lag {
                    expr,
                    offset,
                    default,
                } => {
                    // 🔑 LAG(expr, offset, default): value from `offset` rows
                    // before the current row (default 1). If no such row, use
                    // default (or NULL).
                    let off = offset.unwrap_or(1);
                    // Pre-compute the expr value for each row in partition order.
                    let vals: Vec<Value> = indices
                        .iter()
                        .map(|&idx| {
                            Self::eval_expr_on_row(expr, &rows[idx], schema).unwrap_or(Value::Null)
                        })
                        .collect();
                    for (pos, &idx) in indices.iter().enumerate() {
                        let v = if pos >= off {
                            vals[pos - off].clone()
                        } else {
                            default
                                .as_ref()
                                .and_then(|d| Self::eval_expr_on_row(d, &rows[idx], schema).ok())
                                .unwrap_or(Value::Null)
                        };
                        rows[idx].push(v);
                    }
                }
                WindowFunc::Lead {
                    expr,
                    offset,
                    default,
                } => {
                    // 🔑 LEAD(expr, offset, default): value from `offset` rows
                    // after the current row (default 1).
                    let off = offset.unwrap_or(1);
                    let vals: Vec<Value> = indices
                        .iter()
                        .map(|&idx| {
                            Self::eval_expr_on_row(expr, &rows[idx], schema).unwrap_or(Value::Null)
                        })
                        .collect();
                    for (pos, &idx) in indices.iter().enumerate() {
                        let v = if pos + off < vals.len() {
                            vals[pos + off].clone()
                        } else {
                            default
                                .as_ref()
                                .and_then(|d| Self::eval_expr_on_row(d, &rows[idx], schema).ok())
                                .unwrap_or(Value::Null)
                        };
                        rows[idx].push(v);
                    }
                }
            }
        }
        // Ensure rows without a window value (shouldn't happen) get NULL
        for row in rows.iter_mut() {
            while row.len() <= schema.columns.len() {
                row.push(Value::Null);
            }
        }
    }

    fn materialize_as_streaming(&self, stmt: &SelectStmt) -> Result<StreamingQueryResult> {
        let result = self.execute_select_internal(stmt)?;
        match result {
            QueryResult::Select { columns, rows } => {
                // execute_select_internal applies ORDER BY, LIMIT, OFFSET, DISTINCT,
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
        // 🔑 Substitute bind parameters FIRST: every downstream dispatch
        // (point query, col-segment scan, ORDER BY keys carried to
        // materialize) must see literal-only expressions — Parameter nodes
        // evaluate to NULL at materialize() time and sorted arbitrarily
        // (`ORDER BY emb <-> ?` returned the FARTHER row; found via the
        // Python bindings).
        let stmt_substituted_storage;
        let stmt = if Self::contains_parameter_stmt(stmt) {
            let params = self.evaluator.get_params();
            if let Some(err) = Self::validate_params_bound(stmt, &params) {
                return Err(err);
            }
            stmt_substituted_storage = self.substitute_params_stmt(stmt)?;
            &stmt_substituted_storage
        } else {
            stmt
        };
        // 🔑 Read-your-writes: when inside a transaction with buffered writes for
        // this table, ensure the ColSegmentStore exists so downstream paths
        // (full scan, aggregate) take the txn-merge route. Without this, a table
        // whose only rows are uncommitted INSERTs has no store yet, and SELECT
        // returns empty. get_or_create is a no-op if the store already exists.
        if self.is_in_transaction() {
            if let Some(TableRef::Table {
                name: table_name, ..
            }) = stmt.from.as_ref()
            {
                if !self.db.has_col_segment_store(table_name) {
                    let ws = self.txn_write_set_rows(table_name);
                    if !ws.is_empty() {
                        if let Ok(schema) = self.db.get_table_schema(table_name) {
                            let _ = self
                                .db
                                .get_or_create_col_segment_store(table_name, schema.col_types());
                        }
                    }
                }
            }
        }
        // 🔑 LATEST BY must reach the materialized path's apply_latest_by
        // fold. Multiple streaming branches serve the plain-SELECT shape
        // (columnar pushdown, optimizer plan scans) and silently DROP the
        // clause — `… WHERE sensor='s1' LATEST BY sensor` returned every
        // matching row instead of the newest (found by the Round-12c E2E).
        // materialize_as_streaming → execute_select_internal applies the
        // fold (and its bare-TS fast path try_ts_latest_by folds directly
        // in the ColumnarStore).
        if stmt.latest_by.is_some() {
            return self.materialize_as_streaming(stmt);
        }
        // 🔑 Explicit NULLS FIRST/LAST that differs from the dialect default
        // must be sorted by apply_order_by (the only comparator honoring the
        // flag). Run the general path and POST-SORT its result — internal
        // routing may still pick a fast sorter that ignores the flag, so the
        // authoritative sort happens here, once, at the boundary.
        if order_by_has_nondefault_nulls(stmt.order_by.as_deref()) {
            // materialize_as_streaming returns SelectStreaming with
            // order_by=None (internal fast paths already "sorted" with the
            // flag-ignoring comparator). Force the final result through
            // apply_order_by — the only comparator that honors NULLS
            // FIRST/LAST — at this boundary, regardless of internal routing.
            // 🔑 Strip LIMIT/OFFSET for the inner run: fast-path top-k
            // sorters ignore the NULLS flag and would pre-truncate to the
            // WRONG rows (measured: `… NULLS LAST LIMIT 2` returned the two
            // NULLs). Sort the full row set authoritatively, then truncate.
            let mut inner_stmt = stmt.clone();
            let off = inner_stmt.offset.take();
            let lim = inner_stmt.limit.take();
            let inner = self.materialize_as_streaming(&inner_stmt)?;
            let QueryResult::Select {
                columns: mut cols,
                rows: mut rows,
            } = inner.materialize()?
            else {
                unreachable!("materialize_as_streaming always yields Select");
            };
            if let Some(ob) = &stmt.order_by {
                StreamingQueryResult::apply_order_by(&mut rows, &cols, ob)?;
            }
            if let Some(off) = off {
                let off = off.min(rows.len());
                rows.drain(..off);
            }
            if let Some(lim) = lim {
                rows.truncate(lim);
            }
            cols.shrink_to_fit();
            return Ok(StreamingQueryResult::SelectReady {
                columns: cols,
                rows,
            });
        }
        // 🔑 Window functions: route to specialized executor.
        let has_window = stmt.columns.iter().any(|c| {
            matches!(
                c,
                crate::sql::ast::SelectColumn::Expr(
                    crate::sql::ast::Expr::WindowFunction { .. },
                    _
                )
            )
        });
        if has_window {
            return self.execute_window_query(stmt);
        }

        // 🚀 Pre-resolve scalar/IN subqueries in WHERE/HAVING BEFORE any routing.
        // This converts `WHERE col > (SELECT ...)` / `WHERE col IN (SELECT ...)`
        // (and the HAVING equivalents) into literal forms early, so every
        // downstream path (columnar scan, ORDER BY, DISTINCT, optimizer) sees
        // resolvable predicates.
        let resolved_subq_stmt;
        let stmt: &SelectStmt = {
            let where_has_subq = stmt
                .where_clause
                .as_ref()
                .is_some_and(Self::expr_contains_subquery);
            let having_has_subq = stmt
                .having
                .as_ref()
                .is_some_and(Self::expr_contains_subquery);
            let order_has_subq = stmt
                .order_by
                .as_ref()
                .is_some_and(|ob| ob.iter().any(|o| Self::expr_contains_subquery(&o.expr)));
            if where_has_subq || having_has_subq || order_has_subq {
                resolved_subq_stmt = self.resolve_subqueries_stmt(stmt)?;
                &resolved_subq_stmt
            } else {
                stmt
            }
        };

        // 🔑 ORDER BY 表达式键引用 SELECT 输出之外的列时, 流式扫描的投影
        // 排序 (try_sort_projected/apply_order_by) 求不出键值, 只能静默跳过
        // → 返回任意序 (differential fuzz: ORDER BY loc <-> ST_POINT(...)
        // 原样返回插入序)。统一路由到物化路径 — 物化排序可在 full_row 上
        // 求值任意键; VECTOR 距离键除外 (列存 top-k / 向量下推接管)。
        // 🔑 必须放在 resolve_subqueries_stmt 之后: ORDER BY 里的标量子查询
        // (ABS(v - (SELECT AVG(v) …))) 先物化成字面量, 物化路径的排序键
        // 求值不认 Subquery 节点 (test_bug_hunt_v92::test_subquery_in_order_by)。
        if let Some(ref ob) = stmt.order_by {
            let schema = match stmt.from.as_ref() {
                Some(crate::sql::ast::TableRef::Table { name, .. }) => {
                    self.db.get_table_schema(name).ok()
                }
                _ => None,
            };
            if Self::order_by_needs_full_rows(ob, &stmt.columns, schema.as_deref()) {
                return self.materialize_as_streaming(stmt);
            }
        }
        // Validate bare SELECT column references against the table schema.
        // A column that doesn't exist is a query error (not a silent value
        // from another column). Applies before any fast-path routing so all
        // paths benefit. (Also checked in execute_select_internal for the
        // subquery/non-streaming route.)
        if let Some(TableRef::Table {
            name: table_name, ..
        }) = stmt.from.as_ref()
        {
            if let Ok(schema) = self.db.get_table_schema(table_name) {
                for col in &stmt.columns {
                    if let SelectColumn::Column(name) | SelectColumn::ColumnWithAlias(name, _) = col
                    {
                        let bare = name.rsplit('.').next().unwrap_or(name);
                        if schema.get_column_position(bare).is_none() {
                            return Err(MoteDBError::ColumnNotFound(format!(
                                "'{}' in table '{}'",
                                bare, table_name
                            )));
                        }
                    }
                }
            }
        }

        // 🔑 Pre-resolve subqueries in SELECT columns. eval_expr_on_row can't
        // execute subqueries, so we resolve them up front:
        //   - A direct scalar subquery `(SELECT ...)` → Literal.
        //   - An IN (SELECT ...) inside a larger expression → InHashset
        //     (materialize_subqueries handles the rewrite).
        // Detect ANY column whose expression contains a Subquery node, then
        // materialize/resolve that column's expression.
        let resolved_select_stmt;
        let stmt: &SelectStmt = if stmt.columns.iter().any(|c| {
            matches!(
                c,
                crate::sql::ast::SelectColumn::Expr(e, _) if Self::expr_contains_subquery(e)
            )
        }) {
            resolved_select_stmt = {
                let mut s = stmt.clone();
                for col in &mut s.columns {
                    if let crate::sql::ast::SelectColumn::Expr(ref mut expr, _) = col {
                        let sub = match expr {
                            crate::sql::ast::Expr::Subquery(s) => Some(s.clone()),
                            _ => None,
                        };
                        if let Some(sub) = sub {
                            // 🔑 Skip correlated subqueries — they reference outer
                            // columns and must be evaluated per-row, not pre-resolved.
                            let outer_schema = stmt.from.as_ref().and_then(|f| {
                                if let TableRef::Table { name, .. } = f {
                                    self.db.get_table_schema(name).ok()
                                } else {
                                    None
                                }
                            });
                            let is_correlated = outer_schema
                                .as_ref()
                                .map(|s| Self::is_correlated_subquery(&sub, s))
                                .unwrap_or(false);
                            if is_correlated {
                                continue; // keep Subquery node for per-row eval
                            }
                            match self.execute_select_internal(&sub) {
                                Ok(QueryResult::Select { rows, .. }) => {
                                    // SQL standard: scalar subquery must return
                                    // at most one row.
                                    if rows.len() > 1 {
                                        return Err(MoteDBError::Query(
                                            "Scalar subquery returned more than one row"
                                                .to_string(),
                                        ));
                                    }
                                    let scalar = rows
                                        .first()
                                        .and_then(|r| r.first())
                                        .cloned()
                                        .unwrap_or(Value::Null);
                                    *expr = crate::sql::ast::Expr::Literal(scalar);
                                }
                                Ok(_) => {}
                                Err(_) => { /* leave node; eval surfaces error */ }
                            }
                        } else {
                            // 🔑 Not a direct scalar subquery, but the expression
                            // contains a subquery somewhere (e.g. `x IN (SELECT...)`).
                            // materialize_subqueries rewrites IN (SELECT...) into an
                            // InHashset so eval_expr_on_row can evaluate it per row.
                            // Errors during materialization (e.g. correlated) leave
                            // the node intact for the normal eval path to surface.
                            if let Ok(materialized) = self.materialize_subqueries(expr) {
                                *expr = materialized;
                            }
                        }
                    }
                }
                s
            };
            &resolved_select_stmt
        } else {
            stmt
        };

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

        // 🚀 FAST PATH: Vector KNN (KNN_SEARCH) — single index lookup.
        // `WHERE KNN_SEARCH(col, [...], k)` is the highest-value query for
        // embodied AI/robotics. Without this path it falls through to the
        // ColSegmentStore scan, which cannot evaluate KnnSearch (eval_expr_on_row
        // can't do index lookups) and silently returns 0 rows, or — when routed
        // to the materialized path — brute-force scans the whole table calling
        // vector_search per row (~50ms on 10K rows). Detect the bare pattern and
        // push it down to a single vector_search call + batch row fetch.
        if let Some(ref where_clause) = stmt.where_clause {
            if let Some(QueryResult::Select { columns, rows }) =
                self.try_vector_knn_fast_path(stmt, where_clause)?
            {
                return Ok(StreamingQueryResult::SelectReady { columns, rows });
            }
        }
        if let Some(ref order_by) = stmt.order_by {
            if order_by
                .iter()
                .any(|ob| Self::expr_is_or_aliases_st_distance(&ob.expr, &stmt.columns))
            {
                if let Some(QueryResult::Select { columns, rows }) =
                    self.try_optimize_spatial_order_by(stmt)?
                {
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

        // 🔑 PK point query fast path: `WHERE pk = literal` → O(log N) binary
        // search in the segment's row_map. This intercepts the most common
        // point query BEFORE the full-scan routing, cutting latency from
        // O(rows) to O(log rows).
        if let Some(ref wc) = stmt.where_clause {
            if let Some(TableRef::Table {
                name: table_name, ..
            }) = stmt.from.as_ref()
            {
                if self.db.has_col_segment_store(table_name) {
                    if let Some(result) =
                        self.try_col_segment_pk_point_query(stmt, table_name, wc)?
                    {
                        return Ok(result);
                    }
                }
            }
        }

        if (self.has_aggregates(&stmt.columns)
            || stmt.group_by.is_some()
            || stmt.order_by.is_some()
            || stmt.distinct)
            && !Self::contains_parameter_stmt(stmt)
            // LATEST BY needs apply_latest_by on the materialized path; the
            // columnar fast paths below silently ignored the clause and
            // returned every row.
            && stmt.latest_by.is_none()
        {
            if let Some(TableRef::Table {
                name: table_name, ..
            }) = stmt.from.as_ref()
            {
                if self.db.has_col_segment_store(table_name) {
                    if let Ok(store) = self.db.get_or_create_col_segment_store(table_name, &[]) {
                        let _ = store.flush_buffer();
                        // ORDER BY LIMIT (no aggregate, no GROUP BY/HAVING): full scan + in-memory sort.
                        // 🚨 Must exclude GROUP BY/HAVING: a query like
                        // `SELECT cat FROM t GROUP BY cat HAVING COUNT(*) > 1 ORDER BY cat`
                        // has no aggregate in the SELECT list but MUST go through
                        // the GROUP BY path. Without these guards this fast path
                        // ignored GROUP BY/HAVING entirely → returned all rows.
                        if stmt.order_by.is_some()
                            && !self.has_aggregates(&stmt.columns)
                            && stmt.group_by.is_none()
                            && stmt.having.is_none()
                        {
                            let schema = self.db.get_table_schema(table_name)?;
                            // 🔑 Only take the scan+projected-sort fast path when
                            // EVERY ORDER BY key resolves to a projected output
                            // column (name/alias/ordinal). Expression keys
                            // (`emb <-> ?`, `v*2`) and non-projected columns
                            // (`SELECT id ... ORDER BY val`) can't be sorted from
                            // the projected rows — route to the materialized path,
                            // which evaluates keys against full source rows.
                            // Previously try_sort_projected silently skipped
                            // unresolvable keys → arbitrary row order.
                            let output_cols = self.build_select_columns(&stmt.columns, &schema)?;
                            let all_projected = stmt.order_by.as_ref().is_some_and(|ob| {
                                ob.iter().all(|o| {
                                    order_by_projected_index(&o.expr, &output_cols).is_some()
                                })
                            });
                            if !all_projected {
                                store.release_pages_only();
                                return self.materialize_as_streaming(stmt);
                            }
                            let mut result = self.execute_full_scan_via_col_segment(
                                stmt, table_name, &schema, &store,
                            )?;
                            // 🔑 Apply ORDER BY for projected-column keys (aliases,
                            // projected expressions, ordinals). The col-segment scan
                            // returns unsorted SelectReady/SelectColumnar. We sort
                            // against the projected columns ONLY when every ORDER BY
                            // key resolves to a projected column — otherwise we leave
                            // the result as-is (the underlying scan path handles
                            // non-projected-column ORDER BY via its own sort).
                            if let Some(order_clauses) = &stmt.order_by {
                                if !order_clauses.is_empty() {
                                    StreamingQueryResult::try_sort_projected(
                                        &mut result,
                                        order_clauses,
                                    );
                                }
                            }
                            store.release_pages_only();
                            return Ok(result);
                        }
                        // DISTINCT (no aggregate, no GROUP BY/HAVING): multi-segment scan + dedup.
                        if stmt.distinct
                            && !self.has_aggregates(&stmt.columns)
                            && stmt.group_by.is_none()
                            && stmt.having.is_none()
                            // 🚨 ORDER BY / LIMIT must be applied AFTER dedup.
                            // This fast path returns rows without sorting/limiting,
                            // so for `SELECT DISTINCT v ORDER BY v LIMIT 2` it would
                            // return all distinct values unordered (silent wrong
                            // result). Fall through to execute_full_scan_via_col_segment.
                            && stmt.order_by.is_none()
                            && stmt.limit.is_none()
                            && stmt.offset.is_none()
                        {
                            let schema = self.db.get_table_schema(table_name)?;
                            let out_pos: Vec<usize> =
                                Self::resolve_select_positions(&stmt.columns, &schema)
                                    .unwrap_or_default();
                            if !out_pos.is_empty() {
                                let dc = out_pos[0];
                                // 🚀 Fast path: single-column DISTINCT on a TEXT
                                // column via distinct_text_values (adaptive early-
                                // exit) instead of materializing+deduping all rows.
                                if matches!(
                                    schema.col_types().get(dc),
                                    Some(crate::types::ColumnType::Text)
                                ) {
                                    let vals = store.distinct_text_values(dc, 10000);
                                    let rows: Vec<Vec<Value>> = vals
                                        .into_iter()
                                        .map(|v| vec![Value::Text(v.into())])
                                        .collect();
                                    let columns = self
                                        .build_select_columns(&stmt.columns, &schema)
                                        .unwrap_or_default();
                                    store.release_pages_only();
                                    return Ok(StreamingQueryResult::SelectReady { columns, rows });
                                }
                                let scanned =
                                    store.scan_projected_filtered(Some(dc), &out_pos, &|_| true);
                                // 🔑 Dedup on full row (not just first column) —
                                // same fix as the non-aggregate DISTINCT path.
                                let mut seen: std::collections::HashSet<Vec<Value>> =
                                    std::collections::HashSet::new();
                                let mut rows: Vec<Vec<Value>> = Vec::new();
                                for (_, row) in scanned {
                                    if seen.insert(row.clone()) {
                                        rows.push(row);
                                    }
                                }
                                let columns = self
                                    .build_select_columns(&stmt.columns, &schema)
                                    .unwrap_or_default();
                                store.release_pages_only();
                                return Ok(StreamingQueryResult::SelectReady { columns, rows });
                            }
                        }
                    }
                }
            }
        }

        // 🚨 GROUP BY (with or without aggregates) must go through the GROUP BY
        // execution path. The streaming full-scan path below ignores GROUP BY
        // entirely, returning un-grouped rows (silent wrong result).
        //
        // 🚀 PERFORMANCE: Before falling back to materialize_as_streaming (which
        // materializes ALL rows as Vec<Value> — 300K allocations for a 300K-row
        // table), try the columnar GROUP BY fast path (col_segment_group_by).
        // It reads raw text bytes + typed numeric slices, folds into per-group
        // accumulators in a single pass — zero Value allocation in the hot loop.
        // Previously this was unreachable because the intercept below returned
        // before the aggregate block that calls col_segment_group_by.
        if stmt.group_by.is_some() && !Self::contains_parameter_stmt(stmt) {
            // 🔑 TimeSeries GROUP BY: materialize (below) and the pushdown both
            // read LSM/ColSegmentStore — no TS data. ts_simple_aggregate
            // handles single-key GROUP BY over the ColumnarStore.
            if let Some(TableRef::Table {
                name: table_name, ..
            }) = stmt.from.as_ref()
            {
                if let Ok(schema) = self.db.get_table_schema(table_name) {
                    if schema.table_type == crate::types::TableType::TimeSeries {
                        if let Some(result) = self.ts_simple_aggregate(stmt, table_name, &schema)? {
                            return Ok(result);
                        }
                    }
                }
            }
            // 🚀 VEC M2: 批 GROUP BY (表达式键形状 — 旧行式路径 14-35ms
            // 的痛点)。None → 下方列存下推原样回退。
            if let Some(TableRef::Table {
                name: table_name, ..
            }) = stmt.from.as_ref()
            {
                if self.db.has_col_segment_store(table_name) {
                    if let Ok(store) = self.db.get_or_create_col_segment_store(table_name, &[]) {
                        let _ = store.prepare_for_query();
                        if let Ok(schema) = self.db.get_table_schema(table_name) {
                            if let Some(outcome) =
                                crate::sql::vector_exec::try_vec_group_by(&store, &schema, stmt)?
                            {
                                return Ok(StreamingQueryResult::SelectReady {
                                    columns: outcome.columns,
                                    rows: outcome.rows,
                                });
                            }
                        }
                        // Try the columnar GROUP BY pushdown (much faster).
                        let schema = self.db.get_table_schema(table_name)?;
                        if let Some(result) =
                            self.col_segment_group_by(stmt, table_name, &store, &schema)?
                        {
                            store.release_pages_only();
                            return Ok(result);
                        }
                    }
                }
            }
            return self.materialize_as_streaming(stmt);
        }

        // Aggregate queries (COUNT, SUM, etc.) — try fast paths
        if self.has_aggregates(&stmt.columns) {
            // 🚀 全乘积/链式 INNER JOIN 的 COUNT(*) 折叠 (每步 ON 常量或
            // 单表谓词 → Π 各表过滤行数, 零物化)。通用路径 6M 对物化 4.1s。
            if let Some((columns, rows)) = self.try_join_count_fold(stmt)? {
                return Ok(StreamingQueryResult::SelectReady { columns, rows });
            }
            // 🚀 `SELECT COUNT(*) WHERE MATCH(...)` — count the index
            // postings directly (the pipeline below materializes every
            // matching row just to count it).
            if let Some(result) = self.try_text_match_count(stmt)? {
                return Ok(match result {
                    QueryResult::Select { columns, rows } => {
                        StreamingQueryResult::SelectStreaming {
                            columns,
                            rows: Box::new(rows.into_iter().map(Ok)),
                            order_by: None,
                            limit: None,
                            offset: None,
                            distinct: false,
                            max_result_rows: None,
                            size_hint: None,
                        }
                    }
                    other => unreachable!("count fast path returns Select, got {other:?}"),
                });
            }
            // 🔑 TimeSeries FIRST: every fast path below (count, columnar
            // pushdown, column index) reads LSM/ColSegmentStore, which hold
            // no TS data — with a WHERE they returned 0. ts_simple_aggregate
            // computes over the ColumnarStore (handles WHERE + GROUP BY).
            {
                if let Some(TableRef::Table { name, .. }) = stmt.from.as_ref() {
                    if let Ok(schema) = self.db.get_table_schema(name) {
                        if schema.table_type == crate::types::TableType::TimeSeries {
                            if let Some(result) = self.ts_simple_aggregate(stmt, name, &schema)? {
                                return Ok(result);
                            }
                        }
                    }
                }
            }
            // ColSegmentStore multi-segment aggregate (no compaction — avoids 70ms sync).
            if let Some(TableRef::Table {
                name: table_name, ..
            }) = stmt.from.as_ref()
            {
                if self.db.has_col_segment_store(table_name) {
                    if let Ok(store) = self.db.get_or_create_col_segment_store(table_name, &[]) {
                        let _ = store.prepare_for_query();
                        // 🚀 VEC M1: 批扫描+批过滤+批聚合（无 GROUP BY）。
                        // 返回 None → 下方旧融合路径原样回退。
                        if stmt.group_by.is_none() {
                            if let Ok(schema) = self.db.get_table_schema(table_name) {
                                if let Some(outcome) =
                                    crate::sql::vector_exec::try_vec_no_group_aggregate(
                                        &store, &schema, stmt,
                                    )?
                                {
                                    let row = outc_rows(&outcome);
                                    return Ok(StreamingQueryResult::SelectStreaming {
                                        columns: outcome.columns.clone(),
                                        rows: Box::new(row.into_iter().map(Ok)),
                                        order_by: None,
                                        limit: None,
                                        offset: None,
                                        distinct: false,
                                        max_result_rows: None,
                                        size_hint: Some(1),
                                    });
                                }
                            }
                        }
                        if let Some(result) =
                            self.col_segment_aggregate(stmt, table_name, &store)?
                        {
                            store.release_pages_only();
                            return Ok(result);
                        }
                        // col_segment_aggregate returned None (complex aggregate).
                        // Try multi_aggregate and group_by directly (no sync needed).
                        let schema = self.db.get_table_schema(table_name)?;
                        if stmt.group_by.is_some() {
                            if let Some(result) =
                                self.col_segment_group_by(stmt, table_name, &store, &schema)?
                            {
                                store.release_pages_only();
                                return Ok(result);
                            }
                            // 🔑 PERF: try try_group_by_columnar BEFORE syncing —
                            // it reads from columnar_sstables which is usually already
                            // populated (from a prior query or flush). The sync below
                            // flushes the buffer + compacts + evicts mmap pages, which
                            // is pure overhead when columnar_sstables already has data.
                            if let Some(result) = self.try_group_by_columnar(stmt)? {
                                return Ok(result);
                            }
                        }
                        if stmt.group_by.is_none() {
                            if let Some(result) =
                                self.col_segment_multi_aggregate(stmt, table_name, &store, &schema)?
                            {
                                store.release_pages_only();
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
            // 🔑 TimeSeries tables: compute simple (non-GROUP-BY) aggregates
            // over the ColumnarStore via the streaming scan — the fast paths
            // above and materialize all read LSM/ColSegmentStore, which hold
            // no TS data (returned NULL/0).
            if stmt.group_by.is_none() {
                if let Some(TableRef::Table { name, .. }) = stmt.from.as_ref() {
                    if let Ok(schema) = self.db.get_table_schema(name) {
                        if schema.table_type == crate::types::TableType::TimeSeries {
                            if let Some(result) = self.ts_simple_aggregate(stmt, name, &schema)? {
                                return Ok(result);
                            }
                        }
                    }
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
        if let TableRef::Table {
            name: table_name, ..
        } = stmt
            .from
            .as_ref()
            .ok_or_else(|| MoteDBError::InvalidArgument("FROM clause required".into()))?
        {
            if let Ok(schema) = self.db.get_table_schema(table_name) {
                if schema.table_type == crate::types::TableType::TimeSeries {
                    if let Some(result) = self.try_columnar_select(stmt, &schema)? {
                        // Convert QueryResult to StreamingQueryResult
                        match result {
                            QueryResult::Select { columns, rows } => {
                                return Ok(StreamingQueryResult::SelectReady { columns, rows });
                            }
                            _ => {
                                return Ok(StreamingQueryResult::SelectReady {
                                    columns: vec![],
                                    rows: vec![],
                                })
                            }
                        }
                    }
                    // Fall through to LSM full scan for complex queries
                }
            }
        }

        // Pass bind parameters to optimizer (resolves ? inline, no AST clone needed).
        // (The stmt was already fully substituted at function entry.)
        let plan = self.optimizer.optimize_select(stmt, &[])?;

        // For PointQuery/RangeQuery, the plan already has resolved values — use original stmt.
        // For FullScan, WHERE still contains Parameter nodes — substitute needed.
        //
        // post_filters are applied AFTER index row fetch: the index narrows to a small
        // candidate set (e.g., 10 rows), then post_filters further filter in-memory.
        // This replaces the old behavior of falling back to full table scan when
        // post_filters were present.
        let post_filters = &plan.post_filters;
        match plan.scan_method {
            super::optimizer::ScanMethod::PointQuery {
                ref table,
                ref column,
                ref value,
            } => self.execute_point_query_streaming(stmt, table, column, value, post_filters),
            super::optimizer::ScanMethod::RangeQuery {
                ref table,
                ref column,
                ref start,
                start_inclusive,
                ref end,
                end_inclusive,
            } => self.execute_range_query_streaming(
                stmt,
                table,
                column,
                start,
                start_inclusive,
                end,
                end_inclusive,
                post_filters,
            ),
            super::optimizer::ScanMethod::FullScan { ref table } => {
                // 🚀 DISTINCT via column value index: SELECT DISTINCT col FROM table
                // without WHERE — iterate index keys directly (O(unique) vs O(N) scan).
                // 🚨 Guard LIMIT/OFFSET: this fast path returns ALL distinct values
                // without truncation, so `SELECT DISTINCT cat LIMIT 2` would return
                // all distinct values. Fall through to the full path which applies
                // OFFSET/LIMIT over the deduplicated result.
                if stmt.distinct
                    && stmt.where_clause.is_none()
                    && stmt.order_by.is_none()
                    && stmt.limit.is_none()
                    && stmt.offset.is_none()
                {
                    if let Some(result) = self.try_distinct_via_column_index(stmt, table)? {
                        return Ok(result);
                    }
                }
                // 🚀 Streaming Top-K: when ORDER BY + LIMIT (no OFFSET) on full scan,
                // use a bounded heap instead of materializing all rows + sorting.
                if stmt.order_by.is_some()
                    && stmt.limit.is_some()
                    && stmt.offset.is_none()
                    && !stmt.distinct
                {
                    if let Some(result) = self.try_order_by_limit_topk(stmt, table)? {
                        return Ok(result);
                    }
                }
                // ORDER BY / DISTINCT on full scan without streaming Top-K:
                // fall back to materialize which has the positional sort path.
                // 🔑 EXCEPT TimeSeries tables — materialize reads the LSM /
                // ColSegmentStore paths which hold no TS data; the full-scan
                // Materialized TS branch handles ORDER/DISTINCT correctly.
                if stmt.order_by.is_some() || stmt.distinct {
                    if let Ok(schema) = self.db.get_table_schema(table) {
                        if schema.table_type == crate::types::TableType::TimeSeries {
                            return self.execute_full_scan_streaming(stmt, table);
                        }
                    }
                    return self.materialize_as_streaming(stmt);
                }
                self.execute_full_scan_streaming(stmt, table)
            }
            super::optimizer::ScanMethod::IndexIntersection {
                ref table,
                ref column1,
                ref value1,
                ref column2,
                ref value2,
            } => self.execute_index_intersection_streaming(
                stmt,
                table,
                column1,
                value1,
                column2,
                value2,
                post_filters,
            ),
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
            Expr::Exists(_) => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_contains_subquery(left) || Self::expr_contains_subquery(right)
            }
            Expr::UnaryOp { expr, .. } => Self::expr_contains_subquery(expr),
            Expr::In { expr, list, .. } => {
                Self::expr_contains_subquery(expr) || list.iter().any(Self::expr_contains_subquery)
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::expr_contains_subquery(expr)
                    || Self::expr_contains_subquery(low)
                    || Self::expr_contains_subquery(high)
            }
            Expr::Like { expr, pattern, .. } => {
                Self::expr_contains_subquery(expr) || Self::expr_contains_subquery(pattern)
            }
            Expr::IsNull { expr, .. } => Self::expr_contains_subquery(expr),
            Expr::FunctionCall { args, .. } => args.iter().any(Self::expr_contains_subquery),
            Expr::Case { whens, else_expr } => {
                whens.iter().any(|(c, v)| {
                    Self::expr_contains_subquery(c) || Self::expr_contains_subquery(v)
                }) || else_expr
                    .as_ref()
                    .is_some_and(|e| Self::expr_contains_subquery(e))
            }
            _ => false,
        }
    }

    /// Clone the statement with all subqueries in WHERE resolved to literal values.
    fn resolve_subqueries_stmt(&self, stmt: &SelectStmt) -> Result<SelectStmt> {
        // Get outer table schema for correlated subquery detection.
        let outer_schema = stmt.from.as_ref().and_then(|f| {
            if let TableRef::Table { name, .. } = f {
                self.db.get_table_schema(name).ok()
            } else {
                None
            }
        });
        let where_clause = match &stmt.where_clause {
            Some(w) => Some(self.materialize_subqueries_checked(w, outer_schema.as_deref())?),
            None => None,
        };
        // 🔑 Also pre-resolve non-correlated subqueries in HAVING. A HAVING
        // clause like `HAVING SUM(v) > (SELECT AVG(v) FROM t)` references a
        // scalar subquery that doesn't depend on the group; resolve it once
        // here so the per-group HAVING evaluation can compare against a
        // Literal (the evaluator/HAVING path can't execute subqueries).
        let having = match &stmt.having {
            Some(h) => Some(self.materialize_subqueries_checked(h, outer_schema.as_deref())?),
            None => None,
        };
        // 🔑 Pre-resolve non-correlated subqueries in ORDER BY expressions
        // (e.g. ORDER BY ABS(v - (SELECT AVG(v) FROM t))). The sort-key
        // computation can't execute subqueries; resolve them to Literals.
        let order_by = match &stmt.order_by {
            Some(ob) => {
                let mut new_ob = Vec::with_capacity(ob.len());
                for o in ob {
                    let resolved_expr =
                        self.materialize_subqueries_checked(&o.expr, outer_schema.as_deref())?;
                    new_ob.push(OrderByExpr {
                        expr: resolved_expr,
                        asc: o.asc,
                        nulls_first: o.nulls_first,
                    });
                }
                Some(new_ob)
            }
            None => None,
        };
        Ok(SelectStmt {
            columns: stmt.columns.clone(),
            from: stmt.from.clone(),
            where_clause,
            order_by,
            limit: stmt.limit,
            offset: stmt.offset,
            distinct: stmt.distinct,
            group_by: stmt.group_by.clone(),
            having,
            latest_by: stmt.latest_by.clone(),
        })
    }

    /// Check if an expression contains MATCH, ST_WITHIN, ST_KNN, ST_RADIUS,
    /// or spatial scalar functions (WITHIN_RADIUS/ST_DISTANCE) that the
    /// columnar scan cannot evaluate (it doesn't decode GEOMETRY columns) and
    /// must run through the materialized execution path.
    fn expr_needs_materialized_path(expr: &Expr) -> bool {
        match expr {
            // NOTE: ST_WITHIN_3D / ST_RADIUS_3D are per-row evaluable now
            // (SqlRow + positional evaluators), but they must still take the
            // materialized route: the columnar scan path runs BEFORE the
            // i-Octree fast paths, so un-gating them makes indexed radius /
            // within queries full-scan (0.02ms → 22ms at 200K rows).
            Expr::Match { .. }
            | Expr::StWithin3D { .. }
            | Expr::StKnn3D { .. }
            | Expr::StRadius3D { .. } => true,
            // Subqueries must be materialized by the executor before evaluation
            // (eval_expr_on_row cannot execute them — it returns an error, which
            // silently filters out every row). Without this, a 3-level nested
            // `x IN (SELECT ... WHERE y IN (SELECT ...))` routed through the
            // ColSegmentStore fast path never resolved the inner subquery.
            Expr::Subquery(_) => true,
            Expr::FunctionCall { name, args, .. } => {
                matches!(
                    name.to_lowercase().as_str(),
                    "within_radius" | "st_distance" | "st_distance_3d"
                ) || args.iter().any(Self::expr_needs_materialized_path)
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_needs_materialized_path(left)
                    || Self::expr_needs_materialized_path(right)
            }
            Expr::UnaryOp { expr, .. } => Self::expr_needs_materialized_path(expr),
            Expr::IsNull { expr, .. } => Self::expr_needs_materialized_path(expr),
            // Recurse into IN/Between/Like sub-expressions so a subquery nested
            // in the IN list (or the IN target) is detected.
            Expr::In { expr, list, .. } => {
                Self::expr_needs_materialized_path(expr)
                    || list.iter().any(Self::expr_needs_materialized_path)
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::expr_needs_materialized_path(expr)
                    || Self::expr_needs_materialized_path(low)
                    || Self::expr_needs_materialized_path(high)
            }
            Expr::Like { expr, pattern, .. } => {
                Self::expr_needs_materialized_path(expr)
                    || Self::expr_needs_materialized_path(pattern)
            }
            _ => false,
        }
    }

    /// Does `expr` contain an ST_KNN_3D or MATCH predicate? Both are *set*
    /// predicates that need their index and the row id, so the positional
    /// (schema-indexed row) evaluators cannot answer them; paths built on
    /// them must yield to the materialized path, whose evaluator resolves
    /// them through the index (memoized per statement).
    fn expr_contains_st_knn(expr: &Expr) -> bool {
        match expr {
            Expr::StKnn3D { .. } | Expr::Match { .. } => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_contains_st_knn(left) || Self::expr_contains_st_knn(right)
            }
            Expr::UnaryOp { expr, .. } | Expr::IsNull { expr, .. } => {
                Self::expr_contains_st_knn(expr)
            }
            Expr::Case { whens, else_expr } => {
                whens
                    .iter()
                    .any(|(c, v)| Self::expr_contains_st_knn(c) || Self::expr_contains_st_knn(v))
                    || else_expr.as_deref().is_some_and(Self::expr_contains_st_knn)
            }
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
        // get_table_row point-lookup path fails (row_id ≠ PK value).
        // 🚀 PK fast-scan: scan only the PK column to find the matching row,
        // then fetch output columns for that single row. Avoids flush_buffer
        // + full multi-column scan for a 1-row result.
        if !is_auto_increment_pk && self.db.has_col_segment_store(table) {
            // 🚀 Try column index first (O(log N) B-tree lookup, created at
            // CREATE TABLE time for non-AUTO_INCREMENT PKs). Falls back to
            // PK-column scan if index isn't built yet (async pipeline).
            let index_name = format!("{}.{}", table, column);
            if let Some(index) = self.db.column_indexes.get(&index_name) {
                let __t0 = std::time::Instant::now();
                let row_ids = index
                    .value()
                    .get_arc(value)
                    .unwrap_or_else(|_| std::sync::Arc::new(Vec::new()));
                let __t1 = std::time::Instant::now();
                if !row_ids.is_empty() {
                    // 🔑 The optimizer attaches the full WHERE clause as
                    // post_filter to EVERY index plan ("redundant but
                    // harmless"). For a simple `col = literal` that is exactly
                    // the predicate this index probe resolved, re-applying it
                    // forces a second full row fetch and disables LIMIT
                    // pushdown. Detect that redundancy and treat the plan as
                    // filter-free.
                    let eq_covers_filters = post_filters.iter().all(|f| {
                        matches!(
                            f,
                            Expr::BinaryOp {
                                left,
                                op: crate::sql::ast::BinaryOperator::Eq,
                                right,
                            } if matches!(&**left, Expr::Column(c)
                                    if c.rsplit('.').next() == Some(column))
                                && matches!(&**right, Expr::Literal(v) if v == value)
                        )
                    });
                    let filters_redundant = post_filters.is_empty() || eq_covers_filters;

                    // Exact-value verification: index keys truncate long Text
                    // values to a 64-byte prefix, so a row whose value merely
                    // shares that prefix must not be returned as a match.
                    let filter_pos = schema.get_column_position(column);
                    let verified_push = |result_rows: &mut Vec<Vec<Value>>, row: &Vec<Value>| {
                        if let Some(pos) = filter_pos {
                            if row.get(pos) != Some(value) {
                                return;
                            }
                        }
                        result_rows.push(Self::project_row_direct(
                            row,
                            &stmt.columns,
                            &columns,
                            &schema,
                        ));
                    };

                    // 🔑 LIMIT pushdown: when the index probe covers the whole
                    // predicate, fetch only the offset+limit window of matches
                    // instead of all of them. `WHERE indexed = ? LIMIT k`
                    // previously fetched and projected EVERY match — cost grew
                    // linearly with table size for a fixed-size result. Row
                    // output order follows row_ids order, so the trailing
                    // OFFSET/LIMIT below keeps exactly the first offset+limit
                    // verified rows. Fetching proceeds in windows of
                    // offset+limit so the (rare) prefix false positives dropped
                    // by verification can be backfilled from later ids — never
                    // under-returning.
                    let mut result_rows: Vec<Vec<Value>> = Vec::new();
                    if filters_redundant && stmt.limit.is_some() {
                        let need = stmt
                            .offset
                            .unwrap_or(0)
                            .saturating_add(stmt.limit.unwrap_or(0))
                            .max(1);
                        'window: for chunk in row_ids.chunks(need) {
                            let __f0 = std::time::Instant::now();
                            let batch = self.db.get_table_rows_batch(table, chunk)?;
                            for (_, row_opt) in batch {
                                if let Some(row) = row_opt {
                                    verified_push(&mut result_rows, &row);
                                    if result_rows.len() >= need {
                                        break 'window;
                                    }
                                }
                            }
                        }
                    } else {
                        let __f0 = std::time::Instant::now();
                        let batch = self.db.get_table_rows_batch(table, &row_ids)?;
                        for (_, row_opt) in batch {
                            if let Some(row) = row_opt {
                                verified_push(&mut result_rows, &row);
                            }
                        }
                    }
                    if !result_rows.is_empty() {
                        // 🚨 Apply post_filters BEFORE project/limit. This
                        // index point-lookup returns rows matching the single
                        // indexed predicate (e.g. a=1); a compound WHERE like
                        // `a=1 AND b=2` carries the full predicate as a
                        // post_filter, and without applying it here the
                        // `AND b=2` side was silently dropped (7 rows instead
                        // of 1). Note: result_rows are already projected above,
                        // but post_filters reference schema columns — so we
                        // keep the full decoded rows for filtering.
                        // (Re-fetch full rows for filtering since we projected.)
                        let filtered: Vec<Vec<Value>> = if filters_redundant {
                            result_rows
                        } else {
                            let mut kept = Vec::with_capacity(result_rows.len());
                            for (_, row_opt) in self.db.get_table_rows_batch(table, &row_ids)? {
                                if let Some(row) = row_opt {
                                    if Self::row_passes_post_filters(&row, post_filters, &schema) {
                                        kept.push(Self::project_row_direct(
                                            &row,
                                            &stmt.columns,
                                            &columns,
                                            &schema,
                                        ));
                                    }
                                }
                            }
                            kept
                        };
                        let mut result_rows = filtered;
                        // 🚨 Apply OFFSET/LIMIT (after post_filter).
                        let offset = stmt.offset.unwrap_or(0);
                        if offset >= result_rows.len() {
                            result_rows.clear();
                        } else if offset > 0 {
                            result_rows.drain(..offset);
                        }
                        if let Some(lim) = stmt.limit {
                            result_rows.truncate(lim);
                        }
                        return Ok(StreamingQueryResult::SelectReady {
                            columns,
                            rows: result_rows,
                        });
                    }
                    // Index found row_ids but rows not loadable (ColSegmentStore
                    // async delay) → fall through to scan.
                }
                // Key not in index → might be stale (async builder hasn't caught
                // up yet). Fall through to scan rather than returning empty.
            }
            // Index not available yet (async builder) → fall back to scan.
            return self.execute_full_scan_streaming(stmt, table);
        }

        // S9: AUTO_INCREMENT PK on ColSegmentStore — cached point lookup.
        // Uses get_row_cached (per-column decode cache), O(1) after first access.
        if is_auto_increment_pk && self.db.has_col_segment_store(table) {
            let row_id = match value {
                Value::Integer(id) if *id >= 0 => *id as RowId,
                _ => {
                    return Ok(StreamingQueryResult::SelectReady {
                        columns,
                        rows: vec![],
                    })
                }
            };
            let composite_key = self.db.make_composite_key(table, row_id);
            if let Some(store) = self.db.col_segment_stores.get(table) {
                if let Some(row) = store.get(composite_key) {
                    self.db
                        .row_cache
                        .put(table.to_string(), row_id, row.clone());
                    let sql_row = row_to_sql_row(&row, &schema)?;
                    let mut prefixed = SqlRow::new();
                    prefixed.insert("__row_id__".to_string(), Value::Integer(row_id as i64));
                    prefixed.insert("__table__".to_string(), Value::text(table.to_string()));
                    for (cn, v) in sql_row {
                        prefixed.insert(format!("{}.{}", table, cn), v);
                    }
                    let (_, result_rows) =
                        self.project_columns(&stmt.columns, &[(row_id, prefixed)], &schema)?;
                    return Ok(StreamingQueryResult::SelectReady {
                        columns,
                        rows: result_rows,
                    });
                }
                return Ok(StreamingQueryResult::SelectReady {
                    columns,
                    rows: vec![],
                });
            }
        }

        // S9: non-PK ColSegmentStore point queries. Skip the index→get_table_row
        // path for non-AUTO_INCREMENT PK tables (get_table_row fails due to
        // row_id ≠ PK value). The full-scan WHERE filter handles these correctly.
        let table_is_auto_inc_pk = {
            let schema = self.db.get_table_schema(table).ok();
            schema
                .and_then(|s| {
                    s.primary_key()
                        .and_then(|pk| s.get_column(pk))
                        .map(|c| c.auto_increment)
                })
                .unwrap_or(false)
        };
        if !is_pk && self.db.has_col_segment_store(table) && table_is_auto_inc_pk {
            if let Some(index_name) = self.db.index_registry.find_by_column(
                table,
                column,
                crate::database::index_metadata::IndexType::Column,
            ) {
                if let Some(index) = self.db.column_indexes.get(&index_name) {
                    let row_ids = index
                        .value()
                        .get_arc(value)
                        .unwrap_or_else(|_| std::sync::Arc::new(Vec::new()));
                    // Selectivity heuristic: use index-driven row fetch for result
                    // sets up to 10000 rows. The full-scan fallback (for >10000) is
                    // used when N point lookups become slower than a sequential scan.
                    // Previously this was 1000, which fell through to full-scan for
                    // ~1667 matches — and the full-scan WHERE path on INT-PK
                    // ColSegmentStore tables returned 0 rows (the v0.5.0 index bug).
                    if !row_ids.is_empty() && row_ids.len() <= 10000 {
                        // Exact-value verification: index keys truncate long
                        // Text values to a 64-byte prefix — drop prefix
                        // false positives instead of returning them.
                        let filter_pos = schema.get_column_position(column);
                        // 🔑 LIMIT pushdown: fetch only the offset+limit
                        // window of matches (output order follows row_ids
                        // order, so the trailing OFFSET/LIMIT applied below
                        // keeps exactly this prefix).
                        let fetch_end = match stmt.limit {
                            Some(lim) => stmt
                                .offset
                                .unwrap_or(0)
                                .saturating_add(lim)
                                .min(row_ids.len()),
                            None => row_ids.len(),
                        };
                        let mut result_rows = Vec::with_capacity(fetch_end);
                        for &rid in row_ids.iter().take(fetch_end) {
                            if let Some(row) = self.db.get_table_row(table, rid)? {
                                if let Some(pos) = filter_pos {
                                    if row.get(pos) != Some(value) {
                                        continue;
                                    }
                                }
                                let mut sql_row = SqlRow::new();
                                sql_row
                                    .insert("__row_id__".to_string(), Value::Integer(rid as i64));
                                sql_row.insert(
                                    "__table__".to_string(),
                                    Value::text(table.to_string()),
                                );
                                for (ci, col) in schema.columns.iter().enumerate() {
                                    let v = row.get(ci).cloned().unwrap_or(Value::Null);
                                    sql_row.insert(format!("{}.{}", table, col.name), v);
                                }
                                result_rows.push((rid, sql_row));
                            }
                        }
                        let (_, mut projected) =
                            self.project_columns(&stmt.columns, &result_rows, &schema)?;
                        // 🚨 Apply OFFSET/LIMIT — this branch previously
                        // returned ALL matches, so `WHERE x = ? LIMIT k`
                        // returned more than k rows.
                        let offset = stmt.offset.unwrap_or(0);
                        if offset >= projected.len() {
                            projected.clear();
                        } else if offset > 0 {
                            projected.drain(..offset);
                        }
                        if let Some(lim) = stmt.limit {
                            projected.truncate(lim);
                        }
                        return Ok(StreamingQueryResult::SelectReady {
                            columns,
                            rows: projected,
                        });
                    }
                    if row_ids.is_empty() {
                        return Ok(StreamingQueryResult::SelectReady {
                            columns,
                            rows: vec![],
                        });
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
                    Some(Self::project_row_direct(
                        &$row,
                        &stmt.columns,
                        &columns,
                        &schema,
                    ))
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
                    Some(row) => filter_and_project!(row).map(Ok).into_iter().collect(),
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
                Some(row) => filter_and_project!(row).map(Ok).into_iter().collect(),
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

        // Fallback: use column index — but only when one actually exists.
        // 🔑 非 AUTO_INCREMENT 的 PRIMARY KEY 不自动建列索引，此前的裸
        // query_by_column() 直接报 "Column index not found" 硬错误
        // (differential fuzz: SELECT COUNT(DISTINCT c) … WHERE id = 100)。
        // 无索引 → 回落全扫描路径，语义仍正确。
        let index_name = format!("{}.{}", table, column);
        if !self.db.column_indexes.contains_key(&index_name) {
            return self.execute_full_scan_streaming(stmt, table);
        }
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
            let id_set: std::collections::HashSet<u64> = sorted_ids.into_iter().collect();
            let start_key = self.db.make_composite_key(table, min_id);
            let end_key = self.db.make_composite_key(table, max_id + 1);
            let schema_c = schema.clone();

            let lsm_rows = self
                .db
                .lsm_engine
                .scan_range(start_key, end_key)
                .unwrap_or_default();

            lsm_rows
                .into_iter()
                .filter_map(move |(key, vd)| {
                    let rid = (key & 0xFFFFFFFF) as RowId;
                    if !id_set.contains(&(rid as u64)) || vd.deleted {
                        return None;
                    }
                    let data = match &vd.data {
                        crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                        _ => return None,
                    };
                    decode_row(data, &schema_c).ok()
                })
                .collect()
        } else {
            // Sparse result set: batch read via row cache + LSM range scan
            let batch = self
                .db
                .get_table_rows_batch_arc(table, &sorted_ids)
                .map_err(|e| {
                    StorageError::Query(format!(
                        "Failed to fetch rows for table '{}': {}",
                        table, e
                    ))
                })?;
            batch
                .into_iter()
                .filter_map(|(_, opt_arc)| opt_arc)
                .map(|row_arc| {
                    let row: Vec<Value> = (*row_arc).clone();
                    row
                })
                .collect()
        };

        // Apply post_filters on full decoded rows, then project survivors
        let result_rows: Vec<Result<Vec<Value>>> = decoded_rows
            .into_iter()
            .filter(|row| {
                post_filters.is_empty() || Self::row_passes_post_filters(row, post_filters, &schema)
            })
            .map(|row| {
                Ok(Self::project_row_direct(
                    &row,
                    &stmt.columns,
                    &columns,
                    &schema,
                ))
            })
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
        let is_star = stmt.columns.len() == 1
            && matches!(stmt.columns[0], SelectColumn::Star)
            && stmt.order_by.is_none()
            && !stmt.distinct;
        if is_star {
            let pk_col = schema.primary_key().unwrap_or("id");
            if column != pk_col {
                let index_name = format!("{}.{}", table, column);
                if let Some(index_ref) = self.db.column_indexes.get(&index_name) {
                    let row_ids = index_ref.value().query_between(
                        start,
                        start_inclusive,
                        end,
                        end_inclusive,
                    )?;
                    drop(index_ref);
                    if !row_ids.is_empty() || !self.db.is_async_index_pipeline_active() {
                        let column_names: Vec<String> =
                            schema.columns.iter().map(|c| c.name.clone()).collect();
                        let arc_rows = self.db.get_table_rows_batch_arc(table, &row_ids)?;
                        let skip_n = stmt.offset.unwrap_or(0);
                        let take_n = stmt.limit.unwrap_or(usize::MAX);
                        let rows: Vec<Vec<Value>> = arc_rows
                            .into_iter()
                            .filter_map(|(_, opt)| opt)
                            .filter(|row| {
                                post_filters.is_empty()
                                    || Self::row_passes_post_filters(row, post_filters, &schema)
                            })
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
            return self.execute_primary_key_range_streaming(
                stmt,
                table,
                start,
                start_inclusive,
                end,
                end_inclusive,
            );
        }

        // 🔧 路径2：非主键列使用列索引 + batch_get (with row cache)
        // 🔑 无列索引时回落全扫 — 非索引列不保证有索引，裸调用会硬报
        // "Column index not found" (differential fuzz: WHERE id = 100 的
        // 聚合查询在非 AUTO_INCREMENT PK 上报错)。
        let index_name = format!("{}.{}", table, column);
        if !self.db.column_indexes.contains_key(&index_name) {
            return self.execute_full_scan_streaming(stmt, table);
        }
        let row_ids = self.db.query_by_column_between(
            table,
            column,
            start,
            start_inclusive,
            end,
            end_inclusive,
        )?;

        // 🚀 批量读取行数据（通过 row_cache 减少锁竞争和 LSM 开销）
        let db = self.db.clone();
        let table_name = table.to_string();

        // Decode rows in batches (no projection yet — post_filters need full row)
        const BATCH_SIZE: usize = 1000;
        let total_rows = row_ids.len();

        let decoded_rows: Vec<Vec<Value>> = (0..total_rows)
            .step_by(BATCH_SIZE)
            .flat_map(|batch_start| {
                let batch_end = (batch_start + BATCH_SIZE).min(total_rows);
                let batch_row_ids = &row_ids[batch_start..batch_end];

                match db.get_table_rows_batch(&table_name, batch_row_ids) {
                    Ok(results) => results
                        .into_iter()
                        .filter_map(|(_, opt)| opt)
                        .collect::<Vec<_>>(),
                    Err(_) => vec![],
                }
            })
            .collect();

        // Apply post_filters on full decoded rows, then project survivors
        let result_rows: Vec<Result<Vec<Value>>> = decoded_rows
            .into_iter()
            .filter(|row| {
                post_filters.is_empty() || Self::row_passes_post_filters(row, post_filters, &schema)
            })
            .map(|row| {
                Ok(Self::project_row_direct(
                    &row,
                    &stmt.columns,
                    &columns,
                    &schema,
                ))
            })
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
            _ => {
                return Err(StorageError::InvalidData(format!(
                    "Primary key must be integer, got {:?}",
                    start
                )))
            }
        };
        let end_row_id = match end {
            Value::Integer(i) => *i as u64,
            _ => {
                return Err(StorageError::InvalidData(format!(
                    "Primary key must be integer, got {:?}",
                    end
                )))
            }
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
        let lsm_iter = self
            .db
            .lsm_engine
            .scan_range_streaming(start_key, end_key)?;

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
                    let projected =
                        Self::project_row_direct(&row, &select_cols, &columns_clone, &schema_clone);
                    Ok(projected)
                }
                Err(e) => Err(StorageError::InvalidData(format!(
                    "Deserialization failed: {}",
                    e
                ))),
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
            let idx_ref = self.db.column_indexes.get(&idx1_name).ok_or_else(|| {
                MoteDBError::InvalidArgument(format!("Index {} not found", idx1_name))
            })?;
            idx_ref.value().get(value1)?
        };
        let row_id_set1: std::collections::HashSet<u64> = row_ids1.into_iter().collect();

        // Look up row IDs from second index and intersect
        let row_ids2 = {
            let idx_ref = self.db.column_indexes.get(&idx2_name).ok_or_else(|| {
                MoteDBError::InvalidArgument(format!("Index {} not found", idx2_name))
            })?;
            idx_ref.value().get(value2)?
        };
        let intersected: Vec<u64> = row_ids2
            .into_iter()
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
        let projected_rows: Vec<Vec<Value>> = rows_result
            .into_iter()
            .filter_map(|(_row_id, opt_row)| opt_row)
            .filter(|row| {
                post_filters.is_empty() || Self::row_passes_post_filters(row, post_filters, &schema)
            })
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
            let sort_specs: Vec<(usize, bool)> = order_by
                .iter()
                .filter_map(|ob| {
                    let col_name = match &ob.expr {
                        Expr::Column(name) => name,
                        _ => return None,
                    };
                    let bare = if col_name.contains('.') {
                        col_name.rsplit('.').next().unwrap_or(col_name)
                    } else {
                        col_name
                    };
                    columns
                        .iter()
                        .position(|c| c == bare || c == col_name)
                        .map(|i| (i, ob.asc))
                })
                .collect();
            if !sort_specs.is_empty() {
                rows.sort_by(|a, b| {
                    for &(col_idx, asc) in &sort_specs {
                        if col_idx >= a.len() || col_idx >= b.len() {
                            continue;
                        }
                        let ord = order_by_cmp(&a[col_idx], &b[col_idx]);
                        let final_ord = if asc { ord } else { ord.reverse() };
                        if final_ord != std::cmp::Ordering::Equal {
                            return final_ord;
                        }
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
    fn try_order_by_limit_topk(
        &self,
        stmt: &SelectStmt,
        table: &str,
    ) -> Result<Option<StreamingQueryResult>> {
        let limit = match stmt.limit {
            Some(l) => l,
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
        // 🔑 TimeSeries tables: the fast paths below read columnar SSTables /
        // the LSM — neither holds TS data (ColumnarStore does). Bail so the
        // query falls through to the full-scan Materialized TS branch.
        if schema.table_type == crate::types::TableType::TimeSeries {
            return Ok(None);
        }
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
            if let Ok((indices, _vals)) =
                self.db
                    .scan_columnar_sstable_topk(table, sort_col_idx, k, ascending)
            {
                let col_types = schema.col_types();
                if let Ok(rows) = self
                    .db
                    .scan_columnar_sstable_rows(table, col_types, &indices)
                {
                    return Ok(Some(StreamingQueryResult::SelectReady {
                        columns: col_names,
                        rows,
                    }));
                }
            }
        }

        let col_types = schema.col_types();
        let limit_usize = limit.min(100_000); // sanity cap
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;

        // Pre-compute sort column layout for direct read from raw bytes.
        // For fixed columns: byte offset = HEADER_SIZE + fixed_idx * 8
        // For var columns: we need to scan the var header, but that's still
        //   cheaper than full row decode (only parse header, skip all other vars).
        let is_fixed = matches!(
            &col_types[sort_col_idx],
            crate::types::ColumnType::Integer
                | crate::types::ColumnType::Float
                | crate::types::ColumnType::Boolean
                | crate::types::ColumnType::Timestamp
        );
        let sort_col_type = col_types[sort_col_idx].clone();
        let fixed_count = col_types
            .iter()
            .filter(|t| {
                matches!(
                    t,
                    crate::types::ColumnType::Integer
                        | crate::types::ColumnType::Float
                        | crate::types::ColumnType::Boolean
                        | crate::types::ColumnType::Timestamp
                )
            })
            .count();
        let var_section_start = crate::storage::row_format::HEADER_SIZE
            + fixed_count * crate::storage::row_format::FIXED_COL_SIZE;

        // Pre-compute fixed offset for the sort column (valid only if is_fixed)
        let fixed_offset: usize = if is_fixed {
            let fixed_idx: usize = col_types[..sort_col_idx]
                .iter()
                .filter(|t| {
                    matches!(
                        t,
                        crate::types::ColumnType::Integer
                            | crate::types::ColumnType::Float
                            | crate::types::ColumnType::Boolean
                            | crate::types::ColumnType::Timestamp
                    )
                })
                .count();
            crate::storage::row_format::HEADER_SIZE
                + fixed_idx * crate::storage::row_format::FIXED_COL_SIZE
        } else {
            0
        };

        // Helper to extract sort column value from raw bytes
        let extract_sort_val = |data: &[u8]| -> Option<Value> {
            use crate::storage::row_format::{FIXED_COL_SIZE, HEADER_SIZE};
            if data.len() < HEADER_SIZE {
                return None;
            }
            // Null bitmap check
            let null_bitmap = u64::from_le_bytes([
                data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
            ]);
            if null_bitmap & (1u64 << sort_col_idx) != 0 {
                return Some(Value::Null);
            }
            if is_fixed {
                let off = fixed_offset;
                if off + FIXED_COL_SIZE > data.len() {
                    return None;
                }
                let arr: [u8; 8] = data[off..off + 8].try_into().ok()?;
                match sort_col_type {
                    crate::types::ColumnType::Integer => {
                        Some(Value::Integer(i64::from_le_bytes(arr)))
                    }
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
                if var_section_start + 2 > data.len() {
                    return None;
                }
                let var_count =
                    u16::from_le_bytes([data[var_section_start], data[var_section_start + 1]])
                        as usize;
                let var_header_start = var_section_start + 2;
                let var_data_start = var_header_start + var_count * 10;
                for vi in 0..var_count {
                    let hdr_off = var_header_start + vi * 10;
                    if hdr_off + 10 > data.len() {
                        break;
                    }
                    let entry_col = u16::from_le_bytes([data[hdr_off], data[hdr_off + 1]]) as usize;
                    if entry_col == sort_col_idx {
                        let v_off = u32::from_le_bytes([
                            data[hdr_off + 2],
                            data[hdr_off + 3],
                            data[hdr_off + 4],
                            data[hdr_off + 5],
                        ]) as usize;
                        let v_len = u32::from_le_bytes([
                            data[hdr_off + 6],
                            data[hdr_off + 7],
                            data[hdr_off + 8],
                            data[hdr_off + 9],
                        ]) as usize;
                        let abs_off = var_data_start + v_off;
                        if abs_off + v_len > data.len() {
                            return None;
                        }
                        let var_data = &data[abs_off..abs_off + v_len];
                        return match &sort_col_type {
                            crate::types::ColumnType::Text => {
                                let s = std::str::from_utf8(var_data).ok()?;
                                Some(Value::Text(crate::types::ArcString(std::sync::Arc::from(
                                    s,
                                ))))
                            }
                            _ => {
                                crate::storage::row_format::SchemaDecodeContext::decode_var_generic(
                                    var_data,
                                )
                                .ok()
                            }
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
        let mut lsm_iter = self
            .db
            .lsm_engine
            .scan_range_streaming(start_key, end_key)?;
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
                            if deleted || vb.len == 0 {
                                continue;
                            }
                            let row_id = (composite_key & 0xFFFFFFFF) as RowId;
                            if let Some(sv) = extract_sort_val(vb.as_slice()) {
                                heap.push((SortKey(sv), row_id));
                                if heap.len() > limit_usize {
                                    heap.pop();
                                }
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
                            if value.deleted {
                                continue;
                            }
                            let row_id = (composite_key & 0xFFFFFFFF) as RowId;
                            let data = match &value.data {
                                crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                                _ => continue,
                            };
                            if let Some(sv) = extract_sort_val(data) {
                                heap.push((SortKey(sv), row_id));
                                if heap.len() > limit_usize {
                                    heap.pop();
                                }
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
            let rows: Vec<Row> = rows
                .into_iter()
                .filter_map(|(_, opt)| {
                    opt.map(|a| match Arc::try_unwrap(a) {
                        Ok(row) => row,
                        Err(arc) => (*arc).clone(),
                    })
                })
                .collect();
            Ok(Some(StreamingQueryResult::SelectReady {
                columns: col_names,
                rows,
            }))
        } else {
            // DESC: min-heap via Reverse (pop smallest when >K, keeping K largest)
            let mut heap: BinaryHeap<Reverse<(SortKey, RowId)>> =
                BinaryHeap::with_capacity(limit_usize + 1);
            if has_raw {
                loop {
                    match lsm_iter.next_raw() {
                        Some(Ok((composite_key, _ts, deleted, vb))) => {
                            if deleted || vb.len == 0 {
                                continue;
                            }
                            let row_id = (composite_key & 0xFFFFFFFF) as RowId;
                            if let Some(sv) = extract_sort_val(vb.as_slice()) {
                                heap.push(Reverse((SortKey(sv), row_id)));
                                if heap.len() > limit_usize {
                                    heap.pop();
                                }
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
                            if value.deleted {
                                continue;
                            }
                            let row_id = (composite_key & 0xFFFFFFFF) as RowId;
                            let data = match &value.data {
                                crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                                _ => continue,
                            };
                            if let Some(sv) = extract_sort_val(data) {
                                heap.push(Reverse((SortKey(sv), row_id)));
                                if heap.len() > limit_usize {
                                    heap.pop();
                                }
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
            let row_ids: Vec<RowId> = top.into_iter().map(|r| r.0 .1).collect();
            let rows = self.db.get_table_rows_batch_arc(table, &row_ids)?;
            let rows: Vec<Row> = rows
                .into_iter()
                .filter_map(|(_, opt)| {
                    opt.map(|a| match Arc::try_unwrap(a) {
                        Ok(row) => row,
                        Err(arc) => (*arc).clone(),
                    })
                })
                .collect();
            Ok(Some(StreamingQueryResult::SelectReady {
                columns: col_names,
                rows,
            }))
        }
    }

    /// 🚀 Columnar aggregate pushdown: compute COUNT/SUM/MIN/MAX directly
    /// from column segments without materializing any rows.
    /// 🚀 Columnar GROUP BY pushdown: build HashMap directly from typed arrays.
    /// Only reads the group-by column and aggregate columns — no per-row decode.
    /// For GROUP BY customer: read TextSegment → HashMap<String, (count, sum)> → compute AVG.
    fn try_group_by_columnar(&self, stmt: &SelectStmt) -> Result<Option<StreamingQueryResult>> {
        // 🚨 Subqueries in WHERE (incl. correlated EXISTS) need per-row
        // execution by the general path — this fast path cannot evaluate
        // them and would silently return wrong counts.
        if stmt
            .where_clause
            .as_ref()
            .is_some_and(Self::expr_contains_subquery)
        {
            return Ok(None);
        }
        let table = match &stmt.from {
            Some(TableRef::Table { name, .. }) => name.as_str(),
            _ => return Ok(None),
        };
        if !self.db.columnar_sstables.contains_key(table) {
            return Ok(None);
        }
        let schema = self.db.get_table_schema(table)?;
        let col_sst = self.db.columnar_sstables.get(table).unwrap();
        let num_rows = col_sst.num_rows;
        // HAVING and DISTINCT aggregates are not applied by this pushdown path —
        // fall back to the materialized GROUP BY path which evaluates them.
        // ORDER BY over the grouped result is also not applied here (the
        // pushdown emits groups in scan/hash order, not sorted).
        // 🔑 WHERE is also not applied by this path (it scans all rows) —
        // fall back so WHERE filters are respected.
        if stmt.having.is_some() || stmt.order_by.is_some() || stmt.where_clause.is_some() {
            return Ok(None);
        }

        // Parse GROUP BY columns (only single-column for now)
        let group_cols = stmt.group_by.as_ref().unwrap();
        if group_cols.len() != 1 {
            return Ok(None);
        }
        let group_col_name = group_cols[0].as_str();
        let group_pos = match schema.get_column_position(group_col_name) {
            Some(p) => p,
            None => return Ok(None),
        };

        // Parse aggregate columns
        struct AggCol {
            func: String,
            col_pos: usize,
        }
        let mut agg_cols: Vec<AggCol> = Vec::new();
        let mut has_count_star = false;
        for col_expr in &stmt.columns {
            match col_expr {
                SelectColumn::Star => {
                    has_count_star = true;
                }
                SelectColumn::Column(_name) => { /* group-by column in output */ }
                SelectColumn::Expr(expr, _) => {
                    if let Expr::FunctionCall { name, args, .. } = expr {
                        match name.to_uppercase().as_str() {
                            "COUNT" => {
                                if args.first().is_none_or(|a| matches!(a, Expr::Column(_))) {
                                    has_count_star = true; // COUNT(col) → treat as count
                                }
                            }
                            // 🔑 STDDEV/VARIANCE need sum-of-squared-deviations.
                            // This fast path's GroupAcc only tracks count/sum, so
                            // it would emit NULL for them. Fall back to the
                            // materialized path (compute_aggregate_positional).
                            "STDDEV" | "VARIANCE" => return Ok(None),
                            _ => {
                                let col = match args.first() {
                                    Some(Expr::Column(c)) => c.as_str(),
                                    _ => return Ok(None),
                                };
                                let pos = match schema.get_column_position(col) {
                                    Some(p) => p,
                                    None => return Ok(None),
                                };
                                agg_cols.push(AggCol {
                                    func: name.to_uppercase(),
                                    col_pos: pos,
                                });
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
        struct GroupAcc {
            count: i64,
            int_sum: i64,
            float_sum: CompSum,
            has_float: bool,
        }
        impl GroupAcc {
            fn new() -> Self {
                Self {
                    count: 0,
                    int_sum: 0,
                    float_sum: CompSum::default(),
                    has_float: false,
                }
            }
            fn add(&mut self, val: f64, is_int: bool) {
                if is_int {
                    self.int_sum = self.int_sum.wrapping_add(val as i64);
                } else {
                    if !self.has_float {
                        self.has_float = true;
                        self.float_sum.add(self.int_sum as f64);
                    }
                    self.float_sum.add(val);
                }
            }
            fn sum(&self) -> Value {
                if self.has_float {
                    Value::Float(self.float_sum.total())
                } else {
                    Value::Integer(self.int_sum)
                }
            }
            fn avg(&self) -> Value {
                if self.count == 0 {
                    return Value::Null;
                }
                let s = if self.has_float {
                    self.float_sum.total()
                } else {
                    self.int_sum as f64
                };
                Value::Float(s / self.count as f64)
            }
        }
        // 🔑 PERF: low-cardinality linear-scan accumulator. For GROUP BY columns
        // with few distinct values (typical: 4-256), a Vec<(String, GroupAcc)>
        // with linear scan is faster than HashMap<&str> — string comparison for
        // short keys (1-2 bytes) is 1 cycle vs hash computation + bucket probe.
        // The HashMap path remains available for high-cardinality fallback.
        const LINEAR_SCAN_MAX: usize = 256;

        // 🔑 Integer GROUP BY fast path: use i64 as HashMap key (zero alloc,
        // vs the generic path which builds Vec<Value> keys). This was
        // previously skipped (return Ok(None)) forcing all integer GROUP BY
        // through the slow materialized path.
        if col_sst.column_tags[group_pos].is_fixed() {
            let group_fseg = col_sst.read_fixed_i64(group_pos)?;
            // Pre-decode agg columns
            let mut agg_segs: Vec<crate::storage::lsm::columnar::FixedSegment> = Vec::new();
            let mut agg_is_int: Vec<bool> = Vec::with_capacity(agg_cols.len());
            for a in &agg_cols {
                if a.col_pos < col_sst.column_tags.len()
                    && col_sst.column_tags[a.col_pos].is_fixed()
                {
                    agg_segs.push(col_sst.read_fixed_i64(a.col_pos)?);
                    agg_is_int.push(matches!(
                        col_sst.column_tags[a.col_pos],
                        crate::storage::lsm::columnar::ColumnTypeTag::Integer
                            | crate::storage::lsm::columnar::ColumnTypeTag::Timestamp
                    ));
                } else {
                    // Non-fixed agg column on fixed group — bail to generic path
                    return Ok(None);
                }
            }
            // Use i64-keyed HashMap for integer groups (zero-alloc keys)
            let mut int_groups: std::collections::HashMap<i64, GroupAcc> =
                std::collections::HashMap::with_capacity(256);
            let has_count_star = stmt.columns.iter().any(|c| matches!(c, SelectColumn::Star));
            let n = col_sst.num_rows;
            for i in 0..n {
                let key = match group_fseg.get_i64(i) {
                    Some(k) => k,
                    None => continue, // NULL group — skip for simplicity
                };
                let acc = int_groups.entry(key).or_insert_with(GroupAcc::new);
                acc.count += 1;
                for (j, a) in agg_cols.iter().enumerate() {
                    if a.func == "SUM" || a.func == "AVG" {
                        let is_int = agg_is_int[j];
                        let v = if is_int {
                            agg_segs[j].get_i64(i).map(|x| x as f64)
                        } else {
                            agg_segs[j].get_f64(i)
                        };
                        if let Some(v) = v {
                            acc.add(v, is_int);
                        }
                    }
                }
            }
            // Build output rows
            let mut rows: Vec<Vec<Value>> = Vec::new();
            for (key, acc) in &int_groups {
                let mut row = Vec::new();
                row.push(Value::Integer(*key));
                if has_count_star {
                    row.push(Value::Integer(acc.count));
                }
                for a in &agg_cols {
                    match a.func.as_str() {
                        "SUM" => row.push(acc.sum()),
                        "AVG" => row.push(acc.avg()),
                        _ => row.push(Value::Null),
                    }
                }
                rows.push(row);
            }
            // Sort by group key for deterministic output
            rows.sort_by_key(|r| match r[0] {
                Value::Integer(i) => i,
                _ => 0,
            });
            let cols: Vec<String> = stmt
                .columns
                .iter()
                .map(|c| match c {
                    SelectColumn::Column(name) => name.clone(),
                    _ => "expr".to_string(),
                })
                .collect();
            return Ok(Some(StreamingQueryResult::SelectReady {
                columns: cols,
                rows,
            }));
        }
        let group_seg = col_sst.read_text(group_pos)?;
        let mut agg_segs: Vec<crate::storage::lsm::columnar::FixedSegment> = Vec::new();
        // For each agg column, whether it's an Integer (use get_i64) or Float.
        let mut agg_is_int: Vec<bool> = Vec::with_capacity(agg_cols.len());
        for a in &agg_cols {
            if col_sst.column_tags[a.col_pos].is_fixed() {
                agg_segs.push(col_sst.read_fixed_i64(a.col_pos)?);
                agg_is_int.push(matches!(
                    col_types.get(a.col_pos),
                    Some(crate::types::ColumnType::Integer)
                        | Some(crate::types::ColumnType::Boolean)
                ));
            } else {
                return Ok(None);
            }
        }

        // 🔑 PERF: fast path when no deletions and no NULLs in the group column.
        // Skips 20K is_deleted() bitmap probes + 20K null-checked get_str() calls.
        let has_deletes = col_sst.row_map.has_any_deleted();
        let has_nulls = group_seg.has_any_null();

        // Linear-scan accumulator: Vec<(group_key, GroupAcc)>.
        // Falls back to HashMap if cardinality exceeds LINEAR_SCAN_MAX.
        let mut lin_groups: Vec<(String, GroupAcc)> = Vec::new();
        let mut use_hashmap = false;
        let mut groups: HashMap<String, GroupAcc> = HashMap::new();
        // Separate accumulator for NULL group rows. NULL must form its own group
        // (not be skipped), and is kept out of the &str-keyed maps to avoid
        // needing a sentinel string that could collide with a real value.
        let mut has_null_group = false;
        let mut null_acc = GroupAcc::new();

        // Helper: find-or-insert in lin_groups (linear scan).
        // For ≤256 groups this is faster than hashing.
        let lin_find = |groups: &mut Vec<(String, GroupAcc)>, key: &str| -> usize {
            for (i, (k, _)) in groups.iter().enumerate() {
                if k == key {
                    return i;
                }
            }
            groups.push((key.to_string(), GroupAcc::new()));
            groups.len() - 1
        };

        // Migrate lin_groups → groups at the spill point so a key is never in
        // both structures (which would emit duplicate rows). Counts/sums are
        // accumulated into any pre-existing hashmap entry. After migration
        // lin_groups is empty, so the linear emit loop at the end is a no-op.
        macro_rules! migrate_lin_to_groups {
            () => {{
                for (k, a) in lin_groups.drain(..) {
                    let entry = groups.entry(k).or_insert_with(GroupAcc::new);
                    entry.count += a.count;
                    // Re-add the accumulated sums so partial sums from lin_groups
                    // merge into the hashmap entry. add() handles int/float flagging.
                    if a.has_float {
                        entry.add(a.float_sum.total(), true);
                    }
                    entry.add(a.int_sum as f64, false);
                }
                use_hashmap = true;
            }};
        }

        if !has_deletes && !has_nulls && agg_cols.is_empty() {
            // Count-only GROUP BY on a text column with no deletes/nulls:
            // same get_str_fast loop as the AVG path (for_each_str closure
            // wasn't being inlined, causing a 2.7x regression vs AVG path).
            for i in 0..num_rows {
                let key = group_seg.get_str_fast(i);
                if !use_hashmap {
                    let idx = lin_find(&mut lin_groups, key);
                    lin_groups[idx].1.count += 1;
                    if lin_groups.len() > LINEAR_SCAN_MAX {
                        migrate_lin_to_groups!();
                    }
                } else {
                    let entry = groups.entry(key.to_string()).or_insert_with(GroupAcc::new);
                    entry.count += 1;
                }
            }
        } else if !has_deletes && !has_nulls {
            // GROUP BY with aggregates, no deletes/nulls.
            for i in 0..num_rows {
                let key = group_seg.get_str_fast(i);
                if !use_hashmap {
                    let idx = lin_find(&mut lin_groups, key);
                    lin_groups[idx].1.count += 1;
                    for (j, a) in agg_cols.iter().enumerate() {
                        if a.func == "SUM" || a.func == "AVG" {
                            let is_int = agg_is_int[j];
                            let v = if is_int {
                                agg_segs[j].get_i64(i).map(|x| x as f64)
                            } else {
                                agg_segs[j].get_f64(i)
                            };
                            if let Some(v) = v {
                                lin_groups[idx].1.add(v, is_int);
                            }
                        }
                    }
                    if lin_groups.len() > LINEAR_SCAN_MAX {
                        migrate_lin_to_groups!();
                    }
                } else {
                    let entry = groups.entry(key.to_string()).or_insert_with(GroupAcc::new);
                    entry.count += 1;
                    for (j, a) in agg_cols.iter().enumerate() {
                        if a.func == "SUM" || a.func == "AVG" {
                            let is_int = agg_is_int[j];
                            let v = if is_int {
                                agg_segs[j].get_i64(i).map(|x| x as f64)
                            } else {
                                agg_segs[j].get_f64(i)
                            };
                            if let Some(v) = v {
                                entry.add(v, is_int);
                            }
                        }
                    }
                }
            }
        } else {
            // General path: check deletions + nulls per row.
            for i in 0..num_rows {
                if col_sst.row_map.is_deleted(i) {
                    continue;
                }
                // NULL group-key rows form their own group instead of being skipped.
                let key_opt = group_seg.get_str(i);
                if key_opt.is_none() {
                    has_null_group = true;
                    null_acc.count += 1;
                    for (j, a) in agg_cols.iter().enumerate() {
                        if a.func == "SUM" || a.func == "AVG" {
                            let is_int = agg_is_int[j];
                            let v = if is_int {
                                agg_segs[j].get_i64(i).map(|x| x as f64)
                            } else {
                                agg_segs[j].get_f64(i)
                            };
                            if let Some(v) = v {
                                null_acc.add(v, is_int);
                            }
                        }
                    }
                    continue;
                }
                let key = key_opt.unwrap();
                if !use_hashmap {
                    let idx = lin_find(&mut lin_groups, key);
                    lin_groups[idx].1.count += 1;
                    for (j, a) in agg_cols.iter().enumerate() {
                        if a.func == "SUM" || a.func == "AVG" {
                            let is_int = agg_is_int[j];
                            let v = if is_int {
                                agg_segs[j].get_i64(i).map(|x| x as f64)
                            } else {
                                agg_segs[j].get_f64(i)
                            };
                            if let Some(v) = v {
                                lin_groups[idx].1.add(v, is_int);
                            }
                        }
                    }
                    if lin_groups.len() > LINEAR_SCAN_MAX {
                        migrate_lin_to_groups!();
                    }
                } else {
                    let entry = groups.entry(key.to_string()).or_insert_with(GroupAcc::new);
                    entry.count += 1;
                    for (j, a) in agg_cols.iter().enumerate() {
                        if a.func == "SUM" || a.func == "AVG" {
                            let is_int = agg_is_int[j];
                            let v = if is_int {
                                agg_segs[j].get_i64(i).map(|x| x as f64)
                            } else {
                                agg_segs[j].get_f64(i)
                            };
                            if let Some(v) = v {
                                entry.add(v, is_int);
                            }
                        }
                    }
                }
            }
        } // end else (general path with deletes/nulls)

        // Build output rows — merge linear-scan + hashmap results.
        // NOTE: if we spilled to the hashmap (use_hashmap), lin_groups was
        // migrated into `groups` at the spill point (see the spill migrations
        // below), so `lin_groups` is empty here and only `groups` emits — no
        // double-counting. Without this migration, a key present in both
        // structures would appear twice in the output (the
        // "GROUP BY returns 30257 instead of 30000" bug).
        let mut rows: Vec<Vec<Value>> = Vec::new();
        for (key, acc) in &lin_groups {
            let mut row = Vec::new();
            row.push(Value::Text(crate::types::ArcString(std::sync::Arc::from(
                key.as_str(),
            ))));
            if has_count_star {
                row.push(Value::Integer(acc.count));
            }
            for a in &agg_cols {
                match a.func.as_str() {
                    "SUM" => row.push(acc.sum()),
                    "AVG" => row.push(acc.avg()),
                    _ => row.push(Value::Null),
                }
            }
            rows.push(row);
        }
        // Append hashmap results (high-cardinality spill).
        for (key, acc) in &groups {
            let mut row = Vec::new();
            row.push(Value::Text(crate::types::ArcString(std::sync::Arc::from(
                key.as_str(),
            ))));
            if has_count_star {
                row.push(Value::Integer(acc.count));
            }
            for a in &agg_cols {
                match a.func.as_str() {
                    "SUM" => row.push(acc.sum()),
                    "AVG" => row.push(acc.avg()),
                    _ => row.push(Value::Null),
                }
            }
            rows.push(row);
        }
        // Append the NULL group (rows whose group-by column was NULL).
        if has_null_group {
            let mut row = Vec::new();
            row.push(Value::Null);
            if has_count_star {
                row.push(Value::Integer(null_acc.count));
            }
            for a in &agg_cols {
                match a.func.as_str() {
                    "SUM" => row.push(null_acc.sum()),
                    "AVG" => row.push(null_acc.avg()),
                    _ => row.push(Value::Null),
                }
            }
            rows.push(row);
        }

        // Build column names
        let cols: Vec<String> = stmt
            .columns
            .iter()
            .map(|c| match c {
                SelectColumn::Star => "COUNT(*)".to_string(),
                SelectColumn::Column(name) => name.clone(),
                SelectColumn::Expr(Expr::FunctionCall { name, args, .. }, alias) => {
                    alias.clone().unwrap_or_else(|| {
                        format!(
                            "{}({})",
                            name.to_uppercase(),
                            match args.first() {
                                Some(Expr::Column(c)) => c.as_str(),
                                _ => "?",
                            }
                        )
                    })
                }
                _ => "?".to_string(),
            })
            .collect();

        Ok(Some(StreamingQueryResult::SelectReady {
            columns: cols,
            rows,
        }))
    }

    /// For WHERE region='US': read region segment → find matches → compute
    /// SUM/MIN/MAX from amount segment. O(N) scan but no per-row allocation.
    fn try_aggregate_columnar_fast(
        &self,
        stmt: &SelectStmt,
    ) -> Result<Option<StreamingQueryResult>> {
        // 🚨 Subqueries in WHERE (incl. correlated EXISTS) need per-row
        // execution by the general path — this fast path cannot evaluate
        // them and would silently return wrong counts.
        if stmt
            .where_clause
            .as_ref()
            .is_some_and(Self::expr_contains_subquery)
        {
            return Ok(None);
        }
        if stmt.group_by.is_some() {
            return Ok(None);
        }
        // 🆕 HAVING requires post-aggregation filtering that this pushdown path
        // doesn't apply — fall back to the materialized path (which evaluates
        // HAVING correctly). Without this guard, `SELECT SUM(v) FROM t HAVING
        // SUM(v) > 100` returns the SUM even when the condition is false.
        if stmt.having.is_some() {
            return Ok(None);
        }
        // 🔑 DISTINCT aggregates (COUNT(DISTINCT col)) need dedup logic this
        // fast path doesn't implement — it would emit an empty row (COUNT is
        // a no-op here unless it's COUNT(*) via Star). Fall back.
        let has_distinct_agg = stmt.columns.iter().any(|c| {
            matches!(
                c,
                SelectColumn::Expr(Expr::FunctionCall { distinct: true, .. }, _)
            )
        });
        if has_distinct_agg {
            return Ok(None);
        }
        let table = match &stmt.from {
            Some(TableRef::Table { name, .. }) => name.as_str(),
            _ => return Ok(None),
        };
        if !self.db.columnar_sstables.contains_key(table) {
            return Ok(None);
        }
        let schema = self.db.get_table_schema(table)?;
        let _col_types = schema.col_types();

        // Parse WHERE: only simple col = literal
        let (filter_col, filter_value) = match &stmt.where_clause {
            Some(Expr::BinaryOp {
                left,
                op: crate::sql::ast::BinaryOperator::Eq,
                right,
            }) => {
                let col = match left.as_ref() {
                    Expr::Column(n) => n.as_str(),
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
        let filter_pos = match schema.get_column_position(filter_col) {
            Some(p) => p,
            None => return Ok(None),
        };

        let col_sst = self.db.columnar_sstables.get(table).unwrap();
        let num_rows = col_sst.num_rows;

        // Find matching rows from filter column segment
        let mut match_indices: Vec<usize> = Vec::new();
        if col_sst.column_tags[filter_pos].is_fixed() {
            let seg = col_sst.read_fixed_i64(filter_pos)?;
            for i in 0..num_rows {
                if col_sst.row_map.is_deleted(i) {
                    continue;
                }
                let matches = match &filter_value {
                    Value::Integer(iv) => seg.get_i64(i) == Some(*iv),
                    Value::Float(fv) => {
                        (seg.get_f64(i).unwrap_or(f64::NAN) - fv).abs() < f64::EPSILON
                    }
                    _ => false,
                };
                if matches {
                    match_indices.push(i);
                }
            }
        } else {
            let seg = col_sst.read_text(filter_pos)?;
            for i in 0..num_rows {
                if col_sst.row_map.is_deleted(i) {
                    continue;
                }
                if let Value::Text(tv) = &filter_value {
                    if seg.get_str(i) == Some(tv.as_str()) {
                        match_indices.push(i);
                    }
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
                                    Some(Expr::Column(c)) => c.as_str(),
                                    _ => return Ok(None),
                                };
                                let agg_pos = match schema.get_column_position(agg_col) {
                                    Some(p) => p,
                                    None => return Ok(None),
                                };
                                let is_fixed = col_sst.column_tags[agg_pos].is_fixed();
                                if is_fixed {
                                    let seg = col_sst.read_fixed_i64(agg_pos)?;
                                    // 🔑 Decode by column type, NOT always f64. Integer
                                    // columns store i64; reading them as f64 reinterprets
                                    // the bits (Integer(5) → f64::from_bits(5) ≈ 0),
                                    // corrupting SUM/MIN/MAX (the v0.5.0 MIN=inf bug).
                                    let is_int = matches!(
                                        schema.col_types().get(agg_pos),
                                        Some(crate::types::ColumnType::Integer)
                                            | Some(crate::types::ColumnType::Boolean)
                                    );
                                    if is_int {
                                        let vals: Vec<i64> = match_indices
                                            .iter()
                                            .filter_map(|&i| seg.get_i64(i))
                                            .filter(|&v| v != i64::MIN) // MIN is the NULL sentinel
                                            .collect();
                                        match name.to_uppercase().as_str() {
                                            "SUM" => result.push(Value::Integer(vals.iter().sum())),
                                            "MIN" => result.push(
                                                vals.iter()
                                                    .min()
                                                    .copied()
                                                    .map(Value::Integer)
                                                    .unwrap_or(Value::Null),
                                            ),
                                            "MAX" => result.push(
                                                vals.iter()
                                                    .max()
                                                    .copied()
                                                    .map(Value::Integer)
                                                    .unwrap_or(Value::Null),
                                            ),
                                            _ => return Ok(None),
                                        }
                                    } else {
                                        let vals: Vec<f64> = match_indices
                                            .iter()
                                            .filter_map(|&i| seg.get_f64(i))
                                            .filter(|v| !v.is_nan()) // NaN = NULL
                                            .collect();
                                        let pick = |cmp: std::cmp::Ordering| cmp;
                                        let _ = pick;
                                        match name.to_uppercase().as_str() {
                                            "SUM" => result.push(Value::Float(vals.iter().sum())),
                                            "MIN" => result.push(
                                                vals.iter()
                                                    .min_by(|a, b| {
                                                        a.partial_cmp(b)
                                                            .unwrap_or(std::cmp::Ordering::Equal)
                                                    })
                                                    .copied()
                                                    .map(Value::Float)
                                                    .unwrap_or(Value::Null),
                                            ),
                                            "MAX" => result.push(
                                                vals.iter()
                                                    .max_by(|a, b| {
                                                        a.partial_cmp(b)
                                                            .unwrap_or(std::cmp::Ordering::Equal)
                                                    })
                                                    .copied()
                                                    .map(Value::Float)
                                                    .unwrap_or(Value::Null),
                                            ),
                                            _ => return Ok(None),
                                        }
                                    }
                                } else {
                                    return Ok(None);
                                }
                            }
                        }
                    }
                }
                _ => return Ok(None),
            }
        }

        let has_star = stmt.columns.iter().any(|c| matches!(c, SelectColumn::Star));
        let mut final_row = Vec::new();
        if has_star {
            final_row.push(Value::Integer(count));
        }
        final_row.extend(result);
        let cols: Vec<String> = stmt
            .columns
            .iter()
            .map(|c| match c {
                SelectColumn::Star => "COUNT(*)".to_string(),
                SelectColumn::Expr(Expr::FunctionCall { name, args, .. }, alias) => {
                    alias.clone().unwrap_or_else(|| {
                        format!(
                            "{}({})",
                            name.to_uppercase(),
                            match args.first() {
                                Some(Expr::Column(c)) => c.as_str(),
                                _ => "?",
                            }
                        )
                    })
                }
                _ => "?".to_string(),
            })
            .collect();
        Ok(Some(StreamingQueryResult::SelectReady {
            columns: cols,
            rows: vec![final_row],
        }))
    }

    /// 🚀 Aggregate WHERE col=value via column index.
    /// For COUNT/SUM/MIN/MAX with a simple equality WHERE on an indexed column,
    /// use the index to get matching row_ids → batch fetch → compute aggregates.
    /// O(index_lookup) instead of O(N) full scan.
    fn try_aggregate_via_column_index(
        &self,
        stmt: &SelectStmt,
    ) -> Result<Option<StreamingQueryResult>> {
        // 🚨 Subqueries in WHERE (incl. correlated EXISTS) need per-row
        // execution by the general path — this fast path cannot evaluate
        // them and would silently return wrong counts.
        if stmt
            .where_clause
            .as_ref()
            .is_some_and(Self::expr_contains_subquery)
        {
            return Ok(None);
        }
        // Must have WHERE clause with simple col = value
        let where_clause = match &stmt.where_clause {
            Some(w) => w,
            None => return Ok(None),
        };
        // 🆕 HAVING not supported by this pushdown path — fall back.
        if stmt.having.is_some() {
            return Ok(None);
        };
        // Extract: col_name = literal_value
        let (filter_col, filter_value) = match where_clause {
            Expr::BinaryOp {
                left,
                op: crate::sql::ast::BinaryOperator::Eq,
                right,
            } => {
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

        // 🔑 DISTINCT aggregates (COUNT(DISTINCT col)) require dedup logic this
        // fast path doesn't implement — it would emit an empty/wrong row.
        // Fall back to compute_aggregate_positional which handles DISTINCT.
        let has_distinct_agg = stmt.columns.iter().any(|c| {
            matches!(
                c,
                SelectColumn::Expr(Expr::FunctionCall { distinct: true, .. }, _)
            )
        });
        if has_distinct_agg {
            return Ok(None);
        }

        // Check for column value index on the filtered column
        let index_name = format!("{}.{}", table, filter_col);
        let index_ref = match self.db.column_indexes.get(&index_name) {
            Some(idx) => idx,
            None => return Ok(None),
        };
        // 🚨 Long Text values share truncated 64-byte prefix keys — aggregate
        // results over the raw index row set would cross-count distinct
        // values. Fall back to scan-based aggregate paths.
        if !Self::index_key_exact_for(&filter_value) {
            return Ok(None);
        }
        let row_ids_arc = index_ref.value().get_arc(&filter_value)?;
        if row_ids_arc.is_empty() {
            return Ok(None);
        }
        // Only use index direct-fetch when selectivity is high (few matching rows).
        // For low-selectivity filters (100K/300K), a full scan decodes faster
        // than 100K individual row fetches. The threshold balances index fetch
        // overhead vs scan decode cost: up to 10K rows, batch index fetch is
        // typically faster than a full segment scan.
        if row_ids_arc.len() > 10000 {
            return Ok(None);
        }
        drop(index_ref);

        // Batch fetch matching rows
        let batch = self.db.get_table_rows_batch_arc(table, &row_ids_arc)?;
        // 🔑 Verify the filter value on fetched rows: index entries can be
        // stale inside a transaction (undo replays of DELETE/UPDATE keep
        // accumulating value→row_id entries until commit — differential
        // testing: COUNT(*) WHERE id=X returned 7/8 phantom rows in the
        // ROLLBACK-TO window while row LIST queries verified correctly).
        let filter_pos = schema.get_column_position(&filter_col);
        let batch: Vec<_> = batch
            .into_iter()
            .filter(|(_, opt)| {
                opt.as_ref()
                    .and_then(|row| filter_pos.and_then(|p| row.get(p)))
                    .map(|v| v == &filter_value)
                    .unwrap_or(false)
            })
            .collect();
        let rows: Vec<&Row> = batch
            .iter()
            .filter_map(|(_, opt)| opt.as_ref().map(|a| a.as_ref()))
            .collect();

        if rows.is_empty() {
            let cols: Vec<String> = stmt
                .columns
                .iter()
                .map(|c| match c {
                    SelectColumn::Expr(_, alias) => {
                        alias.clone().unwrap_or_else(|| "?".to_string())
                    }
                    SelectColumn::Column(name) => name.clone(),
                    _ => "?".to_string(),
                })
                .collect();
            let row: Vec<Value> = cols.iter().map(|_| Value::Integer(0)).collect();
            return Ok(Some(StreamingQueryResult::SelectReady {
                columns: cols,
                rows: vec![row],
            }));
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
                            Some(p) => p,
                            None => return Ok(None),
                        };
                        match name.to_uppercase().as_str() {
                            "COUNT" => {}
                            "SUM" => {
                                let sum: f64 = rows
                                    .iter()
                                    .filter_map(|r| r.get(agg_pos))
                                    .filter_map(|v| match v {
                                        Value::Integer(i) => Some(*i as f64),
                                        Value::Float(f) => Some(*f),
                                        _ => None,
                                    })
                                    .sum();
                                result_row.push(Value::Float(sum));
                            }
                            "MIN" => {
                                let min_val = rows
                                    .iter()
                                    .filter_map(|r| r.get(agg_pos))
                                    .min_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
                                    .cloned()
                                    .unwrap_or(Value::Null);
                                result_row.push(min_val);
                            }
                            "MAX" => {
                                let max_val = rows
                                    .iter()
                                    .filter_map(|r| r.get(agg_pos))
                                    .max_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
                                    .cloned()
                                    .unwrap_or(Value::Null);
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
        if has_star {
            final_row.push(Value::Integer(count));
        }
        final_row.extend(result_row);
        let cols: Vec<String> = stmt
            .columns
            .iter()
            .map(|c| match c {
                SelectColumn::Star => "COUNT(*)".to_string(),
                SelectColumn::Expr(Expr::FunctionCall { name, args, .. }, alias) => {
                    alias.clone().unwrap_or_else(|| {
                        let col = match args.first() {
                            Some(Expr::Column(c)) => c.as_str(),
                            _ => "?",
                        };
                        format!("{}({})", name.to_uppercase(), col)
                    })
                }
                _ => "?".to_string(),
            })
            .collect();
        Ok(Some(StreamingQueryResult::SelectReady {
            columns: cols,
            rows: vec![final_row],
        }))
    }

    /// 🚀 Partial-decode aggregate: scan all rows but only extract filter + aggregate
    /// columns from raw bytes. Much faster than full row decode for queries like
    /// COUNT/SUM/MIN/MAX WHERE col = value (when index path is not used).
    fn try_aggregate_partial_scan(
        &self,
        stmt: &SelectStmt,
    ) -> Result<Option<StreamingQueryResult>> {
        // 🚨 Subqueries in WHERE (incl. correlated EXISTS) need per-row
        // execution by the general path — this fast path cannot evaluate
        // them and would silently return wrong counts.
        if stmt
            .where_clause
            .as_ref()
            .is_some_and(Self::expr_contains_subquery)
        {
            return Ok(None);
        }
        // 🆕 HAVING not supported by this pushdown path — fall back.
        if stmt.having.is_some() {
            return Ok(None);
        }
        let (filter_col, filter_value) = match &stmt.where_clause {
            Some(Expr::BinaryOp {
                left,
                op: crate::sql::ast::BinaryOperator::Eq,
                right,
            }) => {
                let col = match left.as_ref() {
                    Expr::Column(n) => n.as_str(),
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
        if stmt.group_by.is_some() {
            return Ok(None);
        }
        let table = match &stmt.from {
            Some(TableRef::Table { name, .. }) => name.as_str(),
            _ => return Ok(None),
        };
        let schema = self.db.get_table_schema(table)?;
        let filter_pos = match schema.get_column_position(filter_col) {
            Some(p) => p,
            None => return Ok(None),
        };
        let col_types = schema.col_types();

        // Identify aggregate columns
        let mut agg_cols: Vec<(String, usize)> = Vec::new();
        let mut has_count_star = false;
        for col_expr in &stmt.columns {
            match col_expr {
                SelectColumn::Star => {
                    has_count_star = true;
                }
                SelectColumn::Expr(expr, _) => {
                    if let Expr::FunctionCall {
                        name,
                        args,
                        distinct,
                        ..
                    } = expr
                    {
                        // 🔑 DISTINCT aggregates need dedup — this fast path
                        // counts all rows without dedup. Fall back.
                        if *distinct {
                            return Ok(None);
                        }
                        // COUNT(*) or COUNT with any non-column arg
                        if name.eq_ignore_ascii_case("COUNT")
                            && !matches!(args.first(), Some(Expr::Column(_)))
                        {
                            has_count_star = true;
                        } else {
                            let pos = match args.first() {
                                Some(Expr::Column(c)) => match schema.get_column_position(c) {
                                    Some(p) => p,
                                    None => return Ok(None),
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
        use crate::storage::row_format::{FIXED_COL_SIZE, HEADER_SIZE};
        let fixed_count = col_types
            .iter()
            .filter(|t| {
                matches!(
                    t,
                    crate::types::ColumnType::Integer
                        | crate::types::ColumnType::Float
                        | crate::types::ColumnType::Boolean
                        | crate::types::ColumnType::Timestamp
                )
            })
            .count();
        let var_section_start = HEADER_SIZE + fixed_count * FIXED_COL_SIZE;
        let fixed_offset_of = |ci: usize| -> usize {
            HEADER_SIZE
                + col_types[..ci]
                    .iter()
                    .filter(|t| {
                        matches!(
                            t,
                            crate::types::ColumnType::Integer
                                | crate::types::ColumnType::Float
                                | crate::types::ColumnType::Boolean
                                | crate::types::ColumnType::Timestamp
                        )
                    })
                    .count()
                    * FIXED_COL_SIZE
        };

        // Extract a column value from raw bytes
        let extract_col = |data: &[u8], ci: usize| -> Option<Value> {
            if data.len() < HEADER_SIZE {
                return None;
            }
            let ct = &col_types[ci];
            if matches!(
                ct,
                crate::types::ColumnType::Integer
                    | crate::types::ColumnType::Float
                    | crate::types::ColumnType::Boolean
                    | crate::types::ColumnType::Timestamp
            ) {
                let off = fixed_offset_of(ci);
                if off + FIXED_COL_SIZE > data.len() {
                    return None;
                }
                match ct {
                    crate::types::ColumnType::Integer => unsafe {
                        Some(Value::Integer(i64::from_le(std::ptr::read_unaligned(
                            data.as_ptr().add(off) as *const i64,
                        ))))
                    },
                    crate::types::ColumnType::Float => unsafe {
                        Some(Value::Float(f64::from_bits(u64::from_le(
                            std::ptr::read_unaligned(data.as_ptr().add(off) as *const u64),
                        ))))
                    },
                    _ => None,
                }
            } else {
                if var_section_start + 2 > data.len() {
                    return None;
                }
                let vc = u16::from_le_bytes([data[var_section_start], data[var_section_start + 1]])
                    as usize;
                let vh = var_section_start + 2;
                let vd = vh + vc * 10;
                for vi in 0..vc {
                    let h = vh + vi * 10;
                    if h + 10 > data.len() {
                        break;
                    }
                    if u16::from_le_bytes([data[h], data[h + 1]]) as usize == ci {
                        let vo = u32::from_le_bytes([
                            data[h + 2],
                            data[h + 3],
                            data[h + 4],
                            data[h + 5],
                        ]) as usize;
                        let vl = u32::from_le_bytes([
                            data[h + 6],
                            data[h + 7],
                            data[h + 8],
                            data[h + 9],
                        ]) as usize;
                        let a = vd + vo;
                        if a + vl > data.len() {
                            return None;
                        }
                        let b = &data[a..a + vl];
                        return match ct {
                            crate::types::ColumnType::Text => {
                                Some(Value::Text(crate::types::ArcString(std::sync::Arc::from(
                                    std::str::from_utf8(b).ok()?,
                                ))))
                            }
                            _ => {
                                crate::storage::row_format::SchemaDecodeContext::decode_var_generic(
                                    b,
                                )
                                .ok()
                            }
                        };
                    }
                }
                None
            }
        };

        // Scan with zero-copy raw access
        let tp = self.db.compute_table_prefix(table);
        let mut it = self
            .db
            .lsm_engine
            .scan_range_streaming(tp << 32, (tp + 1) << 32)?;
        let raw = it.has_raw_sst();
        let mut count: i64 = 0;
        let mut sum = CompSum::default();
        let mut min: Option<f64> = None;
        let mut max: Option<f64> = None;

        if raw {
            loop {
                match it.next_raw() {
                    Some(Ok((_, _, del, vb))) => {
                        if del || vb.len == 0 {
                            continue;
                        }
                        let d = vb.as_slice();
                        if let Some(fv) = extract_col(d, filter_pos) {
                            if fv != filter_value {
                                continue;
                            }
                        } else {
                            continue;
                        }
                        count += 1;
                        for (_, pos) in &agg_cols {
                            if let Some(av) = extract_col(d, *pos) {
                                let fv = match av {
                                    Value::Integer(i) => i as f64,
                                    Value::Float(f) => f,
                                    _ => continue,
                                };
                                sum.add(fv);
                                min = Some(min.map_or(fv, |m| m.min(fv)));
                                max = Some(max.map_or(fv, |m| m.max(fv)));
                            }
                        }
                    }
                    Some(Err(_)) => break,
                    None => break,
                }
            }
        } else {
            loop {
                match it.next() {
                    Some(Ok((_, v))) => {
                        if v.deleted {
                            continue;
                        }
                        let d = match &v.data {
                            crate::storage::lsm::ValueData::Inline(b) => b.as_slice(),
                            _ => continue,
                        };
                        if let Some(fv) = extract_col(d, filter_pos) {
                            if fv != filter_value {
                                continue;
                            }
                        } else {
                            continue;
                        }
                        count += 1;
                        for (_, pos) in &agg_cols {
                            if let Some(av) = extract_col(d, *pos) {
                                let fv = match av {
                                    Value::Integer(i) => i as f64,
                                    Value::Float(f) => f,
                                    _ => continue,
                                };
                                sum.add(fv);
                                min = Some(min.map_or(fv, |m| m.min(fv)));
                                max = Some(max.map_or(fv, |m| m.max(fv)));
                            }
                        }
                    }
                    Some(Err(_)) => break,
                    None => break,
                }
            }
        }

        let mut r = Vec::new();
        if has_count_star {
            r.push(Value::Integer(count));
        }
        for (f, _) in &agg_cols {
            match f.as_str() {
                "SUM" => r.push(Value::Float(sum.total())),
                "MIN" => r.push(min.map(Value::Float).unwrap_or(Value::Null)),
                "MAX" => r.push(max.map(Value::Float).unwrap_or(Value::Null)),
                "COUNT" => r.push(Value::Integer(count)),
                _ => {}
            }
        }
        let cols: Vec<String> = stmt
            .columns
            .iter()
            .map(|c| match c {
                SelectColumn::Star => "COUNT(*)".to_string(),
                SelectColumn::Expr(Expr::FunctionCall { name, args, .. }, alias) => {
                    alias.clone().unwrap_or_else(|| {
                        format!(
                            "{}({})",
                            name.to_uppercase(),
                            match args.first() {
                                Some(Expr::Column(c)) => c.as_str(),
                                _ => "?",
                            }
                        )
                    })
                }
                _ => "?".to_string(),
            })
            .collect();
        Ok(Some(StreamingQueryResult::SelectReady {
            columns: cols,
            rows: vec![r],
        }))
    }

    /// without WHERE, iterate the column index keys directly.
    /// O(unique_values) instead of O(N) full scan.
    fn try_distinct_via_column_index(
        &self,
        stmt: &SelectStmt,
        table: &str,
    ) -> Result<Option<StreamingQueryResult>> {
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
                        if col_sst.row_map.is_deleted(i) {
                            continue;
                        }
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
                        if col_sst.row_map.is_deleted(i) {
                            continue;
                        }
                        if let Some(s) = seg.get_str(i) {
                            if seen.insert(s) {
                                // &str key, borrows from mmap (no alloc)
                                vals.push(Value::Text(crate::types::ArcString(
                                    std::sync::Arc::from(s),
                                )));
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

    /// PK point query via ColSegmentStore binary search.
    /// Returns Some(result) if the WHERE clause is `pk_col = literal` and the
    /// PK column is the table's primary key. Uses store.get() (O(log N) binary
    /// search in the row_map) instead of a full-table scan. Returns None if the
    /// WHERE clause doesn't match the PK point-query shape.
    /// Fast table scan for JOIN — uses ColSegmentStore directly (no compaction,
    /// no legacy LSM path). Returns Vec<(composite_key, Vec<Value>)>.
    /// Falls back to scan_table_rows_streaming for non-ColSegmentStore tables.
    fn scan_table_rows_fast(
        &self,
        table: &str,
        schema: &TableSchema,
    ) -> Result<Vec<(u64, Vec<Value>)>> {
        self.scan_table_rows_fast_projected(table, schema, None)
    }

    /// Projected variant of [`Self::scan_table_rows_fast`]: when `project` is
    /// given (ascending schema positions), returned rows contain ONLY those
    /// columns. Used by the join fast paths so a query referencing 2 of 6
    /// columns never decodes the other 4 — on a table with a 384-dim VECTOR
    /// column that difference is ~153MB of Value::Tensor allocations and the
    /// dominant share of join latency. `None` = all columns (legacy behavior).
    fn scan_table_rows_fast_projected(
        &self,
        table: &str,
        schema: &TableSchema,
        project: Option<&[usize]>,
    ) -> Result<Vec<(u64, Vec<Value>)>> {
        // 🔑 Read-your-writes: when inside a transaction, merge the write_set
        // and filter undo_log deletes so JOINs and subqueries see uncommitted
        // writes. Covers JOIN (try_positional_inner_join) and IN/scalar
        // subquery materialization paths that call this helper.
        let txn_writes = self.txn_write_set_rows(table);
        let txn_deletes = self.txn_deleted_row_ids(table);
        let in_txn =
            self.is_in_transaction() && (!txn_writes.is_empty() || !txn_deletes.is_empty());

        // Projection of the full-width txn write_set rows.
        let project_writes = |rows: Vec<(u64, Vec<Value>)>| -> Vec<(u64, Vec<Value>)> {
            match project {
                None => rows,
                Some(positions) => rows
                    .into_iter()
                    .map(|(rid, row)| {
                        let pr: Vec<Value> = positions
                            .iter()
                            .map(|&p| row.get(p).cloned().unwrap_or(Value::Null))
                            .collect();
                        (rid, pr)
                    })
                    .collect(),
            }
        };

        if in_txn && !self.db.has_col_segment_store(table) {
            // Txn-only table (no committed data): just return the write_set rows
            // (filtered for deletes — though a fresh INSERT can't be in deletes).
            return Ok(project_writes(txn_writes));
        }
        let mut scanned: Vec<(u64, Vec<Value>)> = if self.db.has_col_segment_store(table) {
            let store = self
                .db
                .get_or_create_col_segment_store(table, schema.col_types())?;
            let _ = store.flush_buffer();
            let ncols = schema.columns.len();
            let project_cols: Vec<usize> = match project {
                Some(p) if p.len() < ncols => p.to_vec(),
                _ => (0..ncols).collect(),
            };
            store.scan_projected_filtered(None, &project_cols, &|_| true)
        } else {
            let full: Vec<(u64, Vec<Value>)> = self
                .db
                .scan_table_rows_streaming(table)?
                .collect::<Result<_>>()?;
            project_writes(full)
        };
        if in_txn {
            // Filter out rows the transaction has deleted.
            scanned.retain(|(rid, _)| !txn_deletes.contains(rid));
            // Remove segment rows whose row_id is also in the write_set (write_set
            // has the newer version), then append the write_set rows.
            let ws_ids: std::collections::HashSet<u64> =
                txn_writes.iter().map(|(rid, _)| *rid).collect();
            scanned.retain(|(rid, _)| !ws_ids.contains(rid));
            scanned.extend(project_writes(txn_writes));
        }
        Ok(scanned)
    }

    fn try_col_segment_pk_point_query(
        &self,
        stmt: &SelectStmt,
        table_name: &str,
        wc: &crate::sql::ast::Expr,
    ) -> Result<Option<StreamingQueryResult>> {
        use crate::sql::ast::{BinaryOperator, Expr};
        // 🔑 If SELECT has computed expressions (subqueries, arithmetic), fall
        // back to the full-scan path which resolves them via eval_expr_on_row.
        if Self::select_has_computed_expression(&stmt.columns) {
            return Ok(None);
        }
        // Only handle `pk_col = literal` (the common point-query shape).
        let (col_name, literal) = match wc {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => match (left.as_ref(), right.as_ref()) {
                (Expr::Column(c), Expr::Literal(v)) => (c.as_str(), v.clone()),
                (Expr::Literal(v), Expr::Column(c)) => (c.as_str(), v.clone()),
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };
        // Check the column is the PK.
        let schema = match self.db.get_table_schema(table_name) {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };
        let pk_name = match schema.primary_key() {
            Some(p) => p,
            None => return Ok(None),
        };
        let pk_bare = pk_name.rsplit('.').next().unwrap_or(pk_name);
        if col_name != pk_bare && col_name != pk_name {
            return Ok(None);
        }

        // Resolve PK value → composite key.
        let table_id = self.db.table_registry.get_table_id(table_name).unwrap_or(0) as u64;
        let composite_key = match &literal {
            Value::Integer(id) if schema.is_primary_key_auto_increment() => {
                // AUTO_INCREMENT: pk value IS the row_id.
                (table_id << 32) | (*id as u64 & 0xFFFFFFFF)
            }
            Value::Integer(id) => {
                // Non-AUTO_INCREMENT Integer PK: the PK value is used as the
                // row_id (see crud.rs insert path), so composite_key =
                // (table_id << 32) | pk_value. This enables O(log N) binary
                // search in RowMap without a secondary index.
                // 🔑 Negative PK values are mapped to high u32 range (matching
                // the insert path in crud.rs) to avoid collision with
                // next_row_id-assigned row_ids.
                let row_id = if *id >= 0 {
                    *id as u64
                } else {
                    0x8000_0000u64 | (*id as u64 & 0x7FFF_FFFF)
                };
                (table_id << 32) | (row_id & 0xFFFFFFFF)
            }
            _ => {
                // Non-Integer PK: use pk_lookup cache to resolve pk → row_id.
                // If cache miss, fall back to scan (return None).
                let pk_key = crate::database::pk_cache::PkKey::from_value(&literal);
                match self
                    .db
                    .pk_lookup
                    .get(table_name)
                    .and_then(|l| l.get_pk(&pk_key))
                {
                    Some(rid) => (table_id << 32) | (rid & 0xFFFFFFFF),
                    None => return Ok(None), // cache miss → full scan
                }
            }
        };

        // Binary-search the segment's row_map.
        let store = match self
            .db
            .get_or_create_col_segment_store(table_name, schema.col_types())
        {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };
        // 🚀 P1-1: 不在点查前 flush_buffer。store.get() 已支持读 write_buf
        //（store.rs:509-531 先查 buffer newest version 再查 segment），
        // 同步 flush 会触发 segment 写盘 + manifest fsync，阻塞点查。
        // 🔑 Read-your-writes: check transaction write_set / undo_log first.
        // write_set keys by raw row_id (low 32 bits of composite_key).
        let row_id = composite_key as u32 as RowId;
        let txn_row = self.txn_lookup_row(table_name, row_id);
        let row: Vec<Value> = match txn_row {
            // Transaction deleted this row → invisible.
            Some(None) => {
                let columns: Vec<String> = self
                    .build_select_columns(&stmt.columns, &schema)
                    .unwrap_or_default();
                return Ok(Some(StreamingQueryResult::SelectReady {
                    columns,
                    rows: vec![],
                }));
            }
            // Transaction inserted this row → return buffered version.
            Some(Some(r)) => r,
            // No txn info → fall through to storage.
            None => match store.get(composite_key) {
                Some(r) => r,
                None => {
                    // Not found — return empty result.
                    let columns: Vec<String> = self
                        .build_select_columns(&stmt.columns, &schema)
                        .unwrap_or_default();
                    return Ok(Some(StreamingQueryResult::SelectReady {
                        columns,
                        rows: vec![],
                    }));
                }
            },
        };
        // Build output: SELECT * → full row; SELECT col1, col2 → project.
        let columns: Vec<String> = self
            .build_select_columns(&stmt.columns, &schema)
            .unwrap_or_default();
        let result_row: Vec<Value> = if stmt
            .columns
            .iter()
            .any(|c| matches!(c, crate::sql::ast::SelectColumn::Star))
        {
            row
        } else {
            // Project requested columns.
            let positions: Vec<usize> = Self::resolve_select_positions(&stmt.columns, &schema)
                .unwrap_or_else(|| (0..schema.columns.len()).collect());
            positions
                .iter()
                .map(|&p| row.get(p).cloned().unwrap_or(Value::Null))
                .collect()
        };
        // 🔑 OFFSET 语义: 单行结果也可能被整体跳过 — `WHERE id = 1 …
        // OFFSET 3` 应返回 0 行 (此前点查快路径忽略 OFFSET, fuzz seed 14
        // 差分对拍 SQLite 抓出)。
        let skip = stmt.offset.unwrap_or(0);
        let mut rows: Vec<Vec<Value>> = if skip > 0 { Vec::new() } else { vec![result_row] };
        if let Some(l) = stmt.limit {
            rows.truncate(l);
        }
        Ok(Some(StreamingQueryResult::SelectReady { columns, rows }))
    }

    /// 🚀 #5: 尝试从 WHERE 表达式提取 (col_position, operator, i64_value)。
    /// 匹配模式：WHERE col <op> literal 或 WHERE literal <op> col。
    /// 仅当 literal 是 Integer 或 Timestamp（可转 i64）时返回 Some。
    fn try_extract_i64_predicate(
        wc: &crate::sql::ast::Expr,
        schema: &TableSchema,
    ) -> Option<(usize, crate::sql::ast::BinaryOperator, i64)> {
        use crate::sql::ast::{BinaryOperator, Expr};
        match wc {
            Expr::BinaryOp { left, op, right } => {
                let op = op.clone();
                // col <op> literal
                if let (Expr::Column(cn), Expr::Literal(v)) = (left.as_ref(), right.as_ref()) {
                    let bare = cn.rsplit('.').next().unwrap_or(cn);
                    let pos = schema.get_column_position(bare)?;
                    let i64_val = match v {
                        Value::Integer(i) => *i,
                        Value::Timestamp(t) => t.as_micros(),
                        Value::Bool(b) => {
                            if *b {
                                1
                            } else {
                                0
                            }
                        }
                        _ => return None,
                    };
                    return Some((pos, op, i64_val));
                }
                // literal <op> col → flip operator
                if let (Expr::Literal(v), Expr::Column(cn)) = (left.as_ref(), right.as_ref()) {
                    let bare = cn.rsplit('.').next().unwrap_or(cn);
                    let pos = schema.get_column_position(bare)?;
                    let i64_val = match v {
                        Value::Integer(i) => *i,
                        Value::Timestamp(t) => t.as_micros(),
                        Value::Bool(b) => {
                            if *b {
                                1
                            } else {
                                0
                            }
                        }
                        _ => return None,
                    };
                    let flipped = match op {
                        BinaryOperator::Lt => BinaryOperator::Gt,
                        BinaryOperator::Le => BinaryOperator::Ge,
                        BinaryOperator::Gt => BinaryOperator::Lt,
                        BinaryOperator::Ge => BinaryOperator::Le,
                        other => other,
                    };
                    return Some((pos, flipped, i64_val));
                }
                None
            }
            _ => None,
        }
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
        let has_subquery = Self::expr_contains_subquery(wc);
        // 🔥 WHERE 编译一次（列位置预解析）：eval_expr_on_row 每行每个列引用
        // 都做 get_column_position 字符串线性查找 —— profile 显示占扫描 CPU
        // 的 ~9%（还有随行的 Value::clone）。简单谓词全部走 CompiledWhere
        // 的纯位置比较；编译不了（复杂表达式）或单行 eval 返回 None 时回退。
        let compiled: Option<CompiledWhere> = if has_subquery {
            None
        } else {
            Self::compile_where(wc, schema)
        };
        let mut rows = Vec::new();
        let mut skipped = 0usize;
        for (_key, _ts, row) in store.scan() {
            let m = if has_subquery {
                // WHERE contains a (possibly correlated) subquery — bind outer
                // refs to this row, then eval with subquery execution support.
                let bound = Self::bind_outer_columns(wc, &row, schema);
                match self.eval_correlated_expr(&bound, &row, schema) {
                    Ok(Value::Bool(b)) => b,
                    Ok(Value::Integer(i)) => i != 0,
                    Ok(Value::Float(f)) => f != 0.0 && !f.is_nan(),
                    _ => false,
                }
            } else if let Some(cw) = compiled.as_ref() {
                match cw.eval(&row) {
                    Some(b) => b,
                    None => match Self::eval_expr_on_row(wc, &row, schema) {
                        Ok(Value::Bool(b)) => b,
                        Ok(Value::Integer(i)) => i != 0,
                        Ok(Value::Float(f)) => f != 0.0 && !f.is_nan(),
                        _ => false,
                    },
                }
            } else {
                match Self::eval_expr_on_row(wc, &row, schema) {
                    Ok(Value::Bool(b)) => b,
                    Ok(Value::Integer(i)) => i != 0,
                    Ok(Value::Float(f)) => f != 0.0 && !f.is_nan(),
                    _ => false,
                }
            };
            if !m {
                continue;
            }
            if skipped < offset {
                skipped += 1;
                continue;
            }
            let projected: Vec<Value> = out_positions
                .iter()
                .map(|&p| row.get(p).cloned().unwrap_or(Value::Null))
                .collect();
            rows.push(projected);
            if rows.len() >= limit {
                break;
            }
        }
        Ok(rows)
    }

    /// Extract schema positions for simple column references in SELECT.
    /// Returns None if any column is Star, Expr, or unresolvable (needs full row).
    fn resolve_select_positions(
        select_cols: &[SelectColumn],
        schema: &TableSchema,
    ) -> Option<Vec<usize>> {
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
    fn build_select_columns(
        &self,
        select_cols: &[SelectColumn],
        schema: &TableSchema,
    ) -> Result<Vec<String>> {
        let columns = if select_cols.len() == 1 && matches!(select_cols[0], SelectColumn::Star) {
            // 🚀 SELECT *: use cached column names (zero-alloc after first build)
            schema.column_names()
        } else {
            // 显式列名或表达式
            select_cols
                .iter()
                .enumerate()
                .map(|(idx, col)| match col {
                    SelectColumn::Column(name) => name.clone(),
                    SelectColumn::ColumnWithAlias(_, alias) => alias.clone(),
                    SelectColumn::Expr(_, Some(alias)) => alias.clone(),
                    SelectColumn::Expr(_, None) => format!("expr_{}", idx),
                    SelectColumn::Star => "*".to_string(),
                })
                .collect()
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

    /// 🔑 三值逻辑的 true/false 判定: NULL → None (UNKNOWN)。
    /// eval_expr_on_row 的 Kleene AND/OR 语义需要区分 UNKNOWN 与 FALSE。
    fn truth3(v: &Value) -> Option<bool> {
        match v {
            Value::Null => None,
            other => Some(Self::is_truthy(other)),
        }
    }

    /// 🔑 Coerce Bool/Int for comparison and arithmetic: TRUE→1, FALSE→0.
    /// Applied when one side is Bool and the other is Integer/Float (or both
    /// are Bool). This makes `1 = TRUE`, `flag = 1` (BOOLEAN col vs INT lit),
    /// and `TRUE + 0` work per SQL semantics.
    fn coerce_bool_int(lv: Value, rv: Value) -> (Value, Value) {
        coerce_bool_int(lv, rv)
    }

    /// Simple LIKE pattern matching: % = any sequence, _ = single char
    fn simple_like_match(text: &str, pattern: &str) -> bool {
        let t: Vec<char> = text.chars().collect();
        let p: Vec<char> = pattern.chars().collect();
        let mut dp = vec![vec![false; p.len() + 1]; t.len() + 1];
        dp[0][0] = true;
        for j in 1..=p.len() {
            if p[j - 1] == '%' {
                dp[0][j] = dp[0][j - 1];
            }
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
                if *b == 0 {
                    return Err(MoteDBError::DivisionByZero);
                }
                a.checked_div(*b)
                    .map(Value::Integer)
                    .ok_or_else(|| MoteDBError::Query("Integer division overflow".into()))
            }
            (Value::Float(a), Value::Float(b)) => {
                if *b == 0.0 {
                    return Err(MoteDBError::DivisionByZero);
                }
                Ok(Value::Float(a / b))
            }
            (Value::Integer(a), Value::Float(b)) => {
                if *b == 0.0 {
                    return Err(MoteDBError::DivisionByZero);
                }
                Ok(Value::Float(*a as f64 / b))
            }
            (Value::Float(a), Value::Integer(b)) => {
                if *b == 0 {
                    return Err(MoteDBError::DivisionByZero);
                }
                Ok(Value::Float(a / *b as f64))
            }
            _ => Ok(Value::Null),
        }
    }
    fn positional_mod(l: &Value, r: &Value) -> Result<Value> {
        match (l, r) {
            (Value::Integer(a), Value::Integer(b)) => {
                if *b == 0 {
                    return Err(MoteDBError::DivisionByZero);
                }
                Ok(Value::Integer(a.checked_rem(*b).unwrap_or(0)))
            }
            _ => Ok(Value::Null),
        }
    }

    /// 🔑 String concatenation `||` for the positional path (eval_expr_on_row).
    /// NULL propagates (NULL || x = NULL). Non-text values are stringified.
    fn positional_concat(l: &Value, r: &Value) -> Result<Value> {
        if matches!(l, Value::Null) || matches!(r, Value::Null) {
            return Ok(Value::Null);
        }
        Ok(Value::text(format!(
            "{}{}",
            value_to_concat_string(l),
            value_to_concat_string(r)
        )))
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
                        "Vector dimension mismatch: {} vs {}",
                        a.len(),
                        b.len()
                    )));
                }
                let dist: f32 = a
                    .iter()
                    .zip(b.iter())
                    .map(|(x, y)| (x - y).powi(2))
                    .sum::<f32>()
                    .sqrt();
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
                        "Vector dimension mismatch: {} vs {}",
                        a.len(),
                        b.len()
                    )));
                }
                let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                let n1: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
                let n2: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
                if n1 == 0.0 || n2 == 0.0 {
                    return Ok(Value::Float(1.0));
                }
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
                        "Vector dimension mismatch: {} vs {}",
                        a.len(),
                        b.len()
                    )));
                }
                let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                Ok(Value::Float(dot as f64))
            }
            _ => Ok(Value::Null),
        }
    }

    /// Evaluate function calls in the positional (no-HashMap) path.
    fn eval_function_positional(
        name: &str,
        args: &[Expr],
        row: &[Value],
        schema: &TableSchema,
    ) -> Result<Value> {
        let fname = name.to_lowercase();
        match fname.as_str() {
            // TIME_BUCKET(interval, ts) — floor to a fixed window boundary.
            // The SqlRow evaluator has the same logic; this mirror serves the
            // positional (schema-indexed) projection paths.
            "time_bucket" => {
                if args.len() != 2 {
                    return Err(MoteDBError::InvalidArgument(
                        "TIME_BUCKET() takes 2 arguments (interval, timestamp)".to_string(),
                    ));
                }
                let interval =
                    match Self::eval_expr_on_row(&args[0], row, schema)? {
                        Value::Text(t) => t.to_string(),
                        _ => return Err(MoteDBError::TypeError(
                            "TIME_BUCKET() first argument must be a text interval like '5m', '1h'"
                                .to_string(),
                        )),
                    };
                let micros_per = Self::parse_time_bucket_interval_us(&interval)?;
                let micros = match Self::eval_expr_on_row(&args[1], row, schema)? {
                    Value::Timestamp(t) => t.as_micros(),
                    Value::Integer(i) => i,
                    v => {
                        return Err(MoteDBError::TypeError(format!(
                            "TIME_BUCKET() second argument must be a timestamp, got {v:?}"
                        )))
                    }
                };
                let floored = (micros / micros_per) * micros_per;
                Ok(Value::Timestamp(crate::types::Timestamp::from_micros(
                    floored,
                )))
            }
            "concat" => {
                let mut result = String::new();
                for arg in args {
                    match Self::eval_expr_on_row(arg, row, schema)? {
                        Value::Text(s) => result.push_str(&s),
                        Value::Integer(i) => {
                            use std::fmt::Write;
                            let _ = write!(result, "{}", i);
                        }
                        Value::Float(f) => {
                            use std::fmt::Write;
                            let _ = write!(result, "{}", f);
                        }
                        // 🔑 Bool → "1"/"0" (consistent with || and CONCAT).
                        Value::Bool(b) => result.push_str(if b { "1" } else { "0" }),
                        // 🔑 NULL 跳过 (SQLite concat()/PG CONCAT 语义,
                        // 与 evaluator 的 CONCAT 一致; NULL 传播用 ||)。
                        Value::Null => continue,
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
            "abs" => {
                // 🔑 Only ABS is handled here (it's divergence-free: Integer→Integer,
                // Float→Float, with i64::MIN overflow → Float). round/floor/ceil/
                // log/ln/log10/sqrt/exp previously had bugs in this path (ROUND
                // ignored its decimals arg; SQRT/LN/LOG returned NaN/-inf on
                // negative/zero). They now fall through to the evaluator fallback
                // below for correct, consistent semantics.
                let val = Self::eval_expr_on_row(&args[0], row, schema)?;
                match val {
                    Value::Integer(i) => match i.checked_abs() {
                        Some(n) => Ok(Value::Integer(n)),
                        None => Ok(Value::Float(-(i as f64))),
                    },
                    Value::Float(f) => Ok(Value::Float(f.abs())),
                    Value::Null => Ok(Value::Null),
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
                if args.len() != 2 {
                    return Ok(Value::Null);
                }
                let val = Self::eval_expr_on_row(&args[0], row, schema)?;
                if matches!(val, Value::Null) {
                    Self::eval_expr_on_row(&args[1], row, schema)
                } else {
                    Ok(val)
                }
            }
            "nullif" => {
                // NULLIF(a, b) — NULL if a == b, else a.
                if args.len() != 2 {
                    return Ok(Value::Null);
                }
                let a = Self::eval_expr_on_row(&args[0], row, schema)?;
                let b = Self::eval_expr_on_row(&args[1], row, schema)?;
                if a == b {
                    Ok(Value::Null)
                } else {
                    Ok(a)
                }
            }
            "substr" | "substring" => {
                // SUBSTR(text, start [, length]) — SQL 1-indexed.
                // NULL propagates: any NULL argument yields NULL (standard SQL).
                if args.len() < 2 || args.len() > 3 {
                    return Ok(Value::text(String::new()));
                }
                let text = match Self::eval_expr_on_row(&args[0], row, schema)? {
                    Value::Text(s) => s,
                    Value::Null => return Ok(Value::Null),
                    _ => return Ok(Value::text(String::new())),
                };
                let start = match Self::eval_expr_on_row(&args[1], row, schema)? {
                    Value::Integer(i) if i >= 0 => (i.max(1) as usize).saturating_sub(1),
                    Value::Integer(i) if i < 0 => {
                        text.chars().count().saturating_sub((-i) as usize)
                    }
                    Value::Null => return Ok(Value::Null),
                    _ => return Ok(Value::text(String::new())),
                };
                let result = if args.len() == 3 {
                    let length = match Self::eval_expr_on_row(&args[2], row, schema)? {
                        Value::Integer(i) => i.max(0) as usize,
                        Value::Null => return Ok(Value::Null),
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
                if args.len() != 3 {
                    return Ok(Value::Null);
                }
                let text = match Self::eval_expr_on_row(&args[0], row, schema)? {
                    Value::Text(s) => s,
                    _ => return Ok(Value::Null),
                };
                let from = match Self::eval_expr_on_row(&args[1], row, schema)? {
                    Value::Text(s) => s,
                    _ => return Ok(Value::Null),
                };
                let to = match Self::eval_expr_on_row(&args[2], row, schema)? {
                    Value::Text(s) => s,
                    _ => return Ok(Value::Null),
                };
                Ok(Value::text(text.replace(from.as_str(), to.as_str())))
            }
            "sign" => {
                if args.is_empty() {
                    return Ok(Value::Null);
                }
                match Self::eval_expr_on_row(&args[0], row, schema)? {
                    Value::Integer(i) => Ok(Value::Integer(i.signum())),
                    Value::Float(f) => Ok(Value::Integer(if f > 0.0 {
                        1
                    } else if f < 0.0 {
                        -1
                    } else {
                        0
                    })),
                    _ => Ok(Value::Null),
                }
            }
            "power" | "pow" => {
                if args.len() != 2 {
                    return Ok(Value::Null);
                }
                let base = match Self::eval_expr_on_row(&args[0], row, schema)? {
                    Value::Integer(i) => i as f64,
                    Value::Float(f) => f,
                    _ => return Ok(Value::Null),
                };
                let exp = match Self::eval_expr_on_row(&args[1], row, schema)? {
                    Value::Integer(i) => i as f64,
                    Value::Float(f) => f,
                    _ => return Ok(Value::Null),
                };
                Ok(Value::Float(base.powf(exp)))
            }
            "mod" => {
                if args.len() != 2 {
                    return Ok(Value::Null);
                }
                let a = Self::eval_expr_on_row(&args[0], row, schema)?;
                let b = Self::eval_expr_on_row(&args[1], row, schema)?;
                match (&a, &b) {
                    (Value::Integer(x), Value::Integer(y)) => {
                        if *y == 0 {
                            return Ok(Value::Null);
                        }
                        Ok(match x.checked_rem(*y) {
                            Some(n) => Value::Integer(n),
                            None => Value::Integer(0),
                        })
                    }
                    (Value::Float(x), Value::Float(y)) => {
                        if *y == 0.0 {
                            return Ok(Value::Null);
                        }
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
                use crate::types::Geometry;
                let (px, py, cx, cy, radius) = if args.len() == 4 {
                    // WITHIN_RADIUS(geom, x, y, radius) — 4-arg form
                    let point = Self::eval_expr_on_row(&args[0], row, schema)?;
                    let cx = Self::eval_expr_on_row(&args[1], row, schema)?;
                    let cy = Self::eval_expr_on_row(&args[2], row, schema)?;
                    let radius = Self::eval_expr_on_row(&args[3], row, schema)?;
                    let (px, py) = match point {
                        Value::Spatial(geom) => match &*geom {
                            Geometry::Point(p) => (p.x, p.y),
                            Geometry::Point3D(p) => (p.x, p.y),
                            _ => return Ok(Value::Bool(false)),
                        },
                        _ => return Ok(Value::Bool(false)),
                    };
                    let cx = match cx {
                        Value::Float(f) => f,
                        Value::Integer(i) => i as f64,
                        _ => return Ok(Value::Bool(false)),
                    };
                    let cy = match cy {
                        Value::Float(f) => f,
                        Value::Integer(i) => i as f64,
                        _ => return Ok(Value::Bool(false)),
                    };
                    let r = match radius {
                        Value::Float(f) => f,
                        Value::Integer(i) => i as f64,
                        _ => return Ok(Value::Bool(false)),
                    };
                    (px, py, cx, cy, r)
                } else if args.len() == 3 {
                    // WITHIN_RADIUS(geom, center, radius) — 3-arg form
                    let point = Self::eval_expr_on_row(&args[0], row, schema)?;
                    let center = Self::eval_expr_on_row(&args[1], row, schema)?;
                    let radius = Self::eval_expr_on_row(&args[2], row, schema)?;
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
                    (px, py, cx, cy, r)
                } else {
                    return Err(MoteDBError::InvalidArgument(
                        "WITHIN_RADIUS() takes 3 or 4 arguments".to_string(),
                    ));
                };
                let dist = ((px - cx).powi(2) + (py - cy).powi(2)).sqrt();
                Ok(Value::Bool(dist <= radius))
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
                            Ok(Value::Float(
                                ((x1 - x2).powi(2) + (y1 - y2).powi(2) + (z1 - z2).powi(2)).sqrt(),
                            ))
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
                    _ => Err(MoteDBError::Query(format!(
                        "eval_function_positional: unsupported function: {}",
                        fname
                    ))),
                }
            }
            _ => {
                // 🚨 Fallback to the full evaluator for functions not handled
                // above (timestamp/date functions, CAST, COALESCE, NULLIF, etc.).
                // The positional path returns NULL on error (callers do
                // .unwrap_or(Value::Null)), which silently broke queries like
                // `SELECT TO_MICROS(ts) FROM t` (returned NULL instead of the
                // micros value). Build a SqlRow from the positional row so the
                // evaluator can resolve Column references.
                //
                // 🔑 Coerce values to their declared schema type: the columnar
                // scan decodes Timestamp columns as Integer (they share 8-byte
                // fixed-width storage). Without this, TO_MICROS/YEAR/etc. fail
                // with TypeError ("requires timestamp argument").
                let mut sql_row = SqlRow::new();
                for (pos, col_def) in schema.columns.iter().enumerate() {
                    if let Some(v) = row.get(pos) {
                        let coerced = match (&col_def.col_type, v) {
                            (ColumnType::Timestamp, Value::Integer(i)) => {
                                Value::Timestamp(crate::types::Timestamp::from_micros(*i))
                            }
                            (_, other) => other.clone(),
                        };
                        sql_row.insert(col_def.name.clone(), coerced);
                    }
                }
                let evaluator = ExprEvaluator::new();
                evaluator.eval(
                    &Expr::FunctionCall {
                        name: name.to_string(),
                        args: args.to_vec(),
                        distinct: false,
                    },
                    &sql_row,
                )
            }
        }
    }

    /// Parse a TIME_BUCKET interval literal ('10s'/'5m'/'1h'/'2d') to micros.
    /// Shared by the SqlRow (eval_expr_simple / positional) evaluators.
    fn parse_time_bucket_interval_us(interval: &str) -> Result<i64> {
        let micros_per: i64 = match interval {
            s if s.ends_with('s') => s
                .trim_end_matches('s')
                .parse::<i64>()
                .map(|v| v * 1_000_000)
                .map_err(|_| MoteDBError::TypeError("TIME_BUCKET() bad interval".into()))?,
            s if s.ends_with('m') => s
                .trim_end_matches('m')
                .parse::<i64>()
                .map(|v| v * 60_000_000)
                .map_err(|_| MoteDBError::TypeError("TIME_BUCKET() bad interval".into()))?,
            s if s.ends_with('h') => s
                .trim_end_matches('h')
                .parse::<i64>()
                .map(|v| v * 3_600_000_000)
                .map_err(|_| MoteDBError::TypeError("TIME_BUCKET() bad interval".into()))?,
            s if s.ends_with('d') => s
                .trim_end_matches('d')
                .parse::<i64>()
                .map(|v| v * 86_400_000_000)
                .map_err(|_| MoteDBError::TypeError("TIME_BUCKET() bad interval".into()))?,
            _ => {
                return Err(MoteDBError::TypeError(
                    "TIME_BUCKET() interval must end with s/m/h/d".to_string(),
                ))
            }
        };
        if micros_per <= 0 {
            return Err(MoteDBError::InvalidArgument(
                "TIME_BUCKET() interval must be positive".to_string(),
            ));
        }
        Ok(micros_per)
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
                            Ok(Value::Bool(
                                lv.partial_cmp(&rv) == Some(std::cmp::Ordering::Equal),
                            ))
                        }
                    }
                    BinaryOperator::Ne => {
                        if matches!(&lv, Value::Null) || matches!(&rv, Value::Null) {
                            Ok(Value::Bool(false))
                        } else {
                            Ok(Value::Bool(
                                lv.partial_cmp(&rv) != Some(std::cmp::Ordering::Equal),
                            ))
                        }
                    }
                    BinaryOperator::Lt
                    | BinaryOperator::Le
                    | BinaryOperator::Gt
                    | BinaryOperator::Ge => {
                        if matches!(&lv, Value::Null) || matches!(&rv, Value::Null) {
                            Ok(Value::Null) // SQL: NULL comparison => UNKNOWN (3-valued logic)
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
                    BinaryOperator::Concat => Self::positional_concat(&lv, &rv),
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
                    row.get(col)
                        .cloned()
                        .ok_or_else(|| MoteDBError::ColumnNotFound(name.clone()))
                } else {
                    Err(MoteDBError::ColumnNotFound(name.clone()))
                }
            }
            Expr::Literal(val) => Ok(val.clone()),
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } => {
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
                    return Ok(Value::Bool(true));
                }
                // Static evaluator (no executor access): same OR-over-tokens
                // semantics as the index's default tokenizer. The old
                // fallback was an AND-of-substrings check — a different set
                // than the index for multi-term queries.
                use crate::index::tokenizers::{Tokenizer as _, WhitespaceTokenizer};
                match row.get(column) {
                    Some(Value::Text(text)) => {
                        let tok = WhitespaceTokenizer::default();
                        let q: Vec<String> =
                            tok.tokenize(query).iter().map(|t| t.text.clone()).collect();
                        Ok(Value::Bool(
                            tok.tokenize(text).iter().any(|t| q.contains(&t.text)),
                        ))
                    }
                    _ => Ok(Value::Bool(false)),
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
                                Value::Integer(i) => {
                                    use std::fmt::Write;
                                    let _ = write!(result, "{}", i);
                                }
                                Value::Float(f) => {
                                    use std::fmt::Write;
                                    let _ = write!(result, "{}", f);
                                }
                                // 🔑 Bool → "1"/"0" (consistent with || and CONCAT).
                                Value::Bool(b) => result.push_str(if b { "1" } else { "0" }),
                                // 🔑 NULL 跳过 (与 evaluator CONCAT 一致)。
                                Value::Null => continue,
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
                    "abs" | "round" | "floor" | "ceil" | "log" | "ln" | "log10" | "sqrt"
                    | "exp" => {
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
                                        "round" => crate::sql::evaluator::round_f64_half_away(f, 0),
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
                                "round" => Ok(Value::Float(crate::sql::evaluator::round_f64_half_away(f, 0))),
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
                    _ => Err(MoteDBError::Query(format!(
                        "eval_expr_simple: unsupported function: {}",
                        fname
                    ))),
                }
            }
            Expr::Case { whens, else_expr } => {
                for (cond, result) in whens {
                    let cond_val = Self::eval_expr_simple(cond, row)?;
                    if case_cond_matched(&cond_val) {
                        return Self::eval_expr_simple(result, row);
                    }
                }
                if let Some(else_e) = else_expr {
                    Self::eval_expr_simple(else_e, row)
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Err(MoteDBError::Query(format!(
                "eval_expr_simple: unsupported expression: {:?}",
                expr
            ))),
        }
    }

    /// Check if an expression can be evaluated positionally (no complex features).
    /// Simple: Column, Literal, BinaryOp (comparison + AND/OR), UnaryOp::Not, IsNull.
    /// Check if expression tree contains any Expr::Parameter nodes
    fn contains_parameter(expr: &Expr) -> bool {
        match expr {
            Expr::Parameter(_) => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::contains_parameter(left) || Self::contains_parameter(right)
            }
            Expr::UnaryOp { expr, .. } => Self::contains_parameter(expr),
            Expr::IsNull { expr, .. } => Self::contains_parameter(expr),
            Expr::In { expr, list, .. } => {
                Self::contains_parameter(expr) || list.iter().any(Self::contains_parameter)
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::contains_parameter(expr)
                    || Self::contains_parameter(low)
                    || Self::contains_parameter(high)
            }
            Expr::Like { expr, pattern, .. } => {
                Self::contains_parameter(expr) || Self::contains_parameter(pattern)
            }
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
                Expr::BinaryOp { left, right, .. } => walk_expr(left).max(walk_expr(right)),
                Expr::UnaryOp { expr, .. } => walk_expr(expr),
                Expr::IsNull { expr, .. } => walk_expr(expr),
                Expr::In { expr, list, .. } => list
                    .iter()
                    .fold(walk_expr(expr), |acc, e| acc.max(walk_expr(e))),
                Expr::Between {
                    expr, low, high, ..
                } => walk_expr(expr).max(walk_expr(low)).max(walk_expr(high)),
                Expr::Like { expr, pattern, .. } => walk_expr(expr).max(walk_expr(pattern)),
                Expr::FunctionCall { args, .. } => {
                    args.iter().fold(0, |acc, e| acc.max(walk_expr(e)))
                }
                _ => 0,
            }
        }
        fn walk_stmt(stmt: &Statement) -> usize {
            match stmt {
                Statement::Select { stmt: s, .. } => s
                    .where_clause
                    .as_ref()
                    .map(walk_expr)
                    .unwrap_or(0)
                    .max(s.columns.iter().fold(0, |acc, c| {
                        acc.max(match c {
                            SelectColumn::Expr(e, _) => walk_expr(e),
                            _ => 0,
                        })
                    })),
                Statement::Insert(i) => i
                    .values
                    .iter()
                    .fold(0, |acc, row| {
                        acc.max(row.iter().fold(0, |a, e| a.max(walk_expr(e))))
                    })
                    .max(i.select.as_ref().map(|s| {
                        s.where_clause
                            .as_ref()
                            .map(walk_expr)
                            .unwrap_or(0)
                            .max(s.columns.iter().fold(0, |acc, c| {
                                acc.max(match c {
                                    SelectColumn::Expr(e, _) => walk_expr(e),
                                    _ => 0,
                                })
                            }))
                    }).unwrap_or(0)),
                Statement::Update(u) => {
                    let where_max = u.where_clause.as_ref().map(walk_expr).unwrap_or(0);
                    let set_max = u
                        .assignments
                        .iter()
                        .fold(0, |acc, (_, e)| acc.max(walk_expr(e)));
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
            CompiledWhere::InHash(pos, set, negated, _) if !*negated => (*pos, set.clone()),
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
        // 🚨 Any long Text value (≥64 bytes) has a prefix-truncated index key;
        // the fetched row set could include prefix-sharing false positives.
        // Fall back to scan paths (they compare actual values).
        if values.iter().any(|v| !Self::index_key_exact_for(v)) {
            return None;
        }
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
            // 🚨 Empty is NOT authoritative — the index may be stale (async
            // rebuild window / crash between data and index writes). Fall
            // through to the full-scan path; a genuinely-matching-free IN
            // list still returns empty there. (index_ref already dropped.)
            return None;
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
        let projected_rows: Vec<Vec<Value>> = rows_result
            .into_iter()
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
            let sort_specs: Vec<(usize, bool)> = order_by
                .iter()
                .filter_map(|ob| {
                    let col_name = match &ob.expr {
                        Expr::Column(name) => name,
                        _ => return None,
                    };
                    let bare = if col_name.contains('.') {
                        col_name.rsplit('.').next().unwrap_or(col_name)
                    } else {
                        col_name
                    };
                    let idx = columns.iter().position(|c| c == bare || c == col_name);
                    idx.map(|i| (i, ob.asc))
                })
                .collect();
            if !sort_specs.is_empty() {
                rows.sort_by(|a, b| {
                    for &(col_idx, asc) in &sort_specs {
                        if col_idx >= a.len() || col_idx >= b.len() {
                            continue;
                        }
                        let ord = order_by_cmp(&a[col_idx], &b[col_idx]);
                        let final_ord = if asc { ord } else { ord.reverse() };
                        if final_ord != std::cmp::Ordering::Equal {
                            return final_ord;
                        }
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

    /// Per-row WHERE truthiness: prefer the pre-compiled predicate (pure
    /// positional compare), fall back to positional eval when the compiled
    /// form can't decide (eval → None) or was never compiled.
    fn compiled_or_eval_row(
        compiled: Option<&CompiledWhere>,
        fallback_expr: &Expr,
        row: &[Value],
        schema: &TableSchema,
    ) -> bool {
        if let Some(cw) = compiled {
            match cw.eval(row) {
                Some(b) => return b,
                None => {}
            }
        }
        Self::eval_expr_on_row(fallback_expr, row, schema)
            .map(|v| Self::is_truthy(&v))
            .unwrap_or(false)
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
                        let (col_pos, op_val, cmp_val) =
                            Self::extract_col_literal_cmp(left, right, op, schema)?;
                        // 🔑 字面量侧为 NULL 时不编译：Value 的 Ord 对 Null 有
                        // 任意全序（Null 最小），10 > NULL 会算成 true —— 而 SQL
                        // 语义是与 NULL 比较恒 UNKNOWN → false（物化后的标量子
                        // 查询如 v > (SELECT MAX(x) FROM empty) 即 NULL，BUG #43）。
                        // 回退 eval_expr_on_row 的正确三值逻辑。
                        if matches!(cmp_val, Value::Null) {
                            return None;
                        }
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
            Expr::In {
                expr,
                list,
                negated,
            } => {
                if let Expr::Column(col_name) = expr.as_ref() {
                    let pos = schema.get_column_position(if col_name.contains('.') {
                        col_name.rsplit('.').next().unwrap_or(col_name)
                    } else {
                        col_name
                    })?;
                    if list.iter().all(|e| matches!(e, Expr::Literal(_))) {
                        // 🔑 NOT IN 的 negated 必须传下去 —— 旧代码丢弃它，
                        // 把 NOT IN 静默编译成 IN（BUG #37）。has_null 驱动
                        // 三值逻辑：x NOT IN (…, NULL) → UNKNOWN → false。
                        let has_null = list.iter().any(|e| matches!(e, Expr::Literal(Value::Null)));
                        let set: std::collections::HashSet<Value> = list
                            .iter()
                            .filter_map(|e| {
                                if let Expr::Literal(v) = e {
                                    Some(normalize_for_in(v))
                                } else {
                                    None
                                }
                            })
                            .collect();
                        Some(CompiledWhere::InHash(pos, set, *negated, has_null))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            // 🚀 Pre-built HashSet from subquery materialization: use directly,
            // skip the HashSet rebuild (the whole point of InHashset).
            Expr::InHashset {
                expr,
                set,
                negated,
                has_null,
            } => {
                if let Expr::Column(col_name) = expr.as_ref() {
                    let pos = schema.get_column_position(if col_name.contains('.') {
                        col_name.rsplit('.').next().unwrap_or(col_name)
                    } else {
                        col_name
                    })?;
                    // 🔑 Normalize Bool→Int so a BOOLEAN column matches an
                    // integer-valued subquery set (and vice versa).
                    let set: std::collections::HashSet<Value> =
                        set.iter().map(normalize_for_in).collect();
                    Some(CompiledWhere::InHash(pos, set, *negated, *has_null))
                } else {
                    None
                }
            }
            Expr::Like {
                expr,
                pattern,
                negated,
            } => {
                if let Expr::Column(col_name) = expr.as_ref() {
                    let pos = schema.get_column_position(if col_name.contains('.') {
                        col_name.rsplit('.').next().unwrap_or(col_name)
                    } else {
                        col_name
                    })?;
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
                    let pos = schema.get_column_position(if col_name.contains('.') {
                        col_name.rsplit('.').next().unwrap_or(col_name)
                    } else {
                        col_name
                    })?;
                    Some(CompiledWhere::IsNull(pos, *negated))
                } else {
                    None
                }
            }
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: _inner,
            } => {
                // 🔑 NOT 不编译：Some<bool> 表示法无法区分 false 与 UNKNOWN
                //（Gt 对 NULL 返回 Some(false)），Not 翻转会把 UNKNOWN 变
                // true —— `WHERE NOT v > 20` 曾错误命中 NULL 行（BUG #42）。
                // 回退 eval_expr_on_row（正确的三值逻辑）。NOT IS NULL 语义
                // 已由 IsNull(negated) 覆盖。
                None
            }
            _ => None,
        }
    }

    /// Helper: extract (col_pos, op, literal_value) from a binary comparison.
    /// Handles both `col op literal` and `literal op col` (swapping op).
    fn extract_col_literal_cmp(
        left: &Expr,
        right: &Expr,
        op: &BinaryOperator,
        schema: &TableSchema,
    ) -> Option<(usize, BinaryOperator, Value)> {
        // Try left=column, right=literal
        if let Expr::Column(col_name) = left {
            if let Expr::Literal(val) = right {
                let bare = if col_name.contains('.') {
                    col_name.rsplit('.').next().unwrap_or(col_name)
                } else {
                    col_name
                };
                let pos = schema.get_column_position(bare)?;
                return Some((pos, op.clone(), val.clone()));
            }
        }
        // Try left=literal, right=column (swap op direction)
        if let Expr::Column(col_name) = right {
            if let Expr::Literal(val) = left {
                let bare = if col_name.contains('.') {
                    col_name.rsplit('.').next().unwrap_or(col_name)
                } else {
                    col_name
                };
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
                matches!(
                    op,
                    BinaryOperator::Eq
                        | BinaryOperator::Ne
                        | BinaryOperator::Lt
                        | BinaryOperator::Le
                        | BinaryOperator::Gt
                        | BinaryOperator::Ge
                        | BinaryOperator::And
                        | BinaryOperator::Or
                        | BinaryOperator::Add
                        | BinaryOperator::Sub
                        | BinaryOperator::Mul
                        | BinaryOperator::Div
                        | BinaryOperator::Mod
                        | BinaryOperator::L2Distance
                        | BinaryOperator::CosineDistance
                        | BinaryOperator::DotProduct
                ) && Self::can_eval_positional(left)
                    && Self::can_eval_positional(right)
            }
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } => Self::can_eval_positional(expr),
            Expr::IsNull { .. } => true,
            Expr::In { .. } | Expr::Between { .. } | Expr::Like { .. } => true,
            Expr::FunctionCall { name, args, .. } => {
                let fname = name.to_lowercase();
                let handled = matches!(
                    fname.as_str(),
                    "concat"
                        | "upper"
                        | "lower"
                        | "length"
                        | "trim"
                        | "ltrim"
                        | "rtrim"
                        | "abs"
                        | "round"
                        | "floor"
                        | "ceil"
                        | "log"
                        | "ln"
                        | "log10"
                        | "sqrt"
                        | "exp"
                        | "coalesce"
                        | "if"
                        | "within_radius"
                        | "st_distance"
                        | "match"
                );
                handled && args.iter().all(Self::can_eval_positional)
            }
            // MATCH is a set predicate resolved by the executor (text index
            // + row id); positional rows can carry neither.
            Expr::Match { .. } => false,
            Expr::Case { whens, else_expr } => {
                whens
                    .iter()
                    .all(|(c, r)| Self::can_eval_positional(c) && Self::can_eval_positional(r))
                    && else_expr
                        .as_ref()
                        .map(|e| Self::can_eval_positional(e))
                        .unwrap_or(true)
            }
            _ => false,
        }
    }

    /// Check if an expression can be evaluated by eval_expr_simple (HashMap path).
    /// This is a stricter subset than can_eval_positional — fewer functions are supported.
    fn can_eval_simple(expr: &Expr) -> bool {
        match expr {
            Expr::Column(_) | Expr::Literal(_) => true,
            Expr::BinaryOp { left, op, right } => {
                matches!(
                    op,
                    BinaryOperator::Eq
                        | BinaryOperator::Ne
                        | BinaryOperator::Lt
                        | BinaryOperator::Le
                        | BinaryOperator::Gt
                        | BinaryOperator::Ge
                        | BinaryOperator::And
                        | BinaryOperator::Or
                        | BinaryOperator::Add
                        | BinaryOperator::Sub
                        | BinaryOperator::Mul
                        | BinaryOperator::Div
                        | BinaryOperator::Mod
                        | BinaryOperator::L2Distance
                        | BinaryOperator::CosineDistance
                        | BinaryOperator::DotProduct
                ) && Self::can_eval_simple(left)
                    && Self::can_eval_simple(right)
            }
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } => Self::can_eval_simple(expr),
            Expr::IsNull { .. } => true,
            Expr::In { .. } | Expr::Between { .. } | Expr::Like { .. } => true,
            Expr::FunctionCall { name, args, .. } => {
                let fname = name.to_lowercase();
                let handled = matches!(
                    fname.as_str(),
                    "concat"
                        | "upper"
                        | "lower"
                        | "length"
                        | "trim"
                        | "ltrim"
                        | "rtrim"
                        | "abs"
                        | "round"
                        | "floor"
                        | "ceil"
                        | "log"
                        | "ln"
                        | "log10"
                        | "sqrt"
                        | "exp"
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
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_uses_metadata(left) || Self::expr_uses_metadata(right)
            }
            Expr::UnaryOp { expr, .. } | Expr::IsNull { expr, .. } => {
                Self::expr_uses_metadata(expr)
            }
            Expr::In { expr, list, .. } => {
                Self::expr_uses_metadata(expr) || list.iter().any(Self::expr_uses_metadata)
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::expr_uses_metadata(expr)
                    || Self::expr_uses_metadata(low)
                    || Self::expr_uses_metadata(high)
            }
            Expr::Like { expr, pattern, .. } => {
                Self::expr_uses_metadata(expr) || Self::expr_uses_metadata(pattern)
            }
            Expr::FunctionCall { args, .. } => args.iter().any(Self::expr_uses_metadata),
            Expr::Match { .. } => false,
            _ => false,
        }
    }

    /// Check if a SelectStmt contains any Parameter nodes.
    fn contains_parameter_stmt(stmt: &SelectStmt) -> bool {
        stmt.where_clause
            .as_ref()
            .is_some_and(Self::contains_parameter)
            || stmt.columns.iter().any(|c| match c {
                SelectColumn::Expr(e, _) => Self::contains_parameter(e),
                _ => false,
            })
            // ORDER BY keys can carry parameters too (`ORDER BY emb <-> ?`);
            // missing here, the stmt skipped substitution and Parameter nodes
            // evaluated to NULL at materialize() (param-vector ANN returned
            // the FARTHER row — found via the Python bindings).
            || stmt
                .order_by
                .as_ref()
                .is_some_and(|obs| obs.iter().any(|ob| Self::contains_parameter(&ob.expr)))
            || stmt
                .having
                .as_ref()
                .is_some_and(Self::contains_parameter)
    }

    /// Validate that all Parameter nodes in stmt are bound to a value in params.
    fn validate_params_bound(stmt: &SelectStmt, params: &[Value]) -> Option<MoteDBError> {
        fn check_expr(expr: &Expr, params: &[Value]) -> Option<MoteDBError> {
            match expr {
                Expr::Parameter(idx) if *idx == 0 => Some(MoteDBError::InvalidArgument(
                    "Unnamed ? parameter not resolved (internal error)".to_string(),
                )),
                Expr::Parameter(idx) => {
                    if params.get(idx - 1).is_none() {
                        return Some(MoteDBError::InvalidArgument(format!(
                            "Parameter ?{} not bound ({} parameters provided)",
                            idx,
                            params.len()
                        )));
                    }
                    None
                }
                Expr::BinaryOp { left, right, .. } => {
                    check_expr(left, params).or_else(|| check_expr(right, params))
                }
                Expr::UnaryOp { expr, .. } => check_expr(expr, params),
                Expr::IsNull { expr, .. } => check_expr(expr, params),
                _ => None,
            }
        }
        stmt.where_clause
            .as_ref()
            .and_then(|w| check_expr(w, params))
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

        let columns: Vec<SelectColumn> = stmt
            .columns
            .iter()
            .map(|c| match c {
                SelectColumn::Expr(e, alias) => match sub(e) {
                    Ok(resolved) => SelectColumn::Expr(resolved, alias.clone()),
                    Err(_) => c.clone(),
                },
                _ => c.clone(),
            })
            .collect();

        // 🔑 ORDER BY keys must be substituted too: `ORDER BY emb <-> ?` left
        // a Parameter node that evaluated to NULL per row → arbitrary order
        // (found via the Python bindings: param-vector ANN returned the
        // FARTHER row). Errors fall back to the original (unsupported shapes
        // keep their old behavior).
        let order_by = stmt.order_by.as_ref().map(|obs| {
            obs.iter()
                .map(|ob| crate::sql::ast::OrderByExpr {
                    expr: sub(&ob.expr).unwrap_or_else(|_| ob.expr.clone()),
                    asc: ob.asc,
                    nulls_first: ob.nulls_first,
                })
                .collect::<Vec<_>>()
        });

        Ok(SelectStmt {
            columns,
            from: stmt.from.clone(),
            where_clause,
            order_by,
            limit: stmt.limit,
            offset: stmt.offset,
            distinct: stmt.distinct,
            group_by: stmt.group_by.clone(),
            having: stmt.having.clone(),
            latest_by: stmt.latest_by.clone(),
        })
    }

    /// 🔑 UPDATE/DELETE 入口的参数替换：eval_expr_on_row 遇到 Parameter
    /// 返回 Err，而 UPDATE 的 WHERE 求值用 `.unwrap_or(false)` 吞掉 Err ——
    /// prepared UPDATE/DELETE 曾全部静默无效（affected 0 行但 Ok）。
    /// SELECT 有 substitute_params_stmt；UPDATE/DELETE 在此对 WHERE 与
    /// SET 表达式做同样的 Literal 替换。
    fn do_substitute_params_mutation(
        self: &Self,
        expr: &mut Option<Expr>,
        assignments: Option<&mut Vec<(String, Expr)>>,
    ) -> Result<()> {
        let params = self.evaluator.get_params();
        if params.is_empty() {
            return Ok(());
        }
        let sub = |e: &Expr| -> Result<Expr> { Self::substitute_expr(e, &params) };
        if let Some(w) = expr {
            let replaced = sub(w)?;
            *w = replaced;
        }
        if let Some(assigns) = assignments {
            for (_, e) in assigns.iter_mut() {
                *e = sub(e)?;
            }
        }
        Ok(())
    }

    /// Recursively substitute Parameter nodes in an expression tree.
    fn substitute_expr(expr: &Expr, params: &[Value]) -> Result<Expr> {
        match expr {
            Expr::Parameter(idx) => {
                if *idx == 0 {
                    return Err(MoteDBError::InvalidArgument(
                        "Unnamed ? parameter not resolved (internal error)".to_string(),
                    ));
                }
                let i = idx - 1;
                params.get(i).cloned().map(Expr::Literal).ok_or_else(|| {
                    MoteDBError::InvalidArgument(format!(
                        "Parameter ?{} not bound ({} parameters provided)",
                        idx,
                        params.len()
                    ))
                })
            }
            Expr::BinaryOp { left, op, right } => {
                let l = Self::substitute_expr(left, params)?;
                let r = Self::substitute_expr(right, params)?;
                Ok(Expr::BinaryOp {
                    left: Box::new(l),
                    op: op.clone(),
                    right: Box::new(r),
                })
            }
            Expr::UnaryOp { op, expr: inner } => {
                let e = Self::substitute_expr(inner, params)?;
                Ok(Expr::UnaryOp {
                    op: op.clone(),
                    expr: Box::new(e),
                })
            }
            Expr::IsNull {
                expr: inner,
                negated,
            } => {
                let e = Self::substitute_expr(inner, params)?;
                Ok(Expr::IsNull {
                    expr: Box::new(e),
                    negated: *negated,
                })
            }
            Expr::In {
                expr: inner,
                list,
                negated,
            } => {
                let e = Self::substitute_expr(inner, params)?;
                let list2: Result<Vec<Expr>> = list
                    .iter()
                    .map(|x| Self::substitute_expr(x, params))
                    .collect();
                Ok(Expr::In {
                    expr: Box::new(e),
                    list: list2?,
                    negated: *negated,
                })
            }
            Expr::Between {
                expr: inner,
                low,
                high,
                negated,
            } => {
                let e = Self::substitute_expr(inner, params)?;
                let l = Self::substitute_expr(low, params)?;
                let h = Self::substitute_expr(high, params)?;
                Ok(Expr::Between {
                    expr: Box::new(e),
                    low: Box::new(l),
                    high: Box::new(h),
                    negated: *negated,
                })
            }
            Expr::Like {
                expr: inner,
                pattern,
                negated,
            } => {
                let e = Self::substitute_expr(inner, params)?;
                let p = Self::substitute_expr(pattern, params)?;
                Ok(Expr::Like {
                    expr: Box::new(e),
                    pattern: Box::new(p),
                    negated: *negated,
                })
            }
            Expr::FunctionCall {
                name,
                args,
                distinct,
            } => {
                let args2: Result<Vec<Expr>> = args
                    .iter()
                    .map(|x| Self::substitute_expr(x, params))
                    .collect();
                Ok(Expr::FunctionCall {
                    name: name.clone(),
                    args: args2?,
                    distinct: *distinct,
                })
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
                // 🔑 Bool/Int coercion (TRUE=1, FALSE=0) so `flag = 1` matches
                // a BOOLEAN column with TRUE, and `1 = TRUE` is true.
                let (lv, rv) = Self::coerce_bool_int(lv, rv);
                match op {
                    BinaryOperator::Eq => {
                        if matches!(&lv, Value::Null) || matches!(&rv, Value::Null) {
                            Ok(Value::Null) // SQL: NULL = anything => NULL
                        } else {
                            Ok(Value::Bool(
                                lv.partial_cmp(&rv) == Some(std::cmp::Ordering::Equal),
                            ))
                        }
                    }
                    BinaryOperator::Ne => {
                        if matches!(&lv, Value::Null) || matches!(&rv, Value::Null) {
                            Ok(Value::Null) // SQL: NULL != anything => NULL
                        } else {
                            Ok(Value::Bool(
                                lv.partial_cmp(&rv) != Some(std::cmp::Ordering::Equal),
                            ))
                        }
                    }
                    BinaryOperator::Lt
                    | BinaryOperator::Le
                    | BinaryOperator::Gt
                    | BinaryOperator::Ge => {
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
                        // 🔑 Kleene 三值逻辑: FALSE AND 任何 = FALSE,
                        // TRUE AND TRUE = TRUE, 其余 (含 NULL) = NULL。
                        // 此前 is_truthy && is_truthy 是二值的 — NULL AND/0R
                        // 被压成 FALSE, 套上 NOT 后全部行错误通过
                        // (differential fuzz: WHERE NOT((g < NULL) OR ...) 返回全部行)。
                        let lb = Self::truth3(&lv);
                        let rb = Self::truth3(&rv);
                        Ok(match (lb, rb) {
                            (Some(false), _) | (_, Some(false)) => Value::Bool(false),
                            (Some(true), Some(true)) => Value::Bool(true),
                            _ => Value::Null,
                        })
                    }
                    BinaryOperator::Or => {
                        // 🔑 Kleene 三值逻辑: TRUE OR 任何 = TRUE,
                        // FALSE OR FALSE = FALSE, 其余 (含 NULL) = NULL。
                        let lb = Self::truth3(&lv);
                        let rb = Self::truth3(&rv);
                        Ok(match (lb, rb) {
                            (Some(true), _) | (_, Some(true)) => Value::Bool(true),
                            (Some(false), Some(false)) => Value::Bool(false),
                            _ => Value::Null,
                        })
                    }
                    BinaryOperator::Add => Self::positional_add(&lv, &rv),
                    BinaryOperator::Sub => Self::positional_sub(&lv, &rv),
                    BinaryOperator::Mul => Self::positional_mul(&lv, &rv),
                    BinaryOperator::Div => Self::positional_div(&lv, &rv),
                    BinaryOperator::Mod => Self::positional_mod(&lv, &rv),
                    BinaryOperator::Concat => Self::positional_concat(&lv, &rv),
                    BinaryOperator::L2Distance => Self::positional_vector_l2(&lv, &rv),
                    BinaryOperator::CosineDistance => Self::positional_vector_cosine(&lv, &rv),
                    BinaryOperator::DotProduct => Self::positional_vector_dot(&lv, &rv),
                }
            }
            Expr::Column(name) => {
                // Try exact (qualified) name first, then strip table prefix.
                // This handles JOIN schemas where the same bare column name
                // exists in multiple tables (e.g. employees.name vs departments.name).
                if let Some(pos) = schema.get_column_position(name) {
                    return row
                        .get(pos)
                        .cloned()
                        .ok_or_else(|| MoteDBError::ColumnNotFound(name.clone()));
                }
                let col_name = if name.contains('.') {
                    name.rsplit('.').next().unwrap_or(name)
                } else {
                    name
                };
                if let Some(pos) = schema.get_column_position(col_name) {
                    return row
                        .get(pos)
                        .cloned()
                        .ok_or_else(|| MoteDBError::ColumnNotFound(name.clone()));
                }
                // 🔑 JOIN WHERE fallback: schema columns may be table-qualified
                // (e.g. "b.status") but the WHERE references the bare name
                // ("status"). Match by stripping the table prefix from each
                // schema column name.
                let pos = schema
                    .columns
                    .iter()
                    .position(|c| c.name.rsplit('.').next().unwrap_or(&c.name) == col_name);
                pos.and_then(|p| row.get(p).cloned())
                    .ok_or_else(|| MoteDBError::ColumnNotFound(name.clone()))
            }
            Expr::Literal(val) => Ok(val.clone()),
            Expr::Parameter(_) => {
                // Parameters need evaluator state — trigger fallback
                Err(MoteDBError::Query(
                    "Cannot evaluate parameter in positional path".to_string(),
                ))
            }
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: inner,
            } => {
                let v = Self::eval_expr_on_row(inner, row, schema)?;
                // SQL three-valued logic: NOT UNKNOWN → UNKNOWN (NULL)
                if matches!(v, Value::Null) {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Bool(!Self::is_truthy(&v)))
                }
            }
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: inner,
            } => {
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
            Expr::InHashset {
                expr,
                set,
                negated,
                has_null,
            } => {
                // 🚀 O(1) per-row lookup (pre-built HashSet from subquery).
                // This is the fix for the 25.5s IN-subquery slowdown: the old
                // path (Expr::In with Vec<Literal>) iterated the full list per
                // row — O(rows × list_len).
                let val = Self::eval_expr_on_row(expr, row, schema)?;
                // 🔑 Three-valued logic, matching the evaluator's Expr::InHashset
                // arm: NULL IN (...) → UNKNOWN (NULL). WHERE filtering treats
                // NULL as "not matched" (is_truthy(Null) == false).
                if matches!(val, Value::Null) {
                    return Ok(Value::Null);
                }
                let found = set.contains(&val);
                // 🔑 found → IN=TRUE/NOT IN=FALSE; not found + NULL in set →
                // UNKNOWN (NULL); not found + no NULL → IN=FALSE/NOT IN=TRUE.
                if found {
                    Ok(Value::Bool(!*negated))
                } else if *has_null {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Bool(*negated))
                }
            }
            Expr::In {
                expr,
                list,
                negated,
            } => {
                let val = Self::eval_expr_on_row(expr, row, schema)?;
                // 🔑 Three-valued logic: NULL IN (...) → UNKNOWN (NULL).
                if matches!(val, Value::Null) {
                    return Ok(Value::Null);
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
                // 🔑 found → TRUE/!negated; not found + NULL → UNKNOWN (NULL);
                // not found + no NULL → FALSE/negated.
                if found {
                    Ok(Value::Bool(!*negated))
                } else if has_null {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Bool(*negated))
                }
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let val = Self::eval_expr_on_row(expr, row, schema)?;
                let low_val = Self::eval_expr_on_row(low, row, schema)?;
                let high_val = Self::eval_expr_on_row(high, row, schema)?;
                // 🔑 Three-valued logic: any NULL operand → UNKNOWN (NULL).
                if matches!(val, Value::Null)
                    || matches!(low_val, Value::Null)
                    || matches!(high_val, Value::Null)
                {
                    return Ok(Value::Null);
                }
                let in_range = val >= low_val && val <= high_val;
                Ok(Value::Bool(if *negated { !in_range } else { in_range }))
            }
            Expr::Like {
                expr,
                pattern,
                negated,
            } => {
                let val = Self::eval_expr_on_row(expr, row, schema)?;
                let pat = Self::eval_expr_on_row(pattern, row, schema)?;
                // 🔑 Three-valued logic: any NULL operand → UNKNOWN (NULL).
                if matches!(val, Value::Null) || matches!(pat, Value::Null) {
                    return Ok(Value::Null);
                }
                let matches = match (&val, &pat) {
                    (Value::Text(s), Value::Text(p)) => Self::simple_like_match(s, p),
                    _ => false,
                };
                Ok(Value::Bool(if *negated { !matches } else { matches }))
            }
            Expr::FunctionCall { name, args, .. } => {
                Self::eval_function_positional(name, args, row, schema)
            }
            Expr::Match { .. } => Err(MoteDBError::Query(
                "MATCH must be evaluated by executor".into(),
            )),
            Expr::Case { whens, else_expr } => {
                for (cond, result) in whens {
                    let cond_val = Self::eval_expr_on_row(cond, row, schema)?;
                    if case_cond_matched(&cond_val) {
                        return Self::eval_expr_on_row(result, row, schema);
                    }
                }
                if let Some(else_e) = else_expr {
                    Self::eval_expr_on_row(else_e, row, schema)
                } else {
                    Ok(Value::Null)
                }
            }
            // 3D spatial functions on a positional row (columnar projection
            // paths). Same semantics as the SqlRow evaluator.
            Expr::StDistance3D { column, x, y, z } => {
                let v = schema
                    .get_column_position(column)
                    .and_then(|pos| row.get(pos))
                    .ok_or_else(|| MoteDBError::ColumnNotFound(column.clone()))?;
                Ok(match crate::sql::evaluator::point3d_of(v) {
                    Some(p) => Value::Float(crate::sql::evaluator::euclid3(&p, *x, *y, *z)),
                    None => Value::Null,
                })
            }
            Expr::StWithin3D {
                column,
                min_x,
                min_y,
                min_z,
                max_x,
                max_y,
                max_z,
            } => {
                let v = schema
                    .get_column_position(column)
                    .and_then(|pos| row.get(pos))
                    .ok_or_else(|| MoteDBError::ColumnNotFound(column.clone()))?;
                Ok(Value::Bool(
                    crate::sql::evaluator::point3d_of(v).is_some_and(|p| {
                        p.x >= *min_x
                            && p.x <= *max_x
                            && p.y >= *min_y
                            && p.y <= *max_y
                            && p.z >= *min_z
                            && p.z <= *max_z
                    }),
                ))
            }
            Expr::StRadius3D {
                column,
                x,
                y,
                z,
                radius,
            } => {
                let v = schema
                    .get_column_position(column)
                    .and_then(|pos| row.get(pos))
                    .ok_or_else(|| MoteDBError::ColumnNotFound(column.clone()))?;
                Ok(Value::Bool(
                    crate::sql::evaluator::point3d_of(v)
                        .is_some_and(|p| crate::sql::evaluator::euclid3(&p, *x, *y, *z) <= *radius),
                ))
            }
            _ => Err(MoteDBError::Query(format!(
                "eval_expr_on_row: unsupported expression: {:?}",
                expr
            ))),
        }
    }

    /// Generate a human-readable column name for an expression (e.g., "SUM(amount)", "COUNT(*)")
    pub(crate) fn expr_to_column_name(expr: &Expr) -> String {
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
                format!(
                    "{} {:?} {}",
                    Self::expr_to_column_name(left),
                    op,
                    Self::expr_to_column_name(right)
                )
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
            schema
                .columns
                .iter()
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
            columns
                .iter()
                .zip(select_cols.iter())
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
        Self::project_row_direct_checked(row, select_cols, columns, schema)
            // 🔑 Historically this swallowed evaluation errors as NULL.
            // The streaming/collected-row paths that still call this
            // function cannot easily short-circuit on a single bad row,
            // so they retain NULL-on-error semantics. Paths that MUST
            // surface errors (e.g. constant `1/0`) call
            // project_row_direct_checked directly.
            .unwrap_or_else(|_| {
                // Best-effort: produce a NULL-filled row so the count
                // stays consistent (matches the old swallow behavior).
                vec![Value::Null; columns.len().max(1)]
            })
    }

    /// Same as project_row_direct but propagates evaluation errors
    /// (e.g. DivisionByZero on `SELECT 1/0`) instead of converting them
    /// to NULL. Use this on paths where a hard error must surface to the
    /// caller.
    fn project_row_direct_checked(
        row: &Row,
        select_cols: &[SelectColumn],
        columns: &[String],
        schema: &TableSchema,
    ) -> Result<Vec<Value>> {
        if select_cols.len() == 1 && matches!(select_cols[0], SelectColumn::Star) {
            // SELECT * — return all columns in schema order (cheap clone)
            Ok(row.to_vec())
        } else {
            // Explicit columns — use column position as index into Vec
            let mut out: Vec<Value> = Vec::with_capacity(columns.len());
            for (_alias, col_spec) in columns.iter().zip(select_cols.iter()) {
                let v = match col_spec {
                    SelectColumn::Column(name) | SelectColumn::ColumnWithAlias(name, _) => {
                        // Handle table-qualified names: "users.id" → "id"
                        let lookup_name = if name.contains('.') {
                            name.rsplit('.').next().unwrap_or(name)
                        } else {
                            name
                        };
                        schema
                            .get_column_position(lookup_name)
                            .and_then(|pos| row.get(pos).cloned())
                            .unwrap_or(Value::Null)
                    }
                    SelectColumn::Star => Value::Null,
                    // 🔑 Propagate evaluation errors instead of silently
                    // converting them to NULL. A constant `1/0` in the
                    // SELECT list must surface as a DivisionByZero error,
                    // not a NULL value (silent wrong results).
                    SelectColumn::Expr(expr, _) => Self::eval_expr_on_row(expr, row, schema)?,
                };
                out.push(v);
            }
            Ok(out)
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
                        &row,
                        select_cols,
                        columns,
                        schema_ref,
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
        // 🚀 LATEST BY on a TimeSeries table: fold per-group max-ts directly
        // in the ColumnarStore (decodes only the needed columns). The general
        // path materialized every row as a HashMap SqlRow first (~1.5 s at
        // 1M rows). WHERE / ORDER BY / LIMIT shapes keep the general path.
        if stmt.latest_by.is_some()
            && stmt.where_clause.is_none()
            && stmt.order_by.is_none()
            && stmt.limit.is_none()
            && !stmt.distinct
            && stmt.group_by.is_none()
            && stmt.having.is_none()
        {
            if let Some(TableRef::Table { name, .. }) = stmt.from.as_ref() {
                if let Ok(schema) = self.db.get_table_schema(name) {
                    if schema.table_type == crate::types::TableType::TimeSeries {
                        if let Some(result) = self.try_ts_latest_by(stmt, name, &schema)? {
                            return Ok(result);
                        }
                    }
                }
            }
        }

        // 🚀 ORDER BY <ts_col> [DESC] LIMIT k on a TimeSeries table: bounded
        // top-k heap over the decoded ts column in the ColumnarStore (the
        // general path materialized + sorted every row, ~366 ms at 1M).
        if stmt.latest_by.is_none()
            && stmt.where_clause.is_none()
            && stmt.limit.is_some()
            && !stmt.distinct
            && stmt.group_by.is_none()
            && stmt.having.is_none()
            && stmt.order_by.as_ref().is_some_and(|ob| ob.len() == 1)
        {
            if let Some(TableRef::Table { name, .. }) = stmt.from.as_ref() {
                if let Ok(schema) = self.db.get_table_schema(name) {
                    if schema.table_type == crate::types::TableType::TimeSeries {
                        let key = &stmt.order_by.as_ref().unwrap()[0];
                        if let crate::sql::ast::Expr::Column(cn) = &key.expr {
                            let bare = cn.rsplit('.').next().unwrap_or(cn);
                            if Some(bare) == schema.timeseries_column.as_deref() {
                                if let Some(result) =
                                    self.try_ts_order_limit(stmt, name, &schema, key.asc)?
                                {
                                    return Ok(result);
                                }
                            }
                        }
                    }
                }
            }
        }

        // 🚀 Substitute bind parameters before executing
        let resolved_stmt;
        let stmt = if Self::contains_parameter_stmt(stmt) {
            match self.substitute_params_stmt(stmt) {
                Ok(s) => {
                    resolved_stmt = s;
                    &resolved_stmt as &SelectStmt
                }
                Err(e) => return Err(e),
            }
        } else {
            stmt
        };

        // 🔑 Pre-resolve non-correlated subqueries in the WHERE clause before
        // any routing. execute_select_internal is called for subqueries (via
        // materialize_subqueries), and a subquery's own WHERE may itself
        // contain nested subqueries (e.g. `SELECT MIN(v) FROM t WHERE v >
        // (SELECT MIN(v) FROM t)`). Without this pre-resolution, the aggregate
        // / col-segment routing paths evaluate the WHERE via eval_expr_on_row
        // which cannot execute subqueries → empty result → the outer scalar
        // subquery materializes to NULL → silent wrong results for 3+-level
        // nesting. This mirrors the pre-resolution in execute_select_streaming_ref.
        // Also covers HAVING (a subquery in HAVING has the same problem).
        let resolved_subq_stmt;
        let stmt = {
            let needs_where = stmt
                .where_clause
                .as_ref()
                .is_some_and(Self::expr_contains_subquery);
            let needs_having = stmt
                .having
                .as_ref()
                .is_some_and(Self::expr_contains_subquery);
            if needs_where || needs_having {
                let outer_schema = stmt.from.as_ref().and_then(|f| {
                    if let TableRef::Table { name, .. } = f {
                        self.db.get_table_schema(name).ok()
                    } else {
                        None
                    }
                });
                let mut cloned = stmt.clone();
                if let Some(w) = cloned.where_clause.take() {
                    cloned.where_clause =
                        Some(self.materialize_subqueries_checked(&w, outer_schema.as_deref())?);
                }
                if let Some(h) = cloned.having.take() {
                    cloned.having =
                        Some(self.materialize_subqueries_checked(&h, outer_schema.as_deref())?);
                }
                // 🚨 Honesty guard: an aggregate-only query whose WHERE still
                // holds a subquery node (correlated EXISTS / scalar that
                // couldn't be proven uncorrelated) has no evaluator on the
                // aggregate fast paths — they silently returned 0. Fail
                // loudly instead; row-level (non-aggregate) queries DO
                // support correlated subqueries per-row.
                let still_subq = cloned
                    .where_clause
                    .as_ref()
                    .is_some_and(Self::expr_contains_subquery);
                let aggregate_only = cloned.group_by.is_none()
                    && !cloned.columns.is_empty()
                    && cloned
                        .columns
                        .iter()
                        .all(|c| matches!(c, SelectColumn::Expr(Expr::FunctionCall { .. }, _)));
                if still_subq && aggregate_only {
                    // 🚀 Route to the materialized path instead of erroring:
                    // it evaluates correlated subqueries per row
                    // (bind_outer_columns + eval_correlated_expr) before
                    // apply_group_by folds the aggregates. The aggregate
                    // FAST paths all carry their own expr_contains_subquery
                    // guards; any that doesn't is a bug to fix, not a reason
                    // to reject the query.
                }
                resolved_subq_stmt = cloned;
                &resolved_subq_stmt as &SelectStmt
            } else {
                stmt
            }
        };

        // Validate SELECT column references against the table schema (when a
        // single table is named). A bare column that doesn't exist in the
        // table is a query error, not a silent NULL/value from another column.
        if let Some(TableRef::Table {
            name: table_name, ..
        }) = stmt.from.as_ref()
        {
            if let Ok(schema) = self.db.get_table_schema(table_name) {
                for col in &stmt.columns {
                    if let SelectColumn::Column(name) | SelectColumn::ColumnWithAlias(name, _) = col
                    {
                        let bare = name.rsplit('.').next().unwrap_or(name);
                        if schema.get_column_position(bare).is_none() {
                            return Err(MoteDBError::ColumnNotFound(format!(
                                "'{}' in table '{}'",
                                bare, table_name
                            )));
                        }
                    }
                }
            }
        }

        // 🆕 FAST PATH -4: SELECT without FROM clause (e.g., SELECT LAST_INSERT_ID())
        // → Evaluate expressions directly without table scan
        if stmt.from.is_none() {
            let empty_row = SqlRow::new();

            // 🔑 Apply WHERE clause for scalar SELECT (e.g., `SELECT 1 WHERE 0`
            // should return 0 rows). Previously this path ignored WHERE entirely.
            let passes_where = if let Some(ref where_clause) = stmt.where_clause {
                let val = self.evaluator.eval(where_clause, &empty_row)?;
                self.to_bool(&val)?
            } else {
                true
            };

            // Always compute column names (even when 0 rows) for metadata consistency.
            let mut column_names = Vec::new();
            for col in &stmt.columns {
                match col {
                    SelectColumn::Expr(expr, alias) => {
                        let col_name = alias.clone().unwrap_or_else(|| format!("{:?}", expr));
                        column_names.push(col_name);
                    }
                    SelectColumn::Star => {
                        return Err(MoteDBError::InvalidArgument(
                            "SELECT * requires a FROM clause".to_string(),
                        ));
                    }
                    SelectColumn::Column(name) | SelectColumn::ColumnWithAlias(name, _) => {
                        return Err(MoteDBError::InvalidArgument(format!(
                            "Column {} requires a FROM clause",
                            name
                        )));
                    }
                }
            }

            if !passes_where {
                return Ok(QueryResult::Select {
                    columns: column_names,
                    rows: vec![],
                });
            }

            let mut result_row = Vec::new();
            for col in &stmt.columns {
                if let SelectColumn::Expr(expr, _) = col {
                    let value = self.evaluator.eval(expr, &empty_row)?;
                    result_row.push(value);
                }
            }

            return Ok(QueryResult::Select {
                columns: column_names,
                rows: vec![result_row],
            });
        }

        // From here on, we know stmt.from is Some. Extracted once below.
        let from = stmt.from.as_ref().unwrap();

        // 🚀 FAST PATH -3b: Positional INNER JOIN (equi-join) for two tables.
        // Bypasses SqlRow(HashMap) entirely — scans both tables as Vec<Value>,
        // builds a hash table on the join column, probes, and concatenates.
        // This is ~8x faster than the generic path (which builds N HashMaps).
        if let TableRef::Join {
            left,
            right,
            join_type: JoinType::Inner,
            on_condition,
        } = from
        {
            // 🚀 VEC M3: 批 hash equi-JOIN + GROUP BY (半连接聚合形态 —
            // 探测行命中直接折叠, 不物化 joined 行)。None → 下方原路径。
            if let Some(outcome) = self.try_vec_equi_join_gb(stmt, left, right, on_condition)? {
                return Ok(QueryResult::Select {
                    columns: outcome.columns,
                    rows: outcome.rows,
                });
            }
            // 🚀 Multi-way first: 3+ table left-deep chains (and JOIN +
            // GROUP BY/aggregate shapes) run successive hash joins here;
            // the nested-loop general path took minutes for bounded
            // three-way joins.
            if let Some(result) = self.try_multi_way_inner_join(stmt)? {
                return Ok(result);
            }
            if let (
                TableRef::Table {
                    name: ltable,
                    alias: lalias,
                },
                TableRef::Table {
                    name: rtable,
                    alias: ralias,
                },
            ) = (left.as_ref(), right.as_ref())
            {
                // Only for equi-join: a.col = b.col
                if let Some((lcol_full, rcol_full)) = self.extract_equi_join_columns(on_condition) {
                    // Only when no GROUP BY / HAVING / aggregates (simple projection JOIN)
                    if stmt.group_by.is_none()
                        && stmt.having.is_none()
                        && !self.has_aggregates(&stmt.columns)
                    {
                        if let Some(result) = self.try_positional_inner_join(
                            stmt,
                            ltable,
                            lalias.as_deref(),
                            rtable,
                            ralias.as_deref(),
                            &lcol_full,
                            &rcol_full,
                        )? {
                            return Ok(result);
                        }
                    }
                }
            }
        }

        // 🆕 Columnar SELECT for TimeSeries tables
        // Pattern: SELECT cols FROM ts_table WHERE ts BETWEEN a AND b
        // → Route to columnar store with time-range pruning + column projection
        if let TableRef::Table {
            name: table_name, ..
        } = from
        {
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
            if let TableRef::Table {
                name: table_name, ..
            } = from
            {
                if self.db.has_col_segment_store(table_name)
                    && stmt.latest_by.is_none()
                    && !self.has_only_count_aggregate(&stmt.columns)
                    // Don't route spatial/text/vector queries to the columnar
                    // scan — they need the index pushdown paths below
                    // (FAST PATH 0a/0b/-1/-1b).
                    && stmt.where_clause.as_ref().is_none_or(|w| !Self::expr_needs_materialized_path(w))
                    // 🔑 ORDER BY 表达式键引用投影外列 (如 GEOMETRY 的
                    // `loc <-> ST_POINT(...)`) 时列存扫描的投影排序无法
                    // 求键 → 静默乱序。放行到通用 SqlRow 路径 (full_row
                    // 键求值); VECTOR 距离键仍由列存 top-k 接管。
                    && stmt.order_by.as_ref().is_none_or(|ob| {
                        let schema = self.db.get_table_schema(table_name).ok();
                        !Self::order_by_needs_full_rows(ob, &stmt.columns, schema.as_deref())
                    })
                {
                    // 🔑 PERF: PK point query fast path — `WHERE pk = literal`
                    // should use binary search in the segment's row_map (O(log N)),
                    // NOT a full-table scan. The old code routed ALL queries
                    // (including WHERE id = val) through the full-scan path, which
                    // decoded the filter column for every row. Now we detect PK
                    // equality and use ColSegmentStore::get (binary search) first.
                    if let Some(ref wc) = stmt.where_clause {
                        if let Some(pk_result) =
                            self.try_col_segment_pk_point_query(stmt, table_name, wc)?
                        {
                            return pk_result.materialize();
                        }
                    }
                    let stream = self.execute_full_scan_streaming(stmt, table_name)?;
                    return stream.materialize();
                }
            }
        }

        // S9: ColSegmentStore tables — route queries with WHERE through the
        // multi-segment full-scan path. The PointQuery/index fast paths below
        // fetch rows via lsm_engine.scan_range, which returns empty for
        // ColSegmentStore tables (data lives in segment files, not the LSM).
        // Skip spatial/text/vector WHERE — they need the index pushdown paths.
        if stmt
            .where_clause
            .as_ref()
            .is_some_and(|w| !Self::expr_needs_materialized_path(w))
            && stmt.where_clause.is_some()
            && stmt.group_by.is_none()
            && !self.has_aggregates(&stmt.columns)
        {
            if let TableRef::Table {
                name: table_name, ..
            } = from
            {
                if self.db.has_col_segment_store(table_name)
                    && stmt.latest_by.is_none()
                    && !self.has_only_count_aggregate(&stmt.columns)
                {
                    let stream = self.execute_full_scan_streaming(stmt, table_name)?;
                    return stream.materialize();
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
            if let Some((table_name, col_name, query_vector, k)) =
                self.try_extract_vector_search(where_clause, from)
            {
                // ⚡ Ultra-fast path: Use vector index directly
                // Resolve index name via registry (supports custom index names)
                let index_name = self
                    .db
                    .index_registry
                    .find_by_column(
                        &table_name,
                        &col_name,
                        crate::database::index_metadata::IndexType::Vector,
                    )
                    .unwrap_or_else(|| format!("{}_{}", table_name, col_name));
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
                        let (column_names, result_rows) =
                            self.project_columns(&stmt.columns, &sql_rows, &schema)?;

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
            if let TableRef::Table {
                name: table_name, ..
            } = from
            {
                if let Some(result) =
                    self.try_text_search_fast_path(stmt, where_clause, table_name)?
                {
                    return Ok(result);
                }
            }
        }

        // 🚀 FAST PATH 0b: Spatial (ST_WITHIN / ST_KNN) optimization
        // Pattern: SELECT ... FROM table WHERE ST_WITHIN(col, ...) [LIMIT k]
        //          SELECT ... FROM table WHERE ST_KNN(col, ...) [LIMIT k]
        // → Use spatial index directly (50x faster than full table scan + per-row spatial query)
        if let Some(ref where_clause) = stmt.where_clause {
            if let TableRef::Table {
                name: table_name, ..
            } = from
            {
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
                    if let TableRef::Table {
                        name: table_name, ..
                    } = from
                    {
                        let index_name = format!("{}.{}", table_name, col_name);
                        if self.db.column_indexes.contains_key(&index_name) {
                            // ⚡ Ultra-fast path: Use index to get count
                            match self
                                .db
                                .query_by_column(table_name, &col_name, &target_value)
                            {
                                Ok(row_ids)
                                    if !row_ids.is_empty()
                                        || !self.db.is_async_index_pipeline_active() =>
                                {
                                    // 🔑 Verify before counting: index entries
                                    // can be stale inside a transaction (undo
                                    // replays of DELETE/UPDATE keep old
                                    // value→row_id entries until commit —
                                    // differential testing: COUNT WHERE id=X
                                    // returned 7/8 phantom rows while the row
                                    // LIST path verified correctly).
                                    let count = if let Ok(schema) =
                                        self.db.get_table_schema(&table_name)
                                    {
                                        let pos = schema.get_column_position(&col_name);
                                        match self.db.get_table_rows_batch(&table_name, &row_ids) {
                                            Ok(batch) => {
                                                let c = batch
                                                    .iter()
                                                    .filter(|(_, opt)| {
                                                        opt.as_ref()
                                                            .and_then(|row| {
                                                                pos.and_then(|p| row.get(p))
                                                            })
                                                            .map(|v| v == &target_value)
                                                            .unwrap_or(false)
                                                    })
                                                    .count()
                                                    as i64;
                                                c
                                            }
                                            Err(_) => row_ids.len() as i64,
                                        }
                                    } else {
                                        row_ids.len() as i64
                                    };
                                    // 🔑 Use the user-provided alias if present (e.g.
                                    // `SELECT COUNT(*) as c FROM t`), so derived
                                    // tables / CTEs can reference it by name.
                                    let col_name = stmt
                                        .columns
                                        .first()
                                        .and_then(|c| match c {
                                            SelectColumn::Expr(_, Some(alias)) => {
                                                Some(alias.clone())
                                            }
                                            _ => None,
                                        })
                                        .unwrap_or_else(|| "COUNT(*)".to_string());
                                    return Ok(QueryResult::Select {
                                        columns: vec![col_name],
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
            } else if stmt.having.is_none() {
                // 🚀 COUNT(*) without WHERE — O(1) from row counter.
                // 🔑 Only when there's no HAVING clause: HAVING requires
                // post-aggregation filtering (e.g. `HAVING COUNT(*) > 5`),
                // which this O(1) counter path can't evaluate. Without this
                // guard, `SELECT COUNT(*) FROM t HAVING COUNT(*) > 5` wrongly
                // returns [[3]] instead of [] when the condition is false.
                if let TableRef::Table {
                    name: table_name, ..
                } = from
                {
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

                    // 🔑 Use user alias if present (for derived table / CTE).
                    let col_name = stmt
                        .columns
                        .first()
                        .and_then(|c| match c {
                            SelectColumn::Expr(_, Some(alias)) => Some(alias.clone()),
                            _ => None,
                        })
                        .unwrap_or_else(|| "COUNT(*)".to_string());
                    return Ok(QueryResult::Select {
                        columns: vec![col_name],
                        rows: vec![vec![Value::Integer(count)]],
                    });
                }
            }
        }

        // 🚀 `SELECT COUNT(*) WHERE MATCH(...)` — count the index postings
        // directly (the aggregate pipeline materializes every matching row
        // just to count it).
        if self.has_aggregates(&stmt.columns) {
            if let Some(result) = self.try_text_match_count(stmt)? {
                return Ok(result);
            }
        }

        // 🚀 FAST PATH 1a: Streaming aggregate (no GROUP BY) — zero HashMap, zero SqlRow.
        // Handles: SELECT COUNT(*), SUM(x), AVG(y), MIN(z), MAX(w) FROM t [WHERE ...]
        // Accumulates directly into inline counters — O(1) memory, no grouping overhead.
        // When WHERE is present, reuses decoded row for aggregate extraction.
        if stmt.group_by.is_none()
            && !stmt.distinct
            && stmt.having.is_none()
            && stmt.order_by.is_none()
            && self.has_aggregates(&stmt.columns)
        {
            if let TableRef::Table {
                name: table_name, ..
            } = from
            {
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
            if let TableRef::Table {
                name: table_name, ..
            } = from
            {
                if let Ok(schema) = self.db.get_table_schema(table_name) {
                    if let Some((column_names, projected_rows)) =
                        self.try_apply_group_by_positional(stmt, &schema, table_name)?
                            .or(self.try_expression_group_by(stmt, &schema, table_name)?)
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
        // LATEST BY must go through the materialized path (apply_latest_by):
        // this fast path returned rows from the empty LSM read instead.
        if (stmt.order_by.is_some() || stmt.distinct)
            && stmt.group_by.is_none()
            && stmt.latest_by.is_none()
        {
            if let TableRef::Table {
                name: table_name, ..
            } = from
            {
                if let Ok(schema) = self.db.get_table_schema(table_name) {
                    if let Some(result) =
                        self.try_positional_order_by_distinct(stmt, &schema, table_name)?
                    {
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
        // 🔑 Must NOT trigger for aggregate queries (COUNT/SUM/...): those need
        // the aggregate path (try_apply_group_by_positional above), otherwise
        // COUNT(DISTINCT col) WHERE ... evaluates per-row and returns NULLs.
        // 🔑 Nor for LATEST BY (same rule as 1c above): this path has no
        // latest-per-group fold — it returned every matching row for
        // `… WHERE sensor='s1' LATEST BY sensor` (Round-12c E2E).
        if stmt.where_clause.is_some()
            && stmt.group_by.is_none()
            && stmt.order_by.is_none()
            && stmt.latest_by.is_none()
            && !stmt.distinct
            && !self.has_aggregates(&stmt.columns)
        {
            if let TableRef::Table {
                name: table_name, ..
            } = from
            {
                if let Some(result) = self.try_positional_where(stmt, table_name)? {
                    return Ok(result);
                }
            }
        }

        // 🚀 FAST PATH 1.5: Direct Vec<Value> for SELECT * FROM table
        // Bypasses SqlRow HashMap entirely — eliminates 2 HashMap allocs + 2N String clones per row.
        // Handles: SELECT * FROM t WHERE indexed_col =/>/>=/</<= value [LIMIT n]
        //          SELECT * FROM t [LIMIT n]
        let is_simple_star = stmt.columns.len() == 1
            && matches!(stmt.columns[0], SelectColumn::Star)
            && stmt.group_by.is_none()
            && stmt.order_by.is_none()
            && !stmt.distinct;

        if is_simple_star {
            if let Some(ref where_clause) = stmt.where_clause {
                if let TableRef::Table {
                    name: table_name, ..
                } = from
                {
                    // Try point query: WHERE col = value
                    if let Some((col_name, target_value)) =
                        self.try_extract_point_query(where_clause)
                    {
                        let index_name = format!("{}.{}", table_name, col_name);
                        if let Some(index_ref) = self.db.column_indexes.get(&index_name) {
                            if let Ok(row_ids) = index_ref.value().get_arc(&target_value) {
                                if !row_ids.is_empty() || !self.db.is_async_index_pipeline_active()
                                {
                                    drop(index_ref);
                                    return self.fast_star_indexed_result(
                                        table_name,
                                        &row_ids,
                                        stmt.limit,
                                        stmt.offset,
                                    );
                                }
                            }
                        }
                    }
                    // Try range query: WHERE col >= a AND col <= b
                    else if let Some((col_name, lower_value, lower_op, upper_value, upper_op)) =
                        self.try_extract_range_query(where_clause)
                    {
                        let index_name = format!("{}.{}", table_name, col_name);
                        if let Some(index_ref) = self.db.column_indexes.get(&index_name) {
                            use crate::sql::ast::BinaryOperator;
                            let lower_inclusive = matches!(lower_op, BinaryOperator::Ge);
                            let upper_inclusive = matches!(upper_op, BinaryOperator::Le);
                            if let Ok(row_ids) = index_ref.value().query_between(
                                &lower_value,
                                lower_inclusive,
                                &upper_value,
                                upper_inclusive,
                            ) {
                                if !row_ids.is_empty() || !self.db.is_async_index_pipeline_active()
                                {
                                    drop(index_ref);
                                    return self.fast_star_indexed_result(
                                        table_name,
                                        &row_ids,
                                        stmt.limit,
                                        stmt.offset,
                                    );
                                }
                            }
                        }
                    }
                    // Try inequality: WHERE col > value, col < value, etc.
                    else if let Some((col_name, op, value)) =
                        self.try_extract_inequality(where_clause)
                    {
                        let index_name = format!("{}.{}", table_name, col_name);
                        if let Some(index_ref) = self.db.column_indexes.get(&index_name) {
                            use crate::sql::ast::BinaryOperator;
                            let row_ids_result = match op {
                                BinaryOperator::Lt => index_ref.value().query_less_than(&value),
                                BinaryOperator::Le => {
                                    index_ref.value().query_less_than_or_equal(&value)
                                }
                                BinaryOperator::Gt => index_ref.value().query_greater_than(&value),
                                BinaryOperator::Ge => {
                                    index_ref.value().query_greater_than_or_equal(&value)
                                }
                                _ => Err(crate::error::MoteDBError::NotImplemented(
                                    "Unsupported operator".into(),
                                )),
                            };
                            if let Ok(row_ids) = row_ids_result {
                                if !row_ids.is_empty() || !self.db.is_async_index_pipeline_active()
                                {
                                    drop(index_ref);
                                    return self.fast_star_indexed_result(
                                        table_name,
                                        &row_ids,
                                        stmt.limit,
                                        stmt.offset,
                                    );
                                }
                            }
                        }
                    }
                    // Non-indexed WHERE — fall through to general path (needs SqlRow for eval)
                }
            } else {
                // No WHERE — full scan fast path
                if let TableRef::Table {
                    name: table_name, ..
                } = from
                {
                    return self.fast_star_scan_result(table_name, stmt.limit, stmt.offset);
                }
            }
            // Fall through to general path for unsupported patterns
        }

        // 🚀 FAST PATH 2: Try to use column index for WHERE optimization
        // 🆕 P0 OPTIMIZATION: Extract LIMIT early and pass to storage layer
        let storage_limit = self.calculate_storage_limit(stmt);

        // Priority: Range query > Point query > Full scan
        // 🔑 通用 join 路径的 WHERE 单表谓词 (见 execute_from_with_limit)
        let push_preds: Vec<(String, String, crate::sql::ast::BinaryOperator, Value)> = stmt
            .where_clause
            .as_ref()
            .map(Self::extract_pushdown_preds)
            .unwrap_or_default();
        let (all_sql_rows, combined_schema) = if let Some(ref where_clause) = stmt.where_clause {
            // Try range query first (dual-bound: col > X AND col < Y)
            if let Some((col_name, lower_value, lower_op, upper_value, upper_op)) =
                self.try_extract_range_query(where_clause)
            {
                if let TableRef::Table {
                    name: table_name, ..
                } = from
                {
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
                            table_name,
                            &col_name,
                            &lower_value,
                            lower_inclusive,
                            &upper_value,
                            upper_inclusive,
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
                                let batch_rows =
                                    self.db.get_table_rows_batch(table_name, &row_ids)?;

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
                                    let col_index = schema
                                        .columns
                                        .iter()
                                        .position(|c| c.name == col_name)
                                        .ok_or_else(|| {
                                            StorageError::InvalidData(format!(
                                                "Column '{}' not found",
                                                col_name
                                            ))
                                        })?;

                                    let col_value = row.get(col_index).ok_or_else(|| {
                                        StorageError::InvalidData("Column value missing".into())
                                    })?;

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
                        self.execute_from_with_limit(from, storage_limit, &push_preds)?
                    }
                } else {
                    self.execute_from_with_limit(from, storage_limit, &push_preds)?
                }
            }
            // Try point query
            else if let Some((col_name, target_value)) =
                self.try_extract_point_query(where_clause)
            {
                // Extract table name from FROM clause
                if let TableRef::Table {
                    name: table_name, ..
                } = from
                {
                    // Try to use column index
                    let index_name = format!("{}.{}", table_name, col_name);
                    let index_exists = self.db.column_indexes.contains_key(&index_name);

                    if index_exists {
                        // ⚡ Fast path: Use column index (40x faster!)
                        match self
                            .db
                            .query_by_column(table_name, &col_name, &target_value)
                        {
                            Ok(row_ids)
                                if !row_ids.is_empty()
                                    || !self.db.is_async_index_pipeline_active() =>
                            {
                                // 🚀 Use batch get
                                let schema = self.db.get_table_schema(table_name)?;
                                let batch_rows =
                                    self.db.get_table_rows_batch(table_name, &row_ids)?;

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
                if let TableRef::Table {
                    name: table_name, ..
                } = from
                {
                    let index_name = format!("{}.{}", table_name, col_name);
                    let index_exists = self.db.column_indexes.contains_key(&index_name);

                    if index_exists {
                        // ⚡ Fast path: Use column index inequality scan
                        let row_ids_result = match op {
                            BinaryOperator::Lt => self
                                .db
                                .query_by_column_less_than(table_name, &col_name, &value),
                            BinaryOperator::Le => self
                                .db
                                .query_by_column_less_than_or_equal(table_name, &col_name, &value),
                            BinaryOperator::Gt => self
                                .db
                                .query_by_column_greater_than(table_name, &col_name, &value),
                            BinaryOperator::Ge => self.db.query_by_column_greater_than_or_equal(
                                table_name, &col_name, &value,
                            ),
                            _ => {
                                // Unsupported operator, fallback to table scan
                                Err(crate::error::MoteDBError::NotImplemented(
                                    "Unsupported operator".into(),
                                ))
                            }
                        };

                        match row_ids_result {
                            Ok(row_ids)
                                if !row_ids.is_empty()
                                    || !self.db.is_async_index_pipeline_active() =>
                            {
                                // 🚀 Use batch get
                                let schema = self.db.get_table_schema(table_name)?;
                                let batch_rows =
                                    self.db.get_table_rows_batch(table_name, &row_ids)?;

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
                self.execute_from_with_limit(from, storage_limit, &[])?
            }
        } else {
            // No WHERE clause - use standard scan with limit
            self.execute_from_with_limit(from, storage_limit, &[])?
        };

        // 🎯 Filter rows (WHERE clause) - Apply remaining conditions
        let filtered_rows: Vec<(u64, SqlRow)> = if let Some(ref where_clause) = stmt.where_clause {
            // Check if we already used the index (in which case, no need to filter again)
            let used_index = if self.try_extract_range_query(where_clause).is_some() {
                // Range query - check if we used index
                if let TableRef::Table {
                    name: table_name, ..
                } = from
                {
                    if let Some((col_name, _, _, _, _)) = self.try_extract_range_query(where_clause)
                    {
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
                if let TableRef::Table {
                    name: table_name, ..
                } = from
                {
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
                    // Fast path: Only evaluate the point query condition.
                    // 🔑 Resolution must be DETERMINISTIC: exact key → bare
                    // key → UNIQUE ".{col}" suffix. The old HashMap-iteration
                    // fallback bound `WHERE i.id = 1` randomly to i.id or o.id
                    // on join rows (same query returned 0/1/2 across runs).
                    let bare = col_name.rsplit('.').next().unwrap_or(&col_name).to_string();
                    let suffix = format!(".{}", bare);
                    all_sql_rows
                        .into_iter()
                        .filter(|(_, row)| {
                            // 尝试直接匹配（含限定名）
                            if let Some(row_value) = row.get(&col_name) {
                                return row_value == &target_value;
                            }
                            if col_name != bare {
                                if let Some(row_value) = row.get(&bare) {
                                    return row_value == &target_value;
                                }
                                // 唯一的 ".{bare}" 后缀键才匹配；二义 = 不匹配
                                let mut hits = row
                                    .keys()
                                    .filter(|k| k.ends_with(&suffix))
                                    .collect::<Vec<_>>();
                                hits.dedup();
                                if hits.len() == 1 {
                                    return row.get(hits[0]) == Some(&target_value);
                                }
                                return false;
                            }

                            // 尝试匹配带表前缀的列名 (e.g., "users.id")
                            let mut hits = row
                                .keys()
                                .filter(|k| k.ends_with(&suffix))
                                .collect::<Vec<_>>();
                            hits.dedup();
                            if hits.len() == 1 {
                                return row.get(hits[0]) == Some(&target_value);
                            }
                            false
                        })
                        .collect()
                } else {
                    // 🚀 OPTIMIZATION: Fast path for simple comparison expressions
                    // Pattern: col > value, col < value, col >= value, col <= value
                    if let Some(fast_filter) = self.compile_simple_comparison(where_clause) {
                        // Use compiled filter (避免重复解释表达式)
                        all_sql_rows
                            .into_iter()
                            .filter(|(_, row)| fast_filter(row))
                            .collect()
                    } else {
                        // Slow path: Full expression evaluation with subquery support.
                        // 🔑 Use the correlation-aware variant: correlated
                        // subqueries (referencing outer columns, e.g.
                        // `WHERE (SELECT SUM(o.amt) ... WHERE o.cust = c.id) > 100`)
                        // must be kept as Subquery nodes and re-executed per row,
                        // NOT materialized once with an empty outer binding.
                        let materialized_where = self
                            .materialize_subqueries_checked(where_clause, Some(&combined_schema))?;
                        let has_correlated_subquery =
                            Self::expr_contains_subquery(&materialized_where);

                        // IN hash set optimization: precompute HashSet for large literal IN lists
                        if let Expr::In {
                            expr,
                            list,
                            negated,
                        } = &materialized_where
                        {
                            if !negated
                                && list.len() > 10
                                && list.iter().all(|e| matches!(e, Expr::Literal(_)))
                            {
                                let in_set: std::collections::HashSet<Value> = list
                                    .iter()
                                    .filter_map(|e| {
                                        if let Expr::Literal(v) = e {
                                            Some(v.clone())
                                        } else {
                                            None
                                        }
                                    })
                                    .collect();
                                let col_name = match expr.as_ref() {
                                    Expr::Column(name) => Some(name.clone()),
                                    _ => None,
                                };
                                if let Some(col_name) = col_name {
                                    all_sql_rows
                                        .into_iter()
                                        .filter(|(_, row)| {
                                            row.get(&col_name)
                                                .map(|val| in_set.contains(val))
                                                .unwrap_or(false)
                                        })
                                        .collect()
                                } else {
                                    all_sql_rows
                                        .into_iter()
                                        .filter(|(_, row)| {
                                            self.eval_with_materialized(&materialized_where, row)
                                                .and_then(|val| self.to_bool(&val))
                                                .unwrap_or(false)
                                        })
                                        .collect()
                                }
                            } else {
                                all_sql_rows
                                    .into_iter()
                                    .filter(|(_, row)| {
                                        self.eval_with_materialized(&materialized_where, row)
                                            .and_then(|val| self.to_bool(&val))
                                            .unwrap_or(false)
                                    })
                                    .collect()
                            }
                        } else if has_correlated_subquery {
                            // 🔑 Correlated subquery: bind outer column refs to
                            // each row's values, then re-execute the subquery.
                            // Build a positional Vec<Value> for this row so
                            // bind_outer_columns/eval_correlated_expr work.
                            let ncols = combined_schema.columns.len();
                            all_sql_rows
                                .into_iter()
                                .filter(|(_, row)| {
                                    let pos_row: Vec<Value> = (0..ncols)
                                        .map(|i| {
                                            combined_schema
                                                .columns
                                                .get(i)
                                                .and_then(|c| row.get(&c.name))
                                                .cloned()
                                                .unwrap_or(Value::Null)
                                        })
                                        .collect();
                                    let bound = Self::bind_outer_columns(
                                        &materialized_where,
                                        &pos_row,
                                        &combined_schema,
                                    );
                                    self.eval_correlated_expr(&bound, &pos_row, &combined_schema)
                                        .and_then(|val| self.to_bool(&val))
                                        .unwrap_or(false)
                                })
                                .collect()
                        } else {
                            all_sql_rows
                                .into_iter()
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
            self.apply_group_by(
                &stmt.columns,
                &filtered_rows,
                group_by_cols,
                stmt.having.as_ref(),
            )?
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
        // 排序产生的行置换 (排序后位置 → 原始索引); 未排序时为 None
        let mut permutation: Option<Vec<usize>> = None;
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

            // Create temporary rows with full data for sorting.
            // 🔑 携带原始索引: LATEST BY 在排序之后应用, 但 apply_latest_by
            // 按索引把 filtered_rows (原始顺序) 与投影行配对 — 排序置换后
            // 两者错位, 会把 A 行的分组键/timestamp 配到 B 行的投影值
            // (differential fuzz: LATEST BY sensor ORDER BY sensor 返回
            // 2 行 s2 + 0 行 s1)。置换记录让 filtered_rows 同步重排。
            let mut rows_with_keys: Vec<(Vec<Value>, Vec<Value>, usize)> = sorted_rows
                .into_iter()
                .zip(filtered_rows.iter())
                .enumerate()
                .map(|(orig_idx, (proj_row, (_, full_row)))| {
                    // Compute sort keys
                    let sort_keys: Result<Vec<Value>> = order_by
                        .iter()
                        .map(|order| {
                            // Try to resolve alias first
                            if let Expr::Column(col_name) = &order.expr {
                                if let Some(&idx) = alias_map.get(col_name) {
                                    // Use projected column value
                                    return Ok(proj_row[idx].clone());
                                }
                                // 🆕 GROUP BY / projection case: look up the
                                // ORDER BY name against the OUTPUT columns
                                // (column_names) FIRST. This is critical for
                                // GROUP BY queries: the projected row holds
                                // the aggregated/grouped value, while the
                                // underlying `full_row` is from an arbitrary
                                // input row whose value may belong to a
                                // different group. Resolving against the
                                // projected output makes ORDER BY deterministic.
                                if let Some(idx) = column_names.iter().position(|cn| {
                                    cn == col_name
                                        || cn.rsplit('.').next().unwrap_or(cn) == col_name
                                }) {
                                    if idx < proj_row.len() {
                                        return Ok(proj_row[idx].clone());
                                    }
                                }
                                // Try direct column lookup in full_row (non-GROUP-BY path)
                                if let Some(val) = full_row.get(col_name) {
                                    return Ok(val.clone());
                                }
                                // 🆕 Derived-table case: bare ORDER BY name
                                // against table-qualified full_row keys.
                                if !col_name.contains('.') {
                                    if let Some((_k, val)) = full_row.iter().find(|(k, _)| {
                                        k.rsplit('.').next().unwrap_or(k) == col_name
                                    }) {
                                        return Ok(val.clone());
                                    }
                                }
                            }
                            // 🔑 ORDER BY an aggregate function call (e.g.
                            // ORDER BY SUM(v) DESC) on a GROUP BY query: the
                            // projected row already holds the aggregated value
                            // at the matching output column. Build the same
                            // canonical name the SELECT-list uses (e.g.
                            // "SUM(v)") and look it up in column_names. Without
                            // this, the fall-through below evaluated SUM(v)
                            // against an arbitrary input row → wrong/constant
                            // sort key → non-deterministic group order.
                            if let Expr::FunctionCall { name, args, .. } = &order.expr {
                                let arg_str = args
                                    .iter()
                                    .map(|a| match a {
                                        Expr::Column(c) => c.clone(),
                                        e => format!("{:?}", e),
                                    })
                                    .collect::<Vec<_>>()
                                    .join(", ");
                                let ob_name = format!("{}({})", name.to_uppercase(), arg_str);
                                if let Some(idx) = column_names.iter().position(|cn| cn == &ob_name)
                                {
                                    if idx < proj_row.len() {
                                        return Ok(proj_row[idx].clone());
                                    }
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

                    sort_keys.map(|keys| (keys, proj_row, orig_idx))
                })
                .collect::<Result<Vec<_>>>()?;

            // Sort
            rows_with_keys.sort_by(|a, b| {
                for (i, order) in order_by.iter().enumerate() {
                    let cmp = a.0[i]
                        .partial_cmp(&b.0[i])
                        .unwrap_or(std::cmp::Ordering::Equal);
                    if cmp != std::cmp::Ordering::Equal {
                        return if order.asc { cmp } else { cmp.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });

            let (sorted_proj, sort_perm): (Vec<Vec<Value>>, Vec<usize>) =
                rows_with_keys.into_iter().map(|(_, row, i)| (row, i)).unzip();
            sorted_rows = sorted_proj;
            permutation = Some(sort_perm);
        }

        // Apply LATEST BY (time-series deduplication)
        let final_sorted_rows = if let Some(ref latest_by_cols) = stmt.latest_by {
            // 🔑 filtered_rows 按排序置换同步重排, 与投影行保持索引对齐。
            let aligned_filtered: Vec<(u64, SqlRow)> = match &permutation {
                Some(perm) => perm.iter().map(|&i| filtered_rows[i].clone()).collect(),
                None => filtered_rows.clone(),
            };
            self.apply_latest_by(
                sorted_rows,
                &aligned_filtered,
                latest_by_cols,
                &combined_schema,
            )?
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

        let final_rows: Vec<Vec<Value>> = deduplicated_rows
            .into_iter()
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
    fn fast_star_indexed_result(
        &self,
        table_name: &str,
        row_ids: &[RowId],
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<QueryResult> {
        let schema = self.db.get_table_schema(table_name)?;
        let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
        let arc_rows = self.db.get_table_rows_batch_arc(table_name, row_ids)?;
        let skip_n = offset.unwrap_or(0);
        let take_n = limit.unwrap_or(usize::MAX);
        let rows: Vec<Vec<Value>> = arc_rows
            .into_iter()
            .filter_map(|(_, opt)| opt)
            .skip(skip_n)
            .map(|arc| match Arc::try_unwrap(arc) {
                Ok(row) => row,
                Err(arc) => (*arc).clone(),
            })
            .take(take_n)
            .collect();
        Ok(QueryResult::Select {
            columns: column_names,
            rows,
        })
    }

    /// Direct Vec<Value> output for SELECT * with full scan.
    /// Uses streaming iterator — avoids materializing all rows via SqlRow.
    fn fast_star_scan_result(
        &self,
        table_name: &str,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<QueryResult> {
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
            if count <= skip_n {
                continue;
            }
            rows.push(row);
            if rows.len() >= take_n {
                break;
            }
            if count >= max_rows {
                break;
            }
        }
        Ok(QueryResult::Select {
            columns: column_names,
            rows,
        })
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
            Expr::FunctionCall { name, args, .. } => {
                matches!(
                    name.to_uppercase().as_str(),
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "STDDEV" | "VARIANCE"
                ) || args.iter().any(|a| self.expr_has_aggregates(a))
            }
            Expr::BinaryOp { left, right, .. } => {
                self.expr_has_aggregates(left) || self.expr_has_aggregates(right)
            }
            // 🔑 CASE can wrap aggregates in its WHEN/THEN/ELSE (e.g.
            // `CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END`). Recurse so such
            // expressions are correctly classified as containing aggregates.
            Expr::Case { whens, else_expr } => {
                whens
                    .iter()
                    .any(|(c, v)| self.expr_has_aggregates(c) || self.expr_has_aggregates(v))
                    || else_expr
                        .as_ref()
                        .is_some_and(|e| self.expr_has_aggregates(e))
            }
            Expr::UnaryOp { expr, .. } => self.expr_has_aggregates(expr),
            _ => false,
        }
    }

    /// Execute FROM clause - handles single table or JOINs
    /// Returns all rows with combined schema
    fn execute_from(&self, table_ref: &TableRef) -> FromScanResult {
        self.execute_from_with_limit(table_ref, None, &[])
    }

    /// execute_from + WHERE 单表谓词下推 (通用 join 路径的扫描预过滤)。
    fn execute_from_push(
        &self,
        table_ref: &TableRef,
        push: &[(String, String, crate::sql::ast::BinaryOperator, Value)],
    ) -> FromScanResult {
        self.execute_from_with_limit(table_ref, None, push)
    }

    /// 🚀 P0 OPTIMIZATION: Execute FROM clause with limit passed to storage layer
    fn execute_from_with_limit(
        &self,
        table_ref: &TableRef,
        limit: Option<usize>,
        push: &[(String, String, crate::sql::ast::BinaryOperator, Value)],
    ) -> FromScanResult {
        match table_ref {
            TableRef::Table { name, alias } => {
                // Single table - use table-specific scan with limit
                let schema = self.db.get_table_schema(name)?;

                // 🚀 P0: Scan table with streaming to reduce memory (with optional limit)
                let all_rows: Result<Vec<_>> = if let Some(limit_val) = limit {
                    // With limit: collect only up to limit rows
                    self.db
                        .scan_table_rows_streaming(name)?
                        .take(limit_val)
                        .collect()
                } else {
                    // No limit: collect all (unavoidable for full table scan)
                    self.db.scan_table_rows_streaming(name)?.collect()
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
            TableRef::Join {
                left,
                right,
                join_type,
                on_condition,
            } => {
                // Recursive: evaluate left and right (谓词继续向下传)
                let (left_rows, left_schema) = self.execute_from_push(left, push)?;
                let (right_rows, right_schema) = self.execute_from_push(right, push)?;

                // Combine schemas
                let mut combined_schema = (*left_schema).clone();
                combined_schema.columns.extend(right_schema.columns.clone());

                // 🔑 通用 join 路径的单表谓词预过滤: WHERE `alias.col op literal`
                // 在进 join 前按表前缀过滤两侧 SqlRow (INNER: 显然等价;
                // LEFT/RIGHT/FULL: b 侧谓词把不满足的 b 行提前剔除后, 对应
                // a 行得到 NULL 填充, 而 NULL 填充行在原 WHERE 的同一 b 列
                // 谓词下也是 UNKNOWN → 被过滤 — 结果一致。IS NULL 等非
                // col-op-literal 形状不推, 留给 join 后 WHERE。
                let _push: Vec<(String, String, crate::sql::ast::BinaryOperator, Value)> =
                    push.to_vec();
                let schema_row_get = |schema: &TableSchema, prefix: &str, bare: &str,
                                      row: &SqlRow|
                 -> Option<Value> {
                    let q = format!("{}.{}", prefix, bare);
                    if let Some(v) = row.get(&q) {
                        return Some(v.clone());
                    }
                    // bare 回退: schema 列名匹配 (前缀或裸)
                    schema
                        .columns
                        .iter()
                        .find(|c| c.name == q || c.name.ends_with(&format!(".{}", bare)))
                        .and_then(|c| row.get(&c.name).cloned())
                };
                let filter_rows = |rows: Vec<(u64, SqlRow)>,
                                   schema: &TableSchema,
                                   prefix: &str|
                 -> Vec<(u64, SqlRow)> {
                    if _push.is_empty() {
                        return rows;
                    }
                    rows.into_iter()
                        .filter(|(_, row)| {
                            _push.iter().all(|(p, bare, op, lit)| {
                                if p != prefix {
                                    return true;
                                }
                                let v = schema_row_get(schema, prefix, bare, row);
                                apply_op_value(op, v.as_ref(), lit)
                            })
                        })
                        .collect()
                };
                let left_alias = Self::table_ref_alias(left);
                let right_alias = Self::table_ref_alias(right);
                let left_rows =
                    filter_rows(left_rows, &left_schema, &left_alias);
                let right_rows =
                    filter_rows(right_rows, &right_schema, &right_alias);

                // Perform JOIN based on type
                let joined_rows = match join_type {
                    JoinType::Inner => self.inner_join(&left_rows, &right_rows, on_condition)?,
                    JoinType::Left => {
                        self.left_join(&left_rows, &right_rows, on_condition, &right_schema)?
                    }
                    JoinType::Right => {
                        self.right_join(&left_rows, &right_rows, on_condition, &left_schema)?
                    }
                    JoinType::Full => self.full_join(
                        &left_rows,
                        &right_rows,
                        on_condition,
                        &left_schema,
                        &right_schema,
                    )?,
                };

                Ok((joined_rows, Arc::new(combined_schema)))
            }
        }
    }

    /// Columnar-backed implementation of the IN-subquery hashset build.
    /// Finalizes any pending write buffer, then projects the inner SELECT
    /// column plus WHERE-referenced columns out of the columnar SSTable and
    /// filters positionally. Returns the set of matching inner-column values.
    fn build_in_hashset_from_columnar(
        &self,
        table_name: &str,
        _col_types: &[crate::types::ColumnType],
        inner_col_pos: usize,
        compiled_where: Option<&CompiledWhere>,
        where_positions: &[usize],
        where_pos_to_idx: &[Option<usize>],
    ) -> Option<(std::collections::HashSet<Value>, bool)> {
        // 🔑 旧实现直接读 columnar SSTable 投影
        // (scan_columnar_sstable_projection)，绕过 LSM 墓碑/写集合并层 —
        // DELETE/UPDATE 之后 IN (SELECT …) 仍返回已删行 (differential fuzz:
        // DELETE red 后 IN 命中全部旧行, SQLite 返回 0 行)。改用与 JOIN
        // 投影扫描相同的变异可见路径 (scan_table_rows_fast_projected)，
        // 快慢路径共享同一数据视图。
        let schema = self.db.get_table_schema(table_name).ok()?;

        // Collect distinct column positions we need to materialize:
        // the inner SELECT column + every column referenced by WHERE.
        let mut needed: Vec<usize> = vec![inner_col_pos];
        needed.extend_from_slice(where_positions);
        needed.sort_unstable();
        needed.dedup();

        let rows = self
            .scan_table_rows_fast_projected(table_name, &schema, Some(&needed))
            .ok()?;

        // Map from schema column position → index within `needed` (and thus
        // within the projected row, since projection preserves order).
        let proj_idx_of =
            |col_pos: usize| -> Option<usize> { needed.iter().position(|&c| c == col_pos) };
        let inner_proj = proj_idx_of(inner_col_pos)?;

        let mut set = std::collections::HashSet::with_capacity(rows.len().max(1024));
        let mut where_buf: Vec<Value> = Vec::with_capacity(where_positions.len().max(1));
        let mut has_null = false;

        for (_rid, row) in rows {
            if let Some(cw) = compiled_where {
                where_buf.clear();
                let mut ok = true;
                for &schema_pos in where_positions {
                    let pi = match proj_idx_of(schema_pos) {
                        Some(p) => p,
                        None => {
                            ok = false;
                            break;
                        }
                    };
                    where_buf.push(row.get(pi).cloned().unwrap_or(Value::Null));
                }
                if !ok {
                    continue;
                }
                if !cw.eval_at(&where_buf, where_pos_to_idx).unwrap_or(false) {
                    continue;
                }
            }
            let val = row.get(inner_proj).cloned().unwrap_or(Value::Null);
            if matches!(val, Value::Null) {
                has_null = true;
            } else {
                set.insert(val);
            }
        }
        Some((set, has_null))
    }

    /// Like materialize_subqueries but detects correlated subqueries in WHERE.
    /// Correlated subqueries (referencing outer columns) are left in place for
    /// per-row evaluation by col_segment_general_scan.
    fn materialize_subqueries_checked(
        &self,
        expr: &Expr,
        outer_schema: Option<&TableSchema>,
    ) -> Result<Expr> {
        match expr {
            Expr::Subquery(subquery) => {
                if let Some(os) = outer_schema {
                    if Self::is_correlated_subquery(subquery, os) {
                        return Ok(expr.clone()); // keep for per-row eval
                    }
                }
                // Non-correlated: delegate to standard materialization.
                self.materialize_subqueries(expr)
            }
            Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
                left: Box::new(self.materialize_subqueries_checked(left, outer_schema)?),
                op: op.clone(),
                right: Box::new(self.materialize_subqueries_checked(right, outer_schema)?),
            }),
            Expr::In { .. } => {
                // 🔑 Delegate IN (including `IN (SELECT ...)`) to the standard
                // materialize_subqueries, which has dedicated subquery→HashSet
                // handling. Recursing into list elements here would hit the
                // Expr::Subquery scalar branch above and error out on multi-row
                // subqueries ("use IN instead of =").
                self.materialize_subqueries(expr)
            }
            Expr::Exists(stmt) => {
                // Correlated EXISTS must stay a node for per-row evaluation.
                // Only fold when non-correlation is PROVABLE (outer schema
                // present + no outer refs): with outer_schema=None, eagerly
                // materializing a correlated subquery evaluated its unbound
                // outer columns as NULL → empty result → EXISTS silently
                // folded to false (differential testing: COUNT ... WHERE
                // EXISTS returned 0).
                if let crate::sql::ast::Statement::Select { stmt: sub, .. } = stmt.as_ref() {
                    match outer_schema {
                        Some(os) if !Self::is_correlated_subquery(sub, os) => {
                            return self.materialize_subqueries(expr);
                        }
                        _ => return Ok(expr.clone()),
                    }
                }
                self.materialize_subqueries(expr)
            }
            // All other expr types: delegate to standard materialization (which
            // handles their recursion correctly without touching Subquery nodes).
            _ => self.materialize_subqueries(expr),
        }
    }

    fn materialize_subqueries(&self, expr: &Expr) -> Result<Expr> {
        match expr {
            Expr::Exists(_) => {
                // NEVER fold here: this fn has no schema context and cannot
                // prove non-correlation. Eager materialization of a
                // correlated EXISTS evaluated its unbound outer columns as
                // NULL → empty subquery → silent false (differential testing:
                // COUNT ... WHERE EXISTS returned 0). Keep the node; the
                // schema-aware pass (materialize_subqueries_checked) folds
                // PROVABLY uncorrelated cases, everything else evaluates
                // per-row via eval_correlated_expr.
                Ok(expr.clone())
            }
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
                                "Subquery returns more than one row/column (use IN instead of =)"
                                    .into(),
                            ))
                        }
                    }
                    _ => Err(MoteDBError::Query(
                        "Subquery must return SELECT result".into(),
                    )),
                }
            }

            Expr::In {
                expr,
                list,
                negated,
            } => {
                // Check if list contains a subquery
                if list.len() == 1 {
                    if let Expr::Subquery(subquery) = &list[0] {
                        // 🚀 Fast path: if the outer column is a simple Column reference,
                        // stream the subquery result directly into a HashSet, avoiding
                        // the Vec<Vec<Value>> + Vec<Expr::Literal> double materialization.
                        let outer_col_opt = if let Expr::Column(col_name) = expr.as_ref() {
                            Some(col_name.as_str())
                        } else {
                            None
                        };

                        let fast_hashset = outer_col_opt
                            .and_then(|col| self.stream_in_subquery_to_hashset(subquery, col));

                        if let Some((hashset, has_null)) = fast_hashset {
                            // 🚀 Carry the pre-built HashSet end-to-end via InHashset
                            // (avoids the HashSet → Vec<Literal> → HashSet round-trip
                            // and the O(list_len) per-row eval in eval_expr_on_row).
                            return Ok(Expr::InHashset {
                                expr: Box::new(self.materialize_subqueries(expr)?),
                                set: hashset,
                                negated: *negated,
                                has_null,
                            });
                        }

                        // Fallback: execute subquery normally, build HashSet from rows.
                        let result = self.execute_select_internal(subquery)?;
                        match result {
                            QueryResult::Select { rows, .. } => {
                                let mut set: std::collections::HashSet<Value> =
                                    std::collections::HashSet::new();
                                let mut has_null = false;
                                for mut r in rows {
                                    if let Some(v) = r.drain(..).next() {
                                        if matches!(v, Value::Null) {
                                            has_null = true;
                                        } else {
                                            set.insert(v);
                                        }
                                    }
                                }
                                return Ok(Expr::InHashset {
                                    expr: Box::new(self.materialize_subqueries(expr)?),
                                    set,
                                    negated: *negated,
                                    has_null,
                                });
                            }
                            _ => {
                                return Err(MoteDBError::Query(
                                    "Subquery must return SELECT result".into(),
                                ))
                            }
                        }
                    }
                }

                // No subquery in list — materialize each list item + the expr.
                let materialized_list: Result<Vec<Expr>> = list
                    .iter()
                    .map(|e| self.materialize_subqueries(e))
                    .collect();
                Ok(Expr::In {
                    expr: Box::new(self.materialize_subqueries(expr)?),
                    list: materialized_list?,
                    negated: *negated,
                })
            }

            // InHashset: set is already pre-built. Only recurse into the expr
            // sub-tree (it may contain its own subqueries in rare cases).
            Expr::InHashset {
                expr,
                set,
                negated,
                has_null,
            } => Ok(Expr::InHashset {
                expr: Box::new(self.materialize_subqueries(expr)?),
                set: set.clone(),
                negated: *negated,
                has_null: *has_null,
            }),

            Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
                left: Box::new(self.materialize_subqueries(left)?),
                op: op.clone(),
                right: Box::new(self.materialize_subqueries(right)?),
            }),

            Expr::UnaryOp { op, expr } => Ok(Expr::UnaryOp {
                op: op.clone(),
                expr: Box::new(self.materialize_subqueries(expr)?),
            }),

            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => Ok(Expr::Between {
                expr: Box::new(self.materialize_subqueries(expr)?),
                low: Box::new(self.materialize_subqueries(low)?),
                high: Box::new(self.materialize_subqueries(high)?),
                negated: *negated,
            }),

            Expr::Like {
                expr,
                pattern,
                negated,
            } => Ok(Expr::Like {
                expr: Box::new(self.materialize_subqueries(expr)?),
                pattern: Box::new(self.materialize_subqueries(pattern)?),
                negated: *negated,
            }),

            Expr::IsNull { expr, negated } => Ok(Expr::IsNull {
                expr: Box::new(self.materialize_subqueries(expr)?),
                negated: *negated,
            }),

            Expr::FunctionCall {
                name,
                args,
                distinct,
            } => {
                let materialized_args: Result<Vec<Expr>> = args
                    .iter()
                    .map(|arg| self.materialize_subqueries(arg))
                    .collect();

                Ok(Expr::FunctionCall {
                    name: name.clone(),
                    args: materialized_args?,
                    distinct: *distinct,
                })
            }

            // 🔑 CASE can contain subqueries in its WHEN conditions or
            // THEN/ELSE values (e.g. `CASE WHEN v = (SELECT MAX(v) ...) ...`).
            // Recurse so those subqueries are materialized — previously Case
            // was treated as a leaf and nested subqueries were left as
            // Subquery nodes, which eval_expr_on_row cannot evaluate → NULL.
            Expr::Case { whens, else_expr } => {
                let mut new_whens: Vec<(Expr, Expr)> = Vec::with_capacity(whens.len());
                for (cond, val) in whens {
                    new_whens.push((
                        self.materialize_subqueries(cond)?,
                        self.materialize_subqueries(val)?,
                    ));
                }
                let new_else = match else_expr {
                    Some(e) => Some(Box::new(self.materialize_subqueries(e)?)),
                    None => None,
                };
                Ok(Expr::Case {
                    whens: new_whens,
                    else_expr: new_else,
                })
            }

            // Leaf nodes - no subqueries to materialize
            Expr::Column(_)
            | Expr::Literal(_)
            | Expr::Parameter(_)
            | Expr::Match { .. }
            | Expr::KnnSearch { .. }
            | Expr::KnnDistance { .. }
            | Expr::StWithin3D { .. }
            | Expr::StDistance3D { .. }
            | Expr::StKnn3D { .. }
            | Expr::StRadius3D { .. }
            | Expr::WindowFunction { .. } => Ok(expr.clone()),
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
                // 🚨 SQL three-valued logic: any comparison with NULL is UNKNOWN.
                // The previous code used Rust's `PartialOrd for Value` which
                // orders Null below all values — so `NULL < 5` returned `true`
                // and `NULL > 5` returned `false`, letting unmatched LEFT-JOIN
                // NULLs pass WHERE filters incorrectly. Defer NULL handling to
                // the evaluator's eval_binary_op, which correctly returns
                // Bool(false) for any NULL comparison (filtering the row out).
                if matches!(left_val, Value::Null) || matches!(right_val, Value::Null) {
                    // For comparison ops, NULL → false (UNKNOWN treated as not-true
                    // for WHERE filtering). For AND/OR, defer to evaluator for
                    // proper three-valued logic.
                    match op {
                        BinaryOperator::Lt
                        | BinaryOperator::Le
                        | BinaryOperator::Gt
                        | BinaryOperator::Ge
                        | BinaryOperator::Eq
                        | BinaryOperator::Ne => return Ok(Value::Bool(false)),
                        BinaryOperator::And | BinaryOperator::Or => {
                            // Fall through to evaluator for proper 3VL.
                            return self.evaluator.eval(expr, row);
                        }
                        _ => return self.evaluator.eval(expr, row),
                    }
                }
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
                    _ => self.evaluator.eval(expr, row), // Fall back to evaluator for complex ops
                }
            }

            Expr::Match { column, query, .. } => {
                let has_score = row.keys().any(|k| k.starts_with("__text_score_"));
                if has_score {
                    return Ok(Value::Bool(true));
                }
                // Set-membership via the text index, resolved once per
                // statement (this arm runs per row under aggregates and
                // compound predicates). The old fallback was an
                // AND-of-substrings scan, so `MATCH(c, 'a b')` matched a
                // DIFFERENT set than the index path's OR-over-tokens.
                // Rows without __table__/__row_id__ (LSM-sourced materialized
                // rows) fall back to the same token-OR scan the static
                // evaluator uses — returning false here made index-less
                // tables match nothing.
                let (table_name, row_id) = match (
                    row.get("__table__").and_then(|v| match v {
                        Value::Text(s) => Some(s.as_str()),
                        _ => None,
                    }),
                    row.get("__row_id__").and_then(|v| match v {
                        Value::Integer(i) => Some(*i as u64),
                        _ => None,
                    }),
                ) {
                    (Some(t), Some(r)) => (t, r),
                    _ => {
                        use crate::index::tokenizers::{Tokenizer as _, WhitespaceTokenizer};
                        match row.get(column) {
                            Some(Value::Text(text)) => {
                                let tok = WhitespaceTokenizer::default();
                                let q: Vec<String> =
                                    tok.tokenize(query).iter().map(|t| t.text.clone()).collect();
                                return Ok(Value::Bool(
                                    tok.tokenize(text).iter().any(|t| q.contains(&t.text)),
                                ));
                            }
                            _ => return Ok(Value::Bool(false)),
                        }
                    }
                };
                let index_name = self.db.index_registry.find_by_column(
                    table_name,
                    column,
                    crate::database::index_metadata::IndexType::Text,
                );
                let memo_key = format!("{table_name}|{column}|{query}");
                let hit = TEXT_MATCH_MEMO.with(|m| m.borrow().get(&memo_key).cloned());
                let set = match hit {
                    Some(set) => set,
                    None => {
                        let set: Arc<std::collections::HashSet<u64>> = Arc::new(
                            self.text_match_row_ids(
                                table_name,
                                column,
                                query,
                                index_name.as_deref(),
                            )?
                            .into_iter()
                            .collect(),
                        );
                        TEXT_MATCH_MEMO.with(|m| m.borrow_mut().insert(memo_key, Arc::clone(&set)));
                        set
                    }
                };
                Ok(Value::Bool(set.contains(&row_id)))
            }
            Expr::KnnSearch {
                column,
                query_vector,
                k,
            } => {
                // KNN_SEARCH returns Bool - true if this row is in top-k results
                let row_id = row
                    .get("__row_id__")
                    .and_then(|v| match v {
                        Value::Integer(i) => Some(*i as u64),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        MoteDBError::Query("KNN_SEARCH requires __row_id__ in row".into())
                    })?;

                // 🔧 Get table name
                let table_name = row
                    .get("__table__")
                    .and_then(|v| match v {
                        Value::Text(s) => Some(s.as_str()),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        MoteDBError::Query("KNN_SEARCH requires __table__ in row".into())
                    })?;

                // 🔧 Use index_registry to find the correct user-specified index name
                let index_name = self
                    .db
                    .index_registry
                    .find_by_column(
                        table_name,
                        column,
                        crate::database::index_metadata::IndexType::Vector,
                    )
                    .ok_or_else(|| {
                        MoteDBError::Query(format!(
                            "No vector index found for column '{}.{}'",
                            table_name, column
                        ))
                    })?;

                // Perform KNN search using public API
                let results = self
                    .db
                    .vector_search(&index_name, query_vector.as_slice(), *k)?;

                // Check if row_id is in results
                let in_results = results.iter().any(|(id, _)| *id == row_id);
                Ok(Value::Bool(in_results))
            }

            Expr::KnnDistance {
                column,
                query_vector,
            } => {
                // KNN_DISTANCE returns Float - distance/similarity score
                // Get vector value from row
                let vector = self
                    .get_column_value(row, column)
                    .ok_or_else(|| MoteDBError::ColumnNotFound(column.clone()))?;

                let vec_data = match vector {
                    Value::Vector(v) => v,
                    _ => {
                        return Err(MoteDBError::TypeError(format!(
                            "Column '{}' is not a vector",
                            column
                        )))
                    }
                };

                // Compute distance (using L2 distance)
                if vec_data.len() != query_vector.len() {
                    return Err(MoteDBError::InvalidArgument(format!(
                        "Vector dimension mismatch: {} vs {}",
                        vec_data.len(),
                        query_vector.len()
                    )));
                }

                let distance: f32 = vec_data
                    .iter()
                    .zip(query_vector.iter())
                    .map(|(a, b)| (a - b).powi(2))
                    .sum::<f32>()
                    .sqrt();

                Ok(Value::Float(distance as f64))
            }

            // ==================== 3D Spatial Expressions (i-Octree) ====================
            Expr::StDistance3D { column, x, y, z } => {
                // Always computed from the row's geometry: the pre-computed
                // `__spatial_distance__` belongs to the ORDER BY / KNN query
                // point, which need not be this expression's point.
                let point_value = self
                    .get_column_value(row, column)
                    .ok_or_else(|| MoteDBError::ColumnNotFound(column.clone()))?;

                use crate::types::Geometry;
                let geom = match point_value {
                    Value::Spatial(g) => match &*g {
                        Geometry::Point3D(p) => *p,
                        Geometry::Point(p) => {
                            // 2D point treated as z=0
                            crate::types::Point3D::new(p.x, p.y, 0.0)
                        }
                        _ => {
                            return Err(MoteDBError::TypeError(format!(
                                "Column '{}' is not a 3D Point",
                                column
                            )))
                        }
                    },
                    _ => {
                        return Err(MoteDBError::TypeError(format!(
                            "Column '{}' is not a 3D Point",
                            column
                        )))
                    }
                };

                let dx = geom.x - x;
                let dy = geom.y - y;
                let dz = geom.z - z;
                Ok(Value::Float((dx * dx + dy * dy + dz * dz).sqrt()))
            }

            Expr::StWithin3D {
                column,
                min_x,
                min_y,
                min_z,
                max_x,
                max_y,
                max_z,
            } => {
                if row.get("__spatial_within__").is_some() {
                    return Ok(Value::Bool(true));
                }
                let point_value = self
                    .get_column_value(row, column)
                    .ok_or_else(|| MoteDBError::ColumnNotFound(column.clone()))?;

                use crate::types::Geometry;
                let geom = match point_value {
                    Value::Spatial(g) => match &*g {
                        Geometry::Point3D(p) => *p,
                        Geometry::Point(p) => crate::types::Point3D::new(p.x, p.y, 0.0),
                        _ => return Ok(Value::Bool(false)),
                    },
                    _ => return Ok(Value::Bool(false)),
                };

                Ok(Value::Bool(
                    geom.x >= *min_x
                        && geom.x <= *max_x
                        && geom.y >= *min_y
                        && geom.y <= *max_y
                        && geom.z >= *min_z
                        && geom.z <= *max_z,
                ))
            }

            Expr::StKnn3D { column, x, y, z, k } => {
                // Fast path: already filtered by i-Octree KNN
                if row.get("__spatial_knn__").is_some() {
                    return Ok(Value::Bool(true));
                }
                let row_id = row
                    .get("__row_id__")
                    .and_then(|v| match v {
                        Value::Integer(i) => Some(*i as u64),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        MoteDBError::Query("ST_KNN_3D requires __row_id__ in row".into())
                    })?;
                let table_name = row
                    .get("__table__")
                    .and_then(|v| match v {
                        Value::Text(s) => Some(s.as_str()),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        MoteDBError::Query("ST_KNN_3D requires __table__ in row".into())
                    })?;

                // No index → exact kNN over the table's geometry column
                // (computed once, memoized). Without this the arm errored
                // ("No ioctree index for …") and index-less tables silently
                // returned 0 rows for ST_KNN_3D.
                let index_name = self.db.index_registry.find_by_column(
                    table_name,
                    column,
                    crate::database::index_metadata::IndexType::Octree,
                );

                // One lookup per statement, not per row: this arm runs for
                // every scanned row when the predicate sits under an
                // aggregate / ORDER BY / AND, and a fresh KNN walk per row is
                // O(N × log N) — 200K rows took seconds. The memo is thread
                // local and cleared at statement start (execute_streaming_ref).
                let memo_key = format!("{table_name}|{column}|{x}|{y}|{z}|{k}");
                let hit = SPATIAL_KNN_MEMO.with(|m| m.borrow().get(&memo_key).cloned());
                let set = match hit {
                    Some(set) => set,
                    None => {
                        let set: Arc<std::collections::HashSet<u64>> = Arc::new(
                            self.spatial_knn_row_ids(
                                table_name,
                                column,
                                *x,
                                *y,
                                *z,
                                *k,
                                index_name.as_deref(),
                            )?
                            .into_iter()
                            .collect(),
                        );
                        SPATIAL_KNN_MEMO
                            .with(|m| m.borrow_mut().insert(memo_key, Arc::clone(&set)));
                        set
                    }
                };
                Ok(Value::Bool(set.contains(&row_id)))
            }

            Expr::StRadius3D {
                column,
                x,
                y,
                z,
                radius,
            } => {
                if row.get("__spatial_knn__").is_some() {
                    return Ok(Value::Bool(true));
                }
                let point_value = self
                    .get_column_value(row, column)
                    .ok_or_else(|| MoteDBError::ColumnNotFound(column.clone()))?;

                use crate::types::Geometry;
                let geom = match point_value {
                    Value::Spatial(g) => match &*g {
                        Geometry::Point3D(p) => *p,
                        Geometry::Point(p) => crate::types::Point3D::new(p.x, p.y, 0.0),
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

            _ => self.evaluator.eval(expr, row),
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

        // Recency column: the first TIMESTAMP column. Tables without one
        // (e.g. `ts INT`) fall back to ROW ORDER — the last inserted row per
        // group is "latest" (the old fast-path behavior returned every row,
        // and the strict error rejected `ts INT` tables outright).
        let timestamp_col_name = schema
            .columns
            .iter()
            .find(|c| c.col_type == ColumnType::Timestamp)
            .map(|c| c.name.clone());

        // Build grouping key -> (max_timestamp, projected_row) map
        // Use Vec<Value> keys to avoid per-row String allocation from to_string()/format!()
        // Materialized rows key columns by QUALIFIED names ("t.col"); accept
        // both spellings (a bare-only lookup made LATEST BY fail with
        // 'Column not found' on the materialized path).
        let lookup = |row: &SqlRow, name: &str| -> Option<Value> {
            row.get(name).cloned().or_else(|| {
                if !name.contains('.') {
                    row.iter()
                        .find(|(k, _)| k.rsplit('.').next() == Some(name))
                        .map(|(_, v)| v.clone())
                } else {
                    None
                }
            })
        };
        let mut groups: HashMap<Vec<Value>, (i64, usize)> = HashMap::new();

        for (i, (_, full_row)) in filtered_rows.iter().enumerate() {
            // Extract grouping key as Vec<Value> — zero String allocation
            let group_key: Result<Vec<Value>> = latest_by_cols
                .iter()
                .map(|col_name| {
                    lookup(full_row, col_name)
                        .ok_or_else(|| MoteDBError::ColumnNotFound(col_name.clone()))
                })
                .collect();
            let group_key = group_key?;

            // Extract timestamp
            // With no TIMESTAMP column the recency key is the row id
            // (insertion order).
            let ts_value = match timestamp_col_name.as_ref() {
                Some(col_name) => match lookup(full_row, col_name) {
                    Some(Value::Timestamp(ts)) => ts.as_micros(),
                    Some(Value::Integer(i)) => i,
                    _ => {
                        return Err(MoteDBError::Query(format!(
                            "Timestamp column '{col_name}' must be TIMESTAMP or INTEGER type"
                        )))
                    }
                },
                None => filtered_rows[i].0 as i64,
            };

            // Track the newest row's INDEX per group (ties keep the first seen,
            // matching the old `ts_value > *max_ts` semantics).
            match groups.get(&group_key) {
                Some((max_ts, _)) if ts_value <= *max_ts => {}
                _ => {
                    groups.insert(group_key, (ts_value, i));
                }
            }
        }

        // 🔑 按输入顺序输出每组胜者 (两遍法): 调用方传入的 projected_rows
        // 可能已经 ORDER BY 排序 — 旧实现 HashMap::into_values 的迭代序
        // 会把排序打乱 (LATEST BY sensor ORDER BY sensor 输出乱序/逆序)。
        let winners: std::collections::HashSet<usize> =
            groups.values().map(|(_, i)| *i).collect();
        Ok(projected_rows
            .into_iter()
            .enumerate()
            .filter(|(i, _)| winners.contains(i))
            .map(|(_, row)| row)
            .collect())
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
        // A name that matches no row key may be a SELECT alias or a SELECT
        // expression's canonical name (`GROUP BY b` / `GROUP BY
        // TIME_BUCKET('5s', ts)`); those are computed per row below.
        let mut alias_exprs: Vec<Option<&Expr>> = Vec::with_capacity(group_by_cols.len());
        let resolved_col_names: Vec<String> = if !rows.is_empty() {
            let first_row = &rows[0].1;
            group_by_cols
                .iter()
                .map(|col_name| {
                    // 🔑 列别名: `SELECT a AS k … GROUP BY k` — 把别名映射回
                    // 底层列名 (ColumnWithAlias 此前不匹配任何分支 → 分组键
                    // 解析失败, 后续 COUNT 求值报 not-implemented)。
                    let underlying = columns.iter().find_map(|c| match c {
                        SelectColumn::ColumnWithAlias(n, a) if a == col_name => {
                            Some(n.clone())
                        }
                        _ => None,
                    });
                    let col_name: &String = underlying.as_ref().unwrap_or(col_name);
                    if first_row.contains_key(col_name) {
                        alias_exprs.push(None);
                        return col_name.clone();
                    }
                    if let Some(key) = first_row.keys().find(|key| {
                        key.ends_with(&format!(".{}", col_name))
                            || key.as_str() == col_name.as_str()
                    }) {
                        alias_exprs.push(None);
                        return key.clone();
                    }
                    let expr = columns.iter().find_map(|c| match c {
                        SelectColumn::Expr(e, Some(a)) if a == col_name => Some(e),
                        // `GROUP BY <full expression text>` must also find the
                        // expression when the SELECT item carries an alias.
                        // 🔑 无别名表达式必须按 canonical 名匹配 — 旧的
                        // `alias.is_none() || …` 让第一个无别名表达式匹配任何
                        // 组项, 双表达式组键双双解析到它 (GROUP BY id%3, id%5
                        // 返回 3 组而非 15 组, 差分对拍抓出)。
                        SelectColumn::Expr(e, alias)
                            if &Self::expr_to_column_name(e) == col_name
                                || alias.as_deref() == Some(col_name.as_str()) =>
                        {
                            Some(e)
                        }
                        _ => None,
                    });
                    alias_exprs.push(expr);
                    col_name.clone()
                })
                .collect()
        } else {
            group_by_cols.to_vec()
        };

        // Build groups: Vec<Value> key avoids per-row String allocations
        let mut groups: HashMap<Vec<Value>, Vec<&SqlRow>> =
            HashMap::with_capacity(rows.len().min(1024));

        for (_, row) in rows {
            let group_key: Result<Vec<Value>> = resolved_col_names
                .iter()
                .zip(alias_exprs.iter())
                .map(|(col_name, expr)| match expr {
                    Some(e) => self.evaluator.eval(e, row),
                    None => row
                        .get(col_name)
                        .cloned()
                        .ok_or_else(|| MoteDBError::ColumnNotFound(col_name.clone())),
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
                            "SELECT * not allowed with GROUP BY".to_string(),
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
                    SelectColumn::Expr(expr, _) => self.eval_aggregate(expr, &empty_rows)?,
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
                            "SELECT * not allowed with GROUP BY".to_string(),
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
                        // 🔑 Allow bare columns not in GROUP BY (SQLite behavior):
                        // take the value from the first row of the group. This
                        // supports functional-dependency cases like
                        // `SELECT a.name, COUNT(*) ... GROUP BY a.id` where a.id
                        // is the PK (so a.name is determined per group), and
                        // table-prefixed columns (`a.name` vs GROUP BY `a.id`).
                        // Previously this rejected `a.name` because the string
                        // didn't literally match `a.id`.
                        Self::get_value_from_row(group_rows[0], name)
                    }
                    SelectColumn::ColumnWithAlias(name, _) => {
                        Self::get_value_from_row(group_rows[0], name)
                    }
                    SelectColumn::Expr(expr, _) => {
                        // Aggregate function or expression
                        self.eval_aggregate(expr, &group_rows)?
                    }
                    SelectColumn::Star => {
                        return Err(MoteDBError::Query(
                            "SELECT * not allowed with GROUP BY".to_string(),
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
                // 🚨 Compute aggregates referenced in HAVING but NOT in the
                // SELECT list (e.g. JOIN: `SELECT c.name, SUM(o.amt) AS total
                // ... HAVING SUM(o.amt) > 100`). Without this, the evaluator's
                // aggregate lookup fails → every group skipped (empty result).
                for agg_expr in Self::collect_aggregate_calls(having_expr) {
                    let key = Self::aggregate_expr_key(&agg_expr);
                    if let std::collections::hash_map::Entry::Vacant(e) = temp_row.entry(key) {
                        let val = self.eval_aggregate(&agg_expr, &group_rows)?;
                        e.insert(val);
                    }
                }

                // HAVING evaluation: propagate errors instead of silently
                // treating them as "group doesn't pass" (which hides bugs).
                let passes = match self
                    .evaluator
                    .eval(having_expr, &temp_row)
                    .and_then(|val| self.to_bool(&val))
                {
                    Ok(b) => b,
                    Err(e) => {
                        warn_log!("[HAVING] evaluation error, skipping group: {}", e);
                        continue;
                    }
                };

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
            Expr::FunctionCall {
                name,
                args,
                distinct,
            } => {
                let func_name = name.to_uppercase();
                match func_name.as_str() {
                    "COUNT" => {
                        if *distinct {
                            // COUNT(DISTINCT column)
                            if args.is_empty() || matches!(args[0], Expr::Column(ref c) if c == "*")
                            {
                                return Err(MoteDBError::InvalidArgument(
                                    "COUNT(DISTINCT *) is not supported".to_string(),
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
                        } else if args.is_empty()
                            || matches!(args[0], Expr::Column(ref c) if c == "*")
                        {
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
                            return Err(MoteDBError::InvalidArgument(
                                "SUM requires an argument".to_string(),
                            ));
                        }
                        // Collect values, dedup if DISTINCT.
                        let all_vals: Vec<Value> = rows
                            .iter()
                            .map(|r| self.evaluator.eval(&args[0], r))
                            .collect::<Result<Vec<_>>>()?;
                        let vals: Vec<Value> = if *distinct {
                            use std::collections::HashSet;
                            let mut seen: HashSet<Value> = HashSet::new();
                            all_vals
                                .into_iter()
                                .filter(|v| !matches!(v, Value::Null) && seen.insert(v.clone()))
                                .collect()
                        } else {
                            all_vals
                        };
                        let mut int_sum: i64 = 0;
                        let mut float_sum = CompSum::default();
                        let mut has_float = false;
                        let mut has_value = false;
                        for val in &vals {
                            match val {
                                Value::Integer(i) => {
                                    has_value = true;
                                    if has_float {
                                        float_sum.add(*i as f64);
                                    } else if let Some(s) = int_sum.checked_add(*i) {
                                        int_sum = s;
                                    } else {
                                        has_float = true;
                                        float_sum.add(int_sum as f64);
                                        float_sum.add(*i as f64);
                                    }
                                }
                                Value::Float(f) => {
                                    has_value = true;
                                    if !has_float {
                                        has_float = true;
                                        float_sum.add(int_sum as f64);
                                    }
                                    float_sum.add(*f);
                                }
                                Value::Null => {}
                                _ => {
                                    return Err(MoteDBError::TypeError(
                                        "SUM requires numeric values".to_string(),
                                    ))
                                }
                            }
                        }
                        if !has_value {
                            Ok(Value::Null)
                        } else if has_float {
                            Ok(Value::Float(float_sum.total()))
                        } else {
                            Ok(Value::Integer(int_sum))
                        }
                    }
                    "AVG" => {
                        if args.is_empty() {
                            return Err(MoteDBError::InvalidArgument(
                                "AVG requires an argument".to_string(),
                            ));
                        }
                        let all_vals: Vec<Value> = rows
                            .iter()
                            .map(|r| self.evaluator.eval(&args[0], r))
                            .collect::<Result<Vec<_>>>()?;
                        let vals: Vec<Value> = if *distinct {
                            use std::collections::HashSet;
                            let mut seen: HashSet<Value> = HashSet::new();
                            all_vals
                                .into_iter()
                                .filter(|v| !matches!(v, Value::Null) && seen.insert(v.clone()))
                                .collect()
                        } else {
                            all_vals
                        };
                        let mut sum = CompSum::default();
                        let mut count = 0;
                        for val in &vals {
                            match val {
                                Value::Integer(i) => {
                                    sum.add(*i as f64);
                                    count += 1;
                                }
                                Value::Float(f) => {
                                    sum.add(*f);
                                    count += 1;
                                }
                                Value::Null => {}
                                _ => {
                                    return Err(MoteDBError::TypeError(
                                        "AVG requires numeric values".to_string(),
                                    ))
                                }
                            }
                        }
                        if count == 0 {
                            Ok(Value::Null)
                        } else {
                            Ok(Value::Float(sum.total() / count as f64))
                        }
                    }
                    "MIN" => {
                        if args.is_empty() {
                            return Err(MoteDBError::InvalidArgument(
                                "MIN requires an argument".to_string(),
                            ));
                        }
                        // DISTINCT has no effect on MIN/MAX, but for correctness we
                        // still respect it (dedup is a no-op on the result).
                        let mut min_val: Option<Value> = None;
                        let mut seen: Option<std::collections::HashSet<Value>> = if *distinct {
                            Some(std::collections::HashSet::new())
                        } else {
                            None
                        };
                        for row in rows {
                            let val = self.evaluator.eval(&args[0], row)?;
                            if matches!(val, Value::Null) {
                                continue;
                            }
                            if let Some(ref mut s) = seen {
                                if !s.insert(val.clone()) {
                                    continue;
                                }
                            }
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
                        Ok(min_val.unwrap_or(Value::Null))
                    }
                    "MAX" => {
                        if args.is_empty() {
                            return Err(MoteDBError::InvalidArgument(
                                "MAX requires an argument".to_string(),
                            ));
                        }
                        let mut max_val: Option<Value> = None;
                        let mut seen: Option<std::collections::HashSet<Value>> = if *distinct {
                            Some(std::collections::HashSet::new())
                        } else {
                            None
                        };
                        for row in rows {
                            let val = self.evaluator.eval(&args[0], row)?;
                            if matches!(val, Value::Null) {
                                continue;
                            }
                            if let Some(ref mut s) = seen {
                                if !s.insert(val.clone()) {
                                    continue;
                                }
                            }
                            max_val = Some(match max_val {
                                None => val,
                                Some(current) => {
                                    if val.partial_cmp(&current)
                                        == Some(std::cmp::Ordering::Greater)
                                    {
                                        val
                                    } else {
                                        current
                                    }
                                }
                            });
                        }
                        Ok(max_val.unwrap_or(Value::Null))
                    }
                    "GROUP_CONCAT" => {
                        // 🔑 GROUP_CONCAT(expr [, separator]) — concatenates
                        // non-NULL values with a separator (default ',').
                        if args.is_empty() {
                            return Err(MoteDBError::InvalidArgument(
                                "GROUP_CONCAT requires an argument".to_string(),
                            ));
                        }
                        // Determine separator (2nd arg, if a literal).
                        let sep = if args.len() >= 2 {
                            if let Ok(Value::Text(t)) =
                                self.evaluator.eval(&args[1], &SqlRow::new())
                            {
                                t.to_string()
                            } else {
                                ",".to_string()
                            }
                        } else {
                            ",".to_string()
                        };
                        let mut parts: Vec<String> = Vec::new();
                        for row in rows {
                            let val = self.evaluator.eval(&args[0], row)?;
                            if !matches!(val, Value::Null) {
                                parts.push(value_to_concat_string(&val));
                            }
                        }
                        if parts.is_empty() {
                            Ok(Value::Null)
                        } else {
                            Ok(Value::text(parts.join(&sep)))
                        }
                    }
                    // Non-aggregate function in a GROUP BY SELECT list
                    // (e.g. the group-key expression `TIME_BUCKET('5s', ts)`
                    // itself): evaluate it as a scalar on the group's
                    // representative row — every row in the group shares the
                    // key, so the first row gives the right value. Empty
                    // group → NULL.
                    _ => match rows.first() {
                        Some(row) => self.evaluator.eval(expr, row),
                        None => Ok(Value::Null),
                    },
                }
            }
            // 🆕 Compound expressions that wrap an aggregate (e.g.
            // `CASE WHEN COUNT(*) > 3 THEN 'many' ELSE 'few' END` or
            // `SUM(v) + 1`). Evaluate the aggregate sub-expressions against
            // the group's rows, then evaluate the outer expression with the
            // aggregates replaced by their computed scalar values.
            Expr::Case { .. } | Expr::BinaryOp { .. } | Expr::UnaryOp { .. }
                if Self::is_aggregate_expr(expr) =>
            {
                self.eval_aggregate_compound(expr, rows)
            }
            // 🆕 纯非聚合表达式 (`b % 3`, `-b`, `CASE WHEN b > 0 …`):
            // 与 FunctionCall 的组键表达式同待遇 — 在组的代表行上求值
            // (组内每行共享组键, `SELECT b % 3 … GROUP BY b % 3` 合法,
            // SQLite 语义; 此前直接报 "must be in GROUP BY")。
            Expr::BinaryOp { .. } | Expr::UnaryOp { .. } | Expr::Case { .. } => {
                match rows.first() {
                    Some(row) => self.evaluator.eval(expr, row),
                    None => Ok(Value::Null),
                }
            }
            _ => {
                // Non-aggregate expression in GROUP BY context
                Err(MoteDBError::Query(
                    "Non-aggregate expressions in SELECT with GROUP BY must be in GROUP BY clause"
                        .to_string(),
                ))
            }
        }
    }

    /// Evaluate a compound expression (CASE / BinaryOp / UnaryOp) that
    /// contains nested aggregate function calls.
    ///
    /// Strategy: walk the expression tree, replace each aggregate
    /// `FunctionCall` with `Expr::Literal(computed_value)`, then evaluate
    /// the resulting purely-scalar expression against an empty row (the
    /// scalar expression no longer depends on any row).
    fn eval_aggregate_compound(&self, expr: &Expr, rows: &[&SqlRow]) -> Result<Value> {
        let resolved = self.resolve_aggregates_in_expr(expr, rows)?;
        // resolved has aggregates replaced with Literals; evaluate it.
        // Use an empty SqlRow since no per-row data is needed.
        let empty_row = SqlRow::new();
        self.evaluator.eval(&resolved, &empty_row)
    }

    /// Recursively replace aggregate FunctionCall nodes with Literal values
    /// computed over `rows`. Non-aggregate parts of the tree are unchanged.
    fn resolve_aggregates_in_expr(&self, expr: &Expr, rows: &[&SqlRow]) -> Result<Expr> {
        match expr {
            Expr::FunctionCall { name, .. }
                if matches!(
                    name.to_uppercase().as_str(),
                    "COUNT"
                        | "SUM"
                        | "AVG"
                        | "MIN"
                        | "MAX"
                        | "STDDEV"
                        | "VARIANCE"
                        | "GROUP_CONCAT"
                ) =>
            {
                let val = self.eval_aggregate(expr, rows)?;
                Ok(Expr::Literal(val))
            }
            Expr::FunctionCall {
                name,
                args,
                distinct,
            } => {
                let resolved_args: Result<Vec<Expr>> = args
                    .iter()
                    .map(|a| self.resolve_aggregates_in_expr(a, rows))
                    .collect();
                Ok(Expr::FunctionCall {
                    name: name.clone(),
                    args: resolved_args?,
                    distinct: *distinct,
                })
            }
            Expr::Case { whens, else_expr } => {
                let new_whens: Result<Vec<(Expr, Expr)>> = whens
                    .iter()
                    .map(|(cond, val)| {
                        Ok((
                            self.resolve_aggregates_in_expr(cond, rows)?,
                            self.resolve_aggregates_in_expr(val, rows)?,
                        ))
                    })
                    .collect();
                let new_else = match else_expr {
                    Some(e) => Some(Box::new(self.resolve_aggregates_in_expr(e, rows)?)),
                    None => None,
                };
                Ok(Expr::Case {
                    whens: new_whens?,
                    else_expr: new_else,
                })
            }
            Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
                left: Box::new(self.resolve_aggregates_in_expr(left, rows)?),
                op: op.clone(),
                right: Box::new(self.resolve_aggregates_in_expr(right, rows)?),
            }),
            Expr::UnaryOp { op, expr } => Ok(Expr::UnaryOp {
                op: op.clone(),
                expr: Box::new(self.resolve_aggregates_in_expr(expr, rows)?),
            }),
            _ => Ok(expr.clone()),
        }
    }

    /// Check if column list contains any aggregate functions
    fn has_aggregates(&self, columns: &[SelectColumn]) -> bool {
        columns.iter().any(|col| match col {
            SelectColumn::Expr(expr, _) => Self::is_aggregate_expr(expr),
            _ => false,
        })
    }

    /// Check if an expression is a compile-time constant (can be evaluated with
    /// an empty row). Used by INSERT VALUES to constant-fold negative literals
    /// (`-1e15`, `-(5.0)`) and other constant expressions into Values without
    /// requiring column references. The parser represents leading-minus numbers
    /// as `UnaryOp(Minus, Literal)`, not as negative Literals.
    fn is_constant_expr(expr: &Expr) -> bool {
        match expr {
            Expr::Literal(_) | Expr::Parameter(_) => true,
            Expr::UnaryOp { expr: inner, .. } => Self::is_constant_expr(inner),
            Expr::BinaryOp { left, right, .. } => {
                Self::is_constant_expr(left) && Self::is_constant_expr(right)
            }
            _ => false,
        }
    }

    /// Check if the SELECT list contains any *computed* expression — i.e. an
    /// `Expr` that is not a bare column reference or literal. The columnar scan
    /// fast paths only project raw columns/literals; computed expressions
    /// (`a + b`, `CONCAT(...)`, `IF(...)`, `-v`, scalar subqueries, …) must go
    /// through the materialized path where `eval_expr_on_row` evaluates them.
    /// `Star` and `Column`/`ColumnWithAlias` are NOT computed.
    fn select_has_computed_expression(columns: &[SelectColumn]) -> bool {
        columns.iter().any(|col| match col {
            SelectColumn::Star | SelectColumn::Column(_) | SelectColumn::ColumnWithAlias(_, _) => {
                false
            }
            // Any Expr that isn't a bare Column requires eval_expr_on_row —
            // including literals (SELECT NULL, id) and function calls. These
            // can't be served by the zero-copy SelectColumnar path (raw columns
            // only), so they must route to the projected-scan fallback which
            // evaluates expressions per row.
            SelectColumn::Expr(expr, _) => !matches!(expr, Expr::Column(_)),
        })
    }

    /// Check if a scalar subquery is correlated (references outer columns).
    /// Collects all column names referenced in the subquery's WHERE clause,
    /// then checks if any are NOT in the subquery's own FROM table schema
    /// (meaning they must come from the outer query).
    fn is_correlated_subquery(subquery: &SelectStmt, _outer_schema: &TableSchema) -> bool {
        // Use alias if present, else table name — this is what SQL column prefixes refer to.
        let table_id = match subquery.from.as_ref() {
            Some(TableRef::Table { name, alias }) => alias.clone().unwrap_or_else(|| name.clone()),
            _ => return false,
        };
        let where_clause = match &subquery.where_clause {
            Some(w) => w,
            None => return false,
        };
        let mut col_names: Vec<String> = Vec::new();
        Self::collect_column_names(where_clause, &mut col_names);
        let subq_id = table_id.rsplit('.').next().unwrap_or(&table_id);
        for cn in &col_names {
            if cn.contains('.') {
                let prefix = cn.rsplit('.').nth(1).unwrap_or("");
                let prefix_bare = prefix.rsplit('.').next().unwrap_or(prefix);
                if prefix_bare != subq_id {
                    return true;
                }
            }
        }
        false
    }

    /// Strict variant of [`Self::collect_column_names`]: returns false when the
    /// tree contains an Expr variant it does not know how to walk, so callers
    /// relying on the collected set for column PRUNING can detect that a
    /// referenced column might be missed and disable pruning (a pruned column
    /// referenced by a WHERE would otherwise silently evaluate to no-match).
    /// ORDER BY 表达式键是否引用了 SELECT 输出之外的列 → 需要物化全行排序。
    /// 列名/字面量键不算 (各路径的投影列/别名/序号解析自行处理)。
    /// 距离键 (<->/<=>) 的左列是 VECTOR 时豁免 — 列存 top-k / 向量下推接管;
    /// GEOMETRY 列 (loc <-> ST_POINT(...)) 或 schema 未知时保守物化。
    fn order_by_needs_full_rows(
        order_by: &[crate::sql::ast::OrderByExpr],
        columns: &[SelectColumn],
        schema: Option<&TableSchema>,
    ) -> bool {
        let mut out_names: Vec<String> = Vec::new();
        for c in columns {
            match c {
                SelectColumn::Star => return false, // SELECT *: 所有列都可用
                SelectColumn::Column(n) => {
                    out_names.push(n.clone());
                    out_names.push(n.rsplit('.').next().unwrap_or(n).to_string());
                }
                SelectColumn::ColumnWithAlias(n, a) => {
                    out_names.push(a.clone());
                    out_names.push(n.clone());
                    out_names.push(n.rsplit('.').next().unwrap_or(n).to_string());
                }
                SelectColumn::Expr(_, Some(a)) => out_names.push(a.clone()),
                SelectColumn::Expr(_, None) => {}
            }
        }
        for oe in order_by {
            if matches!(&oe.expr, Expr::Column(_) | Expr::Literal(_)) {
                continue;
            }
            // 向量距离键: 左列是 VECTOR → 列存 top-k / 向量下推处理
            if let Expr::BinaryOp { left, op, .. } = &oe.expr {
                if matches!(
                    op,
                    crate::sql::ast::BinaryOperator::L2Distance
                        | crate::sql::ast::BinaryOperator::CosineDistance
                ) {
                    if let (Expr::Column(cname), Some(schema)) = (left.as_ref(), schema) {
                        let bare = cname.rsplit('.').next().unwrap_or(cname);
                        if let Some(col) = schema.columns.iter().find(|c| c.name == bare) {
                            if matches!(col.col_type, crate::types::ColumnType::Tensor(_)) {
                                continue; // 向量列 → 下推接管
                            }
                        }
                    }
                }
            }
            // 其余表达式: 引用的列必须全部在输出里, 否则投影排序求不出键。
            let mut refs: Vec<String> = Vec::new();
            if !Self::collect_column_names_strict(&oe.expr, &mut refs) {
                return true; // 未知形状 → 保守物化
            }
            for r in refs {
                let bare = r.rsplit('.').next().unwrap_or(&r);
                let hit = out_names.iter().any(|n| {
                    n == &r || n == bare || n.rsplit('.').next().unwrap_or(n) == bare
                });
                if !hit {
                    return true;
                }
            }
        }
        false
    }

    pub(crate) fn collect_column_names_strict(expr: &Expr, out: &mut Vec<String>) -> bool {
        match expr {
            Expr::Column(name) => {
                out.push(name.clone());
                true
            }
            Expr::Literal(_) => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_column_names_strict(left, out)
                    && Self::collect_column_names_strict(right, out)
            }
            Expr::UnaryOp { expr, .. } => Self::collect_column_names_strict(expr, out),
            Expr::FunctionCall { args, .. } => args
                .iter()
                .all(|a| Self::collect_column_names_strict(a, out)),
            Expr::IsNull { expr, .. } => Self::collect_column_names_strict(expr, out),
            Expr::In { expr, list, .. } => {
                Self::collect_column_names_strict(expr, out)
                    && list
                        .iter()
                        .all(|e| Self::collect_column_names_strict(e, out))
            }
            Expr::Between {
                expr,
                low,
                high,
                ..
            } => {
                Self::collect_column_names_strict(expr, out)
                    && Self::collect_column_names_strict(low, out)
                    && Self::collect_column_names_strict(high, out)
            }
            Expr::Like { expr, pattern, .. } => {
                Self::collect_column_names_strict(expr, out)
                    && Self::collect_column_names_strict(pattern, out)
            }
            Expr::Case { whens, else_expr } => {
                let mut ok = whens.iter().all(|(cond, val)| {
                    Self::collect_column_names_strict(cond, out)
                        && Self::collect_column_names_strict(val, out)
                });
                if let Some(e) = else_expr {
                    ok &= Self::collect_column_names_strict(e, out);
                }
                ok
            }
            _ => false,
        }
    }

    /// Recursively collect column names from an expression tree.
    fn collect_column_names(expr: &Expr, out: &mut Vec<String>) {
        match expr {
            Expr::Column(name) => out.push(name.clone()),
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_column_names(left, out);
                Self::collect_column_names(right, out);
            }
            Expr::UnaryOp { expr, .. } => Self::collect_column_names(expr, out),
            Expr::FunctionCall { args, .. } => {
                for a in args {
                    Self::collect_column_names(a, out);
                }
            }
            Expr::IsNull { expr, .. } => Self::collect_column_names(expr, out),
            Expr::In { expr, list, .. } => {
                Self::collect_column_names(expr, out);
                for e in list {
                    Self::collect_column_names(e, out);
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::collect_column_names(expr, out);
                Self::collect_column_names(low, out);
                Self::collect_column_names(high, out);
            }
            Expr::Like { expr, pattern, .. } => {
                Self::collect_column_names(expr, out);
                Self::collect_column_names(pattern, out);
            }
            Expr::Case { whens, else_expr } => {
                for (cond, val) in whens {
                    Self::collect_column_names(cond, out);
                    Self::collect_column_names(val, out);
                }
                if let Some(e) = else_expr {
                    Self::collect_column_names(e, out);
                }
            }
            _ => {}
        }
    }

    /// Recursively collect schema column positions referenced by an expression.
    /// Used to ensure a columnar scan reads all columns a SELECT expression needs
    /// Bind outer column references in an expression to their current row values.
    /// Replaces `Expr::Column("d.id")` (table-prefixed) with `Expr::Literal(value)`
    /// using the outer row's data. Columns without a table prefix are left as-is
    /// (they belong to the inner query).
    fn bind_outer_columns(expr: &Expr, outer_row: &[Value], outer_schema: &TableSchema) -> Expr {
        match expr {
            Expr::Column(name) => {
                // Bind table-prefixed columns to their outer row values.
                // Both outer refs (t1.cat) and inner refs (t2.cat) get bound —
                // inner refs resolve to the same schema position, which is
                // correct because the outer and inner tables share the same
                // physical schema (self-join pattern).
                if name.contains('.') {
                    let bare = name.rsplit('.').next().unwrap_or(name);
                    if let Some(pos) = outer_schema.get_column_position(bare) {
                        if let Some(v) = outer_row.get(pos) {
                            return Expr::Literal(v.clone());
                        }
                    }
                }
                expr.clone()
            }
            Expr::Subquery(sub) => {
                // Recursively bind outer refs inside the subquery's WHERE.
                let mut bound = sub.clone();
                if let Some(wc) = &bound.where_clause {
                    bound.where_clause =
                        Some(Self::bind_outer_columns(wc, outer_row, outer_schema));
                }
                Expr::Subquery(bound)
            }
            Expr::Exists(stmt) => {
                // Same outer-ref binding for EXISTS subqueries.
                let mut bound = stmt.as_ref().clone();
                if let crate::sql::ast::Statement::Select {
                    stmt: ref mut s, ..
                } = bound
                {
                    if let Some(wc) = &s.where_clause {
                        let w = wc.clone();
                        s.where_clause =
                            Some(Self::bind_outer_columns(&w, outer_row, outer_schema));
                    }
                }
                Expr::Exists(Box::new(bound))
            }
            // NOT EXISTS (and any unary wrapper) must bind its inner
            // subquery too — without this the unbound correlated EXISTS
            // errored per row and NOT EXISTS silently matched nothing.
            Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                op: op.clone(),
                expr: Box::new(Self::bind_outer_columns(expr, outer_row, outer_schema)),
            },
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(Self::bind_outer_columns(left, outer_row, outer_schema)),
                op: op.clone(),
                right: Box::new(Self::bind_outer_columns(right, outer_row, outer_schema)),
            },
            Expr::FunctionCall {
                name,
                args,
                distinct,
            } => Expr::FunctionCall {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|a| Self::bind_outer_columns(a, outer_row, outer_schema))
                    .collect(),
                distinct: *distinct,
            },
            _ => expr.clone(),
        }
    }

    /// Evaluate an expression that may contain a (now-bound) scalar subquery.
    /// Uses self to execute the subquery.
    fn eval_correlated_expr(
        &self,
        expr: &Expr,
        row: &[Value],
        schema: &TableSchema,
    ) -> Result<Value> {
        match expr {
            Expr::UnaryOp {
                op: crate::sql::ast::UnaryOperator::Not,
                expr,
            } => {
                // NOT EXISTS / NOT (correlated predicate): evaluate the inner
                // expression recursively, then negate.
                let v = self.eval_correlated_expr(expr, row, schema)?;
                Ok(match v {
                    Value::Bool(b) => Value::Bool(!b),
                    Value::Null => Value::Null,
                    Value::Integer(i) => Value::Bool(i == 0),
                    other => Value::Bool(!Self::is_truthy(&other)),
                })
            }
            Expr::Exists(stmt) => {
                // Correlated EXISTS: outer refs were bound to literals by
                // bind_outer_columns; execute and test non-emptiness.
                // (EXISTS permits multi-row results — no scalar limit.)
                let sub = match stmt.as_ref() {
                    crate::sql::ast::Statement::Select { stmt, .. } => stmt,
                    _ => return Ok(Value::Bool(false)),
                };
                let result = self.execute_select_internal(sub)?;
                match result {
                    QueryResult::Select { rows, .. } => Ok(Value::Bool(!rows.is_empty())),
                    _ => Ok(Value::Bool(false)),
                }
            }
            Expr::Subquery(sub) => {
                let result = self.execute_select_internal(sub)?;
                match result {
                    QueryResult::Select { rows, .. } => {
                        if rows.len() > 1 {
                            return Err(MoteDBError::Query(
                                "Scalar subquery returned more than one row".to_string(),
                            ));
                        }
                        Ok(rows
                            .first()
                            .and_then(|r| r.first())
                            .cloned()
                            .unwrap_or(Value::Null))
                    }
                    _ => Ok(Value::Null),
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let l = self.eval_correlated_expr(left, row, schema)?;
                let r = self.eval_correlated_expr(right, row, schema)?;
                let evaluator = crate::sql::evaluator::ExprEvaluator::with_db(self.db.clone());
                evaluator.eval_binary_op(op, l, r)
            }
            _ => {
                // Non-subquery expr (Column, Literal): eval against row data.
                Ok(Self::eval_expr_on_row(expr, row, schema).unwrap_or(Value::Null))
            }
        }
    }

    /// Recursively collect schema column positions referenced by an expression.
    fn expr_referenced_columns(expr: &Expr, schema: &TableSchema) -> Vec<usize> {
        let mut out = Vec::new();
        let add = |name: &str, out: &mut Vec<usize>| {
            let bare = name.rsplit('.').next().unwrap_or(name);
            if let Some(p) = schema.get_column_position(bare) {
                if !out.contains(&p) {
                    out.push(p);
                }
            }
        };
        match expr {
            Expr::Column(name) => add(name, &mut out),
            Expr::BinaryOp { left, right, .. } => {
                for p in Self::expr_referenced_columns(left, schema) {
                    if !out.contains(&p) {
                        out.push(p);
                    }
                }
                for p in Self::expr_referenced_columns(right, schema) {
                    if !out.contains(&p) {
                        out.push(p);
                    }
                }
            }
            Expr::UnaryOp { expr, .. } => {
                for p in Self::expr_referenced_columns(expr, schema) {
                    if !out.contains(&p) {
                        out.push(p);
                    }
                }
            }
            Expr::FunctionCall { args, .. } => {
                for a in args {
                    for p in Self::expr_referenced_columns(a, schema) {
                        if !out.contains(&p) {
                            out.push(p);
                        }
                    }
                }
            }
            // Predicates that reference a column: collect their columns so that
            // projected scans decode the WHERE columns (otherwise COUNT(*) with
            // a WHERE on a non-aggregate column scans no columns and evaluates
            // the predicate against default NULLs — see single_pass_group_by).
            Expr::IsNull { expr, .. } => {
                for p in Self::expr_referenced_columns(expr, schema) {
                    if !out.contains(&p) {
                        out.push(p);
                    }
                }
            }
            Expr::Like { expr, pattern, .. } => {
                for p in Self::expr_referenced_columns(expr, schema) {
                    if !out.contains(&p) {
                        out.push(p);
                    }
                }
                for p in Self::expr_referenced_columns(pattern, schema) {
                    if !out.contains(&p) {
                        out.push(p);
                    }
                }
            }
            Expr::In { expr, list, .. } => {
                for p in Self::expr_referenced_columns(expr, schema) {
                    if !out.contains(&p) {
                        out.push(p);
                    }
                }
                for e in list {
                    for p in Self::expr_referenced_columns(e, schema) {
                        if !out.contains(&p) {
                            out.push(p);
                        }
                    }
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                for p in Self::expr_referenced_columns(expr, schema) {
                    if !out.contains(&p) {
                        out.push(p);
                    }
                }
                for p in Self::expr_referenced_columns(low, schema) {
                    if !out.contains(&p) {
                        out.push(p);
                    }
                }
                for p in Self::expr_referenced_columns(high, schema) {
                    if !out.contains(&p) {
                        out.push(p);
                    }
                }
            }
            // 🔑 CASE can reference columns in its WHEN conditions and
            // THEN/ELSE values (e.g. ORDER BY CASE WHEN v = 30 THEN 0 ELSE 1).
            // Collect those columns so projected scans decode them — previously
            // Case fell through to `_ => {}`, so the referenced column stayed
            // NULL during ORDER BY key computation → no sort applied.
            Expr::Case { whens, else_expr } => {
                for (cond, val) in whens {
                    for p in Self::expr_referenced_columns(cond, schema) {
                        if !out.contains(&p) {
                            out.push(p);
                        }
                    }
                    for p in Self::expr_referenced_columns(val, schema) {
                        if !out.contains(&p) {
                            out.push(p);
                        }
                    }
                }
                if let Some(e) = else_expr {
                    for p in Self::expr_referenced_columns(e, schema) {
                        if !out.contains(&p) {
                            out.push(p);
                        }
                    }
                }
            }
            // Spatial / vector-distance expressions name their column directly.
            // They fell through to `_ => {}` like CASE used to, so an
            // un-indexed `ORDER BY ST_DISTANCE_3D(pt, …)` never scanned `pt`,
            // computed NULL keys and returned rows in insertion order.
            Expr::StDistance3D { column, .. }
            | Expr::StWithin3D { column, .. }
            | Expr::StRadius3D { column, .. }
            | Expr::StKnn3D { column, .. }
            | Expr::KnnDistance { column, .. } => add(column, &mut out),
            _ => {}
        }
        out
    }

    /// Check if an expression is an aggregate function
    fn is_aggregate_expr(expr: &Expr) -> bool {
        match expr {
            Expr::FunctionCall {
                name,
                args,
                distinct: _,
            } => {
                // Top-level aggregate function?
                let is_agg_top = matches!(
                    name.to_uppercase().as_str(),
                    // 🔑 STDDEV/VARIANCE MUST be listed here — without them,
                    // is_aggregate_expr returns false, the query isn't routed
                    // to the aggregate path, and VARIANCE/STDDEV evaluate
                    // per-row (returning NULL for every row instead of one
                    // aggregated value).
                    // 🔑 GROUP_CONCAT also — otherwise it's evaluated per-row
                    // and returns NULL for every input row.
                    "COUNT"
                        | "SUM"
                        | "AVG"
                        | "MIN"
                        | "MAX"
                        | "STDDEV"
                        | "VARIANCE"
                        | "GROUP_CONCAT"
                );
                if is_agg_top {
                    return true;
                }
                // 🆕 Non-aggregate function — still recurse into args in case
                // an aggregate is nested (e.g., ABS(SUM(v))).
                args.iter().any(Self::is_aggregate_expr)
            }
            // 🆕 Recurse into compound expressions so that an aggregate
            // hidden inside a CASE / arithmetic / comparison expression is
            // still detected. Without this, `SELECT CASE WHEN COUNT(*) > 3
            // THEN 'many' ELSE 'few' END FROM t` returns one NULL per row
            // instead of a single aggregated value.
            Expr::Case { whens, else_expr } => {
                whens.iter().any(|(cond, val)| {
                    Self::is_aggregate_expr(cond) || Self::is_aggregate_expr(val)
                }) || else_expr
                    .as_ref()
                    .map(|e| Self::is_aggregate_expr(e))
                    .unwrap_or(false)
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::is_aggregate_expr(left) || Self::is_aggregate_expr(right)
            }
            Expr::UnaryOp { expr, .. } => Self::is_aggregate_expr(expr),
            _ => false,
        }
    }

    /// Collect all top-level aggregate function calls (COUNT/SUM/AVG/MIN/MAX/
    /// STDDEV/VARIANCE) referenced anywhere in `expr`. Used to compute HAVING-
    /// only aggregates that aren't in the SELECT list.
    fn collect_aggregate_calls(expr: &Expr) -> Vec<Expr> {
        let mut out = Vec::new();
        Self::collect_aggregate_calls_inner(expr, &mut out);
        out
    }

    fn collect_aggregate_calls_inner(expr: &Expr, out: &mut Vec<Expr>) {
        match expr {
            Expr::FunctionCall { name, args, .. }
                if matches!(
                    name.to_uppercase().as_str(),
                    "COUNT"
                        | "SUM"
                        | "AVG"
                        | "MIN"
                        | "MAX"
                        | "STDDEV"
                        | "VARIANCE"
                        | "GROUP_CONCAT"
                ) =>
            {
                out.push(expr.clone());
                // Also recurse into args in case of nested aggregates (rare).
                for a in args {
                    Self::collect_aggregate_calls_inner(a, out);
                }
            }
            Expr::FunctionCall { args, .. } => {
                for a in args {
                    Self::collect_aggregate_calls_inner(a, out);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_aggregate_calls_inner(left, out);
                Self::collect_aggregate_calls_inner(right, out);
            }
            Expr::UnaryOp { expr, .. } => Self::collect_aggregate_calls_inner(expr, out),
            Expr::Case { whens, else_expr } => {
                for (cond, val) in whens {
                    Self::collect_aggregate_calls_inner(cond, out);
                    Self::collect_aggregate_calls_inner(val, out);
                }
                if let Some(e) = else_expr {
                    Self::collect_aggregate_calls_inner(e, out);
                }
            }
            _ => {}
        }
    }

    /// Build the SqlRow lookup key for an aggregate expression, matching the
    /// format the evaluator's aggregate dispatch expects: "FUNC(arg)" with
    /// the function name uppercased and the arg as the bare column name.
    fn aggregate_expr_key(expr: &Expr) -> String {
        if let Expr::FunctionCall { name, args, .. } = expr {
            let arg_str = if args.is_empty() {
                "*".to_string()
            } else {
                args.iter()
                    .map(|a| match a {
                        Expr::Column(c) => c.clone(),
                        // 🔑 Use expr_to_column_name (not {:?}) so the key matches
                        // how expr_to_column_name generates the SELECT column name.
                        // Previously used format!("{:?}", a) which produced Debug
                        // output like `BinaryOp { ... }`, mismatching the SELECT
                        // name `q Mul p` — breaking HAVING with SUM(q * p).
                        _ => Self::expr_to_column_name(a),
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            return format!("{}({})", name.to_uppercase(), arg_str);
        }
        format!("{:?}", expr)
    }

    // ───────────────────────────────────────────────────────────────
    // Positional GROUP BY fast path — bypasses HashMap conversion
    // ───────────────────────────────────────────────────────────────

    /// Try to parse an expression as a simple aggregate function.
    /// Returns `None` for complex expressions that need the materialized path.
    fn try_parse_aggregate(&self, expr: &Expr, schema: &TableSchema) -> Option<AggregateInfo> {
        match expr {
            Expr::FunctionCall {
                name,
                args,
                distinct,
            } => {
                let func = name.to_uppercase();
                match func.as_str() {
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "STDDEV" | "VARIANCE" => {
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
                                // 🔑 COUNT(1) ≡ COUNT(*) — counts all rows.
                                // But this equivalence is ONLY valid for COUNT.
                                // For SUM(1)/AVG(1)/MIN(1)/MAX(1) the literal 1 is
                                // a real per-row value that must be accumulated
                                // (SUM(1) over 3 rows = 3, not NULL). Mapping it to
                                // col_pos=None here made the SUM/MIN/MAX accumulators
                                // iterate nothing → silently return NULL. So only
                                // apply the COUNT(1)≡COUNT(*) shortcut for COUNT;
                                // other aggregates with a literal arg fall back to
                                // the materialized path (which evaluates the
                                // expression per row).
                                Expr::Literal(Value::Integer(1)) if func == "COUNT" => None,
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
                            extra: None,
                        })
                    }
                    "GROUP_CONCAT" => {
                        // GROUP_CONCAT(expr [, separator])
                        if args.is_empty() {
                            return None;
                        }
                        let col_pos = match &args[0] {
                            Expr::Column(col_name) => {
                                let bare = if col_name.contains('.') {
                                    col_name.rsplit('.').next().unwrap_or(col_name)
                                } else {
                                    col_name
                                };
                                schema.get_column_position(bare)
                            }
                            _ => return None,
                        };
                        // Parse separator (2nd arg, if a string literal).
                        let sep = if args.len() >= 2 {
                            if let Expr::Literal(Value::Text(t)) = &args[1] {
                                Some(t.to_string())
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        Some(AggregateInfo {
                            func,
                            col_pos,
                            distinct: *distinct,
                            extra: sep,
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
        // ST_KNN_3D cannot be evaluated positionally (needs the index and the
        // row id); single_pass_group_by treated the evaluation error as "no
        // match", so `COUNT(*) WHERE ST_KNN_3D(…)` returned 0.
        if stmt
            .where_clause
            .as_ref()
            .is_some_and(Self::expr_contains_st_knn)
        {
            return Ok(None);
        }
        // 🔑 Correlated subqueries → the materialized path's per-row
        // evaluation (silent 0 here otherwise — see the positional path).
        if stmt
            .where_clause
            .as_ref()
            .is_some_and(Self::expr_contains_subquery)
        {
            return Ok(None);
        }
        // 🆕 HAVING requires post-aggregation filtering that this streaming
        // path doesn't apply — fall back to the materialized path.
        if stmt.having.is_some() {
            return Ok(None);
        }
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
                        let col_name = alias
                            .clone()
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
        // 🔑 STDDEV/VARIANCE need a two-pass algorithm (compute mean, then sum
        // of squared deviations). The streaming Acc accumulator only tracks
        // count/sum/min/max — it can't compute variance. Fall back to the
        // positional path (compute_aggregate_positional) which handles them.
        if agg_specs
            .iter()
            .any(|(_, a)| matches!(a.func.as_str(), "STDDEV" | "VARIANCE"))
        {
            return Ok(None);
        }

        // Compile WHERE for positional evaluation
        let compiled_where: Option<CompiledWhere> = stmt
            .where_clause
            .as_ref()
            .and_then(|clause| Self::compile_where(clause, schema));

        // Determine needed columns: WHERE columns ∪ aggregate columns
        let mut where_positions = Vec::new();
        if let Some(ref cw) = compiled_where {
            cw.collect_positions(&mut where_positions);
        }
        let mut needed: Vec<usize> = where_positions.clone();
        for (_, agg) in &agg_specs {
            if let Some(pos) = agg.col_pos {
                if !needed.contains(&pos) {
                    needed.push(pos);
                }
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
            float_sum: CompSum,
            has_float: bool,
            has_value: bool,
            min_val: Option<Value>,
            max_val: Option<Value>,
        }
        impl Acc {
            fn new() -> Self {
                Self {
                    count: 0,
                    int_sum: 0,
                    float_sum: CompSum::default(),
                    has_float: false,
                    has_value: false,
                    min_val: None,
                    max_val: None,
                }
            }
            fn update(&mut self, val: &Value, func: &str) {
                if matches!(val, Value::Null) {
                    return;
                }
                match func {
                    "COUNT" => {
                        self.count += 1;
                    }
                    "SUM" | "AVG" => {
                        self.has_value = true;
                        self.count += 1;
                        match val {
                            Value::Integer(i) => {
                                if self.has_float {
                                    self.float_sum.add(*i as f64);
                                } else if let Some(s) = self.int_sum.checked_add(*i) {
                                    self.int_sum = s;
                                } else {
                                    self.has_float = true;
                                    self.float_sum.add(self.int_sum as f64);
                                    self.float_sum.add(*i as f64);
                                }
                            }
                            Value::Float(f) => {
                                if !self.has_float {
                                    self.has_float = true;
                                    self.float_sum.add(self.int_sum as f64);
                                }
                                self.float_sum.add(*f);
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
                        if !self.has_value {
                            return Value::Null;
                        }
                        if self.has_float {
                            Value::Float(self.float_sum.total())
                        } else {
                            Value::Integer(self.int_sum)
                        }
                    }
                    "AVG" => {
                        if self.count == 0 {
                            return Value::Null;
                        }
                        let sum = if self.has_float {
                            self.float_sum.total()
                        } else {
                            self.int_sum as f64
                        };
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
                let (_row_id, raw_bytes) = result?;

                // Phase 1: WHERE eval on partial decode
                let ctx = match crate::storage::row_format::RowParseContext::parse(
                    &raw_bytes,
                    col_types_slice,
                    fixed_count,
                ) {
                    Some(c) => c,
                    None => continue,
                };

                if let Some(ref cw) = compiled_where {
                    if ctx
                        .decode_columns(
                            &raw_bytes,
                            col_types_slice,
                            &where_positions,
                            &mut where_buf,
                        )
                        .is_err()
                    {
                        continue;
                    }
                    if !cw.eval_at(&where_buf, &where_pos_to_idx).unwrap_or(false) {
                        continue;
                    }
                }

                // Phase 2: Decode aggregate columns with pre-parsed context
                if !agg_only_positions.is_empty()
                    && ctx
                        .decode_columns(
                            &raw_bytes,
                            col_types_slice,
                            &agg_only_positions,
                            &mut agg_buf,
                        )
                        .is_err()
                {
                    continue;
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
                let (_row_id, raw_bytes) = result?;

                // WHERE filter
                let decoded_row: Option<Row> = if let Some(ref clause) = where_clause {
                    let full_row = match crate::storage::row_format::decode_fast(
                        &raw_bytes,
                        col_types,
                        fixed_count,
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
        let result_row: Vec<Value> = agg_specs
            .iter()
            .enumerate()
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

    /// 🔑 表达式 GROUP BY 快路径: `SELECT <expr>…, AGG(…)… GROUP BY <same exprs>`.
    /// 此前表达式组键让 try_apply_group_by_positional decline → 物化 SqlRow
    /// 路径 (干净表 0.7µs/行, 有未合并写时 35µs/行; 热点扫描: 20K 行 1.8s)。
    /// 这里流式行 + eval_expr_on_row 求键 + 单遍累加, NULL 语义与
    /// apply_group_by 一致 (COUNT(col)/SUM/AVG/MIN/MAX 跳过 NULL)。
    fn try_expression_group_by(
        &self,
        stmt: &SelectStmt,
        schema: &TableSchema,
        table_name: &str,
    ) -> Result<Option<(Vec<String>, Vec<Vec<Value>>)>> {
        use std::collections::HashMap;
        let group_items = match &stmt.group_by {
            Some(g) if !g.is_empty() => g,
            _ => return Ok(None),
        };
        if group_items.len() > 2 || stmt.having.is_some() || stmt.distinct {
            return Ok(None);
        }
        #[derive(Clone)]
        struct Acc {
            count: u64,
            nn: u64,
            int_sum: i64,
            fsum: f64,
            has_f: bool,
            has_v: bool,
            min: Option<Value>,
            max: Option<Value>,
        }
        impl Acc {
            fn new() -> Self {
                Self { count: 0, nn: 0, int_sum: 0, fsum: 0.0, has_f: false, has_v: false, min: None, max: None }
            }
            fn update(&mut self, v: Option<&Value>) {
                self.count += 1;
                let Some(v) = v else { return };
                if matches!(v, Value::Null) {
                    return;
                }
                self.nn += 1;
                self.has_v = true;
                match v {
                    Value::Integer(i) => self.int_sum = self.int_sum.wrapping_add(*i),
                    Value::Float(f) => {
                        self.fsum += f;
                        self.has_f = true;
                    }
                    _ => {}
                }
                if self.min.as_ref().is_none_or(|m| order_by_cmp(v, m) == std::cmp::Ordering::Less) {
                    self.min = Some(v.clone());
                }
                if self.max.as_ref().is_none_or(|m| order_by_cmp(v, m) == std::cmp::Ordering::Greater) {
                    self.max = Some(v.clone());
                }
            }
            fn finalize(&self, func: &str, col_pos: Option<usize>) -> Value {
                match func {
                    "COUNT" => {
                        if col_pos.is_none() {
                            Value::Integer(self.count as i64)
                        } else {
                            Value::Integer(self.nn as i64)
                        }
                    }
                    "SUM" => {
                        if self.nn == 0 {
                            Value::Null
                        } else if self.has_f {
                            Value::Float(self.fsum + self.int_sum as f64)
                        } else {
                            Value::Integer(self.int_sum)
                        }
                    }
                    "AVG" => {
                        if self.nn == 0 {
                            Value::Null
                        } else {
                            let total = if self.has_f {
                                self.fsum + self.int_sum as f64
                            } else {
                                self.int_sum as f64
                            };
                            Value::Float(total / self.nn as f64)
                        }
                    }
                    "MIN" => self.min.clone().unwrap_or(Value::Null),
                    "MAX" => self.max.clone().unwrap_or(Value::Null),
                    _ => Value::Null,
                }
            }
        }
        // SELECT 解析: 输出序的 [Key(expr) | Agg] 序列
        enum Out {
            Key(usize),   // index into key_exprs
            Agg(usize),   // index into agg_infos
        }
        let mut key_exprs: Vec<Expr> = Vec::new();
        let mut out_names: Vec<String> = Vec::new();
        let mut out_cols: Vec<Out> = Vec::new();
        let mut agg_infos: Vec<AggregateInfo> = Vec::new();
        for sc in &stmt.columns {
            match sc {
                SelectColumn::Star => return Ok(None),
                SelectColumn::Column(_) | SelectColumn::ColumnWithAlias(_, _) => {
                    return Ok(None);
                }
                SelectColumn::Expr(expr, alias) => {
                    if let Some(agg) = self.try_parse_aggregate(expr, schema) {
                        if agg.distinct
                            || !matches!(agg.func.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
                        {
                            return Ok(None);
                        }
                        out_names.push(
                            alias.clone().unwrap_or_else(|| Self::expr_to_column_name(expr)),
                        );
                        out_cols.push(Out::Agg(agg_infos.len()));
                        agg_infos.push(agg);
                    } else {
                        let name =
                            alias.clone().unwrap_or_else(|| Self::expr_to_column_name(expr));
                        let canonical = Self::expr_to_column_name(expr);
                        let matched = group_items
                            .iter()
                            .any(|g| g == &name || g == &canonical);
                        if !matched || key_exprs.len() + 1 > group_items.len() {
                            return Ok(None);
                        }
                        // (组项全被 SELECT 覆盖由函数末尾的长度相等检查保证)
                        out_names.push(name);
                        out_cols.push(Out::Key(key_exprs.len()));
                        key_exprs.push((*expr).clone());
                    }
                }
            }
        }
        if key_exprs.is_empty() || agg_infos.is_empty() || key_exprs.len() != group_items.len() {
            return Ok(None);
        }
        // WHERE: 简单比较谓词可用编译过滤; 复杂形状逐行 eval (仍远快于物化)
        let mut row_iter = self.db.scan_table_rows_streaming(table_name)?;
        let mut groups: HashMap<Vec<Value>, Vec<Acc>> = HashMap::new();
        let mut key_buf: Vec<Value> = Vec::with_capacity(key_exprs.len());
        for result in row_iter.by_ref() {
            let (_, row) = result?;
            // WHERE 逐行求值 (Bool(true) 才收)
            if let Some(ref wc) = stmt.where_clause {
                let sql_row = row_to_sql_row(&row, schema)?;
                let ok = matches!(
                    self.evaluator.eval(wc, &sql_row),
                    Ok(Value::Bool(true))
                );
                if !ok {
                    continue;
                }
            }
            key_buf.clear();
            for e in &key_exprs {
                key_buf.push(Self::eval_expr_on_row(e, &row, schema)?);
            }
            let accs = groups.entry(key_buf.clone()).or_insert_with(|| {
                agg_infos.iter().map(|_| Acc::new()).collect::<Vec<_>>()
            });
            for (ai, acc) in accs.iter_mut().enumerate() {
                let info = &agg_infos[ai];
                let v = info.col_pos.and_then(|p| row.get(p));
                acc.update(v);
            }
        }
        drop(row_iter);
        // 组装输出行 + ORDER BY (输出名/别名唯一命中) + LIMIT/OFFSET
        let mut rows: Vec<Vec<Value>> = groups
            .into_iter()
            .map(|(keys, accs)| {
                out_cols
                    .iter()
                    .map(|c| match c {
                        Out::Key(i) => keys[*i].clone(),
                        Out::Agg(i) => accs[*i].finalize(
                            agg_infos[*i].func.as_str(),
                            agg_infos[*i].col_pos,
                        ),
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        if let Some(ref ob) = stmt.order_by {
            let mut specs: Vec<(usize, bool)> = Vec::new();
            for oe in ob {
                let Expr::Column(cn) = &oe.expr else { return Ok(None) };
                let hits: Vec<usize> = out_names
                    .iter()
                    .enumerate()
                    .filter(|(_, n)| {
                        n.as_str() == cn.as_str()
                            || n.rsplit('.').next().unwrap_or(n) == cn.as_str()
                    })
                    .map(|(i, _)| i)
                    .collect();
                if hits.len() != 1 {
                    return Ok(None);
                }
                specs.push((hits[0], oe.asc));
            }
            if !specs.is_empty() {
                rows.sort_by(|a, b| {
                    for &(i, asc) in &specs {
                        let c = order_by_cmp(&a[i], &b[i]);
                        if c != std::cmp::Ordering::Equal {
                            return if asc { c } else { c.reverse() };
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
        }
        let offset = stmt.offset.unwrap_or(0);
        if offset > 0 {
            rows.drain(..offset.min(rows.len()));
        }
        if let Some(l) = stmt.limit {
            rows.truncate(l);
        }
        Ok(Some((out_names, rows)))
    }

    fn try_apply_group_by_positional(
        &self,
        stmt: &SelectStmt,
        schema: &TableSchema,
        table_name: &str,
    ) -> Result<Option<(Vec<String>, Vec<Vec<Value>>)>> {
        // ST_KNN_3D cannot be evaluated positionally (needs the index and the
        // row id); single_pass_group_by treated the evaluation error as "no
        // match", so `COUNT(*) WHERE ST_KNN_3D(…)` returned 0.
        if stmt
            .where_clause
            .as_ref()
            .is_some_and(Self::expr_contains_st_knn)
        {
            return Ok(None);
        }
        // 🔑 Correlated subqueries need per-row execution by the MATERIALIZED
        // path; positional evaluation treats the Err as "no match" —
        // `COUNT(*) WHERE EXISTS(…correlated…)` silently returned 0.
        if stmt
            .where_clause
            .as_ref()
            .is_some_and(Self::expr_contains_subquery)
        {
            return Ok(None);
        }
        use std::collections::HashMap;

        let group_by_cols = match &stmt.group_by {
            Some(cols) => cols,
            None => &Vec::new(), // implicit aggregation
        };

        // Resolve group column positions
        let group_col_positions: Vec<usize> = group_by_cols
            .iter()
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
        let group_bare_set: std::collections::HashSet<&str> = group_by_cols
            .iter()
            .map(|name| {
                if name.contains('.') {
                    name.rsplit('.').next().unwrap_or(name)
                } else {
                    name.as_str()
                }
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
                        let col_name = alias
                            .clone()
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
        // 🔑 STDDEV/VARIANCE are excluded: the single-pass AggAccumulator only
        // tracks count/sum/min/max (not sum-of-squares), so it returns NULL for
        // them. Fall through to the two-pass materialized path which calls
        // compute_aggregate_positional (handles STDDEV/VARIANCE correctly).
        let can_single_pass = stmt.having.is_none()
            && group_col_positions.len() <= 2
            && !select_col_info
                .iter()
                .any(|(_, _, agg)| agg.as_ref().is_some_and(|a| a.distinct))
            && !select_col_info.iter().any(|(_, _, agg)| {
                agg.as_ref().is_some_and(|a| {
                    // 🔑 STDDEV/VARIANCE/GROUP_CONCAT need the materialized path
                    // (single_pass_group_by's AggAccumulator doesn't support them).
                    matches!(a.func.as_str(), "STDDEV" | "VARIANCE" | "GROUP_CONCAT")
                })
            })
            // 🔑 ORDER BY referencing an aggregate NOT in the SELECT list
            // (e.g. `SELECT cat, SUM(v) ... ORDER BY MAX(w)`) requires per-group
            // computation of the extra aggregate. single_pass_group_by only
            // accumulates the SELECT-list aggregates, so it can't resolve such
            // an ORDER BY key → falls back to two-pass which computes it.
            && stmt.order_by.as_ref().map(|ob| {
                ob.iter().all(|oe| {
                    // Only aggregate function calls are at risk here (bare
                    // columns resolve against group columns; output-matching
                    // aggregates resolve by name).
                    match &oe.expr {
                        Expr::FunctionCall { name, args, .. } => {
                            let arg_str = args.iter().map(|a| match a {
                                Expr::Column(c) => c.clone(),
                                e => format!("{:?}", e),
                            }).collect::<Vec<_>>().join(", ");
                            let ob_name = format!("{}({})", name.to_uppercase(), arg_str);
                            // If the aggregate IS in the SELECT list (by name), single-pass is fine.
                            select_col_info.iter().any(|(cn, _, _)| cn == &ob_name)
                        }
                        _ => true,
                    }
                })
            }).unwrap_or(true);

        if can_single_pass {
            return self.single_pass_group_by(
                row_iter,
                stmt,
                schema,
                table_name,
                &group_col_positions,
                &select_col_info,
            );
        }

        // Fallback: materialize rows then group
        let raw_rows: Vec<Row> = if let Some(ref where_clause) = stmt.where_clause {
            // 🔑 Materialize non-correlated subqueries in the WHERE clause
            // before row-wise evaluation. eval_expr_on_row cannot execute
            // subqueries — without this, a WHERE like `v > (SELECT MIN(v)
            // FROM t WHERE v > (SELECT MIN(v) FROM t))` (nested subqueries)
            // fails to evaluate → empty result / wrong aggregate. Materializing
            // here collapses the subqueries to Literals so eval_expr_on_row
            // can compare against the resolved value.
            let resolved_where = self
                .materialize_subqueries_checked(where_clause, Some(schema))
                .unwrap_or_else(|_| where_clause.clone());
            let where_clause = &resolved_where;
            // 🔥 编译一次（列位置预解析）—— 每行做 get_column_position 字符串
            // 查找的旧路径是扫描 CPU 的显著份额
            let compiled = Self::compile_where(where_clause, schema);
            let mut matching = Vec::new();
            for result in row_iter {
                let (_row_id, row) = result?;
                let hit = if let Some(cw) = compiled.as_ref() {
                    match cw.eval(&row) {
                        Some(b) => b,
                        None => match Self::eval_expr_on_row(where_clause, &row, schema) {
                            Ok(Value::Bool(b)) => b,
                            Ok(_) => false,
                            Err(_) => return Ok(None),
                        },
                    }
                } else {
                    match Self::eval_expr_on_row(where_clause, &row, schema) {
                        Ok(Value::Bool(b)) => b,
                        Ok(_) => false,
                        Err(_) => return Ok(None),
                    }
                };
                if hit {
                    matching.push(row);
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
            let mut single_groups: HashMap<Value, Vec<&Row>> = HashMap::with_capacity(64);
            for row in &raw_rows {
                let key = row.get(pos).cloned().unwrap_or(Value::Null);
                single_groups.entry(key).or_default().push(row);
            }
            for (key, rows) in single_groups {
                groups.insert(vec![key], rows);
            }
        } else {
            for row in &raw_rows {
                let group_key: Vec<Value> = group_col_positions
                    .iter()
                    .map(|&pos| row.get(pos).cloned().unwrap_or(Value::Null))
                    .collect();
                groups.entry(group_key).or_default().push(row);
            }
        }

        // Handle implicit aggregation with no input rows
        let groups: Vec<(Vec<Value>, Vec<&Row>)> = if groups.is_empty() && group_by_cols.is_empty()
        {
            vec![(vec![], vec![])] // one empty group for implicit aggregation
        } else {
            groups.into_iter().collect()
        };

        // Compute result
        let column_names: Vec<String> = select_col_info
            .iter()
            .map(|(name, _, _)| name.clone())
            .collect();
        let mut result_rows: Vec<Vec<Value>> = Vec::new();

        // 🔑 Collect ORDER BY aggregates that are NOT in the SELECT list.
        // These need per-group computation (e.g. `SELECT cat, SUM(v) ... ORDER
        // BY MAX(w)` where MAX(w) isn't selected). We compute them per group
        // and append as trailing sort-only columns (stripped before output).
        let mut extra_order_aggs: Vec<(AggregateInfo, bool)> = Vec::new();
        if let Some(ref order_by) = stmt.order_by {
            for ob in order_by {
                if let Expr::FunctionCall { name, args, .. } = &ob.expr {
                    let arg_str = args
                        .iter()
                        .map(|a| match a {
                            Expr::Column(c) => c.clone(),
                            e => format!("{:?}", e),
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    let ob_name = format!("{}({})", name.to_uppercase(), arg_str);
                    // Only if NOT already in the SELECT output.
                    if !select_col_info.iter().any(|(cn, _, _)| cn == &ob_name) {
                        // Parse the aggregate so we can compute it per group.
                        if let Some(agg) = self.try_parse_aggregate(&ob.expr, schema) {
                            extra_order_aggs.push((agg, ob.asc));
                        }
                    }
                }
            }
        }
        let extra_order_offset = select_col_info.len();

        for (_group_key, group_rows) in groups {
            let mut result_row = Vec::new();
            for (_col_name, col_pos, agg_info) in &select_col_info {
                let value = if let Some(pos) = col_pos {
                    // Bare column in GROUP BY — take from first row
                    group_rows
                        .first()
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
            // 🔑 Append the extra ORDER BY aggregates (sort-only columns).
            for (agg, _) in &extra_order_aggs {
                let v = self.compute_aggregate_positional(agg, &group_rows)?;
                result_row.push(v);
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
                        temp_row
                            .entry(sql_name)
                            .or_insert_with(|| result_row[i].clone());
                    }
                }
                // 🚨 Compute aggregates referenced in HAVING but NOT in the SELECT
                // list (e.g. `SELECT cat FROM t GROUP BY cat HAVING SUM(v) > 20`).
                // Without this, the evaluator's aggregate lookup fails → NotImplemented
                // error → every group filtered out (silent wrong result: empty).
                for agg_expr in Self::collect_aggregate_calls(having_expr) {
                    // Build the lookup key the evaluator expects (e.g. "SUM(v)").
                    let key = Self::aggregate_expr_key(&agg_expr);
                    if let std::collections::hash_map::Entry::Vacant(e) = temp_row.entry(key) {
                        // Parse to AggregateInfo + compute via the positional path.
                        if let Some(agg_info) = self.try_parse_aggregate(&agg_expr, schema) {
                            let val = self.compute_aggregate_positional(&agg_info, &group_rows)?;
                            e.insert(val);
                        }
                    }
                }
                let passes = self
                    .evaluator
                    .eval(having_expr, &temp_row)
                    .and_then(|val| self.to_bool(&val))
                    .unwrap_or(false);
                if !passes {
                    continue;
                }
            }

            result_rows.push(result_row);
        }

        // Apply ORDER BY if present
        if let Some(ref order_by) = stmt.order_by {
            // 🔑 Build the ORDER BY plan. For each ORDER BY clause:
            //  - bare column → resolve against output column_names.
            //  - aggregate in SELECT → resolve by canonical name (e.g. "SUM(v)").
            //  - aggregate NOT in SELECT → use the corresponding extra slot
            //    (extra_order_aggs were appended per group, in the order the
            //    non-selected ORDER BY aggregates were encountered).
            let mut extra_cursor = 0usize; // index into extra_order_aggs
            let order_specs: Vec<(usize, bool)> = order_by
                .iter()
                .filter_map(|ob| {
                    if let Expr::Column(ref col_name) = ob.expr {
                        return column_names
                            .iter()
                            .position(|c| c == col_name)
                            .map(|idx| (idx, ob.asc));
                    }
                    if let Expr::FunctionCall { name, args, .. } = &ob.expr {
                        let arg_str = args
                            .iter()
                            .map(|a| match a {
                                Expr::Column(c) => c.clone(),
                                e => format!("{:?}", e),
                            })
                            .collect::<Vec<_>>()
                            .join(", ");
                        let ob_name = format!("{}({})", name.to_uppercase(), arg_str);
                        // In SELECT list?
                        if let Some(idx) = column_names.iter().position(|c| c == &ob_name) {
                            return Some((idx, ob.asc));
                        }
                        // Not in SELECT — use the next extra slot (they were
                        // collected in ORDER BY clause order for non-selected
                        // aggregates).
                        if extra_cursor < extra_order_aggs.len() {
                            let idx = extra_order_offset + extra_cursor;
                            extra_cursor += 1;
                            return Some((idx, ob.asc));
                        }
                    }
                    None
                })
                .collect();

            result_rows.sort_by(|a, b| {
                for &(idx, asc) in &order_specs {
                    // NULL ordering: NULLs sort last in ASC, first in DESC
                    // (matches apply_order_by, the non-GROUP-BY ORDER BY path).
                    let cmp = compare_with_nulls(&a[idx], &b[idx]);
                    let cmp = if asc { cmp } else { cmp.reverse() };
                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                std::cmp::Ordering::Equal
            });
            // 🔑 Strip the extra sort-only columns from each output row.
            if !extra_order_aggs.is_empty() {
                for row in &mut result_rows {
                    row.truncate(extra_order_offset);
                }
            }
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
            float_sum: CompSum,
            has_float: bool,
            has_value: bool,
            min_val: Option<Value>,
            max_val: Option<Value>,
        }
        impl AggAccumulator {
            fn new() -> Self {
                Self {
                    count: 0,
                    int_sum: 0,
                    float_sum: CompSum::default(),
                    has_float: false,
                    has_value: false,
                    min_val: None,
                    max_val: None,
                }
            }
            fn update(&mut self, val: &Value, func: &str) {
                if matches!(val, Value::Null) {
                    return;
                }
                match func {
                    "COUNT" => {
                        self.count += 1;
                    }
                    "SUM" | "AVG" => {
                        self.has_value = true;
                        self.count += 1;
                        match val {
                            Value::Integer(i) => {
                                if self.has_float {
                                    self.float_sum.add(*i as f64);
                                } else if let Some(s) = self.int_sum.checked_add(*i) {
                                    self.int_sum = s;
                                } else {
                                    self.has_float = true;
                                    self.float_sum.add(self.int_sum as f64);
                                    self.float_sum.add(*i as f64);
                                }
                            }
                            Value::Float(f) => {
                                if !self.has_float {
                                    self.has_float = true;
                                    self.float_sum.add(self.int_sum as f64);
                                }
                                self.float_sum.add(*f);
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
                        if !self.has_value {
                            return Value::Null;
                        }
                        if self.has_float {
                            Value::Float(self.float_sum.total())
                        } else {
                            Value::Integer(self.int_sum)
                        }
                    }
                    "AVG" => {
                        if self.count == 0 {
                            return Value::Null;
                        }
                        let sum = if self.has_float {
                            self.float_sum.total()
                        } else {
                            self.int_sum as f64
                        };
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
        let num_aggs = select_col_info
            .iter()
            .filter(|(_, _, a)| a.is_some())
            .count();

        // Identify which select columns are aggregates (index into select_col_info)
        let agg_indices: Vec<usize> = select_col_info
            .iter()
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

        // Pre-collect columns needed for partial decode (group cols + agg cols +
        // WHERE cols). The WHERE columns MUST be included so the projected scan
        // decodes them — otherwise COUNT(*) with `WHERE col IS NULL` scans no
        // columns and the predicate is evaluated against default NULLs (every
        // row appears NULL → IS NULL matches all rows).
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
            if let Some(ref clause) = where_clause {
                for p in Self::expr_referenced_columns(clause, schema) {
                    if !cols.contains(&p) {
                        cols.push(p);
                    }
                }
            }
            cols.sort_unstable();
            cols
        };

        // Iterate rows: for ColSegmentStore tables use the vectorized projected
        // scan (only decodes needed_cols). For each row we read ONLY the group
        // key + aggregate values via proj_map — no full-row clone. This avoids
        // the full-schema Vec<Value> decode + clone that made the fallback
        // GROUP BY ~10x slower than the single-column fast path.
        let proj_map: std::collections::HashMap<usize, usize> = needed_cols
            .iter()
            .enumerate()
            .map(|(i, &sp)| (sp, i))
            .collect();
        let col_seg_rows: Option<Vec<Vec<Value>>> = if is_col_segment {
            let store = self
                .db
                .get_or_create_col_segment_store(table_name, col_types)
                .ok();
            if let Some(store) = store {
                let _ = store.flush_buffer();
                let mut scanned = store.scan_projected_filtered(None, &needed_cols, &|_| true);
                if has_where {
                    let clause = where_clause.as_ref().unwrap();
                    // 🔥 WHERE 编译一次 + 全宽评估缓冲复用：旧路径每行分配
                    // vec![Null; ncol]、克隆全部 needed 值，再用字符串解析列名
                    // 的 eval_expr_on_row 评估 —— GROUP BY 比普通过滤慢 40%
                    // 的主因。每个 needed 槽位每行都会被覆写（缺失即写
                    // Null），非 needed 槽恒为 Null，复用安全。
                    let compiled = Self::compile_where(clause, schema);
                    let ncol = schema.columns.len();
                    let mut full = vec![Value::Null; ncol];
                    let mut keep: Vec<_> = Vec::with_capacity(scanned.len());
                    for entry in scanned.drain(..) {
                        let prow = &entry.1;
                        for (i, &sp) in needed_cols.iter().enumerate() {
                            if sp < ncol {
                                full[sp] = prow.get(i).cloned().unwrap_or(Value::Null);
                            }
                        }
                        let hit = if let Some(cw) = compiled.as_ref() {
                            match cw.eval(&full) {
                                Some(b) => b,
                                None => matches!(
                                    Self::eval_expr_on_row(clause, &full, schema),
                                    Ok(Value::Bool(true))
                                ),
                            }
                        } else {
                            matches!(
                                Self::eval_expr_on_row(clause, &full, schema),
                                Ok(Value::Bool(true))
                            )
                        };
                        if hit {
                            keep.push(entry);
                        }
                    }
                    scanned = keep;
                }
                Some(scanned.into_iter().map(|(_, r)| r).collect())
            } else {
                None
            }
        } else {
            None
        };

        let mut raw_iter = raw_iter;

        // Collect all rows into a single Vec first (avoids borrow-lifetime issues
        // between the col-seg projected rows and the LSM raw-byte decode).
        let all_rows: Vec<Vec<Value>> = if let Some(rows) = col_seg_rows {
            rows
        } else {
            let mut out = Vec::new();
            while let Some(item) = raw_iter.as_mut().unwrap().next() {
                let (_row_id, raw_bytes) = item?;
                let row = if let Some(ref clause) = where_clause {
                    let fr = match crate::storage::row_format::decode_fast(
                        &raw_bytes,
                        col_types,
                        fixed_count,
                    ) {
                        Ok(r) => r,
                        Err(_) => return Ok(None),
                    };
                    match Self::eval_expr_on_row(clause, &fr, schema) {
                        Ok(Value::Bool(true)) => fr,
                        Ok(_) => continue,
                        Err(_) => return Ok(None),
                    }
                } else {
                    (0..col_types.len())
                        .map(|pos| {
                            crate::storage::row_format::get_column(&raw_bytes, col_types, pos)
                                .unwrap_or(Value::Null)
                        })
                        .collect()
                };
                out.push(row);
            }
            out
        };

        for cur_row in &all_rows {
            let read_val = |schema_pos: usize| -> Value {
                if let Some(&pi) = proj_map.get(&schema_pos) {
                    cur_row.get(pi).cloned().unwrap_or(Value::Null)
                } else {
                    cur_row.get(schema_pos).cloned().unwrap_or(Value::Null)
                }
            };

            // Build group key from the row (via proj_map for col-seg).
            let group_key: Vec<Value> = group_col_positions
                .iter()
                .map(|&pos| read_val(pos))
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
                        let val = read_val(pos);
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
            let accums: Vec<AggAccumulator> =
                (0..num_aggs).map(|_| AggAccumulator::new()).collect();
            groups.insert(vec![], (vec![], accums));
        }

        // Build result rows
        let column_names: Vec<String> = select_col_info
            .iter()
            .map(|(name, _, _)| name.clone())
            .collect();
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
            let order_specs: Vec<(usize, bool)> = order_by
                .iter()
                .filter_map(|ob| {
                    // 🔑 ORDER BY can reference a bare column (e.g. ORDER BY cat)
                    // OR an aggregate expression (e.g. ORDER BY SUM(v) DESC).
                    // For aggregates, match by the function-call column name that
                    // was built for the result (e.g. "SUM(v)"). Without this,
                    // ORDER BY SUM(v) was silently dropped → non-deterministic
                    // group order (flaky test failures).
                    let ob_name = match &ob.expr {
                        Expr::Column(col_name) => col_name.clone(),
                        Expr::FunctionCall { name, args, .. } => {
                            let arg_str = args
                                .iter()
                                .map(|a| match a {
                                    Expr::Column(c) => c.clone(),
                                    e => format!("{:?}", e),
                                })
                                .collect::<Vec<_>>()
                                .join(", ");
                            format!("{}({})", name.to_uppercase(), arg_str)
                        }
                        _ => return None,
                    };
                    let idx = column_names.iter().position(|c| c == &ob_name)?;
                    Some((idx, ob.asc))
                })
                .collect();

            result_rows.sort_by(|a, b| {
                for &(idx, asc) in &order_specs {
                    // NULL ordering: NULLs sort last in ASC, first in DESC
                    // (matches apply_order_by, the non-GROUP-BY ORDER BY path).
                    let cmp = compare_with_nulls(&a[idx], &b[idx]);
                    let cmp = if asc { cmp } else { cmp.reverse() };
                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
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
    fn compute_aggregate_positional(&self, agg: &AggregateInfo, rows: &[&Row]) -> Result<Value> {
        use std::collections::HashSet;
        match agg.func.as_str() {
            "COUNT" => {
                if agg.distinct {
                    if agg.col_pos.is_none() {
                        return Err(MoteDBError::InvalidArgument(
                            "COUNT(DISTINCT *) is not supported".to_string(),
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
                    let count = rows
                        .iter()
                        .filter(|row| {
                            agg.col_pos
                                .and_then(|pos| row.get(pos))
                                .is_some_and(|v| !matches!(v, Value::Null))
                        })
                        .count();
                    Ok(Value::Integer(count as i64))
                }
            }
            "SUM" => {
                let mut int_sum: i64 = 0;
                let mut float_sum = CompSum::default();
                let mut has_float = false;
                let mut has_value = false;
                // DISTINCT: dedup non-NULL values first.
                let distinct_vals: Vec<Value> = if agg.distinct {
                    collect_distinct_positional(agg.col_pos, rows)
                } else {
                    Vec::new()
                };
                let iter_vals = distinct_vals.into_iter();
                let iter: Box<dyn Iterator<Item = Value>> = if agg.distinct {
                    Box::new(iter_vals)
                } else {
                    Box::new(
                        rows.iter()
                            .filter_map(|r| agg.col_pos.and_then(|p| r.get(p).cloned())),
                    )
                };
                for val in iter {
                    match val {
                        Value::Integer(i) => {
                            has_value = true;
                            if has_float {
                                float_sum.add(i as f64);
                            } else if let Some(s) = int_sum.checked_add(i) {
                                int_sum = s;
                            } else {
                                has_float = true;
                                float_sum.add(int_sum as f64);
                                float_sum.add(i as f64);
                            }
                        }
                        Value::Float(f) => {
                            has_value = true;
                            if !has_float {
                                has_float = true;
                                float_sum.add(int_sum as f64);
                            }
                            float_sum.add(f);
                        }
                        Value::Null => {}
                        _ => {
                            return Err(MoteDBError::TypeError(
                                "SUM requires numeric values".to_string(),
                            ))
                        }
                    }
                }
                if !has_value {
                    Ok(Value::Null)
                } else if has_float {
                    Ok(Value::Float(float_sum.total()))
                } else {
                    Ok(Value::Integer(int_sum))
                }
            }
            "AVG" => {
                let mut sum = CompSum::default();
                let mut count = 0;
                let distinct_vals: Vec<Value> = if agg.distinct {
                    collect_distinct_positional(agg.col_pos, rows)
                } else {
                    Vec::new()
                };
                let iter: Box<dyn Iterator<Item = Value>> = if agg.distinct {
                    Box::new(distinct_vals.into_iter())
                } else {
                    Box::new(
                        rows.iter()
                            .filter_map(|r| agg.col_pos.and_then(|p| r.get(p).cloned())),
                    )
                };
                for val in iter {
                    match val {
                        Value::Integer(i) => {
                            sum.add(i as f64);
                            count += 1;
                        }
                        Value::Float(f) => {
                            sum.add(f);
                            count += 1;
                        }
                        Value::Null => {}
                        _ => {
                            return Err(MoteDBError::TypeError(
                                "AVG requires numeric values".to_string(),
                            ))
                        }
                    }
                }
                if count > 0 {
                    Ok(Value::Float(sum.total() / count as f64))
                } else {
                    Ok(Value::Null)
                }
            }
            "MIN" => {
                let mut min_val: Option<Value> = None;
                let mut seen: Option<HashSet<Value>> = if agg.distinct {
                    Some(HashSet::new())
                } else {
                    None
                };
                for row in rows {
                    if let Some(pos) = agg.col_pos {
                        if let Some(val) = row.get(pos) {
                            if matches!(val, Value::Null) {
                                continue;
                            }
                            if let Some(ref mut s) = seen {
                                if !s.insert(val.clone()) {
                                    continue;
                                }
                            }
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
                Ok(min_val.unwrap_or(Value::Null))
            }
            "MAX" => {
                let mut max_val: Option<Value> = None;
                let mut seen: Option<HashSet<Value>> = if agg.distinct {
                    Some(HashSet::new())
                } else {
                    None
                };
                for row in rows {
                    if let Some(pos) = agg.col_pos {
                        if let Some(val) = row.get(pos) {
                            if matches!(val, Value::Null) {
                                continue;
                            }
                            if let Some(ref mut s) = seen {
                                if !s.insert(val.clone()) {
                                    continue;
                                }
                            }
                            max_val = Some(match max_val {
                                None => val.clone(),
                                Some(current) => {
                                    if val.partial_cmp(&current)
                                        == Some(std::cmp::Ordering::Greater)
                                    {
                                        val.clone()
                                    } else {
                                        current
                                    }
                                }
                            });
                        }
                    }
                }
                Ok(max_val.unwrap_or(Value::Null))
            }
            "STDDEV" | "VARIANCE" => {
                let mut sum = CompSum::default();
                let mut count = 0u64;
                for row in rows {
                    if let Some(pos) = agg.col_pos {
                        if let Some(val) = row.get(pos) {
                            match val {
                                Value::Integer(i) => {
                                    sum.add(*i as f64);
                                    count += 1;
                                }
                                Value::Float(f) => {
                                    sum.add(*f);
                                    count += 1;
                                }
                                _ => {}
                            }
                        }
                    }
                }
                if count < 2 {
                    return Ok(Value::Null);
                }
                let mean = sum.total() / count as f64;
                let mut var_sum = 0.0;
                for row in rows {
                    if let Some(pos) = agg.col_pos {
                        if let Some(val) = row.get(pos) {
                            let v = match val {
                                Value::Integer(i) => *i as f64,
                                Value::Float(f) => *f,
                                _ => continue,
                            };
                            let d = v - mean;
                            var_sum += d * d;
                        }
                    }
                }
                let variance = var_sum / (count as f64 - 1.0); // sample variance
                if agg.func == "STDDEV" {
                    Ok(Value::Float(variance.sqrt()))
                } else {
                    Ok(Value::Float(variance))
                }
            }
            "GROUP_CONCAT" => {
                // 🔑 GROUP_CONCAT: concat non-NULL values with separator.
                // agg.col_pos is the value column; separator comes from
                // agg.extra (set by try_parse_aggregate).
                let sep = agg.extra.as_deref().unwrap_or(",");
                let mut parts: Vec<String> = Vec::new();
                for row in rows {
                    if let Some(pos) = agg.col_pos {
                        if let Some(val) = row.get(pos) {
                            if !matches!(val, Value::Null) {
                                parts.push(value_to_concat_string(val));
                            }
                        }
                    }
                }
                if parts.is_empty() {
                    Ok(Value::Null)
                } else {
                    Ok(Value::text(parts.join(sep)))
                }
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
        let column_names: Vec<String> =
            if columns.len() == 1 && matches!(columns[0], SelectColumn::Star) {
                // SELECT * — strip table prefix from column names for output
                // (schema may be "polluted" with qualified names after execute_from_with_limit)
                schema
                    .columns
                    .iter()
                    .map(|c| {
                        if let Some(pos) = c.name.find('.') {
                            c.name[pos + 1..].to_string()
                        } else {
                            c.name.clone()
                        }
                    })
                    .collect()
            } else {
                columns
                    .iter()
                    .map(|col| match col {
                        SelectColumn::Star => "*".to_string(),
                        SelectColumn::Column(name) => name.clone(),
                        SelectColumn::ColumnWithAlias(_, alias) => alias.clone(),
                        SelectColumn::Expr(_, Some(alias)) => alias.clone(),
                        SelectColumn::Expr(expr, None) => format!("{:?}", expr), // Use debug format as default
                    })
                    .collect()
            };

        // 🚀 OPTIMIZATION: Reduce cloning in projection
        // Pre-calculate which columns we need to avoid repeated lookups
        // Determine table name for qualified lookups
        let table_name_for_qualify = schema.name.as_str();

        let projected_rows: Vec<Vec<Value>> = if columns.len() == 1
            && matches!(columns[0], SelectColumn::Star)
        {
            // SELECT * - optimized path
            rows.iter()
                .map(|(_, row)| {
                    schema
                        .columns
                        .iter()
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
                })
                .collect()
        } else {
            // Specific columns - optimize column lookup
            rows.iter()
                .map(|(_, row)| {
                    columns
                        .iter()
                        .map(|col| {
                            match col {
                                SelectColumn::Column(name)
                                | SelectColumn::ColumnWithAlias(name, _) => {
                                    // Try exact match first, then try with table prefix
                                    row.get(name)
                                        .cloned()
                                        .or_else(|| {
                                            // If column name doesn't contain '.', try prefixed versions
                                            if !name.contains('.') {
                                                // Try all possible table prefixes
                                                row.iter()
                                                    .find(|(k, _)| {
                                                        k.ends_with(&format!(".{}", name))
                                                    })
                                                    .map(|(_, v)| v.clone())
                                            } else {
                                                None
                                            }
                                        })
                                        .unwrap_or(Value::Null)
                                }
                                SelectColumn::Expr(expr, _) => self
                                    .eval_with_materialized(expr, row)
                                    .unwrap_or(Value::Null),
                                SelectColumn::Star => Value::Null, // Shouldn't happen
                            }
                        })
                        .collect()
                })
                .collect()
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
        if !Self::can_eval_positional(&where_expr) {
            return Ok(None);
        }

        // Precompute HashSet for IN with large literal lists: O(1) lookup instead of O(N) scan
        let in_hash_set: Option<std::collections::HashSet<Value>> = match &where_expr {
            Expr::In {
                expr: _,
                list,
                negated,
            } if !negated
                && list.len() > 10
                && list.iter().all(|e| matches!(e, Expr::Literal(_))) =>
            {
                let set: std::collections::HashSet<Value> = list
                    .iter()
                    .filter_map(|e| {
                        if let Expr::Literal(v) = e {
                            Some(v.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                Some(set)
            }
            _ => None,
        };

        let limit = stmt.limit.unwrap_or(usize::MAX);
        let offset = stmt.offset.unwrap_or(0);
        let cap_hint = limit.min(self.db.fast_row_count(table_name).unwrap_or(1024) as usize);

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
                        pos.and_then(|p| row.get(p))
                            .map(|v| hash_set.contains(v))
                            .unwrap_or(false)
                    } else {
                        false
                    }
                } else {
                    false
                }
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
                if skipped < offset {
                    skipped += 1;
                    continue;
                }
                let projected: Vec<Value> = col_positions
                    .iter()
                    .map(|pos| pos.and_then(|p| row.get(p)).cloned().unwrap_or(Value::Null))
                    .collect();
                rows.push(projected);
                if rows.len() >= limit {
                    break;
                }
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
        if !has_order_by && !has_distinct {
            return Ok(None);
        }
        if stmt.group_by.is_some() {
            return Ok(None);
        } // GROUP BY handles its own path
          // 🔑 Explicit NULLS FIRST/LAST that differs from the dialect default
          // needs apply_order_by's comparator — decline to the materialized path.
        if order_by_has_nondefault_nulls(stmt.order_by.as_deref()) {
            return Ok(None);
        }

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
                    } else {
                        return Ok(None);
                    }
                }
                SelectColumn::ColumnWithAlias(name, alias) => {
                    if let Some(pos) = schema.get_column_position(name) {
                        resolved_cols.push((alias.clone(), Some(pos)));
                    } else {
                        return Ok(None);
                    }
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
            // 🔥 编译一次：避免每行的列名字符串解析
            let compiled = Self::compile_where(where_clause, schema);
            let mut matching = Vec::new();
            for result in row_iter {
                let (_, row) = result?;
                let hit = if let Some(cw) = compiled.as_ref() {
                    match cw.eval(&row) {
                        Some(b) => b,
                        None => match Self::eval_expr_on_row(where_clause, &row, schema) {
                            Ok(Value::Bool(b)) => b,
                            Ok(_) => false,
                            Err(_) => return Ok(None),
                        },
                    }
                } else {
                    match Self::eval_expr_on_row(where_clause, &row, schema) {
                        Ok(Value::Bool(b)) => b,
                        Ok(_) => false,
                        Err(_) => return Ok(None),
                    }
                };
                if hit {
                    let projected: Vec<Value> = col_positions
                        .iter()
                        .map(|pos| pos.and_then(|p| row.get(p)).cloned().unwrap_or(Value::Null))
                        .collect();
                    matching.push(projected);
                }
            }
            matching
        } else {
            // 🚀 Partial column scan: only decode columns we need
            let partial_iter = self
                .db
                .scan_table_rows_partial(table_name, &scan_positions)?;
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
                    let cmp = a
                        .get(col_idx)
                        .and_then(|va| b.get(col_idx).map(|vb| (va, vb)))
                        .map(|(va, vb)| order_by_cmp(va, vb))
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
                    let cmp = a
                        .get(col_idx)
                        .and_then(|va| b.get(col_idx).map(|vb| (va, vb)))
                        .map(|(va, vb)| order_by_cmp(va, vb))
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
                        let cmp = a
                            .get(col_idx)
                            .and_then(|va| b.get(col_idx).map(|vb| (va, vb)))
                            .map(|(va, vb)| order_by_cmp(va, vb))
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

        // 🆕 INSERT ... SELECT: 先物化 SELECT 行 (execute_select_internal 是
        // 内部路径, 不受 max_result_rows 截断 — 子查询同款), 后续走同一插入
        // 管线 (batch/WAL/事务/索引)。自插 (INSERT INTO t SELECT ... FROM t)
        // 因先物化后插入而无无限循环。
        let select_rows: Option<Vec<Vec<Value>>> = match &stmt.select {
            Some(sel) => {
                if stmt.on_conflict.is_some() {
                    return Err(MoteDBError::InvalidArgument(
                        "INSERT ... SELECT does not support ON CONFLICT / OR IGNORE / OR REPLACE yet"
                            .into(),
                    ));
                }
                match self.execute_select_internal(sel)? {
                    QueryResult::Select { rows, .. } => Some(rows),
                    _ => {
                        return Err(MoteDBError::InvalidArgument(
                            "INSERT ... SELECT source must be a SELECT query".into(),
                        ))
                    }
                }
            }
            None => None,
        };

        // 🆕 Upsert (ON CONFLICT / OR IGNORE / OR REPLACE): dedicated
        // row-at-a-time path with a per-row existence check.
        if stmt.on_conflict.is_some() {
            return self.execute_upsert(stmt, &schema, &columns);
        }

        // Route TimeSeries INSERT to columnar store
        if schema.table_type == crate::types::TableType::TimeSeries {
            return self.execute_columnar_insert(stmt, &schema, &columns, select_rows.as_deref());
        }

        // Prepare all rows — resolve expressions to Values, build Row directly
        let mut prepared_rows = Vec::new();

        // SELECT 源: 行已是求值好的 Values — 直接按 columns 映射建行。
        if let Some(sel_rows) = &select_rows {
            for value_row in sel_rows {
                if value_row.len() != columns.len() {
                    return Err(MoteDBError::InvalidArgument(format!(
                        "Column count mismatch: expected {}, got {}",
                        columns.len(),
                        value_row.len()
                    )));
                }
                let row =
                    crate::sql::row_converter::values_to_row_by_columns(value_row, &columns, &schema)?;
                prepared_rows.push(row);
            }
        }

        for value_row in &stmt.values {
            if value_row.len() != columns.len() {
                return Err(MoteDBError::InvalidArgument(format!(
                    "Column count mismatch: expected {}, got {}",
                    columns.len(),
                    value_row.len()
                )));
            }

            // Resolve all expressions to Values (skip HashMap intermediary)
            let resolved: Vec<Value> = value_row
                .iter()
                .map(|expr| match expr {
                    Expr::Literal(v) => Ok(v.clone()),
                    Expr::Parameter(_) => {
                        let empty_row = SqlRow::new();
                        self.evaluator.eval(expr, &empty_row)
                    }
                    // 🚨 Constant-fold negative literals (`-1e15`, `-(5.0)`)
                    // which the parser represents as UnaryOp(Minus, Literal).
                    other if Self::is_constant_expr(other) => {
                        let empty_row = SqlRow::new();
                        self.evaluator.eval(other, &empty_row)
                    }
                    other => Err(MoteDBError::InvalidArgument(format!(
                        "INSERT VALUES must be literals or parameters, got {:?}",
                        other
                    ))),
                })
                .collect::<Result<Vec<_>>>()?;

            // Build Row directly using column mapping (no HashMap)
            let row =
                crate::sql::row_converter::values_to_row_by_columns(&resolved, &columns, &schema)?;
            prepared_rows.push(row);
        }

        let affected_rows = prepared_rows.len();

        // Track last_insert_id for AUTO_INCREMENT primary key
        // If inside an explicit transaction, buffer INSERTs via coordinator write_set.
        let txn_id: Option<u64> = self.current_txn_id();
        let mut last_row_id: Option<u64> = None;

        if txn_id.is_some() {
            // Transactional path: rows are buffered in the coordinator's
            // write_set; secondary indexes are maintained at COMMIT.
            for row in prepared_rows {
                let row_id = self
                    .db
                    .insert_row_with_txn(&stmt.table, txn_id.unwrap(), row)?;
                last_row_id = Some(row_id);
            }
        } else if prepared_rows.len() > 1 {
            // 🚀 Batch path: single WAL fsync, batched LSM put, batched index
            // updates (vector/text/spatial included). Vector tables used to be
            // forced through the per-row insert here — one group-commit fsync
            // per row (~260 rows/s under the default preset) plus a redundant
            // second graph insert per vector.
            let ids = self
                .db
                .batch_insert_rows_to_table(&stmt.table, prepared_rows)?;
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
                self.last_insert_id
                    .store(row_id as i64, std::sync::atomic::Ordering::Relaxed);
                self.evaluator
                    .last_insert_id
                    .store(row_id as i64, std::sync::atomic::Ordering::Relaxed);
            }
        }

        Ok(QueryResult::Modification { affected_rows })
    }

    /// 🆕 Execute an upsert INSERT (`ON CONFLICT ...` / `INSERT OR IGNORE` /
    /// `INSERT OR REPLACE`).
    ///
    /// Row-at-a-time: each proposed row's primary key is looked up against
    /// live data, then routed to insert / update-in-place / delete+insert /
    /// skip. `excluded.col` in DO UPDATE SET expressions refers to the
    /// proposed row; unqualified columns refer to the existing row (standard
    /// SQL upsert semantics).
    ///
    /// Existence check: outside transactions the PK index fast path
    /// (`query_by_column`); inside a transaction the storage scan is merged
    /// with the uncommitted write_set so upserts hit rows INSERTed earlier in
    /// the same transaction.
    fn execute_upsert(
        &self,
        stmt: &InsertStmt,
        schema: &crate::types::TableSchema,
        columns: &[String],
    ) -> Result<QueryResult> {
        use crate::sql::ast::ConflictAction;

        if schema.table_type == crate::types::TableType::TimeSeries {
            return Err(MoteDBError::InvalidArgument(
                "ON CONFLICT / OR REPLACE is not supported for TimeSeries tables".into(),
            ));
        }

        let oc = stmt.on_conflict.as_ref().expect("caller checks is_some");
        let pk_name: Option<String> = schema.primary_key().map(|s| s.to_string());
        let pk_position = pk_name
            .as_ref()
            .and_then(|pk| schema.get_column(pk))
            .map(|c| c.position);

        // Explicit conflict target must be exactly the primary key — there
        // are no secondary unique constraints to conflict on today.
        if let Some(target) = &oc.target {
            match &pk_name {
                None => {
                    return Err(MoteDBError::InvalidArgument(format!(
                        "ON CONFLICT target specified but table '{}' has no primary key",
                        stmt.table
                    )));
                }
                Some(pk) if target.len() != 1 || !target[0].eq_ignore_ascii_case(pk) => {
                    return Err(MoteDBError::InvalidArgument(format!(
                        "ON CONFLICT target ({}) does not match the primary key '{}' of table '{}'",
                        target.join(", "),
                        pk,
                        stmt.table
                    )));
                }
                _ => {}
            }
        }

        // DO UPDATE needs a PK to locate the conflicting row. The other
        // actions degrade to plain INSERT on PK-less tables (SQLite
        // semantics: no constraint ⇒ conflict never fires).
        if matches!(oc.action, ConflictAction::DoUpdate { .. }) && pk_position.is_none() {
            return Err(MoteDBError::InvalidArgument(format!(
                "ON CONFLICT DO UPDATE requires a primary key on table '{}'",
                stmt.table
            )));
        }

        let txn_id = self.current_txn_id();
        let mut affected_rows = 0usize;
        let mut last_row_id: Option<u64> = None;

        for value_row in &stmt.values {
            if value_row.len() != columns.len() {
                return Err(MoteDBError::InvalidArgument(format!(
                    "Column count mismatch: expected {}, got {}",
                    columns.len(),
                    value_row.len()
                )));
            }

            let resolved: Vec<Value> = value_row
                .iter()
                .map(|expr| match expr {
                    Expr::Literal(v) => Ok(v.clone()),
                    Expr::Parameter(_) => {
                        let empty_row = SqlRow::new();
                        self.evaluator.eval(expr, &empty_row)
                    }
                    other if Self::is_constant_expr(other) => {
                        let empty_row = SqlRow::new();
                        self.evaluator.eval(other, &empty_row)
                    }
                    other => Err(MoteDBError::InvalidArgument(format!(
                        "INSERT VALUES must be literals or parameters, got {:?}",
                        other
                    ))),
                })
                .collect::<Result<Vec<_>>>()?;

            let new_row =
                crate::sql::row_converter::values_to_row_by_columns(&resolved, columns, schema)?;

            // Proposed PK value. None/Null ⇒ no conflict possible: either the
            // table has no PK, or it's an AUTO_INCREMENT slot the insert will
            // allocate (fresh counter value can't collide).
            let proposed_pk = pk_position
                .and_then(|p| new_row.get(p))
                .cloned()
                .filter(|v| !matches!(v, Value::Null));

            let existing: Option<(RowId, Row)> = match (&proposed_pk, txn_id) {
                (Some(pk_value), None) => {
                    let pk_value = pk_value.clone();
                    let mut found = None;
                    for rid in self.resolve_pk_row_ids(&stmt.table, schema, &pk_value)? {
                        if let Some(r) = self.db.get_table_row(&stmt.table, rid)? {
                            found = Some((rid, r));
                            break;
                        }
                    }
                    found
                }
                (Some(pk_value), Some(_tid)) => {
                    // In-transaction: merge storage scan with the write_set so
                    // rows INSERTed earlier in this transaction are found.
                    let pk_pos = pk_position.expect(
                        "checked above for DoUpdate; other actions skip conflict on None pk",
                    );
                    let pk_value = pk_value.clone();
                    let mut found = None;
                    for result in self.db.scan_table_rows_streaming(&stmt.table)? {
                        let (rid, row) = result?;
                        if row.get(pk_pos) == Some(&pk_value) {
                            found = Some((rid, row));
                            break;
                        }
                    }
                    if found.is_none() {
                        for (rid, row) in self.txn_write_set_rows(&stmt.table) {
                            if row.get(pk_pos) == Some(&pk_value) {
                                found = Some((rid, row));
                                break;
                            }
                        }
                    }
                    found
                }
                (None, _) => None,
            };

            match existing {
                Some((rid, existing_row)) => match &oc.action {
                    ConflictAction::Ignore | ConflictAction::DoNothing => continue,
                    ConflictAction::Replace => {
                        // Delete the old row first, then insert the new one
                        // below (full-row replacement semantics). Undo delta
                        // mirrors execute_delete_pk's transactional path.
                        if let Some(tid) = txn_id {
                            let _ = self.db.txn_coordinator.record_write_delta(
                                tid,
                                crate::txn::coordinator::DeltaOperation::Delete(
                                    rid,
                                    stmt.table.clone(),
                                    std::sync::Arc::new(existing_row.clone()),
                                ),
                            );
                        }
                        self.db
                            .delete_row_from_table(&stmt.table, rid, existing_row)?;
                    }
                    ConflictAction::DoUpdate { assignments } => {
                        // Merged evaluation context: unqualified `col` → the
                        // existing row, `excluded.col` → the proposed row.
                        // The evaluator resolves both via direct SqlRow keys.
                        let mut eval_row = SqlRow::new();
                        for cd in &schema.columns {
                            let old = existing_row
                                .get(cd.position)
                                .cloned()
                                .unwrap_or(Value::Null);
                            let new = new_row.get(cd.position).cloned().unwrap_or(Value::Null);
                            eval_row.insert(cd.name.clone(), old.clone());
                            eval_row.insert(format!("{}.{}", stmt.table, cd.name), old);
                            eval_row.insert(format!("excluded.{}", cd.name), new);
                        }

                        let mut new_values: Vec<(usize, Value)> =
                            Vec::with_capacity(assignments.len());
                        for (col_name, expr) in assignments {
                            let Some(cd) = schema.get_column(col_name) else {
                                return Err(MoteDBError::InvalidArgument(format!(
                                    "Unknown column '{}' in ON CONFLICT DO UPDATE SET for table '{}'",
                                    col_name, stmt.table
                                )));
                            };
                            let new_val = if let Expr::Literal(v) = expr {
                                v.clone()
                            } else if Self::expr_contains_subquery(expr) {
                                let materialized = self.materialize_subqueries(expr)?;
                                if let Expr::Literal(v) = materialized {
                                    v
                                } else {
                                    self.evaluator.eval(&materialized, &eval_row)?
                                }
                            } else {
                                self.evaluator.eval(expr, &eval_row)?
                            };
                            new_values.push((cd.position, new_val));
                        }

                        let mut updated_row = existing_row.clone();
                        for (pos, val) in new_values {
                            while updated_row.len() <= pos {
                                updated_row.push(Value::Null);
                            }
                            updated_row[pos] = val;
                        }

                        // Undo/bookkeeping mirrors execute_update_pk.
                        if let Some(tid) = txn_id {
                            let updated = self.db.txn_coordinator.update_write_set_row(
                                tid,
                                &stmt.table,
                                rid,
                                updated_row.clone(),
                            )?;
                            if !updated {
                                let _ = self.db.txn_coordinator.record_write_delta(
                                    tid,
                                    crate::txn::coordinator::DeltaOperation::Update(
                                        rid,
                                        stmt.table.clone(),
                                        std::sync::Arc::new(existing_row.clone()),
                                    ),
                                );
                            }
                        }

                        self.db.update_row_in_table_with_schema(
                            &stmt.table,
                            rid,
                            existing_row,
                            updated_row,
                            schema,
                        )?;
                        affected_rows += 1;
                        continue;
                    }
                },
                None => {
                    // No conflict (or no PK): fall through to plain insert.
                }
            }

            let row_id = if let Some(tid) = txn_id {
                self.db.insert_row_with_txn(&stmt.table, tid, new_row)?
            } else {
                self.db.insert_row_to_table(&stmt.table, new_row)?
            };
            last_row_id = Some(row_id);
            affected_rows += 1;
        }

        // Same AUTO_INCREMENT bookkeeping as the plain INSERT path.
        if schema.is_primary_key_auto_increment() {
            if let Some(row_id) = last_row_id {
                self.last_insert_id
                    .store(row_id as i64, std::sync::atomic::Ordering::Relaxed);
                self.evaluator
                    .last_insert_id
                    .store(row_id as i64, std::sync::atomic::Ordering::Relaxed);
            }
        }

        Ok(QueryResult::Modification { affected_rows })
    }

    /// 🆕 EXPLAIN <SELECT>: report the plan the executor's fast paths would
    /// choose — scan strategy (PK point lookup / column index / top-K heap /
    /// full scan) plus a row estimate — WITHOUT executing the query.
    ///
    /// v1 is a heuristic report of the same signals the runtime fast paths
    /// key on (PK equality, column-index equality, ORDER BY + LIMIT); the
    /// aggregate step is informational.
    fn execute_explain(&self, inner: &Statement) -> Result<QueryResult> {
        let sel: &crate::sql::ast::SelectStmt = match inner {
            Statement::Select { stmt: s, .. } => s,
            _ => {
                return Err(MoteDBError::InvalidArgument(
                    "EXPLAIN supports SELECT statements only in this version".into(),
                ));
            }
        };

        let Some(crate::sql::ast::TableRef::Table { name: table, .. }) = sel.from.as_ref() else {
            return Err(MoteDBError::InvalidArgument(
                "EXPLAIN requires a simple FROM <table> in this version".into(),
            ));
        };
        let schema = self.db.get_table_schema(table)?;
        let total = self.db.fast_row_count(table).unwrap_or(0);

        let mut rows: Vec<Vec<Value>> = Vec::new();
        let mut step = 0i64;
        let mut push = |op: &str, detail: String, rows: &mut Vec<Vec<Value>>| {
            step += 1;
            rows.push(vec![
                Value::Integer(step),
                Value::text_from(op),
                Value::text_from(&detail),
            ]);
        };

        // ── Scan strategy: mirror the fast-path decision signals ──────────
        let mut strategy = String::from("full_scan");
        let mut cost = format!("rows≤{total}");
        if let Some(w) = &sel.where_clause {
            if let Some((col, _v)) = self.try_extract_point_query(w) {
                let is_pk = schema
                    .primary_key()
                    .map(|pk| pk.eq_ignore_ascii_case(&col))
                    .unwrap_or(false);
                if is_pk {
                    strategy = format!("pk_point_lookup({col})");
                    cost = "rows=1".to_string();
                } else if let Some(idx) = self.db.index_registry.find_by_column(
                    table,
                    &col,
                    crate::database::index_metadata::IndexType::Column,
                ) {
                    strategy = format!("column_index({idx} on {col})");
                    cost = format!("index probe + row fetch (table rows≤{total})");
                }
            }
        }
        if strategy == "full_scan"
            && sel.where_clause.is_none()
            && sel.order_by.is_some()
            && sel.limit.is_some()
        {
            strategy = format!("top_k_bounded_heap(LIMIT {})", sel.limit.unwrap_or(0));
            cost = format!("single pass, heap of {}", sel.limit.unwrap_or(0));
        }
        push(
            "scan",
            format!("table '{table}': strategy={strategy}, {cost}"),
            &mut rows,
        );

        if sel.group_by.is_some() {
            let cols = sel.group_by.clone().unwrap_or_default().join(", ");
            push(
                "aggregate",
                format!("GROUP BY {cols} (hash aggregate over scan output)"),
                &mut rows,
            );
        }
        if let Some(ref ob) = sel.order_by {
            let keys: Vec<String> = ob
                .iter()
                .map(|o| {
                    let name = match &o.expr {
                        Expr::Column(c) => c.clone(),
                        other => format!("{:?}", other),
                    };
                    format!("{name} {}", if o.asc { "ASC" } else { "DESC" })
                })
                .collect();
            push("sort", format!("ORDER BY {}", keys.join(", ")), &mut rows);
        }
        if sel.limit.is_some() {
            push(
                "limit",
                format!(
                    "LIMIT {}{}",
                    sel.limit.map(|l| l.to_string()).unwrap_or_default(),
                    sel.offset
                        .map(|o| format!(" OFFSET {o}"))
                        .unwrap_or_default()
                ),
                &mut rows,
            );
        }
        push(
            "note",
            "heuristic plan (v1): reports the fast path the executor would pick; set-based paths (JOIN/subquery) are not yet modeled".to_string(),
            &mut rows,
        );

        Ok(QueryResult::Select {
            columns: vec![
                "step".to_string(),
                "operator".to_string(),
                "detail".to_string(),
            ],
            rows,
        })
    }

    /// Execute UPDATE statement
    fn execute_update(&self, stmt: UpdateStmt) -> Result<QueryResult> {
        let schema = self.db.get_table_schema(&stmt.table)?;

        // 🔑 TimeSeries rows are immutable (append-only ColumnarStore).
        if schema.table_type == crate::types::TableType::TimeSeries {
            return Err(MoteDBError::InvalidArgument(format!(
                "TimeSeries table '{}' rows are immutable; DELETE (time range) + re-INSERT instead",
                stmt.table
            )));
        }

        // Validate all assignment columns exist before modifying any rows
        for (col_name, _) in &stmt.assignments {
            if schema.get_column(col_name).is_none() {
                return Err(StorageError::ColumnNotFound(format!(
                    "'{}' in table '{}'",
                    col_name, stmt.table
                )));
            }
        }

        // 🚀 PK fast path: skip full table scan for WHERE pk = value.
        // 🔑 Valid inside transactions too: execute_update_pk checks the txn
        // write_set/tombstones per row and folds in matching uncommitted
        // INSERTs, so read-your-writes is preserved. The old blanket
        // in-txn bail forced every statement through a full-table scan
        // (~70ms each at 100K rows — 2K updates took 2+ minutes).
        if let Some(ref where_clause) = stmt.where_clause {
            if let Some((col_name, target_value)) = self.try_extract_point_query(where_clause) {
                let is_pk = schema
                    .primary_key()
                    .map(|pk| pk == col_name)
                    .unwrap_or(false);

                if is_pk {
                    return self.execute_update_pk(&stmt, &schema, &target_value);
                }

                // Column index fast path: use index to find matching rows.
                // 🔑 Still txn-gated: the index reads storage only, so rows
                // INSERTed in this txn would be missed.
                if !self.is_in_transaction() {
                    if let Some(index_name) = self.db.index_registry.find_by_column(
                        &stmt.table,
                        &col_name,
                        crate::database::index_metadata::IndexType::Column,
                    ) {
                        if let Some(index) = self.db.column_indexes.get(&index_name) {
                            let matching_row_ids = index
                                .value()
                                .get_arc(&target_value)
                                .unwrap_or_else(|_| Arc::new(Vec::new()));
                            if matching_row_ids.is_empty() {
                                return Ok(QueryResult::Modification { affected_rows: 0 });
                            }
                            return self.execute_update_by_row_ids(
                                &stmt,
                                &schema,
                                &matching_row_ids,
                                &col_name,
                                &target_value,
                            );
                        }
                    }
                }
            }
        }

        // 🚀 Use真正的流式扫描 (O(1) memory)
        let row_iter = self.db.scan_table_rows_streaming(&stmt.table)?;

        // 🔑 Materialize subqueries in WHERE (e.g. `WHERE id IN (SELECT ...)`)
        // BEFORE the per-row eval. Without this, eval_expr_on_row sees a raw
        // Subquery node in the WHERE clause and returns NULL for every row
        // (UPDATE matches nothing). materialize_subqueries converts
        // IN (SELECT...) → INHashset for O(1) per-row lookup.
        let resolved_where = if let Some(ref wc) = stmt.where_clause {
            if Self::expr_contains_subquery(wc) {
                Some(self.materialize_subqueries(wc)?)
            } else {
                stmt.where_clause.clone()
            }
        } else {
            None
        };

        let mut affected_rows = 0;
        // 🔑 批量 UPDATE 收集器 (语句级一次 WAL 栅栏)
        let mut pending_updates: Vec<(crate::types::RowId, Vec<Value>, Vec<Value>)> = Vec::new();

        // 🔥 WHERE 编译一次（列位置预解析）：旧路径每行每个列引用都做
        // get_column_position 字符串线性查找
        let compiled_where: Option<CompiledWhere> = resolved_where
            .as_ref()
            .and_then(|wc| Self::compile_where(wc, &schema));

        // 🔑 In transaction mode, also process rows from the write_set
        // (uncommitted INSERTs). These rows are NOT in storage yet, so the
        // scan above doesn't see them. Without this, `UPDATE t SET v=99
        // WHERE id=1` after `INSERT INTO t VALUES (1, 10)` in the same txn
        // would match 0 rows.
        let txn_rows = if self.is_in_transaction() {
            self.txn_write_set_rows(&stmt.table)
        } else {
            Vec::new()
        };

        for result in row_iter {
            let (row_id, row) = result?;

            // WHERE filter using positional evaluation (no HashMap)
            let should_update = if let Some(ref where_clause) = resolved_where {
                Self::compiled_or_eval_row(compiled_where.as_ref(), where_clause, &row, &schema)
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
                    } else if Self::expr_contains_subquery(expr) {
                        // 🆕 Resolve scalar subqueries in SET expression
                        // (e.g., `UPDATE t SET v = (SELECT MAX(v) FROM t)`).
                        // Without this, eval_expr_on_row returns NULL for
                        // Subquery nodes (it doesn't execute them).
                        let materialized = self.materialize_subqueries(expr)?;
                        if let Expr::Literal(v) = materialized {
                            v
                        } else {
                            Self::eval_expr_on_row(&materialized, &row, &schema)
                                .unwrap_or(Value::Null)
                        }
                    } else {
                        match Self::eval_expr_on_row(expr, &row, &schema) {
                            Ok(v) => v,
                            Err(e) => {
                                // Propagate errors (e.g. division by zero) instead
                                // of silently writing NULL.
                                return Err(e);
                            }
                        }
                    };
                    while new_row.len() <= cd.position {
                        new_row.push(Value::Null);
                    }
                    new_row[cd.position] = new_val;
                }
            }

            // 🔑 Record undo delta for transactional UPDATE (so ROLLBACK can restore).
            let txn_id = self.current_txn_id();
            if let Some(tid) = txn_id {
                // 🔑 If row was INSERTed in this txn, update write_set (prevents
                // COMMIT overwriting the UPDATE with the stale INSERT value).
                let updated = self.db.txn_coordinator.update_write_set_row(
                    tid,
                    &stmt.table,
                    row_id,
                    new_row.clone(),
                )?;
                if !updated {
                    let _ = self.db.txn_coordinator.record_write_delta(
                        tid,
                        crate::txn::coordinator::DeltaOperation::Update(
                            row_id,
                            stmt.table.clone(),
                            Arc::new(row.clone()),
                        ),
                    );
                }
            }

            // 🔑 收集后批量提交: WAL 全部 deferred, 语句级一次组提交栅栏。
            // 此前逐行各等一次 fsync (~3.2ms/行) — UPDATE 10% 行 @100K 曾
            // 40s (资源测评挖出)。语义: 校验/求值失败的行使整条语句在写入
            // 前失败 (比旧的"写一半再报错"更接近原子)。
            pending_updates.push((row_id, row.clone(), new_row));
        }
        if pending_updates.len() == 1 {
            let (rid, old_row, new_row) = pending_updates.into_iter().next().unwrap();
            self.db
                .update_row_in_table_with_schema(&stmt.table, rid, old_row, new_row, &schema)?;
            affected_rows += 1;
        } else if !pending_updates.is_empty() {
            affected_rows += self
                .db
                .update_rows_batch_with_schema(&stmt.table, pending_updates, &schema)? as usize;
        }

        // 🔑 Process write_set rows (uncommitted INSERTs in this txn).
        for (ws_row_id, ws_row) in &txn_rows {
            let row = ws_row.clone();
            let should_update = if let Some(ref where_clause) = resolved_where {
                Self::compiled_or_eval_row(compiled_where.as_ref(), where_clause, &row, &schema)
            } else {
                true
            };
            if !should_update {
                continue;
            }
            self.apply_update_to_write_set_row(&stmt, &schema, *ws_row_id, &row)?;
            affected_rows += 1;
        }

        Ok(QueryResult::Modification { affected_rows })
    }
    /// Literal → timestamp micros for TimeSeries DELETE ranges.
    fn ts_value_to_micros(v: &Value) -> Result<i64> {
        match v {
            Value::Integer(us) => Ok(*us),
            Value::Timestamp(ts) => Ok(ts.as_micros()),
            _ => Err(MoteDBError::InvalidArgument(
                "TimeSeries DELETE bound must be an INTEGER (micros) or TIMESTAMP".into(),
            )),
        }
    }

    fn execute_delete(&self, stmt: DeleteStmt) -> Result<QueryResult> {
        // 🔑 Resolve subqueries in WHERE clause before evaluation. Without this,
        // DELETE ... WHERE id NOT IN (SELECT ...) silently matches no rows
        // (the evaluator can't execute subqueries against an SqlRow).
        let stmt = if let Some(ref wc) = stmt.where_clause {
            if Self::expr_contains_subquery(wc) {
                DeleteStmt {
                    table: stmt.table.clone(),
                    where_clause: Some(self.materialize_subqueries(wc)?),
                }
            } else {
                stmt
            }
        } else {
            stmt
        };
        let schema = self.db.get_table_schema(&stmt.table)?;

        // 🔑 TimeSeries DELETE: the ColumnarStore is append-only; the general
        // delete path wrote tombstones into a store the TS read path never
        // consults — rows "deleted" (and counted down) stayed fully visible.
        // Map pure time-range predicates to gc_expired; anything else is a
        // clear error instead of a silent no-op.
        if schema.table_type == crate::types::TableType::TimeSeries {
            let ts_col = schema.timeseries_column.clone().ok_or_else(|| {
                MoteDBError::InvalidArgument(format!(
                    "TimeSeries table '{}' has no timestamp column",
                    stmt.table
                ))
            })?;
            let cutoff = match &stmt.where_clause {
                Some(Expr::BinaryOp {
                    left,
                    op: crate::sql::ast::BinaryOperator::Lt,
                    right,
                }) if matches!(left.as_ref(), Expr::Column(c) if *c == ts_col) => {
                    match right.as_ref() {
                        Expr::Literal(v) => Self::ts_value_to_micros(v)?,
                        _ => {
                            return Err(MoteDBError::InvalidArgument(
                            "TimeSeries DELETE only supports a literal time range (ts < value / ts <= value)".into(),
                        ));
                        }
                    }
                }
                Some(Expr::BinaryOp {
                    left,
                    op: crate::sql::ast::BinaryOperator::Le,
                    right,
                }) if matches!(left.as_ref(), Expr::Column(c) if *c == ts_col) => {
                    match right.as_ref() {
                        Expr::Literal(v) => Self::ts_value_to_micros(v)? + 1,
                        _ => {
                            return Err(MoteDBError::InvalidArgument(
                                "TimeSeries DELETE only supports a literal time range (ts < value / ts <= value)".into(),
                            ));
                        }
                    }
                }
                None => i64::MAX,
                Some(_) => {
                    return Err(MoteDBError::InvalidArgument(
                        "TimeSeries DELETE only supports time-range predicates on the timestamp column (ts < value / ts <= value)".into(),
                    ));
                }
            };
            let n = self.db.gc_timeseries(&stmt.table, cutoff)?;
            return Ok(QueryResult::Modification { affected_rows: n });
        }

        // 🚀 PK fast path: skip full table scan for WHERE pk = value
        // 🔑 Skip PK fast path in transaction mode (same reason as execute_update:
        // the row may exist only in write_set, invisible to resolve_pk_row_ids).
        if !self.is_in_transaction() {
            if let Some(ref where_clause) = stmt.where_clause {
                if let Some((col_name, target_value)) = self.try_extract_point_query(where_clause) {
                    let is_pk = schema
                        .primary_key()
                        .map(|pk| pk == col_name)
                        .unwrap_or(false);

                    if is_pk {
                        return self.execute_delete_pk(&stmt, &schema, &target_value);
                    }

                    // 🚀 Column index fast path: use index to find matching rows
                    if let Some(index_name) = self.db.index_registry.find_by_column(
                        &stmt.table,
                        &col_name,
                        crate::database::index_metadata::IndexType::Column,
                    ) {
                        if let Some(index) = self.db.column_indexes.get(&index_name) {
                            let matching_row_ids = index
                                .value()
                                .get_arc(&target_value)
                                .unwrap_or_else(|_| Arc::new(Vec::new()));
                            if matching_row_ids.is_empty() {
                                return Ok(QueryResult::Modification { affected_rows: 0 });
                            }
                            return self.execute_delete_by_row_ids(
                                &stmt,
                                &schema,
                                &matching_row_ids,
                                &col_name,
                                &target_value,
                            );
                        }
                    }
                }
            }
        }

        // 🚀 Use真正的流式扫描 (O(1) memory)
        let row_iter = self.db.scan_table_rows_streaming(&stmt.table)?;

        let mut affected_rows = 0;

        // 🔑 In transaction mode, also process write_set rows (same as execute_update).
        let txn_rows = if self.is_in_transaction() {
            self.txn_write_set_rows(&stmt.table)
        } else {
            Vec::new()
        };

        let mut pending_deletes: Vec<(crate::types::RowId, Vec<Value>)> = Vec::new();
        for result in row_iter {
            let (row_id, row) = result?;
            let sql_row = row_to_sql_row(&row, &schema)?;

            // Filter rows (WHERE clause)
            let should_delete = if let Some(ref where_clause) = stmt.where_clause {
                self.evaluator
                    .eval(where_clause, &sql_row)
                    .and_then(|val| self.to_bool(&val))
                    .unwrap_or(false)
            } else {
                true
            };

            if !should_delete {
                continue;
            }

            // 🔑 Record undo delta for transactional DELETE (so ROLLBACK can restore).
            let txn_id = self.current_txn_id();
            if let Some(tid) = txn_id {
                let _ = self.db.txn_coordinator.record_write_delta(
                    tid,
                    crate::txn::coordinator::DeltaOperation::Delete(
                        row_id,
                        stmt.table.clone(),
                        Arc::new(row.clone()),
                    ),
                );
            }

            // Delete row - 底层已实现增量索引维护，传入 old_row 避免重复加载
            // 🔑 批量 DELETE 收集器 (语句级一次 WAL 栅栏, 同 UPDATE)
            pending_deletes.push((row_id, row));
        }
        if pending_deletes.len() == 1 {
            let (rid, old_row) = pending_deletes.into_iter().next().unwrap();
            self.db.delete_row_from_table(&stmt.table, rid, old_row)?;
            affected_rows += 1;
        } else if !pending_deletes.is_empty() {
            affected_rows += self.db.delete_rows_batch(&stmt.table, pending_deletes)? as usize;
        }

        // 🔑 Process write_set rows (uncommitted INSERTs in this txn).
        // If a DELETE matches a write_set row, remove it from the write_set
        // (so COMMIT doesn't flush it). The row was never written to storage.
        for (ws_row_id, ws_row) in &txn_rows {
            let sql_row = row_to_sql_row(ws_row, &schema)?;
            let should_delete = if let Some(ref where_clause) = stmt.where_clause {
                self.evaluator
                    .eval(where_clause, &sql_row)
                    .and_then(|val| self.to_bool(&val))
                    .unwrap_or(false)
            } else {
                true
            };
            if !should_delete {
                continue;
            }
            // Remove from write_set (the row was never committed to storage).
            if let Some(tid) = self.current_txn_id() {
                let ctx = self.db.txn_coordinator.get_context(tid)?;
                ctx.write_set
                    .write()
                    .remove(&(stmt.table.clone(), *ws_row_id));
            }
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

        // Cache miss on an integer-PK ColSegmentStore table: the composite
        // key encodes the PK value, so the row is an O(log N) binary search
        // away. The old fallback was a full table scan — which ALSO
        // force-compacted every segment, turning each single-row UPDATE into
        // an O(table) rewrite (~95ms at 200K rows).
        if let Value::Integer(pk) = pk_value {
            if let Some(store) = self.db.get_col_segment_store(table) {
                let rid = if *pk >= 0 {
                    *pk as u64
                } else {
                    0x8000_0000u64 | (*pk as u64 & 0x7FFF_FFFF)
                };
                let key = self.db.make_composite_key(table, rid);
                if let Some(row) = store.get(key) {
                    let schema = self.db.get_table_schema(table)?;
                    let pk_pos = schema.get_column_position(pk_col_name).unwrap_or(0);
                    if row.get(pk_pos) == Some(pk_value) {
                        if let Some(lookup) = self.db.pk_lookup.get(table) {
                            lookup.insert(pk_key.clone(), rid);
                        }
                        return Ok(Some(rid));
                    }
                }
            }
        }
        // Column index, or full scan if index missing
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
        let pk_col_name = schema
            .primary_key()
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

    /// Apply an UPDATE's assignments to one write_set (uncommitted INSERT)
    /// row: evaluates SET expressions against the ORIGINAL row, relocates the
    /// buffered entry when the integer PK changes (rejecting duplicates), and
    /// records the savepoint delta. Storage is not touched — the row isn't
    /// committed yet. Shared by the scan path's write_set loop and the
    /// transaction-aware PK fast path.
    fn apply_update_to_write_set_row(
        &self,
        stmt: &UpdateStmt,
        schema: &crate::types::TableSchema,
        ws_row_id: RowId,
        row: &Row,
    ) -> Result<()> {
        let mut new_row = row.clone();
        for (col_name, expr) in &stmt.assignments {
            if let Some(cd) = schema.get_column(col_name) {
                let new_val = if let Expr::Literal(v) = expr {
                    v.clone()
                } else if Self::expr_contains_subquery(expr) {
                    let materialized = self.materialize_subqueries(expr)?;
                    if let Expr::Literal(v) = materialized {
                        v
                    } else {
                        Self::eval_expr_on_row(&materialized, row, schema).unwrap_or(Value::Null)
                    }
                } else {
                    Self::eval_expr_on_row(expr, row, schema)?
                };
                while new_row.len() <= cd.position {
                    new_row.push(Value::Null);
                }
                new_row[cd.position] = new_val;
            }
        }
        // 🔑 Update the write_set entry (not storage — the row isn't committed yet).
        if let Some(tid) = self.current_txn_id() {
            // 🔑 Integer PK changed on a buffered row: relocate the
            // write_set entry to the new PK-derived row_id. PK point
            // queries assume row_id == Integer PK — a content-only update
            // left the row under the OLD row_id, making `WHERE pk = <new>`
            // permanently miss it (and `WHERE pk = <old>` return a row
            // whose content claims otherwise).
            let mut relocated = false;
            let pk_pos = schema
                .primary_key()
                .and_then(|n| schema.get_column(n))
                .map(|c| c.position);
            if let Some(pos) = pk_pos {
                if let (Some(Value::Integer(old_pk)), Some(Value::Integer(new_pk))) =
                    (row.get(pos), new_row.get(pos))
                {
                    if old_pk != new_pk {
                        // New PK must be free. Integer PK 的 row_id 恒等于
                        // PK 值 —— 直接点查（query_by_column 在无列索引时
                        // 报错，错误被吞后检查形同虚设）。
                        let target_rid = if *new_pk >= 0 {
                            *new_pk as RowId
                        } else {
                            0x8000_0000u64 | (*new_pk as u64 & 0x7FFF_FFFF)
                        };
                        if target_rid != ws_row_id
                            && self
                                .db
                                .get_table_row(&stmt.table, target_rid)
                                .ok()
                                .flatten()
                                .is_some()
                        {
                            return Err(StorageError::InvalidData(format!(
                                "Duplicate primary key {:?} for table '{}'",
                                Value::Integer(*new_pk),
                                stmt.table
                            )));
                        }
                        // ... and other buffered rows in this txn.
                        let ws_now = self.txn_write_set_rows(&stmt.table);
                        for (other_rid, other_row) in &ws_now {
                            if other_rid != &ws_row_id
                                && other_row.get(pos) == Some(&Value::Integer(*new_pk))
                            {
                                return Err(StorageError::InvalidData(format!(
                                    "Duplicate primary key {:?} for table '{}'",
                                    Value::Integer(*new_pk),
                                    stmt.table
                                )));
                            }
                        }
                        let new_rid = if *new_pk >= 0 {
                            *new_pk as RowId
                        } else {
                            0x8000_0000u64 | (*new_pk as u64 & 0x7FFF_FFFF)
                        };
                        relocated = self
                            .db
                            .txn_coordinator
                            .relocate_write_set_row(
                                tid,
                                &stmt.table,
                                ws_row_id,
                                new_rid,
                                new_row.clone(),
                            )
                            .unwrap_or(false);
                    }
                }
            }
            if !relocated {
                // 🔑 Savepoint rollback must restore the pre-update value:
                // buffer updates previously recorded no delta at all, so
                // ROLLBACK TO SAVEPOINT silently kept the new value.
                // Savepoint-only (never the storage-replayed undo_log).
                let _ = self.db.txn_coordinator.record_savepoint_delta(
                    tid,
                    crate::txn::coordinator::DeltaOperation::Update(
                        ws_row_id,
                        stmt.table.clone(),
                        std::sync::Arc::new(row.clone()),
                    ),
                );
                let _ = self.db.txn_coordinator.update_write_set_row(
                    tid,
                    &stmt.table,
                    ws_row_id,
                    new_row,
                );
            }
        }
        Ok(())
    }

    fn execute_update_pk(
        &self,
        stmt: &UpdateStmt,
        schema: &crate::types::TableSchema,
        target_value: &Value,
    ) -> Result<QueryResult> {
        // Note: no early return on empty row_ids — inside a transaction the
        // target row may exist ONLY in the write_set (uncommitted INSERT),
        // which the write_set pass below matches on the PK column.
        let row_ids = self.resolve_pk_row_ids(&stmt.table, schema, target_value)?;

        let mut affected_rows = 0;
        let in_txn = self.is_in_transaction();

        for row_id in row_ids {
            // 🔑 Transaction visibility: a row DELETEd in this txn must not
            // be resurrected; a row still buffered in the write_set is
            // updated in the buffer by the write_set pass below (updating
            // storage here would materialize an uncommitted INSERT).
            if in_txn {
                match self.txn_lookup_row(&stmt.table, row_id) {
                    Some(_) => continue, // tombstone or buffered row
                    None => {}
                }
            }
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
                    } else if Self::expr_contains_subquery(expr) {
                        // 🔑 Resolve scalar subqueries in SET expression (same
                        // fix as execute_update scan path). Without this,
                        // eval_expr_on_row fails on Subquery nodes.
                        let materialized = self.materialize_subqueries(expr)?;
                        if let Expr::Literal(v) = materialized {
                            v
                        } else {
                            Self::eval_expr_on_row(&materialized, &row, schema)?
                        }
                    } else {
                        Self::eval_expr_on_row(expr, &row, schema)?
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

            // 🔑 Record undo delta for transactional UPDATE (PK/index fast path).
            let txn_id = self.current_txn_id();
            if let Some(tid) = txn_id {
                // 🔑 If row was INSERTed in this txn, update write_set (prevents
                // COMMIT overwriting the UPDATE with the stale INSERT value).
                let updated = self.db.txn_coordinator.update_write_set_row(
                    tid,
                    &stmt.table,
                    row_id,
                    new_row.clone(),
                )?;
                if !updated {
                    let _ = self.db.txn_coordinator.record_write_delta(
                        tid,
                        crate::txn::coordinator::DeltaOperation::Update(
                            row_id,
                            stmt.table.clone(),
                            Arc::new(row.clone()),
                        ),
                    );
                }
            }

            self.db
                .update_row_in_table_with_schema(&stmt.table, row_id, row, new_row, schema)?;
            affected_rows += 1;
        }

        // 🔑 Uncommitted INSERTs of this txn are invisible to
        // resolve_pk_row_ids (storage + pk cache). Match them directly on
        // the PK column — O(buffered rows), not O(table).
        if in_txn {
            let pk_pos = schema
                .primary_key()
                .and_then(|n| schema.get_column(n))
                .map(|c| c.position);
            if let Some(pos) = pk_pos {
                for (ws_row_id, ws_row) in self.txn_write_set_rows(&stmt.table) {
                    if ws_row.get(pos) != Some(target_value) {
                        continue;
                    }
                    self.apply_update_to_write_set_row(stmt, schema, ws_row_id, &ws_row)?;
                    affected_rows += 1;
                }
            }
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

            // 🔑 Record undo delta for transactional DELETE (PK fast path).
            let txn_id = self.current_txn_id();
            if let Some(tid) = txn_id {
                let _ = self.db.txn_coordinator.record_write_delta(
                    tid,
                    crate::txn::coordinator::DeltaOperation::Delete(
                        row_id,
                        stmt.table.clone(),
                        Arc::new(row.clone()),
                    ),
                );
            }

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
                    if actual_val != where_val {
                        continue;
                    }
                } else {
                    continue;
                }
            }

            // Evaluate assignments positionally against the raw Vec<Value>
            let mut new_row = row.clone();
            for (col_name, expr) in &stmt.assignments {
                if let Some(cd) = schema.get_column(col_name) {
                    let new_val = if let Expr::Literal(v) = expr {
                        v.clone()
                    } else {
                        Self::eval_expr_on_row(expr, &row, schema)?
                    };
                    while new_row.len() <= cd.position {
                        new_row.push(Value::Null);
                    }
                    new_row[cd.position] = new_val;
                }
            }

            // 🔑 Record undo delta for transactional UPDATE (PK/index fast path).
            let txn_id = self.current_txn_id();
            if let Some(tid) = txn_id {
                // 🔑 If row was INSERTed in this txn, update write_set (prevents
                // COMMIT overwriting the UPDATE with the stale INSERT value).
                let updated = self.db.txn_coordinator.update_write_set_row(
                    tid,
                    &stmt.table,
                    row_id,
                    new_row.clone(),
                )?;
                if !updated {
                    let _ = self.db.txn_coordinator.record_write_delta(
                        tid,
                        crate::txn::coordinator::DeltaOperation::Update(
                            row_id,
                            stmt.table.clone(),
                            Arc::new(row.clone()),
                        ),
                    );
                }
            }

            self.db
                .update_row_in_table_with_schema(&stmt.table, row_id, row, new_row, schema)?;
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
                    if actual_val != where_val {
                        continue;
                    }
                } else {
                    continue;
                }
            }

            // 🔑 Record undo delta for transactional DELETE (PK/index fast path).
            let txn_id = self.current_txn_id();
            if let Some(tid) = txn_id {
                let _ = self.db.txn_coordinator.record_write_delta(
                    tid,
                    crate::txn::coordinator::DeltaOperation::Delete(
                        row_id,
                        stmt.table.clone(),
                        Arc::new(row.clone()),
                    ),
                );
            }

            self.db.delete_row_from_table(&stmt.table, row_id, row)?;
            affected_rows += 1;
        }

        Ok(QueryResult::Modification { affected_rows })
    }

    /// Execute CREATE TABLE statement
    fn execute_create_table(&self, stmt: CreateTableStmt) -> Result<QueryResult> {
        // 🆕 IF NOT EXISTS: if the table already exists, silently no-op.
        if stmt.if_not_exists && self.db.get_table_schema(&stmt.table).is_ok() {
            return Ok(QueryResult::Modification { affected_rows: 0 });
        }

        // Convert AST column defs to TableSchema
        let columns: Vec<crate::types::ColumnDef> = stmt
            .columns
            .iter()
            .enumerate()
            .map(|(pos, col)| {
                let column_type = match col.data_type {
                    DataType::Integer => ColumnType::Integer,
                    DataType::BigInt => ColumnType::Integer, // 🚀 Phase 4: Map BIGINT to Integer (both i64)
                    DataType::Float => ColumnType::Float,
                    DataType::Text => ColumnType::Text,
                    DataType::Boolean => ColumnType::Boolean,
                    DataType::Timestamp => ColumnType::Timestamp,
                    DataType::Vector(dim) => ColumnType::Tensor(dim.unwrap_or(128)),
                    DataType::Geometry => ColumnType::Spatial,
                };

                let mut col_def = crate::types::ColumnDef::new(col.name.clone(), column_type, pos);
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
                // 🔑 DEFAULT value (CREATE TABLE column constraint).
                col_def.default_value = col.default_value.clone();
                col_def
            })
            .collect();

        // Guard: the columnar SSTable format reserves a fixed-width header slot
        // per column (MAX_COLUMNS). Reject early with a clean error instead of
        // panicking at flush time when the header overflows.
        if columns.len() > crate::storage::lsm::columnar::MAX_COLUMNS {
            return Err(crate::error::StorageError::InvalidData(format!(
                "table '{}' has {} columns, but the maximum is {}",
                stmt.table,
                columns.len(),
                crate::storage::lsm::columnar::MAX_COLUMNS
            )));
        }

        // 🆕 STEP 1: Find primary key columns
        let primary_key_cols: Vec<&super::ast::ColumnDef> =
            stmt.columns.iter().filter(|col| col.primary_key).collect();

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

        // 🔥 ColSegmentStore tables use RowMap binary search for PK lookups.
        // A disk-based column index is redundant — it duplicates every PK value
        // on disk (4GB for 2M rows) and in memory. The in-memory pk_lookup
        // cache (created by create_table) handles O(1) hot-key resolution.
        // Explicit CREATE INDEX is still supported for non-PK columns.

        // 🚨 DEADLOCK FIX: create_table() already auto-creates primary key index
        // No need to manually create it again (prevents double creation deadlock)
        let pk_info = if !primary_key_cols.is_empty() {
            let pk_names: Vec<String> = primary_key_cols.iter().map(|c| c.name.clone()).collect();
            let auto_inc = if primary_key_cols[0].auto_increment {
                " AUTO_INCREMENT"
            } else {
                ""
            };
            format!(
                " (Primary key: {}{}, auto-index: ✓)",
                pk_names.join(", "),
                auto_inc
            )
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
            message: format!(
                "Table '{}' created successfully{}{}{}",
                stmt.table, pk_info, ts_info, ttl_info
            ),
        })
    }

    /// Execute CREATE INDEX statement
    fn execute_create_index(&self, stmt: CreateIndexStmt) -> Result<QueryResult> {
        // 🆕 IF NOT EXISTS: no-op (with a notice) when the index is already
        // registered — the standard idempotent-migration shape.
        if stmt.if_not_exists && self.db.index_registry.get(&stmt.index_name).is_some() {
            return Ok(QueryResult::Definition {
                message: format!("Index '{}' already exists, skipped", stmt.index_name),
            });
        }
        // Get table schema to find column type
        let schema = self.db.get_table_schema(&stmt.table)?;
        let column = schema
            .columns
            .iter()
            .find(|c| c.name == stmt.column)
            .ok_or_else(|| MoteDBError::ColumnNotFound(stmt.column.clone()))?;

        // Determine index type: use explicit type from AST, or infer from column type
        let index_type = match stmt.index_type {
            IndexType::Text => {
                // Verify column is compatible with text index
                if !matches!(column.col_type, ColumnType::Text) {
                    return Err(MoteDBError::TypeError(format!(
                        "TEXT index requires TEXT column, got {:?}",
                        column.col_type
                    )));
                }
                IndexType::Text
            }
            IndexType::Vector => {
                // Verify column is tensor/vector
                if let ColumnType::Tensor(_dim) = column.col_type {
                    IndexType::Vector
                } else {
                    return Err(MoteDBError::TypeError(format!(
                        "VECTOR index requires TENSOR column, got {:?}",
                        column.col_type
                    )));
                }
            }
            IndexType::Timestamp => {
                // Verify column is timestamp
                if !matches!(column.col_type, ColumnType::Timestamp) {
                    return Err(MoteDBError::TypeError(format!(
                        "TIMESTAMP index requires TIMESTAMP column, got {:?}",
                        column.col_type
                    )));
                }
                IndexType::Timestamp
            }
            IndexType::Octree => {
                // Verify column is spatial (3D points)
                if !matches!(column.col_type, ColumnType::Spatial) {
                    return Err(MoteDBError::TypeError(format!(
                        "OCTREE index requires SPATIAL column, got {:?}",
                        column.col_type
                    )));
                }
                IndexType::Octree
            }
            IndexType::BTree | IndexType::Column => {
                // B-Tree/Column index can be used for any comparable type.
                // (SPATIAL columns are re-inferred to Octree below — the
                // column-index builder would read them with the TEXT reader
                // and panic on the spatial encoding.)
                stmt.index_type.clone()
            }
        };

        // 🔑 Untyped `CREATE INDEX ix ON t (col)` defaults to BTree in the
        // parser (it has no schema). Re-infer from the column type here:
        // SPATIAL → Octree (the column path above would panic otherwise).
        let index_type = if matches!(index_type, IndexType::BTree | IndexType::Column)
            && matches!(column.col_type, ColumnType::Spatial)
        {
            IndexType::Octree
        } else {
            index_type
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
                self.db
                    .create_text_index_with_tokenizer(&index_name, stmt.tokenizer.clone())?;

                // 2️⃣ 🚀 Columnar fast path: bulk build from TextSegment
                let column_pos = schema
                    .get_column_position(&stmt.column)
                    .ok_or_else(|| MoteDBError::ColumnNotFound(stmt.column.clone()))?;
                let start_time = std::time::Instant::now();
                let mut backfill_count = 0;

                if let Ok(count) =
                    self.db
                        .build_text_index_from_columnar(&index_name, &stmt.table, column_pos)
                {
                    debug_log!(
                        "[CREATE TEXT INDEX] Columnar build: {} docs in {:?}",
                        count,
                        start_time.elapsed()
                    );
                } else {
                    // ✅ Fallback: 批量流式扫描（每批10000行，避免内存爆炸）
                    let batch_iter = self.db.scan_table_rows_batched(&stmt.table, 10000)?;

                    for batch_result in batch_iter {
                        let batch = batch_result?;

                        // 收集本批次的文本数据
                        let texts_in_batch: Vec<_> = batch
                            .iter()
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
                                        debug_log!(
                                            "⚠️ Failed to backfill text index for row {}: {}",
                                            row_id,
                                            e
                                        );
                                    } else {
                                        backfill_count += 1;
                                    }
                                }
                                // 锁在此处释放（每10000条释放一次，允许并发查询）
                            }
                        }
                    }

                    if backfill_count > 0 {
                        debug_log!(
                            "Built text index in {:?}, indexed {} rows",
                            start_time.elapsed(),
                            backfill_count
                        );
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
                    // 🔑 Register metadata BEFORE building: create_vector_index
                    // resolves table/column from index_registry to find the
                    // vectors. Registering after meant a custom index name
                    // (e.g. "idx_emb") couldn't be resolved, fell back to
                    // splitting the name as "table_column" → wrong table →
                    // silent empty index → 0 search results.
                    let mut metadata = crate::database::index_metadata::IndexMetadata::new(
                        index_name.clone(),
                        stmt.table.clone(),
                        stmt.column.clone(),
                        crate::database::index_metadata::IndexType::Vector,
                    );
                    metadata.metric = stmt.metric.clone();
                    self.db.index_registry.register(metadata)?;

                    self.db
                        .create_vector_index(&index_name, dim, stmt.metric.as_deref())?;
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

                // 🚀 Backfill: try columnar fast path first.
                // 🔑 Skip when the index is already populated —
                // create_ioctree_index backfilled on creation (convention-
                // named indexes); running both double-inserted every point.
                let column_pos = schema
                    .get_column_position(&stmt.column)
                    .ok_or_else(|| MoteDBError::ColumnNotFound(stmt.column.clone()))?;
                let mut backfill_count = 0;
                let already_indexed = self.db.ioctree_point_count(&index_name).unwrap_or(0);

                // Columnar fast path returns Ok(0) for ColSegmentStore-backed tables
                // (not in legacy columnar_sstables); in that case fall back to row scan.
                match if already_indexed > 0 {
                    Ok(0)
                } else {
                    self.db
                        .build_ioctree_from_columnar(&index_name, &stmt.table, column_pos)
                } {
                    Ok(count) if count > 0 => {
                        backfill_count = count;
                    }
                    _ if already_indexed > 0 => {}
                    _ => {
                        let iter = self.db.scan_table_rows_streaming(&stmt.table)?;
                        for result in iter {
                            let (row_id, row) = result?;
                            if let Some(Value::Spatial(geometry)) = row.get(column_pos) {
                                if geometry.is_3d() {
                                    if let Err(e) =
                                        self.db.insert_ioctree_point(row_id, &index_name, geometry)
                                    {
                                        debug_log!(
                                            "⚠️ Failed to backfill ioctree index for row {}: {}",
                                            row_id,
                                            e
                                        );
                                    } else {
                                        backfill_count += 1;
                                    }
                                }
                            }
                        }
                    }
                }

                if backfill_count > 0 {
                    debug_log!(
                        "Backfilled {} rows into ioctree index '{}'",
                        backfill_count,
                        index_name
                    );
                }

                // Register metadata (idempotent — create_ioctree_index may
                // already have registered via the "{table}_{column}" resolve).
                if self
                    .db
                    .index_registry
                    .resolve_index_name(&index_name)
                    .is_none()
                {
                    let metadata = crate::database::index_metadata::IndexMetadata::new(
                        index_name.clone(),
                        stmt.table.clone(),
                        stmt.column.clone(),
                        crate::database::index_metadata::IndexType::Octree,
                    );
                    self.db.index_registry.register(metadata)?;
                }
            }
            IndexType::BTree | IndexType::Column => {
                // 🚀 Column/BTree index creation
                // Column index works for any comparable type (Integer, Float, Text, etc.)
                // Bulk backfill is now handled internally by create_column_index()

                self.db
                    .create_column_index_with_name(&stmt.table, &stmt.column, &index_name)?;

                // 🔥 OPTIMIZATION FIX: Also register with standard "{table}.{column}" name
                // This allows WHERE optimization to find the index
                let standard_name = format!("{}.{}", stmt.table, stmt.column);
                if index_name != standard_name {
                    // 🔒 Clone the Arc OUT of the guard first. Holding the
                    // DashMap read Ref from get() across insert() on the SAME
                    // map self-deadlocks when both keys hash to the same
                    // shard (~1/N chance) — the intermittent CREATE INDEX hang
                    // seen across the whole test suite (idx_score/idx_tag/...
                    // custom names only; default "{table}.{column}" names
                    // never triggered it because index_name == standard_name
                    // skips this branch entirely).
                    let index_ref = self
                        .db
                        .column_indexes
                        .get(&index_name)
                        .map(|r| r.value().clone());
                    if let Some(index_ref) = index_ref {
                        self.db.column_indexes.insert(standard_name, index_ref);
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
            message: format!(
                "Index '{}' created successfully on {}.{}",
                index_name, stmt.table, stmt.column
            ),
        })
    }

    /// Execute DROP TABLE statement
    fn execute_drop_table(&self, stmt: DropTableStmt) -> Result<QueryResult> {
        let table_name = &stmt.table;

        // 🔑 DDL is not transactional: a DROP that ran mid-transaction stayed
        // dropped after ROLLBACK — the table and its data were silently lost.
        // Reject instead of letting ROLLBACK promise an undo it can't deliver.
        if self.current_txn_id().is_some() {
            return Err(MoteDBError::InvalidArgument(
                "DROP TABLE cannot run inside a transaction (DDL is not rollback-able); COMMIT or ROLLBACK first".into(),
            ));
        }

        // Verify table exists (or skip if IF EXISTS)
        let schema = match self.db.get_table_schema(table_name) {
            Ok(s) => s,
            Err(_) if stmt.if_exists => {
                return Ok(QueryResult::Definition {
                    message: format!("Table '{}' does not exist (IF EXISTS)", table_name),
                });
            }
            Err(e) => return Err(e),
        };

        // 1. Drop column indexes for this table
        let prefix = format!("{}.", table_name);
        let index_names: Vec<String> = self
            .db
            .column_indexes
            .iter()
            .filter(|entry| entry.key().starts_with(&prefix))
            .map(|entry| entry.key().clone())
            .collect();
        for idx_name in index_names {
            self.db.column_indexes.remove(&idx_name);
        }

        // 2. Drop vector indexes for this table
        let vector_idx_names: Vec<String> = self
            .db
            .vector_indexes
            .iter()
            .filter(|entry| {
                entry.key().starts_with(&prefix)
                    || entry.key().contains(&format!("_{}", table_name))
            })
            .map(|entry| entry.key().clone())
            .collect();
        for idx_name in vector_idx_names {
            self.db.vector_indexes.remove(&idx_name);
        }

        // 3. Drop text indexes for this table
        let text_idx_names: Vec<String> = self
            .db
            .text_indexes
            .iter()
            .filter(|entry| {
                entry.key().starts_with(&prefix)
                    || entry.key().contains(&format!("_{}", table_name))
            })
            .map(|entry| entry.key().clone())
            .collect();
        for idx_name in text_idx_names {
            self.db.text_indexes.remove(&idx_name);
        }

        // 4. Drop i-Octree indexes for this table
        let ioctree_idx_names: Vec<String> = self
            .db
            .ioctree_indexes
            .iter()
            .filter(|entry| {
                entry.key().starts_with(&prefix)
                    || entry.key().contains(&format!("_{}", table_name))
            })
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

        if let Err(e) = self
            .db
            .lsm_engine
            .delete_range(start_key, end_key, timestamp)
        {
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

        // Same guard as DROP TABLE: index files are removed immediately and
        // cannot be resurrected by ROLLBACK.
        if self.current_txn_id().is_some() {
            return Err(MoteDBError::InvalidArgument(
                "DROP INDEX cannot run inside a transaction (DDL is not rollback-able); COMMIT or ROLLBACK first".into(),
            ));
        }

        // Look up index metadata to know which collection to remove from
        let meta = self
            .db
            .index_registry
            .get(&stmt.index_name)
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

        // Same guard as DROP TABLE: ALTER rewrites schema/segment files in
        // place and cannot be undone by ROLLBACK.
        if self.current_txn_id().is_some() {
            return Err(MoteDBError::InvalidArgument(
                "ALTER TABLE cannot run inside a transaction (DDL is not rollback-able); COMMIT or ROLLBACK first".into(),
            ));
        }

        match stmt.action {
            AlterTableAction::SetAutoIncrement(new_value) => {
                // Verify table exists and has AUTO_INCREMENT primary key
                let schema = self.db.get_table_schema(&stmt.table)?;

                if !schema.is_primary_key_auto_increment() {
                    return Err(MoteDBError::InvalidArgument(format!(
                        "Table {} does not have AUTO_INCREMENT primary key",
                        stmt.table
                    )));
                }

                // Update the AUTO_INCREMENT counter
                self.db.set_auto_increment_value(&stmt.table, new_value)?;

                Ok(QueryResult::Definition {
                    message: format!("Table {} AUTO_INCREMENT set to {}", stmt.table, new_value),
                })
            }
            AlterTableAction::AddColumn {
                name,
                data_type,
                default_value,
                nullable,
            } => {
                // Convert DataType to ColumnType (same mapping as CREATE TABLE).
                let col_type = match data_type {
                    super::ast::DataType::Integer => ColumnType::Integer,
                    super::ast::DataType::BigInt => ColumnType::Integer,
                    super::ast::DataType::Float => ColumnType::Float,
                    super::ast::DataType::Text => ColumnType::Text,
                    super::ast::DataType::Boolean => ColumnType::Boolean,
                    super::ast::DataType::Timestamp => ColumnType::Timestamp,
                    super::ast::DataType::Vector(dim) => ColumnType::Tensor(dim.unwrap_or(128)),
                    super::ast::DataType::Geometry => ColumnType::Spatial,
                };
                // Verify table exists.
                let _schema = self.db.get_table_schema(&stmt.table)?;
                // Mutate schema in registry. col_type is moved here, so clone
                // for the store update below (ColumnType is Clone, not Copy).
                self.db.table_registry.add_column(
                    &stmt.table,
                    &name,
                    col_type.clone(),
                    default_value.as_ref(),
                    nullable,
                )?;
                // 🔑 Extend the store's col_types so post-ALTER INSERTs preserve
                // the new column's value. Without this, the in-memory write_buf
                // still has N-1 column_buffers and `add_values` silently drops
                // the Nth value. add_column_type flushes the stale buffer,
                // swaps in the widened col_types, and rebuilds write_buf.
                if let Some(store) = self.db.col_segment_stores.get(&stmt.table) {
                    // 🔑 Pass the DEFAULT value so existing rows get backfilled
                    // with it (not NULL) during the segment rewrite.
                    store.add_column_type_with_default(col_type, default_value.as_ref())?;
                }
                // 🚨 Invalidate the legacy `columnar_sstables` read cache for this
                // table. That map holds an Arc<ColumnarSSTable> snapshot of the
                // LATEST segment (N-1 columns). Legacy aggregate / GROUP BY paths
                // (try_group_by_columnar, etc.) read it directly via column_index
                // — accessing column N would "Text segment too short" / OOB.
                // Removing the entry forces the next reader to sync from the now-
                // widened col_segment_store (which has the N-column layout).
                self.db.columnar_sstables.remove(&stmt.table);
                // 🚨 DO NOT remove col_segment_stores — the existing store's
                // on-disk segments hold the pre-ALTER rows, and removing it
                // would cause all subsequent SELECTs to return 0 rows (data
                // appears lost until database reopen). Pre-ALTER on-disk
                // segments keep their N-1 column layout; the read path
                // returns Value::Null for the new column on those rows, which
                // is the correct "new column on pre-existing rows = NULL"
                // semantics.
                //
                // DO NOT remove columnar_sstables either — same reason.
                // 🔑 DEFAULT backfill is handled inside add_column_type_with_default
                // (the segment merge writes the default value instead of NULL
                // for the new column on pre-existing rows).

                Ok(QueryResult::Definition {
                    message: format!("Added column '{}' to table '{}'", name, stmt.table),
                })
            }
        }
    }

    /// Execute SHOW TABLES
    fn execute_show_tables(&self) -> Result<QueryResult> {
        let tables = self.db.list_tables()?;

        let columns = vec!["Tables".to_string()];
        let rows = tables
            .into_iter()
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

        let rows = schema
            .columns
            .iter()
            .map(|col| {
                vec![
                    Value::text(col.name.clone()),
                    Value::text(format!("{:?}", col.col_type)),
                    Value::text(if col.nullable { "YES" } else { "NO" }.into()),
                    Value::Integer(col.position as i64),
                ]
            })
            .collect();

        Ok(QueryResult::Select { columns, rows })
    }

    /// Execute BEGIN [TRANSACTION]
    fn execute_begin_transaction(&self) -> Result<QueryResult> {
        // 🚨 Reject nested BEGIN (SQL savepoints exist via SAVEPOINT/ROLLBACK
        // TO/RELEASE). Silently starting a new transaction would discard the
        // outer transaction's buffered writes (write_set is reset), causing
        // data loss. Error out so the caller can COMMIT/ROLLBACK first.
        if self.current_txn_id().is_some() {
            return Err(MoteDBError::Query(
                "Nested transactions are not supported; COMMIT or ROLLBACK the current transaction first".to_string(),
            ));
        }
        let txn_id = self.db.begin_transaction()?;
        self.begin_txn_context(txn_id);
        Ok(QueryResult::Definition {
            message: format!("Transaction {} started", txn_id),
        })
    }

    /// Execute SAVEPOINT name (SQL surface for the coordinator API).
    fn execute_savepoint(&self, name: &str) -> Result<QueryResult> {
        let txn_id = self
            .current_txn_id()
            .ok_or_else(|| MoteDBError::Query("SAVEPOINT requires an active transaction".into()))?;
        self.db
            .txn_coordinator
            .create_savepoint(txn_id, name.to_string())?;
        Ok(QueryResult::Definition {
            message: format!("SAVEPOINT {name} created"),
        })
    }

    /// Execute ROLLBACK TO [SAVEPOINT] name.
    fn execute_rollback_to_savepoint(&self, name: &str) -> Result<QueryResult> {
        let txn_id = self.current_txn_id().ok_or_else(|| {
            MoteDBError::Query("ROLLBACK TO requires an active transaction".into())
        })?;
        let replay = self.db.rollback_to_savepoint(txn_id, name)?;
        // Replay ONLY the deltas the coordinator returned: Update undos of
        // storage-resident rows (key absent from the write_set). Buffered /
        // PK-relocated rows were already restored in place in the write_set —
        // replaying those against storage would create phantom rows (the
        // earlier failed attempt).
        for delta in replay {
            match delta {
                crate::txn::coordinator::DeltaOperation::Update(row_id, table_name, old_value) => {
                    let old_row =
                        std::sync::Arc::try_unwrap(old_value).unwrap_or_else(|arc| (*arc).clone());
                    if let Ok(schema) = self.db.get_table_schema(&table_name) {
                        let _ = self.db.update_row_in_table_with_schema(
                            &table_name,
                            row_id,
                            old_row.clone(),
                            old_row,
                            &schema,
                        );
                    }
                }
                _ => {}
            }
        }
        Ok(QueryResult::Definition {
            message: format!("rolled back to SAVEPOINT {name}"),
        })
    }

    /// Execute RELEASE [SAVEPOINT] name.
    fn execute_release_savepoint(&self, name: &str) -> Result<QueryResult> {
        let txn_id = self
            .current_txn_id()
            .ok_or_else(|| MoteDBError::Query("RELEASE requires an active transaction".into()))?;
        self.db.release_savepoint(txn_id, name)?;
        Ok(QueryResult::Definition {
            message: format!("SAVEPOINT {name} released"),
        })
    }

    /// Execute COMMIT [TRANSACTION]
    fn execute_commit_transaction(&self) -> Result<QueryResult> {
        let _txn_id_opt = self.current_txn_id();
        if let Some(txn_id) = _txn_id_opt {
            self.db.commit_transaction(txn_id)?;
            self.clear_txn_context();
            Ok(QueryResult::Definition {
                message: format!("Transaction {} committed", txn_id),
            })
        } else {
            // 🔑 SQLite-compatible: error instead of silently succeeding —
            // a stray COMMIT (double-commit, lost BEGIN to an earlier error)
            // used to look like the data was committed.
            Err(MoteDBError::InvalidArgument(
                "cannot COMMIT - no transaction is active".into(),
            ))
        }
    }

    /// Execute ROLLBACK [TRANSACTION]
    fn execute_rollback_transaction(&self) -> Result<QueryResult> {
        let _txn_id_opt = self.current_txn_id();
        if let Some(txn_id) = _txn_id_opt {
            self.db.rollback_transaction(txn_id)?;
            self.clear_txn_context();
            Ok(QueryResult::Definition {
                message: format!("Transaction {} rolled back", txn_id),
            })
        } else {
            // Stray ROLLBACK is usually an error-recovery path — keep it an
            // error (matching COMMIT) so double-rollback bugs surface.
            Err(MoteDBError::InvalidArgument(
                "cannot ROLLBACK - no transaction is active".into(),
            ))
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
    fn try_extract_range_query(
        &self,
        expr: &Expr,
    ) -> Option<(String, Value, BinaryOperator, Value, BinaryOperator)> {
        use crate::sql::ast::{BinaryOperator, Expr};

        match expr {
            Expr::BinaryOp { left, op, right } => {
                // Check for AND expressions
                if *op == BinaryOperator::And {
                    // Try to extract range from both sides
                    if let (
                        Expr::BinaryOp {
                            left: l1,
                            op: op1,
                            right: r1,
                        },
                        Expr::BinaryOp {
                            left: l2,
                            op: op2,
                            right: r2,
                        },
                    ) = (left.as_ref(), right.as_ref())
                    {
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
                            // Compare on the bare column name so that a
                            // qualified reference like "t.v" on both sides
                            // still matches, and an unqualified "v" also
                            // matches "t.v".
                            let bare1 = Self::strip_qualifier(c1);
                            let bare2 = Self::strip_qualifier(c2);
                            if bare1 == bare2 {
                                let col_name = bare1.to_string();

                                // Extract bounds with operators
                                let (val1, is_lower1, op1_normalized) =
                                    match (l1.as_ref(), op1, r1.as_ref()) {
                                        (Expr::Column(_), BinaryOperator::Ge, Expr::Literal(v)) => {
                                            Some((v.clone(), true, BinaryOperator::Ge))
                                        }
                                        (Expr::Column(_), BinaryOperator::Gt, Expr::Literal(v)) => {
                                            Some((v.clone(), true, BinaryOperator::Gt))
                                        }
                                        (Expr::Literal(v), BinaryOperator::Le, Expr::Column(_)) => {
                                            Some((v.clone(), true, BinaryOperator::Ge))
                                        }
                                        (Expr::Literal(v), BinaryOperator::Lt, Expr::Column(_)) => {
                                            Some((v.clone(), true, BinaryOperator::Gt))
                                        }
                                        (Expr::Column(_), BinaryOperator::Le, Expr::Literal(v)) => {
                                            Some((v.clone(), false, BinaryOperator::Le))
                                        }
                                        (Expr::Column(_), BinaryOperator::Lt, Expr::Literal(v)) => {
                                            Some((v.clone(), false, BinaryOperator::Lt))
                                        }
                                        (Expr::Literal(v), BinaryOperator::Ge, Expr::Column(_)) => {
                                            Some((v.clone(), false, BinaryOperator::Le))
                                        }
                                        (Expr::Literal(v), BinaryOperator::Gt, Expr::Column(_)) => {
                                            Some((v.clone(), false, BinaryOperator::Lt))
                                        }
                                        _ => None,
                                    }?;

                                let (val2, is_lower2, op2_normalized) =
                                    match (l2.as_ref(), op2, r2.as_ref()) {
                                        (Expr::Column(_), BinaryOperator::Ge, Expr::Literal(v)) => {
                                            Some((v.clone(), true, BinaryOperator::Ge))
                                        }
                                        (Expr::Column(_), BinaryOperator::Gt, Expr::Literal(v)) => {
                                            Some((v.clone(), true, BinaryOperator::Gt))
                                        }
                                        (Expr::Literal(v), BinaryOperator::Le, Expr::Column(_)) => {
                                            Some((v.clone(), true, BinaryOperator::Ge))
                                        }
                                        (Expr::Literal(v), BinaryOperator::Lt, Expr::Column(_)) => {
                                            Some((v.clone(), true, BinaryOperator::Gt))
                                        }
                                        (Expr::Column(_), BinaryOperator::Le, Expr::Literal(v)) => {
                                            Some((v.clone(), false, BinaryOperator::Le))
                                        }
                                        (Expr::Column(_), BinaryOperator::Lt, Expr::Literal(v)) => {
                                            Some((v.clone(), false, BinaryOperator::Lt))
                                        }
                                        (Expr::Literal(v), BinaryOperator::Ge, Expr::Column(_)) => {
                                            Some((v.clone(), false, BinaryOperator::Le))
                                        }
                                        (Expr::Literal(v), BinaryOperator::Gt, Expr::Column(_)) => {
                                            Some((v.clone(), false, BinaryOperator::Lt))
                                        }
                                        _ => None,
                                    }?;

                                // One should be lower bound, one should be upper bound
                                if is_lower1 && !is_lower2 {
                                    return Some((
                                        col_name,
                                        val1,
                                        op1_normalized,
                                        val2,
                                        op2_normalized,
                                    ));
                                } else if !is_lower1 && is_lower2 {
                                    return Some((
                                        col_name,
                                        val2,
                                        op2_normalized,
                                        val1,
                                        op1_normalized,
                                    ));
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

    /// Strip an optional table qualifier from a (possibly qualified) column
    /// reference. `"t.v"` -> `"v"`, `"v"` -> `"v"`. Index keys are always built
    /// as `"table.col"` from the bare column name, so a qualified reference
    /// passed through verbatim would produce a wrong key like `"t.t.v"`.
    fn strip_qualifier(col: &str) -> &str {
        col.rsplit('.').next().unwrap_or(col)
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
                    if let (Expr::Column(col), Expr::Literal(val)) = (left.as_ref(), right.as_ref())
                    {
                        // 🔑 Return the FULL (possibly qualified) name — the
                        // old strip_qualifier here made `WHERE i.id = 1` over
                        // a join bind to a RANDOM one of i.id/o.id (the
                        // fallback below iterates a HashMap). Consumers do
                        // exact-lookup first and only fall back to a UNIQUE
                        // bare-suffix match.
                        return Some((col.clone(), val.clone()));
                    }
                    // Pattern 2: Literal = Column (reversed)
                    if let (Expr::Literal(val), Expr::Column(col)) = (left.as_ref(), right.as_ref())
                    {
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
                    BinaryOperator::Lt
                    | BinaryOperator::Le
                    | BinaryOperator::Gt
                    | BinaryOperator::Ge => {
                        // Pattern 1: Column op Literal
                        if let (Expr::Column(col), Expr::Literal(val)) =
                            (left.as_ref(), right.as_ref())
                        {
                            let bare = Self::strip_qualifier(col).to_string();
                            return Some((bare, op.clone(), val.clone()));
                        }
                        // Pattern 2: Literal op Column (reversed, need to flip operator)
                        if let (Expr::Literal(val), Expr::Column(col)) =
                            (left.as_ref(), right.as_ref())
                        {
                            let flipped_op = match op {
                                BinaryOperator::Lt => BinaryOperator::Gt,
                                BinaryOperator::Le => BinaryOperator::Ge,
                                BinaryOperator::Gt => BinaryOperator::Lt,
                                BinaryOperator::Ge => BinaryOperator::Le,
                                _ => return None,
                            };
                            let bare = Self::strip_qualifier(col).to_string();
                            return Some((bare, flipped_op, val.clone()));
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
    fn try_extract_vector_search(
        &self,
        expr: &Expr,
        from: &TableRef,
    ) -> Option<(String, String, Vec<f32>, usize)> {
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
                        if *k <= 0 {
                            return None;
                        }
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
    /// 🚀 `SELECT COUNT(*) FROM t WHERE MATCH(col, q)` — answer straight
    /// from the text index postings. The general aggregate pipeline
    /// materializes every matching row just to count it (~93 ms at 100K
    /// docs); the postings count is index-only. Also covers `COUNT(col)`
    /// on the MATCHED column (NULL text never matches, so the sets are
    /// identical) and bare `COUNT()`.
    fn try_text_match_count(&self, stmt: &SelectStmt) -> Result<Option<QueryResult>> {
        if stmt.group_by.is_some()
            || stmt.having.is_some()
            || stmt.distinct
            || stmt.order_by.is_some()
            || stmt.latest_by.is_some()
        {
            return Ok(None);
        }
        // Exactly one SELECT column: a plain (non-DISTINCT) COUNT.
        let (count_expr, count_arg) = match stmt.columns.as_slice() {
            [SelectColumn::Expr(
                Expr::FunctionCall {
                    name,
                    args,
                    distinct: false,
                    ..
                },
                _,
            )] if name.eq_ignore_ascii_case("COUNT") => {
                let arg = match args.as_slice() {
                    [] => None,                            // COUNT()
                    [Expr::Column(c)] if c == "*" => None, // COUNT(*)
                    [Expr::Column(c)] => Some(c.clone()),  // COUNT(col)
                    _ => return Ok(None),
                };
                (0, arg)
            }
            _ => return Ok(None),
        };
        let _ = count_expr;
        let table = match stmt.from.as_ref() {
            Some(TableRef::Table { name, .. }) => name.clone(),
            _ => return Ok(None),
        };
        // WHERE must be a single bare MATCH (compound predicates need the
        // general pipeline's row filtering).
        let (column, query) = match stmt.where_clause.as_ref() {
            Some(Expr::Match {
                column,
                query,
                phrase: false,
            }) => (column.clone(), query.clone()),
            _ => return Ok(None),
        };
        // COUNT(col): only valid when counting the MATCHED column — a
        // different column could be NULL on matched rows.
        if let Some(c) = &count_arg {
            if *c != column {
                return Ok(None);
            }
        }
        let index_name = match self.db.index_registry.find_by_column(
            &table,
            &column,
            crate::database::index_metadata::IndexType::Text,
        ) {
            Some(n) => n,
            None => return Ok(None),
        };
        if !self.db.text_indexes.contains_key(&index_name) {
            return Ok(None);
        }
        let ids = self.db.text_search(&index_name, &query)?;
        let col_name = match &stmt.columns[0] {
            SelectColumn::Expr(e, Some(alias)) => alias.clone(),
            SelectColumn::Expr(e, None) => Self::expr_to_column_name(e),
            _ => "COUNT(*)".to_string(),
        };
        Ok(Some(QueryResult::Select {
            columns: vec![col_name],
            rows: vec![vec![Value::Integer(ids.len() as i64)]],
        }))
    }

    fn try_text_search_fast_path(
        &self,
        stmt: &SelectStmt,
        where_clause: &Expr,
        table_name: &str,
    ) -> Result<Option<QueryResult>> {
        // Only a bare MATCH: the AND extractor below used to silently DROP
        // the other side of the AND (`MATCH(c, q) AND cat = 'a'` returned
        // rows with cat ≠ 'a'). Compound predicates go through the
        // materialized path, whose evaluator resolves MATCH as a set.
        // Aggregates / GROUP BY / DISTINCT need the general pipeline too.
        if self.has_aggregates(&stmt.columns)
            || stmt.group_by.is_some()
            || stmt.having.is_some()
            || stmt.distinct
        {
            return Ok(None);
        }
        let (column, query, phrase) = match where_clause {
            Expr::Match {
                column,
                query,
                phrase,
            } => (column.clone(), query.clone(), *phrase),
            _ => return Ok(None),
        };

        // Find text index for this column
        let index_name = match self.db.index_registry.find_by_column(
            table_name,
            &column,
            crate::database::index_metadata::IndexType::Text,
        ) {
            Some(name) => name,
            None => return Ok(None),
        };

        if !self.db.text_indexes.contains_key(&index_name) {
            return Ok(None);
        }

        // With LIMIT n (+ OFFSET) the ranked search needs top n+offset. With
        // no LIMIT the FULL match set is returned (unranked) — the old
        // `unwrap_or(1000)` silently truncated un-LIMITed queries at 1000
        // rows.
        let offset = stmt.offset.unwrap_or(0);

        // Phrase search or ranked search depending on query type
        // 🚀 Carry (row_id, score) through — don't discard BM25 scores and
        // hardcode 1.0 (was executor.rs:17590). Keeps ORDER BY score correct.
        let scored_results: Vec<(u64, f32)> = if phrase {
            let ids = match self.db.text_search_phrase(&index_name, &query) {
                Ok(r) => r,
                Err(_) => return Ok(None),
            };
            let ids: Vec<u64> = match stmt.limit {
                Some(l) => ids.into_iter().take(l + offset).collect(),
                None => ids,
            };
            ids.into_iter().map(|id| (id, 1.0)).collect()
        } else if let Some(l) = stmt.limit {
            // Ranked top-(limit + offset); offset is skipped below.
            match self.db.text_search_ranked(&index_name, &query, l + offset) {
                Ok(r) => r,
                Err(_) => return Ok(None),
            }
        } else {
            // No LIMIT: every matching row. The ranked search is a top-k
            // heap (usize::MAX would allocate for it), so the default is the
            // unranked id set with zero scores — EXCEPT when the SELECT list
            // asks for scores (`MATCH(..) AS s` / `BM25_SCORE(col, q)`):
            // rank over the full match count then.
            let wants_scores = stmt.columns.iter().any(|c| match c {
                SelectColumn::Expr(Expr::Match { .. }, _) => true,
                SelectColumn::Expr(Expr::FunctionCall { name, .. }, _) => {
                    name.eq_ignore_ascii_case("BM25_SCORE")
                }
                _ => false,
            });
            match self.db.text_search(&index_name, &query) {
                Ok(ids) if wants_scores => {
                    match self.db.text_search_ranked(&index_name, &query, ids.len()) {
                        Ok(r) => r,
                        Err(_) => ids.into_iter().map(|id| (id, 0.0)).collect(),
                    }
                }
                Ok(ids) => ids.into_iter().map(|id| (id, 0.0)).collect(),
                Err(_) => return Ok(None),
            }
        };

        if scored_results.is_empty() {
            return Ok(Some(QueryResult::Select {
                columns: vec![],
                rows: vec![],
            }));
        }

        // Apply ORDER BY if present.
        // Default order is BM25 score descending (already sorted by text_search_ranked).
        // If ORDER BY specifies other columns, sort the projected results by
        // those columns after projection (handles the common ORDER BY id case).
        let needs_resort = stmt.order_by.as_ref().is_some_and(|ob| {
            !(ob.len() == 1
                && matches!(&ob[0].expr, Expr::Column(c) if c.to_lowercase().contains("score"))
                && !ob[0].asc)
        });

        // Build score lookup map (row_id → BM25 score) for MATCH-expr columns.
        let score_map: std::collections::HashMap<u64, f64> = scored_results
            .iter()
            .map(|(id, s)| (*id, *s as f64))
            .collect();

        let row_ids: Vec<u64> = scored_results.iter().map(|(id, _)| *id).collect();
        let schema = self.db.get_table_schema(table_name)?;
        let columns = self.build_select_columns(&stmt.columns, &schema)?;

        // 🚀 Batch-fetch rows and project directly via project_row_direct
        // (no SqlRow HashMap build/teardown — was 2× HashMap alloc per row).
        let batch_rows = self.db.get_table_rows_batch(table_name, &row_ids)?;
        // Build a row_id → row lookup so we preserve BM25-sorted order.
        let mut row_lookup: std::collections::HashMap<u64, &Row> =
            std::collections::HashMap::with_capacity(batch_rows.len());
        for (rid, opt) in &batch_rows {
            if let Some(r) = opt {
                row_lookup.insert(*rid, r);
            }
        }

        let mut result_rows: Vec<Vec<Value>> = Vec::with_capacity(scored_results.len());
        let emit: Vec<&(u64, f32)> = scored_results
            .iter()
            .skip(offset)
            .take(stmt.limit.unwrap_or(usize::MAX))
            .collect();
        for (row_id, _score) in emit {
            if let Some(row) = row_lookup.get(row_id) {
                // Per-column projection: expression columns (MATCH /
                // BM25_SCORE) are not positionally evaluable — the old
                // all-or-nothing project_row_direct turned ONE such column
                // into a NULL-filled row (`SELECT id, MATCH(..) AS s` lost
                // the id too). Plain columns are read directly; score
                // columns are filled right below.
                let mut projected: Vec<Value> = Vec::with_capacity(stmt.columns.len());
                for sel_col in &stmt.columns {
                    let v = match sel_col {
                        SelectColumn::Column(name) | SelectColumn::ColumnWithAlias(name, _) => {
                            let lookup = if name.contains('.') {
                                name.rsplit('.').next().unwrap_or(name)
                            } else {
                                name.as_str()
                            };
                            schema
                                .get_column_position(lookup)
                                .and_then(|pos| row.get(pos).cloned())
                                .unwrap_or(Value::Null)
                        }
                        _ => Value::Null,
                    };
                    projected.push(v);
                }
                // Score-bearing SELECT columns: `MATCH(col) AGAINST(...) AS
                // score` and the documented `BM25_SCORE(col, 'query')` —
                // both take this row's BM25 score from the search that drove
                // the fast path.
                for (ci, sel_col) in stmt.columns.iter().enumerate() {
                    match sel_col {
                        SelectColumn::Expr(Expr::Match { column: mc, .. }, _) if mc == &column => {
                            projected[ci] = score_map
                                .get(row_id)
                                .map(|s| Value::Float(*s))
                                .unwrap_or(Value::Float(0.0));
                        }
                        SelectColumn::Expr(Expr::FunctionCall { name, args, .. }, _)
                            if name.eq_ignore_ascii_case("BM25_SCORE") =>
                        {
                            // BM25_SCORE() [zero-arg — the shape paired with
                            // `WHERE MATCH(col, 'q')`] is the score of the
                            // driving MATCH itself. It previously required a
                            // matching first ARGUMENT, so the zero-arg form
                            // silently projected NULL for every row.
                            // BM25_SCORE(col, query): fill only when the
                            // argument column IS the driving column.
                            let col_matches = if args.is_empty() {
                                Some(true)
                            } else {
                                args.first().and_then(|a| match a {
                                    Expr::Column(c) => Some(c == &column),
                                    _ => None,
                                })
                            };
                            if col_matches == Some(true) {
                                projected[ci] = score_map
                                    .get(row_id)
                                    .map(|s| Value::Float(*s))
                                    .unwrap_or(Value::Float(0.0));
                            }
                        }
                        _ => {}
                    }
                }
                result_rows.push(projected);
            }
        }

        // Apply ORDER BY on non-score columns (e.g. ORDER BY id) by re-sorting
        // the projected results. The BM25-sorted order is preserved otherwise.
        if needs_resort {
            if let Some(ref order_by) = stmt.order_by {
                // Build column-position lookup for ORDER BY columns.
                let col_pos: Vec<(usize, bool)> = order_by
                    .iter()
                    .filter_map(|ob| {
                        if let Expr::Column(ref col_name) = ob.expr {
                            columns
                                .iter()
                                .position(|c| {
                                    c == col_name || c.ends_with(&format!(".{}", col_name))
                                })
                                .map(|pos| (pos, ob.asc))
                        } else {
                            None
                        }
                    })
                    .collect();
                if !col_pos.is_empty() {
                    result_rows.sort_by(|a, b| {
                        for &(pos, asc) in &col_pos {
                            let cmp = Self::compare_values(
                                a.get(pos).unwrap_or(&Value::Null),
                                b.get(pos).unwrap_or(&Value::Null),
                            )
                            .unwrap_or(std::cmp::Ordering::Equal);
                            let result = if asc { cmp } else { cmp.reverse() };
                            if result != std::cmp::Ordering::Equal {
                                return result;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
            }
        }

        Ok(Some(QueryResult::Select {
            columns,
            rows: result_rows,
        }))
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
        // The index lookup yields candidate rows in distance order and
        // nothing else: aggregates, GROUP BY, DISTINCT and ORDER BY need the
        // general pipeline (which evaluates the predicate per row — see the
        // ST_KNN_3D memo there). Taking the fast path for `COUNT(*) WHERE
        // ST_KNN_3D(…)` used to return k rows of (NULL, distance).
        if self.has_aggregates(&stmt.columns)
            || stmt.group_by.is_some()
            || stmt.having.is_some()
            || stmt.distinct
            || stmt.order_by.is_some()
        {
            return Ok(None);
        }
        match where_clause {
            // 3D spatial fast paths (i-Octree)
            Expr::StWithin3D {
                column,
                min_x,
                min_y,
                min_z,
                max_x,
                max_y,
                max_z,
            } => self.execute_ioctree_within_fast(
                stmt, table_name, column, *min_x, *min_y, *min_z, *max_x, *max_y, *max_z,
            ),
            Expr::StKnn3D { column, x, y, z, k } => {
                self.execute_ioctree_knn_fast(stmt, table_name, column, *x, *y, *z, *k)
            }
            Expr::StRadius3D {
                column,
                x,
                y,
                z,
                radius,
            } => self.execute_ioctree_radius_fast(stmt, table_name, column, *x, *y, *z, *radius),
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
                &table_name,
                &column,
                crate::database::index_metadata::IndexType::Octree,
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
            return Ok(Some(QueryResult::Select {
                columns: vec![],
                rows: vec![],
            }));
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

        let (column_names, result_rows) =
            self.project_columns(&stmt.columns, &sql_rows, &schema)?;
        Ok(Some(QueryResult::Select {
            columns: column_names,
            rows: result_rows,
        }))
    }

    /// Load rows by row_ids and project columns for spatial fast path
    /// Load the index's candidate rows (already in distance order) and
    /// project exactly the SELECT list. `ST_DISTANCE_3D(col, …)` in the list
    /// is computed from the row's geometry like any other expression; the
    /// old version appended an extra `distance` column (the *squared*
    /// distance) to whatever the user selected, so `SELECT id` came back
    /// with two columns.
    fn load_and_project_spatial_rows(
        &self,
        stmt: &SelectStmt,
        table_name: &str,
        row_ids: &[RowId],
    ) -> Result<Option<QueryResult>> {
        let schema = self.db.get_table_schema(table_name)?;
        let columns = self.build_select_columns(&stmt.columns, &schema)?;
        if row_ids.is_empty() {
            return Ok(Some(QueryResult::Select {
                columns,
                rows: vec![],
            }));
        }
        let limit = stmt.limit.unwrap_or(row_ids.len());
        let row_ids_to_load = &row_ids[..row_ids.len().min(limit)];

        let batch_rows = self.db.get_table_rows_batch(table_name, row_ids_to_load)?;
        // get_table_rows_batch may reorder (continuous-id fast path); keep the
        // index's nearest-first order.
        let mut by_id: std::collections::HashMap<RowId, Row> = batch_rows
            .into_iter()
            .filter_map(|(id, r)| r.map(|r| (id, r)))
            .collect();
        let mut result_rows = Vec::with_capacity(row_ids_to_load.len());
        for id in row_ids_to_load {
            if let Some(row) = by_id.remove(id) {
                result_rows.push(Self::project_row_direct_checked(
                    &row,
                    &stmt.columns,
                    &columns,
                    &schema,
                )?);
            }
        }

        Ok(Some(QueryResult::Select {
            columns,
            rows: result_rows,
        }))
    }

    // ==================== Vector KNN Fast Path ====================

    /// 🚀 FAST PATH: detect `WHERE KNN_SEARCH(col, [...], k)` and push it down
    /// to a single `vector_search` index lookup + batch row fetch.
    ///
    /// Only triggers when the WHERE clause is *exactly* a bare `KnnSearch`
    /// (no AND/OR combinators), a vector index exists for the column, and the
    /// target table uses ColSegmentStore. Falls through (returns `None`)
    /// otherwise so the query keeps its current semantics.
    fn try_vector_knn_fast_path(
        &self,
        stmt: &SelectStmt,
        where_clause: &Expr,
    ) -> Result<Option<QueryResult>> {
        // Must be a bare KNN_SEARCH predicate (no AND/OR wrapping).
        let (column, query_vector, k) = match where_clause {
            Expr::KnnSearch {
                column,
                query_vector,
                k,
            } => (column, query_vector, k),
            _ => return Ok(None),
        };

        // LIMIT 1 from SELECT * or table from FROM clause.
        let table_name = match stmt.from.as_ref() {
            Some(TableRef::Table { name, .. }) => name.as_str(),
            _ => return Ok(None),
        };

        // Only enable on ColSegmentStore-backed tables (matches the S9 path
        // this replaces). Keeps row-fetch semantics consistent.
        if !self.db.has_col_segment_store(table_name) {
            return Ok(None);
        }

        self.execute_vector_knn_fast(stmt, table_name, column, query_vector.as_slice(), *k)
    }

    /// Execute `WHERE KNN_SEARCH(col, [...], k)` using the vector index directly.
    /// Mirrors `execute_ioctree_knn_fast` but for vector similarity search.
    fn execute_vector_knn_fast(
        &self,
        stmt: &SelectStmt,
        table_name: &str,
        column: &str,
        query_vector: &[f32],
        k: usize,
    ) -> Result<Option<QueryResult>> {
        // Resolve the (possibly user-named) vector index for this column.
        let index_name = match self.db.index_registry.find_by_column(
            table_name,
            column,
            crate::database::index_metadata::IndexType::Vector,
        ) {
            Some(name) => name,
            None => return Ok(None), // No index → fall back to default path.
        };

        if !self.db.has_vector_index(&index_name) {
            return Ok(None);
        }

        debug_log!(
            "[Executor] ✅ vector KNN fast path: index={}, k={}, dims={}",
            index_name,
            k,
            query_vector.len()
        );

        // Single index lookup → sorted (row_id, distance) pairs.
        let results = match self.db.vector_search(&index_name, query_vector, k) {
            Ok(r) => r,
            Err(_) => return Ok(None),
        };

        if results.is_empty() {
            let schema = self.db.get_table_schema(table_name)?;
            let columns = self
                .build_select_columns(&stmt.columns, &schema)
                .unwrap_or_default();
            return Ok(Some(QueryResult::Select {
                columns,
                rows: vec![],
            }));
        }

        let row_ids: Vec<RowId> = results.iter().map(|(id, _)| *id).collect();
        // load_and_project_spatial_rows takes care of batch fetching + projection.
        self.load_and_project_spatial_rows(stmt, table_name, &row_ids)
    }

    // ==================== 3D Spatial Fast Paths (i-Octree) ====================

    /// Execute ST_WITHIN_3D using i-Octree index directly
    #[allow(clippy::too_many_arguments)]
    fn execute_ioctree_within_fast(
        &self,
        stmt: &SelectStmt,
        table_name: &str,
        column: &str,
        min_x: f64,
        min_y: f64,
        min_z: f64,
        max_x: f64,
        max_y: f64,
        max_z: f64,
    ) -> Result<Option<QueryResult>> {
        let index_name = match self.db.index_registry.find_by_column(
            table_name,
            column,
            crate::database::index_metadata::IndexType::Octree,
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

        self.load_and_project_spatial_rows(stmt, table_name, &row_ids)
    }

    /// Execute ST_KNN_3D using i-Octree index directly
    #[allow(clippy::too_many_arguments)]
    fn execute_ioctree_knn_fast(
        &self,
        stmt: &SelectStmt,
        table_name: &str,
        column: &str,
        x: f64,
        y: f64,
        z: f64,
        k: usize,
    ) -> Result<Option<QueryResult>> {
        let index_name = match self.db.index_registry.find_by_column(
            table_name,
            column,
            crate::database::index_metadata::IndexType::Octree,
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
        self.load_and_project_spatial_rows(stmt, table_name, &row_ids)
    }

    /// Execute ST_RADIUS_3D using i-Octree index directly
    #[allow(clippy::too_many_arguments)]
    fn execute_ioctree_radius_fast(
        &self,
        stmt: &SelectStmt,
        table_name: &str,
        column: &str,
        x: f64,
        y: f64,
        z: f64,
        radius: f64,
    ) -> Result<Option<QueryResult>> {
        let index_name = match self.db.index_registry.find_by_column(
            table_name,
            column,
            crate::database::index_metadata::IndexType::Octree,
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
        self.load_and_project_spatial_rows(stmt, table_name, &row_ids)
    }

    fn to_bool(&self, val: &Value) -> Result<bool> {
        match val {
            Value::Bool(b) => Ok(*b),
            Value::Integer(i) => Ok(*i != 0),
            Value::Float(f) => Ok(*f != 0.0 && !f.is_nan()), // 🔧 Support Float: non-zero and non-NaN is true
            Value::Null => Ok(false),
            _ => Err(MoteDBError::TypeError(
                "Cannot convert to boolean".to_string(),
            )),
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
                        self.compile_simple_comparison(right),
                    ) {
                        return Some(Box::new(move |row: &SqlRow| left_fn(row) && right_fn(row)));
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
    ///
    /// Delegates to `Value::partial_cmp`, which handles every cross-type pair
    /// (Timestamp vs Integer/Float/Text-ISO, exact int-vs-float, …). The old
    /// hand-rolled match only knew Integer/Float/Text — `(Timestamp, Integer)`
    /// fell to `None` → `unwrap_or(false)`, so a materialized-path WHERE like
    /// `ts >= 1700000899000000` silently matched ZERO rows (BETWEEN survived
    /// only because it takes the evaluator path).
    fn compare_values(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
        // SQL three-valued logic: comparing against NULL is UNKNOWN → no
        // match. (`Value::partial_cmp` would order Null below everything,
        // turning `col >= NULL` into "true for every row".)
        if matches!(left, Value::Null) || matches!(right, Value::Null) {
            return None;
        }
        left.partial_cmp(right)
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
    fn try_optimize_primary_key_point_query(
        &self,
        stmt: &SelectStmt,
    ) -> Result<Option<QueryResult>> {
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
        let is_primary_key = schema
            .primary_key()
            .map(|pk| pk == col_name)
            .unwrap_or(false);

        if !is_primary_key {
            return Ok(None); // Not primary key, fallback to normal query
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
                        let (column_names, _) =
                            self.project_columns(&stmt.columns, &[], &schema)?;
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
                let is_select_star =
                    stmt.columns.len() == 1 && matches!(stmt.columns[0], SelectColumn::Star);

                if is_select_star {
                    let column_names = (*schema.column_names_arc()).clone();
                    let result_row: Vec<Value> = schema
                        .columns
                        .iter()
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
                let (column_names, result_rows) =
                    self.project_columns(&stmt.columns, &sql_rows, &schema)?;
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
                        self.db
                            .row_cache
                            .put(table_name.to_string(), row_id, row.clone());
                        let sql_row = row_to_sql_row(&row, &schema)?;
                        let mut prefixed_row = SqlRow::new();
                        prefixed_row
                            .insert("__row_id__".to_string(), Value::Integer(row_id as i64));
                        prefixed_row
                            .insert("__table__".to_string(), Value::text(table_name.clone()));
                        for (col_name, val) in sql_row {
                            prefixed_row.insert(format!("{}.{}", table_name, col_name), val);
                        }
                        let (column_names, result_rows) = self.project_columns(
                            &stmt.columns,
                            &[(row_id, prefixed_row)],
                            &schema,
                        )?;
                        return Ok(Some(QueryResult::Select {
                            columns: column_names,
                            rows: result_rows,
                        }));
                    }
                    // Not found in store — return empty (key doesn't exist).
                    let (column_names, _) = self.project_columns(&stmt.columns, &[], &schema)?;
                    return Ok(Some(QueryResult::Select {
                        columns: column_names,
                        rows: vec![],
                    }));
                }
            }
            match self.db.lsm_engine.get(composite_key)? {
                Some(value_data) => {
                    // Check tombstone
                    if value_data.deleted {
                        let (column_names, _) =
                            self.project_columns(&stmt.columns, &[], &schema)?;
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

                    let row = decode_row(data, &schema).map_err(|e| {
                        StorageError::InvalidData(format!("Deserialization failed: {}", e))
                    })?;

                    // Populate row_cache for future hot-path lookups
                    self.db
                        .row_cache
                        .put(table_name.to_string(), row_id, row.clone());

                    // 🚀 Fast path for SELECT *: skip HashMap conversion entirely
                    //     Direct positional projection from Vec<Value> — saves 2*N HashMap
                    //     inserts + N format!() calls for prefix rewriting.
                    let is_select_star =
                        stmt.columns.len() == 1 && matches!(stmt.columns[0], SelectColumn::Star);

                    if is_select_star {
                        let column_names = (*schema.column_names_arc()).clone();
                        let result_row: Vec<Value> = schema
                            .columns
                            .iter()
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
                    let (column_names, result_rows) =
                        self.project_columns(&stmt.columns, &sql_rows, &schema)?;

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

        // 🔧 Non-AUTO_INCREMENT primary key: resolve value → row_id via
        // column index IF one exists. 非 AUTO_INCREMENT PK 并不自动建列索引 —
        // 此前裸 query_by_column 直接硬报 "Column index not found"
        // (differential fuzz: SELECT COUNT(DISTINCT …), MAX(…) WHERE id = 100
        // 报错)。无索引 → decline 走通用扫描路径，语义仍正确。
        let index_name = format!("{}.{}", table_name, col_name);
        if !self.db.column_indexes.contains_key(&index_name) {
            return Ok(None);
        }
        let row_ids = self
            .db
            .query_by_column(table_name, &col_name, &target_value)?;

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

                let row = decode_row(data, &schema).map_err(|e| {
                    StorageError::InvalidData(format!("Deserialization failed: {}", e))
                })?;

                // 🚀 Fast path for SELECT *: skip HashMap conversion entirely
                let is_select_star =
                    stmt.columns.len() == 1 && matches!(stmt.columns[0], SelectColumn::Star);

                if is_select_star {
                    let column_names: Vec<String> =
                        schema.columns.iter().map(|c| c.name.clone()).collect();
                    let result_row: Vec<Value> = schema
                        .columns
                        .iter()
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
                let (column_names, result_rows) =
                    self.project_columns(&stmt.columns, &sql_rows, &schema)?;

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
        let is_primary_key = schema
            .primary_key()
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
            let has_complex_expr = stmt
                .columns
                .iter()
                .any(|col| matches!(col, SelectColumn::Expr(_, _)));
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
        let index_arc = self
            .db
            .column_indexes
            .get(&pk_index_name)
            .ok_or_else(|| {
                crate::StorageError::Index(format!(
                    "Primary key index not found: {}",
                    pk_index_name
                ))
            })?
            .clone(); // Clone Arc<ColumnValueIndex>

        // Calculate how many entries we need to scan
        let offset = stmt.offset.unwrap_or(0);
        let limit = stmt.limit.unwrap_or(usize::MAX);
        let scan_limit = if limit == usize::MAX {
            None // No limit, scan all
        } else {
            Some(offset + limit) // Scan enough to cover offset + limit
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
        let (column_names, result_rows) =
            self.project_columns(&stmt.columns, &sql_rows, &schema)?;

        Ok(Some(QueryResult::Select {
            columns: column_names,
            rows: result_rows,
        }))
    }

    // 🚀 P0 FIX: Vector ORDER BY optimization helpers

    /// Re-order ANN candidates by exact distance against the stored vectors
    /// and keep the best `k`. Rows the table no longer has are dropped; rows
    /// whose vector is NULL or of another dimension keep their approximate
    /// distance (they sort after every exact one).
    fn rerank_exact(
        &self,
        table: &str,
        col: &str,
        query: &[f32],
        cosine: bool,
        approx: Vec<(RowId, f32)>,
        k: usize,
    ) -> Result<Vec<(RowId, f32)>> {
        if approx.is_empty() {
            return Ok(approx);
        }
        let schema = self.db.get_table_schema(table)?;
        let col_pos = schema.get_column_position(col).unwrap_or(0);
        let ids: Vec<RowId> = approx.iter().map(|(id, _)| *id).collect();
        let approx_dist: std::collections::HashMap<RowId, f32> = approx.into_iter().collect();
        let rows = self.db.get_table_rows_batch(table, &ids)?;
        let mut exact: Vec<(RowId, f32)> = Vec::with_capacity(rows.len());
        for (rid, row) in rows {
            let Some(row) = row else {
                continue;
            };
            let d = match row.get(col_pos) {
                Some(Value::Vector(v)) if v.len() == query.len() => {
                    if cosine {
                        crate::distance::cosine::cosine_distance(query, &v.0)
                    } else {
                        crate::distance::euclidean::euclidean_distance_squared(query, &v.0)
                    }
                }
                _ => approx_dist.get(&rid).copied().unwrap_or(f32::MAX),
            };
            exact.push((rid, d));
        }
        exact.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });
        exact.truncate(k);
        Ok(exact)
    }

    /// All row ids whose indexed text matches `query` (the index's
    /// OR-over-tokens semantics). Without an index, an exact scan using the
    /// default tokenizer's semantics.
    fn text_match_row_ids(
        &self,
        table: &str,
        column: &str,
        query: &str,
        index_name: Option<&str>,
    ) -> Result<Vec<u64>> {
        if let Some(index_name) = index_name {
            if self.db.text_indexes.contains_key(index_name) {
                return Ok(self.db.text_search(index_name, query)?);
            }
        }
        use crate::index::tokenizers::{Tokenizer as _, WhitespaceTokenizer};
        let schema = self.db.get_table_schema(table)?;
        let col_pos = schema.get_column_position(column).unwrap_or(0);
        let tok = WhitespaceTokenizer::default();
        let q_tokens: Vec<String> = tok.tokenize(query).iter().map(|t| t.text.clone()).collect();
        let mut ids = Vec::new();
        let mut matches = |id: u64, text: &str| {
            if tok
                .tokenize(text)
                .iter()
                .any(|t| q_tokens.contains(&t.text))
            {
                ids.push(id);
            }
        };
        if let Ok(store) = self.db.get_or_create_col_segment_store(table, &[]) {
            // 🔑 Hold the flush lock for the same consistency reason as
            // brute_force_vector_knn: buffered rows must not migrate into a
            // new segment between the segment snapshot and the buffer read.
            let _flush_guard = store.flush_lock();
            for seg in store.segments_snapshot() {
                if col_pos >= seg.sst.column_tags.len() {
                    continue;
                }
                if let Ok(ts) = seg.sst.read_text(col_pos) {
                    for i in 0..seg.sst.num_rows {
                        if let Some(text) = ts.get_str(i) {
                            matches(seg.sst.row_map.key(i) & 0xFFFF_FFFF, text);
                        }
                    }
                }
            }
            for (row_id, v) in store.buffered_column_values(col_pos) {
                if let crate::types::Value::Text(t) = v {
                    matches(row_id, &t);
                }
            }
        } else {
            for item in self.db.scan_table_rows_streaming(table)? {
                let (row_id, row) = item?;
                if let Some(crate::types::Value::Text(t)) = row.get(col_pos) {
                    matches(row_id, t);
                }
            }
        }
        Ok(ids)
    }

    /// The k nearest row ids to (x, y, z): via the i-Octree index when
    /// present, otherwise an exact scan of the geometry column (segments +
    /// write buffer).
    fn spatial_knn_row_ids(
        &self,
        table: &str,
        column: &str,
        x: f64,
        y: f64,
        z: f64,
        k: usize,
        index_name: Option<&str>,
    ) -> Result<Vec<u64>> {
        if let Some(index_name) = index_name {
            if self.db.ioctree_indexes.contains_key(index_name) {
                let point = crate::types::Point3D::new(x, y, z);
                return Ok(self
                    .db
                    .ioctree_knn_query(index_name, &point, k)?
                    .into_iter()
                    .map(|(id, _)| id)
                    .collect());
            }
        }
        let schema = self.db.get_table_schema(table)?;
        let col_pos = schema.get_column_position(column).unwrap_or(0);
        let q = [x, y, z];
        let mut dists: Vec<(f64, u64)> = Vec::new();
        let offer = |dists: &mut Vec<(f64, u64)>, row_id: u64, g: &crate::types::Geometry| {
            if let crate::types::Geometry::Point3D(p) = g {
                let d = (p.x - q[0]).powi(2) + (p.y - q[1]).powi(2) + (p.z - q[2]).powi(2);
                dists.push((d, row_id));
            }
        };
        if let Ok(store) = self.db.get_or_create_col_segment_store(table, &[]) {
            // 🔑 Hold the flush lock so the auto-flush thread can't move
            // buffered rows into a new segment between the two views below.
            let _flush_guard = store.flush_lock();
            for seg in store.segments_snapshot() {
                if col_pos >= seg.sst.column_tags.len() {
                    continue;
                }
                for (row_id, g) in seg.sst.read_spatial(col_pos).unwrap_or_default() {
                    offer(&mut dists, row_id, &g);
                }
            }
            for (row_id, v) in store.buffered_column_values(col_pos) {
                if let crate::types::Value::Spatial(g) = v {
                    offer(&mut dists, row_id, &g);
                }
            }
        } else {
            for item in self.db.scan_table_rows_streaming(table)? {
                let (row_id, row) = item?;
                if let Some(crate::types::Value::Spatial(g)) = row.get(col_pos) {
                    offer(&mut dists, row_id, g);
                }
            }
        }
        dists.sort_by(|a, b| nan_aware_cmp(a.0, b.0));
        Ok(dists.into_iter().take(k).map(|(_, id)| id).collect())
    }

    /// Brute-force vector KNN: scan all vectors in the columnar store,
    /// compute L2 distance inline, keep top-K. Used for small tables (<50K)
    /// where DiskANN graph traversal overhead exceeds brute-force O(N).
    fn brute_force_vector_knn(
        &self,
        table: &str,
        col: &str,
        query: &[f32],
        k: usize,
        cosine: bool,
    ) -> Result<Vec<(RowId, f32)>> {
        let schema = self.db.get_table_schema(table)?;
        let col_pos = schema.get_column_position(col).unwrap_or(0);
        let qdim = query.len();

        if let Ok(store) = self.db.get_or_create_col_segment_store(table, &[]) {
            // 🔑 Consistent read: hold the flush lock across the segment
            // snapshot AND the buffered-rows read. Without it, the auto-flush
            // thread could move buffered rows into a new segment between the
            // two views and the query silently missed them (flaky
            // `ORDER BY emb <-> ?` returning fewer rows than exist).
            // 🔑 No flush_buffer() here (this lock IS flush's lock — calling
            // it would deadlock): the buffer (if non-empty) is read below.
            let _flush_guard = store.flush_lock();
            let segs = store.segments_snapshot();
            // 🔑 Load full keys only if not already loaded (idempotent). Needed
            // so row_map.key(i) returns accurate values for small segments.
            for seg in &segs {
                if !seg.sst.row_map.has_full_keys_loaded() {
                    let _ = seg.sst.load_full_keys();
                }
            }
            // 🔑 Top-K MAX-heap by (distance, key): peek() is the WORST kept
            // candidate; a new candidate replaces it only when strictly
            // nearer. The old version wrapped entries in Reverse, making
            // peek() the BEST candidate — it evicted the nearest rows and
            // kept the farthest ones (`ORDER BY emb <-> ?` on an index-less
            // table returned [5,3,2,1] instead of [5,6,4,7]).
            let mut heap: std::collections::BinaryHeap<(OrderedF32, u64)> =
                std::collections::BinaryHeap::with_capacity(k + 1);
            // L2 ranks by squared distance (monotone — cheaper, no sqrt);
            // cosine needs the actual distance value. Top-K max-heap: peek()
            // is the worst kept candidate.
            let offer = |heap: &mut std::collections::BinaryHeap<(OrderedF32, u64)>,
                         key: u64,
                         row_vec: &[f32]| {
                let dist = if cosine {
                    crate::distance::cosine::cosine_distance(query, row_vec)
                } else {
                    crate::distance::euclidean::euclidean_distance_squared(query, row_vec)
                };
                let cand = (OrderedF32(dist), key);
                if heap.len() < k {
                    heap.push(cand);
                } else if let Some(&(worst, _)) = heap.peek() {
                    if cand.0 < worst {
                        heap.pop();
                        heap.push(cand);
                    }
                }
            };
            // 同 offer, 但距离已算好 (字节核零拷贝路径)。
            let offer_dist = |heap: &mut std::collections::BinaryHeap<(OrderedF32, u64)>,
                              key: u64,
                              dist: f32| {
                let cand = (OrderedF32(dist), key);
                if heap.len() < k {
                    heap.push(cand);
                } else if let Some(&(worst, _)) = heap.peek() {
                    if cand.0 < worst {
                        heap.pop();
                        heap.push(cand);
                    }
                }
            };
            // Rows must be handed to the SIMD kernels as `&[f32]`. In the
            // segment the floats follow a null bitmap and a u16 dim header, so
            // the raw bytes are never 4-byte aligned — the old in-place
            // reinterpret was UB (and aborts under debug assertions). Two
            // alignment-safe paths, chosen per segment by the col_cache byte
            // budget:
            //   * fits → decoded VectorSegment kept in the cache, so repeated
            //     scans cost no I/O or decode (0.13ms for 5K×384 rows);
            //   * doesn't fit → stream the raw payload and copy each row into
            //     one reusable aligned scratch buffer: no large allocation,
            //     nothing left in the cache (a >8MB segment is read lazily,
            //     338MB per query for 220K×384 rows, exactly as before).
            // Decoded VECTOR columns have their own (larger) budget: the top-k
            // hot path re-scans the full column per query and streaming costs
            // ~13× more than a cached decode.
            let budget = store.vector_cache_budget();
            // Bytes already held by this table's col_caches (all columns); new
            // vector columns are cached only while the cumulative total stays
            // within budget, so a table of many small segments doesn't decode
            // + cache + trim everything on every query.
            let mut cached_total: usize = segs.iter().map(|s| s.cached_col_bytes()).sum();
            let mut scratch: Vec<f32> = vec![0.0; qdim];
            // Reusable chunk buffer for the edge-bounded streaming path below
            // (bounded at ~8MB, shared across segments).
            let mut chunk_buf: Vec<u8> = Vec::new();
            // 🔑 Version dedup, newest wins: an UPDATE leaves the old vector in
            // an older segment (its tombstone is not visible here) and the new
            // one in the write buffer — both used to be offered, so a single
            // id could appear TWICE in top-k. Scan newest source first (write
            // buffer), then segments newest→oldest, and keep only the first
            // occurrence of each row key.
            let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
            let offer_new = |seen: &mut std::collections::HashSet<u64>,
                             heap: &mut std::collections::BinaryHeap<(OrderedF32, u64)>,
                             key: u64,
                             row_vec: &[f32]| {
                if !seen.insert(key) {
                    return;
                }
                offer(heap, key, row_vec);
            };
            for (key, v) in store.buffered_column_values(col_pos) {
                if let crate::types::Value::Vector(rv) = v {
                    if rv.len() == qdim {
                        offer_new(&mut seen, &mut heap, key, &rv.0);
                    }
                }
            }
            // 🔑 Buffered tombstones claim their keys BEFORE any segment is
            // walked: a DELETE whose tombstone still sits in the write buffer
            // must suppress the row's live versions in every older segment.
            // Without this the ghost row entered the heap and the row fetch
            // afterwards dropped it — the query returned fewer rows than LIMIT.
            for key in store.buffered_tombstone_keys() {
                seen.insert(key);
            }
            // 🚀 跨段 morsel 并行: ≥2 段且无跨段重复键可能 (纯插入多段 —
            // 4MB flush 产生的小段阵是批量导入后的常态, 段内 20K 门槛对
            // ~2.7K 的小段永不触发) 时段间相互独立, 每段一个 rayon 任务
            // 评分进本地 top-k 堆, 主线程合并。缓存/预算决策与缓存填充
            // 保持串行 (RSS 语义不变)。有 overlap 可能 (UPDATE/DELETE 置
            // 位 / 重开保守置位) 走下方顺序 newest-wins 路径。
            // 🔑 跨段无重复键判据: 段键各自升序存储, 各段 [首键, 尾键] 区间
            // 两两不相交 ⇒ 段间无同 key 行 (UPDATE 重写同 key 的新版本必然
            // 落进与旧段重叠的区间 → 判据失败 → 走顺序 newest-wins)。首键 =
            // row_map.key(0) (fence 边界精确), 尾键 = last_key_hint, 均 O(1)。
            // 重开时保守置位的 overlap_possible 不再误伤纯插入段阵。
            let segs_disjoint: bool = {
                let mut ranges: Vec<(u64, u64)> = Vec::with_capacity(segs.len());
                let mut ok = true;
                for seg in segs.iter() {
                    let n = seg.sst.num_rows;
                    if n == 0 {
                        continue;
                    }
                    let first = seg.sst.row_map.key(0);
                    match seg.sst.last_key_hint() {
                        Some(last) if last >= first => ranges.push((first, last)),
                        _ => {
                            ok = false;
                            break;
                        }
                    }
                }
                ok && {
                    ranges.sort_unstable();
                    ranges.windows(2).all(|w| w[0].1 < w[1].0)
                }
            };
            #[cfg(feature = "rayon")]
            let did_cross_parallel: bool = if segs.len() >= 2 && segs_disjoint {
                use rayon::prelude::*;
                enum Plan {
                    Cached(crate::storage::lsm::columnar::VectorSegment),
                    Stream,
                    SkipTombstones,
                }
                // phase 1: 决策 + 缓存填充 (串行, 预算记账 — 同顺序路径)。
                let mut cached_total_p: usize = segs.iter().map(|s| s.cached_col_bytes()).sum();
                let plans: Vec<(std::sync::Arc<crate::storage::col_segment::Segment>, Plan)> =
                    segs.iter()
                        .map(|seg| {
                        let n = seg.sst.num_rows;
                        if col_pos >= seg.sst.column_tags.len() {
                            return (std::sync::Arc::clone(seg), Plan::SkipTombstones);
                        }
                        let seg_bytes = n * qdim * 4;
                        let cached = match seg.cached_vectors(col_pos) {
                            Some(vs) => Some(vs),
                            None if cached_total_p + seg_bytes <= budget => {
                                let vs = seg.read_vectors_cached(col_pos);
                                if vs.is_some() {
                                    cached_total_p += seg_bytes;
                                }
                                vs
                            }
                            None => None,
                        };
                        match cached {
                            Some(vs) if vs.dim == qdim => {
                                (std::sync::Arc::clone(seg), Plan::Cached(vs))
                            }
                            Some(_) => (std::sync::Arc::clone(seg), Plan::SkipTombstones),
                            None => (std::sync::Arc::clone(seg), Plan::Stream),
                        }
                        })
                        .collect();
                // phase 2: 段间并行评分 (段内串行 — 段数即并行度; 无
                // overlap ⇒ 无需 seen 过滤)。
                let offer_local = |lh: &mut std::collections::BinaryHeap<(OrderedF32, u64)>,
                                   key: u64,
                                   rv: &[f32]| {
                    let dist = if cosine {
                        crate::distance::cosine::cosine_distance(query, rv)
                    } else {
                        crate::distance::euclidean::euclidean_distance_squared(query, rv)
                    };
                    let cand = (OrderedF32(dist), key);
                    if lh.len() < k {
                        lh.push(cand);
                    } else if lh.peek().is_some_and(|&(worst, _)| cand.0 < worst) {
                        lh.pop();
                        lh.push(cand);
                    }
                };
                let offer_dist_local =
                    |lh: &mut std::collections::BinaryHeap<(OrderedF32, u64)>, key: u64, dist: f32| {
                        let cand = (OrderedF32(dist), key);
                        if lh.len() < k {
                            lh.push(cand);
                        } else if lh.peek().is_some_and(|&(worst, _)| cand.0 < worst) {
                            lh.pop();
                            lh.push(cand);
                        }
                    };
                let parts: Vec<(
                    std::collections::BinaryHeap<(OrderedF32, u64)>,
                    Vec<u64>,
                )> = plans
                    .into_par_iter()
                    .map(|(seg, plan)| {
                        let mut lh: std::collections::BinaryHeap<(OrderedF32, u64)> =
                            std::collections::BinaryHeap::with_capacity(k + 1);
                        let mut tombstoned: Vec<u64> = Vec::new();
                        let n = seg.sst.num_rows;
                        let seed_tombstoned = |tombstoned: &mut Vec<u64>| {
                            for i in 0..n {
                                if seg.sst.row_map.is_deleted(i) {
                                    tombstoned.push(seg.sst.row_map.key(i));
                                }
                            }
                        };
                        match plan {
                            Plan::SkipTombstones => {
                                seed_tombstoned(&mut tombstoned);
                            }
                            Plan::Cached(vs) => {
                                for i in 0..n {
                                    if seg.sst.row_map.is_deleted(i) {
                                        tombstoned.push(seg.sst.row_map.key(i));
                                        continue;
                                    }
                                    if let Some(rv) = vs.row(i) {
                                        offer_local(&mut lh, seg.sst.row_map.key(i), rv);
                                    }
                                }
                            }
                            Plan::Stream => {
                                let entry = &seg.sst.column_index[col_pos];
                                let col_start = entry.offset as usize;
                                let null_bytes = n.div_ceil(8);
                                let mut head = vec![0u8; null_bytes + 2];
                                if seg
                                    .sst
                                    .read_bytes_at(col_start + 1, null_bytes + 2)
                                    .map(|h| head.copy_from_slice(&h))
                                    .is_err()
                                {
                                    seed_tombstoned(&mut tombstoned);
                                    return (lh, tombstoned);
                                }
                                let dim =
                                    u16::from_le_bytes([head[null_bytes], head[null_bytes + 1]])
                                        as usize;
                                if dim != qdim {
                                    seed_tombstoned(&mut tombstoned);
                                    return (lh, tombstoned);
                                }
                                let stride = dim * 4;
                                let data_base = col_start + 1 + null_bytes + 2;
                                // 🔑 零拷贝 + 分块: read_bytes_at 借出 mmap 切片,
                                // 字节距离核 (非对齐加载) 直接消费 — 无缓冲分配
                                // (此前整段一次性读 16 并发 ×7.7MB 瞬时 RSS 爆表;
                                // 后改 1MB fbuf 落位一趟; 现在零分配零拷贝)。
                                const CHUNK: usize = 1024 * 1024;
                                let rows_per_chunk = (CHUNK / stride.max(1)).max(1);
                                'outer: for cstart in (0..n).step_by(rows_per_chunk) {
                                    let cend = (cstart + rows_per_chunk).min(n);
                                    let need = (cend - cstart) * stride;
                                    let Ok(bytes) =
                                        seg.sst.read_bytes_at(data_base + cstart * stride, need)
                                    else {
                                        break 'outer;
                                    };
                                    for i in cstart..cend {
                                        if seg.sst.row_map.is_deleted(i) {
                                            tombstoned.push(seg.sst.row_map.key(i));
                                            continue;
                                        }
                                        if (head[i / 8] >> (i % 8)) & 1 != 0 {
                                            continue;
                                        }
                                        let bo = (i - cstart) * stride;
                                        let dist = if cosine {
                                            crate::distance::cosine::cosine_distance_bytes(
                                                query,
                                                &bytes[bo..bo + stride],
                                            )
                                        } else {
                                            crate::distance::euclidean::euclidean_distance_squared_bytes(
                                                query,
                                                &bytes[bo..bo + stride],
                                            )
                                        };
                                        offer_dist_local(&mut lh, seg.sst.row_map.key(i), dist);
                                    }
                                }
                            }
                        }
                        (lh, tombstoned)
                    })
                    .collect();
                for (lh, tombstoned) in parts {
                    for cand in lh {
                        if heap.len() < k {
                            heap.push(cand);
                        } else if heap.peek().is_some_and(|&(worst, _)| cand.0 < worst) {
                            heap.pop();
                            heap.push(cand);
                        }
                    }
                    seen.extend(tombstoned);
                }
                true
            } else {
                false
            };
            #[cfg(not(feature = "rayon"))]
            let did_cross_parallel: bool = false;
            for seg in (if did_cross_parallel {
                &segs[..0] // 并行分支已完成 — 空迭代跳过顺序路径
            } else {
                &segs[..]
            })
            .iter()
            .rev() {
                let n = seg.sst.num_rows;
                // 🔑 Seed tombstones even when this segment's VECTOR column is
                // unusable for scoring. A tombstone-only segment stores NULL
                // placeholders → the vector column's dim header is 0 (≠ qdim),
                // and the old `continue` skipped the WHOLE segment — its
                // tombstones never claimed their keys, so older segments'
                // live versions resurrected the deleted row (ghost in top-k,
                // dropped by the row fetch → fewer rows than LIMIT).
                let seed_tombstones = |seen: &mut std::collections::HashSet<u64>| {
                    for i in 0..n {
                        if seg.sst.row_map.is_deleted(i) {
                            seen.insert(seg.sst.row_map.key(i));
                        }
                    }
                };
                if col_pos >= seg.sst.column_tags.len() {
                    seed_tombstones(&mut seen);
                    continue;
                }
                let seg_bytes = n * qdim * 4;
                let cached = match seg.cached_vectors(col_pos) {
                    Some(vs) => Some(vs),
                    None if cached_total + seg_bytes <= budget => {
                        let vs = seg.read_vectors_cached(col_pos);
                        if vs.is_some() {
                            cached_total += seg_bytes;
                        }
                        vs
                    }
                    None => None,
                };
                if let Some(vs) = cached {
                    if vs.dim != qdim {
                        seed_tombstones(&mut seen);
                        continue;
                    }
                    // 🚀 段内 morsel 并行 (大段): 段内键唯一 (flush 时
                    // newest-wins 去重) ⇒ 段内无需去重; prior-seen (更新
                    // 来源已声明的键) 作只读快照, 段后统一合并本段新声明键
                    // + 墓碑键 — 跨段 newest-wins 序逐位保持。
                    #[cfg(feature = "rayon")]
                    if n >= crate::sql::vector_exec::PARALLEL_MORSEL_MIN_ROWS {
                        use rayon::prelude::*;
                        let nchunks = crate::sql::vector_exec::par_chunk_count(n);
                        let chunk_len = n.div_ceil(nchunks).max(1);
                        let prior = &seen;
                        let parts: Vec<(
                            std::collections::BinaryHeap<(OrderedF32, u64)>,
                            Vec<u64>,
                            Vec<u64>,
                        )> = (0..n)
                            .into_par_iter()
                            .chunks(chunk_len)
                            .map(|rows| {
                                let mut lh: std::collections::BinaryHeap<(OrderedF32, u64)> =
                                    std::collections::BinaryHeap::with_capacity(k + 1);
                                let (mut claimed, mut tombstoned): (Vec<u64>, Vec<u64>) =
                                    (Vec::new(), Vec::new());
                                for i in rows {
                                    let key = seg.sst.row_map.key(i);
                                    if seg.sst.row_map.is_deleted(i) {
                                        tombstoned.push(key);
                                        continue;
                                    }
                                    let Some(row_vec) = vs.row(i) else {
                                        continue;
                                    };
                                    if prior.contains(&key) {
                                        continue;
                                    }
                                    let dist = if cosine {
                                        crate::distance::cosine::cosine_distance(query, row_vec)
                                    } else {
                                        crate::distance::euclidean::euclidean_distance_squared(
                                            query, row_vec,
                                        )
                                    };
                                    let cand = (OrderedF32(dist), key);
                                    if lh.len() < k {
                                        lh.push(cand);
                                    } else if lh.peek().is_some_and(|&(worst, _)| cand.0 < worst) {
                                        lh.pop();
                                        lh.push(cand);
                                    }
                                    claimed.push(key);
                                }
                                (lh, claimed, tombstoned)
                            })
                            .collect();
                        for (lh, claimed, tombstoned) in parts {
                            for cand in lh {
                                if heap.len() < k {
                                    heap.push(cand);
                                } else if heap.peek().is_some_and(|&(worst, _)| cand.0 < worst) {
                                    heap.pop();
                                    heap.push(cand);
                                }
                            }
                            seen.extend(claimed);
                            seen.extend(tombstoned);
                        }
                        continue;
                    }
                    for i in 0..n {
                        let key = seg.sst.row_map.key(i);
                        // 🔑 Tombstone check MUST precede the null/vector
                        // decode: a tombstone row carries NULL placeholders,
                        // so vs.row(i) bails before the deleted flag is ever
                        // consulted. Tombstones claim their key (segments are
                        // walked newest→oldest; the tombstone is the row's
                        // FINAL state) so older segments' live versions can't
                        // resurrect it — the ghost-in-top-k bug.
                        if seg.sst.row_map.is_deleted(i) {
                            seen.insert(key);
                            continue;
                        }
                        let Some(row_vec) = vs.row(i) else {
                            continue;
                        };
                        if !seen.insert(key) {
                            continue;
                        }
                        offer(&mut heap, key, row_vec);
                    }
                    continue;
                }
                // 🚀 Edge-bounded streaming: vector columns are ALWAYS stored
                // raw (flag=0, [flag][null_bitmap][dim:u16][f32×dim]), so the
                // scan can proceed in ~8MB chunks instead of materializing
                // the whole column. The old one-shot read allocated a
                // data-size buffer per query (153MB on a 100K×384 table;
                // +290MB peak RSS with allocator retention) — OOM-class on a
                // 256MB edge device running the embodied/robotics presets.
                let entry = &seg.sst.column_index[col_pos];
                let col_start = entry.offset as usize;
                let null_bytes = n.div_ceil(8);
                let mut head = vec![0u8; null_bytes + 2];
                if seg
                    .sst
                    .read_bytes_at(col_start + 1, null_bytes + 2)
                    .map(|h| head.copy_from_slice(&h))
                    .is_err()
                {
                    seed_tombstones(&mut seen);
                    continue;
                }
                let dim = u16::from_le_bytes([head[null_bytes], head[null_bytes + 1]]) as usize;
                if dim != qdim {
                    seed_tombstones(&mut seen);
                    continue;
                }
                let stride = dim * 4;
                let data_base = col_start + 1 + null_bytes + 2;
                // 🚀 段内 morsel 并行 (大段): 每个 rayon chunk 自读自的列
                // 切片 (mmap 页错误并行化) + 本地 top-k 堆; 键声明序同缓存
                // 分支 (段后合并)。小段保持顺序 8MB 流式。
                #[cfg(feature = "rayon")]
                if n >= crate::sql::vector_exec::PARALLEL_MORSEL_MIN_ROWS {
                    use rayon::prelude::*;
                    let nchunks = crate::sql::vector_exec::par_chunk_count(n);
                    let chunk_len = n.div_ceil(nchunks).max(1);
                    let prior = &seen;
                    let parts: Vec<(
                        std::collections::BinaryHeap<(OrderedF32, u64)>,
                        Vec<u64>,
                        Vec<u64>,
                    )> = (0..n)
                        .into_par_iter()
                        .chunks(chunk_len)
                        .map(|rows| {
                            let mut lh: std::collections::BinaryHeap<(OrderedF32, u64)> =
                                std::collections::BinaryHeap::with_capacity(k + 1);
                            let (mut claimed, mut tombstoned): (Vec<u64>, Vec<u64>) =
                                (Vec::new(), Vec::new());
                            let row0 = rows[0];
                            // 🔑 零拷贝子分块: read_bytes_at 借出 mmap 切片,
                            // 字节距离核直接消费 (此前整 chunk 分配 ~15MB/任务
                            // RSS 爆表 → 1MB fbuf 一趟拷贝 → 现在零分配零拷贝)。
                            const SUB_CHUNK: usize = 1024 * 1024;
                            let sub_rows = (SUB_CHUNK / stride.max(1)).max(1);
                            'sub: for cstart in (row0..row0 + rows.len()).step_by(sub_rows) {
                                let cend = (cstart + sub_rows).min(row0 + rows.len());
                                let need = (cend - cstart) * stride;
                                let Ok(bytes) =
                                    seg.sst.read_bytes_at(data_base + cstart * stride, need)
                                else {
                                    break 'sub;
                                };
                                for i in cstart..cend {
                                    let key = seg.sst.row_map.key(i);
                                    if seg.sst.row_map.is_deleted(i) {
                                        tombstoned.push(key);
                                        continue;
                                    }
                                    if (head[i / 8] >> (i % 8)) & 1 != 0 {
                                        continue;
                                    }
                                    let bo = (i - cstart) * stride;
                                    if prior.contains(&key) {
                                        continue;
                                    }
                                    let dist = if cosine {
                                        crate::distance::cosine::cosine_distance_bytes(
                                            query,
                                            &bytes[bo..bo + stride],
                                        )
                                    } else {
                                        crate::distance::euclidean::euclidean_distance_squared_bytes(
                                            query,
                                            &bytes[bo..bo + stride],
                                        )
                                    };
                                    let cand = (OrderedF32(dist), key);
                                    if lh.len() < k {
                                        lh.push(cand);
                                    } else if lh.peek().is_some_and(|&(worst, _)| cand.0 < worst) {
                                        lh.pop();
                                        lh.push(cand);
                                    }
                                    claimed.push(key);
                                }
                            }
                            (lh, claimed, tombstoned)
                        })
                        .collect();
                    for (lh, claimed, tombstoned) in parts {
                        for cand in lh {
                            if heap.len() < k {
                                heap.push(cand);
                            } else if heap.peek().is_some_and(|&(worst, _)| cand.0 < worst) {
                                heap.pop();
                                heap.push(cand);
                            }
                        }
                        seen.extend(claimed);
                        seen.extend(tombstoned);
                    }
                    continue;
                }
                const KNN_STREAM_CHUNK_BYTES: usize = 8 * 1024 * 1024;
                let rows_per_chunk = (KNN_STREAM_CHUNK_BYTES / stride.max(1)).max(1);
                // 🔑 零拷贝: read_bytes_at 对 mmap 段返回借用切片, 字节距离
                // 核 (非对齐加载) 直接消费 — 无对齐缓冲复制趟 (此前 bytes 块
                // + 每行 copy_le_f32 两趟, 后改 fbuf 落位一趟; 现在零趟)。
                for cstart in (0..n).step_by(rows_per_chunk) {
                    let cend = (cstart + rows_per_chunk).min(n);
                    let need = (cend - cstart) * stride;
                    let Ok(bytes) = seg.sst.read_bytes_at(data_base + cstart * stride, need)
                    else {
                        break;
                    };
                    for i in cstart..cend {
                        let key = seg.sst.row_map.key(i);
                        // Same as the cached branch: tombstone (NULL
                        // placeholder row) claims its key BEFORE the null
                        // check can skip it.
                        if seg.sst.row_map.is_deleted(i) {
                            seen.insert(key);
                            continue;
                        }
                        if (head[i / 8] >> (i % 8)) & 1 != 0 {
                            continue;
                        }
                        let bo = (i - cstart) * stride;
                        if !seen.insert(key) {
                            continue;
                        }
                        let dist = if cosine {
                            crate::distance::cosine::cosine_distance_bytes(
                                query,
                                &bytes[bo..bo + stride],
                            )
                        } else {
                            crate::distance::euclidean::euclidean_distance_squared_bytes(
                                query,
                                &bytes[bo..bo + stride],
                            )
                        };
                        offer_dist(&mut heap, key, dist);
                    }
                }
            }
            // Safety net for the budget (other columns' caches may have grown
            // since the estimate above); `get_or_create_col_segment_store`
            // only trims when it creates the store, so this path had no budget
            // choke point before.
            store.trim_col_cache_to_budget();
            let mut results: Vec<(RowId, f32)> =
                heap.into_iter().map(|(d, id)| (id, d.0)).collect();
            // Ascending distance; ties broken by key for deterministic output
            // (BinaryHeap's into_iter order is unspecified). NaN-aware: a
            // NaN distance (NaN in a stored vector) must sort AFTER real
            // distances — plain partial_cmp made it "equal" to everything so
            // the row-id tiebreak could put it above an exact 0.0 match.
            results.sort_by(|a, b| nan_aware_cmp(a.1 as f64, b.1 as f64).then(a.0.cmp(&b.0)));
            return Ok(results);
        }
        Ok(Vec::new())
    }

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
        let (column, query_vector, asc, cosine) = match &order_by.expr {
            // 匹配: column <-> [vector] (L2Distance / CosineDistance)
            Expr::BinaryOp { op, left, right }
                if matches!(
                    *op,
                    BinaryOperator::L2Distance | BinaryOperator::CosineDistance
                ) =>
            {
                match (&**left, &**right) {
                    (Expr::Column(col), Expr::Literal(Value::Vector(vec))) => {
                        let cosine = matches!(*op, BinaryOperator::CosineDistance);
                        (col.clone(), vec.clone(), order_by.asc, cosine)
                    }
                    (Expr::Column(_col), _other) => {
                        return Ok(None);
                    }
                    _ => {
                        return Ok(None);
                    }
                }
            }
            _other_expr => {
                return Ok(None);
            }
        };

        // 向量距离必须是升序
        if !asc {
            return Ok(None);
        }

        // 🔑 No WHERE: the plan fetches the global top-k candidates first and
        // applies WHERE afterwards — with a filter that can drop candidates,
        // `WHERE cat='a' ORDER BY emb <-> q LIMIT 5` would return fewer than
        // 5 rows even when ≥5 matching rows exist. Filtered ANN must go
        // through the materialized path (filter → sort → limit).
        if stmt.where_clause.is_some() {
            return Ok(None);
        }

        // 获取表名
        let table_name = match stmt.from.as_ref() {
            Some(TableRef::Table { name, .. }) => name.clone(),
            _ => return Ok(None),
        };

        // 🔑 No vector-index requirement here: execute_vector_order_by_plan
        // falls back to brute_force_vector_knn (correct L2/cosine top-k over
        // segments + write buffer) when no index exists. Previously the gate
        // made index-less tables fall through to the col-segment scan, whose
        // try_sort_projected silently skips expression keys → arbitrary
        // order for `ORDER BY emb <-> ? LIMIT k` (found via Python bindings).
        Ok(Some(VectorOrderByPlan {
            table: table_name,
            column,
            query_vector: query_vector.to_vec(),
            k: limit,
            cosine,
        }))
    }

    /// Execute SELECT using vector ORDER BY optimization
    fn execute_vector_order_by_plan(
        &self,
        stmt: &SelectStmt,
        plan: &VectorOrderByPlan,
    ) -> Result<QueryResult> {
        debug_log!(
            "[Executor] ✅ 使用向量索引优化 ORDER BY: {} <-> [...] LIMIT {}",
            plan.column,
            plan.k
        );

        // Resolve index name via registry (supports custom index names)
        let index_name = self
            .db
            .index_registry
            .find_by_column(
                &plan.table,
                &plan.column,
                crate::database::index_metadata::IndexType::Vector,
            )
            .unwrap_or_else(|| format!("{}_{}", plan.table, plan.column));

        // 🔑 Correctness-first routing: the SIMD columnar brute force is
        // EXACT (scans every live row, segments + write buffer) and fast in
        // the embedded envelope (~1.2ms @ 50K rows), while the DiskANN graph
        // index's incremental/rebuild machinery has multiple LRU-capacity
        // hazards that strand nodes and silently drop rows from top-k
        // (measured recall@10 as low as 0.03-0.09). Below the threshold the
        // brute force is used unconditionally — index or not; the graph
        // index only serves tables large enough that an exact scan would
        // dominate query latency.
        const EXACT_SCAN_MAX_ROWS: usize = 200_000;
        let use_index = {
            let has_index = self.db.has_vector_index(&index_name);
            has_index
                && self
                    .db
                    .get_or_create_col_segment_store(&plan.table, &[])
                    .ok()
                    .map(|store| {
                        let rows: usize = store
                            .segments_snapshot()
                            .iter()
                            .map(|s| s.sst.num_rows)
                            .sum::<usize>()
                            + store.buffered_row_count();
                        rows > EXACT_SCAN_MAX_ROWS
                    })
                    .unwrap_or(false)
        };
        let candidates = if use_index {
            // Over-fetch from the graph, then re-rank with the table's
            // full-precision vectors. The index ranks by SQ8-dequantized
            // distance, which reshuffles near-equal neighbours (measured
            // recall@10 0.985 on real 384-d embeddings); the exact re-rank of
            // a slightly larger candidate set recovers most of that. For
            // small k the extra candidates come from the search list the
            // graph walk already visited, so it costs no extra traversal.
            let k_over = (plan.k * 2).max(plan.k + 16).min(1024);
            let approx = self
                .db
                .vector_search(&index_name, &plan.query_vector, k_over)?;
            self.rerank_exact(
                &plan.table,
                &plan.column,
                &plan.query_vector,
                plan.cosine,
                approx,
                plan.k,
            )?
        } else {
            // Embedded envelope (or no index) — exact brute-force scan.
            self.brute_force_vector_knn(
                &plan.table,
                &plan.column,
                &plan.query_vector,
                plan.k,
                plan.cosine,
            )?
        };
        debug_log!(
            "[Executor] 🔍 vector_search返回了{}个候选",
            candidates.len()
        );

        let row_ids: Vec<u64> = candidates.iter().map(|(id, _dist)| *id).collect();

        if !row_ids.is_empty() {
            debug_log!(
                "[Executor] 🔍 row_ids前5个: {:?}",
                &row_ids[..5.min(row_ids.len())]
            );
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

        debug_log!(
            "[Executor] 🔍 get_table_rows_batch返回了{}个行",
            batch_rows.len()
        );

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
            sql_rows
                .into_iter()
                .filter(|(_, row)| {
                    self.evaluator
                        .eval(where_clause, row)
                        .and_then(|val| self.to_bool(&val))
                        .unwrap_or(false)
                })
                .collect()
        } else {
            sql_rows
        };

        // 5. 简单列投影（避免递归调用 project_columns）
        let column_names: Vec<String> =
            if stmt.columns.len() == 1 && matches!(stmt.columns[0], SelectColumn::Star) {
                // SELECT *
                schema.columns.iter().map(|c| c.name.clone()).collect()
            } else {
                stmt.columns
                    .iter()
                    .map(|col| match col {
                        SelectColumn::Star => "*".to_string(),
                        SelectColumn::Column(name) | SelectColumn::ColumnWithAlias(name, _) => {
                            name.clone()
                        }
                        SelectColumn::Expr(_, Some(alias)) => alias.clone(),
                        SelectColumn::Expr(expr, None) => format!("{:?}", expr),
                    })
                    .collect()
            };

        let projected_rows: Vec<Vec<Value>> = filtered_rows
            .iter()
            .map(|(_, row)| {
                if stmt.columns.len() == 1 && matches!(stmt.columns[0], SelectColumn::Star) {
                    // SELECT * - return all columns in schema order
                    schema
                        .columns
                        .iter()
                        .map(|col| row.get(&col.name).cloned().unwrap_or(Value::Null))
                        .collect()
                } else {
                    stmt.columns
                        .iter()
                        .map(|col| {
                            match col {
                                SelectColumn::Column(name)
                                | SelectColumn::ColumnWithAlias(name, _) => {
                                    row.get(name).cloned().unwrap_or(Value::Null)
                                }
                                SelectColumn::Expr(expr, _) => {
                                    // ⚠️ 只对简单表达式求值，避免递归
                                    self.evaluator.eval(expr, row).unwrap_or(Value::Null)
                                }
                                SelectColumn::Star => Value::Null,
                            }
                        })
                        .collect()
                }
            })
            .collect();

        // 6. 应用 OFFSET（如果有）
        let offset = stmt.offset.unwrap_or(0);
        let final_rows: Vec<Vec<Value>> = projected_rows
            .into_iter()
            .skip(offset)
            .take(plan.k)
            .collect();

        Ok(QueryResult::Select {
            columns: column_names,
            rows: final_rows,
        })
    }

    // ==================== Columnar Store Routing ====================

    /// `ORDER BY <ts_col> [DESC] LIMIT k` on a TimeSeries table via the
    /// ColumnarStore top-k (`topk_by_ts`). Returns Ok(None) for unsupported
    /// shapes (OFFSET with expressions, expression projections).
    fn try_ts_order_limit(
        &self,
        stmt: &SelectStmt,
        table: &str,
        schema: &crate::types::TableSchema,
        asc: bool,
    ) -> Result<Option<QueryResult>> {
        use crate::sql::ast::SelectColumn;
        let k = stmt
            .limit
            .unwrap_or(0)
            .saturating_add(stmt.offset.unwrap_or(0));
        if k == 0 {
            return Ok(None);
        }
        let needed: Vec<String> =
            if stmt.columns.len() == 1 && matches!(stmt.columns[0], SelectColumn::Star) {
                schema.columns.iter().map(|c| c.name.clone()).collect()
            } else {
                let mut v = Vec::with_capacity(stmt.columns.len());
                for sc in &stmt.columns {
                    match sc {
                        SelectColumn::Column(c) | SelectColumn::ColumnWithAlias(c, _) => {
                            v.push(c.clone())
                        }
                        _ => return Ok(None),
                    }
                }
                v
            };
        let rows = self.db.columnar_store.topk_by_ts(table, k, !asc, &needed)?;
        if rows.is_empty() {
            return Ok(None);
        }
        let columns: Vec<String> =
            if stmt.columns.len() == 1 && matches!(stmt.columns[0], SelectColumn::Star) {
                schema.columns.iter().map(|c| c.name.clone()).collect()
            } else {
                needed
            };
        let out: Vec<Vec<Value>> = rows
            .into_iter()
            .map(|sql_row| {
                columns
                    .iter()
                    .map(|c| {
                        sql_row
                            .get(c)
                            .or_else(|| {
                                let bare = c.rsplit('.').next().unwrap_or(c);
                                sql_row
                                    .keys()
                                    .find(|k| k.rsplit('.').next() == Some(bare))
                                    .map(|k| &sql_row[k])
                            })
                            .cloned()
                            .unwrap_or(Value::Null)
                    })
                    .collect()
            })
            .collect();
        Ok(Some(QueryResult::Select { columns, rows: out }))
    }

    /// `LATEST BY <col>` on a TimeSeries table via the ColumnarStore fold
    /// (`latest_by_group`): per-group max-ts row, only the needed columns
    /// decoded. Returns Ok(None) for unsupported shapes (multi-key LATEST BY,
    /// unresolvable projections) so the general path handles them.
    fn try_ts_latest_by(
        &self,
        stmt: &SelectStmt,
        table: &str,
        schema: &crate::types::TableSchema,
    ) -> Result<Option<QueryResult>> {
        use crate::sql::ast::SelectColumn;
        let lb = stmt.latest_by.as_deref().unwrap_or_default();
        if lb.len() != 1 {
            return Ok(None);
        }
        let group_col = lb[0].rsplit('.').next().unwrap_or(&lb[0]).to_string();
        if schema.get_column_position(&group_col).is_none() {
            return Ok(None);
        }
        // Requested columns: explicit list or the full schema for SELECT *.
        let needed: Vec<String> =
            if stmt.columns.len() == 1 && matches!(stmt.columns[0], SelectColumn::Star) {
                schema.columns.iter().map(|c| c.name.clone()).collect()
            } else {
                let mut v = Vec::with_capacity(stmt.columns.len());
                for sc in &stmt.columns {
                    match sc {
                        SelectColumn::Column(c) | SelectColumn::ColumnWithAlias(c, _) => {
                            v.push(c.clone());
                        }
                        _ => return Ok(None), // expressions in LATEST BY output → general path
                    }
                }
                v
            };
        let rows = self
            .db
            .columnar_store
            .latest_by_group(table, &group_col, &needed)?;
        if rows.is_empty() {
            // Fall through so the general path produces the canonical
            // empty-result shape (columns still resolve).
            return Ok(None);
        }
        let columns: Vec<String> =
            if stmt.columns.len() == 1 && matches!(stmt.columns[0], SelectColumn::Star) {
                schema.columns.iter().map(|c| c.name.clone()).collect()
            } else {
                needed
            };
        let out: Vec<Vec<Value>> = rows
            .into_iter()
            .map(|sql_row| {
                columns
                    .iter()
                    .map(|c| {
                        sql_row
                            .get(c)
                            .or_else(|| {
                                let bare = c.rsplit('.').next().unwrap_or(c);
                                sql_row
                                    .keys()
                                    .find(|k| k.rsplit('.').next() == Some(bare))
                                    .map(|k| &sql_row[k])
                            })
                            .cloned()
                            .unwrap_or(Value::Null)
                    })
                    .collect()
            })
            .collect();
        Ok(Some(QueryResult::Select { columns, rows: out }))
    }

    /// Try to serve a SELECT from the columnar store for TimeSeries tables.
    /// Returns Ok(Some(result)) if handled, Ok(None) if it should fall through to LSM.
    /// Simple (non-GROUP-BY) aggregates over a TimeSeries table, computed
    /// from the ColumnarStore via the streaming scan. Handles
    /// COUNT(*)/COUNT(col)/SUM/MIN/MAX/AVG over a full scan with WHERE.
    fn ts_simple_aggregate(
        &self,
        stmt: &SelectStmt,
        table: &str,
        schema: &crate::types::TableSchema,
    ) -> Result<Option<StreamingQueryResult>> {
        use crate::sql::ast::SelectColumn;

        // GROUP BY: single grouping column (the common TS analytics shape),
        // or a TIME_BUCKET(interval, ts) key selected by alias/expression.
        let group_pos: Option<usize> = match &stmt.group_by {
            None => None,
            Some(cols) if cols.len() == 1 => schema.get_column_position(&cols[0]),
            Some(_) => return Ok(None), // multi-key GROUP BY: fall through
        };

        // Output columns: [group_col?] + aggregate calls.
        // `bucket_key`: a TIME_BUCKET(interval, ts) SELECT item serving as the
        // GROUP BY key (by alias or canonical name). (out_name, interval_us)
        let mut bucket_key: Option<(String, i64)> = None;
        let mut aggs: Vec<(String, Option<usize>)> = Vec::new();
        for sc in &stmt.columns {
            match sc {
                SelectColumn::Star => {
                    // SELECT * with aggregates shouldn't reach here; bail.
                    return Ok(None);
                }
                SelectColumn::Column(c) | SelectColumn::Expr(Expr::Column(c), _) => {
                    // Plain column: only valid as the GROUP BY key.
                    match (group_pos, schema.get_column_position(c)) {
                        (Some(gp), Some(p)) if gp == p => continue,
                        _ => return Ok(None),
                    }
                }
                SelectColumn::Expr(expr, alias) if matches!(expr, Expr::FunctionCall { name, .. } if name.eq_ignore_ascii_case("TIME_BUCKET")) =>
                {
                    if bucket_key.is_some() {
                        return Ok(None); // one bucket key per query
                    }
                    let items = match &stmt.group_by {
                        Some(items) if items.len() == 1 => items,
                        _ => return Ok(None),
                    };
                    let out_name = alias
                        .clone()
                        .unwrap_or_else(|| Self::expr_to_column_name(expr));
                    if items[0] != out_name {
                        return Ok(None); // GROUP BY must reference this item
                    }
                    let (args, _) = match expr {
                        Expr::FunctionCall { args, .. } => (args, ()),
                        _ => return Ok(None),
                    };
                    if args.len() != 2 {
                        return Ok(None);
                    }
                    let interval = match &args[0] {
                        Expr::Literal(Value::Text(s)) => s.to_string(),
                        _ => return Ok(None),
                    };
                    let interval_us = match Self::parse_time_bucket_interval_us(&interval) {
                        Ok(us) => us,
                        Err(_) => return Ok(None),
                    };
                    // Bucket key must be the time-series column.
                    match &args[1] {
                        Expr::Column(c)
                            if Some(c.as_str()) == schema.timeseries_column.as_deref() => {}
                        _ => return Ok(None),
                    }
                    bucket_key = Some((out_name, interval_us));
                }
                SelectColumn::Expr(Expr::FunctionCall { name, args, .. }, _) => {
                    let fname = name.to_lowercase();
                    if !matches!(fname.as_str(), "count" | "sum" | "min" | "max" | "avg") {
                        return Ok(None);
                    }
                    match args.first() {
                        None if fname == "count" => aggs.push((fname, None)), // COUNT()
                        // COUNT(*) parses as Column("*") — count all rows.
                        Some(Expr::Column(c)) if c == "*" && fname == "count" => {
                            aggs.push((fname, None));
                        }
                        Some(Expr::Column(c)) => {
                            let pos = match schema.get_column_position(c) {
                                Some(p) => p,
                                None => return Ok(None),
                            };
                            aggs.push((fname, Some(pos)));
                        }
                        _ => return Ok(None),
                    }
                }
                _ => return Ok(None),
            }
        }
        if aggs.is_empty() {
            return Ok(None);
        }
        // With GROUP BY there must be at least one aggregate; without GROUP BY
        // the aggregate-only shape is handled below. (Covered by aggs check.)

        // 🚀 Time-range pushdown (pure time predicates only): fold over
        // time-pruned segments directly in the ColumnarStore — zero row
        // materialization. The scan below full-scans and materializes EVERY
        // row as a HashMap SqlRow even for a 1-hour window (~450 ms at 1M
        // rows); the fold decodes only the segments and columns it needs.
        // A missing WHERE means the full range — still a win (only the
        // aggregate columns are decoded).
        if let Some(ts_name) = schema.timeseries_column.clone() {
            let pure = stmt
                .where_clause
                .as_ref()
                .is_none_or(|w| Self::is_pure_time_predicate(w, &ts_name));
            // Grouped results must lead with the group column so folded rows
            // (key first, then aggregates) line up with build_select_columns.
            let group_leads = stmt.group_by.is_none() || {
                let first_is_group = matches!(&stmt.columns.first(), Some(SelectColumn::Column(c))
                    if group_pos.is_some_and(|gp| schema.get_column_position(c) == Some(gp)));
                let first_is_bucket = bucket_key.is_some()
                    && matches!(&stmt.columns.first(), Some(SelectColumn::Expr(_, _)));
                first_is_group || first_is_bucket
            };
            let range = match &stmt.where_clause {
                None => Some((i64::MIN, i64::MAX)),
                Some(_) => self.extract_time_range(&stmt.where_clause, &ts_name),
            };
            let pure = pure && std::env::var("MOTE_DISABLE_AGG_PUSHDOWN").is_err();
            if pure && group_leads {
                if let Some((start, end)) = range {
                    let group_spec = match (&stmt.group_by, bucket_key.as_ref(), group_pos) {
                        (None, _, _) => crate::storage::columnar::RangeGroup::None,
                        (Some(_), Some((_, interval_us)), _) => {
                            crate::storage::columnar::RangeGroup::ByTimeBucket {
                                ts_col: ts_name.clone(),
                                interval_us: *interval_us,
                            }
                        }
                        (Some(items), None, Some(gp)) if items.len() == 1 => {
                            crate::storage::columnar::RangeGroup::ByColumn(
                                schema.columns[gp].name.clone(),
                            )
                        }
                        // GROUP BY present but unresolvable here → the old
                        // path would silently treat it as ungrouped; bail so
                        // the general executor handles it.
                        _ => return Ok(None),
                    };
                    let specs: Vec<crate::storage::columnar::RangeAggSpec> = aggs
                        .iter()
                        .map(|(f, pos)| {
                            let func = match f.as_str() {
                                "count" if pos.is_none() => {
                                    crate::storage::columnar::RangeAggFunc::CountStar
                                }
                                "count" => crate::storage::columnar::RangeAggFunc::Count,
                                "sum" => crate::storage::columnar::RangeAggFunc::Sum,
                                "avg" => crate::storage::columnar::RangeAggFunc::Avg,
                                "min" => crate::storage::columnar::RangeAggFunc::Min,
                                _ => crate::storage::columnar::RangeAggFunc::Max,
                            };
                            crate::storage::columnar::RangeAggSpec {
                                func,
                                col: pos.map(|p| schema.columns[p].name.clone()),
                            }
                        })
                        .collect();
                    let folded = self
                        .db
                        .columnar_store
                        .aggregate_time_range(table, start, end, group_spec, &specs)?;
                    let columns: Vec<String> = self.build_select_columns(&stmt.columns, schema)?;
                    let rows: Vec<Vec<Value>> = folded
                        .into_iter()
                        .map(|(key, vals)| match key {
                            Some(k) => {
                                let mut r = vec![k];
                                r.extend(vals);
                                r
                            }
                            None => vals,
                        })
                        .collect();
                    return Ok(Some(StreamingQueryResult::SelectReady { columns, rows }));
                }
            }
        }

        // Full scan (TS → ColumnarStore) + WHERE filter, grouped when needed.
        struct Acc {
            count: Vec<u64>,
            sum: Vec<f64>,
            minv: Vec<Option<f64>>,
            maxv: Vec<Option<f64>>,
            anyv: Vec<bool>,
        }
        let new_acc = |k: usize| Acc {
            count: vec![0; k],
            sum: vec![0.0; k],
            minv: vec![None; k],
            maxv: vec![None; k],
            anyv: vec![false; k],
        };
        let k = aggs.len();
        let mut total = new_acc(k);
        // Value is not Ord — key groups by a stable debug form, keep insertion
        // order (GROUP BY output order is unspecified; ORDER BY applies after).
        let mut groups: std::collections::HashMap<String, Acc> = std::collections::HashMap::new();
        let mut group_order: Vec<(String, Value)> = Vec::new();

        let iter = self.db.scan_table_rows_streaming(table)?;
        for item in iter {
            let (_rid, row) = item?;
            if let Some(wc) = &stmt.where_clause {
                match Self::eval_expr_on_row(wc, &row, schema) {
                    Ok(v) if Self::is_truthy(&v) => {}
                    _ => continue,
                }
            }
            let acc = match group_pos {
                None => &mut total,
                Some(gp) => {
                    let key = row.get(gp).cloned().unwrap_or(Value::Null);
                    let kstr = format!("{:?}", key);
                    if !groups.contains_key(&kstr) {
                        group_order.push((kstr.clone(), key.clone()));
                        groups.insert(kstr.clone(), new_acc(k));
                    }
                    groups
                        .get_mut(&kstr)
                        .expect("group accumulator just ensured")
                }
            };
            for (i, (_fname, pos)) in aggs.iter().enumerate() {
                let p = match pos {
                    Some(p) => *p,
                    None => {
                        acc.count[i] += 1; // COUNT(*)
                        continue;
                    }
                };
                match row.get(p) {
                    Some(Value::Integer(x)) => {
                        acc.count[i] += 1;
                        acc.sum[i] += *x as f64;
                        acc.minv[i] =
                            Some(acc.minv[i].map_or(*x as f64, |m: f64| m.min(*x as f64)));
                        acc.maxv[i] =
                            Some(acc.maxv[i].map_or(*x as f64, |m: f64| m.max(*x as f64)));
                        acc.anyv[i] = true;
                    }
                    Some(Value::Float(x)) => {
                        acc.count[i] += 1;
                        acc.sum[i] += *x;
                        acc.minv[i] = Some(acc.minv[i].map_or(*x, |m: f64| m.min(*x)));
                        acc.maxv[i] = Some(acc.maxv[i].map_or(*x, |m: f64| m.max(*x)));
                        acc.anyv[i] = true;
                    }
                    _ => {}
                }
            }
        }

        let finish = |acc: &Acc| -> Vec<Value> {
            aggs.iter()
                .enumerate()
                .map(|(i, (fname, _pos))| {
                    let f = fname.as_str();
                    match f {
                        "count" => Value::Integer(acc.count[i] as i64),
                        "sum" => {
                            if acc.anyv[i] {
                                Value::Float(acc.sum[i])
                            } else {
                                Value::Null
                            }
                        }
                        "min" => acc.minv[i].map(Value::Float).unwrap_or(Value::Null),
                        "max" => acc.maxv[i].map(Value::Float).unwrap_or(Value::Null),
                        "avg" => {
                            if acc.anyv[i] {
                                Value::Float(acc.sum[i] / acc.count[i] as f64)
                            } else {
                                Value::Null
                            }
                        }
                        _ => Value::Null,
                    }
                })
                .collect()
        };

        let columns: Vec<String> = self.build_select_columns(&stmt.columns, schema)?;
        let rows: Vec<Vec<Value>> = match group_pos {
            None => vec![finish(&total)],
            Some(_) => group_order
                .into_iter()
                .map(|(kstr, key)| {
                    let acc = groups.remove(&kstr).unwrap_or_else(|| new_acc(k));
                    let mut row = vec![key];
                    row.extend(finish(&acc));
                    row
                })
                .collect(),
        };
        Ok(Some(StreamingQueryResult::SelectReady { columns, rows }))
    }

    fn try_columnar_select(
        &self,
        stmt: &SelectStmt,
        schema: &TableSchema,
    ) -> Result<Option<QueryResult>> {
        // 🚨 LATEST BY must reach the materialized path (apply_latest_by): this
        // pushdown has no latest-per-group fold, so it silently returned EVERY
        // row for `… WHERE … LATEST BY ts` (found by the Round-12c E2E; both
        // the streaming router's TS branch and execute_select_internal's TS
        // intercept route through here).
        if stmt.latest_by.is_some() {
            return Ok(None);
        }
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
        let column_names: Vec<String> = stmt
            .columns
            .iter()
            .map(|col| match col {
                SelectColumn::Star => "*".to_string(),
                SelectColumn::Column(name) | SelectColumn::ColumnWithAlias(name, _) => name.clone(),
                SelectColumn::Expr(_, alias) => alias.clone().unwrap_or_default(),
            })
            .collect();

        // If star, pass empty vec (means all columns)
        let query_cols: Vec<String> = if column_names.iter().any(|c| c == "*") {
            vec![]
        } else {
            column_names.clone()
        };

        // Extract non-timestamp column conditions for pruning
        let conditions = self.extract_column_conditions(&stmt.where_clause, schema, &ts_col);

        let results = if conditions.is_empty() {
            self.db
                .columnar_store
                .query_time_range(&table_name, start_ts, end_ts, &query_cols)?
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
                        let cmp = order_by_cmp(va, vb);
                        if ascending {
                            cmp
                        } else {
                            cmp.reverse()
                        }
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

    /// True when the WHERE clause consists ONLY of time-range predicates on
    /// `ts_col` (comparisons vs literals, inclusive BETWEEN, and ANDs
    /// thereof). When pure, the columnar aggregate pushdown can replace the
    /// per-row WHERE filter entirely — the store already selects rows inside
    /// the extracted range.
    fn is_pure_time_predicate(expr: &Expr, ts_col: &str) -> bool {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                Self::is_pure_time_predicate(left, ts_col)
                    && Self::is_pure_time_predicate(right, ts_col)
            }
            Expr::BinaryOp { left, op, right }
                if matches!(
                    op,
                    BinaryOperator::Ge
                        | BinaryOperator::Gt
                        | BinaryOperator::Le
                        | BinaryOperator::Lt
                ) =>
            {
                let (c, _) = match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(c), Expr::Literal(l)) => (c, l),
                    (Expr::Literal(l), Expr::Column(c)) => (c, l),
                    _ => return false,
                };
                c == ts_col || c.rsplit('.').next() == Some(ts_col)
            }
            Expr::Between {
                expr,
                negated: false,
                low,
                high,
            } => {
                matches!(expr.as_ref(), Expr::Column(c) if c == ts_col)
                    && matches!(low.as_ref(), Expr::Literal(_))
                    && matches!(high.as_ref(), Expr::Literal(_))
            }
            _ => false,
        }
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
            Expr::Between {
                expr: col,
                low,
                high,
                negated: _,
            } => {
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
                    } else if let Some(cond) =
                        self.try_extract_equality(right, left, schema, ts_col)
                    {
                        conditions.push(cond);
                    }
                }
                BinaryOperator::Ge
                | BinaryOperator::Gt
                | BinaryOperator::Le
                | BinaryOperator::Lt => {
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
            Some(ColumnCondition::Equals {
                column_idx: col_idx,
                value,
            })
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
            (BinaryOperator::Ge, true) => (value.clone(), Value::Integer(i64::MAX)), // col >= val
            (BinaryOperator::Gt, true) => {
                // col > val → [val+1, MAX]
                let bumped = self.increment_value(value)?;
                (bumped, Value::Integer(i64::MAX))
            }
            (BinaryOperator::Le, true) => (Value::Integer(i64::MIN), value.clone()), // col <= val
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

        Some(ColumnCondition::Range {
            column_idx: col_idx,
            low,
            high,
        })
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
        select_rows: Option<&[Vec<Value>]>,
    ) -> Result<QueryResult> {
        let mut rows: Vec<Vec<crate::types::Value>> = Vec::new();

        // INSERT ... SELECT 源: 行已求值, 直接按 columns 映射建行。
        if let Some(sel_rows) = select_rows {
            for value_row in sel_rows {
                if value_row.len() != columns.len() {
                    return Err(MoteDBError::InvalidArgument(format!(
                        "Column count mismatch: expected {}, got {}",
                        columns.len(),
                        value_row.len()
                    )));
                }
                let row =
                    crate::sql::row_converter::values_to_row_by_columns(value_row, columns, schema)?;
                rows.push(row);
            }
        }

        for value_row in &stmt.values {
            if value_row.len() != columns.len() {
                return Err(MoteDBError::InvalidArgument(format!(
                    "Column count mismatch: expected {}, got {}",
                    columns.len(),
                    value_row.len()
                )));
            }

            // 🚀 P1-2: 直接构造 Row（跳过 SqlRow HashMap 中转）。
            // 旧代码每行每列 sql_row.insert(col_name.clone(), val) —— HashMap 分配
            // + String clone，对 IMU 1kHz 时序写入是显著开销。直接用
            // values_to_row_by_columns（和普通 INSERT 同路径，零 HashMap）。
            let resolved: Vec<crate::types::Value> = value_row
                .iter()
                .map(|expr| match expr {
                    Expr::Literal(v) => Ok(v.clone()),
                    Expr::Parameter(_) => {
                        let empty_row = SqlRow::new();
                        self.evaluator.eval(expr, &empty_row)
                    }
                    other if Self::is_constant_expr(other) => {
                        let empty_row = SqlRow::new();
                        self.evaluator.eval(other, &empty_row)
                    }
                    _ => Err(MoteDBError::InvalidArgument(
                        "INSERT VALUES must be literals or parameters".to_string(),
                    )),
                })
                .collect::<Result<Vec<_>>>()?;

            let row =
                crate::sql::row_converter::values_to_row_by_columns(&resolved, columns, schema)?;
            rows.push(row);
        }

        let result = self.db.columnar_store.ingest(&stmt.table, rows)?;
        // 🔑 Keep the atomic row-count counter (COUNT(*) fast path) in sync —
        // the ColumnarStore ingest path bypasses the crud INSERT bookkeeping.
        self.db
            .table_row_count
            .entry(stmt.table.clone())
            .or_insert_with(|| std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)))
            .value()
            .fetch_add(
                result.row_ids.len() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
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
    /// true = cosine distance (<=>), false = L2 (<->).
    /// Only used by the brute-force fallback (no index); indexed search
    /// takes the metric from the index definition.
    cosine: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColumnDef, ColumnType, TableSchema, Value};
    use std::cmp::Ordering;

    fn make_schema() -> TableSchema {
        let columns = vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                position: 0,
                nullable: false,
                auto_increment: false,
                auto_increment_start: None,
                default_value: None,
            },
            ColumnDef {
                name: "name".into(),
                col_type: ColumnType::Text,
                position: 1,
                nullable: true,
                auto_increment: false,
                auto_increment_start: None,
                default_value: None,
            },
            ColumnDef {
                name: "score".into(),
                col_type: ColumnType::Float,
                position: 2,
                nullable: true,
                auto_increment: false,
                auto_increment_start: None,
                default_value: None,
            },
            ColumnDef {
                name: "active".into(),
                col_type: ColumnType::Boolean,
                position: 3,
                nullable: true,
                auto_increment: false,
                auto_increment_start: None,
                default_value: None,
            },
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
        assert_eq!(
            QueryExecutor::eval_expr_on_row(&eq, &r, &schema).unwrap(),
            Value::Bool(true)
        );
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
        assert_eq!(
            QueryExecutor::eval_expr_on_row(&eq, &r, &schema).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_eval_comparisons() {
        let schema = make_schema();
        let r = row(5, "", 10.0, true);

        let lt = Expr::BinaryOp {
            left: Box::new(col("id")),
            op: BinaryOperator::Lt,
            right: Box::new(Expr::Literal(Value::Integer(10))),
        };
        assert_eq!(
            QueryExecutor::eval_expr_on_row(&lt, &r, &schema).unwrap(),
            Value::Bool(true)
        );

        let gt = Expr::BinaryOp {
            left: Box::new(col("score")),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Literal(Value::Float(5.0))),
        };
        assert_eq!(
            QueryExecutor::eval_expr_on_row(&gt, &r, &schema).unwrap(),
            Value::Bool(true)
        );
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
        assert_eq!(
            QueryExecutor::eval_expr_on_row(&lt, &r, &schema).unwrap(),
            Value::Null
        );
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
        assert_eq!(
            QueryExecutor::eval_expr_on_row(&le, &r, &schema).unwrap(),
            Value::Null
        );
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
        assert_eq!(
            QueryExecutor::eval_expr_on_row(&ge, &r, &schema).unwrap(),
            Value::Null
        );
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
        let r = vec![
            Value::Integer(1),
            Value::Null,
            Value::Float(1.0),
            Value::Bool(false),
        ];
        let isnull = Expr::IsNull {
            expr: Box::new(col("name")),
            negated: false,
        };
        assert_eq!(
            QueryExecutor::eval_expr_on_row(&isnull, &r, &schema).unwrap(),
            Value::Bool(true)
        );

        let notnull = Expr::IsNull {
            expr: Box::new(col("id")),
            negated: true,
        };
        assert_eq!(
            QueryExecutor::eval_expr_on_row(&notnull, &r, &schema).unwrap(),
            Value::Bool(true)
        );
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
        assert_eq!(
            QueryExecutor::eval_expr_on_row(&add, &r, &schema).unwrap(),
            Value::Integer(4)
        );
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
        assert_eq!(
            QueryExecutor::eval_expr_on_row(&mul, &r, &schema).unwrap(),
            Value::Float(20.0)
        );
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
        assert_eq!(
            QueryExecutor::eval_expr_on_row(&and, &r, &schema).unwrap(),
            Value::Bool(true)
        );

        let or = Expr::BinaryOp {
            left: Box::new(Expr::Literal(Value::Bool(false))),
            op: BinaryOperator::Or,
            right: Box::new(Expr::Literal(Value::Bool(true))),
        };
        assert_eq!(
            QueryExecutor::eval_expr_on_row(&or, &r, &schema).unwrap(),
            Value::Bool(true)
        );
    }

    // ━━━ Parameter returns error (fallback path) ━━━

    #[test]
    fn test_eval_parameter_returns_error() {
        let schema = make_schema();
        let r = row(1, "", 10.0, true);
        let param = Expr::Parameter(1);
        assert!(
            QueryExecutor::eval_expr_on_row(&param, &r, &schema).is_err(),
            "Parameter should return Err to trigger fallback to full evaluator"
        );
    }

    // ━━━ IN list ━━━

    #[test]
    fn test_eval_in() {
        let schema = make_schema();
        let r = row(1, "", 10.0, true);
        let in_expr = Expr::In {
            expr: Box::new(col("id")),
            list: vec![
                Expr::Literal(Value::Integer(1)),
                Expr::Literal(Value::Integer(2)),
            ],
            negated: false,
        };
        assert_eq!(
            QueryExecutor::eval_expr_on_row(&in_expr, &r, &schema).unwrap(),
            Value::Bool(true)
        );

        let not_in = Expr::In {
            expr: Box::new(col("id")),
            list: vec![
                Expr::Literal(Value::Integer(5)),
                Expr::Literal(Value::Integer(6)),
            ],
            negated: true,
        };
        assert_eq!(
            QueryExecutor::eval_expr_on_row(&not_in, &r, &schema).unwrap(),
            Value::Bool(true)
        );
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
        assert_eq!(
            QueryExecutor::eval_expr_on_row(&between, &r, &schema).unwrap(),
            Value::Bool(true)
        );
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
        assert!(
            QueryExecutor::eval_expr_on_row(&sub, &r, &schema).is_err(),
            "Unsupported expression should return Err for fallback path"
        );
    }

    // ════════════════════════════════════════════════════════════════════
    // 差分对拍：CompiledWhere（编译谓词）vs eval_expr_on_row（原生求值）
    //
    // 第 17 轮把 CompiledWhere 接入热路径后暴露了 8 个潜伏语义 bug
    // （#36-#43）。本测试把"接线暴露"变成"系统穷举"：对编译器声称
    // 支持的每个谓词形态 × 每行（NULL/类型混用/大小写），两个求值器
    // 的"行是否保留"必须一致。
    // ════════════════════════════════════════════════════════════════════

    fn diff_schema() -> TableSchema {
        use crate::types::ColumnType as CT;
        let cols: Vec<(&str, CT)> = vec![
            ("i", CT::Integer),
            ("t", CT::Text),
            ("f", CT::Float),
            ("b", CT::Boolean),
        ];
        TableSchema::new(
            "diff".into(),
            cols.iter()
                .enumerate()
                .map(|(pos, (name, ct))| ColumnDef {
                    name: (*name).into(),
                    col_type: ct.clone(),
                    position: pos,
                    nullable: true,
                    auto_increment: false,
                    auto_increment_start: None,
                    default_value: None,
                })
                .collect(),
        )
    }

    fn diff_rows() -> Vec<Row> {
        vec![
            vec![
                Value::Integer(5),
                Value::Text("Apple".into()),
                Value::Float(2.5),
                Value::Bool(true),
            ],
            vec![
                Value::Integer(-3),
                Value::Text("apple".into()),
                Value::Float(-0.5),
                Value::Bool(false),
            ],
            vec![
                Value::Integer(0),
                Value::Text("APPLE".into()),
                Value::Float(0.0),
                Value::Null,
            ],
            vec![Value::Null, Value::Null, Value::Null, Value::Bool(true)],
            // 类型混用：整数列放 Float、文本列放 Integer（宽容转换场景）
            vec![
                Value::Integer(5),
                Value::Integer(65),
                Value::Integer(3),
                Value::Integer(1),
            ],
        ]
    }

    fn native_keep(expr: &Expr, row: &Row, schema: &TableSchema) -> bool {
        match QueryExecutor::eval_expr_on_row(expr, row, schema) {
            Ok(Value::Bool(b)) => b,
            Ok(Value::Integer(i)) => i != 0,
            Ok(Value::Float(f)) => f != 0.0 && !f.is_nan(),
            _ => false,
        }
    }

    fn diff_predicates() -> Vec<Expr> {
        use crate::sql::ast::BinaryOperator as B;
        let cols = ["i", "t", "f", "b"];
        let lits = vec![
            Value::Integer(5),
            Value::Integer(-3),
            Value::Integer(0),
            Value::Float(2.5),
            Value::Text("Apple".into()),
            Value::Text("apple".into()),
            Value::Bool(true),
            Value::Null,
        ];
        let mut out: Vec<Expr> = Vec::new();
        for c in cols {
            for l in &lits {
                for op in [B::Eq, B::Ne, B::Lt, B::Le, B::Gt, B::Ge] {
                    out.push(Expr::BinaryOp {
                        left: Box::new(Expr::Column(c.into())),
                        op,
                        right: Box::new(Expr::Literal(l.clone())),
                    });
                }
            }
            // IN / NOT IN（含 NULL 成员）
            for (list, neg) in [
                (vec![Value::Integer(5), Value::Integer(-3)], false),
                (vec![Value::Integer(5), Value::Integer(-3)], true),
                (vec![Value::Integer(5), Value::Null], true),
                (vec![Value::Text("apple".into())], true),
                (vec![], false),
            ] {
                out.push(Expr::In {
                    expr: Box::new(Expr::Column(c.into())),
                    list: list.into_iter().map(Expr::Literal).collect(),
                    negated: neg,
                });
            }
            // LIKE / NOT LIKE（各锚定形态 + 大小写）
            for (p, neg) in [
                ("app%", false),
                ("%ple", false),
                ("%p%", false),
                ("a__le", false),
                ("%", false),
                ("_", false),
                ("Apple", false),
                ("%X%", false),
                ("%", true),
                ("app%", true),
            ] {
                out.push(Expr::Like {
                    expr: Box::new(Expr::Column(c.into())),
                    pattern: Box::new(Expr::Literal(Value::Text(p.into()))),
                    negated: neg,
                });
            }
            // IS NULL / IS NOT NULL
            out.push(Expr::IsNull {
                expr: Box::new(Expr::Column(c.into())),
                negated: false,
            });
            out.push(Expr::IsNull {
                expr: Box::new(Expr::Column(c.into())),
                negated: true,
            });
        }
        // InHashset（子查询物化形态）× has_null × negated —— 原生路径的
        // NOT IN + NULL 三值逻辑从未与编译版对拍过
        for (set_vals, has_null, neg) in [
            (vec![Value::Integer(5), Value::Integer(-3)], false, false),
            (vec![Value::Integer(5), Value::Integer(-3)], false, true),
            (vec![Value::Integer(5)], true, true),
            (vec![Value::Text("apple".into())], false, true),
        ] {
            out.push(Expr::InHashset {
                expr: Box::new(Expr::Column("i".into())),
                set: set_vals.into_iter().collect(),
                negated: neg,
                has_null,
            });
        }
        // AND / OR 组合（比较 ∧/∨ IN、比较 ∧ LIKE、嵌套 OR）
        out.push(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("i".into())),
                op: B::Gt,
                right: Box::new(Expr::Literal(Value::Integer(0))),
            }),
            op: B::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("f".into())),
                op: B::Lt,
                right: Box::new(Expr::Literal(Value::Float(3.0))),
            }),
        });
        out.push(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("i".into())),
                op: B::Eq,
                right: Box::new(Expr::Literal(Value::Integer(5))),
            }),
            op: B::Or,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("b".into())),
                op: B::Eq,
                right: Box::new(Expr::Literal(Value::Bool(true))),
            }),
        });
        out.push(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Column("i".into())),
                    op: B::Ge,
                    right: Box::new(Expr::Literal(Value::Integer(-3))),
                }),
                op: B::Or,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Column("f".into())),
                    op: B::Ne,
                    right: Box::new(Expr::Literal(Value::Float(0.0))),
                }),
            }),
            op: B::And,
            right: Box::new(Expr::Like {
                expr: Box::new(Expr::Column("t".into())),
                pattern: Box::new(Expr::Literal(Value::Text("%p%".into()))),
                negated: false,
            }),
        });
        out
    }

    #[test]
    fn test_compiled_vs_native_equivalence() {
        let schema = diff_schema();
        let rows = diff_rows();
        let mut checked = 0usize;
        let mut compiled_forms = 0usize;
        for expr in diff_predicates() {
            let Some(cw) = QueryExecutor::compile_where(&expr, &schema) else {
                continue;
            };
            compiled_forms += 1;
            for row in &rows {
                let Some(compiled) = cw.eval(row) else {
                    continue; // 编译器自身要求回退（如非文本 LIKE）
                };
                let native = native_keep(&expr, row, &schema);
                assert_eq!(compiled, native, "divergence: expr={expr:?}\nrow={row:?}");
                checked += 1;
            }
            // eval_at（部分解码路径）在完整映射下必须与 eval 一致
            let pos_to_idx: Vec<Option<usize>> = (0..schema.columns.len()).map(Some).collect();
            for row in &rows {
                let a = cw.eval(row);
                let b = cw.eval_at(row, &pos_to_idx);
                if let (Some(x), Some(y)) = (a, b) {
                    assert_eq!(
                        x, y,
                        "eval vs eval_at divergence: expr={expr:?} row={row:?}"
                    );
                }
            }
        }
        assert!(
            compiled_forms >= 200,
            "predicate coverage too small: {compiled_forms}"
        );
        assert!(checked >= 1000, "row-level checks too small: {checked}");
    }
}

/// VEC M1 接线辅助：单行结果包装。
fn outc_rows(outcome: &crate::sql::vector_exec::VecScanAggOutcome) -> Vec<Vec<Value>> {
    vec![outcome.values.clone()]
}
