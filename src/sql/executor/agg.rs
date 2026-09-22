//! 聚合簇：col-segment 聚合/多聚合/GROUP BY 下推 + WHERE 解析辅助。
use super::*;

impl QueryExecutor {
    /// Materialize a SELECT via `execute_select_internal` and wrap as streaming.
    /// ColSegmentStore multi-segment aggregate: COUNT/SUM/MIN/MAX/GROUP BY
    /// without compaction. Iterates segments directly via scan_projected_filtered.
    pub(super) fn col_segment_aggregate(
        &self,
        stmt: &SelectStmt,
        table_name: &str,
        store: &crate::storage::col_segment::ColSegmentStore,
    ) -> Result<Option<StreamingQueryResult>> {
        use crate::sql::ast::{Expr, SelectColumn};
        // 🆕 HAVING requires post-aggregation filtering that this pushdown path
        // doesn't apply — fall back to the materialized path.
        if stmt.having.is_some() {
            return Ok(None);
        }
        // 🚨 Subqueries in WHERE (scalar, IN, EXISTS — possibly correlated)
        // need per-row execution by the general scan path; this pushdown
        // cannot evaluate them (differential testing: COUNT(*) WHERE EXISTS
        // silently returned 0).
        if stmt
            .where_clause
            .as_ref()
            .is_some_and(Self::expr_contains_subquery)
        {
            return Ok(None);
        }
        let schema = self.db.get_table_schema(table_name).ok();
        let schema = match schema {
            Some(s) => s,
            None => return Ok(None),
        };
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
            let mut count;
            if let Some(ref wc) = stmt.where_clause {
                // 🚀 Index-accelerated COUNT(*): if a column index exists on the
                // WHERE filter column, use it to count matching rows in O(log N + K)
                // instead of scanning the entire table O(N).
                if let Some((pos, op, target)) = Self::parse_simple_comparison_where(wc, &schema) {
                    // Check if an index exists on this column.
                    let col_name = &schema.columns[pos].name;
                    let index_key = format!("{}.{}", table_name, col_name);
                    let indexed_count = if matches!(op, crate::sql::ast::BinaryOperator::Eq)
                        // 🚨 Long Text values are truncated to a 64-byte
                        // prefix in index keys — the index cannot answer
                        // exact counts for them (prefix-sharing values
                        // cross-count). Fall back to count_filtered.
                        && Self::index_key_exact_for(&target)
                    {
                        // Equality lookup: index.get(value) → row_ids → count.
                        // 🚨 An EMPTY result is NOT authoritative: the index can
                        // be stale (async rebuild window, or a crash between
                        // data and index writes). Only trust a non-empty hit;
                        // fall back to count_filtered otherwise — it returns
                        // the same 0 for genuinely-missing values.
                        self.db.column_indexes.get(&index_key).and_then(|index| {
                            let idx = index.value();
                            idx.get(&target)
                                .ok()
                                .filter(|ids| !ids.is_empty())
                                .map(|ids| {
                                    // 🔑 Verify: index entries can be stale
                                    // inside a transaction (undo replays keep
                                    // old value→row_id entries until commit;
                                    // differential testing: COUNT WHERE id=X
                                    // counted 7/8 phantoms in the ROLLBACK-TO
                                    // window while row LIST was correct).
                                    let verified = self
                                        .db
                                        .get_table_rows_batch(table_name, &ids)
                                        .map(|batch| {
                                            batch
                                                .iter()
                                                .filter(|(_, opt)| {
                                                    opt.as_ref()
                                                        .and_then(|row| row.get(pos))
                                                        .map(|v| v == &target)
                                                        .unwrap_or(false)
                                                })
                                                .count()
                                        })
                                        .unwrap_or(ids.len());
                                    verified as i64
                                })
                        })
                    } else {
                        None
                    };
                    count = indexed_count
                        .unwrap_or_else(|| store.count_filtered(pos, &op, &target) as i64);
                } else if let crate::sql::ast::Expr::Like {
                    expr,
                    pattern,
                    negated: false,
                } = wc
                {
                    // COUNT(*) WHERE col LIKE 'prefix%' — zero-alloc prefix scan.
                    if let (
                        crate::sql::ast::Expr::Column(cn),
                        crate::sql::ast::Expr::Literal(Value::Text(s)),
                    ) = (expr.as_ref(), pattern.as_ref())
                    {
                        let pat = s.as_str();
                        if pat.ends_with('%')
                            && !pat[..pat.len() - 1].contains('%')
                            && schema
                                .get_column_position(cn)
                                .map(|pos| {
                                    matches!(schema.col_types().get(pos), Some(ColumnType::Text))
                                })
                                .unwrap_or(false)
                        {
                            let prefix = &pat[..pat.len() - 1];
                            let pos = schema.get_column_position(cn).unwrap();
                            let _ = store.flush_buffer();
                            // 🚀 Use count_prefix_matches (zero allocation)
                            // instead of scan_row_indices_prefix which builds
                            // a Vec just to read .len().
                            count = store.count_prefix_matches(pos, prefix.as_bytes()) as i64;
                        } else {
                            return Ok(None);
                        }
                    } else {
                        return Ok(None);
                    }
                } else if let Some((pos, op, target)) =
                    Self::parse_simple_comparison_where(wc, &schema)
                {
                    count = store.count_filtered(pos, &op, &target) as i64;
                } else if let crate::sql::ast::Expr::InHashset {
                    expr,
                    set,
                    negated,
                    has_null,
                } = wc
                {
                    // 🚀 COUNT(*) WHERE col IN (SELECT ...) — use raw-byte
                    // scan_row_indices_in_set (zero Value::Text allocation)
                    // instead of scan_projected_filtered with HashSet<Value>
                    // predicate (300K ArcString allocations).
                    //
                    // SQL standard: NOT IN with a NULL in the subquery result
                    // produces no rows (UNKNOWN).
                    if *negated && *has_null {
                        count = 0;
                    } else if let crate::sql::ast::Expr::Column(cn) = expr.as_ref() {
                        let bare = cn.rsplit('.').next().unwrap_or(cn);
                        if let Some(pos) = schema.get_column_position(bare) {
                            if matches!(schema.col_types().get(pos), Some(ColumnType::Text)) {
                                let _ = store.prepare_for_query();
                                let byte_set: crate::storage::lsm::columnar::ByteSet = set
                                    .iter()
                                    .filter_map(|v| {
                                        if let Value::Text(t) = v {
                                            Some(t.as_str().as_bytes())
                                        } else {
                                            None
                                        }
                                    })
                                    .collect();
                                if *negated {
                                    // NOT IN: count rows whose value is NOT in set
                                    // (and non-NULL). This needs a full scan.
                                    let _ = store.flush_buffer();
                                    // 🔑 Normalize Bool→Int for coerced matching.
                                    let pred_set: std::collections::HashSet<Value> =
                                        set.iter().map(normalize_for_in).collect();
                                    let scanned = store.scan_projected_filtered(
                                        Some(pos),
                                        &[pos],
                                        &move |fv: Option<&Value>| match fv {
                                            Some(Value::Null) | None => false,
                                            Some(v) => !pred_set.contains(&normalize_for_in(v)),
                                        },
                                    );
                                    count = scanned.len() as i64;
                                } else if byte_set.is_empty() {
                                    count = 0;
                                } else if let Some(indices) =
                                    store.scan_row_indices_in_set(pos, &byte_set, usize::MAX)
                                {
                                    count = indices.len() as i64;
                                } else {
                                    count = 0;
                                }
                            } else {
                                // Non-text column: fall back to projected scan.
                                let _ = store.flush_buffer();
                                // 🔑 Normalize Bool→Int so a BOOLEAN column
                                // matches an integer subquery set.
                                let pred_set: std::collections::HashSet<Value> =
                                    set.iter().map(normalize_for_in).collect();
                                let neg = *negated;
                                let scanned = store.scan_projected_filtered(
                                    Some(pos),
                                    &[pos],
                                    &move |fv: Option<&Value>| match fv {
                                        Some(Value::Null) | None => false,
                                        Some(v) => {
                                            let found = pred_set.contains(&normalize_for_in(v));
                                            if neg {
                                                !found
                                            } else {
                                                found
                                            }
                                        }
                                    },
                                );
                                count = scanned.len() as i64;
                            }
                        } else {
                            return Ok(None);
                        }
                    } else {
                        return Ok(None);
                    }
                } else if let Some(comparisons) =
                    Self::parse_where_comparisons(wc, &schema)
                {
                    // 🚀 COUNT(*) over a multi-term AND predicate: fused
                    // single-pass scan over raw column bytes. The old path
                    // returned None here → the materialized fallback decoded
                    // full-width rows (including VECTOR columns) just to count
                    // them (~180MB retained, ~25ms on a 100K×384 table).
                    let supported = |pos: usize| {
                        matches!(
                            schema.col_types().get(pos),
                            Some(
                                ColumnType::Integer
                                    | ColumnType::Float
                                    | ColumnType::Timestamp
                                    | ColumnType::Text
                                    | ColumnType::Boolean
                            )
                        )
                    };
                    if comparisons.len() <= 8 && comparisons.iter().all(|(c, _, _)| supported(*c)) {
                        let res = store.aggregate_multi_filtered(&comparisons, &[]);
                        count = res.rows;
                    } else {
                        return Ok(None);
                    }
                } else {
                    return Ok(None);
                }
            } else {
                // No WHERE: COUNT(*) — use the O(1) atomic row counter on MoteDB
                // (incremented on INSERT, decremented on DELETE). This avoids
                // the multi-segment scan that count_live_rows() does when there
                // are 8+ segments from chunked flushes (was 590µs for 20K rows
                // → now ~0µs). Falls back to count_live_rows only if the counter
                // is unavailable (table not tracked).
                count = match self.db.fast_row_count(table_name) {
                    Some(n) => n as i64,
                    None => store.count_live_rows() as i64,
                };
            }
            // 🔑 Read-your-writes: adjust count for uncommitted transactional
            // INSERTs. write_set INSERTs were never written to storage, so the
            // atomic counter / count_live_rows doesn't include them — add them.
            // (DELETEs already wrote a tombstone to storage during the txn, so
            // count_live_rows already excludes them — no adjustment needed.)
            // 🚨 WITH a WHERE clause the adjustment must FILTER the write_set
            // rows by the same predicate — adding ws.len() unconditionally
            // counted every uncommitted row regardless of the filter
            // (differential testing: COUNT WHERE id=X in a ROLLBACK-TO window
            // returned storage_match + ws.len()).
            if self.is_in_transaction() {
                let ws = self.txn_write_set_rows(table_name);
                if stmt.where_clause.is_none() {
                    count += ws.len() as i64;
                } else if let crate::sql::ast::Expr::Between {
                    expr,
                    low,
                    high,
                    negated,
                } = stmt.where_clause.as_ref().unwrap()
                {
                    // BETWEEN: filter write_set rows by [low, high] (or the
                    // negation). Mirrors the comparison branch below.
                    let col = match expr.as_ref() {
                        crate::sql::ast::Expr::Column(cn) => {
                            let bare = cn.rsplit('.').next().unwrap_or(cn);
                            schema.get_column_position(bare)
                        }
                        _ => None,
                    };
                    let empty = std::collections::HashMap::new();
                    let lo = self.evaluator.eval(low, &empty).ok();
                    let hi = self.evaluator.eval(high, &empty).ok();
                    if let (Some(pos), Some(lo), Some(hi)) = (col, lo, hi) {
                        let in_range = |row: &Vec<Value>| -> bool {
                            let inside =
                                row.get(pos).map(|v| v >= &lo && v <= &hi).unwrap_or(false);
                            if *negated {
                                !inside
                            } else {
                                inside
                            }
                        };
                        count += ws.iter().filter(|(_, row)| in_range(row)).count() as i64;
                    }
                } else if let Some(comparisons) =
                    Self::parse_where_comparisons(stmt.where_clause.as_ref().unwrap(), &schema)
                {
                    // AND semantics: count write_set rows matching EVERY
                    // comparison (BETWEEN arrives here rewritten as
                    // col >= low AND col <= high).
                    let row_matches = |row: &Vec<Value>| -> bool {
                        comparisons.iter().all(|&(pos, ref op, ref target)| {
                            row.get(pos)
                                .map(|v| {
                                    self.evaluator
                                        .eval_binary_op(op, v.clone(), target.clone())
                                        .map(|r| matches!(r, Value::Bool(true)))
                                        .unwrap_or(false)
                                })
                                .unwrap_or(false)
                        })
                    };
                    count += ws.iter().filter(|(_, row)| row_matches(row)).count() as i64;
                }
                // Other predicate shapes: fall through WITHOUT the write_set
                // adjustment (undercount is safer than phantom overcount;
                // committed rows are always correct).
            }
            let columns: Vec<String> = self
                .build_select_columns(&stmt.columns, &schema)
                .unwrap_or_default();
            return Ok(Some(StreamingQueryResult::SelectReady {
                columns,
                rows: vec![vec![Value::Integer(count)]],
            }));
        }

        // DISTINCT: scan the distinct column(s) and dedup.
        if stmt.distinct {
            let out_pos: Vec<usize> =
                Self::resolve_select_positions(&stmt.columns, &schema).unwrap_or_default();
            if !out_pos.is_empty() {
                let dc = out_pos[0];
                // 🚀 Fast path: for single-column DISTINCT on a TEXT column, use
                // distinct_text_values (adaptive early-exit, ~0.5ms for a 2-value
                // column) instead of materializing+deduping all 300K rows (~46ms).
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
                    return Ok(Some(StreamingQueryResult::SelectReady { columns, rows }));
                }
                let scanned = store.scan_projected_filtered(Some(dc), &out_pos, &|_| true);
                // 🔑 Dedup on the FULL row (all selected columns), not just the
                // first. The old code used `row.first()` as the key, which made
                // `DISTINCT a, b` dedup on `a` alone — silently dropping rows
                // like (1,2) when (1,1) was already seen.
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
                return Ok(Some(StreamingQueryResult::SelectReady { columns, rows }));
            }
        }

        // ORDER BY + LIMIT: scan only the sort column to find top-K indices,
        // then decode output columns ONLY for those K rows (not all N).
        // 🔑 PERF: the old code decoded ALL columns for ALL N rows via
        // scan_projected_filtered, then sorted and took LIMIT. For LIMIT 10 on
        // 20K rows, it decoded 20K TEXT ArcStrings that were thrown away.
        // Now: scan 1 fixed-width column (f64/i64) → top-K heap → decode K rows.
        // 🔑 Only for non-GROUP-BY queries — GROUP BY + LIMIT applies LIMIT to
        // the grouped result, not to the pre-group rows.
        if stmt.order_by.is_some() && stmt.limit.is_some() && stmt.group_by.is_none() {
            let ob = stmt.order_by.as_ref().unwrap();
            if let Some(obe) = ob.first() {
                if let crate::sql::ast::Expr::Column(cn) = &obe.expr {
                    let order_col = schema.get_column_position(cn).unwrap_or(0);
                    let limit = stmt.limit.unwrap();
                    let out_pos: Vec<usize> =
                        Self::resolve_select_positions(&stmt.columns, &schema)
                            .unwrap_or_else(|| (0..col_types.len()).collect());
                    let is_float = matches!(col_types.get(order_col), Some(ColumnType::Float));
                    let desc = !obe.asc;
                    // Find top-K row indices by scanning ONLY the sort column.
                    let top_indices =
                        store.top_k_row_indices_typed(order_col, limit, desc, is_float);
                    let rows = store.decode_rows_at(&top_indices, &out_pos);
                    let columns = self
                        .build_select_columns(&stmt.columns, &schema)
                        .unwrap_or_default();
                    return Ok(Some(StreamingQueryResult::SelectReady { columns, rows }));
                }
            }
        }

        // Multi-aggregate without GROUP BY: COUNT/SUM/MIN/MAX via multi-segment scan.
        // Avoids sync compaction for these common analytical queries.
        if stmt.group_by.is_none() {
            if let Some(result) =
                self.col_segment_multi_aggregate(stmt, table_name, store, &schema)?
            {
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

    /// Parse a simple comparison WHERE clause of the form `col OP literal`
    /// into `(column_position, operator, literal_value)`.
    ///
    /// Supports all six comparison operators (=, !=, <, >, <=, >=). Returns
    /// None for compound expressions (AND/OR), non-comparison operators, or
    /// shapes where the left side isn't a column / right side isn't a literal.
    /// Callers fall back to the materialized path in those cases.
    ///
    /// This exists because two COUNT/aggregate fast paths previously only
    /// matched `=` (or worse, matched every BinaryOp but discarded the operator
    /// and always compared with equality — silently turning `id > 49000` into
    /// `id == 49000` and returning 0 rows).
    /// Column index keys truncate Text values to a 64-byte prefix (zero
    /// padded), so two distinct texts sharing that prefix are
    /// indistinguishable IN THE INDEX. A search value shorter than 64 bytes
    /// has an exact key (collisions would need an embedded NUL at a precise
    /// position in another value); longer texts MUST NOT be answered from the
    /// index alone — callers either verify the actual row value (row-fetch
    /// paths) or fall back to a scan (count/aggregate paths).
    pub(super) fn index_key_exact_for(value: &Value) -> bool {
        match value {
            Value::Text(s) => s.as_str().len() < 64,
            _ => true,
        }
    }

    pub(super) fn parse_simple_comparison_where(
        wc: &crate::sql::ast::Expr,
        schema: &TableSchema,
    ) -> Option<(usize, crate::sql::ast::BinaryOperator, Value)> {
        use crate::sql::ast::{BinaryOperator, Expr};

        /// Negate a Value (for parsing -N as a literal). Returns None if the
        /// value type can't be negated (only Integer/Float supported).
        fn negate_value(v: &Value) -> Option<Value> {
            match v {
                Value::Integer(i) => i.checked_neg().map(Value::Integer),
                Value::Float(f) => Some(Value::Float(-f)),
                _ => None,
            }
        }
        let (left, op, right) = match wc {
            Expr::BinaryOp { left, op, right } => (left, op, right),
            _ => return None,
        };
        // Only comparison operators (skip logical/arith operators).
        match op {
            BinaryOperator::Eq
            | BinaryOperator::Ne
            | BinaryOperator::Lt
            | BinaryOperator::Gt
            | BinaryOperator::Le
            | BinaryOperator::Ge => {}
            _ => return None,
        }
        let pos = match (left.as_ref(), right.as_ref()) {
            // col OP literal  (normal form)
            (Expr::Column(cn), Expr::Literal(v)) => (schema.get_column_position(cn)?, v.clone()),
            // col OP negative-literal  (e.g. WHERE v = -100 → UnaryOp(Minus, Literal(100)))
            (
                Expr::Column(cn),
                Expr::UnaryOp {
                    op: crate::sql::ast::UnaryOperator::Minus,
                    expr,
                },
            ) => {
                if let Expr::Literal(v) = expr.as_ref() {
                    let negated = negate_value(v)?;
                    (schema.get_column_position(cn)?, negated)
                } else {
                    return None;
                }
            }
            // literal OP col  (swapped operand form, e.g. 49000 < id)
            (Expr::Literal(v), Expr::Column(cn)) => {
                let p = schema.get_column_position(cn)?;
                // Flip the operator to match the swapped operands.
                let flipped = match op {
                    BinaryOperator::Eq => BinaryOperator::Eq,
                    BinaryOperator::Ne => BinaryOperator::Ne,
                    BinaryOperator::Lt => BinaryOperator::Gt,
                    BinaryOperator::Gt => BinaryOperator::Lt,
                    BinaryOperator::Le => BinaryOperator::Ge,
                    BinaryOperator::Ge => BinaryOperator::Le,
                    _ => return None,
                };
                return Some((p, flipped, v.clone()));
            }
            _ => return None,
        };
        Some((pos.0, op.clone(), pos.1))
    }

    /// Parse a WHERE clause into a flat list of (col_pos, op, target)
    /// comparisons. Handles:
    ///   - a single comparison:  `col OP lit`
    ///   - AND of comparisons:   `c1 OP1 lit1 AND c2 OP2 lit2 [AND ...]`
    ///
    /// Returns None (→ fall back to materialized path) for OR, nested logic,
    /// or any leaf that isn't a simple comparison. Only top-level AND chains
    /// are flattened; parentheses-free.
    pub(super) fn parse_where_comparisons(
        wc: &crate::sql::ast::Expr,
        schema: &TableSchema,
    ) -> Option<Vec<(usize, crate::sql::ast::BinaryOperator, Value)>> {
        use crate::sql::ast::{BinaryOperator, Expr};
        match wc {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                let mut out = Vec::new();
                // Recursively flatten left + right (handles N-term AND chains).
                out.extend(Self::parse_where_comparisons(left, schema)?);
                out.extend(Self::parse_where_comparisons(right, schema)?);
                Some(out)
            }
            Expr::Between {
                expr,
                negated: false,
                low,
                high,
            } => {
                // 🔑 BETWEEN 折叠为两个闭区间比较, 让融合聚合/下推路径
                // 接管 (资源测评: 单 BETWEEN 的 80% 范围聚合曾走物化路径
                // 88ms + 312MB, 融合路径 1.2ms + ~0)。NOT BETWEEN 是 OR
                // 语义, AND 链表达不了 → None 走通用路径。
                fn literal_or_neg(e: &Expr) -> Option<Value> {
                    match e {
                        Expr::Literal(v) => Some(v.clone()),
                        Expr::UnaryOp {
                            op: crate::sql::ast::UnaryOperator::Minus,
                            expr: inner,
                        } => match inner.as_ref() {
                            Expr::Literal(Value::Integer(i)) => {
                                i.checked_neg().map(Value::Integer)
                            }
                            Expr::Literal(Value::Float(f)) => Some(Value::Float(-f)),
                            _ => None,
                        },
                        _ => None,
                    }
                }
                let (cn, lv, hv) = match expr.as_ref() {
                    Expr::Column(cn) => (cn, literal_or_neg(low)?, literal_or_neg(high)?),
                    _ => return None,
                };
                if matches!(lv, Value::Null) || matches!(hv, Value::Null) {
                    return None;
                }
                let col = schema.get_column_position(cn)?;
                Some(vec![
                    (col, BinaryOperator::Ge, lv),
                    (col, BinaryOperator::Le, hv),
                ])
            }
            _ => {
                // Leaf: must be a single comparison.
                let one = Self::parse_simple_comparison_where(wc, schema)?;
                Some(vec![one])
            }
        }
    }

    /// Build a row predicate closure from a comparison operator + target value.
    /// The closure receives `Option<&Value>` (the filter column's value, None =
    /// NULL) and applies the operator, treating NULLs as non-matching.
    pub(super) fn build_comparison_predicate(
        op: crate::sql::ast::BinaryOperator,
        target: Value,
    ) -> Box<dyn Fn(Option<&Value>) -> bool> {
        use crate::sql::ast::BinaryOperator;
        // 🔑 SQL 三值逻辑: 与 NULL 的任何比较 (= <> < > <= >=) 都是
        // UNKNOWN → 不匹配。Value::partial_cmp 的 NULL-最小全序是
        // ORDER BY 语义，直接用于过滤会让 `grp > NULL` 匹配全部行、
        // `v < 100` 匹配 NULL 值行 (differential fuzz: MIN(cat) WHERE
        // grp > NULL 返回 '' 而非 NULL)。
        if matches!(target, Value::Null) {
            return Box::new(|_| false);
        }
        fn non_null<'a>(fv: Option<&'a Value>) -> Option<&'a Value> {
            fv.filter(|v| !matches!(v, Value::Null))
        }
        match op {
            BinaryOperator::Eq => Box::new(move |fv: Option<&Value>| non_null(fv) == Some(&target)),
            BinaryOperator::Ne => {
                // NULL != target is NULL (not true), so NULLs don't match.
                Box::new(move |fv: Option<&Value>| match non_null(fv) {
                    Some(v) => v != &target,
                    None => false,
                })
            }
            BinaryOperator::Lt => Box::new(move |fv: Option<&Value>| match non_null(fv) {
                Some(v) => v.partial_cmp(&target) == Some(std::cmp::Ordering::Less),
                None => false,
            }),
            BinaryOperator::Gt => Box::new(move |fv: Option<&Value>| match non_null(fv) {
                Some(v) => v.partial_cmp(&target) == Some(std::cmp::Ordering::Greater),
                None => false,
            }),
            BinaryOperator::Le => Box::new(move |fv: Option<&Value>| match non_null(fv) {
                Some(v) => matches!(
                    v.partial_cmp(&target),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                ),
                None => false,
            }),
            BinaryOperator::Ge => Box::new(move |fv: Option<&Value>| match non_null(fv) {
                Some(v) => matches!(
                    v.partial_cmp(&target),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                ),
                None => false,
            }),
            // Any other operator: never match (defensive — shouldn't happen).
            _ => Box::new(|_| false),
        }
    }

    /// Multi-aggregate (COUNT/SUM/MIN/MAX) without GROUP BY, via multi-segment scan.
    /// Avoids sync compaction.
    pub(super) fn col_segment_multi_aggregate(
        &self,
        stmt: &SelectStmt,
        _table_name: &str,
        store: &crate::storage::col_segment::ColSegmentStore,
        schema: &TableSchema,
    ) -> Result<Option<StreamingQueryResult>> {
        use crate::sql::ast::{Expr, SelectColumn};
        // 🆕 HAVING requires post-aggregation filtering that this pushdown path
        // doesn't apply — fall back to the materialized path.
        if stmt.having.is_some() {
            return Ok(None);
        }
        // 🚨 Subqueries in WHERE need per-row execution (see
        // col_segment_aggregate) — fall back.
        if stmt
            .where_clause
            .as_ref()
            .is_some_and(Self::expr_contains_subquery)
        {
            return Ok(None);
        }
        // COUNT(DISTINCT col) and other DISTINCT aggregates are not supported
        // here (this path counts without dedup). Fall back to the materialized
        // path which dedups via HashSet (compute_aggregate_positional).
        let has_distinct = stmt.columns.iter().any(|c| {
            matches!(
                c,
                SelectColumn::Expr(Expr::FunctionCall { distinct: true, .. }, _)
            )
        });
        if has_distinct {
            return Ok(None);
        }
        // Ensure buffered rows are durable — scan_projected_filtered only reads
        // persisted segments. (Subquery resolution calls execute_select_internal
        // directly, bypassing the streaming entry's flush.)
        let _ = store.flush_buffer();
        // Identify aggregate functions and their target columns.
        struct AggInfo {
            func: String,
            col: Option<usize>,
        }
        let mut aggs: Vec<AggInfo> = Vec::new();
        for col in &stmt.columns {
            if let SelectColumn::Expr(Expr::FunctionCall { name, args, .. }, _) = col {
                let func_upper = name.to_uppercase();
                // 🚨 Correctness guard: this fast path only handles bare-column
                // aggregate args (`SUM(col)`, `COUNT(col)`) or no-arg forms
                // (`COUNT(*)`). A compound arg like `SUM(CASE WHEN ... END)` or
                // `SUM(a + b)` has no resolvable column position — pushing it
                // through here silently produces NULL (the accumulator never
                // adds anything). Bail so the materialized path evaluates the
                // expression per row.
                let is_no_arg = args.is_empty()
                    || (args.len() == 1 && matches!(args[0], Expr::Column(ref c) if c == "*"));
                let arg_is_bare_col = args.len() == 1 && matches!(args[0], Expr::Column(_));
                if !is_no_arg && !arg_is_bare_col {
                    return Ok(None);
                }
                let target = args
                    .iter()
                    .filter_map(|a| {
                        if let Expr::Column(cn) = a {
                            // Strip table prefix for qualified names
                            let bare = if cn.contains('.') {
                                cn.rsplit('.').next().unwrap_or(cn)
                            } else {
                                cn
                            };
                            schema.get_column_position(bare)
                        } else {
                            None
                        }
                    })
                    .next();
                // For value aggregates (SUM/AVG/MIN/MAX/STDDEV/VARIANCE) the
                // arg MUST resolve to a column. If it doesn't, bail.
                if matches!(
                    func_upper.as_str(),
                    "SUM" | "AVG" | "MIN" | "MAX" | "STDDEV" | "VARIANCE"
                ) && target.is_none()
                {
                    return Ok(None);
                }
                aggs.push(AggInfo {
                    func: func_upper,
                    col: target,
                });
            } else {
                return Ok(None); // non-aggregate column in SELECT — can't handle
            }
        }
        if aggs.is_empty() {
            return Ok(None);
        }

        // 🚀 Fast path: COUNT + SUM/MIN/MAX WHERE text_col = 'val' — direct column
        // scan without Vec<Value> construction. Avoids 100K allocations + 30MB memory.
        if let Some(Expr::BinaryOp {
            left,
            op: crate::sql::ast::BinaryOperator::Eq,
            right,
        }) = &stmt.where_clause
        {
            if let (Expr::Column(cn), Expr::Literal(Value::Text(s))) =
                (left.as_ref(), right.as_ref())
            {
                if let Some(fc) = schema.get_column_position(cn) {
                    if matches!(schema.col_types().get(fc), Some(ColumnType::Text)) {
                        // Find SUM/MIN/MAX target column.
                        let agg_target = aggs.iter().filter_map(|a| a.col).next();
                        let columns: Vec<String> = self
                            .build_select_columns(&stmt.columns, schema)
                            .unwrap_or_default();
                        if let Some(ac) = agg_target {
                            let has_count = aggs.iter().any(|a| a.func == "COUNT");
                            let has_sum = aggs.iter().any(|a| a.func == "SUM");
                            let has_min = aggs.iter().any(|a| a.func == "MIN");
                            let has_max = aggs.iter().any(|a| a.func == "MAX");
                            if has_count && (has_sum || has_min || has_max) && aggs.len() <= 4 {
                                // 🔑 Single-pass: count_sum_min_max does COUNT+SUM+MIN+MAX
                                // in one scan. Was previously two separate scans
                                // (count_min_max_text_filter + count_sum_text_filter).
                                // Pass the agg column type so the store reads the
                                // column with the correct decoder (i64 vs f64) —
                                // previously it always used get_f64, reinterpreting
                                // integer bytes as a garbage float (data corruption).
                                let agg_type = schema
                                    .col_types()
                                    .get(ac)
                                    .cloned()
                                    .unwrap_or(ColumnType::Float);
                                let stats = store.count_sum_min_max_text_filter(
                                    fc,
                                    s.as_str(),
                                    ac,
                                    agg_type,
                                );
                                let is_int = stats.is_int;
                                let empty = stats.count == 0;
                                let mut row: Vec<Value> = Vec::new();
                                for a in &aggs {
                                    match a.func.as_str() {
                                        // COUNT(*) counts matching rows;
                                        // COUNT(col) counts non-NULL values.
                                        "COUNT" => row.push(Value::Integer(
                                            if a.col.is_none() {
                                                stats.count + stats.null_count
                                            } else {
                                                stats.count
                                            },
                                        )),
                                        // 🔑 Empty set: SUM/MIN/MAX/AVG → NULL.
                                        // SUM of an empty set is NULL (per SQL).
                                        "SUM" => {
                                            if empty {
                                                row.push(Value::Null);
                                            } else if is_int {
                                                row.push(Value::Integer(stats.sum_i));
                                            } else {
                                                row.push(Value::Float(stats.sum_f.total()));
                                            }
                                        }
                                        "MIN" => {
                                            if empty {
                                                row.push(Value::Null);
                                            } else if is_int {
                                                row.push(Value::Integer(stats.min_i));
                                            } else {
                                                row.push(Value::Float(stats.min_f));
                                            }
                                        }
                                        "MAX" => {
                                            if empty {
                                                row.push(Value::Null);
                                            } else if is_int {
                                                row.push(Value::Integer(stats.max_i));
                                            } else {
                                                row.push(Value::Float(stats.max_f));
                                            }
                                        }
                                        "AVG" => {
                                            if empty {
                                                row.push(Value::Null);
                                            } else if is_int {
                                                row.push(Value::Float(
                                                    stats.sum_i as f64 / stats.count as f64,
                                                ));
                                            } else {
                                                row.push(Value::Float(
                                                    stats.sum_f.total() / stats.count as f64,
                                                ));
                                            }
                                        }
                                        _ => row.push(Value::Null),
                                    }
                                }
                                return Ok(Some(StreamingQueryResult::SelectReady {
                                    columns,
                                    rows: vec![row],
                                }));
                            }
                        }
                    }
                }
            }
        }

        // Parse WHERE into a list of (col_pos, op, target) comparisons.
        // Supports either a single comparison or an AND of comparisons
        // (2-term AND is the common `cat = 3 AND amount > 150` shape).
        // Any other shape (OR, nested, etc.) falls back to the materialized
        // path rather than risk a silent miscount.
        let comparisons: Vec<(usize, crate::sql::ast::BinaryOperator, Value)> =
            match stmt.where_clause.as_ref() {
                None => Vec::new(),
                Some(wc) => match Self::parse_where_comparisons(wc, schema) {
                    Some(cs) => cs,
                    None => return Ok(None), // unsupported shape — fall back
                },
            };

        // Choose the scan filter column (first comparison) and the remaining
        // comparisons to apply as a post-filter on the projected rows.
        let (filter_col, post_comparisons): (
            Option<usize>,
            Vec<(usize, crate::sql::ast::BinaryOperator, Value)>,
        ) = if comparisons.is_empty() {
            (None, Vec::new())
        } else {
            let (fc, _, _) = comparisons[0].clone();
            (Some(fc), comparisons[1..].to_vec())
        };

        // 🔑 PERF: single-pass aggregate fast path. When all aggregates operate
        // on the same column (or COUNT(*)) with at most a single-column WHERE,
        // use store.aggregate_filtered — folds SUM/AVG/MIN/MAX/COUNT in one scan
        // over raw column bytes, zero per-row Value allocation.
        //
        // 🚨 This path's AggregateResult only tracks int/float min/max/sum — it
        // cannot represent TEXT or other non-numeric values. A TEXT column
        // would silently return 0 (the default min_int). Skip the fast path
        // when the aggregate column is non-numeric so the materialized path
        // (compute_aggregate_positional) handles it correctly.
        {
            let agg_cols: Vec<Option<usize>> = aggs.iter().map(|a| a.col).collect();
            let single_agg_col = agg_cols.iter().filter_map(|&c| c).next();
            let all_same_col = agg_cols.iter().all(|&c| match (c, single_agg_col) {
                (None, _) => true,
                (Some(a), Some(b)) => a == b,
                (Some(_), None) => false,
            });
            // 🆕 TEXT/Boolean/etc. columns can't use this numeric fast path.
            let agg_col_is_numeric = single_agg_col
                .map(|c| {
                    matches!(
                        schema.col_types().get(c),
                        Some(ColumnType::Integer | ColumnType::Float | ColumnType::Timestamp)
                    )
                })
                .unwrap_or(true); // COUNT(*) has no agg col → numeric ok
            if all_same_col && post_comparisons.is_empty() && agg_col_is_numeric {
                if let Some(ac) = single_agg_col {
                    let (fcol, fop, ftarget) = match comparisons.first() {
                        Some((c, o, t)) => (Some(*c), o.clone(), t.clone()),
                        None => (None, crate::sql::ast::BinaryOperator::Eq, Value::Null),
                    };
                    let agg = store.aggregate_filtered(fcol, ac, &fop, &ftarget);
                    let columns: Vec<String> = self
                        .build_select_columns(&stmt.columns, schema)
                        .unwrap_or_default();
                    let mut row: Vec<Value> = Vec::with_capacity(aggs.len());
                    for a in &aggs {
                        match a.func.as_str() {
                            "COUNT" => {
                                // COUNT(*) counts all rows; COUNT(col) skips NULLs.
                                if a.col.is_none() {
                                    row.push(Value::Integer(agg.count + agg.null_count));
                                } else {
                                    row.push(Value::Integer(agg.count));
                                }
                            }
                            "SUM" => {
                                if agg.count == 0 {
                                    row.push(Value::Null);
                                } else if agg.has_float {
                                    row.push(Value::Float(
                                        agg.float_sum.total() + agg.int_sum as f64,
                                    ));
                                } else {
                                    row.push(Value::Integer(agg.int_sum));
                                }
                            }
                            "AVG" => {
                                if agg.count == 0 {
                                    row.push(Value::Null);
                                } else {
                                    let s = if agg.has_float {
                                        agg.float_sum.total() + agg.int_sum as f64
                                    } else {
                                        agg.int_sum as f64
                                    };
                                    row.push(Value::Float(s / agg.count as f64));
                                }
                            }
                            "MIN" => {
                                if agg.count == 0 {
                                    row.push(Value::Null);
                                } else if agg.has_float {
                                    row.push(Value::Float(agg.min_float));
                                } else {
                                    // 🔑 Preserve Timestamp type: a TIMESTAMP
                                    // column's MIN must return Value::Timestamp,
                                    // not Value::Integer (raw micros).
                                    let is_ts = matches!(
                                        schema.col_types().get(ac),
                                        Some(ColumnType::Timestamp)
                                    );
                                    if is_ts {
                                        row.push(Value::Timestamp(
                                            crate::types::Timestamp::from_micros(agg.min_int),
                                        ));
                                    } else {
                                        row.push(Value::Integer(agg.min_int));
                                    }
                                }
                            }
                            "MAX" => {
                                if agg.count == 0 {
                                    row.push(Value::Null);
                                } else if agg.has_float {
                                    row.push(Value::Float(agg.max_float));
                                } else {
                                    // 🔑 Preserve Timestamp type (see MIN above).
                                    let is_ts = matches!(
                                        schema.col_types().get(ac),
                                        Some(ColumnType::Timestamp)
                                    );
                                    if is_ts {
                                        row.push(Value::Timestamp(
                                            crate::types::Timestamp::from_micros(agg.max_int),
                                        ));
                                    } else {
                                        row.push(Value::Integer(agg.max_int));
                                    }
                                }
                            }
                            _ => return Ok(None),
                        }
                    }
                    return Ok(Some(StreamingQueryResult::SelectReady {
                        columns,
                        rows: vec![row],
                    }));
                }
            }
        }

        // 🚀 Fast path: no-WHERE multi-column aggregate over fixed columns.
        // Directly iterate raw i64/f64 byte slices per column — no Vec<Value>
        // materialization. For `SELECT SUM(qty), AVG(score), MIN(score), MAX(score),
        // COUNT(*) FROM t` this avoids 2M×5 Value allocations.
        if stmt.where_clause.is_none() {
            let all_fixed = aggs.iter().all(|a| match a.col {
                None => true, // COUNT(*)
                Some(c) => matches!(
                    schema.col_types().get(c),
                    Some(ColumnType::Integer | ColumnType::Float | ColumnType::Timestamp)
                ),
            });
            if all_fixed {
                // 🚨 Cross-segment duplicate versions (UPDATE/DELETE of an
                // already-segmented key leaves old + new versions in different
                // segments until compaction) must dedup newest-wins — this
                // raw per-segment loop only skips tombstones. SQLite
                // differential testing caught it as COUNT(a), COUNT(b)
                // overcounting (61 vs 40). Fall back to the materialized path
                // whenever overlap is possible.
                if store.may_have_duplicate_keys() {
                    return Ok(None);
                }
                let _ = store.flush_buffer();
                let segs = store.segments_snapshot();
                // Per-column accumulators.
                let mut counts: Vec<i64> = vec![0; aggs.len()];
                let mut sums: Vec<f64> = vec![0.0; aggs.len()];
                let mut mins: Vec<f64> = vec![f64::INFINITY; aggs.len()];
                let mut maxs: Vec<f64> = vec![f64::NEG_INFINITY; aggs.len()];
                let mut has_float: Vec<bool> = vec![false; aggs.len()];
                let is_float_col: Vec<bool> = aggs
                    .iter()
                    .map(|a| {
                        a.col
                            .map(|c| matches!(schema.col_types().get(c), Some(ColumnType::Float)))
                            .unwrap_or(false)
                    })
                    .collect();

                for seg in &segs {
                    let n = seg.sst.num_rows;
                    if n == 0 {
                        continue;
                    }
                    let has_deletions = seg.sst.row_map.has_any_deleted();

                    // 🚀 Column-major iteration: for each agg column, iterate
                    // its raw typed slice once (auto-vectorizable SIMD) instead
                    // of row-major get_i64(i) per row (which doesn't vectorize
                    // due to Option return + match overhead).
                    for (ai, agg) in aggs.iter().enumerate() {
                        if agg.func == "COUNT" && agg.col.is_none() {
                            // COUNT(*) — count non-deleted rows.
                            if has_deletions {
                                let mut c = 0i64;
                                for i in 0..n {
                                    if !seg.sst.row_map.is_deleted(i) {
                                        c += 1;
                                    }
                                }
                                counts[ai] += c;
                            } else {
                                counts[ai] += n as i64;
                            }
                            continue;
                        }

                        let Some(c) = agg.col else {
                            continue;
                        };
                        if c >= seg.sst.column_tags.len() || !seg.sst.column_tags[c].is_fixed() {
                            continue;
                        }
                        let Some(fs) = seg.read_fixed_cached(c) else {
                            continue;
                        };
                        let nulls = fs.null_bitmap_bytes();
                        let has_nulls = !nulls.is_empty() && fs.has_nulls();

                        if is_float_col[ai] {
                            let raw = fs.raw_f64_typed_slice();
                            let nvals = n.min(raw.len());
                            if !has_deletions && !has_nulls {
                                // 🚀 Fully unchecked SIMD loop — no branches.
                                for &v in raw.iter().take(nvals) {
                                    sums[ai] += v;
                                    if v < mins[ai] {
                                        mins[ai] = v;
                                    }
                                    if v > maxs[ai] {
                                        maxs[ai] = v;
                                    }
                                }
                                counts[ai] += nvals as i64;
                                has_float[ai] = true;
                            } else {
                                for (i, &v) in raw.iter().enumerate().take(nvals) {
                                    if has_deletions && seg.sst.row_map.is_deleted(i) {
                                        continue;
                                    }
                                    if has_nulls && (nulls[i / 8] >> (i % 8)) & 1 != 0 {
                                        continue;
                                    }
                                    sums[ai] += v;
                                    if v < mins[ai] {
                                        mins[ai] = v;
                                    }
                                    if v > maxs[ai] {
                                        maxs[ai] = v;
                                    }
                                    counts[ai] += 1;
                                    has_float[ai] = true;
                                }
                            }
                        } else {
                            let raw = fs.raw_i64_slice();
                            let nvals = n.min(raw.len());
                            if !has_deletions && !has_nulls {
                                // 🚀 Fully unchecked SIMD loop.
                                for &v in raw.iter().take(nvals) {
                                    sums[ai] += v as f64;
                                    let vf = v as f64;
                                    if vf < mins[ai] {
                                        mins[ai] = vf;
                                    }
                                    if vf > maxs[ai] {
                                        maxs[ai] = vf;
                                    }
                                }
                                counts[ai] += nvals as i64;
                            } else {
                                for (i, &v) in raw.iter().enumerate().take(nvals) {
                                    if has_deletions && seg.sst.row_map.is_deleted(i) {
                                        continue;
                                    }
                                    if has_nulls && (nulls[i / 8] >> (i % 8)) & 1 != 0 {
                                        continue;
                                    }
                                    sums[ai] += v as f64;
                                    let vf = v as f64;
                                    if vf < mins[ai] {
                                        mins[ai] = vf;
                                    }
                                    if vf > maxs[ai] {
                                        maxs[ai] = vf;
                                    }
                                    counts[ai] += 1;
                                }
                            }
                        }
                    }
                }

                let columns: Vec<String> = self
                    .build_select_columns(&stmt.columns, schema)
                    .unwrap_or_default();
                let mut row: Vec<Value> = Vec::with_capacity(aggs.len());
                for (ai, agg) in aggs.iter().enumerate() {
                    let cnt = counts[ai];
                    match agg.func.as_str() {
                        "COUNT" => row.push(Value::Integer(cnt)),
                        "SUM" => {
                            if cnt == 0 {
                                row.push(Value::Null);
                            } else if has_float[ai] {
                                row.push(Value::Float(sums[ai]));
                            } else {
                                row.push(Value::Integer(sums[ai] as i64));
                            }
                        }
                        "AVG" => {
                            if cnt == 0 {
                                row.push(Value::Null);
                            } else {
                                row.push(Value::Float(sums[ai] / cnt as f64));
                            }
                        }
                        "MIN" => {
                            if cnt == 0 {
                                row.push(Value::Null);
                            } else if has_float[ai] || is_float_col[ai] {
                                row.push(Value::Float(mins[ai]));
                            } else {
                                row.push(Value::Integer(mins[ai] as i64));
                            }
                        }
                        "MAX" => {
                            if cnt == 0 {
                                row.push(Value::Null);
                            } else if has_float[ai] || is_float_col[ai] {
                                row.push(Value::Float(maxs[ai]));
                            } else {
                                row.push(Value::Integer(maxs[ai] as i64));
                            }
                        }
                        _ => return Ok(None),
                    }
                }
                return Ok(Some(StreamingQueryResult::SelectReady {
                    columns,
                    rows: vec![row],
                }));
            }
        }

        // 🚀 Fused multi-predicate single-pass aggregate: when every predicate
        // column is a scalar type and every aggregate fits the numeric/raw-byte
        // accumulators, fold COUNT/SUM/AVG/MIN/MAX directly over column bytes
        // with zero per-row Value materialization. The path below this one
        // materializes every row passing the FIRST predicate as
        // Vec<(u64, Vec<Value>)> (String allocs for TEXT filters) before
        // post-filtering — on the 100K×384 competitor dataset that was
        // ~180MB retained and 5-25ms; this path is sub-millisecond and flat.
        {
            let scalar_pred = |pos: usize| {
                matches!(
                    schema.col_types().get(pos),
                    Some(
                        ColumnType::Integer
                            | ColumnType::Float
                            | ColumnType::Timestamp
                            | ColumnType::Text
                            | ColumnType::Boolean
                    )
                )
            };
            let numeric_agg = |pos: usize| {
                matches!(
                    schema.col_types().get(pos),
                    Some(
                        ColumnType::Integer
                            | ColumnType::Float
                            | ColumnType::Timestamp
                    )
                )
            };
            let preds_ok = comparisons.len() <= 8
                && comparisons.iter().all(|(c, _, _)| scalar_pred(*c));
            let aggs_ok = aggs.iter().all(|a| match (a.func.as_str(), a.col) {
                ("COUNT", None) => true,
                // COUNT(col): any scalar column (TEXT counts via the raw text
                // decoder's null bitmap; Bool via its 1-byte decoder).
                ("COUNT", Some(c)) => scalar_pred(c),
                ("SUM" | "AVG" | "MIN" | "MAX", Some(c)) => numeric_agg(c),
                _ => false,
            });
            if preds_ok && aggs_ok {
                // Distinct aggregate target columns (index-aligned with
                // res.per_col).
                let mut agg_cols: Vec<usize> = Vec::new();
                for a in &aggs {
                    if let Some(c) = a.col {
                        if !agg_cols.contains(&c) {
                            agg_cols.push(c);
                        }
                    }
                }
                let res = store.aggregate_multi_filtered(&comparisons, &agg_cols);
                let agg_res = |a: &AggInfo| -> Option<&crate::storage::col_segment::AggregateResult> {
                    a.col
                        .and_then(|c| agg_cols.iter().position(|&x| x == c))
                        .map(|i| &res.per_col[i])
                };
                let columns: Vec<String> = self
                    .build_select_columns(&stmt.columns, schema)
                    .unwrap_or_default();
                let mut row: Vec<Value> = Vec::with_capacity(aggs.len());
                for a in &aggs {
                    match a.func.as_str() {
                        "COUNT" => {
                            // COUNT(*) counts all matching rows; COUNT(col)
                            // counts non-NULL values.
                            let n = match a.col {
                                None => res.rows,
                                Some(_) => agg_res(a).map(|r| r.count).unwrap_or(0),
                            };
                            row.push(Value::Integer(n));
                        }
                        "SUM" => {
                            let r = agg_res(a).unwrap();
                            if r.count == 0 {
                                row.push(Value::Null);
                            } else if r.has_float {
                                row.push(Value::Float(r.float_sum.total()));
                            } else {
                                row.push(Value::Integer(r.int_sum));
                            }
                        }
                        "AVG" => {
                            let r = agg_res(a).unwrap();
                            if r.count == 0 {
                                row.push(Value::Null);
                            } else if r.has_float {
                                row.push(Value::Float(r.float_sum.total() / r.count as f64));
                            } else {
                                row.push(Value::Float(r.int_sum as f64 / r.count as f64));
                            }
                        }
                        "MIN" => {
                            let r = agg_res(a).unwrap();
                            if r.count == 0 {
                                row.push(Value::Null);
                            } else if r.has_float {
                                row.push(Value::Float(r.min_float));
                            } else {
                                row.push(Value::Integer(r.min_int));
                            }
                        }
                        "MAX" => {
                            let r = agg_res(a).unwrap();
                            if r.count == 0 {
                                row.push(Value::Null);
                            } else if r.has_float {
                                row.push(Value::Float(r.max_float));
                            } else {
                                row.push(Value::Integer(r.max_int));
                            }
                        }
                        _ => return Ok(None),
                    }
                }
                return Ok(Some(StreamingQueryResult::SelectReady {
                    columns,
                    rows: vec![row],
                }));
            }
        }

        let mut scan_cols: Vec<usize> = Vec::new();
        if let Some(fc) = filter_col {
            scan_cols.push(fc);
        }
        // Post-filter columns must be projected so we can evaluate them.
        for (pc, _, _) in &post_comparisons {
            if !scan_cols.contains(pc) {
                scan_cols.push(*pc);
            }
        }
        for a in &aggs {
            if let Some(c) = a.col {
                if !scan_cols.contains(&c) {
                    scan_cols.push(c);
                }
            }
        }

        // Build the scan predicate from the primary comparison. Honors the
        // actual operator (previously this discarded `op` and always compared
        // with equality, silently turning `id > 49000` into `id == 49000`).
        let pred: Box<dyn Fn(Option<&Value>) -> bool> = match filter_col {
            Some(_) => {
                let (_, op, target) = (
                    comparisons[0].0,
                    comparisons[0].1.clone(),
                    comparisons[0].2.clone(),
                );
                Self::build_comparison_predicate(op, target)
            }
            None => Box::new(|_| true),
        };

        let scanned_raw = store.scan_projected_filtered(filter_col, &scan_cols, &*pred);

        // Apply post-filter comparisons (AND of remaining predicates) on the
        // projected rows. Each post-comparison's column is looked up by its
        // position in scan_cols.
        let scanned: Vec<(u64, Vec<Value>)> = if post_comparisons.is_empty() {
            scanned_raw
        } else {
            // Pre-compute (scan_col_index, op, target) for each post-comparison.
            let post_resolved: Vec<(usize, crate::sql::ast::BinaryOperator, Value)> =
                post_comparisons
                    .into_iter()
                    .map(|(pc, op, target)| {
                        let idx = scan_cols.iter().position(|&s| s == pc).unwrap_or(0);
                        (idx, op, target)
                    })
                    .collect();
            scanned_raw
                .into_iter()
                .filter(|(_, row)| {
                    post_resolved.iter().all(|(idx, op, target)| {
                        let fv = row.get(*idx);
                        apply_op_value(op, fv, target)
                    })
                })
                .collect()
        };

        // Compute aggregates.
        let columns: Vec<String> = self
            .build_select_columns(&stmt.columns, schema)
            .unwrap_or_default();
        let mut result_row: Vec<Value> = Vec::with_capacity(aggs.len());
        for a in &aggs {
            let col_idx_in_scan = a.col.and_then(|c| scan_cols.iter().position(|&s| s == c));
            match a.func.as_str() {
                "COUNT" => {
                    // COUNT(*) counts all rows; COUNT(col) skips NULLs.
                    let n = match a.col {
                        None => scanned.len(), // COUNT(*)
                        Some(_) => scanned
                            .iter()
                            .filter(|(_, row)| {
                                col_idx_in_scan
                                    .and_then(|ci| row.get(ci))
                                    .map(|v| !matches!(v, Value::Null))
                                    .unwrap_or(false)
                            })
                            .count(),
                    };
                    result_row.push(Value::Integer(n as i64));
                }
                "SUM" => {
                    // SUM ignores NULLs; SUM over zero non-NULL values is NULL.
                    let non_null: Vec<&Value> = scanned
                        .iter()
                        .filter_map(|(_, row)| col_idx_in_scan.and_then(|ci| row.get(ci)))
                        .filter(|v| !matches!(v, Value::Null))
                        .collect();
                    if non_null.is_empty() {
                        result_row.push(Value::Null);
                    } else {
                        // Return Integer for all-integer columns (consistency), else Float.
                        // 🔑 Treat BOOLEAN as numeric (TRUE=1, FALSE=0) so
                        // SUM over a BOOLEAN column sums 1s/0s instead of
                        // silently dropping every Bool value (which yielded
                        // Float(-0.0) / wrong NULL for AVG/MIN/MAX).
                        let all_int = non_null
                            .iter()
                            .all(|v| matches!(v, Value::Integer(_) | Value::Bool(_)));
                        if all_int {
                            let s: i64 = non_null
                                .iter()
                                .filter_map(|v| {
                                    if let Value::Integer(i) = v {
                                        Some(*i)
                                    } else if let Value::Bool(b) = v {
                                        Some(if *b { 1 } else { 0 })
                                    } else {
                                        None
                                    }
                                })
                                .filter(|&v| v != i64::MIN)
                                .sum();
                            result_row.push(Value::Integer(s));
                        } else {
                            let s: f64 = non_null
                                .iter()
                                .filter_map(|v| {
                                    if let Value::Float(f) = v {
                                        Some(*f)
                                    } else if let Value::Integer(i) = v {
                                        Some(*i as f64)
                                    } else if let Value::Bool(b) = v {
                                        Some(if *b { 1.0 } else { 0.0 })
                                    } else {
                                        None
                                    }
                                })
                                .sum();
                            result_row.push(Value::Float(s));
                        }
                    }
                }
                "MIN" => {
                    // 🔑 Handle Integer columns too (was Float-only, so Integer
                    // MIN returned the INFINITY fold seed). Decode by value type.
                    // 🔑 BOOLEAN values are folded into the integer collection
                    // (TRUE=1, FALSE=0) so MIN/MAX over a BOOLEAN column works.
                    let ints: Vec<i64> = scanned
                        .iter()
                        .filter_map(|(_, row)| {
                            col_idx_in_scan
                                .and_then(|ci| row.get(ci))
                                .and_then(|v| {
                                    if let Value::Integer(i) = v {
                                        Some(*i)
                                    } else if let Value::Bool(b) = v {
                                        Some(if *b { 1 } else { 0 })
                                    } else {
                                        None
                                    }
                                })
                                .filter(|&v| v != i64::MIN) // MIN = NULL sentinel
                        })
                        .collect();
                    let floats: Vec<f64> = scanned
                        .iter()
                        .filter_map(|(_, row)| {
                            col_idx_in_scan.and_then(|ci| row.get(ci)).and_then(|v| {
                                if let Value::Float(f) = v {
                                    Some(*f)
                                } else {
                                    None
                                }
                            })
                        })
                        .filter(|v| !v.is_nan())
                        .collect();
                    // 🚨 Text MIN: was missing → returned NULL for TEXT columns.
                    let texts: Vec<String> = scanned
                        .iter()
                        .filter_map(|(_, row)| {
                            col_idx_in_scan.and_then(|ci| row.get(ci)).and_then(|v| {
                                if let Value::Text(t) = v {
                                    Some(t.as_str().to_string())
                                } else {
                                    None
                                }
                            })
                        })
                        .collect();
                    if !ints.is_empty() {
                        result_row.push(Value::Integer(*ints.iter().min().unwrap()));
                    } else if !floats.is_empty() {
                        result_row.push(Value::Float(
                            floats.iter().cloned().fold(f64::INFINITY, f64::min),
                        ));
                    } else if !texts.is_empty() {
                        // Alphabetical min (SQL standard for TEXT).
                        let min_text = texts.iter().min().cloned().unwrap();
                        result_row.push(Value::text(min_text));
                    } else {
                        result_row.push(Value::Null);
                    }
                }
                "MAX" => {
                    let ints: Vec<i64> = scanned
                        .iter()
                        .filter_map(|(_, row)| {
                            col_idx_in_scan
                                .and_then(|ci| row.get(ci))
                                .and_then(|v| {
                                    if let Value::Integer(i) = v {
                                        Some(*i)
                                    } else if let Value::Bool(b) = v {
                                        Some(if *b { 1 } else { 0 })
                                    } else {
                                        None
                                    }
                                })
                                .filter(|&v| v != i64::MIN)
                        })
                        .collect();
                    let floats: Vec<f64> = scanned
                        .iter()
                        .filter_map(|(_, row)| {
                            col_idx_in_scan.and_then(|ci| row.get(ci)).and_then(|v| {
                                if let Value::Float(f) = v {
                                    Some(*f)
                                } else {
                                    None
                                }
                            })
                        })
                        .filter(|v| !v.is_nan())
                        .collect();
                    // 🚨 Text MAX: was missing → returned NULL for TEXT columns.
                    let texts: Vec<String> = scanned
                        .iter()
                        .filter_map(|(_, row)| {
                            col_idx_in_scan.and_then(|ci| row.get(ci)).and_then(|v| {
                                if let Value::Text(t) = v {
                                    Some(t.as_str().to_string())
                                } else {
                                    None
                                }
                            })
                        })
                        .collect();
                    if !ints.is_empty() {
                        result_row.push(Value::Integer(*ints.iter().max().unwrap()));
                    } else if !floats.is_empty() {
                        result_row.push(Value::Float(
                            floats.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                        ));
                    } else if !texts.is_empty() {
                        // Alphabetical max (SQL standard for TEXT).
                        let max_text = texts.iter().max().cloned().unwrap();
                        result_row.push(Value::text(max_text));
                    } else {
                        result_row.push(Value::Null);
                    }
                }
                "AVG" => {
                    // AVG ignores NULLs; AVG over zero non-NULL values is NULL.
                    // 🔑 Treat BOOLEAN as numeric (TRUE=1, FALSE=0).
                    let nums: Vec<f64> = scanned
                        .iter()
                        .filter_map(|(_, row)| {
                            col_idx_in_scan.and_then(|ci| row.get(ci)).and_then(|v| {
                                if let Value::Float(f) = v {
                                    Some(*f)
                                } else if let Value::Integer(i) = v {
                                    Some(*i as f64)
                                } else if let Value::Bool(b) = v {
                                    Some(if *b { 1.0 } else { 0.0 })
                                } else {
                                    None
                                }
                            })
                        })
                        .collect();
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
        Ok(Some(StreamingQueryResult::SelectReady {
            columns,
            rows: vec![result_row],
        }))
    }

    pub(super) fn col_segment_group_by(
        &self,
        stmt: &SelectStmt,
        _table_name: &str,
        store: &crate::storage::col_segment::ColSegmentStore,
        schema: &TableSchema,
    ) -> Result<Option<StreamingQueryResult>> {
        use crate::sql::ast::SelectColumn;
        use std::collections::HashMap;

        // Only handle: GROUP BY single column + COUNT(*) [+ SUM/MIN/MAX on fixed col]
        let group_cols = match stmt.group_by.as_ref() {
            Some(g) if g.len() == 1 => &g[0],
            _ => return Ok(None),
        };
        let group_col_name = group_cols.as_str();
        let group_pos = match schema.get_column_position(group_col_name) {
            Some(p) => p,
            None => return Ok(None),
        };
        let col_types = schema.col_types();

        // 🔑 HAVING clause requires post-group filtering — this fast path
        // doesn't support HAVING, so fall through to the materialized path.
        if stmt.having.is_some() {
            return Ok(None);
        }
        // 🔑 WHERE clause: this fast path scans all rows without applying a
        // WHERE filter (it only handles the bare GROUP BY + aggregate case).
        // A WHERE that filters out all rows would incorrectly produce groups
        // with count > 0. Fall back to single_pass_group_by which applies WHERE.
        if stmt.where_clause.is_some() {
            return Ok(None);
        }
        // 🔑 ORDER BY / LIMIT on the grouped result: this path emits groups in
        // scan order without sorting or truncating. Fall back to the path that
        // applies ORDER BY (including on aggregates) and LIMIT correctly.
        if stmt.order_by.is_some() || stmt.limit.is_some() {
            return Ok(None);
        }
        // 🔑 DISTINCT aggregates (COUNT(DISTINCT col), SUM(DISTINCT col), ...)
        // require per-group dedup which this fast path doesn't implement — it
        // would silently count/sum all values without dedup (e.g.
        // COUNT(DISTINCT v) returned the non-distinct count). Fall back to the
        // materialized path (compute_aggregate_positional handles DISTINCT).
        let has_distinct_agg = stmt.columns.iter().any(|c| {
            matches!(
                c,
                SelectColumn::Expr(Expr::FunctionCall { distinct: true, .. }, _)
            )
        });
        if has_distinct_agg {
            return Ok(None);
        }

        // Must flush buffer so segments see all data
        let _ = store.flush_buffer();
        let segs = store.segments_snapshot();

        // TEXT column GROUP BY with COUNT(*) — zero-alloc using &str keys.
        if matches!(
            col_types.get(group_pos),
            Some(crate::types::ColumnType::Text)
        ) {
            // 🚀 Extended GROUP BY fast path: COUNT(*) + SUM/AVG/MIN/MAX on
            // fixed columns, grouped by a text column. Computes all aggregates
            // in a single pass using raw typed slices per agg column.
            let has_count_star = stmt.columns.iter().any(|c| {
                matches!(c, SelectColumn::Expr(
                    crate::sql::ast::Expr::FunctionCall { name, args, .. }, _
                ) if name.eq_ignore_ascii_case("COUNT")
                  && (args.is_empty() || (args.len() == 1 && matches!(args[0], crate::sql::ast::Expr::Column(ref cn) if cn == "*"))))
            });
            // Collect aggregate functions (besides COUNT) and their columns.
            // All must be on fixed-width columns for this path.
            struct GbAgg {
                func: String,
                col: Option<usize>,
            }
            let mut gb_aggs: Vec<GbAgg> = Vec::new();
            let mut all_fixed = has_count_star;
            for col in &stmt.columns {
                if let SelectColumn::Expr(
                    crate::sql::ast::Expr::FunctionCall { name, args, .. },
                    _,
                ) = col
                {
                    let fname = name.to_uppercase();
                    // 🔑 COUNT(*) (no column arg) is tracked separately via
                    // group_counts. But COUNT(col) needs a per-agg nn_count
                    // tracker to count non-NULL values — treat it like other
                    // aggs by adding it to gb_aggs.
                    if fname == "COUNT"
                        && (args.is_empty()
                            || (args.len() == 1
                                && matches!(args[0], crate::sql::ast::Expr::Column(ref cn) if cn == "*")))
                    {
                        continue;
                    }
                    let agg_col = args
                        .iter()
                        .filter_map(|a| {
                            if let crate::sql::ast::Expr::Column(cn) = a {
                                schema.get_column_position(cn)
                            } else {
                                None
                            }
                        })
                        .next();
                    // Check the agg column is fixed-width.
                    if let Some(c) = agg_col {
                        if !matches!(
                            col_types.get(c),
                            Some(ColumnType::Integer | ColumnType::Float | ColumnType::Timestamp)
                        ) {
                            all_fixed = false;
                            break;
                        }
                    }
                    gb_aggs.push(GbAgg {
                        func: fname,
                        col: agg_col,
                    });
                } else if !matches!(col, SelectColumn::Column(_)) {
                    // Non-column, non-function (e.g. expression) — can't handle.
                    all_fixed = false;
                    break;
                }
            }
            // 🔑 Use this fast path for COUNT(*) with optional SUM/AVG/MIN/MAX
            // on fixed columns. Uses a two-phase approach:
            // Phase 1: scan text column → assign group index per row (HashMap)
            // Phase 2: for each agg column, iterate raw typed slice and fold
            // into per-group accumulators (vectorizable inner loop).
            if !has_count_star || !all_fixed {
                return Ok(None);
            }
            // 🔑 STDDEV/VARIANCE need sum-of-squared-deviations — this fast path
            // only tracks sum/min/max. Fall back to compute_aggregate_positional.
            if gb_aggs
                .iter()
                .any(|a| matches!(a.func.as_str(), "STDDEV" | "VARIANCE"))
            {
                return Ok(None);
            }
            // 🔑 Use TextSegment::for_each_str which iterates raw &str without
            // per-row offset/slice overhead.
            // 🚀 Index-based accumulator: HashMap<&str, usize> → counts Vec<i64>.
            // Hot path is 1 hash lookup + i64 increment, no string comparison
            // against every existing key (old linear-scan was O(groups) per row).
            let mut group_keys: Vec<Box<str>> = Vec::with_capacity(16);
            let mut group_counts: Vec<i64> = Vec::with_capacity(16);
            // Per-group, per-agg accumulators.
            let n_aggs = gb_aggs.len();
            let mut group_sums: Vec<Vec<f64>> =
                (0..n_aggs).map(|_| Vec::with_capacity(16)).collect();
            let mut group_mins: Vec<Vec<f64>> =
                (0..n_aggs).map(|_| Vec::with_capacity(16)).collect();
            let mut group_maxs: Vec<Vec<f64>> =
                (0..n_aggs).map(|_| Vec::with_capacity(16)).collect();
            // 🔑 Per-group, per-agg non-NULL value count — needed for correct
            // COUNT(col) and AVG(col) (SQL: both ignore NULL values).
            let mut group_nn_counts: Vec<Vec<i64>> =
                (0..n_aggs).map(|_| Vec::with_capacity(16)).collect();
            let agg_is_float: Vec<bool> = gb_aggs
                .iter()
                .map(|a| {
                    a.col
                        .map(|c| matches!(col_types.get(c), Some(ColumnType::Float)))
                        .unwrap_or(false)
                })
                .collect();
            let mut key_index: HashMap<Box<str>, usize> = HashMap::with_capacity(16);
            let mut null_count: i64 = 0;
            // 🔑 Accumulators for the NULL-key group (rows where the GROUP BY
            // column itself is NULL). These rows are skipped by the main loop
            // (which only processes non-NULL text keys), so we track their
            // aggregates separately here.
            let mut null_sum: Vec<f64> = vec![0.0; n_aggs];
            let mut null_min: Vec<f64> = vec![f64::INFINITY; n_aggs];
            let mut null_max: Vec<f64> = vec![f64::NEG_INFINITY; n_aggs];
            let mut null_nn: Vec<i64> = vec![0; n_aggs];

            // 🔑 Dedup IS needed for GROUP BY when there are 2+ segments: a
            // DELETE creates a tombstone in a newer segment that must suppress
            // the live row in the older segment. Without dedup, the GROUP BY
            // double-counts deleted rows (they still appear in the old segment).
            // was hardcoded false — caused GROUP BY to count deleted rows.
            let need_dedup = segs.len() > 1 || store.may_have_duplicate_keys();
            let mut seen: std::collections::HashSet<u64> = if need_dedup {
                std::collections::HashSet::with_capacity(segs.iter().map(|s| s.sst.num_rows).sum())
            } else {
                std::collections::HashSet::new()
            };

            for seg in segs.iter().rev() {
                let n = seg.sst.num_rows;
                if group_pos >= seg.sst.column_tags.len() {
                    continue;
                }
                // 🔑 慢路径 dedup 用 row_map.key(i) — keys 未加载时它回退到
                // 栅栏键 (每 fence_interval≈2048 行一个), 每 2048 行被当成同
                // key, seen 集合把首行之外的行全部"去重"掉: 100K 行 GROUP BY
                // 只剩 50 行 32 组 (静默错果, 对拍抓出)。必须先 load_full_keys。
                if need_dedup {
                    let _ = seg.sst.load_full_keys();
                }
                let ftext = match seg.read_text_cached(group_pos) {
                    Some(t) => t,
                    None => continue,
                };
                let has_nulls = ftext.has_any_null();
                let has_deletions = seg.sst.row_map.has_any_deleted();

                if !has_nulls && !has_deletions && !need_dedup {
                    // 🚀 Two-phase with raw-byte text decode (avoids get_str_fast's
                    // 3× SegData::slice() overhead per row).
                    // Phase 1: text scan → group index per row via raw bytes.
                    // Phase 2: for each agg column, vectorized raw slice fold.
                    let off_bytes = ftext.offsets_bytes();
                    let str_bytes = ftext.strings_bytes();
                    // 🔑 u32: 组索引曾用 u16 — 超 65535 个不同组时静默截断错组。
                        let mut row_groups: Vec<u32> = Vec::with_capacity(n);
                    const LINEAR_THRESHOLD: usize = 16;
                    let mut use_hash = group_keys.len() >= LINEAR_THRESHOLD;
                    for i in 0..n {
                        // 🚀 Direct raw-byte offset decode (no get_str_fast overhead).
                        let ob = i * 4;
                        let start = u32::from_le_bytes([
                            off_bytes[ob],
                            off_bytes[ob + 1],
                            off_bytes[ob + 2],
                            off_bytes[ob + 3],
                        ]) as usize;
                        let end = u32::from_le_bytes([
                            off_bytes[ob + 4],
                            off_bytes[ob + 5],
                            off_bytes[ob + 6],
                            off_bytes[ob + 7],
                        ]) as usize;
                        // 🚨 Corruption guard (disk-corruption fuzzing): offsets
                        // come straight off disk — a flipped bit can produce
                        // start > end or end > len. Degrade to an empty key
                        // instead of panicking.
                        let key_bytes = str_bytes.get(start..end).unwrap_or(&[]);

                        let idx = if use_hash {
                            if let Some(&idx) =
                                key_index.get(std::str::from_utf8(key_bytes).unwrap_or(""))
                            {
                                idx
                            } else {
                                let boxed: Box<str> =
                                    std::str::from_utf8(key_bytes).unwrap_or("").into();
                                let idx = group_keys.len();
                                group_keys.push(boxed.clone());
                                group_counts.push(0);
                                for ai in 0..n_aggs {
                                    group_sums[ai].push(0.0);
                                    group_mins[ai].push(f64::INFINITY);
                                    group_maxs[ai].push(f64::NEG_INFINITY);
                                    group_nn_counts[ai].push(0);
                                }
                                key_index.insert(boxed, idx);
                                idx
                            }
                        } else {
                            let mut found: Option<usize> = None;
                            for (gi, k) in group_keys.iter().enumerate() {
                                if k.as_bytes() == key_bytes {
                                    found = Some(gi);
                                    break;
                                }
                            }
                            match found {
                                Some(idx) => idx,
                                None => {
                                    let idx = group_keys.len();
                                    group_keys
                                        .push(std::str::from_utf8(key_bytes).unwrap_or("").into());
                                    group_counts.push(0);
                                    for ai in 0..n_aggs {
                                        group_sums[ai].push(0.0);
                                        group_mins[ai].push(f64::INFINITY);
                                        group_maxs[ai].push(f64::NEG_INFINITY);
                                        group_nn_counts[ai].push(0);
                                    }
                                    if group_keys.len() >= LINEAR_THRESHOLD {
                                        use_hash = true;
                                        for (gi, k) in group_keys.iter().enumerate() {
                                            key_index.insert(k.clone(), gi);
                                        }
                                    }
                                    idx
                                }
                            }
                        };
                        group_counts[idx] += 1;
                        row_groups.push(idx as u32);
                    }
                    // Phase 2: vectorized agg fold per column.
                    for (ai, agg) in gb_aggs.iter().enumerate() {
                        let Some(c) = agg.col else {
                            continue;
                        };
                        if c >= seg.sst.column_tags.len() || !seg.sst.column_tags[c].is_fixed() {
                            continue;
                        }
                        let Some(fs) = seg.read_fixed_cached(c) else {
                            continue;
                        };
                        // 🔑 Always track min (cheap) even for SUM/AVG, because the
                        // output stage uses group_mins == INFINITY as the "all values
                        // were NULL" sentinel for SUM. Previously need_minmax=false
                        // skipped this, making every SUM wrongly return NULL.
                        let need_minmax = agg.func == "MIN" || agg.func == "MAX";
                        if agg_is_float[ai] {
                            let raw = fs.raw_f64_typed_slice();
                            if need_minmax {
                                for (i, &v) in raw.iter().enumerate().take(n) {
                                    if v.is_nan() {
                                        continue;
                                    } // NULL sentinel
                                    let gi = row_groups[i] as usize;
                                    group_sums[ai][gi] += v;
                                    group_nn_counts[ai][gi] += 1;
                                    if v < group_mins[ai][gi] {
                                        group_mins[ai][gi] = v;
                                    }
                                    if v > group_maxs[ai][gi] {
                                        group_maxs[ai][gi] = v;
                                    }
                                }
                            } else {
                                for (i, &v) in raw.iter().enumerate().take(n) {
                                    if v.is_nan() {
                                        continue;
                                    } // NULL sentinel
                                    let gi = row_groups[i] as usize;
                                    group_sums[ai][gi] += v;
                                    group_nn_counts[ai][gi] += 1;
                                    if v < group_mins[ai][gi] {
                                        group_mins[ai][gi] = v;
                                    }
                                }
                            }
                        } else {
                            let raw = fs.raw_i64_slice();
                            if need_minmax {
                                for (i, &v) in raw.iter().enumerate().take(n) {
                                    if v == i64::MIN {
                                        continue;
                                    } // NULL sentinel
                                    let gi = row_groups[i] as usize;
                                    let vf = v as f64;
                                    group_sums[ai][gi] += vf;
                                    group_nn_counts[ai][gi] += 1;
                                    if vf < group_mins[ai][gi] {
                                        group_mins[ai][gi] = vf;
                                    }
                                    if vf > group_maxs[ai][gi] {
                                        group_maxs[ai][gi] = vf;
                                    }
                                }
                            } else {
                                for (i, &v) in raw.iter().enumerate().take(n) {
                                    if v == i64::MIN {
                                        continue;
                                    } // NULL sentinel
                                    let gi = row_groups[i] as usize;
                                    let vf = v as f64;
                                    group_sums[ai][gi] += vf;
                                    group_nn_counts[ai][gi] += 1;
                                    if vf < group_mins[ai][gi] {
                                        group_mins[ai][gi] = vf;
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // Slow path: nulls/deletions present.
                    let agg_segs: Vec<Option<crate::storage::lsm::columnar::FixedSegment>> =
                        gb_aggs
                            .iter()
                            .map(|a| {
                                a.col.and_then(|c| {
                                    if c < seg.sst.column_tags.len()
                                        && seg.sst.column_tags[c].is_fixed()
                                    {
                                        seg.read_fixed_cached(c)
                                    } else {
                                        None
                                    }
                                })
                            })
                            .collect();
                    for i in 0..n {
                        if need_dedup {
                            let key = seg.sst.row_map.key(i);
                            if !seen.insert(key) {
                                continue;
                            }
                        }
                        if has_deletions && seg.sst.row_map.is_deleted(i) {
                            continue;
                        }
                        if has_nulls && ftext.is_null(i) {
                            // 🔑 NULL group key — accumulate into the null-group
                            // accumulators (not just count). Previously this only
                            // did null_count += 1 and skipped aggregation, so
                            // SUM/MIN/MAX/AVG over NULL-key rows returned NULL/0.
                            null_count += 1;
                            for (ai, _agg) in gb_aggs.iter().enumerate() {
                                if let Some(ref fs) = agg_segs[ai] {
                                    if agg_is_float[ai] {
                                        if let Some(v) = fs.get_f64(i) {
                                            null_sum[ai] += v;
                                            null_nn[ai] += 1;
                                            if v < null_min[ai] {
                                                null_min[ai] = v;
                                            }
                                            if v > null_max[ai] {
                                                null_max[ai] = v;
                                            }
                                        }
                                    } else if let Some(v) = fs.get_i64(i) {
                                        let vf = v as f64;
                                        null_sum[ai] += vf;
                                        null_nn[ai] += 1;
                                        if vf < null_min[ai] {
                                            null_min[ai] = vf;
                                        }
                                        if vf > null_max[ai] {
                                            null_max[ai] = vf;
                                        }
                                    }
                                }
                            }
                            continue;
                        }
                        let s = ftext.get_str_fast(i);
                        let idx = if let Some(&idx) = key_index.get(s) {
                            idx
                        } else {
                            let boxed: Box<str> = s.into();
                            let idx = group_keys.len();
                            group_keys.push(boxed.clone());
                            group_counts.push(0);
                            for ai in 0..n_aggs {
                                group_sums[ai].push(0.0);
                                group_mins[ai].push(f64::INFINITY);
                                group_maxs[ai].push(f64::NEG_INFINITY);
                                group_nn_counts[ai].push(0);
                            }
                            key_index.insert(boxed, idx);
                            idx
                        };
                        group_counts[idx] += 1;
                        for (ai, _agg) in gb_aggs.iter().enumerate() {
                            if let Some(ref fs) = agg_segs[ai] {
                                if agg_is_float[ai] {
                                    if let Some(v) = fs.get_f64(i) {
                                        group_sums[ai][idx] += v;
                                        group_nn_counts[ai][idx] += 1;
                                        if v < group_mins[ai][idx] {
                                            group_mins[ai][idx] = v;
                                        }
                                        if v > group_maxs[ai][idx] {
                                            group_maxs[ai][idx] = v;
                                        }
                                    }
                                } else if let Some(v) = fs.get_i64(i) {
                                    let vf = v as f64;
                                    group_sums[ai][idx] += vf;
                                    group_nn_counts[ai][idx] += 1;
                                    if vf < group_mins[ai][idx] {
                                        group_mins[ai][idx] = vf;
                                    }
                                    if vf > group_maxs[ai][idx] {
                                        group_maxs[ai][idx] = vf;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Build output — iterate SELECT columns to match output order.
            let columns: Vec<String> = stmt
                .columns
                .iter()
                .map(|c| match c {
                    SelectColumn::Column(name) => name.clone(),
                    SelectColumn::Expr(crate::sql::ast::Expr::FunctionCall { name, .. }, _) => {
                        name.as_str().to_string()
                    }
                    _ => "expr".to_string(),
                })
                .collect();
            let mut rows: Vec<Vec<Value>> = Vec::with_capacity(group_keys.len() + 1);
            for (gi, k) in group_keys.iter().enumerate() {
                let cnt = group_counts[gi];
                let key_val = Value::Text(k.as_ref().into());
                // 🔑 Build the row by iterating ALL SELECT columns in order.
                // Previously this hard-coded "group key first, then aggregates"
                // which produced NULL/wrong slots when the GROUP BY column was
                // not the first SELECT item (e.g. `SELECT COUNT(*), g ...`).
                let mut row: Vec<Value> = Vec::with_capacity(stmt.columns.len());
                for col in stmt.columns.iter() {
                    match col {
                        // The GROUP BY column itself (must match the grouped column).
                        SelectColumn::Column(_) => row.push(key_val.clone()),
                        SelectColumn::Expr(
                            crate::sql::ast::Expr::FunctionCall { name, args, .. },
                            _,
                        ) => {
                            let fname = name.to_uppercase();
                            // Find this agg in gb_aggs.
                            let agg_col = args
                                .iter()
                                .filter_map(|a| {
                                    if let crate::sql::ast::Expr::Column(cn) = a {
                                        schema.get_column_position(cn)
                                    } else {
                                        None
                                    }
                                })
                                .next();
                            let ai = gb_aggs
                                .iter()
                                .position(|a| a.func == fname && a.col == agg_col);
                            match fname.as_str() {
                                "COUNT" => {
                                    if agg_col.is_some() {
                                        // 🔑 COUNT(col): count non-NULL values of col.
                                        // If an explicit agg (SUM/MIN/MAX/AVG) on the
                                        // same col exists, use its nn_count. Otherwise
                                        // COUNT(col) is the only agg on this col — but
                                        // gb_aggs skips COUNT, so there's no tracker.
                                        // Fall back to scanning for a matching nn_count.
                                        let any_agg = gb_aggs
                                            .iter()
                                            .enumerate()
                                            .find(|(_, a)| a.col == agg_col);
                                        if let Some((ai2, _)) = any_agg {
                                            row.push(Value::Integer(group_nn_counts[ai2][gi]));
                                        } else {
                                            // No other agg on this col tracked nn_count.
                                            // COUNT(col) without a co-located agg is rare
                                            // in this fast path (which requires COUNT(*)).
                                            // Approximate with total row count minus the
                                            // NULL sentinel check via min tracker absence.
                                            row.push(Value::Integer(cnt));
                                        }
                                    } else {
                                        // COUNT(*) — total rows in group.
                                        row.push(Value::Integer(cnt));
                                    }
                                }
                                "SUM" => {
                                    if let Some(ai) = ai {
                                        // 🔑 If no non-NULL value was accumulated
                                        // (min still at INFINITY = initial), SUM is NULL.
                                        if group_mins[ai][gi] == f64::INFINITY {
                                            row.push(Value::Null);
                                        } else if agg_is_float[ai] {
                                            row.push(Value::Float(group_sums[ai][gi]));
                                        } else {
                                            row.push(Value::Integer(group_sums[ai][gi] as i64));
                                        }
                                    } else {
                                        row.push(Value::Null);
                                    }
                                }
                                "AVG" => {
                                    if let Some(ai) = ai {
                                        // 🔑 AVG = SUM(non-NULL) / COUNT(non-NULL).
                                        // SQL standard: AVG ignores NULL values.
                                        let nn = group_nn_counts[ai][gi];
                                        if nn > 0 {
                                            row.push(Value::Float(group_sums[ai][gi] / nn as f64));
                                        } else {
                                            row.push(Value::Null);
                                        }
                                    } else {
                                        row.push(Value::Null);
                                    }
                                }
                                "MIN" => {
                                    if let Some(ai) = ai {
                                        // 🔑 No non-NULL value → NULL (don't leak the
                                        // INFINITY initial sentinel as i64::MAX).
                                        if group_mins[ai][gi] == f64::INFINITY {
                                            row.push(Value::Null);
                                        } else if agg_is_float[ai] {
                                            row.push(Value::Float(group_mins[ai][gi]));
                                        } else {
                                            row.push(Value::Integer(group_mins[ai][gi] as i64));
                                        }
                                    } else {
                                        row.push(Value::Null);
                                    }
                                }
                                "MAX" => {
                                    if let Some(ai) = ai {
                                        // 🔑 No non-NULL value → NULL (don't leak the
                                        // NEG_INFINITY initial sentinel as i64::MIN).
                                        if group_mins[ai][gi] == f64::INFINITY {
                                            row.push(Value::Null);
                                        } else if agg_is_float[ai] {
                                            row.push(Value::Float(group_maxs[ai][gi]));
                                        } else {
                                            row.push(Value::Integer(group_maxs[ai][gi] as i64));
                                        }
                                    } else {
                                        row.push(Value::Null);
                                    }
                                }
                                _ => row.push(Value::Null),
                            }
                        }
                        _ => row.push(Value::Null),
                    }
                }
                rows.push(row);
            }
            if null_count > 0 {
                // 🔑 NULL group row: respect SELECT column order. The GROUP BY
                // column is NULL; aggregates use the null_sum/min/max/nn
                // accumulators populated when the main loop encountered NULL keys.
                let mut null_row: Vec<Value> = Vec::with_capacity(stmt.columns.len());
                for col in stmt.columns.iter() {
                    match col {
                        SelectColumn::Column(_) => null_row.push(Value::Null),
                        SelectColumn::Expr(
                            crate::sql::ast::Expr::FunctionCall { name, args, .. },
                            _,
                        ) => {
                            let fname = name.to_uppercase();
                            // Find the agg column + index for this function.
                            let agg_col = args
                                .iter()
                                .filter_map(|a| {
                                    if let crate::sql::ast::Expr::Column(cn) = a {
                                        schema.get_column_position(cn)
                                    } else {
                                        None
                                    }
                                })
                                .next();
                            let is_count_star = fname == "COUNT"
                                && (args.is_empty()
                                    || (args.len() == 1
                                        && matches!(args[0], crate::sql::ast::Expr::Column(ref cn) if cn == "*")));
                            if is_count_star {
                                null_row.push(Value::Integer(null_count));
                            } else {
                                let ai = gb_aggs
                                    .iter()
                                    .position(|a| a.func == fname && a.col == agg_col);
                                match fname.as_str() {
                                    "COUNT" => {
                                        // COUNT(col) over NULL-key group → non-NULL count.
                                        if let Some(ai) = ai {
                                            null_row.push(Value::Integer(null_nn[ai]));
                                        } else {
                                            null_row.push(Value::Integer(null_count));
                                        }
                                    }
                                    "SUM" => {
                                        if let Some(ai) = ai {
                                            if null_min[ai] == f64::INFINITY {
                                                null_row.push(Value::Null);
                                            } else if agg_is_float[ai] {
                                                null_row.push(Value::Float(null_sum[ai]));
                                            } else {
                                                null_row.push(Value::Integer(null_sum[ai] as i64));
                                            }
                                        } else {
                                            null_row.push(Value::Null);
                                        }
                                    }
                                    "AVG" => {
                                        if let Some(ai) = ai {
                                            if null_nn[ai] > 0 {
                                                null_row.push(Value::Float(
                                                    null_sum[ai] / null_nn[ai] as f64,
                                                ));
                                            } else {
                                                null_row.push(Value::Null);
                                            }
                                        } else {
                                            null_row.push(Value::Null);
                                        }
                                    }
                                    "MIN" => {
                                        if let Some(ai) = ai {
                                            if null_min[ai] == f64::INFINITY {
                                                null_row.push(Value::Null);
                                            } else if agg_is_float[ai] {
                                                null_row.push(Value::Float(null_min[ai]));
                                            } else {
                                                null_row.push(Value::Integer(null_min[ai] as i64));
                                            }
                                        } else {
                                            null_row.push(Value::Null);
                                        }
                                    }
                                    "MAX" => {
                                        if let Some(ai) = ai {
                                            if null_min[ai] == f64::INFINITY {
                                                null_row.push(Value::Null);
                                            } else if agg_is_float[ai] {
                                                null_row.push(Value::Float(null_max[ai]));
                                            } else {
                                                null_row.push(Value::Integer(null_max[ai] as i64));
                                            }
                                        } else {
                                            null_row.push(Value::Null);
                                        }
                                    }
                                    _ => null_row.push(Value::Null),
                                }
                            }
                        }
                        _ => null_row.push(Value::Null),
                    }
                }
                rows.push(null_row);
            }
            return Ok(Some(StreamingQueryResult::SelectReady { columns, rows }));
        }

        // Fall back for non-text columns
        Ok(None)
    }
}
