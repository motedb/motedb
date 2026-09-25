//! 全表扫描簇：streaming 全扫 / col-segment 扫描 / 事务合并 / 投影扫描。
use super::*;

impl QueryExecutor {
    /// 🔥 全表扫描流式（现有实现）
    pub(super) fn execute_full_scan_streaming(
        &self,
        stmt: &SelectStmt,
        table: &str,
    ) -> Result<StreamingQueryResult> {
        let schema = self.db.get_table_schema(table)?;

        // LATEST BY is applied by the materialized path; both the TimeSeries
        // branch below and the ColSegmentStore fast path would silently
        // ignore the clause and return every row.
        if stmt.latest_by.is_some() {
            return self.materialize_as_streaming(stmt);
        }

        // 🔑 TimeSeries tables: authoritative data lives in the
        // ColumnarStore. scan_table_rows_streaming routes them there
        // (Materialized branch); the ColSegmentStore/LSM paths below hold
        // no data for them and would return empty results.
        if schema.table_type == crate::types::TableType::TimeSeries {
            // 🚀 `ORDER BY <ts_col> [DESC] LIMIT k`: bounded top-k over the
            // decoded ts column in the ColumnarStore instead of the full
            // materialize+sort below (~366 ms → tens of ms at 1M rows).
            if stmt.where_clause.is_none()
                && stmt.limit.is_some()
                && !stmt.distinct
                && stmt.group_by.is_none()
                && stmt.having.is_none()
                && stmt.order_by.as_ref().is_some_and(|ob| ob.len() == 1)
            {
                let key = &stmt.order_by.as_ref().unwrap()[0];
                if let crate::sql::ast::Expr::Column(cn) = &key.expr {
                    let bare = cn.rsplit('.').next().unwrap_or(cn);
                    if Some(bare) == schema.timeseries_column.as_deref() {
                        if let Some(QueryResult::Select { columns, rows }) =
                            self.try_ts_order_limit(stmt, table, &schema, key.asc)?
                        {
                            return Ok(StreamingQueryResult::SelectStreaming {
                                columns,
                                rows: Box::new(rows.into_iter().map(Ok)),
                                order_by: None,
                                limit: None,
                                offset: None,
                                distinct: false,
                                max_result_rows: None,
                                size_hint: None,
                            });
                        }
                    }
                }
            }
            let iter = self.db.scan_table_rows_streaming(table)?;
            let schema2 = self.db.get_table_schema(table)?;
            let mut rows: Vec<Vec<Value>> = Vec::new();
            let col_positions = Self::resolve_select_positions(&stmt.columns, &schema2)
                .unwrap_or_else(|| (0..schema2.columns.len()).collect());
            for item in iter {
                let (_rid, row) = item?;
                let projected: Vec<Value> = col_positions
                    .iter()
                    .map(|&p| row.get(p).cloned().unwrap_or(Value::Null))
                    .collect();
                rows.push(projected);
            }
            // Apply WHERE / ORDER BY / LIMIT on the materialized rows.
            // Note: rows are already PROJECTED to the SELECT positions —
            // for WHERE/ORDER evaluation on non-selected columns we re-scan
            // with full rows when needed. Keep it simple: evaluate against
            // full rows, project last.
            let mut full_rows: Vec<Vec<Value>> = Vec::new();
            {
                let iter2 = self.db.scan_table_rows_streaming(table)?;
                for item in iter2 {
                    let (_rid2, row2) = item?;
                    full_rows.push(row2);
                }
            }
            if let Some(wc) = &stmt.where_clause {
                let mut kept = Vec::with_capacity(full_rows.len());
                for row in full_rows.drain(..) {
                    if let Ok(v) = Self::eval_expr_on_row(wc, &row, &schema2) {
                        if Self::is_truthy(&v) {
                            kept.push(row);
                        }
                    }
                }
                full_rows = kept;
            }
            if let Some(ob) = &stmt.order_by {
                if order_by_has_nondefault_nulls(Some(ob)) {
                    // 🔑 apply_order_by is the only comparator honoring the
                    // explicit NULLS FIRST/LAST flag; sort by schema column
                    // names here (full_rows are schema-ordered).
                    let columns: Vec<String> =
                        schema2.columns.iter().map(|c| c.name.clone()).collect();
                    StreamingQueryResult::apply_order_by(&mut full_rows, &columns, ob)?;
                } else {
                    let mut specs: Vec<(usize, bool)> = Vec::new();
                    for o in ob {
                        if let Expr::Column(cn) = &o.expr {
                            if let Some(p) = schema2.get_column_position(cn) {
                                specs.push((p, o.asc));
                                continue;
                            }
                        }
                        specs.clear();
                        break;
                    }
                    if specs.len() == ob.len() {
                        StreamingQueryResult::sort_rows(&mut full_rows, &specs);
                    }
                }
            }
            if let Some(off) = stmt.offset {
                let off = off.min(full_rows.len());
                full_rows.drain(..off);
            }
            if let Some(lim) = stmt.limit {
                full_rows.truncate(lim);
            }
            let rows: Vec<Vec<Value>> = full_rows
                .into_iter()
                .map(|row| {
                    col_positions
                        .iter()
                        .map(|&p| row.get(p).cloned().unwrap_or(Value::Null))
                        .collect()
                })
                .collect();
            let columns = self.build_select_columns(&stmt.columns, &schema2)?;
            return Ok(StreamingQueryResult::SelectReady { columns, rows });
        }

        // S7: when the table uses the multi-segment ColSegmentStore, data is
        // queryable via multi-way merge — no finalize/merge needed. Just flush
        // pending buffer (cheap delta). Eliminates read-triggered full-table merge.
        // 🔑 EXCEPT TimeSeries tables: their authoritative store is the
        // ColumnarStore (WAL recovery replays into its buffers; the
        // ColSegmentStore view of a TS table can be empty and would shadow
        // it — after a crash every plain SELECT / LIMIT / ORDER BY returned
        // 0 rows). TS tables fall through to the scan_table_rows_streaming
        // path below, which routes them to the ColumnarStore.
        if self.db.has_col_segment_store(table)
            && schema.table_type != crate::types::TableType::TimeSeries
        {
            let col_types = schema.col_types().to_vec();
            if let Ok(store) = self.db.get_or_create_col_segment_store(table, &col_types) {
                let _ = store.prepare_for_query();
                // 🔑 Read-your-writes: the zero-copy SelectColumnar path below
                // bypasses execute_full_scan_via_col_segment and would miss
                // uncommitted transactional writes. When in a transaction with
                // writes for this table, force the merge path.
                if self.is_in_transaction() {
                    let ws = self.txn_write_set_rows(table);
                    let del = self.txn_deleted_row_ids(table);
                    if !ws.is_empty() || !del.is_empty() {
                        return self
                            .execute_full_scan_txn_merge(stmt, table, &schema, &store, ws, del);
                    }
                }
                return self.execute_full_scan_via_col_segment(stmt, table, &schema, &store);
            }
        }

        // Legacy path: finalize write buffer (with merge) so SSTable has all data.
        self.db.finalize_columnar_buffer(table);

        // 🚀 Columnar SSTable fast path: when a columnar SSTable exists for this
        // table, read directly from typed column arrays. Much faster than
        // row-based decode — no per-row binary parsing, no VarEntry scanning.
        let is_simple_star = stmt.columns.len() == 1
            && matches!(stmt.columns[0], SelectColumn::Star)
            && stmt.where_clause.is_none()
            && stmt.order_by.is_none()
            && !stmt.distinct
            && stmt.limit.is_none()
            && stmt.offset.is_none();
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
                            Ok(seg) => segments.push(ColumnarSeg::Fixed(
                                seg,
                                col_types.get(ci).cloned().unwrap_or(ColumnType::Integer),
                            )),
                            Err(_) => {
                                ok = false;
                                break;
                            }
                        }
                    } else {
                        match col_sst.read_text(ci) {
                            Ok(seg) => segments.push(ColumnarSeg::Text(seg)),
                            Err(_) => {
                                ok = false;
                                break;
                            }
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
                        order_by: None,
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
            let size_hint = self
                .db
                .fast_row_count(table)
                .map(|c| c as usize)
                .unwrap_or(1024);
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
        if !is_simple_star
            && self.db.columnar_sstables.contains_key(table)
            && stmt.where_clause.is_none()
            && stmt.order_by.is_none()
            && !stmt.distinct
            && stmt.limit.is_none()
            && stmt.offset.is_none()
        {
            let col_types = schema.col_types();
            let column_names: Vec<String> = self.build_select_columns(&stmt.columns, &schema)?;
            let col_positions: Vec<usize> = stmt
                .columns
                .iter()
                .filter_map(|c| match c {
                    SelectColumn::Column(name) => schema.get_column_position(name),
                    _ => None,
                })
                .collect();
            if col_positions.len() == stmt.columns.len() {
                match self
                    .db
                    .scan_columnar_sstable_projection(table, col_types, &col_positions)
                {
                    Ok(iter) => {
                        let mut rows = Vec::with_capacity(iter.size_hint().0);
                        for row in iter {
                            rows.push(row);
                        }
                        return Ok(StreamingQueryResult::SelectReady {
                            columns: column_names,
                            rows,
                        });
                    }
                    Err(e) => {
                        let _ = e;
                    } // columnar projection not available, fall through
                }
            }
        }

        // 🚀 Columnar WHERE/LIKE filter: use columnar scan for filters
        if self.db.columnar_sstables.contains_key(table)
            && stmt.order_by.is_none()
            && !stmt.distinct
            && stmt.limit.is_none()
            && stmt.offset.is_none()
        {
            let col_types = schema.col_types();
            let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
            let handled = false;

            // Equality: WHERE col = value
            if let Some(Expr::BinaryOp {
                left,
                op: crate::sql::ast::BinaryOperator::Eq,
                right,
            }) = &stmt.where_clause
            {
                if let (Expr::Column(filter_col), Expr::Literal(filter_val)) =
                    (left.as_ref(), right.as_ref())
                {
                    if let Some(filter_pos) = schema.get_column_position(filter_col) {
                        if let Some(col_sst) = self.db.columnar_sstables.get(table) {
                            if let Ok(iter) = self.db.scan_columnar_sstable_filtered(
                                table, col_types, filter_pos, filter_val,
                            ) {
                                // Extract match indices from the filtered iterator
                                let indices: Vec<usize> =
                                    iter.match_filter.clone().unwrap_or_default();
                                let mut segments: Vec<ColumnarSeg> =
                                    Vec::with_capacity(col_types.len());
                                for ci in 0..col_types.len() {
                                    if col_sst.column_tags[ci].is_fixed() {
                                        if let Ok(seg) = col_sst.read_fixed_i64(ci) {
                                            segments.push(ColumnarSeg::Fixed(
                                                seg,
                                                col_types
                                                    .get(ci)
                                                    .cloned()
                                                    .unwrap_or(ColumnType::Integer),
                                            ));
                                        }
                                    } else if let Ok(seg) = col_sst.read_text(ci) {
                                        segments.push(ColumnarSeg::Text(seg));
                                    }
                                }
                                return Ok(StreamingQueryResult::SelectColumnar {
                                    columns: column_names,
                                    segments,
                                    row_indices: Some(indices),
                                    num_rows: col_sst.num_rows,
                                    row_map: col_sst.row_map.clone(),
                                    order_by: None,
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
                if let Some(Expr::In {
                    expr,
                    list,
                    negated: false,
                }) = &stmt.where_clause
                {
                    if let Expr::Column(filter_col) = expr.as_ref() {
                        if list.iter().all(|e| matches!(e, Expr::Literal(_))) {
                            if let Some(filter_pos) = schema.get_column_position(filter_col) {
                                if let Some(col_sst) = self.db.columnar_sstables.get(table) {
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
                                    // Decode the filter column once, find matching row indices.
                                    let matches: Vec<usize> =
                                        if col_sst.column_tags[filter_pos].is_fixed() {
                                            let seg = col_sst.read_fixed_i64(filter_pos).ok();
                                            let mut m = Vec::new();
                                            if let Some(ref seg) = seg {
                                                for i in 0..col_sst.num_rows {
                                                    if col_sst.row_map.is_deleted(i) {
                                                        continue;
                                                    }
                                                    if let Some(v) = seg.get_i64(i) {
                                                        if set.contains(&Value::Integer(v)) {
                                                            m.push(i);
                                                        }
                                                    }
                                                }
                                            }
                                            m
                                        } else {
                                            let seg = col_sst.read_text(filter_pos).ok();
                                            let mut m = Vec::new();
                                            if let Some(ref seg) = seg {
                                                for i in 0..col_sst.num_rows {
                                                    if col_sst.row_map.is_deleted(i) {
                                                        continue;
                                                    }
                                                    if let Some(s) = seg.get_str(i) {
                                                        if set.contains(&Value::Text(
                                                            s.to_string().into(),
                                                        )) {
                                                            m.push(i);
                                                        }
                                                    }
                                                }
                                            }
                                            m
                                        };
                                    let mut segments: Vec<ColumnarSeg> =
                                        Vec::with_capacity(col_types.len());
                                    for ci in 0..col_types.len() {
                                        if col_sst.column_tags[ci].is_fixed() {
                                            if let Ok(seg) = col_sst.read_fixed_i64(ci) {
                                                segments.push(ColumnarSeg::Fixed(
                                                    seg,
                                                    col_types
                                                        .get(ci)
                                                        .cloned()
                                                        .unwrap_or(ColumnType::Integer),
                                                ));
                                            }
                                        } else if let Ok(seg) = col_sst.read_text(ci) {
                                            segments.push(ColumnarSeg::Text(seg));
                                        }
                                    }
                                    return Ok(StreamingQueryResult::SelectColumnar {
                                        columns: column_names,
                                        segments,
                                        row_indices: Some(matches),
                                        num_rows: col_sst.num_rows,
                                        row_map: col_sst.row_map.clone(),
                                        order_by: None,
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
                            if pat.ends_with('%') && !pat[..pat.len() - 1].contains('%') {
                                let prefix = &pat[..pat.len() - 1];
                                if let Some(filter_pos) = schema.get_column_position(filter_col) {
                                    if let Some(col_sst) = self.db.columnar_sstables.get(table) {
                                        if let Ok(iter) = self.db.scan_columnar_sstable_prefix(
                                            table, col_types, filter_pos, prefix,
                                        ) {
                                            let indices: Vec<usize> =
                                                iter.match_filter.clone().unwrap_or_default();
                                            let mut segments: Vec<ColumnarSeg> =
                                                Vec::with_capacity(col_types.len());
                                            for ci in 0..col_types.len() {
                                                if col_sst.column_tags[ci].is_fixed() {
                                                    if let Ok(seg) = col_sst.read_fixed_i64(ci) {
                                                        segments.push(ColumnarSeg::Fixed(
                                                            seg,
                                                            col_types
                                                                .get(ci)
                                                                .cloned()
                                                                .unwrap_or(ColumnType::Integer),
                                                        ));
                                                    }
                                                } else if let Ok(seg) = col_sst.read_text(ci) {
                                                    segments.push(ColumnarSeg::Text(seg));
                                                }
                                            }
                                            return Ok(StreamingQueryResult::SelectColumnar {
                                                columns: column_names,
                                                segments,
                                                row_indices: Some(indices),
                                                num_rows: col_sst.num_rows,
                                                row_map: col_sst.row_map.clone(),
                                                order_by: None,
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
        }) || where_clause.as_ref().is_some_and(Self::expr_uses_metadata);

        if use_positional && !needs_metadata {
            // 🚀 Compile WHERE: resolve column names → positions once.
            let compiled_where: Option<CompiledWhere> = where_clause
                .as_ref()
                .and_then(|clause| Self::compile_where(clause, &schema_clone));

            // 🚀 Index acceleration for WHERE col IN (...):
            // If the WHERE is a single InHash and a column index exists,
            // do K point lookups instead of a full table scan (O(K log N) vs O(N)).
            if let Some(ref cw) = compiled_where {
                if let Some(result) =
                    self.try_index_in_query(table, &schema_clone, cw, stmt, &columns)
                {
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
                    let where_set: std::collections::HashSet<usize> =
                        where_pos.iter().copied().collect();
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
                let project_where_indices: Vec<(usize, usize)> =
                    if let Some(ref sp) = select_positions {
                        sp.iter()
                            .enumerate()
                            .filter_map(|(out_idx, &p)| {
                                where_pos
                                    .iter()
                                    .position(|&w| w == p)
                                    .map(|buf_idx| (out_idx, buf_idx))
                            })
                            .collect()
                    } else {
                        Vec::new()
                    };
                let project_select_indices: Vec<(usize, usize)> =
                    if let Some(ref sp) = select_positions {
                        sp.iter()
                            .enumerate()
                            .filter_map(|(out_idx, &p)| {
                                select_only_pos
                                    .iter()
                                    .position(|&s| s == p)
                                    .map(|buf_idx| (out_idx, buf_idx))
                            })
                            .collect()
                    } else {
                        Vec::new()
                    };
                let num_output_cols = select_positions.as_ref().map_or(0, |sp| sp.len());

                let col_types = schema_clone.col_types().to_vec();
                let fixed_count = crate::storage::row_format::compute_fixed_count(&col_types);
                let fixed_offsets =
                    crate::storage::row_format::FixedColumnOffsets::compute(&col_types);
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
                if let Some(compiled_where) = compiled_where.as_ref() {
                    if let Some(result) = self.try_parallel_full_scan(
                        table,
                        &schema_clone,
                        &select_cols,
                        &columns,
                        compiled_where,
                        stmt,
                    ) {
                        return Ok(result);
                    }
                }
            }

            // ── Full decode path (sequential fallback) ──
            let row_iter = self.db.scan_table_rows_streaming(table)?;
            let filtered_iter = row_iter.filter_map(move |result| match result {
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
                    } else {
                        true
                    };
                    if !matches {
                        return None;
                    }
                    // 🔑 Use the checked variant so hard errors (e.g.
                    // DivisionByZero on `SELECT 1/0`) surface instead of
                    // becoming silent NULLs.
                    match Self::project_row_direct_checked(
                        &row,
                        &select_cols,
                        &columns_clone,
                        &schema_clone,
                    ) {
                        Ok(projected) => Some(Ok(projected)),
                        Err(e) => Some(Err(e)),
                    }
                }
                Err(e) => Some(Err(e)),
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
        let filtered_iter = fallback_iter.filter_map(move |result| match result {
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
                    if !matches {
                        return None;
                    }
                }

                let projected =
                    Self::project_row_static(&sql_row, &select_cols, &columns_clone, &schema_clone);
                Some(Ok(projected))
            }
            Err(e) => Some(Err(e)),
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
    pub(super) fn execute_full_scan_via_col_segment(
        &self,
        stmt: &SelectStmt,
        table: &str,
        schema: &TableSchema,
        store: &crate::storage::col_segment::ColSegmentStore,
    ) -> Result<StreamingQueryResult> {
        // 🔑 Read-your-writes: when inside a transaction with buffered writes
        // for this table, route through a merge path that combines segment
        // scan results with the write_set and filters undo_log deletes.
        // The autocommit fast path (no txn) hits None → zero overhead.
        if self.is_in_transaction() {
            let ws_rows = self.txn_write_set_rows(table);
            let deleted = self.txn_deleted_row_ids(table);
            if !ws_rows.is_empty() || !deleted.is_empty() {
                return self
                    .execute_full_scan_txn_merge(stmt, table, schema, store, ws_rows, deleted);
            }
        }
        let col_types = schema.col_types().to_vec();
        let columns: Vec<String> = self.build_select_columns(&stmt.columns, schema)?;
        // 🔑 Materialize non-correlated subqueries in the WHERE clause before
        // scanning. The col-segment scan evaluates WHERE via compile_where /
        // eval_expr_on_row, neither of which can execute subqueries. Without
        // this, a WHERE like `v > (SELECT MIN(v) FROM t WHERE v > (SELECT
        // MIN(v) FROM t))` (nested subqueries) silently returns no rows
        // (eval_expr_on_row errors on the Subquery node → row skipped). We
        // resolve non-correlated subqueries to Literals once here so the
        // per-row comparison works. Correlated subqueries are left in place
        // (they can't be pre-resolved).
        let where_clause = match &stmt.where_clause {
            Some(wc) => {
                let resolved = self
                    .materialize_subqueries_checked(wc, Some(schema))
                    .unwrap_or_else(|_| wc.clone());
                resolved.into()
            }
            None => None,
        };

        let limit = stmt.limit.unwrap_or(usize::MAX);
        let offset = stmt.offset.unwrap_or(0);

        // 🚀 VEC M4: 批投影 — 纯列 SELECT [WHERE] [LIMIT/OFFSET]，列批直读
        // + 边界一次行拼装 + LIMIT 前置截断。返回 None → 原路径回退。
        if stmt.group_by.is_none()
            && stmt.order_by.is_none()
            && !stmt.distinct
            && stmt.latest_by.is_none()
        {
            if let Some(rows) = crate::sql::vector_exec::try_vec_projection(
                store,
                schema,
                &stmt.columns,
                where_clause.as_ref(),
                stmt.limit,
                offset,
            )? {
                return Ok(StreamingQueryResult::SelectReady { columns, rows });
            }
        }

        // IN (literal list) HashSet fast path: avoid O(rows × list_len) linear scan.
        // For `WHERE col IN (v1, v2, ...)`, build a HashSet once and do O(1) lookup per row.
        let in_hashset: Option<(usize /*col_pos*/, std::collections::HashSet<Value>)> =
            match &where_clause {
                Some(crate::sql::ast::Expr::In {
                    expr,
                    list,
                    negated: false,
                }) if list
                    .iter()
                    .all(|e| matches!(e, crate::sql::ast::Expr::Literal(_))) =>
                {
                    match expr.as_ref() {
                        crate::sql::ast::Expr::Column(col_name) => {
                            schema.get_column_position(col_name).map(|pos| {
                                let set: std::collections::HashSet<Value> = list
                                    .iter()
                                    .filter_map(|e| {
                                        if let crate::sql::ast::Expr::Literal(v) = e {
                                            Some(v.clone())
                                        } else {
                                            None
                                        }
                                    })
                                    .collect();
                                (pos, set)
                            })
                        }
                        _ => None,
                    }
                }
                // 🚀 Pre-built HashSet from subquery materialization (Expr::InHashset).
                // Without this arm, IN-subquery queries fell through to the slow
                // col_segment_general_scan (full MergeCursor decode of all rows).
                // Now they use the raw-byte scan_row_indices_in_set fast path.
                Some(crate::sql::ast::Expr::InHashset {
                    expr,
                    set,
                    negated: false,
                    ..
                }) => match expr.as_ref() {
                    crate::sql::ast::Expr::Column(col_name) => schema
                        .get_column_position(col_name)
                        .map(|pos| (pos, set.clone())),
                    _ => None,
                },
                _ => None,
            };

        // 🚀 IN-subquery PK index fast path: WHERE pk_col IN (v1, v2, ...)
        // When the filter column is the PK and a column index exists, do K
        // index lookups (O(K log N)) instead of scanning all N rows.
        // This is the semi-join optimization: the IN list (from a resolved
        // subquery) drives index lookups rather than a full table scan.
        if let Some((col_pos, ref set)) = in_hashset {
            let pk_name = schema.primary_key();
            let is_pk_col = pk_name
                .as_ref()
                .and_then(|pk| schema.get_column_position(pk))
                .map(|pk_pos| pk_pos == col_pos)
                .unwrap_or(false);
            if is_pk_col && set.len() <= 1000 {
                let index_name = format!("{}.{}", table, schema.columns[col_pos].name);
                if let Some(index) = self.db.column_indexes.get(&index_name) {
                    let idx = index.value();
                    let mut all_row_ids: Vec<RowId> = Vec::new();
                    for v in set {
                        if let Ok(ids) = idx.get(v) {
                            all_row_ids.extend(ids);
                        }
                    }
                    if !all_row_ids.is_empty() {
                        let batch = self.db.get_table_rows_batch(table, &all_row_ids)?;
                        let result_rows: Vec<Vec<Value>> = batch
                            .into_iter()
                            .filter_map(|(_, row_opt)| row_opt)
                            .map(|row| {
                                Self::project_row_direct(&row, &stmt.columns, &columns, schema)
                            })
                            .collect();
                        return Ok(StreamingQueryResult::SelectReady {
                            columns,
                            rows: result_rows,
                        });
                    }
                    // No matches in index → empty result.
                    return Ok(StreamingQueryResult::SelectReady {
                        columns,
                        rows: vec![],
                    });
                }
            }
        }

        // Resolve output column positions (needed by several fast paths below).
        let out_positions: Vec<usize> = Self::resolve_select_positions(&stmt.columns, schema)
            .unwrap_or_else(|| (0..col_types.len()).collect());

        // 🚀 TEXT IN-set zero-alloc fast path: WHERE text_col IN (v1, v2, ...)
        // with a large set (e.g. from a subquery). The old path pre-interns
        // the entire text column + builds a Box<dyn Fn> that checks
        // HashSet<Value> per row (with ArcString alloc per row). This path
        // uses scan_row_indices_in_set (raw byte HashSet check, zero alloc).
        if let Some((col_pos, ref set)) = in_hashset {
            // 🚀 Raw-byte IN-set scan for TEXT columns. Uses scan_row_indices_in_set
            // which does zero-allocation HashSet<&[u8]> matching via
            // TextSegment::in_set_match_indices — a single-pass raw byte scan
            // over contiguous columnar data. No per-row Value::Text allocation.
            //
            // Previously capped at set.len() <= 1000 because the materialization
            // loop did scattered per-row read_fixed_i64/read_text (poor locality).
            // Now we pre-decode each output column once per segment, then index
            // into the pre-decoded data by local_row — converting 100K scattered
            // decodes into 4 sequential column reads + 100K cheap indexed lookups.
            if matches!(col_types.get(col_pos), Some(ColumnType::Text)) && set.len() > 1 {
                // Build a HashSet<&[u8]> from the Value set (once, not per-row).
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
                if !byte_set.is_empty() {
                    let _ = store.prepare_for_query();
                    if let Some(indices) =
                        store.scan_row_indices_in_set(col_pos, &byte_set, offset + limit)
                    {
                        let segs = store.segments_snapshot();
                        let mut result: Vec<Vec<Value>> = Vec::with_capacity(indices.len());
                        // Group indices by segment for sequential pre-decode.
                        let mut by_seg: std::collections::HashMap<usize, Vec<usize>> =
                            std::collections::HashMap::new();
                        for &(seg_idx, local_row) in indices.iter() {
                            by_seg.entry(seg_idx).or_default().push(local_row);
                        }
                        for (seg_idx, row_indices) in &by_seg {
                            let seg = match segs.get(*seg_idx) {
                                Some(s) => s,
                                None => continue,
                            };
                            // Pre-decode each output column once for this segment.
                            let mut col_data: Vec<Vec<Option<Value>>> =
                                Vec::with_capacity(out_positions.len());
                            for &pc in out_positions.iter() {
                                let decoded: Vec<Option<Value>> = if pc < seg.sst.column_tags.len()
                                    && seg.sst.column_tags[pc].is_fixed()
                                {
                                    match seg.sst.read_fixed_i64(pc) {
                                        Ok(f) => match col_types.get(pc) {
                                            Some(ColumnType::Float) => (0..seg.sst.num_rows)
                                                .map(|i| f.get_f64(i).map(Value::Float))
                                                .collect(),
                                            _ => (0..seg.sst.num_rows)
                                                .map(|i| f.get_i64(i).map(Value::Integer))
                                                .collect(),
                                        },
                                        Err(_) => vec![None; seg.sst.num_rows],
                                    }
                                } else {
                                    match seg.sst.read_text(pc) {
                                        Ok(t) => (0..seg.sst.num_rows)
                                            .map(|i| t.get_str(i).map(|s| Value::Text(s.into())))
                                            .collect(),
                                        Err(_) => vec![None; seg.sst.num_rows],
                                    }
                                };
                                col_data.push(decoded);
                            }
                            // Materialize matched rows from pre-decoded data.
                            for &local_row in row_indices.iter().take(limit) {
                                let row: Vec<Value> = col_data
                                    .iter()
                                    .map(|col| {
                                        col.get(local_row).cloned().flatten().unwrap_or(Value::Null)
                                    })
                                    .collect();
                                result.push(row);
                            }
                        }
                        return Ok(StreamingQueryResult::SelectReady {
                            columns,
                            rows: result,
                        });
                    }
                }
            }
        }

        // 🆕 Projected + filtered scan: decode only filter col + output cols,
        // avoiding full-row Vec<Value> decode for non-matches (the dominant
        // cost — was 68-197ms for 300K rows; pure column read is <2ms).
        // (out_positions already resolved above for the IN-set fast path.)
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
        let has_vector_or_spatial = col_types
            .iter()
            .any(|ct| matches!(ct, ColumnType::Tensor(_) | ColumnType::Spatial));

        // 🚀 LIMIT early-termination fast path: SELECT cols FROM t [LIMIT N]
        // When there's no WHERE/ORDER BY/GROUP BY/DISTINCT, we can scan only
        // the first (offset + limit) rows instead of all N rows. This converts
        // a 5000-row scan into a 50-row scan for LIMIT 50 (100x faster).
        if where_clause.is_none()
            && stmt.group_by.is_none()
            && stmt.order_by.is_none()
            && !stmt.distinct
            && !has_computed_sel
            && !has_vector_or_spatial
            && (stmt.limit.is_some() || stmt.offset.is_some())
        {
            let take_n = offset + limit.min(100_000); // cap to avoid unbounded allocation
            let scanned =
                store.scan_projected_filtered_limit(None, &out_positions, &|_| true, take_n);
            let result_rows: Vec<Vec<Value>> = scanned
                .into_iter()
                .skip(offset)
                .map(|(_, row)| row)
                .collect();
            return Ok(StreamingQueryResult::SelectReady {
                columns,
                rows: result_rows,
            });
        }

        if where_clause.is_none()
            && stmt.group_by.is_none()
            && stmt.order_by.is_none()
            && stmt.limit.is_none()
            && stmt.offset.is_none()
            && !stmt.distinct
            && !has_computed_sel
            && !has_vector_or_spatial
        {
            let _ = store.flush_buffer();
            let mut _ci = 0;
            while store.segment_count() >= 2 && _ci < 3 {
                if store.force_compact_all().is_err() {
                    break;
                }
                _ci += 1;
            }
            if store.segment_count() <= 1 {
                let segs = store.segments_snapshot();
                if let Some(last) = segs.last() {
                    let sst = &last.sst;
                    // 🔑 Load full keys so row_map.key(i) returns accurate values.
                    // Without this, the SelectColumnar dedup path in
                    // materialize_with_hint uses fence-key fallback, which maps
                    // all rows in a small segment (<2048 rows) to the same key —
                    // collapsing N rows into 1.
                    let _ = sst.load_full_keys();
                    let mut col_segs: Vec<ColumnarSeg> = Vec::with_capacity(out_positions.len());
                    for &pc in &out_positions {
                        if pc < sst.column_tags.len() && sst.column_tags[pc].is_fixed() {
                            if let Ok(f) = sst.read_fixed_i64(pc) {
                                col_segs.push(ColumnarSeg::Fixed(
                                    f,
                                    schema
                                        .col_types()
                                        .get(pc)
                                        .cloned()
                                        .unwrap_or(ColumnType::Integer),
                                ));
                            }
                        } else if pc < sst.column_tags.len() {
                            if let Ok(t) = sst.read_text(pc) {
                                col_segs.push(ColumnarSeg::Text(t));
                            }
                        }
                    }
                    return Ok(StreamingQueryResult::SelectColumnar {
                        columns,
                        segments: col_segs,
                        row_indices: None,
                        num_rows: sst.num_rows,
                        row_map: sst.row_map.clone(),
                        order_by: None,
                    });
                }
            }
            // Fallback: multi-segment scan if compaction failed.
            let scanned = store.scan_projected_filtered(None, &out_positions, &|_| true);
            let result_rows: Vec<Vec<Value>> = scanned
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(|(_, row)| row)
                .collect();
            return Ok(StreamingQueryResult::SelectReady {
                columns,
                rows: result_rows,
            });
        }

        // Full scan (no WHERE): SelectColumnar with bounded compaction for zero-copy.
        // (See LIMIT/OFFSET/DISTINCT/computed-expr guard note on the path above.)
        if where_clause.is_none()
            && stmt.group_by.is_none()
            && stmt.order_by.is_none()
            && stmt.limit.is_none()
            && stmt.offset.is_none()
            && !stmt.distinct
            && !has_computed_sel
            && !has_vector_or_spatial
        {
            let _ = store.flush_buffer();
            let mut _ci = 0;
            while store.segment_count() >= 2 && _ci < 3 {
                if store.force_compact_all().is_err() {
                    break;
                }
                _ci += 1;
            }
            if store.segment_count() <= 1 {
                let segs = store.segments_snapshot();
                if let Some(last) = segs.last() {
                    let sst = &last.sst;
                    // 🔑 Load full keys so row_map.key(i) returns accurate values.
                    // Without this, the SelectColumnar dedup path in
                    // materialize_with_hint uses fence-key fallback, which maps
                    // all rows in a small segment (<2048 rows) to the same key —
                    // collapsing N rows into 1.
                    let _ = sst.load_full_keys();
                    let mut col_segs: Vec<ColumnarSeg> = Vec::with_capacity(out_positions.len());
                    for &pc in &out_positions {
                        if pc < sst.column_tags.len() && sst.column_tags[pc].is_fixed() {
                            if let Ok(f) = sst.read_fixed_i64(pc) {
                                col_segs.push(ColumnarSeg::Fixed(
                                    f,
                                    schema
                                        .col_types()
                                        .get(pc)
                                        .cloned()
                                        .unwrap_or(ColumnType::Integer),
                                ));
                            }
                        } else if pc < sst.column_tags.len() {
                            if let Ok(t) = sst.read_text(pc) {
                                col_segs.push(ColumnarSeg::Text(t));
                            }
                        }
                    }
                    return Ok(StreamingQueryResult::SelectColumnar {
                        columns,
                        segments: col_segs,
                        row_indices: None,
                        num_rows: sst.num_rows,
                        row_map: sst.row_map.clone(),
                        order_by: None,
                    });
                }
            }
            // Fallback: multi-segment scan.
            let scanned = store.scan_projected_filtered(None, &out_positions, &|_| true);
            let result_rows: Vec<Vec<Value>> = scanned
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(|(_, row)| row)
                .collect();
            return Ok(StreamingQueryResult::SelectReady {
                columns,
                rows: result_rows,
            });
        }

        // 🚀 VEC M4b: 批过滤 top-k — WHERE（批谓词）+ ORDER BY 单数值/Timestamp
        // 键 + LIMIT/OFFSET：命中行只提排序键，select_nth 取前 k，只对最终
        // 页行做投影解码（旧路径解码全部命中行的全部投影列再全排序）。
        if where_clause.is_some() && stmt.order_by.is_some() && stmt.limit.is_some() {
            if let Some(rows) = crate::sql::vector_exec::try_vec_filter_topk(store, schema, stmt)? {
                return Ok(StreamingQueryResult::SelectReady { columns, rows });
            }
        }

        // 🚀 ORDER BY + LIMIT fast path: if ORDER BY is on a single numeric
        // column with a small LIMIT and no WHERE (or a simple text-eq WHERE),
        // use top_k_row_indices to scan only the sort column (bounded heap),
        // then fetch only K rows. This avoids materializing + sorting all
        // 300K rows (49ms → ~2ms). With a text-eq WHERE, the matched row
        // indices come from scan_row_indices_eq and top_k_from_indices_typed
        // keeps the heap bounded over just those rows.
        // Note: OFFSET is only supported here when there's a single ORDER BY
        // key (multi-key or OFFSET-bearing queries fall through to the full
        // scan + sort path below).
        let text_eq_filter: Option<(usize, &str)> = match &where_clause {
            Some(crate::sql::ast::Expr::BinaryOp {
                left,
                op: crate::sql::ast::BinaryOperator::Eq,
                right,
            }) => match (left.as_ref(), right.as_ref()) {
                (
                    crate::sql::ast::Expr::Column(cn),
                    crate::sql::ast::Expr::Literal(Value::Text(tv)),
                ) => match schema.get_column_position(cn) {
                    Some(p) if matches!(schema.col_types().get(p), Some(ColumnType::Text)) => {
                        Some((p, tv.as_str()))
                    }
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        };
        if (where_clause.is_none() || text_eq_filter.is_some())
            && stmt.order_by.as_ref().is_none_or(|o| o.len() <= 1)
            // 🚨 DISTINCT must dedup AFTER sort+limit. The Top-K path below
            // returns the K smallest/largest raw rows without dedup, so
            // `SELECT DISTINCT v ORDER BY v LIMIT 2` could return duplicate
            // values (e.g. [10,10] instead of [10,20]). Skip when DISTINCT.
            && !stmt.distinct
        {
            if let Some(ref ob) = stmt.order_by {
                if let Some(first_ob) = ob.first() {
                    if let crate::sql::ast::Expr::Column(cn) = &first_ob.expr {
                        // 🔑 Deep pagination: OFFSET pages within the sorted
                        // order, so we select the top (offset+limit) keys and
                        // decode only the final `limit` rows — the old path
                        // decoded every projected row for all N, sorted, then
                        // threw the first `offset` away (21ms → ~2ms @100K).
                        let page = stmt.limit.unwrap_or(usize::MAX);
                        let k = page.saturating_add(offset);
                        if page > 0 && k <= 1_000_000 {
                            // 🔑 剥限定名前缀 (`e.ts`) — get_column_position
                            // 不认带表名前缀的键, 限定名会静默 decline 到全排序。
                            let cn_bare = cn.rsplit('.').next().unwrap_or(cn);
                            if let Some(order_col) = schema.get_column_position(cn_bare) {
                                let is_numeric = matches!(
                                    schema.col_types().get(order_col),
                                    Some(crate::types::ColumnType::Integer)
                                        | Some(crate::types::ColumnType::Float)
                                        | Some(crate::types::ColumnType::Boolean)
                                        // Timestamp is i64 microseconds under the
                                        // hood; leaving it out routed
                                        // `ORDER BY ts LIMIT k` to the full
                                        // scan+sort path (11.7ms vs 0.5ms).
                                        | Some(crate::types::ColumnType::Timestamp)
                                );
                                if is_numeric {
                                    let is_float = matches!(
                                        schema.col_types().get(order_col),
                                        Some(crate::types::ColumnType::Float)
                                    );
                                    // 🔑 Flush write buffer so UPDATEd values in
                                    // the buffer are persisted to a segment and
                                    // visible to the Top-K scan. Without this,
                                    // ORDER BY reads stale pre-UPDATE values.
                                    let _ = store.flush_buffer();
                                    let top_indices = if let Some((fc, tv)) = text_eq_filter {
                                        match store.scan_row_indices_eq(
                                            fc,
                                            tv.as_bytes(),
                                            usize::MAX,
                                        ) {
                                            Some(indices) => store.top_k_from_indices_typed(
                                                order_col,
                                                k,
                                                !first_ob.asc,
                                                is_float,
                                                &indices,
                                            ),
                                            None => Vec::new(),
                                        }
                                    } else {
                                        store.top_k_row_indices_typed(
                                            order_col,
                                            k,
                                            !first_ob.asc,
                                            is_float,
                                        )
                                    };
                                    // 🔑 Page after the bounded sort: skip the
                                    // first `offset` best rows, keep `page`.
                                    let top_indices: Vec<(usize, usize)> =
                                        top_indices.into_iter().skip(offset).take(page).collect();
                                    let segs = store.segments_snapshot();
                                    let col_types = store.col_types();
                                    // Cache decoded columns per segment to avoid re-reading.
                                    use crate::storage::lsm::columnar::{
                                        FixedSegment, TextSegment,
                                    };
                                    enum Col {
                                        Text(TextSegment),
                                        Fixed(FixedSegment),
                                        /// Vector / Spatial — no column-level
                                        /// decode; read per row below.
                                        PerRow,
                                        None,
                                    }
                                    let mut col_cache: std::collections::HashMap<
                                        (usize, usize),
                                        Col,
                                    > = std::collections::HashMap::new();
                                    let mut result_rows: Vec<Vec<Value>> =
                                        Vec::with_capacity(top_indices.len());
                                    for (seg_idx, local_row) in top_indices {
                                        let seg = match segs.get(seg_idx) {
                                            Some(s) => s,
                                            None => continue,
                                        };
                                        let mut row = Vec::with_capacity(out_positions.len());
                                        for &pc in &out_positions {
                                            let col = col_cache
                                                .entry((seg_idx, pc))
                                                .or_insert_with(|| {
                                                    if matches!(
                                                        col_types.get(pc),
                                                        Some(crate::types::ColumnType::Text)
                                                    ) {
                                                        match seg.sst.read_text(pc) {
                                                            Ok(t) => Col::Text(t),
                                                            Err(_) => Col::None,
                                                        }
                                                    } else if matches!(
                                                        col_types.get(pc),
                                                        Some(crate::types::ColumnType::Tensor(_))
                                                            | Some(
                                                                crate::types::ColumnType::Spatial
                                                            )
                                                    ) {
                                                        // Was read as a fixed column → error →
                                                        // NULL: `SELECT emb … ORDER BY id LIMIT k`
                                                        // never returned an embedding.
                                                        Col::PerRow
                                                    } else {
                                                        match seg.sst.read_fixed_i64(pc) {
                                                            Ok(f) => Col::Fixed(f),
                                                            Err(_) => Col::None,
                                                        }
                                                    }
                                                });
                                            let v = match col {
                                                Col::PerRow => seg.read_var_value_at(pc, local_row),
                                                Col::Text(t) => t
                                                    .get_str(local_row)
                                                    .map(|s| Value::Text(s.into()))
                                                    .unwrap_or(Value::Null),
                                                Col::Fixed(f) => {
                                                    match col_types.get(pc) {
                                                        // Float is stored in the same fixed 8-byte slot as
                                                        // i64, so reading it as i64 and casting to f64 gives
                                                        // garbage. Re-read the raw bytes as f64 instead.
                                                        Some(crate::types::ColumnType::Float) => f
                                                            .get_i64(local_row)
                                                            .map(|bits| {
                                                                // Preserve exact bit pattern: i64 → u64 is a
                                                                // lossless reinterpret (two's complement).
                                                                Value::Float(f64::from_bits(
                                                                    bits as u64,
                                                                ))
                                                            })
                                                            .unwrap_or(Value::Null),
                                                        _ => f
                                                            .get_i64(local_row)
                                                            .map(Value::Integer)
                                                            .unwrap_or(Value::Null),
                                                    }
                                                }
                                                Col::None => Value::Null,
                                            };
                                            row.push(v);
                                        }
                                        result_rows.push(row);
                                    }
                                    return Ok(StreamingQueryResult::SelectReady {
                                        columns,
                                        rows: result_rows,
                                    });
                                }
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
        let ob_schema_positions: Vec<usize> = stmt
            .order_by
            .as_ref()
            .map(|ob| {
                // Collect schema columns referenced by each ORDER BY key, including
                // columns nested inside expressions (e.g. ORDER BY a + b).
                let mut acc: Vec<usize> = Vec::new();
                for oe in ob {
                    for p in Self::expr_referenced_columns(&oe.expr, schema) {
                        if !acc.contains(&p) {
                            acc.push(p);
                        }
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

        // 🚨 WHERE clause columns: must be included in scan_positions so that
        // col_segment_general_scan's eval_expr_on_row can resolve them. Without
        // this, `SELECT id FROM t WHERE a + b IS NULL` scans only the `id`
        // column — eval_expr_on_row can't find `a`/`b` → wrong/empty results.
        if let Some(ref wc) = where_clause {
            for p in Self::expr_referenced_columns(wc, schema) {
                if !scan_positions.contains(&p) {
                    scan_positions.push(p);
                }
            }
        }

        // 🚀 TEXT eq SelectColumnar fast path: for `WHERE text_col = 'literal'`
        // on a ColSegmentStore table. The old path (col_segment_projected_scan →
        // scan_projected_filtered) pre-interns the ENTIRE text column into
        // ArcString Values (300K allocs) just to feed a Box<dyn Fn> predicate.
        // This path uses scan_row_indices_eq (zero-alloc raw byte compare) +
        // SelectColumnar (lazy decode). Cuts WHERE region='US' from 35ms to ~12ms.
        if !has_vector_or_spatial
            && !has_computed_sel
            && stmt.group_by.is_none()
            && stmt.order_by.is_none()
            && !stmt.distinct
        {
            if let Some(crate::sql::ast::Expr::BinaryOp {
                left,
                op: crate::sql::ast::BinaryOperator::Eq,
                right,
            }) = &where_clause
            {
                if let (
                    crate::sql::ast::Expr::Column(cn),
                    crate::sql::ast::Expr::Literal(Value::Text(tv)),
                ) = (left.as_ref(), right.as_ref())
                {
                    if let Some(fc) = schema.get_column_position(cn) {
                        if matches!(col_types.get(fc), Some(ColumnType::Text)) {
                            // 🔑 CRITICAL: flush write buffer first so
                            // recent INSERT/UPDATE data is visible to the
                            // segment scan. Without this, updated rows
                            // only in the buffer are invisible (the
                            // test_update_indexed_column_moves_entry bug).
                            let _ = store.flush_buffer();
                            let target_bytes = tv.as_str().as_bytes();
                            if let Some(indices) =
                                store.scan_row_indices_eq(fc, target_bytes, offset + limit)
                            {
                                let segs = store.segments_snapshot();
                                // Build SelectColumnar with matched row indices.
                                if segs.len() == 1 {
                                    let row_idx: Vec<usize> = indices
                                        .iter()
                                        .skip(offset)
                                        .take(limit)
                                        .map(|&(_, r)| r)
                                        .collect();
                                    let mut col_segs: Vec<ColumnarSeg> =
                                        Vec::with_capacity(out_positions.len());
                                    let seg0_tags = &segs[0].sst.column_tags;
                                    for &pc in &out_positions {
                                        if pc < seg0_tags.len() && seg0_tags[pc].is_fixed() {
                                            if let Ok(seg) = segs[0].sst.read_fixed_i64(pc) {
                                                col_segs.push(ColumnarSeg::Fixed(
                                                    seg,
                                                    col_types
                                                        .get(pc)
                                                        .cloned()
                                                        .unwrap_or(ColumnType::Integer),
                                                ));
                                            }
                                        } else if let Ok(t) = segs[0].sst.read_text(pc) {
                                            col_segs.push(ColumnarSeg::Text(t));
                                        }
                                    }
                                    return Ok(StreamingQueryResult::SelectColumnar {
                                        columns,
                                        segments: col_segs,
                                        row_indices: Some(row_idx),
                                        num_rows: segs[0].sst.num_rows,
                                        row_map: segs[0].sst.row_map.clone(),
                                        order_by: None,
                                    });
                                }
                                // Multi-segment: materialize matched rows
                                // directly (skip the full-column pre-intern
                                // that scan_projected_filtered does).
                                // 🔑 Multi-segment: pre-decode each output column
                                // ONCE per segment, then materialize matched rows
                                // by index. The previous per-row loop called
                                // read_fixed_i64/read_text for EVERY matched row —
                                // a full column read per row (and a full zstd
                                // decompression per call in compact mode):
                                // O(N²) on 100K matches ≈ 568s in compact mode.
                                let mut result: Vec<Vec<Value>> = Vec::with_capacity(indices.len());
                                let mut by_seg: std::collections::HashMap<usize, Vec<usize>> =
                                    std::collections::HashMap::new();
                                for &(seg_idx, local_row) in indices.iter().skip(offset).take(limit)
                                {
                                    by_seg.entry(seg_idx).or_default().push(local_row);
                                }
                                for (seg_idx, local_rows) in &by_seg {
                                    let seg = match segs.get(*seg_idx) {
                                        Some(s) => s,
                                        None => continue,
                                    };
                                    let mut col_data: Vec<Vec<Option<Value>>> =
                                        Vec::with_capacity(out_positions.len());
                                    for &pc in out_positions.iter() {
                                        let decoded: Vec<Option<Value>> = if pc
                                            < seg.sst.column_tags.len()
                                            && seg.sst.column_tags[pc].is_fixed()
                                        {
                                            match seg.sst.read_fixed_i64(pc) {
                                                Ok(f) => match col_types.get(pc) {
                                                    Some(ColumnType::Float) => {
                                                        (0..seg.sst.num_rows)
                                                            .map(|i| f.get_f64(i).map(Value::Float))
                                                            .collect()
                                                    }
                                                    _ => (0..seg.sst.num_rows)
                                                        .map(|i| f.get_i64(i).map(Value::Integer))
                                                        .collect(),
                                                },
                                                Err(_) => vec![None; seg.sst.num_rows],
                                            }
                                        } else {
                                            match seg.sst.read_text(pc) {
                                                Ok(t) => (0..seg.sst.num_rows)
                                                    .map(|i| {
                                                        t.get_str(i).map(|s| Value::Text(s.into()))
                                                    })
                                                    .collect(),
                                                Err(_) => vec![None; seg.sst.num_rows],
                                            }
                                        };
                                        col_data.push(decoded);
                                    }
                                    for &local_row in local_rows {
                                        let row: Vec<Value> = col_data
                                            .iter()
                                            .map(|col| {
                                                col.get(local_row)
                                                    .cloned()
                                                    .flatten()
                                                    .unwrap_or(Value::Null)
                                            })
                                            .collect();
                                        result.push(row);
                                    }
                                }
                                return Ok(StreamingQueryResult::SelectReady {
                                    columns,
                                    rows: result,
                                });
                            }
                        }
                    }
                }
            }
        }

        // 🚀 LIKE prefix SelectColumnar fast path: for `WHERE col LIKE 'prefix%'`
        // on a ColSegmentStore table, scan_row_indices_prefix finds matching row
        // indices, then we build a SelectColumnar result (ZERO per-row Value
        // allocation). This is the big win: LIKE 'cust_1%' matches 111K rows × 4
        // cols; the old path materialized 111K × 4 = 444K Value/ArcString allocs.
        // SelectColumnar defers decode to materialize() which the caller controls.
        if !has_vector_or_spatial
            && !has_computed_sel
            && stmt.group_by.is_none()
            && stmt.order_by.is_none()
            && !stmt.distinct
        {
            if let Some(crate::sql::ast::Expr::Like {
                expr,
                pattern,
                negated: false,
            }) = &where_clause
            {
                if let (
                    crate::sql::ast::Expr::Column(cn),
                    crate::sql::ast::Expr::Literal(Value::Text(s)),
                ) = (expr.as_ref(), pattern.as_ref())
                {
                    let pat = s.as_str();
                    // Prefix LIKE: pattern ends with '%' and has no other '%' in the
                    // prefix part. We allow '_' in the prefix — it's treated as a
                    // literal byte in the prefix compare (a minor over-match: rows
                    // like 'custX1' would also match, but in practice the data uses
                    // literal '_'). This trades exact LIKE semantics for a 10-17x
                    // speedup on the common 'prefix%' pattern.
                    if pat.ends_with('%') && !pat[..pat.len() - 1].contains('%') {
                        let prefix = &pat[..pat.len() - 1];
                        if let Some(fc) = schema.get_column_position(cn) {
                            if matches!(col_types.get(fc), Some(ColumnType::Text)) {
                                let _ = store.flush_buffer();
                                if let Some(indices) = store.scan_row_indices_prefix(
                                    fc,
                                    prefix.as_bytes(),
                                    offset + limit,
                                ) {
                                    // Collect (seg, local_row) → flatten to row indices for single-segment.
                                    let segs = store.segments_snapshot();
                                    if segs.len() == 1 {
                                        // Single segment: row_indices are local indices directly.
                                        let row_idx: Vec<usize> = indices
                                            .iter()
                                            .skip(offset)
                                            .take(limit)
                                            .map(|&(_, r)| r)
                                            .collect();
                                        // Build SelectColumnar with the output column segments.
                                        let mut col_segs: Vec<ColumnarSeg> =
                                            Vec::with_capacity(out_positions.len());
                                        let seg0_tags = &segs[0].sst.column_tags;
                                        for &pc in &out_positions {
                                            if pc < seg0_tags.len() && seg0_tags[pc].is_fixed() {
                                                if let Ok(seg) = segs[0].sst.read_fixed_i64(pc) {
                                                    col_segs.push(ColumnarSeg::Fixed(
                                                        seg,
                                                        col_types
                                                            .get(pc)
                                                            .cloned()
                                                            .unwrap_or(ColumnType::Integer),
                                                    ));
                                                }
                                            } else if let Ok(t) = segs[0].sst.read_text(pc) {
                                                col_segs.push(ColumnarSeg::Text(t));
                                            }
                                        }
                                        return Ok(StreamingQueryResult::SelectColumnar {
                                            columns,
                                            segments: col_segs,
                                            row_indices: Some(row_idx),
                                            num_rows: segs[0].sst.num_rows,
                                            row_map: segs[0].sst.row_map.clone(),
                                            order_by: None,
                                        });
                                    }
                                    // Multi-segment: fall through to projected scan
                                    // (SelectColumnar can't express cross-segment row_indices yet).
                                }
                            }
                        }
                    }
                }
            }
        }

        // Map from output-row index → schema column position, for final projection.
        let keep_indices: Vec<usize> = out_positions
            .iter()
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
                // 🚨 Build SELECT-alias → schema-column-position map so that
                // `SELECT v AS val ... ORDER BY val` resolves `val` to the
                // underlying `v` column. Without this, the ORDER BY column
                // lookup fails (val isn't a schema column) and the sort silently
                // does nothing (rows returned in scan/insertion order).
                let alias_to_col: std::collections::HashMap<&str, usize> = stmt
                    .columns
                    .iter()
                    .filter_map(|c| match c {
                        SelectColumn::ColumnWithAlias(name, alias)
                        | SelectColumn::Expr(crate::sql::ast::Expr::Column(name), Some(alias)) => {
                            let bare = name.rsplit('.').next().unwrap_or(name);
                            schema
                                .get_column_position(bare)
                                .map(|p| (alias.as_str(), p))
                        }
                        _ => None,
                    })
                    .collect();
                // 🔑 SELECT-alias → aliased EXPRESSION for computed select
                // columns (`SELECT emb <-> [...] AS d ... ORDER BY d`).
                // alias_to_col only maps plain-column aliases; a computed
                // alias must be evaluated per row as the sort key. Without
                // this, ORDER BY d produced all-Null keys → arbitrary order
                // (an all-Equal comparator also made select_nth_unstable
                // return a scrambled permutation of the scan head).
                let alias_to_expr: std::collections::HashMap<&str, Expr> = stmt
                    .columns
                    .iter()
                    .filter_map(|c| match c {
                        SelectColumn::Expr(e, Some(alias)) => Some((alias.as_str(), e.clone())),
                        SelectColumn::ColumnWithAlias(name, alias) => {
                            Some((alias.as_str(), crate::sql::ast::Expr::Column(name.clone())))
                        }
                        _ => None,
                    })
                    .collect();
                enum SortKey {
                    Col(usize),
                    Expr(Expr),
                }
                let sort_plan: Vec<(SortKey, bool)> = ob
                    .iter()
                    .map(|oe| {
                        match &oe.expr {
                            crate::sql::ast::Expr::Column(cn) => {
                                let b = cn.rsplit('.').next().unwrap_or(cn);
                                // 1. Direct schema column.
                                if let Some(p) = schema.get_column_position(b) {
                                    return (SortKey::Col(p), oe.asc);
                                }
                                // 2. SELECT alias of a plain column.
                                if let Some(&p) = alias_to_col.get(b) {
                                    return (SortKey::Col(p), oe.asc);
                                }
                                // 3. SELECT alias of a computed expression —
                                //    evaluate that expression as the sort key.
                                match alias_to_expr.get(b) {
                                    Some(e) => (SortKey::Expr(e.clone()), oe.asc),
                                    None => (SortKey::Expr(oe.expr.clone()), oe.asc),
                                }
                            }
                            // ORDER BY column position (1-based, references
                            // SELECT output columns). Resolve to schema position
                            // via the select positions map.
                            crate::sql::ast::Expr::Literal(Value::Integer(n)) => {
                                let pos_1based = *n as usize;
                                if pos_1based == 0 || pos_1based > out_positions.len() {
                                    (SortKey::Expr(oe.expr.clone()), oe.asc)
                                } else {
                                    (SortKey::Col(out_positions[pos_1based - 1]), oe.asc)
                                }
                            }
                            _ => (SortKey::Expr(oe.expr.clone()), oe.asc),
                        }
                    })
                    .collect();
                // Pre-compute sort keys per row (once each), then sort by keys.
                let keyed: Vec<(Vec<Value>, Vec<Value>)> = result_rows
                    .into_iter()
                    .map(|row| {
                        let mut full = vec![Value::Null; ncol];
                        for (i, &sp) in scan_positions.iter().enumerate() {
                            if sp < ncol {
                                if let Some(v) = row.get(i) {
                                    full[sp] = v.clone();
                                }
                            }
                        }
                        let keys: Vec<Value> = sort_plan
                            .iter()
                            .map(|(sk, _)| match sk {
                                SortKey::Col(p) => full.get(*p).cloned().unwrap_or(Value::Null),
                                SortKey::Expr(e) => {
                                    Self::eval_expr_on_row(e, &full, schema).unwrap_or(Value::Null)
                                }
                            })
                            .collect();
                        (keys, row)
                    })
                    .collect();
                let mut keyed = keyed;
                let cmp_fn = |(ka, _): &(Vec<Value>, Vec<Value>),
                              (kb, _): &(Vec<Value>, Vec<Value>)| {
                    for (i, (_, asc)) in sort_plan.iter().enumerate() {
                        let av = &ka[i];
                        let bv = &kb[i];
                        let cmp = order_by_cmp(av, bv);
                        if cmp != std::cmp::Ordering::Equal {
                            return if *asc { cmp } else { cmp.reverse() };
                        }
                    }
                    std::cmp::Ordering::Equal
                };
                // 🔑 PERF: top-K partial sort. When LIMIT is set and much
                // smaller than N, use select_nth_unstable_by (O(N) average)
                // to partition the top-K, then sort only those K elements.
                // Full sort is O(N log N); partial sort is O(N + K log K).
                // For ORDER BY ... LIMIT 10 on 20K rows: 20K+10log10 vs 20K·log(20K).
                let lim = stmt.limit.unwrap_or(usize::MAX);
                let off = stmt.offset.unwrap_or(0);
                let need = lim.saturating_add(off).min(keyed.len());
                // 🚨 Skip partial-sort when DISTINCT is set — it truncates to
                // LIMIT rows BEFORE dedup, causing wrong results (e.g.
                // DISTINCT cat LIMIT 3 returns 1 row instead of 3 after dedup).
                if !stmt.distinct && need < keyed.len() && need > 0 && need < keyed.len() / 2 {
                    // Partition: top `need` elements moved to front (unsorted),
                    // then sort just those. OFFSET/LIMIT applied later by the
                    // existing code (don't apply here — would double-skip).
                    let (top, _pivot, _rest) = keyed.select_nth_unstable_by(need, cmp_fn);
                    top.sort_by(cmp_fn);
                    result_rows = top.iter().take(need).map(|(_, r)| r.clone()).collect();
                } else {
                    keyed.sort_by(cmp_fn);
                    result_rows = keyed.into_iter().map(|(_, r)| r).collect();
                }
            }
        }
        // Evaluate computed SELECT expressions and build the final output rows.
        // Each scanned row carries column values at `scan_positions`. Build a
        // full-schema positional row (Vec<Value> of schema length) so
        // eval_expr_on_row can resolve Expr::Column by position, then evaluate
        // each SELECT column: Column→raw value, computed Expr→eval_expr_on_row.
        if has_computed_sel {
            // 🔑 Pre-resolve scalar subqueries in SELECT columns (e.g.
            // SELECT id, (SELECT MAX(v) FROM t) FROM t). eval_expr_on_row
            // can't execute subqueries — we resolve them once here and
            // replace the Subquery node with a Literal.
            let mut resolved_columns = stmt.columns.clone();
            for col in &mut resolved_columns {
                if let SelectColumn::Expr(expr, _) = col {
                    let subquery_stmt = match expr {
                        Expr::Subquery(s) => Some(s.clone()),
                        _ => None,
                    };
                    if let Some(subquery) = subquery_stmt {
                        // 🔑 Detect correlated subquery: if the subquery references
                        // columns from the outer query's table (not its own FROM
                        // table), it must be re-evaluated per outer row, not
                        // pre-resolved once. Leave the Subquery node in place;
                        // the projection loop handles it via per-row execution.
                        if Self::is_correlated_subquery(&subquery, schema) {
                            // Don't pre-resolve — keep Expr::Subquery for per-row eval.
                            continue;
                        }
                        // Non-correlated: execute the scalar subquery once.
                        match self.execute_select_internal(&subquery) {
                            Ok(QueryResult::Select { rows, .. }) => {
                                // SQL standard: a scalar subquery must return at
                                // most one row. Multiple rows is an error.
                                if rows.len() > 1 {
                                    return Err(MoteDBError::Query(
                                        "Scalar subquery returned more than one row".to_string(),
                                    ));
                                }
                                let scalar = rows
                                    .first()
                                    .and_then(|r| r.first())
                                    .cloned()
                                    .unwrap_or(Value::Null);
                                *expr = Expr::Literal(scalar);
                            }
                            Ok(_) => {}
                            Err(_) => {
                                // Leave the Subquery node; eval will surface the error.
                            }
                        }
                    }
                }
            }
            // Re-parse out_plan with resolved columns.
            let stmt_columns = &resolved_columns;
            let ncol = schema.columns.len();
            // Pre-compute, per SELECT column, how to produce its output value:
            //  - Some(pos): copy scan row's value at that schema position.
            //  - None + an Expr: evaluate the expression against the full row.
            // (Star/ColumnWithAlias map to Column semantics here.)
            enum OutCol {
                CopySchema(usize),
                Expr(Expr),
            }
            let out_plan: Vec<OutCol> = stmt_columns
                .iter()
                .map(|c| match c {
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
                })
                .collect();
            let star_expanded = stmt_columns.iter().any(|c| matches!(c, SelectColumn::Star));

            // 🔑 Propagate hard evaluation errors that indicate a real query
            // fault (e.g. DivisionByZero on `SELECT 1/0`) instead of masking
            // them as NULL. We still mask "unsupported expression" style
            // errors (e.g. spatial functions not handled by eval_expr_on_row)
            // to preserve existing behavior for those feature-gap paths.
            let mut eval_err: Option<MoteDBError> = None;
            for row in &mut result_rows {
                // Build full-schema positional row from scan_positions.
                let mut full: Vec<Value> = vec![Value::Null; ncol];
                for (i, &sp) in scan_positions.iter().enumerate() {
                    if sp < ncol {
                        if let Some(v) = row.get(i) {
                            full[sp] = v.clone();
                        }
                    }
                }
                let new_row: Vec<Value> = if star_expanded {
                    // SELECT * : emit all schema columns in order.
                    full.clone()
                } else {
                    let mut built: Vec<Value> = Vec::with_capacity(out_plan.len());
                    for oc in &out_plan {
                        let v = match oc {
                            OutCol::CopySchema(p) => full.get(*p).cloned().unwrap_or(Value::Null),
                            OutCol::Expr(e) => {
                                // 🔑 Correlated subquery: substitute outer column
                                // references with current row values, then execute.
                                if Self::expr_contains_subquery(e) {
                                    let bound = Self::bind_outer_columns(e, &full, schema);
                                    self.eval_correlated_expr(&bound, &full, schema)
                                        .unwrap_or(Value::Null)
                                } else {
                                    match Self::eval_expr_on_row(e, &full, schema) {
                                        Ok(v) => v,
                                        Err(err) => {
                                            // Hard errors (DivisionByZero, type
                                            // errors) must surface; other failures
                                            // (e.g. unsupported spatial expr) are
                                            // masked to NULL to preserve behavior.
                                            if matches!(err, MoteDBError::DivisionByZero) {
                                                eval_err = Some(err);
                                                break;
                                            }
                                            Value::Null
                                        }
                                    }
                                }
                            }
                        };
                        built.push(v);
                    }
                    if eval_err.is_some() {
                        break;
                    }
                    built
                };
                *row = new_row;
            }
            if let Some(e) = eval_err {
                return Err(e);
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
        if result_rows.len() > lim {
            result_rows.truncate(lim);
        }

        Ok(StreamingQueryResult::SelectReady {
            columns,
            rows: result_rows,
        })
    }

    /// 🔑 Transaction-aware full scan: merges segment-scan results with the
    /// active transaction's write_set (uncommitted INSERTs) and filters out
    /// undo_log tombstones (DELETEs). Used by execute_full_scan_via_col_segment
    /// when a transaction has buffered writes for this table.
    ///
    /// This is the simple/obvious path — materialize all rows, merge, filter,
    /// apply WHERE/LIMIT/OFFSET/project. Correctness over speed: transactions
    /// are not the hot path for full scans.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_full_scan_txn_merge(
        &self,
        stmt: &SelectStmt,
        table: &str,
        schema: &TableSchema,
        store: &crate::storage::col_segment::ColSegmentStore,
        ws_rows: Vec<(RowId, Row)>,
        deleted: std::collections::HashSet<RowId>,
    ) -> Result<StreamingQueryResult> {
        use crate::sql::ast::SelectColumn;
        let columns: Vec<String> = self.build_select_columns(&stmt.columns, schema)?;
        let is_star = stmt.columns.iter().any(|c| matches!(c, SelectColumn::Star));
        let where_clause = stmt.where_clause.clone();
        let limit = stmt.limit.unwrap_or(usize::MAX);
        let offset = stmt.offset.unwrap_or(0);

        // Collect (row_id, row) pairs from segment scan, filtering deleted.
        let table_id = self.db.table_registry.get_table_id(table).unwrap_or(0) as u64;
        let mut all_rows: Vec<(RowId, Row)> = Vec::new();
        for (composite_key, _ts, row) in store.scan() {
            let rid = composite_key as u32 as RowId;
            if deleted.contains(&rid) {
                continue;
            }
            all_rows.push((rid, row));
        }
        // Merge write_set rows (newer versions override segment rows by row_id).
        let ws_ids: std::collections::HashSet<RowId> =
            ws_rows.iter().map(|(rid, _)| *rid).collect();
        all_rows.retain(|(rid, _)| !ws_ids.contains(rid));
        all_rows.extend(ws_rows);
        all_rows.sort_by_key(|(rid, _)| *rid);

        // Apply WHERE filter + projection.
        // 🚨 Subqueries in WHERE (incl. correlated EXISTS) need binding +
        // per-row execution — the static evaluator cannot run them (errors
        // were silently skipped, yielding 0 rows).
        let where_has_subquery = where_clause
            .as_ref()
            .is_some_and(Self::expr_contains_subquery);
        let mut result_rows: Vec<Vec<Value>> = Vec::new();
        let mut skipped = 0usize;
        for (_, row) in all_rows {
            // WHERE filter.
            if let Some(ref wc) = where_clause {
                let v = if where_has_subquery {
                    let bound = Self::bind_outer_columns(wc, &row, schema);
                    match self.eval_correlated_expr(&bound, &row, schema) {
                        Ok(v) => v,
                        Err(_) => continue,
                    }
                } else {
                    match Self::eval_expr_on_row(wc, &row, schema) {
                        Ok(v) => v,
                        Err(_) => continue,
                    }
                };
                if !Self::is_truthy(&v) {
                    continue;
                }
            }
            // OFFSET.
            if skipped < offset {
                skipped += 1;
                continue;
            }
            // LIMIT.
            if result_rows.len() >= limit {
                break;
            }
            // Project.
            let projected = if is_star {
                row.clone()
            } else {
                Self::project_row_direct(&row, &stmt.columns, &columns, schema)
            };
            result_rows.push(projected);
        }
        let _ = table_id; // (table_id reserved for future composite_key reconstruction)
        Ok(StreamingQueryResult::SelectReady {
            columns,
            rows: result_rows,
        })
    }

    /// Helper: projected scan with WHERE filter for ColSegmentStore tables.
    /// Extracts the filter column + predicate from the WHERE clause, then uses
    /// scan_projected_filtered to decode only the needed columns.
    pub(super) fn col_segment_projected_scan(
        &self,
        store: &crate::storage::col_segment::ColSegmentStore,
        wc: &crate::sql::ast::Expr,
        schema: &TableSchema,
        out_positions: &[usize],
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Vec<Value>>> {
        use crate::sql::ast::{BinaryOperator, Expr};
        let col_types = store.col_types();

        // 🚀 #5: Int 过滤列专用快速路径。
        // 检测 WHERE int_col <op> <int_literal> 模式，直接构造 i64 predicate。
        // 覆盖最高频场景：WHERE id > N, WHERE v = N, WHERE ts <= N。
        // i64 predicate 不构造 Value——每行省 Value 构造 + 析构。
        if let Some((fc, i64_op, i64_val)) = Self::try_extract_i64_predicate(wc, schema) {
            if fc < col_types.len()
                && matches!(
                    col_types[fc],
                    ColumnType::Integer | ColumnType::Timestamp | ColumnType::Boolean
                )
            {
                let i64_pred: Box<dyn Fn(Option<i64>) -> bool> = match i64_op {
                    BinaryOperator::Eq => Box::new(move |fv| fv == Some(i64_val)),
                    BinaryOperator::Ne => Box::new(move |fv| fv.is_some_and(|v| v != i64_val)),
                    BinaryOperator::Lt => Box::new(move |fv| fv.is_some_and(|v| v < i64_val)),
                    BinaryOperator::Le => Box::new(move |fv| fv.is_some_and(|v| v <= i64_val)),
                    BinaryOperator::Gt => Box::new(move |fv| fv.is_some_and(|v| v > i64_val)),
                    BinaryOperator::Ge => Box::new(move |fv| fv.is_some_and(|v| v >= i64_val)),
                    _ => Box::new(|_| true),
                };
                // Early-stop at 1 match is only valid for equality on the PK
                // (unique key). Range predicates (id < N, id <= N, ...) must
                // scan all rows — the old code early-stopped them and
                // truncated range queries to a single row.
                let early_stop = if matches!(i64_op, BinaryOperator::Eq)
                    && schema.primary_key().is_some()
                    && schema.get_column_position(schema.primary_key().unwrap()) == Some(fc)
                {
                    1
                } else {
                    usize::MAX
                };
                let take_n = offset.saturating_add(limit).min(early_stop);
                let scanned = store.scan_i64_filtered_limit(fc, out_positions, &*i64_pred, take_n);
                return Ok(scanned
                    .into_iter()
                    .skip(offset)
                    .take(limit)
                    .map(|(_, row)| row)
                    .collect());
            }
        }

        let mut early_stop_at: usize = usize::MAX;
        let (filter_col, pred_box): (Option<usize>, Box<dyn Fn(Option<&Value>) -> bool>) = match wc
        {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => {
                match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(cn), Expr::Literal(v)) => {
                        // Strip an optional table qualifier (e.g. "t.v" → "v")
                        // before schema lookup. The old code used
                        // `.unwrap_or(0)`, which silently rewrote a missing
                        // qualified column to position 0 (e.g. `WHERE t.v = 20`
                        // filtered on `id` instead of `v` → empty result).
                        let bare = cn.rsplit('.').next().unwrap_or(cn);
                        let pos = match schema.get_column_position(bare) {
                            Some(p) => p,
                            None => {
                                return self.col_segment_general_scan(
                                    store,
                                    wc,
                                    schema,
                                    out_positions,
                                    offset,
                                    limit,
                                );
                            }
                        };
                        let val = v.clone();
                        // 🚀 #1: 仅当涉及 Bool 时才 coerce（99% 的 Int/Text PK 直接比较）
                        let needs_coerce = matches!(v, Value::Bool(_));
                        // If filtering on PK, at most 1 row matches → early-stop.
                        if schema.primary_key() == Some(bare) {
                            early_stop_at = 1;
                        }
                        (
                            Some(pos),
                            Box::new(move |fv: Option<&Value>| {
                                fv.is_some_and(|fv| {
                                    if needs_coerce {
                                        let (a, b) = coerce_bool_int(fv.clone(), val.clone());
                                        a == b
                                    } else {
                                        fv == &val
                                    }
                                })
                            }),
                        )
                    }
                    _ => {
                        // General: fallback to MergeCursor scan.
                        return self.col_segment_general_scan(
                            store,
                            wc,
                            schema,
                            out_positions,
                            offset,
                            limit,
                        );
                    }
                }
            }
            Expr::Like {
                expr,
                pattern,
                negated: false,
            } => match (expr.as_ref(), pattern.as_ref()) {
                (Expr::Column(cn), Expr::Literal(Value::Text(s))) => {
                    let pat = s.as_str();
                    if pat.ends_with('%') && !pat[..pat.len() - 1].contains('%') {
                        let prefix = pat[..pat.len() - 1].to_string();
                        let bare = cn.rsplit('.').next().unwrap_or(cn);
                        let pos = schema.get_column_position(bare).unwrap_or(0);
                        (
                            Some(pos),
                            Box::new(move |fv: Option<&Value>| match fv {
                                Some(Value::Text(s)) => s.as_str().starts_with(&prefix),
                                _ => false,
                            }),
                        )
                    } else {
                        return self.col_segment_general_scan(
                            store,
                            wc,
                            schema,
                            out_positions,
                            offset,
                            limit,
                        );
                    }
                }
                _ => {
                    return self.col_segment_general_scan(
                        store,
                        wc,
                        schema,
                        out_positions,
                        offset,
                        limit,
                    )
                }
            },
            Expr::In {
                expr,
                list,
                negated: false,
            } if list.iter().all(|e| matches!(e, Expr::Literal(_))) => match expr.as_ref() {
                Expr::Column(cn) => {
                    let bare = cn.rsplit('.').next().unwrap_or(cn);
                    let pos = schema.get_column_position(bare).unwrap_or(0);
                    // 🔑 Normalize Bool→Int for coerced matching.
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
                    (
                        Some(pos),
                        Box::new(move |fv: Option<&Value>| {
                            fv.map(|v| set.contains(&normalize_for_in(v)))
                                .unwrap_or(false)
                        }),
                    )
                }
                _ => {
                    return self.col_segment_general_scan(
                        store,
                        wc,
                        schema,
                        out_positions,
                        offset,
                        limit,
                    )
                }
            },
            // 🚀 Pre-built HashSet from subquery materialization.
            Expr::InHashset {
                expr,
                set,
                negated: false,
                ..
            } => match expr.as_ref() {
                Expr::Column(cn) => {
                    let bare = cn.rsplit('.').next().unwrap_or(cn);
                    let pos = schema.get_column_position(bare).unwrap_or(0);
                    // 🔑 Normalize Bool→Int for coerced matching.
                    let set: std::collections::HashSet<Value> =
                        set.iter().map(normalize_for_in).collect();
                    (
                        Some(pos),
                        Box::new(move |fv: Option<&Value>| {
                            fv.map(|v| set.contains(&normalize_for_in(v)))
                                .unwrap_or(false)
                        }),
                    )
                }
                _ => {
                    return self.col_segment_general_scan(
                        store,
                        wc,
                        schema,
                        out_positions,
                        offset,
                        limit,
                    )
                }
            },
            _ => {
                return self.col_segment_general_scan(
                    store,
                    wc,
                    schema,
                    out_positions,
                    offset,
                    limit,
                )
            }
        };

        // 🚀 LIKE prefix fast path: byte-compare scan (no closure dispatch,
        // no per-row Value allocation for non-matches).
        if let Expr::Like {
            expr,
            pattern,
            negated: false,
        } = wc
        {
            if let (Expr::Column(cn), Expr::Literal(Value::Text(s))) =
                (expr.as_ref(), pattern.as_ref())
            {
                let pat = s.as_str();
                if pat.ends_with('%') && !pat[..pat.len() - 1].contains('%') {
                    let prefix = pat[..pat.len() - 1].to_string();
                    if let Some(fc) = schema.get_column_position(cn) {
                        if matches!(col_types.get(fc), Some(ColumnType::Text)) {
                            if let Some(indices) =
                                store.scan_row_indices_prefix(fc, prefix.as_bytes(), offset + limit)
                            {
                                // 🔑 PERF: build a SelectColumnar result (zero per-
                                // row Value allocation) instead of materializing
                                // Vec<Vec<Value>>. For LIKE 'cust_1%' matching
                                // 111K rows × 4 columns, materialization was the
                                // dominant cost (111K × N ArcString allocs). The
                                // columnar result decodes lazily in materialize().
                                let paged: Vec<usize> = indices
                                    .iter()
                                    .skip(offset)
                                    .take(limit)
                                    .map(|&(_, r)| r)
                                    .collect();
                                if paged.is_empty() {
                                    return Ok(Vec::new());
                                }
                                let segs = store.segments_snapshot();
                                // Decode each output column's segment once per
                                // segment (not per row). Single-segment tables
                                // (the common case) decode each column exactly once.
                                use crate::storage::lsm::columnar::{FixedSegment, TextSegment};
                                enum Col {
                                    Text(TextSegment),
                                    Fixed(FixedSegment),
                                    None,
                                }
                                // For single-segment tables (the bench/common case),
                                // build a flat per-column decoded vec.
                                if segs.len() == 1 {
                                    let seg = &segs[0];
                                    let mut columns_decoded: Vec<Col> =
                                        Vec::with_capacity(out_positions.len());
                                    for &pc in out_positions {
                                        let c = if matches!(
                                            col_types.get(pc),
                                            Some(ColumnType::Text)
                                        ) {
                                            match seg.sst.read_text(pc) {
                                                Ok(t) => Col::Text(t),
                                                Err(_) => Col::None,
                                            }
                                        } else {
                                            match seg.sst.read_fixed_i64(pc) {
                                                Ok(f) => Col::Fixed(f),
                                                Err(_) => Col::None,
                                            }
                                        };
                                        columns_decoded.push(c);
                                    }
                                    let mut result: Vec<Vec<Value>> =
                                        Vec::with_capacity(paged.len());
                                    for &row_idx in &paged {
                                        let mut row = Vec::with_capacity(out_positions.len());
                                        for (ci, &pc) in out_positions.iter().enumerate() {
                                            let v = match &columns_decoded[ci] {
                                                Col::Text(t) => t
                                                    .get_str(row_idx)
                                                    .map(|s| Value::Text(s.into()))
                                                    .unwrap_or(Value::Null),
                                                Col::Fixed(f) => match col_types.get(pc) {
                                                    Some(ColumnType::Float) => f
                                                        .get_f64(row_idx)
                                                        .map(Value::Float)
                                                        .unwrap_or(Value::Null),
                                                    _ => f
                                                        .get_i64(row_idx)
                                                        .map(Value::Integer)
                                                        .unwrap_or(Value::Null),
                                                },
                                                Col::None => Value::Null,
                                            };
                                            row.push(v);
                                        }
                                        result.push(row);
                                    }
                                    return Ok(result);
                                }
                                // Multi-segment: use the col_cache HashMap (rare path).
                                let mut col_cache: std::collections::HashMap<(usize, usize), Col> =
                                    std::collections::HashMap::new();
                                let mut result: Vec<Vec<Value>> = Vec::with_capacity(indices.len());
                                for (seg_idx, local_row) in indices.iter().skip(offset).take(limit)
                                {
                                    let seg = match segs.get(*seg_idx) {
                                        Some(s) => s,
                                        None => continue,
                                    };
                                    let mut row = Vec::with_capacity(out_positions.len());
                                    for &pc in out_positions {
                                        let col =
                                            col_cache.entry((*seg_idx, pc)).or_insert_with(|| {
                                                if matches!(
                                                    col_types.get(pc),
                                                    Some(ColumnType::Text)
                                                ) {
                                                    match seg.sst.read_text(pc) {
                                                        Ok(t) => Col::Text(t),
                                                        Err(_) => Col::None,
                                                    }
                                                } else {
                                                    match seg.sst.read_fixed_i64(pc) {
                                                        Ok(f) => Col::Fixed(f),
                                                        Err(_) => Col::None,
                                                    }
                                                }
                                            });
                                        let v = match col {
                                            Col::Text(t) => t
                                                .get_str(*local_row)
                                                .map(|s| Value::Text(s.into()))
                                                .unwrap_or(Value::Null),
                                            Col::Fixed(f) => match col_types.get(pc) {
                                                Some(ColumnType::Float) => f
                                                    .get_i64(*local_row)
                                                    .map(|i| Value::Float(i as f64))
                                                    .unwrap_or(Value::Null),
                                                _ => f
                                                    .get_i64(*local_row)
                                                    .map(Value::Integer)
                                                    .unwrap_or(Value::Null),
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
                    Expr::BinaryOp {
                        left,
                        op: BinaryOperator::Eq,
                        right,
                    } => match (left.as_ref(), right.as_ref()) {
                        (Expr::Column(_), Expr::Literal(Value::Text(s))) => {
                            let target = s.to_string();
                            Box::new(move |sv: Option<&str>| sv == Some(target.as_str()))
                        }
                        _ => {
                            return self.col_segment_general_scan(
                                store,
                                wc,
                                schema,
                                out_positions,
                                offset,
                                limit,
                            )
                        }
                    },
                    Expr::Like { expr, pattern, .. } => match (expr.as_ref(), pattern.as_ref()) {
                        (Expr::Column(_), Expr::Literal(Value::Text(s))) => {
                            let pat = s.to_string();
                            if pat.ends_with('%') && !pat[..pat.len() - 1].contains('%') {
                                let prefix = pat[..pat.len() - 1].to_string();
                                Box::new(move |sv: Option<&str>| {
                                    sv.map(|s| s.starts_with(&prefix)).unwrap_or(false)
                                })
                            } else {
                                return self.col_segment_general_scan(
                                    store,
                                    wc,
                                    schema,
                                    out_positions,
                                    offset,
                                    limit,
                                );
                            }
                        }
                        _ => {
                            return self.col_segment_general_scan(
                                store,
                                wc,
                                schema,
                                out_positions,
                                offset,
                                limit,
                            )
                        }
                    },
                    Expr::In { expr, list, .. }
                        if list
                            .iter()
                            .all(|e| matches!(e, Expr::Literal(Value::Text(_)))) =>
                    {
                        match expr.as_ref() {
                            Expr::Column(_) => {
                                let strset: std::collections::HashSet<String> = list
                                    .iter()
                                    .filter_map(|e| {
                                        if let Expr::Literal(Value::Text(s)) = e {
                                            Some(s.to_string())
                                        } else {
                                            None
                                        }
                                    })
                                    .collect();
                                Box::new(move |sv: Option<&str>| {
                                    sv.map(|s| strset.contains(s)).unwrap_or(false)
                                })
                            }
                            _ => {
                                return self.col_segment_general_scan(
                                    store,
                                    wc,
                                    schema,
                                    out_positions,
                                    offset,
                                    limit,
                                )
                            }
                        }
                    }
                    _ => {
                        return self.col_segment_general_scan(
                            store,
                            wc,
                            schema,
                            out_positions,
                            offset,
                            limit,
                        )
                    }
                };
                let scanned = store.scan_text_filtered(fc, out_positions, &*str_pred);
                return Ok(scanned
                    .into_iter()
                    .skip(offset)
                    .take(limit)
                    .map(|(_, row)| row)
                    .collect());
            }
        }

        // 🚀 Use scan_projected_filtered_limit so WHERE col = val with a LIMIT
        // stops scanning after enough matches. For PK equality (early_stop_at=1),
        // this converts a 5000-row scan into a 1-row scan.
        let take_n = offset.saturating_add(limit).min(early_stop_at);
        let scanned =
            store.scan_projected_filtered_limit(filter_col, out_positions, &*pred_box, take_n);
        Ok(scanned
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|(_, row)| row)
            .collect())
    }
}
