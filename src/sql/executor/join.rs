//! JOIN 执行簇：multi-way/positional/hash/expr-key join + 左右全外连接。
use super::*;

impl QueryExecutor {
    /// Positional INNER JOIN fast path: scans both tables as Vec<Value> rows,
    /// builds a hash table on the join column, probes, and concatenates — all
    /// without converting to SqlRow(HashMap). Returns None if the query is too
    /// complex for this path (e.g. column not found).
    /// Shared JOIN result finalization: WHERE filter + projection + ORDER BY + LIMIT.
    /// Used by both the hash-join and PK-index-join paths to avoid code duplication.
    pub(super) fn finalize_join_result(
        &self,
        stmt: &SelectStmt,
        joined: Vec<Vec<Value>>,
        combined_cols: &[String],
        lschema: &TableSchema,
        rschema: &TableSchema,
        lprefix: &str,
        rprefix: &str,
    ) -> Result<QueryResult> {
        use crate::sql::ast::SelectColumn;
        // Apply WHERE on the combined (pre-projection) rows if present.
        let filtered_joined = if let Some(ref wc) = stmt.where_clause {
            let temp_schema = {
                let lncol = lschema.columns.len();
                let mut s: TableSchema = (*lschema).clone();
                for c in &mut s.columns {
                    c.name = format!("{}.{}", lprefix, c.name);
                }
                let mut rcols: Vec<_> = rschema.columns.clone();
                for c in &mut rcols {
                    c.name = format!("{}.{}", rprefix, c.name);
                    c.position += lncol;
                }
                s.columns.extend(rcols);
                s.rebuild_column_map();
                s
            };
            joined
                .into_iter()
                .filter(|row| {
                    matches!(
                        Self::eval_expr_on_row(wc, row, &temp_schema),
                        Ok(Value::Bool(true))
                    )
                })
                .collect::<Vec<_>>()
        } else {
            joined
        };

        // 🔑 ORDER BY 先于投影在 combined 全列行上解析: joined 行携带两表
        // 全列 (combined_cols 含 "b.id" 等未投影列)。旧实现投影后才按输出
        // 列名匹配, `ORDER BY a.id, b.id DESC` 的 b.id (未投影) 被裸名
        // "id" 误匹配到 a.id — 第二排序键失效 (differential fuzz)。
        // 限定名精确匹配 combined_cols; 裸名只允许唯一命中; 两者都失败的
        // 键留给投影后按输出名 (别名) 二次解析。
        let mut combined_specs: Vec<(usize, bool)> = Vec::new();
        let mut unresolved_ob: Vec<&crate::sql::ast::OrderByExpr> = Vec::new();
        if let Some(ref order_by) = stmt.order_by {
            for ob in order_by {
                let resolved = match &ob.expr {
                    crate::sql::ast::Expr::Column(cn) => {
                        if cn.contains('.') {
                            combined_cols.iter().position(|c| c == cn)
                        } else {
                            let hits: Vec<usize> = combined_cols
                                .iter()
                                .enumerate()
                                .filter(|(_, c)| c.rsplit('.').next().unwrap_or(c) == cn)
                                .map(|(i, _)| i)
                                .collect();
                            if hits.len() == 1 {
                                Some(hits[0])
                            } else {
                                None
                            }
                        }
                    }
                    _ => None,
                };
                match resolved {
                    Some(i) => combined_specs.push((i, ob.asc)),
                    None => unresolved_ob.push(ob),
                }
            }
        }
        let mut filtered_joined = filtered_joined;
        if !combined_specs.is_empty() {
            filtered_joined.sort_by(|a, b| {
                for &(idx, asc) in &combined_specs {
                    let av = a.get(idx).cloned().unwrap_or(Value::Null);
                    let bv = b.get(idx).cloned().unwrap_or(Value::Null);
                    let cmp = order_by_cmp(&av, &bv);
                    if cmp != std::cmp::Ordering::Equal {
                        return if asc { cmp } else { cmp.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // Resolve output columns.
        let (column_names, projected_rows) = if stmt.columns.len() == 1
            && matches!(stmt.columns[0], SelectColumn::Star)
        {
            let names: Vec<String> = combined_cols
                .iter()
                .map(|c| c.rsplit('.').next().unwrap_or(c).to_string())
                .collect();
            (names, filtered_joined)
        } else {
            let mut col_indices: Vec<Option<usize>> = Vec::new();
            let mut out_names: Vec<String> = Vec::new();
            // Track which combined_cols indices have already been claimed by a
            // previous SELECT column. This prevents two SELECT columns from
            // matching the same combined_col when bare names are ambiguous
            // (e.g. SELECT a.name, b.name where both have suffix "name").
            let mut claimed: std::collections::HashSet<usize> = std::collections::HashSet::new();
            for sc in &stmt.columns {
                match sc {
                    SelectColumn::Star => {
                        for (i, name) in combined_cols.iter().enumerate() {
                            col_indices.push(Some(i));
                            out_names.push(name.clone());
                        }
                    }
                    SelectColumn::Column(name) => {
                        let bare = name.rsplit('.').next().unwrap_or(name);
                        // 🔑 combined_cols are table-qualified ("a.name", "b.val").
                        // 1) Try exact match (qualified or bare exact).
                        // 2) Fall back to bare-suffix match, skipping already-claimed.
                        let pos = combined_cols
                            .iter()
                            .enumerate()
                            .find(|(i, c)| !claimed.contains(i) && (*c == bare || *c == name))
                            .map(|(i, _)| i)
                            .or_else(|| {
                                combined_cols
                                    .iter()
                                    .enumerate()
                                    .find(|(i, c)| {
                                        !claimed.contains(i)
                                            && c.rsplit('.').next().unwrap_or(c) == bare
                                    })
                                    .map(|(i, _)| i)
                            });
                        if let Some(p) = pos {
                            claimed.insert(p);
                            col_indices.push(Some(p));
                            out_names.push(bare.to_string());
                        } else {
                            col_indices.push(None);
                            out_names.push(name.clone());
                        }
                    }
                    SelectColumn::ColumnWithAlias(name, alias) => {
                        let bare = name.rsplit('.').next().unwrap_or(name);
                        let pos = combined_cols
                            .iter()
                            .enumerate()
                            .find(|(i, c)| !claimed.contains(i) && (*c == bare || *c == name))
                            .map(|(i, _)| i)
                            .or_else(|| {
                                combined_cols
                                    .iter()
                                    .enumerate()
                                    .find(|(i, c)| {
                                        !claimed.contains(i)
                                            && c.rsplit('.').next().unwrap_or(c) == bare
                                    })
                                    .map(|(i, _)| i)
                            });
                        if let Some(p) = pos {
                            claimed.insert(p);
                            col_indices.push(Some(p));
                            out_names.push(alias.clone());
                        } else {
                            col_indices.push(None);
                            out_names.push(alias.clone());
                        }
                    }
                    SelectColumn::Expr(_, alias) => {
                        col_indices.push(None);
                        out_names.push(alias.clone().unwrap_or_else(|| "expr".to_string()));
                    }
                }
            }
            let proj: Vec<Vec<Value>> = filtered_joined
                .into_iter()
                .map(|row| {
                    col_indices
                        .iter()
                        .map(|&oi| oi.and_then(|i| row.get(i)).cloned().unwrap_or(Value::Null))
                        .collect()
                })
                .collect();
            (out_names, proj)
        };

        // 投影后二次解析: 只处理 combined 行上没命中的键 (别名/输出名)。
        // 限定名精确匹配输出列; 裸名唯一命中; 未命中则跳过 (旧行为)。
        let final_rows = if !unresolved_ob.is_empty() {
            let sort_specs: Vec<(usize, bool)> = unresolved_ob
                .iter()
                .filter_map(|ob| {
                    if let crate::sql::ast::Expr::Column(cn) = &ob.expr {
                        if cn.contains('.') {
                            column_names.iter().position(|c| c == cn)
                        } else {
                            let hits: Vec<usize> = column_names
                                .iter()
                                .enumerate()
                                .filter(|(_, c)| c.rsplit('.').next().unwrap_or(c) == cn)
                                .map(|(i, _)| i)
                                .collect();
                            if hits.len() == 1 {
                                Some(hits[0])
                            } else {
                                None
                            }
                        }
                        .map(|idx| (idx, ob.asc))
                    } else {
                        None
                    }
                })
                .collect();
            if sort_specs.is_empty() {
                projected_rows
            } else {
                let mut rows = projected_rows;
                rows.sort_by(|a, b| {
                    for &(idx, asc) in &sort_specs {
                        let av = a.get(idx).cloned().unwrap_or(Value::Null);
                        let bv = b.get(idx).cloned().unwrap_or(Value::Null);
                        let cmp = order_by_cmp(&av, &bv);
                        if cmp != std::cmp::Ordering::Equal {
                            return if asc { cmp } else { cmp.reverse() };
                        }
                    }
                    std::cmp::Ordering::Equal
                });
                rows
            }
        } else {
            projected_rows
        };

        // Apply OFFSET/LIMIT.
        let offset = stmt.offset.unwrap_or(0);
        let limit = stmt.limit;
        let final_rows: Vec<Vec<Value>> = final_rows
            .into_iter()
            .skip(offset)
            .take(limit.unwrap_or(usize::MAX))
            .collect();

        Ok(QueryResult::Select {
            columns: column_names,
            rows: final_rows,
        })
    }

    pub(super) fn try_positional_inner_join(
        &self,
        stmt: &SelectStmt,
        ltable: &str,
        lalias: Option<&str>,
        rtable: &str,
        ralias: Option<&str>,
        lcol_full: &str,
        rcol_full: &str,
    ) -> Result<Option<QueryResult>> {
        let lprefix = lalias.unwrap_or(ltable);
        let rprefix = ralias.unwrap_or(rtable);

        // Resolve the bare join column names (strip table prefix).
        let lcol_bare = lcol_full.rsplit('.').next().unwrap_or(lcol_full);
        let rcol_bare = rcol_full.rsplit('.').next().unwrap_or(rcol_full);

        let lschema = match self.db.get_table_schema(ltable) {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };
        let rschema = match self.db.get_table_schema(rtable) {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };

        let lcol_pos = match lschema.get_column_position(lcol_bare) {
            Some(p) => p,
            None => return Ok(None),
        };
        let rcol_pos = match rschema.get_column_position(rcol_bare) {
            Some(p) => p,
            None => return Ok(None),
        };

        // 🔑 PERF: PK-index nested-loop JOIN. When the right (inner) table's
        // join column is its PK and it uses ColSegmentStore, we can do O(K·log N)
        // PK lookups instead of scanning all N inner rows. For LIMIT 100 on a
        // 20K-row inner table, this is 100 binary searches vs 20K row decodes.
        // The outer (left) table is still fully scanned (but can early-stop
        // at LIMIT).
        let r_is_pk = rschema
            .primary_key()
            .is_some_and(|pk| pk.rsplit('.').next().unwrap_or(pk) == rcol_bare);
        if r_is_pk && self.db.has_col_segment_store(rtable) {
            // Scan left table (driver), only join column + all output cols.
            let lrows = self.scan_table_rows_fast(ltable, &lschema)?;
            let rstore = self
                .db
                .get_or_create_col_segment_store(rtable, rschema.col_types())?;
            let _ = rstore.flush_buffer();
            let rtable_id = self.db.table_registry.get_table_id(rtable).unwrap_or(0) as u64;
            let limit = stmt.limit.unwrap_or(usize::MAX);
            // 🔑 Don't early-terminate when WHERE is present — finalize_join_result
            // applies WHERE after the join, so we need all matching rows first,
            // THEN take LIMIT. Early-break before WHERE gives too few results.
            // 🔑 Same for ORDER BY (DESC especially: the top rows come LAST in
            // scan order) and OFFSET (LIMIT+OFFSET rows needed after sorting).
            let has_where =
                stmt.where_clause.is_some() || stmt.order_by.is_some() || stmt.offset.is_some();
            let lncol = lschema.columns.len();
            let rncol = rschema.columns.len();
            let mut joined: Vec<Vec<Value>> = Vec::with_capacity(lrows.len().min(limit));

            for (_, lrow) in &lrows {
                if !has_where && joined.len() >= limit {
                    break;
                }
                let lval = match lrow.get(lcol_pos) {
                    Some(v) => v,
                    None => continue,
                };
                // Resolve PK value → composite key for right table.
                let rkey = match lval {
                    Value::Integer(id) if rschema.is_primary_key_auto_increment() => {
                        (rtable_id << 32) | (*id as u64 & 0xFFFFFFFF)
                    }
                    Value::Integer(_) => {
                        // Non-AUTO_INCREMENT PK: use pk_lookup cache. The join
                        // value is a PK *value*, NOT a row_id — for non-AUTO_INCREMENT
                        // PKs the two differ (row_id comes from a global counter).
                        // The old code fell back to treating the value as a row_id on
                        // a cache miss, which created spurious matches: e.g. a
                        // self-join where manager_id=0 (no matching id) collided with
                        // the first-inserted row's row_id=0, including unmatched rows.
                        // On a cache miss the PK genuinely doesn't exist → no match.
                        let pk_key = crate::database::pk_cache::PkKey::from_value(lval);
                        match self
                            .db
                            .pk_lookup
                            .get(rtable)
                            .and_then(|l| l.get_pk(&pk_key))
                        {
                            Some(rid) => (rtable_id << 32) | (rid & 0xFFFFFFFF),
                            None => continue,
                        }
                    }
                    _ => continue,
                };
                // Binary search in right table's row_map.
                if let Some(rrow) = rstore.get(rkey) {
                    let mut combined = Vec::with_capacity(lncol + rncol);
                    combined.extend_from_slice(lrow);
                    combined.extend_from_slice(&rrow);
                    joined.push(combined);
                }
            }

            // Build column names + apply WHERE + project (shared with hash path).
            let combined_cols: Vec<String> = lschema
                .columns
                .iter()
                .map(|c| format!("{}.{}", lprefix, c.name))
                .chain(
                    rschema
                        .columns
                        .iter()
                        .map(|c| format!("{}.{}", rprefix, c.name)),
                )
                .collect();
            return Ok(Some(self.finalize_join_result(
                stmt,
                joined,
                &combined_cols,
                &lschema,
                &rschema,
                lprefix,
                rprefix,
            )?));
        }

        // 🔑 PERF: scan via ColSegmentStore (avoids the legacy
        // scan_table_rows_streaming path which triggers compaction). When either
        // join column is the PK, we could use index lookups instead of a full
        // scan — but for now, use ColSegmentStore's projected scan which only
        // decodes the join column + output columns (not all columns).
        // The previous code used scan_table_rows_streaming which materialized
        // ALL columns of ALL rows of BOTH tables — 2×N full-row decodes.
        // 🔑 单表谓词下推 (同多路 join): 扫描侧过滤, 避免全表叉积后过滤。
        let all_proj: Vec<usize> = (0..lschema.columns.len()).collect();
        let push_preds = stmt
            .where_clause
            .as_ref()
            .map(Self::extract_pushdown_preds)
            .unwrap_or_default();
        let lpd = Self::map_pushdown_preds(&push_preds, lprefix, &lschema, &all_proj);
        let lrows: Vec<(u64, Vec<Value>)> = self
            .scan_table_rows_fast(ltable, &lschema)?
            .into_iter()
            .filter(|(_, row)| {
                lpd.iter()
                    .all(|(p, op, t)| apply_op_value(op, row.get(*p), t))
            })
            .collect();
        let r_all_proj: Vec<usize> = (0..rschema.columns.len()).collect();
        let rpd = Self::map_pushdown_preds(&push_preds, rprefix, &rschema, &r_all_proj);
        let rrows: Vec<(u64, Vec<Value>)> = self
            .scan_table_rows_fast(rtable, &rschema)?
            .into_iter()
            .filter(|(_, row)| {
                rpd.iter()
                    .all(|(p, op, t)| apply_op_value(op, row.get(*p), t))
            })
            .collect();

        // Build hash table on the smaller side (right) keyed by join column value.
        use std::collections::HashMap;
        // Hash key: numeric values share a Numeric variant so that Integer
        // and Float values that are numerically equal match in joins.
        // Small integers (within f64 exact range) are converted to f64 bits
        // to match Float columns; large integers preserve full 64-bit range.
        #[derive(Hash, PartialEq, Eq)]
        enum JoinKey {
            Numeric(u64), // f64::to_bits() for Float and small Integer (< 2^53)
            Integer(u64), // i64 wrapped to u64 for large Integer, preserves full range
            Text(String),
            Bool(bool),
        }
        fn to_key(v: &Value) -> Option<JoinKey> {
            match v {
                Value::Integer(i) => {
                    const EXACT_MAX: i64 = 1i64 << 53; // 2^53, max exact i64 in f64
                    if *i >= -EXACT_MAX && *i <= EXACT_MAX {
                        Some(JoinKey::Numeric((*i as f64).to_bits()))
                    } else {
                        Some(JoinKey::Integer((*i as u64).wrapping_add(i64::MIN as u64)))
                    }
                }
                Value::Float(f) => Some(JoinKey::Numeric(f.to_bits())),
                Value::Text(s) => Some(JoinKey::Text(s.to_string())),
                Value::Bool(b) => Some(JoinKey::Bool(*b)),
                _ => None,
            }
        }
        let mut hash: HashMap<JoinKey, Vec<usize>> = HashMap::with_capacity(rrows.len());
        for (ri, (_, row)) in rrows.iter().enumerate() {
            if let Some(k) = row.get(rcol_pos).and_then(to_key) {
                hash.entry(k).or_default().push(ri);
            }
        }

        // Probe with the left side and concatenate matching rows.
        // 🔑 PERF: early-terminate when LIMIT is set — avoids probing all N
        // left rows when only K matches are needed. For LIMIT 100 on a 20K-row
        // table this cuts the probe loop from 20K to ~100 iterations.
        // 🔑 但 ORDER BY 存在时禁止早停: join 输出必须先收集全量再排序,
        // 截断 27 行再排序会让 LIMIT 边界取到错误的行 (differential fuzz:
        // ORDER BY i.id, t.id LIMIT 27 的最后一行与无 LIMIT 版本不一致)。
        let limit = stmt.limit.unwrap_or(usize::MAX);
        let early_break_ok = stmt.where_clause.is_none() && stmt.order_by.is_none();
        let has_where = stmt.where_clause.is_some();
        let lncol = lschema.columns.len();
        let mut joined: Vec<Vec<Value>> = Vec::with_capacity(lrows.len().min(limit));
        for (_, lrow) in &lrows {
            if early_break_ok && joined.len() >= limit {
                break;
            }
            if let Some(k) = lrow.get(lcol_pos).and_then(to_key) {
                if let Some(matches) = hash.get(&k) {
                    for &ri in matches {
                        if early_break_ok && joined.len() >= limit {
                            break;
                        }
                        let rrow = &rrows[ri].1;
                        let mut combined = Vec::with_capacity(lncol + rrow.len());
                        combined.extend_from_slice(lrow);
                        combined.extend_from_slice(rrow);
                        joined.push(combined);
                    }
                }
            }
        }

        // Shared result finalization (WHERE + projection + ORDER BY + LIMIT).
        let combined_cols: Vec<String> = lschema
            .columns
            .iter()
            .map(|c| format!("{}.{}", lprefix, c.name))
            .chain(
                rschema
                    .columns
                    .iter()
                    .map(|c| format!("{}.{}", rprefix, c.name)),
            )
            .collect();
        Ok(Some(self.finalize_join_result(
            stmt,
            joined,
            &combined_cols,
            &lschema,
            &rschema,
            lprefix,
            rprefix,
        )?))
    }

    /// Flatten a left-deep INNER-equi-join FROM chain into the base table
    /// plus its join steps. Returns None for non-Inner joins, right-nested
    /// joins, or a bare table.
    pub(super) fn flatten_left_deep_inner(
        from: &TableRef,
    ) -> Option<(
        (String, Option<String>),
        Vec<(String, Option<String>, Expr)>,
    )> {
        match from {
            TableRef::Table { name, alias } => Some(((name.clone(), alias.clone()), vec![])),
            TableRef::Join {
                left,
                right,
                join_type: JoinType::Inner,
                on_condition,
            } => {
                let (mut base, mut steps) = Self::flatten_left_deep_inner(left)?;
                match right.as_ref() {
                    TableRef::Table { name, alias } => {
                        steps.push((name.clone(), alias.clone(), on_condition.clone()));
                        Some((base, steps))
                    }
                    _ => None, // right side must be a plain table (left-deep)
                }
            }
            _ => None,
        }
    }


    /// 🚀 全乘积/链式 INNER JOIN 的 COUNT(*) 折叠。
    ///
    /// `SELECT COUNT(*) FROM a JOIN b ON 1=1` (无跨表约束) 曾走通用路径:
    /// 双侧物化 SqlRow + 每对 combine_rows 建 HashMap + eval — 20K×30K
    /// (6 亿对) 物化数分钟。当每步 ON 都是 (a) 常量表达式 或 (b) 只引用
    /// 单表列的 `col op literal` 谓词, 且 WHERE 亦全分解为单表谓词时,
    /// INNER join 的计数因式分解: 结果 = Π 各表 (过滤后) 行数 — 零物化,
    /// 复杂度 O(Σ N_i)。等值/跨表 ON decline (hash 路径已够快)。
    pub(super) fn try_join_count_fold(
        &self,
        stmt: &SelectStmt,
    ) -> Result<Option<(Vec<String>, Vec<Vec<Value>>)>> {
        use crate::sql::ast::{BinaryOperator, SelectColumn};
        if Self::is_in_transaction_tls() {
            return Ok(None); // 事务内 read-your-writes 语义留给通用路径
        }
        if stmt.group_by.is_some()
            || stmt.having.is_some()
            || stmt.distinct
            || stmt.latest_by.is_some()
        {
            return Ok(None);
        }
        // 恰好一个 COUNT(*)(无参或 *), 无其它输出列。
        if stmt.columns.len() != 1 {
            return Ok(None);
        }
        let agg_expr = match stmt.columns.first() {
            Some(SelectColumn::Expr(e, _)) => e,
            _ => return Ok(None),
        };
        let Expr::FunctionCall {
            name,
            args,
            distinct: false,
            ..
        } = agg_expr
        else {
            return Ok(None);
        };
        if !name.eq_ignore_ascii_case("COUNT") {
            return Ok(None);
        }
        let ok_arg = args.is_empty()
            || (args.len() == 1 && matches!(args.first(), Some(Expr::Column(c)) if c == "*"));
        if !ok_arg {
            return Ok(None);
        }
        let Some(from) = stmt.from.as_ref() else {
            return Ok(None);
        };
        let Some(((btable, balias), steps)) = Self::flatten_left_deep_inner(from) else {
            return Ok(None);
        };
        if steps.is_empty() {
            return Ok(None); // 单表 COUNT 走既有快路径
        }

        // 表清单: (表名, 前缀, 限定谓词[(schema 位, op, 字面量)])
        type Pred = (usize, BinaryOperator, Value);
        let mut tables: Vec<(String, String, Vec<Pred>, Arc<TableSchema>)> = Vec::new();
        {
            let bprefix = balias.unwrap_or_else(|| btable.clone());
            let Ok(bschema) = self.db.get_table_schema(&btable) else {
                return Ok(None);
            };
            tables.push((btable.clone(), bprefix, Vec::new(), bschema));
            for (jt, ja, _) in &steps {
                let Ok(js) = self.db.get_table_schema(jt) else {
                    return Ok(None);
                };
                let jp = ja.clone().unwrap_or_else(|| jt.clone());
                tables.push((jt.clone(), jp, Vec::new(), js));
            }
        }
        let assign_pred = |tables: &mut Vec<(String, String, Vec<Pred>, Arc<TableSchema>)>,
                           prefix: &str,
                           bare: &str,
                           op: BinaryOperator,
                           lit: &Value|
         -> bool {
            for t in tables.iter_mut() {
                if t.1 == prefix {
                    return match t.3.get_column_position(bare) {
                        Some(p) => {
                            t.2.push((p, op, lit.clone()));
                            true
                        }
                        None => false,
                    };
                }
            }
            false
        };

        // WHERE: 严格全分解 — 每个 AND 叶都必须是 `prefix.col op literal`
        // (extract_pushdown_preds 会静默丢弃不匹配的叶, 不能直接用)。
        if let Some(wc) = &stmt.where_clause {
            let preds = Self::extract_pushdown_preds(wc);
            let mut leaves = 0usize;
            fn count_and_leaves(e: &Expr, n: &mut usize) {
                if let Expr::BinaryOp {
                    op: BinaryOperator::And,
                    left,
                    right,
                } = e
                {
                    count_and_leaves(left, n);
                    count_and_leaves(right, n);
                } else {
                    *n += 1;
                }
            }
            count_and_leaves(wc, &mut leaves);
            if preds.len() != leaves {
                return Ok(None); // 有推不下去的 WHERE → 通用路径 (join 后过滤)
            }
            for (p, bare, op, lit) in preds {
                if !assign_pred(&mut tables, &p, &bare, op, &lit) {
                    return Ok(None);
                }
            }
        }

        // 每步 ON: 常量 (true 继续 / falsy → 计数 0) 或单表谓词; 跨表 → decline。
        let zero = |col: String| {
            Some((
                vec![col],
                vec![vec![Value::Integer(0)]],
            ))
        };
        for (_, _, on) in &steps {
            let mut leaves: Vec<&Expr> = Vec::new();
            fn flatten<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
                if let Expr::BinaryOp {
                    op: BinaryOperator::And,
                    left,
                    right,
                } = e
                {
                    flatten(left, out);
                    flatten(right, out);
                } else {
                    out.push(e);
                }
            }
            flatten(on, &mut leaves);
            for leaf in leaves {
                if Self::is_constant_expr(leaf) {
                    // 与通用路径同语义: eval → to_bool → falsy 即不匹配。
                    let v = self
                        .evaluator
                        .eval(leaf, &crate::types::SqlRow::new())
                        .ok()
                        .and_then(|v| self.to_bool(&v).ok())
                        .unwrap_or(false);
                    if !v {
                        return Ok(zero(Self::expr_to_column_name(agg_expr)));
                    }
                    continue;
                }
                // 单表谓词: `prefix.col op literal`
                if let Expr::BinaryOp { left, op, right } = leaf {
                    let ok_op = matches!(
                        op,
                        BinaryOperator::Eq
                            | BinaryOperator::Ne
                            | BinaryOperator::Lt
                            | BinaryOperator::Gt
                            | BinaryOperator::Le
                            | BinaryOperator::Ge
                    );
                    if let (Expr::Column(c), Expr::Literal(v), true) =
                        (left.as_ref(), right.as_ref(), ok_op)
                    {
                        if let Some((p, bare)) = c.split_once('.') {
                            if !assign_pred(&mut tables, p, bare, op.clone(), v) {
                                return Ok(None);
                            }
                            continue;
                        }
                    }
                }
                return Ok(None); // 跨表/复杂 ON → 等值有 hash, 其余走通用路径
            }
        }

        // 计数: 无谓词 O(1) 原子计数器 (缺则投影空扫), 有谓词投影扫描过滤。
        let mut product: u128 = 1;
        for (table, _, preds, schema) in &tables {
            let n: u64 = if preds.is_empty() {
                match self.db.fast_row_count(table) {
                    Some(n) => n,
                    None => {
                        let proj: Vec<usize> = Vec::new();
                        self.scan_table_rows_fast_projected(table, schema, Some(&proj))?
                            .len() as u64
                    }
                }
            } else {
                let mut positions: Vec<usize> = preds.iter().map(|(p, _, _)| *p).collect();
                positions.sort_unstable();
                positions.dedup();
                let pos_to_slot: Vec<Option<usize>> = (0..schema.columns.len())
                    .map(|i| positions.iter().position(|&p| p == i))
                    .collect();
                let rows = self.scan_table_rows_fast_projected(table, schema, Some(&positions))?;
                rows.iter()
                    .filter(|(_, row)| {
                        preds.iter().all(|(p, op, lit)| {
                            let slot = pos_to_slot[*p].unwrap();
                            apply_op_value(op, row.get(slot), lit)
                        })
                    })
                    .count() as u64
            };
            if n == 0 {
                return Ok(zero(Self::expr_to_column_name(agg_expr)));
            }
            product *= n as u128;
            if product > i64::MAX as u128 {
                product = i64::MAX as u128; // 饱和 (旧路径 u64 计数同样溢出语义)
                break;
            }
        }
        Ok(Some((
            vec![Self::expr_to_column_name(agg_expr)],
            vec![vec![Value::Integer(product as i64)]],
        )))
    }

    /// 🚀 Multi-way INNER equi-join (3+ tables). Flattens a left-deep FROM
    /// chain into successive hash joins over concatenated positional rows.
    /// The general path nested-loop evaluated every candidate row pair — a
    /// bounded 10K-row three-way join ran MINUTES. Also covers JOIN +
    /// simple GROUP BY/aggregate shapes (COUNT/SUM/AVG/MIN/MAX on plain
    /// columns, plain-column keys), which previously materialized every
    /// joined row as a HashMap SqlRow (~340 ms for 200K × 5K).
    /// 2-table chains keep try_positional_inner_join (PK-index path).
    /// TableRef 的有效前缀 (alias 或表名); 复合形状返回空串 → 不下推。
    pub(super) fn table_ref_alias(tr: &crate::sql::ast::TableRef) -> String {
        use crate::sql::ast::TableRef;
        match tr {
            TableRef::Table { name, alias } => {
                alias.clone().unwrap_or_else(|| name.clone())
            }
            _ => String::new(),
        }
    }

    /// WHERE 中可下推的单表谓词: AND 链里的 `alias.col op literal`。
    /// 返回 (限定前缀, 裸列名, op, literal)。OR/NOT/LIKE/IN/BETWEEN/表达式/
    /// bare 列名歧义形状不返回 — 留给 join 后过滤, 语义不变。
    /// 🔑 背景: join 快路径曾先物化全表叉积再过滤 (5K×500 自 join
    /// 11.4s + GB 级 RSS, 资源测评挖出)。
    pub(super) fn extract_pushdown_preds(
        wc: &Expr,
    ) -> Vec<(String, String, crate::sql::ast::BinaryOperator, Value)> {
        use crate::sql::ast::BinaryOperator;
        type Out = Vec<(String, String, BinaryOperator, Value)>;
        fn walk(e: &Expr, out: &mut Out) {
            match e {
                Expr::BinaryOp {
                    left,
                    op: BinaryOperator::And,
                    right,
                } => {
                    walk(left, out);
                    walk(right, out);
                }
                Expr::BinaryOp { left, op, right } => {
                    let ok_op = matches!(
                        op,
                        BinaryOperator::Eq
                            | BinaryOperator::Ne
                            | BinaryOperator::Lt
                            | BinaryOperator::Gt
                            | BinaryOperator::Le
                            | BinaryOperator::Ge
                    );
                    if let (Expr::Column(c), Expr::Literal(v), true) =
                        (left.as_ref(), right.as_ref(), ok_op)
                    {
                        if let Some((p, bare)) = c.split_once('.') {
                            out.push((p.to_string(), bare.to_string(), op.clone(), v.clone()));
                        }
                    }
                }
                _ => {}
            }
        }
        let mut out = Vec::new();
        walk(wc, &mut out);
        out
    }

    /// 把 `alias.col op literal` 谓词映射到投影行的列下标。
    /// 列必须在投影内 (needed-collection 已保证 WHERE 列入选); 不在则丢弃该谓词
    /// (留在 join 后过滤)。
    pub(super) fn map_pushdown_preds(
        preds: &[(String, String, crate::sql::ast::BinaryOperator, Value)],
        prefix: &str,
        schema: &TableSchema,
        proj: &[usize],
    ) -> Vec<(usize, crate::sql::ast::BinaryOperator, Value)> {
        preds
            .iter()
            .filter(|(p, _, _, _)| p == prefix)
            .filter_map(|(_, bare, op, lit)| {
                let spos = schema.get_column_position(bare)?;
                let pidx = proj.iter().position(|&x| x == spos)?;
                Some((pidx, op.clone(), lit.clone()))
            })
            .collect()
    }

    /// VEC M3 接线: 2 表 INNER equi-join + GROUP BY 形状 → 批 hash join。
    /// 解析 ON 的等值对、两侧 store/schema, 选小侧 build。
    pub(super) fn try_vec_equi_join_gb(
        &self,
        stmt: &SelectStmt,
        left: &crate::sql::ast::TableRef,
        right: &crate::sql::ast::TableRef,
        on_condition: &Expr,
    ) -> Result<Option<crate::sql::vector_exec::VecJoinGbOutcome>> {
        use crate::sql::ast::TableRef;
        if !crate::sql::vector_exec::vec_enabled() {
            return Ok(None);
        }
        let (TableRef::Table { name: lt, alias: la, .. }, TableRef::Table { name: rt, alias: ra, .. }) =
            (left, right)
        else {
            return Ok(None);
        };
        let (Some(lschema), Some(rschema)) = (
            self.db.get_table_schema(lt).ok(),
            self.db.get_table_schema(rt).ok(),
        ) else {
            return Ok(None);
        };
        if !self.db.has_col_segment_store(lt) || !self.db.has_col_segment_store(rt) {
            return Ok(None);
        }
        let (Some(lstore), Some(rstore)) = (
            self.db.get_or_create_col_segment_store(lt, &[]).ok(),
            self.db.get_or_create_col_segment_store(rt, &[]).ok(),
        ) else {
            return Ok(None);
        };
        let _ = lstore.prepare_for_query();
        let _ = rstore.prepare_for_query();
        // ON: 单等值对 l.col = r.col
        let Some((lc, rc)) = self.extract_equi_join_columns(on_condition) else {
            return Ok(None);
        };
        // 解析到各自 schema 的位置 + 归属别名
        let lalias = la.clone().unwrap_or_else(|| lt.clone());
        let ralias = ra.clone().unwrap_or_else(|| rt.clone());
        let resolve = |c: &str, schema: &crate::types::TableSchema| -> Option<usize> {
            if let Some((p, bare)) = c.split_once('.') {
                let _ = p;
                schema.get_column_position(bare)
            } else {
                schema.get_column_position(c)
            }
        };
        let (lpos, rpos) = match (resolve(&lc, &lschema), resolve(&rc, &rschema)) {
            (Some(a), Some(b)) => (a, b),
            _ => return Ok(None),
        };
        // 小侧 build (段行数和)
        let lrows: usize = lstore.segments_snapshot().iter().map(|s| s.row_count).sum();
        let rrows: usize = rstore.segments_snapshot().iter().map(|s| s.row_count).sum();
        let outcome = if lrows <= rrows {
            crate::sql::vector_exec::try_vec_equi_join_gb(
                (&lstore, &lschema, &lalias),
                (&rstore, &rschema, &ralias),
                lpos,
                rpos,
                stmt,
            )?
        } else {
            crate::sql::vector_exec::try_vec_equi_join_gb(
                (&rstore, &rschema, &ralias),
                (&lstore, &lschema, &lalias),
                rpos,
                lpos,
                stmt,
            )?
        };
        // 🔑 不调 release_pages_only — 它清空批缓存, Utf8 列每次重建
        // 100K Arc (曾使每查询 +7ms)。批缓存受 col_cache_budget 管控。
        Ok(outcome)
    }

    pub(super) fn try_multi_way_inner_join(&self, stmt: &SelectStmt) -> Result<Option<QueryResult>> {
        use crate::sql::ast::SelectColumn;
        use std::collections::HashMap;

        if stmt.distinct || stmt.having.is_some() || stmt.latest_by.is_some() {
            return Ok(None);
        }
        let from = match stmt.from.as_ref() {
            Some(f) => f,
            None => return Ok(None),
        };
        let ((btable, balias), steps) = match Self::flatten_left_deep_inner(from) {
            Some(x) => x,
            None => return Ok(None),
        };
        // 2-table chains with GROUP BY / aggregates also land here: the
        // existing 2-table fast path only handles plain projections, and the
        // general path materializes every joined row as a HashMap SqlRow
        // (~340 ms at 200K × 5K). Plain-projection 2-table chains keep the
        // existing path (PK-index nested loop).
        if steps.len() < 2 && stmt.group_by.is_none() && !self.has_aggregates(&stmt.columns) {
            return Ok(None);
        }

        // ---- column pruning: collect every column referenced anywhere in the
        // statement (SELECT exprs, WHERE, GROUP BY, ORDER BY, each ON) and scan
        // each table projected to just those columns. The un-pruned path
        // materialized full-width rows of BOTH tables — on the 100K-row
        // competitor table that decoded 153MB of VECTOR Values for a query
        // referencing only `device`/`zone` (138 ms, +300MB RSS).
        let mut needed: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut prune_ok = true;
        {
            fn collect_into(
                e: &Expr,
                needed: &mut std::collections::HashSet<String>,
                prune_ok: &mut bool,
            ) {
                let mut names = Vec::new();
                if QueryExecutor::collect_column_names_strict(e, &mut names) {
                    for n in names {
                        let bare = n.rsplit('.').next().unwrap_or(&n);
                        needed.insert(bare.to_string());
                    }
                } else {
                    *prune_ok = false;
                }
            }
            if let Some(ref wc) = stmt.where_clause {
                collect_into(wc, &mut needed, &mut prune_ok);
            }
            if let Some(gb) = &stmt.group_by {
                for g in gb {
                    needed.insert(g.rsplit('.').next().unwrap_or(g).to_string());
                }
            }
            if let Some(ob) = &stmt.order_by {
                for item in ob {
                    collect_into(&item.expr, &mut needed, &mut prune_ok);
                }
            }
            for sc in &stmt.columns {
                match sc {
                    SelectColumn::Column(c) | SelectColumn::ColumnWithAlias(c, _) => {
                        needed.insert(c.rsplit('.').next().unwrap_or(c).to_string());
                    }
                    SelectColumn::Expr(e, _) => collect_into(e, &mut needed, &mut prune_ok),
                    SelectColumn::Star => prune_ok = false,
                }
            }
            for (_, _, on) in &steps {
                match self.extract_equi_join_columns(on) {
                    Some((l, r)) => {
                        needed.insert(l.rsplit('.').next().unwrap_or(&l).to_string());
                        needed.insert(r.rsplit('.').next().unwrap_or(&r).to_string());
                    }
                    None => prune_ok = false,
                }
            }
        }
        let prune = prune_ok && !needed.is_empty();

        // ---- accumulate the join product as concatenated positional rows
        let bschema = match self.db.get_table_schema(&btable) {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };
        let bprefix = balias.unwrap_or_else(|| btable.clone());
        // Positions of base-table columns kept in the product (all if no prune).
        let bproj: Vec<usize> = if prune {
            bschema
                .columns
                .iter()
                .enumerate()
                .filter(|(_, c)| needed.contains(c.name.as_str()))
                .map(|(i, _)| i)
                .collect()
        } else {
            (0..bschema.columns.len()).collect()
        };
        // 🔑 单表谓词下推: WHERE `alias.col op literal` 在扫描侧过滤,
        // 避免 join 先物化全表叉积再过滤 (资源测评: 5K×500 自 join
        // 曾 11.4s + GB 级 RSS)。
        let push_preds = stmt
            .where_clause
            .as_ref()
            .map(Self::extract_pushdown_preds)
            .unwrap_or_default();
        let bpd = Self::map_pushdown_preds(&push_preds, &bprefix, &bschema, &bproj);
        let mut acc_rows: Vec<Vec<Value>> = self
            .scan_table_rows_fast_projected(&btable, &bschema, Some(&bproj))?
            .into_iter()
            .map(|(_, r)| r)
            .filter(|row| {
                bpd.iter()
                    .all(|(p, op, t)| apply_op_value(op, row.get(*p), t))
            })
            .collect();
        // Qualified column names + types of the accumulated product.
        let mut acc_cols: Vec<String> = Vec::with_capacity(64);
        let mut acc_types: Vec<ColumnType> = Vec::with_capacity(64);
        let mut acc_prefixes: Vec<String> = vec![bprefix.clone()];
        for &p in &bproj {
            let c = &bschema.columns[p];
            acc_cols.push(format!("{}.{}", bprefix, c.name));
            acc_types.push(c.col_type.clone());
        }
        // Resolve a column reference against the accumulated product: exact
        // qualified match first, then a UNIQUE bare-suffix match.
        fn acc_resolve_in(cols: &[String], name: &str) -> Option<usize> {
            if let Some(p) = cols.iter().position(|c| c == name) {
                return Some(p);
            }
            let bare = name.rsplit('.').next().unwrap_or(name);
            let mut hit = None;
            for (i, c) in cols.iter().enumerate() {
                if c.rsplit('.').next().unwrap_or(c) == bare {
                    if hit.is_some() {
                        return None; // ambiguous bare name
                    }
                    hit = Some(i);
                }
            }
            hit
        }
        // (acc_cols mutates per step — resolve via the free fn with the live slice)

        for (jtable, jalias, on) in &steps {
            let jschema = match self.db.get_table_schema(jtable) {
                Ok(s) => s,
                Err(_) => return Ok(None),
            };
            let jprefix = jalias.clone().unwrap_or_else(|| jtable.clone());
            // 🔑 ON 合取拆分: `ON a.k = b.k AND b.x < N` → (等值对叶, 残余条件)。
            // 此前带 AND 的 ON 直接 decline → 通用嵌套循环每候选行建 SqlRow,
            // 100K×64-dev 的三表链 20min+ 跑不完 (资源测评)。残余里只引用
            // 新表的 `col op literal` 预过滤扫描; 其余在 probe 循环对合并行
            // 用 eval_expr_on_row 求值 (语义与通用路径 eval(on) 一致)。
            let (on_equi, on_residual): (Expr, Vec<Expr>) = {
                use crate::sql::ast::BinaryOperator;
                let mut leaves: Vec<Expr> = Vec::new();
                fn flatten_and(e: &Expr, out: &mut Vec<Expr>) {
                    if let Expr::BinaryOp {
                        left,
                        op: crate::sql::ast::BinaryOperator::And,
                        right,
                    } = e
                    {
                        flatten_and(left, out);
                        flatten_and(right, out);
                    } else {
                        out.push(e.clone());
                    }
                }
                flatten_and(on, &mut leaves);
                let _ = BinaryOperator::And;
                let mut equi: Option<Expr> = None;
                let mut residual: Vec<Expr> = Vec::new();
                for leaf in leaves {
                    if equi.is_none()
                        && self.extract_equi_join_columns(&leaf).is_some()
                    {
                        equi = Some(leaf);
                    } else {
                        residual.push(leaf);
                    }
                }
                match equi {
                    Some(e) => (e, residual),
                    // 无等值对: 若原 ON 不是合取则保持原语义; 合取但无对 → 通用路径
                    None => (on.clone(), Vec::new()),
                }
            };
            let has_on_equi = self.extract_equi_join_columns(&on_equi).is_some();
            if !has_on_equi {
                return Ok(None); // non-equi ON → general path
            }
            // Equi pair from ON: exactly one side must resolve against the
            // NEW table, the other against the accumulated product.
            let (lcol, rcol) = match self.extract_equi_join_columns(&on_equi) {
                Some(p) => p,
                None => return Ok(None), // non-equi ON → general path
            };
            let jresolve = |name: &str| -> Option<usize> {
                if let Some((p, bare)) = name.split_once('.') {
                    if p != jprefix {
                        return None;
                    }
                    return jschema.get_column_position(bare);
                }
                jschema.get_column_position(name)
            };
            let (acc_pos, j_pos) = match (acc_resolve_in(&acc_cols, &lcol), jresolve(&lcol)) {
                (Some(a), None) => match jresolve(&rcol) {
                    Some(j) => (a, j),
                    None => return Ok(None),
                },
                (None, Some(j)) => match acc_resolve_in(&acc_cols, &rcol) {
                    Some(a) => (a, j),
                    None => return Ok(None),
                },
                _ => return Ok(None), // ambiguous / unresolved
            };
            let _ = j_pos;

            // Projected scan of the joined table (see the pruning block above).
            let jproj: Vec<usize> = if prune {
                jschema
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| needed.contains(c.name.as_str()))
                    .map(|(i, _)| i)
                    .collect()
            } else {
                (0..jschema.columns.len()).collect()
            };
            // The ON column's position within the PROJECTED row.
            let Some(j_idx) = jproj.iter().position(|&p| p == j_pos) else {
                return Ok(None);
            };
            // 🔑 ON 残余里只引用新表的 `col op literal` → 投影位映射后预过滤扫描
            let on_jpd: Vec<(usize, crate::sql::ast::BinaryOperator, Value)> = {
                use crate::sql::ast::{BinaryOperator, Expr};
                let mut raw: Vec<(usize, BinaryOperator, Value)> = Vec::new();
                for e in &on_residual {
                    if let Expr::BinaryOp { left, op, right } = e {
                        let ok_op = matches!(
                            op,
                            BinaryOperator::Eq
                                | BinaryOperator::Ne
                                | BinaryOperator::Lt
                                | BinaryOperator::Gt
                                | BinaryOperator::Le
                                | BinaryOperator::Ge
                        );
                        if let (Expr::Column(cn), Expr::Literal(v), true) =
                            (left.as_ref(), right.as_ref(), ok_op)
                        {
                            if let Some((p, bare)) = cn.split_once('.') {
                                if p == jprefix {
                                    if let Some(pos) = jschema.get_column_position(bare) {
                                        raw.push((pos, op.clone(), v.clone()));
                                    }
                                }
                            }
                        }
                    }
                }
                raw.into_iter()
                    .filter_map(|(spos, op, v)| {
                        let pidx = jproj.iter().position(|&x| x == spos)?;
                        Some((pidx, op, v))
                    })
                    .collect()
            };
            let jpd = Self::map_pushdown_preds(&push_preds, jprefix.as_str(), &jschema, &jproj);
            let jrows: Vec<(u64, Vec<Value>)> = self
                .scan_table_rows_fast_projected(jtable, &jschema, Some(&jproj))?
                .into_iter()
                .filter(|(_, row)| {
                    // 🔑 该表的单表谓词下推 (WHERE) + ON 残余单表谓词
                    jpd.iter()
                        .chain(on_jpd.iter())
                        .all(|(p, op, t)| apply_op_value(op, row.get(*p), t))
                })
                .collect();
            // 🔑 合并行合成 schema (acc 限定列 + 本步 j 限定列) — ON 残余求值用
            let step_schema: TableSchema = if on_residual.is_empty() {
                (*jschema).clone()
            } else {
                let mut sc = (*jschema).clone();
                sc.columns = acc_cols
                    .iter()
                    .zip(acc_types.iter())
                    .enumerate()
                    .map(|(i, (n, t))| crate::types::ColumnDef {
                        name: n.clone(),
                        col_type: t.clone(),
                        position: i,
                        nullable: true,
                        auto_increment: false,
                        auto_increment_start: None,
                        default_value: None,
                    })
                    .collect();
                for (pi, &p) in jproj.iter().enumerate() {
                    let c = &jschema.columns[p];
                    sc.columns.push(crate::types::ColumnDef {
                        name: format!("{}.{}", jprefix, c.name),
                        col_type: c.col_type.clone(),
                        position: acc_cols.len() + pi,
                        nullable: true,
                        auto_increment: false,
                        auto_increment_start: None,
                        default_value: None,
                    });
                }
                sc.rebuild_column_map();
                sc
            };
            // Build the hash side on the NEW table (probe accumulated rows).
            let mut hash: HashMap<&Value, Vec<usize>> = HashMap::with_capacity(jrows.len());
            for (ri, (_, rrow)) in jrows.iter().enumerate() {
                if let Some(v) = rrow.get(j_idx) {
                    hash.entry(v).or_default().push(ri);
                }
            }
            let mut next: Vec<Vec<Value>> = Vec::with_capacity(acc_rows.len());
            for arow in &acc_rows {
                let Some(key) = arow.get(acc_pos) else {
                    continue;
                };
                // 🔑 NULL join keys never match (SQL: NULL = NULL is UNKNOWN).
                if matches!(key, Value::Null) {
                    continue;
                }
                if let Some(matches) = hash.get(key) {
                    for &ri in matches {
                        let mut combined = arow.clone();
                        combined.extend(jrows[ri].1.iter().cloned());
                        // 🔑 ON 残余条件 (非等值部分) 对合并行求值 — 与通用
                        // 路径 eval(on_condition, combined) 同语义。
                        if !on_residual.is_empty()
                            && !on_residual.iter().all(|e| {
                                matches!(
                                    Self::eval_expr_on_row(e, &combined, &step_schema),
                                    Ok(Value::Bool(true))
                                )
                            })
                        {
                            continue;
                        }
                        next.push(combined);
                    }
                }
            }
            acc_rows = next;
            for &p in &jproj {
                let c = &jschema.columns[p];
                acc_cols.push(format!("{}.{}", jprefix, c.name));
                acc_types.push(c.col_type.clone());
            }
            acc_prefixes.push(jprefix);
            if acc_rows.is_empty() {
                break;
            }
        }
        let _ = &acc_prefixes;

        // ---- synthetic schema over the joined product (qualified names)
        let acc_schema: TableSchema = {
            let mut s: TableSchema = (*bschema).clone();
            s.columns.clear();
            for (i, (name, ct)) in acc_cols.iter().zip(acc_types.iter()).enumerate() {
                let mut cd = crate::types::ColumnDef::new(name.clone(), ct.clone(), i);
                cd.position = i;
                s.columns.push(cd);
            }
            s.rebuild_column_map();
            s
        };

        // ---- WHERE on the joined product
        let filtered: Vec<Vec<Value>> = if let Some(ref wc) = stmt.where_clause {
            // 🚀 Simple-comparison AND chains filter positionally (no per-row
            // expression interpretation). acc_schema's columns are qualified
            // ("e.device"), so prefixed references resolve exactly; bare
            // references only parse when unambiguous in the synthetic schema.
            // 🚨 Boolean columns/literals are excluded: the interpreter path
            // coerces `flag = 1` per SQL bool/int semantics, positional
            // Value equality does not.
            let comps = Self::parse_where_comparisons(wc, &acc_schema).filter(|comps| {
                comps.iter().all(|(p, _, t)| {
                    let boolish_col =
                        matches!(acc_types.get(*p), Some(ColumnType::Boolean));
                    let boolish_target = matches!(t, Value::Bool(_));
                    !boolish_col && !boolish_target
                })
            });
            if let Some(comps) = comps {
                acc_rows
                    .into_iter()
                    .filter(|row| {
                        comps
                            .iter()
                            .all(|(p, op, t)| apply_op_value(op, row.get(*p), t))
                    })
                    .collect()
            } else {
                acc_rows
                    .into_iter()
                    .filter(|row| {
                        matches!(
                            Self::eval_expr_on_row(wc, row, &acc_schema),
                            Ok(Value::Bool(true))
                        )
                    })
                    .collect()
            }
        } else {
            acc_rows
        };

        // ---- aggregate / GROUP BY variant
        if stmt.group_by.is_some() || self.has_aggregates(&stmt.columns) {
            // Keys: plain columns; aggregates: plain-column COUNT/SUM/AVG/MIN/MAX.
            let group_pos: Option<Vec<usize>> = match &stmt.group_by {
                None => Some(vec![]),
                Some(items) => {
                    let mut v = Vec::with_capacity(items.len());
                    for it in items {
                        match acc_resolve_in(&acc_cols, it) {
                            Some(p) => v.push(p),
                            None => return Ok(None),
                        }
                    }
                    Some(v)
                }
            };
            let group_pos = match group_pos {
                Some(g) => g,
                None => return Ok(None),
            };
            struct Agg {
                func: String,
                pos: Option<usize>,
            }
            let mut aggs: Vec<Agg> = Vec::new();
            let mut out_names: Vec<String> = Vec::new();
            let mut key_out_pos: Vec<(usize, usize)> = Vec::new(); // (group idx, out idx)
            for sc in &stmt.columns {
                match sc {
                    SelectColumn::Expr(e, alias) => {
                        // Resolve the aggregate MANUALLY against the joined
                        // product: try_parse_aggregate strips table qualifiers
                        // before lookup, but the acc columns ARE qualified
                        // ("o.amt") — SUM(o.amt) resolved to no column and
                        // returned NULL.
                        let Expr::FunctionCall {
                            name,
                            args,
                            distinct,
                            ..
                        } = e
                        else {
                            return Ok(None);
                        };
                        let func = name.to_uppercase();
                        if *distinct
                            || !matches!(func.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
                        {
                            return Ok(None);
                        }
                        let pos = match args.first() {
                            None => None,                              // COUNT()
                            Some(Expr::Column(c)) if c == "*" => None, // COUNT(*)
                            Some(Expr::Column(c)) => match acc_resolve_in(&acc_cols, c) {
                                Some(p) => Some(p),
                                None => return Ok(None),
                            },
                            _ => return Ok(None),
                        };
                        aggs.push(Agg { func, pos });
                        out_names.push(
                            alias
                                .clone()
                                .unwrap_or_else(|| Self::expr_to_column_name(e)),
                        );
                    }
                    SelectColumn::Column(c) => {
                        let Some(p) = acc_resolve_in(&acc_cols, c) else {
                            return Ok(None);
                        };
                        let Some(gi) = group_pos.iter().position(|&g| g == p) else {
                            return Ok(None); // bare column must be a group key
                        };
                        key_out_pos.push((gi, out_names.len()));
                        out_names.push(c.clone());
                    }
                    _ => return Ok(None),
                }
            }
            if aggs.is_empty() {
                return Ok(None);
            }
            #[derive(Clone)]
            struct Acc {
                count: u64,
                nn: Vec<u64>,
                sum: Vec<f64>,
                minv: Vec<Option<f64>>,
                maxv: Vec<Option<f64>>,
            }
            let k = aggs.len();
            let new_acc = |k: usize| Acc {
                count: 0,
                nn: vec![0; k],
                sum: vec![0.0; k],
                minv: vec![None; k],
                maxv: vec![None; k],
            };
            let mut groups: HashMap<Vec<Value>, Acc> = HashMap::with_capacity(256);
            let mut order_keys: Vec<Vec<Value>> = Vec::with_capacity(256);
            for row in &filtered {
                let key: Vec<Value> = group_pos
                    .iter()
                    .map(|&p| row.get(p).cloned().unwrap_or(Value::Null))
                    .collect();
                if !groups.contains_key(&key) {
                    order_keys.push(key.clone());
                    groups.insert(key.clone(), new_acc(k));
                }
                let acc = groups.get_mut(&key).expect("group just ensured");
                acc.count += 1;
                for (i, a) in aggs.iter().enumerate() {
                    let Some(p) = a.pos else { continue };
                    match row.get(p) {
                        Some(Value::Integer(x)) => {
                            acc.nn[i] += 1;
                            acc.sum[i] += *x as f64;
                            acc.minv[i] =
                                Some(acc.minv[i].map_or(*x as f64, |m: f64| m.min(*x as f64)));
                            acc.maxv[i] =
                                Some(acc.maxv[i].map_or(*x as f64, |m: f64| m.max(*x as f64)));
                        }
                        Some(Value::Float(x)) => {
                            acc.nn[i] += 1;
                            acc.sum[i] += *x;
                            acc.minv[i] = Some(acc.minv[i].map_or(*x, |m: f64| m.min(*x)));
                            acc.maxv[i] = Some(acc.maxv[i].map_or(*x, |m: f64| m.max(*x)));
                        }
                        // COUNT(col) counts every NON-NULL value — including
                        // TEXT/TIMESTAMP/BOOL, which carry no numeric fold.
                        Some(Value::Null) | None => {}
                        Some(_) => {
                            acc.nn[i] += 1;
                        }
                    }
                }
            }
            let finish = |acc: &Acc| -> Vec<Value> {
                aggs.iter()
                    .enumerate()
                    .map(|(i, a)| match a.func.as_str() {
                        "COUNT" => Value::Integer(if a.pos.is_none() {
                            acc.count as i64
                        } else {
                            acc.nn[i] as i64
                        }),
                        "SUM" => {
                            if acc.nn[i] > 0 {
                                Value::Float(acc.sum[i])
                            } else {
                                Value::Null
                            }
                        }
                        "AVG" => {
                            if acc.nn[i] > 0 {
                                Value::Float(acc.sum[i] / acc.nn[i] as f64)
                            } else {
                                Value::Null
                            }
                        }
                        "MIN" => acc.minv[i].map(Value::Float).unwrap_or(Value::Null),
                        _ => acc.maxv[i].map(Value::Float).unwrap_or(Value::Null),
                    })
                    .collect()
            };
            let mut rows: Vec<Vec<Value>> = Vec::with_capacity(order_keys.len());
            // 🔑 SQL standard: an ungrouped aggregate over an EMPTY set
            // returns ONE row (COUNT → 0, SUM/AVG/MIN/MAX → NULL). An empty
            // join product used to return zero rows.
            if order_keys.is_empty() && group_pos.is_empty() {
                let vals = finish(&new_acc(k));
                let mut row = vec![Value::Null; out_names.len()];
                let mut vi = 0;
                for (oi, sc) in stmt.columns.iter().enumerate() {
                    if matches!(sc, SelectColumn::Expr(_, _)) {
                        row[oi] = vals[vi].clone();
                        vi += 1;
                    }
                }
                rows.push(row);
            }
            for key in order_keys {
                let acc = groups.remove(&key).unwrap_or_else(|| new_acc(k));
                let mut row = vec![Value::Null; out_names.len()];
                for (gi, oi) in &key_out_pos {
                    row[*oi] = key[*gi].clone();
                }
                let vals = finish(&acc);
                let mut vi = 0;
                for (oi, sc) in stmt.columns.iter().enumerate() {
                    if matches!(sc, SelectColumn::Expr(_, _)) {
                        row[oi] = vals[vi].clone();
                        vi += 1;
                    }
                }
                rows.push(row);
            }
            // ORDER BY on plain output columns or an aggregate that appears
            // in the SELECT list (matched by canonical name, e.g.
            // `ORDER BY COUNT(*) DESC`).
            if let Some(ref ob) = stmt.order_by {
                let mut specs: Vec<(usize, bool)> = Vec::new();
                for oe in ob {
                    let name = match &oe.expr {
                        Expr::Column(cn) => cn.clone(),
                        other => Self::expr_to_column_name(other),
                    };
                    let bare = name.rsplit('.').next().unwrap_or(&name);
                    // Match the output name, or the canonical name of the
                    // SELECT expression at that output position (the output
                    // may be aliased: `COUNT(*) AS n … ORDER BY COUNT(*)`).
                    // 🔑 限定名必须精确匹配; 裸名只允许唯一命中 (否则排序键
                    // 有歧义 → 回落通用路径)。bare-name 回退曾让
                    // `ORDER BY t.grp` 误匹配输出列 `i.grp` (differential fuzz)。
                    let out_match = if name.contains('.') {
                        out_names.iter().position(|n| n == &name)
                    } else {
                        let hits: Vec<usize> = out_names
                            .iter()
                            .enumerate()
                            .filter(|(_, n)| n.rsplit('.').next().unwrap_or(n) == bare)
                            .map(|(i, _)| i)
                            .collect();
                        if hits.len() == 1 { Some(hits[0]) } else { None }
                    };
                    let Some(p) = out_match.or_else(|| {
                        stmt.columns.iter().position(|sc| match sc {
                            SelectColumn::Expr(e, _) => Self::expr_to_column_name(e) == name,
                            _ => false,
                        })
                    }) else {
                        return Ok(None);
                    };
                    specs.push((p, oe.asc));
                }
                if !specs.is_empty() {
                    rows.sort_by(|a, b| {
                        for &(i, asc) in &specs {
                            let c = order_by_cmp(&a[i], &b[i]);
                            let c = if asc { c } else { c.reverse() };
                            if c != std::cmp::Ordering::Equal {
                                return c;
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
            return Ok(Some(QueryResult::Select {
                columns: out_names,
                rows,
            }));
        }

        // ---- projection variant (no aggregates)
        let (column_names, projected): (Vec<String>, Vec<Vec<Value>>) = if stmt.columns.len() == 1
            && matches!(stmt.columns[0], SelectColumn::Star)
        {
            (
                acc_cols
                    .iter()
                    .map(|c| c.rsplit('.').next().unwrap_or(c).to_string())
                    .collect(),
                filtered,
            )
        } else {
            let mut idxs: Vec<usize> = Vec::new();
            let mut names: Vec<String> = Vec::new();
            let mut claimed: std::collections::HashSet<usize> = std::collections::HashSet::new();
            for sc in &stmt.columns {
                match sc {
                    SelectColumn::Star => {
                        for (i, n) in acc_cols.iter().enumerate() {
                            idxs.push(i);
                            names.push(n.clone());
                        }
                    }
                    SelectColumn::Column(c) => {
                        let bare = c.rsplit('.').next().unwrap_or(c);
                        let p = acc_cols
                            .iter()
                            .enumerate()
                            .find(|(i, n)| {
                                !claimed.contains(i)
                                    && (*n == c || n.rsplit('.').next() == Some(bare))
                            })
                            .map(|(i, _)| i);
                        let Some(p) = p else { return Ok(None) };
                        claimed.insert(p);
                        idxs.push(p);
                        names.push(c.clone());
                    }
                    SelectColumn::ColumnWithAlias(c, a) => {
                        let bare = c.rsplit('.').next().unwrap_or(c);
                        let p = acc_cols
                            .iter()
                            .enumerate()
                            .find(|(i, n)| {
                                !claimed.contains(i)
                                    && (*n == c || n.rsplit('.').next() == Some(bare))
                            })
                            .map(|(i, _)| i);
                        let Some(p) = p else { return Ok(None) };
                        claimed.insert(p);
                        idxs.push(p);
                        names.push(a.clone());
                    }
                    SelectColumn::Expr(_, _) => return Ok(None), // expressions → general path
                }
            }
            (
                names,
                filtered
                    .into_iter()
                    .map(|r| idxs.iter().map(|&i| r[i].clone()).collect())
                    .collect(),
            )
        };

            // ORDER BY on projected output columns, then LIMIT/OFFSET.
            let mut rows = projected;
            if let Some(ref ob) = stmt.order_by {
                let mut specs: Vec<(usize, bool)> = Vec::new();
                for oe in ob {
                    let Expr::Column(cn) = &oe.expr else {
                        return Ok(None);
                    };
                    // 🔑 限定名必须精确匹配输出列; 裸名只允许唯一命中。
                    // 此前 bare-name 回退让 `ORDER BY b.id` 误匹配输出列
                    // `a.id` (同为裸名 "id")，第二排序键实际排的是 a.id —
                    // 方向丢失、次序不稳定 (differential fuzz 抓出)。
                    let p = if cn.contains('.') {
                        column_names.iter().position(|n| n == cn)
                    } else {
                        let hits: Vec<usize> = column_names
                            .iter()
                            .enumerate()
                            .filter(|(_, n)| n.rsplit('.').next().unwrap_or(n) == cn)
                            .map(|(i, _)| i)
                            .collect();
                        if hits.len() == 1 { Some(hits[0]) } else { None }
                    };
                    let Some(p) = p else {
                        return Ok(None);
                    };
                    specs.push((p, oe.asc));
                }
            if !specs.is_empty() {
                rows.sort_by(|a, b| {
                    for &(i, asc) in &specs {
                        let c = order_by_cmp(&a[i], &b[i]);
                        let c = if asc { c } else { c.reverse() };
                        if c != std::cmp::Ordering::Equal {
                            return c;
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
        Ok(Some(QueryResult::Select {
            columns: column_names,
            rows,
        }))
    }

    /// INNER JOIN: only rows that match condition in both tables
    ///
    /// 🚀 Optimized with Hash Join for equi-joins

    /// 🔑 计算键 hash join: `ON <col> = <单表表达式>` (如 a.id = b.id - 1)。
    /// 无等值对时曾落 O(N×M) 嵌套循环逐候选建 SqlRow eval — 2K×20K 自 join
    /// 23.5 分钟 (热点扫描)。表达式侧逐行求值建 hash, 列侧探测。
    /// 仅处理单叶 Eq ON; 合取/其他形状由调用方既有路径负责。
    pub(super) fn try_expr_key_hash_join(
        &self,
        left_rows: &[(u64, SqlRow)],
        right_rows: &[(u64, SqlRow)],
        on_condition: &Expr,
    ) -> Result<Option<Vec<(u64, SqlRow)>>> {
        use crate::sql::ast::BinaryOperator;
        let Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } = on_condition
        else {
            return Ok(None);
        };
        // 形态: (Column, expr) 或 (expr, Column)
        let (col_name, expr) = match (left.as_ref(), right.as_ref()) {
            (Expr::Column(c), e) if !matches!(e, Expr::Column(_)) => (c.clone(), e.clone()),
            (e, Expr::Column(c)) if !matches!(e, Expr::Column(_)) => (c.clone(), e.clone()),
            _ => return Ok(None),
        };
        // 表达式的列引用必须全部落在同一侧 (否则无法建单侧 hash)
        let mut refs: Vec<String> = Vec::new();
        if !Self::collect_column_names_strict(&expr, &mut refs) || refs.is_empty() {
            return Ok(None);
        }
        let lk = left_rows.first().map(|(_, r)| r.clone());
        let rk = right_rows.first().map(|(_, r)| r.clone());
        let all_in = |keys: &Option<SqlRow>| {
            keys.as_ref()
                .map_or(false, |k| refs.iter().all(|c| k.contains_key(c)))
        };
        let (expr_left, col_side_right) = if all_in(&lk) && lk.as_ref().map_or(true, |k| !k.contains_key(&col_name) || rk.as_ref().map_or(true, |r| !r.contains_key(&col_name)) || true) {
            // 表达式在左; 列须在右 (或列在左也行? 严格: 列在另一侧才省事)
            let col_in_right = rk.as_ref().map_or(false, |k| k.contains_key(&col_name));
            if col_in_right {
                (true, true)
            } else if rk.as_ref().map_or(false, |k| refs.iter().all(|c| k.contains_key(c))) {
                (false, false) // 表达式在右, 列在左
            } else {
                return Ok(None);
            }
        } else if all_in(&rk) {
            let col_in_left = lk.as_ref().map_or(false, |k| k.contains_key(&col_name));
            if !col_in_left {
                return Ok(None);
            }
            (false, false)
        } else {
            return Ok(None);
        };
        let _ = expr_left;
        let _ = col_side_right;
        // 统一: expr_rows 建 hash, col_rows 探测
        let (expr_rows, col_rows, expr_is_left) = {
            let e_in_l = all_in(&lk);
            if e_in_l {
                (left_rows, right_rows, true)
            } else {
                (right_rows, left_rows, false)
            }
        };
        #[derive(Hash, PartialEq, Eq)]
        enum HK {
            Num(u64),
            Text(String),
            Bool(bool),
        }
        let to_hk = |v: &Value| -> Option<HK> {
            match v {
                Value::Integer(i) => {
                    if *i >= -(1i64 << 53) && *i <= (1i64 << 53) {
                        Some(HK::Num((*i as f64).to_bits()))
                    } else {
                        Some(HK::Num((*i as u64).wrapping_add(i64::MIN as u64)))
                    }
                }
                Value::Float(f) => Some(HK::Num(f.to_bits())),
                Value::Text(t) => Some(HK::Text(t.to_string())),
                Value::Bool(b) => Some(HK::Bool(*b)),
                _ => None,
            }
        };
        use std::collections::HashMap;
        let mut hash: HashMap<HK, Vec<usize>> = HashMap::with_capacity(expr_rows.len());
        for (ri, (_, row)) in expr_rows.iter().enumerate() {
            if let Ok(v) = self.evaluator.eval(&expr, row) {
                if let Some(k) = to_hk(&v) {
                    hash.entry(k).or_default().push(ri);
                }
            }
        }
        let mut out: Vec<(u64, SqlRow)> = Vec::new();
        let mut next_id = 1u64;
        for (_, crow) in col_rows {
            let Some(cv) = crow.get(&col_name) else {
                continue;
            };
            if matches!(cv, Value::Null) {
                continue;
            }
            let Some(hits) = to_hk(cv).and_then(|k| hash.get(&k).cloned()) else {
                continue;
            };
            for ri in hits {
                let (erow_id, erow) = &expr_rows[ri];
                let combined = if expr_is_left {
                    self.combine_rows(erow, crow)
                } else {
                    self.combine_rows(crow, erow)
                };
                out.push((next_id, combined));
                next_id += 1;
                let _ = erow_id;
            }
        }
        Ok(Some(out))
    }

    pub(super) fn inner_join(
        &self,
        left_rows: &[(u64, SqlRow)],
        right_rows: &[(u64, SqlRow)],
        on_condition: &Expr,
    ) -> Result<Vec<(u64, SqlRow)>> {
        // 🔑 ON 合取: 双侧单表残余预过滤后等值走 hash (同 left_join)。
        let (on_equi, on_residual) = Self::split_on_conjunction(on_condition);
        if self.extract_equi_join_columns(&on_equi).is_some() && !on_residual.is_empty() {
            // 内连接: 双侧残余都可预过滤
            let mut left_preds = Vec::new();
            let mut right_preds = Vec::new();
            let mut all_consumed = true;
            // SqlRow 的键即限定列名 — 用首行键集判定残余归属
            let lkeys = left_rows.first().map(|(_, r)| r.clone());
            let rkeys = right_rows.first().map(|(_, r)| r.clone());
            for r in &on_residual {
                if let Some((cn, op, v)) = Self::residual_single_table_pred(r) {
                    if lkeys.as_ref().map_or(false, |k| k.contains_key(&cn)) {
                        left_preds.push((cn, op, v));
                        continue;
                    }
                    if rkeys.as_ref().map_or(false, |k| k.contains_key(&cn)) {
                        right_preds.push((cn, op, v));
                        continue;
                    }
                }
                all_consumed = false;
                break;
            }
            if all_consumed {
                let lf: Vec<(u64, SqlRow)> = if left_preds.is_empty() {
                    left_rows.to_vec()
                } else {
                    left_rows
                        .iter()
                        .filter(|(_, row)| {
                            left_preds.iter().all(|(k, op, lit)| {
                                apply_op_value(op, row.get(k), lit)
                            })
                        })
                        .cloned()
                        .collect()
                };
                let rf: Vec<(u64, SqlRow)> = if right_preds.is_empty() {
                    right_rows.to_vec()
                } else {
                    right_rows
                        .iter()
                        .filter(|(_, row)| {
                            right_preds.iter().all(|(k, op, lit)| {
                                apply_op_value(op, row.get(k), lit)
                            })
                        })
                        .cloned()
                        .collect()
                };
                let (lc, rc) = self.extract_equi_join_columns(&on_equi).unwrap();
                return self.hash_join_inner(&lf, &rf, &lc, &rc);
            }
        }
        // Try to detect equi-join (col1 = col2) for Hash Join optimization.
        // 🔑 仅当残余为空 (纯等值 ON) — 带未消耗残余时 hash 会丢掉残余条件
        // (自 join `t2.tag = t.tag` 全部误匹配, fuzz 抓出)。
        if on_residual.is_empty() {
            if let Some((left_col, right_col)) = self.extract_equi_join_columns(&on_equi) {
                // 🚀 Use Hash Join (O(N + M))
                return self.hash_join_inner(left_rows, right_rows, &left_col, &right_col);
            }
            // 🔑 计算键 hash: `col = 单表表达式` (a.id = b.id - 1) — 此前
            // 嵌套循环 O(N×M) 逐候选建 SqlRow (2K×20K 自 join 23.5min)。
            if let Some(joined) =
                self.try_expr_key_hash_join(left_rows, right_rows, &on_equi)?
            {
                return Ok(joined);
            }
        }

        // Fallback: Nested Loop Join (O(N × M))
        let mut result = Vec::new();
        let mut next_id = 1u64;

        for (_, left_row) in left_rows {
            for (_, right_row) in right_rows {
                // Combine rows
                let combined_row = self.combine_rows(left_row, right_row);

                // Evaluate JOIN condition
                if self
                    .evaluator
                    .eval(on_condition, &combined_row)
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
    pub(super) fn hash_join_inner(
        &self,
        left_rows: &[(u64, SqlRow)],
        right_rows: &[(u64, SqlRow)],
        left_col: &str,
        right_col: &str,
    ) -> Result<Vec<(u64, SqlRow)>> {
        use std::collections::HashMap;

        // 🚨 Normalize ON operand order: extract_equi_join_columns returns
        // (left_col, right_col) in SYNTACTIC order. But `ON b.k = a.id`
        // (reversed) would make left_col=b.k (which is actually in the RIGHT
        // table) and right_col=a.id (LEFT table). The build/probe below would
        // then key the hash on a column absent from right_rows → empty result.
        // Detect this by sampling the row keys and swap if needed.
        let (left_col, right_col) =
            Self::normalize_join_columns(left_rows, right_rows, left_col, right_col);
        let left_col = left_col.as_str();
        let right_col = right_col.as_str();

        // Hash key type — preserves full i64 precision
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        enum HashKey {
            Numeric(u64), // f64::to_bits() for Float and small Integer (< 2^53)
            Integer(u64), // i64::to_bits() for Integer, preserves full 64-bit range
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
                    const EXACT_MAX: i64 = 1i64 << 53; // 2^53, max exact i64 in f64
                    if *i >= -EXACT_MAX && *i <= EXACT_MAX {
                        Some(HashKey::Numeric((*i as f64).to_bits()))
                    } else {
                        Some(HashKey::Integer((*i as u64).wrapping_add(i64::MIN as u64)))
                    }
                }
                // Normalize -0.0 → +0.0 so they hash/match as equal (IEEE-754:
                // 0.0 == -0.0, but their bit patterns differ). Adding 0.0 turns
                // -0.0 into +0.0; non-zero values are unchanged.
                Value::Float(f) => Some(HashKey::Numeric((f + 0.0).to_bits())),
                Value::Text(s) => Some(HashKey::Text(s.to_string())),
                Value::Bool(b) => Some(HashKey::Bool(*b)),
                Value::Null => None, // SQL: NULL != NULL in joins
                // 🚨 Timestamp: hash on micros (matches Integer with the same
                // numeric value). Without this, JOIN ON ts = ts returned 0 rows
                // (fell into _ => None → no hash entry).
                Value::Timestamp(t) => {
                    let i = t.as_micros();
                    if (-(1i64 << 53)..=(1i64 << 53)).contains(&i) {
                        Some(HashKey::Numeric((i as f64).to_bits()))
                    } else {
                        Some(HashKey::Integer((i as u64).wrapping_add(i64::MIN as u64)))
                    }
                }
                _ => None,
            }
        }

        // Step 1: Build hash table on smaller table (right)
        // 🚀 预分配：假设负载因子 0.75
        let mut hash_table: HashMap<HashKey, Vec<&SqlRow>> =
            HashMap::with_capacity((right_rows.len() as f64 / 0.75) as usize);

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
    pub(super) fn extract_equi_join_columns(&self, expr: &Expr) -> Option<(String, String)> {
        match expr {
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Eq => {
                // Check if both sides are column references
                if let (Expr::Column(left_col), Expr::Column(right_col)) =
                    (left.as_ref(), right.as_ref())
                {
                    return Some((left_col.clone(), right_col.clone()));
                }
            }
            _ => {}
        }
        None
    }

    /// Normalize equi-join column order to match the actual left/right row sets.
    ///
    /// `extract_equi_join_columns` returns columns in SYNTACTIC order (the order
    /// they appear in `ON a = b`). But callers pass `left_rows` / `right_rows`
    /// based on FROM-clause order. For `ON b.k = a.id` (reversed), the syntactic
    /// left_col (b.k) actually lives in the RIGHT table, so the hash build would
    /// key on a column absent from right_rows → empty result.
    ///
    /// This samples a row from each side and checks whether each column name
    /// appears as a key (directly or as a `.suffix`). If the columns are
    /// swapped relative to the row sets, swap them back.
    pub(super) fn normalize_join_columns(
        left_rows: &[(u64, SqlRow)],
        right_rows: &[(u64, SqlRow)],
        left_col: &str,
        right_col: &str,
    ) -> (String, String) {
        // Check if a column name resolves against a row's keys.
        // Matches exact key, or any key ending in ".<col>" (table-qualified).
        #[inline]
        fn row_has_col(row: &SqlRow, col: &str) -> bool {
            if row.contains_key(col) {
                return true;
            }
            let suffix = format!(".{}", col);
            row.keys()
                .any(|k| !k.starts_with("__") && k.ends_with(&suffix))
        }
        let (left_sample, right_sample) = match (left_rows.first(), right_rows.first()) {
            (Some((_, l)), Some((_, r))) => (l, r),
            _ => return (left_col.to_string(), right_col.to_string()),
        };
        let left_in_left = row_has_col(left_sample, left_col);
        let right_in_right = row_has_col(right_sample, right_col);
        if left_in_left && right_in_right {
            // Already correctly oriented.
            (left_col.to_string(), right_col.to_string())
        } else {
            // Check the swapped orientation: is left_col in right, and right_col in left?
            let left_in_right = row_has_col(right_sample, left_col);
            let right_in_left = row_has_col(left_sample, right_col);
            if left_in_right && right_in_left {
                // Swap so build/probe line up with the actual row sets.
                (right_col.to_string(), left_col.to_string())
            } else {
                // Can't determine confidently — leave as-is (nested-loop fallback
                // in the caller will evaluate correctly).
                (left_col.to_string(), right_col.to_string())
            }
        }
    }

    /// LEFT JOIN: all rows from left, matched rows from right (NULL if no match)

    /// ON 合取拆分: (等值叶, 残余叶)。无合取或无等值叶 → (原式, []) 由调用方
    /// 自行判断 (extract_equi 失败 → 嵌套循环)。
    pub(super) fn split_on_conjunction(on: &Expr) -> (Expr, Vec<Expr>) {
        fn flatten_and(e: &Expr, out: &mut Vec<Expr>) {
            if let Expr::BinaryOp {
                left,
                op: crate::sql::ast::BinaryOperator::And,
                right,
            } = e
            {
                flatten_and(left, out);
                flatten_and(right, out);
            } else {
                out.push(e.clone());
            }
        }
        let mut leaves = Vec::new();
        flatten_and(on, &mut leaves);
        let mut equi: Option<Expr> = None;
        let mut residual: Vec<Expr> = Vec::new();
        for leaf in leaves {
            if equi.is_none()
                && QueryExecutor::static_extract_equi_ok(&leaf)
            {
                equi = Some(leaf);
            } else {
                residual.push(leaf);
            }
        }
        match equi {
            Some(e) => (e, residual),
            None => (on.clone(), Vec::new()),
        }
    }

    /// 残余叶 → (`限定列` op literal), 不做 schema 判定 (调用方判归属)。
    pub(super) fn residual_single_table_pred(
        e: &Expr,
    ) -> Option<(String, crate::sql::ast::BinaryOperator, Value)> {
        use crate::sql::ast::BinaryOperator;
        if let Expr::BinaryOp { left, op, right } = e {
            let ok_op = matches!(op, BinaryOperator::Eq | BinaryOperator::Ne | BinaryOperator::Lt
                | BinaryOperator::Gt | BinaryOperator::Le | BinaryOperator::Ge);
            if let (Expr::Column(cn), Expr::Literal(v), true) = (left.as_ref(), right.as_ref(), ok_op) {
                return Some((cn.clone(), op.clone(), v.clone()));
            }
        }
        None
    }

    pub(super) fn static_extract_equi_ok(e: &Expr) -> bool {
        // 轻量判定: BinaryOp{Column, cmp, Column}
        matches!(e, Expr::BinaryOp { left, op, right }
            if matches!(op,
                crate::sql::ast::BinaryOperator::Eq
                | crate::sql::ast::BinaryOperator::Ne)
            && matches!(left.as_ref(), Expr::Column(_))
            && matches!(right.as_ref(), Expr::Column(_)))
    }

    /// 残余叶是否为 `schema 列 op literal` 的单表谓词 (限定名)。
    pub(super) fn residual_single_table(e: &Expr, schema: &TableSchema) -> Option<(String, crate::sql::ast::BinaryOperator, Value)> {
        use crate::sql::ast::BinaryOperator;
        if let Expr::BinaryOp { left, op, right } = e {
            let ok_op = matches!(op, BinaryOperator::Eq | BinaryOperator::Ne | BinaryOperator::Lt
                | BinaryOperator::Gt | BinaryOperator::Le | BinaryOperator::Ge);
            if let (Expr::Column(cn), Expr::Literal(v), true) = (left.as_ref(), right.as_ref(), ok_op) {
                if schema.columns.iter().any(|c| c.name == *cn) {
                    return Some((cn.clone(), op.clone(), v.clone()));
                }
            }
        }
        None
    }

    pub(super) fn left_join(
        &self,
        left_rows: &[(u64, SqlRow)],
        right_rows: &[(u64, SqlRow)],
        on_condition: &Expr,
        right_schema: &crate::types::TableSchema,
    ) -> Result<Vec<(u64, SqlRow)>> {
        // Pre-compute NULL row for right side from schema
        let null_right_row: SqlRow = right_schema
            .columns
            .iter()
            .map(|col| (col.name.clone(), Value::Null))
            .collect();

        // 🔑 ON 合取: 拆等值叶 + 残余。右表单表残余 (`b.id <= N`) 预过滤
        // 右侧后等价消耗 — 等值对直接走 hash (此前带 AND 的 ON 整体落
        // 嵌套循环, LEFT JOIN 反连接 @100K 20min+ 跑不完)。跨表/表达式
        // 残余 → 保持嵌套循环逐候选 eval (正确性优先)。
        let (on_equi, on_residual) = Self::split_on_conjunction(on_condition);
        if self.extract_equi_join_columns(&on_equi).is_some() && !on_residual.is_empty() {
            let mut right_f: Vec<(u64, SqlRow)> = Vec::with_capacity(right_rows.len());
            let mut all_consumed = true;
            let mut right_preds: Vec<(String, crate::sql::ast::BinaryOperator, Value)> = Vec::new();
            for r in &on_residual {
                match Self::residual_single_table(r, right_schema) {
                    Some(p) => right_preds.push(p),
                    None => {
                        all_consumed = false;
                        break;
                    }
                }
            }
            if all_consumed && !right_preds.is_empty() {
                for (rid, row) in right_rows {
                    let keep = right_preds.iter().all(|(k, op, lit)| {
                        apply_op_value(op, row.get(k), lit)
                    });
                    if keep {
                        right_f.push((*rid, row.clone()));
                    }
                }
                let (lc, rc) = self.extract_equi_join_columns(&on_equi).unwrap();
                return self.hash_join_left(left_rows, &right_f, &lc, &rc, &null_right_row);
            }
        }
        // Try hash join optimization for equi-join.
        // 🔑 仅当残余为空 (纯等值 ON) — 否则 hash 丢残余条件。
        if on_residual.is_empty() {
            if let Some((left_col, right_col)) = self.extract_equi_join_columns(&on_equi) {
                return self.hash_join_left(
                    left_rows,
                    right_rows,
                    &left_col,
                    &right_col,
                    &null_right_row,
                );
            }
        }

        // Fallback: nested loop
        let mut result = Vec::new();
        let mut next_id = 1u64;

        for (_, left_row) in left_rows {
            let mut matched = false;

            for (_, right_row) in right_rows {
                let combined_row = self.combine_rows(left_row, right_row);

                if self
                    .evaluator
                    .eval(on_condition, &combined_row)
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
    pub(super) fn hash_join_left(
        &self,
        left_rows: &[(u64, SqlRow)],
        right_rows: &[(u64, SqlRow)],
        left_col: &str,
        right_col: &str,
        null_right_row: &SqlRow,
    ) -> Result<Vec<(u64, SqlRow)>> {
        use std::collections::HashMap;

        // 🚨 Normalize ON operand order (see hash_join_inner for rationale).
        let (left_col, right_col) =
            Self::normalize_join_columns(left_rows, right_rows, left_col, right_col);
        let left_col = left_col.as_str();
        let right_col = right_col.as_str();

        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        enum HashKey {
            Numeric(u64),
            Integer(u64),
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
                    const EXACT_MAX: i64 = 1i64 << 53; // 2^53, max exact i64 in f64
                    if *i >= -EXACT_MAX && *i <= EXACT_MAX {
                        Some(HashKey::Numeric((*i as f64).to_bits()))
                    } else {
                        Some(HashKey::Integer((*i as u64).wrapping_add(i64::MIN as u64)))
                    }
                }
                // Normalize -0.0 → +0.0 so they hash/match as equal (IEEE-754:
                // 0.0 == -0.0, but their bit patterns differ). Adding 0.0 turns
                // -0.0 into +0.0; non-zero values are unchanged.
                Value::Float(f) => Some(HashKey::Numeric((f + 0.0).to_bits())),
                Value::Text(s) => Some(HashKey::Text(s.to_string())),
                Value::Bool(b) => Some(HashKey::Bool(*b)),
                Value::Null => None, // SQL: NULL != NULL in joins
                // 🚨 Timestamp: hash on micros (matches Integer). Without this,
                // JOIN ON ts = ts returned 0 rows (fell into _ => None).
                Value::Timestamp(t) => {
                    let i = t.as_micros();
                    if (-(1i64 << 53)..=(1i64 << 53)).contains(&i) {
                        Some(HashKey::Numeric((i as f64).to_bits()))
                    } else {
                        Some(HashKey::Integer((i as u64).wrapping_add(i64::MIN as u64)))
                    }
                }
                _ => None,
            }
        }

        // Build hash table on right
        let mut hash_table: HashMap<HashKey, Vec<&SqlRow>> =
            HashMap::with_capacity((right_rows.len() as f64 / 0.75) as usize);
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
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            };

            if !matched {
                result.push((next_id, self.combine_rows(left_row, null_right_row)));
                next_id += 1;
            }
        }

        Ok(result)
    }

    /// RIGHT JOIN: all rows from right, matched rows from left (NULL if no match)
    pub(super) fn right_join(
        &self,
        left_rows: &[(u64, SqlRow)],
        right_rows: &[(u64, SqlRow)],
        on_condition: &Expr,
        left_schema: &crate::types::TableSchema,
    ) -> Result<Vec<(u64, SqlRow)>> {
        // Pre-compute NULL row for left side from schema
        let null_left_row: SqlRow = left_schema
            .columns
            .iter()
            .map(|col| (col.name.clone(), Value::Null))
            .collect();

        // Try hash join optimization for equi-join
        if let Some((left_col, right_col)) = self.extract_equi_join_columns(on_condition) {
            return self.hash_join_right(
                left_rows,
                right_rows,
                &left_col,
                &right_col,
                &null_left_row,
            );
        }

        // Fallback: nested loop
        let mut result = Vec::new();
        let mut next_id = 1u64;

        for (_, right_row) in right_rows {
            let mut matched = false;

            for (_, left_row) in left_rows {
                let combined_row = self.combine_rows(left_row, right_row);

                if self
                    .evaluator
                    .eval(on_condition, &combined_row)
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
    pub(super) fn hash_join_right(
        &self,
        left_rows: &[(u64, SqlRow)],
        right_rows: &[(u64, SqlRow)],
        left_col: &str,
        right_col: &str,
        null_left_row: &SqlRow,
    ) -> Result<Vec<(u64, SqlRow)>> {
        use std::collections::HashMap;

        // 🚨 Normalize ON operand order (see hash_join_inner for rationale).
        let (left_col, right_col) =
            Self::normalize_join_columns(left_rows, right_rows, left_col, right_col);
        let left_col = left_col.as_str();
        let right_col = right_col.as_str();

        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        enum HashKey {
            Numeric(u64),
            Integer(u64),
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
                    const EXACT_MAX: i64 = 1i64 << 53; // 2^53, max exact i64 in f64
                    if *i >= -EXACT_MAX && *i <= EXACT_MAX {
                        Some(HashKey::Numeric((*i as f64).to_bits()))
                    } else {
                        Some(HashKey::Integer((*i as u64).wrapping_add(i64::MIN as u64)))
                    }
                }
                // Normalize -0.0 → +0.0 so they hash/match as equal (IEEE-754:
                // 0.0 == -0.0, but their bit patterns differ). Adding 0.0 turns
                // -0.0 into +0.0; non-zero values are unchanged.
                Value::Float(f) => Some(HashKey::Numeric((f + 0.0).to_bits())),
                Value::Text(s) => Some(HashKey::Text(s.to_string())),
                Value::Bool(b) => Some(HashKey::Bool(*b)),
                Value::Null => None, // SQL: NULL != NULL in joins
                // 🚨 Timestamp: hash on micros (matches Integer). Without this,
                // JOIN ON ts = ts returned 0 rows (fell into _ => None).
                Value::Timestamp(t) => {
                    let i = t.as_micros();
                    if (-(1i64 << 53)..=(1i64 << 53)).contains(&i) {
                        Some(HashKey::Numeric((i as f64).to_bits()))
                    } else {
                        Some(HashKey::Integer((i as u64).wrapping_add(i64::MIN as u64)))
                    }
                }
                _ => None,
            }
        }

        // Build hash table on left
        let mut hash_table: HashMap<HashKey, Vec<&SqlRow>> =
            HashMap::with_capacity((left_rows.len() as f64 / 0.75) as usize);
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
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            };

            if !matched {
                result.push((next_id, self.combine_rows(null_left_row, right_row)));
                next_id += 1;
            }
        }

        Ok(result)
    }

    /// FULL OUTER JOIN: all rows from both tables (NULL where no match)
    pub(super) fn full_join(
        &self,
        left_rows: &[(u64, SqlRow)],
        right_rows: &[(u64, SqlRow)],
        on_condition: &Expr,
        left_schema: &crate::types::TableSchema,
        right_schema: &crate::types::TableSchema,
    ) -> Result<Vec<(u64, SqlRow)>> {
        // Pre-compute NULL rows from schema
        let null_right_row: SqlRow = right_schema
            .columns
            .iter()
            .map(|col| (col.name.clone(), Value::Null))
            .collect();
        let null_left_row: SqlRow = left_schema
            .columns
            .iter()
            .map(|col| (col.name.clone(), Value::Null))
            .collect();

        // Try hash join optimization for equi-join
        if let Some((left_col, right_col)) = self.extract_equi_join_columns(on_condition) {
            return self.hash_join_full(
                left_rows,
                right_rows,
                &left_col,
                &right_col,
                &null_left_row,
                &null_right_row,
            );
        }

        // Fallback: nested loop
        let mut result = Vec::new();
        let mut next_id = 1u64;
        let mut right_matched = vec![false; right_rows.len()];

        for (_, left_row) in left_rows {
            let mut left_matched = false;

            for (right_idx, (_, right_row)) in right_rows.iter().enumerate() {
                let combined_row = self.combine_rows(left_row, right_row);

                if self
                    .evaluator
                    .eval(on_condition, &combined_row)
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
    pub(super) fn hash_join_full(
        &self,
        left_rows: &[(u64, SqlRow)],
        right_rows: &[(u64, SqlRow)],
        left_col: &str,
        right_col: &str,
        null_left_row: &SqlRow,
        null_right_row: &SqlRow,
    ) -> Result<Vec<(u64, SqlRow)>> {
        use std::collections::HashMap;

        // 🚨 Normalize ON operand order (see hash_join_inner for rationale).
        let (left_col, right_col) =
            Self::normalize_join_columns(left_rows, right_rows, left_col, right_col);
        let left_col = left_col.as_str();
        let right_col = right_col.as_str();

        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        enum HashKey {
            Numeric(u64),
            Integer(u64),
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
                    const EXACT_MAX: i64 = 1i64 << 53; // 2^53, max exact i64 in f64
                    if *i >= -EXACT_MAX && *i <= EXACT_MAX {
                        Some(HashKey::Numeric((*i as f64).to_bits()))
                    } else {
                        Some(HashKey::Integer((*i as u64).wrapping_add(i64::MIN as u64)))
                    }
                }
                // Normalize -0.0 → +0.0 so they hash/match as equal (IEEE-754:
                // 0.0 == -0.0, but their bit patterns differ). Adding 0.0 turns
                // -0.0 into +0.0; non-zero values are unchanged.
                Value::Float(f) => Some(HashKey::Numeric((f + 0.0).to_bits())),
                Value::Text(s) => Some(HashKey::Text(s.to_string())),
                Value::Bool(b) => Some(HashKey::Bool(*b)),
                Value::Null => None, // SQL: NULL != NULL in joins
                // 🚨 Timestamp: hash on micros (matches Integer). Without this,
                // JOIN ON ts = ts returned 0 rows (fell into _ => None).
                Value::Timestamp(t) => {
                    let i = t.as_micros();
                    if (-(1i64 << 53)..=(1i64 << 53)).contains(&i) {
                        Some(HashKey::Numeric((i as f64).to_bits()))
                    } else {
                        Some(HashKey::Integer((i as u64).wrapping_add(i64::MIN as u64)))
                    }
                }
                _ => None,
            }
        }

        // Build hash table on right
        let mut hash_table: HashMap<HashKey, Vec<(usize, &SqlRow)>> =
            HashMap::with_capacity((right_rows.len() as f64 / 0.75) as usize);
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
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            };

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
    pub(super) fn combine_rows(&self, left: &SqlRow, right: &SqlRow) -> SqlRow {
        // Merge two SqlRows into one. The left row's entries are cloned, the
        // right row's entries are also cloned (both sides may match multiple
        // partners in the join). Capacity pre-allocated to avoid rehashing.
        let mut combined = SqlRow::with_capacity(left.len() + right.len());
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
    pub(super) fn stream_in_subquery_to_hashset(
        &self,
        subquery_stmt: &SelectStmt,
        _outer_col_name: &str,
    ) -> Option<(std::collections::HashSet<Value>, bool)> {
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
        let compiled_where: Option<CompiledWhere> = subquery_stmt
            .where_clause
            .as_ref()
            .and_then(|clause| Self::compile_where(clause, &schema));
        // 🔑 WHERE 存在但编译失败 (嵌套子查询/表达式谓词等非
        // col-op-literal 形状) 时必须 decline — 继续执行会静默丢掉
        // WHERE, IN 集合变成全表 (test_bug_hunt_v76::test_nested_subquery:
        // v > (SELECT MIN(v) …) 的内层过滤被丢, 返回 {1,2,3} 而非 {2,3})。
        if subquery_stmt.where_clause.is_some() && compiled_where.is_none() {
            return None;
        }
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
        let (set, has_null) = if self.db.is_columnar_table(table_name) {
            self.build_in_hashset_from_columnar(
                table_name,
                &col_types,
                inner_col_pos,
                compiled_where.as_ref(),
                &where_positions,
                &where_pos_to_idx,
            )?
        } else {
            let raw_iter = self.db.scan_table_raw_streaming(table_name).ok()?;
            let has_where = compiled_where.is_some();
            let cap = if has_where { 1024 } else { 16384 };
            let mut set = std::collections::HashSet::with_capacity(cap);
            let mut where_buf = Vec::with_capacity(where_positions.len().max(1));
            let mut has_null = false;

            for result in raw_iter {
                let (_row_id, raw_bytes) = match result {
                    Ok(r) => r,
                    Err(_) => continue,
                };

                // Phase 1: WHERE eval on partial decode
                if let Some(ref cw) = compiled_where {
                    let ctx = match crate::storage::row_format::RowParseContext::parse(
                        &raw_bytes,
                        &col_types,
                        fixed_count,
                    ) {
                        Some(c) => c,
                        None => continue,
                    };
                    if ctx
                        .decode_columns(&raw_bytes, &col_types, &where_positions, &mut where_buf)
                        .is_err()
                    {
                        continue;
                    }
                    if !cw.eval_at(&where_buf, &where_pos_to_idx).unwrap_or(false) {
                        continue;
                    }
                }

                // Phase 2: Decode the SELECT column and insert into HashSet
                let val =
                    crate::storage::row_format::get_column(&raw_bytes, &col_types, inner_col_pos)
                        .unwrap_or(Value::Null);
                if matches!(val, Value::Null) {
                    has_null = true;
                } else {
                    set.insert(val);
                }
            }
            (set, has_null)
        };

        if set.is_empty() && !has_null {
            // Empty set (no NULLs) means outer IN should match nothing
            return Some((set, false));
        }

        Some((set, has_null))
    }
}
