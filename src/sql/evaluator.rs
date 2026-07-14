/// Expression evaluator - evaluates expressions against rows
use super::ast::{BinaryOperator, Expr, UnaryOperator};
use crate::database::MoteDB;
use crate::error::{MoteDBError, Result};
use crate::types::{SqlRow, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::sync::RwLock;

/// ⚡ Compiled LIKE pattern for fast matching
#[derive(Debug, Clone)]
enum CompiledPattern {
    /// Exact match: "abc" (no wildcards)
    Exact(String),
    /// Prefix match: "abc%"
    Prefix(String),
    /// Suffix match: "%abc"
    Suffix(String),
    /// Contains match: "%abc%"
    Contains(String),
    /// Complex pattern with multiple wildcards
    Complex(Vec<PatternSegment>),
}

#[derive(Debug, Clone)]
enum PatternSegment {
    Literal(String),
    AnyChar,  // _
    AnyChars, // %
}

impl CompiledPattern {
    /// Compile LIKE pattern into optimized form
    fn compile(pattern: &str) -> Self {
        // Fast path: no wildcards
        if !pattern.contains('%') && !pattern.contains('_') {
            return CompiledPattern::Exact(pattern.to_string());
        }

        // Fast path: prefix match "abc%"
        if pattern.ends_with('%')
            && !pattern[..pattern.len() - 1].contains('%')
            && !pattern.contains('_')
        {
            return CompiledPattern::Prefix(pattern[..pattern.len() - 1].to_string());
        }

        // Fast path: suffix match "%abc"
        if pattern.starts_with('%') && !pattern[1..].contains('%') && !pattern.contains('_') {
            return CompiledPattern::Suffix(pattern[1..].to_string());
        }

        // Fast path: contains match "%abc%"
        if pattern.starts_with('%')
            && pattern.ends_with('%')
            && pattern.len() > 2
            && !pattern[1..pattern.len() - 1].contains('%')
            && !pattern.contains('_')
        {
            return CompiledPattern::Contains(pattern[1..pattern.len() - 1].to_string());
        }

        // Complex pattern: parse into segments
        let mut segments = Vec::new();
        let mut current_literal = String::new();

        for ch in pattern.chars() {
            match ch {
                '%' => {
                    if !current_literal.is_empty() {
                        segments.push(PatternSegment::Literal(current_literal.clone()));
                        current_literal.clear();
                    }
                    segments.push(PatternSegment::AnyChars);
                }
                '_' => {
                    if !current_literal.is_empty() {
                        segments.push(PatternSegment::Literal(current_literal.clone()));
                        current_literal.clear();
                    }
                    segments.push(PatternSegment::AnyChar);
                }
                c => {
                    current_literal.push(c);
                }
            }
        }

        if !current_literal.is_empty() {
            segments.push(PatternSegment::Literal(current_literal));
        }

        CompiledPattern::Complex(segments)
    }

    /// Fast matching against compiled pattern
    #[inline]
    fn matches(&self, text: &str) -> bool {
        match self {
            CompiledPattern::Exact(pattern) => text == pattern,
            CompiledPattern::Prefix(prefix) => text.starts_with(prefix),
            CompiledPattern::Suffix(suffix) => text.ends_with(suffix),
            CompiledPattern::Contains(substring) => text.contains(substring),
            CompiledPattern::Complex(segments) => Self::match_complex(text, segments),
        }
    }

    /// Match complex pattern with segments using DP (O(n*m) instead of exponential)
    fn match_complex(text: &str, segments: &[PatternSegment]) -> bool {
        let t: Vec<char> = text.chars().collect();
        let n = t.len();
        let m = segments.len();

        // dp[ti][si] = can we match text[ti..] against segments[si..]
        let mut dp = vec![vec![false; m + 1]; n + 1];
        dp[n][m] = true; // both empty → match

        // Fill base case: text consumed but segments remain (only % can match empty)
        for si in (0..m).rev() {
            if matches!(segments[si], PatternSegment::AnyChars) && dp[n][si + 1] {
                dp[n][si] = true;
            }
        }

        // Fill from bottom-right
        for ti in (0..n).rev() {
            for si in (0..m).rev() {
                match &segments[si] {
                    PatternSegment::AnyChars => {
                        // Match 0 chars (skip %) or 1+ chars (consume char, keep %)
                        dp[ti][si] = dp[ti][si + 1] || dp[ti + 1][si];
                    }
                    PatternSegment::AnyChar => {
                        // Match exactly one character
                        dp[ti][si] = dp[ti + 1][si + 1];
                    }
                    PatternSegment::Literal(literal) => {
                        let chars: Vec<char> = literal.chars().collect();
                        let len = chars.len();
                        if ti + len <= n {
                            let mut matches = true;
                            for (i, &c) in chars.iter().enumerate() {
                                if t[ti + i] != c {
                                    matches = false;
                                    break;
                                }
                            }
                            if matches {
                                dp[ti][si] = dp[ti + len][si + 1];
                            }
                        }
                    }
                }
            }
        }

        dp[0][0]
    }
}

pub struct ExprEvaluator {
    /// ⚡ Pattern cache: pattern string -> compiled pattern
    /// RwLock for concurrent read access (common case)
    pattern_cache: Arc<RwLock<HashMap<String, CompiledPattern>>>,
    /// Store the last AUTO_INCREMENT value inserted (AtomicI64, i64::MIN = None)
    pub(crate) last_insert_id: AtomicI64,
    /// Bind parameters for parameterized queries (?1, ?2, ...)
    params: RwLock<Vec<Value>>,
}

impl ExprEvaluator {
    pub fn new() -> Self {
        Self {
            pattern_cache: Arc::new(RwLock::new(HashMap::new())),
            last_insert_id: AtomicI64::new(i64::MIN),
            params: RwLock::new(Vec::new()),
        }
    }

    /// Convert days since Unix epoch to (year, month, day).
    /// Uses the civil calendar algorithm (correct for all dates).
    fn days_to_date(days_since_epoch: i64) -> (i64, i64, i64) {
        // Shift from Unix epoch (1970-01-01) to Gregorian epoch (0000-03-01)
        // The algorithm works with a year starting on March 1 to simplify leap year handling.
        let z = days_since_epoch + 719468; // days from 0000-03-01 to 1970-01-01
        let era = if z >= 0 {
            z / 146097
        } else {
            (z - 146096) / 146097
        };
        let doe = z - era * 146097; // day of era [0, 146096]
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // year of era [0, 399]
        let y = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day of year [0, 365]
        let mp = (5 * doy + 2) / 153; // month index [0, 11] (March=0)
        let d = doy - (153 * mp + 2) / 5 + 1; // day [1, 31]
        let m = if mp < 10 { mp + 3 } else { mp - 9 }; // month [1, 12]
        let y = if m <= 2 { y + 1 } else { y };
        (y, m, d)
    }

    pub fn with_db(_db: Arc<MoteDB>) -> Self {
        Self {
            pattern_cache: Arc::new(RwLock::new(HashMap::new())),
            last_insert_id: AtomicI64::new(i64::MIN),
            params: RwLock::new(Vec::new()),
        }
    }

    /// Set bind parameters for a parameterized query.
    pub fn set_params(&self, params: Vec<Value>) {
        *self.params.write().unwrap() = params;
    }

    pub fn get_params(&self) -> Vec<Value> {
        self.params.read().unwrap().clone()
    }

    /// Clear bind parameters after execution.
    pub fn clear_params(&self) {
        self.params.write().unwrap().clear();
    }

    /// Evaluate an expression against a row
    pub fn eval(&self, expr: &Expr, row: &SqlRow) -> Result<Value> {
        match expr {
            Expr::Column(name) => {
                // 🔧 FIX: Intelligent column name matching
                // Try 1: Direct match (e.g., "id" or "table.id")
                if let Some(val) = row.get(name) {
                    return Ok(val.clone());
                }

                // Try 2: Match with table prefix (e.g., "id" matches "test.id")
                let mut prefix_matches: Vec<&str> = Vec::new();
                for key in row.keys() {
                    if key.starts_with("__") {
                        continue;
                    }
                    if key.ends_with(&format!(".{}", name)) {
                        prefix_matches.push(key.as_str());
                    }
                }
                if prefix_matches.len() == 1 {
                    return Ok(row[prefix_matches[0]].clone());
                } else if prefix_matches.len() > 1 {
                    return Err(MoteDBError::ColumnNotFound(format!(
                        "Ambiguous column '{}' matches: {}",
                        name,
                        prefix_matches.join(", ")
                    )));
                }

                // Try 3: Case-insensitive match (for robustness).
                // Collect all matches to detect ambiguity — picking the first
                // match silently could return the wrong column on collision.
                let name_lower = name.to_lowercase();
                let mut matches: Vec<&str> = Vec::new();
                for key in row.keys() {
                    if key.to_lowercase() == name_lower {
                        matches.push(key.as_str());
                    }
                }
                if matches.len() == 1 {
                    return Ok(row[matches[0]].clone());
                } else if matches.len() > 1 {
                    return Err(MoteDBError::ColumnNotFound(format!(
                        "Ambiguous column '{}' matches: {}",
                        name,
                        matches.join(", ")
                    )));
                }

                Err(MoteDBError::ColumnNotFound(name.clone()))
            }

            Expr::Literal(val) => Ok(val.clone()),

            Expr::Parameter(idx) => {
                let params = self.params.read().unwrap();
                if *idx == 0 {
                    return Err(MoteDBError::InvalidArgument(
                        "Unnamed ? parameter not resolved (internal error)".to_string(),
                    ));
                }
                let resolved_idx = idx - 1; // ?1 → index 0, ?2 → index 1
                params.get(resolved_idx).cloned().ok_or_else(|| {
                    MoteDBError::InvalidArgument(format!(
                        "Parameter ?{} not bound ({} parameters provided)",
                        idx,
                        params.len()
                    ))
                })
            }

            Expr::BinaryOp { left, op, right } => {
                let left_val = self.eval(left, row)?;
                let right_val = self.eval(right, row)?;
                self.eval_binary_op(op, left_val, right_val)
            }

            Expr::UnaryOp { op, expr } => {
                let val = self.eval(expr, row)?;
                self.eval_unary_op(op, val)
            }

            Expr::FunctionCall {
                name,
                args,
                distinct,
            } => self.eval_function(name, args, *distinct, row),

            Expr::In {
                expr,
                list,
                negated,
            } => {
                let val = self.eval(expr, row)?;

                // SQL NULL semantics: NULL IN (...) returns false (unknown)
                if matches!(val, Value::Null) {
                    return Ok(Value::Bool(false));
                }

                // Fast path: when all items are literals, use O(1) hash comparison
                let all_literals = list.iter().all(|e| matches!(e, Expr::Literal(_)));
                if all_literals {
                    let found = list.iter().any(|item| {
                        if let Expr::Literal(v) = item {
                            val == *v
                        } else {
                            false
                        }
                    });
                    return Ok(Value::Bool(if *negated { !found } else { found }));
                }

                let mut found = false;
                let mut has_null = false;

                for item in list {
                    let item_val = self.eval(item, row)?;
                    if matches!(item_val, Value::Null) {
                        has_null = true;
                        continue;
                    }
                    if val == item_val {
                        found = true;
                        break;
                    }
                }
                // SQL: NOT IN (list with NULL) → unknown → false if no match
                if *negated && !found && has_null {
                    return Ok(Value::Bool(false));
                }
                Ok(Value::Bool(if *negated { !found } else { found }))
            }

            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let val = self.eval(expr, row)?;
                let low_val = self.eval(low, row)?;
                let high_val = self.eval(high, row)?;

                // SQL NULL semantics: if any operand is NULL, exclude the row
                // This correctly handles NOT BETWEEN too (returns false, not true)
                if matches!(val, Value::Null)
                    || matches!(low_val, Value::Null)
                    || matches!(high_val, Value::Null)
                {
                    return Ok(Value::Bool(false));
                }

                let in_range = val >= low_val && val <= high_val;
                Ok(Value::Bool(if *negated { !in_range } else { in_range }))
            }

            Expr::Like {
                expr,
                pattern,
                negated,
            } => {
                let val = self.eval(expr, row)?;
                let pattern_val = self.eval(pattern, row)?;

                // SQL NULL semantics: LIKE NULL = UNKNOWN → false for filtering
                if matches!(val, Value::Null) || matches!(pattern_val, Value::Null) {
                    return Ok(Value::Bool(false));
                }

                let matches = if let (Value::Text(s), Value::Text(p)) = (val, pattern_val) {
                    self.like_match_cached(&s, &p)
                } else {
                    false
                };

                Ok(Value::Bool(if *negated { !matches } else { matches }))
            }

            Expr::IsNull { expr, negated } => {
                let val = self.eval(expr, row)?;
                let is_null = matches!(val, Value::Null);
                Ok(Value::Bool(if *negated { !is_null } else { is_null }))
            }

            Expr::Case { whens, else_expr } => {
                for (cond, result) in whens {
                    let cond_val = self.eval(cond, row)?;
                    if matches!(cond_val, Value::Bool(true)) {
                        return self.eval(result, row);
                    }
                }
                if let Some(else_e) = else_expr {
                    self.eval(else_e, row)
                } else {
                    Ok(Value::Null)
                }
            }

            Expr::Subquery(_) => {
                // Subqueries are handled at executor level, not here
                Err(MoteDBError::Query(
                    "Subquery evaluation must be done by executor".into(),
                ))
            }

            Expr::Match { .. } => {
                // MATCH...AGAINST is handled at executor level (requires index access)
                Err(MoteDBError::Query(
                    "MATCH...AGAINST must be evaluated by executor".into(),
                ))
            }

            Expr::KnnSearch { .. } => {
                // KNN_SEARCH is handled at executor level (requires index access)
                Err(MoteDBError::Query(
                    "KNN_SEARCH must be evaluated by executor".into(),
                ))
            }

            Expr::KnnDistance { .. } => {
                // KNN_DISTANCE is handled at executor level (requires row vector data)
                Err(MoteDBError::Query(
                    "KNN_DISTANCE must be evaluated by executor".into(),
                ))
            }

            Expr::StWithin3D { .. } => Err(MoteDBError::Query(
                "ST_WITHIN_3D must be evaluated by executor".into(),
            )),
            Expr::StDistance3D { .. } => Err(MoteDBError::Query(
                "ST_DISTANCE_3D must be evaluated by executor".into(),
            )),
            Expr::StKnn3D { .. } => Err(MoteDBError::Query(
                "ST_KNN_3D must be evaluated by executor".into(),
            )),
            Expr::StRadius3D { .. } => Err(MoteDBError::Query(
                "ST_RADIUS_3D must be evaluated by executor".into(),
            )),

            Expr::WindowFunction { .. } => {
                // Window functions are handled at executor level (require partition data)
                Err(MoteDBError::Query(
                    "Window functions must be evaluated by executor".into(),
                ))
            }
        }
    }

    fn eval_binary_op(&self, op: &BinaryOperator, left: Value, right: Value) -> Result<Value> {
        // SQL NULL semantics: NULL comparison → false (for WHERE filtering),
        // NULL arithmetic → NULL (for SELECT projection correctness).
        let either_null = matches!(&left, Value::Null) || matches!(&right, Value::Null);
        if either_null {
            match op {
                // Comparison: NULL → UNKNOWN → false for filtering
                BinaryOperator::Eq
                | BinaryOperator::Ne
                | BinaryOperator::Lt
                | BinaryOperator::Gt
                | BinaryOperator::Le
                | BinaryOperator::Ge => {
                    return Ok(Value::Bool(false));
                }
                // AND: FALSE AND anything = FALSE; TRUE AND NULL = NULL
                BinaryOperator::And => {
                    let lb = match &left {
                        Value::Null => None,
                        v => Some(self.to_bool(v)?),
                    };
                    let rb = match &right {
                        Value::Null => None,
                        v => Some(self.to_bool(v)?),
                    };
                    // FALSE short-circuit (SQL three-valued logic)
                    if lb == Some(false) || rb == Some(false) {
                        return Ok(Value::Bool(false));
                    }
                    // Both definitively true
                    if lb == Some(true) && rb == Some(true) {
                        return Ok(Value::Bool(true));
                    }
                    // One or both NULL with no FALSE → unknown
                    return Ok(Value::Null);
                }
                // OR: TRUE OR anything = TRUE; FALSE OR NULL = NULL
                BinaryOperator::Or => {
                    let lb = match &left {
                        Value::Null => None,
                        v => Some(self.to_bool(v)?),
                    };
                    let rb = match &right {
                        Value::Null => None,
                        v => Some(self.to_bool(v)?),
                    };
                    // TRUE short-circuit
                    if lb == Some(true) || rb == Some(true) {
                        return Ok(Value::Bool(true));
                    }
                    // Both definitively false
                    if lb == Some(false) && rb == Some(false) {
                        return Ok(Value::Bool(false));
                    }
                    // One or both NULL with no TRUE → unknown
                    return Ok(Value::Null);
                }
                // Arithmetic/distance: NULL propagates
                BinaryOperator::Add
                | BinaryOperator::Sub
                | BinaryOperator::Mul
                | BinaryOperator::Div
                | BinaryOperator::Mod
                | BinaryOperator::L2Distance
                | BinaryOperator::CosineDistance
                | BinaryOperator::DotProduct => {
                    return Ok(Value::Null);
                }
            }
        }

        match op {
            BinaryOperator::Eq => Ok(Value::Bool(
                left.partial_cmp(&right) == Some(std::cmp::Ordering::Equal),
            )),
            BinaryOperator::Ne => Ok(Value::Bool(
                left.partial_cmp(&right) != Some(std::cmp::Ordering::Equal),
            )),
            BinaryOperator::Lt => {
                // 🐛 DEBUG: Print comparison for debugging
                let result = left < right;
                Ok(Value::Bool(result))
            }
            BinaryOperator::Gt => {
                // 🐛 DEBUG: Print comparison for debugging
                let result = left > right;
                Ok(Value::Bool(result))
            }
            BinaryOperator::Le => Ok(Value::Bool(left <= right)),
            BinaryOperator::Ge => Ok(Value::Bool(left >= right)),

            BinaryOperator::And => {
                let left_bool = self.to_bool(&left)?;
                let right_bool = self.to_bool(&right)?;
                Ok(Value::Bool(left_bool && right_bool))
            }

            BinaryOperator::Or => {
                let left_bool = self.to_bool(&left)?;
                let right_bool = self.to_bool(&right)?;
                Ok(Value::Bool(left_bool || right_bool))
            }

            BinaryOperator::Add => self.add_values(left, right),
            BinaryOperator::Sub => self.sub_values(left, right),
            BinaryOperator::Mul => self.mul_values(left, right),
            BinaryOperator::Div => self.div_values(left, right),
            BinaryOperator::Mod => self.mod_values(left, right),

            // E-SQL Vector Distance Operators
            BinaryOperator::L2Distance => self.l2_distance(left, right),
            BinaryOperator::CosineDistance => self.cosine_distance(left, right),
            BinaryOperator::DotProduct => self.dot_product(left, right),
        }
    }

    fn eval_unary_op(&self, op: &UnaryOperator, val: Value) -> Result<Value> {
        match op {
            UnaryOperator::Not => {
                let b = self.to_bool(&val)?;
                Ok(Value::Bool(!b))
            }
            UnaryOperator::Minus => {
                match val {
                    Value::Integer(i) => {
                        match i.checked_neg() {
                            Some(n) => Ok(Value::Integer(n)),
                            None => Ok(Value::Float(-(i as f64))), // i64::MIN → float
                        }
                    }
                    Value::Float(f) => Ok(Value::Float(-f)),
                    _ => Err(MoteDBError::TypeError(
                        "Cannot negate non-numeric value".to_string(),
                    )),
                }
            }
            UnaryOperator::Plus => Ok(val),
        }
    }

    fn eval_function(
        &self,
        name: &str,
        args: &[Expr],
        _distinct: bool,
        row: &SqlRow,
    ) -> Result<Value> {
        // Note: distinct parameter is only used for aggregate functions like COUNT(DISTINCT)
        // It's ignored for non-aggregate functions

        match name.to_lowercase().as_str() {
            // 🆕 LAST_INSERT_ID() - returns the last AUTO_INCREMENT value
            "last_insert_id" => {
                if !args.is_empty() {
                    return Err(MoteDBError::InvalidArgument(
                        "last_insert_id() takes no arguments".to_string(),
                    ));
                }
                let v = self.last_insert_id.load(Ordering::Relaxed);
                Ok(Value::Integer(if v == i64::MIN { 0 } else { v }))
            }

            // Aggregate functions: look up pre-computed value in row (for HAVING)
            "count" | "sum" | "avg" | "min" | "max" | "stddev" | "variance" => {
                // Build the column name that matches how the executor stored it
                let arg_str = if args.is_empty() {
                    "*".to_string()
                } else {
                    args.iter()
                        .map(|a| match a {
                            Expr::Column(c) => c.clone(),
                            _ => format!("{:?}", a),
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                let key = format!("{}({})", name.to_uppercase(), arg_str);
                if let Some(val) = row.get(&key) {
                    Ok(val.clone())
                } else {
                    Err(MoteDBError::NotImplemented(format!(
                        "Aggregate function {} not yet implemented",
                        name
                    )))
                }
            }

            // String functions
            "lower" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "lower() takes 1 argument".to_string(),
                    ));
                }
                let val = self.eval(&args[0], row)?;
                if let Value::Text(s) = val {
                    Ok(Value::text(s.to_lowercase()))
                } else {
                    Err(MoteDBError::TypeError(
                        "lower() requires text argument".to_string(),
                    ))
                }
            }

            "upper" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "upper() takes 1 argument".to_string(),
                    ));
                }
                let val = self.eval(&args[0], row)?;
                if let Value::Text(s) = val {
                    Ok(Value::text(s.to_uppercase()))
                } else {
                    Err(MoteDBError::TypeError(
                        "upper() requires text argument".to_string(),
                    ))
                }
            }

            "length" | "len" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "length() takes 1 argument".to_string(),
                    ));
                }
                let val = self.eval(&args[0], row)?;
                if let Value::Text(s) = val {
                    Ok(Value::Integer(s.chars().count() as i64))
                } else {
                    Err(MoteDBError::TypeError(
                        "length() requires text argument".to_string(),
                    ))
                }
            }

            // 🆕 String manipulation functions
            "concat" => {
                // CONCAT(str1, str2, ...) - concatenate strings
                // ✅ 优化：预估容量，减少重新分配
                let estimated_capacity = args.len() * 20; // 每个参数估计 20 字节
                let mut result = String::with_capacity(estimated_capacity);
                for arg in args {
                    let val = self.eval(arg, row)?;
                    // ✅ 优化：直接 push_str，避免中间 String 分配
                    match val {
                        Value::Text(s) => result.push_str(&s),
                        Value::Integer(i) => {
                            use std::fmt::Write;
                            let _ = write!(result, "{}", i);
                        }
                        Value::Float(f) => {
                            use std::fmt::Write;
                            let _ = write!(result, "{}", f);
                        }
                        Value::Bool(b) => result.push_str(if b { "true" } else { "false" }),
                        Value::Null => return Ok(Value::Null),
                        _ => {
                            use std::fmt::Write;
                            let _ = write!(result, "{:?}", val);
                        }
                    };
                }
                Ok(Value::text(result))
            }

            "substr" | "substring" => {
                // SUBSTR(text, start [, length])
                if args.len() < 2 || args.len() > 3 {
                    return Err(MoteDBError::InvalidArgument(
                        "substr() takes 2 or 3 arguments (text, start, [length])".to_string(),
                    ));
                }
                let text = match self.eval(&args[0], row)? {
                    Value::Text(s) => s,
                    _ => {
                        return Err(MoteDBError::TypeError(
                            "substr() first argument must be text".to_string(),
                        ))
                    }
                };
                let start = match self.eval(&args[1], row)? {
                    // SQL standard: position is 1-indexed; 0 is treated as 1
                    Value::Integer(i) if i >= 0 => (i.max(1) as usize) - 1,
                    // Negative position counts from end of string
                    Value::Integer(i) if i < 0 => {
                        let from_end = (-i) as usize;
                        text.chars().count().saturating_sub(from_end)
                    }
                    _ => return Ok(Value::text(String::new())),
                };

                let result = if args.len() == 3 {
                    let length = match self.eval(&args[2], row)? {
                        Value::Integer(i) => i.max(0) as usize,
                        _ => {
                            return Err(MoteDBError::TypeError(
                                "substr() length must be integer".to_string(),
                            ))
                        }
                    };
                    text.chars().skip(start).take(length).collect()
                } else {
                    text.chars().skip(start).collect()
                };
                Ok(Value::text(result))
            }

            "trim" => {
                // TRIM(text) - remove leading and trailing whitespace
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "trim() takes 1 argument".to_string(),
                    ));
                }
                let text = match self.eval(&args[0], row)? {
                    Value::Text(s) => s,
                    _ => {
                        return Err(MoteDBError::TypeError(
                            "trim() requires text argument".to_string(),
                        ))
                    }
                };
                // ✅ 优化：如果没有前后空格，直接返回原字符串（Arc clone，零分配）
                if text.trim() == text.as_str() {
                    return Ok(Value::Text(text.clone()));
                }
                // 否则才创建新字符串
                Ok(Value::text(text.trim().to_string()))
            }

            "ltrim" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "ltrim() takes 1 argument".to_string(),
                    ));
                }
                let text = match self.eval(&args[0], row)? {
                    Value::Text(s) => s,
                    _ => {
                        return Err(MoteDBError::TypeError(
                            "ltrim() requires text argument".to_string(),
                        ))
                    }
                };
                // ✅ 优化：如果没有前导空格，直接返回（Arc clone，零分配）
                if text.trim_start() == text.as_str() {
                    return Ok(Value::Text(text.clone()));
                }
                Ok(Value::text(text.trim_start().to_string()))
            }

            "rtrim" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "rtrim() takes 1 argument".to_string(),
                    ));
                }
                let text = match self.eval(&args[0], row)? {
                    Value::Text(s) => s,
                    _ => {
                        return Err(MoteDBError::TypeError(
                            "rtrim() requires text argument".to_string(),
                        ))
                    }
                };
                // ✅ 优化：如果没有尾随空格，直接返回（Arc clone，零分配）
                if text.trim_end() == text.as_str() {
                    return Ok(Value::Text(text.clone()));
                }
                Ok(Value::text(text.trim_end().to_string()))
            }

            "replace" => {
                // REPLACE(text, from, to) - replace all occurrences
                if args.len() != 3 {
                    return Err(MoteDBError::InvalidArgument(
                        "replace() takes 3 arguments (text, from, to)".to_string(),
                    ));
                }
                let text = match self.eval(&args[0], row)? {
                    Value::Text(s) => s,
                    _ => {
                        return Err(MoteDBError::TypeError(
                            "replace() first argument must be text".to_string(),
                        ))
                    }
                };
                let from = match self.eval(&args[1], row)? {
                    Value::Text(s) => s,
                    _ => {
                        return Err(MoteDBError::TypeError(
                            "replace() second argument must be text".to_string(),
                        ))
                    }
                };
                let to = match self.eval(&args[2], row)? {
                    Value::Text(s) => s,
                    _ => {
                        return Err(MoteDBError::TypeError(
                            "replace() third argument must be text".to_string(),
                        ))
                    }
                };
                Ok(Value::text(text.replace(from.as_str(), to.as_str())))
            }

            "reverse" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "reverse() takes 1 argument".to_string(),
                    ));
                }
                let text = match self.eval(&args[0], row)? {
                    Value::Text(s) => s,
                    _ => {
                        return Err(MoteDBError::TypeError(
                            "reverse() requires text argument".to_string(),
                        ))
                    }
                };
                Ok(Value::text(text.chars().rev().collect()))
            }

            "leftstr" | "str_left" => {
                // LEFTSTR(text, length) - get leftmost N characters
                // Renamed to avoid SQL keyword conflict with LEFT JOIN
                if args.len() != 2 {
                    return Err(MoteDBError::InvalidArgument(
                        "leftstr() takes 2 arguments".to_string(),
                    ));
                }
                let text = match self.eval(&args[0], row)? {
                    Value::Text(s) => s,
                    _ => {
                        return Err(MoteDBError::TypeError(
                            "leftstr() first argument must be text".to_string(),
                        ))
                    }
                };
                let length = match self.eval(&args[1], row)? {
                    Value::Integer(i) => i.max(0) as usize,
                    _ => {
                        return Err(MoteDBError::TypeError(
                            "leftstr() second argument must be integer".to_string(),
                        ))
                    }
                };
                Ok(Value::text(text.chars().take(length).collect()))
            }

            "rightstr" | "str_right" => {
                // RIGHTSTR(text, length) - get rightmost N characters
                // Renamed to avoid SQL keyword conflict with RIGHT JOIN
                if args.len() != 2 {
                    return Err(MoteDBError::InvalidArgument(
                        "rightstr() takes 2 arguments".to_string(),
                    ));
                }
                let text = match self.eval(&args[0], row)? {
                    Value::Text(s) => s,
                    _ => {
                        return Err(MoteDBError::TypeError(
                            "rightstr() first argument must be text".to_string(),
                        ))
                    }
                };
                let length = match self.eval(&args[1], row)? {
                    Value::Integer(i) => i.max(0) as usize,
                    _ => {
                        return Err(MoteDBError::TypeError(
                            "rightstr() second argument must be integer".to_string(),
                        ))
                    }
                };
                let char_vec: Vec<char> = text.chars().collect();
                let start_idx = char_vec.len().saturating_sub(length);
                Ok(Value::text(char_vec[start_idx..].iter().collect()))
            }

            "repeat" => {
                // REPEAT(text, n) - repeat text N times
                if args.len() != 2 {
                    return Err(MoteDBError::InvalidArgument(
                        "repeat() takes 2 arguments".to_string(),
                    ));
                }
                let text = match self.eval(&args[0], row)? {
                    Value::Text(s) => s,
                    _ => {
                        return Err(MoteDBError::TypeError(
                            "repeat() first argument must be text".to_string(),
                        ))
                    }
                };
                let count = match self.eval(&args[1], row)? {
                    Value::Integer(i) => {
                        if i > 1_000_000 {
                            return Err(MoteDBError::InvalidArgument(
                                "repeat() count exceeds maximum (1,000,000)".to_string(),
                            ));
                        }
                        i.max(0) as usize
                    }
                    _ => {
                        return Err(MoteDBError::TypeError(
                            "repeat() second argument must be integer".to_string(),
                        ))
                    }
                };
                Ok(Value::text(text.repeat(count)))
            }

            // Math functions
            "abs" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "abs() takes 1 argument".to_string(),
                    ));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Integer(i) => match i.checked_abs() {
                        Some(n) => Ok(Value::Integer(n)),
                        None => Ok(Value::Float(-(i as f64))), // i64::MIN → float
                    },
                    Value::Float(f) => Ok(Value::Float(f.abs())),
                    _ => Err(MoteDBError::TypeError(
                        "abs() requires numeric argument".to_string(),
                    )),
                }
            }

            "round" => {
                // ROUND(number [, decimals])
                if args.is_empty() || args.len() > 2 {
                    return Err(MoteDBError::InvalidArgument(
                        "round() takes 1 or 2 arguments".to_string(),
                    ));
                }
                let val = self.eval(&args[0], row)?;
                let decimals = if args.len() == 2 {
                    match self.eval(&args[1], row)? {
                        Value::Integer(i) => i as i32,
                        _ => {
                            return Err(MoteDBError::TypeError(
                                "round() decimals must be integer".to_string(),
                            ))
                        }
                    }
                } else {
                    0
                };

                match val {
                    Value::Float(f) => {
                        let multiplier = 10_f64.powi(decimals);
                        Ok(Value::Float((f * multiplier).round() / multiplier))
                    }
                    Value::Integer(i) => Ok(Value::Integer(i)),
                    _ => Err(MoteDBError::TypeError(
                        "round() requires numeric argument".to_string(),
                    )),
                }
            }

            "floor" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "floor() takes 1 argument".to_string(),
                    ));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Float(f) => {
                        let floored = f.floor();
                        if floored >= i64::MIN as f64 && floored <= i64::MAX as f64 {
                            Ok(Value::Integer(floored as i64))
                        } else {
                            Ok(Value::Float(floored))
                        }
                    }
                    Value::Integer(i) => Ok(Value::Integer(i)),
                    _ => Err(MoteDBError::TypeError(
                        "floor() requires numeric argument".to_string(),
                    )),
                }
            }

            "ceil" | "ceiling" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "ceil() takes 1 argument".to_string(),
                    ));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Float(f) => {
                        let ceiled = f.ceil();
                        if ceiled >= i64::MIN as f64 && ceiled <= i64::MAX as f64 {
                            Ok(Value::Integer(ceiled as i64))
                        } else {
                            Ok(Value::Float(ceiled))
                        }
                    }
                    Value::Integer(i) => Ok(Value::Integer(i)),
                    _ => Err(MoteDBError::TypeError(
                        "ceil() requires numeric argument".to_string(),
                    )),
                }
            }

            "power" | "pow" => {
                // POWER(base, exponent)
                if args.len() != 2 {
                    return Err(MoteDBError::InvalidArgument(
                        "power() takes 2 arguments".to_string(),
                    ));
                }
                let base = self.to_float(&self.eval(&args[0], row)?)?;
                let exp = self.to_float(&self.eval(&args[1], row)?)?;
                Ok(Value::Float(base.powf(exp)))
            }

            "sqrt" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "sqrt() takes 1 argument".to_string(),
                    ));
                }
                let val = self.to_float(&self.eval(&args[0], row)?)?;
                if val < 0.0 {
                    return Err(MoteDBError::InvalidArgument(
                        "sqrt() of negative number".to_string(),
                    ));
                }
                Ok(Value::Float(val.sqrt()))
            }

            "exp" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "exp() takes 1 argument".to_string(),
                    ));
                }
                let val = self.to_float(&self.eval(&args[0], row)?)?;
                Ok(Value::Float(val.exp()))
            }

            "ln" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "ln() takes 1 argument".to_string(),
                    ));
                }
                let val = self.to_float(&self.eval(&args[0], row)?)?;
                if val <= 0.0 {
                    return Err(MoteDBError::InvalidArgument(
                        "ln() of non-positive number".to_string(),
                    ));
                }
                Ok(Value::Float(val.ln()))
            }

            "log" | "log10" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "log() takes 1 argument".to_string(),
                    ));
                }
                let val = self.to_float(&self.eval(&args[0], row)?)?;
                if val <= 0.0 {
                    return Err(MoteDBError::InvalidArgument(
                        "log() of non-positive number".to_string(),
                    ));
                }
                Ok(Value::Float(val.log10()))
            }

            "mod" => {
                // MOD(dividend, divisor)
                if args.len() != 2 {
                    return Err(MoteDBError::InvalidArgument(
                        "mod() takes 2 arguments".to_string(),
                    ));
                }
                let dividend = self.eval(&args[0], row)?;
                let divisor = self.eval(&args[1], row)?;
                match (&dividend, &divisor) {
                    (Value::Integer(a), Value::Integer(b)) => {
                        if *b == 0 {
                            return Err(MoteDBError::InvalidArgument(
                                "Division by zero in mod()".to_string(),
                            ));
                        }
                        // checked_rem guards against i64::MIN % -1 overflow
                        Ok(match a.checked_rem(*b) {
                            Some(n) => Value::Integer(n),
                            None => Value::Integer(0), // x % -1 == 0 for any x
                        })
                    }
                    _ => {
                        let a = self.to_float(&dividend)?;
                        let b = self.to_float(&divisor)?;
                        if b == 0.0 {
                            return Err(MoteDBError::InvalidArgument(
                                "Division by zero in mod()".to_string(),
                            ));
                        }
                        Ok(Value::Float(a % b))
                    }
                }
            }

            "sign" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "sign() takes 1 argument".to_string(),
                    ));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Integer(i) => Ok(Value::Integer(i.signum())),
                    Value::Float(f) => Ok(Value::Integer(if f > 0.0 {
                        1
                    } else if f < 0.0 {
                        -1
                    } else {
                        0
                    })),
                    _ => Err(MoteDBError::TypeError(
                        "sign() requires numeric argument".to_string(),
                    )),
                }
            }

            "random" | "rand" => {
                if !args.is_empty() {
                    return Err(MoteDBError::InvalidArgument(
                        "random() takes no arguments".to_string(),
                    ));
                }
                use std::collections::hash_map::RandomState;
                use std::hash::{BuildHasher, Hasher};
                let s = RandomState::new();
                let mut hasher = s.build_hasher();
                hasher.write_u64(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
                        .as_nanos() as u64,
                );
                let hash = hasher.finish();
                // Convert to [0, 1) range
                Ok(Value::Float((hash as f64) / (u64::MAX as f64)))
            }

            // 🆕 Conditional functions
            "if" => {
                // IF(condition, true_value, false_value)
                if args.len() != 3 {
                    return Err(MoteDBError::InvalidArgument(
                        "if() takes 3 arguments (condition, true_value, false_value)".to_string(),
                    ));
                }
                let condition = self.eval(&args[0], row)?;
                if self.to_bool(&condition)? {
                    self.eval(&args[1], row)
                } else {
                    self.eval(&args[2], row)
                }
            }

            "coalesce" => {
                // COALESCE(val1, val2, ...) - return first non-NULL value
                if args.is_empty() {
                    return Err(MoteDBError::InvalidArgument(
                        "coalesce() requires at least 1 argument".to_string(),
                    ));
                }
                for arg in args {
                    let val = self.eval(arg, row)?;
                    if !matches!(val, Value::Null) {
                        return Ok(val);
                    }
                }
                Ok(Value::Null)
            }

            "ifnull" | "nvl" => {
                // IFNULL(value, default) - return default if value is NULL
                if args.len() != 2 {
                    return Err(MoteDBError::InvalidArgument(
                        "ifnull() takes 2 arguments (value, default)".to_string(),
                    ));
                }
                let val = self.eval(&args[0], row)?;
                if matches!(val, Value::Null) {
                    self.eval(&args[1], row)
                } else {
                    Ok(val)
                }
            }

            "nullif" => {
                // NULLIF(val1, val2) - return NULL if val1 == val2, otherwise val1
                if args.len() != 2 {
                    return Err(MoteDBError::InvalidArgument(
                        "nullif() takes 2 arguments".to_string(),
                    ));
                }
                let val1 = self.eval(&args[0], row)?;
                let val2 = self.eval(&args[1], row)?;
                if val1 == val2 {
                    Ok(Value::Null)
                } else {
                    Ok(val1)
                }
            }

            // E-SQL Spatial Functions
            "st_distance" => {
                if args.len() != 2 {
                    return Err(MoteDBError::InvalidArgument(
                        "ST_Distance() takes 2 arguments".to_string(),
                    ));
                }
                let p1 = self.eval(&args[0], row)?;
                let p2 = self.eval(&args[1], row)?;
                self.st_distance(p1, p2)
            }

            "within_radius" => {
                if args.len() != 3 {
                    return Err(MoteDBError::InvalidArgument(
                        "WITHIN_RADIUS() takes 3 arguments (point, center, radius)".to_string(),
                    ));
                }
                let point = self.eval(&args[0], row)?;
                let center = self.eval(&args[1], row)?;
                let radius = self.eval(&args[2], row)?;
                self.within_radius(point, center, radius)
            }

            "st_ontopof" => {
                if args.len() != 2 {
                    return Err(MoteDBError::InvalidArgument(
                        "ST_OnTopOf() takes 2 arguments".to_string(),
                    ));
                }
                let p1 = self.eval(&args[0], row)?;
                let p2 = self.eval(&args[1], row)?;
                self.st_ontopof(p1, p2)
            }

            // E-SQL Text Functions
            "match" => {
                // MATCH(column, query_text)
                if args.len() != 2 {
                    return Err(MoteDBError::InvalidArgument(
                        "MATCH() takes 2 arguments (column, query_text)".to_string(),
                    ));
                }

                // Get column name (must be a column reference, not expression)
                let column_name = match &args[0] {
                    Expr::Column(name) => name.clone(),
                    _ => {
                        return Err(MoteDBError::InvalidArgument(
                            "MATCH() first argument must be a column name".to_string(),
                        ))
                    }
                };

                // Get query text
                let query_text = match self.eval(&args[1], row)? {
                    Value::Text(s) => s,
                    _ => {
                        return Err(MoteDBError::TypeError(
                            "MATCH() query must be text".to_string(),
                        ))
                    }
                };

                // Get column value
                let column_value = row
                    .get(&column_name)
                    .ok_or_else(|| MoteDBError::ColumnNotFound(column_name.clone()))?;

                // Simple text matching: check if all query terms appear in column
                // (This is a simplified version; real implementation should use BM25 from text_fts index)
                let matches = match column_value {
                    Value::Text(text) => {
                        let text_lower = text.to_lowercase();
                        let query_lower = query_text.to_lowercase();

                        // Split query into terms
                        let terms: Vec<&str> = query_lower.split_whitespace().collect();

                        // Check if all terms appear in text
                        terms.iter().all(|term| text_lower.contains(term))
                    }
                    _ => false,
                };

                Ok(Value::Bool(matches))
            }

            "fts_match" | "fts_search" => Err(MoteDBError::NotImplemented(
                "Full-text search functions not yet implemented".to_string(),
            )),

            // Timestamp functions
            "now" => {
                // NOW() - current timestamp
                if !args.is_empty() {
                    return Err(MoteDBError::InvalidArgument(
                        "NOW() takes no arguments".to_string(),
                    ));
                }
                use crate::types::Timestamp;
                let ts = Timestamp::now();
                Ok(Value::Timestamp(ts))
            }

            "timestamp_micros" => {
                // TIMESTAMP_MICROS(value) - convert integer to timestamp
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "TIMESTAMP_MICROS() takes 1 argument".to_string(),
                    ));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Integer(micros) => {
                        use crate::types::Timestamp;
                        Ok(Value::Timestamp(Timestamp::from_micros(micros)))
                    }
                    _ => Err(MoteDBError::TypeError(
                        "TIMESTAMP_MICROS() requires integer argument".to_string(),
                    )),
                }
            }

            "timestamp_millis" => {
                // TIMESTAMP_MILLIS(value) - convert integer milliseconds to timestamp
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "TIMESTAMP_MILLIS() takes 1 argument".to_string(),
                    ));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Integer(millis) => {
                        use crate::types::Timestamp;
                        Ok(Value::Timestamp(Timestamp::from_millis(millis)))
                    }
                    _ => Err(MoteDBError::TypeError(
                        "TIMESTAMP_MILLIS() requires integer argument".to_string(),
                    )),
                }
            }

            "timestamp_secs" => {
                // TIMESTAMP_SECS(value) - convert integer seconds to timestamp
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "TIMESTAMP_SECS() takes 1 argument".to_string(),
                    ));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Integer(secs) => {
                        use crate::types::Timestamp;
                        Ok(Value::Timestamp(Timestamp::from_secs(secs)))
                    }
                    _ => Err(MoteDBError::TypeError(
                        "TIMESTAMP_SECS() requires integer argument".to_string(),
                    )),
                }
            }

            "to_micros" => {
                // TO_MICROS(timestamp) - extract microseconds from timestamp
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "TO_MICROS() takes 1 argument".to_string(),
                    ));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Timestamp(ts) => Ok(Value::Integer(ts.as_micros())),
                    _ => Err(MoteDBError::TypeError(
                        "TO_MICROS() requires timestamp argument".to_string(),
                    )),
                }
            }

            // 🆕 P1 Date/Time extraction functions
            "year" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "YEAR() takes 1 argument".to_string(),
                    ));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Timestamp(ts) => {
                        let (y, _, _) = Self::days_to_date(ts.as_micros() / 1_000_000 / 86400);
                        Ok(Value::Integer(y))
                    }
                    _ => Err(MoteDBError::TypeError(
                        "YEAR() requires timestamp argument".to_string(),
                    )),
                }
            }

            "month" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "MONTH() takes 1 argument".to_string(),
                    ));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Timestamp(ts) => {
                        let (_, m, _) = Self::days_to_date(ts.as_micros() / 1_000_000 / 86400);
                        Ok(Value::Integer(m))
                    }
                    _ => Err(MoteDBError::TypeError(
                        "MONTH() requires timestamp argument".to_string(),
                    )),
                }
            }

            "day" | "day_of_month" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "DAY() takes 1 argument".to_string(),
                    ));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Timestamp(ts) => {
                        let (_, _, d) = Self::days_to_date(ts.as_micros() / 1_000_000 / 86400);
                        Ok(Value::Integer(d))
                    }
                    _ => Err(MoteDBError::TypeError(
                        "DAY() requires timestamp argument".to_string(),
                    )),
                }
            }

            "hour" => {
                // HOUR(timestamp) - extract hour (0-23)
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "HOUR() takes 1 argument".to_string(),
                    ));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Timestamp(ts) => {
                        let secs = ts.as_micros() / 1_000_000;
                        let hour = (secs % 86400) / 3600;
                        Ok(Value::Integer(hour))
                    }
                    _ => Err(MoteDBError::TypeError(
                        "HOUR() requires timestamp argument".to_string(),
                    )),
                }
            }

            "minute" => {
                // MINUTE(timestamp) - extract minute (0-59)
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "MINUTE() takes 1 argument".to_string(),
                    ));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Timestamp(ts) => {
                        let secs = ts.as_micros() / 1_000_000;
                        let minute = (secs % 3600) / 60;
                        Ok(Value::Integer(minute))
                    }
                    _ => Err(MoteDBError::TypeError(
                        "MINUTE() requires timestamp argument".to_string(),
                    )),
                }
            }

            "second" => {
                // SECOND(timestamp) - extract second (0-59)
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "SECOND() takes 1 argument".to_string(),
                    ));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Timestamp(ts) => {
                        let secs = ts.as_micros() / 1_000_000;
                        let second = secs % 60;
                        Ok(Value::Integer(second))
                    }
                    _ => Err(MoteDBError::TypeError(
                        "SECOND() requires timestamp argument".to_string(),
                    )),
                }
            }

            "date_add" | "dateadd" => {
                // DATE_ADD(timestamp, interval_seconds) - add seconds to timestamp
                if args.len() != 2 {
                    return Err(MoteDBError::InvalidArgument(
                        "DATE_ADD() takes 2 arguments (timestamp, seconds)".to_string(),
                    ));
                }
                let ts = match self.eval(&args[0], row)? {
                    Value::Timestamp(ts) => ts,
                    _ => {
                        return Err(MoteDBError::TypeError(
                            "DATE_ADD() first argument must be timestamp".to_string(),
                        ))
                    }
                };
                let seconds = match self.eval(&args[1], row)? {
                    Value::Integer(i) => i,
                    _ => {
                        return Err(MoteDBError::TypeError(
                            "DATE_ADD() second argument must be integer".to_string(),
                        ))
                    }
                };

                use crate::types::Timestamp;
                let delta_micros = seconds
                    .checked_mul(1_000_000)
                    .and_then(|d| ts.as_micros().checked_add(d))
                    .ok_or_else(|| {
                        MoteDBError::InvalidArgument("DATE_ADD() argument overflow".to_string())
                    })?;
                Ok(Value::Timestamp(Timestamp::from_micros(delta_micros)))
            }

            "date_diff" | "datediff" => {
                // DATE_DIFF(timestamp1, timestamp2) - difference in seconds
                if args.len() != 2 {
                    return Err(MoteDBError::InvalidArgument(
                        "DATE_DIFF() takes 2 arguments (timestamp1, timestamp2)".to_string(),
                    ));
                }
                let ts1 = match self.eval(&args[0], row)? {
                    Value::Timestamp(ts) => ts,
                    _ => {
                        return Err(MoteDBError::TypeError(
                            "DATE_DIFF() first argument must be timestamp".to_string(),
                        ))
                    }
                };
                let ts2 = match self.eval(&args[1], row)? {
                    Value::Timestamp(ts) => ts,
                    _ => {
                        return Err(MoteDBError::TypeError(
                            "DATE_DIFF() second argument must be timestamp".to_string(),
                        ))
                    }
                };

                let diff_micros =
                    ts1.as_micros()
                        .checked_sub(ts2.as_micros())
                        .ok_or_else(|| {
                            MoteDBError::InvalidArgument(
                                "DATE_DIFF() timestamp difference overflow".to_string(),
                            )
                        })?;
                let diff_seconds = diff_micros / 1_000_000;
                Ok(Value::Integer(diff_seconds))
            }

            "time_bucket" => {
                // TIME_BUCKET(interval_string, timestamp) - floor timestamp to bucket boundary
                // Example: TIME_BUCKET('5m', ts) → floors ts to nearest 5-minute boundary
                //          TIME_BUCKET('1h', ts) → floors ts to nearest 1-hour boundary
                if args.len() != 2 {
                    return Err(MoteDBError::InvalidArgument(
                        "TIME_BUCKET() takes 2 arguments (interval, timestamp)".to_string(),
                    ));
                }

                // Parse interval string
                let interval_str = match self.eval(&args[0], row)? {
                    Value::Text(s) => s,
                    _ => return Err(MoteDBError::TypeError(
                        "TIME_BUCKET() first argument must be a text interval like '5m', '1h', '1d'".to_string()
                    )),
                };

                let bucket_micros = parse_interval_to_micros(&interval_str)?;

                let ts = match self.eval(&args[1], row)? {
                    Value::Timestamp(ts) => ts,
                    _ => {
                        return Err(MoteDBError::TypeError(
                            "TIME_BUCKET() second argument must be timestamp".to_string(),
                        ))
                    }
                };

                let floored = (ts.as_micros() / bucket_micros) * bucket_micros;
                use crate::types::Timestamp;
                Ok(Value::Timestamp(Timestamp::from_micros(floored)))
            }

            "day_of_week" | "dow" => {
                // DAY_OF_WEEK(timestamp) - extract day of week (1=Monday, 7=Sunday)
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument(
                        "DAY_OF_WEEK() takes 1 argument".to_string(),
                    ));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Timestamp(ts) => {
                        let secs = ts.as_micros() / 1_000_000;
                        let days = secs / 86400;
                        // Unix epoch (1970-01-01) was Thursday (day 4)
                        // Calculate day of week: (days + 4) % 7, then map to 1-7
                        let dow = ((days + 3).rem_euclid(7)) + 1; // rem_euclid handles negative days
                        Ok(Value::Integer(dow))
                    }
                    _ => Err(MoteDBError::TypeError(
                        "DAY_OF_WEEK() requires timestamp argument".to_string(),
                    )),
                }
            }

            // 🆕 Type conversion function
            "cast" => {
                // CAST(value AS type) - NOTE: In SQL this is special syntax, but we handle as function
                // Usage: CAST(column, 'INTEGER') or CAST(column, 'TEXT')
                if args.len() != 2 {
                    return Err(MoteDBError::InvalidArgument(
                        "CAST() takes 2 arguments (value, target_type)".to_string(),
                    ));
                }
                let val = self.eval(&args[0], row)?;
                // SQL: CAST(NULL AS <any_type>) → NULL
                if matches!(val, Value::Null) {
                    return Ok(Value::Null);
                }
                let target_type = match self.eval(&args[1], row)? {
                    Value::Text(s) => s.to_uppercase(),
                    _ => {
                        return Err(MoteDBError::TypeError(
                            "CAST() target type must be text".to_string(),
                        ))
                    }
                };

                match target_type.as_str() {
                    "INTEGER" | "INT" => {
                        match val {
                            Value::Integer(i) => Ok(Value::Integer(i)),
                            Value::Float(f) => {
                                // Check for overflow: f64 as i64 is UB for out-of-range values
                                if f >= i64::MAX as f64 || f <= i64::MIN as f64 {
                                    return Err(MoteDBError::TypeError(format!(
                                        "Float {} overflows INTEGER range",
                                        f
                                    )));
                                }
                                Ok(Value::Integer(f as i64))
                            }
                            Value::Text(s) => s.parse::<i64>().map(Value::Integer).map_err(|_| {
                                MoteDBError::TypeError("Cannot parse integer".to_string())
                            }),
                            Value::Bool(b) => Ok(Value::Integer(if b { 1 } else { 0 })),
                            Value::Timestamp(ts) => Ok(Value::Integer(ts.as_micros())),
                            _ => Err(MoteDBError::TypeError(format!(
                                "Cannot cast {:?} to INTEGER",
                                val
                            ))),
                        }
                    }
                    "FLOAT" | "REAL" | "DOUBLE" => match val {
                        Value::Float(f) => Ok(Value::Float(f)),
                        Value::Integer(i) => Ok(Value::Float(i as f64)),
                        Value::Text(s) => s
                            .parse::<f64>()
                            .map(Value::Float)
                            .map_err(|_| MoteDBError::TypeError("Cannot parse float".to_string())),
                        _ => Err(MoteDBError::TypeError(format!(
                            "Cannot cast {:?} to FLOAT",
                            val
                        ))),
                    },
                    "TEXT" | "VARCHAR" | "STRING" => {
                        let text = match val {
                            Value::Text(s) => s.as_str().to_string(),
                            Value::Integer(i) => i.to_string(),
                            Value::Float(f) => f.to_string(),
                            Value::Bool(b) => b.to_string(),
                            Value::Null => return Ok(Value::Null),
                            _ => format!("{:?}", val),
                        };
                        Ok(Value::text(text))
                    }
                    "BOOLEAN" | "BOOL" => {
                        let b = self.to_bool(&val)?;
                        Ok(Value::Bool(b))
                    }
                    "TIMESTAMP" => match val {
                        Value::Timestamp(ts) => Ok(Value::Timestamp(ts)),
                        Value::Integer(micros) => {
                            use crate::types::Timestamp;
                            Ok(Value::Timestamp(Timestamp::from_micros(micros)))
                        }
                        _ => Err(MoteDBError::TypeError(format!(
                            "Cannot cast {:?} to TIMESTAMP",
                            val
                        ))),
                    },
                    _ => Err(MoteDBError::TypeError(format!(
                        "Unknown target type: {}",
                        target_type
                    ))),
                }
            }

            _ => Err(MoteDBError::UnknownFunction(name.to_string())),
        }
    }

    // Helper functions

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

    fn to_float(&self, val: &Value) -> Result<f64> {
        match val {
            Value::Float(f) => Ok(*f),
            Value::Integer(i) => Ok(*i as f64),
            _ => Err(MoteDBError::TypeError(
                "Cannot convert to float".to_string(),
            )),
        }
    }

    fn add_values(&self, left: Value, right: Value) -> Result<Value> {
        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => match l.checked_add(r) {
                Some(n) => Ok(Value::Integer(n)),
                None => Ok(Value::Float(l as f64 + r as f64)),
            },
            (Value::Float(l), Value::Float(r)) => Ok(Value::Float(l + r)),
            (Value::Integer(l), Value::Float(r)) => Ok(Value::Float(l as f64 + r)),
            (Value::Float(l), Value::Integer(r)) => Ok(Value::Float(l + r as f64)),
            (Value::Text(l), Value::Text(r)) => Ok(Value::text(format!("{}{}", l, r))),
            _ => Err(MoteDBError::TypeError("Cannot add these types".to_string())),
        }
    }

    fn sub_values(&self, left: Value, right: Value) -> Result<Value> {
        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => match l.checked_sub(r) {
                Some(n) => Ok(Value::Integer(n)),
                None => Ok(Value::Float(l as f64 - r as f64)),
            },
            (Value::Float(l), Value::Float(r)) => Ok(Value::Float(l - r)),
            (Value::Integer(l), Value::Float(r)) => Ok(Value::Float(l as f64 - r)),
            (Value::Float(l), Value::Integer(r)) => Ok(Value::Float(l - r as f64)),
            _ => Err(MoteDBError::TypeError(
                "Cannot subtract these types".to_string(),
            )),
        }
    }

    fn mul_values(&self, left: Value, right: Value) -> Result<Value> {
        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => match l.checked_mul(r) {
                Some(n) => Ok(Value::Integer(n)),
                None => Ok(Value::Float(l as f64 * r as f64)),
            },
            (Value::Float(l), Value::Float(r)) => Ok(Value::Float(l * r)),
            (Value::Integer(l), Value::Float(r)) => Ok(Value::Float(l as f64 * r)),
            (Value::Float(l), Value::Integer(r)) => Ok(Value::Float(l * r as f64)),
            _ => Err(MoteDBError::TypeError(
                "Cannot multiply these types".to_string(),
            )),
        }
    }

    fn div_values(&self, left: Value, right: Value) -> Result<Value> {
        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => {
                if r == 0 {
                    return Err(MoteDBError::DivisionByZero);
                }
                match l.checked_div(r) {
                    Some(n) => Ok(Value::Integer(n)),
                    None => Ok(Value::Float(l as f64 / r as f64)), // i64::MIN / -1
                }
            }
            (Value::Float(l), Value::Float(r)) => {
                if r == 0.0 {
                    return Err(MoteDBError::DivisionByZero);
                }
                Ok(Value::Float(l / r))
            }
            (Value::Integer(l), Value::Float(r)) => {
                if r == 0.0 {
                    return Err(MoteDBError::DivisionByZero);
                }
                Ok(Value::Float(l as f64 / r))
            }
            (Value::Float(l), Value::Integer(r)) => {
                if r == 0 {
                    return Err(MoteDBError::DivisionByZero);
                }
                Ok(Value::Float(l / r as f64))
            }
            _ => Err(MoteDBError::TypeError(
                "Cannot divide these types".to_string(),
            )),
        }
    }

    fn mod_values(&self, left: Value, right: Value) -> Result<Value> {
        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => {
                if r == 0 {
                    return Err(MoteDBError::DivisionByZero);
                }
                // checked_rem guards against i64::MIN % -1 overflow
                Ok(match l.checked_rem(r) {
                    Some(n) => Value::Integer(n),
                    None => Value::Integer(0), // x % -1 == 0 for any x
                })
            }
            _ => Err(MoteDBError::TypeError(
                "Modulo only works on integers".to_string(),
            )),
        }
    }

    /// ⚡ LIKE pattern matching with compilation cache (5-10x faster)
    /// Caches compiled patterns for repeated use
    #[inline]
    fn like_match_cached(&self, text: &str, pattern: &str) -> bool {
        // Fast path: check read-only cache first
        {
            if let Ok(cache) = self.pattern_cache.read() {
                if let Some(compiled) = cache.get(pattern) {
                    return compiled.matches(text);
                }
            }
        }

        // Slow path: compile and cache pattern
        let compiled = CompiledPattern::compile(pattern);
        let result = compiled.matches(text);

        // Insert into cache (write lock)
        {
            if let Ok(mut cache) = self.pattern_cache.write() {
                // Limit cache size to prevent memory bloat
                if cache.len() < 1000 {
                    cache.insert(pattern.to_string(), compiled);
                }
            }
        }

        result
    }

    // E-SQL Vector Distance Functions

    /// L2 Distance (Euclidean): <->
    fn l2_distance(&self, left: Value, right: Value) -> Result<Value> {
        let (v1, v2) = self.extract_vectors(left, right)?;

        if v1.len() != v2.len() {
            return Err(MoteDBError::TypeError(format!(
                "Vector dimension mismatch: {} vs {}",
                v1.len(),
                v2.len()
            )));
        }

        let dist: f32 = v1
            .iter()
            .zip(v2.iter())
            .map(|(a, b)| (a - b).powi(2))
            .sum::<f32>()
            .sqrt();

        Ok(Value::Float(dist as f64))
    }

    /// Cosine Distance: <=>
    /// Returns 1 - cosine_similarity, range [0, 2]
    fn cosine_distance(&self, left: Value, right: Value) -> Result<Value> {
        let (v1, v2) = self.extract_vectors(left, right)?;

        if v1.len() != v2.len() {
            return Err(MoteDBError::TypeError(format!(
                "Vector dimension mismatch: {} vs {}",
                v1.len(),
                v2.len()
            )));
        }

        let dot: f32 = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();
        let norm1: f32 = v1.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm2: f32 = v2.iter().map(|x| x * x).sum::<f32>().sqrt();

        if norm1 == 0.0 || norm2 == 0.0 {
            return Ok(Value::Float(1.0)); // Maximum distance for zero vectors
        }

        let cosine_sim = (dot / (norm1 * norm2)).clamp(-1.0, 1.0);
        let dist = 1.0 - cosine_sim; // Range: [0, 2]

        Ok(Value::Float(dist as f64))
    }

    /// Dot Product (Inner Product): <#>
    fn dot_product(&self, left: Value, right: Value) -> Result<Value> {
        let (v1, v2) = self.extract_vectors(left, right)?;

        if v1.len() != v2.len() {
            return Err(MoteDBError::TypeError(format!(
                "Vector dimension mismatch: {} vs {}",
                v1.len(),
                v2.len()
            )));
        }

        let dot: f32 = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();

        Ok(Value::Float(dot as f64))
    }

    /// Extract vectors from Value types
    fn extract_vectors(&self, left: Value, right: Value) -> Result<(Vec<f32>, Vec<f32>)> {
        let v1 = match left {
            Value::Vector(v) => v.to_vec(),
            Value::Tensor(t) => t.as_f32().to_vec(),
            _ => {
                return Err(MoteDBError::TypeError(format!(
                    "Left operand is not a vector: {:?}",
                    left
                )))
            }
        };

        let v2 = match right {
            Value::Vector(v) => v.to_vec(),
            Value::Tensor(t) => t.as_f32().to_vec(),
            _ => {
                return Err(MoteDBError::TypeError(format!(
                    "Right operand is not a vector: {:?}",
                    right
                )))
            }
        };

        Ok((v1, v2))
    }

    // E-SQL Spatial Functions

    /// ST_Distance: Compute 3D Euclidean distance between two spatial points
    fn st_distance(&self, p1: Value, p2: Value) -> Result<Value> {
        use crate::types::Geometry;

        let (x1, y1, z1) = match p1 {
            Value::Spatial(g) => match &*g {
                Geometry::Point(p) => (p.x, p.y, 0.0),
                Geometry::Point3D(p) => (p.x, p.y, p.z),
                _ => {
                    return Err(MoteDBError::TypeError(
                        "ST_Distance requires spatial point arguments".to_string(),
                    ))
                }
            },
            _ => {
                return Err(MoteDBError::TypeError(
                    "ST_Distance requires spatial point arguments".to_string(),
                ))
            }
        };

        let (x2, y2, z2) = match p2 {
            Value::Spatial(g) => match &*g {
                Geometry::Point(p) => (p.x, p.y, 0.0),
                Geometry::Point3D(p) => (p.x, p.y, p.z),
                _ => {
                    return Err(MoteDBError::TypeError(
                        "ST_Distance requires spatial point arguments".to_string(),
                    ))
                }
            },
            _ => {
                return Err(MoteDBError::TypeError(
                    "ST_Distance requires spatial point arguments".to_string(),
                ))
            }
        };

        let dist = ((x1 - x2).powi(2) + (y1 - y2).powi(2) + (z1 - z2).powi(2)).sqrt();
        Ok(Value::Float(dist))
    }

    /// WITHIN_RADIUS: Check if a point is within radius of a center point
    fn within_radius(&self, point: Value, center: Value, radius: Value) -> Result<Value> {
        use crate::types::Geometry;

        let (px, py) = match point {
            Value::Spatial(g) => match &*g {
                Geometry::Point(p) => (p.x, p.y),
                Geometry::Point3D(p) => (p.x, p.y),
                _ => {
                    return Err(MoteDBError::TypeError(
                        "WITHIN_RADIUS requires spatial point for first argument".to_string(),
                    ))
                }
            },
            _ => {
                return Err(MoteDBError::TypeError(
                    "WITHIN_RADIUS requires spatial point for first argument".to_string(),
                ))
            }
        };

        let (cx, cy) = match center {
            Value::Spatial(g) => match &*g {
                Geometry::Point(p) => (p.x, p.y),
                Geometry::Point3D(p) => (p.x, p.y),
                _ => {
                    return Err(MoteDBError::TypeError(
                        "WITHIN_RADIUS requires spatial point for center".to_string(),
                    ))
                }
            },
            _ => {
                return Err(MoteDBError::TypeError(
                    "WITHIN_RADIUS requires spatial point for center".to_string(),
                ))
            }
        };

        let r = match radius {
            Value::Float(r) => r,
            Value::Integer(i) => i as f64,
            _ => {
                return Err(MoteDBError::TypeError(
                    "WITHIN_RADIUS requires numeric radius".to_string(),
                ))
            }
        };

        let dist = ((px - cx).powi(2) + (py - cy).powi(2)).sqrt();
        Ok(Value::Bool(dist <= r))
    }

    /// ST_OnTopOf: Check if point p1 is on top of point p2 (p1.y > p2.y)
    fn st_ontopof(&self, p1: Value, p2: Value) -> Result<Value> {
        use crate::types::Geometry;

        let point1 = match p1 {
            Value::Spatial(g) => match &*g {
                Geometry::Point(p) => *p,
                _ => {
                    return Err(MoteDBError::TypeError(
                        "ST_OnTopOf requires spatial point arguments".to_string(),
                    ))
                }
            },
            _ => {
                return Err(MoteDBError::TypeError(
                    "ST_OnTopOf requires spatial point arguments".to_string(),
                ))
            }
        };

        let point2 = match p2 {
            Value::Spatial(g) => match &*g {
                Geometry::Point(p) => *p,
                _ => {
                    return Err(MoteDBError::TypeError(
                        "ST_OnTopOf requires spatial point arguments".to_string(),
                    ))
                }
            },
            _ => {
                return Err(MoteDBError::TypeError(
                    "ST_OnTopOf requires spatial point arguments".to_string(),
                ))
            }
        };

        // In 2D, "on top of" means higher Y coordinate
        // Also check if X coordinates are close (within same vertical region)
        let same_region = (point1.x - point2.x).abs() < 1.0; // Within 1 unit horizontally
        let above = point1.y > point2.y;

        Ok(Value::Bool(same_region && above))
    }
}

/// Parse interval string like '5m', '1h', '30s', '1d' to microseconds.
fn parse_interval_to_micros(interval: &str) -> crate::Result<i64> {
    let interval = interval.trim();
    if interval.is_empty() {
        return Err(crate::MoteDBError::InvalidArgument(
            "Empty interval string".to_string(),
        ));
    }

    // Split into numeric part and unit
    let (num_str, unit) = if let Some(pos) = interval.find(|c: char| !c.is_ascii_digit()) {
        (&interval[..pos], &interval[pos..])
    } else {
        (interval, "s")
    };

    let num: i64 = num_str.parse().map_err(|_| {
        crate::MoteDBError::InvalidArgument(format!("Invalid interval number: '{}'", num_str))
    })?;

    if num <= 0 {
        return Err(crate::MoteDBError::InvalidArgument(
            "Interval must be positive".to_string(),
        ));
    }

    let micros = match unit {
        "s" | "sec" | "second" | "seconds" => num.checked_mul(1_000_000),
        "m" | "min" | "minute" | "minutes" => {
            num.checked_mul(60).and_then(|v| v.checked_mul(1_000_000))
        }
        "h" | "hr" | "hour" | "hours" => {
            num.checked_mul(3600).and_then(|v| v.checked_mul(1_000_000))
        }
        "d" | "day" | "days" => num
            .checked_mul(86400)
            .and_then(|v| v.checked_mul(1_000_000)),
        _ => {
            return Err(crate::MoteDBError::InvalidArgument(format!(
                "Unknown interval unit: '{}'. Use s/m/h/d",
                unit
            )))
        }
    };

    let micros = micros.ok_or_else(|| {
        crate::MoteDBError::InvalidArgument(format!("Interval value overflow: {}", interval))
    })?;

    Ok(micros)
}

impl Default for ExprEvaluator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::{BinaryOperator, Expr, UnaryOperator};
    use crate::types::Value;

    fn eval(expr: &Expr, row: &SqlRow) -> Result<Value> {
        ExprEvaluator::new().eval(expr, row)
    }

    fn row(vals: &[(&str, Value)]) -> SqlRow {
        let mut m = SqlRow::new();
        for (k, v) in vals {
            m.insert(k.to_string(), v.clone());
        }
        m
    }

    fn lit_int(v: i64) -> Expr {
        Expr::Literal(Value::Integer(v))
    }

    fn lit_float(v: f64) -> Expr {
        Expr::Literal(Value::Float(v))
    }

    fn lit_text(v: &str) -> Expr {
        Expr::Literal(Value::Text(crate::types::ArcString::from(v)))
    }

    fn lit_null() -> Expr {
        Expr::Literal(Value::Null)
    }

    fn col(name: &str) -> Expr {
        Expr::Column(name.to_string())
    }

    // ━━━ Literals ━━━

    #[test]
    fn test_eval_literal_int() {
        assert_eq!(eval(&lit_int(42), &row(&[])).unwrap(), Value::Integer(42));
    }

    #[test]
    fn test_eval_literal_null() {
        assert_eq!(eval(&lit_null(), &row(&[])).unwrap(), Value::Null);
    }

    // ━━━ Column reference ━━━

    #[test]
    fn test_eval_column() {
        let r = row(&[
            ("id", Value::Integer(1)),
            ("name", Value::Text("alice".into())),
        ]);
        assert_eq!(eval(&col("id"), &r).unwrap(), Value::Integer(1));
        assert_eq!(eval(&col("name"), &r).unwrap(), Value::Text("alice".into()));
    }

    // ━━━ Arithmetic ━━━

    #[test]
    fn test_eval_arithmetic() {
        let r = row(&[]);
        let add = Expr::BinaryOp {
            left: Box::new(lit_int(3)),
            op: BinaryOperator::Add,
            right: Box::new(lit_int(4)),
        };
        assert_eq!(eval(&add, &r).unwrap(), Value::Integer(7));

        let sub = Expr::BinaryOp {
            left: Box::new(lit_int(10)),
            op: BinaryOperator::Sub,
            right: Box::new(lit_int(3)),
        };
        assert_eq!(eval(&sub, &r).unwrap(), Value::Integer(7));

        let mul = Expr::BinaryOp {
            left: Box::new(lit_int(6)),
            op: BinaryOperator::Mul,
            right: Box::new(lit_int(7)),
        };
        assert_eq!(eval(&mul, &r).unwrap(), Value::Integer(42));

        let div = Expr::BinaryOp {
            left: Box::new(lit_float(10.0)),
            op: BinaryOperator::Div,
            right: Box::new(lit_float(4.0)),
        };
        assert_eq!(eval(&div, &r).unwrap(), Value::Float(2.5));
    }

    // ━━━ Comparison ━━━

    #[test]
    fn test_eval_comparison() {
        let r = row(&[]);
        let eq = Expr::BinaryOp {
            left: Box::new(lit_int(1)),
            op: BinaryOperator::Eq,
            right: Box::new(lit_int(1)),
        };
        assert_eq!(eval(&eq, &r).unwrap(), Value::Bool(true));

        let ne = Expr::BinaryOp {
            left: Box::new(lit_int(1)),
            op: BinaryOperator::Ne,
            right: Box::new(lit_int(2)),
        };
        assert_eq!(eval(&ne, &r).unwrap(), Value::Bool(true));

        let lt = Expr::BinaryOp {
            left: Box::new(lit_int(1)),
            op: BinaryOperator::Lt,
            right: Box::new(lit_int(2)),
        };
        assert_eq!(eval(&lt, &r).unwrap(), Value::Bool(true));

        let gt = Expr::BinaryOp {
            left: Box::new(lit_int(2)),
            op: BinaryOperator::Gt,
            right: Box::new(lit_int(1)),
        };
        assert_eq!(eval(&gt, &r).unwrap(), Value::Bool(true));
    }

    #[test]
    fn test_eval_eq_null() {
        let r = row(&[]);
        // Full evaluator: NULL = anything → false (WHERE-style filtering)
        let eq = Expr::BinaryOp {
            left: Box::new(lit_null()),
            op: BinaryOperator::Eq,
            right: Box::new(lit_int(1)),
        };
        assert_eq!(eval(&eq, &r).unwrap(), Value::Bool(false));
    }

    // ━━━ Logic ━━━

    #[test]
    fn test_eval_and_or() {
        let r = row(&[]);
        let and_t = Expr::BinaryOp {
            left: Box::new(lit_int(1)),
            op: BinaryOperator::And,
            right: Box::new(lit_int(1)),
        };
        assert_eq!(eval(&and_t, &r).unwrap(), Value::Bool(true));

        let and_f = Expr::BinaryOp {
            left: Box::new(lit_int(1)),
            op: BinaryOperator::And,
            right: Box::new(lit_int(0)),
        };
        assert_eq!(eval(&and_f, &r).unwrap(), Value::Bool(false));

        let or_t = Expr::BinaryOp {
            left: Box::new(lit_int(0)),
            op: BinaryOperator::Or,
            right: Box::new(lit_int(1)),
        };
        assert_eq!(eval(&or_t, &r).unwrap(), Value::Bool(true));
    }

    // ━━━ IS NULL ━━━

    #[test]
    fn test_eval_is_null() {
        let r = row(&[]);
        let isnull = Expr::IsNull {
            expr: Box::new(lit_null()),
            negated: false,
        };
        assert_eq!(eval(&isnull, &r).unwrap(), Value::Bool(true));

        let notnull = Expr::IsNull {
            expr: Box::new(lit_int(1)),
            negated: true,
        };
        assert_eq!(eval(&notnull, &r).unwrap(), Value::Bool(true));
    }

    // ━━━ IN ━━━

    #[test]
    fn test_eval_in() {
        let r = row(&[]);
        let in_true = Expr::In {
            expr: Box::new(lit_int(2)),
            list: vec![lit_int(1), lit_int(2), lit_int(3)],
            negated: false,
        };
        assert_eq!(eval(&in_true, &r).unwrap(), Value::Bool(true));

        let in_false = Expr::In {
            expr: Box::new(lit_int(99)),
            list: vec![lit_int(1), lit_int(2)],
            negated: false,
        };
        assert_eq!(eval(&in_false, &r).unwrap(), Value::Bool(false));
    }

    // ━━━ BETWEEN ━━━

    #[test]
    fn test_eval_between() {
        let r = row(&[]);
        let bt = Expr::Between {
            expr: Box::new(lit_int(5)),
            low: Box::new(lit_int(1)),
            high: Box::new(lit_int(10)),
            negated: false,
        };
        assert_eq!(eval(&bt, &r).unwrap(), Value::Bool(true));

        let nb = Expr::Between {
            expr: Box::new(lit_int(0)),
            low: Box::new(lit_int(1)),
            high: Box::new(lit_int(10)),
            negated: true,
        };
        assert_eq!(eval(&nb, &r).unwrap(), Value::Bool(true));
    }

    // ━━━ LIKE ━━━

    #[test]
    fn test_eval_like() {
        let r = row(&[]);
        let like = Expr::Like {
            expr: Box::new(lit_text("hello world")),
            pattern: Box::new(lit_text("%world%")),
            negated: false,
        };
        assert_eq!(eval(&like, &r).unwrap(), Value::Bool(true));

        let nlike = Expr::Like {
            expr: Box::new(lit_text("hello")),
            pattern: Box::new(lit_text("%xyz%")),
            negated: true,
        };
        assert_eq!(eval(&nlike, &r).unwrap(), Value::Bool(true));
    }

    // ━━━ NOT ━━━

    #[test]
    fn test_eval_not() {
        let r = row(&[]);
        let not_true = Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(lit_int(1)),
        };
        assert_eq!(eval(&not_true, &r).unwrap(), Value::Bool(false));

        let not_false = Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(lit_int(0)),
        };
        assert_eq!(eval(&not_false, &r).unwrap(), Value::Bool(true));
    }

    // ━━━ Functions ━━━
    // COALESCE, IFNULL, NULLIF are the most commonly used

    #[test]
    fn test_eval_coalesce() {
        let r = row(&[]);
        // COALESCE(NULL, NULL, 42) = 42
        let func = Expr::FunctionCall {
            name: "COALESCE".to_string(),
            args: vec![lit_null(), lit_null(), lit_int(42)],
            distinct: false,
        };
        assert_eq!(eval(&func, &r).unwrap(), Value::Integer(42));

        // COALESCE(1, 2) = 1
        let func2 = Expr::FunctionCall {
            name: "COALESCE".to_string(),
            args: vec![lit_int(1), lit_int(2)],
            distinct: false,
        };
        assert_eq!(eval(&func2, &r).unwrap(), Value::Integer(1));
    }

    #[test]
    fn test_eval_ifnull() {
        let r = row(&[]);
        // IFNULL(NULL, 'fallback') = 'fallback'
        let func = Expr::FunctionCall {
            name: "IFNULL".to_string(),
            args: vec![lit_null(), lit_text("fallback")],
            distinct: false,
        };
        assert_eq!(eval(&func, &r).unwrap(), Value::Text("fallback".into()));

        // IFNULL(1, 2) = 1
        let func2 = Expr::FunctionCall {
            name: "IFNULL".to_string(),
            args: vec![lit_int(1), lit_int(2)],
            distinct: false,
        };
        assert_eq!(eval(&func2, &r).unwrap(), Value::Integer(1));
    }

    #[test]
    fn test_eval_abs_round() {
        let r = row(&[]);
        let abs = Expr::FunctionCall {
            name: "ABS".to_string(),
            args: vec![lit_int(-5)],
            distinct: false,
        };
        assert_eq!(eval(&abs, &r).unwrap(), Value::Integer(5));

        let round = Expr::FunctionCall {
            name: "ROUND".to_string(),
            args: vec![lit_float(3.7)],
            distinct: false,
        };
        assert_eq!(eval(&round, &r).unwrap(), Value::Float(4.0));
    }

    #[test]
    fn test_eval_length() {
        let r = row(&[]);
        let len = Expr::FunctionCall {
            name: "LENGTH".to_string(),
            args: vec![lit_text("hello")],
            distinct: false,
        };
        assert_eq!(eval(&len, &r).unwrap(), Value::Integer(5));
    }

    // ========== Regression tests ==========

    #[test]
    fn test_or_with_null_true_returns_true() {
        // TRUE OR NULL should be TRUE (not false)
        let r = row(&[]);
        let true_expr = Expr::BinaryOp {
            left: Box::new(lit_int(1)),
            op: BinaryOperator::Eq,
            right: Box::new(lit_int(1)),
        };
        let null_expr = Expr::Literal(Value::Null);

        // TRUE OR NULL
        let or_expr = Expr::BinaryOp {
            left: Box::new(true_expr.clone()),
            op: BinaryOperator::Or,
            right: Box::new(null_expr.clone()),
        };
        assert_eq!(
            eval(&or_expr, &r).unwrap(),
            Value::Bool(true),
            "TRUE OR NULL should be TRUE"
        );

        // NULL OR TRUE
        let or_expr2 = Expr::BinaryOp {
            left: Box::new(null_expr),
            op: BinaryOperator::Or,
            right: Box::new(true_expr),
        };
        assert_eq!(
            eval(&or_expr2, &r).unwrap(),
            Value::Bool(true),
            "NULL OR TRUE should be TRUE"
        );

        // FALSE OR NULL should be FALSE (unknown → false for WHERE)
        let false_expr = Expr::BinaryOp {
            left: Box::new(lit_int(1)),
            op: BinaryOperator::Eq,
            right: Box::new(lit_int(2)),
        };
        let or_expr3 = Expr::BinaryOp {
            left: Box::new(false_expr),
            op: BinaryOperator::Or,
            right: Box::new(Expr::Literal(Value::Null)),
        };
        // FALSE OR NULL = NULL in SQL three-valued logic (was incorrectly FALSE)
        assert!(
            matches!(eval(&or_expr3, &r).unwrap(), Value::Null),
            "FALSE OR NULL should be NULL"
        );
    }

    #[test]
    fn test_and_with_null_false_returns_false() {
        let r = row(&[]);
        let false_expr = Expr::BinaryOp {
            left: Box::new(lit_int(1)),
            op: BinaryOperator::Eq,
            right: Box::new(lit_int(2)),
        };

        // FALSE AND NULL should be FALSE
        let and_expr = Expr::BinaryOp {
            left: Box::new(false_expr),
            op: BinaryOperator::And,
            right: Box::new(Expr::Literal(Value::Null)),
        };
        assert_eq!(
            eval(&and_expr, &r).unwrap(),
            Value::Bool(false),
            "FALSE AND NULL should be FALSE"
        );
    }

    #[test]
    fn test_unary_minus_i64_min() {
        // i64::MIN negation should not panic
        let r = row(&[]);
        let expr = Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: Box::new(lit_int(i64::MIN)),
        };
        let result = eval(&expr, &r).unwrap();
        // Should promote to float since -i64::MIN overflows
        match result {
            Value::Float(f) => assert!(f > 0.0, "negated i64::MIN should be positive float"),
            Value::Integer(i) => assert!(i > 0, "negated i64::MIN should be positive"),
            other => panic!("expected numeric, got {:?}", other),
        }
    }

    #[test]
    fn test_integer_float_cross_type_equality() {
        // Integer(1) == Float(1.0) should be true
        let r = row(&[]);
        let expr = Expr::BinaryOp {
            left: Box::new(lit_int(1)),
            op: BinaryOperator::Eq,
            right: Box::new(lit_float(1.0)),
        };
        assert_eq!(
            eval(&expr, &r).unwrap(),
            Value::Bool(true),
            "Integer(1) == Float(1.0) should be true"
        );

        // Integer(1) != Float(2.0)
        let expr2 = Expr::BinaryOp {
            left: Box::new(lit_int(1)),
            op: BinaryOperator::Eq,
            right: Box::new(lit_float(2.0)),
        };
        assert_eq!(
            eval(&expr2, &r).unwrap(),
            Value::Bool(false),
            "Integer(1) == Float(2.0) should be false"
        );
    }

    #[test]
    fn test_abs_i64_min() {
        // abs(i64::MIN) should promote to float, not panic or wrap
        let r = row(&[]);
        let expr = Expr::FunctionCall {
            name: "abs".to_string(),
            args: vec![lit_int(i64::MIN)],
            distinct: false,
        };
        let result = eval(&expr, &r).unwrap();
        match result {
            Value::Float(f) => {
                assert!(f > 0.0, "abs(i64::MIN) should be positive float, got {}", f)
            }
            Value::Integer(i) => assert!(i > 0, "abs(i64::MIN) should be positive"),
            other => panic!("expected numeric, got {:?}", other),
        }
    }

    #[test]
    fn test_add_i64_max_overflow() {
        // i64::MAX + 1 should promote to float
        let r = row(&[]);
        let expr = Expr::BinaryOp {
            left: Box::new(lit_int(i64::MAX)),
            op: BinaryOperator::Add,
            right: Box::new(lit_int(1)),
        };
        let result = eval(&expr, &r).unwrap();
        assert!(
            matches!(result, Value::Float(_)),
            "overflow add should be float, got {:?}",
            result
        );
    }

    #[test]
    fn test_sub_i64_min_overflow() {
        // i64::MIN - 1 should promote to float
        let r = row(&[]);
        let expr = Expr::BinaryOp {
            left: Box::new(lit_int(i64::MIN)),
            op: BinaryOperator::Sub,
            right: Box::new(lit_int(1)),
        };
        let result = eval(&expr, &r).unwrap();
        assert!(
            matches!(result, Value::Float(_)),
            "overflow sub should be float, got {:?}",
            result
        );
    }

    #[test]
    fn test_mul_i64_max_overflow() {
        // i64::MAX * 2 should promote to float
        let r = row(&[]);
        let expr = Expr::BinaryOp {
            left: Box::new(lit_int(i64::MAX)),
            op: BinaryOperator::Mul,
            right: Box::new(lit_int(2)),
        };
        let result = eval(&expr, &r).unwrap();
        assert!(
            matches!(result, Value::Float(_)),
            "overflow mul should be float, got {:?}",
            result
        );
    }

    #[test]
    fn test_div_i64_min_by_neg1() {
        // i64::MIN / -1 should promote to float
        let r = row(&[]);
        let expr = Expr::BinaryOp {
            left: Box::new(lit_int(i64::MIN)),
            op: BinaryOperator::Div,
            right: Box::new(lit_int(-1)),
        };
        let result = eval(&expr, &r).unwrap();
        assert!(
            matches!(result, Value::Float(_)),
            "i64::MIN / -1 should be float, got {:?}",
            result
        );
    }

    #[test]
    fn test_mod_i64_min_by_neg1() {
        // i64::MIN % -1 should return 0 (math: -1 divides any integer evenly)
        let r = row(&[]);
        let expr = Expr::FunctionCall {
            name: "mod".to_string(),
            args: vec![lit_int(i64::MIN), lit_int(-1)],
            distinct: false,
        };
        let result = eval(&expr, &r).unwrap();
        assert_eq!(result, Value::Integer(0), "i64::MIN % -1 should be 0");
    }
}
