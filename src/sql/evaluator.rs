/// Expression evaluator - evaluates expressions against rows
use super::ast::{Expr, BinaryOperator, UnaryOperator};
use crate::error::{Result, MoteDBError};
use crate::types::{Value, SqlRow};
use crate::database::MoteDB;
use std::sync::Arc;
use std::collections::HashMap;
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
    AnyChar,      // _
    AnyChars,     // %
}

impl CompiledPattern {
    /// Compile LIKE pattern into optimized form
    fn compile(pattern: &str) -> Self {
        // Fast path: no wildcards
        if !pattern.contains('%') && !pattern.contains('_') {
            return CompiledPattern::Exact(pattern.to_string());
        }
        
        // Fast path: prefix match "abc%"
        if pattern.ends_with('%') && !pattern[..pattern.len()-1].contains('%') && !pattern.contains('_') {
            return CompiledPattern::Prefix(pattern[..pattern.len()-1].to_string());
        }
        
        // Fast path: suffix match "%abc"
        if pattern.starts_with('%') && !pattern[1..].contains('%') && !pattern.contains('_') {
            return CompiledPattern::Suffix(pattern[1..].to_string());
        }
        
        // Fast path: contains match "%abc%"
        if pattern.starts_with('%') && pattern.ends_with('%') 
            && pattern.len() > 2
            && !pattern[1..pattern.len()-1].contains('%') 
            && !pattern.contains('_') 
        {
            return CompiledPattern::Contains(pattern[1..pattern.len()-1].to_string());
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
    
    /// Match complex pattern with segments
    fn match_complex(text: &str, segments: &[PatternSegment]) -> bool {
        let text_chars: Vec<char> = text.chars().collect();
        Self::match_segments(&text_chars, segments, 0, 0)
    }
    
    fn match_segments(text: &[char], segments: &[PatternSegment], ti: usize, si: usize) -> bool {
        // All segments matched
        if si >= segments.len() {
            return ti >= text.len();
        }
        
        match &segments[si] {
            PatternSegment::AnyChars => {
                // Try matching 0 or more characters
                if Self::match_segments(text, segments, ti, si + 1) {
                    return true;
                }
                if ti < text.len() && Self::match_segments(text, segments, ti + 1, si) {
                    return true;
                }
                false
            }
            PatternSegment::AnyChar => {
                // Match exactly one character
                if ti < text.len() {
                    Self::match_segments(text, segments, ti + 1, si + 1)
                } else {
                    false
                }
            }
            PatternSegment::Literal(literal) => {
                // Match literal string
                let chars: Vec<char> = literal.chars().collect();
                if ti + chars.len() > text.len() {
                    return false;
                }
                for (i, &c) in chars.iter().enumerate() {
                    if text[ti + i] != c {
                        return false;
                    }
                }
                Self::match_segments(text, segments, ti + chars.len(), si + 1)
            }
        }
    }
}

pub struct ExprEvaluator {
    #[allow(dead_code)]
    db: Option<Arc<MoteDB>>,  // Optional database reference for MATCH() function
    /// ⚡ Pattern cache: pattern string -> compiled pattern
    /// RwLock for concurrent read access (common case)
    pattern_cache: Arc<RwLock<HashMap<String, CompiledPattern>>>,
}

impl ExprEvaluator {
    pub fn new() -> Self {
        Self { 
            db: None,
            pattern_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    pub fn with_db(db: Arc<MoteDB>) -> Self {
        Self { 
            db: Some(db),
            pattern_cache: Arc::new(RwLock::new(HashMap::new())),
        }
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
                for (key, value) in row.iter() {
                    // Skip metadata columns
                    if key.starts_with("__") {
                        continue;
                    }
                    
                    // Match: key ends with ".{name}"
                    if key.ends_with(&format!(".{}", name)) {
                        return Ok(value.clone());
                    }
                }
                
                // Try 3: Case-insensitive match (for robustness)
                let name_lower = name.to_lowercase();
                for (key, value) in row.iter() {
                    if key.to_lowercase() == name_lower {
                        return Ok(value.clone());
                    }
                }
                
                Err(MoteDBError::ColumnNotFound(name.clone()))
            }
            
            Expr::Literal(val) => Ok(val.clone()),
            
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.eval(left, row)?;
                let right_val = self.eval(right, row)?;
                self.eval_binary_op(op, left_val, right_val)
            }
            
            Expr::UnaryOp { op, expr } => {
                let val = self.eval(expr, row)?;
                self.eval_unary_op(op, val)
            }
            
            Expr::FunctionCall { name, args, distinct } => {
                self.eval_function(name, args, *distinct, row)
            }
            
            Expr::In { expr, list, negated } => {
                let val = self.eval(expr, row)?;
                let mut found = false;
                
                // Handle subquery IN: expr IN (SELECT ...)
                // If list contains a single Subquery, it needs special handling
                // But we can't execute subqueries here - they're handled by executor
                // So we'll just evaluate literal lists here
                
                for item in list {
                    let item_val = self.eval(item, row)?;
                    if val == item_val {
                        found = true;
                        break;
                    }
                }
                Ok(Value::Bool(if *negated { !found } else { found }))
            }
            
            Expr::Between { expr, low, high, negated } => {
                let val = self.eval(expr, row)?;
                let low_val = self.eval(low, row)?;
                let high_val = self.eval(high, row)?;
                
                let in_range = val >= low_val && val <= high_val;
                Ok(Value::Bool(if *negated { !in_range } else { in_range }))
            }
            
            Expr::Like { expr, pattern, negated } => {
                let val = self.eval(expr, row)?;
                let pattern_val = self.eval(pattern, row)?;
                
                let matches = if let (Value::Text(s), Value::Text(p)) = (val, pattern_val) {
                    // ⚡ Use compiled pattern cache for fast matching
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
            
            Expr::Subquery(_) => {
                // Subqueries are handled at executor level, not here
                Err(MoteDBError::Query("Subquery evaluation must be done by executor".into()))
            }
            
            Expr::Match { .. } => {
                // MATCH...AGAINST is handled at executor level (requires index access)
                Err(MoteDBError::Query("MATCH...AGAINST must be evaluated by executor".into()))
            }
            
            Expr::KnnSearch { .. } => {
                // KNN_SEARCH is handled at executor level (requires index access)
                Err(MoteDBError::Query("KNN_SEARCH must be evaluated by executor".into()))
            }
            
            Expr::KnnDistance { .. } => {
                // KNN_DISTANCE is handled at executor level (requires row vector data)
                Err(MoteDBError::Query("KNN_DISTANCE must be evaluated by executor".into()))
            }
            
            Expr::StWithin { .. } => {
                // ST_WITHIN is handled at executor level (requires spatial index access)
                Err(MoteDBError::Query("ST_WITHIN must be evaluated by executor".into()))
            }
            
            Expr::StDistance { .. } => {
                // ST_DISTANCE is handled at executor level (requires row geometry data)
                Err(MoteDBError::Query("ST_DISTANCE must be evaluated by executor".into()))
            }
            
            Expr::StKnn { .. } => {
                // ST_KNN is handled at executor level (requires spatial index access)
                Err(MoteDBError::Query("ST_KNN must be evaluated by executor".into()))
            }
            
            Expr::WindowFunction { .. } => {
                // Window functions are handled at executor level (require partition data)
                Err(MoteDBError::Query("Window functions must be evaluated by executor".into()))
            }
        }
    }
    
    fn eval_binary_op(&self, op: &BinaryOperator, left: Value, right: Value) -> Result<Value> {
        match op {
            BinaryOperator::Eq => Ok(Value::Bool(left == right)),
            BinaryOperator::Ne => Ok(Value::Bool(left != right)),
            BinaryOperator::Lt => {
                // 🐛 DEBUG: Print comparison for debugging
                let result = left < right;
                // eprintln!("DEBUG Lt: {:?} < {:?} = {}", left, right, result);
                Ok(Value::Bool(result))
            }
            BinaryOperator::Gt => {
                // 🐛 DEBUG: Print comparison for debugging  
                let result = left > right;
                // eprintln!("DEBUG Gt: {:?} > {:?} = {}", left, right, result);
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
                    Value::Integer(i) => Ok(Value::Integer(-i)),
                    Value::Float(f) => Ok(Value::Float(-f)),
                    _ => Err(MoteDBError::TypeError("Cannot negate non-numeric value".to_string())),
                }
            }
            UnaryOperator::Plus => Ok(val),
        }
    }
    
    fn eval_function(&self, name: &str, args: &[Expr], _distinct: bool, row: &SqlRow) -> Result<Value> {
        // Note: distinct parameter is only used for aggregate functions like COUNT(DISTINCT)
        // It's ignored for non-aggregate functions
        
        match name.to_lowercase().as_str() {
            // Aggregate functions (will be handled by executor for now)
            "count" | "sum" | "avg" | "min" | "max" => {
                Err(MoteDBError::NotImplemented(format!("Aggregate function {} not yet implemented", name)))
            }
            
            // String functions
            "lower" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("lower() takes 1 argument".to_string()));
                }
                let val = self.eval(&args[0], row)?;
                if let Value::Text(s) = val {
                    Ok(Value::Text(s.to_lowercase()))
                } else {
                    Err(MoteDBError::TypeError("lower() requires text argument".to_string()))
                }
            }
            
            "upper" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("upper() takes 1 argument".to_string()));
                }
                let val = self.eval(&args[0], row)?;
                if let Value::Text(s) = val {
                    Ok(Value::Text(s.to_uppercase()))
                } else {
                    Err(MoteDBError::TypeError("upper() requires text argument".to_string()))
                }
            }
            
            "length" | "len" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("length() takes 1 argument".to_string()));
                }
                let val = self.eval(&args[0], row)?;
                if let Value::Text(s) = val {
                    Ok(Value::Integer(s.len() as i64))
                } else {
                    Err(MoteDBError::TypeError("length() requires text argument".to_string()))
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
                        Value::Integer(i) => { use std::fmt::Write; let _ = write!(result, "{}", i); }
                        Value::Float(f) => { use std::fmt::Write; let _ = write!(result, "{}", f); }
                        Value::Bool(b) => result.push_str(if b { "true" } else { "false" }),
                        Value::Null => result.push_str("NULL"),
                        _ => { use std::fmt::Write; let _ = write!(result, "{:?}", val); }
                    };
                }
                Ok(Value::Text(result))
            }
            
            "substr" | "substring" => {
                // SUBSTR(text, start [, length])
                if args.len() < 2 || args.len() > 3 {
                    return Err(MoteDBError::InvalidArgument(
                        "substr() takes 2 or 3 arguments (text, start, [length])".to_string()
                    ));
                }
                let text = match self.eval(&args[0], row)? {
                    Value::Text(s) => s,
                    _ => return Err(MoteDBError::TypeError("substr() first argument must be text".to_string())),
                };
                let start = match self.eval(&args[1], row)? {
                    Value::Integer(i) => i.max(0) as usize, // 1-indexed in SQL
                    _ => return Err(MoteDBError::TypeError("substr() start must be integer".to_string())),
                };
                
                // SQL uses 1-based indexing
                let start_idx = if start > 0 { start - 1 } else { 0 };
                
                let result = if args.len() == 3 {
                    let length = match self.eval(&args[2], row)? {
                        Value::Integer(i) => i.max(0) as usize,
                        _ => return Err(MoteDBError::TypeError("substr() length must be integer".to_string())),
                    };
                    text.chars().skip(start_idx).take(length).collect()
                } else {
                    text.chars().skip(start_idx).collect()
                };
                Ok(Value::Text(result))
            }
            
            "trim" => {
                // TRIM(text) - remove leading and trailing whitespace
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("trim() takes 1 argument".to_string()));
                }
                let text = match self.eval(&args[0], row)? {
                    Value::Text(s) => s,
                    _ => return Err(MoteDBError::TypeError("trim() requires text argument".to_string())),
                };
                // ✅ 优化：如果没有前后空格，直接返回原字符串
                if text.trim() == text.as_str() {
                    return Ok(Value::Text(text));
                }
                // 否则才创建新字符串
                Ok(Value::Text(text.trim().to_string()))
            }
            
            "ltrim" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("ltrim() takes 1 argument".to_string()));
                }
                let text = match self.eval(&args[0], row)? {
                    Value::Text(s) => s,
                    _ => return Err(MoteDBError::TypeError("ltrim() requires text argument".to_string())),
                };
                // ✅ 优化：如果没有前导空格，直接返回
                if text.trim_start() == text.as_str() {
                    return Ok(Value::Text(text));
                }
                Ok(Value::Text(text.trim_start().to_string()))
            }
            
            "rtrim" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("rtrim() takes 1 argument".to_string()));
                }
                let text = match self.eval(&args[0], row)? {
                    Value::Text(s) => s,
                    _ => return Err(MoteDBError::TypeError("rtrim() requires text argument".to_string())),
                };
                // ✅ 优化：如果没有尾随空格，直接返回
                if text.trim_end() == text.as_str() {
                    return Ok(Value::Text(text));
                }
                Ok(Value::Text(text.trim_end().to_string()))
            }
            
            "replace" => {
                // REPLACE(text, from, to) - replace all occurrences
                if args.len() != 3 {
                    return Err(MoteDBError::InvalidArgument(
                        "replace() takes 3 arguments (text, from, to)".to_string()
                    ));
                }
                let text = match self.eval(&args[0], row)? {
                    Value::Text(s) => s,
                    _ => return Err(MoteDBError::TypeError("replace() first argument must be text".to_string())),
                };
                let from = match self.eval(&args[1], row)? {
                    Value::Text(s) => s,
                    _ => return Err(MoteDBError::TypeError("replace() second argument must be text".to_string())),
                };
                let to = match self.eval(&args[2], row)? {
                    Value::Text(s) => s,
                    _ => return Err(MoteDBError::TypeError("replace() third argument must be text".to_string())),
                };
                Ok(Value::Text(text.replace(&from, &to)))
            }
            
            "reverse" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("reverse() takes 1 argument".to_string()));
                }
                let text = match self.eval(&args[0], row)? {
                    Value::Text(s) => s,
                    _ => return Err(MoteDBError::TypeError("reverse() requires text argument".to_string())),
                };
                Ok(Value::Text(text.chars().rev().collect()))
            }
            
            "leftstr" | "str_left" => {
                // LEFTSTR(text, length) - get leftmost N characters
                // Renamed to avoid SQL keyword conflict with LEFT JOIN
                if args.len() != 2 {
                    return Err(MoteDBError::InvalidArgument("leftstr() takes 2 arguments".to_string()));
                }
                let text = match self.eval(&args[0], row)? {
                    Value::Text(s) => s,
                    _ => return Err(MoteDBError::TypeError("leftstr() first argument must be text".to_string())),
                };
                let length = match self.eval(&args[1], row)? {
                    Value::Integer(i) => i.max(0) as usize,
                    _ => return Err(MoteDBError::TypeError("leftstr() second argument must be integer".to_string())),
                };
                Ok(Value::Text(text.chars().take(length).collect()))
            }
            
            "rightstr" | "str_right" => {
                // RIGHTSTR(text, length) - get rightmost N characters
                // Renamed to avoid SQL keyword conflict with RIGHT JOIN
                if args.len() != 2 {
                    return Err(MoteDBError::InvalidArgument("rightstr() takes 2 arguments".to_string()));
                }
                let text = match self.eval(&args[0], row)? {
                    Value::Text(s) => s,
                    _ => return Err(MoteDBError::TypeError("rightstr() first argument must be text".to_string())),
                };
                let length = match self.eval(&args[1], row)? {
                    Value::Integer(i) => i.max(0) as usize,
                    _ => return Err(MoteDBError::TypeError("rightstr() second argument must be integer".to_string())),
                };
                let char_vec: Vec<char> = text.chars().collect();
                let start_idx = char_vec.len().saturating_sub(length);
                Ok(Value::Text(char_vec[start_idx..].iter().collect()))
            }
            
            "repeat" => {
                // REPEAT(text, n) - repeat text N times
                if args.len() != 2 {
                    return Err(MoteDBError::InvalidArgument("repeat() takes 2 arguments".to_string()));
                }
                let text = match self.eval(&args[0], row)? {
                    Value::Text(s) => s,
                    _ => return Err(MoteDBError::TypeError("repeat() first argument must be text".to_string())),
                };
                let count = match self.eval(&args[1], row)? {
                    Value::Integer(i) => i.max(0) as usize,
                    _ => return Err(MoteDBError::TypeError("repeat() second argument must be integer".to_string())),
                };
                Ok(Value::Text(text.repeat(count)))
            }
            
            // Math functions
            "abs" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("abs() takes 1 argument".to_string()));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Integer(i) => Ok(Value::Integer(i.abs())),
                    Value::Float(f) => Ok(Value::Float(f.abs())),
                    _ => Err(MoteDBError::TypeError("abs() requires numeric argument".to_string())),
                }
            }
            
            "round" => {
                // ROUND(number [, decimals])
                if args.is_empty() || args.len() > 2 {
                    return Err(MoteDBError::InvalidArgument("round() takes 1 or 2 arguments".to_string()));
                }
                let val = self.eval(&args[0], row)?;
                let decimals = if args.len() == 2 {
                    match self.eval(&args[1], row)? {
                        Value::Integer(i) => i as i32,
                        _ => return Err(MoteDBError::TypeError("round() decimals must be integer".to_string())),
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
                    _ => Err(MoteDBError::TypeError("round() requires numeric argument".to_string())),
                }
            }
            
            "floor" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("floor() takes 1 argument".to_string()));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Float(f) => Ok(Value::Integer(f.floor() as i64)),
                    Value::Integer(i) => Ok(Value::Integer(i)),
                    _ => Err(MoteDBError::TypeError("floor() requires numeric argument".to_string())),
                }
            }
            
            "ceil" | "ceiling" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("ceil() takes 1 argument".to_string()));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Float(f) => Ok(Value::Integer(f.ceil() as i64)),
                    Value::Integer(i) => Ok(Value::Integer(i)),
                    _ => Err(MoteDBError::TypeError("ceil() requires numeric argument".to_string())),
                }
            }
            
            "power" | "pow" => {
                // POWER(base, exponent)
                if args.len() != 2 {
                    return Err(MoteDBError::InvalidArgument("power() takes 2 arguments".to_string()));
                }
                let base = self.to_float(&self.eval(&args[0], row)?)?;
                let exp = self.to_float(&self.eval(&args[1], row)?)?;
                Ok(Value::Float(base.powf(exp)))
            }
            
            "sqrt" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("sqrt() takes 1 argument".to_string()));
                }
                let val = self.to_float(&self.eval(&args[0], row)?)?;
                if val < 0.0 {
                    return Err(MoteDBError::InvalidArgument("sqrt() of negative number".to_string()));
                }
                Ok(Value::Float(val.sqrt()))
            }
            
            "exp" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("exp() takes 1 argument".to_string()));
                }
                let val = self.to_float(&self.eval(&args[0], row)?)?;
                Ok(Value::Float(val.exp()))
            }
            
            "ln" | "log" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("ln() takes 1 argument".to_string()));
                }
                let val = self.to_float(&self.eval(&args[0], row)?)?;
                if val <= 0.0 {
                    return Err(MoteDBError::InvalidArgument("ln() of non-positive number".to_string()));
                }
                Ok(Value::Float(val.ln()))
            }
            
            "log10" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("log10() takes 1 argument".to_string()));
                }
                let val = self.to_float(&self.eval(&args[0], row)?)?;
                if val <= 0.0 {
                    return Err(MoteDBError::InvalidArgument("log10() of non-positive number".to_string()));
                }
                Ok(Value::Float(val.log10()))
            }
            
            "mod" => {
                // MOD(dividend, divisor)
                if args.len() != 2 {
                    return Err(MoteDBError::InvalidArgument("mod() takes 2 arguments".to_string()));
                }
                let dividend = self.eval(&args[0], row)?;
                let divisor = self.eval(&args[1], row)?;
                match (&dividend, &divisor) {
                    (Value::Integer(a), Value::Integer(b)) => {
                        if *b == 0 {
                            return Err(MoteDBError::InvalidArgument("Division by zero in mod()".to_string()));
                        }
                        Ok(Value::Integer(a % b))
                    }
                    _ => {
                        let a = self.to_float(&dividend)?;
                        let b = self.to_float(&divisor)?;
                        if b == 0.0 {
                            return Err(MoteDBError::InvalidArgument("Division by zero in mod()".to_string()));
                        }
                        Ok(Value::Float(a % b))
                    }
                }
            }
            
            "sign" => {
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("sign() takes 1 argument".to_string()));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Integer(i) => Ok(Value::Integer(i.signum())),
                    Value::Float(f) => Ok(Value::Integer(if f > 0.0 { 1 } else if f < 0.0 { -1 } else { 0 })),
                    _ => Err(MoteDBError::TypeError("sign() requires numeric argument".to_string())),
                }
            }
            
            "random" | "rand" => {
                if !args.is_empty() {
                    return Err(MoteDBError::InvalidArgument("random() takes no arguments".to_string()));
                }
                use std::collections::hash_map::RandomState;
                use std::hash::{BuildHasher, Hasher};
                let s = RandomState::new();
                let mut hasher = s.build_hasher();
                hasher.write_u64(std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_else(|_| std::time::Duration::from_secs(0))
                    .as_nanos() as u64);
                let hash = hasher.finish();
                // Convert to [0, 1) range
                Ok(Value::Float((hash as f64) / (u64::MAX as f64)))
            }
            
            // 🆕 Conditional functions
            "if" => {
                // IF(condition, true_value, false_value)
                if args.len() != 3 {
                    return Err(MoteDBError::InvalidArgument(
                        "if() takes 3 arguments (condition, true_value, false_value)".to_string()
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
                        "coalesce() requires at least 1 argument".to_string()
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
                        "ifnull() takes 2 arguments (value, default)".to_string()
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
                        "nullif() takes 2 arguments".to_string()
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
                    return Err(MoteDBError::InvalidArgument("ST_Distance() takes 2 arguments".to_string()));
                }
                let p1 = self.eval(&args[0], row)?;
                let p2 = self.eval(&args[1], row)?;
                self.st_distance(p1, p2)
            }
            
            "within_radius" => {
                if args.len() != 3 {
                    return Err(MoteDBError::InvalidArgument("WITHIN_RADIUS() takes 3 arguments (point, center, radius)".to_string()));
                }
                let point = self.eval(&args[0], row)?;
                let center = self.eval(&args[1], row)?;
                let radius = self.eval(&args[2], row)?;
                self.within_radius(point, center, radius)
            }
            
            "st_ontopof" => {
                if args.len() != 2 {
                    return Err(MoteDBError::InvalidArgument("ST_OnTopOf() takes 2 arguments".to_string()));
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
                        "MATCH() takes 2 arguments (column, query_text)".to_string()
                    ));
                }
                
                // Get column name (must be a column reference, not expression)
                let column_name = match &args[0] {
                    Expr::Column(name) => name.clone(),
                    _ => return Err(MoteDBError::InvalidArgument(
                        "MATCH() first argument must be a column name".to_string()
                    )),
                };
                
                // Get query text
                let query_text = match self.eval(&args[1], row)? {
                    Value::Text(s) => s,
                    _ => return Err(MoteDBError::TypeError(
                        "MATCH() query must be text".to_string()
                    )),
                };
                
                // Get column value
                let column_value = row.get(&column_name)
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
            
            "fts_match" | "fts_search" => {
                Err(MoteDBError::NotImplemented("Full-text search functions not yet implemented".to_string()))
            }
            
            // Timestamp functions
            "now" => {
                // NOW() - current timestamp
                if !args.is_empty() {
                    return Err(MoteDBError::InvalidArgument("NOW() takes no arguments".to_string()));
                }
                use crate::types::Timestamp;
                let ts = Timestamp::now();
                Ok(Value::Timestamp(ts))
            }
            
            "timestamp_micros" => {
                // TIMESTAMP_MICROS(value) - convert integer to timestamp
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("TIMESTAMP_MICROS() takes 1 argument".to_string()));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Integer(micros) => {
                        use crate::types::Timestamp;
                        Ok(Value::Timestamp(Timestamp::from_micros(micros)))
                    }
                    _ => Err(MoteDBError::TypeError("TIMESTAMP_MICROS() requires integer argument".to_string())),
                }
            }
            
            "timestamp_millis" => {
                // TIMESTAMP_MILLIS(value) - convert integer milliseconds to timestamp
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("TIMESTAMP_MILLIS() takes 1 argument".to_string()));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Integer(millis) => {
                        use crate::types::Timestamp;
                        Ok(Value::Timestamp(Timestamp::from_millis(millis)))
                    }
                    _ => Err(MoteDBError::TypeError("TIMESTAMP_MILLIS() requires integer argument".to_string())),
                }
            }
            
            "timestamp_secs" => {
                // TIMESTAMP_SECS(value) - convert integer seconds to timestamp
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("TIMESTAMP_SECS() takes 1 argument".to_string()));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Integer(secs) => {
                        use crate::types::Timestamp;
                        Ok(Value::Timestamp(Timestamp::from_secs(secs)))
                    }
                    _ => Err(MoteDBError::TypeError("TIMESTAMP_SECS() requires integer argument".to_string())),
                }
            }
            
            "to_micros" => {
                // TO_MICROS(timestamp) - extract microseconds from timestamp
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("TO_MICROS() takes 1 argument".to_string()));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Timestamp(ts) => Ok(Value::Integer(ts.as_micros())),
                    _ => Err(MoteDBError::TypeError("TO_MICROS() requires timestamp argument".to_string())),
                }
            }
            
            // 🆕 P1 Date/Time extraction functions
            "year" => {
                // YEAR(timestamp) - extract year
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("YEAR() takes 1 argument".to_string()));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Timestamp(ts) => {
                        // Convert microseconds to seconds for chrono-like calculation
                        let secs = ts.as_micros() / 1_000_000;
                        // Days since epoch: divide by seconds per day (86400)
                        let days = secs / 86400;
                        // Approximate year: 1970 + days/365.25
                        let year = 1970 + (days as f64 / 365.25) as i64;
                        Ok(Value::Integer(year))
                    }
                    _ => Err(MoteDBError::TypeError("YEAR() requires timestamp argument".to_string())),
                }
            }
            
            "month" => {
                // MONTH(timestamp) - extract month (1-12)
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("MONTH() takes 1 argument".to_string()));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Timestamp(ts) => {
                        let secs = ts.as_micros() / 1_000_000;
                        let days = secs / 86400;
                        // Simplified month calculation (approximate)
                        let days_in_year = days % 365;
                        let month = ((days_in_year / 30) + 1).min(12);
                        Ok(Value::Integer(month))
                    }
                    _ => Err(MoteDBError::TypeError("MONTH() requires timestamp argument".to_string())),
                }
            }
            
            "day" | "day_of_month" => {
                // DAY(timestamp) - extract day of month (1-31)
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("DAY() takes 1 argument".to_string()));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Timestamp(ts) => {
                        let secs = ts.as_micros() / 1_000_000;
                        let days = secs / 86400;
                        // Day within month (approximation)
                        let day = (days % 30) + 1;
                        Ok(Value::Integer(day))
                    }
                    _ => Err(MoteDBError::TypeError("DAY() requires timestamp argument".to_string())),
                }
            }
            
            "hour" => {
                // HOUR(timestamp) - extract hour (0-23)
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("HOUR() takes 1 argument".to_string()));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Timestamp(ts) => {
                        let secs = ts.as_micros() / 1_000_000;
                        let hour = (secs % 86400) / 3600;
                        Ok(Value::Integer(hour))
                    }
                    _ => Err(MoteDBError::TypeError("HOUR() requires timestamp argument".to_string())),
                }
            }
            
            "minute" => {
                // MINUTE(timestamp) - extract minute (0-59)
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("MINUTE() takes 1 argument".to_string()));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Timestamp(ts) => {
                        let secs = ts.as_micros() / 1_000_000;
                        let minute = (secs % 3600) / 60;
                        Ok(Value::Integer(minute))
                    }
                    _ => Err(MoteDBError::TypeError("MINUTE() requires timestamp argument".to_string())),
                }
            }
            
            "second" => {
                // SECOND(timestamp) - extract second (0-59)
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("SECOND() takes 1 argument".to_string()));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Timestamp(ts) => {
                        let secs = ts.as_micros() / 1_000_000;
                        let second = secs % 60;
                        Ok(Value::Integer(second))
                    }
                    _ => Err(MoteDBError::TypeError("SECOND() requires timestamp argument".to_string())),
                }
            }
            
            "date_add" | "dateadd" => {
                // DATE_ADD(timestamp, interval_seconds) - add seconds to timestamp
                if args.len() != 2 {
                    return Err(MoteDBError::InvalidArgument(
                        "DATE_ADD() takes 2 arguments (timestamp, seconds)".to_string()
                    ));
                }
                let ts = match self.eval(&args[0], row)? {
                    Value::Timestamp(ts) => ts,
                    _ => return Err(MoteDBError::TypeError("DATE_ADD() first argument must be timestamp".to_string())),
                };
                let seconds = match self.eval(&args[1], row)? {
                    Value::Integer(i) => i,
                    _ => return Err(MoteDBError::TypeError("DATE_ADD() second argument must be integer".to_string())),
                };
                
                use crate::types::Timestamp;
                let new_micros = ts.as_micros() + (seconds * 1_000_000);
                Ok(Value::Timestamp(Timestamp::from_micros(new_micros)))
            }
            
            "date_diff" | "datediff" => {
                // DATE_DIFF(timestamp1, timestamp2) - difference in seconds
                if args.len() != 2 {
                    return Err(MoteDBError::InvalidArgument(
                        "DATE_DIFF() takes 2 arguments (timestamp1, timestamp2)".to_string()
                    ));
                }
                let ts1 = match self.eval(&args[0], row)? {
                    Value::Timestamp(ts) => ts,
                    _ => return Err(MoteDBError::TypeError("DATE_DIFF() first argument must be timestamp".to_string())),
                };
                let ts2 = match self.eval(&args[1], row)? {
                    Value::Timestamp(ts) => ts,
                    _ => return Err(MoteDBError::TypeError("DATE_DIFF() second argument must be timestamp".to_string())),
                };
                
                let diff_micros = ts1.as_micros() - ts2.as_micros();
                let diff_seconds = diff_micros / 1_000_000;
                Ok(Value::Integer(diff_seconds))
            }
            
            "day_of_week" | "dow" => {
                // DAY_OF_WEEK(timestamp) - extract day of week (1=Monday, 7=Sunday)
                if args.len() != 1 {
                    return Err(MoteDBError::InvalidArgument("DAY_OF_WEEK() takes 1 argument".to_string()));
                }
                let val = self.eval(&args[0], row)?;
                match val {
                    Value::Timestamp(ts) => {
                        let secs = ts.as_micros() / 1_000_000;
                        let days = secs / 86400;
                        // Unix epoch (1970-01-01) was Thursday (day 4)
                        // Calculate day of week: (days + 4) % 7, then map to 1-7
                        let dow = ((days + 3) % 7) + 1; // +3 because epoch was Thursday (4-1=3)
                        Ok(Value::Integer(dow))
                    }
                    _ => Err(MoteDBError::TypeError("DAY_OF_WEEK() requires timestamp argument".to_string())),
                }
            }
            
            // 🆕 Type conversion function
            "cast" => {
                // CAST(value AS type) - NOTE: In SQL this is special syntax, but we handle as function
                // Usage: CAST(column, 'INTEGER') or CAST(column, 'TEXT')
                if args.len() != 2 {
                    return Err(MoteDBError::InvalidArgument(
                        "CAST() takes 2 arguments (value, target_type)".to_string()
                    ));
                }
                let val = self.eval(&args[0], row)?;
                let target_type = match self.eval(&args[1], row)? {
                    Value::Text(s) => s.to_uppercase(),
                    _ => return Err(MoteDBError::TypeError("CAST() target type must be text".to_string())),
                };
                
                match target_type.as_str() {
                    "INTEGER" | "INT" => {
                        match val {
                            Value::Integer(i) => Ok(Value::Integer(i)),
                            Value::Float(f) => Ok(Value::Integer(f as i64)),
                            Value::Text(s) => s.parse::<i64>()
                                .map(Value::Integer)
                                .map_err(|_| MoteDBError::TypeError("Cannot parse integer".to_string())),
                            Value::Bool(b) => Ok(Value::Integer(if b { 1 } else { 0 })),
                            Value::Timestamp(ts) => Ok(Value::Integer(ts.as_micros())),
                            _ => Err(MoteDBError::TypeError(format!("Cannot cast {:?} to INTEGER", val))),
                        }
                    }
                    "FLOAT" | "REAL" | "DOUBLE" => {
                        match val {
                            Value::Float(f) => Ok(Value::Float(f)),
                            Value::Integer(i) => Ok(Value::Float(i as f64)),
                            Value::Text(s) => s.parse::<f64>()
                                .map(Value::Float)
                                .map_err(|_| MoteDBError::TypeError("Cannot parse float".to_string())),
                            _ => Err(MoteDBError::TypeError(format!("Cannot cast {:?} to FLOAT", val))),
                        }
                    }
                    "TEXT" | "VARCHAR" | "STRING" => {
                        let text = match val {
                            Value::Text(s) => s,
                            Value::Integer(i) => i.to_string(),
                            Value::Float(f) => f.to_string(),
                            Value::Bool(b) => b.to_string(),
                            Value::Null => "NULL".to_string(),
                            _ => format!("{:?}", val),
                        };
                        Ok(Value::Text(text))
                    }
                    "BOOLEAN" | "BOOL" => {
                        let b = self.to_bool(&val)?;
                        Ok(Value::Bool(b))
                    }
                    "TIMESTAMP" => {
                        match val {
                            Value::Timestamp(ts) => Ok(Value::Timestamp(ts)),
                            Value::Integer(micros) => {
                                use crate::types::Timestamp;
                                Ok(Value::Timestamp(Timestamp::from_micros(micros)))
                            }
                            _ => Err(MoteDBError::TypeError(format!("Cannot cast {:?} to TIMESTAMP", val))),
                        }
                    }
                    _ => Err(MoteDBError::TypeError(format!("Unknown target type: {}", target_type))),
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
            Value::Null => Ok(false),
            _ => Err(MoteDBError::TypeError("Cannot convert to boolean".to_string())),
        }
    }
    
    fn to_float(&self, val: &Value) -> Result<f64> {
        match val {
            Value::Float(f) => Ok(*f),
            Value::Integer(i) => Ok(*i as f64),
            _ => Err(MoteDBError::TypeError("Cannot convert to float".to_string())),
        }
    }
    
    fn add_values(&self, left: Value, right: Value) -> Result<Value> {
        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => Ok(Value::Integer(l + r)),
            (Value::Float(l), Value::Float(r)) => Ok(Value::Float(l + r)),
            (Value::Integer(l), Value::Float(r)) => Ok(Value::Float(l as f64 + r)),
            (Value::Float(l), Value::Integer(r)) => Ok(Value::Float(l + r as f64)),
            (Value::Text(l), Value::Text(r)) => Ok(Value::Text(format!("{}{}", l, r))),
            _ => Err(MoteDBError::TypeError("Cannot add these types".to_string())),
        }
    }
    
    fn sub_values(&self, left: Value, right: Value) -> Result<Value> {
        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => Ok(Value::Integer(l - r)),
            (Value::Float(l), Value::Float(r)) => Ok(Value::Float(l - r)),
            (Value::Integer(l), Value::Float(r)) => Ok(Value::Float(l as f64 - r)),
            (Value::Float(l), Value::Integer(r)) => Ok(Value::Float(l - r as f64)),
            _ => Err(MoteDBError::TypeError("Cannot subtract these types".to_string())),
        }
    }
    
    fn mul_values(&self, left: Value, right: Value) -> Result<Value> {
        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => Ok(Value::Integer(l * r)),
            (Value::Float(l), Value::Float(r)) => Ok(Value::Float(l * r)),
            (Value::Integer(l), Value::Float(r)) => Ok(Value::Float(l as f64 * r)),
            (Value::Float(l), Value::Integer(r)) => Ok(Value::Float(l * r as f64)),
            _ => Err(MoteDBError::TypeError("Cannot multiply these types".to_string())),
        }
    }
    
    fn div_values(&self, left: Value, right: Value) -> Result<Value> {
        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => {
                if r == 0 {
                    return Err(MoteDBError::DivisionByZero);
                }
                Ok(Value::Integer(l / r))
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
            _ => Err(MoteDBError::TypeError("Cannot divide these types".to_string())),
        }
    }
    
    fn mod_values(&self, left: Value, right: Value) -> Result<Value> {
        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => {
                if r == 0 {
                    return Err(MoteDBError::DivisionByZero);
                }
                Ok(Value::Integer(l % r))
            }
            _ => Err(MoteDBError::TypeError("Modulo only works on integers".to_string())),
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
    
    /// Simple LIKE pattern matching (fallback for non-cached cases)
    /// Supports % (any characters) and _ (single character)
    #[allow(dead_code)]
    fn like_match(&self, text: &str, pattern: &str) -> bool {
        let text_chars: Vec<char> = text.chars().collect();
        let pattern_chars: Vec<char> = pattern.chars().collect();
        
        self.like_match_recursive(&text_chars, &pattern_chars, 0, 0)
    }
    
    fn like_match_recursive(&self, text: &[char], pattern: &[char], ti: usize, pi: usize) -> bool {
        // End of pattern
        if pi >= pattern.len() {
            return ti >= text.len();
        }
        
        // End of text
        if ti >= text.len() {
            // Pattern must be all % to match
            return pattern[pi..].iter().all(|&c| c == '%');
        }
        
        match pattern[pi] {
            '%' => {
                // Try matching 0 or more characters
                self.like_match_recursive(text, pattern, ti, pi + 1) ||
                self.like_match_recursive(text, pattern, ti + 1, pi)
            }
            '_' => {
                // Match exactly one character
                self.like_match_recursive(text, pattern, ti + 1, pi + 1)
            }
            c => {
                // Exact match
                if text[ti] == c {
                    self.like_match_recursive(text, pattern, ti + 1, pi + 1)
                } else {
                    false
                }
            }
        }
    }
    
    // E-SQL Vector Distance Functions
    
    /// L2 Distance (Euclidean): <->
    fn l2_distance(&self, left: Value, right: Value) -> Result<Value> {
        let (v1, v2) = self.extract_vectors(left, right)?;
        
        if v1.len() != v2.len() {
            return Err(MoteDBError::TypeError(format!(
                "Vector dimension mismatch: {} vs {}", v1.len(), v2.len()
            )));
        }
        
        let dist: f32 = v1.iter().zip(v2.iter())
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
                "Vector dimension mismatch: {} vs {}", v1.len(), v2.len()
            )));
        }
        
        let dot: f32 = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();
        let norm1: f32 = v1.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm2: f32 = v2.iter().map(|x| x * x).sum::<f32>().sqrt();
        
        if norm1 == 0.0 || norm2 == 0.0 {
            return Ok(Value::Float(1.0)); // Maximum distance for zero vectors
        }
        
        let cosine_sim = dot / (norm1 * norm2);
        let dist = 1.0 - cosine_sim; // Range: [0, 2]
        
        Ok(Value::Float(dist as f64))
    }
    
    /// Dot Product (Inner Product): <#>
    fn dot_product(&self, left: Value, right: Value) -> Result<Value> {
        let (v1, v2) = self.extract_vectors(left, right)?;
        
        if v1.len() != v2.len() {
            return Err(MoteDBError::TypeError(format!(
                "Vector dimension mismatch: {} vs {}", v1.len(), v2.len()
            )));
        }
        
        let dot: f32 = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();
        
        Ok(Value::Float(dot as f64))
    }
    
    /// Extract vectors from Value types
    fn extract_vectors(&self, left: Value, right: Value) -> Result<(Vec<f32>, Vec<f32>)> {
        let v1 = match left {
            Value::Vector(v) => v,
            Value::Tensor(t) => {
                t.as_f32().to_vec()
            }
            _ => return Err(MoteDBError::TypeError(
                format!("Left operand is not a vector: {:?}", left)
            )),
        };
        
        let v2 = match right {
            Value::Vector(v) => v,
            Value::Tensor(t) => {
                t.as_f32().to_vec()
            }
            _ => return Err(MoteDBError::TypeError(
                format!("Right operand is not a vector: {:?}", right)
            )),
        };
        
        Ok((v1, v2))
    }
    
    // E-SQL Spatial Functions
    
    /// ST_Distance: Compute 3D Euclidean distance between two spatial points
    fn st_distance(&self, p1: Value, p2: Value) -> Result<Value> {
        use crate::types::Geometry;
        
        let point1 = match p1 {
            Value::Spatial(Geometry::Point(p)) => p,
            _ => return Err(MoteDBError::TypeError("ST_Distance requires spatial point arguments".to_string())),
        };
        
        let point2 = match p2 {
            Value::Spatial(Geometry::Point(p)) => p,
            _ => return Err(MoteDBError::TypeError("ST_Distance requires spatial point arguments".to_string())),
        };
        
        let dist = ((point1.x - point2.x).powi(2) + (point1.y - point2.y).powi(2)).sqrt();
        Ok(Value::Float(dist))
    }
    
    /// WITHIN_RADIUS: Check if a point is within radius of a center point
    fn within_radius(&self, point: Value, center: Value, radius: Value) -> Result<Value> {
        use crate::types::Geometry;
        
        let p = match point {
            Value::Spatial(Geometry::Point(p)) => p,
            _ => return Err(MoteDBError::TypeError("WITHIN_RADIUS requires spatial point for first argument".to_string())),
        };
        
        let c = match center {
            Value::Spatial(Geometry::Point(c)) => c,
            _ => return Err(MoteDBError::TypeError("WITHIN_RADIUS requires spatial point for center".to_string())),
        };
        
        let r = match radius {
            Value::Float(r) => r,
            Value::Integer(i) => i as f64,
            _ => return Err(MoteDBError::TypeError("WITHIN_RADIUS requires numeric radius".to_string())),
        };
        
        let dist = ((p.x - c.x).powi(2) + (p.y - c.y).powi(2)).sqrt();
        Ok(Value::Bool(dist <= r))
    }
    
    /// ST_OnTopOf: Check if point p1 is on top of point p2 (p1.y > p2.y)
    fn st_ontopof(&self, p1: Value, p2: Value) -> Result<Value> {
        use crate::types::Geometry;
        
        let point1 = match p1 {
            Value::Spatial(Geometry::Point(p)) => p,
            _ => return Err(MoteDBError::TypeError("ST_OnTopOf requires spatial point arguments".to_string())),
        };
        
        let point2 = match p2 {
            Value::Spatial(Geometry::Point(p)) => p,
            _ => return Err(MoteDBError::TypeError("ST_OnTopOf requires spatial point arguments".to_string())),
        };
        
        // In 2D, "on top of" means higher Y coordinate
        // Also check if X coordinates are close (within same vertical region)
        let same_region = (point1.x - point2.x).abs() < 1.0; // Within 1 unit horizontally
        let above = point1.y > point2.y;
        
        Ok(Value::Bool(same_region && above))
    }
}

impl Default for ExprEvaluator {
    fn default() -> Self {
        Self::new()
    }
}
