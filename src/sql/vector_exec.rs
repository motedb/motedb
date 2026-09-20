//! 向量化执行内核（VEC）— M1：批扫描 + 批过滤 + 无组批聚合。
//!
//! 绞杀者模式第一接线：`try_vec_no_group_aggregate` 收编
//! `SELECT AGG(...) FROM t [WHERE 简单谓词]`（ColSegmentStore 表，无 GROUP
//! BY）。旧路径保留为回退（返回 `None` 即回退；`MOTE_VEC=off` 全局关闭）。
//!
//! 语义对齐（与 fuzz 校准过的旧路径一致）：
//! - 三值逻辑：NULL 参与比较 → UNKNOWN → 不入选 selection；AND/OR 按
//!   Kleene（FALSE 主导 AND、TRUE 主导 OR），NOT(UNKNOWN)=UNKNOWN。
//! - COUNT(col)/SUM/AVG/MIN/MAX 跳过 NULL；COUNT(*) 计全部选中行。
//! - SUM/AVG 混合 int/float：整数精确累加 + CompSum 浮点（同 AggAccumulator
//!   — 差分校准过 SQLite 的 Neumaier 语义）。

use std::sync::Arc;

use crate::sql::ast::{BinaryOperator, Expr, SelectColumn, SelectStmt};
use crate::storage::col_segment::ColSegmentStore;
use crate::storage::colbatch::{ColData, ColumnBatch, ColumnVector, SelectionVec};
use crate::types::{CompSum, ColumnType, TableSchema, Value};
use crate::Result;

/// 🔑 M1 默认关闭 (MOTE_VEC=on 显式开启): 事务回滚的 undo 重插走双写
/// (builder SST + 段缓冲), 段与 builder 可发散 — vec 段批读不全
/// (ryw_delete 回滚后 COUNT 少 1, ACID 审计)。M2 统一可见性后转默认开。
/// 基准/分析负载 (bulk load + checkpoint 后) 显式开启拿到 2.3× 聚合加速。
pub fn vec_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("MOTE_VEC").map_or(false, |v| v == "on"))
}

// ───────────────────────── 批谓词 ─────────────────────────

/// 批谓词叶：列位置 + 操作 + 字面量。
#[derive(Debug, Clone)]
enum VecPredLeaf {
    Cmp {
        col: usize,
        op: BinaryOperator,
        lit: Value,
    },
    IsNull {
        col: usize,
        negated: bool,
    },
}

/// 编译后的批谓词树（AND/OR/NOT/叶）。不可编译 → None 回退行式。
#[derive(Debug, Clone)]
enum VecPred {
    Leaf(VecPredLeaf),
    And(Box<VecPred>, Box<VecPred>),
    Or(Box<VecPred>, Box<VecPred>),
    Not(Box<VecPred>),
}

/// 行级三值结果（M2 升级为 validity 位图合成）。
#[derive(Clone, Copy, PartialEq)]
enum TV {
    True,
    False,
    Unknown,
}

/// Bool/Int/Timestamp/Int-Float 两两强转（fuzz 校准：flag=1 与 TRUE 相等、
/// Timestamp 与 Integer 同为 micros）。
fn coerce_pair(a: &Value, b: &Value) -> (Value, Value) {
    match (a, b) {
        (Value::Bool(x), Value::Integer(_)) => (Value::Integer(*x as i64), b.clone()),
        (Value::Integer(_), Value::Bool(y)) => (a.clone(), Value::Integer(*y as i64)),
        (Value::Timestamp(t), Value::Integer(_)) => (Value::Integer(t.as_micros()), b.clone()),
        (Value::Integer(_), Value::Timestamp(t)) => (a.clone(), Value::Integer(t.as_micros())),
        (Value::Integer(i), Value::Float(_)) => (Value::Float(*i as f64), b.clone()),
        (Value::Float(_), Value::Integer(j)) => (a.clone(), Value::Float(*j as f64)),
        _ => (a.clone(), b.clone()),
    }
}

use std::cmp::Ordering as Ord2;

#[inline]
fn ord_tv(ord: Ord2, op: &BinaryOperator) -> TV {
    let b = match op {
        BinaryOperator::Eq => ord == Ord2::Equal,
        BinaryOperator::Ne => ord != Ord2::Equal,
        BinaryOperator::Lt => ord == Ord2::Less,
        BinaryOperator::Gt => ord == Ord2::Greater,
        BinaryOperator::Le => ord != Ord2::Greater,
        BinaryOperator::Ge => ord != Ord2::Less,
        _ => return TV::Unknown,
    };
    if b {
        TV::True
    } else {
        TV::False
    }
}

#[inline]
fn leaf_tv_leaf(cols: &[std::sync::Arc<ColumnVector>], l: &VecPredLeaf, i: usize) -> TV {
    match l {
        VecPredLeaf::Cmp { col, op, lit } => leaf_tv(&cols[*col], op, lit, i),
        VecPredLeaf::IsNull { col, negated } => {
            let is_null = cols[*col].valid.is_null(i);
            let b = if *negated { !is_null } else { is_null };
            if b {
                TV::True
            } else {
                TV::False
            }
        }
    }
}

/// 🔑 类型化叶求值：直接在 ColData 切片上与字面量比较 — 批热循环零
/// `Value` 构造（行级 Value 版本每行一次分配，Text 还 clone Arc，
/// 首版实测比旧融合路径慢 10×，差分形状对但性能不达标的教训）。
#[inline]
fn leaf_tv(cv: &ColumnVector, op: &BinaryOperator, lit: &Value, i: usize) -> TV {
    if cv.valid.is_null(i) {
        return TV::Unknown;
    }
    match &cv.data {
        ColData::I64(v) => {
            let x = v[i];
            match lit {
                Value::Integer(l) => ord_tv(x.cmp(l), op),
                Value::Float(l) => f64_tv(x as f64, *l, op),
                Value::Timestamp(t) => ord_tv(x.cmp(&t.as_micros()), op),
                Value::Bool(b) => ord_tv(x.cmp(&(*b as i64)), op),
                _ => TV::Unknown,
            }
        }
        ColData::F64(v) => {
            let x = v[i];
            match lit {
                Value::Integer(l) => f64_tv(x, *l as f64, op),
                Value::Float(l) => f64_tv(x, *l, op),
                _ => TV::Unknown,
            }
        }
        ColData::Bool(bits) => {
            let x = (bits[i / 64] >> (i % 64)) & 1 != 0;
            match lit {
                Value::Bool(b) => ord_tv(x.cmp(b), op),
                Value::Integer(n) => ord_tv((x as i64).cmp(n), op),
                _ => TV::Unknown,
            }
        }
        ColData::Utf8(v) => match lit {
            Value::Text(l) => { let a: &str = &v[i]; let b: &str = l.0.as_ref(); ord_tv(a.cmp(b), op) }
            _ => TV::Unknown,
        },
        ColData::Values(v) => lit_cmp(&v[i], op, lit),
    }
}

#[inline]
fn f64_tv(x: f64, l: f64, op: &BinaryOperator) -> TV {
    match x.partial_cmp(&l) {
        Some(ord) => ord_tv(ord, op),
        // NaN 与任何值比较 → UNKNOWN（与 eval_expr_on_row 的 partial_cmp
        // None → 不匹配一致）
        None => TV::Unknown,
    }
}

fn lit_cmp(v: &Value, op: &BinaryOperator, lit: &Value) -> TV {
    use std::cmp::Ordering;
    if matches!(v, Value::Null) || matches!(lit, Value::Null) {
        return TV::Unknown;
    }
    let (v, lit) = coerce_pair(v, lit);
    let Some(ord) = v.partial_cmp(&lit) else {
        return TV::Unknown; // 不可比较类型（Text vs Int 等）→ UNKNOWN
    };
    let b = match op {
        BinaryOperator::Eq => ord == Ordering::Equal,
        BinaryOperator::Ne => ord != Ordering::Equal,
        BinaryOperator::Lt => ord == Ordering::Less,
        BinaryOperator::Gt => ord == Ordering::Greater,
        BinaryOperator::Le => ord != Ordering::Greater,
        BinaryOperator::Ge => ord != Ordering::Less,
        _ => return TV::Unknown,
    };
    if b {
        TV::True
    } else {
        TV::False
    }
}

impl VecPred {
    /// Expr → 批谓词。仅收列-op-字面量 / IS [NOT] NULL / AND/OR/NOT /
    /// BETWEEN（折叠为两个闭区间比较；NOT BETWEEN 是 OR 语义 → 拒收）。
    fn compile(e: &Expr, schema: &TableSchema) -> Option<VecPred> {
        match e {
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And => Some(VecPred::And(
                    Box::new(Self::compile(left, schema)?),
                    Box::new(Self::compile(right, schema)?),
                )),
                BinaryOperator::Or => Some(VecPred::Or(
                    Box::new(Self::compile(left, schema)?),
                    Box::new(Self::compile(right, schema)?),
                )),
                _ => {
                    if !matches!(
                        op,
                        BinaryOperator::Eq
                            | BinaryOperator::Ne
                            | BinaryOperator::Lt
                            | BinaryOperator::Gt
                            | BinaryOperator::Le
                            | BinaryOperator::Ge
                    ) {
                        return None;
                    }
                    // 列 op 字面量（含负数字面量 UnaryOp(Minus, Literal)）
                    let (col, lit) = Self::col_lit(left, right, schema)?;
                    Some(VecPred::Leaf(VecPredLeaf::Cmp {
                        col,
                        op: op.clone(),
                        lit,
                    }))
                }
            },
            Expr::UnaryOp {
                op: crate::sql::ast::UnaryOperator::Not,
                expr,
            } => Self::compile(expr, schema).map(|p| VecPred::Not(Box::new(p))),
            Expr::IsNull { expr, negated } => {
                let Expr::Column(c) = expr.as_ref() else {
                    return None;
                };
                let pos = schema.get_column_position(c)?;
                Some(VecPred::Leaf(VecPredLeaf::IsNull {
                    col: pos,
                    negated: *negated,
                }))
            }
            Expr::Between {
                expr,
                negated: false,
                low,
                high,
            } => {
                let Expr::Column(c) = expr.as_ref() else {
                    return None;
                };
                let pos = schema.get_column_position(c)?;
                let lv = literal_of(low)?;
                let hv = literal_of(high)?;
                Some(VecPred::And(
                    Box::new(VecPred::Leaf(VecPredLeaf::Cmp {
                        col: pos,
                        op: BinaryOperator::Ge,
                        lit: lv,
                    })),
                    Box::new(VecPred::Leaf(VecPredLeaf::Cmp {
                        col: pos,
                        op: BinaryOperator::Le,
                        lit: hv,
                    })),
                ))
            }
            _ => None,
        }
    }

    fn col_lit(left: &Expr, right: &Expr, schema: &TableSchema) -> Option<(usize, Value)> {
        if let (Expr::Column(c), r) = (left, right) {
            let lit = literal_of(r)?;
            // 允许限定名（表内单表查询时剥前缀）
            let bare = c.rsplit('.').next().unwrap_or(c);
            let pos = schema
                .get_column_position(c)
                .or_else(|| schema.get_column_position(bare))?;
            return Some((pos, lit));
        }
        None
    }

    /// 纯 AND 链 → 扁平叶列表（None = 含 OR/NOT 的混合形状）。
    fn as_and_chain<'a>(&'a self, out: &mut Vec<&'a VecPredLeaf>) -> bool {
        match self {
            VecPred::Leaf(l) => {
                out.push(l);
                true
            }
            VecPred::And(l, r) => l.as_and_chain(out) && r.as_and_chain(out),
            _ => false,
        }
    }

    /// 批上求值 → selection（三值：仅 True 入选）。
    /// 🔑 纯 AND 链走扁平单循环 + 行级短路 — 消掉逐行递归枚举匹配
    /// （递归版比旧融合路径慢 2×, AND 链是最常见的谓词形状）。
    fn eval_sel(&self, batch: &ColumnBatch) -> SelectionVec {
        let n = batch.cols.first().map_or(0, |c| c.len());
        let mut chain: Vec<&VecPredLeaf> = Vec::new();
        if self.as_and_chain(&mut chain) {
            let mut sel = SelectionVec::with_capacity(n);
            'rows: for i in 0..n {
                for l in &chain {
                    if leaf_tv_leaf(&batch.cols, l, i) != TV::True {
                        continue 'rows;
                    }
                }
                sel.push(i as u32);
            }
            return sel;
        }
        let mut sel = SelectionVec::with_capacity(n);
        for i in 0..n {
            if self.eval_row(batch, i) == TV::True {
                sel.push(i as u32);
            }
        }
        sel
    }

    fn eval_row(&self, batch: &ColumnBatch, i: usize) -> TV {
        match self {
            VecPred::Leaf(VecPredLeaf::Cmp { col, op, lit }) => {
                leaf_tv(&batch.cols[*col], op, lit, i)
            }
            VecPred::Leaf(VecPredLeaf::IsNull { col, negated }) => {
                let is_null = batch.cols[*col].valid.is_null(i);
                let b = if *negated { !is_null } else { is_null };
                if b {
                    TV::True
                } else {
                    TV::False
                }
            }
            VecPred::And(l, r) => match (l.eval_row(batch, i), r.eval_row(batch, i)) {
                (TV::False, _) | (_, TV::False) => TV::False,
                (TV::True, TV::True) => TV::True,
                _ => TV::Unknown,
            },
            VecPred::Or(l, r) => match (l.eval_row(batch, i), r.eval_row(batch, i)) {
                (TV::True, _) | (_, TV::True) => TV::True,
                (TV::False, TV::False) => TV::False,
                _ => TV::Unknown,
            },
            VecPred::Not(p) => match p.eval_row(batch, i) {
                TV::True => TV::False,
                TV::False => TV::True,
                TV::Unknown => TV::Unknown,
            },
        }
    }
}

fn literal_of(e: &Expr) -> Option<Value> {
    match e {
        Expr::Literal(v) => Some(v.clone()),
        Expr::UnaryOp {
            op: crate::sql::ast::UnaryOperator::Minus,
            expr,
        } => match literal_of(expr)? {
            Value::Integer(i) => i.checked_neg().map(Value::Integer),
            Value::Float(f) => Some(Value::Float(-f)),
            _ => None,
        },
        _ => None,
    }
}

/// 把谓词叶的 schema 列位改写为批内相对位（批只装载 needed 列）。
fn remap_pred(p: &mut VecPred, needed: &[usize]) {
    let map = |col: &mut usize| {
        if let Some(i) = needed.iter().position(|&x| x == *col) {
            *col = i;
        }
    };
    match p {
        VecPred::Leaf(VecPredLeaf::Cmp { col, .. })
        | VecPred::Leaf(VecPredLeaf::IsNull { col, .. }) => map(col),
        VecPred::And(l, r) | VecPred::Or(l, r) => {
            remap_pred(l, needed);
            remap_pred(r, needed);
        }
        VecPred::Not(x) => remap_pred(x, needed),
    }
}

fn collect_pred_cols(p: &VecPred, out: &mut Vec<usize>) {
    match p {
        VecPred::Leaf(VecPredLeaf::Cmp { col, .. })
        | VecPred::Leaf(VecPredLeaf::IsNull { col, .. }) => out.push(*col),
        VecPred::And(l, r) | VecPred::Or(l, r) => {
            collect_pred_cols(l, out);
            collect_pred_cols(r, out);
        }
        VecPred::Not(x) => collect_pred_cols(x, out),
    }
}

// ───────────────────────── 批聚合 ─────────────────────────

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum VecAggFunc {
    CountStar,
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

pub struct VecAggSpec {
    pub func: VecAggFunc,
    pub col: Option<usize>, // None = COUNT(*)
}

#[derive(Default)]
struct VecAcc {
    count: u64, // 选中行数（COUNT(*)）
    nn: u64,    // 非 NULL 计数（COUNT(col)/SUM/AVG 分母）
    int_sum: i64,
    fsum: CompSum,
    has_f: bool,
    min: Option<Value>,
    max: Option<Value>,
}

impl VecAcc {
    /// 批内折叠：定长列直接在类型化切片上按 selection 走（无 Value 物化），
    /// Bool/Utf8/Values 走批边界 get。
    fn fold_batch(&mut self, cv: &ColumnVector, sel: &SelectionVec, func: VecAggFunc) {
        match &cv.data {
            ColData::I64(v) => {
                for s in sel.iter() {
                    let s = s as usize;
                    if cv.valid.is_null(s) {
                        continue;
                    }
                    self.nn += 1;
                    let x = v[s];
                    match func {
                        VecAggFunc::Count | VecAggFunc::CountStar => {}
                        VecAggFunc::Sum | VecAggFunc::Avg => {
                            self.int_sum = self.int_sum.wrapping_add(x)
                        }
                        VecAggFunc::Min => {
                            if self.min.as_ref().is_none_or(|m| match m {
                                Value::Integer(mi) => x < *mi,
                                _ => true,
                            }) {
                                self.min = Some(Value::Integer(x));
                            }
                        }
                        VecAggFunc::Max => {
                            if self.max.as_ref().is_none_or(|m| match m {
                                Value::Integer(mi) => x > *mi,
                                _ => true,
                            }) {
                                self.max = Some(Value::Integer(x));
                            }
                        }
                    }
                }
            }
            ColData::F64(v) => {
                for s in sel.iter() {
                    let s = s as usize;
                    if cv.valid.is_null(s) {
                        continue;
                    }
                    self.nn += 1;
                    let x = v[s];
                    match func {
                        VecAggFunc::Count | VecAggFunc::CountStar => {}
                        VecAggFunc::Sum | VecAggFunc::Avg => {
                            self.fsum.add(x);
                            self.has_f = true;
                        }
                        VecAggFunc::Min => {
                            if self.min.as_ref().is_none_or(|m| match m {
                                Value::Float(mf) => x < *mf,
                                Value::Integer(mi) => x < *mi as f64,
                                _ => true,
                            }) {
                                self.min = Some(Value::Float(x));
                            }
                        }
                        VecAggFunc::Max => {
                            if self.max.as_ref().is_none_or(|m| match m {
                                Value::Float(mf) => x > *mf,
                                Value::Integer(mi) => x > *mi as f64,
                                _ => true,
                            }) {
                                self.max = Some(Value::Float(x));
                            }
                        }
                    }
                }
            }
            _ => {
                // Bool 位图 / Utf8 / Values：批边界 get（含 Timestamp 列在
                // I64 之外不会到这；Text MIN/MAX 走 Value 比较）
                for s in sel.iter() {
                    let s = s as usize;
                    let val = cv.get(s);
                    if matches!(val, Value::Null) {
                        continue;
                    }
                    self.nn += 1;
                    match func {
                        VecAggFunc::Count | VecAggFunc::CountStar => {}
                        VecAggFunc::Sum | VecAggFunc::Avg => match val {
                            Value::Integer(i) => self.int_sum = self.int_sum.wrapping_add(i),
                            Value::Float(f) => {
                                self.fsum.add(f);
                                self.has_f = true;
                            }
                            _ => {}
                        },
                        VecAggFunc::Min => {
                            if self
                                .min
                                .as_ref()
                                .is_none_or(|m| val.partial_cmp(m) == Some(std::cmp::Ordering::Less))
                            {
                                self.min = Some(val);
                            }
                        }
                        VecAggFunc::Max => {
                            if self
                                .max
                                .as_ref()
                                .is_none_or(|m| {
                                    val.partial_cmp(m) == Some(std::cmp::Ordering::Greater)
                                })
                            {
                                self.max = Some(val);
                            }
                        }
                    }
                }
            }
        }
    }

    /// 单行内联折叠（融合过滤+聚合的单遍路径）。
    #[inline]
    fn fold_one(&mut self, cv: &ColumnVector, i: usize, func: VecAggFunc) {
        if cv.valid.is_null(i) {
            return;
        }
        self.nn += 1;
        match &cv.data {
            ColData::I64(v) => {
                let x = v[i];
                match func {
                    VecAggFunc::Sum | VecAggFunc::Avg => {
                        self.int_sum = self.int_sum.wrapping_add(x)
                    }
                    VecAggFunc::Min => {
                        if self.min.as_ref().is_none_or(|m| match m {
                            Value::Integer(mi) => x < *mi,
                            _ => true,
                        }) {
                            self.min = Some(Value::Integer(x));
                        }
                    }
                    VecAggFunc::Max => {
                        if self.max.as_ref().is_none_or(|m| match m {
                            Value::Integer(mi) => x > *mi,
                            _ => true,
                        }) {
                            self.max = Some(Value::Integer(x));
                        }
                    }
                    _ => {}
                }
            }
            ColData::F64(v) => {
                let x = v[i];
                match func {
                    VecAggFunc::Sum | VecAggFunc::Avg => {
                        self.fsum.add(x);
                        self.has_f = true;
                    }
                    VecAggFunc::Min => {
                        if self.min.as_ref().is_none_or(|m| match m {
                            Value::Float(mf) => x < *mf,
                            _ => true,
                        }) {
                            self.min = Some(Value::Float(x));
                        }
                    }
                    VecAggFunc::Max => {
                        if self.max.as_ref().is_none_or(|m| match m {
                            Value::Float(mf) => x > *mf,
                            _ => true,
                        }) {
                            self.max = Some(Value::Float(x));
                        }
                    }
                    _ => {}
                }
            }
            _ => {
                let val = cv.get(i);
                match func {
                    VecAggFunc::Sum | VecAggFunc::Avg => match val {
                        Value::Integer(x) => self.int_sum = self.int_sum.wrapping_add(x),
                        Value::Float(x) => {
                            self.fsum.add(x);
                            self.has_f = true;
                        }
                        _ => {}
                    },
                    VecAggFunc::Min => {
                        if self
                            .min
                            .as_ref()
                            .is_none_or(|m| val.partial_cmp(m) == Some(std::cmp::Ordering::Less))
                        {
                            self.min = Some(val);
                        }
                    }
                    VecAggFunc::Max => {
                        if self
                            .max
                            .as_ref()
                            .is_none_or(|m| val.partial_cmp(m) == Some(std::cmp::Ordering::Greater))
                        {
                            self.max = Some(val);
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    /// 全行折叠（无谓词路径 — 不物化 selection）。
    fn fold_all(&mut self, cv: &ColumnVector, func: VecAggFunc) {
        let n = cv.len();
        for i in 0..n {
            if cv.valid.is_null(i) {
                continue;
            }
            self.nn += 1;
            match &cv.data {
                ColData::I64(v) => match func {
                    VecAggFunc::Sum | VecAggFunc::Avg => {
                        self.int_sum = self.int_sum.wrapping_add(v[i])
                    }
                    VecAggFunc::Min => {
                        if self.min.as_ref().is_none_or(|m| match m {
                            Value::Integer(mi) => v[i] < *mi,
                            _ => true,
                        }) {
                            self.min = Some(Value::Integer(v[i]));
                        }
                    }
                    VecAggFunc::Max => {
                        if self.max.as_ref().is_none_or(|m| match m {
                            Value::Integer(mi) => v[i] > *mi,
                            _ => true,
                        }) {
                            self.max = Some(Value::Integer(v[i]));
                        }
                    }
                    _ => {}
                },
                ColData::F64(v) => match func {
                    VecAggFunc::Sum | VecAggFunc::Avg => {
                        self.fsum.add(v[i]);
                        self.has_f = true;
                    }
                    VecAggFunc::Min => {
                        if self.min.as_ref().is_none_or(|m| match m {
                            Value::Float(mf) => v[i] < *mf,
                            _ => true,
                        }) {
                            self.min = Some(Value::Float(v[i]));
                        }
                    }
                    VecAggFunc::Max => {
                        if self.max.as_ref().is_none_or(|m| match m {
                            Value::Float(mf) => v[i] > *mf,
                            _ => true,
                        }) {
                            self.max = Some(Value::Float(v[i]));
                        }
                    }
                    _ => {}
                },
                _ => {
                    let val = cv.get(i);
                    match func {
                        VecAggFunc::Sum | VecAggFunc::Avg => match val {
                            Value::Integer(x) => self.int_sum = self.int_sum.wrapping_add(x),
                            Value::Float(x) => {
                                self.fsum.add(x);
                                self.has_f = true;
                            }
                            _ => {}
                        },
                        VecAggFunc::Min => {
                            if self
                                .min
                                .as_ref()
                                .is_none_or(|m| val.partial_cmp(m) == Some(std::cmp::Ordering::Less))
                            {
                                self.min = Some(val);
                            }
                        }
                        VecAggFunc::Max => {
                            if self
                                .max
                                .as_ref()
                                .is_none_or(|m| {
                                    val.partial_cmp(m) == Some(std::cmp::Ordering::Greater)
                                })
                            {
                                self.max = Some(val);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    fn finalize(&self, func: VecAggFunc) -> Value {
        match func {
            VecAggFunc::CountStar => Value::Integer(self.count as i64),
            VecAggFunc::Count => Value::Integer(self.nn as i64),
            VecAggFunc::Sum | VecAggFunc::Avg => {
                if self.nn == 0 {
                    return Value::Null;
                }
                if self.has_f {
                    let total = self.fsum.total() + self.int_sum as f64;
                    if func == VecAggFunc::Sum {
                        Value::Float(total)
                    } else {
                        Value::Float(total / self.nn as f64)
                    }
                } else if func == VecAggFunc::Sum {
                    Value::Integer(self.int_sum)
                } else {
                    Value::Float(self.int_sum as f64 / self.nn as f64)
                }
            }
            VecAggFunc::Min => self.min.clone().unwrap_or(Value::Null),
            VecAggFunc::Max => self.max.clone().unwrap_or(Value::Null),
        }
    }
}

// ───────────────────────── 接线 API ─────────────────────────

pub struct VecScanAggOutcome {
    pub columns: Vec<String>,
    pub values: Vec<Value>,
}

/// `SELECT AGG(...)[, AGG(...)] FROM t [WHERE 简单谓词]` — 无 GROUP BY。
/// 收编条件：ColSegmentStore 表、全部 SELECT 项为六种简单聚合、WHERE 可
/// 编译（或无）、聚合列非 Tensor/Spatial。返回 None → 回退旧路径。
pub fn try_vec_no_group_aggregate(
    store: &Arc<ColSegmentStore>,
    schema: &TableSchema,
    stmt: &SelectStmt,
) -> Result<Option<VecScanAggOutcome>> {
    if !vec_enabled() {
        return Ok(None);
    }
    // 🔑 事务 read-your-writes: 段批只含已 flush 的已提交行, 看不到
    // write_set 的未提交 INSERT/DELETE — 事务激活时 decline 走旧路径
    // (ryw_insert_visible_in_full_scan: 事务内 COUNT 必须含未提交行)。
    if crate::sql::executor::QueryExecutor::is_in_transaction_tls() {
        return Ok(None);
    }
    if stmt.group_by.is_some()
        || stmt.having.is_some()
        || stmt.distinct
        || stmt.order_by.is_some()
        || stmt.limit.is_some()
        || stmt.latest_by.is_some()
    {
        return Ok(None);
    }
    let mut specs: Vec<(String, VecAggSpec)> = Vec::new();
    for sc in &stmt.columns {
        let SelectColumn::Expr(expr, alias) = sc else {
            return Ok(None); // 纯列/Star → 非聚合形状
        };
        let Some((func, arg)) = parse_simple_agg(expr) else {
            return Ok(None);
        };
        let col = match arg {
            Some(Expr::Column(c)) => match schema.get_column_position(c) {
                Some(p) => Some(p),
                None => return Ok(None),
            },
            Some(_) => return Ok(None), // 聚合参数必须是简单列
            None => None,
        };
        let name = alias
            .clone()
            .unwrap_or_else(|| crate::sql::executor::QueryExecutor::expr_to_column_name(expr));
        specs.push((
            name,
            VecAggSpec {
                func,
                col: col.filter(|_| !matches!(func, VecAggFunc::CountStar)),
            },
        ));
    }
    if specs.is_empty() {
        return Ok(None);
    }
    let mut pred = match &stmt.where_clause {
        Some(w) => match VecPred::compile(w, schema) {
            Some(p) => Some(p),
            None => return Ok(None),
        },
        None => None,
    };

    let cts = schema.col_types();
    let mut needed: Vec<usize> = Vec::new();
    if let Some(p) = &pred {
        collect_pred_cols(p, &mut needed);
    }
    for (_, s) in &specs {
        if let Some(c) = s.col {
            needed.push(c);
        }
    }
    needed.sort_unstable();
    needed.dedup();
    if needed
        .iter()
        .any(|&c| matches!(cts.get(c), Some(ColumnType::Tensor(_) | ColumnType::Spatial)))
    {
        return Ok(None); // 复杂类型聚合走旧路径
    }

    // 🔑 批只装载 needed 列 — 谓词叶与聚合列的 schema 列位改写为批内相对位
    // (此前 eval 按_schema_位置索引批列 → 越界 panic, 差分首跑抓出)。
    if let Some(p) = pred.as_mut() {
        remap_pred(p, &needed);
    }
    for (_, spec) in specs.iter_mut() {
        if let Some(c) = spec.col.as_mut() {
            if let Some(i) = needed.iter().position(|&x| x == *c) {
                *c = i;
            }
        }
    }

    let _ = store.flush_buffer();
    let segments = store.segments_snapshot();
    // 🔑 需要跨段 key 去重的条件: 有墓碑 ∨ 多段。UPDATE 的墓碑在段合并后
    // 会消失 (合并保留最新版本、无 deleted 位), 但旧段仍残留同 key 旧版本
    // — 只查墓碑会双计 (fuzz seed 18: GROUP BY id%5 多 6 行 = 恰好 6 个
    // 被 UPDATE 的行)。纯插入多段 (row_id 唯一) 也走去重 — 正确性优先,
    // 单段无墓碑 (checkpoint 合并后) 才走快路径。
    let needs_dedup =
        segments.iter().any(|s| s.has_any_deleted()) || segments.len() > 1;
    let mut seen_keys: std::collections::HashSet<u64> = std::collections::HashSet::new();
    // 🔑 M1 保守门: 任一段含墓碑 → decline。已提交 DELETE 的段状态一致
    // (本路径可正确处理), 但 事务回滚 的 undo 重插走 insert_row_to_table
    // 双写 (builder SST + 段), 段与 builder 可能发散 (ryw_delete 回滚后
    // COUNT 少 1, ACID 审计抓出) — M2 统一可见性后放开。
    if segments.iter().any(|s| s.has_any_deleted()) {
        return Ok(None);
    }

    let mut accs: Vec<VecAcc> = specs.iter().map(|_| VecAcc::default()).collect();
    for seg in segments.iter().rev() {
        let n = seg.row_count;
        if n == 0 {
            continue;
        }
        let mut cols: Vec<std::sync::Arc<ColumnVector>> = Vec::with_capacity(needed.len());
        for &c in &needed {
            let Some(cv) = seg.read_column_batch(c, &cts[c]) else {
                return Ok(None); // 读取失败（压缩等）→ 回退
            };
            cols.push(cv);
        }
        let batch = ColumnBatch::new_shared(cols);
        // 🔑 快路径: 全段无墓碑 (纯插入 — row_id 唯一 ⇒ key 唯一) 时跳过
        // key 去重 (100K keys 加载 + HashSet 曾给每查询加 ~3ms)。
        // 有墓碑: 逆序 newest-wins (同 key 重复时新版本在下标大端)。
        let vis_sel = if !needs_dedup {
            let mut s0 = crate::storage::colbatch::SelectionVec::with_capacity(n);
            for i in 0..n {
                s0.push(i as u32);
            }
            s0
        } else {
            let keys = seg.keys();
            let mut vis: Vec<u32> = Vec::with_capacity(n);
            let mut i = n;
            while i > 0 {
                i -= 1;
                if seg.is_row_deleted(i) {
                    seen_keys.insert(keys[i]);
                    continue;
                }
                if seen_keys.insert(keys[i]) {
                    vis.push(i as u32);
                }
            }
            vis.reverse();
            crate::storage::colbatch::SelectionVec::from_vec(vis)
        };
        if vis_sel.is_empty() {
            continue;
        }
        match &pred {
            None => {
                for (ai, (_, spec)) in specs.iter().enumerate() {
                    match spec.func {
                        VecAggFunc::CountStar => accs[ai].count += vis_sel.len() as u64,
                        _ => {
                            let c = spec.col.expect("non-CountStar has col");
                            accs[ai].fold_batch(&batch.cols[c], &vis_sel, spec.func);
                        }
                    }
                }
            }
            Some(p) => {
                let mut chain: Vec<&VecPredLeaf> = Vec::new();
                if p.as_and_chain(&mut chain) {
                    // 🔑 融合过滤+聚合（AND 链, 可见集上单遍）。
                    let has_fold = specs
                        .iter()
                        .any(|(_, sp)| sp.func != VecAggFunc::CountStar);
                    let fold_specs: Vec<(usize, VecAggFunc, usize)> = specs
                        .iter()
                        .enumerate()
                        .filter(|(_, (_, sp))| sp.func != VecAggFunc::CountStar)
                        .map(|(ai, (_, sp))| {
                            (ai, sp.func, sp.col.expect("non-CountStar has col"))
                        })
                        .collect();
                    let mut hits: u64 = 0;
                    for r in vis_sel.iter() {
                        let i = r as usize;
                        let mut pass = true;
                        for l in &chain {
                            if leaf_tv_leaf(&batch.cols, l, i) != TV::True {
                                pass = false;
                                break;
                            }
                        }
                        if !pass {
                            continue;
                        }
                        hits += 1;
                        if has_fold {
                            for &(ai, f, c) in &fold_specs {
                                accs[ai].fold_one(&batch.cols[c], i, f);
                            }
                        }
                    }
                    for (ai, (_, sp)) in specs.iter().enumerate() {
                        if sp.func == VecAggFunc::CountStar {
                            accs[ai].count += hits;
                        }
                    }
                } else {
                    // 混合形状 (OR/NOT): 谓词 selection ∩ 可见集
                    let pred_set: std::collections::HashSet<u32> =
                        p.eval_sel(&batch).iter().collect();
                    let filtered: Vec<u32> = vis_sel
                        .iter()
                        .filter(|i| pred_set.contains(i))
                        .collect();
                    let sel = crate::storage::colbatch::SelectionVec::from_vec(filtered);
                    if sel.is_empty() {
                        continue;
                    }
                    for (ai, (_, spec)) in specs.iter().enumerate() {
                        match spec.func {
                            VecAggFunc::CountStar => accs[ai].count += sel.len() as u64,
                            _ => {
                                let c = spec.col.expect("non-CountStar has col");
                                accs[ai].fold_batch(&batch.cols[c], &sel, spec.func);
                            }
                        }
                    }
                }
            }
        }
    }

    let columns: Vec<String> = specs.iter().map(|(n, _)| n.clone()).collect();
    let values: Vec<Value> = specs
        .iter()
        .zip(accs.iter())
        .map(|((_, spec), acc)| acc.finalize(spec.func))
        .collect();
    Ok(Some(VecScanAggOutcome { columns, values }))
}

#[inline]
fn fold_row(accs: &mut [VecAcc], agg_specs: &[VecAggSpec], cols: &[std::sync::Arc<ColumnVector>], i: usize) {
    for (ai, sp) in agg_specs.iter().enumerate() {
        match sp.func {
            VecAggFunc::CountStar => accs[ai].count += 1,
            _ => {
                let c = sp.col.expect("non-CountStar has col");
                accs[ai].fold_one(&cols[c], i, sp.func);
            }
        }
    }
}

fn batchless_type(cts: &[ColumnType], c: usize) -> ColumnType {
    cts.get(c).cloned().unwrap_or(ColumnType::Integer)
}

fn parse_simple_agg(e: &Expr) -> Option<(VecAggFunc, Option<&Expr>)> {
    let Expr::FunctionCall {
        name,
        args,
        distinct,
        ..
    } = e
    else {
        return None;
    };
    if *distinct {
        return None;
    }
    let f = name.to_uppercase();
    if f == "COUNT" {
        if let Some(Expr::Column(c)) = args.first() {
            if c == "*" {
                return Some((VecAggFunc::CountStar, None));
            }
        }
    }
    match (f.as_str(), args.first()) {
        ("COUNT", Some(a)) => Some((VecAggFunc::Count, Some(a))),
        ("SUM", Some(a)) => Some((VecAggFunc::Sum, Some(a))),
        ("AVG", Some(a)) => Some((VecAggFunc::Avg, Some(a))),
        ("MIN", Some(a)) => Some((VecAggFunc::Min, Some(a))),
        ("MAX", Some(a)) => Some((VecAggFunc::Max, Some(a))),
        _ => None,
    }
}

// ───────────────────────── M2: 批 GROUP BY ─────────────────────────

/// 组键表达式：列引用或小型算术桶表达式（id % N、a + b 等）。
/// 不可求值 → None 回退。
#[derive(Debug, Clone)]
enum KeyExpr {
    Col(usize),
    /// 算术：左操作数（列或嵌套算术）、op、右字面量
    Arith(Box<KeyExpr>, BinaryOperator, Value),
}

impl KeyExpr {
    fn compile(e: &Expr, schema: &TableSchema) -> Option<KeyExpr> {
        match e {
            Expr::Column(c) => {
                let bare = c.rsplit('.').next().unwrap_or(c);
                let pos = schema
                    .get_column_position(c)
                    .or_else(|| schema.get_column_position(bare))?;
                Some(KeyExpr::Col(pos))
            }
            Expr::Literal(v) => Some(KeyExpr::Arith(
                Box::new(KeyExpr::Col(usize::MAX)), // 占位: 常量键不常用, 拒收
                BinaryOperator::Add,
                v.clone(),
            ))
            .filter(|_| false),
            Expr::BinaryOp { left, op, right } => {
                let ok_op = matches!(
                    op,
                    BinaryOperator::Add
                        | BinaryOperator::Sub
                        | BinaryOperator::Mul
                        | BinaryOperator::Mod
                        | BinaryOperator::Div
                );
                if !ok_op {
                    return None;
                }
                // 形态: <expr> op <literal> — id % 5 最常见
                if let (_, Expr::Literal(v)) = (left.as_ref(), right.as_ref()) {
                    let l = Self::compile(left, schema)?;
                    return Some(KeyExpr::Arith(Box::new(l), op.clone(), v.clone()));
                }
                // 形态: <literal> op <expr>
                if let (Expr::Literal(v), _) = (left.as_ref(), right.as_ref()) {
                    let r = Self::compile(right, schema)?;
                    // 常量在左: 5 - x 等 — 翻转 op 语义复杂, 拒收
                    let _ = (v, r);
                    return None;
                }
                None
            }
            _ => None,
        }
    }

    /// 需要的 schema 列位集合。
    fn collect_cols(&self, out: &mut Vec<usize>) {
        match self {
            KeyExpr::Col(c) => {
                if *c != usize::MAX {
                    out.push(*c)
                }
            }
            KeyExpr::Arith(l, _, _) => l.collect_cols(out),
        }
    }

    /// 整型键快路径: 列与字面量均为整型时直接在切片上算 — 零 Value
    /// 构造 (Vec<Value> 键 + 每行分配曾占 1.8µs/行)。
    fn eval_i64(&self, batch: &ColumnBatch, i: usize, remap: &dyn Fn(usize) -> usize) -> Option<i64> {
        match self {
            KeyExpr::Col(c) => {
                if *c == usize::MAX {
                    return None;
                }
                let cv = &batch.cols[remap(*c)];
                if cv.valid.is_null(i) {
                    return None; // NULL 键走通用路径
                }
                match &cv.data {
                    ColData::I64(v) => Some(v[i]),
                    _ => None,
                }
            }
            KeyExpr::Arith(l, op, lit) => {
                let x = l.eval_i64(batch, i, remap)?;
                let y = match lit {
                    Value::Integer(n) => *n,
                    _ => return None,
                };
                match op {
                    BinaryOperator::Add => x.checked_add(y),
                    BinaryOperator::Sub => x.checked_sub(y),
                    BinaryOperator::Mul => x.checked_mul(y),
                    BinaryOperator::Mod => {
                        if y == 0 {
                            None
                        } else {
                            Some(x.rem_euclid(y))
                        }
                    }
                    BinaryOperator::Div => {
                        if y == 0 {
                            None
                        } else {
                            Some(x / y)
                        }
                    }
                    _ => None,
                }
            }
        }
    }

    /// 批上求第 i 行的键值（类型化求值 — 列读 + 算术，零 SqlRow）。
    fn eval(&self, batch: &ColumnBatch, i: usize, remap: &dyn Fn(usize) -> usize) -> Option<Value> {
        match self {
            KeyExpr::Col(c) => {
                if *c == usize::MAX {
                    return None;
                }
                Some(batch.cols[remap(*c)].get(i))
            }
            KeyExpr::Arith(l, op, lit) => {
                let base = l.eval(batch, i, remap)?;
                if matches!(base, Value::Null) || matches!(lit, Value::Null) {
                    return Some(Value::Null); // 算术含 NULL → NULL 键
                }
                arith(&base, op, lit)
            }
        }
    }
}

fn arith(a: &Value, op: &BinaryOperator, b: &Value) -> Option<Value> {
    use std::ops::{Add, Mul, Sub};
    let pair = |x: &Value, y: &Value| -> Option<(i64, i64)> {
        match (x, y) {
            (Value::Integer(p), Value::Integer(q)) => Some((*p, *q)),
            _ => None,
        }
    };
    match op {
        BinaryOperator::Add => pair(a, b).map(|(x, y)| Value::Integer(x.add(y))),
        BinaryOperator::Sub => pair(a, b).map(|(x, y)| Value::Integer(x.sub(y))),
        BinaryOperator::Mul => pair(a, b).map(|(x, y)| Value::Integer(x.mul(y))),
        BinaryOperator::Mod => pair(a, b).and_then(|(x, y)| {
            if y == 0 {
                None
            } else {
                Some(Value::Integer(x.rem_euclid(y)))
            }
        }),
        BinaryOperator::Div => pair(a, b).and_then(|(x, y)| {
            if y == 0 {
                None
            } else if x % y == 0 {
                Some(Value::Integer(x / y))
            } else {
                Some(Value::Float(x as f64 / y as f64))
            }
        }),
        _ => None,
    }
}

pub struct VecGroupByOutcome {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

/// 🚀 M2 批 GROUP BY: `SELECT <键…>, AGG(…)… FROM t [WHERE 简单谓词]
/// GROUP BY <同名键…> [ORDER BY 输出列] [LIMIT]`。
///
/// 与 try_expression_group_by (行式) 的差别: 列从段批读 (CachedCol::Batch
/// 稳态零重建)、键在类型化切片上求值、聚合批内折叠 — 无 SqlRow/全行
/// 物化。收集 M1 同样的保守门 (事务/墓碑 decline)。
#[allow(clippy::too_many_lines)]
pub fn try_vec_group_by(
    store: &Arc<ColSegmentStore>,
    schema: &TableSchema,
    stmt: &SelectStmt,
) -> Result<Option<VecGroupByOutcome>> {
    use std::collections::HashMap;

    if !vec_enabled() {
        return Ok(None);
    }
    if crate::sql::executor::QueryExecutor::is_in_transaction_tls() {
        return Ok(None);
    }
    let Some(group_items) = stmt.group_by.as_ref() else {
        return Ok(None);
    };
    if group_items.is_empty() || group_items.len() > 2 || stmt.having.is_some() || stmt.distinct {
        return Ok(None);
    }
    if stmt.latest_by.is_some() {
        return Ok(None);
    }

    // SELECT 解析: 输出序 [Key(KeyExpr, 输出名) | Agg(spec)]
    enum Out {
        Key(usize),
        Agg(usize),
    }
    let mut key_exprs: Vec<KeyExpr> = Vec::new();
    let mut out_names: Vec<String> = Vec::new();
    let mut out_cols: Vec<Out> = Vec::new();
    let mut agg_specs: Vec<VecAggSpec> = Vec::new();
    for sc in &stmt.columns {
        match sc {
            SelectColumn::Star => return Ok(None),
            SelectColumn::Column(_) | SelectColumn::ColumnWithAlias(_, _) => {
                // 普通列组键: 交给 col_segment_group_by (已 1.5ms 级);
                // 这里只收表达式键形状 (旧路径 14-35ms 的痛点)。
                return Ok(None);
            }
            SelectColumn::Expr(expr, alias) => {
                if let Some((func, arg)) = parse_simple_agg(expr) {
                    let col = match arg {
                        Some(Expr::Column(c)) => {
                            let bare = c.rsplit('.').next().unwrap_or(c);
                            match schema
                                .get_column_position(c)
                                .or_else(|| schema.get_column_position(bare))
                            {
                                Some(p) => Some(p),
                                None => return Ok(None),
                            }
                        }
                        Some(_) => return Ok(None),
                        None => None,
                    };
                    out_names.push(
                        alias
                            .clone()
                            .unwrap_or_else(|| crate::sql::executor::QueryExecutor::expr_to_column_name(expr)),
                    );
                    out_cols.push(Out::Agg(agg_specs.len()));
                    agg_specs.push(VecAggSpec {
                        func,
                        col: col.filter(|_| !matches!(func, VecAggFunc::CountStar)),
                    });
                } else {
                    // 组键表达式 — canonical/别名须匹配某 GROUP BY 项
                    let name = alias
                        .clone()
                        .unwrap_or_else(|| crate::sql::executor::QueryExecutor::expr_to_column_name(expr));
                    let canonical = crate::sql::executor::QueryExecutor::expr_to_column_name(expr);
                    let matched = group_items.iter().any(|g| g == &name || g == &canonical);
                    if !matched || key_exprs.len() + 1 > group_items.len() {
                        return Ok(None);
                    }
                    let Some(ke) = KeyExpr::compile(expr, schema) else { return Ok(None) };
                    out_names.push(name);
                    out_cols.push(Out::Key(key_exprs.len()));
                    key_exprs.push(ke);
                }
            }
        }
    }
    if key_exprs.is_empty() || agg_specs.is_empty() || key_exprs.len() != group_items.len() {
        return Ok(None);
    }
    // WHERE 编译
    let mut pred = match &stmt.where_clause {
        Some(w) => match VecPred::compile(w, schema) {
            Some(p) => Some(p),
            None => return Ok(None),
        },
        None => None,
    };

    let cts = schema.col_types();
    let mut needed: Vec<usize> = Vec::new();
    if let Some(p) = &pred {
        collect_pred_cols(p, &mut needed);
    }
    for ke in &key_exprs {
        ke.collect_cols(&mut needed);
    }
    for sp in &agg_specs {
        if let Some(c) = sp.col {
            needed.push(c);
        }
    }
    needed.sort_unstable();
    needed.dedup();
    if needed
        .iter()
        .any(|&c| matches!(cts.get(c), Some(ColumnType::Tensor(_) | ColumnType::Spatial)))
    {
        return Ok(None);
    }
    if let Some(p) = pred.as_mut() {
        remap_pred(p, &needed);
    }
    for sp in agg_specs.iter_mut() {
        if let Some(c) = sp.col.as_mut() {
            if let Some(i) = needed.iter().position(|&x| x == *c) {
                *c = i;
            }
        }
    }
    let key_remap = |c: usize| -> usize {
        needed.iter().position(|&x| x == c).unwrap_or(0)
    };

    let _ = store.flush_buffer();
    let segments = store.segments_snapshot();
    // 🔑 与 M1 相同的多段/墓碑保守门 (UPDATE 合并后墓碑消失但跨段同 key
    // 残留 → 双计)。M2 直接 decline (M1 有去重机械, 这里表达式键场景
    // post-checkpoint 单段是常态)。
    if segments.iter().any(|s| s.has_any_deleted()) || segments.len() > 1 {
        return Ok(None);
    }

    // 🔑 单整型键快路径: HashMap<i64> + 零 Value 构造; 键含 NULL/文本/浮点
    // 或多键 → 通用 Vec<Value> 路径。
    let single_int_key = key_exprs.len() == 1 && agg_specs.iter().all(|s| {
        s.col.map_or(true, |c| {
            matches!(batchless_type(&cts, needed[c]), ColumnType::Integer | ColumnType::Timestamp)
        })
    });
    let mut groups_i: HashMap<i64, Vec<VecAcc>> = HashMap::new();
    let mut groups_v: HashMap<Vec<Value>, Vec<VecAcc>> = HashMap::new();
    for seg in &segments {
        let n = seg.row_count;
        if n == 0 {
            continue;
        }
        let mut cols: Vec<std::sync::Arc<ColumnVector>> = Vec::with_capacity(needed.len());
        for &c in &needed {
            let Some(cv) = seg.read_column_batch(c, &cts[c]) else {
                return Ok(None);
            };
            cols.push(cv);
        }
        let batch = ColumnBatch::new_shared(cols);
        // 纯插入段 (无墓碑): 全行可见
        let rows: Vec<u32> = match &pred {
            Some(p) => {
                let mut chain: Vec<&VecPredLeaf> = Vec::new();
                if p.as_and_chain(&mut chain) {
                    let mut sel = Vec::with_capacity(n);
                    for i in 0..n {
                        let mut pass = true;
                        for l in &chain {
                            if leaf_tv_leaf(&batch.cols, l, i) != TV::True {
                                pass = false;
                                break;
                            }
                        }
                        if pass {
                            sel.push(i as u32);
                        }
                    }
                    sel
                } else {
                    p.eval_sel(&batch).into_inner()
                }
            }
            None => (0..n as u32).collect(),
        };
        for r in rows {
            let i = r as usize;
            if single_int_key {
                // 快路径: 键为 NULL 时入通用表 (NULL 分组)
                if let Some(k) = key_exprs[0].eval_i64(&batch, i, &key_remap) {
                    let accs = groups_i
                        .entry(k)
                        .or_insert_with(|| agg_specs.iter().map(|_| VecAcc::default()).collect());
                    fold_row(&mut accs[..], &agg_specs, &batch.cols, i);
                    continue;
                }
            }
            let mut key = Vec::with_capacity(key_exprs.len());
            for ke in &key_exprs {
                match ke.eval(&batch, i, &key_remap) {
                    Some(v) => key.push(v),
                    None => return Ok(None),
                }
            }
            let accs = groups_v
                .entry(key)
                .or_insert_with(|| agg_specs.iter().map(|_| VecAcc::default()).collect());
            fold_row(&mut accs[..], &agg_specs, &batch.cols, i);
        }
    }

    // 组装输出行: 键 + 聚合值 (输出序)。i64 快表并入通用表。
    let mut merged: Vec<(Vec<Value>, Vec<VecAcc>)> = groups_v.into_iter().collect();
    for (k, accs) in groups_i {
        merged.push((vec![Value::Integer(k)], accs));
    }
    let rows_from = merged;
    let mut rows: Vec<Vec<Value>> = rows_from
        .into_iter()
        .map(|(keys, accs)| {
            out_cols
                .iter()
                .map(|c| match c {
                    Out::Key(i) => keys[*i].clone(),
                    Out::Agg(i) => accs[*i].finalize(agg_specs[*i].func),
                })
                .collect::<Vec<_>>()
        })
        .collect();

    // ORDER BY: 输出列名/别名唯一命中 (与 M1 的 try_expression_group_by 一致)
    if let Some(ref ob) = stmt.order_by {
        let mut specs: Vec<(usize, bool)> = Vec::new();
        for oe in ob {
            // 两种键: 序号字面量 (ORDER BY 1) / 输出列名或别名
            let hit = match &oe.expr {
                Expr::Literal(Value::Integer(n)) if *n >= 1 => {
                    Some((*n as usize).wrapping_sub(1)).filter(|&p| p < out_names.len())
                }
                Expr::Column(cn) => {
                    let hits: Vec<usize> = out_names
                        .iter()
                        .enumerate()
                        .filter(|(_, nm)| {
                            nm.as_str() == cn.as_str()
                                || nm.rsplit('.').next().unwrap_or(nm) == cn.as_str()
                        })
                        .map(|(i, _)| i)
                        .collect();
                    if hits.len() == 1 {
                        Some(hits[0])
                    } else {
                        None
                    }
                }
                _ => None,
            };
            let Some(p) = hit else {
                return Ok(None);
            };
            specs.push((p, oe.asc));
        }
        if !specs.is_empty() {
            rows.sort_by(|a, b| {
                for &(i, asc) in &specs {
                    let c = crate::storage::colbatch::colbatch_order_cmp(&a[i], &b[i]);
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
    Ok(Some(VecGroupByOutcome {
        columns: out_names,
        rows,
    }))
}
