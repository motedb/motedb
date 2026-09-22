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

/// 🔑 M6 起默认开启 (MOTE_VEC=off 显式关闭回到全旧路径): 事务回滚 undo
/// 双写的发散窗口已被三重保守门完整掩蔽 — 事务内 decline / 任一段含墓碑
/// decline / 多段 decline (合并 newest-wins 修正)。ACID 22/22 (vec on) +
/// 深度差分 fuzz + bigtable 重开 0 发散背书。
/// 🔑 M6: 默认开启。M1-M5 期间默认关闭是因为事务回滚的 undo 重插走双写
/// (builder SST + 段缓冲), 段与 builder 可发散 — 但 vec 路径的三重保守门
/// (事务内 decline / 任一段含墓碑 decline / 多段 decline + 合并 newest-wins
/// 修正) 已把发散窗口完整掩蔽: ACID 审计 22/22 (vec on)、深度差分 fuzz
/// (400 查询 × 4 seed)、bigtable 重开 0 发散全绿。
/// 🔥 灭火开关: MOTE_VEC=off 一键回到全旧路径 (行为与 M5 之前完全一致)。
pub fn vec_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("MOTE_VEC").map_or(true, |v| {
            !(v == "off" || v == "0" || v == "false")
        })
    })
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
        Self::compile_with_alias(e, schema, None)
    }

    /// alias 感知编译: 限定名 `<alias>.<col>` 仅在 alias 匹配时剥前缀解析
    /// (join 的 WHERE 按表侧拆分用)。
    fn compile_with_alias(e: &Expr, schema: &TableSchema, alias: Option<&str>) -> Option<VecPred> {
        match e {
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And => Some(VecPred::And(
                    Box::new(Self::compile_with_alias(left, schema, alias)?),
                    Box::new(Self::compile_with_alias(right, schema, alias)?),
                )),
                BinaryOperator::Or => Some(VecPred::Or(
                    Box::new(Self::compile_with_alias(left, schema, alias)?),
                    Box::new(Self::compile_with_alias(right, schema, alias)?),
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
                    let (col, lit) = Self::col_lit(left, right, schema, alias)?;
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
            } => Self::compile_with_alias(expr, schema, alias)
                .map(|p| VecPred::Not(Box::new(p))),
            Expr::IsNull { expr, negated } => {
                let Expr::Column(c) = expr.as_ref() else {
                    return None;
                };
                let pos = resolve_col_alias(c, schema, alias)?;
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
                let pos = resolve_col_alias(c, schema, alias)?;
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

    fn col_lit(
        left: &Expr,
        right: &Expr,
        schema: &TableSchema,
        alias: Option<&str>,
    ) -> Option<(usize, Value)> {
        if let (Expr::Column(c), r) = (left, right) {
            let lit = literal_of(r)?;
            let pos = resolve_col_alias(c, schema, alias)?;
            // 🔑 Timestamp 列 vs 文本字面量 (`ts = '2024-01-15T10:30:00'`):
            // 旧路径求值时把字符串解析为时间戳比较; vec 叶是 I64 vs Text
            // 类型化比较 → 恒 false (test_timestamp_eq_string 抓出)。
            // 编译期一次性预解析为 micros 整数; 解析失败保留原字面量
            // (数值比较恒 false — 与旧路径未解析字符串不匹配的行为一致)。
            let lit = match (&lit, schema.col_types().get(pos)) {
                (Value::Text(s), Some(ColumnType::Timestamp)) => {
                    match crate::types::Timestamp::parse_iso(s.as_str()) {
                        Some(ts) => Value::Integer(ts.as_micros()),
                        None => lit,
                    }
                }
                _ => lit,
            };
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

/// alias 感知列解析: 裸名直接查; 限定名 `alias.col` 在 alias 匹配时剥前缀,
/// alias 不匹配 → None (该谓词不属于这个表)。
fn resolve_col_alias(c: &str, schema: &TableSchema, alias: Option<&str>) -> Option<usize> {
    if let Some((p, bare)) = c.split_once('.') {
        match alias {
            Some(a) if a == p => schema.get_column_position(bare),
            _ => None,
        }
    } else {
        schema.get_column_position(c)
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
    /// 聚合列是 TIMESTAMP（i64 micros 存储）— MIN/MAX 需还原
    /// Value::Timestamp 而非 Integer（test_timestamp_min_max）。
    pub ts: bool,
}

#[derive(Default, Clone)]
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
    /// 整数累加 — 溢出提升 Float（与旧路径 AggAccumulator 同语义:
    /// int_sum 灌入 fsum 后清零继续; wrapping_add 静默回绕是 v27 修过的
    /// bug, 默认开后差分测试 test_bug_hunt_v27::sum_near_i64_max 抓出）。
    #[inline]
    fn add_int(&mut self, x: i64) {
        if let Some(s) = self.int_sum.checked_add(x) {
            self.int_sum = s;
        } else {
            self.has_f = true;
            self.fsum.add(self.int_sum as f64);
            self.fsum.add(x as f64);
            self.int_sum = 0;
        }
    }

    /// 合并另一个累加器（M5 并行 morsel 的 partial→merge）。
    /// min/max 用引擎级全序 (colbatch_order_cmp) 合并 — 与 fold 的类型化
    /// 比较在数值列上等价。
    fn merge(&mut self, o: &VecAcc) {
        use crate::storage::colbatch::colbatch_order_cmp;
        self.count += o.count;
        self.nn += o.nn;
        self.add_int(o.int_sum);
        self.fsum.merge(&o.fsum);
        self.has_f |= o.has_f;
        if o.min.is_some() {
            let take = self.min.as_ref().is_none_or(|m| {
                colbatch_order_cmp(o.min.as_ref().unwrap(), m) == std::cmp::Ordering::Less
            });
            if take {
                self.min = o.min.clone();
            }
        }
        if o.max.is_some() {
            let take = self.max.as_ref().is_none_or(|m| {
                colbatch_order_cmp(o.max.as_ref().unwrap(), m) == std::cmp::Ordering::Greater
            });
            if take {
                self.max = o.max.clone();
            }
        }
    }

    /// 批内折叠：定长列直接在类型化切片上按 selection 走（无 Value 物化），
    /// Bool/Utf8/Values 走批边界 get。
    fn fold_batch(&mut self, cv: &ColumnVector, sel: &SelectionVec, func: VecAggFunc) {
        self.fold_rows(cv, sel.as_slice(), func)
    }

    /// 同 fold_batch，但直接吃原始行号切片 — M1 并行时 rayon par_chunks 的
    /// chunk 就是 &[u32]，免建 SelectionVec。
    fn fold_rows(&mut self, cv: &ColumnVector, rows: &[u32], func: VecAggFunc) {
        match &cv.data {
            ColData::I64(v) => {
                for s in rows.iter().copied() {
                    let s = s as usize;
                    if cv.valid.is_null(s) {
                        continue;
                    }
                    self.nn += 1;
                    let x = v[s];
                    match func {
                        VecAggFunc::Count | VecAggFunc::CountStar => {}
                        VecAggFunc::Sum | VecAggFunc::Avg => {
                            self.add_int(x)
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
                for s in rows.iter().copied() {
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
                for s in rows.iter().copied() {
                    let s = s as usize;
                    let val = cv.get(s);
                    if matches!(val, Value::Null) {
                        continue;
                    }
                    self.nn += 1;
                    match func {
                        VecAggFunc::Count | VecAggFunc::CountStar => {}
                        VecAggFunc::Sum | VecAggFunc::Avg => match val {
                            Value::Integer(i) => self.add_int(i),
                            Value::Float(f) => {
                                self.fsum.add(f);
                                self.has_f = true;
                            }
                            // 🔑 SUM(BOOLEAN): true→1/false→0（旧路径数值强转）。
                            Value::Bool(b) => self.add_int(b as i64),
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
                        self.add_int(x)
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
                        Value::Integer(x) => self.add_int(x),
                        Value::Float(x) => {
                            self.fsum.add(x);
                            self.has_f = true;
                        }
                        // 🔑 SUM(BOOLEAN): true→1 / false→0 (与旧路径数值
                        // 强转一致; test_sum_boolean 断言 2)。
                        Value::Bool(b) => self.add_int(b as i64),
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
                        self.add_int(v[i])
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
                            Value::Integer(x) => self.add_int(x),
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

    fn finalize(&self, func: VecAggFunc, ts: bool) -> Value {
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
            VecAggFunc::Min => match self.min.clone().unwrap_or(Value::Null) {
                Value::Integer(m) if ts => Value::Timestamp(crate::types::Timestamp::from_micros(m)),
                v => v,
            },
            VecAggFunc::Max => match self.max.clone().unwrap_or(Value::Null) {
                Value::Integer(m) if ts => Value::Timestamp(crate::types::Timestamp::from_micros(m)),
                v => v,
            },
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
                ts: col.map_or(false, |c| {
                    matches!(schema.col_types().get(c), Some(ColumnType::Timestamp))
                }),
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
    // 🔑 去重条件精确化: 墓碑段在下方整体 decline; 多段纯插入 (row_id 唯一
    // ⇒ key 唯一) 不再强制去重 — overlap_possible (UPDATE/DELETE 置位,
    // 重开 2+ 段保守置位, 全量合并清除) 才是跨段重复键的判据。省掉
    // keys 加载 + HashSet (~3ms/100K 行)。
    let needs_dedup = store.may_have_duplicate_keys();
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
        let vis: Vec<u32> = if !needs_dedup {
            (0..n as u32).collect()
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
            vis
        };
        if vis.is_empty() {
            continue;
        }
        // 🔑 M1 morsel 并行 (大段): 去重/可见集已顺序算好, 行折叠按 chunk 分
        // rayon 线程, partial VecAcc 主线程 merge (MIN/MAX 比较可交换, 和走
        // CompSum::merge 保 Neumaier)。三种谓词形状 (无/AND 链/混合) 都在
        // chunk 内闭式求值 — 工作线程不触碰任何 TLS 状态 (雷#1/#2/#3)。
        #[cfg(feature = "rayon")]
        if vis.len() >= PARALLEL_MORSEL_MIN_ROWS {
            use rayon::prelude::*;
            // 混合形状 (OR/NOT) 的谓词集整段先算一次 (chunk 内只做 contains)
            let mixed_set: Option<std::collections::HashSet<u32>> = match &pred {
                Some(p) => {
                    let mut chain: Vec<&VecPredLeaf> = Vec::new();
                    if !p.as_and_chain(&mut chain) {
                        Some(p.eval_sel(&batch).iter().collect())
                    } else {
                        None
                    }
                }
                None => None,
            };
            let chain: Vec<&VecPredLeaf> = match &pred {
                Some(p) => {
                    let mut c = Vec::new();
                    let _ = p.as_and_chain(&mut c);
                    c
                }
                None => Vec::new(),
            };
            let fold_specs: Vec<(usize, VecAggFunc, usize)> = specs
                .iter()
                .enumerate()
                .filter(|(_, (_, sp))| sp.func != VecAggFunc::CountStar)
                .map(|(ai, (_, sp))| (ai, sp.func, sp.col.expect("non-CountStar has col")))
                .collect();
            let nchunks = par_chunk_count(vis.len());
            let chunk_len = vis.len().div_ceil(nchunks).max(1);
            let partials: Vec<Vec<VecAcc>> = vis
                .par_chunks(chunk_len)
                .map(|chunk| {
                    let mut pa: Vec<VecAcc> = specs.iter().map(|_| VecAcc::default()).collect();
                    match &pred {
                        None => {
                            for (ai, (_, sp)) in specs.iter().enumerate() {
                                match sp.func {
                                    VecAggFunc::CountStar => pa[ai].count += chunk.len() as u64,
                                    _ => pa[ai].fold_rows(
                                        &batch.cols[sp.col.expect("non-CountStar has col")],
                                        chunk,
                                        sp.func,
                                    ),
                                }
                            }
                        }
                        Some(_) if !chain.is_empty() => {
                            let mut hits: u64 = 0;
                            for &r in chunk {
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
                                for &(ai, f, c) in &fold_specs {
                                    pa[ai].fold_one(&batch.cols[c], i, f);
                                }
                            }
                            for (ai, (_, sp)) in specs.iter().enumerate() {
                                if sp.func == VecAggFunc::CountStar {
                                    pa[ai].count += hits;
                                }
                            }
                        }
                        Some(_) => {
                            let set = mixed_set.as_ref().expect("mixed pred set precomputed");
                            let mut hits: u64 = 0;
                            for &r in chunk {
                                if !set.contains(&r) {
                                    continue;
                                }
                                hits += 1;
                                for &(ai, f, c) in &fold_specs {
                                    pa[ai].fold_one(&batch.cols[c], r as usize, f);
                                }
                            }
                            for (ai, (_, sp)) in specs.iter().enumerate() {
                                if sp.func == VecAggFunc::CountStar {
                                    pa[ai].count += hits;
                                }
                            }
                        }
                    }
                    pa
                })
                .collect();
            for pa in partials {
                for (d, s) in accs.iter_mut().zip(pa.into_iter()) {
                    d.merge(&s);
                }
            }
            continue;
        }
        let vis_sel = crate::storage::colbatch::SelectionVec::from_vec(vis);
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
        .map(|((_, spec), acc)| acc.finalize(spec.func, spec.ts))
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
/// 路由级门槛: 无 ORDER 纯列键 GROUP BY 让位 &str 还是走 M2 的分界
/// (total rows, 跨段累计)。小表 &str 零分配更快; 大表 M2 morsel 并行更快。
#[cfg(feature = "rayon")]
const PARALLEL_MIN_ROWS: usize = 100_000;

/// 段内折叠门槛: 单段可见行数 ≥ 此值才 par_chunks (per-segment)。批量导入
/// 后的常态是多段 (checkpoint 不合并小段, e.g. 100K 表 = 2×50K), 门槛按段
/// 判时 100K 门槛永远不触发 — 拆独立常量校准到 20K: 单次 par_chunks
/// (≤16 chunk) 调度+merge 开销 ~0.1ms, 20K 行的折叠工作 ≥0.5ms 仍有净收益;
/// 更小的查询不进并行分支零开销。
#[cfg(feature = "rayon")]
const PARALLEL_MORSEL_MIN_ROWS: usize = 20_000;

/// chunk 数: rayon 线程数 (封顶 16 — 更细的 morsel 只增加 merge 成本)。
#[cfg(feature = "rayon")]
fn par_chunk_count(n: usize) -> usize {
    // 线程数封顶 16, 且不超过行数 (n=0 由调用方门槛挡掉)。
    // 🔑 曾把下界写成上界 (.max(n)) → nchunks=n → 每 chunk 1 行,
    // rayon 被百万微型任务淹没 (并行比串行慢 25×, 全线程卡在 join 调度)。
    rayon::current_num_threads().clamp(1, 16).min(n.max(1))
}

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
            SelectColumn::Column(c) | SelectColumn::ColumnWithAlias(c, _) => {
                // 🔑 普通列组键也接管 — 但仅限 col_segment_group_by 会整体
                // decline 的形状 (ORDER BY/LIMIT/OFFSET): 它一见这些就落全
                // 物化+排序 (1M 行 GROUP BY+ORDER BY 147ms vs 无 ORDER 15ms)。
                // M2 的组输出 ORDER BY/LIMIT 尾部只排组数行 (≤基数)。
                // 无 ORDER BY 的纯列键: 小表仍让位 col_segment_group_by 的
                // &str 零分配路径; 大表 (≥PARALLEL_MIN_ROWS) 走 M2 morsel
                // 并行 — 1M 行实测 &str 串行 14.3ms vs M2 并行 4.8ms,
                // "不截胡"在大表上是负优化。
                if stmt.order_by.is_none()
                    && stmt.limit.is_none()
                    && stmt.offset.is_none()
                {
                    #[cfg(not(feature = "rayon"))]
                    {
                        return Ok(None);
                    }
                    #[cfg(feature = "rayon")]
                    {
                        let total_rows: u64 = store
                            .segments_snapshot()
                            .iter()
                            .map(|s| s.row_count as u64)
                            .sum();
                        if total_rows < PARALLEL_MIN_ROWS as u64 {
                            return Ok(None);
                        }
                    }
                }
                let bare = c.rsplit('.').next().unwrap_or(c);
                let matched = group_items
                    .iter()
                    .any(|g| g.rsplit('.').next().unwrap_or(g) == bare || g == c);
                if !matched || key_exprs.len() + 1 > group_items.len() {
                    return Ok(None);
                }
                let pos = match schema
                    .get_column_position(c)
                    .or_else(|| schema.get_column_position(bare))
                {
                    Some(p) => p,
                    None => return Ok(None),
                };
                out_names.push(bare.to_string());
                out_cols.push(Out::Key(key_exprs.len()));
                key_exprs.push(KeyExpr::Col(pos));
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
                        ts: col.map_or(false, |c| {
                            matches!(schema.col_types().get(c), Some(ColumnType::Timestamp))
                        }),
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
    if segments.iter().any(|s| s.has_any_deleted()) || store.may_have_duplicate_keys() {
        return Ok(None);
    }

    // 🔑 单整型键快路径: HashMap<i64> + 零 Value 构造; 键含 NULL/文本/浮点
    // 或多键 → 通用 Vec<Value> 路径。
    let single_key = key_exprs.len() == 1;
    let single_int_key = key_exprs.len() == 1 && agg_specs.iter().all(|s| {
        s.col.map_or(true, |c| {
            matches!(batchless_type(&cts, needed[c]), ColumnType::Integer | ColumnType::Timestamp)
        })
    });
    let mut groups_i: HashMap<i64, Vec<VecAcc>> = HashMap::new();
    // 🔑 单键 Value 组 (文本/浮点/混合): Value 克隆是 Arc 计数或 POD —
    // 零堆分配。此前单键也走 Vec<Value> 通用路径, 每行一次 Vec 分配
    // (1M 行 GROUP BY 42 vs 15ms 的差距来源; 并行时 16 线程在分配器
    // 锁上互相踩踏 → 25× 回退)。
    let mut groups_1: HashMap<Value, Vec<VecAcc>> = HashMap::new();
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
        #[cfg(feature = "rayon")]
        if rows.len() >= PARALLEL_MORSEL_MIN_ROWS {
            use rayon::prelude::*;
            // 🔑 M5 morsel 并行: 行按 chunk 分给 rayon 线程, 各自建 partial
            // 组表, 主线程按组合并。列批 Arc 共享零拷贝; 谓词/键/聚合都是
            // 纯字面量 (雷#1/#2/#3 的 TLS 状态不被工作线程触碰)。
            // 🔑 单键 Value 组零堆分配 (Value 克隆 = Arc 计数/POD) — 并行
            // 线程不在分配器锁上踩踏; 多键才用 Vec<Value>。
            let nchunks = par_chunk_count(rows.len());
            let chunk_len = rows.len().div_ceil(nchunks).max(1);
            let partials: Vec<(
                HashMap<i64, Vec<VecAcc>>,
                HashMap<Value, Vec<VecAcc>>,
                HashMap<Vec<Value>, Vec<VecAcc>>,
                bool, // 键求值失败 (串行路径同位 decline)
            )> = rows
                .par_chunks(chunk_len)
                .map(|chunk| {
                    let mut li: HashMap<i64, Vec<VecAcc>> = HashMap::new();
                    let mut l1: HashMap<Value, Vec<VecAcc>> = HashMap::new();
                    let mut lv: HashMap<Vec<Value>, Vec<VecAcc>> = HashMap::new();
                    let mut ok = true;
                    for &r in chunk {
                        let i = r as usize;
                        if single_int_key {
                            if let Some(k) = key_exprs[0].eval_i64(&batch, i, &key_remap) {
                                let accs = li.entry(k).or_insert_with(|| {
                                    agg_specs.iter().map(|_| VecAcc::default()).collect()
                                });
                                fold_row(&mut accs[..], &agg_specs, &batch.cols, i);
                                continue;
                            }
                        }
                        if single_key {
                            if let Some(kv) = key_exprs[0].eval_i64(&batch, i, &key_remap) {
                                let accs = l1.entry(Value::Integer(kv)).or_insert_with(|| {
                                    agg_specs.iter().map(|_| VecAcc::default()).collect()
                                });
                                fold_row(&mut accs[..], &agg_specs, &batch.cols, i);
                                continue;
                            }
                            if let Some(kv) = key_exprs[0].eval(&batch, i, &key_remap) {
                                let accs = l1.entry(kv).or_insert_with(|| {
                                    agg_specs.iter().map(|_| VecAcc::default()).collect()
                                });
                                fold_row(&mut accs[..], &agg_specs, &batch.cols, i);
                                continue;
                            }
                            ok = false;
                            break;
                        }
                        let mut key = Vec::with_capacity(key_exprs.len());
                        for ke in &key_exprs {
                            match ke.eval(&batch, i, &key_remap) {
                                Some(v) => key.push(v),
                                None => {
                                    ok = false;
                                    break;
                                }
                            }
                        }
                        if !ok {
                            break;
                        }
                        let accs = lv.entry(key).or_insert_with(|| {
                            agg_specs.iter().map(|_| VecAcc::default()).collect()
                        });
                        fold_row(&mut accs[..], &agg_specs, &batch.cols, i);
                    }
                    (li, l1, lv, ok)
                })
                .collect();
            if partials.iter().any(|(_, _, _, ok)| !ok) {
                return Ok(None);
            }
            for (li, l1, lv, _) in partials {
                for (k, src) in li {
                    let e = groups_i
                        .entry(k)
                        .or_insert_with(|| vec![VecAcc::default(); src.len()]);
                    for (d, s) in e.iter_mut().zip(src.iter()) {
                        d.merge(s);
                    }
                }
                for (k, src) in l1 {
                    let e = groups_1
                        .entry(k)
                        .or_insert_with(|| vec![VecAcc::default(); src.len()]);
                    for (d, s) in e.iter_mut().zip(src.iter()) {
                        d.merge(s);
                    }
                }
                for (k, src) in lv {
                    let e = groups_v
                        .entry(k)
                        .or_insert_with(|| vec![VecAcc::default(); src.len()]);
                    for (d, s) in e.iter_mut().zip(src.iter()) {
                        d.merge(s);
                    }
                }
            }
            continue;
        }
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
            if single_key {
                if let Some(kv) = key_exprs[0].eval_i64(&batch, i, &key_remap) {
                    let accs = groups_1
                        .entry(Value::Integer(kv))
                        .or_insert_with(|| agg_specs.iter().map(|_| VecAcc::default()).collect());
                    fold_row(&mut accs[..], &agg_specs, &batch.cols, i);
                    continue;
                }
                if let Some(kv) = key_exprs[0].eval(&batch, i, &key_remap) {
                    let accs = groups_1
                        .entry(kv)
                        .or_insert_with(|| agg_specs.iter().map(|_| VecAcc::default()).collect());
                    fold_row(&mut accs[..], &agg_specs, &batch.cols, i);
                    continue;
                }
                return Ok(None);
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

    // 组装输出行: 键 + 聚合值 (输出序)。i64 快表与单键 Value 表并入通用表。
    let mut merged: Vec<(Vec<Value>, Vec<VecAcc>)> = groups_v.into_iter().collect();
    for (k, accs) in groups_i {
        merged.push((vec![Value::Integer(k)], accs));
    }
    for (k, accs) in groups_1 {
        merged.push((vec![k], accs));
    }
    let rows_from = merged;
    let mut rows: Vec<Vec<Value>> = rows_from
        .into_iter()
        .map(|(keys, accs)| {
            out_cols
                .iter()
                .map(|c| match c {
                    Out::Key(i) => keys[*i].clone(),
                    Out::Agg(i) => accs[*i].finalize(agg_specs[*i].func, agg_specs[*i].ts),
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
                    let cn_bare = cn.rsplit('.').next().unwrap_or(cn);
                    let hits: Vec<usize> = out_names
                        .iter()
                        .enumerate()
                        .filter(|(_, nm)| {
                            nm.as_str() == cn.as_str()
                                || nm.rsplit('.').next().unwrap_or(nm) == cn_bare
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

// ───────────────────────── M3: 批 hash equi-JOIN ─────────────────────────

/// 类型化 join 键 (与既有 hash_join_inner 的 JoinKey 同归一规则:
/// 小整数与浮点共享 Num 位形跨类型匹配, 大整数保全 64 位)。
#[derive(Hash, PartialEq, Eq, Debug)]
enum JKey {
    Num(u64),
    Int(u64),
    Text(std::sync::Arc<str>),
    Bool(bool),
}

fn col_jkey(cv: &ColumnVector, i: usize) -> Option<JKey> {
    if cv.valid.is_null(i) {
        return None; // NULL 键永不匹配
    }
    const EXACT_MAX: i64 = 1i64 << 53;
    match &cv.data {
        ColData::I64(v) => {
            if v[i] >= -EXACT_MAX && v[i] <= EXACT_MAX {
                Some(JKey::Num((v[i] as f64).to_bits()))
            } else {
                Some(JKey::Int((v[i] as u64).wrapping_add(i64::MIN as u64)))
            }
        }
        ColData::F64(v) => Some(JKey::Num(v[i].to_bits())),
        ColData::Bool(bits) => Some(JKey::Bool((bits[i / 64] >> (i % 64)) & 1 != 0)),
        ColData::Utf8(v) => Some(JKey::Text(std::sync::Arc::clone(&v[i]))),
        ColData::Values(v) => match &v[i] {
            Value::Integer(x) => {
                if *x >= -EXACT_MAX && *x <= EXACT_MAX {
                    Some(JKey::Num((*x as f64).to_bits()))
                } else {
                    Some(JKey::Int((*x as u64).wrapping_add(i64::MIN as u64)))
                }
            }
            Value::Float(f) => Some(JKey::Num(f.to_bits())),
            Value::Text(t) => Some(JKey::Text(std::sync::Arc::clone(&t.0))),
            Value::Bool(b) => Some(JKey::Bool(*b)),
            _ => None,
        },
    }
}

pub struct VecJoinGbOutcome {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

/// 🚀 M3 批 hash equi-JOIN + GROUP BY（半连接聚合形态）:
/// `SELECT <探测侧键/聚合> FROM a JOIN b ON a.k = b.k [WHERE 单表谓词]
/// [GROUP BY 探测侧键]`。
///
/// 小侧 build (HashMap<JKey, 匹配数>)、大侧 probe — 探测行命中后**直接折叠
/// 进组累加器**, 不物化 joined 行 (旧路径每 joined 行一次 Vec<Value> clone —
/// JOIN+GROUP BY @100K 16.7ms 的主成本)。SELECT 引用 build 侧非键列 /
/// HAVING / 非 INNER → decline。
#[allow(clippy::too_many_lines)]
pub fn try_vec_equi_join_gb(
    build: (&Arc<ColSegmentStore>, &TableSchema, &str), // (store, schema, alias)
    probe: (&Arc<ColSegmentStore>, &TableSchema, &str),
    build_key_col: usize,
    probe_key_col: usize,
    stmt: &SelectStmt,
) -> Result<Option<VecJoinGbOutcome>> {
    use std::collections::HashMap;
    if !vec_enabled() || crate::sql::executor::QueryExecutor::is_in_transaction_tls() {
        return Ok(None);
    }
    if stmt.having.is_some() || stmt.distinct || stmt.latest_by.is_some() {
        return Ok(None);
    }
    let (bstore, bschema, balias) = build;
    let (pstore, pschema, palias) = probe;
    let Some(group_items) = stmt.group_by.as_ref() else {
        return Ok(None);
    };
    if group_items.is_empty() || group_items.len() > 2 {
        return Ok(None);
    }

    // ── SELECT 解析: 键 (探测侧 KeyExpr) + 聚合 ──
    enum Out {
        Key(usize),
        /// build 侧组键 (维度表列, 如 GROUP BY s.zone) — 值来自 build 表,
        /// 输出时即组键本身 (单键)。
        BKey,
        Agg(usize),
    }
    let mut key_exprs: Vec<KeyExpr> = Vec::new();
    // build 侧组键列 (schema 位); 仅支持单 build 键、且不能与探测键混用。
    let mut bgroup_col: Option<usize> = None;
    let mut out_names: Vec<String> = Vec::new();
    let mut out_cols: Vec<Out> = Vec::new();
    let mut agg_specs: Vec<VecAggSpec> = Vec::new();
    for sc in &stmt.columns {
        match sc {
            SelectColumn::Star => return Ok(None),
            SelectColumn::Column(c) | SelectColumn::ColumnWithAlias(c, _) => {
                // 🔑 探测侧普通列作组键 (bench 形状: SELECT e.device, COUNT(*)) —
                // 剥探测别名后在探测 schema 解析; 必须匹配某 GROUP BY 项。
                let bare = c.rsplit('.').next().unwrap_or(c);
                let matched = group_items
                    .iter()
                    .any(|g| g.rsplit('.').next().unwrap_or(g) == bare || g == c);
                if !matched || key_exprs.len() + 1 > group_items.len() {
                    return Ok(None);
                }
                // 🔑 带表前缀且非探测侧别名时禁止回退探测 bare 名 —
                // `departments.name` 曾错解析到探测表 employees.name
                // (员工名当了组键, test_join_aggregate 多出组)。
                let probe_res = resolve_col_alias(c, pschema, Some(palias)).or_else(|| {
                    if c.contains('.') {
                        None
                    } else {
                        pschema.get_column_position(bare)
                    }
                });
                match probe_res {
                    Some(pos) => {
                        out_names.push(bare.to_string());
                        out_cols.push(Out::Key(key_exprs.len()));
                        key_exprs.push(KeyExpr::Col(pos));
                    }
                    None => {
                        // 🔑 build 侧组键 (维度表列): GROUP BY s.zone 形状。
                        // 解析到 build schema; 仅单键且无探测键混用时支持
                        // (混用键需要物化 joined 行, 超出半连接折叠模型)。
                        let Some(bpos) = resolve_col_alias(c, bschema, Some(balias))
                            .or_else(|| bschema.get_column_position(bare))
                        else {
                            return Ok(None);
                        };
                        if bgroup_col.is_some() || !key_exprs.is_empty() {
                            return Ok(None);
                        }
                        bgroup_col = Some(bpos);
                        out_names.push(bare.to_string());
                        out_cols.push(Out::BKey);
                    }
                }
            }
            SelectColumn::Expr(expr, alias) => {
                if let Some((func, arg)) = parse_simple_agg(expr) {
                    // 聚合参数必须是探测侧列
                    let col = match arg {
                        Some(Expr::Column(c)) => {
                            let bare = c.rsplit('.').next().unwrap_or(c);
                            match resolve_col_alias(c, pschema, Some(palias)).or_else(|| {
                                // 🔑 带前缀且非探测侧 → build 列, 本路径不支
                                // 持 build 侧聚合 → decline (不得错读探测表
                                // 同名列)。
                                if c.contains('.') {
                                    None
                                } else {
                                    pschema.get_column_position(bare)
                                }
                            }) {
                                Some(p) => Some(p),
                                None => return Ok(None),
                            }
                        }
                        Some(_) => return Ok(None),
                        None => None,
                    };
                    out_names.push(alias.clone().unwrap_or_else(|| {
                        crate::sql::executor::QueryExecutor::expr_to_column_name(expr)
                    }));
                    out_cols.push(Out::Agg(agg_specs.len()));
                    agg_specs.push(VecAggSpec {
                        func,
                        col: col.filter(|_| !matches!(func, VecAggFunc::CountStar)),
                        ts: col.map_or(false, |c| {
                            matches!(pschema.col_types().get(c), Some(ColumnType::Timestamp))
                        }),
                    });
                } else {
                    let name = alias.clone().unwrap_or_else(|| {
                        crate::sql::executor::QueryExecutor::expr_to_column_name(expr)
                    });
                    let canonical = crate::sql::executor::QueryExecutor::expr_to_column_name(expr);
                    let matched = group_items.iter().any(|g| g == &name || g == &canonical);
                    if !matched || key_exprs.len() + 1 > group_items.len() {
                        return Ok(None);
                    }
                    // KeyExpr 按**探测侧 schema 位置**编译 (剥探测别名)
                    let stripped = strip_alias(expr, palias);
                    let Some(ke) = KeyExpr::compile(&stripped, pschema) else {
                        return Ok(None);
                    };
                    out_names.push(name);
                    out_cols.push(Out::Key(key_exprs.len()));
                    key_exprs.push(ke);
                }
            }
        }
    }
    let n_keys = key_exprs.len() + usize::from(bgroup_col.is_some());
    if n_keys == 0 || agg_specs.is_empty() || n_keys != group_items.len() {
        return Ok(None);
    }

    // ── WHERE 按表侧拆分 (AND 链叶全部可按别名归类) ──
    let mut bpred = None;
    let mut ppred = None;
    if let Some(w) = &stmt.where_clause {
        match split_where_by_alias(w, balias, palias) {
            Some((be, pe)) => {
                if let Some(e) = be {
                    bpred = VecPred::compile_with_alias(&e, bschema, Some(balias));
                }
                if let Some(e) = pe {
                    ppred = VecPred::compile_with_alias(&e, pschema, Some(palias));
                }
            }
            None => { return Ok(None) }
        }
    }

    // ── 需要的列 ──
    let bcts = bschema.col_types();
    let pcts = pschema.col_types();
    let mut bneeded: Vec<usize> = vec![build_key_col];
    if let Some(p) = &bpred {
        collect_pred_cols(p, &mut bneeded);
    }
    if let Some(gc) = bgroup_col {
        bneeded.push(gc);
    }
    bneeded.sort_unstable();
    bneeded.dedup();
    let mut pneeded: Vec<usize> = vec![probe_key_col];
    if let Some(p) = &ppred {
        collect_pred_cols(p, &mut pneeded);
    }
    for ke in &key_exprs {
        ke.collect_cols(&mut pneeded);
    }
    for sp in &agg_specs {
        if let Some(c) = sp.col {
            pneeded.push(c);
        }
    }
    pneeded.sort_unstable();
    pneeded.dedup();
    if bneeded
        .iter()
        .chain(pneeded.iter())
        .any(|&c| matches!(bcts.get(c), Some(ColumnType::Tensor(_) | ColumnType::Spatial)))
        || pneeded
            .iter()
            .any(|&c| matches!(pcts.get(c), Some(ColumnType::Tensor(_) | ColumnType::Spatial)))
    {
        return Ok(None);
    }
    let mut bpred = bpred;
    if let Some(p) = bpred.as_mut() {
        remap_pred(p, &bneeded);
    }
    let mut ppred = ppred;
    if let Some(p) = ppred.as_mut() {
        remap_pred(p, &pneeded);
    }
    for sp in agg_specs.iter_mut() {
        if let Some(c) = sp.col.as_mut() {
            if let Some(i) = pneeded.iter().position(|&x| x == *c) {
                *c = i;
            }
        }
    }
    let key_remap = |c: usize| -> usize {
        pneeded.iter().position(|&x| x == c).unwrap_or(0)
    };

    // 段门 (与 M1/M2 相同): 墓碑/多段 decline
    let _ = bstore.flush_buffer();
    let bsegs = bstore.segments_snapshot();
    if bsegs.iter().any(|s| s.has_any_deleted()) || bsegs.len() > 1 {
        return Ok(None);
    }
    let _ = pstore.flush_buffer();
    let psegs = pstore.segments_snapshot();
    if psegs.iter().any(|s| s.has_any_deleted()) || psegs.len() > 1 {
        return Ok(None);
    }

    // ── build 侧: 扫描 + 过滤 + HashMap<JKey, (匹配数, 组值)> ──
    // 组值: build 侧组键列的值 (维度表属性)。同 join 键的多行必须同组值,
    // 否则半连接折叠无法把 probe 行归到唯一组 → decline (PK 维度表不会触发)。
    let mut table: HashMap<JKey, (u64, Option<Value>)> = HashMap::new();
    let bg_idx: Option<usize> = bgroup_col
        .and_then(|gc| bneeded.iter().position(|&x| x == gc));
    for seg in &bsegs {
        let n = seg.row_count;
        if n == 0 {
            continue;
        }
        let mut cols: Vec<std::sync::Arc<ColumnVector>> = Vec::with_capacity(bneeded.len());
        for &c in &bneeded {
            let Some(cv) = seg.read_column_batch(c, &bcts[c]) else {
                return Ok(None);
            };
            cols.push(cv);
        }
        let batch = ColumnBatch::new_shared(cols);
        let bkey_idx = bneeded.iter().position(|&x| x == build_key_col).unwrap_or(0);
        let rows: Vec<u32> = match &bpred {
            Some(p) => {
                let mut chain: Vec<&VecPredLeaf> = Vec::new();
                if p.as_and_chain(&mut chain) {
                    let mut sel = Vec::with_capacity(n);
                    for i in 0..n {
                        if chain.iter().all(|l| leaf_tv_leaf(&batch.cols, l, i) == TV::True) {
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
            if let Some(k) = col_jkey(&batch.cols[bkey_idx], i) {
                let gval: Option<Value> = match bg_idx {
                    Some(gi) => Some(batch.cols[gi].get(i)),
                    None => None,
                };
                let e = table.entry(k).or_insert((0, gval.clone()));
                e.0 += 1;
                if e.1 != gval {
                    // 同 join 键跨组值 — 折叠模型不成立
                    return Ok(None);
                }
            }
        }
    }

    // ── probe 侧: 扫描 + 过滤 + 命中直接折叠 ──
    let pkey_idx = pneeded.iter().position(|&x| x == probe_key_col).unwrap_or(0);
    // 🔑 组键==join 键列 → JKey 组 (每行一次 hash); 单键 → Value 组;
    // 多键 → Vec<Value> 组。
    let group_is_join_key = key_exprs.len() == 1
        && matches!(&key_exprs[0], KeyExpr::Col(c) if key_remap(*c) == pkey_idx);
    let mut groups_j: HashMap<JKey, (Option<Value>, Vec<VecAcc>)> = HashMap::new();
    let mut groups_1: std::collections::HashMap<Value, Vec<VecAcc>> = std::collections::HashMap::new();
    let mut groups_v: HashMap<Vec<Value>, Vec<VecAcc>> = HashMap::new();
    let single_key = key_exprs.len() == 1;
    for seg in &psegs {
        let n = seg.row_count;
        if n == 0 {
            continue;
        }
        let mut cols: Vec<std::sync::Arc<ColumnVector>> = Vec::with_capacity(pneeded.len());
        for &c in &pneeded {
            let Some(cv) = seg.read_column_batch(c, &pcts[c]) else {
                return Ok(None);
            };
            cols.push(cv);
        }
        let batch = ColumnBatch::new_shared(cols);
        let rows: Vec<u32> = match &ppred {
            Some(p) => {
                let mut chain: Vec<&VecPredLeaf> = Vec::new();
                if p.as_and_chain(&mut chain) {
                    let mut sel = Vec::with_capacity(n);
                    for i in 0..n {
                        if chain.iter().all(|l| leaf_tv_leaf(&batch.cols, l, i) == TV::True) {
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
        #[cfg(feature = "rayon")]
        if rows.len() >= PARALLEL_MORSEL_MIN_ROWS {
            use rayon::prelude::*;
            // 🔑 M5 morsel 并行: probe 行按 chunk 分线程, 各自 partial 组表,
            // 主线程合并。build 表 (table) 共享只读; 列批 Arc 零拷贝。
            let nchunks = par_chunk_count(rows.len());
            let chunk_len = rows.len().div_ceil(nchunks).max(1);
            let partials: Vec<(
                HashMap<JKey, (Option<Value>, Vec<VecAcc>)>,
                std::collections::HashMap<Value, Vec<VecAcc>>,
                HashMap<Vec<Value>, Vec<VecAcc>>,
                bool,
            )> = rows
                .par_chunks(chunk_len)
                .map(|chunk| {
                    let mut gj: HashMap<JKey, (Option<Value>, Vec<VecAcc>)> = HashMap::new();
                    let mut g1: std::collections::HashMap<Value, Vec<VecAcc>> =
                        std::collections::HashMap::new();
                    let mut gv: HashMap<Vec<Value>, Vec<VecAcc>> = HashMap::new();
                    let mut ok = true;
                    for &r in chunk {
                        let i = r as usize;
                        let k = col_jkey(&batch.cols[pkey_idx], i);
                        let (matches, bgrp) = match &k {
                            Some(k) => match table.get(k) {
                                Some((m, g)) => (*m, g.clone()),
                                None => (0, None),
                            },
                            None => (0, None),
                        };
                        if matches == 0 {
                            continue;
                        }
                        if bgroup_col.is_some() {
                            let gval = bgrp.unwrap_or(Value::Null);
                            let accs = g1
                                .entry(gval)
                                .or_insert_with(|| {
                                    agg_specs.iter().map(|_| VecAcc::default()).collect()
                                });
                            for _ in 0..matches {
                                fold_row(&mut accs[..], &agg_specs, &batch.cols, i);
                            }
                            continue;
                        }
                        if group_is_join_key {
                            let e = gj.entry(k.unwrap()).or_insert_with(|| {
                                (
                                    key_exprs[0].eval(&batch, i, &key_remap),
                                    agg_specs.iter().map(|_| VecAcc::default()).collect::<Vec<_>>(),
                                )
                            });
                            for _ in 0..matches {
                                fold_row(&mut (e.1)[..], &agg_specs, &batch.cols, i);
                            }
                            continue;
                        }
                        if single_key {
                            if let Some(kv) = key_exprs[0].eval_i64(&batch, i, &key_remap) {
                                let accs = g1.entry(Value::Integer(kv)).or_insert_with(|| {
                                    agg_specs.iter().map(|_| VecAcc::default()).collect()
                                });
                                for _ in 0..matches {
                                    fold_row(&mut accs[..], &agg_specs, &batch.cols, i);
                                }
                                continue;
                            }
                            if let Some(kv) = key_exprs[0].eval(&batch, i, &key_remap) {
                                let accs = g1.entry(kv).or_insert_with(|| {
                                    agg_specs.iter().map(|_| VecAcc::default()).collect()
                                });
                                for _ in 0..matches {
                                    fold_row(&mut accs[..], &agg_specs, &batch.cols, i);
                                }
                                continue;
                            }
                            ok = false;
                            break;
                        }
                        let mut key = Vec::with_capacity(key_exprs.len());
                        let mut key_ok = true;
                        for ke in &key_exprs {
                            match ke.eval(&batch, i, &key_remap) {
                                Some(v) => key.push(v),
                                None => {
                                    key_ok = false;
                                    break;
                                }
                            }
                        }
                        if !key_ok {
                            ok = false;
                            break;
                        }
                        let accs = gv.entry(key).or_insert_with(|| {
                            agg_specs.iter().map(|_| VecAcc::default()).collect()
                        });
                        for _ in 0..matches {
                            fold_row(&mut accs[..], &agg_specs, &batch.cols, i);
                        }
                    }
                    (gj, g1, gv, ok)
                })
                .collect();
            if partials.iter().any(|(_, _, _, ok)| !ok) {
                return Ok(None);
            }
            for (gj, g1, gv, _) in partials {
                for (k, (disp, src)) in gj {
                    let e = groups_j
                        .entry(k)
                        .or_insert_with(|| (disp, vec![VecAcc::default(); src.len()]));
                    for (d, s) in (e.1).iter_mut().zip(src.iter()) {
                        d.merge(s);
                    }
                }
                for (k, src) in g1 {
                    let e = groups_1
                        .entry(k)
                        .or_insert_with(|| vec![VecAcc::default(); src.len()]);
                    for (d, s) in e.iter_mut().zip(src.iter()) {
                        d.merge(s);
                    }
                }
                for (k, src) in gv {
                    let e = groups_v
                        .entry(k)
                        .or_insert_with(|| vec![VecAcc::default(); src.len()]);
                    for (d, s) in e.iter_mut().zip(src.iter()) {
                        d.merge(s);
                    }
                }
            }
            continue;
        }
        for r in rows {
            let i = r as usize;
            let k = col_jkey(&batch.cols[pkey_idx], i);
            let (matches, bgrp) = match &k {
                Some(k) => match table.get(k) {
                    Some((m, g)) => (*m, g.clone()),
                    None => (0, None),
                },
                None => (0, None),
            };
            if matches == 0 {
                continue;
            }
            // 🔑 build 侧组键: 组 = 命中 build 行的组值 (单键 Value 组)。
            // build 行组值为 NULL → 归入 NULL 组 (SQL GROUP BY 语义, 不丢弃)。
            if bgroup_col.is_some() {
                let gv = bgrp.unwrap_or(Value::Null);
                let accs = groups_1
                    .entry(gv)
                    .or_insert_with(|| agg_specs.iter().map(|_| VecAcc::default()).collect());
                for _ in 0..matches {
                    fold_row(&mut accs[..], &agg_specs, &batch.cols, i);
                }
                continue;
            }
            // 🔑 组键 == join 键列 (bench 形状: SELECT e.device … ON e.device):
            // 直接以 JKey 为组标识 — 每行只做这一次字符串 hash
            // (分别对 join 键和组键各 hash 一次曾比旧路径还慢)。
            if group_is_join_key {
                let e = groups_j
                    .entry(k.unwrap())
                    .or_insert_with(|| (key_exprs[0].eval(&batch, i, &key_remap), agg_specs.iter().map(|_| VecAcc::default()).collect::<Vec<_>>()));
                for _ in 0..matches {
                    fold_row(&mut (e.1)[..], &agg_specs, &batch.cols, i);
                }
                continue;
            }
            if single_key {
                if let Some(kv) = key_exprs[0].eval_i64(&batch, i, &key_remap) {
                    let accs = groups_1
                        .entry(Value::Integer(kv))
                        .or_insert_with(|| agg_specs.iter().map(|_| VecAcc::default()).collect());
                    for _ in 0..matches {
                        fold_row(&mut accs[..], &agg_specs, &batch.cols, i);
                    }
                    continue;
                }
                if let Some(kv) = key_exprs[0].eval(&batch, i, &key_remap) {
                    let accs = groups_1
                        .entry(kv)
                        .or_insert_with(|| agg_specs.iter().map(|_| VecAcc::default()).collect());
                    for _ in 0..matches {
                        fold_row(&mut accs[..], &agg_specs, &batch.cols, i);
                    }
                    continue;
                }
                return Ok(None);
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
            for _ in 0..matches {
                fold_row(&mut accs[..], &agg_specs, &batch.cols, i);
            }
        }
    }

    // ── 输出 (与 M2 相同: 双表合并 + ORDER BY + LIMIT) ──
    let mut merged: Vec<(Vec<Value>, Vec<VecAcc>)> = groups_v.into_iter().collect();
    for (k, accs) in groups_1 {
        merged.push((vec![k], accs));
    }
    for (_k, (disp, accs)) in groups_j {
        if let Some(d) = disp {
            merged.push((vec![d], accs));
        }
    }
    let mut rows: Vec<Vec<Value>> = merged
        .into_iter()
        .map(|(keys, accs)| {
            out_cols
                .iter()
                .map(|c| match c {
                    Out::Key(i) => keys[*i].clone(),
                    // build 侧组键: 单键 — 组键即 merged key 本身
                    Out::BKey => keys[0].clone(),
                    Out::Agg(i) => accs[*i].finalize(agg_specs[*i].func, agg_specs[*i].ts),
                })
                .collect::<Vec<_>>()
        })
        .collect();
    if let Some(ref ob) = stmt.order_by {
        let mut specs: Vec<(usize, bool)> = Vec::new();
        for oe in ob {
            let hit = match &oe.expr {
                Expr::Literal(Value::Integer(nx)) if *nx >= 1 => {
                    Some((*nx as usize).wrapping_sub(1)).filter(|&p| p < out_names.len())
                }
                Expr::Column(cn) => {
                    let cn_bare = cn.rsplit('.').next().unwrap_or(cn);
                    let hits: Vec<usize> = out_names
                        .iter()
                        .enumerate()
                        .filter(|(_, nm)| {
                            nm.as_str() == cn.as_str()
                                || nm.rsplit('.').next().unwrap_or(nm) == cn_bare
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
    Ok(Some(VecJoinGbOutcome {
        columns: out_names,
        rows,
    }))
}

/// WHERE 按表侧拆分: AND 链叶按列前缀 (alias) 归类到两侧; 叶引用裸名
/// (无前缀) 或跨侧 → None (整体 decline — 保守)。
fn split_where_by_alias(
    e: &Expr,
    balias: &str,
    palias: &str,
) -> Option<(Option<Expr>, Option<Expr>)> {
    fn leaves(e: &Expr, out: &mut Vec<Expr>) {
        if let Expr::BinaryOp {
            left,
            op: crate::sql::ast::BinaryOperator::And,
            right,
        } = e
        {
            leaves(left, out);
            leaves(right, out);
        } else {
            out.push(e.clone());
        }
    }
    let mut ls: Vec<Expr> = Vec::new();
    leaves(e, &mut ls);
    let mut bside: Vec<Expr> = Vec::new();
    let mut pside: Vec<Expr> = Vec::new();
    for leaf in ls {
        let mut cols: Vec<String> = Vec::new();
        if !crate::sql::executor::QueryExecutor::collect_column_names_strict(&leaf, &mut cols) {
            return None;
        }
        if cols.is_empty() {
            return None; // 纯常量叶 — 保守 decline
        }
        let all_b = cols.iter().all(|c| {
            c.split_once('.').map(|(p, _)| p == balias).unwrap_or(false)
        });
        let all_p = cols.iter().all(|c| {
            c.split_once('.').map(|(p, _)| p == palias).unwrap_or(false)
        });
        if all_b {
            bside.push(leaf);
        } else if all_p {
            pside.push(leaf);
        } else {
            return None; // 裸名/跨侧
        }
    }
    let join = |mut v: Vec<Expr>| -> Option<Expr> {
        if v.is_empty() {
            return None;
        }
        while v.len() > 1 {
            let r = v.pop().unwrap();
            let l = v.pop().unwrap();
            v.push(Expr::BinaryOp {
                left: Box::new(l),
                op: crate::sql::ast::BinaryOperator::And,
                right: Box::new(r),
            });
        }
        Some(v.pop().unwrap())
    };
    Some((join(bside), join(pside)))
}

/// 剥表达式中的 `<alias>.` 前缀 (KeyExpr 在探测侧 schema 上编译用)。
fn strip_alias(e: &Expr, alias: &str) -> Expr {
    match e {
        Expr::Column(c) => {
            if let Some((p, bare)) = c.split_once('.') {
                if p == alias {
                    return Expr::Column(bare.to_string());
                }
            }
            e.clone()
        }
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(strip_alias(left, alias)),
            op: op.clone(),
            right: Box::new(strip_alias(right, alias)),
        },
        Expr::UnaryOp { op, expr } => Expr::UnaryOp {
            op: op.clone(),
            expr: Box::new(strip_alias(expr, alias)),
        },
        _ => e.clone(),
    }
}


// ═══════════════════════════════════════════════════════════════════════
// M4a: 批投影 — SELECT 纯列 [WHERE] [LIMIT/OFFSET] 的批输出边界
// ═══════════════════════════════════════════════════════════════════════

/// 批投影：列批直读（段缓存复用）→ 可见性/谓词 selection → 边界一次行拼装，
/// LIMIT/OFFSET 在拼装前生效（budget = offset+limit，跨段提前终止）。
/// 返回 None → 旧 scan_projected_filtered 路径原样回退。
///
/// 🔑 行序精确复刻 `scan_projected_filtered`（分页依赖顺序一致）：
/// 段 new→old（`.rev()`）；段内 need_dedup（多段 ∨ 可能重复键）时 index
/// 降序 + newest-wins 去重（seen 先于墓碑检查 — 墓碑压制旧版本），
/// 否则升序 0..n。单段无 dedup 时跳过 load_full_keys（旧路径每查询必做）。
pub fn try_vec_projection(
    store: &ColSegmentStore,
    schema: &TableSchema,
    select_cols: &[SelectColumn],
    where_clause: Option<&Expr>,
    limit: Option<usize>,
    offset: usize,
) -> Result<Option<Vec<Vec<Value>>>> {
    if !vec_enabled() {
        return Ok(None);
    }
    // 🔑 事务 read-your-writes：段批看不到 write_set 未提交行 → decline。
    if crate::sql::executor::QueryExecutor::is_in_transaction_tls() {
        return Ok(None);
    }
    let cts = schema.col_types();

    let mut pred = match where_clause {
        Some(w) => match VecPred::compile(w, schema) {
            Some(p) => Some(p),
            None => return Ok(None),
        },
        None => None,
    };

    // 投影解析：仅纯列（含限定名 `t.col`）；单 Star → 全列；表达式列 decline。
    let mut proj: Vec<usize> = Vec::new();
    for sc in select_cols {
        match sc {
            SelectColumn::Star => proj.extend(0..schema.columns.len()),
            SelectColumn::Column(n) | SelectColumn::ColumnWithAlias(n, _) => {
                let bare = if n.contains('.') {
                    n.rsplit('.').next().unwrap_or(n)
                } else {
                    n
                };
                match schema.get_column_position(bare) {
                    Some(p) => proj.push(p),
                    None => return Ok(None),
                }
            }
            SelectColumn::Expr(_, _) => return Ok(None),
        }
    }
    if proj.is_empty() {
        return Ok(None);
    }

    // needed = 谓词列 ∪ 投影列（批只装载这些）；Tensor/Spatial decline。
    let mut needed: Vec<usize> = Vec::new();
    if let Some(p) = &pred {
        collect_pred_cols(p, &mut needed);
    }
    needed.extend_from_slice(&proj);
    needed.sort_unstable();
    needed.dedup();
    if needed
        .iter()
        .any(|&c| matches!(cts.get(c), Some(ColumnType::Tensor(_) | ColumnType::Spatial)))
    {
        return Ok(None);
    }
    // 谓词叶列位 → 批内相对位（M1 教训：按 schema 位索引批列会越界）。
    if let Some(p) = pred.as_mut() {
        remap_pred(p, &needed);
    }
    // 投影列的批内位 + Timestamp 语义标记（I64 批需包装回 Value::Timestamp）。
    let proj_meta: Vec<(usize, bool)> = proj
        .iter()
        .map(|&p| {
            let bi = needed.iter().position(|&x| x == p).expect("proj in needed");
            (bi, matches!(cts.get(p), Some(ColumnType::Timestamp)))
        })
        .collect();

    let limit_v = limit.unwrap_or(usize::MAX);
    let budget = offset.saturating_add(limit_v);
    let mut rows_out: Vec<Vec<Value>> = Vec::new();

    let _ = store.flush_buffer();
    let segments = store.segments_snapshot();
    // 🔑 与 M1 相同的保守门：任一段含墓碑 → decline（事务回滚 undo 双写
    // 可能使段发散；DELETE 场景走旧路径）。
    if segments.iter().any(|s| s.has_any_deleted()) {
        return Ok(None);
    }
    // 🔑 同 M1: 纯插入多段 (无 overlap) 不去重 — key 唯一。
    let need_dedup = store.may_have_duplicate_keys();
    let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();

    'outer: for seg in segments.iter().rev() {
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

        // 可见性 selection（行序语义见函数注释）。
        let vis: Vec<u32> = if !need_dedup {
            (0..n)
                .filter(|&i| !seg.is_row_deleted(i))
                .map(|i| i as u32)
                .collect()
        } else {
            let keys = seg.keys();
            let mut v: Vec<u32> = Vec::with_capacity(n);
            for i in (0..n).rev() {
                if !seen.insert(keys[i]) {
                    continue;
                }
                if seg.is_row_deleted(i) {
                    continue;
                }
                v.push(i as u32);
            }
            v
        };
        // 谓词过滤（AND 链单遍短路；混合形状 selection∩vis）。
        let filtered: Vec<u32> = match &pred {
            None => vis,
            Some(p) => {
                let mut chain: Vec<&VecPredLeaf> = Vec::new();
                if p.as_and_chain(&mut chain) {
                    vis.into_iter()
                        .filter(|&r| {
                            chain
                                .iter()
                                .all(|l| leaf_tv_leaf(&batch.cols, l, r as usize) == TV::True)
                        })
                        .collect()
                } else {
                    let set: std::collections::HashSet<u32> =
                        p.eval_sel(&batch).iter().collect();
                    vis.into_iter().filter(|r| set.contains(r)).collect()
                }
            }
        };
        // 边界行拼装（budget 内）。
        for r in filtered {
            if rows_out.len() >= budget {
                break 'outer;
            }
            let i = r as usize;
            let row: Vec<Value> = proj_meta
                .iter()
                .map(|&(bi, is_ts)| {
                    let cv = &batch.cols[bi];
                    if is_ts {
                        crate::storage::colbatch::i64_vec_as_timestamp(cv, i)
                    } else {
                        cv.get(i)
                    }
                })
                .collect();
            rows_out.push(row);
        }
    }
    // LIMIT/OFFSET：拼装后的最后一跳（budget 已限总量）。
    if offset > 0 {
        rows_out.drain(..offset.min(rows_out.len()));
    }
    if rows_out.len() > limit_v {
        rows_out.truncate(limit_v);
    }
    Ok(Some(rows_out))
}

// ═══════════════════════════════════════════════════════════════════════
// M4b: 过滤 top-k — WHERE + ORDER BY 单数值键 + LIMIT/OFFSET
// ═══════════════════════════════════════════════════════════════════════

/// f64 → 全序 u64 键（NaN 安全；与 top_k_row_indices_typed 同编码）。
#[inline]
fn f64_ord_key(v: f64) -> u64 {
    let bits = v.to_bits();
    if bits & (1u64 << 63) != 0 {
        !bits
    } else {
        bits ^ (1u64 << 63)
    }
}

/// 批过滤 top-k：谓词批求值 → 命中行的排序键 typed 提取 → select_nth
/// 取前 k → 只对 K 行做边界行拼装（ORDER BY + LIMIT 的 dashboard 形状：
/// WHERE ts >= ? ORDER BY ts LIMIT 100 — 旧路径解码全部命中行的全部投影列
/// 再全排序）。k = offset+limit 支持深分页，拼装时跳过 offset。
///
/// 语义对齐 `top_k_row_indices_typed`：NULL 排最前(ASC)/最后(DESC)；
/// DESC 用 `u64::MAX - key` 翻转；i64 用符号位翻转直接比。

/// top-k 有序键: 数值列折算 u64 保序键 (NULL 排最前 ASC/最后 DESC, 同引擎级
/// 语义; Bool 按布尔序折 i64 键)。M4 串行/并行 chunk 共用。
#[inline]
fn topk_ord_key(kc: &ColumnVector, i: usize, desc: bool) -> u64 {
    match &kc.data {
        crate::storage::colbatch::ColData::F64(vs) => {
            match (kc.valid.is_valid(i)).then(|| vs[i]) {
                Some(v) => {
                    if desc {
                        u64::MAX - f64_ord_key(v)
                    } else {
                        f64_ord_key(v)
                    }
                }
                None => {
                    if desc {
                        u64::MAX
                    } else {
                        u64::MIN
                    }
                }
            }
        }
        crate::storage::colbatch::ColData::I64(vs) => {
            match (kc.valid.is_valid(i)).then(|| vs[i]) {
                Some(v) => {
                    if desc {
                        !(v as u64 ^ (1u64 << 63))
                    } else {
                        v as u64 ^ (1u64 << 63)
                    }
                }
                None => {
                    if desc {
                        u64::MAX
                    } else {
                        u64::MIN
                    }
                }
            }
        }
        // Bool 位图列按布尔序（false<true）折算 i64 键。
        crate::storage::colbatch::ColData::Bool(_) => {
            let v = match kc.get(i) {
                Value::Bool(x) => x as i64,
                _ => 0,
            };
            if desc {
                !(v as u64 ^ (1u64 << 63))
            } else {
                v as u64 ^ (1u64 << 63)
            }
        }
        _ => {
            if desc {
                u64::MAX
            } else {
                u64::MIN
            }
        }
    }
}

pub fn try_vec_filter_topk(
    store: &ColSegmentStore,
    schema: &TableSchema,
    stmt: &SelectStmt,
) -> Result<Option<Vec<Vec<Value>>>> {
    if !vec_enabled() {
        return Ok(None);
    }
    if crate::sql::executor::QueryExecutor::is_in_transaction_tls() {
        return Ok(None);
    }
    if stmt.distinct || stmt.group_by.is_some() || stmt.having.is_some() || stmt.latest_by.is_some()
    {
        return Ok(None);
    }
    let Some(w) = &stmt.where_clause else {
        return Ok(None); // 无 WHERE 走既有 top_k_row_indices_typed 快路径
    };
    // 单一 ORDER BY 键、纯列（含限定名）、数值/Timestamp。
    let Some(ob) = stmt.order_by.as_ref() else {
        return Ok(None);
    };
    if ob.len() != 1 {
        return Ok(None);
    }
    let obe = &ob[0];
    let Expr::Column(cn) = &obe.expr else {
        return Ok(None);
    };
    let bare = cn.rsplit('.').next().unwrap_or(cn);
    let Some(order_col) = schema.get_column_position(bare) else {
        return Ok(None);
    };
    let desc = !obe.asc;
    let cts = schema.col_types();
    let key_float = matches!(cts.get(order_col), Some(ColumnType::Float));
    let key_ok = matches!(
        cts.get(order_col),
        Some(
            ColumnType::Integer
                | ColumnType::Float
                | ColumnType::Boolean
                | ColumnType::Timestamp
        )
    );
    if !key_ok {
        return Ok(None);
    }
    let Some(page) = stmt.limit else {
        return Ok(None);
    };
    let offset = stmt.offset.unwrap_or(0);
    if page == 0 {
        return Ok(Some(Vec::new()));
    }
    let k = page.saturating_add(offset);
    if k > 1_000_000 {
        return Ok(None);
    }

    let Some(mut pred) = VecPred::compile(w, schema) else {
        return Ok(None);
    };

    // 投影解析（同 try_vec_projection）。
    let mut proj: Vec<usize> = Vec::new();
    for sc in &stmt.columns {
        match sc {
            SelectColumn::Star => proj.extend(0..schema.columns.len()),
            SelectColumn::Column(n) | SelectColumn::ColumnWithAlias(n, _) => {
                let bare = if n.contains('.') {
                    n.rsplit('.').next().unwrap_or(n)
                } else {
                    n
                };
                let Some(p) = schema.get_column_position(bare) else {
                    return Ok(None);
                };
                proj.push(p);
            }
            SelectColumn::Expr(_, _) => return Ok(None),
        }
    }
    if proj.is_empty() {
        return Ok(None);
    }

    let mut needed: Vec<usize> = Vec::new();
    collect_pred_cols(&pred, &mut needed);
    needed.push(order_col);
    needed.extend_from_slice(&proj);
    needed.sort_unstable();
    needed.dedup();
    if needed
        .iter()
        .any(|&c| matches!(cts.get(c), Some(ColumnType::Tensor(_) | ColumnType::Spatial)))
    {
        return Ok(None);
    }
    remap_pred(&mut pred, &needed);
    let Some(order_bi) = needed.iter().position(|&x| x == order_col) else {
        return Ok(None);
    };
    let mut proj_meta: Vec<(usize, bool)> = Vec::with_capacity(proj.len());
    for &p in &proj {
        let Some(bi) = needed.iter().position(|&x| x == p) else {
            return Ok(None);
        };
        proj_meta.push((bi, matches!(cts.get(p), Some(ColumnType::Timestamp))));
    }

    let _ = store.flush_buffer();
    let segments = store.segments_snapshot();
    if segments.iter().any(|s| s.has_any_deleted()) {
        return Ok(None);
    }
    // 🔑 同 M1: 纯插入多段 (无 overlap) 不去重 — key 唯一。
    let need_dedup = store.may_have_duplicate_keys();
    let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();

    // (ord_key, seg_idx, row) — 全命中行入表，select_nth 取前 k。
    let mut entries: Vec<(u64, u32, u32)> = Vec::new();
    for (sidx, seg) in segments.iter().enumerate() {
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
        let vis: Vec<u32> = if !need_dedup {
            (0..n)
                .filter(|&i| !seg.is_row_deleted(i))
                .map(|i| i as u32)
                .collect()
        } else {
            let keys = seg.keys();
            let mut v: Vec<u32> = Vec::with_capacity(n);
            for i in (0..n).rev() {
                if !seen.insert(keys[i]) {
                    continue;
                }
                if seg.is_row_deleted(i) {
                    continue;
                }
                v.push(i as u32);
            }
            v
        };
        // 🔑 M4 morsel 并行 (大段): 谓词过滤 + 有序键提取按 chunk 分 rayon
        // 线程, partial entries 主线程拼接; select_nth/物化仍顺序 (k 有界)。
        // 去重/可见集已顺序算好; 谓词是纯字面量 (雷#1/#2/#3 不触碰)。
        #[cfg(feature = "rayon")]
        if vis.len() >= PARALLEL_MORSEL_MIN_ROWS {
            use rayon::prelude::*;
            let mut chain: Vec<&VecPredLeaf> = Vec::new();
            let is_chain = pred.as_and_chain(&mut chain);
            let mixed_set: Option<std::collections::HashSet<u32>> = if !is_chain {
                Some(pred.eval_sel(&batch).iter().collect())
            } else {
                None
            };
            let kc = &batch.cols[order_bi];
            let nchunks = par_chunk_count(vis.len());
            let chunk_len = vis.len().div_ceil(nchunks).max(1);
            let parts: Vec<Vec<(u64, u32, u32)>> = vis
                .par_chunks(chunk_len)
                .map(|chunk| {
                    let mut out: Vec<(u64, u32, u32)> = Vec::new();
                    for &r in chunk {
                        let pass = if is_chain {
                            chain
                                .iter()
                                .all(|l| leaf_tv_leaf(&batch.cols, l, r as usize) == TV::True)
                        } else {
                            mixed_set.as_ref().map_or(false, |s| s.contains(&r))
                        };
                        if !pass {
                            continue;
                        }
                        out.push((topk_ord_key(kc, r as usize, desc), sidx as u32, r));
                    }
                    out
                })
                .collect();
            let total: usize = parts.iter().map(|p| p.len()).sum();
            entries.reserve(total);
            for p in parts {
                entries.extend(p);
            }
            continue;
        }
        let filtered: Vec<u32> = {
            let mut chain: Vec<&VecPredLeaf> = Vec::new();
            if pred.as_and_chain(&mut chain) {
                vis.into_iter()
                    .filter(|&r| {
                        chain
                            .iter()
                            .all(|l| leaf_tv_leaf(&batch.cols, l, r as usize) == TV::True)
                    })
                    .collect()
            } else {
                let set: std::collections::HashSet<u32> = pred.eval_sel(&batch).iter().collect();
                vis.into_iter().filter(|r| set.contains(r)).collect()
            }
        };
        let kc = &batch.cols[order_bi];
        for r in filtered {
            let i = r as usize;
            let ord_key: u64 = topk_ord_key(kc, i, desc);
            entries.push((ord_key, sidx as u32, r));
        }
    }
    if entries.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let k_actual = k.min(entries.len());
    if k_actual < entries.len() {
        entries.select_nth_unstable_by(k_actual - 1, |a, b| a.0.cmp(&b.0));
    }
    entries.truncate(k_actual);
    entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    // 只对 K 行拼装（read_column_batch 已入段缓存，二次读取零解压）。
    let mut rows_out: Vec<Vec<Value>> = Vec::with_capacity(page.min(k_actual));
    let mut batches: Vec<Option<ColumnBatch>> = vec![None; segments.len()];
    for idx in offset..k_actual {
        let (_, sidx, r) = entries[idx];
        let si = sidx as usize;
        if batches[si].is_none() {
            let seg = &segments[si];
            let mut cols: Vec<std::sync::Arc<ColumnVector>> = Vec::with_capacity(needed.len());
            for &c in &needed {
                let Some(cv) = seg.read_column_batch(c, &cts[c]) else {
                    return Ok(None);
                };
                cols.push(cv);
            }
            batches[si] = Some(ColumnBatch::new_shared(cols));
        }
        let batch = batches[si].as_ref().expect("just set");
        let i = r as usize;
        let row: Vec<Value> = proj_meta
            .iter()
            .map(|&(bi, is_ts)| {
                let cv = &batch.cols[bi];
                if is_ts {
                    crate::storage::colbatch::i64_vec_as_timestamp(cv, i)
                } else {
                    cv.get(i)
                }
            })
            .collect();
        rows_out.push(row);
    }
    Ok(Some(rows_out))
}
