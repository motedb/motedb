//! SQL 比较语义层（L2）— 0.18 求值器统一的唯一定义点。
//!
//! 设计: docs/design/2026-11-018-unification-and-capabilities.md §A1.2。
//! 三层架构:
//! - **L1 类型层** `types::Value` 的 `PartialEq`/`PartialOrd`（同型与数值
//!   跨型、Timestamp/Date↔ISO 文本、Vector 逐元素——本模块只委托，不复制）;
//! - **L2 语义层**（本模块）: NULL 三值裁决、Bool↔数值（SQL TRUE=1）、
//!   方括号向量文本（`'[1.0, 2.0]'`，词法器无向量字面量 token）——SQL
//!   "操作数归一 + 比较" 的唯一实现;
//! - **L3 执行层**（evaluator / executor / vector_exec）只保留控制流:
//!   求值顺序、三值逻辑门、短路、批布局——不允许再出现第二份比较语义。
//!
//! 迁移状态（PR-1）: 本模块已定稿并带特征测试，但**尚未接线**——11 个
//! 现存实现点位按 PR-2..5 逐批切换到此处，PR-5 删旧实现并加 CI 门禁
//! （`scripts/check_sql_cmp.sh`）防回潮。
//!
//! 性能约束: 全部 `#[inline]`；归一未命中路径零分配（`Cow` 借用返回），
//! 保 `apply_op_value` / `lit_cmp` 的逐行热路径（bench 护栏 ≤3%）。

use std::borrow::Cow;

use crate::sql::ast::BinaryOperator;
use crate::types::{TableSchema, Value};

/// SQL 三值比较结果（NULL 或不可比 → `Unknown`）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cmp3 {
    True,
    False,
    Unknown,
}

impl Cmp3 {
    /// WHERE 过滤视角：只有 `True` 放行（`Unknown` 与 `False` 都不匹配）。
    /// 这是 SQL 标准的过滤语义，也是现有所有执行点位的共同行为。
    #[inline]
    pub fn is_true(self) -> bool {
        matches!(self, Cmp3::True)
    }

    #[inline]
    pub fn is_unknown(self) -> bool {
        matches!(self, Cmp3::Unknown)
    }

    #[inline]
    pub fn from_bool(b: bool) -> Cmp3 {
        if b {
            Cmp3::True
        } else {
            Cmp3::False
        }
    }

    /// `NOT` 下的三值取反：`NOT UNKNOWN = UNKNOWN`。
    #[inline]
    pub fn flip(self) -> Cmp3 {
        match self {
            Cmp3::True => Cmp3::False,
            Cmp3::False => Cmp3::True,
            Cmp3::Unknown => Cmp3::Unknown,
        }
    }
}

/// 六个比较算子。与 `ast::BinaryOperator` 解耦，L3 各点位在 match 臂里
/// 构造（编译期已判过算子种类），L2 不依赖解析器枚举的其余臂。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrdOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl OrdOp {
    /// 解析器算子 → 比较算子。非比较算子（算术/逻辑/距离）→ `None`。
    #[inline]
    pub fn from_binary(op: &BinaryOperator) -> Option<OrdOp> {
        Some(match op {
            BinaryOperator::Eq => OrdOp::Eq,
            BinaryOperator::Ne => OrdOp::Ne,
            BinaryOperator::Lt => OrdOp::Lt,
            BinaryOperator::Le => OrdOp::Le,
            BinaryOperator::Gt => OrdOp::Gt,
            BinaryOperator::Ge => OrdOp::Ge,
            _ => return None,
        })
    }

    #[inline]
    fn matches_ordering(self, ord: std::cmp::Ordering) -> bool {
        use std::cmp::Ordering::*;
        match self {
            OrdOp::Eq => ord == Equal,
            OrdOp::Ne => ord != Equal,
            OrdOp::Lt => ord == Less,
            OrdOp::Le => ord != Greater,
            OrdOp::Gt => ord == Greater,
            OrdOp::Ge => ord != Less,
        }
    }
}

/// 任一侧是否参与 L2 归一（Bool 或 Vector）。未参与的对子直接走 L1，
/// 零分配快路径。
#[inline]
fn needs_coercion(a: &Value, b: &Value) -> bool {
    matches!(a, Value::Bool(_) | Value::Vector(_)) || matches!(b, Value::Bool(_) | Value::Vector(_))
}

/// 操作数归一（0.17 已发布语义的合并超集）:
/// - **Bool↔Int/Float、Bool↔Bool → Int/Float**（SQL `TRUE=1`，源:
///   `evaluator::coerce_bool_numeric`）;
/// - **方括号向量文本 ↔ Vector**（`emb = '[1.0, 2.0]'` 里字面量以 TEXT
///   到达——词法器无向量 token；仅当另一侧为 Vector 且可解析时替换，
///   源: `evaluator::coerce_vector_text`）;
/// - 其余对子原样借用（Int↔Float、Timestamp↔Int/Text、Date↔Text 等
///   跨型比较是 **L1** 类型层的职责，不在 L2 重复）。
///
/// 归一不命中 → 双侧 `Cow::Borrowed`，零分配。
#[inline]
pub fn coerce_pair<'a>(a: &'a Value, b: &'a Value) -> (Cow<'a, Value>, Cow<'a, Value>) {
    use Value::*;
    match (a, b) {
        // ── Bool ↔ 数值（evaluator::coerce_bool_numeric 全表） ──
        (Bool(x), Integer(_)) => (Cow::Owned(Integer(*x as i64)), Cow::Borrowed(b)),
        (Integer(_), Bool(y)) => (Cow::Borrowed(a), Cow::Owned(Integer(*y as i64))),
        (Bool(x), Float(_)) => (Cow::Owned(Float(*x as i64 as f64)), Cow::Borrowed(b)),
        (Float(_), Bool(y)) => (Cow::Borrowed(a), Cow::Owned(Float(*y as i64 as f64))),
        (Bool(x), Bool(y)) => (
            Cow::Owned(Integer(*x as i64)),
            Cow::Owned(Integer(*y as i64)),
        ),
        // ── 方括号向量文本 ↔ Vector（evaluator::coerce_vector_text） ──
        // 非向量拼写（解析失败）保持 TEXT——与 0.17 行为一致（不报错、
        // 后续比较自然不相等）。
        (Vector(_), Text(s)) => match Value::parse_bracketed_vector(s.as_str()) {
            Some(v) => (Cow::Borrowed(a), Cow::Owned(v)),
            None => (Cow::Borrowed(a), Cow::Borrowed(b)),
        },
        (Text(s), Vector(_)) => match Value::parse_bracketed_vector(s.as_str()) {
            Some(v) => (Cow::Owned(v), Cow::Borrowed(b)),
            None => (Cow::Borrowed(a), Cow::Borrowed(b)),
        },
        _ => (Cow::Borrowed(a), Cow::Borrowed(b)),
    }
}

/// SQL `=`: 任一 NULL → `Unknown`；否则归一后 L1 相等判定。
///
/// 相等的裁决来源是 **L1 `PartialEq`**（Vector 逐位、大整数↔浮点精确
/// `int_float_eq`）——与 0.17 主路径 `values_equal_coerced`/IN 匹配同源。
/// 注意: 解释器 `eval_binary_op` 的 `=` 走 `partial_cmp == Equal` 路线，
/// 两路线仅在 NaN 向量、超大整数的边界位上分歧——特征测试已锁定，
/// PR-2 接线时按"已发布行为优先"裁决到本实现。
#[inline]
pub fn equal(a: &Value, b: &Value) -> Cmp3 {
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return Cmp3::Unknown;
    }
    if !needs_coercion(a, b) {
        return Cmp3::from_bool(a == b);
    }
    let (a2, b2) = coerce_pair(a, b);
    Cmp3::from_bool(a2 == b2)
}

/// SQL `<`/`<=`/`>`/`>=`/`=`/`<>`: 任一 NULL → `Unknown`；归一后 L1
/// 偏序；**不可比类型（如 Text vs Integer）→ `Unknown`**。
///
/// ⚠ 与 0.17 个别点位的已知分歧（特征测试锁定，PR-2 裁决）:
/// - `eval_binary_op` 对不可比对返回 `False`（`Ne` 则 `True`）;
/// - `vector_exec::lit_cmp` 与本层一致返回 `Unknown`。
/// WHERE 过滤视角两者同效（除 `col <> <不可比字面量>` 外），投影视角
/// 不同；统一到 `Unknown`（SQL 标准，与 #10 一致）。
#[inline]
pub fn compare(a: &Value, b: &Value, op: OrdOp) -> Cmp3 {
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return Cmp3::Unknown;
    }
    if !needs_coercion(a, b) {
        return match a.partial_cmp(b) {
            Some(ord) => Cmp3::from_bool(op.matches_ordering(ord)),
            None => Cmp3::Unknown,
        };
    }
    let (a2, b2) = coerce_pair(a, b);
    match a2.partial_cmp(&b2) {
        Some(ord) => Cmp3::from_bool(op.matches_ordering(ord)),
        None => Cmp3::Unknown,
    }
}

/// 距离算子（`<->` / `<=>` / `<#>`）操作数归一——`coerce_pair` 的超集
/// 预留（Spatial 操作数原样透传给距离实现判定点位几何）。
///
/// 0.17 行为: `eval_binary_op` 在分派距离算子前做 `coerce_vector_text`
/// （方括号文本 → Vector），本函数即该语义的定点。
#[inline]
pub fn coerce_distance_operands<'a>(
    a: &'a Value,
    b: &'a Value,
) -> (Cow<'a, Value>, Cow<'a, Value>) {
    // 目前与 coerce_pair 完全同集；独立入口保留给距离特有归一
    // （维度/精度档位）将来扩展而不污染通用比较。
    coerce_pair(a, b)
}

/// IN 集合成员/探测值归一（吸收 `executor::normalize_for_in`，语义不变）:
/// - Bool → Integer（`TRUE IN (1)` 与 `=` 同语义）;
/// - ISO 文本 → Timestamp（HashSet 匹配要求 eq 与 hash 同型——L1 的 eq
///   已认 Timestamp↔ISO 文本，但两者 hash 不同，须归一到同一表示）;
/// - 方括号向量文本 → Vector（同 hash 一致性理由）;
/// - 其余（含 NULL）原样克隆。
#[inline]
pub fn normalize_set_member(v: &Value) -> Value {
    match v {
        Value::Bool(b) => Value::Integer(if *b { 1 } else { 0 }),
        Value::Text(s) => crate::types::Timestamp::parse_iso(s.as_str())
            .map(Value::Timestamp)
            .or_else(|| Value::parse_bracketed_vector(s.as_str()))
            .unwrap_or_else(|| v.clone()),
        other => other.clone(),
    }
}

/// 编译期按列类型归一字面量（吸收
/// `executor::coerce_vector_literal_for_column`，语义不变）: VECTOR
/// （Tensor）列上的方括号文本字面量 → Vector，使编译期 Eq/Ne 谓词能
/// 命中。显式扩展点: Date/Timestamp 列的字面量归一将来同点接入。
///
/// schema 感知只允许出现在这类**编译期入口**——L2 其余函数不持有
/// schema（设计约束 §A1.2）。
pub fn coerce_literal_for_column(schema: &TableSchema, pos: usize, val: Value) -> Value {
    if let (Some(col), Value::Text(s)) = (schema.columns.get(pos), &val) {
        if matches!(col.col_type, crate::types::ColumnType::Tensor(_)) {
            if let Some(vector) = Value::parse_bracketed_vector(s.as_str()) {
                return vector;
            }
        }
    }
    val
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cmp3_flip_is_kleene() {
        assert_eq!(Cmp3::True.flip(), Cmp3::False);
        assert_eq!(Cmp3::False.flip(), Cmp3::True);
        assert_eq!(Cmp3::Unknown.flip(), Cmp3::Unknown);
    }

    #[test]
    fn ord_op_maps_six_comparisons_only() {
        use BinaryOperator::*;
        assert_eq!(OrdOp::from_binary(&Eq), Some(OrdOp::Eq));
        assert_eq!(OrdOp::from_binary(&Ge), Some(OrdOp::Ge));
        assert_eq!(OrdOp::from_binary(&And), None);
        assert_eq!(OrdOp::from_binary(&L2Distance), None);
    }

    #[test]
    fn coerce_pair_borrows_when_no_coercion() {
        let a = Value::Integer(1);
        let b = Value::text_from("x");
        let (ca, cb) = coerce_pair(&a, &b);
        assert!(matches!(ca, Cow::Borrowed(_)));
        assert!(matches!(cb, Cow::Borrowed(_)));
    }

    #[test]
    fn coerce_pair_parses_bracketed_vector_text() {
        let v = Value::Vector(crate::types::ArcVec::new(vec![1.0, 2.0]));
        let t = Value::text_from("[1.0, 2.0]");
        let (ca, cb) = coerce_pair(&v, &t);
        assert!(matches!(ca, Cow::Borrowed(_)));
        assert!(matches!(cb, Cow::Owned(Value::Vector(_))));
        // 非向量拼写保持 TEXT
        let t2 = Value::text_from("hello");
        let (ca2, cb2) = coerce_pair(&v, &t2);
        assert!(matches!(cb2, Cow::Borrowed(_)));
        assert!(matches!(ca2, Cow::Borrowed(_)));
    }
}
