//! 向量化执行内核（VEC）的列批原语 — M0 地基。
//!
//! 设计（绞杀者模式的第一块砖）：
//! - `ValidityBitmap`：bit=1 → 该行**有效**（非 NULL）。存储段内的 null 位图
//!   语义相反（bit=1=NULL，`FixedSegment::is_null`），转换时翻转。
//! - `SelectionVec`：过滤后存活的逻辑行号（DuckDB selection vector 模式），
//!   过滤不改写数据、只追加行号。
//! - `ColumnVector` = 类型化数据体 + validity；`ColumnBatch` = 多列 + 全批
//!   共享 selection。定长批容量 2048。
//! - 与 `Value` 的相互转换只发生在批边界（push_value / get / materialize），
//!   批内部永远是类型化连续数组，供后续向量化算子自动向量化。
//!
//! 后续（M1+）：`src/sql/vector_exec/` 在此之上构建批扫描/批谓词/批聚合；
//! 缓存批通过 `CachedCol::Batch` 变体接入 store 现有内存预算体系。

use std::sync::Arc;

use crate::types::{ColumnType, Timestamp, Value, ArcString};

/// 向量化批的标准行容量。
pub const VEC_BATCH_ROWS: usize = 2048;

/// NULL 有效性位图：bit i = 1 → 第 i 行有效（非 NULL）。
/// 追加式构造；`and/or/not` 供三值逻辑的批量合成（M1 的 Kleene 算子）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidityBitmap {
    words: Vec<u64>,
    len: usize,
}

impl ValidityBitmap {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            words: Vec::with_capacity(cap.div_ceil(64)),
            len: 0,
        }
    }

    pub fn all_valid(len: usize) -> Self {
        let mut b = Self::with_capacity(len);
        for _ in 0..len {
            b.push(true);
        }
        b
    }

    /// 查询：所有行是否全部有效。满字必须全 1；尾字只比较有效位。
    pub fn is_all_valid(&self) -> bool {
        let full = self.len / 64;
        let rem = self.len % 64;
        for (i, &w) in self.words.iter().enumerate() {
            if i < full {
                if w != u64::MAX {
                    return false;
                }
            } else if i == full && rem != 0 {
                let mask = (1u64 << rem) - 1;
                if w & mask != mask {
                    return false;
                }
            }
        }
        true
    }

    pub fn all_null(len: usize) -> Self {
        let mut b = Self::with_capacity(len);
        for _ in 0..len {
            b.push(false);
        }
        b
    }

    /// 从存储段的 null 位图字节构造（bit=1=NULL → 翻转为 valid）。
    /// 字节数可以超过需要的行数（取前 `len` 位）。
    pub fn from_null_bytes(null_bytes: &[u8], len: usize) -> Self {
        let mut words = vec![0u64; len.div_ceil(64)];
        for i in 0..len {
            let is_null = (null_bytes[i / 8] >> (i % 8)) & 1 != 0;
            if !is_null {
                words[i / 64] |= 1u64 << (i % 64);
            }
        }
        Self { words, len }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn push(&mut self, valid: bool) {
        if self.len % 64 == 0 {
            self.words.push(0);
        }
        if valid {
            let w = self.len / 64;
            self.words[w] |= 1u64 << (self.len % 64);
        }
        self.len += 1;
    }

    #[inline]
    pub fn set(&mut self, i: usize, valid: bool) {
        debug_assert!(i < self.len);
        if valid {
            self.words[i / 64] |= 1u64 << (i % 64);
        } else {
            self.words[i / 64] &= !(1u64 << (i % 64));
        }
    }

    #[inline]
    pub fn is_valid(&self, i: usize) -> bool {
        debug_assert!(i < self.len);
        (self.words[i / 64] >> (i % 64)) & 1 != 0
    }

    #[inline]
    pub fn is_null(&self, i: usize) -> bool {
        !self.is_valid(i)
    }

    pub fn count_valid(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    /// 逐位 AND（Kleene AND 的 validity 合成：双方有效才有效）。
    pub fn and(&self, other: &Self) -> Self {
        debug_assert_eq!(self.len, other.len);
        Self {
            words: self
                .words
                .iter()
                .zip(other.words.iter())
                .map(|(&a, &b)| a & b)
                .collect(),
            len: self.len,
        }
    }

    /// 逐位 OR（Kleene OR：任一有效即有效 — NULL 仅在双方皆 NULL 时出现）。
    pub fn or(&self, other: &Self) -> Self {
        debug_assert_eq!(self.len, other.len);
        Self {
            words: self
                .words
                .iter()
                .zip(other.words.iter())
                .map(|(&a, &b)| a | b)
                .collect(),
            len: self.len,
        }
    }

    pub fn not(&self) -> Self {
        Self {
            words: self.words.iter().map(|&w| !w).collect(),
            len: self.len,
        }
    }
}

/// 过滤后存活的逻辑行号（selection vector）。
#[derive(Debug, Clone, Default)]
pub struct SelectionVec {
    rows: Vec<u32>,
}

impl SelectionVec {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            rows: Vec::with_capacity(cap),
        }
    }

    #[inline]
    pub fn push(&mut self, row: u32) {
        self.rows.push(row);
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    #[inline]
    pub fn get(&self, i: usize) -> u32 {
        self.rows[i]
    }

    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.rows.iter().copied()
    }

    pub fn into_inner(self) -> Vec<u32> {
        self.rows
    }

    pub fn from_vec(rows: Vec<u32>) -> Self {
        Self { rows }
    }
}

/// 单列类型化数据体。Bool 用位图（`Vec<bool>` 有 8× 放大）；
/// `Values` 承接 VECTOR/GEOMETRY/TENSOR（knn 走现有 `&[f32]` SIMD 路径）。
#[derive(Debug, Clone)]
pub enum ColData {
    I64(Vec<i64>),     // Integer / Timestamp(micros)
    F64(Vec<f64>),
    Bool(Vec<u64>),    // bit i = 1 → true
    Utf8(Vec<Arc<str>>),
    Values(Vec<Value>), // 复杂类型 fallback
}

impl ColData {
    pub fn len(&self) -> usize {
        match self {
            ColData::I64(v) => v.len(),
            ColData::F64(v) => v.len(),
            ColData::Bool(bits) => bits.len() * 64,
            ColData::Utf8(v) => v.len(),
            ColData::Values(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// 列向量：类型化数据 + NULL 有效性。
#[derive(Debug, Clone)]
pub struct ColumnVector {
    pub data: ColData,
    pub valid: ValidityBitmap,
}

impl ColumnVector {
    pub fn with_type_capacity(ct: &ColumnType, cap: usize) -> Self {
        let data = match ct {
            ColumnType::Integer | ColumnType::Timestamp => ColData::I64(Vec::with_capacity(cap)),
            ColumnType::Float => ColData::F64(Vec::with_capacity(cap)),
            ColumnType::Boolean => ColData::Bool(Vec::with_capacity(cap.div_ceil(64))),
            ColumnType::Text => ColData::Utf8(Vec::with_capacity(cap)),
            ColumnType::Tensor(_) | ColumnType::Spatial => {
                ColData::Values(Vec::with_capacity(cap))
            }
        };
        Self {
            data,
            valid: ValidityBitmap::with_capacity(cap),
        }
    }

    /// 批边界：追加一个 `Value`。NULL → 追加类型默认占位 + valid=false
    /// （保证数组行号对齐 — 这是旧 `push_value_to_column` 丢 NULL 的教训）。
    /// 类型不匹配按 fallback `Values` 无法中途切换，返回 false 由调用方决定
    /// 重建为 Values 列（M0 语义：schema 驱动构造，正常不会发生）。
    pub fn push_value(&mut self, v: &Value) -> bool {
        match (&mut self.data, v) {
            (ColData::I64(buf), Value::Integer(i)) => {
                buf.push(*i);
                self.valid.push(true);
            }
            (ColData::I64(buf), Value::Timestamp(ts)) => {
                buf.push(ts.as_micros());
                self.valid.push(true);
            }
            (ColData::F64(buf), Value::Float(f)) => {
                buf.push(*f);
                self.valid.push(true);
            }
            (ColData::Bool(bits), Value::Bool(b)) => {
                let bit = self.valid.len();
                if bit % 64 == 0 {
                    bits.push(0);
                }
                if *b {
                    bits[bit / 64] |= 1u64 << (bit % 64);
                }
                self.valid.push(true);
            }
            (ColData::Utf8(buf), Value::Text(s)) => {
                buf.push(Arc::clone(&s.0));
                self.valid.push(true);
            }
            (ColData::Values(buf), v) => {
                buf.push(v.clone());
                self.valid.push(true);
            }
            // NULL：占位值对齐长度
            (ColData::I64(buf), Value::Null) => {
                buf.push(0);
                self.valid.push(false);
            }
            (ColData::F64(buf), Value::Null) => {
                buf.push(0.0);
                self.valid.push(false);
            }
            (ColData::Bool(bits), Value::Null) => {
                let bit = self.valid.len();
                if bit % 64 == 0 {
                    bits.push(0);
                }
                self.valid.push(false);
            }
            (ColData::Utf8(buf), Value::Null) => {
                buf.push(Arc::from(""));
                self.valid.push(false);
            }
            _ => return false,
        }
        true
    }

    /// 批边界：取第 i 行（物理行号，不经 selection）为 `Value`。
    pub fn get(&self, i: usize) -> Value {
        if self.valid.is_null(i) {
            return Value::Null;
        }
        match &self.data {
            ColData::I64(v) => Value::Integer(v[i]),
            ColData::F64(v) => Value::Float(v[i]),
            ColData::Bool(bits) => Value::Bool((bits[i / 64] >> (i % 64)) & 1 != 0),
            ColData::Utf8(v) => Value::Text(ArcString(Arc::clone(&v[i]))),
            ColData::Values(v) => v[i].clone(),
        }
    }

    pub fn len(&self) -> usize {
        self.valid.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 存储侧零物化构造：定长 i64 列（Integer/Timestamp）。
    pub fn from_i64_slice(vals: &[i64], valid: ValidityBitmap) -> Self {
        Self {
            data: ColData::I64(vals.to_vec()),
            valid,
        }
    }

    /// 存储侧零物化构造：定长 f64 列。
    pub fn from_f64_slice(vals: &[f64], valid: ValidityBitmap) -> Self {
        Self {
            data: ColData::F64(vals.to_vec()),
            valid,
        }
    }
}

/// 列批：多列向量 + 全批共享 selection。
#[derive(Debug, Clone, Default)]
pub struct ColumnBatch {
    pub cols: Vec<ColumnVector>,
    pub sel: Option<SelectionVec>,
}

impl ColumnBatch {
    pub fn new(cols: Vec<ColumnVector>) -> Self {
        Self { cols, sel: None }
    }

    pub fn with_types(cts: &[ColumnType], cap: usize) -> Self {
        Self {
            cols: cts
                .iter()
                .map(|ct| ColumnVector::with_type_capacity(ct, cap))
                .collect(),
            sel: None,
        }
    }

    /// 逻辑行数（有 selection 用 selection 长度）。
    pub fn row_count(&self) -> usize {
        match &self.sel {
            Some(s) => s.len(),
            None => self.cols.first().map_or(0, |c| c.len()),
        }
    }

    #[inline]
    fn phys(&self, logical: usize) -> usize {
        match &self.sel {
            Some(s) => s.get(logical) as usize,
            None => logical,
        }
    }

    /// 批边界：取逻辑行的整行 `Value`。
    pub fn get_row(&self, logical: usize) -> Vec<Value> {
        let p = self.phys(logical);
        self.cols.iter().map(|c| c.get(p)).collect()
    }

    /// 批边界：一次性行拼装（LIMIT 在此之前生效 — M4 会前置截断）。
    pub fn materialize_rows(&self, limit: usize) -> Vec<Vec<Value>> {
        let n = self.row_count().min(limit);
        (0..n).map(|i| self.get_row(i)).collect()
    }

    /// 应用 selection（过滤后的行号集合），返回新批（列数据共享 clone）。
    pub fn take_selection(&self, sel: SelectionVec) -> Self {
        Self {
            cols: self.cols.clone(),
            sel: Some(sel),
        }
    }
}

/// `ColumnVector` 的 Timestamp 视图：`I64` 列 + 语义标记的帮助构造。
/// （存储层 Timestamp 与 Integer 同为 i64 micros；get 时按 schema 转回。）
pub fn i64_vec_as_timestamp(cv: &ColumnVector, i: usize) -> Value {
    if cv.valid.is_null(i) {
        return Value::Null;
    }
    match &cv.data {
        ColData::I64(v) => Value::Timestamp(Timestamp::from_micros(v[i])),
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validity_bitmap_bit_ops_boundaries() {
        for len in [0usize, 1, 63, 64, 65, 128, 129] {
            let mut b = ValidityBitmap::with_capacity(len);
            for i in 0..len {
                b.push(i % 3 != 0); // 周期性 NULL
            }
            assert_eq!(b.len(), len);
            for i in 0..len {
                assert_eq!(b.is_valid(i), i % 3 != 0, "len={} i={}", len, i);
            }
            // set 翻转
            if len > 0 {
                let v0 = b.is_valid(0);
                b.set(0, !v0);
                assert_eq!(b.is_valid(0), !v0);
                b.set(0, v0);
            }
            let expected_valid = (0..len).filter(|i| i % 3 != 0).count();
            assert_eq!(b.count_valid(), expected_valid);
        }
    }

    #[test]
    fn validity_from_null_bytes_flips_bits() {
        // 段语义: bit=1 → NULL。3 行: [null, valid, valid]
        let bytes = [0b0000_0001u8];
        let v = ValidityBitmap::from_null_bytes(&bytes, 3);
        assert!(v.is_null(0));
        assert!(v.is_valid(1));
        assert!(v.is_valid(2));
        // 跨字节: 第 9 行 null
        let bytes = [0x00, 0b0000_0010];
        let v = ValidityBitmap::from_null_bytes(&bytes, 10);
        assert!(v.is_null(9));
        assert!(v.is_valid(0));
    }

    #[test]
    fn validity_kleene_composition() {
        let a = ValidityBitmap::all_valid(70);
        let mut b = ValidityBitmap::all_valid(70);
        b.set(5, false);
        let and = a.and(&b);
        assert!(and.is_null(5) && and.is_valid(6));
        let or = a.or(&b);
        assert!(or.is_all_valid());
        let not = b.not();
        assert!(not.is_valid(5) && not.is_null(6));
    }

    #[test]
    fn column_vector_push_get_roundtrip_all_types() {
        let mut cv = ColumnVector::with_type_capacity(&ColumnType::Integer, 8);
        for v in [Value::Integer(1), Value::Null, Value::Integer(-5), Value::Integer(i64::MAX)] {
            assert!(cv.push_value(&v));
        }
        assert_eq!(cv.get(0), Value::Integer(1));
        assert_eq!(cv.get(1), Value::Null);
        assert_eq!(cv.get(2), Value::Integer(-5));
        assert_eq!(cv.get(3), Value::Integer(i64::MAX));

        let mut cv = ColumnVector::with_type_capacity(&ColumnType::Float, 8);
        for v in [
            Value::Float(0.5),
            Value::Null,
            Value::Float(f64::NAN),
            Value::Float(-0.0),
        ] {
            assert!(cv.push_value(&v));
        }
        assert_eq!(cv.get(0), Value::Float(0.5));
        assert_eq!(cv.get(1), Value::Null);
        match cv.get(2) {
            Value::Float(f) => assert!(f.is_nan()),
            o => panic!("{:?}", o),
        }
        assert_eq!(cv.get(3), Value::Float(-0.0));

        let mut cv = ColumnVector::with_type_capacity(&ColumnType::Boolean, 8);
        for v in [Value::Bool(true), Value::Null, Value::Bool(false), Value::Bool(true)] {
            assert!(cv.push_value(&v));
        }
        assert_eq!(cv.get(0), Value::Bool(true));
        assert_eq!(cv.get(1), Value::Null);
        assert_eq!(cv.get(2), Value::Bool(false));
        assert_eq!(cv.get(3), Value::Bool(true));

        let mut cv = ColumnVector::with_type_capacity(&ColumnType::Text, 8);
        cv.push_value(&Value::text("你好".to_string()));
        cv.push_value(&Value::Null);
        assert_eq!(cv.get(0), Value::text("你好".to_string()));
        assert_eq!(cv.get(1), Value::Null);

        // Timestamp 经 I64 存 micros
        let mut cv = ColumnVector::with_type_capacity(&ColumnType::Timestamp, 8);
        cv.push_value(&Value::Timestamp(Timestamp::from_micros(12345)));
        cv.push_value(&Value::Null);
        let v = i64_vec_as_timestamp(&cv, 0);
        assert_eq!(v, Value::Timestamp(Timestamp::from_micros(12345)));
        assert_eq!(i64_vec_as_timestamp(&cv, 1), Value::Null);
    }

    #[test]
    fn batch_selection_and_materialize() {
        let cts = [ColumnType::Integer, ColumnType::Text];
        let mut b = ColumnBatch::with_types(&cts, 8);
        for i in 0..6i64 {
            let row = vec![Value::Integer(i), Value::text(format!("t{}", i))];
            for (c, v) in b.cols.iter_mut().zip(row.iter()) {
                assert!(c.push_value(v));
            }
        }
        // 过滤: 保留偶数行 {0,2,4}
        let mut sel = SelectionVec::with_capacity(4);
        for r in [0u32, 2, 4] {
            sel.push(r);
        }
        let filtered = b.take_selection(sel);
        assert_eq!(filtered.row_count(), 3);
        assert_eq!(filtered.get_row(0), vec![Value::Integer(0), Value::text("t0".to_string())]);
        assert_eq!(filtered.get_row(2), vec![Value::Integer(4), Value::text("t4".to_string())]);
        let rows = filtered.materialize_rows(2);
        assert_eq!(rows.len(), 2);
        // 原批不受影响
        assert_eq!(b.row_count(), 6);
    }

    #[test]
    fn from_slices_zero_materialization() {
        let vals = [1i64, 2, 3, 4];
        let mut valid = ValidityBitmap::all_valid(4);
        valid.set(1, false);
        let cv = ColumnVector::from_i64_slice(&vals, valid);
        assert_eq!(cv.get(0), Value::Integer(1));
        assert_eq!(cv.get(1), Value::Null);
        assert_eq!(cv.get(3), Value::Integer(4));
        assert_eq!(cv.len(), 4);
    }
}
