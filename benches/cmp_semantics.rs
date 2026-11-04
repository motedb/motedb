//! A1 §A1.6 性能护栏 — L2 `sql::semantics` 热路径微基准。
//!
//! PR-2/3/4 接线前后各跑一次（`cargo bench --bench cmp_semantics`），
//! 非命中归一路径相对裸 L1 比较的回归 ≤3% 是门禁。命中归一的路径
//! （Bool/方括号向量）允许分配，单独盯。
//!
//! 形状取自真实热路径: `apply_op_value`（JOIN 下推逐行）与
//! `lit_cmp`（VEC 批扫描逐行）都是"列值 op 字面量"的百万次循环。

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use motedb::sql::semantics::{compare, equal, OrdOp};
use motedb::types::Value;

fn bench_cmp_semantics(c: &mut Criterion) {
    let int_a = Value::Integer(42);
    let int_b = Value::Integer(43);
    let txt_a = Value::text_from("user_42");
    let txt_b = Value::text_from("user_43");
    let bool_a = Value::Bool(true);
    let vec_a = Value::Vector(motedb::types::ArcVec::new(vec![1.0, 2.0, 3.0]));
    let vec_txt = Value::text_from("[1.0, 2.0, 3.0]");

    // ── 非命中归一（Cow 借用、零分配）——护栏主对象 ──
    let mut g = c.benchmark_group("miss-int");
    g.bench_function("l2-equal", |b| {
        b.iter(|| equal(black_box(&int_a), black_box(&int_b)).is_true())
    });
    g.bench_function("bare-l1-eq", |b| {
        b.iter(|| black_box(&int_a) == black_box(&int_b))
    });
    g.bench_function("l2-compare-lt", |b| {
        b.iter(|| compare(black_box(&int_a), black_box(&int_b), OrdOp::Lt).is_true())
    });
    g.bench_function("bare-l1-lt", |b| {
        b.iter(|| {
            black_box(&int_a).partial_cmp(black_box(&int_b)) == Some(std::cmp::Ordering::Less)
        })
    });
    drop(g);

    let mut g = c.benchmark_group("miss-text");
    g.bench_function("l2-equal", |b| {
        b.iter(|| equal(black_box(&txt_a), black_box(&txt_b)).is_true())
    });
    g.bench_function("bare-l1-eq", |b| {
        b.iter(|| black_box(&txt_a) == black_box(&txt_b))
    });
    drop(g);

    // ── 命中归一（分配允许，量级参考） ──
    let mut g = c.benchmark_group("hit");
    g.bench_function("bool-int-equal", |b| {
        b.iter(|| equal(black_box(&bool_a), black_box(&int_b)).is_true())
    });
    g.bench_function("vec-bracketed-equal", |b| {
        b.iter(|| equal(black_box(&vec_a), black_box(&vec_txt)).is_true())
    });
    drop(g);
}

criterion_group!(benches, bench_cmp_semantics);
criterion_main!(benches);
