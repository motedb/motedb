//! A1 PR-1 特征测试（characterization）— 直接调 L2 `sql::semantics`，
//! 把"当前正确行为"固化成合同后再接线迁移（PR-2..5）。
//!
//! 期望值来源: 0.17 已发布语义（values_equal_coerced / eval_binary_op /
//! lit_cmp / normalize_for_in 的合并裁决），见设计文档 §A1.3。
//! 已知分歧点位（NaN、不可比类型的 Ne、Eq-vs-ord 双路线）在矩阵中
//! **显式锁定**并注释——它们是 PR-2 接线时的裁决对象，不是笔误。
//!
//! Run: cargo test --test test_sql_semantics

use motedb::sql::semantics::{
    coerce_distance_operands, coerce_pair, compare, equal, normalize_set_member, Cmp3, OrdOp,
};
use motedb::types::{ArcVec, Value};

// ── 构造帮手 ─────────────────────────────────────────────────────────────

fn v_int(i: i64) -> Value {
    Value::Integer(i)
}

fn v_float(f: f64) -> Value {
    Value::Float(f)
}

fn v_text(s: &str) -> Value {
    Value::text_from(s)
}

fn v_bool(b: bool) -> Value {
    Value::Bool(b)
}

fn v_vec(f: &[f32]) -> Value {
    Value::Vector(ArcVec::new(f.to_vec()))
}

fn v_ts(iso: &str) -> Value {
    Value::Timestamp(motedb::types::Timestamp::parse_iso(iso).expect("valid iso"))
}

fn v_date(s: &str) -> Value {
    Value::Date(motedb::types::Date::parse(s).expect("valid date"))
}

fn v_null() -> Value {
    Value::Null
}

/// 测试侧建模的 Kleene AND/OR（L3 控制流职责，L2 只提供 Cmp3 原语）。
fn kleene_or(a: Cmp3, b: Cmp3) -> Cmp3 {
    match (a, b) {
        (Cmp3::True, _) | (_, Cmp3::True) => Cmp3::True,
        (Cmp3::False, Cmp3::False) => Cmp3::False,
        _ => Cmp3::Unknown,
    }
}

/// `x IN (…)` 的 L2 建模: OR 折叠 `equal(normalize(x), normalize(m))`。
fn in3(probe: &Value, members: &[Value]) -> Cmp3 {
    let p = normalize_set_member(probe);
    let mut acc = Cmp3::False;
    for m in members {
        acc = kleene_or(acc, equal(&p, &normalize_set_member(m)));
    }
    acc
}

// ── 类型对 × 六比较算子矩阵 ──────────────────────────────────────────────

struct Row {
    name: &'static str,
    a: Value,
    b: Value,
    /// `=` 走相等路线（L1 PartialEq — values_equal_coerced 同源）。
    eq: Cmp3,
    /// `=` 走偏序路线（partial_cmp == Equal — eval_binary_op 同源）。
    /// 仅 NaN 系与 eq 分歧，其余行相同。
    ord_eq: Cmp3,
    ne: Cmp3,
    lt: Cmp3,
    le: Cmp3,
    gt: Cmp3,
    ge: Cmp3,
}

use Cmp3::{False, True, Unknown};

fn matrix() -> Vec<Row> {
    vec![
        // ── 同型数值 ──
        Row {
            name: "int == int",
            a: v_int(1),
            b: v_int(1),
            eq: True,
            ord_eq: True,
            ne: False,
            lt: False,
            le: True,
            gt: False,
            ge: True,
        },
        Row {
            name: "int < int",
            a: v_int(1),
            b: v_int(2),
            eq: False,
            ord_eq: False,
            ne: True,
            lt: True,
            le: True,
            gt: False,
            ge: False,
        },
        Row {
            name: "int == float(exact)",
            a: v_int(1),
            b: v_float(1.0),
            eq: True,
            ord_eq: True,
            ne: False,
            lt: False,
            le: True,
            gt: False,
            ge: True,
        },
        Row {
            name: "int < float",
            a: v_int(1),
            b: v_float(1.5),
            eq: False,
            ord_eq: False,
            ne: True,
            lt: True,
            le: True,
            gt: False,
            ge: False,
        },
        Row {
            name: "float == float",
            a: v_float(1.0),
            b: v_float(1.0),
            eq: True,
            ord_eq: True,
            ne: False,
            lt: False,
            le: True,
            gt: False,
            ge: True,
        },
        Row {
            name: "float < float",
            a: v_float(1.0),
            b: v_float(2.0),
            eq: False,
            ord_eq: False,
            ne: True,
            lt: True,
            le: True,
            gt: False,
            ge: False,
        },
        // NaN: L1 PartialEq 特判 NaN==NaN（已发布行为），但偏序不可比 →
        // eq=True 与 ne=Unknown 并存——Eq 路线与 ord 路线的已知分歧，锁定。
        Row {
            name: "NaN == NaN (bit eq)",
            a: v_float(f64::NAN),
            b: v_float(f64::NAN),
            eq: True,
            ord_eq: Unknown,
            ne: Unknown,
            lt: Unknown,
            le: Unknown,
            gt: Unknown,
            ge: Unknown,
        },
        // 大整数精度: int_float_eq/cmp 精确到 i64（2^53+1 在 f64 里舍入）。
        Row {
            name: "2^53+1 != 2^53.0",
            a: v_int(9_007_199_254_740_993),
            b: v_float(9_007_199_254_740_992.0),
            eq: False,
            ord_eq: False,
            ne: True,
            lt: False,
            le: False,
            gt: True,
            ge: True,
        },
        Row {
            name: "i64::MIN == (i64::MIN as f64)",
            a: v_int(i64::MIN),
            b: v_float(i64::MIN as f64),
            eq: True,
            ord_eq: True,
            ne: False,
            lt: False,
            le: True,
            gt: False,
            ge: True,
        },
        // ── Bool ↔ 数值（SQL TRUE=1） ──
        Row {
            name: "TRUE == 1",
            a: v_bool(true),
            b: v_int(1),
            eq: True,
            ord_eq: True,
            ne: False,
            lt: False,
            le: True,
            gt: False,
            ge: True,
        },
        Row {
            name: "TRUE > 0",
            a: v_bool(true),
            b: v_int(0),
            eq: False,
            ord_eq: False,
            ne: True,
            lt: False,
            le: False,
            gt: True,
            ge: True,
        },
        Row {
            name: "FALSE == 0.0",
            a: v_bool(false),
            b: v_float(0.0),
            eq: True,
            ord_eq: True,
            ne: False,
            lt: False,
            le: True,
            gt: False,
            ge: True,
        },
        Row {
            name: "TRUE > FALSE",
            a: v_bool(true),
            b: v_bool(false),
            eq: False,
            ord_eq: False,
            ne: True,
            lt: False,
            le: False,
            gt: True,
            ge: True,
        },
        Row {
            name: "TRUE == TRUE",
            a: v_bool(true),
            b: v_bool(true),
            eq: True,
            ord_eq: True,
            ne: False,
            lt: False,
            le: True,
            gt: False,
            ge: True,
        },
        // Bool vs Text: 无归一臂 → eq False；ord 不可比 → Unknown。
        Row {
            name: "TRUE <> '1' (incomparable)",
            a: v_bool(true),
            b: v_text("1"),
            eq: False,
            ord_eq: Unknown,
            ne: Unknown,
            lt: Unknown,
            le: Unknown,
            gt: Unknown,
            ge: Unknown,
        },
        // ── 文本 ──
        Row {
            name: "text == text",
            a: v_text("abc"),
            b: v_text("abc"),
            eq: True,
            ord_eq: True,
            ne: False,
            lt: False,
            le: True,
            gt: False,
            ge: True,
        },
        Row {
            name: "text < text (lex)",
            a: v_text("abc"),
            b: v_text("abd"),
            eq: False,
            ord_eq: False,
            ne: True,
            lt: True,
            le: True,
            gt: False,
            ge: False,
        },
        // 不可比对: eq False（L1 catch-all），ord → Unknown。注意 eval_binary_op
        // 现状对 Ne 返回 True——分歧点，PR-2 裁决（见 semantics::compare 文档）。
        Row {
            name: "'1' <> 1 (incomparable)",
            a: v_text("1"),
            b: v_int(1),
            eq: False,
            ord_eq: Unknown,
            ne: Unknown,
            lt: Unknown,
            le: Unknown,
            gt: Unknown,
            ge: Unknown,
        },
        // ── 向量（方括号拼写以 TEXT 到达） ──
        Row {
            name: "vec == '[1.0, 2.0]'",
            a: v_vec(&[1.0, 2.0]),
            b: v_text("[1.0, 2.0]"),
            eq: True,
            ord_eq: True,
            ne: False,
            lt: False,
            le: True,
            gt: False,
            ge: True,
        },
        Row {
            name: "vec < '[3.0, 4.0]'",
            a: v_vec(&[1.0, 2.0]),
            b: v_text("[3.0, 4.0]"),
            eq: False,
            ord_eq: False,
            ne: True,
            lt: True,
            le: True,
            gt: False,
            ge: False,
        },
        Row {
            name: "vec vs non-vec text",
            a: v_vec(&[1.0, 2.0]),
            b: v_text("not-a-vec"),
            eq: False,
            ord_eq: Unknown,
            ne: Unknown,
            lt: Unknown,
            le: Unknown,
            gt: Unknown,
            ge: Unknown,
        },
        Row {
            name: "vec == vec (bit)",
            a: v_vec(&[1.0, 2.0]),
            b: v_vec(&[1.0, 2.0]),
            eq: True,
            ord_eq: True,
            ne: False,
            lt: False,
            le: True,
            gt: False,
            ge: True,
        },
        // 长度优先的字典序（L1 已发布语义）。
        Row {
            name: "vec shorter < longer",
            a: v_vec(&[1.0, 2.0]),
            b: v_vec(&[1.0, 2.0, 3.0]),
            eq: False,
            ord_eq: False,
            ne: True,
            lt: True,
            le: True,
            gt: False,
            ge: False,
        },
        // NaN 元素: eq 路线逐位相等 → True；ord 路线 NaN 视作 Greater →
        // ne True。同一对值两条路线结论相反——锁定，PR-2 裁决。
        Row {
            name: "[NaN] == [NaN] but [NaN] <> [NaN]",
            a: v_vec(&[f32::NAN]),
            b: v_vec(&[f32::NAN]),
            eq: True,
            ord_eq: False,
            ne: True,
            lt: False,
            le: False,
            gt: True,
            ge: True,
        },
        // 双侧都是方括号文本 → 纯文本比较（归一仅当另一侧为 Vector）。
        Row {
            name: "'[1,2]' == '[1,2]' as text",
            a: v_text("[1, 2]"),
            b: v_text("[1, 2]"),
            eq: True,
            ord_eq: True,
            ne: False,
            lt: False,
            le: True,
            gt: False,
            ge: True,
        },
        // ── Timestamp / Date（L1 跨型） ──
        Row {
            name: "ts == iso text",
            a: v_ts("2025-06-15T12:30:45"),
            b: v_text("2025-06-15T12:30:45"),
            eq: True,
            ord_eq: True,
            ne: False,
            lt: False,
            le: True,
            gt: False,
            ge: True,
        },
        Row {
            name: "ts vs non-iso text",
            a: v_ts("2025-06-15T12:30:45"),
            b: v_text("nope"),
            eq: False,
            ord_eq: Unknown,
            ne: Unknown,
            lt: Unknown,
            le: Unknown,
            gt: Unknown,
            ge: Unknown,
        },
        Row {
            name: "ts == micros int",
            a: v_ts("2025-06-15T12:30:45"),
            b: v_int(
                motedb::types::Timestamp::parse_iso("2025-06-15T12:30:45")
                    .unwrap()
                    .as_micros(),
            ),
            eq: True,
            ord_eq: True,
            ne: False,
            lt: False,
            le: True,
            gt: False,
            ge: True,
        },
        Row {
            name: "ts < later iso",
            a: v_ts("2025-06-15T12:30:45"),
            b: v_text("2025-06-16T00:00:00"),
            eq: False,
            ord_eq: False,
            ne: True,
            lt: True,
            le: True,
            gt: False,
            ge: False,
        },
        Row {
            name: "date == 'YYYY-MM-DD'",
            a: v_date("2026-03-01"),
            b: v_text("2026-03-01"),
            eq: True,
            ord_eq: True,
            ne: False,
            lt: False,
            le: True,
            gt: False,
            ge: True,
        },
        Row {
            name: "date == midnight ts",
            a: v_date("2026-03-01"),
            b: Value::Timestamp(motedb::types::Timestamp::from_micros(
                motedb::types::Date::parse("2026-03-01").unwrap().days() as i64 * 86_400_000_000,
            )),
            eq: True,
            ord_eq: True,
            ne: False,
            lt: False,
            le: True,
            gt: False,
            ge: True,
        },
        // ── NULL 三值 ──
        Row {
            name: "NULL op int",
            a: v_null(),
            b: v_int(5),
            eq: Unknown,
            ord_eq: Unknown,
            ne: Unknown,
            lt: Unknown,
            le: Unknown,
            gt: Unknown,
            ge: Unknown,
        },
        Row {
            name: "int op NULL",
            a: v_int(5),
            b: v_null(),
            eq: Unknown,
            ord_eq: Unknown,
            ne: Unknown,
            lt: Unknown,
            le: Unknown,
            gt: Unknown,
            ge: Unknown,
        },
        Row {
            name: "NULL op NULL",
            a: v_null(),
            b: v_null(),
            eq: Unknown,
            ord_eq: Unknown,
            ne: Unknown,
            lt: Unknown,
            le: Unknown,
            gt: Unknown,
            ge: Unknown,
        },
        Row {
            name: "NULL op TRUE (null wins)",
            a: v_null(),
            b: v_bool(true),
            eq: Unknown,
            ord_eq: Unknown,
            ne: Unknown,
            lt: Unknown,
            le: Unknown,
            gt: Unknown,
            ge: Unknown,
        },
        Row {
            name: "vec op NULL",
            a: v_vec(&[1.0]),
            b: v_null(),
            eq: Unknown,
            ord_eq: Unknown,
            ne: Unknown,
            lt: Unknown,
            le: Unknown,
            gt: Unknown,
            ge: Unknown,
        },
    ]
}

#[test]
fn type_pair_matrix_eq_and_ordering() {
    for r in matrix() {
        let ctx = format!("case `{}`: {:?} vs {:?}", r.name, r.a, r.b);
        assert_eq!(equal(&r.a, &r.b), r.eq, "equal: {ctx}");
        assert_eq!(
            compare(&r.a, &r.b, OrdOp::Eq),
            r.ord_eq,
            "compare Eq (ord route): {ctx}"
        );
        assert_eq!(compare(&r.a, &r.b, OrdOp::Ne), r.ne, "compare Ne: {ctx}");
        assert_eq!(compare(&r.a, &r.b, OrdOp::Lt), r.lt, "compare Lt: {ctx}");
        assert_eq!(compare(&r.a, &r.b, OrdOp::Le), r.le, "compare Le: {ctx}");
        assert_eq!(compare(&r.a, &r.b, OrdOp::Gt), r.gt, "compare Gt: {ctx}");
        assert_eq!(compare(&r.a, &r.b, OrdOp::Ge), r.ge, "compare Ge: {ctx}");
    }
}

// ── IN / NOT IN（L2 建模） ──────────────────────────────────────────────

#[test]
fn in_membership_three_valued() {
    // 常规命中/未命中
    assert_eq!(in3(&v_int(1), &[v_int(1), v_int(2)]), True);
    assert_eq!(in3(&v_int(3), &[v_int(1), v_int(2)]), False);
    // NULL 探测 → Unknown
    assert_eq!(in3(&v_null(), &[v_int(1)]), Unknown);
    // NULL 成员，无命中 → Unknown（1 = NULL 是 Unknown）
    assert_eq!(in3(&v_int(1), &[v_null()]), Unknown);
    // 命中压倒 Unknown: T OR U = T
    assert_eq!(in3(&v_int(1), &[v_null(), v_int(1)]), True);
    // NOT IN = flip
    assert_eq!(in3(&v_int(3), &[v_int(1), v_int(2)]).flip(), True);
    assert_eq!(in3(&v_int(1), &[v_int(1), v_int(2)]).flip(), False);
    // 经典 SQL 陷阱: x NOT IN (…, NULL) 永远非 True
    assert_eq!(in3(&v_int(3), &[v_int(1), v_null()]).flip(), Unknown);
}

#[test]
fn in_membership_normalized_forms() {
    // Bool↔Int: TRUE IN (1) 与 1 IN (TRUE) 都命中
    assert_eq!(in3(&v_bool(true), &[v_int(1)]), True);
    assert_eq!(in3(&v_int(1), &[v_bool(true)]), True);
    // ISO 文本 ↔ Timestamp（hash 一致性归一）
    assert_eq!(
        in3(
            &v_ts("2025-06-15T12:30:45"),
            &[v_text("2025-06-15T12:30:45")]
        ),
        True
    );
    // 方括号向量文本 ↔ Vector
    assert_eq!(in3(&v_vec(&[1.0, 2.0]), &[v_text("[1.0, 2.0]")]), True);
    // 🔀 PR-4 裁决（推翻 PR-1 的怪癖锁定）: 纯数字文本不再按微秒时间戳
    // 归一——0.17 哈希路径曾把 '1' 当 Timestamp(1µs) 而与 Int 1 判等，
    // 与 equal 路线（不可比即不等）分歧（差分 harness round=161）。
    // ISO 形状照常; 数字时间戳语义由真正的 Timestamp 值承载。
    assert_eq!(in3(&v_int(1), &[v_text("1")]), False);
    // 真正的非数字文本保持原样 → 不命中（而非报错）
    assert_eq!(in3(&v_int(1), &[v_text("1x")]), False);
}

#[test]
fn normalize_set_member_forms() {
    assert_eq!(normalize_set_member(&v_bool(true)), v_int(1));
    assert!(matches!(
        normalize_set_member(&v_text("2025-06-15T12:30:45")),
        Value::Timestamp(_)
    ));
    assert!(matches!(
        normalize_set_member(&v_text("[1.0, 2.0]")),
        Value::Vector(_)
    ));
    // 普通文本原样
    assert_eq!(normalize_set_member(&v_text("plain")), v_text("plain"));
    // NULL 原样
    assert!(matches!(normalize_set_member(&v_null()), Value::Null));
}

// ── 距离算子操作数归一 ──────────────────────────────────────────────────

#[test]
fn distance_operands_parse_bracketed_text() {
    let lhs = v_vec(&[1.0, 0.0]);
    let rhs = v_text("[0.0, 1.0]");
    let (a, b) = coerce_distance_operands(&lhs, &rhs);
    assert!(matches!(a.as_ref(), Value::Vector(_)));
    assert!(matches!(b.as_ref(), Value::Vector(_)));
    // Spatial 透传（点位判定归距离实现，不在 L2 越权）
    let s = v_text("ST_POINT(1 2)");
    let vec = v_vec(&[1.0, 2.0]);
    let (x, _) = coerce_distance_operands(&s, &vec);
    assert!(matches!(x.as_ref(), Value::Text(_)));
}

// ── 编译期字面量归一（coerce_literal_for_column） ───────────────────────

#[test]
fn column_aware_literal_coercion() {
    use motedb::types::{ColumnDef, ColumnType, TableSchema};

    let schema = TableSchema::new(
        "t".into(),
        vec![
            ColumnDef::new("id".into(), ColumnType::Integer, 0),
            ColumnDef::new("emb".into(), ColumnType::Tensor(2), 1),
            ColumnDef::new("name".into(), ColumnType::Text, 2),
        ],
    );

    // VECTOR 列 + 方括号文本 → Vector
    assert!(matches!(
        motedb::sql::semantics::coerce_literal_for_column(&schema, 1, v_text("[1.0, 2.0]")),
        Value::Vector(_)
    ));
    // 非 VECTOR 列不解析（文本列保持文本）
    assert!(matches!(
        motedb::sql::semantics::coerce_literal_for_column(&schema, 2, v_text("[1.0, 2.0]")),
        Value::Text(_)
    ));
    // 非文本字面量原样
    assert_eq!(
        motedb::sql::semantics::coerce_literal_for_column(&schema, 1, v_int(5)),
        v_int(5)
    );
    // 位置越界原样（防御）
    assert_eq!(
        motedb::sql::semantics::coerce_literal_for_column(&schema, 99, v_text("[1.0]")),
        v_text("[1.0]")
    );
}

// ── Cmp3 原语 ───────────────────────────────────────────────────────────

#[test]
fn cmp3_filtering_and_flip() {
    // WHERE 视角: 只有 True 放行
    assert!(Cmp3::True.is_true());
    assert!(!Cmp3::False.is_true());
    assert!(!Cmp3::Unknown.is_true());
    // NOT UNKNOWN = UNKNOWN
    assert_eq!(Cmp3::Unknown.flip(), Cmp3::Unknown);
    assert_eq!(Cmp3::True.flip(), Cmp3::False);
}

#[test]
fn coerce_pair_zero_alloc_on_miss() {
    // 归一未命中 → 双侧借用（热路径零分配合同）
    let a_src = v_int(1);
    let b_src = v_text("x");
    let (a, b) = coerce_pair(&a_src, &b_src);
    assert!(matches!(a, std::borrow::Cow::Borrowed(_)));
    assert!(matches!(b, std::borrow::Cow::Borrowed(_)));
    // Bool 命中 → 一侧 Owned
    let bool_src = v_bool(true);
    let int_src = v_int(1);
    let (a, _) = coerce_pair(&bool_src, &int_src);
    assert!(matches!(a, std::borrow::Cow::Owned(_)));
}

// ── NULL 三值 / Kleene 的引擎现状锁定（探针实测 2026-11-04） ─────────────
//
// 12 条探针在现行引擎（快路径 + 逗号 join）上全部符合 SQL 三值标准：
// 比较遇 NULL → NULL；AND/OR 严格 Kleene；WHERE 过滤 Unknown 不放行。
// #7 `eval_with_materialized` 源码里的两值化 AND/OR 与 NULL→Bool(false)
// 前转在这些形状不可达（未触发的死分支或另有守卫）——PR-2 接线时
// 修正源码形态并用本组用例验证无行为回退。

#[test]
fn engine_null_three_valued_current_state() {
    use motedb::sql::QueryResult;
    use motedb::Database;

    let dir = tempfile::TempDir::new().expect("temp dir");
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 10)").unwrap();

    let scalar = |sql: &str| -> Value {
        match db.execute(sql).unwrap().materialize().unwrap() {
            QueryResult::Select { rows, .. } => rows[0][0].clone(),
            other => panic!("expected select, got {:?}", other),
        }
    };
    let row_count = |sql: &str| -> usize {
        match db.execute(sql).unwrap().materialize().unwrap() {
            QueryResult::Select { rows, .. } => rows.len(),
            other => panic!("expected select, got {:?}", other),
        }
    };

    // 投影视角: NULL 比较 → NULL（非 FALSE）
    assert!(matches!(scalar("SELECT NULL = NULL"), Value::Null));
    assert!(matches!(scalar("SELECT NULL = 1"), Value::Null));
    assert!(matches!(scalar("SELECT NULL < 1"), Value::Null));
    // NOT UNKNOWN = UNKNOWN
    assert!(matches!(scalar("SELECT NOT (NULL = 1)"), Value::Null));
    // Kleene AND/OR: FALSE 支配 AND、TRUE 支配 OR，否则 NULL
    assert!(matches!(scalar("SELECT NULL AND TRUE"), Value::Null));
    assert!(matches!(
        scalar("SELECT NULL AND FALSE"),
        Value::Bool(false)
    ));
    assert!(matches!(scalar("SELECT NULL OR TRUE"), Value::Bool(true)));
    assert!(matches!(scalar("SELECT NULL OR FALSE"), Value::Null));

    // WHERE 过滤视角: Unknown 不放行（含取反后的 Unknown）
    assert_eq!(row_count("SELECT id FROM t WHERE NULL = 1"), 0);
    assert_eq!(row_count("SELECT id FROM t WHERE NOT (NULL = 1)"), 0);
    // v NULL 的行: NOT (NULL = 10) = Unknown → 不放行; v=10 的行: NOT true → 不放行
    assert_eq!(row_count("SELECT id FROM t WHERE NOT (v = 10)"), 0);
    // 逗号 join（慢路径）同样正确
    assert_eq!(
        row_count("SELECT a.id FROM t a, t b WHERE NOT (b.v = 10)"),
        0
    );
}

// ── eval_cmp 复合入口（PR-2 起解释器家族的 `=` 裁决） ───────────────────

#[test]
fn eval_cmp_composite_adjudication() {
    use motedb::sql::semantics::eval_cmp;

    // Eq/Ne 走相等路线: NaN 向量按位判真 → Ne 为 False（与裸 compare 的
    // ord 路线结论相反——复合入口的裁决，见其文档）。
    let nan_vec = v_vec(&[f32::NAN]);
    assert_eq!(eval_cmp(&nan_vec, &nan_vec, OrdOp::Eq), True);
    assert_eq!(eval_cmp(&nan_vec, &nan_vec, OrdOp::Ne), False);
    // 不可比对: Eq False / Ne True（解释器 0.17 发布行为，SQLite 同款）;
    // Lt..Ge → Unknown。
    let t1 = v_text("x");
    let i1 = v_int(5);
    assert_eq!(eval_cmp(&t1, &i1, OrdOp::Eq), False);
    assert_eq!(eval_cmp(&t1, &i1, OrdOp::Ne), True);
    assert_eq!(eval_cmp(&t1, &i1, OrdOp::Lt), Unknown);
    // NULL 三值不变
    assert_eq!(eval_cmp(&v_null(), &i1, OrdOp::Eq), Unknown);
    assert_eq!(eval_cmp(&v_null(), &i1, OrdOp::Ne), Unknown);
    // Bool/方括号向量归一内置
    assert_eq!(eval_cmp(&v_bool(true), &v_int(1), OrdOp::Eq), True);
    assert_eq!(
        eval_cmp(&v_vec(&[1.0, 2.0]), &v_text("[1.0, 2.0]"), OrdOp::Ne),
        False
    );
}
