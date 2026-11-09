//! 🔴 回归: path-matrix 扫描（tests/test_path_matrix_correctness.rs）抓出的
//! 五个静默错误结果 + 一个 panic。每个测试对应一个独立根因，全部是
//! 0.18 差分 harness 九路径与既有测试未覆盖的组合形状。
//!
//! 1. BOOLEAN 列经 ORDER BY + LIMIT 的 top-k 解码走 get_i64 → 越界 panic
//!    （bool 列不是 8 字节定长布局，FixedSegment 数据为空）。
//! 2. MATCH + ORDER BY <非投影列> DESC → 排序被静默跳过（#17 的更广面：
//!    排序键只查投影列，SELECT id … ORDER BY ts DESC 丢方向）。
//! 3. MIN(d)/MAX(d) 返回 Integer(days) —— 向量化聚合只还原 TIMESTAMP。
//! 4. 多段表 WHERE int_col <op> N → 结果按"最新段优先"输出，拆段后行序
//!    翻转、LIMIT 窗口取错行。
//! 5. WHERE fval >= X 多段路径: FLOAT 列用 `i as f64` 把 f64 位模式当整数
//!    再数值转换（1.5 → 4.6e18 量级垃圾值）。
//! 6. DATE 列在 top-k / decode_rows_at / 排序键提取器落 `_ =>` → NULL 或
//!    排序失效（#18 变体一致性家族）。

use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn canon(v: &Value) -> String {
    match v {
        Value::Null => "∅".into(),
        Value::Integer(i) => format!("I:{i}"),
        Value::Float(f) => format!("F:{f:?}"),
        Value::Text(s) => format!("S:{}", s.as_str()),
        Value::Bool(b) => format!("B:{}", if *b { 1 } else { 0 }),
        Value::Timestamp(t) => format!("T:{}", t.as_micros()),
        Value::Date(d) => format!("D:{}", d.days()),
        other => format!("X:{other:?}"),
    }
}

fn setup() -> (Database, tempfile::TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute(
        "CREATE TABLE t (id INT PRIMARY KEY, ival INT, fval REAL, flag BOOLEAN, \
         ts TIMESTAMP, d DATE)",
    )
    .unwrap();
    let d0 = motedb::types::Date::parse("2026-01-01").unwrap().days();
    for i in 0..12i64 {
        db.execute(&format!(
            "INSERT INTO t VALUES ({}, {}, {}, {}, {}, '{}')",
            i,
            (i * 7) % 23,
            i as f64 * 0.25 - 1.5,
            i % 2 == 0,
            1_794_300_000_000_000 + i * 2_220_000_000,
            motedb::types::Date::from_days(d0 + i as i32).to_iso(),
        ))
        .unwrap();
    }
    (db, dir)
}

/// 1: BOOLEAN 投影 + ORDER BY id LIMIT k 不得 panic，且返回 Bool 变体。
#[test]
fn boolean_projection_order_by_limit_no_panic() {
    let (db, _d) = setup();
    let rows = db
        .query("SELECT id, flag FROM t ORDER BY id LIMIT 5")
        .unwrap();
    assert_eq!(rows.len(), 5);
    for (i, r) in rows.iter().enumerate() {
        assert_eq!(r[0], Value::Integer(i as i64), "id order preserved");
        assert!(
            matches!(r[1], Value::Bool(_) | Value::Null),
            "flag must decode as Bool, got {:?}",
            r[1]
        );
    }
}

/// 2: MATCH + ORDER BY ts DESC，投影里没有 ts —— 必须仍然降序（#17 广面）。
#[test]
fn fts_order_by_non_projected_column_keeps_direction() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE f (id INT PRIMARY KEY, ts TIMESTAMP, note TEXT)")
        .unwrap();
    db.execute("CREATE TEXT INDEX f_note ON f(note)").unwrap();
    for i in 0..9i64 {
        db.execute(&format!(
            "INSERT INTO f VALUES ({}, {}, 'tok{} doc{}')",
            i,
            1_794_300_000_000_000 + i * 2_220_000_000,
            i % 3,
            i
        ))
        .unwrap();
    }
    std::thread::sleep(std::time::Duration::from_millis(1500));

    let rows = db
        .query("SELECT id FROM f WHERE MATCH(note) AGAINST('tok1') ORDER BY ts DESC")
        .unwrap();
    let ids: Vec<i64> = rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            other => panic!("id as {other:?}"),
        })
        .collect();
    let mut want = ids.clone();
    want.sort_unstable_by(|a, b| b.cmp(a));
    assert_eq!(ids, want, "ORDER BY ts DESC must hold with ts unprojected");
}

/// 3: MIN/MAX over DATE 返回 Date 变体（不是 Integer(days)）。
#[test]
fn min_max_date_returns_date_variant() {
    let (db, _d) = setup();
    for q in ["SELECT MIN(d) FROM t", "SELECT MAX(d) FROM t"] {
        let rows = db.query(q).unwrap();
        assert!(
            matches!(rows[0][0], Value::Date(_)),
            "{q} must return Value::Date, got {:?}",
            rows[0][0]
        );
    }
}

/// 4: 多段表（两次 flush 边界）后 WHERE ival >= 3 必须仍是文档序。
#[test]
fn multi_segment_where_keeps_document_order() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE m (id INT PRIMARY KEY, ival INT, fval REAL)")
        .unwrap();
    let batch = |lo: i64, hi: i64| {
        for i in lo..hi {
            db.execute(&format!(
                "INSERT INTO m VALUES ({}, {}, {})",
                i,
                i,
                i as f64 * 0.25
            ))
            .unwrap();
        }
        // TEXT-eq 探针触发 flush → 段边界（拆段复现的前提）。
        let _ = db.query("SELECT id FROM m WHERE id = -1").unwrap();
    };
    batch(0, 10);
    batch(10, 20);

    let rows = db.query("SELECT id FROM m WHERE ival >= 5").unwrap();
    let ids: Vec<i64> = rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            other => panic!("id as {other:?}"),
        })
        .collect();
    let want: Vec<i64> = (5..20).collect();
    assert_eq!(ids, want, "multi-segment scan must emit document order");
}

/// 5: 多段 + WHERE fval（浮点谓词）投影 FLOAT 不得出位模式垃圾值。
#[test]
fn multi_segment_float_projection_not_bit_cast() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE m (id INT PRIMARY KEY, sval TEXT, fval REAL)")
        .unwrap();
    let batch = |lo: i64, hi: i64| {
        for i in lo..hi {
            db.execute(&format!("INSERT INTO m VALUES ({}, 's{}', {})", i, i, 1.5))
                .unwrap();
        }
        let _ = db.query("SELECT id FROM m WHERE sval = 'none'").unwrap();
    };
    batch(0, 10);
    batch(10, 20);

    let rows = db.query("SELECT fval FROM m WHERE fval >= 0.5").unwrap();
    assert_eq!(rows.len(), 20);
    for r in &rows {
        assert_eq!(
            r[0],
            Value::Float(1.5),
            "float must round-trip exactly, got {:?}",
            r[0]
        );
    }
}

/// 6: DATE 列跨路径同构 —— WHERE + ORDER BY + LIMIT 组合不得 NULL/Integer。
#[test]
fn date_projection_consistent_across_paths() {
    let (db, _d) = setup();
    let paths = [
        "SELECT d FROM t",
        "SELECT d FROM t WHERE ival >= 7",
        "SELECT d FROM t WHERE ival = 7",
        "SELECT d FROM t ORDER BY id LIMIT 5",
        "SELECT d FROM t WHERE ival >= 7 ORDER BY id LIMIT 5",
        "SELECT d FROM t WHERE ival >= 7 ORDER BY ts DESC LIMIT 100000",
    ];
    let mut variants = std::collections::HashSet::new();
    for q in paths {
        let rows = db.query(q).unwrap();
        assert!(!rows.is_empty(), "{q} returned no rows");
        assert!(
            matches!(rows[0][0], Value::Date(_)),
            "{q}: DATE column must stay Date, got {}",
            canon(&rows[0][0])
        );
        variants.insert(std::mem::discriminant(&rows[0][0]));
    }
    assert_eq!(variants.len(), 1, "one Value variant across all paths");
}

/// 附带: ORDER BY d（DATE 排序键）+ LIMIT 的 top-k 不得静默失效。
#[test]
fn order_by_date_limit_sorts() {
    let (db, _d) = setup();
    let rows = db.query("SELECT d FROM t ORDER BY d DESC LIMIT 3").unwrap();
    let days: Vec<i32> = rows
        .iter()
        .map(|r| match &r[0] {
            Value::Date(d) => d.days(),
            other => panic!("d as {other:?}"),
        })
        .collect();
    let d0 = motedb::types::Date::parse("2026-01-01").unwrap().days();
    assert_eq!(
        days,
        vec![d0 + 11, d0 + 10, d0 + 9],
        "DATE sort keys must work"
    );
}
