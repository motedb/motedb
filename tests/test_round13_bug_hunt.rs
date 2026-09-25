//! Round 13 — Bug 清除计划 (differential fuzz vs SQLite) 挖出的正确性回归.
//!
//! 来源: /tmp/fuzz_diff.py — SQLite(oracle) × MoteDB-LSM × MoteDB-列存 三路
//! 对拍 + 随机变异复跑, 12 seed × 400 查询全绿前共挖出 8 个真 bug。
use motedb::sql::{Lexer, Parser, QueryExecutor};
use motedb::types::Value;
use motedb::{MoteDB, QueryResult};
use std::sync::Arc;

fn ex(db: &Arc<MoteDB>, sql: &str) -> QueryResult {
    let stmt = Parser::new(Lexer::new(sql).tokenize().unwrap())
        .parse()
        .unwrap();
    QueryExecutor::new(db.clone()).execute(stmt).unwrap()
}

fn rows(db: &Arc<MoteDB>, sql: &str) -> Vec<Vec<Value>> {
    match ex(db, sql) {
        QueryResult::Select { rows, .. } => rows,
        other => panic!("expected Select, got {:?}", other),
    }
}

fn i(v: i64) -> Value {
    Value::Integer(v)
}

// ─────────────────────────────────────────────────────────────────────────
// Bug 1: JOIN/WHERE 快过滤的 `col <> x` 把 NULL 行也算进来
// (apply_op_value 的 Ne 对 Value::Null 返回 true — SQL 三值逻辑要求 UNKNOWN)
#[test]
fn join_where_ne_excludes_null_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("b1.mote")).unwrap());
    ex(&db, "CREATE TABLE a (id INT PRIMARY KEY, v INT)");
    ex(&db, "CREATE TABLE b (id INT PRIMARY KEY, aid INT)");
    for (id, v) in [(1, 5i64), (2, 10), (3, -1), (4, 5)] {
        ex(&db, &format!("INSERT INTO a VALUES ({}, {})", id, v));
    }
    // id=3 的 v 用 UPDATE 置 NULL (INSERT 字面量 NULL 走不同路径也顺带覆盖)
    ex(&db, "UPDATE a SET v = NULL WHERE id = 3");
    for (id, aid) in [(1, 1), (2, 2), (3, 3), (4, 4)] {
        ex(&db, &format!("INSERT INTO b VALUES ({}, {})", id, aid));
    }

    // v <> 10: id=1,4 (id=3 的 NULL 是 UNKNOWN → 排除); SQLite 同语义
    let r = rows(
        &db,
        "SELECT a.id FROM a JOIN b ON a.id = b.aid WHERE a.v <> 10 ORDER BY a.id ASC",
    );
    assert_eq!(r, vec![vec![i(1)], vec![i(4)]]);
    let r = rows(
        &db,
        "SELECT COUNT(*) FROM a JOIN b ON a.id = b.aid WHERE a.v <> 10",
    );
    assert_eq!(r[0][0], i(2));

    // 单表路径同样不得把 NULL 算进 <> / <
    let r = rows(&db, "SELECT id FROM a WHERE v <> 10 ORDER BY id ASC");
    assert_eq!(r, vec![vec![i(1)], vec![i(4)]]);
    let r = rows(&db, "SELECT id FROM a WHERE v < 100 ORDER BY id ASC");
    assert_eq!(r, vec![vec![i(1)], vec![i(2)], vec![i(4)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Bug 2: JOIN ORDER BY 未投影列的裸名跨列误匹配 (b.id → a.id), 第二排序键失效
#[test]
fn join_order_by_unprojected_secondary_key() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("b2.mote")).unwrap());
    ex(&db, "CREATE TABLE a (id INT PRIMARY KEY, v INT)");
    ex(&db, "CREATE TABLE b (id INT PRIMARY KEY, aid INT, s REAL)");
    ex(&db, "INSERT INTO a VALUES (1, 1)");
    ex(&db, "INSERT INTO b VALUES (1, 1, 10.0)");
    ex(&db, "INSERT INTO b VALUES (2, 1, 20.0)");
    ex(&db, "INSERT INTO b VALUES (3, 1, 30.0)");

    // b.id 未投影: DESC 必须让 s 按 b.id 降序 → 30, 20, 10
    let r = rows(
        &db,
        "SELECT a.id, b.s FROM a JOIN b ON a.id = b.aid ORDER BY a.id ASC, b.id DESC",
    );
    assert_eq!(
        r,
        vec![
            vec![i(1), Value::Float(30.0)],
            vec![i(1), Value::Float(20.0)],
            vec![i(1), Value::Float(10.0)],
        ]
    );
    let r = rows(
        &db,
        "SELECT a.id, b.s FROM a JOIN b ON a.id = b.aid ORDER BY a.id ASC, b.id ASC",
    );
    assert_eq!(
        r,
        vec![
            vec![i(1), Value::Float(10.0)],
            vec![i(1), Value::Float(20.0)],
            vec![i(1), Value::Float(30.0)],
        ]
    );
}

// Bug 2b: JOIN LIMIT 提前终止发生在排序之前 — LIMIT 边界取错行
#[test]
fn join_order_by_limit_boundary_matches_unlimited() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("b2b.mote")).unwrap());
    ex(&db, "CREATE TABLE items (id INT PRIMARY KEY, val REAL)");
    ex(
        &db,
        "CREATE TABLE tags (id INT PRIMARY KEY, item_id INT, score REAL)",
    );
    for k in 1..=15i64 {
        ex(&db, &format!("INSERT INTO items VALUES ({}, {}.5)", k, k));
        for t in 1..=2i64 {
            ex(
                &db,
                &format!(
                    "INSERT INTO tags VALUES ({}, {}, {}.0)",
                    k * 10 + t,
                    k,
                    (k * 10 + t) as f64
                ),
            );
        }
    }
    let limited = rows(
        &db,
        "SELECT i.id, t.score FROM items i JOIN tags t ON i.id = t.item_id ORDER BY i.id ASC, t.id ASC LIMIT 5",
    );
    let unlimited = rows(
        &db,
        "SELECT i.id, t.score FROM items i JOIN tags t ON i.id = t.item_id ORDER BY i.id ASC, t.id ASC",
    );
    assert_eq!(&limited[..], &unlimited[..5]);
}

// ─────────────────────────────────────────────────────────────────────────
// Bug 3: eval_expr_on_row 的 AND/OR 是二值逻辑 — NOT(NULL OR …) 全部行通过
#[test]
fn three_valued_logic_not_over_null_comparisons() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("b3.mote")).unwrap());
    ex(&db, "CREATE TABLE t (id INT PRIMARY KEY, g INT)");
    ex(&db, "INSERT INTO t VALUES (1, 0)");
    ex(&db, "INSERT INTO t VALUES (2, NULL)");
    ex(&db, "INSERT INTO t VALUES (3, 5)");

    // g < NULL → UNKNOWN; UNKNOWN OR false → UNKNOWN; NOT → UNKNOWN → 全排除
    let r = rows(&db, "SELECT id FROM t WHERE NOT (g < NULL OR g = -5)");
    assert!(r.is_empty());
    let r = rows(
        &db,
        "SELECT id FROM t WHERE NOT ((g < NULL) OR (g IN (-5, -5)))",
    );
    assert!(r.is_empty());
    // NOT (g = 5 OR g > NULL): SQL 语义下 g=0 → (false OR UNKNOWN)=UNKNOWN → NOT→UNKNOWN → 排除
    let r = rows(&db, "SELECT id FROM t WHERE NOT (g = 5 OR g > NULL)");
    assert!(r.is_empty());
    // NOT (g > NULL AND g = 5): Kleene — NULL AND false = FALSE → NOT → TRUE
    // → 只有 g=0 的 id=1 通过; id=3 (NULL AND true = NULL → NOT NULL 排除)
    let r = rows(&db, "SELECT id FROM t WHERE NOT (g > NULL AND g = 5)");
    assert_eq!(r, vec![vec![i(1)]]);
    // 反向: NOT (g IS NULL) / NOT (g IN) 保持正确
    let r = rows(
        &db,
        "SELECT id FROM t WHERE NOT (g IS NULL) ORDER BY id ASC",
    );
    assert_eq!(r, vec![vec![i(1)], vec![i(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Bug 4: ROUND 对 f64 真实二进制值做精确十进制 half-away 舍入 (对齐 SQLite)
#[test]
fn round_matches_binary_value_decimal_rounding() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("b4.mote")).unwrap());
    ex(&db, "CREATE TABLE t (id INT PRIMARY KEY, v REAL)");
    // (输入, 期望 ROUND(v,1)) — 期望值与 SQLite 完全一致
    let cases: Vec<(f64, f64)> = vec![
        (48.05, 48.0), // f64 真值 48.0499… → 48.0 (旧实现给 48.1)
        (0.15, 0.1),   // 真值 0.1499… → 0.1 (旧 0.2)
        (-0.15, -0.1),
        (48.15, 48.1), // 真值 48.1499… → 48.1 (旧 48.2)
        (2.675, 2.7),  // ROUND(v,1) 不涉及; 见下行 2 位
        (1.005, 1.0),
        (2.5, 2.5),
    ];
    for (k, (vin, _)) in cases.iter().enumerate() {
        ex(&db, &format!("INSERT INTO t VALUES ({}, {})", k + 1, vin));
    }
    let r = rows(&db, "SELECT ROUND(v, 1) FROM t ORDER BY id ASC");
    let expect = [48.0, 0.1, -0.1, 48.1, 2.7, 1.0, 2.5];
    for (got, want) in r.iter().zip(expect.iter()) {
        match &got[0] {
            Value::Float(f) => assert!(
                (f - want).abs() < 1e-12,
                "ROUND mismatch: got {} want {}",
                f,
                want
            ),
            other => panic!("expected float, got {:?}", other),
        }
    }
    // 2 位舍入: 2.675 的 f64 真值是 2.67499… → 2.67 (旧 2.68)
    let r = rows(
        &db,
        "SELECT ROUND(2.675, 2), ROUND(0.5, 0), ROUND(1.5, 0), ROUND(2.5, 0) FROM t LIMIT 1",
    );
    if let Value::Float(f) = r[0][0].clone() {
        assert!((f - 2.67).abs() < 1e-12, "ROUND(2.675,2) = {}", f);
    }
    if let Value::Float(f) = r[0][1].clone() {
        assert!((f - 1.0).abs() < 1e-12);
    }
    if let Value::Float(f) = r[0][2].clone() {
        assert!((f - 2.0).abs() < 1e-12);
    }
    if let Value::Float(f) = r[0][3].clone() {
        assert!((f - 3.0).abs() < 1e-12); // half away from zero on真值
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Bug 5: IN (SELECT …) 读取过期列存快照 — DELETE/UPDATE 后仍返回已删行
#[test]
fn in_subquery_sees_delete_and_update() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("b5.mote")).unwrap());
    ex(&db, "CREATE TABLE items (id INT PRIMARY KEY, cat TEXT)");
    ex(
        &db,
        "CREATE TABLE tags (id INT PRIMARY KEY, item_id INT, tag TEXT)",
    );
    for k in 1..=5i64 {
        let tag = if k % 2 == 1 { "red" } else { "blue" };
        ex(
            &db,
            &format!("INSERT INTO tags VALUES ({}, {}, '{}')", k, k, tag),
        );
    }
    for k in 1..=10i64 {
        ex(&db, &format!("INSERT INTO items VALUES ({}, 'c{}')", k, k));
    }
    let q = "SELECT id FROM items WHERE id IN (SELECT item_id FROM tags WHERE tag = 'red') ORDER BY id ASC";
    assert_eq!(rows(&db, q), vec![vec![i(1)], vec![i(3)], vec![i(5)]]);

    // DELETE 后立即查询: 不得命中已删行 (旧实现返回旧快照 {1,3,5})
    ex(&db, "DELETE FROM tags WHERE tag = 'red'");
    assert!(rows(&db, q).is_empty());

    // UPDATE 后: blue→red 应立即可见 (旧实现看到 0 行)
    ex(&db, "UPDATE tags SET tag = 'red'");
    assert_eq!(rows(&db, q), vec![vec![i(2)], vec![i(4)]]);

    // checkpoint (列存段物化) 后仍正确
    db.checkpoint().unwrap();
    assert_eq!(rows(&db, q), vec![vec![i(2)], vec![i(4)]]);

    // 全删后无 IN 子查询 WHERE 也正确
    ex(&db, "DELETE FROM tags");
    assert!(rows(
        &db,
        "SELECT id FROM items WHERE id IN (SELECT item_id FROM tags)"
    )
    .is_empty());
}

// ─────────────────────────────────────────────────────────────────────────
// Bug 6: 非 AUTO_INCREMENT PK 无列索引 — 点查询/聚合点 WHERE 硬报错
#[test]
fn point_where_on_non_autoinc_pk_without_index() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("b6.mote")).unwrap());
    ex(
        &db,
        "CREATE TABLE items (id INT PRIMARY KEY, cat TEXT, val REAL)",
    );
    for k in 1..=100i64 {
        ex(
            &db,
            &format!("INSERT INTO items VALUES ({}, 'c{}', {})", k, k % 5, k),
        );
    }
    // 旧实现: "Index error: Column index 'items.id' not found"
    let r = rows(
        &db,
        "SELECT COUNT(DISTINCT cat), MAX(val) FROM items WHERE id = 100",
    );
    assert_eq!(r, vec![vec![i(1), Value::Float(100.0)]]);
    let r = rows(&db, "SELECT MAX(val) FROM items WHERE id BETWEEN 90 AND 99");
    assert_eq!(r, vec![vec![Value::Float(99.0)]]);
    let r = rows(&db, "SELECT id FROM items WHERE id = 55");
    assert_eq!(r, vec![vec![i(55)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Bug 7: 聚合快路径 build_comparison_predicate 的 NULL 三值逻辑
#[test]
fn aggregate_where_null_literal_comparison_yields_null() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("b7.mote")).unwrap());
    ex(
        &db,
        "CREATE TABLE items (id INT PRIMARY KEY, grp INT, cat TEXT)",
    );
    for k in 1..=20i64 {
        let grp = if k % 7 == 0 {
            "NULL".to_string()
        } else {
            (k % 5).to_string()
        };
        ex(
            &db,
            &format!("INSERT INTO items VALUES ({}, {}, 'c{}')", k, grp, k % 3),
        );
    }
    // grp > NULL → UNKNOWN → 0 行 → MIN(cat) = NULL (旧实现返回 '' — 匹配了全部行)
    let r = rows(&db, "SELECT MIN(cat) FROM items WHERE grp > NULL");
    assert_eq!(r, vec![vec![Value::Null]]);
    // = NULL 同理永不匹配
    let r = rows(&db, "SELECT COUNT(*) FROM items WHERE grp = NULL");
    assert_eq!(r[0][0], i(0));
    // 行值为 NULL 的行不得进入 < 过滤的聚合: grp < 3 与 grp IN (0,1,2) 同数
    let r = rows(&db, "SELECT COUNT(*) FROM items WHERE grp < 3");
    let r2 = rows(&db, "SELECT COUNT(*) FROM items WHERE grp IN (0, 1, 2)");
    assert_eq!(r[0][0], r2[0][0]);
}

// ─────────────────────────────────────────────────────────────────────────
// Bug 8: LATEST BY + ORDER BY — 排序置换后 apply_latest_by 的索引配对错位
#[test]
fn latest_by_with_order_by_picks_correct_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("b8.mote")).unwrap());
    ex(
        &db,
        "CREATE TABLE m (sensor TEXT, ts TIMESTAMP, v REAL) TIMESERIES(ts)",
    );
    let data = [
        ("s1", 100i64, 1.0f64),
        ("s2", 101, 2.0),
        ("s1", 102, 3.0),
        ("s3", 103, 4.0),
        ("s2", 104, 5.0),
        ("s1", 105, 6.0),
    ];
    for (s, t, v) in data {
        ex(
            &db,
            &format!("INSERT INTO m VALUES ('{}', {}, {})", s, t, v),
        );
    }
    // 带 ORDER BY: 每 sensor 一行, 取 max-ts (旧实现错位: 返回 2 行 s2)
    let r = rows(
        &db,
        "SELECT sensor, ts, v FROM m LATEST BY sensor ORDER BY sensor",
    );
    assert_eq!(
        r,
        vec![
            vec![
                motedb::types::Value::text("s1".to_string()),
                ts(105),
                Value::Float(6.0)
            ],
            vec![
                motedb::types::Value::text("s2".to_string()),
                ts(104),
                Value::Float(5.0)
            ],
            vec![
                motedb::types::Value::text("s3".to_string()),
                ts(103),
                Value::Float(4.0)
            ],
        ]
    );
    // checkpoint (列存段) 后仍正确
    db.checkpoint().unwrap();
    let r = rows(
        &db,
        "SELECT sensor, ts, v FROM m LATEST BY sensor ORDER BY sensor",
    );
    assert_eq!(r.len(), 3);
    assert_eq!(r[0][0], motedb::types::Value::text("s1".to_string()));
    assert_eq!(r[0][2], Value::Float(6.0));
}

fn ts(micros: i64) -> Value {
    Value::Timestamp(motedb::types::Timestamp::from_micros(micros))
}

// ─────────────────────────────────────────────────────────────────────────
// Bug 9: GEOMETRY `ORDER BY loc <-> ST_POINT(...)` 被静默跳过 → 插入序
#[test]
fn spatial_order_by_distance_operator_sorts() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("b9.mote")).unwrap());
    ex(&db, "CREATE TABLE poi (id INT PRIMARY KEY, loc GEOMETRY)");
    let pts: [(i64, f64, f64); 6] = [
        (1, 0.0, 0.0),
        (2, 10.0, 0.0),
        (3, 0.0, 10.0),
        (4, -5.0, -5.0),
        (5, 0.5, 0.5),
        (6, 9.9, 0.1),
    ];
    for (id, x, y) in pts {
        db.insert_row_to_table(
            "poi",
            vec![
                i(id),
                Value::Spatial(Box::new(motedb::types::Geometry::Point(
                    motedb::types::Point::new(x, y),
                ))),
            ],
        )
        .unwrap();
    }
    // 距 (50,50): id6 ≈ 4010 < id2 = id3 = 4100 → [6, 2, 3] (2/3 tie 稳定序)
    let r = rows(
        &db,
        "SELECT id FROM poi ORDER BY loc <-> ST_POINT(50.0, 50.0) ASC LIMIT 3",
    );
    assert_eq!(r, vec![vec![i(6)], vec![i(2)], vec![i(3)]]);
    // checkpoint 后仍正确
    db.checkpoint().unwrap();
    let r = rows(
        &db,
        "SELECT id FROM poi ORDER BY loc <-> ST_POINT(50.0, 50.0) ASC LIMIT 3",
    );
    assert_eq!(r, vec![vec![i(6)], vec![i(2)], vec![i(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Round 13b — 扩展形状差分 (SQLite 语义对齐)
#[test]
fn group_by_alias_and_expression() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("r13b1.mote")).unwrap());
    ex(&db, "CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b INT)");
    for i in 1..=20i64 {
        ex(
            &db,
            &format!("INSERT INTO t VALUES ({}, 'x{}', {})", i, i % 3, i * 2),
        );
    }
    // GROUP BY 列别名: SELECT a AS k … GROUP BY k
    let r = rows(
        &db,
        "SELECT a AS k, COUNT(*) FROM t GROUP BY k ORDER BY k ASC",
    );
    assert_eq!(
        r,
        vec![
            vec![motedb::types::Value::text("x0".into()), i(6)],
            vec![motedb::types::Value::text("x1".into()), i(7)],
            vec![motedb::types::Value::text("x2".into()), i(7)],
        ]
    );
    // GROUP BY 表达式: b % 3 (此前 parse error: % 截断语句)
    let r = rows(
        &db,
        "SELECT b % 3 AS m, COUNT(*) FROM t GROUP BY b % 3 ORDER BY m ASC",
    );
    assert_eq!(
        r,
        vec![vec![i(0), i(6)], vec![i(1), i(7)], vec![i(2), i(7)]]
    );
    // GROUP BY 表达式别名形式
    let r2 = rows(
        &db,
        "SELECT b % 3 AS m, COUNT(*) FROM t GROUP BY m ORDER BY m ASC",
    );
    assert_eq!(r, r2);
}

#[test]
fn instr_returns_one_based_position() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("r13b2.mote")).unwrap());
    ex(&db, "CREATE TABLE t (id INT PRIMARY KEY, a TEXT)");
    ex(&db, "INSERT INTO t VALUES (1, 'x1'), (2, 'x2')");
    // 此前: 列上下文静默 NULL / 字面量 "Unknown function"
    let r = rows(&db, "SELECT INSTR(a, '1'), INSTR('abc', 'b'), INSTR('abc', 'z'), INSTR(a, NULL) FROM t WHERE id = 1");
    assert_eq!(r[0], vec![i(2), i(2), i(0), Value::Null]);
}

#[test]
fn concat_skips_null_arguments() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("r13b3.mote")).unwrap());
    ex(&db, "CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT)");
    ex(&db, "INSERT INTO t VALUES (1, 'hello', 'world')");
    ex(&db, "INSERT INTO t VALUES (2, 'hello', NULL)");
    // CONCAT 跳过 NULL (SQLite concat()/PG CONCAT); || 保持传播
    let r = rows(&db, "SELECT CONCAT(a, b), a || b FROM t ORDER BY id ASC");
    assert_eq!(
        r[0],
        vec![
            motedb::types::Value::text("helloworld".into()),
            motedb::types::Value::text("helloworld".into()),
        ]
    );
    assert_eq!(r[1][0], motedb::types::Value::text("hello".into()));
    assert!(matches!(r[1][1], Value::Null));
}

// ─────────────────────────────────────────────────────────────────────────
// Round 13c (资源测评挖出): join 单表谓词下推 — 曾先物化全表叉积再过滤
#[test]
fn join_where_pushdown_selective_and_null_safe() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("pd.mote")).unwrap());
    ex(&db, "CREATE TABLE a (id INT PRIMARY KEY, g INT, v INT)");
    ex(&db, "CREATE TABLE b (id INT PRIMARY KEY, g INT, w INT)");
    for i in 1..=50i64 {
        let v = if i % 7 == 0 {
            "NULL"
        } else {
            &(i * 2).to_string()
        };
        ex(
            &db,
            &format!("INSERT INTO a VALUES ({}, {}, {})", i, i % 5, v),
        );
        ex(
            &db,
            &format!("INSERT INTO b VALUES ({}, {}, {})", i, i % 5, i * 3),
        );
    }
    // 双侧谓词 + 跨表不等式 + NULL 三值: 下推与后过滤结果一致
    let sql = "SELECT COUNT(*) FROM a x JOIN b y ON x.g = y.g \
               WHERE x.id <= 20 AND y.id <= 30 AND x.v IS NOT NULL AND x.v <> 10";
    let got = rows(&db, sql);
    // 手工期望: x∈{id≤20, v 非 NULL 且 ≠10}, y∈{id≤30}, g 相等
    let mut want = 0i64;
    for xid in 1..=20i64 {
        if xid % 7 == 0 || xid * 2 == 10 {
            continue;
        }
        let xg = xid % 5;
        for yid in 1..=30i64 {
            if yid % 5 == xg {
                want += 1;
            }
        }
    }
    assert_eq!(got[0][0], i(want));
    // UPDATE 后下推谓词立刻可见 (无过期快照)
    ex(&db, "UPDATE b SET g = 4 WHERE id <= 30");
    let got = rows(&db, sql);
    let mut want2 = 0i64;
    for xid in 1..=20i64 {
        if xid % 7 == 0 || xid * 2 == 10 {
            continue;
        }
        let xg = xid % 5;
        for _y in 1..=30i64 {
            if 4 == xg {
                want2 += 1;
            }
        }
    }
    assert_eq!(got[0][0], i(want2));
}

// ─────────────────────────────────────────────────────────────────────────
// Round 13c-2: ON 合取的残余条件不得被 hash 快路径丢弃
#[test]
fn join_on_conjunction_residual_evaluated() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("onc.mote")).unwrap());
    ex(&db, "CREATE TABLE items (id INT PRIMARY KEY, cat TEXT)");
    ex(
        &db,
        "CREATE TABLE tags (id INT PRIMARY KEY, item_id INT, tag TEXT)",
    );
    for (i, c) in [(1, "a"), (2, "b")] {
        ex(&db, &format!("INSERT INTO items VALUES ({}, '{}')", i, c));
    }
    for (id, iid, tag) in [(1, 1, "red"), (2, 2, "blue"), (3, 2, "red")] {
        ex(
            &db,
            &format!("INSERT INTO tags VALUES ({}, {}, '{}')", id, iid, tag),
        );
    }
    // 自 join: ON2 残余 `t2.tag = t.tag` (跨表) — 曾被 hash 快路径丢弃,
    // 4 个组合全匹配 (fuzz: q_self_join 大面积 WRONG)
    let r = rows(
        &db,
        "SELECT i.id, t2.id FROM items i JOIN tags t ON i.id = t.item_id \
         JOIN tags t2 ON t2.item_id = i.id AND t2.tag = t.tag ORDER BY i.id ASC, t2.id ASC",
    );
    assert_eq!(
        r,
        vec![vec![i(1), i(1)], vec![i(2), i(2)], vec![i(2), i(3)]]
    );
    // 单表残余仍走预过滤 hash (快路径覆盖): red tags = id1(item1) + id3(item2) → 2
    let r = rows(
        &db,
        "SELECT COUNT(*) FROM items a JOIN tags b ON a.id = b.item_id AND b.tag = 'red'",
    );
    assert_eq!(r[0][0], i(2));
}

// ─────────────────────────────────────────────────────────────────────────
// Round 13e (扫描#2): GROUP BY 表达式双键丢键 + 计算键 hash join
#[test]
fn groupby_double_expression_keys_all_groups() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("gb2.mote")).unwrap());
    ex(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 0..20i64 {
        ex(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    // 双 canonical 表达式组键: 曾双双解析到第一个 SELECT 表达式 → 3 组 (应 15)
    let r = rows(
        &db,
        "SELECT id % 3, id % 5, COUNT(*) FROM t GROUP BY id % 3, id % 5",
    );
    assert_eq!(
        r.len(),
        15,
        "double expression key groups: got {:?}",
        r.len()
    );
    // 快路径 (无 ORDER BY) 与物化路径 (ORDER BY 序号 decline) 一致
    let r2 = rows(
        &db,
        "SELECT id % 3, id % 5, COUNT(*) FROM t GROUP BY id % 3, id % 5 ORDER BY 1, 2",
    );
    assert_eq!(r2.len(), 15);
    let key = |row: &Vec<Value>| -> (i64, i64) {
        match (&row[0], &row[1]) {
            (Value::Integer(a), Value::Integer(b)) => (*a, *b),
            _ => (i64::MIN, i64::MIN),
        }
    };
    let mut a = r;
    a.sort_by_key(key);
    let mut b = r2;
    b.sort_by_key(key);
    assert_eq!(a, b);
}

#[test]
fn non_equi_on_computed_key_join() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("nk.mote")).unwrap());
    ex(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=10i64 {
        ex(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i * 10));
    }
    // a.id = b.id - 1: 相邻对 (1,2),(2,3)…(9,10)
    let r = rows(
        &db,
        "SELECT a.id, b.id FROM t a JOIN t b ON a.id = b.id - 1 ORDER BY a.id ASC",
    );
    let mut expect = Vec::new();
    for k in 1..=9i64 {
        expect.push(vec![i(k), i(k + 1)]);
    }
    assert_eq!(r, expect);
    // 表达式在左: a.id * 2 = b.id → (1,2),(2,4),(3,6),(4,8),(5,10)
    let r = rows(
        &db,
        "SELECT a.id, b.id FROM t a JOIN t b ON a.id * 2 = b.id ORDER BY a.id ASC",
    );
    assert_eq!(
        r,
        vec![
            vec![i(1), i(2)],
            vec![i(2), i(4)],
            vec![i(3), i(6)],
            vec![i(4), i(8)],
            vec![i(5), i(10)],
        ]
    );
}
