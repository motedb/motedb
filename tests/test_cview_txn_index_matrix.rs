//! 🔴 连续视图一致性 × 事务回滚与索引 —— 第六块 oracle 扫描。
//!
//! A. 连续视图（lazy 全量重算 v1）在 INSERT/DELETE 后的输出必须与
//!    等价直接 GROUP BY 完全一致——重算语义下任何偏差都是 bug。
//! B. 事务回滚的行不得残留在 FTS / 向量索引里; COMMIT 的行必须可见。

use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn canon(v: &Value) -> String {
    match v {
        Value::Null => "∅".into(),
        Value::Integer(i) => format!("I:{i}"),
        Value::Float(f) => format!("F:{:.4}", f),
        Value::Text(s) => format!("S:{}", s.as_str()),
        Value::Timestamp(t) => format!("T:{}", t.as_micros()),
        other => format!("{other:?}"),
    }
}

fn rows_canon(rows: &[Vec<Value>]) -> Vec<String> {
    rows.iter()
        .map(|r| r.iter().map(canon).collect::<Vec<_>>().join(","))
        .collect()
}

// ---------------------------------------------------------------------------
// A. 连续视图 == 直接查询
// ---------------------------------------------------------------------------

#[test]
fn continuous_view_matches_direct_query_after_dml() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE sensor (ts TIMESTAMP, device TEXT, v FLOAT)")
        .unwrap();
    let base = 1_794_300_000_000_000i64;
    for i in 0..40 {
        db.execute(&format!(
            "INSERT INTO sensor VALUES ({}, 'dev{}', {})",
            base + (i % 4) * 60_000_000 + i * 1_000_000,
            i % 3,
            i as f64 * 0.5
        ))
        .unwrap();
    }
    db.execute(
        "CREATE CONTINUOUS VIEW cv AS \
         SELECT TIME_BUCKET('1m', ts) AS b, device, AVG(v) AS v_avg \
         FROM sensor GROUP BY b, device",
    )
    .unwrap();

    let direct = "SELECT TIME_BUCKET('1m', ts) AS b, device, AVG(v) AS v_avg \
                  FROM sensor GROUP BY b, device ORDER BY b, device";
    let via_cv = "SELECT b, device, v_avg FROM cv ORDER BY b, device";

    let check = |stage: &str| {
        let a = rows_canon(&db.query(direct).unwrap());
        let b = rows_canon(&db.query(via_cv).unwrap());
        assert_eq!(a, b, "{stage}: continuous view diverged from direct query");
    };
    check("initial");

    // 追加新桶的数据 → 重算必须纳入
    for i in 40..52 {
        db.execute(&format!(
            "INSERT INTO sensor VALUES ({}, 'dev{}', {})",
            base + 600_000_000 + i * 1_000_000,
            i % 3,
            i as f64 * 0.5
        ))
        .unwrap();
    }
    check("after insert new bucket");

    // 删除一个设备的全部数据 → 视图该组消失
    db.execute("DELETE FROM sensor WHERE device = 'dev1'")
        .unwrap();
    check("after delete device");

    // 外层过滤/排序/聚合在视图上工作且与直接路径一致
    let a = rows_canon(
        &db.query("SELECT device, COUNT(*) AS n FROM (SELECT device FROM cv) AS sub GROUP BY device ORDER BY device")
            .unwrap(),
    );
    let b = rows_canon(
        &db.query(
            "SELECT device, COUNT(*) AS n FROM (SELECT DISTINCT device, TIME_BUCKET('1m', ts) AS b FROM sensor) AS sub GROUP BY device ORDER BY device",
        )
        .unwrap(),
    );
    assert_eq!(a, b, "outer aggregation over cv vs direct");
}

// ---------------------------------------------------------------------------
// B. 事务回滚 × 索引（FTS + 向量）
// ---------------------------------------------------------------------------

#[test]
fn rollback_leaves_no_ghost_index_entries() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE d (id INT PRIMARY KEY, body TEXT, emb VECTOR(4))")
        .unwrap();
    for i in 0..20 {
        let e: Vec<String> = (0..4)
            .map(|k| format!("{:.2}", i as f64 + k as f64 * 0.25))
            .collect();
        db.execute(&format!(
            "INSERT INTO d VALUES ({i}, 'plain doc {i}', '[{}]')",
            e.join(",")
        ))
        .unwrap();
    }
    db.execute("CREATE TEXT INDEX d_body ON d(body)").unwrap();
    db.execute("CREATE VECTOR INDEX d_emb ON d(emb)").unwrap();
    std::thread::sleep(std::time::Duration::from_millis(1500));

    let fts = |db: &Database| -> Vec<i64> {
        db.query("SELECT id FROM d WHERE MATCH(body) AGAINST('ghostword') ORDER BY id")
            .unwrap()
            .iter()
            .map(|r| match &r[0] {
                Value::Integer(i) => *i,
                o => panic!("{o:?}"),
            })
            .collect()
    };
    let knn = |db: &Database| -> Vec<i64> {
        db.query("SELECT id FROM d ORDER BY emb <-> '[99.0, 99.0, 99.0, 99.0]' LIMIT 3")
            .unwrap()
            .iter()
            .map(|r| match &r[0] {
                Value::Integer(i) => *i,
                o => panic!("{o:?}"),
            })
            .collect()
    };

    // ROLLBACK: ghost 行不得残留在任一索引
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO d VALUES (500, 'ghostword payload', '[98.0, 98.5, 99.0, 99.5]')")
        .unwrap();
    // 事务内可见（read-your-writes 至少不能漏）
    assert_eq!(fts(&db), vec![500], "in-txn FTS must see the pending row");
    db.execute("ROLLBACK").unwrap();
    assert!(fts(&db).is_empty(), "ROLLBACK must remove the FTS entry");
    assert!(
        !knn(&db).contains(&500),
        "ROLLBACK must remove the vector entry"
    );

    // COMMIT 路径: 行与索引都持久
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO d VALUES (501, 'ghostword committed', '[98.0, 98.5, 99.0, 99.5]')")
        .unwrap();
    db.execute("COMMIT").unwrap();
    assert_eq!(fts(&db), vec![501], "committed row searchable via FTS");
    assert!(knn(&db).contains(&501), "committed row visible to KNN");
}

// ---------------------------------------------------------------------------
// C. 事务内外的向量字面量 INSERT 必须同构（2×2 矩阵）
// ---------------------------------------------------------------------------

#[test]
fn vector_literal_insert_txn_autocommit_parity() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE v (id INT PRIMARY KEY, emb VECTOR(4))")
        .unwrap();

    // 四种组合（引号/裸 × 事务/自动提交）都必须成功且存对值。
    db.execute("INSERT INTO v VALUES (1, '[1.0, 2.0, 3.0, 4.0]')")
        .unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO v VALUES (2, '[5.0, 6.0, 7.0, 8.0]')")
        .unwrap();
    db.execute("COMMIT").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO v VALUES (3, [9.0, 10.0, 11.0, 12.0])")
        .unwrap();
    db.execute("COMMIT").unwrap();
    db.execute("INSERT INTO v VALUES (4, [13.0, 14.0, 15.0, 16.0])")
        .unwrap();

    let rows = db.query("SELECT id, emb FROM v ORDER BY id").unwrap();
    assert_eq!(
        rows.len(),
        4,
        "all four literal/txn combinations must insert"
    );
    let expect: &[(i64, f32)] = &[(1, 1.0), (2, 5.0), (3, 9.0), (4, 13.0)];
    for (r, (id, first)) in rows.iter().zip(expect) {
        assert_eq!(&r[0], &Value::Integer(*id));
        match &r[1] {
            Value::Vector(v) => {
                assert!(
                    !v.0.is_empty() && (v.0[0] - first).abs() < 1e-6,
                    "id {id} first dim"
                );
            }
            other => panic!("id {id} emb as {other:?}"),
        }
    }
    // KNN 精确命中
    let top = &db
        .query("SELECT id FROM v ORDER BY emb <-> '[5.0, 6.0, 7.0, 8.0]' LIMIT 1")
        .unwrap()[0][0];
    assert_eq!(
        top,
        &Value::Integer(2),
        "KNN must rank the exact vector first"
    );

    // 维度不符仍要明确报错（不是静默兜底）
    let bad = db.execute("INSERT INTO v VALUES (9, '[1.0, 2.0]')");
    assert!(bad.is_err(), "wrong-dim insert must fail validation");
}
