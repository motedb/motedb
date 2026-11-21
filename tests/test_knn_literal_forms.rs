//! 🔴 回归: KNN_SEARCH 的查询向量此前只认裸字面量 `[...]`，引号形式
//! `'[...]'`（与 `ORDER BY emb <-> '...'` 只差一对引号）被 parser 拒绝。
//! 统一接受两种拼法，结果必须一致。

use motedb::Database;
use tempfile::TempDir;

#[test]
fn knn_search_accepts_quoted_vector_literal() {
    std::env::set_var("MOTE_VEC_INDEX_MIN_ROWS", "100");
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE v (id INT PRIMARY KEY, emb VECTOR(8))")
        .unwrap();
    for i in 0..50 {
        let vals: Vec<String> = (0..8)
            .map(|d| format!("{:.3}", (i as f64 + d as f64 * 0.25) / 50.0))
            .collect();
        db.execute(&format!(
            "INSERT INTO v VALUES ({i}, '[{}]')",
            vals.join(",")
        ))
        .unwrap();
    }
    // KNN_SEARCH 谓词依赖向量索引（无索引时谓词不被通用路径求值 → 0 行）
    db.execute("CREATE VECTOR INDEX v_emb ON v(emb)").unwrap();
    std::thread::sleep(std::time::Duration::from_secs(3)); // 异步建图
                                                           // row 0 的向量作为查询
    let q: String = (0..8)
        .map(|d| format!("{:.3}", d as f64 * 0.25 / 50.0))
        .collect::<Vec<_>>()
        .join(",");

    let bare = db
        .query(&format!("SELECT id FROM v WHERE KNN_SEARCH(emb, [{q}], 3)"))
        .unwrap();
    let quoted = db
        .query(&format!(
            "SELECT id FROM v WHERE KNN_SEARCH(emb, '[{q}]', 3)"
        ))
        .expect("quoted KNN_SEARCH literal must parse (parity with ORDER BY <->)");
    let orderby = db
        .query(&format!(
            "SELECT id FROM v ORDER BY emb <-> '[{q}]' LIMIT 3"
        ))
        .unwrap();

    let ids = |rows: &Vec<Vec<motedb::types::Value>>| -> Vec<i64> {
        rows.iter()
            .map(|r| match &r[0] {
                motedb::types::Value::Integer(i) => *i,
                other => panic!("id as {other:?}"),
            })
            .collect()
    };
    let (b, qt, o) = (ids(&bare), ids(&quoted), ids(&orderby));
    assert_eq!(b, qt, "quoted and bare KNN_SEARCH must agree");
    assert_eq!(b.first(), Some(&0), "exact-match row must rank first");
    assert_eq!(o.first(), Some(&0), "ORDER BY path agrees");

    // 🔴 维度不匹配: KNN_SEARCH 此前静默返回空（search() 对错维产出零
    // 结果）——必须与 ORDER BY <-> 一致地报 InvalidArgument。
    let bad = db.query("SELECT id FROM v WHERE KNN_SEARCH(emb, [1.0, 2.0], 3)");
    match bad {
        Err(e) => assert!(
            e.to_string().contains("dimension mismatch"),
            "dim mismatch error, got: {e}"
        ),
        Ok(rows) => panic!("wrong-dim KNN_SEARCH must error, got {} rows", rows.len()),
    }
}
