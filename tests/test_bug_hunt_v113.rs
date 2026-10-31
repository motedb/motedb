//! Round-23 bug-hunt probes.
//!
//! Focus areas:
//! * vector KNN consistency under mutation — ORDER BY e <-> q must match
//!   brute-force ground truth after UPDATE/DELETE/INSERT cycles + reopen
//!   (recall drift = the marquee silent-wrong-result class for an AI DB)
//! * Chinese-text MATCH (jieba) — indexed vs non-indexed paths
//! * BLOB 64KB boundary (columnar escape-encoding edge: 0xFFFF + u32 len)
//! * numeric literal forms: 1e5 / 0x1F / 1_000
//! * CASE simple form, bare-boolean WHERE, NULL arithmetic propagation
//! * GROUP_CONCAT content
//! * query_by_column / query_by_column_range API vs SQL ground truth
//! * unicode LENGTH consistency

use motedb::types::{ArcBytes, ArcString, Value};
use motedb::Database;

fn new_db() -> (Database, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path().join("t.mote")).unwrap();
    (db, dir)
}

fn sel(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    db.query(sql).unwrap()
}

fn ints(rows: &[Vec<Value>]) -> Vec<i64> {
    rows.iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            o => panic!("expected int, got {o:?}"),
        })
        .collect()
}

// ---------------------------------------------------------------------
// Vector KNN vs brute force under mutation
// ---------------------------------------------------------------------

/// Deterministic distinct vectors; query chosen off-grid.
fn vec_of(i: i64) -> Vec<f32> {
    // Dimension 0 dominates: 1000·i makes the distance strictly increasing
    // in i (a per-unit step of ~2M vs a bounded ≤~262K contribution from the
    // other dimensions), so NO two distinct ids can tie on distance to Q.
    let mut v: Vec<f32> = (1..8)
        .map(|d| ((i * (d + 1) + d * d) % 97) as f32)
        .collect();
    v.insert(0, (i * 1000) as f32);
    v
}

fn knn_ground_truth(live: &[(i64, Vec<f32>)], q: &[f32], k: usize) -> Vec<i64> {
    let mut scored: Vec<(f64, i64)> = live
        .iter()
        .map(|(id, v)| {
            let d: f64 = v
                .iter()
                .zip(q.iter())
                .map(|(a, b)| {
                    let x = *a as f64 - *b as f64;
                    x * x
                })
                .sum();
            (d, *id)
        })
        .collect();
    scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap().then(a.1.cmp(&b.1)));
    // Distance ties would make exact-id comparison unfair — assert distinct.
    for w in scored.windows(2) {
        assert!(
            (w[0].0 - w[1].0).abs() > 1e-9,
            "distance tie: d={} between ids {} and {}; vecs: {:?} vs {:?}; n_live={}",
            w[0].0,
            w[0].1,
            w[1].1,
            live.iter().find(|(id, _)| *id == w[0].1),
            live.iter().find(|(id, _)| *id == w[1].1),
            live.len(),
        );
    }
    scored.into_iter().take(k).map(|(_, id)| id).collect()
}

fn live_vectors(db: &Database) -> Vec<(i64, Vec<f32>)> {
    sel(db, "SELECT id, e FROM emb")
        .into_iter()
        .map(|r| match (r[0].clone(), r[1].clone()) {
            (Value::Integer(id), Value::Vector(v)) => (id, v.0.to_vec()),
            o => panic!("expected (int, vector), got {o:?}"),
        })
        .collect()
}

const Q: [f32; 8] = [3.5, 1.25, 7.75, 0.5, 9.25, 4.0, 2.5, 6.75];

fn knn_query(db: &Database) -> Vec<i64> {
    let qs = format!(
        "'[{:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}]'",
        Q[0], Q[1], Q[2], Q[3], Q[4], Q[5], Q[6], Q[7]
    );
    ints(&sel(
        db,
        &format!("SELECT id FROM emb ORDER BY e <-> {qs} LIMIT 10"),
    ))
}

fn check_knn(db: &Database, phase: &str) {
    let live: Vec<(i64, Vec<f32>)> = live_vectors(db);
    assert!(!live.is_empty());
    let expected = knn_ground_truth(&live, &Q, 10);
    let got = knn_query(db);
    assert_eq!(got, expected, "{phase}: KNN drifted from brute force");
}

#[test]
fn vector_knn_consistent_under_mutation() {
    let (db, dir) = new_db();
    db.execute("CREATE TABLE emb (id INT PRIMARY KEY, e VECTOR(8))")
        .unwrap();
    let mut live: Vec<(i64, Vec<f32>)> = Vec::new();
    for i in 0..80i64 {
        let v = vec_of(i);
        let lit = format!(
            "'[{:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}]'",
            v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7]
        );
        db.execute(&format!("INSERT INTO emb VALUES ({i}, {lit})"))
            .unwrap();
        live.push((i, v));
    }
    check_knn(&db, "initial");

    // Phase 1: UPDATE 20 vectors to fresh values — same dominant-coordinate
    // discipline (dim 0 = 1000·i keeps distances unique per id), different
    // bounded jitter in dims 1..7 so the value genuinely changes.
    for i in (0..80).step_by(4) {
        let mut v: Vec<f32> = (1..8)
            .map(|d| ((i * (d + 3) + 5 * d * d) % 89) as f32)
            .collect();
        v.insert(0, (i * 1000) as f32);
        let lit = format!(
            "'[{:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}]'",
            v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7]
        );
        db.execute(&format!("UPDATE emb SET e = {lit} WHERE id = {i}"))
            .unwrap();
        if let Some(slot) = live.iter_mut().find(|(id, _)| *id == i) {
            slot.1 = v;
        }
    }
    check_knn(&db, "after update");

    // Phase 2: DELETE 12 rows
    for i in (1..80).step_by(7) {
        db.execute(&format!("DELETE FROM emb WHERE id = {i}"))
            .unwrap();
        live.retain(|(id, _)| *id != i);
    }
    check_knn(&db, "after delete");

    // Phase 3: INSERT 18 new rows
    for k in 0..18i64 {
        let id = 1000 + k;
        let v = vec_of(id);
        let lit = format!(
            "'[{:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}]'",
            v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7]
        );
        db.execute(&format!("INSERT INTO emb VALUES ({id}, {lit})"))
            .unwrap();
        live.push((id, v));
    }
    check_knn(&db, "after insert");

    // Phase 4: checkpoint + reopen — persisted vectors still sort right
    db.checkpoint().unwrap();
    drop(db);
    let db2 = Database::open(dir.path().join("t.mote")).unwrap();
    check_knn(&db2, "after reopen");
}

// ---------------------------------------------------------------------
// Chinese-text MATCH (jieba), indexed vs non-indexed
// ---------------------------------------------------------------------

#[test]
fn chinese_match_search() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)")
        .unwrap();
    db.execute("INSERT INTO docs VALUES (1, '机器人运动控制与规划')")
        .unwrap();
    db.execute("INSERT INTO docs VALUES (2, '深度学习模型训练流程')")
        .unwrap();
    db.execute("INSERT INTO docs VALUES (3, '机器人的神经网络控制器')")
        .unwrap();
    db.execute("INSERT INTO docs VALUES (4, 'database storage engine')")
        .unwrap();

    // '训练' appears only in row 2; 'storage' only in row 4 — unambiguous.
    let train = ints(&sel(
        &db,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('训练') ORDER BY id",
    ));
    assert_eq!(
        train,
        vec![2],
        "non-indexed MATCH finds the one row containing 训练"
    );
    let storage = ints(&sel(
        &db,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('storage')",
    ));
    assert_eq!(storage, vec![4]);

    // Multi-concept AND: 运动 AND 控制 → only row 1 (row 3 has 控制 not 运动)
    let both = ints(&sel(
        &db,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('运动 控制') ORDER BY id",
    ));
    assert_eq!(both, vec![1], "AND semantics: 运动+控制 only in row 1");

    // With a TEXT INDEX the same queries must agree with the row-local path.
    db.execute("CREATE TEXT INDEX doc_idx ON docs(body)")
        .unwrap();
    let train2 = ints(&sel(
        &db,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('训练') ORDER BY id",
    ));
    assert_eq!(train2, train, "indexed and non-indexed MATCH agree on 训练");
    let both2 = ints(&sel(
        &db,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('运动 控制') ORDER BY id",
    ));
    assert_eq!(
        both2, both,
        "indexed and non-indexed MATCH agree on AND query"
    );

    // UPDATE then re-match: FTS must see the new text (round-18 regression zone)
    db.execute("UPDATE docs SET body = '向量数据库存储引擎' WHERE id = 4")
        .unwrap();
    let after = ints(&sel(
        &db,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('向量')",
    ));
    assert_eq!(after, vec![4], "MATCH sees post-UPDATE text");
    let gone = ints(&sel(
        &db,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('storage')",
    ));
    assert_eq!(gone, Vec::<i64>::new(), "old token no longer matches");
}

// ---------------------------------------------------------------------
// BLOB 64KB boundary (columnar escape-encoding edge)
// ---------------------------------------------------------------------

#[test]
fn blob_64kb_boundary_roundtrip() {
    let (db, dir) = new_db();
    db.execute("CREATE TABLE bl (id INT PRIMARY KEY, data BLOB)")
        .unwrap();
    let sizes = [0usize, 1, 100, 65534, 65535, 65536, 65537, 200_000];
    for (i, sz) in sizes.iter().enumerate() {
        let bytes: Vec<u8> = (0..*sz).map(|b| (b % 251) as u8).collect();
        db.execute_prepared(
            "INSERT INTO bl VALUES (?, ?)",
            vec![Value::Integer(i as i64), Value::Blob(ArcBytes::new(bytes))],
        )
        .unwrap();
    }

    let verify = |db: &Database| {
        for (i, sz) in sizes.iter().enumerate() {
            let row = sel(db, &format!("SELECT data FROM bl WHERE id = {i}"))
                .pop()
                .unwrap();
            match &row[0] {
                Value::Blob(b) => {
                    assert_eq!(b.0.len(), *sz, "id {i}: blob len");
                    for (pos, byte) in b.0.iter().enumerate() {
                        assert_eq!(*byte, (pos % 251) as u8, "id {i}: byte {pos}");
                    }
                }
                o => panic!("id {i}: expected blob, got {o:?}"),
            }
        }
    };
    verify(&db);

    // Force segments to disk, then reopen — the escape-encoding boundary
    // (len > 0xFFFF stores a u32 length) must decode back byte-exact.
    db.checkpoint().unwrap();
    drop(db);
    let db2 = Database::open(dir.path().join("t.mote")).unwrap();
    verify(&db2);
}

// ---------------------------------------------------------------------
// Numeric literal forms
// ---------------------------------------------------------------------

#[test]
fn numeric_literal_forms() {
    let (db, _d) = new_db();
    let one = |sql: &str| -> Result<Value, motedb::MoteDBError> {
        Ok(db.query(sql)?.pop().unwrap()[0].clone())
    };

    // Scientific notation → FLOAT(100000)
    assert_eq!(one("SELECT 1e5").unwrap(), Value::Float(100_000.0));
    assert_eq!(one("SELECT 2.5e-1").unwrap(), Value::Float(0.25));
    // Hex literal → INTEGER(31)
    assert_eq!(one("SELECT 0x1F").unwrap(), Value::Integer(31));
    // NOTE: `SELECT 1_000` is legal standard SQL — 1 with implicit alias
    // `_000` (round-20) — so it is NOT probed here as an error.
    // Large i64 literal stays exact
    assert_eq!(
        one("SELECT 9223372036854775807").unwrap(),
        Value::Integer(i64::MAX),
    );
}

// ---------------------------------------------------------------------
// CASE simple form / bare boolean / NULL arithmetic / GROUP_CONCAT
// ---------------------------------------------------------------------

#[test]
fn case_simple_form_boolean_where_null_arith() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE m (id INT PRIMARY KEY, g TEXT, v INT, active BOOLEAN)")
        .unwrap();
    for (i, g, v, a) in [
        (0i64, "a", 5i64, true),
        (1, "b", 15, false),
        (2, "a", 25, true),
    ] {
        db.execute_prepared(
            "INSERT INTO m VALUES (?, ?, ?, ?)",
            vec![
                Value::Integer(i),
                Value::Text(ArcString::from(g)),
                Value::Integer(v),
                Value::Bool(a),
            ],
        )
        .unwrap();
    }

    // CASE simple form: CASE g WHEN 'a' THEN ...
    let r = sel(
        &db,
        "SELECT CASE g WHEN 'a' THEN 1 WHEN 'b' THEN 2 ELSE 0 END c FROM m ORDER BY id",
    );
    let got: Vec<i64> = r
        .iter()
        .map(|x| match &x[0] {
            Value::Integer(n) => *n,
            o => panic!("{o:?}"),
        })
        .collect();
    assert_eq!(got, vec![1, 2, 1]);

    // Bare boolean column as WHERE condition
    let r = ints(&sel(&db, "SELECT id FROM m WHERE active"));
    assert_eq!(r, vec![0, 2]);
    let r = ints(&sel(&db, "SELECT id FROM m WHERE NOT active"));
    assert_eq!(r, vec![1]);

    // NULL arithmetic propagation
    db.execute("INSERT INTO m VALUES (9, 'c', NULL, NULL)")
        .unwrap();
    let r = sel(&db, "SELECT v + 1 FROM m WHERE id = 9").pop().unwrap();
    assert!(matches!(r[0], Value::Null), "NULL + 1 → NULL, got {r:?}");
    let r = sel(&db, "SELECT v * 2 + 10 FROM m WHERE id = 9")
        .pop()
        .unwrap();
    assert!(matches!(r[0], Value::Null), "NULL arithmetic chain → NULL");
    // NULL boolean in WHERE → not matched (three-valued)
    let r = ints(&sel(&db, "SELECT id FROM m WHERE active"));
    assert_eq!(r, vec![0, 2], "NULL active row not matched by WHERE active");

    // GROUP_CONCAT content (order unspecified — compare as a set)
    let r = sel(
        &db,
        "SELECT g, GROUP_CONCAT(v) FROM m WHERE id < 9 GROUP BY g ORDER BY g",
    );
    let parse = |s: &str| -> Vec<i64> {
        let mut xs: Vec<i64> = s.split(',').filter_map(|p| p.trim().parse().ok()).collect();
        xs.sort_unstable();
        xs
    };
    match (&r[0][0], &r[0][1]) {
        (Value::Text(g), Value::Text(j)) => {
            assert_eq!(g.as_str(), "a");
            assert_eq!(parse(j), vec![5, 25]);
        }
        o => panic!("GROUP_CONCAT a-group: {o:?}"),
    }
    match (&r[1][0], &r[1][1]) {
        (Value::Text(g), Value::Text(j)) => {
            assert_eq!(g.as_str(), "b");
            assert_eq!(parse(j), vec![15]);
        }
        o => panic!("GROUP_CONCAT b-group: {o:?}"),
    }
}

// ---------------------------------------------------------------------
// query_by_column / query_by_column_range vs SQL
// ---------------------------------------------------------------------

#[test]
fn column_api_matches_sql() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE c (id INT PRIMARY KEY, val INT)")
        .unwrap();
    for i in 0..50i64 {
        db.execute(&format!("INSERT INTO c VALUES ({i}, {})", i * 2))
            .unwrap();
    }
    db.execute("CREATE INDEX val_idx ON c(val)").unwrap();

    let ids = db.query_by_column("c", "val", &Value::Integer(50)).unwrap();
    let mut ids: Vec<i64> = ids.iter().map(|r| *r as i64).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![25], "point lookup finds the one row with val=50");

    let ids = db
        .query_by_column_range("c", "val", &Value::Integer(10), &Value::Integer(20))
        .unwrap();
    let mut ids: Vec<i64> = ids.iter().map(|r| *r as i64).collect();
    ids.sort_unstable();
    let sql = ints(&sel(
        &db,
        "SELECT id FROM c WHERE val >= 10 AND val <= 20 ORDER BY id",
    ));
    assert_eq!(ids, sql, "range API agrees with SQL (inclusive bounds)");

    // After a mutation the index view must agree with SQL again
    db.execute("UPDATE c SET val = 15 WHERE id = 25").unwrap();
    let ids = db.query_by_column("c", "val", &Value::Integer(50)).unwrap();
    assert!(ids.is_empty(), "old value no longer found after UPDATE");
    // val=15 rows: only id 25 (id 5 has val=10 — the odd value exists only
    // via the UPDATE). Ground truth from SQL, not hand arithmetic.
    let ids = db.query_by_column("c", "val", &Value::Integer(15)).unwrap();
    let mut ids: Vec<i64> = ids.iter().map(|r| *r as i64).collect();
    ids.sort_unstable();
    let sql = ints(&sel(&db, "SELECT id FROM c WHERE val = 15 ORDER BY id"));
    assert_eq!(ids, sql, "index view agrees with SQL after UPDATE");
}

// ---------------------------------------------------------------------
// Unicode LENGTH consistency
// ---------------------------------------------------------------------

#[test]
fn unicode_length_consistency() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO u VALUES (1, '你好世界')").unwrap();
    db.execute("INSERT INTO u VALUES (2, 'abcd')").unwrap();

    // Pin the convention: character count (not bytes) — LENGTH('你好世界')=4
    let r = ints(&sel(&db, "SELECT LENGTH(s) FROM u ORDER BY id"));
    assert_eq!(r, vec![4, 4], "LENGTH counts characters, not bytes");

    // ORDER BY on unicode text: consistency between two runs (collation
    // itself is engine-defined; instability would be the bug)
    let a = sel(&db, "SELECT id FROM u ORDER BY s");
    let b = sel(&db, "SELECT id FROM u ORDER BY s");
    assert_eq!(a, b, "unicode ORDER BY is deterministic");
}
