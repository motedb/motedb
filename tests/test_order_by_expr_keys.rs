//! ORDER BY expression-key correctness on every execution path.
//!
//! Found via the Python bindings: `ORDER BY emb <-> ?` without a vector
//! index (or without LIMIT, or DESC, or with a WHERE filter) routed to the
//! col-segment scan whose try_sort_projected silently skipped expression
//! keys → arbitrary row order. The P0 pushdown also applied WHERE AFTER
//! fetching global top-k, so filtered ANN could return fewer rows than
//! LIMIT even when more matching rows existed.
use motedb::types::{ArcVec, Value};
use motedb::{DBConfig, Database};
use tempfile::TempDir;

fn ids(db: &Database, sql: &str) -> Vec<i64> {
    let r = db.execute(sql).unwrap().materialize().unwrap();
    let (_, rows) = r.select_rows().unwrap();
    rows.iter()
        .map(|row| match &row[0] {
            Value::Integer(i) => *i,
            v => panic!("expected integer id, got {:?}", v),
        })
        .collect()
}

fn ids_prepared(db: &Database, sql: &str, params: Vec<Value>) -> Vec<i64> {
    let r = db
        .execute_prepared(sql, params)
        .unwrap()
        .materialize()
        .unwrap();
    let (_, rows) = r.select_rows().unwrap();
    rows.iter()
        .map(|row| match &row[0] {
            Value::Integer(i) => *i,
            v => panic!("expected integer id, got {:?}", v),
        })
        .collect()
}

/// items on a line: emb = (id, 0, 0) → distance to query is |id - q0|.
fn seed_items(db: &Database) {
    db.execute("CREATE TABLE items (id INT PRIMARY KEY, cat TEXT, emb VECTOR(3))")
        .unwrap();
    for i in 1..=10i64 {
        db.execute(&format!(
            "INSERT INTO items (id, cat, emb) VALUES ({i}, '{}', [{i}.0, 0.0, 0.0])",
            if i % 2 == 0 { "even" } else { "odd" }
        ))
        .unwrap();
    }
    db.execute("CHECKPOINT").unwrap();
}

fn vec3(x: f32, y: f32, z: f32) -> Value {
    Value::Vector(ArcVec::new(vec![x, y, z]))
}

// ── No vector index: brute-force correctness ────────────────────────────────

#[test]
fn ann_literal_no_index_sorted_correctly() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    seed_items(&db);

    // Query at 5.5 → distances |id-5.5|: nearest are 5,6 (0.5), then 4,7 …
    let got = ids(
        &db,
        "SELECT id FROM items ORDER BY emb <-> [5.5, 0.0, 0.0] LIMIT 4",
    );
    assert_eq!(got, vec![5, 6, 4, 7], "must be nearest-first");
}

#[test]
fn ann_param_no_index_sorted_correctly() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    seed_items(&db);

    let got = ids_prepared(
        &db,
        "SELECT id FROM items ORDER BY emb <-> ? LIMIT 4",
        vec![vec3(5.5, 0.0, 0.0)],
    );
    assert_eq!(
        got,
        vec![5, 6, 4, 7],
        "param vector must order like literal"
    );
}

#[test]
fn ann_desc_returns_farthest() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    seed_items(&db);

    let got = ids(
        &db,
        "SELECT id FROM items ORDER BY emb <-> [5.5, 0.0, 0.0] DESC LIMIT 2",
    );
    assert_eq!(got, vec![1, 10], "DESC must return farthest first");
}

#[test]
fn ann_without_limit_sorts_all_rows() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    seed_items(&db);

    let got = ids(&db, "SELECT id FROM items ORDER BY emb <-> [2.0, 0.0, 0.0]");
    assert_eq!(got, vec![2, 1, 3, 4, 5, 6, 7, 8, 9, 10]);
}

#[test]
fn ann_cosine_no_index_sorted_correctly() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE v (id INT PRIMARY KEY, emb VECTOR(2))")
        .unwrap();
    // id 1: parallel to query (cos dist 0); id 2: orthogonal (dist 1);
    // id 3: opposite (dist 2).
    db.execute("INSERT INTO v (id, emb) VALUES (1, [2.0, 0.0])")
        .unwrap();
    db.execute("INSERT INTO v (id, emb) VALUES (2, [0.0, 3.0])")
        .unwrap();
    db.execute("INSERT INTO v (id, emb) VALUES (3, [-1.0, 0.0])")
        .unwrap();
    db.execute("CHECKPOINT").unwrap();

    let got = ids(&db, "SELECT id FROM v ORDER BY emb <=> [1.0, 0.0]");
    assert_eq!(
        got,
        vec![1, 2, 3],
        "cosine order: parallel, orthogonal, opposite"
    );
}

// ── Filtered ANN: WHERE must apply BEFORE top-k ─────────────────────────────

#[test]
fn filtered_ann_returns_limit_matching_rows() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    seed_items(&db);

    // Global top-2 near 5.5 is {5,6} = odd+even; filter to even must still
    // return 2 even rows ({6,4}), not 1 row after dropping odd id 5.
    let got = ids(
        &db,
        "SELECT id FROM items WHERE cat = 'even' ORDER BY emb <-> [5.5, 0.0, 0.0] LIMIT 2",
    );
    assert_eq!(got.len(), 2, "filtered ANN must return LIMIT matching rows");
    assert_eq!(got, vec![6, 4]);
}

#[test]
fn filtered_ann_param_vector() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    seed_items(&db);

    let got = ids_prepared(
        &db,
        "SELECT id FROM items WHERE cat = 'odd' ORDER BY emb <-> ? LIMIT 3",
        vec![vec3(6.0, 0.0, 0.0)],
    );
    assert_eq!(got, vec![5, 7, 3]);
}

// ── General expression / non-projected ORDER BY keys ────────────────────────

#[test]
fn order_by_non_projected_column() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
        .unwrap();
    for (id, val) in [(1, 30), (2, 10), (3, 20)] {
        db.execute(&format!("INSERT INTO t (id, val) VALUES ({id}, {val})"))
            .unwrap();
    }
    db.execute("CHECKPOINT").unwrap();

    let got = ids(&db, "SELECT id FROM t ORDER BY val");
    assert_eq!(
        got,
        vec![2, 3, 1],
        "ORDER BY must work on non-projected column"
    );
}

#[test]
fn order_by_arithmetic_expression() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
        .unwrap();
    for (id, val) in [(1, 5), (2, 1), (3, 3)] {
        db.execute(&format!("INSERT INTO t (id, val) VALUES ({id}, {val})"))
            .unwrap();
    }
    db.execute("CHECKPOINT").unwrap();

    let got = ids(&db, "SELECT id, val FROM t ORDER BY val * -1");
    assert_eq!(got, vec![1, 3, 2], "expression key on projected column");
}

#[test]
fn order_by_expression_key_desc() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    seed_items(&db);

    let got = ids(
        &db,
        "SELECT id, emb FROM items ORDER BY emb <-> [1.0, 0.0, 0.0] DESC LIMIT 3",
    );
    assert_eq!(got, vec![10, 9, 8]);
}

#[test]
fn ann_sees_unflushed_buffered_rows() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, emb VECTOR(2))")
        .unwrap();
    db.execute("INSERT INTO t (id, emb) VALUES (1, [10.0, 0.0])")
        .unwrap();
    db.execute("CHECKPOINT").unwrap(); // flush the far row only
    db.execute("INSERT INTO t (id, emb) VALUES (2, [0.1, 0.0])")
        .unwrap(); // near, buffered

    let got = ids(&db, "SELECT id FROM t ORDER BY emb <-> [0.0, 0.0] LIMIT 1");
    assert_eq!(got, vec![2], "buffered (unflushed) near row must win");
}
