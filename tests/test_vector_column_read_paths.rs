//! A flushed VECTOR column must read back through every query shape, not
//! just full scans. Found while validating multi-row INSERT: the columnar
//! point/top-K/filtered materializers only knew Fixed and Text columns, so
//! `SELECT emb … WHERE id = ?`, `… ORDER BY id LIMIT k`, `… WHERE id > 0`
//! and `… WHERE text = 'x'` all returned NULL for the embedding once the
//! write buffer had been checkpointed into a segment (full scans, which go
//! through read_vectors, were fine — which is why the recall evals passed).
use motedb::types::Value;
use motedb::{DBConfig, Database};
use tempfile::TempDir;

/// Query shapes that must all return row id = 2 with its embedding. `n` is
/// the table's row count (the DESC top-K needs a LIMIT that reaches id 2).
fn shapes(n: usize) -> Vec<(&'static str, String)> {
    vec![
        ("point lookup", "SELECT id, emb FROM d WHERE id = 2".into()),
        ("full scan", "SELECT id, emb FROM d".into()),
        (
            "order-by limit",
            "SELECT id, emb FROM d ORDER BY id LIMIT 3".into(),
        ),
        (
            "order-by desc limit",
            format!("SELECT id, emb FROM d ORDER BY id DESC LIMIT {}", n - 1),
        ),
        (
            "where + order-by limit",
            "SELECT id, emb FROM d WHERE id > 0 ORDER BY id LIMIT 3".into(),
        ),
        (
            "where + limit",
            "SELECT id, emb FROM d WHERE id > 0 LIMIT 3".into(),
        ),
        ("where range", "SELECT id, emb FROM d WHERE id > 0".into()),
        (
            "where text eq",
            "SELECT id, emb FROM d WHERE content = 'n1'".into(),
        ),
    ]
}

/// The emb of the row with id = 2 as returned by `sql`.
fn emb_of_row2(db: &Database, sql: &str) -> Option<Vec<f32>> {
    let r = db.execute(sql).unwrap().materialize().unwrap();
    let (_, rows) = r.select_rows().unwrap();
    let row = rows
        .iter()
        .find(|r| matches!(r[0], Value::Integer(2)))
        .unwrap_or_else(|| panic!("{sql}: row id=2 not returned"));
    match &row[1] {
        Value::Vector(v) => Some(v.to_vec()),
        Value::Null => None,
        other => panic!("{sql}: unexpected emb {other:?}"),
    }
}

fn check_all_shapes(db: &Database, n: usize, phase: &str) {
    for (name, sql) in shapes(n) {
        assert_eq!(
            emb_of_row2(db, &sql),
            Some(vec![1.0, 1.0, 2.0, 3.0]),
            "{phase}: {name} lost the vector column ({sql})"
        );
    }
}

#[test]
fn vector_column_survives_every_read_path_after_checkpoint() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE d (id INT PRIMARY KEY, content TEXT, emb VECTOR(4))")
        .unwrap();
    for i in 0..3 {
        db.execute(&format!(
            "INSERT INTO d (id, content, emb) VALUES ({}, 'n{}', [{}.0, 1.0, 2.0, 3.0])",
            i + 1,
            i,
            i
        ))
        .unwrap();
    }
    check_all_shapes(&db, 3, "before checkpoint");
    db.execute("CHECKPOINT").unwrap();
    check_all_shapes(&db, 3, "after checkpoint");
}

#[test]
fn vector_column_survives_every_read_path_bulk_auto_increment() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE d (id INT PRIMARY KEY AUTO_INCREMENT, content TEXT, emb VECTOR(4))")
        .unwrap();
    // ≥ 100 rows on an AUTO_INCREMENT table takes the fast batch lane.
    let values: Vec<String> = (0..120)
        .map(|i| format!("('n{i}', [{}.0, 1.0, 2.0, 3.0])", i))
        .collect();
    db.execute(&format!(
        "INSERT INTO d (content, emb) VALUES {}",
        values.join(",")
    ))
    .unwrap();
    check_all_shapes(&db, 120, "before checkpoint");
    db.execute("CHECKPOINT").unwrap();
    check_all_shapes(&db, 120, "after checkpoint");
}
