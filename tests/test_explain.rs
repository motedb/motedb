//! EXPLAIN: heuristic plan reporting — scan strategy, aggregate, sort, limit.

use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        motedb::sql::QueryResult::Select { rows, .. } => rows,
        other => panic!("expected Select, got {:?} for {}", other, sql),
    }
}

/// All detail strings of the plan, concatenated, for substring asserts.
fn plan_text(db: &Database, sql: &str) -> String {
    let rs = rows(db, sql);
    rs.iter()
        .map(|r| match &r[2] {
            Value::Text(t) => t.to_string(),
            v => format!("{v:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn test_explain_reports_pk_point_lookup() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();

    let text = plan_text(&db, "EXPLAIN SELECT v FROM t WHERE id = 1");
    assert!(text.contains("pk_point_lookup"), "plan: {text}");
    assert!(text.contains("rows=1"), "plan: {text}");
}

#[test]
fn test_explain_reports_column_index() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, v INTEGER)")
        .unwrap();
    db.execute("CREATE INDEX t_cat ON t (cat)").unwrap();

    let text = plan_text(&db, "EXPLAIN SELECT v FROM t WHERE cat = 'a'");
    assert!(text.contains("column_index"), "plan: {text}");
}

#[test]
fn test_explain_reports_full_scan_and_topk() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();

    let text = plan_text(&db, "EXPLAIN SELECT * FROM t");
    assert!(text.contains("full_scan"), "plan: {text}");

    let text = plan_text(&db, "EXPLAIN SELECT * FROM t ORDER BY v LIMIT 5");
    assert!(text.contains("top_k_bounded_heap"), "plan: {text}");
    assert!(
        text.contains("sort") || text.contains("ORDER BY v"),
        "plan: {text}"
    );
}

#[test]
fn test_explain_reports_aggregate_and_limit() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, v INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a', 1), (2, 'a', 2)")
        .unwrap();

    let text = plan_text(
        &db,
        "EXPLAIN SELECT cat, SUM(v) FROM t GROUP BY cat LIMIT 3",
    );
    assert!(text.contains("aggregate"), "plan: {text}");
    assert!(text.contains("GROUP BY cat"), "plan: {text}");
    assert!(text.contains("LIMIT 3"), "plan: {text}");
}

#[test]
fn test_explain_does_not_execute_the_query() {
    // EXPLAIN must not run the statement: a would-fail type error inside the
    // SELECT is not evaluated (constant folding of WHERE 1/0 is not performed
    // by the heuristic planner).
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();

    let rs = rows(&db, "EXPLAIN SELECT * FROM t WHERE v = 1 / 0");
    assert!(!rs.is_empty());
}

#[test]
fn test_explain_rejects_non_select() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    assert!(db.execute("EXPLAIN INSERT INTO t VALUES (1)").is_err());
}
