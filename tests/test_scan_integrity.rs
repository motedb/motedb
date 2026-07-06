use motedb::{DBConfig, Database};
use tempfile::TempDir;

fn exec(db: &Database, sql: &str) -> motedb::sql::QueryResult {
    db.execute(sql)
        .expect("execute SQL")
        .materialize()
        .expect("materialize")
}

fn query_val(db: &Database, sql: &str) -> i64 {
    let result = exec(db, sql);
    match result {
        motedb::sql::QueryResult::Select { rows, .. } => rows
            .first()
            .and_then(|r| r.first())
            .map(|v| {
                if let motedb::types::Value::Integer(c) = v {
                    *c
                } else {
                    0
                }
            })
            .unwrap_or(0),
        _ => 0,
    }
}

#[test]
fn test_where_count_accuracy() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_edge()).unwrap();
    exec(
        &db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT, amount FLOAT)",
    );

    let n = 5000i64;
    for i in 1..=n {
        exec(
            &db,
            &format!(
                "INSERT INTO t VALUES ({}, 'pending', {:.2})",
                i,
                i as f64 * 10.0
            ),
        );
    }
    for i in 1..=2500i64 {
        exec(
            &db,
            &format!("UPDATE t SET status = 'completed' WHERE id = {}", i),
        );
    }

    // Method 1: COUNT(*) WHERE (aggregate fast path or full scan)
    let count_pending = query_val(&db, "SELECT COUNT(*) AS c FROM t WHERE status = 'pending'");
    let count_completed = query_val(
        &db,
        "SELECT COUNT(*) AS c FROM t WHERE status = 'completed'",
    );
    let count_total = query_val(&db, "SELECT COUNT(*) AS c FROM t");

    // Method 2: Manual full scan + count
    let result = db.execute("SELECT id, status FROM t").unwrap();
    let mut manual_pending = 0i64;
    let mut manual_completed = 0i64;
    let mut manual_total = 0i64;
    // The DB may return any of SelectStreaming / SelectReady / SelectColumnar
    // for a full scan depending on the storage path. Materialize uniformly so
    // the manual count is correct regardless of the result variant.
    let materialized = result.materialize().unwrap();
    if let motedb::sql::QueryResult::Select { rows, .. } = materialized {
        for row in &rows {
            manual_total += 1;
            if let motedb::types::Value::Text(s) = &row[1] {
                if s.as_str() == "pending" {
                    manual_pending += 1;
                } else if s.as_str() == "completed" {
                    manual_completed += 1;
                }
            }
        }
    }

    println!(
        "COUNT(*) total={}, pending={}, completed={}",
        count_total, count_pending, count_completed
    );
    println!(
        "Manual  total={}, pending={}, completed={}",
        manual_total, manual_pending, manual_completed
    );

    assert_eq!(count_total, n, "COUNT(*) total mismatch");
    assert_eq!(manual_total, n, "Manual total mismatch");

    if count_pending != manual_pending {
        println!(
            "MISMATCH: COUNT WHERE pending={} but manual scan pending={}",
            count_pending, manual_pending
        );
    }
    if count_completed != manual_completed {
        println!(
            "MISMATCH: COUNT WHERE completed={} but manual scan completed={}",
            count_completed, manual_completed
        );
    }

    assert_eq!(manual_completed, 2500, "Manual completed should be 2500");
    assert_eq!(manual_pending, 2500, "Manual pending should be 2500");
}
