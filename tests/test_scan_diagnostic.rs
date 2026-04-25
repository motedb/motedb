use motedb::{Database, DBConfig, StreamingQueryResult};
use tempfile::TempDir;

fn exec(db: &Database, sql: &str) -> motedb::sql::QueryResult {
    db.execute(sql).expect("execute SQL").materialize().expect("materialize")
}

fn count_rows(db: &Database) -> usize {
    let mut count = 0;
    let mut result = db.execute("SELECT id FROM t").unwrap();
    if let StreamingQueryResult::SelectStreaming { ref mut rows, .. } = result {
        while let Some(Ok(_)) = rows.next() { count += 1; }
    }
    count
}

fn get_row(db: &Database, id: i64) -> Option<Vec<motedb::types::Value>> {
    let sql = format!("SELECT * FROM t WHERE id = {}", id);
    let result = exec(db, &sql);
    match result {
        motedb::sql::QueryResult::Select { rows, .. } => rows.into_iter().next(),
        _ => None,
    }
}

/// Test A: INSERT + flush + scan (no UPDATE). Does compaction lose INSERT data?
#[test]
fn test_insert_flush_scan() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_edge()).unwrap();
    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT, amount FLOAT)");

    for i in 1..=5000i64 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'pending', {:.2})", i, i as f64 * 10.0));
    }
    db.flush().unwrap();

    let count = count_rows(&db);
    println!("After INSERT+flush: {} rows", count);

    // Check every row via PK
    let mut missing = Vec::new();
    for i in 1..=5000i64 {
        if get_row(&db, i).is_none() { missing.push(i); }
    }
    println!("Missing via PK: {}", missing.len());

    assert_eq!(count, 5000, "INSERT+flush should preserve all rows");
}

/// Test B: INSERT + flush + UPDATE + scan (the failing scenario)
#[test]
fn test_insert_flush_update_scan() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_edge()).unwrap();
    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT, amount FLOAT)");

    for i in 1..=5000i64 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'pending', {:.2})", i, i as f64 * 10.0));
    }
    db.flush().unwrap();

    // Verify data is there before UPDATE
    let pre_count = count_rows(&db);
    println!("Before UPDATE: {} rows", pre_count);
    assert_eq!(pre_count, 5000);

    // UPDATE and immediately scan
    for i in 1..=2500i64 {
        exec(&db, &format!("UPDATE t SET status = 'completed' WHERE id = {}", i));
    }

    let post_count = count_rows(&db);
    println!("After UPDATE: {} rows", post_count);

    let mut missing = Vec::new();
    for i in 1..=5000i64 {
        if get_row(&db, i).is_none() { missing.push(i); }
    }
    println!("Missing via PK: {}", missing.len());

    assert_eq!(post_count, 5000, "Should have 5000 after UPDATE");
}

/// Test C: INSERT + flush + sleep + UPDATE + scan (always passes)
#[test]
fn test_insert_flush_sleep_update_scan() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_edge()).unwrap();
    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT, amount FLOAT)");

    for i in 1..=5000i64 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'pending', {:.2})", i, i as f64 * 10.0));
    }
    db.flush().unwrap();
    std::thread::sleep(std::time::Duration::from_secs(2));

    for i in 1..=2500i64 {
        exec(&db, &format!("UPDATE t SET status = 'completed' WHERE id = {}", i));
    }

    let count = count_rows(&db);
    println!("With sleep: {} rows", count);
    assert_eq!(count, 5000);
}

/// Test D: INSERT + flush + UPDATE + flush + scan (double flush)
#[test]
fn test_double_flush() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_edge()).unwrap();
    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT, amount FLOAT)");

    for i in 1..=5000i64 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'pending', {:.2})", i, i as f64 * 10.0));
    }
    db.flush().unwrap();

    for i in 1..=2500i64 {
        exec(&db, &format!("UPDATE t SET status = 'completed' WHERE id = {}", i));
    }
    db.flush().unwrap();

    let count = count_rows(&db);
    println!("Double flush: {} rows", count);
    assert_eq!(count, 5000);
}
