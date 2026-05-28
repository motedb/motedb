use motedb::Database;
use motedb::config::DBConfig;
use motedb::types::Value;
use motedb::sql::executor::QueryResult;
use tempfile::TempDir;
use std::time::Instant;

fn create_db_with_cache(cache_size: usize) -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_edge();
    config.row_cache_size = Some(cache_size);
    let db = Database::create_with_config(dir.path(), config).unwrap();
    (db, dir)
}

fn get_score(db: &Database, id: i64) -> Value {
    let result = db.execute(&format!("SELECT score FROM t WHERE id = {}", id)).unwrap().materialize().unwrap();
    match result {
        QueryResult::Select { rows, .. } => rows[0][0].clone(),
        _ => panic!("Expected SELECT result"),
    }
}

#[test]
fn test_update_correctness_and_perf() {
    let (db, _dir) = create_db_with_cache(10_000);
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score FLOAT)").unwrap();

    for i in 1..=100i64 {
        db.execute(&format!("INSERT INTO t VALUES ({}, 'user_{}', {})", i, i, i as f64 * 10.0)).unwrap();
    }
    db.flush().unwrap();

    // Verify initial values
    assert_eq!(get_score(&db, 1), Value::Float(10.0), "Initial score for id=1 should be 10.0");
    assert_eq!(get_score(&db, 50), Value::Float(500.0), "Initial score for id=50 should be 500.0");

    // UPDATE row 1 via prepared statement (fast PK path)
    db.execute_prepared("UPDATE t SET score = ? WHERE id = ?", vec![Value::Float(999.0), Value::Integer(1)]).unwrap();
    assert_eq!(get_score(&db, 1), Value::Float(999.0), "Score should be 999.0 after prepared UPDATE");

    // UPDATE row 1 via raw SQL (slow path)
    db.execute("UPDATE t SET score = 123.0 WHERE id = 1").unwrap();
    assert_eq!(get_score(&db, 1), Value::Float(123.0), "Score should be 123.0 after SQL UPDATE");

    // Bulk UPDATE via prepared statement — verify every row
    for i in 1..=100i64 {
        db.execute_prepared("UPDATE t SET score = ? WHERE id = ?", vec![Value::Float(i as f64 * 7.0), Value::Integer(i)]).unwrap();
    }
    for i in 1..=100i64 {
        assert_eq!(get_score(&db, i), Value::Float(i as f64 * 7.0), "After bulk UPDATE, score for id={} wrong", i);
    }

    // Performance: prepared UPDATE
    let n = 500;
    let upd_sql = "UPDATE t SET score = ? WHERE id = ?";
    db.execute_prepared(upd_sql, vec![Value::Float(0.0), Value::Integer(1)]).unwrap();
    let now = Instant::now();
    for i in 1..=n as i64 {
        let target_id = i % 100 + 1;
        let new_score = i as f64;
        let result = db.execute_prepared(upd_sql, vec![Value::Float(new_score), Value::Integer(target_id)]).unwrap();
        assert_eq!(result.affected_rows(), 1, "UPDATE should affect 1 row");
    }
    let elapsed = now.elapsed();
    println!("UPDATE (100 rows, 10K cache): {:.1}µs/op", elapsed.as_micros() as f64 / n as f64);

    // Final: i=500 sets id=1, score=500.0
    assert_eq!(get_score(&db, 1), Value::Float(500.0), "Final score for id=1 should be 500.0");

    db.close().ok();
}

#[test]
fn test_value_size() {
    use std::mem::size_of;
    println!("Value: {} bytes", size_of::<motedb::types::Value>());
    println!("Row (Vec<Value>): {} bytes", size_of::<motedb::types::Row>());
    println!("ArcString: {} bytes", size_of::<motedb::types::ArcString>());
    println!("String: {} bytes", size_of::<String>());
}

#[test]
fn test_scan_layer_timing() {
    use std::time::Instant;
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_edge()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v2 TEXT, v3 FLOAT, v4 TEXT)").unwrap();

    let n = 10_000;
    for i in 1..=n as i64 {
        db.execute(&format!("INSERT INTO t VALUES ({}, 'hello_{}', {}, 'R{}')", i, i % 100, i as f64, i % 4)).unwrap();
    }

    for _ in 0..3 { db.execute("SELECT * FROM t").unwrap(); }

    let iters = 50;

    // Layer 1: SELECT * (fast path, no WHERE)
    let now = Instant::now();
    for _ in 0..iters {
        let _ = db.execute("SELECT * FROM t").unwrap().materialize().unwrap();
    }
    println!("SELECT * ({} rows): {:.1}µs", n, now.elapsed().as_micros() as f64 / iters as f64);

    // Layer 2: GROUP BY region (single-pass partial decode)
    let now = Instant::now();
    for _ in 0..iters {
        let _ = db.execute("SELECT v4, COUNT(*), AVG(v3) FROM t GROUP BY v4").unwrap().materialize().unwrap();
    }
    println!("GROUP BY v4: {:.1}µs", now.elapsed().as_micros() as f64 / iters as f64);

    // Layer 3: WHERE
    let now = Instant::now();
    for _ in 0..iters {
        let _ = db.execute("SELECT * FROM t WHERE v4 = 'R0'").unwrap().materialize().unwrap();
    }
    println!("WHERE v4='R0': {:.1}µs", now.elapsed().as_micros() as f64 / iters as f64);

    // Layer 4: ORDER BY
    let now = Instant::now();
    for _ in 0..iters {
        let _ = db.execute("SELECT * FROM t ORDER BY v3 DESC LIMIT 10").unwrap().materialize().unwrap();
    }
    println!("ORDER BY DESC LIMIT 10: {:.1}µs", now.elapsed().as_micros() as f64 / iters as f64);

    // Layer 5: PK lookup
    let now = Instant::now();
    for i in 1..=1000i64 {
        let _ = db.execute(&format!("SELECT * FROM t WHERE id = {}", i)).unwrap().materialize().unwrap();
    }
    println!("PK SELECT: {:.1}µs/op", now.elapsed().as_micros() as f64 / 1000.0);
}
